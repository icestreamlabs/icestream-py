import asyncio
import datetime
import io
import struct
import uuid
from asyncio import Future, StreamReader, StreamWriter
from asyncio import Server as AsyncIOServer
from typing import Any, Callable, List, Sequence

import kio.schema.api_versions.v0 as api_v0
import kio.schema.api_versions.v1 as api_v1
import kio.schema.api_versions.v2 as api_v2
import kio.schema.api_versions.v3 as api_v3
import kio.schema.api_versions.v4 as api_v4
import kio.schema.metadata.v0 as metadata_v0
import kio.schema.metadata.v1 as metadata_v1
import kio.schema.metadata.v2 as metadata_v2
import kio.schema.metadata.v3 as metadata_v3
import kio.schema.metadata.v4 as metadata_v4
import kio.schema.metadata.v5 as metadata_v5
import kio.schema.metadata.v6 as metadata_v6
import kio.schema.produce.v0 as produce_v0
import kio.schema.produce.v1 as produce_v1
import kio.schema.produce.v2 as produce_v2
import kio.schema.produce.v3 as produce_v3
import kio.schema.produce.v4 as produce_v4
import kio.schema.produce.v5 as produce_v5
import kio.schema.produce.v6 as produce_v6
import kio.schema.produce.v7 as produce_v7
import kio.schema.produce.v8 as produce_v8
import structlog
from kio.index import load_payload_module
from kio.schema.errors import ErrorCode
from kio.schema.types import BrokerId, TopicName
from kio.serial.readers import read_int32
from kio.static.constants import EntityType
from kio.static.primitive import i16, i32, i32Timedelta, i64
from sqlalchemy import select, update
from sqlalchemy.orm import selectinload

from icestream.config import Config
from icestream.kafkaserver.handler import api_compatibility, handle_kafka_request
from icestream.kafkaserver.handlers import KafkaHandler
from icestream.kafkaserver.handlers.api_versions import (
    ApiVersionsRequest,
    ApiVersionsRequestHeader,
    ApiVersionsResponse,
)
from icestream.kafkaserver.handlers.metadata import (
    MetadataRequest,
    MetadataRequestHeader,
    MetadataResponse,
)
from icestream.kafkaserver.handlers.produce import (
    ProduceRequest,
    ProduceRequestHeader,
    ProduceResponse,
)
from icestream.kafkaserver.messages import (
    CreatableTopicResult,
    CreateTopicsRequest,
    CreateTopicsRequestHeader,
    CreateTopicsResponse,
)
from icestream.kafkaserver.protocol import KafkaRecordBatch
from icestream.kafkaserver.types import ProduceTopicPartitionData
from icestream.models import Partition, Topic

log = structlog.get_logger()


class Server:
    def __init__(self, config: Config, queue: asyncio.Queue[ProduceTopicPartitionData]):
        self.listener: AsyncIOServer | None = None
        self.config = config
        self.produce_queue = queue

    async def run(self, host: str = "127.0.0.1", port: int = 9092):
        try:
            self.listener = await asyncio.start_server(Connection(self), host, port)
            log.info(f"Server started listening on {host}:{port}")
            async with self.listener:
                await self.listener.serve_forever()
        except Exception as e:
            log.error(f"Error in server run: {e}")
            if self.listener:
                self.listener.close()


class Connection(KafkaHandler):
    def __init__(self, s: Server):
        self.server: Server = s

    async def __call__(self, reader: StreamReader, writer: StreamWriter) -> None:
        try:
            while True:
                msg_length_bytes = await reader.readexactly(4)
                msg_length = read_int32(io.BytesIO(msg_length_bytes))
                message = await reader.readexactly(msg_length)
                api_key = struct.unpack(">H", message[:2])[0]
                await handle_kafka_request(api_key, message, self, writer)

        except asyncio.IncompleteReadError as e:
            log.info(f"client disconnected with error: {e}")
        except Exception as e:
            log.error(f"error handling connection {e}")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
                log.info("connection closed")
            except Exception as e:
                log.error(f"error closing writer: {e}")

    async def handle_produce_request(
        self,
        header: ProduceRequestHeader,
        req: ProduceRequest,
        api_version: int,
        callback: Callable[[ProduceResponse], Any],
    ):
        topic_responses: List[produce_v8.response.TopicProduceResponse] = []

        for topic in req.topic_data:
            topic_name = topic.name
            partition_responses: List[produce_v8.response.PartitionProduceResponse] = []

            for partition in topic.partition_data:
                partition_idx = partition.index
                record_count = 0
                records = partition.records
                if records is not None:
                    record_count = int(struct.unpack(">I", records[57:61])[0])
                records_data = records[61:]
                magic = records[16]
                error_code: ErrorCode | None = None
                if magic != 2:
                    error_code = ErrorCode.unsupported_for_message_format
                    partition_response = produce_v8.response.PartitionProduceResponse(
                        index=i32(partition_idx),
                        error_code=ErrorCode.none if error_code is None else error_code,
                        base_offset=i64(
                            -1
                        ),  # magic number wrong, so no offsets assigned
                        log_append_time=None,
                        log_start_offset=i64(-1),
                        record_errors=(),
                        error_message="wrong magic number",
                    )
                    partition_responses.append(partition_response)
                    continue

                log.info("produce", records=records[61:])

                log.info(
                    "produce",
                    topic=topic_name,
                    partition=partition_idx,
                    num_records=record_count,
                )

                if record_count == 0:
                    partition_response = produce_v8.response.PartitionProduceResponse(
                        index=i32(partition_idx),
                        error_code=ErrorCode.none if error_code is None else error_code,
                        base_offset=i64(-1),  # no records so no offsets assigned
                        log_append_time=None,
                        log_start_offset=i64(-1),
                        record_errors=(),
                        error_message=None,
                    )
                    partition_responses.append(partition_response)
                    continue

                # allocate the offsets
                async with self.server.config.async_session_factory() as session:
                    result = await session.execute(
                        update(Partition)
                        .where(
                            Partition.topic_name == topic_name,
                            Partition.partition_number == partition_idx,
                        )
                        .values(last_offset=Partition.last_offset + record_count)
                        .returning(
                            Partition.id,
                            Partition.last_offset,
                            Partition.log_start_offset,
                        )
                    )
                    await session.commit()
                row = result.first()
                # if the row is none, it's probably because there's no topic or partition for it
                if row is None:
                    error_code = ErrorCode.unknown_topic_or_partition
                    partition_response = produce_v8.response.PartitionProduceResponse(
                        index=i32(partition_idx),
                        error_code=ErrorCode.none if error_code is None else error_code,
                        base_offset=i64(
                            -1
                        ),  # likely no topic/partition, so no offsets assigned
                        log_append_time=None,
                        log_start_offset=i64(-1),
                        record_errors=(),
                        error_message="unknown topic or partition",
                    )
                    partition_responses.append(partition_response)
                    continue
                _, last_offset, log_start_offset = row
                first_offset = last_offset - record_count + 1

                # need to send the message batch over to the WALManager via self.server.produce_queue (asyncio.Queue)
                # but with a future so that this part that's creating the PartitionProduceResponse can wait for the response from the WALManager
                # but at the end need to append the PartitionProduceResponse to the `partition_responses` list.
                partition_flush_result_fut = Future()
                produce_topic_partition_data = ProduceTopicPartitionData(
                    topic=topic_name,
                    partition=partition_idx,
                    kafka_record_batch=KafkaRecordBatch.from_bytes(records),
                    flush_result=partition_flush_result_fut,
                )
                await self.server.produce_queue.put(produce_topic_partition_data)

                try:
                    await asyncio.wait_for(
                        partition_flush_result_fut,
                        timeout=self.server.config.FLUSH_INTERVAL * 2,
                    )
                    # success
                except asyncio.CancelledError:
                    error_code = ErrorCode.unknown_server_error
                except asyncio.TimeoutError:
                    error_code = ErrorCode.request_timed_out
                except Exception as e:
                    error_code = ErrorCode.unknown_server_error
                finally:
                    partition_response = produce_v8.response.PartitionProduceResponse(
                        index=i32(partition_idx),
                        error_code=ErrorCode.none if error_code is None else error_code,
                        base_offset=i64(first_offset if error_code is None else -1),
                        log_append_time=None,  # TODO
                        log_start_offset=i64(log_start_offset),
                        record_errors=(),
                        error_message=None,
                    )
                    partition_responses.append(partition_response)

            topic_response = produce_v8.response.TopicProduceResponse(
                name=topic_name,
                partition_responses=tuple(partition_responses),
            )
            topic_responses.append(topic_response)

        reference_response = produce_v8.response.ProduceResponse(
            responses=tuple(topic_responses),
            throttle_time=i32Timedelta.parse(datetime.timedelta(milliseconds=0)),
        )

        if api_version == 0:
            topics = []
            for topic in reference_response.responses:
                partitions = [
                    produce_v0.response.PartitionProduceResponse(
                        index=p.index,
                        error_code=p.error_code,
                        base_offset=p.base_offset,
                    )
                    for p in topic.partition_responses
                ]
                topics.append(
                    produce_v0.response.TopicProduceResponse(
                        name=topic.name,
                        partition_responses=tuple(partitions),
                    )
                )
            await callback(produce_v0.response.ProduceResponse(responses=tuple(topics)))

        elif api_version == 1:
            topics = []
            for topic in reference_response.responses:
                partitions = [
                    produce_v1.response.PartitionProduceResponse(
                        index=p.index,
                        error_code=p.error_code,
                        base_offset=p.base_offset,
                    )
                    for p in topic.partition_responses
                ]
                topics.append(
                    produce_v1.response.TopicProduceResponse(
                        name=topic.name,
                        partition_responses=tuple(partitions),
                    )
                )
            await callback(
                produce_v1.response.ProduceResponse(
                    responses=tuple(topics),
                    throttle_time=reference_response.throttle_time,
                )
            )

        elif api_version == 2:
            topics = []
            for topic in reference_response.responses:
                partitions = [
                    produce_v2.response.PartitionProduceResponse(
                        index=p.index,
                        error_code=p.error_code,
                        base_offset=p.base_offset,
                        log_append_time=p.log_append_time,
                    )
                    for p in topic.partition_responses
                ]
                topics.append(
                    produce_v2.response.TopicProduceResponse(
                        name=topic.name,
                        partition_responses=tuple(partitions),
                    )
                )
            await callback(
                produce_v2.response.ProduceResponse(
                    responses=tuple(topics),
                    throttle_time=reference_response.throttle_time,
                )
            )

        elif api_version == 3:
            topics = []
            for topic in reference_response.responses:
                partitions = [
                    produce_v3.response.PartitionProduceResponse(
                        index=p.index,
                        error_code=p.error_code,
                        base_offset=p.base_offset,
                        log_append_time=p.log_append_time,
                    )
                    for p in topic.partition_responses
                ]
                topics.append(
                    produce_v3.response.TopicProduceResponse(
                        name=topic.name,
                        partition_responses=tuple(partitions),
                    )
                )
            await callback(
                produce_v3.response.ProduceResponse(
                    responses=tuple(topics),
                    throttle_time=reference_response.throttle_time,
                )
            )

        elif api_version == 4:
            topics = []
            for topic in reference_response.responses:
                partitions = [
                    produce_v4.response.PartitionProduceResponse(
                        index=p.index,
                        error_code=p.error_code,
                        base_offset=p.base_offset,
                        log_append_time=p.log_append_time,
                    )
                    for p in topic.partition_responses
                ]
                topics.append(
                    produce_v4.response.TopicProduceResponse(
                        name=topic.name,
                        partition_responses=tuple(partitions),
                    )
                )
            await callback(
                produce_v4.response.ProduceResponse(
                    responses=tuple(topics),
                    throttle_time=reference_response.throttle_time,
                )
            )

        elif api_version == 5:
            topics = []
            for topic in reference_response.responses:
                partitions = [
                    produce_v5.response.PartitionProduceResponse(
                        index=p.index,
                        error_code=p.error_code,
                        base_offset=p.base_offset,
                        log_append_time=p.log_append_time,
                        log_start_offset=p.log_start_offset,
                    )
                    for p in topic.partition_responses
                ]
                topics.append(
                    produce_v5.response.TopicProduceResponse(
                        name=topic.name,
                        partition_responses=tuple(partitions),
                    )
                )
            await callback(
                produce_v5.response.ProduceResponse(
                    responses=tuple(topics),
                    throttle_time=reference_response.throttle_time,
                )
            )

        elif api_version == 6:
            topics = []
            for topic in reference_response.responses:
                partitions = [
                    produce_v6.response.PartitionProduceResponse(
                        index=p.index,
                        error_code=p.error_code,
                        base_offset=p.base_offset,
                        log_append_time=p.log_append_time,
                        log_start_offset=p.log_start_offset,
                    )
                    for p in topic.partition_responses
                ]
                topics.append(
                    produce_v6.response.TopicProduceResponse(
                        name=topic.name,
                        partition_responses=tuple(partitions),
                    )
                )
            await callback(
                produce_v6.response.ProduceResponse(
                    responses=tuple(topics),
                    throttle_time=reference_response.throttle_time,
                )
            )

        elif api_version == 7:
            topics = []
            for topic in reference_response.responses:
                partitions = [
                    produce_v7.response.PartitionProduceResponse(
                        index=p.index,
                        error_code=p.error_code,
                        base_offset=p.base_offset,
                        log_append_time=p.log_append_time,
                        log_start_offset=p.log_start_offset,
                    )
                    for p in topic.partition_responses
                ]
                topics.append(
                    produce_v7.response.TopicProduceResponse(
                        name=topic.name,
                        partition_responses=tuple(partitions),
                    )
                )
            await callback(
                produce_v7.response.ProduceResponse(
                    responses=tuple(topics),
                    throttle_time=reference_response.throttle_time,
                )
            )

        elif api_version == 8:
            await callback(reference_response)

        else:
            log.error(f"Unsupported produce version: {api_version}")

    def produce_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: ProduceRequest,
        api_version: int,
    ) -> ProduceResponse:
        # v8 has error_message and record_errors in the PartitionProduceResponse, ignore it for now
        mod = load_payload_module(0, api_version, EntityType.response)
        responses = []
        for i, topic_data in enumerate(req.topic_data):
            partition_response = []
            for j, partition_data in enumerate(topic_data.partition_data):
                partition_produce_response = mod.PartitionProduceResponse(
                    index=partition_data.index,
                    error_code=error_code,
                    base_offset=i64(0),
                )
                partition_response.append(partition_produce_response)

            topic_produce_response = mod.TopicProduceResponse(
                name=topic_data.name, partition_responses=tuple(partition_response)
            )
            responses.append(topic_produce_response)
        resp = mod.ProduceResponse(responses=tuple(responses))
        return resp

    async def handle_metadata_request(
        self,
        header: MetadataRequestHeader,
        req: MetadataRequest,
        api_version: int,
        callback: Callable[[MetadataResponse], Any],
    ):
        log.info("handling metadata request", topics=[t.name for t in req.topics])

        async with self.server.config.async_session_factory() as session:
            topic_result: Sequence[Topic]
            if not req.topics:
                result = await session.execute(
                    select(Topic).options(selectinload(Topic.partitions))
                )
                topic_result = result.scalars().all()
            else:
                topic_names = [t.name for t in req.topics]
                result = await session.execute(
                    select(Topic)
                    .where(Topic.name.in_(topic_names))
                    .options(selectinload(Topic.partitions))
                )
                topic_result = result.scalars().all()

        # get brokers
        # since we're stateless we might be able to get away with spoofing a single broker
        # host would be the lb or k8s service or whatever
        # node id would always be 0
        # rack would always be None
        broker = metadata_v6.response.MetadataResponseBroker(
            node_id=i32(0), host="localhost", port=i32(9092), rack=None
        )

        # we need to respect the topic list passed in by the request
        # in our case it'll get passed to postgres, but an empty list means all of them

        topics: List[metadata_v6.response.MetadataResponseTopic] = []
        for topic in topic_result:
            partition_metadata = [
                metadata_v6.response.MetadataResponsePartition(
                    error_code=ErrorCode.none,
                    partition_index=i32(pidx.partition_number),
                    leader_id=i32(0),
                    replica_nodes=(i32(0),),
                    isr_nodes=(i32(0),),
                    offline_replicas=(),
                )
                for pidx in topic.partitions
            ]
            topics.append(
                metadata_v6.response.MetadataResponseTopic(
                    error_code=ErrorCode.none,
                    name=TopicName(topic.name),
                    is_internal=False,
                    partitions=tuple(partition_metadata),
                )
            )

        response = metadata_v6.response.MetadataResponse(
            throttle_time=i32Timedelta.parse(datetime.timedelta(milliseconds=0)),
            brokers=(broker,),
            cluster_id="test-cluster",
            controller_id=i32(0),
            topics=tuple(topics),
        )

        if api_version == 0:
            _broker = metadata_v0.response.MetadataResponseBroker(
                node_id=broker.node_id,
                host=broker.host,
                port=broker.port,
            )
            _topics = []
            for topic in topics:
                _partition_metadata = []
                for partition in topic.partitions:
                    _partition = metadata_v0.response.MetadataResponsePartition(
                        error_code=partition.error_code,
                        partition_index=partition.partition_index,
                        leader_id=partition.leader_id,
                        replica_nodes=partition.replica_nodes,
                        isr_nodes=partition.isr_nodes,
                    )
                    _partition_metadata.append(_partition)
                _topic = metadata_v0.response.MetadataResponseTopic(
                    error_code=topic.error_code,
                    name=topic.name,
                    partitions=tuple(_partition_metadata),
                )
                _topics.append(_topic)
            _response = metadata_v0.response.MetadataResponse(
                brokers=(_broker,), topics=tuple(_topics)
            )
            await callback(_response)

        elif api_version == 1:
            _broker = metadata_v1.response.MetadataResponseBroker(
                node_id=broker.node_id,
                host=broker.host,
                port=broker.port,
                rack=broker.rack,
            )
            _topics = []
            for topic in topics:
                _partition_metadata = []
                for partition in topic.partitions:
                    _partition = metadata_v1.response.MetadataResponsePartition(
                        error_code=partition.error_code,
                        partition_index=partition.partition_index,
                        leader_id=partition.leader_id,
                        replica_nodes=partition.replica_nodes,
                        isr_nodes=partition.isr_nodes,
                    )
                    _partition_metadata.append(_partition)
                _topic = metadata_v1.response.MetadataResponseTopic(
                    error_code=topic.error_code,
                    name=topic.name,
                    is_internal=topic.is_internal,
                    partitions=tuple(_partition_metadata),
                )
                _topics.append(_topic)
            _response = metadata_v1.response.MetadataResponse(
                brokers=(_broker,),
                topics=tuple(_topics),
                controller_id=BrokerId(_broker.node_id),
            )
            await callback(_response)

        elif api_version == 2:
            _broker = metadata_v2.response.MetadataResponseBroker(
                node_id=broker.node_id,
                host=broker.host,
                port=broker.port,
                rack=broker.rack,
            )
            _topics = []
            for topic in topics:
                _partition_metadata = []
                for partition in topic.partitions:
                    _partition = metadata_v2.response.MetadataResponsePartition(
                        error_code=partition.error_code,
                        partition_index=partition.partition_index,
                        leader_id=partition.leader_id,
                        replica_nodes=partition.replica_nodes,
                        isr_nodes=partition.isr_nodes,
                    )
                    _partition_metadata.append(_partition)
                _topic = metadata_v2.response.MetadataResponseTopic(
                    error_code=topic.error_code,
                    name=topic.name,
                    is_internal=topic.is_internal,
                    partitions=tuple(_partition_metadata),
                )
                _topics.append(_topic)
            _response = metadata_v2.response.MetadataResponse(
                brokers=(_broker,),
                topics=tuple(_topics),
                controller_id=BrokerId(_broker.node_id),
                cluster_id=response.cluster_id,
            )
            await callback(_response)

        elif api_version == 3:
            _broker = metadata_v3.response.MetadataResponseBroker(
                node_id=broker.node_id,
                host=broker.host,
                port=broker.port,
                rack=broker.rack,
            )
            _topics = []
            for topic in topics:
                _partition_metadata = []
                for partition in topic.partitions:
                    _partition = metadata_v3.response.MetadataResponsePartition(
                        error_code=partition.error_code,
                        partition_index=partition.partition_index,
                        leader_id=partition.leader_id,
                        replica_nodes=partition.replica_nodes,
                        isr_nodes=partition.isr_nodes,
                    )
                    _partition_metadata.append(_partition)
                _topic = metadata_v3.response.MetadataResponseTopic(
                    error_code=topic.error_code,
                    name=topic.name,
                    is_internal=topic.is_internal,
                    partitions=tuple(_partition_metadata),
                )
                _topics.append(_topic)
            _response = metadata_v3.response.MetadataResponse(
                brokers=(_broker,),
                topics=tuple(_topics),
                controller_id=BrokerId(_broker.node_id),
                cluster_id=response.cluster_id,
                throttle_time=response.throttle_time,
            )
            await callback(_response)

        elif api_version == 4:
            _broker = metadata_v4.response.MetadataResponseBroker(
                node_id=broker.node_id,
                host=broker.host,
                port=broker.port,
                rack=broker.rack,
            )
            _topics = []
            for topic in topics:
                _partition_metadata = []
                for partition in topic.partitions:
                    _partition = metadata_v4.response.MetadataResponsePartition(
                        error_code=partition.error_code,
                        partition_index=partition.partition_index,
                        leader_id=partition.leader_id,
                        replica_nodes=partition.replica_nodes,
                        isr_nodes=partition.isr_nodes,
                    )
                    _partition_metadata.append(_partition)
                _topic = metadata_v4.response.MetadataResponseTopic(
                    error_code=topic.error_code,
                    name=topic.name,
                    is_internal=topic.is_internal,
                    partitions=tuple(_partition_metadata),
                )
                _topics.append(_topic)
            _response = metadata_v4.response.MetadataResponse(
                brokers=(_broker,),
                topics=tuple(_topics),
                controller_id=BrokerId(_broker.node_id),
                cluster_id=response.cluster_id,
                throttle_time=response.throttle_time,
            )
            await callback(_response)

        elif api_version == 5:
            _broker = metadata_v5.response.MetadataResponseBroker(
                node_id=broker.node_id,
                host=broker.host,
                port=broker.port,
                rack=broker.rack,
            )
            _topics = []
            for topic in topics:
                _partition_metadata = []
                for partition in topic.partitions:
                    _partition = metadata_v5.response.MetadataResponsePartition(
                        error_code=partition.error_code,
                        partition_index=partition.partition_index,
                        leader_id=partition.leader_id,
                        replica_nodes=partition.replica_nodes,
                        isr_nodes=partition.isr_nodes,
                        offline_replicas=partition.offline_replicas,
                    )
                    _partition_metadata.append(_partition)
                _topic = metadata_v5.response.MetadataResponseTopic(
                    error_code=topic.error_code,
                    name=topic.name,
                    is_internal=topic.is_internal,
                    partitions=tuple(_partition_metadata),
                )
                _topics.append(_topic)
            _response = metadata_v5.response.MetadataResponse(
                brokers=(_broker,),
                topics=tuple(_topics),
                controller_id=BrokerId(_broker.node_id),
                cluster_id=response.cluster_id,
                throttle_time=response.throttle_time,
            )
            await callback(_response)

        elif api_version == 6:
            await callback(response)

        else:
            # unsupported - should be an error response
            pass

    def metadata_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: MetadataRequest,
        api_version: int,
    ) -> MetadataResponse:
        # there's no point in returning brokers because there's no error code
        # similarly there's no point in populating the topic partitions
        # just populate the topics with the name and the error code
        # because the typing is weird, the req and api_version might not match
        mod = load_payload_module(3, api_version, EntityType.response)
        response_topic_class = mod.MetadataResponseTopic
        response_class = mod.MetadataResponse
        _topics = []
        for topic in req.topics:
            _topic = response_topic_class(name=topic.name, error_code=error_code)
            _topics.append(_topic)

        return response_class(brokers=(), topics=tuple(_topics))

    async def handle_api_versions_request(
        self,
        header: ApiVersionsRequestHeader,
        req: ApiVersionsRequest,
        api_version: int,
        callback: Callable[[ApiVersionsResponse], Any],
    ):
        versions = tuple(
            api_v4.response.ApiVersion(
                api_key=i16(api_key), min_version=i16(min_ver), max_version=i16(max_ver)
            )
            for api_key, (min_ver, max_ver) in api_compatibility.items()
        )

        response = api_v4.response.ApiVersionsResponse(
            error_code=ErrorCode.none,
            api_keys=versions,
            throttle_time=i32Timedelta.parse(datetime.timedelta(milliseconds=0)),
            supported_features=(),
            finalized_features_epoch=i64(-1),
            finalized_features=(),
            zk_migration_ready=False,
        )
        if api_version == 0:
            _versions = tuple(
                api_v0.response.ApiVersion(
                    api_key=_version.api_key,
                    min_version=_version.min_version,
                    max_version=_version.max_version,
                )
                for _version in response.api_keys
            )
            _response = api_v0.response.ApiVersionsResponse(
                error_code=response.error_code, api_keys=_versions
            )
            await callback(_response)
        elif api_version == 1:
            _versions = tuple(
                api_v1.response.ApiVersion(
                    api_key=_version.api_key,
                    min_version=_version.min_version,
                    max_version=_version.max_version,
                )
                for _version in response.api_keys
            )
            _response = api_v1.response.ApiVersionsResponse(
                error_code=response.error_code,
                api_keys=_versions,
                throttle_time=response.throttle_time,
            )
            await callback(_response)
        elif api_version == 2:
            _versions = tuple(
                api_v2.response.ApiVersion(
                    api_key=_version.api_key,
                    min_version=_version.min_version,
                    max_version=_version.max_version,
                )
                for _version in response.api_keys
            )
            _response = api_v2.response.ApiVersionsResponse(
                error_code=response.error_code,
                api_keys=_versions,
                throttle_time=response.throttle_time,
            )
            await callback(_response)
        elif api_version == 3:
            _versions = tuple(
                api_v3.response.ApiVersion(
                    api_key=_version.api_key,
                    min_version=_version.min_version,
                    max_version=_version.max_version,
                )
                for _version in response.api_keys
            )
            _response = api_v3.response.ApiVersionsResponse(
                error_code=response.error_code,
                api_keys=_versions,
                throttle_time=response.throttle_time,
                supported_features=response.supported_features,
                finalized_features_epoch=response.finalized_features_epoch,
                finalized_features=response.finalized_features,
                zk_migration_ready=response.zk_migration_ready,
            )
            await callback(_response)
        elif api_version == 4:
            await callback(response)
        else:
            log.error(f"unsupported api versions version: {api_version}")

    def api_versions_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: ApiVersionsRequest,
        api_version: int,
    ) -> ApiVersionsResponse:
        mod = load_payload_module(18, api_version, EntityType.response)
        return mod.ApiVersionsResponse(
            error_code=error_code,
            api_keys=(),
        )

    async def handle_create_topics_request(
        self,
        header: CreateTopicsRequestHeader,
        req: CreateTopicsRequest,
        api_version: int,
        callback: Callable[[CreateTopicsResponse], Any],
    ):
        results = []

        for topic in req.topics:
            log.info(
                "create_topic", topic=topic.name, num_partitions=topic.num_partitions
            )

            result = CreatableTopicResult(
                name=topic.name,
                topic_id=uuid.uuid4(),
                error_code=ErrorCode.none,
                error_message=None,
                topic_config_error_code=i16(0),
                num_partitions=i32(topic.num_partitions),
                replication_factor=i16(topic.replication_factor),
                configs=None,  # configs not returned
            )
            results.append(result)

        response = CreateTopicsResponse(
            throttle_time=i32Timedelta.parse(datetime.timedelta(milliseconds=0)),
            topics=tuple(results),
        )
        await callback(response)

    def create_topics_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: CreateTopicsRequest,
        api_version: int,
    ) -> CreateTopicsResponse:
        results = []

        for topic in req.topics:
            result = CreatableTopicResult(
                name=topic.name,
                topic_id=None,
                error_code=error_code,
                error_message=error_message,
                topic_config_error_code=i16(0),
                num_partitions=i32(-1),
                replication_factor=i16(-1),
                configs=None,
            )
            results.append(result)

        return CreateTopicsResponse(
            throttle_time=i32Timedelta.parse(datetime.timedelta(milliseconds=0)),
            topics=tuple(results),
        )
