import asyncio
import datetime
import io
import struct
import uuid
from asyncio import Server as AsyncIOServer, StreamReader, StreamWriter
from collections import defaultdict
from typing import Callable, Any, List

import kio.schema.metadata.v0 as metadata_v0
import kio.schema.metadata.v1 as metadata_v1
import kio.schema.metadata.v2 as metadata_v2
import kio.schema.metadata.v3 as metadata_v3
import kio.schema.metadata.v4 as metadata_v4
import kio.schema.metadata.v5 as metadata_v5
import kio.schema.metadata.v6 as metadata_v6
from kio.index import load_payload_module

from kio.schema.errors import ErrorCode
from kio.schema.types import BrokerId, TopicName
from kio.serial import entity_reader
from kio.serial.readers import read_int32
from kio.static.constants import EntityType
from kio.static.primitive import i64, i32, i32Timedelta, i16
from kio.static.protocol import RequestHeader

import structlog

from kafkaserver.handler import handle_kafka_request, api_compatibility
from kafkaserver.handlers import KafkaHandler
from kafkaserver.handlers.metadata import (
    MetadataRequestHeader,
    MetadataRequest,
    MetadataResponse,
)
from kafkaserver.messages import (
    ProduceRequestHeader,
    ProduceRequest,
    ProduceResponse,
    PartitionProduceResponse,
    TopicProduceResponse,
    # MetadataRequestHeader,
    # MetadataRequest,
    # MetadataResponse,
    # MetadataResponsePartition,
    # MetadataResponseBroker,
    # MetadataResponseTopic,
    ApiVersionsRequestHeader,
    ApiVersionsRequest,
    ApiVersionsResponse,
    ApiVersion,
    CreateTopicsRequestHeader,
    CreateTopicsRequest,
    CreateTopicsResponse,
    CreatableTopicResult,
)
from kafkaserver.metadata import MetadataProvider

log = structlog.get_logger()


class Server:
    def __init__(self):
        self.listener: AsyncIOServer | None = None
        self.metadata_provider = MetadataProvider()

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
        self.offsets: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))

    async def __call__(self, reader: StreamReader, writer: StreamWriter) -> None:
        try:
            while not reader.at_eof():
                msg_length_bytes = await reader.readexactly(4)
                msg_length = read_int32(io.BytesIO(msg_length_bytes))
                message = await reader.readexactly(msg_length)
                api_key = struct.unpack(">H", message[:2])[0]
                await handle_kafka_request(api_key, message, self, writer)

        except Exception as e:
            log.error(f"error handling connection {e}")

    async def handle_produce_request(
        self,
        header: ProduceRequestHeader,
        req: ProduceRequest,
        api_version: int,
        callback: Callable[[ProduceResponse], Any],
    ):
        responses = []
        for topic in req.topic_data:
            topic_name = topic.name
            partition_responses = []
            for partition in topic.partition_data:
                idx = partition.index
                curr_offset = self.offsets[topic_name][idx]
                record_count = len(partition.records)
                log.info(
                    "produce",
                    topic=topic_name,
                    partition=idx,
                    offset=curr_offset,
                    num_records=record_count,
                )
                self.offsets[topic_name][idx] += record_count

                partition_response = PartitionProduceResponse(
                    index=idx,
                    error_code=ErrorCode.none,
                    error_message=None,
                    base_offset=i64(curr_offset),
                    record_errors=(),
                )
                partition_responses.append(partition_response)

            topic_response = TopicProduceResponse(
                name=topic_name, partition_responses=tuple(partition_responses)
            )
            responses.append(topic_response)

        await callback(ProduceResponse(responses=tuple(responses)))

    def produce_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: ProduceRequest,
        api_version: int,
    ) -> ProduceResponse:
        responses = []
        for i, topic_data in enumerate(req.topic_data):
            partition_response = []
            for j, partition_data in enumerate(topic_data.partition_data):
                partition_produce_response = PartitionProduceResponse(
                    index=partition_data.index,
                    error_code=error_code,
                    error_message=error_message,
                    base_offset=i64(0),
                    record_errors=(),
                )
                partition_response.append(partition_produce_response)

            topic_produce_response = TopicProduceResponse(
                name=topic_data.name, partition_responses=tuple(partition_response)
            )
            responses.append(topic_produce_response)
        resp = ProduceResponse(responses=tuple(responses))
        return resp

    async def handle_metadata_request(
        self,
        header: MetadataRequestHeader,
        req: MetadataRequest,
        api_version: int,
        callback: Callable[[MetadataResponse], Any],
    ):
        log.info("handling metadata request", topics=[t.name for t in req.topics])

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
        # currently we're just storing stuff in a dictionary in memory

        topics: List[metadata_v6.response.MetadataResponseTopic] = []
        for topic_name, partitions in self.offsets.items():
            partition_metadata = [
                metadata_v6.response.MetadataResponsePartition(
                    error_code=ErrorCode.none,
                    partition_index=i32(pidx),
                    leader_id=i32(0),
                    replica_nodes=(i32(0),),
                    isr_nodes=(i32(0),),
                    offline_replicas=(),
                )
                for pidx in partitions
            ]
            topics.append(
                metadata_v6.response.MetadataResponseTopic(
                    error_code=ErrorCode.none,
                    name=TopicName(topic_name),
                    is_internal=False,
                    partitions=tuple(partition_metadata),
                )
            )

        response = metadata_v6.response.MetadataResponse(
            throttle_time=i32Timedelta.parse(datetime.timedelta(milliseconds=1000)),
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
            ApiVersion(
                api_key=i16(api_key), min_version=i16(min_ver), max_version=i16(max_ver)
            )
            for api_key, (min_ver, max_ver) in api_compatibility.items()
        )

        response = ApiVersionsResponse(
            error_code=ErrorCode.none,
            api_keys=versions,
            throttle_time=i32Timedelta(0),
            supported_features=(),
            finalized_features_epoch=i64(-1),
            finalized_features=(),
            zk_migration_ready=False,
        )

        await callback(response)

    def api_versions_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: ApiVersionsRequest,
        api_version: int,
    ) -> ApiVersionsResponse:
        return ApiVersionsResponse(
            error_code=error_code,
            api_keys=(),
            throttle_time=i32Timedelta(0),
            supported_features=(),
            finalized_features_epoch=i64(-1),
            finalized_features=(),
            zk_migration_ready=False,
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
            throttle_time=i32Timedelta(0), topics=tuple(results)
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
            throttle_time=i32Timedelta(0), topics=tuple(results)
        )
