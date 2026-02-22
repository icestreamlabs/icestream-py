import asyncio
import datetime
from asyncio import Future
from typing import Any, Callable

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
from kio.static.constants import EntityType
from kio.static.primitive import i32, i32Timedelta, i64
from sqlalchemy import update

from icestream.config import Config
from icestream.kafkaserver.protocol import KafkaRecordBatch
from icestream.kafkaserver.topic_backends import topic_backend_for_name
from icestream.kafkaserver.types import ProduceTopicPartitionData
from icestream.models import Partition

from kio.schema.produce.v0.request import (
    ProduceRequest as ProduceRequestV0,
)
from kio.schema.produce.v0.request import (
    RequestHeader as ProduceRequestHeaderV0,
)
from kio.schema.produce.v0.response import (
    ProduceResponse as ProduceResponseV0,
)
from kio.schema.produce.v0.response import (
    ResponseHeader as ProduceResponseHeaderV0,
)
from kio.schema.produce.v1.request import (
    ProduceRequest as ProduceRequestV1,
)
from kio.schema.produce.v1.request import (
    RequestHeader as ProduceRequestHeaderV1,
)
from kio.schema.produce.v1.response import (
    ProduceResponse as ProduceResponseV1,
)
from kio.schema.produce.v1.response import (
    ResponseHeader as ProduceResponseHeaderV1,
)
from kio.schema.produce.v2.request import (
    ProduceRequest as ProduceRequestV2,
)
from kio.schema.produce.v2.request import (
    RequestHeader as ProduceRequestHeaderV2,
)
from kio.schema.produce.v2.response import (
    ProduceResponse as ProduceResponseV2,
)
from kio.schema.produce.v2.response import (
    ResponseHeader as ProduceResponseHeaderV2,
)
from kio.schema.produce.v3.request import (
    ProduceRequest as ProduceRequestV3,
)
from kio.schema.produce.v3.request import (
    RequestHeader as ProduceRequestHeaderV3,
)
from kio.schema.produce.v3.response import (
    ProduceResponse as ProduceResponseV3,
)
from kio.schema.produce.v3.response import (
    ResponseHeader as ProduceResponseHeaderV3,
)
from kio.schema.produce.v4.request import (
    ProduceRequest as ProduceRequestV4,
)
from kio.schema.produce.v4.request import (
    RequestHeader as ProduceRequestHeaderV4,
)
from kio.schema.produce.v4.response import (
    ProduceResponse as ProduceResponseV4,
)
from kio.schema.produce.v4.response import (
    ResponseHeader as ProduceResponseHeaderV4,
)
from kio.schema.produce.v5.request import (
    ProduceRequest as ProduceRequestV5,
)
from kio.schema.produce.v5.request import (
    RequestHeader as ProduceRequestHeaderV5,
)
from kio.schema.produce.v5.response import (
    ProduceResponse as ProduceResponseV5,
)
from kio.schema.produce.v5.response import (
    ResponseHeader as ProduceResponseHeaderV5,
)
from kio.schema.produce.v6.request import (
    ProduceRequest as ProduceRequestV6,
)
from kio.schema.produce.v6.request import (
    RequestHeader as ProduceRequestHeaderV6,
)
from kio.schema.produce.v6.response import (
    ProduceResponse as ProduceResponseV6,
)
from kio.schema.produce.v6.response import (
    ResponseHeader as ProduceResponseHeaderV6,
)
from kio.schema.produce.v7.request import (
    ProduceRequest as ProduceRequestV7,
)
from kio.schema.produce.v7.request import (
    RequestHeader as ProduceRequestHeaderV7,
)
from kio.schema.produce.v7.response import (
    ProduceResponse as ProduceResponseV7,
)
from kio.schema.produce.v7.response import (
    ResponseHeader as ProduceResponseHeaderV7,
)
from kio.schema.produce.v8.request import (
    ProduceRequest as ProduceRequestV8,
)
from kio.schema.produce.v8.request import (
    RequestHeader as ProduceRequestHeaderV8,
)
from kio.schema.produce.v8.response import (
    ProduceResponse as ProduceResponseV8,
)
from kio.schema.produce.v8.response import (
    ResponseHeader as ProduceResponseHeaderV8,
)

ProduceRequestHeader = (
    ProduceRequestHeaderV0
    | ProduceRequestHeaderV1
    | ProduceRequestHeaderV2
    | ProduceRequestHeaderV3
    | ProduceRequestHeaderV4
    | ProduceRequestHeaderV5
    | ProduceRequestHeaderV6
    | ProduceRequestHeaderV7
    | ProduceRequestHeaderV8
)

ProduceResponseHeader = (
    ProduceResponseHeaderV0
    | ProduceResponseHeaderV1
    | ProduceResponseHeaderV2
    | ProduceResponseHeaderV3
    | ProduceResponseHeaderV4
    | ProduceResponseHeaderV5
    | ProduceResponseHeaderV6
    | ProduceResponseHeaderV7
    | ProduceResponseHeaderV8
)

ProduceRequest = (
    ProduceRequestV0
    | ProduceRequestV1
    | ProduceRequestV2
    | ProduceRequestV3
    | ProduceRequestV4
    | ProduceRequestV5
    | ProduceRequestV6
    | ProduceRequestV7
    | ProduceRequestV8
)

ProduceResponse = (
    ProduceResponseV0
    | ProduceResponseV1
    | ProduceResponseV2
    | ProduceResponseV3
    | ProduceResponseV4
    | ProduceResponseV5
    | ProduceResponseV6
    | ProduceResponseV7
    | ProduceResponseV8
)

log = structlog.get_logger()


async def do_handle_produce_request(
    config: Config,
    produce_queue: asyncio.Queue[ProduceTopicPartitionData],
    req: ProduceRequest,
    api_version: int,
    callback: Callable[[ProduceResponse], Any],
) -> None:
    log.info("handling produce request", request=req)
    topic_responses: list[produce_v8.response.TopicProduceResponse] = []

    for topic in req.topic_data:
        topic_name = topic.name
        partition_responses: list[produce_v8.response.PartitionProduceResponse] = []

        if topic_backend_for_name(topic_name).is_internal:
            for partition in topic.partition_data:
                partition_responses.append(
                    produce_v8.response.PartitionProduceResponse(
                        index=i32(partition.index),
                        error_code=ErrorCode.topic_authorization_failed,
                        base_offset=i64(-1),
                        log_append_time=None,
                        log_start_offset=i64(-1),
                        record_errors=(),
                        error_message="cannot produce to internal topic",
                    )
                )
            topic_responses.append(
                produce_v8.response.TopicProduceResponse(
                    name=topic_name,
                    partition_responses=tuple(partition_responses),
                )
            )
            continue

        for partition in topic.partition_data:
            partition_idx = partition.index
            records = partition.records
            error_code: ErrorCode | None = None
            record_count = 0
            parsed_batch: KafkaRecordBatch | None = None

            try:
                if records is not None:
                    parsed_batch = KafkaRecordBatch.from_bytes(records)
                    record_count = parsed_batch.records_count
            except Exception:
                log.exception(
                    "failed to parse produce record batch",
                    extra={"topic": topic_name, "partition": partition_idx},
                )
                error_code = ErrorCode.invalid_record

            if parsed_batch is None and records is not None and error_code is None:
                error_code = ErrorCode.invalid_record

            if parsed_batch is not None and parsed_batch.magic != 2:
                error_code = ErrorCode.unsupported_for_message_format
                partition_response = produce_v8.response.PartitionProduceResponse(
                    index=i32(partition_idx),
                    error_code=error_code,
                    base_offset=i64(-1),
                    log_append_time=None,
                    log_start_offset=i64(-1),
                    record_errors=(),
                    error_message="wrong magic number",
                )
                partition_responses.append(partition_response)
                continue

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
                    base_offset=i64(-1),
                    log_append_time=None,
                    log_start_offset=i64(-1),
                    record_errors=(),
                    error_message=None,
                )
                partition_responses.append(partition_response)
                continue

            assert config.async_session_factory is not None
            async with config.async_session_factory() as session:
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
            if row is None:
                error_code = ErrorCode.unknown_topic_or_partition
                partition_response = produce_v8.response.PartitionProduceResponse(
                    index=i32(partition_idx),
                    error_code=ErrorCode.none if error_code is None else error_code,
                    base_offset=i64(-1),
                    log_append_time=None,
                    log_start_offset=i64(-1),
                    record_errors=(),
                    error_message="unknown topic or partition",
                )
                partition_responses.append(partition_response)
                continue
            _, last_offset, log_start_offset = row
            first_offset = last_offset - record_count + 1

            # Persist broker-assigned offsets in WAL so compaction/read paths can
            # reason over absolute partition offsets across files.
            assert parsed_batch is not None
            parsed_batch.base_offset = int(first_offset)

            partition_flush_result_fut = Future()
            produce_topic_partition_data = ProduceTopicPartitionData(
                topic=topic_name,
                partition=partition_idx,
                kafka_record_batch=parsed_batch,
                flush_result=partition_flush_result_fut,
            )
            await produce_queue.put(produce_topic_partition_data)

            try:
                await asyncio.wait_for(
                    partition_flush_result_fut,
                    timeout=config.FLUSH_INTERVAL * 2,
                )
            except asyncio.CancelledError:
                error_code = ErrorCode.unknown_server_error
            except asyncio.TimeoutError:
                error_code = ErrorCode.request_timed_out
            except Exception:
                error_code = ErrorCode.unknown_server_error
            finally:
                partition_response = produce_v8.response.PartitionProduceResponse(
                    index=i32(partition_idx),
                    error_code=ErrorCode.none if error_code is None else error_code,
                    base_offset=i64(first_offset if error_code is None else -1),
                    log_append_time=None,
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
        log.error("unsupported produce version", api_version=api_version)


def produce_error_response(
    req: ProduceRequest,
    api_version: int,
    *,
    error_code: ErrorCode,
    error_message: str,
) -> ProduceResponse:
    mod = load_payload_module(0, api_version, EntityType.response)
    responses = []
    for topic_data in req.topic_data:
        partition_response = []
        for partition_data in topic_data.partition_data:
            partition_produce_response = mod.PartitionProduceResponse(
                index=partition_data.index,
                error_code=error_code,
                base_offset=i64(0),
                record_errors=(),
            )
            partition_response.append(partition_produce_response)

        topic_produce_response = mod.TopicProduceResponse(
            name=topic_data.name, partition_responses=tuple(partition_response)
        )
        responses.append(topic_produce_response)
    _ = error_message
    return mod.ProduceResponse(responses=tuple(responses))
