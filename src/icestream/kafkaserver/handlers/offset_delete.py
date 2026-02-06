from kio.schema.offset_delete.v0.request import (
    OffsetDeleteRequest as OffsetDeleteRequestV0,
)
from kio.schema.offset_delete.v0.request import (
    RequestHeader as OffsetDeleteRequestHeaderV0,
)
from kio.schema.offset_delete.v0.response import (
    OffsetDeleteResponse as OffsetDeleteResponseV0,
)
from kio.schema.offset_delete.v0.response import (
    ResponseHeader as OffsetDeleteResponseHeaderV0,
)
from kio.schema.errors import ErrorCode

from sqlalchemy import delete, select

from icestream.config import Config
from icestream.kafkaserver.handlers.offset_response import (
    build_offset_response,
    build_partition_response,
    build_topic_response,
    response_sequence_element,
)
from icestream.kafkaserver.internal_offsets import (
    OffsetLogEntry,
    append_offset_log_entries,
    encode_offset_key,
)
from icestream.kafkaserver.consumer_group_liveness import utc_now
from icestream.models.consumer_groups import ConsumerGroup, GroupOffset

OffsetDeleteRequestHeader = OffsetDeleteRequestHeaderV0

OffsetDeleteResponseHeader = OffsetDeleteResponseHeaderV0

OffsetDeleteRequest = OffsetDeleteRequestV0

OffsetDeleteResponse = OffsetDeleteResponseV0


def _topic_name(topic: object) -> str:
    for name in ("name", "topic", "topic_name"):
        if hasattr(topic, name):
            return str(getattr(topic, name))
    raise ValueError("topic name not found in request")


def _topic_partitions(topic: object):
    for name in ("partitions", "partition_data"):
        if hasattr(topic, name):
            return getattr(topic, name) or ()
    return ()


def _partition_index(partition: object) -> int:
    for name in ("partition_index", "partition", "partition_id", "partition_number"):
        if hasattr(partition, name):
            return int(getattr(partition, name))
    raise ValueError("partition index not found in request")


def _request_topics(req: object):
    for name in ("topics", "topic_data"):
        if hasattr(req, name):
            return getattr(req, name) or ()
    return ()


def _request_group_id(req: object) -> str:
    for name in ("group_id", "group"):
        if hasattr(req, name):
            return str(getattr(req, name))
    raise ValueError("group_id not found in request")


def _build_topic_responses(
    response_cls: type,
    request_topics: tuple,
    partition_errors: dict[tuple[str, int], ErrorCode],
) -> list:
    topic_field = response_sequence_element(response_cls, "topics")
    if topic_field is None:
        topic_field = response_sequence_element(response_cls, "responses")
    if topic_field is None:
        raise ValueError("offset_delete response topic element not found")

    partition_field = response_sequence_element(topic_field, "partitions")
    if partition_field is None:
        partition_field = response_sequence_element(topic_field, "partition_responses")
    if partition_field is None:
        raise ValueError("offset_delete response partition element not found")

    topics: list = []
    for topic in request_topics:
        topic_name = _topic_name(topic)
        partition_responses: list = []
        for partition in _topic_partitions(topic):
            partition_id = _partition_index(partition)
            error_code = partition_errors.get((topic_name, partition_id), ErrorCode.none)
            partition_responses.append(
                build_partition_response(
                    partition_field,
                    partition=partition_id,
                    error_code=error_code,
                )
            )
        topics.append(
            build_topic_response(
                topic_field,
                topic_name=topic_name,
                partitions=partition_responses,
            )
        )
    return topics


async def do_offset_delete(
    config: Config,
    req: OffsetDeleteRequest,
    api_version: int,
) -> OffsetDeleteResponse:
    assert config.async_session_factory is not None

    group_id = _request_group_id(req)
    request_topics = tuple(_request_topics(req))
    response_cls = OffsetDeleteResponseV0

    if not request_topics:
        return build_offset_response(response_cls, topics=())

    partition_errors: dict[tuple[str, int], ErrorCode] = {}
    log_entries: list[OffsetLogEntry] = []
    commit_ts = utc_now()

    async with config.async_session_factory() as session:
        async with session.begin():
            group = (
                await session.execute(
                    select(ConsumerGroup)
                    .where(ConsumerGroup.group_id == group_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()

            if group is None:
                for topic in request_topics:
                    topic_name = _topic_name(topic)
                    for partition in _topic_partitions(topic):
                        partition_errors[(topic_name, _partition_index(partition))] = (
                            ErrorCode.group_id_not_found
                        )
                topics = _build_topic_responses(response_cls, request_topics, partition_errors)
                return build_offset_response(
                    response_cls, topics=topics, error_code=ErrorCode.group_id_not_found
                )

            for topic in request_topics:
                topic_name = _topic_name(topic)
                for partition in _topic_partitions(topic):
                    partition_id = _partition_index(partition)
                    await session.execute(
                        delete(GroupOffset).where(
                            GroupOffset.consumer_group_id == group.id,
                            GroupOffset.topic_name == topic_name,
                            GroupOffset.partition_number == partition_id,
                        )
                    )

                    key_bytes = encode_offset_key(group_id, topic_name, partition_id)
                    log_entries.append(
                        OffsetLogEntry(key_bytes=key_bytes, value_bytes=None)
                    )

            await append_offset_log_entries(
                session,
                config=config,
                entries=log_entries,
                commit_ts=commit_ts,
            )

    topics = _build_topic_responses(response_cls, request_topics, partition_errors)
    return build_offset_response(response_cls, topics=topics, error_code=None)
