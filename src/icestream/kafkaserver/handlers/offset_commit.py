from kio.schema.offset_commit.v0.request import (
    OffsetCommitRequest as OffsetCommitRequestV0,
)
from kio.schema.offset_commit.v0.request import (
    RequestHeader as OffsetCommitRequestHeaderV0,
)
from kio.schema.offset_commit.v0.response import (
    OffsetCommitResponse as OffsetCommitResponseV0,
)
from kio.schema.offset_commit.v0.response import (
    ResponseHeader as OffsetCommitResponseHeaderV0,
)
from kio.schema.offset_commit.v1.request import (
    OffsetCommitRequest as OffsetCommitRequestV1,
)
from kio.schema.offset_commit.v1.request import (
    RequestHeader as OffsetCommitRequestHeaderV1,
)
from kio.schema.offset_commit.v1.response import (
    OffsetCommitResponse as OffsetCommitResponseV1,
)
from kio.schema.offset_commit.v1.response import (
    ResponseHeader as OffsetCommitResponseHeaderV1,
)
from kio.schema.offset_commit.v2.request import (
    OffsetCommitRequest as OffsetCommitRequestV2,
)
from kio.schema.offset_commit.v2.request import (
    RequestHeader as OffsetCommitRequestHeaderV2,
)
from kio.schema.offset_commit.v2.response import (
    OffsetCommitResponse as OffsetCommitResponseV2,
)
from kio.schema.offset_commit.v2.response import (
    ResponseHeader as OffsetCommitResponseHeaderV2,
)
from kio.schema.offset_commit.v3.request import (
    OffsetCommitRequest as OffsetCommitRequestV3,
)
from kio.schema.offset_commit.v3.request import (
    RequestHeader as OffsetCommitRequestHeaderV3,
)
from kio.schema.offset_commit.v3.response import (
    OffsetCommitResponse as OffsetCommitResponseV3,
)
from kio.schema.offset_commit.v3.response import (
    ResponseHeader as OffsetCommitResponseHeaderV3,
)
from kio.schema.offset_commit.v4.request import (
    OffsetCommitRequest as OffsetCommitRequestV4,
)
from kio.schema.offset_commit.v4.request import (
    RequestHeader as OffsetCommitRequestHeaderV4,
)
from kio.schema.offset_commit.v4.response import (
    OffsetCommitResponse as OffsetCommitResponseV4,
)
from kio.schema.offset_commit.v4.response import (
    ResponseHeader as OffsetCommitResponseHeaderV4,
)
from kio.schema.offset_commit.v5.request import (
    OffsetCommitRequest as OffsetCommitRequestV5,
)
from kio.schema.offset_commit.v5.request import (
    RequestHeader as OffsetCommitRequestHeaderV5,
)
from kio.schema.offset_commit.v5.response import (
    OffsetCommitResponse as OffsetCommitResponseV5,
)
from kio.schema.offset_commit.v5.response import (
    ResponseHeader as OffsetCommitResponseHeaderV5,
)
from kio.schema.offset_commit.v6.request import (
    OffsetCommitRequest as OffsetCommitRequestV6,
)
from kio.schema.offset_commit.v6.request import (
    RequestHeader as OffsetCommitRequestHeaderV6,
)
from kio.schema.offset_commit.v6.response import (
    OffsetCommitResponse as OffsetCommitResponseV6,
)
from kio.schema.offset_commit.v6.response import (
    ResponseHeader as OffsetCommitResponseHeaderV6,
)
from kio.schema.offset_commit.v7.request import (
    OffsetCommitRequest as OffsetCommitRequestV7,
)
from kio.schema.offset_commit.v7.request import (
    RequestHeader as OffsetCommitRequestHeaderV7,
)
from kio.schema.offset_commit.v7.response import (
    OffsetCommitResponse as OffsetCommitResponseV7,
)
from kio.schema.offset_commit.v7.response import (
    ResponseHeader as OffsetCommitResponseHeaderV7,
)
from kio.schema.offset_commit.v8.request import (
    OffsetCommitRequest as OffsetCommitRequestV8,
)
from kio.schema.offset_commit.v8.request import (
    RequestHeader as OffsetCommitRequestHeaderV8,
)
from kio.schema.offset_commit.v8.response import (
    OffsetCommitResponse as OffsetCommitResponseV8,
)
from kio.schema.offset_commit.v8.response import (
    ResponseHeader as OffsetCommitResponseHeaderV8,
)
from kio.schema.offset_commit.v9.request import (
    OffsetCommitRequest as OffsetCommitRequestV9,
)
from kio.schema.offset_commit.v9.request import (
    RequestHeader as OffsetCommitRequestHeaderV9,
)
from kio.schema.offset_commit.v9.response import (
    OffsetCommitResponse as OffsetCommitResponseV9,
)
from kio.schema.offset_commit.v9.response import (
    ResponseHeader as OffsetCommitResponseHeaderV9,
)
from kio.schema.errors import ErrorCode

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from icestream.config import Config
from icestream.kafkaserver.consumer_group_liveness import (
    expire_dead_members,
    reset_after_sync_timeout,
    utc_now,
)
from icestream.kafkaserver.handlers.offset_response import (
    build_offset_response,
    build_partition_response,
    build_topic_response,
    response_sequence_element,
)
from icestream.kafkaserver.internal_offsets import (
    OffsetLogEntry,
    append_offset_log_entries,
    commit_timestamp_ms,
    encode_offset_key,
    encode_offset_value,
)
from icestream.models.consumer_groups import ConsumerGroup, GroupMember, GroupOffset


OffsetCommitRequestHeader = (
    OffsetCommitRequestHeaderV0
    | OffsetCommitRequestHeaderV1
    | OffsetCommitRequestHeaderV2
    | OffsetCommitRequestHeaderV3
    | OffsetCommitRequestHeaderV4
    | OffsetCommitRequestHeaderV5
    | OffsetCommitRequestHeaderV6
    | OffsetCommitRequestHeaderV7
    | OffsetCommitRequestHeaderV8
    | OffsetCommitRequestHeaderV9
)

OffsetCommitResponseHeader = (
    OffsetCommitResponseHeaderV0
    | OffsetCommitResponseHeaderV1
    | OffsetCommitResponseHeaderV2
    | OffsetCommitResponseHeaderV3
    | OffsetCommitResponseHeaderV4
    | OffsetCommitResponseHeaderV5
    | OffsetCommitResponseHeaderV6
    | OffsetCommitResponseHeaderV7
    | OffsetCommitResponseHeaderV8
    | OffsetCommitResponseHeaderV9
)

OffsetCommitRequest = (
    OffsetCommitRequestV0
    | OffsetCommitRequestV1
    | OffsetCommitRequestV2
    | OffsetCommitRequestV3
    | OffsetCommitRequestV4
    | OffsetCommitRequestV5
    | OffsetCommitRequestV6
    | OffsetCommitRequestV7
    | OffsetCommitRequestV8
    | OffsetCommitRequestV9
)

OffsetCommitResponse = (
    OffsetCommitResponseV0
    | OffsetCommitResponseV1
    | OffsetCommitResponseV2
    | OffsetCommitResponseV3
    | OffsetCommitResponseV4
    | OffsetCommitResponseV5
    | OffsetCommitResponseV6
    | OffsetCommitResponseV7
    | OffsetCommitResponseV8
    | OffsetCommitResponseV9
)


def _response_class(api_version: int):
    if api_version == 0:
        return OffsetCommitResponseV0
    if api_version == 1:
        return OffsetCommitResponseV1
    if api_version == 2:
        return OffsetCommitResponseV2
    if api_version == 3:
        return OffsetCommitResponseV3
    if api_version == 4:
        return OffsetCommitResponseV4
    if api_version == 5:
        return OffsetCommitResponseV5
    if api_version == 6:
        return OffsetCommitResponseV6
    if api_version == 7:
        return OffsetCommitResponseV7
    if api_version == 8:
        return OffsetCommitResponseV8
    if api_version == 9:
        return OffsetCommitResponseV9
    raise ValueError(f"unsupported offset_commit api version: {api_version}")


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


def _partition_offset(partition: object) -> int:
    for name in ("committed_offset", "offset"):
        if hasattr(partition, name):
            return int(getattr(partition, name))
    raise ValueError("committed offset not found in request")


def _partition_metadata(partition: object) -> str | None:
    for name in ("committed_metadata", "metadata"):
        if hasattr(partition, name):
            return getattr(partition, name)
    return None


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


def _request_generation(req: object) -> int | None:
    for name in ("generation_id", "generation"):
        if hasattr(req, name):
            return int(getattr(req, name))
    return None


def _request_member_id(req: object) -> str | None:
    for name in ("member_id", "member"):
        if hasattr(req, name):
            value = getattr(req, name)
            return None if value is None else str(value)
    return None


def _build_topic_responses(
    response_cls: type,
    request_topics: tuple,
    partition_errors: dict[tuple[str, int], ErrorCode],
) -> list:
    topic_field = response_sequence_element(response_cls, "topics")
    if topic_field is None:
        topic_field = response_sequence_element(response_cls, "responses")
    if topic_field is None:
        raise ValueError("offset_commit response topic element not found")

    partition_field = response_sequence_element(topic_field, "partitions")
    if partition_field is None:
        partition_field = response_sequence_element(topic_field, "partition_responses")
    if partition_field is None:
        raise ValueError("offset_commit response partition element not found")

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


async def do_offset_commit(
    config: Config,
    req: OffsetCommitRequest,
    api_version: int,
) -> OffsetCommitResponse:
    assert config.async_session_factory is not None

    group_id = _request_group_id(req)
    request_topics = tuple(_request_topics(req))
    response_cls = _response_class(api_version)

    if not request_topics:
        return build_offset_response(response_cls, topics=())

    req_generation = _request_generation(req)
    req_member_id = _request_member_id(req) or ""
    admin_commit = req_generation is None or req_generation < 0 or req_member_id == ""
    now = utc_now()

    partition_errors: dict[tuple[str, int], ErrorCode] = {}
    log_entries: list[OffsetLogEntry] = []

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

            if await reset_after_sync_timeout(session, group, now):
                for topic in request_topics:
                    topic_name = _topic_name(topic)
                    for partition in _topic_partitions(topic):
                        partition_errors[(topic_name, _partition_index(partition))] = (
                            ErrorCode.rebalance_in_progress
                        )
                topics = _build_topic_responses(response_cls, request_topics, partition_errors)
                return build_offset_response(
                    response_cls, topics=topics, error_code=ErrorCode.rebalance_in_progress
                )

            member: GroupMember | None = None
            if not admin_commit:
                members, _ = await expire_dead_members(session, group, now)
                member = next((m for m in members if m.member_id == req_member_id), None)
                if member is None:
                    error = ErrorCode.unknown_member_id
                    for topic in request_topics:
                        topic_name = _topic_name(topic)
                        for partition in _topic_partitions(topic):
                            partition_errors[(topic_name, _partition_index(partition))] = error
                    topics = _build_topic_responses(response_cls, request_topics, partition_errors)
                    return build_offset_response(response_cls, topics=topics, error_code=error)

                if req_generation != group.generation:
                    error = ErrorCode.illegal_generation
                    for topic in request_topics:
                        topic_name = _topic_name(topic)
                        for partition in _topic_partitions(topic):
                            partition_errors[(topic_name, _partition_index(partition))] = error
                    topics = _build_topic_responses(response_cls, request_topics, partition_errors)
                    return build_offset_response(response_cls, topics=topics, error_code=error)

                if member.member_generation != group.generation:
                    error = ErrorCode.illegal_generation
                    for topic in request_topics:
                        topic_name = _topic_name(topic)
                        for partition in _topic_partitions(topic):
                            partition_errors[(topic_name, _partition_index(partition))] = error
                    topics = _build_topic_responses(response_cls, request_topics, partition_errors)
                    return build_offset_response(response_cls, topics=topics, error_code=error)

                if group.state != "Stable":
                    error = ErrorCode.rebalance_in_progress
                    for topic in request_topics:
                        topic_name = _topic_name(topic)
                        for partition in _topic_partitions(topic):
                            partition_errors[(topic_name, _partition_index(partition))] = error
                    topics = _build_topic_responses(response_cls, request_topics, partition_errors)
                    return build_offset_response(response_cls, topics=topics, error_code=error)

            commit_ts = now
            commit_ts_ms = commit_timestamp_ms(commit_ts)
            committed_generation = group.generation
            committed_member_id = None if admin_commit else req_member_id

            for topic in request_topics:
                topic_name = _topic_name(topic)
                for partition in _topic_partitions(topic):
                    partition_id = _partition_index(partition)
                    committed_offset = _partition_offset(partition)
                    committed_metadata = _partition_metadata(partition)

                    stmt = insert(GroupOffset).values(
                        consumer_group_id=group.id,
                        topic_name=topic_name,
                        partition_number=partition_id,
                        committed_offset=committed_offset,
                        committed_metadata=committed_metadata,
                        commit_timestamp=commit_ts,
                        committed_generation=committed_generation,
                        committed_member_id=committed_member_id,
                    )
                    stmt = stmt.on_conflict_do_update(
                        index_elements=[
                            GroupOffset.consumer_group_id,
                            GroupOffset.topic_name,
                            GroupOffset.partition_number,
                        ],
                        set_={
                            "committed_offset": stmt.excluded.committed_offset,
                            "committed_metadata": stmt.excluded.committed_metadata,
                            "commit_timestamp": stmt.excluded.commit_timestamp,
                            "committed_generation": stmt.excluded.committed_generation,
                            "committed_member_id": stmt.excluded.committed_member_id,
                        },
                    )
                    await session.execute(stmt)

                    key_bytes = encode_offset_key(group_id, topic_name, partition_id)
                    value_bytes = encode_offset_value(
                        committed_offset=committed_offset,
                        committed_metadata=committed_metadata,
                        commit_timestamp_ms=commit_ts_ms,
                    )
                    log_entries.append(
                        OffsetLogEntry(key_bytes=key_bytes, value_bytes=value_bytes)
                    )

            await append_offset_log_entries(
                session,
                config=config,
                entries=log_entries,
                commit_ts=commit_ts,
            )

    topics = _build_topic_responses(response_cls, request_topics, partition_errors)
    return build_offset_response(response_cls, topics=topics, error_code=None)
