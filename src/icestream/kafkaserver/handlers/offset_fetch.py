from kio.schema.offset_fetch.v0.request import (
    OffsetFetchRequest as OffsetFetchRequestV0,
)
from kio.schema.offset_fetch.v0.request import (
    RequestHeader as OffsetFetchRequestHeaderV0,
)
from kio.schema.offset_fetch.v0.response import (
    OffsetFetchResponse as OffsetFetchResponseV0,
)
from kio.schema.offset_fetch.v0.response import (
    ResponseHeader as OffsetFetchResponseHeaderV0,
)
from kio.schema.offset_fetch.v1.request import (
    OffsetFetchRequest as OffsetFetchRequestV1,
)
from kio.schema.offset_fetch.v1.request import (
    RequestHeader as OffsetFetchRequestHeaderV1,
)
from kio.schema.offset_fetch.v1.response import (
    OffsetFetchResponse as OffsetFetchResponseV1,
)
from kio.schema.offset_fetch.v1.response import (
    ResponseHeader as OffsetFetchResponseHeaderV1,
)
from kio.schema.offset_fetch.v2.request import (
    OffsetFetchRequest as OffsetFetchRequestV2,
)
from kio.schema.offset_fetch.v2.request import (
    RequestHeader as OffsetFetchRequestHeaderV2,
)
from kio.schema.offset_fetch.v2.response import (
    OffsetFetchResponse as OffsetFetchResponseV2,
)
from kio.schema.offset_fetch.v2.response import (
    ResponseHeader as OffsetFetchResponseHeaderV2,
)
from kio.schema.offset_fetch.v3.request import (
    OffsetFetchRequest as OffsetFetchRequestV3,
)
from kio.schema.offset_fetch.v3.request import (
    RequestHeader as OffsetFetchRequestHeaderV3,
)
from kio.schema.offset_fetch.v3.response import (
    OffsetFetchResponse as OffsetFetchResponseV3,
)
from kio.schema.offset_fetch.v3.response import (
    ResponseHeader as OffsetFetchResponseHeaderV3,
)
from kio.schema.offset_fetch.v4.request import (
    OffsetFetchRequest as OffsetFetchRequestV4,
)
from kio.schema.offset_fetch.v4.request import (
    RequestHeader as OffsetFetchRequestHeaderV4,
)
from kio.schema.offset_fetch.v4.response import (
    OffsetFetchResponse as OffsetFetchResponseV4,
)
from kio.schema.offset_fetch.v4.response import (
    ResponseHeader as OffsetFetchResponseHeaderV4,
)
from kio.schema.offset_fetch.v5.request import (
    OffsetFetchRequest as OffsetFetchRequestV5,
)
from kio.schema.offset_fetch.v5.request import (
    RequestHeader as OffsetFetchRequestHeaderV5,
)
from kio.schema.offset_fetch.v5.response import (
    OffsetFetchResponse as OffsetFetchResponseV5,
)
from kio.schema.offset_fetch.v5.response import (
    ResponseHeader as OffsetFetchResponseHeaderV5,
)
from kio.schema.offset_fetch.v6.request import (
    OffsetFetchRequest as OffsetFetchRequestV6,
)
from kio.schema.offset_fetch.v6.request import (
    RequestHeader as OffsetFetchRequestHeaderV6,
)
from kio.schema.offset_fetch.v6.response import (
    OffsetFetchResponse as OffsetFetchResponseV6,
)
from kio.schema.offset_fetch.v6.response import (
    ResponseHeader as OffsetFetchResponseHeaderV6,
)
from kio.schema.offset_fetch.v7.request import (
    OffsetFetchRequest as OffsetFetchRequestV7,
)
from kio.schema.offset_fetch.v7.request import (
    RequestHeader as OffsetFetchRequestHeaderV7,
)
from kio.schema.offset_fetch.v7.response import (
    OffsetFetchResponse as OffsetFetchResponseV7,
)
from kio.schema.offset_fetch.v7.response import (
    ResponseHeader as OffsetFetchResponseHeaderV7,
)
from kio.schema.offset_fetch.v8.request import (
    OffsetFetchRequest as OffsetFetchRequestV8,
)
from kio.schema.offset_fetch.v8.request import (
    RequestHeader as OffsetFetchRequestHeaderV8,
)
from kio.schema.offset_fetch.v8.response import (
    OffsetFetchResponse as OffsetFetchResponseV8,
)
from kio.schema.offset_fetch.v8.response import (
    ResponseHeader as OffsetFetchResponseHeaderV8,
)
from kio.schema.offset_fetch.v9.request import (
    OffsetFetchRequest as OffsetFetchRequestV9,
)
from kio.schema.offset_fetch.v9.request import (
    RequestHeader as OffsetFetchRequestHeaderV9,
)
from kio.schema.offset_fetch.v9.response import (
    OffsetFetchResponse as OffsetFetchResponseV9,
)
from kio.schema.offset_fetch.v9.response import (
    ResponseHeader as OffsetFetchResponseHeaderV9,
)
from kio.schema.errors import ErrorCode

from sqlalchemy import and_, or_, select

from icestream.config import Config
from icestream.kafkaserver.handlers.offset_response import (
    build_group_response,
    build_offset_response,
    build_partition_response,
    build_topic_response,
    response_sequence_element,
)
from icestream.kafkaserver.internal_offsets import commit_timestamp_ms
from icestream.models.consumer_groups import ConsumerGroup, GroupOffset


OffsetFetchRequestHeader = (
    OffsetFetchRequestHeaderV0
    | OffsetFetchRequestHeaderV1
    | OffsetFetchRequestHeaderV2
    | OffsetFetchRequestHeaderV3
    | OffsetFetchRequestHeaderV4
    | OffsetFetchRequestHeaderV5
    | OffsetFetchRequestHeaderV6
    | OffsetFetchRequestHeaderV7
    | OffsetFetchRequestHeaderV8
    | OffsetFetchRequestHeaderV9
)

OffsetFetchResponseHeader = (
    OffsetFetchResponseHeaderV0
    | OffsetFetchResponseHeaderV1
    | OffsetFetchResponseHeaderV2
    | OffsetFetchResponseHeaderV3
    | OffsetFetchResponseHeaderV4
    | OffsetFetchResponseHeaderV5
    | OffsetFetchResponseHeaderV6
    | OffsetFetchResponseHeaderV7
    | OffsetFetchResponseHeaderV8
    | OffsetFetchResponseHeaderV9
)

OffsetFetchRequest = (
    OffsetFetchRequestV0
    | OffsetFetchRequestV1
    | OffsetFetchRequestV2
    | OffsetFetchRequestV3
    | OffsetFetchRequestV4
    | OffsetFetchRequestV5
    | OffsetFetchRequestV6
    | OffsetFetchRequestV7
    | OffsetFetchRequestV8
    | OffsetFetchRequestV9
)

OffsetFetchResponse = (
    OffsetFetchResponseV0
    | OffsetFetchResponseV1
    | OffsetFetchResponseV2
    | OffsetFetchResponseV3
    | OffsetFetchResponseV4
    | OffsetFetchResponseV5
    | OffsetFetchResponseV6
    | OffsetFetchResponseV7
    | OffsetFetchResponseV8
    | OffsetFetchResponseV9
)


def _response_class(api_version: int):
    if api_version == 0:
        return OffsetFetchResponseV0
    if api_version == 1:
        return OffsetFetchResponseV1
    if api_version == 2:
        return OffsetFetchResponseV2
    if api_version == 3:
        return OffsetFetchResponseV3
    if api_version == 4:
        return OffsetFetchResponseV4
    if api_version == 5:
        return OffsetFetchResponseV5
    if api_version == 6:
        return OffsetFetchResponseV6
    if api_version == 7:
        return OffsetFetchResponseV7
    if api_version == 8:
        return OffsetFetchResponseV8
    if api_version == 9:
        return OffsetFetchResponseV9
    raise ValueError(f"unsupported offset_fetch api version: {api_version}")


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
            return getattr(req, name)
    return None


def _request_group_id(req: object) -> str:
    for name in ("group_id", "group"):
        if hasattr(req, name):
            return str(getattr(req, name))
    raise ValueError("group_id not found in request")


def _request_groups(req: object):
    for name in ("groups",):
        if hasattr(req, name):
            return getattr(req, name)
    return None


def _response_shapes(response_cls: type):
    group_field = response_sequence_element(response_cls, "groups")

    topic_field = response_sequence_element(response_cls, "topics")
    if topic_field is None:
        topic_field = response_sequence_element(response_cls, "responses")

    if topic_field is None and group_field is not None:
        topic_field = response_sequence_element(group_field, "topics")
        if topic_field is None:
            topic_field = response_sequence_element(group_field, "responses")

    if topic_field is None:
        raise ValueError("offset_fetch response topic element not found")

    partition_field = response_sequence_element(topic_field, "partitions")
    if partition_field is None:
        partition_field = response_sequence_element(topic_field, "partition_responses")
    if partition_field is None:
        raise ValueError("offset_fetch response partition element not found")

    return group_field, topic_field, partition_field


async def _fetch_offsets_for_group(
    session,
    *,
    group_id: str,
    topics_req: object | None,
    topic_field: type,
    partition_field: type,
):
    group = (
        await session.execute(
            select(ConsumerGroup).where(ConsumerGroup.group_id == group_id)
        )
    ).scalar_one_or_none()

    if group is None:
        topic_responses: list = []
        if topics_req:
            for topic in topics_req:
                topic_name = _topic_name(topic)
                partition_responses: list = []
                for partition in _topic_partitions(topic):
                    partition_id = _partition_index(partition)
                    partition_responses.append(
                        build_partition_response(
                            partition_field,
                            partition=partition_id,
                            error_code=ErrorCode.group_id_not_found,
                            committed_offset=-1,
                            committed_metadata=None,
                            commit_timestamp_ms=-1,
                            leader_epoch=-1,
                        )
                    )
                topic_responses.append(
                    build_topic_response(
                        topic_field,
                        topic_name=topic_name,
                        partitions=partition_responses,
                    )
                )
        return ErrorCode.group_id_not_found, topic_responses

    if not topics_req:
        rows = (
            await session.execute(
                select(GroupOffset).where(GroupOffset.consumer_group_id == group.id)
            )
        ).scalars().all()
        by_topic: dict[str, list[GroupOffset]] = {}
        for row in rows:
            by_topic.setdefault(row.topic_name, []).append(row)

        topic_responses = []
        for topic_name in sorted(by_topic.keys()):
            partition_responses = []
            for row in sorted(by_topic[topic_name], key=lambda r: int(r.partition_number)):
                partition_responses.append(
                    build_partition_response(
                        partition_field,
                        partition=int(row.partition_number),
                        error_code=ErrorCode.none,
                        committed_offset=int(row.committed_offset),
                        committed_metadata=row.committed_metadata,
                        commit_timestamp_ms=commit_timestamp_ms(row.commit_timestamp),
                        leader_epoch=-1,
                    )
                )
            topic_responses.append(
                build_topic_response(
                    topic_field,
                    topic_name=topic_name,
                    partitions=partition_responses,
                )
            )
        return ErrorCode.none, topic_responses

    requested_pairs: list[tuple[str, int]] = []
    for topic in topics_req:
        topic_name = _topic_name(topic)
        for partition in _topic_partitions(topic):
            requested_pairs.append((topic_name, _partition_index(partition)))

    offsets_map: dict[tuple[str, int], GroupOffset] = {}
    if requested_pairs:
        requested_predicates = [
            and_(
                GroupOffset.topic_name == topic_name,
                GroupOffset.partition_number == partition_number,
            )
            for topic_name, partition_number in requested_pairs
        ]
        rows = (
            await session.execute(
                select(GroupOffset).where(
                    GroupOffset.consumer_group_id == group.id,
                    or_(*requested_predicates),
                )
            )
        ).scalars().all()
        offsets_map = {
            (row.topic_name, int(row.partition_number)): row for row in rows
        }

    topic_responses: list = []
    for topic in topics_req:
        topic_name = _topic_name(topic)
        partition_responses: list = []
        for partition in _topic_partitions(topic):
            partition_id = _partition_index(partition)
            row = offsets_map.get((topic_name, partition_id))
            if row is None:
                partition_responses.append(
                    build_partition_response(
                        partition_field,
                        partition=partition_id,
                        error_code=ErrorCode.none,
                        committed_offset=-1,
                        committed_metadata=None,
                        commit_timestamp_ms=-1,
                        leader_epoch=-1,
                    )
                )
                continue

            partition_responses.append(
                build_partition_response(
                    partition_field,
                    partition=partition_id,
                    error_code=ErrorCode.none,
                    committed_offset=int(row.committed_offset),
                    committed_metadata=row.committed_metadata,
                    commit_timestamp_ms=commit_timestamp_ms(row.commit_timestamp),
                    leader_epoch=-1,
                )
            )
        topic_responses.append(
            build_topic_response(
                topic_field,
                topic_name=topic_name,
                partitions=partition_responses,
            )
        )
    return ErrorCode.none, topic_responses


async def do_offset_fetch(
    config: Config,
    req: OffsetFetchRequest,
    api_version: int,
) -> OffsetFetchResponse:
    assert config.async_session_factory is not None

    response_cls = _response_class(api_version)
    group_field, topic_field, partition_field = _response_shapes(response_cls)
    groups_req = _request_groups(req)

    async with config.async_session_factory() as session:
        if groups_req is None:
            group_id = _request_group_id(req)
            topics_req = _request_topics(req)
            error_code, topics = await _fetch_offsets_for_group(
                session,
                group_id=group_id,
                topics_req=topics_req,
                topic_field=topic_field,
                partition_field=partition_field,
            )
            if group_field is not None:
                return build_offset_response(
                    response_cls,
                    groups=(
                        build_group_response(
                            group_field,
                            group_id=group_id,
                            topics=topics,
                            error_code=error_code,
                        ),
                    ),
                )
            return build_offset_response(
                response_cls,
                topics=topics,
                error_code=error_code,
            )

        if group_field is None:
            raise ValueError("offset_fetch response group element not found")

        group_responses: list = []
        for group_req in groups_req:
            group_id = _request_group_id(group_req)
            topics_req = _request_topics(group_req)
            error_code, topics = await _fetch_offsets_for_group(
                session,
                group_id=group_id,
                topics_req=topics_req,
                topic_field=topic_field,
                partition_field=partition_field,
            )
            group_responses.append(
                build_group_response(
                    group_field,
                    group_id=group_id,
                    topics=topics,
                    error_code=error_code,
                )
            )
        return build_offset_response(response_cls, groups=group_responses)
