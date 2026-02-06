import datetime
from dataclasses import dataclass
from types import SimpleNamespace

import pytest
from kio.schema.errors import ErrorCode

from icestream.config import Config
from icestream.kafkaserver.handlers.offset_commit import do_offset_commit
from icestream.kafkaserver.handlers.offset_fetch import (
    _partition_index,
    _request_group_id,
    _request_groups,
    _request_topics,
    _response_class,
    _response_shapes,
    _topic_name,
    _topic_partitions,
    do_offset_fetch,
)
from icestream.kafkaserver.internal_topics import ensure_internal_topics
from icestream.models.consumer_groups import ConsumerGroup, GroupMember
from tests.unit.conftest import insert_topic_partition


async def _create_group(
    session,
    *,
    group_id: str,
    generation: int = 1,
    state: str = "Stable",
) -> ConsumerGroup:
    group = ConsumerGroup(
        group_id=group_id,
        generation=generation,
        state=state,
        protocol_type="consumer",
        selected_protocol="range",
    )
    session.add(group)
    await session.flush()
    return group


async def _create_member(
    session,
    *,
    group: ConsumerGroup,
    member_id: str,
    generation: int,
) -> GroupMember:
    member = GroupMember(
        consumer_group_id=group.id,
        member_id=member_id,
        session_timeout_ms=10_000,
        rebalance_timeout_ms=30_000,
        last_heartbeat_at=datetime.datetime.now(datetime.timezone.utc),
        member_generation=generation,
        join_generation=generation,
        is_in_sync=True,
    )
    session.add(member)
    await session.flush()
    return member


def _topic_responses(resp):
    topics = getattr(resp, "topics", None)
    if topics is not None:
        return topics
    responses = getattr(resp, "responses", None)
    if responses is not None:
        return responses
    return ()


def _partition_responses(topic_resp):
    return getattr(topic_resp, "partitions", getattr(topic_resp, "partition_responses", ()))


@pytest.mark.asyncio
async def test_offset_fetch_v9_groups_request_mixed_existing_and_missing_group(
    config: Config,
) -> None:
    await ensure_internal_topics(config)

    async with config.async_session_factory() as session:
        async with session.begin():
            await insert_topic_partition(session, "topic-grouped", 0)
            group = await _create_group(session, group_id="cg-grouped")
            await _create_member(session, group=group, member_id="member-1", generation=1)

    commit_req = SimpleNamespace(
        group_id="cg-grouped",
        generation_id=1,
        member_id="member-1",
        topics=(
            SimpleNamespace(
                name="topic-grouped",
                partitions=(SimpleNamespace(partition_index=0, committed_offset=12),),
            ),
        ),
    )
    await do_offset_commit(config, commit_req, api_version=9)

    fetch_req = SimpleNamespace(
        groups=(
            SimpleNamespace(
                group_id="cg-grouped",
                topics=(
                    SimpleNamespace(
                        name="topic-grouped",
                        partitions=(SimpleNamespace(partition_index=0),),
                    ),
                ),
            ),
            SimpleNamespace(
                group_id="cg-missing",
                topics=(
                    SimpleNamespace(
                        name="topic-grouped",
                        partitions=(SimpleNamespace(partition_index=0),),
                    ),
                ),
            ),
        )
    )
    fetch_resp = await do_offset_fetch(config, fetch_req, api_version=9)
    assert len(fetch_resp.groups) == 2

    existing_group, missing_group = fetch_resp.groups
    assert existing_group.error_code == ErrorCode.none
    assert missing_group.error_code == ErrorCode.group_id_not_found

    existing_partition = _partition_responses(_topic_responses(existing_group)[0])[0]
    missing_partition = _partition_responses(_topic_responses(missing_group)[0])[0]
    assert existing_partition.error_code == ErrorCode.none
    assert int(existing_partition.committed_offset) == 12
    assert missing_partition.error_code == ErrorCode.group_id_not_found
    assert int(missing_partition.committed_offset) == -1


@pytest.mark.asyncio
async def test_offset_fetch_v9_topics_omitted_returns_all_group_offsets(config: Config) -> None:
    await ensure_internal_topics(config)

    async with config.async_session_factory() as session:
        async with session.begin():
            await insert_topic_partition(session, "topic-a", 0)
            await insert_topic_partition(session, "topic-b", 0)
            group = await _create_group(session, group_id="cg-all-topics")
            await _create_member(session, group=group, member_id="member-1", generation=1)

    commit_req = SimpleNamespace(
        group_id="cg-all-topics",
        generation_id=1,
        member_id="member-1",
        topics=(
            SimpleNamespace(
                name="topic-a",
                partitions=(SimpleNamespace(partition_index=0, committed_offset=7),),
            ),
            SimpleNamespace(
                name="topic-b",
                partitions=(SimpleNamespace(partition_index=0, committed_offset=9),),
            ),
        ),
    )
    await do_offset_commit(config, commit_req, api_version=9)

    fetch_req = SimpleNamespace(groups=(SimpleNamespace(group_id="cg-all-topics"),))
    fetch_resp = await do_offset_fetch(config, fetch_req, api_version=9)

    assert len(fetch_resp.groups) == 1
    topics = _topic_responses(fetch_resp.groups[0])
    assert [t.name for t in topics] == ["topic-a", "topic-b"]


@pytest.mark.asyncio
async def test_offset_fetch_v1_single_group_topic_response_shape(config: Config) -> None:
    await ensure_internal_topics(config)

    async with config.async_session_factory() as session:
        async with session.begin():
            await insert_topic_partition(session, "topic-v1", 0)
            group = await _create_group(session, group_id="cg-v1")
            await _create_member(session, group=group, member_id="member-1", generation=1)

    commit_req = SimpleNamespace(
        group_id="cg-v1",
        generation_id=1,
        member_id="member-1",
        topics=(
            SimpleNamespace(
                name="topic-v1",
                partitions=(SimpleNamespace(partition_index=0, committed_offset=3),),
            ),
        ),
    )
    await do_offset_commit(config, commit_req, api_version=1)

    fetch_req = SimpleNamespace(
        group_id="cg-v1",
        topics=(SimpleNamespace(name="topic-v1", partitions=(SimpleNamespace(partition_index=0),)),),
    )
    fetch_resp = await do_offset_fetch(config, fetch_req, api_version=1)
    topics = _topic_responses(fetch_resp)
    assert len(topics) == 1
    assert _partition_responses(topics[0])[0].error_code == ErrorCode.none


def test_offset_fetch_helpers_error_paths() -> None:
    for api_version in range(10):
        assert _response_class(api_version) is not None
    with pytest.raises(ValueError):
        _response_class(10)

    with pytest.raises(ValueError):
        _topic_name(object())
    with pytest.raises(ValueError):
        _partition_index(object())
    with pytest.raises(ValueError):
        _request_group_id(object())

    assert _topic_partitions(object()) == ()
    assert _request_topics(object()) is None
    assert _request_groups(object()) is None

    @dataclass
    class EmptyResponse:
        pass

    with pytest.raises(ValueError):
        _response_shapes(EmptyResponse)

    @dataclass
    class TopicWithoutPartitions:
        name: str = ""

    @dataclass
    class ResponseWithoutTopicPartitions:
        topics: tuple[TopicWithoutPartitions, ...] = ()

    with pytest.raises(ValueError):
        _response_shapes(ResponseWithoutTopicPartitions)
