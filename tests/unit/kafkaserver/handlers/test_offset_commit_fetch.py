import datetime
from types import SimpleNamespace

import pytest
from kio.schema.errors import ErrorCode
from sqlalchemy import select

from icestream.config import Config
from icestream.kafkaserver.handlers.offset_commit import do_offset_commit
from icestream.kafkaserver.handlers.offset_fetch import do_offset_fetch
from icestream.kafkaserver.internal_topics import ensure_internal_topics
from icestream.models.consumer_groups import ConsumerGroup, GroupMember, GroupOffsetLog
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
    last_heartbeat_at: datetime.datetime | None = None,
) -> GroupMember:
    now = last_heartbeat_at or datetime.datetime.now(datetime.timezone.utc)
    member = GroupMember(
        consumer_group_id=group.id,
        member_id=member_id,
        session_timeout_ms=10_000,
        rebalance_timeout_ms=30_000,
        last_heartbeat_at=now,
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

    groups = getattr(resp, "groups", ())
    if groups:
        group = groups[0]
        return getattr(group, "topics", getattr(group, "responses", ()))
    return ()


def _partition_responses(topic_resp):
    return getattr(topic_resp, "partitions", getattr(topic_resp, "partition_responses", ()))


def _partition_error(partition_resp) -> ErrorCode:
    return partition_resp.error_code


def _partition_offset(partition_resp) -> int:
    value = getattr(partition_resp, "committed_offset", getattr(partition_resp, "offset", -1))
    return int(value)


def _partition_metadata(partition_resp) -> str | None:
    return getattr(partition_resp, "committed_metadata", getattr(partition_resp, "metadata", None))


@pytest.mark.asyncio
async def test_offset_commit_fetch_roundtrip(config: Config) -> None:
    await ensure_internal_topics(config)

    async with config.async_session_factory() as session:
        async with session.begin():
            await insert_topic_partition(session, "topic-a", 0)
            group = await _create_group(session, group_id="cg-roundtrip")
            await _create_member(session, group=group, member_id="member-1", generation=1)

    commit_req = SimpleNamespace(
        group_id="cg-roundtrip",
        generation_id=1,
        member_id="member-1",
        topics=(
            SimpleNamespace(
                name="topic-a",
                partitions=(
                    SimpleNamespace(
                        partition_index=0,
                        committed_offset=42,
                        committed_metadata="meta",
                    ),
                ),
            ),
        ),
    )
    commit_resp = await do_offset_commit(config, commit_req, api_version=9)
    topics = _topic_responses(commit_resp)
    assert len(topics) == 1
    partitions = _partition_responses(topics[0])
    assert len(partitions) == 1
    assert _partition_error(partitions[0]) == ErrorCode.none

    fetch_req = SimpleNamespace(
        group_id="cg-roundtrip",
        topics=(
            SimpleNamespace(
                name="topic-a",
                partitions=(SimpleNamespace(partition_index=0),),
            ),
        ),
    )
    fetch_resp = await do_offset_fetch(config, fetch_req, api_version=9)
    fetch_topics = _topic_responses(fetch_resp)
    partitions = _partition_responses(fetch_topics[0])
    assert _partition_offset(partitions[0]) == 42
    assert _partition_metadata(partitions[0]) == "meta"

    async with config.async_session_factory() as session:
        rows = (
            await session.execute(
                select(GroupOffsetLog).where(GroupOffsetLog.value_bytes.is_not(None))
            )
        ).scalars().all()
        assert rows


@pytest.mark.asyncio
async def test_offset_commit_generation_fencing(config: Config) -> None:
    await ensure_internal_topics(config)

    async with config.async_session_factory() as session:
        async with session.begin():
            await insert_topic_partition(session, "topic-gen", 0)
            group = await _create_group(session, group_id="cg-gen", generation=2)
            await _create_member(session, group=group, member_id="member-1", generation=2)

    commit_req = SimpleNamespace(
        group_id="cg-gen",
        generation_id=1,
        member_id="member-1",
        topics=(
            SimpleNamespace(
                name="topic-gen",
                partitions=(SimpleNamespace(partition_index=0, committed_offset=5),),
            ),
        ),
    )
    resp = await do_offset_commit(config, commit_req, api_version=9)
    partitions = _partition_responses(_topic_responses(resp)[0])
    assert _partition_error(partitions[0]) == ErrorCode.illegal_generation


@pytest.mark.asyncio
async def test_offset_commit_rebalance_in_progress(config: Config) -> None:
    await ensure_internal_topics(config)

    async with config.async_session_factory() as session:
        async with session.begin():
            await insert_topic_partition(session, "topic-rb", 0)
            group = await _create_group(session, group_id="cg-rb", generation=1, state="PreparingRebalance")
            await _create_member(session, group=group, member_id="member-1", generation=1)

    commit_req = SimpleNamespace(
        group_id="cg-rb",
        generation_id=1,
        member_id="member-1",
        topics=(
            SimpleNamespace(
                name="topic-rb",
                partitions=(SimpleNamespace(partition_index=0, committed_offset=7),),
            ),
        ),
    )
    resp = await do_offset_commit(config, commit_req, api_version=9)
    partitions = _partition_responses(_topic_responses(resp)[0])
    assert _partition_error(partitions[0]) == ErrorCode.rebalance_in_progress
