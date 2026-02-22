import datetime as dt
from types import SimpleNamespace

import pytest
from kio.schema.errors import ErrorCode
from kio.schema.heartbeat.v4.request import HeartbeatRequest
from kio.schema.join_group.v9.request import JoinGroupRequest, JoinGroupRequestProtocol
from kio.schema.sync_group.v5.request import SyncGroupRequest
from kio.schema.types import GroupId
from kio.static.primitive import i32

from icestream.config import Config
from icestream.kafkaserver.handlers.heartbeat import do_heartbeat
from icestream.kafkaserver.handlers.join_group import do_join_group
from icestream.kafkaserver.handlers.offset_commit import do_offset_commit
from icestream.kafkaserver.handlers.sync_group import do_sync_group
from icestream.kafkaserver.internal_topics import ensure_internal_topics
from icestream.models.consumer_groups import ConsumerGroup, GroupMember
from tests.unit.conftest import i32timedelta_from_ms, insert_topic_partition


async def _create_group(
    session,
    *,
    group_id: str,
    generation: int,
    state: str,
    selected_protocol: str | None = "range",
    leader_member_id: str | None = None,
    sync_deadline_at: dt.datetime | None = None,
) -> ConsumerGroup:
    group = ConsumerGroup(
        group_id=group_id,
        generation=generation,
        state=state,
        protocol_type="consumer",
        selected_protocol=selected_protocol,
        leader_member_id=leader_member_id,
        sync_phase_deadline_at=sync_deadline_at,
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
    group_instance_id: str | None = None,
    assignment: bytes | None = None,
) -> GroupMember:
    member = GroupMember(
        consumer_group_id=group.id,
        member_id=member_id,
        group_instance_id=group_instance_id,
        session_timeout_ms=10_000,
        rebalance_timeout_ms=30_000,
        last_heartbeat_at=dt.datetime.now(dt.timezone.utc),
        member_generation=generation,
        join_generation=generation,
        is_in_sync=True,
        assignment=assignment,
    )
    session.add(member)
    await session.flush()
    return member


@pytest.mark.asyncio
async def test_completing_rebalance_timeout_recovers_via_sync_then_join(config: Config):
    now = dt.datetime.now(dt.timezone.utc)
    async with config.async_session_factory() as session:
        async with session.begin():
            group = await _create_group(
                session,
                group_id="cg-timeout-recovery",
                generation=1,
                state="CompletingRebalance",
                leader_member_id="leader",
                sync_deadline_at=now - dt.timedelta(seconds=1),
            )
            await _create_member(
                session,
                group=group,
                member_id="leader",
                generation=1,
                assignment=b"old",
            )

    sync_req = SyncGroupRequest(
        group_id=GroupId("cg-timeout-recovery"),
        generation_id=i32(1),
        member_id="leader",
        group_instance_id=None,
        protocol_type="consumer",
        protocol_name="range",
        assignments=tuple(),
    )
    sync_resp = await do_sync_group(config, sync_req, api_version=5)
    assert sync_resp.error_code == ErrorCode.rebalance_in_progress

    join_req = JoinGroupRequest(
        group_id=GroupId("cg-timeout-recovery"),
        session_timeout=i32timedelta_from_ms(10_000),
        rebalance_timeout=i32timedelta_from_ms(30_000),
        member_id="leader",
        group_instance_id=None,
        protocol_type="consumer",
        protocols=(JoinGroupRequestProtocol(name="range", metadata=b""),),
        reason=None,
    )
    join_resp = await do_join_group(config, join_req, api_version=9)
    assert join_resp.error_code == ErrorCode.none


@pytest.mark.asyncio
async def test_generation_fencing_matrix_across_group_handlers(config: Config):
    await ensure_internal_topics(config)
    async with config.async_session_factory() as session:
        async with session.begin():
            await insert_topic_partition(session, "topic-fencing", 0)
            group = await _create_group(
                session,
                group_id="cg-gen-fencing",
                generation=5,
                state="Stable",
                leader_member_id="member-1",
            )
            await _create_member(session, group=group, member_id="member-1", generation=5)

    join_req = JoinGroupRequest(
        group_id=GroupId("cg-gen-fencing"),
        session_timeout=i32timedelta_from_ms(10_000),
        rebalance_timeout=i32timedelta_from_ms(30_000),
        member_id="missing-member",
        group_instance_id=None,
        protocol_type="consumer",
        protocols=(JoinGroupRequestProtocol(name="range", metadata=b""),),
        reason=None,
    )
    join_resp = await do_join_group(config, join_req, api_version=9)
    assert join_resp.error_code == ErrorCode.unknown_member_id

    sync_req = SyncGroupRequest(
        group_id=GroupId("cg-gen-fencing"),
        generation_id=i32(4),
        member_id="member-1",
        group_instance_id=None,
        protocol_type="consumer",
        protocol_name="range",
        assignments=tuple(),
    )
    sync_resp = await do_sync_group(config, sync_req, api_version=5)
    assert sync_resp.error_code == ErrorCode.illegal_generation

    heartbeat_req = HeartbeatRequest(
        group_id=GroupId("cg-gen-fencing"),
        generation_id=i32(4),
        member_id="member-1",
        group_instance_id=None,
    )
    heartbeat_resp = await do_heartbeat(config, heartbeat_req, api_version=4)
    assert heartbeat_resp.error_code == ErrorCode.illegal_generation

    offset_commit_req = SimpleNamespace(
        group_id="cg-gen-fencing",
        generation_id=4,
        member_id="member-1",
        topics=(
            SimpleNamespace(
                name="topic-fencing",
                partitions=(SimpleNamespace(partition_index=0, committed_offset=5),),
            ),
        ),
    )
    offset_commit_resp = await do_offset_commit(config, offset_commit_req, api_version=9)
    partition = offset_commit_resp.topics[0].partitions[0]
    assert partition.error_code == ErrorCode.illegal_generation


@pytest.mark.asyncio
async def test_static_member_fencing_interactions_for_join_sync_and_heartbeat(config: Config):
    async with config.async_session_factory() as session:
        async with session.begin():
            group = await _create_group(
                session,
                group_id="cg-static-fencing",
                generation=1,
                state="Stable",
                leader_member_id="member-1",
            )
            await _create_member(
                session,
                group=group,
                member_id="member-1",
                generation=1,
                group_instance_id="instance-a",
            )

    join_req = JoinGroupRequest(
        group_id=GroupId("cg-static-fencing"),
        session_timeout=i32timedelta_from_ms(10_000),
        rebalance_timeout=i32timedelta_from_ms(30_000),
        member_id="",
        group_instance_id="instance-a",
        protocol_type="consumer",
        protocols=(JoinGroupRequestProtocol(name="range", metadata=b""),),
        reason=None,
    )
    join_resp = await do_join_group(config, join_req, api_version=9)
    assert join_resp.error_code == ErrorCode.fenced_instance_id

    sync_req = SyncGroupRequest(
        group_id=GroupId("cg-static-fencing"),
        generation_id=i32(1),
        member_id="member-1",
        group_instance_id="instance-b",
        protocol_type="consumer",
        protocol_name="range",
        assignments=tuple(),
    )
    sync_resp = await do_sync_group(config, sync_req, api_version=5)
    assert sync_resp.error_code == ErrorCode.fenced_instance_id

    heartbeat_req = HeartbeatRequest(
        group_id=GroupId("cg-static-fencing"),
        generation_id=i32(1),
        member_id="member-1",
        group_instance_id="instance-b",
    )
    heartbeat_resp = await do_heartbeat(config, heartbeat_req, api_version=4)
    assert heartbeat_resp.error_code == ErrorCode.fenced_instance_id
