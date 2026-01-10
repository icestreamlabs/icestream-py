import datetime

import pytest
from kio.schema.errors import ErrorCode
from kio.schema.leave_group.v2.request import LeaveGroupRequest as LeaveGroupRequestV2
from kio.schema.leave_group.v5.request import LeaveGroupRequest, MemberIdentity
from kio.schema.types import GroupId
from sqlalchemy import select

from icestream.config import Config
from icestream.kafkaserver.handlers.leave_group import do_leave_group
from icestream.models.consumer_groups import ConsumerGroup, GroupMember


async def _create_group(
    session,
    *,
    group_id: str,
    generation: int = 1,
    state: str = "Stable",
    protocol_type: str | None = "consumer",
    protocol_name: str | None = "range",
    leader_member_id: str | None = None,
    join_deadline_at: datetime.datetime | None = None,
    sync_deadline_at: datetime.datetime | None = None,
) -> ConsumerGroup:
    group = ConsumerGroup(
        group_id=group_id,
        generation=generation,
        state=state,
        protocol_type=protocol_type,
        selected_protocol=protocol_name,
        leader_member_id=leader_member_id,
        join_phase_deadline_at=join_deadline_at,
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
    last_heartbeat_at: datetime.datetime | None = None,
) -> GroupMember:
    now = last_heartbeat_at or datetime.datetime.now(datetime.timezone.utc)
    member = GroupMember(
        consumer_group_id=group.id,
        member_id=member_id,
        group_instance_id=group_instance_id,
        session_timeout_ms=10_000,
        rebalance_timeout_ms=30_000,
        last_heartbeat_at=now,
        member_generation=generation,
        join_generation=generation,
        is_in_sync=True,
        assignment=None,
    )
    session.add(member)
    await session.flush()
    return member


@pytest.mark.asyncio
async def test_leave_group_unknown_group_v2(config: Config) -> None:
    req = LeaveGroupRequestV2(group_id=GroupId("cg-missing"), member_id="m1")
    resp = await do_leave_group(config, req, api_version=2)
    assert resp.error_code == ErrorCode.unknown_member_id


@pytest.mark.asyncio
async def test_leave_group_unknown_member_returns_member_error(config: Config) -> None:
    async with config.async_session_factory() as session:
        async with session.begin():
            group = await _create_group(session, group_id="cg-unknown")
            await _create_member(session, group=group, member_id="m1", generation=1)

    req = LeaveGroupRequest(
        group_id=GroupId("cg-unknown"),
        members=(MemberIdentity(member_id="m2", group_instance_id=None, reason=None),),
    )
    resp = await do_leave_group(config, req, api_version=5)
    assert resp.error_code == ErrorCode.none
    assert len(resp.members) == 1
    assert resp.members[0].member_id == "m2"
    assert resp.members[0].error_code == ErrorCode.unknown_member_id

    async with config.async_session_factory() as session:
        cg = (
            await session.execute(
                select(ConsumerGroup).where(ConsumerGroup.group_id == "cg-unknown")
            )
        ).scalar_one()
        members = (
            await session.execute(
                select(GroupMember).where(GroupMember.consumer_group_id == cg.id)
            )
        ).scalars().all()
        assert len(members) == 1


@pytest.mark.asyncio
async def test_leave_group_fenced_instance_id(config: Config) -> None:
    group_id = "cg-static"
    instance_id = "inst-1"
    static_member_id = f"{group_id}:{instance_id}"

    async with config.async_session_factory() as session:
        async with session.begin():
            group = await _create_group(session, group_id=group_id)
            await _create_member(
                session,
                group=group,
                member_id=static_member_id,
                generation=1,
                group_instance_id=instance_id,
            )

    req = LeaveGroupRequest(
        group_id=GroupId(group_id),
        members=(MemberIdentity(member_id="other-member", group_instance_id=instance_id, reason=None),),
    )
    resp = await do_leave_group(config, req, api_version=5)
    assert resp.error_code == ErrorCode.none
    assert resp.members[0].error_code == ErrorCode.fenced_instance_id

    async with config.async_session_factory() as session:
        cg = (
            await session.execute(
                select(ConsumerGroup).where(ConsumerGroup.group_id == group_id)
            )
        ).scalar_one()
        members = (
            await session.execute(
                select(GroupMember).where(GroupMember.consumer_group_id == cg.id)
            )
        ).scalars().all()
        assert len(members) == 1


@pytest.mark.asyncio
async def test_leave_group_triggers_rebalance(config: Config) -> None:
    now = datetime.datetime.now(datetime.timezone.utc)
    async with config.async_session_factory() as session:
        async with session.begin():
            group = await _create_group(
                session,
                group_id="cg-rebalance",
                leader_member_id="m1",
                sync_deadline_at=now,
            )
            await _create_member(session, group=group, member_id="m1", generation=1)
            await _create_member(session, group=group, member_id="m2", generation=1)

    req = LeaveGroupRequest(
        group_id=GroupId("cg-rebalance"),
        members=(MemberIdentity(member_id="m1", group_instance_id=None, reason=None),),
    )
    resp = await do_leave_group(config, req, api_version=5)
    assert resp.error_code == ErrorCode.none
    assert resp.members[0].error_code == ErrorCode.none

    async with config.async_session_factory() as session:
        cg = (
            await session.execute(
                select(ConsumerGroup).where(ConsumerGroup.group_id == "cg-rebalance")
            )
        ).scalar_one()
        assert cg.state == "PreparingRebalance"
        assert cg.join_phase_deadline_at is not None
        assert cg.sync_phase_deadline_at is None
        assert cg.leader_member_id is None
        members = (
            await session.execute(
                select(GroupMember).where(GroupMember.consumer_group_id == cg.id)
            )
        ).scalars().all()
        assert len(members) == 1


@pytest.mark.asyncio
async def test_leave_group_last_member_empties_group(config: Config) -> None:
    async with config.async_session_factory() as session:
        async with session.begin():
            group = await _create_group(session, group_id="cg-empty", generation=1)
            await _create_member(session, group=group, member_id="m1", generation=1)

    req = LeaveGroupRequest(
        group_id=GroupId("cg-empty"),
        members=(MemberIdentity(member_id="m1", group_instance_id=None, reason=None),),
    )
    resp = await do_leave_group(config, req, api_version=5)
    assert resp.error_code == ErrorCode.none
    assert resp.members[0].error_code == ErrorCode.none

    async with config.async_session_factory() as session:
        cg = (
            await session.execute(
                select(ConsumerGroup).where(ConsumerGroup.group_id == "cg-empty")
            )
        ).scalar_one()
        assert cg.state == "Empty"
        assert cg.selected_protocol is None
        assert cg.leader_member_id is None
        assert cg.join_phase_deadline_at is None
        assert cg.sync_phase_deadline_at is None
        assert cg.generation == 2
        members = (
            await session.execute(
                select(GroupMember).where(GroupMember.consumer_group_id == cg.id)
            )
        ).scalars().all()
        assert len(members) == 0
