import datetime as dt

import pytest
from kio.schema.errors import ErrorCode
from kio.schema.heartbeat.v4.request import HeartbeatRequest
from kio.schema.heartbeat.v0.response import HeartbeatResponse as HeartbeatResponseV0
from kio.schema.heartbeat.v1.response import HeartbeatResponse as HeartbeatResponseV1
from kio.schema.heartbeat.v2.response import HeartbeatResponse as HeartbeatResponseV2
from kio.schema.heartbeat.v3.response import HeartbeatResponse as HeartbeatResponseV3
from kio.schema.heartbeat.v4.response import HeartbeatResponse as HeartbeatResponseV4
from kio.schema.types import GroupId
from kio.static.primitive import i32
from sqlalchemy import select

from icestream.kafkaserver.handlers.heartbeat import _build_heartbeat_response, do_heartbeat
from icestream.models.consumer_groups import ConsumerGroup, GroupMember
from icestream.utils import zero_throttle


@pytest.mark.asyncio
async def test_heartbeat_updates_member_in_stable_group(config):
    now = dt.datetime.now(dt.timezone.utc)

    async with config.async_session_factory() as session:
        async with session.begin():
            group = ConsumerGroup(
                group_id="group-hb",
                state="Stable",
                generation=1,
                protocol_type="consumer",
                selected_protocol="range",
            )
            session.add(group)
            await session.flush()

            member = GroupMember(
                consumer_group_id=group.id,
                member_id="member-1",
                session_timeout_ms=10_000,
                rebalance_timeout_ms=30_000,
                last_heartbeat_at=now - dt.timedelta(seconds=5),
                member_generation=1,
                join_generation=1,
                is_in_sync=True,
                assignment=b"",
            )
            session.add(member)

    req = HeartbeatRequest(
        group_id=GroupId("group-hb"),
        generation_id=i32(1),
        member_id="member-1",
        group_instance_id=None,
    )
    resp = await do_heartbeat(config, req, api_version=4)

    assert resp.error_code == ErrorCode.none

    async with config.async_session_factory() as session:
        group = (
            await session.execute(
                select(ConsumerGroup).where(ConsumerGroup.group_id == "group-hb")
            )
        ).scalar_one()
        refreshed = (
            await session.execute(
                select(GroupMember).where(
                    GroupMember.consumer_group_id == group.id,
                    GroupMember.member_id == "member-1",
                )
            )
        ).scalar_one()
        assert refreshed.last_heartbeat_at >= now


@pytest.mark.asyncio
async def test_heartbeat_resets_expired_sync_deadline(config):
    now = dt.datetime.now(dt.timezone.utc)

    async with config.async_session_factory() as session:
        async with session.begin():
            group = ConsumerGroup(
                group_id="group-sync",
                state="CompletingRebalance",
                generation=1,
                protocol_type="consumer",
                selected_protocol="range",
                sync_phase_deadline_at=now - dt.timedelta(seconds=1),
            )
            session.add(group)
            await session.flush()

            member = GroupMember(
                consumer_group_id=group.id,
                member_id="member-sync",
                session_timeout_ms=10_000,
                rebalance_timeout_ms=30_000,
                last_heartbeat_at=now - dt.timedelta(seconds=2),
                member_generation=1,
                join_generation=1,
                is_in_sync=True,
                assignment=b"assigned",
            )
            session.add(member)

    req = HeartbeatRequest(
        group_id=GroupId("group-sync"),
        generation_id=i32(1),
        member_id="member-sync",
        group_instance_id=None,
    )
    resp = await do_heartbeat(config, req, api_version=4)

    assert resp.error_code == ErrorCode.rebalance_in_progress

    async with config.async_session_factory() as session:
        refreshed_group = (
            await session.execute(
                select(ConsumerGroup).where(ConsumerGroup.group_id == "group-sync")
            )
        ).scalar_one()
        refreshed_member = (
            await session.execute(
                select(GroupMember).where(
                    GroupMember.consumer_group_id == refreshed_group.id,
                    GroupMember.member_id == "member-sync",
                )
            )
        ).scalar_one()

        assert refreshed_group.state == "PreparingRebalance"
        assert refreshed_group.sync_phase_deadline_at is None
        assert refreshed_member.assignment is None
        assert refreshed_member.is_in_sync is False


@pytest.mark.parametrize(
    ("api_version", "response_cls", "expect_throttle"),
    [
        (0, HeartbeatResponseV0, False),
        (1, HeartbeatResponseV1, True),
        (2, HeartbeatResponseV2, True),
        (3, HeartbeatResponseV3, True),
        (4, HeartbeatResponseV4, True),
    ],
)
def test_build_heartbeat_response_versions(api_version, response_cls, expect_throttle):
    resp = _build_heartbeat_response(ErrorCode.none, api_version)

    assert isinstance(resp, response_cls)
    assert resp.error_code == ErrorCode.none
    if expect_throttle:
        assert resp.throttle_time == zero_throttle()
    else:
        assert not hasattr(resp, "throttle_time")


@pytest.mark.asyncio
async def test_heartbeat_unknown_group_returns_unknown_member(config):
    req = HeartbeatRequest(
        group_id=GroupId("missing-group"),
        generation_id=i32(1),
        member_id="member-1",
        group_instance_id=None,
    )

    resp = await do_heartbeat(config, req, api_version=4)

    assert resp.error_code == ErrorCode.unknown_member_id


@pytest.mark.asyncio
async def test_heartbeat_unknown_member_returns_unknown_member(config):
    now = dt.datetime.now(dt.timezone.utc)

    async with config.async_session_factory() as session:
        async with session.begin():
            group = ConsumerGroup(
                group_id="group-unknown-member",
                state="Stable",
                generation=1,
                protocol_type="consumer",
                selected_protocol="range",
            )
            session.add(group)
            await session.flush()

            member = GroupMember(
                consumer_group_id=group.id,
                member_id="member-1",
                session_timeout_ms=10_000,
                rebalance_timeout_ms=30_000,
                last_heartbeat_at=now,
                member_generation=1,
                join_generation=1,
                is_in_sync=True,
                assignment=b"",
            )
            session.add(member)

    req = HeartbeatRequest(
        group_id=GroupId("group-unknown-member"),
        generation_id=i32(1),
        member_id="member-2",
        group_instance_id=None,
    )
    resp = await do_heartbeat(config, req, api_version=4)

    assert resp.error_code == ErrorCode.unknown_member_id


@pytest.mark.asyncio
async def test_heartbeat_illegal_generation_for_request(config):
    now = dt.datetime.now(dt.timezone.utc)

    async with config.async_session_factory() as session:
        async with session.begin():
            group = ConsumerGroup(
                group_id="group-gen-mismatch",
                state="Stable",
                generation=2,
                protocol_type="consumer",
                selected_protocol="range",
            )
            session.add(group)
            await session.flush()

            member = GroupMember(
                consumer_group_id=group.id,
                member_id="member-1",
                session_timeout_ms=10_000,
                rebalance_timeout_ms=30_000,
                last_heartbeat_at=now,
                member_generation=2,
                join_generation=2,
                is_in_sync=True,
                assignment=b"",
            )
            session.add(member)

    req = HeartbeatRequest(
        group_id=GroupId("group-gen-mismatch"),
        generation_id=i32(1),
        member_id="member-1",
        group_instance_id=None,
    )
    resp = await do_heartbeat(config, req, api_version=4)

    assert resp.error_code == ErrorCode.illegal_generation


@pytest.mark.asyncio
async def test_heartbeat_illegal_generation_for_member(config):
    now = dt.datetime.now(dt.timezone.utc)

    async with config.async_session_factory() as session:
        async with session.begin():
            group = ConsumerGroup(
                group_id="group-member-gen-mismatch",
                state="Stable",
                generation=2,
                protocol_type="consumer",
                selected_protocol="range",
            )
            session.add(group)
            await session.flush()

            member = GroupMember(
                consumer_group_id=group.id,
                member_id="member-1",
                session_timeout_ms=10_000,
                rebalance_timeout_ms=30_000,
                last_heartbeat_at=now,
                member_generation=1,
                join_generation=2,
                is_in_sync=True,
                assignment=b"",
            )
            session.add(member)

    req = HeartbeatRequest(
        group_id=GroupId("group-member-gen-mismatch"),
        generation_id=i32(2),
        member_id="member-1",
        group_instance_id=None,
    )
    resp = await do_heartbeat(config, req, api_version=4)

    assert resp.error_code == ErrorCode.illegal_generation


@pytest.mark.asyncio
async def test_heartbeat_rebalance_in_progress_when_group_not_stable(config):
    now = dt.datetime.now(dt.timezone.utc)

    async with config.async_session_factory() as session:
        async with session.begin():
            group = ConsumerGroup(
                group_id="group-preparing",
                state="PreparingRebalance",
                generation=1,
                protocol_type="consumer",
                selected_protocol="range",
            )
            session.add(group)
            await session.flush()

            member = GroupMember(
                consumer_group_id=group.id,
                member_id="member-1",
                session_timeout_ms=10_000,
                rebalance_timeout_ms=30_000,
                last_heartbeat_at=now,
                member_generation=1,
                join_generation=1,
                is_in_sync=True,
                assignment=b"",
            )
            session.add(member)

    req = HeartbeatRequest(
        group_id=GroupId("group-preparing"),
        generation_id=i32(1),
        member_id="member-1",
        group_instance_id=None,
    )
    resp = await do_heartbeat(config, req, api_version=4)

    assert resp.error_code == ErrorCode.rebalance_in_progress


@pytest.mark.asyncio
async def test_heartbeat_fenced_instance_id(config):
    now = dt.datetime.now(dt.timezone.utc)

    async with config.async_session_factory() as session:
        async with session.begin():
            group = ConsumerGroup(
                group_id="group-instance-fenced",
                state="Stable",
                generation=1,
                protocol_type="consumer",
                selected_protocol="range",
            )
            session.add(group)
            await session.flush()

            member = GroupMember(
                consumer_group_id=group.id,
                member_id="member-1",
                session_timeout_ms=10_000,
                rebalance_timeout_ms=30_000,
                last_heartbeat_at=now,
                member_generation=1,
                join_generation=1,
                is_in_sync=True,
                assignment=b"",
                group_instance_id="instance-a",
            )
            session.add(member)

    req = HeartbeatRequest(
        group_id=GroupId("group-instance-fenced"),
        generation_id=i32(1),
        member_id="member-1",
        group_instance_id="instance-b",
    )
    resp = await do_heartbeat(config, req, api_version=4)

    assert resp.error_code == ErrorCode.fenced_instance_id


@pytest.mark.asyncio
async def test_heartbeat_sets_instance_id_when_missing(config):
    now = dt.datetime.now(dt.timezone.utc)

    async with config.async_session_factory() as session:
        async with session.begin():
            group = ConsumerGroup(
                group_id="group-instance-set",
                state="Stable",
                generation=1,
                protocol_type="consumer",
                selected_protocol="range",
            )
            session.add(group)
            await session.flush()

            member = GroupMember(
                consumer_group_id=group.id,
                member_id="member-1",
                session_timeout_ms=10_000,
                rebalance_timeout_ms=30_000,
                last_heartbeat_at=now,
                member_generation=1,
                join_generation=1,
                is_in_sync=True,
                assignment=b"",
            )
            session.add(member)

    req = HeartbeatRequest(
        group_id=GroupId("group-instance-set"),
        generation_id=i32(1),
        member_id="member-1",
        group_instance_id="instance-a",
    )
    resp = await do_heartbeat(config, req, api_version=4)

    assert resp.error_code == ErrorCode.none

    async with config.async_session_factory() as session:
        group = (
            await session.execute(
                select(ConsumerGroup).where(ConsumerGroup.group_id == "group-instance-set")
            )
        ).scalar_one()
        refreshed = (
            await session.execute(
                select(GroupMember).where(
                    GroupMember.consumer_group_id == group.id,
                    GroupMember.member_id == "member-1",
                )
            )
        ).scalar_one()
        assert refreshed.group_instance_id == "instance-a"
