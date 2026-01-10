import datetime
from types import SimpleNamespace

import pytest
from kio.schema.errors import ErrorCode
from kio.schema.sync_group.v5.request import (
    SyncGroupRequest,
    SyncGroupRequestAssignment,
)
import kio.schema.sync_group.v4.response as resp_v4
import kio.schema.sync_group.v3.response as resp_v3
import kio.schema.sync_group.v2.response as resp_v2
import kio.schema.sync_group.v1.response as resp_v1
import kio.schema.sync_group.v0.response as resp_v0
from kio.schema.types import GroupId
from kio.static.primitive import i32
from sqlalchemy import select

from icestream.config import Config
from icestream.kafkaserver.handlers.sync_group import (
    do_sync_group,
    _phase_1_prepare_sync,
    _phase_2_wait_for_assignment,
    _SyncContext,
)
import icestream.kafkaserver.handlers.sync_group as sync_group
from icestream.models.consumer_groups import ConsumerGroup, GroupMember


def _mk_assignment(member_id: str, payload: bytes) -> SyncGroupRequestAssignment:
    return SyncGroupRequestAssignment(member_id=member_id, assignment=payload)


def _mk_request(
    *,
    group_id: str,
    member_id: str,
    generation: int,
    assignments: tuple[SyncGroupRequestAssignment, ...] = (),
    protocol_type: str | None = "consumer",
    protocol_name: str | None = "range",
    group_instance_id: str | None = None,
) -> SyncGroupRequest:
    return SyncGroupRequest(
        group_id=GroupId(group_id),
        generation_id=i32(generation),
        member_id=member_id,
        group_instance_id=group_instance_id,
        protocol_type=protocol_type,
        protocol_name=protocol_name,
        assignments=assignments,
    )


async def _create_group(
    session,
    *,
    group_id: str,
    generation: int = 1,
    state: str = "CompletingRebalance",
    protocol_type: str | None = "consumer",
    protocol_name: str | None = "range",
    leader_member_id: str | None = None,
    sync_deadline_at: datetime.datetime | None = None,
) -> ConsumerGroup:
    group = ConsumerGroup(
        group_id=group_id,
        generation=generation,
        state=state,
        protocol_type=protocol_type,
        selected_protocol=protocol_name,
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
        is_in_sync=False,
        assignment=assignment,
    )
    session.add(member)
    await session.flush()
    return member


def test_sync_group_to_aware_utc_handles_naive_timestamp():
    naive = datetime.datetime(2025, 1, 1, 12, 0, 0)
    aware = sync_group._to_aware_utc(naive)
    assert aware is not None
    assert aware.tzinfo == datetime.timezone.utc


def test_sync_group_member_expired_when_last_heartbeat_missing():
    member = SimpleNamespace(last_heartbeat_at=None, session_timeout_ms=10_000)
    now = datetime.datetime.now(datetime.timezone.utc)
    assert sync_group._member_expired(member, now) is True


@pytest.mark.asyncio
async def test_sync_group_unknown_group(config: Config):
    req = _mk_request(group_id="cg-missing", member_id="m1", generation=0)
    resp = await do_sync_group(config, req, api_version=5)
    assert resp.error_code == ErrorCode.unknown_member_id
    assert resp.assignment == b""


@pytest.mark.asyncio
async def test_sync_group_illegal_generation(config: Config):
    async with config.async_session_factory() as session:
        async with session.begin():
            await _create_group(session, group_id="cg-gen", generation=2)

    req = _mk_request(group_id="cg-gen", member_id="m1", generation=1)
    resp = await do_sync_group(config, req, api_version=5)
    assert resp.error_code == ErrorCode.illegal_generation


@pytest.mark.asyncio
async def test_sync_group_unknown_member(config: Config):
    async with config.async_session_factory() as session:
        async with session.begin():
            await _create_group(session, group_id="cg-unknown-member", generation=1)

    req = _mk_request(group_id="cg-unknown-member", member_id="m1", generation=1)
    resp = await do_sync_group(config, req, api_version=5)
    assert resp.error_code == ErrorCode.unknown_member_id


@pytest.mark.asyncio
async def test_sync_group_fenced_instance_id(config: Config):
    async with config.async_session_factory() as session:
        async with session.begin():
            group = await _create_group(session, group_id="cg-fenced", generation=1)
            await _create_member(
                session,
                group=group,
                member_id="m1",
                generation=1,
                group_instance_id="inst-1",
            )

    req = _mk_request(
        group_id="cg-fenced",
        member_id="m1",
        generation=1,
        group_instance_id="inst-2",
    )
    resp = await do_sync_group(config, req, api_version=5)
    assert resp.error_code == ErrorCode.fenced_instance_id


@pytest.mark.asyncio
async def test_sync_group_expired_member_returns_unknown_member_id(config: Config):
    past = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=30)
    async with config.async_session_factory() as session:
        async with session.begin():
            group = await _create_group(
                session, group_id="cg-expired", generation=1, leader_member_id="leader"
            )
            await _create_member(
                session, group=group, member_id="leader", generation=1, last_heartbeat_at=past
            )

    req = _mk_request(group_id="cg-expired", member_id="leader", generation=1)
    resp = await do_sync_group(config, req, api_version=5)
    assert resp.error_code == ErrorCode.unknown_member_id


@pytest.mark.asyncio
async def test_sync_group_protocol_type_mismatch(config: Config):
    async with config.async_session_factory() as session:
        async with session.begin():
            group = await _create_group(session, group_id="cg-proto", generation=1)
            await _create_member(session, group=group, member_id="m1", generation=1)

    req = _mk_request(
        group_id="cg-proto",
        member_id="m1",
        generation=1,
        protocol_type="other",
    )
    resp = await do_sync_group(config, req, api_version=5)
    assert resp.error_code == ErrorCode.inconsistent_group_protocol


@pytest.mark.asyncio
async def test_sync_group_protocol_name_mismatch(config: Config):
    async with config.async_session_factory() as session:
        async with session.begin():
            group = await _create_group(session, group_id="cg-proto-name", generation=1)
            await _create_member(session, group=group, member_id="m1", generation=1)

    req = _mk_request(
        group_id="cg-proto-name",
        member_id="m1",
        generation=1,
        protocol_name="sticky",
    )
    resp = await do_sync_group(config, req, api_version=5)
    assert resp.error_code == ErrorCode.inconsistent_group_protocol


@pytest.mark.asyncio
async def test_sync_group_duplicate_assignment_rejected(config: Config):
    async with config.async_session_factory() as session:
        async with session.begin():
            group = await _create_group(
                session, group_id="cg-dup", generation=1, leader_member_id="leader"
            )
            await _create_member(session, group=group, member_id="leader", generation=1)

    assignments = (
        _mk_assignment("leader", b"A"),
        _mk_assignment("leader", b"B"),
    )
    req = _mk_request(
        group_id="cg-dup",
        member_id="leader",
        generation=1,
        assignments=assignments,
    )
    resp = await do_sync_group(config, req, api_version=5)
    assert resp.error_code == ErrorCode.inconsistent_group_protocol


@pytest.mark.asyncio
async def test_sync_group_non_leader_assignments_rejected(config: Config):
    async with config.async_session_factory() as session:
        async with session.begin():
            group = await _create_group(
                session, group_id="cg-nonleader", generation=1, leader_member_id="leader"
            )
            await _create_member(session, group=group, member_id="leader", generation=1)
            await _create_member(session, group=group, member_id="follower", generation=1)

    req = _mk_request(
        group_id="cg-nonleader",
        member_id="follower",
        generation=1,
        assignments=(_mk_assignment("follower", b"F"),),
    )
    resp = await do_sync_group(config, req, api_version=5)
    assert resp.error_code == ErrorCode.inconsistent_group_protocol


@pytest.mark.asyncio
async def test_sync_group_sets_group_instance_id_when_missing(config: Config):
    async with config.async_session_factory() as session:
        async with session.begin():
            group = await _create_group(
                session,
                group_id="cg-instance",
                generation=1,
                leader_member_id="leader",
            )
            await _create_member(session, group=group, member_id="leader", generation=1)

    req = _mk_request(
        group_id="cg-instance",
        member_id="leader",
        generation=1,
        assignments=(_mk_assignment("leader", b"L"),),
        group_instance_id="inst-1",
    )
    resp = await do_sync_group(config, req, api_version=5)
    assert resp.error_code == ErrorCode.none

    async with config.async_session_factory() as session:
        group = (
            await session.execute(
                select(ConsumerGroup).where(ConsumerGroup.group_id == "cg-instance")
            )
        ).scalars().one()
        member = (
            await session.execute(
                select(GroupMember).where(GroupMember.consumer_group_id == group.id)
            )
        ).scalars().one()
        assert member.group_instance_id == "inst-1"


@pytest.mark.asyncio
async def test_sync_group_member_generation_mismatch(config: Config):
    async with config.async_session_factory() as session:
        async with session.begin():
            group = await _create_group(session, group_id="cg-mem-gen", generation=2)
            await _create_member(session, group=group, member_id="m1", generation=1)

    req = _mk_request(group_id="cg-mem-gen", member_id="m1", generation=2)
    resp = await do_sync_group(config, req, api_version=5)
    assert resp.error_code == ErrorCode.illegal_generation


@pytest.mark.asyncio
async def test_sync_group_deadline_expired_triggers_rebalance(config: Config):
    past = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=1)
    async with config.async_session_factory() as session:
        async with session.begin():
            group = await _create_group(
                session,
                group_id="cg-deadline",
                generation=1,
                sync_deadline_at=past,
            )
            await _create_member(
                session, group=group, member_id="leader", generation=1, assignment=b"old"
            )

    req = _mk_request(group_id="cg-deadline", member_id="leader", generation=1)
    resp = await do_sync_group(config, req, api_version=5)
    assert resp.error_code == ErrorCode.rebalance_in_progress

    async with config.async_session_factory() as session:
        group = (
            await session.execute(
                select(ConsumerGroup).where(ConsumerGroup.group_id == "cg-deadline")
            )
        ).scalars().one()
        assert group.state == "PreparingRebalance"
        member = (
            await session.execute(
                select(GroupMember).where(GroupMember.consumer_group_id == group.id)
            )
        ).scalars().one()
        assert member.assignment is None


@pytest.mark.asyncio
async def test_sync_group_leader_and_follower_flow(config: Config):
    leader_id = "leader"
    follower_id = "follower"
    async with config.async_session_factory() as session:
        async with session.begin():
            group = await _create_group(
                session,
                group_id="cg-flow",
                generation=1,
                leader_member_id=leader_id,
            )
            await _create_member(session, group=group, member_id=leader_id, generation=1)
            await _create_member(session, group=group, member_id=follower_id, generation=1)

    leader_req = _mk_request(
        group_id="cg-flow",
        member_id=leader_id,
        generation=1,
        assignments=(
            _mk_assignment(leader_id, b"L"),
            _mk_assignment(follower_id, b"F"),
        ),
    )
    leader_resp = await do_sync_group(config, leader_req, api_version=5)
    assert leader_resp.error_code == ErrorCode.none
    assert leader_resp.assignment == b"L"

    follower_req = _mk_request(
        group_id="cg-flow",
        member_id=follower_id,
        generation=1,
        assignments=(),
    )
    follower_resp = await do_sync_group(config, follower_req, api_version=5)
    assert follower_resp.error_code == ErrorCode.none
    assert follower_resp.assignment == b"F"

    async with config.async_session_factory() as session:
        group = (
            await session.execute(
                select(ConsumerGroup).where(ConsumerGroup.group_id == "cg-flow")
            )
        ).scalars().one()
        assert group.state == "Stable"


@pytest.mark.asyncio
async def test_sync_group_preparing_rebalance_returns_in_progress(config: Config):
    async with config.async_session_factory() as session:
        async with session.begin():
            group = await _create_group(
                session,
                group_id="cg-prep",
                generation=1,
                state="PreparingRebalance",
                leader_member_id="leader",
            )
            await _create_member(session, group=group, member_id="leader", generation=1)

    req = _mk_request(group_id="cg-prep", member_id="leader", generation=1)
    resp = await do_sync_group(config, req, api_version=5)
    assert resp.error_code == ErrorCode.rebalance_in_progress


@pytest.mark.asyncio
async def test_sync_group_sets_protocol_fields_when_missing(config: Config):
    async with config.async_session_factory() as session:
        async with session.begin():
            group = await _create_group(
                session,
                group_id="cg-proto-missing",
                generation=1,
                protocol_type=None,
                protocol_name=None,
                leader_member_id="leader",
            )
            await _create_member(session, group=group, member_id="leader", generation=1)

    req = _mk_request(
        group_id="cg-proto-missing",
        member_id="leader",
        generation=1,
        assignments=(_mk_assignment("leader", b"L"),),
        protocol_type="consumer",
        protocol_name="range",
    )
    resp = await do_sync_group(config, req, api_version=5)
    assert resp.error_code == ErrorCode.none

    async with config.async_session_factory() as session:
        group = (
            await session.execute(
                select(ConsumerGroup).where(ConsumerGroup.group_id == "cg-proto-missing")
            )
        ).scalars().one()
        assert group.protocol_type == "consumer"
        assert group.selected_protocol == "range"


@pytest.mark.asyncio
async def test_sync_group_leader_missing_member_assignment_rejected(config: Config):
    async with config.async_session_factory() as session:
        async with session.begin():
            group = await _create_group(
                session,
                group_id="cg-missing-assignment",
                generation=1,
                leader_member_id="leader",
            )
            await _create_member(session, group=group, member_id="leader", generation=1)
            await _create_member(session, group=group, member_id="follower", generation=1)

    req = _mk_request(
        group_id="cg-missing-assignment",
        member_id="leader",
        generation=1,
        assignments=(_mk_assignment("leader", b"L"),),
    )
    resp = await do_sync_group(config, req, api_version=5)
    assert resp.error_code == ErrorCode.inconsistent_group_protocol


@pytest.mark.asyncio
async def test_sync_group_leader_conflicting_existing_assignment_rejected(config: Config):
    async with config.async_session_factory() as session:
        async with session.begin():
            group = await _create_group(
                session,
                group_id="cg-conflict",
                generation=1,
                leader_member_id="leader",
            )
            await _create_member(
                session, group=group, member_id="leader", generation=1
            )
            await _create_member(
                session,
                group=group,
                member_id="follower",
                generation=1,
                assignment=b"A",
            )

    req = _mk_request(
        group_id="cg-conflict",
        member_id="leader",
        generation=1,
        assignments=(
            _mk_assignment("leader", b"L"),
            _mk_assignment("follower", b"B"),
        ),
    )
    resp = await do_sync_group(config, req, api_version=5)
    assert resp.error_code == ErrorCode.inconsistent_group_protocol


@pytest.mark.asyncio
async def test_sync_group_leader_extra_assignment_rejected(config: Config):
    async with config.async_session_factory() as session:
        async with session.begin():
            group = await _create_group(
                session,
                group_id="cg-extra-assignment",
                generation=1,
                leader_member_id="leader",
            )
            await _create_member(session, group=group, member_id="leader", generation=1)

    req = _mk_request(
        group_id="cg-extra-assignment",
        member_id="leader",
        generation=1,
        assignments=(
            _mk_assignment("leader", b"L"),
            _mk_assignment("ghost", b"G"),
        ),
    )
    resp = await do_sync_group(config, req, api_version=5)
    assert resp.error_code == ErrorCode.inconsistent_group_protocol


@pytest.mark.asyncio
async def test_sync_group_phase2_illegal_generation_after_phase1(config: Config):
    async with config.async_session_factory() as session:
        async with session.begin():
            group = await _create_group(
                session,
                group_id="cg-phase2-gen",
                generation=1,
                leader_member_id="leader",
            )
            await _create_member(session, group=group, member_id="leader", generation=1)
            await _create_member(session, group=group, member_id="follower", generation=1)

    req = _mk_request(group_id="cg-phase2-gen", member_id="follower", generation=1)
    phase1 = await _phase_1_prepare_sync(config, req)
    assert phase1.response is None
    assert phase1.context is not None

    async with config.async_session_factory() as session:
        async with session.begin():
            group = (
                await session.execute(
                    select(ConsumerGroup).where(ConsumerGroup.group_id == "cg-phase2-gen")
                )
            ).scalars().one()
            group.generation = 2

    phase2 = await _phase_2_wait_for_assignment(
        config, phase1.context, sync_group._now()
    )
    assert phase2.response is not None
    assert phase2.response.error_code == ErrorCode.illegal_generation


@pytest.mark.asyncio
async def test_sync_group_phase2_deadline_expired_after_phase1(config: Config):
    now = datetime.datetime.now(datetime.timezone.utc)
    async with config.async_session_factory() as session:
        async with session.begin():
            group = await _create_group(
                session,
                group_id="cg-phase2-deadline",
                generation=1,
                leader_member_id="leader",
                sync_deadline_at=now + datetime.timedelta(seconds=1),
            )
            await _create_member(session, group=group, member_id="leader", generation=1)
            await _create_member(session, group=group, member_id="follower", generation=1)

    req = _mk_request(group_id="cg-phase2-deadline", member_id="follower", generation=1)
    phase1 = await _phase_1_prepare_sync(config, req)
    assert phase1.response is None
    assert phase1.context is not None

    async with config.async_session_factory() as session:
        async with session.begin():
            group = (
                await session.execute(
                    select(ConsumerGroup).where(ConsumerGroup.group_id == "cg-phase2-deadline")
                )
            ).scalars().one()
            group.sync_phase_deadline_at = now - datetime.timedelta(seconds=1)

    phase2 = await _phase_2_wait_for_assignment(
        config, phase1.context, sync_group._now()
    )
    assert phase2.response is not None
    assert phase2.response.error_code == ErrorCode.rebalance_in_progress

    async with config.async_session_factory() as session:
        group = (
            await session.execute(
                select(ConsumerGroup).where(ConsumerGroup.group_id == "cg-phase2-deadline")
            )
        ).scalars().one()
        assert group.state == "PreparingRebalance"


@pytest.mark.asyncio
async def test_sync_group_phase2_unknown_member_after_phase1(config: Config):
    async with config.async_session_factory() as session:
        async with session.begin():
            group = await _create_group(
                session,
                group_id="cg-phase2-missing",
                generation=1,
                leader_member_id="leader",
            )
            await _create_member(session, group=group, member_id="leader", generation=1)
            await _create_member(session, group=group, member_id="follower", generation=1)

    req = _mk_request(group_id="cg-phase2-missing", member_id="follower", generation=1)
    phase1 = await _phase_1_prepare_sync(config, req)
    assert phase1.response is None
    assert phase1.context is not None

    async with config.async_session_factory() as session:
        async with session.begin():
            group = (
                await session.execute(
                    select(ConsumerGroup).where(ConsumerGroup.group_id == "cg-phase2-missing")
                )
            ).scalars().one()
            member = (
                await session.execute(
                    select(GroupMember)
                    .where(GroupMember.consumer_group_id == group.id)
                    .where(GroupMember.member_id == "follower")
                )
            ).scalars().one()
            await session.delete(member)

    phase2 = await _phase_2_wait_for_assignment(
        config, phase1.context, sync_group._now()
    )
    assert phase2.response is not None
    assert phase2.response.error_code == ErrorCode.unknown_member_id


@pytest.mark.asyncio
async def test_sync_group_phase2_unknown_group_returns_error(config: Config):
    context = _SyncContext(
        group_id="cg-unknown-phase2",
        member_id="m1",
        generation=1,
        protocol_type="consumer",
        protocol_name="range",
    )
    phase2 = await _phase_2_wait_for_assignment(
        config, context, sync_group._now()
    )
    assert phase2.response is not None
    assert phase2.response.error_code == ErrorCode.unknown_member_id


@pytest.mark.asyncio
async def test_sync_group_phase2_rebalance_in_progress_state(config: Config):
    async with config.async_session_factory() as session:
        async with session.begin():
            group = await _create_group(
                session,
                group_id="cg-phase2-prep",
                generation=1,
                state="PreparingRebalance",
                leader_member_id="leader",
            )
            await _create_member(session, group=group, member_id="leader", generation=1)
            await _create_member(session, group=group, member_id="follower", generation=1)

    context = _SyncContext(
        group_id="cg-phase2-prep",
        member_id="follower",
        generation=1,
        protocol_type="consumer",
        protocol_name="range",
    )
    phase2 = await _phase_2_wait_for_assignment(
        config, context, sync_group._now()
    )
    assert phase2.response is not None
    assert phase2.response.error_code == ErrorCode.rebalance_in_progress


@pytest.mark.asyncio
async def test_sync_group_phase2_hard_cap_returns_in_progress(config: Config):
    async with config.async_session_factory() as session:
        async with session.begin():
            group = await _create_group(
                session,
                group_id="cg-phase2-cap",
                generation=1,
                leader_member_id="leader",
            )
            await _create_member(session, group=group, member_id="leader", generation=1)
            await _create_member(session, group=group, member_id="follower", generation=1)

    context = _SyncContext(
        group_id="cg-phase2-cap",
        member_id="follower",
        generation=1,
        protocol_type="consumer",
        protocol_name="range",
    )
    started = sync_group._now() - datetime.timedelta(
        milliseconds=sync_group.REQUEST_HARD_CAP_MS + 1
    )
    phase2 = await _phase_2_wait_for_assignment(config, context, started)
    assert phase2.response is not None
    assert phase2.response.error_code == ErrorCode.rebalance_in_progress


@pytest.mark.asyncio
async def test_sync_group_phase2_sleep_path_uses_deadline(config: Config, monkeypatch):
    class _SleepSentinel(Exception):
        pass

    future = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=1)
    async with config.async_session_factory() as session:
        async with session.begin():
            group = await _create_group(
                session,
                group_id="cg-phase2-sleep",
                generation=1,
                leader_member_id="leader",
                sync_deadline_at=future,
            )
            await _create_member(session, group=group, member_id="leader", generation=1)
            await _create_member(session, group=group, member_id="follower", generation=1)

    context = _SyncContext(
        group_id="cg-phase2-sleep",
        member_id="follower",
        generation=1,
        protocol_type="consumer",
        protocol_name="range",
    )

    async def sleep_stub(delay: float) -> None:
        raise _SleepSentinel()

    monkeypatch.setattr(sync_group.asyncio, "sleep", sleep_stub)

    with pytest.raises(_SleepSentinel):
        await _phase_2_wait_for_assignment(config, context, sync_group._now())


def test_sync_group_ladder_response_versions():
    resp = sync_group._build_sync_response(
        ErrorCode.none, "consumer", "range", assignment=b"A"
    )
    assert isinstance(sync_group._ladder_sync_group_response(resp, 4), resp_v4.SyncGroupResponse)
    assert isinstance(sync_group._ladder_sync_group_response(resp, 3), resp_v3.SyncGroupResponse)
    assert isinstance(sync_group._ladder_sync_group_response(resp, 2), resp_v2.SyncGroupResponse)
    assert isinstance(sync_group._ladder_sync_group_response(resp, 1), resp_v1.SyncGroupResponse)
    assert isinstance(sync_group._ladder_sync_group_response(resp, 0), resp_v0.SyncGroupResponse)


@pytest.mark.asyncio
async def test_sync_group_phase2_response_propagates_through_handler(config: Config, monkeypatch):
    async with config.async_session_factory() as session:
        async with session.begin():
            group = await _create_group(
                session,
                group_id="cg-phase2-handler",
                generation=1,
                leader_member_id="leader",
            )
            await _create_member(session, group=group, member_id="leader", generation=1)
            await _create_member(session, group=group, member_id="follower", generation=1)

    monkeypatch.setattr(sync_group, "REQUEST_HARD_CAP_MS", 0)
    req = _mk_request(group_id="cg-phase2-handler", member_id="follower", generation=1)
    resp = await do_sync_group(config, req, api_version=5)
    assert resp.error_code == ErrorCode.rebalance_in_progress
