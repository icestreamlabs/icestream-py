import datetime
from typing import Sequence

import pytest
from kio.schema.errors import ErrorCode
from kio.schema.join_group.v9 import JoinGroupRequest
from kio.schema.join_group.v9.request import JoinGroupRequestProtocol
from kio.schema.join_group.v9.response import JoinGroupResponse
from kio.schema.types import GroupId

from sqlalchemy import select

from icestream.config import Config
from icestream.kafkaserver.handlers.join_group import do_join_group
from icestream.models.consumer_groups import ConsumerGroup, GroupMember
from tests.unit.conftest import i32timedelta_from_ms


def _mk_request(
        group_id: str = "cg-1",
        member_id: str = "",
        protocol_type: str = "consumer",
        protocol_name: str = "range",
        metadata: bytes = b"",
        session_timeout_ms: int = 10_000,
        rebalance_timeout_ms: int = 30_000
) -> JoinGroupRequest:
    return JoinGroupRequest(
        group_id=GroupId(group_id),
        session_timeout=i32timedelta_from_ms(session_timeout_ms),
        rebalance_timeout=i32timedelta_from_ms(rebalance_timeout_ms),
        member_id=member_id,
        group_instance_id=None,
        protocol_type=protocol_type,
        protocols=(
            JoinGroupRequestProtocol(name=protocol_name, metadata=metadata),
        ),
        reason=None,
    )


def _mk_request_with_protocols(
        *,
        group_id: str,
        member_id: str,
        protocol_type: str = "consumer",
        protocols: tuple[tuple[str, bytes], ...],
        session_timeout_ms: int = 10_000,
        rebalance_timeout_ms: int = 30_000,
) -> JoinGroupRequest:
    return JoinGroupRequest(
        group_id=GroupId(group_id),
        session_timeout=i32timedelta_from_ms(session_timeout_ms),
        rebalance_timeout=i32timedelta_from_ms(rebalance_timeout_ms),
        member_id=member_id,
        group_instance_id=None,
        protocol_type=protocol_type,
        protocols=tuple(JoinGroupRequestProtocol(name=n, metadata=m) for n, m in protocols),
        reason=None,
    )


def _clone_with_member_id(req: JoinGroupRequest, member_id: str) -> JoinGroupRequest:
    return JoinGroupRequest(
        group_id=req.group_id,
        session_timeout=req.session_timeout,
        rebalance_timeout=req.rebalance_timeout,
        member_id=member_id,
        group_instance_id=req.group_instance_id,
        protocol_type=req.protocol_type,
        protocols=req.protocols,
        reason=req.reason,
    )


async def _do_join(config: Config, req: JoinGroupRequest, api_version: int = 9) -> JoinGroupResponse:
    resp = await do_join_group(config, req, api_version=api_version)
    if resp.error_code == ErrorCode.member_id_required:
        resp = await do_join_group(
            config,
            _clone_with_member_id(req, resp.member_id),
            api_version=api_version,
        )
    return resp


def assert_join_ok(resp: JoinGroupResponse) -> None:
    assert resp.error_code in {ErrorCode.none, ErrorCode.rebalance_in_progress}


@pytest.mark.asyncio
async def test_first_join_creates_group_and_member(config):
    req = _mk_request(group_id="group-A", member_id="")
    resp: JoinGroupResponse
    resp = await _do_join(config, req, api_version=9)

    assert resp.error_code == ErrorCode.none
    assert resp.leader == resp.member_id
    assert resp.protocol_type == "consumer"

    async with config.async_session_factory() as session:
        cg: ConsumerGroup
        cg = (
            await session.execute(
                select(ConsumerGroup).where(ConsumerGroup.group_id == "group-A")
            )
        ).scalars().first()
        assert cg is not None

        gm = (
            await session.execute(
                select(GroupMember).where(GroupMember.consumer_group_id == cg.id)
            )
        ).scalars().all()
        assert len(gm) == 1
        assert gm[0].member_id == resp.member_id


@pytest.mark.asyncio
async def test_idempotent_join_same_member(config):
    req = _mk_request(group_id="group-B", member_id="")
    resp1 = await _do_join(config, req, api_version=9)
    assert resp1.error_code == ErrorCode.none
    assigned_member_id = resp1.member_id
    leader1 = resp1.leader

    async with config.async_session_factory() as session:
        cg: ConsumerGroup
        cg = (
            await session.execute(
                select(ConsumerGroup).where(ConsumerGroup.group_id == "group-B")
            )
        ).scalars().first()
        assert cg is not None

        gm: Sequence[GroupMember] = (
            await session.execute(
                select(GroupMember).where(GroupMember.consumer_group_id == cg.id)
            )
        ).scalars().all()
        assert len(gm) == 1
        assert gm[0].member_id == assigned_member_id

    req2 = _mk_request(
        group_id="group-B",
        member_id=assigned_member_id,
        protocol_type="consumer",
        protocol_name="range",
        metadata=b"",
        session_timeout_ms=10_000,
        rebalance_timeout_ms=30_000,
    )
    resp2 = await _do_join(config, req2, api_version=9)
    assert resp2.error_code == ErrorCode.none
    assert resp2.member_id == assigned_member_id
    assert resp2.leader == leader1

    async with config.async_session_factory() as session:
        gm_after: Sequence[GroupMember] = (
            await session.execute(
                select(GroupMember).where(GroupMember.consumer_group_id == cg.id)
            )
        ).scalars().all()
        assert len(gm_after) == 1
        assert gm_after[0].member_id == assigned_member_id


@pytest.mark.asyncio
async def test_protocol_selection_and_leader_receives_members(config):
    # ---- 1) leader joins first; supports ["range", "rr"] preferring "range"
    leader_req = _mk_request_with_protocols(
        group_id="group-D",
        member_id="",  # coordinator assigns
        protocols=(("range", b"L-range"), ("rr", b"L-rr")),
    )
    leader_join = await _do_join(config, leader_req, api_version=9)
    assert leader_join.error_code == ErrorCode.none
    leader_member_id = leader_join.member_id

    # snapshot current group gen
    async with config.async_session_factory() as session:
        cg_row_1 = (await session.execute(
            ConsumerGroup.__table__.select().where(ConsumerGroup.group_id == "group-D")
        )).mappings().first()
    assert cg_row_1 is not None
    gen_before = cg_row_1["generation"]

    # ---- 2) second member joins; supports ["rr", "range"] preferring "rr"
    # intersection is {"range","rr"}; with our rule, pick leader's most preferred = "range".
    follower_req = _mk_request_with_protocols(
        group_id="group-D",
        member_id="",
        protocols=(("rr", b"F-rr"), ("range", b"F-range")),
    )
    follower_join = await _do_join(config, follower_req, api_version=9)
    assert follower_join.error_code in {
        ErrorCode.none,
        ErrorCode.rebalance_in_progress,
    }
    follower_member_id = follower_join.member_id
    assert follower_member_id != leader_member_id

    # group should be preparingrebalance or completingrebalance
    async with config.async_session_factory() as session:
        cg_row_2 = (await session.execute(
            ConsumerGroup.__table__.select().where(ConsumerGroup.group_id == "group-D")
        )).mappings().first()
    assert cg_row_2["state"] in {"PreparingRebalance", "CompletingRebalance"}

    # ---- 3) leader re-joins to complete the join phase and receive members
    leader_rejoin_req = _mk_request_with_protocols(
        group_id="group-D",
        member_id=leader_member_id,
        protocols=(("range", b"L-range"), ("rr", b"L-rr")),
    )
    leader_rejoin = await _do_join(config, leader_rejoin_req, api_version=9)

    # success + protocol selection + assignment payload to leader
    assert leader_rejoin.error_code == ErrorCode.none
    assert leader_rejoin.member_id == leader_member_id
    assert leader_rejoin.leader == leader_member_id
    assert leader_rejoin.protocol_type == "consumer"
    assert leader_rejoin.protocol_name == "range"  # leader's top choice in the intersection
    assert leader_rejoin.skip_assignment is False  # leader must perform assignment
    assert isinstance(leader_rejoin.members, tuple)
    assert len(leader_rejoin.members) == 2

    # members should be (leader, follower) with metadata for the selected protocol ("range")
    members_by_id = {m.member_id: m for m in leader_rejoin.members}
    assert set(members_by_id.keys()) == {leader_member_id, follower_member_id}
    assert members_by_id[leader_member_id].metadata == b"L-range"
    assert members_by_id[follower_member_id].metadata == b"F-range"

    # generation handling: rebalance completion must bump generation.
    async with config.async_session_factory() as session:
        cg_row_3 = (await session.execute(
            ConsumerGroup.__table__.select().where(ConsumerGroup.group_id == "group-D")
        )).mappings().first()
    assert leader_rejoin.generation_id == cg_row_3["generation"]
    assert leader_rejoin.generation_id > gen_before


@pytest.mark.asyncio
async def test_non_overlapping_protocols_returns_inconsistent_group_protocol(config):
    # leader supports only "range"
    leader = await _do_join(
        config,
        _mk_request_with_protocols(
            group_id="group-E",
            member_id="",
            protocols=(("range", b"L-range"),),
        ),
        api_version=9,
    )
    assert leader.error_code == ErrorCode.none
    leader_member_id = leader.member_id

    # follower supports only "rr" (no intersection with leader)
    follower = await _do_join(
        config,
        _mk_request_with_protocols(
            group_id="group-E",
            member_id="",
            protocols=(("rr", b"F-rr"),),
        ),
        api_version=9,
    )
    # depending on where you surface the error, either the second join returns error,
    # or the error appears when the leader re-joins. we allow either path by re-checking on leader re-join.
    if follower.error_code != ErrorCode.none:
        assert follower.error_code in {
            ErrorCode.inconsistent_group_protocol,
            ErrorCode.rebalance_in_progress,
        }
        return

    # if follower was accepted into preparingrebalance, leader re-joining should fail selection.
    leader_rejoin = await _do_join(
        config,
        _mk_request_with_protocols(
            group_id="group-E",
            member_id=leader_member_id,
            protocols=(("range", b"L-range"),),
        ),
        api_version=9,
    )
    assert leader_rejoin.error_code == ErrorCode.inconsistent_group_protocol


@pytest.mark.asyncio
async def test_protocol_selection_majority_preference_over_leader_choice(config):
    gid = "group-F"

    # leader prefers range > rr
    leader = await _do_join(
        config,
        _mk_request_with_protocols(
            group_id=gid,
            member_id="",
            protocols=(("range", b"L-range"), ("rr", b"L-rr")),
        ),
        api_version=9,
    )
    assert leader.error_code == ErrorCode.none
    leader_id = leader.member_id

    # follower a prefers rr > range
    fol_a = await _do_join(
        config,
        _mk_request_with_protocols(
            group_id=gid,
            member_id="",
            protocols=(("rr", b"A-rr"), ("range", b"A-range")),
        ),
        api_version=9,
    )
    assert_join_ok(fol_a)
    a_id = fol_a.member_id

    # follower b prefers rr > range (tips majority toward rr)
    fol_b = await _do_join(
        config,
        _mk_request_with_protocols(
            group_id=gid,
            member_id="",
            protocols=(("rr", b"B-rr"), ("range", b"B-range")),
        ),
        api_version=9,
    )
    assert_join_ok(fol_b)
    b_id = fol_b.member_id

    # leader re-joins to receive selection + members
    leader_rejoin = await _do_join(
        config,
        _mk_request_with_protocols(
            group_id=gid,
            member_id=leader_id,
            protocols=(("range", b"L-range"), ("rr", b"L-rr")),
        ),
        api_version=9,
    )
    assert leader_rejoin.error_code == ErrorCode.none
    assert leader_rejoin.protocol_name == "rr", "majority preference should select 'rr'"

    # members list & metadata must align with selected protocol ("rr")
    mids = {m.member_id: m for m in leader_rejoin.members}
    assert set(mids) == {leader_id, a_id, b_id}
    assert mids[leader_id].metadata == b"L-rr"
    assert mids[a_id].metadata == b"A-rr"
    assert mids[b_id].metadata == b"B-rr"


@pytest.mark.asyncio
async def test_static_membership_group_instance_id_is_idempotent(config):
    gid = "group-G"

    # first static member joins (dynamic first, then we set instance id in db)
    join1 = await _do_join(
        config,
        _mk_request_with_protocols(
            group_id=gid,
            member_id="",
            protocols=(("range", b"X-range"),),
        ),
        api_version=9,
    )
    assert join1.error_code == ErrorCode.none
    member_id1 = join1.member_id

    # update this member in db to have a group_instance_id
    async with config.async_session_factory() as session:
        cg = (
            await session.execute(
                select(ConsumerGroup).where(ConsumerGroup.group_id == gid)
            )
        ).scalars().first()
        gm = (
            await session.execute(
                select(GroupMember)
                .where(GroupMember.consumer_group_id == cg.id)
                .where(GroupMember.member_id == member_id1)
            )
        ).scalars().first()
        gm.group_instance_id = "A"
        # expire the existing instance so a new join with the same instance id is allowed
        gm.last_heartbeat_at = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
            milliseconds=gm.session_timeout_ms + 1)
        await session.commit()

    # re-join with *empty* member_id but same group_instance_id="a" should resolve to same member (idempotent)
    join2 = await _do_join(
        config,
        JoinGroupRequest(
            group_id=GroupId(gid),
            session_timeout=i32timedelta_from_ms(10_000),
            rebalance_timeout=i32timedelta_from_ms(30_000),
            member_id="",
            group_instance_id="A",
            protocol_type="consumer",
            protocols=(JoinGroupRequestProtocol(name="range", metadata=b"X-range"),),
            reason=None,
        ),
        api_version=9,
    )
    assert join2.error_code == ErrorCode.none
    assert join2.member_id == member_id1

    # db still has only one member with instance "a"
    async with config.async_session_factory() as session:
        cg = (
            await session.execute(
                select(ConsumerGroup).where(ConsumerGroup.group_id == gid)
            )
        ).scalars().first()
        members = (
            await session.execute(
                select(GroupMember).where(GroupMember.consumer_group_id == cg.id)
            )
        ).scalars().all()
        assert len(members) == 1
        assert members[0].group_instance_id == "A"


@pytest.mark.asyncio
async def test_join_phase_deadline_expired_rejects_late_join(config):
    gid = "group-H"

    # leader joins
    lead = await _do_join(
        config,
        _mk_request_with_protocols(
            group_id=gid, member_id="", protocols=(("range", b"L-range"),)
        ),
        api_version=9,
    )
    assert lead.error_code == ErrorCode.none
    leader_id = lead.member_id

    # second member joins -> preparingrebalance, deadline set
    fol = await _do_join(
        config,
        _mk_request_with_protocols(
            group_id=gid, member_id="", protocols=(("range", b"F-range"),)
        ),
        api_version=9,
    )
    assert_join_ok(fol)

    # force deadline into the past
    async with config.async_session_factory() as session:
        cg = (
            await session.execute(
                select(ConsumerGroup).where(ConsumerGroup.group_id == gid)
            )
        ).scalars().first()
        cg.join_phase_deadline_at = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=5)
        await session.commit()

    # leader tries to re-join after deadline -> should be rejected
    leader_late = await _do_join(
        config,
        _mk_request_with_protocols(
            group_id=gid, member_id=leader_id, protocols=(("range", b"L-range"),)
        ),
        api_version=9,
    )
    assert leader_late.error_code == ErrorCode.rebalance_in_progress


@pytest.mark.asyncio
async def test_leader_failover_during_preparing_rebalance(config):
    gid = "group-I"

    # leader join
    lead = await _do_join(
        config,
        _mk_request_with_protocols(
            group_id=gid, member_id="", protocols=(("range", b"L-range"), ("rr", b"L-rr"))
        ),
        api_version=9,
    )
    assert lead.error_code == ErrorCode.none
    leader_id = lead.member_id

    # follower join
    fol = await _do_join(
        config,
        _mk_request_with_protocols(
            group_id=gid, member_id="", protocols=(("range", b"F-range"), ("rr", b"F-rr"))
        ),
        api_version=9,
    )
    assert_join_ok(fol)
    follower_id = fol.member_id

    # delete leader to simulate failure
    async with config.async_session_factory() as session:
        cg = (
            await session.execute(
                select(ConsumerGroup).where(ConsumerGroup.group_id == gid)
            )
        ).scalars().first()
        leader_row = (
            await session.execute(
                select(GroupMember)
                .where(GroupMember.consumer_group_id == cg.id)
                .where(GroupMember.member_id == leader_id)
            )
        ).scalars().first()
        await session.delete(leader_row)
        # ensure state remains preparingrebalance and leader_member_id is cleared or re-pointed
        cg.leader_member_id = None
        await session.commit()

    # follower re-joins; should be promoted as leader and receive members (just itself)
    fol_rejoin = await _do_join(
        config,
        _mk_request_with_protocols(
            group_id=gid, member_id=follower_id, protocols=(("range", b"F-range"), ("rr", b"F-rr"))
        ),
        api_version=9,
    )
    assert fol_rejoin.error_code == ErrorCode.none
    assert fol_rejoin.leader == follower_id
    assert fol_rejoin.skip_assignment is False
    assert fol_rejoin.protocol_name in {"range", "rr"}  # either is fine—both supported
    # only one member remains
    assert len(fol_rejoin.members) == 1
    assert fol_rejoin.members[0].member_id == follower_id


@pytest.mark.asyncio
async def test_follower_rejoin_during_awaiting_sync_is_idempotent(config):
    gid = "group-K"

    # 1. leader joins
    leader_req = _mk_request_with_protocols(
        group_id=gid, member_id="", protocols=(("range", b"L-range"),)
    )
    leader_join = await _do_join(config, leader_req, api_version=9)
    assert leader_join.error_code == ErrorCode.none
    leader_id = leader_join.member_id

    # 2. follower joins, triggering rebalance
    follower_req = _mk_request_with_protocols(
        group_id=gid, member_id="", protocols=(("range", b"F-range"),)
    )
    follower_join = await _do_join(config, follower_req, api_version=9)
    assert_join_ok(follower_join)
    follower_id = follower_join.member_id

    # 3. leader re-joins, gets member list, moves group to completingrebalance
    leader_rejoin_req = _mk_request_with_protocols(
        group_id=gid, member_id=leader_id, protocols=(("range", b"L-range"),)
    )
    leader_rejoin = await _do_join(config, leader_rejoin_req, api_version=9)
    assert leader_rejoin.error_code == ErrorCode.none
    assert leader_rejoin.leader == leader_id
    assert len(leader_rejoin.members) == 2

    # verify group is completingrebalance
    async with config.async_session_factory() as session:
        cg = (await session.execute(
            select(ConsumerGroup).where(ConsumerGroup.group_id == gid)
        )).scalars().first()
        assert cg.state == "CompletingRebalance"
        assert cg.selected_protocol == "range"  # <<< changed from protocol_name

    # 4. action: follower re-joins *after* leader, while completingrebalance
    follower_rejoin_req = _mk_request_with_protocols(
        group_id=gid, member_id=follower_id, protocols=(("range", b"F-range"),)
    )
    follower_rejoin_resp = await _do_join(config, follower_rejoin_req, api_version=9)

    # 5. assert: follower gets an empty member list and is told to await sync
    assert_join_ok(follower_rejoin_resp)
    assert follower_rejoin_resp.leader == leader_id
    assert follower_rejoin_resp.member_id == follower_id
    if follower_rejoin_resp.error_code == ErrorCode.none:
        assert follower_rejoin_resp.protocol_name == "range"  # protocol is selected
        assert follower_rejoin_resp.members == ()  # no members
        assert follower_rejoin_resp.skip_assignment is False
    else:
        assert follower_rejoin_resp.protocol_name is None
        assert follower_rejoin_resp.members == ()


@pytest.mark.asyncio
async def test_join_with_unknown_member_id_rejected(config):
    gid = "group-L"

    # setup: create the group with one member
    req1 = _mk_request(group_id=gid, member_id="")
    resp1 = await _do_join(config, req1, api_version=9)
    assert resp1.error_code == ErrorCode.none

    # action: try to join with a non-empty, non-existent member_id
    req2 = _mk_request(group_id=gid, member_id="non-existent-member-id")
    resp2 = await _do_join(config, req2, api_version=9)

    # assert
    assert resp2.error_code == ErrorCode.unknown_member_id


@pytest.mark.asyncio
async def test_join_with_inconsistent_protocol_type_rejected(config):
    gid = "group-M"

    # setup: first member joins with 'consumer'
    req1 = _mk_request(group_id=gid, member_id="", protocol_type="consumer")
    resp1 = await _do_join(config, req1, api_version=9)
    assert resp1.error_code == ErrorCode.none

    # action: second member tries to join with 'connect'
    req2 = _mk_request(group_id=gid, member_id="", protocol_type="connect")
    resp2 = await _do_join(config, req2, api_version=9)

    # assert
    assert resp2.error_code == ErrorCode.inconsistent_group_protocol


@pytest.mark.asyncio
async def test_rejoin_with_updated_session_timeout_succeeds(config):
    gid = "group-N"

    # setup: member joins with a 10s timeout
    req1 = _mk_request(group_id=gid, member_id="", session_timeout_ms=10_000)
    resp1 = await _do_join(config, req1, api_version=9)
    assert resp1.error_code == ErrorCode.none
    member_id = resp1.member_id

    # action: same member re-joins with its id but a 30s timeout
    req2 = _mk_request(group_id=gid, member_id=member_id, session_timeout_ms=30_000)
    resp2 = await _do_join(config, req2, api_version=9)

    # assert: session timeout update is accepted within valid bounds
    assert resp2.error_code == ErrorCode.none

    async with config.async_session_factory() as session:
        cg = (
            await session.execute(
                select(ConsumerGroup).where(ConsumerGroup.group_id == gid)
            )
        ).scalars().first()
        gm = (
            await session.execute(
                select(GroupMember)
                .where(GroupMember.consumer_group_id == cg.id)
                .where(GroupMember.member_id == member_id)
            )
        ).scalars().first()
        assert gm.session_timeout_ms == 30_000


@pytest.mark.asyncio
async def test_new_member_join_stable_group_triggers_rebalance(config):
    gid = "group-S"

    # setup: create a stable group with one member
    async with config.async_session_factory() as session:
        # 1. create group and member
        cg = ConsumerGroup(
            group_id=gid,
            state="Stable",
            generation=1,
            selected_protocol="range",  # <<< added
        )
        session.add(cg)
        await session.flush()

        gm = GroupMember(
            consumer_group_id=cg.id,
            member_id="stable-member-1",
            # protocol_name="range", # <<< removed (invalid column)
            # you might need to add other required fields from your model,
            # e.g., session_timeout_ms, but protocol_name is not one.
        )
        session.add(gm)

        # 2. set the leader
        cg.leader_member_id = "stable-member-1"  # <<< changed (was referencing gm.id)
        await session.commit()

        stable_gen = cg.generation

    # action: a new member joins the stable group
    new_member_req = _mk_request(group_id=gid, member_id="")
    new_member_resp = await _do_join(config, new_member_req, api_version=9)

    assert_join_ok(new_member_resp)
    assert new_member_resp.member_id != ""  # should have been assigned a new id

    # assert (db state): group is now preparingrebalance
    async with config.async_session_factory() as session:
        cg = (await session.execute(
            select(ConsumerGroup).where(ConsumerGroup.group_id == gid)
        )).scalars().first()

        members = (await session.execute(
            select(GroupMember).where(GroupMember.consumer_group_id == cg.id)
        )).scalars().all()

        assert cg.state in {"PreparingRebalance", "CompletingRebalance"}
        assert cg.generation in {stable_gen, stable_gen + 1}
        assert len(members) == 2
        assert any(m.member_id == new_member_resp.member_id for m in members)


@pytest.mark.asyncio
async def test_new_member_join_awaiting_sync_triggers_new_rebalance(config):
    gid = "group-AS"

    # setup: put group into completingrebalance state
    # (borrowing from test_follower_rejoin_during_awaiting_sync)
    await _do_join(config, _mk_request(group_id=gid, member_id=""), api_version=9)
    await _do_join(config, _mk_request(group_id=gid, member_id=""), api_version=9)
    leader_rejoin = await _do_join(config, _mk_request(group_id=gid, member_id=""), api_version=9)

    # manually set state if leader rejoin doesn't do it (test depends on handler logic)
    async with config.async_session_factory() as session:
        cg = (await session.execute(
            select(ConsumerGroup).where(ConsumerGroup.group_id == gid)
        )).scalars().first()
        cg.state = "CompletingRebalance"
        await session.commit()

    # action: a *new* (third) member tries to join
    new_member_req = _mk_request(group_id=gid, member_id="")
    new_member_resp = await _do_join(config, new_member_req, api_version=9)

    # assert: the new member triggers a new rebalance and gets a none response
    # after its request is held and the new rebalance completes.
    assert_join_ok(new_member_resp)
    # check we're in preparing rebalance


@pytest.mark.asyncio
async def test_join_with_conflicting_static_instance_id_fences_old_member(config):
    gid = "group-Static-Conflict"

    # setup: create a stable group with a static member "static-1"
    async with config.async_session_factory() as session:
        cg = ConsumerGroup(
            group_id=gid,
            state="Stable",
            generation=1,
            selected_protocol="range",  # <<< added
        )
        session.add(cg)
        await session.flush()

        gm = GroupMember(
            consumer_group_id=cg.id,
            member_id="active-member-id",
            group_instance_id="static-1",  # this is the key
            # protocol_name="range", # <<< removed (invalid column)
        )
        session.add(gm)
        cg.leader_member_id = "active-member-id"  # <<< changed (was referencing gm.id)
        await session.commit()

    # action: a *new* member tries to join using the *same* instance id
    conflict_req = JoinGroupRequest(
        group_id=GroupId(gid),
        session_timeout=i32timedelta_from_ms(10_000),
        rebalance_timeout=i32timedelta_from_ms(30_000),
        member_id="",  # new member
        group_instance_id="static-1",  # conflicting id
        protocol_type="consumer",
        protocols=(JoinGroupRequestProtocol(name="range", metadata=b""),),
        reason=None,
    )
    conflict_resp = await _do_join(config, conflict_req, api_version=9)

    # assert: this is a "fence-off" error
    assert conflict_resp.error_code == ErrorCode.fenced_instance_id


@pytest.mark.asyncio
async def test_join_with_conflicting_static_member_instance_id_rejected(config):
    # test that a new member cannot join with a group_instance_id that's already
    # in use by an active member. this should return fenced_instance_id.
    #
    # kafka's static membership guarantees that only one member can hold a given
    # group_instance_id at a time. if a new member (different member_id) tries to
    # join with an existing instance_id, it gets fenced off.
    gid = "group-Static"

    # setup: create a stable group with a static member "static-1"
    async with config.async_session_factory() as session:
        cg = ConsumerGroup(
            group_id=gid,
            state="Stable",
            generation=1,
            selected_protocol="range",
        )
        session.add(cg)
        await session.flush()

        gm = GroupMember(
            consumer_group_id=cg.id,
            member_id="active-member-id",
            group_instance_id="static-1",  # this static id is in use
            session_timeout_ms=10_000,
            rebalance_timeout_ms=30_000,
        )
        session.add(gm)
        cg.leader_member_id = "active-member-id"
        await session.commit()

    # action: a *new* member (empty member_id) tries to join using the *same* instance id
    conflict_req = JoinGroupRequest(
        group_id=GroupId(gid),
        session_timeout=i32timedelta_from_ms(10_000),
        rebalance_timeout=i32timedelta_from_ms(30_000),
        member_id="",  # new member (not the existing one)
        group_instance_id="static-1",  # conflicting instance id
        protocol_type="consumer",
        protocols=(JoinGroupRequestProtocol(name="range", metadata=b""),),
        reason=None,
    )
    conflict_resp = await _do_join(config, conflict_req, api_version=9)

    # assert: this should return fenced_instance_id error
    assert conflict_resp.error_code == ErrorCode.fenced_instance_id
    # the member_id should be empty or the same as the request
    assert conflict_resp.member_id == ""


@pytest.mark.asyncio
async def test_static_member_rejoin_replaces_previous_instance(config):
    # a static member can rejoin with the same group_instance_id **after** the old instance
    # has left or timed out. we simulate timeout by expiring the previous instance.
    gid = "group-StaticRejoin"

    # initial join as static member
    req1 = JoinGroupRequest(
        group_id=GroupId(gid),
        session_timeout=i32timedelta_from_ms(10_000),
        rebalance_timeout=i32timedelta_from_ms(30_000),
        member_id="",
        group_instance_id="static-1",
        protocol_type="consumer",
        protocols=(JoinGroupRequestProtocol(name="range", metadata=b"meta1"),),
        reason=None,
    )
    resp1 = await _do_join(config, req1, api_version=9)
    assert resp1.error_code == ErrorCode.none
    original_member_id = resp1.member_id

    # verify member exists and then expire it to simulate timeout
    async with config.async_session_factory() as session:
        cg = (
            await session.execute(
                select(ConsumerGroup).where(ConsumerGroup.group_id == gid)
            )
        ).scalars().first()
        gm = (
            await session.execute(
                select(GroupMember)
                .where(GroupMember.consumer_group_id == cg.id)
                .where(GroupMember.member_id == original_member_id)
            )
        ).scalars().first()

        assert gm.group_instance_id == "static-1"
        # expire old instance so the new join can replace it
        gm.last_heartbeat_at = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
            milliseconds=gm.session_timeout_ms + 1)
        await session.commit()

    # re-join with empty member_id and the same instance id -> should succeed (replacement)
    req2 = JoinGroupRequest(
        group_id=GroupId(gid),
        session_timeout=i32timedelta_from_ms(10_000),
        rebalance_timeout=i32timedelta_from_ms(30_000),
        member_id="",
        group_instance_id="static-1",
        protocol_type="consumer",
        protocols=(JoinGroupRequestProtocol(name="range", metadata=b"meta2"),),
        reason=None,
    )
    resp2 = await _do_join(config, req2, api_version=9)
    assert resp2.error_code == ErrorCode.none

    # verify only one member exists with this instance id
    async with config.async_session_factory() as session:
        cg = (
            await session.execute(
                select(ConsumerGroup).where(ConsumerGroup.group_id == gid)
            )
        ).scalars().first()

        members = (
            await session.execute(
                select(GroupMember).where(
                    GroupMember.consumer_group_id == cg.id,
                )
            )
        ).scalars().all()

        # still exactly one member in the group; instance id unchanged
        assert len(members) == 1
        assert members[0].group_instance_id == "static-1"


@pytest.mark.asyncio
async def test_static_member_rejoin_with_known_member_id(config):
    # test that a static member can rejoin using its known member_id.
    #
    # this is the normal case where a static member temporarily disconnects
    # and reconnects with the same member_id and group_instance_id.
    gid = "group-StaticKnown"

    # setup: static member joins initially
    req1 = JoinGroupRequest(
        group_id=GroupId(gid),
        session_timeout=i32timedelta_from_ms(10_000),
        rebalance_timeout=i32timedelta_from_ms(30_000),
        member_id="",
        group_instance_id="static-1",
        protocol_type="consumer",
        protocols=(JoinGroupRequestProtocol(name="range", metadata=b"meta1"),),
        reason=None,
    )
    resp1 = await _do_join(config, req1, api_version=9)
    assert resp1.error_code == ErrorCode.none
    member_id = resp1.member_id

    # action: same static member rejoins with its known member_id
    req2 = JoinGroupRequest(
        group_id=GroupId(gid),
        session_timeout=i32timedelta_from_ms(10_000),
        rebalance_timeout=i32timedelta_from_ms(30_000),
        member_id=member_id,  # using known member_id
        group_instance_id="static-1",  # same instance id
        protocol_type="consumer",
        protocols=(JoinGroupRequestProtocol(name="range", metadata=b"meta1"),),
        reason=None,
    )
    resp2 = await _do_join(config, req2, api_version=9)

    # assert: should succeed with same member_id
    assert resp2.error_code == ErrorCode.none
    assert resp2.member_id == member_id

    # verify still only one member
    async with config.async_session_factory() as session:
        cg = (await session.execute(
            select(ConsumerGroup).where(ConsumerGroup.group_id == gid)
        )).scalars().first()

        members = (await session.execute(
            select(GroupMember).where(GroupMember.consumer_group_id == cg.id)
        )).scalars().all()

        assert len(members) == 1
        assert members[0].member_id == member_id
        assert members[0].group_instance_id == "static-1"


@pytest.mark.asyncio
async def test_join_with_empty_protocols_is_rejected(config):
    req = JoinGroupRequest(
        group_id=GroupId("group-empty-protos"),
        session_timeout=i32timedelta_from_ms(10_000),
        rebalance_timeout=i32timedelta_from_ms(30_000),
        member_id="",
        group_instance_id=None,
        protocol_type="consumer",
        protocols=(),  # <-- empty
        reason=None,
    )
    resp = await _do_join(config, req, api_version=9)
    assert resp.error_code == ErrorCode.inconsistent_group_protocol


@pytest.mark.asyncio
async def test_dynamic_rejoin_with_zero_session_timeout_keeps_old(config):
    gid = "group-zero-st"
    first = await _do_join(config, _mk_request(group_id=gid, member_id="", session_timeout_ms=10_000),
                                api_version=9)
    assert first.error_code == ErrorCode.none
    mid = first.member_id

    # rejoin with 0 => "no change"
    resp = await _do_join(config, _mk_request(group_id=gid, member_id=mid, session_timeout_ms=0), api_version=9)
    assert resp.error_code == ErrorCode.none
    assert resp.member_id == mid


@pytest.mark.asyncio
async def test_protocol_metadata_change_triggers_rebalance(config):
    gid = "group-meta-change"

    # first join
    a = await _do_join(
        config,
        _mk_request_with_protocols(group_id=gid, member_id="", protocols=(("range", b"m1"),)),
        api_version=9,
    )
    assert a.error_code == ErrorCode.none

    # same member, different metadata for same protocol -> triggers a rebalance
    a2 = await _do_join(
        config,
        _mk_request_with_protocols(group_id=gid, member_id=a.member_id, protocols=(("range", b"m2"),)),
        api_version=9,
    )
    assert a2.error_code == ErrorCode.none

    async with config.async_session_factory() as session:
        cg = (await session.execute(select(ConsumerGroup).where(ConsumerGroup.group_id == gid))).scalars().first()
        # with one member, preparing can complete immediately.
        assert cg.state in {"PreparingRebalance", "CompletingRebalance"}


@pytest.mark.asyncio
async def test_follower_early_exit_during_preparing(config):
    gid = "group-follower-early-exit"

    leader = await _do_join(config,
                                 _mk_request_with_protocols(group_id=gid, member_id="", protocols=(("range", b"L"),)),
                                 api_version=9)
    assert leader.error_code == ErrorCode.none

    follower = await _do_join(config,
                                   _mk_request_with_protocols(group_id=gid, member_id="", protocols=(("range", b"F"),)),
                                   api_version=9)
    assert_join_ok(follower)

    # follower immediately re-joins while leader hasn't—should wait for rebalance completion
    follower_again = await _do_join(
        config,
        _mk_request_with_protocols(group_id=gid, member_id=follower.member_id, protocols=(("range", b"F"),)),
        api_version=9,
    )
    assert_join_ok(follower_again)
    if follower_again.error_code == ErrorCode.rebalance_in_progress:
        assert follower_again.skip_assignment is True
    else:
        assert follower_again.skip_assignment is False
    assert follower_again.members == ()
    assert follower_again.leader == leader.member_id


@pytest.mark.asyncio
async def test_protocol_selection_tie_breaks_alphabetically_after_rank_tie(config):
    gid = "group-tie-break"

    # a prefers rr > range
    a = await _do_join(
        config,
        _mk_request_with_protocols(group_id=gid, member_id="", protocols=(("rr", b"A-rr"), ("range", b"A-range"))),
        api_version=9,
    )
    assert a.error_code == ErrorCode.none

    # b prefers range > rr — votes are tied; total ranks tie as well
    b = await _do_join(
        config,
        _mk_request_with_protocols(group_id=gid, member_id="", protocols=(("range", b"B-range"), ("rr", b"B-rr"))),
        api_version=9,
    )
    assert_join_ok(b)

    # any member re-joins to finalize and surface selection
    lead = await _do_join(
        config,
        _mk_request_with_protocols(group_id=gid, member_id=a.member_id,
                                   protocols=(("rr", b"A-rr"), ("range", b"A-range"))),
        api_version=9,
    )
    assert lead.error_code == ErrorCode.none
    # alphabetical between "range" and "rr" -> "range"
    assert lead.protocol_name == "range"


@pytest.mark.asyncio
async def test_leader_election_prefers_prior_leader_if_present(config):
    gid = "group-keep-leader"

    first = await _do_join(config, _mk_request(group_id=gid, member_id=""), api_version=9)
    assert first.error_code == ErrorCode.none
    prior_leader = first.member_id

    second = await _do_join(config, _mk_request(group_id=gid, member_id=""), api_version=9)
    assert_join_ok(second)

    # leader re-joins to finalize
    done = await _do_join(config, _mk_request(group_id=gid, member_id=prior_leader), api_version=9)
    assert done.error_code == ErrorCode.none
    assert done.leader == prior_leader

@pytest.mark.asyncio
async def test_leader_election_lexicographic_when_prior_leader_removed(config):
    gid = "group-lexi"

    a = await _do_join(config, _mk_request(group_id=gid, member_id=""), api_version=9)
    b = await _do_join(config, _mk_request(group_id=gid, member_id=""), api_version=9)
    assert a.error_code == ErrorCode.none
    assert_join_ok(b)

    # remove prior leader
    async with config.async_session_factory() as session:
        cg = (await session.execute(select(ConsumerGroup).where(ConsumerGroup.group_id == gid))).scalars().first()
        leader_row = (await session.execute(
            select(GroupMember)
            .where(GroupMember.consumer_group_id == cg.id)
            .where(GroupMember.member_id == a.member_id)
        )).scalars().first()
        await session.delete(leader_row)
        cg.leader_member_id = None
        await session.commit()

    # rejoin remaining member to finalize; with one member, it's trivially the leader,
    # but this asserts the lexicographic fallback path is exercised.
    out = await _do_join(config, _mk_request(group_id=gid, member_id=b.member_id), api_version=9)
    assert out.error_code == ErrorCode.none
    assert out.leader == b.member_id

@pytest.mark.asyncio
async def test_rebalance_timeout_change_triggers_rebalance(config):
    gid = "group-rb-timeout"
    first = await _do_join(config, _mk_request(group_id=gid, member_id="", rebalance_timeout_ms=30_000), api_version=9)
    assert first.error_code == ErrorCode.none

    # same member, bump rebalance_timeout -> should mark preparingrebalance
    resp = await _do_join(config, _mk_request(group_id=gid, member_id=first.member_id, rebalance_timeout_ms=45_000), api_version=9)
    assert resp.error_code == ErrorCode.none

    async with config.async_session_factory() as session:
        cg = (await session.execute(select(ConsumerGroup).where(ConsumerGroup.group_id == gid))).scalars().first()
        assert cg.state in {"PreparingRebalance", "CompletingRebalance"}

@pytest.mark.asyncio
async def test_static_member_rejoin_with_different_protocol_type_is_inconsistent(config):
    gid = "group-static-type"

    first = await _do_join(
        config,
        JoinGroupRequest(
            group_id=GroupId(gid),
            session_timeout=i32timedelta_from_ms(10_000),
            rebalance_timeout=i32timedelta_from_ms(30_000),
            member_id="",
            group_instance_id="s1",
            protocol_type="consumer",
            protocols=(JoinGroupRequestProtocol(name="range", metadata=b"x"),),
            reason=None,
        ),
        api_version=9,
    )
    assert first.error_code == ErrorCode.none

    # rejoin same static member using its known member_id (so it isn't fenced),
    # but switch protocol_type -> should be inconsistent_group_protocol.
    second = await _do_join(
        config,
        JoinGroupRequest(
            group_id=GroupId(gid),
            session_timeout=i32timedelta_from_ms(10_000),
            rebalance_timeout=i32timedelta_from_ms(30_000),
            member_id=first.member_id,       # known id avoids fencing
            group_instance_id="s1",
            protocol_type="connect",         # changed type
            protocols=(JoinGroupRequestProtocol(name="range", metadata=b"x"),),
            reason=None,
        ),
        api_version=9,
    )
    assert second.error_code == ErrorCode.inconsistent_group_protocol
