import asyncio
import datetime as dt
import hashlib
import uuid
from dataclasses import dataclass
from enum import Enum, auto
from typing import Iterable, Tuple, Optional, List

from kio.schema.errors import ErrorCode
from kio.schema.join_group.v0.request import JoinGroupRequest as JoinGroupRequestV0
from kio.schema.join_group.v0.request import (
    RequestHeader as JoinGroupRequestHeaderV0,
)
from kio.schema.join_group.v0.response import (
    JoinGroupResponse as JoinGroupResponseV0,
)
from kio.schema.join_group.v0.response import (
    ResponseHeader as JoinGroupResponseHeaderV0,
)
from kio.schema.join_group.v1.request import JoinGroupRequest as JoinGroupRequestV1
from kio.schema.join_group.v1.request import (
    RequestHeader as JoinGroupRequestHeaderV1,
)
from kio.schema.join_group.v1.response import (
    JoinGroupResponse as JoinGroupResponseV1,
)
from kio.schema.join_group.v1.response import (
    ResponseHeader as JoinGroupResponseHeaderV1,
)
from kio.schema.join_group.v2.request import JoinGroupRequest as JoinGroupRequestV2
from kio.schema.join_group.v2.request import (
    RequestHeader as JoinGroupRequestHeaderV2,
)
from kio.schema.join_group.v2.response import (
    JoinGroupResponse as JoinGroupResponseV2,
)
from kio.schema.join_group.v2.response import (
    ResponseHeader as JoinGroupResponseHeaderV2,
)
from kio.schema.join_group.v3.request import JoinGroupRequest as JoinGroupRequestV3
from kio.schema.join_group.v3.request import (
    RequestHeader as JoinGroupRequestHeaderV3,
)
from kio.schema.join_group.v3.response import (
    JoinGroupResponse as JoinGroupResponseV3,
)
from kio.schema.join_group.v3.response import (
    ResponseHeader as JoinGroupResponseHeaderV3,
)
from kio.schema.join_group.v4.request import JoinGroupRequest as JoinGroupRequestV4
from kio.schema.join_group.v4.request import (
    RequestHeader as JoinGroupRequestHeaderV4,
)
from kio.schema.join_group.v4.response import (
    JoinGroupResponse as JoinGroupResponseV4,
)
from kio.schema.join_group.v4.response import (
    ResponseHeader as JoinGroupResponseHeaderV4,
)
from kio.schema.join_group.v5.request import JoinGroupRequest as JoinGroupRequestV5
from kio.schema.join_group.v5.request import (
    RequestHeader as JoinGroupRequestHeaderV5,
)
from kio.schema.join_group.v5.response import (
    JoinGroupResponse as JoinGroupResponseV5,
)
from kio.schema.join_group.v5.response import (
    ResponseHeader as JoinGroupResponseHeaderV5,
)
from kio.schema.join_group.v6.request import JoinGroupRequest as JoinGroupRequestV6
from kio.schema.join_group.v6.request import (
    RequestHeader as JoinGroupRequestHeaderV6,
)
from kio.schema.join_group.v6.response import (
    JoinGroupResponse as JoinGroupResponseV6,
)
from kio.schema.join_group.v6.response import (
    ResponseHeader as JoinGroupResponseHeaderV6,
)
from kio.schema.join_group.v7.request import JoinGroupRequest as JoinGroupRequestV7
from kio.schema.join_group.v7.request import (
    RequestHeader as JoinGroupRequestHeaderV7,
)
from kio.schema.join_group.v7.response import (
    JoinGroupResponse as JoinGroupResponseV7,
)
from kio.schema.join_group.v7.response import (
    ResponseHeader as JoinGroupResponseHeaderV7,
)
from kio.schema.join_group.v8.request import JoinGroupRequest as JoinGroupRequestV8
from kio.schema.join_group.v8.request import (
    RequestHeader as JoinGroupRequestHeaderV8,
)
from kio.schema.join_group.v8.response import (
    JoinGroupResponse as JoinGroupResponseV8,
)
from kio.schema.join_group.v8.response import (
    ResponseHeader as JoinGroupResponseHeaderV8,
)
from kio.schema.join_group.v9.request import JoinGroupRequest as JoinGroupRequestV9, JoinGroupRequestProtocol
from kio.schema.join_group.v9.request import (
    RequestHeader as JoinGroupRequestHeaderV9,
)
from kio.schema.join_group.v9.response import (
    JoinGroupResponse as JoinGroupResponseV9,
)
from kio.schema.join_group.v9.response import (
    ResponseHeader as JoinGroupResponseHeaderV9,
)
import kio.schema.join_group.v9.response as resp_v9
import kio.schema.join_group.v8.response as resp_v8
import kio.schema.join_group.v7.response as resp_v7
import kio.schema.join_group.v6.response as resp_v6
import kio.schema.join_group.v5.response as resp_v5
import kio.schema.join_group.v4.response as resp_v4
import kio.schema.join_group.v3.response as resp_v3
import kio.schema.join_group.v2.response as resp_v2
import kio.schema.join_group.v1.response as resp_v1
import kio.schema.join_group.v0.response as resp_v0
from kio.schema.types import GroupId
from kio.static.primitive import i32
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete, and_, update

from icestream.config import Config
from icestream.models.consumer_groups import GroupMemberProtocol, GroupMember, ConsumerGroup
from icestream.utils import zero_throttle

# type unions

JoinGroupRequestHeader = (
    JoinGroupRequestHeaderV0
    | JoinGroupRequestHeaderV1
    | JoinGroupRequestHeaderV2
    | JoinGroupRequestHeaderV3
    | JoinGroupRequestHeaderV4
    | JoinGroupRequestHeaderV5
    | JoinGroupRequestHeaderV6
    | JoinGroupRequestHeaderV7
    | JoinGroupRequestHeaderV8
    | JoinGroupRequestHeaderV9
)

JoinGroupResponseHeader = (
    JoinGroupResponseHeaderV0
    | JoinGroupResponseHeaderV1
    | JoinGroupResponseHeaderV2
    | JoinGroupResponseHeaderV3
    | JoinGroupResponseHeaderV4
    | JoinGroupResponseHeaderV5
    | JoinGroupResponseHeaderV6
    | JoinGroupResponseHeaderV7
    | JoinGroupResponseHeaderV8
    | JoinGroupResponseHeaderV9
)

JoinGroupRequest = (
    JoinGroupRequestV0
    | JoinGroupRequestV1
    | JoinGroupRequestV2
    | JoinGroupRequestV3
    | JoinGroupRequestV4
    | JoinGroupRequestV5
    | JoinGroupRequestV6
    | JoinGroupRequestV7
    | JoinGroupRequestV8
    | JoinGroupRequestV9
)

JoinGroupResponse = (
    JoinGroupResponseV0
    | JoinGroupResponseV1
    | JoinGroupResponseV2
    | JoinGroupResponseV3
    | JoinGroupResponseV4
    | JoinGroupResponseV5
    | JoinGroupResponseV6
    | JoinGroupResponseV7
    | JoinGroupResponseV8
    | JoinGroupResponseV9
)

# constants

JOIN_POLL_INTERVAL_MS = 50  # recheck interval during join window
REQUEST_HARD_CAP_MS = 120_000  # absolute cap per request so we never hang


# original helper functions

def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _ms_to_td(ms: int) -> dt.timedelta:
    return dt.timedelta(milliseconds=max(0, int(ms)))


def _td_to_ms(td) -> int:
    try:
        return int(td)  # i32timedelta supports int(...)
    except Exception:
        if isinstance(td, dt.timedelta):
            return int(td.total_seconds() * 1000)
        return int(td or 0)


def _to_aware_utc(ts: dt.datetime | None) -> dt.datetime | None:
    if ts is None:
        return None
    if ts.tzinfo is None:
        # treat db naive timestamps as utc
        return ts.replace(tzinfo=dt.timezone.utc)
    return ts.astimezone(dt.timezone.utc)


def _new_dynamic_member_id() -> str:
    return f"icestream-{uuid.uuid4()}"


def _static_member_id(group_id: str, instance_id: str) -> str:
    return f"{group_id}:{instance_id}"


def _valid_timeouts(config: Config, session_timeout_ms: int, rebalance_timeout_ms: int) -> bool:
    if session_timeout_ms < config.GROUP_MIN_SESSION_TIMEOUT_MS:
        return False
    if session_timeout_ms > config.GROUP_MAX_SESSION_TIMEOUT_MS:
        return False
    if rebalance_timeout_ms < session_timeout_ms:
        return False
    if rebalance_timeout_ms > config.GROUP_MAX_REBALANCE_TIMEOUT_MS:
        return False
    return True


def _digest_protocols(items: Iterable[Tuple[str, bytes, int]]) -> str:
    # deterministic digest over (name, metadata, preference) triples to detect material changes
    h = hashlib.sha256()
    for name, meta, rank in items:
        safe_meta = meta or b""
        h.update(rank.to_bytes(4, byteorder="big", signed=True))
        h.update(name.encode("utf-8"))
        h.update(b"\x00")
        h.update(safe_meta)
        h.update(b"\xff")
    return h.hexdigest()


async def _load_protocols(session: AsyncSession, member_pk: int) -> list[tuple[str, bytes, int]]:
    q = (
        select(
            GroupMemberProtocol.name,
            GroupMemberProtocol.protocol_metadata,
            GroupMemberProtocol.preference_rank,
        )
        .where(GroupMemberProtocol.group_member_id == member_pk)
        .order_by(GroupMemberProtocol.preference_rank, GroupMemberProtocol.name)
    )
    return list((await session.execute(q)).all())


async def _replace_protocols_return_hash(
    session: AsyncSession,
    member: GroupMember,
    protos: Tuple[JoinGroupRequestProtocol, ...],
) -> tuple[str, str]:
    # replace a member's protocol rows iff content changed
    # returns (old_hash, new_hash)
    new_rows = [(p.name, p.metadata, idx) for idx, p in enumerate(protos)]
    new_hash = _digest_protocols(new_rows)

    old_rows = await _load_protocols(session, member.id)
    old_hash = _digest_protocols(old_rows)

    if new_hash != old_hash:
        await session.execute(
            delete(GroupMemberProtocol).where(GroupMemberProtocol.group_member_id == member.id)
        )
        if new_rows:
            for n, m, rank in new_rows:
                session.add(
                    GroupMemberProtocol(
                        group_member_id=member.id,
                        name=n,
                        protocol_metadata=m or b"",
                        preference_rank=rank,
                    )
                )
                await session.flush()
    member.protocols_hash = new_hash
    return old_hash, new_hash


def _member_expired(m: GroupMember, now: dt.datetime) -> bool:
    hb = _to_aware_utc(m.last_heartbeat_at)
    elapsed_ms = int((now - hb).total_seconds() * 1000)
    return elapsed_ms > max(1, int(m.session_timeout_ms))


def _min_active_rebalance_timeout_ms(members: list[GroupMember], default_ms: int) -> int:
    if not members:
        return default_ms
    return min((m.rebalance_timeout_ms for m in members), default=default_ms)


async def _select_protocol_with_votes(
    session: AsyncSession,
    member_pks: list[int],
) -> Optional[str]:
    # determine selected protocol via majority voting
    if not member_pks:
        return None

    rows = (
        await session.execute(
            select(
                GroupMemberProtocol.group_member_id,
                GroupMemberProtocol.name,
                GroupMemberProtocol.preference_rank,
            ).where(GroupMemberProtocol.group_member_id.in_(member_pks))
        )
    ).all()

    if not rows:
        return None

    per_member: dict[int, list[tuple[int, str]]] = {}
    for member_id, name, rank in rows:
        per_member.setdefault(member_id, []).append((rank, name))

    if len(per_member) != len(member_pks):
        return None  # some member missing protocols

    intersection: Optional[set[str]] = None
    for entries in per_member.values():
        names = {name for _, name in entries}
        intersection = names if intersection is None else (intersection & names)
        if not intersection:
            return None

    votes: dict[str, int] = {proto: 0 for proto in intersection}
    total_rank: dict[str, int] = {proto: 0 for proto in intersection}

    for entries in per_member.values():
        filtered = sorted(((rank, name) for rank, name in entries if name in intersection), key=lambda x: x[0])
        if not filtered:
            return None
        best_name = filtered[0][1]
        votes[best_name] += 1
        for rank, name in filtered:
            total_rank[name] += rank

    max_votes = max(votes.values())
    vote_winners = [name for name, count in votes.items() if count == max_votes]
    if len(vote_winners) == 1:
        return vote_winners[0]

    min_rank = min(total_rank[name] for name in vote_winners)
    rank_winners = [name for name in vote_winners if total_rank[name] == min_rank]
    return sorted(rank_winners)[0]


@dataclass
class _Phase1Result:
    # result of the initial registration phase
    # an error response if validation failed
    response: Optional[JoinGroupResponse] = None
    # the member id assigned to this request
    assigned_member_id: str = ""
    # true if this request arrived *after* a join deadline was already set
    request_past_join_deadline: bool = False


class _TickStatus(Enum):
    # status returned from a single long-poll tick
    CONTINUE = auto()  # poll loop should continue
    BREAK = auto()  # rebalance finished, break loop and go to phase 3
    RETURN = auto()  # return this response to the client immediately


@dataclass
class _TickResult:
    # the result of a single transactional poll-loop tick
    status: _TickStatus
    response: Optional[JoinGroupResponse] = None


@dataclass
class _MemberDetails:
    # wrapper for member determination logic
    member: Optional[GroupMember] = None
    assigned_member_id: str = ""
    error_response: Optional[JoinGroupResponse] = None


# new refactored helper functions

async def _find_or_create_group(session: AsyncSession, group_id: GroupId) -> ConsumerGroup:
    # loads a group by id, creating it if it doesn't exist
    group = (
        await session.execute(
            select(ConsumerGroup)
            .where(ConsumerGroup.group_id == group_id)
            .with_for_update()
        )
    ).scalar_one_or_none()

    if group is None:
        group = ConsumerGroup(group_id=group_id)
        session.add(group)
        await session.flush()
    return group


def _build_inconsistent_protocol_error(
    group: ConsumerGroup,
    assigned_member_id: str,
) -> JoinGroupResponse:
    # builds a v9 inconsistent_group_protocol error response
    return resp_v9.JoinGroupResponse(
        throttle_time=zero_throttle(),
        error_code=ErrorCode.inconsistent_group_protocol,
        generation_id=i32(-1),
        protocol_type=group.protocol_type,
        protocol_name=None,
        leader="",
        skip_assignment=True,
        member_id=assigned_member_id,
        members=tuple(),
    )


def _build_invalid_session_timeout_error(
    group: ConsumerGroup,
    assigned_member_id: str,
) -> JoinGroupResponse:
    return resp_v9.JoinGroupResponse(
        throttle_time=zero_throttle(),
        error_code=ErrorCode.invalid_session_timeout,
        generation_id=i32(-1),
        protocol_type=group.protocol_type,
        protocol_name=None,
        leader="",
        skip_assignment=True,
        member_id=assigned_member_id,
        members=tuple(),
    )


def _build_member_id_required_response(
    group: ConsumerGroup,
    assigned_member_id: str,
) -> JoinGroupResponse:
    return resp_v9.JoinGroupResponse(
        throttle_time=zero_throttle(),
        error_code=ErrorCode.member_id_required,
        generation_id=i32(-1),
        protocol_type=group.protocol_type,
        protocol_name=None,
        leader="",
        skip_assignment=True,
        member_id=assigned_member_id,
        members=tuple(),
    )


async def _determine_member(
    session: AsyncSession,
    group: ConsumerGroup,
    req: JoinGroupRequest,
    now: dt.datetime,
) -> _MemberDetails:
    # encapsulates the complex logic of finding/assigning a member id
    # and checking for fencing or unknown members
    instance_id = getattr(req, "group_instance_id", None)
    member: Optional[GroupMember] = None

    if instance_id:
        # static member
        assigned_member_id = _static_member_id(req.group_id, instance_id)
        member = (
            await session.execute(
                select(GroupMember).where(
                    GroupMember.consumer_group_id == group.id,
                    GroupMember.group_instance_id == instance_id,
                )
            )
        ).scalar_one_or_none()

        if member is not None:
            # found static member: check for fencing
            # fence if this is a "new" join (member_id="") and the member isn't expired
            if req.member_id == "" and not _member_expired(member, now):
                return _MemberDetails(
                    error_response=resp_v9.JoinGroupResponse(
                        throttle_time=zero_throttle(),
                        error_code=ErrorCode.fenced_instance_id,
                        generation_id=i32(group.generation),
                        protocol_type=group.protocol_type,
                        protocol_name=None,
                        leader="",
                        skip_assignment=True,
                        member_id="",
                        members=tuple(),
                    )
                )
            # not fenced, use this member
            assigned_member_id = member.member_id
        else:
            # no existing static member found
            # if v0-style member_id was provided, try to find that
            if req.member_id:
                assigned_member_id = req.member_id
                member = (await session.execute(
                    select(GroupMember).where(
                        GroupMember.consumer_group_id == group.id,
                        GroupMember.member_id == assigned_member_id,
                    )
                )).scalar_one_or_none()
            # else: we will create a new static member using the static id

    elif req.member_id:
        # dynamic member (v0-style re-join)
        assigned_member_id = req.member_id
        member = (
            await session.execute(
                select(GroupMember).where(
                    GroupMember.consumer_group_id == group.id,
                    GroupMember.member_id == assigned_member_id,
                )
            )
        ).scalar_one_or_none()

        if member is None:
            # dynamic member re-joining with an unknown id
            return _MemberDetails(
                assigned_member_id=assigned_member_id,
                error_response=resp_v9.JoinGroupResponse(
                    throttle_time=zero_throttle(),
                    error_code=ErrorCode.unknown_member_id,
                    generation_id=i32(group.generation),
                    protocol_type=group.protocol_type,
                    protocol_name=None,
                    leader="",
                    skip_assignment=True,
                    member_id=assigned_member_id,
                    members=tuple(),
                )
            )
    else:
        # new dynamic member
        assigned_member_id = _new_dynamic_member_id()
        member = None

    return _MemberDetails(member=member, assigned_member_id=assigned_member_id)


async def _update_member_state(
    session: AsyncSession,
    group: ConsumerGroup,
    member: Optional[GroupMember],
    req: JoinGroupRequest,
    assigned_member_id: str,
    now: dt.datetime,
) -> tuple[GroupMember, bool, bool, bool, bool]:
    # updates an existing member or creates a new one based on the joingroup request
    #
    # returns: (member_instance, is_new_member, protocols_changed, timeouts_changed, already_joined_target)
    is_new_member = member is None
    changed_protocols = False
    changed_timeouts = False
    already_joined_target = False

    if is_new_member:
        member = GroupMember(
            consumer_group_id=group.id,
            member_id=assigned_member_id,
            group_instance_id=getattr(req, "group_instance_id", None),
            session_timeout_ms=_td_to_ms(req.session_timeout) or group.session_timeout_ms,
            rebalance_timeout_ms=_td_to_ms(getattr(req, "rebalance_timeout", 0)) or group.rebalance_timeout_ms,
            last_heartbeat_at=now,
            is_in_sync=False,
            member_generation=group.generation,
            join_generation=group.generation + 1,
            assignment=None,
        )
        session.add(member)
        await session.flush()
        old_hash = None
    else:
        # this is an existing member re-joining
        old_hash = member.protocols_hash
        old_session_timeout_ms = member.session_timeout_ms
        old_rebalance_timeout_ms = member.rebalance_timeout_ms
        already_joined_target = member.join_generation >= (group.generation + 1)

        new_session_timeout_ms = _td_to_ms(req.session_timeout)
        new_rebalance_timeout_ms = _td_to_ms(getattr(req, "rebalance_timeout", 0))

        member.group_instance_id = req.group_instance_id or member.group_instance_id
        member.session_timeout_ms = new_session_timeout_ms or old_session_timeout_ms
        member.rebalance_timeout_ms = new_rebalance_timeout_ms or old_rebalance_timeout_ms

        changed_timeouts = (
            member.session_timeout_ms != old_session_timeout_ms
            or member.rebalance_timeout_ms != old_rebalance_timeout_ms
        )

        member.last_heartbeat_at = now
        member.is_in_sync = False
        member.member_generation = group.generation
        member.join_generation = group.generation + 1
        member.assignment = None

    # update protocol rows and compute change
    _, new_hash = await _replace_protocols_return_hash(session, member, req.protocols)
    changed_protocols = is_new_member or (old_hash != new_hash)

    return member, is_new_member, changed_protocols, changed_timeouts, already_joined_target


async def _expire_dead_members(
    session: AsyncSession,
    group: ConsumerGroup,
    members: List[GroupMember],
    now: dt.datetime,
) -> List[GroupMember]:
    # expires dead members, triggers rebalance, and shortens deadline if needed
    expired = [m for m in members if _member_expired(m, now)]
    if not expired:
        return members

    await session.execute(
        delete(GroupMember).where(
            and_(
                GroupMember.consumer_group_id == group.id,
                GroupMember.id.in_([m.id for m in expired]),
            )
        )
    )

    active_members = [m for m in members if m not in expired]

    group.state_version += 1

    if group.state in ("Empty", "Stable"):
        group.state = "PreparingRebalance"

    # recompute min window and possibly shorten
    active_rb_ms = min(
        _min_active_rebalance_timeout_ms(active_members, group.rebalance_timeout_ms),
        REQUEST_HARD_CAP_MS
    )
    recomputed_deadline = now + _ms_to_td(active_rb_ms)

    current_deadline = _to_aware_utc(group.join_phase_deadline_at)
    if current_deadline is None or recomputed_deadline < current_deadline:
        group.join_phase_deadline_at = recomputed_deadline

    return active_members


async def _finalize_rebalance(
    session: AsyncSession,
    group: ConsumerGroup,
    members: List[GroupMember],
    assigned_member_id: str,
    sync_deadline_at: dt.datetime,
) -> Optional[JoinGroupResponse]:
    # called by the leader to finalize the rebalance:
    # 1. selects protocol
    # 2. elects leader
    # 3. bumps group generation
    # 4. updates all member generations
    #
    # returns an error response on failure, none on success
    member_pks = [m.id for m in members]
    selected_protocol = await _select_protocol_with_votes(session, member_pks)

    if not selected_protocol:
        return _build_inconsistent_protocol_error(group, assigned_member_id)

    # elect leader
    prior_leader_id = group.leader_member_id or ""
    member_ids = [m.member_id for m in members if m.member_id]
    if prior_leader_id and prior_leader_id in member_ids:
        leader_member_id = prior_leader_id
    else:
        leader_member_id = (
            sorted(member_ids)[0] if member_ids else assigned_member_id
        )

    # bump generation & reset
    group.generation = group.generation + 1
    group.selected_protocol = selected_protocol
    group.leader_member_id = leader_member_id
    group.state = "CompletingRebalance"
    group.join_phase_deadline_at = None
    group.sync_phase_deadline_at = sync_deadline_at
    group.state_version += 1

    # set all members to the new generation, pending sync
    await session.execute(
        update(GroupMember)
        .where(GroupMember.consumer_group_id == group.id)
        .values(
            is_in_sync=False,
            member_generation=group.generation,
            assignment=None,
        )
    )
    return None


async def _fetch_leader_metadata(
    session: AsyncSession,
    group: ConsumerGroup
) -> Tuple[resp_v9.JoinGroupResponseMember, ...]:
    # fetches all member metadata for the leader's response
    selected = group.selected_protocol or ""
    rows = (await session.execute(
        select(
            GroupMember.member_id,
            GroupMember.group_instance_id,
            GroupMemberProtocol.protocol_metadata,
        )
        .join(
            GroupMemberProtocol,
            and_(
                GroupMemberProtocol.group_member_id == GroupMember.id,
                GroupMemberProtocol.name == selected,
            ),
        )
        .where(GroupMember.consumer_group_id == group.id)
    )).all()

    return tuple(
        resp_v9.JoinGroupResponseMember(
            member_id=mid,
            group_instance_id=inst,
            metadata=meta or b"",
        ) for (mid, inst, meta) in rows
    )


async def _phase_1_handle_registration(
    config: Config,
    req: JoinGroupRequest,
    api_version: int,
    request_started: dt.datetime,
) -> _Phase1Result:
    # phase 1: register/refresh member and potentially open a join window
    # runs in a single transaction
    # returns a phase1result, which may contain an error response for early exit
    assert config.async_session_factory is not None

    async with config.async_session_factory() as session:
        async with session.begin():
            now = _now()
            group = await _find_or_create_group(session, req.group_id)

            # --- 1. determine member id and handle fencing/unknown ---
            member_details = await _determine_member(session, group, req, now)
            if member_details.error_response:
                return _Phase1Result(response=member_details.error_response)

            member = member_details.member
            assigned_member_id = member_details.assigned_member_id

            # --- 2. validate protocols ---
            if not req.protocols:
                return _Phase1Result(
                    response=_build_inconsistent_protocol_error(group, assigned_member_id)
                )

            if group.protocol_type is None:
                group.protocol_type = req.protocol_type
            elif group.protocol_type != req.protocol_type:
                return _Phase1Result(
                    response=_build_inconsistent_protocol_error(group, assigned_member_id)
                )

            # --- 3. validate timeouts ---
            requested_session_ms = _td_to_ms(req.session_timeout)
            if requested_session_ms == 0:
                requested_session_ms = (
                    member.session_timeout_ms if member is not None else group.session_timeout_ms
                )
            requested_rebalance_ms = _td_to_ms(getattr(req, "rebalance_timeout", 0))
            if requested_rebalance_ms == 0:
                requested_rebalance_ms = (
                    member.rebalance_timeout_ms if member is not None else group.rebalance_timeout_ms
                )
            if not _valid_timeouts(config, requested_session_ms, requested_rebalance_ms):
                return _Phase1Result(
                    response=_build_invalid_session_timeout_error(group, assigned_member_id)
                )

            # --- 4. MEMBER_ID_REQUIRED handshake (v4+) ---
            if api_version >= 4 and req.member_id == "":
                if member is None:
                    member = GroupMember(
                        consumer_group_id=group.id,
                        member_id=assigned_member_id,
                        group_instance_id=getattr(req, "group_instance_id", None),
                        session_timeout_ms=requested_session_ms,
                        rebalance_timeout_ms=requested_rebalance_ms,
                        last_heartbeat_at=now,
                        is_in_sync=False,
                        member_generation=group.generation,
                        join_generation=group.generation,
                        assignment=None,
                    )
                    session.add(member)
                    await session.flush()
                else:
                    member.session_timeout_ms = requested_session_ms or member.session_timeout_ms
                    member.rebalance_timeout_ms = requested_rebalance_ms or member.rebalance_timeout_ms
                    member.last_heartbeat_at = now

                await _replace_protocols_return_hash(session, member, req.protocols)
                group.state_version += 1
                return _Phase1Result(
                    response=_build_member_id_required_response(group, assigned_member_id)
                )

            # --- 5. skip assignment for static leader rejoin (KIP-814) ---
            if (
                group.state == "Stable"
                and member is not None
                and member.group_instance_id is not None
                and (group.leader_member_id or "") == member.member_id
                and group.selected_protocol is not None
            ):
                new_hash = _digest_protocols(
                    [(p.name, p.metadata, idx) for idx, p in enumerate(req.protocols)]
                )
                protocols_changed = member.protocols_hash != new_hash
                timeouts_changed = (
                    (requested_session_ms and requested_session_ms != member.session_timeout_ms)
                    or (requested_rebalance_ms and requested_rebalance_ms != member.rebalance_timeout_ms)
                )

                all_members = list(
                    (await session.execute(
                        select(GroupMember).where(GroupMember.consumer_group_id == group.id)
                    )).scalars()
                )
                any_expired = any(_member_expired(m, now) for m in all_members)
                all_assigned = all(
                    (not _member_expired(m, now)) and m.assignment is not None for m in all_members
                )

                if not protocols_changed and not timeouts_changed and not any_expired and all_assigned:
                    member.last_heartbeat_at = now
                    member.session_timeout_ms = requested_session_ms or member.session_timeout_ms
                    member.rebalance_timeout_ms = requested_rebalance_ms or member.rebalance_timeout_ms
                    return _Phase1Result(
                        response=resp_v9.JoinGroupResponse(
                            throttle_time=zero_throttle(),
                            error_code=ErrorCode.none,
                            generation_id=i32(group.generation),
                            protocol_type=group.protocol_type,
                            protocol_name=group.selected_protocol,
                            leader=group.leader_member_id or "",
                            skip_assignment=True,
                            member_id=assigned_member_id,
                            members=tuple(),
                        )
                    )

            # --- 6. create or update member state ---
            member, is_new, protocols_changed, timeouts_changed, already_joined_target = (
                await _update_member_state(
                    session, group, member, req, assigned_member_id, now
                )
            )

            # --- 7. transition group state if needed ---
            needs_rebalance = (
                protocols_changed
                or timeouts_changed
                or is_new
                or not already_joined_target
            )
            mutated = is_new or protocols_changed or timeouts_changed or not already_joined_target
            entered_preparing_now = False
            was_empty = group.state == "Empty"
            if group.state == "CompletingRebalance":
                group.state = "PreparingRebalance"
                entered_preparing_now = True
                group.join_phase_deadline_at = None
                group.sync_phase_deadline_at = None
            elif group.state in ("Empty", "Stable") and needs_rebalance:
                group.state = "PreparingRebalance"
                entered_preparing_now = True
                group.join_phase_deadline_at = None  # will be set below
                group.sync_phase_deadline_at = None

            if mutated:
                group.state_version += 1

            # --- 8. set or check join deadline ---
            all_members = list(
                (await session.execute(
                    select(GroupMember).where(GroupMember.consumer_group_id == group.id)
                )).scalars()
            )
            active = [m for m in all_members if not _member_expired(m, now)]
            min_rb_ms = min(
                _min_active_rebalance_timeout_ms(active, group.rebalance_timeout_ms),
                REQUEST_HARD_CAP_MS
            )
            join_window_ms = min_rb_ms
            if was_empty:
                join_window_ms = max(join_window_ms, config.GROUP_INITIAL_REBALANCE_DELAY_MS)
            join_window_ms = min(join_window_ms, REQUEST_HARD_CAP_MS)
            desired_deadline = now + _ms_to_td(join_window_ms)

            if group.state == "PreparingRebalance":
                if entered_preparing_now or group.join_phase_deadline_at is None:
                    group.join_phase_deadline_at = desired_deadline
                # else: keep existing deadline (no extension)
                if not mutated:
                    group.state_version += 1

            deadline_obj = _to_aware_utc(group.join_phase_deadline_at)
            request_past_join_deadline = False
            if deadline_obj is not None and now > deadline_obj:
                request_past_join_deadline = True

            return _Phase1Result(
                assigned_member_id=assigned_member_id,
                request_past_join_deadline=request_past_join_deadline,
            )


async def _handle_join_group_tick(
    session: AsyncSession,
    group_id: GroupId,
    assigned_member_id: str,
    request_past_join_deadline: bool,
) -> _TickResult:
    # executes a single transactional "tick" of the long-poll loop
    # this function assumes it's running inside an active transaction
    group = (
        await session.execute(
            select(ConsumerGroup)
            .where(ConsumerGroup.group_id == group_id)
            .with_for_update()
        )
    ).scalar_one()

    now = _now()
    members = list(
        (await session.execute(
            select(GroupMember).where(GroupMember.consumer_group_id == group.id)
        )).scalars()
    )

    # expire dead members
    members = await _expire_dead_members(session, group, members, now)

    if not members:
        # group became empty mid-join
        group.state = "Empty"
        group.selected_protocol = None
        group.leader_member_id = None
        group.join_phase_deadline_at = None
        group.sync_phase_deadline_at = None
        group.state_version += 1
        return _TickResult(
            status=_TickStatus.RETURN,
            response=resp_v9.JoinGroupResponse(
                throttle_time=zero_throttle(),
                error_code=ErrorCode.rebalance_in_progress,
                generation_id=i32(-1),
                protocol_type=group.protocol_type,
                protocol_name=None,
                leader="",
                skip_assignment=True,
                member_id=assigned_member_id,
                members=tuple(),
            )
        )

    target_gen = group.generation + 1

    # if any member has joined the next generation, ensure we're preparing
    if group.state not in ("PreparingRebalance", "Empty") and any(
            m.join_generation >= target_gen for m in members):
        group.state = "PreparingRebalance"
        group.sync_phase_deadline_at = None
        if group.join_phase_deadline_at is None:
            active_rb_ms = min(
                _min_active_rebalance_timeout_ms(members, group.rebalance_timeout_ms),
                REQUEST_HARD_CAP_MS
            )
            group.join_phase_deadline_at = now + _ms_to_td(active_rb_ms)
        group.state_version += 1

    all_joined = all(m.join_generation >= target_gen for m in members)
    ddl = _to_aware_utc(group.join_phase_deadline_at)
    deadline_reached = (ddl is not None) and (now >= ddl)

    member_row = next((m for m in members if m.member_id == assigned_member_id), None)

    # handle request timeout if this *specific request* arrived too late
    if deadline_reached and request_past_join_deadline:
        if member_row is not None:
            member_row.join_generation = group.generation
            member_row.member_generation = group.generation
            member_row.is_in_sync = False
        group.join_phase_deadline_at = None
        group.state = "PreparingRebalance"  # force retry
        group.sync_phase_deadline_at = None
        group.state_version += 1
        await session.flush()
        return _TickResult(
            status=_TickStatus.RETURN,
            response=resp_v9.JoinGroupResponse(
                throttle_time=zero_throttle(),
                error_code=ErrorCode.rebalance_in_progress,
                generation_id=i32(-1),
                protocol_type=group.protocol_type,
                protocol_name=None,
                leader=group.leader_member_id or "",
                skip_assignment=True,
                member_id=assigned_member_id,
                members=tuple(),
            )
        )

    # time to finalize the rebalance
    if all_joined or deadline_reached:
        if deadline_reached and not all_joined:
            not_joined = [m for m in members if m.join_generation < target_gen]
            if not_joined:
                await session.execute(
                    delete(GroupMember).where(
                        and_(
                            GroupMember.consumer_group_id == group.id,
                            GroupMember.id.in_([m.id for m in not_joined]),
                        )
                    )
                )
                members = [m for m in members if m.join_generation >= target_gen]
                group.state_version += 1

        if not members:
            group.state = "Empty"
            group.selected_protocol = None
            group.leader_member_id = None
            group.join_phase_deadline_at = None
            group.sync_phase_deadline_at = None
            group.state_version += 1
            return _TickResult(
                status=_TickStatus.RETURN,
                response=resp_v9.JoinGroupResponse(
                    throttle_time=zero_throttle(),
                    error_code=ErrorCode.rebalance_in_progress,
                    generation_id=i32(-1),
                    protocol_type=group.protocol_type,
                    protocol_name=None,
                    leader="",
                    skip_assignment=True,
                    member_id=assigned_member_id,
                    members=tuple(),
                ),
            )

        sync_deadline_at = now + _ms_to_td(
            min(
                _min_active_rebalance_timeout_ms(members, group.rebalance_timeout_ms),
                REQUEST_HARD_CAP_MS,
            )
        )
        error_response = await _finalize_rebalance(
            session, group, members, assigned_member_id, sync_deadline_at
        )
        if error_response:
            return _TickResult(status=_TickStatus.RETURN, response=error_response)

        # success, break the loop
        return _TickResult(status=_TickStatus.BREAK)

    # not finished yet, continue polling
    return _TickResult(status=_TickStatus.CONTINUE)


async def _phase_2_await_rebalance(
    config: Config,
    group_id: GroupId,
    assigned_member_id: str,
    request_started: dt.datetime,
    request_past_join_deadline: bool,
) -> Optional[JoinGroupResponse]:
    # phase 2: long-poll until the rebalance is complete or we time out
    # returns an error/early-exit response if one occurs
    # returns none if the rebalance completed successfully, signaling phase 3
    assert config.async_session_factory is not None

    last_seen_version: Optional[int] = None
    sleep_ms = JOIN_POLL_INTERVAL_MS

    while True:
        now = _now()
        should_finalize = False
        ddl = None
        async with config.async_session_factory() as session:
            group = (
                await session.execute(
                    select(ConsumerGroup).where(ConsumerGroup.group_id == group_id)
                )
            ).scalar_one_or_none()

            if group is None:
                return resp_v9.JoinGroupResponse(
                    throttle_time=zero_throttle(),
                    error_code=ErrorCode.coordinator_load_in_progress,
                    generation_id=i32(-1),
                    protocol_type=None,
                    protocol_name=None,
                    leader="",
                    skip_assignment=True,
                    member_id=assigned_member_id,
                    members=tuple(),
                )

            if group.state == "PreparingRebalance":
                leader_id = group.leader_member_id or ""
                if leader_id and assigned_member_id != leader_id:
                    return resp_v9.JoinGroupResponse(
                        throttle_time=zero_throttle(),
                        error_code=ErrorCode.rebalance_in_progress,
                        generation_id=i32(-1),
                        protocol_type=group.protocol_type,
                        protocol_name=None,
                        leader=leader_id,
                        skip_assignment=True,
                        member_id=assigned_member_id,
                        members=tuple(),
                    )

            if group.state == "CompletingRebalance" or (
                group.state == "Stable" and group.generation > 0
            ):
                return None

            state_changed = last_seen_version is None or group.state_version != last_seen_version
            last_seen_version = group.state_version
            if state_changed:
                sleep_ms = JOIN_POLL_INTERVAL_MS

            ddl = _to_aware_utc(group.join_phase_deadline_at)
            deadline_reached = (ddl is not None) and (now >= ddl)

            if group.state == "PreparingRebalance":
                if deadline_reached or request_past_join_deadline:
                    should_finalize = True
                elif state_changed:
                    members = list(
                        (await session.execute(
                            select(GroupMember).where(GroupMember.consumer_group_id == group.id)
                        )).scalars()
                    )
                    active = [m for m in members if not _member_expired(m, now)]
                    target_gen = group.generation + 1
                    if active and all(m.join_generation >= target_gen for m in active):
                        should_finalize = True

        if should_finalize:
            async with config.async_session_factory() as tick_session:
                async with tick_session.begin():
                    tick_result = await _handle_join_group_tick(
                        tick_session,
                        group_id,
                        assigned_member_id,
                        request_past_join_deadline,
                    )

            if tick_result.status == _TickStatus.RETURN:
                return tick_result.response
            if tick_result.status == _TickStatus.BREAK:
                return None

        # sleep outside txn
        if ddl is not None:
            remaining_ms = max(0, int((ddl - now).total_seconds() * 1000))
            sleep_ms = min(sleep_ms, max(JOIN_POLL_INTERVAL_MS, remaining_ms // 4))
        await asyncio.sleep(sleep_ms / 1000.0)
        sleep_ms = min(int(sleep_ms * 1.5), 1000)

        # hard per-request cap
        if int((_now() - request_started).total_seconds() * 1000) >= REQUEST_HARD_CAP_MS:
            async with config.async_session_factory() as s2:
                g2 = (await s2.execute(
                    select(ConsumerGroup).where(ConsumerGroup.group_id == group_id))
                      ).scalar_one_or_none()
            return resp_v9.JoinGroupResponse(
                throttle_time=zero_throttle(),
                error_code=ErrorCode.coordinator_load_in_progress,
                generation_id=i32(-1),
                protocol_type=(g2.protocol_type if g2 else None),
                protocol_name=None,
                leader="",
                skip_assignment=True,
                member_id=assigned_member_id,
                members=tuple(),
            )


async def _phase_3_build_final_response(
    config: Config,
    group_id: GroupId,
    assigned_member_id: str
) -> JoinGroupResponse:
    # phase 3: build the final success response after the rebalance is complete
    # this is a read-only phase
    assert config.async_session_factory is not None

    async with config.async_session_factory() as session:
        group = (
            await session.execute(
                select(ConsumerGroup).where(ConsumerGroup.group_id == group_id)
            )
        ).scalar_one()

        is_leader = (assigned_member_id == (group.leader_member_id or ""))
        members_tuple = tuple()

        if is_leader:
            members_tuple = await _fetch_leader_metadata(session, group)

        return resp_v9.JoinGroupResponse(
            throttle_time=zero_throttle(),
            error_code=ErrorCode.none,
            generation_id=i32(group.generation),
            protocol_type=group.protocol_type,
            protocol_name=group.selected_protocol,
            leader=group.leader_member_id or "",
            skip_assignment=False,
            member_id=assigned_member_id,
            members=members_tuple,
        )


def _ladder_join_group_response(
    resp: JoinGroupResponse,
    api_version: int,
) -> JoinGroupResponse:
    # down-convert a v9 joingroup response when a client negotiates an older version
    if api_version >= 9 or not isinstance(resp, resp_v9.JoinGroupResponse):
        return resp

    protocol_name = resp.protocol_name or ""

    if api_version == 8:
        members = tuple(
            resp_v8.JoinGroupResponseMember(
                member_id=m.member_id,
                group_instance_id=m.group_instance_id,
                metadata=m.metadata,
            )
            for m in resp.members
        )
        return resp_v8.JoinGroupResponse(
            throttle_time=resp.throttle_time,
            error_code=resp.error_code,
            generation_id=resp.generation_id,
            protocol_type=resp.protocol_type,
            protocol_name=resp.protocol_name,
            leader=resp.leader,
            member_id=resp.member_id,
            members=members,
        )

    if api_version == 7:
        members = tuple(
            resp_v7.JoinGroupResponseMember(
                member_id=m.member_id,
                group_instance_id=m.group_instance_id,
                metadata=m.metadata,
            )
            for m in resp.members
        )
        return resp_v7.JoinGroupResponse(
            throttle_time=resp.throttle_time,
            error_code=resp.error_code,
            generation_id=resp.generation_id,
            protocol_type=resp.protocol_type,
            protocol_name=resp.protocol_name,
            leader=resp.leader,
            member_id=resp.member_id,
            members=members,
        )

    if api_version == 6:
        members = tuple(
            resp_v6.JoinGroupResponseMember(
                member_id=m.member_id,
                group_instance_id=m.group_instance_id,
                metadata=m.metadata,
            )
            for m in resp.members
        )
        return resp_v6.JoinGroupResponse(
            throttle_time=resp.throttle_time,
            error_code=resp.error_code,
            generation_id=resp.generation_id,
            protocol_name=protocol_name,
            leader=resp.leader,
            member_id=resp.member_id,
            members=members,
        )

    if api_version == 5:
        members = tuple(
            resp_v5.JoinGroupResponseMember(
                member_id=m.member_id,
                group_instance_id=m.group_instance_id,
                metadata=m.metadata,
            )
            for m in resp.members
        )
        return resp_v5.JoinGroupResponse(
            throttle_time=resp.throttle_time,
            error_code=resp.error_code,
            generation_id=resp.generation_id,
            protocol_name=protocol_name,
            leader=resp.leader,
            member_id=resp.member_id,
            members=members,
        )

    if api_version == 4:
        members = tuple(
            resp_v4.JoinGroupResponseMember(
                member_id=m.member_id,
                metadata=m.metadata,
            )
            for m in resp.members
        )
        return resp_v4.JoinGroupResponse(
            throttle_time=resp.throttle_time,
            error_code=resp.error_code,
            generation_id=resp.generation_id,
            protocol_name=protocol_name,
            leader=resp.leader,
            member_id=resp.member_id,
            members=members,
        )

    if api_version == 3:
        members = tuple(
            resp_v3.JoinGroupResponseMember(
                member_id=m.member_id,
                metadata=m.metadata,
            )
            for m in resp.members
        )
        return resp_v3.JoinGroupResponse(
            throttle_time=resp.throttle_time,
            error_code=resp.error_code,
            generation_id=resp.generation_id,
            protocol_name=protocol_name,
            leader=resp.leader,
            member_id=resp.member_id,
            members=members,
        )

    if api_version == 2:
        members = tuple(
            resp_v2.JoinGroupResponseMember(
                member_id=m.member_id,
                metadata=m.metadata,
            )
            for m in resp.members
        )
        return resp_v2.JoinGroupResponse(
            throttle_time=resp.throttle_time,
            error_code=resp.error_code,
            generation_id=resp.generation_id,
            protocol_name=protocol_name,
            leader=resp.leader,
            member_id=resp.member_id,
            members=members,
        )

    if api_version == 1:
        members = tuple(
            resp_v1.JoinGroupResponseMember(
                member_id=m.member_id,
                metadata=m.metadata,
            )
            for m in resp.members
        )
        return resp_v1.JoinGroupResponse(
            error_code=resp.error_code,
            generation_id=resp.generation_id,
            protocol_name=protocol_name,
            leader=resp.leader,
            member_id=resp.member_id,
            members=members,
        )

    members = tuple(
        resp_v0.JoinGroupResponseMember(
            member_id=m.member_id,
            metadata=m.metadata,
        )
        for m in resp.members
    )

    return resp_v0.JoinGroupResponse(
        error_code=resp.error_code,
        generation_id=resp.generation_id,
        protocol_name=protocol_name,
        leader=resp.leader,
        member_id=resp.member_id,
        members=members,
    )


async def do_join_group(config, req: JoinGroupRequest, api_version) -> JoinGroupResponse:
    # kafka-compatible joingroup (v9):
    # - protocol selection via intersection (strict)
    # - rebalance on any material change (new member or protocol bytes/names changed)
    # - join window set once per preparing phase (no unbounded extension)
    # - deadline can be shortened after expirations (never extended) to avoid overly long waits
    # - leader gets full membership list for the selected protocol; followers get none
    request_started = _now()

    # phase 1: register/refresh member; maybe open a join window
    p1_result = await _phase_1_handle_registration(config, req, api_version, request_started)

    if p1_result.response:
        return _ladder_join_group_response(p1_result.response, api_version)

    # phase 2: long-poll until we can close or we time out
    p2_response = await _phase_2_await_rebalance(
        config=config,
        group_id=req.group_id,
        assigned_member_id=p1_result.assigned_member_id,
        request_started=request_started,
        request_past_join_deadline=p1_result.request_past_join_deadline,
    )

    if p2_response:
        return _ladder_join_group_response(p2_response, api_version)

    # phase 3: shape response bulk fetch member metadata for leader
    final_response = await _phase_3_build_final_response(
        config=config,
        group_id=req.group_id,
        assigned_member_id=p1_result.assigned_member_id,
    )
    return _ladder_join_group_response(final_response, api_version)
