import asyncio
import datetime
from dataclasses import dataclass
from typing import Optional, Dict, List

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from kio.schema.errors import ErrorCode
from kio.schema.sync_group.v0.request import (
    SyncGroupRequest as SyncGroupRequestV0,
)
from kio.schema.sync_group.v0.request import (
    RequestHeader as SyncGroupRequestHeaderV0,
)
from kio.schema.sync_group.v0.request import (
    SyncGroupRequestAssignment as SyncGroupRequestAssignmentV0,
)
from kio.schema.sync_group.v0.response import (
    SyncGroupResponse as SyncGroupResponseV0,
)
from kio.schema.sync_group.v0.response import (
    ResponseHeader as SyncGroupResponseHeaderV0,
)
from kio.schema.sync_group.v1.request import (
    SyncGroupRequest as SyncGroupRequestV1,
)
from kio.schema.sync_group.v1.request import (
    RequestHeader as SyncGroupRequestHeaderV1,
)
from kio.schema.sync_group.v1.request import (
    SyncGroupRequestAssignment as SyncGroupRequestAssignmentV1,
)
from kio.schema.sync_group.v1.response import (
    SyncGroupResponse as SyncGroupResponseV1,
)
from kio.schema.sync_group.v1.response import (
    ResponseHeader as SyncGroupResponseHeaderV1,
)
from kio.schema.sync_group.v2.request import (
    SyncGroupRequest as SyncGroupRequestV2,
)
from kio.schema.sync_group.v2.request import (
    RequestHeader as SyncGroupRequestHeaderV2,
)
from kio.schema.sync_group.v2.request import (
    SyncGroupRequestAssignment as SyncGroupRequestAssignmentV2,
)
from kio.schema.sync_group.v2.response import (
    SyncGroupResponse as SyncGroupResponseV2,
)
from kio.schema.sync_group.v2.response import (
    ResponseHeader as SyncGroupResponseHeaderV2,
)
from kio.schema.sync_group.v3.request import (
    SyncGroupRequest as SyncGroupRequestV3,
)
from kio.schema.sync_group.v3.request import (
    RequestHeader as SyncGroupRequestHeaderV3,
)
from kio.schema.sync_group.v3.request import (
    SyncGroupRequestAssignment as SyncGroupRequestAssignmentV3,
)
from kio.schema.sync_group.v3.response import (
    SyncGroupResponse as SyncGroupResponseV3,
)
from kio.schema.sync_group.v3.response import (
    ResponseHeader as SyncGroupResponseHeaderV3,
)
from kio.schema.sync_group.v4.request import (
    SyncGroupRequest as SyncGroupRequestV4,
)
from kio.schema.sync_group.v4.request import (
    RequestHeader as SyncGroupRequestHeaderV4,
)
from kio.schema.sync_group.v4.request import (
    SyncGroupRequestAssignment as SyncGroupRequestAssignmentV4,
)
from kio.schema.sync_group.v4.response import (
    SyncGroupResponse as SyncGroupResponseV4,
)
from kio.schema.sync_group.v4.response import (
    ResponseHeader as SyncGroupResponseHeaderV4,
)
from kio.schema.sync_group.v5.request import (
    SyncGroupRequest as SyncGroupRequestV5,
)
from kio.schema.sync_group.v5.request import (
    RequestHeader as SyncGroupRequestHeaderV5,
)
from kio.schema.sync_group.v5.request import (
    SyncGroupRequestAssignment as SyncGroupRequestAssignmentV5,
)
from kio.schema.sync_group.v5.response import (
    SyncGroupResponse as SyncGroupResponseV5,
)
from kio.schema.sync_group.v5.response import (
    ResponseHeader as SyncGroupResponseHeaderV5,
)
import kio.schema.sync_group.v5.response as resp_v5
import kio.schema.sync_group.v4.response as resp_v4
import kio.schema.sync_group.v3.response as resp_v3
import kio.schema.sync_group.v2.response as resp_v2
import kio.schema.sync_group.v1.response as resp_v1
import kio.schema.sync_group.v0.response as resp_v0

from icestream.config import Config
from icestream.models.consumer_groups import ConsumerGroup, GroupMember
from icestream.utils import zero_throttle

SyncGroupRequestHeader = (
    SyncGroupRequestHeaderV0
    | SyncGroupRequestHeaderV1
    | SyncGroupRequestHeaderV2
    | SyncGroupRequestHeaderV3
    | SyncGroupRequestHeaderV4
    | SyncGroupRequestHeaderV5
)

SyncGroupResponseHeader = (
    SyncGroupResponseHeaderV0
    | SyncGroupResponseHeaderV1
    | SyncGroupResponseHeaderV2
    | SyncGroupResponseHeaderV3
    | SyncGroupResponseHeaderV4
    | SyncGroupResponseHeaderV5
)

SyncGroupRequest = (
    SyncGroupRequestV0
    | SyncGroupRequestV1
    | SyncGroupRequestV2
    | SyncGroupRequestV3
    | SyncGroupRequestV4
    | SyncGroupRequestV5
)

SyncGroupResponse = (
    SyncGroupResponseV0
    | SyncGroupResponseV1
    | SyncGroupResponseV2
    | SyncGroupResponseV3
    | SyncGroupResponseV4
    | SyncGroupResponseV5
)

SyncGroupRequestAssignment = (
    SyncGroupRequestAssignmentV0
    | SyncGroupRequestAssignmentV1
    | SyncGroupRequestAssignmentV2
    | SyncGroupRequestAssignmentV3
    | SyncGroupRequestAssignmentV4
    | SyncGroupRequestAssignmentV5
)


# constants & helpers

SYNC_POLL_INTERVAL_MS = 50
REQUEST_HARD_CAP_MS = 120_000


def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _to_aware_utc(ts: datetime.datetime | None) -> datetime.datetime | None:
    if ts is None:
        return None
    if ts.tzinfo is None:
        return ts.replace(tzinfo=datetime.timezone.utc)
    return ts.astimezone(datetime.timezone.utc)


def _member_expired(member: GroupMember, now: datetime.datetime) -> bool:
    last = _to_aware_utc(member.last_heartbeat_at)
    if last is None:
        return True
    elapsed_ms = int((now - last).total_seconds() * 1000)
    return elapsed_ms > max(1, int(member.session_timeout_ms))


async def _load_members_for_update(
    session: AsyncSession,
    group_id: int,
) -> List[GroupMember]:
    result = await session.execute(
        select(GroupMember)
        .where(GroupMember.consumer_group_id == group_id)
        .with_for_update()
    )
    return list(result.scalars())


def _active_members(
    members: List[GroupMember],
    group: ConsumerGroup,
    now: datetime.datetime,
) -> List[GroupMember]:
    return [
        member
        for member in members
        if member.member_generation == group.generation and not _member_expired(member, now)
    ]


def _build_sync_response(
    error_code: ErrorCode,
    protocol_type: Optional[str],
    protocol_name: Optional[str],
    assignment: Optional[bytes] = None,
) -> SyncGroupResponse:
    payload = assignment if assignment is not None else b""
    return resp_v5.SyncGroupResponse(
        throttle_time=zero_throttle(),
        error_code=error_code,
        protocol_type=protocol_type,
        protocol_name=protocol_name,
        assignment=payload,
    )


# phase state

@dataclass(slots=True)
class _SyncContext:
    group_id: str
    member_id: str
    generation: int
    protocol_type: Optional[str]
    protocol_name: Optional[str]


@dataclass(slots=True)
class _Phase1Result:
    response: Optional[SyncGroupResponse] = None
    context: Optional[_SyncContext] = None


@dataclass(slots=True)
class _Phase2Result:
    response: Optional[SyncGroupResponse] = None
    assignment: Optional[bytes] = None
    protocol_type: Optional[str] = None
    protocol_name: Optional[str] = None


# core logic

def _assignments_to_map(assignments: tuple[SyncGroupRequestAssignment, ...]) -> Dict[str, bytes]:
    mapping: Dict[str, bytes] = {}
    for entry in assignments:
        payload = entry.assignment or b""
        if entry.member_id in mapping:
            raise ValueError("duplicate member assignment")
        mapping[entry.member_id] = payload
    return mapping


async def _phase_1_prepare_sync(
    config: Config,
    req: SyncGroupRequest,
) -> _Phase1Result:
    assert config.async_session_factory is not None

    async with config.async_session_factory() as session:
        async with session.begin():
            now = _now()
            group = (
                await session.execute(
                    select(ConsumerGroup)
                    .where(ConsumerGroup.group_id == req.group_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()

            if group is None:
                return _Phase1Result(
                    response=_build_sync_response(ErrorCode.unknown_member_id, None, None)
                )

            deadline = _to_aware_utc(group.sync_phase_deadline_at)
            if deadline is not None and now >= deadline:
                group.state = "PreparingRebalance"
                group.join_phase_deadline_at = None
                group.sync_phase_deadline_at = None
                group.state_version += 1
                await session.execute(
                    update(GroupMember)
                    .where(GroupMember.consumer_group_id == group.id)
                    .values(is_in_sync=False, assignment=None)
                )
                return _Phase1Result(
                    response=_build_sync_response(
                        ErrorCode.rebalance_in_progress,
                        group.protocol_type,
                        group.selected_protocol,
                    )
                )

            req_generation = int(req.generation_id)
            if req_generation != group.generation:
                return _Phase1Result(
                    response=_build_sync_response(
                        ErrorCode.illegal_generation,
                        group.protocol_type,
                        group.selected_protocol,
                    )
                )

            members = await _load_members_for_update(session, group.id)
            member = next((m for m in members if m.member_id == req.member_id), None)

            if member is None:
                return _Phase1Result(
                    response=_build_sync_response(
                        ErrorCode.unknown_member_id,
                        group.protocol_type,
                        group.selected_protocol,
                    )
                )

            if _member_expired(member, now):
                return _Phase1Result(
                    response=_build_sync_response(
                        ErrorCode.unknown_member_id,
                        group.protocol_type,
                        group.selected_protocol,
                    )
                )

            if member.member_generation != group.generation:
                return _Phase1Result(
                    response=_build_sync_response(
                        ErrorCode.illegal_generation,
                        group.protocol_type,
                        group.selected_protocol,
                    )
                )

            if group.state not in ("CompletingRebalance", "Stable"):
                return _Phase1Result(
                    response=_build_sync_response(
                        ErrorCode.rebalance_in_progress,
                        group.protocol_type,
                        group.selected_protocol,
                    )
                )

            req_instance_id = getattr(req, "group_instance_id", None)
            if member.group_instance_id:
                if req_instance_id is None or req_instance_id != member.group_instance_id:
                    return _Phase1Result(
                        response=_build_sync_response(
                            ErrorCode.fenced_instance_id,
                            group.protocol_type,
                            group.selected_protocol,
                        )
                    )
            elif req_instance_id:
                member.group_instance_id = req_instance_id

            req_protocol_type = getattr(req, "protocol_type", None)
            if req_protocol_type is not None:
                if group.protocol_type is None:
                    group.protocol_type = req_protocol_type
                elif group.protocol_type != req_protocol_type:
                    return _Phase1Result(
                        response=_build_sync_response(
                            ErrorCode.inconsistent_group_protocol,
                            group.protocol_type,
                            group.selected_protocol,
                        )
                    )

            req_protocol_name = getattr(req, "protocol_name", None)
            if req_protocol_name is not None:
                if group.selected_protocol is None:
                    group.selected_protocol = req_protocol_name
                elif group.selected_protocol != req_protocol_name:
                    return _Phase1Result(
                        response=_build_sync_response(
                            ErrorCode.inconsistent_group_protocol,
                            group.protocol_type,
                            group.selected_protocol,
                        )
                    )

            leader_id = group.leader_member_id or ""
            is_leader = member.member_id == leader_id

            assignment_map: Dict[str, bytes] = {}
            try:
                assignment_map = _assignments_to_map(req.assignments)
            except ValueError:
                return _Phase1Result(
                    response=_build_sync_response(
                        ErrorCode.inconsistent_group_protocol,
                        group.protocol_type,
                        group.selected_protocol,
                    )
                )

            active_members = _active_members(members, group, now)
            active_ids = {m.member_id for m in active_members}

            if is_leader:
                if active_ids and assignment_map.keys() != active_ids:
                    return _Phase1Result(
                        response=_build_sync_response(
                            ErrorCode.inconsistent_group_protocol,
                            group.protocol_type,
                            group.selected_protocol,
                        )
                    )
                if not active_ids and assignment_map:
                    return _Phase1Result(
                        response=_build_sync_response(
                            ErrorCode.inconsistent_group_protocol,
                            group.protocol_type,
                            group.selected_protocol,
                        )
                    )

                for target in active_members:
                    payload = assignment_map.get(target.member_id, b"")
                    existing = target.assignment
                    if existing is not None and existing != payload:
                        return _Phase1Result(
                            response=_build_sync_response(
                                ErrorCode.inconsistent_group_protocol,
                                group.protocol_type,
                                group.selected_protocol,
                            )
                        )
                    if existing is None:
                        target.assignment = payload
                    if target.member_id == member.member_id:
                        target.is_in_sync = True
                    target.member_generation = group.generation
                    if target.member_id == member.member_id:
                        target.last_heartbeat_at = now

                if not active_members and member.assignment is None:
                    member.assignment = assignment_map.get(member.member_id, b"")
                    member.is_in_sync = True
                    member.last_heartbeat_at = now
            else:
                if assignment_map:
                    return _Phase1Result(
                        response=_build_sync_response(
                            ErrorCode.inconsistent_group_protocol,
                            group.protocol_type,
                            group.selected_protocol,
                        )
                    )
                member.last_heartbeat_at = now
            group.state_version += 1

            context = _SyncContext(
                group_id=req.group_id,
                member_id=member.member_id,
                generation=group.generation,
                protocol_type=group.protocol_type,
                protocol_name=group.selected_protocol,
            )

            return _Phase1Result(context=context)


async def _complete_group_if_ready(
    session: AsyncSession,
    group: ConsumerGroup,
    members: List[GroupMember],
    now: datetime.datetime,
) -> None:
    active = _active_members(members, group, now)
    if active and all(m.is_in_sync for m in active):
        group.state = "Stable"
        group.join_phase_deadline_at = None
        group.sync_phase_deadline_at = None
        group.state_version += 1


async def _phase_2_wait_for_assignment(
    config: Config,
    context: _SyncContext,
    request_started: datetime.datetime,
) -> _Phase2Result:
    assert config.async_session_factory is not None

    last_seen_version: Optional[int] = None
    sleep_ms = SYNC_POLL_INTERVAL_MS

    while True:
        now = _now()
        deadline: datetime.datetime | None = None
        deadline_action: dict[str, int] | None = None
        assignment_action: dict[str, int | str] | None = None
        async with config.async_session_factory() as session:
            group = (
                await session.execute(
                    select(ConsumerGroup).where(ConsumerGroup.group_id == context.group_id)
                )
            ).scalar_one_or_none()

            if group is None:
                return _Phase2Result(
                    response=_build_sync_response(ErrorCode.unknown_member_id, None, None)
                )

            deadline = _to_aware_utc(group.sync_phase_deadline_at)
            if deadline is not None and now >= deadline:
                deadline_action = {"group_id": group.id}
            else:
                if group.generation != context.generation:
                    return _Phase2Result(
                        response=_build_sync_response(
                            ErrorCode.illegal_generation,
                            group.protocol_type,
                            group.selected_protocol,
                        )
                    )

                if group.state not in ("CompletingRebalance", "Stable"):
                    return _Phase2Result(
                        response=_build_sync_response(
                            ErrorCode.rebalance_in_progress,
                            group.protocol_type,
                            group.selected_protocol,
                        )
                    )

                state_changed = last_seen_version is None or group.state_version != last_seen_version
                last_seen_version = group.state_version
                if state_changed:
                    sleep_ms = SYNC_POLL_INTERVAL_MS

                member = (
                    await session.execute(
                        select(GroupMember).where(
                            GroupMember.consumer_group_id == group.id,
                            GroupMember.member_id == context.member_id,
                        )
                    )
                ).scalar_one_or_none()

                if member is None or _member_expired(member, now):
                    return _Phase2Result(
                        response=_build_sync_response(
                            ErrorCode.unknown_member_id,
                            group.protocol_type,
                            group.selected_protocol,
                        )
                    )

                if member.assignment is not None:
                    assignment_action = {"group_id": group.id, "member_id": context.member_id}

        if deadline_action is not None:
            async with config.async_session_factory() as session:
                async with session.begin():
                    locked = (
                        await session.execute(
                            select(ConsumerGroup)
                            .where(ConsumerGroup.id == deadline_action["group_id"])
                            .with_for_update()
                        )
                    ).scalar_one_or_none()
                    if locked is None:
                        return _Phase2Result(
                            response=_build_sync_response(ErrorCode.unknown_member_id, None, None)
                        )
                    locked.state = "PreparingRebalance"
                    locked.join_phase_deadline_at = None
                    locked.sync_phase_deadline_at = None
                    locked.state_version += 1
                    await session.execute(
                        update(GroupMember)
                        .where(GroupMember.consumer_group_id == locked.id)
                        .values(is_in_sync=False, assignment=None)
                    )
                    protocol_type = locked.protocol_type
                    protocol_name = locked.selected_protocol
            return _Phase2Result(
                response=_build_sync_response(
                    ErrorCode.rebalance_in_progress,
                    protocol_type,
                    protocol_name,
                )
            )

        if assignment_action is not None:
            assignment: Optional[bytes] = None
            protocol_type: Optional[str] = None
            protocol_name: Optional[str] = None
            async with config.async_session_factory() as session:
                async with session.begin():
                    locked_group = (
                        await session.execute(
                            select(ConsumerGroup)
                            .where(ConsumerGroup.id == assignment_action["group_id"])
                            .with_for_update()
                        )
                    ).scalar_one_or_none()
                    if locked_group is None:
                        return _Phase2Result(
                            response=_build_sync_response(ErrorCode.unknown_member_id, None, None)
                        )
                    if locked_group.generation != context.generation:
                        return _Phase2Result(
                            response=_build_sync_response(
                                ErrorCode.illegal_generation,
                                locked_group.protocol_type,
                                locked_group.selected_protocol,
                            )
                        )
                    members = await _load_members_for_update(session, locked_group.id)
                    locked_member = next(
                        (m for m in members if m.member_id == assignment_action["member_id"]), None
                    )
                    if locked_member is None or _member_expired(locked_member, now):
                        return _Phase2Result(
                            response=_build_sync_response(
                                ErrorCode.unknown_member_id,
                                locked_group.protocol_type,
                                locked_group.selected_protocol,
                            )
                        )
                    assignment = locked_member.assignment
                    if assignment is not None:
                        if not locked_member.is_in_sync:
                            locked_member.is_in_sync = True
                        locked_member.last_heartbeat_at = now
                        await _complete_group_if_ready(session, locked_group, members, now)
                        protocol_type = locked_group.protocol_type
                        protocol_name = locked_group.selected_protocol

            if assignment is None:
                continue
            return _Phase2Result(
                assignment=assignment,
                protocol_type=protocol_type,
                protocol_name=protocol_name,
            )

        elapsed = int((_now() - request_started).total_seconds() * 1000)
        if elapsed >= REQUEST_HARD_CAP_MS:
            return _Phase2Result(
                response=_build_sync_response(
                    ErrorCode.rebalance_in_progress,
                    context.protocol_type,
                    context.protocol_name,
                )
            )

        if deadline is not None:
            remaining_ms = max(0, int((deadline - now).total_seconds() * 1000))
            sleep_ms = min(sleep_ms, max(SYNC_POLL_INTERVAL_MS, remaining_ms // 4))
        await asyncio.sleep(sleep_ms / 1000.0)
        sleep_ms = min(int(sleep_ms * 1.5), 1000)


def _phase_3_build_response(
    context: _SyncContext,
    assignment: Optional[bytes],
    override_protocol_type: Optional[str],
    override_protocol_name: Optional[str],
) -> SyncGroupResponse:
    protocol_type = override_protocol_type if override_protocol_type is not None else context.protocol_type
    protocol_name = override_protocol_name if override_protocol_name is not None else context.protocol_name
    return _build_sync_response(
        ErrorCode.none,
        protocol_type,
        protocol_name,
        assignment if assignment is not None else b"",
    )


def _ladder_sync_group_response(
    resp: SyncGroupResponse,
    api_version: int,
) -> SyncGroupResponse:
    if api_version >= 5 or not isinstance(resp, resp_v5.SyncGroupResponse):
        return resp

    if api_version == 4:
        return resp_v4.SyncGroupResponse(
            throttle_time=resp.throttle_time,
            error_code=resp.error_code,
            assignment=resp.assignment,
        )

    if api_version == 3:
        return resp_v3.SyncGroupResponse(
            throttle_time=resp.throttle_time,
            error_code=resp.error_code,
            assignment=resp.assignment,
        )

    if api_version == 2:
        return resp_v2.SyncGroupResponse(
            throttle_time=resp.throttle_time,
            error_code=resp.error_code,
            assignment=resp.assignment,
        )

    if api_version == 1:
        return resp_v1.SyncGroupResponse(
            throttle_time=resp.throttle_time,
            error_code=resp.error_code,
            assignment=resp.assignment,
        )

    return resp_v0.SyncGroupResponse(
        error_code=resp.error_code,
        assignment=resp.assignment,
    )


async def do_sync_group(config: Config, req: SyncGroupRequest, api_version: int) -> SyncGroupResponse:
    request_started = _now()

    phase1 = await _phase_1_prepare_sync(config, req)
    if phase1.response:
        return _ladder_sync_group_response(phase1.response, api_version)

    assert phase1.context is not None
    context = phase1.context

    phase2 = await _phase_2_wait_for_assignment(config, context, request_started)
    if phase2.response:
        return _ladder_sync_group_response(phase2.response, api_version)

    assignment = phase2.assignment if phase2.assignment is not None else b""
    response = _phase_3_build_response(
        context=context,
        assignment=assignment,
        override_protocol_type=phase2.protocol_type,
        override_protocol_name=phase2.protocol_name,
    )
    return _ladder_sync_group_response(response, api_version)
