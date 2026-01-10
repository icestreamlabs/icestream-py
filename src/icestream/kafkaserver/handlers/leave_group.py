import datetime as dt
from dataclasses import dataclass
from typing import Iterable, Optional

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from kio.schema.errors import ErrorCode
from kio.schema.leave_group.v0.request import (
    LeaveGroupRequest as LeaveGroupRequestV0,
)
from kio.schema.leave_group.v0.request import (
    RequestHeader as LeaveGroupRequestHeaderV0,
)
from kio.schema.leave_group.v0.response import (
    LeaveGroupResponse as LeaveGroupResponseV0,
)
from kio.schema.leave_group.v0.response import (
    ResponseHeader as LeaveGroupResponseHeaderV0,
)
from kio.schema.leave_group.v1.request import (
    LeaveGroupRequest as LeaveGroupRequestV1,
)
from kio.schema.leave_group.v1.request import (
    RequestHeader as LeaveGroupRequestHeaderV1,
)
from kio.schema.leave_group.v1.response import (
    LeaveGroupResponse as LeaveGroupResponseV1,
)
from kio.schema.leave_group.v1.response import (
    ResponseHeader as LeaveGroupResponseHeaderV1,
)
from kio.schema.leave_group.v2.request import (
    LeaveGroupRequest as LeaveGroupRequestV2,
)
from kio.schema.leave_group.v2.request import (
    RequestHeader as LeaveGroupRequestHeaderV2,
)
from kio.schema.leave_group.v2.response import (
    LeaveGroupResponse as LeaveGroupResponseV2,
)
from kio.schema.leave_group.v2.response import (
    ResponseHeader as LeaveGroupResponseHeaderV2,
)
from kio.schema.leave_group.v3.request import (
    LeaveGroupRequest as LeaveGroupRequestV3,
)
from kio.schema.leave_group.v3.request import (
    RequestHeader as LeaveGroupRequestHeaderV3,
)
from kio.schema.leave_group.v3.response import (
    LeaveGroupResponse as LeaveGroupResponseV3,
)
from kio.schema.leave_group.v3.response import (
    ResponseHeader as LeaveGroupResponseHeaderV3,
)
from kio.schema.leave_group.v4.request import (
    LeaveGroupRequest as LeaveGroupRequestV4,
)
from kio.schema.leave_group.v4.request import (
    RequestHeader as LeaveGroupRequestHeaderV4,
)
from kio.schema.leave_group.v4.response import (
    LeaveGroupResponse as LeaveGroupResponseV4,
)
from kio.schema.leave_group.v4.response import (
    ResponseHeader as LeaveGroupResponseHeaderV4,
)
from kio.schema.leave_group.v5.request import (
    LeaveGroupRequest as LeaveGroupRequestV5,
)
from kio.schema.leave_group.v5.request import (
    RequestHeader as LeaveGroupRequestHeaderV5,
)
from kio.schema.leave_group.v5.response import (
    LeaveGroupResponse as LeaveGroupResponseV5,
)
from kio.schema.leave_group.v5.response import (
    ResponseHeader as LeaveGroupResponseHeaderV5,
)
import kio.schema.leave_group.v5.response as resp_v5
import kio.schema.leave_group.v4.response as resp_v4
import kio.schema.leave_group.v3.response as resp_v3
import kio.schema.leave_group.v2.response as resp_v2
import kio.schema.leave_group.v1.response as resp_v1
import kio.schema.leave_group.v0.response as resp_v0

from icestream.config import Config
from icestream.models.consumer_groups import ConsumerGroup, GroupMember
from icestream.utils import zero_throttle


LeaveGroupRequestHeader = (
    LeaveGroupRequestHeaderV0
    | LeaveGroupRequestHeaderV1
    | LeaveGroupRequestHeaderV2
    | LeaveGroupRequestHeaderV3
    | LeaveGroupRequestHeaderV4
    | LeaveGroupRequestHeaderV5
)

LeaveGroupResponseHeader = (
    LeaveGroupResponseHeaderV0
    | LeaveGroupResponseHeaderV1
    | LeaveGroupResponseHeaderV2
    | LeaveGroupResponseHeaderV3
    | LeaveGroupResponseHeaderV4
    | LeaveGroupResponseHeaderV5
)

LeaveGroupRequest = (
    LeaveGroupRequestV0
    | LeaveGroupRequestV1
    | LeaveGroupRequestV2
    | LeaveGroupRequestV3
    | LeaveGroupRequestV4
    | LeaveGroupRequestV5
)

LeaveGroupResponse = (
    LeaveGroupResponseV0
    | LeaveGroupResponseV1
    | LeaveGroupResponseV2
    | LeaveGroupResponseV3
    | LeaveGroupResponseV4
    | LeaveGroupResponseV5
)


UNKNOWN_MEMBER_ID = "UNKNOWN_MEMBER_ID"
REQUEST_HARD_CAP_MS = 120_000


@dataclass(slots=True)
class _LeaveMember:
    member_id: str
    group_instance_id: Optional[str]
    reason: Optional[str]


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _to_aware_utc(ts: dt.datetime | None) -> dt.datetime | None:
    if ts is None:
        return None
    if ts.tzinfo is None:
        return ts.replace(tzinfo=dt.timezone.utc)
    return ts.astimezone(dt.timezone.utc)


def _is_unknown_member_id(member_id: str | None) -> bool:
    return member_id in (None, "", UNKNOWN_MEMBER_ID)


def _min_rebalance_timeout_ms(
    members: Iterable[GroupMember],
    default_ms: int,
) -> int:
    candidates = [m.rebalance_timeout_ms for m in members]
    return min(candidates, default=default_ms)


def _shorten_join_deadline(
    group: ConsumerGroup,
    members: list[GroupMember],
    now: dt.datetime,
) -> None:
    if not members:
        return
    active_rb_ms = min(
        _min_rebalance_timeout_ms(members, group.rebalance_timeout_ms),
        REQUEST_HARD_CAP_MS,
    )
    recomputed_deadline = now + dt.timedelta(milliseconds=active_rb_ms)
    current_deadline = _to_aware_utc(group.join_phase_deadline_at)
    if current_deadline is None or recomputed_deadline < current_deadline:
        group.join_phase_deadline_at = recomputed_deadline


def _normalize_members(
    req: LeaveGroupRequest,
    api_version: int,
) -> list[_LeaveMember]:
    if api_version <= 2:
        return [_LeaveMember(member_id=req.member_id, group_instance_id=None, reason=None)]
    return [
        _LeaveMember(
            member_id=m.member_id,
            group_instance_id=getattr(m, "group_instance_id", None),
            reason=getattr(m, "reason", None),
        )
        for m in req.members
    ]


def _member_response(
    member_id: str,
    group_instance_id: Optional[str],
    error_code: ErrorCode,
) -> resp_v5.MemberResponse:
    return resp_v5.MemberResponse(
        member_id=member_id,
        group_instance_id=group_instance_id,
        error_code=error_code,
    )


def _ladder_leave_group_response(
    resp: LeaveGroupResponse,
    api_version: int,
) -> LeaveGroupResponse:
    if api_version >= 5 or not isinstance(resp, resp_v5.LeaveGroupResponse):
        return resp

    if api_version == 4:
        members = tuple(
            resp_v4.MemberResponse(
                member_id=m.member_id,
                group_instance_id=m.group_instance_id,
                error_code=m.error_code,
            )
            for m in resp.members
        )
        return resp_v4.LeaveGroupResponse(
            throttle_time=resp.throttle_time,
            error_code=resp.error_code,
            members=members,
        )

    if api_version == 3:
        members = tuple(
            resp_v3.MemberResponse(
                member_id=m.member_id,
                group_instance_id=m.group_instance_id,
                error_code=m.error_code,
            )
            for m in resp.members
        )
        return resp_v3.LeaveGroupResponse(
            throttle_time=resp.throttle_time,
            error_code=resp.error_code,
            members=members,
        )

    if api_version == 2:
        return resp_v2.LeaveGroupResponse(
            throttle_time=resp.throttle_time,
            error_code=resp.error_code,
        )

    if api_version == 1:
        return resp_v1.LeaveGroupResponse(
            throttle_time=resp.throttle_time,
            error_code=resp.error_code,
        )

    return resp_v0.LeaveGroupResponse(error_code=resp.error_code)


async def _load_members_for_update(
    session: AsyncSession,
    group_id: int,
) -> list[GroupMember]:
    result = await session.execute(
        select(GroupMember)
        .where(GroupMember.consumer_group_id == group_id)
        .with_for_update()
    )
    return list(result.scalars())


async def do_leave_group(
    config: Config,
    req: LeaveGroupRequest,
    api_version: int,
) -> LeaveGroupResponse:
    assert config.async_session_factory is not None

    members = _normalize_members(req, api_version)
    if not members:
        resp = resp_v5.LeaveGroupResponse(
            throttle_time=zero_throttle(),
            error_code=ErrorCode.invalid_request,
            members=tuple(),
        )
        return _ladder_leave_group_response(resp, api_version)

    async with config.async_session_factory() as session:
        async with session.begin():
            group = (
                await session.execute(
                    select(ConsumerGroup)
                    .where(ConsumerGroup.group_id == req.group_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()

            if group is None:
                if api_version <= 2:
                    resp = resp_v5.LeaveGroupResponse(
                        throttle_time=zero_throttle(),
                        error_code=ErrorCode.unknown_member_id,
                        members=tuple(),
                    )
                    return _ladder_leave_group_response(resp, api_version)
                member_responses = tuple(
                    _member_response(
                        m.member_id,
                        m.group_instance_id,
                        ErrorCode.unknown_member_id,
                    )
                    for m in members
                )
                resp = resp_v5.LeaveGroupResponse(
                    throttle_time=zero_throttle(),
                    error_code=ErrorCode.none,
                    members=member_responses,
                )
                return _ladder_leave_group_response(resp, api_version)

            group_members = await _load_members_for_update(session, group.id)
            by_member_id = {m.member_id: m for m in group_members}
            by_instance_id = {
                m.group_instance_id: m
                for m in group_members
                if m.group_instance_id
            }

            responses: list[resp_v5.MemberResponse] = []
            removed_ids: list[int] = []
            removed_member_ids: set[str] = set()

            for entry in members:
                member_id = entry.member_id
                instance_id = entry.group_instance_id
                if instance_id:
                    member = by_instance_id.get(instance_id)
                    if member is None:
                        responses.append(
                            _member_response(member_id, instance_id, ErrorCode.unknown_member_id)
                        )
                        continue
                    if not _is_unknown_member_id(member_id) and member.member_id != member_id:
                        responses.append(
                            _member_response(member_id, instance_id, ErrorCode.fenced_instance_id)
                        )
                        continue
                    if member.id not in removed_ids:
                        removed_ids.append(member.id)
                        removed_member_ids.add(member.member_id)
                        by_member_id.pop(member.member_id, None)
                        if member.group_instance_id:
                            by_instance_id.pop(member.group_instance_id, None)
                    responses.append(_member_response(member_id, instance_id, ErrorCode.none))
                    continue

                if _is_unknown_member_id(member_id):
                    responses.append(
                        _member_response(member_id, instance_id, ErrorCode.unknown_member_id)
                    )
                    continue

                member = by_member_id.get(member_id)
                if member is None:
                    responses.append(
                        _member_response(member_id, instance_id, ErrorCode.unknown_member_id)
                    )
                    continue

                if member.id not in removed_ids:
                    removed_ids.append(member.id)
                    removed_member_ids.add(member.member_id)
                    by_member_id.pop(member.member_id, None)
                    if member.group_instance_id:
                        by_instance_id.pop(member.group_instance_id, None)

                responses.append(_member_response(member_id, instance_id, ErrorCode.none))

            if removed_ids:
                await session.execute(
                    delete(GroupMember).where(GroupMember.id.in_(removed_ids))
                )

                remaining = [m for m in group_members if m.id not in removed_ids]
                now = _now()

                if not remaining:
                    group.state = "Empty"
                    group.selected_protocol = None
                    group.leader_member_id = None
                    group.join_phase_deadline_at = None
                    group.sync_phase_deadline_at = None
                    group.state_version += 1
                    group.generation = group.generation + 1
                else:
                    if group.leader_member_id in removed_member_ids:
                        group.leader_member_id = None

                    if group.state in ("Stable", "CompletingRebalance", "Empty"):
                        group.state = "PreparingRebalance"
                    group.sync_phase_deadline_at = None
                    _shorten_join_deadline(group, remaining, now)
                    group.state_version += 1

            if api_version <= 2:
                error_code = responses[0].error_code if responses else ErrorCode.none
                resp = resp_v5.LeaveGroupResponse(
                    throttle_time=zero_throttle(),
                    error_code=error_code,
                    members=tuple(),
                )
            else:
                resp = resp_v5.LeaveGroupResponse(
                    throttle_time=zero_throttle(),
                    error_code=ErrorCode.none,
                    members=tuple(responses),
                )

            return _ladder_leave_group_response(resp, api_version)
