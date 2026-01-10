from kio.schema.heartbeat.v0.request import HeartbeatRequest as HeartbeatRequestV0
from kio.schema.heartbeat.v0.request import (
    RequestHeader as HeartbeatRequestHeaderV0,
)
from kio.schema.heartbeat.v0.response import (
    HeartbeatResponse as HeartbeatResponseV0,
)
from kio.schema.heartbeat.v0.response import (
    ResponseHeader as HeartbeatResponseHeaderV0,
)
from kio.schema.heartbeat.v1.request import HeartbeatRequest as HeartbeatRequestV1
from kio.schema.heartbeat.v1.request import (
    RequestHeader as HeartbeatRequestHeaderV1,
)
from kio.schema.heartbeat.v1.response import (
    HeartbeatResponse as HeartbeatResponseV1,
)
from kio.schema.heartbeat.v1.response import (
    ResponseHeader as HeartbeatResponseHeaderV1,
)
from kio.schema.heartbeat.v2.request import HeartbeatRequest as HeartbeatRequestV2
from kio.schema.heartbeat.v2.request import (
    RequestHeader as HeartbeatRequestHeaderV2,
)
from kio.schema.heartbeat.v2.response import (
    HeartbeatResponse as HeartbeatResponseV2,
)
from kio.schema.heartbeat.v2.response import (
    ResponseHeader as HeartbeatResponseHeaderV2,
)
from kio.schema.heartbeat.v3.request import HeartbeatRequest as HeartbeatRequestV3
from kio.schema.heartbeat.v3.request import (
    RequestHeader as HeartbeatRequestHeaderV3,
)
from kio.schema.heartbeat.v3.response import (
    HeartbeatResponse as HeartbeatResponseV3,
)
from kio.schema.heartbeat.v3.response import (
    ResponseHeader as HeartbeatResponseHeaderV3,
)
from kio.schema.heartbeat.v4.request import HeartbeatRequest as HeartbeatRequestV4
from kio.schema.heartbeat.v4.request import (
    RequestHeader as HeartbeatRequestHeaderV4,
)
from kio.schema.heartbeat.v4.response import (
    HeartbeatResponse as HeartbeatResponseV4,
)
from kio.schema.heartbeat.v4.response import (
    ResponseHeader as HeartbeatResponseHeaderV4,
)
from kio.schema.errors import ErrorCode
from sqlalchemy import select

from icestream.config import Config
from icestream.kafkaserver.consumer_group_liveness import (
    expire_dead_members,
    reset_after_sync_timeout,
    utc_now,
)
from icestream.models.consumer_groups import ConsumerGroup, GroupMember
from icestream.utils import zero_throttle

HeartbeatRequestHeader = (
    HeartbeatRequestHeaderV0
    | HeartbeatRequestHeaderV1
    | HeartbeatRequestHeaderV2
    | HeartbeatRequestHeaderV3
    | HeartbeatRequestHeaderV4
)

HeartbeatResponseHeader = (
    HeartbeatResponseHeaderV0
    | HeartbeatResponseHeaderV1
    | HeartbeatResponseHeaderV2
    | HeartbeatResponseHeaderV3
    | HeartbeatResponseHeaderV4
)

HeartbeatRequest = (
    HeartbeatRequestV0
    | HeartbeatRequestV1
    | HeartbeatRequestV2
    | HeartbeatRequestV3
    | HeartbeatRequestV4
)

HeartbeatResponse = (
    HeartbeatResponseV0
    | HeartbeatResponseV1
    | HeartbeatResponseV2
    | HeartbeatResponseV3
    | HeartbeatResponseV4
)


def _build_heartbeat_response(error_code: ErrorCode, api_version: int) -> HeartbeatResponse:
    if api_version >= 4:
        return HeartbeatResponseV4(
            throttle_time=zero_throttle(),
            error_code=error_code,
        )
    if api_version == 3:
        return HeartbeatResponseV3(
            throttle_time=zero_throttle(),
            error_code=error_code,
        )
    if api_version == 2:
        return HeartbeatResponseV2(
            throttle_time=zero_throttle(),
            error_code=error_code,
        )
    if api_version == 1:
        return HeartbeatResponseV1(
            throttle_time=zero_throttle(),
            error_code=error_code,
        )
    return HeartbeatResponseV0(error_code=error_code)


async def do_heartbeat(
    config: Config,
    req: HeartbeatRequest,
    api_version: int,
) -> HeartbeatResponse:
    assert config.async_session_factory is not None
    now = utc_now()

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
                return _build_heartbeat_response(ErrorCode.unknown_member_id, api_version)

            if await reset_after_sync_timeout(session, group, now):
                return _build_heartbeat_response(ErrorCode.rebalance_in_progress, api_version)

            members, _ = await expire_dead_members(session, group, now)
            member = next((m for m in members if m.member_id == req.member_id), None)
            if member is None:
                return _build_heartbeat_response(ErrorCode.unknown_member_id, api_version)

            req_generation = int(req.generation_id)
            if req_generation != group.generation:
                return _build_heartbeat_response(ErrorCode.illegal_generation, api_version)

            if member.member_generation != group.generation:
                return _build_heartbeat_response(ErrorCode.illegal_generation, api_version)

            if group.state != "Stable":
                return _build_heartbeat_response(ErrorCode.rebalance_in_progress, api_version)

            req_instance_id = getattr(req, "group_instance_id", None)
            if member.group_instance_id:
                if req_instance_id is None or req_instance_id != member.group_instance_id:
                    return _build_heartbeat_response(ErrorCode.fenced_instance_id, api_version)
            elif req_instance_id:
                member.group_instance_id = req_instance_id

            member.last_heartbeat_at = now
            return _build_heartbeat_response(ErrorCode.none, api_version)
