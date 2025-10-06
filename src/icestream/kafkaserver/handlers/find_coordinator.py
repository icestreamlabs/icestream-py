from kio.schema.errors import ErrorCode
from kio.schema.find_coordinator.v0.request import (
    FindCoordinatorRequest as FindCoordinatorRequestV0,
)
from kio.schema.find_coordinator.v0.request import (
    RequestHeader as FindCoordinatorRequestHeaderV0,
)
from kio.schema.find_coordinator.v0.response import (
    FindCoordinatorResponse as FindCoordinatorResponseV0,
)
from kio.schema.find_coordinator.v0.response import (
    ResponseHeader as FindCoordinatorResponseHeaderV0,
)
from kio.schema.find_coordinator.v1.request import (
    FindCoordinatorRequest as FindCoordinatorRequestV1,
)
from kio.schema.find_coordinator.v1.request import (
    RequestHeader as FindCoordinatorRequestHeaderV1,
)
from kio.schema.find_coordinator.v1.response import (
    FindCoordinatorResponse as FindCoordinatorResponseV1,
)
from kio.schema.find_coordinator.v1.response import (
    ResponseHeader as FindCoordinatorResponseHeaderV1,
)
from kio.schema.find_coordinator.v2.request import (
    FindCoordinatorRequest as FindCoordinatorRequestV2,
)
from kio.schema.find_coordinator.v2.request import (
    RequestHeader as FindCoordinatorRequestHeaderV2,
)
from kio.schema.find_coordinator.v2.response import (
    FindCoordinatorResponse as FindCoordinatorResponseV2,
)
from kio.schema.find_coordinator.v2.response import (
    ResponseHeader as FindCoordinatorResponseHeaderV2,
)
from kio.schema.find_coordinator.v3.request import (
    FindCoordinatorRequest as FindCoordinatorRequestV3,
)
from kio.schema.find_coordinator.v3.request import (
    RequestHeader as FindCoordinatorRequestHeaderV3,
)
from kio.schema.find_coordinator.v3.response import (
    FindCoordinatorResponse as FindCoordinatorResponseV3,
)
from kio.schema.find_coordinator.v3.response import (
    ResponseHeader as FindCoordinatorResponseHeaderV3,
)
from kio.schema.find_coordinator.v4.request import (
    FindCoordinatorRequest as FindCoordinatorRequestV4,
)
from kio.schema.find_coordinator.v4.request import (
    RequestHeader as FindCoordinatorRequestHeaderV4,
)
from kio.schema.find_coordinator.v4.response import (
    FindCoordinatorResponse as FindCoordinatorResponseV4,
)
from kio.schema.find_coordinator.v4.response import (
    ResponseHeader as FindCoordinatorResponseHeaderV4,
)
from kio.schema.find_coordinator.v5.request import (
    FindCoordinatorRequest as FindCoordinatorRequestV5,
)
from kio.schema.find_coordinator.v5.request import (
    RequestHeader as FindCoordinatorRequestHeaderV5,
)
from kio.schema.find_coordinator.v5.response import (
    FindCoordinatorResponse as FindCoordinatorResponseV5,
)
from kio.schema.find_coordinator.v5.response import (
    ResponseHeader as FindCoordinatorResponseHeaderV5,
)
from kio.schema.find_coordinator.v6.request import (
    FindCoordinatorRequest as FindCoordinatorRequestV6,
)
from kio.schema.find_coordinator.v6.request import (
    RequestHeader as FindCoordinatorRequestHeaderV6,
)
from kio.schema.find_coordinator.v6.response import (
    FindCoordinatorResponse as FindCoordinatorResponseV6,
)
from kio.schema.find_coordinator.v6.response import (
    ResponseHeader as FindCoordinatorResponseHeaderV6,
)

import kio.schema.find_coordinator.v6 as fc_v6
import kio.schema.find_coordinator.v5 as fc_v5
import kio.schema.find_coordinator.v4 as fc_v4
import kio.schema.find_coordinator.v3 as fc_v3
import kio.schema.find_coordinator.v2 as fc_v2
import kio.schema.find_coordinator.v1 as fc_v1
import kio.schema.find_coordinator.v0 as fc_v0

from kio.schema.types import BrokerId
from kio.static.primitive import i32

from icestream.config import Config
from icestream.utils import zero_throttle

FindCoordinatorRequestHeader = (
    FindCoordinatorRequestHeaderV0
    | FindCoordinatorRequestHeaderV1
    | FindCoordinatorRequestHeaderV2
    | FindCoordinatorRequestHeaderV3
    | FindCoordinatorRequestHeaderV4
    | FindCoordinatorRequestHeaderV5
    | FindCoordinatorRequestHeaderV6
)

FindCoordinatorResponseHeader = (
    FindCoordinatorResponseHeaderV0
    | FindCoordinatorResponseHeaderV1
    | FindCoordinatorResponseHeaderV2
    | FindCoordinatorResponseHeaderV3
    | FindCoordinatorResponseHeaderV4
    | FindCoordinatorResponseHeaderV5
    | FindCoordinatorResponseHeaderV6
)

FindCoordinatorRequest = (
    FindCoordinatorRequestV0
    | FindCoordinatorRequestV1
    | FindCoordinatorRequestV2
    | FindCoordinatorRequestV3
    | FindCoordinatorRequestV4
    | FindCoordinatorRequestV5
    | FindCoordinatorRequestV6
)

FindCoordinatorResponse = (
    FindCoordinatorResponseV0
    | FindCoordinatorResponseV1
    | FindCoordinatorResponseV2
    | FindCoordinatorResponseV3
    | FindCoordinatorResponseV4
    | FindCoordinatorResponseV5
    | FindCoordinatorResponseV6
)

def get_endpoint(config: Config) -> tuple[str, int, int]:
    advertised_host = config.ADVERTISED_HOST
    advertised_port = config.ADVERTISED_PORT
    node_id = 0
    return advertised_host, advertised_port, node_id

def _extract_keys_and_type(req: FindCoordinatorRequest, api_version: int) -> tuple[list[str], int]:
    if api_version == 0:
        return [req.key], 0

    if 1 <= api_version <= 3:
        kt = int(req.key_type)
        return [req.key], kt

    # 4 <= api_version <= 6
    kt = int(req.key_type)
    return list(req.coordinator_keys), kt

def do_response(coordinators: tuple[fc_v6.response.Coordinator, ...], api_version:int) -> FindCoordinatorResponse:
    resp_v6 = fc_v6.response.FindCoordinatorResponse(
        throttle_time=zero_throttle(),
        coordinators=coordinators
    )
    return resp_v6 if api_version == 6 else do_response_ladder(resp_v6, api_version)

async def do_find_coordinator(config: Config, req: FindCoordinatorRequest, api_version: int) -> FindCoordinatorResponse:
    host, port, node_id = get_endpoint(config)
    keys, key_type = _extract_keys_and_type(req, api_version)

    coordinators: list[fc_v6.response.Coordinator] = []
    if key_type not in (0, 1, 2):
        coordinators.append(fc_v6.response.Coordinator(
            key="",
            node_id=BrokerId(-1),
            host="",
            port=i32(0),
            error_code=ErrorCode.invalid_request,
            error_message="no coordinator key"
        ))
        return do_response(tuple(coordinators), api_version)
    if not keys:
        coordinators.append(
            fc_v6.response.Coordinator(
                key="",
                node_id=BrokerId(-1),
                host="",
                port=i32(0),
                error_code=ErrorCode.invalid_request,
                error_message="no coordinator key"
            )
        )
    for k in keys:
        if not k:
            coordinators.append(
                fc_v6.response.Coordinator(
                    key=k,
                    node_id=BrokerId(-1),
                    host="",
                    port=i32(0),
                    error_code=ErrorCode.invalid_request,
                    error_message="empty coordinator key",
                )
            )
            continue
        coordinators.append(
            fc_v6.response.Coordinator(
                key=k,
                node_id=BrokerId(node_id),
                host=host,
                port=i32(port),
                error_code=ErrorCode.none,
                error_message=None
            )
        )
    return do_response(tuple(coordinators), api_version)

def do_response_ladder(resp: FindCoordinatorResponse, api_version: int) -> FindCoordinatorResponse:
    if api_version == 5:
        entries = tuple(
            fc_v5.response.Coordinator(
                key=c.key,
                node_id=c.node_id,
                host=c.host,
                port=c.port,
                error_code=c.error_code,
                error_message=c.error_message,
            )
            for c in resp.coordinators
        )
        return fc_v5.response.FindCoordinatorResponse(
            throttle_time=resp.throttle_time,
            coordinators=entries,
        )

    elif api_version == 4:
        entries = tuple(
            fc_v4.response.Coordinator(
                key=c.key,
                node_id=c.node_id,
                host=c.host,
                port=c.port,
                error_code=c.error_code,
                error_message=c.error_message,
            )
            for c in resp.coordinators
        )
        return fc_v4.response.FindCoordinatorResponse(
            throttle_time=resp.throttle_time,
            coordinators=entries,
        )

    elif api_version == 3:
        first = next(iter(resp.coordinators), None)
        if first is None:
            return fc_v3.response.FindCoordinatorResponse(
                throttle_time=resp.throttle_time,
                error_code=ErrorCode.invalid_request,
                error_message="Empty coordinator key",
                node_id=BrokerId(-1),
                host="",
                port=i32(0),
            )
        return fc_v3.response.FindCoordinatorResponse(
            throttle_time=resp.throttle_time,
            error_code=first.error_code,
            error_message=first.error_message,
            node_id=first.node_id,
            host=first.host,
            port=first.port,
        )

    elif api_version == 2:
        first = next(iter(resp.coordinators), None)
        if first is None:
            return fc_v2.response.FindCoordinatorResponse(
                throttle_time=resp.throttle_time,
                error_code=ErrorCode.invalid_request,
                error_message="Empty coordinator key",
                node_id=BrokerId(-1),
                host="",
                port=i32(0),
            )
        return fc_v2.response.FindCoordinatorResponse(
            throttle_time=resp.throttle_time,
            error_code=first.error_code,
            error_message=first.error_message,
            node_id=first.node_id,
            host=first.host,
            port=first.port,
        )

    elif api_version == 1:
        first = next(iter(resp.coordinators), None)
        if first is None:
            return fc_v1.response.FindCoordinatorResponse(
                throttle_time=resp.throttle_time,
                error_code=ErrorCode.invalid_request,
                error_message="Empty coordinator key",
                node_id=BrokerId(-1),
                host="",
                port=i32(0),
            )
        return fc_v1.response.FindCoordinatorResponse(
            throttle_time=resp.throttle_time,
            error_code=first.error_code,
            error_message=first.error_message,
            node_id=first.node_id,
            host=first.host,
            port=first.port,
        )

    else:
        first = next(iter(resp.coordinators), None)
        if first is None:
            return fc_v0.response.FindCoordinatorResponse(
                error_code=ErrorCode.invalid_request,
                node_id=BrokerId(-1),
                host="",
                port=i32(0),
            )
        return fc_v0.response.FindCoordinatorResponse(
            error_code=first.error_code,
            node_id=first.node_id,
            host=first.host,
            port=first.port,
        )
