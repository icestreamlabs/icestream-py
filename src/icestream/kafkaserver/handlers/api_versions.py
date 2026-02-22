import datetime
from typing import Any, Callable

import kio.schema.api_versions.v0 as api_v0
import kio.schema.api_versions.v1 as api_v1
import kio.schema.api_versions.v2 as api_v2
import kio.schema.api_versions.v3 as api_v3
import kio.schema.api_versions.v4 as api_v4
import structlog
from kio.index import load_payload_module
from kio.schema.errors import ErrorCode
from kio.schema.api_versions.v0.request import (
    ApiVersionsRequest as ApiVersionsRequestV0,
)
from kio.schema.api_versions.v0.request import (
    RequestHeader as ApiVersionsRequestHeaderV0,
)
from kio.schema.api_versions.v0.response import (
    ApiVersionsResponse as ApiVersionsResponseV0,
)
from kio.schema.api_versions.v1.request import (
    ApiVersionsRequest as ApiVersionsRequestV1,
)
from kio.schema.api_versions.v1.request import (
    RequestHeader as ApiVersionsRequestHeaderV1,
)
from kio.schema.api_versions.v1.response import (
    ApiVersionsResponse as ApiVersionsResponseV1,
)
from kio.schema.api_versions.v2.request import (
    ApiVersionsRequest as ApiVersionsRequestV2,
)
from kio.schema.api_versions.v2.request import (
    RequestHeader as ApiVersionsRequestHeaderV2,
)
from kio.schema.api_versions.v2.response import (
    ApiVersionsResponse as ApiVersionsResponseV2,
)
from kio.schema.api_versions.v3.request import (
    ApiVersionsRequest as ApiVersionsRequestV3,
)
from kio.schema.api_versions.v3.request import (
    RequestHeader as ApiVersionsRequestHeaderV3,
)
from kio.schema.api_versions.v3.response import (
    ApiVersionsResponse as ApiVersionsResponseV3,
)
from kio.schema.api_versions.v4.request import (
    ApiVersionsRequest as ApiVersionsRequestV4,
)
from kio.schema.api_versions.v4.request import (
    RequestHeader as ApiVersionsRequestHeaderV4,
)
from kio.schema.api_versions.v4.response import (
    ApiVersionsResponse as ApiVersionsResponseV4,
)
from kio.static.constants import EntityType
from kio.static.primitive import i16, i32Timedelta, i64

ApiVersionsRequestHeader = (
    ApiVersionsRequestHeaderV0
    | ApiVersionsRequestHeaderV1
    | ApiVersionsRequestHeaderV2
    | ApiVersionsRequestHeaderV3
    | ApiVersionsRequestHeaderV4
)

ApiVersionsRequest = (
    ApiVersionsRequestV0
    | ApiVersionsRequestV1
    | ApiVersionsRequestV2
    | ApiVersionsRequestV3
    | ApiVersionsRequestV4
)

ApiVersionsResponse = (
    ApiVersionsResponseV0
    | ApiVersionsResponseV1
    | ApiVersionsResponseV2
    | ApiVersionsResponseV3
    | ApiVersionsResponseV4
)

log = structlog.get_logger()


async def do_handle_api_versions_request(
    req: ApiVersionsRequest,
    api_version: int,
    callback: Callable[[ApiVersionsResponse], Any],
    compatibility: dict[int, tuple[int, int]],
) -> None:
    _ = req
    versions = tuple(
        api_v4.response.ApiVersion(
            api_key=i16(api_key), min_version=i16(min_ver), max_version=i16(max_ver)
        )
        for api_key, (min_ver, max_ver) in compatibility.items()
    )

    response = api_v4.response.ApiVersionsResponse(
        error_code=ErrorCode.none,
        api_keys=versions,
        throttle_time=i32Timedelta.parse(datetime.timedelta(milliseconds=0)),
        supported_features=(),
        finalized_features_epoch=i64(-1),
        finalized_features=(),
        zk_migration_ready=False,
    )
    if api_version == 0:
        _versions = tuple(
            api_v0.response.ApiVersion(
                api_key=_version.api_key,
                min_version=_version.min_version,
                max_version=_version.max_version,
            )
            for _version in response.api_keys
        )
        await callback(
            api_v0.response.ApiVersionsResponse(
                error_code=response.error_code, api_keys=_versions
            )
        )
    elif api_version == 1:
        _versions = tuple(
            api_v1.response.ApiVersion(
                api_key=_version.api_key,
                min_version=_version.min_version,
                max_version=_version.max_version,
            )
            for _version in response.api_keys
        )
        await callback(
            api_v1.response.ApiVersionsResponse(
                error_code=response.error_code,
                api_keys=_versions,
                throttle_time=response.throttle_time,
            )
        )
    elif api_version == 2:
        _versions = tuple(
            api_v2.response.ApiVersion(
                api_key=_version.api_key,
                min_version=_version.min_version,
                max_version=_version.max_version,
            )
            for _version in response.api_keys
        )
        await callback(
            api_v2.response.ApiVersionsResponse(
                error_code=response.error_code,
                api_keys=_versions,
                throttle_time=response.throttle_time,
            )
        )
    elif api_version == 3:
        _versions = tuple(
            api_v3.response.ApiVersion(
                api_key=_version.api_key,
                min_version=_version.min_version,
                max_version=_version.max_version,
            )
            for _version in response.api_keys
        )
        await callback(
            api_v3.response.ApiVersionsResponse(
                error_code=response.error_code,
                api_keys=_versions,
                throttle_time=response.throttle_time,
                supported_features=response.supported_features,
                finalized_features_epoch=response.finalized_features_epoch,
                finalized_features=response.finalized_features,
                zk_migration_ready=response.zk_migration_ready,
            )
        )
    elif api_version == 4:
        await callback(response)
    else:
        log.error("unsupported api versions version", api_version=api_version)


def api_versions_error_response(
    req: ApiVersionsRequest,
    api_version: int,
    *,
    error_code: ErrorCode,
    error_message: str,
) -> ApiVersionsResponse:
    _ = req
    _ = error_message
    mod = load_payload_module(18, api_version, EntityType.response)
    return mod.ApiVersionsResponse(
        error_code=error_code,
        api_keys=(),
    )
