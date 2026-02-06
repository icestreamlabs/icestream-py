from kio.schema.describe_configs.v0.request import (
    DescribeConfigsRequest as DescribeConfigsRequestV0,
)
from kio.schema.describe_configs.v0.request import (
    RequestHeader as DescribeConfigsRequestHeaderV0,
)
from kio.schema.describe_configs.v0.response import (
    DescribeConfigsResponse as DescribeConfigsResponseV0,
)
from kio.schema.describe_configs.v0.response import (
    ResponseHeader as DescribeConfigsResponseHeaderV0,
)
from kio.schema.describe_configs.v1.request import (
    DescribeConfigsRequest as DescribeConfigsRequestV1,
)
from kio.schema.describe_configs.v1.request import (
    RequestHeader as DescribeConfigsRequestHeaderV1,
)
from kio.schema.describe_configs.v1.response import (
    DescribeConfigsResponse as DescribeConfigsResponseV1,
)
from kio.schema.describe_configs.v1.response import (
    ResponseHeader as DescribeConfigsResponseHeaderV1,
)
from kio.schema.describe_configs.v2.request import (
    DescribeConfigsRequest as DescribeConfigsRequestV2,
)
from kio.schema.describe_configs.v2.request import (
    RequestHeader as DescribeConfigsRequestHeaderV2,
)
from kio.schema.describe_configs.v2.response import (
    DescribeConfigsResponse as DescribeConfigsResponseV2,
)
from kio.schema.describe_configs.v2.response import (
    ResponseHeader as DescribeConfigsResponseHeaderV2,
)
from kio.schema.describe_configs.v3.request import (
    DescribeConfigsRequest as DescribeConfigsRequestV3,
)
from kio.schema.describe_configs.v3.request import (
    RequestHeader as DescribeConfigsRequestHeaderV3,
)
from kio.schema.describe_configs.v3.response import (
    DescribeConfigsResponse as DescribeConfigsResponseV3,
)
from kio.schema.describe_configs.v3.response import (
    ResponseHeader as DescribeConfigsResponseHeaderV3,
)
from kio.schema.describe_configs.v4.request import (
    DescribeConfigsRequest as DescribeConfigsRequestV4,
)
from kio.schema.describe_configs.v4.request import (
    RequestHeader as DescribeConfigsRequestHeaderV4,
)
from kio.schema.describe_configs.v4.response import (
    DescribeConfigsResponse as DescribeConfigsResponseV4,
)
from kio.schema.describe_configs.v4.response import (
    ResponseHeader as DescribeConfigsResponseHeaderV4,
)
from kio.schema.errors import ErrorCode

from icestream.kafkaserver.handlers.offset_response import response_sequence_element
from icestream.kafkaserver.topic_backends import topic_backend_for_name
from icestream.utils import zero_throttle


DescribeConfigsRequestHeader = (
    DescribeConfigsRequestHeaderV0
    | DescribeConfigsRequestHeaderV1
    | DescribeConfigsRequestHeaderV2
    | DescribeConfigsRequestHeaderV3
    | DescribeConfigsRequestHeaderV4
)

DescribeConfigsResponseHeader = (
    DescribeConfigsResponseHeaderV0
    | DescribeConfigsResponseHeaderV1
    | DescribeConfigsResponseHeaderV2
    | DescribeConfigsResponseHeaderV3
    | DescribeConfigsResponseHeaderV4
)

DescribeConfigsRequest = (
    DescribeConfigsRequestV0
    | DescribeConfigsRequestV1
    | DescribeConfigsRequestV2
    | DescribeConfigsRequestV3
    | DescribeConfigsRequestV4
)

DescribeConfigsResponse = (
    DescribeConfigsResponseV0
    | DescribeConfigsResponseV1
    | DescribeConfigsResponseV2
    | DescribeConfigsResponseV3
    | DescribeConfigsResponseV4
)


def _response_class(api_version: int):
    if api_version == 0:
        return DescribeConfigsResponseV0
    if api_version == 1:
        return DescribeConfigsResponseV1
    if api_version == 2:
        return DescribeConfigsResponseV2
    if api_version == 3:
        return DescribeConfigsResponseV3
    if api_version == 4:
        return DescribeConfigsResponseV4
    raise ValueError(f"unsupported describe_configs api version: {api_version}")


def _request_resources(req: object):
    if hasattr(req, "resources"):
        return getattr(req, "resources") or ()
    if hasattr(req, "entries"):
        return getattr(req, "entries") or ()
    return ()


def _resource_name(resource: object) -> str:
    for name in ("resource_name", "name", "resource", "resource_id"):
        if hasattr(resource, name):
            return str(getattr(resource, name))
    return ""


def _resource_type(resource: object):
    for name in ("resource_type", "resource_type_id", "type"):
        if hasattr(resource, name):
            return getattr(resource, name)
    return None


def _build_config_entry(entry_cls: type, *, name: str, value: str) -> object:
    fields = getattr(entry_cls, "__dataclass_fields__", {})
    kwargs: dict[str, object] = {}
    if "name" in fields:
        kwargs["name"] = name
    if "value" in fields:
        kwargs["value"] = value
    if "read_only" in fields:
        kwargs["read_only"] = True
    if "is_default" in fields:
        kwargs["is_default"] = False
    if "config_source" in fields:
        kwargs["config_source"] = 0
    if "is_sensitive" in fields:
        kwargs["is_sensitive"] = False
    if "config_type" in fields:
        kwargs["config_type"] = 0
    if "synonyms" in fields:
        kwargs["synonyms"] = tuple()
    if "documentation" in fields:
        kwargs["documentation"] = None
    return entry_cls(**kwargs)


def _build_result(
    result_cls: type,
    *,
    resource: object,
    error_code: ErrorCode,
    error_message: str | None,
    configs: list,
):
    fields = getattr(result_cls, "__dataclass_fields__", {})
    kwargs: dict[str, object] = {}
    resource_name = _resource_name(resource)
    resource_type = _resource_type(resource)
    if "resource_name" in fields:
        kwargs["resource_name"] = resource_name
    elif "name" in fields:
        kwargs["name"] = resource_name
    if "resource_type" in fields and resource_type is not None:
        kwargs["resource_type"] = resource_type
    if "resource_type_id" in fields and resource_type is not None:
        kwargs["resource_type_id"] = resource_type
    if "configs" in fields:
        kwargs["configs"] = tuple(configs)
    elif "config_entries" in fields:
        kwargs["config_entries"] = tuple(configs)
    if "error_code" in fields:
        kwargs["error_code"] = error_code
    if "error_message" in fields:
        kwargs["error_message"] = error_message
    return result_cls(**kwargs)


def _build_response(response_cls: type, results: list) -> DescribeConfigsResponse:
    fields = getattr(response_cls, "__dataclass_fields__", {})
    kwargs: dict[str, object] = {}
    if "throttle_time" in fields:
        kwargs["throttle_time"] = zero_throttle()
    if "results" in fields:
        kwargs["results"] = tuple(results)
    elif "resources" in fields:
        kwargs["resources"] = tuple(results)
    return response_cls(**kwargs)


def describe_configs_error_response(
    req: DescribeConfigsRequest,
    api_version: int,
    *,
    error_code: ErrorCode,
    error_message: str,
) -> DescribeConfigsResponse:
    response_cls = _response_class(api_version)
    resources = _request_resources(req)
    result_cls = response_sequence_element(response_cls, "results")
    if result_cls is None:
        result_cls = response_sequence_element(response_cls, "resources")
    if result_cls is None:
        return _build_response(response_cls, [])

    results = [
        _build_result(
            result_cls,
            resource=resource,
            error_code=error_code,
            error_message=error_message,
            configs=[],
        )
        for resource in resources
    ]
    return _build_response(response_cls, results)


async def do_describe_configs(
    req: DescribeConfigsRequest,
    api_version: int,
) -> DescribeConfigsResponse:
    response_cls = _response_class(api_version)
    resources = _request_resources(req)
    result_cls = response_sequence_element(response_cls, "results")
    if result_cls is None:
        result_cls = response_sequence_element(response_cls, "resources")
    if result_cls is None:
        return _build_response(response_cls, [])

    config_cls = response_sequence_element(result_cls, "configs")
    if config_cls is None:
        config_cls = response_sequence_element(result_cls, "config_entries")

    results = []
    for resource in resources:
        resource_name = _resource_name(resource)
        configs: list = []
        if resource_name and topic_backend_for_name(resource_name).is_internal and config_cls is not None:
            configs.append(_build_config_entry(config_cls, name="cleanup.policy", value="compact"))
        results.append(
            _build_result(
                result_cls,
                resource=resource,
                error_code=ErrorCode.none,
                error_message=None,
                configs=configs,
            )
        )
    return _build_response(response_cls, results)
