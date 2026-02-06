from kio.schema.incremental_alter_configs.v0.request import (
    IncrementalAlterConfigsRequest as IncrementalAlterConfigsRequestV0,
)
from kio.schema.incremental_alter_configs.v0.request import (
    RequestHeader as IncrementalAlterConfigsRequestHeaderV0,
)
from kio.schema.incremental_alter_configs.v0.response import (
    IncrementalAlterConfigsResponse as IncrementalAlterConfigsResponseV0,
)
from kio.schema.incremental_alter_configs.v0.response import (
    ResponseHeader as IncrementalAlterConfigsResponseHeaderV0,
)
from kio.schema.incremental_alter_configs.v1.request import (
    IncrementalAlterConfigsRequest as IncrementalAlterConfigsRequestV1,
)
from kio.schema.incremental_alter_configs.v1.request import (
    RequestHeader as IncrementalAlterConfigsRequestHeaderV1,
)
from kio.schema.incremental_alter_configs.v1.response import (
    IncrementalAlterConfigsResponse as IncrementalAlterConfigsResponseV1,
)
from kio.schema.incremental_alter_configs.v1.response import (
    ResponseHeader as IncrementalAlterConfigsResponseHeaderV1,
)
from kio.schema.errors import ErrorCode

from icestream.kafkaserver.handlers.offset_response import response_sequence_element
from icestream.kafkaserver.topic_backends import topic_backend_for_name
from icestream.utils import zero_throttle


IncrementalAlterConfigsRequestHeader = (
    IncrementalAlterConfigsRequestHeaderV0 | IncrementalAlterConfigsRequestHeaderV1
)

IncrementalAlterConfigsResponseHeader = (
    IncrementalAlterConfigsResponseHeaderV0 | IncrementalAlterConfigsResponseHeaderV1
)

IncrementalAlterConfigsRequest = (
    IncrementalAlterConfigsRequestV0 | IncrementalAlterConfigsRequestV1
)

IncrementalAlterConfigsResponse = (
    IncrementalAlterConfigsResponseV0 | IncrementalAlterConfigsResponseV1
)


def _response_class(api_version: int):
    if api_version == 0:
        return IncrementalAlterConfigsResponseV0
    if api_version == 1:
        return IncrementalAlterConfigsResponseV1
    raise ValueError(f"unsupported incremental_alter_configs api version: {api_version}")


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


def _build_resource_result(
    result_cls: type,
    *,
    resource: object,
    error_code: ErrorCode,
    error_message: str | None,
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
    if "error_code" in fields:
        kwargs["error_code"] = error_code
    if "error_message" in fields:
        kwargs["error_message"] = error_message
    return result_cls(**kwargs)


def _build_response(response_cls: type, results: list) -> IncrementalAlterConfigsResponse:
    fields = getattr(response_cls, "__dataclass_fields__", {})
    kwargs: dict[str, object] = {}
    if "throttle_time" in fields:
        kwargs["throttle_time"] = zero_throttle()
    if "responses" in fields:
        kwargs["responses"] = tuple(results)
    elif "results" in fields:
        kwargs["results"] = tuple(results)
    return response_cls(**kwargs)


def incremental_alter_configs_error_response(
    req: IncrementalAlterConfigsRequest,
    api_version: int,
    *,
    error_code: ErrorCode,
    error_message: str,
) -> IncrementalAlterConfigsResponse:
    response_cls = _response_class(api_version)
    resources = _request_resources(req)
    result_cls = response_sequence_element(response_cls, "responses")
    if result_cls is None:
        result_cls = response_sequence_element(response_cls, "results")
    if result_cls is None:
        return _build_response(response_cls, [])

    results = [
        _build_resource_result(
            result_cls,
            resource=resource,
            error_code=error_code,
            error_message=error_message,
        )
        for resource in resources
    ]
    return _build_response(response_cls, results)


async def do_incremental_alter_configs(
    req: IncrementalAlterConfigsRequest,
    api_version: int,
) -> IncrementalAlterConfigsResponse:
    response_cls = _response_class(api_version)
    resources = _request_resources(req)
    result_cls = response_sequence_element(response_cls, "responses")
    if result_cls is None:
        result_cls = response_sequence_element(response_cls, "results")
    if result_cls is None:
        return _build_response(response_cls, [])

    results = []
    for resource in resources:
        resource_name = _resource_name(resource)
        if resource_name and topic_backend_for_name(resource_name).is_internal:
            results.append(
                _build_resource_result(
                    result_cls,
                    resource=resource,
                    error_code=ErrorCode.topic_authorization_failed,
                    error_message="cannot alter internal topic",
                )
            )
        else:
            results.append(
                _build_resource_result(
                    result_cls,
                    resource=resource,
                    error_code=ErrorCode.none,
                    error_message=None,
                )
            )
    return _build_response(response_cls, results)
