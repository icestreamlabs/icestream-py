from kio.schema.create_partitions.v0.request import CreatePartitionsRequest as CreatePartitionsRequestV0
from kio.schema.create_partitions.v0.request import RequestHeader as CreatePartitionsRequestHeaderV0
from kio.schema.create_partitions.v0.response import CreatePartitionsResponse as CreatePartitionsResponseV0
from kio.schema.create_partitions.v0.response import ResponseHeader as CreatePartitionsResponseHeaderV0
from kio.schema.create_partitions.v1.request import CreatePartitionsRequest as CreatePartitionsRequestV1
from kio.schema.create_partitions.v1.request import RequestHeader as CreatePartitionsRequestHeaderV1
from kio.schema.create_partitions.v1.response import CreatePartitionsResponse as CreatePartitionsResponseV1
from kio.schema.create_partitions.v1.response import ResponseHeader as CreatePartitionsResponseHeaderV1
from kio.schema.create_partitions.v2.request import CreatePartitionsRequest as CreatePartitionsRequestV2
from kio.schema.create_partitions.v2.request import RequestHeader as CreatePartitionsRequestHeaderV2
from kio.schema.create_partitions.v2.response import CreatePartitionsResponse as CreatePartitionsResponseV2
from kio.schema.create_partitions.v2.response import ResponseHeader as CreatePartitionsResponseHeaderV2
from kio.schema.create_partitions.v3.request import CreatePartitionsRequest as CreatePartitionsRequestV3
from kio.schema.create_partitions.v3.request import RequestHeader as CreatePartitionsRequestHeaderV3
from kio.schema.create_partitions.v3.response import CreatePartitionsResponse as CreatePartitionsResponseV3
from kio.schema.create_partitions.v3.response import ResponseHeader as CreatePartitionsResponseHeaderV3
from kio.schema.errors import ErrorCode

from icestream.kafkaserver.handlers.offset_response import response_sequence_element
from icestream.kafkaserver.topic_backends import topic_backend_for_name
from icestream.utils import zero_throttle

CreatePartitionsRequestHeader = (
    CreatePartitionsRequestHeaderV0
    | CreatePartitionsRequestHeaderV1
    | CreatePartitionsRequestHeaderV2
    | CreatePartitionsRequestHeaderV3
)

CreatePartitionsResponseHeader = (
    CreatePartitionsResponseHeaderV0
    | CreatePartitionsResponseHeaderV1
    | CreatePartitionsResponseHeaderV2
    | CreatePartitionsResponseHeaderV3
)

CreatePartitionsRequest = (
    CreatePartitionsRequestV0
    | CreatePartitionsRequestV1
    | CreatePartitionsRequestV2
    | CreatePartitionsRequestV3
)

CreatePartitionsResponse = (
    CreatePartitionsResponseV0
    | CreatePartitionsResponseV1
    | CreatePartitionsResponseV2
    | CreatePartitionsResponseV3
)


def _response_class(api_version: int):
    if api_version == 0:
        return CreatePartitionsResponseV0
    if api_version == 1:
        return CreatePartitionsResponseV1
    if api_version == 2:
        return CreatePartitionsResponseV2
    if api_version == 3:
        return CreatePartitionsResponseV3
    raise ValueError(f"unsupported create_partitions api version: {api_version}")


def _request_topics(req: object):
    if hasattr(req, "topics"):
        return getattr(req, "topics") or ()
    return ()


def _topic_name(entry: object) -> str:
    for name in ("name", "topic", "topic_name"):
        if hasattr(entry, name):
            return str(getattr(entry, name))
    return ""


def _build_topic_result(
    result_cls: type,
    *,
    topic_name: str,
    error_code: ErrorCode,
    error_message: str | None,
):
    fields = getattr(result_cls, "__dataclass_fields__", {})
    kwargs: dict[str, object] = {}
    if "name" in fields:
        kwargs["name"] = topic_name
    elif "topic" in fields:
        kwargs["topic"] = topic_name
    elif "topic_name" in fields:
        kwargs["topic_name"] = topic_name
    if "error_code" in fields:
        kwargs["error_code"] = error_code
    if "error_message" in fields:
        kwargs["error_message"] = error_message
    return result_cls(**kwargs)


def _build_response(response_cls: type, results: list) -> CreatePartitionsResponse:
    fields = getattr(response_cls, "__dataclass_fields__", {})
    kwargs: dict[str, object] = {}
    if "throttle_time" in fields:
        kwargs["throttle_time"] = zero_throttle()
    if "results" in fields:
        kwargs["results"] = tuple(results)
    elif "responses" in fields:
        kwargs["responses"] = tuple(results)
    elif "topics" in fields:
        kwargs["topics"] = tuple(results)
    return response_cls(**kwargs)


def create_partitions_error_response(
    req: CreatePartitionsRequest,
    api_version: int,
    *,
    error_code: ErrorCode,
    error_message: str,
) -> CreatePartitionsResponse:
    response_cls = _response_class(api_version)
    topics = _request_topics(req)
    result_cls = response_sequence_element(response_cls, "results")
    if result_cls is None:
        result_cls = response_sequence_element(response_cls, "responses")
    if result_cls is None:
        result_cls = response_sequence_element(response_cls, "topics")
    if result_cls is None:
        return _build_response(response_cls, [])

    results = [
        _build_topic_result(
            result_cls,
            topic_name=_topic_name(topic),
            error_code=error_code,
            error_message=error_message,
        )
        for topic in topics
    ]
    return _build_response(response_cls, results)


async def do_create_partitions(
    req: CreatePartitionsRequest,
    api_version: int,
) -> CreatePartitionsResponse:
    response_cls = _response_class(api_version)
    topics = _request_topics(req)
    result_cls = response_sequence_element(response_cls, "results")
    if result_cls is None:
        result_cls = response_sequence_element(response_cls, "responses")
    if result_cls is None:
        result_cls = response_sequence_element(response_cls, "topics")
    if result_cls is None:
        return _build_response(response_cls, [])

    results = []
    for topic in topics:
        topic_name = _topic_name(topic)
        if topic_name and topic_backend_for_name(topic_name).is_internal:
            results.append(
                _build_topic_result(
                    result_cls,
                    topic_name=topic_name,
                    error_code=ErrorCode.topic_authorization_failed,
                    error_message="cannot alter internal topic",
                )
            )
        else:
            results.append(
                _build_topic_result(
                    result_cls,
                    topic_name=topic_name,
                    error_code=ErrorCode.none,
                    error_message=None,
                )
            )
    return _build_response(response_cls, results)
