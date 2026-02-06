from kio.schema.delete_topics.v0.request import (
    DeleteTopicsRequest as DeleteTopicsRequestV0,
)
from kio.schema.delete_topics.v0.request import (
    RequestHeader as DeleteTopicsRequestHeaderV0,
)
from kio.schema.delete_topics.v0.response import (
    DeleteTopicsResponse as DeleteTopicsResponseV0,
)
from kio.schema.delete_topics.v0.response import (
    ResponseHeader as DeleteTopicsResponseHeaderV0,
)
from kio.schema.delete_topics.v1.request import (
    DeleteTopicsRequest as DeleteTopicsRequestV1,
)
from kio.schema.delete_topics.v1.request import (
    RequestHeader as DeleteTopicsRequestHeaderV1,
)
from kio.schema.delete_topics.v1.response import (
    DeleteTopicsResponse as DeleteTopicsResponseV1,
)
from kio.schema.delete_topics.v1.response import (
    ResponseHeader as DeleteTopicsResponseHeaderV1,
)
from kio.schema.delete_topics.v2.request import (
    DeleteTopicsRequest as DeleteTopicsRequestV2,
)
from kio.schema.delete_topics.v2.request import (
    RequestHeader as DeleteTopicsRequestHeaderV2,
)
from kio.schema.delete_topics.v2.response import (
    DeleteTopicsResponse as DeleteTopicsResponseV2,
)
from kio.schema.delete_topics.v2.response import (
    ResponseHeader as DeleteTopicsResponseHeaderV2,
)
from kio.schema.delete_topics.v3.request import (
    DeleteTopicsRequest as DeleteTopicsRequestV3,
)
from kio.schema.delete_topics.v3.request import (
    RequestHeader as DeleteTopicsRequestHeaderV3,
)
from kio.schema.delete_topics.v3.response import (
    DeleteTopicsResponse as DeleteTopicsResponseV3,
)
from kio.schema.delete_topics.v3.response import (
    ResponseHeader as DeleteTopicsResponseHeaderV3,
)
from kio.schema.delete_topics.v4.request import (
    DeleteTopicsRequest as DeleteTopicsRequestV4,
)
from kio.schema.delete_topics.v4.request import (
    RequestHeader as DeleteTopicsRequestHeaderV4,
)
from kio.schema.delete_topics.v4.response import (
    DeleteTopicsResponse as DeleteTopicsResponseV4,
)
from kio.schema.delete_topics.v4.response import (
    ResponseHeader as DeleteTopicsResponseHeaderV4,
)
from kio.schema.delete_topics.v5.request import (
    DeleteTopicsRequest as DeleteTopicsRequestV5,
)
from kio.schema.delete_topics.v5.request import (
    RequestHeader as DeleteTopicsRequestHeaderV5,
)
from kio.schema.delete_topics.v5.response import (
    DeleteTopicsResponse as DeleteTopicsResponseV5,
)
from kio.schema.delete_topics.v5.response import (
    ResponseHeader as DeleteTopicsResponseHeaderV5,
)
from kio.schema.delete_topics.v6.request import (
    DeleteTopicsRequest as DeleteTopicsRequestV6,
)
from kio.schema.delete_topics.v6.request import (
    RequestHeader as DeleteTopicsRequestHeaderV6,
)
from kio.schema.delete_topics.v6.response import (
    DeleteTopicsResponse as DeleteTopicsResponseV6,
)
from kio.schema.delete_topics.v6.response import (
    ResponseHeader as DeleteTopicsResponseHeaderV6,
)
from kio.schema.errors import ErrorCode
from sqlalchemy import select

from icestream.config import Config
from icestream.kafkaserver.handlers.offset_response import response_sequence_element
from icestream.kafkaserver.topic_backends import topic_backend_for_name
from icestream.models import Topic
from icestream.utils import zero_throttle

DeleteTopicsRequestHeader = (
    DeleteTopicsRequestHeaderV0
    | DeleteTopicsRequestHeaderV1
    | DeleteTopicsRequestHeaderV2
    | DeleteTopicsRequestHeaderV3
    | DeleteTopicsRequestHeaderV4
    | DeleteTopicsRequestHeaderV5
    | DeleteTopicsRequestHeaderV6
)

DeleteTopicsResponseHeader = (
    DeleteTopicsResponseHeaderV0
    | DeleteTopicsResponseHeaderV1
    | DeleteTopicsResponseHeaderV2
    | DeleteTopicsResponseHeaderV3
    | DeleteTopicsResponseHeaderV4
    | DeleteTopicsResponseHeaderV5
    | DeleteTopicsResponseHeaderV6
)

DeleteTopicsRequest = (
    DeleteTopicsRequestV0
    | DeleteTopicsRequestV1
    | DeleteTopicsRequestV2
    | DeleteTopicsRequestV3
    | DeleteTopicsRequestV4
    | DeleteTopicsRequestV5
    | DeleteTopicsRequestV6
)

DeleteTopicsResponse = (
    DeleteTopicsResponseV0
    | DeleteTopicsResponseV1
    | DeleteTopicsResponseV2
    | DeleteTopicsResponseV3
    | DeleteTopicsResponseV4
    | DeleteTopicsResponseV5
    | DeleteTopicsResponseV6
)


def _response_class(api_version: int):
    if api_version == 0:
        return DeleteTopicsResponseV0
    if api_version == 1:
        return DeleteTopicsResponseV1
    if api_version == 2:
        return DeleteTopicsResponseV2
    if api_version == 3:
        return DeleteTopicsResponseV3
    if api_version == 4:
        return DeleteTopicsResponseV4
    if api_version == 5:
        return DeleteTopicsResponseV5
    if api_version == 6:
        return DeleteTopicsResponseV6
    raise ValueError(f"unsupported delete_topics api version: {api_version}")


def _request_topics(req: object):
    if hasattr(req, "topics"):
        return getattr(req, "topics") or ()
    if hasattr(req, "topic_names"):
        return getattr(req, "topic_names") or ()
    return ()


def _topic_name(entry: object) -> str:
    if isinstance(entry, str):
        return entry
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
    if "topic_id" in fields:
        kwargs["topic_id"] = None
    if "error_code" in fields:
        kwargs["error_code"] = error_code
    if "error_message" in fields:
        kwargs["error_message"] = error_message
    return result_cls(**kwargs)


def _build_response(
    response_cls: type,
    *,
    results: list,
):
    fields = getattr(response_cls, "__dataclass_fields__", {})
    kwargs: dict[str, object] = {}
    if "throttle_time" in fields:
        kwargs["throttle_time"] = zero_throttle()
    if "responses" in fields:
        kwargs["responses"] = tuple(results)
    elif "topics" in fields:
        kwargs["topics"] = tuple(results)
    return response_cls(**kwargs)


def delete_topics_error_response(
    req: DeleteTopicsRequest,
    api_version: int,
    *,
    error_code: ErrorCode,
    error_message: str,
) -> DeleteTopicsResponse:
    response_cls = _response_class(api_version)
    topics_req = _request_topics(req)
    result_cls = response_sequence_element(response_cls, "responses")
    if result_cls is None:
        result_cls = response_sequence_element(response_cls, "topics")
    if result_cls is None:
        return _build_response(response_cls, results=[])

    results = []
    for entry in topics_req:
        topic_name = _topic_name(entry)
        results.append(
            _build_topic_result(
                result_cls,
                topic_name=topic_name,
                error_code=error_code,
                error_message=error_message,
            )
        )
    return _build_response(response_cls, results=results)


async def do_delete_topics(
    config: Config,
    req: DeleteTopicsRequest,
    api_version: int,
) -> DeleteTopicsResponse:
    assert config.async_session_factory is not None

    response_cls = _response_class(api_version)
    topics_req = _request_topics(req)
    result_cls = response_sequence_element(response_cls, "responses")
    if result_cls is None:
        result_cls = response_sequence_element(response_cls, "topics")
    if result_cls is None:
        return _build_response(response_cls, results=[])

    topic_names = [_topic_name(entry) for entry in topics_req]

    async with config.async_session_factory() as session:
        async with session.begin():
            existing_rows = (
                await session.execute(
                    select(Topic).where(Topic.name.in_(topic_names))
                )
            ).scalars().all()
            existing = {row.name: row for row in existing_rows}

            results = []
            for topic_name in topic_names:
                if not topic_name:
                    results.append(
                        _build_topic_result(
                            result_cls,
                            topic_name=topic_name,
                            error_code=ErrorCode.invalid_request,
                            error_message="missing topic name",
                        )
                    )
                    continue

                if topic_backend_for_name(topic_name).is_internal:
                    results.append(
                        _build_topic_result(
                            result_cls,
                            topic_name=topic_name,
                            error_code=ErrorCode.topic_authorization_failed,
                            error_message="cannot delete internal topic",
                        )
                    )
                    continue

                row = existing.get(topic_name)
                if row is None:
                    results.append(
                        _build_topic_result(
                            result_cls,
                            topic_name=topic_name,
                            error_code=ErrorCode.unknown_topic_or_partition,
                            error_message="unknown topic",
                        )
                    )
                    continue

                await session.delete(row)
                results.append(
                    _build_topic_result(
                        result_cls,
                        topic_name=topic_name,
                        error_code=ErrorCode.none,
                        error_message=None,
                    )
                )

    return _build_response(response_cls, results=results)
