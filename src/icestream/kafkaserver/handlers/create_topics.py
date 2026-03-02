import datetime
import uuid
from typing import Any, Callable

import structlog
from kio.index import load_payload_module
from kio.schema.errors import ErrorCode
from kio.schema.create_topics.v0.request import (
    CreateTopicsRequest as CreateTopicsRequestV0,
)
from kio.schema.create_topics.v0.request import (
    RequestHeader as CreateTopicsRequestHeaderV0,
)
from kio.schema.create_topics.v0.response import (
    CreateTopicsResponse as CreateTopicsResponseV0,
)
from kio.schema.create_topics.v0.response import (
    ResponseHeader as CreateTopicsResponseHeaderV0,
)
from kio.schema.create_topics.v1.request import (
    CreateTopicsRequest as CreateTopicsRequestV1,
)
from kio.schema.create_topics.v1.request import (
    RequestHeader as CreateTopicsRequestHeaderV1,
)
from kio.schema.create_topics.v1.response import (
    CreateTopicsResponse as CreateTopicsResponseV1,
)
from kio.schema.create_topics.v1.response import (
    ResponseHeader as CreateTopicsResponseHeaderV1,
)
from kio.schema.create_topics.v2.request import (
    CreateTopicsRequest as CreateTopicsRequestV2,
)
from kio.schema.create_topics.v2.request import (
    RequestHeader as CreateTopicsRequestHeaderV2,
)
from kio.schema.create_topics.v2.response import (
    CreateTopicsResponse as CreateTopicsResponseV2,
)
from kio.schema.create_topics.v2.response import (
    ResponseHeader as CreateTopicsResponseHeaderV2,
)
from kio.schema.create_topics.v3.request import (
    CreateTopicsRequest as CreateTopicsRequestV3,
)
from kio.schema.create_topics.v3.request import (
    RequestHeader as CreateTopicsRequestHeaderV3,
)
from kio.schema.create_topics.v3.response import (
    CreateTopicsResponse as CreateTopicsResponseV3,
)
from kio.schema.create_topics.v3.response import (
    ResponseHeader as CreateTopicsResponseHeaderV3,
)
from kio.schema.create_topics.v4.request import (
    CreateTopicsRequest as CreateTopicsRequestV4,
)
from kio.schema.create_topics.v4.request import (
    RequestHeader as CreateTopicsRequestHeaderV4,
)
from kio.schema.create_topics.v4.response import (
    CreateTopicsResponse as CreateTopicsResponseV4,
)
from kio.schema.create_topics.v4.response import (
    ResponseHeader as CreateTopicsResponseHeaderV4,
)
from kio.schema.create_topics.v5.request import (
    CreateTopicsRequest as CreateTopicsRequestV5,
)
from kio.schema.create_topics.v5.request import (
    RequestHeader as CreateTopicsRequestHeaderV5,
)
from kio.schema.create_topics.v5.response import (
    CreateTopicsResponse as CreateTopicsResponseV5,
)
from kio.schema.create_topics.v5.response import (
    ResponseHeader as CreateTopicsResponseHeaderV5,
)
from kio.schema.create_topics.v6.request import (
    CreateTopicsRequest as CreateTopicsRequestV6,
)
from kio.schema.create_topics.v6.request import (
    RequestHeader as CreateTopicsRequestHeaderV6,
)
from kio.schema.create_topics.v6.response import (
    CreateTopicsResponse as CreateTopicsResponseV6,
)
from kio.schema.create_topics.v6.response import (
    ResponseHeader as CreateTopicsResponseHeaderV6,
)
from kio.schema.create_topics.v7.request import (
    CreateTopicsRequest as CreateTopicsRequestV7,
)
from kio.schema.create_topics.v7.request import (
    RequestHeader as CreateTopicsRequestHeaderV7,
)
from kio.schema.create_topics.v7.response import (
    CreateTopicsResponse as CreateTopicsResponseV7,
)
from kio.schema.create_topics.v7.response import (
    ResponseHeader as CreateTopicsResponseHeaderV7,
)
from kio.static.constants import EntityType
from kio.static.primitive import i16, i32

from icestream.config import Config
from icestream.kafkaserver.topic_backends import topic_backend_for_name
from icestream.models import Partition, Topic


CreateTopicsRequestHeader = (
    CreateTopicsRequestHeaderV0
    | CreateTopicsRequestHeaderV1
    | CreateTopicsRequestHeaderV2
    | CreateTopicsRequestHeaderV3
    | CreateTopicsRequestHeaderV4
    | CreateTopicsRequestHeaderV5
    | CreateTopicsRequestHeaderV6
    | CreateTopicsRequestHeaderV7
)

CreateTopicsResponseHeader = (
    CreateTopicsResponseHeaderV0
    | CreateTopicsResponseHeaderV1
    | CreateTopicsResponseHeaderV2
    | CreateTopicsResponseHeaderV3
    | CreateTopicsResponseHeaderV4
    | CreateTopicsResponseHeaderV5
    | CreateTopicsResponseHeaderV6
    | CreateTopicsResponseHeaderV7
)

CreateTopicsRequest = (
    CreateTopicsRequestV0
    | CreateTopicsRequestV1
    | CreateTopicsRequestV2
    | CreateTopicsRequestV3
    | CreateTopicsRequestV4
    | CreateTopicsRequestV5
    | CreateTopicsRequestV6
    | CreateTopicsRequestV7
)

CreateTopicsResponse = (
    CreateTopicsResponseV0
    | CreateTopicsResponseV1
    | CreateTopicsResponseV2
    | CreateTopicsResponseV3
    | CreateTopicsResponseV4
    | CreateTopicsResponseV5
    | CreateTopicsResponseV6
    | CreateTopicsResponseV7
)

log = structlog.get_logger()


CREATE_TOPICS_API_KEY = 19


def _response_module(api_version: int):
    return load_payload_module(CREATE_TOPICS_API_KEY, api_version, EntityType.response)


def _response_metadata(response_cls: type) -> dict:
    return getattr(response_cls, "__dataclass_fields__", {})


def _result_fields(result_cls: type) -> dict:
    return getattr(result_cls, "__dataclass_fields__", {})


def _build_topic_result(
    result_cls: type,
    *,
    topic_name: str,
    error_code: ErrorCode,
    error_message: str | None = None,
    num_partitions: int = -1,
    replication_factor: int = -1,
):
    fields = _result_fields(result_cls)
    kwargs: dict[str, object] = {}

    if "name" in fields:
        kwargs["name"] = topic_name
    if "topic_id" in fields:
        kwargs["topic_id"] = uuid.uuid4() if error_code == ErrorCode.none else None
    if "error_code" in fields:
        kwargs["error_code"] = error_code
    if "error_message" in fields:
        kwargs["error_message"] = error_message
    if "topic_config_error_code" in fields:
        kwargs["topic_config_error_code"] = i16(0)
    if "num_partitions" in fields:
        kwargs["num_partitions"] = i32(num_partitions)
    if "replication_factor" in fields:
        kwargs["replication_factor"] = i16(replication_factor)
    if "configs" in fields:
        kwargs["configs"] = None

    return result_cls(**kwargs)


def _build_response(response_cls: type, *, topics: list):
    fields = _response_metadata(response_cls)
    kwargs: dict[str, object] = {"topics": tuple(topics)}
    if "throttle_time" in fields:
        kwargs["throttle_time"] = datetime.timedelta(milliseconds=0)
    return response_cls(**kwargs)


def _validate_topic_request(topic: Any) -> tuple[ErrorCode | None, str | None]:
    topic_name = str(topic.name)
    if not topic_name:
        return ErrorCode.invalid_topic_exception, "topic name cannot be empty"

    num_partitions = int(topic.num_partitions)
    if num_partitions <= 0:
        return ErrorCode.invalid_partitions, "num_partitions must be > 0"

    replication_factor = int(topic.replication_factor)
    if replication_factor not in (-1, 1):
        return ErrorCode.invalid_replication_factor, "replication_factor must be 1"

    return None, None


async def do_handle_create_topics_request(
    config: Config,
    req: CreateTopicsRequest,
    api_version: int,
    callback: Callable[[CreateTopicsResponse], Any],
) -> None:
    assert config.async_session_factory is not None
    mod = _response_module(api_version)
    response_cls = mod.CreateTopicsResponse
    result_cls = mod.CreatableTopicResult

    validate_only = bool(getattr(req, "validate_only", False))
    request_topic_names = [str(topic.name) for topic in req.topics]
    existing_names: set[str] = set()
    created_names: set[str] = set()
    results = []

    async with config.async_session_factory() as session:
        async with session.begin():
            existing_rows = (
                await session.execute(
                    Topic.__table__.select().where(Topic.name.in_(request_topic_names))
                )
            ).all()
            existing_names = {str(row.name) for row in existing_rows}

            for topic in req.topics:
                topic_name = str(topic.name)
                num_partitions = int(topic.num_partitions)
                replication_factor = int(topic.replication_factor)
                normalized_replication_factor = (
                    1 if replication_factor == -1 else replication_factor
                )

                log.info(
                    "create_topic_request",
                    topic=topic_name,
                    num_partitions=num_partitions,
                    replication_factor=replication_factor,
                    validate_only=validate_only,
                )

                if topic_backend_for_name(topic_name).is_internal:
                    results.append(
                        _build_topic_result(
                            result_cls,
                            topic_name=topic_name,
                            error_code=ErrorCode.topic_authorization_failed,
                            error_message="cannot create internal topic",
                        )
                    )
                    continue

                validation_error, validation_error_message = _validate_topic_request(
                    topic
                )
                if validation_error is not None:
                    results.append(
                        _build_topic_result(
                            result_cls,
                            topic_name=topic_name,
                            error_code=validation_error,
                            error_message=validation_error_message,
                        )
                    )
                    continue

                if topic_name in existing_names or topic_name in created_names:
                    results.append(
                        _build_topic_result(
                            result_cls,
                            topic_name=topic_name,
                            error_code=ErrorCode.topic_already_exists,
                            error_message="topic already exists",
                        )
                    )
                    continue

                if validate_only:
                    results.append(
                        _build_topic_result(
                            result_cls,
                            topic_name=topic_name,
                            error_code=ErrorCode.none,
                            error_message=None,
                            num_partitions=num_partitions,
                            replication_factor=normalized_replication_factor,
                        )
                    )
                    created_names.add(topic_name)
                    continue

                session.add(Topic(name=topic_name, is_internal=False))
                await session.flush()

                for partition_number in range(num_partitions):
                    session.add(
                        Partition(
                            topic_name=topic_name,
                            partition_number=partition_number,
                            last_offset=-1,
                            log_start_offset=0,
                        )
                    )
                    # Insert partitions one-by-one to stay compatible with mockgres.
                    await session.flush()

                created_names.add(topic_name)
                results.append(
                    _build_topic_result(
                        result_cls,
                        topic_name=topic_name,
                        error_code=ErrorCode.none,
                        error_message=None,
                        num_partitions=num_partitions,
                        replication_factor=normalized_replication_factor,
                    )
                )

    response = _build_response(response_cls, topics=results)
    await callback(response)


def create_topics_error_response(
    req: CreateTopicsRequest,
    api_version: int,
    *,
    error_code: ErrorCode,
    error_message: str,
) -> CreateTopicsResponse:
    mod = _response_module(api_version)
    results = []
    for topic in req.topics:
        results.append(
            _build_topic_result(
                mod.CreatableTopicResult,
                topic_name=str(topic.name),
                error_code=error_code,
                error_message=error_message,
            )
        )

    return _build_response(mod.CreateTopicsResponse, topics=results)
