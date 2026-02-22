import datetime
import uuid
from typing import Any, Callable

import kio.schema.create_topics.v7 as create_topics_v7
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
from kio.static.primitive import i16, i32, i32Timedelta

from icestream.kafkaserver.topic_backends import topic_backend_for_name


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


async def do_handle_create_topics_request(
    req: CreateTopicsRequest,
    api_version: int,
    callback: Callable[[CreateTopicsResponse], Any],
) -> None:
    _ = api_version
    results = []
    for topic in req.topics:
        log.info("create_topic", topic=topic.name, num_partitions=topic.num_partitions)
        if topic_backend_for_name(topic.name).is_internal:
            result = create_topics_v7.response.CreatableTopicResult(
                name=topic.name,
                topic_id=None,
                error_code=ErrorCode.topic_authorization_failed,
                error_message="cannot create internal topic",
                topic_config_error_code=i16(0),
                num_partitions=i32(-1),
                replication_factor=i16(-1),
                configs=None,
            )
        else:
            result = create_topics_v7.response.CreatableTopicResult(
                name=topic.name,
                topic_id=uuid.uuid4(),
                error_code=ErrorCode.none,
                error_message=None,
                topic_config_error_code=i16(0),
                num_partitions=i32(topic.num_partitions),
                replication_factor=i16(topic.replication_factor),
                configs=None,
            )
        results.append(result)

    response = create_topics_v7.response.CreateTopicsResponse(
        throttle_time=i32Timedelta.parse(datetime.timedelta(milliseconds=0)),
        topics=tuple(results),
    )
    await callback(response)


def create_topics_error_response(
    req: CreateTopicsRequest,
    api_version: int,
    *,
    error_code: ErrorCode,
    error_message: str,
) -> CreateTopicsResponse:
    mod = load_payload_module(0, api_version, EntityType.response)
    results = []
    for topic in req.topics:
        results.append(
            mod.CreatableTopicResult(
                name=topic.name,
                topic_id=None,
                error_code=error_code,
                error_message=error_message,
                topic_config_error_code=i16(0),
                num_partitions=i32(-1),
                replication_factor=i16(-1),
                configs=None,
            )
        )

    return mod.CreateTopicsResponse(
        throttle_time=i32Timedelta.parse(datetime.timedelta(milliseconds=0)),
        topics=tuple(results),
    )
