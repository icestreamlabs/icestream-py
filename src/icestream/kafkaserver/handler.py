import io
import struct
from asyncio import StreamWriter
from dataclasses import dataclass
from typing import Awaitable, Callable

import structlog
from kio.index import load_request_schema, load_response_schema
from kio.schema.errors import ErrorCode
from kio.serial import entity_reader, entity_writer
from kio.static.constants import EntityType

from icestream.kafkaserver.handlers import KafkaHandler
from icestream.kafkaserver.handlers.api_versions import (
    ApiVersionsRequest,
    ApiVersionsRequestHeader,
    ApiVersionsResponse,
)
from icestream.kafkaserver.handlers.metadata import (
    MetadataRequest,
    MetadataRequestHeader,
    MetadataResponse,
)
from icestream.kafkaserver.handlers.produce import (
    ProduceRequest,
    ProduceRequestHeader,
    ProduceResponse,
)
from icestream.kafkaserver.messages import (
    CreatableTopicResult,
    CreateTopicsRequest,
    CreateTopicsRequestHeader,
    CreateTopicsResponse,
)

log = structlog.get_logger()

PRODUCE_API_KEY = 0
METADATA_API_KEY = 3
API_VERSIONS_API_KEY = 18
CREATE_TOPICS_API_KEY = 19


@dataclass
class RequestHandlerMeta:
    handler_func: Callable[
        [
            KafkaHandler,
            EntityType.header,
            EntityType.request,
            int,
            Callable[[EntityType.response], Awaitable[None]],
        ],
        Awaitable[None],
    ]
    error_response_func: Callable[
        [KafkaHandler, ErrorCode, str, EntityType.request, int], EntityType.response
    ]


api_compatibility: dict[int, tuple[int, int]] = {
    PRODUCE_API_KEY: (0, 8),
    METADATA_API_KEY: (0, 4),
    API_VERSIONS_API_KEY: (0, 4),
    CREATE_TOPICS_API_KEY: (0, 4),
}


async def handle_produce(
    handler: KafkaHandler,
    header: ProduceRequestHeader,
    req: ProduceRequest,
    api_version: int,
    respond: Callable[[ProduceResponse], Awaitable[None]],
) -> None:
    await handler.handle_produce_request(header, req, api_version, respond)


async def handle_metadata(
    handler: KafkaHandler,
    header: MetadataRequestHeader,
    req: MetadataRequest,
    api_version: int,
    respond: Callable[[MetadataResponse], Awaitable[None]],
) -> None:
    await handler.handle_metadata_request(header, req, api_version, respond)


async def handle_api_versions(
    handler: KafkaHandler,
    header: ApiVersionsRequestHeader,
    req: ApiVersionsRequest,
    api_version: int,
    respond: Callable[[ApiVersionsResponse], Awaitable[None]],
) -> None:
    await handler.handle_api_versions_request(header, req, api_version, respond)


async def handle_create_topics(
    handler: KafkaHandler,
    header: CreateTopicsRequestHeader,
    req: CreateTopicsRequest,
    api_version: int,
    respond: Callable[[CreateTopicsResponse], Awaitable[None]],
) -> None:
    await handler.handle_create_topics_request(header, req, api_version, respond)


def error_produce(
    handler: KafkaHandler,
    code: ErrorCode,
    msg: str,
    req: ProduceRequest,
    api_version: int,
) -> ProduceResponse:
    return handler.produce_request_error_response(code, msg, req, api_version)


def error_metadata(
    handler: KafkaHandler,
    code: ErrorCode,
    msg: str,
    req: MetadataRequest,
    api_version: int,
) -> MetadataResponse:
    return handler.metadata_request_error_response(code, msg, req, api_version)


def error_api_versions(
    handler: KafkaHandler,
    code: ErrorCode,
    msg: str,
    req: ApiVersionsRequest,
    api_version: int,
) -> ApiVersionsResponse:
    return handler.api_versions_request_error_response(code, msg, req, api_version)


def error_create_topics(
    handler: KafkaHandler,
    code: ErrorCode,
    msg: str,
    req: CreateTopicsRequest,
    api_version: int,
) -> CreateTopicsResponse:
    return handler.create_topics_request_error_response(code, msg, req, api_version)


request_map: dict[int, RequestHandlerMeta] = {
    PRODUCE_API_KEY: RequestHandlerMeta(
        handler_func=handle_produce,
        error_response_func=error_produce,
    ),
    METADATA_API_KEY: RequestHandlerMeta(
        handler_func=handle_metadata,
        error_response_func=error_metadata,
    ),
    API_VERSIONS_API_KEY: RequestHandlerMeta(
        handler_func=handle_api_versions,
        error_response_func=error_api_versions,
    ),
    CREATE_TOPICS_API_KEY: RequestHandlerMeta(
        handler_func=handle_create_topics,
        error_response_func=error_create_topics,
    ),
}


async def handle_kafka_request(
    api_key: int, buffer: bytes, handler: KafkaHandler, writer: StreamWriter
):
    if api_key not in request_map or api_key not in api_compatibility:
        return

    api_version = struct.unpack(">H", buffer[2:4])[0]
    buffer = io.BytesIO(buffer)

    log.info(f"got api key {api_key} with api version {api_version}")

    meta = request_map[api_key]
    min_vers, max_vers = api_compatibility[api_key]

    try:
        req_cls = load_request_schema(api_key, api_version)
        read_req = entity_reader(req_cls)
        read_req_header = entity_reader(req_cls.__header_schema__)

        req_header = read_req_header(buffer)
        req = read_req(buffer)

        resp_cls = load_response_schema(api_key, api_version)
        write_resp = entity_writer(resp_cls)
        write_resp_header = entity_writer(resp_cls.__header_schema__)

        response_header = resp_cls.__header_schema__(
            correlation_id=req_header.correlation_id
        )

        async def resp_func(resp: EntityType.response):
            resp_buffer = io.BytesIO()
            write_resp_header(resp_buffer, response_header)
            write_resp(resp_buffer, resp)
            resp_bytes = resp_buffer.getvalue()
            writer.write(len(resp_bytes).to_bytes(4, "big") + resp_bytes)
            await writer.drain()

        if not _is_in_supported_range(api_version, min_vers, max_vers):
            msg = f"supported versions for api key {api_key} are {min_vers} through {max_vers}"
            error_resp = meta.error_response_func(
                handler, ErrorCode.unsupported_version, msg, req, api_version
            )
            await resp_func(error_resp)
            return

        await meta.handler_func(handler, req_header, req, api_version, resp_func)

    except Exception as e:
        log.exception(
            "failed to handle kafka request",
            api_key=api_key,
            version=api_version,
            exception=e,
        )


def _is_in_supported_range(num: int, min_val: int, max_val: int) -> bool:
    return min_val <= num <= max_val
