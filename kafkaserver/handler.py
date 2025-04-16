from dataclasses import dataclass
import io
import struct
from typing import Type, Callable, Awaitable
from asyncio import StreamWriter

from kio.serial import entity_reader, entity_writer
from kio.schema.errors import ErrorCode
from kio.static.constants import EntityType

from kafkaserver.handlers import KafkaHandler
from kafkaserver.messages import ProduceRequest, ProduceRequestHeader, ProduceResponse, ProduceResponseHeader, MetadataRequest, \
    MetadataRequestHeader, MetadataResponse, MetadataResponseHeader

PRODUCE_API_KEY: int = 0
METADATA_API_KEY: int = 3


@dataclass
class RequestHandlerMeta:
    req_class: Type[EntityType.request]
    resp_class: Type[EntityType.response]
    read_req_header: Callable
    read_req: Callable
    write_resp_header: Callable
    write_resp: Callable
    resp_header_class: Type


api_compatibility: dict[int, tuple[int, int]] = {
    PRODUCE_API_KEY: (ProduceRequest.__version__, ProduceRequest.__version__)
}


async def handle_produce(
        handler: KafkaHandler,
        header: ProduceRequestHeader,
        req: ProduceRequest,
        respond: Callable[[ProduceResponse], Awaitable[None]],
) -> None:
    await handler.handle_produce_request(header, req, respond)


# Metadata
async def handle_metadata(
        handler: KafkaHandler,
        header: MetadataRequestHeader,
        req: MetadataRequest,
        respond: Callable[[MetadataResponse], Awaitable[None]],
) -> None:
    await handler.handle_metadata_request(header, req, respond)


# API Versions
async def handle_api_versions(
        handler: KafkaHandler,
        header: ApiVersionsRequest.__header_type__,
        req: ApiVersionsRequest,
        respond: Callable[[ApiVersionsResponse], Awaitable[None]],
) -> None:
    await handler.handle_api_versions_request(header, req, respond)


# Create Topics
async def handle_create_topics(
        handler: KafkaHandler,
        header: CreateTopicsRequest.__header_type__,
        req: CreateTopicsRequest,
        respond: Callable[[CreateTopicsResponse], Awaitable[None]],
) -> None:
    await handler.handle_create_topics_request(header, req, respond)


request_map: dict[int, RequestHandlerMeta] = {
    PRODUCE_API_KEY: RequestHandlerMeta(
        req_class=ProduceRequest,
        resp_class=ProduceResponse,
        read_req_header=entity_reader(ProduceRequest.__header_schema__),
        read_req=entity_reader(ProduceRequest),
        write_resp_header=entity_writer(ProduceResponse.__header_schema__),
        write_resp=entity_writer(ProduceResponse),
        resp_header_class=ProduceResponseHeader,
        handler_func=handle_produce,
    ),
    METADATA_API_KEY: RequestHandlerMeta(
        req_class=MetadataRequest,
        resp_class=MetadataResponse,
        read_req_header=entity_reader(MetadataRequest.__header_schema__),
        read_req=entity_reader(MetadataRequest),
        write_resp_header=entity_writer(MetadataResponse.__header_schema__),
        write_resp=entity_writer(MetadataResponse),
        resp_header_class=MetadataResponseHeader,
        handler_func=handle_metadata,
    ),
    API_VERSIONS_API_KEY: RequestHandlerMeta(
        req_class=ApiVersionsRequest,
        resp_class=ApiVersionsResponse,
        read_req_header=entity_reader(ApiVersionsRequest.__header_schema__),
        read_req=entity_reader(ApiVersionsRequest),
        write_resp_header=entity_writer(ApiVersionsResponse.__header_schema__),
        write_resp=entity_writer(ApiVersionsResponse),
        resp_header_class=ApiVersionsResponseHeader,
        handler_func=handle_api_versions,
    ),
    CREATE_TOPICS_API_KEY: RequestHandlerMeta(
        req_class=CreateTopicsRequest,
        resp_class=CreateTopicsResponse,
        read_req_header=entity_reader(CreateTopicsRequest.__header_schema__),
        read_req=entity_reader(CreateTopicsRequest),
        write_resp_header=entity_writer(CreateTopicsResponse.__header_schema__),
        write_resp=entity_writer(CreateTopicsResponse),
        resp_header_class=CreateTopicsResponseHeader,
        handler_func=handle_create_topics,
    ),
}


async def handle_kafka_request(api_key: int, buffer: bytes, handler: KafkaHandler, writer: StreamWriter):
    api_version = struct.unpack('>H', buffer[2:4])[0]
    buffer = io.BytesIO(buffer)
    match api_key:
        case 0:
            read_req_header = entity_reader(ProduceRequest.__header_schema__)
            read_req = entity_reader(ProduceRequest)
            write_resp_header = entity_writer(ProduceResponse.__header_schema__)
            write_resp = entity_writer(ProduceResponse)

            min_vers, max_vers = api_compatibility[api_key]
            req_header = read_req_header(buffer)
            req = read_req(buffer)
            response_header = ProduceResponseHeader(correlation_id=req_header.correlation_id)

            async def resp_func(resp: EntityType.response):
                resp_buffer = io.BytesIO()
                write_resp_header(resp_buffer, response_header)
                write_resp(resp_buffer, resp)
                resp_buffer_size = resp_buffer.getbuffer().nbytes
                new_bytes_resp = bytearray()
                new_bytes_resp += resp_buffer_size.to_bytes(4, byteorder='big', signed=False)
                resp_buffer.seek(0)
                new_bytes_resp += resp_buffer.read()
                writer.write(new_bytes_resp)
                await writer.drain()

            if not _is_in_supported_range(api_version, min_vers, max_vers):
                error_resp = handler.produce_request_error_response(
                    ErrorCode.unsupported_version,
                    f"supported versions for api key {api_key} are {min_vers} through {max_vers}",
                    req
                )
                await resp_func(error_resp)
                return
            await handler.handle_produce_request(req_header, req, resp_func)
        case 1:
            def resp_func():
                pass
        case _:
            return


def _is_in_supported_range(num: int, min_val: int, max_val: int) -> bool:
    return min_val <= num <= max_val
