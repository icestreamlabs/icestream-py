from typing import Any, Awaitable, Callable, Protocol

from kio.schema.errors import ErrorCode

from icestream.kafkaserver.handlers.api_versions import ApiVersionsRequestHeader, ApiVersionsRequest, \
    ApiVersionsResponse
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

from icestream.kafkaserver.handlers.fetch import (
    FetchRequest,
    FetchRequestHeader,
    FetchResponse,
)

class KafkaHandler(Protocol):
    async def handle_produce_request(
        self,
        header: ProduceRequestHeader,
        req: ProduceRequest,
        api_version: int,
        callback: Callable[[ProduceResponse], Any],
    ):
        pass

    def produce_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: ProduceRequest,
        api_version: int,
    ) -> ProduceResponse:
        pass

    async def handle_metadata_request(
        self,
        header: MetadataRequestHeader,
        req: MetadataRequest,
        api_version: int,
        callback: Callable[[MetadataResponse], Any],
    ):
        pass

    def metadata_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: MetadataRequest,
        api_version: int,
    ) -> MetadataResponse:
        pass

    async def handle_api_versions_request(
        self,
        header: ApiVersionsRequestHeader,
        req: ApiVersionsRequest,
        api_version: int,
        callback: Callable[[ApiVersionsResponse], Awaitable[None]],
    ):
        pass

    def api_versions_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: ApiVersionsRequest,
        api_version: int,
    ) -> ApiVersionsResponse:
        pass

    async def handle_create_topics_request(
        self,
        header: CreateTopicsRequestHeader,
        req: CreateTopicsRequest,
        api_version: int,
        callback: Callable[[CreateTopicsResponse], Awaitable[None]],
    ):
        pass

    def create_topics_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: CreateTopicsRequest,
        api_version: int,
    ) -> CreateTopicsResponse:
        pass

    async def handle_fetch_request(
        self,
        header: FetchRequestHeader,
        req: FetchRequest,
        api_version: int,
        callback: Callable[[FetchResponse], Awaitable[None]],
    ) -> FetchResponse:
        pass

    def fetch_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: FetchRequest,
        api_version: int,
    ) -> FetchResponse:
        pass
