from typing import Protocol, Callable, Any, Awaitable

from kio.schema.errors import ErrorCode
from kafkaserver.messages import ProduceRequest, ProduceResponse, ProduceRequestHeader, MetadataRequestHeader, MetadataRequest, MetadataResponse

class KafkaHandler(Protocol):
    async def handle_produce_request(self, header: ProduceRequestHeader, req: ProduceRequest, callback: Callable[[ProduceResponse], Any]): pass
    def produce_request_error_response(self, error_code: ErrorCode, error_message: str, req: ProduceRequest) -> ProduceResponse: pass
    async def handle_metadata_request(self, header: MetadataRequestHeader, req: MetadataRequest, callback: Callable[[MetadataResponse], Any]): pass
    def metadata_request_error_response(self, error_code: ErrorCode, error_message: str, req: MetadataRequest) -> MetadataResponse: pass
    async def handle_api_versions_request(self, header: ApiVersionsRequestHeader, req: ApiVersionsRequest, callback: Callable[[ApiVersionsResponse], Awaitable[None]]): pass
    def api_versions_request_error_response(self, error_code: ErrorCode, error_message: str, req: ApiVersionsRequest) -> ApiVersionsResponse: pass
    async def handle_create_topics_request(self, header: CreateTopicsRequestHeader, req: CreateTopicsRequest, callback: Callable[[CreateTopicsResponse], Awaitable[None]]): pass
    def create_topics_request_error_response(self, error_code: ErrorCode, error_message: str, req: CreateTopicsRequest) -> CreateTopicsResponse: pass
