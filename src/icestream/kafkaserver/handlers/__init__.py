from typing import Any, Awaitable, Callable, Protocol

from kio.schema.errors import ErrorCode

from icestream.kafkaserver.handlers.add_offsets_to_txn import AddOffsetsToTxnRequestHeader, AddOffsetsToTxnRequest, \
    AddOffsetsToTxnResponse
from icestream.kafkaserver.handlers.add_partitions_to_txn import AddPartitionsToTxnRequestHeader, \
    AddPartitionsToTxnRequest, AddPartitionsToTxnResponse
from icestream.kafkaserver.handlers.add_raft_voter import AddRaftVoterRequestHeader, AddRaftVoterRequest, \
    AddRaftVoterResponse
from icestream.kafkaserver.handlers.alter_client_quotas import AlterClientQuotasRequestHeader, AlterClientQuotasRequest, \
    AlterClientQuotasResponse
from icestream.kafkaserver.handlers.api_versions import ApiVersionsRequestHeader, ApiVersionsRequest, \
    ApiVersionsResponse
from icestream.kafkaserver.handlers.create_topics import CreateTopicsRequest, CreateTopicsResponse, \
    CreateTopicsRequestHeader
from icestream.kafkaserver.handlers.delete_topics import DeleteTopicsRequestHeader, DeleteTopicsRequest, \
    DeleteTopicsResponse
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

    async def handle_delete_topics_request(
            self,
            header: DeleteTopicsRequestHeader,
            req: DeleteTopicsRequest,
            api_version: int,
            callback: Callable[[DeleteTopicsResponse], Awaitable[None]],
    ):
        pass

    def delete_topics_request_error_response(
            self,
            error_code: ErrorCode,
            error_message: str,
            req: DeleteTopicsRequest,
            api_version: int,
    ) -> DeleteTopicsResponse:
        pass

    async def handle_add_offsets_to_txn_request(
            self,
            header: AddOffsetsToTxnRequestHeader,
            req: AddOffsetsToTxnRequest,
            api_version: int,
            callback: Callable[[AddOffsetsToTxnResponse], Awaitable[None]],
    ):
        pass

    def add_offsets_to_txn_request_error_response(
            self,
            error_code: ErrorCode,
            error_message: str,
            req: AddOffsetsToTxnRequest,
            api_version: int,
    ) -> AddOffsetsToTxnResponse:
        pass

    async def handle_add_partitions_to_txn_request(
            self,
            header: AddPartitionsToTxnRequestHeader,
            req: AddPartitionsToTxnRequest,
            api_version: int,
            callback: Callable[[AddPartitionsToTxnResponse], Awaitable[None]],
    ):
        pass

    def add_partitions_to_txn_request_error_response(
            self,
            error_code: ErrorCode,
            error_message: str,
            req: AddPartitionsToTxnRequest,
            api_version: int,
    ) -> AddPartitionsToTxnResponse:
        pass

    async def handle_alter_client_quotas_request(
            self,
            header: AlterClientQuotasRequestHeader,
            req: AlterClientQuotasRequest,
            api_version: int,
            callback: Callable[[AlterClientQuotasResponse], Awaitable[None]],
    ):
        pass

    def alter_client_quotas_request_error_response(
            self,
            error_code: ErrorCode,
            error_message: str,
            req: AlterClientQuotasRequest,
            api_version: int,
    ) -> AlterClientQuotasResponse:
        pass

    async def handle_add_raft_voter_request(
            self,
            header: AddRaftVoterRequestHeader,
            req: AddRaftVoterRequest,
            api_version: int,
            callback: Callable[[AddRaftVoterResponse], Awaitable[None]],
    ):
        pass

    def add_raft_voter_request_error_response(
            self,
            error_code: ErrorCode,
            error_message: str,
            req: AddRaftVoterRequest,
            api_version: int,
    ) -> AddRaftVoterResponse:
        pass
