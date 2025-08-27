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
from icestream.kafkaserver.handlers.delete_acls import (
    DeleteAclsRequest,
    DeleteAclsRequestHeader,
    DeleteAclsResponse,
)
from icestream.kafkaserver.handlers.delete_groups import (
    DeleteGroupsRequest,
    DeleteGroupsRequestHeader,
    DeleteGroupsResponse,
)
from icestream.kafkaserver.handlers.delete_records import (
    DeleteRecordsRequest,
    DeleteRecordsRequestHeader,
    DeleteRecordsResponse,
)
from icestream.kafkaserver.handlers.delete_share_group_state import (
    DeleteShareGroupStateRequest,
    DeleteShareGroupStateRequestHeader,
    DeleteShareGroupStateResponse,
)
from icestream.kafkaserver.handlers.describe_acls import (
    DescribeAclsRequest,
    DescribeAclsRequestHeader,
    DescribeAclsResponse,
)
from icestream.kafkaserver.handlers.describe_client_quotas import (
    DescribeClientQuotasRequest,
    DescribeClientQuotasRequestHeader,
    DescribeClientQuotasResponse,
)
from icestream.kafkaserver.handlers.describe_cluster import (
    DescribeClusterRequest,
    DescribeClusterRequestHeader,
    DescribeClusterResponse,
)
from icestream.kafkaserver.handlers.describe_configs import (
    DescribeConfigsRequest,
    DescribeConfigsRequestHeader,
    DescribeConfigsResponse,
)
from icestream.kafkaserver.handlers.describe_delegation_token import (
    DescribeDelegationTokenRequest,
    DescribeDelegationTokenRequestHeader,
    DescribeDelegationTokenResponse,
)
from icestream.kafkaserver.handlers.describe_groups import (
    DescribeGroupsRequest,
    DescribeGroupsRequestHeader,
    DescribeGroupsResponse,
)
from icestream.kafkaserver.handlers.describe_log_dirs import (
    DescribeLogDirsRequest,
    DescribeLogDirsRequestHeader,
    DescribeLogDirsResponse,
)
from icestream.kafkaserver.handlers.describe_producers import (
    DescribeProducersRequest,
    DescribeProducersRequestHeader,
    DescribeProducersResponse,
)
from icestream.kafkaserver.handlers.describe_quorum import (
    DescribeQuorumRequest,
    DescribeQuorumRequestHeader,
    DescribeQuorumResponse,
)
from icestream.kafkaserver.handlers.describe_topic_partitions import (
    DescribeTopicPartitionsRequest,
    DescribeTopicPartitionsRequestHeader,
    DescribeTopicPartitionsResponse,
)
from icestream.kafkaserver.handlers.describe_transactions import (
    DescribeTransactionsRequest,
    DescribeTransactionsRequestHeader,
    DescribeTransactionsResponse,
)
from icestream.kafkaserver.handlers.describe_user_scram_credentials import (
    DescribeUserScramCredentialsRequest,
    DescribeUserScramCredentialsRequestHeader,
    DescribeUserScramCredentialsResponse,
)
from icestream.kafkaserver.handlers.elect_leaders import (
    ElectLeadersRequest,
    ElectLeadersRequestHeader,
    ElectLeadersResponse,
)
from icestream.kafkaserver.handlers.end_quorum_epoch import (
    EndQuorumEpochRequest,
    EndQuorumEpochRequestHeader,
    EndQuorumEpochResponse,
)
from icestream.kafkaserver.handlers.end_txn import (
    EndTxnRequest,
    EndTxnRequestHeader,
    EndTxnResponse,
)
from icestream.kafkaserver.handlers.envelope import (
    EnvelopeRequest,
    EnvelopeRequestHeader,
    EnvelopeResponse,
)
from icestream.kafkaserver.handlers.create_acls import (
    CreateAclsRequest,
    CreateAclsRequestHeader,
    CreateAclsResponse,
)
from icestream.kafkaserver.handlers.create_delegation_token import (
    CreateDelegationTokenRequest,
    CreateDelegationTokenRequestHeader,
    CreateDelegationTokenResponse,
)
from icestream.kafkaserver.handlers.create_partitions import (
    CreatePartitionsRequest,
    CreatePartitionsRequestHeader,
    CreatePartitionsResponse,
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

from icestream.kafkaserver.handlers.fetch import (
    FetchRequest,
    FetchRequestHeader,
    FetchResponse,
)
from icestream.kafkaserver.handlers.allocate_producer_ids import AllocateProducerIdsRequestHeader, \
    AllocateProducerIdsRequest, AllocateProducerIdsResponse

from icestream.kafkaserver.handlers.alter_configs import (
    AlterConfigsRequest,
    AlterConfigsRequestHeader,
    AlterConfigsResponse,
)
from icestream.kafkaserver.handlers.alter_partition import (
    AlterPartitionRequest,
    AlterPartitionRequestHeader,
    AlterPartitionResponse,
)
from icestream.kafkaserver.handlers.alter_partition_reassignments import (
    AlterPartitionReassignmentsRequest,
    AlterPartitionReassignmentsRequestHeader,
    AlterPartitionReassignmentsResponse,
)
from icestream.kafkaserver.handlers.alter_replica_log_dirs import (
    AlterReplicaLogDirsRequest,
    AlterReplicaLogDirsRequestHeader,
    AlterReplicaLogDirsResponse,
)
from icestream.kafkaserver.handlers.alter_user_scram_credentials import (
    AlterUserScramCredentialsRequest,
    AlterUserScramCredentialsRequestHeader,
    AlterUserScramCredentialsResponse,
)
from icestream.kafkaserver.handlers.assign_replicas_to_dirs import (
    AssignReplicasToDirsRequest,
    AssignReplicasToDirsRequestHeader,
    AssignReplicasToDirsResponse,
)
from icestream.kafkaserver.handlers.begin_quorum_epoch import (
    BeginQuorumEpochRequest,
    BeginQuorumEpochRequestHeader,
    BeginQuorumEpochResponse,
)
from icestream.kafkaserver.handlers.broker_heartbeat import (
    BrokerHeartbeatRequest,
    BrokerHeartbeatRequestHeader,
    BrokerHeartbeatResponse,
)
from icestream.kafkaserver.handlers.broker_registration import (
    BrokerRegistrationRequest,
    BrokerRegistrationRequestHeader,
    BrokerRegistrationResponse,
)
from icestream.kafkaserver.handlers.consumer_group_describe import (
    ConsumerGroupDescribeRequest,
    ConsumerGroupDescribeRequestHeader,
    ConsumerGroupDescribeResponse,
)
from icestream.kafkaserver.handlers.consumer_group_heartbeat import (
    ConsumerGroupHeartbeatRequest,
    ConsumerGroupHeartbeatRequestHeader,
    ConsumerGroupHeartbeatResponse,
)
from icestream.kafkaserver.handlers.controlled_shutdown import (
    ControlledShutdownRequest,
    ControlledShutdownRequestHeader,
    ControlledShutdownResponse,
)
from icestream.kafkaserver.handlers.controller_registration import (
    ControllerRegistrationRequest,
    ControllerRegistrationRequestHeader,
    ControllerRegistrationResponse,
)
from icestream.kafkaserver.handlers.find_coordinator import (
    FindCoordinatorRequest,
    FindCoordinatorRequestHeader,
    FindCoordinatorResponse,
)
from icestream.kafkaserver.handlers.expire_delegation_token import (
    ExpireDelegationTokenRequest,
    ExpireDelegationTokenRequestHeader,
    ExpireDelegationTokenResponse,
)
from icestream.kafkaserver.handlers.fetch_snapshot import (
    FetchSnapshotRequest,
    FetchSnapshotRequestHeader,
    FetchSnapshotResponse,
)
from icestream.kafkaserver.handlers.get_telemetry_subscriptions import (
    GetTelemetrySubscriptionsRequest,
    GetTelemetrySubscriptionsRequestHeader,
    GetTelemetrySubscriptionsResponse,
)
from icestream.kafkaserver.handlers.heartbeat import (
    HeartbeatRequest,
    HeartbeatRequestHeader,
    HeartbeatResponse,
)
from icestream.kafkaserver.handlers.incremental_alter_configs import (
    IncrementalAlterConfigsRequest,
    IncrementalAlterConfigsRequestHeader,
    IncrementalAlterConfigsResponse,
)
from icestream.kafkaserver.handlers.init_producer_id import (
    InitProducerIdRequest,
    InitProducerIdRequestHeader,
    InitProducerIdResponse,
)
from icestream.kafkaserver.handlers.initialize_share_group_state import (
    InitializeShareGroupStateRequest,
    InitializeShareGroupStateRequestHeader,
    InitializeShareGroupStateResponse,
)
from icestream.kafkaserver.handlers.join_group import (
    JoinGroupRequest,
    JoinGroupRequestHeader,
    JoinGroupResponse,
)
from icestream.kafkaserver.handlers.leader_and_isr import (
    LeaderAndIsrRequest,
    LeaderAndIsrRequestHeader,
    LeaderAndIsrResponse,
)
from icestream.kafkaserver.handlers.leave_group import (
    LeaveGroupRequest,
    LeaveGroupRequestHeader,
    LeaveGroupResponse,
)
from icestream.kafkaserver.handlers.list_client_metrics_resources import (
    ListClientMetricsResourcesRequest,
    ListClientMetricsResourcesRequestHeader,
    ListClientMetricsResourcesResponse,
)
from icestream.kafkaserver.handlers.list_groups import (
    ListGroupsRequest,
    ListGroupsRequestHeader,
    ListGroupsResponse,
)
from icestream.kafkaserver.handlers.list_offsets import (
    ListOffsetsRequest,
    ListOffsetsRequestHeader,
    ListOffsetsResponse,
)
from icestream.kafkaserver.handlers.list_partition_reassignments import (
    ListPartitionReassignmentsRequest,
    ListPartitionReassignmentsRequestHeader,
    ListPartitionReassignmentsResponse,
)
from icestream.kafkaserver.handlers.list_transactions import (
    ListTransactionsRequest,
    ListTransactionsRequestHeader,
    ListTransactionsResponse,
)
from icestream.kafkaserver.handlers.offset_commit import (
    OffsetCommitRequest,
    OffsetCommitRequestHeader,
    OffsetCommitResponse,
)
from icestream.kafkaserver.handlers.offset_delete import (
    OffsetDeleteRequest,
    OffsetDeleteRequestHeader,
    OffsetDeleteResponse,
)
from icestream.kafkaserver.handlers.offset_fetch import (
    OffsetFetchRequest,
    OffsetFetchRequestHeader,
    OffsetFetchResponse,
)
from icestream.kafkaserver.handlers.offset_for_leader_epoch import (
    OffsetForLeaderEpochRequest,
    OffsetForLeaderEpochRequestHeader,
    OffsetForLeaderEpochResponse,
)
from icestream.kafkaserver.handlers.push_telemetry import (
    PushTelemetryRequest,
    PushTelemetryRequestHeader,
    PushTelemetryResponse,
)
from icestream.kafkaserver.handlers.read_share_group_state import (
    ReadShareGroupStateRequest,
    ReadShareGroupStateRequestHeader,
    ReadShareGroupStateResponse,
)
from icestream.kafkaserver.handlers.read_share_group_state_summary import (
    ReadShareGroupStateSummaryRequest,
    ReadShareGroupStateSummaryRequestHeader,
    ReadShareGroupStateSummaryResponse,
)
from icestream.kafkaserver.handlers.remove_raft_voter import (
    RemoveRaftVoterRequest,
    RemoveRaftVoterRequestHeader,
    RemoveRaftVoterResponse,
)
from icestream.kafkaserver.handlers.renew_delegation_token import (
    RenewDelegationTokenRequest,
    RenewDelegationTokenRequestHeader,
    RenewDelegationTokenResponse,
)
from icestream.kafkaserver.handlers.sasl_authenticate import (
    SaslAuthenticateRequest,
    SaslAuthenticateRequestHeader,
    SaslAuthenticateResponse,
)
from icestream.kafkaserver.handlers.sasl_handshake import (
    SaslHandshakeRequest,
    SaslHandshakeRequestHeader,
    SaslHandshakeResponse,
)
from icestream.kafkaserver.handlers.share_acknowledge import (
    ShareAcknowledgeRequest,
    ShareAcknowledgeRequestHeader,
    ShareAcknowledgeResponse,
)
from icestream.kafkaserver.handlers.share_fetch import (
    ShareFetchRequest,
    ShareFetchRequestHeader,
    ShareFetchResponse,
)
from icestream.kafkaserver.handlers.share_group_describe import (
    ShareGroupDescribeRequest,
    ShareGroupDescribeRequestHeader,
    ShareGroupDescribeResponse,
)
from icestream.kafkaserver.handlers.share_group_heartbeat import (
    ShareGroupHeartbeatRequest,
    ShareGroupHeartbeatRequestHeader,
    ShareGroupHeartbeatResponse,
)
from icestream.kafkaserver.handlers.stop_replica import (
    StopReplicaRequest,
    StopReplicaRequestHeader,
    StopReplicaResponse,
)
from icestream.kafkaserver.handlers.sync_group import (
    SyncGroupRequest,
    SyncGroupRequestHeader,
    SyncGroupResponse,
)
from icestream.kafkaserver.handlers.txn_offset_commit import (
    TxnOffsetCommitRequest,
    TxnOffsetCommitRequestHeader,
    TxnOffsetCommitResponse,
)
from icestream.kafkaserver.handlers.unregister_broker import (
    UnregisterBrokerRequest,
    UnregisterBrokerRequestHeader,
    UnregisterBrokerResponse,
)
from icestream.kafkaserver.handlers.update_features import (
    UpdateFeaturesRequest,
    UpdateFeaturesRequestHeader,
    UpdateFeaturesResponse,
)
from icestream.kafkaserver.handlers.update_metadata import (
    UpdateMetadataRequest,
    UpdateMetadataRequestHeader,
    UpdateMetadataResponse,
)
from icestream.kafkaserver.handlers.vote import (
    VoteRequest,
    VoteRequestHeader,
    VoteResponse,
)
from icestream.kafkaserver.handlers.write_txn_markers import (
    WriteTxnMarkersRequest,
    WriteTxnMarkersRequestHeader,
    WriteTxnMarkersResponse,
)
from icestream.kafkaserver.handlers.update_raft_voter import (
    UpdateRaftVoterRequest,
    UpdateRaftVoterRequestHeader,
    UpdateRaftVoterResponse,
)
from icestream.kafkaserver.handlers.write_share_group_state import (
    WriteShareGroupStateRequest,
    WriteShareGroupStateRequestHeader,
    WriteShareGroupStateResponse,
)


class KafkaHandler(Protocol):
    async def handle_write_share_group_state_request(
        self,
        header: WriteShareGroupStateRequestHeader,
        req: WriteShareGroupStateRequest,
        api_version: int,
        callback: Callable[[WriteShareGroupStateResponse], Awaitable[None]],
    ):
        pass

    def write_share_group_state_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: WriteShareGroupStateRequest,
        api_version: int,
    ) -> WriteShareGroupStateResponse:
        pass

    async def handle_update_raft_voter_request(
        self,
        header: UpdateRaftVoterRequestHeader,
        req: UpdateRaftVoterRequest,
        api_version: int,
        callback: Callable[[UpdateRaftVoterResponse], Awaitable[None]],
    ):
        pass

    def update_raft_voter_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: UpdateRaftVoterRequest,
        api_version: int,
    ) -> UpdateRaftVoterResponse:
        pass

    async def handle_write_txn_markers_request(
        self,
        header: WriteTxnMarkersRequestHeader,
        req: WriteTxnMarkersRequest,
        api_version: int,
        callback: Callable[[WriteTxnMarkersResponse], Awaitable[None]],
    ):
        pass

    def write_txn_markers_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: WriteTxnMarkersRequest,
        api_version: int,
    ) -> WriteTxnMarkersResponse:
        pass

    async def handle_vote_request(
        self,
        header: VoteRequestHeader,
        req: VoteRequest,
        api_version: int,
        callback: Callable[[VoteResponse], Awaitable[None]],
    ):
        pass

    def vote_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: VoteRequest,
        api_version: int,
    ) -> VoteResponse:
        pass

    async def handle_update_metadata_request(
        self,
        header: UpdateMetadataRequestHeader,
        req: UpdateMetadataRequest,
        api_version: int,
        callback: Callable[[UpdateMetadataResponse], Awaitable[None]],
    ):
        pass

    def update_metadata_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: UpdateMetadataRequest,
        api_version: int,
    ) -> UpdateMetadataResponse:
        pass

    async def handle_update_features_request(
        self,
        header: UpdateFeaturesRequestHeader,
        req: UpdateFeaturesRequest,
        api_version: int,
        callback: Callable[[UpdateFeaturesResponse], Awaitable[None]],
    ):
        pass

    def update_features_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: UpdateFeaturesRequest,
        api_version: int,
    ) -> UpdateFeaturesResponse:
        pass

    async def handle_unregister_broker_request(
        self,
        header: UnregisterBrokerRequestHeader,
        req: UnregisterBrokerRequest,
        api_version: int,
        callback: Callable[[UnregisterBrokerResponse], Awaitable[None]],
    ):
        pass

    def unregister_broker_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: UnregisterBrokerRequest,
        api_version: int,
    ) -> UnregisterBrokerResponse:
        pass

    async def handle_txn_offset_commit_request(
        self,
        header: TxnOffsetCommitRequestHeader,
        req: TxnOffsetCommitRequest,
        api_version: int,
        callback: Callable[[TxnOffsetCommitResponse], Awaitable[None]],
    ):
        pass

    def txn_offset_commit_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: TxnOffsetCommitRequest,
        api_version: int,
    ) -> TxnOffsetCommitResponse:
        pass

    async def handle_sync_group_request(
        self,
        header: SyncGroupRequestHeader,
        req: SyncGroupRequest,
        api_version: int,
        callback: Callable[[SyncGroupResponse], Awaitable[None]],
    ):
        pass

    def sync_group_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: SyncGroupRequest,
        api_version: int,
    ) -> SyncGroupResponse:
        pass

    async def handle_stop_replica_request(
        self,
        header: StopReplicaRequestHeader,
        req: StopReplicaRequest,
        api_version: int,
        callback: Callable[[StopReplicaResponse], Awaitable[None]],
    ):
        pass

    def stop_replica_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: StopReplicaRequest,
        api_version: int,
    ) -> StopReplicaResponse:
        pass

    async def handle_share_group_heartbeat_request(
        self,
        header: ShareGroupHeartbeatRequestHeader,
        req: ShareGroupHeartbeatRequest,
        api_version: int,
        callback: Callable[[ShareGroupHeartbeatResponse], Awaitable[None]],
    ):
        pass

    def share_group_heartbeat_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: ShareGroupHeartbeatRequest,
        api_version: int,
    ) -> ShareGroupHeartbeatResponse:
        pass

    async def handle_share_group_describe_request(
        self,
        header: ShareGroupDescribeRequestHeader,
        req: ShareGroupDescribeRequest,
        api_version: int,
        callback: Callable[[ShareGroupDescribeResponse], Awaitable[None]],
    ):
        pass

    def share_group_describe_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: ShareGroupDescribeRequest,
        api_version: int,
    ) -> ShareGroupDescribeResponse:
        pass

    async def handle_share_fetch_request(
        self,
        header: ShareFetchRequestHeader,
        req: ShareFetchRequest,
        api_version: int,
        callback: Callable[[ShareFetchResponse], Awaitable[None]],
    ):
        pass

    def share_fetch_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: ShareFetchRequest,
        api_version: int,
    ) -> ShareFetchResponse:
        pass

    async def handle_share_acknowledge_request(
        self,
        header: ShareAcknowledgeRequestHeader,
        req: ShareAcknowledgeRequest,
        api_version: int,
        callback: Callable[[ShareAcknowledgeResponse], Awaitable[None]],
    ):
        pass

    def share_acknowledge_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: ShareAcknowledgeRequest,
        api_version: int,
    ) -> ShareAcknowledgeResponse:
        pass

    async def handle_sasl_handshake_request(
        self,
        header: SaslHandshakeRequestHeader,
        req: SaslHandshakeRequest,
        api_version: int,
        callback: Callable[[SaslHandshakeResponse], Awaitable[None]],
    ):
        pass

    def sasl_handshake_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: SaslHandshakeRequest,
        api_version: int,
    ) -> SaslHandshakeResponse:
        pass

    async def handle_sasl_authenticate_request(
        self,
        header: SaslAuthenticateRequestHeader,
        req: SaslAuthenticateRequest,
        api_version: int,
        callback: Callable[[SaslAuthenticateResponse], Awaitable[None]],
    ):
        pass

    def sasl_authenticate_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: SaslAuthenticateRequest,
        api_version: int,
    ) -> SaslAuthenticateResponse:
        pass

    async def handle_renew_delegation_token_request(
        self,
        header: RenewDelegationTokenRequestHeader,
        req: RenewDelegationTokenRequest,
        api_version: int,
        callback: Callable[[RenewDelegationTokenResponse], Awaitable[None]],
    ):
        pass

    def renew_delegation_token_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: RenewDelegationTokenRequest,
        api_version: int,
    ) -> RenewDelegationTokenResponse:
        pass

    async def handle_remove_raft_voter_request(
        self,
        header: RemoveRaftVoterRequestHeader,
        req: RemoveRaftVoterRequest,
        api_version: int,
        callback: Callable[[RemoveRaftVoterResponse], Awaitable[None]],
    ):
        pass

    def remove_raft_voter_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: RemoveRaftVoterRequest,
        api_version: int,
    ) -> RemoveRaftVoterResponse:
        pass

    async def handle_read_share_group_state_summary_request(
        self,
        header: ReadShareGroupStateSummaryRequestHeader,
        req: ReadShareGroupStateSummaryRequest,
        api_version: int,
        callback: Callable[[ReadShareGroupStateSummaryResponse], Awaitable[None]],
    ):
        pass

    def read_share_group_state_summary_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: ReadShareGroupStateSummaryRequest,
        api_version: int,
    ) -> ReadShareGroupStateSummaryResponse:
        pass

    async def handle_read_share_group_state_request(
        self,
        header: ReadShareGroupStateRequestHeader,
        req: ReadShareGroupStateRequest,
        api_version: int,
        callback: Callable[[ReadShareGroupStateResponse], Awaitable[None]],
    ):
        pass

    def read_share_group_state_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: ReadShareGroupStateRequest,
        api_version: int,
    ) -> ReadShareGroupStateResponse:
        pass

    async def handle_push_telemetry_request(
        self,
        header: PushTelemetryRequestHeader,
        req: PushTelemetryRequest,
        api_version: int,
        callback: Callable[[PushTelemetryResponse], Awaitable[None]],
    ):
        pass

    def push_telemetry_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: PushTelemetryRequest,
        api_version: int,
    ) -> PushTelemetryResponse:
        pass

    async def handle_offset_for_leader_epoch_request(
        self,
        header: OffsetForLeaderEpochRequestHeader,
        req: OffsetForLeaderEpochRequest,
        api_version: int,
        callback: Callable[[OffsetForLeaderEpochResponse], Awaitable[None]],
    ):
        pass

    def offset_for_leader_epoch_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: OffsetForLeaderEpochRequest,
        api_version: int,
    ) -> OffsetForLeaderEpochResponse:
        pass

    async def handle_offset_fetch_request(
        self,
        header: OffsetFetchRequestHeader,
        req: OffsetFetchRequest,
        api_version: int,
        callback: Callable[[OffsetFetchResponse], Awaitable[None]],
    ):
        pass

    def offset_fetch_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: OffsetFetchRequest,
        api_version: int,
    ) -> OffsetFetchResponse:
        pass

    async def handle_offset_delete_request(
        self,
        header: OffsetDeleteRequestHeader,
        req: OffsetDeleteRequest,
        api_version: int,
        callback: Callable[[OffsetDeleteResponse], Awaitable[None]],
    ):
        pass

    def offset_delete_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: OffsetDeleteRequest,
        api_version: int,
    ) -> OffsetDeleteResponse:
        pass

    async def handle_offset_commit_request(
        self,
        header: OffsetCommitRequestHeader,
        req: OffsetCommitRequest,
        api_version: int,
        callback: Callable[[OffsetCommitResponse], Awaitable[None]],
    ):
        pass

    def offset_commit_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: OffsetCommitRequest,
        api_version: int,
    ) -> OffsetCommitResponse:
        pass

    async def handle_list_transactions_request(
        self,
        header: ListTransactionsRequestHeader,
        req: ListTransactionsRequest,
        api_version: int,
        callback: Callable[[ListTransactionsResponse], Awaitable[None]],
    ):
        pass

    def list_transactions_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: ListTransactionsRequest,
        api_version: int,
    ) -> ListTransactionsResponse:
        pass

    async def handle_list_partition_reassignments_request(
        self,
        header: ListPartitionReassignmentsRequestHeader,
        req: ListPartitionReassignmentsRequest,
        api_version: int,
        callback: Callable[[ListPartitionReassignmentsResponse], Awaitable[None]],
    ):
        pass

    def list_partition_reassignments_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: ListPartitionReassignmentsRequest,
        api_version: int,
    ) -> ListPartitionReassignmentsResponse:
        pass

    async def handle_list_offsets_request(
        self,
        header: ListOffsetsRequestHeader,
        req: ListOffsetsRequest,
        api_version: int,
        callback: Callable[[ListOffsetsResponse], Awaitable[None]],
    ):
        pass

    def list_offsets_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: ListOffsetsRequest,
        api_version: int,
    ) -> ListOffsetsResponse:
        pass

    async def handle_list_groups_request(
        self,
        header: ListGroupsRequestHeader,
        req: ListGroupsRequest,
        api_version: int,
        callback: Callable[[ListGroupsResponse], Awaitable[None]],
    ):
        pass

    def list_groups_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: ListGroupsRequest,
        api_version: int,
    ) -> ListGroupsResponse:
        pass

    async def handle_list_client_metrics_resources_request(
        self,
        header: ListClientMetricsResourcesRequestHeader,
        req: ListClientMetricsResourcesRequest,
        api_version: int,
        callback: Callable[[ListClientMetricsResourcesResponse], Awaitable[None]],
    ):
        pass

    def list_client_metrics_resources_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: ListClientMetricsResourcesRequest,
        api_version: int,
    ) -> ListClientMetricsResourcesResponse:
        pass

    async def handle_leave_group_request(
        self,
        header: LeaveGroupRequestHeader,
        req: LeaveGroupRequest,
        api_version: int,
        callback: Callable[[LeaveGroupResponse], Awaitable[None]],
    ):
        pass

    def leave_group_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: LeaveGroupRequest,
        api_version: int,
    ) -> LeaveGroupResponse:
        pass

    async def handle_leader_and_isr_request(
        self,
        header: LeaderAndIsrRequestHeader,
        req: LeaderAndIsrRequest,
        api_version: int,
        callback: Callable[[LeaderAndIsrResponse], Awaitable[None]],
    ):
        pass

    def leader_and_isr_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: LeaderAndIsrRequest,
        api_version: int,
    ) -> LeaderAndIsrResponse:
        pass

    async def handle_join_group_request(
        self,
        header: JoinGroupRequestHeader,
        req: JoinGroupRequest,
        api_version: int,
        callback: Callable[[JoinGroupResponse], Awaitable[None]],
    ):
        pass

    def join_group_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: JoinGroupRequest,
        api_version: int,
    ) -> JoinGroupResponse:
        pass

    async def handle_initialize_share_group_state_request(
        self,
        header: InitializeShareGroupStateRequestHeader,
        req: InitializeShareGroupStateRequest,
        api_version: int,
        callback: Callable[[InitializeShareGroupStateResponse], Awaitable[None]],
    ):
        pass

    def initialize_share_group_state_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: InitializeShareGroupStateRequest,
        api_version: int,
    ) -> InitializeShareGroupStateResponse:
        pass

    async def handle_init_producer_id_request(
        self,
        header: InitProducerIdRequestHeader,
        req: InitProducerIdRequest,
        api_version: int,
        callback: Callable[[InitProducerIdResponse], Awaitable[None]],
    ):
        pass

    def init_producer_id_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: InitProducerIdRequest,
        api_version: int,
    ) -> InitProducerIdResponse:
        pass

    async def handle_incremental_alter_configs_request(
        self,
        header: IncrementalAlterConfigsRequestHeader,
        req: IncrementalAlterConfigsRequest,
        api_version: int,
        callback: Callable[[IncrementalAlterConfigsResponse], Awaitable[None]],
    ):
        pass

    def incremental_alter_configs_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: IncrementalAlterConfigsRequest,
        api_version: int,
    ) -> IncrementalAlterConfigsResponse:
        pass

    async def handle_heartbeat_request(
        self,
        header: HeartbeatRequestHeader,
        req: HeartbeatRequest,
        api_version: int,
        callback: Callable[[HeartbeatResponse], Awaitable[None]],
    ):
        pass

    def heartbeat_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: HeartbeatRequest,
        api_version: int,
    ) -> HeartbeatResponse:
        pass

    async def handle_get_telemetry_subscriptions_request(
        self,
        header: GetTelemetrySubscriptionsRequestHeader,
        req: GetTelemetrySubscriptionsRequest,
        api_version: int,
        callback: Callable[[GetTelemetrySubscriptionsResponse], Awaitable[None]],
    ):
        pass

    def get_telemetry_subscriptions_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: GetTelemetrySubscriptionsRequest,
        api_version: int,
    ) -> GetTelemetrySubscriptionsResponse:
        pass

    async def handle_fetch_snapshot_request(
        self,
        header: FetchSnapshotRequestHeader,
        req: FetchSnapshotRequest,
        api_version: int,
        callback: Callable[[FetchSnapshotResponse], Awaitable[None]],
    ):
        pass

    def fetch_snapshot_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: FetchSnapshotRequest,
        api_version: int,
    ) -> FetchSnapshotResponse:
        pass

    async def handle_expire_delegation_token_request(
        self,
        header: ExpireDelegationTokenRequestHeader,
        req: ExpireDelegationTokenRequest,
        api_version: int,
        callback: Callable[[ExpireDelegationTokenResponse], Awaitable[None]],
    ):
        pass

    def expire_delegation_token_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: ExpireDelegationTokenRequest,
        api_version: int,
    ) -> ExpireDelegationTokenResponse:
        pass

    async def handle_find_coordinator_request(
        self,
        header: FindCoordinatorRequestHeader,
        req: FindCoordinatorRequest,
        api_version: int,
        callback: Callable[[FindCoordinatorResponse], Awaitable[None]],
    ):
        pass

    def find_coordinator_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: FindCoordinatorRequest,
        api_version: int,
    ) -> FindCoordinatorResponse:
        pass

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

    async def handle_delete_acls_request(
        self,
        header: DeleteAclsRequestHeader,
        req: DeleteAclsRequest,
        api_version: int,
        callback: Callable[[DeleteAclsResponse], Awaitable[None]],
    ):
        pass

    def delete_acls_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: DeleteAclsRequest,
        api_version: int,
    ) -> DeleteAclsResponse:
        pass

    async def handle_delete_groups_request(
        self,
        header: DeleteGroupsRequestHeader,
        req: DeleteGroupsRequest,
        api_version: int,
        callback: Callable[[DeleteGroupsResponse], Awaitable[None]],
    ):
        pass

    def delete_groups_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: DeleteGroupsRequest,
        api_version: int,
    ) -> DeleteGroupsResponse:
        pass

    async def handle_delete_records_request(
        self,
        header: DeleteRecordsRequestHeader,
        req: DeleteRecordsRequest,
        api_version: int,
        callback: Callable[[DeleteRecordsResponse], Awaitable[None]],
    ):
        pass

    def delete_records_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: DeleteRecordsRequest,
        api_version: int,
    ) -> DeleteRecordsResponse:
        pass

    async def handle_delete_share_group_state_request(
        self,
        header: DeleteShareGroupStateRequestHeader,
        req: DeleteShareGroupStateRequest,
        api_version: int,
        callback: Callable[[DeleteShareGroupStateResponse], Awaitable[None]],
    ):
        pass

    def delete_share_group_state_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: DeleteShareGroupStateRequest,
        api_version: int,
    ) -> DeleteShareGroupStateResponse:
        pass

    async def handle_describe_acls_request(
        self,
        header: DescribeAclsRequestHeader,
        req: DescribeAclsRequest,
        api_version: int,
        callback: Callable[[DescribeAclsResponse], Awaitable[None]],
    ):
        pass

    def describe_acls_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: DescribeAclsRequest,
        api_version: int,
    ) -> DescribeAclsResponse:
        pass

    async def handle_describe_client_quotas_request(
        self,
        header: DescribeClientQuotasRequestHeader,
        req: DescribeClientQuotasRequest,
        api_version: int,
        callback: Callable[[DescribeClientQuotasResponse], Awaitable[None]],
    ):
        pass

    def describe_client_quotas_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: DescribeClientQuotasRequest,
        api_version: int,
    ) -> DescribeClientQuotasResponse:
        pass

    async def handle_describe_cluster_request(
        self,
        header: DescribeClusterRequestHeader,
        req: DescribeClusterRequest,
        api_version: int,
        callback: Callable[[DescribeClusterResponse], Awaitable[None]],
    ):
        pass

    def describe_cluster_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: DescribeClusterRequest,
        api_version: int,
    ) -> DescribeClusterResponse:
        pass

    async def handle_describe_configs_request(
        self,
        header: DescribeConfigsRequestHeader,
        req: DescribeConfigsRequest,
        api_version: int,
        callback: Callable[[DescribeConfigsResponse], Awaitable[None]],
    ):
        pass

    def describe_configs_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: DescribeConfigsRequest,
        api_version: int,
    ) -> DescribeConfigsResponse:
        pass

    async def handle_describe_delegation_token_request(
        self,
        header: DescribeDelegationTokenRequestHeader,
        req: DescribeDelegationTokenRequest,
        api_version: int,
        callback: Callable[[DescribeDelegationTokenResponse], Awaitable[None]],
    ):
        pass

    def describe_delegation_token_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: DescribeDelegationTokenRequest,
        api_version: int,
    ) -> DescribeDelegationTokenResponse:
        pass

    async def handle_describe_groups_request(
        self,
        header: DescribeGroupsRequestHeader,
        req: DescribeGroupsRequest,
        api_version: int,
        callback: Callable[[DescribeGroupsResponse], Awaitable[None]],
    ):
        pass

    def describe_groups_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: DescribeGroupsRequest,
        api_version: int,
    ) -> DescribeGroupsResponse:
        pass

    async def handle_describe_log_dirs_request(
        self,
        header: DescribeLogDirsRequestHeader,
        req: DescribeLogDirsRequest,
        api_version: int,
        callback: Callable[[DescribeLogDirsResponse], Awaitable[None]],
    ):
        pass

    def describe_log_dirs_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: DescribeLogDirsRequest,
        api_version: int,
    ) -> DescribeLogDirsResponse:
        pass

    async def handle_describe_producers_request(
        self,
        header: DescribeProducersRequestHeader,
        req: DescribeProducersRequest,
        api_version: int,
        callback: Callable[[DescribeProducersResponse], Awaitable[None]],
    ):
        pass

    def describe_producers_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: DescribeProducersRequest,
        api_version: int,
    ) -> DescribeProducersResponse:
        pass

    async def handle_describe_quorum_request(
        self,
        header: DescribeQuorumRequestHeader,
        req: DescribeQuorumRequest,
        api_version: int,
        callback: Callable[[DescribeQuorumResponse], Awaitable[None]],
    ):
        pass

    def describe_quorum_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: DescribeQuorumRequest,
        api_version: int,
    ) -> DescribeQuorumResponse:
        pass

    async def handle_describe_topic_partitions_request(
        self,
        header: DescribeTopicPartitionsRequestHeader,
        req: DescribeTopicPartitionsRequest,
        api_version: int,
        callback: Callable[[DescribeTopicPartitionsResponse], Awaitable[None]],
    ):
        pass

    def describe_topic_partitions_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: DescribeTopicPartitionsRequest,
        api_version: int,
    ) -> DescribeTopicPartitionsResponse:
        pass

    async def handle_describe_transactions_request(
        self,
        header: DescribeTransactionsRequestHeader,
        req: DescribeTransactionsRequest,
        api_version: int,
        callback: Callable[[DescribeTransactionsResponse], Awaitable[None]],
    ):
        pass

    def describe_transactions_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: DescribeTransactionsRequest,
        api_version: int,
    ) -> DescribeTransactionsResponse:
        pass

    async def handle_describe_user_scram_credentials_request(
        self,
        header: DescribeUserScramCredentialsRequestHeader,
        req: DescribeUserScramCredentialsRequest,
        api_version: int,
        callback: Callable[[DescribeUserScramCredentialsResponse], Awaitable[None]],
    ):
        pass

    def describe_user_scram_credentials_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: DescribeUserScramCredentialsRequest,
        api_version: int,
    ) -> DescribeUserScramCredentialsResponse:
        pass

    async def handle_elect_leaders_request(
        self,
        header: ElectLeadersRequestHeader,
        req: ElectLeadersRequest,
        api_version: int,
        callback: Callable[[ElectLeadersResponse], Awaitable[None]],
    ):
        pass

    def elect_leaders_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: ElectLeadersRequest,
        api_version: int,
    ) -> ElectLeadersResponse:
        pass

    async def handle_end_quorum_epoch_request(
        self,
        header: EndQuorumEpochRequestHeader,
        req: EndQuorumEpochRequest,
        api_version: int,
        callback: Callable[[EndQuorumEpochResponse], Awaitable[None]],
    ):
        pass

    def end_quorum_epoch_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: EndQuorumEpochRequest,
        api_version: int,
    ) -> EndQuorumEpochResponse:
        pass

    async def handle_end_txn_request(
        self,
        header: EndTxnRequestHeader,
        req: EndTxnRequest,
        api_version: int,
        callback: Callable[[EndTxnResponse], Awaitable[None]],
    ):
        pass

    def end_txn_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: EndTxnRequest,
        api_version: int,
    ) -> EndTxnResponse:
        pass

    async def handle_envelope_request(
        self,
        header: EnvelopeRequestHeader,
        req: EnvelopeRequest,
        api_version: int,
        callback: Callable[[EnvelopeResponse], Awaitable[None]],
    ):
        pass

    def envelope_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: EnvelopeRequest,
        api_version: int,
    ) -> EnvelopeResponse:
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

    async def handle_allocate_producer_ids_request(
            self,
            header: AllocateProducerIdsRequestHeader,
            req: AllocateProducerIdsRequest,
            api_version: int,
            callback: Callable[[AllocateProducerIdsResponse], Awaitable[None]],
    ):
        pass

    def allocate_producer_ids_request_error_response(
            self,
            error_code: ErrorCode,
            error_message: str,
            req: AllocateProducerIdsRequest,
            api_version: int,
    ) -> AllocateProducerIdsResponse:
        pass

    async def handle_alter_configs_request(
            self,
            header: AlterConfigsRequestHeader,
            req: AlterConfigsRequest,
            api_version: int,
            callback: Callable[[AlterConfigsResponse], Awaitable[None]],
    ):
        pass

    def alter_configs_request_error_response(
            self,
            error_code: ErrorCode,
            error_message: str,
            req: AlterConfigsRequest,
            api_version: int,
    ) -> AlterConfigsResponse:
        pass

    async def handle_alter_partition_request(
            self,
            header: AlterPartitionRequestHeader,
            req: AlterPartitionRequest,
            api_version: int,
            callback: Callable[[AlterPartitionResponse], Awaitable[None]],
    ):
        pass

    def alter_partition_request_error_response(
            self,
            error_code: ErrorCode,
            error_message: str,
            req: AlterPartitionRequest,
            api_version: int,
    ) -> AlterPartitionResponse:
        pass

    async def handle_alter_partition_reassignments_request(
            self,
            header: AlterPartitionReassignmentsRequestHeader,
            req: AlterPartitionReassignmentsRequest,
            api_version: int,
            callback: Callable[[AlterPartitionReassignmentsResponse], Awaitable[None]],
    ):
        pass

    def alter_partition_reassignments_request_error_response(
            self,
            error_code: ErrorCode,
            error_message: str,
            req: AlterPartitionReassignmentsRequest,
            api_version: int,
    ) -> AlterPartitionReassignmentsResponse:
        pass

    async def handle_alter_replica_log_dirs_request(
            self,
            header: AlterReplicaLogDirsRequestHeader,
            req: AlterReplicaLogDirsRequest,
            api_version: int,
            callback: Callable[[AlterReplicaLogDirsResponse], Awaitable[None]],
    ):
        pass

    def alter_replica_log_dirs_request_error_response(
            self,
            error_code: ErrorCode,
            error_message: str,
            req: AlterReplicaLogDirsRequest,
            api_version: int,
    ) -> AlterReplicaLogDirsResponse:
        pass

    async def handle_alter_user_scram_credentials_request(
            self,
            header: AlterUserScramCredentialsRequestHeader,
            req: AlterUserScramCredentialsRequest,
            api_version: int,
            callback: Callable[[AlterUserScramCredentialsResponse], Awaitable[None]],
    ):
        pass

    def alter_user_scram_credentials_request_error_response(
            self,
            error_code: ErrorCode,
            error_message: str,
            req: AlterUserScramCredentialsRequest,
            api_version: int,
    ) -> AlterUserScramCredentialsResponse:
        pass

    async def handle_assign_replicas_to_dirs_request(
            self,
            header: AssignReplicasToDirsRequestHeader,
            req: AssignReplicasToDirsRequest,
            api_version: int,
            callback: Callable[[AssignReplicasToDirsResponse], Awaitable[None]],
    ):
        pass

    def assign_replicas_to_dirs_request_error_response(
            self,
            error_code: ErrorCode,
            error_message: str,
            req: AssignReplicasToDirsRequest,
            api_version: int,
    ) -> AssignReplicasToDirsResponse:
        pass

    async def handle_begin_quorum_epoch_request(
            self,
            header: BeginQuorumEpochRequestHeader,
            req: BeginQuorumEpochRequest,
            api_version: int,
            callback: Callable[[BeginQuorumEpochResponse], Awaitable[None]],
    ):
        pass

    def begin_quorum_epoch_request_error_response(
            self,
            error_code: ErrorCode,
            error_message: str,
            req: BeginQuorumEpochRequest,
            api_version: int,
    ) -> BeginQuorumEpochResponse:
        pass

    async def handle_broker_heartbeat_request(
            self,
            header: BrokerHeartbeatRequestHeader,
            req: BrokerHeartbeatRequest,
            api_version: int,
            callback: Callable[[BrokerHeartbeatResponse], Awaitable[None]],
    ):
        pass

    def broker_heartbeat_request_error_response(
            self,
            error_code: ErrorCode,
            error_message: str,
            req: BrokerHeartbeatRequest,
            api_version: int,
    ) -> BrokerHeartbeatResponse:
        pass

    async def handle_broker_registration_request(
            self,
            header: BrokerRegistrationRequestHeader,
            req: BrokerRegistrationRequest,
            api_version: int,
            callback: Callable[[BrokerRegistrationResponse], Awaitable[None]],
    ):
        pass

    def broker_registration_request_error_response(
            self,
            error_code: ErrorCode,
            error_message: str,
            req: BrokerRegistrationRequest,
            api_version: int,
    ) -> BrokerRegistrationResponse:
        pass

    async def handle_consumer_group_describe_request(
            self,
            header: ConsumerGroupDescribeRequestHeader,
            req: ConsumerGroupDescribeRequest,
            api_version: int,
            callback: Callable[[ConsumerGroupDescribeResponse], Awaitable[None]],
    ):
        pass

    def consumer_group_describe_request_error_response(
            self,
            error_code: ErrorCode,
            error_message: str,
            req: ConsumerGroupDescribeRequest,
            api_version: int,
    ) -> ConsumerGroupDescribeResponse:
        pass

    async def handle_consumer_group_heartbeat_request(
            self,
            header: ConsumerGroupHeartbeatRequestHeader,
            req: ConsumerGroupHeartbeatRequest,
            api_version: int,
            callback: Callable[[ConsumerGroupHeartbeatResponse], Awaitable[None]],
    ):
        pass

    def consumer_group_heartbeat_request_error_response(
            self,
            error_code: ErrorCode,
            error_message: str,
            req: ConsumerGroupHeartbeatRequest,
            api_version: int,
    ) -> ConsumerGroupHeartbeatResponse:
        pass

    async def handle_controlled_shutdown_request(
            self,
            header: ControlledShutdownRequestHeader,
            req: ControlledShutdownRequest,
            api_version: int,
            callback: Callable[[ControlledShutdownResponse], Awaitable[None]],
    ):
        pass

    def controlled_shutdown_request_error_response(
            self,
            error_code: ErrorCode,
            error_message: str,
            req: ControlledShutdownRequest,
            api_version: int,
    ) -> ControlledShutdownResponse:
        pass

    async def handle_controller_registration_request(
            self,
            header: ControllerRegistrationRequestHeader,
            req: ControllerRegistrationRequest,
            api_version: int,
            callback: Callable[[ControllerRegistrationResponse], Awaitable[None]],
    ):
        pass

    def controller_registration_request_error_response(
            self,
            error_code: ErrorCode,
            error_message: str,
            req: ControllerRegistrationRequest,
            api_version: int,
    ) -> ControllerRegistrationResponse:
        pass

    async def handle_create_acls_request(
        self,
        header: CreateAclsRequestHeader,
        req: CreateAclsRequest,
        api_version: int,
        callback: Callable[[CreateAclsResponse], Awaitable[None]],
    ):
        pass

    def create_acls_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: CreateAclsRequest,
        api_version: int,
    ) -> CreateAclsResponse:
        pass

    async def handle_create_delegation_token_request(
        self,
        header: CreateDelegationTokenRequestHeader,
        req: CreateDelegationTokenRequest,
        api_version: int,
        callback: Callable[[CreateDelegationTokenResponse], Awaitable[None]],
    ):
        pass

    def create_delegation_token_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: CreateDelegationTokenRequest,
        api_version: int,
    ) -> CreateDelegationTokenResponse:
        pass

    async def handle_create_partitions_request(
        self,
        header: CreatePartitionsRequestHeader,
        req: CreatePartitionsRequest,
        api_version: int,
        callback: Callable[[CreatePartitionsResponse], Awaitable[None]],
    ):
        pass

    def create_partitions_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: CreatePartitionsRequest,
        api_version: int,
    ) -> CreatePartitionsResponse:
        pass
