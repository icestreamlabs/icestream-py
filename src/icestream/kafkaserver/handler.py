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
from icestream.kafkaserver.handlers.create_topics import CreateTopicsRequestHeader, CreateTopicsRequest, \
    CreateTopicsResponse
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
from icestream.kafkaserver.handlers.delete_topics import (
    DeleteTopicsRequest, DeleteTopicsRequestHeader, DeleteTopicsResponse
)
from icestream.kafkaserver.handlers.delete_acls import (
    DeleteAclsRequest, DeleteAclsRequestHeader, DeleteAclsResponse
)
from icestream.kafkaserver.handlers.delete_groups import (
    DeleteGroupsRequest, DeleteGroupsRequestHeader, DeleteGroupsResponse
)
from icestream.kafkaserver.handlers.delete_records import (
    DeleteRecordsRequest, DeleteRecordsRequestHeader, DeleteRecordsResponse
)
from icestream.kafkaserver.handlers.delete_share_group_state import (
    DeleteShareGroupStateRequest, DeleteShareGroupStateRequestHeader, DeleteShareGroupStateResponse
)
from icestream.kafkaserver.handlers.describe_acls import (
    DescribeAclsRequest, DescribeAclsRequestHeader, DescribeAclsResponse
)
from icestream.kafkaserver.handlers.describe_client_quotas import (
    DescribeClientQuotasRequest, DescribeClientQuotasRequestHeader, DescribeClientQuotasResponse
)
from icestream.kafkaserver.handlers.describe_cluster import (
    DescribeClusterRequest, DescribeClusterRequestHeader, DescribeClusterResponse
)
from icestream.kafkaserver.handlers.describe_configs import (
    DescribeConfigsRequest, DescribeConfigsRequestHeader, DescribeConfigsResponse
)
from icestream.kafkaserver.handlers.describe_delegation_token import (
    DescribeDelegationTokenRequest, DescribeDelegationTokenRequestHeader, DescribeDelegationTokenResponse
)
from icestream.kafkaserver.handlers.describe_groups import (
    DescribeGroupsRequest, DescribeGroupsRequestHeader, DescribeGroupsResponse
)
from icestream.kafkaserver.handlers.describe_log_dirs import (
    DescribeLogDirsRequest, DescribeLogDirsRequestHeader, DescribeLogDirsResponse
)
from icestream.kafkaserver.handlers.describe_producers import (
    DescribeProducersRequest, DescribeProducersRequestHeader, DescribeProducersResponse
)
from icestream.kafkaserver.handlers.describe_quorum import (
    DescribeQuorumRequest, DescribeQuorumRequestHeader, DescribeQuorumResponse
)
from icestream.kafkaserver.handlers.describe_topic_partitions import (
    DescribeTopicPartitionsRequest, DescribeTopicPartitionsRequestHeader, DescribeTopicPartitionsResponse
)
from icestream.kafkaserver.handlers.describe_transactions import (
    DescribeTransactionsRequest, DescribeTransactionsRequestHeader, DescribeTransactionsResponse
)
from icestream.kafkaserver.handlers.describe_user_scram_credentials import (
    DescribeUserScramCredentialsRequest, DescribeUserScramCredentialsRequestHeader,
    DescribeUserScramCredentialsResponse
)
from icestream.kafkaserver.handlers.elect_leaders import (
    ElectLeadersRequest, ElectLeadersRequestHeader, ElectLeadersResponse
)
from icestream.kafkaserver.handlers.end_quorum_epoch import (
    EndQuorumEpochRequest, EndQuorumEpochRequestHeader, EndQuorumEpochResponse
)
from icestream.kafkaserver.handlers.end_txn import (
    EndTxnRequest, EndTxnRequestHeader, EndTxnResponse
)
from icestream.kafkaserver.handlers.envelope import (
    EnvelopeRequest, EnvelopeRequestHeader, EnvelopeResponse
)
from icestream.kafkaserver.handlers.add_offsets_to_txn import (
    AddOffsetsToTxnRequest, AddOffsetsToTxnRequestHeader, AddOffsetsToTxnResponse
)
from icestream.kafkaserver.handlers.add_partitions_to_txn import (
    AddPartitionsToTxnRequest, AddPartitionsToTxnRequestHeader, AddPartitionsToTxnResponse
)
from icestream.kafkaserver.handlers.alter_client_quotas import (
    AlterClientQuotasRequest, AlterClientQuotasRequestHeader, AlterClientQuotasResponse
)
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
from icestream.kafkaserver.handlers.add_raft_voter import (
    AddRaftVoterRequest, AddRaftVoterRequestHeader, AddRaftVoterResponse
)
from icestream.kafkaserver.handlers.allocate_producer_ids import (
    AllocateProducerIdsRequest, AllocateProducerIdsRequestHeader, AllocateProducerIdsResponse
)
from icestream.kafkaserver.handlers.create_acls import (
    CreateAclsRequest, CreateAclsRequestHeader, CreateAclsResponse
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

log = structlog.get_logger()

PRODUCE_API_KEY = 0
FETCH_API_KEY = 1
METADATA_API_KEY = 3
CONTROLLED_SHUTDOWN_API_KEY = 7
API_VERSIONS_API_KEY = 18
CREATE_TOPICS_API_KEY = 19
DELETE_TOPICS_API_KEY = 20
DELETE_ACLS_API_KEY = 31
DELETE_GROUPS_API_KEY = 42
DELETE_RECORDS_API_KEY = 21
DELETE_SHARE_GROUP_STATE_API_KEY = 86
DESCRIBE_ACLS_API_KEY = 29
DESCRIBE_CLIENT_QUOTAS_API_KEY = 48
DESCRIBE_CLUSTER_API_KEY = 60
DESCRIBE_CONFIGS_API_KEY = 32
DESCRIBE_DELEGATION_TOKEN_API_KEY = 41
DESCRIBE_GROUPS_API_KEY = 15
DESCRIBE_LOG_DIRS_API_KEY = 35
DESCRIBE_PRODUCERS_API_KEY = 61
DESCRIBE_QUORUM_API_KEY = 55
DESCRIBE_TOPIC_PARTITIONS_API_KEY = 75
DESCRIBE_TRANSACTIONS_API_KEY = 65
DESCRIBE_USER_SCRAM_CREDENTIALS_API_KEY = 50
ELECT_LEADERS_API_KEY = 43
END_QUORUM_EPOCH_API_KEY = 54
END_TXN_API_KEY = 26
ENVELOPE_API_KEY = 58
ADD_PARTITIONS_TO_TXN_API_KEY = 24
ADD_OFFSETS_TO_TXN_API_KEY = 25
CREATE_ACLS_API_KEY = 30
CREATE_DELEGATION_TOKEN_API_KEY = 38
CREATE_PARTITIONS_API_KEY = 37
ALTER_CONFIGS_API_KEY = 33
ALTER_REPLICA_LOG_DIRS_API_KEY = 34
ALTER_USER_SCRAM_CREDENTIALS_API_KEY = 51
BEGIN_QUORUM_EPOCH_API_KEY = 53
BROKER_HEARTBEAT_API_KEY = 63
BROKER_REGISTRATION_API_KEY = 62
CONSUMER_GROUP_DESCRIBE_API_KEY = 69
CONSUMER_GROUP_HEARTBEAT_API_KEY = 68
ALTER_CLIENT_QUOTAS_API_KEY = 49
ALTER_PARTITION_REASSIGNMENTS_API_KEY = 45
ALTER_PARTITION_API_KEY = 56
ALLOCATE_PRODUCER_IDS_API_KEY = 67
CONTROLLER_REGISTRATION_API_KEY = 70
ASSIGN_REPLICAS_TO_DIRS_API_KEY = 73
ADD_RAFT_VOTER_API_KEY = 80


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
    FETCH_API_KEY: (0, 11),
    METADATA_API_KEY: (0, 4),
    CONTROLLED_SHUTDOWN_API_KEY: (0, 3),
    API_VERSIONS_API_KEY: (0, 4),
    CREATE_TOPICS_API_KEY: (0, 4),
    DELETE_TOPICS_API_KEY: (0, 6),
    DELETE_ACLS_API_KEY: (0, 3),
    DELETE_GROUPS_API_KEY: (0, 2),
    DELETE_RECORDS_API_KEY: (0, 2),
    DELETE_SHARE_GROUP_STATE_API_KEY: (0, 0),
    DESCRIBE_ACLS_API_KEY: (0, 3),
    DESCRIBE_CLIENT_QUOTAS_API_KEY: (0, 1),
    DESCRIBE_CLUSTER_API_KEY: (0, 1),
    DESCRIBE_CONFIGS_API_KEY: (0, 4),
    DESCRIBE_DELEGATION_TOKEN_API_KEY: (0, 3),
    DESCRIBE_GROUPS_API_KEY: (0, 5),
    DESCRIBE_LOG_DIRS_API_KEY: (0, 4),
    DESCRIBE_PRODUCERS_API_KEY: (0, 0),
    DESCRIBE_QUORUM_API_KEY: (0, 2),
    DESCRIBE_TOPIC_PARTITIONS_API_KEY: (0, 0),
    DESCRIBE_TRANSACTIONS_API_KEY: (0, 0),
    DESCRIBE_USER_SCRAM_CREDENTIALS_API_KEY: (0, 0),
    ELECT_LEADERS_API_KEY: (0, 2),
    END_QUORUM_EPOCH_API_KEY: (0, 1),
    END_TXN_API_KEY: (0, 4),
    ENVELOPE_API_KEY: (0, 0),
    ADD_OFFSETS_TO_TXN_API_KEY: (0, 4),
    ADD_PARTITIONS_TO_TXN_API_KEY: (0, 5),
    ALTER_CLIENT_QUOTAS_API_KEY: (0, 1),
    ALTER_CONFIGS_API_KEY: (0, 2),
    ALTER_PARTITION_API_KEY: (0, 3),
    ALTER_PARTITION_REASSIGNMENTS_API_KEY: (0, 0),
    ALTER_REPLICA_LOG_DIRS_API_KEY: (0, 2),
    ALTER_USER_SCRAM_CREDENTIALS_API_KEY: (0, 0),
    ASSIGN_REPLICAS_TO_DIRS_API_KEY: (0, 0),
    BEGIN_QUORUM_EPOCH_API_KEY: (0, 1),
    BROKER_HEARTBEAT_API_KEY: (0, 1),
    BROKER_REGISTRATION_API_KEY: (0, 4),
    CONSUMER_GROUP_DESCRIBE_API_KEY: (0, 0),
    CONSUMER_GROUP_HEARTBEAT_API_KEY: (0, 0),
    CONTROLLER_REGISTRATION_API_KEY: (0, 0),
    ADD_RAFT_VOTER_API_KEY: (0, 0),
    ALLOCATE_PRODUCER_IDS_API_KEY: (0, 0),
    CREATE_ACLS_API_KEY: (0, 3),
    CREATE_DELEGATION_TOKEN_API_KEY: (0, 3),
    CREATE_PARTITIONS_API_KEY: (0, 3),
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


async def handle_fetch(
        handler: KafkaHandler,
        header: FetchRequestHeader,
        req: FetchRequest,
        api_version: int,
        respond: Callable[[FetchResponse], Awaitable[None]],
) -> None:
    await handler.handle_fetch_request(header, req, api_version, respond)


async def handle_delete_topics(
        handler: KafkaHandler,
        header: DeleteTopicsRequestHeader,
        req: DeleteTopicsRequest,
        api_version: int,
        respond: Callable[[DeleteTopicsResponse], Awaitable[None]],
) -> None:
    await handler.handle_delete_topics_request(header, req, api_version, respond)


async def handle_delete_acls(
        handler: KafkaHandler,
        header: DeleteAclsRequestHeader,
        req: DeleteAclsRequest,
        api_version: int,
        respond: Callable[[DeleteAclsResponse], Awaitable[None]],
) -> None:
    await handler.handle_delete_acls_request(header, req, api_version, respond)


async def handle_delete_groups(
        handler: KafkaHandler,
        header: DeleteGroupsRequestHeader,
        req: DeleteGroupsRequest,
        api_version: int,
        respond: Callable[[DeleteGroupsResponse], Awaitable[None]],
) -> None:
    await handler.handle_delete_groups_request(header, req, api_version, respond)


async def handle_delete_records(
        handler: KafkaHandler,
        header: DeleteRecordsRequestHeader,
        req: DeleteRecordsRequest,
        api_version: int,
        respond: Callable[[DeleteRecordsResponse], Awaitable[None]],
) -> None:
    await handler.handle_delete_records_request(header, req, api_version, respond)


async def handle_delete_share_group_state(
        handler: KafkaHandler,
        header: DeleteShareGroupStateRequestHeader,
        req: DeleteShareGroupStateRequest,
        api_version: int,
        respond: Callable[[DeleteShareGroupStateResponse], Awaitable[None]],
) -> None:
    await handler.handle_delete_share_group_state_request(header, req, api_version, respond)


async def handle_describe_acls(
        handler: KafkaHandler,
        header: DescribeAclsRequestHeader,
        req: DescribeAclsRequest,
        api_version: int,
        respond: Callable[[DescribeAclsResponse], Awaitable[None]],
) -> None:
    await handler.handle_describe_acls_request(header, req, api_version, respond)


async def handle_describe_client_quotas(
        handler: KafkaHandler,
        header: DescribeClientQuotasRequestHeader,
        req: DescribeClientQuotasRequest,
        api_version: int,
        respond: Callable[[DescribeClientQuotasResponse], Awaitable[None]],
) -> None:
    await handler.handle_describe_client_quotas_request(header, req, api_version, respond)


async def handle_describe_cluster(
        handler: KafkaHandler,
        header: DescribeClusterRequestHeader,
        req: DescribeClusterRequest,
        api_version: int,
        respond: Callable[[DescribeClusterResponse], Awaitable[None]],
) -> None:
    await handler.handle_describe_cluster_request(header, req, api_version, respond)


async def handle_describe_configs(
        handler: KafkaHandler,
        header: DescribeConfigsRequestHeader,
        req: DescribeConfigsRequest,
        api_version: int,
        respond: Callable[[DescribeConfigsResponse], Awaitable[None]],
) -> None:
    await handler.handle_describe_configs_request(header, req, api_version, respond)


async def handle_describe_delegation_token(
        handler: KafkaHandler,
        header: DescribeDelegationTokenRequestHeader,
        req: DescribeDelegationTokenRequest,
        api_version: int,
        respond: Callable[[DescribeDelegationTokenResponse], Awaitable[None]],
) -> None:
    await handler.handle_describe_delegation_token_request(header, req, api_version, respond)


async def handle_describe_groups(
        handler: KafkaHandler,
        header: DescribeGroupsRequestHeader,
        req: DescribeGroupsRequest,
        api_version: int,
        respond: Callable[[DescribeGroupsResponse], Awaitable[None]],
) -> None:
    await handler.handle_describe_groups_request(header, req, api_version, respond)


async def handle_describe_log_dirs(
        handler: KafkaHandler,
        header: DescribeLogDirsRequestHeader,
        req: DescribeLogDirsRequest,
        api_version: int,
        respond: Callable[[DescribeLogDirsResponse], Awaitable[None]],
) -> None:
    await handler.handle_describe_log_dirs_request(header, req, api_version, respond)


async def handle_describe_producers(
        handler: KafkaHandler,
        header: DescribeProducersRequestHeader,
        req: DescribeProducersRequest,
        api_version: int,
        respond: Callable[[DescribeProducersResponse], Awaitable[None]],
) -> None:
    await handler.handle_describe_producers_request(header, req, api_version, respond)


async def handle_describe_quorum(
        handler: KafkaHandler,
        header: DescribeQuorumRequestHeader,
        req: DescribeQuorumRequest,
        api_version: int,
        respond: Callable[[DescribeQuorumResponse], Awaitable[None]],
) -> None:
    await handler.handle_describe_quorum_request(header, req, api_version, respond)


async def handle_describe_topic_partitions(
        handler: KafkaHandler,
        header: DescribeTopicPartitionsRequestHeader,
        req: DescribeTopicPartitionsRequest,
        api_version: int,
        respond: Callable[[DescribeTopicPartitionsResponse], Awaitable[None]],
) -> None:
    await handler.handle_describe_topic_partitions_request(header, req, api_version, respond)


async def handle_describe_transactions(
        handler: KafkaHandler,
        header: DescribeTransactionsRequestHeader,
        req: DescribeTransactionsRequest,
        api_version: int,
        respond: Callable[[DescribeTransactionsResponse], Awaitable[None]],
) -> None:
    await handler.handle_describe_transactions_request(header, req, api_version, respond)


async def handle_describe_user_scram_credentials(
        handler: KafkaHandler,
        header: DescribeUserScramCredentialsRequestHeader,
        req: DescribeUserScramCredentialsRequest,
        api_version: int,
        respond: Callable[[DescribeUserScramCredentialsResponse], Awaitable[None]],
) -> None:
    await handler.handle_describe_user_scram_credentials_request(header, req, api_version, respond)


async def handle_elect_leaders(
        handler: KafkaHandler,
        header: ElectLeadersRequestHeader,
        req: ElectLeadersRequest,
        api_version: int,
        respond: Callable[[ElectLeadersResponse], Awaitable[None]],
) -> None:
    await handler.handle_elect_leaders_request(header, req, api_version, respond)


async def handle_end_quorum_epoch(
        handler: KafkaHandler,
        header: EndQuorumEpochRequestHeader,
        req: EndQuorumEpochRequest,
        api_version: int,
        respond: Callable[[EndQuorumEpochResponse], Awaitable[None]],
) -> None:
    await handler.handle_end_quorum_epoch_request(header, req, api_version, respond)


async def handle_end_txn(
        handler: KafkaHandler,
        header: EndTxnRequestHeader,
        req: EndTxnRequest,
        api_version: int,
        respond: Callable[[EndTxnResponse], Awaitable[None]],
) -> None:
    await handler.handle_end_txn_request(header, req, api_version, respond)


async def handle_envelope(
        handler: KafkaHandler,
        header: EnvelopeRequestHeader,
        req: EnvelopeRequest,
        api_version: int,
        respond: Callable[[EnvelopeResponse], Awaitable[None]],
) -> None:
    await handler.handle_envelope_request(header, req, api_version, respond)


async def handle_controlled_shutdown(
        handler: KafkaHandler,
        header: ControlledShutdownRequestHeader,
        req: ControlledShutdownRequest,
        api_version: int,
        respond: Callable[[ControlledShutdownResponse], Awaitable[None]],
) -> None:
    await handler.handle_controlled_shutdown_request(header, req, api_version, respond)


def error_produce(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: ProduceRequest,
        api_version: int,
) -> ProduceResponse:
    return handler.produce_request_error_response(code, msg, req, api_version)


async def handle_add_offsets_to_txn(
        handler: KafkaHandler,
        header: AddOffsetsToTxnRequestHeader,
        req: AddOffsetsToTxnRequest,
        api_version: int,
        respond: Callable[[AddOffsetsToTxnResponse], Awaitable[None]],
) -> None:
    await handler.handle_add_offsets_to_txn_request(header, req, api_version, respond)


async def handle_add_partitions_to_txn(
        handler: KafkaHandler,
        header: AddPartitionsToTxnRequestHeader,
        req: AddPartitionsToTxnRequest,
        api_version: int,
        respond: Callable[[AddPartitionsToTxnResponse], Awaitable[None]],
) -> None:
    await handler.handle_add_partitions_to_txn_request(
        header, req, api_version, respond
    )


async def handle_alter_client_quotas(
        handler: KafkaHandler,
        header: AlterClientQuotasRequestHeader,
        req: AlterClientQuotasRequest,
        api_version: int,
        respond: Callable[[AlterClientQuotasResponse], Awaitable[None]],
) -> None:
    await handler.handle_alter_client_quotas_request(header, req, api_version, respond)


async def handle_alter_configs(
        handler: KafkaHandler,
        header: AlterConfigsRequestHeader,
        req: AlterConfigsRequest,
        api_version: int,
        respond: Callable[[AlterConfigsResponse], Awaitable[None]],
) -> None:
    await handler.handle_alter_configs_request(header, req, api_version, respond)


async def handle_alter_partition(
        handler: KafkaHandler,
        header: AlterPartitionRequestHeader,
        req: AlterPartitionRequest,
        api_version: int,
        respond: Callable[[AlterPartitionResponse], Awaitable[None]],
) -> None:
    await handler.handle_alter_partition_request(header, req, api_version, respond)


async def handle_alter_partition_reassignments(
        handler: KafkaHandler,
        header: AlterPartitionReassignmentsRequestHeader,
        req: AlterPartitionReassignmentsRequest,
        api_version: int,
        respond: Callable[[AlterPartitionReassignmentsResponse], Awaitable[None]],
) -> None:
    await handler.handle_alter_partition_reassignments_request(header, req, api_version, respond)


async def handle_alter_replica_log_dirs(
        handler: KafkaHandler,
        header: AlterReplicaLogDirsRequestHeader,
        req: AlterReplicaLogDirsRequest,
        api_version: int,
        respond: Callable[[AlterReplicaLogDirsResponse], Awaitable[None]],
) -> None:
    await handler.handle_alter_replica_log_dirs_request(header, req, api_version, respond)


async def handle_alter_user_scram_credentials(
        handler: KafkaHandler,
        header: AlterUserScramCredentialsRequestHeader,
        req: AlterUserScramCredentialsRequest,
        api_version: int,
        respond: Callable[[AlterUserScramCredentialsResponse], Awaitable[None]],
) -> None:
    await handler.handle_alter_user_scram_credentials_request(header, req, api_version, respond)


async def handle_assign_replicas_to_dirs(
        handler: KafkaHandler,
        header: AssignReplicasToDirsRequestHeader,
        req: AssignReplicasToDirsRequest,
        api_version: int,
        respond: Callable[[AssignReplicasToDirsResponse], Awaitable[None]],
) -> None:
    await handler.handle_assign_replicas_to_dirs_request(header, req, api_version, respond)


async def handle_begin_quorum_epoch(
        handler: KafkaHandler,
        header: BeginQuorumEpochRequestHeader,
        req: BeginQuorumEpochRequest,
        api_version: int,
        respond: Callable[[BeginQuorumEpochResponse], Awaitable[None]],
) -> None:
    await handler.handle_begin_quorum_epoch_request(header, req, api_version, respond)


async def handle_broker_heartbeat(
        handler: KafkaHandler,
        header: BrokerHeartbeatRequestHeader,
        req: BrokerHeartbeatRequest,
        api_version: int,
        respond: Callable[[BrokerHeartbeatResponse], Awaitable[None]],
) -> None:
    await handler.handle_broker_heartbeat_request(header, req, api_version, respond)


async def handle_broker_registration(
        handler: KafkaHandler,
        header: BrokerRegistrationRequestHeader,
        req: BrokerRegistrationRequest,
        api_version: int,
        respond: Callable[[BrokerRegistrationResponse], Awaitable[None]],
) -> None:
    await handler.handle_broker_registration_request(header, req, api_version, respond)


async def handle_consumer_group_describe(
        handler: KafkaHandler,
        header: ConsumerGroupDescribeRequestHeader,
        req: ConsumerGroupDescribeRequest,
        api_version: int,
        respond: Callable[[ConsumerGroupDescribeResponse], Awaitable[None]],
) -> None:
    await handler.handle_consumer_group_describe_request(header, req, api_version, respond)


async def handle_consumer_group_heartbeat(
        handler: KafkaHandler,
        header: ConsumerGroupHeartbeatRequestHeader,
        req: ConsumerGroupHeartbeatRequest,
        api_version: int,
        respond: Callable[[ConsumerGroupHeartbeatResponse], Awaitable[None]],
) -> None:
    await handler.handle_consumer_group_heartbeat_request(header, req, api_version, respond)


async def handle_controller_registration(
        handler: KafkaHandler,
        header: ControllerRegistrationRequestHeader,
        req: ControllerRegistrationRequest,
        api_version: int,
        respond: Callable[[ControllerRegistrationResponse], Awaitable[None]],
) -> None:
    await handler.handle_controller_registration_request(header, req, api_version, respond)


async def handle_add_raft_voter(
        handler: KafkaHandler,
        header: AddRaftVoterRequestHeader,
        req: AddRaftVoterRequest,
        api_version: int,
        respond: Callable[[AddRaftVoterResponse], Awaitable[None]],
) -> None:
    await handler.handle_add_raft_voter_request(header, req, api_version, respond)

async def handle_allocate_producer_ids(
        handler: KafkaHandler,
        header: AllocateProducerIdsRequestHeader,
        req: AllocateProducerIdsRequest,
        api_version: int,
        respond: Callable[[AllocateProducerIdsResponse], Awaitable[None]],
) -> None:
    await handler.handle_allocate_producer_ids_request(header, req, api_version, respond)


async def handle_create_acls(
        handler: KafkaHandler,
        header: CreateAclsRequestHeader,
        req: CreateAclsRequest,
        api_version: int,
        respond: Callable[[CreateAclsResponse], Awaitable[None]],
) -> None:
    await handler.handle_create_acls_request(header, req, api_version, respond)


async def handle_create_delegation_token(
        handler: KafkaHandler,
        header: CreateDelegationTokenRequestHeader,
        req: CreateDelegationTokenRequest,
        api_version: int,
        respond: Callable[[CreateDelegationTokenResponse], Awaitable[None]],
) -> None:
    await handler.handle_create_delegation_token_request(header, req, api_version, respond)


async def handle_create_partitions(
        handler: KafkaHandler,
        header: CreatePartitionsRequestHeader,
        req: CreatePartitionsRequest,
        api_version: int,
        respond: Callable[[CreatePartitionsResponse], Awaitable[None]],
) -> None:
    await handler.handle_create_partitions_request(header, req, api_version, respond)


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


def error_fetch(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: FetchRequest,
        api_version: int,
) -> FetchResponse:
    return handler.fetch_request_error_response(code, msg, req, api_version)


def error_delete_topics(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: DeleteTopicsRequest,
        api_version: int,
) -> DeleteTopicsResponse:
    return handler.delete_topics_request_error_response(code, msg, req, api_version)


def error_delete_acls(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: DeleteAclsRequest,
        api_version: int,
) -> DeleteAclsResponse:
    return handler.delete_acls_request_error_response(code, msg, req, api_version)


def error_delete_groups(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: DeleteGroupsRequest,
        api_version: int,
) -> DeleteGroupsResponse:
    return handler.delete_groups_request_error_response(code, msg, req, api_version)


def error_delete_records(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: DeleteRecordsRequest,
        api_version: int,
) -> DeleteRecordsResponse:
    return handler.delete_records_request_error_response(code, msg, req, api_version)


def error_delete_share_group_state(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: DeleteShareGroupStateRequest,
        api_version: int,
) -> DeleteShareGroupStateResponse:
    return handler.delete_share_group_state_request_error_response(code, msg, req, api_version)


def error_describe_acls(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: DescribeAclsRequest,
        api_version: int,
) -> DescribeAclsResponse:
    return handler.describe_acls_request_error_response(code, msg, req, api_version)


def error_describe_client_quotas(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: DescribeClientQuotasRequest,
        api_version: int,
) -> DescribeClientQuotasResponse:
    return handler.describe_client_quotas_request_error_response(code, msg, req, api_version)


def error_describe_cluster(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: DescribeClusterRequest,
        api_version: int,
) -> DescribeClusterResponse:
    return handler.describe_cluster_request_error_response(code, msg, req, api_version)


def error_describe_configs(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: DescribeConfigsRequest,
        api_version: int,
) -> DescribeConfigsResponse:
    return handler.describe_configs_request_error_response(code, msg, req, api_version)


def error_describe_delegation_token(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: DescribeDelegationTokenRequest,
        api_version: int,
) -> DescribeDelegationTokenResponse:
    return handler.describe_delegation_token_request_error_response(code, msg, req, api_version)


def error_describe_groups(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: DescribeGroupsRequest,
        api_version: int,
) -> DescribeGroupsResponse:
    return handler.describe_groups_request_error_response(code, msg, req, api_version)


def error_describe_log_dirs(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: DescribeLogDirsRequest,
        api_version: int,
) -> DescribeLogDirsResponse:
    return handler.describe_log_dirs_request_error_response(code, msg, req, api_version)


def error_describe_producers(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: DescribeProducersRequest,
        api_version: int,
) -> DescribeProducersResponse:
    return handler.describe_producers_request_error_response(code, msg, req, api_version)


def error_describe_quorum(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: DescribeQuorumRequest,
        api_version: int,
) -> DescribeQuorumResponse:
    return handler.describe_quorum_request_error_response(code, msg, req, api_version)


def error_describe_topic_partitions(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: DescribeTopicPartitionsRequest,
        api_version: int,
) -> DescribeTopicPartitionsResponse:
    return handler.describe_topic_partitions_request_error_response(code, msg, req, api_version)


def error_describe_transactions(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: DescribeTransactionsRequest,
        api_version: int,
) -> DescribeTransactionsResponse:
    return handler.describe_transactions_request_error_response(code, msg, req, api_version)


def error_describe_user_scram_credentials(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: DescribeUserScramCredentialsRequest,
        api_version: int,
) -> DescribeUserScramCredentialsResponse:
    return handler.describe_user_scram_credentials_request_error_response(
        code, msg, req, api_version
    )


def error_elect_leaders(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: ElectLeadersRequest,
        api_version: int,
) -> ElectLeadersResponse:
    return handler.elect_leaders_request_error_response(code, msg, req, api_version)


def error_end_quorum_epoch(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: EndQuorumEpochRequest,
        api_version: int,
) -> EndQuorumEpochResponse:
    return handler.end_quorum_epoch_request_error_response(code, msg, req, api_version)


def error_end_txn(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: EndTxnRequest,
        api_version: int,
) -> EndTxnResponse:
    return handler.end_txn_request_error_response(code, msg, req, api_version)


def error_envelope(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: EnvelopeRequest,
        api_version: int,
) -> EnvelopeResponse:
    return handler.envelope_request_error_response(code, msg, req, api_version)


def error_controlled_shutdown(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: ControlledShutdownRequest,
        api_version: int,
) -> ControlledShutdownResponse:
    return handler.controlled_shutdown_request_error_response(
        code, msg, req, api_version
    )


def error_add_offsets_to_txn(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: AddOffsetsToTxnRequest,
        api_version: int,
) -> AddOffsetsToTxnResponse:
    return handler.add_offsets_to_txn_request_error_response(
        code, msg, req, api_version
    )


def error_add_partitions_to_txn(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: AddPartitionsToTxnRequest,
        api_version: int,
) -> AddPartitionsToTxnResponse:
    return handler.add_partitions_to_txn_request_error_response(
        code, msg, req, api_version
    )


def error_alter_client_quotas(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: AlterClientQuotasRequest,
        api_version: int,
) -> AlterClientQuotasResponse:
    return handler.alter_client_quotas_request_error_response(
        code, msg, req, api_version
    )


def error_alter_configs(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: AlterConfigsRequest,
        api_version: int,
) -> AlterConfigsResponse:
    return handler.alter_configs_request_error_response(
        code, msg, req, api_version
    )


def error_alter_partition(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: AlterPartitionRequest,
        api_version: int,
) -> AlterPartitionResponse:
    return handler.alter_partition_request_error_response(
        code, msg, req, api_version
    )


def error_alter_partition_reassignments(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: AlterPartitionReassignmentsRequest,
        api_version: int,
) -> AlterPartitionReassignmentsResponse:
    return handler.alter_partition_reassignments_request_error_response(
        code, msg, req, api_version
    )


def error_alter_replica_log_dirs(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: AlterReplicaLogDirsRequest,
        api_version: int,
) -> AlterReplicaLogDirsResponse:
    return handler.alter_replica_log_dirs_request_error_response(
        code, msg, req, api_version
    )


def error_alter_user_scram_credentials(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: AlterUserScramCredentialsRequest,
        api_version: int,
) -> AlterUserScramCredentialsResponse:
    return handler.alter_user_scram_credentials_request_error_response(
        code, msg, req, api_version
    )


def error_assign_replicas_to_dirs(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: AssignReplicasToDirsRequest,
        api_version: int,
) -> AssignReplicasToDirsResponse:
    return handler.assign_replicas_to_dirs_request_error_response(
        code, msg, req, api_version
    )


def error_begin_quorum_epoch(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: BeginQuorumEpochRequest,
        api_version: int,
) -> BeginQuorumEpochResponse:
    return handler.begin_quorum_epoch_request_error_response(
        code, msg, req, api_version
    )


def error_broker_heartbeat(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: BrokerHeartbeatRequest,
        api_version: int,
) -> BrokerHeartbeatResponse:
    return handler.broker_heartbeat_request_error_response(
        code, msg, req, api_version
    )


def error_broker_registration(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: BrokerRegistrationRequest,
        api_version: int,
) -> BrokerRegistrationResponse:
    return handler.broker_registration_request_error_response(
        code, msg, req, api_version
    )


def error_consumer_group_describe(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: ConsumerGroupDescribeRequest,
        api_version: int,
) -> ConsumerGroupDescribeResponse:
    return handler.consumer_group_describe_request_error_response(
        code, msg, req, api_version
    )


def error_consumer_group_heartbeat(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: ConsumerGroupHeartbeatRequest,
        api_version: int,
) -> ConsumerGroupHeartbeatResponse:
    return handler.consumer_group_heartbeat_request_error_response(
        code, msg, req, api_version
    )


def error_controller_registration(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: ControllerRegistrationRequest,
        api_version: int,
) -> ControllerRegistrationResponse:
    return handler.controller_registration_request_error_response(
        code, msg, req, api_version
    )


def error_add_raft_voter(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: AddRaftVoterRequest,
        api_version: int,
) -> AddRaftVoterResponse:
    return handler.add_raft_voter_request_error_response(
        code, msg, req, api_version
    )

def error_allocate_producer_ids(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: AllocateProducerIdsRequest,
        api_version: int,
) -> AllocateProducerIdsResponse:
    return handler.allocate_producer_ids_request_error_response(code, msg, req, api_version)


def error_create_acls(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: CreateAclsRequest,
        api_version: int,
) -> CreateAclsResponse:
    return handler.create_acls_request_error_response(
        code, msg, req, api_version
    )


def error_create_delegation_token(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: CreateDelegationTokenRequest,
        api_version: int,
) -> CreateDelegationTokenResponse:
    return handler.create_delegation_token_request_error_response(
        code, msg, req, api_version
    )


def error_create_partitions(
        handler: KafkaHandler,
        code: ErrorCode,
        msg: str,
        req: CreatePartitionsRequest,
        api_version: int,
) -> CreatePartitionsResponse:
    return handler.create_partitions_request_error_response(
        code, msg, req, api_version
    )


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
    FETCH_API_KEY: RequestHandlerMeta(
        handler_func=handle_fetch,
        error_response_func=error_fetch,
    ),
    CONTROLLED_SHUTDOWN_API_KEY: RequestHandlerMeta(
        handler_func=handle_controlled_shutdown,
        error_response_func=error_controlled_shutdown,
    ),
    DELETE_TOPICS_API_KEY: RequestHandlerMeta(
        handler_func=handle_delete_topics,
        error_response_func=error_delete_topics,
    ),
    DELETE_ACLS_API_KEY: RequestHandlerMeta(
        handler_func=handle_delete_acls,
        error_response_func=error_delete_acls,
    ),
    DELETE_GROUPS_API_KEY: RequestHandlerMeta(
        handler_func=handle_delete_groups,
        error_response_func=error_delete_groups,
    ),
    DELETE_RECORDS_API_KEY: RequestHandlerMeta(
        handler_func=handle_delete_records,
        error_response_func=error_delete_records,
    ),
    DELETE_SHARE_GROUP_STATE_API_KEY: RequestHandlerMeta(
        handler_func=handle_delete_share_group_state,
        error_response_func=error_delete_share_group_state,
    ),
    DESCRIBE_ACLS_API_KEY: RequestHandlerMeta(
        handler_func=handle_describe_acls,
        error_response_func=error_describe_acls,
    ),
    DESCRIBE_CLIENT_QUOTAS_API_KEY: RequestHandlerMeta(
        handler_func=handle_describe_client_quotas,
        error_response_func=error_describe_client_quotas,
    ),
    DESCRIBE_CLUSTER_API_KEY: RequestHandlerMeta(
        handler_func=handle_describe_cluster,
        error_response_func=error_describe_cluster,
    ),
    DESCRIBE_CONFIGS_API_KEY: RequestHandlerMeta(
        handler_func=handle_describe_configs,
        error_response_func=error_describe_configs,
    ),
    DESCRIBE_DELEGATION_TOKEN_API_KEY: RequestHandlerMeta(
        handler_func=handle_describe_delegation_token,
        error_response_func=error_describe_delegation_token,
    ),
    DESCRIBE_GROUPS_API_KEY: RequestHandlerMeta(
        handler_func=handle_describe_groups,
        error_response_func=error_describe_groups,
    ),
    DESCRIBE_LOG_DIRS_API_KEY: RequestHandlerMeta(
        handler_func=handle_describe_log_dirs,
        error_response_func=error_describe_log_dirs,
    ),
    DESCRIBE_PRODUCERS_API_KEY: RequestHandlerMeta(
        handler_func=handle_describe_producers,
        error_response_func=error_describe_producers,
    ),
    DESCRIBE_QUORUM_API_KEY: RequestHandlerMeta(
        handler_func=handle_describe_quorum,
        error_response_func=error_describe_quorum,
    ),
    DESCRIBE_TOPIC_PARTITIONS_API_KEY: RequestHandlerMeta(
        handler_func=handle_describe_topic_partitions,
        error_response_func=error_describe_topic_partitions,
    ),
    DESCRIBE_TRANSACTIONS_API_KEY: RequestHandlerMeta(
        handler_func=handle_describe_transactions,
        error_response_func=error_describe_transactions,
    ),
    DESCRIBE_USER_SCRAM_CREDENTIALS_API_KEY: RequestHandlerMeta(
        handler_func=handle_describe_user_scram_credentials,
        error_response_func=error_describe_user_scram_credentials,
    ),
    ELECT_LEADERS_API_KEY: RequestHandlerMeta(
        handler_func=handle_elect_leaders,
        error_response_func=error_elect_leaders,
    ),
    END_QUORUM_EPOCH_API_KEY: RequestHandlerMeta(
        handler_func=handle_end_quorum_epoch,
        error_response_func=error_end_quorum_epoch,
    ),
    END_TXN_API_KEY: RequestHandlerMeta(
        handler_func=handle_end_txn,
        error_response_func=error_end_txn,
    ),
    ENVELOPE_API_KEY: RequestHandlerMeta(
        handler_func=handle_envelope,
        error_response_func=error_envelope,
    ),
    ADD_OFFSETS_TO_TXN_API_KEY: RequestHandlerMeta(
        handler_func=handle_add_offsets_to_txn,
        error_response_func=error_add_offsets_to_txn,
    ),
    ADD_PARTITIONS_TO_TXN_API_KEY: RequestHandlerMeta(
        handler_func=handle_add_partitions_to_txn,
        error_response_func=error_add_partitions_to_txn,
    ),
    ALTER_CLIENT_QUOTAS_API_KEY: RequestHandlerMeta(
        handler_func=handle_alter_client_quotas,
        error_response_func=error_alter_client_quotas,
    ),
    ALTER_CONFIGS_API_KEY: RequestHandlerMeta(
        handler_func=handle_alter_configs,
        error_response_func=error_alter_configs,
    ),
    ALTER_PARTITION_API_KEY: RequestHandlerMeta(
        handler_func=handle_alter_partition,
        error_response_func=error_alter_partition,
    ),
    ALTER_PARTITION_REASSIGNMENTS_API_KEY: RequestHandlerMeta(
        handler_func=handle_alter_partition_reassignments,
        error_response_func=error_alter_partition_reassignments,
    ),
    ALTER_REPLICA_LOG_DIRS_API_KEY: RequestHandlerMeta(
        handler_func=handle_alter_replica_log_dirs,
        error_response_func=error_alter_replica_log_dirs,
    ),
    ALTER_USER_SCRAM_CREDENTIALS_API_KEY: RequestHandlerMeta(
        handler_func=handle_alter_user_scram_credentials,
        error_response_func=error_alter_user_scram_credentials,
    ),
    ASSIGN_REPLICAS_TO_DIRS_API_KEY: RequestHandlerMeta(
        handler_func=handle_assign_replicas_to_dirs,
        error_response_func=error_assign_replicas_to_dirs,
    ),
    BEGIN_QUORUM_EPOCH_API_KEY: RequestHandlerMeta(
        handler_func=handle_begin_quorum_epoch,
        error_response_func=error_begin_quorum_epoch,
    ),
    BROKER_HEARTBEAT_API_KEY: RequestHandlerMeta(
        handler_func=handle_broker_heartbeat,
        error_response_func=error_broker_heartbeat,
    ),
    BROKER_REGISTRATION_API_KEY: RequestHandlerMeta(
        handler_func=handle_broker_registration,
        error_response_func=error_broker_registration,
    ),
    CONSUMER_GROUP_DESCRIBE_API_KEY: RequestHandlerMeta(
        handler_func=handle_consumer_group_describe,
        error_response_func=error_consumer_group_describe,
    ),
    CONSUMER_GROUP_HEARTBEAT_API_KEY: RequestHandlerMeta(
        handler_func=handle_consumer_group_heartbeat,
        error_response_func=error_consumer_group_heartbeat,
    ),
    CONTROLLER_REGISTRATION_API_KEY: RequestHandlerMeta(
        handler_func=handle_controller_registration,
        error_response_func=error_controller_registration,
    ),
    ADD_RAFT_VOTER_API_KEY: RequestHandlerMeta(
        handler_func=handle_add_raft_voter,
        error_response_func=error_add_raft_voter,
    ),
    CREATE_ACLS_API_KEY: RequestHandlerMeta(
        handler_func=handle_create_acls,
        error_response_func=error_create_acls,
    ),
    CREATE_DELEGATION_TOKEN_API_KEY: RequestHandlerMeta(
        handler_func=handle_create_delegation_token,
        error_response_func=error_create_delegation_token,
    ),
    CREATE_PARTITIONS_API_KEY: RequestHandlerMeta(
        handler_func=handle_create_partitions,
        error_response_func=error_create_partitions,
    )
}


async def handle_kafka_request(
        api_key: int, buffer: bytes, handler: KafkaHandler, writer: StreamWriter
):
    if api_key not in request_map or api_key not in api_compatibility:
        return

    api_version = struct.unpack(">H", buffer[2:4])[
        0
    ]  # the api version is the next 2 bytes also big endian
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
