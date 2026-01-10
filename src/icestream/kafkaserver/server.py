import asyncio
import datetime
import io
import struct
import uuid
from asyncio import Future, StreamReader, StreamWriter
from asyncio import Server as AsyncIOServer
from typing import Any, Callable, List, Sequence, Awaitable

import kio.schema.api_versions.v0 as api_v0
import kio.schema.api_versions.v1 as api_v1
import kio.schema.api_versions.v2 as api_v2
import kio.schema.api_versions.v3 as api_v3
import kio.schema.api_versions.v4 as api_v4
import kio.schema.metadata.v0 as metadata_v0
import kio.schema.metadata.v1 as metadata_v1
import kio.schema.metadata.v2 as metadata_v2
import kio.schema.metadata.v3 as metadata_v3
import kio.schema.metadata.v4 as metadata_v4
import kio.schema.metadata.v5 as metadata_v5
import kio.schema.metadata.v6 as metadata_v6
import kio.schema.produce.v0 as produce_v0
import kio.schema.produce.v1 as produce_v1
import kio.schema.produce.v2 as produce_v2
import kio.schema.produce.v3 as produce_v3
import kio.schema.produce.v4 as produce_v4
import kio.schema.produce.v5 as produce_v5
import kio.schema.produce.v6 as produce_v6
import kio.schema.produce.v7 as produce_v7
import kio.schema.produce.v8 as produce_v8
import kio.schema.create_topics.v0 as create_topics_v0
import kio.schema.create_topics.v1 as create_topics_v1
import kio.schema.create_topics.v2 as create_topics_v2
import kio.schema.create_topics.v3 as create_topics_v3
import kio.schema.create_topics.v4 as create_topics_v4
import kio.schema.create_topics.v5 as create_topics_v5
import kio.schema.create_topics.v6 as create_topics_v6
import kio.schema.create_topics.v7 as create_topics_v7
import structlog
from kio.index import load_payload_module
from kio.schema.errors import ErrorCode
from kio.schema.types import BrokerId, TopicName
from kio.serial.readers import read_int32
from kio.static.constants import EntityType
from kio.static.primitive import i16, i32, i32Timedelta, i64
from sqlalchemy import select, update
from sqlalchemy.orm import selectinload

from icestream.config import Config
from icestream.kafkaserver.handler import advertised_api_compatibility, handle_kafka_request
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
from icestream.kafkaserver.handlers.create_topics import CreateTopicsRequest, CreateTopicsRequestHeader, \
    CreateTopicsResponse
from icestream.kafkaserver.handlers.fetch import FetchRequest, FetchRequestHeader, FetchResponse, do_fetch
from icestream.kafkaserver.handlers.add_offsets_to_txn import AddOffsetsToTxnRequest, AddOffsetsToTxnRequestHeader, \
    AddOffsetsToTxnResponse
from icestream.kafkaserver.handlers.add_partitions_to_txn import AddPartitionsToTxnRequest, \
    AddPartitionsToTxnRequestHeader, AddPartitionsToTxnResponse
from icestream.kafkaserver.handlers.alter_client_quotas import AlterClientQuotasRequest, AlterClientQuotasRequestHeader, \
    AlterClientQuotasResponse
from icestream.kafkaserver.handlers.alter_configs import AlterConfigsRequest, AlterConfigsRequestHeader, \
    AlterConfigsResponse
from icestream.kafkaserver.handlers.alter_partition import AlterPartitionRequest, AlterPartitionRequestHeader, \
    AlterPartitionResponse
from icestream.kafkaserver.handlers.alter_partition_reassignments import AlterPartitionReassignmentsRequest, \
    AlterPartitionReassignmentsRequestHeader, AlterPartitionReassignmentsResponse
from icestream.kafkaserver.handlers.alter_replica_log_dirs import AlterReplicaLogDirsRequest, \
    AlterReplicaLogDirsRequestHeader, AlterReplicaLogDirsResponse
from icestream.kafkaserver.handlers.alter_user_scram_credentials import AlterUserScramCredentialsRequest, \
    AlterUserScramCredentialsRequestHeader, AlterUserScramCredentialsResponse
from icestream.kafkaserver.handlers.assign_replicas_to_dirs import AssignReplicasToDirsRequest, \
    AssignReplicasToDirsRequestHeader, AssignReplicasToDirsResponse
from icestream.kafkaserver.handlers.begin_quorum_epoch import BeginQuorumEpochRequest, \
    BeginQuorumEpochRequestHeader, BeginQuorumEpochResponse
from icestream.kafkaserver.handlers.broker_heartbeat import BrokerHeartbeatRequest, \
    BrokerHeartbeatRequestHeader, BrokerHeartbeatResponse
from icestream.kafkaserver.handlers.broker_registration import BrokerRegistrationRequest, \
    BrokerRegistrationRequestHeader, BrokerRegistrationResponse
from icestream.kafkaserver.handlers.consumer_group_describe import ConsumerGroupDescribeRequest, \
    ConsumerGroupDescribeRequestHeader, ConsumerGroupDescribeResponse
from icestream.kafkaserver.handlers.consumer_group_heartbeat import ConsumerGroupHeartbeatRequest, \
    ConsumerGroupHeartbeatRequestHeader, ConsumerGroupHeartbeatResponse
from icestream.kafkaserver.handlers.controlled_shutdown import ControlledShutdownRequest, \
    ControlledShutdownRequestHeader, ControlledShutdownResponse
from icestream.kafkaserver.handlers.controller_registration import ControllerRegistrationRequest, \
    ControllerRegistrationRequestHeader, ControllerRegistrationResponse
from icestream.kafkaserver.handlers.delete_topics import DeleteTopicsRequest, DeleteTopicsRequestHeader, \
    DeleteTopicsResponse
from icestream.kafkaserver.handlers.delete_acls import DeleteAclsRequest, DeleteAclsRequestHeader, \
    DeleteAclsResponse
from icestream.kafkaserver.handlers.delete_groups import DeleteGroupsRequest, DeleteGroupsRequestHeader, \
    DeleteGroupsResponse
from icestream.kafkaserver.handlers.delete_records import DeleteRecordsRequest, DeleteRecordsRequestHeader, \
    DeleteRecordsResponse
from icestream.kafkaserver.handlers.delete_share_group_state import DeleteShareGroupStateRequest, \
    DeleteShareGroupStateRequestHeader, DeleteShareGroupStateResponse
from icestream.kafkaserver.handlers.describe_acls import DescribeAclsRequest, DescribeAclsRequestHeader, \
    DescribeAclsResponse
from icestream.kafkaserver.handlers.describe_client_quotas import DescribeClientQuotasRequest, \
    DescribeClientQuotasRequestHeader, DescribeClientQuotasResponse
from icestream.kafkaserver.handlers.describe_cluster import DescribeClusterRequest, DescribeClusterRequestHeader, \
    DescribeClusterResponse
from icestream.kafkaserver.handlers.describe_configs import DescribeConfigsRequest, DescribeConfigsRequestHeader, \
    DescribeConfigsResponse
from icestream.kafkaserver.handlers.describe_delegation_token import DescribeDelegationTokenRequest, \
    DescribeDelegationTokenRequestHeader, DescribeDelegationTokenResponse
from icestream.kafkaserver.handlers.describe_groups import DescribeGroupsRequest, DescribeGroupsRequestHeader, \
    DescribeGroupsResponse
from icestream.kafkaserver.handlers.describe_log_dirs import DescribeLogDirsRequest, DescribeLogDirsRequestHeader, \
    DescribeLogDirsResponse
from icestream.kafkaserver.handlers.describe_producers import DescribeProducersRequest, DescribeProducersRequestHeader, \
    DescribeProducersResponse
from icestream.kafkaserver.handlers.describe_quorum import DescribeQuorumRequest, DescribeQuorumRequestHeader, \
    DescribeQuorumResponse
from icestream.kafkaserver.handlers.describe_topic_partitions import DescribeTopicPartitionsRequest, \
    DescribeTopicPartitionsRequestHeader, DescribeTopicPartitionsResponse
from icestream.kafkaserver.handlers.describe_transactions import DescribeTransactionsRequest, \
    DescribeTransactionsRequestHeader, DescribeTransactionsResponse
from icestream.kafkaserver.handlers.describe_user_scram_credentials import DescribeUserScramCredentialsRequest, \
    DescribeUserScramCredentialsRequestHeader, DescribeUserScramCredentialsResponse
from icestream.kafkaserver.handlers.elect_leaders import ElectLeadersRequest, ElectLeadersRequestHeader, \
    ElectLeadersResponse
from icestream.kafkaserver.handlers.end_quorum_epoch import EndQuorumEpochRequest, EndQuorumEpochRequestHeader, \
    EndQuorumEpochResponse
from icestream.kafkaserver.handlers.end_txn import EndTxnRequest, EndTxnRequestHeader, \
    EndTxnResponse
from icestream.kafkaserver.handlers.envelope import EnvelopeRequest, EnvelopeRequestHeader, \
    EnvelopeResponse
from icestream.kafkaserver.handlers.add_raft_voter import AddRaftVoterRequest, AddRaftVoterRequestHeader, \
    AddRaftVoterResponse
from icestream.kafkaserver.handlers.allocate_producer_ids import AllocateProducerIdsRequest, \
    AllocateProducerIdsRequestHeader, AllocateProducerIdsResponse
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
    do_leave_group,
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
    ListOffsetsResponse, do_list_offsets,
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
from icestream.kafkaserver.handlers.stop_replica import StopReplicaRequest, StopReplicaRequestHeader, \
    StopReplicaResponse
from icestream.kafkaserver.handlers.sync_group import (
    SyncGroupRequest,
    SyncGroupRequestHeader,
    SyncGroupResponse,
    do_sync_group,
)
from icestream.kafkaserver.handlers.txn_offset_commit import TxnOffsetCommitRequest, TxnOffsetCommitRequestHeader, \
    TxnOffsetCommitResponse
from icestream.kafkaserver.handlers.unregister_broker import UnregisterBrokerRequest, UnregisterBrokerRequestHeader, \
    UnregisterBrokerResponse
from icestream.kafkaserver.handlers.update_features import UpdateFeaturesRequest, UpdateFeaturesRequestHeader, \
    UpdateFeaturesResponse
from icestream.kafkaserver.handlers.update_metadata import UpdateMetadataRequest, UpdateMetadataRequestHeader, \
    UpdateMetadataResponse
from icestream.kafkaserver.handlers.vote import VoteRequest, VoteRequestHeader, VoteResponse
from icestream.kafkaserver.handlers.write_txn_markers import WriteTxnMarkersRequest, WriteTxnMarkersRequestHeader, \
    WriteTxnMarkersResponse
from icestream.kafkaserver.handlers.update_raft_voter import UpdateRaftVoterRequest, UpdateRaftVoterRequestHeader, \
    UpdateRaftVoterResponse
from icestream.kafkaserver.handlers.write_share_group_state import WriteShareGroupStateRequest, \
    WriteShareGroupStateRequestHeader, WriteShareGroupStateResponse
from icestream.kafkaserver.protocol import KafkaRecordBatch
from icestream.kafkaserver.types import ProduceTopicPartitionData
from icestream.models import Partition, Topic
from icestream.utils import zero_throttle

log = structlog.get_logger()


class Server:
    def __init__(self, config: Config, queue: asyncio.Queue[ProduceTopicPartitionData]):
        self.listener: AsyncIOServer | None = None
        self.config = config
        self.produce_queue = queue

    async def run(self, host: str = "0.0.0.0", port: int = 9092):
        try:
            self.listener = await asyncio.start_server(Connection(self), host, port)
            log.info(f"Server started listening on {host}:{port}")
            async with self.listener:
                await self.listener.serve_forever()
        except Exception as e:
            log.error(f"Error in server run: {e}")
            if self.listener:
                self.listener.close()


class Connection(KafkaHandler):
    def __init__(self, s: Server):
        self.server: Server = s

    async def __call__(self, reader: StreamReader, writer: StreamWriter) -> None:
        try:
            while True:
                # read more about the kafka protocol here https://kafka.apache.org/0101/protocol.html
                msg_length_bytes = await reader.readexactly(
                    4
                )  # kafka messages are prefixed by their length
                msg_length = read_int32(
                    io.BytesIO(msg_length_bytes)
                )  # read the 4 byte integer, uses >i under the hood
                message = await reader.readexactly(msg_length)
                api_key = struct.unpack(
                    ">H", message[:2]
                )[
                    0
                ]  # the api key is the first field, which is a 2 byte unsigned integer big endian
                await handle_kafka_request(api_key, message, self, writer)

        except asyncio.IncompleteReadError as e:
            log.info(f"client disconnected with error: {e}")
        except Exception as e:
            log.error(f"error handling connection {e}")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
                log.info("connection closed")
            except Exception as e:
                log.error(f"error closing writer: {e}")

    async def handle_produce_request(
        self,
        header: ProduceRequestHeader,
        req: ProduceRequest,
        api_version: int,
        callback: Callable[[ProduceResponse], Any],
    ):
        log.info(f"handling produce request {req}")
        topic_responses: List[produce_v8.response.TopicProduceResponse] = []

        for topic in req.topic_data:
            topic_name = topic.name
            partition_responses: List[produce_v8.response.PartitionProduceResponse] = []

            for partition in topic.partition_data:
                partition_idx = partition.index
                records = partition.records
                error_code: ErrorCode | None = None
                record_count = 0
                parsed_batch: KafkaRecordBatch | None = None

                try:
                    if records is not None:
                        parsed_batch = KafkaRecordBatch.from_bytes(records)
                        record_count = parsed_batch.records_count
                except Exception:
                    log.exception(
                        "failed to parse produce record batch",
                        extra={"topic": topic_name, "partition": partition_idx},
                    )
                    error_code = ErrorCode.invalid_record

                if parsed_batch is None and records is not None and error_code is None:
                    error_code = ErrorCode.invalid_record

                if parsed_batch is not None and parsed_batch.magic != 2:
                    error_code = ErrorCode.unsupported_for_message_format
                    partition_response = produce_v8.response.PartitionProduceResponse(
                        index=i32(partition_idx),
                        error_code=error_code,
                        base_offset=i64(-1),
                        log_append_time=None,
                        log_start_offset=i64(-1),
                        record_errors=(),
                        error_message="wrong magic number",
                    )
                    partition_responses.append(partition_response)
                    continue

                log.info(
                    "produce",
                    topic=topic_name,
                    partition=partition_idx,
                    num_records=record_count,
                )

                if record_count == 0:
                    partition_response = produce_v8.response.PartitionProduceResponse(
                        index=i32(partition_idx),
                        error_code=ErrorCode.none if error_code is None else error_code,
                        base_offset=i64(-1),  # no records so no offsets assigned
                        log_append_time=None,
                        log_start_offset=i64(-1),
                        record_errors=(),
                        error_message=None,
                    )
                    partition_responses.append(partition_response)
                    continue

                # allocate the offsets
                async with self.server.config.async_session_factory() as session:
                    result = await session.execute(
                        update(Partition)
                        .where(
                            Partition.topic_name == topic_name,
                            Partition.partition_number == partition_idx,
                        )
                        .values(last_offset=Partition.last_offset + record_count)
                        .returning(
                            Partition.id,
                            Partition.last_offset,
                            Partition.log_start_offset,
                        )
                    )
                    await session.commit()
                row = result.first()
                # if the row is none, it's probably because there's no topic or partition for it
                if row is None:
                    error_code = ErrorCode.unknown_topic_or_partition
                    partition_response = produce_v8.response.PartitionProduceResponse(
                        index=i32(partition_idx),
                        error_code=ErrorCode.none if error_code is None else error_code,
                        base_offset=i64(
                            -1
                        ),  # likely no topic/partition, so no offsets assigned
                        log_append_time=None,
                        log_start_offset=i64(-1),
                        record_errors=(),
                        error_message="unknown topic or partition",
                    )
                    partition_responses.append(partition_response)
                    continue
                _, last_offset, log_start_offset = row
                first_offset = last_offset - record_count + 1

                # need to send the message batch over to the WALManager via self.server.produce_queue (asyncio.Queue)
                # but with a future so that this part that's creating the PartitionProduceResponse can wait for the response from the WALManager
                # but at the end need to append the PartitionProduceResponse to the `partition_responses` list.
                partition_flush_result_fut = Future()
                produce_topic_partition_data = ProduceTopicPartitionData(
                    topic=topic_name,
                    partition=partition_idx,
                    kafka_record_batch=parsed_batch if parsed_batch else KafkaRecordBatch.from_bytes(records),
                    flush_result=partition_flush_result_fut,
                )
                await self.server.produce_queue.put(produce_topic_partition_data)

                try:
                    await asyncio.wait_for(
                        partition_flush_result_fut,
                        timeout=self.server.config.FLUSH_INTERVAL * 2,
                    )
                    # success
                except asyncio.CancelledError:
                    error_code = ErrorCode.unknown_server_error
                except asyncio.TimeoutError:
                    error_code = ErrorCode.request_timed_out
                except Exception as e:
                    error_code = ErrorCode.unknown_server_error
                finally:
                    partition_response = produce_v8.response.PartitionProduceResponse(
                        index=i32(partition_idx),
                        error_code=ErrorCode.none if error_code is None else error_code,
                        base_offset=i64(first_offset if error_code is None else -1),
                        log_append_time=None,  # TODO
                        log_start_offset=i64(log_start_offset),
                        record_errors=(),
                        error_message=None,
                    )
                    partition_responses.append(partition_response)

            topic_response = produce_v8.response.TopicProduceResponse(
                name=topic_name,
                partition_responses=tuple(partition_responses),
            )
            topic_responses.append(topic_response)

        reference_response = produce_v8.response.ProduceResponse(
            responses=tuple(topic_responses),
            throttle_time=i32Timedelta.parse(datetime.timedelta(milliseconds=0)),
        )

        if api_version == 0:
            topics = []
            for topic in reference_response.responses:
                partitions = [
                    produce_v0.response.PartitionProduceResponse(
                        index=p.index,
                        error_code=p.error_code,
                        base_offset=p.base_offset,
                    )
                    for p in topic.partition_responses
                ]
                topics.append(
                    produce_v0.response.TopicProduceResponse(
                        name=topic.name,
                        partition_responses=tuple(partitions),
                    )
                )
            await callback(produce_v0.response.ProduceResponse(responses=tuple(topics)))

        elif api_version == 1:
            topics = []
            for topic in reference_response.responses:
                partitions = [
                    produce_v1.response.PartitionProduceResponse(
                        index=p.index,
                        error_code=p.error_code,
                        base_offset=p.base_offset,
                    )
                    for p in topic.partition_responses
                ]
                topics.append(
                    produce_v1.response.TopicProduceResponse(
                        name=topic.name,
                        partition_responses=tuple(partitions),
                    )
                )
            await callback(
                produce_v1.response.ProduceResponse(
                    responses=tuple(topics),
                    throttle_time=reference_response.throttle_time,
                )
            )

        elif api_version == 2:
            topics = []
            for topic in reference_response.responses:
                partitions = [
                    produce_v2.response.PartitionProduceResponse(
                        index=p.index,
                        error_code=p.error_code,
                        base_offset=p.base_offset,
                        log_append_time=p.log_append_time,
                    )
                    for p in topic.partition_responses
                ]
                topics.append(
                    produce_v2.response.TopicProduceResponse(
                        name=topic.name,
                        partition_responses=tuple(partitions),
                    )
                )
            await callback(
                produce_v2.response.ProduceResponse(
                    responses=tuple(topics),
                    throttle_time=reference_response.throttle_time,
                )
            )

        elif api_version == 3:
            topics = []
            for topic in reference_response.responses:
                partitions = [
                    produce_v3.response.PartitionProduceResponse(
                        index=p.index,
                        error_code=p.error_code,
                        base_offset=p.base_offset,
                        log_append_time=p.log_append_time,
                    )
                    for p in topic.partition_responses
                ]
                topics.append(
                    produce_v3.response.TopicProduceResponse(
                        name=topic.name,
                        partition_responses=tuple(partitions),
                    )
                )
            await callback(
                produce_v3.response.ProduceResponse(
                    responses=tuple(topics),
                    throttle_time=reference_response.throttle_time,
                )
            )

        elif api_version == 4:
            topics = []
            for topic in reference_response.responses:
                partitions = [
                    produce_v4.response.PartitionProduceResponse(
                        index=p.index,
                        error_code=p.error_code,
                        base_offset=p.base_offset,
                        log_append_time=p.log_append_time,
                    )
                    for p in topic.partition_responses
                ]
                topics.append(
                    produce_v4.response.TopicProduceResponse(
                        name=topic.name,
                        partition_responses=tuple(partitions),
                    )
                )
            await callback(
                produce_v4.response.ProduceResponse(
                    responses=tuple(topics),
                    throttle_time=reference_response.throttle_time,
                )
            )

        elif api_version == 5:
            topics = []
            for topic in reference_response.responses:
                partitions = [
                    produce_v5.response.PartitionProduceResponse(
                        index=p.index,
                        error_code=p.error_code,
                        base_offset=p.base_offset,
                        log_append_time=p.log_append_time,
                        log_start_offset=p.log_start_offset,
                    )
                    for p in topic.partition_responses
                ]
                topics.append(
                    produce_v5.response.TopicProduceResponse(
                        name=topic.name,
                        partition_responses=tuple(partitions),
                    )
                )
            await callback(
                produce_v5.response.ProduceResponse(
                    responses=tuple(topics),
                    throttle_time=reference_response.throttle_time,
                )
            )

        elif api_version == 6:
            topics = []
            for topic in reference_response.responses:
                partitions = [
                    produce_v6.response.PartitionProduceResponse(
                        index=p.index,
                        error_code=p.error_code,
                        base_offset=p.base_offset,
                        log_append_time=p.log_append_time,
                        log_start_offset=p.log_start_offset,
                    )
                    for p in topic.partition_responses
                ]
                topics.append(
                    produce_v6.response.TopicProduceResponse(
                        name=topic.name,
                        partition_responses=tuple(partitions),
                    )
                )
            await callback(
                produce_v6.response.ProduceResponse(
                    responses=tuple(topics),
                    throttle_time=reference_response.throttle_time,
                )
            )

        elif api_version == 7:
            topics = []
            for topic in reference_response.responses:
                partitions = [
                    produce_v7.response.PartitionProduceResponse(
                        index=p.index,
                        error_code=p.error_code,
                        base_offset=p.base_offset,
                        log_append_time=p.log_append_time,
                        log_start_offset=p.log_start_offset,
                    )
                    for p in topic.partition_responses
                ]
                topics.append(
                    produce_v7.response.TopicProduceResponse(
                        name=topic.name,
                        partition_responses=tuple(partitions),
                    )
                )
            await callback(
                produce_v7.response.ProduceResponse(
                    responses=tuple(topics),
                    throttle_time=reference_response.throttle_time,
                )
            )

        elif api_version == 8:
            await callback(reference_response)

        else:
            log.error(f"Unsupported produce version: {api_version}")

    def produce_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: ProduceRequest,
        api_version: int,
    ) -> ProduceResponse:
        # v8 has error_message and record_errors in the PartitionProduceResponse, ignore it for now
        mod = load_payload_module(0, api_version, EntityType.response)
        responses = []
        for i, topic_data in enumerate(req.topic_data):
            partition_response = []
            for j, partition_data in enumerate(topic_data.partition_data):
                partition_produce_response = mod.PartitionProduceResponse(
                    index=partition_data.index,
                    error_code=error_code,
                    base_offset=i64(0),
                    record_errors=(),
                )
                partition_response.append(partition_produce_response)

            topic_produce_response = mod.TopicProduceResponse(
                name=topic_data.name, partition_responses=tuple(partition_response)
            )
            responses.append(topic_produce_response)
        resp = mod.ProduceResponse(responses=tuple(responses))
        return resp

    async def handle_metadata_request(
        self,
        header: MetadataRequestHeader,
        req: MetadataRequest,
        api_version: int,
        callback: Callable[[MetadataResponse], Any],
    ):
        log.info("handling metadata request", topics=[t.name for t in req.topics])

        async with self.server.config.async_session_factory() as session:
            topic_result: Sequence[Topic]
            if not req.topics:
                result = await session.execute(
                    select(Topic).options(selectinload(Topic.partitions))
                )
                topic_result = result.scalars().all()
            else:
                topic_names = [t.name for t in req.topics]
                result = await session.execute(
                    select(Topic)
                    .where(Topic.name.in_(topic_names))
                    .options(selectinload(Topic.partitions))
                )
                topic_result = result.scalars().all()

        # get brokers
        # since we're stateless we might be able to get away with spoofing a single broker
        # host would be the lb or k8s service or whatever
        # node id would always be 0
        # rack would always be None
        broker = metadata_v6.response.MetadataResponseBroker(
            node_id=BrokerId(0), host="localhost", port=i32(9092), rack=None
        )

        # we need to respect the topic list passed in by the request
        # in our case it'll get passed to postgres, but an empty list means all of them

        topics: List[metadata_v6.response.MetadataResponseTopic] = []
        for topic in topic_result:
            partition_metadata = [
                metadata_v6.response.MetadataResponsePartition(
                    error_code=ErrorCode.none,
                    partition_index=i32(pidx.partition_number),
                    leader_id=BrokerId(0),
                    replica_nodes=(BrokerId(0),),
                    isr_nodes=(BrokerId(0),),
                    offline_replicas=(),
                )
                for pidx in topic.partitions
            ]
            topics.append(
                metadata_v6.response.MetadataResponseTopic(
                    error_code=ErrorCode.none,
                    name=TopicName(topic.name),
                    is_internal=False,
                    partitions=tuple(partition_metadata),
                )
            )

        response = metadata_v6.response.MetadataResponse(
            throttle_time=i32Timedelta.parse(datetime.timedelta(milliseconds=0)),
            brokers=(broker,),
            cluster_id="test-cluster",
            controller_id=BrokerId(0),
            topics=tuple(topics),
        )

        if api_version == 0:
            _broker = metadata_v0.response.MetadataResponseBroker(
                node_id=broker.node_id,
                host=broker.host,
                port=broker.port,
            )
            _topics = []
            for topic in topics:
                _partition_metadata = []
                for partition in topic.partitions:
                    _partition = metadata_v0.response.MetadataResponsePartition(
                        error_code=partition.error_code,
                        partition_index=partition.partition_index,
                        leader_id=partition.leader_id,
                        replica_nodes=partition.replica_nodes,
                        isr_nodes=partition.isr_nodes,
                    )
                    _partition_metadata.append(_partition)
                _topic = metadata_v0.response.MetadataResponseTopic(
                    error_code=topic.error_code,
                    name=topic.name,
                    partitions=tuple(_partition_metadata),
                )
                _topics.append(_topic)
            _response = metadata_v0.response.MetadataResponse(
                brokers=(_broker,), topics=tuple(_topics)
            )
            await callback(_response)

        elif api_version == 1:
            _broker = metadata_v1.response.MetadataResponseBroker(
                node_id=broker.node_id,
                host=broker.host,
                port=broker.port,
                rack=broker.rack,
            )
            _topics = []
            for topic in topics:
                _partition_metadata = []
                for partition in topic.partitions:
                    _partition = metadata_v1.response.MetadataResponsePartition(
                        error_code=partition.error_code,
                        partition_index=partition.partition_index,
                        leader_id=partition.leader_id,
                        replica_nodes=partition.replica_nodes,
                        isr_nodes=partition.isr_nodes,
                    )
                    _partition_metadata.append(_partition)
                _topic = metadata_v1.response.MetadataResponseTopic(
                    error_code=topic.error_code,
                    name=topic.name,
                    is_internal=topic.is_internal,
                    partitions=tuple(_partition_metadata),
                )
                _topics.append(_topic)
            _response = metadata_v1.response.MetadataResponse(
                brokers=(_broker,),
                topics=tuple(_topics),
                controller_id=BrokerId(_broker.node_id),
            )
            await callback(_response)

        elif api_version == 2:
            _broker = metadata_v2.response.MetadataResponseBroker(
                node_id=broker.node_id,
                host=broker.host,
                port=broker.port,
                rack=broker.rack,
            )
            _topics = []
            for topic in topics:
                _partition_metadata = []
                for partition in topic.partitions:
                    _partition = metadata_v2.response.MetadataResponsePartition(
                        error_code=partition.error_code,
                        partition_index=partition.partition_index,
                        leader_id=partition.leader_id,
                        replica_nodes=partition.replica_nodes,
                        isr_nodes=partition.isr_nodes,
                    )
                    _partition_metadata.append(_partition)
                _topic = metadata_v2.response.MetadataResponseTopic(
                    error_code=topic.error_code,
                    name=topic.name,
                    is_internal=topic.is_internal,
                    partitions=tuple(_partition_metadata),
                )
                _topics.append(_topic)
            _response = metadata_v2.response.MetadataResponse(
                brokers=(_broker,),
                topics=tuple(_topics),
                controller_id=BrokerId(_broker.node_id),
                cluster_id=response.cluster_id,
            )
            await callback(_response)

        elif api_version == 3:
            _broker = metadata_v3.response.MetadataResponseBroker(
                node_id=broker.node_id,
                host=broker.host,
                port=broker.port,
                rack=broker.rack,
            )
            _topics = []
            for topic in topics:
                _partition_metadata = []
                for partition in topic.partitions:
                    _partition = metadata_v3.response.MetadataResponsePartition(
                        error_code=partition.error_code,
                        partition_index=partition.partition_index,
                        leader_id=partition.leader_id,
                        replica_nodes=partition.replica_nodes,
                        isr_nodes=partition.isr_nodes,
                    )
                    _partition_metadata.append(_partition)
                _topic = metadata_v3.response.MetadataResponseTopic(
                    error_code=topic.error_code,
                    name=topic.name,
                    is_internal=topic.is_internal,
                    partitions=tuple(_partition_metadata),
                )
                _topics.append(_topic)
            _response = metadata_v3.response.MetadataResponse(
                brokers=(_broker,),
                topics=tuple(_topics),
                controller_id=BrokerId(_broker.node_id),
                cluster_id=response.cluster_id,
                throttle_time=response.throttle_time,
            )
            await callback(_response)

        elif api_version == 4:
            _broker = metadata_v4.response.MetadataResponseBroker(
                node_id=broker.node_id,
                host=broker.host,
                port=broker.port,
                rack=broker.rack,
            )
            _topics = []
            for topic in topics:
                _partition_metadata = []
                for partition in topic.partitions:
                    _partition = metadata_v4.response.MetadataResponsePartition(
                        error_code=partition.error_code,
                        partition_index=partition.partition_index,
                        leader_id=partition.leader_id,
                        replica_nodes=partition.replica_nodes,
                        isr_nodes=partition.isr_nodes,
                    )
                    _partition_metadata.append(_partition)
                _topic = metadata_v4.response.MetadataResponseTopic(
                    error_code=topic.error_code,
                    name=topic.name,
                    is_internal=topic.is_internal,
                    partitions=tuple(_partition_metadata),
                )
                _topics.append(_topic)
            _response = metadata_v4.response.MetadataResponse(
                brokers=(_broker,),
                topics=tuple(_topics),
                controller_id=BrokerId(_broker.node_id),
                cluster_id=response.cluster_id,
                throttle_time=response.throttle_time,
            )
            await callback(_response)

        elif api_version == 5:
            _broker = metadata_v5.response.MetadataResponseBroker(
                node_id=broker.node_id,
                host=broker.host,
                port=broker.port,
                rack=broker.rack,
            )
            _topics = []
            for topic in topics:
                _partition_metadata = []
                for partition in topic.partitions:
                    _partition = metadata_v5.response.MetadataResponsePartition(
                        error_code=partition.error_code,
                        partition_index=partition.partition_index,
                        leader_id=partition.leader_id,
                        replica_nodes=partition.replica_nodes,
                        isr_nodes=partition.isr_nodes,
                        offline_replicas=partition.offline_replicas,
                    )
                    _partition_metadata.append(_partition)
                _topic = metadata_v5.response.MetadataResponseTopic(
                    error_code=topic.error_code,
                    name=topic.name,
                    is_internal=topic.is_internal,
                    partitions=tuple(_partition_metadata),
                )
                _topics.append(_topic)
            _response = metadata_v5.response.MetadataResponse(
                brokers=(_broker,),
                topics=tuple(_topics),
                controller_id=BrokerId(_broker.node_id),
                cluster_id=response.cluster_id,
                throttle_time=response.throttle_time,
            )
            await callback(_response)

        elif api_version == 6:
            await callback(response)

        else:
            # unsupported - should be an error response
            pass

    def metadata_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: MetadataRequest,
        api_version: int,
    ) -> MetadataResponse:
        # there's no point in returning brokers because there's no error code
        # similarly there's no point in populating the topic partitions
        # just populate the topics with the name and the error code
        # because the typing is weird, the req and api_version might not match
        mod = load_payload_module(3, api_version, EntityType.response)
        response_topic_class = mod.MetadataResponseTopic
        response_class = mod.MetadataResponse
        _topics = []
        for topic in req.topics:
            _topic = response_topic_class(name=topic.name, error_code=error_code)
            _topics.append(_topic)

        return response_class(brokers=(), topics=tuple(_topics))

    async def handle_api_versions_request(
        self,
        header: ApiVersionsRequestHeader,
        req: ApiVersionsRequest,
        api_version: int,
        callback: Callable[[ApiVersionsResponse], Any],
    ):
        versions = tuple(
            api_v4.response.ApiVersion(
                api_key=i16(api_key), min_version=i16(min_ver), max_version=i16(max_ver)
            )
            for api_key, (min_ver, max_ver) in advertised_api_compatibility.items()
        )

        response = api_v4.response.ApiVersionsResponse(
            error_code=ErrorCode.none,
            api_keys=versions,
            throttle_time=i32Timedelta.parse(datetime.timedelta(milliseconds=0)),
            supported_features=(),
            finalized_features_epoch=i64(-1),
            finalized_features=(),
            zk_migration_ready=False,
        )
        if api_version == 0:
            _versions = tuple(
                api_v0.response.ApiVersion(
                    api_key=_version.api_key,
                    min_version=_version.min_version,
                    max_version=_version.max_version,
                )
                for _version in response.api_keys
            )
            _response = api_v0.response.ApiVersionsResponse(
                error_code=response.error_code, api_keys=_versions
            )
            await callback(_response)
        elif api_version == 1:
            _versions = tuple(
                api_v1.response.ApiVersion(
                    api_key=_version.api_key,
                    min_version=_version.min_version,
                    max_version=_version.max_version,
                )
                for _version in response.api_keys
            )
            _response = api_v1.response.ApiVersionsResponse(
                error_code=response.error_code,
                api_keys=_versions,
                throttle_time=response.throttle_time,
            )
            await callback(_response)
        elif api_version == 2:
            _versions = tuple(
                api_v2.response.ApiVersion(
                    api_key=_version.api_key,
                    min_version=_version.min_version,
                    max_version=_version.max_version,
                )
                for _version in response.api_keys
            )
            _response = api_v2.response.ApiVersionsResponse(
                error_code=response.error_code,
                api_keys=_versions,
                throttle_time=response.throttle_time,
            )
            await callback(_response)
        elif api_version == 3:
            _versions = tuple(
                api_v3.response.ApiVersion(
                    api_key=_version.api_key,
                    min_version=_version.min_version,
                    max_version=_version.max_version,
                )
                for _version in response.api_keys
            )
            _response = api_v3.response.ApiVersionsResponse(
                error_code=response.error_code,
                api_keys=_versions,
                throttle_time=response.throttle_time,
                supported_features=response.supported_features,
                finalized_features_epoch=response.finalized_features_epoch,
                finalized_features=response.finalized_features,
                zk_migration_ready=response.zk_migration_ready,
            )
            await callback(_response)
        elif api_version == 4:
            await callback(response)
        else:
            log.error(f"unsupported api versions version: {api_version}")

    def api_versions_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: ApiVersionsRequest,
        api_version: int,
    ) -> ApiVersionsResponse:
        mod = load_payload_module(18, api_version, EntityType.response)
        return mod.ApiVersionsResponse(
            error_code=error_code,
            api_keys=(),
        )

    async def handle_create_topics_request(
        self,
        header: CreateTopicsRequestHeader,
        req: CreateTopicsRequest,
        api_version: int,
        callback: Callable[[CreateTopicsResponse], Any],
    ):
        results = []

        for topic in req.topics:
            log.info(
                "create_topic", topic=topic.name, num_partitions=topic.num_partitions
            )

            result = create_topics_v7.response.CreatableTopicResult(
                name=topic.name,
                topic_id=uuid.uuid4(),
                error_code=ErrorCode.none,
                error_message=None,
                topic_config_error_code=i16(0),
                num_partitions=i32(topic.num_partitions),
                replication_factor=i16(topic.replication_factor),
                configs=None,  # configs not returned
            )
            results.append(result)

        response = CreateTopicsResponse(
            throttle_time=i32Timedelta.parse(datetime.timedelta(milliseconds=0)),
            topics=tuple(results),
        )
        await callback(response)

    def create_topics_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: CreateTopicsRequest,
        api_version: int,
    ) -> CreateTopicsResponse:
        mod = load_payload_module(0, api_version, EntityType.response)
        results = []

        for topic in req.topics:
            result = mod.CreatableTopicResult(
                name=topic.name,
                topic_id=None,
                error_code=error_code,
                error_message=error_message,
                topic_config_error_code=i16(0),
                num_partitions=i32(-1),
                replication_factor=i16(-1),
                configs=None,
            )
            results.append(result)

        return mod.CreateTopicsResponse(
            throttle_time=i32Timedelta.parse(datetime.timedelta(milliseconds=0)),
            topics=tuple(results),
        )

    async def handle_fetch_request(
        self,
        header: FetchRequestHeader,
        req: FetchRequest,
        api_version: int,
        callback: Callable[[FetchResponse], Awaitable[None]],
    ) -> FetchResponse:
        resp = await do_fetch(self.server.config, req, api_version)
        await callback(resp)

    def fetch_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: FetchRequest,
        api_version: int,
    ) -> FetchResponse:
        mod = load_payload_module(1, api_version, EntityType.response)
        responses = []
        for i, topic in enumerate(req.topics):
            partition_data = []
            for j, partition in enumerate(topic.partitions):
                partition_fetch_data = mod.PartitionData(
                    partition_index=partition.partition,
                    error_code=error_code,
                )
                partition_data.append(partition_fetch_data)
            fetchable_topic_response = mod.FetchableTopicResponse(
                topic=topic.topic,
                partitions=tuple(partition_data)
            )
            responses.append(fetchable_topic_response)
        resp = mod.FetchResponse(
            responses=tuple(responses)
        )
        return resp

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

    async def handle_leave_group_request(
        self,
        header: LeaveGroupRequestHeader,
        req: LeaveGroupRequest,
        api_version: int,
        callback: Callable[[LeaveGroupResponse], Awaitable[None]],
    ):
        log.info(
            "handling leave group request",
            group_id=req.group_id,
            api_version=api_version,
        )
        try:
            resp = await do_leave_group(self.server.config, req, api_version)
        except Exception as exc:  # pragma: no cover - defensive fallback
            log.exception(
                "leave group request failed",
                group_id=req.group_id,
                api_version=api_version,
                error=str(exc),
            )
            fallback = self.leave_group_request_error_response(
                ErrorCode.unknown_server_error,
                "leave group handler failure",
                req,
                api_version,
            )
            await callback(fallback)
            return

        await callback(resp)

    def leave_group_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: LeaveGroupRequest,
        api_version: int,
    ) -> LeaveGroupResponse:
        mod = load_payload_module(13, api_version, EntityType.response)
        fields = getattr(mod.LeaveGroupResponse, "__dataclass_fields__", {})
        kwargs = {"error_code": error_code}
        if "throttle_time" in fields:
            kwargs["throttle_time"] = zero_throttle()
        if "members" in fields:
            kwargs["members"] = tuple()
        return mod.LeaveGroupResponse(**kwargs)

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

    async def handle_list_offsets_request(
        self,
        header: ListOffsetsRequestHeader,
        req: ListOffsetsRequest,
        api_version: int,
        callback: Callable[[ListOffsetsResponse], Awaitable[None]],
    ):
        resp = await do_list_offsets(self.server.config, req, api_version)
        await callback(resp)

    def list_offsets_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: ListOffsetsRequest,
        api_version: int,
    ) -> ListOffsetsResponse:
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

    async def handle_sync_group_request(
        self,
        header: SyncGroupRequestHeader,
        req: SyncGroupRequest,
        api_version: int,
        callback: Callable[[SyncGroupResponse], Awaitable[None]],
    ):
        log.info(
            "handling sync group request",
            group_id=req.group_id,
            member_id=req.member_id,
            generation=int(req.generation_id),
            api_version=api_version,
        )
        try:
            resp = await do_sync_group(self.server.config, req, api_version)
        except Exception as exc:  # pragma: no cover - defensive fallback
            log.exception(
                "sync group request failed",
                group_id=req.group_id,
                member_id=req.member_id,
                api_version=api_version,
                error=str(exc),
            )
            fallback = self.sync_group_request_error_response(
                ErrorCode.unknown_server_error,
                "sync group handler failure",
                req,
                api_version,
            )
            await callback(fallback)
            return

        await callback(resp)

    def sync_group_request_error_response(
        self,
        error_code: ErrorCode,
        error_message: str,
        req: SyncGroupRequest,
        api_version: int,
    ) -> SyncGroupResponse:
        mod = load_payload_module(14, api_version, EntityType.response)
        fields = getattr(mod.SyncGroupResponse, "__dataclass_fields__", {})
        kwargs = {
            "error_code": error_code,
            "assignment": b"",
        }
        if "throttle_time" in fields:
            kwargs["throttle_time"] = zero_throttle()
        if "protocol_type" in fields:
            kwargs["protocol_type"] = None
        if "protocol_name" in fields:
            kwargs["protocol_name"] = None
        return mod.SyncGroupResponse(**kwargs)

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
