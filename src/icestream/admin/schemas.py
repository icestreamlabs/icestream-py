from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from icestream.admin.errors import AdminErrorCode

ADMIN_API_VERSION = "v1"


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ErrorBodyV1(StrictModel):
    code: AdminErrorCode
    message: str


class ErrorEnvelopeV1(StrictModel):
    api_version: Literal["v1"] = ADMIN_API_VERSION
    status: Literal["error"] = "error"
    error: ErrorBodyV1
    request_id: str | None = None


class SuccessEnvelopeV1(StrictModel):
    api_version: Literal["v1"] = ADMIN_API_VERSION
    status: Literal["ok"] = "ok"
    request_id: str | None = None


class TopicPartitionMetadataV1(StrictModel):
    partition_number: int
    log_start_offset: int
    last_offset: int


class TopicSummaryV1(StrictModel):
    id: int
    name: str
    is_internal: bool
    partition_count: int


class TopicDetailV1(StrictModel):
    id: int
    name: str
    is_internal: bool
    partitions: list[TopicPartitionMetadataV1]
    created_at: datetime
    updated_at: datetime


class TopicCreateRequestV1(StrictModel):
    name: str = Field(
        min_length=1,
        max_length=249,
        examples=["orders.v1"],
    )
    num_partitions: int = Field(
        default=3,
        ge=1,
        le=10_000,
        examples=[3],
    )
    is_internal: bool = False


class TopicCreateDataV1(StrictModel):
    topic: TopicSummaryV1


class TopicCreateResponseV1(SuccessEnvelopeV1):
    data: TopicCreateDataV1


class TopicListDataV1(StrictModel):
    topics: list[TopicSummaryV1]
    limit: int
    offset: int
    total: int


class TopicListResponseV1(SuccessEnvelopeV1):
    data: TopicListDataV1


class TopicGetDataV1(StrictModel):
    topic: TopicDetailV1


class TopicGetResponseV1(SuccessEnvelopeV1):
    data: TopicGetDataV1


class TopicDeleteDataV1(StrictModel):
    name: str
    deleted: bool = True


class TopicDeleteResponseV1(SuccessEnvelopeV1):
    data: TopicDeleteDataV1


class HealthDataV1(StrictModel):
    status: Literal["ok"] = "ok"


class HealthResponseV1(SuccessEnvelopeV1):
    data: HealthDataV1


class ReadinessDataV1(StrictModel):
    status: Literal["ready"] = "ready"
    database: Literal["up"] = "up"


class ReadinessResponseV1(SuccessEnvelopeV1):
    data: ReadinessDataV1


class TopicOffsetPartitionVisibilityV1(StrictModel):
    partition_number: int
    log_start_offset: int
    latest_offset: int
    wal_uncompacted_segment_count: int
    wal_uncompacted_min_offset: int | None
    wal_uncompacted_max_offset: int | None
    topic_wal_segment_count: int
    topic_wal_min_offset: int | None
    topic_wal_max_offset: int | None


class TopicOffsetsDataV1(StrictModel):
    topic_name: str
    partitions: list[TopicOffsetPartitionVisibilityV1]


class TopicOffsetsResponseV1(SuccessEnvelopeV1):
    data: TopicOffsetsDataV1


class ConsumerGroupSummaryV1(StrictModel):
    group_id: str
    state: str
    generation: int
    protocol_type: str | None
    selected_protocol: str | None
    member_count: int
    updated_at: datetime


class ConsumerGroupListDataV1(StrictModel):
    groups: list[ConsumerGroupSummaryV1]
    limit: int
    offset: int
    total: int


class ConsumerGroupListResponseV1(SuccessEnvelopeV1):
    data: ConsumerGroupListDataV1


class ConsumerGroupMemberV1(StrictModel):
    member_id: str
    group_instance_id: str | None
    is_in_sync: bool
    last_heartbeat_at: datetime


class ConsumerGroupAssignmentV1(StrictModel):
    member_id: str
    topic_name: str
    partition_number: int
    generation: int


class ConsumerGroupOffsetV1(StrictModel):
    topic_name: str
    partition_number: int
    committed_offset: int
    committed_metadata: str | None
    commit_timestamp: datetime | None
    committed_member_id: str | None
    committed_generation: int


class ConsumerGroupDetailDataV1(StrictModel):
    group: ConsumerGroupSummaryV1
    members: list[ConsumerGroupMemberV1]
    assignments: list[ConsumerGroupAssignmentV1]
    offsets: list[ConsumerGroupOffsetV1]


class ConsumerGroupDetailResponseV1(SuccessEnvelopeV1):
    data: ConsumerGroupDetailDataV1
