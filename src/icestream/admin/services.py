from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import delete, func, select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from icestream.admin.errors import (
    AlreadyExistsError,
    InternalError,
    InvalidRequestError,
    NotFoundError,
)
from icestream.kafkaserver.topic_backends import topic_backend_for_name
from icestream.models import Partition, Topic, TopicWALFile, TopicWALFileOffset, WALFile, WALFileOffset
from icestream.models.consumer_groups import (
    ConsumerGroup,
    GroupAssignment,
    GroupMember,
    GroupOffset,
)

_TOPIC_NAME_RE = re.compile(r"^[A-Za-z0-9._-]+$")
_MAX_TOPIC_NAME_LENGTH = 249
_MAX_PARTITIONS = 10_000


@dataclass(frozen=True, slots=True)
class TopicSummaryResult:
    id: int
    name: str
    is_internal: bool
    partition_count: int


@dataclass(frozen=True, slots=True)
class TopicPartitionResult:
    partition_number: int
    log_start_offset: int
    last_offset: int


@dataclass(frozen=True, slots=True)
class TopicDetailResult:
    id: int
    name: str
    is_internal: bool
    partitions: list[TopicPartitionResult]
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class TopicListResult:
    topics: list[TopicSummaryResult]
    limit: int
    offset: int
    total: int


@dataclass(frozen=True, slots=True)
class TopicPartitionOffsetVisibilityResult:
    partition_number: int
    log_start_offset: int
    latest_offset: int
    wal_uncompacted_segment_count: int
    wal_uncompacted_min_offset: int | None
    wal_uncompacted_max_offset: int | None
    topic_wal_segment_count: int
    topic_wal_min_offset: int | None
    topic_wal_max_offset: int | None


@dataclass(frozen=True, slots=True)
class TopicOffsetsResult:
    topic_name: str
    partitions: list[TopicPartitionOffsetVisibilityResult]


@dataclass(frozen=True, slots=True)
class ConsumerGroupSummaryResult:
    group_id: str
    state: str
    generation: int
    protocol_type: str | None
    selected_protocol: str | None
    member_count: int
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class ConsumerGroupMemberResult:
    member_id: str
    group_instance_id: str | None
    is_in_sync: bool
    last_heartbeat_at: datetime


@dataclass(frozen=True, slots=True)
class ConsumerGroupAssignmentResult:
    member_id: str
    topic_name: str
    partition_number: int
    generation: int


@dataclass(frozen=True, slots=True)
class ConsumerGroupOffsetResult:
    topic_name: str
    partition_number: int
    committed_offset: int
    committed_metadata: str | None
    commit_timestamp: datetime | None
    committed_member_id: str | None
    committed_generation: int


@dataclass(frozen=True, slots=True)
class ConsumerGroupDetailResult:
    summary: ConsumerGroupSummaryResult
    members: list[ConsumerGroupMemberResult]
    assignments: list[ConsumerGroupAssignmentResult]
    offsets: list[ConsumerGroupOffsetResult]


@dataclass(frozen=True, slots=True)
class ConsumerGroupListResult:
    groups: list[ConsumerGroupSummaryResult]
    limit: int
    offset: int
    total: int


class AdminService:
    """Business rules for admin endpoints.

    Intentional Kafka-vs-REST differences:
    - Kafka CreateTopics supports batch creates and replication_factor; REST
      currently supports single-topic creation with fixed single-replica
      semantics only.
    - REST rejects internal-topic lifecycle mutations directly via input
      validation to make operator errors explicit.
    """

    @staticmethod
    def _validate_topic_name(name: str) -> str:
        topic_name = name.strip()
        if not topic_name:
            raise InvalidRequestError("topic name cannot be empty")
        if len(topic_name) > _MAX_TOPIC_NAME_LENGTH:
            raise InvalidRequestError(
                f"topic name must be <= {_MAX_TOPIC_NAME_LENGTH} characters"
            )
        if topic_name in {".", ".."}:
            raise InvalidRequestError("topic name cannot be '.' or '..'")
        if not _TOPIC_NAME_RE.fullmatch(topic_name):
            raise InvalidRequestError(
                "topic name can only contain letters, numbers, '.', '_' or '-'"
            )
        return topic_name

    @staticmethod
    def _validate_partition_count(num_partitions: int) -> int:
        if num_partitions <= 0:
            raise InvalidRequestError("num_partitions must be > 0")
        if num_partitions > _MAX_PARTITIONS:
            raise InvalidRequestError(
                f"num_partitions must be <= {_MAX_PARTITIONS}"
            )
        return num_partitions

    def _validate_pagination(self, *, limit: int, offset: int) -> tuple[int, int]:
        if limit <= 0:
            raise InvalidRequestError("limit must be > 0")
        if limit > 1000:
            raise InvalidRequestError("limit must be <= 1000")
        if offset < 0:
            raise InvalidRequestError("offset must be >= 0")
        return limit, offset

    @staticmethod
    def _validate_group_id(group_id: str) -> str:
        normalized = group_id.strip()
        if not normalized:
            raise InvalidRequestError("group_id cannot be empty")
        return normalized

    async def create_topic(
        self,
        session: AsyncSession,
        *,
        name: str,
        num_partitions: int,
        is_internal: bool = False,
    ) -> TopicSummaryResult:
        topic_name = self._validate_topic_name(name)
        partitions = self._validate_partition_count(num_partitions)

        if is_internal or topic_backend_for_name(topic_name).is_internal:
            raise InvalidRequestError("cannot create internal topic")

        try:
            async with session.begin():
                existing = (
                    await session.execute(select(Topic.id).where(Topic.name == topic_name))
                ).scalar_one_or_none()
                if existing is not None:
                    raise AlreadyExistsError("topic already exists")

                topic = Topic(name=topic_name, is_internal=False)
                session.add(topic)
                await session.flush()

                for idx in range(partitions):
                    session.add(
                        Partition(
                            topic_name=topic_name,
                            partition_number=idx,
                            last_offset=-1,
                            log_start_offset=0,
                        )
                    )
                    # Keep row-by-row for compatibility with mockgres/asyncpg.
                    await session.flush()

            return TopicSummaryResult(
                id=int(topic.id),
                name=topic_name,
                is_internal=False,
                partition_count=partitions,
            )
        except AlreadyExistsError:
            raise
        except IntegrityError as exc:
            raise AlreadyExistsError("topic already exists") from exc
        except InvalidRequestError:
            raise
        except SQLAlchemyError as exc:
            raise InternalError() from exc

    async def list_topics(
        self,
        session: AsyncSession,
        *,
        limit: int,
        offset: int,
    ) -> TopicListResult:
        list_limit, list_offset = self._validate_pagination(limit=limit, offset=offset)
        try:
            topics = (
                await session.execute(
                    select(Topic)
                    .order_by(Topic.name.asc())
                    .offset(list_offset)
                    .limit(list_limit)
                )
            ).scalars().all()

            topic_names = [topic.name for topic in topics]
            counts_by_topic: dict[str, int] = {}
            if topic_names:
                count_rows = (
                    await session.execute(
                        select(Partition.topic_name, func.count(Partition.id))
                        .where(Partition.topic_name.in_(topic_names))
                        .group_by(Partition.topic_name)
                    )
                ).all()
                counts_by_topic = {
                    str(topic_name): int(partition_count)
                    for topic_name, partition_count in count_rows
                }

            total = (
                await session.execute(select(func.count(Topic.id)))
            ).scalar_one()

            return TopicListResult(
                topics=[
                    TopicSummaryResult(
                        id=int(topic.id),
                        name=str(topic.name),
                        is_internal=bool(topic.is_internal),
                        partition_count=counts_by_topic.get(str(topic.name), 0),
                    )
                    for topic in topics
                ],
                limit=list_limit,
                offset=list_offset,
                total=int(total),
            )
        except InvalidRequestError:
            raise
        except SQLAlchemyError as exc:
            raise InternalError() from exc

    async def get_topic(
        self,
        session: AsyncSession,
        *,
        name: str,
    ) -> TopicDetailResult:
        topic_name = self._validate_topic_name(name)
        try:
            topic = (
                await session.execute(select(Topic).where(Topic.name == topic_name))
            ).scalar_one_or_none()
            if topic is None:
                raise NotFoundError("topic not found")

            partitions = (
                await session.execute(
                    select(Partition)
                    .where(Partition.topic_name == topic_name)
                    .order_by(Partition.partition_number.asc())
                )
            ).scalars().all()

            return TopicDetailResult(
                id=int(topic.id),
                name=str(topic.name),
                is_internal=bool(topic.is_internal),
                partitions=[
                    TopicPartitionResult(
                        partition_number=int(partition.partition_number),
                        log_start_offset=int(partition.log_start_offset),
                        last_offset=int(partition.last_offset),
                    )
                    for partition in partitions
                ],
                created_at=topic.created_at,
                updated_at=topic.updated_at,
            )
        except (InvalidRequestError, NotFoundError):
            raise
        except SQLAlchemyError as exc:
            raise InternalError() from exc

    async def delete_topic(
        self,
        session: AsyncSession,
        *,
        name: str,
    ) -> None:
        topic_name = self._validate_topic_name(name)
        if topic_backend_for_name(topic_name).is_internal:
            raise InvalidRequestError("cannot delete internal topic")

        try:
            topic = (
                await session.execute(select(Topic).where(Topic.name == topic_name))
            ).scalar_one_or_none()
            if topic is None:
                raise NotFoundError("topic not found")

            if bool(topic.is_internal):
                raise InvalidRequestError("cannot delete internal topic")

            delete_result = await session.execute(delete(Topic).where(Topic.name == topic_name))
            if int(delete_result.rowcount or 0) == 0:
                await session.rollback()
                raise NotFoundError("topic not found")
            await session.commit()
        except (InvalidRequestError, NotFoundError):
            raise
        except SQLAlchemyError as exc:
            await session.rollback()
            raise InternalError() from exc

    async def ping_database(self, session: AsyncSession) -> bool:
        try:
            await session.execute(select(1))
            return True
        except SQLAlchemyError:
            return False

    async def get_topic_offsets(
        self,
        session: AsyncSession,
        *,
        name: str,
    ) -> TopicOffsetsResult:
        topic_name = self._validate_topic_name(name)
        try:
            topic = (
                await session.execute(select(Topic.id).where(Topic.name == topic_name))
            ).scalar_one_or_none()
            if topic is None:
                raise NotFoundError("topic not found")

            partitions = (
                await session.execute(
                    select(Partition)
                    .where(Partition.topic_name == topic_name)
                    .order_by(Partition.partition_number.asc())
                )
            ).scalars().all()

            wal_rows = (
                await session.execute(
                    select(
                        WALFileOffset.partition_number,
                        func.count(WALFileOffset.id),
                        func.min(WALFileOffset.base_offset),
                        func.max(WALFileOffset.last_offset),
                    )
                    .join(WALFile, WALFile.id == WALFileOffset.wal_file_id)
                    .where(
                        WALFileOffset.topic_name == topic_name,
                        WALFile.compacted_at.is_(None),
                    )
                    .group_by(WALFileOffset.partition_number)
                )
            ).all()
            wal_map = {
                int(partition_number): (
                    int(segment_count),
                    int(min_offset) if min_offset is not None else None,
                    int(max_offset) if max_offset is not None else None,
                )
                for partition_number, segment_count, min_offset, max_offset in wal_rows
            }

            topic_wal_rows = (
                await session.execute(
                    select(
                        TopicWALFileOffset.partition_number,
                        func.count(TopicWALFileOffset.id),
                        func.min(TopicWALFileOffset.base_offset),
                        func.max(TopicWALFileOffset.last_offset),
                    )
                    .join(
                        TopicWALFile,
                        TopicWALFile.id == TopicWALFileOffset.topic_wal_file_id,
                    )
                    .where(
                        TopicWALFileOffset.topic_name == topic_name,
                        TopicWALFile.compacted_at.is_(None),
                    )
                    .group_by(TopicWALFileOffset.partition_number)
                )
            ).all()
            topic_wal_map = {
                int(partition_number): (
                    int(segment_count),
                    int(min_offset) if min_offset is not None else None,
                    int(max_offset) if max_offset is not None else None,
                )
                for partition_number, segment_count, min_offset, max_offset in topic_wal_rows
            }

            return TopicOffsetsResult(
                topic_name=topic_name,
                partitions=[
                    TopicPartitionOffsetVisibilityResult(
                        partition_number=int(partition.partition_number),
                        log_start_offset=int(partition.log_start_offset),
                        latest_offset=int(partition.last_offset),
                        wal_uncompacted_segment_count=wal_map.get(
                            int(partition.partition_number), (0, None, None)
                        )[0],
                        wal_uncompacted_min_offset=wal_map.get(
                            int(partition.partition_number), (0, None, None)
                        )[1],
                        wal_uncompacted_max_offset=wal_map.get(
                            int(partition.partition_number), (0, None, None)
                        )[2],
                        topic_wal_segment_count=topic_wal_map.get(
                            int(partition.partition_number), (0, None, None)
                        )[0],
                        topic_wal_min_offset=topic_wal_map.get(
                            int(partition.partition_number), (0, None, None)
                        )[1],
                        topic_wal_max_offset=topic_wal_map.get(
                            int(partition.partition_number), (0, None, None)
                        )[2],
                    )
                    for partition in partitions
                ],
            )
        except (InvalidRequestError, NotFoundError):
            raise
        except SQLAlchemyError as exc:
            raise InternalError() from exc

    async def list_consumer_groups(
        self,
        session: AsyncSession,
        *,
        limit: int,
        offset: int,
    ) -> ConsumerGroupListResult:
        list_limit, list_offset = self._validate_pagination(limit=limit, offset=offset)
        try:
            groups = (
                await session.execute(
                    select(ConsumerGroup)
                    .order_by(ConsumerGroup.group_id.asc())
                    .offset(list_offset)
                    .limit(list_limit)
                )
            ).scalars().all()

            group_ids = [int(group.id) for group in groups]
            member_counts: dict[int, int] = {}
            if group_ids:
                member_count_rows = (
                    await session.execute(
                        select(GroupMember.consumer_group_id, func.count(GroupMember.id))
                        .where(GroupMember.consumer_group_id.in_(group_ids))
                        .group_by(GroupMember.consumer_group_id)
                    )
                ).all()
                member_counts = {
                    int(group_id): int(member_count)
                    for group_id, member_count in member_count_rows
                }

            total = (
                await session.execute(select(func.count(ConsumerGroup.id)))
            ).scalar_one()

            return ConsumerGroupListResult(
                groups=[
                    ConsumerGroupSummaryResult(
                        group_id=str(group.group_id),
                        state=str(group.state),
                        generation=int(group.generation),
                        protocol_type=group.protocol_type,
                        selected_protocol=group.selected_protocol,
                        member_count=member_counts.get(int(group.id), 0),
                        updated_at=group.updated_at,
                    )
                    for group in groups
                ],
                limit=list_limit,
                offset=list_offset,
                total=int(total),
            )
        except InvalidRequestError:
            raise
        except SQLAlchemyError as exc:
            raise InternalError() from exc

    async def describe_consumer_group(
        self,
        session: AsyncSession,
        *,
        group_id: str,
    ) -> ConsumerGroupDetailResult:
        normalized_group_id = self._validate_group_id(group_id)
        try:
            group = (
                await session.execute(
                    select(ConsumerGroup).where(ConsumerGroup.group_id == normalized_group_id)
                )
            ).scalar_one_or_none()
            if group is None:
                raise NotFoundError("consumer group not found")

            members = (
                await session.execute(
                    select(GroupMember)
                    .where(GroupMember.consumer_group_id == group.id)
                    .order_by(GroupMember.member_id.asc())
                )
            ).scalars().all()
            member_id_lookup = {int(member.id): str(member.member_id) for member in members}

            assignments = (
                await session.execute(
                    select(GroupAssignment)
                    .where(
                        GroupAssignment.consumer_group_id == group.id,
                        GroupAssignment.generation == group.generation,
                    )
                    .order_by(
                        GroupAssignment.topic_name.asc(),
                        GroupAssignment.partition_number.asc(),
                    )
                )
            ).scalars().all()
            offsets = (
                await session.execute(
                    select(GroupOffset)
                    .where(GroupOffset.consumer_group_id == group.id)
                    .order_by(GroupOffset.topic_name.asc(), GroupOffset.partition_number.asc())
                )
            ).scalars().all()

            summary = ConsumerGroupSummaryResult(
                group_id=str(group.group_id),
                state=str(group.state),
                generation=int(group.generation),
                protocol_type=group.protocol_type,
                selected_protocol=group.selected_protocol,
                member_count=len(members),
                updated_at=group.updated_at,
            )

            return ConsumerGroupDetailResult(
                summary=summary,
                members=[
                    ConsumerGroupMemberResult(
                        member_id=str(member.member_id),
                        group_instance_id=member.group_instance_id,
                        is_in_sync=bool(member.is_in_sync),
                        last_heartbeat_at=member.last_heartbeat_at,
                    )
                    for member in members
                ],
                assignments=[
                    ConsumerGroupAssignmentResult(
                        member_id=member_id_lookup.get(int(assignment.group_member_id), ""),
                        topic_name=str(assignment.topic_name),
                        partition_number=int(assignment.partition_number),
                        generation=int(assignment.generation),
                    )
                    for assignment in assignments
                ],
                offsets=[
                    ConsumerGroupOffsetResult(
                        topic_name=str(offset.topic_name),
                        partition_number=int(offset.partition_number),
                        committed_offset=int(offset.committed_offset),
                        committed_metadata=offset.committed_metadata,
                        commit_timestamp=offset.commit_timestamp,
                        committed_member_id=offset.committed_member_id,
                        committed_generation=int(offset.committed_generation),
                    )
                    for offset in offsets
                ],
            )
        except (InvalidRequestError, NotFoundError):
            raise
        except SQLAlchemyError as exc:
            raise InternalError() from exc
