from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    Boolean,
    CheckConstraint,
    ForeignKey,
    Identity,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    LargeBinary,
    and_,
    text,
    ForeignKeyConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from icestream.models import Base, IntIdMixin, TimestampMixin, BigIntIdMixin


class ConsumerGroup(Base, IntIdMixin, TimestampMixin):
    __tablename__ = "consumer_groups"

    group_id: Mapped[str] = mapped_column(String, unique=True, nullable=False)

    protocol_type: Mapped[Optional[str]] = mapped_column(String)
    selected_protocol: Mapped[Optional[str]] = mapped_column(String)

    generation: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    state: Mapped[str] = mapped_column(String, nullable=False, default="Empty")
    leader_member_id: Mapped[Optional[str]] = mapped_column(String)
    state_version: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)

    session_timeout_ms: Mapped[int] = mapped_column(Integer, nullable=False, default=10000)
    rebalance_timeout_ms: Mapped[int] = mapped_column(Integer, nullable=False, default=30000)

    # active while preparingrebalance
    join_phase_deadline_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True))
    # active while completingrebalance
    sync_phase_deadline_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True))

    members: Mapped[list["GroupMember"]] = relationship(back_populates="group", cascade="all, delete-orphan")
    assignments: Mapped[list["GroupAssignment"]] = relationship(back_populates="group", cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_consumer_groups_gid", "group_id"),
        CheckConstraint(
            "state IN ('Empty', 'PreparingRebalance', 'CompletingRebalance', 'Stable')",
            name="ck_consumer_groups_state_valid",
        ),
    )


class GroupMember(Base, BigIntIdMixin, TimestampMixin):
    __tablename__ = "group_members"

    consumer_group_id: Mapped[int] = mapped_column(
        ForeignKey("consumer_groups.id", ondelete="CASCADE"),
        nullable=False,
    )

    member_id: Mapped[str] = mapped_column(String, nullable=False)
    group_instance_id: Mapped[Optional[str]] = mapped_column(String)

    session_timeout_ms: Mapped[int] = mapped_column(Integer, nullable=False, default=10000)
    rebalance_timeout_ms: Mapped[int] = mapped_column(Integer, nullable=False, default=30000)
    last_heartbeat_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=text("CURRENT_TIMESTAMP"), nullable=False
    )

    member_generation: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    is_in_sync: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # the generation this member intends to join (set during preparingrebalance)
    join_generation: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # raw assignment blob returned at syncgroup
    assignment: Mapped[Optional[bytes]] = mapped_column(LargeBinary)

    # quick-change detector (hash over protocol rows: name + metadata)
    protocols_hash: Mapped[Optional[str]] = mapped_column(String)

    group: Mapped["ConsumerGroup"] = relationship(back_populates="members")
    protocols: Mapped[list["GroupMemberProtocol"]] = relationship(
        back_populates="member", cascade="all, delete-orphan"
    )

    __table_args__ = (
        UniqueConstraint("consumer_group_id", "member_id"),
        Index(
            "uq_group_members_gid_instance",
            "consumer_group_id",
            "group_instance_id",
            unique=True,
            postgresql_where=text("group_instance_id IS NOT NULL"),
        ),
        Index("ix_group_members_gid_member", "consumer_group_id", "member_id"),
        Index("ix_group_members_gid_join", "consumer_group_id", "join_generation"),
    )


class GroupMemberProtocol(Base, BigIntIdMixin, TimestampMixin):
    __tablename__ = "group_member_protocols"

    group_member_id: Mapped[int] = mapped_column(
        ForeignKey("group_members.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String, nullable=False)
    protocol_metadata: Mapped[bytes] = mapped_column("metadata", LargeBinary, nullable=False)
    preference_rank: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    member: Mapped["GroupMember"] = relationship(back_populates="protocols")

    __table_args__ = (
        UniqueConstraint("group_member_id", "name"),
        Index("ix_gmp_member", "group_member_id"),
        Index("ix_gmp_member_name", "group_member_id", "name"),
    )


class GroupAssignment(Base, BigIntIdMixin, TimestampMixin):
    __tablename__ = "group_assignments"

    consumer_group_id: Mapped[int] = mapped_column(
        ForeignKey("consumer_groups.id", ondelete="CASCADE"), nullable=False
    )
    generation: Mapped[int] = mapped_column(Integer, nullable=False)

    topic_name: Mapped[str] = mapped_column(String, nullable=False)
    partition_number: Mapped[int] = mapped_column(Integer, nullable=False)

    group_member_id: Mapped[int] = mapped_column(
        ForeignKey("group_members.id", ondelete="CASCADE"), nullable=False
    )

    group: Mapped["ConsumerGroup"] = relationship(back_populates="assignments")
    member: Mapped["GroupMember"] = relationship()

    __table_args__ = (
        ForeignKeyConstraint(
            ["topic_name", "partition_number"],
            ["partitions.topic_name", "partitions.partition_number"],
            ondelete="CASCADE",
        ),
        UniqueConstraint(
            "consumer_group_id", "generation", "topic_name", "partition_number",
            name="uq_group_assignment_gid_gen_tp",
        ),
        Index("ix_group_assignments_gid_gen", "consumer_group_id", "generation"),
        Index("ix_group_assignments_member", "group_member_id"),
    )


class GroupOffset(Base, BigIntIdMixin, TimestampMixin):
    __tablename__ = "group_offsets"

    consumer_group_id: Mapped[int] = mapped_column(
        ForeignKey("consumer_groups.id", ondelete="CASCADE"),
        nullable=False,
    )
    topic_name: Mapped[str] = mapped_column(String, nullable=False)
    partition_number: Mapped[int] = mapped_column(Integer, nullable=False)

    committed_offset: Mapped[int] = mapped_column(BigInteger, nullable=False, default=-1)
    committed_metadata: Mapped[Optional[str]] = mapped_column(Text)
    commit_timestamp: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), server_default=text("CURRENT_TIMESTAMP"))

    committed_generation: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    committed_member_id: Mapped[Optional[str]] = mapped_column(String)

    __table_args__ = (
        ForeignKeyConstraint(
            ["topic_name", "partition_number"],
            ["partitions.topic_name", "partitions.partition_number"],
            ondelete="CASCADE",
        ),
        UniqueConstraint("consumer_group_id", "topic_name", "partition_number"),
        Index("ix_group_offsets_gid_tp", "consumer_group_id", "topic_name", "partition_number"),
    )


class GroupOffsetLog(Base):
    __tablename__ = "group_offset_log"

    topic_partition: Mapped[int] = mapped_column(Integer, primary_key=True)
    log_offset: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    key_bytes: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    value_bytes: Mapped[Optional[bytes]] = mapped_column(LargeBinary)
    commit_ts: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    )
