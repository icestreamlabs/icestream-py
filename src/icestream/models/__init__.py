from datetime import datetime
from typing import Optional

from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    Boolean,
    ForeignKey,
    ForeignKeyConstraint,
    Identity,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
    and_,
    select,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class IntIdMixin:
    id: Mapped[int] = mapped_column(Integer, Identity(always=True), primary_key=True)


class BigIntIdMixin:
    id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(always=True),
        primary_key=True,
    )


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=text("CURRENT_TIMESTAMP"),
        nullable=False,
    )

    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=text("CURRENT_TIMESTAMP"),
        onupdate=text("CURRENT_TIMESTAMP"),
        nullable=False,
    )


class Topic(Base, IntIdMixin, TimestampMixin):
    __tablename__ = "topics"

    name: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    schema: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_internal: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=text("false"),
    )

    partitions: Mapped[list["Partition"]] = relationship(back_populates="topic")


class Partition(Base, IntIdMixin, TimestampMixin):
    __tablename__ = "partitions"

    topic_name: Mapped[str] = mapped_column(
        String,
        ForeignKey("topics.name", ondelete="CASCADE"),
        nullable=False,
    )

    partition_number: Mapped[int] = mapped_column(Integer, nullable=False)
    last_offset: Mapped[int] = mapped_column(BigInteger, nullable=False, default=-1)
    log_start_offset: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)

    topic: Mapped["Topic"] = relationship(back_populates="partitions")

    wal_file_offsets: Mapped[list["WALFileOffset"]] = relationship(
        back_populates="partition",
        primaryjoin=(
            "and_("
            "WALFileOffset.topic_name == Partition.topic_name, "
            "WALFileOffset.partition_number == Partition.partition_number"
            ")"
        ),
        foreign_keys="[WALFileOffset.topic_name, WALFileOffset.partition_number]",
    )

    __table_args__ = (
        UniqueConstraint("topic_name", "partition_number"),
        Index("ix_partition_topic_partition", "topic_name", "partition_number"),
    )


class WALFile(Base, BigIntIdMixin, TimestampMixin):
    __tablename__ = "wal_files"

    uri: Mapped[str] = mapped_column(Text, nullable=False)
    etag: Mapped[Optional[str]] = mapped_column(Text)
    total_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    total_messages: Mapped[int] = mapped_column(Integer, nullable=False)

    compacted_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True))

    wal_file_offsets: Mapped[list["WALFileOffset"]] = relationship(
        back_populates="wal_file"
    )


class WALFileOffset(Base, BigIntIdMixin):
    __tablename__ = "wal_file_offsets"

    wal_file_id: Mapped[int] = mapped_column(
        ForeignKey("wal_files.id", ondelete="CASCADE"), nullable=False
    )

    topic_name: Mapped[str] = mapped_column(nullable=False)
    partition_number: Mapped[int] = mapped_column(nullable=False)

    base_offset: Mapped[int] = mapped_column(BigInteger, nullable=False)
    last_offset: Mapped[int] = mapped_column(BigInteger, nullable=False)

    byte_start: Mapped[int] = mapped_column(BigInteger, nullable=False)
    byte_end: Mapped[int] = mapped_column(BigInteger, nullable=False)

    min_timestamp: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    max_timestamp: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)

    wal_file: Mapped["WALFile"] = relationship(back_populates="wal_file_offsets")

    partition: Mapped["Partition"] = relationship(
        back_populates="wal_file_offsets",
        primaryjoin=and_(
            topic_name == Partition.topic_name,
            partition_number == Partition.partition_number,
        ),
        foreign_keys=[topic_name, partition_number],
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["topic_name", "partition_number"],
            ["partitions.topic_name", "partitions.partition_number"],
            ondelete="CASCADE",
        ),
        Index("ix_wal_file_offset_topic_partition", "topic_name", "partition_number"),
        Index(
            "ix_wal_file_offset_topic_part_ts",
            "topic_name",
            "partition_number",
            "min_timestamp",
            "max_timestamp",
        ),
    )


class ParquetFile(Base, BigIntIdMixin, TimestampMixin):
    __tablename__ = "parquet_files"

    topic_name: Mapped[str] = mapped_column(
        String, ForeignKey("topics.name", ondelete="CASCADE"), nullable=False
    )
    partition_number: Mapped[int] = mapped_column(Integer, nullable=False)

    uri: Mapped[str] = mapped_column(Text, nullable=False)

    total_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    row_count: Mapped[int] = mapped_column(BigInteger, nullable=False)

    min_offset: Mapped[int] = mapped_column(BigInteger, nullable=False)
    max_offset: Mapped[int] = mapped_column(BigInteger, nullable=False)

    min_timestamp: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    max_timestamp: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))

    generation: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    compacted_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))

    partition: Mapped["Partition"] = relationship(
        primaryjoin=and_(
            topic_name == Partition.topic_name,
            partition_number == Partition.partition_number,
        ),
        foreign_keys=[topic_name, partition_number],
        viewonly=True,
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["topic_name", "partition_number"],
            ["partitions.topic_name", "partitions.partition_number"],
            ondelete="CASCADE",
        ),
        UniqueConstraint("uri"),
        Index("ix_pf_topic_partition", "topic_name", "partition_number"),
        Index(
            "ix_pf_topic_partition_min", "topic_name", "partition_number", "min_offset"
        ),
        Index(
            "ix_pf_topic_partition_max", "topic_name", "partition_number", "max_offset"
        ),
    )


class ParquetFileSource(Base, BigIntIdMixin, TimestampMixin):
    __tablename__ = "parquet_file_sources"
    parquet_file_id: Mapped[int] = mapped_column(
        ForeignKey("parquet_files.id", ondelete="CASCADE"), nullable=False
    )
    wal_file_id: Mapped[int] = mapped_column(
        ForeignKey("wal_files.id", ondelete="CASCADE"), nullable=False
    )
    __table_args__ = (
        UniqueConstraint("parquet_file_id", "wal_file_id"),
        Index("ix_pfs_pf", "parquet_file_id"),
        Index("ix_pfs_wf", "wal_file_id"),
    )


class ParquetFileParent(Base, BigIntIdMixin, TimestampMixin):
    __tablename__ = "parquet_file_parents"
    child_parquet_file_id: Mapped[int] = mapped_column(
        ForeignKey("parquet_files.id", ondelete="CASCADE"), nullable=False
    )
    parent_parquet_file_id: Mapped[int] = mapped_column(
        ForeignKey("parquet_files.id", ondelete="CASCADE"), nullable=False
    )
    __table_args__ = (
        UniqueConstraint("child_parquet_file_id", "parent_parquet_file_id"),
        Index("ix_pfp_child", "child_parquet_file_id"),
        Index("ix_pfp_parent", "parent_parquet_file_id"),
    )


async def assert_no_overlap(
    session, topic: str, partition: int, min_off: int, max_off: int
) -> None:
    q = (
        select(ParquetFile.id)
        .where(
            ParquetFile.topic_name == topic,
            ParquetFile.partition_number == partition,
            ParquetFile.compacted_at.is_(None),
            ParquetFile.min_offset <= max_off,
            ParquetFile.max_offset >= min_off,
        )
        .limit(1)
    )
    if (await session.execute(q)).scalar() is not None:
        raise ValueError(
            f"Overlapping Parquet range for {topic}[{partition}] {min_off}-{max_off}"
        )
