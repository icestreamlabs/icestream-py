from __future__ import annotations

import datetime as dt
import struct
from dataclasses import dataclass
from typing import Sequence

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from icestream.config import Config
from icestream.kafkaserver.internal_topics import (
    INTERNAL_OFFSETS_TOPIC,
    internal_topics,
    partition_for_key,
)
from icestream.models import Partition
from icestream.models.consumer_groups import GroupOffsetLog

OFFSET_KEY_VERSION = 1
OFFSET_VALUE_VERSION = 1
OFFSET_VALUE_EXPIRE_TIMESTAMP = -1


@dataclass(frozen=True, slots=True)
class OffsetLogEntry:
    key_bytes: bytes
    value_bytes: bytes | None


def _encode_int16(value: int) -> bytes:
    return struct.pack(">h", int(value))


def _encode_int32(value: int) -> bytes:
    return struct.pack(">i", int(value))


def _encode_int64(value: int) -> bytes:
    return struct.pack(">q", int(value))


def _encode_string(value: str | None) -> bytes:
    if value is None:
        return _encode_int16(-1)
    raw = value.encode("utf-8")
    return _encode_int16(len(raw)) + raw


def encode_offset_key(group_id: str, topic: str, partition: int) -> bytes:
    return b"".join(
        (
            _encode_int16(OFFSET_KEY_VERSION),
            _encode_string(group_id),
            _encode_string(topic),
            _encode_int32(partition),
        )
    )


def encode_offset_value(
    *,
    committed_offset: int,
    committed_metadata: str | None,
    commit_timestamp_ms: int,
) -> bytes:
    return b"".join(
        (
            _encode_int16(OFFSET_VALUE_VERSION),
            _encode_int64(committed_offset),
            _encode_string(committed_metadata),
            _encode_int64(commit_timestamp_ms),
            _encode_int64(OFFSET_VALUE_EXPIRE_TIMESTAMP),
        )
    )


def commit_timestamp_ms(ts: dt.datetime | None) -> int:
    if ts is None:
        return -1
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    return int(ts.timestamp() * 1000)


def internal_offsets_partitions(config: Config) -> int:
    spec = next((s for s in internal_topics(config) if s.name == INTERNAL_OFFSETS_TOPIC), None)
    if spec is None:
        raise ValueError("internal offsets topic not configured")
    return int(spec.partitions)


async def allocate_internal_log_offset(
    session: AsyncSession,
    *,
    partition: int,
) -> int:
    result = await session.execute(
        update(Partition)
        .where(
            Partition.topic_name == INTERNAL_OFFSETS_TOPIC,
            Partition.partition_number == partition,
        )
        .values(last_offset=Partition.last_offset + 1)
        .returning(Partition.last_offset)
    )
    row = result.first()
    if row is None:
        raise ValueError("internal offsets partition missing")
    return int(row[0])


async def append_offset_log_entries(
    session: AsyncSession,
    *,
    config: Config,
    entries: Sequence[OffsetLogEntry],
    commit_ts: dt.datetime,
) -> list[GroupOffsetLog]:
    if not entries:
        return []

    partition_count = internal_offsets_partitions(config)
    rows: list[GroupOffsetLog] = []
    for entry in entries:
        partition = partition_for_key(entry.key_bytes, partition_count)
        log_offset = await allocate_internal_log_offset(session, partition=partition)
        rows.append(
            GroupOffsetLog(
                topic_partition=partition,
                log_offset=log_offset,
                key_bytes=entry.key_bytes,
                value_bytes=entry.value_bytes,
                commit_ts=commit_ts,
            )
        )
    session.add_all(rows)
    return rows
