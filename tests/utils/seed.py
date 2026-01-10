import asyncio
import datetime
import io
from typing import List, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

from sqlalchemy.ext.asyncio import AsyncSession

from icestream.utils import normalize_object_key
from icestream.compaction.schema import PARQUET_RECORD_SCHEMA
from icestream.config import Config
from icestream.kafkaserver.protocol import KafkaRecord, KafkaRecordBatch
from icestream.kafkaserver.types import ProduceTopicPartitionData
from icestream.kafkaserver.wal.serde import encode_kafka_wal_file_with_offsets
from icestream.models import ParquetFile, WALFile, WALFileOffset


async def create_parquet_range(
    config: Config,
    session: AsyncSession,
    *,
    topic: str,
    partition: int,
    min_off: int,
    max_off: int,
    ts_start_ms: int | None,
    ts_step_ms: int = 1000,
    key_prefix: str | None = None,
) -> ParquetFile:
    rows: List[dict] = []
    for off in range(min_off, max_off + 1):
        t = None if ts_start_ms is None else ts_start_ms + (off - min_off) * ts_step_ms
        rows.append(
            {
                "partition": partition,
                "offset": off,
                "timestamp_ms": t,
                "key": None,
                "value": f"v-{off}".encode(),
                "headers": None,
            }
        )

    table = pa.Table.from_pylist(rows, schema=PARQUET_RECORD_SCHEMA)
    sink = io.BytesIO()
    pq.write_table(
        table,
        sink,
        compression="zstd",
        use_dictionary=True,
        write_statistics=True,
        row_group_size=max(1, 1024 // 8),  # small row groups to keep files tiny
    )
    data = sink.getvalue()

    key_prefix = key_prefix or config.PARQUET_PREFIX.rstrip("/")
    key = f"{key_prefix}/topics/{topic}/partition={partition}/{min_off}-{max_off}-gen0.parquet"

    await config.store.put_async(key, io.BytesIO(data))
    uri = normalize_object_key(config, key)

    min_ts = None if ts_start_ms is None else datetime.datetime.fromtimestamp(ts_start_ms / 1000, tz=datetime.UTC)
    max_ts = None if ts_start_ms is None else datetime.datetime.fromtimestamp(
        (ts_start_ms + (max_off - min_off) * ts_step_ms) / 1000, tz=datetime.UTC
    )

    pf = ParquetFile(
        topic_name=topic,
        partition_number=partition,
        uri=uri,
        total_bytes=len(data),
        row_count=table.num_rows,
        min_offset=min_off,
        max_offset=max_off,
        min_timestamp=min_ts,
        max_timestamp=max_ts,
        generation=0,
        compacted_at=None,
    )
    session.add(pf)
    await session.flush()
    return pf

async def create_wal_range(
    config: Config,
    session: AsyncSession,
    *,
    topic: str,
    partition: int,
    base_offset: int,
    count: int,
    ts_start_ms: int | None,
    ts_step_ms: int = 1000,
    compacted_at: datetime.datetime | None = None,
    key_prefix: str = "wal",
) -> Tuple[WALFile, WALFileOffset]:
    records: List[KafkaRecord] = []
    for i in range(count):
        records.append(
            KafkaRecord(
                attributes=0,
                timestamp_delta=(i * ts_step_ms) if ts_start_ms is not None else 0,
                offset_delta=i,
                key=None,
                value=f"wal-{base_offset + i}".encode(),
                headers=[],
            )
        )

    batch = KafkaRecordBatch.from_records(base_offset, records)

    if ts_start_ms is not None:
        batch.base_timestamp = ts_start_ms
        batch.max_timestamp  = ts_start_ms + (count - 1) * ts_step_ms
    else:
        batch.base_timestamp = 0
        batch.max_timestamp  = 0

    ptd = ProduceTopicPartitionData(topic=topic, partition=partition, kafka_record_batch=batch, flush_result=asyncio.Future())
    wal_bytes, meta = encode_kafka_wal_file_with_offsets([ptd], broker_id="test-broker-1")
    md = meta[0]
    last_offset = md["last_offset"]

    key = f"{key_prefix}/topics/{topic}/partition={partition}/{base_offset}-{last_offset}.wal"
    await config.store.put_async(key, io.BytesIO(wal_bytes))
    uri = normalize_object_key(config, key)

    wal = WALFile(
        uri=uri,
        etag=None,
        total_bytes=len(wal_bytes),
        total_messages=count,
        compacted_at=compacted_at,
    )
    session.add(wal)
    await session.flush()

    wfo = WALFileOffset(
        wal_file_id=wal.id,
        topic_name=topic,
        partition_number=partition,
        base_offset=base_offset,
        last_offset=last_offset,
        byte_start=0,
        byte_end=len(wal_bytes),
        min_timestamp=md["min_timestamp"],
        max_timestamp=md["max_timestamp"],
    )
    session.add(wfo)
    await session.flush()
    if ts_start_ms is not None and count > 0:
        expected_max_ts = ts_start_ms + (count - 1) * ts_step_ms
        assert batch.max_timestamp == expected_max_ts, \
            f"Calculated max_timestamp {batch.max_timestamp} does not match expected {expected_max_ts}"
    return wal, wfo
