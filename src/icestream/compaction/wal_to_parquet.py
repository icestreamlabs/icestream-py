import datetime
import io
from collections import defaultdict
from typing import Dict, List, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

from icestream.compaction import build_uri
from icestream.compaction.types import CompactionContext, CompactionProcessor
from icestream.compaction.schema import PARQUET_RECORD_SCHEMA
from icestream.kafkaserver.protocol import decode_kafka_records
from icestream.models import ParquetFile, ParquetFileSource, assert_no_overlap

PARQUET_RECORD_MAPPING = {
    "partition": pa.int32(),
    "offset": pa.int64(),
    "timestamp_ms": pa.int64(),
    "key": pa.binary(),
    "value": pa.binary(),
    "headers": pa.list_(
        pa.struct(
            [
                ("key", pa.string()),
                ("value", pa.binary()),
            ]
        )
    ),
}


class WalToParquetProcessor(CompactionProcessor):
    async def apply(self, ctx: CompactionContext) -> None:
        if not ctx.wal_decoded:
            return

        buckets = self.bucket_records(ctx)

        for (topic, partition), rows in buckets.items():
            if not rows:
                continue

            rows.sort(key=lambda r: r["offset"])
            await self._flush_buckets(ctx, topic, partition, rows)

    @staticmethod
    def _estimate_record_size(r: dict) -> int:
        k = len(r["key"]) if r["key"] else 0
        v = len(r["value"]) if r["value"] else 0
        h = sum(
            (len(hd["key"]) + (len(hd["value"]) if hd["value"] else 0))
            for hd in (r["headers"] or [])
        )
        return k + v + h + 64  # include small buffer

    @staticmethod
    def bucket_records(ctx: CompactionContext) -> Dict[Tuple[str, int], List[dict]]:
        buckets: Dict[Tuple[str, int], List[dict]] = defaultdict(list)

        for wf in ctx.wal_decoded:
            for batch in wf.batches:
                topic = batch.topic
                partition = batch.partition
                base = batch.kafka_record_batch.base_offset
                if base is None:
                    continue

                records = decode_kafka_records(batch.kafka_record_batch.records)
                for rec in records:
                    offset = base + rec.offset_delta
                    ts_ms = batch.kafka_record_batch.base_timestamp
                    if ts_ms is not None:
                        ts_ms = ts_ms + rec.timestamp_delta

                    buckets[(topic, partition)].append(
                        {
                            "partition": partition,
                            "offset": offset,
                            "timestamp_ms": ts_ms,
                            "key": rec.key,
                            "value": rec.value,
                            "headers": [
                                {"key": h.key, "value": h.value} for h in rec.headers
                            ]
                            or None,
                        }
                    )

        return buckets

    async def _flush_buckets(
        self, ctx: CompactionContext, topic: str, partition: int, rows: List[dict]
    ):
        target_bytes = ctx.config.PARQUET_TARGET_FILE_BYTES

        chunk: list[dict] = []
        approx = 0

        for r in rows:
            approx += self._estimate_record_size(r)
            chunk.append(r)
            if approx >= target_bytes:
                await self._flush_chunk(ctx, topic, partition, chunk)
                chunk, approx = [], 0

        if chunk:
            await self._flush_chunk(ctx, topic, partition, chunk)

    async def _flush_chunk(
        self, ctx: CompactionContext, topic: str, partition: int, chunk_rows: List[dict]
    ):
        if not chunk_rows:
            return

        # noinspection PyArgumentList
        table = pa.Table.from_pylist(chunk_rows, schema=PARQUET_RECORD_SCHEMA)

        # row group size estimation
        sample = chunk_rows[: min(1000, len(chunk_rows))]
        kvh = sum(self._estimate_record_size(r) for r in sample)
        avg_row = max(64, kvh // max(1, len(sample)))
        rows_per_rg = max(1, ctx.config.PARQUET_ROW_GROUP_TARGET_BYTES // avg_row)

        sink = io.BytesIO()
        pq.write_table(
            table,
            sink,
            compression="zstd",
            use_dictionary=True,
            write_statistics=True,
            row_group_size=rows_per_rg,
        )
        data = sink.getvalue()
        total_bytes = len(data)
        min_off = chunk_rows[0]["offset"]
        max_off = chunk_rows[-1]["offset"]

        ts_vals = [
            r["timestamp_ms"] for r in chunk_rows if r["timestamp_ms"] is not None
        ]
        min_ts = (
            datetime.datetime.fromtimestamp(min(ts_vals) / 1000, tz=datetime.UTC)
            if ts_vals
            else None
        )
        max_ts = (
            datetime.datetime.fromtimestamp(max(ts_vals) / 1000, tz=datetime.UTC)
            if ts_vals
            else None
        )

        key = (
            ctx.config.PARQUET_PREFIX.rstrip("/")
            + f"/topics/{topic}/partition={partition}/{min_off}-{max_off}-gen0.parquet"
        )
        await ctx.config.store.put_async(key, io.BytesIO(data))

        async with ctx.config.async_session_factory() as session:
            await assert_no_overlap(session, topic, partition, min_off, max_off)
            pf = ParquetFile(
                topic_name=topic,
                partition_number=partition,
                uri=build_uri(ctx.config, key),
                total_bytes=total_bytes,
                row_count=table.num_rows,
                min_offset=min_off,
                max_offset=max_off,
                min_timestamp=min_ts,
                max_timestamp=max_ts,
                generation=0,
            )
            session.add(pf)
            await session.flush()

            for wm in ctx.wal_models:
                session.add(ParquetFileSource(parquet_file_id=pf.id, wal_file_id=wm.id))
