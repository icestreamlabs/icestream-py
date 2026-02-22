from __future__ import annotations

import io
from dataclasses import dataclass
from typing import Any

from icestream.compaction.types import (
    CompactionContext,
    CompactionProcessor,
    CompactionWritePlan,
    TopicWALFilePlan,
    TopicWALOffsetPlan,
)
from icestream.kafkaserver.protocol import KafkaRecordBatch
from icestream.kafkaserver.wal.serde import encode_kafka_wal_file_with_offsets
from icestream.utils import normalize_object_key, escape_topic_key_component


@dataclass
class _BatchPlan:
    topic: str
    partition: int
    kafka_record_batch: KafkaRecordBatch
    source_wal_id: int | None


class WalToTopicWalProcessor(CompactionProcessor):
    async def build_plan(self, ctx: CompactionContext) -> CompactionWritePlan:
        plan = CompactionWritePlan()

        topic_batches = self._group_batches_by_topic(ctx)
        if not topic_batches:
            return plan

        prefix = getattr(ctx.config, "TOPIC_WAL_PREFIX", "topic_wal").rstrip("/")
        target_bytes = int(
            getattr(ctx.config, "TOPIC_WAL_TARGET_BYTES", 256 * 1024 * 1024)
        )
        broker_id = getattr(ctx.config, "BROKER_ID", "unknown")

        for topic in sorted(topic_batches):
            partition_batches = topic_batches[topic]
            self._validate_partition_offsets(topic, partition_batches)
            chunks = self._chunk_batches(partition_batches, target_bytes)

            for chunk_index, chunk in enumerate(chunks):
                payload, meta = encode_kafka_wal_file_with_offsets(chunk, broker_id)
                by_partition = self._partition_meta(meta)
                source_wal_ids = sorted(
                    {
                        item.source_wal_id
                        for item in chunk
                        if item.source_wal_id is not None
                    }
                )

                key = self._build_object_key(
                    prefix=prefix,
                    topic=topic,
                    chunk_index=chunk_index,
                    by_partition=by_partition,
                    source_wal_ids=source_wal_ids,
                )
                put_result = await ctx.config.store.put_async(key, io.BytesIO(payload))
                etag = self._extract_etag(put_result)

                offsets: list[TopicWALOffsetPlan] = []
                for partition in sorted(by_partition):
                    part_meta = by_partition[partition]
                    offsets.append(
                        TopicWALOffsetPlan(
                            topic_name=topic,
                            partition_number=partition,
                            base_offset=int(part_meta["base_offset"]),
                            last_offset=int(part_meta["last_offset"]),
                            byte_start=int(part_meta["byte_start"]),
                            byte_end=int(part_meta["byte_end"]),
                            min_timestamp=self._to_int_or_none(part_meta["min_timestamp"]),
                            max_timestamp=self._to_int_or_none(part_meta["max_timestamp"]),
                        )
                    )

                total_messages = sum(
                    (off.last_offset - off.base_offset + 1) for off in offsets
                )
                plan.topic_wal_files.append(
                    TopicWALFilePlan(
                        topic_name=topic,
                        uri=normalize_object_key(ctx.config, key),
                        etag=etag,
                        total_bytes=len(payload),
                        total_messages=total_messages,
                        offsets=offsets,
                        source_wal_ids=source_wal_ids,
                    )
                )
                plan.uploaded_object_keys.append(key)
                plan.compacted_wal_ids.update(source_wal_ids)

        return plan

    @staticmethod
    def _group_batches_by_topic(
        ctx: CompactionContext,
    ) -> dict[str, dict[int, list[_BatchPlan]]]:
        grouped: dict[str, dict[int, list[_BatchPlan]]] = {}

        for decoded_wal in ctx.wal_decoded:
            source_id = getattr(decoded_wal, "id", None)
            for batch in decoded_wal.batches:
                topic_map = grouped.setdefault(batch.topic, {})
                topic_map.setdefault(batch.partition, []).append(
                    _BatchPlan(
                        topic=batch.topic,
                        partition=batch.partition,
                        kafka_record_batch=batch.kafka_record_batch,
                        source_wal_id=source_id,
                    )
                )

        for partition_map in grouped.values():
            for items in partition_map.values():
                items.sort(
                    key=lambda b: (
                        int(b.kafka_record_batch.base_offset or 0),
                        int(b.kafka_record_batch.last_offset_delta or 0),
                    )
                )

        return grouped

    @staticmethod
    def _validate_partition_offsets(
        topic: str, partition_batches: dict[int, list[_BatchPlan]]
    ) -> None:
        for partition, batches in partition_batches.items():
            prev_last: int | None = None
            for item in batches:
                base = item.kafka_record_batch.base_offset
                lod = item.kafka_record_batch.last_offset_delta
                if base is None or lod is None:
                    continue
                last = int(base) + int(lod)
                if prev_last is not None and int(base) <= prev_last:
                    raise ValueError(
                        f"overlapping offsets for {topic}[{partition}] "
                        f"{base}-{last} (previous ends at {prev_last})"
                    )
                prev_last = last

    @staticmethod
    def _chunk_batches(
        partition_batches: dict[int, list[_BatchPlan]], target_bytes: int
    ) -> list[list[_BatchPlan]]:
        if target_bytes <= 0:
            target_bytes = 1

        chunks: list[list[_BatchPlan]] = []
        chunk: list[_BatchPlan] = []
        chunk_bytes = 0

        for partition in sorted(partition_batches):
            for item in partition_batches[partition]:
                est = WalToTopicWalProcessor._estimate_batch_bytes(item)
                if chunk and chunk_bytes + est > target_bytes:
                    chunks.append(chunk)
                    chunk = []
                    chunk_bytes = 0
                chunk.append(item)
                chunk_bytes += est

        if chunk:
            chunks.append(chunk)
        return chunks

    @staticmethod
    def _estimate_batch_bytes(item: _BatchPlan) -> int:
        rb = item.kafka_record_batch.to_bytes()
        topic_len = len(item.topic.encode("utf-8"))
        return len(rb) + topic_len + 20

    @staticmethod
    def _partition_meta(meta: list[dict[str, Any]]) -> dict[int, dict[str, int | None]]:
        out: dict[int, dict[str, int | None]] = {}
        for item in meta:
            partition = int(item["partition"])
            base_offset = int(item["base_offset"])
            last_offset = int(item["last_offset"])
            byte_start = int(item["byte_start"])
            byte_end = int(item["byte_end"])
            min_ts = WalToTopicWalProcessor._to_int_or_none(item.get("min_timestamp"))
            max_ts = WalToTopicWalProcessor._to_int_or_none(item.get("max_timestamp"))

            if partition not in out:
                out[partition] = {
                    "base_offset": base_offset,
                    "last_offset": last_offset,
                    "byte_start": byte_start,
                    "byte_end": byte_end,
                    "min_timestamp": min_ts,
                    "max_timestamp": max_ts,
                }
                continue

            cur = out[partition]
            if base_offset <= int(cur["last_offset"]):
                raise ValueError(
                    f"overlapping encoded offset spans for partition {partition}: "
                    f"{base_offset}-{last_offset} overlaps {cur['base_offset']}-{cur['last_offset']}"
                )

            cur["last_offset"] = last_offset
            cur["byte_end"] = byte_end
            if min_ts is not None and (
                cur["min_timestamp"] is None or min_ts < int(cur["min_timestamp"])
            ):
                cur["min_timestamp"] = min_ts
            if max_ts is not None and (
                cur["max_timestamp"] is None or max_ts > int(cur["max_timestamp"])
            ):
                cur["max_timestamp"] = max_ts

        return out

    @staticmethod
    def _build_object_key(
        *,
        prefix: str,
        topic: str,
        chunk_index: int,
        by_partition: dict[int, dict[str, int | None]],
        source_wal_ids: list[int],
    ) -> str:
        partitions = sorted(by_partition)
        min_offset = min(int(by_partition[p]["base_offset"]) for p in partitions)
        max_offset = max(int(by_partition[p]["last_offset"]) for p in partitions)
        partition_sig = "-".join(str(p) for p in partitions)
        topic_key = escape_topic_key_component(topic)

        if source_wal_ids:
            source_sig = f"{source_wal_ids[0]}-{source_wal_ids[-1]}"
        else:
            source_sig = "none"

        return (
            f"{prefix}/topics/{topic_key}/segments/"
            f"{min_offset}-{max_offset}-p{partition_sig}-src{source_sig}-chunk{chunk_index}.wal"
        )

    @staticmethod
    def _extract_etag(put_result: Any) -> str | None:
        if put_result is None:
            return None
        if isinstance(put_result, dict):
            return put_result.get("e_tag")
        return getattr(put_result, "etag", None)

    @staticmethod
    def _to_int_or_none(value: Any) -> int | None:
        if value is None:
            return None
        return int(value)
