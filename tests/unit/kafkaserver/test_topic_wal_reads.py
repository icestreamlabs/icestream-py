import io
from dataclasses import dataclass

import pytest

from icestream.kafkaserver.protocol import KafkaRecord, KafkaRecordBatch
from icestream.kafkaserver.topic_wal_reads import read_topic_wal_partition_batches
from icestream.kafkaserver.wal.serde import encode_kafka_wal_file_with_offsets


@dataclass
class _BatchShape:
    topic: str
    partition: int
    kafka_record_batch: KafkaRecordBatch


def _make_batch(base_offset: int, count: int) -> KafkaRecordBatch:
    records: list[KafkaRecord] = []
    for i in range(count):
        records.append(
            KafkaRecord(
                attributes=0,
                timestamp_delta=i,
                offset_delta=i,
                key=None,
                value=f"v-{base_offset + i}".encode(),
                headers=[],
            )
        )
    batch = KafkaRecordBatch.from_records(base_offset, records)
    batch.base_timestamp = 1_700_000_000_000
    batch.max_timestamp = 1_700_000_000_000 + (count - 1)
    return batch


@pytest.mark.asyncio
async def test_partial_range_reads_single_partition_batch(config):
    batch = _make_batch(0, 3)
    payload, meta = encode_kafka_wal_file_with_offsets(
        [_BatchShape(topic="orders", partition=0, kafka_record_batch=batch)],
        broker_id="b1",
    )

    key = "topic_wal/topics/orders/segments/0-2-p0-srcseed-chunk0.wal"
    await config.store.put_async(key, io.BytesIO(payload))

    batches = await read_topic_wal_partition_batches(
        config,
        uri=key,
        topic="orders",
        partition=0,
        byte_start=int(meta[0]["byte_start"]),
        byte_end=int(meta[0]["byte_end"]),
    )

    assert len(batches) == 1
    assert batches[0].base_offset == 0
    assert batches[0].last_offset_delta == 2


@pytest.mark.asyncio
async def test_partial_range_falls_back_to_full_read_on_span_boundary(config):
    b1 = _make_batch(0, 2)
    b2 = _make_batch(2, 2)
    payload, meta = encode_kafka_wal_file_with_offsets(
        [
            _BatchShape(topic="orders", partition=0, kafka_record_batch=b1),
            _BatchShape(topic="orders", partition=0, kafka_record_batch=b2),
        ],
        broker_id="b1",
    )

    key = "topic_wal/topics/orders/segments/0-3-p0-srcseed-chunk0.wal"
    await config.store.put_async(key, io.BytesIO(payload))

    batches = await read_topic_wal_partition_batches(
        config,
        uri=key,
        topic="orders",
        partition=0,
        byte_start=int(meta[0]["byte_start"]),
        byte_end=int(meta[1]["byte_end"]),
    )

    assert [b.base_offset for b in batches] == [0, 2]
