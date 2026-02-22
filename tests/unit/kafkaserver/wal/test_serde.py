import asyncio
from dataclasses import dataclass
from io import BytesIO

from icestream.kafkaserver.protocol import KafkaRecord, KafkaRecordBatch
from icestream.kafkaserver.types import ProduceTopicPartitionData
from icestream.kafkaserver.utils import decode_varint
from icestream.kafkaserver.wal.serde import (
    decode_kafka_wal_file,
    encode_kafka_wal_file_with_offsets,
)


@dataclass
class FakeWalBatch:
    topic: str
    partition: int
    kafka_record_batch: KafkaRecordBatch


def _build_batch(base_offset: int = 42) -> KafkaRecordBatch:
    recs = [
        KafkaRecord(
            attributes=0,
            timestamp_delta=0,
            offset_delta=0,
            key=b"k0",
            value=b"v0",
            headers=[],
        ),
        KafkaRecord(
            attributes=0,
            timestamp_delta=5,
            offset_delta=1,
            key=b"k1",
            value=b"v1",
            headers=[],
        ),
    ]
    batch = KafkaRecordBatch.from_records(base_offset, recs)
    batch.base_timestamp = 1_700_000_000_000
    batch.max_timestamp = 1_700_000_000_005
    return batch


def test_encode_decode_roundtrip_produce_shape():
    batch = _build_batch()
    ptd = ProduceTopicPartitionData(
        topic="orders",
        partition=3,
        kafka_record_batch=batch,
        flush_result=asyncio.Future(),
    )

    encoded, offsets = encode_kafka_wal_file_with_offsets([ptd], broker_id="b1")
    decoded = decode_kafka_wal_file(encoded)

    assert encoded[:4] == b"WAL1"
    assert decoded.version == 1
    assert decoded.broker_id == "b1"
    assert len(decoded.batches) == 1
    assert decoded.batches[0].topic == "orders"
    assert decoded.batches[0].partition == 3
    assert decoded.batches[0].kafka_record_batch.base_offset == 42

    assert len(offsets) == 1
    assert offsets[0]["topic"] == "orders"
    assert offsets[0]["partition"] == 3
    assert offsets[0]["base_offset"] == 42
    assert offsets[0]["last_offset"] == 43
    assert offsets[0]["byte_end"] > offsets[0]["byte_start"] >= 0
    assert offsets[0]["min_timestamp"] == 1_700_000_000_000
    assert offsets[0]["max_timestamp"] == 1_700_000_000_005


def test_encode_accepts_generic_batch_shape():
    batch = _build_batch(base_offset=100)
    generic = FakeWalBatch(topic="payments", partition=0, kafka_record_batch=batch)

    encoded, offsets = encode_kafka_wal_file_with_offsets([generic], broker_id="broker-x")
    decoded = decode_kafka_wal_file(encoded)

    assert len(decoded.batches) == 1
    assert decoded.batches[0].topic == "payments"
    assert decoded.batches[0].partition == 0
    assert decoded.batches[0].kafka_record_batch.base_offset == 100
    assert offsets[0]["base_offset"] == 100
    assert offsets[0]["last_offset"] == 101


def test_offset_byte_span_points_to_embedded_record_batch():
    batch = _build_batch(base_offset=7)
    generic = FakeWalBatch(topic="topic-a", partition=1, kafka_record_batch=batch)

    encoded, offsets = encode_kafka_wal_file_with_offsets([generic], broker_id="broker-y")

    start = offsets[0]["byte_start"]
    end = offsets[0]["byte_end"]
    span = memoryview(encoded)[start:end]

    # Metadata byte span starts at the length-prefix varint, then record-batch bytes.
    span_reader = BytesIO(bytes(span))
    size = decode_varint(span_reader)
    rb_bytes = span_reader.read(size)
    roundtrip_batch = KafkaRecordBatch.from_bytes(rb_bytes)

    assert roundtrip_batch.base_offset == 7
    assert roundtrip_batch.last_offset_delta == 1
