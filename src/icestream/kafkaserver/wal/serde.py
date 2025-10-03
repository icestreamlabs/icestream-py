import struct
import time
from io import BytesIO
from typing import List

from icestream.kafkaserver.protocol import (
    KafkaRecordBatch,
    KafkaRecordHeader,
    KafkaRecord, decode_kafka_records,
)
from icestream.kafkaserver.types import ProduceTopicPartitionData
from icestream.kafkaserver.utils import encode_varint, decode_varint
from icestream.kafkaserver.wal import WALBatch, WALFile


def encode_kafka_wal_file_with_offsets(
        batches: List[ProduceTopicPartitionData], broker_id: str
) -> tuple[bytes, list[dict]]:
    buf = BytesIO()
    offset_metadata = []

    buf.write(b"WAL1")
    buf.write(struct.pack(">B", 1))
    buf.write(struct.pack(">Q", int(time.time() * 1000)))
    broker_bytes = broker_id.encode("utf-8")
    buf.write(encode_varint(len(broker_bytes)))
    buf.write(broker_bytes)
    buf.write(encode_varint(len(batches)))

    for batch in batches:
        topic_bytes = batch.topic.encode("utf-8")
        buf.write(encode_varint(len(topic_bytes)))
        buf.write(topic_bytes)
        buf.write(struct.pack(">i", batch.partition))

        rb = batch.kafka_record_batch

        record_batch_bytes = rb.to_bytes()

        byte_start = buf.tell()
        buf.write(encode_varint(len(record_batch_bytes)))
        buf.write(record_batch_bytes)
        byte_end = buf.tell()

        now_ts = int(time.time() * 1000)
        min_ts = now_ts
        max_ts = now_ts

        if rb.base_timestamp != 0 and rb.max_timestamp != 0:
            min_ts = rb.base_timestamp
            max_ts = rb.max_timestamp

        offset_metadata.append(
            {
                "topic": batch.topic,
                "partition": batch.partition,
                "base_offset": rb.base_offset,
                "last_offset": rb.base_offset + rb.last_offset_delta,
                "byte_start": byte_start,
                "byte_end": byte_end,
                "min_timestamp": min_ts,
                "max_timestamp": max_ts,
            }
        )

    return buf.getvalue(), offset_metadata


def decode_kafka_wal_file(data: bytes) -> WALFile:
    buf = BytesIO(data)

    if buf.read(4) != b"WAL1":
        raise ValueError("Invalid WAL file magic")

    version = struct.unpack(">B", buf.read(1))[0]
    flushed_at = struct.unpack(">Q", buf.read(8))[0]
    broker_id_len = decode_varint(buf)
    broker_id = buf.read(broker_id_len).decode("utf-8")
    batch_count = decode_varint(buf)

    batches = []
    for _ in range(batch_count):
        topic_len = decode_varint(buf)
        topic = buf.read(topic_len).decode("utf-8")
        partition = struct.unpack(">i", buf.read(4))[0]
        batch_len = decode_varint(buf)
        record_batch_bytes = buf.read(batch_len)
        kafka_record_batch = KafkaRecordBatch.from_bytes(record_batch_bytes)
        batches.append(WALBatch(topic, partition, kafka_record_batch))

    return WALFile(version, flushed_at, broker_id, batches)
