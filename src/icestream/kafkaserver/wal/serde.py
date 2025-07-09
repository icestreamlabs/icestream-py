import struct
import time
from io import BytesIO
from typing import List

from icestream.kafkaserver.protocol import KafkaRecordBatch
from icestream.kafkaserver.types import ProduceTopicPartitionData
from icestream.kafkaserver.wal import WALBatch, WALFile


def encode_varint(n: int) -> bytes:
    out = bytearray()
    while True:
        to_write = n & 0x7F
        n >>= 7
        if n:
            out.append(to_write | 0x80)
        else:
            out.append(to_write)
            break
    return bytes(out)


def encode_kafka_wal_file(
    batches: List[ProduceTopicPartitionData], broker_id: str
) -> bytes:
    buf = BytesIO()

    # wal file header
    buf.write(b"WAL1")
    buf.write(struct.pack(">B", 1))  # version
    buf.write(struct.pack(">Q", int(time.time() * 1000)))  # flushed_at
    broker_bytes = broker_id.encode("utf-8")
    buf.write(encode_varint(len(broker_bytes)))
    buf.write(broker_bytes)
    buf.write(encode_varint(len(batches)))

    for batch in batches:
        topic_bytes = batch.topic.encode("utf-8")
        buf.write(encode_varint(len(topic_bytes)))
        buf.write(topic_bytes)
        buf.write(struct.pack(">i", batch.partition))

        # overwrite base_offset in the record batch
        out = BytesIO()
        rb = batch.kafka_record_batch
        out.write(struct.pack(">q", rb.base_offset))
        out.write(struct.pack(">i", rb.batch_length))
        out.write(struct.pack(">i", rb.partition_leader_epoch))
        out.write(struct.pack(">b", rb.magic))
        out.write(struct.pack(">I", rb.crc))
        out.write(struct.pack(">h", rb.attributes))
        out.write(struct.pack(">i", rb.last_offset_delta))
        out.write(struct.pack(">q", rb.base_timestamp))
        out.write(struct.pack(">q", rb.max_timestamp))
        out.write(struct.pack(">q", rb.producer_id))
        out.write(struct.pack(">h", rb.producer_epoch))
        out.write(struct.pack(">i", rb.base_sequence))
        out.write(struct.pack(">i", rb.records_count))
        out.write(rb.records)
        record_batch_bytes = out.getvalue()

        buf.write(encode_varint(len(record_batch_bytes)))
        buf.write(record_batch_bytes)

    return buf.getvalue()


def decode_varint(buf: BytesIO) -> int:
    shift = 0
    result = 0
    while True:
        b = buf.read(1)
        if not b:
            raise EOFError("Unexpected EOF in varint")
        byte = b[0]
        result |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            break
        shift += 7
    return result


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
