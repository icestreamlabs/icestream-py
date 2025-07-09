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


def encode_kafka_wal_file_with_offsets(
    batches: List[ProduceTopicPartitionData], broker_id: str
) -> tuple[bytes, list[dict]]:
    buf = BytesIO()
    offset_metadata = []

    # Write WAL file header
    buf.write(b"WAL1")  # Magic prefix
    buf.write(struct.pack(">B", 1))  # '>B': big-endian unsigned char (1 byte) - version
    buf.write(struct.pack(">Q", int(time.time() * 1000)))  # '>Q': big-endian unsigned long long (8 bytes) - flushed_at timestamp in ms

    # Write broker ID as length-prefixed UTF-8 string
    broker_bytes = broker_id.encode("utf-8")
    buf.write(encode_varint(len(broker_bytes)))  # Length of broker ID (varint)
    buf.write(broker_bytes)
    buf.write(encode_varint(len(batches)))  # Number of batches (varint)

    for batch in batches:
        topic_bytes = batch.topic.encode("utf-8")
        buf.write(encode_varint(len(topic_bytes)))  # Length of topic name (varint)
        buf.write(topic_bytes)
        buf.write(struct.pack(">i", batch.partition))  # '>i': big-endian signed int (4 bytes) - partition number

        record_batch_bytes_io = BytesIO()
        rb = batch.kafka_record_batch

        # Serialize Kafka record batch fields in order (see Kafka protocol spec)

        # '>q': big-endian signed long long (8 bytes) - base offset
        record_batch_bytes_io.write(struct.pack(">q", rb.base_offset))

        # '>i': big-endian signed int (4 bytes) - total batch length
        record_batch_bytes_io.write(struct.pack(">i", rb.batch_length))

        # '>i': big-endian signed int (4 bytes) - partition leader epoch
        record_batch_bytes_io.write(struct.pack(">i", rb.partition_leader_epoch))

        # '>b': big-endian signed char (1 byte) - magic byte (always 2)
        record_batch_bytes_io.write(struct.pack(">b", rb.magic))

        # '>I': big-endian unsigned int (4 bytes) - CRC32C checksum
        record_batch_bytes_io.write(struct.pack(">I", rb.crc))

        # '>h': big-endian signed short (2 bytes) - attributes bitfield
        record_batch_bytes_io.write(struct.pack(">h", rb.attributes))

        # '>i': big-endian signed int (4 bytes) - last offset delta
        record_batch_bytes_io.write(struct.pack(">i", rb.last_offset_delta))

        # '>q': big-endian signed long long (8 bytes) - base timestamp (ms)
        record_batch_bytes_io.write(struct.pack(">q", rb.base_timestamp))

        # '>q': big-endian signed long long (8 bytes) - max timestamp (ms)
        record_batch_bytes_io.write(struct.pack(">q", rb.max_timestamp))

        # '>q': big-endian signed long long (8 bytes) - producer ID
        record_batch_bytes_io.write(struct.pack(">q", rb.producer_id))

        # '>h': big-endian signed short (2 bytes) - producer epoch
        record_batch_bytes_io.write(struct.pack(">h", rb.producer_epoch))

        # '>i': big-endian signed int (4 bytes) - base sequence number
        record_batch_bytes_io.write(struct.pack(">i", rb.base_sequence))

        # '>i': big-endian signed int (4 bytes) - number of records
        record_batch_bytes_io.write(struct.pack(">i", rb.records_count))

        # Raw record data (variable length)
        record_batch_bytes_io.write(rb.records)

        record_batch_bytes = record_batch_bytes_io.getvalue()

        byte_start = buf.tell()
        buf.write(encode_varint(len(record_batch_bytes)))  # Length of batch (varint)
        buf.write(record_batch_bytes)
        byte_end = buf.tell()

        offset_metadata.append({
            "topic": batch.topic,
            "partition": batch.partition,
            "base_offset": rb.base_offset,
            "last_offset": rb.base_offset + rb.last_offset_delta,
            "byte_start": byte_start,
            "byte_end": byte_end,
        })

    return buf.getvalue(), offset_metadata

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
