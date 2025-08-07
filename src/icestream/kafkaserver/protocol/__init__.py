import struct
from dataclasses import dataclass
from io import BytesIO
from typing import Self, Optional, List

from icestream.kafkaserver.utils import decode_signed_varint, decode_varint, decode_signed_varlong


@dataclass
class KafkaRecordBatch:
    base_offset: int
    batch_length: int
    partition_leader_epoch: int
    magic: int
    crc: int
    attributes: int
    last_offset_delta: int
    base_timestamp: int
    max_timestamp: int
    producer_id: int
    producer_epoch: int
    base_sequence: int
    records_count: int
    records: bytes  # raw payload of [Record] section

    @classmethod
    def from_bytes(cls, data: bytes) -> Self:
        buf = BytesIO(data)

        base_offset = struct.unpack(">q", buf.read(8))[0]
        batch_length = struct.unpack(">i", buf.read(4))[0]
        partition_leader_epoch = struct.unpack(">i", buf.read(4))[0]
        magic = struct.unpack(">b", buf.read(1))[0]
        crc = struct.unpack(">I", buf.read(4))[0]
        attributes = struct.unpack(">h", buf.read(2))[0]
        last_offset_delta = struct.unpack(">i", buf.read(4))[0]
        base_timestamp = struct.unpack(">q", buf.read(8))[0]
        max_timestamp = struct.unpack(">q", buf.read(8))[0]
        producer_id = struct.unpack(">q", buf.read(8))[0]
        producer_epoch = struct.unpack(">h", buf.read(2))[0]
        base_sequence = struct.unpack(">i", buf.read(4))[0]
        records_count = struct.unpack(">i", buf.read(4))[0]
        records = buf.read(
            batch_length - (buf.tell() - 12)
        )  # 12 = base_offset(8) + batch_length(4)

        return KafkaRecordBatch(
            base_offset,
            batch_length,
            partition_leader_epoch,
            magic,
            crc,
            attributes,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records_count,
            records,
        )


from dataclasses import dataclass
from typing import List, Optional
from io import BytesIO
import struct


@dataclass
class KafkaRecordHeader:
    key: str
    value: Optional[bytes]


@dataclass
class KafkaRecord:
    attributes: int
    timestamp_delta: int
    offset_delta: int
    key: Optional[bytes]
    value: Optional[bytes]
    headers: List[KafkaRecordHeader]

    @classmethod
    def from_bytes(cls, buf: BytesIO) -> Self:
        start_pos = buf.tell()
        length = decode_varint(buf)
        record_end = start_pos + length + (buf.tell() - start_pos)

        attributes = struct.unpack(">b", buf.read(1))[0]
        timestamp_delta = decode_signed_varlong(buf)
        offset_delta = decode_signed_varint(buf)

        key_len = decode_signed_varint(buf)
        key = buf.read(key_len) if key_len >= 0 else None

        value_len = decode_signed_varint(buf)
        value = buf.read(value_len) if value_len >= 0 else None

        headers_count = decode_varint(buf)
        headers = []
        for _ in range(headers_count):
            key_len = decode_varint(buf)
            key_str = buf.read(key_len).decode("utf-8")
            val_len = decode_signed_varint(buf)
            val = buf.read(val_len) if val_len >= 0 else None
            headers.append(KafkaRecordHeader(key_str, val))

        if buf.tell() > record_end:
            raise ValueError("Record over-read")
        elif buf.tell() < record_end:
            buf.seek(record_end)

        return cls(
            attributes=attributes,
            timestamp_delta=timestamp_delta,
            offset_delta=offset_delta,
            key=key,
            value=value,
            headers=headers,
        )


def decode_kafka_records(records_blob: bytes) -> List[KafkaRecord]:
    buf = BytesIO(records_blob)
    records = []
    while buf.tell() < len(records_blob):
        records.append(KafkaRecord.from_bytes(buf))
    return records
