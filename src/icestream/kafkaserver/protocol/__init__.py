from typing import Self, Optional, List

from dataclasses import dataclass
from typing import List, Optional
from io import BytesIO
import struct

from icestream.kafkaserver.utils import (
    decode_signed_varint,
    decode_varint,
    decode_signed_varlong, encode_signed_varlong, encode_signed_varint, encode_varint,
)


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

        attributes = struct.unpack(">B", buf.read(1))[0] # unsigned byte 0..255
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

    def to_bytes(self) -> bytes:
        body = bytearray()

        # fixed fields
        body += struct.pack(">B", self.attributes & 0xFF) # unsigned byte with mask to be safe
        body += encode_signed_varlong(self.timestamp_delta)
        body += encode_signed_varint(self.offset_delta)

        # key
        if self.key is None:
            body += encode_signed_varint(-1)
        else:
            body += encode_signed_varint(len(self.key))
            body += self.key

        # value
        if self.value is None:
            body += encode_signed_varint(-1)
        else:
            body += encode_signed_varint(len(self.value))
            body += self.value

        # headers
        body += encode_varint(len(self.headers))
        for h in self.headers:
            key_bytes = h.key.encode("utf-8")
            body += encode_varint(len(key_bytes))
            body += key_bytes
            if h.value is None:
                body += encode_signed_varint(-1)
            else:
                body += encode_signed_varint(len(h.value))
                body += h.value

        return encode_varint(len(body)) + bytes(body)

def decode_kafka_records(records_blob: bytes) -> List[KafkaRecord]:
    buf = BytesIO(records_blob)
    records = []
    while buf.tell() < len(records_blob):
        records.append(KafkaRecord.from_bytes(buf))
    return records


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
    def from_records(cls, offset: int, records: List[KafkaRecord]) -> Self:
        # serialize records
        record_blobs = b"".join(r.to_bytes() for r in records)

        # compute metadata
        base_offset = offset
        # Kafka's batch_length EXCLUDES the first 12 bytes (base_offset + batch_length)
        header_after_12 = (
                4  # partition_leader_epoch
                + 1  # magic
                + 4  # crc
                + 2  # attributes
                + 4  # last_offset_delta
                + 8  # base_timestamp
                + 8  # max_timestamp
                + 8  # producer_id
                + 2  # producer_epoch
                + 4  # base_sequence
                + 4  # records_count
        )
        batch_length = header_after_12 + len(record_blobs)
        partition_leader_epoch = 0
        magic = 2
        crc = 0  # you can compute CRC later if needed
        attributes = 0
        last_offset_delta = len(records) - 1
        base_timestamp = 0
        max_timestamp = 0
        producer_id = -1
        producer_epoch = -1
        base_sequence = -1
        records_count = len(records)

        return cls(
            base_offset=base_offset,
            batch_length=batch_length,
            partition_leader_epoch=partition_leader_epoch,
            magic=magic,
            crc=crc,
            attributes=attributes,
            last_offset_delta=last_offset_delta,
            base_timestamp=base_timestamp,
            max_timestamp=max_timestamp,
            producer_id=producer_id,
            producer_epoch=producer_epoch,
            base_sequence=base_sequence,
            records_count=records_count,
            records=record_blobs,
        )

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

    def to_bytes(self) -> bytes:
        body = bytearray()

        body += struct.pack(">q", self.base_offset)
        body += struct.pack(">i", self.batch_length)
        body += struct.pack(">i", self.partition_leader_epoch)
        body += struct.pack(">b", self.magic)
        body += struct.pack(">I", self.crc)
        body += struct.pack(">h", self.attributes)
        body += struct.pack(">i", self.last_offset_delta)
        body += struct.pack(">q", self.base_timestamp)
        body += struct.pack(">q", self.max_timestamp)
        body += struct.pack(">q", self.producer_id)
        body += struct.pack(">h", self.producer_epoch)
        body += struct.pack(">i", self.base_sequence)
        body += struct.pack(">i", self.records_count)

        body += self.records

        return bytes(body)
