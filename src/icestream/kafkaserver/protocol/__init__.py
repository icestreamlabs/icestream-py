import struct
from dataclasses import dataclass
from io import BytesIO
from typing import Self


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
