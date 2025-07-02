from dataclasses import dataclass
from typing import List


@dataclass
class DecodedBatch:
    topic: str
    partition: int
    base_offset: int
    record_count: int
    compression: int
    uncompressed_size: int
    batch_bytes: bytes

@dataclass
class DecodedWALFile:
    version: int
    flushed_at: int
    broker_id: str
    batches: List[DecodedBatch]
