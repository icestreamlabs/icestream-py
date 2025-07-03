from dataclasses import dataclass
from typing import List

from icestream.kafkaserver.protocol import KafkaRecordBatch


@dataclass
class WALBatch:
    topic: str
    partition: int
    kafka_record_batch: KafkaRecordBatch


@dataclass
class WALFile:
    version: int
    flushed_at: int
    broker_id: str
    batches: List[WALBatch]
