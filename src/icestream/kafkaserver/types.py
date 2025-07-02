import asyncio
from dataclasses import dataclass



@dataclass
class ProduceTopicPartitionData:
    topic: str
    partition: int
    kafka_record_batch: KafkaRecordBatch
    flush_result: asyncio.Future[bool]
