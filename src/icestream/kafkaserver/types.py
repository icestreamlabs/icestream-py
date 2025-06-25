import asyncio
from dataclasses import dataclass


@dataclass
class ProduceTopicPartitionData:
    topic: str
    partition: int
    batch_bytes: bytes
    flush_result: asyncio.Future[None]
