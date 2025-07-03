import asyncio
import time
from typing import List

from icestream.config import Config
from icestream.kafkaserver.types import ProduceTopicPartitionData
from icestream.logger import log
from icestream.kafkaserver.wal.serde import encode_kafka_wal_file


class WALManager:
    def __init__(self, config: Config, queue: asyncio.Queue[ProduceTopicPartitionData]):
        self.config = config
        self.queue = queue
        self.buffer: List[ProduceTopicPartitionData] = []
        self.buffer_size = 0
        self.last_flush_time = time.monotonic()
        log.info(f"WALManager initialized: flush size={self.config.FLUSH_SIZE}, interval={self.config.FLUSH_INTERVAL}")

    async def run(self):
        flush_interval = self.config.FLUSH_INTERVAL
        log.info("WALManager started")
        try:
            while True:
                timeout = flush_interval - (time.monotonic() - self.last_flush_time)
                try:
                    item = await asyncio.wait_for(self.queue.get(), timeout=max(0, timeout))
                    self.buffer.append(item)
                    self.buffer_size += item.kafka_record_batch.batch_length  # TODO
                    if self.buffer_size >= self.config.FLUSH_SIZE:
                        await self._flush()
                except asyncio.TimeoutError:
                    await self._flush()
        except asyncio.CancelledError:
            log.info("WALManager run loop cancelled, flushing remaining buffer")
            if self.buffer:
                await self._flush()
            raise
        finally:
            log.info("WALManager stopped")

    async def _flush(self):
        self.last_flush_time = time.monotonic()

        if not self.buffer:
            return

        batch_to_flush = self.buffer
        self.buffer = []
        self.buffer_size = 0

        try:
            encoded = encode_kafka_wal_file(batch_to_flush, broker_id="foo") # TODO
            log.info(f"WALManager encoded {len(encoded)} bytes for {len(batch_to_flush)} batches")

            await asyncio.sleep(0.1) # TODO

            for item in batch_to_flush:
                if not item.flush_result.done():
                    item.flush_result.set_result(True)

            log.info("WALManager flushed successfully")
        except Exception as e:
            log.exception("WALManager flush failed")
            for item in batch_to_flush:
                if not item.flush_result.done():
                    item.flush_result.set_exception(e)
