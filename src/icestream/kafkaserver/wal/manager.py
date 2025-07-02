import asyncio
import time
from typing import List

from icestream.config import Config
from icestream.kafkaserver.types import ProduceTopicPartitionData
from icestream.logger import log



class WALManager:
    def __init__(self, config: Config, queue: asyncio.Queue[ProduceTopicPartitionData]):
        self.config = config
        self.queue = queue
        self._periodic_check_task: asyncio.Task | None = None
        self.buffer: List[ProduceTopicPartitionData] = []
        self.buffer_size = 0
        self.last_flush_time = time.monotonic()
        log.info(f"WALmanager initialized: flush size={self.config.FLUSH_SIZE}, interval={self.config.FLUSH_INTERVAL}")

    async def start(self):
        if self._periodic_check_task is None or self._periodic_check_task.done():
            self._periodic_check_task = asyncio.create_task(self._run_periodic_flush_check())
            log.info(f"WALManager started")

    async def _run_periodic_flush_check(self):
        flush_interval = self.config.FLUSH_INTERVAL
        while True:
            timeout = flush_interval - (time.monotonic() - self.last_flush_time)
            try:
                item = await asyncio.wait_for(self.queue.get(), timeout=max(0, timeout))
                self.buffer.append(item)
                self.buffer_size += len(item.batch_bytes)
                if self.buffer_size >= self.config.FLUSH_SIZE:
                    await self._flush()
            except asyncio.TimeoutError:
                if self.buffer:
                    await self._flush()

    async def _flush(self):
        pass

    async def stop(self):
        log.info("WALManager stopping")
        if self._periodic_check_task:
            self._periodic_check_task.cancel()
            try:
                await self._periodic_check_task
            except asyncio.CancelledError:
                pass
        if self.buffer:
            await self._flush()
        log.info("WALManager stopped")
