import asyncio

from icestream.config import Config
from icestream.kafkaserver.types import ProduceTopicPartitionData
from icestream.logger import log



class WALManager:
    def __init__(self, config: Config, queue: asyncio.Queue[ProduceTopicPartitionData]):
        self.config = config
        self.queue = queue
        self._periodic_check_task: asyncio.Task | None = None

    async def start(self):
        if self._periodic_check_task is None or self._periodic_check_task.done():
            self._periodic_check_task = asyncio.create_task(self._run_periodic_flush_check())
            log.info(f"WALManager started")

    async def _run_periodic_flush_check(self):
        while True:
            await asyncio.sleep(self.config.FLUSH_INTERVAL)
