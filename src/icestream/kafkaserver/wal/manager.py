import asyncio
import time
import uuid
from io import BytesIO
from typing import List

from icestream.config import Config
from icestream.kafkaserver.types import ProduceTopicPartitionData
from icestream.kafkaserver.wal.serde import encode_kafka_wal_file
from icestream.logger import log


class WALManager:
    def __init__(self, config: Config, queue: asyncio.Queue[ProduceTopicPartitionData]):
        self.config = config
        self.queue = queue
        self.buffer: List[ProduceTopicPartitionData] = []
        self.buffer_size = 0
        self.last_flush_time = time.monotonic()
        self.flush_semaphore = asyncio.Semaphore(self.config.MAX_IN_FLIGHT_FLUSHES)
        log.info(
            f"WALManager initialized: flush size={self.config.FLUSH_SIZE}, "
            f"interval={self.config.FLUSH_INTERVAL}, "
            f"max in-flight flushes={self.config.MAX_IN_FLIGHT_FLUSHES}"
        )

    async def run(self):
        flush_interval = self.config.FLUSH_INTERVAL
        log.info("WALManager started")
        try:
            while True:
                timeout = flush_interval - (time.monotonic() - self.last_flush_time)
                try:
                    item = await asyncio.wait_for(
                        self.queue.get(), timeout=max(0, timeout)
                    )
                    self.buffer.append(item)
                    self.buffer_size += item.kafka_record_batch.batch_length  # TODO
                    if self.buffer_size >= self.config.FLUSH_SIZE:
                        await self._launch_flush()
                except asyncio.TimeoutError:
                    await self._launch_flush()
        except asyncio.CancelledError:
            log.info("WALManager run loop cancelled, flushing remaining buffer")
            if self.buffer:
                await self._flush(self.buffer)
            raise
        finally:
            log.info("WALManager stopped")

    async def _launch_flush(self):
        if not self.buffer:
            return
        batch_to_flush = self.buffer
        self.buffer = []
        self.buffer_size = 0
        self.last_flush_time = time.monotonic()
        asyncio.create_task(self._flush(batch_to_flush))

    async def _flush(self, batch_to_flush: List[ProduceTopicPartitionData]):
        try:
            async with self.flush_semaphore:
                encoded = encode_kafka_wal_file(batch_to_flush, broker_id="foo")  # TODO
                object_key = self._generate_object_key()
                log.info(
                    f"WALManager encoded {len(encoded)} bytes for {len(batch_to_flush)} batches"
                )

                put_result = await self.config.store.put_async(
                    path=object_key,
                    file=BytesIO(encoded),
                    use_multipart=len(encoded) > 5 * 1024 * 1024,
                )
                log.info(f"put_result {put_result}")

                for item in batch_to_flush:
                    if not item.flush_result.done():
                        item.flush_result.set_result(True)

                log.info("WALManager flushed successfully")
        except Exception as e:
            log.exception("WALManager flush failed")
            for item in batch_to_flush:
                if not item.flush_result.done():
                    item.flush_result.set_exception(e)

    def _generate_object_key(self) -> str:
        ts = int(time.time() * 1000)
        uid = uuid.uuid4().hex
        return f"wal/{ts}-{uid}.wal"
