import asyncio
import time
import uuid
from io import BytesIO
from typing import List, Callable

from icestream.config import Config
from icestream.kafkaserver.types import ProduceTopicPartitionData
from icestream.kafkaserver.wal.serde import encode_kafka_wal_file_with_offsets
from icestream.logger import log
from icestream.models import WALFile, WALFileOffset


class WALManager:
    def __init__(self, config: Config, queue: asyncio.Queue[ProduceTopicPartitionData], time_source: Callable[[], float] = time.monotonic):
        self.config = config
        self.queue = queue
        self.time_source = time_source
        self.buffer: List[ProduceTopicPartitionData] = []
        self.buffer_size = 0
        self.last_flush_time = self.time_source()
        self.flush_semaphore = asyncio.Semaphore(self.config.MAX_IN_FLIGHT_FLUSHES)
        self.pending_flushes: set[asyncio.Task] = set()
        log.info(
            f"WALManager initialized: flush size={self.config.FLUSH_SIZE}, "
            f"interval={self.config.FLUSH_INTERVAL}, "
            f"max in-flight flushes={self.config.MAX_IN_FLIGHT_FLUSHES}"
        )

    async def run(self):
        log.info("WALManager started")
        try:
            while True:
                await self.run_once()
        except asyncio.CancelledError:
            log.info("WALManager run loop cancelled, flushing remaining buffer")

            for task in self.pending_flushes:
                if not task.done():
                    task.cancel()
            if self.pending_flushes:
                await asyncio.gather(*self.pending_flushes, return_exceptions=True)

            if self.buffer:
                await self._flush(self.buffer)
            raise
        finally:
            log.info("WALManager stopped")

    async def run_once(self, now: float | None = None):
        now = now or self.time_source()
        timeout = self.config.FLUSH_INTERVAL - (now - self.last_flush_time)
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

    async def _launch_flush(self):
        self.last_flush_time = time.monotonic()
        if not self.buffer:
            return
        batch_to_flush = self.buffer
        self.buffer = []
        self.buffer_size = 0

        flush_task = asyncio.create_task(self._flush(batch_to_flush))
        self.pending_flushes.add(flush_task)
        flush_task.add_done_callback(lambda t: self.pending_flushes.discard(t))


    async def _flush(self, batch_to_flush: List[ProduceTopicPartitionData]):
        try:
            async with self.flush_semaphore:
                encoded, offsets = encode_kafka_wal_file_with_offsets(
                    batch_to_flush, broker_id="foo"  # TODO
                )
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

                async with self.config.async_session_factory() as session:
                    total_records = sum(o["last_offset"] - o["base_offset"] + 1 for o in offsets)

                    wal_file = WALFile(
                        uri=f"{self.config.WAL_BUCKET}{"/" + self.config.WAL_BUCKET_PREFIX if self.config.WAL_BUCKET_PREFIX else ""}/{object_key}",
                        etag=getattr(put_result, "etag", None),
                        total_bytes=len(encoded),
                        total_messages=total_records,
                    )
                    session.add(wal_file)
                    await session.flush()  # get wal_file.id

                    for o in offsets:
                        wal_file_offset = WALFileOffset(
                            wal_file_id=wal_file.id,
                            topic_name=o["topic"],
                            partition_number=o["partition"],
                            base_offset=o["base_offset"],
                            last_offset=o["last_offset"],
                            byte_start=o["byte_start"],
                            byte_end=o["byte_end"],
                        )
                        session.add(wal_file_offset)

                    await session.commit()

                for item in batch_to_flush:
                    if not item.flush_result.done():
                        item.flush_result.set_result(True)

                log.info("WALManager flushed successfully")

        except asyncio.CancelledError:
            log.info("WALManager flush cancelled, setting futures to cancelled")
            for item in batch_to_flush:
                if not item.flush_result.done():
                    item.flush_result.cancel()

        except Exception as e:
            log.exception("WALManager flush failed")
            for item in batch_to_flush:
                if not item.flush_result.done():
                    item.flush_result.set_exception(e)

    @staticmethod
    def _generate_object_key() -> str:
        now = time.gmtime()
        ts = int(time.time() * 1000)
        uid = uuid.uuid4().hex
        bucket = uid[:2]
        return (
            f"wal/{now.tm_year:04}/{now.tm_mon:02}/{now.tm_mday:02}/"
            f"{now.tm_hour:02}/{now.tm_min:02}/{bucket}/{ts}-{uid}.wal"
        )
