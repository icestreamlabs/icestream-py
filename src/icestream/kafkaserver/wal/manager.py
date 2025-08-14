import asyncio
import time
import uuid
from io import BytesIO
from typing import Callable, Optional

from icestream.config import Config
from icestream.kafkaserver.types import ProduceTopicPartitionData
from icestream.kafkaserver.wal.serde import encode_kafka_wal_file_with_offsets
from icestream.logger import log
from icestream.models import WALFile, WALFileOffset


def default_size_estimator(item: ProduceTopicPartitionData) -> int:
    try:
        krb = item.kafka_record_batch
        return max(0, int(12 + krb.batch_length))
    except Exception:
        return 1024  # conservative fallback


class WALManager:
    def __init__(
        self,
        config: Config,
        queue: asyncio.Queue[ProduceTopicPartitionData],
        *,
        time_source: Callable[[], float] = time.monotonic,
        size_estimator: Optional[Callable[[ProduceTopicPartitionData], int]] = None,
    ):
        self.config = config
        self.queue = queue
        self.time_source = time_source
        self.size_estimator = size_estimator or default_size_estimator

        self.buffer: list[ProduceTopicPartitionData] = []
        self.buffer_size = 0
        self.buffer_count = 0
        self.last_flush_time = self.time_source()

        self.flush_semaphore = asyncio.Semaphore(self.config.MAX_IN_FLIGHT_FLUSHES)
        self.pending_flushes: set[asyncio.Task] = set()

        self._max_batches = getattr(self.config, "FLUSH_MAX_BATCHES", None)
        self._flush_timeout = float(getattr(self.config, "FLUSH_TIMEOUT", 60.0))

        log.info(
            "WALManager initialized",
            extra={
                "flush_size": getattr(self.config, "FLUSH_SIZE", None),
                "flush_interval": getattr(self.config, "FLUSH_INTERVAL", None),
                "max_in_flight_flushes": getattr(
                    self.config, "MAX_IN_FLIGHT_FLUSHES", None
                ),
                "flush_max_batches": self._max_batches,
                "flush_timeout_s": self._flush_timeout,
            },
        )

    async def run(self):
        log.info("WALManager started")
        try:
            while True:
                await self.run_once()
        except asyncio.CancelledError:
            log.info("WALManager run loop cancelled; waiting for in-flight flushes")

            if self.pending_flushes:
                done, pending = await asyncio.wait(
                    self.pending_flushes, timeout=self._flush_timeout
                )
                for t in pending:
                    t.cancel()
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True)

            if self.buffer:
                try:
                    await asyncio.wait_for(
                        self._flush(self._swap_buffer_for_flush()),
                        timeout=self._flush_timeout,
                    )
                except Exception:
                    log.exception("Final buffer flush failed during shutdown")

            raise
        finally:
            log.info("WALManager stopped")

    async def run_once(self, now: float | None = None):
        now = now or self.time_source()
        timeout = max(0.0, self.config.FLUSH_INTERVAL - (now - self.last_flush_time))

        try:
            item = await asyncio.wait_for(self.queue.get(), timeout=timeout)
            self._buffer_item(item)

            should_flush = (
                self.config.FLUSH_SIZE is not None
                and self.buffer_size >= self.config.FLUSH_SIZE
            ) or (
                self._max_batches is not None and self.buffer_count >= self._max_batches
            )
            if should_flush:
                await self._launch_flush()
        except asyncio.TimeoutError:
            await self._launch_flush()

    def _buffer_item(self, item: ProduceTopicPartitionData) -> None:
        self.buffer.append(item)
        self.buffer_count += 1

        try:
            est = int(self.size_estimator(item))
        except Exception:
            log.exception("size_estimator raised; using fallback")
            est = 1024
        self.buffer_size += max(0, est)

    async def _launch_flush(self):
        self.last_flush_time = self.time_source()
        if not self.buffer:
            return

        batch_to_flush = self._swap_buffer_for_flush()

        flush_task = asyncio.create_task(self._timed_flush(batch_to_flush))
        self.pending_flushes.add(flush_task)
        flush_task.add_done_callback(lambda t: self.pending_flushes.discard(t))

    async def _timed_flush(self, batch_to_flush: list[ProduceTopicPartitionData]):
        try:
            await asyncio.wait_for(
                self._flush(batch_to_flush),
                timeout=self._flush_timeout,
            )
        except asyncio.TimeoutError:
            log.error(
                "WALManager flush timed out",
                extra={
                    "batches": len(batch_to_flush),
                    "timeout_s": self._flush_timeout,
                },
            )
            for item in batch_to_flush:
                if not item.flush_result.done():
                    item.flush_result.set_exception(
                        asyncio.TimeoutError("flush timed out")
                    )
        except asyncio.CancelledError:
            for item in batch_to_flush:
                if not item.flush_result.done():
                    item.flush_result.cancel()
            raise
        except Exception as e:
            for item in batch_to_flush:
                if not item.flush_result.done():
                    item.flush_result.set_exception(e)

    def _swap_buffer_for_flush(self) -> list[ProduceTopicPartitionData]:
        batch_to_flush = self.buffer
        self._reset_buffer()
        return batch_to_flush

    def _reset_buffer(self):
        self.buffer = []
        self.buffer_size = 0
        self.buffer_count = 0

    async def _flush(self, batch_to_flush: list[ProduceTopicPartitionData]):
        object_key = None
        try:
            async with self.flush_semaphore:
                t0 = self.time_source()
                broker_id = getattr(self.config, "BROKER_ID", "unknown")

                encoded, offsets = encode_kafka_wal_file_with_offsets(
                    batch_to_flush, broker_id
                )
                encode_ms = int((self.time_source() - t0) * 1000)

                object_key = self._generate_object_key(broker_id=broker_id)
                encoded_len = len(encoded)
                log.info(
                    "WALManager encoded batch",
                    extra={
                        "bytes": encoded_len,
                        "batches": len(batch_to_flush),
                        "encode_ms": encode_ms,
                        "object_key": object_key,
                    },
                )

                t1 = self.time_source()
                put_result = await self.config.store.put_async(
                    path=object_key,
                    file=BytesIO(encoded),
                    use_multipart=encoded_len > 5 * 1024 * 1024,
                )
                upload_ms = int((self.time_source() - t1) * 1000)

                uri = self._build_wal_uri(object_key)

                total_records = 0
                for o in offsets:
                    base_off = o["base_offset"]
                    last_off = o["last_offset"]
                    if last_off >= base_off:
                        total_records += last_off - base_off + 1

                t2 = self.time_source()
                async with self.config.async_session_factory() as session:
                    wal_file = WALFile(
                        uri=uri,
                        etag=getattr(put_result, "etag", None),
                        total_bytes=encoded_len,
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
                db_ms = int((self.time_source() - t2) * 1000)

                for item in batch_to_flush:
                    if not item.flush_result.done():
                        item.flush_result.set_result(True)

                total_ms = int((self.time_source() - t0) * 1000)
                log.info(
                    "WALManager flushed successfully",
                    extra={
                        "object_key": object_key,
                        "bytes": encoded_len,
                        "batches": len(batch_to_flush),
                        "upload_ms": upload_ms,
                        "db_ms": db_ms,
                        "total_ms": total_ms,
                        "etag": getattr(put_result, "etag", None),
                    },
                )

        except Exception as e:
            log.exception("WALManager flush failed", extra={"object_key": object_key})
            for item in batch_to_flush:
                if not item.flush_result.done():
                    item.flush_result.set_exception(e)

    def _build_wal_uri(self, object_key: str) -> str:
        prefix = getattr(self.config, "WAL_BUCKET_PREFIX", None)
        if prefix:
            return f"{self.config.WAL_BUCKET}/{prefix}/{object_key}"
        return f"{self.config.WAL_BUCKET}/{object_key}"

    @staticmethod
    def _generate_object_key(*, broker_id: str = "unknown") -> str:
        now = time.gmtime()
        ts_ms = int(time.time() * 1000)
        uid = uuid.uuid4().hex
        bucket = uid[:2]
        return (
            f"wal/{now.tm_year:04}/{now.tm_mon:02}/{now.tm_mday:02}/"
            f"{now.tm_hour:02}/{now.tm_min:02}/{bucket}/"
            f"{ts_ms}-{broker_id}-{uid}.wal"
        )
