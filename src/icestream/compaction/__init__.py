import asyncio
import datetime
import time
from collections import defaultdict
from typing import Protocol, List, Callable, Sequence

import httpx
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from icestream.config import Config
from icestream.kafkaserver.wal import WALFile, WALBatch
from icestream.kafkaserver.wal.serde import decode_kafka_wal_file
from icestream.models import WALFile as WALFileModel, Topic
from icestream.logger import log


class CompactionProcessor(Protocol):
    async def apply(self, config: Config, wal_files: List[WALFile]): ...


class CompactorWorker:
    def __init__(self, config: Config, processors: List[CompactionProcessor], time_source: Callable[[], float] = time.monotonic):
        self.config = config
        self.time_source = time_source
        self.processors = processors

    async def run(self):
        log.info("CompactorWorker started")
        try:
            while True:
                await self.run_once()
        except asyncio.CancelledError:
            pass
        finally:
            log.info("CompactorWorker stopped")

    async def run_once(self, now: float | None = None):
        start_time = now or self.time_source()
        total_bytes = 0
        selected_files: List[WALFileModel] = []
        async with self.config.async_session_factory() as session:
            while total_bytes < self.config.MAX_COMPACTION_BYTES and len(selected_files) < self.config.MAX_COMPACTION_WAL_FILES:
                limit = min(self.config.MAX_COMPACTION_SELECT_LIMIT, self.config.MAX_COMPACTION_WAL_FILES - len(selected_files))
                result = await session.execute(
                    select(WALFileModel)
                    .options(joinedload(WALFileModel.wal_file_offsets))
                    .where(WALFileModel.compacted_at.is_(None))
                    .with_for_update(skip_locked=True)
                    .limit(limit)
                )
                batch: Sequence[WALFileModel] = result.scalars().all()
                if not batch:
                    break

                for wal in batch:
                    if len(selected_files) >= self.config.MAX_COMPACTION_WAL_FILES:
                        break
                    if total_bytes + wal.total_bytes > self.config.MAX_COMPACTION_BYTES:
                        break
                    selected_files.append(wal)
                    total_bytes += wal.total_bytes

            decoded: List[WALFile] = []

            for wal in selected_files:
                data = await self.config.store.get_async(wal.uri)
                decoded_file = decode_kafka_wal_file(bytes(data.bytes()))
                decoded.append(decoded_file)

            for processor in self.processors:
                await processor.apply(self.config, decoded)

            for wal in selected_files:
                wal.compacted_at = datetime.datetime.now(datetime.UTC)

            await session.commit()

        elapsed_time = self.time_source() - start_time
        sleep_time = max(0, self.config.COMPACTION_INTERVAL - elapsed_time)
        await asyncio.sleep(sleep_time)



# currently only support Avro for schema
# in theory can support plain json with some caveats
# also non schema data, ie pure key/value straight from kafka records as key and value columns
class IcebergCompactor(CompactionProcessor):
    async def apply(self, config: Config, wal_files: list[WALFile]):
        batches_by_topic: dict[str, list[WALBatch]] = defaultdict(list)
        for wal_file in wal_files:
            for batch in wal_file.batches:
                batches_by_topic[batch.topic].append(batch)
        async with httpx.AsyncClient() as http_client:
            async with config.async_session_factory() as session:
                topics = await session.execute(select(Topic))
                for topic in topics.scalars():
                    if not topic.schema:
                        log.info(f"no schema for topic {topic.name}")
                        continue

