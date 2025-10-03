import asyncio
import datetime
import time
from collections import defaultdict
from typing import Sequence

import httpx
from sqlalchemy import select
from sqlalchemy.orm import joinedload, selectinload

from icestream.compaction.types import CompactionProcessor, CompactionContext
from icestream.config import Config
from icestream.kafkaserver.wal import WALFile as DecodedWALFile, WALBatch
from icestream.kafkaserver.wal.serde import decode_kafka_wal_file
from icestream.models import WALFile as WALFileModel, Topic, ParquetFile
from icestream.logger import log
from icestream.utils import wal_uri_to_object_key


class CompactorWorker:
    def __init__(
            self,
            config: Config,
            processors: list[CompactionProcessor],
            time_source=time.monotonic,
    ):
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

        async with self.config.async_session_factory() as session:
            wal_models, total_bytes = await self._select_wal_models(session)

            if not wal_models:
                await self._sleep_until_next_interval(start_time)
                return

            wal_decoded: list[DecodedWALFile] = await self._fetch_and_decode(wal_models)
            parquet_candidates = await self._select_parquet_candidates(session)

            ctx = CompactionContext(
                config=self.config,
                wal_models=wal_models,
                wal_decoded=wal_decoded,
                parquet_candidates=parquet_candidates,
                now_monotonic=start_time,
            )

            for p in self.processors:
                await p.apply(ctx)

            now_ts = datetime.datetime.now(datetime.UTC)
            for wm in wal_models:
                if wm.compacted_at is None:
                    wm.compacted_at = now_ts
            await session.commit()

        await self._sleep_until_next_interval(start_time)

    async def _sleep_until_next_interval(self, start_time: float):
        elapsed = self.time_source() - start_time
        sleep_time = max(0, self.config.COMPACTION_INTERVAL - elapsed)
        await asyncio.sleep(sleep_time)

    async def _select_wal_models(self, session) -> tuple[Sequence[WALFileModel], int]:
        total_bytes = 0
        selected: list[WALFileModel] = []
        seen_ids: set[int] = set()

        while (
                total_bytes < self.config.MAX_COMPACTION_BYTES
                and len(selected) < self.config.MAX_COMPACTION_WAL_FILES
        ):
            limit = min(
                self.config.MAX_COMPACTION_SELECT_LIMIT,
                self.config.MAX_COMPACTION_WAL_FILES - len(selected),
            )

            id_rows = await session.scalars(
                select(WALFileModel.id)
                .where(
                    WALFileModel.compacted_at.is_(None),
                    ~WALFileModel.id.in_(seen_ids)
                )
                .order_by(WALFileModel.id)
                .with_for_update(skip_locked=True)
                .limit(limit)
            )
            ids = list(id_rows)
            if not ids:
                break
            seen_ids.update(ids)

            models = (
                await session.execute(
                    select(WALFileModel)
                    .options(selectinload(WALFileModel.wal_file_offsets))
                    .where(WALFileModel.id.in_(ids))
                    .order_by(WALFileModel.id)
                )
            ).scalars().all()

            for wal in models:
                if len(selected) >= self.config.MAX_COMPACTION_WAL_FILES:
                    break
                if total_bytes + wal.total_bytes > self.config.MAX_COMPACTION_BYTES:
                    continue
                selected.append(wal)
                total_bytes += wal.total_bytes

        return selected, total_bytes

    async def _fetch_and_decode(
            self, wal_models: Sequence[WALFileModel]
    ) -> list[DecodedWALFile]:
        decoded: list[DecodedWALFile] = []
        for wal in wal_models:
            object_key = self._wal_uri_to_object_key(wal.uri)
            get_result = await self.config.store.get_async(object_key)
            data = await get_result.bytes_async()
            decoded_file = decode_kafka_wal_file(bytes(data))
            decoded_file.id = wal.id  # type: ignore[attr-defined]
            decoded.append(decoded_file)
        return decoded

    async def _select_parquet_candidates(self, session):
        rows = (
            (
                await session.execute(
                    select(ParquetFile)
                    .where(ParquetFile.compacted_at.is_(None))
                    .order_by(
                        ParquetFile.topic_name,
                        ParquetFile.partition_number,
                        ParquetFile.min_offset,
                    )
                )
            )
            .scalars()
            .all()
        )

        out: dict[tuple[str, int], list[ParquetFile]] = {}
        if not rows:
            return out

        now = datetime.datetime.now(datetime.UTC)
        tg = self.config.PARQUET_COMPACTION_TARGET_BYTES
        min_files = self.config.PARQUET_COMPACTION_MIN_INPUT_FILES
        max_files = self.config.PARQUET_COMPACTION_MAX_INPUT_FILES
        force_age_sec = self.config.PARQUET_COMPACTION_FORCE_AGE_SEC

        # group by (topic, partition)
        cur_key = None
        bucket: list[ParquetFile] = []
        for pf in rows:
            key = (pf.topic_name, pf.partition_number)
            if cur_key is None:
                cur_key = key
            if key != cur_key:
                self._maybe_pick_bucket(
                    out, cur_key, bucket, now, tg, min_files, max_files, force_age_sec
                )
                cur_key, bucket = key, []
            bucket.append(pf)
        if bucket:
            self._maybe_pick_bucket(
                out, cur_key, bucket, now, tg, min_files, max_files, force_age_sec
            )

        return out

    @staticmethod
    def _maybe_pick_bucket(
            out, key, files, now, target_bytes, min_files, max_files, force_age_sec
    ):
        if not files:
            return
        # size trigger
        total = 0
        chosen: list[ParquetFile] = []
        for f in files:
            if len(chosen) >= max_files:
                break
            chosen.append(f)
            total += f.total_bytes
            if total >= target_bytes and len(chosen) >= min_files:
                out[key] = chosen
                return
        # age trigger
        oldest = files[0]
        oldest_ts = oldest.created_at or oldest.updated_at
        if (
                oldest_ts
                and (now - oldest_ts).total_seconds() >= force_age_sec
                and len(files) >= min_files
        ):
            out[key] = files[:max_files]

    def _wal_uri_to_object_key(self, uri: str) -> str:
        return wal_uri_to_object_key(self.config, uri)


# currently only support Avro for schema
# in theory can support plain json with some caveats
# also non schema data, ie pure key/value straight from kafka records as key and value columns
class IcebergCompactor(CompactionProcessor):
    async def apply(self, ctx: CompactionContext):
        batches_by_topic: dict[str, list[WALBatch]] = defaultdict(list)
        for wal_file in ctx.wal_decoded:
            for batch in wal_file.batches:
                batches_by_topic[batch.topic].append(batch)
        async with httpx.AsyncClient() as http_client:
            async with ctx.config.async_session_factory() as session:
                topics = await session.execute(select(Topic))
                for topic in topics.scalars():
                    if not topic.schema:
                        log.info(f"no schema for topic {topic.name}")
                        continue


def build_uri(config, key: str) -> str:
    bucket_prefix = (
        f"/{config.WAL_BUCKET_PREFIX.strip('/')}" if config.WAL_BUCKET_PREFIX else ""
    )
    return f"{bucket_prefix}/{key}"

