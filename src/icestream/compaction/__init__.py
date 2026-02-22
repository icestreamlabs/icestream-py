import asyncio
import datetime
import inspect
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from typing import Sequence

import httpx
from sqlalchemy import select, update
from sqlalchemy.orm import joinedload, selectinload

from icestream.compaction.types import (
    CompactionProcessor,
    CompactionContext,
    CompactionWritePlan,
)
from icestream.config import Config
from icestream.kafkaserver.wal import WALFile as DecodedWALFile, WALBatch
from icestream.kafkaserver.wal.serde import decode_kafka_wal_file
from icestream.models import (
    WALFile as WALFileModel,
    Topic,
    ParquetFile,
    TopicWALFile,
    TopicWALFileOffset,
    TopicWALFileSource,
)
from icestream.logger import log
from icestream.utils import normalize_object_key


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
        write_plan = CompactionWritePlan()
        had_candidates = False
        orphan_cleanup_attempted = False
        wal_models: Sequence[WALFileModel] = []

        try:
            async with self.config.async_session_factory() as session:
                async with self._session_begin(session):
                    wal_models, total_bytes = await self._select_wal_models(session)

                    if not wal_models:
                        had_candidates = False
                    else:
                        had_candidates = True

                        wal_decoded: list[DecodedWALFile] = await self._fetch_and_decode(
                            wal_models
                        )
                        parquet_candidates = await self._select_parquet_candidates(session)

                        ctx = CompactionContext(
                            config=self.config,
                            wal_models=wal_models,
                            wal_decoded=wal_decoded,
                            parquet_candidates=parquet_candidates,
                            now_monotonic=start_time,
                        )

                        for p in self.processors:
                            proc_plan = await self._build_processor_plan(p, ctx)
                            write_plan.extend(proc_plan)

                        await self._finalize_topic_wal_rows(session, write_plan)
                        await self._mark_source_wals_compacted(
                            session, write_plan, wal_models
                        )

            if had_candidates:
                log.info(
                    "compaction finalized",
                    extra={
                        "uploaded": len(write_plan.uploaded_object_keys),
                        "finalized": len(write_plan.topic_wal_files),
                        "orphan_cleanup_attempted": orphan_cleanup_attempted,
                    },
                )
        except Exception:
            orphan_cleanup_attempted = bool(write_plan.uploaded_object_keys)
            if orphan_cleanup_attempted:
                await self._cleanup_orphaned_objects(write_plan.uploaded_object_keys)
            log.exception(
                "compaction finalize failed",
                extra={
                    "uploaded": len(write_plan.uploaded_object_keys),
                    "finalized": 0,
                    "orphan_cleanup_attempted": orphan_cleanup_attempted,
                },
            )
            raise

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
            where_clauses = [WALFileModel.compacted_at.is_(None)]
            if seen_ids:
                where_clauses.append(WALFileModel.id.notin_(tuple(seen_ids)))

            limit = min(
                self.config.MAX_COMPACTION_SELECT_LIMIT,
                self.config.MAX_COMPACTION_WAL_FILES - len(selected),
            )

            id_rows = await session.scalars(
                select(WALFileModel.id)
                .where(*where_clauses)
                .order_by(WALFileModel.id)
                # Keep SKIP LOCKED for this sprint; lease-based selection is deferred.
                .with_for_update(skip_locked=True)
                .limit(limit)
            )
            ids = [wal_id for wal_id in id_rows if wal_id not in seen_ids]
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
            object_key = normalize_object_key(self.config, wal.uri)
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

    async def _build_processor_plan(
            self, processor: CompactionProcessor, ctx: CompactionContext
    ) -> CompactionWritePlan:
        if "build_plan" in type(processor).__dict__:
            return await processor.build_plan(ctx)

        # Legacy compatibility for non-topic-WAL processors that still mutate DB directly.
        if "apply" in type(processor).__dict__:
            await processor.apply(ctx)
            legacy_plan = CompactionWritePlan()
            legacy_plan.compacted_wal_ids.update(
                wm.id for wm in ctx.wal_models if wm.id is not None
            )
            return legacy_plan

        build_plan = getattr(processor, "build_plan", None)
        if callable(build_plan):
            return await build_plan(ctx)

        apply = getattr(processor, "apply", None)
        if callable(apply):
            await apply(ctx)
            legacy_plan = CompactionWritePlan()
            legacy_plan.compacted_wal_ids.update(
                wm.id for wm in ctx.wal_models if wm.id is not None
            )
            return legacy_plan

        return CompactionWritePlan()

    async def _finalize_topic_wal_rows(
            self, session, write_plan: CompactionWritePlan
    ) -> None:
        for file_plan in write_plan.topic_wal_files:
            twf = TopicWALFile(
                topic_name=file_plan.topic_name,
                uri=file_plan.uri,
                etag=file_plan.etag,
                total_bytes=file_plan.total_bytes,
                total_messages=file_plan.total_messages,
            )
            session.add(twf)
            await session.flush()

            for offset in file_plan.offsets:
                session.add(
                    TopicWALFileOffset(
                        topic_wal_file_id=twf.id,
                        topic_name=offset.topic_name,
                        partition_number=offset.partition_number,
                        base_offset=offset.base_offset,
                        last_offset=offset.last_offset,
                        byte_start=offset.byte_start,
                        byte_end=offset.byte_end,
                        min_timestamp=offset.min_timestamp,
                        max_timestamp=offset.max_timestamp,
                    )
                )
                await session.flush()

            for wal_file_id in sorted(set(file_plan.source_wal_ids)):
                session.add(
                    TopicWALFileSource(
                        topic_wal_file_id=twf.id,
                        wal_file_id=wal_file_id,
                    )
                )
                await session.flush()

    async def _mark_source_wals_compacted(
            self,
            session,
            write_plan: CompactionWritePlan,
            wal_models: Sequence[WALFileModel],
    ) -> None:
        if not write_plan.compacted_wal_ids:
            return
        now_ts = datetime.datetime.now(datetime.UTC)
        await session.execute(
            update(WALFileModel)
            .where(
                WALFileModel.id.in_(write_plan.compacted_wal_ids),
                WALFileModel.compacted_at.is_(None),
            )
            .values(compacted_at=now_ts)
        )
        compacted_ids = set(write_plan.compacted_wal_ids)
        for wal_model in wal_models:
            if wal_model.id in compacted_ids and wal_model.compacted_at is None:
                wal_model.compacted_at = now_ts

    async def _cleanup_orphaned_objects(self, object_keys: list[str]) -> None:
        keys = sorted(set(object_keys))
        if not keys:
            return
        try:
            await self.config.store.delete_async(keys)
        except Exception:
            log.exception(
                "best-effort orphan cleanup failed",
                extra={"count": len(keys)},
            )

    @staticmethod
    @asynccontextmanager
    async def _session_begin(session):
        begin = getattr(session, "begin", None)
        if (
                callable(begin)
                and not inspect.iscoroutinefunction(begin)
        ):
            tx = begin()
            if hasattr(tx, "__aenter__") and hasattr(tx, "__aexit__"):
                async with tx:
                    yield
                return
        yield

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
