import datetime

import pytest
from sqlalchemy import select

from icestream.compaction import CompactorWorker
from icestream.compaction.wal_to_topic_wal import WalToTopicWalProcessor
from icestream.kafkaserver.wal.serde import decode_kafka_wal_file
from icestream.models import (
    Partition,
    TopicWALFile,
    TopicWALFileOffset,
    TopicWALFileSource,
    WALFile,
)
from icestream.utils import normalize_object_key
from tests.unit.conftest import insert_topic_partition
from tests.utils.seed import create_wal_range


@pytest.mark.asyncio
async def test_topic_wal_compaction_writes_single_topic_wal1_and_lineage(config):
    config.COMPACTION_INTERVAL = 0
    config.MAX_COMPACTION_SELECT_LIMIT = 2
    config.MAX_COMPACTION_WAL_FILES = 2
    config.MAX_COMPACTION_BYTES = 10_000_000
    config.TOPIC_WAL_TARGET_BYTES = 10_000_000
    config.TOPIC_WAL_PREFIX = "topic_wal"

    base_time = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)

    async with config.async_session_factory() as session:
        await insert_topic_partition(session, "orders", 0, last_offset=4)
        session.add(
            Partition(
                topic_name="orders",
                partition_number=1,
                last_offset=4,
                log_start_offset=0,
            )
        )
        await session.flush()

        wal_a, _ = await create_wal_range(
            config,
            session,
            topic="orders",
            partition=0,
            base_offset=0,
            count=5,
            ts_start_ms=int(base_time.timestamp() * 1000),
            compacted_at=None,
        )
        wal_b, _ = await create_wal_range(
            config,
            session,
            topic="orders",
            partition=1,
            base_offset=0,
            count=5,
            ts_start_ms=int(base_time.timestamp() * 1000),
            compacted_at=None,
        )
        source_ids = {wal_a.id, wal_b.id}
        await session.commit()

    worker = CompactorWorker(config, [WalToTopicWalProcessor()])
    await worker.run_once()

    async with config.async_session_factory() as session:
        topic_files = (
            await session.execute(select(TopicWALFile).order_by(TopicWALFile.id))
        ).scalars().all()
        assert topic_files
        assert {row.topic_name for row in topic_files} == {"orders"}

        offsets = (
            await session.execute(
                select(TopicWALFileOffset).order_by(
                    TopicWALFileOffset.topic_wal_file_id,
                    TopicWALFileOffset.partition_number,
                    TopicWALFileOffset.base_offset,
                )
            )
        ).scalars().all()
        assert offsets
        assert {row.topic_name for row in offsets} == {"orders"}

        lineage = (
            await session.execute(select(TopicWALFileSource))
        ).scalars().all()
        assert lineage
        assert {row.wal_file_id for row in lineage} == source_ids

        source_rows = (
            await session.execute(select(WALFile).where(WALFile.id.in_(source_ids)))
        ).scalars().all()
        assert source_rows
        assert all(row.compacted_at is not None for row in source_rows)

    for topic_file in topic_files:
        get_result = await config.store.get_async(
            normalize_object_key(config, topic_file.uri)
        )
        payload = await get_result.bytes_async()
        decoded = decode_kafka_wal_file(bytes(payload))
        assert decoded.version == 1
        assert decoded.batches
        assert {batch.topic for batch in decoded.batches} == {"orders"}


@pytest.mark.asyncio
async def test_topic_wal_compaction_preserves_partition_offset_order(config):
    config.COMPACTION_INTERVAL = 0
    config.MAX_COMPACTION_SELECT_LIMIT = 2
    config.MAX_COMPACTION_WAL_FILES = 2
    config.MAX_COMPACTION_BYTES = 10_000_000
    config.TOPIC_WAL_TARGET_BYTES = 256
    config.TOPIC_WAL_PREFIX = "topic_wal"

    base_time = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)

    async with config.async_session_factory() as session:
        await insert_topic_partition(session, "events", 0, last_offset=19)

        await create_wal_range(
            config,
            session,
            topic="events",
            partition=0,
            base_offset=0,
            count=10,
            ts_start_ms=int(base_time.timestamp() * 1000),
            compacted_at=None,
        )
        await create_wal_range(
            config,
            session,
            topic="events",
            partition=0,
            base_offset=10,
            count=10,
            ts_start_ms=int(base_time.timestamp() * 1000) + 10_000,
            compacted_at=None,
        )
        await session.commit()

    worker = CompactorWorker(config, [WalToTopicWalProcessor()])
    await worker.run_once()

    async with config.async_session_factory() as session:
        offsets = (
            await session.execute(
                select(TopicWALFileOffset)
                .where(
                    TopicWALFileOffset.topic_name == "events",
                    TopicWALFileOffset.partition_number == 0,
                )
                .order_by(TopicWALFileOffset.base_offset)
            )
        ).scalars().all()

    assert offsets
    for idx in range(len(offsets) - 1):
        assert offsets[idx + 1].base_offset > offsets[idx].last_offset
