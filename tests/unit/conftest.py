import datetime
from asyncio import StreamWriter, Queue
from typing import Dict
from unittest.mock import MagicMock, AsyncMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from icestream.config import Config
from icestream.db import run_migrations
from icestream.kafkaserver.server import Connection, Server
from icestream.models import Topic, Partition, WALFile, WALFileOffset, ParquetFile
from tests.utils.seed import create_wal_range, create_parquet_range
from tests.utils.time import FakeClock


@pytest.fixture
def fake_clock():
    return FakeClock()


@pytest.fixture
def mock_async_session_factory():
    session = MagicMock()
    session.add = MagicMock()
    session.flush = AsyncMock()
    session.commit = AsyncMock()

    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=session)
    cm.__aexit__ = AsyncMock(return_value=None)

    factory = MagicMock(return_value=cm)
    return factory, session


@pytest.fixture
def base_config(mock_async_session_factory):
    factory, _ = mock_async_session_factory
    cfg = MagicMock()
    cfg.WAL_BUCKET = "bucket"
    cfg.WAL_BUCKET_PREFIX = ""
    cfg.store.put_async = AsyncMock(return_value=MagicMock(etag="etag123"))
    cfg.async_session_factory = factory
    cfg.FLUSH_SIZE = 10**9
    cfg.FLUSH_INTERVAL = 5
    cfg.MAX_IN_FLIGHT_FLUSHES = 2
    cfg.FLUSH_TIMEOUT = 60.0
    cfg.BROKER_ID = "b1"
    return cfg


@pytest.fixture
def stream_writer():
    return AsyncMock(spec=StreamWriter)


@pytest.fixture
def handler(base_config):
    queue = Queue()
    handler = Connection(Server(config=base_config, queue=queue))
    return handler


@pytest.fixture
async def config():
    config = Config()
    await run_migrations(config)
    return config

def ts_ms(dtobj: datetime.datetime) -> int:
    return int(dtobj.timestamp() * 1000)

async def insert_topic_partition(
        session: AsyncSession,
        topic: str,
        partition: int,
        last_offset: int = -1,
        log_start_offset: int = 0,
):
    t = Topic(name=topic)
    p = Partition(
        topic_name=topic,
        partition_number=partition,
        last_offset=last_offset,
        log_start_offset=log_start_offset
    )
    session.add_all([t, p])
    await session.flush()

async def insert_wal_with_offsets(
        session: AsyncSession,
        topic: str,
        partition: int,
        base_offset: int,
        last_offset: int,
        uri: str,
        min_ts_ms: int | None,
        max_ts_ms: int | None,
        byte_start: int = 0,
        byte_end: int = 100,
        compacted_at: datetime.datetime | None = None
):
    wal = WALFile(
        uri=uri,
        etag=None,
        total_bytes=max(0, byte_end - byte_start),
        total_messages=(last_offset - base_offset + 1) if last_offset >= base_offset else 0,
        compacted_at=compacted_at
    )
    session.add(wal)
    await session.flush()

    wal_file_offset = WALFileOffset(
        wal_file_id=wal.id,
        topic_name=topic,
        partition_number=partition,
        base_offset=base_offset,
        last_offset=last_offset,
        byte_start=byte_start,
        byte_end=byte_end,
        min_timestamp=min_ts_ms,
        max_timestamp=max_ts_ms,
    )
    session.add(wal_file_offset)
    await session.flush()
    
async def insert_parquet_file(
    session: AsyncSession,
    topic: str,
    partition: int,
    min_off: int,
    max_off: int,
    min_ts: datetime.datetime | None,
    max_ts: datetime.datetime | None,
    uri: str,
    total_bytes: int = 123,
    row_count: int = 10,
    generation: int = 0,
) -> ParquetFile:
    pf = ParquetFile(
        topic_name=topic,
        partition_number=partition,
        uri=uri,
        total_bytes=total_bytes,
        row_count=row_count,
        min_offset=min_off,
        max_offset=max_off,
        min_timestamp=min_ts,
        max_timestamp=max_ts,
        generation=generation,
        compacted_at=None,
    )
    session.add(pf)
    await session.flush()
    return pf

@pytest.fixture
async def seeded_topics(config: Config) -> Dict[str, Dict]:
    """
    creates 4 topics with data in db and object store:
      1) mixed            : compacted wal + non-compacted wal + parquet
      2) compacted_only   : compacted wal + parquet
      3) wal_only         : non-compacted wal only
      4) empty            : no wal/parquet
    """
    base_time = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)

    async with config.async_session_factory() as session:
        # mixed
        await insert_topic_partition(session, "mixed", 0, last_offset=39, log_start_offset=0)

        # wal: compacted [0..9] (timestamps t..t+9s)
        await create_wal_range(
            config, session,
            topic="mixed", partition=0,
            base_offset=0, count=10,
            ts_start_ms=ts_ms(base_time + datetime.timedelta(seconds=0)),
            ts_step_ms=1000,
            compacted_at=base_time + datetime.timedelta(minutes=5),
        )
        # wal: non-compacted [10..19] (timestamps t+20s..t+29s) ts gap
        await create_wal_range(
            config, session,
            topic="mixed", partition=0,
            base_offset=10, count=10,
            ts_start_ms=ts_ms(base_time + datetime.timedelta(seconds=20)),
            ts_step_ms=1000,
            compacted_at=None,
        )

        # parquet: [0..19] timestamps t..t+19s
        await create_parquet_range(
            config, session,
            topic="mixed", partition=0,
            min_off=0, max_off=19,
            ts_start_ms=ts_ms(base_time),
            ts_step_ms=1000,
        )
        # parquet: [20..39] timestamps t+40s..t+59s ts gap
        await create_parquet_range(
            config, session,
            topic="mixed", partition=0,
            min_off=20, max_off=39,
            ts_start_ms=ts_ms(base_time + datetime.timedelta(seconds=40)),
            ts_step_ms=1000,
        )

        # compacted_only
        await insert_topic_partition(session, "compacted_only", 0, last_offset=49, log_start_offset=0)

        # wal: compacted [0..49] timestamps t..t+49s
        await create_wal_range(
            config, session,
            topic="compacted_only", partition=0,
            base_offset=0, count=50,
            ts_start_ms=ts_ms(base_time),
            ts_step_ms=1000,
            compacted_at=base_time + datetime.timedelta(minutes=10),
        )
        # parquet: full cover [0..49]
        await create_parquet_range(
            config, session,
            topic="compacted_only", partition=0,
            min_off=0, max_off=49,
            ts_start_ms=ts_ms(base_time),
            ts_step_ms=1000,
        )

        # wal only
        await insert_topic_partition(session, "wal_only", 0, last_offset=29, log_start_offset=0)

        # wal: non-compacted [0..14] t..t+14s
        await create_wal_range(
            config, session,
            topic="wal_only", partition=0,
            base_offset=0, count=15,
            ts_start_ms=ts_ms(base_time),
            ts_step_ms=1000,
            compacted_at=None,
        )
        # wal: non-compacted [15..29] t+30s..t+44s ts gap for boundary behavior
        await create_wal_range(
            config, session,
            topic="wal_only", partition=0,
            base_offset=15, count=15,
            ts_start_ms=ts_ms(base_time + datetime.timedelta(seconds=30)),
            ts_step_ms=1000,
            compacted_at=None,
        )

        # empty
        await insert_topic_partition(session, "empty", 0, last_offset=-1, log_start_offset=0)

        await session.commit()

    return {
        "mixed": {"topic": "mixed", "partition": 0, "base_time": base_time},
        "compacted_only": {"topic": "compacted_only", "partition": 0, "base_time": base_time},
        "wal_only": {"topic": "wal_only", "partition": 0, "base_time": base_time},
        "empty": {"topic": "empty", "partition": 0, "base_time": base_time},
    }
