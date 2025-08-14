import datetime
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from icestream.compaction import CompactorWorker
from icestream.compaction.types import CompactionProcessor, CompactionContext
from icestream.models import WALFile
from tests.utils.time import FakeClock


class DummyProcessor(CompactionProcessor):
    def __init__(self):
        self.called = False
        self.ctx = None

    async def apply(self, ctx: CompactionContext) -> None:
        self.called = True
        self.ctx = ctx


@pytest.mark.asyncio
async def test_run_once_selects_and_processes_wal(monkeypatch):
    mock_config = MagicMock()
    mock_config.COMPACTION_INTERVAL = 0
    mock_config.MAX_COMPACTION_SELECT_LIMIT = 10
    mock_config.MAX_COMPACTION_WAL_FILES = 5
    mock_config.MAX_COMPACTION_BYTES = 10_000_000

    fake_wal = MagicMock(spec=WALFile)
    fake_wal.total_bytes = 1000
    fake_wal.compacted_at = None
    fake_wal.uri = "s3://test/wal1"
    fake_wal.id = 123
    mock_config.store.get_async = AsyncMock(
        return_value=MagicMock(bytes=MagicMock(return_value=b"mock-data"))
    )

    monkeypatch.setattr(
        "icestream.compaction.decode_kafka_wal_file",
        lambda data: MagicMock(id=None, batches=[]),
    )

    mock_session = MagicMock()
    mock_session.execute = AsyncMock(
        return_value=MagicMock(scalars=MagicMock(return_value=[fake_wal]))
    )
    mock_session.commit = AsyncMock()

    mock_config.async_session_factory = MagicMock(
        return_value=MagicMock(
            __aenter__=AsyncMock(return_value=mock_session),
            __aexit__=AsyncMock(),
        )
    )

    worker = CompactorWorker(mock_config, processors=[DummyProcessor()])
    worker._select_wal_models = AsyncMock(
        return_value=([fake_wal], fake_wal.total_bytes)
    )
    worker._fetch_and_decode = AsyncMock(
        return_value=[MagicMock(id=fake_wal.id, batches=[])]
    )
    worker._select_parquet_candidates = AsyncMock(return_value={})

    await worker.run_once()

    assert fake_wal.compacted_at is not None
    mock_session.commit.assert_called_once()
    worker._select_wal_models.assert_called_once()
    worker._fetch_and_decode.assert_called_once()
    worker._select_parquet_candidates.assert_called_once()


@pytest.mark.asyncio
async def test_no_wal_files_skips_processing(monkeypatch):
    mock_config = MagicMock()
    mock_config.COMPACTION_INTERVAL = 0
    mock_config.async_session_factory = MagicMock(
        return_value=MagicMock(
            __aenter__=AsyncMock(),
            __aexit__=AsyncMock(),
        )
    )

    worker = CompactorWorker(mock_config, processors=[DummyProcessor()])
    worker._select_wal_models = AsyncMock(return_value=([], 0))
    worker._fetch_and_decode = AsyncMock()
    worker._select_parquet_candidates = AsyncMock(return_value={})

    await worker.run_once()

    worker._fetch_and_decode.assert_not_called()


@pytest.mark.asyncio
async def test_multiple_processors_called(monkeypatch):
    mock_config = MagicMock()
    mock_config.COMPACTION_INTERVAL = 0
    mock_config.async_session_factory = MagicMock(
        return_value=MagicMock(
            __aenter__=AsyncMock(),
            __aexit__=AsyncMock(),
        )
    )

    fake_wal = MagicMock(spec=WALFile)
    fake_wal.total_bytes = 1000
    fake_wal.compacted_at = None
    fake_wal.uri = "s3://test/wal1"
    fake_wal.id = 123

    dummy1 = DummyProcessor()
    dummy2 = DummyProcessor()

    worker = CompactorWorker(mock_config, processors=[dummy1, dummy2])

    worker._select_wal_models = AsyncMock(
        return_value=([fake_wal], fake_wal.total_bytes)
    )
    worker._fetch_and_decode = AsyncMock(
        return_value=[MagicMock(id=fake_wal.id, batches=[])]
    )
    worker._select_parquet_candidates = AsyncMock(return_value={})

    await worker.run_once()

    assert dummy1.called
    assert dummy2.called


@pytest.mark.asyncio
async def test_run_once_respects_sleep():
    fake_clock = FakeClock(start=1000.0)

    config = MagicMock()
    config.COMPACTION_INTERVAL = 5

    mock_session = MagicMock()
    session_cm = MagicMock()
    session_cm.__aenter__ = AsyncMock(return_value=mock_session)
    session_cm.__aexit__ = AsyncMock()
    config.async_session_factory = MagicMock(return_value=session_cm)

    processor = DummyProcessor()
    worker = CompactorWorker(config, [processor], time_source=fake_clock)

    async def fake_select_wal_models(*_):
        fake_clock.advance(1.0)
        return [], 0

    worker._select_wal_models = AsyncMock(side_effect=fake_select_wal_models)
    worker._fetch_and_decode = AsyncMock()
    worker._select_parquet_candidates = AsyncMock()

    with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        await worker.run_once()
        mock_sleep.assert_awaited_with(4)  # 5s interval - 1s elapsed


@pytest.mark.asyncio
async def test_compacted_at_only_set_if_none():
    config = MagicMock()
    config.COMPACTION_INTERVAL = 0
    config.async_session_factory = MagicMock(
        return_value=MagicMock(__aenter__=AsyncMock(), __aexit__=AsyncMock())
    )

    old_time = datetime.datetime(2022, 1, 1, tzinfo=datetime.UTC)
    new_time = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)

    wal1 = MagicMock(spec=WALFile, compacted_at=None)
    wal2 = MagicMock(spec=WALFile, compacted_at=old_time)
    for wal in (wal1, wal2):
        wal.total_bytes = 100
        wal.uri = "s3://x"
        wal.id = 1

    processor = DummyProcessor()
    worker = CompactorWorker(config, [processor])

    worker._select_wal_models = AsyncMock(return_value=([wal1, wal2], 200))
    worker._fetch_and_decode = AsyncMock(return_value=[MagicMock(id=1)])
    worker._select_parquet_candidates = AsyncMock(return_value={})

    with patch("icestream.compaction.datetime") as mock_dt:
        mock_dt.datetime.now.return_value = new_time
        mock_dt.datetime.UTC = datetime.UTC

        await worker.run_once()

    assert wal1.compacted_at == new_time
    assert wal2.compacted_at == old_time  # unchanged


def test_maybe_pick_bucket_triggers_on_size():
    now = datetime.datetime.now(datetime.UTC)
    files = []
    for i in range(3):
        f = MagicMock()
        f.total_bytes = 100
        f.created_at = now - datetime.timedelta(seconds=500)
        f.updated_at = None
        files.append(f)

    out = {}
    CompactorWorker._maybe_pick_bucket(
        out=out,
        key=("topic", 0),
        files=files,
        now=now,
        target_bytes=200,  # triggers after 2 files
        min_files=2,
        max_files=3,
        force_age_sec=1000,
    )

    assert ("topic", 0) in out
    assert len(out[("topic", 0)]) == 2


def test_maybe_pick_bucket_triggers_on_age():
    now = datetime.datetime.now(datetime.UTC)
    files = []
    for i in range(5):
        f = MagicMock()
        f.total_bytes = 50
        f.created_at = now - datetime.timedelta(seconds=2000)
        f.updated_at = None
        files.append(f)

    out = {}
    CompactorWorker._maybe_pick_bucket(
        out=out,
        key=("topic", 0),
        files=files,
        now=now,
        target_bytes=10000,  # never reached
        min_files=3,
        max_files=4,
        force_age_sec=1000,
    )

    assert ("topic", 0) in out
    assert len(out[("topic", 0)]) == 4  # max_files


@pytest.mark.asyncio
async def test_fetch_and_decode_handles_exception():
    config = MagicMock()
    config.async_session_factory = MagicMock(
        return_value=MagicMock(__aenter__=AsyncMock(), __aexit__=AsyncMock())
    )
    config.COMPACTION_INTERVAL = 0

    wal = MagicMock(spec=WALFile)
    wal.total_bytes = 100
    wal.uri = "s3://missing"
    wal.id = 1

    config.store.get_async = AsyncMock(side_effect=Exception("fetch failed"))

    worker = CompactorWorker(config, processors=[])

    with pytest.raises(Exception, match="fetch failed"):
        await worker._fetch_and_decode([wal])


@pytest.mark.asyncio
async def test_parquet_candidates_grouped_by_partition():
    now = datetime.datetime.now(datetime.UTC)

    pf1 = MagicMock(
        topic_name="t",
        partition_number=0,
        created_at=now - datetime.timedelta(seconds=1200),
        total_bytes=100,
    )
    pf2 = MagicMock(
        topic_name="t",
        partition_number=0,
        created_at=now - datetime.timedelta(seconds=1100),
        total_bytes=100,
    )
    pf3 = MagicMock(
        topic_name="t",
        partition_number=1,
        created_at=now - datetime.timedelta(seconds=500),
        total_bytes=100,
    )

    # Prepare mock scalars().all()
    scalars_mock = MagicMock()
    scalars_mock.all.return_value = [pf1, pf2, pf3]

    # Prepare mock session.execute()
    execute_mock = MagicMock()
    execute_mock.scalars.return_value = scalars_mock

    mock_session = MagicMock()
    mock_session.execute = AsyncMock(return_value=execute_mock)

    config = MagicMock()
    config.PARQUET_COMPACTION_TARGET_BYTES = 150
    config.PARQUET_COMPACTION_MIN_INPUT_FILES = 2
    config.PARQUET_COMPACTION_MAX_INPUT_FILES = 3
    config.PARQUET_COMPACTION_FORCE_AGE_SEC = 1000

    worker = CompactorWorker(config, processors=[])
    result = await worker._select_parquet_candidates(mock_session)

    # pf1 + pf2 should be selected by size
    assert ("t", 0) in result
    assert ("t", 1) not in result


@pytest.mark.asyncio
async def test_parquet_candidates_triggered_by_size():
    now = datetime.datetime.now(datetime.UTC)

    # These two files exceed target bytes and meet min file count
    pf1 = MagicMock(
        topic_name="topic", partition_number=0, total_bytes=80, created_at=now
    )
    pf2 = MagicMock(
        topic_name="topic", partition_number=0, total_bytes=80, created_at=now
    )

    scalars_mock = MagicMock()
    scalars_mock.all.return_value = [pf1, pf2]

    execute_mock = MagicMock()
    execute_mock.scalars.return_value = scalars_mock

    mock_session = MagicMock()
    mock_session.execute = AsyncMock(return_value=execute_mock)

    config = MagicMock()
    config.PARQUET_COMPACTION_TARGET_BYTES = 100  # Trigger at 100
    config.PARQUET_COMPACTION_MIN_INPUT_FILES = 2
    config.PARQUET_COMPACTION_MAX_INPUT_FILES = 10
    config.PARQUET_COMPACTION_FORCE_AGE_SEC = 10000  # So age won't trigger

    worker = CompactorWorker(config, processors=[])
    result = await worker._select_parquet_candidates(mock_session)

    assert ("topic", 0) in result
    assert result[("topic", 0)] == [pf1, pf2]


@pytest.mark.asyncio
async def test_parquet_candidates_triggered_by_age():
    now = datetime.datetime.now(datetime.UTC)

    pf1 = MagicMock(
        topic_name="topic",
        partition_number=0,
        total_bytes=10,
        created_at=now - datetime.timedelta(seconds=2000),
    )
    pf2 = MagicMock(
        topic_name="topic",
        partition_number=0,
        total_bytes=10,
        created_at=now - datetime.timedelta(seconds=1800),
    )
    pf3 = MagicMock(
        topic_name="topic",
        partition_number=0,
        total_bytes=10,
        created_at=now - datetime.timedelta(seconds=1600),
    )

    scalars_mock = MagicMock()
    scalars_mock.all.return_value = [pf1, pf2, pf3]

    execute_mock = MagicMock()
    execute_mock.scalars.return_value = scalars_mock

    mock_session = MagicMock()
    mock_session.execute = AsyncMock(return_value=execute_mock)

    config = MagicMock()
    config.PARQUET_COMPACTION_TARGET_BYTES = 1000  # Unreachable
    config.PARQUET_COMPACTION_MIN_INPUT_FILES = 3
    config.PARQUET_COMPACTION_MAX_INPUT_FILES = 4
    config.PARQUET_COMPACTION_FORCE_AGE_SEC = 1000  # All are old

    worker = CompactorWorker(config, processors=[])
    result = await worker._select_parquet_candidates(mock_session)

    assert ("topic", 0) in result
    assert result[("topic", 0)] == [pf1, pf2, pf3]


@pytest.mark.asyncio
async def test_fetch_and_decode_raises_on_failure():
    config = MagicMock()
    config.store.get_async = AsyncMock(side_effect=Exception("fetch error"))

    wal = MagicMock(uri="s3://bad/path", id=1)
    worker = CompactorWorker(config, processors=[])

    with pytest.raises(Exception, match="fetch error"):
        await worker._fetch_and_decode([wal])


@pytest.mark.asyncio
async def test_parquet_candidates_grouped_by_topic_and_partition():
    now = datetime.datetime.now(datetime.UTC)

    pf1 = MagicMock(
        topic_name="t",
        partition_number=0,
        total_bytes=100,
        created_at=now - datetime.timedelta(seconds=1200),
    )
    pf2 = MagicMock(
        topic_name="t",
        partition_number=0,
        total_bytes=100,
        created_at=now - datetime.timedelta(seconds=1100),
    )
    pf3 = MagicMock(
        topic_name="t",
        partition_number=1,
        total_bytes=100,
        created_at=now - datetime.timedelta(seconds=1200),
    )
    pf4 = MagicMock(
        topic_name="t",
        partition_number=1,
        total_bytes=100,
        created_at=now - datetime.timedelta(seconds=1100),
    )

    scalars_mock = MagicMock()
    scalars_mock.all.return_value = [pf1, pf2, pf3, pf4]

    execute_mock = MagicMock()
    execute_mock.scalars.return_value = scalars_mock

    mock_session = MagicMock()
    mock_session.execute = AsyncMock(return_value=execute_mock)

    config = MagicMock()
    config.PARQUET_COMPACTION_TARGET_BYTES = 150
    config.PARQUET_COMPACTION_MIN_INPUT_FILES = 2
    config.PARQUET_COMPACTION_MAX_INPUT_FILES = 10
    config.PARQUET_COMPACTION_FORCE_AGE_SEC = 1000

    worker = CompactorWorker(config, processors=[])
    result = await worker._select_parquet_candidates(mock_session)

    assert ("t", 0) in result
    assert ("t", 1) in result
    assert result[("t", 0)] == [pf1, pf2]
    assert result[("t", 1)] == [pf3, pf4]


def test_maybe_pick_bucket_noop_on_empty():
    out = {}
    CompactorWorker._maybe_pick_bucket(
        out=out,
        key=("k", 0),
        files=[],
        now=datetime.datetime.now(datetime.UTC),
        target_bytes=100,
        min_files=2,
        max_files=5,
        force_age_sec=1000,
    )
    assert out == {}


def test_maybe_pick_bucket_does_not_trigger_if_too_few_files():
    now = datetime.datetime.now(datetime.UTC)
    pf = MagicMock(total_bytes=500, created_at=now - datetime.timedelta(seconds=2000))

    out = {}
    CompactorWorker._maybe_pick_bucket(
        out=out,
        key=("t", 0),
        files=[pf],
        now=now,
        target_bytes=100,
        min_files=2,
        max_files=5,
        force_age_sec=1000,
    )
    assert out == {}
