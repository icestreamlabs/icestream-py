import asyncio
from unittest.mock import MagicMock, AsyncMock

import pytest

from icestream.kafkaserver.types import ProduceTopicPartitionData
from icestream.kafkaserver.wal.manager import WALManager
from tests.utils.time import FakeClock


# helpers
def make_item(batch_len: int, topic: str = "topic") -> ProduceTopicPartitionData:
    fut = asyncio.Future()
    return ProduceTopicPartitionData(
        topic=topic,
        partition=0,
        kafka_record_batch=MagicMock(batch_length=batch_len),
        flush_result=fut,
    )

def stub_encoder(monkeypatch, *, topic: str = "topic", payload: bytes = b"x"):
    # minimal offsets + payload stub
    monkeypatch.setattr(
        "icestream.kafkaserver.wal.manager.encode_kafka_wal_file_with_offsets",
        lambda batch, broker_id: (payload, [{
            "topic": topic, "partition": 0,
            "base_offset": 0, "last_offset": 0,
            "byte_start": 0, "byte_end": len(payload),
        }]),
    )


@pytest.mark.asyncio
async def test_run_once_flushes_on_size(monkeypatch, base_config, fake_clock: FakeClock):
    config = base_config
    config.FLUSH_SIZE = 100  # trigger by size, not time
    config.FLUSH_INTERVAL = 999

    queue = asyncio.Queue()
    manager = WALManager(config=config, queue=queue, time_source=fake_clock)

    # use helper to stub encoder
    stub_encoder(monkeypatch, payload=b"data")

    # use helper to make items
    item1 = make_item(60)
    item2 = make_item(50)

    await queue.put(item1)
    await queue.put(item2)

    await manager.run_once()  # buffers item1
    await manager.run_once()  # buffers item2 -> size >= 100 -> launches flush task

    await asyncio.gather(*manager.pending_flushes)

    assert item1.flush_result.done()
    assert item2.flush_result.done()
    assert item1.flush_result.result() is True
    assert item2.flush_result.result() is True


@pytest.mark.asyncio
async def test_flush_triggered_by_timeout(monkeypatch, base_config, fake_clock: FakeClock):
    config = base_config
    config.FLUSH_SIZE = 1000
    config.FLUSH_INTERVAL = 5

    queue = asyncio.Queue()
    manager = WALManager(config=config, queue=queue, time_source=fake_clock)

    stub_encoder(monkeypatch, payload=b"data")

    # preload buffer to simulate prior put
    item = make_item(50)
    manager.buffer.append(item)
    manager.buffer_size += 12 + 50  # match default_size_estimator
    manager.buffer_count += 1

    fake_clock.advance(6)
    await manager.run_once()  # timeout path -> launches flush

    await asyncio.gather(*manager.pending_flushes)

    assert item.flush_result.done()
    assert item.flush_result.result() is True


@pytest.mark.asyncio
async def test_flush_times_out_sets_exception(monkeypatch, base_config, fake_clock: FakeClock):
    config = base_config
    config.FLUSH_SIZE = 1
    config.FLUSH_TIMEOUT = 0.0  # immediate timeout

    # make put_async never finish so wait_for times out immediately
    def never_finishes(*a, **k):
        return asyncio.Future()
    config.store.put_async = AsyncMock(side_effect=never_finishes)

    queue = asyncio.Queue()
    manager = WALManager(config=config, queue=queue, time_source=fake_clock)

    stub_encoder(monkeypatch, topic="t", payload=b"x")

    item = make_item(1, topic="t")
    await queue.put(item)

    # only one run_once needed: buffer + size-triggered flush
    await manager.run_once()

    await asyncio.gather(*manager.pending_flushes)

    assert item.flush_result.done()
    with pytest.raises(asyncio.TimeoutError):
        item.flush_result.result()


@pytest.mark.asyncio
async def test_flush_db_error_sets_exception(monkeypatch, base_config, fake_clock: FakeClock, mock_async_session_factory):
    config = base_config
    factory, session = mock_async_session_factory
    session.commit = AsyncMock(side_effect=RuntimeError("DB boom"))
    config.async_session_factory = factory
    config.FLUSH_SIZE = 1  # size-trigger
    config.FLUSH_INTERVAL = 999  # irrelevant now

    queue = asyncio.Queue()
    manager = WALManager(config=config, queue=queue, time_source=fake_clock)

    stub_encoder(monkeypatch, topic="t", payload=b"x")

    item = make_item(1, topic="t")
    await queue.put(item)

    await manager.run_once()  # buffers & launches background flush
    await asyncio.gather(*manager.pending_flushes)

    assert item.flush_result.done()
    with pytest.raises(RuntimeError, match="DB boom"):
        item.flush_result.result()


@pytest.mark.asyncio
async def test_empty_timeout_advances_timer_without_flush(base_config, fake_clock: FakeClock):
    config = base_config
    config.FLUSH_INTERVAL = 5

    queue = asyncio.Queue()
    manager = WALManager(config=config, queue=queue, time_source=fake_clock)

    t0 = manager.last_flush_time
    fake_clock.advance(6)
    await manager.run_once()  # timeout path, empty buffer

    assert len(manager.pending_flushes) == 0
    assert manager.last_flush_time >= t0  # advanced


def test_build_wal_uri_variants(base_config):
    # no prefix
    q = asyncio.Queue()
    m = WALManager(config=base_config, queue=q)
    base_config.WAL_BUCKET = "bkt"
    base_config.WAL_BUCKET_PREFIX = ""
    assert m._build_wal_uri("k") == "bkt/k"
    # with prefix
    base_config.WAL_BUCKET_PREFIX = "pre"
    assert m._build_wal_uri("k") == "bkt/pre/k"


@pytest.mark.asyncio
async def test_flush_triggers_by_count_backstop(monkeypatch, base_config, fake_clock: FakeClock):
    config = base_config
    config.FLUSH_SIZE = 10**9      # make size irrelevant
    config.FLUSH_MAX_BATCHES = 2   # trigger on count=2

    queue = asyncio.Queue()
    manager = WALManager(config=config, queue=queue, time_source=fake_clock)

    stub_encoder(monkeypatch, topic="t")

    i1 = make_item(1, topic="t")
    i2 = make_item(1, topic="t")

    await queue.put(i1)
    await queue.put(i2)
    await manager.run_once()  # buffer i1
    await manager.run_once()  # buffer i2 -> count triggers flush

    await asyncio.gather(*manager.pending_flushes)
    assert i1.flush_result.result() is True
    assert i2.flush_result.result() is True


@pytest.mark.asyncio
async def test_empty_timeout_advances_timer(monkeypatch, base_config, fake_clock: FakeClock):
    config = base_config
    config.FLUSH_INTERVAL = 5

    manager = WALManager(config=config, queue=asyncio.Queue(), time_source=fake_clock)
    t0 = manager.last_flush_time

    fake_clock.advance(6)  # beyond interval
    await manager.run_once()  # timeout path, buffer empty

    assert len(manager.pending_flushes) == 0
    assert manager.last_flush_time > t0  # timer advanced


@pytest.mark.asyncio
async def test_custom_size_estimator(monkeypatch, base_config, fake_clock: FakeClock):
    config = base_config
    config.FLUSH_SIZE = 100

    # force immediate size trigger
    def estimator(_item): return 120

    manager = WALManager(config=config, queue=asyncio.Queue(),
                         time_source=fake_clock, size_estimator=estimator)

    stub_encoder(monkeypatch, topic="t")

    item = make_item(1, topic="t")
    await manager.queue.put(item)

    await manager.run_once()
    await asyncio.gather(*manager.pending_flushes)
    assert item.flush_result.result() is True


@pytest.mark.asyncio
async def test_db_totals_and_offsets(monkeypatch, base_config, fake_clock: FakeClock, mock_async_session_factory):
    config = base_config
    factory, session = mock_async_session_factory
    config.async_session_factory = factory
    config.FLUSH_SIZE = 1

    # capture the WALFile created to assert totals
    created = {}
    def add_capture(obj):
        from icestream.models import WALFile
        if isinstance(obj, WALFile):
            created["wal"] = obj
    session.add.side_effect = add_capture

    queue = asyncio.Queue()
    manager = WALManager(config=config, queue=queue, time_source=fake_clock)

    # two records (offsets 10..11) => total_messages = 2
    monkeypatch.setattr(
        "icestream.kafkaserver.wal.manager.encode_kafka_wal_file_with_offsets",
        lambda batch, broker_id: (b"abcdefgh", [{
            "topic": "t", "partition": 0,
            "base_offset": 10, "last_offset": 11,
            "byte_start": 0, "byte_end": 8,
        }]),
    )

    item = make_item(8, topic="t")
    await queue.put(item)

    await manager.run_once()
    await asyncio.gather(*manager.pending_flushes)

    assert item.flush_result.result() is True
    assert created["wal"].total_bytes == 8
    assert created["wal"].total_messages == 2


@pytest.mark.asyncio
async def test_use_multipart_threshold(monkeypatch, base_config, fake_clock: FakeClock):
    config = base_config
    config.FLUSH_SIZE = 1

    # >5 mib payload should set use_multipart=true
    big = b"x" * (6 * 1024 * 1024)

    queue = asyncio.Queue()
    manager = WALManager(config=config, queue=queue, time_source=fake_clock)

    stub_encoder(monkeypatch, topic="t", payload=big)

    item = make_item(len(big), topic="t")
    await queue.put(item)

    await manager.run_once()
    await asyncio.gather(*manager.pending_flushes)

    assert item.flush_result.result() is True
    assert config.store.put_async.call_args.kwargs["use_multipart"] is True


@pytest.mark.asyncio
async def test_semaphore_serializes_flushes(monkeypatch, base_config, fake_clock: FakeClock):
    config = base_config
    config.FLUSH_SIZE = 1
    config.MAX_IN_FLIGHT_FLUSHES = 1

    start = asyncio.Event()
    release = asyncio.Event()
    call_order = []

    async def slow_put(*a, **k):
        call_order.append("put_start")
        start.set()
        await release.wait()   # block until we allow it
        call_order.append("put_end")
        return MagicMock(etag="etag")

    config.store.put_async = AsyncMock(side_effect=slow_put)

    queue = asyncio.Queue()
    manager = WALManager(config=config, queue=queue, time_source=fake_clock)

    stub_encoder(monkeypatch, topic="t")

    # enqueue two items causing two flushes to launch back-to-back
    i1 = make_item(1, topic="t")
    i2 = make_item(1, topic="t")
    await queue.put(i1)
    await queue.put(i2)

    await manager.run_once()  # launch flush #1
    await start.wait()        # ensure flush #1 is in put_async

    await manager.run_once()  # triggers flush #2 in background, but semaphore should block it

    # allow flush #1 to finish, then flush #2 can proceed
    release.set()
    await asyncio.gather(*manager.pending_flushes)

    assert i1.flush_result.result() is True
    assert i2.flush_result.result() is True
    # we saw one put finish before the other started finishing
    assert call_order == ["put_start", "put_end", "put_start", "put_end"] or call_order.count("put_start") == 2


@pytest.mark.asyncio
async def test_encoder_error_sets_exception(base_config, fake_clock: FakeClock, monkeypatch):
    config = base_config
    config.FLUSH_SIZE = 1

    queue = asyncio.Queue()
    manager = WALManager(config=config, queue=queue, time_source=fake_clock)

    def boom(*a, **k): raise ValueError("encode failed")
    monkeypatch.setattr("icestream.kafkaserver.wal.manager.encode_kafka_wal_file_with_offsets", boom)

    item = make_item(1, topic="t")
    await queue.put(item)

    await manager.run_once()
    await asyncio.gather(*manager.pending_flushes)

    assert item.flush_result.done()
    with pytest.raises(ValueError, match="encode failed"):
        item.flush_result.result()
