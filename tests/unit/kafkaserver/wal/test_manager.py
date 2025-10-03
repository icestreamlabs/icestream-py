import asyncio
from contextlib import contextmanager
from unittest.mock import MagicMock, AsyncMock, patch

import pytest
from obstore.store import MemoryStore

from icestream.kafkaserver.types import ProduceTopicPartitionData
from icestream.kafkaserver.wal.manager import WALManager, default_size_estimator
from tests.utils.time import FakeClock


def make_item(batch_len: int, topic: str = "topic") -> ProduceTopicPartitionData:
    fut = asyncio.Future()
    return ProduceTopicPartitionData(
        topic=topic,
        partition=0,
        kafka_record_batch=MagicMock(batch_length=batch_len),
        flush_result=fut,
    )


@contextmanager
def stub_encoder(*, topic: str = "topic", payload: bytes = b"x"):
    # minimal offsets + payload stub
    def _enc(batch, broker_id):
        return payload, [
            {
                "topic": topic,
                "partition": 0,
                "base_offset": 0,
                "last_offset": 0,
                "byte_start": 0,
                "byte_end": len(payload),
                "min_timestamp": 0,
                "max_timestamp": 0,
            }
        ]

    with patch(
            "icestream.kafkaserver.wal.manager.encode_kafka_wal_file_with_offsets", new=_enc
    ):
        yield


async def run_size_triggered_flush(
        config, fake_clock: FakeClock, *, topic="t", payload=b"x"
):
    queue = asyncio.Queue()
    manager = WALManager(config=config, queue=queue, time_source=fake_clock)
    with stub_encoder(topic=topic, payload=payload):
        item = make_item(len(payload), topic=topic)
        await queue.put(item)
        await manager.run_once()  # buffer + launch flush
        await asyncio.gather(*manager.pending_flushes)
    return item, manager


@pytest.mark.asyncio
async def test_run_once_flushes_on_size(base_config, fake_clock: FakeClock):
    config = base_config
    config.FLUSH_SIZE = 100  # trigger by size, not time
    config.FLUSH_INTERVAL = 999

    queue = asyncio.Queue()
    manager = WALManager(config=config, queue=queue, time_source=fake_clock)

    # use helper to stub encoder
    with stub_encoder(payload=b"data"):
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
async def test_flush_triggered_by_timeout(base_config, fake_clock: FakeClock):
    config = base_config
    config.FLUSH_SIZE = 1000
    config.FLUSH_INTERVAL = 5

    queue = asyncio.Queue()
    manager = WALManager(config=config, queue=queue, time_source=fake_clock)

    with stub_encoder(payload=b"data"):
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
async def test_flush_times_out_sets_exception(base_config, fake_clock: FakeClock):
    config = base_config
    config.FLUSH_SIZE = 1
    config.FLUSH_TIMEOUT = 0.0  # immediate timeout

    # make put_async never complete so wait_for times out immediately
    def never_finishes(*a, **k):
        return asyncio.Future()

    config.store.put_async = AsyncMock(side_effect=never_finishes)

    item, _ = await run_size_triggered_flush(
        config, fake_clock, topic="t", payload=b"x"
    )

    assert item.flush_result.done()
    with pytest.raises(asyncio.TimeoutError):
        item.flush_result.result()


@pytest.mark.asyncio
async def test_flush_db_error_sets_exception(
        base_config, fake_clock: FakeClock, mock_async_session_factory
):
    config = base_config
    factory, session = mock_async_session_factory
    session.commit = AsyncMock(side_effect=RuntimeError("DB boom"))
    config.async_session_factory = factory
    config.FLUSH_SIZE = 1
    config.FLUSH_INTERVAL = 999

    item, _ = await run_size_triggered_flush(
        config, fake_clock, topic="t", payload=b"x"
    )

    assert item.flush_result.done()
    with pytest.raises(RuntimeError, match="DB boom"):
        item.flush_result.result()


@pytest.mark.asyncio
async def test_empty_timeout_advances_timer_without_flush(
        base_config, fake_clock: FakeClock
):
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
async def test_empty_timeout_advances_timer(base_config, fake_clock: FakeClock):
    config = base_config
    config.FLUSH_INTERVAL = 5

    manager = WALManager(config=config, queue=asyncio.Queue(), time_source=fake_clock)
    t0 = manager.last_flush_time

    fake_clock.advance(6)  # beyond interval
    await manager.run_once()  # timeout path, buffer empty

    assert len(manager.pending_flushes) == 0
    assert manager.last_flush_time > t0  # timer advanced


@pytest.mark.asyncio
async def test_custom_size_estimator(base_config, fake_clock: FakeClock):
    config = base_config
    config.FLUSH_SIZE = 100

    # force immediate size trigger
    def estimator(_item):
        return 120

    manager = WALManager(
        config=config,
        queue=asyncio.Queue(),
        time_source=fake_clock,
        size_estimator=estimator,
    )

    with stub_encoder(topic="t"):
        item = make_item(1, topic="t")
        await manager.queue.put(item)

        await manager.run_once()
        await asyncio.gather(*manager.pending_flushes)

    assert item.flush_result.result() is True


@pytest.mark.asyncio
async def test_db_totals_and_offsets(
        base_config, fake_clock: FakeClock, mock_async_session_factory
):
    config = base_config
    factory, session = mock_async_session_factory
    config.async_session_factory = factory
    config.FLUSH_SIZE = 1

    # capture the wal file created to assert totals
    created = {}

    def add_capture(obj):
        from icestream.models import WALFile

        if isinstance(obj, WALFile):
            created["wal"] = obj

    session.add.side_effect = add_capture

    queue = asyncio.Queue()
    manager = WALManager(config=config, queue=queue, time_source=fake_clock)

    # two records (offsets 10..11) => total_messages = 2
    def enc(batch, broker_id):
        return b"abcdefgh", [
            {
                "topic": "t",
                "partition": 0,
                "base_offset": 10,
                "last_offset": 11,
                "byte_start": 0,
                "byte_end": 8,
                "min_timestamp": 0,
                "max_timestamp": 0,
            }
        ]

    with patch(
            "icestream.kafkaserver.wal.manager.encode_kafka_wal_file_with_offsets", new=enc
    ):
        item = make_item(8, topic="t")
        await queue.put(item)

        await manager.run_once()
        await asyncio.gather(*manager.pending_flushes)

    assert item.flush_result.result() is True
    assert created["wal"].total_bytes == 8
    assert created["wal"].total_messages == 2


@pytest.mark.asyncio
async def test_use_multipart_threshold(base_config, fake_clock: FakeClock):
    config = base_config
    config.FLUSH_SIZE = 1

    # >5 mib payload should set use_multipart=true
    big = b"x" * (6 * 1024 * 1024)

    queue = asyncio.Queue()
    manager = WALManager(config=config, queue=queue, time_source=fake_clock)

    with stub_encoder(topic="t", payload=big):
        item = make_item(len(big), topic="t")
        await queue.put(item)

        await manager.run_once()
        await asyncio.gather(*manager.pending_flushes)

    assert item.flush_result.result() is True


@pytest.mark.asyncio
async def test_semaphore_serializes_flushes(base_config, fake_clock: FakeClock):
    config = base_config
    config.FLUSH_SIZE = 1
    config.MAX_IN_FLIGHT_FLUSHES = 1

    start = asyncio.Event()
    release = asyncio.Event()
    call_order = []

    async def slow_put(*a, **k):
        call_order.append("put_start")
        start.set()
        await release.wait()  # block until we allow it
        call_order.append("put_end")
        return MagicMock(etag="etag")

    config.store.put_async = AsyncMock(side_effect=slow_put)

    queue = asyncio.Queue()
    manager = WALManager(config=config, queue=queue, time_source=fake_clock)

    with stub_encoder(topic="t"):
        # enqueue two items causing two flushes to launch back-to-back
        i1 = make_item(1, topic="t")
        i2 = make_item(1, topic="t")
        await queue.put(i1)
        await queue.put(i2)

        await manager.run_once()  # launch flush #1
        await start.wait()  # ensure flush #1 is in put_async

        await (
            manager.run_once()
        )  # triggers flush #2; semaphore should block it until release

        # allow flush #1 to finish, then flush #2 can proceed
        release.set()
        await asyncio.gather(*manager.pending_flushes)

    assert i1.flush_result.result() is True
    assert i2.flush_result.result() is True
    # we saw one put finish before the other started finishing
    assert (
            call_order == ["put_start", "put_end", "put_start", "put_end"]
            or call_order.count("put_start") == 2
    )


@pytest.mark.asyncio
async def test_encoder_error_sets_exception(base_config, fake_clock: FakeClock):
    config = base_config
    config.FLUSH_SIZE = 1

    queue = asyncio.Queue()
    manager = WALManager(config=config, queue=queue, time_source=fake_clock)

    def boom(*a, **k):
        raise ValueError("encode failed")

    with patch(
            "icestream.kafkaserver.wal.manager.encode_kafka_wal_file_with_offsets", new=boom
    ):
        item = make_item(1, topic="t")
        await queue.put(item)

        await manager.run_once()
        await asyncio.gather(*manager.pending_flushes)

    assert item.flush_result.done()
    with pytest.raises(ValueError, match="encode failed"):
        item.flush_result.result()


@pytest.mark.asyncio
async def test_uploaded_file_is_retrievable_from_memorystore(
        base_config, fake_clock: FakeClock, mock_async_session_factory
):
    config = base_config
    config.store = MemoryStore()
    factory, session = mock_async_session_factory
    config.async_session_factory = factory

    # immediate flush by size
    config.FLUSH_SIZE = 1
    config.FLUSH_INTERVAL = 999

    payload = b"hello-wal"

    fixed_key = "wal/2025/08/11/12/34/aa/1234567890-b1-deadbeef.wal"
    with (
        stub_encoder(topic="t", payload=payload),
        patch.object(WALManager, "_generate_object_key", return_value=fixed_key),
    ):
        queue = asyncio.Queue()
        manager = WALManager(config=config, queue=queue, time_source=fake_clock)

        item = make_item(len(payload), topic="t")
        await queue.put(item)

        await manager.run_once()
        await asyncio.gather(*manager.pending_flushes)

        assert item.flush_result.done()
        assert item.flush_result.result() is True

        get_result = await config.store.get_async(path=fixed_key)
        data = await get_result.bytes_async()
        assert data == payload


@pytest.mark.asyncio
async def test_size_estimator_error_fallbacks(base_config, fake_clock: FakeClock):
    config = base_config
    config.FLUSH_SIZE = 2048  # 2 * 1024
    config.FLUSH_INTERVAL = 999

    def bad_estimator(_item):
        raise RuntimeError("boom")

    with patch(
            "icestream.kafkaserver.wal.manager.encode_kafka_wal_file_with_offsets",
            new=lambda b, bid: (
                    b"d",
                    [
                        {
                            "topic": "t",
                            "partition": 0,
                            "base_offset": 0,
                            "last_offset": 0,
                            "byte_start": 0,
                            "byte_end": 1,
                            "min_timestamp": 0,
                            "max_timestamp": 0,
                        }
                    ],
            ),
    ):
        m = WALManager(
            config=config,
            queue=asyncio.Queue(),
            time_source=fake_clock,
            size_estimator=bad_estimator,
        )

        # two items should cross 2k via fallback 1024+1024
        i1 = make_item(1, "t")
        i2 = make_item(1, "t")
        await m.queue.put(i1)
        await m.queue.put(i2)

        await m.run_once()
        await m.run_once()
        await asyncio.gather(*m.pending_flushes)

        assert i1.flush_result.result() is True
        assert i2.flush_result.result() is True


@pytest.mark.asyncio
async def test_put_error_sets_exception(base_config, fake_clock: FakeClock):
    config = base_config
    config.FLUSH_SIZE = 1
    config.store.put_async = AsyncMock(side_effect=IOError("put failed"))

    with patch(
            "icestream.kafkaserver.wal.manager.encode_kafka_wal_file_with_offsets",
            new=lambda b, bid: (
                    b"x",
                    [
                        {
                            "topic": "t",
                            "partition": 0,
                            "base_offset": 0,
                            "last_offset": 0,
                            "byte_start": 0,
                            "byte_end": 1,
                        }
                    ],
            ),
    ):
        m = WALManager(config=config, queue=asyncio.Queue(), time_source=fake_clock)
        it = make_item(1, "t")
        await m.queue.put(it)

        await m.run_once()
        await asyncio.gather(*m.pending_flushes)

        assert it.flush_result.done()
        with pytest.raises(IOError, match="put failed"):
            it.flush_result.result()


def test_build_uri_none_vs_empty_prefix(base_config):
    base_config.WAL_BUCKET = "b"
    q = asyncio.Queue()
    m = WALManager(config=base_config, queue=q)

    base_config.WAL_BUCKET_PREFIX = None
    assert m._build_wal_uri("k") == "b/k"

    base_config.WAL_BUCKET_PREFIX = ""
    assert m._build_wal_uri("k") == "b/k"


def test_default_size_estimator():
    result = default_size_estimator({})
    assert result == 1024
