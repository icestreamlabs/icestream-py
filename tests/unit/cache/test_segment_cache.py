import asyncio
from dataclasses import dataclass

import pytest

from icestream.cache.object_reads import read_object_bytes
from icestream.cache.read_through import ReadThroughSegmentCache
from icestream.cache.segment_cache import SegmentCacheKey


class _MemoryBackend:
    def __init__(self):
        self.values: dict[str, bytes] = {}
        self.get_calls = 0
        self.put_calls = 0
        self.remove_calls = 0

    async def get(self, key: str) -> bytes | None:
        self.get_calls += 1
        return self.values.get(key)

    async def put(self, key: str, value: bytes) -> None:
        self.put_calls += 1
        self.values[key] = value

    async def remove(self, key: str) -> None:
        self.remove_calls += 1
        self.values.pop(key, None)

    async def close(self) -> None:
        return None


class _FailingBackend:
    async def get(self, key: str) -> bytes | None:
        raise RuntimeError("cache get failed")

    async def put(self, key: str, value: bytes) -> None:
        raise RuntimeError("cache put failed")

    async def remove(self, key: str) -> None:
        raise RuntimeError("cache remove failed")

    async def close(self) -> None:
        return None


class _SlowBackend(_MemoryBackend):
    async def get(self, key: str) -> bytes | None:
        await asyncio.sleep(0.05)
        return await super().get(key)

    async def put(self, key: str, value: bytes) -> None:
        await asyncio.sleep(0.05)
        await super().put(key, value)


@dataclass
class _RangeStore:
    payload: bytes
    full_reads: int = 0
    range_reads: int = 0

    async def get_async(self, object_key: str):
        self.full_reads += 1

        class _Result:
            def __init__(self, data: bytes):
                self._data = data

            async def bytes_async(self):
                return self._data

        return _Result(self.payload)

    async def get_range_async(self, object_key: str, *, start: int, length: int):
        self.range_reads += 1
        return self.payload[start : start + length]


@dataclass
class _ConfigShim:
    store: _RangeStore
    segment_object_reader: ReadThroughSegmentCache | None
    WAL_BUCKET: str = "bucket"
    WAL_BUCKET_PREFIX: str | None = None

    async def get_segment_object_reader(self):
        return self.segment_object_reader


def test_segment_cache_key_is_deterministic_and_range_aware():
    a = SegmentCacheKey(
        object_key="topic_wal/a.wal",
        byte_start=10,
        byte_end=100,
        version_token="etag-1",
    )
    b = SegmentCacheKey(
        object_key="topic_wal/a.wal",
        byte_start=10,
        byte_end=100,
        version_token="etag-1",
    )
    c = SegmentCacheKey(
        object_key="topic_wal/a.wal",
        byte_start=100,
        byte_end=200,
        version_token="etag-1",
    )
    d = SegmentCacheKey(
        object_key="topic_wal/a.wal",
        byte_start=10,
        byte_end=100,
        version_token="etag-2",
    )

    assert a.to_lookup_key() == b.to_lookup_key()
    assert a.to_lookup_key() != c.to_lookup_key()
    assert a.to_lookup_key() != d.to_lookup_key()


@pytest.mark.asyncio
async def test_read_through_cache_falls_back_when_backend_fails():
    reader = ReadThroughSegmentCache(_FailingBackend())
    calls = 0

    async def fetcher() -> bytes:
        nonlocal calls
        calls += 1
        return b"payload"

    payload = await reader.read(SegmentCacheKey(object_key="wal/1"), fetcher)
    assert payload == b"payload"
    assert calls == 1
    assert reader.stats.get_errors == 1
    assert reader.stats.put_errors == 1


@pytest.mark.asyncio
async def test_read_through_cache_coalesces_concurrent_misses():
    backend = _MemoryBackend()
    reader = ReadThroughSegmentCache(backend)
    calls = 0

    async def fetcher() -> bytes:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.05)
        return b"same-bytes"

    key = SegmentCacheKey(object_key="wal/shared")
    results = await asyncio.gather(
        *(reader.read(key, fetcher) for _ in range(8))
    )

    assert all(r == b"same-bytes" for r in results)
    assert calls == 1
    assert reader.stats.coalesced_waits == 7


@pytest.mark.asyncio
async def test_read_through_cache_respects_timeouts_and_returns_payload():
    reader = ReadThroughSegmentCache(
        _SlowBackend(),
        get_timeout_seconds=0.005,
        put_timeout_seconds=0.005,
    )

    payload = await reader.read(
        SegmentCacheKey(object_key="wal/slow"),
        lambda: asyncio.sleep(0, result=b"slow-fallback"),
    )
    assert payload == b"slow-fallback"
    assert reader.stats.get_errors == 1
    assert reader.stats.put_errors == 1


@pytest.mark.asyncio
async def test_read_object_bytes_uses_separate_keys_for_distinct_ranges():
    backend = _MemoryBackend()
    reader = ReadThroughSegmentCache(backend)
    cfg = _ConfigShim(
        store=_RangeStore(payload=b"abcdefgh"),
        segment_object_reader=reader,
    )

    first = await read_object_bytes(
        cfg,
        uri="topic_wal/a.wal",
        version_token="etag-1",
        byte_start=0,
        byte_end=4,
    )
    first_again = await read_object_bytes(
        cfg,
        uri="topic_wal/a.wal",
        version_token="etag-1",
        byte_start=0,
        byte_end=4,
    )
    second = await read_object_bytes(
        cfg,
        uri="topic_wal/a.wal",
        version_token="etag-1",
        byte_start=4,
        byte_end=8,
    )

    assert first == b"abcd"
    assert first_again == b"abcd"
    assert second == b"efgh"
    assert cfg.store.range_reads == 2
