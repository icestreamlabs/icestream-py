from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Awaitable, Callable

from icestream.cache.segment_cache import SegmentCacheBackend, SegmentCacheKey
from icestream.logger import log


@dataclass(slots=True)
class SegmentCacheStats:
    hits: int = 0
    misses: int = 0
    fills: int = 0
    coalesced_waits: int = 0
    get_errors: int = 0
    put_errors: int = 0
    get_latency_ms: float = 0.0
    put_latency_ms: float = 0.0


class ReadThroughSegmentCache:
    def __init__(
        self,
        backend: SegmentCacheBackend,
        *,
        get_timeout_seconds: float = 0.05,
        put_timeout_seconds: float = 0.05,
    ):
        self._backend = backend
        self._get_timeout_seconds = get_timeout_seconds
        self._put_timeout_seconds = put_timeout_seconds
        self.stats = SegmentCacheStats()
        self._inflight: dict[str, asyncio.Task[bytes]] = {}
        self._inflight_lock = asyncio.Lock()

    async def read(
        self,
        cache_key: SegmentCacheKey,
        fetcher: Callable[[], Awaitable[bytes]],
    ) -> bytes:
        lookup_key = cache_key.to_lookup_key()
        get_started = time.perf_counter()
        cached: bytes | None = None
        try:
            cached = await asyncio.wait_for(
                self._backend.get(lookup_key),
                timeout=self._get_timeout_seconds,
            )
        except Exception as exc:
            self.stats.get_errors += 1
            log.warning(
                "segment_cache_get_failed",
                cache_key=lookup_key,
                error=type(exc).__name__,
            )
        finally:
            self.stats.get_latency_ms += (time.perf_counter() - get_started) * 1000

        if cached is not None:
            self.stats.hits += 1
            return cached

        self.stats.misses += 1
        loader_task: asyncio.Task[bytes]

        async with self._inflight_lock:
            existing = self._inflight.get(lookup_key)
            if existing is not None:
                self.stats.coalesced_waits += 1
                log.info("segment_cache_coalesced_wait", cache_key=lookup_key)
                loader_task = existing
            else:
                loader_task = asyncio.create_task(
                    self._load_and_fill(cache_key, lookup_key, fetcher)
                )
                self._inflight[lookup_key] = loader_task

        return await loader_task

    async def _load_and_fill(
        self,
        cache_key: SegmentCacheKey,
        lookup_key: str,
        fetcher: Callable[[], Awaitable[bytes]],
    ) -> bytes:
        try:
            payload = await fetcher()
            put_started = time.perf_counter()
            try:
                await asyncio.wait_for(
                    self._backend.put(lookup_key, payload),
                    timeout=self._put_timeout_seconds,
                )
            except Exception as exc:
                self.stats.put_errors += 1
                log.warning(
                    "segment_cache_put_failed",
                    cache_key=lookup_key,
                    error=type(exc).__name__,
                )
            else:
                self.stats.fills += 1
                log.info(
                    "segment_cache_miss_fill",
                    cache_key=lookup_key,
                    object_key=cache_key.normalized_object_key,
                    byte_range=cache_key.normalized_range,
                    payload_size_bytes=len(payload),
                )
            finally:
                self.stats.put_latency_ms += (time.perf_counter() - put_started) * 1000
            return payload
        finally:
            async with self._inflight_lock:
                current = self._inflight.get(lookup_key)
                if current is asyncio.current_task():
                    self._inflight.pop(lookup_key, None)

    async def close(self) -> None:
        await self._backend.close()
