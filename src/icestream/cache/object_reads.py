from __future__ import annotations

from typing import Protocol

from icestream.cache.segment_cache import SegmentCacheKey
from icestream.config import Config
from icestream.utils import normalize_object_key


class SegmentObjectReader(Protocol):
    async def read(self, cache_key: SegmentCacheKey, fetcher) -> bytes: ...


def _to_bytes(value) -> bytes:
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    if hasattr(value, "to_bytes"):
        return value.to_bytes()
    return bytes(value)


async def _read_direct(
    config: Config,
    *,
    object_key: str,
    byte_start: int | None,
    byte_end: int | None,
) -> bytes:
    if (
        byte_start is not None
        and byte_end is not None
        and byte_end > byte_start
        and hasattr(config.store, "get_range_async")
    ):
        span = await config.store.get_range_async(
            object_key,
            start=byte_start,
            length=(byte_end - byte_start),
        )
        return _to_bytes(span)

    get_result = await config.store.get_async(object_key)
    data = await get_result.bytes_async()
    return bytes(data)


async def read_object_bytes(
    config: Config,
    *,
    uri: str,
    version_token: str | None = None,
    byte_start: int | None = None,
    byte_end: int | None = None,
) -> bytes:
    object_key = normalize_object_key(config, uri)

    reader = getattr(config, "segment_object_reader", None)
    get_reader = getattr(config, "get_segment_object_reader", None)
    if callable(get_reader):
        reader = await get_reader()
    if reader is None:
        return await _read_direct(
            config,
            object_key=object_key,
            byte_start=byte_start,
            byte_end=byte_end,
        )

    cache_key = SegmentCacheKey(
        object_key=object_key,
        byte_start=byte_start,
        byte_end=byte_end,
        version_token=version_token,
    )
    return await reader.read(
        cache_key,
        lambda: _read_direct(
            config,
            object_key=object_key,
            byte_start=byte_start,
            byte_end=byte_end,
        ),
    )
