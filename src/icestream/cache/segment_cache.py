from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from typing import Any, Protocol


_CACHE_KEY_FORMAT_VERSION = "v1"


class SegmentCacheBackend(Protocol):
    async def get(self, key: str) -> bytes | None: ...

    async def put(self, key: str, value: bytes) -> None: ...

    async def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class SegmentCacheKey:
    object_key: str
    byte_start: int | None = None
    byte_end: int | None = None
    version_token: str | None = None

    def __post_init__(self) -> None:
        if self.byte_start is not None and self.byte_start < 0:
            raise ValueError("byte_start must be >= 0")
        if self.byte_end is not None and self.byte_end < 0:
            raise ValueError("byte_end must be >= 0")
        if (
            self.byte_start is not None
            and self.byte_end is not None
            and self.byte_end <= self.byte_start
        ):
            raise ValueError("byte_end must be > byte_start for ranged keys")

    @property
    def normalized_object_key(self) -> str:
        key = self.object_key.strip()
        return key.lstrip("/")

    @property
    def normalized_version_token(self) -> str:
        token = (self.version_token or "").strip()
        return token if token else "no-version"

    @property
    def normalized_range(self) -> str:
        if self.byte_start is None and self.byte_end is None:
            return "full"
        return f"{self.byte_start or 0}:{self.byte_end if self.byte_end is not None else ''}"

    def to_lookup_key(self) -> str:
        """
        Deterministic immutable cache key invariants:
        - Object key must be normalized before construction (no bucket/prefix).
        - Range is represented as a half-open span [start, end), or `full`.
        - Version token (ETag/version id) is always included when known to
          prevent stale key reuse when an object key is rewritten.
        """
        wire = (
            f"{_CACHE_KEY_FORMAT_VERSION}|"
            f"obj={self.normalized_object_key}|"
            f"range={self.normalized_range}|"
            f"ver={self.normalized_version_token}"
        )
        digest = sha256(wire.encode("utf-8")).hexdigest()
        return (
            f"{_CACHE_KEY_FORMAT_VERSION}:"
            f"{self.normalized_range}:"
            f"{self.normalized_version_token}:"
            f"{digest}"
        )


class NoopSegmentCache:
    async def get(self, key: str) -> bytes | None:
        return None

    async def put(self, key: str, value: bytes) -> None:
        return None

    async def close(self) -> None:
        return None


class FoyerSegmentCache:
    def __init__(self, cache: Any):
        self._cache = cache

    @classmethod
    async def create_memory(cls, *, memory_capacity_bytes: int) -> FoyerSegmentCache:
        import foyer

        return cls(foyer.AsyncCache(memory_capacity_bytes))

    @classmethod
    async def create_hybrid(
        cls,
        *,
        cache_dir: str,
        memory_capacity_bytes: int,
        disk_capacity_bytes: int,
    ) -> FoyerSegmentCache:
        import foyer

        cache = await foyer.AsyncHybridCache.create(
            cache_dir,
            memory_capacity=memory_capacity_bytes,
            storage_capacity=disk_capacity_bytes,
        )
        return cls(cache)

    async def get(self, key: str) -> bytes | None:
        value = await self._cache.get(key)
        if value is None:
            return None
        return bytes(value)

    async def put(self, key: str, value: bytes) -> None:
        await self._cache.insert(key, bytes(value))

    async def close(self) -> None:
        close = getattr(self._cache, "close", None)
        if close is None:
            return
        await close()
