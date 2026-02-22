from dataclasses import dataclass, field
from typing import Sequence, Protocol

from icestream.config import Config
from icestream.kafkaserver.wal import WALFile as DecodedWALFile, WALBatch
from icestream.models import WALFile as WALFileModel, ParquetFile


@dataclass
class CompactionContext:
    config: Config
    wal_models: Sequence[WALFileModel]
    wal_decoded: Sequence[DecodedWALFile]
    parquet_candidates: dict[tuple[str, int], list[ParquetFile]]
    now_monotonic: float


@dataclass
class TopicWALOffsetPlan:
    topic_name: str
    partition_number: int
    base_offset: int
    last_offset: int
    byte_start: int
    byte_end: int
    min_timestamp: int | None
    max_timestamp: int | None


@dataclass
class TopicWALFilePlan:
    topic_name: str
    uri: str
    etag: str | None
    total_bytes: int
    total_messages: int
    offsets: list[TopicWALOffsetPlan]
    source_wal_ids: list[int]


@dataclass
class CompactionWritePlan:
    topic_wal_files: list[TopicWALFilePlan] = field(default_factory=list)
    uploaded_object_keys: list[str] = field(default_factory=list)
    compacted_wal_ids: set[int] = field(default_factory=set)

    def extend(self, other: "CompactionWritePlan") -> None:
        self.topic_wal_files.extend(other.topic_wal_files)
        self.uploaded_object_keys.extend(other.uploaded_object_keys)
        self.compacted_wal_ids.update(other.compacted_wal_ids)


class CompactionProcessor(Protocol):
    async def build_plan(self, ctx: CompactionContext) -> CompactionWritePlan: ...
