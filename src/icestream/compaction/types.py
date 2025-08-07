from dataclasses import dataclass
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


class CompactionProcessor(Protocol):
    async def apply(self, ctx: CompactionContext): ...


