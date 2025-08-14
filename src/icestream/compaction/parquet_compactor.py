from __future__ import annotations

import datetime
import io
from dataclasses import dataclass
from typing import List

import pyarrow.parquet as pq

from icestream.compaction import build_uri
from icestream.compaction.types import CompactionContext, CompactionProcessor
from icestream.compaction.schema import PARQUET_RECORD_SCHEMA
from icestream.models import (
    ParquetFile,
    ParquetFileParent,
    assert_no_overlap,
)


@dataclass
class _OutputState:
    sink: io.BytesIO
    writer: pq.ParquetWriter
    out_rows: int
    approx: int
    out_min_off: int
    last_off: int


class ParquetCompactor(CompactionProcessor):
    def __init__(self) -> None:
        pass

    async def apply(self, ctx: CompactionContext) -> None:
        if not ctx.parquet_candidates:
            return

        async with ctx.config.async_session_factory() as session:
            for (topic, partition), parents in ctx.parquet_candidates.items():
                if not parents:
                    continue
                await self._compact_group(ctx, session, topic, partition, parents)
            await session.commit()

    async def _compact_group(
        self,
        ctx: CompactionContext,
        session,
        topic: str,
        partition: int,
        parents: List[ParquetFile],
    ) -> None:
        target_bytes = ctx.config.PARQUET_COMPACTION_TARGET_BYTES
        max_gen = max((p.generation for p in parents), default=0)

        # initialize first output
        state = self._new_output(out_min_off=parents[0].min_offset, last_off=parents[0].min_offset)

        for parent in parents:
            # read parent file as a stream
            get_result = await ctx.config.store.get_async(parent.uri)
            blob = await get_result.bytes_async()
            pf = pq.ParquetFile(io.BytesIO(bytes(blob)))

            for rg_idx in range(pf.num_row_groups):
                table = pf.read_row_group(rg_idx)
                for batch in table.to_batches():
                    state.writer.write_batch(batch)
                    state.out_rows += batch.num_rows
                    # estimate growth (arrays in batch have .nbytes)
                    state.approx += sum(arr.nbytes for arr in batch.columns)
                    # last offset from "offset" column; schema is stable so index 1
                    state.last_off = int(batch.column(1)[-1].as_py())

                    if state.approx >= target_bytes:
                        await self._finalize_and_register(
                            ctx, session, topic, partition, parents, max_gen, state
                        )
                        # new file starts right after the last written offset
                        state = self._new_output(out_min_off=state.last_off + 1, last_off=state.last_off + 1)

        # finalize trailing output
        await self._finalize_and_register(ctx, session, topic, partition, parents, max_gen, state)

        # tombstone parents
        now_ts = datetime.datetime.now(datetime.UTC)
        for p in parents:
            if p.compacted_at is None:
                p.compacted_at = now_ts

    @staticmethod
    def _new_output(out_min_off: int, last_off: int) -> _OutputState:
        sink = io.BytesIO()
        writer = pq.ParquetWriter(
            sink,
            PARQUET_RECORD_SCHEMA,
            compression="zstd",
            use_dictionary=True,
            write_statistics=True,
        )
        return _OutputState(
            sink=sink,
            writer=writer,
            out_rows=0,
            approx=0,
            out_min_off=out_min_off,
            last_off=last_off,
        )

    @staticmethod
    async def _finalize_and_register(
            ctx: CompactionContext,
        session,
        topic: str,
        partition: int,
        parents: List[ParquetFile],
        max_gen: int,
        state: _OutputState,
    ):
        """
        Close current writer, upload object, insert ParquetFile + lineage, and
        prepare for next output (callers will immediately _new_output()).
        """
        if state.writer is None or state.out_rows == 0:
            return None

        state.writer.close()
        data = state.sink.getvalue()
        total_bytes = len(data)

        key = (
            ctx.config.PARQUET_PREFIX.rstrip("/")
            + f"/topics/{topic}/partition={partition}/{state.out_min_off}-{state.last_off}-gen{max_gen+1}.parquet"
        )
        # put_async expects IO[bytes]
        await ctx.config.store.put_async(key, io.BytesIO(data))
        uri = build_uri(ctx.config, key)

        await assert_no_overlap(session, topic, partition, state.out_min_off, state.last_off)
        pf = ParquetFile(
            topic_name=topic,
            partition_number=partition,
            uri=uri,
            total_bytes=total_bytes,
            row_count=state.out_rows,
            min_offset=state.out_min_off,
            max_offset=state.last_off,
            min_timestamp=None,
            max_timestamp=None,
            generation=max_gen + 1,
        )
        session.add(pf)
        await session.flush()

        # lineage: record all parents that contributed
        for parent in parents:
            session.add(
                ParquetFileParent(child_parquet_file_id=pf.id, parent_parquet_file_id=parent.id)
            )

        return pf
