from __future__ import annotations
import datetime
import io
from typing import List

import pyarrow as pa
import pyarrow.parquet as pq

from icestream.compaction.types import CompactionContext, CompactionProcessor
from icestream.compaction.schema import PARQUET_RECORD_SCHEMA
from icestream.kafkaserver.protocol import decode_kafka_records
from icestream.models import ParquetFile, ParquetFileSource, assert_no_overlap, ParquetFileParent


class ParquetCompactor(CompactionProcessor):
    name = "parquet_compactor"

    def __init__(self):
        pass

    async def apply(self, ctx: CompactionContext) -> None:
        if not ctx.parquet_candidates:
            return

        target_bytes = ctx.config.PARQUET_COMPACTION_TARGET_BYTES

        async with ctx.config.async_session_factory() as session:
            for (topic, partition), parents in ctx.parquet_candidates.items():
                if not parents:
                    continue

                # parents assumed sorted by min_offset
                max_gen = max((p.generation for p in parents), default=0)

                sink = io.BytesIO()
                writer = pq.ParquetWriter(
                    sink, PARQUET_RECORD_SCHEMA,
                    compression="zstd", use_dictionary=True, write_statistics=True
                )
                out_rows = 0
                approx = 0
                out_min_off = parents[0].min_offset
                last_off = out_min_off

                async def finalize_and_register():
                    nonlocal sink, writer, out_rows, approx, out_min_off, last_off
                    if writer is None or out_rows == 0:
                        return None
                    writer.close()
                    data = sink.getvalue()
                    total_bytes = len(data)
                    key = ctx.config.PARQUET_PREFIX.rstrip("/") + f"/topics/{topic}/partition={partition}/{out_min_off}-{last_off}-gen{max_gen+1}.parquet"
                    uri = await ctx.config.store.put_async(key, data)

                    await assert_no_overlap(session, topic, partition, out_min_off, last_off)
                    pf = ParquetFile(
                        topic_name=topic,
                        partition_number=partition,
                        uri=uri,
                        total_bytes=total_bytes,
                        row_count=out_rows,
                        min_offset=out_min_off,
                        max_offset=last_off,
                        min_timestamp=None,
                        max_timestamp=None,
                        generation=max_gen + 1,
                    )
                    session.add(pf)
                    await session.flush()

                    # lineage
                    for parent in parents:
                        session.add(ParquetFileParent(child_parquet_file_id=pf.id, parent_parquet_file_id=parent.id))

                    # reset for next output
                    sink = io.BytesIO()
                    writer = pq.ParquetWriter(
                        sink, PARQUET_RECORD_SCHEMA,
                        compression="zstd", use_dictionary=True, write_statistics=True
                    )
                    out_rows = 0
                    approx = 0
                    out_min_off = last_off + 1
                    return pf

                # stream read each parent, row group by row group
                for p in parents:
                    blob = await ctx.config.store.get_async(p.uri)
                    pf = pq.ParquetFile(io.BytesIO(bytes(blob.bytes())))
                    for rg_index in range(pf.num_row_groups):
                        table = pf.read_row_group(rg_index)
                        for batch in table.to_batches():
                            writer.write_batch(batch)
                            out_rows += batch.num_rows
                            # estimate growth
                            approx += sum(arr.nbytes for arr in batch.columns)
                            # track last offset written (from last element of "offset" column)
                            last_off = int(batch.column(1)[-1].as_py())  # column 1 == "offset"
                            if approx >= target_bytes:
                                await finalize_and_register()

                # finalize trailing file
                await finalize_and_register()

                # tombstone parents
                now_ts = datetime.datetime.now(datetime.UTC)
                for parent in parents:
                    if parent.compacted_at is None:
                        parent.compacted_at = now_ts

            await session.commit()
