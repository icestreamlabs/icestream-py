import datetime
import io

from kio.schema.errors import ErrorCode
from kio.schema.fetch.v0.request import (
    FetchRequest as FetchRequestV0,
)
from kio.schema.fetch.v0.request import (
    RequestHeader as FetchRequestHeaderV0,
)
from kio.schema.fetch.v0.response import (
    FetchResponse as FetchResponseV0,
)
from kio.schema.fetch.v0.response import (
    ResponseHeader as FetchResponseHeaderV0,
)
from kio.schema.fetch.v1.request import (
    FetchRequest as FetchRequestV1,
)
from kio.schema.fetch.v1.request import (
    RequestHeader as FetchRequestHeaderV1,
)
from kio.schema.fetch.v1.response import (
    FetchResponse as FetchResponseV1,
)
from kio.schema.fetch.v1.response import (
    ResponseHeader as FetchResponseHeaderV1,
)
from kio.schema.fetch.v2.request import (
    FetchRequest as FetchRequestV2,
)
from kio.schema.fetch.v2.request import (
    RequestHeader as FetchRequestHeaderV2,
)
from kio.schema.fetch.v2.response import (
    FetchResponse as FetchResponseV2,
)
from kio.schema.fetch.v2.response import (
    ResponseHeader as FetchResponseHeaderV2,
)
from kio.schema.fetch.v3.request import (
    FetchRequest as FetchRequestV3,
)
from kio.schema.fetch.v3.request import (
    RequestHeader as FetchRequestHeaderV3,
)
from kio.schema.fetch.v3.response import (
    FetchResponse as FetchResponseV3,
)
from kio.schema.fetch.v3.response import (
    ResponseHeader as FetchResponseHeaderV3,
)
from kio.schema.fetch.v4.request import (
    FetchRequest as FetchRequestV4,
)
from kio.schema.fetch.v4.request import (
    RequestHeader as FetchRequestHeaderV4,
)
from kio.schema.fetch.v4.response import (
    FetchResponse as FetchResponseV4,
)
from kio.schema.fetch.v4.response import (
    ResponseHeader as FetchResponseHeaderV4,
)
from kio.schema.fetch.v5.request import (
    FetchRequest as FetchRequestV5,
)
from kio.schema.fetch.v5.request import (
    RequestHeader as FetchRequestHeaderV5,
)
from kio.schema.fetch.v5.response import (
    FetchResponse as FetchResponseV5,
)
from kio.schema.fetch.v5.response import (
    ResponseHeader as FetchResponseHeaderV5,
)
from kio.schema.fetch.v6.request import (
    FetchRequest as FetchRequestV6,
)
from kio.schema.fetch.v6.request import (
    RequestHeader as FetchRequestHeaderV6,
)
from kio.schema.fetch.v6.response import (
    FetchResponse as FetchResponseV6,
)
from kio.schema.fetch.v6.response import (
    ResponseHeader as FetchResponseHeaderV6,
)
from kio.schema.fetch.v7.request import (
    FetchRequest as FetchRequestV7,
)
from kio.schema.fetch.v7.request import (
    RequestHeader as FetchRequestHeaderV7,
)
from kio.schema.fetch.v7.response import (
    FetchResponse as FetchResponseV7,
)
from kio.schema.fetch.v7.response import (
    ResponseHeader as FetchResponseHeaderV7,
)
from kio.schema.fetch.v8.request import (
    FetchRequest as FetchRequestV8,
)
from kio.schema.fetch.v8.request import (
    RequestHeader as FetchRequestHeaderV8,
)
from kio.schema.fetch.v8.response import (
    FetchResponse as FetchResponseV8,
)
from kio.schema.fetch.v8.response import (
    ResponseHeader as FetchResponseHeaderV8,
)
from kio.schema.fetch.v9.request import (
    FetchRequest as FetchRequestV9,
)
from kio.schema.fetch.v9.request import (
    RequestHeader as FetchRequestHeaderV9,
)
from kio.schema.fetch.v9.response import (
    FetchResponse as FetchResponseV9,
)
from kio.schema.fetch.v9.response import (
    ResponseHeader as FetchResponseHeaderV9,
)
from kio.schema.fetch.v10.request import (
    FetchRequest as FetchRequestV10,
)
from kio.schema.fetch.v10.request import (
    RequestHeader as FetchRequestHeaderV10,
)
from kio.schema.fetch.v10.response import (
    FetchResponse as FetchResponseV10,
)
from kio.schema.fetch.v10.response import (
    ResponseHeader as FetchResponseHeaderV10,
)
from kio.schema.fetch.v11.request import (
    FetchRequest as FetchRequestV11,
)
from kio.schema.fetch.v11.request import (
    RequestHeader as FetchRequestHeaderV11,
)
from kio.schema.fetch.v11.response import (
    FetchResponse as FetchResponseV11,
)
from kio.schema.fetch.v11.response import (
    ResponseHeader as FetchResponseHeaderV11,
)
from kio.schema.types import BrokerId
from kio.static.primitive import i32, i64, Records, i32Timedelta

from icestream.config import Config
import kio.schema.fetch.v0 as fetch_v0
import kio.schema.fetch.v1 as fetch_v1
import kio.schema.fetch.v2 as fetch_v2
import kio.schema.fetch.v3 as fetch_v3
import kio.schema.fetch.v4 as fetch_v4
import kio.schema.fetch.v5 as fetch_v5
import kio.schema.fetch.v6 as fetch_v6
import kio.schema.fetch.v7 as fetch_v7
import kio.schema.fetch.v8 as fetch_v8
import kio.schema.fetch.v9 as fetch_v9
import kio.schema.fetch.v10 as fetch_v10
import kio.schema.fetch.v11 as fetch_v11
from sqlalchemy import select, update, asc
import pyarrow.parquet as pq

from icestream.kafkaserver.protocol import decode_kafka_records, KafkaRecord, KafkaRecordBatch, KafkaRecordHeader
from icestream.kafkaserver.wal.serde import decode_kafka_wal_file
from icestream.models import Partition, WALFileOffset, WALFile, ParquetFile
from icestream.utils import wal_uri_to_object_key

FetchRequestHeader = (
        FetchRequestHeaderV0
        | FetchRequestHeaderV1
        | FetchRequestHeaderV2
        | FetchRequestHeaderV3
        | FetchRequestHeaderV4
        | FetchRequestHeaderV5
        | FetchRequestHeaderV6
        | FetchRequestHeaderV7
        | FetchRequestHeaderV8
        | FetchRequestHeaderV9
        | FetchRequestHeaderV10
        | FetchRequestHeaderV11
)

FetchResponseHeader = (
        FetchResponseHeaderV0
        | FetchResponseHeaderV1
        | FetchResponseHeaderV2
        | FetchResponseHeaderV3
        | FetchResponseHeaderV4
        | FetchResponseHeaderV5
        | FetchResponseHeaderV6
        | FetchResponseHeaderV7
        | FetchResponseHeaderV8
        | FetchResponseHeaderV9
        | FetchResponseHeaderV10
        | FetchResponseHeaderV11
)

FetchRequest = (
        FetchRequestV0
        | FetchRequestV1
        | FetchRequestV2
        | FetchRequestV3
        | FetchRequestV4
        | FetchRequestV5
        | FetchRequestV6
        | FetchRequestV7
        | FetchRequestV8
        | FetchRequestV9
        | FetchRequestV10
        | FetchRequestV11
)

FetchResponse = (
        FetchResponseV0
        | FetchResponseV1
        | FetchResponseV2
        | FetchResponseV3
        | FetchResponseV4
        | FetchResponseV5
        | FetchResponseV6
        | FetchResponseV7
        | FetchResponseV8
        | FetchResponseV9
        | FetchResponseV10
        | FetchResponseV11
)


async def do_fetch(config: Config, req: FetchRequest, api_version: int) -> FetchResponse:
    topic_responses: list[fetch_v11.response.FetchableTopicResponse] = []

    async with config.async_session_factory() as session:
        for t in req.topics:
            topic_name = t.topic
            partition_responses: list[fetch_v11.response.PartitionData] = []

            for pr in t.partitions:
                partition = int(pr.partition)
                start = int(pr.fetch_offset)  # absolute offset (ListOffsets resolves sentinels)

                # load partition metadata
                row = (
                    await session.execute(
                        select(Partition.last_offset, Partition.log_start_offset)
                        .where(
                            Partition.topic_name == topic_name,
                            Partition.partition_number == partition,
                        )
                    )
                ).one_or_none()

                if row is None:
                    partition_responses.append(
                        fetch_v11.response.PartitionData(
                            partition_index=i32(partition),
                            error_code=ErrorCode.unknown_topic_or_partition,
                            high_watermark=i64(-1),
                            last_stable_offset=i64(-1),
                            log_start_offset=i64(-1),
                            records=Records(b""),
                            aborted_transactions=(),
                            preferred_read_replica=BrokerId(-1)
                        )
                    )
                    continue

                last_offset, log_start = int(row[0]), int(row[1])
                high_watermark = last_offset
                log_end_offset = last_offset + 1

                max_bytes = int(pr.partition_max_bytes)
                if max_bytes <= 0: # 0 or less is invalid, don't enforce broker side limits yet
                    partition_responses.append(
                        fetch_v11.response.PartitionData(
                            partition_index=i32(partition),
                            error_code=ErrorCode.invalid_request,
                            high_watermark=i64(high_watermark),
                            last_stable_offset=i64(high_watermark),
                            log_start_offset=i64(log_start),
                            records=Records(b""),
                            aborted_transactions=(),
                            preferred_read_replica=BrokerId(-1),
                        )
                    )
                    continue

                # range checks for absolute fetch offset
                if start < log_start or start > log_end_offset:
                    partition_responses.append(
                        fetch_v11.response.PartitionData(
                            partition_index=i32(partition),
                            error_code=ErrorCode.offset_out_of_range,
                            high_watermark=i64(high_watermark),
                            last_stable_offset=i64(high_watermark),
                            log_start_offset=i64(log_start),
                            records=Records(b""),
                            aborted_transactions=(),
                            preferred_read_replica=BrokerId(-1)
                        )
                    )
                    continue

                if start == log_end_offset:
                    # valid but nothing to return
                    partition_responses.append(
                        fetch_v11.response.PartitionData(
                            partition_index=i32(partition),
                            error_code=ErrorCode.none,
                            high_watermark=i64(high_watermark),
                            log_start_offset=i64(log_start),
                            records=Records(b""),
                            aborted_transactions=(),
                            last_stable_offset=i64(high_watermark),
                            preferred_read_replica=BrokerId(-1)
                        )
                    )
                    continue

                # fetch uncompacted WAL, then parquet
                records_blob = b""
                remaining_bytes = max_bytes
                current_offset = start

                # wal
                wal_rows = (
                    await session.execute(
                        select(WALFileOffset, WALFile)
                        .join(WALFile, WALFile.id == WALFileOffset.wal_file_id)
                        .where(
                            WALFile.compacted_at.is_(None),
                            WALFileOffset.topic_name == topic_name,
                            WALFileOffset.partition_number == partition,
                            WALFileOffset.last_offset >= current_offset,
                        )
                        .order_by(asc(WALFileOffset.base_offset))
                    )
                ).all()

                for off, wf in wal_rows:
                    if remaining_bytes <= 0:
                        break

                    obj = await config.store.get_async(wal_uri_to_object_key(config, wf.uri))
                    data = await obj.bytes_async()
                    decoded = decode_kafka_wal_file(bytes(data))

                    for b in decoded.batches:
                        if b.topic != topic_name or b.partition != partition:
                            continue

                        base = b.kafka_record_batch.base_offset
                        lod = b.kafka_record_batch.last_offset_delta
                        if base is None or lod is None:
                            continue

                        last_in_batch = base + lod
                        if last_in_batch < current_offset:
                            continue

                        recs = decode_kafka_records(b.kafka_record_batch.records)

                        # choose first record at/after current_offset as new base_abs
                        base_abs = None
                        for r in recs:
                            a = base + r.offset_delta
                            if a >= current_offset:
                                base_abs = a
                                break
                        if base_abs is None:
                            continue

                        fixed: list[KafkaRecord] = []
                        # we keep timestamp deltas relative to a base_ts; if unavailable, use 0
                        # we take base_ts from the first kept record's absolute ts if present
                        # (Kafka allows 0 if unknown)
                        # We can't read base_timestamp from Parquet here; for WAL we have it:
                        base_ts = b.kafka_record_batch.base_timestamp or 0
                        # But base_ts in v2 batch is a *base*, and per-record timestamp_delta is rel to that base_ts.
                        # We keep original deltas as-is; since we're trimming, they still align (because base_ts doesn't change).
                        for r in recs:
                            a = base + r.offset_delta
                            if a < current_offset:
                                continue
                            fixed.append(
                                KafkaRecord(
                                    offset_delta=a - base_abs,
                                    timestamp_delta=r.timestamp_delta,
                                    key=r.key,
                                    value=r.value,
                                    headers=r.headers,
                                    attributes=r.attributes,
                                )
                            )
                        if not fixed:
                            continue

                        batch = KafkaRecordBatch.from_records(
                            offset=base_abs,
                            records=fixed,
                            attributes=b.kafka_record_batch.attributes
                        )
                        raw = batch.to_bytes()

                        if len(raw) > remaining_bytes and records_blob:
                            # don't exceed remaining if we've already got some bytes
                            remaining_bytes = 0
                            break

                        records_blob += raw
                        remaining_bytes -= len(raw)
                        current_offset = base_abs + fixed[-1].offset_delta + 1

                        if remaining_bytes <= 0:
                            break

                # parquet
                if remaining_bytes > 0:
                    pfs = (
                        await session.execute(
                            select(ParquetFile)
                            .where(
                                ParquetFile.topic_name == topic_name,
                                ParquetFile.partition_number == partition,
                                ParquetFile.compacted_at.is_(None),
                                ParquetFile.max_offset >= current_offset,
                            )
                            .order_by(asc(ParquetFile.min_offset))
                        )
                    ).scalars().all()

                    for pf in pfs:
                        if remaining_bytes <= 0:
                            break

                        obj = await config.store.get_async(wal_uri_to_object_key(config, pf.uri))
                        blob = await obj.bytes_async()
                        pfq = pq.ParquetFile(io.BytesIO(bytes(blob)))

                        target_group_bytes = max(128 * 1024, min(1 * 1024 * 1024, remaining_bytes))
                        group_rows: list[dict] = []
                        approx = 0

                        def flush_group():
                            nonlocal group_rows, approx, records_blob, remaining_bytes, current_offset
                            if not group_rows:
                                return
                            group_rows.sort(key=lambda r: r["offset"])
                            base_abs = group_rows[0]["offset"]
                            # pick a base timestamp (first non-null) else 0
                            base_ts = next((r["timestamp_ms"] for r in group_rows if r["timestamp_ms"] is not None), 0)
                            recs: list[KafkaRecord] = []
                            for r in group_rows:
                                recs.append(
                                    KafkaRecord(
                                        offset_delta=int(r["offset"] - base_abs),
                                        timestamp_delta=(
                                            0 if r["timestamp_ms"] is None else int(r["timestamp_ms"] - base_ts)),
                                        key=r["key"],
                                        value=r["value"],
                                        headers=[KafkaRecordHeader(key=h["key"], value=h["value"])
                                                 for h in (r["headers"] or [])],
                                        attributes=0, # TODO NEED TO FIX
                                    )
                                )
                            batch = KafkaRecordBatch.from_records(offset=base_abs, records=recs)
                            raw = batch.to_bytes()

                            if len(raw) > remaining_bytes and records_blob:
                                # don't exceed limit if we already have something
                                group_rows.clear()
                                approx = 0
                                return

                            records_blob += raw
                            remaining_bytes -= len(raw)
                            current_offset = group_rows[-1]["offset"] + 1
                            group_rows.clear()
                            approx = 0

                        for rg in range(pfq.num_row_groups):
                            if remaining_bytes <= 0:
                                break
                            tbl = pfq.read_row_group(
                                rg,
                                columns=["offset", "timestamp_ms", "key", "value", "headers"],
                            )

                            off = tbl.column(tbl.schema.get_field_index("offset"))
                            ts = tbl.column(tbl.schema.get_field_index("timestamp_ms"))
                            key = tbl.column(tbl.schema.get_field_index("key"))
                            val = tbl.column(tbl.schema.get_field_index("value"))
                            hdr = tbl.column(tbl.schema.get_field_index("headers"))

                            for i in range(tbl.num_rows):
                                if remaining_bytes <= 0:
                                    break
                                o = int(off[i].as_py())
                                if o < current_offset:
                                    continue

                                row = {
                                    "offset": o,
                                    "timestamp_ms": ts[i].as_py(),
                                    "key": key[i].as_py(),
                                    "value": val[i].as_py(),
                                    "headers": hdr[i].as_py(),
                                }
                                # approximate payload size to size batches
                                k = len(row["key"]) if row["key"] else 0
                                v = len(row["value"]) if row["value"] else 0
                                hsz = 0
                                if row["headers"]:
                                    for h0 in row["headers"]:
                                        hsz += (len(h0["key"]) if h0["key"] else 0) + (
                                            len(h0["value"]) if h0["value"] else 0)
                                approx += k + v + hsz + 64
                                group_rows.append(row)

                                if approx >= target_group_bytes:
                                    flush_group()

                            # flush any remainder of this row group
                            if remaining_bytes > 0:
                                flush_group()

                # finalize this partition response
                partition_responses.append(
                    fetch_v11.response.PartitionData(
                        partition_index=i32(partition),
                        error_code=ErrorCode.none,
                        high_watermark=i64(high_watermark),
                        log_start_offset=i64(log_start),
                        records=records_blob,
                        aborted_transactions=(),
                        last_stable_offset=i64(high_watermark),
                        preferred_read_replica=BrokerId(-1)
                    )
                )

            topic_responses.append(
                fetch_v11.response.FetchableTopicResponse(
                    topic=topic_name,
                    partitions=tuple(partition_responses),
                )
            )
    response_v11 = fetch_v11.FetchResponse(responses=tuple(topic_responses),
                                           throttle_time=i32Timedelta.parse(datetime.timedelta(milliseconds=0)),
                                           error_code=ErrorCode.none)
    if api_version == 11:
        return response_v11
    else:
        return do_response_ladder(response_v11, api_version)


def do_response_ladder(resp: FetchResponse, api_version: int) -> FetchResponse:
    if api_version == 0:
        _topics = tuple(
            fetch_v0.response.FetchableTopicResponse(
                topic=t.topic,
                partitions=tuple(
                    fetch_v0.response.PartitionData(
                        partition_index=p.partition_index,
                        error_code=p.error_code,
                        high_watermark=p.high_watermark,
                        records=p.records,
                    )
                    for p in t.partitions
                ),
            )
            for t in resp.responses
        )
        return fetch_v0.FetchResponse(
            responses=_topics,
        )

    elif api_version == 1:
        _topics = tuple(
            fetch_v1.response.FetchableTopicResponse(
                topic=t.topic,
                partitions=tuple(
                    fetch_v1.response.PartitionData(
                        partition_index=p.partition_index,
                        error_code=p.error_code,
                        high_watermark=p.high_watermark,
                        records=p.records,
                    )
                    for p in t.partitions
                ),
            )
            for t in resp.responses
        )
        return fetch_v1.FetchResponse(
            responses=_topics,
            throttle_time=resp.throttle_time,
        )

    elif api_version == 2:
        _topics = tuple(
            fetch_v2.response.FetchableTopicResponse(
                topic=t.topic,
                partitions=tuple(
                    fetch_v2.response.PartitionData(
                        partition_index=p.partition_index,
                        error_code=p.error_code,
                        high_watermark=p.high_watermark,
                        records=p.records,
                    )
                    for p in t.partitions
                ),
            )
            for t in resp.responses
        )
        return fetch_v2.FetchResponse(
            responses=_topics,
            throttle_time=resp.throttle_time,
        )

    elif api_version == 3:
        _topics = tuple(
            fetch_v3.response.FetchableTopicResponse(
                topic=t.topic,
                partitions=tuple(
                    fetch_v3.response.PartitionData(
                        partition_index=p.partition_index,
                        error_code=p.error_code,
                        high_watermark=p.high_watermark,
                        records=p.records,
                    )
                    for p in t.partitions
                ),
            )
            for t in resp.responses
        )
        return fetch_v3.FetchResponse(
            responses=_topics,
            throttle_time=resp.throttle_time,
        )

    elif api_version == 4:
        _topics = tuple(
            fetch_v4.response.FetchableTopicResponse(
                topic=t.topic,
                partitions=tuple(
                    fetch_v4.response.PartitionData(
                        partition_index=p.partition_index,
                        error_code=p.error_code,
                        high_watermark=p.high_watermark,
                        last_stable_offset=p.last_stable_offset,
                        aborted_transactions=p.aborted_transactions,
                        records=p.records,
                    )
                    for p in t.partitions
                ),
            )
            for t in resp.responses
        )
        return fetch_v4.FetchResponse(
            responses=_topics,
            throttle_time=resp.throttle_time,
        )

    elif api_version == 5:
        _topics = tuple(
            fetch_v5.response.FetchableTopicResponse(
                topic=t.topic,
                partitions=tuple(
                    fetch_v5.response.PartitionData(
                        partition_index=p.partition_index,
                        error_code=p.error_code,
                        high_watermark=p.high_watermark,
                        last_stable_offset=p.last_stable_offset,
                        log_start_offset=p.log_start_offset,
                        aborted_transactions=p.aborted_transactions,
                        records=p.records,
                    )
                    for p in t.partitions
                ),
            )
            for t in resp.responses
        )
        return fetch_v5.FetchResponse(
            responses=_topics,
            throttle_time=resp.throttle_time,
        )

    elif api_version == 6:
        _topics = tuple(
            fetch_v6.response.FetchableTopicResponse(
                topic=t.topic,
                partitions=tuple(
                    fetch_v6.response.PartitionData(
                        partition_index=p.partition_index,
                        error_code=p.error_code,
                        high_watermark=p.high_watermark,
                        last_stable_offset=p.last_stable_offset,
                        log_start_offset=p.log_start_offset,
                        aborted_transactions=p.aborted_transactions,
                        records=p.records,
                    )
                    for p in t.partitions
                ),
            )
            for t in resp.responses
        )
        return fetch_v6.FetchResponse(
            responses=_topics,
            throttle_time=resp.throttle_time,
        )

    elif api_version == 7:
        _topics = tuple(
            fetch_v7.response.FetchableTopicResponse(
                topic=t.topic,
                partitions=tuple(
                    fetch_v7.response.PartitionData(
                        partition_index=p.partition_index,
                        error_code=p.error_code,
                        high_watermark=p.high_watermark,
                        last_stable_offset=p.last_stable_offset,
                        log_start_offset=p.log_start_offset,
                        aborted_transactions=p.aborted_transactions,
                        records=p.records,
                    )
                    for p in t.partitions
                ),
            )
            for t in resp.responses
        )
        return fetch_v7.FetchResponse(
            error_code=resp.error_code,
            responses=_topics,
            throttle_time=resp.throttle_time,
            session_id=resp.session_id,
        )

    elif api_version == 8:
        _topics = tuple(
            fetch_v8.response.FetchableTopicResponse(
                topic=t.topic,
                partitions=tuple(
                    fetch_v8.response.PartitionData(
                        partition_index=p.partition_index,
                        error_code=p.error_code,
                        high_watermark=p.high_watermark,
                        last_stable_offset=p.last_stable_offset,
                        log_start_offset=p.log_start_offset,
                        aborted_transactions=p.aborted_transactions,
                        records=p.records,
                    )
                    for p in t.partitions
                ),
            )
            for t in resp.responses
        )
        return fetch_v8.FetchResponse(
            error_code=resp.error_code,
            responses=_topics,
            throttle_time=resp.throttle_time,
            session_id=resp.session_id,
        )

    elif api_version == 9:
        _topics = tuple(
            fetch_v9.response.FetchableTopicResponse(
                topic=t.topic,
                partitions=tuple(
                    fetch_v9.response.PartitionData(
                        partition_index=p.partition_index,
                        error_code=p.error_code,
                        high_watermark=p.high_watermark,
                        last_stable_offset=p.last_stable_offset,
                        log_start_offset=p.log_start_offset,
                        aborted_transactions=p.aborted_transactions,
                        records=p.records,
                    )
                    for p in t.partitions
                ),
            )
            for t in resp.responses
        )
        return fetch_v9.FetchResponse(
            error_code=resp.error_code,
            responses=_topics,
            throttle_time=resp.throttle_time,
            session_id=resp.session_id,
        )

    elif api_version == 10:
        _topics = tuple(
            fetch_v10.response.FetchableTopicResponse(
                topic=t.topic,
                partitions=tuple(
                    fetch_v10.response.PartitionData(
                        partition_index=p.partition_index,
                        error_code=p.error_code,
                        high_watermark=p.high_watermark,
                        last_stable_offset=p.last_stable_offset,
                        log_start_offset=p.log_start_offset,
                        aborted_transactions=p.aborted_transactions,
                        records=p.records,
                    )
                    for p in t.partitions
                ),
            )
            for t in resp.responses
        )
        return fetch_v10.FetchResponse(
            error_code=resp.error_code,
            responses=_topics,
            throttle_time=resp.throttle_time,
            session_id=resp.session_id,
        )

    elif api_version == 11:
        return resp

    else:
        raise ValueError(f"unsupported fetch api version: {api_version}")
