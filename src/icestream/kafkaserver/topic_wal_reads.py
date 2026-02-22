from __future__ import annotations

from io import BytesIO

from icestream.config import Config
from icestream.kafkaserver.protocol import KafkaRecordBatch
from icestream.kafkaserver.utils import decode_varint
from icestream.kafkaserver.wal.serde import decode_kafka_wal_file
from icestream.utils import normalize_object_key


def _to_bytes(value) -> bytes:
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    if hasattr(value, "to_bytes"):
        return value.to_bytes()
    return bytes(value)


def _decode_partial_record_batch_span(data: bytes) -> list[KafkaRecordBatch]:
    if not data:
        return []

    buf = BytesIO(data)
    batches: list[KafkaRecordBatch] = []

    while buf.tell() < len(data):
        rb_len = decode_varint(buf)
        if rb_len < 0:
            raise ValueError("negative record batch length in partial topic-wal span")

        rb_bytes = buf.read(rb_len)
        if len(rb_bytes) != rb_len:
            raise ValueError("incomplete record batch in partial topic-wal span")

        batches.append(KafkaRecordBatch.from_bytes(rb_bytes))

    return batches


async def read_topic_wal_partition_batches(
    config: Config,
    *,
    uri: str,
    topic: str,
    partition: int,
    byte_start: int,
    byte_end: int,
) -> list[KafkaRecordBatch]:
    object_key = normalize_object_key(config, uri)

    if byte_end > byte_start and hasattr(config.store, "get_range_async"):
        try:
            span = await config.store.get_range_async(
                object_key,
                start=byte_start,
                length=(byte_end - byte_start),
            )
            span_batches = _decode_partial_record_batch_span(_to_bytes(span))
            if span_batches:
                return span_batches
        except Exception:
            # Fallback to full object reads when range APIs are unavailable or
            # spans cannot be decoded as standalone batch bytes.
            pass

    get_result = await config.store.get_async(object_key)
    data = await get_result.bytes_async()
    decoded = decode_kafka_wal_file(bytes(data))

    return [
        b.kafka_record_batch
        for b in decoded.batches
        if b.topic == topic and b.partition == partition
    ]
