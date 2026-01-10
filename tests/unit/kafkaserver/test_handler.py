import datetime
import inspect
import io
import struct
from unittest.mock import patch, AsyncMock, MagicMock

import pytest
from kio.index import load_payload_module, load_response_schema
from kio.schema.errors import ErrorCode
from kio.schema.types import TopicName
from kio.serial import entity_writer, entity_reader
from kio.static.constants import EntityType
from kio.static.primitive import i32, i16, i32Timedelta

from icestream.kafkaserver.handler import (
    handle_kafka_request,
    api_compatibility,
    PRODUCE_API_KEY,
    METADATA_API_KEY,
)
import icestream.kafkaserver.handler as handler_module


def make_buf_for_version(version: int, trailing: bytes = b"X") -> bytes:
    return b"\x00\x00" + struct.pack(">H", version) + trailing


def build_produce_request_buf(
    version: int, *, correlation_id: int = 12345, records=None
) -> bytes:
    mod = load_payload_module(PRODUCE_API_KEY, version, EntityType.request)
    req_cls = mod.ProduceRequest

    write_header = entity_writer(req_cls.__header_schema__)
    write_request = entity_writer(req_cls)

    req_header = req_cls.__header_schema__(
        request_api_key=PRODUCE_API_KEY,
        request_api_version=version,
        correlation_id=i32(correlation_id),
        client_id="test",
    )
    req = req_cls(
        acks=i16(0),
        timeout=i32Timedelta.parse(datetime.timedelta(milliseconds=0)),
        topic_data=(
            mod.TopicProduceData(
                name=TopicName("test_topic"),
                partition_data=(
                    mod.PartitionProduceData(index=i32(0), records=records),
                ),
            ),
        ),
    )
    buf = io.BytesIO()
    write_header(buf, req_header)
    write_request(buf, req)
    return buf.getvalue()


@pytest.mark.asyncio
async def test_unknown_api_key_returns_early(stream_writer, handler):
    unknown_key = -1
    buf = make_buf_for_version(0)
    await handle_kafka_request(unknown_key, buf, handler, stream_writer)
    stream_writer.assert_not_awaited()


@pytest.mark.asyncio
async def test_version_out_of_range_triggers_error_response(stream_writer, handler):
    out_of_range_version = api_compatibility[PRODUCE_API_KEY][-1] + 1
    buf = build_produce_request_buf(out_of_range_version, correlation_id=12345)

    await handle_kafka_request(PRODUCE_API_KEY, buf, handler, stream_writer)

    payload = stream_writer.write.call_args[0][0]

    resp_cls = load_response_schema(PRODUCE_API_KEY, out_of_range_version)
    read_header = entity_reader(resp_cls.__header_schema__)
    read_resp = entity_reader(resp_cls)

    resp_buf = io.BytesIO(payload[4:])
    _ = read_header(resp_buf)
    resp = read_resp(resp_buf)

    assert (
        resp.responses[0].partition_responses[0].error_code
        == ErrorCode.unsupported_version
    )


@pytest.mark.asyncio
async def test_supported_version_calls_handler_without_error(stream_writer, handler):
    version = api_compatibility[PRODUCE_API_KEY][-1]  # use max supported
    buf = build_produce_request_buf(version, correlation_id=777)

    async def _handle(header, request, api_version, respond):
        assert header.correlation_id == i32(777)
        assert api_version == version

    with patch.object(
        handler, "handle_produce_request", new=AsyncMock(side_effect=_handle)
    ) as mocked:
        await handle_kafka_request(PRODUCE_API_KEY, buf, handler, stream_writer)
        mocked.assert_awaited()

    # don't expect anything to be written
    stream_writer.assert_not_awaited()


@pytest.mark.asyncio
async def test_exception_in_handler_is_swallowed_with_produce(stream_writer, handler):
    version = api_compatibility[PRODUCE_API_KEY][0]
    buf = build_produce_request_buf(version, correlation_id=999)

    with patch.object(
        handler,
        "handle_produce_request",
        new=AsyncMock(side_effect=RuntimeError("boom")),
    ):
        # should not raise
        await handle_kafka_request(PRODUCE_API_KEY, buf, handler, stream_writer)

    stream_writer.assert_not_awaited()


def _iter_forwarding_handlers():
    for name, fn in inspect.getmembers(handler_module, inspect.iscoroutinefunction):
        if not name.startswith("handle_"):
            continue
        if name == "handle_kafka_request":
            continue
        if len(inspect.signature(fn).parameters) != 5:
            continue
        yield name, fn


@pytest.mark.asyncio
@pytest.mark.parametrize("name, fn", list(_iter_forwarding_handlers()))
async def test_handler_wrappers_forward_calls(name, fn):
    handler = MagicMock()
    method_name = f"{name}_request"
    method = AsyncMock()
    setattr(handler, method_name, method)
    header = object()
    request = object()
    respond = AsyncMock()

    await fn(handler, header, request, 7, respond)

    method.assert_awaited_once_with(header, request, 7, respond)
