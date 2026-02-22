import io
from unittest.mock import AsyncMock, patch

from kio.schema.consumer_group_describe.v0.request import (
    ConsumerGroupDescribeRequest,
)
from kio.schema.consumer_group_heartbeat.v0.request import (
    ConsumerGroupHeartbeatRequest,
)
import kio.schema.find_coordinator.v6 as find_coordinator_v6
import kio.schema.heartbeat.v4 as heartbeat_v4
import kio.schema.join_group.v9 as join_group_v9
import pytest
from kio.index import load_response_schema
from kio.schema.errors import ErrorCode
from kio.schema.find_coordinator.v6.request import FindCoordinatorRequest
from kio.schema.heartbeat.v4.request import HeartbeatRequest
from kio.schema.join_group.v9.request import JoinGroupRequest, JoinGroupRequestProtocol
from kio.schema.types import BrokerId, GroupId
from kio.serial import entity_reader, entity_writer
from kio.static.primitive import i32

from icestream.kafkaserver.handler import (
    CONSUMER_GROUP_DESCRIBE_API_KEY,
    CONSUMER_GROUP_HEARTBEAT_API_KEY,
    FIND_COORDINATOR_API_KEY,
    HEARTBEAT_API_KEY,
    JOIN_GROUP_API_KEY,
    handle_kafka_request,
)
from tests.unit.conftest import i32timedelta_from_ms


def _build_request_buffer(api_key: int, api_version: int, request: object) -> bytes:
    request_cls = type(request)
    header_cls = request_cls.__header_schema__
    write_header = entity_writer(header_cls)
    write_request = entity_writer(request_cls)
    header = header_cls(
        request_api_key=api_key,
        request_api_version=api_version,
        correlation_id=i32(42),
        client_id="tests",
    )
    buf = io.BytesIO()
    write_header(buf, header)
    write_request(buf, request)
    return buf.getvalue()


def _decode_response(api_key: int, api_version: int, payload: bytes):
    response_cls = load_response_schema(api_key, api_version)
    read_header = entity_reader(response_cls.__header_schema__)
    read_response = entity_reader(response_cls)
    buf = io.BytesIO(payload[4:])
    _ = read_header(buf)
    return read_response(buf)


@pytest.mark.asyncio
async def test_handle_kafka_request_wires_find_coordinator(handler, stream_writer):
    req = FindCoordinatorRequest(key_type=0, coordinator_keys=("cg-1",))
    buffer = _build_request_buffer(FIND_COORDINATOR_API_KEY, 6, req)
    expected = find_coordinator_v6.response.FindCoordinatorResponse(
        throttle_time=i32timedelta_from_ms(0),
        coordinators=(
            find_coordinator_v6.response.Coordinator(
                key="cg-1",
                node_id=BrokerId(0),
                host="localhost",
                port=i32(9092),
                error_code=ErrorCode.none,
                error_message=None,
            ),
        ),
    )

    with patch(
        "icestream.kafkaserver.server.do_find_coordinator",
        new=AsyncMock(return_value=expected),
    ) as mocked:
        await handle_kafka_request(FIND_COORDINATOR_API_KEY, buffer, handler, stream_writer)

    mocked.assert_awaited_once_with(handler.server.config, req, 6)
    payload = stream_writer.write.call_args[0][0]
    resp = _decode_response(FIND_COORDINATOR_API_KEY, 6, payload)
    assert resp.coordinators[0].error_code == ErrorCode.none
    assert resp.coordinators[0].key == "cg-1"


@pytest.mark.asyncio
async def test_handle_kafka_request_wires_join_group(handler, stream_writer):
    req = JoinGroupRequest(
        group_id=GroupId("cg-join"),
        session_timeout=i32timedelta_from_ms(10_000),
        rebalance_timeout=i32timedelta_from_ms(30_000),
        member_id="member-1",
        group_instance_id=None,
        protocol_type="consumer",
        protocols=(JoinGroupRequestProtocol(name="range", metadata=b""),),
        reason=None,
    )
    buffer = _build_request_buffer(JOIN_GROUP_API_KEY, 9, req)
    expected = join_group_v9.response.JoinGroupResponse(
        throttle_time=i32timedelta_from_ms(0),
        error_code=ErrorCode.none,
        generation_id=i32(3),
        protocol_type="consumer",
        protocol_name="range",
        leader="member-1",
        skip_assignment=False,
        member_id="member-1",
        members=tuple(),
    )

    with patch(
        "icestream.kafkaserver.server.do_join_group",
        new=AsyncMock(return_value=expected),
    ) as mocked:
        await handle_kafka_request(JOIN_GROUP_API_KEY, buffer, handler, stream_writer)

    mocked.assert_awaited_once_with(handler.server.config, req, 9)
    payload = stream_writer.write.call_args[0][0]
    resp = _decode_response(JOIN_GROUP_API_KEY, 9, payload)
    assert resp.error_code == ErrorCode.none
    assert resp.member_id == "member-1"


@pytest.mark.asyncio
async def test_handle_kafka_request_join_group_returns_fallback_on_exception(
    handler, stream_writer
):
    req = JoinGroupRequest(
        group_id=GroupId("cg-join"),
        session_timeout=i32timedelta_from_ms(10_000),
        rebalance_timeout=i32timedelta_from_ms(30_000),
        member_id="member-1",
        group_instance_id=None,
        protocol_type="consumer",
        protocols=(JoinGroupRequestProtocol(name="range", metadata=b""),),
        reason=None,
    )
    buffer = _build_request_buffer(JOIN_GROUP_API_KEY, 9, req)

    with patch(
        "icestream.kafkaserver.server.do_join_group",
        new=AsyncMock(side_effect=RuntimeError("boom")),
    ):
        await handle_kafka_request(JOIN_GROUP_API_KEY, buffer, handler, stream_writer)

    payload = stream_writer.write.call_args[0][0]
    resp = _decode_response(JOIN_GROUP_API_KEY, 9, payload)
    assert resp.error_code == ErrorCode.unknown_server_error


@pytest.mark.asyncio
async def test_handle_kafka_request_wires_heartbeat(handler, stream_writer):
    req = HeartbeatRequest(
        group_id=GroupId("cg-heartbeat"),
        generation_id=i32(1),
        member_id="member-1",
        group_instance_id=None,
    )
    buffer = _build_request_buffer(HEARTBEAT_API_KEY, 4, req)
    expected = heartbeat_v4.response.HeartbeatResponse(
        throttle_time=i32timedelta_from_ms(0),
        error_code=ErrorCode.none,
    )

    with patch(
        "icestream.kafkaserver.server.do_heartbeat",
        new=AsyncMock(return_value=expected),
    ) as mocked:
        await handle_kafka_request(HEARTBEAT_API_KEY, buffer, handler, stream_writer)

    mocked.assert_awaited_once_with(handler.server.config, req, 4)
    payload = stream_writer.write.call_args[0][0]
    resp = _decode_response(HEARTBEAT_API_KEY, 4, payload)
    assert resp.error_code == ErrorCode.none


@pytest.mark.asyncio
async def test_consumer_group_heartbeat_returns_explicit_unsupported_error(
    handler, stream_writer
):
    req = ConsumerGroupHeartbeatRequest(
        group_id=GroupId("cg-new-proto"),
        member_id="m-1",
        member_epoch=i32(1),
        instance_id=None,
        rack_id=None,
        rebalance_timeout=i32timedelta_from_ms(30_000),
        subscribed_topic_names=tuple(),
        server_assignor=None,
        topic_partitions=tuple(),
    )
    buffer = _build_request_buffer(CONSUMER_GROUP_HEARTBEAT_API_KEY, 0, req)
    await handle_kafka_request(
        CONSUMER_GROUP_HEARTBEAT_API_KEY, buffer, handler, stream_writer
    )

    payload = stream_writer.write.call_args[0][0]
    resp = _decode_response(CONSUMER_GROUP_HEARTBEAT_API_KEY, 0, payload)
    assert resp.error_code == ErrorCode.unsupported_version
    assert "not enabled" in (resp.error_message or "")


@pytest.mark.asyncio
async def test_consumer_group_describe_returns_explicit_unsupported_error(
    handler, stream_writer
):
    req = ConsumerGroupDescribeRequest(
        group_ids=("cg-new-proto",),
        include_authorized_operations=False,
    )
    buffer = _build_request_buffer(CONSUMER_GROUP_DESCRIBE_API_KEY, 0, req)
    await handle_kafka_request(
        CONSUMER_GROUP_DESCRIBE_API_KEY, buffer, handler, stream_writer
    )

    payload = stream_writer.write.call_args[0][0]
    resp = _decode_response(CONSUMER_GROUP_DESCRIBE_API_KEY, 0, payload)
    assert len(resp.groups) == 1
    assert resp.groups[0].error_code == ErrorCode.unsupported_version
    assert "not enabled" in (resp.groups[0].error_message or "")
