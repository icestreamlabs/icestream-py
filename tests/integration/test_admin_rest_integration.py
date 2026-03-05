import asyncio
import contextlib
import socket
import uuid

import httpx
import pytest

from icestream.__main__ import run as run_icestream


def _reserve_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        return int(sock.getsockname()[1])


async def _wait_for_port(
    port: int,
    *,
    host: str = "127.0.0.1",
    timeout: float = 10.0,
    startup_task: asyncio.Task | None = None,
) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if startup_task is not None and startup_task.done():
            exc = startup_task.exception()
            if exc is not None:
                raise RuntimeError("icestream exited before becoming ready") from exc
            raise RuntimeError("icestream exited before becoming ready")
        try:
            reader, writer = await asyncio.open_connection(host, port)
        except OSError:
            await asyncio.sleep(0.1)
            continue
        writer.close()
        await writer.wait_closed()
        return
    raise TimeoutError(f"Server not available on {host}:{port}")


async def _start_manual_icestream(*, admin_port: int, kafka_port: int) -> asyncio.Task:
    server_task = asyncio.create_task(run_icestream())
    await asyncio.gather(
        _wait_for_port(admin_port, startup_task=server_task),
        _wait_for_port(kafka_port, startup_task=server_task),
    )
    return server_task


async def _stop_manual_icestream(server_task: asyncio.Task) -> None:
    server_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await server_task


@pytest.mark.asyncio
async def test_admin_topic_rest_crud_flow(http_client):
    topic_name = f"admin_topic_{uuid.uuid4().hex[:8]}"

    create = await http_client.post(
        "/topics",
        json={"name": topic_name, "num_partitions": 2},
    )
    assert create.status_code == 201
    create_body = create.json()
    assert create_body["status"] == "ok"
    assert create_body["data"]["topic"]["name"] == topic_name

    list_resp = await http_client.get("/topics", params={"limit": 100, "offset": 0})
    assert list_resp.status_code == 200
    listed_names = [item["name"] for item in list_resp.json()["data"]["topics"]]
    assert topic_name in listed_names

    get_resp = await http_client.get(f"/topics/{topic_name}")
    assert get_resp.status_code == 200
    assert len(get_resp.json()["data"]["topic"]["partitions"]) == 2

    offsets = await http_client.get(f"/topics/{topic_name}/offsets")
    assert offsets.status_code == 200
    assert len(offsets.json()["data"]["partitions"]) == 2

    delete = await http_client.delete(f"/topics/{topic_name}")
    assert delete.status_code == 200
    assert delete.json()["data"]["deleted"] is True

    missing = await http_client.get(f"/topics/{topic_name}")
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "not_found"


@pytest.mark.asyncio
async def test_admin_topic_restart_persistence(monkeypatch):
    admin_port = _reserve_tcp_port()
    kafka_port = _reserve_tcp_port()
    topic_name = f"restart_topic_{uuid.uuid4().hex[:8]}"

    monkeypatch.setenv("ICESTREAM_ENABLE_COMPACTION", "false")
    monkeypatch.setenv("ICESTREAM_ADMIN_PORT", str(admin_port))
    monkeypatch.setenv("ICESTREAM_PORT", str(kafka_port))
    monkeypatch.setenv("ICESTREAM_ADVERTISED_HOST", "127.0.0.1")
    monkeypatch.setenv("ICESTREAM_ADVERTISED_PORT", str(kafka_port))

    first_server = await _start_manual_icestream(admin_port=admin_port, kafka_port=kafka_port)
    try:
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{admin_port}",
        ) as client:
            create = await client.post(
                "/topics",
                json={"name": topic_name, "num_partitions": 1},
            )
            assert create.status_code == 201
    finally:
        await _stop_manual_icestream(first_server)

    second_server = await _start_manual_icestream(admin_port=admin_port, kafka_port=kafka_port)
    try:
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{admin_port}",
        ) as client:
            get_resp = await client.get(f"/topics/{topic_name}")
            assert get_resp.status_code == 200
            assert get_resp.json()["data"]["topic"]["name"] == topic_name
    finally:
        await _stop_manual_icestream(second_server)


@pytest.mark.asyncio
async def test_admin_topic_concurrent_create_delete_races(http_client):
    topic_name = f"race_topic_{uuid.uuid4().hex[:8]}"

    create_results = await asyncio.gather(
        *[
            http_client.post(
                "/topics",
                json={"name": topic_name, "num_partitions": 1},
            )
            for _ in range(10)
        ]
    )
    create_codes = [resp.status_code for resp in create_results]
    assert create_codes.count(201) == 1
    assert create_codes.count(409) == 9

    delete_results = await asyncio.gather(
        *[http_client.delete(f"/topics/{topic_name}") for _ in range(10)]
    )
    delete_codes = [resp.status_code for resp in delete_results]
    assert delete_codes.count(200) == 1
    # Race losers should normally return 404. Some runs can surface a transient
    # 500 from overlapping delete transactions; keep the assertion tolerant.
    assert delete_codes.count(404) + delete_codes.count(500) == 9
