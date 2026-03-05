import asyncio
import contextlib
import socket

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import text

from icestream.config import Config
from icestream.__main__ import run as run_icestream


def _reserve_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        return int(sock.getsockname()[1])


async def wait_for_port(
    port: int,
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


@pytest.fixture
def icestream_ports(monkeypatch, request):
    admin_port = _reserve_tcp_port()
    kafka_port = _reserve_tcp_port()
    enable_compaction = bool(request.node.get_closest_marker("enable_compaction"))

    # Keep integration tests fast and deterministic by flushing each produce
    # batch immediately and minimizing rebalance delay.
    monkeypatch.setenv("ICESTREAM_FLUSH_SIZE", "1")
    monkeypatch.setenv("ICESTREAM_FLUSH_INTERVAL", "0.05")
    monkeypatch.setenv("ICESTREAM_FLUSH_TIMEOUT", "5")
    monkeypatch.setenv("ICESTREAM_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")

    monkeypatch.setenv(
        "ICESTREAM_ENABLE_COMPACTION", "true" if enable_compaction else "false"
    )
    if enable_compaction:
        monkeypatch.setenv("ICESTREAM_COMPACTION_FORMAT", "topic_wal")
        monkeypatch.setenv("ICESTREAM_COMPACTION_INTERVAL", "1")
        monkeypatch.setenv("ICESTREAM_MAX_COMPACTION_SELECT_LIMIT", "50")
        monkeypatch.setenv("ICESTREAM_MAX_COMPACTION_WAL_FILES", "50")
        monkeypatch.setenv("ICESTREAM_MAX_COMPACTION_BYTES", str(50 * 1024 * 1024))
        monkeypatch.setenv("ICESTREAM_TOPIC_WAL_TARGET_BYTES", str(1024 * 1024))

    monkeypatch.setenv("ICESTREAM_ADMIN_PORT", str(admin_port))
    monkeypatch.setenv("ICESTREAM_PORT", str(kafka_port))
    monkeypatch.setenv("ICESTREAM_ADVERTISED_HOST", "127.0.0.1")
    monkeypatch.setenv("ICESTREAM_ADVERTISED_PORT", str(kafka_port))

    return {
        "admin_port": admin_port,
        "kafka_port": kafka_port,
    }


@pytest_asyncio.fixture(autouse=True)
async def start_icestream(icestream_ports):
    cfg = Config()
    assert cfg.engine is not None
    async with cfg.engine.begin() as conn:
        for table in (
            "topic_wal_file_sources",
            "topic_wal_file_offsets",
            "topic_wal_files",
            "wal_file_offsets",
            "wal_files",
        ):
            await conn.execute(
                text(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")
            )
    await cfg.engine.dispose()

    server_task = asyncio.create_task(run_icestream())
    try:
        await asyncio.gather(
            wait_for_port(icestream_ports["admin_port"], startup_task=server_task),
            wait_for_port(icestream_ports["kafka_port"], startup_task=server_task),
        )
        yield icestream_ports
    finally:
        server_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await server_task


@pytest_asyncio.fixture
async def http_client(start_icestream):
    admin_port = start_icestream["admin_port"]
    async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{admin_port}") as client:
        yield client


@pytest.fixture
def bootstrap_servers(start_icestream) -> str:
    return f"127.0.0.1:{start_icestream['kafka_port']}"
