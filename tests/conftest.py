import asyncio
import os
import re
import shlex
import socket
import tempfile
import time
from contextlib import suppress
from pathlib import Path
from typing import Optional

import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from icestream.config import Config
from icestream.db import run_migrations


def _pick_port(preferred: int = 6543) -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("127.0.0.1", preferred))
            return preferred
        except OSError:
            sock.bind(("127.0.0.1", 0))
            return sock.getsockname()[1]


def _normalize_db_url(url: str) -> str:
    parsed = make_url(url)
    driver = parsed.drivername
    if driver == "postgres":
        driver = "postgresql"
    if not driver.startswith("postgresql+asyncpg"):
        driver = "postgresql+asyncpg"
    parsed = parsed.set(drivername=driver)
    return str(parsed)


def _build_url(*, host: str, port: int, database: str) -> str:
    return f"postgresql+asyncpg://{host}:{port}/{database}"


def _parse_listen_address(token: str) -> Optional[tuple[str, int]]:
    token = token.strip().strip(",")
    if token.startswith("[") and "]" in token:
        host, _, rest = token[1:].partition("]")
        if rest.startswith(":") and rest[1:].isdigit():
            return host, int(rest[1:])
        return None
    if ":" not in token:
        return None
    host, port_str = token.rsplit(":", 1)
    if not port_str.isdigit():
        return None
    return host, int(port_str)


def _extract_url(line: str) -> Optional[str]:
    for token in line.split():
        cleaned = token.strip().strip("'\"").strip(",")
        if cleaned.startswith("postgres://") or cleaned.startswith("postgresql://"):
            return cleaned
    return None


def _extract_listen_address(line: str) -> Optional[tuple[str, int]]:
    match = re.search(r"listening on\s+([^\s]+)", line)
    if not match:
        return None
    return _parse_listen_address(match.group(1))


async def _drain_output(proc: asyncio.subprocess.Process, lines: list[str]) -> None:
    if proc.stdout is None:
        return
    while True:
        line = await proc.stdout.readline()
        if not line:
            break
        lines.append(line.decode().rstrip())


async def _wait_for_db(url: str, timeout_seconds: float = 5.0) -> None:
    engine = create_async_engine(url, future=True)
    start = time.time()
    last_error: Optional[Exception] = None
    try:
        while time.time() - start < timeout_seconds:
            try:
                async with engine.connect() as conn:
                    await conn.execute(text("SELECT 1"))
                return
            except Exception as exc:
                last_error = exc
                await asyncio.sleep(0.2)
    finally:
        await engine.dispose()

    detail = f": {last_error}" if last_error else ""
    raise RuntimeError(f"mockgres did not become ready{detail}")


@pytest_asyncio.fixture(scope="session", loop_scope="session", autouse=True)
async def _ensure_test_db():
    if os.getenv("ICESTREAM_DATABASE_URL"):
        yield
        return

    cmd_env = os.getenv("POSTGRES_BIN_PATH")
    host = "127.0.0.1"
    port = _pick_port()
    temp_dir = tempfile.TemporaryDirectory(prefix="icestream-mockgres-")
    data_dir = temp_dir.name

    if cmd_env:
        cmd = [
            arg.replace("{port}", str(port)).replace("{data_dir}", str(data_dir))
            for arg in shlex.split(cmd_env)
        ]
    else:
        cmd = ["mockgres", "--host", host, "--port", str(port)]

    is_mockgres = cmd and Path(cmd[0]).name == "mockgres"
    url: Optional[str] = None
    if is_mockgres and not cmd_env:
        url = _build_url(host=host, port=port, database="postgres")

    env = os.environ.copy()
    env["PGPORT"] = str(port)
    env["PGDATA"] = str(data_dir)

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )

    listen_host: Optional[str] = None
    listen_port: Optional[int] = None
    output_lines: list[str] = []
    output_task = asyncio.create_task(_drain_output(proc, output_lines))

    if url is None:
        start = time.time()
        next_index = 0
        while time.time() - start < 5:
            while next_index < len(output_lines):
                line = output_lines[next_index]
                next_index += 1
                if url is None:
                    url = _extract_url(line)
                if listen_host is None:
                    addr = _extract_listen_address(line)
                    if addr:
                        listen_host, listen_port = addr
            if url:
                break
            if proc.returncode is not None:
                break
            await asyncio.sleep(0.05)

    if not url:
        host = listen_host or "localhost"
        port = listen_port or port
        url = _build_url(host=host, port=port, database="postgres")

    db_url = _normalize_db_url(url)
    os.environ["ICESTREAM_DATABASE_URL"] = db_url

    config = Config()
    if proc.returncode is not None:
        tail = "\n".join(output_lines[-10:])
        detail = f"\nmockgres output:\n{tail}" if tail else ""
        with suppress(Exception):
            proc.terminate()
        with suppress(Exception):
            await asyncio.wait_for(proc.wait(), timeout=5)
        with suppress(Exception):
            proc.kill()
        with suppress(Exception):
            await asyncio.wait_for(proc.wait(), timeout=5)
        with suppress(Exception):
            temp_dir.cleanup()
        if not output_task.done():
            output_task.cancel()
        with suppress(asyncio.CancelledError):
            await output_task
        raise RuntimeError(f"mockgres exited before migrations started{detail}")

    try:
        await _wait_for_db(db_url, timeout_seconds=10.0)
        await run_migrations(config)
        if is_mockgres:
            os.environ["ICESTREAM_USING_MOCKGRES"] = "1"
    except Exception as exc:
        detail_text = str(exc)
        detail = f": {detail_text}" if detail_text else f": {exc!r}"
        tail = "\n".join(output_lines[-10:])
        output_detail = f"\nmockgres output:\n{tail}" if tail else ""
        with suppress(Exception):
            proc.terminate()
        with suppress(Exception):
            await asyncio.wait_for(proc.wait(), timeout=5)
        with suppress(Exception):
            proc.kill()
        with suppress(Exception):
            await asyncio.wait_for(proc.wait(), timeout=5)
        with suppress(Exception):
            temp_dir.cleanup()
        if not output_task.done():
            output_task.cancel()
        with suppress(asyncio.CancelledError):
            await output_task
        raise RuntimeError(
            f"mockgres started but migrations did not complete{detail}{output_detail}"
        ) from exc

    try:
        yield
    finally:
        with suppress(Exception):
            proc.terminate()
        with suppress(Exception):
            await asyncio.wait_for(proc.wait(), timeout=5)
        with suppress(Exception):
            proc.kill()
        with suppress(Exception):
            await asyncio.wait_for(proc.wait(), timeout=5)
        with suppress(Exception):
            temp_dir.cleanup()
        if not output_task.done():
            output_task.cancel()
        with suppress(asyncio.CancelledError):
            await output_task
