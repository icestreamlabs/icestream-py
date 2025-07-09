import socket
import subprocess
import time

import httpx
import pytest


def wait_for_port(port, host="localhost", timeout=5.0):
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.1)
    raise TimeoutError(f"Server not available on {host}:{port}")


@pytest.fixture(scope="session", autouse=True)
def start_icestream():
    proc = subprocess.Popen(["python", "-m", "icestream"])
    wait_for_port(8080)  # admin api
    wait_for_port(9092)  # kafka
    yield
    proc.terminate()
    proc.wait()


@pytest.fixture
async def http_client(start_icestream):
    async with httpx.AsyncClient(base_url="http://localhost:8080") as client:
        yield client
