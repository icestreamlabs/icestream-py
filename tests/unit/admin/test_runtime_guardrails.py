import asyncio

import httpx
import pytest

from icestream.admin import AdminApi
from icestream.config import Config


def test_admin_config_invalid_port(monkeypatch):
    monkeypatch.setenv("ICESTREAM_ADMIN_API_ENABLED", "true")
    monkeypatch.setenv("ICESTREAM_ADMIN_PORT", "0")
    with pytest.raises(ValueError, match="ICESTREAM_ADMIN_PORT"):
        Config()


def test_admin_config_invalid_request_limits(monkeypatch):
    monkeypatch.setenv("ICESTREAM_ADMIN_API_ENABLED", "true")
    monkeypatch.setenv("ICESTREAM_ADMIN_MAX_REQUEST_BYTES", "0")
    with pytest.raises(ValueError, match="ICESTREAM_ADMIN_MAX_REQUEST_BYTES"):
        Config()


def test_segment_cache_invalid_mode(monkeypatch):
    monkeypatch.setenv("ICESTREAM_SEGMENT_CACHE_MODE", "unknown")
    with pytest.raises(ValueError, match="ICESTREAM_SEGMENT_CACHE_MODE"):
        Config()


def test_segment_cache_hybrid_requires_disk_capacity(monkeypatch):
    monkeypatch.setenv("ICESTREAM_SEGMENT_CACHE_ENABLED", "true")
    monkeypatch.setenv("ICESTREAM_SEGMENT_CACHE_MODE", "hybrid")
    monkeypatch.setenv("ICESTREAM_SEGMENT_CACHE_DISK_CAPACITY_BYTES", "0")
    with pytest.raises(ValueError, match="ICESTREAM_SEGMENT_CACHE_DISK_CAPACITY_BYTES"):
        Config()


@pytest.mark.asyncio
async def test_admin_config_allows_disabled_admin_api_with_invalid_port(monkeypatch):
    monkeypatch.setenv("ICESTREAM_ADMIN_API_ENABLED", "false")
    monkeypatch.setenv("ICESTREAM_ADMIN_PORT", "0")
    cfg = Config()
    try:
        assert cfg.ADMIN_API_ENABLED is False
    finally:
        if cfg.engine is not None:
            await cfg.engine.dispose()


@pytest.mark.asyncio
async def test_admin_middleware_rejects_oversized_request_body(monkeypatch):
    monkeypatch.setenv("ICESTREAM_ADMIN_MAX_REQUEST_BYTES", "8")
    monkeypatch.setenv("ICESTREAM_ADMIN_REQUEST_TIMEOUT_SECONDS", "1")
    cfg = Config()
    try:
        api = AdminApi(cfg)
        transport = httpx.ASGITransport(app=api.app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://testserver",
        ) as client:
            response = await client.request("GET", "/healthz", content=b"0123456789")
        assert response.status_code == 413
        payload = response.json()
        assert payload["error"]["code"] == "request_too_large"
    finally:
        if cfg.engine is not None:
            await cfg.engine.dispose()


@pytest.mark.asyncio
async def test_admin_middleware_enforces_request_timeout(monkeypatch):
    monkeypatch.setenv("ICESTREAM_ADMIN_REQUEST_TIMEOUT_SECONDS", "0.01")
    cfg = Config()
    try:
        api = AdminApi(cfg)

        @api.app.get("/_slow")
        async def _slow():
            await asyncio.sleep(0.05)
            return {"ok": True}

        transport = httpx.ASGITransport(app=api.app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://testserver",
        ) as client:
            response = await client.get("/_slow")
        assert response.status_code == 408
        payload = response.json()
        assert payload["error"]["code"] == "request_timeout"
    finally:
        if cfg.engine is not None:
            await cfg.engine.dispose()


@pytest.mark.asyncio
async def test_admin_middleware_propagates_request_id(monkeypatch):
    monkeypatch.setenv("ICESTREAM_ADMIN_MAX_REQUEST_BYTES", "1024")
    cfg = Config()
    try:
        api = AdminApi(cfg)
        transport = httpx.ASGITransport(app=api.app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://testserver",
        ) as client:
            response = await client.get(
                "/healthz",
                headers={"x-request-id": "req-123"},
            )
        assert response.status_code == 200
        assert response.headers["x-request-id"] == "req-123"
        payload = response.json()
        assert payload["request_id"] == "req-123"
    finally:
        if cfg.engine is not None:
            await cfg.engine.dispose()
