import uuid

import httpx
import pytest

from icestream.admin import AdminApi


@pytest.mark.asyncio
async def test_create_topic_invalid_payload_maps_to_invalid_request(config):
    api = AdminApi(config)
    transport = httpx.ASGITransport(app=api.app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
    ) as client:
        response = await client.post(
            "/topics",
            json={"name": "bad name with spaces", "num_partitions": 1},
        )

    assert response.status_code == 400
    payload = response.json()
    assert payload["status"] == "error"
    assert payload["error"]["code"] == "invalid_request"


@pytest.mark.asyncio
async def test_create_topic_conflict_maps_to_already_exists(config):
    api = AdminApi(config)
    topic_name = f"topic_{uuid.uuid4().hex[:8]}"
    transport = httpx.ASGITransport(app=api.app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
    ) as client:
        first = await client.post(
            "/topics",
            json={"name": topic_name, "num_partitions": 1},
        )
        second = await client.post(
            "/topics",
            json={"name": topic_name, "num_partitions": 1},
        )

    assert first.status_code == 201
    assert second.status_code == 409
    payload = second.json()
    assert payload["error"]["code"] == "already_exists"


@pytest.mark.asyncio
async def test_get_unknown_topic_maps_to_not_found(config):
    api = AdminApi(config)
    transport = httpx.ASGITransport(app=api.app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
    ) as client:
        response = await client.get("/topics/missing_topic")

    assert response.status_code == 404
    payload = response.json()
    assert payload["error"]["code"] == "not_found"


@pytest.mark.asyncio
async def test_delete_internal_topic_maps_to_invalid_request(config):
    api = AdminApi(config)
    transport = httpx.ASGITransport(app=api.app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
    ) as client:
        response = await client.delete("/topics/__consumer_offsets")

    assert response.status_code == 400
    payload = response.json()
    assert payload["error"]["code"] == "invalid_request"
