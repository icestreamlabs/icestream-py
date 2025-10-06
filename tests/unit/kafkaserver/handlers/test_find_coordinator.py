import pytest
from kio.schema.errors import ErrorCode
from kio.schema.find_coordinator.v6 import FindCoordinatorRequest
from kio.static.primitive import i8

from icestream.kafkaserver.handlers.find_coordinator import do_find_coordinator


def _make_req(key_type: int, keys: tuple[str, ...]) -> FindCoordinatorRequest:
    return FindCoordinatorRequest(key_type=i8(key_type), coordinator_keys=keys)

@pytest.mark.asyncio
@pytest.mark.parametrize("key_type,keys", [
    (0, ("group-a",)),
    (1, ("txn-abc",)),
])
async def test_single_key_returns_coordinator_ok(config, key_type, keys):
    req = _make_req(key_type, keys)
    resp = await do_find_coordinator(config, req, api_version=6)

    assert len(resp.coordinators) == 1

    coordinator = resp.coordinators[0]
    assert coordinator.key == keys[0]
    assert coordinator.host == config.ADVERTISED_HOST
    assert coordinator.port == config.ADVERTISED_PORT
    assert int(coordinator.node_id) == 0
    assert coordinator.error_code == ErrorCode.none
    assert coordinator.error_message is None

@pytest.mark.asyncio
async def test_empty_keys_returns_error(config):
    req = _make_req(0, ())
    resp = await do_find_coordinator(config, req, api_version=6)

    assert len(resp.coordinators) == 1
    coordinator = resp.coordinators[0]
    assert coordinator.error_code != ErrorCode.none

@pytest.mark.asyncio
async def test_multiple_keys_returns_all(config):
    keys = ("g1", "g2", "g3")
    req = _make_req(0, keys)
    resp = await do_find_coordinator(config, req, api_version=6)
    assert len(resp.coordinators) == len(keys)
    for c in resp.coordinators:
        assert c.error_code == ErrorCode.none

@pytest.mark.asyncio
async def test_invalid_key_type_yields_error(config):
    req = _make_req(5, ("k",))
    resp = await do_find_coordinator(config, req, api_version=6)
    assert len(resp.coordinators) == 1
    assert resp.coordinators[0].error_code == ErrorCode.invalid_request
