import pytest

from icestream.kafkaserver.handler import (
    CONSUMER_GROUP_DESCRIBE_API_KEY,
    CONSUMER_GROUP_HEARTBEAT_API_KEY,
    DELETE_GROUPS_API_KEY,
    DESCRIBE_GROUPS_API_KEY,
    FIND_COORDINATOR_API_KEY,
    HEARTBEAT_API_KEY,
    JOIN_GROUP_API_KEY,
    LEAVE_GROUP_API_KEY,
    LIST_GROUPS_API_KEY,
    OFFSET_COMMIT_API_KEY,
    OFFSET_DELETE_API_KEY,
    OFFSET_FETCH_API_KEY,
    PRODUCE_API_KEY,
    TXN_OFFSET_COMMIT_API_KEY,
    UNIMPLEMENTED_GROUP_API_KEYS,
)


IMPLEMENTED_CLASSIC_GROUP_API_KEYS = frozenset(
    {
        FIND_COORDINATOR_API_KEY,
        JOIN_GROUP_API_KEY,
        HEARTBEAT_API_KEY,
        LEAVE_GROUP_API_KEY,
        LIST_GROUPS_API_KEY,
        DESCRIBE_GROUPS_API_KEY,
        OFFSET_COMMIT_API_KEY,
        OFFSET_FETCH_API_KEY,
        OFFSET_DELETE_API_KEY,
    }
)

HIDDEN_GROUP_API_KEYS = frozenset(
    {
        DELETE_GROUPS_API_KEY,
        TXN_OFFSET_COMMIT_API_KEY,
        CONSUMER_GROUP_DESCRIBE_API_KEY,
        CONSUMER_GROUP_HEARTBEAT_API_KEY,
    }
)


@pytest.mark.asyncio
async def test_api_versions_hides_unimplemented_group_apis(handler):
    responses = []

    async def capture(resp):
        responses.append(resp)

    await handler.handle_api_versions_request(None, None, 4, capture)

    assert responses
    resp = responses[0]
    advertised = {int(api.api_key) for api in resp.api_keys}

    assert PRODUCE_API_KEY in advertised
    for api_key in UNIMPLEMENTED_GROUP_API_KEYS:
        assert api_key not in advertised


@pytest.mark.asyncio
async def test_api_versions_advertises_implemented_classic_group_apis(handler):
    responses = []

    async def capture(resp):
        responses.append(resp)

    await handler.handle_api_versions_request(None, None, 4, capture)
    advertised = {int(api.api_key) for api in responses[0].api_keys}

    for api_key in IMPLEMENTED_CLASSIC_GROUP_API_KEYS:
        assert api_key in advertised
    for api_key in HIDDEN_GROUP_API_KEYS:
        assert api_key not in advertised


def test_implemented_group_apis_never_reenter_unimplemented_set():
    overlap = IMPLEMENTED_CLASSIC_GROUP_API_KEYS & UNIMPLEMENTED_GROUP_API_KEYS
    assert not overlap
