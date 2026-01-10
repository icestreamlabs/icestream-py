import pytest

from icestream.kafkaserver.handler import PRODUCE_API_KEY, UNIMPLEMENTED_GROUP_API_KEYS


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
