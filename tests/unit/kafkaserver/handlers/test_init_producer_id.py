from types import SimpleNamespace

import pytest
from kio.schema.errors import ErrorCode
from sqlalchemy import select

from icestream.kafkaserver.handlers.init_producer_id import do_init_producer_id
from icestream.models import ProducerSession


@pytest.mark.asyncio
async def test_init_producer_id_allocates_non_transactional_session(config):
    req = SimpleNamespace(transactional_id=None, producer_id=-1, producer_epoch=-1)

    resp = await do_init_producer_id(config, req, api_version=5)

    assert resp.error_code == ErrorCode.none
    assert int(resp.producer_id) >= 0
    assert int(resp.producer_epoch) == 0

    async with config.async_session_factory() as session:
        rows = (
            await session.execute(
                select(ProducerSession).where(
                    ProducerSession.producer_id == int(resp.producer_id)
                )
            )
        ).scalars().all()

    assert len(rows) == 1
    assert rows[0].transactional_id is None


@pytest.mark.asyncio
async def test_init_producer_id_reuses_transactional_session_and_bumps_epoch(config):
    init_req = SimpleNamespace(transactional_id="txn-reuse", producer_id=-1, producer_epoch=-1)
    first = await do_init_producer_id(config, init_req, api_version=5)

    reuse_req = SimpleNamespace(
        transactional_id="txn-reuse",
        producer_id=int(first.producer_id),
        producer_epoch=int(first.producer_epoch),
    )
    second = await do_init_producer_id(config, reuse_req, api_version=5)

    assert first.error_code == ErrorCode.none
    assert second.error_code == ErrorCode.none
    assert int(second.producer_id) == int(first.producer_id)
    assert int(second.producer_epoch) == int(first.producer_epoch) + 1


@pytest.mark.asyncio
async def test_init_producer_id_rejects_stale_epoch_for_transactional_session(config):
    init_req = SimpleNamespace(transactional_id="txn-stale", producer_id=-1, producer_epoch=-1)
    first = await do_init_producer_id(config, init_req, api_version=5)

    bump_req = SimpleNamespace(
        transactional_id="txn-stale",
        producer_id=int(first.producer_id),
        producer_epoch=int(first.producer_epoch),
    )
    second = await do_init_producer_id(config, bump_req, api_version=5)

    stale_req = SimpleNamespace(
        transactional_id="txn-stale",
        producer_id=int(first.producer_id),
        producer_epoch=int(first.producer_epoch),
    )
    stale_resp = await do_init_producer_id(config, stale_req, api_version=5)

    assert second.error_code == ErrorCode.none
    assert stale_resp.error_code == ErrorCode.invalid_producer_epoch
    assert int(stale_resp.producer_id) == int(second.producer_id)
    assert int(stale_resp.producer_epoch) == int(second.producer_epoch)
