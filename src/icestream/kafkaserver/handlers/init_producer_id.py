import datetime

from kio.index import load_payload_module
from kio.schema.errors import ErrorCode
from kio.static.constants import EntityType
from kio.static.primitive import i16, i32Timedelta, i64
from sqlalchemy import select

from icestream.config import Config
from icestream.models import ProducerSession

from kio.schema.init_producer_id.v0.request import (
    InitProducerIdRequest as InitProducerIdRequestV0,
)
from kio.schema.init_producer_id.v0.request import (
    RequestHeader as InitProducerIdRequestHeaderV0,
)
from kio.schema.init_producer_id.v0.response import (
    InitProducerIdResponse as InitProducerIdResponseV0,
)
from kio.schema.init_producer_id.v0.response import (
    ResponseHeader as InitProducerIdResponseHeaderV0,
)
from kio.schema.init_producer_id.v1.request import (
    InitProducerIdRequest as InitProducerIdRequestV1,
)
from kio.schema.init_producer_id.v1.request import (
    RequestHeader as InitProducerIdRequestHeaderV1,
)
from kio.schema.init_producer_id.v1.response import (
    InitProducerIdResponse as InitProducerIdResponseV1,
)
from kio.schema.init_producer_id.v1.response import (
    ResponseHeader as InitProducerIdResponseHeaderV1,
)
from kio.schema.init_producer_id.v2.request import (
    InitProducerIdRequest as InitProducerIdRequestV2,
)
from kio.schema.init_producer_id.v2.request import (
    RequestHeader as InitProducerIdRequestHeaderV2,
)
from kio.schema.init_producer_id.v2.response import (
    InitProducerIdResponse as InitProducerIdResponseV2,
)
from kio.schema.init_producer_id.v2.response import (
    ResponseHeader as InitProducerIdResponseHeaderV2,
)
from kio.schema.init_producer_id.v3.request import (
    InitProducerIdRequest as InitProducerIdRequestV3,
)
from kio.schema.init_producer_id.v3.request import (
    RequestHeader as InitProducerIdRequestHeaderV3,
)
from kio.schema.init_producer_id.v3.response import (
    InitProducerIdResponse as InitProducerIdResponseV3,
)
from kio.schema.init_producer_id.v3.response import (
    ResponseHeader as InitProducerIdResponseHeaderV3,
)
from kio.schema.init_producer_id.v4.request import (
    InitProducerIdRequest as InitProducerIdRequestV4,
)
from kio.schema.init_producer_id.v4.request import (
    RequestHeader as InitProducerIdRequestHeaderV4,
)
from kio.schema.init_producer_id.v4.response import (
    InitProducerIdResponse as InitProducerIdResponseV4,
)
from kio.schema.init_producer_id.v4.response import (
    ResponseHeader as InitProducerIdResponseHeaderV4,
)
from kio.schema.init_producer_id.v5.request import (
    InitProducerIdRequest as InitProducerIdRequestV5,
)
from kio.schema.init_producer_id.v5.request import (
    RequestHeader as InitProducerIdRequestHeaderV5,
)
from kio.schema.init_producer_id.v5.response import (
    InitProducerIdResponse as InitProducerIdResponseV5,
)
from kio.schema.init_producer_id.v5.response import (
    ResponseHeader as InitProducerIdResponseHeaderV5,
)


InitProducerIdRequestHeader = (
    InitProducerIdRequestHeaderV0
    | InitProducerIdRequestHeaderV1
    | InitProducerIdRequestHeaderV2
    | InitProducerIdRequestHeaderV3
    | InitProducerIdRequestHeaderV4
    | InitProducerIdRequestHeaderV5
)

InitProducerIdResponseHeader = (
    InitProducerIdResponseHeaderV0
    | InitProducerIdResponseHeaderV1
    | InitProducerIdResponseHeaderV2
    | InitProducerIdResponseHeaderV3
    | InitProducerIdResponseHeaderV4
    | InitProducerIdResponseHeaderV5
)

InitProducerIdRequest = (
    InitProducerIdRequestV0
    | InitProducerIdRequestV1
    | InitProducerIdRequestV2
    | InitProducerIdRequestV3
    | InitProducerIdRequestV4
    | InitProducerIdRequestV5
)

InitProducerIdResponse = (
    InitProducerIdResponseV0
    | InitProducerIdResponseV1
    | InitProducerIdResponseV2
    | InitProducerIdResponseV3
    | InitProducerIdResponseV4
    | InitProducerIdResponseV5
)


INIT_PRODUCER_ID_API_KEY = 22


def _response_module(api_version: int):
    return load_payload_module(
        INIT_PRODUCER_ID_API_KEY,
        api_version,
        EntityType.response,
    )


def _response(
    *,
    api_version: int,
    error_code: ErrorCode,
    producer_id: int,
    producer_epoch: int,
):
    mod = _response_module(api_version)
    return mod.InitProducerIdResponse(
        throttle_time=i32Timedelta.parse(datetime.timedelta(milliseconds=0)),
        error_code=error_code,
        producer_id=i64(producer_id),
        producer_epoch=i16(producer_epoch),
    )


def _normalize_transactional_id(raw: object) -> str | None:
    if raw is None:
        return None
    value = str(raw).strip()
    if not value:
        return None
    return value


async def do_init_producer_id(
    config: Config,
    req: InitProducerIdRequest,
    api_version: int,
) -> InitProducerIdResponse:
    assert config.async_session_factory is not None

    now = datetime.datetime.now(datetime.timezone.utc)
    expires_at = now + datetime.timedelta(seconds=config.PRODUCER_SESSION_TTL_SECONDS)
    transactional_id = _normalize_transactional_id(getattr(req, "transactional_id", None))
    request_producer_id = int(getattr(req, "producer_id", -1))
    request_producer_epoch = int(getattr(req, "producer_epoch", -1))

    async with config.async_session_factory() as session:
        async with session.begin():
            if transactional_id is None:
                producer_session = ProducerSession(
                    transactional_id=None,
                    producer_epoch=0,
                    expires_at=expires_at,
                    last_seen_at=now,
                )
                session.add(producer_session)
                await session.flush()
                return _response(
                    api_version=api_version,
                    error_code=ErrorCode.none,
                    producer_id=producer_session.producer_id,
                    producer_epoch=producer_session.producer_epoch,
                )

            producer_session = (
                await session.execute(
                    select(ProducerSession)
                    .where(ProducerSession.transactional_id == transactional_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()

            if producer_session is None:
                producer_session = ProducerSession(
                    transactional_id=transactional_id,
                    producer_epoch=0,
                    expires_at=expires_at,
                    last_seen_at=now,
                )
                session.add(producer_session)
                await session.flush()
                return _response(
                    api_version=api_version,
                    error_code=ErrorCode.none,
                    producer_id=producer_session.producer_id,
                    producer_epoch=producer_session.producer_epoch,
                )

            if request_producer_id not in (-1, producer_session.producer_id):
                return _response(
                    api_version=api_version,
                    error_code=ErrorCode.invalid_producer_id_mapping,
                    producer_id=producer_session.producer_id,
                    producer_epoch=producer_session.producer_epoch,
                )

            if request_producer_epoch not in (-1, producer_session.producer_epoch):
                return _response(
                    api_version=api_version,
                    error_code=ErrorCode.invalid_producer_epoch,
                    producer_id=producer_session.producer_id,
                    producer_epoch=producer_session.producer_epoch,
                )

            producer_session.producer_epoch = (producer_session.producer_epoch + 1) % 32768
            producer_session.last_seen_at = now
            producer_session.expires_at = expires_at
            await session.flush()

            return _response(
                api_version=api_version,
                error_code=ErrorCode.none,
                producer_id=producer_session.producer_id,
                producer_epoch=producer_session.producer_epoch,
            )


def init_producer_id_error_response(
    req: InitProducerIdRequest,
    api_version: int,
    *,
    error_code: ErrorCode,
    error_message: str,
) -> InitProducerIdResponse:
    _ = req
    _ = error_message
    return _response(
        api_version=api_version,
        error_code=error_code,
        producer_id=-1,
        producer_epoch=-1,
    )
