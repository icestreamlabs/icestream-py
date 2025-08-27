from kio.schema.offset_commit.v0.request import (
    OffsetCommitRequest as OffsetCommitRequestV0,
)
from kio.schema.offset_commit.v0.request import (
    RequestHeader as OffsetCommitRequestHeaderV0,
)
from kio.schema.offset_commit.v0.response import (
    OffsetCommitResponse as OffsetCommitResponseV0,
)
from kio.schema.offset_commit.v0.response import (
    ResponseHeader as OffsetCommitResponseHeaderV0,
)
from kio.schema.offset_commit.v1.request import (
    OffsetCommitRequest as OffsetCommitRequestV1,
)
from kio.schema.offset_commit.v1.request import (
    RequestHeader as OffsetCommitRequestHeaderV1,
)
from kio.schema.offset_commit.v1.response import (
    OffsetCommitResponse as OffsetCommitResponseV1,
)
from kio.schema.offset_commit.v1.response import (
    ResponseHeader as OffsetCommitResponseHeaderV1,
)
from kio.schema.offset_commit.v2.request import (
    OffsetCommitRequest as OffsetCommitRequestV2,
)
from kio.schema.offset_commit.v2.request import (
    RequestHeader as OffsetCommitRequestHeaderV2,
)
from kio.schema.offset_commit.v2.response import (
    OffsetCommitResponse as OffsetCommitResponseV2,
)
from kio.schema.offset_commit.v2.response import (
    ResponseHeader as OffsetCommitResponseHeaderV2,
)
from kio.schema.offset_commit.v3.request import (
    OffsetCommitRequest as OffsetCommitRequestV3,
)
from kio.schema.offset_commit.v3.request import (
    RequestHeader as OffsetCommitRequestHeaderV3,
)
from kio.schema.offset_commit.v3.response import (
    OffsetCommitResponse as OffsetCommitResponseV3,
)
from kio.schema.offset_commit.v3.response import (
    ResponseHeader as OffsetCommitResponseHeaderV3,
)
from kio.schema.offset_commit.v4.request import (
    OffsetCommitRequest as OffsetCommitRequestV4,
)
from kio.schema.offset_commit.v4.request import (
    RequestHeader as OffsetCommitRequestHeaderV4,
)
from kio.schema.offset_commit.v4.response import (
    OffsetCommitResponse as OffsetCommitResponseV4,
)
from kio.schema.offset_commit.v4.response import (
    ResponseHeader as OffsetCommitResponseHeaderV4,
)
from kio.schema.offset_commit.v5.request import (
    OffsetCommitRequest as OffsetCommitRequestV5,
)
from kio.schema.offset_commit.v5.request import (
    RequestHeader as OffsetCommitRequestHeaderV5,
)
from kio.schema.offset_commit.v5.response import (
    OffsetCommitResponse as OffsetCommitResponseV5,
)
from kio.schema.offset_commit.v5.response import (
    ResponseHeader as OffsetCommitResponseHeaderV5,
)
from kio.schema.offset_commit.v6.request import (
    OffsetCommitRequest as OffsetCommitRequestV6,
)
from kio.schema.offset_commit.v6.request import (
    RequestHeader as OffsetCommitRequestHeaderV6,
)
from kio.schema.offset_commit.v6.response import (
    OffsetCommitResponse as OffsetCommitResponseV6,
)
from kio.schema.offset_commit.v6.response import (
    ResponseHeader as OffsetCommitResponseHeaderV6,
)
from kio.schema.offset_commit.v7.request import (
    OffsetCommitRequest as OffsetCommitRequestV7,
)
from kio.schema.offset_commit.v7.request import (
    RequestHeader as OffsetCommitRequestHeaderV7,
)
from kio.schema.offset_commit.v7.response import (
    OffsetCommitResponse as OffsetCommitResponseV7,
)
from kio.schema.offset_commit.v7.response import (
    ResponseHeader as OffsetCommitResponseHeaderV7,
)
from kio.schema.offset_commit.v8.request import (
    OffsetCommitRequest as OffsetCommitRequestV8,
)
from kio.schema.offset_commit.v8.request import (
    RequestHeader as OffsetCommitRequestHeaderV8,
)
from kio.schema.offset_commit.v8.response import (
    OffsetCommitResponse as OffsetCommitResponseV8,
)
from kio.schema.offset_commit.v8.response import (
    ResponseHeader as OffsetCommitResponseHeaderV8,
)
from kio.schema.offset_commit.v9.request import (
    OffsetCommitRequest as OffsetCommitRequestV9,
)
from kio.schema.offset_commit.v9.request import (
    RequestHeader as OffsetCommitRequestHeaderV9,
)
from kio.schema.offset_commit.v9.response import (
    OffsetCommitResponse as OffsetCommitResponseV9,
)
from kio.schema.offset_commit.v9.response import (
    ResponseHeader as OffsetCommitResponseHeaderV9,
)


OffsetCommitRequestHeader = (
    OffsetCommitRequestHeaderV0
    | OffsetCommitRequestHeaderV1
    | OffsetCommitRequestHeaderV2
    | OffsetCommitRequestHeaderV3
    | OffsetCommitRequestHeaderV4
    | OffsetCommitRequestHeaderV5
    | OffsetCommitRequestHeaderV6
    | OffsetCommitRequestHeaderV7
    | OffsetCommitRequestHeaderV8
    | OffsetCommitRequestHeaderV9
)

OffsetCommitResponseHeader = (
    OffsetCommitResponseHeaderV0
    | OffsetCommitResponseHeaderV1
    | OffsetCommitResponseHeaderV2
    | OffsetCommitResponseHeaderV3
    | OffsetCommitResponseHeaderV4
    | OffsetCommitResponseHeaderV5
    | OffsetCommitResponseHeaderV6
    | OffsetCommitResponseHeaderV7
    | OffsetCommitResponseHeaderV8
    | OffsetCommitResponseHeaderV9
)

OffsetCommitRequest = (
    OffsetCommitRequestV0
    | OffsetCommitRequestV1
    | OffsetCommitRequestV2
    | OffsetCommitRequestV3
    | OffsetCommitRequestV4
    | OffsetCommitRequestV5
    | OffsetCommitRequestV6
    | OffsetCommitRequestV7
    | OffsetCommitRequestV8
    | OffsetCommitRequestV9
)

OffsetCommitResponse = (
    OffsetCommitResponseV0
    | OffsetCommitResponseV1
    | OffsetCommitResponseV2
    | OffsetCommitResponseV3
    | OffsetCommitResponseV4
    | OffsetCommitResponseV5
    | OffsetCommitResponseV6
    | OffsetCommitResponseV7
    | OffsetCommitResponseV8
    | OffsetCommitResponseV9
)
