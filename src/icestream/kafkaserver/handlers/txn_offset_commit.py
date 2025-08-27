from kio.schema.txn_offset_commit.v0.request import (
    TxnOffsetCommitRequest as TxnOffsetCommitRequestV0,
)
from kio.schema.txn_offset_commit.v0.request import (
    RequestHeader as TxnOffsetCommitRequestHeaderV0,
)
from kio.schema.txn_offset_commit.v0.response import (
    TxnOffsetCommitResponse as TxnOffsetCommitResponseV0,
)
from kio.schema.txn_offset_commit.v0.response import (
    ResponseHeader as TxnOffsetCommitResponseHeaderV0,
)
from kio.schema.txn_offset_commit.v1.request import (
    TxnOffsetCommitRequest as TxnOffsetCommitRequestV1,
)
from kio.schema.txn_offset_commit.v1.request import (
    RequestHeader as TxnOffsetCommitRequestHeaderV1,
)
from kio.schema.txn_offset_commit.v1.response import (
    TxnOffsetCommitResponse as TxnOffsetCommitResponseV1,
)
from kio.schema.txn_offset_commit.v1.response import (
    ResponseHeader as TxnOffsetCommitResponseHeaderV1,
)
from kio.schema.txn_offset_commit.v2.request import (
    TxnOffsetCommitRequest as TxnOffsetCommitRequestV2,
)
from kio.schema.txn_offset_commit.v2.request import (
    RequestHeader as TxnOffsetCommitRequestHeaderV2,
)
from kio.schema.txn_offset_commit.v2.response import (
    TxnOffsetCommitResponse as TxnOffsetCommitResponseV2,
)
from kio.schema.txn_offset_commit.v2.response import (
    ResponseHeader as TxnOffsetCommitResponseHeaderV2,
)
from kio.schema.txn_offset_commit.v3.request import (
    TxnOffsetCommitRequest as TxnOffsetCommitRequestV3,
)
from kio.schema.txn_offset_commit.v3.request import (
    RequestHeader as TxnOffsetCommitRequestHeaderV3,
)
from kio.schema.txn_offset_commit.v3.response import (
    TxnOffsetCommitResponse as TxnOffsetCommitResponseV3,
)
from kio.schema.txn_offset_commit.v3.response import (
    ResponseHeader as TxnOffsetCommitResponseHeaderV3,
)
from kio.schema.txn_offset_commit.v4.request import (
    TxnOffsetCommitRequest as TxnOffsetCommitRequestV4,
)
from kio.schema.txn_offset_commit.v4.request import (
    RequestHeader as TxnOffsetCommitRequestHeaderV4,
)
from kio.schema.txn_offset_commit.v4.response import (
    TxnOffsetCommitResponse as TxnOffsetCommitResponseV4,
)
from kio.schema.txn_offset_commit.v4.response import (
    ResponseHeader as TxnOffsetCommitResponseHeaderV4,
)

TxnOffsetCommitRequestHeader = (
    TxnOffsetCommitRequestHeaderV0
    | TxnOffsetCommitRequestHeaderV1
    | TxnOffsetCommitRequestHeaderV2
    | TxnOffsetCommitRequestHeaderV3
    | TxnOffsetCommitRequestHeaderV4
)

TxnOffsetCommitResponseHeader = (
    TxnOffsetCommitResponseHeaderV0
    | TxnOffsetCommitResponseHeaderV1
    | TxnOffsetCommitResponseHeaderV2
    | TxnOffsetCommitResponseHeaderV3
    | TxnOffsetCommitResponseHeaderV4
)

TxnOffsetCommitRequest = (
    TxnOffsetCommitRequestV0
    | TxnOffsetCommitRequestV1
    | TxnOffsetCommitRequestV2
    | TxnOffsetCommitRequestV3
    | TxnOffsetCommitRequestV4
)

TxnOffsetCommitResponse = (
    TxnOffsetCommitResponseV0
    | TxnOffsetCommitResponseV1
    | TxnOffsetCommitResponseV2
    | TxnOffsetCommitResponseV3
    | TxnOffsetCommitResponseV4
)
