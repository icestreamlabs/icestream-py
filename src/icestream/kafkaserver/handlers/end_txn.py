from kio.schema.end_txn.v0.request import (
    EndTxnRequest as EndTxnRequestV0,
)
from kio.schema.end_txn.v0.request import (
    RequestHeader as EndTxnRequestHeaderV0,
)
from kio.schema.end_txn.v0.response import (
    EndTxnResponse as EndTxnResponseV0,
)
from kio.schema.end_txn.v0.response import (
    ResponseHeader as EndTxnResponseHeaderV0,
)
from kio.schema.end_txn.v1.request import (
    EndTxnRequest as EndTxnRequestV1,
)
from kio.schema.end_txn.v1.request import (
    RequestHeader as EndTxnRequestHeaderV1,
)
from kio.schema.end_txn.v1.response import (
    EndTxnResponse as EndTxnResponseV1,
)
from kio.schema.end_txn.v1.response import (
    ResponseHeader as EndTxnResponseHeaderV1,
)
from kio.schema.end_txn.v2.request import (
    EndTxnRequest as EndTxnRequestV2,
)
from kio.schema.end_txn.v2.request import (
    RequestHeader as EndTxnRequestHeaderV2,
)
from kio.schema.end_txn.v2.response import (
    EndTxnResponse as EndTxnResponseV2,
)
from kio.schema.end_txn.v2.response import (
    ResponseHeader as EndTxnResponseHeaderV2,
)
from kio.schema.end_txn.v3.request import (
    EndTxnRequest as EndTxnRequestV3,
)
from kio.schema.end_txn.v3.request import (
    RequestHeader as EndTxnRequestHeaderV3,
)
from kio.schema.end_txn.v3.response import (
    EndTxnResponse as EndTxnResponseV3,
)
from kio.schema.end_txn.v3.response import (
    ResponseHeader as EndTxnResponseHeaderV3,
)
from kio.schema.end_txn.v4.request import (
    EndTxnRequest as EndTxnRequestV4,
)
from kio.schema.end_txn.v4.request import (
    RequestHeader as EndTxnRequestHeaderV4,
)
from kio.schema.end_txn.v4.response import (
    EndTxnResponse as EndTxnResponseV4,
)
from kio.schema.end_txn.v4.response import (
    ResponseHeader as EndTxnResponseHeaderV4,
)

EndTxnRequestHeader = (
    EndTxnRequestHeaderV0
    | EndTxnRequestHeaderV1
    | EndTxnRequestHeaderV2
    | EndTxnRequestHeaderV3
    | EndTxnRequestHeaderV4
)

EndTxnResponseHeader = (
    EndTxnResponseHeaderV0
    | EndTxnResponseHeaderV1
    | EndTxnResponseHeaderV2
    | EndTxnResponseHeaderV3
    | EndTxnResponseHeaderV4
)

EndTxnRequest = (
    EndTxnRequestV0
    | EndTxnRequestV1
    | EndTxnRequestV2
    | EndTxnRequestV3
    | EndTxnRequestV4
)

EndTxnResponse = (
    EndTxnResponseV0
    | EndTxnResponseV1
    | EndTxnResponseV2
    | EndTxnResponseV3
    | EndTxnResponseV4
)
