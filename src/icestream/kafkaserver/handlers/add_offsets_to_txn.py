from kio.schema.add_offsets_to_txn.v0.request import (
    AddOffsetsToTxnRequest as AddOffsetsToTxnRequestV0,
)
from kio.schema.add_offsets_to_txn.v0.request import (
    RequestHeader as AddOffsetsToTxnRequestHeaderV0,
)
from kio.schema.add_offsets_to_txn.v0.response import (
    AddOffsetsToTxnResponse as AddOffsetsToTxnResponseV0,
)
from kio.schema.add_offsets_to_txn.v0.response import (
    ResponseHeader as AddOffsetsToTxnResponseHeaderV0,
)
from kio.schema.add_offsets_to_txn.v1.request import (
    AddOffsetsToTxnRequest as AddOffsetsToTxnRequestV1,
)
from kio.schema.add_offsets_to_txn.v1.request import (
    RequestHeader as AddOffsetsToTxnRequestHeaderV1,
)
from kio.schema.add_offsets_to_txn.v1.response import (
    AddOffsetsToTxnResponse as AddOffsetsToTxnResponseV1,
)
from kio.schema.add_offsets_to_txn.v1.response import (
    ResponseHeader as AddOffsetsToTxnResponseHeaderV1,
)
from kio.schema.add_offsets_to_txn.v2.request import (
    AddOffsetsToTxnRequest as AddOffsetsToTxnRequestV2,
)
from kio.schema.add_offsets_to_txn.v2.request import (
    RequestHeader as AddOffsetsToTxnRequestHeaderV2,
)
from kio.schema.add_offsets_to_txn.v2.response import (
    AddOffsetsToTxnResponse as AddOffsetsToTxnResponseV2,
)
from kio.schema.add_offsets_to_txn.v2.response import (
    ResponseHeader as AddOffsetsToTxnResponseHeaderV2,
)
from kio.schema.add_offsets_to_txn.v3.request import (
    AddOffsetsToTxnRequest as AddOffsetsToTxnRequestV3,
)
from kio.schema.add_offsets_to_txn.v3.request import (
    RequestHeader as AddOffsetsToTxnRequestHeaderV3,
)
from kio.schema.add_offsets_to_txn.v3.response import (
    AddOffsetsToTxnResponse as AddOffsetsToTxnResponseV3,
)
from kio.schema.add_offsets_to_txn.v3.response import (
    ResponseHeader as AddOffsetsToTxnResponseHeaderV3,
)
from kio.schema.add_offsets_to_txn.v4.request import (
    AddOffsetsToTxnRequest as AddOffsetsToTxnRequestV4,
)
from kio.schema.add_offsets_to_txn.v4.request import (
    RequestHeader as AddOffsetsToTxnRequestHeaderV4,
)
from kio.schema.add_offsets_to_txn.v4.response import (
    AddOffsetsToTxnResponse as AddOffsetsToTxnResponseV4,
)
from kio.schema.add_offsets_to_txn.v4.response import (
    ResponseHeader as AddOffsetsToTxnResponseHeaderV4,
)

AddOffsetsToTxnRequestHeader = (
    AddOffsetsToTxnRequestHeaderV0
    | AddOffsetsToTxnRequestHeaderV1
    | AddOffsetsToTxnRequestHeaderV2
    | AddOffsetsToTxnRequestHeaderV3
    | AddOffsetsToTxnRequestHeaderV4
)

AddOffsetsToTxnResponseHeader = (
    AddOffsetsToTxnResponseHeaderV0
    | AddOffsetsToTxnResponseHeaderV1
    | AddOffsetsToTxnResponseHeaderV2
    | AddOffsetsToTxnResponseHeaderV3
    | AddOffsetsToTxnResponseHeaderV4
)

AddOffsetsToTxnRequest = (
    AddOffsetsToTxnRequestV0
    | AddOffsetsToTxnRequestV1
    | AddOffsetsToTxnRequestV2
    | AddOffsetsToTxnRequestV3
    | AddOffsetsToTxnRequestV4
)

AddOffsetsToTxnResponse = (
    AddOffsetsToTxnResponseV0
    | AddOffsetsToTxnResponseV1
    | AddOffsetsToTxnResponseV2
    | AddOffsetsToTxnResponseV3
    | AddOffsetsToTxnResponseV4
)
