from kio.schema.add_partitions_to_txn.v0.request import (
    AddPartitionsToTxnRequest as AddPartitionsToTxnRequestV0,
)
from kio.schema.add_partitions_to_txn.v0.request import (
    RequestHeader as AddPartitionsToTxnRequestHeaderV0,
)
from kio.schema.add_partitions_to_txn.v0.response import (
    AddPartitionsToTxnResponse as AddPartitionsToTxnResponseV0,
)
from kio.schema.add_partitions_to_txn.v0.response import (
    ResponseHeader as AddPartitionsToTxnResponseHeaderV0,
)
from kio.schema.add_partitions_to_txn.v1.request import (
    AddPartitionsToTxnRequest as AddPartitionsToTxnRequestV1,
)
from kio.schema.add_partitions_to_txn.v1.request import (
    RequestHeader as AddPartitionsToTxnRequestHeaderV1,
)
from kio.schema.add_partitions_to_txn.v1.response import (
    AddPartitionsToTxnResponse as AddPartitionsToTxnResponseV1,
)
from kio.schema.add_partitions_to_txn.v1.response import (
    ResponseHeader as AddPartitionsToTxnResponseHeaderV1,
)
from kio.schema.add_partitions_to_txn.v2.request import (
    AddPartitionsToTxnRequest as AddPartitionsToTxnRequestV2,
)
from kio.schema.add_partitions_to_txn.v2.request import (
    RequestHeader as AddPartitionsToTxnRequestHeaderV2,
)
from kio.schema.add_partitions_to_txn.v2.response import (
    AddPartitionsToTxnResponse as AddPartitionsToTxnResponseV2,
)
from kio.schema.add_partitions_to_txn.v2.response import (
    ResponseHeader as AddPartitionsToTxnResponseHeaderV2,
)
from kio.schema.add_partitions_to_txn.v3.request import (
    AddPartitionsToTxnRequest as AddPartitionsToTxnRequestV3,
)
from kio.schema.add_partitions_to_txn.v3.request import (
    RequestHeader as AddPartitionsToTxnRequestHeaderV3,
)
from kio.schema.add_partitions_to_txn.v3.response import (
    AddPartitionsToTxnResponse as AddPartitionsToTxnResponseV3,
)
from kio.schema.add_partitions_to_txn.v3.response import (
    ResponseHeader as AddPartitionsToTxnResponseHeaderV3,
)
from kio.schema.add_partitions_to_txn.v4.request import (
    AddPartitionsToTxnRequest as AddPartitionsToTxnRequestV4,
)
from kio.schema.add_partitions_to_txn.v4.request import (
    RequestHeader as AddPartitionsToTxnRequestHeaderV4,
)
from kio.schema.add_partitions_to_txn.v4.response import (
    AddPartitionsToTxnResponse as AddPartitionsToTxnResponseV4,
)
from kio.schema.add_partitions_to_txn.v4.response import (
    ResponseHeader as AddPartitionsToTxnResponseHeaderV4,
)
from kio.schema.add_partitions_to_txn.v5.request import (
    AddPartitionsToTxnRequest as AddPartitionsToTxnRequestV5,
)
from kio.schema.add_partitions_to_txn.v5.request import (
    RequestHeader as AddPartitionsToTxnRequestHeaderV5,
)
from kio.schema.add_partitions_to_txn.v5.response import (
    AddPartitionsToTxnResponse as AddPartitionsToTxnResponseV5,
)
from kio.schema.add_partitions_to_txn.v5.response import (
    ResponseHeader as AddPartitionsToTxnResponseHeaderV5,
)

AddPartitionsToTxnRequestHeader = (
    AddPartitionsToTxnRequestHeaderV0
    | AddPartitionsToTxnRequestHeaderV1
    | AddPartitionsToTxnRequestHeaderV2
    | AddPartitionsToTxnRequestHeaderV3
    | AddPartitionsToTxnRequestHeaderV4
    | AddPartitionsToTxnRequestHeaderV5
)

AddPartitionsToTxnResponseHeader = (
    AddPartitionsToTxnResponseHeaderV0
    | AddPartitionsToTxnResponseHeaderV1
    | AddPartitionsToTxnResponseHeaderV2
    | AddPartitionsToTxnResponseHeaderV3
    | AddPartitionsToTxnResponseHeaderV4
    | AddPartitionsToTxnResponseHeaderV5
)

AddPartitionsToTxnRequest = (
    AddPartitionsToTxnRequestV0
    | AddPartitionsToTxnRequestV1
    | AddPartitionsToTxnRequestV2
    | AddPartitionsToTxnRequestV3
    | AddPartitionsToTxnRequestV4
    | AddPartitionsToTxnRequestV5
)

AddPartitionsToTxnResponse = (
    AddPartitionsToTxnResponseV0
    | AddPartitionsToTxnResponseV1
    | AddPartitionsToTxnResponseV2
    | AddPartitionsToTxnResponseV3
    | AddPartitionsToTxnResponseV4
    | AddPartitionsToTxnResponseV5
)
