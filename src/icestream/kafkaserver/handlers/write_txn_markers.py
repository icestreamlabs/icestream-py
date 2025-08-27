from kio.schema.write_txn_markers.v0.request import (
    WriteTxnMarkersRequest as WriteTxnMarkersRequestV0,
)
from kio.schema.write_txn_markers.v0.request import (
    RequestHeader as WriteTxnMarkersRequestHeaderV0,
)
from kio.schema.write_txn_markers.v0.response import (
    WriteTxnMarkersResponse as WriteTxnMarkersResponseV0,
)
from kio.schema.write_txn_markers.v0.response import (
    ResponseHeader as WriteTxnMarkersResponseHeaderV0,
)
from kio.schema.write_txn_markers.v1.request import (
    WriteTxnMarkersRequest as WriteTxnMarkersRequestV1,
)
from kio.schema.write_txn_markers.v1.request import (
    RequestHeader as WriteTxnMarkersRequestHeaderV1,
)
from kio.schema.write_txn_markers.v1.response import (
    WriteTxnMarkersResponse as WriteTxnMarkersResponseV1,
)
from kio.schema.write_txn_markers.v1.response import (
    ResponseHeader as WriteTxnMarkersResponseHeaderV1,
)

WriteTxnMarkersRequestHeader = (
    WriteTxnMarkersRequestHeaderV0 | WriteTxnMarkersRequestHeaderV1
)
WriteTxnMarkersResponseHeader = (
    WriteTxnMarkersResponseHeaderV0 | WriteTxnMarkersResponseHeaderV1
)
WriteTxnMarkersRequest = WriteTxnMarkersRequestV0 | WriteTxnMarkersRequestV1
WriteTxnMarkersResponse = WriteTxnMarkersResponseV0 | WriteTxnMarkersResponseV1
