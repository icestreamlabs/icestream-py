from kio.schema.end_quorum_epoch.v0.request import (
    EndQuorumEpochRequest as EndQuorumEpochRequestV0,
)
from kio.schema.end_quorum_epoch.v0.request import (
    RequestHeader as EndQuorumEpochRequestHeaderV0,
)
from kio.schema.end_quorum_epoch.v0.response import (
    EndQuorumEpochResponse as EndQuorumEpochResponseV0,
)
from kio.schema.end_quorum_epoch.v0.response import (
    ResponseHeader as EndQuorumEpochResponseHeaderV0,
)
from kio.schema.end_quorum_epoch.v1.request import (
    EndQuorumEpochRequest as EndQuorumEpochRequestV1,
)
from kio.schema.end_quorum_epoch.v1.request import (
    RequestHeader as EndQuorumEpochRequestHeaderV1,
)
from kio.schema.end_quorum_epoch.v1.response import (
    EndQuorumEpochResponse as EndQuorumEpochResponseV1,
)
from kio.schema.end_quorum_epoch.v1.response import (
    ResponseHeader as EndQuorumEpochResponseHeaderV1,
)

EndQuorumEpochRequestHeader = (
    EndQuorumEpochRequestHeaderV0
    | EndQuorumEpochRequestHeaderV1
)

EndQuorumEpochResponseHeader = (
    EndQuorumEpochResponseHeaderV0
    | EndQuorumEpochResponseHeaderV1
)

EndQuorumEpochRequest = (
    EndQuorumEpochRequestV0
    | EndQuorumEpochRequestV1
)

EndQuorumEpochResponse = (
    EndQuorumEpochResponseV0
    | EndQuorumEpochResponseV1
)
