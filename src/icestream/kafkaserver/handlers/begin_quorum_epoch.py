from kio.schema.begin_quorum_epoch.v0.request import (
    BeginQuorumEpochRequest as BeginQuorumEpochRequestV0,
)
from kio.schema.begin_quorum_epoch.v0.request import (
    RequestHeader as BeginQuorumEpochRequestHeaderV0,
)
from kio.schema.begin_quorum_epoch.v0.response import (
    BeginQuorumEpochResponse as BeginQuorumEpochResponseV0,
)
from kio.schema.begin_quorum_epoch.v0.response import (
    ResponseHeader as BeginQuorumEpochResponseHeaderV0,
)
from kio.schema.begin_quorum_epoch.v1.request import (
    BeginQuorumEpochRequest as BeginQuorumEpochRequestV1,
)
from kio.schema.begin_quorum_epoch.v1.request import (
    RequestHeader as BeginQuorumEpochRequestHeaderV1,
)
from kio.schema.begin_quorum_epoch.v1.response import (
    BeginQuorumEpochResponse as BeginQuorumEpochResponseV1,
)
from kio.schema.begin_quorum_epoch.v1.response import (
    ResponseHeader as BeginQuorumEpochResponseHeaderV1,
)


BeginQuorumEpochRequestHeader = (
    BeginQuorumEpochRequestHeaderV0 | BeginQuorumEpochRequestHeaderV1
)

BeginQuorumEpochResponseHeader = (
    BeginQuorumEpochResponseHeaderV0 | BeginQuorumEpochResponseHeaderV1
)

BeginQuorumEpochRequest = (
    BeginQuorumEpochRequestV0 | BeginQuorumEpochRequestV1
)

BeginQuorumEpochResponse = (
    BeginQuorumEpochResponseV0 | BeginQuorumEpochResponseV1
)
