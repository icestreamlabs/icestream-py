from kio.schema.stop_replica.v0.request import (
    StopReplicaRequest as StopReplicaRequestV0,
)
from kio.schema.stop_replica.v0.request import (
    RequestHeader as StopReplicaRequestHeaderV0,
)
from kio.schema.stop_replica.v0.response import (
    StopReplicaResponse as StopReplicaResponseV0,
)
from kio.schema.stop_replica.v0.response import (
    ResponseHeader as StopReplicaResponseHeaderV0,
)
from kio.schema.stop_replica.v1.request import (
    StopReplicaRequest as StopReplicaRequestV1,
)
from kio.schema.stop_replica.v1.request import (
    RequestHeader as StopReplicaRequestHeaderV1,
)
from kio.schema.stop_replica.v1.response import (
    StopReplicaResponse as StopReplicaResponseV1,
)
from kio.schema.stop_replica.v1.response import (
    ResponseHeader as StopReplicaResponseHeaderV1,
)
from kio.schema.stop_replica.v2.request import (
    StopReplicaRequest as StopReplicaRequestV2,
)
from kio.schema.stop_replica.v2.request import (
    RequestHeader as StopReplicaRequestHeaderV2,
)
from kio.schema.stop_replica.v2.response import (
    StopReplicaResponse as StopReplicaResponseV2,
)
from kio.schema.stop_replica.v2.response import (
    ResponseHeader as StopReplicaResponseHeaderV2,
)
from kio.schema.stop_replica.v3.request import (
    StopReplicaRequest as StopReplicaRequestV3,
)
from kio.schema.stop_replica.v3.request import (
    RequestHeader as StopReplicaRequestHeaderV3,
)
from kio.schema.stop_replica.v3.response import (
    StopReplicaResponse as StopReplicaResponseV3,
)
from kio.schema.stop_replica.v3.response import (
    ResponseHeader as StopReplicaResponseHeaderV3,
)
from kio.schema.stop_replica.v4.request import (
    StopReplicaRequest as StopReplicaRequestV4,
)
from kio.schema.stop_replica.v4.request import (
    RequestHeader as StopReplicaRequestHeaderV4,
)
from kio.schema.stop_replica.v4.response import (
    StopReplicaResponse as StopReplicaResponseV4,
)
from kio.schema.stop_replica.v4.response import (
    ResponseHeader as StopReplicaResponseHeaderV4,
)

StopReplicaRequestHeader = (
    StopReplicaRequestHeaderV0
    | StopReplicaRequestHeaderV1
    | StopReplicaRequestHeaderV2
    | StopReplicaRequestHeaderV3
    | StopReplicaRequestHeaderV4
)

StopReplicaResponseHeader = (
    StopReplicaResponseHeaderV0
    | StopReplicaResponseHeaderV1
    | StopReplicaResponseHeaderV2
    | StopReplicaResponseHeaderV3
    | StopReplicaResponseHeaderV4
)

StopReplicaRequest = (
    StopReplicaRequestV0
    | StopReplicaRequestV1
    | StopReplicaRequestV2
    | StopReplicaRequestV3
    | StopReplicaRequestV4
)

StopReplicaResponse = (
    StopReplicaResponseV0
    | StopReplicaResponseV1
    | StopReplicaResponseV2
    | StopReplicaResponseV3
    | StopReplicaResponseV4
)
