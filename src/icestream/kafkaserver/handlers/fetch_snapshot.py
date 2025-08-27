from kio.schema.fetch_snapshot.v0.request import (
    FetchSnapshotRequest as FetchSnapshotRequestV0,
)
from kio.schema.fetch_snapshot.v0.request import (
    RequestHeader as FetchSnapshotRequestHeaderV0,
)
from kio.schema.fetch_snapshot.v0.response import (
    FetchSnapshotResponse as FetchSnapshotResponseV0,
)
from kio.schema.fetch_snapshot.v0.response import (
    ResponseHeader as FetchSnapshotResponseHeaderV0,
)
from kio.schema.fetch_snapshot.v1.request import (
    FetchSnapshotRequest as FetchSnapshotRequestV1,
)
from kio.schema.fetch_snapshot.v1.request import (
    RequestHeader as FetchSnapshotRequestHeaderV1,
)
from kio.schema.fetch_snapshot.v1.response import (
    FetchSnapshotResponse as FetchSnapshotResponseV1,
)
from kio.schema.fetch_snapshot.v1.response import (
    ResponseHeader as FetchSnapshotResponseHeaderV1,
)

FetchSnapshotRequestHeader = (
    FetchSnapshotRequestHeaderV0 | FetchSnapshotRequestHeaderV1
)

FetchSnapshotResponseHeader = (
    FetchSnapshotResponseHeaderV0 | FetchSnapshotResponseHeaderV1
)

FetchSnapshotRequest = FetchSnapshotRequestV0 | FetchSnapshotRequestV1

FetchSnapshotResponse = FetchSnapshotResponseV0 | FetchSnapshotResponseV1
