from kio.schema.delete_records.v0.request import (
    DeleteRecordsRequest as DeleteRecordsRequestV0,
)
from kio.schema.delete_records.v0.request import (
    RequestHeader as DeleteRecordsRequestHeaderV0,
)
from kio.schema.delete_records.v0.response import (
    DeleteRecordsResponse as DeleteRecordsResponseV0,
)
from kio.schema.delete_records.v0.response import (
    ResponseHeader as DeleteRecordsResponseHeaderV0,
)
from kio.schema.delete_records.v1.request import (
    DeleteRecordsRequest as DeleteRecordsRequestV1,
)
from kio.schema.delete_records.v1.request import (
    RequestHeader as DeleteRecordsRequestHeaderV1,
)
from kio.schema.delete_records.v1.response import (
    DeleteRecordsResponse as DeleteRecordsResponseV1,
)
from kio.schema.delete_records.v1.response import (
    ResponseHeader as DeleteRecordsResponseHeaderV1,
)
from kio.schema.delete_records.v2.request import (
    DeleteRecordsRequest as DeleteRecordsRequestV2,
)
from kio.schema.delete_records.v2.request import (
    RequestHeader as DeleteRecordsRequestHeaderV2,
)
from kio.schema.delete_records.v2.response import (
    DeleteRecordsResponse as DeleteRecordsResponseV2,
)
from kio.schema.delete_records.v2.response import (
    ResponseHeader as DeleteRecordsResponseHeaderV2,
)

DeleteRecordsRequestHeader = (
    DeleteRecordsRequestHeaderV0
    | DeleteRecordsRequestHeaderV1
    | DeleteRecordsRequestHeaderV2
)

DeleteRecordsResponseHeader = (
    DeleteRecordsResponseHeaderV0
    | DeleteRecordsResponseHeaderV1
    | DeleteRecordsResponseHeaderV2
)

DeleteRecordsRequest = (
    DeleteRecordsRequestV0
    | DeleteRecordsRequestV1
    | DeleteRecordsRequestV2
)

DeleteRecordsResponse = (
    DeleteRecordsResponseV0
    | DeleteRecordsResponseV1
    | DeleteRecordsResponseV2
)
