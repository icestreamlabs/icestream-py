from kio.schema.delete_topics.v0.request import (
    DeleteTopicsRequest as DeleteTopicsRequestV0,
)
from kio.schema.delete_topics.v0.request import (
    RequestHeader as DeleteTopicsRequestHeaderV0,
)
from kio.schema.delete_topics.v0.response import (
    DeleteTopicsResponse as DeleteTopicsResponseV0,
)
from kio.schema.delete_topics.v0.response import (
    ResponseHeader as DeleteTopicsResponseHeaderV0,
)
from kio.schema.delete_topics.v1.request import (
    DeleteTopicsRequest as DeleteTopicsRequestV1,
)
from kio.schema.delete_topics.v1.request import (
    RequestHeader as DeleteTopicsRequestHeaderV1,
)
from kio.schema.delete_topics.v1.response import (
    DeleteTopicsResponse as DeleteTopicsResponseV1,
)
from kio.schema.delete_topics.v1.response import (
    ResponseHeader as DeleteTopicsResponseHeaderV1,
)
from kio.schema.delete_topics.v2.request import (
    DeleteTopicsRequest as DeleteTopicsRequestV2,
)
from kio.schema.delete_topics.v2.request import (
    RequestHeader as DeleteTopicsRequestHeaderV2,
)
from kio.schema.delete_topics.v2.response import (
    DeleteTopicsResponse as DeleteTopicsResponseV2,
)
from kio.schema.delete_topics.v2.response import (
    ResponseHeader as DeleteTopicsResponseHeaderV2,
)
from kio.schema.delete_topics.v3.request import (
    DeleteTopicsRequest as DeleteTopicsRequestV3,
)
from kio.schema.delete_topics.v3.request import (
    RequestHeader as DeleteTopicsRequestHeaderV3,
)
from kio.schema.delete_topics.v3.response import (
    DeleteTopicsResponse as DeleteTopicsResponseV3,
)
from kio.schema.delete_topics.v3.response import (
    ResponseHeader as DeleteTopicsResponseHeaderV3,
)
from kio.schema.delete_topics.v4.request import (
    DeleteTopicsRequest as DeleteTopicsRequestV4,
)
from kio.schema.delete_topics.v4.request import (
    RequestHeader as DeleteTopicsRequestHeaderV4,
)
from kio.schema.delete_topics.v4.response import (
    DeleteTopicsResponse as DeleteTopicsResponseV4,
)
from kio.schema.delete_topics.v4.response import (
    ResponseHeader as DeleteTopicsResponseHeaderV4,
)
from kio.schema.delete_topics.v5.request import (
    DeleteTopicsRequest as DeleteTopicsRequestV5,
)
from kio.schema.delete_topics.v5.request import (
    RequestHeader as DeleteTopicsRequestHeaderV5,
)
from kio.schema.delete_topics.v5.response import (
    DeleteTopicsResponse as DeleteTopicsResponseV5,
)
from kio.schema.delete_topics.v5.response import (
    ResponseHeader as DeleteTopicsResponseHeaderV5,
)
from kio.schema.delete_topics.v6.request import (
    DeleteTopicsRequest as DeleteTopicsRequestV6,
)
from kio.schema.delete_topics.v6.request import (
    RequestHeader as DeleteTopicsRequestHeaderV6,
)
from kio.schema.delete_topics.v6.response import (
    DeleteTopicsResponse as DeleteTopicsResponseV6,
)
from kio.schema.delete_topics.v6.response import (
    ResponseHeader as DeleteTopicsResponseHeaderV6,
)

DeleteTopicsRequestHeader = (
    DeleteTopicsRequestHeaderV0
    | DeleteTopicsRequestHeaderV1
    | DeleteTopicsRequestHeaderV2
    | DeleteTopicsRequestHeaderV3
    | DeleteTopicsRequestHeaderV4
    | DeleteTopicsRequestHeaderV5
    | DeleteTopicsRequestHeaderV6
)

DeleteTopicsResponseHeader = (
    DeleteTopicsResponseHeaderV0
    | DeleteTopicsResponseHeaderV1
    | DeleteTopicsResponseHeaderV2
    | DeleteTopicsResponseHeaderV3
    | DeleteTopicsResponseHeaderV4
    | DeleteTopicsResponseHeaderV5
    | DeleteTopicsResponseHeaderV6
)

DeleteTopicsRequest = (
    DeleteTopicsRequestV0
    | DeleteTopicsRequestV1
    | DeleteTopicsRequestV2
    | DeleteTopicsRequestV3
    | DeleteTopicsRequestV4
    | DeleteTopicsRequestV5
    | DeleteTopicsRequestV6
)

DeleteTopicsResponse = (
    DeleteTopicsResponseV0
    | DeleteTopicsResponseV1
    | DeleteTopicsResponseV2
    | DeleteTopicsResponseV3
    | DeleteTopicsResponseV4
    | DeleteTopicsResponseV5
    | DeleteTopicsResponseV6
)
