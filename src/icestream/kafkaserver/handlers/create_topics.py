from kio.schema.create_topics.v0.request import (
    CreateTopicsRequest as CreateTopicsRequestV0,
)
from kio.schema.create_topics.v0.request import (
    RequestHeader as CreateTopicsRequestHeaderV0,
)
from kio.schema.create_topics.v0.response import (
    CreateTopicsResponse as CreateTopicsResponseV0,
)
from kio.schema.create_topics.v0.response import (
    ResponseHeader as CreateTopicsResponseHeaderV0,
)
from kio.schema.create_topics.v1.request import (
    CreateTopicsRequest as CreateTopicsRequestV1,
)
from kio.schema.create_topics.v1.request import (
    RequestHeader as CreateTopicsRequestHeaderV1,
)
from kio.schema.create_topics.v1.response import (
    CreateTopicsResponse as CreateTopicsResponseV1,
)
from kio.schema.create_topics.v1.response import (
    ResponseHeader as CreateTopicsResponseHeaderV1,
)
from kio.schema.create_topics.v2.request import (
    CreateTopicsRequest as CreateTopicsRequestV2,
)
from kio.schema.create_topics.v2.request import (
    RequestHeader as CreateTopicsRequestHeaderV2,
)
from kio.schema.create_topics.v2.response import (
    CreateTopicsResponse as CreateTopicsResponseV2,
)
from kio.schema.create_topics.v2.response import (
    ResponseHeader as CreateTopicsResponseHeaderV2,
)
from kio.schema.create_topics.v3.request import (
    CreateTopicsRequest as CreateTopicsRequestV3,
)
from kio.schema.create_topics.v3.request import (
    RequestHeader as CreateTopicsRequestHeaderV3,
)
from kio.schema.create_topics.v3.response import (
    CreateTopicsResponse as CreateTopicsResponseV3,
)
from kio.schema.create_topics.v3.response import (
    ResponseHeader as CreateTopicsResponseHeaderV3,
)
from kio.schema.create_topics.v4.request import (
    CreateTopicsRequest as CreateTopicsRequestV4,
)
from kio.schema.create_topics.v4.request import (
    RequestHeader as CreateTopicsRequestHeaderV4,
)
from kio.schema.create_topics.v4.response import (
    CreateTopicsResponse as CreateTopicsResponseV4,
)
from kio.schema.create_topics.v4.response import (
    ResponseHeader as CreateTopicsResponseHeaderV4,
)
from kio.schema.create_topics.v5.request import (
    CreateTopicsRequest as CreateTopicsRequestV5,
)
from kio.schema.create_topics.v5.request import (
    RequestHeader as CreateTopicsRequestHeaderV5,
)
from kio.schema.create_topics.v5.response import (
    CreateTopicsResponse as CreateTopicsResponseV5,
)
from kio.schema.create_topics.v5.response import (
    ResponseHeader as CreateTopicsResponseHeaderV5,
)
from kio.schema.create_topics.v6.request import (
    CreateTopicsRequest as CreateTopicsRequestV6,
)
from kio.schema.create_topics.v6.request import (
    RequestHeader as CreateTopicsRequestHeaderV6,
)
from kio.schema.create_topics.v6.response import (
    CreateTopicsResponse as CreateTopicsResponseV6,
)
from kio.schema.create_topics.v6.response import (
    ResponseHeader as CreateTopicsResponseHeaderV6,
)
from kio.schema.create_topics.v7.request import (
    CreateTopicsRequest as CreateTopicsRequestV7,
)
from kio.schema.create_topics.v7.request import (
    RequestHeader as CreateTopicsRequestHeaderV7,
)
from kio.schema.create_topics.v7.response import (
    CreateTopicsResponse as CreateTopicsResponseV7,
)
from kio.schema.create_topics.v7.response import (
    ResponseHeader as CreateTopicsResponseHeaderV7,
)


CreateTopicsRequestHeader = (
    CreateTopicsRequestHeaderV0
    | CreateTopicsRequestHeaderV1
    | CreateTopicsRequestHeaderV2
    | CreateTopicsRequestHeaderV3
    | CreateTopicsRequestHeaderV4
    | CreateTopicsRequestHeaderV5
    | CreateTopicsRequestHeaderV6
    | CreateTopicsRequestHeaderV7
)

CreateTopicsResponseHeader = (
    CreateTopicsResponseHeaderV0
    | CreateTopicsResponseHeaderV1
    | CreateTopicsResponseHeaderV2
    | CreateTopicsResponseHeaderV3
    | CreateTopicsResponseHeaderV4
    | CreateTopicsResponseHeaderV5
    | CreateTopicsResponseHeaderV6
    | CreateTopicsResponseHeaderV7
)

CreateTopicsRequest = (
    CreateTopicsRequestV0
    | CreateTopicsRequestV1
    | CreateTopicsRequestV2
    | CreateTopicsRequestV3
    | CreateTopicsRequestV4
    | CreateTopicsRequestV5
    | CreateTopicsRequestV6
    | CreateTopicsRequestV7
)

CreateTopicsResponse = (
    CreateTopicsResponseV0
    | CreateTopicsResponseV1
    | CreateTopicsResponseV2
    | CreateTopicsResponseV3
    | CreateTopicsResponseV4
    | CreateTopicsResponseV5
    | CreateTopicsResponseV6
    | CreateTopicsResponseV7
)
