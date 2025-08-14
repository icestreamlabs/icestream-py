from kio.schema.fetch.v0.request import (
    FetchRequest as FetchRequestV0,
)
from kio.schema.fetch.v0.request import (
    RequestHeader as FetchRequestHeaderV0,
)
from kio.schema.fetch.v0.response import (
    FetchResponse as FetchResponseV0,
)
from kio.schema.fetch.v0.response import (
    ResponseHeader as FetchResponseHeaderV0,
)
from kio.schema.fetch.v1.request import (
    FetchRequest as FetchRequestV1,
)
from kio.schema.fetch.v1.request import (
    RequestHeader as FetchRequestHeaderV1,
)
from kio.schema.fetch.v1.response import (
    FetchResponse as FetchResponseV1,
)
from kio.schema.fetch.v1.response import (
    ResponseHeader as FetchResponseHeaderV1,
)
from kio.schema.fetch.v2.request import (
    FetchRequest as FetchRequestV2,
)
from kio.schema.fetch.v2.request import (
    RequestHeader as FetchRequestHeaderV2,
)
from kio.schema.fetch.v2.response import (
    FetchResponse as FetchResponseV2,
)
from kio.schema.fetch.v2.response import (
    ResponseHeader as FetchResponseHeaderV2,
)
from kio.schema.fetch.v3.request import (
    FetchRequest as FetchRequestV3,
)
from kio.schema.fetch.v3.request import (
    RequestHeader as FetchRequestHeaderV3,
)
from kio.schema.fetch.v3.response import (
    FetchResponse as FetchResponseV3,
)
from kio.schema.fetch.v3.response import (
    ResponseHeader as FetchResponseHeaderV3,
)
from kio.schema.fetch.v4.request import (
    FetchRequest as FetchRequestV4,
)
from kio.schema.fetch.v4.request import (
    RequestHeader as FetchRequestHeaderV4,
)
from kio.schema.fetch.v4.response import (
    FetchResponse as FetchResponseV4,
)
from kio.schema.fetch.v4.response import (
    ResponseHeader as FetchResponseHeaderV4,
)
from kio.schema.fetch.v5.request import (
    FetchRequest as FetchRequestV5,
)
from kio.schema.fetch.v5.request import (
    RequestHeader as FetchRequestHeaderV5,
)
from kio.schema.fetch.v5.response import (
    FetchResponse as FetchResponseV5,
)
from kio.schema.fetch.v5.response import (
    ResponseHeader as FetchResponseHeaderV5,
)
from kio.schema.fetch.v6.request import (
    FetchRequest as FetchRequestV6,
)
from kio.schema.fetch.v6.request import (
    RequestHeader as FetchRequestHeaderV6,
)
from kio.schema.fetch.v6.response import (
    FetchResponse as FetchResponseV6,
)
from kio.schema.fetch.v6.response import (
    ResponseHeader as FetchResponseHeaderV6,
)
from kio.schema.fetch.v7.request import (
    FetchRequest as FetchRequestV7,
)
from kio.schema.fetch.v7.request import (
    RequestHeader as FetchRequestHeaderV7,
)
from kio.schema.fetch.v7.response import (
    FetchResponse as FetchResponseV7,
)
from kio.schema.fetch.v7.response import (
    ResponseHeader as FetchResponseHeaderV7,
)
from kio.schema.fetch.v8.request import (
    FetchRequest as FetchRequestV8,
)
from kio.schema.fetch.v8.request import (
    RequestHeader as FetchRequestHeaderV8,
)
from kio.schema.fetch.v8.response import (
    FetchResponse as FetchResponseV8,
)
from kio.schema.fetch.v8.response import (
    ResponseHeader as FetchResponseHeaderV8,
)
from kio.schema.fetch.v9.request import (
    FetchRequest as FetchRequestV9,
)
from kio.schema.fetch.v9.request import (
    RequestHeader as FetchRequestHeaderV9,
)
from kio.schema.fetch.v9.response import (
    FetchResponse as FetchResponseV9,
)
from kio.schema.fetch.v9.response import (
    ResponseHeader as FetchResponseHeaderV9,
)
from kio.schema.fetch.v10.request import (
    FetchRequest as FetchRequestV10,
)
from kio.schema.fetch.v10.request import (
    RequestHeader as FetchRequestHeaderV10,
)
from kio.schema.fetch.v10.response import (
    FetchResponse as FetchResponseV10,
)
from kio.schema.fetch.v10.response import (
    ResponseHeader as FetchResponseHeaderV10,
)
from kio.schema.fetch.v11.request import (
    FetchRequest as FetchRequestV11,
)
from kio.schema.fetch.v11.request import (
    RequestHeader as FetchRequestHeaderV11,
)
from kio.schema.fetch.v11.response import (
    FetchResponse as FetchResponseV11,
)
from kio.schema.fetch.v11.response import (
    ResponseHeader as FetchResponseHeaderV11,
)

FetchRequestHeader = (
    FetchRequestHeaderV0
    | FetchRequestHeaderV1
    | FetchRequestHeaderV2
    | FetchRequestHeaderV3
    | FetchRequestHeaderV4
    | FetchRequestHeaderV5
    | FetchRequestHeaderV6
    | FetchRequestHeaderV7
    | FetchRequestHeaderV8
    | FetchRequestHeaderV9
    | FetchRequestHeaderV10
    | FetchRequestHeaderV11
)

FetchResponseHeader = (
    FetchResponseHeaderV0
    | FetchResponseHeaderV1
    | FetchResponseHeaderV2
    | FetchResponseHeaderV3
    | FetchResponseHeaderV4
    | FetchResponseHeaderV5
    | FetchResponseHeaderV6
    | FetchResponseHeaderV7
    | FetchResponseHeaderV8
    | FetchResponseHeaderV9
    | FetchResponseHeaderV10
    | FetchResponseHeaderV11
)

FetchRequest = (
    FetchRequestV0
    | FetchRequestV1
    | FetchRequestV2
    | FetchRequestV3
    | FetchRequestV4
    | FetchRequestV5
    | FetchRequestV6
    | FetchRequestV7
    | FetchRequestV8
    | FetchRequestV9
    | FetchRequestV10
    | FetchRequestV11
)

FetchResponse = (
    FetchResponseV0
    | FetchResponseV1
    | FetchResponseV2
    | FetchResponseV3
    | FetchResponseV4
    | FetchResponseV5
    | FetchResponseV6
    | FetchResponseV7
    | FetchResponseV8
    | FetchResponseV9
    | FetchResponseV10
    | FetchResponseV11
)
