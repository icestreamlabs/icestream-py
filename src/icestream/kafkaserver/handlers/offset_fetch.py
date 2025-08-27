from kio.schema.offset_fetch.v0.request import (
    OffsetFetchRequest as OffsetFetchRequestV0,
)
from kio.schema.offset_fetch.v0.request import (
    RequestHeader as OffsetFetchRequestHeaderV0,
)
from kio.schema.offset_fetch.v0.response import (
    OffsetFetchResponse as OffsetFetchResponseV0,
)
from kio.schema.offset_fetch.v0.response import (
    ResponseHeader as OffsetFetchResponseHeaderV0,
)
from kio.schema.offset_fetch.v1.request import (
    OffsetFetchRequest as OffsetFetchRequestV1,
)
from kio.schema.offset_fetch.v1.request import (
    RequestHeader as OffsetFetchRequestHeaderV1,
)
from kio.schema.offset_fetch.v1.response import (
    OffsetFetchResponse as OffsetFetchResponseV1,
)
from kio.schema.offset_fetch.v1.response import (
    ResponseHeader as OffsetFetchResponseHeaderV1,
)
from kio.schema.offset_fetch.v2.request import (
    OffsetFetchRequest as OffsetFetchRequestV2,
)
from kio.schema.offset_fetch.v2.request import (
    RequestHeader as OffsetFetchRequestHeaderV2,
)
from kio.schema.offset_fetch.v2.response import (
    OffsetFetchResponse as OffsetFetchResponseV2,
)
from kio.schema.offset_fetch.v2.response import (
    ResponseHeader as OffsetFetchResponseHeaderV2,
)
from kio.schema.offset_fetch.v3.request import (
    OffsetFetchRequest as OffsetFetchRequestV3,
)
from kio.schema.offset_fetch.v3.request import (
    RequestHeader as OffsetFetchRequestHeaderV3,
)
from kio.schema.offset_fetch.v3.response import (
    OffsetFetchResponse as OffsetFetchResponseV3,
)
from kio.schema.offset_fetch.v3.response import (
    ResponseHeader as OffsetFetchResponseHeaderV3,
)
from kio.schema.offset_fetch.v4.request import (
    OffsetFetchRequest as OffsetFetchRequestV4,
)
from kio.schema.offset_fetch.v4.request import (
    RequestHeader as OffsetFetchRequestHeaderV4,
)
from kio.schema.offset_fetch.v4.response import (
    OffsetFetchResponse as OffsetFetchResponseV4,
)
from kio.schema.offset_fetch.v4.response import (
    ResponseHeader as OffsetFetchResponseHeaderV4,
)
from kio.schema.offset_fetch.v5.request import (
    OffsetFetchRequest as OffsetFetchRequestV5,
)
from kio.schema.offset_fetch.v5.request import (
    RequestHeader as OffsetFetchRequestHeaderV5,
)
from kio.schema.offset_fetch.v5.response import (
    OffsetFetchResponse as OffsetFetchResponseV5,
)
from kio.schema.offset_fetch.v5.response import (
    ResponseHeader as OffsetFetchResponseHeaderV5,
)
from kio.schema.offset_fetch.v6.request import (
    OffsetFetchRequest as OffsetFetchRequestV6,
)
from kio.schema.offset_fetch.v6.request import (
    RequestHeader as OffsetFetchRequestHeaderV6,
)
from kio.schema.offset_fetch.v6.response import (
    OffsetFetchResponse as OffsetFetchResponseV6,
)
from kio.schema.offset_fetch.v6.response import (
    ResponseHeader as OffsetFetchResponseHeaderV6,
)
from kio.schema.offset_fetch.v7.request import (
    OffsetFetchRequest as OffsetFetchRequestV7,
)
from kio.schema.offset_fetch.v7.request import (
    RequestHeader as OffsetFetchRequestHeaderV7,
)
from kio.schema.offset_fetch.v7.response import (
    OffsetFetchResponse as OffsetFetchResponseV7,
)
from kio.schema.offset_fetch.v7.response import (
    ResponseHeader as OffsetFetchResponseHeaderV7,
)
from kio.schema.offset_fetch.v8.request import (
    OffsetFetchRequest as OffsetFetchRequestV8,
)
from kio.schema.offset_fetch.v8.request import (
    RequestHeader as OffsetFetchRequestHeaderV8,
)
from kio.schema.offset_fetch.v8.response import (
    OffsetFetchResponse as OffsetFetchResponseV8,
)
from kio.schema.offset_fetch.v8.response import (
    ResponseHeader as OffsetFetchResponseHeaderV8,
)
from kio.schema.offset_fetch.v9.request import (
    OffsetFetchRequest as OffsetFetchRequestV9,
)
from kio.schema.offset_fetch.v9.request import (
    RequestHeader as OffsetFetchRequestHeaderV9,
)
from kio.schema.offset_fetch.v9.response import (
    OffsetFetchResponse as OffsetFetchResponseV9,
)
from kio.schema.offset_fetch.v9.response import (
    ResponseHeader as OffsetFetchResponseHeaderV9,
)


OffsetFetchRequestHeader = (
    OffsetFetchRequestHeaderV0
    | OffsetFetchRequestHeaderV1
    | OffsetFetchRequestHeaderV2
    | OffsetFetchRequestHeaderV3
    | OffsetFetchRequestHeaderV4
    | OffsetFetchRequestHeaderV5
    | OffsetFetchRequestHeaderV6
    | OffsetFetchRequestHeaderV7
    | OffsetFetchRequestHeaderV8
    | OffsetFetchRequestHeaderV9
)

OffsetFetchResponseHeader = (
    OffsetFetchResponseHeaderV0
    | OffsetFetchResponseHeaderV1
    | OffsetFetchResponseHeaderV2
    | OffsetFetchResponseHeaderV3
    | OffsetFetchResponseHeaderV4
    | OffsetFetchResponseHeaderV5
    | OffsetFetchResponseHeaderV6
    | OffsetFetchResponseHeaderV7
    | OffsetFetchResponseHeaderV8
    | OffsetFetchResponseHeaderV9
)

OffsetFetchRequest = (
    OffsetFetchRequestV0
    | OffsetFetchRequestV1
    | OffsetFetchRequestV2
    | OffsetFetchRequestV3
    | OffsetFetchRequestV4
    | OffsetFetchRequestV5
    | OffsetFetchRequestV6
    | OffsetFetchRequestV7
    | OffsetFetchRequestV8
    | OffsetFetchRequestV9
)

OffsetFetchResponse = (
    OffsetFetchResponseV0
    | OffsetFetchResponseV1
    | OffsetFetchResponseV2
    | OffsetFetchResponseV3
    | OffsetFetchResponseV4
    | OffsetFetchResponseV5
    | OffsetFetchResponseV6
    | OffsetFetchResponseV7
    | OffsetFetchResponseV8
    | OffsetFetchResponseV9
)
