from kio.schema.list_offsets.v0.request import (
    ListOffsetsRequest as ListOffsetsRequestV0,
)
from kio.schema.list_offsets.v0.request import (
    RequestHeader as ListOffsetsRequestHeaderV0,
)
from kio.schema.list_offsets.v0.response import (
    ListOffsetsResponse as ListOffsetsResponseV0,
)
from kio.schema.list_offsets.v0.response import (
    ResponseHeader as ListOffsetsResponseHeaderV0,
)
from kio.schema.list_offsets.v1.request import (
    ListOffsetsRequest as ListOffsetsRequestV1,
)
from kio.schema.list_offsets.v1.request import (
    RequestHeader as ListOffsetsRequestHeaderV1,
)
from kio.schema.list_offsets.v1.response import (
    ListOffsetsResponse as ListOffsetsResponseV1,
)
from kio.schema.list_offsets.v1.response import (
    ResponseHeader as ListOffsetsResponseHeaderV1,
)
from kio.schema.list_offsets.v2.request import (
    ListOffsetsRequest as ListOffsetsRequestV2,
)
from kio.schema.list_offsets.v2.request import (
    RequestHeader as ListOffsetsRequestHeaderV2,
)
from kio.schema.list_offsets.v2.response import (
    ListOffsetsResponse as ListOffsetsResponseV2,
)
from kio.schema.list_offsets.v2.response import (
    ResponseHeader as ListOffsetsResponseHeaderV2,
)
from kio.schema.list_offsets.v3.request import (
    ListOffsetsRequest as ListOffsetsRequestV3,
)
from kio.schema.list_offsets.v3.request import (
    RequestHeader as ListOffsetsRequestHeaderV3,
)
from kio.schema.list_offsets.v3.response import (
    ListOffsetsResponse as ListOffsetsResponseV3,
)
from kio.schema.list_offsets.v3.response import (
    ResponseHeader as ListOffsetsResponseHeaderV3,
)
from kio.schema.list_offsets.v4.request import (
    ListOffsetsRequest as ListOffsetsRequestV4,
)
from kio.schema.list_offsets.v4.request import (
    RequestHeader as ListOffsetsRequestHeaderV4,
)
from kio.schema.list_offsets.v4.response import (
    ListOffsetsResponse as ListOffsetsResponseV4,
)
from kio.schema.list_offsets.v4.response import (
    ResponseHeader as ListOffsetsResponseHeaderV4,
)
from kio.schema.list_offsets.v5.request import (
    ListOffsetsRequest as ListOffsetsRequestV5,
)
from kio.schema.list_offsets.v5.request import (
    RequestHeader as ListOffsetsRequestHeaderV5,
)
from kio.schema.list_offsets.v5.response import (
    ListOffsetsResponse as ListOffsetsResponseV5,
)
from kio.schema.list_offsets.v5.response import (
    ResponseHeader as ListOffsetsResponseHeaderV5,
)
from kio.schema.list_offsets.v6.request import (
    ListOffsetsRequest as ListOffsetsRequestV6,
)
from kio.schema.list_offsets.v6.request import (
    RequestHeader as ListOffsetsRequestHeaderV6,
)
from kio.schema.list_offsets.v6.response import (
    ListOffsetsResponse as ListOffsetsResponseV6,
)
from kio.schema.list_offsets.v6.response import (
    ResponseHeader as ListOffsetsResponseHeaderV6,
)
from kio.schema.list_offsets.v7.request import (
    ListOffsetsRequest as ListOffsetsRequestV7,
)
from kio.schema.list_offsets.v7.request import (
    RequestHeader as ListOffsetsRequestHeaderV7,
)
from kio.schema.list_offsets.v7.response import (
    ListOffsetsResponse as ListOffsetsResponseV7,
)
from kio.schema.list_offsets.v7.response import (
    ResponseHeader as ListOffsetsResponseHeaderV7,
)
from kio.schema.list_offsets.v8.request import (
    ListOffsetsRequest as ListOffsetsRequestV8,
)
from kio.schema.list_offsets.v8.request import (
    RequestHeader as ListOffsetsRequestHeaderV8,
)
from kio.schema.list_offsets.v8.response import (
    ListOffsetsResponse as ListOffsetsResponseV8,
)
from kio.schema.list_offsets.v8.response import (
    ResponseHeader as ListOffsetsResponseHeaderV8,
)
from kio.schema.list_offsets.v9.request import (
    ListOffsetsRequest as ListOffsetsRequestV9,
)
from kio.schema.list_offsets.v9.request import (
    RequestHeader as ListOffsetsRequestHeaderV9,
)
from kio.schema.list_offsets.v9.response import (
    ListOffsetsResponse as ListOffsetsResponseV9,
)
from kio.schema.list_offsets.v9.response import (
    ResponseHeader as ListOffsetsResponseHeaderV9,
)


ListOffsetsRequestHeader = (
    ListOffsetsRequestHeaderV0
    | ListOffsetsRequestHeaderV1
    | ListOffsetsRequestHeaderV2
    | ListOffsetsRequestHeaderV3
    | ListOffsetsRequestHeaderV4
    | ListOffsetsRequestHeaderV5
    | ListOffsetsRequestHeaderV6
    | ListOffsetsRequestHeaderV7
    | ListOffsetsRequestHeaderV8
    | ListOffsetsRequestHeaderV9
)

ListOffsetsResponseHeader = (
    ListOffsetsResponseHeaderV0
    | ListOffsetsResponseHeaderV1
    | ListOffsetsResponseHeaderV2
    | ListOffsetsResponseHeaderV3
    | ListOffsetsResponseHeaderV4
    | ListOffsetsResponseHeaderV5
    | ListOffsetsResponseHeaderV6
    | ListOffsetsResponseHeaderV7
    | ListOffsetsResponseHeaderV8
    | ListOffsetsResponseHeaderV9
)

ListOffsetsRequest = (
    ListOffsetsRequestV0
    | ListOffsetsRequestV1
    | ListOffsetsRequestV2
    | ListOffsetsRequestV3
    | ListOffsetsRequestV4
    | ListOffsetsRequestV5
    | ListOffsetsRequestV6
    | ListOffsetsRequestV7
    | ListOffsetsRequestV8
    | ListOffsetsRequestV9
)

ListOffsetsResponse = (
    ListOffsetsResponseV0
    | ListOffsetsResponseV1
    | ListOffsetsResponseV2
    | ListOffsetsResponseV3
    | ListOffsetsResponseV4
    | ListOffsetsResponseV5
    | ListOffsetsResponseV6
    | ListOffsetsResponseV7
    | ListOffsetsResponseV8
    | ListOffsetsResponseV9
)
