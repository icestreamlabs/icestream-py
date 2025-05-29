from kio.schema.api_versions.v0.request import (
    ApiVersionsRequest as ApiVersionsRequestV0,
)
from kio.schema.api_versions.v0.request import (
    RequestHeader as ApiVersionsRequestHeaderV0,
)
from kio.schema.api_versions.v0.response import (
    ApiVersionsResponse as ApiVersionsResponseV0,
)
from kio.schema.api_versions.v0.response import (
    ResponseHeader as ApiVersionsResponseHeaderV0,
)
from kio.schema.api_versions.v1.request import (
    ApiVersionsRequest as ApiVersionsRequestV1,
)
from kio.schema.api_versions.v1.request import (
    RequestHeader as ApiVersionsRequestHeaderV1,
)
from kio.schema.api_versions.v1.response import (
    ApiVersionsResponse as ApiVersionsResponseV1,
)
from kio.schema.api_versions.v1.response import (
    ResponseHeader as ApiVersionsResponseHeaderV1,
)
from kio.schema.api_versions.v2.request import (
    ApiVersionsRequest as ApiVersionsRequestV2,
)
from kio.schema.api_versions.v2.request import (
    RequestHeader as ApiVersionsRequestHeaderV2,
)
from kio.schema.api_versions.v2.response import (
    ApiVersionsResponse as ApiVersionsResponseV2,
)
from kio.schema.api_versions.v2.response import (
    ResponseHeader as ApiVersionsResponseHeaderV2,
)
from kio.schema.api_versions.v3.request import (
    ApiVersionsRequest as ApiVersionsRequestV3,
)
from kio.schema.api_versions.v3.request import (
    RequestHeader as ApiVersionsRequestHeaderV3,
)
from kio.schema.api_versions.v3.response import (
    ApiVersionsResponse as ApiVersionsResponseV3,
)
from kio.schema.api_versions.v3.response import (
    ResponseHeader as ApiVersionsResponseHeaderV3,
)
from kio.schema.api_versions.v4.request import (
    ApiVersionsRequest as ApiVersionsRequestV4,
)
from kio.schema.api_versions.v4.request import (
    RequestHeader as ApiVersionsRequestHeaderV4,
)
from kio.schema.api_versions.v4.response import (
    ApiVersionsResponse as ApiVersionsResponseV4,
)
from kio.schema.api_versions.v4.response import (
    ResponseHeader as ApiVersionsResponseHeaderV4,
)

ApiVersionsRequestHeader = (
    ApiVersionsRequestHeaderV0
    | ApiVersionsRequestHeaderV1
    | ApiVersionsRequestHeaderV2
    | ApiVersionsRequestHeaderV3
    | ApiVersionsRequestHeaderV4
)

ApiVersionsRequest = (
    ApiVersionsRequestV0
    | ApiVersionsRequestV1
    | ApiVersionsRequestV2
    | ApiVersionsRequestV3
    | ApiVersionsRequestV4
)

ApiVersionsResponse = (
    ApiVersionsResponseV0
    | ApiVersionsResponseV1
    | ApiVersionsResponseV2
    | ApiVersionsResponseV3
    | ApiVersionsResponseV4
)
