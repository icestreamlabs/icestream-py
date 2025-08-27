from kio.schema.update_metadata.v0.request import (
    UpdateMetadataRequest as UpdateMetadataRequestV0,
)
from kio.schema.update_metadata.v0.request import (
    RequestHeader as UpdateMetadataRequestHeaderV0,
)
from kio.schema.update_metadata.v0.response import (
    UpdateMetadataResponse as UpdateMetadataResponseV0,
)
from kio.schema.update_metadata.v0.response import (
    ResponseHeader as UpdateMetadataResponseHeaderV0,
)
from kio.schema.update_metadata.v1.request import (
    UpdateMetadataRequest as UpdateMetadataRequestV1,
)
from kio.schema.update_metadata.v1.request import (
    RequestHeader as UpdateMetadataRequestHeaderV1,
)
from kio.schema.update_metadata.v1.response import (
    UpdateMetadataResponse as UpdateMetadataResponseV1,
)
from kio.schema.update_metadata.v1.response import (
    ResponseHeader as UpdateMetadataResponseHeaderV1,
)
from kio.schema.update_metadata.v2.request import (
    UpdateMetadataRequest as UpdateMetadataRequestV2,
)
from kio.schema.update_metadata.v2.request import (
    RequestHeader as UpdateMetadataRequestHeaderV2,
)
from kio.schema.update_metadata.v2.response import (
    UpdateMetadataResponse as UpdateMetadataResponseV2,
)
from kio.schema.update_metadata.v2.response import (
    ResponseHeader as UpdateMetadataResponseHeaderV2,
)
from kio.schema.update_metadata.v3.request import (
    UpdateMetadataRequest as UpdateMetadataRequestV3,
)
from kio.schema.update_metadata.v3.request import (
    RequestHeader as UpdateMetadataRequestHeaderV3,
)
from kio.schema.update_metadata.v3.response import (
    UpdateMetadataResponse as UpdateMetadataResponseV3,
)
from kio.schema.update_metadata.v3.response import (
    ResponseHeader as UpdateMetadataResponseHeaderV3,
)
from kio.schema.update_metadata.v4.request import (
    UpdateMetadataRequest as UpdateMetadataRequestV4,
)
from kio.schema.update_metadata.v4.request import (
    RequestHeader as UpdateMetadataRequestHeaderV4,
)
from kio.schema.update_metadata.v4.response import (
    UpdateMetadataResponse as UpdateMetadataResponseV4,
)
from kio.schema.update_metadata.v4.response import (
    ResponseHeader as UpdateMetadataResponseHeaderV4,
)
from kio.schema.update_metadata.v5.request import (
    UpdateMetadataRequest as UpdateMetadataRequestV5,
)
from kio.schema.update_metadata.v5.request import (
    RequestHeader as UpdateMetadataRequestHeaderV5,
)
from kio.schema.update_metadata.v5.response import (
    UpdateMetadataResponse as UpdateMetadataResponseV5,
)
from kio.schema.update_metadata.v5.response import (
    ResponseHeader as UpdateMetadataResponseHeaderV5,
)
from kio.schema.update_metadata.v6.request import (
    UpdateMetadataRequest as UpdateMetadataRequestV6,
)
from kio.schema.update_metadata.v6.request import (
    RequestHeader as UpdateMetadataRequestHeaderV6,
)
from kio.schema.update_metadata.v6.response import (
    UpdateMetadataResponse as UpdateMetadataResponseV6,
)
from kio.schema.update_metadata.v6.response import (
    ResponseHeader as UpdateMetadataResponseHeaderV6,
)
from kio.schema.update_metadata.v7.request import (
    UpdateMetadataRequest as UpdateMetadataRequestV7,
)
from kio.schema.update_metadata.v7.request import (
    RequestHeader as UpdateMetadataRequestHeaderV7,
)
from kio.schema.update_metadata.v7.response import (
    UpdateMetadataResponse as UpdateMetadataResponseV7,
)
from kio.schema.update_metadata.v7.response import (
    ResponseHeader as UpdateMetadataResponseHeaderV7,
)
from kio.schema.update_metadata.v8.request import (
    UpdateMetadataRequest as UpdateMetadataRequestV8,
)
from kio.schema.update_metadata.v8.request import (
    RequestHeader as UpdateMetadataRequestHeaderV8,
)
from kio.schema.update_metadata.v8.response import (
    UpdateMetadataResponse as UpdateMetadataResponseV8,
)
from kio.schema.update_metadata.v8.response import (
    ResponseHeader as UpdateMetadataResponseHeaderV8,
)

UpdateMetadataRequestHeader = (
    UpdateMetadataRequestHeaderV0
    | UpdateMetadataRequestHeaderV1
    | UpdateMetadataRequestHeaderV2
    | UpdateMetadataRequestHeaderV3
    | UpdateMetadataRequestHeaderV4
    | UpdateMetadataRequestHeaderV5
    | UpdateMetadataRequestHeaderV6
    | UpdateMetadataRequestHeaderV7
    | UpdateMetadataRequestHeaderV8
)

UpdateMetadataResponseHeader = (
    UpdateMetadataResponseHeaderV0
    | UpdateMetadataResponseHeaderV1
    | UpdateMetadataResponseHeaderV2
    | UpdateMetadataResponseHeaderV3
    | UpdateMetadataResponseHeaderV4
    | UpdateMetadataResponseHeaderV5
    | UpdateMetadataResponseHeaderV6
    | UpdateMetadataResponseHeaderV7
    | UpdateMetadataResponseHeaderV8
)

UpdateMetadataRequest = (
    UpdateMetadataRequestV0
    | UpdateMetadataRequestV1
    | UpdateMetadataRequestV2
    | UpdateMetadataRequestV3
    | UpdateMetadataRequestV4
    | UpdateMetadataRequestV5
    | UpdateMetadataRequestV6
    | UpdateMetadataRequestV7
    | UpdateMetadataRequestV8
)

UpdateMetadataResponse = (
    UpdateMetadataResponseV0
    | UpdateMetadataResponseV1
    | UpdateMetadataResponseV2
    | UpdateMetadataResponseV3
    | UpdateMetadataResponseV4
    | UpdateMetadataResponseV5
    | UpdateMetadataResponseV6
    | UpdateMetadataResponseV7
    | UpdateMetadataResponseV8
)
