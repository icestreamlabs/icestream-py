from kio.schema.alter_configs.v0.request import (
    AlterConfigsRequest as AlterConfigsRequestV0,
)
from kio.schema.alter_configs.v0.request import (
    RequestHeader as AlterConfigsRequestHeaderV0,
)
from kio.schema.alter_configs.v0.response import (
    AlterConfigsResponse as AlterConfigsResponseV0,
)
from kio.schema.alter_configs.v0.response import (
    ResponseHeader as AlterConfigsResponseHeaderV0,
)
from kio.schema.alter_configs.v1.request import (
    AlterConfigsRequest as AlterConfigsRequestV1,
)
from kio.schema.alter_configs.v1.request import (
    RequestHeader as AlterConfigsRequestHeaderV1,
)
from kio.schema.alter_configs.v1.response import (
    AlterConfigsResponse as AlterConfigsResponseV1,
)
from kio.schema.alter_configs.v1.response import (
    ResponseHeader as AlterConfigsResponseHeaderV1,
)
from kio.schema.alter_configs.v2.request import (
    AlterConfigsRequest as AlterConfigsRequestV2,
)
from kio.schema.alter_configs.v2.request import (
    RequestHeader as AlterConfigsRequestHeaderV2,
)
from kio.schema.alter_configs.v2.response import (
    AlterConfigsResponse as AlterConfigsResponseV2,
)
from kio.schema.alter_configs.v2.response import (
    ResponseHeader as AlterConfigsResponseHeaderV2,
)

AlterConfigsRequestHeader = (
    AlterConfigsRequestHeaderV0 | AlterConfigsRequestHeaderV1 | AlterConfigsRequestHeaderV2
)

AlterConfigsResponseHeader = (
    AlterConfigsResponseHeaderV0 | AlterConfigsResponseHeaderV1 | AlterConfigsResponseHeaderV2
)

AlterConfigsRequest = (
    AlterConfigsRequestV0 | AlterConfigsRequestV1 | AlterConfigsRequestV2
)

AlterConfigsResponse = (
    AlterConfigsResponseV0 | AlterConfigsResponseV1 | AlterConfigsResponseV2
)
