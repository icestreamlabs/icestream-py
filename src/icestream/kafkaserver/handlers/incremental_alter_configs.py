from kio.schema.incremental_alter_configs.v0.request import (
    IncrementalAlterConfigsRequest as IncrementalAlterConfigsRequestV0,
)
from kio.schema.incremental_alter_configs.v0.request import (
    RequestHeader as IncrementalAlterConfigsRequestHeaderV0,
)
from kio.schema.incremental_alter_configs.v0.response import (
    IncrementalAlterConfigsResponse as IncrementalAlterConfigsResponseV0,
)
from kio.schema.incremental_alter_configs.v0.response import (
    ResponseHeader as IncrementalAlterConfigsResponseHeaderV0,
)
from kio.schema.incremental_alter_configs.v1.request import (
    IncrementalAlterConfigsRequest as IncrementalAlterConfigsRequestV1,
)
from kio.schema.incremental_alter_configs.v1.request import (
    RequestHeader as IncrementalAlterConfigsRequestHeaderV1,
)
from kio.schema.incremental_alter_configs.v1.response import (
    IncrementalAlterConfigsResponse as IncrementalAlterConfigsResponseV1,
)
from kio.schema.incremental_alter_configs.v1.response import (
    ResponseHeader as IncrementalAlterConfigsResponseHeaderV1,
)


IncrementalAlterConfigsRequestHeader = (
    IncrementalAlterConfigsRequestHeaderV0 | IncrementalAlterConfigsRequestHeaderV1
)

IncrementalAlterConfigsResponseHeader = (
    IncrementalAlterConfigsResponseHeaderV0 | IncrementalAlterConfigsResponseHeaderV1
)

IncrementalAlterConfigsRequest = (
    IncrementalAlterConfigsRequestV0 | IncrementalAlterConfigsRequestV1
)

IncrementalAlterConfigsResponse = (
    IncrementalAlterConfigsResponseV0 | IncrementalAlterConfigsResponseV1
)
