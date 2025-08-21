from kio.schema.alter_partition.v0.request import (
    AlterPartitionRequest as AlterPartitionRequestV0,
)
from kio.schema.alter_partition.v0.request import (
    RequestHeader as AlterPartitionRequestHeaderV0,
)
from kio.schema.alter_partition.v0.response import (
    AlterPartitionResponse as AlterPartitionResponseV0,
)
from kio.schema.alter_partition.v0.response import (
    ResponseHeader as AlterPartitionResponseHeaderV0,
)
from kio.schema.alter_partition.v1.request import (
    AlterPartitionRequest as AlterPartitionRequestV1,
)
from kio.schema.alter_partition.v1.request import (
    RequestHeader as AlterPartitionRequestHeaderV1,
)
from kio.schema.alter_partition.v1.response import (
    AlterPartitionResponse as AlterPartitionResponseV1,
)
from kio.schema.alter_partition.v1.response import (
    ResponseHeader as AlterPartitionResponseHeaderV1,
)
from kio.schema.alter_partition.v2.request import (
    AlterPartitionRequest as AlterPartitionRequestV2,
)
from kio.schema.alter_partition.v2.request import (
    RequestHeader as AlterPartitionRequestHeaderV2,
)
from kio.schema.alter_partition.v2.response import (
    AlterPartitionResponse as AlterPartitionResponseV2,
)
from kio.schema.alter_partition.v2.response import (
    ResponseHeader as AlterPartitionResponseHeaderV2,
)
from kio.schema.alter_partition.v3.request import (
    AlterPartitionRequest as AlterPartitionRequestV3,
)
from kio.schema.alter_partition.v3.request import (
    RequestHeader as AlterPartitionRequestHeaderV3,
)
from kio.schema.alter_partition.v3.response import (
    AlterPartitionResponse as AlterPartitionResponseV3,
)
from kio.schema.alter_partition.v3.response import (
    ResponseHeader as AlterPartitionResponseHeaderV3,
)

AlterPartitionRequestHeader = (
    AlterPartitionRequestHeaderV0 | AlterPartitionRequestHeaderV1 | AlterPartitionRequestHeaderV2 | AlterPartitionRequestHeaderV3
)

AlterPartitionResponseHeader = (
    AlterPartitionResponseHeaderV0 | AlterPartitionResponseHeaderV1 | AlterPartitionResponseHeaderV2 | AlterPartitionResponseHeaderV3
)

AlterPartitionRequest = (
    AlterPartitionRequestV0 | AlterPartitionRequestV1 | AlterPartitionRequestV2 | AlterPartitionRequestV3
)

AlterPartitionResponse = (
    AlterPartitionResponseV0 | AlterPartitionResponseV1 | AlterPartitionResponseV2 | AlterPartitionResponseV3
)
