from typing import Any, Callable

from kio.schema.metadata.v0.request import (
    MetadataRequest as MetadataRequestV0,
)
from kio.schema.metadata.v0.request import (
    RequestHeader as MetadataRequestHeaderV0,
)
from kio.schema.metadata.v0.response import (
    MetadataResponse as MetadataResponseV0,
)
from kio.schema.metadata.v1.request import (
    MetadataRequest as MetadataRequestV1,
)
from kio.schema.metadata.v1.request import (
    RequestHeader as MetadataRequestHeaderV1,
)
from kio.schema.metadata.v1.response import (
    MetadataResponse as MetadataResponseV1,
)
from kio.schema.metadata.v2.request import (
    MetadataRequest as MetadataRequestV2,
)
from kio.schema.metadata.v2.request import (
    RequestHeader as MetadataRequestHeaderV2,
)
from kio.schema.metadata.v2.response import (
    MetadataResponse as MetadataResponseV2,
)
from kio.schema.metadata.v3.request import (
    MetadataRequest as MetadataRequestV3,
)
from kio.schema.metadata.v3.request import (
    RequestHeader as MetadataRequestHeaderV3,
)
from kio.schema.metadata.v3.response import (
    MetadataResponse as MetadataResponseV3,
)
from kio.schema.metadata.v4.request import (
    MetadataRequest as MetadataRequestV4,
)
from kio.schema.metadata.v4.request import (
    RequestHeader as MetadataRequestHeaderV4,
)
from kio.schema.metadata.v4.response import (
    MetadataResponse as MetadataResponseV4,
)
from kio.schema.metadata.v5.request import (
    MetadataRequest as MetadataRequestV5,
)
from kio.schema.metadata.v5.request import (
    RequestHeader as MetadataRequestHeaderV5,
)
from kio.schema.metadata.v5.response import (
    MetadataResponse as MetadataResponseV5,
)
from kio.schema.metadata.v6.request import (
    MetadataRequest as MetadataRequestV6,
)
from kio.schema.metadata.v6.request import (
    RequestHeader as MetadataRequestHeaderV6,
)
from kio.schema.metadata.v6.response import (
    MetadataResponse as MetadataResponseV6,
)

MetadataRequestHeader = (
    MetadataRequestHeaderV0
    | MetadataRequestHeaderV1
    | MetadataRequestHeaderV2
    | MetadataRequestHeaderV3
    | MetadataRequestHeaderV4
    | MetadataRequestHeaderV5
    | MetadataRequestHeaderV6
)

MetadataRequest = (
    MetadataRequestV0
    | MetadataRequestV1
    | MetadataRequestV2
    | MetadataRequestV3
    | MetadataRequestV4
    | MetadataRequestV5
    | MetadataRequestV6
)

MetadataResponse = (
    MetadataResponseV0
    | MetadataResponseV1
    | MetadataResponseV2
    | MetadataResponseV3
    | MetadataResponseV4
    | MetadataResponseV5
    | MetadataResponseV6
)


async def do_handle_metadata_request(
    header: MetadataRequestHeader,
    req: MetadataRequest,
    api_version: int,
    callback: Callable[[MetadataResponse], Any],
):
    pass
