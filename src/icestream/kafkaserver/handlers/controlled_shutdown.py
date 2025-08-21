from kio.schema.controlled_shutdown.v0.request import (
    ControlledShutdownRequest as ControlledShutdownRequestV0,
)
from kio.schema.controlled_shutdown.v0.request import (
    RequestHeader as ControlledShutdownRequestHeaderV0,
)
from kio.schema.controlled_shutdown.v0.response import (
    ControlledShutdownResponse as ControlledShutdownResponseV0,
)
from kio.schema.controlled_shutdown.v0.response import (
    ResponseHeader as ControlledShutdownResponseHeaderV0,
)
from kio.schema.controlled_shutdown.v1.request import (
    ControlledShutdownRequest as ControlledShutdownRequestV1,
)
from kio.schema.controlled_shutdown.v1.request import (
    RequestHeader as ControlledShutdownRequestHeaderV1,
)
from kio.schema.controlled_shutdown.v1.response import (
    ControlledShutdownResponse as ControlledShutdownResponseV1,
)
from kio.schema.controlled_shutdown.v1.response import (
    ResponseHeader as ControlledShutdownResponseHeaderV1,
)
from kio.schema.controlled_shutdown.v2.request import (
    ControlledShutdownRequest as ControlledShutdownRequestV2,
)
from kio.schema.controlled_shutdown.v2.request import (
    RequestHeader as ControlledShutdownRequestHeaderV2,
)
from kio.schema.controlled_shutdown.v2.response import (
    ControlledShutdownResponse as ControlledShutdownResponseV2,
)
from kio.schema.controlled_shutdown.v2.response import (
    ResponseHeader as ControlledShutdownResponseHeaderV2,
)
from kio.schema.controlled_shutdown.v3.request import (
    ControlledShutdownRequest as ControlledShutdownRequestV3,
)
from kio.schema.controlled_shutdown.v3.request import (
    RequestHeader as ControlledShutdownRequestHeaderV3,
)
from kio.schema.controlled_shutdown.v3.response import (
    ControlledShutdownResponse as ControlledShutdownResponseV3,
)
from kio.schema.controlled_shutdown.v3.response import (
    ResponseHeader as ControlledShutdownResponseHeaderV3,
)


ControlledShutdownRequestHeader = (
    ControlledShutdownRequestHeaderV0 | ControlledShutdownRequestHeaderV1 | ControlledShutdownRequestHeaderV2 | ControlledShutdownRequestHeaderV3
)

ControlledShutdownResponseHeader = (
    ControlledShutdownResponseHeaderV0 | ControlledShutdownResponseHeaderV1 | ControlledShutdownResponseHeaderV2 | ControlledShutdownResponseHeaderV3
)

ControlledShutdownRequest = (
    ControlledShutdownRequestV0 | ControlledShutdownRequestV1 | ControlledShutdownRequestV2 | ControlledShutdownRequestV3
)

ControlledShutdownResponse = (
    ControlledShutdownResponseV0 | ControlledShutdownResponseV1 | ControlledShutdownResponseV2 | ControlledShutdownResponseV3
)
