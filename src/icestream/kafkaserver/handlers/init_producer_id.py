from kio.schema.init_producer_id.v0.request import (
    InitProducerIdRequest as InitProducerIdRequestV0,
)
from kio.schema.init_producer_id.v0.request import (
    RequestHeader as InitProducerIdRequestHeaderV0,
)
from kio.schema.init_producer_id.v0.response import (
    InitProducerIdResponse as InitProducerIdResponseV0,
)
from kio.schema.init_producer_id.v0.response import (
    ResponseHeader as InitProducerIdResponseHeaderV0,
)
from kio.schema.init_producer_id.v1.request import (
    InitProducerIdRequest as InitProducerIdRequestV1,
)
from kio.schema.init_producer_id.v1.request import (
    RequestHeader as InitProducerIdRequestHeaderV1,
)
from kio.schema.init_producer_id.v1.response import (
    InitProducerIdResponse as InitProducerIdResponseV1,
)
from kio.schema.init_producer_id.v1.response import (
    ResponseHeader as InitProducerIdResponseHeaderV1,
)
from kio.schema.init_producer_id.v2.request import (
    InitProducerIdRequest as InitProducerIdRequestV2,
)
from kio.schema.init_producer_id.v2.request import (
    RequestHeader as InitProducerIdRequestHeaderV2,
)
from kio.schema.init_producer_id.v2.response import (
    InitProducerIdResponse as InitProducerIdResponseV2,
)
from kio.schema.init_producer_id.v2.response import (
    ResponseHeader as InitProducerIdResponseHeaderV2,
)
from kio.schema.init_producer_id.v3.request import (
    InitProducerIdRequest as InitProducerIdRequestV3,
)
from kio.schema.init_producer_id.v3.request import (
    RequestHeader as InitProducerIdRequestHeaderV3,
)
from kio.schema.init_producer_id.v3.response import (
    InitProducerIdResponse as InitProducerIdResponseV3,
)
from kio.schema.init_producer_id.v3.response import (
    ResponseHeader as InitProducerIdResponseHeaderV3,
)
from kio.schema.init_producer_id.v4.request import (
    InitProducerIdRequest as InitProducerIdRequestV4,
)
from kio.schema.init_producer_id.v4.request import (
    RequestHeader as InitProducerIdRequestHeaderV4,
)
from kio.schema.init_producer_id.v4.response import (
    InitProducerIdResponse as InitProducerIdResponseV4,
)
from kio.schema.init_producer_id.v4.response import (
    ResponseHeader as InitProducerIdResponseHeaderV4,
)
from kio.schema.init_producer_id.v5.request import (
    InitProducerIdRequest as InitProducerIdRequestV5,
)
from kio.schema.init_producer_id.v5.request import (
    RequestHeader as InitProducerIdRequestHeaderV5,
)
from kio.schema.init_producer_id.v5.response import (
    InitProducerIdResponse as InitProducerIdResponseV5,
)
from kio.schema.init_producer_id.v5.response import (
    ResponseHeader as InitProducerIdResponseHeaderV5,
)


InitProducerIdRequestHeader = (
    InitProducerIdRequestHeaderV0
    | InitProducerIdRequestHeaderV1
    | InitProducerIdRequestHeaderV2
    | InitProducerIdRequestHeaderV3
    | InitProducerIdRequestHeaderV4
    | InitProducerIdRequestHeaderV5
)

InitProducerIdResponseHeader = (
    InitProducerIdResponseHeaderV0
    | InitProducerIdResponseHeaderV1
    | InitProducerIdResponseHeaderV2
    | InitProducerIdResponseHeaderV3
    | InitProducerIdResponseHeaderV4
    | InitProducerIdResponseHeaderV5
)

InitProducerIdRequest = (
    InitProducerIdRequestV0
    | InitProducerIdRequestV1
    | InitProducerIdRequestV2
    | InitProducerIdRequestV3
    | InitProducerIdRequestV4
    | InitProducerIdRequestV5
)

InitProducerIdResponse = (
    InitProducerIdResponseV0
    | InitProducerIdResponseV1
    | InitProducerIdResponseV2
    | InitProducerIdResponseV3
    | InitProducerIdResponseV4
    | InitProducerIdResponseV5
)
