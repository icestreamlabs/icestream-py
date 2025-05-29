from kio.schema.produce.v0.request import (
    ProduceRequest as ProduceRequestV0,
)
from kio.schema.produce.v0.request import (
    RequestHeader as ProduceRequestHeaderV0,
)
from kio.schema.produce.v0.response import (
    ProduceResponse as ProduceResponseV0,
)
from kio.schema.produce.v0.response import (
    ResponseHeader as ProduceResponseHeaderV0,
)
from kio.schema.produce.v1.request import (
    ProduceRequest as ProduceRequestV1,
)
from kio.schema.produce.v1.request import (
    RequestHeader as ProduceRequestHeaderV1,
)
from kio.schema.produce.v1.response import (
    ProduceResponse as ProduceResponseV1,
)
from kio.schema.produce.v1.response import (
    ResponseHeader as ProduceResponseHeaderV1,
)
from kio.schema.produce.v2.request import (
    ProduceRequest as ProduceRequestV2,
)
from kio.schema.produce.v2.request import (
    RequestHeader as ProduceRequestHeaderV2,
)
from kio.schema.produce.v2.response import (
    ProduceResponse as ProduceResponseV2,
)
from kio.schema.produce.v2.response import (
    ResponseHeader as ProduceResponseHeaderV2,
)
from kio.schema.produce.v3.request import (
    ProduceRequest as ProduceRequestV3,
)
from kio.schema.produce.v3.request import (
    RequestHeader as ProduceRequestHeaderV3,
)
from kio.schema.produce.v3.response import (
    ProduceResponse as ProduceResponseV3,
)
from kio.schema.produce.v3.response import (
    ResponseHeader as ProduceResponseHeaderV3,
)
from kio.schema.produce.v4.request import (
    ProduceRequest as ProduceRequestV4,
)
from kio.schema.produce.v4.request import (
    RequestHeader as ProduceRequestHeaderV4,
)
from kio.schema.produce.v4.response import (
    ProduceResponse as ProduceResponseV4,
)
from kio.schema.produce.v4.response import (
    ResponseHeader as ProduceResponseHeaderV4,
)
from kio.schema.produce.v5.request import (
    ProduceRequest as ProduceRequestV5,
)
from kio.schema.produce.v5.request import (
    RequestHeader as ProduceRequestHeaderV5,
)
from kio.schema.produce.v5.response import (
    ProduceResponse as ProduceResponseV5,
)
from kio.schema.produce.v5.response import (
    ResponseHeader as ProduceResponseHeaderV5,
)
from kio.schema.produce.v6.request import (
    ProduceRequest as ProduceRequestV6,
)
from kio.schema.produce.v6.request import (
    RequestHeader as ProduceRequestHeaderV6,
)
from kio.schema.produce.v6.response import (
    ProduceResponse as ProduceResponseV6,
)
from kio.schema.produce.v6.response import (
    ResponseHeader as ProduceResponseHeaderV6,
)
from kio.schema.produce.v7.request import (
    ProduceRequest as ProduceRequestV7,
)
from kio.schema.produce.v7.request import (
    RequestHeader as ProduceRequestHeaderV7,
)
from kio.schema.produce.v7.response import (
    ProduceResponse as ProduceResponseV7,
)
from kio.schema.produce.v7.response import (
    ResponseHeader as ProduceResponseHeaderV7,
)
from kio.schema.produce.v8.request import (
    ProduceRequest as ProduceRequestV8,
)
from kio.schema.produce.v8.request import (
    RequestHeader as ProduceRequestHeaderV8,
)
from kio.schema.produce.v8.response import (
    ProduceResponse as ProduceResponseV8,
)
from kio.schema.produce.v8.response import (
    ResponseHeader as ProduceResponseHeaderV8,
)

ProduceRequestHeader = (
    ProduceRequestHeaderV0
    | ProduceRequestHeaderV1
    | ProduceRequestHeaderV2
    | ProduceRequestHeaderV3
    | ProduceRequestHeaderV4
    | ProduceRequestHeaderV5
    | ProduceRequestHeaderV6
    | ProduceRequestHeaderV7
    | ProduceRequestHeaderV8
)

ProduceResponseHeader = (
    ProduceResponseHeaderV0
    | ProduceResponseHeaderV1
    | ProduceResponseHeaderV2
    | ProduceResponseHeaderV3
    | ProduceResponseHeaderV4
    | ProduceResponseHeaderV5
    | ProduceResponseHeaderV6
    | ProduceResponseHeaderV7
    | ProduceResponseHeaderV8
)

ProduceRequest = (
    ProduceRequestV0
    | ProduceRequestV1
    | ProduceRequestV2
    | ProduceRequestV3
    | ProduceRequestV4
    | ProduceRequestV5
    | ProduceRequestV6
    | ProduceRequestV7
    | ProduceRequestV8
)

ProduceResponse = (
    ProduceResponseV0
    | ProduceResponseV1
    | ProduceResponseV2
    | ProduceResponseV3
    | ProduceResponseV4
    | ProduceResponseV5
    | ProduceResponseV6
    | ProduceResponseV7
    | ProduceResponseV8
)
