from kio.schema.broker_registration.v0.request import (
    BrokerRegistrationRequest as BrokerRegistrationRequestV0,
)
from kio.schema.broker_registration.v0.request import (
    RequestHeader as BrokerRegistrationRequestHeaderV0,
)
from kio.schema.broker_registration.v0.response import (
    BrokerRegistrationResponse as BrokerRegistrationResponseV0,
)
from kio.schema.broker_registration.v0.response import (
    ResponseHeader as BrokerRegistrationResponseHeaderV0,
)
from kio.schema.broker_registration.v1.request import (
    BrokerRegistrationRequest as BrokerRegistrationRequestV1,
)
from kio.schema.broker_registration.v1.request import (
    RequestHeader as BrokerRegistrationRequestHeaderV1,
)
from kio.schema.broker_registration.v1.response import (
    BrokerRegistrationResponse as BrokerRegistrationResponseV1,
)
from kio.schema.broker_registration.v1.response import (
    ResponseHeader as BrokerRegistrationResponseHeaderV1,
)
from kio.schema.broker_registration.v2.request import (
    BrokerRegistrationRequest as BrokerRegistrationRequestV2,
)
from kio.schema.broker_registration.v2.request import (
    RequestHeader as BrokerRegistrationRequestHeaderV2,
)
from kio.schema.broker_registration.v2.response import (
    BrokerRegistrationResponse as BrokerRegistrationResponseV2,
)
from kio.schema.broker_registration.v2.response import (
    ResponseHeader as BrokerRegistrationResponseHeaderV2,
)
from kio.schema.broker_registration.v3.request import (
    BrokerRegistrationRequest as BrokerRegistrationRequestV3,
)
from kio.schema.broker_registration.v3.request import (
    RequestHeader as BrokerRegistrationRequestHeaderV3,
)
from kio.schema.broker_registration.v3.response import (
    BrokerRegistrationResponse as BrokerRegistrationResponseV3,
)
from kio.schema.broker_registration.v3.response import (
    ResponseHeader as BrokerRegistrationResponseHeaderV3,
)
from kio.schema.broker_registration.v4.request import (
    BrokerRegistrationRequest as BrokerRegistrationRequestV4,
)
from kio.schema.broker_registration.v4.request import (
    RequestHeader as BrokerRegistrationRequestHeaderV4,
)
from kio.schema.broker_registration.v4.response import (
    BrokerRegistrationResponse as BrokerRegistrationResponseV4,
)
from kio.schema.broker_registration.v4.response import (
    ResponseHeader as BrokerRegistrationResponseHeaderV4,
)


BrokerRegistrationRequestHeader = (
    BrokerRegistrationRequestHeaderV0 | BrokerRegistrationRequestHeaderV1 | BrokerRegistrationRequestHeaderV2 | BrokerRegistrationRequestHeaderV3 | BrokerRegistrationRequestHeaderV4
)

BrokerRegistrationResponseHeader = (
    BrokerRegistrationResponseHeaderV0 | BrokerRegistrationResponseHeaderV1 | BrokerRegistrationResponseHeaderV2 | BrokerRegistrationResponseHeaderV3 | BrokerRegistrationResponseHeaderV4
)

BrokerRegistrationRequest = (
    BrokerRegistrationRequestV0 | BrokerRegistrationRequestV1 | BrokerRegistrationRequestV2 | BrokerRegistrationRequestV3 | BrokerRegistrationRequestV4
)

BrokerRegistrationResponse = (
    BrokerRegistrationResponseV0 | BrokerRegistrationResponseV1 | BrokerRegistrationResponseV2 | BrokerRegistrationResponseV3 | BrokerRegistrationResponseV4
)
