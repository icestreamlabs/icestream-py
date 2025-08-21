from kio.schema.broker_heartbeat.v0.request import (
    BrokerHeartbeatRequest as BrokerHeartbeatRequestV0,
)
from kio.schema.broker_heartbeat.v0.request import (
    RequestHeader as BrokerHeartbeatRequestHeaderV0,
)
from kio.schema.broker_heartbeat.v0.response import (
    BrokerHeartbeatResponse as BrokerHeartbeatResponseV0,
)
from kio.schema.broker_heartbeat.v0.response import (
    ResponseHeader as BrokerHeartbeatResponseHeaderV0,
)
from kio.schema.broker_heartbeat.v1.request import (
    BrokerHeartbeatRequest as BrokerHeartbeatRequestV1,
)
from kio.schema.broker_heartbeat.v1.request import (
    RequestHeader as BrokerHeartbeatRequestHeaderV1,
)
from kio.schema.broker_heartbeat.v1.response import (
    BrokerHeartbeatResponse as BrokerHeartbeatResponseV1,
)
from kio.schema.broker_heartbeat.v1.response import (
    ResponseHeader as BrokerHeartbeatResponseHeaderV1,
)


BrokerHeartbeatRequestHeader = (
    BrokerHeartbeatRequestHeaderV0 | BrokerHeartbeatRequestHeaderV1
)

BrokerHeartbeatResponseHeader = (
    BrokerHeartbeatResponseHeaderV0 | BrokerHeartbeatResponseHeaderV1
)

BrokerHeartbeatRequest = (
    BrokerHeartbeatRequestV0 | BrokerHeartbeatRequestV1
)

BrokerHeartbeatResponse = (
    BrokerHeartbeatResponseV0 | BrokerHeartbeatResponseV1
)
