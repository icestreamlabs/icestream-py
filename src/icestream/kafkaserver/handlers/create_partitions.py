from kio.schema.create_partitions.v0.request import CreatePartitionsRequest as CreatePartitionsRequestV0
from kio.schema.create_partitions.v0.request import RequestHeader as CreatePartitionsRequestHeaderV0
from kio.schema.create_partitions.v0.response import CreatePartitionsResponse as CreatePartitionsResponseV0
from kio.schema.create_partitions.v0.response import ResponseHeader as CreatePartitionsResponseHeaderV0
from kio.schema.create_partitions.v1.request import CreatePartitionsRequest as CreatePartitionsRequestV1
from kio.schema.create_partitions.v1.request import RequestHeader as CreatePartitionsRequestHeaderV1
from kio.schema.create_partitions.v1.response import CreatePartitionsResponse as CreatePartitionsResponseV1
from kio.schema.create_partitions.v1.response import ResponseHeader as CreatePartitionsResponseHeaderV1
from kio.schema.create_partitions.v2.request import CreatePartitionsRequest as CreatePartitionsRequestV2
from kio.schema.create_partitions.v2.request import RequestHeader as CreatePartitionsRequestHeaderV2
from kio.schema.create_partitions.v2.response import CreatePartitionsResponse as CreatePartitionsResponseV2
from kio.schema.create_partitions.v2.response import ResponseHeader as CreatePartitionsResponseHeaderV2
from kio.schema.create_partitions.v3.request import CreatePartitionsRequest as CreatePartitionsRequestV3
from kio.schema.create_partitions.v3.request import RequestHeader as CreatePartitionsRequestHeaderV3
from kio.schema.create_partitions.v3.response import CreatePartitionsResponse as CreatePartitionsResponseV3
from kio.schema.create_partitions.v3.response import ResponseHeader as CreatePartitionsResponseHeaderV3

CreatePartitionsRequestHeader = (
    CreatePartitionsRequestHeaderV0
    | CreatePartitionsRequestHeaderV1
    | CreatePartitionsRequestHeaderV2
    | CreatePartitionsRequestHeaderV3
)

CreatePartitionsResponseHeader = (
    CreatePartitionsResponseHeaderV0
    | CreatePartitionsResponseHeaderV1
    | CreatePartitionsResponseHeaderV2
    | CreatePartitionsResponseHeaderV3
)

CreatePartitionsRequest = (
    CreatePartitionsRequestV0
    | CreatePartitionsRequestV1
    | CreatePartitionsRequestV2
    | CreatePartitionsRequestV3
)

CreatePartitionsResponse = (
    CreatePartitionsResponseV0
    | CreatePartitionsResponseV1
    | CreatePartitionsResponseV2
    | CreatePartitionsResponseV3
)
