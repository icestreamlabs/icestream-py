from kio.schema.update_features.v0.request import (
    UpdateFeaturesRequest as UpdateFeaturesRequestV0,
)
from kio.schema.update_features.v0.request import (
    RequestHeader as UpdateFeaturesRequestHeaderV0,
)
from kio.schema.update_features.v0.response import (
    UpdateFeaturesResponse as UpdateFeaturesResponseV0,
)
from kio.schema.update_features.v0.response import (
    ResponseHeader as UpdateFeaturesResponseHeaderV0,
)
from kio.schema.update_features.v1.request import (
    UpdateFeaturesRequest as UpdateFeaturesRequestV1,
)
from kio.schema.update_features.v1.request import (
    RequestHeader as UpdateFeaturesRequestHeaderV1,
)
from kio.schema.update_features.v1.response import (
    UpdateFeaturesResponse as UpdateFeaturesResponseV1,
)
from kio.schema.update_features.v1.response import (
    ResponseHeader as UpdateFeaturesResponseHeaderV1,
)

UpdateFeaturesRequestHeader = (
    UpdateFeaturesRequestHeaderV0 | UpdateFeaturesRequestHeaderV1
)
UpdateFeaturesResponseHeader = (
    UpdateFeaturesResponseHeaderV0 | UpdateFeaturesResponseHeaderV1
)
UpdateFeaturesRequest = UpdateFeaturesRequestV0 | UpdateFeaturesRequestV1
UpdateFeaturesResponse = UpdateFeaturesResponseV0 | UpdateFeaturesResponseV1
