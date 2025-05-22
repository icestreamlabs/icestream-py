from typing import Callable, Any

from kio.schema.metadata.v0.request import (
    MetadataRequest as MetadataRequestV0,
    MetadataRequestTopic as MetadataRequestTopicV0,
    TopicName as TopicNameV0,
    RequestHeader as MetadataRequestHeaderV0,
)
from kio.schema.metadata.v0.response import (
    MetadataResponse as MetadataResponseV0,
    ResponseHeader as MetadataResponseHeaderV0,
    MetadataResponsePartition as MetadataResponsePartitionV0,
    MetadataResponseTopic as MetadataResponseTopicV0,
    MetadataResponseBroker as MetadataResponseBrokerV0,
)

from kio.schema.metadata.v1.request import (
    MetadataRequest as MetadataRequestV1,
    MetadataRequestTopic as MetadataRequestTopicV1,
    TopicName as TopicNameV1,
    RequestHeader as MetadataRequestHeaderV1,
)
from kio.schema.metadata.v1.response import (
    MetadataResponse as MetadataResponseV1,
    ResponseHeader as MetadataResponseHeaderV1,
    MetadataResponsePartition as MetadataResponsePartitionV1,
    MetadataResponseTopic as MetadataResponseTopicV1,
    MetadataResponseBroker as MetadataResponseBrokerV1,
)

from kio.schema.metadata.v2.request import (
    MetadataRequest as MetadataRequestV2,
    MetadataRequestTopic as MetadataRequestTopicV2,
    TopicName as TopicNameV2,
    RequestHeader as MetadataRequestHeaderV2,
)
from kio.schema.metadata.v2.response import (
    MetadataResponse as MetadataResponseV2,
    ResponseHeader as MetadataResponseHeaderV2,
    MetadataResponsePartition as MetadataResponsePartitionV2,
    MetadataResponseTopic as MetadataResponseTopicV2,
    MetadataResponseBroker as MetadataResponseBrokerV2,
)

from kio.schema.metadata.v3.request import (
    MetadataRequest as MetadataRequestV3,
    MetadataRequestTopic as MetadataRequestTopicV3,
    TopicName as TopicNameV3,
    RequestHeader as MetadataRequestHeaderV3,
)
from kio.schema.metadata.v3.response import (
    MetadataResponse as MetadataResponseV3,
    ResponseHeader as MetadataResponseHeaderV3,
    MetadataResponsePartition as MetadataResponsePartitionV3,
    MetadataResponseTopic as MetadataResponseTopicV3,
    MetadataResponseBroker as MetadataResponseBrokerV3,
)

from kio.schema.metadata.v4.request import (
    MetadataRequest as MetadataRequestV4,
    MetadataRequestTopic as MetadataRequestTopicV4,
    TopicName as TopicNameV4,
    RequestHeader as MetadataRequestHeaderV4,
)
from kio.schema.metadata.v4.response import (
    MetadataResponse as MetadataResponseV4,
    ResponseHeader as MetadataResponseHeaderV4,
    MetadataResponsePartition as MetadataResponsePartitionV4,
    MetadataResponseTopic as MetadataResponseTopicV4,
    MetadataResponseBroker as MetadataResponseBrokerV4,
)

from kio.schema.metadata.v5.request import (
    MetadataRequest as MetadataRequestV5,
    MetadataRequestTopic as MetadataRequestTopicV5,
    TopicName as TopicNameV5,
    RequestHeader as MetadataRequestHeaderV5,
)
from kio.schema.metadata.v5.response import (
    MetadataResponse as MetadataResponseV5,
    ResponseHeader as MetadataResponseHeaderV5,
    MetadataResponsePartition as MetadataResponsePartitionV5,
    MetadataResponseTopic as MetadataResponseTopicV5,
    MetadataResponseBroker as MetadataResponseBrokerV5,
)

from kio.schema.metadata.v6.request import (
    MetadataRequest as MetadataRequestV6,
    MetadataRequestTopic as MetadataRequestTopicV6,
    TopicName as TopicNameV6,
    RequestHeader as MetadataRequestHeaderV6,
)
from kio.schema.metadata.v6.response import (
    MetadataResponse as MetadataResponseV6,
    ResponseHeader as MetadataResponseHeaderV6,
    MetadataResponsePartition as MetadataResponsePartitionV6,
    MetadataResponseTopic as MetadataResponseTopicV6,
    MetadataResponseBroker as MetadataResponseBrokerV6,
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
