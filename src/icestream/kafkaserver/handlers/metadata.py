import datetime
from typing import Any, Callable, Sequence

import kio.schema.metadata.v0 as metadata_v0
import kio.schema.metadata.v1 as metadata_v1
import kio.schema.metadata.v2 as metadata_v2
import kio.schema.metadata.v3 as metadata_v3
import kio.schema.metadata.v4 as metadata_v4
import kio.schema.metadata.v5 as metadata_v5
import kio.schema.metadata.v6 as metadata_v6
import structlog
from kio.index import load_payload_module
from kio.schema.errors import ErrorCode
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
from kio.schema.types import BrokerId, TopicName
from kio.static.constants import EntityType
from kio.static.primitive import i32, i32Timedelta
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from icestream.config import Config
from icestream.models import Topic

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

log = structlog.get_logger()


async def do_handle_metadata_request(
    config: Config,
    req: MetadataRequest,
    api_version: int,
    callback: Callable[[MetadataResponse], Any],
) -> None:
    log.info(
        "handling metadata request",
        topics=[t.name for t in getattr(req, "topics", ()) or ()],
    )
    assert config.async_session_factory is not None
    async with config.async_session_factory() as session:
        topic_result: Sequence[Topic]
        if not req.topics:
            result = await session.execute(
                select(Topic).options(selectinload(Topic.partitions))
            )
            topic_result = result.scalars().all()
        else:
            topic_names = [t.name for t in req.topics]
            result = await session.execute(
                select(Topic)
                .where(Topic.name.in_(topic_names))
                .options(selectinload(Topic.partitions))
            )
            topic_result = result.scalars().all()

    broker = metadata_v6.response.MetadataResponseBroker(
        node_id=BrokerId(0), host=config.ADVERTISED_HOST, port=i32(config.ADVERTISED_PORT), rack=None
    )

    topics: list[metadata_v6.response.MetadataResponseTopic] = []
    for topic in topic_result:
        partition_metadata = [
            metadata_v6.response.MetadataResponsePartition(
                error_code=ErrorCode.none,
                partition_index=i32(pidx.partition_number),
                leader_id=BrokerId(0),
                replica_nodes=(BrokerId(0),),
                isr_nodes=(BrokerId(0),),
                offline_replicas=(),
            )
            for pidx in topic.partitions
        ]
        topics.append(
            metadata_v6.response.MetadataResponseTopic(
                error_code=ErrorCode.none,
                name=TopicName(topic.name),
                is_internal=topic.is_internal,
                partitions=tuple(partition_metadata),
            )
        )

    response = metadata_v6.response.MetadataResponse(
        throttle_time=i32Timedelta.parse(datetime.timedelta(milliseconds=0)),
        brokers=(broker,),
        cluster_id="test-cluster",
        controller_id=BrokerId(0),
        topics=tuple(topics),
    )

    if api_version == 0:
        _broker = metadata_v0.response.MetadataResponseBroker(
            node_id=broker.node_id,
            host=broker.host,
            port=broker.port,
        )
        _topics = []
        for topic in topics:
            _partition_metadata = []
            for partition in topic.partitions:
                _partition = metadata_v0.response.MetadataResponsePartition(
                    error_code=partition.error_code,
                    partition_index=partition.partition_index,
                    leader_id=partition.leader_id,
                    replica_nodes=partition.replica_nodes,
                    isr_nodes=partition.isr_nodes,
                )
                _partition_metadata.append(_partition)
            _topic = metadata_v0.response.MetadataResponseTopic(
                error_code=topic.error_code,
                name=topic.name,
                partitions=tuple(_partition_metadata),
            )
            _topics.append(_topic)
        await callback(metadata_v0.response.MetadataResponse(brokers=(_broker,), topics=tuple(_topics)))
    elif api_version == 1:
        _broker = metadata_v1.response.MetadataResponseBroker(
            node_id=broker.node_id,
            host=broker.host,
            port=broker.port,
            rack=broker.rack,
        )
        _topics = []
        for topic in topics:
            _partition_metadata = []
            for partition in topic.partitions:
                _partition = metadata_v1.response.MetadataResponsePartition(
                    error_code=partition.error_code,
                    partition_index=partition.partition_index,
                    leader_id=partition.leader_id,
                    replica_nodes=partition.replica_nodes,
                    isr_nodes=partition.isr_nodes,
                )
                _partition_metadata.append(_partition)
            _topic = metadata_v1.response.MetadataResponseTopic(
                error_code=topic.error_code,
                name=topic.name,
                is_internal=topic.is_internal,
                partitions=tuple(_partition_metadata),
            )
            _topics.append(_topic)
        await callback(
            metadata_v1.response.MetadataResponse(
                brokers=(_broker,),
                topics=tuple(_topics),
                controller_id=BrokerId(_broker.node_id),
            )
        )
    elif api_version == 2:
        _broker = metadata_v2.response.MetadataResponseBroker(
            node_id=broker.node_id,
            host=broker.host,
            port=broker.port,
            rack=broker.rack,
        )
        _topics = []
        for topic in topics:
            _partition_metadata = []
            for partition in topic.partitions:
                _partition = metadata_v2.response.MetadataResponsePartition(
                    error_code=partition.error_code,
                    partition_index=partition.partition_index,
                    leader_id=partition.leader_id,
                    replica_nodes=partition.replica_nodes,
                    isr_nodes=partition.isr_nodes,
                )
                _partition_metadata.append(_partition)
            _topic = metadata_v2.response.MetadataResponseTopic(
                error_code=topic.error_code,
                name=topic.name,
                is_internal=topic.is_internal,
                partitions=tuple(_partition_metadata),
            )
            _topics.append(_topic)
        await callback(
            metadata_v2.response.MetadataResponse(
                brokers=(_broker,),
                topics=tuple(_topics),
                controller_id=BrokerId(_broker.node_id),
                cluster_id=response.cluster_id,
            )
        )
    elif api_version == 3:
        _broker = metadata_v3.response.MetadataResponseBroker(
            node_id=broker.node_id,
            host=broker.host,
            port=broker.port,
            rack=broker.rack,
        )
        _topics = []
        for topic in topics:
            _partition_metadata = []
            for partition in topic.partitions:
                _partition = metadata_v3.response.MetadataResponsePartition(
                    error_code=partition.error_code,
                    partition_index=partition.partition_index,
                    leader_id=partition.leader_id,
                    replica_nodes=partition.replica_nodes,
                    isr_nodes=partition.isr_nodes,
                )
                _partition_metadata.append(_partition)
            _topic = metadata_v3.response.MetadataResponseTopic(
                error_code=topic.error_code,
                name=topic.name,
                is_internal=topic.is_internal,
                partitions=tuple(_partition_metadata),
            )
            _topics.append(_topic)
        await callback(
            metadata_v3.response.MetadataResponse(
                brokers=(_broker,),
                topics=tuple(_topics),
                controller_id=BrokerId(_broker.node_id),
                cluster_id=response.cluster_id,
                throttle_time=response.throttle_time,
            )
        )
    elif api_version == 4:
        _broker = metadata_v4.response.MetadataResponseBroker(
            node_id=broker.node_id,
            host=broker.host,
            port=broker.port,
            rack=broker.rack,
        )
        _topics = []
        for topic in topics:
            _partition_metadata = []
            for partition in topic.partitions:
                _partition = metadata_v4.response.MetadataResponsePartition(
                    error_code=partition.error_code,
                    partition_index=partition.partition_index,
                    leader_id=partition.leader_id,
                    replica_nodes=partition.replica_nodes,
                    isr_nodes=partition.isr_nodes,
                )
                _partition_metadata.append(_partition)
            _topic = metadata_v4.response.MetadataResponseTopic(
                error_code=topic.error_code,
                name=topic.name,
                is_internal=topic.is_internal,
                partitions=tuple(_partition_metadata),
            )
            _topics.append(_topic)
        await callback(
            metadata_v4.response.MetadataResponse(
                brokers=(_broker,),
                topics=tuple(_topics),
                controller_id=BrokerId(_broker.node_id),
                cluster_id=response.cluster_id,
                throttle_time=response.throttle_time,
            )
        )
    elif api_version == 5:
        _broker = metadata_v5.response.MetadataResponseBroker(
            node_id=broker.node_id,
            host=broker.host,
            port=broker.port,
            rack=broker.rack,
        )
        _topics = []
        for topic in topics:
            _partition_metadata = []
            for partition in topic.partitions:
                _partition = metadata_v5.response.MetadataResponsePartition(
                    error_code=partition.error_code,
                    partition_index=partition.partition_index,
                    leader_id=partition.leader_id,
                    replica_nodes=partition.replica_nodes,
                    isr_nodes=partition.isr_nodes,
                    offline_replicas=partition.offline_replicas,
                )
                _partition_metadata.append(_partition)
            _topic = metadata_v5.response.MetadataResponseTopic(
                error_code=topic.error_code,
                name=topic.name,
                is_internal=topic.is_internal,
                partitions=tuple(_partition_metadata),
            )
            _topics.append(_topic)
        await callback(
            metadata_v5.response.MetadataResponse(
                brokers=(_broker,),
                topics=tuple(_topics),
                controller_id=BrokerId(_broker.node_id),
                cluster_id=response.cluster_id,
                throttle_time=response.throttle_time,
            )
        )
    elif api_version == 6:
        await callback(response)
    else:
        log.error("unsupported metadata version", api_version=api_version)


def metadata_error_response(
    req: MetadataRequest,
    api_version: int,
    *,
    error_code: ErrorCode,
    error_message: str,
) -> MetadataResponse:
    mod = load_payload_module(3, api_version, EntityType.response)
    response_topic_class = mod.MetadataResponseTopic
    response_class = mod.MetadataResponse
    _topics = []
    for topic in req.topics:
        _topics.append(response_topic_class(name=topic.name, error_code=error_code))
    _ = error_message
    return response_class(brokers=(), topics=tuple(_topics))
