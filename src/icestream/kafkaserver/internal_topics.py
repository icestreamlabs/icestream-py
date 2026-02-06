from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select

from icestream.config import Config
from icestream.logger import log
from icestream.models import Partition, Topic

INTERNAL_OFFSETS_TOPIC = "__consumer_offsets"
INTERNAL_TOPICS = frozenset({INTERNAL_OFFSETS_TOPIC})


@dataclass(frozen=True)
class InternalTopicSpec:
    name: str
    partitions: int


def internal_topics(config: Config) -> tuple[InternalTopicSpec, ...]:
    return (
        InternalTopicSpec(
            name=INTERNAL_OFFSETS_TOPIC,
            partitions=int(config.OFFSETS_TOPIC_PARTITIONS),
        ),
    )


async def ensure_internal_topics(config: Config) -> None:
    assert config.async_session_factory is not None
    specs = internal_topics(config)
    if not specs:
        return

    names = [spec.name for spec in specs]
    async with config.async_session_factory() as session:
        result = await session.execute(select(Topic).where(Topic.name.in_(names)))
        existing = {topic.name: topic for topic in result.scalars().all()}
        partition_rows = await session.execute(
            select(Partition.topic_name, Partition.partition_number).where(
                Partition.topic_name.in_(names)
            )
        )
        existing_parts_by_topic: dict[str, set[int]] = {name: set() for name in names}
        for topic_name, partition_number in partition_rows.all():
            existing_parts_by_topic[topic_name].add(partition_number)

        for spec in specs:
            topic = existing.get(spec.name)
            created = False
            if topic is None:
                topic = Topic(name=spec.name, is_internal=True)
                session.add(topic)
                await session.flush()
                created = True
            elif not topic.is_internal:
                topic.is_internal = True

            existing_parts = existing_parts_by_topic.setdefault(topic.name, set())
            for idx in range(spec.partitions):
                if idx not in existing_parts:
                    session.add(
                        Partition(
                            topic_name=topic.name,
                            partition_number=idx,
                            last_offset=-1,
                        )
                    )
                    # Avoid asyncpg insertmanyvalues failures when creating many partitions.
                    await session.flush()
                    existing_parts.add(idx)

            if existing_parts and max(existing_parts) + 1 > spec.partitions:
                log.warning(
                    "internal topic has more partitions than configured",
                    topic=topic.name,
                    configured=spec.partitions,
                    existing=len(existing_parts),
                )
            if created:
                log.info(
                    "created internal topic",
                    topic=topic.name,
                    partitions=spec.partitions,
                )

        await session.commit()


def is_internal_topic(name: str) -> bool:
    return name in INTERNAL_TOPICS


def murmur2(data: bytes) -> int:
    length = len(data)
    seed = 0x9747B28C
    m = 0x5BD1E995
    r = 24

    h = (seed ^ length) & 0xFFFFFFFF
    length4 = length // 4

    for i in range(length4):
        i4 = i * 4
        k = (
            (data[i4 + 0] & 0xFF)
            | ((data[i4 + 1] & 0xFF) << 8)
            | ((data[i4 + 2] & 0xFF) << 16)
            | ((data[i4 + 3] & 0xFF) << 24)
        )
        k = (k * m) & 0xFFFFFFFF
        k ^= (k & 0xFFFFFFFF) >> r
        k = (k * m) & 0xFFFFFFFF

        h = (h * m) & 0xFFFFFFFF
        h ^= k

    remaining = length & 3
    idx = length4 * 4
    if remaining == 3:
        h ^= (data[idx + 2] & 0xFF) << 16
    if remaining >= 2:
        h ^= (data[idx + 1] & 0xFF) << 8
    if remaining >= 1:
        h ^= data[idx + 0] & 0xFF
        h = (h * m) & 0xFFFFFFFF

    h ^= (h & 0xFFFFFFFF) >> 13
    h = (h * m) & 0xFFFFFFFF
    h ^= (h & 0xFFFFFFFF) >> 15

    return h if h < 0x80000000 else h - 0x100000000


def to_positive(value: int) -> int:
    return value & 0x7FFFFFFF


def partition_for_key(key_bytes: bytes, partitions: int) -> int:
    if partitions <= 0:
        raise ValueError("partitions must be positive")
    return to_positive(murmur2(key_bytes)) % partitions
