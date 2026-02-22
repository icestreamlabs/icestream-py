import asyncio
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition


async def _wait_for_assignment(
    consumer: AIOKafkaConsumer, timeout: float = 10.0
) -> set[TopicPartition]:
    async def _poll() -> set[TopicPartition]:
        while not consumer.assignment():
            await asyncio.sleep(0.1)
        return consumer.assignment()

    return await asyncio.wait_for(_poll(), timeout=timeout)


async def _wait_for_group_assignments(
    consumers: list[AIOKafkaConsumer],
    *,
    expected_total_partitions: int,
    expected_non_empty_consumers: int,
    require_single_partition_each: bool,
    timeout: float = 20.0,
) -> list[set[TopicPartition]]:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    last_matching_signature: tuple[tuple[tuple[str, int], ...], ...] | None = None
    last_signature: tuple[tuple[tuple[str, int], ...], ...] = tuple()
    while loop.time() < deadline:
        assignments = [set(consumer.assignment()) for consumer in consumers]
        union = set().union(*assignments)
        total_assigned = sum(len(assignment) for assignment in assignments)
        non_empty = sum(1 for assignment in assignments if assignment)
        signature = tuple(
            tuple(sorted((tp.topic, tp.partition) for tp in assignment))
            for assignment in assignments
        )
        last_signature = signature

        matches = (
            len(union) == expected_total_partitions
            and total_assigned == len(union)
            and non_empty == expected_non_empty_consumers
        )
        if require_single_partition_each:
            matches = matches and all(len(assignment) == 1 for assignment in assignments)

        if matches and signature == last_matching_signature:
            return assignments

        last_matching_signature = signature if matches else None
        await asyncio.sleep(0.2)

    raise AssertionError(
        "consumer group assignment did not converge: "
        f"expected_total_partitions={expected_total_partitions}, "
        f"expected_non_empty_consumers={expected_non_empty_consumers}, "
        f"require_single_partition_each={require_single_partition_each}, "
        f"last_signature={last_signature}"
    )


async def _consume_one_from_partition(
    consumer: AIOKafkaConsumer,
    partition: TopicPartition,
    *,
    timeout: float = 10.0,
):
    async def _poll():
        while True:
            batches = await consumer.getmany(timeout_ms=1000, max_records=1)
            records = batches.get(partition, ())
            if records:
                return records[0]

    return await asyncio.wait_for(_poll(), timeout=timeout)


async def _produce_one_message_per_partition(
    topic: str,
    *,
    bootstrap_servers: str,
    payload_by_partition: dict[int, bytes],
) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await producer.start()
    try:
        pending = []
        for partition, payload in payload_by_partition.items():
            send_coro = await producer.send(topic, payload, partition=partition)
            pending.append(send_coro)
        await asyncio.gather(*pending)
    finally:
        await producer.stop()


async def _stop_consumer_with_timeout(
    consumer: AIOKafkaConsumer, timeout: float = 5.0
) -> None:
    try:
        await asyncio.wait_for(consumer.stop(), timeout=timeout)
    except Exception:
        return


@pytest.mark.asyncio
async def test_consumer_group_join_consume_and_commit(http_client, bootstrap_servers):
    topic = f"test_consumer_group_{uuid4().hex[:8]}"
    group_id = f"{topic}_group"

    resp = await http_client.post(
        "/topics",
        json={"name": topic, "num_partitions": 1},
    )
    assert resp.status_code in (200, 201, 400)

    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await producer.start()
    try:
        send_coro = await producer.send(
            topic, b"hello consumer group", key=b"group_key"
        )
        metadata = await asyncio.wait_for(send_coro, timeout=10.0)
        assert metadata is not None
    finally:
        await producer.stop()

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        assignments = await _wait_for_assignment(consumer, timeout=10.0)
        assert assignments

        msg = await asyncio.wait_for(consumer.getone(), timeout=10.0)
        assert msg.topic == topic
        assert msg.value == b"hello consumer group"

        await consumer.commit()
        committed = await consumer.committed(TopicPartition(topic, msg.partition))
        assert committed == msg.offset + 1
    finally:
        await _stop_consumer_with_timeout(consumer)


@pytest.mark.asyncio
async def test_consumer_group_rebalance_with_three_consumers(http_client, bootstrap_servers):
    topic = f"test_consumer_group_rebalance_{uuid4().hex[:8]}"
    group_id = f"{topic}_group"

    resp = await http_client.post(
        "/topics",
        json={"name": topic, "num_partitions": 3},
    )
    assert resp.status_code in (200, 201, 400)

    consumers: list[AIOKafkaConsumer] = []
    try:
        for idx in (1, 2):
            consumer = AIOKafkaConsumer(
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                client_id=f"{group_id}-consumer-{idx}",
            )
            await consumer.start()
            consumer.subscribe([topic])
            consumers.append(consumer)

        first_assignments = await _wait_for_group_assignments(
            consumers,
            expected_total_partitions=3,
            expected_non_empty_consumers=2,
            require_single_partition_each=False,
        )
        assert sorted(len(assignment) for assignment in first_assignments) == [1, 2]

        consumer_3 = AIOKafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            client_id=f"{group_id}-consumer-3",
        )
        await consumer_3.start()
        consumer_3.subscribe([topic])
        consumers.append(consumer_3)

        second_assignments = await _wait_for_group_assignments(
            consumers,
            expected_total_partitions=3,
            expected_non_empty_consumers=3,
            require_single_partition_each=True,
        )

        assignment_pairs = [
            (consumer, next(iter(assignment)))
            for consumer, assignment in zip(consumers, second_assignments)
        ]
        assert len({tp.partition for _, tp in assignment_pairs}) == 3

        payload_by_partition = {
            0: b"group-rebalance-partition-0",
            1: b"group-rebalance-partition-1",
            2: b"group-rebalance-partition-2",
        }
        await _produce_one_message_per_partition(
            topic,
            bootstrap_servers=bootstrap_servers,
            payload_by_partition=payload_by_partition,
        )

        records = await asyncio.gather(
            *[
                _consume_one_from_partition(consumer, tp)
                for consumer, tp in assignment_pairs
            ]
        )
        for record in records:
            assert record.topic == topic
            assert record.value == payload_by_partition[record.partition]

        for consumer, tp in assignment_pairs:
            await consumer.commit()
            committed = await consumer.committed(tp)
            assert committed == 1
    finally:
        await asyncio.gather(
            *(_stop_consumer_with_timeout(consumer) for consumer in consumers),
            return_exceptions=True,
        )
