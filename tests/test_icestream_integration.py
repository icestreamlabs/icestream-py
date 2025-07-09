import pytest
from aiokafka import AIOKafkaProducer

TEST_TOPIC = "test_topic"
BOOTSTRAP_SERVERS = "localhost:9092"


@pytest.mark.asyncio
async def test_produce_single_message(http_client):
    resp = await http_client.post(
        "/topics",
        json={"name": TEST_TOPIC, "num_partitions": 3},
    )
    assert resp.status_code in (200, 201, 400)

    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        await producer.send(TEST_TOPIC, b"hello kafka from test", key=b"test_key")
    finally:
        await producer.stop()
