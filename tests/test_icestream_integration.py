import asyncio
import threading

import pytest
from aiokafka import AIOKafkaProducer

from icestream.kafkaserver.server import Server

BOOTSTRAP_SERVERS = "localhost:9092"
TEST_TOPIC = "test_topic"


def run_server():
    server = Server()
    asyncio.run(server.run())


@pytest.fixture(scope="session", autouse=True)
def start_server():
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()


@pytest.mark.asyncio
async def test_produce_single_message():
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    try:
        await producer.start()
        value = b"hello kafka from test"
        key = b"test_key"
        await producer.send(TEST_TOPIC, value, key=key)
    finally:
        await producer.stop()
