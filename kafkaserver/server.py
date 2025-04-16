import asyncio
import io
import struct
from asyncio import Server as AsyncIOServer, StreamReader, StreamWriter
from typing import Callable, Any

from kio.schema.errors import ErrorCode
from kio.serial import entity_reader
from kio.serial.readers import read_int32
from kio.static.primitive import i64
from kio.static.protocol import RequestHeader

from kafkaserver.handler import handle_kafka_request
from kafkaserver.handlers import KafkaHandler
from kafkaserver.messages import ProduceRequestHeader, ProduceRequest, ProduceResponse, PartitionProduceResponse, TopicProduceResponse


class Server:
    def __init__(self):
        self.listener: AsyncIOServer | None = None

    async def run(self, host: str = '127.0.0.1', port: int = 9092):
        self.listener = await asyncio.start_server(Connection(self), host, port)
        async with self.listener:
            await self.listener.serve_forever()


class Connection(KafkaHandler):
    def __init__(self, s: Server):
        self.server: Server = s

    async def __call__(self, reader: StreamReader, writer: StreamWriter) -> None:
        try:
            while not reader.at_eof():
                msg_length_bytes = await reader.readexactly(4)
                msg_length = read_int32(io.BytesIO(msg_length_bytes))
                message = await reader.readexactly(msg_length)
                api_key = struct.unpack('>H', message[:2])[0]
                await handle_kafka_request(api_key, message, self, writer)

        except Exception as e:
            pass

    async def handle_produce_request(self, header: ProduceRequestHeader, req: ProduceRequest,
                                     callback: Callable[[ProduceResponse], Any]):
        pass

    def produce_request_error_response(self, error_code: ErrorCode, error_message: str, req: ProduceRequest) -> ProduceResponse:
        responses = []
        for i, topic_data in enumerate(req.topic_data):
            partition_response = []
            for j, partition_data in enumerate(topic_data.partition_data):
                partition_produce_response = PartitionProduceResponse(
                    index=partition_data.index, error_code=error_code,
                    error_message=error_message, base_offset=i64(0),
                    record_errors=()
                )
                partition_response.append(partition_produce_response)

            topic_produce_response = TopicProduceResponse(
                name=topic_data.name,
                partition_responses=tuple(partition_response)
            )
            responses.append(topic_produce_response)
        resp = ProduceResponse(responses=tuple(responses))
        return resp
