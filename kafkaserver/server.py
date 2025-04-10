import asyncio
from asyncio import Server as AsyncIOServer, StreamReader, StreamWriter

from kafkaserver.handlers import KafkaHandler


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
        pass
