from typing import Protocol, Callable, Any

from kio.schema.errors import ErrorCode
from kafkaserver.messages import ProduceRequest, ProduceResponse, ProduceRequestHeader

class KafkaHandler(Protocol):
    async def handle_produce_request(self, header: ProduceRequestHeader, req: ProduceRequest, callback: Callable[[ProduceResponse], Any]): pass
    def produce_request_error_response(self, error_code: ErrorCode, error_message: str, req: ProduceRequest) -> ProduceResponse: pass
