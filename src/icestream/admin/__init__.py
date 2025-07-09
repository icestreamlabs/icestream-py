from typing import AsyncGenerator, Callable

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from icestream.config import Config
from icestream.models import Partition, Topic


class TopicCreate(BaseModel):
    name: str
    num_partitions: int = 3


class TopicResponse(BaseModel):
    id: int
    name: str


class AdminApi:
    def __init__(self, config: Config):
        self.config = config
        self.app = FastAPI(title="icestream REST API")
        self._add_routes()

    def _get_session(self) -> Callable[[], AsyncGenerator[AsyncSession, None]]:
        async def _get_session_inner() -> AsyncGenerator[AsyncSession, None]:
            async with self.config.async_session_factory() as session:
                yield session

        return _get_session_inner

    def _add_routes(self):
        @self.app.post("/topics", response_model=TopicResponse, status_code=201)
        async def create_topic(
            data: TopicCreate,
            session: AsyncSession = Depends(self._get_session()),
        ):
            result = await session.execute(select(Topic).where(Topic.name == data.name))
            if result.scalar_one_or_none():
                raise HTTPException(400, detail="Topic already exists")

            new_topic = Topic(name=data.name)
            session.add(new_topic)
            await session.flush()

            for idx in range(data.num_partitions):
                session.add(
                    Partition(
                        topic_name=new_topic.name, partition_number=idx, last_offset=-1
                    )
                )
            await session.commit()

            return TopicResponse(id=new_topic.id, name=new_topic.name)
