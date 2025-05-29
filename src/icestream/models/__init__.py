from sqlalchemy import Integer, Identity
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Topic(DeclarativeBase):
    __tablename__ = "topics"

    id: Mapped[int] = mapped_column(Integer, Identity(always=True), primary_key=True)
