from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TopicBackend:
    name: str
    is_internal: bool


OBJECT_STORE_BACKEND = TopicBackend(name="object_store", is_internal=False)
POSTGRES_INTERNAL_BACKEND = TopicBackend(name="postgres_internal", is_internal=True)


def topic_backend_for_name(topic_name: str) -> TopicBackend:
    if topic_name.startswith("__"):
        return POSTGRES_INTERNAL_BACKEND
    return OBJECT_STORE_BACKEND
