from __future__ import annotations

from typing import Any, Iterable, get_args, get_origin

from kio.schema.errors import ErrorCode
from kio.static.primitive import i32, i64

from icestream.utils import zero_throttle


def _sequence_element(annotation: Any) -> type[Any] | None:
    origin = get_origin(annotation)
    if origin in (list, tuple):
        args = get_args(annotation)
        if not args:
            return None
        if len(args) == 2 and args[1] is Ellipsis:
            return args[0]
        return args[0]
    return None


def _is_optional(annotation: Any) -> bool:
    origin = get_origin(annotation)
    if origin is None:
        return False
    args = get_args(annotation)
    return any(arg is type(None) for arg in args)


def _first_field(cls: type[Any], names: Iterable[str]) -> str | None:
    fields = getattr(cls, "__dataclass_fields__", {})
    for name in names:
        if name in fields:
            return name
    return None


def _field_annotation(cls: type[Any], field_name: str) -> Any:
    fields = getattr(cls, "__dataclass_fields__", {})
    field = fields.get(field_name)
    if field is not None and getattr(field, "type", None) is not None:
        return field.type
    return getattr(cls, "__annotations__", {}).get(field_name)


def _string_value(cls: type[Any], field_name: str, value: str | None) -> str | None:
    annotation = _field_annotation(cls, field_name)
    if value is None and annotation is not None and not _is_optional(annotation):
        return ""
    return value


def build_partition_response(
    partition_cls: type[Any],
    *,
    partition: int,
    error_code: ErrorCode,
    committed_offset: int | None = None,
    committed_metadata: str | None = None,
    commit_timestamp_ms: int | None = None,
    leader_epoch: int | None = None,
) -> Any:
    fields = getattr(partition_cls, "__dataclass_fields__", {})
    kwargs: dict[str, Any] = {}

    if "partition_index" in fields:
        kwargs["partition_index"] = i32(partition)
    elif "partition" in fields:
        kwargs["partition"] = i32(partition)
    elif "partition_id" in fields:
        kwargs["partition_id"] = i32(partition)

    if "error_code" in fields:
        kwargs["error_code"] = error_code

    if committed_offset is not None:
        if "committed_offset" in fields:
            kwargs["committed_offset"] = i64(committed_offset)
        elif "offset" in fields:
            kwargs["offset"] = i64(committed_offset)

    if "committed_metadata" in fields:
        kwargs["committed_metadata"] = _string_value(
            partition_cls, "committed_metadata", committed_metadata
        )
    elif "metadata" in fields:
        kwargs["metadata"] = _string_value(partition_cls, "metadata", committed_metadata)

    if commit_timestamp_ms is not None:
        if "commit_timestamp" in fields:
            kwargs["commit_timestamp"] = i64(commit_timestamp_ms)
        elif "timestamp" in fields:
            kwargs["timestamp"] = i64(commit_timestamp_ms)
        elif "committed_timestamp" in fields:
            kwargs["committed_timestamp"] = i64(commit_timestamp_ms)

    if leader_epoch is not None:
        if "committed_leader_epoch" in fields:
            kwargs["committed_leader_epoch"] = i32(leader_epoch)
        elif "leader_epoch" in fields:
            kwargs["leader_epoch"] = i32(leader_epoch)

    return partition_cls(**kwargs)


def build_topic_response(
    topic_cls: type[Any],
    *,
    topic_name: str,
    partitions: Iterable[Any],
) -> Any:
    fields = getattr(topic_cls, "__dataclass_fields__", {})
    kwargs: dict[str, Any] = {}

    if "name" in fields:
        kwargs["name"] = topic_name
    elif "topic" in fields:
        kwargs["topic"] = topic_name
    elif "topic_name" in fields:
        kwargs["topic_name"] = topic_name

    partition_field = _first_field(topic_cls, ("partitions", "partition_responses", "responses"))
    if partition_field:
        kwargs[partition_field] = tuple(partitions)

    return topic_cls(**kwargs)


def build_group_response(
    group_cls: type[Any],
    *,
    group_id: str,
    topics: Iterable[Any],
    error_code: ErrorCode | None,
) -> Any:
    fields = getattr(group_cls, "__dataclass_fields__", {})
    kwargs: dict[str, Any] = {}

    if "group_id" in fields:
        kwargs["group_id"] = group_id
    elif "group" in fields:
        kwargs["group"] = group_id

    topic_field = _first_field(group_cls, ("topics", "responses"))
    if topic_field:
        kwargs[topic_field] = tuple(topics)

    if error_code is not None and "error_code" in fields:
        kwargs["error_code"] = error_code

    return group_cls(**kwargs)


def build_offset_response(
    response_cls: type[Any],
    *,
    topics: Iterable[Any] | None = None,
    groups: Iterable[Any] | None = None,
    error_code: ErrorCode | None = None,
) -> Any:
    fields = getattr(response_cls, "__dataclass_fields__", {})
    kwargs: dict[str, Any] = {}

    if "throttle_time" in fields:
        kwargs["throttle_time"] = zero_throttle()

    if error_code is not None and "error_code" in fields:
        kwargs["error_code"] = error_code

    if groups is not None:
        group_field = _first_field(response_cls, ("groups",))
        if group_field:
            kwargs[group_field] = tuple(groups)
            return response_cls(**kwargs)

    if topics is not None:
        topic_field = _first_field(response_cls, ("topics", "responses"))
        if topic_field:
            kwargs[topic_field] = tuple(topics)

    return response_cls(**kwargs)


def response_sequence_element(response_cls: type[Any], field_name: str) -> type[Any] | None:
    annotation = _field_annotation(response_cls, field_name)
    return _sequence_element(annotation)
