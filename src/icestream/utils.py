import datetime

from kio.static.primitive import i32Timedelta

from icestream.config import Config


def normalize_object_key(config: Config, uri_or_key: str) -> str:
    """Return an object key without bucket/prefix, handling legacy stored URIs."""
    key = uri_or_key
    legacy = False

    if "://" in key:
        legacy = True
        key = key.split("://", 1)[1]
        key = key.split("/", 1)[1] if "/" in key else ""

    bucket = config.WAL_BUCKET
    if key.startswith(bucket + "/"):
        legacy = True
        key = key[len(bucket) + 1:]

    if key.startswith("/"):
        legacy = True
        key = key.lstrip("/")

    if legacy:
        prefix = (config.WAL_BUCKET_PREFIX or "").strip("/")
        if prefix and key.startswith(prefix + "/"):
            key = key[len(prefix) + 1:]

    return key

def zero_throttle():
    return i32timedelta_from_ms(0)

def i32timedelta_from_ms(ms: int):
    return i32Timedelta.parse(datetime.timedelta(milliseconds=ms))
