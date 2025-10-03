from icestream.config import Config


def wal_uri_to_object_key(config: Config, uri: str) -> str:
    bucket = config.WAL_BUCKET
    prefix = (config.WAL_BUCKET_PREFIX or "").strip("/")
    # strip bucket from start
    if uri.startswith(bucket + "/"):
        key = uri[len(bucket) + 1:]
    else:
        key = uri
    # strip prefix if set
    if prefix and key.startswith(prefix + "/"):
        key = key[len(prefix) + 1:]
    return key
