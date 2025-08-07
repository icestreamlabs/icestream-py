from io import BytesIO


def encode_varint(n: int) -> bytes:
    out = bytearray()
    while True:
        to_write = n & 0x7F
        n >>= 7
        if n:
            out.append(to_write | 0x80)
        else:
            out.append(to_write)
            break
    return bytes(out)


def decode_varint(buf: BytesIO) -> int:
    shift = 0
    result = 0
    while True:
        b = buf.read(1)
        if not b:
            raise EOFError("Unexpected EOF in varint")
        byte = b[0]
        result |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            break
        shift += 7
    return result


def decode_varlong(buf: BytesIO) -> int:
    return decode_varint(buf)


def decode_signed_varint(buf: BytesIO) -> int:
    raw = decode_varint(buf)
    return (raw >> 1) ^ -(raw & 1)


def decode_signed_varlong(buf: BytesIO) -> int:
    return decode_signed_varint(buf)
