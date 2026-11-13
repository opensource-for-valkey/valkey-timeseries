"""A codec for the series module value inside a `DUMP` payload or an RDB file.

The module value is a sequence of opcode-tagged values (`2` unsigned/signed integer, `4` double,
`5` string) ending in opcode `0`, in this order:

    id, label count [name, value]..., retention, rounding marker [kind, digits],
    duplicate policy, max_time_delta, max_value_delta, encoding, chunk_size_bytes,
    chunk count, chunks..., source-link marker [key], rule count [rules...]

and per chunk a type (1 uncompressed, 2 gorilla, 3 chimp) followed by its fields (see
`CHUNK_FIELDS`). Signed integers are stored as their two's-complement `u64`. A `DUMP` payload
wraps it in the RDB type byte and module id, then an RDB version and a CRC64 footer; an RDB file
stores the same bytes after the key name, and ends in a CRC64 of the whole file.

Payloads must be taken with `rdbcompression no`, so that strings are stored raw.
"""

import struct

U64 = 1 << 64

# --- CRC64 (Jones, reflected 0x95AC9329AC4BC9B5, init 0), as the DUMP footer uses ----------

_CRC_TABLE = []
for _i in range(256):
    _c = _i
    for _ in range(8):
        _c = (_c >> 1) ^ 0x95AC9329AC4BC9B5 if _c & 1 else _c >> 1
    _CRC_TABLE.append(_c)


def crc64(data: bytes) -> int:
    crc = 0
    for b in data:
        crc = _CRC_TABLE[(crc ^ b) & 0xFF] ^ (crc >> 8)
    return crc


assert crc64(b"123456789") == 0xE9C6D914C4B8D9CA

# --- RDB length / module opcode codec ---------------------------------------------------------

OP_EOF, OP_SINT, OP_UINT, OP_FLOAT, OP_DOUBLE, OP_STRING = 0, 1, 2, 3, 4, 5


def read_len(buf, pos):
    """Returns (value, is_encoded_string_kind, new_pos)."""
    b = buf[pos]
    kind = b >> 6
    if kind == 0:
        return b & 0x3F, None, pos + 1
    if kind == 1:
        return ((b & 0x3F) << 8) | buf[pos + 1], None, pos + 2
    if b == 0x80:
        return struct.unpack(">I", buf[pos + 1:pos + 5])[0], None, pos + 5
    if b == 0x81:
        return struct.unpack(">Q", buf[pos + 1:pos + 9])[0], None, pos + 9
    assert kind == 3, f"unsupported length byte {b:#x}"
    return None, b & 0x3F, pos + 1


def write_len(n):
    if n < 1 << 6:
        return bytes([n])
    if n < 1 << 14:
        return bytes([0x40 | (n >> 8), n & 0xFF])
    if n < 1 << 32:
        return b"\x80" + struct.pack(">I", n)
    return b"\x81" + struct.pack(">Q", n)


def read_string(buf, pos):
    n, enc, pos = read_len(buf, pos)
    if enc is None:
        return bytes(buf[pos:pos + n]), pos + n
    # Integer-encoded strings (`rdbcompression no` rules out LZF).
    width = {0: 1, 1: 2, 2: 4}[enc]
    value = int.from_bytes(buf[pos:pos + width], "little", signed=True)
    return str(value).encode(), pos + width


class Payload:
    """A DUMP payload as (prefix, [(opcode, value)], version). `prefix` is the RDB type byte and
    the module id; `version` the footer's two RDB-version bytes."""

    def __init__(self, dump: bytes):
        assert dump[0] == 7, "expected RDB_TYPE_MODULE_2"
        _, _, pos = read_len(dump, 1)
        self.prefix = dump[:pos]
        self.version = dump[-10:-8]
        assert struct.unpack("<Q", dump[-8:])[0] == crc64(dump[:-8])
        body = dump[:-10]
        self.tokens = []
        while True:
            op, _, pos = read_len(body, pos)
            if op == OP_EOF:
                break
            if op in (OP_SINT, OP_UINT):
                value, _, pos = read_len(body, pos)
            elif op == OP_DOUBLE:
                value = struct.unpack("<d", body[pos:pos + 8])[0]
                pos += 8
            elif op == OP_STRING:
                value, pos = read_string(body, pos)
            else:
                raise AssertionError(f"unexpected opcode {op}")
            self.tokens.append([op, value])
        assert pos == len(body)

    def encode(self, tokens=None) -> bytes:
        out = bytearray(self.prefix)
        for op, value in tokens if tokens is not None else self.tokens:
            out += write_len(op)
            if op in (OP_SINT, OP_UINT):
                out += write_len(value % U64)
            elif op == OP_DOUBLE:
                out += struct.pack("<d", value)
            else:
                out += write_len(len(value)) + value
        out += write_len(OP_EOF) + self.version
        return bytes(out) + struct.pack("<Q", crc64(bytes(out)))


CHUNK_FIELDS = {
    2: ["max_size", "count", "first_ts", "last_ts", "last_value", "ts_delta", "leading",
        "trailing", "stream", "free_bits"],
    3: ["max_size", "count", "first_ts", "last_ts", "last_value", "last_delta", "last_alpha",
        "stored_lz", "stored_tz", "stored_val", "first", "stream", "free_bits"],
}


class Series:
    """Token positions of a parsed series payload, by field name."""

    def __init__(self, payload: Payload):
        t = payload.tokens
        self.payload = payload
        self.fields = {"id": 0, "label_count": 1}
        i = 2 + 2 * t[1][1]
        self.fields["retention"] = i
        i += 1
        i += 3 if t[i][1] == 0xFE else 1
        self.fields.update(dup_policy=i, max_time_delta=i + 1, max_value_delta=i + 2,
                           encoding=i + 3, chunk_size=i + 4, chunk_count=i + 5)
        i += 6
        self.chunks = []  # (start, end) token spans, type included
        for _ in range(t[self.fields["chunk_count"]][1]):
            start = i
            kind = t[i][1]
            if kind == 1:
                n = t[i + 4][1]
                i += 5 + 2 * n
            else:
                i += 1 + len(CHUNK_FIELDS[kind])
            self.chunks.append((start, i))
        self.tail = i

    def chunk_field(self, chunk, name):
        start, _ = self.chunks[chunk]
        kind = self.payload.tokens[start][1]
        return start + 1 + CHUNK_FIELDS[kind].index(name)

    def with_chunks(self, spans):
        """The payload with its chunks replaced by these token spans, count updated."""
        t = self.payload.tokens
        head = [list(x) for x in t[:self.chunks[0][0] if self.chunks else self.tail]]
        head[self.fields["chunk_count"]][1] = len(spans)
        body = [list(x) for span in spans for x in span]
        return self.payload.encode(head + body + [list(x) for x in t[self.tail:]])

    def span(self, chunk):
        start, end = self.chunks[chunk]
        return self.payload.tokens[start:end]


def empty_chunk(span):
    """The tokens of an empty chunk of the same type as `span`, as the encoders start one."""
    tokens = [list(x) for x in span]
    kind = tokens[0][1]
    if kind == 1:  # type, flag, max_size, max_elements, len
        tokens = tokens[:5]
        tokens[4][1] = 0
        return tokens
    empty = {
        2: {"count": 0, "first_ts": 0, "last_ts": 0, "last_value": 0.0, "ts_delta": 0,
            "leading": 0, "trailing": 0, "stream": b"", "free_bits": 0},
        3: {"count": 0, "first_ts": 0, "last_ts": 0, "last_value": 0.0, "last_delta": 0,
            "last_alpha": -1, "stored_lz": 64, "stored_tz": 0, "stored_val": 0, "first": 1,
            "stream": b"", "free_bits": 0},
    }[kind]
    for name, value in empty.items():
        tokens[1 + CHUNK_FIELDS[kind].index(name)][1] = value
    return tokens


def set_token(payload: Payload, edits) -> bytes:
    tokens = [list(x) for x in payload.tokens]
    for index, value in edits.items():
        tokens[index][1] = value
    return payload.encode(tokens)
