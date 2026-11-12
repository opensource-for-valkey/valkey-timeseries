"""Forged `RESTORE` payloads: valid framing, hostile values.

`RESTORE` is the client-reachable entry to the series loader, and its DUMP footer is only an RDB
version and a CRC64 that anyone can recompute. So each case here takes a genuine `DUMP` of a
series, rewrites one or more values inside the module payload, recomputes the footer, and
requires `RESTORE` to refuse it with "Bad data format" while the server stays up. The unmodified
payloads must still restore.

The module value is a sequence of opcode-tagged values (`2` unsigned/signed integer, `4` double,
`5` string) ending in opcode `0`, in this order:

    id, label count [name, value]..., retention, rounding marker [kind, digits],
    duplicate policy, max_time_delta, max_value_delta, encoding, chunk_size_bytes,
    chunk count, chunks..., source-link marker [key], rule count [rules...]

and per chunk a type (1 uncompressed, 2 gorilla, 3 chimp) followed by its fields (see
`CHUNK_FIELDS`). Signed integers are stored as their two's-complement `u64`.
"""

import struct

import pytest
from valkey import ResponseError
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytestframework.conftest import resource_port_tracker

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


def set_token(payload: Payload, edits) -> bytes:
    tokens = [list(x) for x in payload.tokens]
    for index, value in edits.items():
        tokens[index][1] = value
    return payload.encode(tokens)


class TestRestoreHostilePayloads(ValkeyTimeSeriesTestCaseBase):

    def dump(self, key, encoding, chunk_size=4096, samples=20):
        client = self.client
        client.config_set("rdbcompression", "no")
        client.execute_command("TS.CREATE", key, "ENCODING", encoding, "CHUNK_SIZE", chunk_size)
        for i in range(1, samples + 1):
            client.execute_command("TS.ADD", key, i * 1000, i * 1.5)
        payload = Payload(client.dump(key))
        assert payload.encode() == client.dump(key), "codec must round-trip"
        return payload

    def assert_rejected(self, payload: bytes, what: str):
        client = self.client
        key = "hostile:target"
        client.delete(key)
        with pytest.raises(ResponseError, match="Bad data format"):
            client.restore(key, 0, payload)
            raise AssertionError(f"RESTORE accepted {what}")
        assert client.ping(), f"server died on {what}"
        assert client.exists(key) == 0, what

    def assert_restores(self, payload: bytes, source: str):
        client = self.client
        key = "hostile:honest"
        client.delete(key)
        assert client.restore(key, 0, payload) == b"OK"
        assert client.execute_command("TS.RANGE", key, "-", "+") == \
            client.execute_command("TS.RANGE", source, "-", "+")

    def assert_restores_with(self, payload: bytes, **info):
        client = self.client
        key = "hostile:edited"
        client.delete(key)
        assert client.restore(key, 0, payload) == b"OK"
        got = self.ts_info(key)
        for name, value in info.items():
            assert got[name] == value, (name, got[name])

    @pytest.mark.parametrize("encoding", ["GORILLA", "CHIMP"])
    def test_forged_compressed_headers(self, encoding):
        payload = self.dump("src", encoding)
        self.assert_restores(payload.encode(), "src")
        s = Series(payload)
        f = lambda name: s.chunk_field(0, name)  # noqa: E731

        # The field map points where it should: every edit below is a forgery, not a misparse.
        t = payload.tokens
        assert (t[f("count")][1], t[f("first_ts")][1], t[f("last_ts")][1]) == (20, 1000, 20000)
        assert t[f("last_value")][1] == 30.0 and isinstance(t[f("stream")][1], bytes)
        assert t[f("free_bits")][1] < 8
        assert t[s.fields["encoding"]][1] == encoding.lower().encode()
        # And an honest edit through the same map restores and shows up.
        self.assert_restores_with(set_token(payload, {s.fields["chunk_size"]: 8192,
                                                      s.fields["retention"]: 5_000_000}),
                                  chunkSize=8192, retentionTime=5_000_000)

        cases = {
            "count 2^60": {f("count"): 1 << 60},
            "count 5 of 20": {f("count"): 5},
            "count 0 over a full stream": {f("count"): 0},
            "a last sample never written": {f("last_ts"): 99000, f("last_value"): 12345.0},
            "first and last swapped": {f("first_ts"): 20000, f("last_ts"): 1000},
            "negative first timestamp": {f("first_ts"): -101},
            "zeroed stream": {f("stream"): b"\0" * len(payload.tokens[f("stream")][1])},
            "trailing stream bytes": {f("stream"): payload.tokens[f("stream")][1] + b"\xab" * 8},
            "eight free bits": {f("free_bits"): 8},
            "chunk type 258": {s.chunks[0][0]: 256 + {"GORILLA": 2, "CHIMP": 3}[encoding]},
            "chunk_size_bytes 0": {s.fields["chunk_size"]: 0},
            "chunk_size_bytes u64::MAX": {s.fields["chunk_size"]: U64 - 1},
            "retention u64::MAX": {s.fields["retention"]: U64 - 1},
            "16M chunks declared": {s.fields["chunk_count"]: 16 * 1024 * 1024},
        }
        if encoding == "CHIMP":
            cases["chimp stored_val u64::MAX"] = {f("stored_val"): U64 - 1}
            cases["chimp last_delta"] = {f("last_delta"): 999}
        else:
            cases["gorilla window"] = {f("ts_delta"): 999}
        for what, edits in cases.items():
            self.assert_rejected(set_token(payload, edits), f"{encoding}: {what}")
        assert self.server.is_alive()

    @pytest.mark.parametrize("encoding", ["UNCOMPRESSED", "GORILLA", "CHIMP"])
    def test_forged_chunk_layouts(self, encoding):
        payload = self.dump("multi", encoding, chunk_size=64, samples=400)
        s = Series(payload)
        assert len(s.chunks) > 3
        self.assert_restores(payload.encode(), "multi")
        spans = [s.span(i) for i in range(len(s.chunks))]

        self.assert_rejected(s.with_chunks(spans[::-1]), "descending chunks")
        self.assert_rejected(s.with_chunks(spans + spans), "every chunk twice")
        self.assert_rejected(s.with_chunks(spans[:2] + spans[1:]), "one chunk twice")
        # Rebuilding the payload from its own spans is a no-op, so the edits above are the
        # only thing the server rejected.
        self.assert_restores(s.with_chunks(spans), "multi")

    def test_mixed_encodings_are_rejected(self):
        chimp = Series(self.dump("mix:chimp", "CHIMP"))
        plain = self.dump("mix:plain", "UNCOMPRESSED")
        late = Series(plain)
        # Shift the uncompressed samples past the Chimp ones so only the encoding is wrong.
        span = [list(x) for x in late.span(0)]
        for i in range(5, len(span), 2):
            span[i][1] += 100000
        self.assert_rejected(chimp.with_chunks([chimp.span(0), span]),
                             "an uncompressed chunk in a chimp series")

    @pytest.mark.parametrize("encoding", ["GORILLA", "CHIMP"])
    def test_forged_count_before_an_uncompressed_chunk(self, encoding):
        """The payload that aborted the server: a compressed chunk claiming 2^60 samples, then
        an uncompressed chunk at 30000 and 31000. `TS.REVRANGE ... FILTER_BY_TS 31000 5000`
        buffered the uncompressed chunk, then reserved 2^60 samples for the compressed one."""
        head = Series(self.dump("abort:head", encoding))
        plain = Series(self.dump("abort:tail", "UNCOMPRESSED", samples=2))
        tail = [list(x) for x in plain.span(0)]
        tail[5][1], tail[7][1] = 30000, 31000  # timestamps 1000, 2000 -> 30000, 31000
        forged = [list(x) for x in head.span(0)]
        forged[1 + CHUNK_FIELDS[forged[0][1]].index("count")][1] = 1 << 60

        self.assert_restores(head.with_chunks([head.span(0)]), "abort:head")
        self.assert_rejected(head.with_chunks([forged, tail]), f"{encoding}: the abort payload")
        assert self.server.is_alive()

    def test_negative_uncompressed_timestamp(self):
        payload = self.dump("neg", "UNCOMPRESSED")
        s = Series(payload)
        first_ts = s.chunks[0][0] + 5
        self.assert_rejected(set_token(payload, {first_ts: -101}), "timestamp -101")

    def test_oversized_chunk(self):
        """A valid constant Gorilla chunk at step 1 (2 bits a sample), larger than any chunk
        the write path stores: decoding it on every read stalled the server for seconds."""
        payload = self.dump("big", "GORILLA")
        s = Series(payload)
        f = lambda name: s.chunk_field(0, name)  # noqa: E731
        stream = b"\0" * 9 + b"\x01" + b"\0" * (3 * 1024 * 1024)
        count = 2 + (len(stream) - 10) * 4
        edits = {f("stream"): stream, f("count"): count, f("first_ts"): 0,
                 f("last_ts"): count - 1, f("last_value"): 0.0, f("ts_delta"): 1,
                 f("leading"): 0, f("trailing"): 0, f("free_bits"): 0}
        self.assert_rejected(set_token(payload, edits), "a 3 MiB chunk")
