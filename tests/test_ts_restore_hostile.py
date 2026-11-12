"""Forged `RESTORE` payloads: valid framing, hostile values.

`RESTORE` is the client-reachable entry to the series loader, and its DUMP footer is only an RDB
version and a CRC64 that anyone can recompute. So each case here takes a genuine `DUMP` of a
series, rewrites one or more values inside the module payload, recomputes the footer, and
requires `RESTORE` to refuse it with "Bad data format" while the server stays up. The unmodified
payloads must still restore.

The payload codec is in `series_payload.py`.
"""

import pytest
from valkey import ResponseError
from series_payload import CHUNK_FIELDS, U64, Payload, Series, empty_chunk, set_token
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


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

    @pytest.mark.parametrize("encoding", ["UNCOMPRESSED", "GORILLA", "CHIMP"])
    def test_empty_chunks_are_dropped(self, encoding):
        """An empty chunk's bounds read as zero. Interior or trailing ones broke the ordering the
        chunk search assumes past 16 chunks, so deletes and bounded reads of existing samples
        found nothing while a full scan still returned them. They load, without the empties."""
        payload = self.dump("empties", encoding, chunk_size=64, samples=1500)
        s = Series(payload)
        assert len(s.chunks) > 16, "needs the binary-search path"
        spans = [s.span(i) for i in range(len(s.chunks))]
        empty = empty_chunk(spans[0])
        middle = len(spans) // 2
        edited = s.with_chunks([empty] + spans[:middle] + [empty] + spans[middle:] + [empty])

        client = self.client
        key = "hostile:empties"
        client.delete(key)
        repairs = int(client.info("ts_rdb")["ts_rdb_load_repairs_total"])
        assert client.restore(key, 0, edited) == b"OK"
        assert int(client.info("ts_rdb")["ts_rdb_load_repairs_total"]) == repairs + 1
        assert client.execute_command("TS.RANGE", key, "-", "+") == \
            client.execute_command("TS.RANGE", "empties", "-", "+")
        client.execute_command("COPY", key, f"{key}:reads")
        for timestamp, _ in client.execute_command("TS.RANGE", "empties", "-", "+"):
            assert len(client.execute_command("TS.RANGE", f"{key}:reads", timestamp,
                                              timestamp)) == 1, f"read at {timestamp}"
            assert client.execute_command("TS.DEL", key, timestamp, timestamp) == 1, \
                f"delete at {timestamp}"
        assert self.ts_info(f"{key}:reads")["chunkCount"] == len(spans)

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
