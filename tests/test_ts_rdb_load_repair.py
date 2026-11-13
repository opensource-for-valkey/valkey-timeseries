"""The server reloading an RDB it wrote: write-path rules are repaired or tolerated, not fatal.

A series load error fails the whole RDB load, and the load stops at that key, so everything after
it is lost too. A rule the write path is supposed to keep can still be broken by a writer bug (a
`TS.ADDBULK` timestamp past `i64::MAX` once was), so on the server's own data the loader repairs
what it can and loads the rest as it is, counting both. `RESTORE` keeps refusing the same states.

Each case saves an RDB, rewrites one series' module value inside the file, recomputes the file's
CRC64 and reloads it with `DEBUG RELOAD NOSAVE`.
"""

import os
import struct

import pytest
from valkey import ResponseError
from series_payload import CHUNK_FIELDS, Payload, Series, crc64, empty_chunk, set_token
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytestframework.conftest import resource_port_tracker

# The largest chunk an RDB or RESTORE payload may carry (`MAX_LOADED_CHUNK_BYTES`).
MAX_LOADED_CHUNK_BYTES = 2 * 1024 * 1024


class TestRdbLoadRepair(ValkeyTimeSeriesTestCaseBase):

    def dump(self, key, encoding, chunk_size=4096, samples=20):
        client = self.client
        client.config_set("rdbcompression", "no")
        client.execute_command("TS.CREATE", key, "ENCODING", encoding, "CHUNK_SIZE", chunk_size)
        for i in range(1, samples + 1):
            client.execute_command("TS.ADD", key, i * 1000, i * 1.5)
        return Payload(client.dump(key))

    def counters(self):
        info = self.client.info("ts_rdb")
        return int(info["ts_rdb_load_repairs_total"]), int(info["ts_rdb_load_tolerated_total"])

    def reload_with(self, key, payload: bytes):
        """Saves an RDB, replaces `key`'s module value in it with `payload`'s, and reloads it.
        Other keys go before and after the series, so a load that stopped there would lose
        some of them."""
        client = self.client
        for i in range(10):
            client.set(f"bystander:{i}", i)
            client.execute_command("TS.ADD", f"healthy:{i}", 1000, i)
        keys = client.dbsize()

        original = client.dump(key)
        client.save()
        path = os.path.join(client.config_get("dir")["dir"],
                            client.config_get("dbfilename")["dbfilename"])
        with open(path, "rb") as f:
            rdb = f.read()
        # A DUMP is the RDB type byte, the module value and a 10-byte footer; the file holds
        # the same value after the key name.
        old, new = original[1:-10], payload[1:-10]
        assert rdb.count(old) == 1, "the series' value must appear once in the RDB"
        rdb = rdb.replace(old, new)
        rdb = rdb[:-8] + struct.pack("<Q", crc64(rdb[:-8]))
        with open(path, "wb") as f:
            f.write(rdb)

        assert client.execute_command("DEBUG", "RELOAD", "NOSAVE") == b"OK"
        assert client.dbsize() == keys, "the load lost keys"
        for i in range(10):
            assert client.get(f"bystander:{i}") == str(i).encode()
            assert client.execute_command("TS.GET", f"healthy:{i}") == [1000, str(i).encode()]

    def assert_restore_refuses(self, key):
        """The repaired or tolerated state is still judged strictly by RESTORE, or not at all if
        repairing made it valid."""
        with pytest.raises(ResponseError, match="Bad data format"):
            self.client.restore(f"{key}:copy", 0, self.client.dump(key))

    def chunk_sizes(self, key):
        raw = self.client.execute_command("TS.INFO", key, "DEBUG")
        info = dict(zip(raw[::2], raw[1::2]))
        return [dict(zip(c[::2], c[1::2]))[b"size"] for c in info[b"Chunks"]]

    @pytest.mark.parametrize("encoding", ["UNCOMPRESSED", "CHIMP"])
    def test_negative_timestamp_is_loaded_as_it_is(self, encoding):
        """The state an unchecked TS.ADDBULK timestamp left: refusing it made the server refuse
        its own RDB. Reads never see a negative sample, as before these checks existed.

        The Chimp case forges only the header's first timestamp; its stream still starts at
        1000, since Python has no Chimp encoder. A negative sample inside a compressed stream
        is covered by the trust table in `load_invariant_tests`."""
        payload = self.dump("neg", encoding)
        s = Series(payload)
        first_ts = s.chunks[0][0] + (5 if encoding == "UNCOMPRESSED" else
                                     1 + CHUNK_FIELDS[3].index("first_ts"))
        edited = set_token(payload, {first_ts: -101})

        repairs, tolerated = self.counters()
        self.reload_with("neg", edited)
        assert self.counters() == (repairs, tolerated + 1)
        # The uncompressed edit replaced the first sample; the Chimp one left the stream whole.
        first = 2 if encoding == "UNCOMPRESSED" else 1
        assert self.client.execute_command("TS.RANGE", "neg", "-", "+") == \
            [[i * 1000, str(i * 1.5).encode().replace(b".0", b"")] for i in range(first, 21)]
        assert self.client.execute_command("TS.GET", "neg") == [20000, b"30"]
        self.assert_restore_refuses("neg")

    def test_oversized_chunk_is_split(self):
        """A valid constant Gorilla chunk past the load limit, as a defrag merge can build. It
        loads split by its budget, with every sample."""
        payload = self.dump("big", "GORILLA")
        s = Series(payload)
        f = lambda name: s.chunk_field(0, name)  # noqa: E731
        stream = b"\0" * 9 + b"\x01" + b"\0" * (MAX_LOADED_CHUNK_BYTES + 64 * 1024)
        count = 2 + (len(stream) - 10) * 4
        edited = set_token(payload, {f("stream"): stream, f("count"): count, f("first_ts"): 0,
                                     f("last_ts"): count - 1, f("last_value"): 0.0,
                                     f("ts_delta"): 1, f("leading"): 0, f("trailing"): 0,
                                     f("free_bits"): 0})

        repairs, tolerated = self.counters()
        self.reload_with("big", edited)
        assert self.counters() == (repairs + 1, tolerated)
        info = self.ts_info("big")
        assert info["totalSamples"] == count
        assert info["chunkCount"] > 1
        assert max(self.chunk_sizes("big")) <= 4096 + 4096 // 4
        assert self.client.execute_command("TS.GET", "big") == [count - 1, b"0"]
        assert self.client.execute_command(
            "TS.RANGE", "big", "-", "+", "AGGREGATION", "count", count * 2) == [[0, str(count).encode()]]

        # The repair was saved: the next reload has nothing left to fix, and RESTORE accepts it.
        self.client.execute_command("DEBUG", "RELOAD")
        assert self.counters() == (repairs + 1, tolerated)
        assert self.client.restore("big:copy", 0, self.client.dump("big")) == b"OK"

    def test_oversized_uncompressed_chunk_is_split(self):
        """An uncompressed chunk loads with its capacity raised to its sample count. Its split
        must still go by bytes: a chunk over twice the load limit used to stop at two halves
        each still over it, and the halves kept the raised capacity, so appends could grow the
        last one past its budget again."""
        s = Series(self.dump("wide", "UNCOMPRESSED"))
        span = s.span(0)
        head = [list(x) for x in span[:5]]  # type, flag, max_size, max_elements, len
        ts_op, value_op = span[5][0], span[6][0]
        count = 2 * MAX_LOADED_CHUNK_BYTES // 16 + 1024
        head[4][1] = count
        samples = [[op, v] for i in range(1, count + 1)
                   for op, v in ((ts_op, i), (value_op, float(i % 7)))]
        edited = s.with_chunks([head + samples])

        repairs, tolerated = self.counters()
        self.reload_with("wide", edited)
        assert self.counters() == (repairs + 1, tolerated)
        assert self.ts_info("wide")["totalSamples"] == count
        assert max(self.chunk_sizes("wide")) <= 4096 + 4096 // 4

        # More than a chunk's worth of appends after the last sample stays within the budget.
        for i in range(1, 600):
            self.client.execute_command("TS.ADD", "wide", count + i, 1)
        assert max(self.chunk_sizes("wide")) <= 4096 + 4096 // 4

        self.client.execute_command("DEBUG", "RELOAD")
        assert self.counters() == (repairs + 1, tolerated)
        assert self.client.restore("wide:copy", 0, self.client.dump("wide")) == b"OK"

    def test_chunk_of_another_encoding_is_re_encoded(self):
        """A Chimp series whose last chunk is uncompressed: the layout a forged count turned into
        an abort. RESTORE refuses it; the server's own copy loads re-encoded."""
        chimp = Series(self.dump("mix", "CHIMP"))
        plain = Series(self.dump("mix:plain", "UNCOMPRESSED"))
        span = [list(x) for x in plain.span(0)]
        for i in range(5, len(span), 2):
            span[i][1] += 100000  # after the Chimp samples
        edited = chimp.with_chunks([chimp.span(0), span])
        with pytest.raises(ResponseError, match="Bad data format"):
            self.client.restore("mix:refused", 0, edited)

        expected = self.client.execute_command("TS.RANGE", "mix", "-", "+") + [
            [ts + 100000, value]
            for ts, value in self.client.execute_command("TS.RANGE", "mix:plain", "-", "+")]
        repairs, tolerated = self.counters()
        self.reload_with("mix", edited)
        assert self.counters() == (repairs + 1, tolerated)
        assert self.client.execute_command("TS.RANGE", "mix", "-", "+") == expected
        assert self.client.restore("mix:copy", 0, self.client.dump("mix")) == b"OK"

    def test_interior_empty_chunks_are_dropped(self):
        payload = self.dump("empties", "UNCOMPRESSED", chunk_size=48, samples=60)
        s = Series(payload)
        assert len(s.chunks) > 16
        spans = [s.span(i) for i in range(len(s.chunks))]
        empty = empty_chunk(spans[0])
        edited = s.with_chunks(spans[:10] + [empty] + spans[10:] + [empty])

        repairs, tolerated = self.counters()
        self.reload_with("empties", edited)
        assert self.counters() == (repairs + 1, tolerated)
        assert self.ts_info("empties")["chunkCount"] == len(spans)
        for i in range(1, 61):
            assert self.client.execute_command("TS.DEL", "empties", i * 1000, i * 1000) == 1
