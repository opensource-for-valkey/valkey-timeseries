"""Whatever a write command accepts must load back unchanged, through RESTORE and a reload.

The loader refuses a `RESTORE` payload that breaks a write-path rule, and repairs the server's
own RDB, so a rule tighter than some command shows up here as a refused copy or a changed digest:
`TS.ADDBULK` once stored a timestamp past `i64::MAX` as a negative one, which the loader then
refused. Each case checks `DUMP` -> `RESTORE` and `DEBUG RELOAD`, comparing `DEBUG DIGEST-VALUE`
and the samples.
"""

import json
import random

import pytest
from valkey import ResponseError
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytestframework.conftest import resource_port_tracker

I64_MAX = 2**63 - 1
ENCODINGS = ["COMPRESSED", "GORILLA", "UNCOMPRESSED"]


class TestWriteLoadRoundtrip(ValkeyTimeSeriesTestCaseBase):

    def snapshot(self, key):
        client = self.client
        return (client.execute_command("DEBUG", "DIGEST-VALUE", key),
                client.execute_command("TS.RANGE", key, "-", "+"))

    def assert_round_trips(self, keys):
        """Every key restores from its own DUMP and survives a reload, unchanged."""
        client = self.client
        before = {key: self.snapshot(key) for key in keys}
        for key in keys:
            copy = f"{key}:copy"
            assert client.restore(copy, 0, client.dump(key)) == b"OK", key
            digest, samples = self.snapshot(copy)
            assert digest == before[key][0] and samples == before[key][1], key
            client.delete(copy)
        assert client.execute_command("DEBUG", "RELOAD") == b"OK"
        for key in keys:
            assert self.snapshot(key) == before[key], f"{key} changed across a reload"

    def max_retention(self):
        """The largest RETENTION `TS.CREATE` accepts, found by bisection so that a parser
        change moves the test with it."""
        client = self.client
        lo, hi = 0, I64_MAX
        while lo < hi:
            mid = (lo + hi + 1) // 2
            try:
                client.execute_command("TS.CREATE", "probe:retention", "RETENTION", mid)
                client.delete("probe:retention")
                lo = mid
            except Exception:
                hi = mid - 1
        assert lo > 10 * 365 * 86_400_000, "expected at least ten years"
        return lo

    @pytest.mark.parametrize("chunk_size", [48, 1048576])
    @pytest.mark.parametrize("encoding", ENCODINGS)
    def test_write_command_extremes(self, encoding, chunk_size):
        client = self.client
        keys = []

        def create(name, *options):
            key = f"{name}:{encoding}:{chunk_size}"
            client.execute_command("TS.CREATE", key, "ENCODING", encoding, "CHUNK_SIZE",
                                   chunk_size, "DUPLICATE_POLICY", "LAST", *options)
            keys.append(key)
            return key

        key = create("add")
        for timestamp, value in ((0, "1.7976931348623157e308"), (1, "5e-324"), (2, "-0"),
                                 (I64_MAX - 1, "-inf"), (I64_MAX, "nan")):
            assert client.execute_command("TS.ADD", key, timestamp, value) == timestamp
        # Into the past, onto both ends.
        client.execute_command("TS.ADD", key, 0, "inf")
        client.execute_command("TS.ADD", key, I64_MAX, 7)

        key = create("madd")
        assert client.execute_command("TS.MADD", key, 0, 1, key, I64_MAX, 2) == [0, I64_MAX]
        assert client.execute_command("TS.MADD", key, 1, 3, key, I64_MAX - 1, 4) == \
            [1, I64_MAX - 1]

        for command, name in (("TS.INCRBY", "incrby"), ("TS.DECRBY", "decrby")):
            key = create(name)
            client.execute_command(command, key, 1.5, "TIMESTAMP", 0)
            client.execute_command(command, key, "1e300", "TIMESTAMP", I64_MAX)
            client.execute_command(command, key, 1, "TIMESTAMP", I64_MAX)

        key = create("addbulk")
        payload = json.dumps({"timestamps": [I64_MAX, 0, I64_MAX - 1, 1],
                              "values": [1.0, 2.0, 3.0, 4.0]})
        assert client.execute_command("TS.ADDBULK", key, payload) == [4, 4]

        # The longest retention: the floor sits 2^61 ms below the last sample, so the sample
        # at 0 is expired, and stays expired across the reload.
        key = create("retention", "RETENTION", self.max_retention())
        client.execute_command("TS.ADD", key, 0, 1)
        client.execute_command("TS.ADD", key, I64_MAX, 2)

        self.assert_round_trips(keys)

    def test_random_command_sequences_round_trip(self):
        """A fixed-seed run of random command sequences with reloads in between: every
        intermediate and final state restores from its own DUMP and survives a reload."""
        client = self.client
        rnd = random.Random(20261112)

        def write(*args):
            # A command may refuse its input (a backfill behind the retention window): that is
            # the command's business, and the state it leaves is what gets round-tripped.
            try:
                client.execute_command(*args)
            except ResponseError:
                pass

        for n in range(30):
            encoding = rnd.choice(ENCODINGS)
            key = f"seq:{n}"
            options = ["ENCODING", encoding, "CHUNK_SIZE", rnd.choice([48, 128, 512, 4096]),
                       "DUPLICATE_POLICY", "LAST"]
            if rnd.random() < 0.3:
                options += ["RETENTION", rnd.choice([500, 5_000, 50_000])]
            client.execute_command("TS.CREATE", key, *options)
            last = rnd.randrange(0, 5_000)
            for _ in range(rnd.randrange(10, 50)):
                r = rnd.random()
                if r < 0.4:
                    for _ in range(rnd.randrange(1, 40)):
                        last += rnd.choice([1, 1, 10, 1000, rnd.randrange(1, 100_000)])
                        value = rnd.choice([1.5, rnd.uniform(-1e9, 1e9),
                                            round(rnd.uniform(0, 1000), 2)])
                        write("TS.ADD", key, last, repr(value))
                elif r < 0.5:
                    write("TS.ADD", key, max(0, last - rnd.randrange(20_000)),
                                           rnd.randrange(100))
                elif r < 0.62:
                    base = max(0, last - rnd.randrange(30_000))
                    stamps = sorted({base + rnd.randrange(40_000) for _ in range(rnd.randrange(1, 400))})
                    write("TS.ADDBULK", key, json.dumps(
                        {"timestamps": stamps, "values": [rnd.uniform(-1e6, 1e6) for _ in stamps]}))
                    last = max(last, stamps[-1])
                elif r < 0.7:
                    args = []
                    for _ in range(rnd.randrange(1, 30)):
                        timestamp = max(0, last - rnd.randrange(-500, 10_000))
                        args += [key, timestamp, rnd.randrange(1000)]
                        last = max(last, timestamp)
                    write("TS.MADD", *args)
                elif r < 0.8:
                    start = max(0, last - rnd.randrange(30_000))
                    write("TS.DEL", key, start, start + rnd.randrange(5_000))
                elif r < 0.86:
                    write("TS.ALTER", key, "CHUNK_SIZE",
                                           rnd.choice([48, 96, 256, 1024]))
                elif r < 0.9:
                    write("TS.INCRBY", key, 1.25, "TIMESTAMP", last)
                else:
                    self.assert_round_trips([key])
            self.assert_round_trips([key])
            client.delete(key)
