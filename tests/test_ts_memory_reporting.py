# tests/test_ts_memory_reporting.py
"""`MEMORY USAGE`, `TS.INFO memoryUsage` and `INFO ts_memory` must be usable for capacity
planning (MEM-1).

Three separate accounting defects made them not so:

  * `MetricName` implemented `GetSize::get_size`, but a containing type's derived impl calls
    `get_heap_size_with_tracker`, which was left on its default of zero. A series' labels
    therefore cost nothing at all: `TS.INFO memoryUsage` for a seven-label series reported
    exactly `size_of::<TimeSeries>()`.
  * the compressed chunk encodings reported the bit stream's `Vec` *capacity* plus the encoder
    struct as their `size`. The stream grows by doubling, so `TS.INFO DEBUG` reported ~2x the
    bytes actually encoded -- a chunk created with `CHUNK_SIZE 4096` reported 8288 -- and
    `bytesPerSample` with it.
  * the label index hangs off no key at all, so neither `MEMORY USAGE` nor `TS.INFO` could ever
    see it, and nothing else reported it either.

The per-key assertions here are deliberately structural (A costs more than B, X is bounded by
Y) rather than pinned to byte counts, which are a function of struct layout.
"""
import pytest
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase

CHUNK_SIZE = 4096
# A chunk is sealed once its payload reaches CHUNK_SIZE, so it can overshoot by at most the
# encoding of one sample. Generous next to the ~2x the capacity-based figure reported.
CHUNK_SIZE_SLACK = 256

LONG_LABELS = [
    "instance", "host-0000000000.prod.example.internal",
    "job", "node_exporter_with_a_long_job_name",
    "region", "us-east-1-availability-zone-alpha",
    "service", "checkout_api_backend_service",
    "team", "platform_reliability_engineering",
]


class TestMemoryReporting(ValkeyTimeSeriesTestCaseBase):
    @staticmethod
    def _ts_info(client, key: str) -> dict:
        flat = client.execute_command("TS.INFO", key)
        return {flat[i]: flat[i + 1] for i in range(0, len(flat), 2)}

    @staticmethod
    def _chunks(client, key: str) -> list:
        flat = client.execute_command("TS.INFO", key, "DEBUG")
        info = {flat[i]: flat[i + 1] for i in range(0, len(flat), 2)}
        return [{c[i]: c[i + 1] for i in range(0, len(c), 2)} for c in info[b"Chunks"]]

    @staticmethod
    def _ts_memory(client) -> dict:
        """The `ts_memory` fields, as ints. The client parses INFO for us, but only into
        `str -> str`; an absent section comes back as an empty mapping."""
        parsed = client.execute_command("INFO", "ts_memory")
        return {
            k: int(v)
            for k, v in parsed.items()
            if k.startswith("ts_") and str(v).lstrip("-").isdigit()
        }

    def _fill(self, client, key: str, samples: int):
        pipe = client.pipeline(transaction=False)
        for i in range(samples):
            pipe.execute_command("TS.ADD", key, 1600000000000 + i * 1000, i * 1.37)
        pipe.execute()

    # ---- labels -----------------------------------------------------------------

    def test_labels_are_counted_in_memory_usage(self):
        """The headline symptom: a labelled series used to report the same bytes as a bare one."""
        client = self.server.get_new_client()
        client.execute_command("TS.CREATE", "mem:bare")
        client.execute_command("TS.CREATE", "mem:labelled", "LABELS", *LONG_LABELS)

        bare = self._ts_info(client, "mem:bare")[b"memoryUsage"]
        labelled = self._ts_info(client, "mem:labelled")[b"memoryUsage"]

        label_bytes = sum(len(x) for x in LONG_LABELS)
        assert labelled >= bare + label_bytes, (
            f"a series carrying {label_bytes} bytes of label text reported {labelled} against "
            f"{bare} for an identical series with no labels"
        )

    def test_memory_usage_grows_with_label_count(self):
        client = self.server.get_new_client()
        client.execute_command("TS.CREATE", "mem:few", "LABELS", *LONG_LABELS[:2])
        client.execute_command("TS.CREATE", "mem:many", "LABELS", *LONG_LABELS)

        few = client.execute_command("MEMORY", "USAGE", "mem:few")
        many = client.execute_command("MEMORY", "USAGE", "mem:many")
        assert many > few

    def test_shared_labels_are_not_counted_once_per_series(self):
        """Interned pairs are shared, so each holder reports a share rather than the whole thing.

        Charging every holder the full allocation would make the sum of `MEMORY USAGE` over a
        keyspace exceed what the module actually holds, by the sharing factor.
        """
        client = self.server.get_new_client()
        client.execute_command("TS.CREATE", "mem:sole", "LABELS", *LONG_LABELS, "uniq", "sole")
        sole = client.execute_command("MEMORY", "USAGE", "mem:sole")

        for i in range(8):
            client.execute_command(
                "TS.CREATE", f"mem:shared{i}", "LABELS", *LONG_LABELS, "uniq", f"s{i}"
            )
        shared = client.execute_command("MEMORY", "USAGE", "mem:shared0")

        assert shared < sole, (
            f"a series sharing its labels nine ways reported {shared}, no less than the {sole} "
            f"reported when it held them alone"
        )

    # ---- chunks -----------------------------------------------------------------

    @pytest.mark.parametrize("encoding", ["COMPRESSED", "UNCOMPRESSED"])
    def test_chunk_size_is_bounded_by_the_configured_chunk_size(self, encoding):
        """`size` is the encoded payload, not the bit stream's doubled `Vec` capacity."""
        client = self.server.get_new_client()
        key = f"mem:chunks:{encoding}"
        client.execute_command(
            "TS.CREATE", key, "CHUNK_SIZE", CHUNK_SIZE, "ENCODING", encoding
        )
        self._fill(client, key, 20000)

        chunks = self._chunks(client, key)
        assert len(chunks) > 1, "series never filled a chunk, the bound is untested"
        for chunk in chunks:
            assert chunk[b"size"] <= CHUNK_SIZE + CHUNK_SIZE_SLACK, (
                f"{encoding}: a chunk configured at {CHUNK_SIZE} bytes reports "
                f"{chunk[b'size']} -- the reported size is counting allocator slack"
            )

    def test_bytes_per_sample_is_bounded_by_the_uncompressed_size(self):
        """A compressed sample cannot cost more than the 16 raw bytes it replaces."""
        client = self.server.get_new_client()
        client.execute_command("TS.CREATE", "mem:bps", "CHUNK_SIZE", CHUNK_SIZE)
        self._fill(client, "mem:bps", 20000)

        for chunk in self._chunks(client, "mem:bps"):
            if chunk[b"samples"] < 16:
                continue  # header overhead dominates; the estimate is documented as unreliable
            assert 0 < float(chunk[b"bytesPerSample"]) <= 16.0, chunk

    def test_memory_usage_covers_the_encoded_payload(self):
        """The allocation is still reported in full -- `size` shrinking must not shrink this."""
        client = self.server.get_new_client()
        client.execute_command("TS.CREATE", "mem:cover", "CHUNK_SIZE", CHUNK_SIZE)
        self._fill(client, "mem:cover", 20000)

        payload = sum(c[b"size"] for c in self._chunks(client, "mem:cover"))
        reported = self._ts_info(client, "mem:cover")[b"memoryUsage"]
        assert reported >= payload, f"memoryUsage {reported} is below {payload} bytes of payload"

    # ---- the index --------------------------------------------------------------

    def test_info_reports_index_memory(self):
        client = self.server.get_new_client()
        empty = self._ts_memory(client)
        assert empty, "the module publishes no ts_memory INFO section"
        assert empty["ts_index_total_bytes"] == 0

        for i in range(200):
            client.execute_command(
                "TS.CREATE", f"mem:idx{i}", "LABELS", *LONG_LABELS, "uniq", f"series_{i}"
            )

        filled = self._ts_memory(client)
        assert filled["ts_index_series"] == 200
        assert filled["ts_index_terms"] > 0
        assert filled["ts_index_total_bytes"] > 0
        assert filled["ts_index_total_bytes"] == (
            filled["ts_index_terms_bytes"]
            + filled["ts_index_postings_bytes"]
            + filled["ts_index_id_to_key_bytes"]
            + filled["ts_index_bookkeeping_bytes"]
        )

    def test_index_memory_is_released_on_flush(self):
        client = self.server.get_new_client()
        for i in range(200):
            client.execute_command(
                "TS.CREATE", f"mem:drop{i}", "LABELS", *LONG_LABELS, "uniq", f"series_{i}"
            )
        assert self._ts_memory(client)["ts_index_total_bytes"] > 0

        client.execute_command("FLUSHALL")

        after = self._ts_memory(client)
        assert after["ts_index_total_bytes"] == 0
        assert after["ts_index_series"] == 0

    def test_index_memory_is_reported_across_databases(self):
        client = self.server.get_new_client()
        for db in (0, 1):
            client.execute_command("SELECT", db)
            for i in range(50):
                client.execute_command(
                    "TS.CREATE", f"mem:db{db}:{i}", "LABELS", *LONG_LABELS, "uniq", f"s{i}"
                )

        totals = self._ts_memory(client)
        assert totals["ts_index_series"] == 100
        assert totals["ts_index_databases"] >= 2
