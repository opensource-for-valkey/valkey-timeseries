import pytest

from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTimeSeriesIngest(ValkeyTimeSeriesTestCaseBase):
    def get_sample(self, key, timestamp):
        res = self.client.execute_command("TS.RANGE", key, '-', '+', "FILTER_BY_TS", timestamp)
        assert len(res) == 1, f"Expected one sample at timestamp {timestamp} in series {key}, got: {res}"
        return res[0]

    def test_basic_returns_success_and_total(self):
        key = "series_addbulk_basic"
        payload = r'{"values":[1,2,3],"timestamps":[1000,2000,3000]}'

        res = self.client.execute_command("TS.ADDBULK", key, payload)
        assert res == [3, 3], f"Expected ingestion to succeed with total 3, got: {res}"

        # Verify the samples exist and are retrievable.
        assert self.get_sample(key, 1000) == [1000, b"1"]
        assert self.get_sample(key, 2000) == [2000, b"2"]
        assert self.get_sample(key, 3000) == [3000, b"3"]

    def test_sorts_samples_by_timestamp(self):
        client = self.client
        key = "series_addbulk_basic"
        payload = r'{"values":[3,1,2],"timestamps":[3000,1000,2000]}'

        res = client.execute_command("TS.ADDBULK", key, payload)
        assert res == [3, 3]

        assert self.get_sample(key, 1000) == [1000, b"1"]
        assert self.get_sample(key, 2000) == [2000, b"2"]
        assert self.get_sample(key, 3000) == [3000, b"3"]

    def test_rejects_mismatched_array_lengths(self):
        payload = r'{"values":[1,2],"timestamps":[1000]}'

        with pytest.raises(Exception) as excinfo:
            self.client.execute_command("TS.ADDBULK", "series_addbulk_bad_lengths", payload)

        assert "length mismatch" in str(excinfo.value).lower()

    def test_requires_values(self):
        payload = r'{"timestamps":[1000,2000]}'

        with pytest.raises(Exception) as excinfo:
            self.client.execute_command("TS.ADDBULK", "series_addbulk_missing_values", payload)

        assert "missing values" in str(excinfo.value).lower()

    def test_ingest_requires_timestamps(self):
        client = self.server.get_new_client()
        payload = r'{"values":[1,2]}'

        with pytest.raises(Exception) as excinfo:
            client.execute_command("TS.ADDBULK", "series_addbulk_missing_timestamps", payload)

        assert "missing timestamps" in str(excinfo.value).lower()

    def test_ingest_rejects_empty_arrays(self):
        with pytest.raises(Exception) as excinfo1:
            self.client.execute_command("TS.ADDBULK", "series_addbulk_empty_1", r'{"values":[],"timestamps":[1]}')
        assert "no timestamps or values" in str(excinfo1.value).lower()

        with pytest.raises(Exception) as excinfo2:
            self.client.execute_command("TS.ADDBULK", "series_addbulk_empty_2", r'{"values":[1],"timestamps":[]}')
        assert "no timestamps or values" in str(excinfo2.value).lower()

    def test_invalid_json_returns_error(self):
        payload = r'{"values":[1,2],"timestamps":[1000,2000]'  # missing closing brace

        with pytest.raises(Exception):
            self.client.execute_command("TS.ADDBULK", "series_addbulk_invalid_json", payload)

    def test_non_numeric_entries_raise_error(self):
        # Non-numeric value "x" will be dropped, causing values.len != timestamps.len -> error.
        payload = r'{"values":[1,"x",3],"timestamps":[1000,2000,3000]}'

        with pytest.raises(Exception) as excinfo:
            self.client.execute_command("TS.ADDBULK", "series_addbulk_non_numeric", payload)

        assert "invalid value" in str(excinfo.value).lower() or "length mismatch" in str(excinfo.value).lower()

    def test_runs_compactions_and_writes_destination_series(self):
        src = "series_addbulk_compact_src"
        dest = "series_addbulk_compact_dest"

        # Create source and destination series, then attach a compaction rule (bucket=10).
        self.client.execute_command("TS.CREATE", src)
        self.client.execute_command("TS.CREATE", dest)
        self.client.execute_command("TS.CREATERULE", src, dest, "AGGREGATION", "SUM", 10)

        # Ingest 3 samples that fall into 2 buckets: [0..9] and [10..19].
        # Expected SUMs: bucket 0 -> 1+2=3, bucket 10 -> 3
        payload = r'{"values":[1,2,3],"timestamps":[1,2,11]}'
        res = self.client.execute_command("TS.ADDBULK", src, payload)
        assert res == [3, 3]

        # The destination receives only closed buckets: bucket 0 closed when ts=11 arrived,
        # while bucket 10 is still open and is served via the LATEST flag (same as TS.ADD).
        assert self.get_sample(dest, 0) == [0, b"3"]
        assert self.client.execute_command("TS.RANGE", dest, "-", "+") == [[0, b"3"]]
        latest = self.client.execute_command("TS.GET", dest, "LATEST")
        assert latest == [10, b"3"]

    def test_addbulk_after_add_does_not_double_count_open_bucket(self):
        src = "series_addbulk_mixed_src"
        dest = "series_addbulk_mixed_dest"

        self.client.execute_command("TS.CREATE", src)
        self.client.execute_command("TS.CREATE", dest)
        self.client.execute_command("TS.CREATERULE", src, dest, "AGGREGATION", "SUM", 10)

        # Seed the open bucket [0..9] via single-sample adds so the rule carries live
        # open-bucket aggregator state into the bulk insert.
        self.client.execute_command("TS.ADD", src, 1, 5)
        self.client.execute_command("TS.ADD", src, 2, 5)

        # Bulk-append into the same open bucket, then into the next bucket to close it.
        payload = r'{"values":[5,7],"timestamps":[3,11]}'
        assert self.client.execute_command("TS.ADDBULK", src, payload) == [2, 2]

        # Bucket 0 closed with 5+5+5=15; double-counting the seeded samples would yield 25.
        assert self.get_sample(dest, 0) == [0, b"15"]
        assert self.client.execute_command("TS.GET", dest, "LATEST") == [10, b"7"]

        # An out-of-order bulk insert into the already-closed bucket must trigger a
        # recalculation of that bucket in the destination.
        payload = r'{"values":[100],"timestamps":[4]}'
        assert self.client.execute_command("TS.ADDBULK", src, payload) == [1, 1]
        assert self.get_sample(dest, 0) == [0, b"115"]

    def test_runs_nested_compactions(self):
        src = "series_addbulk_nested_compact_src"
        mid = "series_addbulk_nested_compact_mid"
        dest = "series_addbulk_nested_compact_dest"

        # Create series and chain compaction rules:
        # src --SUM(10)--> mid --SUM(20)--> dest
        self.client.execute_command("TS.CREATE", src)
        self.client.execute_command("TS.CREATE", mid)
        self.client.execute_command("TS.CREATE", dest)

        self.client.execute_command("TS.CREATERULE", src, mid, "AGGREGATION", "SUM", 10)
        self.client.execute_command("TS.CREATERULE", mid, dest, "AGGREGATION", "SUM", 20)

        # Ingest samples that span two 10-sized buckets:
        # bucket 0: ts=1,2 => 1+2=3
        # bucket 10: ts=11 => 3
        payload = r'{"values":[1,2,3],"timestamps":[1,2,11]}'
        res = self.client.execute_command("TS.ADDBULK", src, payload)
        assert res == [3, 3]

        # First-level compaction results in mid: bucket 0 closed when ts=11 arrived;
        # bucket 10 is still open and only visible via LATEST.
        assert self.get_sample(mid, 0) == [0, b"3"]
        assert self.client.execute_command("TS.GET", mid, "LATEST") == [10, b"3"]

        # Second-level compaction (20-sized bucket over mid's own stored samples): bucket 0
        # spans [0..19] and is still open, so dest holds no closed buckets yet. Its LATEST
        # reflects only mid's own *closed* contribution so far (mid(0)=3); mid's bucket 10
        # (value 3) is itself still open and hasn't been committed to mid, so it isn't
        # visible to dest yet. This is not 1+2+3=6 (the raw src total) — a mid->dest rule
        # aggregates mid's stored samples, not src's raw stream.
        assert self.client.execute_command("TS.RANGE", dest, "-", "+") == []
        assert self.client.execute_command("TS.GET", dest, "LATEST") == [0, b"3"]

    def test_large_batch_runs_multi_level_compactions(self):
        src = "series_addbulk_large_compact_src"
        l1 = "series_addbulk_large_compact_l1"
        l2 = "series_addbulk_large_compact_l2"
        l3 = "series_addbulk_large_compact_l3"

        # Create the series and a 3-level compaction chain:
        # src --SUM(10)--> l1 --SUM(50)--> l2 --SUM(100)--> l3
        self.client.execute_command("TS.CREATE", src)
        self.client.execute_command("TS.CREATE", l1)
        self.client.execute_command("TS.CREATE", l2)
        self.client.execute_command("TS.CREATE", l3)

        self.client.execute_command("TS.CREATERULE", src, l1, "AGGREGATION", "SUM", 10)
        self.client.execute_command("TS.CREATERULE", l1, l2, "AGGREGATION", "SUM", 50)
        self.client.execute_command("TS.CREATERULE", l2, l3, "AGGREGATION", "SUM", 100)

        # Large ingestion: timestamps 0..999, all values are 1.
        # This creates predictable sums per bucket at each level.
        n = 1000
        timestamps = list(range(n))
        values = [1] * n

        payload = f'{{"values":{values},"timestamps":{timestamps}}}'
        res = self.client.execute_command("TS.ADDBULK", src, payload)
        assert res == [n, n]

        # The final bucket at each level is still open (source last ts is 999) and is
        # served via LATEST instead of being written to the destination.

        # Level 1: bucket=10, each closed bucket sum is 10; bucket 990 is open.
        assert self.get_sample(l1, 0) == [0, b"10"]
        assert self.get_sample(l1, 900) == [900, b"10"]
        assert self.get_sample(l1, 980) == [980, b"10"]
        assert self.client.execute_command("TS.GET", l1, "LATEST") == [990, b"10"]

        # Level 2: bucket=50, each closed bucket sums to 50; bucket 950 is open.
        # l2's rule aggregates l1's own stored samples: of l1's five 10-sized sub-buckets
        # in [950,1000) (950,960,970,980,990), only 950/960/970/980 have closed and been
        # committed to l1 (990 is l1's own open bucket); l2's open-bucket LATEST reflects
        # just those four closed contributions (4*10=40), not the eventual full 50.
        assert self.get_sample(l2, 0) == [0, b"50"]
        assert self.get_sample(l2, 900) == [900, b"50"]
        assert self.client.execute_command("TS.GET", l2, "LATEST") == [950, b"40"]

        # Level 3: bucket=100, each closed bucket sums to 100; bucket 900 is open.
        # l3's rule aggregates l2's own stored samples: of l2's two 50-sized sub-buckets
        # in [900,1000) (900,950), only 900 has closed and been committed to l2 (950 is
        # l2's own open bucket, see above); l3's open-bucket LATEST reflects just that one
        # closed contribution (50), not the eventual full 100.
        assert self.get_sample(l3, 0) == [0, b"100"]
        assert self.get_sample(l3, 800) == [800, b"100"]
        assert self.client.execute_command("TS.GET", l3, "LATEST") == [900, b"50"]

    def test_creation_options_are_applied(self):
        """Options after the payload used to be read from one argument too far along, so every
        option but LABELS was rejected."""
        payload = '{"values":[1,2],"timestamps":[1000,2000]}'
        assert self.client.execute_command(
            "TS.ADDBULK", "bulk:opts", payload,
            "RETENTION", 100, "DUPLICATE_POLICY", "LAST", "LABELS", "a", "b",
        ) == [2, 2]
        info = self.ts_info("bulk:opts")
        assert info["retentionTime"] == 100
        assert info["duplicatePolicy"] == "last"
        assert info["labels"] == {"a": "b"}

    def test_on_duplicate_overrides_the_series_policy(self):
        self.client.execute_command("TS.CREATE", "bulk:ondup", "DUPLICATE_POLICY", "BLOCK")
        self.client.execute_command("TS.ADD", "bulk:ondup", 1000, 5)
        assert self.client.execute_command(
            "TS.ADDBULK", "bulk:ondup", '{"values":[3],"timestamps":[1000]}', "ON_DUPLICATE", "SUM",
        ) == [1, 1]
        assert float(self.client.execute_command("TS.GET", "bulk:ondup")[1]) == 8.0
