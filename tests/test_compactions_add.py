import time
from typing import List, Tuple, Optional

import pytest
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util.waiters import *

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestCompactionAdd(ValkeyTimeSeriesTestCaseBase):
    """Test compaction behavior when adding samples to TimeSeries"""

    def create_source_and_dest_series(
        self, source_key: str, dest_key: str, retention_ms: Optional[int] = None
    ) -> None:
        """Helper to create source and destination time series"""
        # Create the source series
        if retention_ms:
            self.client.execute_command(
                "TS.CREATE", source_key, "RETENTION", retention_ms
            )
        else:
            self.client.execute_command("TS.CREATE", source_key)

        # Create destination series for compaction
        self.client.execute_command("TS.CREATE", dest_key)

    def add_compaction_rule(
        self,
        source_key: str,
        dest_key: str,
        aggregation: str,
        bucket_duration_ms: int,
        align_timestamp: int = 0,
    ) -> None:
        """Helper to add compaction rule between series"""
        self.client.execute_command(
            "TS.CREATERULE",
            source_key,
            dest_key,
            "AGGREGATION",
            aggregation,
            bucket_duration_ms,
            align_timestamp,
        )

    def add_sample(self, key: str, timestamp: int, value: float) -> None:
        """Helper to add a sample to a series"""
        self.client.execute_command("TS.ADD", key, timestamp, value)

    def get_samples(
        self, key: str, start_ts: int = 0, end_ts: int = None
    ) -> List[Tuple[int, float]]:
        """Helper to get all samples from a series"""
        if end_ts is None:
            end_ts = int(time.time() * 1000)
        return self.client.execute_command("TS.RANGE", key, start_ts, end_ts)

    def test_basic_compaction_on_sample_add(self):
        """Test that compaction occurs when adding samples to source series"""
        source_key = "test:source:basic"
        dest_key = "test:dest:basic"

        self.create_source_and_dest_series(source_key, dest_key)
        self.add_compaction_rule(
            source_key, dest_key, "avg", 10000
        )  # 10-second buckets

        # Add samples spanning multiple buckets
        base_ts = 1000
        samples = [
            (base_ts, 10.0),
            (base_ts + 5000, 20.0),  # Same bucket
            (base_ts + 12000, 30.0),  # New bucket
            (base_ts + 18000, 40.0),  # Same bucket as previous
        ]

        for ts, value in samples:
            self.add_sample(source_key, ts, value)

        # Verify the source series has all samples
        source_samples = self.client.execute_command("TS.RANGE", source_key, 0, "+")
        assert len(source_samples) == 4

        # Verify compaction created aggregated samples
        dest_samples = self.client.execute_command("TS.RANGE", dest_key, 0, "+")
        assert len(dest_samples) >= 1  # At least one completed bucket

        # Verify first bucket average: (10 + 20) / 2 = 15
        first_bucket = dest_samples[0]
        assert first_bucket[1] == b"15"

    def test_compaction_with_different_aggregations(self):
        """Test compaction with various aggregation types"""
        aggregations = ["avg", "sum", "min", "max", "count"]
        base_ts = 100000  # Fixed the base timestamp for consistency in tests

        for agg in aggregations:
            source_key = f"test:source:{agg}"
            dest_key = f"test:dest:{agg}"

            self.create_source_and_dest_series(source_key, dest_key)
            self.add_compaction_rule(source_key, dest_key, agg, 10000)

            # Add samples that will complete at least one bucket
            samples = [(base_ts, 10.0), (base_ts + 5000, 20.0), (base_ts + 15000, 30.0)]

            for ts, value in samples:
                self.client.execute_command("TS.ADD", source_key, ts, value)

            # Verify compaction occurred
            dest_samples = self.client.execute_command("TS.RANGE", dest_key, "-", "+")
            assert len(dest_samples) >= 1

            # Verify aggregation results
            if agg == "avg":
                assert dest_samples[0][1] == b"15"  # (10 + 20) / 2
            elif agg == "sum":
                assert dest_samples[0][1] == b"30"  # 10 + 20
            elif agg == "min":
                assert dest_samples[0][1] == b"10"
            elif agg == "max":
                assert dest_samples[0][1] == b"20"
            elif agg == "count":
                assert dest_samples[0][1] == b"2"

    def test_compaction_with_align_timestamp(self):
        """Test that compaction respects align timestamp parameter"""
        source_key = "test:source:aligned"
        dest_key = "test:dest:aligned"

        self.create_source_and_dest_series(source_key, dest_key)

        # Align to 5-second offset within 10-second buckets
        align_ts = 5000
        self.add_compaction_rule(source_key, dest_key, "avg", 10000, align_ts)

        base_ts = int(time.time() * 1000)
        # Round down to the nearest 10 seconds, then add align offset
        aligned_base = (base_ts // 10000) * 10000 + align_ts

        samples = [
            (aligned_base, 10.0),
            (aligned_base + 3000, 20.0),  # Same aligned bucket
            (aligned_base + 12000, 30.0),  # Next aligned bucket
        ]

        for ts, value in samples:
            self.client.execute_command("TS.ADD", source_key, ts, value)

        dest_samples = self.client.execute_command("TS.RANGE", dest_key, 0, "+")
        assert len(dest_samples) >= 1

        # Verify the first bucket is aligned correctly
        first_bucket_ts = dest_samples[0][0]
        assert (first_bucket_ts - align_ts) % 10000 == 0


    def test_compaction_maintains_order_with_out_of_order_samples(self):
        """Test compaction handles out-of-order sample insertion correctly"""
        source_key = "test:source:ooo"
        dest_key = "test:dest:ooo"

        self.create_source_and_dest_series(source_key, dest_key)
        self.add_compaction_rule(source_key, dest_key, "avg", 10000, 1000)

        # Use a fixed base timestamp aligned to bucket boundary (align=1000, bucket=10000)
        # Bucket covers [1000, 11000), so all samples at 1000..8000 are in the same bucket.
        base_ts = 1000

        # Add samples out of order — all within the same bucket [1000, 11000)
        samples = [
            (base_ts + 7000, 30.0),  # Future sample first
            (base_ts, 10.0),  # Past sample
            (base_ts + 4000, 20.0),  # Middle sample
            (base_ts + 2000, 15.0),  # Another past sample (upsert scenario)
        ]

        for ts, value in samples:
            self.client.execute_command("TS.ADD", source_key, ts, value)

        # Add a sample in the next bucket to finalize the first one
        self.client.execute_command("TS.ADD", source_key, base_ts + 10000, 0.0)

        # Verify source maintains all samples in correct order
        source_samples = self.client.execute_command("TS.RANGE", source_key, "-", "+")
        assert len(source_samples) == 5

        # Verify compaction handled the out-of-order inserts correctly
        dest_samples = self.client.execute_command("TS.RANGE", dest_key, "-", "+")
        assert len(dest_samples) >= 1

        # (10 + 15 + 20 + 30) / 4 = 18.75
        assert dest_samples[0][1] == b"18.75"

    def test_compaction_bucket_finalization(self):
        """Test that compaction properly finalizes buckets when new buckets start"""
        source_key = "test:source:finalize"
        dest_key = "test:dest:finalize"

        self.create_source_and_dest_series(source_key, dest_key)
        self.add_compaction_rule(source_key, dest_key, "sum", 5000)  # 5 second buckets

        base_ts = 100000

        # Add samples to the first bucket
        self.client.execute_command("TS.ADD", source_key, base_ts, 10.0)
        self.client.execute_command("TS.ADD", source_key, base_ts + 2000, 20.0)

        # Verify no compaction yet (bucket not finalized)
        dest_samples = self.client.execute_command("TS.RANGE", dest_key, "-", "+")
        assert len(dest_samples) == 0

        # Add sample to new bucket - should finalize the previous bucket
        self.client.execute_command("TS.ADD", source_key, base_ts + 7000, 30.0)

        # Now the first bucket should be finalized and compacted
        dest_samples = self.client.execute_command("TS.RANGE", dest_key, "-", "+")
        assert len(dest_samples) == 1
        assert dest_samples[0][1] == b"30"  # sum: 10 + 20

    def test_compaction_with_sample_upsert(self):
        """Test compaction handles sample upserts (timestamp <= last_timestamp) correctly"""
        source_key = "test:source:upsert"
        dest_key = "test:dest:upsert"

        self.create_source_and_dest_series(source_key, dest_key)
        self.add_compaction_rule(source_key, dest_key, "avg", 10000, 1000)

        base_ts = 1000

        # Add initial samples
        self.add_sample(source_key, base_ts, 10.0)
        self.add_sample(source_key, base_ts + 5000, 20.0)
        self.add_sample(source_key, base_ts + 15000, 30.0)  # Finalizes first bucket

        # Verify initial compaction
        dest_samples = self.client.execute_command("TS.RANGE", dest_key, "-", "+")
        initial_count = len(dest_samples)

        # Upsert an earlier sample (should recalculate bucket)
        self.add_sample(source_key, base_ts + 2000, 40.0)

        # Verify compaction was recalculated
        dest_samples_after = self.client.execute_command("TS.RANGE", dest_key, "-", "+")
        assert len(dest_samples_after) >= initial_count

        # Value should have changed due to upsert. The recomputed bucket
        # [1000, 11000) holds (10 + 40 + 20) / 3; the sample at 16000 belongs
        # to the next bucket and must NOT be folded into the recompute
        # (reference-verified: RedisTimeSeries returns the same value).
        new_value = float(dest_samples_after[0][1])
        assert new_value == pytest.approx(70 / 3)

    def test_compaction_across_multiple_destination_series(self):
        """Test that one source can compact to multiple destinations with different rules"""
        source_key = "test:source:multi"
        dest_key_1 = "test:dest:multi:1"
        dest_key_2 = "test:dest:multi:2"

        self.create_source_and_dest_series(source_key, dest_key_1)
        self.client.execute_command("TS.CREATE", dest_key_2)

        # Create different compaction rules
        self.add_compaction_rule(source_key, dest_key_1, "avg", 10000)  # 10s avg
        self.add_compaction_rule(source_key, dest_key_2, "max", 20000)  # 20s max

        base_ts = 1000

        # Add samples spanning multiple bucket sizes
        samples = [
            (base_ts, 10.0),
            (base_ts + 5000, 20.0),
            (base_ts + 12000, 30.0),
            (base_ts + 18000, 40.0),
            (
                base_ts + 25000,
                50.0,
            ),  # Forces both rules to finalize at least one bucket
        ]

        for ts, value in samples:
            self.add_sample(source_key, ts, value)

        # Verify both destinations received compacted data
        dest1_samples = self.client.execute_command("TS.RANGE", dest_key_1, "-", "+")
        dest2_samples = self.client.execute_command("TS.RANGE", dest_key_2, "-", "+")

        assert len(dest1_samples) >= 1  # At least one 10s bucket completed
        assert len(dest2_samples) >= 1  # At least one 20s bucket completed

        # Verify aggregation differences
        # dest1 should have avg of the first bucket: (10 + 20) / 2 = 15
        assert float(dest1_samples[0][1]) == 15.0

        # dest2 should have max of first 20s bucket: max(10, 20, 30, 40) = 40
        assert float(dest2_samples[0][1]) == 40.0

    def test_compaction_with_retention_policy(self):
        """Test compaction works correctly with retention policies"""
        source_key = "test:source:retention"
        dest_key = "test:dest:retention"

        # Create a series with 30-second retention
        retention_ms = 30000
        self.create_source_and_dest_series(source_key, dest_key, retention_ms)
        self.add_compaction_rule(source_key, dest_key, "avg", 5000)  # 5 second buckets

        base_ts = int(time.time() * 1000)

        # Add samples over time
        old_samples = [
            (base_ts - 40000, 10.0),  # Should be dropped due to retention
            (base_ts - 20000, 20.0),  # Within retention
            (base_ts - 10000, 30.0),  # Within retention
            (base_ts, 40.0),  # Current
        ]

        for ts, value in old_samples:
            self.add_sample(source_key, ts, value)
            # Very old samples might be rejected

        # Verify retention policy applied to source
        source_samples = self.client.execute_command("TS.RANGE", source_key, "-", "+")

        # All samples within retention should be preserved
        valid_samples = [s for s in old_samples if s[0] >= (base_ts - retention_ms)]
        assert len(source_samples) <= len(valid_samples)

        # Verify compaction still occurred for valid samples
        dest_samples = self.client.execute_command("TS.RANGE", dest_key, "-", "+")
        assert len(dest_samples) >= 1

    def _test_compaction_error_handling(self):
        """Test compaction handles errors gracefully"""
        source_key = "test:source:error"
        dest_key = "test:dest:error"

        self.create_source_and_dest_series(source_key, dest_key)
        self.add_compaction_rule(source_key, dest_key, "avg", 10000)

        # Add some normal samples
        base_ts = int(time.time() * 1000)
        self.add_sample(source_key, base_ts, 10.0)
        self.add_sample(source_key, base_ts + 5000, 10.0)

        # Delete destination series to simulate error condition
        self.client.execute_command("DEL", dest_key)

        # Adding new sample should handle missing destination gracefully
        try:
            self.add_sample(source_key, base_ts + 15000, 10.0)
            # Should not raise exception, just log the error internally
        except Exception as e:
            pytest.fail(f"Compaction error handling failed: {e}")

        # Verify source series still accepts samples
        source_samples = self.client.execute_command("TS.RANGE", source_key, "-", "+")
        assert len(source_samples) == 3

    def test_compaction_performance_with_many_samples(self):
        """Test compaction performance with a large number of samples"""
        source_key = "test:source:perf"
        dest_key = "test:dest:perf"

        self.create_source_and_dest_series(source_key, dest_key)
        self.add_compaction_rule(source_key, dest_key, "avg", 1000)  # 1-second buckets

        # Add many samples
        base_ts = int(time.time() * 1000)
        sample_count = 1000

        start_time = time.time()

        for i in range(sample_count):
            ts = base_ts + (i * 100)  # 100ms intervals
            value = float(i % 100)
            self.add_sample(source_key, ts, value)

        end_time = time.time()

        # Verify all samples were added
        source_samples = self.client.execute_command("TS.RANGE", source_key, "-", "+")
        assert len(source_samples) == sample_count

        # Verify compaction occurred efficiently
        dest_samples = self.client.execute_command("TS.RANGE", dest_key, "-", "+")
        expected_buckets = sample_count // 10  # ~10 samples per 1s bucket
        assert len(dest_samples) >= expected_buckets * 0.8  # Allow some variance

        # Performance check - should complete within reasonable time
        duration = end_time - start_time
        assert duration < 10.0, f"Compaction took too long: {duration}s"

    def test_compaction_maintains_consistent_state(self):
        """Test that compaction maintains a consistent state across operations"""
        source_key = "test:source:consistent"
        dest_key = "test:dest:consistent"

        self.create_source_and_dest_series(source_key, dest_key)
        self.add_compaction_rule(source_key, dest_key, "sum", 5000)

        base_ts = 100000

        # Add samples in mixed order to test state consistency
        operations = [
            (base_ts, 10.0),
            (base_ts + 2000, 20.0),
            (base_ts + 7000, 30.0),  # New bucket
            (base_ts + 1000, 15.0),  # Upsert in first bucket
            (base_ts + 8000, 35.0),  # Add to the second bucket
            (base_ts + 12000, 40.0),  # Third bucket
        ]

        for ts, value in operations:
            self.add_sample(source_key, ts, value)

        info = self.ts_info(source_key)
        print(info)

        # Verify that the final state is consistent
        source_samples = self.client.execute_command("TS.RANGE", source_key, "-", "+")
        print(source_samples)
        dest_samples = self.client.execute_command("TS.RANGE", dest_key, "-", "+")
        print(dest_samples)

        # Source should have all samples
        assert len(source_samples) == len(operations)

        # Destination should have compacted buckets
        assert len(dest_samples) >= 2  # At least two completed buckets

        # Verify compaction calculations are correct
        # (reference-verified: RedisTimeSeries returns the same values)
        # First bucket [100000, 105000): 10 + 20 + 15 (upsert) = 45
        # Second bucket [105000, 110000): 30 + 35 = 65
        assert len(dest_samples) >= 2

        assert float(dest_samples[0][1]) == 45.0
        assert float(dest_samples[1][1]) == 65.0

    def test_chained_avg_aggregates_over_intermediate_series_not_raw_source(self):
        """A chained rule aggregates its own declared source series, not the top-level
        raw stream. For a non-associative aggregator like AVG this is only observable
        with a chain: mid->dest AVG must average mid's own stored bucket values, not
        recompute an average directly over src's raw samples (those two differ unless
        every mid sub-bucket happens to have the same sample count)."""
        src = "test:chained_avg:src"
        mid = "test:chained_avg:mid"
        dest = "test:chained_avg:dest"

        self.client.execute_command("TS.CREATE", src)
        self.client.execute_command("TS.CREATE", mid)
        self.client.execute_command("TS.CREATE", dest)

        # src --AVG(10)--> mid --AVG(20)--> dest
        self.add_compaction_rule(src, mid, "avg", 10)
        self.add_compaction_rule(mid, dest, "avg", 20)

        # mid bucket [0,10): 3 samples (10,20,30) -> avg 20; closes when ts=11 arrives.
        self.add_sample(src, 1, 10.0)
        self.add_sample(src, 2, 20.0)
        self.add_sample(src, 3, 30.0)

        # mid bucket [10,20): 1 sample (100) -> avg 100; closes when ts=21 arrives.
        self.add_sample(src, 11, 100.0)

        # Closes mid bucket [10,20) and starts mid's next bucket; dest's bucket [0,20)
        # still open at this point (needs mid's *next* closure to finalize).
        self.add_sample(src, 21, 1.0)

        # Closes mid bucket [20,30), which closes dest's bucket [0,20).
        self.add_sample(src, 31, 5.0)

        # mid must hold its own two closed sub-bucket averages: avg(10,20,30)=20 and
        # avg(100)=100 -- not raw sums/counts from src. (ts=31 also closed mid's third
        # bucket [20,30) with avg(1)=1, irrelevant to this assertion, hence the range cap.)
        mid_samples = self.get_samples(mid, 0, 19)
        assert mid_samples == [[0, b"20"], [10, b"100"]]

        # dest's rule (mid->dest AVG) must average mid's own two stored values (20, 100)
        # = 60. The old (buggy) behavior fed src's raw samples straight into dest's
        # aggregator, which would instead compute avg(10,20,30,100) = 40.
        dest_samples = self.get_samples(dest, 0, 19)
        assert dest_samples == [[0, b"60"]]
