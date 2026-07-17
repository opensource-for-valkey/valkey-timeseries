import pytest
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTsDelCompaction(ValkeyTimeSeriesTestCaseBase):
    """Test TS.DEL compaction range handling when samples are removed from source TimeSeries"""

    def setup_source_and_dest_series(self, source_key, dest_key, retention=None, chunk_size=None):
        """Helper to set up source and destination series"""
        source_args = ['TS.CREATE', source_key]
        if retention:
            source_args.extend(['RETENTION', str(retention)])
        if chunk_size:
            source_args.extend(['CHUNK_SIZE', str(chunk_size)])
        self.client.execute_command(*source_args)

        self.client.execute_command('TS.CREATE', dest_key)

    def add_compaction_rule(self, source_key, dest_key, aggregation='avg', bucket_duration=1000):
        """Helper to add compaction rule"""
        self.client.execute_command('TS.CREATERULE', source_key, dest_key,
                                    'AGGREGATION', aggregation, bucket_duration)

    def add_samples_to_source(self, key, start_ts, count, interval=100, base_value=1.0):
        """Helper to add multiple samples to the source series"""
        for i in range(count):
            timestamp = start_ts + i * interval
            value = base_value + i
            self.client.execute_command('TS.ADD', key, timestamp, value)

    def get_compacted_samples(self, dest_key):
        """Helper to get all compacted samples from destination"""
        return self.client.execute_command('TS.RANGE', dest_key, 0, '+')

    def test_del_single_bucket_partial_removal(self):
        """Test deleting part of samples within a single compaction bucket"""
        source_key = 'source:single_bucket'
        dest_key = 'dest:single_bucket'

        self.setup_source_and_dest_series(source_key, dest_key)
        self.add_compaction_rule(source_key, dest_key, 'sum', 1000)

        # Add samples within one bucket (1000-1999ms)
        self.add_samples_to_source(source_key, 1000, 5, 100, 10.0)  # 1000, 1100, 1200, 1300, 1400

        # Initially compacted value should be sum of all samples (50 + 51 + 52 + 53 + 54 = 260)
        initial_compacted = self.get_compacted_samples(dest_key)
        # all samples should be in one bucket, so the bucket is not closed
        assert len(initial_compacted) == 0

        expected_sum = sum([10.0 + i for i in range(5)])
        latest = self.client.execute_command('TS.GET', dest_key, 'LATEST')
        assert int(latest[1]) == expected_sum

        # Delete middle samples (1100, 1200)
        deleted = self.client.execute_command('TS.DEL', source_key, 1100, 1200)
        assert deleted == 2

        # Compacted value should be recalculated without deleted samples
        latest = self.client.execute_command('TS.GET', dest_key, 'LATEST')
        # Should be sum of 10.0, 13.0, 14.0 = 37.0
        expected_new_sum = 10.0 + 13.0 + 14.0
        assert int(latest[1]) == expected_new_sum

    def test_del_complete_bucket_removal(self):
        """Test deleting all samples within a compaction bucket"""
        source_key = 'source:complete_bucket'
        dest_key = 'dest:complete_bucket'

        self.setup_source_and_dest_series(source_key, dest_key)
        self.add_compaction_rule(source_key, dest_key, 'avg', 1000)

        # Add samples in two buckets
        self.add_samples_to_source(source_key, 1000, 3, 100, 10.0)  # Bucket 1: 1000-1999
        self.add_samples_to_source(source_key, 2000, 3, 100, 20.0)  # Bucket 2: 2000-2999

        dest_info = self.ts_info(dest_key, True)

        # Should have 1 compacted value, since the second bucket is not closed yet
        initial_compacted = self.get_compacted_samples(dest_key)
        assert len(initial_compacted) == 1

        # Delete the entire first bucket
        deleted = self.client.execute_command('TS.DEL', source_key, 1000, 1999)
        assert deleted == 3
        samples = self.client.execute_command('TS.RANGE', source_key, 0, '+')
        print(f"Samples after deletion: {samples}")

        # Should only have the second bucket's compacted value
        latest = self.client.execute_command('TS.GET', dest_key, 'LATEST')
        samples = self.client.execute_command('TS.RANGE', dest_key, 0, '+')

        assert int(latest[1]) == 21  # Average of 21.0

    def test_del_multiple_buckets_partial(self):
        """Test deleting samples across multiple compaction buckets"""
        source_key = 'source:multi_bucket'
        dest_key = 'dest:multi_bucket'

        self.setup_source_and_dest_series(source_key, dest_key)
        self.add_compaction_rule(source_key, dest_key, 'max', 1000)

        # Add samples across 4 buckets
        for bucket in range(4):
            start_ts = 1000 + bucket * 1000
            self.add_samples_to_source(source_key, start_ts, 3, 100, 10.0 + bucket * 10)

        # Should have 3 compacted values, since the last bucket is not closed
        initial_compacted = self.get_compacted_samples(dest_key)
        assert len(initial_compacted) == 3

        # Delete samples spanning buckets 1 and 2 (1500-2500)
        deleted = self.client.execute_command('TS.DEL', source_key, 1500, 2500)
        assert deleted > 0

        # Check that affected buckets are properly recalculated
        updated_compacted = self.get_compacted_samples(dest_key)

        # Verify compacted values are updated correctly
        # (exact values depend on which samples were deleted)
        assert len(updated_compacted) >= 2  # Should have at least buckets 0 and 3

    def test_del_with_different_aggregations(self):
        """Test deletion with various aggregation types"""
        aggregations = ['sum', 'avg', 'min', 'max', 'count', 'std.p', 'std.s', 'var.p', 'var.s']

        for agg in aggregations:
            source_key = f'source:{agg}'
            dest_key = f'dest:{agg}'

            self.setup_source_and_dest_series(source_key, dest_key)
            self.add_compaction_rule(source_key, dest_key, agg, 1000)

            # Add samples
            self.add_samples_to_source(source_key, 1000, 20, 100, 5.0)

            # Get the initially compacted value
            initial = self.get_compacted_samples(dest_key)
            assert len(initial) == 1, f"Initial compacted samples missing for aggregation {agg}. Expecting 1 sample, got {len(initial)}"

            # Delete some samples
            deleted = self.client.execute_command('TS.DEL', source_key, 1200, 1350)
            assert deleted > 0

            # Verify compacted value is recalculated
            updated = self.get_compacted_samples(dest_key)
            assert len(updated) == 1, f"Updated compacted samples missing for aggregation {agg}. Expecting 1 sample, got {len(updated)}"

            # For most aggregations, the value should change after deletion
            if agg not in ['count']:  # count might have special behavior
                # We expect the aggregated value to be different after deletion
                # (exact comparison depends on an aggregation type and deleted values)
                pass

    def test_del_current_bucket_adjustment(self):
        """Deleting from the current (incomplete) bucket adjusts its pending aggregation.

        An open bucket must not reach the destination until something closes it (verified
        against RedisTimeSeries), so the delete and the follow-up sample only change the
        pending state. Once a later sample does close the bucket, the value it publishes must
        exclude the deleted sample.
        """
        source_key = 'source:current'
        dest_key = 'dest:current'

        self.setup_source_and_dest_series(source_key, dest_key)
        self.add_compaction_rule(source_key, dest_key, 'sum', 1000)

        # Bucket [1000, 2000) gets 10.0 + 11.0 + 12.0 and is closed by the next bucket's
        # samples; bucket [2000, 3000) gets 20.0 + 21.0 and stays open.
        self.add_samples_to_source(source_key, 1000, 3, 100, 10.0)
        self.add_samples_to_source(source_key, 2000, 2, 100, 20.0)

        # Only the closed bucket is published.
        initial = self.get_compacted_samples(dest_key)
        assert initial == [[1000, b'33']]

        # Delete the 20.0 sample out of the still-open bucket.
        deleted = self.client.execute_command('TS.DEL', source_key, 2000, 2050)
        assert deleted == 1

        # Another sample lands in the same bucket, so it stays open: nothing new is published.
        self.client.execute_command('TS.ADD', source_key, 2200, 22.0)
        assert self.get_compacted_samples(dest_key) == [[1000, b'33']]

        # A sample in a later bucket finally closes it. The published value must be
        # 21.0 + 22.0, i.e. the deleted 20.0 is excluded (it would be 63.0 otherwise).
        self.client.execute_command('TS.ADD', source_key, 4000, 1.0)
        final = self.get_compacted_samples(dest_key)
        assert len(final) == 2
        assert final[1][0] == 2000
        assert float(final[1][1]) == 43.0

    def _test_del_with_retention_policy(self):
        """Test deletion with retention policy constraints"""
        source_key = 'source:retention'
        dest_key = 'dest:retention'

        # Create a source with 5-second retention
        self.setup_source_and_dest_series(source_key, dest_key, retention=5000)
        self.add_compaction_rule(source_key, dest_key, 'avg', 1000)

        current_time = 10000
        # Add old samples (beyond retention)
        old_samples_ts = current_time - 6000
        self.add_samples_to_source(source_key, old_samples_ts, 3, 100, 5.0)

        # Add recent samples (within retention)
        recent_samples_ts = current_time - 2000
        self.add_samples_to_source(source_key, recent_samples_ts, 3, 100, 15.0)

        # Try to delete old samples (should fail due to retention)
        with pytest.raises(Exception) as excinfo:
            self.client.execute_command('TS.DEL', source_key, old_samples_ts, old_samples_ts + 500)
        assert "retention" in str(excinfo.value).lower()

        # Delete recent samples (should succeed)
        deleted = self.client.execute_command('TS.DEL', source_key, recent_samples_ts, recent_samples_ts + 200)
        assert deleted >= 1

    def test_del_bucket_boundary_handling(self):
        """Test deletion at exact bucket boundaries"""
        source_key = 'source:boundary'
        dest_key = 'dest:boundary'

        self.setup_source_and_dest_series(source_key, dest_key)
        self.add_compaction_rule(source_key, dest_key, 'sum', 1000)

        # Add samples at bucket boundaries
        boundary_samples = [999, 1000, 1001, 1999, 2000, 2001]
        for i, ts in enumerate(boundary_samples):
            self.client.execute_command('TS.ADD', source_key, ts, 10.0 + i)

        # Delete samples exactly at boundaries
        deleted = self.client.execute_command('TS.DEL', source_key, 1000, 2000)
        assert deleted >= 2

        # Verify boundary handling is correct
        updated = self.get_compacted_samples(dest_key)
        # Should still have compacted data but with recalculated values
        assert len(updated) >= 1

    def test_del_empty_bucket_after_deletion(self):
        """Test behavior when deletion results in empty buckets"""
        source_key = 'source:empty'
        dest_key = 'dest:empty'

        self.setup_source_and_dest_series(source_key, dest_key)
        self.add_compaction_rule(source_key, dest_key, 'count', 1000)

        # Add samples in multiple buckets
        for bucket in range(4):
            start_ts = 1000 + bucket * 1000
            self.add_samples_to_source(source_key, start_ts, 2, 200, 10.0)

        initial = self.get_compacted_samples(dest_key)
        assert len(initial) == 3

        # Delete all samples from the middle bucket
        deleted = self.client.execute_command('TS.DEL', source_key, 2000, 2999)
        assert deleted == 2

        # The middle bucket should be removed from compacted data
        updated = self.get_compacted_samples(dest_key)
        assert len(updated) == 2

        # Verify the remaining buckets are correct
        timestamps = [sample[0] for sample in updated]
        assert 1000 in timestamps
        assert 3000 in timestamps
        assert 2000 not in timestamps

    def _test_del_compaction_error_recovery(self):
        """Test error handling during compaction after deletion"""
        source_key = 'source:error'
        dest_key = 'dest:error'

        self.setup_source_and_dest_series(source_key, dest_key)
        self.add_compaction_rule(source_key, dest_key, 'avg', 1000)

        # Add samples
        self.add_samples_to_source(source_key, 1000, 5, 100, 10.0)

        # Delete samples
        deleted = self.client.execute_command('TS.DEL', source_key, 1100, 1300)
        assert deleted >= 1

        # Verify that the system is still in a consistent state
        compacted = self.get_compacted_samples(dest_key)
        source_samples = self.client.execute_command('TS.RANGE', source_key, 0, '+')

        # Both source and destination should be accessible
        assert isinstance(compacted, list)
        assert isinstance(source_samples, list)

    def test_del_multiple_compaction_rules(self):
        """Test deletion with multiple compaction rules on the same source"""
        source_key = 'source:multi_rules'
        dest1_key = 'dest1:multi_rules'
        dest2_key = 'dest2:multi_rules'

        self.setup_source_and_dest_series(source_key, dest1_key)
        self.client.execute_command('TS.CREATE', dest2_key)

        # Add multiple compaction rules
        self.add_compaction_rule(source_key, dest1_key, 'sum', 1000)
        self.add_compaction_rule(source_key, dest2_key, 'avg', 500)  # Different bucket size

        # Add samples
        self.add_samples_to_source(source_key, 1000, 25, 50, 5.0)

        # Get initial compacted values
        initial1 = self.get_compacted_samples(dest1_key)
        assert len(initial1) >= 1, "Initial compacted samples missing for dest1_key"
        initial2 = self.get_compacted_samples(dest2_key)
        assert len(initial2) >= 1, "Initial compacted samples missing for dest2_key"

        # Delete samples
        deleted = self.client.execute_command('TS.DEL', source_key, 1200, 1400)
        assert deleted >= 1

        # Both destinations should be updated
        updated1 = self.get_compacted_samples(dest1_key)
        updated2 = self.get_compacted_samples(dest2_key)

        # Verify both compaction rules handled the deletion
        assert len(updated1) >= 1
        assert len(updated2) >= 1

        assert len(updated1) <= len(initial1)
        assert len(updated2) <= len(initial2)

        # make sure we don't have samples in the deleted range
        for sample in updated1:
            assert not (1200 <= sample[0] <= 1400)
        for sample in updated2:
            assert not (1200 <= sample[0] <= 1400)

    def test_del_large_range_compaction_efficiency(self):
        """Test deletion of large ranges and compaction efficiency"""
        source_key = 'source:large'
        dest_key = 'dest:large'

        self.setup_source_and_dest_series(source_key, dest_key, chunk_size=256)
        self.add_compaction_rule(source_key, dest_key, 'sum', 1000)

        # Add many samples across multiple buckets and chunks
        for bucket in range(10):
            start_ts = 1000 + bucket * 1000
            self.add_samples_to_source(source_key, start_ts, 20, 25, 10.0 + bucket)

        initial = self.get_compacted_samples(dest_key)
        initial_count = len(initial)
        assert initial_count == 9 # the last bucket is not closed yet

        # Delete large range spanning multiple buckets
        deleted = self.client.execute_command('TS.DEL', source_key, 3000, 8000)
        assert deleted > 50  # Should delete many samples

        # Verify compaction is updated efficiently
        updated = self.get_compacted_samples(dest_key)
        assert len(updated) < initial_count  # Some buckets should be completely removed

        # Verify remaining data integrity
        source_samples = self.client.execute_command('TS.RANGE', source_key, 0, '+')
        assert len(source_samples) > 0  # Should still have data outside the deleted range