from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util.waiters import *

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestDebug(ValkeyTimeSeriesTestCaseBase):
    """Integration tests for DEBUG DIGEST-VALUE."""

    def test_digest_varies_with_different_data(self):
        """Test that DEBUG DIGEST-VALUE produces different digests for series with different data."""
        key1 = 'ts_data_1'
        key2 = 'ts_data_2'

        # Create two series with different data
        self.client.execute_command('TS.CREATE', key1)
        self.client.execute_command('TS.CREATE', key2)

        # Add different samples to each series
        self.client.execute_command('TS.ADD', key1, 1000, 10.5)
        self.client.execute_command('TS.ADD', key1, 2000, 20.5)

        self.client.execute_command('TS.ADD', key2, 1000, 30.5)
        self.client.execute_command('TS.ADD', key2, 2000, 40.5)

        # Get digest values
        digest1 = self.client.execute_command('DEBUG DIGEST-VALUE', key1)
        digest2 = self.client.execute_command('DEBUG DIGEST-VALUE', key2)

        # Digests should be different
        assert digest1 != digest2, "Digests should differ when series have different data"

    def test_digest_varies_with_different_timestamps(self):
        """Test that DEBUG DIGEST-VALUE produces different digests for series with same values but different timestamps."""
        key1 = 'ts_timestamps_1'
        key2 = 'ts_timestamps_2'

        # Create two series with the same values but different timestamps
        self.client.execute_command('TS.CREATE', key1)
        self.client.execute_command('TS.CREATE', key2)

        self.client.execute_command('TS.ADD', key1, 1000, 10.0)
        self.client.execute_command('TS.ADD', key1, 2000, 20.0)

        self.client.execute_command('TS.ADD', key2, 1500, 10.0)  # Different timestamps
        self.client.execute_command('TS.ADD', key2, 2500, 20.0)

        digest1 = self.client.execute_command('DEBUG DIGEST-VALUE', key1)
        digest2 = self.client.execute_command('DEBUG DIGEST-VALUE', key2)

        assert digest1 != digest2, "Digests should differ when series have different timestamps"

    def test_digest_varies_with_different_labels(self):
        """Test that DEBUG DIGEST-VALUE produces different digests for series with different labels."""
        key1 = 'ts_labels_1'
        key2 = 'ts_labels_2'

        # Create series with different labels
        self.client.execute_command('TS.CREATE', key1, 'LABELS', 'region', 'us-east', 'env', 'prod')
        self.client.execute_command('TS.CREATE', key2, 'LABELS', 'region', 'us-west', 'env', 'dev')

        # Add the same data to both
        self.client.execute_command('TS.ADD', key1, 1000, 10.0)
        self.client.execute_command('TS.ADD', key2, 1000, 10.0)

        digest1 = self.client.execute_command('DEBUG DIGEST-VALUE', key1)
        digest2 = self.client.execute_command('DEBUG DIGEST-VALUE', key2)

        assert digest1 != digest2, "Digests should differ when series have different labels"

    def test_digest_varies_with_different_retention(self):
        """Test that DEBUG DIGEST-VALUE produces different digests for series with different retention policies."""
        key1 = 'ts_retention_1'
        key2 = 'ts_retention_2'

        # Create series with different retention policies
        self.client.execute_command('TS.CREATE', key1, 'RETENTION', 86400000)  # 1 day
        self.client.execute_command('TS.CREATE', key2, 'RETENTION', 172800000)  # 2 days

        # Add the same data to both
        self.client.execute_command('TS.ADD', key1, 1000, 10.0)
        self.client.execute_command('TS.ADD', key2, 1000, 10.0)

        digest1 = self.client.execute_command('DEBUG DIGEST-VALUE', key1)
        digest2 = self.client.execute_command('DEBUG DIGEST-VALUE', key2)

        assert digest1 != digest2, "Digests should differ when series have different retention policies"

    def test_digest_varies_with_different_chunk_size(self):
        """Test that DEBUG DIGEST-VALUE produces different digests for series with different chunk sizes."""
        key1 = 'ts_chunk_1'
        key2 = 'ts_chunk_2'

        # Create series with different chunk sizes
        self.client.execute_command('TS.CREATE', key1, 'CHUNK_SIZE', 128)
        self.client.execute_command('TS.CREATE', key2, 'CHUNK_SIZE', 256)

        # Add the same data to both
        self.client.execute_command('TS.ADD', key1, 1000, 10.0)
        self.client.execute_command('TS.ADD', key2, 1000, 10.0)

        digest1 = self.client.execute_command('DEBUG DIGEST-VALUE', key1)
        digest2 = self.client.execute_command('DEBUG DIGEST-VALUE', key2)

        assert digest1 != digest2, "Digests should differ when series have different chunk sizes"

    def test_digest_varies_with_different_duplicate_policy(self):
        """Test that DEBUG DIGEST-VALUE produces different digests for series with different duplicate policies."""
        key1 = 'ts_dup_1'
        key2 = 'ts_dup_2'

        # Create series with different duplicate policies
        self.client.execute_command('TS.CREATE', key1, 'DUPLICATE_POLICY', 'LAST')
        self.client.execute_command('TS.CREATE', key2, 'DUPLICATE_POLICY', 'FIRST')

        # Add the same data to both
        self.client.execute_command('TS.ADD', key1, 1000, 10.0)
        self.client.execute_command('TS.ADD', key2, 1000, 10.0)

        digest1 = self.client.execute_command('DEBUG DIGEST-VALUE', key1)
        digest2 = self.client.execute_command('DEBUG DIGEST-VALUE', key2)

        assert digest1 != digest2, "Digests should differ when series have different duplicate policies"

    def test_digest_varies_with_different_compression(self):
        """Test that DEBUG DIGEST-VALUE produces different digests for series with different compression algorithms."""
        key1 = 'ts_comp_1'
        key2 = 'ts_comp_2'

        # Create series with different compression algorithms
        self.client.execute_command('TS.CREATE', key1, 'ENCODING', 'UNCOMPRESSED')
        self.client.execute_command('TS.CREATE', key2, 'ENCODING', 'COMPRESSED')

        # Add the same data to both
        for i in range(10):
            self.client.execute_command('TS.ADD', key1, 1000 + i * 100, i * 1.5)
            self.client.execute_command('TS.ADD', key2, 1000 + i * 100, i * 1.5)

        digest1 = self.client.execute_command('DEBUG DIGEST-VALUE', key1)
        digest2 = self.client.execute_command('DEBUG DIGEST-VALUE', key2)

        assert digest1 != digest2, "Digests should differ when series have different compression algorithms"

    def test_digest_consistent_for_identical_series(self):
        """Test that DEBUG DIGEST-VALUE produces identical digests for identical series."""
        key1 = 'ts_identical_1'
        key2 = 'ts_identical_2'

        # Create identical series
        self.client.execute_command('TS.CREATE', key1, 'LABELS', 'app', 'test', 'RETENTION', 86400000, 'CHUNK_SIZE', 128)
        self.client.execute_command('TS.CREATE', key2, 'LABELS', 'app', 'test', 'RETENTION', 86400000, 'CHUNK_SIZE', 128)

        # Add identical data
        timestamps_values = [(1000, 10.0), (2000, 20.0), (3000, 30.0)]
        for ts, val in timestamps_values:
            self.client.execute_command('TS.ADD', key1, ts, val)
            self.client.execute_command('TS.ADD', key2, ts, val)

        digest1 = self.client.execute_command('DEBUG DIGEST-VALUE', key1)
        digest2 = self.client.execute_command('DEBUG DIGEST-VALUE', key2)

        assert digest1 == digest2, "Digests should be identical for identical series"

    def test_digest_changes_after_data_modification(self):
        """Test that DEBUG DIGEST-VALUE changes after data is modified."""
        key = 'ts_modify'

        # Create series and add initial data
        self.client.execute_command('TS.CREATE', key)
        self.client.execute_command('TS.ADD', key, 1000, 10.0)

        initial_digest = self.client.execute_command('DEBUG DIGEST-VALUE', key)

        # Add more data
        self.client.execute_command('TS.ADD', key, 2000, 20.0)

        modified_digest = self.client.execute_command('DEBUG DIGEST-VALUE', key)

        assert initial_digest != modified_digest, "Digest should change after data modification"

    def test_digest_with_compaction_rules(self):
        """Test that DEBUG DIGEST-VALUE varies for series with different compaction rules."""
        source_key = 'ts_source'
        dest_key1 = 'ts_dest_1'
        dest_key2 = 'ts_dest_2'

        # Create source series
        self.client.execute_command('TS.CREATE', source_key)

        # Create destination series with different aggregation rules
        self.client.execute_command('TS.CREATE', dest_key1)
        self.client.execute_command('TS.CREATE', dest_key2)

        # Add compaction rules with different aggregations
        self.client.execute_command('TS.CREATERULE', source_key, dest_key1, 'AGGREGATION', 'avg', 60000)
        self.client.execute_command('TS.CREATERULE', source_key, dest_key2, 'AGGREGATION', 'max', 60000)

        # Add data to the source
        for i in range(5):
            self.client.execute_command('TS.ADD', source_key, 1000 + i * 10000, i * 2.0)

        # Get digests for source (which now has different rules)
        source_digest = self.client.execute_command('DEBUG DIGEST-VALUE', source_key)

        # Create another source without rules for comparison
        source_no_rules = 'ts_source_no_rules'
        self.client.execute_command('TS.CREATE', source_no_rules)
        for i in range(5):
            self.client.execute_command('TS.ADD', source_no_rules, 1000 + i * 10000, i * 2.0)

        source_no_rules_digest = self.client.execute_command('DEBUG DIGEST-VALUE', source_no_rules)

        assert source_digest != source_no_rules_digest, "Digests should differ when series have different compaction rules"

    def test_digest_with_multiple_chunks(self):
        """Test that DEBUG DIGEST-VALUE accounts for multiple chunks in a series."""
        key = 'ts_multi_chunk'
        chunk_size = 64  # Small chunk size to force multiple chunks

        self.client.execute_command('TS.CREATE', key, 'CHUNK_SIZE', chunk_size)

        # Add enough data to create multiple chunks
        num_samples = chunk_size * 3  # Should create at least 3 chunks
        for i in range(num_samples):
            self.client.execute_command('TS.ADD', key, 1000 + i * 100, i * 0.5)

        multi_chunk_digest = self.client.execute_command('DEBUG DIGEST-VALUE', key)

        # Compare with a series that has fewer samples (single chunk)
        key_single = 'ts_single_chunk'
        self.client.execute_command('TS.CREATE', key_single, 'CHUNK_SIZE', chunk_size)

        # Add only a few samples (single chunk)
        for i in range(5):
            self.client.execute_command('TS.ADD', key_single, 1000 + i * 100, i * 0.5)

        single_chunk_digest = self.client.execute_command('DEBUG DIGEST-VALUE', key_single)

        assert multi_chunk_digest != single_chunk_digest, "Digests should differ between single and multi-chunk series"

    def test_digest_survives_dump_restore_of_overfull_chunk(self):
        """A lossless DUMP/RESTORE must not change the digest, even when an out-of-order
        insert left an uncompressed chunk past its capacity (the load normalizes it)."""
        key = 'ts_overfull'
        self.client.execute_command('TS.CREATE', key, 'ENCODING', 'UNCOMPRESSED', 'CHUNK_SIZE', 48)
        for ts in (10, 20, 30, 15):
            self.client.execute_command('TS.ADD', key, ts, ts)

        info = self.ts_info(key, debug=True)
        assert info['chunks'][0]['samples'] == 4, "setup should leave one overfull chunk"

        restored = 'ts_overfull_restored'
        self.client.restore(restored, 0, self.client.dump(key))

        assert self.client.execute_command('TS.RANGE', restored, '-', '+') == \
            self.client.execute_command('TS.RANGE', key, '-', '+')
        assert self.client.execute_command('DEBUG DIGEST-VALUE', restored) == \
            self.client.execute_command('DEBUG DIGEST-VALUE', key), \
            "Digest should survive a lossless DUMP/RESTORE"

    def test_digest_empty_series(self):
        """Test that DEBUG DIGEST-VALUE works with empty series and produces different digests for different empty series."""
        key1 = 'ts_empty_1'
        key2 = 'ts_empty_2'

        # Create an empty series with different properties
        self.client.execute_command('TS.CREATE', key1, 'RETENTION', 86400000)
        self.client.execute_command('TS.CREATE', key2, 'RETENTION', 172800000)

        digest1 = self.client.execute_command('DEBUG DIGEST-VALUE', key1)
        digest2 = self.client.execute_command('DEBUG DIGEST-VALUE', key2)

        # Even empty series should have different digests if they have different properties
        assert digest1 != digest2, "Digests should differ for empty series with different properties"

        # Verify both digests are valid (not None/empty)
        assert digest1 is not None and digest1 != b'', "Empty series should still have a valid digest"
        assert digest2 is not None and digest2 != b'', "Empty series should still have a valid digest"