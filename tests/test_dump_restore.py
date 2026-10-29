import time
import pytest
from valkeytestframework.util.waiters import *
from valkeytestframework.valkey_test_case import ValkeyAction
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from valkey.exceptions import ResponseError


# CRC-64/Jones (reflected), the DUMP payload footer checksum.
_CRC_TBL = []
for _i in range(256):
    _crc = _i
    for _ in range(8):
        _crc = (_crc >> 1) ^ 0x95AC9329AC4BC9B5 if _crc & 1 else _crc >> 1
    _CRC_TBL.append(_crc)


def _with_encver(payload: bytes, encver: int) -> bytes:
    """Rewrite a MODULE_2 DUMP payload's module-type encoding version and fix the CRC."""
    assert payload[0] == 7, "expected RDB_TYPE_MODULE_2 opcode"
    assert payload[1] == 0x81, "expected 64-bit length marker for the module id"
    mid = int.from_bytes(payload[2:10], "big")
    mid = (mid & ~0x3FF) | encver
    body = payload[:2] + mid.to_bytes(8, "big") + payload[10:-8]
    crc = 0
    for b in body:
        crc = _CRC_TBL[(crc ^ b) & 0xFF] ^ (crc >> 8)
    return body + crc.to_bytes(8, "little")


class TestTimeseriesDumpRestore(ValkeyTimeSeriesTestCaseBase):
    """Tests for DUMP/RESTORE serialization of TimeSeries data"""

    @pytest.mark.parametrize('values', ['quantized', 'counter_2p50'])
    def test_chimp_dump_restore_and_append(self, values):
        """Decimal-place headers, reused windows and the timestamp tiers survive sealed
        chunks, DUMP/RESTORE and continued writes."""
        client = self.client
        client.execute_command('TS.CREATE', 'src', 'ENCODING', 'CHIMP', 'CHUNK_SIZE', 512,
                               'DUPLICATE_POLICY', 'LAST')

        # Values go out as strings this test controls and are compared as parsed floats,
        # so no Python arithmetic has to round-trip through the server's formatting.
        def val(i):
            if values == 'quantized':
                return f'{100 + i / 100:.2f}'
            return str((1 << 50) + 7 * i)

        def ts(i):
            return 1000 * (i + 1) + (i % 3) * 7  # jittered

        expected = [(ts(i), val(i)) for i in range(1001)]
        with client.pipeline(transaction=False) as p:
            for t, v in expected[:1000]:
                p.execute_command('TS.ADD', 'src', t, v)
            p.execute()
        payload = client.execute_command('DUMP', 'src')
        client.execute_command('RESTORE', 'dst', 0, payload)
        client.execute_command('TS.ADD', 'dst', *expected[1000])
        # A duplicate rewrites a sealed chunk, exercising decode -> encode too.
        rewritten = f'{101.01:.2f}' if values == 'quantized' else str((1 << 50) + 8)
        client.execute_command('TS.ADD', 'dst', expected[1][0], rewritten)
        expected[1] = (expected[1][0], rewritten)

        actual = client.execute_command('TS.RANGE', 'dst', '-', '+')
        assert len(actual) == len(expected)
        for i, ((timestamp, value), (want_ts, want)) in enumerate(zip(actual, expected)):
            assert timestamp == want_ts, (i, timestamp)
            assert float(value) == float(want), (i, value, want)

    @pytest.mark.parametrize('encver', [1, 2])
    def test_restore_rejects_older_encoding_versions(self, encver):
        """Payloads from before the version-3 format are refused, not misdecoded."""
        client = self.client
        client.execute_command('TS.CREATE', 'cur', 'ENCODING', 'CHIMP')
        for i in range(100):
            client.execute_command('TS.ADD', 'cur', 1000 + i, 1.5 + i / 100)
        payload = client.execute_command('DUMP', 'cur')

        # Sanity: the payload with its true version loads.
        assert client.execute_command('RESTORE', 'cur_copy', 0, payload) == b'OK'

        with pytest.raises(ResponseError, match='Bad data format'):
            client.execute_command('RESTORE', f'old{encver}', 0, _with_encver(payload, encver))
        assert client.execute_command('EXISTS', f'old{encver}') == 0
        assert client.ping()

    @pytest.mark.parametrize('offset', [1 << 30, 1 << 40, 1 << 50])
    def test_gorilla_large_counter_dump_restore_and_append(self, offset):
        """Extended XOR windows survive sealed chunks, RESTORE and continued writes."""
        client = self.client
        client.execute_command('TS.CREATE', 'large_counter', 'ENCODING', 'GORILLA',
                               'CHUNK_SIZE', 512, 'DUPLICATE_POLICY', 'LAST')
        with client.pipeline(transaction=False) as pipeline:
            for i in range(1000):
                pipeline.execute_command('TS.ADD', 'large_counter', 1000 * (i + 1), offset + i)
            pipeline.execute()
        payload = client.execute_command('DUMP', 'large_counter')
        client.execute_command('RESTORE', 'large_restored', 0, payload)
        client.execute_command('TS.ADD', 'large_restored', 1001000, offset + 1000)
        # A duplicate rewrites a sealed chunk, exercising decode -> encode too.
        client.execute_command('TS.ADD', 'large_restored', 2000, offset + 3)
        actual = client.execute_command('TS.RANGE', 'large_restored', '-', '+')
        assert len(actual) == 1001
        for i, (timestamp, value) in enumerate(actual):
            assert timestamp == 1000 * (i + 1)
            assert float(value) == float(offset + (3 if i == 1 else i))

    def test_basic_dump_restore(self):
        """Test basic DUMP/RESTORE preserves all series properties"""
        client = self.client

        # Create timeseries with various properties
        client.execute_command('TS.CREATE', 'ts_basic',
                               'RETENTION', 86400000,
                               'CHUNK_SIZE', 512,
                               'DUPLICATE_POLICY', 'LAST',
                               'LABELS', 'sensor', 'temp', 'location', 'room1')

        # Add samples
        timestamps = [1000, 2000, 3000, 4000, 5000]
        values = [20.5, 21.0, 20.8, 22.1, 21.5]
        for ts, val in zip(timestamps, values):
            client.execute_command('TS.ADD', 'ts_basic', ts, val)

        # Get the original state
        original_info = self.ts_info('ts_basic')
        original_range = client.execute_command('TS.RANGE', 'ts_basic', '-', '+')
        original_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'ts_basic')

        # DUMP the key
        dump_data = client.execute_command('DUMP', 'ts_basic')
        assert dump_data is not None

        # Delete the original key
        client.execute_command('DEL', 'ts_basic')

        # Verify that the key is gone
        assert client.execute_command('EXISTS', 'ts_basic') == 0

        # RESTORE the key
        client.execute_command('RESTORE', 'ts_basic', 0, dump_data)

        # Verify restoration
        restored_info = self.ts_info('ts_basic')
        restored_range = client.execute_command('TS.RANGE', 'ts_basic', '-', '+')
        restored_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'ts_basic')

        # Validate digest matches
        assert restored_digest == original_digest

        # Validate properties
        assert restored_info['totalSamples'] == original_info['totalSamples']
        assert restored_info['retentionTime'] == original_info['retentionTime']
        assert restored_info['chunkSize'] == original_info['chunkSize']
        assert restored_info['duplicatePolicy'] == original_info['duplicatePolicy']
        assert restored_info['labels'] == original_info['labels']
        assert restored_info['firstTimestamp'] == original_info['firstTimestamp']
        assert restored_info['lastTimestamp'] == original_info['lastTimestamp']

        # Validate data integrity
        assert len(restored_range) == len(original_range)
        for orig, rest in zip(original_range, restored_range):
            assert orig[0] == rest[0]  # timestamp
            assert float(orig[1]) == float(rest[1])  # value

    def test_dump_restore_with_compaction_rules(self):
        """Test DUMP/RESTORE preserves compaction rules on source series"""
        client = self.client

        # Create source timeseries
        client.execute_command('TS.CREATE', 'src_series',
                               'RETENTION', 3600000,
                               'LABELS', 'type', 'source')

        # Create compaction destination series
        client.execute_command('TS.CREATE', 'dest_avg',
                               'RETENTION', 86400000,
                               'LABELS', 'type', 'avg_agg')

        client.execute_command('TS.CREATE', 'dest_max',
                               'RETENTION', 86400000,
                               'LABELS', 'type', 'max_agg')

        client.execute_command('TS.CREATE', 'dest_min',
                               'RETENTION', 86400000,
                               'LABELS', 'type', 'min_agg')

        # Create compaction rules
        client.execute_command('TS.CREATERULE', 'src_series', 'dest_avg',
                               'AGGREGATION', 'avg', 60000)
        client.execute_command('TS.CREATERULE', 'src_series', 'dest_max',
                               'AGGREGATION', 'max', 60000)
        client.execute_command('TS.CREATERULE', 'src_series', 'dest_min',
                               'AGGREGATION', 'min', 60000, 0)

        # Add data to source
        base_time = 1000000
        for i in range(120):
            client.execute_command('TS.ADD', 'src_series', base_time + i * 1000, 50 + (i % 30))

        time.sleep(0.1)  # Allow compactions to process

        # Get the original state
        original_src_info = self.ts_info('src_series')
        original_src_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'src_series')

        # Verify rules exist
        assert len(original_src_info['rules']) == 3

        # DUMP the source series
        dump_data = client.execute_command('DUMP', 'src_series')

        # Delete and restore
        client.execute_command('DEL', 'src_series')
        client.execute_command('RESTORE', 'src_series', 0, dump_data)

        # Verify restoration
        restored_src_info = self.ts_info('src_series')
        restored_src_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'src_series')

        # Validate digest
        assert restored_src_digest == original_src_digest

        # Validate compaction rules are preserved
        assert len(restored_src_info['rules']) == len(original_src_info['rules'])

        original_rules = original_src_info['rules']
        restored_rules = restored_src_info['rules']

        for (orig_rule, rest_rule) in zip(original_rules, restored_rules):
            assert orig_rule == rest_rule  # bucket duration

        # Verify data integrity
        assert restored_src_info['totalSamples'] == original_src_info['totalSamples']

    def test_dump_restore_compaction_destination(self):
        """Test DUMP/RESTORE preserves compaction destination state (sourceKey reference)"""
        client = self.client

        # Create source and destination
        client.execute_command('TS.CREATE', 'comp_source',
                               'LABELS', 'type', 'raw')

        client.execute_command('TS.CREATE', 'comp_dest',
                               'LABELS', 'type', 'aggregated')

        # Create rule
        client.execute_command('TS.CREATERULE', 'comp_source', 'comp_dest',
                               'AGGREGATION', 'sum', 10000)

        # Add data
        for i in range(50):
            client.execute_command('TS.ADD', 'comp_source', 1000 + i * 1000, i * 2.5)

        # Get the original destination state
        original_dest_info = self.ts_info('comp_dest')
        original_dest_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'comp_dest')
        original_dest_range = client.execute_command('TS.RANGE', 'comp_dest', '-', '+')

        # DUMP destination
        dump_data = client.execute_command('DUMP', 'comp_dest')

        # Delete and restore destination
        client.execute_command('DEL', 'comp_dest')
        client.execute_command('RESTORE', 'comp_dest', 0, dump_data)

        # Verify
        restored_dest_info = self.ts_info('comp_dest')
        restored_dest_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'comp_dest')
        restored_dest_range = client.execute_command('TS.RANGE', 'comp_dest', '-', '+')

        assert restored_dest_digest == original_dest_digest
        assert restored_dest_info['totalSamples'] == original_dest_info['totalSamples']

        # Check sourceKey is preserved
        if 'sourceKey' in original_dest_info:
            assert restored_dest_info.get('sourceKey') == original_dest_info['sourceKey']

        # Validate aggregated data
        assert len(restored_dest_range) == len(original_dest_range)
        for orig, rest in zip(original_dest_range, restored_dest_range):
            assert orig[0] == rest[0]
            assert float(orig[1]) == float(rest[1])

    def test_dump_restore_preserves_partial_buckets(self):
        """Test DUMP/RESTORE preserves partial bucket state in compaction"""
        client = self.client

        # Create source series with compaction rule
        client.execute_command('TS.CREATE', 'partial_src',
                               'RETENTION', '0',
                               'CHUNK_SIZE', '4096',
                               'LABELS', 'type', 'partial_test')

        # Create a destination with 10-second buckets
        client.execute_command('TS.CREATE', 'partial_dest',
                               'RETENTION', '0',
                               'CHUNK_SIZE', '4096')

        # Add compaction rule: 10-second SUM buckets
        client.execute_command('TS.CREATERULE', 'partial_src', 'partial_dest',
                               'AGGREGATION', 'sum', '10000')

        # Add samples that create partial buckets
        # Bucket 1: [0-10000) - complete with samples at 1000, 2000, 3000
        client.execute_command('TS.ADD', 'partial_src', 1000, 10.0)
        client.execute_command('TS.ADD', 'partial_src', 2000, 20.0)
        client.execute_command('TS.ADD', 'partial_src', 3000, 30.0)

        # Bucket 2: [10000-20000) - complete with samples at 11000, 12000
        client.execute_command('TS.ADD', 'partial_src', 11000, 40.0)
        client.execute_command('TS.ADD', 'partial_src', 12000, 50.0)

        # Bucket 3: [20000-30000) - PARTIAL with only a sample at 21000
        client.execute_command('TS.ADD', 'partial_src', 21000, 60.0)

        # Capture the original state
        original_src_info = self.ts_info('partial_src')
        original_dest_info = self.ts_info('partial_dest')
        original_src_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'partial_src')
        original_dest_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'partial_dest')
        original_dest_range = client.execute_command('TS.RANGE', 'partial_dest', '-', '+')

        # DUMP both series
        src_dump = client.execute_command('DUMP', 'partial_src')
        dest_dump = client.execute_command('DUMP', 'partial_dest')

        # Delete both
        client.execute_command('DEL', 'partial_src', 'partial_dest')

        # Restore both
        client.execute_command('RESTORE', 'partial_src', 0, src_dump)
        client.execute_command('RESTORE', 'partial_dest', 0, dest_dump)

        # Verify restored state matches
        restored_src_info = self.ts_info('partial_src')
        restored_dest_info = self.ts_info('partial_dest')
        restored_src_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'partial_src')
        restored_dest_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'partial_dest')
        restored_dest_range = client.execute_command('TS.RANGE', 'partial_dest', '-', '+')

        # Verify digests match exactly
        assert restored_src_digest == original_src_digest, "Source digest mismatch after restore"
        assert restored_dest_digest == original_dest_digest, "Dest digest mismatch after restore"

        # Verify sample counts
        assert restored_src_info['totalSamples'] == original_src_info['totalSamples']
        assert restored_dest_info['totalSamples'] == original_dest_info['totalSamples']

        # Verify compaction destination data
        assert len(restored_dest_range) == len(original_dest_range)
        for orig, rest in zip(original_dest_range, restored_dest_range):
            first_value = float(orig[1])
            restored_value = float(rest[1])
            assert orig[0] == rest[0], f"Timestamp mismatch: {orig[0]} vs {rest[0]}"
            assert abs(first_value - restored_value) < 1e-9, f"Value mismatch: {orig[1]} vs {rest[1]}"

        # Now add more samples to the partial bucket and verify compaction continues correctly
        # Adding to bucket 3: [20000-30000)
        client.execute_command('TS.ADD', 'partial_src', 25000, 70.0)

        # Complete bucket 3 by adding sample in bucket 4
        client.execute_command('TS.ADD', 'partial_src', 31000, 80.0)

        # Verify compaction produced a correct result
        final_dest_range = client.execute_command('TS.RANGE', 'partial_dest', '-', '+')

        # Expected buckets after all compaction:
        # Bucket 1 [0-10000): sum = 10 + 20 + 30 = 60
        # Bucket 2 [10000-20000): sum = 40 + 50 = 90
        # Bucket 3 [20000-30000): sum = 60 + 70 = 130
        expected_sums = [60.0, 90.0, 130.0]

        assert len(final_dest_range) == 3, f"Expected 3 completed buckets, got {len(final_dest_range)}"

        for i, (ts, val) in enumerate(final_dest_range):
            val = float(val)
            expected = float(expected_sums[i])
            assert abs(val - expected) < 1e-9, \
                f"Bucket {i} sum mismatch: expected {expected_sums[i]}, got {val}"

    def test_dump_restore_empty_series(self):
        """Test DUMP/RESTORE works with empty timeseries"""
        client = self.client

        client.execute_command('TS.CREATE', 'empty_series',
                               'RETENTION', 3600000,
                               'CHUNK_SIZE', 256,
                               'DUPLICATE_POLICY', 'BLOCK',
                               'LABELS', 'status', 'empty')

        original_info = self.ts_info('empty_series')
        original_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'empty_series')

        dump_data = client.execute_command('DUMP', 'empty_series')
        client.execute_command('DEL', 'empty_series')
        client.execute_command('RESTORE', 'empty_series', 0, dump_data)

        restored_info = self.ts_info('empty_series')
        restored_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'empty_series')

        assert restored_digest == original_digest
        assert restored_info['totalSamples'] == 0
        assert restored_info['retentionTime'] == original_info['retentionTime']
        assert restored_info['chunkSize'] == original_info['chunkSize']
        assert restored_info['duplicatePolicy'] == original_info['duplicatePolicy']
        assert restored_info['labels'] == original_info['labels']

        # Verify series is functional after restore
        client.execute_command('TS.ADD', 'empty_series', 1000, 42.0)
        assert self.ts_info('empty_series')['totalSamples'] == 1

    def test_dump_restore_large_dataset(self):
        """Test DUMP/RESTORE with a large dataset spanning multiple chunks"""
        client = self.client

        client.execute_command('TS.CREATE', 'large_series',
                               'CHUNK_SIZE', 256,
                               'LABELS', 'size', 'large')

        # Add enough samples to span multiple chunks
        base_time = 1000000
        sample_count = 10000
        for i in range(sample_count):
            client.execute_command('TS.ADD', 'large_series', base_time + i * 100, i * 0.1)

        original_info = self.ts_info('large_series')
        original_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'large_series')

        # Verify we have multiple chunks
        assert original_info['chunkCount'] > 1

        dump_data = client.execute_command('DUMP', 'large_series')
        client.execute_command('DEL', 'large_series')
        client.execute_command('RESTORE', 'large_series', 0, dump_data)

        restored_info = self.ts_info('large_series')
        restored_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'large_series')

        assert restored_digest == original_digest
        assert restored_info['totalSamples'] == sample_count
        assert restored_info['chunkCount'] == original_info['chunkCount']
        assert restored_info['firstTimestamp'] == original_info['firstTimestamp']
        assert restored_info['lastTimestamp'] == original_info['lastTimestamp']

    def test_dump_restore_different_encodings(self):
        """Test DUMP/RESTORE with different chunk encodings"""
        client = self.client

        encodings = ['UNCOMPRESSED', 'COMPRESSED']

        for encoding in encodings:
            key = f'ts_{encoding.lower()}'

            client.execute_command('TS.CREATE', key,
                                   'ENCODING', encoding,
                                   'LABELS', 'encoding', encoding)

            # Add data
            for i in range(500):
                client.execute_command('TS.ADD', key, 1000 + i * 100, i * 1.5)

            original_info = self.ts_info(key)
            original_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', key)

            dump_data = client.execute_command('DUMP', key)
            client.execute_command('DEL', key)
            client.execute_command('RESTORE', key, 0, dump_data)

            restored_info = self.ts_info(key)
            restored_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', key)

            assert restored_digest == original_digest, f"Digest mismatch for {encoding}"
            assert restored_info['totalSamples'] == original_info['totalSamples']

    def test_dump_restore_to_different_key(self):
        """Test DUMP/RESTORE to a different key name"""
        client = self.client

        client.execute_command('TS.CREATE', 'original_key',
                               'RETENTION', 3600000,
                               'LABELS', 'name', 'original')

        for i in range(100):
            client.execute_command('TS.ADD', 'original_key', 1000 + i * 100, i)

        original_info = self.ts_info('original_key')
        original_range = client.execute_command('TS.RANGE', 'original_key', '-', '+')

        dump_data = client.execute_command('DUMP', 'original_key')

        # Restore to a different key
        client.execute_command('RESTORE', 'new_key', 0, dump_data)

        # Verify both keys exist with the same data
        new_info = self.ts_info('new_key')
        new_range = client.execute_command('TS.RANGE', 'new_key', '-', '+')

        assert new_info['totalSamples'] == original_info['totalSamples']
        assert new_info['retentionTime'] == original_info['retentionTime']
        assert len(new_range) == len(original_range)

        for orig, new in zip(original_range, new_range):
            assert orig[0] == new[0]
            assert float(orig[1]) == float(new[1])

    def test_dump_restore_with_rounding(self):
        """Test DUMP/RESTORE preserves rounding configuration"""
        client = self.client

        # Test with significant digits rounding
        client.execute_command('TS.CREATE', 'ts_rounding',
                               'SIGNIFICANT_DIGITS', 3,
                               'LABELS', 'rounding', 'enabled')

        # Add samples with many decimal places
        client.execute_command('TS.ADD', 'ts_rounding', 1000, 3.14159265359)
        client.execute_command('TS.ADD', 'ts_rounding', 2000, 2.71828182845)

        original_info = self.ts_info('ts_rounding')
        original_range = client.execute_command('TS.RANGE', 'ts_rounding', '-', '+')
        original_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'ts_rounding')

        dump_data = client.execute_command('DUMP', 'ts_rounding')
        client.execute_command('DEL', 'ts_rounding')
        client.execute_command('RESTORE', 'ts_rounding', 0, dump_data)

        restored_info = self.ts_info('ts_rounding')
        restored_range = client.execute_command('TS.RANGE', 'ts_rounding', '-', '+')
        restored_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'ts_rounding')

        assert restored_digest == original_digest
        assert len(restored_range) == len(original_range)

        for orig, rest in zip(original_range, restored_range):
            assert orig[0] == rest[0]
            assert float(orig[1]) == float(rest[1])

    def test_dump_restore_full_compaction_state(self):
        """Test DUMP/RESTORE preserves the full compaction ecosystem state"""
        client = self.client

        # Create a complete compaction setup
        client.execute_command('TS.CREATE', 'metrics_raw',
                               'RETENTION', 3600000,
                               'LABELS', 'metric', 'cpu', 'level', 'raw')

        # Multiple aggregation destinations
        agg_configs = [
            ('metrics_1m_avg', 'avg', 60000),
            ('metrics_1m_max', 'max', 60000),
            ('metrics_5m_avg', 'avg', 300000),
            ('metrics_5m_sum', 'sum', 300000),
        ]

        for dest, agg, bucket in agg_configs:
            client.execute_command('TS.CREATE', dest,
                                   'RETENTION', 86400000,
                                   'LABELS', 'metric', 'cpu', 'level', 'aggregated')
            client.execute_command('TS.CREATERULE', 'metrics_raw', dest,
                                   'AGGREGATION', agg, bucket)

        # Generate data
        base_time = 1000000
        for i in range(600):  # 10 minutes of data at 1-second intervals
            client.execute_command('TS.ADD', 'metrics_raw', base_time + i * 1000, 50 + (i % 50))

        time.sleep(0.2)  # Allow compactions

        # Capture the original state for all series
        original_states = {}
        all_keys = ['metrics_raw'] + [c[0] for c in agg_configs]

        for key in all_keys:
            original_states[key] = {
                'info': self.ts_info(key),
                'digest': client.execute_command('DEBUG', 'DIGEST-VALUE', key),
                'range': client.execute_command('TS.RANGE', key, '-', '+'),
            }

        # DUMP all series
        dumps = {key: client.execute_command('DUMP', key) for key in all_keys}

        # Delete all
        for key in all_keys:
            client.execute_command('DEL', key)

        # Restore all
        for key in all_keys:
            client.execute_command('RESTORE', key, 0, dumps[key])

        # Verify all series
        for key in all_keys:
            restored_info = self.ts_info(key)
            restored_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', key)
            restored_range = client.execute_command('TS.RANGE', key, '-', '+')

            orig = original_states[key]

            assert restored_digest == orig['digest'], f"Digest mismatch for {key}"
            assert restored_info['totalSamples'] == orig['info']['totalSamples'], f"Sample count mismatch for {key}"
            assert len(restored_range) == len(orig['range']), f"Range length mismatch for {key}"

        # Verify compaction rules on source
        restored_raw_info = self.ts_info('metrics_raw')
        assert len(restored_raw_info['rules']) == len(agg_configs)

    def test_dump_restore_replace_existing(self):
        """Test RESTORE with REPLACE option overwrites existing key"""
        client = self.client

        # Create the first series
        client.execute_command('TS.CREATE', 'ts_replace',
                               'LABELS', 'version', '1')
        client.execute_command('TS.ADD', 'ts_replace', 1000, 10.0)

        first_dump = client.execute_command('DUMP', 'ts_replace')

        # Modify the series
        client.execute_command('TS.ADD', 'ts_replace', 2000, 20.0)
        client.execute_command('TS.ADD', 'ts_replace', 3000, 30.0)

        # Restore the original state with REPLACE
        client.execute_command('RESTORE', 'ts_replace', 0, first_dump, 'REPLACE')

        restored_info = self.ts_info('ts_replace')
        restored_range = client.execute_command('TS.RANGE', 'ts_replace', '-', '+')

        # Should have only 1 sample from the first dump
        assert restored_info['totalSamples'] == 1
        assert len(restored_range) == 1
        assert restored_range[0][0] == 1000
