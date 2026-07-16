import pytest
import valkey
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTimeseriesInfo(ValkeyTimeSeriesTestCaseBase):
    def test_info_basic(self):
        """Test TS.INFO on a basic time series with data"""
        key = 'ts_basic'
        self.client.execute_command('TS.CREATE', key)
        ts1 = self.client.execute_command('TS.ADD', key, 1000, 10.1)
        ts2 = self.client.execute_command('TS.ADD', key, 2000, 20.2)

        info = self.ts_info(key, True)
        labels = info['labels']

        assert info['totalSamples'] == 2
        assert info['memoryUsage'] > 0
        assert info['retentionTime'] == 0  # Default
        assert len(info['chunks']) >= 1
        assert info['firstTimestamp'] == ts1
        assert info['lastTimestamp'] == ts2
        assert labels is None or len(labels) == 0
        # assert info['rules'] == []
        assert info['duplicatePolicy'] == "block"  # default

    def test_info_with_options(self):
        """Test TS.INFO on a time series created with options"""
        key = 'ts_options'
        retention = 60000  # 1 minute
        chunk_size = 128
        labels = ['sensor', 'temp', 'area', 'A1']
        duplicate_policy = 'last'

        self.client.execute_command(
            'TS.CREATE', key,
            'RETENTION', retention,
            'CHUNK_SIZE', chunk_size,
            'DUPLICATE_POLICY', duplicate_policy,
            'LABELS', *labels,
        )
        ts1 = self.client.execute_command('TS.ADD', key, 3000, 30.3)

        info = self.ts_info(key, True)
        labels = info['labels']

        assert info['totalSamples'] == 1
        assert info['retentionTime'] == retention
        assert len(info['chunks']) == 1
        assert info['chunkSize'] == chunk_size
        assert info['firstTimestamp'] == ts1
        assert info['lastTimestamp'] == ts1
        # assert info['rules'] == []
        assert info['duplicatePolicy'] == duplicate_policy
        assert labels['sensor'] == 'temp'
        assert labels['area'] == 'A1'

    def test_info_empty_series(self):
        """Test TS.INFO on an existing but empty time series"""
        key = 'ts_empty'
        self.client.execute_command('TS.CREATE', key, 'LABELS', 'status', 'init')

        info = self.ts_info(key, True)
        labels = info['labels']
        print(info)

        assert info['totalSamples'] == 0
        assert info['memoryUsage'] > 0  # Metadata still uses memory
        assert info['retentionTime'] == 0
        assert info['chunks'] == []  # No data chunks yet
        assert info['firstTimestamp'] == 0
        assert info['lastTimestamp'] == 0
        assert labels['status'] == 'init'
        # assert info['rules'] == []
        assert info['duplicatePolicy'] == "block"  # default
        assert labels['status'] == 'init'  # No data chunks yet

    def test_info_non_existent_key(self):
        """Test TS.INFO on a non-existent key"""
        with pytest.raises(ResponseError, match="key does not exist"):
            self.client.execute_command('TS.INFO', 'ts_nonexistent')

    def test_info_wrong_type(self):
        """Test TS.INFO on a key of the wrong type"""
        key = 'string_key'
        self.client.set(key, 'hello world')

        with pytest.raises(ResponseError, match="WRONGTYPE Operation"):
            self.client.execute_command('TS.INFO', key)

    def test_info_debug(self):
        """Test TS.INFO DEBUG option"""
        key = 'ts_debug'
        chunk_size = 128  # Use a smaller chunk size for easier testing
        self.client.execute_command('TS.CREATE', key, 'CHUNK_SIZE', chunk_size)

        # Add enough samples to potentially create more than one chunk
        for i in range(int(chunk_size * 1.5)):
            self.client.execute_command('TS.ADD', key, 1000 + i * 10, i)

        info = self.ts_info(key, True)

        assert 'chunks' in info
        assert isinstance(info['chunks'], list)
        assert len(info['chunks']) >= 1  # Should have at least one chunk

        # Check details of the first chunk (keys might vary slightly)
        first_chunk = info['chunks'][0]
        assert 'startTimestamp' in first_chunk
        assert 'endTimestamp' in first_chunk
        assert 'samples' in first_chunk
        assert 'size' in first_chunk
        assert first_chunk['samples'] > 0
        assert first_chunk['size'] > 0

    def test_info_reflects_single_rule_creation(self):
        """Test that TS.INFO shows rules after TS.CREATERULE"""
        source_key = 'ts_source'
        dest_key = 'ts_dest'

        # Create source and destination series
        self.client.execute_command('TS.CREATE', source_key)
        self.client.execute_command('TS.CREATE', dest_key)

        # Verify no rules initially
        source_info = self.ts_info(source_key, True)
        dest_info = self.ts_info(dest_key, True)
        assert 'rules' not in source_info or len(source_info['rules']) == 0
        # sourceKey is always present, nil for a non-compaction series
        assert dest_info['sourceKey'] is None

        # Create a compaction rule
        self.client.execute_command(
            'TS.CREATERULE', source_key, dest_key,
            'AGGREGATION', 'avg', '60000'  # 1 minute buckets
        )

        # Verify rule appears in source TS.INFO
        source_info = self.ts_info(source_key, True)
        assert 'rules' in source_info
        assert len(source_info['rules']) == 1

        rule = source_info['rules'][0]
        assert rule.dest_key == dest_key  # destination key
        assert rule.bucket_duration == 60000  # bucket duration
        # TS.INFO reports the rule aggregator uppercase, matching RedisTimeSeries.
        assert rule.aggregation == 'AVG'  # aggregation type

        # Verify destination shows source key
        dest_info = self.ts_info(dest_key, True)
        assert 'sourceKey' in dest_info
        assert dest_info['sourceKey'] == source_key

    def test_info_reflects_various_rule_aggregation_types(self):
        """Test that TS.INFO correctly shows different aggregation types"""
        source_key = 'ts_agg_source'
        align_ts = 1000

        aggregations = [
            ('avg', 'ts_avg'),
            ('sum', 'ts_sum'),
            ('min', 'ts_min'),
            ('max', 'ts_max'),
            ('count', 'ts_count'),
            ('first', 'ts_first'),
            ('last', 'ts_last'),
            ('std.p', 'ts_stdp'),
            ('std.s', 'ts_stds'),
            ('var.p', 'ts_varp'),
            ('var.s', 'ts_vars')
        ]

        # Create a source series
        self.client.execute_command('TS.CREATE', source_key)

        # Create destination series and rules
        for agg_type, dest_key in aggregations:
            self.client.execute_command('TS.CREATE', dest_key)
            self.client.execute_command(
                'TS.CREATERULE', source_key, dest_key,
                'AGGREGATION', agg_type, '60000', align_ts
            )

        # Verify all aggregation types appear correctly
        source_info = self.ts_info(source_key, True)
        assert 'rules' in source_info
        rules = source_info['rules']
        assert len(rules) == len(aggregations)
        print(rules)

        for rule in rules:
            # TS.INFO reports the aggregator uppercase (matching RedisTimeSeries),
            # while `aggregations` lists them lowercase; compare case-insensitively.
            aggr = list(filter(lambda item: item[0].lower() == rule.aggregation.lower(), aggregations))
            assert len(aggr) == 1
            dest_key = aggr[0][1]
            assert rule.dest_key == dest_key
            assert rule.alignment == align_ts
            assert rule.bucket_duration == 60000

    def test_info_after_rule_deletion(self):
        """Test that TS.INFO correctly updates after TS.DELETERULE"""
        source_key = 'ts_del_source'
        dest_key = 'ts_del_dest'

        # Create series and rule
        self.client.execute_command('TS.CREATE', source_key)
        self.client.execute_command('TS.CREATE', dest_key)
        self.client.execute_command(
            'TS.CREATERULE', source_key, dest_key,
            'AGGREGATION', 'avg', '60000'
        )

        # Verify rule exists
        source_info = self.ts_info(source_key, True)
        assert 'rules' in source_info
        assert len(source_info['rules']) == 1

        dest_info = self.ts_info(dest_key, True)
        assert 'sourceKey' in dest_info
        assert dest_info['sourceKey'] == source_key

        # Delete the rule
        self.client.execute_command('TS.DELETERULE', source_key, dest_key)

        # Verify rule is removed from source
        source_info = self.ts_info(source_key, True)
        assert 'rules' not in source_info or len(source_info['rules']) == 0

        # Verify source reference is removed from destination
        # (sourceKey is always present, nil once the series is no longer a compaction)
        dest_info = self.ts_info(dest_key, True)
        assert dest_info['sourceKey'] is None

    def _resp3_client(self):
        """A RESP3 (HELLO 3) client against the same server as self.client."""
        return valkey.Valkey(
            host=self.server.bind_ip, port=self.server.port, protocol=3
        )

    def test_info_resp3_labels_and_rules_are_maps(self):
        """RESP3 renders `labels` and `rules` as native maps, matching RedisTimeSeries.

        In RESP2 both are arrays (labels: [[name, value], ...]; rules:
        [[destKey, bucket, agg, align], ...]). In RESP3 they are maps
        (labels: {name: value}; rules: {destKey: [bucket, agg, align]}).
        """
        c3 = self._resp3_client()

        src = 'ts_resp3_src'
        dst = 'ts_resp3_dst'
        c3.execute_command('TS.CREATE', src, 'LABELS', 'sensor', 's1', 'area', 'us-east')
        c3.execute_command('TS.CREATE', dst)
        c3.execute_command('TS.CREATERULE', src, dst, 'AGGREGATION', 'avg', 1000)

        src_info = c3.execute_command('TS.INFO', src)
        assert isinstance(src_info, dict), f"RESP3 TS.INFO should be a map, got {type(src_info)}"

        labels = src_info[b'labels']
        assert isinstance(labels, dict), f"RESP3 labels should be a map, got {labels!r}"
        assert labels == {b'sensor': b's1', b'area': b'us-east'}

        rules = src_info[b'rules']
        assert isinstance(rules, dict), f"RESP3 rules should be a map, got {rules!r}"
        assert list(rules.keys()) == [dst.encode()]
        bucket, agg, align = rules[dst.encode()]
        assert bucket == 1000
        assert agg == b'AVG'  # aggregator reported uppercase, matching RedisTimeSeries
        assert align == 0

        # A label-less / rule-less series yields empty maps, not nil.
        dst_info = c3.execute_command('TS.INFO', dst)
        assert dst_info[b'labels'] == {}
        assert dst_info[b'rules'] == {}
        assert dst_info[b'sourceKey'] == src.encode()

    def test_info_debug_bytes_per_sample_is_double(self):
        """Per-chunk `bytesPerSample` is replied as a double, matching
        RedisTimeSeries (ReplyWithDouble): native double on RESP3, numeric
        bulk string on RESP2."""
        key = 'ts_dbg_bps'
        self.client.execute_command('TS.CREATE', key)
        self.client.execute_command('TS.ADD', key, 100, 1.5)

        # RESP2: a bulk string carrying a numeric value
        raw = self.client.execute_command('TS.INFO', key, 'DEBUG')
        d = dict(zip(raw[::2], raw[1::2]))
        chunk = d[b'Chunks'][0]
        c = dict(zip(chunk[::2], chunk[1::2]))
        bps = c[b'bytesPerSample']
        assert isinstance(bps, bytes), f"RESP2 bytesPerSample should be a bulk string, got {bps!r}"
        assert float(bps) > 0

        # RESP3: a native double
        c3 = self._resp3_client()
        info3 = c3.execute_command('TS.INFO', key, 'DEBUG')
        chunk3 = info3[b'Chunks'][0]
        assert isinstance(chunk3, dict)
        bps3 = chunk3[b'bytesPerSample']
        assert isinstance(bps3, float), f"RESP3 bytesPerSample should be a double, got {bps3!r}"
        assert bps3 > 0

    def test_info_resp2_labels_and_rules_stay_arrays(self):
        """RESP2 keeps the array shapes for `labels` and `rules` (unchanged)."""
        src = 'ts_resp2_src'
        dst = 'ts_resp2_dst'
        self.client.execute_command('TS.CREATE', src, 'LABELS', 'sensor', 's1')
        self.client.execute_command('TS.CREATE', dst)
        self.client.execute_command('TS.CREATERULE', src, dst, 'AGGREGATION', 'avg', 1000)

        src_info = self.client.execute_command('TS.INFO', src)
        d = dict(zip(src_info[::2], src_info[1::2]))
        assert d[b'labels'] == [[b'sensor', b's1']]
        # Aggregator reported uppercase, matching RedisTimeSeries.
        assert d[b'rules'] == [[dst.encode(), 1000, b'AVG', 0]]

        dst_info = self.client.execute_command('TS.INFO', dst)
        d = dict(zip(dst_info[::2], dst_info[1::2]))
        assert d[b'labels'] == []
        assert d[b'rules'] == []
