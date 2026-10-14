import time
from valkeytestframework.util.waiters import *
from valkeytestframework.valkey_test_case import ValkeyAction
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


class TestTimeseriesAofRewrite(ValkeyTimeSeriesTestCaseBase):

    def test_basic_timeseries_aofrewrite_and_restore(self):
        """Test basic timeseries AOF rewrite and restore functionality"""
        client = self.client
        client.config_set('appendonly', 'yes')
        # Wait for any initial AOF rewrite to complete
        wait_for_equal(lambda: client.info('persistence')['aof_rewrite_in_progress'], 0, timeout=30)
        # Create a basic timeseries with some properties
        client.execute_command('TS.CREATE', 'basic_ts',
                               'RETENTION', 10000,
                               'CHUNK_SIZE', 256,
                               'DUPLICATE_POLICY', 'LAST',
                               'LABELS', 'sensor', 'temperature', 'location', 'room1')

        # Add some data points
        timestamps = [1000, 2000, 3000, 4000, 5000]
        values = [20.5, 21.0, 20.8, 22.1, 21.5]
        for ts, val in zip(timestamps, values):
            client.execute_command('TS.ADD', 'basic_ts', ts, val)

        # Get original info
        original_info = self.ts_info('basic_ts')
        original_digest = client.execute_command('DEBUG', 'DIGEST')
        original_object_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'basic_ts')

        # Perform AOF rewrite
        client.bgrewriteaof()
        self.server.wait_for_action_done(ValkeyAction.AOF_REWRITE)
        time.sleep(1)  # Allow time for completion

        # Restart server
        # Add appendonly to server args so it loads AOF on restart
        self.server.args['appendonly'] = 'yes'
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()

        # Verify digests match
        restored_digest = client.execute_command('DEBUG', 'DIGEST')
        restored_object_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'basic_ts')
        assert restored_digest == original_digest
        assert restored_object_digest == original_object_digest

        # Verify all properties are preserved
        restored_info = self.ts_info('basic_ts')

        # Check basic properties
        assert restored_info['totalSamples'] == original_info['totalSamples']
        # assert restored_info['memoryUsage'] == original_info['memoryUsage']
        assert restored_info['retentionTime'] == original_info['retentionTime']
        assert restored_info['chunkSize'] == original_info['chunkSize']
        assert restored_info['duplicatePolicy'] == original_info['duplicatePolicy']
        assert restored_info['labels'] == original_info['labels']

        # Verify data integrity
        range_result = client.execute_command('TS.RANGE', 'basic_ts', '-', '+')
        assert len(range_result) == 5
        for i, (ts, val) in enumerate(range_result):
            assert ts == timestamps[i]
            assert float(val) == values[i]

        client.execute_command('DEL', 'basic_ts')

    def test_timeseries_with_compaction_rules_aofrewrite(self):
        """Test AOF rewrite for timeseries with compaction rules"""
        client = self.client
        client.config_set('appendonly', 'yes')
        # Wait for any initial AOF rewrite to complete
        wait_for_equal(lambda: client.info('persistence')['aof_rewrite_in_progress'], 0, timeout=30)
        # Create source timeseries with longer retention to avoid data loss during test
        client.execute_command('TS.CREATE', 'source_ts',
                               'RETENTION', 200000,  # Increased from 20000 to avoid retention trimming
                               'LABELS', 'type', 'raw_data', 'metric', 'cpu_usage')

        # Create destination timeseries for compaction
        client.execute_command('TS.CREATE', 'avg_1min',
                               'RETENTION', 600000,  # Increased from 60000
                               'LABELS', 'type', 'aggregated', 'metric', 'cpu_usage', 'interval', '1min')

        client.execute_command('TS.CREATE', 'max_5min',
                               'RETENTION', 3000000,  # Increased from 300000
                               'LABELS', 'type', 'aggregated', 'metric', 'cpu_usage', 'interval', '5min')

        # Create compaction rules
        client.execute_command('TS.CREATERULE', 'source_ts', 'avg_1min', 'AGGREGATION', 'avg', 60000)
        client.execute_command('TS.CREATERULE', 'source_ts', 'max_5min', 'AGGREGATION', 'max', 300000)

        # Add data to trigger compactions
        base_time = 1000000
        for i in range(100):
            client.execute_command('TS.ADD', 'source_ts', base_time + i * 1000, 50 + (i % 20))

        # Wait for compactions to process
        time.sleep(0.1)

        # Get the original state
        original_source_info = self.ts_info('source_ts')
        original_avg_info = self.ts_info('avg_1min')
        original_max_info = self.ts_info('max_5min')

        # Digests for every series, the source included: its rule state (open bucket, accumulator)
        # is part of the RDB payload the rewrite embeds, so it round-trips too.
        original_source_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'source_ts')
        original_avg_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'avg_1min')
        original_max_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'max_5min')

        # Perform AOF rewrite
        client.bgrewriteaof()
        self.server.wait_for_action_done(ValkeyAction.AOF_REWRITE)
        time.sleep(1)

        # Restart server
        # Add appendonly to server args so it loads AOF on restart
        self.server.args['appendonly'] = 'yes'
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()

        # Verify all timeseries are restored with correct properties
        restored_source_info = self.ts_info('source_ts')
        restored_avg_info = self.ts_info('avg_1min')
        restored_max_info = self.ts_info('max_5min')

        restored_source_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'source_ts')
        restored_avg_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'avg_1min')
        restored_max_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'max_5min')

        assert restored_source_digest == original_source_digest, "source_ts digest mismatch"
        assert restored_avg_digest == original_avg_digest, "avg_1min digest mismatch"
        assert restored_max_digest == original_max_digest, "max_5min digest mismatch"

        # Check source timeseries properties
        assert restored_source_info['totalSamples'] == original_source_info['totalSamples']
        assert restored_source_info['labels'] == original_source_info['labels']
        assert restored_source_info['retentionTime'] == original_source_info['retentionTime']

        # Check compaction destination properties
        assert restored_avg_info['totalSamples'] == original_avg_info['totalSamples']
        assert restored_avg_info['labels'] == original_avg_info['labels']
        assert restored_max_info['totalSamples'] == original_max_info['totalSamples']
        assert restored_max_info['labels'] == original_max_info['labels']

        # Verify compaction rules are preserved
        assert len(restored_source_info['rules']) == 2
        rule_destinations = [rule.dest_key for rule in restored_source_info['rules']]
        assert 'avg_1min' in rule_destinations
        assert 'max_5min' in rule_destinations

        # Clean up
        client.execute_command('DEL', 'source_ts', 'avg_1min', 'max_5min')

    def test_timeseries_complex_properties_aofrewrite(self):
        """Test AOF rewrite for timeseries with various complex properties"""
        client = self.client
        client.config_set('appendonly', 'yes')
        # Wait for any initial AOF rewrite to complete
        wait_for_equal(lambda: client.info('persistence')['aof_rewrite_in_progress'], 0, timeout=30)

        # Create timeseries with all possible properties
        client.execute_command('TS.CREATE', 'complex_ts',
                               'RETENTION', 86400000,  # 1 day
                               'CHUNK_SIZE', 4096,
                               'DUPLICATE_POLICY', 'MIN',
                               'LABELS', 'service', 'web-server', 'datacenter', 'us-east', 'version', '2.1')

        # Add varied data
        base_time = 1609459200000  # 2021-01-01 00:00:00 UTC
        for i in range(1000):
            timestamp = base_time + i * 60000  # Every minute
            value = 100 + (i % 50) * 2.5 - 25  # Varying values
            client.execute_command('TS.ADD', 'complex_ts', timestamp, value)

        # Get the original state
        original_info = self.ts_info('complex_ts', debug=True)
        original_range = client.execute_command('TS.RANGE', 'complex_ts', '-', '+')
        original_digest = client.execute_command('DEBUG', 'DIGEST')

        # Perform AOF rewrite
        client.bgrewriteaof()
        self.server.wait_for_action_done(ValkeyAction.AOF_REWRITE)
        time.sleep(1)

        # Restart server
        # Add appendonly to server args so it loads AOF on restart
        self.server.args['appendonly'] = 'yes'
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()

        # Verify complete restoration
        restored_info = self.ts_info('complex_ts', debug=True)
        restored_range = client.execute_command('TS.RANGE', 'complex_ts', '-', '+')
        restored_digest = client.execute_command('DEBUG', 'DIGEST')

        assert restored_digest == original_digest

        # Verify all properties match exactly
        critical_properties = ['totalSamples', 'firstTimestamp',
                               'lastTimestamp', 'retentionTime', 'chunkSize',
                               'duplicatePolicy', 'labels']

        for prop in critical_properties:
            if prop in original_info and prop in restored_info:
                assert restored_info[prop] == original_info[prop], f"Property {prop} mismatch"

        # Verify data integrity
        assert len(restored_range) == len(original_range)
        assert restored_range == original_range

        client.execute_command('DEL', 'complex_ts')

    def test_multiple_timeseries_aofrewrite(self):
        """Test AOF rewrite with multiple timeseries having different properties"""
        client = self.client
        client.config_set('appendonly', 'yes')
        # Wait for any initial AOF rewrite to complete
        wait_for_equal(lambda: client.info('persistence')['aof_rewrite_in_progress'], 0, timeout=30)

        # Create multiple timeseries with different configurations
        timeseries_configs = [
            ('ts1', {'RETENTION': 60000, 'DUPLICATE_POLICY': 'LAST', 'LABELS': ['type', 'cpu']}),
            ('ts2',
             {'RETENTION': 120000, 'DUPLICATE_POLICY': 'FIRST', 'CHUNK_SIZE': 512, 'LABELS': ['type', 'memory']}),
            ('ts3', {'RETENTION': 300000, 'DUPLICATE_POLICY': 'MAX', 'LABELS': ['type', 'disk', 'unit', 'GB']}),
            ('ts4', {'LABELS': ['type', 'network', 'interface', 'eth0']}),  # No retention specified
        ]

        original_infos = {}
        original_data = {}

        for ts_name, config in timeseries_configs:
            # Build command
            cmd = ['TS.CREATE', ts_name]
            if 'RETENTION' in config:
                cmd.extend(['RETENTION', config['RETENTION']])
            if 'CHUNK_SIZE' in config:
                cmd.extend(['CHUNK_SIZE', config['CHUNK_SIZE']])
            if 'DUPLICATE_POLICY' in config:
                cmd.extend(['DUPLICATE_POLICY', config['DUPLICATE_POLICY']])
            if 'LABELS' in config:
                cmd.append('LABELS')
                cmd.extend(config['LABELS'])

            client.execute_command(*cmd)

            # Add some data
            data_points = []
            for i in range(10):
                ts = 1000 + i * 1000
                val = (hash(ts_name + str(i)) % 1000) / 10.0
                client.execute_command('TS.ADD', ts_name, ts, val)
                data_points.append((ts, val))

            original_infos[ts_name] = self.ts_info(ts_name)
            original_data[ts_name] = data_points

        # Get overall digest
        original_digest = client.execute_command('DEBUG', 'DIGEST')

        # Perform AOF rewrite
        client.bgrewriteaof()
        self.server.wait_for_action_done(ValkeyAction.AOF_REWRITE)
        time.sleep(1)

        # Restart server
        # Add appendonly to server args so it loads AOF on restart
        self.server.args['appendonly'] = 'yes'
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()

        # Verify everything is restored correctly
        restored_digest = client.execute_command('DEBUG', 'DIGEST')
        assert restored_digest == original_digest

        for ts_name, config in timeseries_configs:
            restored_info = self.ts_info(ts_name)
            original_info = original_infos[ts_name]

            # Verify key properties
            assert restored_info['totalSamples'] == original_info['totalSamples']
            assert restored_info['labels'] == original_info['labels']
            if 'RETENTION' in config:
                assert restored_info['retentionTime'] == config['RETENTION']
            if 'DUPLICATE_POLICY' in config:
                assert restored_info['duplicatePolicy'] == config['DUPLICATE_POLICY'].lower()

            # Verify data
            restored_range = client.execute_command('TS.RANGE', ts_name, '-', '+')
            expected_data = original_data[ts_name]
            assert len(restored_range) == len(expected_data)

            for i, (ts, val) in enumerate(restored_range):
                assert ts == expected_data[i][0]
                assert abs(float(val) - expected_data[i][1]) < 0.001

        # Clean up
        for ts_name, _ in timeseries_configs:
            client.execute_command('DEL', ts_name)

    def test_timeseries_aofrewrite_with_expiration(self):
        """Test AOF rewrite with timeseries that have TTL"""
        client = self.client
        client.config_set('appendonly', 'yes')
        # Wait for any initial AOF rewrite to complete
        wait_for_equal(lambda: client.info('persistence')['aof_rewrite_in_progress'], 0, timeout=30)

        # Create timeseries and set TTL
        client.execute_command('TS.CREATE', 'expiring_ts',
                               'RETENTION', 10000,
                               'LABELS', 'temp', 'test')

        # Add data
        client.execute_command('TS.ADD', 'expiring_ts', 1000, 42.0)
        client.execute_command('TS.ADD', 'expiring_ts', 2000, 43.0)

        # Set expiration
        client.execute_command('EXPIRE', 'expiring_ts', 30)  # 30 seconds

        original_ttl = client.execute_command('TTL', 'expiring_ts')
        original_info = self.ts_info('expiring_ts')
        original_digest = client.execute_command('DEBUG', 'DIGEST')

        # Perform AOF rewrite
        client.bgrewriteaof()
        self.server.wait_for_action_done(ValkeyAction.AOF_REWRITE)
        time.sleep(1)

        # Restart server
        # Add appendonly to server args so it loads AOF on restart
        self.server.args['appendonly'] = 'yes'
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()

        # Verify restoration including TTL
        restored_digest = client.execute_command('DEBUG', 'DIGEST')
        assert restored_digest == original_digest

        restored_info = self.ts_info('expiring_ts')
        restored_ttl = client.execute_command('TTL', 'expiring_ts')

        # Verify properties
        assert restored_info['totalSamples'] == original_info['totalSamples']
        assert restored_info['labels'] == original_info['labels']

        # TTL should be preserved (allowing for some time drift)
        assert restored_ttl > 0  # Should still be positive
        assert abs(restored_ttl - original_ttl) <= 2  # Allow 2-second drift

        client.execute_command('DEL', 'expiring_ts')

    def test_empty_timeseries_aofrewrite(self):
        """Test AOF rewrite with empty timeseries (no data points)"""
        client = self.client
        client.config_set('appendonly', 'yes')
        # Wait for any initial AOF rewrite to complete
        wait_for_equal(lambda: client.info('persistence')['aof_rewrite_in_progress'], 0, timeout=30)

        # Create empty timeseries with various properties
        client.execute_command('TS.CREATE', 'empty_ts',
                               'RETENTION', 5000,
                               'CHUNK_SIZE', 128,
                               'DUPLICATE_POLICY', 'SUM',
                               'LABELS', 'status', 'empty', 'test', 'case')

        original_info = self.ts_info('empty_ts')
        original_digest = client.execute_command('DEBUG', 'DIGEST')

        # Perform AOF rewrite
        client.bgrewriteaof()
        self.server.wait_for_action_done(ValkeyAction.AOF_REWRITE)
        time.sleep(1)

        # Restart server
        # Add appendonly to server args so it loads AOF on restart
        self.server.args['appendonly'] = 'yes'
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()

        # Verify empty timeseries is properly restored
        restored_digest = client.execute_command('DEBUG', 'DIGEST')
        assert restored_digest == original_digest

        restored_info = self.ts_info('empty_ts')

        # Verify all properties are preserved
        assert restored_info['totalSamples'] == 0
        assert restored_info['totalSamples'] == original_info['totalSamples']
        assert restored_info['retentionTime'] == original_info['retentionTime']
        assert restored_info['chunkSize'] == original_info['chunkSize']
        assert restored_info['duplicatePolicy'] == original_info['duplicatePolicy']
        assert restored_info['labels'] == original_info['labels']

        # Verify it's still functional
        client.execute_command('TS.ADD', 'empty_ts', 1000, 1.0)
        new_info = self.ts_info('empty_ts')
        assert new_info['totalSamples'] == 1

        client.execute_command('DEL', 'empty_ts')
