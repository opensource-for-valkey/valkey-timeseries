import pytest
import math
from valkey import ResponseError
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase, CompactionRule
from valkeytestframework.conftest import resource_port_tracker


class TestTimeseriesAdd(ValkeyTimeSeriesTestCaseBase):
    def test_basic_add(self):
        """Test basic TS.ADD command functionality"""
        # Create a timeseries
        assert self.client.execute_command("TS.CREATE", "ts1") == b"OK"

        # Add a sample
        timestamp = 16000
        value = 10.5
        result = self.client.execute_command("TS.ADD", "ts1", timestamp, value)
        assert result == timestamp

        # Verify the sample was added
        samples = self.client.execute_command("TS.RANGE", "ts1", "-", "+")
        assert samples == [[timestamp, b'10.5']]

    def test_add_with_labels(self):
        """Test adding samples with labels"""
        # Create timeseries with labels
        self.client.execute_command("TS.CREATE", "ts2", "LABELS", "sensor", "temp", "location", "room1")

        # Add a sample
        timestamp = 160000
        result = self.client.execute_command("TS.ADD", "ts2", timestamp, 22.5)
        assert result == timestamp

        # Verify the labels were preserved
        info = self.ts_info("ts2")
        labels = info["labels"]
        assert labels["sensor"] == "temp"
        assert labels["location"] == "room1"

    def test_add_auto_timestamp(self):
        """Test TS.ADD with '*' automatic timestamp"""
        self.client.execute_command("TS.CREATE", "ts3")

        # Add a sample with an automatic timestamp
        result = self.client.execute_command("TS.ADD", "ts3", "*", 33.5)

        # Verify a timestamp was generated (should be a recent timestamp)
        assert isinstance(result, int)
        assert result > 160000  # Some time after 2020

        # Verify the sample was added
        samples = self.client.execute_command("TS.RANGE", "ts3", "-", "+")
        assert samples == [[result, b'33.5']]

    def test_add_creates_key(self):
        """Test TS.ADD creates a new timeseries if it doesn't exist"""
        # Add to a non-existent timeseries
        timestamp = 160000
        result = self.client.execute_command("TS.ADD", "ts_auto_create", timestamp, 44.5)
        assert result == timestamp

        # Verify the timeseries was created
        assert self.client.execute_command("EXISTS", "ts_auto_create") == 1

        # Verify the sample was added
        samples = self.client.execute_command("TS.RANGE", "ts_auto_create", "-", "+")

        assert samples == [[timestamp, b'44.5']]

    def test_add_with_retention(self):
        """Test TS.ADD with a retention option"""
        # Add to a non-existent timeseries with retention
        timestamp = 160000
        retention = 10000  # 10 seconds
        result = self.client.execute_command(
            "TS.ADD", "ts_retention", timestamp, 55.5, "RETENTION", retention
        )
        assert result == timestamp

        # Verify the retention was set
        info = self.ts_info("ts_retention")
        assert int(info["retentionTime"]) == retention

    def test_add_with_encoding(self):
        """Test TS.ADD with different encoding options"""
        # Test with UNCOMPRESSED encoding
        timestamp = 160000
        self.client.execute_command(
            "TS.ADD", "ts_uncompressed", timestamp, 66.5, "ENCODING", "UNCOMPRESSED"
        )

        info = self.ts_info("ts_uncompressed")
        assert info["chunkType"] == "uncompressed"

        # Test with COMPRESSED encoding
        self.client.execute_command(
            "TS.ADD", "ts_compressed", timestamp, 77.5, "ENCODING", "COMPRESSED"
        )

        info = self.ts_info("ts_compressed")
        assert info["chunkType"] == "compressed"

    def test_add_with_chunk_size(self):
        """Test TS.ADD with a chunk size option"""
        timestamp = 160000
        chunk_size = 128  # Small chunk size for testing

        self.client.execute_command(
            "TS.ADD", "ts_chunk_size", timestamp, 88.5, "CHUNK_SIZE", chunk_size
        )

        info = self.ts_info("ts_chunk_size")
        assert info["chunkSize"] == chunk_size

    def test_add_with_duplicate_policy(self):
        """Test TS.ADD with different duplicate policies"""

        # Add first sample
        timestamp = 160000

        self.client.execute_command(
            "TS.ADD", "ts_dup_first", timestamp, 30.0, "DUPLICATE_POLICY", "FIRST"
        )

        info = self.ts_info("ts_dup_first")
        assert info["duplicatePolicy"] == "first"

        self.client.execute_command(
            "TS.ADD", "ts_dup_first", timestamp, 40.0
        )
        samples = self.client.execute_command("TS.RANGE", "ts_dup_first", "-", "+")
        assert float(samples[0][1]) == 30.0  # First value preserved

        # Add duplicate with LAST policy (should update to new value)
        self.client.execute_command(
            "TS.ADD", "ts_dup_last", timestamp, 10.0, "DUPLICATE_POLICY", "LAST"
        )
        self.client.execute_command(
            "TS.ADD", "ts_dup_last", timestamp, 99.0
        )
        samples = self.client.execute_command("TS.RANGE", "ts_dup_last", "-", "+")
        assert float(samples[0][1]) == 99.0  # Last value used

        # Add duplicate with MAX policy
        self.client.execute_command(
            "TS.ADD", "ts_dup_max", timestamp, 40.0, "DUPLICATE_POLICY", "MAX"
        )
        self.client.execute_command(
            "TS.ADD", "ts_dup_max", timestamp, 20.0
        )

        samples = self.client.execute_command("TS.RANGE", "ts_dup_max", "-", "+")
        assert float(samples[0][1]) == 40.0  # Higher value kept

        # Add duplicate with MIN policy
        self.client.execute_command(
            "TS.ADD", "ts_dup_min", timestamp, 20.0, "DUPLICATE_POLICY", "MIN"
        )
        self.client.execute_command(
            "TS.ADD", "ts_dup_min", timestamp, 10.0
        )

        samples = self.client.execute_command("TS.RANGE", "ts_dup_min", "-", "+")
        assert float(samples[0][1]) == 10.0  # Lower value kept

        # Add duplicate with SUM policy
        self.client.execute_command(
            "TS.ADD", "ts_dup_sum", timestamp, 5.0, "DUPLICATE_POLICY", "SUM"
        )
        self.client.execute_command(
            "TS.ADD", "ts_dup_sum", timestamp, 20.0
        )

        samples = self.client.execute_command("TS.RANGE", "ts_dup_sum", "-", "+")
        assert float(samples[0][1]) == 25.0  # Sum of values

        # Add duplicate with BLOCK policy (should fail)
        self.client.execute_command(
            "TS.ADD", "ts_dup_block", timestamp, 5.0, "DUPLICATE_POLICY", "BLOCK"
        )

        with pytest.raises(ResponseError) as exception_info:
            self.client.execute_command(
                "TS.ADD", "ts_dup_block", timestamp, 20.0
            )
        assert "DUPLICATE_POLICY is set to BLOCK" in str(exception_info.value)

    def test_add_on_wrong_type_key(self):
        """TS.ADD on a non-timeseries key replies RTS's TSDB error, not WRONGTYPE"""
        self.client.execute_command('SET', 'ts_add_str', 'hello')

        with pytest.raises(ResponseError, match='TSDB: the key is not a TSDB key'):
            self.client.execute_command('TS.ADD', 'ts_add_str', 100, 1.0)

    def test_add_with_labels_creation(self):
        """Test TS.ADD with labels when creating a new timeseries"""
        timestamp = 160000

        # Add with labels to a non-existent timeseries
        self.client.execute_command(
            "TS.ADD", "ts_with_labels", timestamp, 99.5,
            "LABELS", "sensor", "humidity", "location", "outside"
        )

        # Verify the labels were set
        info = self.ts_info("ts_with_labels")
        labels = info['labels']
        assert labels["sensor"] == "humidity"
        assert labels["location"] == "outside"

    def test_add_multiple_samples(self):
        """Test adding multiple samples to a timeseries"""
        self.client.execute_command("TS.CREATE", "ts_multi")

        # Add multiple samples with different timestamps
        base_ts = 160000
        for i in range(10):
            ts = base_ts + (i * 1000)
            result = self.client.execute_command("TS.ADD", "ts_multi", ts, i * 1.5)
            assert result == ts

        # Verify all samples were added
        samples = self.client.execute_command("TS.RANGE", "ts_multi", "-", "+")
        assert len(samples) == 10
        for i, sample in enumerate(samples):
            value = i * 1.5
            actual = sample[1]
            assert sample[0] == base_ts + (i * 1000)
            assert float(actual) == float(value)

    def test_add_invalid_values(self):
        """Test TS.ADD with invalid values and parameters"""
        self.client.execute_command("TS.CREATE", "ts_invalid")

        # Test with invalid timestamp
        with pytest.raises(ResponseError):
            self.client.execute_command("TS.ADD", "ts_invalid", "invalid_ts", 10.0)

        # Test with invalid value
        with pytest.raises(ResponseError):
            self.client.execute_command("TS.ADD", "ts_invalid", 160000, "not_a_number")

        self.client.execute_command(
            "TS.ADD", "ts_invalid", 160000, 10.0, "CHUNK_SIZE", "invalid"
        )

        # Test with invalid chunk size
        with pytest.raises(ResponseError):
            self.client.execute_command(
                "TS.ADD", "ts_invalid", 160000, 10.0, "CHUNK_SIZE", "invalid"
            )

        # Test with invalid duplicate policy
        with pytest.raises(ResponseError):
            self.client.execute_command(
                "TS.ADD", "ts_invalid", 160000, 10.0, "ON_DUPLICATE", "INVALID_POLICY"
            )

        # Test with an odd number of label pairs
        with pytest.raises(ResponseError):
            self.client.execute_command(
                "TS.ADD", "ts_invalid", 160000, 10.0, "LABELS", "key1", "val1", "key2"
            )

    def test_add_with_decimal_digits(self):
        """Test TS.ADD with a decimal digits option"""
        timestamp = 160000

        # Add with decimal digits precision
        self.client.execute_command(
            "TS.ADD", "ts_decimal", timestamp, 123.456789, "DECIMAL_DIGITS", 2
        )

        # Verify the value was rounded to the specified precision
        samples = self.client.execute_command("TS.RANGE", "ts_decimal", "-", "+")
        assert samples[0][1] == b'123.46'  # Rounded to 2 decimal places

    def test_add_with_significant_digits(self):
        """Test TS.ADD with a significant digits option"""
        timestamp = 160000

        # Add with significant digits precision
        self.client.execute_command(
            "TS.ADD", "ts_significant", timestamp, 123.456789, "SIGNIFICANT_DIGITS", 3
        )

        # Verify the value was rounded to the specified precision
        samples = self.client.execute_command("TS.RANGE", "ts_significant", "-", "+")
        assert samples[0][1] == b'123'  # 3 significant digits

    def test_add_sample_before_first(self):
        """Test adding a sample before the first sample in the timeseries"""
        self.client.execute_command("TS.CREATE", "ts_before_first")

        # Add a sample with a timestamp
        timestamp1 = 10000
        self.client.execute_command("TS.ADD", "ts_before_first", timestamp1, 20.5)

        # Add a sample with a timestamp before the first sample
        timestamp2 = 5000
        self.client.execute_command("TS.ADD", "ts_before_first", timestamp2, 10.5)

        # Verify the sample was added
        samples = self.client.execute_command("TS.RANGE", "ts_before_first", "-", "+")
        assert samples == [[timestamp2, b'10.5'], [timestamp1, b'20.5']]

    def test_add_out_of_range_values(self):
        """Test TS.ADD with extreme values"""
        self.client.execute_command("TS.CREATE", "ts_extreme")

        # Test with a very large timestamp
        max_timestamp = 9223372036854775807  # i64::MAX
        self.client.execute_command("TS.ADD", "ts_extreme", max_timestamp, 100.0)

        # Test with a very large value
        large_value = 1.7976931348623157e+308  # close to f64::MAX
        self.client.execute_command("TS.ADD", "ts_extreme", 160000, large_value)

        # Verify the samples
        samples = self.client.execute_command("TS.RANGE", "ts_extreme", "-", "+")
        assert len(samples) == 2
        assert samples[1][0] == max_timestamp
        assert abs(float(samples[0][1]) - large_value) < 1e300

    ## ========= Compaction Config Tests ========= ##
    def set_policy(self, policy: str) -> None:
        """Helper to set the compaction policy"""
        cmd = f"CONFIG SET ts.ts-compaction-policy {policy}"
        # print(f"Setting compaction policy: {cmd}")
        assert self.client.execute_command(cmd) == b"OK", f"Failed to set compaction policy: {policy}"

    def test_basic_compaction_policy_on_new_key(self):
        """Test that the compaction policy is applied when creating new series with TS.ADD"""
        # Set up compaction policy configuration
        policy_config = "avg:1M:1h:5s"
        suffix = "AVG_60000_5000"
        source_key = 'temperature:sensor1'
        self.set_policy(policy_config)

        dest_key = f"{source_key}_{suffix}"

        expected_rules = [
            CompactionRule(dest_key, 60000, "avg", 5000)
        ]

        # Use TS.ADD on a non-existent key - should create series and apply compaction rules
        result = self.client.execute_command('TS.ADD', source_key, 1000, 25.5, 'LABELS', 'sensor', 'temp1')
        assert result == 1000

        # Verify the destination key was created
        self.validate_rules(source_key, expected_rules)

    def test_multiple_compaction_policies_applied(self):
        """Test that multiple compaction policies are applied to matching series"""
        # Configure multiple policies
        policy_config = "avg:1M:2h:10s;sum:5M:1h:20s;max:15s:30M"
        source_key = 'sensor:temp1'

        expected_rules = [
            CompactionRule(f"{source_key}_AVG_60000_10000", 60000, "avg", 10000),
            CompactionRule(f"{source_key}_SUM_300000_20000", 300000, "sum", 20000),
            CompactionRule(f"{source_key}_MAX_15000", 15000, "max", 0)
        ]

        self.set_policy(policy_config)

        # Add a sample to a non-existent key
        self.client.execute_command('TS.ADD', source_key, 2000, 30.0)

        # Verify multiple compaction rules were added
        self.validate_rules(source_key, expected_rules)
        # TODO: validate values have been added

    def test_pattern_matching_in_compaction_policy(self):
        """Test that filter patterns in compaction policy work correctly"""
        policy_config = "sum:60s:1h:5s|^metrics:cpu:*"
        suffix = "SUM_60000_5000"

        self.set_policy(policy_config)

        # Test matching key
        matching_key = 'metrics:cpu:server1'
        dest_key = f"{matching_key}_{suffix}"

        expected_rules = [
            CompactionRule(dest_key, 60000, "sum", 5000)
        ]

        self.client.execute_command('TS.ADD', matching_key, 3000, 75.5)

        self.validate_rules(matching_key, expected_rules)

        # Test non-matching key (should not get compaction rules from this policy)
        non_matching_key = 'metrics:memory:server1'
        self.client.execute_command('TS.ADD', non_matching_key, 3000, 80.0)

        info = self.ts_info(non_matching_key)
        rules = info.get('rules', [])
        # Should not have the CPU compaction rule
        assert not any(rule[0] == 'metrics:cpu:server1' for rule in rules)

    def test_compaction_policy_with_different_aggregations(self):
        """Test compaction policies with various aggregation functions"""
        aggregations = ['avg', 'sum', 'min', 'max', 'count']

        for agg in aggregations:
            # Clear previous config
            self.set_policy("none")

            policy_config = f"{agg}:1M:1h:10s"
            suffix = f"{agg.upper()}_60000_10000"
            self.set_policy(policy_config)

            expected_rules = [
                CompactionRule(f'test_{agg}:sensor1_{suffix}', 60000, agg, 10000)
            ]
            # Add to source
            source_key = f'test_{agg}:sensor1'
            self.client.execute_command('TS.ADD', source_key, 4000, 10.0)

            self.validate_rules(source_key, expected_rules)

    def test_compaction_rules_applied_only_on_new_series(self):
        """Test that compaction rules are only applied when the source series doesn't exist"""
        # Set compaction policy
        policy_config = "sum:30s:1h:5s"
        suffix = "SUM_30000_5000"
        self.set_policy(policy_config)

        # Create destination
        dest_key = f'existing:test_{suffix}'

        # First, create the series manually without compaction rules
        source_key = 'existing:test'
        self.client.execute_command('TS.CREATE', source_key)

        # Verify no rules initially
        info = self.ts_info(source_key)
        initial_rules = info.get('rules', [])

        # Now add sample to existing series
        self.client.execute_command('TS.ADD', source_key, 5000, 15.0)

        # Verify rules haven't changed (compaction policy not applied to existing series)
        info = self.ts_info(source_key)
        current_rules = info.get('rules', [])
        assert len(current_rules) == len(initial_rules)

        exists = self.client.execute_command('EXISTS', dest_key)
        assert exists == 0, f"Expected no compaction rule for {dest_key}, but it exists"

    def test_compaction_policy_with_alignment(self):
        """Test compaction policy with timestamp alignment"""
        policy_config = "range:10M:6h:5s"
        suffix = "RANGE_600000_5000"
        self.set_policy(policy_config)

        # Create source via TS.ADD
        source_key = 'aligned:test'
        dest_key = f'{source_key}_{suffix}'
        self.client.execute_command('TS.ADD', source_key, 6000, 20.0)

        # Verify rule includes alignment
        info = self.ts_info(source_key)
        rules = info.get('rules', [])
        assert len(rules) >= 1

        # Check for alignment parameter (rule format: [dest, bucket_duration, aggregation, alignment])
        found_rule = any(
            rule.dest_key == dest_key and rule.alignment == 5000
            for rule in rules
        )
        assert found_rule, "Expected rule with alignment not found"

    def test_empty_compaction_policy_config(self):
        """Test that an empty compaction policy config doesn't create rules"""
        # Set empty policy
        self.set_policy('none')

        # Create series via TS.ADD
        source_key = 'no_policy:test'
        self.client.execute_command('TS.ADD', source_key, 7000, 25.0)

        # Verify no compaction rules
        info = self.ts_info(source_key)
        rules = info.get('rules', [])
        assert len(rules) == 0, f"Expected no rules, got {rules}"

    def test_compaction_policy_config_replacement(self):
        """Test that setting new compaction policy replaces old one for new series"""
        # Set an initial policy
        initial_policy = "min:2M:1h"
        self.set_policy(initial_policy)

        # Create series with initial policy
        first_key = 'initial:test'
        self.client.execute_command('TS.ADD', first_key, 8000, 30.0)

        dest_key = f"{first_key}_MIN_120000"
        expected_rules = [
            CompactionRule(dest_key, 120000, "min")
        ]
        self.validate_rules(first_key, expected_rules)

        # Change policy
        new_policy = "max:1M:2h"
        self.set_policy(new_policy)

        # Create destination for new policy
        self.client.execute_command('TS.CREATE', 'new_dest:test')

        # Create new series with new policy
        second_key = 'new:test'
        self.client.execute_command('TS.ADD', second_key, 8000, 35.0)

        dest_key = f"{second_key}_MAX_60000"
        expected_rules = [
            CompactionRule(dest_key, 60000, "max")
        ]

        # Validate that new rules are applied
        self.validate_rules(second_key, expected_rules)

    def test_compaction_with_ts_add_labels(self):
        """Test that compaction rules are applied when TS.ADD creates a series with labels"""
        policy_config = "avg:1M:1h:5s"
        suffix = "AVG_60000_5000"

        self.set_policy(policy_config)

        # Create destination
        self.client.execute_command('TS.CREATE', 'labeled_dest:sensor1')

        # Create a series with TS.ADD including labels
        source_key = 'labeled:sensor1'
        self.client.execute_command(
            'TS.ADD', source_key, 9000, 40.0,
            'LABELS', 'sensor_type', 'temperature', 'location', 'room1'
        )
        dest_key = f"{source_key}_{suffix}"

        # Verify series has labels and compaction rule
        info = self.ts_info(source_key)

        # Check labels
        labels = info.get('labels', {})
        assert 'sensor_type' in labels
        assert labels['sensor_type'] == 'temperature'

        # Check compaction rule
        rules = info.get('rules', [])
        assert len(rules) >= 1
        assert any(rule.dest_key == dest_key for rule in rules)

    def test_compaction_rules_functional_after_creation(self):
        """Test that compaction rules created from config actually work"""
        policy_config = "sum:1M:1h:5000"
        self.set_policy(policy_config)

        # Create the source and add multiple samples
        source_key = 'functional:test'
        base_ts = 10000

        dest_key = f"{source_key}_SUM_60000_5000"

        expected_rules = [
            CompactionRule(dest_key, 60000, "sum", 5000)
        ]

        # The first sample creates the series and applies compaction rules
        self.client.execute_command('TS.ADD', source_key, base_ts, 10.0)

        self.validate_rules(source_key, expected_rules)

        # Add more samples in the same bucket (60 seconds)
        self.client.execute_command('TS.ADD', source_key, base_ts + 30000, 20.0)
        self.client.execute_command('TS.ADD', source_key, base_ts + 50000, 30.0)

        # Add sample in next bucket to trigger compaction
        self.client.execute_command('TS.ADD', source_key, base_ts + 70000, 40.0)

        # Verify compaction occurred
        dest_samples = self.client.execute_command('TS.RANGE', dest_key, '-', '+')
        assert len(dest_samples) >= 1, "No compacted samples found"

        # Verify the sum aggregation (10 + 20 + 30 = 60)
        assert dest_samples[0][1] == b'60', f"Expected sum of 60, got {dest_samples[0][1]}"

    def test_add_nan_value(self):
        """Test TS.ADD with NaN value - should be stored and retrieved"""
        self.client.execute_command("TS.CREATE", "ts_nan")
        timestamp = 160000

        result = self.client.execute_command("TS.ADD", "ts_nan", timestamp, "nan")
        assert result == timestamp

        result = self.client.execute_command('TS.GET', 'ts_nan')
        assert math.isnan(float(result[1]))

    def test_add_positive_nan_value(self):
        """Test TS.ADD with '+nan' value (explicit positive NaN)"""
        self.client.execute_command("TS.CREATE", "ts_pos_nan")
        timestamp = 160000

        result = self.client.execute_command("TS.ADD", "ts_pos_nan", timestamp, "+nan")
        assert result == timestamp

        result = self.client.execute_command('TS.GET', 'ts_pos_nan')
        assert math.isnan(float(result[1]))

    def test_add_nan_case_insensitive(self):
        """Test TS.ADD with NaN in various cases (NaN, NAN, nan, nAn)"""
        self.client.execute_command("TS.CREATE", "ts_nan_case")
        base_ts = 160000

        for i, nan_str in enumerate(["nan", "NaN", "NAN", "nAn"]):
            ts = base_ts + i * 1000
            result = self.client.execute_command("TS.ADD", "ts_nan_case", ts, nan_str)
            assert result == ts

        samples = self.client.execute_command("TS.RANGE", "ts_nan_case", "-", "+")

        assert len(samples) == 4
        for sample in samples:
            assert math.isnan(float(sample[1])), f"Expected NaN, got {sample[1]}"

    def test_add_nan_duplicate_policy_first(self):
        """Test NaN with FIRST duplicate policy: original value is preserved"""
        self.client.execute_command(
            "TS.CREATE", "ts_nan_dup_first", "DUPLICATE_POLICY", "first"
        )
        timestamp = 160000

        self.client.execute_command("TS.ADD", "ts_nan_dup_first", timestamp, 42.0)
        self.client.execute_command("TS.ADD", "ts_nan_dup_first", timestamp, "nan")

        samples = self.client.execute_command("TS.RANGE", "ts_nan_dup_first", "-", "+")
        assert len(samples) == 1
        assert float(samples[0][1]) == 42.0, "FIRST policy should preserve original value"

    def test_add_nan_duplicate_policy_last(self):
        """Test NaN with LAST duplicate policy: non-NaN value is kept"""
        self.client.execute_command(
            "TS.CREATE", "ts_nan_dup_last", "DUPLICATE_POLICY", "last"
        )
        timestamp = 160000

        self.client.execute_command("TS.ADD", "ts_nan_dup_last", timestamp, 42.0)
        self.client.execute_command("TS.ADD", "ts_nan_dup_last", timestamp, "nan")

        samples = self.client.execute_command("TS.RANGE", "ts_nan_dup_last", "-", "+")
        assert len(samples) == 1

        assert 42.0 == float(samples[0][1]), "LAST policy should keep the non-NaN value"

    def test_add_nan_duplicate_first_replaces_nan(self):
        """Test NaN first, then real value with LAST policy: real value replaces NaN"""
        self.client.execute_command(
            "TS.CREATE", "ts_nan_then_val", "DUPLICATE_POLICY", "last"
        )
        timestamp = 160000

        self.client.execute_command("TS.ADD", "ts_nan_then_val", timestamp, "nan")
        self.client.execute_command("TS.ADD", "ts_nan_then_val", timestamp, 99.0)

        samples = self.client.execute_command("TS.RANGE", "ts_nan_then_val", "-", "+")
        assert len(samples) == 1
        assert float(samples[0][1]) == 99.0, "LAST policy should replace NaN with new value"

    def test_add_nan_with_min_max_sum_duplicate_policy(self):
        """Test NaN with MIN/MAX/SUM policies: NaN should be treated as a value that can be compared/summed"""
        for policy in ["MIN", "MAX", "SUM"]:
            key = f"ts_nan_{policy.lower()}"
            self.client.execute_command(
                "TS.CREATE", key, "DUPLICATE_POLICY", policy
            )
            timestamp = 160000

            # Add NaN first
            self.client.execute_command("TS.ADD", key, timestamp, "nan")
            with pytest.raises(ResponseError):
                self.client.execute_command('ts.add', key, timestamp, 25.0)

    def test_add_nan_with_inf(self):
        """Test TS.ADD accepts NaN alongside infinity values"""
        self.client.execute_command("TS.CREATE", "ts_nan_inf")
        base_ts = 160000

        self.client.execute_command("TS.ADD", "ts_nan_inf", base_ts, "nan")
        self.client.execute_command("TS.ADD", "ts_nan_inf", base_ts + 1000, "inf")
        self.client.execute_command("TS.ADD", "ts_nan_inf", base_ts + 2000, "-inf")

        samples = self.client.execute_command("TS.RANGE", "ts_nan_inf", "-", "+")
        assert len(samples) == 3
        assert math.isnan(float(samples[0][1]))
        assert math.isinf(float(samples[1][1])) and float(samples[1][1]) > 0
        assert math.isinf(float(samples[2][1])) and float(samples[2][1]) < 0

    def test_add_invalid_nan_string(self):
        """Test TS.ADD rejects malformed NaN-like strings"""
        self.client.execute_command("TS.CREATE", "ts_invalid_nan")
        timestamp = 160000

        for bad_val in ["nann", "na", "notnan", "nan1"]:
            with pytest.raises(ResponseError):
                self.client.execute_command(
                    "TS.ADD", "ts_invalid_nan", timestamp, bad_val
                )
