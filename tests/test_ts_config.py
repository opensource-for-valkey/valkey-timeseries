# tests/test_ts_config_reflection.py
import time

import pytest
from valkey import ResponseError
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase

DEFAULT_CHUNK_SIZE = 4096
DEFAULT_DUPLICATE_POLICY = "block"
DEFAULT_RETENTION = 0


class TestTimeseriesConfig(ValkeyTimeSeriesTestCaseBase):
    def set_config(self, name: str, value):
        # All module configs are namespaced with "ts."
        self.client.execute_command("CONFIG", "SET", f"ts.{name}", value)

    def get_config(self, name: str):
        # All module configs are namespaced with "ts."
        return self.client.execute_command("CONFIG", "GET", f"ts.{name}")[1]

    def reset_defaults(self):
        # Reset the configs we touched back to sane defaults
        self.set_config("ts-chunk-size-bytes", DEFAULT_CHUNK_SIZE)
        self.set_config("ts-duplicate-policy", DEFAULT_DUPLICATE_POLICY)
        self.set_config("ts-retention-policy", DEFAULT_RETENTION)

    def test_config_chunk_size_applies_to_new_series(self):
        key = "ts_cfg_chunksize"
        new_chunk_size = 8192
        try:
            self.set_config("ts-chunk-size-bytes", new_chunk_size)
            self.client.execute_command("TS.CREATE", key)
            info = self.ts_info(key, True)
            assert info["chunkSize"] == new_chunk_size
        finally:
            self.reset_defaults()

    def test_config_duplicate_policy_applies_to_new_series(self):
        key = "ts_cfg_duplicate_policy"
        new_policy = "last"
        try:
            self.set_config("ts-duplicate-policy", new_policy)
            self.client.execute_command("TS.CREATE", key)
            info = self.ts_info(key, True)
            assert info["duplicatePolicy"] == new_policy
        finally:
            self.reset_defaults()

    def test_config_retention_applies_to_new_series(self):
        key = "ts_cfg_retention"
        new_retention = 60000  # 60 seconds
        try:
            self.set_config("ts-retention-policy", new_retention)
            self.client.execute_command("TS.CREATE", key)
            info = self.ts_info(key, True)
            assert info["retentionTime"] == new_retention
        finally:
            self.reset_defaults()

    def test_config_changes_do_not_affect_existing_series(self):
        key_old = "ts_cfg_existing"
        key_new = "ts_cfg_new"
        new_chunk_size = 16384
        new_policy = "last"
        new_retention = 120000

        try:
            # Create series with defaults
            self.client.execute_command("TS.CREATE", key_old)

            # Change configs
            self.set_config("ts-chunk-size-bytes", new_chunk_size)
            self.set_config("ts-duplicate-policy", new_policy)
            self.set_config("ts-retention-policy", new_retention)

            # Create another series after config changes
            self.client.execute_command("TS.CREATE", key_new)

            # Old series should keep defaults
            info_old = self.ts_info(key_old, True)
            assert info_old["chunkSize"] == DEFAULT_CHUNK_SIZE
            assert info_old["duplicatePolicy"] == DEFAULT_DUPLICATE_POLICY
            assert info_old["retentionTime"] == DEFAULT_RETENTION

            # New series should reflect a new config
            info_new = self.ts_info(key_new, True)
            assert info_new["chunkSize"] == new_chunk_size
            assert info_new["duplicatePolicy"] == new_policy
            assert info_new["retentionTime"] == new_retention
        finally:
            self.reset_defaults()

    def set_compaction_policy(self, policy):
        """Helper to set the compaction policy configuration"""
        self.set_config("ts-compaction-policy", policy)

    def create_series_and_add_data(self, key: str, base_ts: int = 1000):
        """Helper to create a series and add some sample data"""
        self.client.execute_command("TS.CREATE", key)
        # Add samples that span multiple buckets to trigger compaction
        for i in range(5):
            self.client.execute_command("TS.ADD", key, base_ts + (i * 15000), float(i * 10))

    def assert_compaction_rule(self, key: str, expected_aggregation: str = None,
                               expected_bucket_duration: int = None,
                               expected_rule_count: int = None):
        """
        Helper method to check compaction rules in a time series INFO response.

        Args:
            key: The time series key to check
            expected_aggregation: Expected aggregation type (e.g., 'avg', 'sum', 'max')
            expected_bucket_duration: Expected bucket duration in milliseconds
            expected_rule_count: Expected number of compaction rules (if None, checks > 0)
        """
        info = self.ts_info(key)

        assert info is not None, f"TS.INFO returned None for key {key}"

        rules = info.get('rules', [])
        rule_count = len(rules)

        if expected_rule_count is not None:
            assert rule_count == expected_rule_count, f"Expected {expected_rule_count} rules for {key}, got {rule_count}"
        elif expected_aggregation is not None or expected_bucket_duration is not None:
            assert rule_count > 0, f"No compaction rules found for {key}"

        if expected_aggregation is not None or expected_bucket_duration is not None:
            # Check the first rule (most common case)
            rule = rules[0]
            if expected_aggregation is not None:
                assert rule.aggregation.lower() == expected_aggregation.lower(), f"Wrong aggregation for {key}: expected {expected_aggregation}, got {rule.aggregation}"
            if expected_bucket_duration is not None:
                assert rule.bucket_duration == expected_bucket_duration, f"Wrong bucket duration for {key}: expected {expected_bucket_duration}, got {rule.bucket_duration}"

    def assert_no_compaction_rules(self, key: str):
        """Helper method to assert that a key has no compaction rules"""
        info = self.ts_info(key)
        print(f"Compaction info for {key}: {info}")
        rules = info.get('rules', [])
        assert len(rules) == 0, f"Unexpected compaction rules found for {key}"

    def test_commands_which_create_compactions_from_policy(self):
        """Test that commands which create compactions from policy work as expected"""

        # TS.ADD, TS.MADD, TS.INCRBY and TS.DECRBY create compactions based on the policy set

        # Set a compaction policy
        policy = "avg:10s:1h"
        self.set_compaction_policy(policy)

        # Test with TS.ADD
        ts_add_key = "ts_add_key"
        compaction_key = f"{ts_add_key}_AVG_10000"
        self.client.execute_command("TS.ADD", ts_add_key, 1000, 10.0)
        self.assert_compaction_rule(ts_add_key, "avg", 10000)
        assert self.client.execute_command("EXISTS", compaction_key) == 1

        # Test with TS.INCRBY
        ts_incr_key = "ts_incr_key"
        compaction_key_incr = f"{ts_incr_key}_AVG_10000"
        self.client.execute_command("TS.INCRBY", ts_incr_key, 5.0, "RETENTION", 60000)
        self.assert_compaction_rule(ts_incr_key, "avg", 10000)
        assert self.client.execute_command("EXISTS", compaction_key_incr) == 1

        # Test with TS.DECRBY
        ts_decr_key = "ts_decr_key"
        compaction_key_decr = f"{ts_decr_key}_AVG_10000"
        self.client.execute_command("TS.DECRBY", ts_decr_key, 1.0, "RETENTION", 60000, "ENCODING", "COMPRESSED")
        self.assert_compaction_rule(ts_decr_key, "avg", 10000)
        assert self.client.execute_command("EXISTS", compaction_key_decr) == 1

        # Add multiple samples
        # self.client.execute_command('TS.MADD',
        #                                      'ts1', 1000, 10.0,
        #                                      'ts2', 2000, 20.0,
        #                                      'ts3', 3000, 30.0)
        # madd_keys = ['ts1', 'ts2', 'ts3']
        # for key in madd_keys:
        #     compaction_key_madd = f"{key}_AVG_10000"
        #     self.assert_compaction_rule(key, "avg", 10000)
        #     assert self.client.execute_command("EXISTS", compaction_key_madd) == 1

    def test_create_does_not_create_compaction_rules(self):
        """Test that TS.CREATE does not create compaction rules from config"""

        # Set a compaction policy
        policy = "avg:10s:1h"
        self.set_compaction_policy(policy)

        # Create a time series
        ts_key = "ts_create_key"
        self.client.execute_command("TS.CREATE", ts_key)

        # Verify no compaction rules were created
        self.assert_no_compaction_rules(ts_key)

    def test_global_policy_applies_to_all_series(self):
        """Test that a global policy (no filter) applies to all series"""

        # Set a global compaction policy (no filter specified)
        policy = "avg:10s:1h"
        self.set_compaction_policy(policy)

        # Create a series with different naming patterns
        series_keys = [
            "global:test:1",
            "metrics:cpu:usage",
            "sensor_temperature",
            "app-latency-p99"
        ]

        # Verify that compaction rules were created for all series
        for key in series_keys:
            ts = int(time.time() * 1000)  # Current time in milliseconds
            self.client.execute_command("TS.ADD", key, ts, 120.0)  # Add sample data
            self.assert_compaction_rule(key, "avg", 10000)

    def test_filtered_policy_applies_only_to_matching_keys(self):
        """Test that filtered policies only apply to keys matching the regex filter"""

        # Set a filtered compaction policy that only matches keys starting with "metrics:"
        policy = "avg:5s:30m|^metrics:.*"
        self.set_compaction_policy(policy)

        # Create a series with matching and non-matching patterns
        matching_keys = [
            "metrics:cpu:usage",
            "metrics:memory:free",
            "metrics:disk:io"
        ]

        non_matching_keys = [
            "logs:error:count",
            "events:user:login",
            "sensor:temperature",
            "app-latency"
        ]

        # Create all series and add data
        all_keys = matching_keys + non_matching_keys
        for key in all_keys:
            ts = int(time.time() * 1000)  # Current time in milliseconds
            self.client.execute_command("TS.ADD", key, ts, 100.0)

        # Verify matching keys have compaction rules
        for key in matching_keys:
            self.assert_compaction_rule(key, "avg", 5000)

        # Verify non-matching keys do NOT have compaction rules
        for key in non_matching_keys:
            self.assert_no_compaction_rules(key)

    def test_multiple_filtered_policies_with_different_patterns(self):
        """Test multiple filtered policies with different regex patterns"""

        # Set multiple filtered policies
        policy = "avg:10s:1h|^metrics:.*;sum:30s:6h|^logs:.*;max:1M:24h|^sensors:.*"
        self.set_compaction_policy(policy)

        test_cases = [
            # (key, expected_aggregation, expected_bucket_duration)
            ("metrics:cpu:load", "avg", 10000),
            ("metrics:network:bandwidth", "avg", 10000),
            ("logs:application:errors", "sum", 30000),
            ("logs:system:warnings", "sum", 30000),
            ("sensors:temperature:room1", "max", 60000),
            ("sensors:humidity:outdoor", "max", 60000),
            ("unmatched:series", None, None),  # Should not match any pattern
            ("random:data:point", None, None)  # Should not match any pattern
        ]

        for key, expected_agg, expected_bucket in test_cases:
            ts = int(time.time() * 1000)  # Current time in milliseconds
            self.client.execute_command("TS.ADD", key, ts, 120.0)  # Add sample data

            if expected_agg is None:
                # Should not have any compaction rules
                self.assert_no_compaction_rules(key)
            else:
                # Should have matching compaction rule
                self.assert_compaction_rule(key, expected_agg, expected_bucket)

    def test_complex_regex_patterns(self):
        """Test complex regex patterns in compaction policy filters"""

        # Complex regex: matches series with specific patterns
        # - app:(prod|staging):.*  - application metrics from prod or staging
        # - sensor_[0-9]+_temp     - numbered temperature sensors
        policy = "avg:15s:2h|app:(prod|staging):.*;min:1M:12h|sensor_[0-9]+_temp"
        self.set_compaction_policy(policy)

        test_cases = [
            # App environment patterns
            ("app:prod:latency", "avg", 15000),
            ("app:staging:errors", "avg", 15000),
            ("app:prod:cpu:usage", "avg", 15000),
            ("app:dev:latency", None, None),  # dev environment not included
            ("app:test:errors", None, None),  # test environment is not included

            # Sensor patterns
            ("sensor_1_temp", "min", 60000),
            ("sensor_42_temp", "min", 60000),
            ("sensor_999_temp", "min", 60000),
            ("sensor_temp", None, None),  # Missing number
            ("sensor_1_humidity", None, None),  # Not temperature
            ("sensor_abc_temp", None, None),  # Non-numeric ID

            # Unmatched patterns
            ("other:metrics", None, None),
            ("random_data", None, None)
        ]

        for key, expected_agg, expected_bucket in test_cases:
            ts = int(time.time() * 1000)  # Current time in milliseconds
            self.client.execute_command("TS.ADD", key, ts, 110.0)  # Add sample data

            if expected_agg is None:
                self.assert_no_compaction_rules(key)
            else:
                self.assert_compaction_rule(key, expected_agg, expected_bucket)

    def test_policy_changes_affect_only_new_series(self):
        """Test that policy changes only affect newly created series"""

        # Create series before setting policy
        pre_policy_keys = ["before:policy:1", "before:policy:2"]
        for key in pre_policy_keys:
            ts = int(time.time() * 1000)  # Current time in milliseconds
            self.client.execute_command("TS.ADD", key, ts, 100.0)

        # Set a filtered policy
        policy = "avg:10s:1h|^after:.*"
        self.set_compaction_policy(policy)

        # Create series after setting policy
        post_policy_matching = ["after:policy:1", "after:policy:2"]
        post_policy_non_matching = ["other:policy:1", "other:policy:2"]

        for key in post_policy_matching + post_policy_non_matching:
            ts = int(time.time() * 1000)
            self.client.execute_command("TS.ADD", key, ts, 200.0)

        # Verify pre-policy series have no compaction rules
        for key in pre_policy_keys:
            self.assert_no_compaction_rules(key)

        # Verify post-policy matching series have compaction rules
        for key in post_policy_matching:
            self.assert_compaction_rule(key, "avg", 10000)

        # Verify post-policy non-matching series have no compaction rules
        for key in post_policy_non_matching:
            self.assert_no_compaction_rules(key)

    def test_mixed_global_and_filtered_policies(self):
        """Test combination of global and filtered policies"""

        # Set both global and filtered policies
        # Global policy: sum:1m:6h (applies to all)
        # Filtered policy: avg:10s:1h:high_freq:.* (applies only to high_freq series)
        policy = "sum:1M:6h;avg:10s:1h|high_freq:.*"
        self.set_compaction_policy(policy)

        test_cases = [
            # Keys matching high_freq filter should get both policies
            ("high_freq:cpu", 2),  # Should have 2 rules: global sum + filtered avg
            ("high_freq:memory", 2),

            # Keys not matching filter should only get global policy
            ("low_freq:disk", 1),  # Should have 1 rule: global sum only
            ("normal:network", 1),
            ("metrics:database", 1)
        ]

        for key, expected_rule_count in test_cases:
            ts = int(time.time() * 1000)  # Current time in milliseconds
            self.client.execute_command("TS.ADD", key, ts, 120.0)  # Add sample data

            self.assert_compaction_rule(key, expected_rule_count=expected_rule_count)

            info = self.ts_info(key)
            rules = info.get('rules', [])

            # All should have the global sum rule
            sum_rules = [r for r in rules if r.aggregation.lower() == 'sum' and r.bucket_duration == 60000]
            assert len(sum_rules) == 1, f"Expected 1 sum rule for {key}"

            if expected_rule_count == 2:
                # high_freq keys should also have avg rule
                avg_rules = [r for r in rules if r.aggregation.lower() == 'avg' and r.bucket_duration == 10000]
                assert len(avg_rules) == 1, f"Expected 1 avg rule for high_freq key {key}"

    def test_case_sensitive_regex_matching(self):
        """Test that regex matching is case-sensitive"""

        # Set a policy that matches lowercase "metrics" only
        policy = "avg:10s:1h|metrics:.*"
        self.set_compaction_policy(policy)

        test_cases = [
            ("metrics:cpu", True),  # lowercase - should match
            ("metrics:memory", True),  # lowercase - should match
            ("Metrics:cpu", False),  # uppercase M - should not match
            ("METRICS:memory", False),  # all uppercase - should not match
            ("MetRics:disk", False),  # mixed case - should not match
        ]

        for key, should_have_rules in test_cases:
            ts = int(time.time() * 1000)  # Current time in milliseconds
            self.client.execute_command("TS.ADD", key, ts, 120.0)  # Add sample data

            if should_have_rules:
                self.assert_compaction_rule(key, "avg", 10000)
            else:
                self.assert_no_compaction_rules(key)

    def test_special_regex_characters_in_keys(self):
        """Test handling of keys containing special regex characters"""

        # Policy that should match keys containing dots and other special chars
        policy = r"avg:10s:1h|^app\.prod\..*"  # Escaped dots to match literal dots
        self.set_compaction_policy(policy)

        test_cases = [
            ("app.prod.latency", True),  # Should match (literal dots)
            ("app.prod.cpu.usage", True),  # Should match
            ("appXprodXlatency", False),  # Should not match (X instead of .)
            ("app-prod-latency", False),  # Should not match (- instead of .)
            ("app.dev.latency", False),  # Should not match (dev instead of prod)
        ]

        for key, should_have_rules in test_cases:
            self.client.execute_command("TS.ADD", key, int(time.time() * 1000), 120.0)

            if should_have_rules:
                self.assert_compaction_rule(key, "avg", 10000)
            else:
                self.assert_no_compaction_rules(key)


class TestTimeseriesRoundingConfig(ValkeyTimeSeriesTestCaseBase):
    """`none` disables rounding; `0` is an ordinary digit count.

    The module-wide defaults and the per-series DECIMAL_DIGITS / SIGNIFICANT_DIGITS arguments
    must agree on what each value means.
    """

    def set_config(self, name: str, value):
        self.client.execute_command("CONFIG", "SET", f"ts.{name}", value)

    def sample_value(self, key):
        return float(self.client.execute_command("TS.GET", key)[1])

    def setup_method(self, method):
        # Each test starts from "rounding disabled".
        pass

    def test_config_zero_decimal_digits_rounds_to_whole_numbers(self):
        self.set_config("ts-decimal-digits", "0")
        try:
            self.client.execute_command("TS.CREATE", "round_cfg_zero")
            self.client.execute_command("TS.ADD", "round_cfg_zero", 1000, 3.7)
            assert self.sample_value("round_cfg_zero") == 4.0
        finally:
            self.set_config("ts-decimal-digits", "none")

    def test_config_none_disables_rounding(self):
        self.set_config("ts-decimal-digits", "none")
        self.client.execute_command("TS.CREATE", "round_cfg_none")
        self.client.execute_command("TS.ADD", "round_cfg_none", 1000, 3.7)
        assert self.sample_value("round_cfg_none") == 3.7

    def test_config_and_per_series_zero_agree(self):
        """The whole point: `0` means the same thing on both paths."""
        self.set_config("ts-decimal-digits", "0")
        try:
            self.client.execute_command("TS.CREATE", "round_from_config")
            self.client.execute_command("TS.ADD", "round_from_config", 1000, 3.7)
        finally:
            self.set_config("ts-decimal-digits", "none")

        self.client.execute_command("TS.CREATE", "round_from_arg", "DECIMAL_DIGITS", 0)
        self.client.execute_command("TS.ADD", "round_from_arg", 1000, 3.7)

        assert self.sample_value("round_from_config") == self.sample_value("round_from_arg") == 4.0

    def test_zero_significant_digits_rejected_on_both_paths(self):
        """Zero significant digits is not a quantity, so neither path accepts it."""
        with pytest.raises(ResponseError):
            self.set_config("ts-significant-digits", "0")

        with pytest.raises(ResponseError, match="SIGNIFICANT_DIGITS"):
            self.client.execute_command("TS.CREATE", "sig_zero", "SIGNIFICANT_DIGITS", 0)

        # 1 is the minimum and is accepted on both.
        self.set_config("ts-significant-digits", "1")
        self.set_config("ts-significant-digits", "none")
        self.client.execute_command("TS.CREATE", "sig_one", "SIGNIFICANT_DIGITS", 1)

    def test_zero_decimal_digits_conflicts_with_significant_digits(self):
        """`0` now activates rounding, so it participates in the mutual-exclusion rule."""
        self.set_config("ts-significant-digits", "5")
        try:
            with pytest.raises(ResponseError, match="Cannot set both"):
                self.set_config("ts-decimal-digits", "0")
        finally:
            self.set_config("ts-significant-digits", "none")
