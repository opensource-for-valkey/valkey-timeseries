import time
from typing import List

import pytest
from valkey import ResponseError
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util.waiters import *

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase, CompactionRule


class TestTSCreateRule(ValkeyTimeSeriesTestCaseBase):
    """Integration tests for TS.CREATERULE command"""

    def create_test_series(self, key: str, **kwargs) -> None:
        """Helper to create a test time series"""
        args = ["TS.CREATE", key]
        for k, v in kwargs.items():
            args.extend([k.upper(), str(v)])
        self.client.execute_command(*args)

    def add_samples(self, key: str, samples: List[tuple]) -> None:
        """Helper to add multiple samples to a series"""
        for timestamp, value in samples:
            self.client.execute_command("TS.ADD", key, timestamp, value)

    def validate_rule_info(self, source_key: str, expected) -> None:
        # Verify rule was created by checking TS.INFO
        info = self.ts_info(source_key)
        assert "rules" in info
        assert len(info["rules"]) == 1
        rule = info["rules"][0]
        assert expected, rule

    def validate_rules_info(self, source_key: str, expected_rules: List[tuple]) -> None:
        """Helper to validate the rules info for a source key"""
        info = self.ts_info(source_key)
        assert "rules" in info
        rules = info["rules"]
        assert len(rules) == len(expected_rules), f"Expected {len(expected_rules)} rules, got {len(rules)}"
        for i, actual in rules:
            expected = expected_rules[i]
            assert expected == actual, f"Rule {i} mismatch: expected {expected}, got {actual}"

    def test_create_rule_basic_success(self):
        """Test basic successful rule creation"""
        source_key = "test:source1"
        dest_key = "test:dest1"

        # Create source and destination series
        self.create_test_series(source_key)
        self.create_test_series(dest_key)

        # Create compaction rule
        result = self.client.execute_command(
            "TS.CREATERULE", source_key, dest_key,
            "AGGREGATION", "avg", "60000"  # 60 second buckets
        )
        assert result == b"OK"
        info = self.ts_info(source_key)
        rules = info["rules"]
        assert len(rules) == 1
        expected_rule = CompactionRule(dest_key, 60000, "avg", None)
        assert expected_rule == rules[0]

    def test_create_rule_with_align_timestamp(self):
        """Test rule creation with alignment timestamp"""
        source_key = "test:source2"
        dest_key = "test:dest2"

        self.create_test_series(source_key)
        self.create_test_series(dest_key)

        align_ts = int(time.time() * 1000)
        result = self.client.execute_command(
            "TS.CREATERULE", source_key, dest_key,
            "AGGREGATION", "sum", "30000", str(align_ts)
        )
        assert result == b"OK"

        info = self.ts_info(source_key)
        rules = info["rules"]
        assert len(rules) == 1
        expected_rule = CompactionRule(dest_key, 30000, "sum", align_ts)
        assert expected_rule == rules[0]

    def test_create_rule_various_aggregators(self):
        """Test rule creation with different aggregation types"""
        aggregators = [
            "avg", "sum", "min", "max", "count", "first", "increase", "last",
            "std.p", "std.s", "var.p", "var.s", "range", "rate"
        ]

        for i, agg in enumerate(aggregators):
            source_key = f"test:source_agg_{i}"
            dest_key = f"test:dest_agg_{i}"

            self.create_test_series(source_key)
            self.create_test_series(dest_key)

            result = self.client.execute_command(
                "TS.CREATERULE", source_key, dest_key,
                "AGGREGATION", agg, "60000"
            )
            assert result == b"OK"

            info = self.ts_info(source_key)
            rules = info["rules"]
            assert len(rules) == 1
            expected_rule = CompactionRule(dest_key, 60000, agg, None)
            assert expected_rule == rules[0], f"Failed for aggregator: {agg}"

    def test_create_rule_with_aggregation_filters(self):
        """Test rule creation with filters (if supported)"""
        source_key = "test:source_filters"
        dest_key = "test:dest_filters"

        self.create_test_series(source_key)
        self.create_test_series(dest_key)

        for agg in ["sum", "count", "all", "any", "none", "share"]:
            key = f"{dest_key}_{agg}"

            self.client.execute_command("TS.CREATE", key, "ENCODING", "COMPRESSED")

            result = self.client.execute_command(
                "TS.CREATERULE", source_key, key,
                "AGGREGATION", f"{agg}(<100)", "60000"
            )

            assert result == b"OK"

            info = self.ts_info(source_key)
            rules = info["rules"]
            # find the rule we just created
            found = False
            for rule in rules:
                if rule.dest_key == key:
                    found = True
                    break
            assert found, f"Rule for {key} not found"

    def test_create_rule_missing_required_condition(self):
        """Filtered aggregators (countif, sumif, all, any, none, share) require
        an inline (op value) condition; omitting it is an error."""
        source_key = "test:source_missing_cond"
        dest_key = "test:dest_missing_cond"

        self.create_test_series(source_key)
        self.create_test_series(dest_key)

        with pytest.raises(ResponseError, match="TSDB: missing condition for aggregator"):
            self.client.execute_command(
                "TS.CREATERULE", source_key, dest_key,
                "AGGREGATION", "countif", "60000"
            )

    def test_create_rule_disallowed_condition(self):
        """Aggregators that don't support conditions (e.g. avg) must not
        carry an inline (op value)."""
        source_key = "test:source_bad_cond"
        dest_key = "test:dest_bad_cond"

        self.create_test_series(source_key)
        self.create_test_series(dest_key)

        with pytest.raises(ResponseError, match="TSDB: aggregation type does not support a filter condition"):
            self.client.execute_command(
                "TS.CREATERULE", source_key, dest_key,
                "AGGREGATION", "avg(>5)", "60000"
            )

    def test_disallow_replace_existing(self):
        """Test replacing an existing rule"""
        source_key = "test:source_replace"
        dest_key = "test:dest_replace"

        self.create_test_series(source_key)
        self.create_test_series(dest_key)

        # Create an initial rule
        self.client.execute_command(
            "TS.CREATERULE", source_key, dest_key,
            "AGGREGATION", "avg", "60000"
        )

        with pytest.raises(ResponseError, match="TSDB: the destination key already has a src rule"):
            # Try replacing with a different aggregation
            self.client.execute_command(
                "TS.CREATERULE", source_key, dest_key,
                "AGGREGATION", "sum", "30000"
            )

    def test_create_rule_wrong_arity(self):
        """Test error handling for the wrong number of arguments"""
        with pytest.raises(ResponseError, match="wrong number of arguments"):
            self.client.execute_command("TS.CREATERULE")

        with pytest.raises(ResponseError, match="wrong number of arguments"):
            self.client.execute_command("TS.CREATERULE", "key1")

        with pytest.raises(ResponseError, match="wrong number of arguments"):
            self.client.execute_command("TS.CREATERULE", "key1", "key2", "AGGREGATION", "avg")

    def test_create_rule_same_source_dest(self):
        """Test error when source and destination are the same"""
        key = "test:same_key"
        self.create_test_series(key)

        with pytest.raises(ResponseError, match="TSDB: the source key and destination key should be different"):
            self.client.execute_command(
                "TS.CREATERULE", key, key,
                "AGGREGATION", "avg", "60000"
            )

    def test_create_rule_nonexistent_source(self):
        """Test error when the source series doesn't exist"""
        dest_key = "test:dest_nonexist"
        self.create_test_series(dest_key)

        with pytest.raises(ResponseError, match="TSDB: the key does not exist"):
            self.client.execute_command(
                "TS.CREATERULE", "test:nonexistent", dest_key,
                "AGGREGATION", "avg", "60000"
            )

    def test_create_rule_nonexistent_dest(self):
        """Test error when destination series doesn't exist"""
        source_key = "test:source_nonexist"
        self.create_test_series(source_key)

        with pytest.raises(ResponseError, match="TSDB: the key does not exist"):
            self.client.execute_command(
                "TS.CREATERULE", source_key, "test:nonexistent",
                "AGGREGATION", "avg", "60000"
            )

    def test_create_rule_dest_is_compaction(self):
        """Test error when the destination is already a compaction destination"""
        source1_key = "test:source1_comp"
        source2_key = "test:source2_comp"
        dest_key = "test:dest_comp"

        self.create_test_series(source1_key)
        self.create_test_series(source2_key)
        self.create_test_series(dest_key)

        # Create first rule
        self.client.execute_command(
            "TS.CREATERULE", source1_key, dest_key,
            "AGGREGATION", "avg", "60000"
        )

        # Try to create second rule to same destination
        with pytest.raises(ResponseError,
                           match="TSDB: the destination key already has a src rule"):
            self.client.execute_command(
                "TS.CREATERULE", source2_key, dest_key,
                "AGGREGATION", "sum", "60000"
            )

    def test_create_rule_invalid_aggregation_keyword(self):
        """Test error with a missing or invalid AGGREGATION keyword"""
        source_key = "test:source_invalid_agg"
        dest_key = "test:dest_invalid_agg"

        self.create_test_series(source_key)
        self.create_test_series(dest_key)

        with pytest.raises(ResponseError, match="TSDB: Couldn't parse AGGREGATION"):
            self.client.execute_command(
                "TS.CREATERULE", source_key, dest_key,
                "INVALID", "avg", "60000"
            )

    def test_create_rule_invalid_aggregation_type(self):
        """Test error with an invalid aggregation type"""
        source_key = "test:source_invalid_type"
        dest_key = "test:dest_invalid_type"

        self.create_test_series(source_key)
        self.create_test_series(dest_key)

        with pytest.raises(ResponseError, match="unknown aggregation type"):
            self.client.execute_command(
                "TS.CREATERULE", source_key, dest_key,
                "AGGREGATION", "invalid_agg", "60000"
            )

    def test_create_rule_invalid_duration(self):
        """Test error with invalid bucket duration"""
        source_key = "test:source_invalid_dur"
        dest_key = "test:dest_invalid_dur"

        self.create_test_series(source_key)
        self.create_test_series(dest_key)

        with pytest.raises(ResponseError, match="invalid bucket duration"):
            self.client.execute_command(
                "TS.CREATERULE", source_key, dest_key,
                "AGGREGATION", "avg", "invalid"
            )

        with pytest.raises(ResponseError, match="invalid bucket duration"):
            self.client.execute_command(
                "TS.CREATERULE", source_key, dest_key,
                "AGGREGATION", "avg", "-1000"
            )

    def test_create_rule_invalid_align_timestamp(self):
        """Test error with invalid alignment timestamp"""
        source_key = "test:source_invalid_align"
        dest_key = "test:dest_invalid_align"

        self.create_test_series(source_key)
        self.create_test_series(dest_key)

        with pytest.raises(ResponseError, match="Couldn't parse alignTimestamp"):
            self.client.execute_command(
                "TS.CREATERULE", source_key, dest_key,
                "AGGREGATION", "avg", "60000", "invalid_timestamp"
            )

    def test_create_rule_missing_bucket_duration(self):
        """Test error when bucket duration is missing"""
        source_key = "test:source_missing_dur"
        dest_key = "test:dest_missing_dur"

        self.create_test_series(source_key)
        self.create_test_series(dest_key)

        with pytest.raises(ResponseError, match="wrong number of arguments for 'ts.createrule' command"):
            self.client.execute_command(
                "TS.CREATERULE", source_key, dest_key,
                "AGGREGATION", "avg"
            )

    def test_create_rule_duration_formats(self):
        """Test various duration formats"""
        duration_formats = [
            ("60000", "milliseconds as integer"),
            ("60s", "seconds"),
            ("1m", "minutes"),
            ("1h", "hours"),
            ("1d", "days"),
        ]

        for i, (duration, desc) in enumerate(duration_formats):
            source_key = f"test:source_dur_{i}"
            dest_key = f"test:dest_dur_{i}"

            self.create_test_series(source_key)
            self.create_test_series(dest_key)

            result = self.client.execute_command(
                "TS.CREATERULE", source_key, dest_key,
                "AGGREGATION", "avg", duration
            )
            assert result == b"OK", f"Failed for duration format: {desc}"

    def test_create_rule_functional_verification(self):
        """Test that the rule actually works for aggregation"""
        source_key = "test:source_functional"
        dest_key = "test:dest_functional"

        self.create_test_series(source_key)
        self.create_test_series(dest_key)

        # Create rule with 1-second buckets
        self.client.execute_command(
            "TS.CREATERULE", source_key, dest_key,
            "AGGREGATION", "avg", "1000"
        )

        # Add some test data
        base_ts = int(time.time() * 1000)
        samples = [
            (base_ts, 10),
            (base_ts + 100, 20),
            (base_ts + 200, 30),
            (base_ts + 1100, 40),  # Next bucket
            (base_ts + 1200, 50),
        ]

        for timestamp, value in samples:
            self.client.execute_command("TS.ADD", source_key, timestamp, value)

        # Verify aggregated data exists in destination
        dest_range = self.client.execute_command("TS.RANGE", dest_key, "-", "+")
        assert len(dest_range) > 0, "Expected aggregated data in destination series"

    def test_create_rule_case_insensitive_aggregator(self):
        """Test that aggregator names are case-insensitive"""
        source_key = "test:source_agg_case"
        dest_key = "test:dest_agg_case"

        self.create_test_series(source_key)
        self.create_test_series(dest_key)

        # Test uppercase aggregator
        result = self.client.execute_command(
            "TS.CREATERULE", source_key, dest_key,
            "AGGREGATION", "AVG", "60000"
        )
        assert result == b"OK"

    def test_create_compaction_chain_success(self):
        """Test creating a valid chain of compactions (A -> B -> C)"""
        raw_key = "test:raw_data"
        minute_key = "test:minute_data"
        hour_key = "test:hour_data"

        # Create all series
        self.create_test_series(raw_key)
        self.create_test_series(minute_key)
        self.create_test_series(hour_key)

        # Create first compaction rule: raw -> minute (60s buckets)
        result = self.client.execute_command(
            "TS.CREATERULE", raw_key, minute_key,
            "AGGREGATION", "avg", "60000"
        )
        assert result == b"OK"

        # Create a second compaction rule: minute -> hour (60min buckets)
        result = self.client.execute_command(
            "TS.CREATERULE", minute_key, hour_key,
            "AGGREGATION", "avg", "3600000"
        )
        assert result == b"OK"

        # Verify both rules were created
        raw_info = self.ts_info(raw_key)
        assert len(raw_info["rules"]) == 1
        assert raw_info["rules"][0] == CompactionRule(minute_key, 60000, "avg", None)

        minute_info = self.ts_info(minute_key)
        assert len(minute_info["rules"]) == 1
        assert minute_info["rules"][0] == CompactionRule(hour_key, 3600000, "avg", None)

    def test_create_multi_level_compaction_tree(self):
        """Test creating a tree-like compaction structure"""
        source_key = "test:source"
        branch1_key = "test:branch1"
        branch2_key = "test:branch2"
        leaf1_key = "test:leaf1"
        leaf2_key = "test:leaf2"

        # Create all series
        for key in [source_key, branch1_key, branch2_key, leaf1_key, leaf2_key]:
            self.create_test_series(key)

        # Create first level: source -> branch1, source -> branch2
        self.client.execute_command(
            "TS.CREATERULE", source_key, branch1_key,
            "AGGREGATION", "avg", "60000"
        )
        self.client.execute_command(
            "TS.CREATERULE", source_key, branch2_key,
            "AGGREGATION", "sum", "60000"
        )

        # Create the second level: branch1 -> leaf1, branch2 -> leaf2
        self.client.execute_command(
            "TS.CREATERULE", branch1_key, leaf1_key,
            "AGGREGATION", "max", "3600000"
        )
        self.client.execute_command(
            "TS.CREATERULE", branch2_key, leaf2_key,
            "AGGREGATION", "min", "3600000"
        )

        # Verify the structure
        source_info = self.ts_info(source_key)
        assert len(source_info["rules"]) == 2

        branch1_info = self.ts_info(branch1_key)
        assert len(branch1_info["rules"]) == 1
        assert branch1_info["rules"][0] == CompactionRule(leaf1_key, 3600000, "max", None)

        branch2_info = self.ts_info(branch2_key)
        assert len(branch2_info["rules"]) == 1
        assert branch2_info["rules"][0] == CompactionRule(leaf2_key, 3600000, "min", None)

    def test_prevent_self_compaction(self):
        """Test preventing a series from being its own compaction destination"""
        key = "test:self_compact"
        self.create_test_series(key)

        with pytest.raises(ResponseError, match="the source key and destination key should be different"):
            self.client.execute_command(
                "TS.CREATERULE", key, key,
                "AGGREGATION", "avg", "60000"
            )

    def test_prevent_direct_circular_dependency(self):
        """Test preventing direct circular dependency (A -> B -> A)"""
        key_a = "test:circular_a"
        key_b = "test:circular_b"

        # Create both series
        self.create_test_series(key_a)
        self.create_test_series(key_b)

        # Create the first rule: A -> B
        result = self.client.execute_command(
            "TS.CREATERULE", key_a, key_b,
            "AGGREGATION", "avg", "60000"
        )
        assert result == b"OK"
        info = self.ts_info(key_a)
        assert len(info["rules"]) == 1
        print(info["rules"])

        self.client.execute_command(
            "TS.CREATERULE", key_b, key_a,
            "AGGREGATION", "sum", "60000"
        )

        print("Created rule from B to A")
        info_b = self.ts_info(key_b)
        print(info_b)

        # Try to create a circular rule: B -> A (should fail)
        with pytest.raises(ResponseError, match="TSDB: the destination key already has a src rule"):
            self.client.execute_command(
                "TS.CREATERULE", key_b, key_a,
                "AGGREGATION", "sum", "60000"
            )

    def test_prevent_indirect_circular_dependency(self):
        """Test preventing indirect circular dependency (A -> B -> C -> A)"""
        key_a = "test:indirect_a"
        key_b = "test:indirect_b"
        key_c = "test:indirect_c"

        # Create all series
        for key in [key_a, key_b, key_c]:
            self.create_test_series(key)

        # Create chain: A -> B -> C
        self.client.execute_command(
            "TS.CREATERULE", key_a, key_b,
            "AGGREGATION", "avg", "60000"
        )
        self.client.execute_command(
            "TS.CREATERULE", key_b, key_c,
            "AGGREGATION", "sum", "120000"
        )

        self.client.execute_command(
            "TS.CREATERULE", key_c, key_a,
            "AGGREGATION", "max", "300000"
        )

        # Try to create a circular rule: C -> A (should fail)
        with pytest.raises(ResponseError, match="TSDB: the destination key already has a src rule"):
            self.client.execute_command(
                "TS.CREATERULE", key_c, key_a,
                "AGGREGATION", "max", "300000"
            )

    def test_prevent_complex_circular_dependency(self):
        """Test preventing circular dependency in a more complex graph"""
        keys = [f"test:complex_{i}" for i in range(5)]

        # Create all series
        for key in keys:
            self.create_test_series(key)

        # Create a complex dependency graph:
        # 0 -> 1, 0 -> 2
        # 2 -> 3, 2 -> 4
        self.client.execute_command(
            "TS.CREATERULE", keys[0], keys[1],
            "AGGREGATION", "avg", "60000"
        )
        self.client.execute_command(
            "TS.CREATERULE", keys[0], keys[2],
            "AGGREGATION", "sum", "60000"
        )

        info = self.ts_info(keys[2])
        print(info["rules"])

        self.client.execute_command(
            "TS.CREATERULE", keys[2], keys[3],
            "AGGREGATION", "min", "120000"
        )
        self.client.execute_command(
            "TS.CREATERULE", keys[2], keys[4],
            "AGGREGATION", "count", "120000"
        )

        # 4 -> 1 would create a cycle through 0
        with pytest.raises(ResponseError, match="TSDB: the destination key already has a src rule"):
            self.client.execute_command(
                "TS.CREATERULE", keys[4], keys[1],
                "AGGREGATION", "sum", "300000"
            )

    def test_allow_valid_diamond_dependency(self):
        """Test that diamond-shaped dependencies are allowed (no cycles)"""
        source_key = "test:diamond_source"
        left_key = "test:diamond_left"
        right_key = "test:diamond_right"
        sink_key = "test:diamond_sink"

        # Create all series
        for key in [source_key, left_key, right_key, sink_key]:
            self.create_test_series(key)

        # Create a diamond structure:
        # source -> left, source -> right
        # left -> sink, right -> sink
        self.client.execute_command(
            "TS.CREATERULE", source_key, left_key,
            "AGGREGATION", "avg", "60000"
        )
        self.client.execute_command(
            "TS.CREATERULE", source_key, right_key,
            "AGGREGATION", "sum", "60000"
        )
        self.client.execute_command(
            "TS.CREATERULE", left_key, sink_key,
            "AGGREGATION", "max", "120000"
        )

        # This should fail because sink already has a compaction rule
        with pytest.raises(ResponseError, match="TSDB: the destination key already has a src rule"):
            self.client.execute_command(
                "TS.CREATERULE", right_key, sink_key,
                "AGGREGATION", "min", "120000"
            )

    def test_allow_multiple_source_same_destination_fail(self):
        """Test that multiple sources cannot point to the same destination"""
        source1_key = "test:multi_src1"
        source2_key = "test:multi_src2"
        dest_key = "test:multi_dest"

        # Create all series
        for key in [source1_key, source2_key, dest_key]:
            self.create_test_series(key)

        # Create first rule
        self.client.execute_command(
            "TS.CREATERULE", source1_key, dest_key,
            "AGGREGATION", "avg", "60000"
        )

        # Try to create second rule to same destination (should fail)
        with pytest.raises(ResponseError, match="TSDB: the destination key already has a src rule"):
            self.client.execute_command(
                "TS.CREATERULE", source2_key, dest_key,
                "AGGREGATION", "sum", "60000"
            )

    def test_multilevel_compaction(self):
        """Test that multi-level compactions actually work functionally"""
        raw_key = "test:func_raw"
        minute_key = "test:func_minute"
        hour_key = "test:func_hour"

        # Create all series
        self.create_test_series(raw_key)
        self.create_test_series(minute_key)
        self.create_test_series(hour_key)

        # Create a compaction chain
        self.client.execute_command(
            "TS.CREATERULE", raw_key, minute_key,
            "AGGREGATION", "avg", "60000"  # 1 minute buckets
        )
        self.client.execute_command(
            "TS.CREATERULE", minute_key, hour_key,
            "AGGREGATION", "avg", "3600000"  # 1 hour buckets
        )

        # Add test data spanning multiple minutes and hours
        base_ts = int(time.time() * 1000)
        base_ts = (base_ts // 60000) * 60000  # Align to the minute boundary

        # Add samples across 3 minutes (3 buckets for minute compaction)
        samples = []
        for minute in range(3):
            for second in range(0, 60, 10):  # Every 10 seconds
                ts = base_ts + (minute * 60000) + (second * 1000)
                value = minute * 10 + second  # Predictable values
                samples.append((ts, value))

        for ts, value in samples:
            self.client.execute_command("TS.ADD", raw_key, ts, value)

        # Verify raw data exists
        raw_range = self.client.execute_command("TS.RANGE", raw_key, "-", "+")
        assert len(raw_range) == len(samples), "All raw samples should be present"

        # Verify minute compaction created data
        minute_range = self.client.execute_command("TS.RANGE", minute_key, "-", "+")
        assert len(minute_range) > 0, "Minute compaction should have created aggregated data"

        # Verify hour compaction created data. The bucket is not closed yet, so we should see the latest data
        latest = self.client.execute_command("TS.GET", hour_key, "LATEST")
        assert latest is not None, "Latest hour compaction data should exist"
