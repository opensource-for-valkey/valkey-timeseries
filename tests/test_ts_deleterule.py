import pytest
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTSDeleteRule(ValkeyTimeSeriesTestCaseBase):

    def create_test_series_with_rule(self, source_key="source", dest_key="dest"):
        """Helper to create series with an existing rule."""
        self.client.execute_command("TS.CREATE", source_key, "LABELS", "type", "source")
        self.client.execute_command("TS.CREATE", dest_key, "LABELS", "type", "dest")
        self.client.execute_command("TS.CREATERULE", source_key, dest_key, "AGGREGATION", "SUM", "5s")
        return source_key, dest_key

    def test_delete_rule_basic_success(self):
        """Test basic successful deletion of a compaction rule."""
        source_key, dest_key = self.create_test_series_with_rule()

        # Verify rule exists
        info = self.ts_info(source_key)
        assert len(info["rules"]) == 1

        # Delete the rule
        result = self.client.execute_command("TS.DELETERULE", source_key, dest_key)
        assert result == b"OK"

        # Verify rule was deleted
        info = self.ts_info(source_key)
        assert len(info["rules"]) == 0

        # Verify dest series src_series field is cleared
        dest_info = self.ts_info(dest_key)
        assert "src_series" not in dest_info or dest_info["src_series"] is None

    def test_delete_rule_wrong_arity(self):
        """Test error handling for the wrong number of arguments."""
        source_key, dest_key = self.create_test_series_with_rule()

        # Too few arguments
        with pytest.raises(Exception) as exc_info:
            self.client.execute_command("TS.DELETERULE", source_key)
        assert "wrong number of arguments" in str(exc_info.value).lower()

        # Too many arguments
        with pytest.raises(Exception) as exc_info:
            self.client.execute_command("TS.DELETERULE", source_key, dest_key, "extra")
        assert "wrong number of arguments" in str(exc_info.value).lower()

    def test_delete_rule_nonexistent_source(self):
        """Test error when the source key doesn't exist."""
        dest_key = "dest"
        self.client.execute_command("TS.CREATE", dest_key)

        with pytest.raises(Exception) as exc_info:
            self.client.execute_command("TS.DELETERULE", "nonexistent", dest_key)
        assert "key does not exist" in str(exc_info.value).lower()

    def test_delete_rule_nonexistent_dest(self):
        """A destination that doesn't exist can't be the target of a rule, so it is
        reported as a missing rule rather than a missing key (RedisTimeSeries
        parity — it only ever reaches the destination through the source's rules)."""
        source_key = "source"
        self.client.execute_command("TS.CREATE", source_key)

        with pytest.raises(Exception) as exc_info:
            self.client.execute_command("TS.DELETERULE", source_key, "nonexistent")
        assert "compaction rule does not exist" in str(exc_info.value).lower()

    def test_delete_rule_no_rule_exists(self):
        """Test error when no rule exists between a source and dest."""
        source_key = "source"
        dest_key = "dest"
        self.client.execute_command("TS.CREATE", source_key)
        self.client.execute_command("TS.CREATE", dest_key)

        with pytest.raises(Exception, match="TSDB: compaction rule does not exist") as exc_info:
            self.client.execute_command("TS.DELETERULE", source_key, dest_key)

    def test_delete_rule_wrong_source(self):
        """Test error when dest series source doesn't match the provided source."""
        source1_key, dest_key = self.create_test_series_with_rule("source1", "dest")
        source2_key = "source2"
        self.client.execute_command("TS.CREATE", source2_key)

        with pytest.raises(Exception, match="TSDB: compaction rule does not exist") as exc_info:
            self.client.execute_command("TS.DELETERULE", source2_key, dest_key)

    def test_delete_rule_functional_verification(self):
        """Test that rule deletion actually stops compaction."""
        source_key, dest_key = self.create_test_series_with_rule()

        # Add some samples before deletion
        for i in range(5):
            self.client.execute_command("TS.ADD", source_key, 1000 + i * 1000, i * 10)

        # Delete the rule
        self.client.execute_command("TS.DELETERULE", source_key, dest_key)

        # Add more samples after deletion
        for i in range(5, 10):
            self.client.execute_command("TS.ADD", source_key, 1000 + i * 1000, i * 10)

        # Verify dest series is no longer updated
        dest_info = self.ts_info(dest_key)
        assert "src_series" not in dest_info or dest_info["src_series"] is None

        # Verify source series has no rules
        source_info = self.ts_info(source_key)
        assert len(source_info["rules"]) == 0

    def test_delete_rule_preserves_dest_series(self):
        """Test that deleting a rule doesn't delete the destination series."""
        source_key, dest_key = self.create_test_series_with_rule()

        # Add some data to dest through compaction
        for i in range(5):
            self.client.execute_command("TS.ADD", source_key, 1000 + i * 1000, i * 10)

        # Delete the rule
        self.client.execute_command("TS.DELETERULE", source_key, dest_key)

        # Verify dest series still exists and has data
        exists = self.client.execute_command("EXISTS", dest_key)
        assert exists == 1

        # Verify we can still query the dest series
        dest_samples = self.client.execute_command("TS.RANGE", dest_key, "-", "+")
        assert len(dest_samples) > 0

    def test_delete_rule_multiple_rules(self):
        """Test deleting one rule when the source has multiple rules."""
        source_key = "source"
        dest1_key = "dest1"
        dest2_key = "dest2"

        # Create series and rules
        self.client.execute_command("TS.CREATE", source_key)
        self.client.execute_command("TS.CREATE", dest1_key)
        self.client.execute_command("TS.CREATE", dest2_key)

        self.client.execute_command("TS.CREATERULE", source_key, dest1_key, "AGGREGATION", "SUM", "5s")
        self.client.execute_command("TS.CREATERULE", source_key, dest2_key, "AGGREGATION", "AVG", "10s")

        # Verify both rules exist
        info = self.ts_info(source_key)
        print(info)

        rules = info["rules"]
        assert len(info["rules"]) == 2

        # Delete one rule
        result = self.client.execute_command("TS.DELETERULE", source_key, dest1_key)
        assert result == b"OK"

        # Verify only one rule remains
        info = self.ts_info(source_key)
        rules = info["rules"]

        assert len(rules) == 1
        assert rules[0].dest_key == dest2_key

        # Verify dest1 src_series is cleared but dest2 is not
        dest1_info = self.ts_info(dest1_key)
        assert "sourceKey" not in dest1_info or dest1_info["sourceKey"] is None

        dest2_info = self.ts_info(dest2_key)
        assert dest2_info["sourceKey"] == source_key
