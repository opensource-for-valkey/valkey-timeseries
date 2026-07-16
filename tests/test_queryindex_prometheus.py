import pytest
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTsQueryIndex(ValkeyTimeSeriesTestCaseBase):
    """
    Test cases for TS.QUERYINDEX command with Prometheus matching semantics. Based on the tests
    used by the Prometheus project for their label matching logic.
    """

    def assert_query_rejected(self, *filters):
        """Assert a TS.QUERYINDEX filter list is rejected for lacking a bounded matcher."""
        self.assert_filters_rejected('TS.QUERYINDEX', *filters)

    def setup_test_data(self, client):
        """Create a set of time series with different label combinations for testing"""
        # Create test series with various labels
        client.execute_command('TS.CREATE', 'ts1', 'LABELS', 'n', '1')
        client.execute_command('TS.CREATE', 'ts2', 'LABELS', 'n', '1', 'i', 'a')
        client.execute_command('TS.CREATE', 'ts3', 'LABELS', 'n', '1', 'i', 'b')
        client.execute_command('TS.CREATE', 'ts4', 'LABELS', 'n', '1', 'i', '\n')
        client.execute_command('TS.CREATE', 'ts5', 'LABELS', 'n', '2')
        client.execute_command('TS.CREATE', 'ts6', 'LABELS', 'n', '2.5')
        client.execute_command('TS.CREATE', 'ts7', 'LABELS', 'i', 'c')
        client.execute_command('TS.CREATE', 'ts8', 'LABELS', 'complex', 'val1&val2')

    def test_basic_equal_matching(self):
        """Test simple equality matching of TS.QUERYINDEX"""
        self.setup_test_data(self.client)

        # Basic filter matching all 'n=1' series
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'n="1"'))
        assert result == [b'ts1', b'ts2', b'ts3', b'ts4']

        # Match specific label combination
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'n="1"', 'i=a'))
        assert result == [b'ts2']

        # Non-existent value
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'n=nonexistent'))
        assert result == []

    def test_empty_label_filtering(self):
        """Test filtering for series with or without specific labels"""
        self.setup_test_data(self.client)

        # 'i=' means "series lacking label i" -- satisfied by every series without the label,
        # so on its own it is unbounded and rejected.
        self.assert_query_rejected('i=')

        # Paired with a bounded filter it is allowed, and narrows the result.
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'n=1', 'i='))
        assert result == [b'ts1']

        # Find series with 'i' label
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'i=~".+"'))
        assert result == [b'ts2', b'ts3', b'ts4', b'ts7']

    def test_not_equal_matching(self):
        """Test negation matching with TS.QUERYINDEX"""
        self.setup_test_data(self.client)

        # A lone negative matcher is unbounded and rejected.
        self.assert_query_rejected('n!=1')

        # Combine equality and negation -- 'n=1' bounds the query, so 'i!=a' is allowed.
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'n=1', 'i!=a'))
        assert result == [b'ts1', b'ts3', b'ts4']

        # Negation of empty value ("has the label set") is still a negative matcher.
        self.assert_query_rejected('i!=')

    def test_regex_matching(self):
        """Test regex matching capabilities of TS.QUERYINDEX"""
        self.setup_test_data(self.client)

        # Match with regex pattern
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'n=~"^1$"'))
        assert result == [b'ts1', b'ts2', b'ts3', b'ts4']

        # Match with OR pattern
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'n=~"1|2"'))
        assert result == [b'ts1', b'ts2', b'ts3', b'ts4', b'ts5']

        # `.*` matches the empty string, so `n=~".*"` is satisfied by series that lack 'n'
        # entirely -- it selects everything and is rejected on its own.
        self.assert_query_rejected('n=~".*"')

        # Match non-empty values with .+ -- this cannot match a missing label, so it is bounded.
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'i=~".+"'))
        assert result == [b'ts2', b'ts3', b'ts4', b'ts7']

    def test_regex_not_matching(self):
        """Test regex negation matching of TS.QUERYINDEX"""
        self.setup_test_data(self.client)

        # Negative regex matchers are unbounded on their own.
        self.assert_query_rejected('n!~"^1$"')
        self.assert_query_rejected('n!~"1|2"')

        # `n!~".*"` is the exception: it normalizes to "match nothing", so it can only ever
        # return the empty set and is bounded rather than a keyspace scan.
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'n!~".*"'))
        assert result == []

        # Paired with a bounded filter they are allowed.
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'i=~".+"', 'n!~"^1$"'))
        assert result == [b'ts7']

    def test_complex_combinations(self):
        """Test more complex query combinations"""
        self.setup_test_data(self.client)

        # Combination of equals, not equals and regex
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'n=1', 'i!=a', 'i=~".*"'))
        assert result == [b'ts1', b'ts3', b'ts4']

        # Using multiple mutually exclusive conditions
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'n=1', 'n=2'))
        assert result == []

        # Complex regex pattern
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'n=~"^[12].*$"'))
        assert result == [b'ts1', b'ts2', b'ts3', b'ts4', b'ts5', b'ts6']

    def test_special_characters(self):
        """Test handling of special characters in labels and values"""
        self.setup_test_data(self.client)

        # Match newline character
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'i="\n"'))
        assert result == [b'ts4']

        # Match with regex for a special character
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'i=~"\\n"'))
        assert result == [b'ts4']

        # Match ampersand in value
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'complex="val1&val2"'))
        assert result == [b'ts8']

    def test_error_cases(self):
        """Test error conditions with TS.QUERYINDEX"""
        self.setup_test_data(self.client)

        # Empty query should return error. Commands are registered in lowercase
        # (matching RedisTimeSeries), so the arity error echoes the lowercase name.
        with pytest.raises(ResponseError, match="wrong number of arguments for 'ts.queryindex' command"):
            self.client.execute_command('TS.QUERYINDEX')

        # Invalid filter format
        # Commented out because any valid identifier is possibly a Prometheus vector selector.
        # self.verify_error_response(self.client, 'TS.QUERYINDEX invalid_filter',
        #                            "Invalid filter: invalid_filter")

        # Invalid regex pattern
        with pytest.raises(ResponseError) as execInfo:
            self.client.execute_command("TS.QUERYINDEX", "n=~[abc")

        assert "parse error: unexpected token \"[\"" in str(execInfo.value)

    def test_empty_result_cases(self):
        """Test cases that should return empty results"""
        self.setup_test_data(self.client)

        # Non-existent label value
        result = self.client.execute_command('TS.QUERYINDEX', 'n=nonexistent')
        assert result == []

        # Impossible combination
        result = self.client.execute_command('TS.QUERYINDEX', 'n=1', 'n=2')
        assert result == []

        # Combination of regex patterns that can't be satisfied
        result = self.client.execute_command('TS.QUERYINDEX', 'i=~"a.*"', 'i=~"b.*"')
        assert result == []

    def test_query_with_multiple_filters(self):
        """Test multiple filter queries in a single command"""
        self.setup_test_data(self.client)

        # Using multiple independent filters
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'n=1', 'i=a'))
        assert result == [b'ts2']

        # Multiple filters with regex
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'n=~"^[12]$"', 'i=~"[ab]"'))
        assert result == [b'ts2', b'ts3']

        # Multiple filters with negation
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'n=1', 'i!=', 'i!=a'))
        assert result == [b'ts3', b'ts4']

    def test_match_all_patterns(self):
        """Test special patterns that match all or none"""
        self.setup_test_data(self.client)

        # A bare wildcard selects every series, bounded by nothing, so it is rejected.
        self.assert_query_rejected('n=~".*"')

        # Match all and filter with another condition
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'n=~".*"', 'i=a'))
        assert result == [b'ts2']

        # Using .+ to match non-empty values only
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'i=~".+"'))
        assert result == [b'ts2', b'ts3', b'ts4', b'ts7']

        # Not matching anything
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'n!~".*"', 'i!~".*"'))
        assert result == []  # No series can satisfy this