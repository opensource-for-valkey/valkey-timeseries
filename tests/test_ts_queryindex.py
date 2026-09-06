import pytest
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTsQueryIndex(ValkeyTimeSeriesTestCaseBase):

    def assert_query_rejected(self, *filters):
        """Assert a TS.QUERYINDEX filter list is rejected for lacking a bounded matcher."""
        self.assert_filters_rejected('TS.QUERYINDEX', *filters)

    def setup_test_data(self, client):
        """Create a set of time series with different label combinations for testing"""
        # Create test series with various labels
        client.execute_command('TS.CREATE', 'ts1', 'LABELS', 'name', 'cpu', 'type', 'usage', 'node', 'node1')
        client.execute_command('TS.CREATE', 'ts2', 'LABELS', 'name', 'cpu', 'type', 'usage', 'node', 'node2')
        client.execute_command('TS.CREATE', 'ts3', 'LABELS', 'name', 'memory', 'type', 'usage', 'node', 'node1')
        client.execute_command('TS.CREATE', 'ts4', 'LABELS', 'name', 'memory', 'type', 'usage', 'node', 'node2')
        client.execute_command('TS.CREATE', 'ts5', 'LABELS', 'name', 'cpu', 'type', 'temperature', 'node', 'node1')
        client.execute_command('TS.CREATE', 'ts6', 'LABELS', 'name', 'cpu', 'node', 'node3')
        client.execute_command('TS.CREATE', 'ts7', 'LABELS', 'name', 'disk', 'type', 'usage', 'node', 'node3')
        client.execute_command('TS.CREATE', 'ts8', 'LABELS', 'type', 'usage')  # No name label

    def test_basic_query(self):
        """Test basic TS.QUERYINDEX functionality"""
        self.setup_test_data(self.client)

        # Query for all CPU metrics
        result = self.client.execute_command('TS.QUERYINDEX', 'name=cpu')
        assert result == [b'ts1', b'ts2', b'ts5', b'ts6']

        # Query for all metrics from node1
        result = self.client.execute_command('TS.QUERYINDEX', 'node=node1')
        assert result == [b'ts1', b'ts3', b'ts5']

    def test_compound_filters(self):
        """Test querying with multiple label conditions"""
        self.setup_test_data(self.client)

        # Query for CPU usage metrics
        result = self.client.execute_command('TS.QUERYINDEX', 'name=cpu', 'type=usage')
        assert result == [b'ts1', b'ts2']

        # Query for all usage metrics from node2
        result = self.client.execute_command('TS.QUERYINDEX', 'type=usage', 'node=node2')
        assert result == [b'ts2', b'ts4']

    def test_regex_matching(self):
        """Test querying with regex patterns"""
        self.setup_test_data(self.client)

        # Query for all metrics with a name matching 'c.*'
        result = self.client.execute_command('TS.QUERYINDEX', 'name=~"c.*"')
        assert result == [b'ts1', b'ts2', b'ts5', b'ts6']

        # Query for all metrics with a node matching 'node [12]'
        result = self.client.execute_command('TS.QUERYINDEX', 'node=~"node[12]"')
        assert result == [b'ts1', b'ts2', b'ts3', b'ts4', b'ts5']

        # Match using alternation
        result = self.client.execute_command('TS.QUERYINDEX', 'name=~"cpu|disk"')
        assert result == [b'ts1', b'ts2', b'ts5', b'ts6', b'ts7']

    def test_negative_matching(self):
        """Test querying with negation patterns"""
        self.setup_test_data(self.client)

        # A bare negative matcher is unbounded and rejected -- it must be paired with a
        # filter that cannot be satisfied by a missing label.
        self.assert_query_rejected('name!=cpu')
        self.assert_query_rejected('type!=usage')

        # Query for all usage metrics with a name not equal to cpu
        result = self.client.execute_command('TS.QUERYINDEX', 'type=usage', 'name!=cpu')
        assert result == [b'ts3', b'ts4', b'ts7', b'ts8']

        # Query for all named metrics with type not matching 'usage'
        result = self.client.execute_command('TS.QUERYINDEX', 'name=~".+"', 'type!=usage')
        assert result == [b'ts5', b'ts6']

    def test_prometheus_not_regex_matcher(self):
        """Test Prometheus-style regex negation matchers (label!~"regex")"""
        self.setup_test_data(self.client)

        # Negative regex matchers are unbounded on their own, so each is paired with
        # 'type=usage' (ts1-ts4, ts7, ts8) or 'name=~".+"' (ts1-ts7) to bound the query.
        self.assert_query_rejected('name!~"c.*"')
        self.assert_query_rejected('name!~"cpu|memory"')
        self.assert_query_rejected('node!~"node[12]"')

        # Not matching regex
        result = self.client.execute_command('TS.QUERYINDEX', 'type=usage', 'name!~"c.*"')
        assert result == [b'ts3', b'ts4', b'ts7', b'ts8']

        # Not matching regex alternation
        result = self.client.execute_command('TS.QUERYINDEX', 'type=usage', 'name!~"cpu|memory"')
        assert result == [b'ts7', b'ts8']

        # Not matching using character class. ts8 has no 'node' label -- it would satisfy the
        # negative matcher, but it has no 'name' either, so the bounding filter excludes it.
        result = self.client.execute_command('TS.QUERYINDEX', 'name=~".+"', 'node!~"node[12]"')
        assert result == [b'ts6', b'ts7']

    def test_complex_queries(self):
        """Test more complex query combinations"""
        self.setup_test_data(self.client)

        # CPU metrics that are not usage type
        result = self.client.execute_command('TS.QUERYINDEX', 'name=cpu', 'type!=usage')
        assert result == [b'ts5', b'ts6']

        # Non-CPU metrics that are usage type
        result = self.client.execute_command('TS.QUERYINDEX', 'name!=cpu', 'type=usage')
        assert result == [b'ts3', b'ts4', b'ts7', b'ts8']

        # Regex not matching, bounded by the 'type' filter
        result = self.client.execute_command('TS.QUERYINDEX', 'type=usage', 'name!~"c.*"')
        assert result == [b'ts3', b'ts4', b'ts7', b'ts8']

    def test_missing_labels(self):
        """Test querying for metrics with missing labels"""
        self.setup_test_data(self.client)

        # 'label=' matches every series lacking that label, so it cannot bound a query
        # on its own and must be paired with a bounded filter.
        self.assert_query_rejected('name=')
        self.assert_query_rejected('node=')

        # Find usage series without the 'name' label
        result = self.client.execute_command('TS.QUERYINDEX', 'type=usage', 'name=')
        assert result == [b'ts8']

        # Find usage series without the 'node' label
        result = self.client.execute_command('TS.QUERYINDEX', 'type=usage', 'node=')
        assert result == [b'ts8']

    def test_combined_operations(self):
        """Test combination of different operations"""
        self.setup_test_data(self.client)

        # Both filters here match a missing label -- `.*` matches empty and `!=` is negative --
        # so the list as a whole is unbounded and rejected. `.+` is the bounded form.
        self.assert_query_rejected('name=~".*"', 'type!=usage')

        # Find series that match regex but don't match another condition
        result = self.client.execute_command('TS.QUERYINDEX', 'name=~".+"', 'type!=usage')
        assert result == [b'ts5', b'ts6']

        # Mix of equals, not equals, and regex
        result = self.client.execute_command('TS.QUERYINDEX', 'name=cpu', 'node!=node1', 'type=~".*"')
        assert result == [b'ts2', b'ts6']

    def test_error_cases(self):
        """Test error conditions"""
        self.setup_test_data(self.client)

        # Empty query should return error
        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command('TS.QUERYINDEX')
        assert "wrong number of arguments for 'ts.queryindex' command" in str(excinfo.value).lower()

    def test_no_results(self):
        """Test queries that should return no results"""
        self.setup_test_data(self.client)

        # Query for non-existent label value
        result = self.client.execute_command('TS.QUERYINDEX', 'name=nonexistent')
        assert result == []

        # Query with impossible combination
        result = self.client.execute_command('TS.QUERYINDEX', 'name=cpu', 'name=memory')
        assert result == []

    def test_list_include_queries(self):
        """Test executing multiple QUERYINDEX commands sequentially"""
        self.setup_test_data(self.client)

        # First query
        result1 = sorted(self.client.execute_command('TS.QUERYINDEX', 'name=(cpu,disk)'))
        assert result1 == [b'ts1', b'ts2', b'ts5', b'ts6', b'ts7']

    def setup_or_test_data(self, client):
        client.execute_command('TS.CREATE', 'ts1', 'METRIC', 'http_status{status="200",method="GET"}')
        client.execute_command('TS.CREATE', 'ts2', 'METRIC', 'http_status{status="200",method="POST"}')
        client.execute_command('TS.CREATE', 'ts3', 'METRIC', 'http_status{status="404",method="GET"}')
        client.execute_command('TS.CREATE', 'ts4', 'METRIC', 'http_status{status="500",method="POST"}')
        client.execute_command('TS.CREATE', 'ts5', 'METRIC', 'api_host{name="server1",env="prod"}')
        client.execute_command('TS.CREATE', 'ts6', 'METRIC', 'api_host{name="server2",env="prod"}')
        client.execute_command('TS.CREATE', 'ts7', 'METRIC', 'api_host{name="server1",env="staging"}')
        client.execute_command('TS.CREATE', 'ts8', 'METRIC', 'api_host{name="server2",env="staging"}')

    def test_or_status_200_or_404(self):
        self.setup_or_test_data(self.client)
        result = self.client.execute_command('TS.QUERYINDEX', 'http_status{status="200" or status="404"}')
        assert result == [b'ts1', b'ts2', b'ts3']

    def test_or_multiple_metric_names(self):
        self.setup_or_test_data(self.client)
        result = self.client.execute_command('TS.QUERYINDEX',
                                             'http_status{method="GET"} or api_host{env=~"prod|staging"}')
        assert result == [b'ts1', b'ts3', b'ts5', b'ts6', b'ts7', b'ts8']

    def test_or_and_conditions(self):
        self.setup_or_test_data(self.client)
        result = self.client.execute_command('TS.QUERYINDEX',
                                             'api_host{name="server1",env="prod"} or api_host{name="server2",env="staging"}')
        assert result == [b'ts5', b'ts8']

    def test_or_regex_matchers(self):
        self.setup_or_test_data(self.client)
        result = self.client.execute_command('TS.QUERYINDEX',
                                             'http_status{method=~"GET$"} or http_status{method=~"POST$"}')
        assert result == [b'ts1', b'ts2', b'ts3', b'ts4']

    def test_or_not_equal(self):
        self.setup_or_test_data(self.client)
        result = self.client.execute_command('TS.QUERYINDEX',
                                             'http_status{status!="200",method="GET"} or api_host{env="prod"}')
        assert result == [b'ts3', b'ts5', b'ts6']

    def test_or_empty_branch(self):
        self.setup_or_test_data(self.client)
        result = self.client.execute_command('TS.QUERYINDEX', 'status=999 or env=prod')
        assert result == [b'ts5', b'ts6']

    def test_or_all_empty(self):
        self.setup_or_test_data(self.client)
        result = self.client.execute_command('TS.QUERYINDEX', 'status=999 or env=development')
        assert result == []

    def test_or_overlapping(self):
        self.setup_or_test_data(self.client)
        # OR: method=GET OR (method=GET AND status=200)
        result = self.client.execute_command('TS.QUERYINDEX',
                                             'http_status{method="GET"} or http_status{method="GET",status="200"}')
        assert result == [b'ts1', b'ts3']

    def test_or_regex_not_equal(self):
        self.setup_or_test_data(self.client)
        result = self.client.execute_command('TS.QUERYINDEX', 'http_status{method!~"GET"} or api_host{env="staging"}')
        assert result == [b'ts2', b'ts4', b'ts7', b'ts8']

    def test_filter_by_range(self):
        """Test querying with range filters on labels"""

        self.client.execute_command('TS.CREATE', 'ts9', 'LABELS', 'name', 'cpu', 'type', 'usage', 'node', 'node1',
                                    'load', '5')
        self.client.execute_command('TS.CREATE', 'ts10', 'LABELS', 'name', 'cpu', 'type', 'usage', 'node', 'node2',
                                    'load', '15')
        self.client.execute_command('TS.CREATE', 'ts11', 'LABELS', 'name', 'cpu', 'type', 'usage', 'node', 'node3',
                                    'load', '25')

        start_ts = 1000
        end_ts = 1000

        for i in range(20):
            ts = start_ts + (i * 1000)
            end_ts = ts
            self.client.execute_command('TS.ADD', 'ts9', ts, i * 10)
            self.client.execute_command('TS.ADD', 'ts11', ts, i * 30)

        # Query for series with data
        result = self.client.execute_command('TS.QUERYINDEX', 'FILTER_BY_RANGE', start_ts, end_ts, 'name=cpu')
        assert result == [b'ts11', b'ts9']

        # query for series without data in range
        result = self.client.execute_command('TS.QUERYINDEX', 'FILTER_BY_RANGE', 'NOT', start_ts, end_ts, 'name=cpu')
        assert result == [b'ts10']

    def test_hashtag_is_accepted_and_ignored_outside_a_cluster(self):
        """HASHTAG only scopes the cluster fanout, so a standalone server still answers in full."""
        self.setup_test_data(self.client)

        expected = [b'ts1', b'ts2', b'ts5', b'ts6']

        for args in (
            ('HASHTAG', 'anything'),
            ('HASHTAG', 'a,b,c'),
            ('HASHTAG', '{braced}'),
            ('hashtag', 'lowercase'),
        ):
            result = self.client.execute_command('TS.QUERYINDEX', *args, 'name=cpu')
            assert result == expected, args

    def test_hashtag_combines_with_filter_by_range_in_either_order(self):
        """Both leading options are recognised regardless of their relative order."""
        self.client.execute_command('TS.CREATE', 'ts:in', 'LABELS', 'name', 'cpu')
        self.client.execute_command('TS.CREATE', 'ts:out', 'LABELS', 'name', 'cpu')
        self.client.execute_command('TS.ADD', 'ts:in', 1000, 1)
        self.client.execute_command('TS.ADD', 'ts:out', 9000, 1)

        for args in (
            ('FILTER_BY_RANGE', 1000, 2000, 'HASHTAG', 'tag'),
            ('HASHTAG', 'tag', 'FILTER_BY_RANGE', 1000, 2000),
        ):
            result = self.client.execute_command('TS.QUERYINDEX', *args, 'name=cpu')
            assert result == [b'ts:in'], args

        result = self.client.execute_command('TS.QUERYINDEX', 'HASHTAG', 'tag',
                                             'FILTER_BY_RANGE', 'NOT', 1000, 2000, 'name=cpu')
        assert result == [b'ts:out']

    def test_hashtag_error_cases(self):
        """HASHTAG needs a non-empty value, and at least one selector must remain after it."""
        self.setup_test_data(self.client)

        # An empty value is rejected rather than treated as "no tags".
        with pytest.raises(ResponseError, match="missing HASHTAG argument"):
            self.client.execute_command('TS.QUERYINDEX', 'HASHTAG', '', 'name=cpu')

        # There is no FILTER keyword to delimit the tag list, so a selector placed where the
        # value belongs is swallowed as the tag list and no selector is left.
        with pytest.raises(ResponseError, match="please provide at least one matcher"):
            self.client.execute_command('TS.QUERYINDEX', 'HASHTAG', 'name=cpu')

        # Trailing HASHTAG is a selector position: `HASHTAG` and `tag` are read as bare
        # metric-name selectors (__name__="HASHTAG" and __name__="tag"), which no series
        # matches, so the query silently returns nothing instead of scoping a fanout.
        assert self.client.execute_command('TS.QUERYINDEX', 'name=cpu', 'HASHTAG', 'tag') == []
