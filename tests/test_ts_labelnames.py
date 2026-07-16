import pytest
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from common import LabelSearchResponse, LabelValue


class TestTsLabelNames(ValkeyTimeSeriesTestCaseBase):

    def setup_test_data(self, client):
        """Create a set of time series with different label combinations for testing"""
        # Create multi series with various labels
        client.execute_command('TS.CREATE', 'ts1', 'LABELS', 'name', 'cpu', 'type', 'usage', 'node', 'node1', "ts1",
                               "1")
        client.execute_command('TS.CREATE', 'ts2', 'LABELS', 'name', 'cpu', 'type', 'usage', 'node', 'node2', "ts2",
                               "1")
        client.execute_command('TS.CREATE', 'ts3', 'LABELS', 'name', 'memory', 'type', 'usage', 'node', 'node1', "ts3",
                               "1")
        client.execute_command('TS.CREATE', 'ts4', 'LABELS', 'name', 'memory', 'type', 'usage', 'node', 'node2', "ts4",
                               "1")
        client.execute_command('TS.CREATE', 'ts5', 'LABELS', 'name', 'cpu', 'type', 'temperature', 'node', 'node1',
                               "ts5", "1")
        client.execute_command('TS.CREATE', 'ts6', 'LABELS', 'name', 'cpu', 'node', 'node3', "ts6",
                               "1")  # No type label
        client.execute_command('TS.CREATE', 'ts7', 'LABELS', 'name', 'disk', 'type', 'usage', 'node', 'node3', "ts7",
                               "1")
        client.execute_command('TS.CREATE', 'ts8', 'LABELS', 'type', 'usage', "ts8", "1")  # No name label
        client.execute_command('TS.CREATE', 'ts9', 'LABELS', 'location', 'datacenter', 'rack', 'rack1', "ts9",
                               "1")  # Different labels

    def exec_sorted_values(self, *args):
        """Helper method to execute a command and return a LabelValuesResponse"""
        result = self.client.execute_command('TS.LABELNAMES', *args)
        result = LabelSearchResponse.parse(result)
        values = sorted([lv.value for lv in result.results])
        return values

    def test_labelnames_with_filter(self):
        """Test TS.LABELNAMES with FILTER parameter"""
        self.setup_test_data(self.client)

        # Get label names for CPU series
        result = self.exec_sorted_values('FILTER', 'name=cpu')
        assert result == [b'name', b'node', b'ts1', b'ts2', b'ts5', b'ts6', b'type']

        # Get label names for node1 series
        result = self.exec_sorted_values('FILTER', 'node=node1')
        assert result == [b'name', b'node', b'ts1', b'ts3', b'ts5', b'type']

    def test_labelnames_with_multiple_filters(self):
        """Test TS.LABELNAMES with multiple filter conditions"""
        self.setup_test_data(self.client)

        # Get label names for CPU usage series
        result = self.exec_sorted_values('FILTER', 'name=cpu', 'type=usage')
        assert result == [b'name', b'node', b'ts1', b'ts2', b'type']

        # Get label names for series with location label
        result = self.exec_sorted_values('FILTER', 'location=datacenter')
        assert result == [b'location', b'rack', b'ts9']

    def test_labelnames_with_regex_filters(self):
        """Test TS.LABELNAMES with regex filter expressions"""
        self.setup_test_data(self.client)

        # Get label names for series where name matches regex
        result = self.exec_sorted_values('FILTER', 'name=~"c.*"')
        assert result == [b'name', b'node', b'ts1', b'ts2', b'ts5', b'ts6', b'type']

        # Get label names for series where node matches a pattern
        result = self.exec_sorted_values('FILTER', 'node=~"node[12]"')
        assert result == [b'name', b'node', b'ts1', b'ts2', b'ts3', b'ts4', b'ts5', b'type']

    def test_labelnames_with_time_range(self):
        """Test TS.LABELNAMES with time range filters"""
        self.setup_test_data(self.client)

        now = 1000

        # From setup_test_data, these series have name=cpu:
        # ts1: name=cpu, node=node1, type=user
        # ts2: name=cpu, node=node1, type=system
        # ts5: name=cpu, node=node2, type=user
        # ts6: name=cpu, node=node2, type=system

        # Add samples at different timestamps to series with name=cpu
        self.client.execute_command('TS.ADD', 'ts1', now, 10)
        self.client.execute_command('TS.ADD', 'ts2', now + 100, 20)
        self.client.execute_command('TS.ADD', 'ts5', now + 200, 30)
        self.client.execute_command('TS.ADD', 'ts6', now + 500, 40)

        # Query from timestamp 150 onwards - should only include ts5 and ts6
        # (ts1 at now=1000, ts2 at now+100=1100, ts5 at now+200=1200, ts6 at now+500=1500)
        result = self.exec_sorted_values('FILTER_BY_RANGE', now + 150, "+", 'FILTER', 'name=cpu')

        # Should return label names from ts5 (now+200) and ts6 (now+500)
        assert b'name' in result
        assert b'node' in result
        assert b'type' in result
        assert b'ts5' in result
        assert b'ts6' in result

        # Verify we're not getting labels from series outside the range
        # Both ts5 and ts6 have node=node2, so we should see that
        assert len(result) == 5  # name, node, ts5, ts6, type

        # Query with both START and END - should only include ts2 and ts5
        result = self.exec_sorted_values('FILTER_BY_RANGE', now + 50, now + 250, 'FILTER', 'name=cpu')

        # Should return labels from ts2 (now+100) and ts5 (now+200)
        assert b'name' in result
        assert b'node' in result
        assert b'type' in result
        assert b'ts2' in result
        assert b'ts5' in result
        assert len(result) == 5

        # Query that matches only one series
        result = self.exec_sorted_values('FILTER_BY_RANGE', now + 400, "+", 'FILTER', 'name=cpu')

        # Should only include ts6 (now+500)
        assert b'name' in result
        assert b'node' in result
        assert b'ts6' in result
        assert len(result) == 3

        # Test negative date range filtering with NOT
        # Exclude ts1 (now) and ts2 (now+100) - should return ts5 and ts6
        result = self.exec_sorted_values('FILTER_BY_RANGE', 'NOT', now, now + 150, 'FILTER', 'name=cpu')
        assert b'name' in result
        assert b'node' in result
        assert b'ts5' in result
        assert b'ts6' in result
        assert b'ts1' not in result
        assert b'ts2' not in result
        assert len(result) == 5  # name, node, ts5, ts6, type

        # Exclude middle series (ts2 and ts5) - should return ts1 and ts6
        result = self.exec_sorted_values('FILTER_BY_RANGE', 'NOT', now + 50, now + 250, 'FILTER', 'name=cpu')
        assert b'name' in result
        assert b'node' in result
        assert b'type' in result
        assert b'ts1' in result
        assert b'ts6' in result
        assert b'ts2' not in result
        assert b'ts5' not in result
        assert len(result) == 5

        # Exclude ts6 (now+500) - should return ts1, ts2, ts5
        result = self.exec_sorted_values('FILTER_BY_RANGE', 'NOT', now + 400, "+", 'FILTER', 'name=cpu')
        assert b'name' in result
        assert b'node' in result
        assert b'type' in result
        assert b'ts1' in result
        assert b'ts2' in result
        assert b'ts5' in result
        assert b'ts6' not in result
        assert len(result) == 6

        # Exclude early data - should only include ts6
        result = self.exec_sorted_values('FILTER_BY_RANGE', 'NOT', "-", now + 250, 'FILTER', 'name=cpu')
        assert b'name' in result
        assert b'node' in result
        assert b'ts6' in result
        assert b'ts1' not in result
        assert b'ts2' not in result
        assert b'ts5' not in result
        assert len(result) == 3

        # Exclude late data (ts6) - should return ts1, ts2, ts5
        result = self.exec_sorted_values('FILTER_BY_RANGE', 'NOT', now + 400, "+", 'FILTER', 'name=cpu')
        assert b'name' in result
        assert b'node' in result
        assert b'type' in result
        assert b'ts1' in result
        assert b'ts2' in result
        assert b'ts5' in result
        assert b'ts6' not in result
        assert len(result) == 6

    def test_labelnames_with_combined_parameters(self):
        """Test TS.LABELNAMES with combined filter and time range parameters"""
        self.setup_test_data(self.client)

        # Add samples with different timestamps
        now = 1000

        # Add samples to different series at different times
        self.client.execute_command('TS.ADD', 'ts1', now, 10)
        self.client.execute_command('TS.ADD', 'ts2', now + 100, 20)
        self.client.execute_command('TS.ADD', 'ts3', now + 200, 30)
        self.client.execute_command('TS.ADD', 'ts9', now + 500, 40)

        # Query with filter and time range
        result = self.exec_sorted_values('FILTER_BY_RANGE', now, now + 150, 'FILTER', 'name=~"c.*"')

        assert result == [b'name', b'node', b'ts1', b'ts2', b'type']

    def test_labelnames_empty_result(self):
        """Test TS.LABELNAMES when no series match the criteria"""
        self.setup_test_data(self.client)

        # Filter that doesn't match any series
        result = self.exec_sorted_values('FILTER', 'nonexistent=value')
        assert result == []

        # Time range that doesn't match any samples
        now = 1000
        self.client.execute_command('TS.ADD', 'ts1', now, 10)
        result = self.exec_sorted_values('FILTER_BY_RANGE', now + 2000, "+", 'FILTER', 'name=cpu')
        assert result == []

    def test_labelnames_with_limit(self):
        """Test TS.LABELNAMES with LIMIT parameter"""
        self.setup_test_data(self.client)

        # Get top 2 label names
        result = self.exec_sorted_values('LIMIT', 2, 'FILTER', 'name=cpu')
        assert len(result) == 2

        # Verify sorted order (alphabetical)
        all_labels = [b'name', b'node', b'ts1', b'ts2', b'ts5', b'ts6', b'type']
        assert result == all_labels[:2]

        expected = all_labels[:5]
        result = self.exec_sorted_values('LIMIT', 5, 'FILTER', 'name=cpu')
        assert result == expected

    def test_labelnames_error_cases(self):
        """Test error conditions for TS.LABELNAMES"""
        self.setup_test_data(self.client)

        # Invalid filter format
        # This is invalid in redis, but this is a valid Prometheus filter (equivalent to {__name__="invalid_filter"})
        # or invalid_filter{}
        # self.verify_error_response(self.client, 'TS.LABELNAMES FILTER invalid_filter',
        #                            "Invalid filter: invalid_filter")

        # Invalid time format
        self.verify_error_response(self.client, 'TS.LABELNAMES FILTER_BY_RANGE invalid_time',
                                   "TSDB: wrong fromTimestamp")

        # Invalid limit format
        self.verify_error_response(self.client, 'TS.LABELNAMES LIMIT invalid_limit',
                                   "TSDB: invalid LIMIT value")

    def test_labelnames_after_series_deletion(self):
        """Test TS.LABELNAMES after deleting time series"""
        self.setup_test_data(self.client)

        # Delete series with unique labels
        self.client.execute_command('DEL', 'ts9')

        # Verify unique labels are no longer returned
        result = self.exec_sorted_values('FILTER', 'location=datacenter')
        assert result == []

    def test_labelnames_with_complex_filters(self):
        """Test TS.LABELNAMES with complex filter combinations"""
        self.setup_test_data(self.client)

        # Complex filter: CPU metrics that are not a usage type
        result = self.exec_sorted_values('FILTER', 'name=cpu', 'type!=usage')

        assert result == [b'name', b'node', b'ts5', b'ts6', b'type']

        # A lone negative regex matcher is unbounded and rejected.
        self.assert_filters_rejected('TS.LABELNAMES', 'FILTER', 'node!~"node[12]"')

        # Complex filter with regex: nodes that don't match the pattern. 'name=~".+"' supplies
        # the positive matcher, which also excludes ts8 and ts9 -- neither has a 'name' label.
        result = self.exec_sorted_values('FILTER', 'name=~".+"', 'node!~"node[12]"')
        assert result == [b'name', b'node', b'ts6', b'ts7', b'type']

    def test_labelnames_with_empty_database(self):
        """Test TS.LABELNAMES with an empty database"""
        # Ensure that the database is empty
        self.client.execute_command('FLUSHALL')

        # Verify no labels are returned
        result = self.exec_sorted_values('FILTER', 'name=cpu')
        assert result == []

    # -------------------------------------------------------------------------
    # Fuzzy search tests
    # -------------------------------------------------------------------------

    def setup_fuzzy_test_data(self, client):
        """Create series whose label names exercise fuzzy / prefix matching."""
        # label names: metric, metrics, meta, node, node_id, type, team, env
        client.execute_command(
            'TS.CREATE', 'fts1',
            'LABELS',
            'metric', 'cpu',
            'node', 'n1',
            'type', 'gauge',
            'env', 'prod',
        )
        client.execute_command(
            'TS.CREATE', 'fts2',
            'LABELS',
            'metrics', 'disk',
            'node_id', '42',
            'team', 'infra',
            'env', 'staging',
        )
        client.execute_command(
            'TS.CREATE', 'fts3',
            'LABELS',
            'meta', 'extra',
            'node', 'n2',
            'type', 'counter',
        )

    def _exec_fuzzy(self, *args):
        """Run TS.LABELNAMES and return a LabelSearchResponse."""
        raw = self.client.execute_command('TS.LABELNAMES', *args)
        return LabelSearchResponse.parse(raw)

    def test_labelnames_search_exact_term(self):
        """SEARCH with an exact match should return labels that equal (or
        score highly for) the search term."""
        self.setup_fuzzy_test_data(self.client)

        result = self._exec_fuzzy('SEARCH', 'node', 'FILTER', 'env=prod')
        values = [lv.value for lv in result.results]
        # 'node' should be in the results; it is an exact label name in fts1
        assert b'node' in values

    def test_labelnames_search_prefix(self):
        """SEARCH with a prefix should match labels that start with that prefix."""
        self.setup_fuzzy_test_data(self.client)

        # 'met' is a common prefix of 'metric', 'metrics', 'meta'
        result = self._exec_fuzzy('SEARCH', 'met')
        values = [lv.value for lv in result.results]
        # at least one of the 'met*' label names must appear
        assert any(v in values for v in [b'metric', b'metrics', b'meta']), (
            f"Expected at least one 'met*' label name in {values}"
        )

    def test_labelnames_search_unscoped_by_label_name(self):
        """Unscoped SEARCH (no FILTER) should find label names across the entire index."""
        # Create a small set of series with label names that share a common prefix
        self.client.execute_command(
            'TS.CREATE', 'ls1', 'LABELS', 'region', 'us', 'service', 'api'
        )
        self.client.execute_command(
            'TS.CREATE', 'ls2', 'LABELS', 'reg', 'abc', 'region_code', 'us-east'
        )
        self.client.execute_command(
            'TS.CREATE', 'ls3', 'LABELS', 'meta', 'x'
        )

        # Search for the prefix 'reg' without any FILTER — this exercises the unscoped code path
        result = self._exec_fuzzy('SEARCH', 'reg')
        values = [lv.value for lv in result.results]

        # Expect at least one of the label names that start with 'reg' to be present
        assert any(v in values for v in [b'reg', b'region', b'region_code']), (
            f"Expected at least one 'reg*' label name in {values}"
        )

    def test_labelnames_unscoped_no_args(self):
        """TS.LABELNAMES with no arguments should return all label names in the index."""
        # Create series with several distinct label names
        self.client.execute_command('TS.CREATE', 'ua1', 'LABELS', 'aaa', '1', 'bbb', '2')
        self.client.execute_command('TS.CREATE', 'ua2', 'LABELS', 'ccc', '3', 'aaa', '4')

        # Call TS.LABELNAMES with no args (unscoped) and collect sorted values
        result = self.exec_sorted_values()

        # Ensure all label names created are present
        assert b'aaa' in result
        assert b'bbb' in result
        assert b'ccc' in result

    def test_labelnames_search_returns_highest_scores_first(self):
        """Default ordering when SEARCH is specified should be by relevance
        score descending."""
        self.setup_fuzzy_test_data(self.client)

        # 'node' exactly matches the label name 'node'; 'node_id' is close
        result = self._exec_fuzzy(
            'SEARCH', 'node',
            'INCLUDE_METADATA',
        )
        print(f"SEARCH 'node' results with scores: {[(lv.value, lv.score) for lv in result.results]}")
        assert len(result.results) >= 1
        # Results must be ordered score DESC (each score <= previous)
        scores = [lv.score for lv in result.results if lv.score is not None]
        assert scores == sorted(scores, reverse=True), (
            f"Results not sorted by score desc: {scores}"
        )
        # The exact match 'node' must have the highest score
        top = result.results[0]
        assert top.value == b'node', (
            f"Expected 'node' as top match, got {top.value!r}"
        )

    def test_labelnames_search_include_metadata(self):
        """INCLUDE_METADATA should include score and cardinality fields."""
        self.setup_fuzzy_test_data(self.client)

        result = self._exec_fuzzy(
            'SEARCH', 'type',
            'INCLUDE_METADATA',
        )
        assert len(result.results) >= 1
        for lv in result.results:
            assert lv.score is not None, f"score missing for {lv.value!r}"
            assert lv.cardinality is not None, (
                f"cardinality missing for {lv.value!r}"
            )
            assert isinstance(lv.cardinality, int)
            assert lv.cardinality >= 1

    def test_labelnames_search_fuzz_threshold_strict(self):
        """A high FUZZ_THRESHOLD should restrict results to very close matches."""
        self.setup_fuzzy_test_data(self.client)

        # threshold=0.99 should only accept near-perfect matches for 'metric'
        result_strict = self._exec_fuzzy(
            'SEARCH', 'metric',
            'FUZZY_THRESHOLD', '0.99',
        )
        values_strict = {lv.value for lv in result_strict.results}

        # A lenient threshold should return more results
        result_lenient = self._exec_fuzzy(
            'SEARCH', 'metric',
            'FUZZY_THRESHOLD', '0.5',
        )
        values_lenient = {lv.value for lv in result_lenient.results}

        # Strict should be a subset of (or equal to) lenient results
        assert values_strict.issubset(values_lenient), (
            f"Strict results {values_strict} not a subset of lenient {values_lenient}"
        )

    def test_labelnames_search_fuzz_algo_jarowinkler(self):
        """FUZZY_ALGO jarowinkler should produce results for a close typo."""
        self.setup_fuzzy_test_data(self.client)

        # 'metirc' is a transposition of 'metric' — JaroWinkler handles it well
        result = self._exec_fuzzy(
            'SEARCH', 'metirc',
            'FUZZY_ALGORITHM', 'jarowinkler',
            'FUZZY_THRESHOLD', '0.5',
        )
        values = [lv.value for lv in result.results]
        assert b'metric' in values or b'metrics' in values, (
            f"Expected 'metric'/'metrics' in JaroWinkler results, got {values}"
        )

    def test_labelnames_search_fuzz_algo_subsequence(self):
        """FUZZY_ALGORITHM subsequence should match labels containing the term
        as a subsequence."""
        self.setup_fuzzy_test_data(self.client)

        # 'nde' is a subsequence of 'node' and 'node_id'
        result = self._exec_fuzzy(
            'SEARCH', 'nde',
            'FUZZY_ALGORITHM', 'subsequence',
            'FUZZY_THRESHOLD', '0.3',
        )
        values = [lv.value for lv in result.results]
        assert any(v in values for v in [b'node', b'node_id']), (
            f"Expected 'node' or 'node_id' in subsequence results, got {values}"
        )

    def test_labelnames_search_ignore_case(self):
        """IGNORE_CASE true should match label names regardless of case."""
        self.setup_fuzzy_test_data(self.client)

        result_no_ignore = self._exec_fuzzy(
            'SEARCH', 'NODE',
            'FUZZY_THRESHOLD', '0.9',
        )
        result_ignore = self._exec_fuzzy(
            'SEARCH', 'NODE',
            'IGNORE_CASE', 
            'FUZZY_THRESHOLD', '0.9',
        )
        values_ignore = [lv.value for lv in result_ignore.results]
        values_no_ignore = [lv.value for lv in result_no_ignore.results]

        # Case-insensitive must find 'node'; case-sensitive may miss it
        assert b'node' in values_ignore, (
            f"'node' not found with IGNORE_CASE true; got {values_ignore}"
        )
        # case-sensitive with all-uppercase 'NODE' should score lower /
        # potentially not match exact label 'node' — it must have fewer
        # high-confidence matches than the case-insensitive search
        assert len(values_ignore) >= len(values_no_ignore)

    def test_labelnames_search_sortby_value(self):
        """SORTBY value ASC should return results in alphabetical order."""
        self.setup_fuzzy_test_data(self.client)

        result = self._exec_fuzzy(
            'SEARCH', 'me',
            'FUZZY_THRESHOLD', '0.3',
            'SORTBY', 'value', 'ASC',
        )
        values = [lv.value for lv in result.results]
        assert values == sorted(values), (
            f"Results not sorted alphabetically: {values}"
        )

    def test_labelnames_search_sortby_value_desc(self):
        """SORTBY value DESC should return results in reverse alphabetical order."""
        self.setup_fuzzy_test_data(self.client)

        result = self._exec_fuzzy(
            'SEARCH', 'me',
            'FUZZY_THRESHOLD', '0.3',
            'SORTBY', 'value', 'DESC',
        )
        values = [lv.value for lv in result.results]
        assert values == sorted(values, reverse=True), (
            f"Results not sorted reverse-alphabetically: {values}"
        )

    def test_labelnames_search_sortby_cardinality(self):
        """SORTBY cardinality with INCLUDE_METADATA should order by cardinality."""
        self.setup_fuzzy_test_data(self.client)

        result = self._exec_fuzzy(
            'SEARCH', 'n',
            'FUZZY_THRESHOLD', '0.1',
            'SORTBY', 'cardinality', 'ASC',
            'INCLUDE_METADATA',
        )
        cards = [lv.cardinality for lv in result.results if lv.cardinality is not None]
        assert cards == sorted(cards), (
            f"Results not sorted by cardinality ASC: {cards}"
        )

    def test_labelnames_search_with_filter(self):
        """SEARCH combined with FILTER should restrict candidates before scoring."""
        self.setup_fuzzy_test_data(self.client)

        # Only fts2 has env=staging; its label names are metrics, node_id, team, env
        result = self._exec_fuzzy(
            'SEARCH', 'node',
            'FUZZY_THRESHOLD', '0.5',
            'FILTER', 'env=staging',
        )
        values = [lv.value for lv in result.results]
        # 'node_id' is in fts2's labels and should match 'node' fuzzily
        assert b'node_id' in values, (
            f"Expected 'node_id' in results for env=staging search, got {values}"
        )
        # 'node' belongs only to fts1/fts3 which are excluded by the filter
        assert b'node' not in values, (
            f"'node' should be excluded when filtered to env=staging, got {values}"
        )

    def test_labelnames_search_with_limit(self):
        """LIMIT should cap the number of fuzzy-search results returned."""
        self.setup_fuzzy_test_data(self.client)

        result = self._exec_fuzzy(
            'SEARCH', 'e',
            'FUZZY_THRESHOLD', '0.1',
            'LIMIT', '2',
        )
        assert len(result.results) <= 2, (
            f"Expected at most 2 results with LIMIT 2, got {len(result.results)}"
        )

    def test_labelnames_search_no_match(self):
        """A SEARCH term with zero matching labels should return an empty result."""
        self.setup_fuzzy_test_data(self.client)

        # 'zzzzzzzzz' is very unlikely to match any label name fuzzily at a
        # high threshold
        result = self._exec_fuzzy(
            'SEARCH', 'zzzzzzzzz',
            'FUZZY_THRESHOLD', '0.99',
        )
        assert result.results == [], (
            f"Expected empty results for nonsense term, got {result.results}"
        )

    def test_labelnames_search_multiple_terms(self):
        """Multiple SEARCH terms should aggregate matches from all terms."""
        self.setup_fuzzy_test_data(self.client)

        # 'node' matches node/node_id; 'type' matches type
        result = self._exec_fuzzy(
            'SEARCH', 'node', 'type',
            'FUZZY_THRESHOLD', '0.8',
            'SORTBY', 'value', 'ASC',
        )
        values = [lv.value for lv in result.results]
        assert b'node' in values, f"'node' not found in multi-term results: {values}"
        assert b'type' in values, f"'type' not found in multi-term results: {values}"
