import pytest
from valkey import ResponseError

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTsQueryLabels(ValkeyTimeSeriesTestCaseBase):
    """Test cases for TS.QUERYLABELS.

    TS.QUERYLABELS <LABELS | VALUES label> [FILTER filterExpr [filterExpr ...]]
    returns the distinct label names (LABELS) or the distinct values of a single
    label (VALUES label) across the matching time series — or across every indexed
    series when FILTER is omitted. Unlike TS.QUERYINDEX it silently omits series the
    caller may not read.
    """

    def setup_test_data(self, client):
        """Create series with the canonical sensor label set (see the redis.io example)."""
        client.execute_command(
            'TS.CREATE', 'telemetry:study:temperature',
            'LABELS', 'room', 'study', 'type', 'temperature',
        )
        client.execute_command(
            'TS.CREATE', 'telemetry:study:humidity',
            'LABELS', 'room', 'study', 'type', 'humidity',
        )
        client.execute_command(
            'TS.CREATE', 'telemetry:kitchen:temperature',
            'LABELS', 'room', 'kitchen', 'type', 'temperature',
        )
        client.execute_command(
            'TS.CREATE', 'telemetry:kitchen:humidity',
            'LABELS', 'room', 'kitchen', 'type', 'humidity',
        )

    def test_labels_no_filter(self):
        """LABELS with no FILTER returns every distinct label name."""
        self.setup_test_data(self.client)
        result = self.client.execute_command('TS.QUERYLABELS', 'LABELS')
        assert sorted(result) == [b'room', b'type']

    def test_labels_with_filter(self):
        """LABELS with FILTER restricts the result to matching series."""
        self.setup_test_data(self.client)
        result = self.client.execute_command(
            'TS.QUERYLABELS', 'LABELS', 'FILTER', 'room=kitchen',
        )
        assert sorted(result) == [b'room', b'type']

        # A filter that only matches study series still yields the same label names.
        result = self.client.execute_command(
            'TS.QUERYLABELS', 'LABELS', 'FILTER', 'type=humidity',
        )
        assert sorted(result) == [b'room', b'type']

    def test_labels_with_compound_filter(self):
        self.setup_test_data(self.client)
        # Two conjunctive filters narrow the set; add a distinctive label.
        self.client.execute_command(
            'TS.CREATE', 'telemetry:study:pressure',
            'LABELS', 'room', 'study', 'type', 'pressure', 'unit', 'hpa',
        )
        result = self.client.execute_command(
            'TS.QUERYLABELS', 'LABELS', 'FILTER', 'room=study', 'type=pressure',
        )
        assert sorted(result) == [b'room', b'type', b'unit']

    def test_values_no_filter(self):
        """VALUES label with no FILTER returns every distinct value of the label."""
        self.setup_test_data(self.client)
        result = self.client.execute_command('TS.QUERYLABELS', 'VALUES', 'room')
        assert sorted(result) == [b'kitchen', b'study']

        result = self.client.execute_command('TS.QUERYLABELS', 'VALUES', 'type')
        assert sorted(result) == [b'humidity', b'temperature']

    def test_values_with_filter(self):
        self.setup_test_data(self.client)
        result = self.client.execute_command(
            'TS.QUERYLABELS', 'VALUES', 'room', 'FILTER', 'type=humidity',
        )
        assert sorted(result) == [b'kitchen', b'study']

        # A value that appears only outside the filter's match set is omitted.
        self.client.execute_command(
            'TS.CREATE', 'telemetry:garage:temperature',
            'LABELS', 'room', 'garage', 'type', 'temperature',
        )
        result = self.client.execute_command(
            'TS.QUERYLABELS', 'VALUES', 'room', 'FILTER', 'type=humidity',
        )
        assert sorted(result) == [b'kitchen', b'study']

    def test_values_label_absent_from_matching_series(self):
        """VALUES for a label no matching series carries returns an empty set, not an error."""
        self.setup_test_data(self.client)
        result = self.client.execute_command('TS.QUERYLABELS', 'VALUES', 'nope')
        assert result == []

    def test_no_match_returns_empty(self):
        self.setup_test_data(self.client)
        assert self.client.execute_command('TS.QUERYLABELS', 'LABELS', 'FILTER', 'room=nowhere') == []
        assert self.client.execute_command('TS.QUERYLABELS', 'VALUES', 'room', 'FILTER', 'room=nowhere') == []

    def test_empty_db_returns_empty(self):
        assert self.client.execute_command('TS.QUERYLABELS', 'LABELS') == []
        assert self.client.execute_command('TS.QUERYLABELS', 'VALUES', 'room') == []

    def test_case_insensitive_subtype(self):
        self.setup_test_data(self.client)
        assert sorted(self.client.execute_command('TS.QUERYLABELS', 'labels')) == [b'room', b'type']
        assert sorted(self.client.execute_command('TS.QUERYLABELS', 'values', 'room')) == [b'kitchen', b'study']

    def test_deduplication_across_series(self):
        """Each name/value appears at most once, however many series carry it."""
        self.setup_test_data(self.client)
        # Both temperature series share the `type=temperature` value; it must appear once.
        result = self.client.execute_command('TS.QUERYLABELS', 'VALUES', 'type')
        assert sorted(result) == [b'humidity', b'temperature']

    def test_regex_filter(self):
        self.setup_test_data(self.client)
        result = self.client.execute_command('TS.QUERYLABELS', 'VALUES', 'type', 'FILTER', 'room=~"kit.*"')
        assert sorted(result) == [b'humidity', b'temperature']

    # -- error cases -------------------------------------------------------

    def test_unknown_subtype(self):
        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command('TS.QUERYLABELS', 'FOO')
        assert "unknown subtype, must be one of LABELS|VALUES" in str(excinfo.value)

    def test_missing_label_after_values(self):
        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command('TS.QUERYLABELS', 'VALUES')
        assert "wrong number of arguments for 'ts.querylabels' command" in str(excinfo.value).lower()

    def test_no_arguments(self):
        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command('TS.QUERYLABELS')
        assert "wrong number of arguments for 'ts.querylabels' command" in str(excinfo.value).lower()

    def test_unknown_argument_after_subtype(self):
        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command('TS.QUERYLABELS', 'LABELS', 'FOO')
        assert "unknown argument, expected FILTER" in str(excinfo.value)

        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command('TS.QUERYLABELS', 'VALUES', 'type', 'FOO')
        assert "unknown argument, expected FILTER" in str(excinfo.value)

    def test_filter_with_no_expressions(self):
        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command('TS.QUERYLABELS', 'LABELS', 'FILTER')
        assert "FILTER given with no filter expressions" in str(excinfo.value)

    def test_filter_without_bounded_matcher(self):
        # A filter list that can only match the whole keyspace is rejected.
        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command('TS.QUERYLABELS', 'LABELS', 'FILTER', 'room!=')
        assert "please provide at least one matcher" in str(excinfo.value)

    def test_malformed_filter(self):
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.QUERYLABELS', 'LABELS', 'FILTER', 'room==')

    def test_acl_omits_unreadable_series(self):
        """TS.QUERYLABELS silently omits series the caller may not read (unlike TS.QUERYINDEX)."""
        self.setup_test_data(self.client)
        # The limited user may only read the kitchen series.
        self.client.execute_command(
            'ACL', 'SETUSER', 'ql_limited', 'ON', '>pw',
            '+@read', '+@timeseries', '~telemetry:kitchen:*',
        )
        try:
            limited = self.server.get_new_client()
            limited.execute_command('AUTH', 'ql_limited', 'pw')
            # Only kitchen series are readable, so study-only labels/values are omitted.
            assert sorted(limited.execute_command('TS.QUERYLABELS', 'LABELS')) == [b'room', b'type']
            assert sorted(limited.execute_command('TS.QUERYLABELS', 'VALUES', 'room')) == [b'kitchen']
            # A filter selecting only unreadable series yields an empty result, not an error.
            assert limited.execute_command('TS.QUERYLABELS', 'LABELS', 'FILTER', 'room=study') == []
            limited.close()
        finally:
            self.client.execute_command('ACL', 'DELUSER', 'ql_limited')
