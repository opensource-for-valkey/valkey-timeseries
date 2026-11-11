import pytest
from valkey import ValkeyCluster
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesClusterTestCase


class TestTimeSeriesMultiAggregationClustered(ValkeyTimeSeriesClusterTestCase):
    """Cluster integration tests for multi-aggregation over TS.MRANGE /
    TS.MREVRANGE. These exercise the shard-side aggregation push-down path:
    each series lives on one shard, so per-series bucket rows are computed
    shard-side and shipped as one chunk per aggregation column; the
    coordinator merges/reduces/reverses/counts without re-aggregating.

    Series are spread across hash slots (via {slotN} hash tags) so the query
    genuinely fans out to multiple shards."""

    def setup_clustered_data(self):
        cluster_client: ValkeyCluster = self.new_cluster_client()

        # region=us spans two slots (a on slot1, c on slot2); region=eu likewise.
        cluster_client.execute_command('TS.CREATE', 'm:{slot1}:a', 'LABELS', 'region', 'us', 'kind', 'x')
        cluster_client.execute_command('TS.CREATE', 'm:{slot1}:b', 'LABELS', 'region', 'eu', 'kind', 'y')
        cluster_client.execute_command('TS.CREATE', 'm:{slot2}:c', 'LABELS', 'region', 'us', 'kind', 'z')
        cluster_client.execute_command('TS.CREATE', 'm:{slot2}:d', 'LABELS', 'region', 'eu', 'kind', 'w')

        # a: bucket[1000,2000)=avg 3/max 4; (single bucket)
        cluster_client.execute_command('TS.ADD', 'm:{slot1}:a', 1000, 2.0)
        cluster_client.execute_command('TS.ADD', 'm:{slot1}:a', 1500, 4.0)
        # c: bucket[1000,2000)=avg 10/max 10; bucket[2000,3000)=avg 6/max 6
        cluster_client.execute_command('TS.ADD', 'm:{slot2}:c', 1000, 10.0)
        cluster_client.execute_command('TS.ADD', 'm:{slot2}:c', 2500, 6.0)
        # b (eu): bucket[1000,2000)=avg 20/max 30/count 2; bucket[2000,3000)=count 1
        cluster_client.execute_command('TS.ADD', 'm:{slot1}:b', 1000, 10.0)
        cluster_client.execute_command('TS.ADD', 'm:{slot1}:b', 1500, 30.0)
        cluster_client.execute_command('TS.ADD', 'm:{slot1}:b', 2000, 20.0)
        # d (eu): bucket[1000,2000)=avg 5/max 5
        cluster_client.execute_command('TS.ADD', 'm:{slot2}:d', 1000, 5.0)

    @staticmethod
    def series_by_key(result):
        return {series[0]: series[2] for series in result}

    def test_mrange_cme_rejects_unbounded_filter(self):
        """A 'match everything' filter list must still carry a positive matcher.

        The tests below use 'kind=~".+"' to select every series. The older idiom
        'kind!=none' selects the same set but is a negative matcher, so a filter list
        containing only that is rejected as an unbounded keyspace scan.
        """
        self.setup_clustered_data()
        client = self.new_client_for_primary(0)

        self.assert_filters_rejected(
            'TS.MRANGE', '-', '+', 'AGGREGATION', 'avg', 1000,
            'FILTER', 'kind!=none', client=client)

        # The bounded form selects every series, since all of them have a 'kind' label.
        result = client.execute_command(
            'TS.MRANGE', '-', '+', 'AGGREGATION', 'avg', 1000, 'FILTER', 'kind=~".+"')
        assert len(result) == 4

    def test_mrange_cme_multi_aggregation_basic(self):
        """Per-series multi-aggregation across slots: each bucket row is
        [ts, avg, max, count] in the requested column order."""
        self.setup_clustered_data()
        client = self.new_client_for_primary(0)

        result = client.execute_command(
            'TS.MRANGE', '-', '+',
            'AGGREGATION', 'avg,max,count', 1000,
            'FILTER', 'region=us')

        assert len(result) == 2
        by_key = self.series_by_key(result)
        # a: one bucket
        assert by_key[b'm:{slot1}:a'] == [[1000, b'3', b'4', b'2']]
        # c: two buckets across the range
        assert by_key[b'm:{slot2}:c'] == [
            [1000, b'10', b'10', b'1'],
            [2000, b'6', b'6', b'1'],
        ]

    def test_mrange_cme_multi_aggregation_column_matches_single(self):
        """Column i of a multi query equals running that aggregator alone,
        for every series returned by the fanout."""
        self.setup_clustered_data()
        client = self.new_client_for_primary(0)
        aggregators = ['avg', 'max', 'min', 'sum', 'count']

        multi = client.execute_command(
            'TS.MRANGE', '-', '+',
            'AGGREGATION', ','.join(aggregators), 1000,
            'FILTER', 'kind=~".+"')
        multi_by_key = self.series_by_key(multi)

        for i, agg in enumerate(aggregators):
            single = client.execute_command(
                'TS.MRANGE', '-', '+',
                'AGGREGATION', agg, 1000,
                'FILTER', 'kind=~".+"')
            single_by_key = self.series_by_key(single)
            assert multi_by_key.keys() == single_by_key.keys()
            for key, rows in single_by_key.items():
                mrows = multi_by_key[key]
                assert len(mrows) == len(rows), f"{key} {agg}"
                for mrow, srow in zip(mrows, rows):
                    assert mrow[0] == srow[0]
                    assert mrow[1 + i] == srow[1], f"{key} col {i} ({agg})"

    def test_mrange_cme_multi_aggregation_count(self):
        """COUNT push-down with multi-aggregation: the shard truncates rows,
        the coordinator re-applies the authoritative COUNT."""
        self.setup_clustered_data()
        client = self.new_client_for_primary(0)

        result = client.execute_command(
            'TS.MRANGE', '-', '+',
            'AGGREGATION', 'avg,max', 1000,
            'COUNT', 1,
            'FILTER', 'region=us')

        by_key = self.series_by_key(result)
        # first bucket only, in ascending order
        assert by_key[b'm:{slot1}:a'] == [[1000, b'3', b'4']]
        assert by_key[b'm:{slot2}:c'] == [[1000, b'10', b'10']]

    def test_mrevrange_cme_multi_aggregation(self):
        """TS.MREVRANGE returns multi-column rows in descending bucket order;
        COUNT keeps the latest buckets."""
        self.setup_clustered_data()
        client = self.new_client_for_primary(0)

        result = client.execute_command(
            'TS.MREVRANGE', '-', '+',
            'AGGREGATION', 'avg,max', 1000,
            'COUNT', 1,
            'FILTER', 'region=us')

        by_key = self.series_by_key(result)
        # c's latest bucket is [2000,3000)
        assert by_key[b'm:{slot2}:c'] == [[2000, b'6', b'6']]
        # a has a single bucket
        assert by_key[b'm:{slot1}:a'] == [[1000, b'3', b'4']]

        # full reverse for c: descending timestamps
        result = client.execute_command(
            'TS.MREVRANGE', '-', '+',
            'AGGREGATION', 'avg,max', 1000,
            'FILTER', 'region=us')
        by_key = self.series_by_key(result)
        ts = [row[0] for row in by_key[b'm:{slot2}:c']]
        assert ts == sorted(ts, reverse=True)

    def test_mrange_cme_multi_aggregation_groupby(self):
        """Column-wise reduce across slots: for a group spanning two shards,
        each aggregation column is reduced independently per bucket timestamp."""
        self.setup_clustered_data()
        client = self.new_client_for_primary(0)

        result = client.execute_command(
            'TS.MRANGE', '-', '+',
            'AGGREGATION', 'avg,max', 1000,
            'WITHLABELS',
            'FILTER', 'region=us',
            'GROUPBY', 'region', 'REDUCE', 'sum')

        assert len(result) == 1
        key, labels, rows = result[0]
        assert key == b'region=us'
        labels_dict = {item[0]: item[1] for item in labels}
        # __source__ is the sorted union of member keys across shards
        assert labels_dict[b'__source__'] == b'm:{slot1}:a,m:{slot2}:c'
        # ts 1000: avg 3+10=13, max 4+10=14; ts 2000: only c -> 6, 6
        assert rows == [[1000, b'13', b'14'], [2000, b'6', b'6']]

    def test_mrevrange_cme_multi_aggregation_groupby_count(self):
        """Grouped multi-aggregation, reversed with COUNT: reduce column-wise,
        then reverse and truncate to the latest buckets."""
        self.setup_clustered_data()
        client = self.new_client_for_primary(0)

        result = client.execute_command(
            'TS.MREVRANGE', '-', '+',
            'AGGREGATION', 'avg,max', 1000,
            'FILTER', 'region=us',
            'GROUPBY', 'region', 'REDUCE', 'sum',
            'COUNT', 1)

        assert len(result) == 1
        _key, _labels, rows = result[0]
        # latest bucket of the group is ts 2000 (c only) -> 6, 6
        assert rows == [[2000, b'6', b'6']]

    def test_last_empty_carry(self):
        """A `last` column carries through gaps."""
        cluster_client = self.new_cluster_client()
        for timestamp, value in ((0, 1), (100, 2)):
            cluster_client.execute_command('TS.ADD', 'ts:{slot1}:carry', timestamp, value,
                                           'LABELS', 'k', 'carry')

        client = self.new_client_for_primary(0)
        expected = [[0, b'1', b'1'], [25, b'1', b'0'], [50, b'1', b'0'], [75, b'1', b'0'],
                    [100, b'2', b'2']]
        result = client.execute_command('TS.MRANGE', 0, 100,
                                        'AGGREGATION', 'last,sum', 25, 'EMPTY',
                                        'FILTER', 'k=carry')
        assert result[0][2] == expected

    @pytest.mark.parametrize('command, window, aggregation, expected', [
        pytest.param('TS.MRANGE', (50, 75), ('last', 25),
                     [[50, b'1'], [75, b'1']], id='edge-fill-and-seed'),
        pytest.param('TS.MRANGE', (150, 250), ('sum', 25),
                     [[150, b'0'], [175, b'0'], [200, b'0'], [225, b'0'], [250, b'0']],
                     id='window-inside-a-gap'),
        pytest.param('TS.MREVRANGE', (50, 75), ('last', 25),
                     [[75, b'1'], [50, b'1']], id='reverse-edge-fill'),
        pytest.param('TS.MRANGE', (150, 250), ('last,sum', 25),
                     [[150, b'2', b'0'], [175, b'2', b'0'], [200, b'2', b'0'], [225, b'2', b'0'],
                      [250, b'2', b'0']],
                     id='multi-aggregation'),
    ])
    def test_empty_reaches_past_the_window(self, command, window, aggregation, expected):
        """EMPTY needs what lies outside the window, which only shards can see, and is
        answered as on a standalone server."""
        cluster_client = self.new_cluster_client()
        for timestamp, value in ((0, 1), (100, 2), (300, 3)):
            cluster_client.execute_command('TS.ADD', 'ts:{slot1}:edge', timestamp, value,
                                           'LABELS', 'k', 'edge')

        result = self.new_client_for_primary(0).execute_command(
            command, *window, 'AGGREGATION', *aggregation, 'EMPTY', 'FILTER', 'k=edge')

        assert result[0][2] == expected

    def test_mrange_matches_per_series_range(self):
        """Each series in an aggregated MRANGE is what TS.RANGE / TS.REVRANGE answers for
        that key alone, which a single node computes as a standalone server does."""
        self.setup_clustered_data()
        cluster_client: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)

        queries = [
            ('TS.MRANGE', '-', '+', 'AGGREGATION', 'avg,max,count', 1000,
             'FILTER', 'kind=~".+"'),
            ('TS.MREVRANGE', '-', '+', 'AGGREGATION', 'sum,min', 1000,
             'COUNT', 2, 'FILTER', 'region=us'),
            ('TS.MRANGE', '-', '+', 'AGGREGATION', 'avg', 1000, 'FILTER', 'kind=~".+"'),
            ('TS.MREVRANGE', 1000, 2500, 'AGGREGATION', 'max', 500, 'EMPTY',
             'COUNT', 3, 'FILTER', 'kind=~".+"'),
        ]
        for query in queries:
            single = 'TS.RANGE' if query[0] == 'TS.MRANGE' else 'TS.REVRANGE'
            args = query[1:query.index('FILTER')]
            result = client.execute_command(*query)
            assert result, query
            for key, _labels, rows in result:
                expected = cluster_client.execute_command(single, key, *args)
                assert rows == expected, (query, key)

    def test_mrange_cme_multi_aggregation_empty(self):
        """A multi-aggregation query matching no series returns an empty list."""
        self.setup_clustered_data()
        client = self.new_client_for_primary(0)

        result = client.execute_command(
            'TS.MRANGE', '-', '+',
            'AGGREGATION', 'avg,max', 1000,
            'FILTER', 'region=nonexistent')

        assert result == []

    def test_mrange_cme_multi_aggregation_empty_range_series(self):
        """A series with no samples in the queried range contributes an empty
        row set (0 columns on the wire) and is surfaced as an empty series."""
        self.setup_clustered_data()
        client = self.new_client_for_primary(0)

        # Range entirely after all samples: every matched series is empty.
        result = client.execute_command(
            'TS.MRANGE', 100000, 200000,
            'AGGREGATION', 'avg,max', 1000,
            'FILTER', 'region=us')

        assert len(result) == 2
        for series in result:
            assert series[2] == []
