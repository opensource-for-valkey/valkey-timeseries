from valkey import ResponseError, ValkeyCluster
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesClusterTestCase
import pytest


class TestTimeSeriesMRangeClustered(ValkeyTimeSeriesClusterTestCase):
    """Integration tests for TS.MRANGE and TS.MREVRANGE in clustered mode."""

    def setup_clustered_data(self):
        """Create test time series with hash tags for cluster slot alignment."""
        # Use hash tags to control slot distribution
        cluster_client: ValkeyCluster = self.new_cluster_client()

        cluster_client.execute_command('TS.CREATE', 'ts:{slot1}:temp1', 'LABELS', 'sensor', 'temp', 'region', 'east')
        cluster_client.execute_command('TS.CREATE', 'ts:{slot1}:temp2', 'LABELS', 'sensor', 'temp', 'region', 'west')
        cluster_client.execute_command('TS.CREATE', 'ts:{slot2}:humid1', 'LABELS', 'sensor', 'humid', 'region', 'east')
        cluster_client.execute_command('TS.CREATE', 'ts:{slot2}:humid2', 'LABELS', 'sensor', 'humid', 'region', 'west')

        self.start_ts = 1000

        for i in range(0, 100, 10):
            cluster_client.execute_command('TS.ADD', 'ts:{slot1}:temp1', self.start_ts + i, 20 + i / 10)
            cluster_client.execute_command('TS.ADD', 'ts:{slot1}:temp2', self.start_ts + i, 25 + i / 10)
            cluster_client.execute_command('TS.ADD', 'ts:{slot2}:humid1', self.start_ts + i, 50 + (i % 20))
            cluster_client.execute_command('TS.ADD', 'ts:{slot2}:humid2', self.start_ts + i, 60 + (i % 15))

    def test_mrange_cme_rejects_unbounded_filter(self):
        """A 'match everything' filter list must still carry a positive matcher.

        The tests below use 'sensor=~".+"' / 'region=~".+"' to select every series. The
        older idiom 'sensor!=none' selects the same set but is a negative matcher, so a
        filter list containing only that has nothing to intersect against but the whole
        keyspace and is rejected.
        """
        self.setup_clustered_data()

        client = self.new_client_for_primary(0)
        self.assert_filters_rejected('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                     'FILTER', 'sensor!=none', client=client)

        # The bounded form selects every series, since all of them have a 'sensor' label.
        result = client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                        'FILTER', 'sensor=~".+"')
        assert len(result) == 4

    def test_mrange_cme_basic(self):
        """Test TS.MRANGE across series in different hash slots."""
        self.setup_clustered_data()

        client = self.new_client_for_primary(0)
        result = client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                        'FILTER', 'sensor=temp')

        assert len(result) == 2
        for series in result:
            assert series[0] in [b'ts:{slot1}:temp1', b'ts:{slot1}:temp2']
            assert len(series[2]) == 10

    def test_mrange_cme_with_aggregation(self):
        """Test TS.MRANGE with aggregation across different slots."""
        self.setup_clustered_data()

        client = self.new_client_for_primary(0)

        result = client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                        'AGGREGATION', 'avg', 20,
                                        'FILTER', 'region=east')

        assert len(result) == 2
        for series in result:
            assert series[0] in [b'ts:{slot1}:temp1', b'ts:{slot2}:humid1']
            assert len(series[2]) in [5, 6]

    def test_mrange_cme_groupby(self):
        """Test TS.MRANGE GROUPBY across series in different slots."""
        self.setup_clustered_data()

        client = self.new_client_for_primary(0)

        result = client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                        'WITHLABELS',
                                        'FILTER', 'region=~".+"',
                                        'GROUPBY', 'region',
                                        'REDUCE', 'sum')

        print("GroupBy result", result)

        assert len(result) == 2

        regions_found = set()
        for series in result:
            labels_dict = {item[0]: item[1] for item in series[1]}
            regions_found.add(labels_dict.get(b'region'))
            assert len(series[2]) == 10

        assert regions_found == {b'east', b'west'}

    def test_mrange_cme_groupby_with_aggregation(self):
        """Test TS.MRANGE GROUPBY with AGGREGATION across slots."""
        self.setup_clustered_data()

        client = self.new_client_for_primary(0)
        result = client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                        'AGGREGATION', 'avg', 20,
                                        'WITHLABELS',
                                        'FILTER', 'sensor=~".+"',
                                        'GROUPBY', 'sensor',
                                        'REDUCE', 'max')

        assert len(result) == 2

        sensors_found = set()
        for series in result:
            labels_dict = {item[0]: item[1] for item in series[1]}
            sensors_found.add(labels_dict.get(b'sensor'))
            assert labels_dict[b'__reducer__'] == b'max'

        assert sensors_found == {b'temp', b'humid'}

    def test_mrange_cme_count(self):
        """Test TS.MRANGE COUNT across different slots."""
        self.setup_clustered_data()

        client = self.new_client_for_primary(0)
        result = client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                        'COUNT', 3,
                                        'FILTER', 'region=west')

        assert len(result) == 2
        for series in result:
            assert len(series[2]) == 3
            timestamps = [sample[0] for sample in series[2]]
            assert timestamps[0] == self.start_ts

    def test_mrange_cme_groupby_count(self):
        """Test TS.MRANGE GROUPBY with COUNT across slots."""
        self.setup_clustered_data()

        client = self.new_client_for_primary(0)
        result = client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                        'COUNT', 4,
                                        'FILTER', 'sensor=~".+"',
                                        'GROUPBY', 'region',
                                        'REDUCE', 'avg')

        assert len(result) == 2
        for series in result:
            assert len(series[2]) == 4

    def test_mrange_cme_withlabels(self):
        """Test TS.MRANGE WITHLABELS across different slots."""
        self.setup_clustered_data()

        client = self.new_client_for_primary(0)
        result = client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                        'WITHLABELS',
                                        'FILTER', 'region=east')

        assert len(result) == 2
        for series in result:
            labels_dict = {item[0]: item[1] for item in series[1]}
            assert labels_dict[b'region'] == b'east'
            assert b'sensor' in labels_dict

    def test_mrange_cme_selected_labels(self):
        """Test TS.MRANGE SELECTED_LABELS across different slots."""
        self.setup_clustered_data()

        client = self.new_client_for_primary(0)
        result = client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                        'SELECTED_LABELS', 'region',
                                        'FILTER', 'sensor=~".+"')

        assert len(result) == 4
        for series in result:
            labels_dict = {item[0]: item[1] for item in series[1]}
            assert len(labels_dict) == 1
            assert b'region' in labels_dict

    def test_mrange_cme_filter_by_value(self):
        """Test TS.MRANGE FILTER_BY_VALUE across different slots."""
        self.setup_clustered_data()

        client = self.new_client_for_primary(0)
        result = client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                        'FILTER_BY_VALUE', 55, 70,
                                        'FILTER', 'sensor=humid')

        assert len(result) == 2
        for series in result:
            for ts, val in series[2]:
                val = float(val.decode())
                assert 55 <= val <= 70

    @pytest.mark.parametrize('command, options', [
        ('TS.MRANGE', []),
        ('TS.MREVRANGE', []),
        ('TS.MRANGE', ['AGGREGATION', 'sum', 10]),
        ('TS.MRANGE', ['AGGREGATION', 'sum', 10, 'EMPTY']),
        ('TS.MRANGE', ['AGGREGATION', 'sum,max', 10]),
    ])
    def test_mrange_cme_filter_by_ts_outside_window_matches_nothing(self, command, options):
        """Every FILTER_BY_TS entry outside the window leaves nothing to match.

        That must stay distinct from "no filter", which returns the whole window. Each series
        is still listed, with no samples, as it is on a standalone server.
        """
        self.setup_clustered_data()

        client = self.new_client_for_primary(0)
        result = client.execute_command(command, self.start_ts, self.start_ts + 90,
                                        'FILTER_BY_TS', self.start_ts - 500,
                                        *options,
                                        'FILTER', 'sensor=~".+"')

        assert sorted((series[0], series[2]) for series in result) == [
            (b'ts:{slot1}:temp1', []),
            (b'ts:{slot1}:temp2', []),
            (b'ts:{slot2}:humid1', []),
            (b'ts:{slot2}:humid2', []),
        ]

    def test_mrange_cme_filter_by_ts_outside_window_groupby(self):
        """A FILTER_BY_TS entirely outside the window leaves every group with no samples."""
        self.setup_clustered_data()

        client = self.new_client_for_primary(0)
        result = client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 90,
                                        'FILTER_BY_TS', self.start_ts - 500,
                                        'FILTER', 'sensor=~".+"',
                                        'GROUPBY', 'sensor', 'REDUCE', 'sum')

        assert sorted((group[0], group[2]) for group in result) == [
            (b'sensor=humid', []),
            (b'sensor=temp', []),
        ]

    def test_mrange_cme_filter_by_ts_outside_window_excludeempty(self):
        """EXCLUDEEMPTY drops every series a FILTER_BY_TS outside the window has emptied."""
        self.setup_clustered_data()

        client = self.new_client_for_primary(0)
        result = client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 90,
                                        'FILTER_BY_TS', self.start_ts - 500,
                                        'EXCLUDEEMPTY',
                                        'FILTER', 'sensor=~".+"')

        assert result == []

    def test_mrange_cme_filter_by_ts_partly_outside_window(self):
        """Only the entries inside the window return samples."""
        self.setup_clustered_data()

        client = self.new_client_for_primary(0)
        result = client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 90,
                                        'FILTER_BY_TS', self.start_ts - 500, self.start_ts + 50,
                                        'FILTER', 'sensor=temp')

        assert len(result) == 2
        for series in result:
            assert [sample[0] for sample in series[2]] == [self.start_ts + 50]

    def test_mrange_cme_empty_fill_sees_filter_by_ts_entries_outside_the_window(self):
        """Shards get the whole list, so entries past each edge let EMPTY fill out to it."""
        client = self.new_client_for_primary(0)
        cluster_client: ValkeyCluster = self.new_cluster_client()
        for key in ('ts:{slot1}:fill', 'ts:{slot2}:fill'):
            for timestamp, value in ((10, 1), (55, 2), (90, 3)):
                cluster_client.execute_command('TS.ADD', key, timestamp, value, 'LABELS', 'k', 'fill')

        result = client.execute_command('TS.MRANGE', 50, 70,
                                        'FILTER_BY_TS', 10, 55, 90,
                                        'AGGREGATION', 'sum', 5, 'EMPTY',
                                        'FILTER', 'k=fill')

        assert len(result) == 2
        for series in result:
            assert series[2] == [[50, b'0'], [55, b'2'], [60, b'0'], [65, b'0'], [70, b'0']]

    def test_mrevrange_cme_last_empty_fill_around_stored_nan(self):
        """Shards aggregate ascending, but `last` still fills as the newest-first reply requires.

        Expected rows are replies from the pinned reference. Forward, bucket 10 would be NaN.
        """
        client = self.new_client_for_primary(0)
        cluster_client: ValkeyCluster = self.new_cluster_client()
        for key in ('ts:{slot1}:nan', 'ts:{slot2}:nan'):
            for timestamp, value in ((0, 5), (10, 'nan'), (100, 7)):
                cluster_client.execute_command('TS.ADD', key, timestamp, value, 'LABELS', 'k', 'nan')

        result = client.execute_command('TS.MREVRANGE', 10, 200,
                                        'AGGREGATION', 'last', 10, 'EMPTY',
                                        'FILTER', 'k=nan')

        assert len(result) == 2
        for series in result:
            assert series[2] == [[100, b'7']] + [[t, b'5'] for t in range(90, 0, -10)]

    @pytest.mark.parametrize('options, expected', [
        pytest.param(['COUNT', 2], [[6000, b'2'], [5000, b'9']], id='raw'),
        pytest.param(['COUNT', 1, 'FILTER_BY_VALUE', 4, 6], [[250, b'5']], id='raw-value-filter'),
        pytest.param(['COUNT', 1, 'FILTER_BY_TS', 0, 250], [[250, b'5']], id='raw-timestamp-filter'),
        pytest.param(['COUNT', 2, 'AGGREGATION', 'sum', 100],
                     [[6000, b'2'], [5000, b'9']], id='aggregated'),
        pytest.param(['COUNT', 1, 'FILTER_BY_VALUE', 4, 6, 'AGGREGATION', 'sum', 100],
                     [[200, b'5']], id='aggregated-value-filter-widens'),
        pytest.param(['COUNT', 2, 'FILTER_BY_TS', 0, 250, 5000, 'AGGREGATION', 'sum', 100],
                     [[5000, b'9'], [200, b'5']], id='aggregated-timestamp-filter'),
        pytest.param(['COUNT', 3, 'AGGREGATION', 'sum', 100, 'EMPTY'],
                     [[6000, b'2'], [5900, b'0'], [5800, b'0']], id='empty'),
        pytest.param(['COUNT', 3, 'AGGREGATION', 'last', 100, 'EMPTY'],
                     [[6000, b'2'], [5900, b'9'], [5800, b'9']], id='empty-last'),
        pytest.param(['COUNT', 2, 'AGGREGATION', 'avg,max', 100],
                     [[6000, b'2', b'2'], [5000, b'9', b'9']], id='multi-aggregation'),
        pytest.param(['COUNT', 50, 'AGGREGATION', 'sum', 1000],
                     [[6000, b'2'], [5000, b'9'], [0, b'6']], id='count-above-the-buckets'),
    ])
    def test_mrevrange_cme_count_returns_each_series_tail(self, options, expected):
        """Shards keep each series' newest COUNT items, however they choose to read them."""
        client = self.new_client_for_primary(0)
        cluster_client: ValkeyCluster = self.new_cluster_client()
        keys = ('ts:{slot1}:tail', 'ts:{slot2}:tail')
        for key in keys:
            for timestamp, value in ((0, 1), (250, 5), (260, 'nan'), (5000, 9), (6000, 2)):
                cluster_client.execute_command('TS.ADD', key, timestamp, value, 'LABELS', 'k', 'tail')

        result = client.execute_command('TS.MREVRANGE', '-', '+', *options, 'FILTER', 'k=tail')

        assert sorted(series[0].decode() for series in result) == sorted(keys)
        for series in result:
            assert series[2] == expected

    def test_mrevrange_cme_count_with_latest_includes_the_open_bucket(self):
        """LATEST's open bucket is the newest item, so it is part of a shard's tail."""
        client = self.new_client_for_primary(0)
        cluster_client: ValkeyCluster = self.new_cluster_client()
        cluster_client.execute_command('TS.CREATE', 'ts:{slot1}:src')
        cluster_client.execute_command('TS.CREATE', 'ts:{slot1}:dst', 'LABELS', 'k', 'open')
        cluster_client.execute_command('TS.CREATERULE', 'ts:{slot1}:src', 'ts:{slot1}:dst',
                                       'AGGREGATION', 'sum', 100)
        for timestamp, value in ((10, 1), (110, 2), (210, 3), (310, 4)):
            cluster_client.execute_command('TS.ADD', 'ts:{slot1}:src', timestamp, value)

        result = client.execute_command('TS.MREVRANGE', '-', '+', 'LATEST', 'COUNT', 2,
                                        'FILTER', 'k=open')
        assert result[0][2] == [[300, b'4'], [200, b'3']]

        result = client.execute_command('TS.MREVRANGE', '-', '+', 'LATEST', 'COUNT', 2,
                                        'AGGREGATION', 'sum', 200, 'FILTER', 'k=open')
        assert result[0][2] == [[200, b'7'], [0, b'3']]

    def test_mrevrange_cme_count_with_groupby_reduces_each_bucket_fully(self):
        """A bucket in the reply has to include every series holding it, not just the newest."""
        client = self.new_client_for_primary(0)
        cluster_client: ValkeyCluster = self.new_cluster_client()
        for timestamp, value in ((0, 1), (100, 2), (200, 3)):
            cluster_client.execute_command('TS.ADD', 'ts:{slot1}:grp', timestamp, value,
                                           'LABELS', 'k', 'grp')
        for timestamp, value in ((100, 10), (200, 20), (900, 30)):
            cluster_client.execute_command('TS.ADD', 'ts:{slot2}:grp', timestamp, value,
                                           'LABELS', 'k', 'grp')

        for reducer, expected in (('sum', [[900, b'30'], [200, b'23']]),
                                  ('range', [[900, b'0'], [200, b'17']])):
            result = client.execute_command('TS.MREVRANGE', '-', '+', 'COUNT', 2, 'FILTER', 'k=grp',
                                            'GROUPBY', 'k', 'REDUCE', reducer)
            assert result[0][2] == expected, reducer

    def test_mrange_cme_empty_result(self):
        """Test TS.MRANGE with no matching series across slots."""
        self.setup_clustered_data()

        client = self.new_client_for_primary(0)
        result = client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                        'FILTER', 'sensor=nonexistent')

        assert len(result) == 0

    def test_mrange_cme_groupby_reduce_min(self):
        """Test TS.MRANGE GROUPBY with REDUCE min across slots."""
        self.setup_clustered_data()

        client = self.new_client_for_primary(0)
        result = client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                        'WITHLABELS',
                                        'FILTER', 'sensor=temp',
                                        'GROUPBY', 'sensor',
                                        'REDUCE', 'min')

        assert len(result) == 1
        labels_dict = {item[0]: item[1] for item in result[0][1]}
        assert labels_dict[b'__reducer__'] == b'min'

        for ts, val in result[0][2]:
            val = float(val.decode())
            assert val >= 20

    def test_mrange_cme_groupby_reduce_max(self):
        """Test TS.MRANGE GROUPBY with REDUCE max across slots."""
        self.setup_clustered_data()

        client = self.new_client_for_primary(0)
        result = client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                        'WITHLABELS',
                                        'FILTER', 'sensor=humid',
                                        'GROUPBY', 'region',
                                        'REDUCE', 'max')

        assert len(result) == 2
        for series in result:
            labels_dict = {item[0]: item[1] for item in series[1]}
            assert labels_dict[b'__reducer__'] == b'max'

    def test_mrange_cme_latest(self):
        """Test TS.MRANGE with LATEST flag across slots (compaction)."""
        cluster_client = self.new_cluster_client()

        # Setup source and dest series in different slots
        # Slot 1
        cluster_client.execute_command('TS.CREATE', 'ts:{slot1}:src', 'LABELS', 'type', 'source', 'pair', '1')
        cluster_client.execute_command('TS.CREATE', 'ts:{slot1}:dst', 'LABELS', 'type', 'dest', 'pair', '1')
        cluster_client.execute_command('TS.CREATERULE', 'ts:{slot1}:src', 'ts:{slot1}:dst', 'AGGREGATION', 'sum', 10)

        # Slot 2
        cluster_client.execute_command('TS.CREATE', 'ts:{slot2}:src', 'LABELS', 'type', 'source', 'pair', '2')
        cluster_client.execute_command('TS.CREATE', 'ts:{slot2}:dst', 'LABELS', 'type', 'dest', 'pair', '2')
        cluster_client.execute_command('TS.CREATERULE', 'ts:{slot2}:src', 'ts:{slot2}:dst', 'AGGREGATION', 'sum', 10)

        # Add samples
        # Bucket 1000-1010 (Closed by 1010)
        cluster_client.execute_command('TS.ADD', 'ts:{slot1}:src', 1000, 10)
        cluster_client.execute_command('TS.ADD', 'ts:{slot1}:src', 1005, 10)
        cluster_client.execute_command('TS.ADD', 'ts:{slot2}:src', 1000, 5)
        cluster_client.execute_command('TS.ADD', 'ts:{slot2}:src', 1005, 5)

        # Bucket 1010-1020 (Open)
        cluster_client.execute_command('TS.ADD', 'ts:{slot1}:src', 1010, 20)
        cluster_client.execute_command('TS.ADD', 'ts:{slot1}:src', 1015, 20)
        cluster_client.execute_command('TS.ADD', 'ts:{slot2}:src', 1010, 15)
        cluster_client.execute_command('TS.ADD', 'ts:{slot2}:src', 1015, 15)

        client = self.new_client_for_primary(0)

        # 1. Without LATEST
        result = client.execute_command('TS.MRANGE', '-', '+', 'FILTER', 'type=dest')
        assert len(result) == 2
        for series in result:
            # Should only have the closed bucket (1000)
            assert len(series[2]) == 1
            assert series[2][0][0] == 1000

        # 2. With LATEST
        result = client.execute_command('TS.MRANGE', '-', '+', 'LATEST', 'FILTER', 'type=dest')
        assert len(result) == 2
        for series in result:
            # Should have closed bucket (1000) and open bucket (1010)
            assert len(series[2]) == 2
            timestamps = [s[0] for s in series[2]]
            assert timestamps == [1000, 1010]

            # Check values
            key = series[0].decode()
            if 'ts:{slot1}:dst' in key:
                assert series[2][1][1] == b'40'  # 20 + 20
            elif 'ts:{slot2}:dst' in key:
                assert series[2][1][1] == b'30'  # 15 + 15

    # TS.MREVRANGE ---
    def test_mrevrange_cme(self):
        """Test TS.MREVRANGE across different slots."""
        self.setup_clustered_data()

        client = self.new_client_for_primary(0)
        result = client.execute_command('TS.MREVRANGE', self.start_ts, self.start_ts + 100,
                                        'FILTER', 'sensor=humid')

        assert len(result) == 2
        for series in result:
            timestamps = [sample[0] for sample in series[2]]
            assert timestamps == sorted(timestamps, reverse=True)

    def test_mrevrange_cme_groupby(self):
        """Test TS.MREVRANGE GROUPBY across slots."""
        self.setup_clustered_data()

        client = self.new_client_for_primary(0)
        result = client.execute_command('TS.MREVRANGE', self.start_ts, self.start_ts + 100,
                                        'FILTER', 'region=~".+"',
                                        'GROUPBY', 'sensor',
                                        'REDUCE', 'sum')

        assert len(result) == 2
        for series in result:
            timestamps = [sample[0] for sample in series[2]]
            assert timestamps == sorted(timestamps, reverse=True)

    def test_mrevrange_cme_count_aggregation(self):
        """Test TS.MREVRANGE with COUNT and AGGREGATION across slots."""
        self.setup_clustered_data()

        client = self.new_client_for_primary(0)
        result = client.execute_command('TS.MREVRANGE', self.start_ts, self.start_ts + 100,
                                        'AGGREGATION', 'sum', 25,
                                        'COUNT', 2,
                                        'FILTER', 'region=east')

        assert len(result) == 2
        for series in result:
            assert len(series[2]) == 2
            timestamps = [sample[0] for sample in series[2]]
            assert timestamps == sorted(timestamps, reverse=True)

    def test_mrevrange_cme_withlabels(self):
        """TS.MREVRANGE WITHLABELS across slots returns labels and reverse-ordered samples."""
        self.setup_clustered_data()

        client = self.new_client_for_primary(0)
        result = client.execute_command(
            'TS.MREVRANGE', self.start_ts, self.start_ts + 100,
            'WITHLABELS',
            'FILTER', 'sensor=temp'
        )

        assert len(result) == 2
        for series in result:
            labels_dict = {item[0]: item[1] for item in series[1]}
            assert labels_dict[b'sensor'] == b'temp'
            timestamps = [sample[0] for sample in series[2]]
            assert timestamps == sorted(timestamps, reverse=True)

    def test_mrevrange_cme_selected_labels(self):
        """TS.MREVRANGE SELECTED_LABELS across slots returns only requested labels."""
        self.setup_clustered_data()

        client = self.new_client_for_primary(0)
        result = client.execute_command(
            'TS.MREVRANGE', self.start_ts, self.start_ts + 100,
            'SELECTED_LABELS', 'region',
            'FILTER', 'sensor=~".+"'
        )

        assert len(result) == 4
        for series in result:
            labels_dict = {item[0].decode(): item[1].decode() for item in series[1]}
            assert set(labels_dict.keys()) == {'region'}
            timestamps = [sample[0] for sample in series[2]]
            assert timestamps == sorted(timestamps, reverse=True)

    def test_mrevrange_cme_latest(self):
        """Test TS.MREVRANGE with LATEST flag across slots (compaction)."""
        cluster_client = self.new_cluster_client()

        # Setup source and dest series in different slots
        cluster_client.execute_command('TS.CREATE', 'ts:{slot1}:src_rev', 'LABELS', 'type', 'source_rev', 'pair', '1')
        cluster_client.execute_command('TS.CREATE', 'ts:{slot1}:dst_rev', 'LABELS', 'type', 'dest_rev', 'pair', '1')
        cluster_client.execute_command('TS.CREATERULE', 'ts:{slot1}:src_rev', 'ts:{slot1}:dst_rev', 'AGGREGATION',
                                       'sum', 10)

        cluster_client.execute_command('TS.CREATE', 'ts:{slot2}:src_rev', 'LABELS', 'type', 'source_rev', 'pair', '2')
        cluster_client.execute_command('TS.CREATE', 'ts:{slot2}:dst_rev', 'LABELS', 'type', 'dest_rev', 'pair', '2')
        cluster_client.execute_command('TS.CREATERULE', 'ts:{slot2}:src_rev', 'ts:{slot2}:dst_rev', 'AGGREGATION',
                                       'sum', 10)

        # Add samples
        # Bucket 1000-1010 (Closed)
        cluster_client.execute_command('TS.ADD', 'ts:{slot1}:src_rev', 1000, 10)
        cluster_client.execute_command('TS.ADD', 'ts:{slot1}:src_rev', 1005, 10)
        cluster_client.execute_command('TS.ADD', 'ts:{slot2}:src_rev', 1000, 5)
        cluster_client.execute_command('TS.ADD', 'ts:{slot2}:src_rev', 1005, 5)

        # Bucket 1010-1020 (Open)
        cluster_client.execute_command('TS.ADD', 'ts:{slot1}:src_rev', 1010, 20)
        cluster_client.execute_command('TS.ADD', 'ts:{slot1}:src_rev', 1015, 20)
        cluster_client.execute_command('TS.ADD', 'ts:{slot2}:src_rev', 1010, 15)
        cluster_client.execute_command('TS.ADD', 'ts:{slot2}:src_rev', 1015, 15)

        client = self.new_client_for_primary(0)

        # 1. Without LATEST
        result = client.execute_command('TS.MREVRANGE', '-', '+', 'FILTER', 'type=dest_rev')
        assert len(result) == 2
        for series in result:
            assert len(series[2]) == 1
            assert series[2][0][0] == 1000

        # 2. With LATEST
        result = client.execute_command('TS.MREVRANGE', '-', '+', 'LATEST', 'FILTER', 'type=dest_rev')
        assert len(result) == 2
        for series in result:
            assert len(series[2]) == 2
            timestamps = [s[0] for s in series[2]]
            # Reverse order
            assert timestamps == [1010, 1000]

            key = series[0].decode()
            if 'ts:{slot1}:dst_rev' in key:
                assert series[2][0][1] == b'40'
            elif 'ts:{slot2}:dst_rev' in key:
                assert series[2][0][1] == b'30'

    def test_mrange_cme_exclude_empty(self):
        """EXCLUDEEMPTY drops empty series regardless of which shard owns them.

        The series with no in-range samples are split across two slots, so a
        correct result requires both the shard-side pre-filter and the
        coordinator's authoritative pass to agree.
        """
        self.setup_clustered_data()

        cluster_client: ValkeyCluster = self.new_cluster_client()
        # Two more series matching 'sensor=temp'/'sensor=humid', one per slot,
        # with samples well outside the queried range.
        cluster_client.execute_command('TS.CREATE', 'ts:{slot1}:temp3', 'LABELS', 'sensor', 'temp', 'region', 'north')
        cluster_client.execute_command('TS.CREATE', 'ts:{slot2}:humid3', 'LABELS', 'sensor', 'humid', 'region', 'north')
        cluster_client.execute_command('TS.ADD', 'ts:{slot1}:temp3', 900000, 1)
        cluster_client.execute_command('TS.ADD', 'ts:{slot2}:humid3', 900000, 1)

        client = self.new_client_for_primary(0)

        result = client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                        'FILTER', 'sensor=~".+"')
        assert len(result) == 6
        empty = [series[0] for series in result if len(series[2]) == 0]
        assert sorted(empty) == [b'ts:{slot1}:temp3', b'ts:{slot2}:humid3']

        result = client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                        'EXCLUDEEMPTY', 'FILTER', 'sensor=~".+"')
        assert [series[0] for series in result] == [
            b'ts:{slot1}:temp1', b'ts:{slot1}:temp2',
            b'ts:{slot2}:humid1', b'ts:{slot2}:humid2',
        ]

        result = client.execute_command('TS.MREVRANGE', self.start_ts, self.start_ts + 100,
                                        'EXCLUDEEMPTY', 'FILTER', 'sensor=~".+"')
        assert len(result) == 4
        for series in result:
            timestamps = [sample[0] for sample in series[2]]
            assert timestamps == sorted(timestamps, reverse=True)

    def test_mrange_cme_exclude_empty_with_groupby_is_an_error(self):
        """The GROUPBY conflict is rejected at parse time, before any fanout."""
        self.setup_clustered_data()

        client = self.new_client_for_primary(0)
        for command in ('TS.MRANGE', 'TS.MREVRANGE'):
            with pytest.raises(ResponseError, match="TSDB: EXCLUDEEMPTY is not allowed with GROUPBY"):
                client.execute_command(command, self.start_ts, self.start_ts + 100,
                                       'EXCLUDEEMPTY', 'FILTER', 'sensor=~".+"',
                                       'GROUPBY', 'region', 'REDUCE', 'max')

    def tag_per_primary(self, cluster_client: ValkeyCluster):
        """Return one hash tag per primary, each owned by a different node.

        The tag -> slot -> node mapping depends on how the harness splits the slot
        range across primaries, so the tags are discovered at runtime rather than
        hard-coded ({slot1} and {slot2} happen to share a primary here).
        """
        by_node = {}
        for i in range(1000):
            tag = f'tag{i}'
            node = cluster_client.get_node_from_key('{%s}' % tag)
            by_node.setdefault((node.host, node.port), tag)
            if len(by_node) == self.CLUSTER_SIZE:
                break

        assert len(by_node) == self.CLUSTER_SIZE, \
            f'found tags for only {len(by_node)} of {self.CLUSTER_SIZE} primaries'
        return [by_node[node] for node in sorted(by_node)]

    def setup_tagged_data(self):
        """One series per primary, all matching FILTER sensor=tagged."""
        cluster_client: ValkeyCluster = self.new_cluster_client()
        tags = self.tag_per_primary(cluster_client)

        self.start_ts = 1000
        keys = []
        for i, tag in enumerate(tags):
            key = f'ts:{{{tag}}}:sensor'
            cluster_client.execute_command('TS.CREATE', key, 'LABELS', 'sensor', 'tagged', 'shard', str(i))
            for offset in range(0, 100, 10):
                cluster_client.execute_command('TS.ADD', key, self.start_ts + offset, i * 100 + offset)
            keys.append(key.encode())
        return tags, keys

    def test_mrange_cme_hashtag_scopes_fanout(self):
        """TS.MRANGE HASHTAG restricts fanout to the shards owning the tags' slots"""
        tags, keys = self.setup_tagged_data()
        client = self.new_client_for_primary(0)

        def mrange_keys(*args):
            result = client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                            *args, 'FILTER', 'sensor=tagged')
            # Every returned series still carries its samples.
            for series in result:
                assert len(series[2]) == 10
            return sorted(series[0] for series in result)

        # Without HASHTAG every shard answers.
        assert mrange_keys() == sorted(keys)

        # A single tag reaches only the shard owning its slot.
        for tag, key in zip(tags, keys):
            assert mrange_keys('HASHTAG', tag) == [key], f'HASHTAG {tag}'

        # Tags are comma separated and the reply is their union.
        assert mrange_keys('HASHTAG', ','.join(tags[:2])) == sorted(keys[:2])
        assert mrange_keys('HASHTAG', ','.join(tags)) == sorted(keys)

        # The braced form hashes to the same slot as the bare tag.
        assert mrange_keys('HASHTAG', '{%s}' % tags[0]) == [keys[0]]

    def test_mrevrange_cme_hashtag_scopes_fanout(self):
        """HASHTAG scopes TS.MREVRANGE the same way and composes with other options"""
        tags, keys = self.setup_tagged_data()
        client = self.new_client_for_primary(0)

        result = client.execute_command('TS.MREVRANGE', self.start_ts, self.start_ts + 100,
                                        'HASHTAG', tags[2], 'COUNT', 3, 'WITHLABELS',
                                        'FILTER', 'sensor=tagged')
        assert len(result) == 1
        assert result[0][0] == keys[2]
        assert {label_pair[0]: label_pair[1] for label_pair in result[0][1]} == {b'sensor': b'tagged', b'shard': b'2'}

        timestamps = [sample[0] for sample in result[0][2]]
        assert timestamps == [self.start_ts + 90, self.start_ts + 80, self.start_ts + 70]

    def test_mrange_cme_hashtag_with_aggregation_and_groupby(self):
        """A HASHTAG-scoped fanout still aggregates and groups over the shards it reached"""
        tags, keys = self.setup_tagged_data()
        client = self.new_client_for_primary(0)

        result = client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                        'HASHTAG', ','.join(tags[:2]),
                                        'AGGREGATION', 'max', 100,
                                        'FILTER', 'sensor=tagged')
        assert sorted(series[0] for series in result) == sorted(keys[:2])

        # GROUPBY collapses only the series the HASHTAG-scoped fanout returned.
        result = client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                        'HASHTAG', tags[0],
                                        'FILTER', 'sensor=tagged',
                                        'GROUPBY', 'sensor', 'REDUCE', 'max')
        assert len(result) == 1
        assert result[0][0] == b'sensor=tagged'
