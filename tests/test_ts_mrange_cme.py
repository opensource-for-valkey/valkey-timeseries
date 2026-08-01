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
