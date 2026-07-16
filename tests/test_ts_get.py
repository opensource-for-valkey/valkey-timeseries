import math

import pytest
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTsGet(ValkeyTimeSeriesTestCaseBase):

    def test_get_basic(self):
        """Test basic TS.GET functionality with a single sample"""
        # Create a time series and add a sample
        self.client.execute_command('TS.CREATE', 'ts1')
        self.client.execute_command('TS.ADD', 'ts1', 1000, 10.5)

        # Get the latest sample
        result = self.client.execute_command('TS.GET', 'ts1')
        assert result == [1000, b'10.5']

    def test_get_multiple_samples(self):
        """Test TS.GET returns the latest sample when multiple samples exist"""
        # Create a time series and add multiple samples
        self.client.execute_command('TS.CREATE', 'ts2')
        self.client.execute_command('TS.ADD', 'ts2', 1000, 10.5)
        self.client.execute_command('TS.ADD', 'ts2', 2000, 20.5)
        self.client.execute_command('TS.ADD', 'ts2', 3000, 30.5)

        # Get the latest sample
        result = self.client.execute_command('TS.GET', 'ts2')
        assert result == [3000, b'30.5']

    def test_get_empty_series(self):
        """Test TS.GET returns empty array for a series with no samples"""
        # Create an empty time series
        self.client.execute_command('TS.CREATE', 'empty_ts')

        # Get sample from empty series
        result = self.client.execute_command('TS.GET', 'empty_ts')
        assert result == []

    def test_get_nonexistent_key(self):
        """Test TS.GET behavior with a nonexistent key"""
        # Attempt to get sample from nonexistent time series
        with pytest.raises(Exception) as excinfo:
            self.client.execute_command('TS.GET', 'nonexistent_ts')
        assert "the key does not exist" in str(excinfo.value)

    def test_get_after_del(self):
        """Test TS.GET after deleting samples"""
        # Create a time series and add samples
        self.client.execute_command('TS.CREATE', 'ts_del')
        self.client.execute_command('TS.ADD', 'ts_del', 1000, 10.5)
        self.client.execute_command('TS.ADD', 'ts_del', 2000, 20.5)
        self.client.execute_command('TS.ADD', 'ts_del', 3000, 30.5)

        # Delete the latest sample
        self.client.execute_command('TS.DEL', 'ts_del', 3000, 3000)

        # Get the latest sample (should be the second-latest)
        result = self.client.execute_command('TS.GET', 'ts_del')
        assert result == [2000, b'20.5']

    def test_get_after_all_deleted(self):
        """Test TS.GET after deleting all samples"""
        # Create a time series and add a sample
        self.client.execute_command('TS.CREATE', 'ts_all_del')
        self.client.execute_command('TS.ADD', 'ts_all_del', 1000, 10.5)

        # Delete the only sample
        self.client.execute_command('TS.DEL', 'ts_all_del', 0, 2000)

        # Get the latest sample (should be empty)
        result = self.client.execute_command('TS.GET', 'ts_all_del')
        assert result == []

    def test_get_after_add_with_same_timestamp(self):
        """Test TS.GET after adding a sample with the same timestamp"""
        # Create a time series and add a sample
        self.client.execute_command('TS.CREATE', 'ts_same_ts', 'DUPLICATE_POLICY', 'LAST')
        self.client.execute_command('TS.ADD', 'ts_same_ts', 1000, 10.5)

        # Add another sample with the same timestamp
        self.client.execute_command('TS.ADD', 'ts_same_ts', 1000, 20.5)

        # Get the latest sample (should return the most recently added value)
        result = self.client.execute_command('TS.GET', 'ts_same_ts')
        assert result == [1000, b'20.5']

    def test_get_latest_with_compaction(self):
        """Test TS.GET with LATEST returns partial bucket from compaction rule"""
        # Create source and destination time series
        self.client.execute_command('TS.CREATE', 'source')
        self.client.execute_command('TS.CREATE', 'dest')

        # Create a compaction rule: 5-second buckets with avg aggregation
        self.client.execute_command('TS.CREATERULE', 'source', 'dest', 'AGGREGATION', 'avg', 5000)

        # Add samples to source series (within same bucket: 1000-5999)
        self.client.execute_command('TS.ADD', 'source', 1000, 10)
        self.client.execute_command('TS.ADD', 'source', 2000, 20)
        self.client.execute_command('TS.ADD', 'source', 3000, 30)

        # At this point, bucket is not closed yet (partial bucket)
        # Regular GET on dest should return the last sample since the last bucket hasn't been finalized
        result = self.client.execute_command('TS.GET', 'dest')
        assert result == []

        # LATEST should return the partial aggregation (avg of 10, 20, 30 = 20)
        result = self.client.execute_command('TS.GET', 'dest', 'LATEST')
        assert result[0] == 0  # bucket start timestamp
        assert float(result[1]) == 20.0  # avg(10, 20, 30)

    def test_get_latest_with_closed_bucket(self):
        """Test TS.GET with LATEST when bucket is closed returns finalized value"""
        # Create source and destination time series
        self.client.execute_command('TS.CREATE', 'source')
        self.client.execute_command('TS.CREATE', 'dest')

        # Create a compaction rule: 5-second buckets
        self.client.execute_command('TS.CREATERULE', 'source', 'dest', 'AGGREGATION', 'sum', 5000)

        # Add samples to first bucket (1000-5999)
        self.client.execute_command('TS.ADD', 'source', 1000, 10)
        self.client.execute_command('TS.ADD', 'source', 2000, 20)

        # Add sample to next bucket (6000+) - this closes the first bucket
        self.client.execute_command('TS.ADD', 'source', 6000, 30)

        # Regular GET should now return the closed bucket value
        result = self.client.execute_command('TS.GET', 'dest')
        assert result[0] == 0  # first bucket start
        assert float(result[1]) == 30.0  # sum(10, 20)

        # LATEST should return the partial second bucket
        result_latest = self.client.execute_command('TS.GET', 'dest', 'LATEST')
        assert result_latest[0] == 5000  # second bucket start
        assert float(result_latest[1]) == 30.0  # sum(30)

    def test_get_latest_no_compaction_rule(self):
        """Test TS.GET with LATEST on series without compaction returns latest sample"""
        # Create a simple time series without compaction
        self.client.execute_command('TS.CREATE', 'simple')
        self.client.execute_command('TS.ADD', 'simple', 1000, 10)
        self.client.execute_command('TS.ADD', 'simple', 2000, 20)
        self.client.execute_command('TS.ADD', 'simple', 3000, 30)

        # Both GET and GET LATEST should return the same latest sample
        result = self.client.execute_command('TS.GET', 'simple')
        result_latest = self.client.execute_command('TS.GET', 'simple', 'LATEST')

        assert result == result_latest
        assert result[0] == 3000
        assert float(result[1]) == 30.0

    def test_get_latest_compaction_empty_source(self):
        """Test TS.GET with LATEST on compaction dest when source is empty"""
        # Create source and destination
        self.client.execute_command('TS.CREATE', 'empty_source')
        self.client.execute_command('TS.CREATE', 'empty_dest')
        self.client.execute_command('TS.CREATERULE', 'empty_source', 'empty_dest', 'AGGREGATION', 'avg', 5000)

        # Both GET and GET LATEST should return empty
        result = self.client.execute_command('TS.GET', 'empty_dest')
        result_latest = self.client.execute_command('TS.GET', 'empty_dest', 'LATEST')

        assert result == []
        assert result_latest == []

    def test_get_no_args(self):
        """Test TS.GET with no arguments"""
        # Test missing key argument
        self.verify_error_response(self.client, 'TS.GET',
                                   "wrong number of arguments for 'ts.get' command")

    def test_get_too_many_args(self):
        """Test TS.GET with too many arguments"""
        # Create a time series
        self.client.execute_command('TS.CREATE', 'ts_extra')

        # Test with extra arguments
        self.verify_error_response(self.client, 'TS.GET ts_extra latest other',
                                   "wrong number of arguments for 'ts.get' command")

    def test_get_with_nan_value(self):
        """Test TS.GET with a NaN value in the time series"""
        # Create a time series and add a NaN sample
        self.client.execute_command('TS.CREATE', 'ts_nan')
        self.client.execute_command('TS.ADD', 'ts_nan', 1000, 'NaN')

        # Get the latest sample
        result = self.client.execute_command('TS.GET', 'ts_nan')
        assert math.isnan(float(result[1]))
