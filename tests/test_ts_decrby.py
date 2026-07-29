import pytest
import time
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTsDecrby(ValkeyTimeSeriesTestCaseBase):

    def test_decrby_basic(self):
        """Test basic TS.DECRBY functionality"""
        # Create a time series and add a sample
        self.client.execute_command('TS.CREATE', 'ts1')
        self.client.execute_command('TS.ADD', 'ts1', 1000, 50.0)

        # Decrement the latest value
        result = self.client.execute_command('TS.DECRBY', 'ts1', 10.5)
        assert isinstance(result, int)  # Returns the timestamp of the new sample

        # Verify the new latest value
        latest_sample = self.client.execute_command('TS.GET', 'ts1')
        assert latest_sample[0] == result  # Timestamp should match
        assert float(latest_sample[1]) == pytest.approx(39.5)  # 50.5 - 10.5

        # Verify the sample was added
        samples = self.client.execute_command('TS.RANGE', 'ts1', '-', '+')
        print(samples)
        assert samples == [[1000, b'39.5']]

    def test_decrby_creates_key(self):
        """Test TS.DECRBY creates a new timeseries if it doesn't exist"""
        # Decrement a non-existent key
        value_to_decrement = 15.0
        result = self.client.execute_command('TS.DECRBY', 'ts_new', value_to_decrement)
        assert isinstance(result, int)  # Returns timestamp

        # Verify the timeseries was created
        assert self.client.execute_command("EXISTS", "ts_new") == 1

        # Verify the sample was added (starts at 0, so 0 - 15.0)
        latest_sample = self.client.execute_command('TS.GET', 'ts_new')
        assert latest_sample[0] == result
        assert float(latest_sample[1]) == pytest.approx(-15.0)

    def test_decrby_empty_series(self):
        """Test TS.DECRBY on an existing but empty series"""
        self.client.execute_command('TS.CREATE', 'ts_empty')

        # Decrement the empty series
        value_to_decrement = 5.0
        result = self.client.execute_command('TS.DECRBY', 'ts_empty', value_to_decrement)
        assert isinstance(result, int)  # Returns timestamp

        # Verify the sample was added (starts at 0, so 0 - 5.0)
        latest_sample = self.client.execute_command('TS.GET', 'ts_empty')
        assert latest_sample[0] == result
        assert float(latest_sample[1]) == pytest.approx(-5.0)

    def test_decrby_with_timestamp(self):
        """Test TS.DECRBY with a specific timestamp"""
        self.client.execute_command('TS.CREATE', 'ts_timestamp')
        self.client.execute_command('TS.ADD', 'ts_timestamp', 1000, 100.0)

        # Decrement with a specific timestamp
        target_timestamp = 2000
        result = self.client.execute_command('TS.DECRBY', 'ts_timestamp', 25.0, 'TIMESTAMP', target_timestamp)
        assert result == target_timestamp

        # Verify the new sample
        latest_sample = self.client.execute_command('TS.GET', 'ts_timestamp')
        assert latest_sample[0] == target_timestamp
        assert float(latest_sample[1]) == pytest.approx(75.0)  # 100.0 - 25.0

        # Decrement with '*' timestamp
        current_time_approx = int(time.time() * 1000)
        result_star = self.client.execute_command('TS.DECRBY', 'ts_timestamp', 5.0, 'TIMESTAMP', '*')
        assert isinstance(result_star, int)
        assert abs(result_star - current_time_approx) < 5000  # Check if the timestamp is recent

        latest_sample_star = self.client.execute_command('TS.GET', 'ts_timestamp')
        assert latest_sample_star[0] == result_star
        assert float(latest_sample_star[1]) == pytest.approx(70.0)  # 75.0 - 5.0

    def test_decrby_with_options_on_create(self):
        """Test TS.DECRBY creating a key with options"""
        value_to_decrement = 10.0
        retention_ms = 60000
        chunk_size = 128
        labels = ['sensor', 'temp', 'area', 'A1']

        # Decrement non-existent key with options
        result = self.client.execute_command(
            'TS.DECRBY', 'ts_opts', value_to_decrement,
            'RETENTION', retention_ms,
            'CHUNK_SIZE', chunk_size,
            'LABELS', *labels
        )
        assert isinstance(result, int)

        # Verify options were set
        info_dict = self.ts_info('ts_opts')
        labels = info_dict['labels']
        assert info_dict['retentionTime'] == retention_ms
        assert info_dict['chunkSize'] == chunk_size
        assert labels['sensor'] == 'temp'
        assert labels['area'] == 'A1'

        # Verify the value
        latest_sample = self.client.execute_command('TS.GET', 'ts_opts')
        assert latest_sample[0] == result
        assert float(latest_sample[1]) == pytest.approx(-10.0)  # 0 - 10.0

    def test_decrby_by_negative(self):
        """Test TS.DECRBY with a negative value (should increment)"""
        self.client.execute_command('TS.CREATE', 'ts_neg')
        self.client.execute_command('TS.ADD', 'ts_neg', 1000, 20.0)

        # Decrement by a negative value
        result = self.client.execute_command('TS.DECRBY', 'ts_neg', -5.5)
        assert isinstance(result, int)

        # Verify the value was incremented
        latest_sample = self.client.execute_command('TS.GET', 'ts_neg')
        assert float(latest_sample[1]) == pytest.approx(25.5)  # 20.0 - (-5.5)

    def test_decrby_by_zero(self):
        """Test TS.DECRBY with zero value (should add sample with same value)"""
        self.client.execute_command('TS.CREATE', 'ts_zero')
        self.client.execute_command('TS.ADD', 'ts_zero', 1000, 30.0)

        # Decrement by zero
        result = self.client.execute_command('TS.DECRBY', 'ts_zero', 0)
        assert isinstance(result, int)

        # Verify the value is unchanged, but new sample added
        latest_sample = self.client.execute_command('TS.GET', 'ts_zero')
        assert latest_sample[0] == result
        assert float(latest_sample[1]) == pytest.approx(30.0)

        samples = self.client.execute_command('TS.RANGE', 'ts_zero', '-', '+')
        print(samples)
        assert len(samples) == 1
        assert samples[0][0] == result

    def test_decrby_error_handling(self):
        """Test error conditions for TS.DECRBY"""
        self.client.execute_command('TS.CREATE', 'ts_err')
        self.client.execute_command('TS.ADD', 'ts_err', 1000, 10.0)

        # Wrong number of arguments
        with pytest.raises(ResponseError, match="wrong number of arguments"):
            self.client.execute_command('TS.DECRBY', 'ts_err')

        # Non-numeric decrement value
        with pytest.raises(ResponseError, match="invalid increase/decrease value"):
            self.client.execute_command('TS.DECRBY', 'ts_err', 'not_a_number')

        # Invalid timestamp
        with pytest.raises(ResponseError, match="invalid timestamp"):
            self.client.execute_command('TS.DECRBY', 'ts_err', 5.0, 'TIMESTAMP', 'invalid_ts')

        # Timestamp older than the last sample
        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command('TS.DECRBY', 'ts_err', 5.0, 'TIMESTAMP', 500)
        assert "timestamp must be equal to or higher than the maximum existing timestamp" in str(excinfo.value).lower()

        # Wrong type of key
        self.client.set('string_key', 'hello')
        with pytest.raises(ResponseError, match="WRONGTYPE"):
            self.client.execute_command('TS.DECRBY', 'string_key', 1.0)

    def test_decrby_rejects_nan_delta(self):
        """TS.DECRBY should reject NaN as the increment value"""
        self.verify_error_response(
            self.client, 'TS.DECRBY ts_nan_delta nan',
            "TSDB: invalid increase/decrease value"
        )

    def test_decrby_rejects_when_last_sample_is_nan(self):
        """TS.DECRBY should reject incrementing when the latest sample value is NaN"""
        self.client.execute_command('TS.CREATE', 'ts_nan_sample')
        self.client.execute_command('TS.ADD', 'ts_nan_sample', 1000, 'nan')

        self.verify_error_response(
            self.client, 'TS.DECRBY ts_nan_sample 1',
            "TSDB: cannot increment/decrement NaN value"
        )

        sample = self.client.execute_command('TS.GET', 'ts_nan_sample')
        assert sample[0] == 1000
        assert sample[1].lower() == b'nan'
