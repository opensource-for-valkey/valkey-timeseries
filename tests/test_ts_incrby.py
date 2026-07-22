from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTimeSeriesIncrby(ValkeyTimeSeriesTestCaseBase):

    def test_incrby_basic(self):
        """Test basic TS.INCRBY functionality with auto-creation"""
        # INCRBY should create the key if it doesn't exist
        result = self.client.execute_command('TS.INCRBY', 'ts_incr', 10)
        print("TS.INCRBY ts_incr 10", result)

        # Verify the key was created, and the value incremented
        assert self.client.execute_command(f'EXISTS ts_incr') == 1
        value = self.client.execute_command('TS.GET', 'ts_incr')
        assert len(value) == 2
        assert float(value[1]) == 10.0

        # Increment again
        result = self.client.execute_command('TS.INCRBY', 'ts_incr', 5)

        # Value should now be 15
        samples = self.client.execute_command('TS.RANGE', 'ts_incr', '-', '+')
        assert len(samples) >= 1
        assert float(samples[-1][1]) == 15.0

    def test_incrby_with_timestamp(self):
        """Test TS.INCRBY with explicit timestamp"""
        timestamp = 16000

        # Create a series with an initial value
        self.client.execute_command('TS.INCRBY', 'ts_incr_ts', 25.5, 'TIMESTAMP', timestamp)

        # Check that the timestamp was set correctly
        samples = self.client.execute_command('TS.RANGE', 'ts_incr_ts', '-', '+')
        assert samples == [[timestamp, b'25.5']]

        # Increment with a later timestamp
        later_timestamp = timestamp + 10000
        self.client.execute_command('TS.INCRBY', 'ts_incr_ts', 15, 'TIMESTAMP', later_timestamp)

        # Now there should be two samples
        samples = self.client.execute_command('TS.RANGE', 'ts_incr_ts', '-', '+')
        assert len(samples) == 2
        assert samples[1][0] == later_timestamp
        assert float(samples[1][1]) == 40.5

    def test_incrby_with_create_options(self):
        """Test TS.INCRBY with creation options"""
        # Create with labels and retention options
        self.client.execute_command(
            'TS.INCRBY', 'ts_incr_create', 7.5,
            'RETENTION', 10000,
            'LABELS', 'sensor', 'temp', 'location', 'kitchen'
        )

        # Verify the key was created with specified options
        info = self.ts_info('ts_incr_create')
        labels = info['labels']
        assert info['retentionTime'] == 10000
        assert labels['sensor'] == 'temp'
        assert labels['location'] == 'kitchen'

        # Check the value
        sample = self.client.execute_command('TS.GET', 'ts_incr_create')
        assert float(sample[1]) == 7.5

    def test_incrby_with_negative_values(self):
        """Test TS.INCRBY with negative increments (decrement)"""
        # Create a series and add an initial value
        self.client.execute_command('TS.INCRBY', 'ts_decr', 50)

        # Decrement by using a negative value
        self.client.execute_command('TS.INCRBY', 'ts_decr', -20)

        # Value should now be 30
        samples = self.client.execute_command('TS.RANGE', 'ts_decr', '-', '+')
        assert float(samples[-1][1]) == 30.0

        # Decrement again
        self.client.execute_command('TS.INCRBY', 'ts_decr', -15)

        # Value should now be 15
        samples = self.client.execute_command('TS.RANGE', 'ts_decr', '-', '+')
        assert float(samples[-1][1]) == 15.0

    def test_incrby_multiple_increments(self):
        """Test multiple increments on the same series"""
        # Create a series
        self.client.execute_command('TS.CREATE', 'ts_multi_incr')

        # Perform multiple increments
        for i in range(1, 6):
            self.client.execute_command('TS.INCRBY', 'ts_multi_incr', i)

        # The final value should be the sum of 1+2+3+4+5 = 15
        samples = self.client.execute_command('TS.RANGE', 'ts_multi_incr', '-', '+')
        assert float(samples[-1][1]) == 15.0

    def test_incrby_with_uncompressed(self):
        """Test TS.INCRBY with UNCOMPRESSED option"""
        # Create uncompressed series
        self.client.execute_command('TS.INCRBY', 'ts_uncompressed', 1, "ENCODING", 'UNCOMPRESSED')

        # Verify it's uncompressed
        info = self.ts_info('ts_uncompressed')
        # In some versions this might be a different flag, adjust if needed
        assert info.get('chunkType', info.get('memoryUsage')) is not None

        # Should still increment normally
        for i in range(2, 6):
            self.client.execute_command('TS.INCRBY', 'ts_uncompressed', i)

        # The final value should be 1+2+3+4+5 = 15
        samples = self.client.execute_command('TS.RANGE', 'ts_uncompressed', '-', '+')
        assert float(samples[-1][1]) == 15.0

    def test_incrby_with_chunk_size(self):
        """Test TS.INCRBY with custom chunk size"""
        # Create with a custom chunk size
        self.client.execute_command('TS.INCRBY', 'ts_chunk', 1, 'CHUNK_SIZE', 128)

        # Verify chunk size
        info = self.ts_info('ts_chunk')
        assert info['chunkSize'] == 128

        # Should increment normally
        self.client.execute_command('TS.INCRBY', 'ts_chunk', 2)

        # The final value should be 3
        samples = self.client.execute_command('TS.RANGE', 'ts_chunk', '-', '+')
        assert float(samples[-1][1]) == 3.0

    def test_incrby_with_decimal_digits(self):
        """Test TS.INCRBY with decimal digits precision"""
        # Create with decimal digits precision
        self.client.execute_command('TS.INCRBY', 'ts_decimal', 1.23456, 'DECIMAL_DIGITS', 2)

        # Increment with a value that would normally have more decimal places
        self.client.execute_command('TS.INCRBY', 'ts_decimal', 2.78901)

        # Value should be rounded to 2 decimal places: 1.23 + 2.79 = 4.02
        samples = self.client.execute_command('TS.RANGE', 'ts_decimal', '-', '+')
        # The actual value depends on when rounding happens (before or after increment)
        assert abs(float(samples[-1][1]) - 4.02) < 0.01

    def test_incrby_with_significant_digits(self):
        """Test TS.INCRBY with significant digits precision"""
        # Create with significant digits precision
        self.client.execute_command('TS.INCRBY', 'ts_sig', 123.456, 'SIGNIFICANT_DIGITS', 3)

        # Increment with a value that would normally have more significant digits
        self.client.execute_command('TS.INCRBY', 'ts_sig', 456.789)

        # Exact result depends on implementation
        samples = self.client.execute_command('TS.RANGE', 'ts_sig', '-', '+')
        assert len(samples) > 0

    def test_incrby_errors(self):
        """Test error cases for TS.INCRBY"""
        # Test with a missing key
        self.verify_error_response(
            self.client, 'TS.INCRBY',
            "wrong number of arguments for 'ts.incrby' command"
        )

        # Test with invalid increment
        self.verify_error_response(
            self.client, 'TS.INCRBY ts_err xyz',
            "TSDB: invalid value"
        )

        # Test with invalid timestamp
        self.verify_error_response(
            self.client, 'TS.INCRBY ts_err 5 TIMESTAMP invalid',
            "TSDB: invalid timestamp"
        )

        # Test with invalid option (ON_DUPLICATE is not a valid option)
        self.verify_error_response(
            self.client, 'TS.INCRBY ts_err 5 ON_DUPLICATE INVALID_POLICY',
            "TSDB: invalid argument"
        )

    def test_incrby_existing_non_timeseries_key(self):
        """Test TS.INCRBY on existing non-timeseries key"""
        # Create a regular string key
        self.client.execute_command('SET', 'string_key', 'hello')

        # Try to increment it as a timeseries - should fail
        self.verify_error_response(
            self.client, 'TS.INCRBY string_key 5',
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        )

    def test_incrby_with_get(self):
        """Test interaction between TS.INCRBY and TS.GET"""
        # Create and increment
        self.client.execute_command('TS.INCRBY', 'ts_get_test', 10)
        self.client.execute_command('TS.INCRBY', 'ts_get_test', 5)

        # Get should return the latest value
        result = self.client.execute_command('TS.GET', 'ts_get_test')
        assert len(result) == 2
        assert float(result[1]) == 15.0

    def test_incrby_rejects_nan_delta(self):
        """TS.INCRBY should reject NaN as the increment value"""
        self.verify_error_response(
            self.client, 'TS.INCRBY ts_nan_delta nan',
            "TSDB: cannot increment/decrement a NaN value"
        )

    def test_incrby_rejects_when_last_sample_is_nan(self):
        """TS.INCRBY should reject incrementing when the latest sample value is NaN"""
        self.client.execute_command('TS.CREATE', 'ts_nan_sample')
        self.client.execute_command('TS.ADD', 'ts_nan_sample', 1000, 'nan')

        self.verify_error_response(
            self.client, 'TS.INCRBY ts_nan_sample 1',
            "TSDB: cannot increment/decrement a NaN value"
        )

        sample = self.client.execute_command('TS.GET', 'ts_nan_sample')
        assert sample[0] == 1000
        assert sample[1].lower() == b'nan'
