import pytest
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


# todo: validate index changes when labels are altered
class TestTimeSeriesAlter(ValkeyTimeSeriesTestCaseBase):

    def setup_data(self):
        # Create a base series for alteration tests
        self.key = 'ts_alter_test'
        self.initial_retention = 0
        self.initial_labels = ['sensor', 'temp', 'area', 'A1']
        self.initial_chunk_size = 4096  # Default chunk size might vary, get from INFO if needed
        self.initial_policy = None  # Default BLOCK

        self.client.execute_command(
            'TS.CREATE', self.key,
            'LABELS', *self.initial_labels
        )
        self.client.execute_command('TS.ADD', self.key, 1000, 25)

        # Verify initial state (especially default chunk size)
        info = self.ts_info(self.key, True)
        print(info)
        self.initial_chunk_size = info['chunkSize']  # Get actual default
        self.initial_retention = info['retentionTime']

    def test_alter_retention(self):
        """Test altering the retention period"""
        self.setup_data()

        new_retention = 60000  # 1 minute
        assert self.client.execute_command('TS.ALTER', self.key, 'RETENTION', new_retention) == b'OK'

        info = self.ts_info(self.key)
        labels = info['labels']
        assert info['retentionTime'] == new_retention
        # Other properties should remain unchanged
        assert labels['sensor'] == 'temp'
        assert labels['area'] == 'A1'

    @pytest.mark.parametrize('encoding', ['COMPRESSED', 'UNCOMPRESSED'])
    @pytest.mark.parametrize('widened', [0, 100000])
    def test_widening_retention_does_not_resurrect_expired_samples(self, encoding, widened):
        """Expired samples may still be stored until something trims them physically, and
        when that happens differs between nodes (the trim cron is per node). Widening the
        window must not bring them back, or the answer would depend on trim history.

        `lazy` keeps what the write path left behind; `trimmed` holds the same data with
        every expired sample dropped (TS.DEL trims before it deletes, and nothing at
        timestamp 0 is inside the window)."""
        pipe = self.client.pipeline(transaction=False)
        for key in ('lazy', 'trimmed'):
            pipe.execute_command('TS.CREATE', key, 'RETENTION', 100, 'ENCODING', encoding)
            for ts in range(1, 201):
                pipe.execute_command('TS.ADD', key, ts, ts)
        pipe.execute()
        assert self.client.execute_command('TS.DEL', 'trimmed', 0, 0) == 0

        visible = self.client.execute_command('TS.RANGE', 'lazy', '-', '+')
        assert len(visible) == 101
        assert self.client.execute_command('TS.RANGE', 'trimmed', '-', '+') == visible

        for key in ('lazy', 'trimmed'):
            assert self.client.execute_command('TS.ALTER', key, 'RETENTION', widened) == b'OK'

        for key in ('lazy', 'trimmed'):
            assert self.client.execute_command('TS.RANGE', key, '-', '+') == visible, key
            info = self.ts_info(key)
            assert info['totalSamples'] == 101, key
            assert info['firstTimestamp'] == 100, key
        assert self.client.execute_command('DEBUG', 'DIGEST-VALUE', 'lazy') == \
            self.client.execute_command('DEBUG', 'DIGEST-VALUE', 'trimmed')

    def test_alter_chunk_size(self):
        """Test altering the chunk size"""
        self.setup_data()

        new_chunk_size = 128
        assert self.client.execute_command('TS.ALTER', self.key, 'CHUNK_SIZE', new_chunk_size) == b'OK'

        info = self.ts_info(self.key)
        labels = info['labels']

        assert info['chunkSize'] == new_chunk_size
        # Other properties should remain unchanged
        assert info['retentionTime'] == self.initial_retention
        # Other properties should remain unchanged
        assert labels['sensor'] == 'temp'
        assert labels['area'] == 'A1'

    def test_alter_labels(self):
        """Test altering labels"""
        self.setup_data()

        new_labels = ['region', 'emea', 'flavor', 'chocolate']
        assert self.client.execute_command('TS.ALTER', self.key, 'LABELS', *new_labels) == b'OK'

        info = self.ts_info(self.key)
        labels = info['labels']

        assert labels['region'] == 'emea'
        assert labels['flavor'] == 'chocolate'

    def test_alter_labels_clear(self):
        """Test altering labels to an empty set"""
        self.setup_data()

        assert self.client.execute_command('TS.ALTER', self.key, 'LABELS') == b'OK'

        info = self.ts_info(self.key)
        labels = info['labels']

        assert labels is None or len(labels) == 0

    def test_alter_duplicate_policy(self):
        """Test altering the duplicate policy"""
        self.setup_data()

        new_policy = 'SUM'

        # Use ON_DUPLICATE or DUPLICATE_POLICY based on command implementation
        assert self.client.execute_command('TS.ALTER', self.key, 'DUPLICATE_POLICY', new_policy) == b'OK'

        info = self.ts_info(self.key)
        labels = info['labels']

        assert info['duplicatePolicy'] == new_policy.lower()
        # Other properties should remain unchanged
        assert info['retentionTime'] == self.initial_retention
        assert info['chunkSize'] == self.initial_chunk_size

        assert labels['sensor'] == 'temp'
        assert labels['area'] == 'A1'

    def test_alter_multiple_properties(self):
        """Test altering multiple properties at once"""
        self.setup_data()

        new_retention = 30000
        new_labels = ['location', 'server_room']
        new_policy = 'MAX'

        assert self.client.execute_command(
            'TS.ALTER', self.key,
            'RETENTION', new_retention,
            'DUPLICATE_POLICY', new_policy,
            'LABELS', *new_labels,
        ) == b'OK'

        info = self.ts_info(self.key)
        labels = info['labels']

        assert info['retentionTime'] == new_retention
        assert info['duplicatePolicy'] == new_policy.lower()
        assert info['chunkSize'] == self.initial_chunk_size

        assert labels['location'] == 'server_room'

    def test_alter_non_existent_key(self):
        """Test TS.ALTER on a non-existent key"""
        self.setup_data()

        with pytest.raises(ResponseError, match="key does not exist"):
            self.client.execute_command('TS.ALTER', 'non_existent_key', 'RETENTION', 1000)

    def test_alter_wrong_type(self):
        """Test TS.ALTER on a key of the wrong type"""
        self.setup_data()

        string_key = 'my_string_key'
        self.client.set(string_key, 'hello')
        with pytest.raises(ResponseError, match="WRONGTYPE"):
            self.client.execute_command('TS.ALTER', string_key, 'RETENTION', 1000)

    def test_alter_invalid_arguments(self):
        """Test TS.ALTER with invalid argument values"""
        self.setup_data()

        # Invalid retention
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.ALTER', self.key, 'RETENTION', -10)
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.ALTER', self.key, 'RETENTION', 'not_a_number')

        # Invalid chunk size
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.ALTER', self.key, 'CHUNK_SIZE', 0)  # Must be > 0
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.ALTER', self.key, 'CHUNK_SIZE', 'not_a_number')

        # Invalid duplicate policy
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.ALTER', self.key, 'DUPLICATE_POLICY', 'INVALID_POLICY')

        # Odd number of labels
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.ALTER', self.key, 'LABELS', 'label1')

        # Unknown option
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.ALTER', self.key, 'UNKNOWN_OPTION', 'value')

    def test_alter_encoding_is_validated_then_ignored(self):
        """As on RedisTimeSeries, ENCODING is accepted but a series keeps the encoding it was
        created with. Every chunk must keep it too, or the series would no longer load."""
        client = self.client
        for created in ('UNCOMPRESSED', 'GORILLA', 'CHIMP'):
            key = f'alter:enc:{created}'
            client.execute_command('TS.CREATE', key, 'ENCODING', created, 'CHUNK_SIZE', 64)
            for ts in range(1, 41):
                client.execute_command('TS.ADD', key, ts, ts)
            before = self.ts_info(key)['chunkType']
            next_ts = 41
            for args in (['ENCODING', 'UNCOMPRESSED'], ['ENCODING', 'compressed'],
                         ['ENCODING', 'gorilla'], ['ENCODING', 'chimp'],
                         ['COMPRESSED'], ['UNCOMPRESSED']):
                assert client.execute_command('TS.ALTER', key, *args) == b'OK', args
                # Appends past the next chunk boundary still use the series' encoding.
                for _ in range(40):
                    client.execute_command('TS.ADD', key, next_ts, next_ts)
                    next_ts += 1
            assert self.ts_info(key)['chunkType'] == before
            with pytest.raises(ResponseError, match='unknown ENCODING parameter'):
                client.execute_command('TS.ALTER', key, 'ENCODING', 'foo')
            with pytest.raises(ResponseError):
                client.execute_command('TS.ALTER', key, 'ENCODING')
            samples = client.execute_command('TS.RANGE', key, '-', '+')

            client.execute_command('DEBUG', 'RELOAD')
            assert client.execute_command('TS.RANGE', key, '-', '+') == samples
            assert self.ts_info(key)['chunkType'] == before

    def test_alter_rounding(self):
        """Test altering SIGNIFICANT_DIGITS / DECIMAL_DIGITS"""
        self.setup_data()

        assert self.client.execute_command('TS.ALTER', self.key, 'SIGNIFICANT_DIGITS', 3) == b'OK'
        info = self.ts_info(self.key)
        assert info['rounding'] == [b'significantDigits', 3]
        # Rounding applies to values written after the change
        self.client.execute_command('TS.ADD', self.key, 2000, 3.14159)
        assert self.client.execute_command('TS.GET', self.key) == [2000, b'3.14']

        assert self.client.execute_command('TS.ALTER', self.key, 'DECIMAL_DIGITS', 1) == b'OK'
        info = self.ts_info(self.key)
        assert info['rounding'] == [b'decimalDigits', 1]
        self.client.execute_command('TS.ADD', self.key, 3000, 2.71828)
        assert self.client.execute_command('TS.GET', self.key) == [3000, b'2.7']

        # An ALTER that does not mention rounding leaves it unchanged
        assert self.client.execute_command('TS.ALTER', self.key, 'RETENTION', 60000) == b'OK'
        info = self.ts_info(self.key)
        assert info['rounding'] == [b'decimalDigits', 1]
        assert info['retentionTime'] == 60000

        # The two rounding options are mutually exclusive
        with pytest.raises(ResponseError):
            self.client.execute_command(
                'TS.ALTER', self.key, 'SIGNIFICANT_DIGITS', 3, 'DECIMAL_DIGITS', 1)
