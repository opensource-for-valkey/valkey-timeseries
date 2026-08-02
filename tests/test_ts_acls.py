import os
from typing import List

import pytest
import time

from valkey import ResponseError, Valkey

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


class TestTimeSeriesACL(ValkeyTimeSeriesTestCaseBase):
    """Integration tests for TimeSeries ACL validation"""

    def _verify_user_permissions(
            self, client: Valkey, cmd: List[str], should_access: bool
    ):
        try:
            client.execute_command(*cmd)
        except ResponseError as e:
            if should_access:
                # Make sure the error is not related to permission issues
                assert "has no permissions to run" not in str(e)
        except Exception as e:
            # Any other error is acceptable. This is done to avoid errors
            # Of missing index
            assert True

    def create_test_user(self, username: str, password: str, acl_rules: list) -> None:
        """Create a test user with specific ACL permissions"""

        acl_command = ['ACL', 'SETUSER', username, 'ON', f'>{password}'] + acl_rules
        self.client.execute_command(*acl_command)

    def get_user_client(self, username: str, password: str):
        """Get a client authenticated as a specific user"""
        client = self.server.get_new_client()
        client.execute_command('AUTH', username, password)
        return client

    def test_ts_add_acl_permissions(self):
        """Test TS.ADD command with various ACL permissions"""
        # User with full TS permissions
        self.create_test_user('ts_full', 'password123', [
            '+@all', '+@timeseries', '+@keyspace', '~*'
        ])

        # User with only TS.ADD permission
        self.create_test_user('ts_add_only', 'password123', ['+@write', '+@timeseries', '+TS.ADD', '~*'])

        # User with no TS permissions
        self.create_test_user('no_ts', 'password123', [
            '+@read', '-@timeseries'
        ])

        # Test full permissions user
        ts_full_client = self.get_user_client('ts_full', 'password123')

        result = ts_full_client.execute_command('TS.ADD', 'ts:acl:full', '*', 100.5)
        assert result is not None

        # Test add-only user
        ts_add_client = self.get_user_client('ts_add_only', 'password123')
        result = ts_add_client.execute_command('TS.ADD', 'ts:acl:add_only', '*', 200.5)
        assert result is not None

        # Test user without TS permissions
        no_ts_client = self.get_user_client('no_ts', 'password123')

        with pytest.raises(Exception, match="no permissions to run the 'ts.add' command"):
            no_ts_client.execute_command('TS.ADD', 'ts:acl:denied', '*', 300.5)

    def test_ts_range_acl_permissions(self):
        """Test TS.RANGE command with various ACL permissions"""
        # Setup test data as admin
        self.client.execute_command('TS.CREATE', 'ts:acl:range_test')
        timestamp = int(time.time() * 1000)
        self.client.execute_command('TS.ADD', 'ts:acl:range_test', timestamp, 42.0)

        # User with read permissions
        self.create_test_user('ts_range_reader', 'password123', [
            '+@all', '+@read', '+ts.range', '~*', '&*'
        ])

        # User without read permissions
        self.create_test_user('range_no_read_user', 'password123', [
            '+@timeseries', '-@read', '+ts.range', '~non_existent', '&*'
        ])

        # Test reader can access TS.RANGE
        reader_client = self.get_user_client('ts_range_reader', 'password123')

        result = reader_client.execute_command('TS.RANGE', 'ts:acl:range_test', '-', '+')
        assert len(result) > 0

        # Test user without read permissions
        no_read_client = self.get_user_client('range_no_read_user', 'password123')

        with pytest.raises(Exception, match="No permissions to access a key"):
            no_read_client.execute_command('TS.RANGE', 'ts:acl:range_test', '-', '+')

    def test_ts_nrange_acl_permissions(self):
        """TS.NRANGE fails closed when the user cannot read *every* key it names."""
        # Two series under different prefixes, so a key pattern can cover one but not the other.
        for key in ('ts:acl:nrange:allowed', 'ts:acl:other:denied'):
            self.client.execute_command('TS.CREATE', key)
            self.client.execute_command('TS.ADD', key, 1000, 1.0)

        self.create_test_user('nrange_partial', 'password123', [
            '+@read', '+@timeseries', '~ts:acl:nrange:*', '&*'
        ])
        partial_client = self.get_user_client('nrange_partial', 'password123')

        # The permitted key on its own is fine.
        result = partial_client.execute_command('TS.NRANGE', 1, 'ts:acl:nrange:allowed', '-', '+')
        assert len(result) == 1

        # Adding a key the user cannot read fails the whole query, in either position and in
        # either direction.
        for command in ('TS.NRANGE', 'TS.NREVRANGE'):
            for argv in (
                    (2, 'ts:acl:nrange:allowed', 'ts:acl:other:denied', '-', '+'),
                    (2, 'ts:acl:other:denied', 'ts:acl:nrange:allowed', '-', '+'),
            ):
                with pytest.raises(ResponseError) as exc_info:
                    partial_client.execute_command(command, *argv)
                assert 'NOPERM' in str(exc_info.value) or 'permission' in str(exc_info.value).lower()

    def test_compaction_rule_acl_permissions(self):
        """Test compaction rule operations with ACL permissions"""
        # Setup source and destination series as admin
        self.client.execute_command('TS.CREATE', 'ts:acl:source')
        self.client.execute_command('TS.CREATE', 'ts:acl:dest')
        self.client.execute_command('TS.CREATE', 'ts:acl:dest2')
        timestamp = int(time.time() * 1000)
        self.client.execute_command('TS.ADD', 'ts:acl:source', timestamp, 100.0)

        # User with compaction permissions
        self.create_test_user('ts_compaction', 'password123', [
            '+@all', '+@timeseries', '+ts.createrule', '+ts.deleterule', '~ts:acl:*'
        ])

        # User without compaction permissions
        self.create_test_user('no_compaction', 'password123', [
            '+@read', '+ts.createrule', '-ts.deleterule'
        ])

        # Test user with compaction permissions
        compaction_client = self.get_user_client('ts_compaction', 'password123')
        result = compaction_client.execute_command(
            'TS.CREATERULE', 'ts:acl:source', 'ts:acl:dest',
            'AGGREGATION', 'avg', 60000
        )
        assert result == 'OK' or result == b'OK'

        # Test user without compaction permissions
        no_compaction_client = self.get_user_client('no_compaction', 'password123')
        with pytest.raises(Exception, match="No permissions to access a key"):
            no_compaction_client.execute_command(
                'TS.CREATERULE', 'ts:acl:source', 'ts:acl:dest2',
                'AGGREGATION', 'sum', 60000
            )

    def test_key_pattern_acl_restrictions(self):
        """Test ACL restrictions based on key patterns"""
        # User restricted to specific key patterns
        self.create_test_user('pattern_user', 'password123', [
            '+@all', '~ts:allowed:*', '+ts.add'
        ])

        pattern_client = self.get_user_client('pattern_user', 'password123')

        # Should succeed for an allowed pattern
        result = pattern_client.execute_command('TS.ADD', 'ts:allowed:metric1', '*', 100.0)
        assert result is not None

        # Should fail for a disallowed pattern
        with pytest.raises(Exception, match="No permissions to access a key"):
            pattern_client.execute_command('TS.ADD', 'ts:forbidden:metric1', '*', 100.0)

    def test_acl_with_compaction_workflow(self):
        """Test ACL permissions in a complete compaction workflow"""

        # Setup users with specific roles
        self.create_test_user('data_producer', 'password123', [
            '+ts.add', '+ts.create', '~ts:acl:workflow*'
        ])

        self.create_test_user('rule_manager', 'password123', [
            '+ts.createrule', '+ts.deleterule', '+@read', '+@write', '+@timeseries', '~*'
        ])

        self.create_test_user('data_consumer', 'password123', [
            '+@read', '+ts.range', '~ts:acl:workflow*'
        ])

        # Producer creates series and adds data
        producer = self.get_user_client('data_producer', 'password123')
        producer.execute_command('TS.CREATE', 'ts:acl:workflow:source')
        producer.execute_command('TS.CREATE', 'ts:acl:workflow:dest')
        producer.execute_command('TS.CREATE', 'ts:acl:workflow:dest2')

        timestamp = int(time.time() * 1000)
        for i in range(5):
            producer.execute_command('TS.ADD', 'ts:acl:workflow:source', timestamp + i * 1000, i * 10.0)

        # Manager sets up compaction rule
        manager = self.get_user_client('rule_manager', 'password123')
        manager.execute_command(
            'TS.CREATERULE', 'ts:acl:workflow:source', 'ts:acl:workflow:dest',
            'AGGREGATION', 'avg', 5000
        )

        # Consumer reads aggregated data
        consumer = self.get_user_client('data_consumer', 'password123')
        result = consumer.execute_command('TS.RANGE', 'ts:acl:workflow:dest', '-', '+')

        # Verify the workflow succeeded (the result may be empty if the aggregation window not complete)
        assert isinstance(result, list)

        # Verify role separation - producer can't create rules
        producer = self.get_user_client('data_producer', 'password123')
        with pytest.raises(Exception, match="User data_producer has no permissions to run the 'ts.createrule' command"):
            producer.execute_command(
                'TS.CREATERULE', 'ts:acl:workflow:source', 'ts:acl:workflow:dest2',
                'AGGREGATION', 'sum', 5000
            )

        # Consumer can't delete rules
        consumer = self.get_user_client('data_consumer', 'password123')
        with pytest.raises(Exception, match="User data_consumer has no permissions to run the 'ts.deleterule' command"):
            consumer.execute_command('TS.DELETERULE', 'ts:acl:workflow:source', 'ts:acl:workflow:dest')

    def test_acl_command_category_restrictions(self):
        """Test ACL restrictions using command categories"""
        # Set up test data as admin first
        self.client.execute_command('TS.CREATE', 'ts:acl:categories')
        self.client.execute_command('TS.ADD', 'ts:acl:categories', '1000', 42.0)

        # User with only read category
        self.create_test_user('read_only', 'password123', [
            '+@read', '-@write', '+ts.range', '~*'
        ])

        self.create_test_user('ts_safe', 'password123', [
            '+@timeseries', '+ts.add', '+ts.range', '+@read', '~ts:acl:categories'
        ])

        # Read-only user can read but not write
        read_only_client = self.get_user_client('read_only', 'password123')
        result = read_only_client.execute_command('TS.RANGE', 'ts:acl:categories', '-', '+')
        assert len(result) > 0

        with pytest.raises(Exception, match="User read_only has no permissions to run the 'ts.add' command"):
            read_only_client.execute_command('TS.ADD', 'ts:acl:categories', '2000', 100.0)

        # TS safe user can use timeseries commands
        ts_safe_client = self.get_user_client('ts_safe', 'password123')
        result = ts_safe_client.execute_command('TS.ADD', 'ts:acl:categories', '3000', 200.0)
        assert result is not None

    def test_acl_with_multiple_series(self):
        """Test ACL validation with multiple timeseries operations"""
        # Create a user with limited permissions
        self.create_test_user('multi_user', 'password123', [
            '+ts.add', '+ts.range', '+ts.create', '+@timeseries', '~ts:acl:multi*'
        ])

        multi_client = self.get_user_client('multi_user', 'password123')

        # Create multiple series
        series_names = ['ts:acl:multi1', 'ts:acl:multi2', 'ts:acl:multi3']

        for series_name in series_names:
            multi_client.execute_command('TS.CREATE', series_name)

        # Add data to each series
        base_timestamp = int(time.time() * 1000)
        for i, series_name in enumerate(series_names):
            for j in range(10):
                timestamp = base_timestamp + j * 1000
                value = float(i * 10 + j)
                result = multi_client.execute_command('TS.ADD', series_name, timestamp, value)
                assert result is not None

        # Verify data can be read back
        for series_name in series_names:
            result = multi_client.execute_command('TS.RANGE', series_name, '-', '+')
            assert len(result) == 10

    def test_acl_info_command_permissions(self):
        """Test TS.INFO command with ACL permissions"""
        # Setup test data as admin
        self.client.execute_command('TS.CREATE', 'ts:acl:info_test',
                                    'RETENTION', 10000,
                                    'LABELS', 'sensor', 'temperature')
        self.client.execute_command('TS.ADD', 'ts:acl:info_test', '*', 25.5)

        # User with info permissions
        self.create_test_user('info_user', 'password123', ['+@read', '+@timeseries', '+ts.info', '~*'])

        # User without info permissions
        self.create_test_user('no_info', 'password123', ['-ts.info', '+@timeseries'])

        info_client = self.get_user_client('info_user', 'password123')

        # User with info permissions can access TS.INFO
        result = info_client.execute_command('TS.INFO', 'ts:acl:info_test')
        assert isinstance(result, list)
        assert len(result) > 0

        # User without key access gets denied. TS.INFO declares a key spec (key at
        # position 1), so the denial is enforced by the server's core ACL layer -- the same
        # "No permissions to access a key" path as the other keyed read commands above --
        # rather than by the module's own permission check.
        no_info_client = self.get_user_client('no_info', 'password123')
        with pytest.raises(Exception, match="No permissions to access a key"):
            no_info_client.execute_command('TS.INFO', 'ts:acl:info_test')

    def test_acl_queryindex_permissions(self):
        """Test TS.QUERYINDEX command with ACL permissions"""
        # Set up test data with labels
        self.client.execute_command('TS.CREATE', 'ts:acl:sensor1',
                                    'LABELS', 'type', 'temperature', 'location', 'room1')
        self.client.execute_command('TS.CREATE', 'ts:acl:sensor2',
                                    'LABELS', 'type', 'humidity', 'location', 'room1')

        # User with query permissions
        self.create_test_user('query_user', 'password123', [
            '+@read', '+ts.queryindex', '+@timeseries', '~*', '&*'
        ])

        # User without query permissions
        self.create_test_user('no_query', 'password123', [
            '+ts.add', '-ts.queryindex'
        ])

        query_client = self.get_user_client('query_user', 'password123')

        # User with query permissions can use TS.QUERYINDEX
        result = query_client.execute_command('TS.QUERYINDEX', 'location=room1')
        assert isinstance(result, list)
        assert len(result) >= 2  # Should find both sensors

        # User without query permissions gets denied
        no_query_client = self.get_user_client('no_query', 'password123')
        with pytest.raises(Exception) as exc_info:
            no_query_client.execute_command('TS.QUERYINDEX', 'location=room1')
        assert 'NOPERM' in str(exc_info.value) or 'permission' in str(exc_info.value).lower()

        ## TODO: for multi-key commands like MADD, MRANGE, MREVRANGE, MGET, JOIN we need to test with multiple keys
        ## We should throw an error if user does not have access to all keys in that case

    def test_timeseries_command_acl_categories(self):
        # List of commands and their acl categories
        timeseries_commands = [
            ('TS.ADD', [b'write', b'denyoom', b'module'], [b'@write', b'@timeseries']),
            ('TS.CREATE', [b'write', b'denyoom', b'module'], [b'@write', b'@fast', b'@timeseries']),
            ('TS.MADD', [b'write', b'denyoom', b'module'], [b'@write', b'@timeseries']),
            ('TS.INFO', [b'readonly', b'module'], [b'@read', b'@fast', b'@timeseries']),
            ('TS.CARD', [b'readonly', b'module'], [b'@read', b'@timeseries']),
            ('TS.ALTER', [b'write', b'denyoom', b'module'], [b'@write', b'@timeseries']),
            ('TS.DEL', [b'write', b'denyoom', b'module'], [b'@write', b'@timeseries']),
            ('TS.GET', [b'readonly', b'module', b'fast'], [b'@read', b'@fast', b'@timeseries']),
            ('TS.RANGE', [b'readonly', b'module'], [b'@read', b'@timeseries']),
            ('TS.READ', [b'readonly', b'module'], [b'@read', b'@timeseries']),
            ('TS.REVRANGE', [b'readonly', b'module'], [b'@read', b'@timeseries']),
            ('TS.MRANGE', [b'readonly', b'module'], [b'@read', b'@timeseries']),
            ('TS.MREVRANGE', [b'readonly', b'module'], [b'@read', b'@timeseries']),
            # movablekeys comes from the numkeys key spec: the key positions are not fixed.
            ('TS.NRANGE', [b'readonly', b'module', b'movablekeys'], [b'@read', b'@timeseries']),
            ('TS.NREVRANGE', [b'readonly', b'module', b'movablekeys'], [b'@read', b'@timeseries']),
            ('TS.INCRBY', [b'write', b'denyoom', b'module'], [b'@write', b'@timeseries']),
            ('TS.DECRBY', [b'write', b'denyoom', b'module'], [b'@write', b'@timeseries']),
            ('TS.CREATERULE', [b'write', b'denyoom', b'module'], [b'@write', b'@timeseries']),
            ('TS.DELETERULE', [b'write', b'denyoom', b'module'], [b'@write', b'@timeseries']),
            ('TS.QUERYINDEX', [b'readonly', b'module'], [b'@read', b'@timeseries']),
            ('TS.LABELSTATS', [b'readonly', b'module'], [b'@read', b'@timeseries']),
            ('TS.JOIN', [b'readonly', b'module'], [b'@read', b'@timeseries']),
            ('TS.MGET', [b'readonly', b'module', b'fast'], [b'@read', b'@timeseries']),
            ('TS.LABELNAMES', [b'readonly', b'module'], [b'@read', b'@timeseries']),
            ('TS.LABELVALUES', [b'readonly', b'module'], [b'@read', b'@timeseries']),
        ]
        for cmd in timeseries_commands:
            # Get the info of the commands and compare the acl categories
            cmd_info = self.client.execute_command(f'COMMAND INFO {cmd[0]}')
            assert cmd_info[0][2] == cmd[
                1], f"ACL categories for command {cmd[0]} do not match. Expected {cmd[1]}, got {cmd_info[0][2]}"
            for category in cmd[2]:
                assert category in cmd_info[0][6], f"Category {category} not found in command {cmd[0]}"

    def verify_valid_user_permissions(self, client, cmd):
        cmd_name = cmd[0].split()[0]
        try:
            result = client.execute_command(cmd[0])
            if cmd[0].startswith("BF.M"):
                assert len(result) == cmd[1]
                # The first add in a new bloom object should always return 1. For MEXISTS the first item we check will have been added as well so should exist
                assert result[0] == 1
            else:
                assert result == cmd[1], f"{cmd_name} should work for default user"
        except Exception as e:
            assert False, f"user should be able to execute {cmd_name}: {str(e)}"
