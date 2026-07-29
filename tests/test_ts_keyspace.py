import time

import pytest
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


class TestNotifications(ValkeyTimeSeriesTestCaseBase):
    """Integration tests for keyspace notifications."""

    def collect_notifications(self, timeout=2.0):
        """Collect all notifications within the timeout period."""
        notifications = []
        start_time = time.time()

        while time.time() - start_time < timeout:
            msg = wait_for_message(self.keyspace_client_subscribe, ignore_subscribe_messages=True)
            if msg and msg['type'] == 'pmessage':
                channel = msg['channel'].decode('utf-8')
                channel = channel.removeprefix('__keyspace@0__:')
                channel = channel.removeprefix('__keyevent@0__:')
                data = msg['data'].decode('utf-8')
                notifications.append({
                    'channel': channel,
                    'data': data
                })

        return notifications

    def create_ts(self, key, timestamp=None, value=1.0):
        """Helper to create a time series with a sample data point."""
        if timestamp is None:
            timestamp = int(time.time() * 1000)

        self.client.execute_command("TS.CREATE", key, "LABELS", "key", key, "sensor", "temp")
        self.client.execute_command("TS.ADD", key, timestamp, value)
        return key

    def create_ts_with_compaction_rule(self, source_key, dest_key, aggregation_type="avg", bucket_duration=60000):
        """Helper to create source and destination time series with compaction rule."""
        # Create source time series
        self.client.execute_command("TS.CREATE", source_key, "LABELS", "sensor", "temperature")

        # Create destination time series (compaction series)
        self.client.execute_command("TS.CREATE", dest_key, "LABELS", "type", "compaction")

        # Create compaction rule
        result = self.client.execute_command(
            "TS.CREATERULE", source_key, dest_key,
            "AGGREGATION", aggregation_type, bucket_duration
        )
        assert result == b'OK'

    def test_create_notifications(self):
        """Test that TS.CREATE generates appropriate keyspace notifications."""

        self.create_subscribe_clients()

        # Clear any existing notifications
        self.collect_notifications(timeout=0.5)

        # Execute TS.CREATE
        key = "ts:notifications:create"
        result = self.client.execute_command("TS.CREATE", key, "LABELS", "key", key, "sensor", "temp")
        assert result == b'OK'

        # Collect notifications
        notifications = self.collect_notifications()

        # Verify we received the expected notifications
        assert_message(notifications, "ts.create", key)

    def test_alter_notification(self):
        """Test that TS.ALTER generates appropriate keyspace notifications."""

        self.create_subscribe_clients()

        # Create a time series
        key = "ts:notifications:alter"
        self.create_ts(key)

        # Clear any existing notifications
        self.collect_notifications(timeout=0.5)

        # Execute TS.ALTER
        result = self.client.execute_command("TS.ALTER", key, "LABELS", "new_label", "value")
        assert result == b'OK'

        # Collect notifications
        notifications = self.collect_notifications()

        # Verify we received the expected notifications
        assert_message(notifications, "ts.alter", key)

    def test_add_notifications(self):
        """Test that TS.ADD generates appropriate keyspace notifications."""

        self.create_subscribe_clients()

        # Create a time series
        key = "ts:notifications:add"
        self.create_ts(key)

        # Clear any existing notifications
        self.collect_notifications(timeout=0.5)

        # Execute TS.ADD
        timestamp = int(time.time() * 1000)
        self.client.execute_command("TS.ADD", key, timestamp, 42.0)

        # Collect notifications
        notifications = self.collect_notifications()

        # Verify we received the expected notifications
        assert_message(notifications, "ts.add", key)

    def test_madd_notifications(self):
        """Test that TS.MADD generates appropriate keyspace notifications."""

        self.create_subscribe_clients()

        # TS.MADD does not create series (RedisTimeSeries parity), so they have to
        # exist before the batch — otherwise every item is a per-item error and no
        # ts.add event is emitted at all.
        keys = ["ts:notifications:madd1", "ts:notifications:madd2"]
        for key in keys:
            self.create_ts(key)

        # Clear any existing notifications
        self.collect_notifications(timeout=0.5)

        result = self.client.execute_command("TS.MADD", keys[0], 1500, 10.0, keys[1], 2000, 20.0)
        assert isinstance(result, list) and len(result) == len(keys)

        # Collect notifications
        notifications = self.collect_notifications(timeout=1)

        # Verify we received the expected notifications
        for key in keys:
            assert_message(notifications, "ts.add", key)

    def test_incrby_notifications(self):
        """Test that TS.INCRBY generates appropriate keyspace notifications."""

        self.create_subscribe_clients()

        # Create a time series
        key = "ts:notifications:incrby"
        self.create_ts(key)

        # Clear any existing notifications
        self.collect_notifications(timeout=0.5)

        # Execute TS.INCRBY
        result = self.client.execute_command("TS.INCRBY", key, 10.0)
        assert isinstance(result, int)
        notifications = self.collect_notifications(timeout=0.5)
        assert_message(notifications, "ts.incrby", key)

    def test_decrby_notifications(self):
        """Test that TS.DECRBY generates appropriate keyspace notifications."""

        self.create_subscribe_clients()

        # Create a time series
        key = "ts:notifications:decrby"
        self.create_ts(key)

        # Clear any existing notifications
        self.collect_notifications(timeout=0.5)

        # Execute TS.DECRBY
        result = self.client.execute_command("TS.DECRBY", key, 5.0)
        assert isinstance(result, int)

        # Collect notifications
        notifications = self.collect_notifications(timeout=0.5)

        # Verify we received the expected notifications
        assert_message(notifications, "ts.decrby", key)

    def test_del_notifications(self):
        """Test that TS.DEL generates appropriate keyspace notifications."""

        self.create_subscribe_clients()

        # Create a time series
        key = "ts:notifications:del"
        self.create_ts(key)

        # Clear any existing notifications
        self.collect_notifications(timeout=0.5)

        # Execute TS.DEL
        result = self.client.execute_command("TS.DEL", key, 0, int(time.time() * 1000))
        assert result == 1

        # Collect notifications
        notifications = self.collect_notifications(timeout=0.5)

        # Verify we received the expected notifications
        assert_message(notifications, "ts.del", key)

    def test_createrule_notifications(self):
        """Test that TS.CREATERULE generates appropriate keyspace notifications."""

        self.create_subscribe_clients()

        # Create source and destination time series
        source_key = "ts:source:basic"
        dest_key = "ts:dest:basic"

        self.create_ts(source_key)
        self.create_ts(dest_key)

        # Clear any existing notifications
        self.collect_notifications(timeout=0.5)

        # Execute TS.CREATERULE
        result = self.client.execute_command(
            "TS.CREATERULE", source_key, dest_key,
            "AGGREGATION", "avg", "60000"
        )
        assert result == b'OK'

        # Collect notifications
        notifications = self.collect_notifications()

        # Verify we received the expected notifications
        assert_message(notifications, "ts.createrule:src", source_key)
        assert_message(notifications, "ts.createrule:dest", dest_key)

    def test_deleterule_notifications(self):
        """Test that TS.DELETERULE generates appropriate keyspace notifications."""

        self.create_subscribe_clients()

        # Create source and destination time series
        source_key = "ts:source:delrule"
        dest_key = "ts:dest:delrule"

        self.create_ts(source_key)
        self.create_ts(dest_key)

        # Create a rule to delete later
        self.client.execute_command(
            "TS.CREATERULE", source_key, dest_key,
            "AGGREGATION", "avg", "60000"
        )

        # Clear any existing notifications
        self.collect_notifications(timeout=0.5)

        # Execute TS.DELETERULE
        result = self.client.execute_command("TS.DELETERULE", source_key, dest_key)
        assert result == b'OK'

        # Collect notifications
        notifications = self.collect_notifications(timeout=1.0)

        # Verify we received the expected notifications
        assert_message(notifications, "ts.deleterule:src", source_key)
        assert_message(notifications, "ts.deleterule:dest", dest_key)

    def test_compaction_add_notifications(self):
        """Test that adding samples to parent series generates compaction notifications."""

        self.create_subscribe_clients()

        # Create source and destination time series with compaction rule
        source_key = "ts:parent:temp"
        dest_key = "ts:compaction:temp:avg"

        self.create_ts_with_compaction_rule(source_key, dest_key)

        # Clear any existing notifications
        self.collect_notifications(timeout=0.5)

        # Add samples to the parent series to trigger compaction
        base_timestamp = int(time.time() * 1000)

        # Add multiple samples within the same bucket to ensure aggregation occurs
        for i in range(3):
            timestamp = base_timestamp + (i * 1000)  # 1 second apart
            self.client.execute_command("TS.ADD", source_key, timestamp, 20.0 + i)

        # Add a sample in the next bucket to finalize the previous bucket
        next_bucket_timestamp = base_timestamp + 65000  # Move to next minute bucket
        self.client.execute_command("TS.ADD", source_key, next_bucket_timestamp, 25.0)

        # Collect notifications
        notifications = self.collect_notifications(timeout=1.0)

        # Verify we received compaction notifications for the destination series
        compaction_notifications = [
            n for n in notifications
            if n['data'] == 'ts.add:dest' and n['channel'] == dest_key
        ]

        # Should have at least one compaction notification when bucket is finalized
        assert len(
            compaction_notifications) >= 1, f"Expected compaction notification for '{dest_key}', got: {notifications}"

    def create_subscribe_clients(self):
        self.keyspace_client = self.server.get_new_client()
        self.keyspace_client_subscribe = self.keyspace_client.pubsub()
        self.keyspace_client_subscribe.psubscribe('__key*__:*')
        self.keyspace_client.execute_command('CONFIG', 'SET', 'notify-keyspace-events', 'KEA')
        return self.keyspace_client_subscribe


def wait_for_message(pubsub, timeout=0.5, ignore_subscribe_messages=False, node=None, func=None):
    now = time.time()
    timeout = now + timeout
    while now < timeout:
        if node:
            message = pubsub.get_sharded_message(
                ignore_subscribe_messages=ignore_subscribe_messages, target_node=node
            )
        elif func:
            message = func(ignore_subscribe_messages=ignore_subscribe_messages)
        else:
            message = pubsub.get_message(
                ignore_subscribe_messages=ignore_subscribe_messages
            )
        if message is not None:
            return message
        time.sleep(0.01)
        now = time.time()
    return None


def assert_message(notifications, cmd, key):
    """Check if the message matches the expected structure."""
    for notification in notifications:
        if notification['data'] == cmd and notification['channel'] == key:
            return
    assert False, f"Expected message with cmd '{cmd}' and key '{key}' not found in notifications: {notifications}"
