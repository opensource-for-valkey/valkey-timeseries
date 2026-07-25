"""
Integration tests for Atomic Slot Migration (ASM) index consistency.

Tests verify that the timeseries index is properly maintained during slot migrations:
1. All timeseries are removed from the source index and cannot be queried
2. Source replica nodes are consistent with the primary node after migration 
3. The migrated series are visible in the target shard after migration
4. Indexes on target replicas are consistent with the target primary

Reference: https://valkey.io/topics/atomic-slot-migration/

These tests require Valkey >= 9.0 (ASM minimum version).
"""

import logging
import time
import pytest
from valkey import Valkey, ValkeyCluster
from valkeytestframework.util.waiters import wait_for_true, WaitTimeout
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesClusterTestCaseDebugMode

logger = logging.getLogger(__name__)

# Ceiling for polling the async post-migration index catch-up (the background thread that drains
# delayed-indexing keys after an ImportCompleted/ExportCompleted event). Polling returns as soon
# as the condition is met, so this only bounds the failure case rather than adding to happy-path
# runtime the way the fixed sleeps it replaces did.
INDEX_CATCHUP_TIMEOUT = 15


def get_server_version(client: Valkey) -> tuple:
    """Extract the server version as (major, minor, patch) from INFO server."""
    info = client.info("server")  # type: ignore[union-attr]
    version_str = info.get("valkey_version") or info.get("redis_version", "0.0.0")  # type: ignore[union-attr]
    parts = version_str.split(".")
    return tuple(int(p.split("-")[0]) for p in parts[:3])


class TestAtomicSlotMigration(ValkeyTimeSeriesClusterTestCaseDebugMode):
    """Test Atomic Slot Migration index consistency.

    Runs with debug-mode enabled: these tests use TS._DEBUG QUERYINDEX to inspect a single
    node's local index, and that command surface is gated on debug-mode.
    """

    # Use 3 shards with 1 replica each for comprehensive migration testing
    CLUSTER_SIZE = 3
    REPLICAS_COUNT = 1

    def _skip_if_asm_not_supported(self, client: Valkey):
        """Skip test if server version < 9.0 (ASM minimum version)."""
        version = get_server_version(client)
        if version < (9, 0, 0):
            pytest.skip(f"ASM requires Valkey >= 9.0, found {'.'.join(map(str, version))}")

    def _skip_if_asm_commands_not_supported(self, client: Valkey):
        """Skip when this server binary does not expose ASM cluster migration commands."""
        help_reply = client.execute_command("CLUSTER", "HELP")  # type: ignore[union-attr]
        lines = []
        for item in help_reply:  # type: ignore[union-attr]
            if isinstance(item, bytes):
                lines.append(item.decode("utf-8"))
            else:
                lines.append(str(item))
        joined = "\n".join(lines).upper()
        has_migrate_cmd = "MIGRATESLOTS" in joined
        has_getslotmigrations = "GETSLOTMIGRATIONS" in joined
        if not has_migrate_cmd or not has_getslotmigrations:
            pytest.skip("ASM migration command not available in this server build")

    def _get_node_id(self, client: Valkey) -> str:
        """Get the cluster node ID for a client."""
        node_id = client.execute_command("CLUSTER MYID")  # type: ignore[union-attr]
        return node_id.decode("utf-8") if isinstance(node_id, bytes) else node_id  # type: ignore[union-attr,return-type]

    def _get_key_slot(self, client: Valkey, key: str) -> int:
        """Get the hash slot for a key."""
        slot = client.execute_command("CLUSTER KEYSLOT", key)  # type: ignore[union-attr]
        return int(slot)  # type: ignore[arg-type]

    def _shard_index_for_slot(self, slot: int) -> int:
        """Determine which shard owns a given slot."""
        ranges = self._split_range_pairs(0, 16384, self.CLUSTER_SIZE)
        for idx, (start, end) in enumerate(ranges):
            if start <= slot < end:
                return idx
        raise ValueError(f"Slot {slot} not found in any shard range")

    # Terminal job states reported by CLUSTER GETSLOTMIGRATIONS (see
    # slotMigrationJobStateToString in the server). A job in any of these states is no longer
    # in progress.
    TERMINAL_MIGRATION_STATES = {"success", "cancelled", "failed"}

    @staticmethod
    def _migration_state(migration: dict):
        state = migration.get("state") or migration.get(b"state")
        if isinstance(state, bytes):
            state = state.decode("utf-8")
        return str(state) if state is not None else None

    def _wait_for_no_migrations(self, client: Valkey, timeout: int = 30):
        """Wait until every slot migration job on this node has reached a terminal state.

        The server retains finished jobs in CLUSTER GETSLOTMIGRATIONS as an operator log (trimmed
        only once it exceeds cluster-slot-migration-log-max-len, default 1000), so waiting for an
        *empty* reply never succeeds. Instead we wait until no job is still in progress, i.e. every
        job is in a terminal state (success/failed/cancelled).
        """
        deadline = time.time() + timeout
        last_states = None

        while time.time() < deadline:
            try:
                migrations = self._get_slot_migrations(client)
            except Exception as e:
                logger.warning(f"Error checking migrations: {e}")
                time.sleep(1)
                continue

            states = [self._migration_state(m) for m in migrations]
            in_progress = [s for s in states if s not in self.TERMINAL_MIGRATION_STATES]
            if not in_progress:
                return

            if states != last_states:
                logger.warning(f"Waiting for migrations to reach terminal state; states={states}")
                last_states = states

            time.sleep(1)

        raise TimeoutError(
            f"Timed out waiting for migrations to complete; last_states={last_states}"
        )

    def _poll_until(self, check, timeout: int = INDEX_CATCHUP_TIMEOUT):
        """Poll `check` (a zero-arg callable) until it returns truthy or `timeout` elapses.

        Swallows the timeout so callers can fall through to one final direct check and raise
        their own, more informative assertion message instead of a generic WaitTimeout.
        """
        try:
            wait_for_true(check, timeout=timeout)
        except WaitTimeout:
            pass

    def _assert_dbsize_zero(self, client: Valkey, message: str):
        """Assert a node's keyspace is empty, polling first.

        Migration-driven deletes (e.g. the source's post-export cleanup) can lag slightly behind
        the migration job reaching a terminal state.
        """
        self._poll_until(lambda: client.execute_command("DBSIZE") == 0)  # type: ignore[union-attr]
        actual = client.execute_command("DBSIZE")  # type: ignore[union-attr]
        assert actual == 0, f"{message} (actual DBSIZE={actual})"

    def _assert_queryindex_empty(self, client: Valkey, label_filter: str):
        """Assert TS.QUERYINDEX returns an empty list for the given filter."""
        self._poll_until(lambda: client.execute_command("TS.QUERYINDEX", label_filter) == [])  # type: ignore[union-attr]
        result = client.execute_command("TS.QUERYINDEX", label_filter)  # type: ignore[union-attr]
        assert result == [], f"Expected empty queryindex result, got {result}"

    def _assert_queryindex_contains(self, client: Valkey, label_filter: str, expected_keys: list):
        """Assert TS.QUERYINDEX returns exactly the expected keys.

        Polls first: TS.QUERYINDEX reflects the cluster-wide index, which catches up to a
        migration asynchronously (see INDEX_CATCHUP_TIMEOUT).
        """
        expected_set = set(k.encode() if isinstance(k, str) else k for k in expected_keys)
        self._poll_until(lambda: set(client.execute_command("TS.QUERYINDEX", label_filter)) == expected_set)  # type: ignore[union-attr]
        result = client.execute_command("TS.QUERYINDEX", label_filter)  # type: ignore[union-attr]
        result_set = set(result)  # type: ignore[arg-type]
        assert result_set == expected_set, f"Expected {expected_set}, got {result_set}"

    def _local_queryindex(self, client: Valkey, label_filter: str) -> set:
        """Query only the given node's local index (no cluster fanout).

        TS.QUERYINDEX fans out to all shards, so it cannot observe a single node's local index
        state (e.g. that a source node cleared its index after a migration, since the destination
        still holds the keys). TS._DEBUG QUERYINDEX runs against the local index only.
        """
        result = client.execute_command("TS._DEBUG", "QUERYINDEX", label_filter)  # type: ignore[union-attr]
        return set(result)  # type: ignore[arg-type]

    def _assert_local_queryindex_empty(self, client: Valkey, label_filter: str):
        """Assert a node's local index has no series matching the filter.

        Polls first: local index catch-up after a migration runs on a background thread (see
        INDEX_CATCHUP_TIMEOUT).
        """
        self._poll_until(lambda: self._local_queryindex(client, label_filter) == set())
        result = self._local_queryindex(client, label_filter)
        assert result == set(), f"Expected empty local queryindex result, got {result}"

    def _assert_local_queryindex_contains(self, client: Valkey, label_filter: str, expected_keys: list):
        """Assert a node's local index contains exactly the expected keys.

        Polls first: local index catch-up after a migration runs on a background thread (see
        INDEX_CATCHUP_TIMEOUT).
        """
        expected_set = set(k.encode() if isinstance(k, str) else k for k in expected_keys)
        self._poll_until(lambda: self._local_queryindex(client, label_filter) == expected_set)
        result = self._local_queryindex(client, label_filter)
        assert result == expected_set, f"Expected {expected_set}, got {result}"

    def _create_series_in_slot(self, cluster_client: ValkeyCluster, hash_tag: str, count: int = 5):
        """Create multiple time series with a common hashtag and migration label."""
        keys = []
        base_ts = 1000

        for i in range(count):
            # Wrap the hash tag in braces so the slot is determined by the tag (matching
            # _find_hash_tag_for_shard's probe), not by the whole key.
            key = f"ts:{{{hash_tag}}}:series{i}"
            keys.append(key)
            cluster_client.execute_command(
                "TS.CREATE", key,
                "LABELS", "migration", "yes", "series_id", str(i), "tag", hash_tag
            )

            # Add some sample data
            for j in range(10):
                cluster_client.execute_command(
                    "TS.ADD", key, base_ts + j * 100, 10.0 + i + j * 0.1
                )

        return keys

    def _migrate_slot(self, source_client: Valkey, target_node_id: str, slot: int, timeout: int = 30):
        """Perform slot migration and wait for completion."""
        logger.info(f"Migrating slot {slot} to node {target_node_id}")
        self._migrate_slot_range(source_client, target_node_id, slot, slot)

        # Wait for migration to complete. Background index catch-up (delayed-indexing drain,
        # post-migration cleanup) continues asynchronously past this point; callers poll for it
        # via the _assert_*queryindex* / _assert_dbsize_zero helpers rather than sleeping here.
        self._wait_for_no_migrations(source_client, timeout=timeout)

    def _get_slot_migrations(self, client: Valkey) -> list[dict]:
        """Normalize CLUSTER GETSLOTMIGRATIONS output into list[dict]."""
        raw = client.execute_command("CLUSTER", "GETSLOTMIGRATIONS")
        if not raw:
            return []

        def to_str(v):
            if isinstance(v, bytes):
                return v.decode("utf-8")
            return v

        # RESP3 map/list style from cluster clients
        if isinstance(raw, dict):
            values = raw.values()
            return [v for v in values if isinstance(v, dict)]

        # RESP2 flattened pairs, where each entry is an array [k1,v1,k2,v2,...]
        out: list[dict] = []
        if isinstance(raw, list):
            for entry in raw:
                if isinstance(entry, dict):
                    out.append(entry)
                    continue
                if not isinstance(entry, list):
                    continue
                m = {}
                for i in range(0, len(entry) - 1, 2):
                    k = to_str(entry[i])
                    m[k] = entry[i + 1]
                out.append(m)
        return out

    def _find_migration_job_for_slot(self, client: Valkey, slot: int):
        """Return the migration job name for a slot if present."""
        migrations = self._get_slot_migrations(client)
        slot_token = str(slot)
        slot_range_token = f"{slot}-{slot}"

        for migration in migrations:
            name = migration.get("name") or migration.get(b"name")
            slot_ranges = migration.get("slot_ranges") or migration.get(b"slot_ranges")
            if isinstance(slot_ranges, bytes):
                slot_ranges = slot_ranges.decode("utf-8")

            if not slot_ranges or not name:
                continue

            slot_ranges = str(slot_ranges)
            if slot_token in slot_ranges or slot_range_token in slot_ranges:
                return name.decode("utf-8") if isinstance(name, bytes) else str(name)

        return None

    def _migrate_slot_range(self, source_client: Valkey, target_node_id: str, start_slot: int, end_slot: int):
        """Start slot migration without waiting, with command compatibility fallback."""
        self._skip_if_asm_commands_not_supported(source_client)
        source_client.execute_command("CLUSTER", "MIGRATESLOTS", "SLOTSRANGE", start_slot, end_slot, "NODE", target_node_id)

    def _abort_migration(self, source_client: Valkey):
        """Abort a migration job.
        """
        source_client.execute_command("CLUSTER", "CANCELSLOTMIGRATIONS")

    def test_source_index_cleared_after_migration(self):
        """Test that the source primary index is cleared after migration."""
        # Setup
        cluster_client = self.new_cluster_client()
        source_client = self.new_client_for_primary(0)
        self._skip_if_asm_not_supported(source_client)

        # Create test data with controlled slot placement
        hash_tag = self._find_hash_tag_for_shard(source_client, "asm_test_src", shard_index=0)
        keys = self._create_series_in_slot(cluster_client, hash_tag, count=5)

        # Determine slot and validate it's in source shard
        slot = self._get_key_slot(source_client, keys[0])
        assert self._shard_index_for_slot(slot) == 0, "Keys not in source shard"

        # Verify keys are indexed on source before migration
        self._assert_queryindex_contains(source_client, "migration=yes", keys)

        # Get target node ID
        target_client = self.new_client_for_primary(1)
        target_node_id = self._get_node_id(target_client)

        # Perform migration
        self._migrate_slot(source_client, target_node_id, slot)

        # Assert: the source's *local* index no longer contains the migrated series. TS.QUERYINDEX
        # fans out cluster-wide and would still observe the keys on the destination, so we must
        # query the source's local index directly.
        self._assert_local_queryindex_empty(source_client, "migration=yes")

        # The source no longer owns the slot, so its keyspace should be empty (a plain EXISTS would
        # be answered with a MOVED redirect, so we check the node-local DBSIZE instead).
        self._assert_dbsize_zero(source_client, "Source keyspace should be empty after migration")

    def test_source_replicas_consistent_after_migration(self):
        """Test that source replicas are consistent with primary after migration."""
        # Setup
        cluster_client = self.new_cluster_client()
        source_rg = self.get_replication_group(0)
        source_primary_client = source_rg.get_primary_connection()
        self._skip_if_asm_not_supported(source_primary_client)

        # Create test data
        hash_tag = self._find_hash_tag_for_shard(source_primary_client, "asm_test_replica", shard_index=0)
        keys = self._create_series_in_slot(cluster_client, hash_tag, count=5)

        slot = self._get_key_slot(source_primary_client, keys[0])
        assert self._shard_index_for_slot(slot) == 0, "Keys not in source shard"

        # Wait for initial replication
        source_rg.wait_for_replica_offset_to_sync_up(0)

        # Verify keys are on replica before migration. A replica only serves reads for its shard's
        # slots after READONLY is enabled on the connection.
        source_replica_client = source_rg.get_replica_connection(0)
        source_replica_client.execute_command("READONLY")
        for key in keys:
            exists = source_replica_client.execute_command("EXISTS", key)
            assert exists == 1, f"Key {key} not replicated to source replica"

        # Perform migration
        target_client = self.new_client_for_primary(1)
        target_node_id = self._get_node_id(target_client)
        self._migrate_slot(source_primary_client, target_node_id, slot)

        # Wait for DEL commands to replicate to source replicas
        source_rg.wait_for_replica_offset_to_sync_up(0)

        # Assert: the source replica's local index should also be empty (the DELs propagated from
        # the primary drive the replica's index cleanup). Polls internally for the replica's
        # index cleanup to catch up with the replicated DELs.
        self._assert_local_queryindex_empty(source_replica_client, "migration=yes")

        # The replica no longer owns the slot, so its keyspace should be empty.
        self._assert_dbsize_zero(
            source_replica_client, "Source replica keyspace should be empty after migration"
        )

    def test_target_index_populated_after_migration(self):
        """Test that the target primary index is populated after migration."""
        # Setup
        cluster_client = self.new_cluster_client()
        source_client = self.new_client_for_primary(0)
        self._skip_if_asm_not_supported(source_client)

        # Create test data
        hash_tag = self._find_hash_tag_for_shard(source_client, "asm_test_target", shard_index=0)
        keys = self._create_series_in_slot(cluster_client, hash_tag, count=5)

        slot = self._get_key_slot(source_client, keys[0])

        # Perform migration
        target_client = self.new_client_for_primary(1)
        target_node_id = self._get_node_id(target_client)
        self._migrate_slot(source_client, target_node_id, slot)

        # Assert: target index should contain all migrated keys
        self._assert_queryindex_contains(target_client, "migration=yes", keys)

        # Verify keys exist on target with correct data
        for key in keys:
            exists = target_client.execute_command("EXISTS", key)
            assert exists == 1, f"Key {key} not found on target after migration"

            # Verify data integrity
            result = target_client.execute_command("TS.RANGE", key, "-", "+")  # type: ignore[union-attr]
            assert len(result) == 10, f"Expected 10 samples in {key}, got {len(result)}"  # type: ignore[arg-type]

        # Verify MRANGE works across the migrated keys
        mrange_result = target_client.execute_command(  # type: ignore[union-attr]
            "TS.MRANGE", "-", "+", "FILTER", "migration=yes"
        )
        assert len(mrange_result) == 5, f"Expected 5 series in MRANGE, got {len(mrange_result)}"  # type: ignore[arg-type]

    def test_target_replicas_index_consistent(self):
        """Test that target replica indexes are consistent with primary after migration."""
        # Setup
        cluster_client = self.new_cluster_client()
        source_client = self.new_client_for_primary(0)
        self._skip_if_asm_not_supported(source_client)

        # Create test data
        hash_tag = self._find_hash_tag_for_shard(source_client, "asm_test_target_replica", shard_index=0)
        keys = self._create_series_in_slot(cluster_client, hash_tag, count=5)

        slot = self._get_key_slot(source_client, keys[0])

        # Perform migration to shard 1
        target_rg = self.get_replication_group(1)
        target_primary_client = target_rg.get_primary_connection()
        target_node_id = self._get_node_id(target_primary_client)
        self._migrate_slot(source_client, target_node_id, slot)

        # Wait for replication to target replicas
        target_rg.wait_for_replica_offset_to_sync_up(0)

        # Compare the primary's and replica's *local* indexes (TS.QUERYINDEX fans out, so it would
        # return the same cluster-wide set from either node and could not detect a replica-local
        # inconsistency).
        target_replica_client = target_rg.get_replica_connection(0)
        target_replica_client.execute_command("READONLY")

        expected_keys = set(k.encode() for k in keys)

        # Poll: the primary's delayed-indexing drain and the replica's own index catch-up (driven
        # by the replicated writes) both continue asynchronously past the migration completing.
        self._poll_until(
            lambda: self._local_queryindex(target_primary_client, "migration=yes") == expected_keys
            and self._local_queryindex(target_replica_client, "migration=yes") == expected_keys
        )

        primary_keys = self._local_queryindex(target_primary_client, "migration=yes")
        replica_keys = self._local_queryindex(target_replica_client, "migration=yes")

        assert replica_keys == primary_keys, \
            f"Target replica index mismatch. Primary: {primary_keys}, Replica: {replica_keys}"
        assert replica_keys == expected_keys, \
            f"Target replica index missing migrated keys: {replica_keys}"

        # The replica's index having the full set of migrated keys (matching the primary) is the
        # consistency guarantee under test. Key-routed data reads against the replica are avoided
        # here because slot-ownership config propagates via the cluster bus and can lag the data
        # replication offset, making cross-node routed reads racy mid-reconfiguration; the target
        # primary's data integrity is already covered by test_target_index_populated_after_migration.

    def test_multiple_slots_migration(self):
        """Test migration of multiple series across different slots."""
        # Setup
        cluster_client = self.new_cluster_client()
        source_client = self.new_client_for_primary(0)
        self._skip_if_asm_not_supported(source_client)

        # Create series in multiple slots (all should be in shard 0)
        all_keys = []
        slots_created = set()

        for i in range(3):
            hash_tag = self._find_hash_tag_for_shard(source_client, f"asm_multi_{i}", shard_index=0)
            keys = self._create_series_in_slot(cluster_client, hash_tag, count=3)
            all_keys.extend(keys)

            slot = self._get_key_slot(source_client, keys[0])
            slots_created.add(slot)
            assert self._shard_index_for_slot(slot) == 0, f"Keys not in source shard for tag {hash_tag}"

        logger.info(f"Created series in slots: {slots_created}")

        # Verify all keys indexed on source
        source_result = source_client.execute_command("TS.QUERYINDEX", "migration=yes")  # type: ignore[union-attr]
        assert len(source_result) == 9, f"Expected 9 keys on source, got {len(source_result)}"  # type: ignore[arg-type]

        # Migrate all slots to target
        target_client = self.new_client_for_primary(1)
        target_node_id = self._get_node_id(target_client)

        for slot in sorted(slots_created):
            self._migrate_slot(source_client, target_node_id, slot)

        # Assert: source's local index should have no migration=yes keys.
        self._assert_local_queryindex_empty(source_client, "migration=yes")

        # Assert: target should have all keys (cluster-wide view).
        self._assert_queryindex_contains(target_client, "migration=yes", all_keys)

    def test_migration_with_label_filtering(self):
        """Test that label filtering works correctly after migration."""
        # Setup
        cluster_client = self.new_cluster_client()
        source_client = self.new_client_for_primary(0)
        self._skip_if_asm_not_supported(source_client)

        # Create series with different labels in same slot
        hash_tag = self._find_hash_tag_for_shard(source_client, "asm_labels", shard_index=0)
        keys_migrated = []
        keys_other = []

        for i in range(3):
            key_migrated = f"ts:{{{hash_tag}}}:mig{i}"
            keys_migrated.append(key_migrated)
            cluster_client.execute_command(
                "TS.CREATE", key_migrated,
                "LABELS", "migration", "yes", "type", "migrated"
            )

            key_other = f"ts:{{{hash_tag}}}:other{i}"
            keys_other.append(key_other)
            cluster_client.execute_command(
                "TS.CREATE", key_other,
                "LABELS", "migration", "no", "type", "other"
            )

        slot = self._get_key_slot(source_client, keys_migrated[0])

        # Migrate the slot
        target_client = self.new_client_for_primary(1)
        target_node_id = self._get_node_id(target_client)
        self._migrate_slot(source_client, target_node_id, slot)

        # Assert: filtering by migration=yes on target returns only migrated keys
        self._assert_queryindex_contains(target_client, "migration=yes", keys_migrated)

        # Assert: filtering by migration=no on target returns other keys
        self._assert_queryindex_contains(target_client, "migration=no", keys_other)

        # Assert: source's local index has no keys from this slot.
        self._assert_local_queryindex_empty(source_client, "migration=yes")
        self._assert_local_queryindex_empty(source_client, "migration=no")

    def test_mget_after_migration(self):
        """Test TS.MGET works correctly after migration."""
        # Setup
        cluster_client = self.new_cluster_client()
        source_client = self.new_client_for_primary(0)
        self._skip_if_asm_not_supported(source_client)

        # Create test data with latest values
        hash_tag = self._find_hash_tag_for_shard(source_client, "asm_mget", shard_index=0)
        keys = self._create_series_in_slot(cluster_client, hash_tag, count=3)

        # Add a recent sample to each
        latest_ts = 10000
        for i, key in enumerate(keys):
            cluster_client.execute_command("TS.ADD", key, latest_ts, 100.0 + i)

        slot = self._get_key_slot(source_client, keys[0])

        # Migrate
        target_client = self.new_client_for_primary(1)
        target_node_id = self._get_node_id(target_client)
        self._migrate_slot(source_client, target_node_id, slot)

        # Assert: MGET on target returns correct latest values. TS.MGET's FILTER resolves through
        # the same index as TS.QUERYINDEX, so it lags the migration the same way; poll for it.
        def mget():
            return target_client.execute_command("TS.MGET", "FILTER", "migration=yes")  # type: ignore[union-attr]

        self._poll_until(lambda: len(mget()) == 3)  # type: ignore[arg-type]
        result = mget()
        assert len(result) == 3, f"Expected 3 results from MGET, got {len(result)}"  # type: ignore[arg-type]

        for series_result in result:  # type: ignore[union-attr]
            key = series_result[0]
            labels = series_result[1]
            latest = series_result[2]

            # Verify latest timestamp and value
            assert latest[0] == latest_ts, f"Wrong timestamp for {key}"
            assert 100.0 <= float(latest[1]) < 103.0, f"Wrong value for {key}"

    def test_import_aborted_clears_delayed_keys(self):
        """Exercise ImportAborted and verify target index stays clean after abort."""
        cluster_client = self.new_cluster_client()
        source_client = self.new_client_for_primary(0)
        self._skip_if_asm_not_supported(source_client)

        target_client = self.new_client_for_primary(1)
        target_node_id = self._get_node_id(target_client)

        # Use enough keys to keep migration in-flight long enough to abort.
        hash_tag = self._find_hash_tag_for_shard(source_client, "asm_abort", shard_index=0)
        keys = self._create_series_in_slot(cluster_client, hash_tag, count=128)

        slot = self._get_key_slot(source_client, keys[0])
        assert self._shard_index_for_slot(slot) == 0, "Keys not in source shard"

        # Start migration and wait until job appears, then abort.
        self._migrate_slot_range(source_client, target_node_id, slot, slot)

        wait_for_true(
            lambda: self._find_migration_job_for_slot(target_client, slot) is not None,
            timeout=20,
        )
        job_name = self._find_migration_job_for_slot(target_client, slot)
        assert job_name is not None, "Migration job not found on target"

        self._abort_migration(source_client)

        self._wait_for_no_migrations(source_client, timeout=30)
        self._wait_for_no_migrations(target_client, timeout=30)

        # ImportAborted path should clear delayed keys and keep the target's local index clean.
        # (A fanned-out TS.QUERYINDEX would still see the keys on the source, which correctly keeps
        # them after an aborted import, so we query the target's local index directly.) Polls
        # internally for the abort's delayed-key cleanup to finish.
        self._assert_local_queryindex_empty(target_client, f"tag={hash_tag}")
        self._assert_local_queryindex_empty(target_client, "migration=yes")

        # The aborted import should leave no keys in the target's keyspace.
        self._assert_dbsize_zero(target_client, "Target keyspace should be empty after aborted import")

    def _find_hash_tag_for_shard(self, client: Valkey, prefix: str, shard_index: int, max_attempts: int = 512) -> str:
        """Find a hash tag whose slot is owned by the requested shard."""
        for i in range(max_attempts):
            candidate = f"{prefix}_{i}"
            probe_key = f"ts:{{{candidate}}}:probe"
            slot = self._get_key_slot(client, probe_key)
            if self._shard_index_for_slot(slot) == shard_index:
                return candidate
        raise AssertionError(f"Failed to find hash tag for shard {shard_index} after {max_attempts} attempts")
