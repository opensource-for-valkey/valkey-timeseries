"""Integration tests for postings-index RDB aux persistence.

Covers the aux payload save/load cycle, the ts-index-persist config gate, the dangling-id
reconciliation sweep and count-verification repair scan, and the AOF/replication interactions
called out in that document's "Interaction with other flows" section.
"""
import os

import pytest
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util.waiters import wait_for_equal, wait_for_true, TEST_MAX_WAIT_TIME_SECONDS
from valkeytestframework.valkey_test_case import ReplicationTestCase

from common import SERVER_PATH, get_module_path
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


PRELOADED_LOG = "Preloaded postings index"
DISABLED_LOG = "ts-index-persist is disabled; discarding persisted postings index and rebuilding"
DANGLING_LOG = "dangling ids marked stale"
REPAIR_LOG = "scanning keyspace to repair"
SKIP_SWEEP_LOG = "skipping reconciliation sweep"
RECONCILE_LOG = "does not match the loaded set"


def wait_for_log(server, needle, timeout=TEST_MAX_WAIT_TIME_SECONDS):
    """The reconciliation sweep runs on the module thread pool after loading ends
    (`reconcile_preloaded_indexes` -> `threads::spawn`), so its log lines can land after
    `is_rdb_done_loading()` is already true. Poll rather than checking once."""
    wait_for_true(lambda: server.verify_string_in_logfile(needle), timeout=timeout)


def wait_for_sweep_decision(server, timeout=TEST_MAX_WAIT_TIME_SECONDS):
    """Wait until the sweep has logged its per-db verdict — either the digest fast path or
    the reconcile branch. Only meaningful for a db that actually preloaded; with no aux
    payload the sweep never runs and nothing is logged."""
    wait_for_true(
        lambda: server.verify_string_in_logfile(SKIP_SWEEP_LOG)
        or server.verify_string_in_logfile(RECONCILE_LOG),
        timeout=timeout,
    )


class TestIndexPersistenceBasic(ValkeyTimeSeriesTestCaseBase):

    def _create_series(self, client, count, prefix="ts"):
        """Create `count` series with a mix of shared and per-series labels."""
        keys = []
        for i in range(count):
            key = f"{prefix}:{i}"
            keys.append(key)
            client.execute_command(
                "TS.CREATE", key,
                "LABELS", "service", "api", "shard", str(i % 3), "instance", str(i),
            )
            client.execute_command("TS.ADD", key, 1000 + i, float(i))
        return keys

    def test_index_preloaded_after_restart(self):
        """Happy path: aux payload preloads the index and reconciliation finds no drift."""
        client = self.server.get_new_client()
        keys = self._create_series(client, 10)

        pre_card = client.execute_command("TS.CARD", "FILTER", "service=api")
        pre_shard0 = sorted(client.execute_command("TS.QUERYINDEX", "shard=0"))
        assert pre_card == 10

        client.bgsave()
        self.server.wait_for_save_done()
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()
        wait_for_equal(lambda: self.server.is_rdb_done_loading(), True)

        assert self.server.verify_string_in_logfile(PRELOADED_LOG)
        # On a clean load the per-(id, key_name) digests agree, so the O(N) keyspace-probing
        # sweep is skipped entirely and nothing needs repairing.
        wait_for_log(self.server, SKIP_SWEEP_LOG)
        assert not self.server.verify_string_in_logfile(DANGLING_LOG)
        assert not self.server.verify_string_in_logfile(REPAIR_LOG)

        post_card = client.execute_command("TS.CARD", "FILTER", "service=api")
        post_shard0 = sorted(client.execute_command("TS.QUERYINDEX", "shard=0"))
        assert post_card == pre_card
        assert post_shard0 == pre_shard0
        for key in keys:
            assert client.execute_command("EXISTS", key) == 1

    def test_index_preloaded_with_many_series_and_labels(self):
        """Larger/more varied index still preloads to an exactly correct state."""
        client = self.server.get_new_client()
        keys = self._create_series(client, 60)

        pre_card_total = client.execute_command("TS.CARD")
        pre_per_shard = {
            s: sorted(client.execute_command("TS.QUERYINDEX", f"shard={s}"))
            for s in ("0", "1", "2")
        }

        client.bgsave()
        self.server.wait_for_save_done()
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()
        wait_for_equal(lambda: self.server.is_rdb_done_loading(), True)
        assert self.server.verify_string_in_logfile(PRELOADED_LOG)

        assert client.execute_command("TS.CARD") == pre_card_total
        for s in ("0", "1", "2"):
            assert sorted(client.execute_command("TS.QUERYINDEX", f"shard={s}")) == pre_per_shard[s]
        for key in keys:
            assert client.execute_command("EXISTS", key) == 1

    def test_flushall_then_restart_yields_empty_index(self):
        """An empty db must not leave a stale/garbage aux payload behind."""
        client = self.server.get_new_client()
        self._create_series(client, 5)
        client.flushall()
        assert client.execute_command("DBSIZE") == 0

        client.bgsave()
        self.server.wait_for_save_done()
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()
        wait_for_equal(lambda: self.server.is_rdb_done_loading(), True)

        # Nothing to preload: build_aux_payload writes no aux field when every db is empty.
        assert not self.server.verify_string_in_logfile(PRELOADED_LOG)
        assert client.execute_command("DBSIZE") == 0
        assert client.execute_command("TS.CARD") == 0

        # And the server still works normally afterward.
        client.execute_command("TS.CREATE", "ts:after", "LABELS", "service", "api")
        assert client.execute_command("TS.CARD", "FILTER", "service=api") == 1

    def test_ts_index_persist_disabled_falls_back_to_rebuild(self):
        """With the feature off at load time, a real aux payload is read (to keep the RDB
        stream in sync) and discarded, falling back to the per-key rebuild.

        `ts-index-persist` is a module-registered config: this build's CLI/config-file
        parser resolves module directives before modules load, so it cannot be set via
        `--ts-index-persist` at process startup outside a real config file with
        `loadmodule` preceding it. `CONFIG SET` + `DEBUG RELOAD NOSAVE` sidesteps that:
        it flips the config at runtime and reloads the *existing* RDB (written earlier,
        while the feature was on) without re-saving it, giving the load path a genuine
        aux payload to discard.
        """
        client = self.server.get_new_client()
        keys = self._create_series(client, 8)
        pre_card = client.execute_command("TS.CARD", "FILTER", "service=api")

        # Written with ts-index-persist on (default): the RDB now carries a real aux payload.
        client.bgsave()
        self.server.wait_for_save_done()

        # The module namespaces its registered configs as "ts.<name>".
        client.config_set("ts.ts-index-persist", "no")
        client.execute_command("DEBUG", "RELOAD", "NOSAVE")

        assert self.server.verify_string_in_logfile(DISABLED_LOG)
        assert not self.server.verify_string_in_logfile(PRELOADED_LOG)

        # Rebuilt via the per-key path: still fully correct.
        assert client.execute_command("TS.CARD", "FILTER", "service=api") == pre_card
        for key in keys:
            assert client.execute_command("EXISTS", key) == 1

    def test_deleted_series_do_not_reappear_after_restart(self):
        """Deleted series are gone from id_to_key at save time, so they cannot leak back in."""
        client = self.server.get_new_client()
        keys = self._create_series(client, 10)
        to_delete = keys[2:5]
        for key in to_delete:
            client.execute_command("DEL", key)

        client.bgsave()
        self.server.wait_for_save_done()
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()
        wait_for_equal(lambda: self.server.is_rdb_done_loading(), True)

        assert self.server.verify_string_in_logfile(PRELOADED_LOG)
        wait_for_sweep_decision(self.server)
        assert not self.server.verify_string_in_logfile(REPAIR_LOG)
        assert client.execute_command("TS.CARD") == len(keys) - len(to_delete)
        for key in to_delete:
            assert client.execute_command("EXISTS", key) == 0
        for key in keys:
            if key not in to_delete:
                assert client.execute_command("EXISTS", key) == 1


class TestIndexPersistenceDanglingRepair(ValkeyTimeSeriesTestCaseBase):
    """Exercises the case where the aux payload names an id whose key does not load
    (RDB body corruption / a fork landing mid-write). The digest must force a
    reconciliation sweep, the dangling id must be dropped from the index, and the
    count-verification scan must repair it so the renamed key becomes findable under its
    real name."""

    @property
    def rdb_path(self):
        return os.path.join(self.server.cwd, self.server.args["dbfilename"])

    def _rename_key_in_rdb_body(self, old_name: bytes, new_name: bytes):
        """Flip a key's literal bytes in the on-disk RDB, in place (same length only).

        The aux payload is LZF-compressed by the engine, so a literal key name should
        appear exactly once in the file: in the RDB body's key-value section. This
        simulates the aux payload and the keyspace disagreeing about a key's name,
        without touching the (compressed, harder to hand-edit) aux blob itself.
        """
        assert len(old_name) == len(new_name), "in-place edit requires equal-length names"
        with open(self.rdb_path, "rb") as f:
            data = bytearray(f.read())
        idx = data.find(old_name)
        assert idx != -1, f"key name {old_name!r} not found in rdb file"
        assert data.find(old_name, idx + 1) == -1, (
            f"key name {old_name!r} found more than once; test assumption violated"
        )
        data[idx:idx + len(old_name)] = new_name
        with open(self.rdb_path, "wb") as f:
            f.write(bytes(data))

    def test_dangling_id_triggers_reconciliation_and_repair_scan(self):
        # rdbchecksum is IMMUTABLE_CONFIG (can't CONFIG SET at runtime); disable it from
        # process start so the hand-edited RDB below isn't rejected by checksum verification
        # before our aux/per-key logic ever runs. The dataset is still empty at this point.
        self.server.args["rdbchecksum"] = "no"
        self.server.restart(remove_rdb=True, remove_nodes_conf=True, connect_client=True)
        assert self.server.is_alive()
        client = self.server.get_new_client()

        keys = [f"ts:{i}" for i in range(7)]
        for i, key in enumerate(keys):
            client.execute_command("TS.CREATE", key, "LABELS", "region", f"us-east-{i}", "service", "api")

        client.bgsave()
        self.server.wait_for_save_done()
        self.server.exit(cleanup=False, remove_nodes_conf=False)

        old_name, new_name = b"ts:4", b"tz:4"
        self._rename_key_in_rdb_body(old_name, new_name)

        self.server.start(connect_client=True)
        assert self.server.is_alive()
        wait_for_equal(lambda: self.server.is_rdb_done_loading(), True)

        assert self.server.verify_string_in_logfile(PRELOADED_LOG)
        # The sweep is asynchronous; wait for its output rather than sampling the log once.
        wait_for_log(self.server, RECONCILE_LOG)
        wait_for_log(self.server, REPAIR_LOG)
        # The renamed key leaves the cardinalities equal (7 loaded, 7 indexed), so a count-only
        # check would wave this through; the (id, key_name) digest is what forces the sweep.
        assert not self.server.verify_string_in_logfile(SKIP_SWEEP_LOG)
        # Deliberately not asserting DANGLING_LOG: the retention-trim cron
        # (`process_series_trim` -> `fetch_series_batch`) fires on the first cron tick after
        # loading ends, opens every indexed key too, and marks the same id stale by the same
        # mechanism. Both racers run on the module thread pool, so either can reach the
        # dangling id first; when the cron wins, `reconcile_db` finds nothing dangling and
        # logs only at debug level, while the index is already short one entry so the repair
        # scan still fires. The sweep's own stale-marking is covered deterministically by the
        # `reindex_revokes_stale_marking` unit test in src/series/index/persistence.rs.

        client = self.server.get_new_client()
        # Repair must converge to the correct final state regardless of GC timing.
        wait_for_equal(lambda: client.execute_command("TS.CARD", "FILTER", "service=api"), len(keys))

        assert client.execute_command("EXISTS", old_name.decode()) == 0
        assert client.execute_command("EXISTS", new_name.decode()) == 1
        # The dangling id must resolve under its real (renamed) key, not the stale aux name.
        assert client.execute_command("TS.QUERYINDEX", "region=us-east-4") == [new_name]

        for i, key in enumerate(keys):
            if key == old_name.decode():
                continue
            assert client.execute_command("EXISTS", key) == 1


class TestIndexPersistenceDebugReload(ValkeyTimeSeriesTestCaseBase):

    def test_debug_reload_preserves_index_correctness(self):
        """DEBUG RELOAD exercises aux save/load without a process restart. Freeing the
        pre-reload objects can race the aux payload's index install (asynchronous key
        frees during the engine's flush-before-load step), so the index can transiently
        under-count until the reconciliation/repair pass converges it. The end state
        must always be exactly correct."""
        client = self.server.get_new_client()
        keys = []
        for i in range(12):
            key = f"ts:{i}"
            keys.append(key)
            client.execute_command("TS.CREATE", key, "LABELS", "region", f"us-east-{i}", "service", "api")
            client.execute_command("TS.ADD", key, 1000 + i, float(i))

        pre_digest = client.execute_command("DEBUG", "DIGEST")
        pre_queryindex = sorted(client.execute_command("TS.QUERYINDEX", "service=api"))

        client.execute_command("DEBUG", "RELOAD")

        # The repair scan (if triggered) runs on a background thread; give it a moment
        # to converge rather than asserting instantaneous consistency.
        wait_for_equal(
            lambda: client.execute_command("TS.CARD", "FILTER", "service=api"),
            len(keys),
            timeout=TEST_MAX_WAIT_TIME_SECONDS,
        )

        post_digest = client.execute_command("DEBUG", "DIGEST")
        post_queryindex = sorted(client.execute_command("TS.QUERYINDEX", "service=api"))
        assert post_digest == pre_digest
        assert post_queryindex == pre_queryindex
        for key in keys:
            assert client.execute_command("EXISTS", key) == 1

    def test_debug_reload_multiple_cycles_stay_consistent(self):
        """Repeated reloads must not accumulate drift (stale ids, phantom entries)."""
        client = self.server.get_new_client()
        for i in range(8):
            client.execute_command(
                "TS.CREATE", f"ts:{i}", "LABELS", "region", f"us-east-{i}", "service", "api",
            )

        for _ in range(3):
            client.execute_command("DEBUG", "RELOAD")
            wait_for_equal(
                lambda: client.execute_command("TS.CARD", "FILTER", "service=api"),
                8,
                timeout=TEST_MAX_WAIT_TIME_SECONDS,
            )

        assert client.execute_command("TS.CARD") == 8
        assert len(client.execute_command("TS.QUERYINDEX", "service=api")) == 8


class TestIndexPersistenceAof(ValkeyTimeSeriesTestCaseBase):

    AOF_REWRITE_DONE_LOG = "Background AOF rewrite finished successfully"

    def _count_string_in_logfile(self, string):
        """Count occurrences rather than mere presence: with no samples in these series
        the rewrite is fast enough that the initial CONFIG SET appendonly yes triggers
        its own (empty-dataset) rewrite completing before our own bgrewriteaof() call
        even starts. Checking presence alone (or the shared harness's
        `wait_for_action_done`, which reads `aof_last_bgrewrite_status` without
        confirming it reflects *this* rewrite) can observe that earlier completion and
        proceed before our data-bearing rewrite has actually finished, so callers must
        wait for the count to increase past a baseline captured before triggering it."""
        path = os.path.join(self.server.cwd, self.server.args["logfile"])
        if not os.path.exists(path):
            return 0
        with open(path) as f:
            return sum(1 for line in f if string in line)

    def _bgrewriteaof_and_wait(self, client):
        finished_before = self._count_string_in_logfile(self.AOF_REWRITE_DONE_LOG)
        client.bgrewriteaof()
        wait_for_true(
            lambda: self._count_string_in_logfile(self.AOF_REWRITE_DONE_LOG) > finished_before
        )

    def test_aof_with_rdb_preamble_preloads_index(self):
        """Default AOF (RDB preamble on) carries the aux fields: restart preloads."""
        client = self.client
        client.config_set("appendonly", "yes")
        wait_for_equal(lambda: client.info("persistence")["aof_rewrite_in_progress"], 0, timeout=30)

        keys = []
        for i in range(9):
            key = f"ts:{i}"
            keys.append(key)
            client.execute_command("TS.CREATE", key, "LABELS", "service", "api", "idx", str(i))

        self._bgrewriteaof_and_wait(client)

        self.server.args["appendonly"] = "yes"
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()
        wait_for_true(lambda: self.server.verify_string_in_logfile("DB loaded from"))

        assert self.server.verify_string_in_logfile(PRELOADED_LOG)
        client = self.server.get_new_client()
        assert client.execute_command("TS.CARD", "FILTER", "service=api") == len(keys)
        for key in keys:
            assert client.execute_command("EXISTS", key) == 1

    def test_plain_aof_command_format_rebuilds_without_preload(self):
        """Command-format AOF (no RDB preamble) carries no aux field: per-key rebuild,
        as documented in docs/postings-index-persistence.md."""
        client = self.client
        client.config_set("appendonly", "yes")
        client.config_set("aof-use-rdb-preamble", "no")
        wait_for_equal(lambda: client.info("persistence")["aof_rewrite_in_progress"], 0, timeout=30)

        keys = []
        for i in range(6):
            key = f"ts:{i}"
            keys.append(key)
            client.execute_command("TS.CREATE", key, "LABELS", "service", "api", "idx", str(i))

        self._bgrewriteaof_and_wait(client)

        self.server.args["appendonly"] = "yes"
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()
        wait_for_true(lambda: self.server.verify_string_in_logfile("DB loaded from"))

        # No aux payload in a command-format AOF: preload never fires.
        assert not self.server.verify_string_in_logfile(PRELOADED_LOG)
        client = self.server.get_new_client()
        assert client.execute_command("TS.CARD", "FILTER", "service=api") == len(keys)
        for key in keys:
            assert client.execute_command("EXISTS", key) == 1


class TestIndexPersistenceReplication(ReplicationTestCase):
    """Full-sync replicas receive the primary's RDB (including aux fields) and should
    preload their index rather than rebuild it per key."""

    @pytest.fixture(autouse=True)
    def setup_test(self, setup):
        # get_module_path() rather than os.getenv: with MODULE_PATH unset the raw getenv
        # yields "--loadmodule None" and the server never comes up.
        self.args = {"enable-debug-command": "yes", "loadmodule": get_module_path()}
        self.server, self.client = self.create_server(
            testdir=self.testdir, server_path=SERVER_PATH, args=self.args
        )

    def test_full_sync_replica_preloads_index(self):
        client = self.client

        keys = []
        for i in range(10):
            key = f"ts:{i}"
            keys.append(key)
            client.execute_command("TS.CREATE", key, "LABELS", "region", f"us-east-{i}", "service", "api")
            client.execute_command("TS.ADD", key, 1000 + i, float(i))

        pre_queryindex = sorted(client.execute_command("TS.QUERYINDEX", "service=api"))

        # The replica attaches after the primary already has data, forcing a full sync
        # that must transfer a non-trivial aux payload.
        self.setup_replication(num_replicas=1)
        replica = self.replicas[0]

        wait_for_log(replica, PRELOADED_LOG)

        replica_card = replica.client.execute_command("TS.CARD", "FILTER", "service=api")
        assert replica_card == len(keys)
        replica_queryindex = sorted(replica.client.execute_command("TS.QUERYINDEX", "service=api"))
        assert replica_queryindex == pre_queryindex
        for key in keys:
            assert replica.client.execute_command("EXISTS", key) == 1
