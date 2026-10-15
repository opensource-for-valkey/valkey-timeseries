import json
import logging
import os
import time
from typing import List

import pytest

hypothesis = pytest.importorskip("hypothesis")
from hypothesis import HealthCheck, given, settings  # noqa: E402
from hypothesis import strategies as st  # noqa: E402

from common import SERVER_PATH, get_module_path, parse_info_response  # noqa: E402
from valkeytestframework.conftest import resource_port_tracker  # noqa: E402
from valkeytestframework.valkey_test_case import ReplicationTestCase  # noqa: E402

logger = logging.getLogger(__name__)

REPLICATION_TIMEOUT = 3

FUZZ_SRC, FUZZ_MID, FUZZ_TOP = "ts:fuzz:src", "ts:fuzz:mid", "ts:fuzz:top"

_sample = st.tuples(
    st.integers(0, 4000),
    st.floats(-1000, 1000, allow_nan=False, allow_infinity=False),
)
_fuzz_op = st.one_of(
    st.tuples(st.just("ADD"), _sample),
    st.tuples(st.just("MADD"), st.lists(_sample, min_size=1, max_size=5)),
    st.tuples(st.just("ADDBULK"), st.lists(_sample, min_size=1, max_size=8, unique_by=lambda s: s[0])),
    st.tuples(st.just("DEL"), st.integers(0, 4000), st.integers(0, 4000)),
)


class TestTimeSeriesReplication(ReplicationTestCase):
    """Test replication functionality"""

    # Override the default replica count for replication testing
    REPLICAS_COUNT = 1

    @pytest.fixture(autouse=True)
    def setup_test(self, setup):
        use_external = os.environ.get("VALKEY_EXTERNAL_SERVER", "false").lower() == "true"

        if use_external:
            master_host = os.environ.get("VALKEY_HOST", "localhost")
            master_port = int(os.environ.get("VALKEY_PORT", "6379"))
            self.server, self.client = self.create_server(
                testdir=self.testdir,
                bind_ip=master_host,
                port=master_port,
                external_server=True
            )

            replica_host = os.environ.get("VALKEY_REPLICA_HOST", "localhost")
            replica_port = int(os.environ.get("VALKEY_REPLICA_PORT", "6380"))
            replica_server, replica_client = self.create_server(
                testdir=self.testdir,
                bind_ip=replica_host,
                port=replica_port,
                external_server=True
            )

            self.replicas = [replica_server]
            self.num_replicas = 1
            self.wait_for_primary_link_up_all_replicas()
        else:
            self.args = {"enable-debug-command": "yes", 'loadmodule': get_module_path()}
            self.server, self.client = self.create_server(testdir=self.testdir, server_path=SERVER_PATH, args=self.args)
            self.setup_replication(num_replicas=1)

    def get_config_file_lines(self, test_dir, port) -> List[str]:
        return [
            "enable-debug-command yes",
            f"dir {test_dir}",
            "repl-diskless-sync yes",
            "repl-diskless-sync-delay 0",
            "cluster-enabled yes",
            f"cluster-config-file nodes_{port}.conf",
            f"loadmodule {get_module_path()}",
        ]

    # Wait for replication to propagate
    def wait_for_key_exists(self, key=None):
        self.waitForReplicaToSyncUp(self.replicas[0])
        assert self.replicas[0].client.execute_command('EXISTS key') == 1

    def wait_for_replication(self):
        self.waitForReplicaToSyncUp(self.replicas[0])
        # The link being up says nothing about the replica having applied the primary's latest
        # writes; WAIT blocks until the replica acknowledges the primary's current offset.
        assert self.client.execute_command("WAIT", 1, 5000) == 1, \
            "replica did not acknowledge the primary's offset within 5 s"

    def test_basic_replication(self):
        """Test that basic time series operations replicate to replicas"""
        # Create a time series on primary
        key = "ts:basic_repl"
        client = self.client

        assert client.execute_command(f"TS.CREATE {key}") == b"OK"

        # Add samples to primary
        timestamps = [1000, 2000, 3000]
        values = [10.5, 20.3, 30.7]

        for ts, val in zip(timestamps, values):
            client.execute_command(f"TS.ADD {key} {ts} {val}")

        # Wait for replication to propagate
        self.wait_for_replication()

        # Verify data on replica
        replica_client = self.replicas[0].client

        result = replica_client.execute_command(f"TS.RANGE {key} - +")

        assert len(result) == 3
        for i, (ts, val) in enumerate(result):
            assert ts == timestamps[i]
            assert float(val) == values[i]

    def test_replication_ts_create_with_labels(self):
        """Test that time series with labels replicate correctly"""
        key = "ts:labeled"
        labels = {"sensor": "temp", "location": "room1", "unit": "celsius"}

        client = self.client

        # Create time series with labels on primary
        labels_str = " ".join([f"{k} {v}" for k, v in labels.items()])
        assert client.execute_command(
            f"TS.CREATE {key} LABELS {labels_str}"
        ) == b"OK"

        # Add data
        client.execute_command(f"TS.ADD {key} 1000 25.5")
        client.execute_command(f"TS.ADD {key} 2000 26.8")

        self.wait_for_replication()

        replica = self.replicas[0].client

        exists = replica.execute_command(f"EXISTS {key}")
        print(f"EXISTS {key} == {exists}")

        info = replica.execute_command(f"TS.INFO {key}")
        info_dict = parse_info_response(info)
        assert info_dict["labels"] == labels
        assert info_dict["totalSamples"] == 2

    def test_replication_ts_create_with_retention(self):
        """Test that time series retention replicates correctly"""
        key = "ts:retention"
        retention_ms = 100000

        client = self.client

        # Create time series with retention on primary
        assert client.execute_command(
            f"TS.CREATE {key} RETENTION {retention_ms}"
        ) == b"OK"

        # Add samples
        for i in range(10):
            client.execute_command(f"TS.ADD {key} {1000 + i * 1000} {i}")

        # Wait for replication
        self.wait_for_replication()

        replica = self.replicas[0].client
        exists = replica.execute_command(f"EXISTS {key}")
        assert exists == 1

        info = replica.execute_command(f"TS.INFO {key}")
        info_dict = parse_info_response(info)
        assert info_dict["retentionTime"] == retention_ms
        assert info_dict["totalSamples"] == 10

    def test_replication_ts_createrule(self):
        """Test that compaction rules replicate correctly"""
        source_key = "ts:source"
        dest_key = "ts:dest"

        client = self.client

        # Create source and destination time series
        assert client.execute_command(f"TS.CREATE {source_key}") == b"OK"
        assert client.execute_command(f"TS.CREATE {dest_key}") == b"OK"

        # Create a compaction rule
        assert client.execute_command(
            f"TS.CREATERULE {source_key} {dest_key} AGGREGATION avg 60000"
        ) == b"OK"

        # Add samples to the source
        for i in range(10):
            client.execute_command(f"TS.ADD {source_key} {1000 + i * 10000} {i * 10}")

        self.wait_for_replication()
        replica = self.replicas[0].client

        assert replica.execute_command(f"EXISTS {source_key}") == 1
        assert replica.execute_command(f"EXISTS {dest_key}") == 1

        # Verify compaction rules on replicas
        info = replica.execute_command(f"TS.INFO {source_key}")
        info_dict = parse_info_response(info)

        assert "rules" in info_dict
        assert len(info_dict["rules"]) == 1

        rule = info_dict["rules"][0]
        assert rule.dest_key == dest_key
        assert rule.bucket_duration == 60000
        # TS.INFO reports the rule aggregator uppercase, matching RedisTimeSeries.
        assert rule.aggregation == "AVG"

    def test_replication_ts_del(self):
        """Test that deletions replicate correctly"""
        key = "ts:to_delete"

        client = self.client

        # Create time series and add data
        assert client.execute_command(f"TS.CREATE {key}") == b"OK"
        client.execute_command(f"TS.ADD {key} 1000 10")
        client.execute_command(f"TS.ADD {key} 2000 20")

        # Wait for replication
        self.wait_for_replication()

        # Delete the key on primary
        assert client.execute_command(f"DEL {key}") == 1

        # Wait for the replicas to sync
        self.wait_for_replication()

        replica = self.replicas[0].client

        assert replica.execute_command(f"EXISTS {key}") == 0
        assert client.execute_command(f"EXISTS {key}") == 0

    def test_replication_ts_del_range(self):
        """Test that TS.DEL replicates correctly"""
        key = "ts:del_range"

        client = self.client

        # Create time series and add samples
        assert client.execute_command(f"TS.CREATE {key}") == b"OK"

        timestamps = [1000, 2000, 3000, 4000, 5000]
        for ts in timestamps:
            client.execute_command(f"TS.ADD {key} {ts} {ts}")

        # Wait for replication
        self.wait_for_replication()

        # Delete range on primary
        deleted = client.execute_command(f"TS.DEL {key} 2000 3000")
        assert deleted == 2

        # Wait for the replicas to sync
        self.wait_for_replication()

        # Verify deletion on replicas
        replica = self.replicas[0].client

        result = replica.execute_command(f"TS.RANGE {key} - +")
        assert len(result) == 3
        assert result[0][0] == 1000
        assert result[1][0] == 4000
        assert result[2][0] == 5000

    def test_replication_ts_mdel_series(self):
        """TS.MDEL without a range replicates the keys it removed.

        `TS.MDEL` is not replicated verbatim (see `delete_series_by_selectors`): a verbatim replay
        would re-evaluate the filter, and the timestamp bounds, on the replica. It propagates the
        resolved effects instead, so this pins that the replica ends up with the same keyspace.
        """
        client = self.client

        for name in ("cpu", "cpu2", "mem"):
            key = f"ts:mdel_series:{name}"
            metric = "cpu" if name.startswith("cpu") else "mem"
            client.execute_command(f"TS.CREATE {key} LABELS name {metric}")
            client.execute_command(f"TS.ADD {key} 1000 10")

        self.wait_for_replication()

        deleted = client.execute_command("TS.MDEL FILTER name=cpu")
        assert deleted == 2

        self.wait_for_replication()

        replica = self.replicas[0].client
        for name in ("cpu", "cpu2"):
            key = f"ts:mdel_series:{name}"
            assert client.execute_command(f"EXISTS {key}") == 0
            assert replica.execute_command(f"EXISTS {key}") == 0
        assert replica.execute_command("EXISTS ts:mdel_series:mem") == 1

    def test_replication_ts_mdel_range(self):
        """TS.MDEL with a range replicates the samples it removed, and only those."""
        client = self.client

        keys = ["ts:mdel_range:cpu", "ts:mdel_range:mem"]
        for key in keys:
            metric = "cpu" if key.endswith("cpu") else "mem"
            client.execute_command(f"TS.CREATE {key} LABELS name {metric}")
            for ts in (1000, 2000, 3000, 4000, 5000):
                client.execute_command(f"TS.ADD {key} {ts} {ts}")

        self.wait_for_replication()

        deleted = client.execute_command("TS.MDEL 2000 3000 FILTER name=cpu")
        assert deleted == 2

        self.wait_for_replication()

        replica = self.replicas[0].client
        for key in keys:
            assert replica.execute_command(f"TS.RANGE {key} - +") == client.execute_command(
                f"TS.RANGE {key} - +"
            )
        assert [row[0] for row in replica.execute_command("TS.RANGE ts:mdel_range:cpu - +")] == [
            1000,
            4000,
            5000,
        ]

    def test_replication_ts_alter(self):
        """Test that TS.ALTER replicates correctly"""
        key = "ts:alter"

        client = self.client

        # Create time series
        assert client.execute_command(f"TS.CREATE {key}") == b"OK"

        # Wait for replication
        self.wait_for_replication()

        # Alter retention and labels on primary
        new_retention = 50000
        assert client.execute_command(
            f"TS.ALTER {key} RETENTION {new_retention} LABELS sensor temp location room2"
        ) == b"OK"

        self.wait_for_replication()

        # Verify changes on replica
        replica = self.replicas[0].client

        info = replica.execute_command(f"TS.INFO {key}")
        info_dict = parse_info_response(info)
        assert info_dict["retentionTime"] == new_retention
        assert info_dict["labels"]["sensor"] == "temp"
        assert info_dict["labels"]["location"] == "room2"

    def test_replication_ts_madd(self):
        """Test that TS.MADD replicates correctly"""
        keys = ["ts:madd1", "ts:madd2", "ts:madd3"]

        client = self.client

        # Create time series
        for key in keys:
            assert client.execute_command(f"TS.CREATE {key}") == b"OK"

        # Use TS.MADD to add samples to multiple keys
        madd_cmd = "TS.MADD"
        for i, key in enumerate(keys):
            madd_cmd += f" {key} {1000 + i * 100} {10 + i}"

        result = client.execute_command(madd_cmd)
        assert len(result) == len(keys)

        # Wait for replication
        self.wait_for_replication()

        replica = self.replicas[0].client

        # Verify data on replicas
        for replica in self.replicas:
            for i, key in enumerate(keys):
                result = replica.client.execute_command(f"TS.RANGE {key} - +")
                assert len(result) == 1
                assert result[0][0] == 1000 + i * 100
                assert float(result[0][1]) == 10 + i

    def test_replication_ts_deleterule(self):
        """Test that TS.DELETERULE replicates correctly"""
        source_key = "ts:delrule_src"
        dest_key = "ts:delrule_dst"

        client = self.client

        # Create time series with rule
        assert client.execute_command(f"TS.CREATE {source_key}") == b"OK"
        assert client.execute_command(f"TS.CREATE {dest_key}") == b"OK"
        assert client.execute_command(
            f"TS.CREATERULE {source_key} {dest_key} AGGREGATION sum 10000"
        ) == b"OK"

        # Wait for replication
        self.wait_for_replication()

        # Delete the rule on primary
        assert client.execute_command(
            f"TS.DELETERULE {source_key} {dest_key}"
        ) == b"OK"

        # Wait for replicas to sync
        self.wait_for_replication()
        replica_client = self.replicas[0].client

        # Verify rule deletion on replicas
        info = replica_client.execute_command(f"TS.INFO {source_key}")
        info_dict = parse_info_response(info)
        # Rules should be empty or not present
        assert "rules" not in info_dict or len(info_dict["rules"]) == 0

    def assert_series_matches_replica(self, key):
        """Assert the replica holds byte-identical samples for `key`."""
        self.wait_for_replication()
        replica_client = self.replicas[0].client
        primary_samples = self.client.execute_command(f"TS.RANGE {key} - +")
        replica_samples = replica_client.execute_command(f"TS.RANGE {key} - +")
        assert replica_samples == primary_samples
        return primary_samples

    # --- compaction equivalence helpers (docs/plans/2026-10-14-compaction-refactor.md)

    @staticmethod
    def info_debug(client, key):
        """`TS.INFO key DEBUG` as a dict (the RESP2 flat list, pairwise)."""
        flat = client.execute_command("TS.INFO", key, "DEBUG")
        return {flat[i]: flat[i + 1] for i in range(0, len(flat), 2)}

    def set_compat_mode(self, mode):
        for client in (self.client, self.replicas[0].client):
            client.execute_command("CONFIG", "SET", "ts.ts-compatibility-mode", mode)

    def rule(self, src, dst, agg="sum", bucket=1000, align=None, src_options=()):
        """Create `src` (with `src_options`), `dst`, and a rule between them on the primary."""
        self.client.execute_command("TS.CREATE", src, *src_options)
        self.client.execute_command("TS.CREATE", dst)
        args = ["TS.CREATERULE", src, dst, "AGGREGATION", agg, bucket]
        if align is not None:
            args.append(align)
        assert self.client.execute_command(*args) == b"OK"

    def assert_compaction_equivalent(self, *keys):
        """The replica holds the same stored data as the primary for every key: digest, samples,
        TS.GET, and the open bucket as TS.GET … LATEST reports it."""
        self.wait_for_replication()
        replica = self.replicas[0].client
        for key in keys:
            for cmd in (
                ("DEBUG", "DIGEST-VALUE", key),
                ("TS.RANGE", key, "-", "+"),
                ("TS.GET", key),
                ("TS.GET", key, "LATEST"),
            ):
                assert replica.execute_command(*cmd) == self.client.execute_command(*cmd), (key, cmd)

    def assert_rule_state_equivalent(self, *keys):
        """The replica's open-bucket state (TS.INFO DEBUG rulesState) matches the primary's.
        Replicas recompute, so this always holds; a mismatch is a determinism bug in the engine."""
        self.wait_for_replication()
        replica = self.replicas[0].client
        for key in keys:
            assert self.info_debug(replica, key)[b"rulesState"] == \
                self.info_debug(self.client, key)[b"rulesState"], key

    @staticmethod
    def wait_link_up(client, timeout=10):
        deadline = time.time() + timeout
        while time.time() < deadline:
            info = client.info("replication")
            if info.get("master_link_status") == "up" and not info.get("master_sync_in_progress", 0):
                return
            time.sleep(0.1)
        raise AssertionError("replica link did not come up")

    def force_full_resync(self):
        """Detach the replica, wipe it, re-attach it: with a new replication id the next link-up
        is a full sync of the primary's current snapshot, never a partial one."""
        replica = self.replicas[0].client
        replica.execute_command("REPLICAOF", "NO", "ONE")
        replica.execute_command("FLUSHALL")
        replica.execute_command("REPLICAOF", self.server.bind_ip, self.server.port)
        self.wait_link_up(replica)
        self.wait_for_replication()

    @staticmethod
    def wait_caught_up(primary, replica, timeout=10):
        """`replica` has applied everything `primary` has propagated (offset comparison; the
        framework's waiter only knows the original primary/replica pair)."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            target = primary.info("replication")["master_repl_offset"]
            if replica.info("replication")["master_repl_offset"] >= target:
                return
            time.sleep(0.05)
        raise AssertionError("replica did not catch up")

    def promote_replica(self):
        """Swap roles: the replica becomes primary and the old primary follows it. Returns
        (new_primary_client, new_replica_client). The helpers above keep pointing at
        `self.client`/`self.replicas[0]`, so compare explicitly after this."""
        replica = self.replicas[0]
        replica.client.execute_command("REPLICAOF", "NO", "ONE")
        self.client.execute_command("REPLICAOF", replica.bind_ip, replica.port)
        self.wait_link_up(self.client)
        return replica.client, self.client

    def assert_equivalent_between(self, a, b, *keys):
        for key in keys:
            for cmd in (("DEBUG", "DIGEST-VALUE", key), ("TS.RANGE", key, "-", "+"), ("TS.GET", key, "LATEST")):
                assert a.execute_command(*cmd) == b.execute_command(*cmd), (key, cmd)

    def await_propagated_command(self, monitor, name, deadline_seconds=5):
        """Return the first command MONITOR reports on the replica whose name is `name`."""
        deadline = time.time() + deadline_seconds
        while time.time() < deadline:
            entry = monitor.next_command()
            if entry is None:
                continue
            command = entry.get("command", "")
            if command.lower().startswith(name.lower()):
                return command
        raise AssertionError(f"{name} was never propagated to the replica")

    def test_replication_ts_incrby_propagates_explicit_timestamp(self):
        """The propagated TS.INCRBY carries the timestamp the primary resolved.

        Whether an implicit "now" visibly diverges depends on how far apart the primary's
        and replica's clock reads happen to land, so assert the propagated command itself:
        MONITOR on the replica shows exactly what arrived over the replication link.
        """
        key = "ts:incr_propagated"

        monitor_client = self.replicas[0].get_new_client()
        monitor_client.connection_pool.connection_kwargs["socket_timeout"] = 5

        with monitor_client.monitor() as monitor:
            returned = self.client.execute_command(f"TS.INCRBY {key} 3 RETENTION 60000")
            propagated = self.await_propagated_command(monitor, "ts.incrby")

        parts = propagated.split()
        assert parts[1].strip('"') == key
        assert "TIMESTAMP" in [p.strip('"').upper() for p in parts], (
            f"TS.INCRBY propagated without an explicit timestamp: {propagated}"
        )
        ts_index = [p.strip('"').upper() for p in parts].index("TIMESTAMP")
        assert int(parts[ts_index + 1].strip('"')) == returned
        # The auto-creation options ride along so a replica that has to create the series
        # builds the same one.
        assert "RETENTION" in [p.strip('"').upper() for p in parts]

    def test_replication_ts_incrby_without_timestamp(self):
        """TS.INCRBY without TIMESTAMP must land on the same timestamp on the replica.

        The primary derives "now" when the caller omits TIMESTAMP. If the command were
        replicated verbatim the replica would derive its own clock reading and diverge.
        """
        key = "ts:incr_no_ts"

        assert self.client.execute_command(f"TS.CREATE {key}") == b"OK"

        returned = []
        for _ in range(5):
            returned.append(self.client.execute_command(f"TS.INCRBY {key} 1"))

        samples = self.assert_series_matches_replica(key)

        # Every sample sits on a timestamp the primary reported, not one the replica invented.
        assert {ts for ts, _ in samples} <= set(returned)
        # The counter total survives regardless of how many increments collapsed into the
        # same millisecond.
        assert sum(float(v) for _, v in samples) == 5.0

    def test_replication_ts_decrby_without_timestamp(self):
        """TS.DECRBY without TIMESTAMP replicates the primary's resolved timestamp."""
        key = "ts:decr_no_ts"

        assert self.client.execute_command(f"TS.CREATE {key}") == b"OK"
        self.client.execute_command(f"TS.ADD {key} * 100")

        for _ in range(4):
            self.client.execute_command(f"TS.DECRBY {key} 2.5")

        self.assert_series_matches_replica(key)

    def test_replication_ts_incrby_autocreates_with_options(self):
        """An auto-creating TS.INCRBY carries its creation options to the replica."""
        key = "ts:incr_autocreate"

        self.client.execute_command(
            f"TS.INCRBY {key} 7 RETENTION 500000 CHUNK_SIZE 128 LABELS sensor temp room r1"
        )

        self.wait_for_replication()
        replica_client = self.replicas[0].client

        assert replica_client.execute_command(f"EXISTS {key}") == 1

        info = parse_info_response(replica_client.execute_command(f"TS.INFO {key}"))
        assert info["retentionTime"] == 500000
        assert info["chunkSize"] == 128
        assert info["labels"] == {"sensor": "temp", "room": "r1"}

        self.assert_series_matches_replica(key)

    def test_replication_ts_incrby_retention_trim(self):
        """Retention trimming driven by TS.INCRBY leaves the same window on the replica.

        The clock-derived first sample anchors every later timestamp. If the replica read
        its own clock it would anchor elsewhere and then either reject the explicit-timestamp
        increments as too old or keep a stray sample of its own — either way the retained
        window would differ.
        """
        key = "ts:incr_retention"

        assert self.client.execute_command(f"TS.CREATE {key} RETENTION 5000") == b"OK"

        anchor = self.client.execute_command(f"TS.INCRBY {key} 1")
        for i in range(1, 10):
            self.client.execute_command(f"TS.INCRBY {key} 1 TIMESTAMP {anchor + i * 1000}")

        self.assert_series_matches_replica(key)

        # Trimming is chunk-granular, so which old samples survive is an implementation
        # detail — but the retained window's endpoints must be identical on both sides.
        primary_info = parse_info_response(self.client.execute_command(f"TS.INFO {key}"))
        replica_info = parse_info_response(
            self.replicas[0].client.execute_command(f"TS.INFO {key}")
        )
        assert primary_info["lastTimestamp"] == anchor + 9000
        assert replica_info["firstTimestamp"] == primary_info["firstTimestamp"]
        assert replica_info["lastTimestamp"] == primary_info["lastTimestamp"]
        assert replica_info["totalSamples"] == primary_info["totalSamples"]

    def test_replication_ts_incrby_compaction(self):
        """Compaction fed by TS.INCRBY produces the same destination series on the replica.

        Bucket boundaries are absolute, so an anchor sample the replica timed itself would
        distribute the rollup across different buckets.
        """
        source_key = "ts:incr_comp_src"
        dest_key = "ts:incr_comp_dst"

        assert self.client.execute_command(f"TS.CREATE {source_key}") == b"OK"
        assert self.client.execute_command(f"TS.CREATE {dest_key}") == b"OK"
        assert self.client.execute_command(
            f"TS.CREATERULE {source_key} {dest_key} AGGREGATION sum 1000"
        ) == b"OK"

        anchor = self.client.execute_command(f"TS.INCRBY {source_key} 2")
        for i in range(1, 12):
            self.client.execute_command(
                f"TS.INCRBY {source_key} 2 TIMESTAMP {anchor + i * 400}"
            )
        # Close the trailing bucket so the rollup is committed on both sides.
        self.client.execute_command(f"TS.INCRBY {source_key} 2 TIMESTAMP {anchor + 20000}")

        self.assert_series_matches_replica(source_key)
        dest_samples = self.assert_series_matches_replica(dest_key)
        assert len(dest_samples) > 0

    def test_replication_ts_addbulk_autocreate(self):
        """An auto-creating TS.ADDBULK builds the same series, options and samples on the replica."""
        key = "ts:addbulk_autocreate"
        # Unsorted on purpose: the replica must sort the replayed payload the same way.
        payload = json.dumps({"timestamps": [3000, 1000, 2000], "values": [3.5, 1.25, 2]})

        assert self.client.execute_command(
            "TS.ADDBULK", key, payload,
            "RETENTION", 500000, "DUPLICATE_POLICY", "LAST", "LABELS", "sensor", "temp",
        ) == [3, 3]

        samples = self.assert_series_matches_replica(key)
        assert [ts for ts, _ in samples] == [1000, 2000, 3000]

        info = parse_info_response(self.replicas[0].client.execute_command(f"TS.INFO {key}"))
        assert info["retentionTime"] == 500000
        assert info["duplicatePolicy"] == "last"
        assert info["labels"] == {"sensor": "temp"}

    def test_replication_ts_addbulk_duplicates(self):
        """Samples a duplicate policy rejects or merges end up the same on the replica.

        The replica replays the payload against its own copy of the series, so a rejected
        sample must be rejected there too, and an ON_DUPLICATE SUM must be applied once.
        """
        blocked = "ts:addbulk_block"
        summed = "ts:addbulk_sum"
        for key in (blocked, summed):
            assert self.client.execute_command(
                f"TS.CREATE {key} DUPLICATE_POLICY BLOCK"
            ) == b"OK"
            self.client.execute_command(f"TS.ADD {key} 1000 5")

        payload = json.dumps({"timestamps": [1000, 2000], "values": [3, 7]})
        assert self.client.execute_command("TS.ADDBULK", blocked, payload) == [1, 2]
        assert self.client.execute_command(
            "TS.ADDBULK", summed, payload, "ON_DUPLICATE", "SUM"
        ) == [2, 2]

        assert self.assert_series_matches_replica(blocked) == [[1000, b"5"], [2000, b"7"]]
        assert self.assert_series_matches_replica(summed) == [[1000, b"8"], [2000, b"7"]]

    def test_replication_ts_addbulk_compaction(self):
        """Compaction fed by TS.ADDBULK produces the same destination series on the replica,
        including a closed bucket that a later out-of-order batch makes recalculate."""
        src = "ts:addbulk_comp_src"
        dest = "ts:addbulk_comp_dst"

        assert self.client.execute_command(f"TS.CREATE {src}") == b"OK"
        assert self.client.execute_command(f"TS.CREATE {dest}") == b"OK"
        assert self.client.execute_command(
            f"TS.CREATERULE {src} {dest} AGGREGATION sum 10"
        ) == b"OK"

        timestamps = list(range(0, 100))
        payload = json.dumps({"timestamps": timestamps, "values": [1] * len(timestamps)})
        assert self.client.execute_command("TS.ADDBULK", src, payload) == [100, 100]

        # Lands in the already-closed bucket 0, which must be recalculated on both sides.
        late = json.dumps({"timestamps": [5], "values": [100]})
        assert self.client.execute_command(
            "TS.ADDBULK", src, late, "ON_DUPLICATE", "SUM"
        ) == [1, 1]

        self.assert_series_matches_replica(src)
        dest_samples = self.assert_series_matches_replica(dest)
        assert dest_samples[0] == [0, b"110"]
        assert len(dest_samples) == 9  # bucket 90 is still open

        replica = self.replicas[0].client
        assert replica.execute_command(f"TS.GET {dest} LATEST") == self.client.execute_command(
            f"TS.GET {dest} LATEST"
        )

    def test_replication_ts_addbulk_retention(self):
        """Retention trimming driven by a large TS.ADDBULK leaves the same window on the replica."""
        key = "ts:addbulk_retention"
        assert self.client.execute_command(
            f"TS.CREATE {key} RETENTION 1000 ENCODING UNCOMPRESSED CHUNK_SIZE 128"
        ) == b"OK"

        timestamps = list(range(0, 10_000, 10))
        payload = json.dumps({"timestamps": timestamps, "values": [1.0] * len(timestamps)})
        assert self.client.execute_command("TS.ADDBULK", key, payload) == [1000, 1000]

        samples = self.assert_series_matches_replica(key)
        assert len(samples) == 101

        primary_info = parse_info_response(self.client.execute_command(f"TS.INFO {key}"))
        replica_info = parse_info_response(
            self.replicas[0].client.execute_command(f"TS.INFO {key}")
        )
        for field in ("firstTimestamp", "lastTimestamp", "totalSamples"):
            assert replica_info[field] == primary_info[field], field

    def test_replication_autocreate_pins_primary_defaults(self):
        """An auto-created series is replicated with the primary's resolved defaults ahead of the
        write, so a replica whose ts-retention-policy differs still builds the same series — and
        a rule fed by it uses the same retention floor."""
        replica = self.replicas[0].client
        replica.execute_command("CONFIG", "SET", "ts.ts-retention-policy", 500)
        self.client.execute_command("CONFIG", "SET", "ts.ts-retention-policy", 3000)

        self.client.execute_command("TS.ADD", "ts:auto:plain", 0, 1)
        self.client.execute_command("TS.ADD", "ts:auto:metric", 0, 1, "METRIC", 'cpu{host="a"}')
        self.client.execute_command("TS.INCRBY", "ts:auto:incr", 1, "LABELS", "k", "v")
        self.client.execute_command("TS.ADDBULK", "ts:auto:bulk", json.dumps({"timestamps": [0, 1000], "values": [1, 2]}))
        self.wait_for_replication()
        for key in ("ts:auto:plain", "ts:auto:metric", "ts:auto:incr", "ts:auto:bulk"):
            primary_info = parse_info_response(self.client.execute_command("TS.INFO", key))
            replica_info = parse_info_response(replica.execute_command("TS.INFO", key))
            for field in ("retentionTime", "chunkSize", "labels", "duplicatePolicy"):
                assert replica_info[field] == primary_info[field], (key, field)
            assert primary_info["retentionTime"] == 3000, key

        # The destination is an explicit TS.CREATE, which replicates verbatim: left bare it would
        # pick up the replica's own retention (500) and trim the rollup differently. Pin it.
        self.client.execute_command("TS.CREATE", "ts:auto:dst", "RETENTION", 3000)
        self.client.execute_command("TS.CREATERULE", "ts:auto:plain", "ts:auto:dst", "AGGREGATION", "sum", 1000)
        for ts in range(500, 6000, 500):
            self.client.execute_command("TS.ADD", "ts:auto:plain", ts, 1)
        self.client.execute_command("TS.ADD", "ts:auto:plain", 2600, 10)   # back-fill near the floor
        self.assert_compaction_equivalent("ts:auto:plain", "ts:auto:dst")

    @staticmethod
    def info_without_memory(client, key):
        """`TS.INFO key` as a dict, minus `memoryUsage` (an allocator detail, not series state)."""
        flat = client.execute_command("TS.INFO", key)
        info = {flat[i]: flat[i + 1] for i in range(0, len(flat), 2)}
        info.pop(b"memoryUsage")
        return info

    def test_replication_autocreate_materializes_every_option(self):
        """Every configuration-derived option an auto-create resolves reaches the replica,
        whichever command created the series and whichever way the two nodes' configs differ."""
        replica = self.replicas[0].client
        primary_config = {
            "ts-encoding": "gorilla", "ts-chunk-size-bytes": 1024, "ts-duplicate-policy": "last",
            "ts-retention-policy": 7000, "ts-ignore-max-time-diff": 5, "ts-ignore-max-val-diff": "0.5",
            "ts-decimal-digits": 2,
        }
        replica_config = {
            "ts-encoding": "uncompressed", "ts-chunk-size-bytes": 2048, "ts-duplicate-policy": "sum",
            "ts-retention-policy": 100, "ts-ignore-max-time-diff": 9, "ts-ignore-max-val-diff": "0.9",
            "ts-significant-digits": 4,
        }
        for name, value in primary_config.items():
            self.client.execute_command("CONFIG", "SET", f"ts.{name}", value)
        for name, value in replica_config.items():
            replica.execute_command("CONFIG", "SET", f"ts.{name}", value)

        payload = json.dumps({"timestamps": [1000, 2000], "values": [1.234, 2.345]})
        self.client.execute_command("TS.ADD", "ts:opt:add", 1000, 1.234)
        self.client.execute_command(
            "TS.ADD", "ts:opt:inline", 1000, 1.234,
            "RETENTION", 9000, "ENCODING", "uncompressed", "CHUNK_SIZE", 256, "DUPLICATE_POLICY", "max",
            "LABELS", "a", "b",
        )
        self.client.execute_command("TS.ADD", "ts:opt:metric", 1000, 1.234, "METRIC", 'cpu{host="a"}')
        # No rounding over the primary's configured default; the replica has a different default.
        self.client.execute_command("TS.ADD", "ts:opt:norounding", 1000, 1.23456789, "DECIMAL_DIGITS", "none")
        # `LABELS __name__ …` is accepted, and can spell what the METRIC grammar cannot.
        self.client.execute_command("TS.ADD", "ts:opt:odd", 1000, 1.234, "LABELS", "__name__", "cpu total", "1x", "a")
        self.client.execute_command("TS.INCRBY", "ts:opt:incr", 1.234, "TIMESTAMP", 1000, "LABELS", "k", "v")
        self.client.execute_command("TS.DECRBY", "ts:opt:decr", 1.234, "TIMESTAMP", 1000)
        self.client.execute_command("TS.ADDBULK", "ts:opt:bulk", payload, "LABELS", "k", "v")
        keys = [
            "ts:opt:add", "ts:opt:inline", "ts:opt:metric", "ts:opt:norounding", "ts:opt:odd", "ts:opt:incr",
            "ts:opt:decr", "ts:opt:bulk",
        ]

        self.assert_compaction_equivalent(*keys)
        for key in keys:
            assert self.info_without_memory(replica, key) == self.info_without_memory(self.client, key), key
        # The primary's retention, not the replica's 100, and its rounding, not the replica's.
        assert parse_info_response(replica.execute_command("TS.INFO", "ts:opt:add"))["retentionTime"] == 7000
        assert replica.execute_command("TS.GET", "ts:opt:add") == [1000, b"1.23"]
        assert replica.execute_command("TS.GET", "ts:opt:norounding") == [1000, b"1.23456789"]
        assert parse_info_response(replica.execute_command("TS.INFO", "ts:opt:inline"))["retentionTime"] == 9000

        # The other direction: the primary is back on its defaults (no retention, no ignore, no
        # rounding, ...) while the replica keeps its non-default ones, including a rounding default.
        # Nothing in the replicated create may be left for the replica's configuration to fill in.
        for name, default in (
            ("ts-encoding", "compressed"), ("ts-chunk-size-bytes", 4096), ("ts-duplicate-policy", "block"),
            ("ts-retention-policy", 0), ("ts-ignore-max-time-diff", 0), ("ts-ignore-max-val-diff", 0),
            ("ts-decimal-digits", "none"),
        ):
            self.client.execute_command("CONFIG", "SET", f"ts.{name}", default)

        self.client.execute_command("TS.ADD", "ts:opt:defaults", 1000, 1.23456789)
        self.client.execute_command("TS.INCRBY", "ts:opt:defaults:incr", 1.23456789, "TIMESTAMP", 1000)
        self.assert_compaction_equivalent("ts:opt:defaults", "ts:opt:defaults:incr")
        for key in ("ts:opt:defaults", "ts:opt:defaults:incr"):
            assert self.info_without_memory(replica, key) == self.info_without_memory(self.client, key), key
        defaults = parse_info_response(replica.execute_command("TS.INFO", "ts:opt:defaults"))
        assert defaults["retentionTime"] == 0
        assert defaults["chunkSize"] == 4096
        assert defaults["duplicatePolicy"] == "block"
        # Unrounded on the primary, so unrounded on the replica, whose ts-significant-digits is 4.
        assert replica.execute_command("TS.GET", "ts:opt:defaults") == [1000, b"1.23456789"]

    def test_replication_explicit_create_pins_primary_defaults(self):
        """An explicit TS.CREATE is replicated resolved too: a bare one takes retention, encoding,
        chunk size and rounding from the primary's configuration, not the replica's."""
        replica = self.replicas[0].client
        keys = []

        def create_and_fill(prefix):
            for suffix, options in (
                ("bare", ()),
                ("labels", ("LABELS", "a", "b")),
                ("metric", ("METRIC", 'cpu{host="' + prefix + '"}')),
                ("override", ("RETENTION", 9000, "DUPLICATE_POLICY", "sum")),
            ):
                key = f"ts:explicit:{prefix}:{suffix}"
                self.client.execute_command("TS.CREATE", key, *options)
                # The series exists, so these are plain writes: nothing here creates anything.
                self.client.execute_command("TS.ADD", key, 1000, 1.23456789)
                self.client.execute_command("TS.ADD", key, 3500, 2.3456789)
                keys.append(key)

        # The primary has a retention and a rounding default; the replica a different retention
        # and none.
        self.client.execute_command("CONFIG", "SET", "ts.ts-retention-policy", 3000)
        self.client.execute_command("CONFIG", "SET", "ts.ts-decimal-digits", 2)
        replica.execute_command("CONFIG", "SET", "ts.ts-retention-policy", 500)
        create_and_fill("rounded")

        # And the other way round: the replica has a rounding default, the primary none.
        self.client.execute_command("CONFIG", "SET", "ts.ts-decimal-digits", "none")
        replica.execute_command("CONFIG", "SET", "ts.ts-significant-digits", 4)
        create_and_fill("plain")

        self.assert_compaction_equivalent(*keys)
        for key in keys:
            assert self.info_without_memory(replica, key) == self.info_without_memory(self.client, key), key
        bare = parse_info_response(replica.execute_command("TS.INFO", "ts:explicit:rounded:bare"))
        assert bare["retentionTime"] == 3000
        # Under the primary's 3000 ms window the first sample is still in range; under the
        # replica's own 500 ms it would be gone.
        assert len(replica.execute_command("TS.RANGE", "ts:explicit:rounded:bare", "-", "+")) == 2
        assert replica.execute_command("TS.GET", "ts:explicit:rounded:bare") == [3500, b"2.35"]
        assert parse_info_response(
            replica.execute_command("TS.INFO", "ts:explicit:rounded:override")
        )["retentionTime"] == 9000
        assert replica.execute_command("TS.GET", "ts:explicit:plain:bare") == [3500, b"2.3456789"]

    @staticmethod
    def stored_chunks(client, key):
        """Per-chunk `TS.INFO DEBUG` entries: what the node stores, expired samples included."""
        flat = client.execute_command("TS.INFO", key, "DEBUG")
        info = {flat[i]: flat[i + 1] for i in range(0, len(flat), 2)}
        return [{c[i]: c[i + 1] for i in range(0, len(c), 2)} for c in info[b"Chunks"]]

    def test_replication_retention_trim_cron(self):
        """The retention-trim cron runs on every node and is not replicated, so each node
        trims on its own schedule. That is safe only because the retention floor follows
        the series' last sample rather than the clock: both nodes must hide, and eventually
        drop, exactly the same samples.

        The write path defers re-encoding a compressed head chunk until a quarter of its
        span has expired, so the expired prefix here stays stored until the cron removes it.
        If the cron were ever restricted to primaries without a replicated trim, the replica
        would keep that prefix and the wait below would time out.
        """
        key = "ts:trim_cron"
        retention = 10_000
        last_ts = 12_000
        floor = last_ts - retention

        assert self.client.execute_command(
            f"TS.CREATE {key} RETENTION {retention} ENCODING COMPRESSED CHUNK_SIZE 16384"
        ) == b"OK"
        pipe = self.client.pipeline(transaction=False)
        for ts in range(0, last_ts + 1, 100):
            pipe.execute_command("TS.ADD", key, ts, ts // 100)
        pipe.execute()

        samples = self.assert_series_matches_replica(key)
        assert samples[0][0] == floor
        assert len(samples) == (last_ts - floor) // 100 + 1

        # Precondition: one compressed chunk whose expired prefix the write path kept.
        # (The cron fires every 10 s, so it could in principle land between the adds and
        # this check; the window is a few milliseconds.)
        chunks = self.stored_chunks(self.client, key)
        assert len(chunks) == 1
        assert chunks[0][b"startTimestamp"] < floor

        replica = self.replicas[0].client

        def assert_nodes_agree():
            primary_info = parse_info_response(self.client.execute_command(f"TS.INFO {key}"))
            replica_info = parse_info_response(replica.execute_command(f"TS.INFO {key}"))
            for field in ("firstTimestamp", "lastTimestamp", "totalSamples"):
                assert replica_info[field] == primary_info[field], field
            assert primary_info["firstTimestamp"] == floor
            assert replica.execute_command(f"TS.RANGE {key} - +") == samples
            assert self.client.execute_command(f"TS.RANGE {key} - +") == samples
            assert replica.execute_command("DEBUG", "DIGEST-VALUE", key) == \
                self.client.execute_command("DEBUG", "DIGEST-VALUE", key)

        assert_nodes_agree()

        # The cron runs every 10 s, and a run that finds the db cursor exhausted only resets
        # it, so the series may not be reached until the second run after it was written.
        deadline = time.time() + 30
        while True:
            trimmed = [
                self.stored_chunks(client, key)[0][b"startTimestamp"] >= floor
                for client in (self.client, replica)
            ]
            if all(trimmed):
                break
            assert time.time() < deadline, f"trim cron did not run (primary, replica) = {trimmed}"
            time.sleep(0.5)

        assert_nodes_agree()

    def test_replication_compaction_add_paths(self):
        """TS.ADD-fed rule: append, same-timestamp upsert, back-fill into a closed bucket, and a
        back-fill two buckets back leave identical destination data and open-bucket state."""
        src, dst = "ts:comp_add:src", "ts:comp_add:dst"
        self.rule(src, dst, "avg", 1000, src_options=("DUPLICATE_POLICY", "LAST"))
        for ts, value in ((0, 1), (500, 2), (1000, 3), (1500, 4), (2500, 5)):
            self.client.execute_command("TS.ADD", src, ts, value)
        self.assert_compaction_equivalent(src, dst)
        self.assert_rule_state_equivalent(src)
        self.client.execute_command("TS.ADD", src, 2500, 50)   # upsert in the open bucket
        self.client.execute_command("TS.ADD", src, 1200, 60)   # back-fill the closed bucket 1000
        self.client.execute_command("TS.ADD", src, 10, 70)     # back-fill two buckets back
        self.assert_compaction_equivalent(src, dst)
        self.assert_rule_state_equivalent(src)

    def test_replication_compaction_madd(self):
        """TS.MADD: out-of-order items, in-batch duplicates under each duplicate policy, and a
        BLOCK-rejected duplicate (which the primary leaves out of the propagated TS.MADD)."""
        pairs = []
        for policy in ("BLOCK", "SUM", "LAST", "MIN"):
            src, dst = f"ts:comp_madd:{policy}:src", f"ts:comp_madd:{policy}:dst"
            self.rule(src, dst, "sum", 1000, src_options=("DUPLICATE_POLICY", policy))
            pairs.append((src, dst))
        for src, _ in pairs:
            self.client.execute_command("TS.MADD", src, 0, 1, src, 2000, 2, src, 1000, 3)
            self.client.execute_command("TS.MADD", src, 500, 4, src, 500, 5, src, 2500, 6)
            self.client.execute_command("TS.MADD", src, 2500, 7, src, 3100, 8)
        keys = [key for pair in pairs for key in pair]
        self.assert_compaction_equivalent(*keys)
        self.assert_rule_state_equivalent(*[src for src, _ in pairs])

    def test_replication_compaction_del(self):
        """TS.DEL: a partial bucket, fully covered middle buckets, and a trailing removal that
        re-opens an already published bucket."""
        src, dst = "ts:comp_del:src", "ts:comp_del:dst"
        self.rule(src, dst, "sum", 1000)
        for ts in range(0, 6000, 100):
            self.client.execute_command("TS.ADD", src, ts, 1)
        self.assert_compaction_equivalent(src, dst)
        assert self.client.execute_command("TS.DEL", src, 1200, 1400) == 3      # partial bucket
        self.assert_compaction_equivalent(src, dst)
        assert self.client.execute_command("TS.DEL", src, 1900, 4100) == 23     # boundaries + middle
        self.assert_compaction_equivalent(src, dst)
        assert self.client.execute_command("TS.DEL", src, 4500, 6000) == 15     # trailing: re-opens 4000
        self.assert_compaction_equivalent(src, dst)
        self.assert_rule_state_equivalent(src)
        self.client.execute_command("TS.ADD", src, 7000, 9)                    # closes it again
        self.assert_compaction_equivalent(src, dst)

    def test_replication_ts_del_after_compaction_failure(self):
        """A rule failing on the primary does not stop TS.DEL from replicating: the source delete
        is already committed, so the command must reply with the count and propagate the resolved
        TS.DEL (seen via MONITOR on the replica), or the replica keeps samples the primary deleted.
        `TS._DEBUG` is enabled on the primary only, so the replica's own application succeeds."""
        src, dst = "ts:comp_del_fail:src", "ts:comp_del_fail:dst"
        self.client.execute_command("CONFIG", "SET", "ts.debug-mode", "yes")
        try:
            self.rule(src, dst, "sum", 1000)
            for ts, value in ((0, 1), (300, 2), (600, 4)):
                self.client.execute_command("TS.ADD", src, ts, value)
            self.wait_for_replication()

            monitor_client = self.replicas[0].get_new_client()
            monitor_client.connection_pool.connection_kwargs["socket_timeout"] = 5
            with monitor_client.monitor() as monitor:
                self.client.execute_command("TS._DEBUG", "FAIL_NEXT_COMPACTION")
                assert self.client.execute_command("TS.DEL", src, 300, 300) == 1
                propagated = self.await_propagated_command(monitor, "ts.del")
            assert [p.strip('"') for p in propagated.split()[1:]] == [src, "300", "300"]

            self.wait_for_replication()
            replica = self.replicas[0].client
            expected = [[0, b"1"], [600, b"4"]]
            assert self.client.execute_command("TS.RANGE", src, "-", "+") == expected
            assert replica.execute_command("TS.RANGE", src, "-", "+") == expected
            assert self.info_debug(self.client, src)[b"rulesState"][0][4] == 1   # primary: dirty
            assert self.info_debug(replica, src)[b"rulesState"][0][4] == 0       # replica: applied
        finally:
            self.client.execute_command("CONFIG", "SET", "ts.debug-mode", "no")

    def test_replication_compaction_chain_and_diamond(self):
        """A three-level chain, and a source with two rules."""
        self.rule("ts:chain:src", "ts:chain:mid", "sum", 1000)
        self.client.execute_command("TS.CREATE", "ts:chain:top")
        self.client.execute_command("TS.CREATERULE", "ts:chain:mid", "ts:chain:top", "AGGREGATION", "avg", 2000)
        self.client.execute_command("TS.CREATE", "ts:chain:max")
        self.client.execute_command("TS.CREATERULE", "ts:chain:src", "ts:chain:max", "AGGREGATION", "max", 500)
        for ts in range(0, 9000, 250):
            self.client.execute_command("TS.ADD", "ts:chain:src", ts, ts % 7)
        self.client.execute_command("TS.ADD", "ts:chain:src", 1255, 100)   # back-fill cascades mid -> top
        self.client.execute_command("TS.DEL", "ts:chain:src", 3000, 4999)  # removal cascades
        keys = ("ts:chain:src", "ts:chain:mid", "ts:chain:top", "ts:chain:max")
        self.assert_compaction_equivalent(*keys)
        self.assert_rule_state_equivalent("ts:chain:src", "ts:chain:mid")

    def test_replication_compaction_createrule_on_existing_data(self):
        src, dst = "ts:late_rule:src", "ts:late_rule:dst"
        self.client.execute_command("TS.CREATE", src)
        for ts in range(0, 3500, 500):
            self.client.execute_command("TS.ADD", src, ts, 1)
        self.client.execute_command("TS.CREATE", dst)
        self.client.execute_command("TS.CREATERULE", src, dst, "AGGREGATION", "sum", 1000)
        self.client.execute_command("TS.ADD", src, 3500, 1)
        self.client.execute_command("TS.ADD", src, 4000, 1)
        self.assert_compaction_equivalent(src, dst)
        self.assert_rule_state_equivalent(src)

    def test_replication_compaction_deleterule_mid_bucket_then_recreate(self):
        src, dst = "ts:rerule:src", "ts:rerule:dst"
        self.rule(src, dst, "sum", 1000)
        for ts in (0, 500, 1000, 1200):
            self.client.execute_command("TS.ADD", src, ts, 1)
        assert self.client.execute_command("TS.DELETERULE", src, dst) == b"OK"
        self.client.execute_command("TS.ADD", src, 1500, 1)
        self.client.execute_command("TS.CREATERULE", src, dst, "AGGREGATION", "sum", 1000)
        self.client.execute_command("TS.ADD", src, 2000, 1)
        self.client.execute_command("TS.ADD", src, 3000, 1)
        self.assert_compaction_equivalent(src, dst)
        self.assert_rule_state_equivalent(src)

    def test_replication_compaction_direct_dest_writes(self):
        """A client may write straight to a destination; a source TS.DEL covering that bucket
        recalculates it from the source and erases the direct write (DIV-0021 model)."""
        src, dst = "ts:direct:src", "ts:direct:dst"
        self.rule(src, dst, "sum", 1000)
        # dst already holds the compaction's own sample at 0, so the direct write is an upsert
        self.client.execute_command("TS.ALTER", dst, "DUPLICATE_POLICY", "LAST")
        for ts in (0, 500, 1000, 1500, 2000):
            self.client.execute_command("TS.ADD", src, ts, 1)
        self.client.execute_command("TS.ADD", dst, 0, 99)      # direct destination write
        self.client.execute_command("TS.ADD", dst, 5000, 42)   # direct write ahead of the source
        self.assert_compaction_equivalent(src, dst)
        self.client.execute_command("TS.DEL", src, 0, 999)
        self.assert_compaction_equivalent(src, dst)

    def test_replication_compaction_source_retention(self):
        """Retention on the source sets the recalculation floor; both nodes must use the same."""
        src, dst = "ts:ret_rule:src", "ts:ret_rule:dst"
        self.rule(src, dst, "sum", 1000, src_options=("RETENTION", 2500))
        for ts in range(0, 6000, 250):
            self.client.execute_command("TS.ADD", src, ts, 1)
        self.client.execute_command("TS.ADD", src, 3300, 10)    # back-fill at the window's edge
        self.assert_compaction_equivalent(src, dst)
        self.assert_rule_state_equivalent(src)

    def test_replication_compaction_policy_rules(self):
        """Rules created by ts-compaction-policy, with the same policy on both nodes.

        The primary attaches policy rules in its own HashMap iteration order; the replica gets
        them as TS.CREATERULE commands in that same order (it never applies the policy itself),
        so the source's rule order and DEBUG DIGEST-VALUE match. Six rules, so that two
        independently built orders would coincide only with probability 1/720
        (compaction-replication-plan.md section 3.8)."""
        policy = "max:1M:1h;avg:2M:1h;min:1M:1h;sum:2M:1h;count:1M:1h;last:2M:1h"
        for client in (self.client, self.replicas[0].client):
            client.execute_command("CONFIG", "SET", "ts.ts-compaction-policy", policy)
        # TS.CREATE never applies the policy; the auto-creating TS.ADD does.
        self.client.execute_command("TS.ADD", "ts:policy:src", 10_000, 10_000, "LABELS", "a", "b")
        for ts in range(20_000, 300_000, 10_000):
            self.client.execute_command("TS.ADD", "ts:policy:src", ts, ts)
        rules = parse_info_response(self.client.execute_command("TS.INFO", "ts:policy:src"))["rules"]
        dests = [r.dest_key for r in rules]
        assert len(dests) == 6
        self.assert_compaction_equivalent("ts:policy:src", *dests)
        self.assert_rule_state_equivalent("ts:policy:src")

    POLICY_SOURCES = ("ts:pol:add", "ts:pol:incrby", "ts:pol:addbulk")

    def auto_create_policy_sources(self):
        """Create every POLICY_SOURCES key through a different auto-creating write."""
        self.client.execute_command("TS.ADD", "ts:pol:add", 0, 1, "LABELS", "a", "b")
        self.client.execute_command("TS.INCRBY", "ts:pol:incrby", 1, "TIMESTAMP", 0, "LABELS", "a", "b")
        self.client.execute_command(
            "TS.ADDBULK", "ts:pol:addbulk", json.dumps({"timestamps": [0], "values": [1]}),
            "LABELS", "a", "b",
        )

    def test_replication_policy_destinations_come_from_the_primary(self):
        """Policy-created destinations are replicated as explicit creates and rules; the replica's
        own ts-compaction-policy never shapes replicated data."""
        replica = self.replicas[0].client
        self.client.execute_command("CONFIG", "SET", "ts.ts-compaction-policy", "max:1M:1h;avg:2M:1h")
        replica.execute_command("CONFIG", "SET", "ts.ts-compaction-policy", "min:5M:1h")

        # An explicit TS.CREATE never applies the policy, on either node.
        self.client.execute_command("TS.CREATE", "ts:pol:explicit", "LABELS", "a", "b")
        self.auto_create_policy_sources()
        self.wait_for_replication()

        describe = lambda rules: [(r.dest_key, r.bucket_duration, r.aggregation.lower(), r.alignment) for r in rules]
        for client in (self.client, replica):
            assert parse_info_response(client.execute_command("TS.INFO", "ts:pol:explicit"))["rules"] == []
        for src in self.POLICY_SOURCES:
            primary_rules = parse_info_response(self.client.execute_command("TS.INFO", src))["rules"]
            replica_rules = parse_info_response(replica.execute_command("TS.INFO", src))["rules"]
            # Same rules in the same order: the order is part of the stored value.
            assert describe(replica_rules) == describe(primary_rules), src
            assert sorted((r.aggregation.lower(), r.bucket_duration) for r in primary_rules) == \
                [("avg", 120_000), ("max", 60_000)], src
            for r in primary_rules:
                assert replica.execute_command("EXISTS", r.dest_key) == 1, r.dest_key
                primary_dest = parse_info_response(self.client.execute_command("TS.INFO", r.dest_key))
                replica_dest = parse_info_response(replica.execute_command("TS.INFO", r.dest_key))
                assert replica_dest["sourceKey"] == primary_dest["sourceKey"] == src
                for field in ("retentionTime", "chunkSize", "labels", "duplicatePolicy"):
                    assert replica_dest[field] == primary_dest[field], (r.dest_key, field)
        assert replica.execute_command("TS.QUERYINDEX", "aggregation=min") == []

        # Writes after creation compact identically on both nodes.
        dests = []
        for src in self.POLICY_SOURCES:
            for ts in range(10_000, 300_000, 10_000):
                self.client.execute_command("TS.ADD", src, ts, ts)
            dests += [r.dest_key for r in parse_info_response(self.client.execute_command("TS.INFO", src))["rules"]]
        assert all(self.client.execute_command("TS.RANGE", d, "-", "+") != [] for d in dests)
        self.assert_compaction_equivalent(*self.POLICY_SOURCES, *dests)
        self.assert_rule_state_equivalent(*self.POLICY_SOURCES)

    def test_replication_policy_destination_stream_order(self):
        """What reaches the replica for a policy auto-create: the source's TS.CREATE, then each
        destination's TS.CREATE followed by its TS.CREATERULE, then the write itself."""
        self.client.execute_command("CONFIG", "SET", "ts.ts-compaction-policy", "max:1M:1h;avg:2M:1h")
        monitor_client = self.replicas[0].get_new_client()
        monitor_client.connection_pool.connection_kwargs["socket_timeout"] = 5

        seen = []
        with monitor_client.monitor() as monitor:
            self.client.execute_command("TS.ADD", "ts:pol:order", 0, 1, "LABELS", "a", "b")
            deadline = time.time() + 5
            while time.time() < deadline:
                entry = monitor.next_command()
                if entry is None:
                    continue
                parts = [p.strip('"') for p in entry.get("command", "").split()]
                if not parts or not parts[0].lower().startswith("ts."):
                    continue
                seen.append((parts[0].lower(), parts[1], parts[2] if len(parts) > 2 else None))
                if parts[0].lower() == "ts.add":
                    break

        rules = parse_info_response(self.client.execute_command("TS.INFO", "ts:pol:order"))["rules"]
        expected = [("ts.create", "ts:pol:order")]
        for r in rules:
            expected += [("ts.create", r.dest_key), ("ts.createrule", "ts:pol:order")]
        expected.append(("ts.add", "ts:pol:order"))
        assert [(cmd, key) for cmd, key, _ in seen] == expected, seen
        # Each TS.CREATERULE names the destination created just before it, in rule order.
        assert [dest for cmd, _, dest in seen if cmd == "ts.createrule"] == [r.dest_key for r in rules]

    def test_replication_policy_destination_already_exists_is_skipped(self):
        """A policy destination key that already exists is left alone on both nodes: no rule,
        no replicated create, the existing value untouched."""
        replica = self.replicas[0].client
        self.client.execute_command("CONFIG", "SET", "ts.ts-compaction-policy", "max:1M:1h;avg:2M:1h")
        self.client.execute_command("SET", "ts:pol:taken_MAX_60000", "mine")
        self.client.execute_command("TS.ADD", "ts:pol:taken", 0, 1)
        self.wait_for_replication()
        for client in (self.client, replica):
            rules = parse_info_response(client.execute_command("TS.INFO", "ts:pol:taken"))["rules"]
            assert [(r.aggregation.lower(), r.bucket_duration) for r in rules] == [("avg", 120_000)]
            assert client.execute_command("GET", "ts:pol:taken_MAX_60000") == b"mine"
        assert self.server.verify_string_in_logfile("ts:pol:taken_MAX_60000")

    def test_replication_compaction_full_resync_mid_bucket(self):
        """A full sync taken mid-bucket ships the open-bucket state in the RDB; the stream after
        it must continue from exactly that state."""
        src, dst = "ts:resync:src", "ts:resync:dst"
        self.rule(src, dst, "avg", 1000)
        for ts, value in ((0, 1), (300, 2.5), (450, 7), (1000, 9), (1200, 3)):
            self.client.execute_command("TS.ADD", src, ts, value)
        self.force_full_resync()
        self.assert_compaction_equivalent(src, dst)
        self.assert_rule_state_equivalent(src)
        self.client.execute_command("TS.ADD", src, 1700, 5)   # same open bucket as the snapshot
        self.client.execute_command("TS.ADD", src, 2000, 5)   # closes it
        self.client.execute_command("TS.ADD", src, 200, 100)  # back-fill
        self.client.execute_command("TS.DEL", src, 1100, 2100)
        self.assert_compaction_equivalent(src, dst)
        self.assert_rule_state_equivalent(src)

    def test_replication_compaction_reload_mid_sequence(self):
        """DEBUG RELOAD on the primary mid-bucket: its rule state round-trips through its own RDB
        while the replica keeps its in-memory copy; the two must still agree afterwards."""
        src, dst = "ts:reload:src", "ts:reload:dst"
        self.rule(src, dst, "std.s", 1000)
        for ts, value in ((0, 1), (300, 2.5), (450, 7)):
            self.client.execute_command("TS.ADD", src, ts, value)
        self.client.execute_command("DEBUG", "RELOAD")
        self.assert_compaction_equivalent(src, dst)
        self.assert_rule_state_equivalent(src)
        for ts, value in ((800, 2), (1000, 9), (100, 4)):
            self.client.execute_command("TS.ADD", src, ts, value)
        self.assert_compaction_equivalent(src, dst)
        self.assert_rule_state_equivalent(src)

    def test_replication_compaction_promotion(self):
        """Promote the replica mid-bucket, keep writing on it, and check the old primary (now a
        replica) converges. The values are exact: avg(9, 3) = 6 closes bucket 1000; the back-fill
        makes bucket 0 avg(1, 2.5, 7, 100) = 27.625; the delete of 1200 recalculates 1000 to 9."""
        src, dst = "ts:promote:src", "ts:promote:dst"
        self.rule(src, dst, "avg", 1000)
        for ts, value in ((0, 1), (300, 2.5), (450, 7), (1000, 9)):
            self.client.execute_command("TS.ADD", src, ts, value)
        self.assert_compaction_equivalent(src, dst)

        new_primary, new_replica = self.promote_replica()
        for ts, value in ((1200, 3), (2000, 5), (100, 100)):   # same open bucket, close, back-fill
            new_primary.execute_command("TS.ADD", src, ts, value)
        new_primary.execute_command("TS.DEL", src, 1100, 1300)
        assert new_primary.execute_command("TS.RANGE", dst, "-", "+") == [[0, b"27.625"], [1000, b"9"]]
        self.wait_caught_up(new_primary, new_replica)
        self.assert_equivalent_between(new_primary, new_replica, src, dst)

    def apply_fuzz_op(self, op):
        """Apply one generated op to the primary. Every generated op is valid, so a rejection is a
        regression and must fail the example, not be swallowed."""
        kind = op[0]
        if kind == "ADD":
            ts, value = op[1]
            self.client.execute_command("TS.ADD", FUZZ_SRC, ts, value)
        elif kind == "MADD":
            args = [arg for ts, value in op[1] for arg in (FUZZ_SRC, ts, value)]
            self.client.execute_command("TS.MADD", *args)
        elif kind == "ADDBULK":
            payload = json.dumps({"timestamps": [ts for ts, _ in op[1]], "values": [v for _, v in op[1]]})
            self.client.execute_command("TS.ADDBULK", FUZZ_SRC, payload, "ON_DUPLICATE", "LAST")
        else:
            lo, hi = sorted(op[1:])
            self.client.execute_command("TS.DEL", FUZZ_SRC, lo, hi)

    @settings(
        max_examples=40,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
    )
    @given(ops=st.lists(_fuzz_op, min_size=1, max_size=30))
    def test_replication_compaction_random_sequences(self, ops):
        """Replica oracle: any sequence of writes and deletes through a two-level chain leaves the
        replica with the primary's destinations. A failure is a determinism bug in the engine:
        promote Hypothesis's shrunk example into a named test next to this one."""
        self.client.execute_command("FLUSHALL")
        self.wait_for_replication()
        self.client.execute_command("TS.CREATE", FUZZ_SRC, "DUPLICATE_POLICY", "LAST")
        self.client.execute_command("TS.CREATE", FUZZ_MID)
        self.client.execute_command("TS.CREATE", FUZZ_TOP)
        self.client.execute_command("TS.CREATERULE", FUZZ_SRC, FUZZ_MID, "AGGREGATION", "sum", 1000)
        self.client.execute_command("TS.CREATERULE", FUZZ_MID, FUZZ_TOP, "AGGREGATION", "avg", 2000)
        for op in ops:
            self.apply_fuzz_op(op)
        self.assert_compaction_equivalent(FUZZ_SRC, FUZZ_MID, FUZZ_TOP)
        self.assert_rule_state_equivalent(FUZZ_SRC, FUZZ_MID)
