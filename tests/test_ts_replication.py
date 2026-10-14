import json
import logging
import os
import time
from typing import List

import pytest

from common import SERVER_PATH, get_module_path, parse_info_response
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.valkey_test_case import ReplicationTestCase

logger = logging.getLogger(__name__)

REPLICATION_TIMEOUT = 3


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

    @pytest.mark.xfail(
        strict=True,
        reason="compaction-replication-plan.md section 3.8: policy-created rules are attached in "
        "per-process HashSet order, so the source's rule order (and DEBUG DIGEST-VALUE) differs "
        "between primary and replica",
    )
    def test_replication_compaction_policy_rules(self):
        """Rules created by ts-compaction-policy, with the same policy on both nodes.

        Six rules, so that two independently shuffled rule orders coincide with probability
        1/720 and the strict xfail above is not itself flaky."""
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
