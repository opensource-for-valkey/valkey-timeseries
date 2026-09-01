# tests/test_ts_mdel_replication_cme.py
"""Cluster-mode replication coverage for TS.MDEL.

TS.MDEL is the module's only fanout command that writes, and the clustered path had two defects
that together diverged every replica in the cluster:

  * the fanout inherited the `FanoutClientCommand` default target mode, which picks one *random*
    node per shard -- primaries and replicas alike -- so a shard's deletes could be applied
    directly on a replica while its primary kept the data; and
  * the clustered branch returned before any replication at all, so the deletes a primary *did*
    apply never reached that primary's replicas.

Neither was observable from the rest of the suite: `ValkeyTimeSeriesClusterTestCase` defaults to
`REPLICAS_COUNT = 0`, and with no replicas both bugs are inert. These tests run three shards with
one replica each and compare every replica against its own primary.
"""
import pytest
from valkey import Valkey, ValkeyCluster
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesClusterTestCase

SAMPLES = [(1, 10.0), (2, 20.0), (3, 30.0), (4, 40.0), (5, 50.0)]


class TestTsMDelReplicationCluster(ValkeyTimeSeriesClusterTestCase):
    CLUSTER_SIZE = 3
    REPLICAS_COUNT = 1

    # ------------------------------------------------------------------ helpers

    def _shard_index_for_slot(self, slot: int) -> int:
        """Which shard owns `slot`, given the even split the harness performs at setup."""
        for idx, (start, end) in enumerate(self._split_range_pairs(0, 16384, self.CLUSTER_SIZE)):
            if start <= slot < end:
                return idx
        raise AssertionError(f"slot {slot} is outside every shard range")

    def _hash_tag_for_shard(self, client: Valkey, shard_index: int) -> str:
        """A hash tag whose slot is owned by `shard_index`."""
        for i in range(0, 4096):
            tag = f"t{i}"
            slot = int(client.execute_command("CLUSTER KEYSLOT", tag))
            if self._shard_index_for_slot(slot) == shard_index:
                return tag
        raise AssertionError(f"no hash tag found for shard {shard_index}")

    def _create_series(self, cluster: ValkeyCluster, tags: list) -> dict:
        """One `cpu` series and one `mem` series per shard, so every shard takes part in the
        delete. Returns {shard_index: (cpu_key, mem_key)}."""
        keys = {}
        for shard_index, tag in enumerate(tags):
            cpu = f"mdelrepl:cpu:{{{tag}}}"
            mem = f"mdelrepl:mem:{{{tag}}}"
            cluster.execute_command("TS.CREATE", cpu, "LABELS", "name", "cpu")
            cluster.execute_command("TS.CREATE", mem, "LABELS", "name", "mem")
            for key in (cpu, mem):
                for ts, value in SAMPLES:
                    cluster.execute_command("TS.ADD", key, ts, value)
            keys[shard_index] = (cpu, mem)
        return keys

    def _sync_all_replicas(self):
        for rg in self.replication_groups:
            rg.wait_for_replica_offset_to_sync_up(0)

    def _primary_dbsizes(self) -> list:
        return [rg.get_primary_connection().execute_command("DBSIZE") for rg in self.replication_groups]

    def _readonly_replica(self, shard_index: int) -> Valkey:
        """A replica connection that serves its shard's slots locally instead of redirecting."""
        replica = self.replication_groups[shard_index].get_replica_connection(0)
        replica.execute_command("READONLY")
        return replica

    def _flush_cluster(self, cluster: ValkeyCluster):
        for rg in self.replication_groups:
            rg.get_primary_connection().execute_command("FLUSHALL")

    # ------------------------------------------------------------------- tests

    def test_mdel_series_deletion_reaches_replicas(self):
        """A whole-series TS.MDEL must leave every replica agreeing with its own primary."""
        cluster = self.new_cluster_client()
        coordinator = self.new_client_for_primary(0)

        tags = [self._hash_tag_for_shard(coordinator, i) for i in range(self.CLUSTER_SIZE)]
        self._create_series(cluster, tags)
        self._sync_all_replicas()

        assert sum(self._primary_dbsizes()) == 2 * self.CLUSTER_SIZE

        deleted = coordinator.execute_command("TS.MDEL", "FILTER", "name=cpu")
        assert int(deleted) == self.CLUSTER_SIZE

        # The primaries applied it (this is the targeting half of MDEL-1: a slice routed to a
        # replica would leave that shard's primary untouched).
        assert sum(self._primary_dbsizes()) == self.CLUSTER_SIZE

        self._sync_all_replicas()

        # ...and so did every replica (the replication half).
        for shard_index, rg in enumerate(self.replication_groups):
            primary_size = rg.get_primary_connection().execute_command("DBSIZE")
            replica_size = rg.get_replica_connection(0).execute_command("DBSIZE")
            assert replica_size == primary_size, (
                f"shard {shard_index}: replica holds {replica_size} keys but its primary holds "
                f"{primary_size} -- the TS.MDEL deletion did not replicate"
            )

    def test_mdel_range_deletion_reaches_replicas(self):
        """A range TS.MDEL must remove the same samples on each replica as on its primary."""
        cluster = self.new_cluster_client()
        coordinator = self.new_client_for_primary(0)

        tags = [self._hash_tag_for_shard(coordinator, i) for i in range(self.CLUSTER_SIZE)]
        keys = self._create_series(cluster, tags)
        self._sync_all_replicas()

        deleted = coordinator.execute_command("TS.MDEL", 2, 4, "FILTER", "name=cpu")
        # Three samples (2, 3, 4) removed from one cpu series per shard.
        assert int(deleted) == 3 * self.CLUSTER_SIZE

        self._sync_all_replicas()

        for shard_index, (cpu, mem) in keys.items():
            primary = self.replication_groups[shard_index].get_primary_connection()
            replica = self._readonly_replica(shard_index)

            assert primary.execute_command("TS.RANGE", cpu, "-", "+") == [[1, b"10"], [5, b"50"]]
            for key in (cpu, mem):
                assert replica.execute_command("TS.RANGE", key, "-", "+") == primary.execute_command(
                    "TS.RANGE", key, "-", "+"
                ), f"shard {shard_index}: replica disagrees with its primary on {key}"

    def test_mdel_always_deletes_on_the_primary(self):
        """Every shard's slice must land on that shard's primary, on every attempt.

        The former default target mode chose uniformly between a shard's primary and its replica,
        so a single round left at least one primary untouched roughly seven times in eight. Five
        rounds make an accidental pass vanishingly unlikely while keeping the test quick.
        """
        cluster = self.new_cluster_client()
        coordinator = self.new_client_for_primary(0)
        tags = [self._hash_tag_for_shard(coordinator, i) for i in range(self.CLUSTER_SIZE)]

        for round_index in range(5):
            self._flush_cluster(cluster)
            self._create_series(cluster, tags)

            deleted = coordinator.execute_command("TS.MDEL", "FILTER", "name=cpu")
            assert int(deleted) == self.CLUSTER_SIZE

            sizes = self._primary_dbsizes()
            assert all(size == 1 for size in sizes), (
                f"round {round_index}: primaries hold {sizes}; every shard should have had its "
                f"cpu series deleted and kept its mem series"
            )

        # The replica guard must never have fired: with primary-only targeting no shard's slice
        # is ever delivered to a replica.
        for rg in self.replication_groups:
            log = rg.replicas[0].logfile
            with open(log, "rb") as handle:
                contents = handle.read().decode("utf-8", "replace")
            assert "refusing to apply a fanout write on a replica" not in contents
