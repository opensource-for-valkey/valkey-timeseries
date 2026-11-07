# tests/test_ts_failover_cme.py
"""Fanout after a primary failover.

After a failover the old primary stays in `CLUSTER NODES` as a slot-less `master` until it
rejoins or is forgotten. Fanout used to treat every master as a shard, so that dead node was
selected on every multi-shard command and each one failed with the generic aggregate error.
"""
import os
import signal

from valkey import Valkey
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util.waiters import wait_for_true
from valkey_timeseries_test_case import ValkeyTimeSeriesClusterTestCaseDebugMode


class TestTsFanoutAfterFailover(ValkeyTimeSeriesClusterTestCaseDebugMode):
    CLUSTER_SIZE = 3
    REPLICAS_COUNT = 1

    def _shard_index_for_slot(self, slot: int) -> int:
        for idx, (start, end) in enumerate(self._split_range_pairs(0, 16384, self.CLUSTER_SIZE)):
            if start <= slot < end:
                return idx
        raise AssertionError(f"slot {slot} is outside every shard range")

    def _hash_tag_for_shard(self, client: Valkey, shard_index: int) -> str:
        for i in range(0, 4096):
            tag = f"t{i}"
            if self._shard_index_for_slot(int(client.execute_command("CLUSTER KEYSLOT", tag))) == shard_index:
                return tag
        raise AssertionError(f"no hash tag found for shard {shard_index}")

    @staticmethod
    def _nodes(client: Valkey) -> list:
        """CLUSTER NODES as (id, flags, [(start, end)]) tuples, from the client's parsed reply."""
        parsed = client.execute_command("CLUSTER NODES")
        return [
            (
                node["node_id"],
                node["flags"].split(","),
                # A single slot is parsed as a one-element list.
                [(int(r[0]), int(r[-1])) for r in node["slots"]],
            )
            for node in parsed.values()
        ]

    @classmethod
    def _owner_of_slot(cls, client: Valkey, slot: int):
        for node_id, flags, ranges in cls._nodes(client):
            if "master" in flags and any(start <= slot <= end for start, end in ranges):
                return node_id
        return None

    @classmethod
    def _is_slotless_master(cls, client: Valkey, node_id: str) -> bool:
        return any(
            nid == node_id and "master" in flags and not ranges
            for nid, flags, ranges in cls._nodes(client)
        )

    def test_fanout_after_primary_failover(self):
        cluster = self.new_cluster_client()
        probe = self.client_for_primary(0)
        for shard_index in range(self.CLUSTER_SIZE):
            tag = self._hash_tag_for_shard(probe, shard_index)
            key = f"failover:{{{tag}}}"
            cluster.execute_command("TS.CREATE", key, "LABELS", "suite", "failover")
            cluster.execute_command("TS.ADD", key, 1, shard_index)
        for rg in self.replication_groups:
            rg.wait_for_replica_offset_to_sync_up(0)

        for rg in self.replication_groups:
            assert rg.get_primary_connection().execute_command("TS.CARD", "FILTER", "suite=failover") == 3

        # Kill shard 0's primary outright and promote its replica without the old primary's
        # agreement, as an operator would after losing the node.
        failed_group = self.replication_groups[0]
        old_primary_id = failed_group.get_primary_connection().execute_command("CLUSTER MYID")
        if isinstance(old_primary_id, bytes):
            old_primary_id = old_primary_id.decode()
        os.kill(failed_group.primary.server.pid(), signal.SIGKILL)
        failed_group.primary.server.server.wait()

        promoted = failed_group.get_replica_connection(0)
        promoted.execute_command("CLUSTER FAILOVER", "TAKEOVER")
        promoted_id = promoted.execute_command("CLUSTER MYID")
        if isinstance(promoted_id, bytes):
            promoted_id = promoted_id.decode()

        live = [promoted] + [rg.get_primary_connection() for rg in self.replication_groups[1:]]
        # Every live node routes shard 0's slots to the promoted replica, and still lists the
        # old primary as a slot-less master: the state that used to break every fanout.
        wait_for_true(
            lambda: all(
                self._owner_of_slot(c, 0) == promoted_id and self._is_slotless_master(c, old_primary_id)
                for c in live
            ),
            timeout=30,
        )
        for client in live:
            client.execute_command("TS._DEBUG", "REFRESH_CLUSTERMAP")

        for client in live:
            for _ in range(5):
                assert client.execute_command("TS.CARD", "FILTER", "suite=failover") == 3
            reply = client.execute_command("TS.MGET", "FILTER", "suite=failover")
            assert len(reply) == 3
