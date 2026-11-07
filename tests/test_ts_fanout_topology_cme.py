# tests/test_ts_fanout_topology_cme.py
"""Fanout target selection on a cluster with replicas.

* After a failover the old primary stays in `CLUSTER NODES` as a slot-less `master` until it
  rejoins or is forgotten. Fanout used to treat every master as a shard, so that dead node was
  selected on every multi-shard command and each one failed with the generic aggregate error.
* HASHTAG routing used to pick a node per *slot*, so two tags on one shard could select its
  primary and its replica and count that shard twice.
"""
import os
import signal

from valkey import Valkey
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util.waiters import wait_for_true
from valkey_timeseries_test_case import ValkeyTimeSeriesClusterTestCaseDebugMode


class TestTsFanoutTopology(ValkeyTimeSeriesClusterTestCaseDebugMode):
    CLUSTER_SIZE = 3
    REPLICAS_COUNT = 1

    def get_config_file_lines(self, test_dir, port):
        # Detect a dead node in seconds rather than the default 15 s.
        return super().get_config_file_lines(test_dir, port) + ["cluster-node-timeout 2000"]

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

    def _hash_tags_for_shard(self, client: Valkey, shard_index: int, count: int) -> list:
        """`count` hash tags with distinct slots, all owned by `shard_index`."""
        tags, slots = [], set()
        for i in range(0, 4096):
            tag = f"t{i}"
            slot = int(client.execute_command("CLUSTER KEYSLOT", tag))
            if self._shard_index_for_slot(slot) == shard_index and slot not in slots:
                tags.append(tag)
                slots.add(slot)
                if len(tags) == count:
                    return tags
        raise AssertionError(f"not enough hash tags found for shard {shard_index}")

    def test_a_failed_replica_is_not_selected(self):
        cluster = self.new_cluster_client()
        probe = self.client_for_primary(0)
        for shard_index in range(self.CLUSTER_SIZE):
            tag = self._hash_tag_for_shard(probe, shard_index)
            cluster.execute_command("TS.CREATE", f"failedreplica:{{{tag}}}", "LABELS", "suite", "failedreplica")
        for rg in self.replication_groups:
            rg.wait_for_replica_offset_to_sync_up(0)

        group = self.replication_groups[0]
        replica_id = group.get_replica_connection(0).execute_command("CLUSTER MYID")
        if isinstance(replica_id, bytes):
            replica_id = replica_id.decode()
        os.kill(group.replicas[0].server.pid(), signal.SIGKILL)
        group.replicas[0].server.server.wait()

        live = [rg.get_primary_connection() for rg in self.replication_groups]
        wait_for_true(
            lambda: all(
                any(nid == replica_id and "fail" in flags for nid, flags, _ in self._nodes(c))
                for c in live
            ),
            timeout=30,
        )
        for client in live:
            client.execute_command("TS._DEBUG", "REFRESH_CLUSTERMAP")

        # READONLY routing may pick a replica; with the dead one still a candidate, about half
        # of these failed with the generic fanout error.
        coordinator = self.new_client_for_primary(0)
        coordinator.execute_command("READONLY")
        for _ in range(20):
            assert coordinator.execute_command("TS.CARD", "FILTER", "suite=failedreplica") == 3

    def test_hashtags_on_one_shard_count_it_once(self):
        cluster = self.new_cluster_client()
        probe = self.client_for_primary(0)
        tags = self._hash_tags_for_shard(probe, 0, 2)
        cluster.execute_command("TS.CREATE", f"hashtags:{{{tags[0]}}}", "LABELS", "suite", "hashtags")
        group = self.replication_groups[0]
        group.wait_for_replica_offset_to_sync_up(0)

        # READONLY connections may route to replicas; with a node chosen per slot, the two tags
        # picked the primary and the replica half the time.
        readonly = [group.get_primary_connection(), group.get_replica_connection(0)]
        for client in readonly:
            client.execute_command("READONLY")
        for client in readonly:
            for _ in range(30):
                reply = client.execute_command(
                    "TS.CARD", "HASHTAG", ",".join(tags), "FILTER", "suite=hashtags"
                )
                assert reply == 1

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
