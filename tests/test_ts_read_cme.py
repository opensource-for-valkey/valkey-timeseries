"""Cluster-mode tests for TS.READ.

TS.READ is a plain single-key command in cluster mode: there is no fanout, no coordinator state,
and nothing replicated or persisted. Almost nothing about it is therefore worth re-asserting on a
cluster — the standalone suite in test_ts_read.py already covers the semantics, and re-running
them through a cluster client only adds network hops. Each test here builds its own three-node
cluster (`setup_test` is a function-scoped autouse fixture), so a test that proves nothing new
costs a full cluster spin-up.

Two things survive that filter, because for each one a cluster is the only place the failure can
appear:

1. The declared `key_spec` — the reason "single-key" is true at all. It is hand-written per
   command, so a wrong index or range is a per-command typo that no shared harness catches.
2. What happens to an *already blocked* client when its slot moves. This is not a property of
   TS.READ so much as of the decision to block via the server's blocked-on-keys machinery.

Deliberately not covered here: routing correctness for a second key on another shard, an empty
read of a missing key, and a blocked reader woken by a write to the same shard. All three are the
standalone behavior with extra hops — the wakeup one in particular routes its write back to the
very node the reader is parked on, making it the standalone test in cluster clothing.

Only `_BlockedReader` and `REPLY_TIMEOUT` are imported from the standalone suite; the test classes
there are not re-collected here.
"""

import pytest
from valkey import ResponseError, ValkeyCluster
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util.waiters import wait_for_equal

from test_ts_read import REPLY_TIMEOUT, _BlockedReader
from valkey_timeseries_test_case import ValkeyTimeSeriesClusterTestCase

# A hash tag pins the key to one shard so a test can name the node that must own it.
TS1 = "ts:{1}:cpu"


class TsReadClusterBase(ValkeyTimeSeriesClusterTestCase):
    """Shared lookups for naming the primary that owns a key, and one that does not."""

    def primary_for_key(self, cluster, key):
        """The primary server handle that owns `key`."""
        port = cluster.get_node_from_key(key).port
        return self.primary_on_port(port)

    def primary_on_port(self, port, invert=False):
        """The primary listening on `port`, or (with `invert`) any primary that is not it."""
        for index in range(self.CLUSTER_SIZE):
            primary = self.replication_groups[index].primary.server
            if (primary.port == port) != invert:
                return primary
        raise AssertionError(
            f"no primary in the test cluster {'other than' if invert else 'on'} port {port}"
        )


class TestTsReadKeySpec(TsReadClusterBase):
    """The key spec at ts_read.rs is what makes TS.READ routable.

    The sharp assertion is the redirect, not the happy path: a cluster client that reaches the
    right shard only proves the *client* worked something out. A node replying MOVED proves the
    *server* extracted the key from the command's declared spec. With a missing or misindexed
    spec the non-owning node would find no key to route on and would serve the read locally,
    handing back an empty array for a series that exists on another shard — a wrong answer, and a
    silent one.
    """

    def test_a_non_owning_primary_redirects_instead_of_reading_locally(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        cluster.execute_command("TS.ADD", TS1, 100, 10)
        cluster.execute_command("TS.ADD", TS1, 200, 20)

        # Routed by the key spec, the read lands on the owner and sees the samples.
        assert cluster.execute_command("TS.READ", TS1, "-") == [[100, b"10"], [200, b"20"]]

        owner = self.primary_for_key(cluster, TS1)
        stranger = self.primary_on_port(owner.port, invert=True).get_new_client()
        slot = stranger.execute_command("CLUSTER", "KEYSLOT", TS1)

        with pytest.raises(ResponseError, match="MOVED") as exc_info:
            stranger.execute_command("TS.READ", TS1, "-")
        assert str(slot) in str(exc_info.value), (
            f"MOVED should name the key's slot {slot}: {exc_info.value}"
        )
        assert str(owner.port) in str(exc_info.value), (
            f"MOVED should point at the owner (port {owner.port}): {exc_info.value}"
        )


class TestTsReadSlotMigration(TsReadClusterBase):
    """A blocked client whose slot migrates away is redirected by the server.

    `clusterRedirectBlockedClientIfNeeded` explicitly covers `BLOCKED_MODULE` clients, gated on
    `moduleClientIsBlockedOnKeys(c)` — precisely the kind of block TS.READ creates. When the
    cluster configuration changes, valkey checks every blocked client and unblocks any whose slot 
    this node no longer serves, replying MOVED (or a cluster-down error for an unassigned slot).

    So the decision is: **rely on the server, add nothing.** Blocking on keys is what buys this;
    a module that rolled its own waiting (a worker thread parked on a condvar, say) would have
    gotten no redirect and would have needed the cluster event handlers the plan contemplated.
    Nothing in this module hooks slot migration, and nothing should.

    That is also why this test cannot be dropped as "single-node behavior": it does not test
    TS.READ's semantics, it pins the blocking *mechanism*. Swap block_on_keys for any hand-rolled
    wait and every standalone test still passes while this client hangs through a migration, or
    worse, wakes to serve an empty snapshot for a slot the node no longer owns.

    The non-blocking form is unaffected either way — it resolves before it could be redirected.
    """

    def test_a_blocked_reader_is_redirected_when_its_slot_moves(self):
        cluster: ValkeyCluster = self.new_cluster_client()

        # Block on a *missing* key. An empty slot is what makes the ownership handoff below
        # legal: a node still holding keys in the slot refuses SETSLOT outright.
        owner = self.primary_for_key(cluster, TS1)
        other = self.primary_on_port(owner.port, invert=True)
        control = owner.get_new_client()

        slot = control.execute_command("CLUSTER", "KEYSLOT", TS1)
        target_id = other.get_new_client().execute_command("CLUSTER", "MYID")
        if isinstance(target_id, bytes):
            target_id = target_id.decode()

        blocked_clients = lambda: control.info("clients")["blocked_clients"]
        # BLOCK 0: the redirect is the only thing that can end this client, so a timeout cannot
        # be mistaken for the behavior under test.
        reader = _BlockedReader(owner, ("TS.READ", TS1, "-", "BLOCK", 0, 1)).start()
        try:
            wait_for_equal(blocked_clients, 1, timeout=REPLY_TIMEOUT)

            # Hand the slot to another primary out from under the waiting client.
            control.execute_command("CLUSTER", "SETSLOT", slot, "NODE", target_id)

            # The reader is released with a MOVED pointing at the new owner — it neither hangs
            # nor receives a misleading empty snapshot for a key it no longer owns.
            with pytest.raises(ResponseError, match="MOVED") as exc_info:
                reader.result()
            assert str(slot) in str(exc_info.value), (
                f"MOVED should name the migrated slot {slot}: {exc_info.value}"
            )
            assert str(other.port) in str(exc_info.value), (
                f"MOVED should point at the new owner (port {other.port}): {exc_info.value}"
            )

            wait_for_equal(blocked_clients, 0, timeout=REPLY_TIMEOUT)
            assert control.execute_command("PING")
        finally:
            reader.close(control)
