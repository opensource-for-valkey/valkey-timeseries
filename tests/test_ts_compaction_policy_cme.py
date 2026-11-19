from valkey import ValkeyCluster
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesClusterTestCase


class TestCompactionPolicyCluster(ValkeyTimeSeriesClusterTestCase):
    """`ts-compaction-policy` in cluster mode, where a destination must share its source's slot."""

    def _set_policy_everywhere(self, policy):
        for i in range(self.CLUSTER_SIZE):
            self.client_for_primary(i).execute_command(
                "CONFIG", "SET", "ts.ts-compaction-policy", policy
            )

    def _skipped_total(self):
        return sum(
            self.client_for_primary(i).info("ts_compaction")["ts_compaction_policy_skipped_total"]
            for i in range(self.CLUSTER_SIZE)
        )

    def _rules(self, client, key):
        info = client.execute_command("TS.INFO", key)
        info = dict(zip(info[::2], info[1::2]))
        return [rule[0] for rule in info[b"rules"]]

    def test_destination_in_another_slot_is_skipped(self):
        """An untagged source's destination (`cpu_AVG_60000`) hashes to a different slot, which
        the source's node may not own. It used to be created there anyway, out of the cluster's
        reach (MOVED, then 'key does not exist'). It is now skipped and counted, and the source
        is created without that rule."""
        self._set_policy_everywhere("avg:1M:1d")
        cluster: ValkeyCluster = self.new_cluster_client()
        assert cluster.keyslot("cpu") != cluster.keyslot("cpu_AVG_60000")

        before = self._skipped_total()
        cluster.execute_command("TS.ADD", "cpu", 1000, 1)
        assert self._rules(cluster, "cpu") == []
        # KEYS reports what each node holds, whatever the slot (EXISTS would be redirected).
        for i in range(self.CLUSTER_SIZE):
            assert self.client_for_primary(i).keys("cpu_AVG_60000") == []
        assert self._skipped_total() == before + 1

    def test_tagged_source_keeps_its_destination(self):
        """A source with a hash tag shares its slot with `{tag}..._AVG_60000`, so the policy
        applies as on a standalone server."""
        self._set_policy_everywhere("avg:1M:1d")
        cluster: ValkeyCluster = self.new_cluster_client()
        assert cluster.keyslot("{cpu}.load") == cluster.keyslot("{cpu}.load_AVG_60000")

        before = self._skipped_total()
        cluster.execute_command("TS.ADD", "{cpu}.load", 1000, 1)
        assert self._rules(cluster, "{cpu}.load") == [b"{cpu}.load_AVG_60000"]
        info = cluster.execute_command("TS.INFO", "{cpu}.load_AVG_60000")
        info = dict(zip(info[::2], info[1::2]))
        assert info[b"sourceKey"] == b"{cpu}.load"
        assert self._skipped_total() == before
