# tests/test_ts_mdel_cluster.py
import pytest
from valkey import ResponseError, ValkeyCluster
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesClusterTestCase

TS1 = b"mdel:ts1:{1}"
TS2 = b"mdel:ts2:{2}"
TS3 = b"mdel:ts3:{3}"


class TestTsMDelCluster(ValkeyTimeSeriesClusterTestCase):
    def _create_series(self):
        cluster: ValkeyCluster = self.new_cluster_client()

        cluster.execute_command("TS.CREATE", TS1, "LABELS", "name", "cpu", "node", "node1")
        cluster.execute_command("TS.CREATE", TS2, "LABELS", "name", "cpu", "node", "node2")
        cluster.execute_command("TS.CREATE", TS3, "LABELS", "name", "mem", "node", "node3")

        for ts in (TS1, TS2, TS3):
            cluster.execute_command("TS.ADD", ts, 1, 10)
            cluster.execute_command("TS.ADD", ts, 2, 20)
            cluster.execute_command("TS.ADD", ts, 3, 30)
            cluster.execute_command("TS.ADD", ts, 4, 40)
            cluster.execute_command("TS.ADD", ts, 5, 50)

    def test_mdel_series_deletion_cme(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)

        self._create_series()

        res = client.execute_command("TS.MDEL", "FILTER", "name=cpu")
        assert int(res) == 2

        assert cluster.execute_command("EXISTS", TS1) == 0
        assert cluster.execute_command("EXISTS", TS2) == 0
        assert cluster.execute_command("EXISTS", TS3) == 1

        mem_keys = client.execute_command("TS.QUERYINDEX", "name=mem")
        assert mem_keys == [TS3]

    def test_mdel_range_deletion_cme(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)

        self._create_series()

        res = client.execute_command("TS.MDEL", 2, 4, "FILTER", "name=cpu")
        assert int(res) == 6

        for ts in (TS1, TS2):
            vals = cluster.execute_command("TS.RANGE", ts, "-", "+")
            assert vals == [[1, b"10"], [5, b"50"]]

        vals3 = cluster.execute_command("TS.RANGE", TS3, "-", "+")
        assert vals3 == [[1, b"10"], [2, b"20"], [3, b"30"], [4, b"40"], [5, b"50"]]

    def test_mdel_no_matches_cme(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)

        self._create_series()

        res = client.execute_command("TS.MDEL", "FILTER", "name=does_not_exist")
        assert int(res) == 0

        assert cluster.execute_command("EXISTS", TS1) == 1
        assert cluster.execute_command("EXISTS", TS2) == 1
        assert cluster.execute_command("EXISTS", TS3) == 1

    def test_mdel_error_missing_filter(self):
        client = self.new_client_for_primary(0)
        with pytest.raises(ResponseError):
            client.execute_command("TS.MDEL", 1, 2)

    def test_mdel_refused_in_multi_and_lua_cme(self):
        # Inside MULTI or Lua the server cannot block the client, so the fanout
        # must be refused outright; before this check it still ran and deleted
        # on every shard while the client saw only a "blocking command" error.
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)

        self._create_series()

        pipe = client.pipeline(transaction=True)
        pipe.execute_command("TS.MDEL", "FILTER", "name=cpu")
        with pytest.raises(ResponseError, match="MULTI or Lua"):
            pipe.execute()

        with pytest.raises(ResponseError, match="MULTI or Lua"):
            client.execute_command(
                "EVAL", "return redis.call('TS.MDEL', 'FILTER', 'name=cpu')", 0
            )

        for ts in (TS1, TS2, TS3):
            assert cluster.execute_command("EXISTS", ts) == 1
