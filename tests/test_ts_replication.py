import logging
import os
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
