from valkey import Valkey, ValkeyCluster
import pytest

from common import LabelSearchResponse
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesClusterTestCase

TS1 = "ts1:{1}"
TS2 = "ts2:{2}"
TS3 = "ts3:{3}"


class TestTimeSeriesMetricNamesCME(ValkeyTimeSeriesClusterTestCase):
    @staticmethod
    def setup_test_data(client: ValkeyCluster):
        client.execute_command("TS.CREATE", TS1, "METRIC", 'cpu_usage_total{env="prod"}')
        client.execute_command("TS.CREATE", TS2, "METRIC", 'mem_usage_bytes{env="prod"}')
        client.execute_command("TS.CREATE", TS3, "METRIC", 'cpu_idle_total{env="dev"}')

    def test_metricnames_cluster_fanout(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        client: Valkey = self.new_client_for_primary(0)
        self.setup_test_data(cluster)

        result = client.execute_command(
            "TS.METRICNAMES",
            "FILTER",
            'env=~"(prod|dev)"',
        )

        labels = LabelSearchResponse.parse(result)
        names = [item.value for item in labels.results]

        assert sorted(names) == sorted([b"cpu_idle_total", b"cpu_usage_total", b"mem_usage_bytes"])

    def tag_per_primary(self, cluster_client: ValkeyCluster):
        """Return one hash tag per primary, each owned by a different node.

        The tag -> slot -> node mapping depends on how the harness splits the slot
        range across primaries, so the tags are discovered at runtime rather than
        hard-coded (e.g. {1} and {2} can land on the same primary here).
        """
        by_node = {}
        for i in range(1000):
            tag = f'tag{i}'
            node = cluster_client.get_node_from_key('{%s}' % tag)
            by_node.setdefault((node.host, node.port), tag)
            if len(by_node) == self.CLUSTER_SIZE:
                break

        assert len(by_node) == self.CLUSTER_SIZE, \
            f'found tags for only {len(by_node)} of {self.CLUSTER_SIZE} primaries'
        return [by_node[node] for node in sorted(by_node)]

    def test_metricnames_hashtag_scopes_cluster_fanout(self):
        """HASHTAG queries only the slot(s) selected by the supplied hash tag."""
        cluster: ValkeyCluster = self.new_cluster_client()
        client: Valkey = self.new_client_for_primary(0)

        tag_a, tag_b, tag_c = self.tag_per_primary(cluster)

        cluster.execute_command("TS.CREATE", f"ts1:{{{tag_a}}}", "METRIC", 'cpu_usage_total{env="prod"}')
        cluster.execute_command("TS.CREATE", f"ts2:{{{tag_b}}}", "METRIC", 'mem_usage_bytes{env="prod"}')
        cluster.execute_command("TS.CREATE", f"ts3:{{{tag_c}}}", "METRIC", 'cpu_idle_total{env="dev"}')

        result = client.execute_command("TS.METRICNAMES", "HASHTAG", tag_a, "FILTER", 'env=~".+"')
        names = [item.value for item in LabelSearchResponse.parse(result).results]
        assert names == [b"cpu_usage_total"]

        result = client.execute_command("TS.METRICNAMES", "FILTER", 'env=~".+"', "HASHTAG", f"{tag_a},{tag_c}")
        names = [item.value for item in LabelSearchResponse.parse(result).results]
        assert names == [b"cpu_idle_total", b"cpu_usage_total"]
