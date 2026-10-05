"""
TS._DEBUG STRINGPOOLSTATS in cluster mode: the coordinator sums every primary's string pool.
"""

import pytest
from valkey import ResponseError, ValkeyCluster

from valkey_timeseries_test_case import ValkeyTimeSeriesClusterTestCaseDebugMode
from valkeytestframework.conftest import resource_port_tracker


def bucket_fields(bucket):
    """Flat [key, value, ...] array -> dict keyed by field name."""
    return {bucket[i].decode(): bucket[i + 1] for i in range(0, len(bucket), 2)}


def top_k_entries(entries):
    return {e['value'].decode(): e for e in map(bucket_fields, entries)}


class TestStringPoolStatsCME(ValkeyTimeSeriesClusterTestCaseDebugMode):

    def tag_per_primary(self, cluster_client: ValkeyCluster):
        """One hash tag per primary, discovered at runtime since the slot split varies."""
        by_node = {}
        for i in range(1000):
            tag = f'tag{i}'
            node = cluster_client.get_node_from_key('{%s}' % tag)
            by_node.setdefault((node.host, node.port), tag)
            if len(by_node) == self.CLUSTER_SIZE:
                break
        assert len(by_node) == self.CLUSTER_SIZE
        ports = [self.get_primary_port(i) for i in range(self.CLUSTER_SIZE)]
        return [next(tag for (_, port), tag in by_node.items() if port == p) for p in ports]

    def populate(self, cluster_client: ValkeyCluster):
        # Every primary holds `env=prod` twice, and one long label value unique to it.
        for i, tag in enumerate(self.tag_per_primary(cluster_client)):
            for j in range(2):
                cluster_client.execute_command(
                    'TS.CREATE', f'pool:{{{tag}}}:{j}',
                    'LABELS', 'env', 'prod', 'host', f'host-{i}-{"x" * (40 + i)}-{j}')

    def local_stats(self, k):
        return [self.client_for_primary(i).execute_command('TS._DEBUG', 'STRINGPOOLSTATS', k, 'LOCAL')
                for i in range(self.CLUSTER_SIZE)]

    def test_sums_every_primary(self):
        cluster_client = self.new_cluster_client()
        self.populate(cluster_client)

        locals_ = self.local_stats(0)
        merged = self.client_for_primary(0).execute_command('TS._DEBUG', 'STRINGPOOLSTATS')
        assert len(merged) == 4

        total = bucket_fields(merged[0])
        for field in ('count', 'bytes', 'allocated'):
            assert total[field] == sum(bucket_fields(r[0])[field] for r in locals_), field

        savings = bucket_fields(merged[3])
        for field in ('memorySavedBytes', 'holders', 'holderSlotBytes', 'totalStorageBytes'):
            assert savings[field] == sum(bucket_fields(r[3])[field] for r in locals_), field
        # Ratios are recomputed from the sums, not summed.
        saved = savings['memorySavedBytes']
        expected_pct = saved / (saved + savings['totalStorageBytes']) * 100.0 if saved else 0.0
        assert float(savings['storageSavedPct']) == pytest.approx(expected_pct)

        # Buckets are summed by key.
        for index in (1, 2):
            expected = {}
            for r in locals_:
                for key, bucket in r[index]:
                    expected[key] = expected.get(key, 0) + bucket_fields(bucket)['count']
            assert {key: bucket_fields(b)['count'] for key, b in merged[index]} == expected

    def test_merges_top_k_across_primaries(self):
        cluster_client = self.new_cluster_client()
        self.populate(cluster_client)
        k = 50

        locals_ = self.local_stats(k)
        merged = self.client_for_primary(1).execute_command('TS._DEBUG', 'STRINGPOOLSTATS', k)
        assert len(merged) == 6

        by_ref = top_k_entries(merged[4])
        assert by_ref['env=prod']['refCount'] == sum(
            top_k_entries(r[4])['env=prod']['refCount'] for r in locals_)

        # The longest strings are each held by one primary; all of them make the merged list,
        # longest first.
        by_size = [bucket_fields(e) for e in merged[5]]
        sizes = [e['bytes'] for e in by_size]
        assert sizes == sorted(sizes, reverse=True)
        longest = {e['value'].decode() for e in by_size if b'x' * 40 in e['value']}
        assert len(longest) == 2 * self.CLUSTER_SIZE

    def test_local_reports_one_node(self):
        cluster_client = self.new_cluster_client()
        self.populate(cluster_client)

        local = self.client_for_primary(0).execute_command('TS._DEBUG', 'STRINGPOOLSTATS', 'LOCAL')
        merged = self.client_for_primary(0).execute_command('TS._DEBUG', 'STRINGPOOLSTATS')
        assert bucket_fields(local[0])['count'] < bucket_fields(merged[0])['count']

    def test_peer_with_debug_mode_off_fails_the_command(self):
        peer = self.client_for_primary(1)
        peer.execute_command('CONFIG', 'SET', 'ts.debug-mode', 'no')
        try:
            with pytest.raises(ResponseError):
                self.client_for_primary(0).execute_command('TS._DEBUG', 'STRINGPOOLSTATS')
        finally:
            peer.execute_command('CONFIG', 'SET', 'ts.debug-mode', 'yes')
