"""
TS._DEBUG in cluster mode: STRINGPOOLSTATS sums every primary's string pool, and INDEXMEMORY sums
one node per shard's label index, preferring replicas.
"""

import pytest
from valkey import ResponseError, ValkeyCluster

from common import SERVER_VERSION
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


def index_memory(client, *args):
    return bucket_fields(client.execute_command('TS._DEBUG', 'INDEXMEMORY', *args))


class TestIndexMemoryCME(ValkeyTimeSeriesClusterTestCaseDebugMode):
    REPLICAS_COUNT = 1

    # Every field a replica's index shares exactly with its primary's. `bookkeepingBytes`
    # includes the stale-id tombstones, which each node sweeps on its own schedule.
    EXACT_FIELDS = ('termsBytes', 'postingsBytes', 'idToKeyBytes', 'terms', 'series', 'databases')

    def populate(self):
        cluster_client = self.new_cluster_client()
        for i in range(60):
            cluster_client.execute_command(
                'TS.CREATE', f'idxmem:{i}', 'LABELS', 'env', 'prod', 'uniq', f'series-{i}')
        for i in range(self.CLUSTER_SIZE):
            self.get_replication_group(i).wait_for_replica_offset_to_sync_up(0)

    def replica(self, shard):
        return self.get_replication_group(shard).get_replica_connection(0)

    def test_sums_one_node_per_shard(self):
        self.populate()

        locals_ = [index_memory(self.client_for_primary(i), 'LOCAL') for i in range(self.CLUSTER_SIZE)]
        merged = index_memory(self.client_for_primary(0))

        assert merged['nodes'] == self.CLUSTER_SIZE
        assert merged['series'] == 60
        for field in self.EXACT_FIELDS:
            assert merged[field] == sum(r[field] for r in locals_), field
        assert merged['totalBytes'] == (
            merged['termsBytes'] + merged['postingsBytes']
            + merged['idToKeyBytes'] + merged['bookkeepingBytes'])

    def test_replica_mirrors_its_primary(self):
        self.populate()
        for i in range(self.CLUSTER_SIZE):
            primary = index_memory(self.client_for_primary(i), 'LOCAL')
            replica = index_memory(self.replica(i), 'LOCAL')
            for field in self.EXACT_FIELDS:
                assert replica[field] == primary[field], (i, field)

    def test_reads_from_replicas(self):
        """With debug-mode off on every other primary, only their replicas can answer."""
        self.populate()
        peers = [self.client_for_primary(i) for i in range(1, self.CLUSTER_SIZE)]
        for peer in peers:
            peer.execute_command('CONFIG', 'SET', 'ts.debug-mode', 'no')
        try:
            merged = index_memory(self.client_for_primary(0))
            assert merged['nodes'] == self.CLUSTER_SIZE
            assert merged['series'] == 60
        finally:
            for peer in peers:
                peer.execute_command('CONFIG', 'SET', 'ts.debug-mode', 'yes')

    def test_replica_with_debug_mode_off_fails_the_command(self):
        replica = self.replica(1)
        replica.execute_command('CONFIG', 'SET', 'ts.debug-mode', 'no')
        try:
            with pytest.raises(ResponseError):
                index_memory(self.client_for_primary(0))
        finally:
            replica.execute_command('CONFIG', 'SET', 'ts.debug-mode', 'yes')

    def test_local_reports_one_node(self):
        self.populate()
        local = index_memory(self.client_for_primary(0), 'LOCAL')
        merged = index_memory(self.client_for_primary(0))
        assert local['nodes'] == 1
        assert local['series'] < merged['series']


def fanout_stats(client):
    """This node's `fanout` section of TS._DEBUG STATS, as a dict."""
    flat = client.execute_command('TS._DEBUG', 'STATS', 'fanout')
    return {flat[i].decode(): flat[i + 1] for i in range(0, len(flat), 2)}


class TestFanoutWireStatsCME(ValkeyTimeSeriesClusterTestCaseDebugMode):
    """Fanout messages and payload bytes are counted by the node that sends them."""

    def populate(self, samples):
        cluster_client = self.new_cluster_client()
        for i in range(30):
            key = f'wire:{i}'
            cluster_client.execute_command('TS.CREATE', key, 'LABELS', 'env', 'prod', 'uniq', f's{i}')
            for ts in range(1, samples + 1):
                cluster_client.execute_command('TS.ADD', key, ts, ts * 1.5)

    def primaries(self):
        return [self.client_for_primary(i) for i in range(self.CLUSTER_SIZE)]

    def reset_all(self):
        for client in self.primaries():
            assert client.execute_command('TS._DEBUG', 'STATS', 'RESET') == b'OK'

    def mrange(self, *filters):
        """Runs TS.MRANGE with node 0 as coordinator; returns each node's fanout stats."""
        self.reset_all()
        self.client_for_primary(0).execute_command('TS.MRANGE', '-', '+', 'FILTER', *filters)
        return [fanout_stats(client) for client in self.primaries()]

    def test_requests_and_responses_are_counted_where_they_are_sent(self):
        self.populate(samples=5)
        coordinator, *peers = self.mrange('env=prod')

        # One request to each peer; the local share never touches the bus.
        assert coordinator['fanout_requests_sent_total'] == self.CLUSTER_SIZE - 1
        assert coordinator['fanout_request_sent_bytes_total'] > 0
        assert coordinator['fanout_responses_sent_total'] == 0
        assert coordinator['fanout_response_sent_bytes_total'] == 0

        for peer in peers:
            assert peer['fanout_requests_sent_total'] == 0
            assert peer['fanout_responses_sent_total'] == 1
            assert peer['fanout_response_sent_bytes_total'] > 0
            assert peer['fanout_error_responses_sent_total'] == 0

    def test_bytes_follow_the_payload(self):
        self.populate(samples=200)
        empty = self.mrange('env=nowhere')
        full = self.mrange('env=prod')

        # The same query shape costs the same to send, whatever it matches.
        assert full[0]['fanout_request_sent_bytes_total'] == pytest.approx(
            empty[0]['fanout_request_sent_bytes_total'], abs=16)
        # Answers carrying samples cost more than empty ones.
        for i in range(1, self.CLUSTER_SIZE):
            assert full[i]['fanout_response_sent_bytes_total'] > \
                empty[i]['fanout_response_sent_bytes_total'] + 100, i

    def test_error_responses_are_counted_apart(self):
        """A peer with debug-mode off answers TS._DEBUG INDEXMEMORY with an error."""
        self.reset_all()
        peer = self.client_for_primary(1)
        peer.execute_command('CONFIG', 'SET', 'ts.debug-mode', 'no')
        try:
            with pytest.raises(ResponseError):
                self.client_for_primary(0).execute_command('TS._DEBUG', 'INDEXMEMORY')
        finally:
            peer.execute_command('CONFIG', 'SET', 'ts.debug-mode', 'yes')

        stats = fanout_stats(peer)
        assert stats['fanout_error_responses_sent_total'] == 1
        assert stats['fanout_error_response_sent_bytes_total'] > 0
        assert stats['fanout_responses_sent_total'] == 0


def server_major_version():
    """`unstable` is ahead of every release."""
    head = SERVER_VERSION.split('.')[0]
    return int(head) if head.isdigit() else 1 << 30


@pytest.mark.skipif(server_major_version() < 9, reason='cluster-databases needs Valkey >= 9.0')
class TestIndexMemoryMultiDbCME(ValkeyTimeSeriesClusterTestCaseDebugMode):
    """Each node measures the coordinator's selected database, which travels with the request."""
    REPLICAS_COUNT = 1
    SERIES_PER_SHARD = {0: 4, 1: 9}

    def get_config_file_lines(self, test_dir, port):
        return super().get_config_file_lines(test_dir, port) + ['cluster-databases 16']

    def tag_for_shard(self, client, shard):
        start, end = self._split_range_pairs(0, 16384, self.CLUSTER_SIZE)[shard]
        for i in range(4096):
            tag = f't{i}'
            if start <= int(client.execute_command('CLUSTER KEYSLOT', tag)) < end:
                return tag
        raise AssertionError(f'no hash tag for shard {shard}')

    def populate(self):
        for shard in range(self.CLUSTER_SIZE):
            primary = self.new_client_for_primary(shard)
            tag = self.tag_for_shard(primary, shard)
            for db, count in self.SERIES_PER_SHARD.items():
                primary.select(db)
                for i in range(count):
                    primary.execute_command(
                        'TS.CREATE', f'idxmem:{{{tag}}}:{db}:{i}',
                        'LABELS', 'env', f'db{db}', 'uniq', f's{i}')
            self.get_replication_group(shard).wait_for_replica_offset_to_sync_up(0)

    def test_defaults_to_the_selected_db(self):
        self.populate()
        coordinator = self.new_client_for_primary(0)

        replies = {}
        for db in self.SERIES_PER_SHARD:
            coordinator.select(db)
            replies[db] = index_memory(coordinator)
        all_dbs = index_memory(coordinator, 'ALLDBS')

        for db, count in self.SERIES_PER_SHARD.items():
            assert replies[db]['series'] == count * self.CLUSTER_SIZE, db
            assert replies[db]['databases'] == self.CLUSTER_SIZE, db
            assert replies[db]['nodes'] == self.CLUSTER_SIZE, db
        assert all_dbs['series'] == sum(self.SERIES_PER_SHARD.values()) * self.CLUSTER_SIZE
        assert all_dbs['databases'] == len(self.SERIES_PER_SHARD) * self.CLUSTER_SIZE
        for field in ('termsBytes', 'postingsBytes', 'idToKeyBytes', 'terms', 'series'):
            assert sum(r[field] for r in replies.values()) == all_dbs[field], field

        # An empty database reports zero everywhere.
        coordinator.select(5)
        assert index_memory(coordinator)['series'] == 0
