"""
TS._DEBUG in cluster mode. Every subcommand reports the connected node unless given CLUSTER: then
STRINGPOOLSTATS sums every primary's string pool, INDEXMEMORY sums one node per shard's label
index, preferring replicas, and STATS covers every node.
"""

import threading
import time

import pytest
from valkey import ResponseError, Valkey, ValkeyCluster

from common import SERVER_VERSION
from valkey_timeseries_test_case import ValkeyTimeSeriesClusterTestCaseDebugMode
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util.waiters import wait_for_true


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
        return [self.client_for_primary(i).execute_command('TS._DEBUG', 'STRINGPOOLSTATS', k)
                for i in range(self.CLUSTER_SIZE)]

    def test_sums_every_primary(self):
        cluster_client = self.new_cluster_client()
        self.populate(cluster_client)

        locals_ = self.local_stats(0)
        merged = self.client_for_primary(0).execute_command('TS._DEBUG', 'STRINGPOOLSTATS', 'CLUSTER')
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
        merged = self.client_for_primary(1).execute_command('TS._DEBUG', 'STRINGPOOLSTATS', k, 'CLUSTER')
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

    def test_default_reports_one_node(self):
        cluster_client = self.new_cluster_client()
        self.populate(cluster_client)

        local = self.client_for_primary(0).execute_command('TS._DEBUG', 'STRINGPOOLSTATS')
        merged = self.client_for_primary(0).execute_command('TS._DEBUG', 'STRINGPOOLSTATS', 'CLUSTER')
        assert bucket_fields(local[0])['count'] < bucket_fields(merged[0])['count']

    def test_peer_with_debug_mode_off_fails_the_command(self):
        peer = self.client_for_primary(1)
        peer.execute_command('CONFIG', 'SET', 'ts.debug-mode', 'no')
        try:
            with pytest.raises(ResponseError):
                self.client_for_primary(0).execute_command('TS._DEBUG', 'STRINGPOOLSTATS', 'CLUSTER')
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

        locals_ = [index_memory(self.client_for_primary(i)) for i in range(self.CLUSTER_SIZE)]
        merged = index_memory(self.client_for_primary(0), 'CLUSTER')

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
            primary = index_memory(self.client_for_primary(i))
            replica = index_memory(self.replica(i))
            for field in self.EXACT_FIELDS:
                assert replica[field] == primary[field], (i, field)

    def test_reads_from_replicas(self):
        """With debug-mode off on every other primary, only their replicas can answer."""
        self.populate()
        peers = [self.client_for_primary(i) for i in range(1, self.CLUSTER_SIZE)]
        for peer in peers:
            peer.execute_command('CONFIG', 'SET', 'ts.debug-mode', 'no')
        try:
            merged = index_memory(self.client_for_primary(0), 'CLUSTER')
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
                index_memory(self.client_for_primary(0), 'CLUSTER')
        finally:
            replica.execute_command('CONFIG', 'SET', 'ts.debug-mode', 'yes')

    def test_default_reports_one_node(self):
        self.populate()
        local = index_memory(self.client_for_primary(0))
        merged = index_memory(self.client_for_primary(0), 'CLUSTER')
        assert local['nodes'] == 1
        assert local['series'] < merged['series']


def fanout_stats(client):
    """This node's own `fanout` section of TS._DEBUG STATS, as a dict."""
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
                self.client_for_primary(0).execute_command('TS._DEBUG', 'INDEXMEMORY', 'CLUSTER')
        finally:
            peer.execute_command('CONFIG', 'SET', 'ts.debug-mode', 'yes')

        stats = fanout_stats(peer)
        assert stats['fanout_error_responses_sent_total'] == 1
        assert stats['fanout_error_response_sent_bytes_total'] > 0
        assert stats['fanout_responses_sent_total'] == 0


def debug_stats(client, *sections):
    """This node's own TS._DEBUG STATS for the given sections, as a dict."""
    flat = client.execute_command('TS._DEBUG', 'STATS', *sections)
    return {flat[i].decode(): flat[i + 1] for i in range(0, len(flat), 2)}


def histogram_count(value):
    return {value[i].decode(): value[i + 1] for i in range(0, len(value), 2)}['count']


def shard_errors(stats):
    return {name: value for name, value in stats.items()
            if name.startswith('fanout_errors_') and value}


class TestFanoutStatsCME(ValkeyTimeSeriesClusterTestCaseDebugMode):
    """Coordinator, serving and cluster-map metrics around real fanouts."""

    TIMEOUT_CONFIG = 'ts.ts-fanout-command-timeout'

    def primaries(self):
        return [self.client_for_primary(i) for i in range(self.CLUSTER_SIZE)]

    def reset_all(self):
        for client in self.primaries():
            assert client.execute_command('TS._DEBUG', 'STATS', 'RESET') == b'OK'

    def tag_per_primary(self):
        """One hash tag per primary, in primary order."""
        cluster_client = self.new_cluster_client()
        by_port = {}
        for i in range(1000):
            node = cluster_client.get_node_from_key('{tag%d}' % i)
            by_port.setdefault(node.port, f'tag{i}')
            if len(by_port) == self.CLUSTER_SIZE:
                break
        return [by_port[self.get_primary_port(i)] for i in range(self.CLUSTER_SIZE)]

    def populate(self):
        cluster_client = self.new_cluster_client()
        tags = self.tag_per_primary()
        for tag in tags:
            for j in range(3):
                key = f'fo:{{{tag}}}:{j}'
                cluster_client.execute_command('TS.CREATE', key, 'LABELS', 'env', 'prod')
                cluster_client.execute_command('TS.ADD', key, 1000, j)
        return tags

    def test_one_operation_on_the_coordinator_one_request_served_per_peer(self):
        self.populate()
        self.reset_all()
        coordinator, *peers = self.primaries()

        coordinator.execute_command('TS.MRANGE', '-', '+', 'FILTER', 'env=prod')

        stats = debug_stats(coordinator, 'fanout')
        assert stats['fanout_operations_total'] == 1
        assert stats['fanout_targets_total'] == self.CLUSTER_SIZE
        assert stats['fanout_local_only_total'] == 0
        assert stats['fanout_inflight'] == 0
        assert histogram_count(stats['fanout_duration_seconds']) == 1
        assert shard_errors(stats) == {}
        assert stats['fanout_aborts_total'] == 0
        assert stats['fanout_generic_error_replies_total'] == 0
        assert stats['fanout_served_ok_total'] == 0
        for peer in peers:
            peer_stats = debug_stats(peer, 'fanout')
            assert peer_stats['fanout_served_ok_total'] == 1
            assert peer_stats['fanout_operations_total'] == 0

    def test_a_hashtag_owned_by_the_coordinator_stays_local(self):
        tags = self.populate()
        self.reset_all()
        coordinator, *peers = self.primaries()

        assert coordinator.execute_command('TS.CARD', 'HASHTAG', tags[0], 'FILTER', 'env=prod') == 3

        stats = debug_stats(coordinator, 'fanout')
        assert stats['fanout_operations_total'] == 1
        assert stats['fanout_local_only_total'] == 1
        assert stats['fanout_targets_total'] == 1
        assert stats['fanout_requests_sent_total'] == 0
        assert histogram_count(stats['fanout_duration_seconds']) == 1
        for peer in peers:
            assert debug_stats(peer, 'fanout')['fanout_served_ok_total'] == 0

    def test_a_shard_failure_is_counted_by_kind_and_answered_generically(self):
        """A peer with debug-mode off fails its share of TS._DEBUG INDEXMEMORY."""
        self.reset_all()
        coordinator, peer = self.primaries()[:2]
        peer.execute_command('CONFIG', 'SET', 'ts.debug-mode', 'no')
        try:
            with pytest.raises(ResponseError, match='Internal error in fanout operation'):
                coordinator.execute_command('TS._DEBUG', 'INDEXMEMORY', 'CLUSTER')
        finally:
            peer.execute_command('CONFIG', 'SET', 'ts.debug-mode', 'yes')

        stats = debug_stats(coordinator, 'fanout')
        assert sum(shard_errors(stats).values()) == 1, shard_errors(stats)
        assert stats['fanout_generic_error_replies_total'] == 1
        assert stats['fanout_aborts_total'] == 0
        assert debug_stats(peer, 'fanout')['fanout_served_errors_total'] == 1

    def test_a_key_permission_denial_aborts_the_fanout(self):
        tags = self.populate()
        # ACLs are per node: the user exists everywhere, but may read only the coordinator's keys.
        for client in self.primaries():
            client.execute_command('ACL', 'SETUSER', 'fo_limited', 'on', 'nopass',
                                   f'~fo:{{{tags[0]}}}:*', '+@all')
        self.reset_all()
        coordinator = self.primaries()[0]
        limited = self.new_client_for_primary(0)
        limited.execute_command('AUTH', 'fo_limited', 'any')

        with pytest.raises(ResponseError):
            limited.execute_command('TS.MRANGE', '-', '+', 'FILTER', 'env=prod')

        stats = debug_stats(coordinator, 'fanout')
        assert stats['fanout_aborts_total'] == 1
        errors = shard_errors(stats)
        assert set(errors) <= {'fanout_errors_key_permissions_total',
                               'fanout_errors_permissions_total'}, errors
        assert sum(errors.values()) >= 1

    def test_timeouts(self):
        """A sleeping peer runs the fanout into both deadlines; its late answer is dropped."""
        self.populate()
        coordinator, peer = self.primaries()[:2]
        coordinator.execute_command('CONFIG', 'SET', self.TIMEOUT_CONFIG, '500ms')
        self.reset_all()
        sleeper = threading.Thread(
            target=lambda: self.new_client_for_primary(1).execute_command('DEBUG', 'SLEEP', 2))
        sleeper.start()
        try:
            time.sleep(0.2)
            with pytest.raises(ResponseError, match='did not reply'):
                coordinator.execute_command('TS.MRANGE', '-', '+', 'FILTER', 'env=prod')
        finally:
            sleeper.join()
            coordinator.execute_command('CONFIG', 'SET', self.TIMEOUT_CONFIG, '5000ms')

        stats = debug_stats(coordinator, 'fanout')
        assert stats['fanout_rpc_timeouts_total'] == 1
        assert stats['fanout_errors_timeout_total'] == 1
        # The two deadlines race: the client's fires unless the RPC's result reached it first.
        assert stats['fanout_client_timeouts_total'] <= 1
        assert stats['fanout_inflight'] == 0
        # Once awake, the peer answers a request the coordinator no longer has.
        wait_for_true(lambda: debug_stats(peer, 'fanout')['fanout_served_ok_total'] == 1)
        wait_for_true(lambda: debug_stats(coordinator, 'fanout')[
            'fanout_ignored_unknown_request_total'] >= 1)

    def test_inflight_lists_a_request_waiting_on_a_peer(self):
        self.populate()
        coordinator = self.primaries()[0]
        sleeper = threading.Thread(
            target=lambda: self.new_client_for_primary(1).execute_command('DEBUG', 'SLEEP', 1))
        result = {}

        def run_mrange():
            result['reply'] = self.new_client_for_primary(0).execute_command(
                'TS.MRANGE', '-', '+', 'FILTER', 'env=prod')

        sleeper.start()
        time.sleep(0.2)
        query = threading.Thread(target=run_mrange)
        query.start()
        try:
            wait_for_true(lambda: coordinator.execute_command('TS._DEBUG', 'INFLIGHT') != [])
            [entry] = coordinator.execute_command('TS._DEBUG', 'INFLIGHT')
            fields = {entry[i].decode(): entry[i + 1] for i in range(0, len(entry), 2)}
            assert list(fields) == ['id', 'command', 'ageMs', 'remoteTargets', 'outstanding']
            assert fields['command'] == b'mrange'
            assert int(fields['id'])
            assert fields['remoteTargets'] == self.CLUSTER_SIZE - 1
            assert 1 <= fields['outstanding'] <= self.CLUSTER_SIZE - 1
            assert fields['ageMs'] >= 0
            assert debug_stats(coordinator, 'fanout')['fanout_inflight'] == 1
        finally:
            sleeper.join()
            query.join()
        assert len(result['reply']) == 3 * self.CLUSTER_SIZE
        wait_for_true(lambda: coordinator.execute_command('TS._DEBUG', 'INFLIGHT') == [])
        assert debug_stats(coordinator, 'fanout')['fanout_inflight'] == 0

    def test_cluster_map_metrics(self):
        coordinator = self.primaries()[0]
        coordinator.execute_command('TS._DEBUG', 'INDEXMEMORY', 'CLUSTER')

        stats = debug_stats(coordinator, 'clustermap')
        assert stats['clustermap_refreshes_total'] >= 1
        assert stats['clustermap_refreshes_total'] == (
            stats['clustermap_refresh_changed_total']
            + stats['clustermap_refresh_unchanged_total']
            + stats['clustermap_refresh_failures_total'])
        assert stats['clustermap_refresh_failures_total'] == 0
        assert float(stats['clustermap_age_seconds']) >= 0
        assert float(stats['clustermap_refresh_interval_seconds']) > 0


def cluster_stats(client, *args):
    """TS._DEBUG STATS across the cluster, as a dict."""
    flat = client.execute_command('TS._DEBUG', 'STATS', *args, 'CLUSTER')
    return {flat[i].decode(): flat[i + 1] for i in range(0, len(flat), 2)}


def per_node(gauge):
    """A cluster-view gauge: flat [address, value, ...] -> {address: value}."""
    return {gauge[i].decode(): gauge[i + 1] for i in range(0, len(gauge), 2)}


class TestStatsClusterViewCME(ValkeyTimeSeriesClusterTestCaseDebugMode):
    """TS._DEBUG STATS CLUSTER: every node, replicas included, summed or listed per node."""

    REPLICAS_COUNT = 1

    def all_nodes(self):
        """A client and `host:port` address for every node, primaries and replicas."""
        nodes = []
        for i in range(self.CLUSTER_SIZE):
            group = self.get_replication_group(i)
            nodes.append(self.client_for_primary(i))
            nodes.append(group.get_replica_connection(0))
        return nodes

    def node_ports(self):
        ports = set()
        for line in self.client_for_primary(0).execute_command('CLUSTER', 'NODES').decode().splitlines():
            address = line.split()[1].split('@')[0]
            ports.add(int(address.rsplit(':', 1)[1]))
        return ports

    def test_counters_and_histograms_sum_over_every_node(self):
        coordinator = self.client_for_primary(0)
        for client in self.all_nodes():
            client.execute_command('TS._DEBUG', 'STATS', 'RESET')
        coordinator.execute_command('TS._DEBUG', 'INDEXMEMORY', 'CLUSTER')

        local = [debug_stats(client, 'fanout', 'cron') for client in self.all_nodes()]
        cluster = cluster_stats(coordinator, 'fanout', 'cron')

        # INDEXMEMORY asked one node per shard, a replica where there is one, so never the
        # coordinator: every shard's answer was served remotely. The reading itself is served after
        # each node's snapshot, so it does not count itself.
        served = sum(stats['fanout_served_ok_total'] for stats in local)
        assert served == self.CLUSTER_SIZE
        assert cluster['fanout_served_ok_total'] == served
        # The cron keeps ticking between the reads, so the cluster view can only be ahead.
        assert cluster['cron_ticks_total'] >= sum(s['cron_ticks_total'] for s in local)
        assert histogram_count(cluster['cron_tick_duration_seconds']) >= sum(
            histogram_count(s['cron_tick_duration_seconds']) for s in local)

    def test_gauges_are_listed_per_node(self):
        cluster = cluster_stats(self.client_for_primary(0), 'cron', 'exec')

        for name in ('cron_interval_seconds', 'exec_fanout_queued'):
            values = per_node(cluster[name])
            ports = {int(address.rsplit(':', 1)[1]) for address in values}
            assert ports == self.node_ports(), name
            assert list(values) == sorted(values), 'sorted by address'
        assert all(float(v) > 0 for v in per_node(cluster['cron_interval_seconds']).values())

    def test_resp3_gauges_are_maps_of_nodes(self):
        server = self.replication_groups[0].primary.server
        resp3 = Valkey(host=server.bind_ip, port=server.port, protocol=3)
        cluster = resp3.execute_command('TS._DEBUG', 'STATS', 'cron', 'CLUSTER')

        assert isinstance(cluster, dict)
        values = cluster[b'cron_interval_seconds']
        assert isinstance(values, dict)
        assert {int(address.decode().rsplit(':', 1)[1]) for address in values} == self.node_ports()
        assert all(isinstance(value, float) for value in values.values())

    def test_default_reports_one_node_in_the_single_node_layout(self):
        local = debug_stats(self.client_for_primary(0), 'cron')
        assert float(local['cron_interval_seconds']) > 0  # a scalar, not a per-node list

    def test_verbose_and_section_order_match_a_single_node(self):
        coordinator = self.client_for_primary(0)
        local_names = list(debug_stats(coordinator))
        entries = coordinator.execute_command('TS._DEBUG', 'STATS', 'VERBOSE', 'CLUSTER')
        assert [e[1].decode() for e in entries] == local_names
        for entry in entries:
            fields = {entry[i].decode(): entry[i + 1] for i in range(0, len(entry), 2)}
            assert fields['description'], fields['name']
            if fields['kind'] == b'gauge':
                assert len(per_node(fields['value'])) == len(self.node_ports()), fields['name']

    def test_reset_cluster_starts_every_node_over(self):
        coordinator = self.client_for_primary(0)
        wait_for_true(lambda: all(debug_stats(c, 'cron')['cron_ticks_total'] >= 20
                                  for c in self.all_nodes()))

        assert coordinator.execute_command('TS._DEBUG', 'STATS', 'RESET', 'CLUSTER') == b'OK'

        for client in self.all_nodes():
            assert debug_stats(client, 'cron')['cron_ticks_total'] < 10

    def test_reset_starts_only_this_node_over(self):
        coordinator, peer = self.client_for_primary(0), self.client_for_primary(1)
        wait_for_true(lambda: debug_stats(peer, 'cron')['cron_ticks_total'] >= 20)

        assert coordinator.execute_command('TS._DEBUG', 'STATS', 'RESET') == b'OK'

        assert debug_stats(coordinator, 'cron')['cron_ticks_total'] < 10
        assert debug_stats(peer, 'cron')['cron_ticks_total'] >= 20

    def test_a_node_with_debug_mode_off_fails_the_command(self):
        replica = self.get_replication_group(1).get_replica_connection(0)
        replica.execute_command('CONFIG', 'SET', 'ts.debug-mode', 'no')
        try:
            with pytest.raises(ResponseError):
                cluster_stats(self.client_for_primary(0))
            # Without CLUSTER, only this node is asked.
            assert debug_stats(self.client_for_primary(0), 'cron')
        finally:
            replica.execute_command('CONFIG', 'SET', 'ts.debug-mode', 'yes')


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
            replies[db] = index_memory(coordinator, 'CLUSTER')
        all_dbs = index_memory(coordinator, 'ALLDBS', 'CLUSTER')

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
        assert index_memory(coordinator, 'CLUSTER')['series'] == 0
