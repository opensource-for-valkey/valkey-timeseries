"""Cluster integration tests for the PromQL `HASHTAG` option.

`TS.QUERY` / `TS.QUERYRANGE` accept `HASHTAG tag,...` as a *routing* hint: the
coordinator contacts only the shards owning those hash tags, and every selector
in the expression is evaluated over that same shard set. See
`docs/plans/promql-hashtag-option-plan.md`.

What that means, and what these tests are for:

* Supplying a tag is an explicit request to evaluate over a partial cluster.
  Series on non-targeted shards are simply absent, so aggregations and binary
  expressions are computed from the selected shards only. Several tests below
  assert exactly that — a scoped `sum(...)` is *not* the cluster-wide sum.
* `HASHTAG` scopes shards, it does not filter keys or labels. Once a shard is
  selected, every matching series on it is in scope, including series whose key
  names contain a different tag. `test_tag_pulls_in_every_series_on_its_shard`
  pins that.
* The scope must survive the optimized paths too — aggregation push-down,
  rollup push-down, and the ordinary-selector fallback a peer without push-down
  support falls back to. The push-down tests here run with the config toggled
  both ways for that reason.

The fixture places every series on a known primary via `{hN}` tags, and
`test_fixture_spans_all_shards` guards the placement: without it a routing test
could pass while all the data sat on one shard.
"""

from datetime import datetime, timezone

import pytest
from valkey import ResponseError, Valkey, ValkeyCluster
from valkeytestframework.conftest import resource_port_tracker

from query_result import QueryResult
from valkey_timeseries_test_case import ValkeyTimeSeriesClusterTestCase

# Full config names for CONFIG SET (module prefix `ts` + config name).
AGGREGATION_PUSHDOWN_CONFIG = 'ts.ts-fanout-aggregation-pushdown'
ROLLUP_PUSHDOWN_CONFIG = 'ts.ts-fanout-rollup-pushdown'

# The harness splits the 16384 slots evenly across three primaries, at
# [0, 5461), [5461, 10922), [10922, 16384), so these tags land one shard each.
# `slot_of` below recomputes it, and test_fixture_spans_all_shards checks the
# keys really did land where this table says.
TAG_BY_NODE = {0: 'h2', 1: 'h1', 2: 'h0'}
NODE_BY_TAG = {tag: node for node, tag in TAG_BY_NODE.items()}

# A second tag per shard, used to show that two different tags naming one shard
# select that shard once rather than twice. test_alias_tags_share_a_shard
# checks the aliases really are co-resident.
ALIAS_BY_TAG = {'h2': 't2', 'h1': 't1', 'h0': 't0'}

T0 = int(datetime(2026, 4, 6, 20, 0, 0, tzinfo=timezone.utc).timestamp())
T1 = T0 + 60

# instance -> (shard tag, value at T0, value at T1). Each shard's total is an
# order of magnitude apart from its neighbours', so a scoped sum names the
# shard set it came from unambiguously.
REQUEST_SERIES = {
    's0-a': ('h0', 1, 2),
    's0-b': ('h0', 2, 4),
    's1-a': ('h1', 10, 20),
    's1-b': ('h1', 20, 40),
    's2-a': ('h2', 100, 200),
    's2-b': ('h2', 200, 400),
}

# One error series per request series, same label set bar `__name__`, so
# `http_requests_total - http_errors_total` matches 1:1 and the binary
# expression has a selector on each side to scope.
ERROR_VALUE = 1.0

# A metric that exists on shard {h1} only, so a query scoped to another shard
# has a genuinely empty answer rather than a smaller one.
SHARD_LOCAL_KEY = 'local:only:{h1}'
SHARD_LOCAL_VALUE = 42.0

# Sum of the T1 values per shard, and the whole-cluster total.
SUM_BY_TAG = {'h0': 6.0, 'h1': 60.0, 'h2': 600.0}
SUM_ALL = 666.0


def _rfc3339(epoch_seconds: int) -> str:
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).strftime(
        '%Y-%m-%dT%H:%M:%SZ')


def slot_of(tag: str) -> int:
    """CRC16-XMODEM of the tag, mod 16384 — the slot a `{tag}` key hashes to."""
    crc = 0
    for byte in tag.encode():
        crc ^= byte << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) & 0xFFFF if crc & 0x8000 else (crc << 1) & 0xFFFF
    return crc % 16384


class TestPromQLHashTagCluster(ValkeyTimeSeriesClusterTestCase):
    """`HASHTAG` scoping for TS.QUERY / TS.QUERYRANGE over a 3-shard cluster."""

    # ── fixture & helpers ─────────────────────────────────────────────

    def setup_fleet(self):
        """Create the request/error fixture, two series per shard."""
        cluster: ValkeyCluster = self.new_cluster_client()

        for instance, (tag, v0, v1) in REQUEST_SERIES.items():
            req_key = f'req:{instance}:{{{tag}}}'
            cluster.execute_command(
                'TS.CREATE', req_key, 'METRIC',
                f'http_requests_total{{shard="{tag}",instance="{instance}"}}')
            cluster.execute_command('TS.ADD', req_key, _rfc3339(T0), v0)
            cluster.execute_command('TS.ADD', req_key, _rfc3339(T1), v1)

            err_key = f'err:{instance}:{{{tag}}}'
            cluster.execute_command(
                'TS.CREATE', err_key, 'METRIC',
                f'http_errors_total{{shard="{tag}",instance="{instance}"}}')
            cluster.execute_command('TS.ADD', err_key, _rfc3339(T0), ERROR_VALUE)
            cluster.execute_command('TS.ADD', err_key, _rfc3339(T1), ERROR_VALUE)

        cluster.execute_command(
            'TS.CREATE', SHARD_LOCAL_KEY, 'METRIC', 'shard_local_metric{shard="h1"}')
        cluster.execute_command('TS.ADD', SHARD_LOCAL_KEY, _rfc3339(T1), SHARD_LOCAL_VALUE)

    def coordinator(self) -> Valkey:
        """A plain (non-cluster-aware) client to the node that fans out."""
        return self.new_client_for_primary(0)

    def instant_query(self, query: str, tags=None, time=T1, client=None) -> QueryResult:
        client = client or self.coordinator()
        args = ['TS.QUERY', query, 'TIME', str(time)]
        if tags is not None:
            args.extend(['HASHTAG', tags])
        return QueryResult.from_raw(client.execute_command(*args))

    def range_query(self, query: str, tags=None, start=T0, end=T1, step='60s') -> QueryResult:
        # TIME reads a bare integer as Unix *seconds* (the Prometheus HTTP
        # API convention); START/END read one as milliseconds (the TS.*
        # convention). RFC3339 sidesteps the difference.
        args = ['TS.QUERYRANGE', query, 'STEP', step,
                'START', _rfc3339(start), 'END', _rfc3339(end)]
        if tags is not None:
            args.extend(['HASHTAG', tags])
        return QueryResult.from_raw(self.coordinator().execute_command(*args))

    def set_config(self, name, value):
        """Set a config on every primary — the coordinator consults its own."""
        for i in range(self.CLUSTER_SIZE):
            self.new_client_for_primary(i).execute_command('CONFIG', 'SET', name, value)

    @pytest.fixture(params=['yes', 'no'], ids=['pushdown-on', 'pushdown-off'])
    def aggregation_pushdown(self, request):
        self.set_config(AGGREGATION_PUSHDOWN_CONFIG, request.param)
        yield request.param
        self.set_config(AGGREGATION_PUSHDOWN_CONFIG, 'yes')

    @pytest.fixture(params=['yes', 'no'], ids=['pushdown-on', 'pushdown-off'])
    def rollup_pushdown(self, request):
        self.set_config(ROLLUP_PUSHDOWN_CONFIG, request.param)
        yield request.param
        self.set_config(ROLLUP_PUSHDOWN_CONFIG, 'yes')

    @staticmethod
    def instants_by_instance(result: QueryResult) -> dict:
        assert result.is_vector(), f"expected vector, got {result.result_type}"
        by_instance = {}
        for sample in result.result:
            key = sample.metric['instance']
            assert key not in by_instance, f"instance {key} appeared twice"
            by_instance[key] = sample.value.value
        return by_instance

    @staticmethod
    def ranges_by_instance(result: QueryResult) -> dict:
        assert result.is_matrix(), f"expected matrix, got {result.result_type}"
        by_instance = {}
        for series in result.result:
            key = series.metric['instance']
            assert key not in by_instance, f"instance {key} appeared twice"
            by_instance[key] = [v.value for v in series.values]
        return by_instance

    def single_value(self, result: QueryResult) -> float:
        assert result.is_vector(), f"expected vector, got {result.result_type}"
        assert len(result.result) == 1, f"expected one sample, got {result.result}"
        return result.result[0].value.value

    @staticmethod
    def instances_on(*tags) -> set:
        return {i for i, (tag, _, _) in REQUEST_SERIES.items() if tag in tags}

    # ── the fixture really is spread across shards ────────────────────

    def test_fixture_spans_all_shards(self):
        """Without this, every routing assertion below could be passing on a
        single shard and proving nothing about the fanout."""
        self.setup_fleet()

        for node, tag in TAG_BY_NODE.items():
            client = self.new_client_for_primary(node)
            keys = [k.decode() for k in client.execute_command('KEYS', '*')]
            assert keys, f"primary {node} holds none of the fixture"
            for key in keys:
                assert key.endswith(f'{{{tag}}}'), \
                    f"primary {node} holds {key}, expected only {{{tag}}} keys"

    def test_alias_tags_share_a_shard(self):
        """The alias tags used below really do name the same shards as the
        fixture tags — otherwise the de-duplication tests prove nothing."""
        bounds = (5461, 10922)
        for tag, alias in ALIAS_BY_TAG.items():
            def node_of(t):
                s = slot_of(t)
                return 0 if s < bounds[0] else (1 if s < bounds[1] else 2)
            assert node_of(tag) == node_of(alias), \
                f"{alias} (slot {slot_of(alias)}) is not on {tag}'s shard"
            assert NODE_BY_TAG[tag] == node_of(tag), \
                f"TAG_BY_NODE disagrees with the slot split for {tag}"

    # ── 1-3: no tag / one tag / two tags ──────────────────────────────

    def test_unscoped_query_returns_every_shard(self):
        self.setup_fleet()

        result = self.instant_query('http_requests_total')
        assert set(self.instants_by_instance(result)) == set(REQUEST_SERIES)

    def test_one_tag_returns_only_its_shard(self):
        self.setup_fleet()

        for tag in ('h0', 'h1', 'h2'):
            result = self.instant_query('http_requests_total', tags=tag)
            assert set(self.instants_by_instance(result)) == self.instances_on(tag), tag

    def test_two_tags_return_the_union_of_their_shards(self):
        self.setup_fleet()

        result = self.instant_query('http_requests_total', tags='h0,h1')
        assert set(self.instants_by_instance(result)) == self.instances_on('h0', 'h1')

    def test_range_query_scoping_matches_the_instant_query(self):
        """TS.QUERYRANGE takes the same clause with the same meaning."""
        self.setup_fleet()

        unscoped = self.ranges_by_instance(self.range_query('http_requests_total'))
        assert set(unscoped) == set(REQUEST_SERIES)

        for tags, expected in (('h1', self.instances_on('h1')),
                               ('h0,h2', self.instances_on('h0', 'h2'))):
            scoped = self.ranges_by_instance(
                self.range_query('http_requests_total', tags=tags))
            assert set(scoped) == expected, tags
            # The samples themselves are untouched by the routing scope.
            for instance in expected:
                assert scoped[instance] == unscoped[instance], (tags, instance)

    # ── 4: repeats and co-resident tags do not duplicate series ───────

    def test_repeated_and_co_resident_tags_do_not_duplicate_series(self):
        """`ClusterMap::get_targets` de-duplicates the target nodes, so naming
        one shard several ways still contacts it once. `instants_by_instance`
        rejects a repeated series outright."""
        self.setup_fleet()

        expected = self.instances_on('h1')
        for tags in ('h1,h1', f'h1,{ALIAS_BY_TAG["h1"]}', f'{ALIAS_BY_TAG["h1"]},h1,h1'):
            result = self.instant_query('http_requests_total', tags=tags)
            assert set(self.instants_by_instance(result)) == expected, tags

    def test_repeated_hashtag_clause_uses_the_last_list(self):
        """A repeated clause replaces the scope rather than accumulating it."""
        self.setup_fleet()

        raw = self.coordinator().execute_command(
            'TS.QUERY', 'http_requests_total', 'HASHTAG', 'h0',
            'TIME', str(T1), 'HASHTAG', 'h1')
        result = QueryResult.from_raw(raw)
        assert set(self.instants_by_instance(result)) == self.instances_on('h1')

    # ── 5: braced and bare forms are equivalent ───────────────────────

    def test_braced_and_bare_tags_select_the_same_shard(self):
        self.setup_fleet()

        bare = self.instants_by_instance(
            self.instant_query('http_requests_total', tags='h1'))
        braced = self.instants_by_instance(
            self.instant_query('http_requests_total', tags='{h1}'))
        assert bare == braced

        bare_pair = self.instants_by_instance(
            self.instant_query('http_requests_total', tags='h0,h2'))
        braced_pair = self.instants_by_instance(
            self.instant_query('http_requests_total', tags='{h0},{h2}'))
        assert bare_pair == braced_pair

    # ── 6: a shard with no matching series answers empty ──────────────

    def test_tag_for_a_shard_without_matching_series_returns_empty(self):
        """The tag still names a valid slot; that shard simply has nothing to
        contribute, which is an empty result rather than an error."""
        self.setup_fleet()

        vector = self.instant_query('shard_local_metric', tags='h0')
        assert vector.is_vector()
        assert vector.result == [], f"expected an empty vector, got {vector.result}"

        matrix = self.range_query('shard_local_metric', tags='h0')
        assert matrix.is_matrix()
        assert matrix.result == [], f"expected an empty matrix, got {matrix.result}"

        # ...and the shard that does hold it still answers.
        on_shard = self.instant_query('shard_local_metric', tags='h1')
        assert self.single_value(on_shard) == SHARD_LOCAL_VALUE

    # ── HASHTAG scopes shards, it does not filter keys ────────────────

    def test_tag_pulls_in_every_series_on_its_shard(self):
        """A series whose key names a *different* tag is still in scope once
        its shard is selected. This is the documented `HASHTAG` contract and
        the easiest thing to get wrong by adding a shard-side filter."""
        self.setup_fleet()
        alias = ALIAS_BY_TAG['h1']

        # A key tagged {t1} lives on {h1}'s shard, so `HASHTAG h1` must see it.
        cluster = self.new_cluster_client()
        cluster.execute_command(
            'TS.CREATE', f'req:alias:{{{alias}}}', 'METRIC',
            'http_requests_total{shard="h1",instance="alias"}')
        cluster.execute_command('TS.ADD', f'req:alias:{{{alias}}}', _rfc3339(T1), 7)

        result = self.instant_query('http_requests_total', tags='h1')
        assert set(self.instants_by_instance(result)) == self.instances_on('h1') | {'alias'}

    # ── 7: scoped aggregation, push-down on and off ───────────────────

    def test_scoped_aggregation_sums_only_selected_shards(self, aggregation_pushdown):
        """A scoped `sum(...)` is the sum over the selected shards — not the
        cluster-wide sum with a filter applied afterwards."""
        self.setup_fleet()

        assert self.single_value(self.instant_query('sum(http_requests_total)')) == SUM_ALL

        for tags, expected in (
            ('h0', SUM_BY_TAG['h0']),
            ('h1', SUM_BY_TAG['h1']),
            ('h2', SUM_BY_TAG['h2']),
            ('h0,h1', SUM_BY_TAG['h0'] + SUM_BY_TAG['h1']),
            ('h0,h2', SUM_BY_TAG['h0'] + SUM_BY_TAG['h2']),
            ('h0,h1,h2', SUM_ALL),
        ):
            got = self.single_value(self.instant_query('sum(http_requests_total)', tags=tags))
            assert got == expected, (aggregation_pushdown, tags, got)

    def test_scoped_grouped_aggregation(self, aggregation_pushdown):
        """`sum by (shard)` scoped to two shards yields exactly two groups."""
        self.setup_fleet()

        result = self.instant_query('sum by (shard) (http_requests_total)', tags='h0,h2')
        by_shard = {s.metric['shard']: s.value.value for s in result.result}
        assert by_shard == {'h0': SUM_BY_TAG['h0'], 'h2': SUM_BY_TAG['h2']}, \
            aggregation_pushdown

    # ── 8: scoped rollup, push-down on and off ────────────────────────

    def test_scoped_rollup_returns_only_selected_series(self, rollup_pushdown):
        """`rate(...[2m])` scoped to a shard yields that shard's series only,
        with the same values it has in the unscoped answer."""
        self.setup_fleet()

        unscoped = self.instants_by_instance(
            self.instant_query('rate(http_requests_total[2m])'))
        assert set(unscoped) == set(REQUEST_SERIES), rollup_pushdown

        for tags in ('h1', 'h0,h2'):
            scoped = self.instants_by_instance(
                self.instant_query('rate(http_requests_total[2m])', tags=tags))
            expected = self.instances_on(*tags.split(','))
            assert set(scoped) == expected, (rollup_pushdown, tags)
            for instance in expected:
                assert scoped[instance] == unscoped[instance], \
                    (rollup_pushdown, tags, instance)

    def test_scoped_fused_rollup_aggregation(self, rollup_pushdown):
        """A rollup inside an aggregation — the fused push-down path — keeps
        the scope through both halves."""
        self.setup_fleet()

        scoped = self.single_value(
            self.instant_query('sum(rate(http_requests_total[2m]))', tags='h0'))
        unscoped_parts = self.instants_by_instance(
            self.instant_query('rate(http_requests_total[2m])', tags='h0'))
        expected = sum(unscoped_parts.values())
        assert scoped == pytest.approx(expected, rel=1e-12), rollup_pushdown

    # ── 9: every selector in the expression shares the scope ──────────

    def test_binary_expression_scopes_both_sides(self):
        """`a - b` has a selector on each side; both must be restricted to the
        same shard set, and the matching must still succeed."""
        self.setup_fleet()

        result = self.instant_query(
            'http_requests_total - http_errors_total', tags='h1')
        by_instance = self.instants_by_instance(result)

        expected = {i: REQUEST_SERIES[i][2] - ERROR_VALUE for i in self.instances_on('h1')}
        assert by_instance == expected

    def test_binary_expression_union_of_two_shards(self):
        self.setup_fleet()

        result = self.instant_query(
            'http_requests_total - http_errors_total', tags='h0,h2')
        by_instance = self.instants_by_instance(result)

        expected = {i: REQUEST_SERIES[i][2] - ERROR_VALUE
                    for i in self.instances_on('h0', 'h2')}
        assert by_instance == expected

    def test_subquery_and_nested_selectors_share_the_scope(self):
        """A subquery re-reads through the same reader, so it is scoped too."""
        self.setup_fleet()

        result = self.instant_query('max_over_time(http_requests_total[2m:60s])', tags='h1')
        assert set(self.instants_by_instance(result)) == self.instances_on('h1')

    def test_selectorless_expression_is_unaffected(self):
        """`1 + 2` performs no fanout, so the clause is accepted and inert."""
        self.setup_fleet()

        raw = self.coordinator().execute_command(
            'TS.QUERY', '1 + 2', 'TIME', str(T1), 'HASHTAG', 'h0')
        result = QueryResult.from_raw(raw)
        assert result.is_scalar()
        assert result.result.value == 3.0

    # ── 10: ACL identity and the selected database ────────────────────

    def test_routing_scope_does_not_change_the_caller_identity(self):
        """Scoping picks which shards are contacted; it never changes who is
        asking. So a restricted user's scoped answer is exactly their unscoped
        answer narrowed to the selected shards — the same restriction an
        unrestricted user sees, applied on top of whatever that identity is
        already entitled to.

        Stated this way the test holds regardless of how much per-key ACL the
        PromQL read path applies, which is deliberately not what is under test
        here (tests/test_ts_acls_cme.py owns cross-shard identity, and
        `test_query_preserves_callers_acl_identity` in tests/test_ts_query.py
        owns the single-node key checks).

        There is also no database dimension to test: cluster mode has only
        database 0, so `SELECT` cannot move a scoped query off it. The
        single-node database behaviour is covered by
        `test_query_respects_selected_db` in tests/test_ts_query.py.
        """
        self.setup_fleet()
        for i in range(self.CLUSTER_SIZE):
            self.new_client_for_primary(i).execute_command(
                'ACL', 'SETUSER', 'h1only', 'RESET', 'ON', '>pw',
                '+@timeseries', '+@read', '~*{h1}')

        scoped_user = self.new_client_for_primary(0)
        scoped_user.execute_command('AUTH', 'h1only', 'pw')

        unscoped = self.instants_by_instance(
            self.instant_query('http_requests_total', client=scoped_user))

        for tags in ('h1', 'h0', 'h0,h2', 'h0,h1,h2'):
            selected = set(tags.split(','))
            scoped = self.instants_by_instance(
                self.instant_query('http_requests_total', tags=tags, client=scoped_user))
            expected = {i: v for i, v in unscoped.items()
                        if REQUEST_SERIES[i][0] in selected}
            assert scoped == expected, tags

    def test_hashtag_does_not_bypass_command_permissions(self):
        """A caller denied TS.QUERY stays denied with the clause present: the
        routing scope is not a second path into the command."""
        self.setup_fleet()
        for i in range(self.CLUSTER_SIZE):
            self.new_client_for_primary(i).execute_command(
                'ACL', 'SETUSER', 'noquery', 'RESET', 'ON', '>pw',
                '+@timeseries', '+@read', '~*', '-TS.QUERY')

        denied = self.new_client_for_primary(0)
        denied.execute_command('AUTH', 'noquery', 'pw')

        for args in (('TIME', str(T1)), ('TIME', str(T1), 'HASHTAG', 'h1')):
            with pytest.raises(ResponseError, match='(?i)permission'):
                denied.execute_command('TS.QUERY', 'http_requests_total', *args)

    # ── option placement and validation over a real cluster ───────────

    def test_hashtag_composes_with_every_other_option(self):
        self.setup_fleet()
        expected = self.instances_on('h1')
        client = self.coordinator()

        for args in (
            ('HASHTAG', 'h1', 'TIME', str(T1)),
            ('TIME', str(T1), 'HASHTAG', 'h1'),
            ('TIME', str(T1), 'LOOKBACK_DELTA', '5m', 'hashtag', 'h1'),
            ('HASHTAG', 'h1', 'TIMEOUT', '10s', 'TIME', str(T1)),
        ):
            result = QueryResult.from_raw(
                client.execute_command('TS.QUERY', 'http_requests_total', *args))
            assert set(self.instants_by_instance(result)) == expected, args

    def test_malformed_hashtag_is_rejected(self):
        self.setup_fleet()
        client = self.coordinator()

        for bad in ('', ',h1', 'h1,', 'h0,,h1'):
            with pytest.raises(ResponseError, match="missing HASHTAG argument"):
                client.execute_command('TS.QUERY', 'http_requests_total',
                                       'TIME', str(T1), 'HASHTAG', bad)

        with pytest.raises(ResponseError, match="missing HASHTAG argument"):
            client.execute_command('TS.QUERY', 'http_requests_total',
                                   'TIME', str(T1), 'HASHTAG')
        with pytest.raises(ResponseError, match="missing HASHTAG argument"):
            client.execute_command('TS.QUERYRANGE', 'http_requests_total',
                                   'STEP', '60s', 'HASHTAG')
