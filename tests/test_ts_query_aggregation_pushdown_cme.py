"""Cluster integration tests for PromQL aggregation push-down (TS.QUERY).

`sum by (job) (http_requests_total)` on a cluster is evaluated by shipping the
whole aggregation — selector *and* operator — to every shard, so what crosses
the wire is one partial state (or one candidate sample) per group per shard
instead of the raw input vector. See
`src/promql/engine/fanout/aggregation_fanout_command.rs`.

Each series here lives on a known primary (via `{hN}` hash tags), and the
groups deliberately straddle shards, so these queries genuinely exercise the
three push-down strategies:

* Reduce      — sum/avg/min/max/count/group/stddev/stdvar ship a mergeable
                partial state per group.
* Select      — topk/bottomk/limitk/limit_ratio select locally, and the
                coordinator re-selects over the union.
* CountValues — count_values ships per-(group, value) counts to be summed.

`quantile` has no decomposable form and must stay on the coordinator.

Every assertion is the answer a single node would give for the same input;
`test_pushdown_on_off_equivalence` checks that directly by re-running the
queries with the push-down toggled off.
"""

import math
from contextlib import contextmanager
from datetime import datetime, timezone

from valkey import ValkeyCluster
from valkeytestframework.conftest import resource_port_tracker

from query_result import QueryResult
from valkey_timeseries_test_case import ValkeyTimeSeriesClusterTestCase

# Full config name for CONFIG SET (module prefix `ts` + config name).
PUSHDOWN_CONFIG = 'ts.ts-fanout-aggregation-pushdown'

# Hash tags landing on distinct primaries of the 3-node cluster the harness
# builds: it splits the 16384 slots evenly, so {h2} -> node 0, {h1} -> node 1,
# {h0} -> node 2. test_fixture_spans_all_shards guards the assumption.
TAG_BY_NODE = {0: 'h2', 1: 'h1', 2: 'h0'}

T0 = '2026-04-06T20:00:00Z'
T1 = '2026-04-06T20:01:00Z'

# key, job, instance, region, value at T0, value at T1.
# 'api' spans all three shards; 'web' spans two.
REQUEST_SERIES = [
    ('req:api-1:{h0}', 'api', 'api-1', 'us', 100, 1),
    ('req:api-2:{h1}', 'api', 'api-2', 'us', 200, 2),
    ('req:api-3:{h2}', 'api', 'api-3', 'eu', 300, 3),
    ('req:web-1:{h1}', 'web', 'web-1', 'us', 1000, 10),
    ('req:web-2:{h2}', 'web', 'web-2', 'eu', 2000, 20),
]

# key, job, instance, response code (as a sample value) — the same code recurs
# on different shards so count_values has something to merge. Every series
# needs a distinct label set; two series sharing one is not a valid input
# vector.
CODE_SERIES = [
    ('code:api-1:{h0}', 'api', 'api-1', 200),
    ('code:api-2:{h1}', 'api', 'api-2', 500),
    ('code:web-1:{h2}', 'web', 'web-1', 200),
    ('code:web-2:{h1}', 'web', 'web-2', 200),
]


def _epoch_seconds(rfc3339: str) -> int:
    parsed = datetime.strptime(rfc3339, '%Y-%m-%dT%H:%M:%SZ')
    return int(parsed.replace(tzinfo=timezone.utc).timestamp())


class TestPromQLAggregationPushdownCluster(ValkeyTimeSeriesClusterTestCase):
    """PromQL aggregation push-down over a real 3-shard cluster."""

    # ── fixtures & helpers ────────────────────────────────────────────

    def setup_fleet(self):
        """Create the request/response fixture, spread across all shards."""
        cluster_client: ValkeyCluster = self.new_cluster_client()

        for key, job, instance, region, v0, v1 in REQUEST_SERIES:
            metric = (
                f'http_requests_total{{job="{job}",instance="{instance}",'
                f'region="{region}"}}'
            )
            cluster_client.execute_command('TS.CREATE', key, 'METRIC', metric)
            cluster_client.execute_command('TS.ADD', key, T0, v0)
            cluster_client.execute_command('TS.ADD', key, T1, v1)

        for key, job, instance, code in CODE_SERIES:
            metric = f'http_response_code{{job="{job}",instance="{instance}"}}'
            cluster_client.execute_command('TS.CREATE', key, 'METRIC', metric)
            cluster_client.execute_command('TS.ADD', key, T1, code)

    def coordinator(self):
        """A plain (non-cluster-aware) client to the node that fans out."""
        return self.new_client_for_primary(0)

    def instant_query(self, query: str, time=T1, client=None) -> QueryResult:
        client = client or self.coordinator()
        args = ['TS.QUERY', query]
        if time is not None:
            args.extend(['TIME', str(time)])
        return QueryResult.from_raw(client.execute_command(*args))

    def range_query(self, query: str, start: str, end: str, step: str) -> QueryResult:
        raw = self.coordinator().execute_command(
            'TS.QUERYRANGE', query, 'STEP', step, 'START', start, 'END', end)
        return QueryResult.from_raw(raw)

    def set_pushdown(self, value):
        """Toggle push-down on every primary (the coordinator consults it)."""
        for i in range(self.CLUSTER_SIZE):
            self.new_client_for_primary(i).execute_command(
                'CONFIG', 'SET', PUSHDOWN_CONFIG, value)

    @contextmanager
    def pushdown_disabled(self):
        self.set_pushdown('no')
        try:
            yield
        finally:
            self.set_pushdown('yes')

    @staticmethod
    def values_by_label(result: QueryResult, label: str) -> dict[str, float]:
        assert result.is_vector(), f"expected vector, got {result.result_type}"
        by_label = {}
        for sample in result.result:
            key = sample.metric[label]
            assert key not in by_label, f"duplicate group for {label}={key}"
            by_label[key] = sample.value.value
        return by_label

    @staticmethod
    def values_by_labelset(result: QueryResult) -> dict[frozenset, float]:
        """Key every sample by its full label set — used where the output label
        set is the thing under test (count_values, `without`, topk)."""
        assert result.is_vector(), f"expected vector, got {result.result_type}"
        by_labels = {}
        for sample in result.result:
            key = frozenset(sample.metric.items())
            assert key not in by_labels, f"duplicate label set {dict(key)}"
            by_labels[key] = sample.value.value
        return by_labels

    def assert_values_close(self, actual: dict, expected: dict, context=''):
        assert actual.keys() == expected.keys(), f"{context}: groups differ"
        for key, want in expected.items():
            got = actual[key]
            assert math.isclose(got, want, rel_tol=1e-9), \
                f"{context}: {key} expected {want}, got {got}"

    def assert_single_value(self, result: QueryResult, expected: float):
        assert result.is_vector(), f"expected vector, got {result.result_type}"
        assert len(result.result) == 1, f"expected one sample, got {result.result}"
        got = result.result[0].value.value
        assert math.isclose(got, expected, rel_tol=1e-9), \
            f"expected {expected}, got {got}"
        return result.result[0]

    # ── the fixture really is spread across shards ────────────────────

    def test_fixture_spans_all_shards(self):
        """Without this, every other test in the file could be passing on a
        single shard and proving nothing about the fanout."""
        self.setup_fleet()

        for node, tag in TAG_BY_NODE.items():
            client = self.new_client_for_primary(node)
            keys = [k.decode() for k in client.execute_command('KEYS', '*')]
            assert keys, f"primary {node} holds none of the fixture"
            for key in keys:
                assert key.endswith(f'{{{tag}}}'), \
                    f"primary {node} holds {key}, expected only {{{tag}}} keys"

    # ── Reduce strategy: mergeable partial state per group ────────────

    def test_reductions_by_job(self):
        """Each reduction over groups that straddle shards: api = [1,2,3]
        across three shards, web = [10,20] across two."""
        self.setup_fleet()

        cases = [
            ('sum', {'api': 6.0, 'web': 30.0}),
            ('avg', {'api': 2.0, 'web': 15.0}),
            ('min', {'api': 1.0, 'web': 10.0}),
            ('max', {'api': 3.0, 'web': 20.0}),
            ('count', {'api': 3.0, 'web': 2.0}),
            ('group', {'api': 1.0, 'web': 1.0}),
            # population stddev/stdvar, as Prometheus computes them
            ('stddev', {'api': math.sqrt(2.0 / 3.0), 'web': 5.0}),
            ('stdvar', {'api': 2.0 / 3.0, 'web': 25.0}),
        ]

        for op, expected in cases:
            result = self.instant_query(f'{op} by (job) (http_requests_total)')
            self.assert_values_close(
                self.values_by_label(result, 'job'), expected, context=op)

    def test_reduction_without_modifier_drops_all_labels(self):
        """A bare `sum(...)` reduces the whole vector to one unlabelled
        sample; `__name__` must not survive the aggregation."""
        self.setup_fleet()

        result = self.instant_query('sum(http_requests_total)')
        sample = self.assert_single_value(result, 36.0)
        assert sample.metric == {}, f"expected no labels, got {sample.metric}"

    def test_reduction_with_without_modifier(self):
        """`without (instance)` groups by every remaining label; the (api, us)
        group spans two shards.

        The grouping labels are asserted rather than the full label set: this
        engine currently keeps `__name__` through a `without` aggregation
        (Prometheus drops it), on the push-down path and the single-node path
        alike — test_pushdown_on_off_equivalence is what pins the two together.
        """
        self.setup_fleet()

        result = self.instant_query(
            'sum without (instance) (http_requests_total)')

        by_group = {}
        for sample in result.result:
            assert 'instance' not in sample.metric, \
                f"`without (instance)` kept the instance label: {sample.metric}"
            by_group[(sample.metric['job'], sample.metric['region'])] = sample.value.value

        self.assert_values_close(
            by_group,
            {('api', 'us'): 3.0, ('api', 'eu'): 3.0,
             ('web', 'us'): 10.0, ('web', 'eu'): 20.0},
            context='without')

    def test_group_confined_to_one_shard(self):
        """A group whose series all live on one shard still comes back through
        the same path — the coordinator merges a single partial state."""
        self.setup_fleet()

        result = self.instant_query(
            'sum by (job) (http_requests_total{region="eu"})')

        self.assert_values_close(
            self.values_by_label(result, 'job'),
            {'api': 3.0, 'web': 20.0},
            context='single-shard groups')

    # ── evaluation timestamp / time modifiers ─────────────────────────

    def test_result_carries_query_evaluation_timestamp(self):
        """The pushed-down result is stamped with the query's evaluation time,
        not with the timestamp of the samples the shards selected."""
        self.setup_fleet()

        eval_ts = _epoch_seconds(T1) + 30  # inside the 5m lookback window
        result = self.instant_query('sum by (job) (http_requests_total)', eval_ts)

        by_job = self.values_by_label(result, 'job')
        self.assert_values_close(by_job, {'api': 6.0, 'web': 30.0},
                                 context='lookback')
        for sample in result.result:
            assert sample.value.timestamp == eval_ts * 1000, \
                f"expected eval timestamp {eval_ts * 1000}, got {sample.value.timestamp}"

    def test_offset_shifts_selection_but_not_output_timestamp(self):
        """`offset` moves the shards' selection back to the T0 samples while
        the output stays stamped at the evaluation time."""
        self.setup_fleet()

        result = self.instant_query(
            'sum by (job) (http_requests_total offset 1m)')

        self.assert_values_close(
            self.values_by_label(result, 'job'),
            {'api': 600.0, 'web': 3000.0},
            context='offset')
        for sample in result.result:
            assert sample.value.timestamp == _epoch_seconds(T1) * 1000

    def test_at_modifier_pins_selection_time(self):
        """`@ <ts>` pins the selection to T0 for every shard."""
        self.setup_fleet()

        result = self.instant_query(
            f'sum by (job) (http_requests_total @ {_epoch_seconds(T0)})')

        self.assert_values_close(
            self.values_by_label(result, 'job'),
            {'api': 600.0, 'web': 3000.0},
            context='@ modifier')

    def test_parenthesized_selector_is_pushed_down(self):
        """`sum((metric))` aggregates a selector just as `sum(metric)` does."""
        self.setup_fleet()

        result = self.instant_query('sum by (job) ((http_requests_total))')

        self.assert_values_close(
            self.values_by_label(result, 'job'),
            {'api': 6.0, 'web': 30.0},
            context='parenthesized')

    # ── Select strategy: shard-local selection, re-selected globally ──

    def test_topk_bottomk_across_shards(self):
        """topk/bottomk survive being run twice — once per shard, once over
        the union — and keep the original series labels."""
        self.setup_fleet()

        top = self.instant_query('topk(2, http_requests_total)')
        assert self.values_by_label(top, 'instance') == {'web-2': 20.0, 'web-1': 10.0}
        # selected samples keep their identity, unlike reductions
        assert top.result[0].metric['__name__'] == 'http_requests_total'

        bottom = self.instant_query('bottomk(2, http_requests_total)')
        assert self.values_by_label(bottom, 'instance') == {'api-1': 1.0, 'api-2': 2.0}

    def test_topk_by_group_across_shards(self):
        """Grouped selection: one winner per job, each from a different shard."""
        self.setup_fleet()

        result = self.instant_query('topk by (job) (1, http_requests_total)')

        by_instance = self.values_by_label(result, 'instance')
        assert by_instance == {'api-3': 3.0, 'web-2': 20.0}

    def test_topk_k_larger_than_input(self):
        """k above the input size returns the whole vector."""
        self.setup_fleet()

        result = self.instant_query('topk(50, http_requests_total)')

        assert self.values_by_label(result, 'instance') == {
            'api-1': 1.0, 'api-2': 2.0, 'api-3': 3.0, 'web-1': 10.0, 'web-2': 20.0,
        }

    def test_limitk_and_limit_ratio_cardinality(self):
        """limitk/limit_ratio pick an arbitrary-but-deterministic subset; the
        size of that subset must not change because the input was split across
        shards."""
        self.setup_fleet()

        assert len(self.instant_query('limitk(2, http_requests_total)').result) == 2
        assert len(self.instant_query('limitk(0, http_requests_total)').result) == 0
        assert len(self.instant_query('limitk(50, http_requests_total)').result) == 5
        assert len(self.instant_query('limit_ratio(1.0, http_requests_total)').result) == 5
        assert len(self.instant_query('limit_ratio(0, http_requests_total)').result) == 0

    # ── CountValues strategy: per-(group, value) counts, summed ───────

    def test_count_values_across_shards(self):
        """The same value seen on different shards is counted once, together:
        code 200 appears on all three shards."""
        self.setup_fleet()

        result = self.instant_query('count_values("code", http_response_code)')

        expected = {
            frozenset({'code': '200'}.items()): 3.0,
            frozenset({'code': '500'}.items()): 1.0,
        }
        self.assert_values_close(self.values_by_labelset(result), expected,
                                 context='count_values')

    def test_count_values_by_group(self):
        """Grouped count_values keys the counts by (group labels + value)."""
        self.setup_fleet()

        result = self.instant_query(
            'count_values by (job) ("code", http_response_code)')

        expected = {
            frozenset({'job': 'api', 'code': '200'}.items()): 1.0,
            frozenset({'job': 'api', 'code': '500'}.items()): 1.0,
            frozenset({'job': 'web', 'code': '200'}.items()): 2.0,
        }
        self.assert_values_close(self.values_by_labelset(result), expected,
                                 context='count_values by')

    # ── operators and operands that must not be pushed down ───────────

    def test_quantile_is_evaluated_on_the_coordinator(self):
        """`quantile` has no decomposable partial state, so the coordinator
        pulls the raw vector and interpolates itself — the answer must still
        be the single-node one."""
        self.setup_fleet()

        by_job = self.instant_query('quantile by (job) (0.5, http_requests_total)')
        self.assert_values_close(
            self.values_by_label(by_job, 'job'),
            {'api': 2.0, 'web': 15.0},
            context='quantile by job')

        # median of [1, 2, 3, 10, 20] over the whole fleet
        self.assert_single_value(
            self.instant_query('quantile(0.5, http_requests_total)'), 3.0)

    def test_aggregation_over_expression_is_not_pushed_down(self):
        """Only a bare vector selector can be pushed down; an operand that has
        to be evaluated first falls back to coordinator-side aggregation."""
        self.setup_fleet()

        result = self.instant_query('sum by (job) (http_requests_total * 2)')

        self.assert_values_close(
            self.values_by_label(result, 'job'),
            {'api': 12.0, 'web': 60.0},
            context='sum of expression')

    def test_nested_aggregation(self):
        """The inner aggregation is pushed down; the outer one runs over its
        (already reduced) output on the coordinator."""
        self.setup_fleet()

        self.assert_single_value(
            self.instant_query('max(sum by (job) (http_requests_total))'), 30.0)
        self.assert_single_value(
            self.instant_query('count(sum by (job) (http_requests_total))'), 2.0)

    def test_range_query_aggregation_matches_instant_queries(self):
        """Range queries preload their selectors, so the aggregation is *not*
        pushed down there. Each step must still equal the instant query at the
        same timestamp."""
        self.setup_fleet()

        result = self.range_query(
            'sum by (job) (http_requests_total)', T0, T1, '60s')

        assert result.is_matrix(), f"expected matrix, got {result.result_type}"
        by_job = {series.metric['job']: [s.value for s in series.values]
                  for series in result.result}
        assert by_job == {'api': [600.0, 6.0], 'web': [3000.0, 30.0]}

    # ── empty inputs ──────────────────────────────────────────────────

    def test_aggregation_over_no_matching_series(self):
        """No shard has anything to contribute: an empty vector, not an error
        and not a zero-valued group."""
        self.setup_fleet()

        assert self.instant_query(
            'sum by (job) (http_requests_total{job="nope"})').result == []
        assert self.instant_query('sum(no_such_metric)').result == []
        assert self.instant_query('topk(3, no_such_metric)').result == []
        assert self.instant_query(
            'count_values("code", no_such_metric)').result == []

    def test_aggregation_before_any_sample_exists(self):
        """Selecting at a time older than every sample leaves each shard with
        an empty local vector."""
        self.setup_fleet()

        result = self.instant_query('sum by (job) (http_requests_total)',
                                    _epoch_seconds(T0) - 3600)
        assert result.result == []

    # ── the push-down must not change the answer ──────────────────────

    def test_pushdown_on_off_equivalence(self):
        """Toggling the coordinator config off restores the flow where the raw
        instant vector is shipped and the coordinator aggregates. Both flows
        must produce the same groups and the same values."""
        self.setup_fleet()

        queries = [
            'sum by (job) (http_requests_total)',
            'avg by (job) (http_requests_total)',
            'min by (job) (http_requests_total)',
            'max by (job) (http_requests_total)',
            'count by (job) (http_requests_total)',
            'group by (job) (http_requests_total)',
            'stddev by (job) (http_requests_total)',
            'stdvar by (job) (http_requests_total)',
            'sum(http_requests_total)',
            'sum without (instance) (http_requests_total)',
            'sum by (job) (http_requests_total offset 1m)',
            'topk(2, http_requests_total)',
            'bottomk(2, http_requests_total)',
            'topk by (job) (1, http_requests_total)',
            'limitk(2, http_requests_total)',
            'limit_ratio(0.5, http_requests_total)',
            'count_values("code", http_response_code)',
            'count_values by (job) ("code", http_response_code)',
            'quantile by (job) (0.5, http_requests_total)',
            'sum by (job) (http_requests_total{region="eu"})',
            'sum by (job) (http_requests_total{job="nope"})',
        ]

        with_pushdown = [self.instant_query(q) for q in queries]
        with self.pushdown_disabled():
            without_pushdown = [self.instant_query(q) for q in queries]

        for query, on, off in zip(queries, with_pushdown, without_pushdown):
            self.assert_values_close(
                self.values_by_labelset(on),
                self.values_by_labelset(off),
                context=f'push-down mismatch for `{query}`')
