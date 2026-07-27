"""Cluster integration tests for PromQL rollup push-down (TS.QUERY).

A PromQL series is owned entirely by one shard, so `rate(m[5m])` over that
series' window needs no cross-shard merge algebra: the shard computes the final
value and the coordinator concatenates. `sum by (job) (rate(m[5m]))` goes one
step further and has the shard accumulate its rolled-up values into per-group
partials. See `src/promql/engine/fanout/rollup_fanout_command.rs`.

What this file is for, and what it is not:

* It proves the *answers* over a real 3-shard cluster — for the step grid, the
  time modifiers, sparse and NaN windows, `__name__` handling, the fused and
  unfused paths, and the coordinator-only functions. Each series lives on a
  known primary (via `{hN}` hash tags) and the groups deliberately straddle
  shards, so a fanout genuinely happens.
* It cannot observe push-down *directly*: nothing in INFO, COMMANDSTATS or the
  log distinguishes a pushed-down rollup from a coordinator-side one. What it
  does have is the on/off equivalence tests, which compare the two paths against
  each other — so a defect introduced anywhere in the shard-side reduction shows
  up as a divergence. (Verified by mutation: shifting the shard's window ends by
  1ms fails 11 tests here, both equivalence tests among them.) What it cannot
  reach at all is the mixed-version behaviour — the config is consulted only by
  the coordinator and shards obey the request, so no CONFIG SET makes a peer
  behave like an older build. That is pinned by the round-trip tests beside
  `RollupFanoutCommand`.

Exactness follows §10.2 of `docs/promql-rollup-pushdown-plan.md`: unfused rollup
values are compared with `==`, because the same kernel reduces the same window on
either path. Fused aggregation is the documented exception — merging per-shard
partials sums in a different order than a single-node reduction — so the fused
comparisons assert shape exactly and values to a relative 1e-12.
"""

import math
from contextlib import contextmanager
from datetime import datetime, timezone

from valkey import ValkeyCluster
from valkeytestframework.conftest import resource_port_tracker

from query_result import QueryResult
from valkey_timeseries_test_case import ValkeyTimeSeriesClusterTestCase

# Full config name for CONFIG SET (module prefix `ts` + config name).
PUSHDOWN_CONFIG = 'ts.ts-fanout-rollup-pushdown'

# Hash tags landing on distinct primaries of the 3-node cluster the harness
# builds: it splits the 16384 slots evenly, so {h2} -> node 0, {h1} -> node 1,
# {h0} -> node 2. test_fixture_spans_all_shards guards the assumption.
TAG_BY_NODE = {0: 'h2', 1: 'h1', 2: 'h0'}

T0 = int(datetime(2026, 4, 6, 20, 0, 0, tzinfo=timezone.utc).timestamp())

# Samples land every 30s, five of them. Every instant query below evaluates at
# EVAL over a `[2m]` range, so the window is (T0, T0+120] — the last four
# samples, with the T0 sample deliberately just outside it. A rollup that reads
# one sample too many is off by a whole term in every expectation here.
SAMPLE_OFFSETS = (0, 30, 60, 90, 120)
EVAL = T0 + 120
RANGE = '2m'

# instance -> (job, hash tag, five sample values)
#
# Both jobs straddle shards, and no shard holds a whole job. Values are chosen
# so that every reduction below lands on a float that is exact in binary:
# `web-3` is the flat-ish one, which is what gives `changes`, `stddev_over_time`
# and `ts_of_max_over_time` something to distinguish.
GAUGE_SERIES = {
    'api-1': ('api', 'h0', (1, 2, 3, 4, 5)),
    'api-2': ('api', 'h1', (10, 20, 30, 40, 50)),
    'api-3': ('api', 'h2', (100, 200, 300, 400, 500)),
    'web-1': ('web', 'h1', (2, 4, 6, 8, 10)),
    'web-2': ('web', 'h2', (3, 6, 9, 12, 15)),
    'web-3': ('web', 'h0', (7, 7, 7, 8, 8)),
}

# Counters: two perfectly linear (1/s and 2/s, which is what makes `rate` land
# on an exact 1.0 and 2.0 after extrapolation) and one that resets between
# T0+60 and T0+90.
COUNTER_SERIES = {
    'api-1': ('api', 'h0', (0, 30, 60, 90, 120)),
    'api-2': ('api', 'h1', (0, 60, 120, 180, 240)),
    'api-3': ('api', 'h2', (0, 30, 60, 10, 40)),
}

# One sample, at T0 only: every window after the first is empty, which is what
# the sparse-transport and NaN-vs-absent tests need.
SPARSE_KEY = 'sparse:only:{h1}'
SPARSE_VALUE = 42


def _rfc3339(epoch_seconds: int) -> str:
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).strftime(
        '%Y-%m-%dT%H:%M:%SZ')


class TestPromQLRollupPushdownCluster(ValkeyTimeSeriesClusterTestCase):
    """PromQL rollup push-down over a real 3-shard cluster."""

    # ── fixtures & helpers ────────────────────────────────────────────

    def setup_fleet(self):
        """Create the gauge/counter/sparse fixture and enable push-down."""
        self.set_pushdown('yes')
        cluster_client: ValkeyCluster = self.new_cluster_client()

        for instance, (job, tag, values) in GAUGE_SERIES.items():
            key = f'mem:{instance}:{{{tag}}}'
            metric = f'mem_usage{{job="{job}",instance="{instance}"}}'
            cluster_client.execute_command('TS.CREATE', key, 'METRIC', metric)
            for offset, value in zip(SAMPLE_OFFSETS, values):
                cluster_client.execute_command(
                    'TS.ADD', key, _rfc3339(T0 + offset), value)

        for instance, (job, tag, values) in COUNTER_SERIES.items():
            key = f'req:{instance}:{{{tag}}}'
            metric = f'http_requests_total{{job="{job}",instance="{instance}"}}'
            cluster_client.execute_command('TS.CREATE', key, 'METRIC', metric)
            for offset, value in zip(SAMPLE_OFFSETS, values):
                cluster_client.execute_command(
                    'TS.ADD', key, _rfc3339(T0 + offset), value)

        cluster_client.execute_command(
            'TS.CREATE', SPARSE_KEY, 'METRIC', 'sparse_metric{instance="only"}')
        cluster_client.execute_command(
            'TS.ADD', SPARSE_KEY, _rfc3339(T0), SPARSE_VALUE)

    def coordinator(self):
        """A plain (non-cluster-aware) client to the node that fans out."""
        return self.new_client_for_primary(0)

    def instant_query(self, query: str, time=EVAL) -> QueryResult:
        args = ['TS.QUERY', query]
        if time is not None:
            args.extend(['TIME', str(time)])
        return QueryResult.from_raw(self.coordinator().execute_command(*args))

    def range_query(self, query: str, start=T0, end=EVAL, step='60s') -> QueryResult:
        # START/END go over as RFC 3339. A bare integer is not portable between
        # the two commands: TS.QUERY's TIME reads it as Unix *seconds* (the
        # Prometheus HTTP API convention) while TS.QUERYRANGE's START/END read
        # it as milliseconds (the TS.* convention).
        raw = self.coordinator().execute_command(
            'TS.QUERYRANGE', query, 'STEP', step,
            'START', _rfc3339(start), 'END', _rfc3339(end))
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

    # ── result shaping ────────────────────────────────────────────────

    @staticmethod
    def values_by_instance(result: QueryResult) -> dict:
        assert result.is_vector(), f"expected vector, got {result.result_type}"
        by_instance = {}
        for sample in result.result:
            key = sample.metric['instance']
            assert key not in by_instance, f"duplicate series for {key}"
            by_instance[key] = sample.value.value
        return by_instance

    @staticmethod
    def values_by_label(result: QueryResult, label: str) -> dict:
        assert result.is_vector(), f"expected vector, got {result.result_type}"
        by_label = {}
        for sample in result.result:
            key = sample.metric[label]
            assert key not in by_label, f"duplicate group for {label}={key}"
            by_label[key] = sample.value.value
        return by_label

    @staticmethod
    def vector_by_labelset(result: QueryResult) -> dict:
        """Every sample keyed by its full label set and carrying its timestamp
        — the shape §10.2 asks to compare exactly."""
        assert result.is_vector(), f"expected vector, got {result.result_type}"
        by_labels = {}
        for sample in result.result:
            key = frozenset(sample.metric.items())
            assert key not in by_labels, f"duplicate label set {dict(key)}"
            by_labels[key] = (sample.value.timestamp, sample.value.value)
        return by_labels

    @staticmethod
    def matrix_by_labelset(result: QueryResult) -> dict:
        """Every series keyed by its full label set, as an ordered list of
        `(timestamp, value)` — so a missing step is a missing entry, not a
        silently equal one."""
        assert result.is_matrix(), f"expected matrix, got {result.result_type}"
        by_labels = {}
        for series in result.result:
            key = frozenset(series.metric.items())
            assert key not in by_labels, f"duplicate label set {dict(key)}"
            by_labels[key] = [(s.timestamp, s.value) for s in series.values]
        return by_labels

    @staticmethod
    def steps_by_instance(result: QueryResult) -> dict:
        assert result.is_matrix(), f"expected matrix, got {result.result_type}"
        return {series.metric['instance']: [s.value for s in series.values]
                for series in result.result}

    @staticmethod
    def steps_by_label(result: QueryResult, label: str) -> dict:
        assert result.is_matrix(), f"expected matrix, got {result.result_type}"
        return {series.metric[label]: [s.value for s in series.values]
                for series in result.result}

    # ── comparison ────────────────────────────────────────────────────

    @classmethod
    def _identical(cls, a, b) -> bool:
        """Exact, with NaN equal to NaN — NaN is a legitimate rolled-up value
        here, so it has to compare rather than poison the comparison. Tuples are
        compared element-wise for the same reason: `(t, nan) == (t, nan)` is
        False, which would make every NaN point look like a mismatch."""
        if isinstance(a, tuple) and isinstance(b, tuple):
            return len(a) == len(b) and all(
                cls._identical(x, y) for x, y in zip(a, b))
        if isinstance(a, float) and isinstance(b, float):
            if math.isnan(a) and math.isnan(b):
                return True
        return a == b

    def assert_values_exact(self, actual: dict, expected: dict, context=''):
        assert actual.keys() == expected.keys(), \
            f"{context}: series differ\n  got      {sorted(map(str, actual))}" \
            f"\n  expected {sorted(map(str, expected))}"
        for key, want in expected.items():
            got = actual[key]
            assert self._identical(got, want), \
                f"{context}: {key} expected {want!r}, got {got!r}"

    def assert_steps_exact(self, actual: dict, expected: dict, context=''):
        assert actual.keys() == expected.keys(), \
            f"{context}: series differ\n  got      {sorted(map(str, actual))}" \
            f"\n  expected {sorted(map(str, expected))}"
        for key, want in expected.items():
            got = actual[key]
            assert len(got) == len(want), \
                f"{context}: {key} has {len(got)} steps, expected {len(want)}: " \
                f"got {got!r}, expected {want!r}"
            for i, (g, w) in enumerate(zip(got, want)):
                assert self._identical(g, w), \
                    f"{context}: {key} step {i} expected {w!r}, got {g!r}"

    def assert_steps_near(self, actual: dict, expected: dict, context=''):
        """Shape exactly, values to a relative 1e-12 — for fused aggregation,
        where the per-shard partial merge order legitimately differs from a
        single-node reduction. Shape is the part that must never degrade."""
        assert actual.keys() == expected.keys(), \
            f"{context}: series differ\n  got      {sorted(map(str, actual))}" \
            f"\n  expected {sorted(map(str, expected))}"
        for key, want in expected.items():
            got = actual[key]
            assert len(got) == len(want), \
                f"{context}: {key} has {len(got)} steps, expected {len(want)}"
            for i, (g, w) in enumerate(zip(got, want)):
                if isinstance(g, tuple):
                    # (timestamp, value) — the timestamp is still exact.
                    assert g[0] == w[0], \
                        f"{context}: {key} step {i} timestamp {g[0]} != {w[0]}"
                    g, w = g[1], w[1]
                assert self._identical(g, w) or math.isclose(g, w, rel_tol=1e-12), \
                    f"{context}: {key} step {i} expected {w!r}, got {g!r}"

    def assert_single_value(self, result: QueryResult, expected: float):
        assert result.is_vector(), f"expected vector, got {result.result_type}"
        assert len(result.result) == 1, f"expected one sample, got {result.result}"
        got = result.result[0].value.value
        assert self._identical(got, expected), f"expected {expected}, got {got}"
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

    def test_pushdown_config_is_registered(self):
        """The toggle the rest of this file flips has to exist and round-trip;
        a typo'd name would make every `pushdown_disabled()` block a no-op and
        the equivalence tests vacuous."""
        client = self.coordinator()

        def current():
            raw = client.execute_command('CONFIG', 'GET', PUSHDOWN_CONFIG)
            assert raw, f"{PUSHDOWN_CONFIG} is not a registered config"
            # RESP2 answers CONFIG GET with a flat [name, value] list.
            return (raw[PUSHDOWN_CONFIG] if isinstance(raw, dict) else raw[1])

        for value in ('no', 'yes'):
            client.execute_command('CONFIG', 'SET', PUSHDOWN_CONFIG, value)
            got = current()
            got = got.decode() if isinstance(got, bytes) else got
            assert got == value, f"set {value}, read back {got}"

    # ── the *_over_time family, per series, across shards ─────────────

    def test_over_time_family_across_shards(self):
        """Each reduction over the (T0, T0+120] window of all six series, which
        live three-way split across the cluster."""
        self.setup_fleet()

        cases = [
            ('sum_over_time',
             {'api-1': 14, 'api-2': 140, 'api-3': 1400,
              'web-1': 28, 'web-2': 42, 'web-3': 30}),
            ('count_over_time',
             {'api-1': 4, 'api-2': 4, 'api-3': 4,
              'web-1': 4, 'web-2': 4, 'web-3': 4}),
            ('avg_over_time',
             {'api-1': 3.5, 'api-2': 35, 'api-3': 350,
              'web-1': 7, 'web-2': 10.5, 'web-3': 7.5}),
            ('min_over_time',
             {'api-1': 2, 'api-2': 20, 'api-3': 200,
              'web-1': 4, 'web-2': 6, 'web-3': 7}),
            ('max_over_time',
             {'api-1': 5, 'api-2': 50, 'api-3': 500,
              'web-1': 10, 'web-2': 15, 'web-3': 8}),
            ('first_over_time',
             {'api-1': 2, 'api-2': 20, 'api-3': 200,
              'web-1': 4, 'web-2': 6, 'web-3': 7}),
            ('last_over_time',
             {'api-1': 5, 'api-2': 50, 'api-3': 500,
              'web-1': 10, 'web-2': 15, 'web-3': 8}),
            ('present_over_time',
             {'api-1': 1, 'api-2': 1, 'api-3': 1,
              'web-1': 1, 'web-2': 1, 'web-3': 1}),
            ('mad_over_time',
             {'api-1': 1, 'api-2': 10, 'api-3': 100,
              'web-1': 2, 'web-2': 3, 'web-3': 0.5}),
            # `changes` is the one that separates web-3 (7 7 8 8) from the rest.
            ('changes',
             {'api-1': 3, 'api-2': 3, 'api-3': 3,
              'web-1': 3, 'web-2': 3, 'web-3': 1}),
        ]

        for func, expected in cases:
            result = self.instant_query(f'{func}(mem_usage[{RANGE}])')
            self.assert_values_exact(
                self.values_by_instance(result), expected, context=func)

    # Population variance of each window, exactly: [2 3 4 5] -> 1.25, and so on.
    VARIANCES = {'api-1': 1.25, 'api-2': 125, 'api-3': 12500,
                 'web-1': 5, 'web-2': 11.25, 'web-3': 0.25}

    def test_variance_over_time_across_shards(self):
        """Split out from the exact cases: the running variance is computed by
        a compensated accumulation, so it lands within an ulp or two of the
        closed-form answer rather than on it (`web-3` comes back as
        0.2500000000000001), and `stddev_over_time` is irrational for most of
        this fixture besides. The exact proof for both is
        test_pushdown_on_off_equivalence_instant, which compares with `==`."""
        self.setup_fleet()

        stdvar = self.values_by_instance(
            self.instant_query(f'stdvar_over_time(mem_usage[{RANGE}])'))
        stddev = self.values_by_instance(
            self.instant_query(f'stddev_over_time(mem_usage[{RANGE}])'))

        for instance, variance in self.VARIANCES.items():
            assert math.isclose(stdvar[instance], variance, rel_tol=1e-12), \
                f"stdvar_over_time {instance}: expected {variance}, got {stdvar[instance]}"
            want = math.sqrt(variance)
            assert math.isclose(stddev[instance], want, rel_tol=1e-12), \
                f"stddev_over_time {instance}: expected {want}, got {stddev[instance]}"

    def test_quantile_over_time_across_shards(self):
        """phi = 0.25 interpolates between the first two samples of each
        window, so a window assembled in the wrong order gives itself away."""
        self.setup_fleet()

        result = self.instant_query(
            f'quantile_over_time(0.25, mem_usage[{RANGE}])')

        self.assert_values_exact(
            self.values_by_instance(result),
            {'api-1': 2.75, 'api-2': 27.5, 'api-3': 275,
             'web-1': 5.5, 'web-2': 8.25, 'web-3': 7},
            context='quantile_over_time(0.25)')

    def test_ts_of_functions_across_shards(self):
        """The `ts_of_*` family returns a timestamp in seconds.

        `ts_of_min`/`ts_of_max` are asserted only for the five strictly
        monotonic series: web-3 has ties at both ends, and which of the tied
        samples wins is a semantics question for the promqltest goldens, not
        for the fanout. That web-3 answers the *same* either way is covered by
        test_pushdown_on_off_equivalence_instant, which includes it."""
        self.setup_fleet()

        first = self.values_by_instance(
            self.instant_query(f'ts_of_first_over_time(mem_usage[{RANGE}])'))
        assert first == {i: float(T0 + 30) for i in GAUGE_SERIES}

        last = self.values_by_instance(
            self.instant_query(f'ts_of_last_over_time(mem_usage[{RANGE}])'))
        assert last == {i: float(T0 + 120) for i in GAUGE_SERIES}

        monotonic = [i for i in GAUGE_SERIES if i != 'web-3']

        low = self.values_by_instance(
            self.instant_query(f'ts_of_min_over_time(mem_usage[{RANGE}])'))
        assert {i: low[i] for i in monotonic} == \
            {i: float(T0 + 30) for i in monotonic}

        high = self.values_by_instance(
            self.instant_query(f'ts_of_max_over_time(mem_usage[{RANGE}])'))
        assert {i: high[i] for i in monotonic} == \
            {i: float(T0 + 120) for i in monotonic}

    # ── counter functions, including a reset on one shard ─────────────

    def test_counter_functions_across_shards(self):
        """api-3 resets (60 -> 10) between T0+60 and T0+90, on a different
        shard from the two clean counters."""
        self.setup_fleet()

        resets = self.instant_query(f'resets(http_requests_total[{RANGE}])')
        self.assert_values_exact(
            self.values_by_instance(resets),
            {'api-1': 0, 'api-2': 0, 'api-3': 1},
            context='resets')

        idelta = self.instant_query(f'idelta(http_requests_total[{RANGE}])')
        self.assert_values_exact(
            self.values_by_instance(idelta),
            {'api-1': 30, 'api-2': 60, 'api-3': 30},
            context='idelta')

        irate = self.instant_query(f'irate(http_requests_total[{RANGE}])')
        self.assert_values_exact(
            self.values_by_instance(irate),
            {'api-1': 1, 'api-2': 2, 'api-3': 1},
            context='irate')

    def test_rate_and_increase_on_linear_counters(self):
        """api-1 and api-2 rise at exactly 1/s and 2/s. Their windows hold four
        samples spanning 90s of a 120s range, and extrapolation reaches both
        edges (the gap to the start is 30s, under the 1.1x average-interval
        threshold, and the counter-zero clamp lands on the same 30s), so the
        answers stay exactly 1.0 and 2.0.

        The extrapolation itself is a chain of divisions, so the answers land
        an ulp off (0.9999999999999999) rather than on 1.0 — hence the
        tolerance. The exact proof is test_pushdown_on_off_equivalence_instant.

        api-3 is excluded: its reset makes the extrapolated value irrational.
        The equivalence test covers it exactly."""
        self.setup_fleet()

        rate = self.values_by_instance(
            self.instant_query(f'rate(http_requests_total[{RANGE}])'))
        assert math.isclose(rate['api-1'], 1.0, rel_tol=1e-12), rate
        assert math.isclose(rate['api-2'], 2.0, rel_tol=1e-12), rate

        increase = self.values_by_instance(
            self.instant_query(f'increase(http_requests_total[{RANGE}])'))
        assert math.isclose(increase['api-1'], 120.0, rel_tol=1e-12), increase
        assert math.isclose(increase['api-2'], 240.0, rel_tol=1e-12), increase

    def test_deriv_across_shards(self):
        """Least-squares slope, in units per second. Symmetric inputs make the
        regression land exactly on the counters' true rates."""
        self.setup_fleet()

        result = self.values_by_instance(
            self.instant_query(f'deriv(http_requests_total[{RANGE}])'))
        assert result['api-1'] == 1.0, result
        assert result['api-2'] == 2.0, result

    # ── delayed __name__ removal, over a fanout ───────────────────────

    def test_metric_name_dropped_except_first_and_last_over_time(self):
        """Range-vector functions drop `__name__`; `first_over_time` and
        `last_over_time` are the two that preserve it, because they return a
        sample rather than a computed value. The rule has to survive the trip
        through the shards."""
        self.setup_fleet()

        for func in ('sum_over_time', 'count_over_time', 'avg_over_time',
                     'min_over_time', 'max_over_time', 'present_over_time',
                     'changes', 'resets', 'rate', 'irate', 'deriv',
                     'ts_of_last_over_time'):
            metric = ('http_requests_total'
                      if func in ('rate', 'irate', 'resets') else 'mem_usage')
            result = self.instant_query(f'{func}({metric}[{RANGE}])')
            assert result.result, f"{func} returned nothing"
            for sample in result.result:
                assert '__name__' not in sample.metric, \
                    f"{func} kept __name__: {sample.metric}"
                assert 'instance' in sample.metric, \
                    f"{func} dropped the other labels too: {sample.metric}"

        for func in ('first_over_time', 'last_over_time'):
            result = self.instant_query(f'{func}(mem_usage[{RANGE}])')
            assert result.result, f"{func} returned nothing"
            for sample in result.result:
                assert sample.metric.get('__name__') == 'mem_usage', \
                    f"{func} dropped __name__: {sample.metric}"

    # ── the step grid (one fanout for the whole range query) ──────────

    def test_range_query_walks_the_full_step_grid(self):
        """Three steps at 60s over a 2m range, so consecutive windows overlap
        by two thirds. Each step's window is (step - 2m, step]:

            T0      -> the T0 sample only
            T0+60   -> T0, T0+30, T0+60
            T0+120  -> T0+30 .. T0+120 (the T0 sample has fallen out)

        The window width comes from the selector, never from the query step —
        a rollup that re-derived the grid from the step would return the same
        value at every step here."""
        self.setup_fleet()

        result = self.range_query(f'sum_over_time(mem_usage[{RANGE}])')
        self.assert_steps_exact(
            self.steps_by_instance(result),
            {'api-1': [1, 6, 14],
             'api-2': [10, 60, 140],
             'api-3': [100, 600, 1400],
             'web-1': [2, 12, 28],
             'web-2': [3, 18, 42],
             'web-3': [7, 21, 30]},
            context='sum_over_time grid')

        counts = self.range_query(f'count_over_time(mem_usage[{RANGE}])')
        self.assert_steps_exact(
            self.steps_by_instance(counts),
            {instance: [1, 3, 4] for instance in GAUGE_SERIES},
            context='count_over_time grid')

    def test_range_query_stamps_every_step(self):
        """Each point carries its own step timestamp, in milliseconds."""
        self.setup_fleet()

        result = self.range_query(f'count_over_time(mem_usage[{RANGE}])')

        want = [(T0 + offset) * 1000 for offset in (0, 60, 120)]
        for series in result.result:
            assert [s.timestamp for s in series.values] == want, \
                f"{series.metric}: got {[s.timestamp for s in series.values]}"

    def test_range_query_narrower_than_the_step(self):
        """A range shorter than the step leaves gaps between windows rather
        than overlaps: at 60s steps a `[30s]` window covers (step-30, step],
        so only the sample landing exactly on each step is in scope."""
        self.setup_fleet()

        result = self.range_query('count_over_time(mem_usage[30s])')
        self.assert_steps_exact(
            self.steps_by_instance(result),
            {instance: [1, 1, 1] for instance in GAUGE_SERIES},
            context='[30s] grid')

    # ── time modifiers, resolved by the coordinator ───────────────────

    def test_offset_shifts_every_window(self):
        """`offset 60s` moves each step's window back one step, so the answer
        is the un-offset grid shifted by one."""
        self.setup_fleet()

        instant = self.instant_query(
            f'sum_over_time(mem_usage[{RANGE}] offset 60s)')
        self.assert_values_exact(
            self.values_by_instance(instant),
            {'api-1': 6, 'api-2': 60, 'api-3': 600,
             'web-1': 12, 'web-2': 18, 'web-3': 21},
            context='offset instant')

        result = self.range_query(f'sum_over_time(mem_usage[{RANGE}] offset 60s)')
        self.assert_steps_exact(
            self.steps_by_instance(result),
            {'api-1': [1, 6],
             'api-2': [10, 60],
             'api-3': [100, 600],
             'web-1': [2, 12],
             'web-2': [3, 18],
             'web-3': [7, 21]},
            context='offset grid')

    def test_at_modifier_collapses_the_grid(self):
        """`@` pins every step to one window, so the range query broadcasts a
        single value across the grid instead of walking it."""
        self.setup_fleet()

        result = self.range_query(
            f'sum_over_time(mem_usage[{RANGE}] @ {EVAL})')

        self.assert_steps_exact(
            self.steps_by_instance(result),
            {'api-1': [14, 14, 14],
             'api-2': [140, 140, 140],
             'api-3': [1400, 1400, 1400],
             'web-1': [28, 28, 28],
             'web-2': [42, 42, 42],
             'web-3': [30, 30, 30]},
            context='@ grid')

    def test_at_start_and_end(self):
        """`@ start()` and `@ end()` pin the grid to the query bounds."""
        self.setup_fleet()

        at_start = self.range_query(
            f'sum_over_time(mem_usage[{RANGE}] @ start())')
        self.assert_steps_exact(
            self.steps_by_instance(at_start),
            {'api-1': [1, 1, 1], 'api-2': [10, 10, 10], 'api-3': [100, 100, 100],
             'web-1': [2, 2, 2], 'web-2': [3, 3, 3], 'web-3': [7, 7, 7]},
            context='@ start()')

        at_end = self.range_query(f'sum_over_time(mem_usage[{RANGE}] @ end())')
        self.assert_steps_exact(
            self.steps_by_instance(at_end),
            {'api-1': [14, 14, 14], 'api-2': [140, 140, 140],
             'api-3': [1400, 1400, 1400], 'web-1': [28, 28, 28],
             'web-2': [42, 42, 42], 'web-3': [30, 30, 30]},
            context='@ end()')

    # ── sparse windows and NaN ────────────────────────────────────────

    def test_empty_windows_are_absent_not_zero(self):
        """`sparse_metric` has one sample, at T0. The two later windows hold
        nothing, and a window with nothing in it produces no point — not a 0
        and not a NaN."""
        self.setup_fleet()

        result = self.range_query('count_over_time(sparse_metric[1m])')

        assert len(result.result) == 1, f"expected one series, got {result.result}"
        series = result.result[0]
        assert [(s.timestamp, s.value) for s in series.values] == \
            [(T0 * 1000, 1.0)], f"got {series.values}"

    def test_nan_is_a_value_not_an_absence(self):
        """A NaN quantile is a legitimate rolled-up value, so it has to travel
        as a point. The distinction from an empty window is the whole reason
        the transport is sparse rather than NaN-filled: here step T0 carries a
        NaN and the later steps carry nothing at all."""
        self.setup_fleet()

        result = self.range_query('quantile_over_time(NaN, sparse_metric[1m])')

        assert len(result.result) == 1, f"expected one series, got {result.result}"
        points = result.result[0].values
        assert len(points) == 1, f"expected one point, got {points}"
        assert points[0].timestamp == T0 * 1000
        assert math.isnan(points[0].value), f"expected NaN, got {points[0].value}"

    def test_series_with_no_sample_in_any_window(self):
        """Selecting a range entirely after the only sample leaves every window
        empty on every shard: an empty matrix, not a series of gaps."""
        self.setup_fleet()

        result = self.range_query('count_over_time(sparse_metric[1m])',
                                  start=T0 + 300, end=T0 + 420)
        assert result.result == [], f"expected nothing, got {result.result}"

    # ── fusion with the outer aggregation ─────────────────────────────

    def test_fused_reductions_by_job(self):
        """`sum by (job) (sum_over_time(...))` — the shard reduces its windows
        and folds them into per-group partials; the coordinator merges. Both
        groups straddle shards, so neither is a single-partial shortcut."""
        self.setup_fleet()

        cases = [
            ('sum', 'sum_over_time', {'api': 1554, 'web': 100}),
            ('max', 'max_over_time', {'api': 500, 'web': 15}),
            ('min', 'min_over_time', {'api': 2, 'web': 4}),
            ('count', 'count_over_time', {'api': 3, 'web': 3}),
            ('group', 'sum_over_time', {'api': 1, 'web': 1}),
            ('sum', 'count_over_time', {'api': 12, 'web': 12}),
        ]

        for op, rollup, expected in cases:
            result = self.instant_query(
                f'{op} by (job) ({rollup}(mem_usage[{RANGE}]))')
            self.assert_values_exact(
                self.values_by_label(result, 'job'), expected,
                context=f'{op} by (job) ({rollup})')

    def test_fused_reduction_without_modifier_drops_all_labels(self):
        """A bare `sum(...)` over a rollup reduces the fleet to one unlabelled
        sample. `__name__` was already dropped by the rollup; the grouping must
        not resurrect it."""
        self.setup_fleet()

        sample = self.assert_single_value(
            self.instant_query(f'sum(sum_over_time(mem_usage[{RANGE}]))'), 1654)
        assert sample.metric == {}, f"expected no labels, got {sample.metric}"

    def test_fused_reduction_with_without_modifier(self):
        """`without (instance)` keeps every other label of the rolled-up
        series, and must not reinstate the `__name__` the rollup dropped."""
        self.setup_fleet()

        result = self.instant_query(
            f'sum without (instance) (sum_over_time(mem_usage[{RANGE}]))')

        for sample in result.result:
            assert 'instance' not in sample.metric, \
                f"`without (instance)` kept instance: {sample.metric}"
            assert '__name__' not in sample.metric, \
                f"grouping resurrected __name__: {sample.metric}"
        self.assert_values_exact(
            self.values_by_label(result, 'job'),
            {'api': 1554, 'web': 100},
            context='fused without')

    def test_fused_range_query_grid(self):
        """Fusion across the whole step grid: one request, one partial per
        (group, step). Values are integral here, so they compare exactly even
        though the merge order differs from a single-node reduction."""
        self.setup_fleet()

        result = self.range_query(
            f'sum by (job) (sum_over_time(mem_usage[{RANGE}]))')

        self.assert_steps_exact(
            self.steps_by_label(result, 'job'),
            {'api': [111, 666, 1554], 'web': [12, 51, 100]},
            context='fused grid')

    def test_avg_fusion_across_shards(self):
        """`avg` ships a (sum, count) partial rather than a mean, which is what
        keeps a three-series group and a two-series group from being averaged
        as equals."""
        self.setup_fleet()

        result = self.instant_query(
            f'avg by (job) (sum_over_time(mem_usage[{RANGE}]))')

        self.assert_steps_near(
            {k: [v] for k, v in self.values_by_label(result, 'job').items()},
            {'api': [1554 / 3], 'web': [100 / 3]},
            context='avg fusion')

    def test_topk_over_a_rollup_is_not_fused(self):
        """`topk` needs the individual rolled-up samples to choose among, so it
        keeps the rollup push-down and selects on the coordinator. The winners
        keep their series labels — minus the `__name__` the rollup dropped."""
        self.setup_fleet()

        result = self.instant_query(
            f'topk(2, sum_over_time(mem_usage[{RANGE}]))')

        self.assert_values_exact(
            self.values_by_instance(result),
            {'api-3': 1400, 'api-2': 140},
            context='topk over rollup')
        for sample in result.result:
            assert '__name__' not in sample.metric, \
                f"topk resurrected __name__: {sample.metric}"

        bottom = self.instant_query(
            f'bottomk(1, sum_over_time(mem_usage[{RANGE}]))')
        self.assert_values_exact(
            self.values_by_instance(bottom), {'api-1': 14},
            context='bottomk over rollup')

    def test_quantile_over_a_rollup_is_not_fused(self):
        """`quantile` has no decomposable partial state, so the coordinator
        collects the rolled-up samples and interpolates them itself."""
        self.setup_fleet()

        result = self.instant_query(
            f'quantile by (job) (0.5, sum_over_time(mem_usage[{RANGE}]))')

        # api rolls up to [14, 140, 1400] and web to [28, 30, 42].
        self.assert_values_exact(
            self.values_by_label(result, 'job'),
            {'api': 140, 'web': 30},
            context='quantile over rollup')

    # ── functions that must stay on the coordinator ───────────────────

    def test_absent_over_time_is_cluster_wide(self):
        """`absent_over_time` is the one rollup that can never be pushed down:
        its answer depends on absence across the *whole* cluster. Pushed to the
        shards, the two primaries that do not hold `api-1` would each report it
        absent."""
        self.setup_fleet()

        present = self.instant_query(
            f'absent_over_time(mem_usage{{instance="api-1"}}[{RANGE}])')
        assert present.result == [], \
            f"api-1 exists on one shard; got {present.result}"

        assert self.instant_query(
            f'absent_over_time(mem_usage[{RANGE}])').result == []

        # Absent everywhere: exactly one sample, once, for the whole cluster —
        # not one per shard that failed to find it.
        #
        # The output *labels* are not asserted: this engine returns `{}` where
        # Prometheus copies the selector's equality matchers
        # (`{instance="nope"}`), which is a pre-existing divergence unrelated to
        # push-down — `absent_over_time` is never pushed down. The upstream
        # cases that cover it sit inside an `ignore` block in
        # `promqltest/testdata/functions.test`.
        self.assert_single_value(self.instant_query(
            f'absent_over_time(mem_usage{{instance="nope"}}[{RANGE}])'), 1)

        self.assert_single_value(
            self.instant_query(f'absent_over_time(no_such_metric[{RANGE}])'), 1)

    def test_predict_linear_on_the_coordinator(self):
        """`predict_linear` predicts relative to the query's evaluation
        timestamp, which `@`/`offset` divorce from the window end a shard is
        told, so it is evaluated coordinator-side. The answer must still be the
        single-node one: api-1 is at 120 and rising 1/s, so 30s out is 150."""
        self.setup_fleet()

        result = self.values_by_instance(
            self.instant_query(f'predict_linear(http_requests_total[{RANGE}], 30)'))
        assert result['api-1'] == 150.0, result
        assert result['api-2'] == 300.0, result

    def test_double_exponential_smoothing_on_the_coordinator(self):
        """Two scalar parameters, and the request carries one — so this stays
        coordinator-side. It still has to answer for every series."""
        self.setup_fleet()

        result = self.instant_query(
            f'double_exponential_smoothing(mem_usage[{RANGE}], 0.5, 0.5)')
        assert set(self.values_by_instance(result)) == set(GAUGE_SERIES)

    def test_experimental_functions_run_when_enabled(self):
        """`ts_of_*_over_time` is experimental *and* pushable, so it is the one
        function where the coordinator's authority over experimental gating and
        the rollup preload path meet.

        Only the enabled direction is asserted. The disabled direction is not
        testable from here: `ts-promql-enable-experimental-functions` is cached
        into `PROMQL_CONFIG` at startup and only refreshed by a config-changed
        event handler that never fires for module configs, so `CONFIG SET`
        reports OK, `CONFIG GET` reports the new value, and queries keep using
        the old one. That is a config-plumbing gap, not a push-down one — the
        gate itself is covered by the evaluator's unit tests. (The push-down
        toggle is unaffected: it is read straight from the atomic the config
        framework writes, which test_pushdown_config_is_registered and the
        equivalence tests exercise.)"""
        self.setup_fleet()

        assert self.instant_query(
            f'ts_of_last_over_time(mem_usage[{RANGE}])').result
        assert self.instant_query(
            f'sum by (job) (ts_of_last_over_time(mem_usage[{RANGE}]))').result

    # ── selectors ─────────────────────────────────────────────────────

    def test_selector_matchers_reach_the_shards(self):
        """Regex, negation and multi-matcher selectors are serialized into the
        rollup request; a matcher lost in transit would widen the result."""
        self.setup_fleet()

        self.assert_values_exact(
            self.values_by_instance(self.instant_query(
                f'sum_over_time(mem_usage{{instance=~"api-.*"}}[{RANGE}])')),
            {'api-1': 14, 'api-2': 140, 'api-3': 1400},
            context='regex')

        self.assert_values_exact(
            self.values_by_instance(self.instant_query(
                f'sum_over_time(mem_usage{{job="web",instance!="web-3"}}[{RANGE}])')),
            {'web-1': 28, 'web-2': 42},
            context='negation')

        self.assert_values_exact(
            self.values_by_instance(self.instant_query(
                f'sum_over_time(mem_usage{{instance!~"api-.*"}}[{RANGE}])')),
            {'web-1': 28, 'web-2': 42, 'web-3': 30},
            context='negative regex')

    def test_selector_confined_to_one_shard(self):
        """A rollup whose whole input lives on one shard still comes back
        through the same path."""
        self.setup_fleet()

        result = self.instant_query(
            f'sum_over_time(mem_usage{{instance="api-1"}}[{RANGE}])')
        self.assert_values_exact(
            self.values_by_instance(result), {'api-1': 14},
            context='single shard')

    # ── empty inputs ──────────────────────────────────────────────────

    def test_rollup_over_no_matching_series(self):
        """No shard has anything to contribute: an empty result, not an error
        and not a zero-valued series."""
        self.setup_fleet()

        assert self.instant_query(f'sum_over_time(no_such_metric[{RANGE}])').result == []
        assert self.instant_query(
            f'rate(http_requests_total{{job="nope"}}[{RANGE}])').result == []
        assert self.instant_query(
            f'sum by (job) (sum_over_time(no_such_metric[{RANGE}]))').result == []
        assert self.range_query(f'sum_over_time(no_such_metric[{RANGE}])').result == []

    def test_rollup_before_any_sample_exists(self):
        """Evaluating an hour before the first sample leaves every window empty
        on every shard."""
        self.setup_fleet()

        assert self.instant_query(
            f'sum_over_time(mem_usage[{RANGE}])', time=T0 - 3600).result == []
        assert self.instant_query(
            f'sum by (job) (sum_over_time(mem_usage[{RANGE}]))',
            time=T0 - 3600).result == []

    # ── the push-down must not change the answer ──────────────────────

    # Unfused: the same kernel reduces the same window on either path, so these
    # are compared with `==` (§10.2). Covers every pushable function, the
    # coordinator-only ones, both time modifiers, and a subquery whose inner
    # rollup is pushed down per subquery step.
    EQUIVALENCE_INSTANT = [
        'sum_over_time(mem_usage[2m])',
        'count_over_time(mem_usage[2m])',
        'avg_over_time(mem_usage[2m])',
        'min_over_time(mem_usage[2m])',
        'max_over_time(mem_usage[2m])',
        'stddev_over_time(mem_usage[2m])',
        'stdvar_over_time(mem_usage[2m])',
        'mad_over_time(mem_usage[2m])',
        'present_over_time(mem_usage[2m])',
        'first_over_time(mem_usage[2m])',
        'last_over_time(mem_usage[2m])',
        'quantile_over_time(0.25, mem_usage[2m])',
        'quantile_over_time(0.9, mem_usage[2m])',
        'ts_of_first_over_time(mem_usage[2m])',
        'ts_of_last_over_time(mem_usage[2m])',
        'ts_of_min_over_time(mem_usage[2m])',
        'ts_of_max_over_time(mem_usage[2m])',
        'changes(mem_usage[2m])',
        'rate(http_requests_total[2m])',
        'increase(http_requests_total[2m])',
        'delta(mem_usage[2m])',
        'irate(http_requests_total[2m])',
        'idelta(http_requests_total[2m])',
        'deriv(http_requests_total[2m])',
        'resets(http_requests_total[2m])',
        # coordinator-only by construction
        'absent_over_time(mem_usage{instance="nope"}[2m])',
        'predict_linear(http_requests_total[2m], 30)',
        'double_exponential_smoothing(mem_usage[2m], 0.5, 0.5)',
        # time modifiers
        'sum_over_time(mem_usage[2m] offset 60s)',
        f'sum_over_time(mem_usage[2m] @ {T0 + 60})',
        'rate(http_requests_total[2m] offset 30s)',
        # selectors
        'sum_over_time(mem_usage{instance=~"api-.*"}[2m])',
        'sum_over_time(mem_usage{job="web",instance!="web-3"}[2m])',
        'sum_over_time(no_such_metric[2m])',
        # a subquery: the inner rollup is a bare matrix selector at each
        # subquery step, so it is pushed down per step
        'sum_over_time(rate(http_requests_total[2m])[2m:1m])',
        # not aggregations, but they consume a pushed-down rollup
        'topk(2, sum_over_time(mem_usage[2m]))',
        'sort(sum_over_time(mem_usage[2m]))',
        'sum_over_time(mem_usage[2m]) * 2',
    ]

    EQUIVALENCE_RANGE = [
        'sum_over_time(mem_usage[2m])',
        'count_over_time(mem_usage[2m])',
        'avg_over_time(mem_usage[2m])',
        'last_over_time(mem_usage[2m])',
        'quantile_over_time(0.25, mem_usage[2m])',
        'rate(http_requests_total[2m])',
        'increase(http_requests_total[2m])',
        'irate(http_requests_total[2m])',
        'resets(http_requests_total[2m])',
        'deriv(http_requests_total[2m])',
        'count_over_time(mem_usage[30s])',
        'count_over_time(sparse_metric[1m])',
        'quantile_over_time(NaN, sparse_metric[1m])',
        'sum_over_time(mem_usage[2m] offset 60s)',
        f'sum_over_time(mem_usage[2m] @ {EVAL})',
        'sum_over_time(mem_usage[2m] @ start())',
        'absent_over_time(mem_usage{instance="nope"}[2m])',
        'predict_linear(http_requests_total[2m], 30)',
        'topk(2, sum_over_time(mem_usage[2m]))',
    ]

    # Fused: the coordinator merges per-shard partials, whose summation order
    # legitimately differs from a single-node reduction. Shape stays exact.
    EQUIVALENCE_FUSED = [
        'sum by (job) (sum_over_time(mem_usage[2m]))',
        'avg by (job) (avg_over_time(mem_usage[2m]))',
        'min by (job) (min_over_time(mem_usage[2m]))',
        'max by (job) (max_over_time(mem_usage[2m]))',
        'count by (job) (count_over_time(mem_usage[2m]))',
        'group by (job) (sum_over_time(mem_usage[2m]))',
        'stddev by (job) (sum_over_time(mem_usage[2m]))',
        'stdvar by (job) (sum_over_time(mem_usage[2m]))',
        'sum(rate(http_requests_total[2m]))',
        'sum without (instance) (sum_over_time(mem_usage[2m]))',
        'sum by (job) (sum_over_time(mem_usage[2m] offset 60s))',
        'quantile by (job) (0.5, sum_over_time(mem_usage[2m]))',
        'sum by (job) (sum_over_time(no_such_metric[2m]))',
    ]

    def test_pushdown_on_off_equivalence_instant(self):
        """Toggling the coordinator config off reverts to selecting the raw
        matrix and reducing it locally. Same series, same timestamps, same
        values — exactly."""
        self.setup_fleet()

        on = [self.instant_query(q) for q in self.EQUIVALENCE_INSTANT]
        with self.pushdown_disabled():
            off = [self.instant_query(q) for q in self.EQUIVALENCE_INSTANT]

        for query, a, b in zip(self.EQUIVALENCE_INSTANT, on, off):
            self.assert_values_exact(
                self.vector_by_labelset(a), self.vector_by_labelset(b),
                context=f'push-down mismatch for `{query}`')

    def test_pushdown_on_off_equivalence_range(self):
        """The same, over the step grid — where push-down replaces a fanout per
        step with one fanout for the whole range."""
        self.setup_fleet()

        on = [self.range_query(q) for q in self.EQUIVALENCE_RANGE]
        with self.pushdown_disabled():
            off = [self.range_query(q) for q in self.EQUIVALENCE_RANGE]

        for query, a, b in zip(self.EQUIVALENCE_RANGE, on, off):
            self.assert_steps_exact(
                self.matrix_by_labelset(a), self.matrix_by_labelset(b),
                context=f'push-down mismatch for `{query}`')

    def test_fused_pushdown_on_off_equivalence(self):
        """Fused aggregation, instant and range. Shape exactly; values to a
        relative 1e-12, because merging per-shard partials sums in a different
        order than the single-node reduction the toggle falls back to."""
        self.setup_fleet()

        on_instant = [self.instant_query(q) for q in self.EQUIVALENCE_FUSED]
        on_range = [self.range_query(q) for q in self.EQUIVALENCE_FUSED]
        with self.pushdown_disabled():
            off_instant = [self.instant_query(q) for q in self.EQUIVALENCE_FUSED]
            off_range = [self.range_query(q) for q in self.EQUIVALENCE_FUSED]

        for query, a, b in zip(self.EQUIVALENCE_FUSED, on_instant, off_instant):
            self.assert_steps_near(
                {k: [v] for k, v in self.vector_by_labelset(a).items()},
                {k: [v] for k, v in self.vector_by_labelset(b).items()},
                context=f'fused instant mismatch for `{query}`')

        for query, a, b in zip(self.EQUIVALENCE_FUSED, on_range, off_range):
            self.assert_steps_near(
                self.matrix_by_labelset(a), self.matrix_by_labelset(b),
                context=f'fused range mismatch for `{query}`')
