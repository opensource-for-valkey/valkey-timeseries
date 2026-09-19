"""Cluster integration tests for PromQL stepped selector push-down (TS.QUERYRANGE).

A range query reads every bare vector selector over its whole step grid. Before
`GridQuery` the shards shipped every raw sample in `[start - lookback, end]` and
the coordinator bucketed them to one sample per step; now the shard runs the
bucketing (`for_each_step_sample`'s rule) and ships one point per series per
step — or, for an aggregation directly over the selector, one partial per group
per step. See `src/promql/engine/fanout/grid_fanout_command.rs` and
`docs/plans/selector-pushdown-plan.md`.

What this file is for, and what it is not:

* It proves the *answers* over a real 3-shard cluster for the shapes the plan
  names — bare selectors, binary operations over shifted selectors,
  `timestamp()`, the reducing aggregations fused onto a selector, the selecting
  ones that are not, the time modifiers, sparse steps — against hand-computed
  expectations, so the on/off comparisons below cannot be comparing two wrong
  answers. Each series lives on a known primary (via `{hN}` hash tags) and the
  groups deliberately straddle shards, so a fanout genuinely happens.
* It cannot observe push-down *directly*: nothing in INFO or the log
  distinguishes a stepped response from a raw one. The on/off equivalence tests
  compare the two paths — `ts-fanout-rollup-pushdown` off routes the raw span
  back through the coordinator — so a defect in the shard-side stage shows up
  as a divergence.
* The size rule (a series whose span is smaller than its grid output travels
  raw) is exercised by querying at a step finer than the sample cadence; the
  answer must not depend on which form travelled. Under a fused reduction the
  shard sizes per *group* instead — see `test_sparse_groups_fold_on_the_shards`
  in the rollup suite for the fixture that triggers that.

Exactness: stepped values are the stored samples, so unfused comparisons are
`==`. Fused aggregation merges per-shard partials in a different order than a
single-node reduction, so those compare shape exactly and values to a relative
1e-12 — the same rule the rollup suite applies.
"""

import math
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import List

import pytest
from valkey import ResponseError, ValkeyCluster
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

# Samples land every 30s, five of them. The default range query below walks
# T0..T0+120 at a 60s step, so its three steps pick sample index 0, 2 and 4.
SAMPLE_OFFSETS = (0, 30, 60, 90, 120)
END = T0 + 120

# instance -> (job, hash tag, five sample values)
#
# Both jobs straddle shards, and no shard holds a whole job. Values are chosen
# so that the sums below are exact in binary.
GAUGE_SERIES = {
    'api-1': ('api', 'h0', (1, 2, 3, 4, 5)),
    'api-2': ('api', 'h1', (10, 20, 30, 40, 50)),
    'api-3': ('api', 'h2', (100, 200, 300, 400, 500)),
    'web-1': ('web', 'h1', (2, 4, 6, 8, 10)),
    'web-2': ('web', 'h2', (3, 6, 9, 12, 15)),
    'web-3': ('web', 'h0', (7, 7, 7, 8, 8)),
}

# One sample, at T0 only: with a 5m lookback it is picked at every step up to
# T0+300 and absent after — which is what the sparse-step tests need.
SPARSE_KEY = 'sparse:only:{h1}'
SPARSE_VALUE = 42


def _rfc3339(epoch_seconds: int) -> str:
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).strftime(
        '%Y-%m-%dT%H:%M:%SZ')


def _values_at(index: int) -> dict:
    """Every gauge's value at sample `index`, by instance."""
    return {instance: float(values[index])
            for instance, (_, _, values) in GAUGE_SERIES.items()}


def _job_sum(job: str, index: int) -> float:
    return float(sum(values[index] for _, (j, _, values) in GAUGE_SERIES.items()
                     if j == job))


class SelectorPushdownClusterBase(ValkeyTimeSeriesClusterTestCase):
    """Fixture, query helpers and comparison helpers shared by the two classes
    below."""

    # ── fixtures & helpers ────────────────────────────────────────────

    def setup_fleet(self):
        """Create the gauge and sparse fixture, with push-down on."""
        self.set_pushdown('yes')
        cluster_client: ValkeyCluster = self.new_cluster_client()

        for instance, (job, tag, values) in GAUGE_SERIES.items():
            key = f'mem:{instance}:{{{tag}}}'
            metric = f'mem_usage{{job="{job}",instance="{instance}"}}'
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

    def range_query(self, query: str, start=T0, end=END, step='60s') -> QueryResult:
        # START/END go over as RFC 3339: TS.QUERYRANGE reads a bare integer as
        # milliseconds, TS.QUERY's TIME reads it as seconds.
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
    def _points(series) -> list:
        """A series' points as `(step in seconds, value)`. The wire carries
        milliseconds; the fixture and every expectation here are in seconds,
        and the steps are whole seconds."""
        return [(s.timestamp // 1000, s.value) for s in series.values]

    @classmethod
    def matrix_by_labelset(cls, result: QueryResult) -> dict:
        """Every series keyed by its full label set, as an ordered list of
        `(timestamp, value)` — so a missing step is a missing entry, not a
        silently equal one."""
        assert result.is_matrix(), f"expected matrix, got {result.result_type}"
        by_labels = {}
        for series in result.result:
            key = frozenset(series.metric.items())
            assert key not in by_labels, f"duplicate label set {dict(key)}"
            by_labels[key] = cls._points(series)
        return by_labels

    @classmethod
    def points_by_label(cls, result: QueryResult, label: str) -> dict:
        assert result.is_matrix(), f"expected matrix, got {result.result_type}"
        out = {}
        for series in result.result:
            key = series.metric[label]
            assert key not in out, f"duplicate series for {label}={key}"
            out[key] = cls._points(series)
        return out

    @staticmethod
    def steps_by_instance(result: QueryResult) -> dict:
        return {k: [v for _, v in points]
                for k, points in SelectorPushdownClusterBase.points_by_label(
                    result, 'instance').items()}

    # ── comparison ────────────────────────────────────────────────────

    @classmethod
    def _identical(cls, a, b) -> bool:
        if isinstance(a, tuple) and isinstance(b, tuple):
            return len(a) == len(b) and all(
                cls._identical(x, y) for x, y in zip(a, b))
        if isinstance(a, float) and isinstance(b, float):
            if math.isnan(a) and math.isnan(b):
                return True
        return a == b

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
        single-node reduction."""
        assert actual.keys() == expected.keys(), \
            f"{context}: series differ\n  got      {sorted(map(str, actual))}" \
            f"\n  expected {sorted(map(str, expected))}"
        for key, want in expected.items():
            got = actual[key]
            assert len(got) == len(want), \
                f"{context}: {key} has {len(got)} steps, expected {len(want)}"
            for i, (g, w) in enumerate(zip(got, want)):
                if isinstance(g, tuple):
                    assert g[0] == w[0], \
                        f"{context}: {key} step {i} timestamp {g[0]} != {w[0]}"
                    g, w = g[1], w[1]
                assert self._identical(g, w) or math.isclose(g, w, rel_tol=1e-12), \
                    f"{context}: {key} step {i} expected {w!r}, got {g!r}"


class TestPromQLSelectorPushdownCluster(SelectorPushdownClusterBase):
    """Stepped selector push-down over a real 3-shard cluster."""

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

    def test_pushdown_is_on_by_default(self):
        """The grid push-down is the default path; the toggle is only an
        escape hatch."""
        raw = self.coordinator().execute_command('CONFIG', 'GET', PUSHDOWN_CONFIG)
        got = raw[PUSHDOWN_CONFIG] if isinstance(raw, dict) else raw[1]
        got = got.decode() if isinstance(got, bytes) else got
        assert got == 'yes'

    # ── bare selectors ────────────────────────────────────────────────

    def test_bare_selector_picks_the_last_sample_per_step(self):
        """One point per series per step, each the last sample at or before
        the step — stamped with the step, valued from the sample."""
        self.setup_fleet()
        result = self.range_query('mem_usage')
        expected = {
            instance: [(T0, _values_at(0)[instance]),
                       (T0 + 60, _values_at(2)[instance]),
                       (END, _values_at(4)[instance])]
            for instance in GAUGE_SERIES
        }
        self.assert_steps_exact(self.points_by_label(result, 'instance'), expected)

    def test_step_finer_than_cadence_takes_the_raw_form(self):
        """At a 10s step there are 13 window ends and only 5 samples per
        series: the shard ships the span raw and the coordinator steps it.
        Same rule, same answer — every step repeats the last sample."""
        self.setup_fleet()
        result = self.range_query('mem_usage', step='10s')
        want = []
        for i in range(0, 121, 10):
            want.append((T0 + i, i // 30))  # (step, sample index)
        expected = {
            instance: [(ts, float(GAUGE_SERIES[instance][2][idx])) for ts, idx in want]
            for instance in GAUGE_SERIES
        }
        self.assert_steps_exact(self.points_by_label(result, 'instance'), expected)

    def test_timestamp_reports_the_samples_own_time(self):
        """`timestamp()` reads the picked sample's timestamp, not the step's:
        the stepped form has to carry it. At a 45s step the picks are the
        samples at T0, T0+30 and T0+90."""
        self.setup_fleet()
        result = self.range_query('timestamp(mem_usage)', step='45s')
        expected = {
            instance: [(T0, float(T0)), (T0 + 45, float(T0 + 30)),
                       (T0 + 90, float(T0 + 90))]
            for instance in GAUGE_SERIES
        }
        self.assert_steps_exact(self.points_by_label(result, 'instance'), expected)

    def test_offset_shifts_every_window(self):
        """`offset 60s` reads a minute earlier at every step; the first step
        has nothing before T0 and is absent."""
        self.setup_fleet()
        result = self.range_query('mem_usage offset 60s')
        expected = {
            instance: [(T0 + 60, _values_at(0)[instance]),
                       (END, _values_at(2)[instance])]
            for instance in GAUGE_SERIES
        }
        self.assert_steps_exact(self.points_by_label(result, 'instance'), expected)

    def test_at_modifier_pins_every_step(self):
        """`@ END` collapses the grid onto one window end, which the
        coordinator replicates at every step."""
        self.setup_fleet()
        result = self.range_query(f'mem_usage @ {END}')
        expected = {
            instance: [(t, _values_at(4)[instance]) for t in (T0, T0 + 60, END)]
            for instance in GAUGE_SERIES
        }
        self.assert_steps_exact(self.points_by_label(result, 'instance'), expected)

    def test_at_start_and_end(self):
        self.setup_fleet()
        start = self.range_query('mem_usage @ start()')
        end = self.range_query('mem_usage @ end()')
        for instance in GAUGE_SERIES:
            assert self.steps_by_instance(start)[instance] == [_values_at(0)[instance]] * 3
            assert self.steps_by_instance(end)[instance] == [_values_at(4)[instance]] * 3

    def test_binary_operation_over_shifted_selectors(self):
        """Two stepped requests joined on the coordinator: the shifted side is
        absent at T0, so the ratio is too."""
        self.setup_fleet()
        result = self.range_query(
            'mem_usage / on(job, instance) mem_usage offset 60s')
        got = self.points_by_label(result, 'instance')
        assert got.keys() == GAUGE_SERIES.keys()
        for instance, (_, _, values) in GAUGE_SERIES.items():
            assert [ts for ts, _ in got[instance]] == [T0 + 60, END], instance
            assert got[instance][0][1] == pytest.approx(values[2] / values[0]), instance
            assert got[instance][1][1] == pytest.approx(values[4] / values[2]), instance

    def test_selector_beside_its_own_aggregation(self):
        """`avg(mem_usage) / mem_usage`: the selector under the aggregation is
        covered by the fused request, the bare one is its own stepped request,
        and the two must agree on every step."""
        self.setup_fleet()
        result = self.range_query('avg(mem_usage) / on() group_right mem_usage')
        got = self.points_by_label(result, 'instance')
        for instance, (_, _, values) in GAUGE_SERIES.items():
            for (ts, v), idx in zip(got[instance], (0, 2, 4)):
                mean = sum(_values_at(idx).values()) / len(GAUGE_SERIES)
                assert v == pytest.approx(mean / values[idx]), (instance, ts)

    # ── sparse steps ──────────────────────────────────────────────────

    def test_step_without_a_sample_in_the_lookback_is_absent(self):
        """The sparse series has one sample, at T0. Steps whose lookback
        window `(t - 5m, t]` misses it are absent — not NaN, not stale."""
        self.setup_fleet()
        result = self.range_query('sparse_metric', start=T0, end=T0 + 600, step='300s')
        got = self.points_by_label(result, 'instance')
        assert got == {'only': [(T0, float(SPARSE_VALUE))]}

        # …and the fused form has no group at those steps either.
        result = self.range_query('count(sparse_metric)', start=T0, end=T0 + 600,
                                  step='300s')
        assert self.matrix_by_labelset(result) == {frozenset(): [(T0, 1.0)]}

    def test_selector_over_no_matching_series(self):
        self.setup_fleet()
        assert self.range_query('no_such_metric').result == []
        assert self.range_query('sum(no_such_metric)').result == []

    # ── fused aggregations ────────────────────────────────────────────

    def test_fused_sum_by_job(self):
        """One partial per (job, step) from each shard, merged here: the
        groups straddle shards, so a merge genuinely happens."""
        self.setup_fleet()
        result = self.range_query('sum by (job) (mem_usage)')
        expected = {
            job: [(T0, _job_sum(job, 0)), (T0 + 60, _job_sum(job, 2)),
                  (END, _job_sum(job, 4))]
            for job in ('api', 'web')
        }
        self.assert_steps_exact(self.points_by_label(result, 'job'), expected)

    def test_fused_reductions(self):
        self.setup_fleet()
        n = len(GAUGE_SERIES)
        cases = {
            'sum(mem_usage)': [float(sum(_values_at(i).values())) for i in (0, 2, 4)],
            'avg(mem_usage)': [sum(_values_at(i).values()) / n for i in (0, 2, 4)],
            'min(mem_usage)': [min(_values_at(i).values()) for i in (0, 2, 4)],
            'max(mem_usage)': [max(_values_at(i).values()) for i in (0, 2, 4)],
            'count(mem_usage)': [float(n)] * 3,
            'group(mem_usage)': [1.0] * 3,
        }
        for query, want in cases.items():
            got = self.matrix_by_labelset(self.range_query(query))
            self.assert_steps_near(
                got, {frozenset(): list(zip((T0, T0 + 60, END), want))},
                context=query)

        # Population variance and its root, merged from per-shard partials.
        for query, fn in (('stdvar(mem_usage)', lambda v: v),
                          ('stddev(mem_usage)', math.sqrt)):
            got = self.matrix_by_labelset(self.range_query(query))
            want = []
            for idx, ts in zip((0, 2, 4), (T0, T0 + 60, END)):
                vals = list(_values_at(idx).values())
                mean = sum(vals) / n
                want.append((ts, fn(sum((v - mean) ** 2 for v in vals) / n)))
            self.assert_steps_near(got, {frozenset(): want}, context=query)

    def test_fused_without_modifier(self):
        self.setup_fleet()
        result = self.range_query('sum without (instance) (mem_usage)')
        got = self.points_by_label(result, 'job')
        for job in ('api', 'web'):
            assert got[job] == [(T0, _job_sum(job, 0)), (T0 + 60, _job_sum(job, 2)),
                                (END, _job_sum(job, 4))]

    def test_fused_by_name_keeps_the_name(self):
        self.setup_fleet()
        result = self.range_query('sum by (__name__) (mem_usage)')
        assert len(result.result) == 1
        assert result.result[0].metric == {'__name__': 'mem_usage'}

    def test_fused_over_shifted_selector(self):
        """The fused request carries the resolved window ends, so a modifier
        on the inner selector shifts the fold too."""
        self.setup_fleet()
        result = self.range_query('sum by (job) (mem_usage offset 60s)')
        got = self.points_by_label(result, 'job')
        for job in ('api', 'web'):
            assert got[job] == [(T0 + 60, _job_sum(job, 0)), (END, _job_sum(job, 2))]

    def test_fused_at_a_step_finer_than_cadence(self):
        """Raw series from the shards are folded into the partials on the
        coordinator; the merge order changes but the groups do not."""
        self.setup_fleet()
        result = self.range_query('sum by (job) (mem_usage)', step='10s')
        got = self.points_by_label(result, 'job')
        for job in ('api', 'web'):
            want = [(T0 + i, _job_sum(job, i // 30)) for i in range(0, 121, 10)]
            assert got[job] == want, job

    def test_selecting_aggregations_are_fused_per_step(self):
        """topk and friends fuse too: each shard ships its own per-step
        picks and the coordinator selects across them, so what crosses the
        wire is k series per shard per step rather than every series. The
        answers are the ones the coordinator-side selection gave."""
        self.setup_fleet()
        result = self.range_query('topk(1, mem_usage)')
        got = self.points_by_label(result, 'instance')
        assert list(got) == ['api-3']
        assert got['api-3'] == [(T0, 100.0), (T0 + 60, 300.0), (END, 500.0)]

        result = self.range_query('bottomk(1, mem_usage) by (job)')
        got = self.points_by_label(result, 'instance')
        # web-3 (7, 7, 8) overtakes web-1 (2, 6, 10) at the last step.
        assert got == {
            'api-1': [(T0, 1.0), (T0 + 60, 3.0), (END, 5.0)],
            'web-1': [(T0, 2.0), (T0 + 60, 6.0)],
            'web-3': [(END, 8.0)],
        }

        result = self.range_query('quantile(0.5, mem_usage) by (job)')
        assert set(self.points_by_label(result, 'job')) == {'api', 'web'}

    # ── push-down on/off equivalence ──────────────────────────────────

    EQUIVALENCE_EXACT = [
        'mem_usage',
        'mem_usage offset 30s',
        f'mem_usage @ {END}',
        'mem_usage @ start()',
        'timestamp(mem_usage)',
        'mem_usage / on(job, instance) mem_usage offset 60s',
        'abs(mem_usage) + mem_usage',
        'topk(2, mem_usage)',
        'topk(1, mem_usage) by (job)',
        'bottomk(4, mem_usage)',
        'limitk(2, mem_usage)',
        'limit_ratio(0.5, mem_usage)',
        'count_values("v", mem_usage)',
        'count_values("v", mem_usage) by (job)',
        'topk(2, sum_over_time(mem_usage[60s]))',
        'timestamp(topk(1, mem_usage))',
        'sparse_metric',
        'no_such_metric',
        'mem_usage{job="api"} and on(job) mem_usage{instance="api-2"}',
    ]

    EQUIVALENCE_FUSED = [
        'sum(mem_usage)',
        'avg by (job) (mem_usage)',
        'count(mem_usage)',
        'min by (job) (mem_usage)',
        'max without (instance) (mem_usage)',
        'stddev by (job) (mem_usage)',
        'stdvar(mem_usage)',
        'group by (job) (mem_usage)',
        'sum by (job) (mem_usage offset 60s)',
        f'sum by (job) (mem_usage @ {END})',
        'avg(mem_usage) / on() group_right mem_usage',
        'count(sparse_metric)',
    ]

    def _equivalence(self, queries: List[str], step: str, near: bool):
        on = [self.range_query(q, step=step) for q in queries]
        with self.pushdown_disabled():
            off = [self.range_query(q, step=step) for q in queries]
        compare = self.assert_steps_near if near else self.assert_steps_exact
        for query, a, b in zip(queries, on, off):
            compare(self.matrix_by_labelset(a), self.matrix_by_labelset(b),
                    context=f'push-down mismatch for `{query}` at step {step}')

    def test_pushdown_on_off_equivalence(self):
        """Toggling the coordinator config off ships the raw span and steps it
        locally. Same series, same timestamps, same values — exactly. At a 60s
        step the shards answer stepped; at 10s the size rule makes them answer
        raw, and both must still agree with the coordinator-side path."""
        self.setup_fleet()
        for step in ('60s', '10s'):
            self._equivalence(self.EQUIVALENCE_EXACT, step, near=False)

    def test_fused_pushdown_on_off_equivalence(self):
        """Fused aggregation: shape exactly, values to a relative 1e-12."""
        self.setup_fleet()
        for step in ('60s', '10s'):
            self._equivalence(self.EQUIVALENCE_FUSED, step, near=True)


class TestPromQLSelectorPushdownLimitsCluster(SelectorPushdownClusterBase):
    """The query limits, seeded at startup, applied to what the grid path
    returns rather than to what the shards read."""

    # Each shard's span for a selector is at most two series times five
    # samples. A budget of 12 admits every shard's read and refuses a
    # coordinator total of 15 stepped points; a series limit of 4 admits each
    # shard's two and refuses the six the unfiltered selector returns.
    MAX_SAMPLES = 12
    MAX_SERIES = 4

    def get_config_file_lines(self, test_dir, port) -> List[str]:
        lines = super().get_config_file_lines(test_dir, port)
        return [
            f'{line} ts-promql-max-samples-per-query {self.MAX_SAMPLES} '
            f'ts-promql-max-response-series {self.MAX_SERIES}'
            if line.startswith('loadmodule ') else line
            for line in lines
        ]

    def test_sample_budget_counts_stepped_points(self):
        """The budget is charged with the points the grid returns: the three
        `api` series over five 30s steps are 15 > 12 points, even though no
        single shard read more than 5 samples for them."""
        self.setup_fleet()
        with pytest.raises(ResponseError, match='too many samples'):
            self.range_query('mem_usage{job="api"}', step='30s')
        # A coarser grid fits: three steps, nine points.
        result = self.range_query('mem_usage{job="api"}')
        assert len(result.result) == 3
        # A fused request returns groups × steps — well under the budget.
        result = self.range_query('sum by (job) (mem_usage)', step='30s')
        assert len(result.result) == 2

    def test_max_series_bounds_what_the_query_returns(self):
        """`max_series` bounds the series returned, not the ones a shard
        matched: the six-series selector is refused, the two-group fold and
        the two-series selector are not."""
        self.setup_fleet()
        with pytest.raises(ResponseError, match='max series'):
            self.range_query('mem_usage', end=T0 + 60)
        assert len(self.range_query('sum by (job) (mem_usage)').result) == 2
        assert len(self.range_query('mem_usage{job="web"}', end=T0 + 60).result) == 3
