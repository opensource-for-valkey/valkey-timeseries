"""Cluster integration tests for the data-derived filter push-down
(TS.QUERYRANGE, `ts-promql-derived-filter-pushdown`).

A range query's binary operation — `a{job="api"} - b`, `a and on(job) b{...}`,
`sum by (job) (a) / on(job) group_left count by (job) (b)` — reads both operands
over the whole step grid before anything is matched, so the wider operand ships
every series it has and the coordinator discards the ones that could never
match. The push-down asks each shard, before planning, which labels every
series of an operand carries (`label-profile` fanout), adds those as matchers to
the other operand where they provably prune, and plans the narrowed tree. See
`src/promql/engine/derived_filters.rs` and
`docs/plans/derived-filter-pushdown-plan.md`.

What this file is for:

* The *answers* over a real 3-shard cluster, hand-computed, for each shape the
  push-down narrows and for the ones it must leave alone (`or`), with the
  push-down on and off. The toggle is a module load argument, so on and off are
  two test classes over one set of expectations; a divergence between them is a
  defect in the narrowing.
* Direct evidence that fewer series were read. `ts-promql-max-response-series`
  bounds what a selector read returns: with it set to 4, `mem_usage{job="api"} +
  mem_usage` reads six series for the right operand without the push-down and
  is refused, and reads three with it and succeeds. The same limit refuses
  `... or mem_usage` either way, because `or` may not be narrowed.

The fixture is the stepped selector suite's: six `mem_usage` gauges over two
jobs, each series on a known primary, both jobs straddling shards.
"""

from typing import List

import pytest
from valkey import ResponseError
from valkeytestframework.conftest import resource_port_tracker

from query_result import QueryResult
from test_ts_query_selector_pushdown_cme import (
    END,
    GAUGE_SERIES,
    T0,
    SelectorPushdownClusterBase,
    _job_sum,
)

# The default 60s grid over T0..T0+120 picks sample index 0, 2 and 4.
STEPS = (T0, T0 + 60, T0 + 120)
STEP_INDEX = (0, 2, 4)


def _instances(job: str) -> List[str]:
    return [i for i, (j, _, _) in GAUGE_SERIES.items() if j == job]


def _series_points(instance: str) -> list:
    """`(step, value)` for one gauge at every step of the default grid."""
    values = GAUGE_SERIES[instance][2]
    return [(step, float(values[index])) for step, index in zip(STEPS, STEP_INDEX)]


class DerivedFilterPushdownBase(SelectorPushdownClusterBase):
    """One class per toggle value; the shapes are shared below."""

    DERIVED_FILTER_PUSHDOWN = 'yes'
    MAX_SERIES = 0

    def get_config_file_lines(self, test_dir, port) -> List[str]:
        lines = super().get_config_file_lines(test_dir, port)
        extra = f' ts-promql-derived-filter-pushdown {self.DERIVED_FILTER_PUSHDOWN}'
        if self.MAX_SERIES:
            extra += f' ts-promql-max-response-series {self.MAX_SERIES}'
        return [f'{line}{extra}' if line.startswith('loadmodule ') else line
                for line in lines]

    def by_instance(self, query: str, **kwargs) -> dict:
        return self.points_by_label(self.range_query(query, **kwargs), 'instance')


class DerivedFilterShapes:
    """The shapes, as test methods, mixed into the on and the off class."""

    # ── arithmetic over a selective and a wide operand ─────────────────

    def test_selective_side_narrows_the_wide_one(self):
        """`job="api"` (and the three api instances) cross to the right
        operand; the answer is the api series' step-over-step change. With
        `offset 60s` the first step has no sample to shift to and is absent."""
        self.setup_fleet()
        got = self.by_instance('mem_usage{job="api"} - mem_usage offset 60s')
        expected = {}
        for instance in _instances('api'):
            values = GAUGE_SERIES[instance][2]
            expected[instance] = [
                (T0 + 60, float(values[2] - values[0])),
                (T0 + 120, float(values[4] - values[2])),
            ]
        self.assert_steps_exact(got, expected, 'api minus previous minute')

    def test_group_left_join_on_instance(self):
        """`on(instance) group_left`: only `instance` crosses (the derived
        three-value alternation), and every api series divides by itself."""
        self.setup_fleet()
        got = self.by_instance(
            'mem_usage{job="api"} / on(instance) group_left mem_usage')
        expected = {i: [(step, 1.0) for step in STEPS] for i in _instances('api')}
        self.assert_steps_exact(got, expected, 'self-join')

    # ── set operations ─────────────────────────────────────────────────

    def test_and_narrows_the_left_operand(self):
        """`and on(job)`: the right operand's `job="web"` is what the match
        needs, so the left reads the web series only — and yields them, with
        their own values and names intact."""
        self.setup_fleet()
        result = self.range_query('mem_usage and on(job) mem_usage{instance="web-1"}')
        got = self.points_by_label(result, 'instance')
        expected = {i: _series_points(i) for i in _instances('web')}
        self.assert_steps_exact(got, expected, 'and on(job)')
        assert all(s.metric['__name__'] == 'mem_usage' for s in result.result)

    def test_unless_keeps_the_left_operands_complement(self):
        """`unless on(job)` with a web right side leaves the api series; the
        right's filters cross only under `on(job)`, and the left's never
        cross (a series absent from the right is exactly what survives)."""
        self.setup_fleet()
        got = self.by_instance('mem_usage unless on(job) mem_usage{job="web"}')
        expected = {i: _series_points(i) for i in _instances('api')}
        self.assert_steps_exact(got, expected, 'unless on(job)')

    def test_or_is_never_narrowed(self):
        """`or` keeps series from both sides, so nothing crosses: the api
        series and web-1, each with its own values."""
        self.setup_fleet()
        got = self.by_instance('mem_usage{job="api"} or mem_usage{instance="web-1"}')
        expected = {i: _series_points(i) for i in _instances('api') + ['web-1']}
        self.assert_steps_exact(got, expected, 'or')

    # ── aggregated and rolled-up operands ──────────────────────────────

    def test_grouping_labels_cross_an_aggregation(self):
        """`sum by (job)` offers `job="api"` to the right, where it narrows
        the `count by (job)` to the api series — the answer is the api sum
        over three, exactly."""
        self.setup_fleet()
        got = self.points_by_label(self.range_query(
            'sum by (job) (mem_usage{job="api"}) '
            '/ on(job) group_left count by (job) (mem_usage)'), 'job')
        expected = {'api': [(step, _job_sum('api', index) / 3)
                            for step, index in zip(STEPS, STEP_INDEX)]}
        self.assert_steps_near(got, expected, 'sum by / count by')

    def test_rollup_operands_are_narrowed_at_their_selectors(self):
        """The matchers land on the selector under the rollup; each api
        window divides by itself."""
        self.setup_fleet()
        got = self.by_instance(
            'sum_over_time(mem_usage{job="api"}[60s]) '
            '/ on(instance) sum_over_time(mem_usage[60s])')
        expected = {i: [(step, 1.0) for step in STEPS] for i in _instances('api')}
        self.assert_steps_exact(got, expected, 'rollup self-join')

    # ── edges ──────────────────────────────────────────────────────────

    def test_an_operand_matching_nothing_is_harmless(self):
        """A profile of zero series derives nothing; the read then finds
        nothing and the operation is empty, not an error."""
        self.setup_fleet()
        result = self.range_query('mem_usage{job="api"} - no_such_metric')
        assert result.is_matrix()
        assert result.result == []

    def test_instant_queries_are_unaffected(self):
        """The instant path keeps its own, evaluation-time push-down."""
        self.setup_fleet()
        raw = self.coordinator().execute_command(
            'TS.QUERY', 'mem_usage{job="api"} - mem_usage offset 60s', 'TIME', END)
        result = QueryResult.from_raw(raw)
        got = {s.metric['instance']: s.value.value for s in result.result}
        expected = {i: float(GAUGE_SERIES[i][2][4] - GAUGE_SERIES[i][2][2])
                    for i in _instances('api')}
        assert got == expected


class TestPromQLDerivedFilterPushdownCluster(DerivedFilterShapes, DerivedFilterPushdownBase):
    """Push-down on (the default)."""

    def test_pushdown_is_on_by_default(self):
        raw = self.coordinator().execute_command(
            'CONFIG', 'GET', 'ts.ts-promql-derived-filter-pushdown')
        got = raw['ts.ts-promql-derived-filter-pushdown'] if isinstance(raw, dict) else raw[1]
        got = got.decode() if isinstance(got, bytes) else got
        assert got == 'yes'


class TestPromQLDerivedFilterPushdownOffCluster(DerivedFilterShapes, DerivedFilterPushdownBase):
    """Push-down off: the same answers from the unnarrowed trees."""

    DERIVED_FILTER_PUSHDOWN = 'no'


class TestPromQLDerivedFilterPushdownLimitsCluster(DerivedFilterPushdownBase):
    """Direct evidence of narrowing: a series limit the unnarrowed read
    exceeds and the narrowed one does not."""

    MAX_SERIES = 4

    def test_narrowed_operand_fits_the_series_limit(self):
        """`mem_usage` alone is six series and refused; narrowed to the api
        job by the left operand it is three, and the query succeeds."""
        self.setup_fleet()
        with pytest.raises(ResponseError, match='max series'):
            self.range_query('mem_usage', end=T0 + 60)
        got = self.by_instance('mem_usage{job="api"} + mem_usage')
        expected = {i: [(step, 2 * value) for step, value in _series_points(i)]
                    for i in _instances('api')}
        self.assert_steps_exact(got, expected, 'narrowed sum')

    def test_or_still_reads_the_whole_operand(self):
        """The control: `or` may not be narrowed, so the six-series read
        is refused exactly as it is without the push-down."""
        self.setup_fleet()
        with pytest.raises(ResponseError, match='max series'):
            self.range_query('mem_usage{job="api"} or mem_usage')


class TestPromQLDerivedFilterPushdownOffLimitsCluster(DerivedFilterPushdownBase):
    """The same limit with the push-down off refuses the wide operand."""

    DERIVED_FILTER_PUSHDOWN = 'no'
    MAX_SERIES = 4

    def test_unnarrowed_operand_exceeds_the_series_limit(self):
        self.setup_fleet()
        with pytest.raises(ResponseError, match='max series'):
            self.range_query('mem_usage{job="api"} + mem_usage')
