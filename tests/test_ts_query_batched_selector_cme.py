"""Cluster integration tests for batched shard-side selector reads.

A shard answering its share of a PromQL fan-out no longer holds the module lock
for the whole read: it walks the matched series `DEFAULT_SERIES_BATCH_SIZE`
(512) at a time, taking the lock per batch, and decodes range reads after it is
released. See `src/series/index/batched_querier.rs`.

The fixture puts more than two batches' worth of series on one shard, so every
fan-out path below crosses batch boundaries: the instant selector, the instant
aggregation push-down, the grid push-down and the raw range read it falls back
to, and the derived-filter label profile. Answers are compared against
hand-computed values, and each push-down is checked on and off, so a series
lost or duplicated at a boundary shows up as a difference.

What this cannot observe is interleaving: nothing here writes between two
batches of one read. The per-batch id handling that interleaving relies on
(renames, deletes, flushes) is unit-tested in `batched_querier.rs`.
"""

from datetime import datetime, timezone

import pytest
from valkey import ResponseError
from valkeytestframework.conftest import resource_port_tracker

from query_result import QueryResult
from valkey_timeseries_test_case import ValkeyTimeSeriesClusterTestCase

AGGREGATION_PUSHDOWN_CONFIG = 'ts.ts-fanout-aggregation-pushdown'
ROLLUP_PUSHDOWN_CONFIG = 'ts.ts-fanout-rollup-pushdown'
MAX_RESPONSE_SERIES_CONFIG = 'ts.ts-promql-max-response-series'

# {h1} lands on primary 1 of the harness's 3-node cluster; see
# test_ts_query_selector_pushdown_cme.py, whose fixture check guards the mapping.
WIDE_TAG = 'h1'
OTHER_TAGS = ('h0', 'h2')

# 512 per batch: three batches on the wide shard, the last one partial.
WIDE_COUNT = 1300

T0 = int(datetime(2026, 4, 6, 20, 0, 0, tzinfo=timezone.utc).timestamp())
OFFSETS = (0, 30, 60)
END = T0 + OFFSETS[-1]

# The few `side` series the derived-filter queries join `wide` against, spread
# over the other two shards. Their `i` values fall in the first, middle and
# last batch of the wide shard.
SIDE_IDS = (3, 700, WIDE_COUNT - 1)


def _rfc3339(epoch_seconds: int) -> str:
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).strftime(
        '%Y-%m-%dT%H:%M:%SZ')


def _value(i: int, k: int) -> float:
    """Series `i`'s sample at offset index `k`: integers, so every sum is exact."""
    return float(i * 10 + k)


def _wide_ids() -> list:
    """Every `wide` series: the big shard's, then one on each other shard."""
    return list(range(WIDE_COUNT)) + [WIDE_COUNT + n for n in range(len(OTHER_TAGS))]


def _group(i: int) -> str:
    return 'even' if i % 2 == 0 else 'odd'


class TestBatchedSelectorReadsCluster(ValkeyTimeSeriesClusterTestCase):

    # ── fixture & helpers ─────────────────────────────────────────────

    def setup_fleet(self):
        # The default (1000) is below one shard's share here; the limit is not
        # what these tests are about.
        self.set_config(MAX_RESPONSE_SERIES_CONFIG, '0')

        wide_tag_of = {i: WIDE_TAG for i in range(WIDE_COUNT)}
        for n, tag in enumerate(OTHER_TAGS):
            wide_tag_of[WIDE_COUNT + n] = tag

        # One pipeline per primary: every key in it carries that primary's tag.
        by_tag = {}
        for i, tag in wide_tag_of.items():
            by_tag.setdefault(tag, []).append(
                (f'wide:{i}:{{{tag}}}', f'wide{{i="{i}",g="{_group(i)}"}}', i))
        for n, i in enumerate(SIDE_IDS):
            tag = OTHER_TAGS[n % len(OTHER_TAGS)]
            by_tag.setdefault(tag, []).append(
                (f'side:{i}:{{{tag}}}', f'side{{i="{i}"}}', i))

        for node, tag in ((0, 'h2'), (1, 'h1'), (2, 'h0')):
            pipe = self.new_client_for_primary(node).pipeline(transaction=False)
            for key, metric, i in by_tag.get(tag, []):
                pipe.execute_command('TS.CREATE', key, 'METRIC', metric)
                for k, offset in enumerate(OFFSETS):
                    pipe.execute_command('TS.ADD', key, (T0 + offset) * 1000, _value(i, k))
            pipe.execute()

        wide_shard = self.new_client_for_primary(1)
        assert wide_shard.execute_command('DBSIZE') >= WIDE_COUNT, \
            "the wide shard must hold more than two batches of matching series"

    def coordinator(self, client=None):
        return client or self.new_client_for_primary(0)

    def instant_query(self, query: str, time=END, client=None) -> QueryResult:
        raw = self.coordinator(client).execute_command('TS.QUERY', query, 'TIME', str(time))
        return QueryResult.from_raw(raw)

    def range_query(self, query: str, step='30s') -> QueryResult:
        raw = self.coordinator().execute_command(
            'TS.QUERYRANGE', query, 'STEP', step,
            'START', _rfc3339(T0), 'END', _rfc3339(END))
        return QueryResult.from_raw(raw)

    def set_config(self, name, value):
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
    def instants_by(result: QueryResult, label: str) -> dict:
        assert result.is_vector(), f"expected vector, got {result.result_type}"
        out = {}
        for sample in result.result:
            key = sample.metric[label]
            assert key not in out, f"duplicate series for {label}={key}"
            out[key] = sample.value.value
        return out

    @staticmethod
    def ranges_by(result: QueryResult, label: str) -> dict:
        assert result.is_matrix(), f"expected matrix, got {result.result_type}"
        out = {}
        for series in result.result:
            key = series.metric[label]
            assert key not in out, f"duplicate series for {label}={key}"
            out[key] = [(s.timestamp // 1000, s.value) for s in series.values]
        return out

    @staticmethod
    def expected_points(i: int) -> list:
        return [(T0 + offset, _value(i, k)) for k, offset in enumerate(OFFSETS)]

    @staticmethod
    def expected_group_sums(k: int) -> dict:
        sums = {}
        for i in _wide_ids():
            sums[_group(i)] = sums.get(_group(i), 0.0) + _value(i, k)
        return sums

    # ── instant reads ─────────────────────────────────────────────────

    def test_instant_selector_returns_every_series_once(self):
        self.setup_fleet()

        got = self.instants_by(self.instant_query('wide'), 'i')

        assert got == {str(i): _value(i, 2) for i in _wide_ids()}

    def test_instant_aggregation_sums_across_batches(self, aggregation_pushdown):
        self.setup_fleet()

        got = self.instants_by(self.instant_query('sum by (g) (wide)'), 'g')

        assert got == self.expected_group_sums(2)

    def test_instant_count_sees_every_batch(self, aggregation_pushdown):
        self.setup_fleet()

        result = self.instant_query('count(wide)')

        assert result.is_vector() and len(result.result) == 1
        assert result.result[0].value.value == float(len(_wide_ids()))

    # ── range reads ───────────────────────────────────────────────────

    def test_range_selector_returns_every_point(self, rollup_pushdown):
        """Push-down on: the grid read. Off: the raw range read. Both copy chunks
        per batch and decode after the lock is released."""
        self.setup_fleet()

        got = self.ranges_by(self.range_query('wide'), 'i')

        assert got == {str(i): self.expected_points(i) for i in _wide_ids()}

    def test_range_aggregation_sums_across_batches(self, rollup_pushdown):
        self.setup_fleet()

        got = self.ranges_by(self.range_query('sum by (g) (wide)'), 'g')

        expected = {g: [] for g in ('even', 'odd')}
        for k, offset in enumerate(OFFSETS):
            for g, total in self.expected_group_sums(k).items():
                expected[g].append((T0 + offset, total))
        assert got == expected

    # ── derived filters (label profile) ───────────────────────────────

    def test_join_narrowed_by_a_derived_filter(self):
        """`side` is small and `wide` spans three batches on one shard: whichever
        operand is profiled, the join must keep exactly the shared `i`s."""
        self.setup_fleet()

        for query in ('wide and on(i) side', 'side and on(i) wide'):
            metric = query.split()[0]
            got = self.ranges_by(self.range_query(query), 'i')
            assert sorted(got, key=int) == [str(i) for i in SIDE_IDS], query
            if metric == 'wide':
                for i in SIDE_IDS:
                    assert got[str(i)] == self.expected_points(i), query

    # ── ACL ───────────────────────────────────────────────────────────

    def test_acl_identity_holds_in_every_batch(self):
        """Each batch takes the lock afresh and reinstalls the caller's identity.
        A user who may read `wide:*` gets the whole answer; one more matching
        series outside the grant, in the last batch, fails the read closed."""
        self.setup_fleet()
        for i in range(self.CLUSTER_SIZE):
            self.new_client_for_primary(i).execute_command(
                'ACL', 'SETUSER', 'wideonly', 'RESET', 'ON', '>pw',
                '+@timeseries', '+@read', '~wide:*')
        user = self.new_client_for_primary(0)
        user.execute_command('AUTH', 'wideonly', 'pw')

        got = self.instants_by(self.instant_query('wide', client=user), 'i')
        assert got == {str(i): _value(i, 2) for i in _wide_ids()}

        # Created last, so it has the highest id on the wide shard: the read
        # walks ids in order, so this is in the final batch.
        hidden = f'hidden:{{{WIDE_TAG}}}'
        wide_shard = self.new_client_for_primary(1)
        wide_shard.execute_command(
            'TS.CREATE', hidden, 'METRIC', f'wide{{i="hidden",g="even"}}')
        wide_shard.execute_command('TS.ADD', hidden, END * 1000, 1)

        with pytest.raises(ResponseError, match='(?i)permission'):
            self.instant_query('wide', client=user)


# ── Heavy series: batches bounded by copied bytes, not just series ────────

# 48-byte uncompressed chunks hold three samples; each weighs 112 (the chunk
# struct) + 48 bytes against a batch's 4 MiB copy budget (`DEFAULT_BATCH_COPY_BYTES`).
HEAVY_COUNT = 200
HEAVY_CHUNK_SAMPLES = 3
HEAVY_SAMPLES = 900
HEAVY_CHUNK_WEIGHT = 112 + 48
COPY_BUDGET = 4 << 20
MAX_SAMPLES_CONFIG = 'ts.ts-promql-max-samples-per-query'


def _heavy_value(i: int, j: int) -> float:
    return float(i * 1000 + j)


class TestBatchedCopyBudgetCluster(ValkeyTimeSeriesClusterTestCase):
    """200 series on one shard is a single batch by count, but their chunks
    weigh about 9.6 MB, so the shard's share is split across batches by bytes,
    on both the grid push-down and the raw range read."""

    def setup_heavy(self, count=HEAVY_COUNT, metric='heavy'):
        shard = self.new_client_for_primary(1)
        pipe = shard.pipeline(transaction=False)
        for i in range(count):
            key = f'{metric}:{i}:{{{WIDE_TAG}}}'
            pipe.execute_command('TS.CREATE', key, 'ENCODING', 'UNCOMPRESSED',
                                 'CHUNK_SIZE', 48, 'METRIC', f'{metric}{{i="{i}"}}')
            args = []
            for j in range(HEAVY_SAMPLES):
                args += [key, (T0 + j) * 1000, _heavy_value(i, j)]
            pipe.execute_command('TS.MADD', *args)
        pipe.execute()

        info = shard.execute_command('TS.INFO', f'{metric}:0:{{{WIDE_TAG}}}')
        chunks = info[info.index(b'chunkCount') + 1]
        assert chunks == HEAVY_SAMPLES // HEAVY_CHUNK_SAMPLES, info

    def instant_query(self, query: str, time: int) -> QueryResult:
        raw = self.new_client_for_primary(0).execute_command(
            'TS.QUERY', query, 'TIME', str(time))
        return QueryResult.from_raw(raw)

    def set_config(self, name, value):
        for i in range(self.CLUSTER_SIZE):
            self.new_client_for_primary(i).execute_command('CONFIG', 'SET', name, value)

    @pytest.fixture(params=['yes', 'no'], ids=['pushdown-on', 'pushdown-off'])
    def rollup_pushdown(self, request):
        self.set_config(ROLLUP_PUSHDOWN_CONFIG, request.param)
        yield request.param
        self.set_config(ROLLUP_PUSHDOWN_CONFIG, 'yes')

    def test_rollup_over_heavy_series_reads_every_sample_once(self, rollup_pushdown):
        self.setup_heavy()
        assert HEAVY_COUNT * (HEAVY_SAMPLES // HEAVY_CHUNK_SAMPLES) * HEAVY_CHUNK_WEIGHT \
            > 2 * COPY_BUDGET, "the share must span more than two batches by bytes"
        at = T0 + HEAVY_SAMPLES - 1

        counts = TestBatchedSelectorReadsCluster.instants_by(
            self.instant_query('count_over_time(heavy[1000s])', at), 'i')
        sums = TestBatchedSelectorReadsCluster.instants_by(
            self.instant_query('sum_over_time(heavy[1000s])', at), 'i')

        assert counts == {str(i): float(HEAVY_SAMPLES) for i in range(HEAVY_COUNT)}
        assert sums == {
            str(i): sum(_heavy_value(i, j) for j in range(HEAVY_SAMPLES))
            for i in range(HEAVY_COUNT)
        }

    def test_sample_limit_admits_a_read_exactly_at_it(self):
        """The shard's early check counts only chunks wholly inside the window,
        so it never refuses what the exact count admits. One series, so the
        shard's share is the whole read."""
        self.setup_heavy(count=1, metric='budget')
        cases = ((T0 + 800, '500s', 500), (T0 + HEAVY_SAMPLES - 1, '1000s', HEAVY_SAMPLES))
        try:
            for at, window, samples in cases:
                query = f'budget[{window}]'
                self.set_config(MAX_SAMPLES_CONFIG, str(samples))
                result = self.instant_query(query, at)
                assert result.is_matrix() and len(result.result) == 1, query
                assert len(result.result[0].values) == samples, query

                self.set_config(MAX_SAMPLES_CONFIG, str(samples - 1))
                # The shard refuses its share; the fan-out currently replaces a
                # shard's error text with a generic one.
                with pytest.raises(ResponseError,
                                   match='too many samples|Internal error in fanout'):
                    self.instant_query(query, at)
        finally:
            self.set_config(MAX_SAMPLES_CONFIG, '50000000')
