"""Single-node integration tests for batched PromQL selector reads.

On a single node the PromQL selector executor reads the keyspace itself, a share
of 512 series per module-lock hold among the tasks it has queued, decoding range
reads between holds (`run_local_batch` in
`src/promql/engine/selector_batch_executor.rs`). The fixture matches more than
two holds' worth of series, so every read below crosses hold boundaries: the
instant selector, the raw vector an aggregation is evaluated over, range and
grid reads, the label profile behind derived filters, and ACL enforcement.

The cluster counterpart is `test_ts_query_batched_selector_cme.py`.
"""

from datetime import datetime, timezone

import pytest
from valkey import ResponseError
from valkeytestframework.conftest import resource_port_tracker

from query_result import QueryResult
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase

WIDE_COUNT = 1300
SIDE_IDS = (3, 700, WIDE_COUNT - 1)

T0 = int(datetime(2026, 4, 6, 20, 0, 0, tzinfo=timezone.utc).timestamp())
OFFSETS = (0, 30, 60)
END = T0 + OFFSETS[-1]


def _rfc3339(epoch_seconds: int) -> str:
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).strftime(
        '%Y-%m-%dT%H:%M:%SZ')


def _value(i: int, k: int) -> float:
    """Series `i`'s sample at offset index `k`: integers, so every sum is exact."""
    return float(i * 10 + k)


def _group(i: int) -> str:
    return 'even' if i % 2 == 0 else 'odd'


class TestBatchedLocalSelectorReads(ValkeyTimeSeriesTestCaseBase):

    def setup_fleet(self):
        # The default (1000) is below the fixture; the limit is not what these
        # tests are about.
        self.client.execute_command('CONFIG', 'SET', 'ts.ts-promql-max-response-series', '0')
        pipe = self.client.pipeline(transaction=False)
        series = [(f'wide:{i}', f'wide{{i="{i}",g="{_group(i)}"}}', i) for i in range(WIDE_COUNT)]
        series += [(f'side:{i}', f'side{{i="{i}"}}', i) for i in SIDE_IDS]
        for key, metric, i in series:
            pipe.execute_command('TS.CREATE', key, 'METRIC', metric)
            for k, offset in enumerate(OFFSETS):
                pipe.execute_command('TS.ADD', key, (T0 + offset) * 1000, _value(i, k))
        pipe.execute()

    def instant_query(self, query: str, client=None) -> QueryResult:
        client = client or self.client
        return QueryResult.from_raw(
            client.execute_command('TS.QUERY', query, 'TIME', str(END)))

    def range_query(self, query: str) -> QueryResult:
        return QueryResult.from_raw(self.client.execute_command(
            'TS.QUERYRANGE', query, 'STEP', '30s',
            'START', _rfc3339(T0), 'END', _rfc3339(END)))

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
    def group_sums(k: int) -> dict:
        sums = {}
        for i in range(WIDE_COUNT):
            sums[_group(i)] = sums.get(_group(i), 0.0) + _value(i, k)
        return sums

    def test_instant_selector_returns_every_series_once(self):
        self.setup_fleet()

        got = self.instants_by(self.instant_query('wide'), 'i')

        assert got == {str(i): _value(i, 2) for i in range(WIDE_COUNT)}

    def test_instant_aggregation_over_every_hold(self):
        self.setup_fleet()

        assert self.instants_by(self.instant_query('sum by (g) (wide)'), 'g') == self.group_sums(2)
        count = self.instant_query('count(wide)')
        assert count.result[0].value.value == float(WIDE_COUNT)

    def test_range_selector_returns_every_point(self):
        self.setup_fleet()

        got = self.ranges_by(self.range_query('wide'), 'i')

        assert got == {str(i): self.expected_points(i) for i in range(WIDE_COUNT)}

    def test_range_aggregation_over_every_hold(self):
        self.setup_fleet()

        got = self.ranges_by(self.range_query('sum by (g) (wide)'), 'g')

        expected = {g: [] for g in ('even', 'odd')}
        for k, offset in enumerate(OFFSETS):
            for g, total in self.group_sums(k).items():
                expected[g].append((T0 + offset, total))
        assert got == expected

    def test_queries_sharing_holds_answer_independently(self):
        """Selectors of one expression are queued together and share each hold;
        a small one must not lose or borrow series from a large one."""
        self.setup_fleet()

        got = self.instants_by(self.instant_query('wide + on(i) side'), 'i')

        assert got == {str(i): 2 * _value(i, 2) for i in SIDE_IDS}

    def test_join_narrowed_by_a_derived_filter(self):
        self.setup_fleet()

        for query in ('wide and on(i) side', 'side and on(i) wide'):
            got = self.ranges_by(self.range_query(query), 'i')
            assert sorted(got, key=int) == [str(i) for i in SIDE_IDS], query

    def test_acl_identity_holds_in_every_hold(self):
        self.setup_fleet()
        self.client.execute_command(
            'ACL', 'SETUSER', 'wideonly', 'RESET', 'ON', '>pw',
            '+@timeseries', '+@read', '~wide:*')
        user = self.server.get_new_client()
        user.execute_command('AUTH', 'wideonly', 'pw')

        got = self.instants_by(self.instant_query('wide', client=user), 'i')
        assert got == {str(i): _value(i, 2) for i in range(WIDE_COUNT)}

        # Created last, so it has the highest id and is read in the last hold.
        self.client.execute_command('TS.CREATE', 'hidden', 'METRIC', 'wide{i="hidden",g="even"}')
        self.client.execute_command('TS.ADD', 'hidden', END * 1000, 1)
        with pytest.raises(ResponseError, match='(?i)permission'):
            self.instant_query('wide', client=user)


# ── Heavy series: holds bounded by copied bytes, not just series ──────────

# 48-byte uncompressed chunks hold three samples; each weighs 112 (the chunk
# struct) + 48 bytes against a hold's 4 MiB copy budget (`DEFAULT_BATCH_COPY_BYTES`).
HEAVY_COUNT = 200
HEAVY_CHUNK_SAMPLES = 3
HEAVY_SAMPLES = 900
HEAVY_CHUNK_WEIGHT = 112 + 48
COPY_BUDGET = 4 << 20


def _heavy_value(i: int, j: int) -> float:
    return float(i * 1000 + j)


class TestBatchedLocalCopyBudget(ValkeyTimeSeriesTestCaseBase):
    """200 series is a single hold by count, but their chunks weigh about
    9.6 MB, so a range read over all of them is split across holds by bytes.
    A series lost or read twice where a hold ends part-way through the planned
    ids changes the per-series counts and sums."""

    def setup_heavy(self, count=HEAVY_COUNT, metric='heavy'):
        pipe = self.client.pipeline(transaction=False)
        for i in range(count):
            key = f'{metric}:{i}'
            pipe.execute_command('TS.CREATE', key, 'ENCODING', 'UNCOMPRESSED',
                                 'CHUNK_SIZE', 48, 'METRIC', f'{metric}{{i="{i}"}}')
            args = []
            for j in range(HEAVY_SAMPLES):
                args += [key, (T0 + j) * 1000, _heavy_value(i, j)]
            pipe.execute_command('TS.MADD', *args)
        pipe.execute()

        info = self.client.execute_command('TS.INFO', f'{metric}:0')
        chunks = info[info.index(b'chunkCount') + 1]
        assert chunks == HEAVY_SAMPLES // HEAVY_CHUNK_SAMPLES, info

    def instant_query(self, query: str, time: int) -> QueryResult:
        return QueryResult.from_raw(
            self.client.execute_command('TS.QUERY', query, 'TIME', str(time)))

    def test_rollup_over_heavy_series_reads_every_sample_once(self):
        self.setup_heavy()
        assert HEAVY_COUNT * (HEAVY_SAMPLES // HEAVY_CHUNK_SAMPLES) * HEAVY_CHUNK_WEIGHT \
            > 2 * COPY_BUDGET, "the read must span more than two holds by bytes"
        at = T0 + HEAVY_SAMPLES - 1

        counts = TestBatchedLocalSelectorReads.instants_by(
            self.instant_query('count_over_time(heavy[1000s])', at), 'i')
        sums = TestBatchedLocalSelectorReads.instants_by(
            self.instant_query('sum_over_time(heavy[1000s])', at), 'i')

        assert counts == {str(i): float(HEAVY_SAMPLES) for i in range(HEAVY_COUNT)}
        assert sums == {
            str(i): sum(_heavy_value(i, j) for j in range(HEAVY_SAMPLES))
            for i in range(HEAVY_COUNT)
        }

    def test_sample_limit_admits_a_read_exactly_at_it(self):
        """The early check counts only chunks wholly inside the window, so it
        never refuses what the exact count admits: a window that cuts a chunk
        (500 samples, from j=301) and one that covers whole chunks (all 900)
        each pass at their exact count and fail one below it."""
        self.setup_heavy(count=1, metric='budget')
        cases = ((T0 + 800, '500s', 500), (T0 + HEAVY_SAMPLES - 1, '1000s', HEAVY_SAMPLES))
        try:
            for at, window, samples in cases:
                query = f'budget[{window}]'
                self.client.execute_command(
                    'CONFIG', 'SET', 'ts.ts-promql-max-samples-per-query', str(samples))
                result = self.instant_query(query, at)
                assert result.is_matrix() and len(result.result) == 1, query
                assert len(result.result[0].values) == samples, query

                self.client.execute_command(
                    'CONFIG', 'SET', 'ts.ts-promql-max-samples-per-query', str(samples - 1))
                with pytest.raises(ResponseError, match='too many samples'):
                    self.instant_query(query, at)
        finally:
            self.client.execute_command(
                'CONFIG', 'SET', 'ts.ts-promql-max-samples-per-query', '50000000')
