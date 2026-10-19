import pytest
from valkey import ResponseError, Valkey
from valkeytestframework.conftest import resource_port_tracker

from common import LabelSearchResponse
from valkey_timeseries_test_case import (
    ValkeyTimeSeriesTestCaseBase,
    ValkeyTimeSeriesTestCaseDebugMode,
)


# Two label-index entries per series, plus `service=api`: 8,401 entries, over the 8,192-entry
# budget for a name or plain value scan, and over every other inline budget.
SERIES = 4200


def populate(client, count=SERIES):
    pipe = client.pipeline(transaction=False)
    for i in range(count):
        pipe.execute_command(
            'TS.CREATE', f'series:{i}', 'LABELS',
            'service', 'api', 'instance', f'host{i:04}', '__name__', f'metric{i:04}',
        )
    pipe.execute()


class TestLabelSearchBackground(ValkeyTimeSeriesTestCaseBase):
    @pytest.mark.parametrize('protocol', [2, 3])
    @pytest.mark.parametrize('command', [
        ('TS.LABELNAMES', 'INCLUDE_METADATA'),
        ('TS.LABELVALUES', 'instance', 'SEARCH', 'host', 'LIMIT', 3),
        ('TS.METRICNAMES', 'SEARCH', 'metric', 'SORTBY', 'cardinality', 'DESC', 'LIMIT', 3,
         'INCLUDE_METADATA'),
        ('TS.LABELVALUES', 'instance', 'FILTER', 'service=api', 'LIMIT', 3, 'INCLUDE_METADATA'),
    ])
    def test_background_label_search_preserves_database_and_inline_results(self, protocol, command):
        client = Valkey(host=self.server.bind_ip, port=self.server.port,
                        db=1, protocol=protocol, socket_timeout=10)
        try:
            populate(client)
            background = client.execute_command(*command)
            tx = client.pipeline(transaction=True)
            tx.execute_command(*command)
            assert tx.execute() == [background]

            parsed = LabelSearchResponse.parse(background)
            assert len(parsed.results) == 3
            if command[0] == 'TS.LABELNAMES':
                assert {r.value: r.cardinality for r in parsed.results} == {
                    b'__name__': SERIES, b'instance': SERIES, b'service': SERIES,
                }
            else:
                prefix = b'metric' if command[0] == 'TS.METRICNAMES' else b'host'
                values = [r.value for r in parsed.results]
                assert values == [prefix + b'0000', prefix + b'0001', prefix + b'0002']
                assert parsed.has_more

            # A script cannot block its client either.
            assert client.eval('redis.call(unpack(ARGV)); return 1', 0, *command) == 1
            assert client.ping()
        finally:
            client.close()

    def test_background_label_search_keeps_callers_acl_identity(self):
        populate(self.client)
        self.client.execute_command(
            'ACL', 'SETUSER', 'label-reader', 'on', '>password',
            '+ts.labelnames', '+ts.labelvalues', '+ts.metricnames', '~series:*',
        )
        reader = Valkey(host=self.server.bind_ip, port=self.server.port,
                        username='label-reader', password='password', socket_timeout=10)
        try:
            for command in [('TS.LABELNAMES',), ('TS.LABELVALUES', 'instance', 'SEARCH', 'host'),
                            ('TS.METRICNAMES', 'SORTBY', 'cardinality')]:
                with pytest.raises(ResponseError, match='insufficient permissions to query labels'):
                    reader.execute_command(*command)
            self.client.execute_command('ACL', 'SETUSER', 'label-reader', '~*')
            result = reader.execute_command('TS.LABELNAMES')
            assert len(LabelSearchResponse.parse(result).results) == 3
        finally:
            reader.close()


    @pytest.mark.parametrize('protocol', [2, 3])
    def test_background_querylabels_matches_inline(self, protocol):
        client = Valkey(host=self.server.bind_ip, port=self.server.port,
                        db=1, protocol=protocol, socket_timeout=10)
        try:
            populate(client)
            for command in [('TS.QUERYLABELS', 'LABELS'),
                            ('TS.QUERYLABELS', 'VALUES', 'instance'),
                            ('TS.QUERYLABELS', 'VALUES', 'instance', 'FILTER', 'service=api')]:
                background = client.execute_command(*command)
                tx = client.pipeline(transaction=True)
                tx.execute_command(*command)
                assert tx.execute() == [background]
                if command[1] == 'LABELS':
                    assert set(background) == {b'__name__', b'instance', b'service'}
                else:
                    assert len(background) == SERIES
                # A script cannot block its client either.
                assert client.eval('redis.call(unpack(ARGV)); return 1', 0, *command) == 1
        finally:
            client.close()

    def test_background_querylabels_omits_unreadable_series(self):
        populate(self.client)
        self.client.execute_command(
            'ACL', 'SETUSER', 'partial-reader', 'on', '>password', '+ts.querylabels', '+multi', '+exec',
            '~series:1*',
        )
        reader = Valkey(host=self.server.bind_ip, port=self.server.port,
                        username='partial-reader', password='password', socket_timeout=10)
        try:
            expected = {f'host{i:04}'.encode() for i in range(SERIES) if str(i).startswith('1')}
            background = reader.execute_command('TS.QUERYLABELS', 'VALUES', 'instance')
            assert set(background) == expected
            tx = reader.pipeline(transaction=True)
            tx.execute_command('TS.QUERYLABELS', 'VALUES', 'instance')
            assert set(tx.execute()[0]) == expected
        finally:
            reader.close()


class TestLabelSearchBackgroundScheduling(ValkeyTimeSeriesTestCaseDebugMode):
    def test_small_value_limit_stays_inline_and_large_name_scan_uses_lane(self):
        client = Valkey(host=self.server.bind_ip, port=self.server.port, socket_timeout=10)
        try:
            populate(client)
            self.arm_analysis_job_panic(client)
            # These cheap searches must leave the worker-only panic hook untouched.
            result = client.execute_command('TS.LABELVALUES', 'instance', 'LIMIT', 1)
            assert len(LabelSearchResponse.parse(result).results) == 1
            result = client.execute_command('TS.LABELVALUES', 'missing', 'SEARCH', 'host')
            assert not LabelSearchResponse.parse(result).results
            # A ranked scan of 4,200 metric names is under the 8,192-entry budget.
            result = client.execute_command(
                'TS.METRICNAMES', 'SORTBY', 'cardinality', 'DESC', 'LIMIT', 3)
            assert len(LabelSearchResponse.parse(result).results) == 3
            # Names require summing the values' cardinalities, despite LIMIT 1.
            with pytest.raises(ResponseError, match='internal error'):
                client.execute_command('TS.LABELNAMES', 'LIMIT', 1)
            assert client.ping()
            result = client.execute_command('TS.LABELNAMES')
            assert len(LabelSearchResponse.parse(result).results) == 3
        finally:
            client.close()


LANE_CASES = [
    ('TS.LABELNAMES', 'INCLUDE_METADATA'),
    ('TS.LABELVALUES', 'instance', 'SEARCH', 'host', 'LIMIT', 3),
    ('TS.METRICNAMES', 'SEARCH', 'metric', 'SORTBY', 'cardinality', 'DESC', 'LIMIT', 3,
     'INCLUDE_METADATA'),
    ('TS.LABELVALUES', 'instance', 'FILTER', 'service=api', 'LIMIT', 3, 'INCLUDE_METADATA'),
]


class TestLabelSearchLaneUse(ValkeyTimeSeriesTestCaseDebugMode):
    """Pins which searches leave the main thread: an armed panic hook fires only on the lane."""

    @pytest.mark.parametrize('command', LANE_CASES)
    def test_large_searches_run_on_the_lane(self, command):
        client = Valkey(host=self.server.bind_ip, port=self.server.port, socket_timeout=10)
        try:
            populate(client)
            self.arm_analysis_job_panic(client)
            with pytest.raises(ResponseError, match='internal error'):
                client.execute_command(*command)
            # The hook is spent: the same search now answers.
            assert LabelSearchResponse.parse(client.execute_command(*command)).results
        finally:
            client.close()

    def test_a_name_search_is_budgeted_like_a_name_scan(self):
        """~900 index entries: over the SEARCH value budget (256), under the name budget (8192)."""
        client = Valkey(host=self.server.bind_ip, port=self.server.port, socket_timeout=10)
        try:
            populate(client, count=300)
            self.arm_analysis_job_panic(client)
            result = client.execute_command('TS.LABELNAMES', 'SEARCH', 'serv', 'inst', 'name')
            assert LabelSearchResponse.parse(result).results
            # Still armed: the search above ran inline. A value search with SEARCH over the
            # same 300 values exceeds its budget and takes the lane.
            with pytest.raises(ResponseError, match='internal error'):
                client.execute_command('TS.LABELVALUES', 'instance', 'SEARCH', 'host')
        finally:
            client.close()

    def test_an_unauthorized_search_is_refused_before_the_lane(self):
        populate(self.client)
        self.client.execute_command(
            'ACL', 'SETUSER', 'narrow-reader', 'on', '>password',
            '+ts.labelnames', '+ts.labelvalues', '+ts.metricnames', '~series:*',
        )
        reader = Valkey(host=self.server.bind_ip, port=self.server.port,
                        username='narrow-reader', password='password', socket_timeout=10)
        try:
            self.arm_analysis_job_panic(self.client)
            for command in LANE_CASES:
                with pytest.raises(ResponseError, match='insufficient permissions to query labels'):
                    reader.execute_command(*command)
            # None of them reached the lane: the hook is still armed for the next job.
            with pytest.raises(ResponseError, match='internal error'):
                self.client.execute_command(*LANE_CASES[0])
        finally:
            reader.close()

    def test_querylabels_uses_the_lane_only_for_a_large_database(self):
        small = Valkey(host=self.server.bind_ip, port=self.server.port, db=1, socket_timeout=10)
        large = Valkey(host=self.server.bind_ip, port=self.server.port, db=0, socket_timeout=10)
        try:
            populate(small, count=300)
            populate(large)
            self.arm_analysis_job_panic(large)
            # 300 series: at most one batch, read inline, so the hook stays armed.
            assert set(small.execute_command('TS.QUERYLABELS', 'LABELS')) == {
                b'__name__', b'instance', b'service',
            }
            with pytest.raises(ResponseError, match='internal error'):
                large.execute_command('TS.QUERYLABELS', 'LABELS')
            assert large.execute_command('TS.QUERYLABELS', 'LABELS')
        finally:
            small.close()
            large.close()

    def test_a_narrow_equality_filter_stays_inline_on_a_large_database(self):
        """Equality filters are planned up front, so 1,300 series do not send them to the lane."""
        client = Valkey(host=self.server.bind_ip, port=self.server.port, socket_timeout=10)
        try:
            populate(client)
            self.arm_analysis_job_panic(client)
            assert client.execute_command(
                'TS.QUERYLABELS', 'VALUES', 'instance', 'FILTER', 'instance=host0001',
            ) == [b'host0001']
            assert set(client.execute_command(
                'TS.QUERYLABELS', 'LABELS', 'FILTER', 'instance=(host0001,host0002)',
            )) == {b'__name__', b'instance', b'service'}
            result = client.execute_command(
                'TS.LABELVALUES', 'instance', 'FILTER', 'instance=host0001')
            assert [r.value for r in LabelSearchResponse.parse(result).results] == [b'host0001']
            result = client.execute_command('TS.LABELNAMES', 'FILTER', 'instance=host0001')
            assert len(LabelSearchResponse.parse(result).results) == 3

            # Every search above ran inline: the hook is still armed. Filters that are not
            # planned up front are sized by the database and leave.
            for command in [
                ('TS.QUERYLABELS', 'VALUES', 'instance', 'FILTER', 'instance=~"host000.*"'),
                ('TS.LABELVALUES', 'instance', 'FILTER', 'instance=~"host000.*"'),
                # Narrow, but reporting name cardinalities walks the whole term dictionary.
                ('TS.LABELNAMES', 'FILTER', 'instance=host0001', 'INCLUDE_METADATA'),
            ]:
                self.arm_analysis_job_panic(client)
                with pytest.raises(ResponseError, match='internal error'):
                    client.execute_command(*command)
                # The hook is spent; the same search now answers.
                assert client.execute_command(*command)
        finally:
            client.close()

