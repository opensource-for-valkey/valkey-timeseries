import pytest
from valkey import ResponseError, Valkey
from valkeytestframework.conftest import resource_port_tracker

from valkey_timeseries_test_case import (
    ValkeyTimeSeriesTestCaseBase,
    ValkeyTimeSeriesTestCaseDebugMode,
)


def populate(client, count=1300):
    pipe = client.pipeline(transaction=False)
    for i in range(count):
        key = f'series:{i}'
        pipe.execute_command(
            'TS.CREATE', key, 'LABELS', 'service', 'api', 'instance', f'host{i:04}',
        )
        # Equal groups of empty series and series on either range boundary or outside it.
        if i % 4:
            pipe.execute_command('TS.ADD', key, (i % 4) * 1000, i)
    pipe.execute()


class TestTsCardBackground(ValkeyTimeSeriesTestCaseBase):
    @pytest.mark.parametrize('protocol', [2, 3])
    def test_background_count_matches_inline_in_selected_database(self, protocol):
        client = Valkey(host=self.server.bind_ip, port=self.server.port,
                        db=1, protocol=protocol, socket_timeout=10)
        try:
            populate(client)
            # The worker must keep the caller's database, even when DB 0 has matching labels.
            populate(self.client, count=12)
            for range_args, filters, expected in [
                ((1000, 2000), ('service=api',), 650),
                (('NOT', 1000, 2000), ('service=api',), 650),
                ((2000, 2000), ('service=api',), 325),
                ((4000, 5000), ('service=api',), 0),
                (('NOT', 4000, 5000), ('service=api',), 1300),
                ((1000, 2000), ('service=api', 'instance!=host0001'), 649),
                ((1000, 2000), ('instance=~"host000[12]+"',), 2),
                ((1000, 2000), ('service=missing',), 0),
            ]:
                command = ('TS.CARD', 'FILTER_BY_RANGE', *range_args, 'FILTER', *filters)
                assert client.execute_command(*command) == expected
                tx = client.pipeline(transaction=True)
                tx.execute_command(*command)
                assert tx.execute() == [expected]
                assert client.eval('return redis.call(unpack(ARGV))', 0, *command) == expected
        finally:
            client.close()

    def test_background_count_fails_on_unreadable_series_after_two_batches(self):
        populate(self.client, count=1024)
        # This match is outside the range, but still requires permission. Its id follows
        # two complete batches the reader can access.
        self.client.execute_command('TS.ADD', 'secret', 4000, 1, 'LABELS', 'service', 'api')
        self.client.execute_command(
            'ACL', 'SETUSER', 'card-reader', 'on', '>password',
            '+ts.card', '+multi', '+exec', '~series:*',
        )
        reader = Valkey(host=self.server.bind_ip, port=self.server.port,
                        username='card-reader', password='password', socket_timeout=10)
        try:
            # Index-only filtered counts keep their existing metadata visibility.
            assert reader.execute_command('TS.CARD', 'FILTER', 'service=api') == 1025
            with pytest.raises(ResponseError, match='read permission'):
                reader.execute_command('TS.CARD')
            command = ('TS.CARD', 'FILTER_BY_RANGE', 1000, 2000, 'FILTER', 'service=api')
            with pytest.raises(ResponseError, match='read permission'):
                reader.execute_command(*command)
            tx = reader.pipeline(transaction=True)
            tx.execute_command(*command)
            with pytest.raises(ResponseError, match='read permission'):
                tx.execute()
            # A later job sees the caller's updated permissions.
            self.client.execute_command('ACL', 'SETUSER', 'card-reader', '~secret')
            assert reader.execute_command(*command) == 512
            assert reader.execute_command(
                'TS.CARD', 'FILTER_BY_RANGE', 'NOT', 1000, 2000, 'FILTER', 'service=api',
            ) == 513
        finally:
            reader.close()


class TestTsCardBackgroundScheduling(ValkeyTimeSeriesTestCaseDebugMode):
    def test_only_large_range_reads_use_the_lane(self):
        # The clients select different-sized databases; the panic hook is process-global.
        small = Valkey(host=self.server.bind_ip, port=self.server.port, db=1, socket_timeout=10)
        large = Valkey(host=self.server.bind_ip, port=self.server.port, db=0, socket_timeout=10)
        try:
            populate(small, count=512)
            populate(large, count=513)
            self.arm_analysis_job_panic(large)
            command = ('TS.CARD', 'FILTER_BY_RANGE', 1000, 2000, 'FILTER', 'service=api')
            assert small.execute_command(*command) == 256
            assert large.execute_command('TS.CARD') == 513
            assert large.execute_command('TS.CARD', 'FILTER', 'service=api') == 513
            for selector, expected in [
                ('instance=host0001', 1),
                ('instance=(host0001,host0002)', 2),
                # The parser simplifies this regex to the same cheap literal list.
                ('instance=~"host000[12]"', 2),
                ('service=missing', 0),
            ]:
                assert large.execute_command(
                    'TS.CARD', 'FILTER_BY_RANGE', 1000, 2000, 'FILTER', selector,
                ) == expected
            # Even a large read stays inline when the client cannot block.
            tx = large.pipeline(transaction=True)
            tx.execute_command(*command)
            assert tx.execute() == [256]
            assert large.eval('return redis.call(unpack(ARGV))', 0, *command) == 256
            with pytest.raises(ResponseError, match='requires at least one matcher'):
                large.execute_command('TS.CARD', 'FILTER_BY_RANGE', 1000, 2000)
            # All the reads above left the worker-only hook armed. 513 matches use the lane.
            with pytest.raises(ResponseError, match='internal error'):
                large.execute_command(*command)
            assert large.execute_command(*command) == 256
            assert large.ping()
        finally:
            small.close()
            large.close()

    def test_expensive_selectors_are_sized_by_database_before_planning(self):
        small = Valkey(host=self.server.bind_ip, port=self.server.port, db=1, socket_timeout=10)
        large = Valkey(host=self.server.bind_ip, port=self.server.port, db=0, socket_timeout=10)
        try:
            populate(small, count=12)
            populate(large)
            self.arm_analysis_job_panic(large)
            command = ('TS.CARD', 'FILTER_BY_RANGE', 1000, 2000,
                       'FILTER', 'instance=~"host000[12]+"')
            assert small.execute_command(*command) == 2
            # Index-only counts still use the inline bitmap path, including regex filters.
            assert large.execute_command('TS.CARD', 'FILTER', 'instance=~"host000[12]+"') == 2
            with pytest.raises(ResponseError, match='internal error'):
                large.execute_command(*command)
            assert large.execute_command(*command) == 2
        finally:
            small.close()
            large.close()
