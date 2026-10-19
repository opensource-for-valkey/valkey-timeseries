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


def keys(indexes):
    return sorted(f'series:{i}'.encode() for i in indexes)


class TestTsQueryIndexBackground(ValkeyTimeSeriesTestCaseBase):
    @pytest.mark.parametrize('protocol', [2, 3])
    def test_background_query_matches_inline_in_selected_database(self, protocol):
        client = Valkey(host=self.server.bind_ip, port=self.server.port,
                        db=1, protocol=protocol, socket_timeout=10)
        try:
            populate(client)
            # The worker must keep the caller's database, even when DB 0 has matching labels.
            populate(self.client, count=12)
            for args, expected in [
                (('service=api',), keys(range(1300))),
                (('FILTER_BY_RANGE', 1000, 2000, 'service=api'),
                 keys(i for i in range(1300) if i % 4 in (1, 2))),
                (('FILTER_BY_RANGE', 'NOT', 1000, 2000, 'service=api'),
                 keys(i for i in range(1300) if i % 4 in (0, 3))),
                (('service=api', 'instance!=host0001'), keys(i for i in range(1300) if i != 1)),
                (('instance=~"host000[12]+"',), keys([1, 2])),
                (('service=missing',), []),
            ]:
                command = ('TS.QUERYINDEX', *args)
                # Reply order is undefined, but background and inline reads list the same
                # keys in the same (index) order.
                background = client.execute_command(*command)
                assert sorted(background) == expected
                tx = client.pipeline(transaction=True)
                tx.execute_command(*command)
                assert tx.execute() == [background]
                assert client.eval('return redis.call(unpack(ARGV))', 0, *command) == background
        finally:
            client.close()

    def test_background_query_keeps_access_rules(self):
        populate(self.client, count=1024)
        # This match is outside the range, but still requires permission. Its id follows
        # two complete batches the reader can access.
        self.client.execute_command('TS.ADD', 'secret', 4000, 1, 'LABELS', 'service', 'api')
        self.client.execute_command(
            'ACL', 'SETUSER', 'index-reader', 'on', '>password',
            '+ts.queryindex', '+multi', '+exec', '~series:*',
        )
        reader = Valkey(host=self.server.bind_ip, port=self.server.port,
                        username='index-reader', password='password', socket_timeout=10)
        try:
            # Without FILTER_BY_RANGE the index answers, and unreadable keys are listed.
            assert sorted(reader.execute_command('TS.QUERYINDEX', 'service=api')) == sorted(
                keys(range(1024)) + [b'secret'])
            command = ('TS.QUERYINDEX', 'FILTER_BY_RANGE', 1000, 2000, 'service=api')
            with pytest.raises(ResponseError, match='read permission'):
                reader.execute_command(*command)
            tx = reader.pipeline(transaction=True)
            tx.execute_command(*command)
            with pytest.raises(ResponseError, match='read permission'):
                tx.execute()
            # A later job sees the caller's updated permissions.
            self.client.execute_command('ACL', 'SETUSER', 'index-reader', '~secret')
            assert sorted(reader.execute_command(*command)) == keys(
                i for i in range(1024) if i % 4 in (1, 2))
        finally:
            reader.close()


class TestTsQueryIndexBackgroundScheduling(ValkeyTimeSeriesTestCaseDebugMode):
    @pytest.mark.parametrize('range_args', [(), ('FILTER_BY_RANGE', 1000, 2000)])
    def test_only_large_queries_use_the_lane(self, range_args):
        # The clients select different-sized databases; the panic hook is process-global.
        small = Valkey(host=self.server.bind_ip, port=self.server.port, db=1, socket_timeout=10)
        large = Valkey(host=self.server.bind_ip, port=self.server.port, db=0, socket_timeout=10)
        try:
            populate(small, count=512)
            populate(large, count=513)
            self.arm_analysis_job_panic(large)
            command = ('TS.QUERYINDEX', *range_args, 'service=api')
            assert small.execute_command(*command)
            for selector in [
                'instance=host0001',
                'instance=(host0001,host0002)',
                # The parser simplifies this regex to the same cheap literal list.
                'instance=~"host000[12]"',
                'service=missing',
            ]:
                large.execute_command('TS.QUERYINDEX', *range_args, selector)
            # Even a large query stays inline when the client cannot block.
            tx = large.pipeline(transaction=True)
            tx.execute_command(*command)
            expected = tx.execute()[0]
            assert large.eval('return redis.call(unpack(ARGV))', 0, *command) == expected
            # All the queries above left the worker-only hook armed. 513 matches use the lane.
            with pytest.raises(ResponseError, match='internal error'):
                large.execute_command(*command)
            assert large.execute_command(*command) == expected
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
            command = ('TS.QUERYINDEX', 'instance=~"host000[12]+"')
            assert sorted(small.execute_command(*command)) == keys([1, 2])
            with pytest.raises(ResponseError, match='internal error'):
                large.execute_command(*command)
            assert sorted(large.execute_command(*command)) == keys([1, 2])
        finally:
            small.close()
            large.close()
