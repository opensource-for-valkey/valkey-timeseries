"""Compaction rules link their two series by key name.

A source's rule names its destination's key and the destination names its source's key. A rule
is honoured only while the destination still names the source (the back-link), so a key that
is deleted and re-created, overwritten by RENAME, or re-pointed at another source never receives
compaction output meant for another series. RENAME re-points the partners of the renamed key.
"""

from valkey.exceptions import ResponseError
from valkeytestframework.conftest import resource_port_tracker

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase

BUCKET = 1000


def _decode(value):
    return value.decode('utf-8') if isinstance(value, bytes) else value


class TestCompactionLinks(ValkeyTimeSeriesTestCaseBase):

    def create_rule(self, source, dest, aggregation='sum', bucket=BUCKET):
        self.client.execute_command('TS.CREATERULE', source, dest, 'AGGREGATION', aggregation, bucket)

    def source_key(self, key):
        return _decode(self.ts_info(key).get('sourceKey'))

    def rule_dests(self, key):
        return [rule.dest_key for rule in self.ts_info(key)['rules']]

    def add(self, key, *samples):
        for ts, value in samples:
            self.client.execute_command('TS.ADD', key, ts, value)

    def dest_samples(self, key):
        return [(int(ts), float(v)) for ts, v in self.client.execute_command('TS.RANGE', key, '-', '+')]

    def test_rename_source_keeps_rule(self):
        self.client.execute_command('TS.CREATE', 'src')
        self.client.execute_command('TS.CREATE', 'dst')
        self.create_rule('src', 'dst')
        self.add('src', (0, 1), (500, 2))

        self.client.rename('src', 'src:renamed')

        assert self.source_key('dst') == 'src:renamed'
        assert self.rule_dests('src:renamed') == ['dst']
        # Closing the first bucket through the renamed key still reaches the destination.
        self.add('src:renamed', (1000, 5))
        assert self.dest_samples('dst') == [(0, 3.0)]

    def test_rename_destination_keeps_rule(self):
        self.client.execute_command('TS.CREATE', 'src')
        self.client.execute_command('TS.CREATE', 'dst')
        self.create_rule('src', 'dst')
        self.add('src', (0, 1), (500, 2))

        self.client.rename('dst', 'dst:renamed')

        assert self.rule_dests('src') == ['dst:renamed']
        assert self.source_key('dst:renamed') == 'src'
        self.add('src', (1000, 5))
        assert self.dest_samples('dst:renamed') == [(0, 3.0)]
        # LATEST finds the open bucket through the renamed destination.
        latest = self.client.execute_command('TS.GET', 'dst:renamed', 'LATEST')
        assert (int(latest[0]), float(latest[1])) == (1000, 5.0)

    def test_recreated_destination_does_not_receive_output(self):
        self.client.execute_command('TS.CREATE', 'src')
        self.client.execute_command('TS.CREATE', 'dst')
        self.create_rule('src', 'dst')
        self.add('src', (0, 1))

        self.client.delete('dst')
        self.client.execute_command('TS.CREATE', 'dst')

        # The new `dst` is not a compaction destination, so the old rule is dead.
        assert self.rule_dests('src') == []
        self.add('src', (1000, 2), (2000, 3))
        assert self.dest_samples('dst') == []
        assert self.source_key('dst') is None

    def test_destination_claimed_by_another_source(self):
        """A stale rule must not feed a destination that now belongs to another source."""
        for key in ('a', 'b', 'dst'):
            self.client.execute_command('TS.CREATE', key)
        self.create_rule('a', 'dst')
        self.add('a', (0, 100))

        self.client.delete('dst')
        self.client.execute_command('TS.CREATE', 'dst')
        self.create_rule('b', 'dst')

        self.add('a', (1000, 100), (2000, 100))
        self.add('b', (0, 1), (1000, 2), (2000, 3))

        assert self.dest_samples('dst') == [(0, 1.0), (1000, 2.0)]
        assert self.source_key('dst') == 'b'
        assert self.rule_dests('a') == []
        assert self.rule_dests('b') == ['dst']

    def test_deleterule_of_stale_rule_keeps_live_link(self):
        for key in ('a', 'b', 'dst'):
            self.client.execute_command('TS.CREATE', key)
        self.create_rule('a', 'dst')

        self.client.delete('dst')
        self.client.execute_command('TS.CREATE', 'dst')
        self.create_rule('b', 'dst')

        # `a` still holds its (stale) rule for `dst`; deleting it must not unlink `b`'s rule.
        self.client.execute_command('TS.DELETERULE', 'a', 'dst')

        assert self.source_key('dst') == 'b'
        self.add('b', (0, 1), (1000, 2))
        assert self.dest_samples('dst') == [(0, 1.0)]

    def test_rename_over_destination(self):
        """RENAME onto a destination replaces it with an unrelated series."""
        for key in ('src', 'dst', 'other'):
            self.client.execute_command('TS.CREATE', key)
        self.create_rule('src', 'dst')
        self.add('other', (0, 42))

        self.client.rename('other', 'dst')

        self.add('src', (0, 1), (1000, 2))
        assert self.dest_samples('dst') == [(0, 42.0)]
        assert self.rule_dests('src') == []

    def test_links_survive_reload(self):
        self.client.execute_command('TS.CREATE', 'src')
        self.client.execute_command('TS.CREATE', 'mid')
        self.client.execute_command('TS.CREATE', 'top')
        self.create_rule('src', 'mid')
        self.create_rule('mid', 'top', bucket=2 * BUCKET)
        self.add('src', (0, 1), (500, 2))

        self.client.execute_command('DEBUG', 'RELOAD')

        assert self.rule_dests('src') == ['mid']
        assert self.source_key('mid') == 'src'
        assert self.rule_dests('mid') == ['top']
        assert self.source_key('top') == 'mid'
        self.add('src', (1000, 5), (2000, 7), (3000, 1))
        assert self.dest_samples('mid') == [(0, 3.0), (1000, 5.0), (2000, 7.0)]
        assert self.dest_samples('top') == [(0, 8.0)]


    def test_restore_under_another_key_does_not_adopt_rules(self):
        self.client.execute_command('TS.CREATE', 'src')
        self.client.execute_command('TS.CREATE', 'dst')
        self.create_rule('src', 'dst')
        self.add('src', (0, 1))

        self.client.restore('src:copy', 0, self.client.dump('src'))

        # The copy is stored under its own key, which `dst` does not name as its source.
        assert self.rule_dests('src:copy') == []
        self.add('src:copy', (1000, 100), (2000, 100))
        self.add('src', (500, 2), (1000, 5))
        assert self.dest_samples('dst') == [(0, 3.0)]
        assert self.source_key('dst') == 'src'

    def test_createrule_replaces_a_stale_source_link(self):
        """`dst` still carries a link to its deleted source; that must not block a new rule."""
        for key in ('src', 'dst', 'other'):
            self.client.execute_command('TS.CREATE', key)
        self.create_rule('src', 'dst')
        self.client.delete('src')

        self.create_rule('other', 'dst', aggregation='max')

        assert self.source_key('dst') == 'other'
        self.add('other', (0, 5), (1000, 7))
        assert self.dest_samples('dst') == [(0, 5.0)]

    def test_restored_copy_of_a_destination_accepts_a_rule(self):
        for key in ('src', 'dst', 'other'):
            self.client.execute_command('TS.CREATE', key)
        self.create_rule('src', 'dst')
        self.client.restore('dst:copy', 0, self.client.dump('dst'))

        # The copy's link names `src`, which feeds `dst`, not the copy.
        self.create_rule('other', 'dst:copy', aggregation='max')

        assert self.source_key('dst:copy') == 'other'
        assert self.source_key('dst') == 'src'
        self.add('src', (0, 1), (1000, 2))
        self.add('other', (0, 10), (1000, 20))
        assert self.dest_samples('dst') == [(0, 1.0)]
        assert self.dest_samples('dst:copy') == [(0, 10.0)]

    def test_createrule_rejects_a_destination_with_a_live_source(self):
        for key in ('src', 'dst', 'other'):
            self.client.execute_command('TS.CREATE', key)
        self.create_rule('src', 'dst')
        try:
            self.create_rule('other', 'dst')
            assert False, 'expected the second source to be refused'
        except ResponseError as e:
            assert 'already has a src rule' in str(e)
        assert self.source_key('dst') == 'src'

    def test_stale_rule_does_not_close_a_cycle(self):
        """`a`'s rule for a deleted-and-recreated `b` is dead, so `b -> a` is not a cycle."""
        self.client.execute_command('TS.CREATE', 'a')
        self.client.execute_command('TS.CREATE', 'b')
        self.create_rule('a', 'b')

        self.client.delete('b')
        self.client.execute_command('TS.CREATE', 'b')
        # No write to `a` in between: its stale rule has not been pruned yet.
        self.create_rule('b', 'a')

        assert self.source_key('a') == 'b'
        assert self.rule_dests('b') == ['a']
        self.add('b', (0, 1), (500, 2), (1000, 5))
        assert self.dest_samples('a') == [(0, 3.0)]
        # A direct write to `a` must not feed its dead rule's old destination.
        self.add('a', (2000, 7), (3000, 9))
        assert self.dest_samples('b') == [(0, 1.0), (500, 2.0), (1000, 5.0)]

    def test_stale_rule_mid_chain_does_not_close_a_cycle(self):
        for key in ('a', 'b', 'c'):
            self.client.execute_command('TS.CREATE', key)
        self.create_rule('a', 'b')
        self.create_rule('b', 'c')

        self.client.delete('b')
        self.client.execute_command('TS.CREATE', 'b')
        self.create_rule('b', 'c')

        # The new `b` feeds `c`, but `a -> b` is dead, so `a` no longer reaches `c`.
        self.create_rule('c', 'a')
        assert self.source_key('a') == 'c'
        assert self.source_key('c') == 'b'

    def test_restored_destination_still_closes_a_cycle(self):
        """A destination restored under its own key names its source again, reviving the rule."""
        self.client.execute_command('TS.CREATE', 'a')
        self.client.execute_command('TS.CREATE', 'b')
        self.create_rule('a', 'b')
        dump = self.client.dump('b')

        self.client.delete('b')
        self.client.restore('b', 0, dump)

        try:
            self.create_rule('b', 'a')
            assert False, 'expected the cycle to be refused'
        except ResponseError as e:
            assert 'circular dependency' in str(e)
        assert self.source_key('a') is None

    def test_createrule_replaces_a_stale_rule_for_the_same_destination(self):
        self.client.execute_command('TS.CREATE', 'src')
        self.client.execute_command('TS.CREATE', 'dst')
        self.create_rule('src', 'dst', aggregation='sum')

        self.client.delete('dst')
        self.client.execute_command('TS.CREATE', 'dst')
        # No write to `src` in between: its stale `sum` rule for `dst` is still stored.
        self.create_rule('src', 'dst', aggregation='max')

        assert self.rule_dests('src') == ['dst']
        assert self.source_key('dst') == 'src'
        # The new `max` rule feeds `dst`, not the stale `sum` one.
        self.add('src', (0, 1), (500, 5), (1000, 0))
        assert self.dest_samples('dst') == [(0, 5.0)]

    def test_createrule_still_rejects_a_live_duplicate(self):
        self.client.execute_command('TS.CREATE', 'src')
        self.client.execute_command('TS.CREATE', 'dst')
        self.create_rule('src', 'dst')
        try:
            self.create_rule('src', 'dst', aggregation='max')
            assert False, 'expected the duplicate rule to be refused'
        except ResponseError as e:
            assert 'already has a src rule' in str(e)
        assert self.rule_dests('src') == ['dst']
