"""TS.DEL parity, including the retention-model divergence DIV-0021.

Most TS.DEL behavior is plain `diff` coverage. The one exception is deleting a range
that covers only already-expired samples: that is pinned per-engine rather than routed
through `diff`, because the only registry regex able to match it ("reference=<n>
subject=0" on TS.DEL) would also absorb a genuine "TS.DEL deletes nothing" regression
(plan §5.3, and see tests/compat/README.md "Divergences the registry can not express").

Written clean-room from public RedisTimeSeries documentation and black-box observation
of the reference server. Do NOT consult RedisTimeSeries source or test code.
"""

from __future__ import annotations

import pytest
from valkey.exceptions import ResponseError

from compat_helpers import mk_populated, mk_series


class TestDel:
    def test_del_range(self, diff):
        mk_populated(diff, "del:a", [(100, 1.0), (200, 2.0), (300, 3.0)])
        diff("TS.DEL", "del:a", 100, 200)
        diff("TS.RANGE", "del:a", "-", "+")

    def test_del_no_match_returns_zero(self, diff):
        mk_populated(diff, "del:b", [(100, 1.0)])
        diff("TS.DEL", "del:b", 500, 600)

    def test_del_whole_series(self, diff):
        mk_populated(diff, "del:c", [(100, 1.0), (200, 2.0)])
        diff("TS.DEL", "del:c", 0, 1000)
        diff("TS.RANGE", "del:c", "-", "+")
        diff("TS.INFO", "del:c")

    def test_del_empty_series(self, diff):
        mk_series(diff, "del:d")
        diff("TS.DEL", "del:d", 0, 100)

    def test_del_missing_key(self, diff):
        with pytest.raises(ResponseError):
            diff("TS.DEL", "del:nonexistent", 0, 100)

    def test_del_wrongtype(self, diff):
        diff("SET", "del:string", "hello")
        with pytest.raises(ResponseError):
            diff("TS.DEL", "del:string", 0, 100)


class TestDelRetention:
    """DIV-0021: retention model (lazy physical trim vs eager) leaks through TS.DEL."""

    @staticmethod
    def _expired(client):
        """retention=1000 with samples at ts=0 and ts=1001 -> ts=0 is out of window."""
        client.execute_command("TS.CREATE", "del:ret", "RETENTION", 1000)
        client.execute_command("TS.ADD", "del:ret", 0, 0)
        client.execute_command("TS.ADD", "del:ret", 1001, 0)

    def test_reads_agree_on_both_engines(self, diff):
        """The divergence is confined to TS.DEL: every read path must still match."""
        diff("TS.CREATE", "del:r2", "RETENTION", 1000)
        diff("TS.ADD", "del:r2", 0, 0)
        diff("TS.ADD", "del:r2", 1001, 0)
        diff("TS.RANGE", "del:r2", "-", "+")      # ts=0 hidden on both
        diff("TS.RANGE", "del:r2", 0, 0)          # explicit expired ts -> empty on both
        diff("TS.GET", "del:r2")
        diff("TS.INFO", "del:r2")                 # totalSamples/firstTimestamp agree

    def test_del_of_expired_range_counts_differently(self, diff):
        """RTS still physically holds the out-of-window sample and reports deleting it;
        we trimmed it on write, so there is nothing left to delete. Pins DIV-0021."""
        self._expired(diff.reference)
        self._expired(diff.subject)

        assert diff.reference.execute_command("TS.DEL", "del:ret", 0, 0) == 1
        assert diff.subject.execute_command("TS.DEL", "del:ret", 0, 0) == 0

        # Converged afterwards: RTS's second delete finds nothing either, and the
        # retained window is identical on both engines.
        assert diff.reference.execute_command("TS.DEL", "del:ret", 0, 0) == 0
        assert diff.subject.execute_command("TS.DEL", "del:ret", 0, 0) == 0
        assert (
            diff.reference.execute_command("TS.RANGE", "del:ret", "-", "+")
            == diff.subject.execute_command("TS.RANGE", "del:ret", "-", "+")
        )

    def test_del_of_live_range_under_retention_agrees(self, diff):
        """A range covering *live* samples must still match exactly — this is the
        delta class DIV-0021 must not be allowed to mask."""
        diff("TS.CREATE", "del:r3", "RETENTION", 5000)
        for ts in (1000, 2000, 3000, 6000):
            diff("TS.ADD", "del:r3", ts, 1.0)
        diff("TS.DEL", "del:r3", 2000, 3000)
        diff("TS.RANGE", "del:r3", "-", "+")
