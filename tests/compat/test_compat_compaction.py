"""Tier A compaction deep-dive (test plan §6, "Compaction deep-dive scenarios").

The plan calls compaction the highest historical-divergence area and gives it its
own module. Scenarios here are the ones it enumerates:

  - bucket finalization timing (when does a sample land downstream)
  - out-of-order writes into an already-finalized bucket
  - TS.DEL on source ranges covered by a finalized bucket
  - retention expiring source data under an active rule
  - twa edge cases (single sample, bucket edges) — DIV-0012, see below
  - restart persistence of partial-bucket state (DEBUG RELOAD on both engines
    mid-bucket, compare downstream after the next write)

`twa` is unimplemented (DIV-0012), so its edge cases are not covered here; the
gap is pinned once in test_compat_range.py rather than repeated per scenario.

Every command issued through the `diff` fixture is sent to both engines and its
reply diffed automatically. Downstream state is probed with TS.RANGE/TS.INFO
after each mutation — the plan's L2 (semantics) layer, which is what actually
catches compaction divergence: a wrong bucket is a wrong TS.RANGE on the
destination, not a wrong TS.ADD reply.

Written clean-room from public RedisTimeSeries documentation and black-box
observation of the reference server. Do NOT consult RedisTimeSeries source or
test code (see tests/compat/README.md).
"""

import pytest
from valkey.exceptions import ResponseError

from compat_helpers import AGGREGATORS, mk_series

BUCKET = 1000


def _rule(diff, src, dst, aggregator="sum", bucket=BUCKET, *extra):
    """Source + destination + rule, with all engine defaults pinned."""
    mk_series(diff, src)
    mk_series(diff, dst)
    diff("TS.CREATERULE", src, dst, "AGGREGATION", aggregator, bucket, *extra)


def _probe(diff, src, dst):
    """The state that must agree after any compaction-affecting mutation."""
    diff("TS.RANGE", src, "-", "+")
    diff("TS.RANGE", dst, "-", "+")
    diff("TS.INFO", src)
    diff("TS.INFO", dst)


class TestBucketFinalization:
    """When does a source sample become a downstream sample?"""

    def test_bucket_lands_downstream_only_once_closed(self, diff):
        _rule(diff, "c:fin:src", "c:fin:dst")
        # Bucket [0,1000) stays open: nothing downstream yet.
        diff("TS.ADD", "c:fin:src", 0, 1.0)
        diff("TS.RANGE", "c:fin:dst", "-", "+")
        diff("TS.ADD", "c:fin:src", 500, 2.0)
        diff("TS.RANGE", "c:fin:dst", "-", "+")
        # A write past the boundary closes it.
        diff("TS.ADD", "c:fin:src", 1000, 4.0)
        _probe(diff, "c:fin:src", "c:fin:dst")
        # ... and the new bucket is now the open one.
        diff("TS.ADD", "c:fin:src", 1500, 8.0)
        diff("TS.RANGE", "c:fin:dst", "-", "+")
        diff("TS.ADD", "c:fin:src", 2000, 16.0)
        _probe(diff, "c:fin:src", "c:fin:dst")

    def test_sample_exactly_on_boundary_closes_previous_bucket(self, diff):
        _rule(diff, "c:bnd:src", "c:bnd:dst")
        diff("TS.ADD", "c:bnd:src", 999, 1.0)
        diff("TS.RANGE", "c:bnd:dst", "-", "+")
        diff("TS.ADD", "c:bnd:src", 1000, 2.0)  # boundary: closes [0,1000)
        _probe(diff, "c:bnd:src", "c:bnd:dst")

    def test_skipping_buckets_does_not_emit_empty_ones(self, diff):
        """A gap in the source must not fabricate downstream buckets."""
        _rule(diff, "c:skip:src", "c:skip:dst")
        diff("TS.ADD", "c:skip:src", 0, 1.0)
        diff("TS.ADD", "c:skip:src", 5000, 2.0)  # jumps over [1000,5000)
        _probe(diff, "c:skip:src", "c:skip:dst")

    def test_latest_exposes_the_open_bucket(self, diff):
        _rule(diff, "c:open:src", "c:open:dst")
        diff("TS.ADD", "c:open:src", 0, 1.0)
        diff("TS.ADD", "c:open:src", 500, 2.0)
        diff("TS.RANGE", "c:open:dst", "-", "+")
        diff("TS.RANGE", "c:open:dst", "-", "+", "LATEST")
        diff("TS.GET", "c:open:dst")
        diff("TS.GET", "c:open:dst", "LATEST")

    def test_latest_open_bucket_under_a_bounded_range(self, diff):
        """LATEST must hide the open bucket unless the query reaches past the last
        *stored* sample — and single-key and multi-key must agree on that.

        With no bucket closed yet the destination has no stored samples, so a query
        ending at the open bucket's own start reaches nothing: both engines report
        empty. TS.MRANGE used to re-derive this rule locally and checked only that the
        bucket's timestamp fell in range, so it reported a bucket TS.RANGE omitted —
        the engine contradicting itself. Found by the Tier C fuzzer.
        """
        _rule(diff, "c:lob:src", "c:lob:dst", "avg", 500)
        diff("TS.ALTER", "c:lob:dst", "LABELS", "grp", "lob")
        diff("TS.ADD", "c:lob:src", 0, 0)  # opens [0,500); nothing closed

        # Bounded ranges, single-key and multi-key, must agree with each other.
        for end in (0, 1, 250, 499, 500, 1000):
            diff("TS.RANGE", "c:lob:dst", 0, end, "LATEST")
            diff("TS.REVRANGE", "c:lob:dst", 0, end, "LATEST")
            diff("TS.MRANGE", 0, end, "LATEST", "FILTER", "grp=lob")
            diff("TS.MREVRANGE", 0, end, "LATEST", "FILTER", "grp=lob")
        diff("TS.RANGE", "c:lob:dst", "-", "+", "LATEST")
        diff("TS.MRANGE", "-", "+", "LATEST", "FILTER", "grp=lob")

        # Once a bucket closes, the stored sample is visible and LATEST adds the new
        # open one on top — the same on both paths.
        diff("TS.ADD", "c:lob:src", 500, 0)
        for end in (0, 499, 500, 999, 1000):
            diff("TS.RANGE", "c:lob:dst", 0, end, "LATEST")
            diff("TS.MRANGE", 0, end, "LATEST", "FILTER", "grp=lob")

    @pytest.mark.parametrize("agg", AGGREGATORS)
    def test_each_aggregator_finalizes_the_same_bucket(self, diff, agg):
        _rule(diff, f"c:agg:src:{agg}", f"c:agg:dst:{agg}", agg)
        for ts, value in [(0, 1.0), (250, 2.0), (750, 4.0), (1000, 8.0), (2000, 16.0)]:
            diff("TS.ADD", f"c:agg:src:{agg}", ts, value)
        _probe(diff, f"c:agg:src:{agg}", f"c:agg:dst:{agg}")


class TestOutOfOrderWrites:
    def test_write_into_finalized_bucket(self, diff):
        """The headline case: does a late sample rewrite a closed bucket?"""
        _rule(diff, "c:ooo:src", "c:ooo:dst")
        diff("TS.ADD", "c:ooo:src", 0, 1.0)
        diff("TS.ADD", "c:ooo:src", 1000, 2.0)   # closes [0,1000) as sum=1
        diff("TS.RANGE", "c:ooo:dst", "-", "+")
        diff("TS.ADD", "c:ooo:src", 500, 99.0)   # late arrival into [0,1000)
        _probe(diff, "c:ooo:src", "c:ooo:dst")

    def test_write_into_bucket_two_back(self, diff):
        _rule(diff, "c:ooo2:src", "c:ooo2:dst")
        for ts, value in [(0, 1.0), (1000, 2.0), (2000, 4.0)]:
            diff("TS.ADD", "c:ooo2:src", ts, value)
        diff("TS.RANGE", "c:ooo2:dst", "-", "+")
        diff("TS.ADD", "c:ooo2:src", 100, 99.0)
        _probe(diff, "c:ooo2:src", "c:ooo2:dst")

    def test_out_of_order_within_the_open_bucket(self, diff):
        _rule(diff, "c:ooo3:src", "c:ooo3:dst")
        diff("TS.ADD", "c:ooo3:src", 750, 1.0)
        diff("TS.ADD", "c:ooo3:src", 250, 2.0)  # still open, still in order-free
        diff("TS.ADD", "c:ooo3:src", 1000, 4.0)  # closes it
        _probe(diff, "c:ooo3:src", "c:ooo3:dst")

    def test_duplicate_timestamp_in_finalized_bucket(self, diff):
        """Regression guard: recalculating a bucket must include ts=0 samples.

        The recalculation filter used to drop timestamp 0, so overwriting a lone
        ts=0 sample (DUPLICATE_POLICY LAST) deleted the downstream bucket instead
        of updating it (subject=[] vs reference=[[0, 50.0]]).
        """
        _rule(diff, "c:dup:src", "c:dup:dst")
        diff("TS.ALTER", "c:dup:src", "DUPLICATE_POLICY", "LAST")
        diff("TS.ADD", "c:dup:src", 0, 1.0)
        diff("TS.ADD", "c:dup:src", 1000, 2.0)
        diff("TS.RANGE", "c:dup:dst", "-", "+")
        diff("TS.ADD", "c:dup:src", 0, 50.0)  # overwrite inside a closed bucket
        _probe(diff, "c:dup:src", "c:dup:dst")


class TestDeleteInteraction:
    def test_del_range_covered_by_finalized_bucket(self, diff):
        _rule(diff, "c:del:src", "c:del:dst")
        for ts, value in [(0, 1.0), (500, 2.0), (1000, 4.0), (2000, 8.0)]:
            diff("TS.ADD", "c:del:src", ts, value)
        diff("TS.RANGE", "c:del:dst", "-", "+")
        diff("TS.DEL", "c:del:src", 0, 999)  # exactly one finalized bucket
        _probe(diff, "c:del:src", "c:del:dst")

    def test_del_partial_bucket(self, diff):
        _rule(diff, "c:delp:src", "c:delp:dst")
        for ts, value in [(0, 1.0), (500, 2.0), (1000, 4.0), (2000, 8.0)]:
            diff("TS.ADD", "c:delp:src", ts, value)
        diff("TS.DEL", "c:delp:src", 0, 250)  # removes one of two samples
        _probe(diff, "c:delp:src", "c:delp:dst")

    def test_del_across_several_buckets(self, diff):
        _rule(diff, "c:dela:src", "c:dela:dst")
        for ts in range(0, 5000, 250):
            diff("TS.ADD", "c:dela:src", ts, ts / 1000.0)
        diff("TS.RANGE", "c:dela:dst", "-", "+")
        diff("TS.DEL", "c:dela:src", 500, 3500)
        _probe(diff, "c:dela:src", "c:dela:dst")

    def test_del_on_the_open_bucket(self, diff):
        _rule(diff, "c:delo:src", "c:delo:dst")
        diff("TS.ADD", "c:delo:src", 0, 1.0)
        diff("TS.ADD", "c:delo:src", 1000, 2.0)
        diff("TS.ADD", "c:delo:src", 1500, 4.0)  # open bucket [1000,2000)
        diff("TS.DEL", "c:delo:src", 1500, 1500)
        diff("TS.ADD", "c:delo:src", 2000, 8.0)  # closes it
        _probe(diff, "c:delo:src", "c:delo:dst")

    def test_del_directly_on_the_destination(self, diff):
        _rule(diff, "c:deld:src", "c:deld:dst")
        for ts, value in [(0, 1.0), (1000, 2.0), (2000, 4.0)]:
            diff("TS.ADD", "c:deld:src", ts, value)
        diff("TS.RANGE", "c:deld:dst", "-", "+")
        diff("TS.DEL", "c:deld:dst", 0, 0)
        _probe(diff, "c:deld:src", "c:deld:dst")


class TestRetentionInteraction:
    """Retention applied eagerly on add, matching RTS.

    TS.INFO's `totalSamples`/`firstTimestamp` reflect the retained window (not
    the physically-buffered samples) because a write now trims the series
    synchronously, as RedisTimeSeries does — see TimeSeries::add / trim.
    """

    def test_retention_trims_source_under_an_active_rule(self, diff):
        mk_series(diff, "c:ret:src")
        mk_series(diff, "c:ret:dst")
        diff("TS.ALTER", "c:ret:src", "RETENTION", 2000)
        diff("TS.CREATERULE", "c:ret:src", "c:ret:dst", "AGGREGATION", "sum", BUCKET)
        # Each write advances the retention window; downstream must keep the
        # buckets it already finalized even as their source samples age out.
        for ts, value in [(0, 1.0), (1000, 2.0), (2000, 4.0), (3000, 8.0), (5000, 16.0)]:
            diff("TS.ADD", "c:ret:src", ts, value)
        _probe(diff, "c:ret:src", "c:ret:dst")

    def test_retention_on_the_destination(self, diff):
        mk_series(diff, "c:retd:src")
        mk_series(diff, "c:retd:dst")
        diff("TS.ALTER", "c:retd:dst", "RETENTION", 2000)
        diff("TS.CREATERULE", "c:retd:src", "c:retd:dst", "AGGREGATION", "sum", BUCKET)
        for ts in range(0, 6000, 500):
            diff("TS.ADD", "c:retd:src", ts, 1.0)
        _probe(diff, "c:retd:src", "c:retd:dst")


class TestRuleLifecycle:
    def test_createrule_on_a_source_that_already_has_data(self, diff):
        """Pre-existing samples are not back-filled downstream."""
        mk_series(diff, "c:pre:src")
        mk_series(diff, "c:pre:dst")
        for ts, value in [(0, 1.0), (500, 2.0), (1000, 4.0)]:
            diff("TS.ADD", "c:pre:src", ts, value)
        diff("TS.CREATERULE", "c:pre:src", "c:pre:dst", "AGGREGATION", "sum", BUCKET)
        diff("TS.RANGE", "c:pre:dst", "-", "+")
        diff("TS.ADD", "c:pre:src", 2000, 8.0)
        _probe(diff, "c:pre:src", "c:pre:dst")

    def test_deleterule_stops_compaction_and_keeps_data(self, diff):
        _rule(diff, "c:drule:src", "c:drule:dst")
        for ts, value in [(0, 1.0), (1000, 2.0), (2000, 4.0)]:
            diff("TS.ADD", "c:drule:src", ts, value)
        diff("TS.RANGE", "c:drule:dst", "-", "+")
        diff("TS.DELETERULE", "c:drule:src", "c:drule:dst")
        diff("TS.INFO", "c:drule:src")
        diff("TS.ADD", "c:drule:src", 3000, 8.0)
        diff("TS.ADD", "c:drule:src", 4000, 16.0)
        _probe(diff, "c:drule:src", "c:drule:dst")

    def test_deleting_the_source_key(self, diff):
        _rule(diff, "c:dsrc:src", "c:dsrc:dst")
        for ts, value in [(0, 1.0), (1000, 2.0)]:
            diff("TS.ADD", "c:dsrc:src", ts, value)
        diff("DEL", "c:dsrc:src")
        diff("TS.RANGE", "c:dsrc:dst", "-", "+")
        diff("TS.INFO", "c:dsrc:dst")

    def test_deleting_the_destination_key(self, diff):
        _rule(diff, "c:ddst:src", "c:ddst:dst")
        diff("TS.ADD", "c:ddst:src", 0, 1.0)
        diff("DEL", "c:ddst:dst")
        diff("TS.INFO", "c:ddst:src")
        # Writes must survive a destination that vanished underneath the rule.
        diff("TS.ADD", "c:ddst:src", 1000, 2.0)
        diff("TS.ADD", "c:ddst:src", 2000, 4.0)
        diff("TS.RANGE", "c:ddst:src", "-", "+")
        diff("TS.INFO", "c:ddst:src")

    def test_multiple_rules_from_one_source(self, diff):
        mk_series(diff, "c:multi:src")
        mk_series(diff, "c:multi:sum")
        mk_series(diff, "c:multi:max")
        diff("TS.CREATERULE", "c:multi:src", "c:multi:sum", "AGGREGATION", "sum", BUCKET)
        diff("TS.CREATERULE", "c:multi:src", "c:multi:max", "AGGREGATION", "max", 2000)
        for ts in range(0, 5000, 250):
            diff("TS.ADD", "c:multi:src", ts, ts / 1000.0)
        diff("TS.RANGE", "c:multi:sum", "-", "+")
        diff("TS.RANGE", "c:multi:max", "-", "+")
        diff("TS.INFO", "c:multi:src")

    def test_self_rule_rejected(self, diff):
        mk_series(diff, "c:self")
        with pytest.raises(ResponseError):
            diff("TS.CREATERULE", "c:self", "c:self", "AGGREGATION", "avg", BUCKET)

    def test_rule_on_rule_is_an_accepted_input_superset(self, diff):
        """DIV-0017: we allow chaining a rule off a compaction destination.

        RTS refuses ("the source key already has a source rule"), so a
        destination is a leaf there. We accept it, which means a downstream
        series can itself compact into a third — an accepted-input superset,
        non-registrable per plan §5.2, so it is asserted per-engine here.
        """
        _rule(diff, "c:chain:a", "c:chain:b")
        mk_series(diff, "c:chain:c")
        with pytest.raises(ResponseError):
            diff.reference.execute_command(
                "TS.CREATERULE", "c:chain:b", "c:chain:c", "AGGREGATION", "avg", BUCKET
            )
        diff.subject.execute_command(
            "TS.CREATERULE", "c:chain:b", "c:chain:c", "AGGREGATION", "avg", BUCKET
        )

    def test_missing_source_or_destination(self, diff):
        mk_series(diff, "c:exists")
        with pytest.raises(ResponseError):
            diff("TS.CREATERULE", "c:nosuch", "c:exists", "AGGREGATION", "avg", BUCKET)
        with pytest.raises(ResponseError):
            diff("TS.CREATERULE", "c:exists", "c:nosuch", "AGGREGATION", "avg", BUCKET)

    def test_deleterule_nonexistent(self, diff):
        mk_series(diff, "c:norule:src")
        mk_series(diff, "c:norule:dst")
        with pytest.raises(ResponseError):
            diff("TS.DELETERULE", "c:norule:src", "c:norule:dst")


class TestAlignTimestamp:
    @pytest.mark.parametrize("align", [0, 100, 500])
    def test_align_timestamp_shifts_buckets(self, diff, align):
        _rule(diff, f"c:al:src:{align}", f"c:al:dst:{align}", "sum", BUCKET, align)
        for ts in range(0, 4000, 250):
            diff("TS.ADD", f"c:al:src:{align}", ts, 1.0)
        _probe(diff, f"c:al:src:{align}", f"c:al:dst:{align}")


class TestIncrByCompaction:
    def test_incrby_feeds_the_rule(self, diff):
        _rule(diff, "c:incr:src", "c:incr:dst")
        for ts in (100, 500, 1000, 1500, 2000):
            diff("TS.INCRBY", "c:incr:src", 2, "TIMESTAMP", ts)
        _probe(diff, "c:incr:src", "c:incr:dst")


class TestReloadPartialBucket:
    """Plan §6: DEBUG RELOAD mid-bucket on both engines, compare downstream
    after the next write.

    Each engine must round-trip *itself* (our RDB is not readable by RTS and
    vice versa — DIV-0010); the diff is on the post-reload replies.
    """

    def _reload(self, diff):
        diff.reference.execute_command("DEBUG", "RELOAD")
        diff.subject.execute_command("DEBUG", "RELOAD")

    def test_reload_mid_bucket_then_close_it(self, diff):
        _rule(diff, "c:rel:src", "c:rel:dst")
        diff("TS.ADD", "c:rel:src", 0, 1.0)
        diff("TS.ADD", "c:rel:src", 500, 2.0)  # bucket [0,1000) open, sum so far 3
        self._reload(diff)
        # Partial-bucket state must survive the reload: closing the bucket after
        # restart must still produce sum=3, not sum=0 or a lost bucket.
        diff("TS.ADD", "c:rel:src", 1000, 4.0)
        _probe(diff, "c:rel:src", "c:rel:dst")

    def test_reload_preserves_rule_metadata(self, diff):
        _rule(diff, "c:relm:src", "c:relm:dst", "avg", 2000, 500)
        diff("TS.ADD", "c:relm:src", 0, 1.0)
        self._reload(diff)
        diff("TS.INFO", "c:relm:src")
        diff("TS.INFO", "c:relm:dst")

    def test_reload_between_every_bucket(self, diff):
        _rule(diff, "c:relb:src", "c:relb:dst", "max")
        for ts, value in [(0, 1.0), (500, 5.0), (1000, 2.0), (1500, 9.0), (2000, 3.0)]:
            diff("TS.ADD", "c:relb:src", ts, value)
            self._reload(diff)
        _probe(diff, "c:relb:src", "c:relb:dst")

    def test_reload_after_out_of_order_write(self, diff):
        _rule(diff, "c:relo:src", "c:relo:dst")
        diff("TS.ADD", "c:relo:src", 0, 1.0)
        diff("TS.ADD", "c:relo:src", 1000, 2.0)
        diff("TS.ADD", "c:relo:src", 500, 99.0)
        self._reload(diff)
        diff("TS.ADD", "c:relo:src", 2000, 4.0)
        _probe(diff, "c:relo:src", "c:relo:dst")


@pytest.fixture
def strict_subject(diff):
    """Run one test with the subject in `ts-compatibility-mode strict`.

    Yields the same `diff` client. The subject server is session-scoped, so the
    previous value is always restored — a leak here would silently change what
    every later test expects. Module configs are namespaced by the module name
    (DIV-0008), hence the `ts.` prefix.
    """
    param = "ts.ts-compatibility-mode"
    current = diff.subject.execute_command("CONFIG", "GET", param)
    # valkey-py parses CONFIG GET into a dict; older/raw paths give a flat list.
    previous = (
        next(iter(current.values())) if isinstance(current, dict) else current[1]
    )
    diff.subject.execute_command("CONFIG", "SET", param, "strict")
    try:
        yield diff
    finally:
        diff.subject.execute_command("CONFIG", "SET", param, previous)


class TestOutOfOrderBackfill:
    """DIV-0023: TS.GET on a destination whose *older* bucket was back-filled.

    Pinned per-engine rather than via `diff`: the only registry regex able to match it
    would cover "reference=<ts> subject=<ts>" on TS.GET/TS.MGET and absorb any real
    last-sample bug (plan §5.3).
    """

    @staticmethod
    def _backfill(client):
        client.execute_command("TS.CREATE", "c:bsrc", "LABELS", "grp", "bf")
        client.execute_command("TS.CREATE", "c:bdst", "LABELS", "grp", "bf")
        client.execute_command(
            "TS.CREATERULE", "c:bsrc", "c:bdst", "AGGREGATION", "avg", 500
        )
        client.execute_command("TS.ADD", "c:bsrc", 0, 0)      # opens bucket [0,500)
        client.execute_command("TS.ADD", "c:bsrc", 1000, 0)   # closes it, opens [1000,1500)
        client.execute_command("TS.ADD", "c:bsrc", 500, 0)    # back-fills [500,1000)

    def test_backfilled_bucket_is_stored_identically(self, diff):
        """The stored downstream data agrees — the divergence is only in TS.GET."""
        diff("TS.CREATE", "c:b2src")
        diff("TS.CREATE", "c:b2dst")
        diff("TS.CREATERULE", "c:b2src", "c:b2dst", "AGGREGATION", "avg", 500)
        diff("TS.ADD", "c:b2src", 0, 0)
        diff("TS.ADD", "c:b2src", 1000, 0)
        diff("TS.ADD", "c:b2src", 500, 0)
        diff("TS.RANGE", "c:b2dst", "-", "+")
        diff("TS.GET", "c:b2dst", "LATEST")

    def test_get_after_backfilling_an_older_bucket(self, diff):
        """RTS keeps reporting the stale bucket; we report the last sample actually
        stored (which its own TS.RANGE agrees on). Pins DIV-0023."""
        self._backfill(diff.reference)
        self._backfill(diff.subject)

        # Identical stored data on both engines.
        assert (
            diff.reference.execute_command("TS.RANGE", "c:bdst", "-", "+")
            == diff.subject.execute_command("TS.RANGE", "c:bdst", "-", "+")
        )

        ref_get = diff.reference.execute_command("TS.GET", "c:bdst")
        sub_get = diff.subject.execute_command("TS.GET", "c:bdst")
        assert ref_get[0] == 0, f"expected RTS to report the stale bucket, got {ref_get!r}"
        assert sub_get[0] == 500, f"expected the last stored bucket, got {sub_get!r}"

        # LATEST (the open bucket) agrees on both.
        assert (
            diff.reference.execute_command("TS.GET", "c:bdst", "LATEST")
            == diff.subject.execute_command("TS.GET", "c:bdst", "LATEST")
        )

    def test_strict_mode_reports_the_stale_bucket_like_rts(self, strict_subject):
        """DIV-0023 gated: in `ts-compatibility-mode strict` TS.GET/TS.MGET report
        RTS's cached destination last-sample instead of the last stored bucket.

        Per-engine assertions for the same reason as the sibling test above.
        """
        diff = strict_subject
        self._backfill(diff.reference)
        self._backfill(diff.subject)

        ref_get = diff.reference.execute_command("TS.GET", "c:bdst")
        sub_get = diff.subject.execute_command("TS.GET", "c:bdst")
        assert sub_get == ref_get, f"strict should match RTS, got {sub_get!r} vs {ref_get!r}"
        assert sub_get[0] == 0, f"expected the stale bucket under strict, got {sub_get!r}"

        # TS.MGET rides on the same accessor. Cross-series order is undefined
        # (COMPATIBILITY.md), so normalize it — `diff` would, this raw compare must too.
        def by_key(reply):
            return reply if isinstance(reply, dict) else sorted(reply, key=lambda e: e[0])

        assert by_key(diff.reference.execute_command("TS.MGET", "FILTER", "grp=bf")) == by_key(
            diff.subject.execute_command("TS.MGET", "FILTER", "grp=bf")
        )

        # The gate touches neither LATEST nor the stored data.
        assert (
            diff.reference.execute_command("TS.GET", "c:bdst", "LATEST")
            == diff.subject.execute_command("TS.GET", "c:bdst", "LATEST")
        )
        assert (
            diff.reference.execute_command("TS.RANGE", "c:bdst", "-", "+")
            == diff.subject.execute_command("TS.RANGE", "c:bdst", "-", "+")
        )

    @staticmethod
    def _backfill_one_madd(client):
        """The same three writes as `_backfill`, but as ONE out-of-order TS.MADD."""
        client.execute_command("TS.CREATE", "c:msrc", "LABELS", "grp", "mbf")
        client.execute_command("TS.CREATE", "c:mdst", "LABELS", "grp", "mbf")
        client.execute_command(
            "TS.CREATERULE", "c:msrc", "c:mdst", "AGGREGATION", "avg", 500
        )
        # Argument order matters: 1000 closes [0,500) by forward progress, then 500
        # back-fills the skipped [500,1000).
        client.execute_command(
            "TS.MADD", "c:msrc", 0, 0, "c:msrc", 1000, 0, "c:msrc", 500, 0
        )

    def test_strict_mode_honors_madd_argument_order(self, strict_subject):
        """A single TS.MADD carrying both the closing sample and the back-fill must
        land on the same cached last-sample as the equivalent TS.ADD sequence.

        We merge a MADD as one sorted run, which is right for the stored data but
        erases which closes were forward — sorted, this looks like two forward closes
        and the marker would advance to 500 where RTS reports 0. Regression test for
        the input-order fix in `last_forward_close_in_input_order`; found by the
        Tier C fuzzer. Per-engine assertions for the same reason as the siblings above.
        """
        diff = strict_subject
        self._backfill_one_madd(diff.reference)
        self._backfill_one_madd(diff.subject)

        ref_get = diff.reference.execute_command("TS.GET", "c:mdst")
        sub_get = diff.subject.execute_command("TS.GET", "c:mdst")
        assert sub_get == ref_get, f"strict should match RTS, got {sub_get!r} vs {ref_get!r}"
        assert sub_get[0] == 0, f"expected the forward-closed bucket, got {sub_get!r}"

        # TS.INFO reads the same accessor and must not disagree with TS.GET.
        def last_ts(client):
            reply = client.execute_command("TS.INFO", "c:mdst")
            items = reply.items() if isinstance(reply, dict) else zip(reply[::2], reply[1::2])
            fields = {
                (k.decode() if isinstance(k, bytes) else k): v for k, v in items
            }
            return fields["lastTimestamp"]

        assert last_ts(diff.subject) == last_ts(diff.reference)

        # Stored data is unaffected by the marker.
        assert (
            diff.reference.execute_command("TS.RANGE", "c:mdst", "-", "+")
            == diff.subject.execute_command("TS.RANGE", "c:mdst", "-", "+")
        )

    @staticmethod
    def _delete_then_recreate(client):
        """Forward-close a bucket, delete it, then write past it and re-create it."""
        client.execute_command("TS.CREATE", "c:rsrc", "LABELS", "grp", "res")
        client.execute_command("TS.CREATE", "c:rdst", "LABELS", "grp", "res")
        client.execute_command(
            "TS.CREATERULE", "c:rsrc", "c:rdst", "AGGREGATION", "avg", 500
        )
        client.execute_command("TS.ADD", "c:rsrc", 0, 0)
        client.execute_command("TS.MADD", "c:rsrc", 1000, 0)  # closes [0,500) -> dst(0)
        client.execute_command("TS.DEL", "c:rsrc", 0, 0)      # dst(0) recalculated away
        client.execute_command("TS.ADD", "c:rsrc", 500, 0)    # back-fills dst(500)
        client.execute_command("TS.ADD", "c:rsrc", 0, 0)      # re-creates dst(0)

    def test_strict_mode_does_not_resurrect_a_deleted_marker(self, strict_subject):
        """A cached last-sample whose bucket was deleted must stay dead.

        Leaving it set lets a later write that re-creates the same timestamp make it
        readable again, dragging the reported last-sample backwards to a bucket the
        reference stopped reporting once it was removed. Regression test for the
        marker-liveness check in `add_dest_bucket`; found by the Tier C fuzzer.
        """
        diff = strict_subject
        self._delete_then_recreate(diff.reference)
        self._delete_then_recreate(diff.subject)

        ref_get = diff.reference.execute_command("TS.GET", "c:rdst")
        sub_get = diff.subject.execute_command("TS.GET", "c:rdst")
        assert sub_get == ref_get, f"strict should match RTS, got {sub_get!r} vs {ref_get!r}"
        assert sub_get[0] == 500, f"expected the bucket written after the delete, got {sub_get!r}"

        assert (
            diff.reference.execute_command("TS.RANGE", "c:rdst", "-", "+")
            == diff.subject.execute_command("TS.RANGE", "c:rdst", "-", "+")
        )

    @staticmethod
    def _backfill_then_delete_then_recreate(client):
        """Like `_delete_then_recreate`, but the back-fill lands BEFORE the delete, so a
        newer bucket survives it."""
        client.execute_command("TS.CREATE", "c:vsrc", "LABELS", "grp", "surv")
        client.execute_command("TS.CREATE", "c:vdst", "LABELS", "grp", "surv")
        client.execute_command(
            "TS.CREATERULE", "c:vsrc", "c:vdst", "AGGREGATION", "avg", 500
        )
        client.execute_command("TS.ADD", "c:vsrc", 0, 0)
        client.execute_command("TS.MADD", "c:vsrc", 1000, 0)  # closes [0,500) -> dst(0)
        client.execute_command("TS.ADD", "c:vsrc", 500, 0)    # back-fills dst(500)
        client.execute_command("TS.DEL", "c:vsrc", 0, 0)      # removes dst(0); dst(500) lives
        client.execute_command("TS.ADD", "c:vsrc", 0, 0)      # re-creates dst(0)

    def test_strict_mode_falls_back_to_the_surviving_bucket(self, strict_subject):
        """When the marker's bucket is removed, the cache falls to what the destination
        still holds — not to whatever is written next.

        The sibling test above leaves the destination empty, so "next write" and
        "surviving last sample" coincide and cannot tell the two rules apart. Here a newer
        bucket survives the delete, so claiming the marker for the next write (which
        re-creates the *older* timestamp) would report it and diverge.
        """
        diff = strict_subject
        self._backfill_then_delete_then_recreate(diff.reference)
        self._backfill_then_delete_then_recreate(diff.subject)

        ref_get = diff.reference.execute_command("TS.GET", "c:vdst")
        sub_get = diff.subject.execute_command("TS.GET", "c:vdst")
        assert sub_get == ref_get, f"strict should match RTS, got {sub_get!r} vs {ref_get!r}"
        assert sub_get[0] == 500, f"expected the surviving bucket, got {sub_get!r}"

        assert (
            diff.reference.execute_command("TS.RANGE", "c:vdst", "-", "+")
            == diff.subject.execute_command("TS.RANGE", "c:vdst", "-", "+")
        )

    def test_strict_mode_advances_on_the_next_forward_close(self, strict_subject):
        """The cached last-sample is stale only until forward progress closes the
        next bucket — at which point strict tracks RTS again."""
        diff = strict_subject
        self._backfill(diff.reference)
        self._backfill(diff.subject)

        # Closes [1000,1500), which both engines publish downstream.
        diff.reference.execute_command("TS.ADD", "c:bsrc", 2000, 0)
        diff.subject.execute_command("TS.ADD", "c:bsrc", 2000, 0)

        ref_get = diff.reference.execute_command("TS.GET", "c:bdst")
        sub_get = diff.subject.execute_command("TS.GET", "c:bdst")
        assert sub_get == ref_get, f"strict should match RTS, got {sub_get!r} vs {ref_get!r}"
        assert ref_get[0] == 1000, f"expected the newly closed bucket, got {ref_get!r}"


class TestReverseLatestAggregationOnDestination:
    """DIV-0030/0031: reverse + LATEST + AGGREGATION on a compaction destination, with the
    range ending before the destination's open bucket.

    `LATEST` appends the rule's still-open bucket to the destination's data. Reading
    backwards with a re-aggregation, RTS appears to start from that appended bucket, find it
    past the range end, and stop — returning nothing and dropping the closed bucket that is
    inside the range. We return it, which is what our own forward query and our own
    non-LATEST reverse query both return.

    Per-engine assertions rather than `diff`: the registry entries are scoped to
    "reference=[] subject=[[...]]", so routing the diverging call through `diff` would
    exercise the entry rather than the behavior. The boundary tests below are what keep the
    entry honest — they pin that dropping any single condition makes the engines agree, so a
    regression that widened the divergence would fail here instead of being absorbed.
    """

    @staticmethod
    def _rule_with_open_bucket(client):
        client.execute_command("TS.CREATE", "c:lsrc", "LABELS", "grp", "lat")
        client.execute_command("TS.CREATE", "c:ldst", "LABELS", "grp", "lat")
        client.execute_command(
            "TS.CREATERULE", "c:lsrc", "c:ldst", "AGGREGATION", "avg", 500
        )
        client.execute_command("TS.ADD", "c:lsrc", 0, 0)      # opens [0,500)
        client.execute_command("TS.MADD", "c:lsrc", 500, 0)   # closes it, opens [500,1000)

    def test_reverse_latest_aggregation_drops_the_in_range_bucket_on_rts(self, diff):
        self._rule_with_open_bucket(diff.reference)
        self._rule_with_open_bucket(diff.subject)

        args = ("TS.REVRANGE", "c:ldst", "-", 1, "LATEST", "AGGREGATION", "avg", 500)
        ref = diff.reference.execute_command(*args)
        sub = diff.subject.execute_command(*args)

        assert ref == [], f"expected RTS to drop the in-range bucket, got {ref!r}"
        assert [s[0] for s in sub] == [0], f"expected the closed bucket at 0, got {sub!r}"

        # The multi-key form behaves the same (DIV-0031).
        margs = ("TS.MREVRANGE", "-", 1, "LATEST", "AGGREGATION", "avg", 500,
                 "FILTER", "grp=lat")

        def samples_by_key(reply):
            """RESP2 gives [key, labels, samples] rows; RESP3 a map whose value list
            carries extra metadata entries. Samples are last in both."""
            items = reply.items() if isinstance(reply, dict) else ((r[0], r) for r in reply)
            return {k: v[-1] for k, v in items}

        mref = samples_by_key(diff.reference.execute_command(*margs))
        msub = samples_by_key(diff.subject.execute_command(*margs))
        assert mref[b"c:ldst"] == []
        assert [s[0] for s in msub[b"c:ldst"]] == [0]

    def test_our_reverse_agrees_with_our_own_forward(self, diff):
        """The reason we do not match RTS here: its reply contradicts itself."""
        self._rule_with_open_bucket(diff.subject)

        fwd = diff.subject.execute_command(
            "TS.RANGE", "c:ldst", "-", 1, "LATEST", "AGGREGATION", "avg", 500
        )
        rev = diff.subject.execute_command(
            "TS.REVRANGE", "c:ldst", "-", 1, "LATEST", "AGGREGATION", "avg", 500
        )
        assert [s[0] for s in fwd] == [s[0] for s in rev] == [0]

    def test_dropping_any_single_condition_agrees(self, diff):
        """All five conditions are required; each of these must diff clean, so the
        registry entries can not quietly widen."""
        self._rule_with_open_bucket(diff.reference)
        self._rule_with_open_bucket(diff.subject)

        # no LATEST
        diff("TS.REVRANGE", "c:ldst", "-", 1, "AGGREGATION", "avg", 500)
        # no AGGREGATION
        diff("TS.REVRANGE", "c:ldst", "-", 1, "LATEST")
        # forward instead of reverse
        diff("TS.RANGE", "c:ldst", "-", 1, "LATEST", "AGGREGATION", "avg", 500)
        # range end at/after the open bucket
        diff("TS.REVRANGE", "c:ldst", "-", 500, "LATEST", "AGGREGATION", "avg", 500)
        diff("TS.REVRANGE", "c:ldst", "-", "+", "LATEST", "AGGREGATION", "avg", 500)

    def test_plain_series_with_the_same_samples_agrees(self, diff):
        """Not a general reverse/LATEST bug: it needs a compaction destination."""
        diff("TS.CREATE", "c:lplain", "LABELS", "grp", "lat2")
        diff("TS.ADD", "c:lplain", 0, 0)
        diff("TS.ADD", "c:lplain", 500, 0)
        diff("TS.REVRANGE", "c:lplain", "-", 1, "LATEST", "AGGREGATION", "avg", 500)
