//! `TS.READ` — read samples at or after a cursor, optionally waiting for more to arrive.
//!
//! Behavior is pinned to the 8.10 reference by black-box probe; see
//! `docs/plans/ts-read-implementation-plan.md` §1 and §6 for the captured observations behind each
//! decision here.

use crate::commands::parse_duration_ms;
use crate::common::binop::ComparisonOperator;
use crate::common::block_on_keys::{BlockedKeyHandler, ReadyStatus, block_client_on_key};
use crate::common::context::is_blocking_denied;
use crate::common::replies::{ReplyContext, reply_with_samples};
use crate::common::{Sample, Timestamp};
use crate::error_consts;
use crate::series::request_types::ValueComparisonFilter;
use crate::series::{TimeSeries, get_timeseries};
use valkey_module::raw::KeyType;
use valkey_module::{
    AclPermissions, Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};

/// The resolved lower bound of a read, fixed once when the command starts and held stable for the
/// lifetime of a blocked client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Cursor {
    /// Inclusive lower bound. Everything at or after this timestamp qualifies.
    From(Timestamp),
    /// Past the end of the timestamp domain: `$` resolved against a series whose newest sample is
    /// already at `i64::MAX`. Nothing can ever qualify, so this reads as permanently empty rather
    /// than overflowing to `i64::MIN` or wrongly including that newest sample.
    PastMax,
}

impl Cursor {
    /// Resolve the command's `timestamp` argument against the series it will read.
    ///
    /// `series` is `None` for a missing key. An absent or empty series resolves every sentinel to
    /// "everything, including whatever arrives later": there is no stored data to anchor `-`, `+`,
    /// or `$` to, and a blocked client must still wake on the first sample written.
    fn resolve(spec: CursorSpec, series: Option<&TimeSeries>) -> Cursor {
        let Some(series) = series.filter(|s| !s.is_empty()) else {
            return match spec {
                CursorSpec::Literal(ts) => Cursor::From(ts),
                _ => Cursor::From(Timestamp::MIN),
            };
        };

        match spec {
            CursorSpec::Literal(ts) => Cursor::From(ts),
            CursorSpec::Earliest => Cursor::From(series.first_timestamp),
            CursorSpec::Latest => Cursor::From(series.last_timestamp()),
            CursorSpec::Next => match series.last_timestamp().checked_add(1) {
                Some(next) => Cursor::From(next),
                None => Cursor::PastMax,
            },
        }
    }
}

/// The `timestamp` argument as written by the client, before it is resolved against stored data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CursorSpec {
    /// A literal millisecond timestamp.
    Literal(Timestamp),
    /// `-`: the earliest stored sample.
    Earliest,
    /// `+`: the newest stored sample, inclusive.
    Latest,
    /// `$`: one past the newest stored sample — only data that arrives later.
    Next,
}

impl CursorSpec {
    fn parse(arg: &str) -> ValkeyResult<Self> {
        match arg {
            "-" => Ok(CursorSpec::Earliest),
            "+" => Ok(CursorSpec::Latest),
            "$" => Ok(CursorSpec::Next),
            _ => {
                // Only a plain non-negative integer is accepted. Notably *not*
                // `parse_timestamp`, which also takes `*` and relative forms the reference
                // rejects here with "invalid timestamp".
                let ts: Timestamp = arg
                    .parse()
                    .map_err(|_| ValkeyError::Str(error_consts::INVALID_TIMESTAMP))?;
                if ts < 0 {
                    return Err(ValkeyError::Str(error_consts::INVALID_TIMESTAMP));
                }
                Ok(CursorSpec::Literal(ts))
            }
        }
    }
}

/// The optional clauses, after validation.
///
/// Not `Eq`: `condition` holds an `f64`, which is only `PartialEq`. `ValueComparisonFilter`'s own
/// `PartialEq` treats two NaN comparison values as equal, so `CONDITION == nan` still compares
/// equal to itself.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub(crate) struct ReadOptions {
    /// `BLOCK milliseconds min_count`. `None` means return whatever exists right now.
    pub block: Option<BlockOptions>,
    /// `MAX_COUNT max_count`. `None` means an unbounded reply.
    pub max_count: Option<usize>,
    /// `CONDITION op value`. `None` means every timestamp-eligible sample qualifies.
    pub condition: Option<ValueComparisonFilter>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BlockOptions {
    /// Wait limit. Zero means wait indefinitely.
    pub timeout_ms: i64,
    /// How many qualifying samples must exist before the client is served.
    pub min_count: usize,
}

/// Parse `[BLOCK milliseconds min_count] [MAX_COUNT max_count] [CONDITION op value]` from the tail
/// of the argument list. The clauses may come in any order, and keywords are case-insensitive.
///
/// Malformed input splits into two failure classes, matching the reference: a duplicated clause,
/// a missing value, or an unrecognized token is plain wrong-arity, while an out-of-range value
/// gets a specific `TSDB:` message.
///
/// `CONDITION` is an additive extension with no reference behavior to match; it follows the same
/// two-class split. Its operator must be one of the six exact spellings — a fused `CONDITION >500`
/// is an invalid *operator*, not wrong-arity, because the clause is unambiguously present.
fn parse_read_options(args: &[&str]) -> ValkeyResult<ReadOptions> {
    let mut iter = args.iter().copied();
    let mut options = ReadOptions::default();

    while let Some(arg) = iter.next() {
        hashify::fnc_map_ignore_case!(arg.as_bytes(),
            "BLOCK" => {
                if options.block.is_some() {
                    return Err(ValkeyError::WrongArity);
                }
                let timeout_str = iter.next().ok_or(ValkeyError::WrongArity)?;
                let timeout_ms = if let Ok(value) = timeout_str.parse::<i64>() {
                    if value < 0 {
                        return Err(ValkeyError::Str(error_consts::READ_BLOCK_MS_MUST_BE_NON_NEGATIVE));
                    }
                    value
                } else {
                    parse_duration_ms(timeout_str).map_err(|_| {
                        ValkeyError::Str(error_consts::READ_BLOCK_MS_MUST_BE_NON_NEGATIVE)
                    })?
                };
                let min_count = iter.next()
                    .ok_or(ValkeyError::WrongArity)
                    .map(parse_i64)?
                    .filter(|c| *c > 0)
                    .ok_or(ValkeyError::Str(error_consts::READ_MIN_COUNT_MUST_BE_POSITIVE))?;

                options.block = Some(BlockOptions {
                    timeout_ms,
                    min_count: min_count as usize,
                });
            },
            "MAX_COUNT" => {
                if options.max_count.is_some() {
                    return Err(ValkeyError::WrongArity);
                }
                let max_count = iter.next()
                    .ok_or(ValkeyError::WrongArity)
                    .map(parse_i64)?
                    .filter(|c| *c > 0)
                    .ok_or(ValkeyError::Str(error_consts::READ_MAX_COUNT_MUST_BE_POSITIVE))?;
                options.max_count = Some(max_count as usize);
            },
            "CONDITION" => {
                if options.condition.is_some() {
                    return Err(ValkeyError::WrongArity);
                }
                // Already `TSDB: invalid comparison operator`, the same text every other
                // condition-taking surface returns.
                let operator = ComparisonOperator::try_from(
                    iter.next().ok_or(ValkeyError::WrongArity)?
                )?;
                // The same spellings `TS.ADD` accepts for a sample value, so a condition can name any
                // value the series can store — `nan`, `inf`, `-inf`, exponent notation.
                let value = iter.next()
                    .ok_or(ValkeyError::WrongArity)?
                    .parse::<f64>()
                    .map_err(|_| ValkeyError::Str(error_consts::READ_CONDITION_VALUE_MUST_BE_A_NUMBER))?;
                options.condition = Some(ValueComparisonFilter { operator, value });
            },
            _ => return Err(ValkeyError::WrongArity)
        );
    }

    // `CONDITION` on its own stays a plain filtered read. Inferring `BLOCK 0 1 MAX_COUNT 1` from
    // it — an "alert" shorthand — was tried and reverted: it leaves no way to spell "filter this
    // read without blocking", silently truncates a multi-match reply to one sample, turns a read
    // the caller never asked to block into one that never returns, and makes every conditioned
    // read fail inside MULTI/EVAL on the deny-blocking guard.

    // Checked before any key access: the reference rejects this even for a missing key.
    if let (Some(block), Some(max_count)) = (options.block, options.max_count)
        && block.min_count > max_count
    {
        return Err(ValkeyError::Str(
            error_consts::READ_MIN_COUNT_EXCEEDS_MAX_COUNT,
        ));
    }

    Ok(options)
}

fn parse_i64(arg: &str) -> Option<i64> {
    arg.parse::<i64>().ok()
}

/// What a read of the current stored data found.
enum ReadOutcome {
    /// The key is absent, or holds an empty series. Replies as an empty array.
    Empty,
    /// At least `min_count` samples qualify; these are they, already capped by `MAX_COUNT`.
    Samples(Vec<Sample>),
    /// Fewer than `min_count` samples qualify. Carries what exists so a timeout can still reply
    /// with the partial result.
    Insufficient(Vec<Sample>),
}

impl ReadOutcome {
    /// The samples to send, for the paths that reply regardless of the threshold.
    fn into_samples(self) -> Vec<Sample> {
        match self {
            ReadOutcome::Empty => Vec::new(),
            ReadOutcome::Samples(samples) | ReadOutcome::Insufficient(samples) => samples,
        }
    }
}

/// Collect the qualifying samples for `cursor`: those at or after it, and — when `CONDITION` is
/// given — those whose value also satisfies the comparison.
///
/// `min_count` decides readiness and `max_count` bounds the reply, and they are *not* the same
/// bound. Readiness only needs to know whether the threshold is reachable, so this stops as soon
/// as the answer is settled. What "as soon as" costs differs between the two paths, and the
/// difference is deliberate:
///
/// - **Without a condition** the scan limit bounds *samples inspected*: without a cap this reads
///   at most `max(min_count, 1)` samples beyond what it will return. An uncapped read is still
///   bounded where it counts, because the expensive case — walking a long tail only to report
///   "not enough yet" — cannot happen. If the answer is `Insufficient`, then fewer than
///   `min_count` samples existed at all, so the scan stopped there on its own.
/// - **With a condition** that reasoning no longer holds, and the limit bounds *matches
///   collected* instead. A satisfied read still stops as soon as it has enough, but an
///   unsatisfied one scans the entire tail looking for matches that are not there. This is an
///   accepted cost, not an oversight: the alternatives (a scan watermark, a per-wakeup budget)
///   each break a correctness guarantee this command already makes about out-of-order writes and
///   timeout snapshots. Callers pair `CONDITION` with `$` or a recent cursor to bound the tail.
///
/// Either way this runs once per blocked client per signal, with the server's execution lock
/// held.
fn collect_samples(series: &TimeSeries, cursor: Cursor, options: &ReadOptions) -> ReadOutcome {
    let Cursor::From(start) = cursor else {
        // Nothing can ever be at or after "past i64::MAX".
        return ReadOutcome::Empty;
    };

    let min_count = options.block.map_or(1, |b| b.min_count);
    // Read enough to answer both questions and no more. See the two bounds above.
    let scan_limit = options
        .max_count
        .map_or(usize::MAX, |max| max.max(min_count));

    let iter = series.range_iter(start, Timestamp::MAX);
    // Filter *before* take. Taking first would cap the scan at the first `scan_limit`
    // timestamp-eligible samples and silently miss every match sitting past them.
    let mut samples: Vec<Sample> = match options.condition {
        Some(condition) => iter
            .filter(|sample| condition.compare(sample.value))
            .take(scan_limit)
            .collect(),
        None => iter.take(scan_limit).collect(),
    };

    if samples.len() < min_count {
        return ReadOutcome::Insufficient(samples);
    }

    if let Some(max_count) = options.max_count {
        samples.truncate(max_count);
    }

    ReadOutcome::Samples(samples)
}

/// Open the key and read it. `Ok(None)` means the key is absent — not an error for this command.
///
/// Shared by the initial call and by both blocked-client callbacks. Readiness is always
/// re-evaluated from current stored data: retention, out-of-order writes, or another client may
/// have changed the qualifying set since the signal that woke us.
fn read_current(
    ctx: &Context,
    key: &ValkeyString,
    spec: CursorSpec,
    resolved: Option<Cursor>,
    options: &ReadOptions,
) -> ValkeyResult<(Cursor, ReadOutcome)> {
    let guard = get_timeseries(ctx, key, Some(AclPermissions::ACCESS), false)?;
    let series = guard.as_deref();

    // A blocked client keeps the cursor it resolved at block time; a fresh call resolves now.
    let cursor = resolved.unwrap_or_else(|| Cursor::resolve(spec, series));

    let Some(series) = series else {
        return Ok((cursor, ReadOutcome::Empty));
    };
    Ok((cursor, collect_samples(series, cursor, options)))
}

/// Private state for a blocked reader. Owned by the server between the block and its resolution.
struct PendingRead {
    key: ValkeyString,
    cursor: Cursor,
    options: ReadOptions,
}

impl PendingRead {
    /// Re-read and decide. Shared by both callbacks, which differ only in what they do when the
    /// threshold is still unmet.
    fn reread(&self, ctx: &ReplyContext) -> ValkeyResult<ReadOutcome> {
        read_current(
            ctx.context(),
            &self.key,
            // Irrelevant: `resolved` short-circuits it. The cursor was fixed at block time.
            CursorSpec::Literal(0),
            Some(self.cursor),
            &self.options,
        )
        .map(|(_, outcome)| outcome)
    }
}

impl BlockedKeyHandler for PendingRead {
    fn on_ready(&self, ctx: &ReplyContext) -> ReadyStatus {
        match self.reread(ctx) {
            // Key deleted while we waited, or still empty: an empty array is a success reply, and
            // deletion is terminal for this client because we blocked with UNBLOCK_DELETED.
            Ok(ReadOutcome::Empty) => {
                if key_exists(ctx, &self.key) {
                    // The key is still there and simply has nothing for us yet.
                    ReadyStatus::NotReady
                } else {
                    reply_with_samples(ctx.raw(), std::iter::empty());
                    ReadyStatus::Replied
                }
            }
            Ok(ReadOutcome::Samples(samples)) => {
                reply_with_samples(ctx.raw(), samples.into_iter());
                ReadyStatus::Replied
            }
            Ok(ReadOutcome::Insufficient(_)) => ReadyStatus::NotReady,
            Err(e) => {
                // Wrong type, or ACL revoked while blocked. Either way the client cannot be
                // served by waiting longer.
                reply_error(ctx, &e);
                ReadyStatus::Replied
            }
        }
    }

    fn on_timeout(&self, ctx: &ReplyContext) {
        match self.reread(ctx) {
            Ok(outcome) => {
                let samples = outcome.into_samples();
                reply_with_samples(ctx.raw(), samples.into_iter());
            }
            Err(e) => reply_error(ctx, &e),
        }
    }
}

/// Whether the key still holds a value of any type. Distinguishes "deleted while blocked" (a
/// terminal empty reply) from "exists but has nothing for this cursor yet" (keep waiting).
fn key_exists(ctx: &ReplyContext, key: &ValkeyString) -> bool {
    ctx.context().open_key(key).key_type() != KeyType::Empty
}

fn reply_error(ctx: &ReplyContext, err: &ValkeyError) {
    match err {
        ValkeyError::Str(s) => ctx.reply_error_string(s),
        ValkeyError::String(s) => ctx.reply_error_string(s),
        ValkeyError::WrongType => ctx.reply_error_string(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        ),
        other => ctx.reply_error_string(&other.to_string()),
    };
}

/// TS.READ key timestamp [BLOCK milliseconds min_count] [MAX_COUNT max_count] [CONDITION op value]
#[valkey_module_macros::command({
    name: "ts.read",
    flags: [ReadOnly],
    summary: "Read: return up to max_count samples with timestamp >= timestamp, optionally only those whose value satisfies CONDITION. With BLOCK, waits up to milliseconds ms until at least min_count qualifying samples exist",
    complexity: "O(log(n)+m) where n is the number of samples in the series and m is the number of samples examined. Without CONDITION m is the number of returned samples; with CONDITION a sparse condition may examine every sample at or after timestamp",
    since: "8.10.0",
    // Matches the reference, which is the only command in either module to carry a tip. The
    // reply depends on when it is served, not just on the arguments, so a proxy must never
    // serve it from cache. This is the sole `tips` user in this module; the macro's `tips`
    // field is what makes matching the reference possible here rather than a divergence.
    tips: "dont_cache",
    arity: -3,
    key_spec: [{
        flags: [ReadOnly, Access],
        begin_search: Index({ index: 1 }),
        find_keys: Range({ last_key: 0, steps: 1, limit: 0 })
    }]
})]
pub fn ts_read_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 3 {
        return Err(ValkeyError::WrongArity);
    }

    let key = args[1].clone();
    let cursor_spec = args[2].try_as_str()?;
    let spec = CursorSpec::parse(cursor_spec)?;
    // A non-UTF8 token cannot match either keyword, so it lands in the same bucket as any other
    // unrecognized token: wrong-arity.
    let tail: Vec<&str> = args[3..]
        .iter()
        .map(|a| a.try_as_str())
        .collect::<ValkeyResult<_>>()
        .map_err(|_| ValkeyError::WrongArity)?;
    let options = parse_read_options(&tail)?;

    let (cursor, outcome) = read_current(ctx, &key, spec, None, &options)?;

    let Some(block) = options.block else {
        reply_with_samples(ctx, outcome.into_samples().into_iter());
        return Ok(ValkeyValue::NoReply);
    };

    match outcome {
        // Threshold already met — reply now. This happens *before* the deny-blocking check, so a
        // satisfiable TS.READ inside MULTI or Lua succeeds rather than erroring, matching the
        // reference.
        ReadOutcome::Samples(samples) => {
            reply_with_samples(ctx, samples.into_iter());
            Ok(ValkeyValue::NoReply)
        }
        ReadOutcome::Empty | ReadOutcome::Insufficient(_) => {
            // Not satisfiable now, so we would have to block. Refuse if the context forbids it —
            // this check is necessary, not defensive: blocking a deny-blocking client trips a
            // serverAssert and takes the process down.
            if is_blocking_denied(ctx) {
                return Err(ValkeyError::Str(error_consts::READ_BLOCKING_NOT_ALLOWED));
            }

            let pending = PendingRead {
                key: key.clone(),
                cursor,
                options,
            };
            if block_client_on_key(ctx, &key, block.timeout_ms, pending) {
                Ok(ValkeyValue::NoReply)
            } else {
                // The server refused to block; we still owe a reply.
                ctx.log_warning("TS.READ: failed to block client; replying with current data");
                reply_with_samples(ctx, std::iter::empty());
                Ok(ValkeyValue::NoReply)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::series::TimeSeries;

    fn series_with(timestamps: &[Timestamp]) -> TimeSeries {
        let mut series = TimeSeries::default();
        for ts in timestamps {
            assert!(
                series.add(*ts, *ts as f64, None).is_ok(),
                "failed to seed sample at {ts}"
            );
        }
        series
    }

    fn series_with_values(samples: &[(Timestamp, f64)]) -> TimeSeries {
        let mut series = TimeSeries::default();
        for (ts, value) in samples {
            assert!(
                series.add(*ts, *value, None).is_ok(),
                "failed to seed sample at {ts}"
            );
        }
        series
    }

    fn opts(min_count: Option<usize>, max_count: Option<usize>) -> ReadOptions {
        ReadOptions {
            block: min_count.map(|min_count| BlockOptions {
                timeout_ms: 0,
                min_count,
            }),
            max_count,
            condition: None,
        }
    }

    fn cond_opts(
        min_count: Option<usize>,
        max_count: Option<usize>,
        operator: ComparisonOperator,
        value: f64,
    ) -> ReadOptions {
        ReadOptions {
            condition: Some(ValueComparisonFilter { operator, value }),
            ..opts(min_count, max_count)
        }
    }

    /// The values of a conditioned read over `samples`, for the value-semantics tests.
    fn matching_values(
        samples: &[(Timestamp, f64)],
        operator: ComparisonOperator,
        value: f64,
    ) -> Vec<f64> {
        let series = series_with_values(samples);
        collect_samples(
            &series,
            Cursor::From(0),
            &cond_opts(None, None, operator, value),
        )
        .into_samples()
        .iter()
        .map(|s| s.value)
        .collect()
    }

    // -- cursor parsing ----------------------------------------------------

    #[test]
    fn parses_sentinels_and_literals() {
        assert_eq!(CursorSpec::parse("-").unwrap(), CursorSpec::Earliest);
        assert_eq!(CursorSpec::parse("+").unwrap(), CursorSpec::Latest);
        assert_eq!(CursorSpec::parse("$").unwrap(), CursorSpec::Next);
        assert_eq!(CursorSpec::parse("0").unwrap(), CursorSpec::Literal(0));
        assert_eq!(
            CursorSpec::parse("1234567890").unwrap(),
            CursorSpec::Literal(1234567890)
        );
    }

    #[test]
    fn rejects_negative_and_unparseable_timestamps() {
        // Probe #9: the reference answers "TSDB: invalid timestamp" for a negative literal.
        for bad in ["-5", "abc", "", "*", "1.5", "9223372036854775808"] {
            assert!(CursorSpec::parse(bad).is_err(), "should reject {bad:?}");
        }
    }

    // -- cursor resolution -------------------------------------------------

    #[test]
    fn resolves_sentinels_against_stored_data() {
        let series = series_with(&[100, 200, 300]);
        let s = Some(&series);
        assert_eq!(
            Cursor::resolve(CursorSpec::Earliest, s),
            Cursor::From(100),
            "`-` is the earliest stored sample"
        );
        assert_eq!(
            Cursor::resolve(CursorSpec::Latest, s),
            Cursor::From(300),
            "`+` is the newest sample, inclusive"
        );
        assert_eq!(
            Cursor::resolve(CursorSpec::Next, s),
            Cursor::From(301),
            "`$` is one past the newest sample"
        );
        assert_eq!(
            Cursor::resolve(CursorSpec::Literal(250), s),
            Cursor::From(250)
        );
    }

    #[test]
    fn resolves_next_at_i64_max_without_overflowing() {
        // Probe #7: `$` against a series whose newest sample is i64::MAX reads as permanently
        // empty. Wrapping to i64::MIN here would instead return the entire series.
        let series = series_with(&[Timestamp::MAX]);
        assert_eq!(
            Cursor::resolve(CursorSpec::Next, Some(&series)),
            Cursor::PastMax
        );
        assert_eq!(
            Cursor::resolve(CursorSpec::Latest, Some(&series)),
            Cursor::From(Timestamp::MAX),
            "`+` still includes that sample"
        );
    }

    #[test]
    fn past_max_cursor_never_yields_samples() {
        let series = series_with(&[Timestamp::MAX]);
        let outcome = collect_samples(&series, Cursor::PastMax, &opts(None, None));
        assert!(outcome.into_samples().is_empty());
    }

    #[test]
    fn missing_and_empty_series_resolve_to_everything() {
        // Probes #1, #2, #21: all four forms read empty, but a client blocked on a missing key
        // must still wake on the first sample written, so the cursor cannot be "the latest".
        let empty = TimeSeries::default();
        for spec in [CursorSpec::Earliest, CursorSpec::Latest, CursorSpec::Next] {
            assert_eq!(
                Cursor::resolve(spec, None),
                Cursor::From(Timestamp::MIN),
                "missing key, {spec:?}"
            );
            assert_eq!(
                Cursor::resolve(spec, Some(&empty)),
                Cursor::From(Timestamp::MIN),
                "empty series, {spec:?}"
            );
        }
        assert_eq!(
            Cursor::resolve(CursorSpec::Literal(42), None),
            Cursor::From(42),
            "a literal is still itself"
        );
    }

    // -- retrieval ---------------------------------------------------------

    #[test]
    fn reads_inclusively_from_the_cursor() {
        let series = series_with(&[100, 200, 300]);
        let samples = collect_samples(&series, Cursor::From(200), &opts(None, None)).into_samples();
        assert_eq!(
            samples.iter().map(|s| s.timestamp).collect::<Vec<_>>(),
            vec![200, 300],
            "probe #4: the boundary sample is included"
        );
    }

    #[test]
    fn returns_out_of_order_writes_in_ascending_order() {
        let series = series_with(&[300, 100, 200]);
        let samples = collect_samples(&series, Cursor::From(0), &opts(None, None)).into_samples();
        assert_eq!(
            samples.iter().map(|s| s.timestamp).collect::<Vec<_>>(),
            vec![100, 200, 300]
        );
    }

    #[test]
    fn max_count_truncates_the_reply() {
        let series = series_with(&[100, 200, 300]);
        let samples =
            collect_samples(&series, Cursor::From(0), &opts(None, Some(2))).into_samples();
        assert_eq!(
            samples.iter().map(|s| s.timestamp).collect::<Vec<_>>(),
            vec![100, 200],
            "probe #11"
        );
    }

    #[test]
    fn unbounded_read_returns_everything() {
        let series = series_with(&[1, 2, 3, 4, 5]);
        let samples = collect_samples(&series, Cursor::From(0), &opts(None, None)).into_samples();
        assert_eq!(samples.len(), 5);
    }

    #[test]
    fn threshold_decides_sufficiency() {
        let series = series_with(&[100, 200]);
        assert!(matches!(
            collect_samples(&series, Cursor::From(0), &opts(Some(3), None)),
            ReadOutcome::Insufficient(_)
        ));
        assert!(matches!(
            collect_samples(&series, Cursor::From(0), &opts(Some(2), None)),
            ReadOutcome::Samples(_)
        ));
    }

    #[test]
    fn insufficient_still_carries_the_partial_result() {
        // Probe #18: a timeout replies with what exists, even below min_count.
        let series = series_with(&[100]);
        let outcome = collect_samples(&series, Cursor::From(0), &opts(Some(3), None));
        assert!(matches!(outcome, ReadOutcome::Insufficient(_)));
        assert_eq!(outcome.into_samples().len(), 1);
    }

    #[test]
    fn readiness_scan_without_a_condition_is_bounded_by_the_threshold() {
        // The readiness question only needs min_count samples to answer. Scanning the whole tail
        // would make each write cost O(blocked_readers × tail_length).
        let series = series_with(&(0..1000).collect::<Vec<_>>());
        let outcome = collect_samples(&series, Cursor::From(0), &opts(Some(3), Some(3)));
        assert_eq!(outcome.into_samples().len(), 3);
    }

    #[test]
    fn an_uncapped_insufficient_read_without_a_condition_is_bounded_by_the_threshold_too() {
        // Without MAX_COUNT the scan limit is unbounded, which looks like the pathological case
        // the bound above exists to prevent. It is not: falling short of min_count means there
        // were fewer than min_count samples to read. This pins that reasoning, because the
        // readiness callback runs once per blocked client per write.
        let series = series_with(&[100, 200]);
        let outcome = collect_samples(&series, Cursor::From(0), &opts(Some(50), None));
        assert!(matches!(outcome, ReadOutcome::Insufficient(_)));
        assert_eq!(
            outcome.into_samples().len(),
            2,
            "an insufficient result is self-bounding: it cannot exceed min_count"
        );
    }

    // -- retrieval with a condition ----------------------------------------

    #[test]
    fn a_condition_reads_sample_first() {
        // `CONDITION > 5` keeps 6, not 4 — the sample is the left operand.
        let samples = [(100, 4.0), (200, 5.0), (300, 6.0)];
        assert_eq!(
            matching_values(&samples, ComparisonOperator::GreaterThan, 5.0),
            vec![6.0]
        );
        assert_eq!(
            matching_values(&samples, ComparisonOperator::LessThan, 5.0),
            vec![4.0]
        );
    }

    #[test]
    fn every_operator_selects_the_expected_samples() {
        let samples = [(100, 4.0), (200, 5.0), (300, 6.0)];
        for (operator, expected) in [
            (ComparisonOperator::Equal, vec![5.0]),
            (ComparisonOperator::NotEqual, vec![4.0, 6.0]),
            (ComparisonOperator::GreaterThan, vec![6.0]),
            (ComparisonOperator::GreaterThanOrEqual, vec![5.0, 6.0]),
            (ComparisonOperator::LessThan, vec![4.0]),
            (ComparisonOperator::LessThanOrEqual, vec![4.0, 5.0]),
        ] {
            assert_eq!(
                matching_values(&samples, operator, 5.0),
                expected,
                "operator {operator}"
            );
        }
    }

    #[test]
    fn a_condition_narrows_an_already_timestamp_bounded_read() {
        // The condition filters *after* the cursor, never before it.
        let series = series_with_values(&[(100, 9.0), (200, 1.0), (300, 9.0)]);
        let samples = collect_samples(
            &series,
            Cursor::From(200),
            &cond_opts(None, None, ComparisonOperator::GreaterThan, 5.0),
        )
        .into_samples();
        assert_eq!(
            samples.iter().map(|s| s.timestamp).collect::<Vec<_>>(),
            vec![300],
            "the matching sample below the cursor stays excluded"
        );
    }

    #[test]
    fn a_condition_matching_nothing_reads_empty() {
        let series = series_with_values(&[(100, 1.0), (200, 2.0)]);
        let outcome = collect_samples(
            &series,
            Cursor::From(0),
            &cond_opts(None, None, ComparisonOperator::GreaterThan, 500.0),
        );
        assert!(outcome.into_samples().is_empty());
    }

    #[test]
    fn nan_semantics_differ_between_equality_and_ordering() {
        // `==`/`!=` carry the module's custom NaN handling; the four ordering operators are plain
        // IEEE and are therefore always false against a NaN on either side.
        let samples = [(100, f64::NAN), (200, 1.0), (300, f64::INFINITY)];

        let eq_nan = matching_values(&samples, ComparisonOperator::Equal, f64::NAN);
        assert_eq!(eq_nan.len(), 1, "== nan selects exactly the NaN samples");
        assert!(eq_nan[0].is_nan());

        assert_eq!(
            matching_values(&samples, ComparisonOperator::NotEqual, f64::NAN),
            vec![1.0, f64::INFINITY],
            "!= nan excludes the NaN sample"
        );

        for operator in [
            ComparisonOperator::GreaterThan,
            ComparisonOperator::GreaterThanOrEqual,
            ComparisonOperator::LessThan,
            ComparisonOperator::LessThanOrEqual,
        ] {
            assert!(
                matching_values(&samples, operator, f64::NAN).is_empty(),
                "{operator} nan matches nothing"
            );
            let against_nan_sample = matching_values(&[(100, f64::NAN)], operator, 0.0);
            assert!(
                against_nan_sample.is_empty(),
                "a NaN sample never satisfies {operator}"
            );
        }
    }

    #[test]
    fn infinities_compare_as_ordinary_values() {
        let samples = [(100, f64::NEG_INFINITY), (200, 0.0), (300, f64::INFINITY)];
        assert_eq!(
            matching_values(&samples, ComparisonOperator::GreaterThan, 0.0),
            vec![f64::INFINITY]
        );
        assert_eq!(
            matching_values(&samples, ComparisonOperator::Equal, f64::NEG_INFINITY),
            vec![f64::NEG_INFINITY]
        );
    }

    #[test]
    fn signed_zeros_compare_equal() {
        // IEEE: -0.0 == 0.0. A condition on either spelling selects both.
        let samples = [(100, 0.0), (200, -0.0), (300, 1.0)];
        assert_eq!(
            matching_values(&samples, ComparisonOperator::Equal, -0.0).len(),
            2
        );
        assert_eq!(
            matching_values(&samples, ComparisonOperator::LessThanOrEqual, 0.0).len(),
            2
        );
        assert!(
            matching_values(&samples, ComparisonOperator::LessThan, 0.0).is_empty(),
            "-0.0 is not less than 0.0"
        );
    }

    #[test]
    fn readiness_counts_matching_samples_not_all_samples() {
        // Five samples qualify by timestamp but only two by value, so a threshold of three is
        // unmet — the count that decides readiness is the count of matches.
        let samples = [
            (100, 1.0),
            (200, 600.0),
            (300, 2.0),
            (400, 700.0),
            (500, 3.0),
        ];
        let series = series_with_values(&samples);
        let unmet = collect_samples(
            &series,
            Cursor::From(0),
            &cond_opts(Some(3), None, ComparisonOperator::GreaterThan, 500.0),
        );
        assert!(matches!(unmet, ReadOutcome::Insufficient(_)));
        assert_eq!(
            unmet.into_samples().len(),
            2,
            "the partial result is the matches, not the timestamp-eligible samples"
        );

        let met = collect_samples(
            &series,
            Cursor::From(0),
            &cond_opts(Some(2), None, ComparisonOperator::GreaterThan, 500.0),
        );
        assert!(matches!(met, ReadOutcome::Samples(_)));
    }

    #[test]
    fn max_count_truncates_matches_only_after_filtering() {
        // The case a `take`-before-`filter` implementation gets wrong: every match sits past
        // position max_count in the underlying series, so capping the scan first would report
        // nothing at all.
        let mut samples: Vec<(Timestamp, f64)> = (0..50).map(|i| (i as Timestamp, 1.0)).collect();
        samples.extend([(50, 600.0), (51, 700.0), (52, 800.0)]);
        let series = series_with_values(&samples);

        let collected = collect_samples(
            &series,
            Cursor::From(0),
            &cond_opts(None, Some(2), ComparisonOperator::GreaterThan, 500.0),
        )
        .into_samples();
        assert_eq!(
            collected.iter().map(|s| s.value).collect::<Vec<_>>(),
            vec![600.0, 700.0],
            "MAX_COUNT caps matches, not source samples inspected"
        );
    }

    #[test]
    fn an_unsatisfied_conditioned_read_examines_the_whole_tail() {
        // The accepted cost, pinned so it stays a decision rather than a surprise: with a
        // condition, the scan limit bounds matches collected, not samples inspected. Both matches
        // here sit at the very end of a 1000-sample tail, and an unmet threshold still finds them.
        let mut samples: Vec<(Timestamp, f64)> = (0..998).map(|i| (i as Timestamp, 1.0)).collect();
        samples.extend([(998, 600.0), (999, 700.0)]);
        let series = series_with_values(&samples);

        let outcome = collect_samples(
            &series,
            Cursor::From(0),
            &cond_opts(Some(5), None, ComparisonOperator::GreaterThan, 500.0),
        );
        assert!(matches!(outcome, ReadOutcome::Insufficient(_)));
        assert_eq!(
            outcome
                .into_samples()
                .iter()
                .map(|s| s.value)
                .collect::<Vec<_>>(),
            vec![600.0, 700.0],
            "an insufficient conditioned read is no longer self-bounding"
        );
    }

    #[test]
    fn a_satisfied_conditioned_read_still_stops_early() {
        // The other half of the bound: once enough matches are in hand the scan ends, even though
        // a long tail of further samples remains.
        let mut samples: Vec<(Timestamp, f64)> = vec![(0, 600.0), (1, 700.0)];
        samples.extend((2..1000).map(|i| (i as Timestamp, 900.0)));
        let series = series_with_values(&samples);

        let outcome = collect_samples(
            &series,
            Cursor::From(0),
            &cond_opts(Some(2), Some(2), ComparisonOperator::GreaterThan, 500.0),
        );
        assert!(matches!(outcome, ReadOutcome::Samples(_)));
        assert_eq!(outcome.into_samples().len(), 2);
    }

    // -- blocked conditioned reads with MAX_COUNT > 1 ----------------------
    //
    // `min_count` and `max_count` are separate bounds, and a condition makes them separate in a
    // way the unconditioned path never exercises: the scan limit counts *matches*, so these pin
    // that a blocked reader replies at `min_count` matches, never returns more than `max_count`,
    // and still finds matches that sit past position `max_count` in the underlying series.

    #[test]
    fn a_blocked_conditioned_read_caps_at_max_count_not_min_count() {
        // Six matches available, threshold 2, cap 4 — the reply is the four earliest matches.
        // Replying with `min_count` would under-deliver; replying with all six would overrun the
        // cap the caller set.
        let samples: Vec<(Timestamp, f64)> = (0..12)
            .map(|i| {
                (
                    i as Timestamp,
                    if i % 2 == 0 { 1.0 } else { 600.0 + i as f64 },
                )
            })
            .collect();
        let series = series_with_values(&samples);

        let outcome = collect_samples(
            &series,
            Cursor::From(0),
            &cond_opts(Some(2), Some(4), ComparisonOperator::GreaterThan, 500.0),
        );
        assert!(matches!(outcome, ReadOutcome::Samples(_)));
        assert_eq!(
            outcome
                .into_samples()
                .iter()
                .map(|s| s.timestamp)
                .collect::<Vec<_>>(),
            vec![1, 3, 5, 7],
            "the four earliest matches, in ascending order"
        );
    }

    #[test]
    fn a_blocked_conditioned_read_returns_every_match_between_the_two_bounds() {
        // Three matches, threshold 2, cap 5: under the cap, so nothing is truncated. A reply of
        // exactly `min_count` here would silently drop the third match.
        let series = series_with_values(&[
            (100, 1.0),
            (200, 600.0),
            (300, 2.0),
            (400, 700.0),
            (500, 800.0),
        ]);
        let outcome = collect_samples(
            &series,
            Cursor::From(0),
            &cond_opts(Some(2), Some(5), ComparisonOperator::GreaterThan, 500.0),
        );
        assert!(matches!(outcome, ReadOutcome::Samples(_)));
        assert_eq!(
            outcome
                .into_samples()
                .iter()
                .map(|s| s.timestamp)
                .collect::<Vec<_>>(),
            vec![200, 400, 500]
        );
    }

    #[test]
    fn a_blocked_conditioned_read_finds_matches_past_position_max_count() {
        // The filter-before-take property, on the blocking path: every match sits well past
        // position `max_count` in the series, so a scan capped by position would report
        // `Insufficient` and leave the reader blocked forever.
        let mut samples: Vec<(Timestamp, f64)> = (0..50).map(|i| (i as Timestamp, 1.0)).collect();
        samples.extend([(50, 600.0), (51, 700.0), (52, 800.0), (53, 900.0)]);
        let series = series_with_values(&samples);

        let outcome = collect_samples(
            &series,
            Cursor::From(0),
            &cond_opts(Some(2), Some(3), ComparisonOperator::GreaterThan, 500.0),
        );
        assert!(matches!(outcome, ReadOutcome::Samples(_)));
        assert_eq!(
            outcome
                .into_samples()
                .iter()
                .map(|s| s.timestamp)
                .collect::<Vec<_>>(),
            vec![50, 51, 52]
        );
    }

    #[test]
    fn max_count_above_one_does_not_mask_an_unmet_threshold() {
        // The cap bounds the scan, so it could in principle stop before the threshold is settled.
        // It cannot: the limit is `max(max_count, min_count)` and the parser enforces
        // `min_count <= max_count`, so hitting the limit means at least `min_count` matches are
        // in hand. Falling short therefore always means the tail really was exhausted.
        let series = series_with_values(&[(100, 1.0), (200, 600.0), (300, 2.0)]);
        let outcome = collect_samples(
            &series,
            Cursor::From(0),
            &cond_opts(Some(3), Some(5), ComparisonOperator::GreaterThan, 500.0),
        );
        assert!(matches!(outcome, ReadOutcome::Insufficient(_)));
        assert_eq!(
            outcome
                .into_samples()
                .iter()
                .map(|s| s.timestamp)
                .collect::<Vec<_>>(),
            vec![200],
            "the timeout snapshot carries the matches found, below the threshold"
        );
    }

    #[test]
    fn an_uncapped_blocked_conditioned_read_returns_every_match() {
        // No MAX_COUNT: the scan limit is unbounded, so the reply is every match, not `min_count`
        // of them.
        let samples: Vec<(Timestamp, f64)> = (0..20)
            .map(|i| (i as Timestamp, if i % 2 == 0 { 1.0 } else { 600.0 }))
            .collect();
        let series = series_with_values(&samples);

        let outcome = collect_samples(
            &series,
            Cursor::From(0),
            &cond_opts(Some(2), None, ComparisonOperator::GreaterThan, 500.0),
        );
        assert!(matches!(outcome, ReadOutcome::Samples(_)));
        assert_eq!(outcome.into_samples().len(), 10);
    }

    // -- option parsing ----------------------------------------------------

    fn parse(args: &[&str]) -> ValkeyResult<ReadOptions> {
        parse_read_options(args)
    }

    fn err_of(args: &[&str]) -> String {
        match parse(args) {
            Err(ValkeyError::Str(s)) => s.to_string(),
            Err(ValkeyError::WrongArity) => "WRONGARITY".to_string(),
            Err(other) => format!("{other:?}"),
            Ok(_) => panic!("expected {args:?} to fail"),
        }
    }

    #[test]
    fn accepts_either_option_order_and_any_case() {
        // Probe #12.
        let a = parse(&["BLOCK", "50", "1", "MAX_COUNT", "2"]).unwrap();
        let b = parse(&["MAX_COUNT", "2", "BLOCK", "50", "1"]).unwrap();
        assert_eq!(a, b);
        assert_eq!(a.max_count, Some(2));
        assert_eq!(a.block.unwrap().min_count, 1);
        assert_eq!(a.block.unwrap().timeout_ms, 50);

        assert_eq!(parse(&["block", "50", "1", "max_count", "2"]).unwrap(), a);
        assert_eq!(parse(&["BlOcK", "50", "1", "Max_Count", "2"]).unwrap(), a);
    }

    #[test]
    fn no_options_is_an_unbounded_immediate_read() {
        let parsed = parse(&[]).unwrap();
        assert_eq!(parsed.block, None);
        assert_eq!(parsed.max_count, None);
    }

    #[test]
    fn malformed_options_are_arity_errors() {
        // Probe #13: the reference reports plain wrong-arity for all of these, not a syntax or
        // TSDB: error. The class is observable through the ERR prefix, so it is part of the
        // contract.
        assert_eq!(
            err_of(&["BLOCK", "10", "1", "BLOCK", "10", "1"]),
            "WRONGARITY"
        );
        assert_eq!(err_of(&["MAX_COUNT", "1", "MAX_COUNT", "2"]), "WRONGARITY");
        assert_eq!(err_of(&["BLOCK", "50"]), "WRONGARITY");
        assert_eq!(err_of(&["MAX_COUNT"]), "WRONGARITY");
        assert_eq!(err_of(&["BOGUS"]), "WRONGARITY");
        assert_eq!(err_of(&["BLOCK", "50", "1", "EXTRA"]), "WRONGARITY");
    }

    #[test]
    fn out_of_range_values_get_specific_messages() {
        // Probes #14, #15, #16.
        assert_eq!(
            err_of(&["MAX_COUNT", "0"]),
            error_consts::READ_MAX_COUNT_MUST_BE_POSITIVE
        );
        assert_eq!(
            err_of(&["MAX_COUNT", "-1"]),
            error_consts::READ_MAX_COUNT_MUST_BE_POSITIVE
        );
        assert_eq!(
            err_of(&["MAX_COUNT", "abc"]),
            error_consts::READ_MAX_COUNT_MUST_BE_POSITIVE
        );
        assert_eq!(
            err_of(&["BLOCK", "50", "0"]),
            error_consts::READ_MIN_COUNT_MUST_BE_POSITIVE
        );
        assert_eq!(
            err_of(&["BLOCK", "50", "-1"]),
            error_consts::READ_MIN_COUNT_MUST_BE_POSITIVE
        );
        assert_eq!(
            err_of(&["BLOCK", "-1", "1"]),
            error_consts::READ_BLOCK_MS_MUST_BE_NON_NEGATIVE
        );
        assert_eq!(
            err_of(&["BLOCK", "abc", "1"]),
            error_consts::READ_BLOCK_MS_MUST_BE_NON_NEGATIVE
        );
    }

    #[test]
    fn min_count_may_not_exceed_max_count() {
        // Probe #17, and it is rejected before any key access.
        assert_eq!(
            err_of(&["BLOCK", "500", "5", "MAX_COUNT", "1"]),
            error_consts::READ_MIN_COUNT_EXCEEDS_MAX_COUNT
        );
        assert_eq!(
            err_of(&["MAX_COUNT", "1", "BLOCK", "500", "5"]),
            error_consts::READ_MIN_COUNT_EXCEEDS_MAX_COUNT
        );
        // Equal is fine.
        assert!(parse(&["BLOCK", "500", "2", "MAX_COUNT", "2"]).is_ok());
    }

    #[test]
    fn block_zero_is_accepted_as_indefinite() {
        let parsed = parse(&["BLOCK", "0", "1"]).unwrap();
        assert_eq!(parsed.block.unwrap().timeout_ms, 0);
    }

    // -- CONDITION parsing -------------------------------------------------

    #[test]
    fn parses_all_six_operators() {
        for (spelling, operator) in [
            ("==", ComparisonOperator::Equal),
            ("!=", ComparisonOperator::NotEqual),
            (">", ComparisonOperator::GreaterThan),
            (">=", ComparisonOperator::GreaterThanOrEqual),
            ("<", ComparisonOperator::LessThan),
            ("<=", ComparisonOperator::LessThanOrEqual),
        ] {
            let parsed = parse(&["CONDITION", spelling, "500"]).unwrap();
            assert_eq!(
                parsed.condition,
                Some(ValueComparisonFilter {
                    operator,
                    value: 500.0
                }),
                "operator {spelling}"
            );
        }
    }

    #[test]
    fn rejects_operator_spellings_that_are_not_the_six() {
        // `=` is not an alias for `==`, and the fused form is an invalid *operator* rather than
        // wrong-arity: the clause is unambiguously present, only its operator token is wrong.
        for bad in ["=", ">500", "=>", "<>", "gt", "!", ""] {
            assert_eq!(
                err_of(&["CONDITION", bad, "500"]),
                error_consts::INVALID_COMPARISON_OPERATOR,
                "operator {bad:?}"
            );
        }
    }

    #[test]
    fn rejects_condition_values_that_are_not_numbers() {
        for bad in ["abc", "", "5x", "1,5"] {
            assert_eq!(
                err_of(&["CONDITION", ">", bad]),
                error_consts::READ_CONDITION_VALUE_MUST_BE_A_NUMBER,
                "value {bad:?}"
            );
        }
    }

    #[test]
    fn accepts_every_value_spelling_a_sample_can_hold() {
        // Same spellings TS.ADD accepts, so a condition can name any storable value.
        assert!(
            parse(&["CONDITION", "==", "nan"])
                .unwrap()
                .condition
                .unwrap()
                .value
                .is_nan()
        );
        assert!(
            parse(&["CONDITION", "==", "+NaN"])
                .unwrap()
                .condition
                .unwrap()
                .value
                .is_nan()
        );
        for (spelling, expected) in [
            ("inf", f64::INFINITY),
            ("-inf", f64::NEG_INFINITY),
            ("infinity", f64::INFINITY),
            ("1e3", 1000.0),
            ("-2.5E-2", -0.025),
            ("-0", -0.0),
        ] {
            let parsed = parse(&["CONDITION", ">", spelling])
                .unwrap()
                .condition
                .unwrap()
                .value;
            assert_eq!(parsed, expected, "value {spelling}");
            // -0.0 == 0.0, so the equality above can't tell the two zeroes apart.
            assert_eq!(
                parsed.is_sign_negative(),
                expected.is_sign_negative(),
                "sign of {spelling}"
            );
        }
    }

    #[test]
    fn condition_parses_in_any_position_and_any_case() {
        let expected = ReadOptions {
            block: Some(BlockOptions {
                timeout_ms: 50,
                min_count: 1,
            }),
            max_count: Some(2),
            condition: Some(ValueComparisonFilter {
                operator: ComparisonOperator::GreaterThan,
                value: 500.0,
            }),
        };
        for ordering in [
            vec![
                "BLOCK",
                "50",
                "1",
                "MAX_COUNT",
                "2",
                "CONDITION",
                ">",
                "500",
            ],
            vec![
                "CONDITION",
                ">",
                "500",
                "BLOCK",
                "50",
                "1",
                "MAX_COUNT",
                "2",
            ],
            vec![
                "MAX_COUNT",
                "2",
                "CONDITION",
                ">",
                "500",
                "BLOCK",
                "50",
                "1",
            ],
            vec![
                "BLOCK",
                "50",
                "1",
                "CONDITION",
                ">",
                "500",
                "MAX_COUNT",
                "2",
            ],
            vec![
                "condition",
                ">",
                "500",
                "block",
                "50",
                "1",
                "max_count",
                "2",
            ],
            vec![
                "CoNdItIoN",
                ">",
                "500",
                "BlOcK",
                "50",
                "1",
                "MAX_count",
                "2",
            ],
        ] {
            assert_eq!(parse(&ordering).unwrap(), expected, "ordering {ordering:?}");
        }

        // On its own, too.
        let alone = parse(&["CONDITION", "<=", "0"]).unwrap();
        assert_eq!(alone.block, None);
        assert_eq!(alone.max_count, None);
        assert_eq!(
            alone.condition.unwrap().operator,
            ComparisonOperator::LessThanOrEqual
        );
    }

    #[test]
    fn malformed_condition_clauses_are_arity_errors() {
        // Same failure-class split the other clauses use: a duplicate or a truncated clause is
        // wrong-arity, only the two content failures get a TSDB: message.
        assert_eq!(
            err_of(&["CONDITION", ">", "1", "CONDITION", "<", "2"]),
            "WRONGARITY"
        );
        assert_eq!(err_of(&["CONDITION"]), "WRONGARITY");
        assert_eq!(err_of(&["CONDITION", ">"]), "WRONGARITY");
        assert_eq!(err_of(&["CONDITION", ">", "500", "EXTRA"]), "WRONGARITY");
        assert_eq!(
            err_of(&["CONDITION", ">", "500", "MAX_COUNT"]),
            "WRONGARITY"
        );
    }

    #[test]
    fn a_condition_does_not_disturb_the_min_max_count_check() {
        assert_eq!(
            err_of(&["CONDITION", ">", "1", "BLOCK", "500", "5", "MAX_COUNT", "1"]),
            error_consts::READ_MIN_COUNT_EXCEEDS_MAX_COUNT
        );
        assert!(parse(&["BLOCK", "500", "2", "MAX_COUNT", "2", "CONDITION", ">", "1"]).is_ok());
    }
}
