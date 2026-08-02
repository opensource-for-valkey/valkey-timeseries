//! `TS.READ` — read samples at or after a cursor, optionally waiting for more to arrive.
//!
//! Behavior is pinned to the 8.10 reference by black-box probe; see
//! `docs/ts-read-implementation-plan.md` §1 and §6 for the captured observations behind each
//! decision here.

use crate::common::block_on_keys::{BlockedKeyHandler, ReadyStatus, block_client_on_key};
use crate::common::context::is_blocking_denied;
use crate::common::replies::{ReplyContext, reply_with_samples};
use crate::common::{Sample, Timestamp};
use crate::error_consts;
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct ReadOptions {
    /// `BLOCK milliseconds min_count`. `None` means return whatever exists right now.
    pub block: Option<BlockOptions>,
    /// `MAX_COUNT max_count`. `None` means an unbounded reply.
    pub max_count: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BlockOptions {
    /// Wait limit. Zero means wait indefinitely.
    pub timeout_ms: i64,
    /// How many qualifying samples must exist before the client is served.
    pub min_count: usize,
}

/// Parse `[BLOCK milliseconds min_count] [MAX_COUNT max_count]` from the tail of the argument
/// list. Either clause may come first, and keywords are case-insensitive.
///
/// Malformed input splits into two failure classes, matching the reference: a duplicated clause,
/// a missing value, or an unrecognized token is plain wrong-arity, while an out-of-range value
/// gets a specific `TSDB:` message.
fn parse_read_options(args: &[&str]) -> ValkeyResult<ReadOptions> {
    let mut options = ReadOptions::default();
    let mut idx = 0;

    while idx < args.len() {
        let token = args[idx];

        if token.eq_ignore_ascii_case("BLOCK") {
            if options.block.is_some() || idx + 2 >= args.len() {
                return Err(ValkeyError::WrongArity);
            }
            let timeout_ms =
                parse_i64(args[idx + 1])
                    .filter(|ms| *ms >= 0)
                    .ok_or(ValkeyError::Str(
                        error_consts::READ_BLOCK_MS_MUST_BE_NON_NEGATIVE,
                    ))?;
            let min_count = parse_i64(args[idx + 2])
                .filter(|c| *c > 0)
                .ok_or(ValkeyError::Str(
                    error_consts::READ_MIN_COUNT_MUST_BE_POSITIVE,
                ))?;
            options.block = Some(BlockOptions {
                timeout_ms,
                min_count: min_count as usize,
            });
            idx += 3;
        } else if token.eq_ignore_ascii_case("MAX_COUNT") {
            if options.max_count.is_some() || idx + 1 >= args.len() {
                return Err(ValkeyError::WrongArity);
            }
            let max_count = parse_i64(args[idx + 1])
                .filter(|c| *c > 0)
                .ok_or(ValkeyError::Str(
                    error_consts::READ_MAX_COUNT_MUST_BE_POSITIVE,
                ))?;
            options.max_count = Some(max_count as usize);
            idx += 2;
        } else {
            return Err(ValkeyError::WrongArity);
        }
    }

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

/// Collect the qualifying samples for `cursor`, bounded two different ways.
///
/// `min_count` decides readiness and `max_count` bounds the reply, and they are *not* the same
/// bound. Readiness only needs to know whether the threshold is reachable, so this stops as soon
/// as the answer is settled: without a cap it reads at most `max(min_count, 1)` samples beyond
/// what it will return. That matters because this runs once per blocked client per signal, with
/// the server's execution lock held — an unbounded scan here would make every write cost
/// `O(blocked_readers × tail_length)`.
fn collect_samples(series: &TimeSeries, cursor: Cursor, options: &ReadOptions) -> ReadOutcome {
    let Cursor::From(start) = cursor else {
        // Nothing can ever be at or after "past i64::MAX".
        return ReadOutcome::Empty;
    };

    let min_count = options.block.map_or(1, |b| b.min_count);
    // Read enough to answer both questions and no more.
    //
    // An uncapped read is still bounded where it counts. The expensive case would be a
    // readiness check that walks a long tail only to report "not enough yet" — but that cannot
    // happen: if the answer is `Insufficient`, then fewer than `min_count` samples qualified in
    // the first place, so the scan stopped there on its own. The only way this reads a long tail
    // is when the threshold is met, and then every sample it read is one the reply owes the
    // client anyway.
    let scan_limit = options
        .max_count
        .map_or(usize::MAX, |max| max.max(min_count));

    let mut samples: Vec<Sample> = series
        .range_iter(start, Timestamp::MAX)
        .take(scan_limit)
        .collect();

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

/// TS.READ key timestamp [BLOCK milliseconds min_count] [MAX_COUNT max_count]
#[valkey_module_macros::command({
    name: "ts.read",
    flags: [ReadOnly],
    summary: "Read: return up to max_count samples with timestamp >= timestamp. With BLOCK, waits up to milliseconds ms until at least min_count qualifying samples exist",
    complexity: "O(log(n)+k) where n is the number of samples in the series and k is the number of returned samples",
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
    let spec = CursorSpec::parse(args[2].try_as_str()?)?;
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

    fn opts(min_count: Option<usize>, max_count: Option<usize>) -> ReadOptions {
        ReadOptions {
            block: min_count.map(|min_count| BlockOptions {
                timeout_ms: 0,
                min_count,
            }),
            max_count,
        }
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
    fn readiness_scan_is_bounded_by_the_threshold() {
        // The readiness question only needs min_count samples to answer. Scanning the whole tail
        // would make each write cost O(blocked_readers × tail_length).
        let series = series_with(&(0..1000).collect::<Vec<_>>());
        let outcome = collect_samples(&series, Cursor::From(0), &opts(Some(3), Some(3)));
        assert_eq!(outcome.into_samples().len(), 3);
    }

    #[test]
    fn an_uncapped_insufficient_read_is_bounded_by_the_threshold_too() {
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
}
