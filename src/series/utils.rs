use crate::common::constants::METRIC_NAME_LABEL;
use crate::common::context::{create_key_string, get_current_db, notify_keyspace_event};
use crate::common::rounding::RoundingStrategy;
use crate::error_consts;
use crate::labels::{InternedLabel, Label, MetricName};
use crate::parser::metric_name::parse_metric_name;
use crate::series::acl::{KeyAccess, check_key_permissions};
use crate::series::chunks::ChunkEncoding;
use crate::series::index::{get_db_index, next_timeseries_id};
use crate::series::series_data_type::VK_TIME_SERIES_TYPE;
use crate::series::{
    SeriesGuard, SeriesGuardMut, SeriesLink, TimeSeries, TimeSeriesOptions,
    create_compaction_rules_from_config,
};
use std::ops::Deref;
use std::time::Duration;
use valkey_module::key::ValkeyKeyWritable;
use valkey_module::{AclPermissions, Context, ValkeyError, ValkeyResult, ValkeyString};

/// Runs `f` against the series stored at `key`, opened read-only.
///
/// Errors with `KEY_NOT_FOUND` when the key is missing, `WRONGTYPE` when it holds another
/// type, and the ACL error when `permissions` is given and the caller lacks it. See
/// [`try_get_timeseries`] for the ordering of those checks.
pub fn with_timeseries<R>(
    ctx: &Context,
    key: &ValkeyString,
    permissions: Option<AclPermissions>,
    f: impl FnOnce(&TimeSeries) -> ValkeyResult<R>,
) -> ValkeyResult<R> {
    let series = get_timeseries(ctx, key, permissions)?;
    f(&series)
}

/// Runs `f` against the series stored at `key`, opened for writing.
///
/// Same error contract as [`with_timeseries`].
pub fn with_timeseries_mut<R>(
    ctx: &Context,
    key: &ValkeyString,
    permissions: Option<AclPermissions>,
    f: impl FnOnce(&mut TimeSeries) -> ValkeyResult<R>,
) -> ValkeyResult<R> {
    let mut series = get_timeseries_mut(ctx, key, permissions)?;
    f(&mut series)
}

/// Opens `key` read-only and returns a guard over the series it holds.
///
/// A missing key is `KEY_NOT_FOUND`; use [`try_get_timeseries`] when absence is an
/// expected outcome rather than an error.
pub fn get_timeseries<'a>(
    ctx: &'a Context,
    key: &ValkeyString,
    permissions: Option<AclPermissions>,
) -> ValkeyResult<SeriesGuard<'a>> {
    try_get_timeseries(ctx, key, permissions)?.ok_or_else(invalid_series_key_error)
}

/// Opens `key` read-only and returns a guard over the series it holds, or `Ok(None)` when
/// the key does not exist.
///
/// When `permissions` is given the ACL check runs *before* the key is looked up, so a
/// caller without access learns nothing about whether the key exists or what type it
/// holds — the same order the server applies to its own key-spec checks. A key holding a
/// non-TSDB value surfaces the standard `WRONGTYPE` error rather than valkey-module-rs's
/// raw "Existing key has wrong Valkey type", matching RTS.
pub fn try_get_timeseries<'a>(
    ctx: &'a Context,
    key: &ValkeyString,
    permissions: Option<AclPermissions>,
) -> ValkeyResult<Option<SeriesGuard<'a>>> {
    if let Some(permissions) = permissions {
        check_key_permissions(ctx, key, &permissions)?;
    }
    open_timeseries(ctx, key)
}

/// [`try_get_timeseries`] for a loop over many keys: the caller's identity is
/// resolved once into `access` and each key costs one permission check.
pub fn try_get_timeseries_as<'a>(
    ctx: &'a Context,
    key: &ValkeyString,
    access: &KeyAccess,
) -> ValkeyResult<Option<SeriesGuard<'a>>> {
    access.check(key)?;
    open_timeseries(ctx, key)
}

fn open_timeseries<'a>(
    ctx: &'a Context,
    key: &ValkeyString,
) -> ValkeyResult<Option<SeriesGuard<'a>>> {
    match SeriesGuard::from_key(ctx, key) {
        Ok(guard) => Ok(Some(guard)),
        Err(ValkeyError::Str(err)) if err == error_consts::KEY_NOT_FOUND => Ok(None),
        Err(e) => Err(e),
    }
}

/// Opens `key` for writing and returns a mutable guard over the series it holds.
///
/// A missing key is `KEY_NOT_FOUND`; use [`try_get_timeseries_mut`] when absence is an
/// expected outcome (e.g. the auto-create write commands).
pub fn get_timeseries_mut<'a>(
    ctx: &'a Context,
    key: &ValkeyString,
    permissions: Option<AclPermissions>,
) -> ValkeyResult<SeriesGuardMut<'a>> {
    try_get_timeseries_mut(ctx, key, permissions)?.ok_or_else(invalid_series_key_error)
}

/// Opens `key` for writing and returns a mutable guard over the series it holds, or
/// `Ok(None)` when the key does not exist.
///
/// Same check ordering and error contract as [`try_get_timeseries`].
pub fn try_get_timeseries_mut<'a>(
    ctx: &'a Context,
    key: &ValkeyString,
    permissions: Option<AclPermissions>,
) -> ValkeyResult<Option<SeriesGuardMut<'a>>> {
    if let Some(permissions) = permissions {
        check_key_permissions(ctx, key, &permissions)?;
    }
    match SeriesGuardMut::from_key(ctx, key) {
        Ok(guard) => Ok(Some(guard)),
        Err(ValkeyError::Str(err)) if err == error_consts::KEY_NOT_FOUND => Ok(None),
        Err(e) => Err(e),
    }
}

pub(crate) fn invalid_series_key_error() -> ValkeyError {
    ValkeyError::Str(error_consts::KEY_NOT_FOUND)
}

pub fn create_series(
    key: &ValkeyString,
    options: TimeSeriesOptions,
    ctx: &Context,
) -> ValkeyResult<TimeSeries> {
    let mut ts = TimeSeries::with_options(options)?;
    if ts.id == 0 {
        ts.id = next_timeseries_id();
    }
    ts.key = key.as_slice().into();

    let db = get_current_db(ctx);

    ts._db = Some(db);
    let guard = get_db_index(db);

    let index = guard.deref();

    // Check if this refers to an existing series (a pre-existing series with the same label-value pairs)
    // We do this only in the case where we have a __name__ label, signaling that the user is
    // opting in to Prometheus semantics, meaning a metric name is unique to a series.
    if ts.labels.get_value(METRIC_NAME_LABEL).is_some() {
        let labels = ts.labels.to_label_vec();
        // will return an error if the series already exists
        if index.series_id_by_labels(&labels).is_some() {
            return Err(ValkeyError::Str(error_consts::DUPLICATE_SERIES));
        }
    }

    index.index_timeseries(&ts, key.iter().as_slice());
    Ok(ts)
}

pub fn create_and_store_internal(
    ctx: &Context,
    key: &ValkeyString,
    options: TimeSeriesOptions,
    replicate: bool,
    notify: bool,
) -> ValkeyResult<()> {
    let _key = ValkeyKeyWritable::open(ctx.ctx, key);
    // check if this refers to an existing series
    if !_key.is_empty() {
        return Err(ValkeyError::Str(error_consts::DUPLICATE_KEY));
    }

    let ts = create_series(key, options, ctx)?;
    _key.set_value(&VK_TIME_SERIES_TYPE, ts)?;

    if replicate {
        ctx.replicate_verbatim();
    }
    // Only an explicit TS.CREATE emits `ts.create`; auto-creating write
    // commands (TS.ADD/TS.MADD/TS.INCRBY/...) emit just their own write
    // event, matching RedisTimeSeries (compat plan §7.3).
    if notify {
        notify_keyspace_event(ctx, c"ts.create", key);
        ctx.log_verbose("series created");
    }

    Ok(())
}

/// `explicit_create` distinguishes a client-issued `TS.CREATE` from the
/// auto-create performed by write commands (TS.ADD/TS.INCRBY/TS.ADDBULK):
///
/// - explicit: this call replicates verbatim and emits `ts.create`.
/// - auto-create: no `ts.create` event; the series itself is replicated as a resolved
///   `TS.CREATE` (see the body), and the calling command replicates its own write. (The old
///   corner case — a command erroring after the auto-create left the primary with an empty
///   series the replica never saw — is gone: the `TS.CREATE` is already queued.)
pub fn create_and_store_series<'a>(
    ctx: &'a Context,
    key: &ValkeyString,
    options: TimeSeriesOptions,
    explicit_create: bool,
    add_compactions: bool,
) -> ValkeyResult<SeriesGuardMut<'a>> {
    // Check before anything is stored, replicated or notified: a denial after
    // the create would leave the key behind.
    check_key_permissions(ctx, key, &AclPermissions::INSERT)?;
    create_and_store_internal(ctx, key, options, explicit_create, explicit_create)?;

    let mut series = get_timeseries_mut(ctx, key, None)?;

    if !explicit_create {
        // Auto-create: replicate the series as the primary built it, defaults materialized, ahead
        // of the write command's own propagation (`alsoPropagate` keeps call order). The replica's
        // replayed write then finds the series and its inline creation options are inert, exactly
        // as on the primary when the series already exists. `TS.CREATE` here, not
        // `replicate_verbatim`: that would copy the *write's* argv and apply it twice.
        let create_args = resolved_create_args(ctx, &series);
        let mut replication_args: Vec<&ValkeyString> = Vec::with_capacity(create_args.len() + 1);
        replication_args.push(key);
        replication_args.extend(create_args.iter());
        ctx.replicate("TS.CREATE", replication_args.as_slice());
    }

    if add_compactions {
        // If compactions are enabled, add the default compaction rules
        add_default_compactions(ctx, &mut series, key)?
    }
    Ok(series)
}

/// `TS.CREATE` options that recreate `series` exactly, every configuration-derived default
/// materialized. An auto-creating write replicates this ahead of itself so a replica (or an AOF
/// replay) builds the series from the primary's configuration, not its own: `ts-retention-policy`,
/// `ts-encoding` and the chunk size are per node, and a retention mismatch changes recalculation
/// floors and therefore compaction output.
///
/// `DUPLICATE_POLICY` is only emitted when the series pins one: an unset policy follows the
/// node's `ts-duplicate-policy` at write time, and pinning it here would change that.
///
/// `TS.CREATE` starts from the receiving node's configured defaults, so an option that is *absent*
/// here falls back to the replica's own value. That is harmless for the ones omitted above, and
/// `IGNORE` is therefore always written. Rounding is the one exception: there is no `TS.CREATE`
/// spelling for "no rounding", so a series without it is created with whatever the replica's
/// `ts-decimal-digits`/`ts-significant-digits` say.
///
/// A series with a `__name__` label is re-created through `METRIC` when that text parses back to
/// exactly the same label set, and through `LABELS` otherwise. `LABELS __name__ …` is accepted
/// too, so a series can hold a name or label names the metric grammar cannot spell
/// (`cpu total`, `1x`); `METRIC`, in turn, is the only way to spell an empty label value.
pub(crate) fn resolved_create_args(ctx: &Context, series: &TimeSeries) -> Vec<ValkeyString> {
    create_option_strings(series)
        .into_iter()
        .map(|arg| ctx.create_string(arg.as_bytes()))
        .collect()
}

/// The text of [`resolved_create_args`], separated so the rendering can be tested against the
/// option parsers without a server (building a `ValkeyString` needs one).
fn create_option_strings(series: &TimeSeries) -> Vec<String> {
    let mut args: Vec<String> = vec![
        "RETENTION".into(),
        series.retention.as_millis().to_string(),
        "ENCODING".into(),
        series.chunk_encoding.name().to_string(),
        "CHUNK_SIZE".into(),
        series.chunk_size_bytes.to_string(),
    ];
    if let Some(policy) = series.sample_duplicates.policy {
        args.push("DUPLICATE_POLICY".into());
        args.push(policy.as_str().to_string());
    }
    // Always spelled out, zeros included: `TS.CREATE` starts from the node's configured
    // `ts-ignore-max-*` defaults, so omitting a zero would let the replica's own apply.
    let dup = &series.sample_duplicates;
    args.push("IGNORE".into());
    args.push(dup.max_time_delta.to_string());
    args.push(dup.max_value_delta.to_string());
    match series.rounding {
        Some(RoundingStrategy::SignificantDigits(digits)) => {
            args.push("SIGNIFICANT_DIGITS".into());
            args.push(digits.to_string());
        }
        Some(RoundingStrategy::DecimalDigits(digits)) => {
            args.push("DECIMAL_DIGITS".into());
            args.push(digits.to_string());
        }
        None => {}
    }
    if series.labels.get_value(METRIC_NAME_LABEL).is_some()
        && let Some(metric) = metric_text(series)
    {
        args.push("METRIC".into());
        args.push(metric);
    } else if !series.labels.is_empty() {
        args.push("LABELS".into());
        for label in series.labels.iter() {
            args.push(label.name.to_string());
            args.push(label.value.to_string());
        }
    }
    args
}

/// The `METRIC` operand for `series`, if `TS.CREATE` would read it back as the same label set.
fn metric_text(series: &TimeSeries) -> Option<String> {
    let text = series.prometheus_metric_name();
    let parsed = parse_metric_name(&text).ok()?;
    (MetricName::new(&parsed) == series.labels).then_some(text)
}

fn add_default_compactions(
    ctx: &Context,
    series: &mut TimeSeries,
    key: &ValkeyString,
) -> ValkeyResult<()> {
    let key_str = key.to_string_lossy();
    let Some(compaction_rules) = create_compaction_rules_from_config(&key_str) else {
        // No compaction rules available for this key
        return Ok(());
    };

    let mut base_config = TimeSeriesOptions::from_config();
    base_config.chunk_encoding = ChunkEncoding::Uncompressed;

    // create a new series for each compaction rule
    let mut rules = Vec::with_capacity(compaction_rules.len());
    for (dest_key, (rule, retention)) in compaction_rules.into_iter() {
        let bucket_duration = rule.bucket_duration;
        let agg_type = rule.aggregator.aggregation_type();

        let mut labels = series.labels.to_label_vec();

        labels.push(Label::new("aggregation", agg_type.name()));
        let duration_str = bucket_duration.to_string();
        labels.push(Label::new("time_bucket", &duration_str));

        let child_key = create_key_string(ctx, dest_key.as_bytes());
        let options = TimeSeriesOptions {
            src: Some(SeriesLink::from_key(key.as_slice())),
            retention: Some(Duration::from_millis(retention)),
            labels: Some(labels),
            ..base_config
        };

        let value_key = ValkeyKeyWritable::open(ctx.ctx, &child_key);
        if !value_key.is_empty() {
            let msg = format!("TSDB: compaction series for key '{child_key}' already exists.");
            ctx.log_warning(&msg);
            continue;
        }

        let destination = match create_series(&child_key, options, ctx) {
            Ok(dest_series) => dest_series,
            Err(e) => {
                let msg =
                    format!("TSDB: error creating compaction series for key \"{child_key}\": {e}");
                ctx.log_warning(&msg);
                continue;
            }
        };

        value_key.set_value(&VK_TIME_SERIES_TYPE, destination)?;

        rules.push(rule);
    }
    series.rules = rules;

    Ok(())
}

pub fn get_series_labels<'a>(
    series: &'a TimeSeries,
    with_labels: bool,
    selected_labels: &[String],
) -> Vec<Option<InternedLabel<'a>>> {
    if !with_labels && selected_labels.is_empty() {
        return vec![];
    }

    if selected_labels.is_empty() {
        series.labels.iter().map(Some).collect::<Vec<_>>()
    } else {
        selected_labels
            .iter()
            .map(|name| series.get_label(name))
            .collect::<Vec<_>>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::command_parser::{
        parse_chunk_size, parse_duration, parse_duration_ms,
        parse_metric_name as parse_create_metric,
    };
    use crate::parser::number::parse_number;
    use crate::series::DuplicatePolicy;
    use crate::series::chunks::ChunkEncoding;

    fn series_with(options: TimeSeriesOptions) -> TimeSeries {
        TimeSeries::with_options(options).expect("valid options")
    }

    /// The operand(s) following `keyword`, which must appear exactly once in the option list.
    fn operands<'a>(args: &'a [String], keyword: &str, count: usize) -> &'a [String] {
        let positions: Vec<usize> = args
            .iter()
            .enumerate()
            .filter(|(_, arg)| arg.as_str() == keyword)
            .map(|(i, _)| i)
            .collect();
        assert_eq!(positions.len(), 1, "{keyword} in {args:?}");
        &args[positions[0] + 1..positions[0] + 1 + count]
    }

    #[test]
    fn test_renders_every_resolved_default() {
        let series = series_with(TimeSeriesOptions {
            retention: Some(Duration::from_millis(3000)),
            chunk_encoding: ChunkEncoding::Gorilla,
            chunk_size: Some(4096),
            duplicate_policy: Some(DuplicatePolicy::Sum),
            ignore: Some((10, 0.5)),
            rounding: Some(RoundingStrategy::DecimalDigits(2)),
            labels: Some(vec![Label::new("sensor", "temp"), Label::new("room", "r1")]),
            ..Default::default()
        });
        assert_eq!(
            create_option_strings(&series),
            [
                "RETENTION",
                "3000",
                "ENCODING",
                "gorilla",
                "CHUNK_SIZE",
                "4096",
                "DUPLICATE_POLICY",
                "sum",
                "IGNORE",
                "10",
                "0.5",
                "DECIMAL_DIGITS",
                "2",
                "LABELS",
                "room",
                "r1",
                "sensor",
                "temp",
            ]
        );
    }

    #[test]
    fn test_unset_options_are_not_pinned() {
        let series = series_with(TimeSeriesOptions {
            retention: Some(Duration::ZERO),
            chunk_size: Some(128),
            duplicate_policy: None,
            ..Default::default()
        });
        let args = create_option_strings(&series);
        for keyword in ["DUPLICATE_POLICY", "LABELS", "METRIC"] {
            assert!(!args.iter().any(|a| a == keyword), "{keyword} in {args:?}");
        }
        assert!(
            !args
                .iter()
                .any(|a| a == "SIGNIFICANT_DIGITS" || a == "DECIMAL_DIGITS"),
            "{args:?}"
        );
        // A zero retention is pinned too: omitting it would let the replica apply its own.
        assert_eq!(operands(&args, "RETENTION", 1), ["0"]);
        assert_eq!(operands(&args, "CHUNK_SIZE", 1), ["128"]);
    }

    #[test]
    fn test_rounding_strategies_render_their_own_keyword() {
        for (rounding, keyword, digits) in [
            (
                RoundingStrategy::SignificantDigits(5),
                "SIGNIFICANT_DIGITS",
                "5",
            ),
            (RoundingStrategy::DecimalDigits(0), "DECIMAL_DIGITS", "0"),
        ] {
            let series = series_with(TimeSeriesOptions {
                rounding: Some(rounding),
                ..Default::default()
            });
            let args = create_option_strings(&series);
            assert_eq!(operands(&args, keyword, 1), [digits]);
        }
    }

    #[test]
    fn test_every_encoding_name_is_accepted_by_ts_create() {
        for encoding in [
            ChunkEncoding::Uncompressed,
            ChunkEncoding::Gorilla,
            ChunkEncoding::Chimp,
        ] {
            let series = series_with(TimeSeriesOptions {
                chunk_encoding: encoding,
                ..Default::default()
            });
            let args = create_option_strings(&series);
            let name = &operands(&args, "ENCODING", 1)[0];
            assert_eq!(ChunkEncoding::try_from(name.as_str()).unwrap(), encoding);
        }
    }

    #[test]
    fn test_every_duplicate_policy_name_is_accepted_by_ts_create() {
        for policy in [
            DuplicatePolicy::Block,
            DuplicatePolicy::KeepFirst,
            DuplicatePolicy::KeepLast,
            DuplicatePolicy::Min,
            DuplicatePolicy::Max,
            DuplicatePolicy::Sum,
        ] {
            let series = series_with(TimeSeriesOptions {
                duplicate_policy: Some(policy),
                ..Default::default()
            });
            let args = create_option_strings(&series);
            let name = &operands(&args, "DUPLICATE_POLICY", 1)[0];
            assert_eq!(
                DuplicatePolicy::try_from(name.as_bytes()).unwrap(),
                policy,
                "{name}"
            );
        }
    }

    #[test]
    fn test_retention_and_chunk_size_parse_back_exactly() {
        for retention_ms in [0_u64, 1, 500, 3000, 86_400_000, 31_536_000_000, 1 << 40] {
            let series = series_with(TimeSeriesOptions {
                retention: Some(Duration::from_millis(retention_ms)),
                ..Default::default()
            });
            let args = create_option_strings(&series);
            let rendered = &operands(&args, "RETENTION", 1)[0];
            assert_eq!(
                parse_duration(rendered).unwrap(),
                Duration::from_millis(retention_ms),
                "{rendered}"
            );
        }
        for chunk_size in [48_usize, 128, 4096, 1_048_576] {
            let series = series_with(TimeSeriesOptions {
                chunk_size: Some(chunk_size),
                ..Default::default()
            });
            let args = create_option_strings(&series);
            let rendered = &operands(&args, "CHUNK_SIZE", 1)[0];
            assert_eq!(
                parse_chunk_size(rendered).unwrap(),
                chunk_size,
                "{rendered}"
            );
        }
    }

    #[test]
    fn test_ignore_operands_parse_back_to_the_same_values() {
        let values = [
            0.0,
            0.1,
            0.5,
            1.0,
            0.300_000_000_000_000_04,
            1e-9,
            123_456_789.123_456_79,
            1e15,
            1e300,
            f64::MIN_POSITIVE,
            5e-324,
            f64::MAX,
            f64::INFINITY,
        ];
        for (time_delta, value_delta) in
            values
                .iter()
                .map(|&v| (1_u64, v))
                .chain([(0, 0.25), (60_000, 0.0), (1 << 40, 2.0)])
        {
            let series = series_with(TimeSeriesOptions {
                ignore: Some((time_delta, value_delta)),
                ..Default::default()
            });
            let args = create_option_strings(&series);
            let [time, value] = operands(&args, "IGNORE", 2) else {
                unreachable!()
            };
            assert_eq!(
                parse_duration_ms(time).unwrap(),
                time_delta as i64,
                "{time}"
            );
            assert_eq!(
                parse_number(value).unwrap().to_bits(),
                value_delta.to_bits(),
                "{value}"
            );
        }
    }

    /// Zeros are written too: `TS.CREATE` starts from the node's configured `ts-ignore-max-*`
    /// defaults, so leaving them out would let a replica's own non-zero ones apply.
    #[test]
    fn test_zero_ignore_is_pinned() {
        let series = series_with(TimeSeriesOptions {
            ignore: Some((0, 0.0)),
            ..Default::default()
        });
        let args = create_option_strings(&series);
        assert_eq!(operands(&args, "IGNORE", 2), ["0", "0"]);
    }

    fn render_labels(labels: &[Label]) -> (TimeSeries, Vec<String>) {
        let series = series_with(TimeSeriesOptions {
            labels: Some(labels.to_vec()),
            ..Default::default()
        });
        let args = create_option_strings(&series);
        (series, args)
    }

    #[test]
    fn test_metric_round_trips_through_the_metric_parser() {
        let cases: Vec<Vec<Label>> = vec![
            vec![Label::new("__name__", "cpu")],
            vec![Label::new("__name__", "cpu"), Label::new("host", "a")],
            vec![
                Label::new("__name__", "http_requests_total"),
                Label::new("method", "POST"),
                Label::new("status", "500"),
            ],
            vec![Label::new("__name__", "a:b:c"), Label::new("l", "v")],
            vec![
                Label::new("__name__", "cpu"),
                Label::new("quote", "say \"hi\""),
                Label::new("slash", "a\\b"),
                Label::new("newline", "a\nb"),
                Label::new("braces", "{x=\"y\"},z"),
                Label::new("unicode", "caf\u{e9} \u{1f600}"),
                Label::new("spaces", "  padded  "),
            ],
            // An empty value can only be spelled through METRIC.
            vec![Label::new("__name__", "cpu"), Label::new("a", "")],
        ];
        for labels in &cases {
            let (series, args) = render_labels(labels);
            assert!(
                !args.contains(&"LABELS".to_string()),
                "{labels:?}: {args:?}"
            );
            let metric = &operands(&args, "METRIC", 1)[0];
            let parsed = parse_create_metric(metric).expect("TS.CREATE METRIC accepts it");
            assert_eq!(MetricName::new(&parsed), series.labels, "{metric}");
        }
    }

    /// `LABELS __name__ …` is accepted by `TS.CREATE`/`TS.ALTER`, so a series can hold a name or
    /// label names the metric grammar cannot spell. Those fall back to `LABELS`, which keeps
    /// `__name__` as an ordinary pair; rendering them through `METRIC` would make the replica's
    /// `TS.CREATE` fail and rebuild the series from its own defaults.
    #[test]
    fn test_labels_the_metric_grammar_cannot_spell_fall_back_to_labels() {
        let cases: Vec<Vec<Label>> = vec![
            vec![Label::new("__name__", "cpu total"), Label::new("1x", "a")],
            vec![Label::new("__name__", "cpu"), Label::new("caf\u{e9}", "a")],
            vec![Label::new("__name__", "9cpu")],
            vec![Label::new("__name__", "cpu"), Label::new("a b", "c")],
        ];
        for labels in &cases {
            let (series, args) = render_labels(labels);
            assert!(
                !args.contains(&"METRIC".to_string()),
                "{labels:?}: {args:?}"
            );
            let pos = args.iter().position(|a| a == "LABELS").unwrap();
            let pairs: Vec<Label> = args[pos + 1..]
                .chunks(2)
                .map(|pair| Label::new(&pair[0], &pair[1]))
                .collect();
            assert_eq!(MetricName::new(&pairs), series.labels, "{args:?}");
            assert!(args.contains(&"__name__".to_string()), "{args:?}");
        }
    }

    #[test]
    fn test_plain_labels_render_in_name_order() {
        let series = series_with(TimeSeriesOptions {
            labels: Some(vec![
                Label::new("b", "2"),
                Label::new("a", "1"),
                Label::new("caf\u{e9}", "\u{1f600}"),
            ]),
            ..Default::default()
        });
        let args = create_option_strings(&series);
        let pos = args.iter().position(|a| a == "LABELS").unwrap();
        assert_eq!(
            args[pos + 1..],
            ["a", "1", "b", "2", "caf\u{e9}", "\u{1f600}"]
        );
    }
}
