use crate::analysis::outliers::{
    Anomaly, AnomalyDetectionMethodOptions, AnomalyDirection, AnomalyMethod, AnomalyOptions,
    AnomalyResult, ESDOutlierOptions, EWMA_DEFAULT_ALPHA, EsdEstimator, MADAnomalyOptions,
    MethodInfo, RCF_DEFAULT_NUM_TREES, RCF_DEFAULT_SAMPLE_SIZE, RCFOptions, RCFThreshold,
    SmoothedZScoreOptions, detect_anomalies,
};
use crate::analysis::seasonality::Seasonality;
use crate::commands::{
    CommandArgIterator, CommandArgToken, parse_command_arg_token, parse_timestamp_range,
};
use crate::common::Sample;
use crate::common::hash::{IntMap, IntSet};
use crate::common::replies::{
    ReplyContext, ThreadSafeReplyContext, block_client, reply_with_sample,
};
use crate::common::threads::spawn;
use crate::error_consts;
use crate::series::{TimestampRange, get_timeseries};
use valkey_module::{
    AclPermissions, Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};

const MAX_SEASONALITY_PERIODS: usize = 4;

static COMMAND_OPTIONS: [CommandArgToken; 4] = [
    CommandArgToken::Direction,
    CommandArgToken::Output,
    CommandArgToken::Method,
    CommandArgToken::Seasonality,
];

enum OutputFormat {
    Full,
    Simple,
    Cleaned,
}

#[derive(PartialEq, Eq)]
enum ZScoreType {
    Standard,
    Modified,
    Smoothed,
}

/// TS.OUTLIERS key fromTimestamp toTimestamp
///     METHOD <method> [method-specific-options]
///     [OUTPUT <full|simple|cleaned>]
///     [DIRECTION <positive|negative|both>]
///     [SEASONALITY <period1> [period2] ...]
#[valkey_module_macros::command({
    name: "TS.OUTLIERS",
    flags: [ReadOnly, DenyOOM],
    summary: "Detect outlier samples in a time series over a timestamp range.",
    complexity: "O(N) where N is the number of samples in the requested range.",
    since: "1.0.0",
    arity: -6,
    key_spec: [{
        flags: [ReadOnly, Access],
        begin_search: Index({ index: 1 }),
        find_keys: Range({ last_key: 0, steps: 1, limit: 0 })
    }]
})]
pub fn ts_outliers_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 6 {
        return Err(ValkeyError::WrongArity);
    }

    let mut args = args.into_iter().skip(1).peekable();

    let key = args.next_arg()?;
    // Parse timestamps
    let date_range = parse_timestamp_range(&mut args)?;

    let mut anomaly_direction = AnomalyDirection::Both;
    let mut output_format = OutputFormat::Simple;
    let mut options: Option<AnomalyOptions> = None;
    let mut seasonality: Option<Seasonality> = None;

    while let Some(arg) = args.next() {
        let token = parse_command_arg_token(arg.as_slice()).unwrap_or_default();
        match token {
            CommandArgToken::Output => {
                let format_str = args.next_str()?;
                output_format = parse_output_format(format_str)?;
            }
            CommandArgToken::Direction => {
                let dir_str = args.next_str()?;
                anomaly_direction = dir_str.parse()?;
            }
            CommandArgToken::Seasonality => {
                seasonality = Some(parse_seasonality(&mut args)?);
            }
            CommandArgToken::Method => {
                if options.is_some() {
                    return Err(ValkeyError::Str("TSDB: outliers METHOD already specified"));
                }

                let method: AnomalyMethod = args.next_str()?.parse()?;
                options = Some(parse_method_options(method, &mut args)?);
            }
            _ => {
                return Err(ValkeyError::String(format!(
                    "TSDB: unknown option OUTLIERS {arg}"
                )));
            }
        }
    }

    let mut options = options.unwrap_or_default();
    options.seasonality = seasonality;

    process_request(
        ctx,
        key,
        date_range,
        options,
        anomaly_direction,
        output_format,
    )
}

fn process_request(
    ctx: &Context,
    key: ValkeyString,
    date_range: TimestampRange,
    options: AnomalyOptions,
    anomaly_direction: AnomalyDirection,
    output_format: OutputFormat,
) -> ValkeyResult {
    let key = ctx.create_string(key);

    let samples = match get_timeseries(ctx, &key, Some(AclPermissions::ACCESS), false) {
        Ok(Some(series)) => {
            let (start, end) = date_range.get_series_range(&series, None, false);
            series.get_range(start, end)
        }
        Ok(None) => return Err(ValkeyError::Str(error_consts::KEY_NOT_FOUND)),
        Err(e) => return Err(e),
    };

    if !should_run_in_background(samples.len(), options.method()) {
        let values: Vec<f64> = samples.iter().map(|s| s.value).collect();
        let reply_ctx = ReplyContext::new(ctx.ctx);
        return match detect_anomalies(&values, &options) {
            Err(err) => Err(ValkeyError::String(format!(
                "TSDB: outlier detection failed: {err}"
            ))),
            Ok(result) => send_reply(
                &reply_ctx,
                result,
                samples,
                anomaly_direction,
                output_format,
                &options,
            ),
        };
    }

    let blocked_client = block_client(ctx);
    spawn(move || {
        let thread_ctx = ThreadSafeReplyContext::with_blocked_client(blocked_client);

        let values: Vec<f64> = samples.iter().map(|s| s.value).collect();
        match detect_anomalies(&values, &options) {
            Err(err) => {
                thread_ctx.reply(Err(ValkeyError::String(format!(
                    "TSDB: outlier detection failed: {err}"
                ))));
            }
            Ok(result) => {
                let ctx = thread_ctx.get_reply_context();
                let _ = send_reply(
                    &ctx,
                    result,
                    samples,
                    anomaly_direction,
                    output_format,
                    &options,
                );
            }
        }
    });

    // Reply will be sent from the background thread
    Ok(ValkeyValue::NoReply)
}

const SPAWN_THRESHOLD_SAMPLES: usize = 1000;

fn is_cpu_intensive(method: AnomalyMethod) -> bool {
    matches!(method, AnomalyMethod::RandomCutForest | AnomalyMethod::Esd)
}

/// Just a naive heuristic to decide whether to spawn a background thread for anomaly detection
/// based on the number of samples and method complexity.
fn should_run_in_background(samples: usize, method: AnomalyMethod) -> bool {
    match is_cpu_intensive(method) {
        true => samples > SPAWN_THRESHOLD_SAMPLES,
        false => samples > 5_000, // For lightweight methods, we can allow more samples before spawning
    }
}

fn parse_method_options(
    method: AnomalyMethod,
    args: &mut CommandArgIterator,
) -> ValkeyResult<AnomalyOptions> {
    match method {
        AnomalyMethod::Ewma => parse_ewma_options(args),
        AnomalyMethod::Cusum => {
            // Cusum has no additional options
            Ok(AnomalyOptions {
                options: AnomalyDetectionMethodOptions::Cusum,
                ..Default::default()
            })
        }
        AnomalyMethod::ZScore => parse_zscore_options(args),
        AnomalyMethod::ModifiedZScore => parse_modified_zscore_options(args),
        AnomalyMethod::SmoothedZScore => parse_smoothed_zscore_options(args),
        AnomalyMethod::Mad => parse_mad_options(args),
        AnomalyMethod::DoubleMAD => parse_double_mad_options(args),
        AnomalyMethod::InterquartileRange => parse_iqr_options(args),
        AnomalyMethod::RandomCutForest => parse_rcf_options(args),
        AnomalyMethod::Esd => parse_esd_options(args),
    }
}

fn parse_seasonality(args: &mut CommandArgIterator) -> ValkeyResult<Seasonality> {
    if let Some(arg) = args.peek() {
        // check for auto
        if arg.as_slice().eq_ignore_ascii_case(b"auto") {
            args.next(); // consume AUTO
            return Ok(Seasonality::Auto);
        }
    }
    let mut periods: Vec<usize> = Vec::with_capacity(4);

    // loop while the next token is a number
    while let Some(v) = args.peek() {
        if let Ok(value) = v.parse_unsigned_integer() {
            periods.push(value as usize);
            args.next();
            continue;
        }
        break;
    }
    if periods.is_empty() || periods.len() > MAX_SEASONALITY_PERIODS {
        return Err(ValkeyError::Str("TSDB: invalid SEASONALITY periods"));
    }
    // periods should be unique and sorted
    periods.sort_unstable();
    if !periods.windows(2).all(|w| w[0] != w[1]) {
        return Err(ValkeyError::Str("TSDB: SEASONALITY periods must be unique"));
    }

    Ok(Seasonality::Periods(periods))
}

fn parse_output_format(arg: &str) -> ValkeyResult<OutputFormat> {
    match arg.len() {
        4 if arg.eq_ignore_ascii_case("full") => {
            return Ok(OutputFormat::Full);
        }
        6 if arg.eq_ignore_ascii_case("simple") => {
            return Ok(OutputFormat::Simple);
        }
        7 if arg.eq_ignore_ascii_case("cleaned") => {
            return Ok(OutputFormat::Cleaned);
        }
        _ => {}
    }
    Err(ValkeyError::String(format!(
        "TSDB: unknown output format option {arg}"
    )))
}

fn parse_ewma_options(args: &mut CommandArgIterator) -> ValkeyResult<AnomalyOptions> {
    // EWMA [ALPHA <value>]
    let alpha = if let Some(arg) = args.peek() {
        let slice = arg.as_slice();
        if slice.len() == 5 && slice.eq_ignore_ascii_case(b"alpha") {
            args.next(); // consume ALPHA
            Some(parse_single_value(args, "ALPHA")?)
        } else {
            None
        }
    } else {
        None
    };

    let mut options = AnomalyOptions {
        ..Default::default()
    };

    options.options = AnomalyDetectionMethodOptions::Ewma(alpha);

    Ok(options)
}

fn parse_modified_zscore_options(args: &mut CommandArgIterator) -> ValkeyResult<AnomalyOptions> {
    let threshold = parse_optional_threshold_option(args)?;

    let options = AnomalyOptions {
        options: AnomalyDetectionMethodOptions::ModifiedZScore(threshold),
        ..Default::default()
    };

    Ok(options)
}

#[inline]
fn is_command_option(arg: &[u8]) -> bool {
    parse_command_arg_token(arg).is_some_and(|token| COMMAND_OPTIONS.contains(&token))
}

fn parse_zscore_options(args: &mut CommandArgIterator) -> ValkeyResult<AnomalyOptions> {
    let mut threshold: Option<f64> = None;
    let mut influence: Option<f64> = None;
    let mut lag: Option<usize> = None;
    let mut zscore_type: Option<ZScoreType> = None;

    if let Some(arg) = args.peek() {
        zscore_type = hashify::tiny_map_ignore_case!(arg.as_slice(),
            "STANDARD" => ZScoreType::Standard,
            "MODIFIED" => ZScoreType::Modified,
            "SMOOTHED" => ZScoreType::Smoothed
        );
        if zscore_type.is_some() {
            args.next();
        }
    }

    while let Some(arg) = args.peek() {
        if is_command_option(arg.as_slice()) {
            break;
        }
        let arg = args.next().unwrap();
        let arg_slice = arg.as_slice();
        hashify::fnc_map_ignore_case!(arg_slice,
            "THRESHOLD" => {
                threshold = Some(parse_positive_value(args, "THRESHOLD")?);
            },
            "INFLUENCE" => {
                influence = Some(parse_positive_value(args, "INFLUENCE")?);
            },
            "LAG" => {
                lag = Some(parse_positive_value(args, "LAG")? as usize);
            },
            _ => {
                return Err(ValkeyError::String(format!("TSDB: unknown zscore option {arg}")));
            }
        );
    }

    if lag.is_some() || influence.is_some() {
        if zscore_type.is_none() {
            zscore_type = Some(ZScoreType::Smoothed);
        } else if zscore_type != Some(ZScoreType::Smoothed) {
            return Err(ValkeyError::String(
                "TSDB: LAG and INFLUENCE options only apply to smoothed z-score".to_string(),
            ));
        }
    }

    let zscore_type = zscore_type.unwrap_or(ZScoreType::Standard);
    let options = match zscore_type {
        ZScoreType::Standard => AnomalyDetectionMethodOptions::ZScore(threshold),
        ZScoreType::Modified => AnomalyDetectionMethodOptions::ModifiedZScore(threshold),
        ZScoreType::Smoothed => {
            let mut smoothed_options = SmoothedZScoreOptions::default();
            if let Some(threshold) = threshold {
                smoothed_options.threshold = threshold;
            }
            if let Some(influence) = influence {
                smoothed_options.influence = influence;
            }
            if let Some(lag) = lag {
                smoothed_options.lag = lag;
            }
            AnomalyDetectionMethodOptions::SmoothedZScore(smoothed_options)
        }
    };

    Ok(AnomalyOptions {
        options,
        ..Default::default()
    })
}

fn parse_smoothed_zscore_options(args: &mut CommandArgIterator) -> ValkeyResult<AnomalyOptions> {
    let mut smoothed_options = SmoothedZScoreOptions::default();

    while let Some(arg) = args.peek() {
        if is_command_option(arg.as_slice()) {
            break;
        }
        let arg = args.next().unwrap();
        let arg_slice = arg.as_slice();
        hashify::fnc_map_ignore_case!(arg_slice,
            "THRESHOLD" => {
                smoothed_options.threshold = parse_positive_value(args, "THRESHOLD")?;
            },
            "INFLUENCE" => {
                smoothed_options.influence = parse_positive_value(args, "INFLUENCE")?;
            },
            "LAG" => {
                smoothed_options.lag = parse_positive_value(args, "LAG")? as usize;
            },
            _ => {
                return Err(ValkeyError::String(format!("TSDB: unknown smoothed zscore option {arg}")));
            }
        );
    }

    Ok(AnomalyOptions {
        options: AnomalyDetectionMethodOptions::SmoothedZScore(smoothed_options),
        ..Default::default()
    })
}

// Mad [ESTIMATOR <mad-estimator>] [<value>], e.g., Mad ESTIMATOR HarrellDavis THRESHOLD 3.0
fn parse_mad_options(args: &mut CommandArgIterator) -> ValkeyResult<AnomalyOptions> {
    let mut mad_options = MADAnomalyOptions::default();
    while let Some(arg) = args.peek() {
        if is_command_option(arg.as_slice()) {
            break;
        }
        let arg = args.next().unwrap();
        let arg_slice = arg.as_slice();
        hashify::fnc_map_ignore_case!(arg_slice,
            "ESTIMATOR" => {
                 let estimator_arg = args
                    .next_str()
                    .map_err(|_| ValkeyError::Str("TSDB: Missing Mad estimator type"))?;
                 mad_options.estimator = estimator_arg.parse()?;
            },
            "THRESHOLD" => {
                 mad_options.k = parse_positive_value(args, "THRESHOLD")?;
            },
            _ => {
                 return Err(ValkeyError::String(format!("TSDB: unknown Mad option {arg}")));
            }
        );
    }

    Ok(AnomalyOptions {
        options: AnomalyDetectionMethodOptions::Mad(mad_options),
        ..Default::default()
    })
}

/// doubleMAD [ESTIMATOR <mad-estimator>] [THRESHOLD <value>]
/// e.g., doubleMAD ESTIMATOR HarrellDavis THRESHOLD 3.0
fn parse_double_mad_options(args: &mut CommandArgIterator) -> ValkeyResult<AnomalyOptions> {
    let mut double_mad_options = MADAnomalyOptions::default();

    while let Some(arg) = args.peek() {
        if is_command_option(arg.as_slice()) {
            break;
        }
        let arg = args.next().unwrap();
        let arg_slice = arg.as_slice();
        hashify::fnc_map_ignore_case!(arg_slice,
            "ESTIMATOR" => {
                let estimator_arg = args
                    .next_str()
                    .map_err(|_| ValkeyError::Str("TSDB: Missing Double Mad estimator type"))?;
                double_mad_options.estimator = estimator_arg.parse()?;
            },
            "THRESHOLD" => {
                double_mad_options.k = parse_positive_value(args, "THRESHOLD")?;
            },
            _ => {
                return Err(ValkeyError::String(format!("TSDB: unknown Double Mad option {arg}")));
            }
        );
    }

    Ok(AnomalyOptions {
        options: AnomalyDetectionMethodOptions::DoubleMAD(double_mad_options),
        ..Default::default()
    })
}

fn parse_iqr_options(args: &mut CommandArgIterator) -> ValkeyResult<AnomalyOptions> {
    let threshold = parse_optional_threshold_option(args)?;

    Ok(AnomalyOptions {
        options: AnomalyDetectionMethodOptions::InterQuartileRange(threshold),
        ..Default::default()
    })
}

fn parse_rcf_options(args: &mut CommandArgIterator) -> ValkeyResult<AnomalyOptions> {
    let mut rcf_options = RCFOptions::default();

    while let Some(arg) = args.peek() {
        if is_command_option(arg.as_slice()) {
            break;
        }
        let arg = args.next().unwrap();
        let arg_slice = arg.as_slice();
        hashify::fnc_map_ignore_case!(arg_slice,
            "NUM_TREES" => {
                rcf_options.num_trees = Some(parse_positive_value(args, "NUM_TREES")? as usize);
            },
            "SAMPLE_SIZE" => {
                rcf_options.sample_size = Some(parse_positive_value(args, "SAMPLE_SIZE")? as usize);
            },
            "THRESHOLD" => {
                let val = parse_positive_value(args, "THRESHOLD")?;
                rcf_options.threshold = Some(
                    RCFThreshold::std_dev(val)
                        .map_err(|e| ValkeyError::String(format!("TSDB: {e}")))?
                );
            },
            "CONTAMINATION" => {
                let val = parse_positive_value(args, "CONTAMINATION")?;
                rcf_options.threshold = Some(
                    RCFThreshold::contamination(val)
                        .map_err(|e| ValkeyError::String(format!("TSDB: {e}")))?
                );
            },
            "DECAY" => {
                rcf_options.time_decay = Some(parse_single_value(args, "DECAY")?);
            },
            "SHINGLE_SIZE" => {
                rcf_options.shingle_size = Some(parse_positive_value(args, "SHINGLE_SIZE")? as usize);
            },
            "OUTPUT_AFTER" => {
                rcf_options.output_after = Some(parse_positive_value(args, "OUTPUT_AFTER")? as usize);
            },
            _ => {
                return Err(ValkeyError::String(format!("TSDB: unknown RCF option {arg}")));
            }
        );
    }

    Ok(AnomalyOptions {
        options: AnomalyDetectionMethodOptions::Rcf(rcf_options),
        ..Default::default()
    })
}

fn parse_esd_options(args: &mut CommandArgIterator) -> ValkeyResult<AnomalyOptions> {
    let mut options = AnomalyOptions {
        ..Default::default()
    };

    let mut esd_options = ESDOutlierOptions::default();

    // Peek before consuming: a trailing command option (OUTPUT, DIRECTION, ...)
    // belongs to the caller's loop. Consuming it here and then breaking would
    // swallow the token and leave its argument to be parsed as a top-level one.
    while let Some(arg) = args.peek() {
        if is_command_option(arg.as_slice()) {
            break;
        }
        let arg = args.next().unwrap();
        let arg_slice = arg.as_slice();

        hashify::fnc_map_ignore_case!(arg_slice,
           "ALPHA" => {
                esd_options.alpha = parse_positive_value(args, "ALPHA")?;
            },
            "HYBRID" => {
                esd_options.estimator = EsdEstimator::Hybrid;
            },
            "CLASSIC" => {
                esd_options.estimator = EsdEstimator::Classic;
            },
            "MAX_OUTLIERS" => {
                esd_options.max_outliers = Some(parse_positive_value(args, "MAX_OUTLIERS")? as usize);
            },
            _ => {
                return Err(ValkeyError::String(format!("TSDB: unknown ESD option {arg}")));
            }
        );
    }

    options.options = AnomalyDetectionMethodOptions::Esd(Option::from(esd_options));

    Ok(options)
}

fn parse_optional_threshold_option(args: &mut CommandArgIterator) -> ValkeyResult<Option<f64>> {
    if let Some(arg) = args.peek()
        && arg.eq_ignore_ascii_case(b"threshold")
    {
        let _ = args.next();
        Ok(Some(parse_positive_value(args, "THRESHOLD")?))
    } else {
        Ok(None)
    }
}

fn parse_single_value(iter: &mut CommandArgIterator, option_name: &str) -> ValkeyResult<f64> {
    let Ok(value_str) = iter.next_str() else {
        return Err(ValkeyError::String(format!(
            "TSDB: Missing value for {option_name}"
        )));
    };

    value_str.parse().map_err(|_e| {
        ValkeyError::String(format!(
            "TSDB: invalid value for {option_name}: {value_str}"
        ))
    })
}

fn parse_positive_value(iter: &mut CommandArgIterator, option_name: &str) -> ValkeyResult<f64> {
    let value = parse_single_value(iter, option_name)?;
    if !value.is_finite() {
        return Err(ValkeyError::String(format!(
            "TSDB: {option_name} must be finite"
        )));
    }
    if value <= 0.0 {
        return Err(ValkeyError::String(format!(
            "TSDB: {option_name} must be positive"
        )));
    }
    Ok(value)
}

fn send_reply(
    ctx: &ReplyContext,
    result: AnomalyResult,
    samples: Vec<Sample>,
    direction: AnomalyDirection,
    output_format: OutputFormat,
    options: &AnomalyOptions,
) -> ValkeyResult {
    match output_format {
        OutputFormat::Full => reply_output_full(ctx, result, &samples, direction, options),
        OutputFormat::Simple => reply_simple(ctx, result, &samples, direction),
        OutputFormat::Cleaned => reply_cleaned(ctx, &result, &samples, direction),
    }
}

/// Returns anomalies only as a list of tuples (timestamp, value, anomaly_direction, anomaly score)
fn reply_simple(
    ctx: &ReplyContext,
    result: AnomalyResult,
    samples: &[Sample],
    direction: AnomalyDirection,
) -> ValkeyResult {
    reply_with_anomalies(ctx, &result, samples, direction);
    Ok(ValkeyValue::NoReply)
}

/// Returns only samples excluding anomalies in the specified direction.
fn reply_cleaned(
    ctx: &ReplyContext,
    result: &AnomalyResult,
    samples: &[Sample],
    direction: AnomalyDirection,
) -> ValkeyResult {
    reply_with_cleaned_samples(ctx, samples, &result.anomalies, direction);
    Ok(ValkeyValue::NoReply)
}

fn reply_with_cleaned_samples(
    ctx: &ReplyContext,
    samples: &[Sample],
    outliers: &[Anomaly],
    direction: AnomalyDirection,
) {
    if outliers.is_empty() {
        return reply_with_samples(ctx, samples);
    }

    let excluded_indices: IntSet<usize> = outliers
        .iter()
        .filter(|anomaly| anomaly.signal.matches_direction(direction))
        .map(|anomaly| anomaly.index)
        .collect();

    ctx.reply_with_postponed_array();
    let mut count: usize = 0;

    for sample in samples.iter().enumerate().filter_map(|(idx, sample)| {
        if excluded_indices.contains(&idx) {
            None
        } else {
            Some(sample)
        }
    }) {
        reply_with_sample(ctx, sample);
        count += 1;
    }

    ctx.reply_with_array_len(count);
}

/// Returns anomalies only as a list of tuples (timestamp, value, anomaly_direction, score)
fn reply_with_anomalies(
    ctx: &ReplyContext,
    result: &AnomalyResult,
    samples: &[Sample],
    direction: AnomalyDirection,
) {
    ctx.reply_with_postponed_array();
    let mut count: usize = 0;
    // Collect only the anomalies
    for (outlier, sample) in result.anomalies.iter().filter_map(|outlier| {
        if !outlier.signal.matches_direction(direction) {
            return None;
        }
        samples.get(outlier.index).map(|sample| (outlier, sample))
    }) {
        reply_with_outlier(ctx, outlier, sample);
        count += 1;
    }

    ctx.reply_with_array_len(count);
}

fn reply_output_full(
    ctx: &ReplyContext,
    result: AnomalyResult,
    samples: &[Sample],
    direction: AnomalyDirection,
    options: &AnomalyOptions,
) -> ValkeyResult {
    let mut map_len = 4; // method, direction, samples, parameters
    if result.method_info.is_some() {
        map_len += 1;
    }
    if options.seasonality.is_some() {
        map_len += 1;
    }

    ctx.reply_with_map(map_len);

    // Add method info
    ctx.reply_with_string("method");
    ctx.reply_with_string(result.method.short_name());
    ctx.reply_with_string("direction");
    ctx.reply_with_string(direction.name());

    // Add samples with scores and signal
    ctx.reply_with_string("samples");
    reply_with_samples_scores_and_signals(
        ctx,
        samples,
        &result.anomalies,
        &result.scores,
        direction,
    );

    // Add parameters
    ctx.reply_with_string("parameters");
    reply_with_parameters(ctx, &options.options, result.threshold);

    // Add seasonality info if available
    if let Some(seasonality) = &options.seasonality {
        ctx.reply_with_string("seasonality");
        match seasonality {
            Seasonality::Auto => {
                ctx.reply_with_map(1);
                ctx.reply_with_string("method");
                ctx.reply_with_string("auto");
            }
            Seasonality::Periods(periods) => {
                ctx.reply_with_map(1);
                ctx.reply_with_string("periods");
                ctx.reply_with_array(periods.len());
                for p in periods {
                    ctx.reply_with_integer(*p as i64);
                }
            }
        }
    }

    // Add method-specific info if available
    if let Some(method_info) = result.method_info {
        ctx.reply_with_string("method_info");
        match method_info {
            MethodInfo::Fenced {
                lower_fence,
                upper_fence,
                center_line,
            } => {
                let mut map_len: usize = 2;
                if center_line.is_some() {
                    map_len += 1;
                }
                ctx.reply_with_map(map_len);
                ctx.reply_with_string("lower_fence");
                ctx.reply_with_double(lower_fence);
                ctx.reply_with_string("upper_fence");
                ctx.reply_with_double(upper_fence);
                if let Some(center) = center_line {
                    ctx.reply_with_string("center_line");
                    ctx.reply_with_double(center);
                }
            }
            MethodInfo::Spc {
                control_limits,
                center_line,
            } => {
                ctx.reply_with_map(2);

                ctx.reply_with_string("control_limits");
                ctx.reply_with_array(2);
                ctx.reply_with_double(control_limits.0);
                ctx.reply_with_double(control_limits.1);

                ctx.reply_with_string("center_line");
                ctx.reply_with_double(center_line);
            }
        }
    }

    Ok(ValkeyValue::NoReply)
}

fn reply_with_samples(ctx: &ReplyContext, samples: &[Sample]) {
    ctx.reply_with_array(samples.len());
    for sample in samples {
        reply_with_sample(ctx, sample);
    }
}

fn reply_with_samples_scores_and_signals(
    ctx: &ReplyContext,
    samples: &[Sample],
    anomalies: &[Anomaly],
    scores: &[f64],
    direction: AnomalyDirection,
) {
    if anomalies.is_empty() {
        ctx.reply_with_array(samples.len());
        for (sample, &score) in samples.iter().zip(scores.iter()) {
            ctx.reply_with_array(4);
            ctx.reply_with_integer(sample.timestamp);
            ctx.reply_with_double(sample.value);
            ctx.reply_with_double(score);
            ctx.reply_with_integer(0);
        }
        return;
    }

    let map: IntMap<usize, &Anomaly> = anomalies
        .iter()
        .map(|anomaly| (anomaly.index, anomaly))
        .collect();

    ctx.reply_with_array(samples.len());

    for (idx, (sample, &score)) in samples.iter().zip(scores.iter()).enumerate() {
        let mut signal = 0;

        if let Some(anomaly) = map
            .get(&idx)
            .filter(|a| a.signal.matches_direction(direction))
        {
            signal = anomaly.signal as i64;
        }

        ctx.reply_with_array(4);
        ctx.reply_with_integer(sample.timestamp);
        ctx.reply_with_double(sample.value);
        ctx.reply_with_double(score);
        ctx.reply_with_integer(signal);
    }
}

fn reply_with_outlier(ctx: &ReplyContext, outlier: &Anomaly, sample: &Sample) {
    ctx.reply_with_array(4);
    ctx.reply_with_integer(sample.timestamp);
    ctx.reply_with_double(sample.value);
    ctx.reply_with_integer(outlier.signal as i64);
    ctx.reply_with_double(outlier.score);
}

fn reply_with_parameters(
    ctx: &ReplyContext,
    options: &AnomalyDetectionMethodOptions,
    threshold: f64,
) {
    match options {
        AnomalyDetectionMethodOptions::Ewma(alpha) => {
            ctx.reply_with_map(1);
            ctx.reply_with_string("alpha");
            ctx.reply_with_double(alpha.unwrap_or(EWMA_DEFAULT_ALPHA));
        }
        AnomalyDetectionMethodOptions::ZScore(_)
        | AnomalyDetectionMethodOptions::ModifiedZScore(_)
        | AnomalyDetectionMethodOptions::InterQuartileRange(_) => {
            ctx.reply_with_map(1);
            ctx.reply_with_string("threshold");
            ctx.reply_with_double(threshold);
        }
        AnomalyDetectionMethodOptions::SmoothedZScore(opts) => {
            ctx.reply_with_map(3);
            ctx.reply_with_string("threshold");
            ctx.reply_with_double(opts.threshold);
            ctx.reply_with_string("lag");
            ctx.reply_with_integer(opts.lag as i64);
            ctx.reply_with_string("influence");
            ctx.reply_with_double(opts.influence);
        }
        AnomalyDetectionMethodOptions::Mad(opts)
        | AnomalyDetectionMethodOptions::DoubleMAD(opts) => {
            ctx.reply_with_map(2);
            ctx.reply_with_string("threshold");
            ctx.reply_with_double(opts.k);
            ctx.reply_with_string("estimator");
            ctx.reply_with_string(opts.estimator.alias());
        }
        AnomalyDetectionMethodOptions::Rcf(opts) => {
            let mut map_len = 3; // num_trees, sample_size, threshold
            if opts.time_decay.is_some() {
                map_len += 1;
            }
            if opts.shingle_size.is_some() {
                map_len += 1;
            }
            if opts.output_after.is_some() {
                map_len += 1;
            }
            ctx.reply_with_map(map_len);

            ctx.reply_with_string("num_trees");
            ctx.reply_with_integer(opts.num_trees.unwrap_or(RCF_DEFAULT_NUM_TREES) as i64);
            ctx.reply_with_string("sample_size");
            ctx.reply_with_integer(opts.sample_size.unwrap_or(RCF_DEFAULT_SAMPLE_SIZE) as i64);

            if let Some(threshold_opt) = &opts.threshold {
                match threshold_opt {
                    RCFThreshold::Contamination(c) => {
                        ctx.reply_with_string("contamination");
                        ctx.reply_with_double(*c);
                    }
                    RCFThreshold::StdDev(s) => {
                        ctx.reply_with_string("threshold");
                        ctx.reply_with_double(*s);
                    }
                }
            } else {
                // Always include threshold, even if default
                ctx.reply_with_string("threshold");
                ctx.reply_with_double(threshold);
            }

            if let Some(decay) = opts.time_decay {
                ctx.reply_with_string("decay");
                ctx.reply_with_double(decay);
            }
            if let Some(shingle) = opts.shingle_size {
                ctx.reply_with_string("shingle_size");
                ctx.reply_with_integer(shingle as i64);
            }
            if let Some(warmup) = opts.output_after {
                ctx.reply_with_string("output_after");
                ctx.reply_with_integer(warmup as i64);
            }
        }
        AnomalyDetectionMethodOptions::Esd(opts) => {
            let o = opts.clone().unwrap_or_default();
            let mut map_len = 2; // alpha, hybrid
            if o.max_outliers.is_some() {
                map_len += 1;
            }
            ctx.reply_with_map(map_len);
            ctx.reply_with_string("alpha");
            ctx.reply_with_double(o.alpha);
            ctx.reply_with_string("hybrid");
            ctx.reply_with_bool(o.estimator.is_hybrid());
            if let Some(max) = o.max_outliers {
                ctx.reply_with_string("max_outliers");
                ctx.reply_with_integer(max as i64);
            }
        }
        AnomalyDetectionMethodOptions::Cusum => {
            ctx.reply_with_map(0);
        }
    }
}
