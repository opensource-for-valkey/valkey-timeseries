use crate::commands::command_parser::{
    CommandArgToken, parse_chunk_compression, parse_chunk_size, parse_command_arg_token,
    parse_decimal_digit_rounding, parse_duplicate_policy, parse_ignore_options, parse_metric_name,
    parse_retention, parse_significant_digit_rounding,
};
use crate::error_consts;
use crate::labels::Label;
use crate::series::{DuplicatePolicy, TimeSeriesOptions, create_and_store_series};
use valkey_module::{Context, NextArg, VALKEY_OK, ValkeyError, ValkeyResult, ValkeyString};

/// Create a new time series
///
/// TS.CREATE key
///   [METRIC metric]
///   [RETENTION retentionPeriod]
///   [ENCODING <gorilla|chimp|uncompressed|compressed>]
///   [CHUNK_SIZE chunkSize]
///   [DUPLICATE_POLICY duplicatePolicy]
///   [SIGNIFICANT_DIGITS significantDigits | DECIMAL_DIGITS decimalDigits]
///   [IGNORE ignoreMaxTimediff ignoreMaxValDiff]
///   [LABELS label1=value1 label2=value2 ...]
#[valkey_module_macros::command({
    name: "TS.CREATE",
    flags: [Write, DenyOOM],
    summary: "Create a new time series.",
    complexity: "O(1)",
    since: "1.0.0",
    arity: -2,
    key_spec: [{
        flags: [ReadWrite, Insert],
        begin_search: Index({ index: 1 }),
        find_keys: Range({ last_key: 0, steps: 1, limit: 0 })
    }]
})]
pub fn ts_create_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let (parsed_key, options) = parse_create_options(args)?;

    let _ = create_and_store_series(ctx, &parsed_key, options, true, false)?;

    VALKEY_OK
}

pub fn parse_create_options(
    args: Vec<ValkeyString>,
) -> ValkeyResult<(ValkeyString, TimeSeriesOptions)> {
    if args.len() < 2 {
        return Err(ValkeyError::WrongArity);
    }

    let mut args = args;
    let key = args.remove(1);

    let options = parse_series_options(args, 1, &[CommandArgToken::OnDuplicate])?;

    // if options.labels.is_empty() {
    //     return Err(ValkeyError::Str(
    //         error_consts::INVALID_OR_MISSING_METRIC_NAME,
    //     ));
    // }

    Ok((key, options))
}

pub fn parse_series_options(
    args: Vec<ValkeyString>,
    args_to_skip: usize,
    invalid_args: &[CommandArgToken],
) -> ValkeyResult<TimeSeriesOptions> {
    let mut metric_set = false;

    let mut options = TimeSeriesOptions::from_config();

    // Labels are variadic, so we handle them first to make parsing easier.
    let pos = args.iter().rposition(|x| x.eq_ignore_ascii_case(b"labels"));

    // Extract and process labels if they exist
    let args = if let Some(pos) = pos {
        let mut args_inner = args;
        let label_section = args_inner.split_off(pos);
        // Skip the "LABELS" token itself and parse the remaining elements
        if label_section.len() > 1 {
            options.labels = Some(parse_labels(&label_section[1..])?);
        } else {
            options.labels = Some(Vec::new()); // we explicitly set it to empty if no labels are provided
        }
        metric_set = true;
        args_inner
    } else {
        args
    };

    // Process the remaining arguments (skipping the key)
    let mut args_iter = args.into_iter().skip(args_to_skip).peekable();

    while let Some(arg) = args_iter.next() {
        let token = parse_command_arg_token(arg.as_slice()).unwrap_or_default();
        if invalid_args.contains(&token) {
            return Err(ValkeyError::Str(error_consts::INVALID_ARGUMENT));
        }

        match token {
            CommandArgToken::ChunkSize => {
                let arg = args_iter
                    .next_str()
                    .map_err(|_| ValkeyError::Str(error_consts::CANNOT_PARSE_CHUNK_SIZE))?;
                options.chunk_size = Some(parse_chunk_size(arg)?)
            }
            CommandArgToken::Encoding => {
                options.chunk_encoding = parse_chunk_compression(&mut args_iter)?;
            }
            CommandArgToken::DecimalDigits => {
                if options.rounding.is_some() {
                    return Err(ValkeyError::Str(error_consts::ROUNDING_ALREADY_SET));
                }
                let rounding = parse_decimal_digit_rounding(&mut args_iter)?;
                options.rounding = Some(rounding);
            }
            CommandArgToken::DuplicatePolicy => {
                let Some(arg) = args_iter.next() else {
                    return Err(ValkeyError::Str(error_consts::MISSING_DUPLICATE_POLICY));
                };
                let policy: DuplicatePolicy = DuplicatePolicy::try_from(arg.as_slice())?;
                let mut ignore_options = options.sample_duplicate_policy.unwrap_or_default();
                ignore_options.policy = Some(policy);
                options.sample_duplicate_policy = Some(ignore_options);
            }
            CommandArgToken::OnDuplicate => {
                options.on_duplicate = Some(parse_duplicate_policy(&mut args_iter)?);
            }
            CommandArgToken::Metric => {
                if metric_set {
                    return Err(ValkeyError::Str(error_consts::METRIC_ALREADY_SET));
                }
                let metric = args_iter.next_string()?;
                options.labels = Some(parse_metric_name(&metric)?);
            }
            CommandArgToken::Ignore => {
                let (ignore_max_timediff, ignore_max_val_diff) =
                    parse_ignore_options(&mut args_iter)?;
                let mut ignore_options = options.sample_duplicate_policy.unwrap_or_default();
                ignore_options.max_time_delta = ignore_max_timediff as u64;
                ignore_options.max_value_delta = ignore_max_val_diff;
                options.sample_duplicate_policy = Some(ignore_options);
            }
            CommandArgToken::Retention => options.retention(parse_retention(&mut args_iter)?),
            CommandArgToken::SignificantDigits => {
                if options.rounding.is_some() {
                    return Err(ValkeyError::Str(error_consts::ROUNDING_ALREADY_SET));
                }
                options.rounding = Some(parse_significant_digit_rounding(&mut args_iter)?);
            }
            _ => {
                return Err(ValkeyError::Str(error_consts::INVALID_ARGUMENT));
            }
        };
    }

    Ok(options)
}

/// Parse labels from the command arguments. It's variadic, so it should be the last argument
/// in the command, otherwise we end in ambiguity
fn parse_labels(args: &[ValkeyString]) -> ValkeyResult<Vec<Label>> {
    if args.is_empty() {
        return Ok(Vec::new());
    }
    if !args.len().is_multiple_of(2) {
        return Err(ValkeyError::Str(error_consts::CANNOT_PARSE_LABELS));
    }

    let mut labels = Vec::with_capacity(args.len() / 2);

    for arg in args.chunks(2) {
        let name = &arg[0];
        let value = &arg[1];

        if name.is_empty() || value.is_empty() {
            return Err(ValkeyError::Str(error_consts::DUPLICATE_LABEL));
        }

        let label = Label::new(name.to_string_lossy(), value.to_string_lossy());
        labels.push(label);
    }

    Ok(labels)
}
