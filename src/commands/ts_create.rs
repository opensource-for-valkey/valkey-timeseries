use crate::commands::command_parser::{
    CommandArgToken, parse_chunk_compression, parse_chunk_size, parse_command_arg_token,
    parse_decimal_digit_rounding, parse_duplicate_policy, parse_ignore_options, parse_metric_name,
    parse_retention, parse_significant_digit_rounding,
};
use crate::error_consts;
use crate::labels::Label;
use crate::series::chunks::ChunkEncoding;
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
    name: "ts.create",
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
    parse_series_options_onto(
        TimeSeriesOptions::from_config(),
        args,
        args_to_skip,
        invalid_args,
    )
}

/// Parse series options onto `base`.
///
/// Creating a series starts from the module configuration, so every unset option
/// lands on its configured default. Altering one starts from
/// [`TimeSeriesOptions::empty`] instead: `TS.ALTER` must leave a property it was
/// not given alone, which it can only tell apart if the parser leaves it `None`.
pub fn parse_series_options_onto(
    base: TimeSeriesOptions,
    args: Vec<ValkeyString>,
    args_to_skip: usize,
    invalid_args: &[CommandArgToken],
) -> ValkeyResult<TimeSeriesOptions> {
    let mut metric_set = false;

    let mut options = base;

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

    // RedisTimeSeries resolves each option from its *first* occurrence and never
    // looks at a later one — `RETENTION 100 RETENTION bogus` is accepted and
    // keeps 100. Repeats of the shared options are therefore consumed (operands
    // included, unvalidated) and discarded rather than overwriting the value.
    let mut seen: Vec<CommandArgToken> = Vec::new();

    while let Some(arg) = args_iter.next() {
        let token = parse_command_arg_token(arg.as_slice()).unwrap_or_default();
        if invalid_args.contains(&token) {
            return Err(ValkeyError::Str(error_consts::INVALID_ARGUMENT));
        }

        if let Some(operands) = first_occurrence_wins_operands(token) {
            if seen.contains(&token) {
                for _ in 0..operands {
                    if args_iter.next().is_none() {
                        break;
                    }
                }
                continue;
            }
            seen.push(token);
        }

        // Encoding has two spellings and they are not symmetric on RTS: an
        // explicit `ENCODING <v>` wins over a bare COMPRESSED/UNCOMPRESSED keyword
        // wherever the two appear relative to each other, while two bare keywords
        // resolve first-wins like every other option.
        if matches!(
            token,
            CommandArgToken::Compressed | CommandArgToken::Uncompressed
        ) {
            if seen.contains(&CommandArgToken::Encoding)
                || seen.contains(&CommandArgToken::Compressed)
            {
                continue;
            }
            seen.push(CommandArgToken::Compressed);
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
            // RTS still accepts the pre-ENCODING spelling — a bare COMPRESSED or
            // UNCOMPRESSED keyword with no operand — on TS.CREATE, TS.ADD and the
            // counter commands.
            CommandArgToken::Compressed => {
                options.chunk_encoding = ChunkEncoding::default();
            }
            CommandArgToken::Uncompressed => {
                options.chunk_encoding = ChunkEncoding::Uncompressed;
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
                options.duplicate_policy = Some(policy);
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
                options.ignore = Some((ignore_max_timediff as u64, ignore_max_val_diff));
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

/// Operand count for the options RedisTimeSeries resolves first-occurrence-wins,
/// or `None` for options outside that shared surface — the Valkey-TimeSeries-only
/// ones keep their own already-set diagnostics. The bare COMPRESSED/UNCOMPRESSED
/// keywords are handled separately (see the call site).
fn first_occurrence_wins_operands(token: CommandArgToken) -> Option<usize> {
    match token {
        CommandArgToken::Retention
        | CommandArgToken::Encoding
        | CommandArgToken::ChunkSize
        | CommandArgToken::DuplicatePolicy
        | CommandArgToken::OnDuplicate => Some(1),
        CommandArgToken::Ignore => Some(2),
        _ => None,
    }
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
            // An empty label name or value is a LABELS parse failure, not a
            // duplicate — and "Couldn't parse LABELS" is the RTS text for it.
            return Err(ValkeyError::Str(error_consts::CANNOT_PARSE_LABELS));
        }

        let label = Label::new(name.to_string_lossy(), value.to_string_lossy());
        labels.push(label);
    }

    Ok(labels)
}
