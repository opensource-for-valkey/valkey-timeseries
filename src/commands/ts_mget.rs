use super::ts_mget_fanout_command::MGetFanoutCommand;
use crate::commands::command_parser::CommandArgToken;
use crate::commands::fanout_codec::MGetValue;
use crate::commands::utils::reply_with_mget_values;
use crate::commands::{parse_command_arg_token, parse_label_list, parse_series_selector_list};
use crate::error_consts;
use crate::fanout::{FanoutClientCommand, is_clustered};
use crate::labels::Label;
use crate::series::index::with_matched_series;
use crate::series::request_types::{MGetRequest, MGetSeriesData, MatchFilterOptions};
use crate::series::{get_latest_compaction_sample, get_series_labels};
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString};

/// TS.MGET
///   [LATEST]
///   [WITHLABELS | SELECTED_LABELS label...]
///   [FILTER filterExpr...]
#[valkey_module_macros::command({
    name: "ts.mget",
    flags: [ReadOnly, Fast],
    summary: "Get the last sample of each time series matching a filter.",
    complexity: "O(N) where N is the number of time series that match the filters.",
    since: "1.0.0",
    arity: -2,
    key_spec: []
})]
pub fn ts_mget_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 2 {
        return Err(ValkeyError::WrongArity);
    }

    let options = parse_mget_options(args)?;

    if is_clustered(ctx) {
        let operation = MGetFanoutCommand::new(options);
        return operation.exec(ctx);
    }

    let mget_results = process_mget_request(ctx, options)?;
    let mget_results = mget_results
        .into_iter()
        .map(Into::into)
        .collect::<Vec<MGetValue>>();

    reply_with_mget_values(ctx, &mget_results)
}

/// Parsing commands with variadic args gets wonky. For example, if we have something like:
///
/// `TS.MGET SELECTED_LABELS label1 label2 FILTER a=b x=y`
///
/// It's not clear if FILTER a=b and x=y are to be parsed as part of SELECTED_LABELS.
/// To avoid this, we need to check backwards for variadic command arguments and remove them
/// before handling the rest of the arguments.
pub fn parse_mget_options(args: Vec<ValkeyString>) -> ValkeyResult<MGetRequest> {
    let supported_tokens = &[
        CommandArgToken::SelectedLabels,
        CommandArgToken::Filter,
        CommandArgToken::Latest,
        CommandArgToken::WithLabels,
    ];

    let mut options = MGetRequest::default();

    let Some(filter_index) = args
        .iter()
        .rposition(|arg| arg.as_slice().eq_ignore_ascii_case(b"FILTER"))
    else {
        return Err(ValkeyError::WrongArity);
    };

    let mut args = args;
    let filter_args = args.split_off(filter_index);

    if filter_args.len() < 2 {
        return Err(ValkeyError::WrongArity);
    }

    let mut args = args.into_iter().skip(1).peekable(); // Skip the command name

    while let Some(arg) = args.next() {
        let token = parse_command_arg_token(arg.as_slice())
            .ok_or(ValkeyError::Str(error_consts::INVALID_ARGUMENT))?;

        match token {
            CommandArgToken::SelectedLabels => {
                options.selected_labels = parse_label_list(&mut args, supported_tokens)?;
                if options.selected_labels.is_empty() {
                    return Err(ValkeyError::Str(
                        "TSDB: SELECT_LABELS should have at least 1 parameter",
                    ));
                }
            }
            CommandArgToken::WithLabels => {
                options.with_labels = true;
            }
            CommandArgToken::Latest => {
                options.latest = true;
            }
            CommandArgToken::Filter => {
                return Err(ValkeyError::Str("TSDB: FILTER must be the last argument"));
            }
            _ => return Err(ValkeyError::Str(error_consts::INVALID_ARGUMENT)),
        }
    }

    if options.with_labels && !options.selected_labels.is_empty() {
        return Err(ValkeyError::Str(
            error_consts::WITH_LABELS_AND_SELECTED_LABELS_SPECIFIED,
        ));
    }

    let mut args = filter_args.into_iter().skip(1).peekable();
    options.filters = parse_series_selector_list(&mut args, &[])?;

    args.done()?;

    if options.filters.is_empty() {
        return Err(ValkeyError::Str(error_consts::MISSING_FILTER));
    }

    if !options.selected_labels.is_empty() && options.with_labels {
        return Err(ValkeyError::Str(
            error_consts::WITH_LABELS_AND_SELECTED_LABELS_SPECIFIED,
        ));
    }

    Ok(options)
}

pub fn process_mget_request(
    ctx: &Context,
    options: MGetRequest,
) -> ValkeyResult<Vec<MGetSeriesData>> {
    let with_labels = options.with_labels;
    let selected_labels = &options.selected_labels;
    let mut series = Vec::with_capacity(8);

    let opts: MatchFilterOptions = options.filters.into();

    with_matched_series(ctx, &mut series, &opts, move |acc, series, series_key| {
        let sample = if options.latest {
            get_latest_compaction_sample(ctx, series).or(series.last_sample)
        } else {
            series.last_sample
        };
        // SELECTED_LABELS entries are positionally aligned with the request:
        // a label missing from the series keeps its requested name with an
        // empty value, so the reply can render [name, nil] like the reference.
        let series_labels = get_series_labels(series, with_labels, selected_labels);
        let labels = if selected_labels.is_empty() {
            series_labels
                .into_iter()
                .map(|label| label.map(|x| Label::new(x.name, x.value)))
                .collect()
        } else {
            series_labels
                .into_iter()
                .zip(selected_labels.iter())
                .map(|(label, requested)| {
                    Some(label.map_or_else(
                        || Label::new(requested.as_str(), ""),
                        |x| Label::new(x.name, x.value),
                    ))
                })
                .collect()
        };

        acc.push(MGetSeriesData {
            sample,
            labels,
            series_key,
        });
    })?;

    Ok(series)
}
