use super::ts_mget_fanout_command::MGetFanoutCommand;
use crate::commands::command_parser::{CommandArgToken, parse_hash_tags};
use crate::commands::fanout_codec::MGetValue;
use crate::commands::utils::reply_with_mget_values;
use crate::commands::{parse_command_arg_token, parse_label_list, parse_series_selector_list};
use crate::error_consts;
use crate::fanout::{FanoutClientCommand, is_clustered};
use crate::labels::Label;
use crate::series::index::with_matched_series;
use crate::series::request_types::{MGetRequest, MGetSeriesData, MatchFilterOptions};
use crate::series::{get_latest_compaction_sample, get_series_labels};
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString};

acl_categories!(TS_MGET, "ts.mget", "fast read timeseries");
/// TS.MGET
///   [LATEST]
///   [WITHLABELS | SELECTED_LABELS label...]
///   [HASHTAG hash_tag,...]
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

/// Parses `TS.MGET [LATEST] [WITHLABELS | SELECTED_LABELS label...] [HASHTAG tags] FILTER
/// filterExpr...`, walking the options in order.
///
/// The variadic lists (SELECTED_LABELS, FILTER) end at the next option token, so options may
/// also follow the FILTER list (DIV-0056, matching TS.MRANGE's DIV-0050). This used to take
/// everything after the last FILTER as filter expressions; in this dialect a bare word is a
/// metric-name selector, so `FILTER z=1 WITHLABELS` parsed as `__name__="WITHLABELS"` and
/// replied with an empty array — a silently wrong answer where the reference errors.
pub fn parse_mget_options(args: Vec<ValkeyString>) -> ValkeyResult<MGetRequest> {
    const OPTION_TOKENS: &[CommandArgToken] = &[
        CommandArgToken::SelectedLabels,
        CommandArgToken::Filter,
        CommandArgToken::Latest,
        CommandArgToken::WithLabels,
        CommandArgToken::HashTag,
    ];

    let mut options = MGetRequest::default();
    let mut filter_seen = false;

    let mut args = args.into_iter().skip(1).peekable(); // Skip the command name

    while let Some(arg) = args.next() {
        let token = parse_command_arg_token(arg.as_slice())
            .ok_or(ValkeyError::Str(error_consts::INVALID_ARGUMENT))?;

        match token {
            CommandArgToken::SelectedLabels => {
                options.selected_labels = parse_label_list(&mut args, OPTION_TOKENS)?;
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
            CommandArgToken::HashTag => {
                // An option token is not a tag value: `HASHTAG FILTER ...` is a missing value.
                if args.peek().is_some_and(|next| {
                    parse_command_arg_token(next.as_slice())
                        .is_some_and(|t| OPTION_TOKENS.contains(&t))
                }) {
                    return Err(ValkeyError::Str(error_consts::MISSING_HASHTAG));
                }
                options.tags = parse_hash_tags(&mut args)?;
            }
            CommandArgToken::Filter => {
                // The reference rejects a second FILTER too.
                if filter_seen {
                    return Err(ValkeyError::Str("TSDB: FILTER specified more than once"));
                }
                filter_seen = true;
                // An empty list is an arity error, as the reference reports it (and as a bare
                // trailing `FILTER` always was here), not a selector-parse error.
                let list_is_empty = args.peek().is_none_or(|next| {
                    parse_command_arg_token(next.as_slice())
                        .is_some_and(|t| OPTION_TOKENS.contains(&t))
                });
                if list_is_empty {
                    return Err(ValkeyError::WrongArity);
                }
                options.filters = parse_series_selector_list(&mut args, OPTION_TOKENS)?;
            }
            _ => return Err(ValkeyError::Str(error_consts::INVALID_ARGUMENT)),
        }
    }

    if !filter_seen {
        return Err(ValkeyError::WrongArity);
    }

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
            series.reported_last_sample()
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
