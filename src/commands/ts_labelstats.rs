use crate::commands::parse_stats_command_args;
use crate::commands::ts_labelstats_fanout_command::LabelStatsFanoutCommand;
use crate::common::replies::ReplyContext;
use crate::fanout::{FanoutClientCommand, is_clustered};
use crate::series::index::{PostingStat, PostingsStats, get_timeseries_index};
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

/// https://prometheus.io/docs/prometheus/latest/querying/api/#tsdb-stats
#[valkey_module_macros::command({
    name: "ts.labelstats",
    flags: [ReadOnly],
    summary: "Return cardinality statistics about labels and metric names.",
    complexity: "O(N) where N is the number of indexed label-value pairs.",
    since: "1.0.0",
    arity: -1,
    key_spec: []
})]
pub fn ts_labelstats_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() > 5 {
        return Err(ValkeyError::WrongArity);
    }

    let mut args = args.into_iter().skip(1).peekable();
    let (label, limit) = parse_stats_command_args(&mut args)?;

    if is_clustered(ctx) {
        let operation = LabelStatsFanoutCommand::new(limit, label);
        return operation.exec(ctx);
    }

    let index = get_timeseries_index(ctx);
    let selected_label = label.as_deref().unwrap_or("");
    let stats = index.stats(selected_label, limit);
    let reply_ctx = ReplyContext::new(ctx.ctx);

    reply_with_postings_stats(&reply_ctx, &stats);
    Ok(ValkeyValue::NoReply)
}

pub(super) fn reply_with_postings_stats(ctx: &ReplyContext, stats: &PostingsStats) {
    let map_len = 6 + if stats.series_count_by_focus_label_value.is_some() {
        1
    } else {
        0
    };
    ctx.reply_with_map(map_len);

    ctx.reply_with_string("totalSeries");
    ctx.reply_with_integer(stats.series_count as i64);

    ctx.reply_with_string("totalLabels");
    ctx.reply_with_integer(stats.label_count as i64);

    ctx.reply_with_string("totalLabelValuePairs");
    ctx.reply_with_integer(stats.total_label_value_pairs as i64);

    ctx.reply_with_string("seriesCountByMetricName");
    reply_with_stats_slice(ctx, &stats.series_count_by_metric_name);
    ctx.reply_with_string("labelValueCountByLabelName");
    reply_with_stats_slice(ctx, &stats.series_count_by_label_name);
    if let Some(series_count_by_focus_label_value) = &stats.series_count_by_focus_label_value {
        ctx.reply_with_string("seriesCountByFocusLabelValue");
        reply_with_stats_slice(ctx, series_count_by_focus_label_value);
    }
    ctx.reply_with_string("seriesCountByLabelValuePair");
    reply_with_stats_slice(ctx, &stats.series_count_by_label_value_pairs);
}

fn reply_with_stats_slice(ctx: &ReplyContext, items: &[PostingStat]) {
    ctx.reply_with_array(items.len());
    for item in items {
        reply_with_posting_stat(ctx, item);
    }
}

fn reply_with_posting_stat(ctx: &ReplyContext, stat: &PostingStat) {
    ctx.reply_with_map(1);
    ctx.reply_with_string(&stat.name);
    ctx.reply_with_integer(stat.count as i64);
}
