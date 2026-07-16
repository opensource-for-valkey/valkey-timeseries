use crate::aggregators::{AggregationType, Aggregator};
use crate::commands::command_parser::{parse_inline_condition, split_aggregator_condition};
use crate::commands::{CommandArgIterator, parse_duration};
use crate::error_consts;
use crate::parser::timestamp::parse_timestamp;
use crate::series::request_types::AggregatorConfig;
use crate::series::{
    CompactionRule, SeriesRef, check_new_rule_circular_dependency, get_timeseries_mut,
};
use valkey_module::{
    AclPermissions, Context, NextArg, NotifyEvent, VALKEY_OK, ValkeyError, ValkeyResult,
    ValkeyString,
};

///
/// TS.CREATERULE sourceKey destKey AGGREGATION aggregator bucketDuration [alignTimestamp]
///
/// Creates a compaction rule from sourceKey to destKey with a specific aggregator and bucket duration.
/// sourceKey must be different from destKey, and the user must be authorized to read from sourceKey and write to destKey.
///
#[valkey_module_macros::command({
    name: "ts.createrule",
    flags: [Write, DenyOOM],
    summary: "Create a compaction rule from a source time series to a destination series.",
    complexity: "O(1)",
    since: "1.0.0",
    arity: -6,
    key_spec: [{
        flags: [ReadWrite, Update],
        begin_search: Index({ index: 1 }),
        find_keys: Range({ last_key: 1, steps: 1, limit: 0 })
    }]
})]
pub fn ts_createrule_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    // Check for minimum number of arguments: command, sourceKey, destKey, AGGREGATION, aggregator, bucketDuration
    if args.len() < 6 {
        return Err(ValkeyError::WrongArity);
    }
    let mut args = args.into_iter().skip(1).peekable();

    // expect is fine here because we already checked the length above
    let source_key = args.next().expect("BUG: reading source_key in CREATERULE"); // Skip the command name
    let dest_key = args.next().expect("BUG: reading source_key in CREATERULE");

    // Validate that sourceKey is different from destKey
    if source_key == dest_key {
        return Err(ValkeyError::Str(
            "TSDB: the source key and destination key should be different",
        ));
    }

    // Get source time series (must exist, writable)
    let mut source_series = get_timeseries_mut(
        ctx,
        &source_key,
        true,
        Some(AclPermissions::UPDATE),
    )?
    .expect(
        "BUG in create_rule: should have returned a value before this point (must_exist = true)",
    );

    let source_id = source_series.id;

    // Get destination time series (must exist, writable)
    let mut dest_series = get_timeseries_mut(ctx, &dest_key, true, Some(AclPermissions::UPDATE))?
        .expect(
        "BUG in create_rule: should have returned a value before this point (must_exist = true)",
    );

    let dest_id = dest_series.id;

    if dest_series.is_compaction() {
        return Err(ValkeyError::Str(
            "TSDB: the destination key already has a src rule",
        ));
    }

    // check for duplicate compaction rule
    if source_series
        .rules
        .iter()
        .any(|rule| rule.dest_id == dest_id)
    {
        // match error from redis-ts
        return Err(ValkeyError::Str(
            "TSDB: the destination key already has a src rule",
        ));
    }

    // Parse aggregation options
    let rule = parse_args(&mut args, dest_id)?;

    check_new_rule_circular_dependency(ctx, &mut source_series, &mut dest_series)?;

    source_series.add_compaction_rule(rule);
    // Add the rule to the destination series
    dest_series.src_series = Some(source_id);

    // Replicate the command
    ctx.replicate_verbatim();

    ctx.notify_keyspace_event(NotifyEvent::MODULE, "ts.createrule:src", &source_key);
    ctx.notify_keyspace_event(NotifyEvent::MODULE, "ts.createrule:dest", &dest_key);

    VALKEY_OK
}

fn parse_args(args: &mut CommandArgIterator, dest_id: SeriesRef) -> ValkeyResult<CompactionRule> {
    let aggregation = args.next_str()?;
    if !aggregation.eq_ignore_ascii_case("AGGREGATION") {
        return Err(ValkeyError::Str(error_consts::CANNOT_PARSE_AGGREGATION));
    }

    let (agg_name, cond_str) = split_aggregator_condition(args.next_str()?)?;
    let aggregation_type = AggregationType::try_from(agg_name)?;
    let value_filter = cond_str.map(parse_inline_condition).transpose()?;

    let duration_str = args
        .next_str()
        .map_err(|_| ValkeyError::Str("TSDB: missing bucket duration"))?;
    let duration = parse_duration(duration_str)
        .map_err(|_| ValkeyError::Str("TSDB: invalid bucket duration"))?;

    // possible align timestamp
    let align_timestamp = if let Ok(align_str) = args.next_str() {
        parse_timestamp(align_str, false)
            .map_err(|_| ValkeyError::Str(error_consts::INVALID_ALIGNMENT_TIMESTAMP))?
    } else {
        0
    };

    let bucket_duration = duration.as_millis() as u64;
    // Configure the aggregator with the possible value filter
    let aggr_config = AggregatorConfig::new(aggregation_type, value_filter)?;
    let mut aggregator = aggr_config.create_aggregator();

    // if we're a Rate aggregator, we need to set the bucket duration
    if let Aggregator::Rate(r) = &mut aggregator {
        r.set_window_ms(bucket_duration);
    }

    Ok(CompactionRule {
        dest_id,
        aggregator,
        bucket_duration,
        align_timestamp,
        bucket_start: None,
        has_samples: false,
    })
}
