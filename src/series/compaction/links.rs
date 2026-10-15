use crate::aggregators::AggregationHandler;
use crate::common::Sample;
use crate::error_consts;
use crate::series::{SeriesLink, TimeSeries, try_get_timeseries, try_get_timeseries_mut};
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString};

/// The still-open bucket of the rule that feeds `series`, a compaction destination (`LATEST`).
pub(crate) fn get_latest_compaction_sample(ctx: &Context, series: &TimeSeries) -> Option<Sample> {
    let parent_key = series.src_series.as_ref()?.to_key_string(ctx);
    let Ok(Some(parent)) = try_get_timeseries(ctx, &parent_key, None) else {
        // No source series or it doesn't exist
        return None;
    };

    let rule = parent
        .rules
        .iter()
        .find(|rule| rule.dest.points_to(&series.key))?;
    let start = rule.bucket_start?;

    let mut agg = rule.aggregator.clone();
    let value = AggregationHandler::finalize(&mut agg);

    let sample = Sample::new(start, value);
    Some(sample)
}

/// Points the compaction partners of a series renamed from `old_key` to its current key at that
/// key: its source's rule for it, and each of its destinations' source link.
pub(crate) fn relink_renamed_series(ctx: &Context, series: &TimeSeries, old_key: &[u8]) {
    let new_key: &[u8] = &series.key;
    // Never re-open the renamed series itself: the caller holds it.
    let is_partner = |link: &SeriesLink| !link.points_to(new_key);

    if let Some(source_link) = series.src_series.as_ref().filter(|l| is_partner(l))
        && let Ok(Some(mut source)) =
            try_get_timeseries_mut(ctx, &source_link.to_key_string(ctx), None)
    {
        for rule in source.rules.iter_mut() {
            if rule.dest.points_to(old_key) {
                rule.dest = SeriesLink::from_key(new_key);
            }
        }
    }

    for rule in series.rules.iter().filter(|rule| is_partner(&rule.dest)) {
        if let Ok(Some(mut dest)) = try_get_timeseries_mut(ctx, &rule.dest.to_key_string(ctx), None)
            && dest
                .src_series
                .as_ref()
                .is_some_and(|src| src.points_to(old_key))
        {
            dest.src_series = Some(SeriesLink::from_key(new_key));
        }
    }
}

/// Check if adding a new compaction rule would create a circular dependency
pub fn check_new_rule_circular_dependency(
    ctx: &Context,
    source_key: &ValkeyString,
    dest_key: &ValkeyString,
) -> ValkeyResult<()> {
    // Adding source -> destination creates a cycle exactly when the
    // destination can already reach the source. Traverse with read-only
    // guards: callers acquire mutable guards only after this check, so a
    // path that reaches the source cannot alias an existing mutable borrow.
    if dependency_reaches(
        ctx,
        dest_key,
        source_key.as_slice(),
        &mut std::collections::HashSet::new(),
    )? {
        return Err(ValkeyError::Str(
            error_consts::COMPACTION_CIRCULAR_DEPENDENCY,
        ));
    }

    Ok(())
}

fn dependency_reaches(
    ctx: &Context,
    current: &ValkeyString,
    target: &[u8],
    visited: &mut std::collections::HashSet<Vec<u8>>,
) -> ValkeyResult<bool> {
    if current.as_slice() == target {
        return Ok(true);
    }
    if !visited.insert(current.as_slice().to_vec()) {
        return Ok(false);
    }

    let Some(series) = try_get_timeseries(ctx, current, None)? else {
        return Ok(false);
    };

    for rule in &series.rules {
        if dependency_reaches(ctx, &rule.dest.to_key_string(ctx), target, visited)? {
            return Ok(true);
        }
    }
    Ok(false)
}
