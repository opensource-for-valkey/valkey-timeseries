use crate::aggregators::AggregationHandler;
use crate::common::Sample;
use crate::error_consts;
use crate::series::{SeriesLink, TimeSeries, try_get_timeseries, try_get_timeseries_mut};
use std::collections::HashSet;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString};

/// The key of the series that feeds `series`, while that source still has a rule for it.
///
/// `src_series` alone does not make `series` a compaction destination: the source may since
/// have been deleted or overwritten, or `series` may be a `RESTORE`d copy of a destination,
/// whose link still names the original's source. Like compaction itself, anything that asks
/// whether a series has a source must check the back-link.
pub(crate) fn live_source_key(ctx: &Context, series: &TimeSeries) -> Option<ValkeyString> {
    let source_key = series.src_series.as_ref()?.to_key_string(ctx);
    let feeds_series = matches!(
        try_get_timeseries(ctx, &source_key, None),
        Ok(Some(source)) if source
            .rules
            .iter()
            .any(|rule| rule.dest.points_to(&series.key))
    );
    feeds_series.then_some(source_key)
}

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

/// The longest chain of compaction rules `TS.CREATERULE` builds: the number of rules from a
/// series with no source down to its furthest destination. Every write, delete and replicated
/// command walks the chain below the series it touches, so its length bounds that work.
pub const MAX_COMPACTION_CHAIN_LENGTH: usize = 8;

/// Checks that a new `source -> dest` rule keeps the rules a forest of short chains: it must not
/// close a cycle, and the chain through it must hold at most [`MAX_COMPACTION_CHAIN_LENGTH`] rules.
pub fn check_new_rule_links(
    ctx: &Context,
    source_key: &ValkeyString,
    dest_key: &ValkeyString,
) -> ValkeyResult<()> {
    // Read-only guards: callers acquire mutable guards only after this check, so a path that
    // reaches the source cannot alias an existing mutable borrow.
    let node = |key: &[u8]| rule_node(ctx, key);

    // Adding source -> dest creates a cycle exactly when dest can already reach the source.
    let below = walk_downstream(dest_key.as_slice(), source_key.as_slice(), node)?;
    if below.reaches_target {
        return Err(ValkeyError::Str(
            error_consts::COMPACTION_CIRCULAR_DEPENDENCY,
        ));
    }

    // Each series has at most one live source, so the chain through the new rule is the one
    // above `source`, the rule itself, and the longest branch below `dest`.
    let length = rules_above(source_key.as_slice(), node)? + 1 + below.longest;
    if length > MAX_COMPACTION_CHAIN_LENGTH {
        return Err(ValkeyError::String(format!(
            "TSDB: compaction rule would make a chain of {length} rules; at most \
             {MAX_COMPACTION_CHAIN_LENGTH} are allowed"
        )));
    }
    Ok(())
}

/// One series as the rule-graph walks see it.
#[derive(Clone, Debug, Default)]
struct RuleNode {
    /// The source its back-link names, if any.
    source: Option<Vec<u8>>,
    /// The destination keys of its rules.
    dests: Vec<Vec<u8>>,
}

/// A key that holds no series is no node, whether missing or overwritten with another type: a
/// link to it is stale, and the walks skip it rather than fail the new rule with `WRONGTYPE`.
fn rule_node(ctx: &Context, key: &[u8]) -> ValkeyResult<Option<RuleNode>> {
    let key = ctx.create_string(key);
    let series = match try_get_timeseries(ctx, &key, None) {
        Err(ValkeyError::WrongType) => return Ok(None),
        other => other?,
    };
    Ok(series.map(|series| RuleNode {
        source: series.src_series.as_ref().map(|link| link.key().to_vec()),
        dests: series
            .rules
            .iter()
            .map(|rule| rule.dest.key().to_vec())
            .collect(),
    }))
}

/// What [`walk_downstream`] found below its start.
#[derive(Debug, PartialEq)]
struct Downstream {
    /// The target is reachable through live rules (the walk stops there).
    reaches_target: bool,
    /// The most rules between the start and any series below it.
    longest: usize,
}

/// Walks the live rules below `start`, looking for `target`.
///
/// Iterative, with an explicit stack: a chain is created at most
/// [`MAX_COMPACTION_CHAIN_LENGTH`] rules long, but one restored or loaded from data written
/// before the cap is not, and a recursive walk spends a stack frame per rule — enough of them
/// overflows the stack, which aborts the server.
///
/// A rule is an edge only while its destination's back-link names the rule's owner. A stale rule
/// (the destination was deleted and re-created, overwritten...) is not, and is checked before the
/// target: the target itself may be the re-created key.
fn walk_downstream(
    start: &[u8],
    target: &[u8],
    node: impl Fn(&[u8]) -> ValkeyResult<Option<RuleNode>>,
) -> ValkeyResult<Downstream> {
    let mut visited: HashSet<Vec<u8>> = HashSet::new();
    let mut longest = 0;
    // (series, the source whose rule led to it, rules between `start` and it)
    let mut stack: Vec<(Vec<u8>, Option<Vec<u8>>, usize)> = vec![(start.to_vec(), None, 0)];
    while let Some((key, from, depth)) = stack.pop() {
        let Some(series) = node(&key)? else {
            continue;
        };
        if let Some(from) = &from
            && series.source.as_deref() != Some(from.as_slice())
        {
            continue;
        }
        if key == target {
            return Ok(Downstream {
                reaches_target: true,
                longest,
            });
        }
        if !visited.insert(key.clone()) {
            continue;
        }
        longest = longest.max(depth);
        for dest in series.dests {
            stack.push((dest, Some(key.clone()), depth + 1));
        }
    }
    Ok(Downstream {
        reaches_target: false,
        longest,
    })
}

/// How many rules feed `start` from above: the length of its chain of live sources. A source
/// is live while it still has a rule for the series below it (see [`live_source_key`]).
fn rules_above(
    start: &[u8],
    node: impl Fn(&[u8]) -> ValkeyResult<Option<RuleNode>>,
) -> ValkeyResult<usize> {
    let mut visited: HashSet<Vec<u8>> = HashSet::new();
    let mut current = start.to_vec();
    let Some(mut series) = node(&current)? else {
        return Ok(0);
    };
    let mut count = 0;
    while let Some(source) = series.source.take() {
        let Some(parent) = node(&source)? else {
            break;
        };
        if !parent.dests.contains(&current) {
            break;
        }
        // Rule creation refuses cycles; this only keeps corrupt links from looping.
        if !visited.insert(std::mem::replace(&mut current, source)) {
            break;
        }
        count += 1;
        series = parent;
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// A rule graph from `source -> dest` edges, every key with a live back-link to its source.
    fn graph(edges: &[(String, String)]) -> HashMap<Vec<u8>, RuleNode> {
        let mut nodes: HashMap<Vec<u8>, RuleNode> = HashMap::new();
        for (source, dest) in edges {
            let (source, dest) = (source.as_bytes().to_vec(), dest.as_bytes().to_vec());
            nodes
                .entry(source.clone())
                .or_default()
                .dests
                .push(dest.clone());
            nodes.entry(dest).or_default().source = Some(source);
        }
        nodes
    }

    fn lookup(
        nodes: &HashMap<Vec<u8>, RuleNode>,
    ) -> impl Fn(&[u8]) -> ValkeyResult<Option<RuleNode>> + '_ {
        |key| Ok(nodes.get(key).cloned())
    }

    fn edges(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(s, d)| (s.to_string(), d.to_string()))
            .collect()
    }

    /// Far deeper than any thread's stack would allow one frame per rule (a test thread's is
    /// 2 MiB): the walks must not recurse.
    #[test]
    fn a_very_long_chain_walks_without_recursing() {
        const LINKS: usize = 200_000;
        let chain: Vec<(String, String)> = (0..LINKS)
            .map(|i| (format!("k{i}"), format!("k{}", i + 1)))
            .collect();
        let nodes = graph(&chain);
        let below = walk_downstream(b"k0", b"elsewhere", lookup(&nodes)).unwrap();
        assert_eq!(
            below,
            Downstream {
                reaches_target: false,
                longest: LINKS
            }
        );
        let tail = format!("k{LINKS}");
        assert_eq!(rules_above(tail.as_bytes(), lookup(&nodes)).unwrap(), LINKS);
        let reaches = walk_downstream(b"k0", tail.as_bytes(), lookup(&nodes)).unwrap();
        assert!(reaches.reaches_target);
    }

    #[test]
    fn a_rule_back_up_the_chain_reaches_its_source() {
        let nodes = graph(&edges(&[("a", "b"), ("b", "c")]));
        // c -> a would close a -> b -> c -> a.
        assert!(
            walk_downstream(b"a", b"c", lookup(&nodes))
                .unwrap()
                .reaches_target
        );
        assert!(
            !walk_downstream(b"c", b"a", lookup(&nodes))
                .unwrap()
                .reaches_target
        );
    }

    #[test]
    fn the_longest_branch_counts_below_a_fan_out() {
        let nodes = graph(&edges(&[("a", "b"), ("a", "c"), ("c", "d"), ("d", "e")]));
        let below = walk_downstream(b"a", b"x", lookup(&nodes)).unwrap();
        assert_eq!(below.longest, 3);
        assert_eq!(rules_above(b"e", lookup(&nodes)).unwrap(), 3);
        assert_eq!(rules_above(b"b", lookup(&nodes)).unwrap(), 1);
        assert_eq!(rules_above(b"a", lookup(&nodes)).unwrap(), 0);
    }

    #[test]
    fn a_stale_rule_is_not_an_edge_in_either_direction() {
        let mut nodes = graph(&edges(&[("a", "b"), ("b", "c")]));
        // b was re-created: it no longer names a as its source, though a still has a rule for it.
        nodes.get_mut(b"b".as_slice()).unwrap().source = Some(b"other".to_vec());
        let below = walk_downstream(b"a", b"c", lookup(&nodes)).unwrap();
        assert_eq!(
            below,
            Downstream {
                reaches_target: false,
                longest: 0
            }
        );
        // c's source b is live, but b's own source link is not.
        assert_eq!(rules_above(b"c", lookup(&nodes)).unwrap(), 1);
        // A back-link whose source dropped the rule is stale too.
        nodes.get_mut(b"a".as_slice()).unwrap().dests.clear();
        nodes.get_mut(b"b".as_slice()).unwrap().source = Some(b"a".to_vec());
        assert_eq!(rules_above(b"c", lookup(&nodes)).unwrap(), 1);
    }

    #[test]
    fn corrupt_links_that_loop_do_not_hang() {
        let mut nodes = graph(&edges(&[("a", "b"), ("b", "a")]));
        nodes.get_mut(b"a".as_slice()).unwrap().source = Some(b"b".to_vec());
        assert!(rules_above(b"a", lookup(&nodes)).unwrap() <= 2);
        let below = walk_downstream(b"a", b"x", lookup(&nodes)).unwrap();
        assert!(!below.reaches_target);
    }
}
