//! Data-derived filter push-down for range queries.
//!
//! `a{namespace="prod"} * on(uid) group_left b` reads every series of `b` and
//! then discards all but the `prod` ones when it matches. An instant query
//! avoids that: it evaluates one operand, reads the label values its result
//! carries, and adds them as matchers to the other operand before reading it
//! (`Evaluator::eval_binop_with_pushdown`). A range query cannot — every
//! selector is read once for the whole step grid *before* anything is
//! evaluated, and a selector rewritten afterwards would miss the grid loaded
//! for the original.
//!
//! So here the same narrowing happens before planning, and its facts come
//! from the series index rather than from an evaluation: for each selector
//! under such a binary operation, a [`LabelProfile`] says which labels every
//! one of its series carries and with which values. The static optimizer's
//! rules ([`pushdown_filters_in_place_with`]) then do what they do for
//! written matchers, with two differences at the leaves: a selector's common
//! filters are those its profile proves, and a filter is added to a selector
//! only when that selector's own profile shows it would prune something.
//! The second is what keeps `a - b` — where both sides carry the same values
//! — a byte-identical tree.
//!
//! A profile is a superset of what any read will return (the index is
//! time-agnostic, and unauthorized series are counted too), and the
//! structural rules were sound for written matchers: a derived filter never
//! excludes a series that could have matched at any step. Where a profile is
//! unavailable, the leaf behaves exactly as in the static pass.

use crate::common::threads::IntoParRayon;
use crate::promql::binops::can_push_down_common_filters;
use crate::promql::engine::label_profile::LabelProfile;
use crate::promql::engine::{QueryOptions, QueryReader};
use crate::promql::hashers::SelectorKey;
use crate::promql::optimizer::pushdown::{LeafFilters, pushdown_filters_in_place_with};
use crate::promql::{PromqlResult, QueryError};
use ahash::{AHashMap, AHashSet};
use orx_parallel::{ParIter, ParIterResult};
use promql_parser::label::{METRIC_NAME, MatchOp, Matcher};
use promql_parser::parser::{BinaryExpr, Expr, VectorSelector};

/// Narrow the selectors of `expr`'s binary operations by what the index
/// knows about their operands' series. A no-op, without touching `reader`,
/// for an expression with no binary operation the push-down applies to.
///
/// Meant to run on the tree a range query will plan and evaluate, after the
/// static optimizer (if enabled) and before preloading.
pub fn derive_filters_in_place<R: QueryReader + ?Sized>(
    expr: &mut Expr,
    reader: &R,
    options: QueryOptions,
) -> PromqlResult<()> {
    let leaves = ProfiledLeaves::collect(expr, reader, options)?;
    if leaves.is_empty() {
        return Ok(());
    }
    pushdown_filters_in_place_with(expr, &leaves);
    Ok(())
}

/// The profiles of the selectors under the push-down's binary operations,
/// looked up by the selector node's address: the pass rewrites matcher
/// lists in place and never moves a node, so the address stays good while a
/// selector's matchers — and therefore its structural key — change under it.
/// A nested binary operation thus sees its operands' pre-rewrite profiles,
/// which are supersets of the narrowed selectors' and stay sound.
struct ProfiledLeaves {
    by_node: AHashMap<usize, SelectorKey>,
    profiles: AHashMap<SelectorKey, LabelProfile>,
}

impl ProfiledLeaves {
    fn collect<R: QueryReader + ?Sized>(
        expr: &Expr,
        reader: &R,
        options: QueryOptions,
    ) -> PromqlResult<Self> {
        let mut leaves = Vec::new();
        collect_operand_leaves(expr, &mut leaves);

        let mut by_node = AHashMap::with_capacity(leaves.len());
        let mut seen = AHashSet::new();
        let mut unique = Vec::new();
        for vs in leaves {
            // The pass folds a `__name__="m"` matcher into the selector's
            // name before any lookup, so key the normalized form.
            let normalized = normalized(vs);
            let key = SelectorKey::from_selector(&normalized);
            by_node.insert(node_id(vs), key);
            if seen.insert(key) {
                unique.push((key, normalized));
            }
        }

        let profiles: Vec<(SelectorKey, Option<LabelProfile>)> = unique
            .into_par_rayon()
            .map(|(key, vs)| match reader.label_profile(&vs, options) {
                Ok(profile) => Ok((key, profile)),
                Err(QueryError::Timeout) => Err(QueryError::Timeout),
                // The read that follows will report whatever is wrong; a
                // profile is only ever an optimization.
                Err(err) => {
                    tracing::debug!(error = %err, "label profile unavailable; selector left as written");
                    Ok((key, None))
                }
            })
            .into_fallible_result()
            .collect()?;

        Ok(Self {
            by_node,
            profiles: profiles
                .into_iter()
                .filter_map(|(key, profile)| profile.map(|p| (key, p)))
                .collect(),
        })
    }

    /// No selector sits under an operation the push-down applies to.
    fn is_empty(&self) -> bool {
        self.by_node.is_empty()
    }

    fn profile(&self, vs: &VectorSelector) -> Option<&LabelProfile> {
        self.by_node
            .get(&node_id(vs))
            .and_then(|key| self.profiles.get(key))
    }
}

impl LeafFilters for ProfiledLeaves {
    /// The profile's filters together with the written matchers. The written
    /// ones still matter: a `l=~".*[0-4]$"` crosses the operation even when
    /// `l` has too many values to enumerate, and a matcher an inner operation
    /// just added is tighter than the pre-rewrite profile. Whatever is
    /// redundant is dropped where it lands, by `retain_pruning`.
    fn common_filters(&self, vs: &VectorSelector) -> Vec<Matcher> {
        let mut filters = self
            .profile(vs)
            .map(LabelProfile::common_filters)
            .unwrap_or_default();
        for written in vs.matchers.matchers.iter() {
            if written.name != METRIC_NAME && !filters.contains(written) {
                filters.push(written.clone());
            }
        }
        filters
    }

    fn retain_pruning(&self, vs: &VectorSelector, filters: &mut Vec<Matcher>) {
        if let Some(profile) = self.profile(vs) {
            filters.retain(|m| !profile.satisfied_by_all(m));
        }
    }

    /// The instant path's guard: both operands vectors with labels to offer,
    /// no `or`, no fill modifier.
    fn narrows(&self, be: &BinaryExpr) -> bool {
        can_push_down_common_filters(be)
    }
}

fn node_id(vs: &VectorSelector) -> usize {
    vs as *const VectorSelector as usize
}

/// `vs` with a `__name__="m"` equality matcher moved into its name, as the
/// static pass does at each selector.
fn normalized(vs: &VectorSelector) -> VectorSelector {
    let mut vs = vs.clone();
    if vs.name.is_none()
        && let Some(pos) = vs
            .matchers
            .matchers
            .iter()
            .position(|m| m.name == METRIC_NAME && m.op == MatchOp::Equal)
    {
        let m = vs.matchers.matchers.remove(pos);
        vs.name = Some(m.value);
    }
    vs
}

/// Every selector under a binary operation the push-down applies to.
fn collect_operand_leaves<'a>(expr: &'a Expr, out: &mut Vec<&'a VectorSelector>) {
    match expr {
        Expr::Binary(be) if can_push_down_common_filters(be) => {
            collect_all_selectors(&be.lhs, out);
            collect_all_selectors(&be.rhs, out);
        }
        Expr::Binary(be) => {
            collect_operand_leaves(&be.lhs, out);
            collect_operand_leaves(&be.rhs, out);
        }
        Expr::Aggregate(agg) => {
            collect_operand_leaves(&agg.expr, out);
            if let Some(param) = &agg.param {
                collect_operand_leaves(param, out);
            }
        }
        Expr::Call(call) => {
            for arg in &call.args.args {
                collect_operand_leaves(arg, out);
            }
        }
        Expr::Unary(u) => collect_operand_leaves(&u.expr, out),
        Expr::Paren(p) => collect_operand_leaves(&p.expr, out),
        Expr::Subquery(s) => collect_operand_leaves(&s.expr, out),
        Expr::VectorSelector(_)
        | Expr::MatrixSelector(_)
        | Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::Extension(_) => {}
    }
}

fn collect_all_selectors<'a>(expr: &'a Expr, out: &mut Vec<&'a VectorSelector>) {
    match expr {
        Expr::VectorSelector(vs) => out.push(vs),
        Expr::MatrixSelector(ms) => out.push(&ms.vs),
        Expr::Binary(be) => {
            collect_all_selectors(&be.lhs, out);
            collect_all_selectors(&be.rhs, out);
        }
        Expr::Aggregate(agg) => {
            collect_all_selectors(&agg.expr, out);
            if let Some(param) = &agg.param {
                collect_all_selectors(param, out);
            }
        }
        Expr::Call(call) => {
            for arg in &call.args.args {
                collect_all_selectors(arg, out);
            }
        }
        Expr::Unary(u) => collect_all_selectors(&u.expr, out),
        Expr::Paren(p) => collect_all_selectors(&p.expr, out),
        Expr::Subquery(s) => collect_all_selectors(&s.expr, out),
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::Extension(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::promql::engine::label_profile::LabelProfileBuilder;
    use crate::promql::engine::query_reader::{AggregationOutcome, AggregationRequest};
    use crate::promql::exec::types::EvalLabels;
    use crate::promql::model::{InstantSample, RangeSample};
    use std::sync::Mutex;

    /// A selector's series, as label pairs.
    type SeriesTable<'a> = &'a [&'a [(&'a str, &'a str)]];

    /// A reader that answers profiles from a table and records what was asked.
    struct TableReader {
        profiles: AHashMap<String, LabelProfile>,
        asked: Mutex<Vec<String>>,
    }

    impl TableReader {
        fn new(entries: Vec<(&str, SeriesTable<'_>)>) -> Self {
            let profiles = entries
                .into_iter()
                .map(|(selector, series)| {
                    let mut builder = LabelProfileBuilder::new();
                    for labels in series {
                        builder.add_series(labels.iter().copied());
                    }
                    (selector.to_string(), builder.finish())
                })
                .collect();
            Self {
                profiles,
                asked: Mutex::new(Vec::new()),
            }
        }
    }

    impl QueryReader for TableReader {
        fn query(
            &self,
            _: &VectorSelector,
            _: i64,
            _: QueryOptions,
        ) -> PromqlResult<Vec<InstantSample<EvalLabels>>> {
            unreachable!("the pass reads no samples")
        }

        fn query_range(
            &self,
            _: &VectorSelector,
            _: i64,
            _: i64,
            _: QueryOptions,
        ) -> PromqlResult<Vec<RangeSample<EvalLabels>>> {
            unreachable!("the pass reads no samples")
        }

        fn query_aggregation(
            &self,
            _: &VectorSelector,
            _: i64,
            _: &AggregationRequest,
            _: QueryOptions,
        ) -> PromqlResult<AggregationOutcome> {
            unreachable!()
        }

        fn label_profile(
            &self,
            selector: &VectorSelector,
            _: QueryOptions,
        ) -> PromqlResult<Option<LabelProfile>> {
            let mut stripped = selector.clone();
            stripped.offset = None;
            stripped.at = None;
            let rendered = stripped.to_string();
            self.asked.lock().unwrap().push(rendered.clone());
            Ok(self.profiles.get(&rendered).cloned())
        }
    }

    fn options() -> QueryOptions {
        QueryOptions {
            timeout: None,
            deadline: None,
            ..QueryOptions::default()
        }
    }

    fn rewrite(query: &str, reader: &TableReader) -> String {
        let mut expr = promql_parser::parser::parse(query).unwrap();
        derive_filters_in_place(&mut expr, reader, options()).unwrap();
        expr.to_string()
    }

    const CPU_US: SeriesTable<'static> = &[
        &[
            ("__name__", "cpu"),
            ("region", "us"),
            ("host", "a"),
            ("metric", "cpu"),
        ],
        &[
            ("__name__", "cpu"),
            ("region", "us"),
            ("host", "b"),
            ("metric", "cpu"),
        ],
    ];
    const CPU_ALL: SeriesTable<'static> = &[
        &[
            ("__name__", "cpu"),
            ("region", "us"),
            ("host", "a"),
            ("metric", "cpu"),
        ],
        &[
            ("__name__", "cpu"),
            ("region", "us"),
            ("host", "b"),
            ("metric", "cpu"),
        ],
        &[
            ("__name__", "cpu"),
            ("region", "eu"),
            ("host", "c"),
            ("metric", "cpu"),
        ],
        &[
            ("__name__", "cpu"),
            ("region", "ap"),
            ("host", "d"),
            ("metric", "cpu"),
        ],
    ];

    #[test]
    fn a_selective_side_narrows_the_other_and_only_where_it_prunes() {
        let reader = TableReader::new(vec![(r#"cpu{region="us"}"#, CPU_US), ("cpu", CPU_ALL)]);
        // `region="us"` prunes eu/ap on the right, and so does `host=~"a|b"`;
        // `metric="cpu"` is true of every series on both sides and is not
        // added. Nothing flows back: every filter derived from the right is
        // already satisfied by the left.
        assert_eq!(
            rewrite(r#"cpu{region="us"} - cpu offset 5m"#, &reader),
            r#"cpu{region="us"} - cpu{host=~"a|b",region="us"} offset 5m"#
        );
    }

    #[test]
    fn identical_operands_are_left_alone() {
        let reader = TableReader::new(vec![("cpu", CPU_ALL)]);
        assert_eq!(
            rewrite("cpu - cpu offset 5m", &reader),
            "cpu - cpu offset 5m"
        );
        // One profile for the two occurrences.
        assert_eq!(reader.asked.lock().unwrap().len(), 1);
    }

    #[test]
    fn no_binary_operation_means_no_profile_is_asked_for() {
        let reader = TableReader::new(vec![("cpu", CPU_ALL)]);
        assert_eq!(
            rewrite("sum by (region) (cpu)", &reader),
            "sum by (region) (cpu)"
        );
        assert!(reader.asked.lock().unwrap().is_empty());
    }

    #[test]
    fn or_and_fill_are_never_narrowed() {
        let reader = TableReader::new(vec![(r#"cpu{region="us"}"#, CPU_US), ("cpu", CPU_ALL)]);
        assert_eq!(
            rewrite(r#"cpu{region="us"} or cpu"#, &reader),
            r#"cpu{region="us"} or cpu"#
        );
        assert!(reader.asked.lock().unwrap().is_empty());
    }

    #[test]
    fn matching_modifiers_trim_what_crosses() {
        let reader = TableReader::new(vec![(r#"cpu{region="us"}"#, CPU_US), ("cpu", CPU_ALL)]);
        // Only `host` takes part in the match, so only `host` crosses.
        assert_eq!(
            rewrite(r#"cpu{region="us"} / on(host) cpu"#, &reader),
            r#"cpu{region="us"} / on (host) cpu{host=~"a|b"}"#
        );
        // `host` is ignored, so `region` (and `metric`, redundant) cross.
        assert_eq!(
            rewrite(r#"cpu{region="us"} / ignoring(host) cpu"#, &reader),
            r#"cpu{region="us"} / ignoring (host) cpu{region="us"}"#
        );
    }

    #[test]
    fn an_unavailable_profile_falls_back_to_the_written_matchers() {
        // Nothing is known about either side: the static rule applies, and
        // with no target profile every written matcher is pushed.
        let reader = TableReader::new(vec![]);
        assert_eq!(
            rewrite(r#"cpu{region="us"} - cpu"#, &reader),
            r#"cpu{region="us"} - cpu{region="us"}"#
        );
        // Known on one side only: what the left proves is pushed right (no
        // profile to say it is redundant), and the right's written matcher
        // is the only thing that crosses back — redundant on the left.
        let reader = TableReader::new(vec![(r#"cpu{region="us"}"#, CPU_US)]);
        assert_eq!(
            rewrite(r#"cpu{region="us"} - cpu{metric="cpu"}"#, &reader),
            r#"cpu{region="us"} - cpu{host=~"a|b",metric="cpu",region="us"}"#
        );
    }

    #[test]
    fn an_aggregated_operand_contributes_its_grouping_labels_only() {
        let reader = TableReader::new(vec![(r#"cpu{region="us"}"#, CPU_US), ("cpu", CPU_ALL)]);
        assert_eq!(
            rewrite(
                r#"sum by (region) (cpu{region="us"}) / on(region) group_left count by (region) (cpu)"#,
                &reader
            ),
            r#"sum by (region) (cpu{region="us"}) / on (region) group_left () count by (region) (cpu{region="us"})"#
        );
        // A label-less aggregation has nothing to offer and nothing is asked.
        let reader = TableReader::new(vec![(r#"cpu{region="us"}"#, CPU_US), ("cpu", CPU_ALL)]);
        assert_eq!(
            rewrite(r#"sum(cpu{region="us"}) / sum(cpu)"#, &reader),
            r#"sum(cpu{region="us"}) / sum(cpu)"#
        );
        assert!(reader.asked.lock().unwrap().is_empty());
    }

    #[test]
    fn rollup_and_subquery_operands_are_narrowed_at_their_selectors() {
        let reader = TableReader::new(vec![(r#"cpu{region="us"}"#, CPU_US), ("cpu", CPU_ALL)]);
        assert_eq!(
            rewrite(
                r#"rate(cpu{region="us"}[5m]) / on(host) rate(cpu[5m])"#,
                &reader
            ),
            r#"rate(cpu{region="us"}[5m]) / on (host) rate(cpu{host=~"a|b"}[5m])"#
        );
        assert_eq!(
            rewrite(
                r#"cpu{region="us"} / on(host) max_over_time(cpu[5m:1m])"#,
                &reader
            ),
            r#"cpu{region="us"} / on (host) max_over_time(cpu{host=~"a|b"}[5m:1m])"#
        );
    }

    #[test]
    fn nested_operations_use_the_one_batch_of_profiles() {
        let reader = TableReader::new(vec![(r#"cpu{region="us"}"#, CPU_US), ("cpu", CPU_ALL)]);
        assert_eq!(
            rewrite(
                r#"(cpu{region="us"} - cpu offset 5m) / on(host) cpu"#,
                &reader
            ),
            r#"(cpu{region="us"} - cpu{host=~"a|b",region="us"} offset 5m) / on (host) cpu{host=~"a|b"}"#
        );
        assert_eq!(reader.asked.lock().unwrap().len(), 2);
    }

    /// The narrowed tree evaluates to exactly what the original does: a
    /// derived filter only ever removes series that could not have matched.
    #[test]
    fn narrowed_range_queries_evaluate_identically() {
        use crate::common::Sample;
        use crate::labels::Labels;
        use crate::promql::engine::evaluate_range;
        use crate::promql::engine::memory_series_querier::MemorySeriesQuerier;
        use promql_parser::parser::EvalStmt;
        use std::sync::Arc;
        use std::time::{Duration, UNIX_EPOCH};

        let querier = MemorySeriesQuerier::new();
        let regions = ["us", "eu", "ap", "sa"];
        for (r, region) in regions.iter().enumerate() {
            for h in 0..3 {
                let host = format!("h{}", r * 3 + h);
                let cpu = Labels::from_pairs(&[
                    ("__name__", "cpu"),
                    ("region", region),
                    ("host", &host),
                    ("metric", "cpu"),
                ]);
                // `mem` exists for a subset of hosts only, and carries a
                // label `cpu` does not.
                let mem = Labels::from_pairs(&[
                    ("__name__", "mem"),
                    ("region", region),
                    ("host", &host),
                    ("kind", "rss"),
                ]);
                for point in 0..=200 {
                    let ts = point * 10_000;
                    querier.add_sample(&cpu, Sample::new(ts, (point + h as i64) as f64));
                    if h != 1 {
                        querier.add_sample(&mem, Sample::new(ts, (point + 1) as f64));
                    }
                }
            }
        }
        let reader: Arc<dyn QueryReader> = Arc::new(querier);

        let queries = [
            r#"cpu{region="us"} - cpu offset 5m"#,
            r#"cpu{region="us"} / on(host) mem"#,
            r#"cpu{region="us", host="h0"} / ignoring(host, metric, kind) mem{host="h2"}"#,
            r#"mem{region=~"us|eu"} * on(host) group_left cpu"#,
            r#"cpu and on(host) mem{host=~"h[0-3]"}"#,
            r#"cpu unless on(host) mem{region="ap"}"#,
            r#"cpu{region="us"} or mem"#,
            r#"sum by (region) (cpu{region="us"}) / on(region) group_left count by (region) (mem)"#,
            r#"rate(cpu{region="us"}[2m]) / on(host) rate(mem[2m])"#,
            r#"cpu{region="us"} / on(host) max_over_time(mem[2m:1m])"#,
            r#"(cpu{region="us"} - cpu offset 5m) / on(host) mem"#,
            r#"label_replace(cpu{region="us"}, "h", "$1", "host", "(.*)") * on(host) mem"#,
            r#"count_values("v", cpu{region="us"}) * on(v) group_left count_values("v", mem)"#,
            r#"cpu{region="us"} @ 1500 - cpu"#,
        ];
        for query in queries {
            let run = |derived: bool| {
                let opts = QueryOptions {
                    derived_filter_pushdown: derived,
                    ..options()
                };
                let stmt = EvalStmt {
                    expr: promql_parser::parser::parse(query).unwrap(),
                    start: UNIX_EPOCH + Duration::from_millis(1_200_000),
                    end: UNIX_EPOCH + Duration::from_millis(1_800_000),
                    interval: Duration::from_secs(60),
                    lookback_delta: opts.lookback_delta,
                };
                let mut result: Vec<(String, Vec<(i64, f64)>)> =
                    evaluate_range(reader.clone(), stmt, opts)
                        .unwrap_or_else(|e| panic!("{query}: {e}"))
                        .into_iter()
                        .map(|s| {
                            (
                                s.labels.to_string(),
                                s.samples.iter().map(|p| (p.timestamp, p.value)).collect(),
                            )
                        })
                        .collect();
                result.sort_by(|a, b| a.0.cmp(&b.0));
                result
            };
            let (with, without) = (run(true), run(false));
            assert!(!with.is_empty(), "{query}: empty result proves nothing");
            assert_eq!(with, without, "{query}");
        }
    }

    #[test]
    fn a_metric_name_matcher_is_normalized_before_lookup() {
        let reader = TableReader::new(vec![(r#"cpu{region="us"}"#, CPU_US), ("cpu", CPU_ALL)]);
        assert_eq!(
            rewrite(r#"{__name__="cpu",region="us"} - cpu"#, &reader),
            r#"cpu{region="us"} - cpu{host=~"a|b",region="us"}"#
        );
    }
}
