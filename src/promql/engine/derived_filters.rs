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
//! A profile is a superset of what any read will return, and the structural
//! rules were sound for written matchers: a derived filter never excludes a
//! series that could have matched at any step. It covers the window the
//! selector's reads span over the whole query — offset, `@`, lookback or
//! matrix range, and each enclosing subquery's grid applied — so a series
//! that stopped reporting before it (a rolled-out pod) is left out, but one
//! with a sample anywhere in it is in, at whichever step. A selector that
//! occurs more than once is profiled once, over the hull of its windows.
//! Unauthorized series are counted too. Where a profile is unavailable, the
//! leaf behaves exactly as in the static pass.

use crate::common::logging::log_debug;
use crate::common::threads::IntoParRayon;
use crate::promql::binops::can_push_down_common_filters;
use crate::promql::engine::label_profile::{LabelProfile, LabelValueProfile, ProfileWindow};
use crate::promql::engine::{QueryOptions, QueryReader};
use crate::promql::exec::planner::PreloadGrid;
use crate::promql::hashers::SelectorKey;
use crate::promql::optimizer::pushdown::{LeafFilters, pushdown_filters_in_place_with};
use crate::promql::time::{duration_ms, selector_bounds};
use crate::promql::utils::subquery_step_ms;
use crate::promql::{PromqlResult, QueryError};
use ahash::AHashMap;
use orx_parallel::{Par, ParResult};
use promql_parser::label::{METRIC_NAME, MatchOp, Matcher};
use promql_parser::parser::token::{T_LOR, T_LUNLESS};
use promql_parser::parser::value::ValueType;
use promql_parser::parser::{BinaryExpr, Expr, SubqueryExpr, VectorSelector};
use std::cell::RefCell;
use std::collections::hash_map::Entry;

/// Narrow the selectors of `expr`'s binary operations by what the index
/// knows about their operands' series. A no-op, without touching `reader`,
/// for an expression with no binary operation the push-down applies to.
///
/// Meant to run on the tree a range query will plan and evaluate over `grid`,
/// after the static optimizer (if enabled) and before preloading.
pub(crate) fn derive_filters_in_place<R: QueryReader + ?Sized>(
    expr: &mut Expr,
    reader: &R,
    grid: &PreloadGrid,
    options: QueryOptions,
) -> PromqlResult<()> {
    let mut leaves = ProfiledLeaves::collect(expr, reader, grid, options)?;
    if leaves.is_empty() {
        return Ok(());
    }
    short_circuit_empty_operands(expr, Some(grid), &mut leaves);
    pushdown_filters_in_place_with(expr, &leaves);
    Ok(())
}

/// Where one operand of a binary operation is known to be empty — its
/// selector matches no series, and the operand's shape passes emptiness
/// through — the operation's result is empty for every operator but `or`
/// (and, for `unless`, only when the left side is), so reading the other
/// operand is wasted work. It is replaced by that empty selector: nothing
/// more is read, and `empty op empty` is the same empty result.
///
/// The selector is planted bare, without the subquery, `offset`, `@` or
/// matrix range it sat under, and is read over the window of its new place
/// (`grid`, the grid `expr` is evaluated over). It is planted only where that
/// window lies within the one it was proved empty over; elsewhere it could
/// read live series the original never did (`max_over_time(a[5m:1m] offset
/// 1h) + b` would read today's `a`), which costs samples, never a result.
fn short_circuit_empty_operands(
    expr: &mut Expr,
    grid: Option<&PreloadGrid>,
    leaves: &mut ProfiledLeaves,
) {
    match expr {
        Expr::Binary(be) => {
            if can_short_circuit(be) {
                let plantable = |vs: &VectorSelector| leaves.reads_nothing_at(vs, grid);
                let lhs_empty =
                    empty_selector_of(&be.lhs, leaves, &plantable).map(|vs| leaves.detach(vs));
                let rhs_empty =
                    empty_selector_of(&be.rhs, leaves, &plantable).map(|vs| leaves.detach(vs));
                match (lhs_empty, rhs_empty) {
                    (Some(_), Some(_)) | (None, None) => {}
                    (Some(empty), None) => leaves.replace_with_empty(&mut be.rhs, empty),
                    (None, Some(empty)) if be.op.id() != T_LUNLESS => {
                        leaves.replace_with_empty(&mut be.lhs, empty)
                    }
                    (None, Some(_)) => {}
                }
            }
            short_circuit_empty_operands(&mut be.lhs, grid, leaves);
            short_circuit_empty_operands(&mut be.rhs, grid, leaves);
        }
        Expr::Aggregate(agg) => {
            short_circuit_empty_operands(&mut agg.expr, grid, leaves);
            if let Some(param) = agg.param.as_mut() {
                short_circuit_empty_operands(param, grid, leaves);
            }
        }
        Expr::Call(call) => {
            for arg in call.args.args.iter_mut() {
                short_circuit_empty_operands(arg, grid, leaves);
            }
        }
        Expr::Unary(u) => short_circuit_empty_operands(&mut u.expr, grid, leaves),
        Expr::Paren(p) => short_circuit_empty_operands(&mut p.expr, grid, leaves),
        Expr::Subquery(s) => {
            let inner = subquery_grid(s, grid);
            short_circuit_empty_operands(&mut s.expr, inner.as_ref(), leaves)
        }
        Expr::VectorSelector(_)
        | Expr::MatrixSelector(_)
        | Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::Extension(_) => {}
    }
}

/// Both operands vectors, no fill (which synthesizes results for unmatched
/// series), not `or` (which keeps both sides). Looser than the narrowing
/// guard: a label-less `sum(x)` offers no filters but is empty when `x` is.
fn can_short_circuit(be: &BinaryExpr) -> bool {
    !has_fill(be)
        && be.op.id() != T_LOR
        && be.lhs.value_type() == ValueType::Vector
        && be.rhs.value_type() == ValueType::Vector
}

fn has_fill(be: &BinaryExpr) -> bool {
    be.modifier
        .as_ref()
        .is_some_and(|m| m.fill_values.lhs.is_some() || m.fill_values.rhs.is_some())
}

/// The selector that makes `expr`'s result empty, if the profiles prove one:
/// a selector matching no series, under a shape that yields nothing from
/// nothing — aggregations, series-to-series functions, rollups, subqueries,
/// and binary operations other than `or` (both sides) and `unless` (the
/// right side). `absent`/`absent_over_time`, functions that do not return a
/// vector (`scalar()` of nothing is NaN, not empty), functions without a
/// vector argument and `fill()` operations (they synthesize the missing side)
/// are the shapes that do not.
///
/// Only a selector `plantable` accepts is returned; an `or` side that makes
/// no selector available still counts as empty.
fn empty_selector_of<'a>(
    expr: &'a Expr,
    leaves: &ProfiledLeaves,
    plantable: &dyn Fn(&VectorSelector) -> bool,
) -> Option<&'a VectorSelector> {
    let empty = |vs: &'a VectorSelector| {
        leaves
            .profile(vs)
            .filter(|p| p.series == 0 && plantable(vs))
            .map(|_| vs)
    };
    let is_empty = |expr: &Expr| empty_selector_of(expr, leaves, &|_| true).is_some();
    match expr {
        Expr::VectorSelector(vs) => empty(vs),
        Expr::MatrixSelector(ms) => empty(&ms.vs),
        Expr::Paren(p) => empty_selector_of(&p.expr, leaves, plantable),
        Expr::Unary(u) => empty_selector_of(&u.expr, leaves, plantable),
        Expr::Subquery(s) => empty_selector_of(&s.expr, leaves, plantable),
        Expr::Aggregate(agg) => empty_selector_of(&agg.expr, leaves, plantable),
        Expr::Call(call) => {
            if call.func.return_type != ValueType::Vector
                || matches!(call.func.name, "absent" | "absent_over_time")
            {
                return None;
            }
            let pos = call
                .func
                .arg_types
                .iter()
                .position(|&arg| arg != ValueType::Scalar && arg != ValueType::String)?;
            empty_selector_of(call.args.args.get(pos)?, leaves, plantable)
        }
        Expr::Binary(be) if has_fill(be) => None,
        Expr::Binary(be) => match be.op.id() {
            T_LOR if is_empty(&be.lhs) && is_empty(&be.rhs) => {
                empty_selector_of(&be.lhs, leaves, plantable)
                    .or_else(|| empty_selector_of(&be.rhs, leaves, plantable))
            }
            T_LOR => None,
            T_LUNLESS => empty_selector_of(&be.lhs, leaves, plantable),
            _ => empty_selector_of(&be.lhs, leaves, plantable)
                .or_else(|| empty_selector_of(&be.rhs, leaves, plantable)),
        },
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::Extension(_) => None,
    }
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
    /// The window each profile covers; `None` for the whole index.
    windows: AHashMap<SelectorKey, Option<ProfileWindow>>,
    /// Each profile's derived filters, built on first use: a nested operation
    /// asks for a leaf's filters at every level above it, and building them
    /// compiles a regex per multi-valued label.
    derived: RefCell<AHashMap<SelectorKey, Vec<Matcher>>>,
}

impl ProfiledLeaves {
    fn collect<R: QueryReader + ?Sized>(
        expr: &Expr,
        reader: &R,
        grid: &PreloadGrid,
        options: QueryOptions,
    ) -> PromqlResult<Self> {
        let mut leaves = Vec::new();
        collect_operand_leaves(expr, Some(grid), &mut leaves);

        let mut by_node = AHashMap::with_capacity(leaves.len());
        // Per key, the selector and the hull of its occurrences' windows:
        // `SelectorKey` leaves out offset and `@`, so `cpu - cpu offset 5m` is
        // one profile over both windows rather than one fan-out each.
        let mut unique: AHashMap<SelectorKey, (VectorSelector, Option<ProfileWindow>)> =
            AHashMap::new();
        for (vs, window) in leaves {
            // The pass folds a `__name__="m"` matcher into the selector's
            // name before any lookup, so key the normalized form.
            let normalized = normalized(vs);
            let key = SelectorKey::from_selector(&normalized);
            by_node.insert(node_id(vs), key);
            match unique.entry(key) {
                Entry::Vacant(entry) => {
                    entry.insert((normalized, window));
                }
                Entry::Occupied(mut entry) => {
                    let hull = &mut entry.get_mut().1;
                    *hull = (*hull).zip(window).map(|(a, b)| a.hull(b));
                }
            }
        }
        let windows = unique
            .iter()
            .map(|(key, (_, window))| (*key, *window))
            .collect();

        let profiles: Vec<(SelectorKey, Option<LabelProfile>)> = unique
            .into_iter()
            .map(|(key, (vs, window))| (key, vs, window))
            .collect::<Vec<_>>()
            .into_par_rayon()
            .map(
                |(key, vs, window)| match reader.label_profile(&vs, window, options) {
                    Ok(profile) => Ok((key, profile)),
                    Err(QueryError::Timeout) => Err(QueryError::Timeout),
                    // The read that follows will report whatever is wrong; a
                    // profile is only ever an optimization.
                    Err(err) => {
                        log_debug(format!(
                            "label profile unavailable; selector left as written: {err}"
                        ));
                        Ok((key, None))
                    }
                },
            )
            .into_fallible()
            .collect()?;

        Ok(Self {
            by_node,
            profiles: profiles
                .into_iter()
                .filter_map(|(key, profile)| profile.map(|p| (key, p)))
                .collect(),
            windows,
            derived: RefCell::default(),
        })
    }

    /// No selector sits under an operation the push-down applies to.
    fn is_empty(&self) -> bool {
        self.by_node.is_empty()
    }

    /// A profiled selector as a value, with its profile key, ready to be
    /// planted elsewhere in the tree.
    fn detach(&self, vs: &VectorSelector) -> (VectorSelector, Option<SelectorKey>) {
        (vs.clone(), self.by_node.get(&node_id(vs)).copied())
    }

    /// Put `empty` — a selector known to match nothing — in place of
    /// `operand`, and register the new node under the empty profile. The
    /// node is written into the operand's existing box, whose address may
    /// already be a profiled leaf's (the wide side of `a{nope} - b` is the
    /// leaf `b`), so the entry is overwritten rather than left to chance.
    fn replace_with_empty(
        &mut self,
        operand: &mut Box<Expr>,
        (empty, key): (VectorSelector, Option<SelectorKey>),
    ) {
        **operand = Expr::VectorSelector(empty);
        if let (Expr::VectorSelector(vs), Some(key)) = (operand.as_ref(), key) {
            self.by_node.insert(node_id(vs), key);
        }
    }

    /// Whether `vs`, read as written over `grid`, falls within the window
    /// its profile covers — so, when that profile counted no series, reads
    /// nothing there either.
    fn reads_nothing_at(&self, vs: &VectorSelector, grid: Option<&PreloadGrid>) -> bool {
        let Some(proven) = self
            .by_node
            .get(&node_id(vs))
            .and_then(|key| self.windows.get(key))
        else {
            return false;
        };
        match proven {
            None => true,
            Some(proven) => leaf_window(vs, grid, None).is_some_and(|w| proven.contains(w)),
        }
    }

    fn profile(&self, vs: &VectorSelector) -> Option<&LabelProfile> {
        self.by_node
            .get(&node_id(vs))
            .and_then(|key| self.profiles.get(key))
    }

    /// `vs`'s profile's common filters, less those that could prune no
    /// operand of the query — `retain_pruning` would drop them wherever they
    /// landed, after their regex had been compiled for nothing.
    fn derived_filters(&self, vs: &VectorSelector) -> Vec<Matcher> {
        let Some(key) = self.by_node.get(&node_id(vs)) else {
            return Vec::new();
        };
        let Some(profile) = self.profiles.get(key) else {
            return Vec::new();
        };
        if let Some(filters) = self.derived.borrow().get(key) {
            return filters.clone();
        }
        let filters = profile.common_filters_where(|label| self.could_prune_a_leaf(label));
        self.derived.borrow_mut().insert(*key, filters.clone());
        filters
    }

    /// Whether the filter derived from `label` could narrow any selector the
    /// pass may push it into. A selector without a profile keeps whatever it
    /// is given, so it always could.
    fn could_prune_a_leaf(&self, label: &LabelValueProfile) -> bool {
        self.by_node.values().any(|key| {
            self.profiles
                .get(key)
                .is_none_or(|profile| profile.could_be_pruned_by(label))
        })
    }
}

impl LeafFilters for ProfiledLeaves {
    /// The profile's filters together with the written matchers. The written
    /// ones still matter: a `l=~".*[0-4]$"` crosses the operation even when
    /// `l` has too many values to enumerate, and a matcher an inner operation
    /// just added is tighter than the pre-rewrite profile. Whatever is
    /// redundant is dropped where it lands, by `retain_pruning`.
    fn common_filters(&self, vs: &VectorSelector) -> Vec<Matcher> {
        let mut filters = self.derived_filters(vs);
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

/// A selector, with the window its reads cover over the whole query; `None`
/// where that is not known, which profiles it over the whole index.
type Leaf<'a> = (&'a VectorSelector, Option<ProfileWindow>);

/// Every selector under a binary operation the push-down applies to, in an
/// expression evaluated over `grid`.
fn collect_operand_leaves<'a>(expr: &'a Expr, grid: Option<&PreloadGrid>, out: &mut Vec<Leaf<'a>>) {
    match expr {
        Expr::Binary(be) if can_push_down_common_filters(be) || can_short_circuit(be) => {
            collect_all_selectors(&be.lhs, grid, out);
            collect_all_selectors(&be.rhs, grid, out);
        }
        Expr::Binary(be) => {
            collect_operand_leaves(&be.lhs, grid, out);
            collect_operand_leaves(&be.rhs, grid, out);
        }
        Expr::Aggregate(agg) => {
            collect_operand_leaves(&agg.expr, grid, out);
            if let Some(param) = &agg.param {
                collect_operand_leaves(param, grid, out);
            }
        }
        Expr::Call(call) => {
            for arg in &call.args.args {
                collect_operand_leaves(arg, grid, out);
            }
        }
        Expr::Unary(u) => collect_operand_leaves(&u.expr, grid, out),
        Expr::Paren(p) => collect_operand_leaves(&p.expr, grid, out),
        Expr::Subquery(s) => collect_operand_leaves(&s.expr, subquery_grid(s, grid).as_ref(), out),
        Expr::VectorSelector(_)
        | Expr::MatrixSelector(_)
        | Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::Extension(_) => {}
    }
}

fn collect_all_selectors<'a>(expr: &'a Expr, grid: Option<&PreloadGrid>, out: &mut Vec<Leaf<'a>>) {
    match expr {
        Expr::VectorSelector(vs) => out.push((vs, leaf_window(vs, grid, None))),
        Expr::MatrixSelector(ms) => out.push((
            &ms.vs,
            leaf_window(&ms.vs, grid, Some(duration_ms(ms.range))),
        )),
        Expr::Binary(be) => {
            collect_all_selectors(&be.lhs, grid, out);
            collect_all_selectors(&be.rhs, grid, out);
        }
        Expr::Aggregate(agg) => {
            collect_all_selectors(&agg.expr, grid, out);
            if let Some(param) = &agg.param {
                collect_all_selectors(param, grid, out);
            }
        }
        Expr::Call(call) => {
            for arg in &call.args.args {
                collect_all_selectors(arg, grid, out);
            }
        }
        Expr::Unary(u) => collect_all_selectors(&u.expr, grid, out),
        Expr::Paren(p) => collect_all_selectors(&p.expr, grid, out),
        Expr::Subquery(s) => collect_all_selectors(&s.expr, subquery_grid(s, grid).as_ref(), out),
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::Extension(_) => {}
    }
}

/// The grid `s`'s inner expression is evaluated over, across every step of
/// `grid`: the union the preload reads it over too.
fn subquery_grid(s: &SubqueryExpr, grid: Option<&PreloadGrid>) -> Option<PreloadGrid> {
    grid?.subquery_union(s, subquery_step_ms(s))
}

/// The span `vs` reads over `grid`: every step with `@` and `offset` applied,
/// widened back by the matrix range for a range selector (`range_ms`) and by
/// the lookback for an instant one. The same bounds a preload of it reads.
/// Inclusive at the bottom, where a window is open there: a millisecond
/// looser, which is sound.
fn leaf_window(
    vs: &VectorSelector,
    grid: Option<&PreloadGrid>,
    range_ms: Option<i64>,
) -> Option<ProfileWindow> {
    let grid = grid?;
    // A grid with no step reads nothing; no window describes that better
    // than the whole index does.
    grid.last_step()?;
    let (start_ms, end_ms) = selector_bounds(
        vs.at.as_ref(),
        vs.offset.as_ref(),
        grid.at_start_ms,
        grid.at_end_ms,
        grid.start_ms,
        grid.end_ms,
        range_ms.unwrap_or(grid.lookback_delta_ms),
    );
    Some(ProfileWindow::new(start_ms, end_ms))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Sample;
    use crate::labels::Labels;
    use crate::promql::engine::label_profile::{
        LabelProfileBuilder, ProfileWindow, join_regexp_values,
    };
    use crate::promql::engine::memory_series_querier::MemorySeriesQuerier;
    use crate::promql::engine::query_reader::{AggregationOutcome, AggregationRequest};
    use crate::promql::exec::types::EvalLabels;
    use crate::promql::model::{InstantSample, RangeSample};
    use std::sync::Mutex;

    /// A selector's series, as label pairs.
    type SeriesTable<'a> = &'a [&'a [(&'a str, &'a str)]];

    /// A reader that answers profiles from a table and records what was asked,
    /// and over which window.
    struct TableReader {
        profiles: AHashMap<String, LabelProfile>,
        asked: Mutex<Vec<String>>,
        windows: Mutex<AHashMap<String, Option<ProfileWindow>>>,
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
                windows: Mutex::default(),
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
            window: Option<ProfileWindow>,
            _: QueryOptions,
        ) -> PromqlResult<Option<LabelProfile>> {
            let mut stripped = selector.clone();
            stripped.offset = None;
            stripped.at = None;
            let rendered = stripped.to_string();
            self.asked.lock().unwrap().push(rendered.clone());
            self.windows
                .lock()
                .unwrap()
                .insert(rendered.clone(), window);
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

    /// An hour at one-minute steps, with the default five-minute lookback.
    fn grid() -> PreloadGrid {
        PreloadGrid {
            start_ms: HOUR,
            end_ms: 2 * HOUR,
            step_ms: 60_000,
            at_start_ms: HOUR,
            at_end_ms: 2 * HOUR,
            lookback_delta_ms: LOOKBACK,
            sample_timestamps: false,
        }
    }

    const HOUR: i64 = 3_600_000;
    const LOOKBACK: i64 = 300_000;

    fn rewrite(query: &str, reader: &TableReader) -> String {
        let mut expr = promql_parser::parser::parse(query).unwrap();
        derive_filters_in_place(&mut expr, reader, &grid(), options()).unwrap();
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

    /// Twenty `host-NNNNNN.example.net:9100` names: their alternation exceeds
    /// the general regex size limit (eleven fit), which used to drop the
    /// filter. Within the byte budget it is derived and pushed.
    #[test]
    fn a_long_value_set_within_the_byte_budget_is_pushed() {
        fn table(job: &str, hosts: std::ops::Range<usize>) -> SeriesTable<'static> {
            let rows: Vec<&'static [(&'static str, &'static str)]> = hosts
                .map(|i| {
                    let host: &'static str =
                        Box::leak(format!("host-{i:06}.example.net:9100").into_boxed_str());
                    let job: &'static str = Box::leak(job.to_string().into_boxed_str());
                    let row: &'static [(&str, &str)] = Box::leak(
                        vec![("__name__", "cpu"), ("job", job), ("instance", host)].into(),
                    );
                    row
                })
                .collect();
            Box::leak(rows.into_boxed_slice())
        }
        let batch = table("batch", 0..20);
        let all: SeriesTable<'static> =
            Box::leak([batch, table("web", 20..40)].concat().into_boxed_slice());
        let reader = TableReader::new(vec![(r#"cpu{job="batch"}"#, batch), ("cpu", all)]);

        let rewritten = rewrite(r#"cpu{job="batch"} - cpu offset 5m"#, &reader);
        let expected = join_regexp_values(
            (0..20)
                .map(|i| format!("host-{i:06}.example.net:9100"))
                .collect::<Vec<_>>()
                .iter()
                .map(String::as_str),
        );
        assert_eq!(
            rewritten,
            format!(r#"cpu{{job="batch"}} - cpu{{instance=~"{expected}",job="batch"}} offset 5m"#)
                .replace('\\', r"\\")
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

    /// With the static optimizer on, a range query gets its rewrites but not
    /// its blind push-down (`simplify_expr`): that would leave a matcher that
    /// prunes nothing — `cpu{metric="cpu"}` on the right below — and this
    /// pass only ever adds matchers. The written filters still cross, where
    /// the profiles show they prune.
    #[test]
    fn the_static_optimizer_leaves_the_push_down_to_the_pass() {
        let reader = TableReader::new(vec![
            (r#"cpu{region="us"}"#, CPU_US),
            (r#"cpu{metric="cpu"}"#, CPU_ALL),
            ("cpu", CPU_ALL),
        ]);
        let rewrite_simplified = |query: &str| {
            let expr = promql_parser::parser::parse(query).unwrap();
            let mut expr = crate::promql::optimizer::simplify_expr(expr).unwrap();
            derive_filters_in_place(&mut expr, &reader, &grid(), options()).unwrap();
            expr.to_string()
        };

        assert_eq!(
            rewrite_simplified(r#"cpu{metric="cpu"} - cpu offset 5m"#),
            r#"cpu{metric="cpu"} - cpu offset 5m"#
        );
        assert_eq!(
            rewrite_simplified(r#"cpu{region="us"} - cpu offset 5m"#),
            r#"cpu{region="us"} - cpu{host=~"a|b",region="us"} offset 5m"#
        );
        // The rewrites still apply: `A + A` becomes `A * 2`, which has no
        // vector operand pair left to narrow, so no profile is asked for.
        reader.asked.lock().unwrap().clear();
        assert_eq!(rewrite_simplified("cpu + cpu"), "cpu * 2");
        assert!(reader.asked.lock().unwrap().is_empty());
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
    fn a_scalar_argument_is_never_narrowed() {
        const RATIO: SeriesTable<'static> = &[&[("__name__", "ratio")]];
        let reader = TableReader::new(vec![
            (r#"cpu{region="us"}"#, CPU_US),
            ("cpu", CPU_ALL),
            ("ratio", RATIO),
        ]);
        // `cpu offset 5m` is narrowed; `ratio` has no `region` and must keep
        // every series, or `scalar()` would turn into NaN.
        assert_eq!(
            rewrite(
                r#"cpu{region="us"} > cpu offset 5m * scalar(ratio)"#,
                &reader
            ),
            r#"cpu{region="us"} > cpu{host=~"a|b",region="us"} offset 5m * scalar(ratio)"#
        );
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

    /// `group_left(a)` writes bar's `a` onto the result, so what the index
    /// proves about foo's `a` is not true of the join, and must not narrow
    /// the operand matched on it.
    #[test]
    fn a_label_the_group_clause_overwrites_does_not_cross() {
        const FOO: SeriesTable<'static> = &[
            &[("__name__", "foo"), ("a", "x"), ("instance", "i1")],
            &[("__name__", "foo"), ("a", "x"), ("instance", "i2")],
        ];
        const BAR: SeriesTable<'static> = &[
            &[("__name__", "bar"), ("a", "y"), ("instance", "i1")],
            &[("__name__", "bar"), ("a", "z"), ("instance", "i2")],
        ];
        const BAZ: SeriesTable<'static> = &[
            &[("__name__", "baz"), ("a", "y")],
            &[("__name__", "baz"), ("a", "z")],
        ];
        let reader = TableReader::new(vec![("foo", FOO), ("bar", BAR), ("baz", BAZ)]);
        assert_eq!(
            rewrite(
                r#"baz and on(a) (foo * on(instance) group_left(a) bar)"#,
                &reader
            ),
            r#"baz and on (a) (foo * on (instance) group_left (a) bar)"#
        );
        assert_eq!(
            rewrite(
                r#"baz and on(a) (bar * on(instance) group_right(a) foo)"#,
                &reader
            ),
            r#"baz and on (a) (bar * on (instance) group_right (a) foo)"#
        );
    }

    /// The inner join trims the set to `host` for its own operands; that must
    /// not cost the outer right operand the `region` it is matched on.
    #[test]
    fn a_join_on_the_left_does_not_narrow_what_its_sibling_receives() {
        let reader = TableReader::new(vec![(r#"cpu{region="us"}"#, CPU_US), ("cpu", CPU_ALL)]);
        assert_eq!(
            rewrite(
                r#"(cpu{region="us"} * on(host) group_left cpu offset 1m) and on(region) cpu offset 5m"#,
                &reader
            ),
            r#"(cpu{region="us"} * on (host) group_left () cpu{host=~"a|b"} offset 1m) and on (region) cpu{region="us"} offset 5m"#
        );
    }

    /// A grouped join's result carries the many side's labels and the group
    /// clause's labels from the one side: a filter reaching it from above goes
    /// to whichever side the label comes from — and nowhere under `fill()`.
    #[test]
    fn a_filter_reaching_a_grouped_join_goes_to_the_side_its_label_comes_from() {
        let reader = TableReader::new(vec![(r#"cpu{region="us"}"#, CPU_US), ("cpu", CPU_ALL)]);
        assert_eq!(
            rewrite(
                r#"(cpu offset 1m * on(host) group_left cpu offset 2m) and on(region) cpu{region="us"}"#,
                &reader
            ),
            r#"(cpu{region="us"} offset 1m * on (host) group_left () cpu offset 2m) and on (region) cpu{region="us"}"#
        );
        assert_eq!(
            rewrite(
                r#"cpu{region="us"} and on(region) (cpu offset 1m * on(host) group_left(region) cpu offset 2m)"#,
                &reader
            ),
            r#"cpu{region="us"} and on (region) (cpu offset 1m * on (host) group_left (region) cpu{region="us"} offset 2m)"#
        );
        assert_eq!(
            rewrite(
                r#"(cpu offset 1m * on(host) group_left fill(0) cpu offset 2m) and on(region) cpu{region="us"}"#,
                &reader
            ),
            r#"(cpu offset 1m * on (host) group_left () fill (0) cpu offset 2m) and on (region) cpu{region="us"}"#
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
        // A label-less aggregation has nothing to offer: the profiles are
        // asked for (an empty operand would still short-circuit) but no
        // filter crosses.
        let reader = TableReader::new(vec![(r#"cpu{region="us"}"#, CPU_US), ("cpu", CPU_ALL)]);
        assert_eq!(
            rewrite(r#"sum(cpu{region="us"}) / sum(cpu)"#, &reader),
            r#"sum(cpu{region="us"}) / sum(cpu)"#
        );
        assert_eq!(reader.asked.lock().unwrap().len(), 2);
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
    /// That holds with the static optimizer on too, whose rewrites the pass
    /// then profiles and narrows further.
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
        // `foo` carries one `a` everywhere, `bar` and `baz` others: the
        // join's `a` is bar's, and narrowing `baz` by foo's would empty it.
        for (name, a, instance) in [
            ("foo", "x", Some("i1")),
            ("foo", "x", Some("i2")),
            ("bar", "y", Some("i1")),
            ("bar", "z", Some("i2")),
            ("baz", "y", None),
            ("baz", "z", None),
        ] {
            let mut pairs = vec![("__name__", name), ("a", a)];
            if let Some(instance) = instance {
                pairs.push(("instance", instance));
            }
            let labels = Labels::from_pairs(&pairs);
            for point in 0..=200 {
                querier.add_sample(&labels, Sample::new(point * 10_000, 1.0));
            }
        }
        let reader: Arc<dyn QueryReader> = Arc::new(querier);

        let queries = [
            r#"baz and on(a) (foo * on(instance) group_left(a) bar)"#,
            r#"baz and on(a) (bar * on(instance) group_right(a) foo)"#,
            r#"cpu{region="us"} - cpu offset 5m"#,
            r#"cpu{region="us"} / on(host) mem"#,
            r#"cpu{region="us", host="h0"} / ignoring(host, metric, kind) mem{host="h2"}"#,
            r#"mem{region=~"us|eu"} * on(host) group_left cpu"#,
            r#"(cpu{region="us"} * on(host) group_left mem) and on(region) cpu offset 5m"#,
            r#"(mem{region="eu"} * on(host) group_left cpu) unless on(region, kind) mem{region="us"}"#,
            r#"label_replace(cpu{region="ap"}, "metric", "m", "", "") and on(region) mem"#,
            r#"(cpu * on(host) group_left(kind) mem) and on(region) cpu{region="us"} offset 5m"#,
            r#"(mem * on(host) group_left(metric) cpu) and on(metric, region) cpu{region="eu"}"#,
            r#"cpu{region="us"} and on(region) (mem * on(host) group_left(region) cpu{region="us"})"#,
            r#"(cpu * on(host) group_left(kind) fill(0) mem) and on(region) cpu{region="us"} offset 5m"#,
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
            r#"cpu{region="mars"} - cpu"#,
            r#"sum(cpu) / count(cpu{region="mars"})"#,
            r#"cpu unless on(host) mem{region="mars"}"#,
            r#"cpu{region="mars"} or mem{region="us"}"#,
            r#"absent(cpu{region="mars"}) * on() group_right mem{region="us"}"#,
            // Static rewrites the derived pass then sees: `A + A → A * 2`,
            // folded constants, and written filters already pushed across.
            r#"cpu + cpu"#,
            r#"cpu{region="us"} + on(host) ((2 - 1) * mem)"#,
            r#"cpu{metric="cpu"} - cpu offset 5m"#,
            r#"cpu{host=~"h[0-4]"} / on(host) mem"#,
            r#"mem / on(region) group_left sum by (region) (cpu{region=~"us|eu"})"#,
        ];
        for query in queries {
            let run = |optimize: bool, derived: bool| {
                let opts = QueryOptions {
                    optimize_queries: optimize,
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
            let unrewritten = run(false, false);
            for (optimize, derived) in [(false, true), (true, false), (true, true)] {
                assert_eq!(
                    run(optimize, derived),
                    unrewritten,
                    "{query} (optimize={optimize}, derived={derived})"
                );
            }
            let empty_by_design = query.contains("mars")
                && !query.contains("unless")
                && !query.contains(" or ")
                && !query.contains("absent");
            assert_eq!(
                unrewritten.is_empty(),
                empty_by_design,
                "{query}: {unrewritten:?}"
            );
        }
    }

    const NOTHING: SeriesTable<'static> = &[];

    #[test]
    fn an_empty_operand_short_circuits_the_other_side() {
        let reader = TableReader::new(vec![
            (r#"cpu{region="mars"}"#, NOTHING),
            ("cpu", CPU_ALL),
            ("mem", CPU_ALL),
        ]);
        // Arithmetic, comparison and `and`: either side empty empties the
        // result, so the wide side is not read at all.
        assert_eq!(
            rewrite(r#"cpu{region="mars"} - cpu"#, &reader),
            r#"cpu{region="mars"} - cpu{region="mars"}"#
        );
        assert_eq!(
            rewrite(r#"cpu > bool on(host) cpu{region="mars"}"#, &reader),
            r#"cpu{region="mars"} > bool on (host) cpu{region="mars"}"#
        );
        assert_eq!(
            rewrite(r#"sum(cpu) / sum(rate(cpu{region="mars"}[5m]))"#, &reader),
            r#"cpu{region="mars"} / sum(rate(cpu{region="mars"}[5m]))"#
        );
        // `unless`: only an empty left side empties the result.
        assert_eq!(
            rewrite(r#"cpu{region="mars"} unless mem"#, &reader),
            r#"cpu{region="mars"} unless cpu{region="mars"}"#
        );
        assert_eq!(
            rewrite(r#"cpu unless on(host) cpu{region="mars"}"#, &reader),
            r#"cpu unless on (host) cpu{region="mars"}"#
        );
        // `or` keeps both sides; `absent` yields something from nothing.
        assert_eq!(
            rewrite(r#"cpu{region="mars"} or mem"#, &reader),
            r#"cpu{region="mars"} or mem"#
        );
        assert_eq!(
            rewrite(
                r#"absent(cpu{region="mars"}) * on() group_right mem"#,
                &reader
            ),
            r#"absent(cpu{region="mars"}) * on () group_right () mem"#
        );
        // A nested `or` is empty only when both its sides are.
        assert_eq!(
            rewrite(r#"(cpu{region="mars"} or mem) - cpu"#, &reader),
            r#"(cpu{region="mars"} or mem) - cpu"#
        );
        // `scalar()` of nothing is NaN, not empty: `mem * NaN` still has
        // series, so neither side of the outer operation may be dropped.
        assert_eq!(
            rewrite(r#"cpu - (mem * scalar(cpu{region="mars"}))"#, &reader),
            r#"cpu - (mem * scalar(cpu{region="mars"}))"#
        );
        // A nested `fill()` produces series for the side that has none.
        assert_eq!(
            rewrite(r#"cpu - (mem + fill(0) cpu{region="mars"})"#, &reader),
            r#"cpu - (mem + fill (0) cpu{region="mars"})"#
        );
    }

    #[test]
    fn a_metric_name_matcher_is_normalized_before_lookup() {
        let reader = TableReader::new(vec![(r#"cpu{region="us"}"#, CPU_US), ("cpu", CPU_ALL)]);
        assert_eq!(
            rewrite(r#"{__name__="cpu",region="us"} - cpu"#, &reader),
            r#"cpu{region="us"} - cpu{host=~"a|b",region="us"}"#
        );
    }

    /// The window each selector of `query` was profiled over, `(start, end)`
    /// in ms.
    fn windows(query: &str) -> Vec<(String, Option<(i64, i64)>)> {
        let reader = TableReader::new(vec![]);
        rewrite(query, &reader);
        let mut windows: Vec<_> = reader
            .windows
            .lock()
            .unwrap()
            .iter()
            .map(|(selector, w)| (selector.clone(), w.map(|w| (w.start_ms, w.end_ms))))
            .collect();
        windows.sort();
        windows
    }

    fn window(selector: &str, start_ms: i64, end_ms: i64) -> (String, Option<(i64, i64)>) {
        (selector.to_string(), Some((start_ms, end_ms)))
    }

    #[test]
    fn a_selector_is_profiled_over_the_span_its_reads_cover() {
        // Lookback for an instant selector, the matrix range for a range one.
        assert_eq!(
            windows("a - b"),
            vec![
                window("a", HOUR - LOOKBACK, 2 * HOUR),
                window("b", HOUR - LOOKBACK, 2 * HOUR)
            ]
        );
        assert_eq!(
            windows("rate(a[10m]) - rate(b[2m])"),
            vec![
                window("a", HOUR - 600_000, 2 * HOUR),
                window("b", HOUR - 120_000, 2 * HOUR)
            ]
        );
        // `offset` shifts the span; `@ <t>` pins it to one evaluation time.
        assert_eq!(
            windows("a - b offset 1h"),
            vec![
                window("a", HOUR - LOOKBACK, 2 * HOUR),
                window("b", -LOOKBACK, HOUR)
            ]
        );
        assert_eq!(
            windows("a - b @ 1800"),
            vec![
                window("a", HOUR - LOOKBACK, 2 * HOUR),
                window("b", 1_800_000 - LOOKBACK, 1_800_000)
            ]
        );
        assert_eq!(
            windows("a - b @ 1800 offset -10m"),
            vec![
                window("a", HOUR - LOOKBACK, 2 * HOUR),
                window("b", 2_400_000 - LOOKBACK, 2_400_000)
            ]
        );
        // `@ start()` / `@ end()` are evaluated at every step of a range
        // query, so they cover the whole range, as the preload does.
        for at in ["start()", "end()"] {
            assert_eq!(
                windows(&format!("a - b @ {at}")),
                vec![
                    window("a", HOUR - LOOKBACK, 2 * HOUR),
                    window("b", HOUR - LOOKBACK, 2 * HOUR)
                ],
                "@ {at}"
            );
        }
    }

    #[test]
    fn a_selector_under_a_subquery_is_profiled_over_its_union_grid() {
        let query = "max_over_time((a - b offset 5m)[30m:1m])";
        let Expr::Call(call) = promql_parser::parser::parse(query).unwrap() else {
            unreachable!()
        };
        let Expr::Subquery(sq) = call.args.args[0].as_ref() else {
            unreachable!()
        };
        // What the preload reads the subquery over.
        let union = grid().subquery_union(sq, 60_000).unwrap();
        assert_eq!(
            (union.start_ms, union.end_ms),
            (HOUR - 29 * 60_000, 2 * HOUR)
        );
        assert_eq!(
            windows(query),
            vec![
                window("a", union.start_ms - LOOKBACK, union.end_ms),
                window(
                    "b",
                    union.start_ms - 300_000 - LOOKBACK,
                    union.end_ms - 300_000
                )
            ]
        );

        // Nested: the outer subquery's union grid is the inner one's outer grid.
        let nested = "max_over_time(rate((a - b)[10m:1m])[30m:5m])";
        let Expr::Call(outer) = promql_parser::parser::parse(nested).unwrap() else {
            unreachable!()
        };
        let Expr::Subquery(outer_sq) = outer.args.args[0].as_ref() else {
            unreachable!()
        };
        let Expr::Call(rate) = outer_sq.expr.as_ref() else {
            unreachable!()
        };
        let Expr::Subquery(inner_sq) = rate.args.args[0].as_ref() else {
            unreachable!()
        };
        let inner = grid()
            .subquery_union(outer_sq, 300_000)
            .unwrap()
            .subquery_union(inner_sq, 60_000)
            .unwrap();
        assert_eq!(
            windows(nested),
            vec![
                window("a", inner.start_ms - LOOKBACK, inner.end_ms),
                window("b", inner.start_ms - LOOKBACK, inner.end_ms)
            ]
        );
    }

    #[test]
    fn a_selector_occurring_twice_is_profiled_once_over_the_hull() {
        let reader = TableReader::new(vec![]);
        rewrite("x - x offset 1h", &reader);
        assert_eq!(reader.asked.lock().unwrap().len(), 1);
        assert_eq!(
            windows("x - x offset 1h"),
            vec![window("x", -LOOKBACK, 2 * HOUR)]
        );
    }

    /// `m{region="eu"}` stopped reporting ten minutes in; `m{region="us"}`
    /// and both regions' `info` report across the query's hour.
    fn retired_eu() -> MemorySeriesQuerier {
        let querier = MemorySeriesQuerier::new();
        let m_eu = Labels::from_pairs(&[("__name__", "m"), ("region", "eu")]);
        let m_us = Labels::from_pairs(&[("__name__", "m"), ("region", "us")]);
        let info_eu = Labels::from_pairs(&[("__name__", "info"), ("region", "eu")]);
        let info_us = Labels::from_pairs(&[("__name__", "info"), ("region", "us")]);
        for minute in 0..10 {
            querier.add_sample(&m_eu, Sample::new(minute * 60_000, 1.0));
        }
        for minute in 60..=120 {
            let t = minute * 60_000;
            for labels in [&m_us, &info_eu, &info_us] {
                querier.add_sample(labels, Sample::new(t, 1.0));
            }
        }
        querier
    }

    fn rewrite_with(query: &str, reader: &MemorySeriesQuerier) -> String {
        let mut expr = promql_parser::parser::parse(query).unwrap();
        derive_filters_in_place(&mut expr, reader, &grid(), options()).unwrap();
        expr.to_string()
    }

    #[test]
    fn a_retired_series_does_not_widen_the_filters() {
        let reader = retired_eu();
        // Over the whole index `m` carries both regions, which prunes nothing
        // from `info`; over the hour only `us` reports.
        assert_eq!(
            rewrite_with("info - m", &reader),
            r#"info{region="us"} - m"#
        );
        // An offset back into the first ten minutes sees `eu` again.
        assert_eq!(
            rewrite_with("info - m offset 55m", &reader),
            "info - m offset 55m"
        );
    }

    #[test]
    fn an_operand_of_retired_series_only_short_circuits_the_other() {
        let reader = retired_eu();
        assert_eq!(
            rewrite_with(r#"info - m{region="eu"}"#, &reader),
            r#"m{region="eu"} - m{region="eu"}"#
        );
        // An hour back it reports, so it narrows `info` instead.
        assert_eq!(
            rewrite_with(r#"info - m{region="eu"} offset 1h"#, &reader),
            r#"info{region="eu"} - m{region="eu"} offset 1h"#
        );
    }

    /// Two `a` series reporting three minutes before the query's end and one
    /// `b` reporting at it; nothing earlier.
    fn current_only() -> MemorySeriesQuerier {
        let querier = MemorySeriesQuerier::new();
        for i in ["1", "2"] {
            let a = Labels::from_pairs(&[("__name__", "a"), ("i", i)]);
            querier.add_sample(&a, Sample::new(2 * HOUR - 180_000, 1.0));
        }
        let b = Labels::from_pairs(&[("__name__", "b")]);
        querier.add_sample(&b, Sample::new(2 * HOUR, 1.0));
        querier
    }

    /// The empty operand's selector is planted bare, without the subquery,
    /// `@`, `offset` or matrix range around it: it is only planted where its
    /// own reads there fall within the window it was proved empty over.
    #[test]
    fn an_empty_selector_is_planted_only_where_it_still_reads_nothing() {
        let reader = current_only();
        // The query's last step only, as the evaluation below runs it.
        let at_end = PreloadGrid {
            start_ms: 2 * HOUR,
            at_start_ms: 2 * HOUR,
            ..grid()
        };
        let normalized = |q: &str| promql_parser::parser::parse(q).unwrap().to_string();
        let rewrite = |q: &str| {
            let mut expr = promql_parser::parser::parse(q).unwrap();
            derive_filters_in_place(&mut expr, &reader, &at_end, options()).unwrap();
            expr.to_string()
        };
        for query in [
            "max_over_time(a[5m:1m] offset 1h) + b",
            "max_over_time(a[5m:1m] @ 3600) + b",
            "rate(a[1m]) + b",
        ] {
            assert_eq!(rewrite(query), normalized(query), "{query}");
        }
        // A subquery's grid reaches further back than the lookback, so its
        // own `offset` selector read bare covers no more than it was proved
        // empty over.
        assert_eq!(
            rewrite("max_over_time(b offset 1h [5m:1m]) + a"),
            normalized("max_over_time(b offset 1h [5m:1m]) + b offset 1h")
        );
    }

    /// Whatever the pass plants, the query reads no more than the original:
    /// a one-sample budget the original fits in is not exceeded.
    #[test]
    fn a_short_circuit_reads_nothing_the_original_does_not() {
        use crate::promql::engine::evaluate_range;
        use promql_parser::parser::EvalStmt;
        use std::sync::Arc;
        use std::time::{Duration, UNIX_EPOCH};

        let reader: Arc<dyn QueryReader> = Arc::new(current_only());
        for query in [
            "max_over_time(a[5m:1m] offset 1h) + b",
            "max_over_time(a[5m:1m] @ 3600) + b",
            "rate(a[1m]) + b",
        ] {
            for derived in [false, true] {
                let opts = QueryOptions {
                    derived_filter_pushdown: derived,
                    max_samples: 1,
                    ..options()
                };
                let stmt = EvalStmt {
                    expr: promql_parser::parser::parse(query).unwrap(),
                    start: UNIX_EPOCH + Duration::from_millis(2 * HOUR as u64),
                    end: UNIX_EPOCH + Duration::from_millis(2 * HOUR as u64),
                    interval: Duration::from_secs(60),
                    lookback_delta: opts.lookback_delta,
                };
                let result = evaluate_range(reader.clone(), stmt, opts)
                    .unwrap_or_else(|e| panic!("{query} (derived={derived}): {e}"));
                assert!(result.is_empty(), "{query} (derived={derived}): {result:?}");
            }
        }
    }
}
