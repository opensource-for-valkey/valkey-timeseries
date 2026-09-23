use crate::labels::HasFingerprint;
use crate::promql::binops::can_push_down_common_filters;
use crate::promql::functions::{PromqlFunctionKind, resolve_function};
use crate::promql::hashers::FingerprintHashSet;
use ahash::HashSetExt;
use promql_parser::label::{METRIC_NAME, Matcher, Matchers};
use promql_parser::parser::token::{T_COUNT_VALUES, T_LOR, T_LUNLESS};
use promql_parser::parser::value::ValueType;
use promql_parser::parser::{
    AggregateExpr, BinaryExpr, Expr, LabelModifier, VectorMatchCardinality, VectorSelector,
};
use smallvec::SmallVec;
use std::borrow::Cow;
use std::ops::Deref;
use std::vec::Vec;

/// What the filter push-down knows about a selector's series.
///
/// Every structural rule of the push-down — which labels survive an
/// aggregation, a `label_replace`, an `on()`/`ignoring()` — is the same
/// whatever the source of the leaf facts. The leaf rule is what varies: the
/// static optimizer knows only the matchers written on a selector, while a
/// pass with access to the series index knows the label values the selector's
/// series actually carry (see `engine::derived_filters`).
pub trait LeafFilters {
    /// The label filters every series `vs` yields is known to satisfy, never
    /// including `__name__`.
    fn common_filters(&self, vs: &VectorSelector) -> Vec<Matcher>;

    /// Drop from `filters` those that would exclude no series `vs` reads.
    /// The default keeps every filter: a resolver that cannot tell pushes
    /// them all, as the static optimizer always has.
    fn retain_pruning(&self, _vs: &VectorSelector, _filters: &mut Vec<Matcher>) {}

    /// Whether the operands of `be` may be narrowed by each other's filters
    /// at all. The default says yes for every operation.
    fn narrows(&self, _be: &BinaryExpr) -> bool {
        true
    }
}

/// The static optimizer's leaf rule: the matchers written on the selector.
pub struct WrittenFilters;

impl LeafFilters for WrittenFilters {
    fn common_filters(&self, vs: &VectorSelector) -> Vec<Matcher> {
        get_common_label_filters_without_metric_name(&vs.matchers)
    }

    /// The same guard as the runtime push-down: no `fill()` (it keeps
    /// unmatched series a filter would drop), no `or`, and both operands
    /// label-carrying vectors.
    fn narrows(&self, be: &BinaryExpr) -> bool {
        can_push_down_common_filters(be)
    }
}

/// Whether `e`'s value carries series labels. A scalar or string — a literal,
/// `time()`, `scalar(x)`, `2 * scalar(x)` — has none: it matches every series
/// on the other side of a binary operation, so it neither contributes filters
/// nor can be narrowed by them. Pushing a filter into the selector under
/// `scalar(x)` would change the value it yields, not prune series.
fn carries_labels(e: &Expr) -> bool {
    matches!(e.value_type(), ValueType::Vector | ValueType::Matrix)
}

/// `push_down_filters` optimizes expressions to improve their performance.
///
/// It performs the following optimizations:
///
/// - Adds missing filters to `foo{filters1} op bar{filters2}`
///   according to https://utcc.utoronto.ca/~cks/space/blog/sysadmin/PrometheusLabelNonOptimization
pub fn push_down_filters(expr: &Expr) -> Cow<'_, Expr> {
    if can_pushdown_filters(expr) {
        let mut clone = expr.clone();
        pushdown_filters_in_place(&mut clone);
        Cow::Owned(clone)
    } else {
        Cow::Borrowed(expr)
    }
}

pub fn can_pushdown_filters(expr: &Expr) -> bool {
    use Expr::*;

    match expr {
        Call(call) => call
            .args
            .args
            .iter()
            .any(|x| can_pushdown_filters(x.deref())),
        Binary(be) => can_pushdown_filters(&be.lhs) || can_pushdown_filters(&be.rhs),
        Aggregate(agg) => {
            can_pushdown_filters(&agg.expr)
                || agg.param.as_ref().is_some_and(|e| can_pushdown_filters(e))
        }
        Paren(p) => can_pushdown_filters(&p.expr),
        Unary(unary) => can_pushdown_filters(&unary.expr),
        Subquery(s) => can_pushdown_filters(&s.expr),
        _ => false,
    }
}

pub fn pushdown_filters_in_place(expr: &mut Expr) {
    pushdown_filters_in_place_with(expr, &WrittenFilters)
}

/// [`pushdown_filters_in_place`] with the leaf rule supplied by `leaves`.
pub fn pushdown_filters_in_place_with(expr: &mut Expr, leaves: &dyn LeafFilters) {
    use Expr::*;

    match expr {
        VectorSelector(vs) => {
            if vs.name.is_none()
                && let Some(pos) = vs.matchers.matchers.iter().position(|m| {
                    m.name == METRIC_NAME && m.op == promql_parser::label::MatchOp::Equal
                })
            {
                let m = vs.matchers.matchers.remove(pos);
                vs.name = Some(m.value);
            }
        }
        Call(f) => {
            for arg in f.args.args.iter_mut() {
                pushdown_filters_in_place_with(arg, leaves);
            }
        }
        Aggregate(agg) => {
            pushdown_filters_in_place_with(&mut agg.expr, leaves);
            if let Some(param) = agg.param.as_mut() {
                pushdown_filters_in_place_with(param, leaves);
            }
        }
        Binary(be) => {
            pushdown_filters_in_place_with(&mut be.lhs, leaves);
            pushdown_filters_in_place_with(&mut be.rhs, leaves);
            if leaves.narrows(be) {
                let mut lfs = get_common_label_filters_with(expr, leaves);
                push_down_binary_op_filters_in_place_with(expr, &mut lfs, leaves);
            }
        }
        Unary(unary) => pushdown_filters_in_place_with(&mut unary.expr, leaves),
        Paren(p) => pushdown_filters_in_place_with(&mut p.expr, leaves),
        Subquery(s) => pushdown_filters_in_place_with(&mut s.expr, leaves),
        _ => {}
    }
}

pub fn get_common_label_filters(e: &Expr) -> Vec<Matcher> {
    get_common_label_filters_with(e, &WrittenFilters)
}

/// The label filters every series of `e`'s result satisfies, with the leaf
/// rule supplied by `leaves`.
pub fn get_common_label_filters_with(e: &Expr, leaves: &dyn LeafFilters) -> Vec<Matcher> {
    use Expr::*;

    if !carries_labels(e) {
        return vec![];
    }

    match e {
        VectorSelector(m) => leaves.common_filters(m),
        Subquery(s) => get_common_label_filters_with(&s.expr, leaves),
        MatrixSelector(m) => leaves.common_filters(&m.vs),
        Call(fe) => {
            if let Some(func) = resolve_function(fe.func.name) {
                let kind = func.kind();
                return match kind {
                    PromqlFunctionKind::LabelJoin | PromqlFunctionKind::LabelReplace => {
                        get_common_label_filters_for_label_replace(&fe.args.args, leaves)
                    }
                    PromqlFunctionKind::CountOverTime => {
                        get_common_label_filters_for_count_values_over_time(&fe.args.args, leaves)
                    }
                    _ => {
                        let Some(pos) =
                            fe.func.arg_types.iter().position(|&arg| {
                                arg != ValueType::Scalar && arg != ValueType::String
                            })
                        else {
                            return vec![];
                        };
                        let arg = &fe.args.args[pos];
                        get_common_label_filters_with(arg, leaves)
                    }
                };
            }
            vec![]
        }
        Aggregate(agg) => {
            let mut filters = get_common_label_filters_with(&agg.expr, leaves);
            trim_filters_by_aggr_modifier(&mut filters, agg);
            filters
        }
        Unary(unary) => get_common_label_filters_with(&unary.expr, leaves),
        Paren(p) => get_common_label_filters_with(&p.expr, leaves),
        Binary(binary) => {
            let mut lfs_left = get_common_label_filters_with(&binary.lhs, leaves);
            let mut lfs_right = get_common_label_filters_with(&binary.rhs, leaves);
            let card = VectorMatchCardinality::OneToOne;
            let group_modifier: Option<LabelModifier> = None;

            let (group_modifier, join_modifier) = if let Some(modifier) = &binary.modifier {
                (&modifier.matching, &modifier.card)
            } else {
                (&group_modifier, &card)
            };

            // `fill()` emits a series from either side alone, like `or`: only
            // filters true of both sides hold for every output series.
            let fills = binary
                .modifier
                .as_ref()
                .is_some_and(|m| m.fill_values.lhs.is_some() || m.fill_values.rhs.is_some());
            if fills {
                let mut common = intersect_label_filters(lfs_left, lfs_right);
                trim_filters_by_match_modifier(&mut common, group_modifier);
                return common;
            }

            match binary.op.id() {
                T_LOR => {
                    // {fCommon, f1} or {fCommon, f2} -> {fCommon}
                    // {fCommon, f1} or on() {fCommon, f2} -> {}
                    // {fCommon, f1} or on(fCommon) {fCommon, f2} -> {fCommon}
                    // {fCommon, f1} or on(f1) {fCommon, f2} -> {}
                    // {fCommon, f1} or on(f2) {fCommon, f2} -> {}
                    // {fCommon, f1} or on(f3) {fCommon, f2} -> {}
                    lfs_left = intersect_label_filters(lfs_left, lfs_right);
                    trim_filters_by_match_modifier(&mut lfs_left, group_modifier);
                    lfs_left
                }
                T_LUNLESS => {
                    // {f1} unless {f2} -> {f1}
                    // {f1} unless on() {f2} -> {}
                    // {f1} unless on(f1) {f2} -> {f1}
                    // {f1} unless on(f2) {f2} -> {}
                    // {f1} unless on(f1, f2) {f2} -> {f1}
                    // {f1} unless on(f3) {f2} -> {}
                    trim_filters_by_match_modifier(&mut lfs_left, group_modifier);
                    lfs_left
                }
                _ => {
                    match join_modifier {
                        // group_left
                        VectorMatchCardinality::ManyToOne(_) => {
                            // {f1} * group_left() {f2} -> {f1, f2}
                            // {f1} * on() group_left() {f2} -> {f1}
                            // {f1} * on(f1) group_left() {f2} -> {f1}
                            // {f1} * on(f2) group_left() {f2} -> {f1, f2}
                            // {f1} * on(f1, f2) group_left() {f2} -> {f1, f2}
                            // {f1} * on(f3) group_left() {f2} -> {f1}
                            trim_filters_by_match_modifier(&mut lfs_right, group_modifier);
                            union_label_filters(lfs_left, lfs_right)
                        }
                        // group_right
                        VectorMatchCardinality::OneToMany(_) => {
                            // {f1} * group_right() {f2} -> {f1, f2}
                            // {f1} * on() group_right() {f2} -> {f2}
                            // {f1} * on(f1) group_right() {f2} -> {f1, f2}
                            // {f1} * on(f2) group_right() {f2} -> {f2}
                            // {f1} * on(f1, f2) group_right() {f2} -> {f1, f2}
                            // {f1} * on(f3) group_right() {f2} -> {f2}
                            trim_filters_by_match_modifier(&mut lfs_left, group_modifier);
                            union_label_filters(lfs_left, lfs_right)
                        }
                        _ => {
                            // {f1} * {f2} -> {f1, f2}
                            // {f1} * on() {f2} -> {}
                            // {f1} * on(f1) {f2} -> {f1}
                            // {f1} * on(f2) {f2} -> {f2}
                            // {f1} * on(f1, f2) {f2} -> {f2}
                            // {f1} * on(f3} {f2} -> {}
                            lfs_left = union_label_filters(lfs_left, lfs_right);
                            trim_filters_by_match_modifier(&mut lfs_left, group_modifier);
                            lfs_left
                        }
                    }
                }
            }
        }
        _ => {
            vec![]
        }
    }
}

fn get_common_label_filters_for_count_values_over_time(
    args: &[Box<Expr>],
    leaves: &dyn LeafFilters,
) -> Vec<Matcher> {
    if args.len() != 2 {
        return vec![];
    }
    let lfs = get_common_label_filters_with(&args[1], leaves);
    drop_label_filters_for_label_name(&lfs, &args[0])
}

fn get_common_label_filters_for_label_replace(
    args: &[Box<Expr>],
    leaves: &dyn LeafFilters,
) -> Vec<Matcher> {
    if args.len() < 2 {
        return vec![];
    }
    let lfs = get_common_label_filters_with(&args[0], leaves);
    drop_label_filters_for_label_name(&lfs, &args[1])
}

fn trim_filters_by_aggr_modifier(lfs: &mut Vec<Matcher>, afe: &AggregateExpr) {
    match &afe.modifier {
        None => lfs.clear(),
        Some(modifier) => match modifier {
            LabelModifier::Include(args) => filter_label_filters_on(lfs, &args.labels),
            LabelModifier::Exclude(args) => filter_label_filters_ignoring(lfs, &args.labels),
        },
    }
}

/// Trims lfs by the specified be.modifier.matching (e.g., on() or ignoring()).
///
/// The following cases are possible:
/// - It returns lfs as is if be doesn't contain any group modifier
/// - It returns only filters specified in on()
/// - It drops filters specified inside ignoring()
pub fn trim_filters_by_match_modifier(
    lfs: &mut Vec<Matcher>,
    group_modifier: &Option<LabelModifier>,
) {
    match group_modifier {
        None => {}
        Some(modifier) => match modifier {
            LabelModifier::Include(labels) => filter_label_filters_on(lfs, &labels.labels),
            LabelModifier::Exclude(labels) => filter_label_filters_ignoring(lfs, &labels.labels),
        },
    }
}

fn get_common_label_filters_without_metric_name(matchers: &Matchers) -> Vec<Matcher> {
    if !matchers.or_matchers.is_empty() {
        let lfss = &matchers.or_matchers;
        let head = &lfss[0];
        let mut lfs_a = get_label_filters_without_metric_name(head);
        for lfs in lfss[1..].iter() {
            if lfs_a.is_empty() {
                return vec![];
            }
            let lfs_b = get_label_filters_without_metric_name(lfs);
            lfs_a = intersect_label_filters(lfs_a, lfs_b);
        }
        return lfs_a;
    }
    if !matchers.matchers.is_empty() {
        return get_label_filters_without_metric_name(&matchers.matchers);
    }
    vec![]
}

// todo: use lifetimes instead of cloning
fn get_label_filters_without_metric_name(lfs: &[Matcher]) -> Vec<Matcher> {
    lfs.iter()
        .filter(|&x| x.name != METRIC_NAME)
        .cloned()
        .collect::<Vec<_>>()
}

/// Pushes down the given common_filters to `expr` if possible.
///
/// `expr` must be a part of a binary operation - either left or right.
///
/// For example, if e contains `foo + sum(bar)` and common_filters=`{x="y"}`,
/// then the returned expression will contain `foo{x="y"} + sum(bar)`.
///
/// The `{x="y"}` cannot be pushed down to `sum(bar)`, since this
/// may change binary operation results.
pub fn pushdown_binary_op_filters(expr: &Expr, common_filters: Vec<Matcher>) -> Cow<'_, Expr> {
    // according to pushdown_binary_op_filters_in_place, only the following types need to be
    // handled, so exit otherwise
    if common_filters.is_empty() || !can_pushdown_op_filters(expr) {
        return Cow::Borrowed(expr);
    }

    let mut copy = expr.clone();
    let mut common_filters = common_filters;
    push_down_binary_op_filters_in_place(&mut copy, &mut common_filters);
    Cow::Owned(copy)
}

fn can_pushdown_op_filters(expr: &Expr) -> bool {
    use Expr::*;
    // these are the types handled below in pushdown_binary_op_filters_in_place
    matches!(expr, |Call(_)| Binary(_)
        | Aggregate(_)
        | Paren(_)
        | Subquery(_)
        | Unary(_))
}

/// Append `common_filters` to the selector `vs`, less those `leaves` knows
/// would prune nothing there.
fn push_filters_to_selector(
    vs: &mut VectorSelector,
    common_filters: &[Matcher],
    leaves: &dyn LeafFilters,
) {
    // Owned: the retained set is this selector's, not the sibling's the
    // caller's list goes on to.
    let mut filters = common_filters.to_vec();
    leaves.retain_pruning(vs, &mut filters);
    if !filters.is_empty() {
        push_filters_to_matchers(&mut vs.matchers, &filters);
    }
}

fn push_filters_to_matchers(matchers: &mut Matchers, common_filters: &[Matcher]) {
    if !matchers.matchers.is_empty() {
        union_label_filters_internal(&mut matchers.matchers, common_filters);
        matchers
            .matchers
            .sort_by(|a, b| a.name.cmp(&b.name).then(a.value.cmp(&b.value)));
    } else if !matchers.or_matchers.is_empty() {
        for matcher in matchers.or_matchers.iter_mut() {
            union_label_filters_internal(matcher, common_filters);
            matcher.sort_by(|a, b| a.name.cmp(&b.name).then(a.value.cmp(&b.value)));
        }
    } else {
        let mut new_matchers = common_filters.to_vec();
        new_matchers.sort_by(|a, b| a.name.cmp(&b.name).then(a.value.cmp(&b.value)));
        matchers.matchers = new_matchers;
    }
}

pub fn push_down_binary_op_filters_in_place(e: &mut Expr, common_filters: &mut Vec<Matcher>) {
    push_down_binary_op_filters_in_place_with(e, common_filters, &WrittenFilters)
}

/// [`push_down_binary_op_filters_in_place`] with `leaves` deciding, at each
/// selector, which of the filters are worth adding.
pub fn push_down_binary_op_filters_in_place_with(
    e: &mut Expr,
    common_filters: &mut Vec<Matcher>,
    leaves: &dyn LeafFilters,
) {
    use Expr::*;

    if common_filters.is_empty() || !carries_labels(e) {
        return;
    }

    match e {
        VectorSelector(me) => {
            push_filters_to_selector(me, common_filters, leaves);
        }
        MatrixSelector(me) => {
            push_filters_to_selector(&mut me.vs, common_filters, leaves);
        }
        Subquery(s) => {
            push_down_binary_op_filters_in_place_with(&mut s.expr, common_filters, leaves)
        }
        Call(fe) => match fe.func.name {
            "label_replace" | "label_join" => {
                pushdown_label_filters_for_label_replace(&mut fe.args.args, common_filters, leaves)
            }
            _ => {
                if fe.func.name == "absent" || fe.func.name == "absent_over_time" {
                    return;
                }
                if let Some(index) = fe
                    .func
                    .arg_types
                    .iter()
                    .position(|&arg| arg != ValueType::Scalar && arg != ValueType::String)
                    && let Some(arg) = fe.args.args.get_mut(index)
                {
                    push_down_binary_op_filters_in_place_with(arg, common_filters, leaves);
                }
            }
        },
        Unary(unary) => {
            push_down_binary_op_filters_in_place_with(&mut unary.expr, common_filters, leaves);
        }
        Binary(bo) => {
            if let Some(modifier) = &bo.modifier {
                trim_filters_by_match_modifier(common_filters, &modifier.matching);
            }
            push_down_binary_op_filters_in_place_with(&mut bo.lhs, common_filters, leaves);
            push_down_binary_op_filters_in_place_with(&mut bo.rhs, common_filters, leaves);
        }
        Aggregate(aggr) => {
            // Grouping labels pass through an aggregation unchanged, so a filter
            // on one is equally true of the input series. A label the aggregation
            // *synthesizes* is not: `count_values` writes its value label onto the
            // output, and the input series carry no such label, so pushing a
            // filter on it into the selector would match nothing.
            if aggr.op.id() == T_COUNT_VALUES
                && let Some(label_name) = aggr.param.as_deref()
            {
                *common_filters = drop_label_filters_for_label_name(common_filters, label_name);
            }
            trim_filters_by_aggr_modifier(common_filters, aggr);
            push_down_binary_op_filters_in_place_with(&mut aggr.expr, common_filters, leaves);
            // `aggr.param` is a scalar or string (the `k` of topk, the quantile,
            // the count_values label) — never an operand of the binary op's label
            // matching. Rewriting a selector under it, as in `topk(scalar(x), y)`,
            // would change the parameter's value rather than prune series.
        }
        Paren(p) => push_down_binary_op_filters_in_place_with(&mut p.expr, common_filters, leaves),
        _ => {}
    }
}

fn pushdown_label_filters_for_label_replace(
    args: &mut [Box<Expr>],
    lfs: &mut Vec<Matcher>,
    leaves: &dyn LeafFilters,
) {
    if args.len() < 2 {
        return;
    }
    *lfs = drop_label_filters_for_label_name(lfs, &args[1]);
    if let Some(arg) = args.get_mut(0) {
        push_down_binary_op_filters_in_place_with(arg, lfs, leaves);
    }
}

#[inline]
fn get_label_filters_set(filters: &[Matcher]) -> FingerprintHashSet {
    let mut set: FingerprintHashSet = FingerprintHashSet::with_capacity(filters.len());
    for label in filters.iter() {
        let sig = label.fingerprint();
        set.insert(sig);
    }
    set
}

fn intersect_label_filters(first: Vec<Matcher>, second: Vec<Matcher>) -> Vec<Matcher> {
    if first.is_empty() || second.is_empty() {
        return vec![];
    }
    let set = get_label_filters_set(&first);
    let mut result = Vec::with_capacity(first.len());
    for matcher in second.into_iter() {
        let sig = matcher.fingerprint();
        if set.contains(&sig) {
            result.push(matcher);
        }
    }
    result
}

fn union_label_filters(first: Vec<Matcher>, second: Vec<Matcher>) -> Vec<Matcher> {
    if first.is_empty() {
        return second;
    }
    if second.is_empty() {
        return first;
    }

    let set: FingerprintHashSet = get_label_filters_set(&first);
    // reuse first to avoid allocations
    let mut result = first;
    for matcher in second.into_iter() {
        let signature = matcher.fingerprint();
        if !set.contains(&signature) {
            result.push(matcher);
        }
    }
    result
}

fn union_label_filters_internal(first: &mut Vec<Matcher>, second: &[Matcher]) {
    // use SmallVec here because generally the number of filters is small, and we want to avoid allocation
    let set: FingerprintHashSet = get_label_filters_set(first);
    for matcher in second.iter() {
        let sig = matcher.fingerprint();
        if !set.contains(&sig) {
            first.push(matcher.clone());
        }
    }
}

fn drop_label_filters_for_label_names<'a>(
    lfs: &[Matcher],
    label_names: impl Iterator<Item = &'a Expr>,
) -> Vec<Matcher> {
    if lfs.is_empty() {
        return vec![];
    }
    let mut names_set: SmallVec<[&str; 4]> = SmallVec::new();
    for label_name in label_names {
        if let Some(v) = get_expr_as_string(label_name) {
            names_set.push(v);
        }
    }
    lfs.iter()
        .filter(|x| !names_set.contains(&x.name.as_str()))
        .cloned()
        .collect()
}

fn drop_label_filters_for_label_name(lfs: &[Matcher], label_name: &Expr) -> Vec<Matcher> {
    let name = if let Some(v) = get_expr_as_string(label_name) {
        v
    } else {
        return vec![];
    };
    lfs.iter().filter(|x| !x.name.eq(name)).cloned().collect()
}

fn filter_label_filters_on(lfs: &mut Vec<Matcher>, args: &[String]) {
    if !args.is_empty() {
        let m: SmallVec<[&String; 8]> = args.iter().collect();
        lfs.retain(|x| m.contains(&&x.name))
    } else {
        lfs.clear()
    }
}

fn filter_label_filters_ignoring(lfs: &mut Vec<Matcher>, args: &[String]) {
    if !args.is_empty() {
        let m: SmallVec<[&String; 8]> = args.iter().collect();
        lfs.retain(|x| !m.contains(&&x.name));
    }
}

fn get_expr_as_string(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::StringLiteral(se) => Some(se.val.as_str()),
        _ => None,
    }
}
