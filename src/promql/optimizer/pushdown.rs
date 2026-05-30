use crate::common::constants::METRIC_NAME_LABEL;
use crate::promql::functions::{PromqlFunctionKind, resolve_function};
use crate::promql::hashers::{FingerprintHashSet, HasFingerprint};
use ahash::HashSetExt;
use promql_parser::label::{Matcher, Matchers};
use promql_parser::parser::token::{T_LOR, T_LUNLESS};
use promql_parser::parser::value::ValueType;
use promql_parser::parser::{AggregateExpr, Expr, LabelModifier, VectorMatchCardinality};
use smallvec::SmallVec;
use std::borrow::Cow;
use std::ops::Deref;
use std::vec::Vec;

/// `push_down_filters` optimizes e to improve its performance.
///
/// It performs the following optimizations:
///
/// - Adds missing filters to `foo{filters1} op bar{filters2}`
///   according to https://utcc.utoronto.ca/~cks/space/blog/sysadmin/PrometheusLabelNonOptimization
pub fn push_down_filters(expr: &Expr) -> Cow<'_, Expr> {
    if can_pushdown_filters(expr) {
        let mut clone = expr.clone();
        optimize_in_place(&mut clone);
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
        Unary(unary) => can_pushdown_filters(&unary.expr),
        Subquery(s) => can_pushdown_filters(&s.expr),
        _ => false,
    }
}

pub fn optimize_in_place(expr: &mut Expr) {
    use Expr::*;

    match expr {
        VectorSelector(vs) => {
            if vs.name.is_none()
                && let Some(pos) = vs.matchers.matchers.iter().position(|m| {
                    m.name == "__name__" && m.op == promql_parser::label::MatchOp::Equal
                })
            {
                let m = vs.matchers.matchers.remove(pos);
                vs.name = Some(m.value);
            }
        }
        Call(f) => {
            for arg in f.args.args.iter_mut() {
                optimize_in_place(arg);
            }
        }
        Aggregate(agg) => {
            optimize_in_place(&mut agg.expr);
            if let Some(param) = agg.param.as_mut() {
                optimize_in_place(param);
            }
        }
        Binary(be) => {
            optimize_in_place(&mut be.lhs);
            optimize_in_place(&mut be.rhs);
            let mut lfs = get_common_label_filters(expr);
            push_down_binary_op_filters_in_place(expr, &mut lfs);
        }
        Unary(unary) => optimize_in_place(&mut unary.expr),
        Paren(p) => optimize_in_place(&mut p.expr),
        Subquery(s) => optimize_in_place(&mut s.expr),
        _ => {}
    }
}

pub fn get_common_label_filters(e: &Expr) -> Vec<Matcher> {
    use Expr::*;

    match e {
        VectorSelector(m) => get_common_label_filters_without_metric_name(&m.matchers),
        Subquery(s) => get_common_label_filters(&s.expr),
        MatrixSelector(m) => get_common_label_filters_without_metric_name(&m.vs.matchers),
        Call(fe) => {
            if let Some(func) = resolve_function(fe.func.name) {
                let kind = func.kind();
                return match kind {
                    PromqlFunctionKind::LabelJoin | PromqlFunctionKind::LabelReplace => {
                        get_common_label_filters_for_label_replace(&fe.args.args)
                    }
                    PromqlFunctionKind::CountOverTime => {
                        get_common_label_filters_for_count_values_over_time(&fe.args.args)
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
                        get_common_label_filters(arg)
                    }
                };
            }
            vec![]
        }
        Aggregate(agg) => {
            let mut filters = get_common_label_filters(&agg.expr);
            trim_filters_by_aggr_modifier(&mut filters, agg);
            filters
        }
        Unary(unary) => get_common_label_filters(&unary.expr),
        Paren(p) => get_common_label_filters(&p.expr),
        Binary(binary) => {
            let mut lfs_left = get_common_label_filters(&binary.lhs);
            let mut lfs_right = get_common_label_filters(&binary.rhs);
            let card = VectorMatchCardinality::OneToOne;
            let group_modifier: Option<LabelModifier> = None;

            let (group_modifier, join_modifier) = if let Some(modifier) = &binary.modifier {
                (&modifier.matching, &modifier.card)
            } else {
                (&group_modifier, &card)
            };

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

fn intersect_label_filters_for_all_args(args: &[Expr]) -> Vec<Matcher> {
    if args.is_empty() {
        return vec![];
    }
    let mut lfs_a = get_common_label_filters(&args[0]);
    for arg in &args[1..] {
        let lfs_next = get_common_label_filters(arg);
        lfs_a = intersect_label_filters(lfs_a, lfs_next)
    }
    lfs_a
}

fn get_common_label_filters_for_count_values_over_time(args: &[Box<Expr>]) -> Vec<Matcher> {
    if args.len() != 2 {
        return vec![];
    }
    let lfs = get_common_label_filters(&args[1]);
    drop_label_filters_for_label_name(&lfs, &args[0])
}

fn get_common_label_filters_for_label_replace(args: &[Box<Expr>]) -> Vec<Matcher> {
    if args.len() < 2 {
        return vec![];
    }
    let lfs = get_common_label_filters(&args[0]);
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
        .filter(|&x| x.name != METRIC_NAME_LABEL)
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
    use Expr::*;

    if common_filters.is_empty() {
        return;
    }

    match e {
        VectorSelector(me) => {
            push_filters_to_matchers(&mut me.matchers, common_filters);
        }
        MatrixSelector(me) => {
            push_filters_to_matchers(&mut me.vs.matchers, common_filters);
        }
        Subquery(s) => push_down_binary_op_filters_in_place(&mut s.expr, common_filters),
        Call(fe) => match fe.func.name {
            "label_replace" | "label_join" => {
                pushdown_label_filters_for_label_replace(&mut fe.args.args, common_filters)
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
                    push_down_binary_op_filters_in_place(arg, common_filters);
                }
            }
        },
        Unary(unary) => {
            push_down_binary_op_filters_in_place(&mut unary.expr, common_filters);
        }
        Binary(bo) => {
            if let Some(modifier) = &bo.modifier {
                trim_filters_by_match_modifier(common_filters, &modifier.matching);
            }
            push_down_binary_op_filters_in_place(&mut bo.lhs, common_filters);
            push_down_binary_op_filters_in_place(&mut bo.rhs, common_filters);
        }
        Aggregate(aggr) => {
            trim_filters_by_aggr_modifier(common_filters, aggr);
            push_down_binary_op_filters_in_place(&mut aggr.expr, common_filters);
            if let Some(expr) = aggr.param.as_mut() {
                push_down_binary_op_filters_in_place(expr, common_filters);
            }
        }
        Paren(p) => push_down_binary_op_filters_in_place(&mut p.expr, common_filters),
        _ => {}
    }
}

fn pushdown_label_filters_for_all_args(lfs: &mut Vec<Matcher>, args: &mut [Box<Expr>]) {
    for arg in args {
        push_down_binary_op_filters_in_place(arg, lfs)
    }
}

fn pushdown_label_filters_for_count_values_over_time(args: &mut [Expr], lfs: &mut Vec<Matcher>) {
    if args.len() != 2 {
        return;
    }
    *lfs = drop_label_filters_for_label_name(lfs, &args[0]);
    push_down_binary_op_filters_in_place(&mut args[1], lfs);
}

fn pushdown_label_filters_for_label_replace(args: &mut [Box<Expr>], lfs: &mut Vec<Matcher>) {
    if args.len() < 2 {
        return;
    }
    *lfs = drop_label_filters_for_label_name(lfs, &args[1]);
    if let Some(arg) = args.get_mut(0) {
        push_down_binary_op_filters_in_place(arg, lfs);
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
    let mut names_set: SmallVec<&str, 4> = SmallVec::new();
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
        let m: SmallVec<&String, 8> = args.iter().collect();
        lfs.retain(|x| m.contains(&&x.name))
    } else {
        lfs.clear()
    }
}

fn filter_label_filters_ignoring(lfs: &mut Vec<Matcher>, args: &[String]) {
    if !args.is_empty() {
        let m: SmallVec<&String, 8> = args.iter().collect();
        lfs.retain(|x| !m.contains(&&x.name));
    }
}

fn get_expr_as_string(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::StringLiteral(se) => Some(se.val.as_str()),
        _ => None,
    }
}
