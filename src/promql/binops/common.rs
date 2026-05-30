use crate::promql::optimizer::pushdown;
use crate::promql::{EvalResult, EvalSample, ExprResult};
use ahash::AHashSet;
use promql_parser::label::{METRIC_NAME, MatchOp, Matcher};
use promql_parser::parser::token::T_LOR;
use promql_parser::parser::{AggregateExpr, BinaryExpr, Expr, LabelModifier};
use regex::{Regex, escape};
use std::borrow::Cow;

pub(in crate::promql) fn push_down_filters<'a>(
    expr: &'a BinaryExpr,
    first: &ExprResult,
    dest: &'a Expr,
) -> EvalResult<Cow<'a, Expr>> {
    let ExprResult::InstantVector(samples) = first else {
        return Ok(Cow::Borrowed(dest));
    };
    let mut common_filters = get_common_label_filters(samples);
    if !common_filters.is_empty() {
        if let Some(modifier) = &expr.modifier {
            pushdown::trim_filters_by_match_modifier(&mut common_filters, &modifier.matching);
        }
        let mut copy = dest.clone();
        pushdown::push_down_binary_op_filters_in_place(&mut copy, &mut common_filters);
        return Ok(Cow::Owned(copy));
    }
    Ok(Cow::Borrowed(dest))
}

#[inline]
fn is_aggregate_non_grouping(agg: &AggregateExpr) -> bool {
    let Some(modifier) = &agg.modifier else {
        return false;
    };
    match modifier {
        LabelModifier::Include(args) => args.labels.is_empty(),
        LabelModifier::Exclude(args) => args.labels.is_empty(),
    }
}

pub(in crate::promql) fn can_push_down_common_filters(be: &BinaryExpr) -> bool {
    // When fill modifiers are present, all series from both sides must be considered
    // (the fill pass synthesizes results for series that have no match on the other side).
    // Pushing label filters would incorrectly exclude series that should be included via fill.
    if be
        .modifier
        .as_ref()
        .map(|m| m.fill_values.lhs.is_some() || m.fill_values.rhs.is_some())
        .unwrap_or(false)
    {
        return false;
    }

    be.op.id() != T_LOR
        && match (&be.lhs.as_ref(), &be.rhs.as_ref()) {
            (Expr::Aggregate(left), Expr::Aggregate(right)) => {
                !(is_aggregate_non_grouping(left) || is_aggregate_non_grouping(right))
            }
            (Expr::StringLiteral(_), _) => false,
            (_, Expr::StringLiteral(_)) => false,
            (Expr::NumberLiteral(_), _) => false,
            (_, Expr::NumberLiteral(_)) => false,
            _ => true,
        }
}

pub(in crate::promql) fn get_common_label_filters(samples: &[EvalSample]) -> Vec<Matcher> {
    let mut kv_map: halfbrown::HashMap<&String, AHashSet<&str>> = halfbrown::HashMap::new();
    for ts in samples.iter() {
        for label in ts.labels.iter() {
            // Never push down __name__: binary-op matching always ignores __name__ by default
            // (unless an explicit `on(__name__)` modifier is used). Pushing it down would
            // incorrectly filter out the other side when the two sides have different metric names
            // (e.g. `cpu_usage + memory_bytes`).
            if label.name == METRIC_NAME {
                continue;
            }
            kv_map.entry(&label.name).or_default().insert(&label.value);
        }
    }

    let mut lfs: Vec<Matcher> = Vec::with_capacity(kv_map.len());
    for (key, values) in kv_map {
        if values.len() != samples.len() {
            // Skip the tag, since it doesn't belong to all the time series.
            continue;
        }

        if values.len() > 60 {
            // Skip the filter on the given tag, since it needs to enumerate too many unique values.
            // This may slow down the provider for matching time series.
            continue;
        }

        let lf = if values.len() == 1 {
            // Safety: length checked above.
            let val = *values.iter().next().unwrap();
            Matcher::new(MatchOp::Equal, key.as_str(), val)
        } else {
            let str_value = join_regexp_values(values);
            // Safety: the regex is an alternation generated from the values, so it should be valid.
            let regex = Regex::new(&str_value).unwrap();
            Matcher::new(MatchOp::Re(regex), key.as_str(), str_value.as_str())
        };

        lfs.push(lf);
    }

    lfs
}

fn join_regexp_values(values: AHashSet<&str>) -> String {
    let len = values.len();
    let init_size = values.iter().fold(0, |res, &x| res + x.len() + 3);
    let mut res = String::with_capacity(init_size);
    for (i, &s) in values.iter().enumerate() {
        let s_quoted = escape(s);
        res.push_str(s_quoted.as_str());
        if i < len - 1 {
            res.push('|')
        }
    }
    res
}
