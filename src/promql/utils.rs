use crate::common::Timestamp;
use crate::promql::hashers::{HasFingerprint, SeriesFingerprint};
use crate::promql::{EvalResult, EvaluationError, QueryError, QueryValue};
use promql_parser::parser::{Expr, ParenExpr, UnaryExpr, VectorSelector};
use std::ops::{Bound, RangeBounds};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Convert a `RangeBounds<SystemTime>` into `(start: SystemTime, end: SystemTime)`.
///
/// `Excluded` bounds are adjusted by 1 ms — the smallest sample timestamp
/// granularity — so that `start..end` excludes the exact boundary timestamps.
pub(crate) fn range_bounds_to_system_time(
    range: impl RangeBounds<SystemTime>,
) -> (SystemTime, SystemTime) {
    let start = match range.start_bound() {
        Bound::Included(t) => *t,
        Bound::Excluded(t) => *t + Duration::from_millis(1),
        Bound::Unbounded => UNIX_EPOCH,
    };
    let end = match range.end_bound() {
        Bound::Included(t) => *t,
        Bound::Excluded(t) => t
            .checked_sub(Duration::from_millis(1))
            .unwrap_or(UNIX_EPOCH),
        Bound::Unbounded => UNIX_EPOCH + Duration::from_secs(i64::MAX as u64),
    };
    (start, end)
}

/// Convert a `RangeBounds<SystemTime>` into `(start_secs, end_secs)` as `i64`.
///
/// Returns an error if either bound resolves to a time before the Unix epoch.
/// Unbounded starts resolve to 0, unbounded ends resolve to `i64::MAX`.
pub(crate) fn range_bounds_to_secs(
    range: impl RangeBounds<SystemTime>,
) -> Result<(i64, i64), QueryError> {
    let (start, end) = range_bounds_to_system_time(range);
    let start_secs = start
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .map_err(|_| QueryError::InvalidQuery("start time is before Unix epoch".to_string()))?;
    let end_secs = end
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .map_err(|_| QueryError::InvalidQuery("end time is before Unix epoch".to_string()))?;
    Ok((start_secs, end_secs))
}

pub struct ExtractedSelector<'a> {
    pub vector_selector: &'a VectorSelector,
    pub selector: &'a Expr,
    pub parent: Option<&'a Expr>,
    pub hash: SeriesFingerprint,
    pub is_range_selector: bool,
    pub resolved_values: Option<QueryValue>,
    pub range: Option<Duration>,
    pub bounds: (Timestamp, Timestamp),
}

/// Extracts vector selectors from a PromQL expression
///
/// The goal is to facilitate optimizations such as parallel fetching of data for those metrics.
pub fn extract_selectors_from_expr<'a>(expr: &'a Expr, collector: &mut Vec<ExtractedSelector<'a>>) {
    fn extract_internal<'a>(
        prom_expr: &'a Expr,
        parent: Option<&'a Expr>,
        collector: &mut Vec<ExtractedSelector<'a>>,
    ) {
        match prom_expr {
            Expr::Aggregate(agg) => {
                extract_internal(&agg.expr, Some(prom_expr), collector);
                if let Some(expr) = &agg.param {
                    extract_internal(expr, Some(prom_expr), collector);
                }
            }
            Expr::Binary(be) => {
                extract_internal(&be.lhs, Some(prom_expr), collector);
                extract_internal(&be.rhs, Some(prom_expr), collector);
            }
            Expr::Call(call) => {
                call.args
                    .args
                    .iter()
                    .for_each(|expr| extract_internal(expr, Some(prom_expr), collector));
            }
            Expr::Paren(ParenExpr { expr }) => extract_internal(expr, Some(prom_expr), collector),
            Expr::Subquery(expr) => extract_internal(&expr.expr, Some(prom_expr), collector),
            Expr::Unary(UnaryExpr { expr }) => extract_internal(expr, Some(prom_expr), collector),
            Expr::VectorSelector(selector) => {
                let display = selector.to_string();
                let hash = display.fingerprint();
                // todo: if the hash is already in the list, we can skip adding it again.
                // This can happen when the same selector is used multiple times in the query, for example in a binary operation.
                let is_range_selector = matches!(
                    parent,
                    Some(Expr::Subquery(_)) | Some(Expr::MatrixSelector(_))
                );
                let range = if let Some(Expr::Subquery(sub)) = parent {
                    Some(sub.range)
                } else if let Some(Expr::MatrixSelector(parent)) = parent {
                    Some(parent.range)
                } else {
                    None
                };
                collector.push(ExtractedSelector {
                    vector_selector: selector,
                    selector: prom_expr,
                    parent,
                    hash,
                    is_range_selector,
                    resolved_values: None,
                    range,
                    bounds: (0, 0),
                });
            }
            _ => { /* do nothing for other expression types */ }
        }
    }

    extract_internal(expr, None, collector);
}

#[inline]
fn calc_points(start: Timestamp, end: Timestamp, step: &Duration) -> i64 {
    (end - start).saturating_div((step.as_millis() + 1) as i64)
}

/// The minimum number of points per timeseries for enabling time rounding.
/// This improves the cache hit ratio for frequently requested queries over
/// big time ranges.
const MIN_TIMESERIES_POINTS_FOR_TIME_ROUNDING: i64 = 50;

pub(in crate::promql) fn adjust_start_end(
    start: Timestamp,
    end: Timestamp,
    step: Duration,
) -> (Timestamp, Timestamp) {
    // if disableCache {
    //     // do not adjust start and end values when cache is disabled.
    //     // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/563
    //     return (start, end);
    // }
    let points = calc_points(start, end, &step);
    if points < MIN_TIMESERIES_POINTS_FOR_TIME_ROUNDING {
        // Too small a number of points for rounding.
        return (start, end);
    }

    // Round start and end to values divisible by step
    // to enable response caching (see EvalConfig.mayCache).
    let (start, end) = align_start_end(start, end, &step);

    // Make sure that the new number of points is the same as the initial number of points.
    let mut new_points = calc_points(start, end, &step);
    let mut _end = end;
    let _step = step.as_millis() as i64;
    while new_points > points {
        _end = end.saturating_sub(_step);
        new_points -= 1;
    }

    (start, _end)
}

pub(in crate::promql) fn align_start_end(
    start: Timestamp,
    end: Timestamp,
    step: &Duration,
) -> (Timestamp, Timestamp) {
    let step = step.as_millis() as i64;
    // Round start to the nearest smaller value divisible by step.
    let new_start = start - start % step;
    // Round end to the nearest bigger value divisible by step.
    let adjust = end % step;
    let mut new_end = end;
    if adjust > 0 {
        new_end += step - adjust
    }
    (new_start, new_end)
}

/// Checks the maximum number of points that may be returned per each time series.
///
/// The number mustn't exceed `max_points_per_timeseries`.
pub(crate) fn validate_max_points_per_timeseries(
    start: Timestamp,
    end: Timestamp,
    step: Duration,
    max_points_per_timeseries: usize,
) -> EvalResult<()> {
    let points = calc_points(start, end, &step);
    if (max_points_per_timeseries > 0) && points > max_points_per_timeseries as i64 {
        let msg = format!(
            "too many points for the given step={:?}, start={start} and end={end}: {points}; cannot exceed {}",
            step, max_points_per_timeseries
        );
        Err(EvaluationError::InternalError(msg))
    } else {
        Ok(())
    }
}
