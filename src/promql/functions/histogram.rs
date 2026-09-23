use std::cell::RefCell;
use std::rc::Rc;

use crate::labels::HasFingerprint;
use crate::promql::functions::utils::{
    exact_arity_error, expect_instant_vector, expect_scalar, is_inf,
};
use crate::promql::functions::{PromQLArg, PromQLFunction};
use crate::promql::hashers::FingerprintHashMap;
use crate::promql::model::is_stale_nan;
use crate::promql::{EvalContext, EvalResult, EvalSample, ExprResult};
use ahash::AHashMap;

static ELLIPSIS: &str = "...";
static LE: &str = "le";

#[derive(Copy, Clone)]
pub(in crate::promql) struct HistogramFractionFunctions;

impl PromQLFunction for HistogramFractionFunctions {
    fn apply(&self, _arg: PromQLArg, _ctx: &EvalContext) -> EvalResult<ExprResult> {
        Err(exact_arity_error("histogram_fraction", 3, 0))
    }

    fn apply_args(&self, args: Vec<PromQLArg>, _ctx: &EvalContext) -> EvalResult<ExprResult> {
        histogram_fraction(args)
    }
}

#[derive(Copy, Clone)]
pub(in crate::promql) struct HistogramQuantileFunction;

impl PromQLFunction for HistogramQuantileFunction {
    fn apply(&self, _arg: PromQLArg, _ctx: &EvalContext) -> EvalResult<ExprResult> {
        Err(exact_arity_error("histogram_quantile", 2, 0))
    }

    fn apply_args(&self, args: Vec<PromQLArg>, _ctx: &EvalContext) -> EvalResult<ExprResult> {
        histogram_quantile(args)
    }
}

type SharedTimeseries = Rc<RefCell<EvalSample>>;

/// Group timeseries by MetricGroup+tags excluding `vmrange` tag.
#[derive(Clone, Default)]
struct Bucket {
    start_str: String,
    end_str: String,
    start: f64,
    end: f64,
    ts: SharedTimeseries,
}

impl Bucket {
    fn is_set(&self) -> bool {
        !self.start_str.is_empty()
            || !self.end_str.is_empty() && self.start != 0.0 && self.end != 0.0
    }

    fn is_zero_ts(&self) -> bool {
        let ts = self.ts.borrow();
        ts.value <= 0.0
    }

    /// Convert the ` vmrange ` label in each group of time series to the ` le ` label.
    fn copy_ts(&self, le_str: &str) -> SharedTimeseries {
        let src = self.ts.borrow();
        let mut ts = src.clone();
        ts.value = 0.0;
        ts.labels.set(LE, le_str.to_string());
        Rc::new(RefCell::new(ts))
    }

    fn set_le(&mut self, end_str: &str) {
        let mut ts = self.ts.borrow_mut();
        ts.labels.set(LE, end_str.to_string());
    }

    pub fn pop_timeseries(&mut self) -> Option<EvalSample> {
        match self.ts.try_borrow_mut() {
            Ok(refcell) => {
                // you can do refcell.into_inner here
                Some(refcell.to_owned())
            }
            Err(_) => {
                // another Rc still exists, so you cannot take ownership of the value
                debug_assert!(false, "Cannot borrow mutably");
                None
            }
        }
    }
}

fn vmrange_buckets_to_le(tss: Vec<EvalSample>) -> Vec<EvalSample> {
    let mut rvs: Vec<EvalSample> = Vec::with_capacity(tss.len());

    let mut buckets: FingerprintHashMap<Vec<Bucket>> = FingerprintHashMap::default();

    let empty_str = "".to_string();

    for ts in tss.into_iter() {
        let vm_range = ts.labels.get("vmrange").unwrap_or(&empty_str);

        if vm_range.is_empty() {
            if let Some(le) = ts.labels.get(LE)
                && !le.is_empty()
            {
                // Keep Prometheus-compatible buckets.
                rvs.push(ts);
            }
            continue;
        }

        let n = match vm_range.find(ELLIPSIS) {
            Some(pos) => pos,
            None => continue,
        };

        let start_str = &vm_range[0..n];
        let start = match start_str.parse::<f64>() {
            Err(_) => continue,
            Ok(n) => n,
        };

        let end_str = &vm_range[(n + ELLIPSIS.len())..vm_range.len()];
        let end = match end_str.parse::<f64>() {
            Err(_) => continue,
            Ok(n) => n,
        };

        // prevent borrowing of value from ts.metric_name
        let start_string = start_str.to_string();
        let end_string = end_str.to_string();

        let mut _ts = ts;
        _ts.labels.remove(LE);
        _ts.labels.remove("vmrange");

        let key = _ts.labels.fingerprint();
        // series.push(_ts);
        let shared_ts = Rc::new(RefCell::new(_ts));

        buckets.entry(key).or_default().push(Bucket {
            start_str: start_string,
            end_str: end_string,
            start,
            end,
            ts: shared_ts,
        });
    }

    let default_bucket: Bucket = Default::default();

    let mut uniq_ts: AHashMap<String, SharedTimeseries> = AHashMap::with_capacity(8);

    for xss in buckets.values_mut() {
        xss.sort_by(|a, b| a.end.total_cmp(&b.end));
        let mut xss_new: Vec<Bucket> = Vec::with_capacity(xss.len() + 2);
        let mut xs_prev: &Bucket = &default_bucket;

        uniq_ts.clear();

        for xs in xss.iter_mut() {
            if xs.is_zero_ts() {
                // Skip time series with zeros. They are substituted by xss_new below.
                // Skip buckets with zero values - they will be merged into a single bucket
                // when the next non-zero bucket appears.

                // Do not store xs in xsPrev to properly create `le` time series
                // for zero buckets.
                // See https://github.com/VictoriaMetrics/VictoriaMetrics/pull/4021
                continue;
            }

            if xs.start != xs_prev.end {
                // There is a gap between the previous bucket and the current bucket,
                // or the previous bucket is skipped because it was zero.
                // Fill it with a time series with le=xs.start.
                if !uniq_ts.contains_key(&xs.start_str) {
                    let copy = xs.copy_ts(&xs.start_str);

                    uniq_ts.insert(xs.start_str.to_string(), xs.ts.clone());
                    xss_new.push(Bucket {
                        start_str: "".to_string(),
                        start: 0.0,
                        end_str: xs.start_str.clone(),
                        end: xs.start,
                        ts: copy,
                    });
                }
            }

            // ugly, but otherwise we get a borrow error if we do xs.set_le(&xs.end_str);
            let end_str = xs.end_str.clone();
            // Convert the current time series to a time series with le=xs.end
            xs.set_le(&end_str);

            if let Some(_shared_ts) = uniq_ts.get(&end_str) {
                // Cannot merge EvalSample with EvalSamples - skip merging here
                // The current implementation doesn't support merging at this stage
            } else {
                uniq_ts.insert(end_str, xs.ts.clone());
                xss_new.push(xs.clone());
            }

            xs_prev = xs;
        }

        if xs_prev.is_set() && !is_inf(xs_prev.end, 1) && !xs_prev.is_zero_ts() {
            let ts = xs_prev.copy_ts("+Inf");

            xss_new.push(Bucket {
                start_str: "".to_string(),
                end_str: "+Inf".to_string(),
                start: 0.0,
                end: f64::INFINITY,
                ts,
            })
        }

        *xss = xss_new;
        if xss.is_empty() {
            continue;
        }

        let mut count: f64 = 0.0;
        for xs in xss.iter_mut() {
            let mut ts = xs.ts.borrow_mut();
            let v = ts.value;
            if v > 0.0 {
                count += v
            }
            ts.value = count
        }

        for xs in xss.iter_mut() {
            if let Some(ts) = xs.pop_timeseries() {
                rvs.push(ts);
            }
        }
    }

    rvs
}

/// `histogram_fraction(lower, upper, buckets)`: the estimated fraction of each
/// classic histogram's observations between `lower` and `upper`, as
/// Prometheus' `funcHistogramFraction` computes it (see [`bucket_fraction`]).
fn histogram_fraction(args: Vec<PromQLArg>) -> EvalResult<ExprResult> {
    if args.len() != 3 {
        return Err(exact_arity_error("histogram_fraction", 3, args.len()));
    }

    let mut arg_iter = args.into_iter();
    let lower = expect_scalar(arg_iter.next().unwrap(), "histogram_fraction", "lower")?;
    let upper = expect_scalar(arg_iter.next().unwrap(), "histogram_fraction", "upper")?;
    let series = expect_instant_vector(arg_iter.next().unwrap(), "histogram_fraction")?;

    let rvs = classic_histograms(series)
        .map(|(mut sample, mut buckets)| {
            sample.value = bucket_fraction(lower, upper, &mut buckets);
            sample.drop_name = true;
            sample
        })
        .collect();

    Ok(ExprResult::InstantVector(rvs))
}

pub(super) fn histogram_quantile(args: Vec<PromQLArg>) -> EvalResult<ExprResult> {
    if args.len() != 2 {
        return Err(exact_arity_error("histogram_quantile", 2, args.len()));
    }

    let mut arg_iter = args.into_iter();
    let phi = expect_scalar(arg_iter.next().unwrap(), "histogram_quantile", "phi")?;
    let series = expect_instant_vector(arg_iter.next().unwrap(), "histogram_quantile")?;

    let rvs = classic_histograms(series)
        .map(|(mut sample, mut buckets)| {
            sample.value = bucket_quantile(phi, &mut buckets);
            sample.drop_name = true;
            sample
        })
        .collect();

    Ok(ExprResult::InstantVector(rvs))
}

/// The classic histograms in `series`, each as its output sample (the first
/// bucket's, without `le`) and its buckets.
///
/// One histogram per label set without `le`, the metric name included, as
/// Prometheus groups them: the name's drop is pending until the result is
/// rendered, so `a_bucket` and `b_bucket` stay separate histograms. A series
/// whose `le` does not parse as a float is not a bucket. VictoriaMetrics
/// `vmrange` buckets are converted to `le` buckets first; Prometheus-format
/// buckets pass through unchanged.
fn classic_histograms(
    series: Vec<EvalSample>,
) -> impl Iterator<Item = (EvalSample, Vec<ClassicBucket>)> {
    let mut histograms: FingerprintHashMap<(EvalSample, Vec<ClassicBucket>)> =
        FingerprintHashMap::default();
    for mut ts in vmrange_buckets_to_le(series) {
        let Some(upper_bound) = ts.labels.get(LE).and_then(|le| le.parse::<f64>().ok()) else {
            continue;
        };
        ts.labels.remove(LE);
        let bucket = ClassicBucket {
            upper_bound,
            count: ts.value,
        };
        histograms
            .entry(ts.labels.fingerprint())
            .or_insert_with(|| (ts, Vec::new()))
            .1
            .push(bucket);
    }
    histograms.into_iter().map(|(_, histogram)| histogram)
}

/// One bucket of a classic histogram: its `le` and its cumulative count.
#[derive(Clone, Copy, Debug)]
pub(super) struct ClassicBucket {
    pub upper_bound: f64,
    pub count: f64,
}

/// Relative differences between adjacent bucket counts below this are taken
/// for floating-point noise and ignored (Prometheus' `smallDeltaTolerance`).
const SMALL_DELTA_TOLERANCE: f64 = 1e-12;

/// The `q` quantile of a classic histogram, as Prometheus' `BucketQuantile`
/// (promql/quantile.go) computes it:
///
/// - `q` NaN is NaN, below 0 is -Inf, above 1 is +Inf — before the buckets are
///   looked at, so even an empty or incomplete histogram answers.
/// - Without a `+Inf` bucket, with fewer than two buckets or with no
///   observations, the answer is NaN.
/// - Buckets with the same bound are merged, and counts are made monotonic
///   (see [`ensure_monotonic_and_ignore_small_deltas`]).
/// - A rank in the `+Inf` bucket answers the highest finite bound. A rank in
///   a lowest bucket whose bound is at most 0 answers that bound. Otherwise
///   the value is interpolated linearly within the bucket, from the previous
///   bound — or from 0 for a lowest bucket above 0.
pub(super) fn bucket_quantile(q: f64, buckets: &mut Vec<ClassicBucket>) -> f64 {
    if q.is_nan() {
        return f64::NAN;
    }
    if q < 0.0 {
        return f64::NEG_INFINITY;
    }
    if q > 1.0 {
        return f64::INFINITY;
    }
    buckets.sort_by(|a, b| a.upper_bound.total_cmp(&b.upper_bound));
    if !buckets
        .last()
        .is_some_and(|b| b.upper_bound == f64::INFINITY)
    {
        return f64::NAN;
    }

    coalesce_buckets(buckets);
    ensure_monotonic_and_ignore_small_deltas(buckets, SMALL_DELTA_TOLERANCE);

    let n = buckets.len();
    if n < 2 {
        return f64::NAN;
    }
    let observations = buckets[n - 1].count;
    if observations == 0.0 {
        return f64::NAN;
    }
    let mut rank = q * observations;
    let b = go_sort_search(n - 1, |i| buckets[i].count >= rank);

    if b == n - 1 {
        return buckets[n - 2].upper_bound;
    }
    if b == 0 && buckets[0].upper_bound <= 0.0 {
        return buckets[0].upper_bound;
    }
    let bucket_end = buckets[b].upper_bound;
    let mut bucket_start = 0.0;
    let mut count = buckets[b].count;
    if b > 0 {
        bucket_start = buckets[b - 1].upper_bound;
        count -= buckets[b - 1].count;
        rank -= buckets[b - 1].count;
    }
    bucket_start + (bucket_end - bucket_start) * (rank / count)
}

/// Go's `sort.Search`: the smallest `i` in `[0, n)` for which `f(i)` holds,
/// or `n`, found by the same bisection. Kept exact rather than replaced with
/// a linear scan so a NaN count — which makes `f` non-monotonic — lands on
/// the same bucket as in Prometheus.
fn go_sort_search(n: usize, f: impl Fn(usize) -> bool) -> usize {
    let (mut i, mut j) = (0, n);
    while i < j {
        let h = (i + j) / 2;
        if !f(h) {
            i = h + 1;
        } else {
            j = h;
        }
    }
    i
}

/// Merge adjacent buckets with the same upper bound, summing their counts.
/// `buckets` must be sorted by bound.
fn coalesce_buckets(buckets: &mut Vec<ClassicBucket>) {
    buckets.dedup_by(|next, kept| {
        if next.upper_bound == kept.upper_bound {
            kept.count += next.count;
            true
        } else {
            false
        }
    });
}

/// Make counts non-decreasing with the bound, as a cumulative histogram's
/// must be. A difference within `tolerance` (relative) is floating-point noise
/// and is flattened in either direction; any other decrease is lifted to the
/// previous count. Neither moves the running reference count.
fn ensure_monotonic_and_ignore_small_deltas(buckets: &mut [ClassicBucket], tolerance: f64) {
    let Some(first) = buckets.first() else {
        return;
    };
    let mut prev = first.count;
    for bucket in buckets.iter_mut().skip(1) {
        let curr = bucket.count;
        if curr == prev {
            continue;
        }
        if almost_equal(prev, curr, tolerance) || curr < prev {
            bucket.count = prev;
            continue;
        }
        prev = curr;
    }
}

/// Prometheus' `almost.Equal`: equality within a relative `epsilon`, with two
/// NaNs equal and the stale marker equal only to itself.
fn almost_equal(a: f64, b: f64, epsilon: f64) -> bool {
    if is_stale_nan(a) || is_stale_nan(b) {
        return is_stale_nan(a) && is_stale_nan(b);
    }
    if a.is_nan() && b.is_nan() {
        return true;
    }
    if a == b {
        return true;
    }
    let abs_sum = a.abs() + b.abs();
    let diff = (a - b).abs();
    if a == 0.0 || b == 0.0 || abs_sum < f64::MIN_POSITIVE {
        return diff < epsilon * f64::MIN_POSITIVE;
    }
    diff / abs_sum.min(f64::MAX) < epsilon
}

/// The estimated fraction of a classic histogram's observations in
/// `[lower, upper]`, as Prometheus' `BucketFraction` (promql/quantile.go)
/// computes it:
///
/// - Without a `+Inf` bucket, with no observations, or with a NaN bound, NaN.
///   `lower >= upper` is 0, not an error.
/// - Buckets with the same bound are merged; counts are *not* made monotonic.
/// - Observations are assumed spread linearly within a bucket. The lowest
///   bucket starts at 0 when its bound is above 0 and at -Inf otherwise, and
///   an infinite-width bucket (the first when it starts at -Inf, and `+Inf`)
///   contributes nothing by interpolation: a bound inside it counts the whole
///   bucket, or none of it.
pub(super) fn bucket_fraction(lower: f64, upper: f64, buckets: &mut Vec<ClassicBucket>) -> f64 {
    buckets.sort_by(|a, b| a.upper_bound.total_cmp(&b.upper_bound));
    if !buckets
        .last()
        .is_some_and(|b| b.upper_bound == f64::INFINITY)
    {
        return f64::NAN;
    }
    coalesce_buckets(buckets);

    let count = buckets[buckets.len() - 1].count;
    if count == 0.0 || lower.is_nan() || upper.is_nan() {
        return f64::NAN;
    }
    if lower >= upper {
        return 0.0;
    }

    let mut rank = 0.0;
    let (mut lower_rank, mut upper_rank) = (0.0, 0.0);
    let (mut lower_set, mut upper_set) = (false, false);

    let mut lower_bound = if buckets[0].upper_bound <= 0.0 {
        f64::NEG_INFINITY
    } else {
        0.0
    };

    for (i, b) in buckets.iter().enumerate() {
        if i > 0 {
            lower_bound = buckets[i - 1].upper_bound;
        }
        let upper_bound = b.upper_bound;

        // `v` is finite. An infinite-width bucket is not interpolated: for a
        // +Inf upper bound the second term is 0 and the rank stays at the
        // previous bucket's, and a -Inf lower bound takes the whole bucket.
        let interpolate_linearly = |v: f64| -> f64 {
            if lower_bound == f64::NEG_INFINITY {
                return b.count;
            }
            rank + (b.count - rank) * (v - lower_bound) / (upper_bound - lower_bound)
        };

        if !lower_set && lower_bound >= lower {
            lower_rank = rank;
            lower_set = true;
        }
        if !upper_set && lower_bound >= upper {
            upper_rank = rank;
            upper_set = true;
        }
        if lower_set && upper_set {
            break;
        }
        if !lower_set && lower_bound < lower && upper_bound > lower {
            lower_rank = interpolate_linearly(lower);
            lower_set = true;
        }
        if !upper_set && lower_bound < upper && upper_bound > upper {
            upper_rank = interpolate_linearly(upper);
            upper_set = true;
        }
        if lower_set && upper_set {
            break;
        }
        rank = b.count;
    }
    if !lower_set || lower_rank > count {
        lower_rank = count;
    }
    if !upper_set || upper_rank > count {
        upper_rank = count;
    }

    (upper_rank - lower_rank) / count
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::labels::Labels;

    fn buckets(pairs: &[(f64, f64)]) -> Vec<ClassicBucket> {
        pairs
            .iter()
            .map(|&(upper_bound, count)| ClassicBucket { upper_bound, count })
            .collect()
    }

    const INF: f64 = f64::INFINITY;

    #[test]
    fn out_of_range_phi_answers_before_the_buckets_are_read() {
        // Even a histogram with no observations, or no buckets at all.
        let mut empty = buckets(&[(1.0, 0.0), (INF, 0.0)]);
        assert_eq!(bucket_quantile(1.5, &mut empty), INF);
        assert_eq!(bucket_quantile(-0.5, &mut Vec::new()), f64::NEG_INFINITY);
        assert!(bucket_quantile(f64::NAN, &mut empty).is_nan());
    }

    #[test]
    fn incomplete_or_empty_histograms_are_nan() {
        // No +Inf bucket.
        assert!(bucket_quantile(0.5, &mut buckets(&[(1.0, 5.0), (2.0, 10.0)])).is_nan());
        // Only the +Inf bucket.
        assert!(bucket_quantile(0.5, &mut buckets(&[(INF, 10.0)])).is_nan());
        // No observations.
        assert!(bucket_quantile(0.5, &mut buckets(&[(1.0, 0.0), (INF, 0.0)])).is_nan());
    }

    #[test]
    fn interpolates_within_the_bucket_holding_the_rank() {
        let mut h = buckets(&[(INF, 10.0), (1.0, 2.0), (2.0, 6.0)]); // unsorted on purpose
        // rank 4 falls in (1, 2], two of its four observations in.
        assert_eq!(bucket_quantile(0.4, &mut h), 1.5);
        // The lowest bucket interpolates from 0 when its bound is above 0.
        assert_eq!(bucket_quantile(0.1, &mut h), 0.5);
        // A rank in the +Inf bucket answers the highest finite bound.
        assert_eq!(bucket_quantile(0.9, &mut h), 2.0);
    }

    #[test]
    fn a_lowest_bucket_at_or_below_zero_answers_its_bound() {
        let mut h = buckets(&[(-1.0, 5.0), (INF, 10.0)]);
        assert_eq!(bucket_quantile(0.25, &mut h), -1.0);
    }

    #[test]
    fn same_bound_buckets_merge_and_counts_are_made_monotonic() {
        // Two `le="1"` series sum to 4; the dip at le=2 is lifted to 4.
        let mut h = buckets(&[(1.0, 1.0), (1.0, 3.0), (2.0, 3.0), (INF, 8.0)]);
        // rank 4 is reached at le=1 exactly.
        assert_eq!(bucket_quantile(0.5, &mut h), 1.0);
    }

    #[test]
    fn fraction_edge_cases_follow_prometheus() {
        let h = || buckets(&[(1.0, 2.0), (2.0, 6.0), (INF, 10.0)]);
        // lower >= upper is an empty range, not an error.
        assert_eq!(bucket_fraction(2.0, 2.0, &mut h()), 0.0);
        assert_eq!(bucket_fraction(3.0, 2.0, &mut h()), 0.0);
        // NaN bounds, no +Inf bucket, or no observations are NaN.
        assert!(bucket_fraction(f64::NAN, 1.0, &mut h()).is_nan());
        assert!(bucket_fraction(0.0, 1.0, &mut buckets(&[(1.0, 2.0), (2.0, 6.0)])).is_nan());
        assert!(bucket_fraction(0.0, 1.0, &mut buckets(&[(1.0, 0.0), (INF, 0.0)])).is_nan());
        // Linear within a bucket: (1, 2] holds 4 of 10; half of it is 0.2.
        assert_eq!(bucket_fraction(1.0, 1.5, &mut h()), 0.2);
        // The lowest bucket starts at 0 when its bound is above 0.
        assert_eq!(bucket_fraction(0.0, 0.5, &mut h()), 0.1);
        // The +Inf bucket is not interpolated: nothing above 2 is counted
        // for an upper bound inside it, all of it for +Inf itself.
        assert_eq!(bucket_fraction(2.0, 100.0, &mut h()), 0.0);
        assert_eq!(bucket_fraction(2.0, INF, &mut h()), 0.4);
    }

    #[test]
    fn a_lowest_bucket_at_or_below_zero_starts_at_minus_infinity() {
        // (-Inf, -1] holds 5 of 10 and is not interpolated: any bound inside
        // it takes the whole bucket.
        let h = || buckets(&[(-1.0, 5.0), (1.0, 8.0), (INF, 10.0)]);
        assert_eq!(bucket_fraction(f64::NEG_INFINITY, -2.0, &mut h()), 0.5);
        // (-1, 1] holds 3; from 0 to 1 is half of it.
        assert_eq!(bucket_fraction(0.0, 1.0, &mut h()), 0.15);
    }

    fn bucket(name: &str, le: &str, value: f64) -> EvalSample {
        EvalSample {
            timestamp_ms: 1000,
            value,
            labels: Labels::from_pairs(&[("__name__", name), ("job", "x"), ("le", le)]).into(),
            drop_name: false,
        }
    }

    #[test]
    fn histograms_are_grouped_by_name_too_and_bad_le_is_skipped() {
        let series = vec![
            bucket("a_bucket", "1", 2.0),
            bucket("a_bucket", "+Inf", 4.0),
            bucket("b_bucket", "1", 1.0),
            bucket("b_bucket", "+Inf", 4.0),
            // Not a float: not a bucket.
            bucket("a_bucket", "1kb", 100.0),
        ];
        let result = histogram_quantile(vec![
            PromQLArg::Scalar(0.25),
            PromQLArg::InstantVector(series),
        ])
        .unwrap();
        let ExprResult::InstantVector(mut samples) = result else {
            panic!("expected an instant vector");
        };
        samples.sort_by(|a, b| a.labels.cmp(&b.labels));
        assert_eq!(samples.len(), 2, "one result per histogram");
        // a: rank 1 of (0, 1] holding 2 → 0.5; b: rank 1 of (0, 1] holding 1 → 1.
        assert_eq!(samples[0].labels.get("__name__"), Some("a_bucket"));
        assert_eq!(samples[0].value, 0.5);
        assert_eq!(samples[1].labels.get("__name__"), Some("b_bucket"));
        assert_eq!(samples[1].value, 1.0);
        for s in &samples {
            assert!(
                s.drop_name,
                "the name is dropped when the result is rendered"
            );
            assert_eq!(s.labels.get("le"), None);
        }
    }
}
