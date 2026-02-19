use std::cell::RefCell;
use std::rc::Rc;

use crate::parser::number::parse_number;
use crate::promql::functions::utils::{
    exact_arity_error, expect_instant_vector, expect_scalar, expect_string, is_inf,
};
use crate::promql::functions::{PromQLArg, PromQLFunction};
use crate::promql::hashers::FingerprintHashMap;
use crate::promql::{EvalResult, EvalSample, EvaluationError, ExprResult};
use ahash::AHashMap;

static ELLIPSIS: &str = "...";
static LE: &str = "le";

#[derive(Copy, Clone)]
pub(in crate::promql) struct HistogramFractionFunctions;

impl PromQLFunction for HistogramFractionFunctions {
    fn apply(&self, _arg: PromQLArg, _eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        Err(exact_arity_error("histogram_fraction", 3, 0))
    }

    fn apply_args(&self, args: Vec<PromQLArg>, _eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        histogram_fraction(args)
    }
}

#[derive(Copy, Clone)]
pub(in crate::promql) struct HistogramQuantileFunction;

impl PromQLFunction for HistogramQuantileFunction {
    fn apply(&self, _arg: PromQLArg, _eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        Err(exact_arity_error("histogram_quantile", 2, 0))
    }

    fn apply_args(&self, args: Vec<PromQLArg>, _eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
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
        ts.labels.insert(LE, le_str.to_string());
        Rc::new(RefCell::new(ts))
    }

    fn set_le(&mut self, end_str: &str) {
        let mut ts = self.ts.borrow_mut();
        ts.labels.insert(LE, end_str.to_string());
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

        let key = _ts.labels.signature();
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

// histogram_fraction is a shortcut for `histogram_share(upperLe, buckets) - histogram_share(lowerLe, buckets)`;
// histogram_fraction(x, y) = histogram_fraction(-Inf, y) - histogram_fraction(-Inf, x) = histogram_share(y) - histogram_share(x).
// This function is supported by PromQL.
fn histogram_fraction(args: Vec<PromQLArg>) -> EvalResult<ExprResult> {
    if args.len() != 3 {
        return Err(exact_arity_error("histogram_fraction", 3, args.len()));
    }

    let mut arg_iter = args.into_iter();
    let lower_bound = arg_iter.next().unwrap();
    let upper_bound = arg_iter.next().unwrap();
    let vector = arg_iter.next().unwrap();

    let lower = expect_scalar(lower_bound, "histogram_fraction", "lower")?;
    let upper = expect_scalar(upper_bound, "histogram_fraction", "upper")?;
    if lower >= upper {
        return Err(EvaluationError::ArgumentError(format!(
            "lower le cannot be greater than upper le; got lower le: {lower}, upper le: {upper}"
        )));
    }

    let series = expect_instant_vector(vector, "histogram_fraction")?;

    // Convert buckets with `vmrange` labels to buckets with `le` labels.
    let mut tss = vmrange_buckets_to_le(series);

    // Group metrics by all tags excluding "le"
    let m = group_le_timeseries(&mut tss);

    let fraction = |lower_le: f64, upper_le: f64, xss: &mut [LeTimeseries]| -> f64 {
        if lower_le.is_nan() || upper_le.is_nan() || xss.is_empty() {
            return f64::NAN;
        }
        fix_broken_buckets(xss);
        let v_last: f64 = xss[xss.len() - 1].ts.value;
        // Define `share` as a small function that operates on the provided slice
        // to avoid capturing `xss` by the closure, which would make it FnOnce
        // and prohibit calling it multiple times.
        fn share_fn(le_req: f64, xss: &mut [LeTimeseries], v_last: f64) -> f64 {
            if le_req < 0.0 {
                return 0.0;
            }
            if le_req.is_infinite() && le_req.is_sign_positive() {
                return 1.0;
            }
            let mut v_prev: f64 = 0.0;
            let mut le_prev: f64 = 0.0;
            for xs in xss.iter() {
                let v = xs.ts.value;
                let le = xs.le;
                if le_req >= le {
                    v_prev = v;
                    le_prev = le;
                    continue;
                }
                // precondition: le_prev <= le_req < le
                let lower = v_prev / v_last;
                if le.is_infinite() && le.is_sign_positive() {
                    return lower;
                }
                if le_prev == le_req {
                    return lower;
                }
                let q = lower + (v - v_prev) / v_last * (le_req - le_prev) / (le - le_prev);
                return q;
            }
            1.0
        }
        share_fn(upper_le, xss, v_last) - share_fn(lower_le, xss, v_last)
    };

    let mut rvs: Vec<EvalSample> = Vec::with_capacity(m.len());
    for (_, mut xss) in m.into_iter() {
        xss.sort_by(|a, b| a.le.total_cmp(&b.le));

        xss = merge_same_le(&mut xss);
        let mut dst = xss[0].ts.clone();
        dst.value = fraction(lower, upper, &mut xss);
        rvs.push(dst);
    }
    Ok(ExprResult::InstantVector(rvs))
}

pub(super) fn histogram_quantile(args: Vec<PromQLArg>) -> EvalResult<ExprResult> {
    let arg_len = args.len();

    if arg_len != 2 {
        return Err(exact_arity_error("histogram_quantile", 2, args.len()));
    }

    let mut arg_iter = args.into_iter();
    let phi_arg = arg_iter.next().unwrap();
    let phi_arg = expect_scalar(phi_arg, "histogram_quantile", "phi")?;

    let series_arg = arg_iter.next().unwrap();
    // Convert buckets with `vmrange` labels to buckets with `le` labels.
    let series = expect_instant_vector(series_arg, "histogram_quantile")?;

    let mut tss = vmrange_buckets_to_le(series);

    // Parse bounds_label. See https://github.com/prometheus/prometheus/issues/5706 for details.
    let bounds_label = if let Some(bound_arg) = arg_iter.next() {
        expect_string(bound_arg, "histogram_quantile", "bounds_label")?
    } else {
        "".to_string()
    };

    // Group metrics by all tags excluding "le"
    let mut m = group_le_timeseries(&mut tss);

    // Calculate quantile for each group in m
    let last_non_inf = |_i: usize, xss: &[LeTimeseries]| -> f64 {
        if let Some(v) = xss.iter().rev().find(|x| x.le.is_finite()) {
            v.le
        } else {
            f64::NAN
        }
    };

    let quantile = |phi: f64, xss: &mut Vec<LeTimeseries>| -> (f64, f64, f64) {
        if phi.is_nan() {
            return (f64::NAN, f64::NAN, f64::NAN);
        }
        fix_broken_buckets(xss);
        let mut v_last: f64 = 0.0;
        if !xss.is_empty() {
            v_last = xss[xss.len() - 1].ts.value
        }
        if v_last == 0.0 {
            return (f64::NAN, f64::NAN, f64::NAN);
        }
        if phi < 0.0 {
            return (f64::NEG_INFINITY, f64::NEG_INFINITY, xss[0].ts.value);
        }
        if phi > 1.0 {
            return (f64::INFINITY, v_last, f64::INFINITY);
        }
        let v_req = v_last * phi;
        let mut v_prev: f64 = 0.0;
        let mut le_prev: f64 = 0.0;
        for xs in xss.iter() {
            let v = xs.ts.value;
            let le = xs.le;
            if v <= 0.0 {
                // Skip zero buckets.
                le_prev = le;
                continue;
            }
            if v < v_req {
                v_prev = v;
                le_prev = le;
                continue;
            }
            if is_inf(le, 0) {
                break;
            }
            if v == v_prev {
                return (le_prev, le_prev, v);
            }
            let vv = le_prev + (le - le_prev) * (v_req - v_prev) / (v - v_prev);
            return (vv, le_prev, le);
        }
        let vv = last_non_inf(0, xss);
        (vv, vv, f64::INFINITY)
    };

    let mut rvs: Vec<EvalSample> = Vec::with_capacity(m.len());
    for (_, xss) in m.iter_mut() {
        xss.sort_by(|a, b| a.le.total_cmp(&b.le));

        let mut xss = merge_same_le(xss);

        if xss.is_empty() {
            continue;
        }

        let (mut ts_lower, mut ts_upper) = if !bounds_label.is_empty() {
            let mut ts_lower = xss[0].ts.clone(); // todo: use take and clone instead of 2 clones ?
            ts_lower.labels.insert(&bounds_label, "lower".to_string());

            let mut ts_upper = xss[0].ts.clone();
            ts_upper.labels.insert(&bounds_label, "upper".to_string());
            (ts_lower, ts_upper)
        } else {
            (EvalSample::default(), EvalSample::default())
        };

        let (v, lower, upper) = quantile(phi_arg, &mut xss);
        xss[0].ts.value = v;
        if !bounds_label.is_empty() {
            ts_lower.value = lower;
            ts_upper.value = upper;
        }

        let mut dst: LeTimeseries = if xss.len() == 1 {
            xss.remove(0)
        } else {
            xss.swap_remove(0)
        };

        rvs.push(std::mem::take(&mut dst.ts));
        if !bounds_label.is_empty() {
            rvs.push(ts_lower);
            rvs.push(ts_upper);
        }
    }

    Ok(ExprResult::InstantVector(rvs))
}

#[derive(Default)]
pub(super) struct LeTimeseries {
    pub le: f64,
    pub ts: EvalSample,
}

fn group_le_timeseries(tss: &mut [EvalSample]) -> FingerprintHashMap<Vec<LeTimeseries>> {
    let mut m: FingerprintHashMap<Vec<LeTimeseries>> = FingerprintHashMap::default();

    for ts in tss.iter_mut() {
        if let Some(tag_value) = ts.labels.get(LE) {
            if tag_value.is_empty() {
                continue;
            }

            if let Ok(le) = parse_number(tag_value) {
                ts.labels.reset_metric_group();
                ts.labels.remove("le");
                let key = ts.labels.signature();

                m.entry(key).or_default().push(LeTimeseries {
                    le,
                    ts: std::mem::take(ts),
                });
            }
        }
    }

    m
}

pub(super) fn fix_broken_buckets(xss: &mut [LeTimeseries]) {
    // Buckets are already sorted by le, so their values must be in ascending order,
    // since the next bucket includes all the previous buckets.
    // If the next bucket has a lower value than the current bucket,
    // then the current bucket must be substituted with the next bucket value.
    // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/2819
    if xss.len() < 2 {
        return;
    }

    // Substitute upper bucket values with lower bucket values if the upper values are NaN
    // or are bigger than the lower bucket values.
    let mut v_next = xss[0].ts.value; // todo: check i to avoid panic
    for lts in xss.iter_mut().skip(1) {
        let v = lts.ts.value;
        if v.is_nan() || v_next > v {
            lts.ts.value = v_next;
        } else {
            v_next = v;
        }
    }
}

fn merge_same_le(xss: &mut [LeTimeseries]) -> Vec<LeTimeseries> {
    // Merge buckets with identical le values.
    // See https://github.com/VictoriaMetrics/VictoriaMetrics/pull/3225
    let mut prev_le = xss[0].le;
    let mut dst = Vec::with_capacity(xss.len());
    let mut iter = xss.iter_mut();
    let first = iter.next();
    if first.is_none() {
        return dst;
    }
    dst.push(std::mem::take(first.unwrap()));
    let mut dst_index = 0;

    for xs in iter {
        if xs.le != prev_le {
            prev_le = xs.le;
            dst.push(std::mem::take(xs));
            dst_index = dst.len() - 1;
            continue;
        }

        if let Some(dst) = dst.get_mut(dst_index) {
            dst.ts.value += xs.ts.value;
        }
    }
    dst
}
