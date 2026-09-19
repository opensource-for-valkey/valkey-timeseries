use crate::promql::binops::ensure_unique_labelsets;
use crate::promql::functions::types::{PromQLArg, PromQLFunction};
use crate::promql::functions::utils::{
    exact_arity_error, expect_exact_arg_count, expect_min_arg_count, expect_string,
    is_valid_label_name, min_arity_error,
};
use crate::promql::{EvalContext, EvalResult, EvalSample, EvaluationError, ExprResult};
use ahash::AHashMap;
use promql_parser::label::METRIC_NAME;
use regex::Regex;
use std::cell::RefCell;
use std::time::Instant;

struct CachedRegex {
    regex: Regex,
    created: Instant,
}

thread_local! {
    /// Compiled `label_replace` patterns, keyed by the anchored source.
    ///
    /// The regex argument reaches the function as a string evaluated afresh at
    /// every step, so a range query would otherwise compile the same pattern
    /// once per step — and compiling costs ~200x a match.
    ///
    /// The cache is per thread, not shared, on purpose. Range steps are
    /// evaluated in parallel, and a `Regex` shared across workers serialises
    /// them twice on every match: its cache pool has a lock-free path for one
    /// owner thread only, and every `captures` call clones the shared
    /// `GroupInfo` `Arc`. A per-thread compile (a clone would still share the
    /// `Arc`) measured 5x faster than any shared map at 8 workers x 100
    /// series; the one-time compile per worker is 20-35 µs.
    ///
    /// Patterns come from user queries, so the map is bounded: once it holds
    /// [`REGEX_CACHE_CAPACITY`] entries, the oldest tenth make room for the
    /// next pattern rather than the map growing with every distinct query.
    static REGEX_CACHE: RefCell<AHashMap<String, CachedRegex>> = RefCell::new(AHashMap::new());
}

const REGEX_CACHE_CAPACITY: usize = 64;

/// How many entries a full cache gives up at once: a tenth, rounded up so a
/// full cache always frees at least one slot.
const REGEX_CACHE_EVICT_COUNT: usize = REGEX_CACHE_CAPACITY.div_ceil(10);

/// Drops the [`REGEX_CACHE_EVICT_COUNT`] entries compiled longest ago.
fn evict_oldest(cache: &mut AHashMap<String, CachedRegex>) {
    let mut by_age: Vec<(Instant, &String)> = cache
        .iter()
        .map(|(pattern, entry)| (entry.created, pattern))
        .collect();
    let evict = REGEX_CACHE_EVICT_COUNT.min(by_age.len());
    if evict == 0 {
        return;
    }
    if evict < by_age.len() {
        by_age.select_nth_unstable_by_key(evict - 1, |(created, _)| *created);
    }
    let oldest: Vec<String> = by_age[..evict]
        .iter()
        .map(|(_, pattern)| (*pattern).clone())
        .collect();
    for pattern in oldest {
        cache.remove(&pattern);
    }
}

/// Runs `f` against the compiled, fully anchored form of `regex_src`,
/// compiling it on this thread's first sight of the pattern.
fn with_anchored_regex<T>(regex_src: &str, f: impl FnOnce(&Regex) -> T) -> EvalResult<T> {
    let anchored = format!("^(?s:{regex_src})$");
    REGEX_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if !cache.contains_key(&anchored) {
            let regex = Regex::new(&anchored)
                .map_err(|err| EvaluationError::InternalError(err.to_string()))?;
            if cache.len() >= REGEX_CACHE_CAPACITY {
                evict_oldest(&mut cache);
            }
            cache.insert(
                anchored.clone(),
                CachedRegex {
                    regex,
                    created: Instant::now(),
                },
            );
        }
        Ok(f(&cache[&anchored].regex))
    })
}

#[derive(Copy, Clone)]
pub(in crate::promql) struct LabelReplaceFunction;

fn apply_label_replace_to_samples(
    mut samples: Vec<EvalSample>,
    dst_label: &str,
    replacement: &str,
    src_label: &str,
    regex_src: &str,
) -> EvalResult<ExprResult> {
    if !is_valid_label_name(dst_label) {
        return Err(EvaluationError::InternalError(format!(
            "invalid label name {:?}",
            dst_label
        )));
    }

    with_anchored_regex(regex_src, |regex| {
        for sample in &mut samples {
            let src_value = sample.labels.get(src_label).unwrap_or_default();

            if let Some(captures) = regex.captures(src_value) {
                let mut replaced = String::new();
                captures.expand(replacement, &mut replaced);

                if replaced.is_empty() {
                    sample.labels.remove(dst_label);
                } else {
                    sample.labels.set(dst_label, replaced);
                }

                if dst_label == METRIC_NAME {
                    sample.drop_name = false;
                }
            }
        }
    })?;

    Ok(ExprResult::InstantVector(samples))
}

impl PromQLFunction for LabelReplaceFunction {
    fn apply(&self, _arg: PromQLArg, _ctx: &EvalContext) -> EvalResult<ExprResult> {
        Err(exact_arity_error("label_replace", 5, 1))
    }

    fn apply_args(&self, args: Vec<PromQLArg>, _ctx: &EvalContext) -> EvalResult<ExprResult> {
        expect_exact_arg_count("label_replace", 5, args.len())?;

        let mut args_iter = args.into_iter();
        let PromQLArg::InstantVector(samples) =
            args_iter.next().expect("validated args.len() == 5")
        else {
            return Err(EvaluationError::InternalError(
                "label_replace expects an instant vector as its first argument".to_string(),
            ));
        };

        let dst_label = expect_string(
            args_iter.next().expect("validated args.len() == 5"),
            "label_replace",
            "destination_label",
        )?;
        let replacement = expect_string(
            args_iter.next().expect("validated args.len() == 5"),
            "label_replace",
            "replacement",
        )?;
        let src_label = expect_string(
            args_iter.next().expect("validated args.len() == 5"),
            "label_replace",
            "source_label",
        )?;
        let regex_src = expect_string(
            args_iter.next().expect("validated args.len() == 5"),
            "label_replace",
            "regex",
        )?;

        let result = apply_label_replace_to_samples(
            samples,
            &dst_label,
            &replacement,
            &src_label,
            &regex_src,
        )?;
        let ExprResult::InstantVector(iv) = &result else {
            // should never occur!
            panic!("Invalid return value in LabelReplaceFunction");
        };
        ensure_unique_labelsets(iv)?;
        Ok(result)
    }
}

impl Default for LabelReplaceFunction {
    fn default() -> Self {
        LabelReplaceFunction
    }
}

/// `label_join(v instant-vector, dst_label string, separator string, src_label_1 string, src_label_2 string, ...)`
#[derive(Copy, Clone)]
pub(in crate::promql) struct LabelJoinFunction;

impl PromQLFunction for LabelJoinFunction {
    fn apply(&self, _arg: PromQLArg, _ctx: &EvalContext) -> EvalResult<ExprResult> {
        Err(min_arity_error("label_join", 3, 1))
    }

    fn apply_args(&self, args: Vec<PromQLArg>, _ctx: &EvalContext) -> EvalResult<ExprResult> {
        let actual_args = args.len();
        expect_min_arg_count("label_join", 3, actual_args)?;

        let mut args_iter = args.into_iter();
        let PromQLArg::InstantVector(mut samples) = args_iter
            .next()
            .expect("validated args.len() == ctx.raw_args.len() >= 3")
        else {
            return Err(EvaluationError::InternalError(
                "label_join expects an instant vector as its first argument".to_string(),
            ));
        };
        let dst_arg = args_iter
            .next()
            .expect("validated evaluated_args.len() == 4");
        let separator_arg = args_iter
            .next()
            .expect("validated evaluated_args.len() == 3");

        let dst_label = expect_string(dst_arg, "label_join", "destination_label")?;
        let separator = expect_string(separator_arg, "label_join", "separator")?;
        let src_labels = args_iter
            .map(|arg| expect_string(arg, "label_join", "source_label"))
            .collect::<EvalResult<Vec<_>>>()?;

        if !is_valid_label_name(&dst_label) {
            return Err(EvaluationError::InternalError(format!(
                "invalid label name {:?}",
                dst_label
            )));
        }

        for sample in &mut samples {
            let mut joined = String::new();
            for (index, src_label) in src_labels.iter().enumerate() {
                if index > 0 {
                    joined.push_str(&separator);
                }
                if let Some(value) = sample.labels.get(src_label) {
                    joined.push_str(value);
                }
            }

            if joined.is_empty() {
                sample.labels.remove(&dst_label);
            } else {
                sample.labels.set(&dst_label, joined);
            }

            if dst_label == METRIC_NAME {
                sample.drop_name = false;
            }
        }

        ensure_unique_labelsets(&samples)?;

        Ok(ExprResult::InstantVector(samples))
    }
}

impl Default for LabelJoinFunction {
    fn default() -> Self {
        LabelJoinFunction
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cached_patterns() -> Vec<String> {
        REGEX_CACHE.with(|cache| {
            let cache = cache.borrow();
            let mut entries: Vec<(Instant, &String)> = cache
                .iter()
                .map(|(pattern, entry)| (entry.created, pattern))
                .collect();
            entries.sort();
            entries.into_iter().map(|(_, p)| p.clone()).collect()
        })
    }

    #[test]
    fn should_evict_oldest_tenth_when_full() {
        // Thread-local state: this test's thread starts with an empty cache.
        REGEX_CACHE.with(|cache| cache.borrow_mut().clear());

        for i in 0..REGEX_CACHE_CAPACITY {
            with_anchored_regex(&format!("p{i}"), |_| ()).unwrap();
        }
        assert_eq!(cached_patterns().len(), REGEX_CACHE_CAPACITY);

        with_anchored_regex("overflow", |_| ()).unwrap();

        let remaining = cached_patterns();
        assert_eq!(
            remaining.len(),
            REGEX_CACHE_CAPACITY - REGEX_CACHE_EVICT_COUNT + 1
        );
        // The oldest tenth is gone, everything younger survived in order,
        // and the newcomer is the youngest entry.
        for i in 0..REGEX_CACHE_EVICT_COUNT {
            assert!(!remaining.contains(&format!("^(?s:p{i})$")));
        }
        let survivors: Vec<String> = (REGEX_CACHE_EVICT_COUNT..REGEX_CACHE_CAPACITY)
            .map(|i| format!("^(?s:p{i})$"))
            .chain(std::iter::once("^(?s:overflow)$".to_string()))
            .collect();
        assert_eq!(remaining, survivors);
    }

    #[test]
    fn should_reuse_cached_regex_for_same_pattern() {
        REGEX_CACHE.with(|cache| cache.borrow_mut().clear());
        let created = |pattern: &str| {
            with_anchored_regex(pattern, |_| ()).unwrap();
            REGEX_CACHE.with(|c| c.borrow()[&format!("^(?s:{pattern})$")].created)
        };
        let first = created("same");
        let second = created("same");
        assert_eq!(first, second, "a cache hit must not recompile");
        assert_eq!(cached_patterns().len(), 1);
    }
}
