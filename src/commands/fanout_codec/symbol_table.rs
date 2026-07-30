//! Response-level label interning for MRANGE cluster fanout.
//!
//! Real `TS.MRANGE` responses repeat a small universe of label names (and
//! often values) across every matched series. [`intern_labels`] rewrites each
//! series' `labels: Vec<Label>` into a pair of index arrays
//! (`label_name_refs` / `label_value_refs`) against two dictionaries shared by
//! the whole `MultiRangeResponse`; [`resolve_labels`] is the coordinator-side
//! inverse.
//!
//! [`resolve_labels`] is safe to call on any response without the caller first 
//! establishing that it was interned. It rewrites `labels` only for series that 
//! actually carry refs, so a series that has none passes through untouched instead 
//! of being emptied. Note that within one response version that guard changes no 
//! outcome (a label-less series interns to empty refs and its `labels` is already empty);
//! it is there so the function cannot be misused into silently discarding labels.
//!
//! `resolve_labels` runs on peer-controlled input, so it returns `Err` on any
//! malformed index or ragged ref-array pair rather than panicking (see the
//! `rdb_load_len` rationale in `src/common/rdb.rs` for why fanout/RDB input is
//! treated as untrusted throughout this crate).

use super::generated::{Label, SeriesRangeResponse};
use std::collections::HashMap;
use valkey_module::{ValkeyError, ValkeyResult};

/// Rewrites every series' `labels` into indices against two per-response
/// dictionaries (returned as `(names, values)`), clearing `labels` in the
/// process. Attach the returned dictionaries to
/// `MultiRangeResponse::symbol_table_names` / `symbol_table_values`.
///
/// A series with no labels gets empty ref arrays, which [`resolve_labels`]
/// reads as "nothing to resolve" — so the round trip is still correct without
/// needing a separate marker for the empty case.
pub fn intern_labels(series: &mut [SeriesRangeResponse]) -> (Vec<String>, Vec<String>) {
    let mut names: Vec<String> = Vec::new();
    let mut name_ids: HashMap<String, u32> = HashMap::new();
    let mut values: Vec<String> = Vec::new();
    let mut value_ids: HashMap<String, u32> = HashMap::new();

    for s in series.iter_mut() {
        let labels = std::mem::take(&mut s.labels);
        let mut name_refs = Vec::with_capacity(labels.len());
        let mut value_refs = Vec::with_capacity(labels.len());
        for label in labels {
            let name_idx = match name_ids.get(label.name.as_str()) {
                Some(&idx) => idx,
                None => {
                    let idx = names.len() as u32;
                    name_ids.insert(label.name.clone(), idx);
                    names.push(label.name);
                    idx
                }
            };
            let value_idx = match value_ids.get(label.value.as_str()) {
                Some(&idx) => idx,
                None => {
                    let idx = values.len() as u32;
                    value_ids.insert(label.value.clone(), idx);
                    values.push(label.value);
                    idx
                }
            };
            name_refs.push(name_idx);
            value_refs.push(value_idx);
        }
        s.label_name_refs = name_refs;
        s.label_value_refs = value_refs;
    }

    (names, values)
}

/// Inverse of [`intern_labels`]: resolves each series' `label_name_refs` /
/// `label_value_refs` against the response-level dictionaries back into a
/// concrete `labels: Vec<Label>`, clearing the ref arrays in the process.
///
/// Total: safe to call on any response without first checking whether it was
/// interned. A series whose ref arrays are both empty is left exactly as
/// received, `labels` included, rather than being emptied.
///
/// Peer-controlled: a ref-array length mismatch or an out-of-range index is a
/// malformed response and is rejected with `Err`, never indexed directly.
pub fn resolve_labels(
    series: &mut [SeriesRangeResponse],
    names: &[String],
    values: &[String],
) -> ValkeyResult<()> {
    for s in series.iter_mut() {
        let name_refs = std::mem::take(&mut s.label_name_refs);
        let value_refs = std::mem::take(&mut s.label_value_refs);
        // Nothing to resolve. Returning early rather than assigning an empty
        // vec keeps this function from discarding `labels` when it is handed a
        // response that was never interned.
        if name_refs.is_empty() && value_refs.is_empty() {
            continue;
        }
        if name_refs.len() != value_refs.len() {
            return Err(ValkeyError::String(format!(
                "TSDB: malformed symbol-table response: series '{}' has {} label name refs but {} value refs",
                s.key,
                name_refs.len(),
                value_refs.len()
            )));
        }

        let mut labels = Vec::with_capacity(name_refs.len());
        for (name_idx, value_idx) in name_refs.into_iter().zip(value_refs) {
            let name = names.get(name_idx as usize).ok_or_else(|| {
                ValkeyError::String(format!(
                    "TSDB: malformed symbol-table response: series '{}' label name ref {name_idx} out of range ({} names)",
                    s.key,
                    names.len()
                ))
            })?;
            let value = values.get(value_idx as usize).ok_or_else(|| {
                ValkeyError::String(format!(
                    "TSDB: malformed symbol-table response: series '{}' label value ref {value_idx} out of range ({} values)",
                    s.key,
                    values.len()
                ))
            })?;
            labels.push(Label {
                name: name.clone(),
                value: value.clone(),
            });
        }
        s.labels = labels;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn series(key: &str, labels: Vec<(&str, &str)>) -> SeriesRangeResponse {
        SeriesRangeResponse {
            key: key.into(),
            group_label_value: String::new(),
            labels: labels
                .into_iter()
                .map(|(name, value)| Label {
                    name: name.into(),
                    value: value.into(),
                })
                .collect(),
            columns: Vec::new(),
            label_name_refs: Vec::new(),
            label_value_refs: Vec::new(),
        }
    }

    #[test]
    fn intern_then_resolve_roundtrips() {
        let mut batch = vec![
            series(
                "a",
                vec![("region", "us-east-1"), ("env", "prod"), ("job", "api")],
            ),
            series(
                "b",
                vec![("region", "us-east-1"), ("env", "staging"), ("job", "api")],
            ),
            series("c", vec![]),
        ];
        let original = batch.clone();

        let (names, values) = intern_labels(&mut batch);
        // Shared labels across series a/b must collapse to one dictionary
        // entry each: region, env, job (names) and us-east-1, prod, api,
        // staging (values) — not 6 and 6.
        assert_eq!(names.len(), 3, "names: {names:?}");
        assert_eq!(values.len(), 4, "values: {values:?}");
        for s in &batch {
            assert!(s.labels.is_empty());
        }

        resolve_labels(&mut batch, &names, &values).expect("resolve");
        for (got, want) in batch.iter().zip(&original) {
            assert_eq!(got.labels, want.labels, "series '{}'", want.key);
            assert!(got.label_name_refs.is_empty());
            assert!(got.label_value_refs.is_empty());
        }
    }

    #[test]
    fn empty_batch_produces_empty_tables() {
        let mut batch: Vec<SeriesRangeResponse> = Vec::new();
        let (names, values) = intern_labels(&mut batch);
        assert!(names.is_empty());
        assert!(values.is_empty());
    }

    /// `resolve_labels` is total: handed a series that carries `labels`
    /// directly and no refs, it must leave them alone rather than emptying
    /// them. Pins the property that makes the function safe to call
    /// unconditionally.
    #[test]
    fn resolve_leaves_uninterned_labels_untouched() {
        let mut batch = vec![series("a", vec![("region", "us-east-1"), ("env", "prod")])];
        let expected = batch[0].labels.clone();

        // No dictionaries at all.
        resolve_labels(&mut batch, &[], &[]).expect("resolve");
        assert_eq!(batch[0].labels, expected);
    }

    /// A label-less series interns to empty refs, the one shape that is
    /// ambiguous in proto3 (empty and absent repeated fields are the same
    /// bytes). It must round-trip as label-less rather than being rejected.
    #[test]
    fn label_less_series_roundtrips() {
        let mut batch = vec![series("a", vec![])];
        let (names, values) = intern_labels(&mut batch);
        resolve_labels(&mut batch, &names, &values).expect("resolve");
        assert!(batch[0].labels.is_empty());
    }

    /// Only *one* empty ref array is a ragged pair, not an uninterned series,
    /// so it must still be rejected rather than silently skipped.
    #[test]
    fn half_empty_ref_arrays_rejected() {
        let mut batch = vec![SeriesRangeResponse {
            label_name_refs: Vec::new(),
            label_value_refs: vec![0],
            ..series("a", vec![])
        }];
        let err = resolve_labels(&mut batch, &["region".into()], &["us-east-1".into()])
            .expect_err("half-empty ref pair must be rejected");
        assert!(err.to_string().contains("series 'a'"), "{err}");
    }

    #[test]
    fn mismatched_ref_array_lengths_rejected() {
        let mut batch = vec![SeriesRangeResponse {
            label_name_refs: vec![0, 1],
            label_value_refs: vec![0],
            ..series("a", vec![])
        }];
        let err = resolve_labels(&mut batch, &["region".into()], &["us-east-1".into()])
            .expect_err("ragged ref arrays must be rejected");
        let msg = err.to_string();
        assert!(msg.contains("series 'a'"), "{msg}");
        assert!(msg.contains("2 label name refs"), "{msg}");
        assert!(msg.contains("1 value refs"), "{msg}");
    }

    #[test]
    fn out_of_range_name_ref_rejected() {
        let mut batch = vec![SeriesRangeResponse {
            label_name_refs: vec![5],
            label_value_refs: vec![0],
            ..series("a", vec![])
        }];
        let err = resolve_labels(&mut batch, &["region".into()], &["us-east-1".into()])
            .expect_err("out-of-range name ref must be rejected");
        let msg = err.to_string();
        assert!(msg.contains("series 'a'"), "{msg}");
        assert!(msg.contains("name ref 5"), "{msg}");
    }

    #[test]
    fn out_of_range_value_ref_rejected() {
        let mut batch = vec![SeriesRangeResponse {
            label_name_refs: vec![0],
            label_value_refs: vec![7],
            ..series("a", vec![])
        }];
        let err = resolve_labels(&mut batch, &["region".into()], &["us-east-1".into()])
            .expect_err("out-of-range value ref must be rejected");
        let msg = err.to_string();
        assert!(msg.contains("series 'a'"), "{msg}");
        assert!(msg.contains("value ref 7"), "{msg}");
    }
}
