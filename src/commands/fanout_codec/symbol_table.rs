//! Response-level label interning for MRANGE cluster fanout.
//!
//! Real `TS.MRANGE` responses repeat a small universe of label names (and
//! often values) across every matched series. [`intern_labels`] rewrites each
//! series' `labels: Vec<Label>` into `SymbolTableRef` entries against two
//! dictionaries shared by
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
//! malformed index rather than panicking (see the
//! `rdb_load_len` rationale in `src/common/rdb.rs` for why fanout/RDB input is
//! treated as untrusted throughout this crate).

use super::generated::{Label, SeriesRangeResponse, SymbolTable, SymbolTableRef};
use crate::common::context::key_for_display;

use crate::promql::generated::InstantSample;
use crate::promql::{EvalLabels, EvalSample, SplitLabel};
use std::collections::HashMap;
use valkey_module::{ValkeyError, ValkeyResult};

/// Rewrites every series' `labels` into indices against a per-response
/// [`SymbolTable`], clearing `labels` in the process.
///
/// A series with no labels gets an empty ref list, which [`resolve_labels`]
/// reads as "nothing to resolve" — so the round trip is still correct without
/// needing a separate marker for the empty case.
pub trait SymbolTableRefs {
    fn take_labels(&mut self) -> Vec<Label>;
    fn set_label_refs(&mut self, refs: Vec<SymbolTableRef>);
    fn take_label_refs(&mut self) -> Vec<SymbolTableRef>;
    fn set_labels(&mut self, labels: Vec<Label>);
    fn key_for_display(&self) -> String;
}

impl SymbolTableRefs for SeriesRangeResponse {
    fn take_labels(&mut self) -> Vec<Label> {
        std::mem::take(&mut self.labels)
    }

    fn set_label_refs(&mut self, refs: Vec<SymbolTableRef>) {
        self.label_refs = refs;
    }

    fn take_label_refs(&mut self) -> Vec<SymbolTableRef> {
        std::mem::take(&mut self.label_refs)
    }

    fn set_labels(&mut self, labels: Vec<Label>) {
        self.labels = labels;
    }

    fn key_for_display(&self) -> String {
        key_for_display(&self.key).into_owned()
    }
}

impl SymbolTableRefs for InstantSample {
    fn take_labels(&mut self) -> Vec<Label> {
        std::mem::take(&mut self.labels)
    }

    fn set_label_refs(&mut self, refs: Vec<SymbolTableRef>) {
        self.label_refs = refs;
    }

    fn take_label_refs(&mut self) -> Vec<SymbolTableRef> {
        std::mem::take(&mut self.label_refs)
    }

    fn set_labels(&mut self, labels: Vec<Label>) {
        self.labels = labels;
    }

    fn key_for_display(&self) -> String {
        self.key.clone()
    }
}

pub fn intern_labels<T: SymbolTableRefs>(series: &mut [T]) -> SymbolTable {
    let mut table = SymbolTable::default();
    let mut name_ids: HashMap<String, u32> = HashMap::new();
    let mut value_ids: HashMap<String, u32> = HashMap::new();

    for s in series.iter_mut() {
        let labels = s.take_labels();
        let mut refs = Vec::with_capacity(labels.len());
        for label in labels {
            let name_idx = match name_ids.get(label.name.as_str()) {
                Some(&idx) => idx,
                None => {
                    let idx = table.names.len() as u32;
                    name_ids.insert(label.name.clone(), idx);
                    table.names.push(label.name);
                    idx
                }
            };
            let value_idx = match value_ids.get(label.value.as_str()) {
                Some(&idx) => idx,
                None => {
                    let idx = table.values.len() as u32;
                    value_ids.insert(label.value.clone(), idx);
                    table.values.push(label.value);
                    idx
                }
            };
            refs.push(SymbolTableRef {
                name: name_idx,
                value: value_idx,
            });
        }
        s.set_label_refs(refs);
    }

    table
}

/// Inverse of [`intern_labels`]: resolves each series' `label_refs` against the
/// response-level dictionaries back into a
/// concrete `labels: Vec<Label>`, clearing the ref arrays in the process.
///
/// Total: safe to call on any response without first checking whether it was
/// interned. A series whose ref arrays are both empty is left exactly as
/// received, `labels` included, rather than being emptied.
///
/// Peer-controlled: an out-of-range index is a malformed response and is
/// rejected with `Err`, never indexed directly.
pub fn resolve_labels<T: SymbolTableRefs>(
    series: &mut [T],
    table: &SymbolTable,
) -> ValkeyResult<()> {
    for s in series.iter_mut() {
        let refs = s.take_label_refs();
        // Nothing to resolve. Returning early rather than assigning an empty
        // vec keeps this function from discarding `labels` when it is handed a
        // response that was never interned.
        if refs.is_empty() {
            continue;
        }

        let mut labels = Vec::with_capacity(refs.len());
        for symbol_ref in refs {
            let name = table.names.get(symbol_ref.name as usize).ok_or_else(|| {
                ValkeyError::String(format!(
                    "TSDB: malformed symbol-table response: series '{}' label name ref {} out of range ({} names)",
                    s.key_for_display(),
                    symbol_ref.name,
                    table.names.len()
                ))
            })?;
            let value = table.values.get(symbol_ref.value as usize).ok_or_else(|| {
                ValkeyError::String(format!(
                    "TSDB: malformed symbol-table response: series '{}' label value ref {} out of range ({} values)",
                    s.key_for_display(),
                    symbol_ref.value,
                    table.values.len()
                ))
            })?;
            labels.push(Label {
                name: name.clone(),
                value: value.clone(),
            });
        }
        // The concrete response type owns the labels; resolution is provided
        // by the type-specific caller after validating the references.
        s.set_labels(labels);
    }
    Ok(())
}

/// Coordinator-side resolution of a PromQL response straight into evaluator
/// labels, skipping the owned `Label` strings [`resolve_labels`] rebuilds.
///
/// Each distinct `(name, value)` pair is interned once per response as a
/// [`SplitLabel`] (the form local series already carry) and every sample that
/// references it takes a refcount bump, so a 500-series instant response costs
/// ~500 interned strings instead of 4 000 `String` clones. A sample that
/// carries inline labels instead of refs (a peer that did not intern) goes
/// through the ordinary owned conversion.
///
/// Peer-controlled input: an out-of-range ref is rejected with `Err`, never
/// indexed directly, exactly as in [`resolve_labels`].
pub struct EvalLabelResolver<'a> {
    table: &'a SymbolTable,
    pairs: HashMap<u64, SplitLabel, ahash::RandomState>,
}

impl<'a> EvalLabelResolver<'a> {
    pub fn new(table: &'a SymbolTable) -> Self {
        Self {
            table,
            pairs: HashMap::with_capacity_and_hasher(
                table.values.len(),
                ahash::RandomState::default(),
            ),
        }
    }

    /// The evaluator labels for one interned element, consuming its refs (or
    /// its inline labels when it has none).
    pub fn resolve<T: SymbolTableRefs>(&mut self, s: &mut T) -> ValkeyResult<EvalLabels> {
        let refs = s.take_label_refs();
        if refs.is_empty() {
            let labels = s.take_labels().into_iter().map(crate::Label::from).collect();
            return Ok(EvalLabels::shared(labels));
        }
        let mut split = Vec::with_capacity(refs.len());
        for symbol_ref in refs {
            let key = (u64::from(symbol_ref.name) << 32) | u64::from(symbol_ref.value);
            let label = match self.pairs.get(&key) {
                Some(label) => label.clone(),
                None => {
                    let name = self.table.names.get(symbol_ref.name as usize).ok_or_else(|| {
                        ValkeyError::String(format!(
                            "TSDB: malformed symbol-table response: series '{}' label name ref {} out of range ({} names)",
                            s.key_for_display(),
                            symbol_ref.name,
                            self.table.names.len()
                        ))
                    })?;
                    let value = self.table.values.get(symbol_ref.value as usize).ok_or_else(|| {
                        ValkeyError::String(format!(
                            "TSDB: malformed symbol-table response: series '{}' label value ref {} out of range ({} values)",
                            s.key_for_display(),
                            symbol_ref.value,
                            self.table.values.len()
                        ))
                    })?;
                    let label = SplitLabel::new(name, value);
                    self.pairs.insert(key, label.clone());
                    label
                }
            };
            split.push(label);
        }
        Ok(EvalLabels::from_split(split))
    }

    /// [`Self::resolve`] for an instant sample, producing the evaluator sample.
    pub fn resolve_sample(&mut self, mut sample: InstantSample) -> ValkeyResult<EvalSample> {
        let labels = self.resolve(&mut sample)?;
        Ok(EvalSample {
            timestamp_ms: sample.timestamp,
            value: sample.value,
            labels,
            drop_name: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::labels::HasFingerprint;

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
            label_refs: Vec::new(),
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

        let table = intern_labels(&mut batch);
        // Shared labels across series a/b must collapse to one dictionary
        // entry each: region, env, job (names) and us-east-1, prod, api,
        // staging (values) — not 6 and 6.
        assert_eq!(table.names.len(), 3, "names: {:?}", table.names);
        assert_eq!(table.values.len(), 4, "values: {:?}", table.values);
        for s in &batch {
            assert!(s.labels.is_empty());
        }

        resolve_labels(&mut batch, &table).expect("resolve");
        for (got, want) in batch.iter().zip(&original) {
            assert_eq!(
                got.labels,
                want.labels,
                "series '{}'",
                key_for_display(&want.key)
            );
            assert!(got.label_refs.is_empty());
        }
    }

    #[test]
    fn empty_batch_produces_empty_tables() {
        let mut batch: Vec<SeriesRangeResponse> = Vec::new();
        let table = intern_labels(&mut batch);
        assert!(table.names.is_empty());
        assert!(table.values.is_empty());
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
        resolve_labels(&mut batch, &SymbolTable::default()).expect("resolve");
        assert_eq!(batch[0].labels, expected);
    }

    /// A label-less series interns to empty refs, the one shape that is
    /// ambiguous in proto3 (empty and absent repeated fields are the same
    /// bytes). It must round-trip as label-less rather than being rejected.
    #[test]
    fn label_less_series_roundtrips() {
        let mut batch = vec![series("a", vec![])];
        let table = intern_labels(&mut batch);
        resolve_labels(&mut batch, &table).expect("resolve");
        assert!(batch[0].labels.is_empty());
    }

    #[test]
    fn instant_samples_roundtrip_through_symbol_table() {
        let mut samples = vec![InstantSample {
            labels: vec![Label {
                name: "region".into(),
                value: "us-east-1".into(),
            }],
            value: 1.0,
            timestamp: 42,
            key: "a".into(),
            label_refs: Vec::new(),
        }];

        let table = intern_labels(&mut samples);
        assert!(samples[0].labels.is_empty());
        assert_eq!(table.names, vec!["region"]);
        assert_eq!(table.values, vec!["us-east-1"]);

        resolve_labels(&mut samples, &table).expect("resolve");
        assert_eq!(samples[0].labels[0].name, "region");
        assert_eq!(samples[0].labels[0].value, "us-east-1");
    }

    #[test]
    fn out_of_range_name_ref_rejected() {
        let mut batch = vec![SeriesRangeResponse {
            label_refs: vec![SymbolTableRef { name: 5, value: 0 }],
            ..series("a", vec![])
        }];
        let err = resolve_labels(
            &mut batch,
            &SymbolTable {
                names: vec!["region".into()],
                values: vec!["us-east-1".into()],
            },
        )
        .expect_err("out-of-range name ref must be rejected");
        let msg = err.to_string();
        assert!(msg.contains("series 'a'"), "{msg}");
        assert!(msg.contains("name ref 5"), "{msg}");
    }

    #[test]
    fn out_of_range_value_ref_rejected() {
        let mut batch = vec![SeriesRangeResponse {
            label_refs: vec![SymbolTableRef { name: 0, value: 7 }],
            ..series("a", vec![])
        }];
        let err = resolve_labels(
            &mut batch,
            &SymbolTable {
                names: vec!["region".into()],
                values: vec!["us-east-1".into()],
            },
        )
        .expect_err("out-of-range value ref must be rejected");
        let msg = err.to_string();
        assert!(msg.contains("series 'a'"), "{msg}");
        assert!(msg.contains("value ref 7"), "{msg}");
    }
    fn instant(key: &str, labels: Vec<(&str, &str)>) -> InstantSample {
        InstantSample {
            labels: labels
                .into_iter()
                .map(|(name, value)| Label {
                    name: name.into(),
                    value: value.into(),
                })
                .collect(),
            value: 1.0,
            timestamp: 42,
            key: key.into(),
            label_refs: Vec::new(),
        }
    }

    #[test]
    fn eval_label_resolver_matches_owned_resolution() {
        let mut samples = vec![
            instant("a", vec![("__name__", "cpu"), ("host", "h1"), ("region", "us")]),
            instant("b", vec![("__name__", "cpu"), ("host", "h2"), ("region", "us")]),
            instant("c", vec![]),
        ];
        let expected: Vec<EvalLabels> = samples
            .iter()
            .map(|s| EvalLabels::shared(s.labels.iter().cloned().map(crate::Label::from).collect()))
            .collect();
        let table = intern_labels(&mut samples);

        let mut resolver = EvalLabelResolver::new(&table);
        let resolved: Vec<EvalSample> = samples
            .into_iter()
            .map(|s| resolver.resolve_sample(s).expect("resolve"))
            .collect();
        for (got, want) in resolved.iter().zip(&expected) {
            assert_eq!(got.labels, *want, "{} vs {}", got.labels, want);
            assert_eq!(got.labels.fingerprint(), want.fingerprint());
        }
        assert_eq!(resolved[0].timestamp_ms, 42);
        assert_eq!(resolved[0].value, 1.0);
        // Every distinct pair was interned exactly once and is shared by index.
        assert_eq!(resolver.pairs.len(), 4, "{:?}", resolver.pairs.keys());
        assert!(matches!(resolved[0].labels, EvalLabels::Interned(_)));
    }

    #[test]
    fn eval_label_resolver_accepts_inline_labels() {
        // A peer that did not intern ships `labels` and no refs.
        let sample = instant("a", vec![("region", "us")]);
        let table = SymbolTable::default();
        let mut resolver = EvalLabelResolver::new(&table);
        let got = resolver.resolve_sample(sample).expect("resolve");
        assert_eq!(got.labels, EvalLabels::from_pairs(&[("region", "us")]));
    }

    #[test]
    fn eval_label_resolver_rejects_out_of_range_refs() {
        let table = SymbolTable {
            names: vec!["region".into()],
            values: vec!["us".into()],
        };
        for (name, value, needle) in [(3u32, 0u32, "name ref 3"), (0, 9, "value ref 9")] {
            let sample = InstantSample {
                label_refs: vec![SymbolTableRef { name, value }],
                ..instant("a", vec![])
            };
            let err = EvalLabelResolver::new(&table)
                .resolve_sample(sample)
                .expect_err("out-of-range ref must be rejected");
            let msg = err.to_string();
            assert!(msg.contains("series 'a'") && msg.contains(needle), "{msg}");
        }
    }

}
