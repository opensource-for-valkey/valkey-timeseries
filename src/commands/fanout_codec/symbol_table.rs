//! Response-level label interning for cluster fanout.
//!
//! Real `TS.MRANGE` and PromQL fan-out responses repeat a small universe of
//! label names (and often values) across every matched series. Each labelled
//! element carries its labels as two parallel packed `u32` arrays indexing a
//! per-response [`SymbolTable`] instead of inline `name`/`value` strings.
//!
//! Three ways to produce the refs, one to consume them:
//!
//! - [`intern_labels`] rewrites owned `labels: Vec<Label>` in place (MRANGE and
//!   the aggregated PromQL outputs, which are built as owned labels anyway).
//! - [`SymbolTableBuilder`] interns straight from a series' [`MetricName`] by
//!   the identity of its interned `name=value` entries: a hit is one integer
//!   lookup, and no owned strings are built for a label already in the table.
//!   This is what the shard's instant-query handler uses.
//! - [`resolve_labels`] is the coordinator-side inverse for consumers that
//!   want owned `Label`s back (MRANGE); [`EvalLabelResolver`] goes straight to
//!   evaluator labels for the PromQL paths.
//!
//! Both resolvers are safe to call on any response without the caller first
//! establishing that it was interned: an element whose ref arrays are empty
//! keeps its inline `labels` rather than being emptied. They run on
//! peer-controlled input, so a malformed index or ragged pair of arrays is an
//! `Err`, never a panic (see the `rdb_load_len` rationale in
//! `src/common/rdb.rs` for why fanout/RDB input is treated as untrusted
//! throughout this crate).

use super::generated::{Label, SeriesRangeResponse, SymbolTable};
use crate::common::context::key_for_display;
use crate::labels::MetricName;
use crate::promql::generated::InstantSample;
use crate::promql::{EvalLabels, EvalSample, SplitLabel};
use std::collections::HashMap;
use std::sync::Arc;
use valkey_module::{ValkeyError, ValkeyResult};

/// A labelled wire element: owned `labels`, or two parallel ref arrays into
/// the response's [`SymbolTable`].
pub trait SymbolTableRefs {
    fn take_labels(&mut self) -> Vec<Label>;
    fn set_label_refs(&mut self, names: Vec<u32>, values: Vec<u32>);
    fn take_label_refs(&mut self) -> (Vec<u32>, Vec<u32>);
    fn set_labels(&mut self, labels: Vec<Label>);
    /// How the element is named in a malformed-response error.
    fn describe(&self) -> String;
}

impl SymbolTableRefs for SeriesRangeResponse {
    fn take_labels(&mut self) -> Vec<Label> {
        std::mem::take(&mut self.labels)
    }

    fn set_label_refs(&mut self, names: Vec<u32>, values: Vec<u32>) {
        self.label_name_refs = names;
        self.label_value_refs = values;
    }

    fn take_label_refs(&mut self) -> (Vec<u32>, Vec<u32>) {
        (
            std::mem::take(&mut self.label_name_refs),
            std::mem::take(&mut self.label_value_refs),
        )
    }

    fn set_labels(&mut self, labels: Vec<Label>) {
        self.labels = labels;
    }

    fn describe(&self) -> String {
        format!("series '{}'", key_for_display(&self.key))
    }
}

impl SymbolTableRefs for InstantSample {
    fn take_labels(&mut self) -> Vec<Label> {
        std::mem::take(&mut self.labels)
    }

    fn set_label_refs(&mut self, names: Vec<u32>, values: Vec<u32>) {
        self.label_name_refs = names;
        self.label_value_refs = values;
    }

    fn take_label_refs(&mut self) -> (Vec<u32>, Vec<u32>) {
        (
            std::mem::take(&mut self.label_name_refs),
            std::mem::take(&mut self.label_value_refs),
        )
    }

    fn set_labels(&mut self, labels: Vec<Label>) {
        self.labels = labels;
    }

    fn describe(&self) -> String {
        format!("instant sample at {}", self.timestamp)
    }
}

/// Builds a response's [`SymbolTable`] from series labels as they are read
/// from storage.
///
/// Storage keeps every label as one interned `name=value` string, and the
/// interner guarantees one allocation per distinct string while it is alive,
/// so a label's address identifies it: the pair cache is keyed by that
/// address and a hit costs one integer lookup. Only the first sight of a
/// label splits it and copies its two halves into the table. The borrowed
/// keys tie the builder to the series it reads from; [`Self::finish`]
/// releases them.
#[derive(Default)]
pub struct SymbolTableBuilder<'a> {
    table: SymbolTable,
    name_ids: HashMap<&'a str, u32, ahash::RandomState>,
    value_ids: HashMap<&'a str, u32, ahash::RandomState>,
    /// `name=value` address → (name ref, value ref).
    pairs: HashMap<usize, (u32, u32), ahash::RandomState>,
}

impl<'a> SymbolTableBuilder<'a> {
    /// The ref arrays for one series' labels, in storage (name) order.
    /// Entries without a separator are malformed; storage never produces
    /// them, and `MetricName::iter` skips them the same way.
    pub fn intern(&mut self, labels: &'a MetricName) -> (Vec<u32>, Vec<u32>) {
        let mut names = Vec::with_capacity(labels.len());
        let mut values = Vec::with_capacity(labels.len());
        for raw in labels.raw_entries() {
            let key = raw.as_bytes().as_ptr() as usize;
            let (name_ref, value_ref) = match self.pairs.get(&key) {
                Some(&refs) => refs,
                None => {
                    let Some((name, value)) = raw.split_once('=') else {
                        continue;
                    };
                    let refs = (
                        Self::id(&mut self.name_ids, &mut self.table.names, name),
                        Self::id(&mut self.value_ids, &mut self.table.values, value),
                    );
                    self.pairs.insert(key, refs);
                    refs
                }
            };
            names.push(name_ref);
            values.push(value_ref);
        }
        (names, values)
    }

    fn id(
        ids: &mut HashMap<&'a str, u32, ahash::RandomState>,
        symbols: &mut Vec<String>,
        symbol: &'a str,
    ) -> u32 {
        *ids.entry(symbol).or_insert_with(|| {
            symbols.push(symbol.to_owned());
            (symbols.len() - 1) as u32
        })
    }

    pub fn finish(self) -> SymbolTable {
        self.table
    }
}

/// Rewrites every element's owned `labels` into refs against a per-response
/// [`SymbolTable`], clearing `labels` in the process.
///
/// An element with no labels gets empty ref arrays, which the resolvers read
/// as "nothing to resolve" — so the round trip is still correct without
/// needing a separate marker for the empty case.
pub fn intern_labels<T: SymbolTableRefs>(series: &mut [T]) -> SymbolTable {
    let mut table = SymbolTable::default();
    let mut name_ids: HashMap<String, u32, ahash::RandomState> = HashMap::default();
    let mut value_ids: HashMap<String, u32, ahash::RandomState> = HashMap::default();

    for s in series.iter_mut() {
        let labels = s.take_labels();
        let mut names = Vec::with_capacity(labels.len());
        let mut values = Vec::with_capacity(labels.len());
        for label in labels {
            names.push(owned_id(&mut name_ids, &mut table.names, label.name));
            values.push(owned_id(&mut value_ids, &mut table.values, label.value));
        }
        s.set_label_refs(names, values);
    }

    table
}

fn owned_id(
    ids: &mut HashMap<String, u32, ahash::RandomState>,
    symbols: &mut Vec<String>,
    symbol: String,
) -> u32 {
    if let Some(&idx) = ids.get(symbol.as_str()) {
        return idx;
    }
    let idx = symbols.len() as u32;
    symbols.push(symbol.clone());
    ids.insert(symbol, idx);
    idx
}

/// Looks up one ref pair, rejecting anything the table cannot answer.
fn lookup<'t, T: SymbolTableRefs>(
    table: &'t SymbolTable,
    element: &T,
    name_ref: u32,
    value_ref: u32,
) -> ValkeyResult<(&'t str, &'t str)> {
    let name = table.names.get(name_ref as usize).ok_or_else(|| {
        ValkeyError::String(format!(
            "TSDB: malformed symbol-table response: {} label name ref {} out of range ({} names)",
            element.describe(),
            name_ref,
            table.names.len()
        ))
    })?;
    let value = table.values.get(value_ref as usize).ok_or_else(|| {
        ValkeyError::String(format!(
            "TSDB: malformed symbol-table response: {} label value ref {} out of range ({} values)",
            element.describe(),
            value_ref,
            table.values.len()
        ))
    })?;
    Ok((name, value))
}

/// The two ref arrays of an element, or `None` when it carries no refs.
fn take_ref_pairs<T: SymbolTableRefs>(element: &mut T) -> ValkeyResult<Option<(Vec<u32>, Vec<u32>)>> {
    let (names, values) = element.take_label_refs();
    if names.is_empty() && values.is_empty() {
        return Ok(None);
    }
    if names.len() != values.len() {
        return Err(ValkeyError::String(format!(
            "TSDB: malformed symbol-table response: {} has {} label name refs but {} value refs",
            element.describe(),
            names.len(),
            values.len()
        )));
    }
    Ok(Some((names, values)))
}

/// Inverse of [`intern_labels`]: resolves each element's refs against the
/// response-level dictionaries back into owned `labels: Vec<Label>`,
/// clearing the ref arrays in the process.
///
/// Total: safe to call on any response without first checking whether it was
/// interned. An element whose ref arrays are both empty is left exactly as
/// received, `labels` included, rather than being emptied.
pub fn resolve_labels<T: SymbolTableRefs>(
    series: &mut [T],
    table: &SymbolTable,
) -> ValkeyResult<()> {
    for s in series.iter_mut() {
        // Nothing to resolve. Returning early rather than assigning an empty
        // vec keeps this function from discarding `labels` when it is handed a
        // response that was never interned.
        let Some((names, values)) = take_ref_pairs(s)? else {
            continue;
        };
        let mut labels = Vec::with_capacity(names.len());
        for (name_ref, value_ref) in names.into_iter().zip(values) {
            let (name, value) = lookup(table, s, name_ref, value_ref)?;
            labels.push(Label {
                name: name.to_owned(),
                value: value.to_owned(),
            });
        }
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

    /// The evaluator labels for one element, consuming its refs (or its
    /// inline labels when it has none).
    pub fn resolve<T: SymbolTableRefs>(&mut self, s: &mut T) -> ValkeyResult<EvalLabels> {
        let Some((names, values)) = take_ref_pairs(s)? else {
            let labels = s.take_labels().into_iter().map(crate::Label::from).collect();
            return Ok(EvalLabels::shared(labels));
        };
        // Check every ref first so the fill below cannot fail: an infallible
        // `TrustedLen` iterator collects straight into the `Arc<[_]>`, one
        // allocation per series, instead of through a scratch `Vec`.
        for (&name_ref, &value_ref) in names.iter().zip(&values) {
            lookup(self.table, s, name_ref, value_ref)?;
        }
        let split: Arc<[SplitLabel]> = names
            .into_iter()
            .zip(values)
            .map(|(name_ref, value_ref)| self.pair(name_ref, value_ref))
            .collect();
        Ok(EvalLabels::from_split_shared(split))
    }

    /// The interned label for one validated ref pair, created on first sight.
    #[inline]
    fn pair(&mut self, name_ref: u32, value_ref: u32) -> SplitLabel {
        let table = self.table;
        self.pairs
            .entry((u64::from(name_ref) << 32) | u64::from(value_ref))
            .or_insert_with(|| {
                SplitLabel::new(
                    &table.names[name_ref as usize],
                    &table.values[value_ref as usize],
                )
            })
            .clone()
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
            assert!(got.label_name_refs.is_empty() && got.label_value_refs.is_empty());
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
            label_name_refs: Vec::new(),
            label_value_refs: Vec::new(),
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
            label_name_refs: vec![5],
            label_value_refs: vec![0],
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
            label_name_refs: vec![0],
            label_value_refs: vec![7],
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
    fn instant(id: u32, labels: Vec<(&str, &str)>) -> InstantSample {
        InstantSample {
            labels: labels
                .into_iter()
                .map(|(name, value)| Label {
                    name: name.into(),
                    value: value.into(),
                })
                .collect(),
            value: 1.0,
            timestamp: 42 + i64::from(id),
            label_name_refs: Vec::new(),
            label_value_refs: Vec::new(),
        }
    }

    #[test]
    fn eval_label_resolver_matches_owned_resolution() {
        let mut samples = vec![
            instant(0, vec![("__name__", "cpu"), ("host", "h1"), ("region", "us")]),
            instant(1, vec![("__name__", "cpu"), ("host", "h2"), ("region", "us")]),
            instant(2, vec![]),
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
        let sample = instant(0, vec![("region", "us")]);
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
        for (names, values, needle) in [
            (vec![3u32], vec![0u32], "name ref 3"),
            (vec![0], vec![9], "value ref 9"),
            (vec![0, 0], vec![0], "2 label name refs but 1 value refs"),
        ] {
            let sample = InstantSample {
                label_name_refs: names,
                label_value_refs: values,
                ..instant(0, vec![])
            };
            let err = EvalLabelResolver::new(&table)
                .resolve_sample(sample)
                .expect_err("malformed refs must be rejected");
            let msg = err.to_string();
            assert!(msg.contains("instant sample at 42") && msg.contains(needle), "{msg}");
        }
    }

    #[test]
    fn builder_interns_by_identity_and_matches_owned_interning() {
        use crate::labels::MetricName;
        let metric = |pairs: &[(&str, &str)]| {
            let mut m = MetricName::default();
            for (name, value) in pairs {
                m.add_label(name, value);
            }
            m
        };
        let a = metric(&[("__name__", "cpu"), ("host", "h1"), ("region", "us")]);
        let b = metric(&[("__name__", "cpu"), ("host", "h2"), ("region", "us")]);
        let c = MetricName::default();
        let mut builder = SymbolTableBuilder::default();
        let ra = builder.intern(&a);
        let rb = builder.intern(&b);
        let rc = builder.intern(&c);
        // Second sight of `__name__=cpu` and `region=us` hit the address cache:
        // four distinct pairs across two series.
        assert_eq!(builder.pairs.len(), 4);
        let table = builder.finish();
        assert_eq!(table.names, vec!["__name__", "host", "region"]);
        assert_eq!(table.values, vec!["cpu", "h1", "us", "h2"]);
        assert_eq!(ra, (vec![0, 1, 2], vec![0, 1, 2]));
        assert_eq!(rb, (vec![0, 1, 2], vec![0, 3, 2]));
        assert_eq!(rc, (vec![], vec![]));

        // Resolving through the table gives the same labels the owned path
        // would have shipped.
        let mut sample = InstantSample {
            label_name_refs: rb.0,
            label_value_refs: rb.1,
            ..instant(1, vec![])
        };
        let got = EvalLabelResolver::new(&table).resolve(&mut sample).expect("resolve");
        assert_eq!(got, EvalLabels::interned(&b));
    }

}
