use super::label::{Label, SeriesLabel, check_label_name};
use crate::common::constants::METRIC_NAME_LABEL;
use crate::common::rdb::{MAX_RDB_COLLECTION_LEN, rdb_load_len, rdb_load_string, rdb_save_usize};
use crate::common::string_interner::InternedString;
use crate::parser::ParseError;
use crate::parser::metric_name::parse_metric_name;
use crate::parser::utils::{is_bare_name, quote_string, write_name};
use get_size2::{GetSize, GetSizeTracker};
use std::collections::HashMap;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;
use valkey_module::{ValkeyResult, ValkeyValue, raw};

const EMPTY_LABEL: &str = "";

// Real series never come close to this many labels; it only rejects a corrupt/hostile
// count before it reaches `with_capacity`.
pub const MAX_LABELS_PER_SERIES: usize = 128;

pub struct InternedLabel<'a> {
    pub name: &'a str,
    pub value: &'a str,
}

impl From<InternedLabel<'_>> for ValkeyValue {
    fn from(label: InternedLabel) -> Self {
        let row = vec![
            ValkeyValue::from(label.name),
            ValkeyValue::from(label.value),
        ];
        ValkeyValue::from(row)
    }
}

impl SeriesLabel for InternedLabel<'_> {
    fn name(&self) -> &str {
        self.name
    }

    fn value(&self) -> &str {
        self.value
    }
}

/// A time series is optionally identified by a series of label-value pairs used to retrieve the
/// series in queries. Given that these labels are used to group semantically similar time series,
/// they are necessarily duplicated. We take advantage of this fact to intern label-value pairs,
/// meaning that only a single allocation is made per unique pair, irrespective of the number of
/// series it occurs in.
///
/// Each pair is one interned `name=value` string ([`InternedString`], a single pointer), and the
/// set is one shared, immutable slice: cloning a `MetricName` or handing a query the labels of a
/// series is a reference-count bump, never an allocation. Mutation rebuilds the slice, which is
/// fine for how rarely labels change (`TS.ALTER`, load).
///
/// The labels are kept in name order so that lookups are binary searches and two sets with the
/// same pairs compare equal.
#[derive(Debug, Clone, Default, Hash, PartialEq, Eq)]
pub struct MetricName(Arc<[InternedString]>);

impl GetSize for MetricName {
    /// `get_heap_size_with_tracker` is the extension point a containing type's derived `GetSize`
    /// calls. This used to override `get_size` instead, which left `TimeSeries`' derived impl on
    /// the trait default of zero: a series carrying 40 labels and ~1.9KB of interned strings
    /// reported a `memoryUsage` of exactly `size_of::<TimeSeries>()`, labels included for free.
    ///
    /// The tracker is deliberately not consulted. It exists to charge a shared allocation once
    /// per traversal, which here would bill one arbitrary holder for memory others share and
    /// the rest nothing. Both layers of sharing are split evenly instead, so per-key numbers add
    /// up across the keyspace:
    ///
    /// - each interned string is shared by every slice that holds it, and
    ///   [`InternedString::amortized_size`] gives this slice its share;
    /// - the slice is shared by every `MetricName` cloned from it (`COPY` clones the series), so
    ///   the slice and its string shares are divided again by [`Arc::strong_count`].
    ///
    /// The count includes holders that are not keys, such as a query or fan-out holding a
    /// series' labels; while one lives, each key reports a smaller share. A set that is edited
    /// gets a slice of its own and returns to the full charge.
    fn get_heap_size_with_tracker<T: GetSizeTracker>(&self, tracker: T) -> (usize, T) {
        let string_bytes: usize = self.0.iter().map(InternedString::amortized_size).sum();
        let holders = Arc::strong_count(&self.0);
        (
            (self.slice_bytes() + string_bytes).div_ceil(holders),
            tracker,
        )
    }
}

impl MetricName {
    pub fn new(labels: &[Label]) -> Self {
        Self::from_entries(
            labels
                .iter()
                .map(|label| InternedString::new_pair(label.name(), label.value()))
                .collect(),
        )
    }

    /// Build from `(name, value)` pairs; a repeated name keeps the last value.
    pub fn from_pairs<'a>(pairs: impl IntoIterator<Item = (&'a str, &'a str)>) -> Self {
        Self::from_entries(
            pairs
                .into_iter()
                .map(|(name, value)| InternedString::new_pair(name, value))
                .collect(),
        )
    }

    /// Build from entries in any order: one sort by name, then repeated names collapse to the
    /// last value. Inserting one at a time shifts the vector on every out-of-order entry, which
    /// is quadratic for reverse-ordered input.
    fn from_entries(mut entries: Vec<InternedString>) -> Self {
        // Stable, so equal names stay in input order and the swap below keeps the last one.
        entries.sort_by(|a, b| a.name().cmp(b.name()));
        entries.dedup_by(|later, kept| {
            let same = later.name() == kept.name();
            if same {
                std::mem::swap(later, kept);
            }
            same
        });
        Self(Self::share(entries))
    }

    /// The slice for `entries`. An empty set is the one shared empty slice
    /// (`Arc::<[_]>::default()`), never an allocation of its own, so every empty set costs
    /// the same: nothing.
    fn share(entries: Vec<InternedString>) -> Arc<[InternedString]> {
        if entries.is_empty() {
            Arc::default()
        } else {
            Arc::from(entries)
        }
    }

    /// Heap bytes of the slice itself, before dividing among its holders: the `Arc` counts plus
    /// one pointer per label. The empty slice is shared process-wide (see [`Self::share`]), so it
    /// is charged to no one.
    fn slice_bytes(&self) -> usize {
        if self.0.is_empty() {
            0
        } else {
            2 * size_of::<usize>() + self.0.len() * size_of::<InternedString>()
        }
    }

    pub fn clear(&mut self) {
        self.0 = Arc::default();
    }

    fn key_of(tag: &InternedString) -> &str {
        tag.name()
    }

    fn find_index(&self, key: &str) -> Result<usize, usize> {
        self.0.binary_search_by_key(&key, Self::key_of)
    }

    /// Insert into a name-ordered vector, replacing an existing value for the name.
    fn insert_pair(entries: &mut Vec<InternedString>, key: &str, value: &str) {
        let interned_value = InternedString::new_pair(key, value);
        match entries.binary_search_by_key(&key, Self::key_of) {
            Ok(idx) => entries[idx] = interned_value,
            Err(idx) => entries.insert(idx, interned_value),
        }
    }

    /// Rebuild the slice through a vector edit. Every entry is retained once
    /// for the copy and released once with the old slice; rare, see the type docs.
    fn edit(&mut self, f: impl FnOnce(&mut Vec<InternedString>)) {
        let mut entries: Vec<InternedString> = self.0.to_vec();
        f(&mut entries);
        self.0 = Self::share(entries);
    }

    /// adds a new label to mn with the given key and value.
    pub fn add_label(&mut self, key: &str, value: &str) {
        self.edit(|entries| Self::insert_pair(entries, key, value));
    }

    pub fn get_tag(&'_ self, key: &str) -> Option<InternedLabel<'_>> {
        let idx = self.find_index(key).ok()?;
        let (name, value) = self.0[idx].split_pair()?;
        Some(InternedLabel { name, value })
    }

    pub fn get_value(&self, key: &str) -> Option<&str> {
        let idx = self.find_index(key).ok()?;
        let (_, value) = self.0[idx].split_pair()?;
        Some(value)
    }

    pub fn remove_label(&mut self, key: &str) {
        if let Ok(idx) = self.find_index(key) {
            self.edit(|entries| {
                entries.remove(idx);
            });
        }
    }

    pub fn get_measurement(&self) -> &str {
        self.get_value(METRIC_NAME_LABEL).unwrap_or(EMPTY_LABEL)
    }

    pub fn iter(&'_ self) -> impl Iterator<Item = InternedLabel<'_>> {
        self.0.iter().filter_map(|x| {
            x.split_pair()
                .map(|(name, value)| InternedLabel { name, value })
        })
    }

    pub fn to_label_vec(&self) -> Vec<Label> {
        self.iter()
            .map(|label| Label::new(label.name, label.value))
            .collect()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn to_rdb(&self, rdb: *mut raw::RedisModuleIO) {
        rdb_save_usize(rdb, self.0.len());
        for label in self.iter() {
            raw::save_string(rdb, label.name);
            raw::save_string(rdb, label.value);
        }
    }

    pub fn from_rdb(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<Self> {
        // Not `MAX_LABELS_PER_SERIES`: `TS.CREATE … LABELS` does not enforce it,
        // and a series `rdb_load` error aborts the whole load, so a tighter
        // bound here turns an accepted write into a server that cannot start.
        // The generic guard still rejects a corrupt count, and the allocation
        // is capped up front: each label is a read that fails on a short payload.
        let count = rdb_load_len(rdb, MAX_RDB_COLLECTION_LEN)?;
        let mut entries = Vec::with_capacity(count.min(MAX_LABELS_PER_SERIES));
        for _ in 0..count {
            let name = rdb_load_string(rdb)?;
            let value = rdb_load_string(rdb)?;
            // Same rule as the write path, so a crafted `RESTORE` payload can't store a label
            // no write would accept, and the pool never holds two labels with the same text.
            check_label_name(name.as_bytes())?;
            entries.push(InternedString::new_pair(&name, &value));
        }
        Ok(Self::from_entries(entries))
    }
}

impl From<HashMap<String, String>> for MetricName {
    fn from(map: HashMap<String, String>) -> Self {
        Self::from_pairs(map.iter().map(|(k, v)| (k.as_str(), v.as_str())))
    }
}

impl From<&[Label]> for MetricName {
    fn from(labels: &[Label]) -> Self {
        MetricName::new(labels)
    }
}

impl From<Vec<Label>> for MetricName {
    fn from(labels: Vec<Label>) -> Self {
        MetricName::new(&labels)
    }
}

impl FromStr for MetricName {
    type Err = ParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let labels = parse_metric_name(s)?;
        Ok(MetricName::new(&labels))
    }
}

/// Writes the metric definition [`parse_metric_name`] reads back: `name{label="value",…}`, with
/// any name that is not an identifier quoted, and a metric name that is not one written inside
/// the braces as Prometheus 3 does (`{"my metric", a="1"}`). A set without `__name__` renders
/// as its label set alone (`{a="1"}`); it is a label set, not a metric definition, and does not
/// parse back.
impl Display for MetricName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = self.get_measurement();
        let mut first = true;
        if is_bare_name(name) {
            write!(f, "{name}{{")?;
        } else {
            f.write_str("{")?;
            if !name.is_empty() {
                f.write_str(&quote_string(name))?;
                first = false;
            }
        }
        for label in self
            .iter()
            .filter(|l| !l.name.is_empty() && l.name != METRIC_NAME_LABEL)
        {
            if !first {
                f.write_str(",")?;
            }
            first = false;
            write_name(f, label.name)?;
            write!(f, "={}", quote_string(label.value))?;
        }
        f.write_str("}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_label() {
        let mut metric_name = MetricName::default();
        metric_name.add_label("key1", "value1");
        metric_name.add_label("key2", "value2");

        assert_eq!(metric_name.get_value("key1"), Some("value1"));
        assert_eq!(metric_name.get_value("key2"), Some("value2"));
    }

    #[test]
    fn test_get_measurement() {
        let mut metric_name = MetricName::default();
        metric_name.add_label("key1", "value1");

        assert_eq!(metric_name.get_measurement(), "");

        metric_name.add_label(METRIC_NAME_LABEL, "metric_name");
        assert_eq!(metric_name.get_measurement(), "metric_name");
    }

    #[test]
    fn labels_are_kept_in_name_order_not_raw_order() {
        // "a1=x" < "a=x" as strings ('1' < '='), but `a` < `a1` as names.
        let mn = MetricName::from_pairs([("a1", "x"), ("a", "x"), ("__name__", "m")]);
        let names: Vec<&str> = mn.iter().map(|l| l.name).collect();
        assert_eq!(names, ["__name__", "a", "a1"]);
        assert_eq!(mn.get_value("a"), Some("x"));
        assert_eq!(mn.get_value("a1"), Some("x"));
    }

    #[test]
    fn repeated_names_keep_the_last_value_in_any_order() {
        let mn = MetricName::from_pairs([
            ("c", "1"),
            ("a", "1"),
            ("c", "2"),
            ("b", "1"),
            ("a", "2"),
            ("c", "3"),
        ]);
        let pairs: Vec<(&str, &str)> = mn.iter().map(|l| (l.name, l.value)).collect();
        assert_eq!(pairs, [("a", "2"), ("b", "1"), ("c", "3")]);
    }

    /// `a=1` and `a=b=2` used to intern `a=b`'s pair as name `a`, value `b=2`, which then
    /// replaced the real `a` as a repeated name: one label lost, the other renamed.
    #[test]
    fn names_containing_the_separator_survive() {
        let mn = MetricName::new(&[Label::new("a", "1"), Label::new("a=b", "2")]);
        let pairs: Vec<(&str, &str)> = mn.iter().map(|l| (l.name, l.value)).collect();
        assert_eq!(pairs, [("a", "1"), ("a=b", "2")]);
        assert_eq!(mn.get_value("a"), Some("1"));
        assert_eq!(mn.get_value("a=b"), Some("2"));

        let mut edited = mn.clone();
        edited.add_label("a", "b=2");
        assert_eq!(edited.get_value("a"), Some("b=2"));
        assert_eq!(edited.get_value("a=b"), Some("2"));
        assert_ne!(edited, mn);
    }

    #[test]
    fn clone_and_shared_are_the_same_allocation() {
        let mn: MetricName = r#"m{a="1",b="2"}"#.parse().unwrap();
        let shared = Arc::clone(&mn.0);
        assert!(Arc::ptr_eq(&shared, &mn.clone().0));
        assert_eq!(shared.len(), 3);
        assert_eq!(shared[1].split_pair(), Some(("a", "1")));

        let mut altered = mn.clone();
        altered.add_label("c", "3");
        assert!(!Arc::ptr_eq(&altered.0, &mn.0));
        assert_eq!(mn.len(), 3);
        assert_eq!(altered.len(), 4);
        altered.remove_label("a");
        assert_eq!(altered.get_value("a"), None);
        assert_eq!(altered.len(), 3);
    }

    #[test]
    fn test_len_and_is_empty() {
        let mut metric_name = MetricName::default();
        metric_name.add_label("key1", "value1");
        metric_name.add_label("key2", "value2");

        assert_eq!(metric_name.len(), 2);
        assert!(!metric_name.is_empty());
    }

    #[test]
    fn test_display() {
        let mut metric_name = MetricName::default();
        metric_name.add_label(METRIC_NAME_LABEL, "metric");
        metric_name.add_label("key1", "value1");
        metric_name.add_label("key2", "value2");

        let display = format!("{metric_name}");

        assert_eq!(display, "metric{key1=\"value1\",key2=\"value2\"}");
    }

    /// The heap accounting has to hang off `get_heap_size_with_tracker`, because that -- not
    /// `get_size` -- is what a containing type's derived `GetSize` calls. Overriding `get_size`
    /// alone reported the labels as costing nothing at all from inside `TimeSeries`.
    #[test]
    fn test_heap_size_counts_interned_labels() {
        let _pool = crate::common::string_interner::test_pool::isolated();
        let empty = MetricName::default().get_heap_size();

        let mut metric_name = MetricName::default();
        for i in 0..8 {
            metric_name.add_label(
                &format!("heap_size_label_{i}"),
                &format!("a_reasonably_long_label_value_{i}"),
            );
        }

        let labelled = metric_name.get_heap_size();
        let payload: usize = (0..8)
            .map(|i| format!("heap_size_label_{i}=a_reasonably_long_label_value_{i}").len())
            .sum();

        assert!(
            labelled >= empty + payload,
            "heap size {labelled} does not cover {payload} bytes of label text over the empty \
             baseline of {empty}",
        );
    }

    /// One pool allocation backs every series carrying the same pair, so each holder reports a
    /// share of it: charging all of it to all of them would make the sum of `MEMORY USAGE` over
    /// a keyspace exceed what the module holds by the sharing factor.
    #[test]
    fn test_shared_labels_are_amortized_across_holders() {
        let _pool = crate::common::string_interner::test_pool::isolated();
        let mut only = MetricName::default();
        only.add_label("amortize_probe", "shared_value_for_the_amortization_test");
        let sole_holder = only.get_heap_size();

        let mut sharers = Vec::new();
        for _ in 0..4 {
            let mut mn = MetricName::default();
            mn.add_label("amortize_probe", "shared_value_for_the_amortization_test");
            sharers.push(mn);
        }

        let per_holder = sharers[0].get_heap_size();
        assert!(
            per_holder < sole_holder,
            "a pair shared five ways ({per_holder}) should cost each holder less than one held \
             alone ({sole_holder})",
        );
        // Every holder agrees, and together they account for the whole allocation once.
        for mn in &sharers {
            assert_eq!(mn.get_heap_size(), per_holder);
        }
    }

    /// `COPY` clones a series, which clones its `MetricName`: one slice, two holders. Each must
    /// report half of what one holder alone reports, so the two keys' `MEMORY USAGE` add up to
    /// the memory actually held, and an edit gives a holder its own slice and the full charge.
    #[test]
    fn test_cloned_labels_split_the_shared_slice() {
        let _pool = crate::common::string_interner::test_pool::isolated();
        let original = MetricName::from_pairs([
            ("clone_probe_a", "value_for_the_clone_accounting_test"),
            ("clone_probe_b", "another_value_for_the_clone_test"),
        ]);
        let alone = original.get_heap_size();
        assert!(alone > 0);

        let copy = original.clone();
        assert!(
            Arc::ptr_eq(&original.0, &copy.0),
            "a clone shares the slice"
        );
        let (a, b) = (original.get_heap_size(), copy.get_heap_size());
        assert_eq!(a, b);
        // Each share is rounded up, so the pair can overshoot by a byte.
        assert!(a + b >= alone && a + b <= alone + 1, "{a} + {b} vs {alone}");

        // Editing the copy gives it a slice of its own; the original is alone again.
        let mut edited = copy.clone();
        drop(copy);
        edited.add_label("clone_probe_c", "x");
        assert!(!Arc::ptr_eq(&original.0, &edited.0));
        // The two slices still share the two interned strings, so neither holder is back to
        // the full charge, but neither is halved any more: the slice is no longer split.
        assert!(
            original.get_heap_size() > a,
            "{} vs {a}",
            original.get_heap_size()
        );
        assert!(original.get_heap_size() < alone);
        drop(edited);
        assert_eq!(original.get_heap_size(), alone);
    }

    /// An empty set is the one shared empty slice however it was made, and costs nothing.
    #[test]
    fn test_empty_label_sets_are_shared_and_free() {
        let default = MetricName::default();
        let built = MetricName::new(&[]);
        let mut cleared = MetricName::from_pairs([("empty_probe", "v")]);
        cleared.clear();
        let mut emptied = MetricName::from_pairs([("empty_probe", "v")]);
        emptied.remove_label("empty_probe");
        for (what, mn) in [
            ("built", &built),
            ("cleared", &cleared),
            ("emptied", &emptied),
        ] {
            assert!(mn.is_empty(), "{what}");
            assert!(
                Arc::ptr_eq(&mn.0, &default.0),
                "{what} allocated its own empty slice"
            );
            assert_eq!(mn.get_heap_size(), 0, "{what}");
        }
        assert_eq!(default.get_heap_size(), 0);
    }

    /// Interning on a realistic fleet: every `key=value` pair the fleet repeats is held once,
    /// and the per-holder accounting sums back to the pool's footprint. Runs against a pool of
    /// its own, so the pool-wide figures (`memory_saved_pct`, the shared allocations behind
    /// `amortized_size`) describe this fleet alone rather than whatever other tests intern.
    #[test]
    fn fleet_labels_intern_to_unique_pairs() {
        let _pool = crate::common::string_interner::test_pool::isolated();
        use crate::common::string_interner::InternedString;
        use crate::tests::generators::{FleetPreset, FleetTopology};
        use std::collections::HashSet;

        let fleet = FleetTopology::preset(FleetPreset::Small).generate();
        let unique: HashSet<String> = fleet
            .iter()
            .flat_map(|s| s.labels.iter().map(|l| format!("{}={}", l.name, l.value)))
            .collect();
        let pairs: usize = fleet.iter().map(|s| s.labels.len()).sum();
        assert!(
            unique.len() * 50 < pairs,
            "fleet does not repeat labels: {} / {pairs}",
            unique.len()
        );

        let names: Vec<MetricName> = fleet.iter().map(|s| MetricName::new(&s.labels)).collect();
        let stats = InternedString::get_stats();

        // Attribute the pool's growth to `names` itself: one entry per distinct interned string
        // it holds. Diffing two process-global pool snapshots (`interned_count` before vs.
        // `stats.total_stats.count` after) instead tied the assertion to unrelated interner
        // traffic and left the `usize` subtraction able to underflow if the pool shrank between
        // the two reads.
        let retained: HashSet<&InternedString> = names.iter().flat_map(|mn| mn.0.iter()).collect();
        let retained_count = retained.len();
        assert!(
            retained_count <= unique.len() && retained_count + 64 >= unique.len(),
            "names retain {retained_count} pool entries for {} unique pairs",
            unique.len()
        );
        assert!(stats.memory_saved_pct > 95.0, "{}", stats.memory_saved_pct);
        // The pool-only figure above is what `TS._DEBUG STRINGPOOLSTATS` has always reported,
        // and on a fleet-shaped label set it says 99% however much the labels actually cost:
        // both sides of its ratio count heap allocations only. `storage_saved_pct` adds the
        // slot each holder keeps either way, so it lands materially lower (~82% here) and is
        // the figure to quote for label memory. It cannot go the other way.
        assert!(
            stats.storage_saved_pct < stats.memory_saved_pct,
            "storage {} should trail pool-only {}",
            stats.storage_saved_pct,
            stats.memory_saved_pct
        );
        assert!(
            stats.storage_saved_pct > 50.0,
            "interning should still pay on a fleet: {}",
            stats.storage_saved_pct
        );
        // The slots are real memory the pool never sees: a fleet holds far more label
        // occurrences than distinct pairs, so they outweigh the pool itself.
        assert!(
            stats.holder_slot_bytes > stats.total_stats.allocated,
            "slots {} should outweigh the pool {} on a repetitive fleet",
            stats.holder_slot_bytes,
            stats.total_stats.allocated
        );

        // `amortized_size` splits each shared allocation across its holders, so the per-series
        // string bytes sum to (about) what the pool holds for the fleet.
        let pool_bytes: usize = unique
            .iter()
            .map(|p| p.len() + 2 * size_of::<usize>())
            .sum();
        let amortized: usize = names
            .iter()
            .map(|mn| mn.get_heap_size() - mn.slice_bytes())
            .sum();
        // div_ceil rounds each share up, so the sum overshoots by at most one byte per pair.
        assert!(
            amortized >= pool_bytes && amortized <= pool_bytes + pairs,
            "{amortized} vs {pool_bytes}"
        );
        drop(names);
    }

    /// `Display` writes a definition `FromStr` reads back as the same label set, whatever the
    /// names hold (LABELS-02: names were written raw, so `m{"a b"="v"}` rendered as `m{a b="v"}`).
    #[test]
    fn test_display_round_trips_through_from_str() {
        let cases: &[(&[(&str, &str)], &str)] = &[
            (&[("__name__", "m"), ("a", "v")], r#"m{a="v"}"#),
            (&[("__name__", "m"), ("a b", "v")], r#"m{"a b"="v"}"#),
            (&[("__name__", "m"), ("9", "v")], r#"m{"9"="v"}"#),
            (
                &[("__name__", "m"), ("or", "1"), ("OR", "2")],
                r#"m{"OR"="2","or"="1"}"#,
            ),
            (&[("__name__", "m"), ("a.b-c:d", "v")], r#"m{a.b-c:d="v"}"#),
            (&[("__name__", "m"), ("région", "v")], r#"m{"région"="v"}"#),
            (&[("__name__", "a b")], r#"{"a b"}"#),
            (&[("__name__", "9cpu"), ("x", "1")], r#"{"9cpu",x="1"}"#),
            (&[("__name__", "or")], r#"{"or"}"#),
            (
                &[("__name__", "m"), ("q", "say \"hi\"\\")],
                r#"m{q="say \"hi\"\\"}"#,
            ),
        ];
        for (pairs, rendered) in cases {
            let mn = MetricName::from_pairs(pairs.iter().copied());
            let text = mn.to_string();
            assert_eq!(&text, rendered, "{pairs:?}");
            let back: MetricName = text.parse().unwrap_or_else(|e| panic!("{text}: {e}"));
            assert_eq!(back, mn, "{text}");
        }
        // Without `__name__` it is a label set, not a metric definition.
        let unnamed = MetricName::from_pairs([("a b", "v")]);
        assert_eq!(unnamed.to_string(), r#"{"a b"="v"}"#);
        assert!(unnamed.to_string().parse::<MetricName>().is_err());
    }
}
