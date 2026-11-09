/// Stat holds values for a single cardinality statistic.
#[derive(Debug, Clone, Default)]
pub struct PostingStat {
    pub name: String,
    pub count: u64,
}

#[derive(Clone, Debug, Default)]
pub struct PostingsStats {
    pub series_count_by_metric_name: Vec<PostingStat>,
    pub series_count_by_label_name: Vec<PostingStat>,
    pub series_count_by_label_value_pairs: Vec<PostingStat>,
    pub series_count_by_focus_label_value: Option<Vec<PostingStat>>,
    pub total_label_value_pairs: usize,
    pub label_count: usize,
    pub series_count: u64,
}

/// The `limit` best [`PostingStat`]s pushed into it: highest count first, and between equal
/// counts the lexicographically smaller name. Ranking by both makes the selection at the cutoff
/// independent of push order, so a scan in index order, a hash-map drain on the coordinator and
/// a second process all keep the same entries.
pub(crate) struct StatsMaxHeap {
    max_length: usize,
    /// Index of the entry that ranks last, valid once `items` is non-empty.
    worst: usize,
    items: Vec<PostingStat>,
}

/// Up-front capacity cap: `limit` can be far larger than what a scan finds (and is `usize::MAX`
/// for an unbounded list).
const INITIAL_CAPACITY: usize = 64;

/// Whether `(count, name)` ranks ahead of `other`.
#[inline]
fn ranks_ahead(count: u64, name: &str, other: &PostingStat) -> bool {
    count > other.count || (count == other.count && name < other.name.as_str())
}

impl StatsMaxHeap {
    pub fn new(length: usize) -> Self {
        StatsMaxHeap {
            max_length: length,
            worst: 0,
            items: Vec::with_capacity(length.min(INITIAL_CAPACITY)),
        }
    }

    /// Whether an entry ranked `(count, name)` would be kept if pushed now. Lets a caller skip
    /// building the entry's `String` for the many that would not.
    #[inline]
    pub fn would_accept(&self, count: u64, name: &str) -> bool {
        if self.items.len() < self.max_length {
            return true;
        }
        self.items
            .get(self.worst)
            .is_some_and(|worst| ranks_ahead(count, name, worst))
    }

    pub fn push(&mut self, item: PostingStat) {
        if self.items.len() < self.max_length {
            if self
                .items
                .get(self.worst)
                .is_none_or(|worst| ranks_ahead(worst.count, &worst.name, &item))
            {
                self.worst = self.items.len();
            }
            self.items.push(item);
            return;
        }
        if !self.would_accept(item.count, &item.name) {
            return;
        }
        self.items[self.worst] = item;
        // Only an entry that displaces another pays for the rescan; in a scan in name order, ties
        // at the cutoff never do.
        self.worst = 0;
        for i in 1..self.items.len() {
            let (current, candidate) = (&self.items[self.worst], &self.items[i]);
            if ranks_ahead(current.count, &current.name, candidate) {
                self.worst = i;
            }
        }
    }

    /// The kept entries, highest count first, ties by ascending name.
    pub fn into_vec(self) -> Vec<PostingStat> {
        let mut items = self.items;
        items.sort_unstable_by(|a, b| b.count.cmp(&a.count).then_with(|| a.name.cmp(&b.name)));
        items
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stat(name: &str, count: u64) -> PostingStat {
        PostingStat {
            name: name.to_string(),
            count,
        }
    }

    fn kept(heap: StatsMaxHeap) -> Vec<(String, u64)> {
        heap.into_vec()
            .into_iter()
            .map(|stat| (stat.name, stat.count))
            .collect()
    }

    fn pairs(items: &[(&str, u64)]) -> Vec<(String, u64)> {
        items
            .iter()
            .map(|&(name, count)| (name.to_string(), count))
            .collect()
    }

    #[test]
    fn output_is_sorted_by_count_then_name() {
        let mut heap = StatsMaxHeap::new(10);
        for (name, count) in [("a", 1), ("b", 3), ("c", 2), ("d", 3)] {
            heap.push(stat(name, count));
        }
        assert_eq!(kept(heap), pairs(&[("b", 3), ("d", 3), ("c", 2), ("a", 1)]));
    }

    #[test]
    fn keeps_the_highest_counts_whatever_the_push_order() {
        let counts = [("f", 6), ("e", 5), ("d", 4), ("c", 3), ("b", 2), ("a", 1)];
        for rotation in 0..counts.len() {
            let mut heap = StatsMaxHeap::new(3);
            for &(name, count) in counts.iter().cycle().skip(rotation).take(counts.len()) {
                heap.push(stat(name, count));
            }
            assert_eq!(kept(heap), pairs(&[("f", 6), ("e", 5), ("d", 4)]));
        }
    }

    #[test]
    fn ties_at_the_cutoff_keep_the_smallest_names_in_any_order() {
        let names = ["e", "a", "d", "b", "c"];
        for rotation in 0..names.len() {
            let mut heap = StatsMaxHeap::new(2);
            for &name in names.iter().cycle().skip(rotation).take(names.len()) {
                heap.push(stat(name, 1));
            }
            assert_eq!(
                kept(heap),
                pairs(&[("a", 1), ("b", 1)]),
                "rotation {rotation}"
            );
        }
    }

    #[test]
    fn would_accept_matches_push() {
        let mut heap = StatsMaxHeap::new(2);
        assert!(heap.would_accept(0, "z"));
        heap.push(stat("b", 2));
        heap.push(stat("c", 1));
        assert!(!heap.would_accept(1, "d"), "a tie with a larger name");
        assert!(heap.would_accept(1, "a"), "a tie with a smaller name");
        assert!(heap.would_accept(2, "z"));
        assert!(!heap.would_accept(0, "a"));
    }

    #[test]
    fn a_zero_limit_keeps_nothing() {
        let mut heap = StatsMaxHeap::new(0);
        assert!(!heap.would_accept(u64::MAX, ""));
        heap.push(stat("a", u64::MAX));
        assert!(heap.into_vec().is_empty());
    }

    #[test]
    fn unbounded_keeps_everything_sorted() {
        let mut heap = StatsMaxHeap::new(usize::MAX);
        for i in 0..1000u64 {
            heap.push(stat(&format!("n{i:04}"), i % 7));
        }
        let kept = heap.into_vec();
        assert_eq!(kept.len(), 1000);
        assert!(kept.windows(2).all(
            |w| w[0].count > w[1].count || (w[0].count == w[1].count && w[0].name < w[1].name)
        ));
    }
}
