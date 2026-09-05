use std::sync::atomic::{AtomicUsize, Ordering};

/// PromQL workers hold their permit from command admission until their delayed
/// reply is complete. This bounds both queued Rayon jobs and blocked clients.
static ACTIVE_QUERIES: AtomicUsize = AtomicUsize::new(0);

pub(crate) struct QueryPermit<'a> {
    active: &'a AtomicUsize,
}

impl Drop for QueryPermit<'_> {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::Release);
    }
}

fn try_acquire_query_permit(active: &AtomicUsize, limit: usize) -> Option<QueryPermit<'_>> {
    let mut current = active.load(Ordering::Acquire);
    loop {
        if current >= limit {
            return None;
        }
        match active.compare_exchange_weak(
            current,
            current + 1,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return Some(QueryPermit { active }),
            Err(observed) => current = observed,
        }
    }
}

/// Reserve capacity for one PromQL command. The ceiling matches the module's
/// immutable Rayon pool size, so admitted commands can make progress without
/// creating additional operating-system threads.
pub(crate) fn try_acquire_promql_query() -> Option<QueryPermit<'static>> {
    try_acquire_query_permit(&ACTIVE_QUERIES, crate::config::num_threads().max(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admission_rejects_at_capacity_and_releases_on_drop() {
        let active = AtomicUsize::new(0);
        let first = try_acquire_query_permit(&active, 1).expect("first query is admitted");
        assert!(try_acquire_query_permit(&active, 1).is_none());

        drop(first);

        assert!(try_acquire_query_permit(&active, 1).is_some());
    }
}
