use super::role::on_pinned_worker;
use core::num::NonZeroUsize;
use orx_parallel::{
    IntoParIter, IterIntoParIter, Par, ParCollection, ParCollectionMut, Parallelizable, Runner,
    ThreadPool,
};

/// orx-parallel adapter that runs a computation on orx's own rayon-core pool
/// (`orx_parallel::Pool::global()`, sized from `ts-num-threads` in
/// [`init_thread_pool`](super::init_thread_pool)), except on a worker of a pinned pool
/// (see [`ThreadRole::is_pinned`](super::ThreadRole::is_pinned)), where it stays on that pool.
///
/// A bare `.par()` would always use orx's pool, including from a pinned worker, and so would
/// move a PromQL evaluation's blocking jobs onto the pool that module-lock holders wait on.
/// Enter parallel iteration through the `*_rayon` methods below rather than the bare orx
/// entry points.
#[derive(Clone, Copy, Debug, Default)]
pub struct ModulePool;

impl ThreadPool for ModulePool {
    type ScopeRef<'s, 'env, 'scope>
        = &'s rayon_core::Scope<'scope>
    where
        'scope: 's,
        'env: 'scope + 's;

    fn scope<'env, 'scope, F>(&'env self, f: F)
    where
        'env: 'scope,
        for<'s> F: FnOnce(&'s rayon_core::Scope<'scope>) + Send,
    {
        if on_pinned_worker() {
            rayon_core::scope(f)
        } else {
            rayon_core::ThreadPool::scope(orx_parallel::Pool::global(), f)
        }
    }

    fn max_num_threads(&self) -> NonZeroUsize {
        let threads = if on_pinned_worker() {
            rayon_core::current_num_threads()
        } else {
            orx_parallel::Pool::global().current_num_threads()
        };
        NonZeroUsize::new(threads.max(1)).expect(">0")
    }
}

/// orx-parallel adapter over one specific `rayon_core::ThreadPool`, for a
/// computation that must not depend on a shared pool — the PromQL selector
/// executor's materialization, which runs while evaluation workers may be parked
/// waiting for it. Attach with `.with_pool(RayonPool(&pool))`.
#[derive(Clone, Copy)]
pub struct RayonPool(pub &'static rayon_core::ThreadPool);

impl ThreadPool for RayonPool {
    type ScopeRef<'s, 'env, 'scope>
        = &'s rayon_core::Scope<'scope>
    where
        'scope: 's,
        'env: 'scope + 's;

    fn scope<'env, 'scope, F>(&'env self, f: F)
    where
        'env: 'scope,
        for<'s> F: FnOnce(&'s rayon_core::Scope<'scope>) + Send,
    {
        self.0.scope(f)
    }

    fn max_num_threads(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.0.current_num_threads().max(1)).expect(">0")
    }
}

/// orx 3's `.with_pool(pool)`, which 4.0 replaced with `.runner(..)`: runs the computation on
/// `pool` with fixed chunk sizing — the executor 3.x attached there (4.0's default runner
/// chunks adaptively).
pub trait ParWithPool: Par {
    fn with_pool<P: ThreadPool + Sync>(self, pool: P) -> impl Par<Item = Self::Item> {
        self.runner(Runner::fixed_with_pool(pool))
    }
}

impl<T: Par> ParWithPool for T {}

/// `.par()` on [`ModulePool`], for borrowed sources such as `&[T]` and ranges.
///
/// orx splits `.par()` across two disjoint blanket traits ([`Parallelizable`] for types that
/// are themselves concurrent-iterable, [`ParCollection`] for owning collections
/// like `Vec<T>`), so `par_rayon` is split the same way; both resolve at the call site
/// exactly where orx's own `.par()` does.
pub trait ParRayon: Parallelizable {
    fn par_rayon(&self) -> impl Par<Item = Self::Item> {
        self.par().with_pool(ModulePool)
    }
}

impl<T: Parallelizable> ParRayon for T {}

/// `.par()` on [`ModulePool`], for owning collections such as `Vec<T>`.
pub trait ParCollectionRayon: ParCollection {
    fn par_rayon(&self) -> impl Par<Item = &Self::Item> {
        self.par().with_pool(ModulePool)
    }
}

impl<T: ParCollection> ParCollectionRayon for T {}

/// `.par_mut()` on [`ModulePool`].
pub trait ParMutRayon: ParCollectionMut {
    fn par_mut_rayon(&mut self) -> impl Par<Item = &mut Self::Item> {
        self.par_mut().with_pool(ModulePool)
    }
}

impl<T: ParCollectionMut> ParMutRayon for T {}

/// `.into_par()` on [`ModulePool`].
pub trait IntoParRayon: IntoParIter {
    fn into_par_rayon(self) -> impl Par<Item = Self::Item>
    where
        Self: Sized,
    {
        self.into_par().with_pool(ModulePool)
    }

    /// `.into_par()` on one specific pool, for a computation that must not depend on the
    /// caller's (see [`RayonPool`]).
    fn into_par_on(self, pool: &'static rayon_core::ThreadPool) -> impl Par<Item = Self::Item>
    where
        Self: Sized,
    {
        self.into_par().with_pool(RayonPool(pool))
    }
}

impl<T: IntoParIter> IntoParRayon for T {}

/// `.iter_into_par()` on [`ModulePool`].
pub trait IterIntoParRayon: IterIntoParIter {
    fn iter_into_par_rayon(self) -> impl Par<Item = Self::Item>
    where
        Self: Sized,
        Self::Item: Send,
    {
        self.iter_into_par().with_pool(ModulePool)
    }
}

impl<T: IterIntoParIter> IterIntoParRayon for T {}

#[cfg(test)]
mod tests {
    use super::{ModulePool, ParCollectionRayon, ParRayon};
    use crate::common::threads::{ThreadRole, set_thread_role};
    use orx_parallel::{Par, Pool, ThreadPool};

    fn names_of_workers(inputs: &[u64]) -> Vec<String> {
        inputs
            .par_rayon()
            .map(|_| std::thread::current().name().unwrap_or("").to_string())
            .collect()
    }

    #[test]
    fn unpinned_callers_run_on_the_orx_pool() {
        let inputs: Vec<u64> = (0..10_000).collect();
        assert_eq!(inputs.par_rayon().sum(), (0..10_000u64).sum::<u64>());
        let on_orx = inputs
            .par_rayon()
            .map(|_| Pool::global().current_thread_index().is_some())
            .collect::<Vec<_>>();
        assert!(on_orx.into_iter().all(|on| on));

        // A worker of some other, unpinned pool also hands its work to orx's pool.
        let other = rayon_core::ThreadPoolBuilder::new()
            .num_threads(2)
            .thread_name(|i| format!("orx-unpinned-test-{i}"))
            .build()
            .unwrap();
        let names = other.install(|| names_of_workers(&inputs));
        assert!(
            names.iter().all(|n| !n.starts_with("orx-unpinned-test-")),
            "{names:?}"
        );
    }

    #[test]
    fn pinned_workers_keep_their_work() {
        let pool = rayon_core::ThreadPoolBuilder::new()
            .num_threads(3)
            .thread_name(|i| format!("orx-pinned-test-{i}"))
            .start_handler(|_| set_thread_role(ThreadRole::BlockingPool))
            .build()
            .unwrap();
        let inputs: Vec<u64> = (0..10_000).collect();
        let (sum, names, threads) = pool.install(|| {
            (
                inputs.par_rayon().sum(),
                names_of_workers(&inputs),
                ModulePool.max_num_threads().get(),
            )
        });
        assert_eq!(sum, (0..10_000u64).sum::<u64>());
        assert!(
            names.iter().all(|n| n.starts_with("orx-pinned-test-")),
            "{names:?}"
        );
        assert_eq!(threads, 3);
    }

    #[test]
    fn max_threads_tracks_the_orx_pool_when_unpinned() {
        assert_eq!(
            ModulePool.max_num_threads().get(),
            Pool::global().current_num_threads().max(1)
        );
    }
}
