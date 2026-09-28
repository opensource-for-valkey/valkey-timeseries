mod executor;

pub use executor::{BoundedExecutor, ExecutorBusy};

use crate::common::context::{get_current_db, set_current_db};
use crate::is_main_thread;
use rayon_core::Scope;
use std::env;
use std::os::raw::c_void;
use valkey_module::logging::log_notice;
use valkey_module::{Context, MODULE_CONTEXT, raw};

const MAX_NUM_THREADS_ENV_VARIABLE: &str = "ORX_NUM_THREADS";

/// Sizes and builds the rayon-core pool that runs every orx-parallel `.par()` computation.
///
/// With the `persistent-pool-rayon` feature, orx's default runner executes on a dedicated
/// rayon-core pool (not rayon's global pool used by [`spawn`]/[`join`]). orx builds it lazily on
/// first use, sized from `ORX_NUM_THREADS` capped at the core count, and never resizes it. So the
/// variable must be set before anything touches the pool, and we force the build here rather than
/// leave it to the first command — which would pay for spawning the workers, and would size the
/// pool to every core if it ran before this function.
///
/// Must run after the module config is loaded (`ts-num-threads`).
pub fn init_thread_pool() {
    let threads = crate::config::num_threads();
    unsafe {
        env::set_var(MAX_NUM_THREADS_ENV_VARIABLE, threads.to_string());
    }
    let actual = orx_parallel::Pool::global().current_num_threads();
    if actual != threads {
        log_notice(format!(
            "parallel query pool has {actual} threads (ts-num-threads={threads}, capped at the \
             available cores)"
        ));
    }
}

/// Spawn a job which runs asynchronously.
/// The job must be `'static` and thus cannot borrow local variables.
///
/// The job runs on a pool worker, so it must not take the module lock: see
/// [`spawn_background`].
pub fn spawn<F: FnOnce() + Send + 'static>(job: F) {
    rayon_core::spawn(job)
}

/// Spawn a job on its own thread, off the rayon pool.
///
/// For jobs that take `MODULE_CONTEXT` (the module GIL). A pool worker that
/// holds the GIL and then waits on the pool — a `scope`, a `join`, a
/// `par_*` fan-out — waits by stealing whatever job is pending. If that job
/// takes the GIL too (a fan-out request, the trim cron, an index sweep), the
/// worker blocks on a lock it already holds; and once every worker is parked
/// on the GIL, a main-thread command that waits on the pool while holding it
/// (TS.JOIN) never gets a worker back. Either way the server freezes. A
/// detached thread waits on the pool without stealing, so a GIL holder here
/// can never pick up such a job.
///
/// One thread per call, so only for jobs with a bounded number of callers (one-shot
/// work, or a periodic task guarded against overlapping runs). Per-request work goes
/// to a [`BoundedExecutor`] instead.
pub fn spawn_background<F: FnOnce() + Send + 'static>(name: &str, job: F) {
    if let Err(err) = std::thread::Builder::new()
        .name(name.to_string())
        .spawn(job)
    {
        log_notice(format!("failed to spawn background thread {name}: {err}"));
    }
}

/// Spawn a job that runs holding the valkey GIL (module lock).
///
/// On its own thread, not the pool: see [`spawn_background`].
pub fn spawn_with_context<F: FnOnce(&Context) + Send + 'static>(job: F) {
    spawn_background("ts-with-context", move || {
        let ctx = MODULE_CONTEXT.lock();
        job(&ctx);
    });
}

/// Spawn scoped jobs which guarantee to be finished before this method returns and thus allows
/// borrowing local variables.
pub fn spawn_scoped<'scope, OP, R>(op: OP) -> R
where
    OP: FnOnce(&Scope<'scope>) -> R + Send,
    R: Send,
{
    rayon_core::scope(op)
}

pub fn join<A, B, RA, RB>(oper_a: A, oper_b: B) -> (RA, RB)
where
    A: Send + FnOnce() -> RA,
    B: Send + FnOnce() -> RB,
    RA: Send,
    RB: Send,
{
    rayon_core::join(oper_a, oper_b)
}

pub fn join_scoped<'scope, A, B, RA, RB>(oper_a: A, oper_b: B) -> (RA, RB)
where
    A: Send + FnOnce(&Scope<'scope>) -> RA + 'scope,
    B: Send + FnOnce(&Scope<'scope>) -> RB + 'scope,
    RA: Send + 'scope,
    RB: Send + 'scope,
{
    // does this make sense?
    spawn_scoped(|s| join(|| oper_a(s), || oper_b(s)))
}

extern "C" fn event_loop_callback_wrapper<F>(data: *mut c_void)
where
    F: FnOnce() + 'static,
{
    let callback: Box<F> = unsafe { Box::from_raw(data as *mut F) };
    callback();
}

/// Runs `callback` with a module [`Context`] while already on the main thread.
///
/// The caller must be on the main thread with the module GIL held (true for command handlers,
/// server-event callbacks, and event-loop one-shot callbacks). We therefore must NOT lock a
/// thread-safe/detached context — that re-acquires the GIL and self-deadlocks. Instead we obtain a
/// throwaway thread-safe context (which does no locking) and use it directly.
fn with_main_thread_context<F>(callback: F)
where
    F: FnOnce(&Context),
{
    let raw_ctx = unsafe { raw::RedisModule_GetThreadSafeContext.unwrap()(std::ptr::null_mut()) };
    let ctx = Context::new(raw_ctx);
    let saved_db = get_current_db(&ctx);
    callback(&ctx);
    set_current_db(&ctx, saved_db);
    unsafe { raw::RedisModule_FreeThreadSafeContext.unwrap()(raw_ctx) };
}

extern "C" fn event_loop_callback_wrapper_with_context<F>(data: *mut c_void)
where
    F: FnOnce(&Context) + 'static,
{
    let callback: Box<F> = unsafe { Box::from_raw(data as *mut F) };
    with_main_thread_context(|ctx| callback(ctx));
}

/// Executes a given closure on the Valkey main thread. The provided closure will be executed as a one-shot operation.
///
/// # Parameters
/// - `force_async`: If true, the closure will be executed asynchronously even if it's already on the main thread.
/// - `callback`: The closure to be executed on the main thread.
///
/// # Example
/// ```rust,no_run
/// use valkey_timeseries::common::threads::run_on_main_thread;
///
/// // A simple closure to be executed on the main thread
/// run_on_main_thread(false, || {
///     println!("This is running on the main thread!");
/// });
/// ```
pub fn run_on_main_thread<F>(force_async: bool, callback: F)
where
    F: FnOnce() + Send + 'static,
{
    if is_main_thread() && !force_async {
        callback();
        return;
    }

    // Move the closure to the heap so it has a stable memory address
    let boxed_callback = Box::new(callback);
    let raw_data = Box::into_raw(boxed_callback) as *mut c_void;

    let event_loop_callback = event_loop_callback_wrapper::<F>;

    unsafe {
        raw::ValkeyModule_EventLoopAddOneShot.unwrap()(Some(event_loop_callback), raw_data);
    }
}

/// Executes a given closure on the Valkey main thread, providing a reference to the module [`Context`].
/// The provided closure will be executed as a one-shot operation.
///
/// # Parameters
/// - `force_async`: If true, the closure will be executed asynchronously even if it's already on the main thread.
/// - `callback`: The closure to be executed on the main thread. It receives a reference to the module [`Context`].
///
/// # Example
/// ```rust,no_run
/// use valkey_module::Context;
/// use valkey_timeseries::common::threads::run_on_main_thread_with_context;
///
/// // A simple closure to be executed on the main thread with access to Context
/// run_on_main_thread_with_context(false, |ctx: &Context| {
///     ctx.log_notice("This is running on the main thread with context!");
/// });
/// ```
pub fn run_on_main_thread_with_context<F>(force_async: bool, callback: F)
where
    F: FnOnce(&Context) + Send + 'static,
{
    if is_main_thread() && !force_async {
        with_main_thread_context(callback);
        return;
    }

    // Move the closure to the heap so it has a stable memory address
    let boxed_callback = Box::new(callback);
    let raw_data = Box::into_raw(boxed_callback) as *mut c_void;

    let event_loop_callback = event_loop_callback_wrapper_with_context::<F>;

    unsafe {
        raw::ValkeyModule_EventLoopAddOneShot.unwrap()(Some(event_loop_callback), raw_data);
    }
}

#[cfg(test)]
mod tests {
    use super::join;
    use orx_parallel::{Par, ParCollection, Parallelizable, Pool};

    /// Whether every item of a parallel computation ran on a worker of orx's rayon pool.
    fn all_on_orx_pool(items: &[usize]) -> bool {
        let pool = Pool::global();
        items
            .par()
            .map(|_| pool.current_thread_index().is_some())
            .collect::<Vec<_>>()
            .into_iter()
            .all(|on_pool| on_pool)
    }

    #[test]
    fn par_runs_on_the_orx_rayon_pool() {
        let items: Vec<usize> = (0..10_000).collect();
        assert!(Pool::global().current_thread_index().is_none());
        assert!(all_on_orx_pool(&items));
    }

    #[test]
    fn nested_par_stays_on_the_orx_rayon_pool() {
        let outer: Vec<usize> = (0..64).collect();
        let inner: Vec<usize> = (0..1_000).collect();
        let all = outer
            .par()
            .map(|_| all_on_orx_pool(&inner))
            .collect::<Vec<_>>();
        assert!(all.into_iter().all(|on_pool| on_pool));
    }

    #[test]
    fn par_from_the_global_rayon_pool_runs_on_the_orx_pool() {
        let items: Vec<usize> = (0..10_000).collect();
        let (a, b) = join(|| all_on_orx_pool(&items), || all_on_orx_pool(&items));
        assert!(a && b);
    }
}
