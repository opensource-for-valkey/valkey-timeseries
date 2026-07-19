mod batch_worker;

use crate::common::context::{get_current_db, set_current_db};
use crate::is_main_thread;
#[allow(unused_imports)]
pub(crate) use batch_worker::{
    BatchRequest, BatchWorker, exec_with_payload, global_valkey_task_worker, send_with_payload,
    submit_valkey_task, submit_valkey_task_and_forget,
};
use rayon_core::{Scope, ThreadPoolBuilder};
use std::os::raw::c_void;
use valkey_module::logging::log_notice;
use valkey_module::{Context, MODULE_CONTEXT, raw};

/// Builds the module's global rayon thread pool, sized from `config::NUM_THREADS`
/// (`ts-num-threads`). Config registration runs before this in `initialize()`, so the config
/// value is already resolved (from `valkey.conf`/`MODULE LOAD` args, or its default) by the
/// time this reads it. `ts-num-threads` is registered `IMMUTABLE` because rayon's global pool
/// cannot be resized once built — there is no later point at which this needs to re-run.
pub fn init_thread_pool() {
    let threads = crate::config::num_threads();
    log_notice(format!("Setting number of threads to {threads}"));
    ThreadPoolBuilder::new()
        .num_threads(threads)
        .thread_name(|index| format!("valkey-timeseries-{index}"))
        .build_global()
        .unwrap();
}

/// Spawn a job which runs asynchronously.
/// The job must be `'static` and thus cannot borrow local variables.
pub fn spawn<F: FnOnce() + Send + 'static>(job: F) {
    rayon_core::spawn(job)
}

/// Spawn a job in the context of a valkey GIL (Global Interpreter Lock).
pub fn spawn_with_context<F: FnOnce(&Context) + Send + 'static>(job: F) {
    spawn(move || {
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
    spawn_scoped(|s| rayon_core::join(|| oper_a(s), || oper_b(s)))
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
/// ```no_run
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
