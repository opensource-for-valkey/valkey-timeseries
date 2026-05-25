use crate::common::context::{get_current_db, set_current_db};
use smallvec::SmallVec;
use std::num::NonZeroUsize;
use std::sync::LazyLock;
use std::sync::mpsc;
use std::thread;
use valkey_module::{Context, MODULE_CONTEXT};

pub type Task = Box<dyn FnOnce() + Send>;
pub type ValkeyTask<T = ()> = Box<dyn FnOnce(&Context) -> T + Send>;

const DEFAULT_VALKEY_TASK_WORKER_BATCH_SIZE: usize = 4;

pub struct ValkeyTaskWithPayload<P: Send, T = ()> {
    pub task: ValkeyTask<T>,
    pub payload: P,
}

/// Trait for processing work items in a batch worker.
///
/// Implement this trait to define custom processing logic for items submitted
/// to a `BatchWorker`. The trait is automatically implemented for closures and
/// functions, so you can use simple lambdas or pass stateful processors (structs).
pub trait BatchProcessor<ITEM, OUTPUT, ERROR>: Send + 'static {
    /// Process a single work item given the Valkey context.
    /// Returns Ok(output) on success or Err(error) on failure.
    fn process(&self, ctx: &Context, item: ITEM) -> Result<OUTPUT, ERROR>;
}

/// Blanket impl: closures/functions automatically implement BatchProcessor.
/// This allows you to pass any `Fn(&Context, ITEM) -> Result<OUTPUT, ERROR>` directly.
impl<F, ITEM, OUTPUT, ERROR> BatchProcessor<ITEM, OUTPUT, ERROR> for F
where
    F: Fn(&Context, ITEM) -> Result<OUTPUT, ERROR> + Send + 'static,
{
    fn process(&self, ctx: &Context, item: ITEM) -> Result<OUTPUT, ERROR> {
        self(ctx, item)
    }
}

/// A single batched request for a `BatchWorker`.
pub struct BatchRequest<W, O, E>
where
    W: Send + 'static,
    O: Send + 'static,
    E: Send + 'static,
{
    pub item: W,
    /// responder receives the processed result (Ok) or the error (Err)
    pub responder: mpsc::SyncSender<Result<O, E>>,
}

/// A worker class that allows safe, non-deadlocking access to the valkey keyspace. Batches incoming requests
/// and processes them in a single thread, holding the MODULE_CONTEXT lock for the duration of processing each batch.
///
/// This ensures that all operations on the keyspace are serialized and prevents deadlocks when multiple threads need to access the context.
///
/// ## Generic types:
///  - `ITEM` - the work item
///  - `OUTPUT` - the output type returned on success
///  - `ERROR` - the error type returned on failure
pub struct BatchWorker<ITEM, OUTPUT, ERROR>
where
    ITEM: Send + 'static,
    OUTPUT: Send + 'static,
    ERROR: Send + 'static,
{
    sender: mpsc::Sender<BatchRequest<ITEM, OUTPUT, ERROR>>,
}

impl<ITEM, OUTPUT, ERROR> BatchWorker<ITEM, OUTPUT, ERROR>
where
    ITEM: Send + 'static,
    OUTPUT: Send + 'static,
    ERROR: Send + 'static,
{
    /// Create a new batch worker with the given batch size and processor.
    ///
    /// The processor is moved into the worker thread and held for its lifetime.
    /// See the `BatchProcessor` trait for details on implementing custom processors.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Using a closure
    /// let worker = BatchWorker::new(
    ///     NonZeroUsize::new(32).unwrap(),
    ///     |ctx, item| {
    ///         // process item
    ///         Ok(result)
    ///     },
    /// );
    ///
    /// // Using a struct with state
    /// struct MyProcessor { cache: Mutex<HashMap<...>> }
    /// impl BatchProcessor<MyItem, MyOut, MyErr> for MyProcessor { ... }
    /// let worker = BatchWorker::new(
    ///     NonZeroUsize::new(32).unwrap(),
    ///     MyProcessor { cache: ... },
    /// );
    /// ```
    pub fn new<P>(batch_size: NonZeroUsize, processor: P) -> Self
    where
        P: BatchProcessor<ITEM, OUTPUT, ERROR>,
    {
        let (tx, rx) = mpsc::channel::<BatchRequest<ITEM, OUTPUT, ERROR>>();
        let batch_size = batch_size.get();
        thread::spawn(move || {
            // Worker loop: wait for the first request, then drain additional pending requests
            // to form a batch. Hold the MODULE_CONTEXT for the duration of processing
            // the batch to serialize access to valkey keyspace.
            loop {
                let first = match rx.recv() {
                    Ok(r) => r,
                    Err(_) => break, // channel closed, exit thread
                };

                let mut batch: SmallVec<BatchRequest<ITEM, OUTPUT, ERROR>, 6> = SmallVec::new();
                batch.push(first);

                let mut count = 1;
                while count < batch_size
                    && let Ok(r) = rx.try_recv()
                {
                    batch.push(r);
                    count += 1;
                }

                {
                    let ctx = MODULE_CONTEXT.lock();
                    let saved_db = get_current_db(&ctx);
                    for req in batch {
                        let res = processor.process(&ctx, req.item);
                        // Restore the original database after processing each item to prevent side
                        // effects between requests in the batch
                        set_current_db(&ctx, saved_db);
                        req.responder.send(res).unwrap_or_else(|e| {
                            eprintln!("Failed to send batch response: {}", e);
                        });
                    }
                }
            }
        });

        BatchWorker { sender: tx }
    }

    /// Get a reference to the inner sender.
    pub fn sender(&self) -> &mpsc::Sender<BatchRequest<ITEM, OUTPUT, ERROR>> {
        &self.sender
    }

    pub fn send(&self, item: ITEM) -> Option<Result<OUTPUT, ERROR>> {
        let (responder_tx, responder_rx) = mpsc::sync_channel(1);
        let request = BatchRequest {
            item,
            responder: responder_tx,
        };
        self.sender.send(request).unwrap_or_else(|e| {
            eprintln!("Failed to send query request: {}", e);
        });

        match responder_rx.recv() {
            Ok(res) => match res {
                Ok(val) => Some(Ok(val)),
                Err(err) => Some(Err(err)),
            },
            Err(e) => {
                let msg = format!("Failed to receive query response: {}", e);
                eprintln!("{}", msg);
                None
            }
        }
    }

    pub fn send_and_forget(&self, item: ITEM) {
        let (responder_tx, _responder_rx) = mpsc::sync_channel(1);
        let request = BatchRequest {
            item,
            responder: responder_tx,
        };
        self.sender.send(request).unwrap_or_else(|e| {
            eprintln!("Failed to send query request: {}", e);
        });
    }
}

/// Dispatcher for generic thread-based tasks requiring access to the Valkey keyspace.
pub struct ValkeyTaskDispatcher;

impl<T: Send + 'static, E: Send + 'static> BatchProcessor<ValkeyTask<T>, T, E>
for ValkeyTaskDispatcher
{
    fn process(&self, ctx: &Context, task: ValkeyTask<T>) -> Result<T, E> {
        Ok(task(ctx))
    }
}

pub type ValkeyTaskWorker<T, E> = BatchWorker<ValkeyTask<T>, T, E>;

static GLOBAL_VALKEY_TASK_WORKER: LazyLock<ValkeyTaskWorker<(), String>> = LazyLock::new(|| {
    let batch_size = NonZeroUsize::new(DEFAULT_VALKEY_TASK_WORKER_BATCH_SIZE)
        .expect("DEFAULT_VALKEY_TASK_WORKER_BATCH_SIZE must be non-zero");
    BatchWorker::new(batch_size, ValkeyTaskDispatcher)
});

pub fn global_valkey_task_worker() -> &'static ValkeyTaskWorker<(), String> {
    &GLOBAL_VALKEY_TASK_WORKER
}

pub(crate) fn submit_valkey_task(task: ValkeyTask<()>) -> Option<Result<(), String>> {
    global_valkey_task_worker().send(task)
}

pub(crate) fn submit_valkey_task_and_forget(task: ValkeyTask<()>) {
    global_valkey_task_worker().send_and_forget(task);
}

pub(crate) fn send_with_payload<F, T>(
    task: F,
    payload: T,
) -> Result<(), String>
where
    T: Send + 'static,
    F: Fn(&Context, T) -> Result<(), String> + Send + 'static,
{
    let closure: ValkeyTask<()> = Box::new(move |ctx: &Context| {
        let _ = task(ctx, payload);
    });

    let Some(result) = submit_valkey_task(closure) else {
        return Err("Failed to submit valkey task with payload".to_string());
    };
    result
}


pub(crate) fn exec_with_payload<F, T, R>(
    task: F,
    payload: T,
) -> Result<R, String>
where
    T: Send + 'static,
    R: Send + 'static,
    F: Fn(&Context, T) -> Result<R, String> + Send + 'static,
{
    let (tx, rx) = mpsc::sync_channel(1);
    let closure: ValkeyTask<()> = Box::new(move |ctx: &Context| {
        let result = task(ctx, payload);
        tx.send(result).unwrap();
    });

    let Some(_result) = submit_valkey_task(closure) else {
        return Err("Failed to submit valkey task with payload".to_string());
    };

    rx.recv().map_err(|e| e.to_string())?
}