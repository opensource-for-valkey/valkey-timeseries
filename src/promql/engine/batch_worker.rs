use crate::common::threads::spawn;
use std::sync::mpsc;
use valkey_module::{Context, MODULE_CONTEXT};

/// A single batched request for a `BatchWorker`.
pub struct BatchRequest<W, O, E> {
    pub item: W,
    /// responder receives the processed result (Ok) or the error (Err)
    pub responder: mpsc::SyncSender<Result<O, E>>,
}

/// A trait for workers that process items in batches on a dedicated thread which
/// holds the Valkey `Context` (GIL) while servicing a batch of requests.
///
/// Implementors must provide the `process` method. A default `spawn_worker`
/// implementation is provided which will drive a worker loop and service incoming
/// `BatchRequest`s in batches, minimizing repeated lock acquisitions on the
/// global `MODULE_CONTEXT`.
pub trait BatchWorker: Send + Sync + 'static {
    /// The work item type
    type WorkItem: Send + 'static;

    /// The output type returned on success
    type Output: Send + 'static;

    /// The error type returned on failure
    type Error: Send + 'static;

    /// Process a single work item in the context of the provided Valkey `Context`.
    /// This is fallible: return `Ok(output)` on success or `Err(error)` on failure.
    fn process(&self, ctx: &Context, item: Self::WorkItem) -> Result<Self::Output, Self::Error>;

    /// Spawn the worker thread and return a channel Sender that accepts `BatchRequest`s.
    ///
    /// The default implementation consumes `self` and moves it into the spawned thread.
    fn spawn_worker(self) -> BatchWorkerRunner<Self>
    where
        Self: Sized + Send + 'static,
    {
        let (tx, rx) = mpsc::channel::<BatchRequest<Self::WorkItem, Self::Output, Self::Error>>();

        spawn(move || {
            // Worker loop: wait for the first request, then drain additional pending requests
            // to form a batch. Hold the MODULE_CONTEXT for the duration of processing
            // the batch to serialize access to valkey keyspace.
            loop {
                let first = match rx.recv() {
                    Ok(r) => r,
                    Err(_) => break, // channel closed, exit thread
                };

                let mut batch = vec![first];
                while let Ok(r) = rx.try_recv() {
                    batch.push(r);
                }

                let ctx = MODULE_CONTEXT.lock();
                for req in batch {
                    let res = self.process(&ctx, req.item);
                    req.responder.send(res).unwrap_or_else(|e| {
                        eprintln!("Failed to send batch response: {}", e);
                    });
                }
            }
        });

        BatchWorkerRunner { sender: tx }
    }
}

/// A small handle returned when spawning a batch worker. It contains the channel
/// sender used to submit requests to the worker.
pub struct BatchWorkerRunner<W: BatchWorker> {
    pub sender: mpsc::Sender<BatchRequest<W::WorkItem, W::Output, W::Error>>,
}

impl<W: BatchWorker> BatchWorkerRunner<W> {
    /// Get a reference to the inner sender.
    pub fn sender(&self) -> &mpsc::Sender<BatchRequest<W::WorkItem, W::Output, W::Error>> {
        &self.sender
    }
}

pub fn send_batch_request<W>(
    runner: &BatchWorkerRunner<W>,
    item: W::WorkItem,
) -> Option<Result<W::Output, W::Error>>
where
    W: BatchWorker,
{
    let (responder_tx, responder_rx) = mpsc::sync_channel(1);
    let request = BatchRequest {
        item,
        responder: responder_tx,
    };
    runner.sender.send(request).unwrap_or_else(|e| {
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
