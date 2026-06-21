use crate::common::context::{get_current_db, set_current_db};
use crate::common::logging::log_warning;
use crate::common::time::current_time_millis;
use crate::common::{Sample, Timestamp};
use crate::fanout::{FanoutCommand, is_clustered};
use crate::fanout::{FanoutCommandResult, exec_command, get_cluster_command_timeout};
use crate::labels::Labels;
use crate::labels::filters::SeriesSelector;
use crate::promql::engine::{
    InstantVectorSelectorFanoutCommand, RangeVectorSelectorFanoutCommand, instant_lookback_start_ms, proto_labels_to_labels,
    validate_max_points, validate_max_series,
};
use crate::promql::{
    InstantSample, QueryError, QueryOptions, QueryResult, QueryValue, RangeSample,
};
use crate::series::index::series_by_selectors;
use orx_parallel::IterIntoParIter;
use orx_parallel::ParIter;
use promql_parser::label::Matchers;
use std::ops::Deref;
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;
use valkey_module::{Context, MODULE_CONTEXT};

/// Max number of requests to process in a single batch to
/// avoid excessively locking the GIL and starving other tasks.
const MAX_BATCH_SIZE: usize = 4;

struct InstantVectorSelectorCommand {
    matchers: Matchers,
    timestamp: Timestamp,
    options: QueryOptions,
}

struct RangeSelectorCommand {
    matchers: Matchers,
    start_timestamp: Timestamp,
    end_timestamp: Timestamp,
    options: QueryOptions,
}

enum SelectorCommand {
    Vector(InstantVectorSelectorCommand),
    Range(RangeSelectorCommand),
}

impl SelectorCommand {
    fn db(&self) -> i32 {
        match self {
            SelectorCommand::Vector(iqc) => iqc.options.db,
            SelectorCommand::Range(rc) => rc.options.db,
        }
    }
}

/// A single batched request for a `QueryWorker`.
struct SelectorRequest {
    item: SelectorCommand,
    /// responder receives the processed result (Ok) or the error (Err)
    responder: mpsc::SyncSender<QueryResult<QueryValue>>,
}

impl SelectorRequest {
    fn db(&self) -> i32 {
        self.item.db()
    }
}

/// A worker responsible for executing PromQL selectors as part of a keyspace batch operation.
///
/// The `QueryWorker` optimizes latency in the PromQL evaluator (especially in cluster mode) by:
///
/// - Serializing access to the Valkey keyspace via `MODULE_CONTEXT` to avoid deadlocks, ensuring that
///   we can query safely from multiple threads.
/// - Collecting incoming query requests into a batch and processing the batch in a single lock
///   acquisition to reduce locking overhead.
///
/// # Design
///
/// Unlike a traditional worker that spawns a dedicated background thread, `QueryWorker` uses
/// **cooperative batching**: each caller that sends a request attempts to become the "processor"
/// by acquiring the receiver lock. The processor drains all pending requests from the shared
/// channel with `try_recv()`, processes them under a single `MODULE_CONTEXT` acquisition, then
/// releases the lock. This eliminates the need for a dedicated thread while preserving batching
/// behaviour.
///
/// For local queries, the worker processes the request directly, so processing is essentially serialized.
/// For cluster queries, a synchronous call is made per query and the context is released. The processing itself
/// is executed in parallel across all target cluster nodes, and results are returned asynchronously without 
/// holding the GIL.
///  
/// This design allows us to achieve good performance without needing multiple background threads for processing queries concurrently.
///
/// # Note
/// The `QueryWorker` is designed for internal use within the PromQL engine and is not intended to be
/// used directly by external callers. It is exposed as a handle that can be used to perform queries,
/// but the internal implementation details are abstracted away.
///
/// # Important Considerations
///  - Reuse the same `QueryWorker` instance (it is a global `LazyLock`).
///  - The processor holds `MODULE_CONTEXT` while processing requests, so it should not be used for
///    long-running or blocking operations to avoid starving other tasks that require the GIL.
///
/// # Example
/// ```ignore
/// use crate::promql::engine::query_worker::QueryWorker;
/// use crate::promql::engine::QueryOptions;
/// use promql_parser::label::Matchers;
///
///
/// // Create a worker handle and perform queries via the provided API.
/// let query_worker = QueryWorker::new();
/// let now = current_time_millis();
/// let options = QueryOptions {
///     timeout: Some(now + 60_000), // 1 minute from now
///     lookback_delta: None,
///     max_series: 1000,
/// };
/// let matchers = vec![Matcher::new("job", "=", "prometheus")];
/// let _ = query_worker.query(matchers, now, options);
/// ```
pub struct QueryWorker {
    sender: mpsc::Sender<SelectorRequest>,
    receiver: Mutex<mpsc::Receiver<SelectorRequest>>,
}

impl QueryWorker {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel();
        Self {
            sender: tx,
            receiver: Mutex::new(rx),
        }
    }

    pub fn query(
        &self,
        matchers: Matchers,
        timestamp: Timestamp,
        options: QueryOptions,
    ) -> QueryResult<QueryValue> {
        let command = SelectorCommand::Vector(InstantVectorSelectorCommand {
            matchers,
            timestamp,
            options,
        });
        self.send_request(command)
    }

    pub fn query_range(
        &self,
        matchers: Matchers,
        start: Timestamp,
        end: Timestamp,
        options: QueryOptions,
    ) -> QueryResult<QueryValue> {
        let command = SelectorCommand::Range(RangeSelectorCommand {
            matchers,
            start_timestamp: start,
            end_timestamp: end,
            options,
        });
        self.send_request(command)
    }

    fn send_request(&self, command: SelectorCommand) -> QueryResult<QueryValue> {
        let (responder_tx, responder_rx) = mpsc::sync_channel(1);
        let request = SelectorRequest {
            item: command,
            responder: responder_tx,
        };

        // Send our request to the shared channel.
        if let Err(e) = self.sender.send(request) {
            let msg = format!("promql: failed to send query request to worker: {e}");
            log_warning(msg.clone());
            return Err(QueryError::Execution(msg));
        }

        // Try to become the processor. If another caller is already processing,
        // we simply wait for our result — they will handle our request too.
        self.try_process_batches();

        match responder_rx.recv() {
            Ok(res) => match res {
                Ok(val) => Ok(val),
                Err(err) => Err(err),
            },
            Err(e) => {
                let msg = format!("Failed to receive query response: {}", e);
                Err(QueryError::Execution(msg))
            }
        }
    }

    /// Attempt to become the batch processor. Drains all pending requests from
    /// the shared channel and processes them under a single `MODULE_CONTEXT`
    /// acquisition. Repeats until the channel is empty.
    ///
    /// The receiver lock is released between drain-and-process cycles so that
    /// other callers can step in if needed, and to avoid holding the lock
    /// across potentially long `MODULE_CONTEXT` critical sections.
    fn try_process_batches(&self) {
        loop {
            let batch = {
                // Scope the receiver lock to just draining — release before
                // we acquire MODULE_CONTEXT to avoid lock inversion.
                let rx = match self.receiver.try_lock() {
                    Ok(rx) => rx,
                    Err(std::sync::TryLockError::WouldBlock) => {
                        // Another thread is currently draining / processing.
                        // Our request is already in the channel; the other
                        // thread will pick it up in its next drain iteration.
                        return;
                    }
                    Err(std::sync::TryLockError::Poisoned(_)) => {
                        // A previous processor panicked. The channel is still
                        // intact; continue and recover.
                        continue;
                    }
                };

                let mut batch = Vec::with_capacity(MAX_BATCH_SIZE);
                while let Ok(req) = rx.try_recv() {
                    batch.push(req);
                    if batch.len() >= MAX_BATCH_SIZE {
                        break;
                    }
                }
                batch
                // MutexGuard dropped here — receiver lock released
            };

            if batch.is_empty() {
                break; // nothing left to process
            }

            let ctx = MODULE_CONTEXT.lock();
            for req in batch {
                process_request(&ctx, req);
            }
            // ctx dropped here — MODULE_CONTEXT released

            // Loop back to check for requests that arrived while we were
            // inside the MODULE_CONTEXT critical section.
        }
    }
}

fn process_request(ctx: &Context, request: SelectorRequest) {
    let original_db = get_current_db(ctx);
    let target_db = request.db();

    if target_db != original_db {
        let _ = set_current_db(ctx, target_db);
    }

    if is_clustered(ctx) {
        // In cluster mode, process_cluster handles sending the response itself.
        process_cluster(ctx, request)
    } else {
        let result = process_local(ctx, request.item);
        send_to_responder(&request.responder, result);
    };

    if target_db != original_db {
        let _ = set_current_db(ctx, original_db);
    }
}

fn process_local(ctx: &Context, command: SelectorCommand) -> QueryResult<QueryValue> {
    match command {
        SelectorCommand::Vector(iqc) => {
            let timestamp = iqc.timestamp;
            let selector: SeriesSelector = SeriesSelector::from(iqc.matchers);
            query_instant_local(ctx, selector, timestamp, iqc.options).map(QueryValue::Vector)
        }
        SelectorCommand::Range(rc) => {
            let start = rc.start_timestamp;
            let end = rc.end_timestamp;
            let selector: SeriesSelector = SeriesSelector::from(rc.matchers);
            query_range_local(ctx, selector, start, end, rc.options).map(QueryValue::Matrix)
        }
    }
}

fn calculate_timeout(opts: &QueryOptions) -> Duration {
    opts.timeout.unwrap_or_else(get_cluster_command_timeout)
    // todo: cap with promql config max query duration
}

fn validate_max_series_(series_count: usize, max_series: usize) -> QueryResult<()> {
    if let Err(msg) = validate_max_series(series_count, max_series) {
        log_warning(&msg);
        return Err(QueryError::Execution(msg));
    }
    Ok(())
}

fn validate_max_points_per_series(
    points_count: usize,
    max_points: Option<usize>,
) -> QueryResult<()> {
    if let Some(max) = max_points
        && max > 0
        && let Err(err) = validate_max_points(points_count, Some(max))
    {
        log_warning(&err);
        return Err(QueryError::Execution(err));
    }
    Ok(())
}

fn send_to_responder(
    responder: &mpsc::SyncSender<QueryResult<QueryValue>>,
    result: QueryResult<QueryValue>,
) {
    if responder.send(result).is_err() {
        log_warning("promql: failed to send query response to requester");
    }
}

fn run_clustered_vector_selector(
    ctx: &Context,
    iqc: InstantVectorSelectorCommand,
    responder: mpsc::SyncSender<QueryResult<QueryValue>>,
) {
    let timeout = calculate_timeout(&iqc.options);
    let timestamp = iqc.timestamp;
    let lookback_delta = iqc.options.lookback_delta.as_millis() as u64;
    let cmd = InstantVectorSelectorFanoutCommand::new(
        iqc.matchers,
        timestamp,
        lookback_delta,
        iqc.options.max_series as u64,
        iqc.options.max_points_per_series.unwrap_or(0) as u64,
        timeout,
    );

    let max_series = iqc.options.max_series;
    let targets = cmd.get_targets(ctx);
    let responder = Arc::new(responder);
    let cloned_responder = responder.clone();

    let handler = move |cmd: InstantVectorSelectorFanoutCommand, result: FanoutCommandResult| {
        let query_result = match result {
            Ok(()) => {
                let resp = cmd.get_response();
                let mut samples: Vec<InstantSample> = Vec::with_capacity(resp.samples.len());

                for s in resp.samples {
                    let labels = proto_labels_to_labels(s.labels);
                    samples.push(InstantSample {
                        labels,
                        timestamp_ms: s.timestamp,
                        value: s.value,
                    });
                }

                validate_max_series_(samples.len(), max_series).map(|_| QueryValue::Vector(samples))
            }
            Err(e) => {
                log_warning(format!(
                    "promql: cluster command failed for instant query: {e}"
                ));

                // Return empty result on error to avoid failing the entire batch.
                Ok(QueryValue::Vector(vec![]))
            }
        };

        send_to_responder(&responder, query_result);
    };

    if let Err(e) = exec_command(ctx, cmd, targets, timeout, handler) {
        send_to_responder(&cloned_responder, Err(e.into()));
    }
}

fn run_clustered_range_selector(
    ctx: &Context,
    rc: RangeSelectorCommand,
    responder: mpsc::SyncSender<QueryResult<QueryValue>>,
) {
    let timeout = calculate_timeout(&rc.options);
    let cmd = RangeVectorSelectorFanoutCommand::new(
        rc.matchers,
        rc.start_timestamp,
        rc.end_timestamp,
        rc.options.max_series as u64,
        rc.options.max_points_per_series.unwrap_or(0) as u64,
        timeout,
    );

    let max_series = rc.options.max_series;
    let max_points_per_series = rc.options.max_points_per_series;
    let targets = cmd.get_targets(ctx);
    let responder = Arc::new(responder);
    let cloned_responder = responder.clone();

    let handler = move |cmd: RangeVectorSelectorFanoutCommand, result: FanoutCommandResult| {
        let query_result = match result {
            Ok(()) => {
                let resp = cmd.get_response();

                validate_max_series_(resp.series.len(), max_series).and_then(|_| {
                    let mut ranges: Vec<RangeSample> = Vec::with_capacity(resp.series.len());

                    for rs in resp.series {
                        validate_max_points_per_series(rs.samples.len(), max_points_per_series)?;

                        let samples: Vec<Sample> = rs
                            .samples
                            .into_iter()
                            .map(|s| Sample::new(s.timestamp, s.value))
                            .collect();

                        let labels = proto_labels_to_labels(rs.labels);
                        ranges.push(RangeSample { labels, samples });
                    }

                    Ok(QueryValue::Matrix(ranges))
                })
            }
            Err(e) => {
                log_warning(format!(
                    "promql: cluster command failed for range query: {e}"
                ));

                // Return empty result on error to avoid failing the entire batch.
                Ok(QueryValue::Matrix(vec![]))
            }
        };

        send_to_responder(&responder, query_result);
    };

    if let Err(e) = exec_command(ctx, cmd, targets, timeout, handler) {
        send_to_responder(&cloned_responder, Err(e.into()));
    }
}

fn process_cluster(ctx: &Context, batch_request: SelectorRequest) {
    match batch_request.item {
        SelectorCommand::Vector(iqc) => {
            run_clustered_vector_selector(ctx, iqc, batch_request.responder);
        }
        SelectorCommand::Range(rc) => {
            run_clustered_range_selector(ctx, rc, batch_request.responder);
        }
    }
}

pub(in crate::promql) fn query_instant_local(
    ctx: &Context,
    selector: SeriesSelector,
    timestamp: Timestamp,
    options: QueryOptions,
) -> QueryResult<Vec<InstantSample>> {
    if let Some(d) = options.deadline
        && current_time_millis() > d
    {
        return Err(QueryError::Timeout);
    }
    let series = series_by_selectors(ctx, &[selector], None)
        .map_err(|e| QueryError::Execution(e.to_string()))?;

    validate_max_series_(series.len(), options.max_series)?;
    // for instant queries, series length == number of returned samples, so should probably
    // coalesce this with the max points per series validation.
    validate_max_points_per_series(series.len(), options.max_points_per_series)?;

    // PromQL instant-query semantics: return the most recent sample per series
    // whose timestamp falls within the lookback window (timestamp - lookback_delta, timestamp].
    // This mirrors the Prometheus staleness semantics described in:
    // https://prometheus.io/docs/prometheus/latest/querying/basics/#staleness
    let lookback_delta_ms = options.lookback_delta.as_millis() as Timestamp;
    // The lower bound is exclusive per PromQL spec, so subtract 1 to make the
    // TimeSeries::get_range inclusive-lower-bound call behave correctly.
    let lookback_start_ms = instant_lookback_start_ms(timestamp, lookback_delta_ms);

    let samples = series
        .iter()
        .map(|(s, _)| s.deref())
        .iter_into_par()
        .filter_map(|s| {
            // Fetch all samples within [lookback_start_ms, timestamp] and pick the last one.
            let range = s.get_range(lookback_start_ms, timestamp);

            let sample = range.last()?;

            let labels: Labels = (&s.labels).into();
            Some(InstantSample {
                timestamp_ms: sample.timestamp,
                value: sample.value,
                labels,
            })
        })
        .collect::<Vec<_>>();

    Ok(samples)
}

pub(in crate::promql) fn query_range_local(
    ctx: &Context,
    selector: SeriesSelector,
    start_time: i64,
    end_time: i64,
    options: QueryOptions,
) -> QueryResult<Vec<RangeSample>> {
    let series = series_by_selectors(ctx, &[selector], None)
        .map_err(|e| QueryError::Execution(e.to_string()))?;

    validate_max_series_(series.len(), options.max_series)?;

    let ranges = series
        .iter()
        .map(|(s, _)| s.deref())
        .iter_into_par()
        .filter_map(|s| {
            let samples = s.get_range(start_time, end_time);
            if samples.is_empty() {
                return None;
            }

            let labels: Labels = (&s.labels).into();

            let range = RangeSample { samples, labels };
            Some(range)
        })
        .collect::<Vec<_>>();

    for range in &ranges {
        validate_max_points_per_series(range.samples.len(), options.max_points_per_series)?;
    }

    Ok(ranges)
}
