use crate::common::context::{get_current_db, set_current_db};
use crate::common::logging::log_warning;
use crate::common::threads::IterIntoParRayon;
use crate::common::time::current_time_millis;
use crate::common::{Sample, Timestamp};
use crate::fanout::{FanoutCommandResult, FanoutError, exec_command, get_cluster_command_timeout};
use crate::fanout::{compute_hash_tag_fanout_target, is_clustered, with_fanout_user};
use crate::labels::filters::SeriesSelector;
use crate::promql::EvalLabels;
use crate::promql::engine::query_reader::{
    AggregationOutcome, AggregationRequest, RollupOutcome, RollupRequest,
};
use crate::promql::engine::{
    AggregationFanoutCommand, InstantVectorParams, InstantVectorSelectorFanoutCommand,
    RangeVectorSelectorFanoutCommand, RollupFanoutCommand, get_series_range,
    instant_lookback_start_ms, proto_labels_to_eval_labels, validate_max_points,
    validate_max_series,
};
use crate::promql::{InstantSample, QueryError, QueryOptions, QueryResult, RangeSample};
use crate::series::index::series_by_selectors;
use orx_parallel::ParIter;
use orx_parallel::ParIterResult;
use promql_parser::label::Matchers;
use std::ops::Deref;
use std::sync::{Arc, mpsc};
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

/// An instant vector to select *and* the aggregation to apply to it, so that in
/// cluster mode both can be pushed to the shards that hold the data.
struct AggregationSelectorCommand {
    matchers: Matchers,
    timestamp: Timestamp,
    aggregation: AggregationRequest,
    options: QueryOptions,
}

/// The series to read *and* the rollup to reduce their windows with, so that in
/// cluster mode both can be pushed to the shards that hold the data.
struct RollupSelectorCommand {
    matchers: Matchers,
    rollup: RollupRequest,
    options: QueryOptions,
}

enum SelectorTaskKind {
    Vector(InstantVectorSelectorCommand),
    Range(RangeSelectorCommand),
    Aggregation(AggregationSelectorCommand),
    Rollup(RollupSelectorCommand),
}

impl SelectorTaskKind {
    fn db(&self) -> i32 {
        match self {
            SelectorTaskKind::Vector(iqc) => iqc.options.db,
            SelectorTaskKind::Range(rc) => rc.options.db,
            SelectorTaskKind::Aggregation(ac) => ac.options.db,
            SelectorTaskKind::Rollup(rc) => rc.options.db,
        }
    }
}

/// What a selector task produced. One channel carries every task kind, so the
/// aggregation task's richer answer (did the source aggregate, or must the
/// caller?) needs its own variant rather than a bare [`QueryValue`].
enum SelectorOutput {
    /// Instant-vector selector result, labels still by refcount from storage.
    Vector(Vec<InstantSample<EvalLabels>>),
    /// Range-vector selector result, labels still by refcount from storage.
    Matrix(Vec<RangeSample<EvalLabels>>),
    Aggregation(AggregationOutcome),
    Rollup(RollupOutcome),
}

impl SelectorOutput {
    /// Unwrap an instant-vector selector result. The variant is chosen by the
    /// task kind, so a mismatch is a bug in this module rather than a query error.
    fn into_vector(self) -> QueryResult<Vec<InstantSample<EvalLabels>>> {
        match self {
            SelectorOutput::Vector(samples) => Ok(samples),
            _ => Err(QueryError::Execution(
                "BUG: selector task returned a non-vector outcome".to_string(),
            )),
        }
    }

    /// Unwrap a range-vector selector result; see [`Self::into_vector`].
    fn into_matrix(self) -> QueryResult<Vec<RangeSample<EvalLabels>>> {
        match self {
            SelectorOutput::Matrix(series) => Ok(series),
            _ => Err(QueryError::Execution(
                "BUG: selector task returned a non-matrix outcome".to_string(),
            )),
        }
    }

    fn into_aggregation(self) -> QueryResult<AggregationOutcome> {
        match self {
            SelectorOutput::Aggregation(outcome) => Ok(outcome),
            _ => Err(QueryError::Execution(
                "BUG: aggregation task returned a non-aggregation result".to_string(),
            )),
        }
    }

    fn into_rollup(self) -> QueryResult<RollupOutcome> {
        match self {
            SelectorOutput::Rollup(outcome) => Ok(outcome),
            _ => Err(QueryError::Execution(
                "BUG: rollup task returned a non-rollup result".to_string(),
            )),
        }
    }
}

/// A single batched request for a `SelectorBatchExecutor`.
struct SelectorTask {
    kind: SelectorTaskKind,
    /// The client identity captured before PromQL evaluation moved onto a
    /// background thread. It must accompany every selector read, including
    /// reads that start a cluster fanout.
    caller_user: Option<String>,
    /// The command's `HASHTAG` routing scope, empty when none was requested.
    /// A field on the task rather than on each kind: every selector in one
    /// expression shares the same scope regardless of which task kind carries
    /// it, and the local execution path ignores it entirely.
    hash_tags: Arc<[String]>,
    /// responder receives the processed result (Ok) or the error (Err)
    responder: mpsc::SyncSender<QueryResult<SelectorOutput>>,
}

impl SelectorTask {
    fn db(&self) -> i32 {
        self.kind.db()
    }
}

/// An executor responsible for executing PromQL selectors as part of a keyspace batch operation.
///
/// The `SelectorBatchExecutor` optimizes latency in the PromQL evaluator (especially in cluster mode) by:
///
/// - Serializing access to the Valkey keyspace via `MODULE_CONTEXT` to avoid deadlocks, ensuring that
///   we can query safely from multiple threads.
/// - Collecting incoming selector tasks into a batch and processing the batch in a single lock
///   acquisition to reduce locking overhead.
///
/// # Design
///
/// One dedicated thread owns the processor role for the life of the module. Submitters only
/// enqueue a task and wait on its responder; they never take the module lock or touch the
/// keyspace themselves.
///
/// The evaluator calls in from inside rayon jobs (`preload_grid` fans selectors out on the
/// pool; rollup reads happen from step chunks), and the processor's own reads fan out on the
/// same pool (`TimeSeries::get_range` splits across chunks). Two invariants keep that from
/// deadlocking:
///
/// 1. The processor is never a pool worker. The earlier cooperative design let the first
///    submitter drain the queue; a worker in that role could be handed another submitter's
///    closure by work-stealing while it waited on its own fan-out, and that closure would
///    then wait on the processor's own thread forever.
/// 2. A submitter that *is* a pool worker never parks. [`wait_for_result`] keeps it executing
///    pool jobs until its answer arrives, so the processor's injected work always finds a
///    thread even when every worker is waiting on a selector. Parking them instead was
///    observed to freeze the server: eight workers in `recv`, the processor holding the module
///    lock waiting for a chunk fan-out nobody could run, and the main thread waiting on the lock.
///
/// For local queries, the thread processes the task directly, so processing is serialized.
/// For cluster queries, a synchronous call is made per query and the context is released. The processing itself
/// is executed in parallel across all target cluster nodes, and results are returned asynchronously without
/// holding the GIL.
///
/// # Note
/// The `SelectorBatchExecutor` is designed for internal use within the PromQL engine and is not intended to be
/// used directly by external callers. It is exposed as a handle that can be used to perform queries,
/// but the internal implementation details are abstracted away.
///
/// # Example
/// ```ignore
/// use crate::promql::engine::SelectorBatchExecutor;
/// use crate::promql::engine::QueryOptions;
/// use promql_parser::label::Matchers;
///
///
/// // Create an executor handle and perform queries via the provided API.
/// let executor = SelectorBatchExecutor::new();
/// let now = current_time_millis();
/// let options = QueryOptions {
///     timeout: Some(now + 60_000), // 1 minute from now
///     lookback_delta: None,
///     max_series: 1000,
/// };
/// let matchers = vec![Matcher::new("job", "=", "prometheus")];
/// let _ = executor.query(matchers, now, options);
/// ```
pub struct SelectorBatchExecutor {
    /// Hand-off to the processor thread. Unbounded: a submitter blocks on its
    /// responder anyway, so back-pressure here would only add a second wait.
    sender: mpsc::Sender<SelectorTask>,
}

/// The processor loop: wait for one task, sweep up whatever else is already
/// queued (bounded by [`MAX_BATCH_SIZE`]), then run the batch under a single
/// module-lock acquisition. Exits when the executor handle is dropped.
fn run_processor(receiver: mpsc::Receiver<SelectorTask>) {
    while let Ok(first) = receiver.recv() {
        let batch = collect_batch(&receiver, first, MAX_BATCH_SIZE);

        let ctx = MODULE_CONTEXT.lock();
        for task in batch {
            execute_selector_task(&ctx, task);
        }
        // ctx dropped here — MODULE_CONTEXT released
    }
}

/// `first` plus up to `max_batch_size - 1` tasks that are already waiting.
/// Never blocks: a task that arrives after the sweep starts the next batch.
fn collect_batch<T>(receiver: &mpsc::Receiver<T>, first: T, max_batch_size: usize) -> Vec<T> {
    let mut batch = Vec::with_capacity(max_batch_size);
    batch.push(first);
    while batch.len() < max_batch_size {
        match receiver.try_recv() {
            Ok(task) => batch.push(task),
            Err(_) => break,
        }
    }
    batch
}

impl SelectorBatchExecutor {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel();
        std::thread::Builder::new()
            .name("ts-promql-selector".to_string())
            .spawn(move || run_processor(receiver))
            .expect("failed to spawn the PromQL selector executor thread");
        Self { sender }
    }

    pub fn query(
        &self,
        matchers: Matchers,
        timestamp: Timestamp,
        options: QueryOptions,
        caller_user: Option<String>,
        hash_tags: Arc<[String]>,
    ) -> QueryResult<Vec<InstantSample<EvalLabels>>> {
        let command = SelectorTaskKind::Vector(InstantVectorSelectorCommand {
            matchers,
            timestamp,
            options,
        });
        self.submit_selector_task(command, caller_user, hash_tags)?
            .into_vector()
    }

    pub fn query_range(
        &self,
        matchers: Matchers,
        start: Timestamp,
        end: Timestamp,
        options: QueryOptions,
        caller_user: Option<String>,
        hash_tags: Arc<[String]>,
    ) -> QueryResult<Vec<RangeSample<EvalLabels>>> {
        let command = SelectorTaskKind::Range(RangeSelectorCommand {
            matchers,
            start_timestamp: start,
            end_timestamp: end,
            options,
        });
        self.submit_selector_task(command, caller_user, hash_tags)?
            .into_matrix()
    }

    /// Evaluate `aggregation` over the instant vector `matchers` selects at
    /// `timestamp`.
    ///
    /// In cluster mode the whole aggregation is pushed to the shards and only
    /// the reduced result comes back. On a single node there is nothing to push
    /// down to, so the raw instant vector is returned for the caller to
    /// aggregate — doing it here would hold the module lock for the length of
    /// the aggregation.
    pub fn query_aggregation(
        &self,
        matchers: Matchers,
        timestamp: Timestamp,
        aggregation: AggregationRequest,
        options: QueryOptions,
        caller_user: Option<String>,
        hash_tags: Arc<[String]>,
    ) -> QueryResult<AggregationOutcome> {
        let command = SelectorTaskKind::Aggregation(AggregationSelectorCommand {
            matchers,
            timestamp,
            aggregation,
            options,
        });
        self.submit_selector_task(command, caller_user, hash_tags)?
            .into_aggregation()
    }

    /// Reduce the windows `matchers` selects with `rollup`.
    ///
    /// In cluster mode the whole rollup is pushed to the shards and only one
    /// value per series per step comes back. On a single node there is nothing
    /// to push down to, so the raw windows are returned for the caller to reduce
    /// — doing it here would hold the module lock for the length of the
    /// reduction.
    pub fn query_rollup(
        &self,
        matchers: Matchers,
        rollup: RollupRequest,
        options: QueryOptions,
        caller_user: Option<String>,
        hash_tags: Arc<[String]>,
    ) -> QueryResult<RollupOutcome> {
        let command = SelectorTaskKind::Rollup(RollupSelectorCommand {
            matchers,
            rollup,
            options,
        });
        self.submit_selector_task(command, caller_user, hash_tags)?
            .into_rollup()
    }

    fn submit_selector_task(
        &self,
        command: SelectorTaskKind,
        caller_user: Option<String>,
        hash_tags: Arc<[String]>,
    ) -> QueryResult<SelectorOutput> {
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let task = SelectorTask {
            kind: command,
            caller_user,
            hash_tags,
            responder: result_tx,
        };

        if self.sender.send(task).is_err() {
            return Err(QueryError::Execution(
                "selector executor thread is not running".to_string(),
            ));
        }

        match wait_for_result(&result_rx) {
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
}

/// How long a waiting pool worker sleeps when the pool has nothing for it to run. It only
/// bounds how long an injected job can sit unclaimed while *every* worker is waiting here;
/// a delivered result wakes the sleeper immediately.
const WORKER_WAIT_BACKOFF: Duration = Duration::from_micros(250);

/// Wait for a selector result without starving the rayon pool.
///
/// Off the pool this is a plain blocking `recv`. On a pool worker it alternates between
/// checking the responder and running one pending pool job, so the processor's fan-outs
/// (and other queries' work) keep making progress on this thread while it waits. A stolen
/// job may itself submit a selector and wait here again; that nests safely because the
/// processor is a dedicated thread that answers every task in order.
fn wait_for_result<T>(rx: &mpsc::Receiver<T>) -> Result<T, mpsc::RecvError> {
    if rayon_core::current_thread_index().is_none() {
        return rx.recv();
    }
    loop {
        match rx.try_recv() {
            Ok(value) => return Ok(value),
            Err(mpsc::TryRecvError::Disconnected) => return Err(mpsc::RecvError),
            Err(mpsc::TryRecvError::Empty) => {}
        }
        if matches!(rayon_core::yield_now(), Some(rayon_core::Yield::Executed)) {
            continue;
        }
        match rx.recv_timeout(WORKER_WAIT_BACKOFF) {
            Ok(value) => return Ok(value),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => return Err(mpsc::RecvError),
        }
    }
}

fn execute_selector_task(ctx: &Context, task: SelectorTask) {
    let SelectorTask {
        kind,
        caller_user,
        hash_tags,
        responder,
    } = task;
    let original_db = get_current_db(ctx);
    let target_db = kind.db();

    if target_db != original_db {
        let _ = set_current_db(ctx, target_db);
    }

    let error_responder = responder.clone();
    let result = with_fanout_user(ctx, caller_user.as_deref(), |ctx| {
        if is_clustered(ctx) {
            // The fanout command captures the authenticated user while this
            // scope is active and propagates it to every shard.
            execute_selector_task_cluster(
                ctx,
                SelectorTask {
                    kind,
                    caller_user: None,
                    hash_tags,
                    responder,
                },
            );
        } else {
            // The local index is the whole picture on a single node, so the
            // routing scope is deliberately ignored here: `HASHTAG` selects
            // shards, it does not filter keys or labels.
            let result = execute_selector_task_local(ctx, kind);
            deliver_task_result(&responder, result);
        }
        Ok(())
    });

    if let Err(err) = result {
        deliver_task_result(
            &error_responder,
            Err(QueryError::Execution(err.to_string())),
        );
    }

    if target_db != original_db {
        let _ = set_current_db(ctx, original_db);
    }
}

fn execute_selector_task_local(
    ctx: &Context,
    command: SelectorTaskKind,
) -> QueryResult<SelectorOutput> {
    match command {
        SelectorTaskKind::Vector(iqc) => {
            let timestamp = iqc.timestamp;
            let selector: SeriesSelector = SeriesSelector::from(iqc.matchers);
            query_instant_local(ctx, selector, timestamp, iqc.options).map(SelectorOutput::Vector)
        }
        SelectorTaskKind::Range(rc) => {
            let start = rc.start_timestamp;
            let end = rc.end_timestamp;
            let selector: SeriesSelector = SeriesSelector::from(rc.matchers);
            query_range_local(ctx, selector, start, end, rc.options).map(SelectorOutput::Matrix)
        }
        SelectorTaskKind::Aggregation(ac) => {
            // Single node: there is no shard to push the operator to, so hand
            // the raw vector back and let the caller aggregate it outside the
            // module lock.
            let timestamp = ac.timestamp;
            let selector: SeriesSelector = SeriesSelector::from(ac.matchers);
            query_instant_local(ctx, selector, timestamp, ac.options)
                .map(|samples| SelectorOutput::Aggregation(AggregationOutcome::Raw(samples)))
        }
        SelectorTaskKind::Rollup(rc) => {
            // Single node: same reasoning as the aggregation task — read the
            // windows and let the caller reduce them outside the module lock.
            let Some((start, end)) = rc.rollup.fetch_bounds() else {
                return Ok(SelectorOutput::Rollup(RollupOutcome::Raw(Vec::new())));
            };
            let selector: SeriesSelector = SeriesSelector::from(rc.matchers);
            query_range_local(ctx, selector, start, end, rc.options)
                .map(|series| SelectorOutput::Rollup(RollupOutcome::Raw(series)))
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

fn deliver_task_result(
    responder: &mpsc::SyncSender<QueryResult<SelectorOutput>>,
    result: QueryResult<SelectorOutput>,
) {
    if responder.send(result).is_err() {
        log_warning("promql: failed to send query response to requester");
    }
}

/// Convert a cluster selector failure into the query result delivered to the
/// evaluator. An empty selector result is a valid PromQL answer, so it must
/// never be used to hide a failed shard request.
fn selector_fanout_failure(query_kind: &str, error: FanoutError) -> QueryError {
    log_warning(format!(
        "promql: cluster command failed for {query_kind} query: {error}"
    ));
    error.into()
}

fn execute_cluster_vector_selector(
    ctx: &Context,
    iqc: InstantVectorSelectorCommand,
    hash_tags: &[String],
    responder: mpsc::SyncSender<QueryResult<SelectorOutput>>,
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
    let targets = compute_hash_tag_fanout_target(ctx, hash_tags);
    let responder = Arc::new(responder);
    let cloned_responder = responder.clone();

    let handler = move |cmd: InstantVectorSelectorFanoutCommand, result: FanoutCommandResult| {
        let query_result = match result {
            Ok(()) => {
                let resp = cmd.get_response();
                let mut samples: Vec<InstantSample<EvalLabels>> =
                    Vec::with_capacity(resp.samples.len());

                for s in resp.samples {
                    let labels = proto_labels_to_eval_labels(s.labels);
                    samples.push(InstantSample {
                        labels,
                        timestamp_ms: s.timestamp,
                        value: s.value,
                    });
                }

                validate_max_series_(samples.len(), max_series)
                    .map(|_| SelectorOutput::Vector(samples))
            }
            Err(e) => Err(selector_fanout_failure("instant", e)),
        };

        deliver_task_result(&responder, query_result);
    };

    if let Err(e) = exec_command(ctx, cmd, targets, timeout, handler) {
        deliver_task_result(&cloned_responder, Err(e.into()));
    }
}

fn execute_cluster_range_selector(
    ctx: &Context,
    rc: RangeSelectorCommand,
    hash_tags: &[String],
    responder: mpsc::SyncSender<QueryResult<SelectorOutput>>,
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
    let targets = compute_hash_tag_fanout_target(ctx, hash_tags);
    let responder = Arc::new(responder);
    let cloned_responder = responder.clone();

    let handler = move |cmd: RangeVectorSelectorFanoutCommand, result: FanoutCommandResult| {
        let query_result = match result {
            Ok(()) => {
                let resp = cmd.get_response();

                validate_max_series_(resp.series.len(), max_series).and_then(|_| {
                    let mut ranges: Vec<RangeSample<EvalLabels>> =
                        Vec::with_capacity(resp.series.len());

                    for rs in resp.series {
                        validate_max_points_per_series(rs.samples.len(), max_points_per_series)?;

                        let samples: Vec<Sample> = rs
                            .samples
                            .into_iter()
                            .map(|s| Sample::new(s.timestamp, s.value))
                            .collect();

                        let labels = proto_labels_to_eval_labels(rs.labels);
                        ranges.push(RangeSample { labels, samples });
                    }

                    Ok(SelectorOutput::Matrix(ranges))
                })
            }
            Err(e) => Err(selector_fanout_failure("range", e)),
        };

        deliver_task_result(&responder, query_result);
    };

    if let Err(e) = exec_command(ctx, cmd, targets, timeout, handler) {
        deliver_task_result(&cloned_responder, Err(e.into()));
    }
}

/// Push an aggregation to the shards and reduce their answers here.
///
/// The push-down is transparent to the caller: a cluster that cannot evaluate it
/// (a peer without support) reports [`AggregationOutcome::Unsupported`] and the
/// caller falls back to selecting the raw vector, so the query still answers.
fn execute_cluster_aggregation(
    ctx: &Context,
    ac: AggregationSelectorCommand,
    hash_tags: &[String],
    responder: mpsc::SyncSender<QueryResult<SelectorOutput>>,
) {
    let timeout = calculate_timeout(&ac.options);
    let vector = InstantVectorParams {
        matchers: ac.matchers,
        timestamp: ac.timestamp,
        lookback_delta: ac.options.lookback_delta.as_millis() as u64,
        max_series: ac.options.max_series as u64,
        max_points_per_series: ac.options.max_points_per_series.unwrap_or(0) as u64,
    };
    let cmd = AggregationFanoutCommand::new(vector, ac.aggregation, timeout);

    let max_series = ac.options.max_series;
    let targets = compute_hash_tag_fanout_target(ctx, hash_tags);
    let responder = Arc::new(responder);
    let cloned_responder = responder.clone();

    let handler = move |cmd: AggregationFanoutCommand, result: FanoutCommandResult| {
        let query_result = match result {
            Ok(()) => cmd
                .into_result()
                .map_err(QueryError::from)
                .and_then(|samples| {
                    // The aggregated vector, not the input, is what this bounds:
                    // one sample per group.
                    validate_max_series_(samples.len(), max_series)?;
                    let samples = samples
                        .into_iter()
                        .map(|s| InstantSample {
                            labels: s.labels,
                            timestamp_ms: s.timestamp_ms,
                            value: s.value,
                        })
                        .collect();
                    Ok(SelectorOutput::Aggregation(AggregationOutcome::Aggregated(
                        samples,
                    )))
                }),
            Err(e) if cmd.peer_unsupported() => {
                log_warning(format!(
                    "promql: aggregation push-down unsupported by a peer, falling back: {e}"
                ));
                Ok(SelectorOutput::Aggregation(AggregationOutcome::Unsupported))
            }
            Err(e) => {
                log_warning(format!(
                    "promql: cluster command failed for aggregation query: {e}"
                ));
                Err(e.into())
            }
        };

        deliver_task_result(&responder, query_result);
    };

    if let Err(e) = exec_command(ctx, cmd, targets, timeout, handler) {
        deliver_task_result(&cloned_responder, Err(e.into()));
    }
}

/// Push a rollup to the shards and collect their per-series values here.
///
/// A series lives on exactly one shard, so the shards' outputs are disjoint and
/// the coordinator concatenates rather than merges. As with aggregation, a
/// cluster that cannot evaluate the rollup (a peer without support) reports
/// [`RollupOutcome::Unsupported`] and the caller falls back to selecting the raw
/// matrix, so the query still answers.
fn execute_cluster_rollup(
    ctx: &Context,
    rc: RollupSelectorCommand,
    hash_tags: &[String],
    responder: mpsc::SyncSender<QueryResult<SelectorOutput>>,
) {
    let timeout = calculate_timeout(&rc.options);
    let max_series = rc.options.max_series;
    let max_points_per_series = rc.options.max_points_per_series;
    let cmd = RollupFanoutCommand::new(
        rc.matchers,
        rc.rollup,
        max_series as u64,
        max_points_per_series.unwrap_or(0) as u64,
        timeout,
    );

    let targets = compute_hash_tag_fanout_target(ctx, hash_tags);
    let responder = Arc::new(responder);
    let cloned_responder = responder.clone();

    let handler = move |cmd: RollupFanoutCommand, result: FanoutCommandResult| {
        let query_result = match result {
            Ok(()) => {
                let series = cmd.into_result();
                // The rolled-up output, not the input, is what these bound: one
                // series per input series, one point per step that produced one.
                validate_max_series_(series.len(), max_series).and_then(|_| {
                    for s in &series {
                        validate_max_points_per_series(s.samples.len(), max_points_per_series)?;
                    }
                    Ok(SelectorOutput::Rollup(RollupOutcome::Rolled(series)))
                })
            }
            Err(e) if cmd.peer_unsupported() => {
                log_warning(format!(
                    "promql: rollup push-down unsupported by a peer, falling back: {e}"
                ));
                Ok(SelectorOutput::Rollup(RollupOutcome::Unsupported))
            }
            Err(e) => {
                log_warning(format!(
                    "promql: cluster command failed for rollup query: {e}"
                ));
                Err(e.into())
            }
        };

        deliver_task_result(&responder, query_result);
    };

    if let Err(e) = exec_command(ctx, cmd, targets, timeout, handler) {
        deliver_task_result(&cloned_responder, Err(e.into()));
    }
}

fn execute_selector_task_cluster(ctx: &Context, task: SelectorTask) {
    // Every path below routes through the same scope, including the push-down
    // ones: a push-down that a peer cannot serve falls back to the ordinary
    // selector read on the same reader, which carries these same tags.
    let hash_tags = task.hash_tags;
    match task.kind {
        SelectorTaskKind::Vector(iqc) => {
            execute_cluster_vector_selector(ctx, iqc, &hash_tags, task.responder);
        }
        SelectorTaskKind::Range(rc) => {
            execute_cluster_range_selector(ctx, rc, &hash_tags, task.responder);
        }
        SelectorTaskKind::Aggregation(ac) => {
            execute_cluster_aggregation(ctx, ac, &hash_tags, task.responder);
        }
        SelectorTaskKind::Rollup(rc) => {
            execute_cluster_rollup(ctx, rc, &hash_tags, task.responder);
        }
    }
}

pub(in crate::promql) fn query_instant_local(
    ctx: &Context,
    selector: SeriesSelector,
    timestamp: Timestamp,
    options: QueryOptions,
) -> QueryResult<Vec<InstantSample<EvalLabels>>> {
    if let Some(d) = options.deadline
        && current_time_millis() > d
    {
        return Err(QueryError::Timeout);
    }
    let series = series_by_selectors(ctx, &[selector], None)
        .map_err(|e| QueryError::Execution(e.to_string()))?;

    // PromQL instant-query semantics: return the most recent sample per series
    // whose timestamp falls within the lookback window (timestamp - lookback_delta, timestamp].
    // This mirrors the Prometheus staleness semantics described in:
    // https://prometheus.io/docs/prometheus/latest/querying/basics/#staleness
    let lookback_delta_ms = options.lookback_delta.as_millis() as Timestamp;
    // The lower bound is exclusive per PromQL spec, so subtract 1 to make the
    // TimeSeries::get_range inclusive-lower-bound call behave correctly.
    let lookback_start_ms = instant_lookback_start_ms(timestamp, lookback_delta_ms);

    // Fans out on the rayon pool from the processor thread. Safe only because a
    // worker waiting on this task keeps running pool jobs (see `wait_for_result`);
    // the chunk-level fan-out inside `get_range` relies on the same guarantee.
    let samples = series
        .iter()
        .map(|(s, _)| s.deref())
        .iter_into_par_rayon()
        .filter_map(|s| {
            let sample = s.last_sample_in_range(lookback_start_ms, timestamp)?;

            let labels = EvalLabels::interned(&s.labels);
            Some(InstantSample {
                timestamp_ms: sample.timestamp,
                value: sample.value,
                labels,
            })
        })
        .collect::<Vec<_>>();

    // Bound what the query returns, not what the selector matched: a series
    // whose latest sample predates the lookback window contributes nothing, so
    // it must not count against the limit. The cluster paths filter first for
    // the same reason, and a query's fate should not turn on which one ran it.
    validate_max_series_(samples.len(), options.max_series)?;
    // No max-points-per-series validation here: an instant query yields at most one
    // sample per series, so the per-series point limit can never be exceeded.

    Ok(samples)
}

pub(in crate::promql) fn query_range_local(
    ctx: &Context,
    selector: SeriesSelector,
    start_time: i64,
    end_time: i64,
    options: QueryOptions,
) -> QueryResult<Vec<RangeSample<EvalLabels>>> {
    let series = series_by_selectors(ctx, &[selector], None)
        .map_err(|e| QueryError::Execution(e.to_string()))?;

    // On the pool for the same reason as in `query_instant_local`.
    let ranges = series
        .iter()
        .map(|(s, _)| s.deref())
        .iter_into_par_rayon()
        .filter_map(|s| {
            let samples =
                match get_series_range(s, start_time, end_time, options.max_points_per_series) {
                    Ok(samples) => samples,
                    Err(err) => {
                        log_warning(&err);
                        return Some(Err(QueryError::Execution(err)));
                    }
                };
            if samples.is_empty() {
                return None;
            }

            let labels = EvalLabels::interned(&s.labels);

            let range = RangeSample { samples, labels };
            Some(Ok(range))
        })
        .into_fallible_result()
        .collect::<Vec<_>>()?;

    // Bound the series the query returns, not the ones the selector matched:
    // an empty range contributes nothing. This is also the path a single-node
    // rollup takes, so it must agree with the pushed-down one about which
    // queries `max_series` rejects.
    validate_max_series_(ranges.len(), options.max_series)?;

    Ok(ranges)
}

#[cfg(test)]
mod selector_batch_executor_tests {
    use super::{collect_batch, selector_fanout_failure, wait_for_result};
    use crate::fanout::FanoutError;
    use crate::promql::QueryError;
    use std::sync::mpsc;
    use std::time::Duration;

    #[test]
    fn collect_batch_sweeps_only_what_is_already_queued() {
        let (tx, rx) = mpsc::channel();
        for i in 2..=3 {
            tx.send(i).unwrap();
        }
        // Nothing else arrives, so the sweep must return without waiting.
        assert_eq!(collect_batch(&rx, 1, 8), vec![1, 2, 3]);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn collect_batch_is_bounded() {
        let (tx, rx) = mpsc::channel();
        for i in 2..=10 {
            tx.send(i).unwrap();
        }
        assert_eq!(collect_batch(&rx, 1, 4), vec![1, 2, 3, 4]);
        // The remainder stays queued for the next batch, in order.
        assert_eq!(collect_batch(&rx, rx.recv().unwrap(), 4), vec![5, 6, 7, 8]);
        assert_eq!(rx.recv().unwrap(), 9);
    }

    /// One worker, one job that produces the answer, and the same worker waiting for it.
    /// A parking wait deadlocks here (the job is in the waiter's own queue); the yielding
    /// wait runs the job and returns. Mirrors the processor's chunk fan-out landing on a
    /// pool whose every worker is waiting on the processor.
    #[test]
    fn worker_waiting_for_a_result_keeps_running_pool_jobs() {
        let pool = rayon_core::ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .unwrap();
        let (done_tx, done_rx) = mpsc::channel();
        std::thread::spawn(move || {
            let answer = pool.install(|| {
                let (tx, rx) = mpsc::sync_channel(1);
                rayon_core::spawn(move || tx.send(42).unwrap());
                wait_for_result(&rx)
            });
            done_tx.send(answer).unwrap();
        });
        let answer = done_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("the waiting worker never ran the job that answers it");
        assert_eq!(answer, Ok(42));
    }

    #[test]
    fn wait_for_result_off_the_pool_is_a_plain_recv() {
        let (tx, rx) = mpsc::sync_channel(1);
        tx.send("x").unwrap();
        assert_eq!(wait_for_result(&rx), Ok("x"));
        drop(tx);
        assert!(wait_for_result(&rx).is_err());
    }

    #[test]
    fn collect_batch_tolerates_a_dropped_sender() {
        let (tx, rx) = mpsc::channel::<u32>();
        drop(tx);
        assert_eq!(collect_batch(&rx, 7, 4), vec![7]);
    }

    #[test]
    fn selector_fanout_failure_preserves_timeout() {
        assert!(matches!(
            selector_fanout_failure("instant", FanoutError::timeout()),
            QueryError::Timeout
        ));
    }

    #[test]
    fn selector_fanout_failure_is_not_an_empty_result() {
        assert!(matches!(
            selector_fanout_failure("range", FanoutError::custom("shard unavailable")),
            QueryError::Execution(message) if message.contains("shard unavailable")
        ));
    }
}
