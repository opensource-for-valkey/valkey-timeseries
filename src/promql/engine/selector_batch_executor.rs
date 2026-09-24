use crate::common::Timestamp;
use crate::common::context::{get_current_db, set_current_db};
use crate::common::logging::log_warning;
use crate::common::threads::{ParWithPool, RayonPool, panic_message};
use crate::common::time::current_time_millis;
use crate::fanout::{FanoutCommandResult, FanoutError, exec_command, get_cluster_command_timeout};
use crate::fanout::{compute_hash_tag_fanout_target, is_clustered, with_fanout_user};
use crate::labels::filters::SeriesSelector;
use crate::promql::EvalLabels;
use crate::promql::engine::label_profile::{LabelProfile, profiled_series_cap};
use crate::promql::engine::query_reader::{
    AggregationOutcome, AggregationRequest, GridOutcome, GridRequest,
};
use crate::promql::engine::sample_budget::{SampleBudget, too_many_samples, validate_max_samples};
use crate::promql::engine::{
    AggregationFanoutCommand, GridFanoutCommand, InstantVectorParams,
    InstantVectorSelectorFanoutCommand, LabelProfileFanoutCommand,
    RangeVectorSelectorFanoutCommand, WireRangeSeries, get_snapshot_range,
    instant_lookback_start_ms, local_label_profile, validate_max_points, validate_max_series,
};
use crate::promql::{InstantSample, QueryError, QueryOptions, QueryResult, RangeSample};
use crate::series::chunks::ChunkOps;
use crate::series::index::series_by_selectors;
use crate::series::{RangeSnapshot, TimeSeries};
use orx_parallel::IntoParIter;
use orx_parallel::Par;
use orx_parallel::ParResult;
use promql_parser::label::Matchers;
use std::ops::Deref;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, LazyLock, mpsc};
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

/// The series to read *and* the step grid to evaluate them over — stepped
/// selection, a rollup, or either fused with an aggregation — so that in
/// cluster mode both can be pushed to the shards that hold the data.
struct GridSelectorCommand {
    matchers: Matchers,
    request: GridRequest,
    options: QueryOptions,
}

/// The labels of the series a selector matches, from the index alone: what
/// the derived filter push-down narrows a range query's operands with.
struct ProfileSelectorCommand {
    matchers: Matchers,
    options: QueryOptions,
}

enum SelectorTaskKind {
    Vector(InstantVectorSelectorCommand),
    Range(RangeSelectorCommand),
    Aggregation(AggregationSelectorCommand),
    Grid(GridSelectorCommand),
    Profile(ProfileSelectorCommand),
}

impl SelectorTaskKind {
    fn db(&self) -> i32 {
        match self {
            SelectorTaskKind::Vector(iqc) => iqc.options.db,
            SelectorTaskKind::Range(rc) => rc.options.db,
            SelectorTaskKind::Aggregation(ac) => ac.options.db,
            SelectorTaskKind::Grid(gc) => gc.options.db,
            SelectorTaskKind::Profile(pc) => pc.options.db,
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
    /// A cluster range read, each series still in the chunk its shard packed
    /// it into. Decoded by the requester on the executor's pool rather than
    /// in the fanout callback, which runs on the main thread and serially.
    WireMatrix(Vec<WireRangeSeries>),
    Aggregation(AggregationOutcome),
    Grid(GridOutcome),
    /// A label profile, or `None` when the source declined to build one.
    Profile(Option<LabelProfile>),
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
            SelectorOutput::WireMatrix(series) => Ok(series
                .into_par()
                .with_pool(RayonPool(&MATERIALIZE_POOL))
                .map(WireRangeSeries::decode)
                .collect()),
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

    fn into_grid(self) -> QueryResult<GridOutcome> {
        match self {
            SelectorOutput::Grid(outcome) => Ok(outcome),
            _ => Err(QueryError::Execution(
                "BUG: grid task returned a non-grid result".to_string(),
            )),
        }
    }

    fn into_profile(self) -> QueryResult<Option<LabelProfile>> {
        match self {
            SelectorOutput::Profile(profile) => Ok(profile),
            _ => Err(QueryError::Execution(
                "BUG: profile task returned a non-profile result".to_string(),
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
/// enqueue a task and block on its responder; they never take the module lock or touch the
/// keyspace themselves.
///
/// The evaluator calls in from inside rayon jobs (`preload_grid` fans selectors out on the
/// pool; grid reads happen from step chunks), and a submitter may therefore be a pool
/// worker blocked in `recv`. Two invariants keep that from deadlocking:
///
/// 1. The processor is never a pool worker. The earlier cooperative design let the first
///    submitter drain the queue; a worker in that role could be handed another submitter's
///    closure by work-stealing while it waited on its own fan-out, and that closure would
///    then wait on the processor's own thread forever.
/// 2. Nothing the processor does needs the global pool. Its materialization fans out on a
///    private pool ([`MATERIALIZE_POOL`]), and `TimeSeries::get_range` decodes on the calling
///    thread when that thread is a pool worker. So every global worker may sit in `recv` at
///    once and the processor still finishes. (An earlier version instead had waiting workers
///    keep running pool jobs; that let one worker nest a blocking wait per stolen closure,
///    which under a burst of subquery steps recursed until the stack overflowed.)
///
/// A third rule lives with the callers: a pool job must not hold the module lock while it
/// waits on the pool — see `threads::spawn_background`.
///
/// For local queries the thread does two things per batch. Under the module lock it resolves
/// each task's series, answers the instant reads (one cached sample per series), and copies
/// out the compressed chunks a range or grid read touches ([`RangeSnapshot`]). It then
/// releases the lock and decodes those chunks on [`MATERIALIZE_POOL`], so the lock is held
/// for the memcpy rather than for the decode — the main thread serves commands while a
/// range read materializes.
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

        // Under the module lock: resolve series, answer what is cheap to
        // answer, and copy out the chunks the range reads need. Decoding them
        // — the bulk of a range read — happens below, with the lock released
        // and the main thread free to serve commands meanwhile.
        let deferred: Vec<DeferredRangeDecode> = {
            let ctx = MODULE_CONTEXT.lock();
            batch
                .into_iter()
                .filter_map(|task| {
                    isolate_panic("selector task", || execute_selector_task(&ctx, task)).flatten()
                })
                .collect()
            // ctx dropped here — MODULE_CONTEXT released
        };
        for work in deferred {
            isolate_panic("range decode", || work.finish());
        }
    }
}

/// Run one task of the processor, surviving its panic.
///
/// The processor is one thread for the whole process: a panic that unwound
/// through [`run_processor`] ended it, and every later `TS.QUERY` failed with
/// "selector executor thread is not running" until a restart. Caught here, a
/// panic fails only the task that raised it — its responder is dropped during
/// the unwind, so its caller gets an error rather than waiting — and the rest
/// of the batch is still served. The tasks only read, so no state is left
/// half-written by the unwind.
fn isolate_panic<T>(what: &str, task: impl FnOnce() -> T) -> Option<T> {
    match catch_unwind(AssertUnwindSafe(task)) {
        Ok(value) => Some(value),
        Err(payload) => {
            log_warning(format!(
                "PromQL selector executor: {what} panicked: {}",
                panic_message(payload.as_ref())
            ));
            None
        }
    }
}

/// A local range or grid read whose chunks were copied under the module
/// lock and still have to be decoded, validated and answered.
struct DeferredRangeDecode {
    series: Vec<(EvalLabels, RangeSnapshot)>,
    options: QueryOptions,
    grid: bool,
    responder: mpsc::SyncSender<QueryResult<SelectorOutput>>,
}

impl DeferredRangeDecode {
    fn finish(self) {
        let result = decode_range_snapshots(self.series, &self.options).map(|ranges| {
            if self.grid {
                SelectorOutput::Grid(GridOutcome::Raw(ranges))
            } else {
                SelectorOutput::Matrix(ranges)
            }
        });
        deliver_task_result(&self.responder, result);
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

    /// Evaluate `request` over the grid for the series `matchers` selects.
    ///
    /// In cluster mode the whole grid is pushed to the shards and only one
    /// point per series per step (or one partial per group per step) comes
    /// back. On a single node there is nothing to push down to, so the raw
    /// spans are returned for the caller to evaluate — doing it here would
    /// hold the module lock for the length of the evaluation.
    pub fn query_grid(
        &self,
        matchers: Matchers,
        request: GridRequest,
        options: QueryOptions,
        caller_user: Option<String>,
        hash_tags: Arc<[String]>,
    ) -> QueryResult<GridOutcome> {
        let command = SelectorTaskKind::Grid(GridSelectorCommand {
            matchers,
            request,
            options,
        });
        self.submit_selector_task(command, caller_user, hash_tags)?
            .into_grid()
    }

    /// The labels of the series `matchers` select — see
    /// [`crate::promql::engine::label_profile`]. Answered from the index on
    /// a single node and by every shard in a cluster; `None` when the
    /// selector matches more series than the query's cap.
    pub fn label_profile(
        &self,
        matchers: Matchers,
        options: QueryOptions,
        caller_user: Option<String>,
        hash_tags: Arc<[String]>,
    ) -> QueryResult<Option<LabelProfile>> {
        let command = SelectorTaskKind::Profile(ProfileSelectorCommand { matchers, options });
        self.submit_selector_task(command, caller_user, hash_tags)?
            .into_profile()
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

/// The pool the processor materializes on. Private to the executor so that its work never
/// depends on the global pool, whose workers may all be parked in
/// [`SelectorBatchExecutor::submit_selector_task`] waiting for exactly this work.
static MATERIALIZE_POOL: LazyLock<rayon_core::ThreadPool> = LazyLock::new(|| {
    rayon_core::ThreadPoolBuilder::new()
        .num_threads(crate::config::num_threads())
        .thread_name(|index| format!("ts-promql-io-{index}"))
        .build()
        .expect("failed to build the PromQL materialization pool")
});

/// Wait for a selector result. A plain blocking wait, on a pool worker too: the processor
/// needs nothing from this thread's pool to answer (see the type-level docs), and a wait
/// that ran other jobs meanwhile would stack one blocking wait per stolen closure.
fn wait_for_result<T>(rx: &mpsc::Receiver<T>) -> Result<T, mpsc::RecvError> {
    rx.recv()
}

fn execute_selector_task(ctx: &Context, task: SelectorTask) -> Option<DeferredRangeDecode> {
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
    let mut deferred = None;
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
            match execute_selector_task_local(ctx, kind) {
                LocalOutcome::Answered(result) => deliver_task_result(&responder, result),
                LocalOutcome::Deferred(series, options, grid) => {
                    deferred = Some(DeferredRangeDecode {
                        series,
                        options,
                        grid,
                        responder,
                    });
                }
            }
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
    deferred
}

/// What running a task under the module lock produced: a finished answer, or
/// — for the range reads — the copied chunks still to decode once the lock is
/// gone (`series`, the task's options, and whether the answer is a grid's).
enum LocalOutcome {
    Answered(QueryResult<SelectorOutput>),
    Deferred(Vec<(EvalLabels, RangeSnapshot)>, QueryOptions, bool),
}

fn execute_selector_task_local(ctx: &Context, command: SelectorTaskKind) -> LocalOutcome {
    match command {
        SelectorTaskKind::Vector(iqc) => {
            let timestamp = iqc.timestamp;
            let selector: SeriesSelector = SeriesSelector::from(iqc.matchers);
            LocalOutcome::Answered(
                query_instant_local(ctx, selector, timestamp, iqc.options)
                    .map(SelectorOutput::Vector),
            )
        }
        SelectorTaskKind::Range(rc) => {
            let start = rc.start_timestamp;
            let end = rc.end_timestamp;
            let selector: SeriesSelector = SeriesSelector::from(rc.matchers);
            match snapshot_range_local(ctx, selector, start, end, &rc.options) {
                Ok(series) => LocalOutcome::Deferred(series, rc.options, false),
                Err(err) => LocalOutcome::Answered(Err(err)),
            }
        }
        SelectorTaskKind::Aggregation(ac) => {
            // Single node: there is no shard to push the operator to, so hand
            // the raw vector back and let the caller aggregate it outside the
            // module lock.
            let timestamp = ac.timestamp;
            let selector: SeriesSelector = SeriesSelector::from(ac.matchers);
            LocalOutcome::Answered(
                query_instant_local(ctx, selector, timestamp, ac.options)
                    .map(|samples| SelectorOutput::Aggregation(AggregationOutcome::Raw(samples))),
            )
        }
        SelectorTaskKind::Grid(gc) => {
            // Single node: same reasoning as the aggregation task — read the
            // spans and let the caller evaluate them outside the module lock.
            let Some((start, end)) = gc.request.fetch_bounds() else {
                return LocalOutcome::Answered(Ok(SelectorOutput::Grid(GridOutcome::Raw(
                    Vec::new(),
                ))));
            };
            let selector: SeriesSelector = SeriesSelector::from(gc.matchers);
            match snapshot_range_local(ctx, selector, start, end, &gc.options) {
                Ok(series) => LocalOutcome::Deferred(series, gc.options, true),
                Err(err) => LocalOutcome::Answered(Err(err)),
            }
        }
        SelectorTaskKind::Profile(pc) => {
            let selector: SeriesSelector = SeriesSelector::from(pc.matchers);
            let cap = profiled_series_cap(&pc.options);
            LocalOutcome::Answered(
                local_label_profile(ctx, selector, cap)
                    .map(SelectorOutput::Profile)
                    .map_err(|e| QueryError::Execution(e.to_string())),
            )
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
                let samples = cmd.into_samples();
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
    let max_samples = rc.options.max_samples;
    let targets = compute_hash_tag_fanout_target(ctx, hash_tags);
    let responder = Arc::new(responder);
    let cloned_responder = responder.clone();

    let handler = move |cmd: RangeVectorSelectorFanoutCommand, result: FanoutCommandResult| {
        let query_result = match result {
            Ok(()) => {
                let resp = cmd.get_response();

                validate_max_series_(resp.series.len(), max_series).and_then(|_| {
                    // The chunks know their length without being decoded, so
                    // the limits are checked before any sample is materialized.
                    let series = resp
                        .series
                        .into_iter()
                        .map(|rs| {
                            WireRangeSeries::try_from(rs).map_err(|e| {
                                QueryError::Execution(format!(
                                    "undecodable range series in cluster response: {e}"
                                ))
                            })
                        })
                        .collect::<QueryResult<Vec<_>>>()?;
                    validate_max_samples(series.iter().map(|s| s.chunk.len()).sum(), max_samples)?;
                    for s in &series {
                        validate_max_points_per_series(s.chunk.len(), max_points_per_series)?;
                    }
                    // Still packed: the requester decodes them in parallel.
                    Ok(SelectorOutput::WireMatrix(series))
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

/// Push a grid query to the shards and collect their output here.
///
/// A series lives on exactly one shard, so the shards' per-series outputs are
/// disjoint and the coordinator concatenates rather than merges; a fused
/// request's per-`(group, step)` partials are merged by the command itself.
fn execute_cluster_grid(
    ctx: &Context,
    gc: GridSelectorCommand,
    hash_tags: &[String],
    responder: mpsc::SyncSender<QueryResult<SelectorOutput>>,
) {
    let timeout = calculate_timeout(&gc.options);
    let max_series = gc.options.max_series;
    let max_points_per_series = gc.options.max_points_per_series;
    let cmd = GridFanoutCommand::new(
        gc.matchers,
        gc.request,
        max_series as u64,
        max_points_per_series.unwrap_or(0) as u64,
        timeout,
    );

    let targets = compute_hash_tag_fanout_target(ctx, hash_tags);
    let responder = Arc::new(responder);
    let cloned_responder = responder.clone();

    let handler = move |cmd: GridFanoutCommand, result: FanoutCommandResult| {
        let query_result = match result.and_then(|()| cmd.into_result()) {
            Ok(outcome) => {
                // The grid output, not the input, is what these bound: one
                // entry per series (or group), one point per step that produced
                // one.
                let points: Vec<usize> = match &outcome {
                    GridOutcome::Stepped(series) => series.iter().map(|s| s.points.len()).collect(),
                    GridOutcome::Rolled(series)
                    | GridOutcome::Reduced(series)
                    | GridOutcome::Raw(series) => series.iter().map(|s| s.samples.len()).collect(),
                };
                validate_max_series_(points.len(), max_series).and_then(|_| {
                    for count in points {
                        validate_max_points_per_series(count, max_points_per_series)?;
                    }
                    Ok(SelectorOutput::Grid(outcome))
                })
            }
            Err(e) => Err(selector_fanout_failure("grid", e)),
        };

        deliver_task_result(&responder, query_result);
    };

    if let Err(e) = exec_command(ctx, cmd, targets, timeout, handler) {
        deliver_task_result(&cloned_responder, Err(e.into()));
    }
}

/// Every shard profiles its own series; the command adds the counts up. The
/// query's cap is sent along so that no shard walks a selector the
/// coordinator would decline anyway.
fn execute_cluster_label_profile(
    ctx: &Context,
    pc: ProfileSelectorCommand,
    hash_tags: &[String],
    responder: mpsc::SyncSender<QueryResult<SelectorOutput>>,
) {
    let timeout = calculate_timeout(&pc.options);
    let cmd = LabelProfileFanoutCommand::new(
        pc.matchers,
        profiled_series_cap(&pc.options) as u64,
        timeout,
    );

    let targets = compute_hash_tag_fanout_target(ctx, hash_tags);
    let responder = Arc::new(responder);
    let cloned_responder = responder.clone();

    let handler = move |cmd: LabelProfileFanoutCommand, result: FanoutCommandResult| {
        let query_result = match result {
            Ok(()) => Ok(SelectorOutput::Profile(cmd.into_result())),
            Err(e) => Err(selector_fanout_failure("label-profile", e)),
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
        SelectorTaskKind::Grid(gc) => {
            execute_cluster_grid(ctx, gc, &hash_tags, task.responder);
        }
        SelectorTaskKind::Profile(pc) => {
            execute_cluster_label_profile(ctx, pc, &hash_tags, task.responder);
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

    // The executor's own pool, never the global one: its workers may all be
    // waiting on this very task. From a `Vec` rather than `iter_into_par` —
    // one cached sample per item is far too little work to pull through a
    // mutex-wrapped iterator (see `snapshot_range_local`).
    let series: Vec<&TimeSeries> = series.iter().map(|(s, _)| s.deref()).collect();
    let samples = series
        .into_par()
        .with_pool(RayonPool(&MATERIALIZE_POOL))
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

/// The under-lock half of a local range read: resolve the series and copy
/// out the chunks the range touches. Nothing here decodes a sample.
fn snapshot_range_local(
    ctx: &Context,
    selector: SeriesSelector,
    start_time: i64,
    end_time: i64,
    options: &QueryOptions,
) -> QueryResult<Vec<(EvalLabels, RangeSnapshot)>> {
    if let Some(d) = options.deadline
        && current_time_millis() > d
    {
        return Err(QueryError::Timeout);
    }
    let series = series_by_selectors(ctx, &[selector], None)
        .map_err(|e| QueryError::Execution(e.to_string()))?;
    // The copy fans out on the executor's pool, as the decode it replaced did:
    // one thread copying a query's worth of chunks was most of what the
    // decode had cost, and the lock is held either way. From a `Vec`, not
    // `iter_into_par`: that wraps the iterator in a mutex, and with work this
    // small per item the pull lock convoys through the kernel.
    let series: Vec<&TimeSeries> = series.iter().map(|(s, _)| s.deref()).collect();
    Ok(series
        .into_par()
        .with_pool(RayonPool(&MATERIALIZE_POOL))
        .map(|s| {
            (
                EvalLabels::interned(&s.labels),
                s.snapshot_range(start_time, end_time),
            )
        })
        .collect())
}

/// The other half, off the lock: decode every snapshot on the executor's own
/// pool and apply the per-series and series-count limits.
fn decode_range_snapshots(
    series: Vec<(EvalLabels, RangeSnapshot)>,
    options: &QueryOptions,
) -> QueryResult<Vec<RangeSample<EvalLabels>>> {
    let max_points = options.max_points_per_series;
    // Exact accounting for this read against the query's sample budget, and an
    // early stop: once it is spent, the remaining series are not decoded at all,
    // so the overshoot is at most one series per pool thread.
    let budget = SampleBudget::new(options.max_samples);
    let ranges = series
        .into_par()
        .with_pool(RayonPool(&MATERIALIZE_POOL))
        .filter_map(|(labels, snapshot)| {
            if budget.exhausted() {
                return Some(Err(too_many_samples(budget.loaded(), budget.limit())));
            }
            let samples = match get_snapshot_range(&snapshot, max_points) {
                Ok(samples) => samples,
                Err(err) => {
                    log_warning(&err);
                    return Some(Err(QueryError::Execution(err)));
                }
            };
            if let Err(err) = budget.charge(samples.len()) {
                return Some(Err(err));
            }
            if samples.is_empty() {
                return None;
            }
            Some(Ok(RangeSample { samples, labels }))
        })
        .into_fallible()
        .collect::<Vec<_>>()?;

    // Bound the series the query returns, not the ones the selector matched:
    // an empty range contributes nothing. This is also the path a single-node
    // grid query takes, so it must agree with the pushed-down one about which
    // queries `max_series` rejects.
    validate_max_series_(ranges.len(), options.max_series)?;

    Ok(ranges)
}

#[cfg(test)]
mod selector_batch_executor_tests {
    use super::{MATERIALIZE_POOL, collect_batch, selector_fanout_failure, wait_for_result};
    use crate::common::threads::{ParWithPool, RayonPool};
    use crate::fanout::FanoutError;
    use crate::promql::QueryError;
    use orx_parallel::{IntoParIter, Par};
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

    /// Every worker of the caller's pool parked in `wait_for_result` while the answer is
    /// produced by a processor thread that fans out on a pool of its own. This is the
    /// executor's shape under load; it must complete without any caller-pool worker
    /// running a job.
    #[test]
    fn parked_pool_never_starves_a_processor_with_its_own_pool() {
        let callers = rayon_core::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .unwrap();
        let (task_tx, task_rx) = mpsc::channel::<mpsc::SyncSender<usize>>();
        std::thread::spawn(move || {
            for responder in task_rx {
                let sum: usize = (0..10_000usize)
                    .into_par()
                    .with_pool(RayonPool(&MATERIALIZE_POOL))
                    .sum();
                responder.send(sum).unwrap();
            }
        });
        let (done_tx, done_rx) = mpsc::channel();
        callers.spawn_broadcast(move |_| {
            let (tx, rx) = mpsc::sync_channel(1);
            task_tx.send(tx).unwrap();
            done_tx.send(wait_for_result(&rx)).unwrap();
        });
        for _ in 0..2 {
            let answer = done_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("a parked caller pool starved the processor");
            assert_eq!(answer, Ok((0..10_000usize).sum()));
        }
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
    fn a_panicking_task_fails_alone() {
        // The processor's loop shape: several tasks, each owning a responder.
        // A panic drops the panicking task's responder, so its caller sees an
        // error instead of waiting, and the tasks after it are still served.
        let (first_tx, first_rx) = mpsc::sync_channel::<u32>(1);
        let (second_tx, second_rx) = mpsc::sync_channel::<u32>(1);
        for (panics, responder) in [(true, first_tx), (false, second_tx)] {
            super::isolate_panic("test task", move || {
                if panics {
                    panic!("boom");
                }
                responder.send(7).unwrap();
            });
        }
        assert!(
            first_rx.recv().is_err(),
            "the panicking task's caller is told"
        );
        assert_eq!(second_rx.recv(), Ok(7), "the next task still runs");
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
