use crate::common::Timestamp;
use crate::common::context::{get_current_db, set_current_db};
use crate::common::logging::log_warning;
use crate::common::threads::{
    IntoParRayon, LockGil, MATERIALIZE_POOL, ThreadRole, check_may_block, panic_message,
    run_on_pool_cold, set_thread_role,
};
use crate::common::time::current_time_millis;
use crate::fanout::{FanoutCommandResult, FanoutError, exec_command, get_cluster_command_timeout};
use crate::fanout::{compute_hash_tag_fanout_target, is_clustered, with_fanout_user};
use crate::labels::filters::SeriesSelector;
use crate::promql::EvalLabels;
use crate::promql::engine::label_profile::{
    LabelProfile, LabelProfileBuilder, profiled_series_cap,
};
use crate::promql::engine::query_reader::{
    AggregationOutcome, AggregationRequest, GridOutcome, GridRequest,
};
use crate::promql::engine::sample_budget::{SampleBudget, too_many_samples, validate_max_samples};
use crate::promql::engine::{
    AggregationFanoutCommand, GridFanoutCommand, InstantVectorParams,
    InstantVectorSelectorFanoutCommand, LabelProfileFanoutCommand,
    RangeVectorSelectorFanoutCommand, WireRangeResponse, check_unique_series, decode_range_series,
    get_snapshot_range, instant_lookback_start_ms, validate_max_points, validate_max_series,
};
use crate::promql::{InstantSample, QueryError, QueryOptions, QueryResult, RangeSample};
use crate::series::chunks::ChunkOps;
use crate::series::index::{
    BatchLimit, DEFAULT_BATCH_COPY_BYTES, DEFAULT_SERIES_BATCH_SIZE, SeriesCursor,
};
use crate::series::{RangeSnapshot, TimeSeries};
use orx_parallel::Par;
use orx_parallel::ParResult;
use promql_parser::label::Matchers;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, mpsc};
use std::time::Duration;
use valkey_module::{Context, MODULE_CONTEXT};

/// Max number of requests to process in a single batch. The batch's reads share
/// each lock hold (see [`run_local_batch`]), so this bounds how many tasks split
/// one hold's series between them.
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
    /// A cluster range read, one entry per shard: each series still in the
    /// chunk its shard packed it into, its labels still refs into that
    /// shard's symbol table. Resolved and decoded by the requester on the
    /// executor's pool rather than in the fanout callback, which runs on the
    /// main thread and serially.
    WireMatrix(Vec<WireRangeResponse>),
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
            // Decoded in parallel on the materialization pool, but waited for
            // cold: the caller is usually an evaluation worker, and entering
            // another pool directly would have it run evaluation jobs while it
            // waits, each of which may block on this executor in turn.
            SelectorOutput::WireMatrix(responses) => run_on_pool_cold(&MATERIALIZE_POOL, || {
                // Labels per response (each resolver caches its own table's
                // pairs), then the check across all of them, then samples
                // per series.
                let mut labelled = Vec::new();
                for response in responses
                    .into_par_on(&MATERIALIZE_POOL)
                    .map(WireRangeResponse::into_labelled)
                    .collect::<Vec<_>>()
                {
                    labelled.extend(response.map_err(|e| {
                        QueryError::Execution(format!(
                            "undecodable range series in cluster response: {e}"
                        ))
                    })?);
                }
                check_unique_series(labelled.iter().map(|(labels, _)| labels))
                    .map_err(QueryError::Execution)?;
                Ok(labelled
                    .into_par_on(&MATERIALIZE_POOL)
                    .map(decode_range_series)
                    .collect())
            }),
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

/// An executor responsible for executing PromQL selectors as part of a keyspace batch operation.
///
/// The `SelectorBatchExecutor` optimizes latency in the PromQL evaluator (especially in cluster mode) by:
///
/// - Serializing access to the Valkey keyspace via `MODULE_CONTEXT` to avoid deadlocks, ensuring that
///   we can query safely from multiple threads.
/// - Collecting incoming selector tasks into a batch whose reads share each lock acquisition, to
///   reduce locking overhead.
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
/// 2. Nothing the processor does needs a shared pool. Its materialization fans out on a
///    private pool ([`MATERIALIZE_POOL`]), and `TimeSeries::get_range` decodes on the calling
///    thread when that thread is a pool worker. So every evaluation worker may sit in `recv` at
///    once and the processor still finishes. (An earlier version instead had waiting workers
///    keep running pool jobs; that let one worker nest a blocking wait per stolen closure,
///    which under a burst of subquery steps recursed until the stack overflowed.)
///
/// These are rules R1–R3 of `common::threads`; `wait_for_result` checks the submitter's side.
///
/// For local queries the thread reads the keyspace a batch of series per module-lock hold,
/// shared among the queued tasks ([`run_local_batch`]): under the lock it answers the instant
/// reads (one cached sample per series) and copies out the compressed chunks a range or grid
/// read touches ([`RangeSnapshot`]); between holds it decodes them on [`MATERIALIZE_POOL`]. So
/// the lock is held for at most a hold's worth of copying, however many series a selector
/// matches, and the main thread serves commands in between.
/// For cluster queries, a synchronous call is made per query and the context is released. The processing itself
/// is executed in parallel across all target cluster nodes, and results are returned asynchronously without
/// holding the GIL.
///
/// There is one, [`SERIES_SELECTOR`](super::querier::SERIES_SELECTOR), which
/// `ValkeySeriesQuerier` submits every selector read to.
pub struct SelectorBatchExecutor {
    /// Hand-off to the processor thread. Unbounded: a submitter blocks on its
    /// responder anyway, so back-pressure here would only add a second wait.
    /// `None` when the thread could not be started: every query then fails,
    /// rather than the executor panicking inside its `LazyLock` and poisoning it.
    sender: Option<mpsc::Sender<SelectorTask>>,
}

/// The processor loop: wait for one task, sweep up whatever else is already
/// queued (bounded by [`MAX_BATCH_SIZE`]), then run the batch. Exits when the
/// executor handle is dropped.
///
/// In a cluster every task only starts a fan-out, so the batch shares one
/// short module-lock hold. On a single node the tasks read the keyspace
/// themselves, a batch of series per hold, shared among them: see
/// [`run_local_batch`].
fn run_processor(receiver: mpsc::Receiver<SelectorTask>) {
    while let Ok(first) = receiver.recv() {
        let batch = collect_batch(&receiver, first, MAX_BATCH_SIZE);

        let local = {
            let ctx = MODULE_CONTEXT.lock_gil();
            if is_clustered(&ctx) {
                for task in batch {
                    isolate_panic("selector task", || execute_selector_task(&ctx, task));
                }
                Vec::new()
            } else {
                batch
            }
        };
        if !local.is_empty() {
            run_local_batch(local);
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
        let spawned = std::thread::Builder::new()
            .name("ts-promql-selector".to_string())
            .spawn(move || {
                set_thread_role(ThreadRole::Blocking);
                run_processor(receiver)
            });
        let sender = match spawned {
            Ok(_) => Some(sender),
            Err(err) => {
                log_warning(format!(
                    "failed to spawn the PromQL selector executor thread: {err}"
                ));
                None
            }
        };
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

        let sent = self
            .sender
            .as_ref()
            .is_some_and(|sender| sender.send(task).is_ok());
        if !sent {
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

/// Wait for a selector result. A plain blocking wait, on a pool worker too: the processor
/// needs nothing from this thread's pool to answer (see the type-level docs), and a wait
/// that ran other jobs meanwhile would stack one blocking wait per stolen closure.
#[track_caller]
fn wait_for_result<T>(rx: &mpsc::Receiver<T>) -> Result<T, mpsc::RecvError> {
    check_may_block();
    rx.recv()
}

/// Start one task's cluster fan-out, under the module lock, as the task's
/// database and caller.
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
    // The fanout command captures the authenticated user while this scope is
    // active and propagates it to every shard.
    let result = with_fanout_user(ctx, caller_user.as_deref(), |ctx| {
        execute_selector_task_cluster(
            ctx,
            SelectorTask {
                kind,
                caller_user: None,
                hash_tags,
                responder,
            },
        );
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
                let responses = cmd.get_responses();
                let series_count = responses.iter().map(|r| r.series.len()).sum();

                validate_max_series_(series_count, max_series).and_then(|_| {
                    // The chunks know their length without being decoded, so
                    // the limits are checked before any sample is materialized.
                    let responses = responses
                        .into_iter()
                        .map(|resp| {
                            WireRangeResponse::try_from(resp).map_err(|e| {
                                QueryError::Execution(format!(
                                    "undecodable range series in cluster response: {e}"
                                ))
                            })
                        })
                        .collect::<QueryResult<Vec<_>>>()?;
                    let series = || responses.iter().flat_map(|r| r.series());
                    validate_max_samples(series().map(|s| s.chunk.len()).sum(), max_samples)?;
                    for s in series() {
                        validate_max_points_per_series(s.chunk.len(), max_points_per_series)?;
                    }
                    // Still packed: the requester resolves and decodes them
                    // in parallel.
                    Ok(SelectorOutput::WireMatrix(responses))
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
        let peer_unsupported = cmd.peer_unsupported();
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
                    GridOutcome::Unsupported => Vec::new(),
                };
                validate_max_series_(points.len(), max_series).and_then(|_| {
                    for count in points {
                        validate_max_points_per_series(count, max_points_per_series)?;
                    }
                    Ok(SelectorOutput::Grid(outcome))
                })
            }
            Err(e) if peer_unsupported => {
                log_warning(format!(
                    "promql: grid push-down unsupported by a peer, evaluating without it: {e}"
                ));
                Ok(SelectorOutput::Grid(GridOutcome::Unsupported))
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

/// What one module-lock hold on a single node may read, in units of an instant
/// series: a range or grid series costs [`RANGE_SERIES_COST`] (its chunks are
/// copied), an instant or profile series one (a cached sample, or its labels).
/// So a hold reads 512 range series, as a fan-out share's batch does, or 4096
/// instant ones — with one hold per 512, a 15 000-series instant query paid
/// thirty lock acquisitions and lost 15 % of its throughput.
///
/// Range and grid reads also share [`DEFAULT_BATCH_COPY_BYTES`] of chunk
/// copies per hold, as a fan-out share's batch does: a long window over dense
/// series copies hundreds of chunks per series, and the series count alone
/// would let one hold copy them all.
const HOLD_BUDGET: usize = RANGE_SERIES_COST * DEFAULT_SERIES_BATCH_SIZE.get();

/// The cost of a range or grid series against [`HOLD_BUDGET`].
const RANGE_SERIES_COST: usize = 8;

/// Run a batch of single-node tasks to completion.
///
/// Each task is planned with no lock (the postings only), then read under
/// module-lock holds of [`HOLD_BUDGET`], with its database selected
/// and its caller's identity installed. A hold's series go to the tasks with
/// the fewest left first. So small tasks finish in the first hold, sharing one
/// lock acquisition (which can mean waiting out a main-thread event-loop
/// iteration), and large ones run one after another, each split across holds,
/// as they did before the reads were split at all. An even split looked fairer
/// and was worse: four equal reads all finished at the end instead of one
/// after another (4T each against 2.5T on average, queries 16–30 % slower) and
/// held all four reads' decoded samples at once (peak memory +60 %). Under the lock an instant
/// read takes its samples and a profile its labels; a range or grid read copies
/// the chunks it touches ([`RangeSnapshot`]) and decodes them on
/// [`MATERIALIZE_POOL`] once the lock is released, before the next hold. The
/// copy is sequential: the lock holder never waits on a pool.
///
/// The local index is the whole picture on a single node, so the routing scope
/// is deliberately ignored here: `HASHTAG` selects shards, it does not filter
/// keys or labels.
fn run_local_batch(tasks: Vec<SelectorTask>) {
    let mut reads: Vec<LocalRead> = tasks.into_iter().filter_map(LocalRead::plan).collect();
    while !reads.is_empty() {
        // Shortest remaining first; stable, so equal reads keep their arrival order.
        reads.sort_by_key(LocalRead::remaining_cost);
        let mut copied: Vec<Option<CopiedSnapshots>> = Vec::with_capacity(reads.len());
        {
            let ctx = MODULE_CONTEXT.lock_gil();
            let original_db = get_current_db(&ctx);
            let mut budget = HOLD_BUDGET;
            let mut copy_bytes = DEFAULT_BATCH_COPY_BYTES;
            for read in reads.iter_mut() {
                let cost = read.series_cost();
                let share = (budget / cost).min(read.cursor.remaining());
                let out_of_bytes = read.copies_chunks() && copy_bytes == 0;
                if (share == 0 || out_of_bytes) && !read.cursor.is_done() {
                    copied.push(None);
                    continue;
                }
                budget -= share * cost;
                let batch = isolate_panic("selector read", || read.copy(&ctx, share, copy_bytes));
                copied.push(match batch {
                    Some(Ok(batch)) => {
                        let bytes: usize =
                            batch.iter().flatten().map(|(_, s)| s.copied_bytes()).sum();
                        copy_bytes = copy_bytes.saturating_sub(bytes);
                        batch
                    }
                    Some(Err(err)) => {
                        read.fail(err);
                        None
                    }
                    // A panic: the responder goes with the read, failing its caller.
                    None => {
                        read.failed = true;
                        None
                    }
                });
            }
            if get_current_db(&ctx) != original_db {
                let _ = set_current_db(&ctx, original_db);
            }
        }

        for (read, batch) in reads.iter_mut().zip(copied) {
            if let Some(batch) = batch
                && isolate_panic("range decode", || read.decode(batch)).is_none()
            {
                read.failed = true;
            }
        }

        reads.retain_mut(|read| {
            if read.failed {
                return false;
            }
            if read.cursor.is_done() {
                read.finish();
                return false;
            }
            if read
                .options
                .deadline
                .is_some_and(|d| current_time_millis() > d)
            {
                read.fail(QueryError::Timeout);
                return false;
            }
            true
        });
        if !reads.is_empty() {
            // As between a fan-out share's batches: let a waiting main thread in.
            std::thread::yield_now();
        }
    }
}

/// One range or grid batch's chunks, copied under the lock for decoding after.
type CopiedSnapshots = Vec<(EvalLabels, RangeSnapshot)>;

/// One single-node task's read in progress.
struct LocalRead {
    cursor: SeriesCursor,
    caller_user: Option<String>,
    options: QueryOptions,
    responder: mpsc::SyncSender<QueryResult<SelectorOutput>>,
    state: ReadState,
    /// Answered with an error, or lost to a panic: nothing more to do.
    failed: bool,
}

enum ReadState {
    /// The newest sample in `[start, end]` of every series: a vector, or the
    /// raw input of an aggregation the caller evaluates itself.
    Instant {
        start: Timestamp,
        end: Timestamp,
        aggregation: bool,
        samples: Vec<InstantSample<EvalLabels>>,
    },
    /// The samples in `[start, end]` of every series that has any.
    Range {
        start: Timestamp,
        end: Timestamp,
        grid: bool,
        budget: SampleBudget,
        ranges: Vec<RangeSample<EvalLabels>>,
    },
    /// The label profile of the matched series, up to `cap` of them.
    Profile {
        cap: usize,
        matched: usize,
        builder: LabelProfileBuilder,
        overflow: bool,
    },
}

impl LocalRead {
    /// What reading one series costs against [`HOLD_BUDGET`].
    fn series_cost(&self) -> usize {
        if self.copies_chunks() {
            RANGE_SERIES_COST
        } else {
            1
        }
    }

    /// Whether this read copies chunks, against a hold's
    /// [`DEFAULT_BATCH_COPY_BYTES`].
    fn copies_chunks(&self) -> bool {
        matches!(self.state, ReadState::Range { .. })
    }

    /// What the rest of the read costs: the order a hold serves reads in.
    fn remaining_cost(&self) -> usize {
        self.cursor.remaining() * self.series_cost()
    }

    /// Plan `task`'s read, or answer it on the spot (`None`): past its deadline,
    /// a grid with nothing to fetch, or a selector the index cannot plan.
    fn plan(task: SelectorTask) -> Option<Self> {
        let SelectorTask {
            kind,
            caller_user,
            hash_tags: _,
            responder,
        } = task;
        let db = kind.db();
        let (matchers, options, state) = match kind {
            SelectorTaskKind::Vector(iqc) => {
                let lookback_ms = iqc.options.lookback_delta.as_millis() as Timestamp;
                let state = ReadState::Instant {
                    // PromQL's lookback window is (timestamp - lookback, timestamp].
                    start: instant_lookback_start_ms(iqc.timestamp, lookback_ms),
                    end: iqc.timestamp,
                    aggregation: false,
                    samples: Vec::new(),
                };
                (iqc.matchers, iqc.options, state)
            }
            SelectorTaskKind::Aggregation(ac) => {
                // Single node: there is no shard to push the operator to, so the
                // raw vector goes back and the caller aggregates it off the lock.
                let lookback_ms = ac.options.lookback_delta.as_millis() as Timestamp;
                let state = ReadState::Instant {
                    start: instant_lookback_start_ms(ac.timestamp, lookback_ms),
                    end: ac.timestamp,
                    aggregation: true,
                    samples: Vec::new(),
                };
                (ac.matchers, ac.options, state)
            }
            SelectorTaskKind::Range(rc) => {
                let state = ReadState::Range {
                    start: rc.start_timestamp,
                    end: rc.end_timestamp,
                    grid: false,
                    budget: SampleBudget::new(rc.options.max_samples),
                    ranges: Vec::new(),
                };
                (rc.matchers, rc.options, state)
            }
            SelectorTaskKind::Grid(gc) => {
                // Single node: same reasoning as the aggregation task — read the
                // spans and let the caller evaluate them off the lock.
                let Some((start, end)) = gc.request.fetch_bounds() else {
                    let empty = SelectorOutput::Grid(GridOutcome::Raw(Vec::new()));
                    deliver_task_result(&responder, Ok(empty));
                    return None;
                };
                let state = ReadState::Range {
                    start,
                    end,
                    grid: true,
                    budget: SampleBudget::new(gc.options.max_samples),
                    ranges: Vec::new(),
                };
                (gc.matchers, gc.options, state)
            }
            SelectorTaskKind::Profile(pc) => {
                let state = ReadState::Profile {
                    cap: profiled_series_cap(&pc.options),
                    matched: 0,
                    builder: LabelProfileBuilder::new(),
                    overflow: false,
                };
                (pc.matchers, pc.options, state)
            }
        };

        if options.deadline.is_some_and(|d| current_time_millis() > d) {
            deliver_task_result(&responder, Err(QueryError::Timeout));
            return None;
        }
        let selector = SeriesSelector::from(matchers);
        match SeriesCursor::plan(db, &[selector], None) {
            Ok(cursor) => Some(Self {
                cursor,
                caller_user,
                options,
                responder,
                state,
                failed: false,
            }),
            Err(err) => {
                deliver_task_result(&responder, Err(QueryError::Execution(err.to_string())));
                None
            }
        }
    }

    /// Under the lock: read up to `share` more series as this task's database and
    /// caller. A range read hands back the chunks it copied, for [`Self::decode`],
    /// stopping once they reach `copy_bytes`; it fails here, before decoding,
    /// when the samples it is sure to load would overrun its sample budget.
    fn copy(
        &mut self,
        ctx: &Context,
        share: usize,
        copy_bytes: usize,
    ) -> QueryResult<Option<CopiedSnapshots>> {
        if self.cursor.is_done() {
            return Ok(None);
        }
        let _ = set_current_db(ctx, self.cursor.db());
        let Self { cursor, state, .. } = self;
        // Kept apart from the read's own errors, which become `Execution`: this
        // one must reach the caller as the budget error `decode` would raise.
        let mut over_budget = None;
        let copied = with_fanout_user(ctx, self.caller_user.as_deref(), |ctx| {
            Ok(match state {
                ReadState::Instant {
                    start,
                    end,
                    samples,
                    ..
                } => {
                    let (start, end) = (*start, *end);
                    // Straight into the answer, sized for every planned series on
                    // the first hold: a batch `Vec` per hold, and the answer
                    // doubling across holds, left macOS's allocator holding
                    // 300 MB it no longer used under four concurrent queries.
                    if samples.capacity() == 0 {
                        samples.reserve_exact(cursor.remaining());
                    }
                    cursor.next_batch(ctx, share, &mut |_, batch| {
                        samples.extend(batch.iter().filter_map(|(s, _)| {
                            let sample = s.last_sample_in_range(start, end)?;
                            Some(InstantSample {
                                timestamp_ms: sample.timestamp,
                                value: sample.value,
                                labels: EvalLabels::interned(&s.labels),
                            })
                        }));
                        Ok(())
                    })?;
                    None
                }
                ReadState::Range {
                    start, end, budget, ..
                } => {
                    let (start, end) = (*start, *end);
                    let limit = BatchLimit::weighed(share, copy_bytes, |s: &TimeSeries| {
                        s.range_copy_bytes(start, end)
                    });
                    cursor.next_batch_within(ctx, &limit, &mut |_, batch| {
                        let mut copied = Vec::with_capacity(batch.len());
                        let mut pending = 0usize;
                        for (s, _) in batch.iter() {
                            let snapshot = s.snapshot_range(start, end);
                            if snapshot.is_empty() {
                                continue;
                            }
                            pending = pending.saturating_add(snapshot.min_samples());
                            if let Err(err) = budget.check_ahead(pending) {
                                over_budget = Some(err);
                                break;
                            }
                            copied.push((EvalLabels::interned(&s.labels), snapshot));
                        }
                        Ok(copied)
                    })?
                }
                ReadState::Profile {
                    cap,
                    matched,
                    builder,
                    overflow,
                } => {
                    let cap = *cap;
                    cursor.next_batch(ctx, share, &mut |_, batch| {
                        for (s, _) in batch.iter() {
                            *matched += 1;
                            if cap > 0 && *matched > cap {
                                *overflow = true;
                                break;
                            }
                            // The builder copies what it keeps.
                            builder.add_series(s.labels.iter().map(|l| (l.name, l.value)));
                        }
                        Ok(())
                    })?;
                    if *overflow {
                        // Past the cap the profile is not worth finishing.
                        cursor.finish();
                    }
                    None
                }
            })
        })
        .map_err(|err| QueryError::Execution(err.to_string()))?;
        match over_budget {
            Some(err) => Err(err),
            None => Ok(copied),
        }
    }

    /// Off the lock: decode one batch's chunks on the executor's own pool,
    /// charging this read's sample budget and applying the per-series limit.
    fn decode(&mut self, batch: CopiedSnapshots) {
        let ReadState::Range { budget, ranges, .. } = &mut self.state else {
            return;
        };
        match decode_snapshot_batch(batch, &self.options, budget) {
            Ok(decoded) => ranges.extend(decoded),
            Err(err) => self.fail(err),
        }
    }

    /// Every series is read: apply the series-count limit and answer.
    fn finish(&mut self) {
        let result = match std::mem::replace(
            &mut self.state,
            ReadState::Instant {
                start: 0,
                end: 0,
                aggregation: false,
                samples: Vec::new(),
            },
        ) {
            ReadState::Instant {
                samples,
                aggregation,
                ..
            } => {
                // Bound what the query returns, not what the selector matched: a
                // series whose latest sample predates the lookback window
                // contributes nothing, so it must not count against the limit.
                // The cluster paths filter first for the same reason. No
                // per-series point limit: an instant read yields one sample.
                validate_max_series_(samples.len(), self.options.max_series).map(|()| {
                    if aggregation {
                        SelectorOutput::Aggregation(AggregationOutcome::Raw(samples))
                    } else {
                        SelectorOutput::Vector(samples)
                    }
                })
            }
            ReadState::Range { ranges, grid, .. } => {
                // The series the query returns, as above. This is also the path
                // a single-node grid query takes, so it must agree with the
                // pushed-down one about which queries `max_series` rejects.
                validate_max_series_(ranges.len(), self.options.max_series).map(|()| {
                    if grid {
                        SelectorOutput::Grid(GridOutcome::Raw(ranges))
                    } else {
                        SelectorOutput::Matrix(ranges)
                    }
                })
            }
            ReadState::Profile {
                builder, overflow, ..
            } => Ok(SelectorOutput::Profile(
                (!overflow).then(|| builder.finish()),
            )),
        };
        deliver_task_result(&self.responder, result);
    }

    fn fail(&mut self, err: QueryError) {
        self.failed = true;
        deliver_task_result(&self.responder, Err(err));
    }
}

/// Decode one batch of a local range read on the executor's own pool. Exact
/// accounting against the query's sample budget, and an early stop: once it is
/// spent the remaining series are not decoded, so the overshoot is at most one
/// series per pool thread.
fn decode_snapshot_batch(
    series: CopiedSnapshots,
    options: &QueryOptions,
    budget: &SampleBudget,
) -> QueryResult<Vec<RangeSample<EvalLabels>>> {
    let max_points = options.max_points_per_series;
    series
        .into_par_on(&MATERIALIZE_POOL)
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
        .collect::<Vec<_>>()
}

#[cfg(test)]
mod selector_batch_executor_tests {
    use super::{MATERIALIZE_POOL, collect_batch, selector_fanout_failure, wait_for_result};
    use crate::common::threads::{IntoParRayon, ThreadRole, set_thread_role};
    use crate::fanout::FanoutError;
    use crate::promql::QueryError;
    use orx_parallel::Par;
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
    #[expect(
        clippy::disallowed_methods,
        reason = "the scenario needs every worker of a raw pool parked at once"
    )]
    fn parked_pool_never_starves_a_processor_with_its_own_pool() {
        let callers = rayon_core::ThreadPoolBuilder::new()
            .num_threads(2)
            .start_handler(|_| set_thread_role(ThreadRole::BlockingPool))
            .build()
            .unwrap();
        let (task_tx, task_rx) = mpsc::channel::<mpsc::SyncSender<usize>>();
        std::thread::spawn(move || {
            for responder in task_rx {
                let sum: usize = (0..10_000usize).into_par_on(&MATERIALIZE_POOL).sum();
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
