use crate::common::threads::spawn;
use crate::common::time::current_time_millis;
use crate::common::{Sample, Timestamp};
use crate::fanout::get_cluster_command_timeout;
use crate::fanout::{is_clustered, FanoutCommand};
use crate::labels::filters::SeriesSelector;
use crate::labels::Label;
use crate::promql::engine::{
    QueryFanoutCommand, QueryRangeFanoutCommand,
};
use crate::promql::generated::Label as ProtoLabel;
use crate::promql::{
    InstantSample, Labels, QueryError, QueryOptions, QueryResult, QueryValue, RangeSample,
};
use crate::series::index::series_by_selectors;
use orx_parallel::IterIntoParIter;
use orx_parallel::ParIter;
use promql_parser::label::Matchers;
use std::ops::Deref;
use std::sync::mpsc;
use std::time::Duration;
use valkey_module::{Context, MODULE_CONTEXT};

struct InstantQueryCommand {
    matchers: Matchers,
    timestamp: Timestamp,
    options: QueryOptions,
}

struct RangeQueryCommand {
    matchers: Matchers,
    start_timestamp: Timestamp,
    end_timestamp: Timestamp,
    options: QueryOptions,
}

enum QueryCommand {
    Instant(InstantQueryCommand),
    Range(RangeQueryCommand),
}

/// A single batched request for a `BatchWorker`.
struct BatchRequest {
    pub item: QueryCommand,
    /// responder receives the processed result (Ok) or the error (Err)
    pub responder: mpsc::SyncSender<QueryResult<QueryValue>>,
}


/// A worker responsible for executing PromQL query tasks as part of a keyspace batch operation.
///
/// The `QueryWorker` optimizes latency in the PromQL evaluator (especially in cluster mode) by:
///
/// - Serializing access to the Valkey keyspace so the worker thread can safely hold the GIL while
///   processing requests.
/// - Collecting incoming query requests into a batch and processing the batch in a single lock
///   acquisition to reduce locking overhead.
///
/// # Example
/// ```rust
/// // Create a worker handle and perform queries via the provided API.
/// let query_worker = QueryWorker::new();
/// let _ = query_worker.query("up".parse().unwrap(), 1_600_000_000_000i64);
/// ```
///
/// The concrete worker implementation is internal; callers use `QueryWorker::new()` and the
/// `query` / `query_range` methods exposed on the handle.
pub struct QueryWorker {
    // Runner is parameterized by the concrete worker implementation type.
    runner: mpsc::Sender<BatchRequest>,
}

impl QueryWorker {
    pub fn new() -> Self {
        let runner = spawn_worker();
        Self { runner }
    }

    pub fn query(
        &self,
        matchers: Matchers,
        timestamp: Timestamp,
        options: QueryOptions,
    ) -> QueryResult<QueryValue> {
        let command = QueryCommand::Instant(InstantQueryCommand {
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
        let command = QueryCommand::Range(RangeQueryCommand {
            matchers,
            start_timestamp: start,
            end_timestamp: end,
            options,
        });
        self.send_request(command)
    }

    fn send_request(&self, command: QueryCommand) -> QueryResult<QueryValue> {
        let (responder_tx, responder_rx) = mpsc::sync_channel(1);
        let request = BatchRequest {
            item: command,
            responder: responder_tx,
        };
        // Forward request to the runner's sender
        self.runner.send(request).unwrap_or_else(|e| {
            eprintln!("Failed to send query request: {}", e);
        });

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
}

fn process_command(ctx: &Context, item: QueryCommand) -> QueryResult<QueryValue> {
    if is_clustered(ctx) {
        process_cluster(ctx, item)
    } else {
        process_local(ctx, item)
    }
}

fn process_local(ctx: &Context, item: QueryCommand) -> QueryResult<QueryValue> {
    match item {
        QueryCommand::Instant(iqc) => {
            let timestamp = iqc.timestamp;
            let selector: SeriesSelector = SeriesSelector::from(iqc.matchers);
            query_instant_local(ctx, selector, timestamp, iqc.options).map(QueryValue::Vector)
        }
        QueryCommand::Range(rc) => {
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

fn process_cluster(ctx: &Context, item: QueryCommand) -> QueryResult<QueryValue> {
    match item {
        QueryCommand::Instant(iqc) => {
            let timeout = calculate_timeout(&iqc.options);
            let cmd = QueryFanoutCommand::new(iqc.matchers, iqc.timestamp, timeout);

            match cmd.exec_sync(ctx) {
                Ok(resp) => {
                    // resp.samples: Vec<proto::InstantSample>
                    let mut samples: Vec<InstantSample> = Vec::with_capacity(resp.samples.len());
                    for s in resp.samples {
                        let labels = convert_labels(s.labels);
                        samples.push(InstantSample {
                            labels,
                            timestamp_ms: s.timestamp,
                            value: s.value,
                        });
                    }
                    Ok(QueryValue::Vector(samples))
                }
                Err(e) => Err(QueryError::Execution(e.to_string())),
            }
        }
        QueryCommand::Range(rc) => {
            let timeout = calculate_timeout(&rc.options);
            let cmd = QueryRangeFanoutCommand::new(
                rc.matchers,
                rc.start_timestamp,
                rc.end_timestamp,
                timeout,
            );
            match cmd.exec_sync(ctx) {
                Ok(resp) => {
                    let ranges = resp
                        .series
                        .into_iter()
                        .map(|rs| {
                            let samples: Vec<Sample> = rs
                                .samples
                                .into_iter()
                                .map(|s| Sample::new(s.timestamp, s.value))
                                .collect();
                            let labels = convert_labels(rs.labels);
                            RangeSample { labels, samples }
                        })
                        .collect();
                    Ok(QueryValue::Matrix(ranges))
                }
                Err(e) => Err(QueryError::Execution(e.to_string())),
            }
        }
    }
}

fn convert_labels(labels: Vec<ProtoLabel>) -> Labels {
    let labels = labels
        .into_iter()
        .map(|x| Label {
            name: x.name,
            value: x.value,
        })
        .collect();
    Labels::new(labels)
}


/// Spawn the worker thread and return a channel Sender that accepts `BatchRequest`s.
fn spawn_worker() -> mpsc::Sender<BatchRequest>
{
    let (tx, rx) = mpsc::channel::<BatchRequest>();

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
                let res = process_command(&ctx, req.item);
                req.responder.send(res).unwrap_or_else(|e| {
                    eprintln!("Failed to send batch response: {}", e);
                });
            }
        }
    });

    tx
}


pub(super) fn query_instant_local(
    ctx: &Context,
    selector: SeriesSelector,
    timestamp: Timestamp,
    options: QueryOptions,
) -> QueryResult<Vec<InstantSample>> {
    if let Some(d) = options.deadline
        && current_time_millis() > d
    {
        return Err(QueryError::Execution("query timed out".to_string()));
    }
    let series = series_by_selectors(ctx, &[selector], None)
        .map_err(|e| QueryError::Execution(e.to_string()))?;

    let samples = series
        .iter()
        .map(|(s, _)| s.deref())
        .iter_into_par()
        .filter_map(|s| {
            // todo: log error if any
            let Ok(Some(sample)) = s.get_sample(timestamp) else {
                return None;
            };

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

pub(super) fn query_range_local(
    ctx: &Context,
    selector: SeriesSelector,
    start_time: i64,
    end_time: i64,
    options: QueryOptions,
) -> QueryResult<Vec<RangeSample>> {
    let max_series = if options.max_series == 0 {
        usize::MAX
    } else {
        options.max_series
    };

    let series = series_by_selectors(ctx, &[selector], None)
        .map_err(|e| QueryError::Execution(e.to_string()))?;
    let ranges = series
        .iter()
        .take(max_series)
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

    Ok(ranges)
}
