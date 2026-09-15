//! Request traces: every command a case sends, encoded once, before timing.
//!
//! A case yields three frame streams:
//!
//! * `setup` — engine-specific `TS.CREATE`s (the encoding argument differs
//!   by product); hashed per engine.
//! * `preload` — untimed `TS.MADD` loading for read and memory cases;
//!   identical on both engines.
//! * `workload` — the timed stream, split per connection; identical on both
//!   engines. Write workloads are fixed-length traces; read workloads are a
//!   fixed cycle the trial loops over.
//!
//! "Both engines receive the same trace" is checked, not assumed: the run
//! manifest records the SHA-256 of each stream per engine and the report
//! refuses a pair whose workload digests differ. Every frame carries what
//! the reply must look like, so the executor validates instead of discarding.

use std::fmt::Write as _;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::fixture::{Fixture, hex};
use crate::scenario::{
    Aggregator, Case, CaseKind, KeyDistribution, RangeWindow, Reducer, Scenario, SeriesSpec,
};

/// Which product a setup stream targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Engine {
    Subject,
    Reference,
}

impl Engine {
    pub fn name(self) -> &'static str {
        match self {
            Self::Subject => "subject",
            Self::Reference => "reference",
        }
    }
}

/// What a reply must be for the request to count as successful.
#[derive(Debug, Clone, PartialEq)]
pub enum Expect {
    /// `+OK`.
    Ok,
    /// Exactly this integer (the timestamp echoed by `TS.ADD`).
    Int(i64),
    /// An array of exactly these integers (`TS.MADD`).
    Ints(Vec<i64>),
    /// `[timestamp, value]` with this timestamp; the value is compared as f64.
    Sample { timestamp: i64, value: f64 },
    /// An array of exactly `count` `[timestamp, value]` pairs whose timestamps
    /// fall within `[from, to]`.
    Samples { count: usize, from: i64, to: i64 },
    /// Aggregated buckets: exactly these `(timestamp, value)` pairs in this
    /// order (or reversed). `exact` buckets must match bit for bit; otherwise
    /// values are compared with a relative tolerance and every deviation is
    /// counted so summation-order differences stay visible.
    Buckets {
        buckets: Vec<(i64, f64)>,
        exact: bool,
        reverse: bool,
    },
    /// Exactly this set of keys, in any order (`TS.QUERYINDEX`).
    Keys(Vec<String>),
    /// One `[key, labels, [timestamp, value]]` entry per key, in any order,
    /// each carrying exactly that key's last sample (`TS.MGET`).
    LastSamples(Vec<(String, i64, f64)>),
    /// One entry per key, in any order, each with exactly `count` samples
    /// inside `[from, to]` (raw `TS.MRANGE`).
    MultiSeries(Vec<(String, usize, i64, i64)>),
    /// One entry per group name, in any order, each with exactly this many
    /// buckets (`TS.MRANGE ... GROUPBY ... REDUCE`). Values are not checked:
    /// reducer results depend on summation order across series.
    Groups(Vec<(String, usize)>),
}

#[derive(Debug, Clone)]
pub struct Frame {
    /// RESP2 multi-bulk encoding of the command. Valid under RESP3 too — the
    /// protocol only changes reply shapes, never request shapes.
    pub bytes: Vec<u8>,
    /// Consumed by the trial executor (plan step 2); built and tested now so
    /// the trace contract is complete.
    #[allow(dead_code)]
    pub expect: Expect,
}

impl Frame {
    pub fn new(args: &[&[u8]], expect: Expect) -> Self {
        Self {
            bytes: encode(args),
            expect,
        }
    }
}

/// RESP multi-bulk encoding.
pub fn encode(args: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::with_capacity(args.iter().map(|a| a.len() + 8).sum::<usize>() + 8);
    let _ = write!(Bytes(&mut out), "*{}\r\n", args.len());
    for a in args {
        let _ = write!(Bytes(&mut out), "${}\r\n", a.len());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
    out
}

struct Bytes<'a>(&'a mut Vec<u8>);

impl std::fmt::Write for Bytes<'_> {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        self.0.extend_from_slice(s.as_bytes());
        Ok(())
    }
}

/// One connection's share of a workload stream.
#[derive(Debug, Clone, Default)]
pub struct Stream {
    pub frames: Vec<Frame>,
}

impl Stream {
    pub fn bytes(&self) -> u64 {
        self.frames.iter().map(|f| f.bytes.len() as u64).sum()
    }
}

#[derive(Debug, Clone)]
pub struct CaseTrace {
    pub case_id: String,
    pub setup_subject: Stream,
    pub setup_reference: Stream,
    pub preload: Stream,
    /// One entry per connection.
    pub workload: Vec<Stream>,
    /// Samples the timed phase writes (0 for reads).
    pub written_samples: u64,
    /// Samples the preload phase writes.
    pub preloaded_samples: u64,
}

/// Digests written into the run manifest.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct CaseDigest {
    pub case_id: String,
    pub setup_subject_sha256: String,
    pub setup_reference_sha256: String,
    pub preload_sha256: String,
    /// Over all connections, in connection order.
    pub workload_sha256: String,
    pub setup_frames: usize,
    pub preload_frames: usize,
    pub workload_frames: usize,
    pub workload_bytes: u64,
    pub connections: usize,
    pub written_samples: u64,
    pub preloaded_samples: u64,
}

impl CaseTrace {
    pub fn digest(&self) -> CaseDigest {
        CaseDigest {
            case_id: self.case_id.clone(),
            setup_subject_sha256: stream_digest(std::slice::from_ref(&self.setup_subject)),
            setup_reference_sha256: stream_digest(std::slice::from_ref(&self.setup_reference)),
            preload_sha256: stream_digest(std::slice::from_ref(&self.preload)),
            workload_sha256: stream_digest(&self.workload),
            setup_frames: self.setup_subject.frames.len(),
            preload_frames: self.preload.frames.len(),
            workload_frames: self.workload.iter().map(|s| s.frames.len()).sum(),
            workload_bytes: self.workload.iter().map(Stream::bytes).sum(),
            connections: self.workload.len(),
            written_samples: self.written_samples,
            preloaded_samples: self.preloaded_samples,
        }
    }

    /// The setup stream for one engine (used by the executor, plan step 2).
    #[allow(dead_code)]
    pub fn setup(&self, engine: Engine) -> &Stream {
        match engine {
            Engine::Subject => &self.setup_subject,
            Engine::Reference => &self.setup_reference,
        }
    }
}

/// Length-prefixed so `["ab","c"]` and `["a","bc"]` never collide.
fn stream_digest(streams: &[Stream]) -> String {
    let mut h = Sha256::new();
    for (i, s) in streams.iter().enumerate() {
        h.update((i as u64).to_le_bytes());
        h.update((s.frames.len() as u64).to_le_bytes());
        for f in &s.frames {
            h.update((f.bytes.len() as u64).to_le_bytes());
            h.update(&f.bytes);
        }
    }
    hex(&h.finalize())
}

// -------- Builders --------

/// Deterministic per-request key selection for read cases. SplitMix64 with a
/// seed derived from the case id, so two runs of the same scenario replay the
/// same requests and so do the two engines within a run.
struct Rng(u64);

impl Rng {
    fn for_case(case_id: &str) -> Self {
        let mut z = 0x05EE_D0F5_A1E5_u64;
        for b in case_id.bytes() {
            z ^= b as u64;
            z = z.wrapping_mul(0x0000_0100_0000_01b3);
        }
        Self(z)
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn create_frame(
    key: &str,
    labels: &[(String, String)],
    spec: &SeriesSpec,
    engine: Engine,
) -> Frame {
    let chunk = spec.chunk_size.to_string();
    let encoding = match engine {
        Engine::Subject => spec.encoding.subject.as_arg(),
        Engine::Reference => spec.encoding.reference.as_arg(),
    };
    let mut args: Vec<&[u8]> = vec![
        b"TS.CREATE",
        key.as_bytes(),
        b"ENCODING",
        encoding.as_bytes(),
        b"CHUNK_SIZE",
        chunk.as_bytes(),
        b"DUPLICATE_POLICY",
        spec.duplicate_policy.as_arg().as_bytes(),
    ];
    if !labels.is_empty() {
        args.push(b"LABELS");
        for (k, v) in labels {
            args.push(k.as_bytes());
            args.push(v.as_bytes());
        }
    }
    Frame::new(&args, Expect::Ok)
}

fn setup_stream(fixture: &Fixture, spec: &SeriesSpec, engine: Engine) -> Stream {
    Stream {
        frames: fixture
            .series
            .iter()
            .map(|s| create_frame(&s.key, &s.labels, spec, engine))
            .collect(),
    }
}

/// Series indices owned by connection `conn` of `connections`: a round-robin
/// split, so every writer gets a disjoint, deterministic set.
fn owned_series(series: usize, connections: usize, conn: usize) -> Vec<usize> {
    (conn..series).step_by(connections).collect()
}

/// `(series, sample)` pairs in the order a writer emits them: timestamp-major
/// over its own series, so every per-series stream is monotonic and the
/// interleaving looks like concurrent ingestion rather than a series-by-series
/// bulk load.
fn writer_order(
    owned: &[usize],
    samples: usize,
    run: usize,
) -> impl Iterator<Item = (usize, usize)> + '_ {
    // Blocks of `run` consecutive samples per series, cycling over the writer's
    // series: `run == 1` is timestamp-major (per-tick fan-in), `run == batch`
    // makes each MADD carry one series' buffered samples. Per-series streams
    // stay monotonic either way.
    let run = run.max(1);
    (0..samples).step_by(run).flat_map(move |j| {
        owned
            .iter()
            .flat_map(move |&i| (j..(j + run).min(samples)).map(move |jj| (i, jj)))
    })
}

fn add_stream(fixture: &Fixture, owned: &[usize]) -> Stream {
    let samples = fixture.manifest.shape.samples_per_series;
    let mut frames = Vec::with_capacity(owned.len() * samples);
    for (i, j) in writer_order(owned, samples, 1) {
        let s = &fixture.series[i];
        let sample = &s.samples[j];
        let ts = sample.timestamp.to_string();
        frames.push(Frame::new(
            &[
                b"TS.ADD",
                s.key.as_bytes(),
                ts.as_bytes(),
                sample.value_text.as_bytes(),
            ],
            Expect::Int(sample.timestamp),
        ));
    }
    Stream { frames }
}

fn madd_stream(fixture: &Fixture, owned: &[usize], batch: usize, run: usize) -> Stream {
    let samples = fixture.manifest.shape.samples_per_series;
    let mut frames = Vec::with_capacity((owned.len() * samples).div_ceil(batch));
    let order: Vec<(usize, usize)> = writer_order(owned, samples, run).collect();
    for chunk in order.chunks(batch) {
        let mut owned_args: Vec<Vec<u8>> = Vec::with_capacity(chunk.len() * 3);
        let mut expect = Vec::with_capacity(chunk.len());
        for &(i, j) in chunk {
            let s = &fixture.series[i];
            let sample = &s.samples[j];
            owned_args.push(s.key.as_bytes().to_vec());
            owned_args.push(sample.timestamp.to_string().into_bytes());
            owned_args.push(sample.value_text.as_bytes().to_vec());
            expect.push(sample.timestamp);
        }
        let mut args: Vec<&[u8]> = Vec::with_capacity(owned_args.len() + 1);
        args.push(b"TS.MADD");
        args.extend(owned_args.iter().map(Vec::as_slice));
        frames.push(Frame::new(&args, Expect::Ints(expect)));
    }
    Stream { frames }
}

/// Preload for read/memory cases: one MADD stream over all series, batched
/// generously; this phase is untimed so its shape only needs to be identical
/// on both engines.
const PRELOAD_BATCH: usize = 128;

fn preload_stream(fixture: &Fixture) -> Stream {
    let all: Vec<usize> = (0..fixture.series.len()).collect();
    madd_stream(fixture, &all, PRELOAD_BATCH, 1)
}

fn pick_series(rng: &mut Rng, distribution: &KeyDistribution, series: usize) -> usize {
    match distribution {
        KeyDistribution::Uniform {} => rng.below(series),
        KeyDistribution::Hot {
            keys,
            share_percent,
        } => {
            if rng.below(100) < *share_percent as usize {
                rng.below(*keys)
            } else {
                rng.below(series)
            }
        }
    }
}

fn get_streams(
    fixture: &Fixture,
    case: &Case,
    distribution: &KeyDistribution,
    cycle: usize,
) -> Vec<Stream> {
    let connections = case.connections as usize;
    let mut rng = Rng::for_case(&case.id);
    let mut streams = vec![Stream::default(); connections];
    for n in 0..cycle {
        let i = pick_series(&mut rng, distribution, fixture.series.len());
        let s = &fixture.series[i];
        let last = s.samples.last().expect("series has samples");
        streams[n % connections].frames.push(Frame::new(
            &[b"TS.GET", s.key.as_bytes()],
            Expect::Sample {
                timestamp: last.timestamp,
                value: last.value,
            },
        ));
    }
    streams
}

/// `(from, to, expected count)` for a window over one series, using the
/// series' real timestamps so jittered and irregular fixtures get exact
/// expectations too.
fn window_bounds(
    samples: &[crate::fixture::SampleText],
    window: &RangeWindow,
) -> (i64, i64, usize) {
    let n = samples.len();
    match window {
        RangeWindow::Recent { points } => {
            let start = n - *points.min(&n);
            (
                samples[start].timestamp,
                samples[n - 1].timestamp,
                n - start,
            )
        }
        RangeWindow::Middle { percent } => {
            let span = (n * *percent as usize).div_ceil(100).clamp(1, n);
            let start = (n - span) / 2;
            let end = start + span - 1;
            (samples[start].timestamp, samples[end].timestamp, span)
        }
        RangeWindow::Full {} => (samples[0].timestamp, samples[n - 1].timestamp, n),
    }
}

fn range_streams(
    fixture: &Fixture,
    case: &Case,
    window: &RangeWindow,
    reverse: bool,
    cycle: usize,
) -> Vec<Stream> {
    let connections = case.connections as usize;
    let mut rng = Rng::for_case(&case.id);
    let mut streams = vec![Stream::default(); connections];
    let cmd: &[u8] = if reverse { b"TS.REVRANGE" } else { b"TS.RANGE" };
    for n in 0..cycle {
        let i = rng.below(fixture.series.len());
        let s = &fixture.series[i];
        let (from, to, count) = window_bounds(&s.samples, window);
        let (from_arg, to_arg) = window_args(window, from, to);
        streams[n % connections].frames.push(Frame::new(
            &[
                cmd,
                s.key.as_bytes(),
                from_arg.as_bytes(),
                to_arg.as_bytes(),
            ],
            Expect::Samples { count, from, to },
        ));
    }
    streams
}

fn window_args(window: &RangeWindow, from: i64, to: i64) -> (String, String) {
    match window {
        RangeWindow::Full {} => ("-".to_string(), "+".to_string()),
        _ => (from.to_string(), to.to_string()),
    }
}

/// Bucket width that yields about `buckets` buckets over `[from, to]`.
pub fn bucket_ms(from: i64, to: i64, buckets: usize) -> i64 {
    let span = (to - from + 1).max(1);
    (span as f64 / buckets as f64).ceil().max(1.0) as i64
}

/// The oracle for `ALIGN start AGGREGATION <agg> <bucket>` over one series'
/// samples within `[from, to]`: buckets start at `from`, empty buckets are
/// omitted, the bucket timestamp is its start. Sum and avg are accumulated in
/// timestamp order; an engine that sums differently shows up as a counted
/// deviation, never as a silent pass.
pub fn aggregate(
    samples: &[crate::fixture::SampleText],
    from: i64,
    to: i64,
    bucket: i64,
    agg: Aggregator,
) -> Vec<(i64, f64)> {
    let mut out: Vec<(i64, f64)> = Vec::new();
    let mut current: Option<(i64, f64, f64, f64, u64)> = None; // (start, min, max, sum, count)
    let flush = |c: Option<(i64, f64, f64, f64, u64)>, out: &mut Vec<(i64, f64)>| {
        if let Some((start, min, max, sum, count)) = c {
            let v = match agg {
                Aggregator::Min => min,
                Aggregator::Max => max,
                Aggregator::Count => count as f64,
                Aggregator::Sum => sum,
                Aggregator::Avg => sum / count as f64,
            };
            out.push((start, v));
        }
    };
    for s in samples {
        if s.timestamp < from || s.timestamp > to {
            continue;
        }
        let start = from + ((s.timestamp - from) / bucket) * bucket;
        match current {
            Some((cs, min, max, sum, count)) if cs == start => {
                current = Some((
                    cs,
                    min.min(s.value),
                    max.max(s.value),
                    sum + s.value,
                    count + 1,
                ));
            }
            other => {
                flush(other, &mut out);
                current = Some((start, s.value, s.value, s.value, 1));
            }
        }
    }
    flush(current, &mut out);
    out
}

fn aggregate_streams(
    fixture: &Fixture,
    case: &Case,
    window: &RangeWindow,
    aggregator: Aggregator,
    buckets: usize,
    reverse: bool,
    cycle: usize,
) -> Vec<Stream> {
    let connections = case.connections as usize;
    let mut rng = Rng::for_case(&case.id);
    let mut streams = vec![Stream::default(); connections];
    let cmd: &[u8] = if reverse { b"TS.REVRANGE" } else { b"TS.RANGE" };
    for n in 0..cycle {
        let i = rng.below(fixture.series.len());
        let s = &fixture.series[i];
        let (from, to, _) = window_bounds(&s.samples, window);
        // `ALIGN start` needs an explicit start timestamp on both products, so
        // aggregated windows never use the `-`/`+` shorthand.
        let (from_arg, to_arg) = (from.to_string(), to.to_string());
        let bucket = bucket_ms(from, to, buckets);
        let bucket_arg = bucket.to_string();
        let expected = aggregate(&s.samples, from, to, bucket, aggregator);
        streams[n % connections].frames.push(Frame::new(
            &[
                cmd,
                s.key.as_bytes(),
                from_arg.as_bytes(),
                to_arg.as_bytes(),
                b"ALIGN",
                b"start",
                b"AGGREGATION",
                aggregator.as_arg().as_bytes(),
                bucket_arg.as_bytes(),
            ],
            Expect::Buckets {
                buckets: expected,
                exact: aggregator.is_exact(),
                reverse,
            },
        ));
    }
    streams
}

/// Series indices carrying each value of label `l<label>`, indexed by value.
fn label_index(fixture: &Fixture, label: usize) -> Vec<(String, Vec<usize>)> {
    let name = &fixture.manifest.labels.definitions[label].name;
    let mut by_value: Vec<(String, Vec<usize>)> = Vec::new();
    for (i, s) in fixture.series.iter().enumerate() {
        let value = s
            .labels
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
            .expect("fixture series carries every label");
        match by_value.iter_mut().find(|(v, _)| *v == value) {
            Some((_, idx)) => idx.push(i),
            None => by_value.push((value, vec![i])),
        }
    }
    by_value
}

fn filter_arg(fixture: &Fixture, label: usize, value: &str) -> String {
    format!(
        "{}={}",
        fixture.manifest.labels.definitions[label].name, value
    )
}

fn queryindex_streams(fixture: &Fixture, case: &Case, label: usize, cycle: usize) -> Vec<Stream> {
    let connections = case.connections as usize;
    let index = label_index(fixture, label);
    let mut rng = Rng::for_case(&case.id);
    let mut streams = vec![Stream::default(); connections];
    for n in 0..cycle {
        let (value, matched) = &index[rng.below(index.len())];
        let filter = filter_arg(fixture, label, value);
        let mut keys: Vec<String> = matched
            .iter()
            .map(|&i| fixture.series[i].key.clone())
            .collect();
        keys.sort();
        streams[n % connections].frames.push(Frame::new(
            &[b"TS.QUERYINDEX", filter.as_bytes()],
            Expect::Keys(keys),
        ));
    }
    streams
}

fn mget_streams(fixture: &Fixture, case: &Case, label: usize, cycle: usize) -> Vec<Stream> {
    let connections = case.connections as usize;
    let index = label_index(fixture, label);
    let mut rng = Rng::for_case(&case.id);
    let mut streams = vec![Stream::default(); connections];
    for n in 0..cycle {
        let (value, matched) = &index[rng.below(index.len())];
        let filter = filter_arg(fixture, label, value);
        let mut expected: Vec<(String, i64, f64)> = matched
            .iter()
            .map(|&i| {
                let s = &fixture.series[i];
                let last = s.samples.last().expect("series has samples");
                (s.key.clone(), last.timestamp, last.value)
            })
            .collect();
        expected.sort_by(|a, b| a.0.cmp(&b.0));
        streams[n % connections].frames.push(Frame::new(
            &[b"TS.MGET", b"FILTER", filter.as_bytes()],
            Expect::LastSamples(expected),
        ));
    }
    streams
}

fn mrange_streams(
    fixture: &Fixture,
    case: &Case,
    label: usize,
    window: &RangeWindow,
    cycle: usize,
) -> Vec<Stream> {
    let connections = case.connections as usize;
    let index = label_index(fixture, label);
    let mut rng = Rng::for_case(&case.id);
    let mut streams = vec![Stream::default(); connections];
    for n in 0..cycle {
        let (value, matched) = &index[rng.below(index.len())];
        let filter = filter_arg(fixture, label, value);
        // The window is taken from the first matched series and applied to
        // all of them; each series' expected count is computed from its own
        // timestamps, so jittered fixtures still get exact expectations.
        let first = &fixture.series[matched[0]];
        let (from, to, _) = window_bounds(&first.samples, window);
        let (from_arg, to_arg) = window_args(window, from, to);
        let mut expected: Vec<(String, usize, i64, i64)> = matched
            .iter()
            .map(|&i| {
                let s = &fixture.series[i];
                let (f, t) = match window {
                    RangeWindow::Full {} => (i64::MIN, i64::MAX),
                    _ => (from, to),
                };
                let count = s
                    .samples
                    .iter()
                    .filter(|x| x.timestamp >= f && x.timestamp <= t)
                    .count();
                (s.key.clone(), count, f, t)
            })
            .collect();
        expected.sort_by(|a, b| a.0.cmp(&b.0));
        streams[n % connections].frames.push(Frame::new(
            &[
                b"TS.MRANGE",
                from_arg.as_bytes(),
                to_arg.as_bytes(),
                b"FILTER",
                filter.as_bytes(),
            ],
            Expect::MultiSeries(expected),
        ));
    }
    streams
}

#[allow(clippy::too_many_arguments)]
fn groupby_streams(
    fixture: &Fixture,
    case: &Case,
    label: usize,
    group_label: usize,
    window: &RangeWindow,
    aggregator: Aggregator,
    buckets: usize,
    reducer: Reducer,
    cycle: usize,
) -> Vec<Stream> {
    let connections = case.connections as usize;
    let index = label_index(fixture, label);
    let group_name = &fixture.manifest.labels.definitions[group_label].name;
    let mut rng = Rng::for_case(&case.id);
    let mut streams = vec![Stream::default(); connections];
    for n in 0..cycle {
        let (value, matched) = &index[rng.below(index.len())];
        let filter = filter_arg(fixture, label, value);
        let first = &fixture.series[matched[0]];
        let (from, to, _) = window_bounds(&first.samples, window);
        // Explicit bounds: `ALIGN start` rejects the `-` shorthand.
        let (from_arg, to_arg) = (from.to_string(), to.to_string());
        let bucket = bucket_ms(from, to, buckets);
        let bucket_arg = bucket.to_string();
        // Group -> union of its members' non-empty buckets.
        let mut groups: Vec<(String, Vec<i64>)> = Vec::new();
        for &i in matched {
            let s = &fixture.series[i];
            let gv = s
                .labels
                .iter()
                .find(|(k, _)| k == group_name)
                .map(|(_, v)| v.clone())
                .expect("fixture series carries every label");
            let starts: Vec<i64> = aggregate(&s.samples, from, to, bucket, aggregator)
                .into_iter()
                .map(|(ts, _)| ts)
                .collect();
            let name = format!("{group_name}={gv}");
            match groups.iter_mut().find(|(g, _)| *g == name) {
                Some((_, all)) => all.extend(starts),
                None => groups.push((name, starts)),
            }
        }
        let mut expected: Vec<(String, usize)> = groups
            .into_iter()
            .map(|(name, mut starts)| {
                starts.sort_unstable();
                starts.dedup();
                (name, starts.len())
            })
            .collect();
        expected.sort_by(|a, b| a.0.cmp(&b.0));
        streams[n % connections].frames.push(Frame::new(
            &[
                b"TS.MRANGE",
                from_arg.as_bytes(),
                to_arg.as_bytes(),
                b"ALIGN",
                b"start",
                b"AGGREGATION",
                aggregator.as_arg().as_bytes(),
                bucket_arg.as_bytes(),
                b"FILTER",
                filter.as_bytes(),
                b"GROUPBY",
                group_name.as_bytes(),
                b"REDUCE",
                reducer.as_arg().as_bytes(),
            ],
            Expect::Groups(expected),
        ));
    }
    streams
}

pub fn build_case(scenario: &Scenario, fixture: &Fixture, case: &Case) -> CaseTrace {
    let spec = &scenario.series;
    let series = fixture.series.len();
    let samples = fixture.manifest.shape.samples_per_series;
    let connections = case.connections as usize;
    let total = (series * samples) as u64;

    let setup_subject = setup_stream(fixture, spec, Engine::Subject);
    let setup_reference = setup_stream(fixture, spec, Engine::Reference);

    let (preload, workload, written, preloaded) = match &case.kind {
        CaseKind::Add {} => {
            let workload = (0..connections)
                .map(|c| add_stream(fixture, &owned_series(series, connections, c)))
                .collect();
            (Stream::default(), workload, total, 0)
        }
        CaseKind::Madd {
            batch,
            samples_per_series,
        } => {
            let workload = (0..connections)
                .map(|c| {
                    madd_stream(
                        fixture,
                        &owned_series(series, connections, c),
                        *batch,
                        *samples_per_series,
                    )
                })
                .collect();
            (Stream::default(), workload, total, 0)
        }
        CaseKind::Get { distribution } => (
            preload_stream(fixture),
            get_streams(fixture, case, distribution, scenario.read_cycle_requests),
            0,
            total,
        ),
        CaseKind::Range { window, reverse } => (
            preload_stream(fixture),
            range_streams(
                fixture,
                case,
                window,
                *reverse,
                scenario.read_cycle_requests,
            ),
            0,
            total,
        ),
        CaseKind::Memory {} => (preload_stream(fixture), Vec::new(), 0, total),
        CaseKind::Aggregate {
            window,
            aggregator,
            buckets,
            reverse,
        } => (
            preload_stream(fixture),
            aggregate_streams(
                fixture,
                case,
                window,
                *aggregator,
                *buckets,
                *reverse,
                scenario.read_cycle_requests,
            ),
            0,
            total,
        ),
        CaseKind::QueryIndex { label } => (
            preload_stream(fixture),
            queryindex_streams(fixture, case, *label, scenario.read_cycle_requests),
            0,
            total,
        ),
        CaseKind::Mget { label } => (
            preload_stream(fixture),
            mget_streams(fixture, case, *label, scenario.read_cycle_requests),
            0,
            total,
        ),
        CaseKind::Mrange { label, window } => (
            preload_stream(fixture),
            mrange_streams(fixture, case, *label, window, scenario.read_cycle_requests),
            0,
            total,
        ),
        CaseKind::GroupBy {
            label,
            group_label,
            window,
            aggregator,
            buckets,
            reducer,
        } => (
            preload_stream(fixture),
            groupby_streams(
                fixture,
                case,
                *label,
                *group_label,
                window,
                *aggregator,
                *buckets,
                *reducer,
                scenario.read_cycle_requests,
            ),
            0,
            total,
        ),
    };

    CaseTrace {
        case_id: case.id.clone(),
        setup_subject,
        setup_reference,
        preload,
        workload,
        written_samples: written,
        preloaded_samples: preloaded,
    }
}

pub fn build_all(scenario: &Scenario, fixture: &Fixture) -> Vec<CaseTrace> {
    scenario
        .cases
        .iter()
        .map(|c| build_case(scenario, fixture, c))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::tests::write_fixture;

    fn fixture(name: &str, series: usize, samples: usize) -> Fixture {
        let dir =
            std::env::temp_dir().join(format!("server_bench-trace-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        write_fixture(&dir, series, samples);
        Fixture::load(&dir).unwrap()
    }

    fn scenario(cases: serde_json::Value, series: usize, samples: usize) -> Scenario {
        let s: Scenario = serde_json::from_value(serde_json::json!({
            "schema_version": 1, "name": "t", "description": "",
            "fixture": {"series": series, "samples_per_series": samples, "workload": "drift",
                        "timestamp_model": "regular", "label_cardinality": [1, 10]},
            "series": {"chunk_size": 4096, "duplicate_policy": "BLOCK",
                       "encoding": {"subject": "chimp", "reference": "compressed"}},
            "trials": 1, "warmup_seconds": 0, "read_duration_seconds": 1,
            "read_cycle_requests": 50,
            "cases": cases
        }))
        .unwrap();
        s.validate().unwrap();
        s
    }

    #[test]
    fn resp_encoding_is_exact() {
        assert_eq!(encode(&[b"PING"]), b"*1\r\n$4\r\nPING\r\n");
        assert_eq!(
            encode(&[b"TS.ADD", b"k", b"1", b"2.5"]),
            b"*4\r\n$6\r\nTS.ADD\r\n$1\r\nk\r\n$1\r\n1\r\n$3\r\n2.5\r\n"
        );
    }

    #[test]
    fn setup_differs_only_by_encoding_argument() {
        let f = fixture("setup", 2, 3);
        let s = scenario(serde_json::json!([{"id": "a", "kind": "add"}]), 2, 3);
        let t = build_case(&s, &f, &s.cases[0]);
        let sub = String::from_utf8(t.setup_subject.frames[0].bytes.clone()).unwrap();
        let refr = String::from_utf8(t.setup_reference.frames[0].bytes.clone()).unwrap();
        assert!(sub.contains("$5\r\nCHIMP\r\n"), "{sub}");
        assert!(refr.contains("$10\r\nCOMPRESSED\r\n"), "{refr}");
        assert_eq!(
            sub.replace("$5\r\nCHIMP\r\n", "$10\r\nCOMPRESSED\r\n"),
            refr
        );
        assert!(sub.contains("LABELS\r\n$2\r\nl0\r\n$8\r\nv0000000\r\n$2\r\nl1\r\n"));
        assert!(
            sub.contains("CHUNK_SIZE\r\n$4\r\n4096\r\n$16\r\nDUPLICATE_POLICY\r\n$5\r\nBLOCK\r\n")
        );
    }

    #[test]
    fn writers_own_disjoint_series_with_monotonic_streams() {
        let f = fixture("add", 5, 4);
        let s = scenario(
            serde_json::json!([{"id": "a", "kind": "add", "connections": 2}]),
            5,
            4,
        );
        let t = build_case(&s, &f, &s.cases[0]);
        assert_eq!(t.workload.len(), 2);
        assert_eq!(t.workload[0].frames.len(), 3 * 4); // series 0,2,4
        assert_eq!(t.workload[1].frames.len(), 2 * 4); // series 1,3
        assert_eq!(t.written_samples, 20);
        assert_eq!(t.preloaded_samples, 0);

        // Every key appears in exactly one connection; timestamps per key ascend.
        let mut seen = std::collections::HashMap::<String, (usize, i64)>::new();
        for (c, stream) in t.workload.iter().enumerate() {
            for fr in &stream.frames {
                let text = String::from_utf8(fr.bytes.clone()).unwrap();
                let parts: Vec<&str> = text.split("\r\n").collect();
                let key = parts[4].to_string();
                let Expect::Int(ts) = fr.expect else { panic!() };
                let entry = seen.entry(key).or_insert((c, i64::MIN));
                assert_eq!(entry.0, c, "key written by two connections");
                assert!(ts > entry.1, "timestamps must ascend per key");
                entry.1 = ts;
            }
        }
        assert_eq!(seen.len(), 5);
    }

    #[test]
    fn madd_batches_carry_per_entry_expectations() {
        let f = fixture("madd", 3, 4);
        let s = scenario(
            serde_json::json!([{"id": "m", "kind": "madd", "batch": 5}]),
            3,
            4,
        );
        let t = build_case(&s, &f, &s.cases[0]);
        let frames = &t.workload[0].frames;
        assert_eq!(frames.len(), 3); // 12 samples / 5 = 5,5,2
        let Expect::Ints(ref e) = frames[2].expect else {
            panic!()
        };
        assert_eq!(e.len(), 2);
        assert!(frames[0].bytes.starts_with(b"*16\r\n$7\r\nTS.MADD\r\n"));
    }

    #[test]
    fn madd_runs_group_consecutive_samples_per_series() {
        let f = fixture("maddrun", 3, 8);
        let s = scenario(
            serde_json::json!([{"id": "m", "kind": "madd", "batch": 8, "samples_per_series": 4}]),
            3,
            8,
        );
        let t = build_case(&s, &f, &s.cases[0]);
        // 3 series x 8 samples = 24 samples, 8 per command = 3 commands.
        let frames = &t.workload[0].frames;
        assert_eq!(frames.len(), 3);
        let keys_of = |fr: &Frame| -> Vec<String> {
            String::from_utf8(fr.bytes.clone())
                .unwrap()
                .split("\r\n")
                .filter(|p| p.starts_with("bench:"))
                .map(str::to_string)
                .collect()
        };
        // First command: series 0 samples 0..4, then series 1 samples 0..4.
        let mut want: Vec<String> = vec!["bench:0".into(); 4];
        want.extend(vec!["bench:1".to_string(); 4]);
        assert_eq!(keys_of(&frames[0]), want);
        // Per-series streams stay monotonic across commands.
        let mut last = std::collections::HashMap::<String, i64>::new();
        for fr in frames {
            let Expect::Ints(ts) = &fr.expect else {
                panic!()
            };
            for (k, t) in keys_of(fr).into_iter().zip(ts) {
                let e = last.entry(k).or_insert(i64::MIN);
                assert!(*t > *e);
                *e = *t;
            }
        }
    }

    #[test]
    fn read_cycles_are_deterministic_and_split_round_robin() {
        let f = fixture("get", 4, 3);
        let s = scenario(
            serde_json::json!([{"id": "g", "kind": "get", "distribution": {"type": "uniform"}, "connections": 3}]),
            4,
            3,
        );
        let a = build_case(&s, &f, &s.cases[0]);
        let b = build_case(&s, &f, &s.cases[0]);
        assert_eq!(a.digest(), b.digest());
        assert_eq!(a.workload.iter().map(|w| w.frames.len()).sum::<usize>(), 50);
        assert_eq!(a.workload[0].frames.len(), 17);
        assert_eq!(a.workload[2].frames.len(), 16);
        assert_eq!(a.preloaded_samples, 12);
        assert_eq!(a.preload.frames.len(), 1);
        let Expect::Sample { timestamp, .. } = a.workload[0].frames[0].expect else {
            panic!()
        };
        assert_eq!(timestamp, 1_700_000_000_000 + 2000);
    }

    #[test]
    fn hot_distribution_favours_the_hot_keys() {
        let f = fixture("hot", 100, 2);
        let s = scenario(
            serde_json::json!([{"id": "h", "kind": "get", "distribution": {"type": "hot", "keys": 2, "share_percent": 90}}]),
            100,
            2,
        );
        let t = build_case(&s, &f, &s.cases[0]);
        let hot = t.workload[0]
            .frames
            .iter()
            .filter(|fr| {
                let text = String::from_utf8(fr.bytes.clone()).unwrap();
                text.contains("bench:0\r\n") || text.contains("bench:1\r\n")
            })
            .count();
        assert!(hot >= 40, "{hot} of 50 requests hit the hot keys");
    }

    #[test]
    fn range_windows_have_exact_expectations() {
        let samples: Vec<crate::fixture::SampleText> = (0..10)
            .map(|j| crate::fixture::SampleText {
                timestamp: 1000 + j * 7,
                value_text: "0".into(),
                value: 0.0,
            })
            .collect();
        assert_eq!(
            window_bounds(&samples, &RangeWindow::Recent { points: 3 }),
            (1049, 1063, 3)
        );
        assert_eq!(
            window_bounds(&samples, &RangeWindow::Full {}),
            (1000, 1063, 10)
        );
        assert_eq!(
            window_bounds(&samples, &RangeWindow::Middle { percent: 10 }),
            (1028, 1028, 1)
        );
        assert_eq!(
            window_bounds(&samples, &RangeWindow::Middle { percent: 50 }),
            (1014, 1042, 5)
        );
        assert_eq!(
            window_bounds(&samples, &RangeWindow::Middle { percent: 100 }),
            (1000, 1063, 10)
        );
    }

    #[test]
    fn full_range_uses_open_bounds() {
        let f = fixture("range", 2, 4);
        let s = scenario(
            serde_json::json!([{"id": "r", "kind": "range", "window": {"type": "full"}, "reverse": true}]),
            2,
            4,
        );
        let t = build_case(&s, &f, &s.cases[0]);
        let text = String::from_utf8(t.workload[0].frames[0].bytes.clone()).unwrap();
        assert!(text.starts_with("*4\r\n$11\r\nTS.REVRANGE\r\n"), "{text}");
        assert!(text.ends_with("$1\r\n-\r\n$1\r\n+\r\n"), "{text}");
    }

    #[test]
    fn aggregation_oracle_buckets_from_the_window_start() {
        let samples: Vec<crate::fixture::SampleText> = (0..10)
            .map(|j| crate::fixture::SampleText {
                timestamp: 1000 + j * 1000,
                value_text: String::new(),
                value: j as f64,
            })
            .collect();
        // 4 s buckets over [1000, 10000]: {0,1,2,3} {4,5,6,7} {8,9}
        let sum = aggregate(&samples, 1000, 10000, 4000, Aggregator::Sum);
        assert_eq!(sum, vec![(1000, 6.0), (5000, 22.0), (9000, 17.0)]);
        let count = aggregate(&samples, 1000, 10000, 4000, Aggregator::Count);
        assert_eq!(count, vec![(1000, 4.0), (5000, 4.0), (9000, 2.0)]);
        let avg = aggregate(&samples, 2000, 9000, 4000, Aggregator::Avg);
        assert_eq!(avg, vec![(2000, 2.5), (6000, 6.5)]);
        let max = aggregate(&samples, 1000, 10000, 100_000, Aggregator::Max);
        assert_eq!(max, vec![(1000, 9.0)]);
        assert_eq!(bucket_ms(1000, 10000, 3), 3001); // span of 9001 ms, inclusive
        assert_eq!(bucket_ms(0, 0, 10), 1);
    }

    #[test]
    fn label_queries_carry_exact_expected_sets() {
        // write_fixture: l0 = v0000000 on every series, l1 = v<i % 10>.
        let f = fixture("labels", 25, 3);
        let s = scenario(
            serde_json::json!([
                {"id": "qi", "kind": "queryindex", "label": 1},
                {"id": "mg", "kind": "mget", "label": 0},
                {"id": "mr", "kind": "mrange", "label": 1, "window": {"type": "recent", "points": 2}},
                {"id": "gb", "kind": "groupby", "label": 0, "group_label": 1, "window": {"type": "full"},
                 "aggregator": "avg", "buckets": 2, "reducer": "sum"}
            ]),
            25,
            3,
        );
        let qi = build_case(&s, &f, &s.cases[0]);
        let Expect::Keys(keys) = &qi.workload[0].frames[0].expect else {
            panic!()
        };
        // Values v0000000..v0000004 match 3 series each, v0000005..v0000009 match 2.
        assert!(keys.len() == 2 || keys.len() == 3, "{keys:?}");
        let text = String::from_utf8(qi.workload[0].frames[0].bytes.clone()).unwrap();
        assert!(
            text.starts_with("*2\r\n$13\r\nTS.QUERYINDEX\r\n$11\r\nl1=v"),
            "{text}"
        );

        let mg = build_case(&s, &f, &s.cases[1]);
        let Expect::LastSamples(entries) = &mg.workload[0].frames[0].expect else {
            panic!()
        };
        assert_eq!(entries.len(), 25);
        assert_eq!(entries[0].1, 1_700_000_000_000 + 2000);

        let mr = build_case(&s, &f, &s.cases[2]);
        let Expect::MultiSeries(entries) = &mr.workload[0].frames[0].expect else {
            panic!()
        };
        assert!(entries.iter().all(|(_, count, _, _)| *count == 2));

        let gb = build_case(&s, &f, &s.cases[3]);
        let text = String::from_utf8(gb.workload[0].frames[0].bytes.clone()).unwrap();
        assert!(
            text.starts_with("*14\r\n$9\r\nTS.MRANGE\r\n$13\r\n1700000000000\r\n$13\r\n1700000002000\r\n$5\r\nALIGN\r\n$5\r\nstart\r\n"),
            "aggregated windows must carry explicit bounds: {text}"
        );
        let Expect::Groups(groups) = &gb.workload[0].frames[0].expect else {
            panic!()
        };
        assert_eq!(groups.len(), 10, "one group per l1 value");
        assert!(groups.iter().all(|(_, n)| *n == 2), "{groups:?}");
        assert_eq!(groups[0].0, "l1=v0000000");
        let text = String::from_utf8(gb.workload[0].frames[0].bytes.clone()).unwrap();
        assert!(
            text.contains("GROUPBY\r\n$2\r\nl1\r\n$6\r\nREDUCE\r\n$3\r\nsum\r\n"),
            "{text}"
        );
    }

    #[test]
    fn digest_depends_on_every_frame() {
        let f = fixture("digest", 3, 3);
        let s = scenario(serde_json::json!([{"id": "a", "kind": "add"}]), 3, 3);
        let mut t = build_case(&s, &f, &s.cases[0]);
        let before = t.digest();
        t.workload[0].frames[4].bytes[10] ^= 1;
        let after = t.digest();
        assert_ne!(before.workload_sha256, after.workload_sha256);
        assert_eq!(before.setup_subject_sha256, after.setup_subject_sha256);
    }
}
