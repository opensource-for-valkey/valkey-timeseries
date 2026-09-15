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
use crate::scenario::{Case, CaseKind, KeyDistribution, RangeWindow, Scenario, SeriesSpec};

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
fn writer_order(owned: &[usize], samples: usize) -> impl Iterator<Item = (usize, usize)> + '_ {
    (0..samples).flat_map(move |j| owned.iter().map(move |&i| (i, j)))
}

fn add_stream(fixture: &Fixture, owned: &[usize]) -> Stream {
    let samples = fixture.manifest.shape.samples_per_series;
    let mut frames = Vec::with_capacity(owned.len() * samples);
    for (i, j) in writer_order(owned, samples) {
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

fn madd_stream(fixture: &Fixture, owned: &[usize], batch: usize) -> Stream {
    let samples = fixture.manifest.shape.samples_per_series;
    let mut frames = Vec::with_capacity((owned.len() * samples).div_ceil(batch));
    let order: Vec<(usize, usize)> = writer_order(owned, samples).collect();
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
    madd_stream(fixture, &all, PRELOAD_BATCH)
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
        let (from_arg, to_arg) = match window {
            RangeWindow::Full {} => ("-".to_string(), "+".to_string()),
            _ => (from.to_string(), to.to_string()),
        };
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
        CaseKind::Madd { batch } => {
            let workload = (0..connections)
                .map(|c| madd_stream(fixture, &owned_series(series, connections, c), *batch))
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
