//! Evaluate transient sparse checkpoints before committing to a production cache.
//! `cargo run --release --features enable-system-alloc,test-utils --bin gorilla_seek_report`
//! Columns separate one-time index construction, steady-state reads and memory.

use std::hint::black_box;
use std::io::{BufWriter, Write};
use std::mem::size_of;
use std::time::Instant;

use valkey_timeseries::common::Sample;
use valkey_timeseries::series::chunks::{
    ChunkOps, GorillaCheckpoint, GorillaChunk, GorillaEncoder, GorillaIterator,
};
use valkey_timeseries::tests::generators::{
    DatasetKey, TimestampModel, ValueWorkload, dataset_seed, generate_dataset,
};

struct Index<'a> {
    encoder: &'a GorillaEncoder,
    checkpoints: Vec<GorillaCheckpoint>,
}

impl<'a> Index<'a> {
    fn new(encoder: &'a GorillaEncoder, stride: usize) -> Self {
        let mut checkpoints = Vec::with_capacity(encoder.num_samples / stride);
        let mut iter = encoder.iter();
        let mut n = 0;
        while let Some(sample) = iter.next() {
            sample.expect("index decode");
            n += 1;
            if n % stride == 0 && n < encoder.num_samples {
                checkpoints.push(iter.checkpoint());
            }
        }
        Self {
            encoder,
            checkpoints,
        }
    }

    fn range(&self, start: i64, end: i64) -> impl Iterator<Item = Sample> + '_ {
        // A checkpoint points *after* its timestamp: choosing an equal timestamp
        // would lose the first matching sample. Keep the comparison strict.
        let i = self.checkpoints.partition_point(|c| c.timestamp() < start);
        let iter = if i == 0 {
            self.encoder.iter()
        } else {
            GorillaIterator::resume(self.encoder, self.checkpoints[i - 1]).expect("resume")
        };
        iter.map(|s| s.expect("indexed decode"))
            .skip_while(move |s| s.timestamp < start)
            .take_while(move |s| s.timestamp <= end)
    }

    fn memory_bytes(&self) -> usize {
        size_of::<Self>() + self.checkpoints.capacity() * size_of::<GorillaCheckpoint>()
    }
}

fn median<T>(iterations: usize, mut op: impl FnMut() -> T) -> u128 {
    for _ in 0..3 {
        black_box(op());
    }
    let mut times = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let start = Instant::now();
        let result = op();
        times.push(start.elapsed().as_nanos());
        black_box(result);
    }
    times.sort_unstable();
    times[times.len() / 2]
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut out = BufWriter::new(std::io::stdout().lock());
    writeln!(
        out,
        "workload,chunk_budget,samples,payload_bytes,stride,index_bytes,index_build_ns,scan_tail100_ns,index_tail100_ns,scan_point_ns,index_point_ns"
    )?;
    for workload in [
        ValueWorkload::Constant,
        ValueWorkload::Drift,
        ValueWorkload::Periodic,
        ValueWorkload::Counter,
    ] {
        let key = DatasetKey::new(workload, TimestampModel::Regular);
        let samples = generate_dataset(key, 300_000, dataset_seed(key));
        for budget in [4096, 65536] {
            let mut chunk = GorillaChunk::with_max_size(budget);
            let mut encoder = GorillaEncoder::with_soft_cap(budget);
            for sample in &samples {
                if chunk.is_full() {
                    break;
                }
                chunk.add_sample(sample).expect("fill chunk");
                encoder.add_sample(sample).expect("fill encoder");
            }
            let n = chunk.len();
            let tail = samples[n.saturating_sub(100)].timestamp;
            let last = samples[n - 1].timestamp;
            let point = samples[n * 9 / 10].timestamp;
            for stride in [64, 256, 1024] {
                let index = Index::new(&encoder, stride);
                // Check exact hits at checkpoint boundaries, the final point,
                // windows between samples, and a reversed/empty window.
                for i in (0..n).step_by(stride) {
                    let ts = samples[i].timestamp;
                    let actual: Vec<_> = index.range(ts, ts).collect();
                    assert_eq!(actual, samples[i..=i]);
                    assert!(index.range(ts + 1, ts + 999).next().is_none());
                }
                assert_eq!(
                    index.range(tail, last).collect::<Vec<_>>(),
                    samples[n.saturating_sub(100)..n]
                );
                assert!(index.range(last + 1, last).next().is_none());
                let build = median(20, || Index::new(&encoder, stride));
                let scan_tail = median(100, || {
                    chunk.range_iter(tail, last).for_each(|s| {
                        black_box((s.timestamp, s.value));
                    })
                });
                let indexed_tail = median(100, || {
                    index.range(tail, last).for_each(|s| {
                        black_box((s.timestamp, s.value));
                    })
                });
                let scan_point = median(100, || {
                    chunk.range_iter(point, point).for_each(|s| {
                        black_box((s.timestamp, s.value));
                    })
                });
                let indexed_point = median(100, || {
                    index.range(point, point).for_each(|s| {
                        black_box((s.timestamp, s.value));
                    })
                });
                writeln!(
                    out,
                    "{},{budget},{n},{},{stride},{},{build},{scan_tail},{indexed_tail},{scan_point},{indexed_point}",
                    workload.id(),
                    chunk.size(),
                    index.memory_bytes()
                )?;
            }
        }
    }
    Ok(())
}
