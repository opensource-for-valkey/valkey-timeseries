//! Whole-codec property test for [`ChimpCompressor`] / [`ChimpDecompressor`].
//!
//! Seeded, deterministic random streams mix value shapes (random bit patterns, decimals,
//! offset counters, repeats, special values, nudged powers of ten, quantized walks, ulp
//! steps, bit flips) with timestamp delta-of-deltas drawn across every tier. Each stream is
//! checked for:
//!
//! - (a) bit-exact decode;
//! - (b) serialize → deserialize → keep appending at random split points: same bytes and an
//!   equal compressor as the uninterrupted encode, and the reloaded stream decodes;
//! - (c) corrupted streams and serialized blobs: decoding returns `Ok`/`Err` without
//!   panicking and terminates, with the true sample count and with `u64::MAX`.
//!
//! The test also asserts that the generator reached every ELF case, Chimp flag (including
//! the extended `01` window) and timestamp tier, so a generator change cannot silently stop
//! covering one.

use super::encoder::{LEADING_ZERO_LOG, LZ_BUCKETS, LeadingZeroLog};
use super::{ChimpCompressor, ChimpDecompressor};
use crate::common::{Sample, Timestamp};
use std::panic::{AssertUnwindSafe, catch_unwind};

/// 600 seeds of up to `MAX_LEN` samples each (about 300 k samples): ~2 s in a debug build.
const SEEDS: u64 = 600;
const MAX_LEN: u64 = 1000;
/// Corrupted variants decoded per stream.
const CORRUPTIONS: usize = 20;

/// SplitMix64: a small local PRNG, so the test needs no dependency and is reproducible.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed ^ 0x5DEE_CE66_D1CE_4E5B)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `0..n` (`n > 0`; the modulo bias is irrelevant here).
    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }

    /// Uniform in `lo..=hi`.
    fn range(&mut self, lo: i64, hi: i64) -> i64 {
        let span = (hi as i128 - lo as i128 + 1) as u128;
        (lo as i128 + (self.next_u64() as u128 % span) as i128) as i64
    }

    fn coin(&mut self) -> bool {
        self.next_u64() & 1 == 1
    }

    fn sign(&mut self) -> f64 {
        if self.coin() { -1.0 } else { 1.0 }
    }
}

/// Timestamp tiers by encoded width: `0`, then the 7/9/12/16/32/64-bit payload tiers.
const TS_TIER_BITS: [u32; 7] = [1, 9, 12, 16, 21, 38, 70];

/// The tier the encoder uses for a delta-of-delta (zig-zag width, as `write_timestamp`).
fn ts_tier(dod: i64) -> usize {
    if dod == 0 {
        return 0;
    }
    let zz = ((dod << 1) ^ (dod >> 63)) as u64;
    match 64 - zz.leading_zeros() {
        1..=7 => 1,
        8..=9 => 2,
        10..=12 => 3,
        13..=16 => 4,
        17..=32 => 5,
        _ => 6,
    }
}

/// A delta-of-delta: mostly zero (with a per-stream probability), else a magnitude in one
/// tier's range with a random sign, or any `i64`.
fn draw_dod(rng: &mut Rng, zero_per_16: u64) -> i64 {
    if rng.below(16) < zero_per_16 {
        return 0;
    }
    let (lo, hi): (i64, i64) = match rng.below(9) {
        0 => (1, 3),
        1 => (1, 63),
        2 => (64, 255),
        3 => (256, 2047),
        4 => (2048, 32767),
        5 => (32768, 1 << 31),
        6 => ((1 << 31) + 1, 1 << 62),
        7 => (1, 1_000),
        _ => return rng.next_u64() as i64,
    };
    let m = rng.range(lo, hi);
    if rng.coin() { -m } else { m }
}

fn first_timestamp(rng: &mut Rng) -> Timestamp {
    match rng.below(6) {
        0 => 0,
        1 => 1_700_000_000_000 + rng.range(0, 1 << 30),
        2 => i64::MIN + rng.range(0, 1000),
        3 => i64::MAX - rng.range(0, 1000),
        4 => -rng.range(0, 1 << 40),
        _ => rng.next_u64() as i64,
    }
}

fn pow10(k: u32) -> f64 {
    10f64.powi(k as i32)
}

/// A value shape, held for a segment of the stream.
enum Shape {
    RandomBits,
    /// `m / 10^alpha` with a fresh `m` each sample.
    Decimal {
        alpha: u32,
        digits: u32,
    },
    /// An offset counter: integer or fractional steps above `2^20..=2^52`.
    Counter {
        cur: f64,
        step: f64,
    },
    Repeat,
    Special,
    NudgedPow10,
    /// `m / 10^alpha` or `m * q` for a random walk `m`.
    QuantizedWalk {
        m: i64,
        alpha: u32,
        q: Option<f64>,
    },
    NearUlp,
    BitFlip,
}

fn new_shape(rng: &mut Rng) -> Shape {
    match rng.below(9) {
        0 => Shape::RandomBits,
        1 => Shape::Decimal {
            alpha: rng.below(18) as u32,
            digits: 1 + rng.below(17) as u32,
        },
        2 => {
            let offset = (1u64 << (20 + rng.below(33))) as f64;
            let step = match rng.below(4) {
                0 => (1 + rng.below(1000)) as f64,
                1 => [0.5, 0.25, 0.125, 0.1, 0.01, 0.001][rng.below(6) as usize],
                2 => (1 + rng.below(10_000)) as f64 / pow10(1 + rng.below(4) as u32),
                _ => 1.0,
            };
            Shape::Counter {
                cur: offset + rng.below(1000) as f64,
                step,
            }
        }
        3 => Shape::Repeat,
        4 => Shape::Special,
        5 => Shape::NudgedPow10,
        6 => Shape::QuantizedWalk {
            m: rng.range(-1_000_000, 1_000_000),
            alpha: rng.below(7) as u32,
            q: rng
                .coin()
                .then(|| [0.5, 0.25, 0.05, 0.1, 0.2][rng.below(5) as usize]),
        },
        7 => Shape::NearUlp,
        _ => Shape::BitFlip,
    }
}

fn special(rng: &mut Rng) -> f64 {
    let sign = rng.next_u64() & (1 << 63);
    match rng.below(10) {
        0 => 0.0,
        1 => -0.0,
        2 => f64::INFINITY,
        3 => f64::NEG_INFINITY,
        // NaN with a random nonzero payload (quiet or signalling) and sign.
        4 => f64::from_bits(sign | 0x7ff0_0000_0000_0000 | (1 + rng.below((1 << 52) - 1))),
        5 => f64::NAN,
        // Subnormal (or zero, when the mantissa draw is zero).
        6 => f64::from_bits(sign | (rng.next_u64() & 0x000f_ffff_ffff_ffff)),
        7 => f64::MIN_POSITIVE * rng.sign(),
        8 => [f64::MAX, f64::MIN, f64::EPSILON][rng.below(3) as usize],
        _ => f64::from_bits(sign | rng.below(1000)),
    }
}

fn next_value(rng: &mut Rng, shape: &mut Shape, prev: f64) -> f64 {
    match shape {
        Shape::RandomBits => f64::from_bits(rng.next_u64()),
        Shape::Decimal { alpha, digits } => {
            let m = rng.below(10u64.pow(*digits)) as f64;
            rng.sign() * m / pow10(*alpha)
        }
        Shape::Counter { cur, step } => {
            *cur += match rng.below(8) {
                0 => 0.0,
                1 => 2.0 * *step,
                _ => *step,
            };
            *cur
        }
        Shape::Repeat => prev,
        Shape::Special => special(rng),
        Shape::NudgedPow10 => {
            let k = rng.below(45) as i32 - 22;
            let bits = 10f64
                .powi(k)
                .to_bits()
                .wrapping_add(rng.range(-4, 4) as u64);
            rng.sign() * f64::from_bits(bits)
        }
        Shape::QuantizedWalk { m, alpha, q } => {
            *m += if rng.below(16) == 0 {
                rng.range(-100_000, 100_000)
            } else {
                rng.range(-5, 5)
            };
            match q {
                Some(q) => *m as f64 * *q,
                None => *m as f64 / pow10(*alpha),
            }
        }
        Shape::NearUlp => f64::from_bits(prev.to_bits().wrapping_add(rng.range(-1, 1) as u64)),
        Shape::BitFlip => f64::from_bits(prev.to_bits() ^ (1 << rng.below(64))),
    }
}

fn generate(rng: &mut Rng) -> Vec<Sample> {
    let len = match rng.below(10) {
        0 => rng.below(4),
        _ => rng.below(MAX_LEN + 1),
    } as usize;
    let zero_per_16 = [2, 8, 15][rng.below(3) as usize];
    let mut out = Vec::with_capacity(len);
    let mut ts = first_timestamp(rng);
    let mut delta: i64 = 0;
    let mut prev = 0.0;
    let mut shape = new_shape(rng);
    let mut left = 0;
    for i in 0..len {
        if left == 0 {
            shape = new_shape(rng);
            left = 1 + rng.below(64);
        }
        left -= 1;
        if i > 0 {
            delta = delta.wrapping_add(draw_dod(rng, zero_per_16));
            ts = ts.wrapping_add(delta);
        }
        let v = next_value(rng, &mut shape, prev);
        out.push(Sample::new(ts, v));
        prev = v;
    }
    out
}

#[derive(Debug, Default)]
struct Coverage {
    streams: u64,
    samples: u64,
    elf_repeat: u64,
    elf_case_0: u64,
    elf_case_10: u64,
    elf_case_11: u64,
    flag_00: u64,
    flag_10: u64,
    flag_01: u64,
    flag_01_extended: u64,
    flag_11: u64,
    ts_tier: [u64; 7],
    ts_tier_negative: [u64; 7],
    splits: u64,
    corruptions: u64,
    corrupt_ok: u64,
    corrupt_err: u64,
}

fn assert_decodes(bytes: &[u8], expected: &[Sample], ctx: &str) {
    let got = ChimpDecompressor::new(bytes, expected.len() as u64)
        .collect()
        .unwrap_or_else(|e| panic!("{ctx}: decode failed: {e}"));
    assert_eq!(got.len(), expected.len(), "{ctx}: sample count");
    for (i, (g, e)) in got.iter().zip(expected).enumerate() {
        assert_eq!(g.timestamp, e.timestamp, "{ctx}: timestamp {i}");
        assert_eq!(
            g.value.to_bits(),
            e.value.to_bits(),
            "{ctx}: value {i}: got {} expected {}",
            g.value,
            e.value
        );
    }
}

/// Encode with the leading-zero log on, counting the Chimp flags and ELF cases written.
fn encode_counting(samples: &[Sample], cov: &mut Coverage) -> ChimpCompressor {
    let fresh = || LeadingZeroLog {
        by_header: [[0; 64]; 3],
        padding_bits: [0; 3],
    };
    LEADING_ZERO_LOG.set(Some(Box::new(fresh())));
    let mut seen = fresh();
    let mut c = ChimpCompressor::new();
    for s in samples {
        c.add(*s).unwrap();
        // An extended `01` is a `01` whose window is wider than the top bucket. At most one
        // log entry changes per sample: find it and recover the window's leading zeros.
        LEADING_ZERO_LOG.with_borrow(|log| {
            let log = log.as_ref().unwrap();
            if let Some(nlz) = (0..64).find(|&n| log.by_header[1][n] != seen.by_header[1][n]) {
                let pad = log.padding_bits[1] - seen.padding_bits[1];
                let lz = nlz as i64 - pad as i64;
                if lz > i64::from(LZ_BUCKETS[LZ_BUCKETS.len() - 1]) {
                    cov.flag_01_extended += 1;
                }
                seen.by_header[1][nlz] = log.by_header[1][nlz];
                seen.padding_bits[1] = log.padding_bits[1];
            }
        });
    }
    let log = LEADING_ZERO_LOG.take().unwrap();
    let total = |h: usize| log.by_header[h].iter().map(|&n| u64::from(n)).sum::<u64>();
    let written = total(0) + total(1) + total(2);
    cov.flag_10 += total(0);
    cov.flag_01 += total(1);
    cov.flag_11 += total(2);
    // Every value after the first writes one Chimp flag; the unlogged ones are `00`.
    cov.flag_00 += c.count().saturating_sub(1) - written;

    let s = &c.stats;
    cov.elf_repeat += u64::from(s.repeats);
    cov.elf_case_0 += u64::from(s.same_alpha);
    cov.elf_case_10 += u64::from(s.raw);
    cov.elf_case_11 += u64::from(s.new_alpha);
    c
}

/// (b): the same samples through a compressor that is serialized and reloaded at `splits`.
fn check_resume(samples: &[Sample], full: &ChimpCompressor, splits: &[usize], ctx: &str) {
    let reload = |c: &ChimpCompressor, at: usize| {
        let mut buf = Vec::new();
        c.serialize(&mut buf);
        let r = ChimpCompressor::deserialize(&buf)
            .unwrap_or_else(|e| panic!("{ctx}: deserialize at {at}: {e:?}"));
        assert_eq!(&r, c, "{ctx}: reloaded compressor differs at {at}");
        assert_decodes(r.bytes(), &samples[..at], &format!("{ctx} split {at}"));
        r
    };
    let mut c = ChimpCompressor::new();
    let mut next = 0;
    for (i, s) in samples.iter().enumerate() {
        if splits.get(next) == Some(&i) {
            c = reload(&c, i);
            next += 1;
        }
        c.add(*s).unwrap();
    }
    if splits.get(next) == Some(&samples.len()) {
        c = reload(&c, samples.len());
    }
    assert_eq!(c.bytes(), full.bytes(), "{ctx}: resumed bytes differ");
    assert_eq!(&c, full, "{ctx}: resumed compressor differs");
}

/// Decode `bytes` claiming `count` samples. Each decoded sample consumes at least one bit,
/// so a decoder still yielding samples past `bits + 1` of them would never stop.
fn decode_bounded(bytes: &[u8], count: u64) -> Result<(), String> {
    let limit = bytes.len() as u64 * 8 + 1;
    let mut d = ChimpDecompressor::new(bytes, count);
    for _ in 0..=limit {
        match d.next_sample() {
            Ok(Some(_)) => {}
            Ok(None) => return Ok(()),
            Err(e) => return Err(e.to_string()),
        }
    }
    panic!("decoder did not terminate within {limit} samples")
}

fn corrupt(rng: &mut Rng, data: &mut Vec<u8>) -> String {
    let flip = |rng: &mut Rng, data: &mut Vec<u8>| {
        let bit = rng.below(data.len() as u64 * 8);
        data[(bit / 8) as usize] ^= 1 << (bit % 8);
        bit
    };
    match rng.below(5) {
        0 if !data.is_empty() => format!("flip bit {}", flip(rng, data)),
        1 if !data.is_empty() => {
            let n = 2 + rng.below(7);
            let bits: Vec<_> = (0..n).map(|_| flip(rng, data)).collect();
            format!("flip bits {bits:?}")
        }
        2 if !data.is_empty() => {
            let i = rng.below(data.len() as u64) as usize;
            data[i] = [0x00, 0xff][rng.below(2) as usize];
            format!("overwrite byte {i} with {:#04x}", data[i])
        }
        3 if !data.is_empty() => {
            let len = rng.below(data.len() as u64) as usize;
            data.truncate(len);
            let bit = (!data.is_empty()).then(|| flip(rng, data));
            format!("truncate to {len}, flip {bit:?}")
        }
        _ => {
            let len = rng.below(data.len() as u64 + 1) as usize;
            data.truncate(len);
            format!("truncate to {len}")
        }
    }
}

/// (c): corrupted bit streams and serialized blobs decode to `Ok`/`Err` without panicking.
fn check_corruptions(rng: &mut Rng, full: &ChimpCompressor, n: u64, cov: &mut Coverage, ctx: &str) {
    let mut blob = Vec::new();
    full.serialize(&mut blob);
    for k in 0..CORRUPTIONS {
        // One in four corrupts the serialized compressor instead of the bare bit stream.
        let serialized = k % 4 == 3;
        let mut data = if serialized {
            blob.clone()
        } else {
            full.bytes().to_vec()
        };
        let what = corrupt(rng, &mut data);
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            if serialized {
                match ChimpCompressor::deserialize(&data) {
                    Ok(c) => vec![decode_bounded(c.bytes(), c.count())],
                    Err(_) => vec![Err("deserialize".into())],
                }
            } else {
                vec![decode_bounded(&data, n), decode_bounded(&data, u64::MAX)]
            }
        }));
        let results = outcome.unwrap_or_else(|_| {
            panic!("{ctx}: corruption {k} ({what}, serialized: {serialized}) panicked")
        });
        for r in results {
            cov.corruptions += 1;
            match r {
                Ok(()) => cov.corrupt_ok += 1,
                Err(_) => cov.corrupt_err += 1,
            }
        }
    }
}

#[test]
fn chimp_codec_properties() {
    let started = std::time::Instant::now();
    let mut cov = Coverage::default();
    for seed in 0..SEEDS {
        let ctx = format!("seed {seed}");
        let mut rng = Rng::new(seed);
        let samples = generate(&mut rng);
        let n = samples.len();
        cov.streams += 1;
        cov.samples += n as u64;
        for w in samples.windows(3) {
            let dod = (w[2].timestamp.wrapping_sub(w[1].timestamp))
                .wrapping_sub(w[1].timestamp.wrapping_sub(w[0].timestamp));
            cov.ts_tier[ts_tier(dod)] += 1;
            cov.ts_tier_negative[ts_tier(dod)] += u64::from(dod < 0);
        }
        if n >= 2 {
            // The second sample's delta-of-delta is its delta (the encoder starts at 0).
            let dod = samples[1].timestamp.wrapping_sub(samples[0].timestamp);
            cov.ts_tier[ts_tier(dod)] += 1;
            cov.ts_tier_negative[ts_tier(dod)] += u64::from(dod < 0);
        }

        // (a) bit-exact decode.
        let full = encode_counting(&samples, &mut cov);
        assert_eq!(full.count(), n as u64, "{ctx}");
        assert_decodes(full.bytes(), &samples, &ctx);

        // (b) persist and resume at up to three split points, ends included.
        let mut splits: Vec<usize> = (0..1 + rng.below(3))
            .map(|_| rng.below(n as u64 + 1) as usize)
            .collect();
        splits.sort_unstable();
        splits.dedup();
        cov.splits += splits.len() as u64;
        check_resume(&samples, &full, &splits, &ctx);

        // (c) corruptions.
        check_corruptions(&mut rng, &full, n as u64, &mut cov, &ctx);
    }
    let elapsed = started.elapsed();
    println!("chimp_codec_properties: {SEEDS} seeds in {elapsed:?}");
    println!("timestamp tier bits: {TS_TIER_BITS:?}");
    println!("{cov:#?}");

    let c = &cov;
    for (name, n) in [
        ("ELF repeat", c.elf_repeat),
        ("ELF case 0", c.elf_case_0),
        ("ELF case 10", c.elf_case_10),
        ("ELF case 11", c.elf_case_11),
        ("Chimp flag 00", c.flag_00),
        ("Chimp flag 10", c.flag_10),
        ("Chimp flag 01", c.flag_01),
        ("Chimp extended 01", c.flag_01_extended),
        ("Chimp flag 11", c.flag_11),
        ("corrupt Ok", c.corrupt_ok),
        ("corrupt Err", c.corrupt_err),
    ] {
        assert!(n > 0, "generator never reached {name}: {cov:#?}");
    }
    for tier in 0..7 {
        assert!(c.ts_tier[tier] > 0, "timestamp tier {tier} never hit");
        if tier > 0 {
            assert!(
                c.ts_tier_negative[tier] > 0 && c.ts_tier_negative[tier] < c.ts_tier[tier],
                "timestamp tier {tier} not hit with both signs"
            );
        }
    }
}
