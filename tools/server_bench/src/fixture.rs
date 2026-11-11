//! Loading and verifying fixtures exported by `tools/benchmark_dataset.rs`.
//!
//! The driver trusts nothing about a fixture directory until every file
//! hashes to what `fixture.json` claims and the manifest matches what the
//! scenario asked for. A fixture is data shared by both engines, so a stale
//! or hand-edited one would silently invalidate every pair in a run.

use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read};
use std::path::Path;

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::scenario::FixtureSpec;

/// Must match `FORMAT_VERSION` in `tools/benchmark_dataset.rs`.
pub const FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureManifest {
    pub format_version: u32,
    pub tool: String,
    pub generator: GeneratorInfo,
    pub shape: Shape,
    pub keys: KeyRule,
    pub labels: LabelRules,
    pub files: Files,
    pub sha256: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GeneratorInfo {
    pub dataset_key: String,
    pub workload: String,
    pub timestamp_model: String,
    pub dataset_seed: u64,
    pub seed_overridden: bool,
    pub series_seed_rule: String,
    pub start_ts: i64,
    pub interval_ms: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Shape {
    pub series: usize,
    pub samples_per_series: usize,
    pub total_samples: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct KeyRule {
    pub prefix: String,
    pub index_width: usize,
    pub rule: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LabelRules {
    pub value_rule: String,
    pub definitions: Vec<LabelDef>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LabelDef {
    pub name: String,
    pub cardinality: usize,
    pub value_len: usize,
    /// `expected_matches[k]` = number of series whose value is `v<k>`.
    pub expected_matches: Vec<usize>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Files {
    #[serde(rename = "series.tsv")]
    pub series: FileDigest,
    #[serde(rename = "samples.tsv")]
    pub samples: FileDigest,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FileDigest {
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone)]
pub struct SeriesDef {
    pub key: String,
    /// `(name, value)` pairs in file order.
    pub labels: Vec<(String, String)>,
    /// Samples in ascending timestamp order; the value is kept as the exact
    /// text from the file so it reaches the wire unchanged.
    pub samples: Vec<SampleText>,
}

#[derive(Debug, Clone)]
pub struct SampleText {
    pub timestamp: i64,
    pub value_text: String,
    pub value: f64,
}

#[derive(Debug)]
pub struct Fixture {
    pub manifest: FixtureManifest,
    pub series: Vec<SeriesDef>,
}

impl Fixture {
    /// Read the manifest and verify file digests without loading samples.
    pub fn verify(dir: &Path) -> Result<FixtureManifest> {
        let manifest_path = dir.join("fixture.json");
        let text = fs::read_to_string(&manifest_path)
            .with_context(|| format!("reading {}", manifest_path.display()))?;
        let manifest: FixtureManifest = serde_json::from_str(&text)
            .with_context(|| format!("parsing {}", manifest_path.display()))?;
        ensure!(
            manifest.format_version == FORMAT_VERSION,
            "fixture format_version {} is not supported (driver understands {FORMAT_VERSION})",
            manifest.format_version
        );
        ensure!(
            manifest.tool == "benchmark_dataset",
            "fixture was not written by benchmark_dataset (tool = {:?})",
            manifest.tool
        );
        ensure!(
            manifest.shape.total_samples
                == manifest.shape.series * manifest.shape.samples_per_series,
            "fixture shape is inconsistent"
        );

        check_file(dir, "series.tsv", &manifest.files.series)?;
        check_file(dir, "samples.tsv", &manifest.files.samples)?;

        let combined = combined_digest(&manifest.files);
        ensure!(
            combined == manifest.sha256,
            "fixture combined digest mismatch: manifest says {}, files hash to {combined}",
            manifest.sha256
        );
        Ok(manifest)
    }

    /// Verify, then load every series and sample.
    pub fn load(dir: &Path) -> Result<Self> {
        let manifest = Self::verify(dir)?;
        let mut series = read_series(dir, &manifest)?;
        read_samples(dir, &manifest, &mut series)?;
        Ok(Self { manifest, series })
    }

    /// The scenario names a fixture shape; the directory must hold exactly that.
    pub fn matches_spec(manifest: &FixtureManifest, spec: &FixtureSpec) -> Result<()> {
        let mut problems = Vec::new();
        let mut want = |what: &str, got: String, expected: String| {
            if got != expected {
                problems.push(format!(
                    "{what}: fixture has {got}, scenario wants {expected}"
                ));
            }
        };
        want(
            "series",
            manifest.shape.series.to_string(),
            spec.series.to_string(),
        );
        want(
            "samples_per_series",
            manifest.shape.samples_per_series.to_string(),
            spec.samples_per_series.to_string(),
        );
        want(
            "workload",
            manifest.generator.workload.clone(),
            spec.workload.clone(),
        );
        want(
            "timestamp_model",
            manifest.generator.timestamp_model.clone(),
            spec.timestamp_model.clone(),
        );
        want(
            "interval_ms",
            manifest.generator.interval_ms.to_string(),
            spec.interval_ms.to_string(),
        );
        want(
            "key_prefix",
            manifest.keys.prefix.clone(),
            spec.key_prefix.clone(),
        );
        let cards: Vec<usize> = manifest
            .labels
            .definitions
            .iter()
            .map(|l| l.cardinality)
            .collect();
        want(
            "label_cardinality",
            format!("{cards:?}"),
            format!("{:?}", spec.label_cardinality),
        );
        if let Some(l) = manifest.labels.definitions.first() {
            want(
                "label_value_len",
                l.value_len.to_string(),
                spec.label_value_len.to_string(),
            );
        }
        if problems.is_empty() {
            Ok(())
        } else {
            bail!(
                "fixture does not match the scenario:\n  {}",
                problems.join("\n  ")
            )
        }
    }
}

fn combined_digest(files: &Files) -> String {
    // Mirrors `write_manifest` in tools/benchmark_dataset.rs exactly.
    let mut h = Sha256::new();
    h.update(b"series.tsv\n");
    h.update(files.series.sha256.as_bytes());
    h.update(b"\nsamples.tsv\n");
    h.update(files.samples.sha256.as_bytes());
    h.update(b"\n");
    hex(&h.finalize())
}

pub fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

pub fn sha256_file(path: &Path) -> Result<(u64, String)> {
    let mut f = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut h = Sha256::new();
    let mut buf = vec![0u8; 1 << 16];
    let mut total = 0u64;
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
        total += n as u64;
    }
    Ok((total, hex(&h.finalize())))
}

fn check_file(dir: &Path, name: &str, want: &FileDigest) -> Result<()> {
    let path = dir.join(name);
    let (bytes, sha) = sha256_file(&path)?;
    ensure!(
        bytes == want.bytes,
        "{name}: {bytes} bytes on disk, manifest says {}",
        want.bytes
    );
    ensure!(
        sha == want.sha256,
        "{name}: sha256 {sha} on disk, manifest says {}",
        want.sha256
    );
    Ok(())
}

fn read_series(dir: &Path, manifest: &FixtureManifest) -> Result<Vec<SeriesDef>> {
    let path = dir.join("series.tsv");
    let reader = BufReader::new(File::open(&path)?);
    let mut out = Vec::with_capacity(manifest.shape.series);
    for (lineno, line) in reader.lines().enumerate() {
        let line = line?;
        let mut fields = line.split('\t');
        let index: usize = fields
            .next()
            .and_then(|s| s.parse().ok())
            .with_context(|| format!("series.tsv:{}: bad index", lineno + 1))?;
        ensure!(
            index == lineno,
            "series.tsv:{}: index {index} out of order",
            lineno + 1
        );
        let key = fields
            .next()
            .with_context(|| format!("series.tsv:{}: missing key", lineno + 1))?
            .to_string();
        let mut labels = Vec::new();
        for kv in fields {
            let (k, v) = kv
                .split_once('=')
                .with_context(|| format!("series.tsv:{}: bad label {kv:?}", lineno + 1))?;
            labels.push((k.to_string(), v.to_string()));
        }
        ensure!(
            labels.len() == manifest.labels.definitions.len(),
            "series.tsv:{}: {} labels, manifest declares {}",
            lineno + 1,
            labels.len(),
            manifest.labels.definitions.len()
        );
        out.push(SeriesDef {
            key,
            labels,
            samples: Vec::with_capacity(manifest.shape.samples_per_series),
        });
    }
    ensure!(
        out.len() == manifest.shape.series,
        "series.tsv holds {} series, manifest says {}",
        out.len(),
        manifest.shape.series
    );
    Ok(out)
}

fn read_samples(dir: &Path, manifest: &FixtureManifest, series: &mut [SeriesDef]) -> Result<()> {
    let path = dir.join("samples.tsv");
    let mut reader = BufReader::new(File::open(&path)?);
    let mut line = String::new();
    let mut lineno = 0usize;
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        lineno += 1;
        let l = line.trim_end_matches(['\n', '\r']);
        let mut fields = l.split('\t');
        let (Some(idx), Some(ts), Some(val), None) =
            (fields.next(), fields.next(), fields.next(), fields.next())
        else {
            bail!("samples.tsv:{lineno}: expected 3 tab-separated fields");
        };
        let idx: usize = idx
            .parse()
            .with_context(|| format!("samples.tsv:{lineno}: bad series index"))?;
        let timestamp: i64 = ts
            .parse()
            .with_context(|| format!("samples.tsv:{lineno}: bad timestamp"))?;
        let value: f64 = val
            .parse()
            .with_context(|| format!("samples.tsv:{lineno}: bad value"))?;
        let s = series
            .get_mut(idx)
            .with_context(|| format!("samples.tsv:{lineno}: series index {idx} out of range"))?;
        if let Some(prev) = s.samples.last() {
            ensure!(
                timestamp > prev.timestamp,
                "samples.tsv:{lineno}: series {idx} timestamp {timestamp} not after {}",
                prev.timestamp
            );
        }
        s.samples.push(SampleText {
            timestamp,
            value_text: val.to_string(),
            value,
        });
    }
    for (i, s) in series.iter().enumerate() {
        ensure!(
            s.samples.len() == manifest.shape.samples_per_series,
            "series {i} has {} samples, manifest says {}",
            s.samples.len(),
            manifest.shape.samples_per_series
        );
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::io::Write;
    use std::path::PathBuf;

    /// A tiny hand-built fixture in the exporter's format, so these tests do
    /// not need the module crate built.
    pub(crate) fn write_fixture(dir: &Path, series: usize, samples: usize) -> FixtureManifest {
        fs::create_dir_all(dir).unwrap();
        let mut s = Vec::new();
        for i in 0..series {
            writeln!(s, "{i}\tbench:{i}\tl0=v0000000\tl1=v{:07}", i % 10).unwrap();
        }
        let mut p = Vec::new();
        for i in 0..series {
            for j in 0..samples {
                writeln!(
                    p,
                    "{i}\t{}\t{}.{i}",
                    1_700_000_000_000i64 + j as i64 * 1000,
                    100 + j
                )
                .unwrap();
            }
        }
        fs::write(dir.join("series.tsv"), &s).unwrap();
        fs::write(dir.join("samples.tsv"), &p).unwrap();
        let files = Files {
            series: FileDigest {
                bytes: s.len() as u64,
                sha256: hex(&Sha256::digest(&s)),
            },
            samples: FileDigest {
                bytes: p.len() as u64,
                sha256: hex(&Sha256::digest(&p)),
            },
        };
        let sha256 = combined_digest(&files);
        let manifest = FixtureManifest {
            format_version: FORMAT_VERSION,
            tool: "benchmark_dataset".into(),
            generator: GeneratorInfo {
                dataset_key: "drift/regular".into(),
                workload: "drift".into(),
                timestamp_model: "regular".into(),
                dataset_seed: 1,
                seed_overridden: false,
                series_seed_rule: "test".into(),
                start_ts: 1_700_000_000_000,
                interval_ms: 1000,
            },
            shape: Shape {
                series,
                samples_per_series: samples,
                total_samples: series * samples,
            },
            keys: KeyRule {
                prefix: "bench".into(),
                index_width: 1,
                rule: "test".into(),
            },
            labels: LabelRules {
                value_rule: "test".into(),
                definitions: vec![
                    LabelDef {
                        name: "l0".into(),
                        cardinality: 1,
                        value_len: 8,
                        expected_matches: vec![series],
                    },
                    LabelDef {
                        name: "l1".into(),
                        cardinality: 10,
                        value_len: 8,
                        expected_matches: (0..10)
                            .map(|k| (series.saturating_sub(k)).div_ceil(10))
                            .collect(),
                    },
                ],
            },
            files,
            sha256,
        };
        fs::write(
            dir.join("fixture.json"),
            serde_json::to_string_pretty(&manifest).unwrap(),
        )
        .unwrap();
        manifest
    }

    fn tmp(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "server_bench-fixture-{name}-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn loads_a_consistent_fixture() {
        let dir = tmp("ok");
        write_fixture(&dir, 3, 4);
        let f = Fixture::load(&dir).unwrap();
        assert_eq!(f.series.len(), 3);
        assert_eq!(f.series[2].key, "bench:2");
        assert_eq!(
            f.series[2].labels[1],
            ("l1".to_string(), "v0000002".to_string())
        );
        assert_eq!(f.series[1].samples.len(), 4);
        assert_eq!(f.series[1].samples[3].value_text, "103.1");
        assert_eq!(f.series[1].samples[3].value, 103.1);
    }

    #[test]
    fn tampered_samples_are_refused() {
        let dir = tmp("tamper");
        write_fixture(&dir, 2, 2);
        let p = dir.join("samples.tsv");
        let mut text = fs::read_to_string(&p).unwrap();
        text = text.replacen("100.0", "100.5", 1);
        fs::write(&p, text).unwrap();
        let err = Fixture::verify(&dir).unwrap_err().to_string();
        assert!(err.contains("samples.tsv: sha256"), "{err}");
    }

    #[test]
    fn manifest_digest_mismatch_is_refused() {
        let dir = tmp("digest");
        let mut m = write_fixture(&dir, 2, 2);
        m.sha256 = "0".repeat(64);
        fs::write(dir.join("fixture.json"), serde_json::to_string(&m).unwrap()).unwrap();
        let err = Fixture::verify(&dir).unwrap_err().to_string();
        assert!(err.contains("combined digest mismatch"), "{err}");
    }

    #[test]
    fn spec_mismatch_lists_every_difference() {
        let dir = tmp("spec");
        let m = write_fixture(&dir, 2, 2);
        let spec: FixtureSpec = serde_json::from_value(serde_json::json!({
            "series": 3, "samples_per_series": 2, "workload": "noisy", "timestamp_model": "regular",
            "label_cardinality": [1, 10]
        }))
        .unwrap();
        let err = Fixture::matches_spec(&m, &spec).unwrap_err().to_string();
        assert!(
            err.contains("series: fixture has 2, scenario wants 3"),
            "{err}"
        );
        assert!(
            err.contains("workload: fixture has drift, scenario wants noisy"),
            "{err}"
        );
        assert!(!err.contains("samples_per_series"), "{err}");
    }
}
