use super::generated::{CompressionType, Label as FanoutLabel, SampleData, SeriesRangeResponse};
use crate::common::logging::log_warning;
use crate::common::{MultiSample, Sample};
use crate::error_consts;
use crate::labels::Label;
use crate::series::TimeSeries;
use crate::series::chunks::{
    Chunk, ChunkEncoding, ChunkOps, TimeSeriesChunk, samples_to_chunk_lossless,
};
use crate::series::request_types::{MRangeSeriesResult, SeriesResultData};
use smallvec::SmallVec;
use valkey_module::{ValkeyError, ValkeyResult};

// for future compatibility
const VERSION: u32 = 1;

fn get_compression_type(chunk: &TimeSeriesChunk) -> CompressionType {
    chunk.get_encoding().into()
}

/// Serializes a TimeSeriesChunk for fanout.
pub fn serialize_chunk(chunk: TimeSeriesChunk) -> ValkeyResult<SampleData> {
    let compression = get_compression_type(&chunk);
    // estimate of the size needed
    let capacity = chunk.len() * chunk.bytes_per_sample();
    let mut data: Vec<u8> = Vec::with_capacity(capacity);
    chunk.serialize(&mut data);

    Ok(SampleData {
        version: VERSION,
        compression: compression.into(),
        data,
    })
}

/// Deserializes TimeSeriesChunk coming from a cluster node.
pub fn deserialize_chunk(chunk: &SampleData) -> ValkeyResult<TimeSeriesChunk> {
    let data = &chunk.data;

    let encoding = CompressionType::try_from(chunk.compression)
        .map_err(|_| ValkeyError::Str(error_consts::CHUNK_DECOMPRESSION))?;

    let deserialized = TimeSeriesChunk::deserialize(data.as_ref())
        .map_err(|_| ValkeyError::Str(error_consts::CHUNK_DECOMPRESSION))?; // ?? better error

    let encoding: ChunkEncoding = encoding.try_into()?;
    if encoding != deserialized.get_encoding() {
        log_warning("Invalid encoding type for deserialized time series chunk");
        return Err(ValkeyError::Str(error_consts::CHUNK_DECOMPRESSION));
    }
    Ok(deserialized)
}

impl TryFrom<SampleData> for TimeSeriesChunk {
    type Error = ValkeyError;

    fn try_from(value: SampleData) -> Result<Self, Self::Error> {
        deserialize_chunk(&value)
    }
}

impl TryFrom<&SampleData> for TimeSeriesChunk {
    type Error = ValkeyError;

    fn try_from(value: &SampleData) -> Result<Self, Self::Error> {
        deserialize_chunk(value)
    }
}

impl TryFrom<&SampleData> for TimeSeries {
    type Error = ValkeyError;

    fn try_from(value: &SampleData) -> Result<Self, Self::Error> {
        let chunk = deserialize_chunk(value)?;
        TimeSeries::from_chunk(chunk).map_err(|e| {
            ValkeyError::String(format!("Failed to create time series from chunk: {e}"))
        })
    }
}

impl TryFrom<SampleData> for TimeSeries {
    type Error = ValkeyError;

    fn try_from(value: SampleData) -> Result<Self, Self::Error> {
        (&value).try_into()
    }
}

/// Transpose multi-aggregation rows into one chunk per aggregation column:
/// column `i` becomes a chunk of `(bucket_ts, values[i])` pairs. An empty row
/// set yields zero columns (the column count is unknowable from empty rows, and
/// the coordinator never needs it for an empty series). Callers rely on the
/// resulting length: 1 => single/raw chunk, >= 2 => multi-aggregation.
fn serialize_rows(rows: &[MultiSample]) -> ValkeyResult<Vec<SampleData>> {
    let n = match rows.first() {
        Some(first) => first.values.len(),
        None => return Ok(Vec::new()),
    };

    let mut columns: Vec<Vec<Sample>> = vec![Vec::with_capacity(rows.len()); n];
    for row in rows {
        debug_assert_eq!(row.values.len(), n, "ragged multi-aggregation row");
        for (column, value) in columns.iter_mut().zip(row.values.iter()) {
            column.push(Sample {
                timestamp: row.timestamp,
                value: *value,
            });
        }
    }

    // Same size/CPU trade-off as any other fan-out payload, so the same policy
    // decides it. A bucket count below the threshold ships uncompressed rather
    // than paying a chunk header per aggregation column.
    columns
        .into_iter()
        .map(|samples| {
            serialize_chunk(samples_to_chunk_lossless(samples)).map_err(|e| {
                ValkeyError::String(format!("Failed to serialize aggregation column: {e}"))
            })
        })
        .collect()
}

/// Rebuild multi-aggregation rows from one chunk per aggregation column
/// (inverse of `serialize_rows`). Every column must decode to the same length
/// and timestamps; a mismatch means a corrupt/incompatible peer.
fn deserialize_rows(columns: &[SampleData]) -> ValkeyResult<Vec<MultiSample>> {
    let cols: Vec<Vec<Sample>> = columns
        .iter()
        .enumerate()
        .map(|(idx, c)| {
            deserialize_chunk(c)
                .map(|chunk| chunk.iter().collect::<Vec<_>>())
                .map_err(|e| ValkeyError::String(format!("{e} (column {idx})")))
        })
        .collect::<ValkeyResult<_>>()?;

    let n = cols.len();
    let len = cols[0].len();
    if let Some((idx, col)) = cols.iter().enumerate().find(|(_, col)| col.len() != len) {
        return Err(ValkeyError::String(format!(
            "TSDB: multi-aggregation columns have mismatched lengths: column {idx} has {} buckets, expected {len} ({n} columns)",
            col.len()
        )));
    }

    let mut rows = Vec::with_capacity(len);
    for i in 0..len {
        let timestamp = cols[0][i].timestamp;
        let mut values: SmallVec<[f64; 4]> = SmallVec::with_capacity(n);
        for (idx, col) in cols.iter().enumerate() {
            if col[i].timestamp != timestamp {
                return Err(ValkeyError::String(format!(
                    "TSDB: multi-aggregation columns have mismatched timestamps: column {idx} has {} at bucket {i}, expected {timestamp} ({n} columns, {len} buckets)",
                    col[i].timestamp
                )));
            }
            values.push(col[i].value);
        }
        rows.push(MultiSample { timestamp, values });
    }
    Ok(rows)
}

impl TryFrom<MRangeSeriesResult> for SeriesRangeResponse {
    type Error = ValkeyError;

    fn try_from(value: MRangeSeriesResult) -> Result<Self, Self::Error> {
        let group_label_value = value.group_label_value.unwrap_or_default();

        let columns = match value.data {
            SeriesResultData::Chunk(chunk) => {
                let data = serialize_chunk(chunk).map_err(|e| {
                    ValkeyError::String(format!("Failed to serialize sample data: {e}"))
                })?;
                vec![data]
            }
            SeriesResultData::Rows(rows) => serialize_rows(&rows)?,
        };

        let labels: Vec<FanoutLabel> = value.labels.into_iter().map(Into::into).collect();

        Ok(SeriesRangeResponse {
            key: value.key,
            group_label_value,
            labels,
            columns,
        })
    }
}

impl TryFrom<SeriesRangeResponse> for MRangeSeriesResult {
    type Error = ValkeyError;

    fn try_from(value: SeriesRangeResponse) -> Result<Self, Self::Error> {
        let key = value.key;
        let group_label_value = Some(value.group_label_value);
        // 1 column => raw/single-aggregation chunk; >= 2 => multi-aggregation
        // rows; 0 => an empty multi-aggregation series (no buckets). Decode
        // failures name the series so triage can locate the owning shard.
        let with_key = |e: ValkeyError| ValkeyError::String(format!("{e} (series '{key}')"));
        let data = match value.columns.len() {
            0 => SeriesResultData::Rows(Vec::new()),
            1 => SeriesResultData::Chunk(deserialize_chunk(&value.columns[0]).map_err(with_key)?),
            _ => SeriesResultData::Rows(deserialize_rows(&value.columns).map_err(with_key)?),
        };

        let labels: Vec<Label> = value.labels.into_iter().map(Into::into).collect();

        Ok(MRangeSeriesResult {
            key,
            group_label_value,
            labels,
            data,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::series::chunks::UncompressedChunk;

    fn multi_result(key: &str, rows: Vec<MultiSample>) -> MRangeSeriesResult {
        MRangeSeriesResult {
            key: key.into(),
            group_label_value: Some("g".into()),
            labels: Vec::new(),
            data: SeriesResultData::Rows(rows),
        }
    }

    fn row(ts: i64, values: &[f64]) -> MultiSample {
        MultiSample {
            timestamp: ts,
            values: values.iter().copied().collect(),
        }
    }

    fn roundtrip(result: MRangeSeriesResult) -> SeriesResultData {
        let wire: SeriesRangeResponse = result.try_into().expect("serialize");
        let back: MRangeSeriesResult = wire.try_into().expect("deserialize");
        back.data
    }

    #[test]
    fn multi_column_rows_roundtrip() {
        let rows = vec![
            row(0, &[1.0, 10.0, 100.0]),
            row(60, &[2.0, 20.0, 200.0]),
            row(120, &[3.0, 30.0, 300.0]),
        ];
        match roundtrip(multi_result("a", rows.clone())) {
            SeriesResultData::Rows(got) => assert_eq!(got, rows),
            SeriesResultData::Chunk(_) => panic!("expected rows"),
        }
    }

    #[test]
    fn multi_column_nan_values_roundtrip() {
        // EMPTY back-fill and all-NaN columns produce NaN bucket values.
        let rows = vec![row(0, &[f64::NAN, 1.0]), row(60, &[2.0, f64::NAN])];
        match roundtrip(multi_result("a", rows.clone())) {
            SeriesResultData::Rows(got) => {
                assert_eq!(got, rows, "NaN compares equal via MultiSample::eq");
                assert!(got[0].values[0].is_nan());
                assert!(got[1].values[1].is_nan());
            }
            SeriesResultData::Chunk(_) => panic!("expected rows"),
        }
    }

    /// The short NaN case above stays under `WIRE_COMPRESSION_MIN_SAMPLES` and
    /// so travels uncompressed. Past the threshold the columns are Chimp-coded,
    /// which is the encoding that actually has to carry a NaN payload through a
    /// XOR-of-previous-value scheme — cover that tier explicitly.
    #[test]
    fn multi_column_nan_values_roundtrip_compressed() {
        use crate::series::chunks::WIRE_COMPRESSION_MIN_SAMPLES;

        let len = WIRE_COMPRESSION_MIN_SAMPLES * 2;
        // Column 0 alternates NaN with real values, column 1 is entirely NaN
        // (an all-EMPTY bucket range), column 2 stays finite as a control.
        let rows: Vec<MultiSample> = (0..len)
            .map(|i| {
                row(
                    i as i64 * 60,
                    &[
                        if i % 2 == 0 { f64::NAN } else { i as f64 },
                        f64::NAN,
                        i as f64 * 1.5,
                    ],
                )
            })
            .collect();

        // Guard the premise: this must exercise the compressed tier.
        let wire: SeriesRangeResponse = multi_result("a", rows.clone())
            .try_into()
            .expect("serialize");
        for (idx, column) in wire.columns.iter().enumerate() {
            let encoding = deserialize_chunk(column).expect("decode").get_encoding();
            assert_eq!(
                encoding,
                ChunkEncoding::Chimp,
                "column {idx} should be compressed at {len} buckets, got {encoding:?}"
            );
        }

        let back: MRangeSeriesResult = wire.try_into().expect("deserialize");
        match back.data {
            SeriesResultData::Rows(got) => {
                assert_eq!(got, rows);
                for (i, r) in got.iter().enumerate() {
                    assert!(r.values[1].is_nan(), "all-NaN column lost NaN at {i}");
                    if i % 2 == 0 {
                        assert!(r.values[0].is_nan(), "interleaved NaN lost at {i}");
                    }
                }
            }
            SeriesResultData::Chunk(_) => panic!("expected rows"),
        }
    }

    /// Below the threshold the fan-out payload must stay uncompressed: a chunk
    /// header costs more than the encoder saves on a handful of buckets.
    #[test]
    fn short_columns_ship_uncompressed() {
        let rows = vec![row(0, &[1.0, 2.0]), row(60, &[3.0, 4.0])];
        let wire: SeriesRangeResponse = multi_result("a", rows).try_into().expect("serialize");
        for column in &wire.columns {
            assert_eq!(
                deserialize_chunk(column).expect("decode").get_encoding(),
                ChunkEncoding::Uncompressed,
            );
        }
    }

    #[test]
    fn empty_multi_series_roundtrips_as_empty_rows() {
        // Zero rows -> zero columns -> empty Rows (not a Chunk).
        match roundtrip(multi_result("a", Vec::new())) {
            SeriesResultData::Rows(got) => assert!(got.is_empty()),
            SeriesResultData::Chunk(_) => panic!("expected empty rows"),
        }
    }

    #[test]
    fn single_column_chunk_roundtrips_as_chunk() {
        let chunk = TimeSeriesChunk::Uncompressed(UncompressedChunk::from_vec(vec![
            Sample {
                timestamp: 0,
                value: 1.0,
            },
            Sample {
                timestamp: 5,
                value: 2.0,
            },
        ]));
        let result = MRangeSeriesResult {
            key: "a".into(),
            group_label_value: Some("g".into()),
            labels: Vec::new(),
            data: SeriesResultData::Chunk(chunk),
        };
        match roundtrip(result) {
            SeriesResultData::Chunk(got) => {
                let samples: Vec<Sample> = got.iter().collect();
                assert_eq!(samples.len(), 2);
                assert_eq!(
                    samples[0],
                    Sample {
                        timestamp: 0,
                        value: 1.0
                    }
                );
            }
            SeriesResultData::Rows(_) => panic!("expected chunk"),
        }
    }

    #[test]
    fn mismatched_column_lengths_rejected() {
        // Hand-build a response with two columns of unequal length.
        let short = serialize_chunk(TimeSeriesChunk::Uncompressed(UncompressedChunk::from_vec(
            vec![Sample {
                timestamp: 0,
                value: 1.0,
            }],
        )))
        .unwrap();
        let long = serialize_chunk(TimeSeriesChunk::Uncompressed(UncompressedChunk::from_vec(
            vec![
                Sample {
                    timestamp: 0,
                    value: 1.0,
                },
                Sample {
                    timestamp: 5,
                    value: 2.0,
                },
            ],
        )))
        .unwrap();
        let wire = SeriesRangeResponse {
            key: "a".into(),
            group_label_value: "g".into(),
            labels: Vec::new(),
            columns: vec![short.clone(), long.clone()],
        };
        let result: Result<MRangeSeriesResult, _> = wire.try_into();
        // The rejection names the offending column, both lengths, and the
        // series, so an incident log pinpoints the payload directly.
        let msg = result.unwrap_err().to_string();
        for needle in ["column 1", "2 buckets", "expected 1", "series 'a'"] {
            assert!(msg.contains(needle), "missing '{needle}' in: {msg}");
        }

        // Equal lengths but disagreeing timestamps are likewise rejected
        // with the bucket position and both timestamps.
        let shifted = serialize_chunk(TimeSeriesChunk::Uncompressed(UncompressedChunk::from_vec(
            vec![Sample {
                timestamp: 7,
                value: 1.0,
            }],
        )))
        .unwrap();
        let wire = SeriesRangeResponse {
            key: "b".into(),
            group_label_value: "g".into(),
            labels: Vec::new(),
            columns: vec![short, shifted],
        };
        let result: Result<MRangeSeriesResult, _> = wire.try_into();
        let msg = result.unwrap_err().to_string();
        for needle in [
            "mismatched timestamps",
            "column 1",
            "expected 0",
            "series 'b'",
        ] {
            assert!(msg.contains(needle), "missing '{needle}' in: {msg}");
        }
    }

    fn chunk_of(samples: Vec<Sample>) -> SampleData {
        serialize_chunk(TimeSeriesChunk::Uncompressed(UncompressedChunk::from_vec(
            samples,
        )))
        .unwrap()
    }

    fn wire_with_columns(columns: Vec<SampleData>) -> SeriesRangeResponse {
        SeriesRangeResponse {
            key: "a".into(),
            group_label_value: "g".into(),
            labels: Vec::new(),
            columns,
        }
    }

    /// Randomized fuzz over `deserialize_rows`'s ragged-column defense: shards
    /// that are corrupt, buggy, or from an incompatible version can send
    /// aggregation columns whose bucket counts disagree. Every generated
    /// mismatch must be rejected, never silently truncated/zipped.
    #[test]
    fn ragged_columns_rejected_randomized() {
        use rand::{RngExt, SeedableRng};

        let mut rng = rand::rngs::StdRng::seed_from_u64(0xC01A_D5EE);
        for _ in 0..200 {
            let num_columns = rng.random_range(2..6);
            let base_len = rng.random_range(1..8);
            let odd_column = rng.random_range(0..num_columns);
            // Force a real mismatch: the odd column's length differs from
            // every other column's length.
            let odd_len = base_len + rng.random_range(1..5);

            let columns: Vec<SampleData> = (0..num_columns)
                .map(|c| {
                    let len = if c == odd_column { odd_len } else { base_len };
                    let samples = (0..len)
                        .map(|i| Sample {
                            timestamp: i as i64 * 60,
                            value: rng.random(),
                        })
                        .collect();
                    chunk_of(samples)
                })
                .collect();

            let wire = wire_with_columns(columns);
            let result: Result<MRangeSeriesResult, _> = wire.try_into();
            let msg = match result {
                Ok(_) => panic!(
                    "expected rejection: num_columns={num_columns} base_len={base_len} odd_column={odd_column} odd_len={odd_len}"
                ),
                Err(e) => e.to_string(),
            };
            assert!(
                msg.contains("mismatched lengths"),
                "num_columns={num_columns} base_len={base_len} odd_column={odd_column} odd_len={odd_len}: {msg}"
            );
        }
    }

    /// Randomized fuzz over `deserialize_rows`'s timestamp-alignment defense:
    /// columns can agree on length while one bucket in one column drifts to a
    /// different timestamp than its peers. This must be rejected rather than
    /// silently paired with the wrong bucket.
    #[test]
    fn column_timestamp_drift_rejected_randomized() {
        use rand::{RngExt, SeedableRng};

        let mut rng = rand::rngs::StdRng::seed_from_u64(0xD21F_7A5E);
        for _ in 0..200 {
            let num_columns = rng.random_range(2..6);
            let len = rng.random_range(2..8);
            let drift_column = rng.random_range(0..num_columns);
            let drift_bucket = rng.random_range(0..len);
            let drift_delta = loop {
                let d = rng.random_range(-30i64..=30);
                if d != 0 {
                    break d;
                }
            };

            let columns: Vec<SampleData> = (0..num_columns)
                .map(|c| {
                    let samples = (0..len)
                        .map(|i| {
                            let mut timestamp = i as i64 * 60;
                            if c == drift_column && i == drift_bucket {
                                timestamp += drift_delta;
                            }
                            Sample {
                                timestamp,
                                value: rng.random(),
                            }
                        })
                        .collect();
                    chunk_of(samples)
                })
                .collect();

            let wire = wire_with_columns(columns);
            let result: Result<MRangeSeriesResult, _> = wire.try_into();
            let msg = match result {
                Ok(_) => panic!(
                    "expected rejection: num_columns={num_columns} len={len} drift_column={drift_column} drift_bucket={drift_bucket} drift_delta={drift_delta}"
                ),
                Err(e) => e.to_string(),
            };
            assert!(
                msg.contains("mismatched timestamps"),
                "num_columns={num_columns} len={len} drift_column={drift_column} drift_bucket={drift_bucket} drift_delta={drift_delta}: {msg}"
            );
        }
    }
}
