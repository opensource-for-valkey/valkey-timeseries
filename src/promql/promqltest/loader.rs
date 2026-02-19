use crate::common::Sample;
use crate::promql::Labels;
use crate::promql::engine::test_utils::MockSeriesQuerier;
use crate::promql::promqltest::dsl::SeriesLoad;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

/// Load series data into TSDB
pub(super) fn load_series(
    storage: Arc<MockSeriesQuerier>,
    interval: std::time::Duration,
    series: &[SeriesLoad],
) -> Result<(), String> {
    for s in series {
        // Collect all samples for this series
        let mut samples: Vec<Sample> = Vec::new();

        for (step, value) in &s.values {
            // Validate step index
            if *step < 0 {
                return Err(format!("Negative step index not allowed: {}", step));
            }

            // Safe timestamp calculation with overflow checking
            let delta = interval
                .checked_mul(*step as u32)
                .ok_or_else(|| format!("Timestamp overflow for step {}", step))?;

            let ts = UNIX_EPOCH + delta;
            let ts_ms = ts
                .duration_since(UNIX_EPOCH)
                .map_err(|e| format!("Invalid timestamp: {}", e))?
                .as_millis() as i64;

            samples.push(Sample::new(ts_ms, *value));
        }

        // Sort by timestamp and deduplicate (keep last value per timestamp)
        // This matches Prometheus promqltest semantics
        samples.sort_by_key(|s| s.timestamp);
        samples.dedup_by_key(|s| s.timestamp);

        let labels: Labels = (&s.labels).into();
        for sample in samples {
            storage.add_sample(&labels, sample);
        }
    }

    Ok(())
}
