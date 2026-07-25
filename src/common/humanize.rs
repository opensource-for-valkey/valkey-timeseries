use crate::common::constants::{
    MILLIS_PER_DAY, MILLIS_PER_HOUR, MILLIS_PER_MIN, MILLIS_PER_SEC, MILLIS_PER_YEAR,
};
use std::time::Duration;

/// humanize converts a given number to a human-readable format
/// by adding metric prefixes https://en.wikipedia.org/wiki/Metric_prefix
pub fn humanize(v: f64) -> String {
    let mut v = v;
    if v == 0f64 || v.is_nan() || v.is_infinite() {
        return format!("{v:.4}");
    }
    let mut prefix: &str;
    if v.abs() >= 1.0 {
        prefix = "";
        for p in ["k", "M", "G", "T", "P", "E", "Z", "Y"] {
            if v.abs() < 1000.0 {
                break;
            }
            prefix = p;
            v /= 1000.0;
        }
        return format!("{v:.4}{prefix}");
    }
    prefix = "";
    for p in ["m", "u", "n", "p", "f", "a", "z", "y"] {
        if v.abs() >= 1.0 {
            break;
        }
        prefix = p;
        v *= 1000.0;
    }
    format!("{v:.4}{prefix}")
}

/// Units used when rendering a duration, largest first.
///
/// Every suffix is one the duration parser accepts. Weeks are deliberately omitted even though
/// the parser understands `w`: rendering 30 days as `4w2d` is less readable than `30d`.
const DURATION_UNITS: [(&str, u64); 6] = [
    ("y", MILLIS_PER_YEAR),
    ("d", MILLIS_PER_DAY),
    ("h", MILLIS_PER_HOUR),
    ("m", MILLIS_PER_MIN),
    ("s", MILLIS_PER_SEC),
    ("ms", 1),
];

/// Renders a duration given in milliseconds, largest unit first.
///
/// The rendering is exact: any remainder is carried into the next smaller unit, so a value is
/// emitted as the concatenated segments the duration parser understands (`5400000` becomes
/// `1h30m`). `parse_duration_value(&humanize_duration_ms(v))` therefore returns `v` again.
pub fn humanize_duration_ms(v: i64) -> String {
    if v == 0 {
        return "0ms".to_string();
    }

    let mut out = String::new();
    if v < 0 {
        out.push('-');
    }
    // `unsigned_abs` rather than `abs`, so `i64::MIN` does not overflow.
    let mut remaining = v.unsigned_abs();

    for (suffix, unit) in DURATION_UNITS {
        let count = remaining / unit;
        if count > 0 {
            out.push_str(&count.to_string());
            out.push_str(suffix);
            remaining -= count * unit;
        }
    }

    out
}

pub fn humanize_duration(v: &Duration) -> String {
    humanize_duration_ms(v.as_millis().try_into().unwrap_or(i64::MAX))
}

pub fn humanize_bytes(size: f64) -> String {
    let mut suffix: &str = "";
    let mut size = size;
    for p in ["ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi", "Yi"] {
        if size.abs() < 1024f64 {
            break;
        }
        suffix = p;
        size /= 1024.0
    }
    format!("{size:.4}{suffix}")
}

const TB: u64 = 1 << 40;
const GB: u64 = 1 << 30;
const MB: u64 = 1 << 20;
const KB: u64 = 1 << 10;

/// Present size in human-readable form
pub fn human_readable_size(size: usize) -> String {
    let size = size as u64;
    let (value, unit) = {
        if size >= 2 * TB {
            (size as f64 / TB as f64, "TB")
        } else if size >= 2 * GB {
            (size as f64 / GB as f64, "GB")
        } else if size >= 2 * MB {
            (size as f64 / MB as f64, "MB")
        } else if size >= 2 * KB {
            (size as f64 / KB as f64, "KB")
        } else {
            (size as f64, "B")
        }
    };
    format!("{value:.1} {unit}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_duration_value;

    #[test]
    fn humanize_duration_ms_renders_single_units() {
        assert_eq!(humanize_duration_ms(0), "0ms");
        assert_eq!(humanize_duration_ms(1), "1ms");
        assert_eq!(humanize_duration_ms(999), "999ms");
        assert_eq!(humanize_duration_ms(1_000), "1s");
        assert_eq!(humanize_duration_ms(60_000), "1m");
        assert_eq!(humanize_duration_ms(3_600_000), "1h");
        assert_eq!(humanize_duration_ms(86_400_000), "1d");
        assert_eq!(humanize_duration_ms(31_536_000_000), "1y");
    }

    /// Values below a second used to be multiplied by 1000 and still labelled `ms`, and values
    /// spanning more than one unit were divided by 1000 per unit rather than by that unit's own
    /// size — so 500ms rendered as `500000ms` and one hour as `3m`.
    #[test]
    fn humanize_duration_ms_regressions() {
        assert_eq!(humanize_duration_ms(500), "500ms");
        assert_eq!(humanize_duration_ms(750), "750ms");
        assert_eq!(humanize_duration_ms(3_600_000), "1h");
        assert_eq!(humanize_duration_ms(10_000), "10s");
    }

    #[test]
    fn humanize_duration_ms_carries_remainders() {
        assert_eq!(humanize_duration_ms(5_400_000), "1h30m");
        assert_eq!(humanize_duration_ms(1_500), "1s500ms");
        assert_eq!(humanize_duration_ms(90_061_001), "1d1h1m1s1ms");
    }

    #[test]
    fn humanize_duration_ms_handles_negatives() {
        assert_eq!(humanize_duration_ms(-500), "-500ms");
        assert_eq!(humanize_duration_ms(-5_400_000), "-1h30m");
        // Must not overflow.
        let _ = humanize_duration_ms(i64::MIN);
    }

    /// The rendered form is fed back to users (config range errors) and into query text
    /// (`ASOF JOIN ... TOLERANCE`), so it has to parse back to the value it came from.
    #[test]
    fn humanize_duration_ms_round_trips_through_the_parser() {
        let cases = [
            0i64,
            1,
            500,
            750,
            999,
            1_000,
            1_500,
            10_000,
            60_000,
            90_061_001,
            3_600_000,
            5_400_000,
            86_400_000,
            31_536_000_000,
            10 * 31_536_000_000,
            100 * 31_536_000_000,
            -500,
            -5_400_000,
        ];

        for value in cases {
            let rendered = humanize_duration_ms(value);
            assert_eq!(
                parse_duration_value(&rendered).unwrap(),
                value,
                "{value}ms rendered as {rendered:?} did not parse back"
            );
        }
    }

    #[test]
    fn humanize_duration_matches_millisecond_rendering() {
        assert_eq!(humanize_duration(&Duration::from_millis(750)), "750ms");
        assert_eq!(humanize_duration(&Duration::from_secs(3600)), "1h");
        assert_eq!(humanize_duration(&Duration::ZERO), "0ms");
    }
}
