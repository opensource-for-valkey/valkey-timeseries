use crate::common::Timestamp;
use crate::common::constants::MAX_TIMESTAMP;
use crate::common::humanize::humanize_duration_ms;
use crate::common::time::current_time_millis;
use crate::error_consts;
use crate::parser::duration::parse_duration_value;
use crate::parser::timestamp::parse_timestamp;
use crate::series::TimeSeries;
use std::cmp::Ordering;
use std::fmt::Display;
use std::ops::RangeBounds;
use valkey_module::{ValkeyError, ValkeyResult, ValkeyString};

/// Simple date range with start and end timestamps.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct DateRange {
    pub start: Timestamp,
    pub end: Timestamp,
}

impl Default for DateRange {
    fn default() -> Self {
        Self {
            start: 0,
            end: MAX_TIMESTAMP,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Copy)]
pub enum TimestampValue {
    /// The timestamp of the earliest sample in the series
    Earliest,
    /// The timestamp of the latest sample in the series
    Latest,
    #[default]
    /// The current time
    Now,
    /// A specific timestamp
    Specific(Timestamp),
    /// A timestamp with a given delta(ms) from the current timestamp
    Relative(i64),
}

impl TimestampValue {
    pub fn as_timestamp(&self, now: Option<Timestamp>) -> Timestamp {
        use TimestampValue::*;
        match self {
            Earliest => 0,
            Latest => MAX_TIMESTAMP,
            Now => now.unwrap_or_else(current_time_millis),
            Specific(ts) => *ts,
            Relative(delta) => now
                .unwrap_or_else(current_time_millis)
                .saturating_add(*delta),
        }
    }

    pub fn as_series_timestamp(&self, series: &TimeSeries, now: Option<Timestamp>) -> Timestamp {
        use TimestampValue::*;

        match self {
            Earliest => series.get_min_timestamp(),
            Latest => series.last_timestamp(),
            Now => now.unwrap_or_else(current_time_millis),
            Specific(ts) => *ts,
            Relative(delta) => {
                let now = now.unwrap_or_else(current_time_millis);
                now.saturating_add(*delta)
            }
        }
    }
}

impl TryFrom<&str> for TimestampValue {
    type Error = ValkeyError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        use crate::error_consts;
        use TimestampValue::*;

        let len = value.len();

        if len == 0 {
            return Err(ValkeyError::Str(error_consts::INVALID_TIMESTAMP));
        }

        if len == 1 {
            match value {
                "-" => return Ok(Earliest),
                "+" => return Ok(Latest),
                "*" => return Ok(Now),
                _ => {}
            }
        }

        // Ergonomics. Support something like TS.RANGE key -6hrs -3hrs.
        //
        // The offset must carry a unit ("-6hrs"): a bare signed integer is an
        // absolute timestamp, not an offset. Reading "-1000" as "1000ms ago"
        // would silently turn a negative timestamp — which RedisTimeSeries
        // rejects outright — into a valid query over a completely different
        // window, so the sign alone must not select the relative form.
        if let Some(ch) = value.chars().next()
            && (ch == '-' || ch == '+')
            && !value[1..].bytes().all(|b| b.is_ascii_digit())
        {
            let value = &value[1..];
            let mut ms = parse_duration_ms(value)?;

            if ch == '-' {
                ms = -ms;
            }
            return Ok(Relative(ms));
        }

        let ts = parse_timestamp(value, false)
            .map_err(|_| ValkeyError::Str(error_consts::INVALID_TIMESTAMP))?;

        if ts < 0 {
            return Err(ValkeyError::Str(error_consts::NEGATIVE_TIMESTAMP));
        }

        Ok(Specific(ts))
    }
}

fn parse_duration_ms(arg: &str) -> ValkeyResult<i64> {
    parse_duration_value(arg).map_err(|_| ValkeyError::Str(error_consts::INVALID_DURATION))
}

impl TryFrom<String> for TimestampValue {
    type Error = ValkeyError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        TimestampValue::try_from(value.as_str())
    }
}

impl TryFrom<&ValkeyString> for TimestampValue {
    type Error = ValkeyError;

    fn try_from(value: &ValkeyString) -> Result<Self, Self::Error> {
        use TimestampValue::*;
        if value.len() == 1 {
            let bytes = value.as_slice();
            match bytes[0] {
                b'-' => return Ok(Earliest),
                b'+' => return Ok(Latest),
                b'*' => return Ok(Now),
                _ => {}
            }
        }
        if let Ok(int_val) = value.parse_integer() {
            if int_val < 0 {
                return Err(ValkeyError::Str(
                    "TSDB: invalid timestamp, must be a non-negative integer",
                ));
            }
            Ok(Specific(int_val))
        } else {
            let date_str = value.to_string_lossy();
            date_str.as_str().try_into()
        }
    }
}

impl From<Timestamp> for TimestampValue {
    fn from(ts: Timestamp) -> Self {
        TimestampValue::Specific(ts)
    }
}

impl Display for TimestampValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use TimestampValue::*;
        match self {
            Earliest => write!(f, "-"),
            Latest => write!(f, "+"),
            Specific(ts) => write!(f, "{ts}"),
            Now => write!(f, "*"),
            Relative(delta) => {
                let human = humanize_duration_ms(*delta);
                if delta.is_negative() {
                    write!(f, "{human} ago")
                } else {
                    write!(f, "{human} from now")
                }
            }
        }
    }
}

impl From<TimestampValue> for Timestamp {
    fn from(value: TimestampValue) -> Self {
        value.as_timestamp(None)
    }
}

impl PartialOrd for TimestampValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use TimestampValue::*;

        match (self, other) {
            (Now, Now) => Some(Ordering::Equal),
            (Earliest, Earliest) => Some(Ordering::Equal),
            (Latest, Latest) => Some(Ordering::Equal),
            (Relative(x), Relative(y)) => x.partial_cmp(y),
            (Specific(a), Specific(b)) => a.partial_cmp(b),
            (Now, Specific(v)) => {
                let now = current_time_millis();
                now.partial_cmp(v)
            }
            (Specific(v), Now) => {
                let now = current_time_millis();
                v.partial_cmp(&now)
            }
            (Relative(y), Now) => y.partial_cmp(&0i64),
            (Now, Relative(y)) => 0i64.partial_cmp(y),
            (Specific(_), Relative(y)) => 0i64.partial_cmp(y),
            (Relative(delta), Specific(y)) => {
                let relative = current_time_millis() + *delta;
                relative.partial_cmp(y)
            }
            (Earliest, _) => Some(Ordering::Less),
            (_, Earliest) => Some(Ordering::Greater),
            (Latest, _) => Some(Ordering::Greater),
            (_, Latest) => Some(Ordering::Less),
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct TimestampRange {
    pub start: TimestampValue,
    pub end: TimestampValue,
}

impl TimestampRange {
    /// An inverted range (start > end) is *not* an error: it selects no samples.
    /// Rejecting it would make us stricter than RedisTimeSeries, which answers
    /// every inverted-window query with an empty result (and `TS.DEL` with 0).
    /// [`Self::is_inverted`] is the single place readers short-circuit on.
    pub fn new(start: TimestampValue, end: TimestampValue) -> ValkeyResult<Self> {
        Ok(TimestampRange { start, end })
    }

    pub fn from_timestamps(start: Timestamp, end: Timestamp) -> ValkeyResult<Self> {
        Ok(TimestampRange {
            start: TimestampValue::Specific(start),
            end: TimestampValue::Specific(end),
        })
    }

    /// True when the window can not contain a sample, so callers can skip the
    /// fetch entirely rather than rely on every iterator handling start > end.
    pub fn is_inverted(&self) -> bool {
        self.start > self.end
    }

    pub fn get_series_range(
        &self,
        series: &TimeSeries,
        now: Option<Timestamp>,
        check_retention: bool,
    ) -> (Timestamp, Timestamp) {
        use TimestampValue::*;

        // In case a retention is set shouldn't return chunks older than the retention
        let mut start_timestamp = self.start.as_series_timestamp(series, now);
        let end_timestamp = if let Relative(delta) = self.end {
            start_timestamp.wrapping_add(delta)
        } else {
            self.end.as_series_timestamp(series, now)
        };

        if check_retention && !series.retention.is_zero() {
            let earliest = series.get_min_timestamp();
            start_timestamp = start_timestamp.max(earliest);
        }

        (start_timestamp, end_timestamp)
    }

    pub fn get_timestamps(&self, now: Option<Timestamp>) -> (Timestamp, Timestamp) {
        use TimestampValue::*;

        let start_timestamp = self.start.as_timestamp(now);
        let end_timestamp = if let Relative(delta) = self.end {
            start_timestamp.wrapping_add(delta)
        } else {
            self.end.as_timestamp(now)
        };

        (start_timestamp, end_timestamp)
    }

    pub fn is_empty(&self) -> bool {
        self.end == TimestampValue::Latest && self.start == TimestampValue::Earliest
    }

    pub fn resolve(self, now: Option<Timestamp>) -> DateRange {
        let (start, end) = self.get_timestamps(now);
        DateRange { start, end }
    }
}

impl Display for TimestampRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} to {}", self.start, self.end)
    }
}

impl Default for TimestampRange {
    fn default() -> Self {
        Self {
            start: TimestampValue::Earliest,
            end: TimestampValue::Latest,
        }
    }
}

impl RangeBounds<TimestampValue> for TimestampValue {
    fn start_bound(&self) -> std::ops::Bound<&TimestampValue> {
        std::ops::Bound::Included(self)
    }

    fn end_bound(&self) -> std::ops::Bound<&TimestampValue> {
        std::ops::Bound::Included(self)
    }
}

#[cfg(test)]
mod tests {
    use crate::common::Sample;
    use crate::common::constants::MAX_TIMESTAMP;
    use crate::common::time::current_time_millis;
    use crate::error_consts::INVALID_TIMESTAMP;
    use crate::series::timestamp_range::TimestampValue;
    use crate::series::{TimeSeries, TimestampRange};
    use std::cmp::Ordering;
    use valkey_module::ValkeyError;

    #[test]
    fn test_timestamp_range_value_try_from_earliest() {
        let input = "-";
        let result = TimestampValue::try_from(input);
        assert!(
            matches!(result, Ok(TimestampValue::Earliest)),
            "Expected Ok(Earliest), got {result:?}"
        );
    }

    #[test]
    fn test_timestamp_range_value_try_from_latest() {
        let input = "+";
        let result = TimestampValue::try_from(input);
        assert!(
            matches!(result, Ok(TimestampValue::Latest)),
            "Expected Ok(Latest), got {result:?}",
        );
    }

    #[test]
    fn test_timestamp_range_value_try_from_now() {
        let input = "*";
        let result = TimestampValue::try_from(input);
        assert!(
            matches!(result, Ok(TimestampValue::Now)),
            "Expected Ok(Now), got {result:?}"
        );
    }

    #[test]
    fn test_timestamp_range_value_try_from_positive_number() {
        let input = "12345678900";
        let result = TimestampValue::try_from(input);
        assert!(result.is_ok());
        let expected = TimestampValue::Specific(12345678900);
        assert_eq!(result.unwrap(), expected);
    }

    #[test]
    fn test_timestamp_range_value_try_from_negative_ms() {
        let input = "-12345678900";
        let result = TimestampValue::try_from(input);
        assert!(result.is_ok());
        let expected = TimestampValue::Relative(-12345678900);
        assert_eq!(result.unwrap(), expected);
    }

    #[test]
    fn test_timestamp_range_value_try_from_positive_duration() {
        let input = "+5s";
        let result = TimestampValue::try_from(input);
        assert!(result.is_ok());
        let expected_delta = 5000;
        assert_eq!(result.unwrap(), TimestampValue::Relative(expected_delta));
    }

    #[test]
    fn test_timestamp_range_value_try_from_negative_duration() {
        let input = "-5s";
        let result = TimestampValue::try_from(input);
        assert!(result.is_ok());
        let expected_delta = -5000;
        assert_eq!(result.unwrap(), TimestampValue::Relative(expected_delta));
    }

    #[test]
    fn test_timestamp_range_value_try_from_invalid_timestamp() {
        let input = "invalid_timestamp";
        let result = TimestampValue::try_from(input);
        assert!(matches!(result, Err(ValkeyError::Str(msg)) if msg == INVALID_TIMESTAMP));
    }

    #[test]
    fn test_timestamp_value_as_timestamp_now() {
        let now = TimestampValue::Now;
        let current_time = current_time_millis();
        let result = now.as_timestamp(None);
        assert!(
            (current_time..=current_time + 1).contains(&result),
            "Expected current time, got {result}"
        );
    }

    #[test]
    fn test_timestamp_value_as_timestamp_value() {
        let specific_timestamp = 1627849200000; // Example specific timestamp
        let value = TimestampValue::Specific(specific_timestamp);
        let result = value.as_timestamp(None);
        assert_eq!(
            result, specific_timestamp,
            "Expected {specific_timestamp}, got {result}"
        );
    }

    #[test]
    fn test_timestamp_value_as_series_timestamp_earliest() {
        let series = TimeSeries {
            first_timestamp: 1000,
            last_sample: Some(Sample {
                timestamp: 2000,
                value: 100.0,
            }),
            ..Default::default()
        };
        let earliest = TimestampValue::Earliest;
        let result = earliest.as_series_timestamp(&series, None);
        assert_eq!(
            result, series.first_timestamp,
            "Expected {}, got {result}",
            series.first_timestamp,
        );
    }

    #[test]
    fn test_timestamp_value_as_series_timestamp_latest() {
        let series = TimeSeries {
            first_timestamp: 1000,
            last_sample: Some(Sample {
                timestamp: 2000,
                value: 100.0,
            }),
            ..Default::default()
        };
        let latest = TimestampValue::Latest;
        let result = latest.as_series_timestamp(&series, None);
        let last_timestamp = series.last_timestamp();
        assert_eq!(
            result, last_timestamp,
            "Expected {last_timestamp}, got {result}"
        );
    }

    #[test]
    fn test_timestamp_value_as_series_timestamp_relative_positive_delta() {
        let series = TimeSeries {
            first_timestamp: 1000,
            last_sample: Some(Sample {
                timestamp: 2000,
                value: 100.0,
            }),
            ..Default::default()
        };
        let positive_delta = 5000; // 5 seconds
        let relative = TimestampValue::Relative(positive_delta);
        let current_time = current_time_millis();
        let result = relative.as_series_timestamp(&series, Some(current_time));
        assert_eq!(
            result,
            current_time.wrapping_add(positive_delta),
            "Expected {}, got {result}",
            current_time.wrapping_add(positive_delta),
        );
    }

    #[test]
    fn test_timestamp_value_as_series_timestamp_relative_negative_delta() {
        let series = TimeSeries {
            first_timestamp: 1000,
            last_sample: Some(Sample {
                timestamp: 2000,
                value: 100.0,
            }),
            ..Default::default()
        };
        let negative_delta = -5000; // -5 seconds
        let relative = TimestampValue::Relative(negative_delta);
        let current_time = current_time_millis();
        let result = relative.as_series_timestamp(&series, Some(current_time));
        assert_eq!(
            result,
            current_time.wrapping_add(negative_delta),
            "Expected {}, got {result}",
            current_time.wrapping_add(negative_delta)
        );
    }

    #[test]
    fn test_timestamp_value_as_series_timestamp_relative_overflow() {
        let series = TimeSeries {
            first_timestamp: 1000,
            last_sample: Some(Sample {
                timestamp: 2000,
                value: 100.0,
            }),
            ..Default::default()
        };
        let large_delta = i64::MAX; // A large positive delta that will cause overflow
        let relative = TimestampValue::Relative(large_delta);
        let result = relative.as_series_timestamp(&series, None);
        assert_eq!(
            result, MAX_TIMESTAMP,
            "Expected {MAX_TIMESTAMP}, got {result}",
        );
    }

    #[test]
    fn test_timestamp_value_as_series_timestamp_relative_underflow() {
        let series = TimeSeries {
            first_timestamp: 1000,
            last_sample: Some(Sample {
                timestamp: 2000,
                value: 100.0,
            }),
            ..Default::default()
        };
        let large_negative_delta = i64::MIN; // A large negative delta that will cause underflow
        let relative = TimestampValue::Relative(large_negative_delta);
        let current_time = current_time_millis();
        let result = relative.as_series_timestamp(&series, None);
        assert_eq!(
            result,
            current_time.wrapping_add(large_negative_delta),
            "Expected {}, got {result}",
            current_time.wrapping_add(large_negative_delta)
        );
    }

    #[test]
    fn test_timestamp_value_as_series_timestamp_value_zero() {
        let series = TimeSeries {
            first_timestamp: 1000,
            last_sample: Some(Sample {
                timestamp: 2000,
                value: 100.0,
            }),
            ..Default::default()
        };
        let zero_value = TimestampValue::Specific(0);
        let result = zero_value.as_series_timestamp(&series, None);
        assert_eq!(result, 0, "Expected 0, got {result}");
    }

    #[test]
    fn test_timestamp_value_as_series_timestamp_now() {
        let series = TimeSeries {
            first_timestamp: 1000,
            last_sample: Some(Sample {
                timestamp: 2000,
                value: 100.0,
            }),
            ..Default::default()
        };
        let now = TimestampValue::Now;
        let current_time = current_time_millis();
        let result = now.as_series_timestamp(&series, None);
        assert!(
            (current_time..=current_time + 1).contains(&result),
            "Expected current time, got {result}"
        );
    }

    #[test]
    fn test_timestamp_partial_cmp_relative_values() {
        let delta = 5000; // 5 seconds
        let relative1 = TimestampValue::Relative(delta);
        let relative2 = TimestampValue::Relative(delta);
        let result = relative1.partial_cmp(&relative2);
        assert_eq!(
            result,
            Some(Ordering::Equal),
            "Expected Ordering::Equal, got {result:?}"
        );

        assert!(TimestampValue::Relative(1000) < TimestampValue::Relative(2000));
        assert!(TimestampValue::Relative(1000) <= TimestampValue::Relative(2000));

        assert!(TimestampValue::Relative(3400) > TimestampValue::Relative(2000));
        assert!(TimestampValue::Relative(3400) >= TimestampValue::Relative(2000));
    }

    #[test]
    fn test_partial_cmp_earliest_with_other_values() {
        use crate::series::timestamp_range::TimestampValue;
        use std::cmp::Ordering;

        let earliest = TimestampValue::Earliest;
        let latest = TimestampValue::Latest;
        let now = TimestampValue::Now;
        let value = TimestampValue::Specific(1000);
        let relative = TimestampValue::Relative(5000);

        assert_eq!(
            earliest.partial_cmp(&latest),
            Some(Ordering::Less),
            "Expected Earliest < Latest"
        );
        assert_eq!(
            earliest.partial_cmp(&now),
            Some(Ordering::Less),
            "Expected Earliest < Now"
        );
        assert_eq!(
            earliest.partial_cmp(&value),
            Some(Ordering::Less),
            "Expected Earliest < Value"
        );
        assert_eq!(
            earliest.partial_cmp(&relative),
            Some(Ordering::Less),
            "Expected Earliest < Relative"
        );
    }

    #[test]
    fn test_partial_cmp_greater_than_earliest() {
        use crate::series::timestamp_range::TimestampValue;
        use std::cmp::Ordering;

        let earliest = TimestampValue::Earliest;

        let values = vec![
            TimestampValue::Latest,
            TimestampValue::Now,
            TimestampValue::Specific(1000),
            TimestampValue::Relative(1000),
        ];

        for value in values {
            let result = value.partial_cmp(&earliest);
            assert_eq!(
                result,
                Some(Ordering::Greater),
                "Expected {value:?} to be greater than Earliest, got {result:?}"
            );
        }
    }

    #[test]
    fn test_partial_cmp_latest_greater_than_others() {
        use crate::series::timestamp_range::TimestampValue;
        use std::cmp::Ordering;

        let latest = TimestampValue::Latest;

        let values = vec![
            TimestampValue::Earliest,
            TimestampValue::Now,
            TimestampValue::Specific(1000),
            TimestampValue::Relative(5000),
        ];

        for value in values {
            assert_eq!(
                latest.partial_cmp(&value),
                Some(Ordering::Greater),
                "Expected Latest to be greater than {value:?}."
            );
        }
    }

    #[test]
    fn test_partial_cmp_with_latest() {
        use crate::series::timestamp_range::TimestampValue;
        use std::cmp::Ordering;

        let latest = TimestampValue::Latest;

        let earliest = TimestampValue::Earliest;
        assert_eq!(
            earliest.partial_cmp(&latest),
            Some(Ordering::Less),
            "Expected Earliest to be less than Latest"
        );

        let now = TimestampValue::Now;
        assert_eq!(
            now.partial_cmp(&latest),
            Some(Ordering::Less),
            "Expected Now to be less than Latest"
        );

        let value = TimestampValue::Specific(1000);
        assert_eq!(
            value.partial_cmp(&latest),
            Some(Ordering::Less),
            "Expected Value to be less than Latest"
        );

        let relative = TimestampValue::Relative(5000);
        assert_eq!(
            relative.partial_cmp(&latest),
            Some(Ordering::Less),
            "Expected Relative to be less than Latest"
        );
    }

    #[test]
    fn test_partial_cmp_equal_values() {
        let timestamp1 = TimestampValue::Specific(1627849200000);
        let timestamp2 = TimestampValue::Specific(1627849200000);
        let result = timestamp1.partial_cmp(&timestamp2);
        assert_eq!(
            result,
            Some(Ordering::Equal),
            "Expected Ordering::Equal, got {result:?}"
        );
    }

    #[test]
    fn test_partial_cmp_now_with_relative_negative() {
        let now = TimestampValue::Now;
        let negative_delta = -1000; // -1 second
        let relative_negative = TimestampValue::Relative(negative_delta);

        let result = now.partial_cmp(&relative_negative);

        assert_eq!(
            result,
            Some(Ordering::Greater),
            "Expected Now to be greater than Relative(-1000), got {result:?}"
        );
    }

    #[test]
    fn test_partial_cmp_relative_with_now_positive() {
        let relative_value = TimestampValue::Relative(1000); // 1 second in the future
        let now_value = TimestampValue::Now;

        let result = relative_value.partial_cmp(&now_value);
        assert_eq!(
            result,
            Some(Ordering::Greater),
            "Expected Relative to be greater than Now, got {result:?}"
        );
    }

    #[test]
    fn test_partial_cmp_now_with_value_current_time() {
        let now = TimestampValue::Now;
        let current_time = current_time_millis();
        let value = TimestampValue::Specific(current_time);
        let result = now.partial_cmp(&value);
        assert_eq!(
            result,
            Some(Ordering::Equal),
            "Expected Ordering::Equal, got {result:?}"
        );
    }

    #[test]
    fn test_partial_cmp_value_future_with_now() {
        let future_timestamp = current_time_millis() + 10000; // 10 seconds in the future
        let value = TimestampValue::Specific(future_timestamp);
        let now = TimestampValue::Now;

        let result = value.partial_cmp(&now);

        assert_eq!(
            result,
            Some(Ordering::Greater),
            "Expected Value to be greater than Now, got {result:?}"
        );
    }

    #[test]
    fn test_timestamp_range_new_start_greater_than_end() {
        // An inverted range is accepted and reported as inverted; RTS answers
        // such a window with an empty result rather than an error.
        let start = TimestampValue::Specific(2000);
        let end = TimestampValue::Specific(1000);
        let range = TimestampRange::new(start, end).expect("inverted range must be accepted");

        assert!(range.is_inverted());
    }

    #[test]
    fn test_timestamp_range_new_negative_start_greater_than_end() {
        let start = TimestampValue::Specific(-1000);
        let end = TimestampValue::Specific(-2000);
        let range = TimestampRange::new(start, end).expect("inverted range must be accepted");

        assert!(range.is_inverted());
    }

    #[test]
    fn test_timestamp_range_not_inverted() {
        let range = TimestampRange::from_timestamps(1000, 2000).unwrap();
        assert!(!range.is_inverted());

        let equal = TimestampRange::from_timestamps(1000, 1000).unwrap();
        assert!(
            !equal.is_inverted(),
            "a single-point window is not inverted"
        );
    }

    #[test]
    fn test_timestamp_range_new_start_less_than_end() {
        let start = TimestampValue::Specific(1000);
        let end = TimestampValue::Specific(2000);
        let result = TimestampRange::new(start, end);

        assert!(
            matches!(result, Ok(range) if range.start == start && range.end == end),
            "Expected Ok with start and end values, got {result:?}"
        );
    }

    #[test]
    fn test_get_series_range_earliest_no_retention_check() {
        let series = TimeSeries {
            first_timestamp: 1000,
            last_sample: Some(Sample {
                timestamp: 2000,
                value: 100.0,
            }),
            ..Default::default()
        };
        let timestamp_range = TimestampRange {
            start: TimestampValue::Earliest,
            end: TimestampValue::Latest,
        };
        let (start, end) = timestamp_range.get_series_range(&series, None, false);
        assert_eq!(
            start, series.first_timestamp,
            "Expected start to be the earliest timestamp, got {start}"
        );
        let last_timestamp = series.last_timestamp();
        assert_eq!(
            end, last_timestamp,
            "Expected end to be the latest timestamp, got {end}"
        );
    }

    #[test]
    fn test_get_series_range_with_retention_limit() {
        let series = TimeSeries {
            first_timestamp: 1000,
            last_sample: Some(Sample {
                timestamp: 20000,
                value: 100.0,
            }),
            retention: std::time::Duration::from_secs(5), // 5-second retention
            ..Default::default()
        };

        let start = TimestampValue::Earliest;
        let end = TimestampValue::Latest;
        let range = TimestampRange::new(start, end).unwrap();

        let (start_timestamp, end_timestamp) = range.get_series_range(&series, None, true);

        let last_timestamp = series.last_timestamp();

        let expected_earliest_retention =
            last_timestamp.saturating_sub(series.retention.as_millis() as i64);

        assert_eq!(
            start_timestamp, expected_earliest_retention,
            "Expected start timestamp to be adjusted to retention limit"
        );

        assert_eq!(
            end_timestamp, last_timestamp,
            "Expected end timestamp to be the last timestamp of the series"
        );
    }

    #[test]
    fn test_get_series_range_latest_end_no_retention_check() {
        let series = TimeSeries {
            first_timestamp: 1000,
            last_sample: Some(Sample {
                timestamp: 5000,
                value: 100.0,
            }),
            ..Default::default()
        };
        let timestamp_range = TimestampRange {
            start: TimestampValue::Earliest,
            end: TimestampValue::Latest,
        };
        let check_retention = false;
        let (start, end) = timestamp_range.get_series_range(&series, None, check_retention);
        assert_eq!(
            start, series.first_timestamp,
            "Expected start timestamp to be {}, got {start}",
            series.first_timestamp
        );

        let last_timestamp = series.last_timestamp();
        assert_eq!(
            end, last_timestamp,
            "Expected end timestamp to be {last_timestamp}, got {end}"
        );
    }

    #[test]
    fn test_get_series_range_with_relative_end() {
        let series = TimeSeries {
            first_timestamp: 1000,
            last_sample: Some(Sample {
                timestamp: 5000,
                value: 100.0,
            }),
            ..Default::default()
        };
        let start = TimestampValue::Specific(2000);
        let end_delta = 3000; // 3 seconds
        let end = TimestampValue::Relative(end_delta);
        let timestamp_range = TimestampRange::new(start, end).unwrap();

        let (start_timestamp, end_timestamp) =
            timestamp_range.get_series_range(&series, None, false);

        assert_eq!(
            start_timestamp, 2000,
            "Expected start timestamp to be 2000, got {start_timestamp}",
        );
        assert_eq!(
            end_timestamp, 5000,
            "Expected end timestamp to be 5000, got {end_timestamp}",
        );
    }

    #[test]
    fn test_get_series_range_now_start_no_retention() {
        let series = TimeSeries {
            first_timestamp: 1000,
            last_sample: Some(Sample {
                timestamp: 2000,
                value: 100.0,
            }),
            ..Default::default()
        };
        let timestamp_range = TimestampRange {
            start: TimestampValue::Now,
            end: TimestampValue::Latest,
        };
        let current_time = current_time_millis();
        let (start_timestamp, end_timestamp) =
            timestamp_range.get_series_range(&series, None, false);
        assert!(
            (current_time..=current_time + 1).contains(&start_timestamp),
            "Expected start timestamp to be current time, got {start_timestamp}"
        );
        assert_eq!(
            end_timestamp,
            series.last_timestamp(),
            "Expected end timestamp to be {}, got {end_timestamp}",
            series.last_timestamp(),
        );
    }
}
