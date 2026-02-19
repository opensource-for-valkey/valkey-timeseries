use crate::promql::functions::types::{PromQLArg, PromQLFunction};
use crate::promql::functions::utils::{exact_arity_error, max_arity_error};
use crate::promql::{EvalResult, EvalSample, ExprResult};
use chrono::{DateTime, Datelike, NaiveDate, Timelike, Utc};
use std::default::Default;

#[derive(Copy, Clone)]
pub(in crate::promql) struct TimestampFunction;

impl PromQLFunction for TimestampFunction {
    fn apply(&self, arg: PromQLArg, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        let mut samples = arg.into_instant_vector()?;
        for sample in &mut samples {
            sample.value = sample.timestamp_ms as f64 / 1000.0;
            sample.timestamp_ms = eval_timestamp_ms;
            sample.drop_name = true;
        }

        Ok(ExprResult::InstantVector(samples))
    }
}

impl Default for TimestampFunction {
    fn default() -> Self {
        TimestampFunction
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) enum DateTimePart {
    Year,
    Month,
    DayOfMonth,
    DayOfYear,
    DayOfWeek,
    Hour,
    Minute,
    DaysInMonth,
}

impl DateTimePart {
    fn extract(&self, dt: DateTime<Utc>) -> f64 {
        match self {
            Self::Year => dt.year() as f64,
            Self::Month => dt.month() as f64,
            Self::DayOfMonth => dt.day() as f64,
            Self::DayOfYear => dt.ordinal() as f64,
            Self::DayOfWeek => dt.weekday().num_days_from_sunday() as f64,
            Self::Hour => dt.hour() as f64,
            Self::Minute => dt.minute() as f64,
            Self::DaysInMonth => days_in_month(dt) as f64,
        }
    }
}

fn datetime_from_seconds(value: f64) -> Option<DateTime<Utc>> {
    if !value.is_finite() {
        return None;
    }

    let seconds = value.trunc();
    if !(i64::MIN as f64..=i64::MAX as f64).contains(&seconds) {
        return None;
    }

    DateTime::from_timestamp(seconds as i64, 0)
}

fn datetime_from_millis(value: i64) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp(value / 1000, 0)
}

fn days_in_month(dt: DateTime<Utc>) -> u32 {
    let start_of_month =
        NaiveDate::from_ymd_opt(dt.year(), dt.month(), 1).expect("valid start of month");
    let start_of_next_month = if dt.month() == 12 {
        NaiveDate::from_ymd_opt(dt.year() + 1, 1, 1).expect("valid start of next month")
    } else {
        NaiveDate::from_ymd_opt(dt.year(), dt.month() + 1, 1).expect("valid start of next month")
    };

    start_of_next_month
        .signed_duration_since(start_of_month)
        .num_days() as u32
}

fn eval_datetime_function(samples: &mut [EvalSample], eval_timestamp_ms: i64, part: DateTimePart) {
    for sample in samples {
        sample.value = datetime_from_seconds(sample.value)
            .map(|dt| part.extract(dt))
            .unwrap_or(f64::NAN);
        sample.timestamp_ms = eval_timestamp_ms;
        sample.drop_name = true;
    }
}

fn sample_value(part: DateTimePart, dt: DateTime<Utc>) -> f64 {
    part.extract(dt)
}

macro_rules! make_datetime_function {
    ( $name: ident, $func_name: literal, $part: expr ) => {
        #[derive(Copy, Clone)]
        pub(in crate::promql) struct $name;

        impl $name {
            pub fn new() -> Self {
                Self
            }
        }

        impl PromQLFunction for $name {
            fn apply(&self, arg: PromQLArg, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
                let mut samples = arg.into_instant_vector()?;
                eval_datetime_function(&mut samples, eval_timestamp_ms, $part);
                Ok(ExprResult::InstantVector(samples))
            }

            fn apply_args(
                &self,
                args: Vec<PromQLArg>,
                eval_timestamp_ms: i64,
            ) -> EvalResult<ExprResult> {
                match args.len() {
                    0 => Ok(ExprResult::InstantVector(vec![EvalSample {
                        timestamp_ms: eval_timestamp_ms,
                        value: datetime_from_millis(eval_timestamp_ms)
                            .map(|dt| sample_value($part, dt))
                            .unwrap_or(f64::NAN),
                        labels: Default::default(),
                        drop_name: false,
                    }])),
                    1 => self.apply(
                        args.into_iter().next().expect("single arg"),
                        eval_timestamp_ms,
                    ),
                    _ => Err(max_arity_error($func_name, 1, args.len())),
                }
            }
        }
    };
}

make_datetime_function!(YearFunction, "year", DateTimePart::Year);
make_datetime_function!(MonthFunction, "month", DateTimePart::Month);
make_datetime_function!(DayOfMonthFunction, "day_of_month", DateTimePart::DayOfMonth);
make_datetime_function!(DayOfYearFunction, "day_of_year", DateTimePart::DayOfYear);
make_datetime_function!(DayOfWeekFunction, "day_of_week", DateTimePart::DayOfWeek);
make_datetime_function!(HourFunction, "hour", DateTimePart::Hour);
make_datetime_function!(MinuteFunction, "minute", DateTimePart::Minute);
make_datetime_function!(
    DaysInMonthFunction,
    "days_in_month",
    DateTimePart::DaysInMonth
);

#[derive(Copy, Clone)]
pub(in crate::promql) struct TimeFunction;

impl PromQLFunction for TimeFunction {
    fn apply(&self, _arg: PromQLArg, _eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        Err(exact_arity_error("time", 0, 1))
    }

    fn apply_args(&self, args: Vec<PromQLArg>, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        if !args.is_empty() {
            return Err(exact_arity_error("time", 0, args.len()));
        }

        Ok(ExprResult::InstantVector(vec![EvalSample {
            timestamp_ms: eval_timestamp_ms,
            value: eval_timestamp_ms as f64 / 1000.0,
            labels: Default::default(),
            drop_name: false,
        }]))
    }
}

impl Default for TimeFunction {
    fn default() -> Self {
        TimeFunction
    }
}
