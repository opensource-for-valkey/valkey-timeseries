/// Mon Dec 31 4192 06:13:20 GMT+0000
pub const MAX_TIMESTAMP: i64 = i64::MAX;
pub const METRIC_NAME_LABEL: &str = "__name__";

pub const REDUCER_KEY: &str = "__reducer__";
pub const SOURCE_KEY: &str = "__source__";
pub static META_KEY_LABEL: &str = "__meta:key__";

pub const MILLIS_PER_SEC: u64 = 1000;
pub const MILLIS_PER_MIN: u64 = 60 * MILLIS_PER_SEC;
pub const MILLIS_PER_HOUR: u64 = 60 * MILLIS_PER_MIN;
pub const MILLIS_PER_DAY: u64 = 24 * MILLIS_PER_HOUR;
pub const MILLIS_PER_WEEK: u64 = 7 * MILLIS_PER_DAY;
pub const MILLIS_PER_YEAR: u64 = 365 * MILLIS_PER_DAY;
