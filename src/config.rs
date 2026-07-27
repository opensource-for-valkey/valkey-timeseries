use crate::common::humanize::humanize_duration_ms;
use crate::common::rounding::{
    MAX_DECIMAL_DIGITS, MAX_SIGNIFICANT_DIGITS, MIN_DECIMAL_DIGITS, MIN_SIGNIFICANT_DIGITS,
    RoundingStrategy,
};
use crate::common::sync::lock;
use crate::error_consts;
use crate::parser::number::parse_number;
use crate::parser::parse_duration_value;
use crate::series::chunks::{ChunkEncoding, validate_chunk_size};
use crate::series::{
    DuplicatePolicy, add_compaction_policies_from_config, clear_compaction_policy_config,
};
use std::borrow::Cow;
use std::os::raw::c_int;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicU16, AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::Duration;
use valkey_module::configuration::{
    ConfigurationContext, ConfigurationFlags, get_bool_default_config_value,
    get_i64_default_config_value, get_string_default_config_value, register_bool_configuration,
    register_i64_configuration, register_string_configuration,
};
use valkey_module::logging::{log_notice, log_warning};
use valkey_module::{
    ConfigurationValue, Context, RedisModule_LoadConfigs, ValkeyError, ValkeyGILGuard,
    ValkeyResult, ValkeyString, raw,
};

use crate::promql::engine::config::PROMQL_CONFIG;

/// Minimal Valkey version that supports the TimeSeries Module
pub const TIMESERIES_MIN_SUPPORTED_VERSION: &[i64; 3] = &[8, 0, 0];
pub const SPLIT_FACTOR: f64 = 1.2;

const ONE_DAY_MS: i64 = 24 * 60 * 60 * 1000;
const ONE_YEAR_MS: i64 = 365 * ONE_DAY_MS;

pub(crate) const FANOUT_COMMAND_TIMEOUT_MIN: i64 = 500;
pub(crate) const FANOUT_COMMAND_TIMEOUT_MAX: i64 = 10000;

pub const CHUNK_SIZE_MIN: i64 = 64;
pub const CHUNK_SIZE_MAX: i64 = 1024 * 1024;
pub const CHUNK_SIZE_DEFAULT: i64 = 4 * 1024;
// Rounding bounds come from the rounding module, which is what actually applies them: the
// per-series `DECIMAL_DIGITS`/`SIGNIFICANT_DIGITS` command arguments are validated against the
// same constants. Note the two minima differ: `DECIMAL_DIGITS 0` rounds to whole numbers,
// whereas "zero significant digits" is not a quantity and is rejected.
pub const DECIMAL_DIGITS_MAX: i64 = MAX_DECIMAL_DIGITS as i64;
pub const DECIMAL_DIGITS_MIN: i64 = MIN_DECIMAL_DIGITS as i64;
pub const SIGNIFICANT_DIGITS_MAX: i64 = MAX_SIGNIFICANT_DIGITS as i64;
pub const SIGNIFICANT_DIGITS_MIN: i64 = MIN_SIGNIFICANT_DIGITS as i64;
pub const DEFAULT_CHUNK_SIZE_BYTES: usize = CHUNK_SIZE_DEFAULT as usize;
pub const DEFAULT_CHUNK_ENCODING: ChunkEncoding = ChunkEncoding::Chimp;
pub const DEFAULT_DUPLICATE_POLICY: DuplicatePolicy = DuplicatePolicy::Block;
pub const IGNORE_MAX_TIME_DIFF_DEFAULT: i64 = 0;
pub const IGNORE_MAX_TIME_DIFF_MIN: i64 = 0;
pub const IGNORE_MAX_TIME_DIFF_MAX: i64 = ONE_YEAR_MS * 100; // 100 years
pub const IGNORE_MAX_VALUE_DIFF_MIN: f64 = 0.0;
pub const IGNORE_MAX_VALUE_DIFF_MAX: f64 = f64::MAX;

pub const MIN_THREADS: i64 = 1;
pub const MAX_THREADS: i64 = 16;
pub const DEFAULT_THREADS: i64 = 4;

pub const RETENTION_POLICY_MIN: i64 = 0;
pub const RETENTION_POLICY_MAX: i64 = 10 * ONE_YEAR_MS; // 10 years

/// The default compaction policy: no automatic downsampling rules.
pub(crate) const DEFAULT_COMPACTION_POLICY: &str = "";

pub const INDEX_BUILD_MAX_MEMORY_MIN: i64 = 0; // 0 = unlimited
pub const INDEX_BUILD_MAX_MEMORY_MAX: i64 = i64::MAX;
pub const INDEX_BUILD_MAX_MEMORY_DEFAULT: i64 = 256 * 1024 * 1024; // 256 MiB

pub const CLUSTER_MAP_EXPIRATION_MS_DEFAULT: u64 = 750; // default: 0.75 seconds
pub(crate) const CLUSTER_MAP_EXPIRATION_MIN_MS: i64 = 0; // min: 0 (no cache)
pub(crate) const CLUSTER_MAP_EXPIRATION_MAX_MS: i64 = 3_600_000; // max: 1 hour

/// The type of value a configuration parameter holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigType {
    Integer,
    Float,
    Boolean,
    String,
    Duration,
    Enum,
}

impl ConfigType {
    pub fn as_str(self) -> &'static str {
        match self {
            ConfigType::Integer => "integer",
            ConfigType::Float => "float",
            ConfigType::Boolean => "boolean",
            ConfigType::String => "string",
            ConfigType::Duration => "duration",
            ConfigType::Enum => "enum",
        }
    }
}

/// A configuration value: a parameter's default, one end of its range, or its current value.
///
/// Every variant is `const`-constructible — `Cow::Borrowed` included — so that [`CONFIGS`] can
/// be a `static` while the same type still carries values only known at runtime (an encoding
/// name, the compaction policy). Defaults and bounds are therefore declared exactly once, in
/// the registry, rather than once for registration and again for `TS._DEBUG LIST_CONFIGS`.
#[derive(Debug, Clone, PartialEq)]
pub enum ConfigValue {
    Integer(i64),
    Float(f64),
    Boolean(bool),
    String(Cow<'static, str>),
    /// A duration, in milliseconds.
    DurationMs(i64),
}

/// Reported as the value or default of a parameter that is unset or disabled.
pub const CONFIG_VALUE_NONE: &str = "none";

impl ConfigValue {
    /// Convenience constructor for a borrowed string, usable in `const` position.
    pub const fn str(value: &'static str) -> Self {
        ConfigValue::String(Cow::Borrowed(value))
    }

    /// A value meaning "unset" or "disabled".
    pub const fn none() -> Self {
        ConfigValue::str(CONFIG_VALUE_NONE)
    }

    /// The literal text registered with the server as this parameter's default.
    fn as_registration_str(&self) -> Cow<'static, str> {
        match self {
            ConfigValue::String(s) => s.clone(),
            ConfigValue::Integer(v) => Cow::Owned(v.to_string()),
            ConfigValue::DurationMs(ms) => Cow::Owned(ms.to_string()),
            ConfigValue::Float(v) => Cow::Owned(v.to_string()),
            ConfigValue::Boolean(b) => Cow::Borrowed(if *b { "yes" } else { "no" }),
        }
    }

    fn as_i64(&self) -> Option<i64> {
        match self {
            ConfigValue::Integer(v) | ConfigValue::DurationMs(v) => Some(*v),
            _ => None,
        }
    }

    fn as_bool(&self) -> Option<bool> {
        match self {
            ConfigValue::Boolean(b) => Some(*b),
            _ => None,
        }
    }
}

/// Where a parameter's value lives, and how a newly-set value is validated.
///
/// The store is reached through a `fn` pointer rather than a reference so that the registry
/// stays `const`-constructible: the string stores are `LazyLock`s, which cannot be
/// dereferenced in a `static` initializer.
pub enum ConfigStorage {
    /// A string-valued parameter. `apply` parses the raw text and commits the typed value to
    /// the parameter's own store; it is the only place a string parameter is validated, and
    /// the server rejects the `CONFIG SET` when it returns `Err`.
    Str {
        cell: fn() -> &'static ValkeyGILGuard<ValkeyString>,
        apply: fn(&str) -> ValkeyResult<()>,
    },
    /// An integer parameter. The server enforces `min`/`max` from the descriptor before the
    /// value reaches `cell`; `validate` covers constraints the server cannot express (for
    /// example the chunk-size alignment rule).
    I64 {
        cell: fn() -> &'static AtomicI64,
        validate: Option<fn(i64) -> ValkeyResult<()>>,
    },
    /// A boolean parameter. The server writes straight to `cell`; there is nothing to parse.
    Bool { cell: fn() -> &'static AtomicBool },
}

/// Everything the module knows about one configuration parameter.
///
/// This is the single source of truth: [`register_config`] registers from it, the server's
/// set-callback dispatches through [`ConfigStorage`] rather than matching on the name, and
/// `TS._DEBUG LIST_CONFIGS` reports from it. Adding a parameter means adding one entry.
pub struct ConfigDesc {
    pub name: &'static str,
    pub kind: ConfigType,
    pub default: ConfigValue,
    pub min: Option<ConfigValue>,
    pub max: Option<ConfigValue>,
    pub flags: ConfigurationFlags,
    pub description: &'static str,
    pub storage: ConfigStorage,
    /// Reads the parameter's current value from its typed store, for reporting.
    pub read: fn() -> ConfigValue,
}

impl ConfigDesc {
    /// `ConfigurationFlags` is neither `Copy` nor `Clone`, so hand registration a fresh value
    /// built from the same bits.
    fn flags(&self) -> ConfigurationFlags {
        ConfigurationFlags::from_bits_truncate(self.flags.bits())
    }

    /// Whether the parameter can be changed after startup.
    pub fn is_mutable(&self) -> bool {
        !self.flags.contains(ConfigurationFlags::IMMUTABLE)
    }
}

pub static CHUNK_SIZE: AtomicI64 = AtomicI64::new(CHUNK_SIZE_DEFAULT);

/// Size of the module's global rayon thread pool (`ts-num-threads`).
///
/// This is the single source of truth for pool size: `init_thread_pool()` reads it directly
/// when building the global rayon pool, and heuristics that scale work by thread count
/// (`multi_del.rs`, `rcf_outlier_detector.rs`) read it too.
///
/// Rayon's global thread pool cannot be resized once built (`ThreadPoolBuilder::build_global`
/// has no counterpart to shrink/grow an already-initialized `Registry`), so this config is
/// registered with `ConfigurationFlags::IMMUTABLE`: it can only be set at startup (`valkey.conf`
/// or `MODULE LOAD` args), and `CONFIG SET ts-num-threads` is rejected by the server itself
/// rather than silently no-op-ing.
pub static NUM_THREADS: AtomicI64 = AtomicI64::new(DEFAULT_THREADS);

pub fn num_threads() -> usize {
    NUM_THREADS.load(Ordering::Relaxed) as usize
}

pub const DEFAULT_FANOUT_COMMAND_TIMEOUT_MS: u64 = 5000;

/// Rounding applied to sample values (`ts-decimal-digits` / `ts-significant-digits`).
///
/// The two parameters are mutually exclusive, so a single store holds both: packing the kind
/// and the digit count into one atomic makes "decimal and significant rounding both active"
/// unrepresentable, where the previous three separate stores could disagree. Read through
/// [`rounding_strategy`]; written through [`store_rounding_strategy`].
static ROUNDING_STRATEGY: AtomicU16 = AtomicU16::new(ROUNDING_NONE);

/// Packed encoding of `Option<RoundingStrategy>`: the high byte is the kind, the low byte the
/// digit count. Digit counts are validated against `*_DIGITS_MAX` (<= 18) before being stored.
const ROUNDING_NONE: u16 = 0;
const ROUNDING_DECIMAL: u16 = 1 << 8;
const ROUNDING_SIGNIFICANT: u16 = 2 << 8;

fn encode_rounding_strategy(strategy: Option<RoundingStrategy>) -> u16 {
    match strategy {
        None => ROUNDING_NONE,
        Some(RoundingStrategy::DecimalDigits(digits)) => ROUNDING_DECIMAL | digits as u16,
        Some(RoundingStrategy::SignificantDigits(digits)) => ROUNDING_SIGNIFICANT | digits as u16,
    }
}

fn decode_rounding_strategy(packed: u16) -> Option<RoundingStrategy> {
    let digits = (packed & 0xFF) as u8;
    match packed & 0xFF00 {
        ROUNDING_DECIMAL => Some(RoundingStrategy::DecimalDigits(digits)),
        ROUNDING_SIGNIFICANT => Some(RoundingStrategy::SignificantDigits(digits)),
        _ => None,
    }
}

/// The rounding applied to sample values, or `None` when rounding is disabled.
pub fn rounding_strategy() -> Option<RoundingStrategy> {
    decode_rounding_strategy(ROUNDING_STRATEGY.load(Ordering::Relaxed))
}

fn store_rounding_strategy(strategy: Option<RoundingStrategy>) {
    ROUNDING_STRATEGY.store(encode_rounding_strategy(strategy), Ordering::SeqCst);
}

/// `ts-ignore-max-time-diff`, in milliseconds.
static IGNORE_MAX_TIME_DIFF: AtomicI64 = AtomicI64::new(IGNORE_MAX_TIME_DIFF_DEFAULT);

/// `ts-ignore-max-value-diff`, held as the `f64` bit pattern.
static IGNORE_MAX_VALUE_DIFF: AtomicU64 = AtomicU64::new(0);

pub fn ignore_max_value_diff() -> f64 {
    f64::from_bits(IGNORE_MAX_VALUE_DIFF.load(Ordering::Relaxed))
}

/// `ts-retention-policy`, in milliseconds. Zero means "no expiry".
static RETENTION_PERIOD_MS: AtomicU64 = AtomicU64::new(0);

pub fn retention_period() -> Duration {
    Duration::from_millis(RETENTION_PERIOD_MS.load(Ordering::Relaxed))
}

/// `ts-encoding`, held as the `#[repr(u8)]` discriminant of [`ChunkEncoding`].
static CHUNK_ENCODING: AtomicU8 = AtomicU8::new(DEFAULT_CHUNK_ENCODING as u8);

pub fn chunk_encoding() -> ChunkEncoding {
    ChunkEncoding::try_from(CHUNK_ENCODING.load(Ordering::Relaxed))
        .unwrap_or(DEFAULT_CHUNK_ENCODING)
}

/// `ts-duplicate-policy`, held as the `#[repr(u8)]` discriminant of [`DuplicatePolicy`].
static DUPLICATE_POLICY: AtomicU8 = AtomicU8::new(DEFAULT_DUPLICATE_POLICY as u8);

pub fn duplicate_policy() -> DuplicatePolicy {
    DuplicatePolicy::from_u8(DUPLICATE_POLICY.load(Ordering::Relaxed))
        .unwrap_or(DEFAULT_DUPLICATE_POLICY)
}

/// `ts-chunk-size`, in bytes.
pub fn chunk_size_bytes() -> usize {
    CHUNK_SIZE.load(Ordering::Relaxed) as usize
}

pub fn ignore_max_time_diff() -> u64 {
    IGNORE_MAX_TIME_DIFF.load(Ordering::Relaxed) as u64
}

/// `ts-fanout-command-timeout`, in milliseconds.
pub static FANOUT_COMMAND_TIMEOUT: AtomicU64 = AtomicU64::new(DEFAULT_FANOUT_COMMAND_TIMEOUT_MS);

/// `ts-cluster-map-expiration-ms`. Read as raw milliseconds by the cluster-map backoff
/// arithmetic in `fanout`, so it is not wrapped in a `Duration` accessor.
pub static CLUSTER_MAP_EXPIRATION_MS: AtomicU64 = AtomicU64::new(CLUSTER_MAP_EXPIRATION_MS_DEFAULT);

/// Typed store for `ts-compaction-policy`. The parsed rules live in the compaction module;
/// this keeps the policy text so the parameter can be reported like any other.
pub static COMPACTION_POLICY: LazyLock<Mutex<String>> =
    LazyLock::new(|| Mutex::new(DEFAULT_COMPACTION_POLICY.to_string()));

/// Builds a string parameter's storage cell, seeded with the default declared in [`CONFIGS`].
///
/// The seed is transient — registration passes the resolved default separately and
/// `RedisModule_LoadConfigs` overwrites the cell as soon as registration finishes — but taking
/// it from the registry means there is no second copy of the default to drift out of step.
fn default_string_cell(name: &str) -> ValkeyGILGuard<ValkeyString> {
    let default = find_config(name)
        .map(|desc| desc.default.as_registration_str())
        .unwrap_or(Cow::Borrowed(""));
    ValkeyGILGuard::new(ValkeyString::create(None, default.as_ref()))
}

static CHUNK_ENCODING_STRING: LazyLock<ValkeyGILGuard<ValkeyString>> =
    LazyLock::new(|| default_string_cell("ts-encoding"));
static DUPLICATE_POLICY_STRING: LazyLock<ValkeyGILGuard<ValkeyString>> =
    LazyLock::new(|| default_string_cell("ts-duplicate-policy"));
static RETENTION_POLICY_STRING: LazyLock<ValkeyGILGuard<ValkeyString>> =
    LazyLock::new(|| default_string_cell("ts-retention-policy"));
static COMPACTION_POLICY_STRING: LazyLock<ValkeyGILGuard<ValkeyString>> =
    LazyLock::new(|| default_string_cell("ts-compaction-policy"));
static IGNORE_MAX_TIME_DIFF_STRING: LazyLock<ValkeyGILGuard<ValkeyString>> =
    LazyLock::new(|| default_string_cell("ts-ignore-max-time-diff"));
static IGNORE_MAX_VALUE_DIFF_STRING: LazyLock<ValkeyGILGuard<ValkeyString>> =
    LazyLock::new(|| default_string_cell("ts-ignore-max-value-diff"));
static DECIMAL_DIGITS_STRING: LazyLock<ValkeyGILGuard<ValkeyString>> =
    LazyLock::new(|| default_string_cell("ts-decimal-digits"));
static SIGNIFICANT_DIGITS_STRING: LazyLock<ValkeyGILGuard<ValkeyString>> =
    LazyLock::new(|| default_string_cell("ts-significant-digits"));
static FANOUT_COMMAND_TIMEOUT_STRING: LazyLock<ValkeyGILGuard<ValkeyString>> =
    LazyLock::new(|| default_string_cell("ts-fanout-command-timeout"));
static CLUSTER_MAP_EXPIRATION_STRING: LazyLock<ValkeyGILGuard<ValkeyString>> =
    LazyLock::new(|| default_string_cell("ts-cluster-map-expiration-ms"));

/// Gate for the `TS._DEBUG` command surface (`debug-mode`, default off).
///
/// `TS._DEBUG` exposes internal state (the string-interner pool, the node-local postings
/// index, the configuration registry) that is useful for diagnostics but is not part of the
/// module's supported API, so it stays off unless an operator turns it on.
static IS_DEBUG_MODE: AtomicBool = AtomicBool::new(false);

pub fn is_debug_mode_enabled() -> bool {
    IS_DEBUG_MODE.load(Ordering::Relaxed)
}

/// Runtime toggle for shard-side aggregation push-down in MRANGE fanout and in
/// PromQL aggregation fanout (`ts-fanout-aggregation-pushdown`, default on).
/// Consulted by the coordinator only; shards obey the request flag.
///
/// This is NOT a mixed-version safety mechanism — the fanout compatibility
/// handshake (self-describing responses + envelope feature gate, see
/// `docs/fanout-compatibility-handshake.md`) makes version skew correct
/// automatically, so no config action is needed across a rolling upgrade.
///
/// Its remaining purpose is an emergency/diagnostic escape hatch for the
/// push-down code path itself: flipping it off at runtime routes every query
/// back through the older coordinator-side aggregation path without a module
/// rollback — useful to mitigate a latent push-down bug or a pathological
/// resource case, or to A/B isolate whether an issue lives in push-down.
pub static FANOUT_AGGREGATION_PUSHDOWN: AtomicBool = AtomicBool::new(true);

pub fn is_fanout_aggregation_pushdown_enabled() -> bool {
    FANOUT_AGGREGATION_PUSHDOWN.load(Ordering::Relaxed)
}

/// Runtime toggle for persisting the postings index as an RDB aux field
/// (`ts-index-persist`, default on; see docs/postings-index-persistence.md).
/// Gates both save and load: with it off, BGSAVE writes no aux payload and
/// load discards any payload found in the RDB (the payload is still consumed
/// to keep the RDB stream in sync) and rebuilds the index per key. Loading a
/// payload-bearing RDB is always tolerated regardless of this setting.
pub static INDEX_PERSIST: AtomicBool = AtomicBool::new(true);

pub fn is_index_persist_enabled() -> bool {
    INDEX_PERSIST.load(Ordering::Relaxed)
}

/// Cap on the transient buffer used by the sorted bulk index build during RDB/replication
/// loads (`ts-index-build-max-memory`, bytes, 0 = unlimited; default 256MiB). The buffer holds `(id, key, label-keys)`
/// tuples at exactly the moment the loading dataset's own footprint peaks, so it must be
/// bounded: crossing the cap drains the buffer with one sorted bulk build and degrades to
/// per-key indexing for the remainder of the load window (`bulk_build.rs`).
pub static INDEX_BUILD_MAX_MEMORY: AtomicI64 = AtomicI64::new(INDEX_BUILD_MAX_MEMORY_DEFAULT);

pub fn index_build_max_memory() -> i64 {
    INDEX_BUILD_MAX_MEMORY.load(Ordering::Relaxed)
}

fn parse_duration_in_range(name: &str, value: &str, min: i64, max: i64) -> ValkeyResult<i64> {
    let duration = parse_duration_value(value).map_err(|_e| {
        ValkeyError::String(format!(
            "error parsing \"{name}\". Expected duration, got {value}"
        ))
    })?;
    if duration < 0 {
        return Err(ValkeyError::String(format!(
            "Invalid duration value ({duration}) for \"{name}\". Must be positive",
        )));
    }
    if duration < min || duration > max {
        return Err(ValkeyError::String(format!(
            "Invalid value ({duration}) for \"{name}\". Must be in the range [{}, {}]",
            humanize_duration_ms(min),
            humanize_duration_ms(max),
        )));
    }
    Ok(duration)
}

fn validate_number_range(name: &str, value: f64, min: f64, max: f64) -> ValkeyResult<()> {
    if value < min || value > max {
        return Err(ValkeyError::String(format!(
            "Invalid value ({value}) for \"{name}\". Must be in the range [{min}, {max}]",
        )));
    }
    Ok(())
}

fn parse_number_in_range(name: &str, value: &str, min: f64, max: f64) -> ValkeyResult<f64> {
    let number = parse_number(value).map_err(|_e| {
        ValkeyError::String(format!(
            "error parsing \"{name}\". Expected number, got {value}"
        ))
    })?;
    validate_number_range(name, number, min, max)?;
    Ok(number)
}

fn update_duplicate_policy(val: &str) -> ValkeyResult<()> {
    let policy = DuplicatePolicy::try_from(val)
        .map_err(|_| ValkeyError::Str(error_consts::INVALID_DUPLICATE_POLICY))?;
    DUPLICATE_POLICY.store(policy.as_u8(), Ordering::SeqCst);
    Ok(())
}

fn update_chunk_encoding(val: &str) -> ValkeyResult<()> {
    let encoding = ChunkEncoding::try_from(val)
        .map_err(|_| ValkeyError::Str(error_consts::INVALID_CHUNK_ENCODING))?;
    CHUNK_ENCODING.store(encoding as u8, Ordering::SeqCst);
    Ok(())
}

fn update_compaction_policy(v: &str) -> ValkeyResult<()> {
    if v.is_empty() || v.eq_ignore_ascii_case(CONFIG_VALUE_NONE) {
        clear_compaction_policy_config();
        // Clearing must also clear the stored text, or the parameter keeps reporting the
        // policy that was just removed.
        *lock(&COMPACTION_POLICY) = String::new();
        return Ok(());
    }
    add_compaction_policies_from_config(v, true)?;
    *lock(&COMPACTION_POLICY) = v.to_string();

    Ok(())
}

fn update_retention_policy(val: &str) -> ValkeyResult<()> {
    let duration = parse_duration_in_range(
        "ts-retention-policy",
        val,
        RETENTION_POLICY_MIN,
        RETENTION_POLICY_MAX,
    )?;
    RETENTION_PERIOD_MS.store(duration.max(0) as u64, Ordering::SeqCst);
    Ok(())
}

fn update_ignore_max_time_diff(val: &str) -> ValkeyResult<()> {
    let duration = parse_duration_in_range(
        "ts-ignore-max-time-diff",
        val,
        IGNORE_MAX_TIME_DIFF_MIN,
        IGNORE_MAX_TIME_DIFF_MAX,
    )?;
    IGNORE_MAX_TIME_DIFF.store(duration, Ordering::SeqCst);
    Ok(())
}

fn update_ignore_max_value_diff(val: &str) -> ValkeyResult<()> {
    let value = parse_number_in_range(
        "ts-ignore-max-value-diff",
        val,
        IGNORE_MAX_VALUE_DIFF_MIN,
        IGNORE_MAX_VALUE_DIFF_MAX,
    )?;
    IGNORE_MAX_VALUE_DIFF.store(value.to_bits(), Ordering::SeqCst);
    Ok(())
}

fn update_fanout_command_timeout(val: &str) -> ValkeyResult<()> {
    let duration = parse_duration_in_range(
        "ts-fanout-command-timeout",
        val,
        FANOUT_COMMAND_TIMEOUT_MIN,
        FANOUT_COMMAND_TIMEOUT_MAX,
    )?;
    FANOUT_COMMAND_TIMEOUT.store(duration as u64, Ordering::SeqCst);
    Ok(())
}

fn update_cluster_map_expiration(val: &str) -> ValkeyResult<()> {
    let duration = parse_duration_in_range(
        "ts-cluster-map-expiration-ms",
        val,
        CLUSTER_MAP_EXPIRATION_MIN_MS,
        CLUSTER_MAP_EXPIRATION_MAX_MS,
    )?;
    CLUSTER_MAP_EXPIRATION_MS.store(duration as u64, Ordering::SeqCst);
    Ok(())
}

/// What a rounding set-request asks for.
///
/// `none` and `0` are different requests: `0` is a real digit count, not a way to switch
/// rounding off. Keeping them distinct is what lets `ts-decimal-digits 0` mean whole-number
/// rounding, matching the per-series `DECIMAL_DIGITS 0` argument.
enum RoundingRequest {
    Disable,
    Digits(u8),
}

/// Which of the two mutually exclusive rounding parameters a value belongs to.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum RoundingKind {
    Decimal,
    Significant,
}

impl RoundingKind {
    fn of(strategy: &RoundingStrategy) -> Self {
        match strategy {
            RoundingStrategy::DecimalDigits(_) => RoundingKind::Decimal,
            RoundingStrategy::SignificantDigits(_) => RoundingKind::Significant,
        }
    }

    fn build(self, digits: u8) -> RoundingStrategy {
        match self {
            RoundingKind::Decimal => RoundingStrategy::DecimalDigits(digits),
            RoundingKind::Significant => RoundingStrategy::SignificantDigits(digits),
        }
    }
}

/// Applies `ts-decimal-digits` or `ts-significant-digits`.
///
/// The two are mutually exclusive and share one store, so each may only touch rounding of its
/// own kind: `none` (or 0) disables *this* kind and leaves the other alone, and setting a
/// digit count while the other kind is active is rejected. Previously the disable path cleared
/// the store outright, so `CONFIG SET ts-decimal-digits none` silently switched off an active
/// significant-digits rounding. To switch kinds, disable the active one and then set the other.
///
/// Only `none` disables rounding; `0` is a digit count like any other, so
/// `ts-decimal-digits 0` rounds samples to whole numbers exactly as the per-series
/// `DECIMAL_DIGITS 0` argument does. `ts-significant-digits` has no meaningful zero and
/// rejects it — see [`MIN_SIGNIFICANT_DIGITS`].
///
/// Read-modify-write is safe without a lock: config set callbacks run on the main thread with
/// the module GIL held.
fn update_rounding(
    kind: RoundingKind,
    name: &str,
    val: &str,
    min: i64,
    max: i64,
) -> ValkeyResult<()> {
    let request = if val.eq_ignore_ascii_case(CONFIG_VALUE_NONE) {
        RoundingRequest::Disable
    } else {
        RoundingRequest::Digits(parse_number_in_range(name, val, min as f64, max as f64)? as u8)
    };

    let active_kind = rounding_strategy().as_ref().map(RoundingKind::of);

    match request {
        // Disabling only clears rounding of this kind; the other kind is left alone.
        RoundingRequest::Disable => {
            if active_kind == Some(kind) {
                store_rounding_strategy(None);
            }
        }
        RoundingRequest::Digits(digits) => {
            if let Some(active_kind) = active_kind
                && active_kind != kind
            {
                return Err(ValkeyError::String(
                    "Cannot set both ts-decimal-digits and ts-significant-digits".to_string(),
                ));
            }
            store_rounding_strategy(Some(kind.build(digits)));
        }
    }

    Ok(())
}

fn update_decimal_digits(val: &str) -> ValkeyResult<()> {
    update_rounding(
        RoundingKind::Decimal,
        "ts-decimal-digits",
        val,
        DECIMAL_DIGITS_MIN,
        DECIMAL_DIGITS_MAX,
    )
}

fn update_significant_digits(val: &str) -> ValkeyResult<()> {
    update_rounding(
        RoundingKind::Significant,
        "ts-significant-digits",
        val,
        SIGNIFICANT_DIGITS_MIN,
        SIGNIFICANT_DIGITS_MAX,
    )
}

// Readers for the `read` hook of each registry entry. Each one reports from the parameter's
// typed store, which is the value the module actually acts on.

fn read_chunk_size() -> ConfigValue {
    ConfigValue::Integer(chunk_size_bytes() as i64)
}

fn read_chunk_encoding() -> ConfigValue {
    ConfigValue::str(chunk_encoding().name())
}

fn read_duplicate_policy() -> ConfigValue {
    ConfigValue::str(duplicate_policy().as_str())
}

fn read_retention_policy() -> ConfigValue {
    let period = retention_period();
    if period.is_zero() {
        ConfigValue::none()
    } else {
        ConfigValue::DurationMs(period.as_millis() as i64)
    }
}

fn read_compaction_policy() -> ConfigValue {
    ConfigValue::String(Cow::Owned(lock(&COMPACTION_POLICY).clone()))
}

fn read_decimal_digits() -> ConfigValue {
    match rounding_strategy() {
        Some(RoundingStrategy::DecimalDigits(digits)) => ConfigValue::Integer(digits as i64),
        _ => ConfigValue::none(),
    }
}

fn read_significant_digits() -> ConfigValue {
    match rounding_strategy() {
        Some(RoundingStrategy::SignificantDigits(digits)) => ConfigValue::Integer(digits as i64),
        _ => ConfigValue::none(),
    }
}

fn read_ignore_max_time_diff() -> ConfigValue {
    ConfigValue::DurationMs(ignore_max_time_diff() as i64)
}

fn read_ignore_max_value_diff() -> ConfigValue {
    ConfigValue::Float(ignore_max_value_diff())
}

fn read_num_threads() -> ConfigValue {
    ConfigValue::Integer(NUM_THREADS.load(Ordering::Relaxed))
}

fn read_fanout_command_timeout() -> ConfigValue {
    ConfigValue::DurationMs(FANOUT_COMMAND_TIMEOUT.load(Ordering::Relaxed) as i64)
}

fn read_cluster_map_expiration() -> ConfigValue {
    ConfigValue::DurationMs(CLUSTER_MAP_EXPIRATION_MS.load(Ordering::Relaxed) as i64)
}

fn read_index_build_max_memory() -> ConfigValue {
    ConfigValue::Integer(index_build_max_memory())
}

fn read_fanout_aggregation_pushdown() -> ConfigValue {
    ConfigValue::Boolean(is_fanout_aggregation_pushdown_enabled())
}

fn read_index_persist() -> ConfigValue {
    ConfigValue::Boolean(is_index_persist_enabled())
}

fn read_debug_mode() -> ConfigValue {
    ConfigValue::Boolean(is_debug_mode_enabled())
}

/// Constraint on `ts-chunk-size` that the server's own numeric range check cannot express:
/// the size must additionally be a multiple of 8.
fn validate_chunk_size_config(chunk_size: i64) -> ValkeyResult<()> {
    validate_chunk_size(chunk_size as usize)?;
    Ok(())
}

fn get_string_default<'a>(
    args: &'a [ValkeyString],
    name: &str,
    default: &'a str,
) -> ValkeyResult<&'a str> {
    match get_string_default_config_value(args, name, default) {
        Ok(v) => Ok(v),
        Err(e) => {
            let msg = format!("Error getting default string config value for {name}: {e}");
            log_warning(&msg);
            Err(ValkeyError::String(msg))
        }
    }
}

fn get_i64_default(args: &[ValkeyString], name: &str, default: i64) -> ValkeyResult<i64> {
    match get_i64_default_config_value(args, name, default) {
        Ok(v) => Ok(v),
        Err(e) => {
            let msg = format!("Error getting default config value for {name}: {e}");
            log_warning(&msg);
            Err(ValkeyError::String(msg))
        }
    }
}

/// The registry of every configuration parameter the module exposes.
///
/// This is the single source of truth. `register_config` registers each entry with the
/// server, the set-callback it installs dispatches straight to that entry's `apply`/`validate`
/// hook (so there is no name-matching step that can fall through to an unhandled parameter),
/// and `TS._DEBUG LIST_CONFIGS` reports name/type/default/bounds/description from here.
pub static CONFIGS: &[ConfigDesc] = &[
    ConfigDesc {
        name: "ts-chunk-size",
        read: read_chunk_size,
        kind: ConfigType::Integer,
        default: ConfigValue::Integer(CHUNK_SIZE_DEFAULT),
        min: Some(ConfigValue::Integer(CHUNK_SIZE_MIN)),
        max: Some(ConfigValue::Integer(CHUNK_SIZE_MAX)),
        flags: ConfigurationFlags::DEFAULT,
        description: "Maximum memory used for each time series chunk in bytes",
        storage: ConfigStorage::I64 {
            cell: || &CHUNK_SIZE,
            validate: Some(validate_chunk_size_config),
        },
    },
    ConfigDesc {
        name: "ts-encoding",
        read: read_chunk_encoding,
        kind: ConfigType::Enum,
        default: ConfigValue::str(DEFAULT_CHUNK_ENCODING.name()),
        min: None,
        max: None,
        flags: ConfigurationFlags::DEFAULT,
        description: "Default chunk encoding: GORILLA or UNCOMPRESSED",
        storage: ConfigStorage::Str {
            cell: || &CHUNK_ENCODING_STRING,
            apply: update_chunk_encoding,
        },
    },
    ConfigDesc {
        name: "ts-duplicate-policy",
        read: read_duplicate_policy,
        kind: ConfigType::Enum,
        default: ConfigValue::str(DEFAULT_DUPLICATE_POLICY.as_str()),
        min: None,
        max: None,
        flags: ConfigurationFlags::DEFAULT,
        description: "Policy for handling duplicate samples: BLOCK, FIRST, LAST, MIN, MAX, SUM",
        storage: ConfigStorage::Str {
            cell: || &DUPLICATE_POLICY_STRING,
            apply: update_duplicate_policy,
        },
    },
    ConfigDesc {
        name: "ts-retention-policy",
        read: read_retention_policy,
        kind: ConfigType::Duration,
        default: ConfigValue::DurationMs(RETENTION_POLICY_MIN),
        min: Some(ConfigValue::DurationMs(RETENTION_POLICY_MIN)),
        max: Some(ConfigValue::DurationMs(RETENTION_POLICY_MAX)),
        flags: ConfigurationFlags::DEFAULT,
        description: "Default retention period in milliseconds (0 = no expiry)",
        storage: ConfigStorage::Str {
            cell: || &RETENTION_POLICY_STRING,
            apply: update_retention_policy,
        },
    },
    ConfigDesc {
        name: "ts-compaction-policy",
        read: read_compaction_policy,
        kind: ConfigType::String,
        default: ConfigValue::str(DEFAULT_COMPACTION_POLICY),
        min: None,
        max: None,
        flags: ConfigurationFlags::DEFAULT,
        description: "Default compaction rules applied to all new time series",
        storage: ConfigStorage::Str {
            cell: || &COMPACTION_POLICY_STRING,
            apply: update_compaction_policy,
        },
    },
    ConfigDesc {
        name: "ts-decimal-digits",
        read: read_decimal_digits,
        kind: ConfigType::Integer,
        // The default is the "disabled" sentinel, not a digit count: rounding is off unless
        // this or `ts-significant-digits` is set.
        default: ConfigValue::none(),
        min: Some(ConfigValue::Integer(DECIMAL_DIGITS_MIN)),
        max: Some(ConfigValue::Integer(DECIMAL_DIGITS_MAX)),
        flags: ConfigurationFlags::DEFAULT,
        description: "Round sample values to this many decimal places (none = disabled, \
                      0 = round to whole numbers; mutually exclusive with \
                      ts-significant-digits)",
        storage: ConfigStorage::Str {
            cell: || &DECIMAL_DIGITS_STRING,
            apply: update_decimal_digits,
        },
    },
    ConfigDesc {
        name: "ts-significant-digits",
        read: read_significant_digits,
        kind: ConfigType::Integer,
        default: ConfigValue::none(),
        min: Some(ConfigValue::Integer(SIGNIFICANT_DIGITS_MIN)),
        max: Some(ConfigValue::Integer(SIGNIFICANT_DIGITS_MAX)),
        flags: ConfigurationFlags::DEFAULT,
        description: "Round sample values to this many significant digits (none = disabled; \
                      16 is accepted but rounds nothing; mutually exclusive with \
                      ts-decimal-digits)",
        storage: ConfigStorage::Str {
            cell: || &SIGNIFICANT_DIGITS_STRING,
            apply: update_significant_digits,
        },
    },
    ConfigDesc {
        name: "ts-ignore-max-time-diff",
        read: read_ignore_max_time_diff,
        kind: ConfigType::Duration,
        default: ConfigValue::DurationMs(IGNORE_MAX_TIME_DIFF_DEFAULT),
        min: Some(ConfigValue::DurationMs(IGNORE_MAX_TIME_DIFF_MIN)),
        max: Some(ConfigValue::DurationMs(IGNORE_MAX_TIME_DIFF_MAX)),
        flags: ConfigurationFlags::DEFAULT,
        description: "Max time delta (ms) for which a duplicate sample is ignored",
        storage: ConfigStorage::Str {
            cell: || &IGNORE_MAX_TIME_DIFF_STRING,
            apply: update_ignore_max_time_diff,
        },
    },
    ConfigDesc {
        name: "ts-ignore-max-value-diff",
        read: read_ignore_max_value_diff,
        kind: ConfigType::Float,
        default: ConfigValue::Float(IGNORE_MAX_VALUE_DIFF_MIN),
        min: Some(ConfigValue::Float(IGNORE_MAX_VALUE_DIFF_MIN)),
        max: Some(ConfigValue::Float(IGNORE_MAX_VALUE_DIFF_MAX)),
        flags: ConfigurationFlags::DEFAULT,
        description: "Max value delta for which a duplicate sample is ignored",
        storage: ConfigStorage::Str {
            cell: || &IGNORE_MAX_VALUE_DIFF_STRING,
            apply: update_ignore_max_value_diff,
        },
    },
    ConfigDesc {
        name: "ts-num-threads",
        read: read_num_threads,
        kind: ConfigType::Integer,
        default: ConfigValue::Integer(DEFAULT_THREADS),
        min: Some(ConfigValue::Integer(MIN_THREADS)),
        max: Some(ConfigValue::Integer(MAX_THREADS)),
        // Rayon's global thread pool cannot be resized after `build_global()`, so this can
        // only be set at startup; runtime `CONFIG SET` is rejected by the server itself.
        flags: ConfigurationFlags::IMMUTABLE,
        description: "Number of worker threads for parallel query processing",
        storage: ConfigStorage::I64 {
            cell: || &NUM_THREADS,
            validate: None,
        },
    },
    ConfigDesc {
        name: "ts-fanout-command-timeout",
        read: read_fanout_command_timeout,
        kind: ConfigType::Duration,
        default: ConfigValue::DurationMs(DEFAULT_FANOUT_COMMAND_TIMEOUT_MS as i64),
        min: Some(ConfigValue::DurationMs(FANOUT_COMMAND_TIMEOUT_MIN)),
        max: Some(ConfigValue::DurationMs(FANOUT_COMMAND_TIMEOUT_MAX)),
        flags: ConfigurationFlags::DEFAULT,
        description: "Timeout in milliseconds for fanout (cluster scatter/gather) commands",
        storage: ConfigStorage::Str {
            cell: || &FANOUT_COMMAND_TIMEOUT_STRING,
            apply: update_fanout_command_timeout,
        },
    },
    ConfigDesc {
        name: "ts-cluster-map-expiration-ms",
        read: read_cluster_map_expiration,
        kind: ConfigType::Duration,
        default: ConfigValue::DurationMs(CLUSTER_MAP_EXPIRATION_MS_DEFAULT as i64),
        min: Some(ConfigValue::DurationMs(CLUSTER_MAP_EXPIRATION_MIN_MS)),
        max: Some(ConfigValue::DurationMs(CLUSTER_MAP_EXPIRATION_MAX_MS)),
        flags: ConfigurationFlags::DEFAULT,
        description: "How long (ms) cluster slot-map entries are cached (0 = no cache)",
        storage: ConfigStorage::Str {
            cell: || &CLUSTER_MAP_EXPIRATION_STRING,
            apply: update_cluster_map_expiration,
        },
    },
    ConfigDesc {
        name: "ts-index-build-max-memory",
        read: read_index_build_max_memory,
        kind: ConfigType::Integer,
        default: ConfigValue::Integer(INDEX_BUILD_MAX_MEMORY_DEFAULT),
        min: Some(ConfigValue::Integer(INDEX_BUILD_MAX_MEMORY_MIN)),
        max: Some(ConfigValue::Integer(INDEX_BUILD_MAX_MEMORY_MAX)),
        flags: ConfigurationFlags::DEFAULT,
        description: "Cap in bytes on the transient buffer used by the bulk index build during \
                      RDB/replication load (0 = unlimited)",
        storage: ConfigStorage::I64 {
            cell: || &INDEX_BUILD_MAX_MEMORY,
            validate: None,
        },
    },
    ConfigDesc {
        name: "ts-fanout-aggregation-pushdown",
        read: read_fanout_aggregation_pushdown,
        kind: ConfigType::Boolean,
        default: ConfigValue::Boolean(true),
        min: None,
        max: None,
        flags: ConfigurationFlags::DEFAULT,
        description: "Push MRANGE aggregation down to shards during cluster fanout",
        storage: ConfigStorage::Bool {
            cell: || &FANOUT_AGGREGATION_PUSHDOWN,
        },
    },
    ConfigDesc {
        name: "ts-index-persist",
        read: read_index_persist,
        kind: ConfigType::Boolean,
        default: ConfigValue::Boolean(true),
        min: None,
        max: None,
        flags: ConfigurationFlags::DEFAULT,
        description: "Persist the postings index as an RDB aux field instead of rebuilding it on load",
        storage: ConfigStorage::Bool {
            cell: || &INDEX_PERSIST,
        },
    },
    ConfigDesc {
        name: "debug-mode",
        read: read_debug_mode,
        kind: ConfigType::Boolean,
        default: ConfigValue::Boolean(false),
        min: None,
        max: None,
        flags: ConfigurationFlags::DEFAULT,
        description: "Enable the TS._DEBUG command surface (disabled by default)",
        storage: ConfigStorage::Bool {
            cell: || &IS_DEBUG_MODE,
        },
    },
];

/// Looks up a parameter by name. Names are matched case-insensitively, as the server does.
pub fn find_config(name: &str) -> Option<&'static ConfigDesc> {
    CONFIGS
        .iter()
        .find(|desc| desc.name.eq_ignore_ascii_case(name))
}

fn registry_error(name: &str, what: &str) -> ValkeyError {
    ValkeyError::String(format!(
        "internal error: configuration \"{name}\" has no valid {what} in the registry"
    ))
}

/// Resolves the i64 bound `bound` for parameter `name`, which the registry must supply.
fn required_i64(bound: Option<&ConfigValue>, name: &str, what: &str) -> ValkeyResult<i64> {
    bound
        .and_then(|v| v.as_i64())
        .ok_or_else(|| registry_error(name, what))
}

/// Records an accepted configuration change.
///
/// Every parameter logs the same way, which is what the removed `config_changed_event_handler`
/// used to provide (it dumped the whole cached struct on any change).
fn log_config_set(name: &str, value: &str) {
    log_notice(format!("Setting {name} to {value}"));
}

fn register_string_param(
    ctx: &Context,
    args: &[ValkeyString],
    desc: &'static ConfigDesc,
    cell: fn() -> &'static ValkeyGILGuard<ValkeyString>,
    apply: fn(&str) -> ValkeyResult<()>,
) -> ValkeyResult<()> {
    let fallback = desc.default.as_registration_str();
    let default = get_string_default(args, desc.name, &fallback)?;

    // Applying the resolved default here would be redundant: `RedisModule_LoadConfigs` sets
    // every parameter once registration completes, which runs `apply` with this same value.
    register_string_configuration::<ValkeyGILGuard<ValkeyString>>(
        ctx,
        desc.name,
        cell(),
        default,
        desc.flags(),
        None,
        Some(Box::new(
            move |config_ctx: &ConfigurationContext,
                  name: &str,
                  val: &'static ValkeyGILGuard<ValkeyString>| {
                let raw = val.get(config_ctx).to_string_lossy();
                apply(&raw)?;
                log_config_set(name, &raw);
                Ok(())
            },
        )),
    );
    Ok(())
}

fn register_i64_param(
    ctx: &Context,
    args: &[ValkeyString],
    desc: &'static ConfigDesc,
    cell: fn() -> &'static AtomicI64,
    validate: Option<fn(i64) -> ValkeyResult<()>>,
) -> ValkeyResult<()> {
    let fallback = required_i64(Some(&desc.default), desc.name, "default")?;
    let min = required_i64(desc.min.as_ref(), desc.name, "minimum")?;
    let max = required_i64(desc.max.as_ref(), desc.name, "maximum")?;
    let default = get_i64_default(args, desc.name, fallback)?;

    // Surface a bad startup default as a warning rather than a failed module load, matching
    // the behaviour operators already rely on: the server applies it regardless, and the
    // parameter can still be corrected at runtime.
    if let Some(validate) = validate
        && let Err(e) = validate(default)
    {
        log_warning(format!(
            "Error validating default value ({default}) for \"{}\": {e}. Please correct this configuration parameter.",
            desc.name
        ));
    }

    register_i64_configuration(
        ctx,
        desc.name,
        cell(),
        default,
        min,
        max,
        desc.flags(),
        None,
        Some(Box::new(
            move |_config_ctx: &ConfigurationContext, name: &str, atomic: &'static AtomicI64| {
                let value = atomic.load(Ordering::SeqCst);
                if let Some(validate) = validate {
                    validate(value)?;
                }
                log_config_set(name, &value.to_string());
                Ok(())
            },
        )),
    );
    Ok(())
}

fn register_bool_param(
    ctx: &Context,
    args: &[ValkeyString],
    desc: &'static ConfigDesc,
    cell: fn() -> &'static AtomicBool,
) -> ValkeyResult<()> {
    let fallback = desc
        .default
        .as_bool()
        .ok_or_else(|| registry_error(desc.name, "default"))?;
    let default = get_bool_default_config_value(args, desc.name, fallback)?;

    register_bool_configuration(
        ctx,
        desc.name,
        cell(),
        default,
        desc.flags(),
        None,
        Some(Box::new(
            move |config_ctx: &ConfigurationContext, name: &str, val: &'static AtomicBool| {
                log_config_set(name, if val.get(config_ctx) { "yes" } else { "no" });
                Ok(())
            },
        )),
    );
    Ok(())
}

pub(super) fn register_config(ctx: &Context, args: &[ValkeyString]) -> ValkeyResult<()> {
    for desc in CONFIGS {
        match desc.storage {
            ConfigStorage::Str { cell, apply } => {
                register_string_param(ctx, args, desc, cell, apply)?
            }
            ConfigStorage::I64 { cell, validate } => {
                register_i64_param(ctx, args, desc, cell, validate)?
            }
            ConfigStorage::Bool { cell } => register_bool_param(ctx, args, desc, cell)?,
        }
    }

    // Apply the resolved values (from `valkey.conf` / `MODULE LOAD` args, or the registered
    // defaults). This runs each parameter's set path, so it is where a startup value that the
    // module itself rejects surfaces — for example `ts-decimal-digits` and
    // `ts-significant-digits` both set, which the mutual-exclusion check refuses.
    //
    // If applying the startup configuration fails, module initialization fails too (we return Err).
    // Log a warning so operators can see why the module did not start.
    let status = unsafe { RedisModule_LoadConfigs.unwrap()(ctx.ctx) };
    if status != raw::REDISMODULE_OK as c_int {
        log_warning(
            "Failed to apply the startup configuration: one or more timeseries parameters were \
             rejected (see the preceding errors). The module cannot start.",
        );
        return Err(ValkeyError::Str(
            "TSDB: invalid startup configuration; see the preceding errors in the server log",
        ));
    }

    // Initialize PROMQL_CONFIG from the freshly loaded Valkey config
    if let Ok(mut prom_guard) = PROMQL_CONFIG.write() {
        prom_guard.apply_ts_config(is_debug_mode_enabled());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The exact default string each parameter is registered with. Registration defaults are
    /// user-visible (`CONFIG GET` reports them on a fresh server), so the registry must
    /// reproduce them byte for byte and a change here is a change to documented behaviour.
    const EXPECTED_REGISTRATION_DEFAULTS: &[(&str, &str)] = &[
        ("ts-chunk-size", "4096"),
        ("ts-encoding", "chimp"),
        // Lowercase: this is what the server registers and what `CONFIG GET` returns.
        // `TS._DEBUG LIST_CONFIGS` reports the same value, having previously hardcoded "BLOCK".
        ("ts-duplicate-policy", "block"),
        ("ts-retention-policy", "0"),
        ("ts-compaction-policy", ""),
        ("ts-decimal-digits", "none"),
        ("ts-significant-digits", "none"),
        ("ts-ignore-max-time-diff", "0"),
        ("ts-ignore-max-value-diff", "0"),
        ("ts-num-threads", "4"),
        ("ts-fanout-command-timeout", "5000"),
        ("ts-cluster-map-expiration-ms", "750"),
        ("ts-index-build-max-memory", "268435456"),
        ("ts-fanout-aggregation-pushdown", "yes"),
        ("ts-index-persist", "yes"),
        ("debug-mode", "no"),
    ];

    #[test]
    fn registration_defaults_are_unchanged() {
        for (name, expected) in EXPECTED_REGISTRATION_DEFAULTS {
            let desc = find_config(name).unwrap_or_else(|| panic!("{name} is not registered"));
            assert_eq!(
                desc.default.as_registration_str(),
                *expected,
                "registration default for {name} changed"
            );
        }
        assert_eq!(
            CONFIGS.len(),
            EXPECTED_REGISTRATION_DEFAULTS.len(),
            "a parameter was added or removed without updating this test"
        );
    }

    #[test]
    fn names_are_unique() {
        for (i, desc) in CONFIGS.iter().enumerate() {
            assert!(
                CONFIGS[..i]
                    .iter()
                    .all(|prior| !prior.name.eq_ignore_ascii_case(desc.name)),
                "duplicate configuration name: {}",
                desc.name
            );
        }
    }

    /// Numeric parameters are registered directly with the server, which needs a concrete
    /// default, min and max — a missing or wrongly-typed bound would fail module load.
    #[test]
    fn numeric_params_have_usable_bounds() {
        for desc in CONFIGS {
            let ConfigStorage::I64 { .. } = desc.storage else {
                continue;
            };
            let default = required_i64(Some(&desc.default), desc.name, "default").unwrap();
            let min = required_i64(desc.min.as_ref(), desc.name, "minimum").unwrap();
            let max = required_i64(desc.max.as_ref(), desc.name, "maximum").unwrap();
            assert!(min <= max, "{}: min exceeds max", desc.name);
            assert!(
                (min..=max).contains(&default),
                "{}: default {default} is outside [{min}, {max}]",
                desc.name
            );
        }
    }

    #[test]
    fn boolean_params_have_boolean_defaults() {
        for desc in CONFIGS {
            let ConfigStorage::Bool { .. } = desc.storage else {
                continue;
            };
            assert!(
                desc.default.as_bool().is_some(),
                "{}: default is not a boolean",
                desc.name
            );
            assert_eq!(desc.kind, ConfigType::Boolean, "{}: wrong kind", desc.name);
        }
    }

    /// Every parameter must document itself; `TS._DEBUG LIST_CONFIGS` reports the description.
    #[test]
    fn descriptions_are_present() {
        for desc in CONFIGS {
            assert!(
                !desc.description.trim().is_empty(),
                "{}: missing description",
                desc.name
            );
        }
    }

    /// Whether `value` is a variant the parameter's declared type allows. The "none" sentinel
    /// is always allowed: it is how an unset or disabled parameter reports itself.
    fn matches_kind(value: &ConfigValue, kind: ConfigType) -> bool {
        if *value == ConfigValue::none() {
            return true;
        }
        match kind {
            ConfigType::Integer => matches!(value, ConfigValue::Integer(_)),
            ConfigType::Float => matches!(value, ConfigValue::Float(_)),
            ConfigType::Boolean => matches!(value, ConfigValue::Boolean(_)),
            ConfigType::String | ConfigType::Enum => matches!(value, ConfigValue::String(_)),
            ConfigType::Duration => matches!(value, ConfigValue::DurationMs(_)),
        }
    }

    /// The reported value must agree with the declared type, or clients cannot rely on the
    /// `type` field of `TS._DEBUG LIST_CONFIGS`.
    #[test]
    fn read_values_match_declared_kind() {
        for desc in CONFIGS {
            let value = (desc.read)();
            assert!(
                matches_kind(&value, desc.kind),
                "{}: value {value:?} does not match declared type {:?}",
                desc.name,
                desc.kind
            );
        }
    }

    #[test]
    fn defaults_and_bounds_match_declared_kind() {
        for desc in CONFIGS {
            assert!(
                matches_kind(&desc.default, desc.kind),
                "{}: default {:?} does not match declared type {:?}",
                desc.name,
                desc.default,
                desc.kind
            );
            for (label, bound) in [("min", &desc.min), ("max", &desc.max)] {
                if let Some(bound) = bound {
                    assert!(
                        matches_kind(bound, desc.kind),
                        "{}: {label} {bound:?} does not match declared type {:?}",
                        desc.name,
                        desc.kind
                    );
                }
            }
        }
    }

    /// The packed rounding representation must survive a round trip for every kind and every
    /// digit count the parameters accept, or a `CONFIG SET` would silently change the value.
    #[test]
    fn rounding_strategy_encoding_round_trips() {
        let max = DECIMAL_DIGITS_MAX.max(SIGNIFICANT_DIGITS_MAX) as u8;
        let mut cases = vec![None];
        for digits in 0..=max {
            cases.push(Some(RoundingStrategy::DecimalDigits(digits)));
            cases.push(Some(RoundingStrategy::SignificantDigits(digits)));
        }

        for strategy in cases {
            let packed = encode_rounding_strategy(strategy);
            assert_eq!(
                decode_rounding_strategy(packed),
                strategy,
                "round trip failed for {strategy:?}"
            );
        }
    }

    /// The two kinds must never collide in the packed form: that is the invariant the single
    /// store exists to enforce.
    #[test]
    fn rounding_kinds_encode_distinctly() {
        for digits in 0..=18u8 {
            assert_ne!(
                encode_rounding_strategy(Some(RoundingStrategy::DecimalDigits(digits))),
                encode_rounding_strategy(Some(RoundingStrategy::SignificantDigits(digits))),
            );
            assert_ne!(
                encode_rounding_strategy(Some(RoundingStrategy::DecimalDigits(digits))),
                encode_rounding_strategy(None),
            );
        }
    }

    #[test]
    fn duplicate_policy_discriminants_round_trip() {
        for policy in [
            DuplicatePolicy::Block,
            DuplicatePolicy::KeepFirst,
            DuplicatePolicy::KeepLast,
            DuplicatePolicy::Min,
            DuplicatePolicy::Max,
            DuplicatePolicy::Sum,
        ] {
            assert_eq!(DuplicatePolicy::from_u8(policy.as_u8()), Some(policy));
        }
        assert_eq!(DuplicatePolicy::from_u8(6), None);
    }

    #[test]
    fn chunk_encoding_discriminants_round_trip() {
        for encoding in [
            ChunkEncoding::Uncompressed,
            ChunkEncoding::Gorilla,
            ChunkEncoding::Chimp,
        ] {
            assert_eq!(ChunkEncoding::try_from(encoding as u8).ok(), Some(encoding));
        }
    }

    /// The module-wide rounding defaults and the per-series `DECIMAL_DIGITS` /
    /// `SIGNIFICANT_DIGITS` command arguments must accept the same range.
    #[test]
    fn rounding_bounds_match_the_per_series_command_bounds() {
        assert_eq!(DECIMAL_DIGITS_MAX, MAX_DECIMAL_DIGITS as i64);
        assert_eq!(SIGNIFICANT_DIGITS_MAX, MAX_SIGNIFICANT_DIGITS as i64);

        // A digit count the config accepts must survive the packed representation.
        for digits in DECIMAL_DIGITS_MIN..=DECIMAL_DIGITS_MAX {
            let strategy = Some(RoundingStrategy::DecimalDigits(digits as u8));
            assert_eq!(
                decode_rounding_strategy(encode_rounding_strategy(strategy)),
                strategy
            );
        }
    }

    /// Serializes the tests that mutate the module-level configuration stores, which are
    /// process-wide and would otherwise interfere when tests run in parallel.
    static CONFIG_TEST_LOCK: Mutex<()> = Mutex::new(());

    fn config_test_lock() -> std::sync::MutexGuard<'static, ()> {
        CONFIG_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// A parameter's declared default must be a value that parameter actually accepts.
    ///
    /// This is what ties the registry's metadata to its behaviour: the default is what the
    /// server registers and what `RedisModule_LoadConfigs` feeds straight back into `apply`
    /// at startup, so a default outside the parameter's own range, or a mistyped enum name,
    /// would fail on every module load.
    #[test]
    fn declared_defaults_are_accepted_by_their_own_parser() {
        let _guard = config_test_lock();
        store_rounding_strategy(None);

        for desc in CONFIGS {
            let ConfigStorage::Str { apply, .. } = desc.storage else {
                continue;
            };
            let default = desc.default.as_registration_str();

            apply(&default).unwrap_or_else(|e| {
                panic!(
                    "{}: declared default {default:?} is rejected by its own parser: {e}",
                    desc.name
                )
            });

            // The parameter must now report the value it was just set to. Retention is the one
            // exception: zero means "no expiry", which reports as the "none" sentinel.
            let value = (desc.read)();
            assert!(
                value == desc.default || value == ConfigValue::none(),
                "{}: default {:?} applied but reads back as {value:?}",
                desc.name,
                desc.default
            );
        }

        store_rounding_strategy(None);
    }

    /// `ts-decimal-digits` and `ts-significant-digits` share one store. Each may only disable
    /// its own kind, and neither may override the other while it is active.
    #[test]
    fn rounding_parameters_are_mutually_exclusive() {
        let _guard = config_test_lock();
        let restore = rounding_strategy();

        store_rounding_strategy(None);
        update_decimal_digits("3").unwrap();
        assert_eq!(
            rounding_strategy(),
            Some(RoundingStrategy::DecimalDigits(3))
        );

        // The other kind cannot take over while this one is active.
        assert!(update_significant_digits("5").is_err());
        assert_eq!(
            rounding_strategy(),
            Some(RoundingStrategy::DecimalDigits(3))
        );

        // Disabling the inactive kind must leave the active one alone.
        update_significant_digits("none").unwrap();
        assert_eq!(
            rounding_strategy(),
            Some(RoundingStrategy::DecimalDigits(3))
        );

        // A parameter may always be re-set to a new value of its own kind.
        update_decimal_digits("7").unwrap();
        assert_eq!(
            rounding_strategy(),
            Some(RoundingStrategy::DecimalDigits(7))
        );

        // Disabling the active kind clears rounding and frees the other kind.
        update_decimal_digits("none").unwrap();
        assert_eq!(rounding_strategy(), None);
        update_significant_digits("5").unwrap();
        assert_eq!(
            rounding_strategy(),
            Some(RoundingStrategy::SignificantDigits(5))
        );

        store_rounding_strategy(restore);
    }

    /// Only `none` disables rounding. `0` is an ordinary digit count, so `ts-decimal-digits 0`
    /// rounds to whole numbers exactly as the per-series `DECIMAL_DIGITS 0` argument does.
    #[test]
    fn zero_decimal_digits_rounds_to_whole_numbers() {
        let _guard = config_test_lock();
        let restore = rounding_strategy();
        store_rounding_strategy(None);

        update_decimal_digits("0").unwrap();
        assert_eq!(
            rounding_strategy(),
            Some(RoundingStrategy::DecimalDigits(0))
        );
        assert_eq!(rounding_strategy().unwrap().round(3.7), 4.0);
        // It reports as a digit count, not as the "disabled" sentinel.
        assert_eq!(read_decimal_digits(), ConfigValue::Integer(0));

        update_decimal_digits(CONFIG_VALUE_NONE).unwrap();
        assert_eq!(rounding_strategy(), None);
        assert_eq!(read_decimal_digits(), ConfigValue::none());

        store_rounding_strategy(restore);
    }

    /// Zero is rejected for significant digits rather than stored as a strategy that rounds
    /// nothing: unlike decimal places, "zero significant digits" is not a quantity.
    #[test]
    fn zero_significant_digits_is_rejected() {
        let _guard = config_test_lock();
        let restore = rounding_strategy();
        store_rounding_strategy(None);

        assert!(update_significant_digits("0").is_err());
        assert_eq!(rounding_strategy(), None);

        update_significant_digits(&SIGNIFICANT_DIGITS_MIN.to_string()).unwrap();
        assert_eq!(
            rounding_strategy(),
            Some(RoundingStrategy::SignificantDigits(
                SIGNIFICANT_DIGITS_MIN as u8
            ))
        );

        store_rounding_strategy(restore);
    }

    /// `ts-decimal-digits 0` now activates rounding, so it takes part in the mutual-exclusion
    /// rule that it used to bypass by being a synonym for `none`.
    #[test]
    fn zero_decimal_digits_conflicts_with_active_significant_digits() {
        let _guard = config_test_lock();
        let restore = rounding_strategy();

        store_rounding_strategy(Some(RoundingStrategy::SignificantDigits(5)));
        assert!(update_decimal_digits("0").is_err());
        assert_eq!(
            rounding_strategy(),
            Some(RoundingStrategy::SignificantDigits(5))
        );

        store_rounding_strategy(restore);
    }

    /// `ts-num-threads` is the one parameter that cannot change after startup, because rayon's
    /// global pool cannot be resized once built.
    #[test]
    fn only_num_threads_is_immutable() {
        for desc in CONFIGS {
            assert_eq!(
                desc.is_mutable(),
                desc.name != "ts-num-threads",
                "{}: unexpected mutability",
                desc.name
            );
        }
    }
}
