pub const CANNOT_ADD_SAMPLE: &str = "TSDB: could not add sample";
pub const CHUNK_SPLIT: &str = "TSDB: could not split chunk";
pub const CAPACITY_FULL: &str = "TSDB: chunk at full capacity";
pub const CHUNK_COMPRESSION: &str = "TSDB: cannot compress chunk";
pub const CHUNK_DECOMPRESSION: &str = "TSDB: cannot decompress chunk";
pub const DUPLICATE_LABEL: &str = "TSDB: duplicate label";
pub const DUPLICATE_LABELS: &str = "TSDB: duplicate labels";
// Exact RTS text (compat finding #8): emitted for every duplicate-blocked
// upsert (TS.ADD and per-item TS.MADD), regardless of which clause applies.
pub const DUPLICATE_SAMPLE: &str = "TSDB: Error at upsert, update is not supported when DUPLICATE_POLICY is set to BLOCK mode, or either current or new value is NaN and DUPLICATE_POLICY is MAX/MIN/SUM";
pub const DUPLICATE_UPSERT_SAMPLE: &str = "TSDB: duplicate sample at upsert";
pub const SAMPLE_TOO_CLOSE: &str = "TSDB: sample too close to previous in value or timestamp";
pub const DUPLICATE_SERIES: &str = "TSDB: duplicate series";
pub const SAMPLE_MERGE_ERROR: &str = "TSDB: error merging samples";
pub const ERROR_FETCHING_SAMPLE: &str = "TSDB: fetching sample";
pub const INTERNAL_ERROR: &str = "TSDB: internal error";
pub const INVALID_ALIGN: &str = "TSDB: unknown ALIGN parameter";
pub const ALIGN_REQUIRES_AGGREGATION: &str =
    "TSDB: ALIGN parameter can only be used with AGGREGATION";
pub const START_ALIGN_NEEDS_EXPLICIT_START: &str =
    "TSDB: start alignment can only be used with explicit start timestamp";
pub const END_ALIGN_NEEDS_EXPLICIT_END: &str =
    "TSDB: end alignment can only be used with explicit end timestamp";
pub const INVALID_ARGUMENT: &str = "TSDB: invalid argument";
pub const INVALID_VALUE: &str = "TSDB: invalid value";
// Both verbatim RTS text for the counter commands: the first for an unusable
// increment *operand* (unparseable, empty, NaN), the second for incrementing a
// series whose last stored sample is NaN.
pub const INVALID_INCREMENT_VALUE: &str = "TSDB: invalid increase/decrease value";
pub const CANNOT_INCREMENT_DECREMENT_NAN: &str = "TSDB: cannot increment/decrement NaN value";
pub const INVALID_BUCKET_ALIGNMENT: &str = "TSDB: invalid bucket alignment";
pub const INVALID_ALIGNMENT_TIMESTAMP: &str = "TSDB: Couldn't parse alignTimestamp";
pub const INVALID_BUCKET_TIMESTAMP_TYPE: &str = "TSDB: unknown BUCKETTIMESTAMP parameter";
pub const INVALID_BOOLEAN: &str = "TSDB: invalid boolean argument";
// Exact RTS text for the shared TS.CREATE/TS.ALTER/TS.ADD option surface
// (verified by probing the reference container). A missing ENCODING *value* is
// not one of these: RTS reports plain wrong-arity there, so that site returns
// ValkeyError::WrongArity rather than a TSDB: message.
pub const INVALID_CHUNK_ENCODING: &str = "TSDB: unknown ENCODING parameter";
pub const CANNOT_PARSE_CHUNK_SIZE: &str = "TSDB: Couldn't parse CHUNK_SIZE";
pub const INVALID_CHUNK_SIZE: &str = "TSDB: invalid chunk size";
pub const INVALID_DUPLICATE_POLICY: &str = "TSDB: Unknown DUPLICATE_POLICY";
pub const MISSING_DUPLICATE_POLICY: &str = "TSDB: Couldn't parse DUPLICATE_POLICY";
pub const DEBUG_MODE_DISABLED: &str = "TSDB: TS._DEBUG is disabled. Set the 'ts.debug-mode' configuration parameter to yes to enable it";
pub const INVALID_DURATION: &str = "TSDB: invalid duration";
pub const INVALID_INTEGER: &str = "TSDB: invalid integer";
pub const INVALID_JOIN_KEY: &str = "TSDB: invalid join key";
pub const DUPLICATE_JOIN_KEYS: &str = "TSDB: duplicate join keys";
pub const MISSING_JOIN_REDUCER: &str = "TSDB: join aggregation requires a reducer";
pub const INVALID_ASOF_TOLERANCE: &str = "TSDB: negative ASOF tolerance not valid";
pub const INVALID_ASOF_STRATEGY: &str = "TSDB: invalid ASOF strategy";
pub const INVALID_METRIC_NAME: &str = "TSDB: invalid metric name";
pub const INVALID_OR_MISSING_METRIC_NAME: &str = "TSDB: invalid or missing metric name";
pub const METRIC_ALREADY_SET: &str = "TSDB: metric already set";
pub const INVALID_NUMBER: &str = "TSDB: invalid number";
pub const INVALID_SERIES_SELECTOR: &str = "TSDB: series selector is invalid";

pub const INVALID_COMPATIBILITY_MODE: &str =
    "TSDB: invalid compatibility mode, expected one of: extended, strict";
pub const INVALID_STEP_DURATION: &str = "TSDB: invalid step duration";
pub const INVALID_TIMESTAMP: &str = "TSDB: invalid timestamp";
pub const UNKNOWN_AGGREGATION_TYPE: &str = "TSDB: Unknown aggregation type";
pub const DUPLICATE_AGGREGATION: &str = "TSDB: duplicate aggregation";
pub const TOO_MANY_AGGREGATIONS: &str = "TSDB: too many aggregations (max 16)";
pub const INVALID_AGGREGATION_LIST: &str = "TSDB: invalid aggregation list";
pub const INVALID_REDUCER_TYPE: &str = "TSDB: Invalid reducer type";
pub const INVALID_AGGREGATION_CONDITION: &str = "TSDB: invalid aggregation condition";
pub const MULTI_AGGREGATION_UNSUPPORTED: &str =
    "TSDB: multiple aggregations are not supported for TS.JOIN";
// TS.NRANGE takes its keys as an explicit `numkeys key [key ...]` prefix. Both texts are
// RedisTimeSeries 8.10's, confirmed by probing the reference container; a numkeys that
// overruns the argument list is reported there as wrong arity rather than with a message of
// its own, so this file has no constant for it.
pub const INVALID_NUMKEYS: &str = "TSDB: numkeys must be a positive integer";
// One AGGREGATION operand per key, in key order (each a comma-separated aggregator list).
pub const AGGREGATOR_COUNT_MISMATCH: &str =
    "TSDB: the number of AGGREGATION arguments must be equal to numkeys";
pub const INVALID_START_TIMESTAMP: &str = "TSDB: wrong fromTimestamp";
pub const INVALID_END_TIMESTAMP: &str = "TSDB: wrong toTimestamp";
// Write-path rejection of a negative absolute timestamp. The range family does not use
// this: it reports a bad bound positionally as INVALID_START/END_TIMESTAMP regardless of
// why the bound failed to parse. Wording (including the unhyphenated "nonnegative") is
// verbatim RedisTimeSeries 8.6.2, confirmed by probing the reference container.
pub const NEGATIVE_TIMESTAMP: &str = "TSDB: invalid timestamp, must be a nonnegative integer";
pub const ERROR_ADDING_SAMPLE: &str = "TSDB: error at add";
pub const MISSING_TIMESTAMP_FILTER_VALUE: &str =
    "TSDB: FILTER_BY_TS one or more arguments are missing";
pub const TOO_MANY_TIMESTAMP_FILTER_VALUES: &str = "TSDB: too many timestamp filter values";
pub const KEY_NOT_FOUND: &str = "TSDB: the key does not exist";
pub const INVALID_TIMESERIES_KEY: &str = "TSDB: the key is not a TSDB key";
pub const KEY_READ_PERMISSION_ERROR: &str = "TSDB: key permission error";
pub const KEY_WRITE_PERMISSION_ERROR: &str =
    "TSDB: the current user does not have permissions to write to a given key";
pub const KEY_UPDATE_PERMISSION_ERROR: &str = "TSDB: key update permission error";
pub const KEY_DELETE_PERMISSION_ERROR: &str = "TSDB: key delete permission error";
pub const ALL_KEYS_READ_PERMISSION_ERROR: &str = "TSDB: current user doesn't have read permission to one or more keys that match the specified filter";
pub const ALL_KEYS_WRITE_PERMISSION_ERROR: &str = "TSDB: current user doesn't have write permission to one or more keys that match the specified filter";
pub const DUPLICATE_KEY: &str = "TSDB: key already exists";
pub const MISSING_FILTER: &str = "TSDB: please provide at least one matcher";
pub const INVALID_TIMESTAMP_FILTER: &str = "TSDB: FILTER_BY_TS one or more arguments are missing";
pub const INVALID_REGEX: &str = "TSDB: invalid regex";
pub const INVALID_IGNORE_OPTIONS: &str = "TSDB: invalid ignore options";
pub const CANNOT_PARSE_IGNORE: &str = "TSDB: Couldn't parse IGNORE";
pub const NEGATIVE_IGNORE_VALUES: &str = "TSDB: IGNORE arguments cannot be negative";

pub const CANNOT_PARSE_LABELS: &str = "TSDB: Couldn't parse LABELS";
pub const CANNOT_PARSE_RETENTION: &str = "TSDB: Couldn't parse RETENTION";
pub const CANNOT_PARSE_AGGREGATION: &str = "TSDB: Couldn't parse AGGREGATION";
pub const BUCKET_DURATION_TOO_SMALL: &str = "TSDB: bucketDuration must be greater than zero";
pub const FILTER_BY_VALUE_MISSING_ARGS: &str =
    "TSDB: FILTER_BY_VALUE one or more arguments are missing";
pub const CANNOT_PARSE_MIN: &str = "TSDB: Couldn't parse MIN";
pub const CANNOT_PARSE_MAX: &str = "TSDB: Couldn't parse MAX";

pub const NO_SERIES_FOUND: &str = "TSDB: no series found";
pub const SAMPLE_TOO_OLD: &str = "TSDB: Timestamp is older than retention";
pub const SERIES_NOT_FOUND: &str = "TSDB: series not found";
pub const GROUP_NOT_FOUND: &str = "TSDB: group not found";
pub const LABELS_ALREADY_SET: &str = "TSDB: labels already set";
pub const INVALID_LABEL_NAME: &str = "TSDB: invalid label name";
pub const INVALID_LABEL_VALUE: &str = "TSDB: invalid label value";
pub const TOO_MANY_LABELS: &str = "TSDB: too many labels";
pub const MISSING_LABEL_VALUE: &str = "TSDB: empty or missing label value";
pub const MISSING_LIMIT_VALUE: &str = "TSDB: missing LIMIT value";
pub const INVALID_LIMIT_VALUE: &str = "TSDB: invalid LIMIT value";
pub const MISSING_COUNT_VALUE: &str = "TSDB: COUNT argument is missing";
pub const INVALID_COUNT_VALUE: &str = "TSDB: Invalid COUNT value";
pub const CANNOT_PARSE_COUNT: &str = "TSDB: Couldn't parse COUNT";
pub const ROUNDING_ALREADY_SET: &str = "TSDB: rounding already set";
pub const DUPLICATE_SAMPLE_BLOCKED: &str = "TSDB: Error at upsert, duplicate sample blocked";
pub const PERMISSION_DENIED: &str = "TSDB: current user doesn't have read permission to one or more keys that match the specified filter";
pub const COMMAND_SERIALIZATION_ERROR: &str = "TSDB: command serialization error";
pub const COMMAND_DESERIALIZATION_ERROR: &str = "TSDB: command deserialization error";
pub const CLUSTER_MODE_ERROR: &str = "TSDB: cluster mode not supported";
pub const NO_CLUSTER_NODES_AVAILABLE: &str = "TSDB: no cluster nodes available";
pub const WITH_LABELS_AND_SELECTED_LABELS_SPECIFIED: &str =
    "TSDB: cannot accept WITHLABELS and SELECT_LABELS together";
pub const EMPTY_SELECTED_LABELS: &str = "TSDB: SELECT_LABELS should have at least 1 parameter";
pub const EXCLUDE_EMPTY_WITH_GROUPBY: &str = "TSDB: EXCLUDEEMPTY is not allowed with GROUPBY";
pub const COMPACTION_CIRCULAR_DEPENDENCY: &str = "TSDB: circular dependency in compaction rules";
pub const COMPACTION_RULE_NOT_FOUND: &str = "TSDB: compaction rule does not exist";
pub const INVALID_COMPARISON_OPERATOR: &str = "TSDB: invalid comparison operator";
pub const TOO_MANY_SAMPLES: &str = "TSDB: too many samples";

// TS.READ. All four texts are verbatim from the 8.10 reference (probed 2026-08-01, see
// docs/plans/ts-read-implementation-plan.md §6). Note the failure *class* split the reference uses:
// duplicated/malformed options resolve to plain wrong-arity, only the value-range failures
// below get a TSDB: message.
pub const READ_MAX_COUNT_MUST_BE_POSITIVE: &str = "TSDB: MAX_COUNT must be a positive integer";
pub const READ_MIN_COUNT_MUST_BE_POSITIVE: &str =
    "TSDB: BLOCK min_count must be a positive integer";
pub const READ_BLOCK_MS_MUST_BE_NON_NEGATIVE: &str =
    "TSDB: BLOCK milliseconds must be a non-negative integer";
pub const READ_MIN_COUNT_EXCEEDS_MAX_COUNT: &str = "TSDB: BLOCK min_count must be <= MAX_COUNT";
pub const READ_BLOCKING_NOT_ALLOWED: &str = "TSDB: blocking TS.READ (with BLOCK) is not allowed inside MULTI, EVAL, or a deny-blocking context";
// CONDITION is an additive extension with no reference text to match; the wording follows the
// clause-named style of the probed constants above. A bad operator reuses
// INVALID_COMPARISON_OPERATOR, which `ComparisonOperator::try_from` already returns.
pub const READ_CONDITION_VALUE_MUST_BE_A_NUMBER: &str = "TSDB: CONDITION value must be a number";

// TS.QUERYLABELS
pub const UNKNOWN_QUERY_LABELS_SUBTYPE: &str =
    "TSDB: unknown subtype, must be one of LABELS|VALUES";
pub const QUERY_LABELS_EXPECTED_FILTER: &str = "TSDB: unknown argument, expected FILTER";
pub const FILTER_WITH_NO_EXPRESSIONS: &str = "TSDB: FILTER given with no filter expressions";
