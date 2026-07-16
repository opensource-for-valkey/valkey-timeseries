use crate::aggregators::AggregationType;
use crate::common::Timestamp;
use crate::parser::parse_positive_duration_value;
use crate::series::CompactionRule;
use ahash::{HashSet, HashSetExt};
use regex::RegexSet;
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::{LazyLock, RwLock};
use valkey_module::{ValkeyError, ValkeyResult};

#[derive(Debug, Copy, Clone, PartialEq, Hash, Eq)]
pub struct CompactionPolicy {
    pub aggregator: AggregationType,
    pub bucket_duration_ms: u64,
    pub retention_ms: u64,
    pub bucket_alignment: Timestamp,
}

impl CompactionPolicy {
    pub fn format_key(&self, key: &str) -> String {
        let mut result = format!("{key}_{}", self.aggregator.name().to_ascii_uppercase());

        if self.bucket_duration_ms > 0 {
            let msg = format!("_{}", self.bucket_duration_ms);
            result.push_str(&msg);
        }

        if self.bucket_alignment > 0 {
            let msg = format!("_{}", self.bucket_alignment);
            result.push_str(&msg);
        }

        result
    }
}

impl Display for CompactionPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}:{}",
            self.aggregator.name(),
            self.bucket_duration_ms,
            self.retention_ms,
        )?;
        if self.bucket_alignment > 0 {
            write!(f, ":{}", self.bucket_alignment)
        } else {
            Ok(())
        }
    }
}

pub struct PolicyConfig {
    filters: RegexSet,
    /// Maps filter strings to indices of policies that match the filter.
    filtered_policies: HashMap<String, Vec<CompactionPolicy>>,
    global_policies: Vec<CompactionPolicy>,
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl PolicyConfig {
    pub fn new() -> Self {
        PolicyConfig {
            filters: RegexSet::empty(),
            filtered_policies: HashMap::new(),
            global_policies: Vec::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.global_policies.is_empty() && self.filtered_policies.is_empty()
    }

    pub(crate) fn clear(&mut self) {
        self.filters = RegexSet::empty();
        self.filtered_policies.clear();
        self.global_policies.clear();
    }

    fn add_regex_filter(&mut self, filter: &str) -> ValkeyResult<()> {
        // add the filter to the regex set
        let mut new_filters: Vec<String> = self.filtered_policies.keys().cloned().collect();
        new_filters.push(filter.to_string());
        self.filters = RegexSet::new(&new_filters).map_err(|_| {
            let msg = format!("TSDB: invalid compaction regex filter: \"{filter}\"");
            ValkeyError::String(msg)
        })?;
        Ok(())
    }

    pub fn add_policies(
        &mut self,
        policies: Vec<CompactionPolicy>,
        filter: Option<String>,
    ) -> ValkeyResult<()> {
        if let Some(filter) = filter {
            // see if we've already added this filter
            let indices = match self.filtered_policies.get_mut(&filter) {
                Some(indices) => indices,
                None => {
                    self.add_regex_filter(&filter)?;
                    // if not, we need to create a new entry
                    self.filtered_policies
                        .insert(filter.clone(), Vec::with_capacity(policies.len()));
                    self.filtered_policies.get_mut(&filter).unwrap()
                }
            };
            let existing_set: HashSet<&CompactionPolicy> = indices.iter().collect();

            for policy in policies.iter() {
                if existing_set.contains(policy) {
                    let msg = format!(
                        "TSDB: duplicate compaction policy with filter \"{filter}\": {policy}",
                    );
                    return Err(ValkeyError::String(msg));
                }
            }

            indices.extend(policies);

            return Ok(());
        }

        self.global_policies.extend(policies);

        Ok(())
    }

    /// Add policies with filters to the global configuration
    pub fn add_policies_from_config(
        &mut self,
        config_value: &str,
        replace: bool,
    ) -> ValkeyResult<()> {
        let policies_with_filters = parse_compaction_policies(config_value)?;

        // Group policies by filter for efficient storage
        let mut filter_groups: HashMap<Option<String>, Vec<CompactionPolicy>> = HashMap::new();

        for (policy, filter) in policies_with_filters {
            filter_groups.entry(filter).or_default().push(policy);
        }

        if replace {
            self.clear();
        }

        // Add each group to the config
        for (filter, policies) in filter_groups {
            self.add_policies(policies, filter)?;
        }

        Ok(())
    }

    fn match_policy(&self, key: &str) -> Option<HashSet<CompactionPolicy>> {
        if self.is_empty() {
            return None;
        }

        let mut result_set: HashSet<CompactionPolicy> =
            HashSet::with_capacity(self.global_policies.len());

        if !self.global_policies.is_empty() {
            for policy in &self.global_policies {
                result_set.insert(*policy);
            }
        }

        if self.filters.is_empty() {
            return Some(result_set);
        }

        let matches = self.filters.matches(key);
        if matches.matched_any() {
            let filters = self.filters.patterns();
            for match_idx in matches.iter() {
                let Some(pattern) = filters.get(match_idx) else {
                    continue;
                };
                let Some(policies) = self.filtered_policies.get(pattern) else {
                    continue;
                };
                for policy in policies {
                    result_set.insert(*policy);
                }
            }
        }

        if result_set.is_empty() {
            None
        } else {
            Some(result_set)
        }
    }

    pub fn create_compaction_rules(
        &self,
        key: &str,
    ) -> Option<HashMap<String, (CompactionRule, u64)>> {
        let policies = self.match_policy(key)?;

        if policies.is_empty() {
            return None;
        }

        let mut rules = HashMap::with_capacity(policies.len());
        for policy in policies {
            let rule_key = policy.format_key(key);
            let rule = CompactionRule {
                dest_id: 0, // This will be set later when the rule is applied
                aggregator: policy.aggregator.into(),
                bucket_duration: policy.bucket_duration_ms,
                align_timestamp: policy.bucket_alignment,
                bucket_start: None, // End timestamp is not set initially
                has_samples: false,
            };
            rules.insert(rule_key, (rule, policy.retention_ms));
        }
        Some(rules)
    }
}

pub static COMPACTION_POLICY_CONFIG: LazyLock<RwLock<PolicyConfig>> =
    LazyLock::new(|| RwLock::new(PolicyConfig::new()));

fn check_duplicate_policy(
    policies: &[CompactionPolicy],
    new_policy: &CompactionPolicy,
) -> ValkeyResult<()> {
    for policy in policies {
        if policy == new_policy {
            let msg = format!("TSDB: Duplicate compaction policy found: {new_policy}");
            return Err(ValkeyError::String(msg));
        }
    }
    Ok(())
}

fn parse_duration(val: &str) -> ValkeyResult<u64> {
    // handle redis styled spec (capital M for minutes, m for milliseconds)
    let Some(last_char) = val.chars().last() else {
        return Err(ValkeyError::String(format!(
            "TSDB: invalid duration value: '{val}'"
        )));
    };
    let duration = match last_char {
        'm' => {
            let stripped = val.strip_suffix('m').unwrap_or(val);
            let new_val = format!("{stripped}ms");
            parse_positive_duration_value(&new_val)
        }
        'M' => {
            let stripped = val.strip_suffix('M').unwrap_or(val);
            let new_val = format!("{stripped}m");
            parse_positive_duration_value(&new_val)
        }
        _ => parse_positive_duration_value(val),
    }
    .map_err(|_| ValkeyError::String(format!("TSDB: invalid duration value: '{val}'")))?;

    Ok(duration as u64)
}

/// A compaction policy is a set of rules that define how to compact time series data.
/// The value from configuration is expected to be in the format:
///  `aggregation_type:bucket_duration:retention:[align_timestamp]`
///
/// E.g., `avg:60:2h:0`
///
/// Where:
///  - `aggregation_type` is the type of aggregation to use (e.g., `avg`, `sum`, etc.)
///  - `bucket_duration` is the duration of each bucket in seconds
///  - `retention` is the retention period in seconds
///  - The `align_timestamp` is optional and defaults to 0 if not provided.
///
fn parse_compaction_policy(val: &str) -> ValkeyResult<CompactionPolicy> {
    let args: Vec<&str> = val.split(':').collect();
    if args.len() < 3 || args.len() > 4 {
        let msg = format!(
            "TSDB: invalid compaction policy format: '{val}'. Expected: aggregation_type:bucket_duration:retention[:align_timestamp]",
        );
        return Err(ValkeyError::String(msg));
    }
    let aggregator = AggregationType::try_from(args[0])?;
    let Ok(bucket_duration_ms) = parse_duration(args[1]) else {
        return Err(ValkeyError::String(
            "TSDB: invalid bucket duration in compaction policy".to_string(),
        ));
    };
    if bucket_duration_ms == 0 {
        return Err(ValkeyError::String(
            "TSDB: compaction policy bucket duration cannot be zero".to_string(),
        ));
    }
    let Ok(retention_ms) = parse_duration(args[2]) else {
        return Err(ValkeyError::Str(
            "TSDB: invalid retention in compaction policy",
        ));
    };
    // Same redis-style duration grammar as the other fields ('m' = ms,
    // 'M' = minutes). Any alignment value is valid — bucket assignment is
    // modular, so an alignment larger than the bucket duration is simply
    // congruent to `align % bucket_duration` (RTS accepts these too).
    let bucket_alignment = if args.len() == 4 {
        match parse_duration(args[3]) {
            Ok(ts) => ts as i64,
            _ => {
                return Err(ValkeyError::String(
                    "TSDB: invalid align timestamp in compaction policy".to_string(),
                ));
            }
        }
    } else {
        0 // Default align timestamp
    };

    Ok(CompactionPolicy {
        aggregator,
        bucket_duration_ms,
        retention_ms,
        bucket_alignment,
    })
}

/// Parse a single compaction policy with optional regex filter
/// Format: `aggregation_type:bucket_duration:retention[:align_timestamp][|regex_filter]`
///
/// Examples:
/// - `avg:60:2h:0` (no filter)
/// - `avg:60:2h:0|^metrics\..*` (with filter)
/// - `sum:30:1h|cpu_.*|memory_.*` (multiple filters with OR logic)
fn parse_compaction_policy_with_filter(
    val: &str,
) -> ValkeyResult<(CompactionPolicy, Option<String>)> {
    // Split on the first pipe to separate policy from filter
    let (policy_part, filter_part) = match val.split_once('|') {
        Some((policy, filter)) => (policy, Some(filter.to_string())),
        None => (val, None),
    };

    // Parse the policy part
    let policy = parse_compaction_policy(policy_part)?;

    // Validate regex filter if present
    if let Some(ref filter) = filter_part {
        if filter.trim().is_empty() {
            return Err(ValkeyError::String(
                "TSDB: empty regex filter in compaction policy".to_string(),
            ));
        }

        // Test if the regex is valid by trying to compile it
        regex::Regex::new(filter)
            .map_err(|_| ValkeyError::String(format!("TSDB: invalid regex filter \"{filter}\"")))?;
    }

    Ok((policy, filter_part))
}

/// Parse multiple compaction policies with optional filters
/// Format: `policy1[|filter1];policy2[|filter2];...`
pub(crate) fn parse_compaction_policies(
    val: &str,
) -> ValkeyResult<Vec<(CompactionPolicy, Option<String>)>> {
    let policies: Vec<&str> = val.split(';').collect();
    if policies.is_empty() {
        return Err(ValkeyError::String(
            "TSDB: no compaction policies provided".to_string(),
        ));
    }

    let mut resolved_policies: Vec<(CompactionPolicy, Option<String>)> =
        Vec::with_capacity(policies.len());
    for policy_str in policies.iter() {
        if policy_str.is_empty() {
            return Err(ValkeyError::String(
                "Empty compaction policy found".to_string(),
            ));
        }

        let (policy, filter) = parse_compaction_policy_with_filter(policy_str)?;
        resolved_policies.push((policy, filter));
    }

    Ok(resolved_policies)
}

/// Add policies with filters to the global configuration
pub fn add_compaction_policies_from_config(config_value: &str, replace: bool) -> ValkeyResult<()> {
    let mut config = COMPACTION_POLICY_CONFIG.write().map_err(|_| {
        ValkeyError::Str("TSDB: failed to acquire write lock on compaction policy config")
    })?;

    config.add_policies_from_config(config_value, replace)
}

pub fn clear_compaction_policy_config() {
    let mut config = COMPACTION_POLICY_CONFIG
        .write()
        .expect("Failed to acquire write lock on compaction policy config");
    config.clear();
}

pub fn create_compaction_rules_from_config(
    series_key: &str,
) -> Option<HashMap<String, (CompactionRule, u64)>> {
    let compaction_config = COMPACTION_POLICY_CONFIG
        .read()
        .expect("Failed to acquire compaction policy config read lock");
    compaction_config.create_compaction_rules(series_key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregators::AggregationType;

    fn create_policy(
        aggregator: AggregationType,
        bucket_duration_ms: u64,
        align_timestamp: i64,
    ) -> CompactionPolicy {
        CompactionPolicy {
            aggregator,
            bucket_duration_ms,
            retention_ms: 0, // Not used in format_key
            bucket_alignment: align_timestamp,
        }
    }

    #[test]
    /// Make sure we can parse redis-style durations correctly.
    fn test_parse_duration_milliseconds() {
        // Test lowercase 'm' for milliseconds
        assert_eq!(parse_duration("100m").unwrap(), 100);
        assert_eq!(parse_duration("1m").unwrap(), 1);
        assert_eq!(parse_duration("1000m").unwrap(), 1000);
        assert_eq!(parse_duration("0m").unwrap(), 0);
    }

    #[test]
    /// Make sure we can parse redis-style durations correctly.
    fn test_parse_duration_minutes() {
        // Test uppercase 'M' for minutes
        assert_eq!(parse_duration("1M").unwrap(), 60_000);
        assert_eq!(parse_duration("5M").unwrap(), 300_000);
        assert_eq!(parse_duration("60M").unwrap(), 3_600_000);
        assert_eq!(parse_duration("0M").unwrap(), 0);
    }

    #[test]
    fn test_parse_policy_without_filter() {
        let (policy, filter) = parse_compaction_policy_with_filter("avg:5M:2h:10s").unwrap();
        assert_eq!(policy.aggregator, AggregationType::Avg);
        assert_eq!(policy.bucket_duration_ms, 300000);
        assert!(filter.is_none());
    }

    #[test]
    fn test_parse_policy_with_filter() {
        let (policy, filter) =
            parse_compaction_policy_with_filter("sum:30s:1h|^metrics\\..*").unwrap();
        assert_eq!(policy.aggregator, AggregationType::Sum);
        assert_eq!(policy.bucket_duration_ms, 30000);
        assert_eq!(filter.unwrap(), "^metrics\\..*");
    }

    #[test]
    fn test_parse_multiple_policies_mixed() {
        let policies =
            parse_compaction_policies("avg:1M:2h:10s;sum:5M:1h|cpu_.*;max:15s:30m|memory_.*")
                .unwrap();

        assert_eq!(policies.len(), 3);
        assert!(policies[0].1.is_none()); // No filter
        assert_eq!(policies[1].1.as_ref().unwrap(), "cpu_.*");
        assert_eq!(policies[2].1.as_ref().unwrap(), "memory_.*");
    }

    #[test]
    fn test_invalid_regex_filter() {
        let result = parse_compaction_policy_with_filter("avg:60:2h:0|[invalid");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("invalid regex filter")
        );
    }

    #[test]
    fn test_empty_filter() {
        let result = parse_compaction_policy_with_filter("avg:60:2h:0|");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("empty regex filter")
        );
    }

    #[test]
    fn test_format_key_basic() {
        let policy = create_policy(AggregationType::Avg, 60000, 0);
        assert_eq!(policy.format_key("temperature"), "temperature_AVG_60000");
    }

    #[test]
    fn test_format_key_with_bucket_duration() {
        let policy = create_policy(AggregationType::Sum, 30000, 0);
        assert_eq!(policy.format_key("cpu_usage"), "cpu_usage_SUM_30000");
    }

    #[test]
    fn test_format_key_with_both_duration_and_alignment() {
        let policy = create_policy(AggregationType::Min, 60000, 5000);
        assert_eq!(policy.format_key("disk_io"), "disk_io_MIN_60000_5000");
    }

    #[test]
    fn test_format_key_all_aggregation_types() {
        let test_cases = vec![
            (AggregationType::Avg, "AVG"),
            (AggregationType::Sum, "SUM"),
            (AggregationType::Min, "MIN"),
            (AggregationType::Max, "MAX"),
            (AggregationType::Range, "RANGE"),
            (AggregationType::Count, "COUNT"),
            (AggregationType::StdP, "STD.P"),
            (AggregationType::VarP, "VAR.P"),
            (AggregationType::First, "FIRST"),
            (AggregationType::Last, "LAST"),
        ];

        for (aggregator, expected_name) in test_cases {
            let policy = create_policy(aggregator, 2000, 0);
            let result = policy.format_key("test");
            assert_eq!(result, format!("test_{expected_name}_2000"));
        }
    }

    #[test]
    fn test_format_key_zero_alignment() {
        // Only align_timestamp is zero
        let policy = create_policy(AggregationType::Sum, 45000, 0);
        assert_eq!(policy.format_key("requests"), "requests_SUM_45000");
    }

    #[test]
    fn test_format_key_unicode_characters() {
        let policy = create_policy(AggregationType::Sum, 30000, 1000);
        assert_eq!(policy.format_key("温度传感器"), "温度传感器_SUM_30000_1000");
        assert_eq!(
            policy.format_key("señor_métrica"),
            "señor_métrica_SUM_30000_1000"
        );
    }

    #[test]
    fn test_format_key_case_insensitive_aggregator() {
        // Verify that aggregator names are always uppercase regardless of internal representation
        let policy = create_policy(AggregationType::Sum, 60000, 0);
        let result = policy.format_key("test");
        assert!(result.contains("SUM"));
        assert!(!result.contains("sum"));
        assert!(!result.contains("Sum"));
    }

    #[test]
    fn test_no_policies_configured() {
        let config = PolicyConfig::new();
        let result = config.create_compaction_rules("any_key");
        assert!(result.is_none());
    }

    #[test]
    fn test_global_policies_without_filters() {
        let mut config = PolicyConfig::new();

        // Add global policies (no filters)
        let config_str = "avg:60s:2h;sum:30s:1h";
        config.add_policies_from_config(config_str, false).unwrap();

        let rules = config.create_compaction_rules("temperature").unwrap();

        // Should have 2 rules
        assert_eq!(rules.len(), 2);

        // Check AVG rule
        assert!(rules.contains_key("temperature_AVG_60000"));
        let (avg_rule, avg_retention) = &rules["temperature_AVG_60000"];
        assert_eq!(avg_rule.aggregator, AggregationType::Avg.into());
        assert_eq!(avg_rule.bucket_duration, 60000);
        assert_eq!(avg_rule.align_timestamp, 0);
        assert_eq!(*avg_retention, 7200000); // 2h in ms

        // Check SUM rule
        assert!(rules.contains_key("temperature_SUM_30000"));
        let (sum_rule, sum_retention) = &rules["temperature_SUM_30000"];
        assert_eq!(sum_rule.aggregator, AggregationType::Sum.into());
        assert_eq!(sum_rule.bucket_duration, 30000);
        assert_eq!(sum_rule.align_timestamp, 0);
        assert_eq!(*sum_retention, 3600000); // 1h in ms
    }

    #[test]
    fn test_filtered_policies_matching_key() {
        let mut config = PolicyConfig::new();

        // Add policies with regex filters
        let config_str = "max:60s:1h:2s|^temp.*";
        config.add_policies_from_config(config_str, false).unwrap();

        // Key matches filter
        let rules = config
            .create_compaction_rules("temperature_sensor")
            .unwrap();
        assert_eq!(rules.len(), 1);

        let rule_key = "temperature_sensor_MAX_60000_2000";
        assert!(rules.contains_key(rule_key));
        let (rule, retention) = &rules[rule_key];
        assert_eq!(rule.aggregator, AggregationType::Max.into());
        assert_eq!(*retention, 3600000);
    }

    #[test]
    fn test_filtered_policies_non_matching_key() {
        let mut config = PolicyConfig::new();

        // Add policies with regex filters
        let config_str = "max:60s:1h:5s|^temp.*";
        config.add_policies_from_config(config_str, false).unwrap();

        // Key doesn't match filter
        let rules = config.create_compaction_rules("pressure_sensor");
        assert!(rules.is_none());
    }

    #[test]
    fn test_mixed_global_and_filtered_policies() {
        let mut config = PolicyConfig::new();

        // Add both global and filtered policies
        let config_str = "avg:60s:2h;sum:30s:1h:5s|^cpu.*";
        config.add_policies_from_config(config_str, false).unwrap();

        // Test key that matches filter - should get both global and filtered
        let rules = config.create_compaction_rules("cpu_usage").unwrap();
        assert_eq!(rules.len(), 2);

        // Should have global AVG rule
        assert!(rules.contains_key("cpu_usage_AVG_60000"));

        // Should have filtered SUM rule
        assert!(rules.contains_key("cpu_usage_SUM_30000_5000"));
    }

    #[test]
    fn test_mixed_policies_non_matching_key() {
        let mut config = PolicyConfig::new();

        // Add both global and filtered policies
        let config_str = "avg:60s:2h;sum:30s:1h|^cpu.*";
        config.add_policies_from_config(config_str, false).unwrap();

        // Test key that doesn't match filter - should only get global
        let rules = config.create_compaction_rules("memory_usage").unwrap();
        assert_eq!(rules.len(), 1);

        // Should only have global AVG rule
        assert!(rules.contains_key("memory_usage_AVG_60000"));
        assert!(!rules.contains_key("memory_usage_SUM_30000"));
    }

    #[test]
    fn test_multiple_filters_different_patterns() {
        let mut config = PolicyConfig::new();

        // Add policies with different filter patterns
        let config_str = "max:60s:1h|^temp.*;min:30s:30m|^pressure.*";
        config.add_policies_from_config(config_str, false).unwrap();

        // Test key matching first filter
        let temp_rules = config.create_compaction_rules("temperature").unwrap();
        assert_eq!(temp_rules.len(), 1);
        assert!(temp_rules.contains_key("temperature_MAX_60000"));

        // Test key matching the second filter
        let pressure_rules = config.create_compaction_rules("pressure_gauge").unwrap();
        assert_eq!(pressure_rules.len(), 1);
        assert!(pressure_rules.contains_key("pressure_gauge_MIN_30000"));

        // Test key matching neither filter
        let cpu_rules = config.create_compaction_rules("cpu_usage");
        assert!(cpu_rules.is_none());
    }

    #[test]
    fn test_multiple_policies_same_filter() {
        let mut config = PolicyConfig::new();

        // Add multiple policies with the same filter
        let config_str = "avg:60s:2h|^metrics.*;sum:30s:1h|^metrics.*";
        config.add_policies_from_config(config_str, false).unwrap();

        let rules = config.create_compaction_rules("metrics_cpu").unwrap();
        assert_eq!(rules.len(), 2);

        assert!(rules.contains_key("metrics_cpu_AVG_60000"));
        assert!(rules.contains_key("metrics_cpu_SUM_30000"));
    }

    #[test]
    fn test_complex_regex_patterns() {
        let mut config = PolicyConfig::new();

        // Add policies with complex regex patterns
        let config_str = r#"avg:60s:1h|^(temp|cpu)_.*$;max:30s:30m|.*_sensor$"#;
        config.add_policies_from_config(config_str, false).unwrap();

        // Test keys matching the first pattern
        let temp_rules = config.create_compaction_rules("temp_indoor").unwrap();
        assert_eq!(temp_rules.len(), 1);
        assert!(temp_rules.contains_key("temp_indoor_AVG_60000"));

        let cpu_rules = config.create_compaction_rules("cpu_load").unwrap();
        assert_eq!(cpu_rules.len(), 1);
        assert!(cpu_rules.contains_key("cpu_load_AVG_60000"));

        // Test key matching the second pattern
        let sensor_rules = config.create_compaction_rules("humidity_sensor").unwrap();
        assert_eq!(sensor_rules.len(), 1);
        assert!(sensor_rules.contains_key("humidity_sensor_MAX_30000"));

        // Test key matching both patterns
        let both_rules = config.create_compaction_rules("temp_sensor").unwrap();
        assert_eq!(both_rules.len(), 2);
        assert!(both_rules.contains_key("temp_sensor_AVG_60000"));
        assert!(both_rules.contains_key("temp_sensor_MAX_30000"));
    }

    #[test]
    fn test_rule_properties_are_correct() {
        let mut config = PolicyConfig::new();

        let config_str = "std.p:45s:90M:5000|^test.*";
        config.add_policies_from_config(config_str, false).unwrap();

        let rules = config.create_compaction_rules("test_key").unwrap();
        assert_eq!(rules.len(), 1);

        let rule_key = "test_key_STD.P_45000_5000";
        let (rule, retention) = &rules[rule_key];

        // Check all rule properties
        assert_eq!(rule.dest_id, 0); // Should be 0 initially
        assert_eq!(rule.aggregator, AggregationType::StdP.into());
        assert_eq!(rule.bucket_duration, 45000); // 45s in ms
        assert_eq!(rule.align_timestamp, 5000);
        assert!(rule.bucket_start.is_none()); // Should be None initially
        assert_eq!(*retention, 5400000); // 90m in ms
    }

    #[test]
    fn test_duplicate_policies_same_config() {
        let mut config = PolicyConfig::new();

        // This should work fine - same policy applied to different keys
        let config_str = "avg:60s:1h|^temp.*;avg:60s:1h|^cpu.*";
        config.add_policies_from_config(config_str, false).unwrap();

        let temp_rules = config.create_compaction_rules("temp_1").unwrap();
        let cpu_rules = config.create_compaction_rules("cpu_1").unwrap();

        assert_eq!(temp_rules.len(), 1);
        assert_eq!(cpu_rules.len(), 1);
    }

    #[test]
    fn test_special_characters_in_key() {
        let mut config = PolicyConfig::new();

        let config_str = "sum:30s:1h|.*sensor.*";
        config.add_policies_from_config(config_str, false).unwrap();

        let test_keys = vec![
            "temp-sensor",
            "pressure.sensor",
            "humidity_sensor",
            "sensor:outdoor",
            "sensor with spaces",
        ];

        for key in test_keys {
            let rules = config.create_compaction_rules(key);
            assert!(rules.is_some(), "Rules should exist for key: {key}");
            let rules = rules.unwrap();
            assert_eq!(rules.len(), 1);

            let expected_key = format!("{key}_SUM_30000");
            assert!(rules.contains_key(&expected_key));
        }
    }

    #[test]
    fn test_special_regex_characters_in_keys() {
        let mut config = PolicyConfig::new();
        let policy_str = r"avg:10s:1h|^app\.prod\..*"; // Escaped dots to match literal dots

        config.add_policies_from_config(policy_str, false).unwrap();

        let test_cases = vec![
            ("app.prod.latency", true),   // Should match (literal dots)
            ("app.prod.cpu.usage", true), // Should match
            ("appXprodXlatency", false),  // Should not match (X instead of .)
            ("app-prod-latency", false),  // Should not match (- instead of .)
            ("app.dev.latency", false),   // Should not match (dev instead of prod)
        ];

        for (key, should_match) in test_cases {
            let rules = config.create_compaction_rules(key);
            if should_match {
                assert!(rules.is_some(), "Expected match for key: {key}");
            } else {
                assert!(rules.is_none(), "Expected no match for key: {key}");
            }
        }
    }

    #[test]
    fn test_case_sensitive_regex_matching() {
        let mut config = PolicyConfig::new();

        let config_str = "avg:60s:1h|^TEMP.*";
        config.add_policies_from_config(config_str, false).unwrap();

        // Should match
        let upper_rules = config.create_compaction_rules("TEMPERATURE");
        assert!(upper_rules.is_some());

        // Should not match (case-sensitive)
        let lower_rules = config.create_compaction_rules("temperature");
        assert!(lower_rules.is_none());
    }

    #[test]
    fn test_replace_vs_append_policies() {
        let mut config = PolicyConfig::new();

        // Add initial policy
        let config_str1 = "avg:60s:1h";
        config.add_policies_from_config(config_str1, false).unwrap();

        let rules1 = config.create_compaction_rules("test").unwrap();
        assert_eq!(rules1.len(), 1);

        // Append another policy
        let config_str2 = "sum:30s:30m";
        config.add_policies_from_config(config_str2, false).unwrap();

        let rules2 = config.create_compaction_rules("test").unwrap();
        assert_eq!(rules2.len(), 2); // Should have both

        // Replace with the new policy
        let config_str3 = "max:45s:45m:0";
        config.add_policies_from_config(config_str3, true).unwrap();

        let rules3 = config.create_compaction_rules("test").unwrap();
        assert_eq!(rules3.len(), 1); // Should only have the new one
        assert!(rules3.contains_key("test_MAX_45000"));
    }
}

#[cfg(test)]
mod rts_grammar_parity {
    use super::*;

    /// RTS 8.6 accepts every one of these policy strings (verified black-box
    /// against the pinned reference image; compat plan §7.1). `twa` variants
    /// are excluded — the TWA aggregator itself is not implemented yet.
    #[test]
    fn accepts_rts_grammar() {
        for s in [
            "max:1M:1h",
            "avg:2h:10d",
            "avg:2h:10d:2d", // alignment larger than the bucket (modular)
            "std.p:1m:1h",
            "avg:1h:0",      // zero retention
            "max:1M:1h;min:10s:5d",
            "max:1M:1h;min:10s:5d:5m",
        ] {
            assert!(
                parse_compaction_policies(s).is_ok(),
                "RTS accepts {s:?}; our parser rejected it"
            );
        }
    }

    #[test]
    fn rejects_invalid_grammar() {
        for s in ["max:1M", "max", "bogus:x:y"] {
            assert!(
                parse_compaction_policies(s).is_err(),
                "RTS rejects {s:?}; our parser accepted it"
            );
        }
    }
}
