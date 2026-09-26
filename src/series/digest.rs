use crate::aggregators::Aggregator;
use crate::common::hash::DeterministicHasher;
use crate::common::rounding::RoundingStrategy;
use crate::labels::MetricName;
use crate::series::{CompactionRule, SampleDuplicatePolicy, SeriesLink};
use std::hash::BuildHasher;
use valkey_module::digest::Digest;

fn calc_aggregator_digest(aggregator: &Aggregator, digest: &mut Digest) {
    let hash = DeterministicHasher::default().hash_one(aggregator);
    digest.add_string_buffer(hash.to_le_bytes().as_ref());
}

pub(super) fn calc_duplicate_policy_digest(policy: &SampleDuplicatePolicy, digest: &mut Digest) {
    // Handle sample_duplicates
    let policy_name = if let Some(policy) = policy.policy {
        policy.as_str()
    } else {
        "default"
    };
    digest.add_string_buffer(policy_name.as_bytes());
    digest.add_string_buffer(&policy.max_value_delta.to_le_bytes());
    digest.add_long_long(policy.max_time_delta as i64);
}

pub(super) fn calc_link_digest(link: Option<&SeriesLink>, digest: &mut Digest) {
    match link {
        None => digest.add_long_long(-1),
        Some(link) => digest.add_string_buffer(link.key()),
    }
}

pub(super) fn calc_compaction_digest(rule: &CompactionRule, digest: &mut Digest) {
    calc_link_digest(Some(&rule.dest), digest);
    digest.add_long_long(rule.bucket_duration as i64);
    digest.add_long_long(rule.align_timestamp);
    digest.add_long_long(rule.bucket_start.unwrap_or(-1));
    calc_aggregator_digest(&rule.aggregator, digest);
}

pub(super) fn calc_rounding_digest(rounding: &RoundingStrategy, digest: &mut Digest) {
    match rounding {
        RoundingStrategy::SignificantDigits(digits) => {
            digest.add_string_buffer(b"sig");
            digest.add_long_long(*digits as i64);
        }
        RoundingStrategy::DecimalDigits(digits) => {
            digest.add_string_buffer(b"dec");
            digest.add_long_long(*digits as i64);
        }
    };
}

pub(super) fn calc_metric_name_digest(labels: &MetricName, digest: &mut Digest) {
    digest.add_long_long(labels.len() as i64);
    for label in labels.iter() {
        digest.add_string_buffer(label.name.as_bytes());
        digest.add_string_buffer(b"=");
        digest.add_string_buffer(label.value.as_bytes());
    }
}
