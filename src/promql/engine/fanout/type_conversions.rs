use crate::common::Sample;
use crate::common::constants::METRIC_NAME_LABEL;
use crate::labels::filters::{
    FilterList, LabelFilter, MatchOp, OrFiltersList, PredicateMatch, PredicateValue, RegexMatcher,
    SeriesSelector,
};
use crate::labels::{InternedLabel, Label, MetricName, SeriesLabel};
use crate::promql::generated::{
    InstantSample as ProtoInstantSample, Label as ProtoLabel, LabelMatcher as ProtoLabelMatcher,
    LabelMatcherList, OrMatcherList, RangeSample as ProtoRangeSample, Sample as ProtoSample,
    SeriesSelector as ProtoSeriesSelector, label_matcher, series_selector,
    series_selector::Matchers as ProtoMatchers,
};
use crate::promql::{EvalSample, RangeSample};
use ahash::AHashMap;
use promql_parser::label::{Matcher, Matchers};
use promql_parser::parser::VectorSelector;
use valkey_module::ValkeyError;

impl TryFrom<i32> for MatchOp {
    type Error = ValkeyError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        let op = label_matcher::Type::try_from(value)?;
        Ok(op.into())
    }
}

impl From<Matcher> for ProtoLabelMatcher {
    fn from(matcher: Matcher) -> Self {
        use promql_parser::label::MatchOp;
        match matcher.op {
            MatchOp::Equal => ProtoLabelMatcher {
                r#type: label_matcher::Type::Eq.into(),
                name: matcher.name,
                value: matcher.value,
            },
            MatchOp::NotEqual => ProtoLabelMatcher {
                r#type: label_matcher::Type::Neq.into(),
                name: matcher.name,
                value: matcher.value,
            },
            MatchOp::Re(re) => ProtoLabelMatcher {
                r#type: label_matcher::Type::Re as i32,
                name: matcher.name,
                value: re.as_str().to_string(),
            },
            MatchOp::NotRe(re) => ProtoLabelMatcher {
                r#type: label_matcher::Type::Nre as i32,
                name: matcher.name,
                value: re.as_str().to_string(),
            },
        }
    }
}

impl From<&Matcher> for ProtoLabelMatcher {
    fn from(matcher: &Matcher) -> Self {
        use promql_parser::label::MatchOp;
        match &matcher.op {
            MatchOp::Equal => ProtoLabelMatcher {
                r#type: label_matcher::Type::Eq.into(),
                name: matcher.name.clone(),
                value: matcher.value.clone(),
            },
            MatchOp::NotEqual => ProtoLabelMatcher {
                r#type: label_matcher::Type::Neq.into(),
                name: matcher.name.clone(),
                value: matcher.value.clone(),
            },
            MatchOp::Re(re) => ProtoLabelMatcher {
                r#type: label_matcher::Type::Re as i32,
                name: matcher.name.clone(),
                value: re.as_str().to_string(),
            },
            MatchOp::NotRe(re) => ProtoLabelMatcher {
                r#type: label_matcher::Type::Nre as i32,
                name: matcher.name.clone(),
                value: re.as_str().to_string(),
            },
        }
    }
}

impl TryFrom<&Matchers> for ProtoMatchers {
    type Error = ValkeyError;

    fn try_from(matchers: &Matchers) -> Result<Self, Self::Error> {
        if !matchers.matchers.is_empty() {
            // If there are any matchers, we need to convert them to ProtoMatchers
            Ok(ProtoMatchers::AndFilters(LabelMatcherList {
                matchers: matchers
                    .matchers
                    .iter()
                    .map(ProtoLabelMatcher::from)
                    .collect(),
            }))
        } else if !matchers.or_matchers.is_empty() {
            let filters = matchers
                .or_matchers
                .iter()
                .map(|group| LabelMatcherList {
                    matchers: group.iter().map(ProtoLabelMatcher::from).collect(),
                })
                .collect();
            Ok(ProtoMatchers::OrFilters(OrMatcherList { filters }))
        } else {
            // If there are no matchers, we can return an empty And matcher (or we could define a separate variant for this case)
            Ok(ProtoMatchers::AndFilters(LabelMatcherList {
                matchers: Vec::new(),
            }))
        }
    }
}

impl From<Matchers> for ProtoMatchers {
    fn from(matchers: Matchers) -> Self {
        if !matchers.matchers.is_empty() {
            // If there are any matchers, we need to convert them to ProtoMatchers
            ProtoMatchers::AndFilters(LabelMatcherList {
                matchers: matchers
                    .matchers
                    .into_iter()
                    .map(ProtoLabelMatcher::from)
                    .collect(),
            })
        } else if !matchers.or_matchers.is_empty() {
            let filters = matchers
                .or_matchers
                .into_iter()
                .map(|group| LabelMatcherList {
                    matchers: group.into_iter().map(ProtoLabelMatcher::from).collect(),
                })
                .collect();
            ProtoMatchers::OrFilters(OrMatcherList { filters })
        } else {
            // If there are no matchers, we can return an empty And matcher (or we could define a separate variant for this case)
            ProtoMatchers::AndFilters(LabelMatcherList {
                matchers: Vec::new(),
            })
        }
    }
}

impl From<Matchers> for ProtoSeriesSelector {
    fn from(matchers: Matchers) -> Self {
        ProtoSeriesSelector {
            matchers: Some(matchers.into()),
        }
    }
}

impl From<ProtoLabel> for Label {
    fn from(proto: ProtoLabel) -> Self {
        Label {
            name: proto.name,
            value: proto.value,
        }
    }
}

impl From<Label> for ProtoLabel {
    fn from(label: Label) -> Self {
        ProtoLabel {
            name: label.name,
            value: label.value,
        }
    }
}

impl From<Sample> for ProtoSample {
    fn from(sample: Sample) -> Self {
        ProtoSample {
            timestamp: sample.timestamp,
            value: sample.value,
        }
    }
}

impl From<ProtoSample> for Sample {
    fn from(proto: ProtoSample) -> Self {
        Sample {
            timestamp: proto.timestamp,
            value: proto.value,
        }
    }
}

impl From<InternedLabel<'_>> for ProtoLabel {
    fn from(label: InternedLabel) -> Self {
        ProtoLabel {
            name: label.name().to_string(),
            value: label.value().to_string(),
        }
    }
}

impl From<MetricName> for Vec<ProtoLabel> {
    fn from(metric_name: MetricName) -> Self {
        metric_name.iter().map(ProtoLabel::from).collect()
    }
}

impl From<label_matcher::Type> for MatchOp {
    fn from(proto_type: label_matcher::Type) -> Self {
        match proto_type {
            label_matcher::Type::Eq => MatchOp::Equal,
            label_matcher::Type::Neq => MatchOp::NotEqual,
            label_matcher::Type::Re => MatchOp::RegexEqual,
            label_matcher::Type::Nre => MatchOp::RegexNotEqual,
        }
    }
}

impl TryFrom<ProtoLabelMatcher> for LabelFilter {
    type Error = ValkeyError;

    fn try_from(proto: ProtoLabelMatcher) -> Result<Self, Self::Error> {
        let operator: MatchOp = proto.r#type.try_into()?;
        let matcher = match operator {
            MatchOp::Equal | MatchOp::NotEqual => {
                let value = if proto.value.is_empty() {
                    PredicateValue::Empty
                } else {
                    PredicateValue::String(proto.value.clone())
                };
                if operator == MatchOp::Equal {
                    PredicateMatch::Equal(value)
                } else {
                    PredicateMatch::NotEqual(value)
                }
            }
            MatchOp::RegexEqual | MatchOp::RegexNotEqual => {
                // For regex matchers, an empty value doesn't make sense. We can choose to treat it as matching nothing or everything.
                // Here we will treat it as matching nothing (i.e., it won't match any series).
                if proto.value.is_empty() {
                    panic!("Invalid regex matcher with empty value");
                }
                let matcher = RegexMatcher::create(&proto.value)
                    .map_err(|e| ValkeyError::String(e.to_string()))?;
                if operator == MatchOp::RegexEqual {
                    PredicateMatch::RegexEqual(matcher)
                } else {
                    PredicateMatch::RegexNotEqual(matcher)
                }
            }
        };

        Ok(LabelFilter {
            label: proto.name,
            matcher,
        })
    }
}

impl TryFrom<ProtoSeriesSelector> for SeriesSelector {
    type Error = ValkeyError;

    fn try_from(value: ProtoSeriesSelector) -> Result<Self, Self::Error> {
        match value.matchers {
            Some(ProtoMatchers::AndFilters(and_filters)) => {
                let mut filters = FilterList::default();
                for filter in and_filters.matchers.into_iter() {
                    let f = filter.try_into()?;
                    filters.push(f);
                }
                Ok(SeriesSelector::And(filters))
            }
            Some(ProtoMatchers::OrFilters(or_filters)) => {
                let mut or_list: OrFiltersList = OrFiltersList::default();
                for and_filters in or_filters.filters.into_iter() {
                    let mut filters = FilterList::default();
                    for group in and_filters.matchers.into_iter() {
                        let g = group.try_into()?;
                        filters.push(g);
                    }
                    or_list.push(filters);
                }
                Ok(SeriesSelector::Or(or_list))
            }
            _ => Err(ValkeyError::Str(
                "Invalid matcher type in SeriesSelector conversion",
            )),
        }
    }
}

impl From<ProtoInstantSample> for EvalSample {
    fn from(proto: ProtoInstantSample) -> Self {
        let mut labels: AHashMap<String, String> = AHashMap::default();
        for label in proto.labels.iter() {
            labels.insert(label.name.clone(), label.value.clone());
        }

        EvalSample {
            timestamp_ms: proto.timestamp,
            value: proto.value,
            labels: labels.into(),
            drop_name: false,
        }
    }
}

impl From<ProtoRangeSample> for RangeSample {
    fn from(proto: ProtoRangeSample) -> Self {
        let labels = proto
            .labels
            .into_iter()
            .map(|l| l.into())
            .collect::<Vec<Label>>()
            .into();

        let samples = proto.samples.into_iter().map(|s| s.into()).collect();

        RangeSample { labels, samples }
    }
}

impl From<Matcher> for LabelFilter {
    fn from(matcher: Matcher) -> Self {
        let operator = match matcher.op {
            promql_parser::label::MatchOp::Equal => MatchOp::Equal,
            promql_parser::label::MatchOp::NotEqual => MatchOp::NotEqual,
            promql_parser::label::MatchOp::Re(_re) => MatchOp::RegexEqual,
            promql_parser::label::MatchOp::NotRe(_re) => MatchOp::RegexNotEqual,
        };
        let predicate = match operator {
            MatchOp::Equal | MatchOp::NotEqual => {
                let value = if matcher.value.is_empty() {
                    PredicateValue::Empty
                } else {
                    PredicateValue::String(matcher.value.clone())
                };
                if operator == MatchOp::Equal {
                    PredicateMatch::Equal(value)
                } else {
                    PredicateMatch::NotEqual(value)
                }
            }
            MatchOp::RegexEqual | MatchOp::RegexNotEqual => {
                // For regex matchers, an empty value doesn't make sense. We can choose to treat it as matching nothing or everything.
                // Here we will treat it as matching nothing (i.e., it won't match any series).
                if matcher.value.is_empty() {
                    panic!("Invalid regex matcher with empty value");
                }
                let regex_matcher =
                    RegexMatcher::create(&matcher.value).expect("Failed to create regex matcher");
                if operator == MatchOp::RegexEqual {
                    PredicateMatch::RegexEqual(regex_matcher)
                } else {
                    PredicateMatch::RegexNotEqual(regex_matcher)
                }
            }
        };

        LabelFilter {
            label: matcher.name,
            matcher: predicate,
        }
    }
}

impl From<&Matcher> for LabelFilter {
    fn from(matcher: &Matcher) -> Self {
        let operator = match &matcher.op {
            promql_parser::label::MatchOp::Equal => MatchOp::Equal,
            promql_parser::label::MatchOp::NotEqual => MatchOp::NotEqual,
            promql_parser::label::MatchOp::Re(_re) => MatchOp::RegexEqual,
            promql_parser::label::MatchOp::NotRe(_re) => MatchOp::RegexNotEqual,
        };
        let predicate = match operator {
            MatchOp::Equal | MatchOp::NotEqual => {
                let value = if matcher.value.is_empty() {
                    PredicateValue::Empty
                } else {
                    PredicateValue::String(matcher.value.clone())
                };
                if operator == MatchOp::Equal {
                    PredicateMatch::Equal(value)
                } else {
                    PredicateMatch::NotEqual(value)
                }
            }
            MatchOp::RegexEqual | MatchOp::RegexNotEqual => {
                if matcher.value.is_empty() {
                    panic!("Invalid regex matcher with empty value");
                }
                let regex_matcher =
                    RegexMatcher::create(&matcher.value).expect("Failed to create regex matcher");
                if operator == MatchOp::RegexEqual {
                    PredicateMatch::RegexEqual(regex_matcher)
                } else {
                    PredicateMatch::RegexNotEqual(regex_matcher)
                }
            }
        };

        LabelFilter {
            label: matcher.name.clone(),
            matcher: predicate,
        }
    }
}

impl From<Matchers> for SeriesSelector {
    fn from(matchers: Matchers) -> Self {
        if !matchers.matchers.is_empty() {
            let mut filters = FilterList::default();
            for filter in matchers.matchers.into_iter().map(|m| m.into()) {
                filters.push(filter);
            }
            SeriesSelector::And(filters)
        } else if !matchers.or_matchers.is_empty() {
            let mut or_list: OrFiltersList = OrFiltersList::default();
            for and_filter in matchers.or_matchers.into_iter() {
                let mut filters = FilterList::default();
                for filter in and_filter.into_iter().map(|m| m.into()) {
                    filters.push(filter);
                }
                or_list.push(filters);
            }
            SeriesSelector::Or(or_list)
        } else {
            // If there are no matchers, we can return an empty And selector (or we could define a separate variant for this case)
            SeriesSelector::And(FilterList::default())
        }
    }
}

impl From<VectorSelector> for SeriesSelector {
    fn from(vs: VectorSelector) -> Self {
        let mut selector = SeriesSelector::from(vs.matchers);
        if let Some(name) = vs.name {
            let name_filter = LabelFilter::equals(METRIC_NAME_LABEL.to_string(), &name);
            match &mut selector {
                SeriesSelector::And(filters) => {
                    filters.insert(0, name_filter);
                }
                SeriesSelector::Or(or_list) => {
                    for filters in or_list.iter_mut() {
                        filters.insert(0, name_filter.clone());
                    }
                }
            }
        }
        selector
    }
}

impl From<&VectorSelector> for SeriesSelector {
    fn from(vs: &VectorSelector) -> Self {
        // Convert from borrowed VectorSelector by reusing the existing From<&Matchers>
        // implementation to build the base selector, then prepend the __name__ filter
        let mut selector = SeriesSelector::from(&vs.matchers);
        if let Some(ref name) = vs.name {
            let name_filter = LabelFilter::equals(METRIC_NAME_LABEL.to_string(), name);
            match &mut selector {
                SeriesSelector::And(filters) => {
                    filters.insert(0, name_filter);
                }
                SeriesSelector::Or(or_list) => {
                    for filters in or_list.iter_mut() {
                        filters.insert(0, name_filter.clone());
                    }
                }
            }
        }
        selector
    }
}

impl From<&Matchers> for SeriesSelector {
    fn from(matchers: &Matchers) -> Self {
        if !matchers.matchers.is_empty() {
            let mut filters = FilterList::default();
            for filter in matchers.matchers.iter().map(LabelFilter::from) {
                filters.push(filter);
            }
            SeriesSelector::And(filters)
        } else if !matchers.or_matchers.is_empty() {
            let mut or_list: OrFiltersList = OrFiltersList::default();
            for and_filter in matchers.or_matchers.iter() {
                let mut filters = FilterList::default();
                for filter in and_filter.iter().map(LabelFilter::from) {
                    filters.push(filter);
                }
                or_list.push(filters);
            }
            SeriesSelector::Or(or_list)
        } else {
            SeriesSelector::And(FilterList::default())
        }
    }
}

impl From<VectorSelector> for ProtoSeriesSelector {
    fn from(vs: VectorSelector) -> Self {
        let mut selector = ProtoSeriesSelector::from(vs.matchers);
        if let Some(name) = vs.name {
            let name_filter = ProtoLabelMatcher {
                name: METRIC_NAME_LABEL.to_string(),
                value: name.to_string(),
                r#type: label_matcher::Type::Eq as i32,
            };
            match selector.matchers {
                Some(series_selector::Matchers::AndFilters(ref mut filters)) => {
                    filters.matchers.insert(0, name_filter);
                }
                Some(series_selector::Matchers::OrFilters(ref mut or_list)) => {
                    for filters in or_list.filters.iter_mut() {
                        filters.matchers.insert(0, name_filter.clone());
                    }
                }
                _ => {}
            }
        }
        selector
    }
}

impl From<&VectorSelector> for ProtoSeriesSelector {
    fn from(vs: &VectorSelector) -> Self {
        // Build a ProtoSeriesSelector from a borrowed VectorSelector without cloning the
        // whole VectorSelector. We reuse the TryFrom<&Matchers> impl to build the
        // inner ProtoMatchers, then insert the __name__ matcher if a metric name is set.
        let mut selector = ProtoSeriesSelector {
            matchers: Some(
                ProtoMatchers::try_from(&vs.matchers)
                    .expect("invalid matchers when converting VectorSelector to proto"),
            ),
        };

        if let Some(ref name) = vs.name {
            let name_filter = ProtoLabelMatcher {
                name: METRIC_NAME_LABEL.to_string(),
                value: name.to_string(),
                r#type: label_matcher::Type::Eq as i32,
            };
            match selector.matchers {
                Some(series_selector::Matchers::AndFilters(ref mut filters)) => {
                    filters.matchers.insert(0, name_filter);
                }
                Some(series_selector::Matchers::OrFilters(ref mut or_list)) => {
                    for filters in or_list.filters.iter_mut() {
                        filters.matchers.insert(0, name_filter.clone());
                    }
                }
                _ => {}
            }
        }

        selector
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use promql_parser::label::{MatchOp as PromMatchOp, Matcher, Matchers};
    use promql_parser::parser::VectorSelector;

    #[test]
    fn test_vector_selector_to_series_selector_with_name() {
        let vs = VectorSelector {
            name: Some("http_requests_total".to_string()),
            matchers: Matchers {
                matchers: vec![Matcher::new(PromMatchOp::Equal, "job", "api")],
                or_matchers: vec![],
            },
            offset: None,
            at: None,
        };

        let selector = SeriesSelector::from(vs);
        match selector {
            SeriesSelector::And(filters) => {
                assert_eq!(filters.len(), 2);
                assert_eq!(filters[0].label, "__name__");
                assert_eq!(filters[0].op(), MatchOp::Equal);
                assert!(filters[0].matches("http_requests_total"));
                assert_eq!(filters[1].label, "job");
                assert_eq!(filters[1].op(), MatchOp::Equal);
                assert!(filters[1].matches("api"));
            }
            _ => panic!("Expected SeriesSelector::And"),
        }
    }

    #[test]
    fn test_vector_selector_to_series_selector_without_name() {
        let vs = VectorSelector {
            name: None,
            matchers: Matchers {
                matchers: vec![Matcher::new(PromMatchOp::Equal, "job", "api")],
                or_matchers: vec![],
            },
            offset: None,
            at: None,
        };

        let selector = SeriesSelector::from(vs);
        match selector {
            SeriesSelector::And(filters) => {
                assert_eq!(filters.len(), 1);
                assert_eq!(filters[0].label, "job");
            }
            _ => panic!("Expected SeriesSelector::And"),
        }
    }

    #[test]
    fn test_vector_selector_to_series_selector_with_or_matchers() {
        let vs = VectorSelector {
            name: Some("http_requests_total".to_string()),
            matchers: Matchers {
                matchers: vec![],
                or_matchers: vec![
                    vec![Matcher::new(PromMatchOp::Equal, "job", "api")],
                    vec![Matcher::new(PromMatchOp::Equal, "job", "worker")],
                ],
            },
            offset: None,
            at: None,
        };

        let selector = SeriesSelector::from(vs);
        match selector {
            SeriesSelector::Or(or_list) => {
                assert_eq!(or_list.len(), 2);
                for filters in or_list.iter() {
                    assert_eq!(filters.len(), 2);
                    assert_eq!(filters[0].label, "__name__");
                    assert!(filters[0].matches("http_requests_total"));
                }
                assert!(or_list[0][1].matches("api"));
                assert!(or_list[1][1].matches("worker"));
            }
            _ => panic!("Expected SeriesSelector::Or"),
        }
    }
}
