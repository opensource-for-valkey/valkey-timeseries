# Label and Metric Discovery

## Overview

As time-series datasets grow in cardinality and complexity, finding the right metrics and labels can become a challenge.
Traditional approaches often force client applications (such as UI dashboards or autocomplete widgets) to download
large, unfiltered datasets just to find a few relevant label names or values. This results in slow load times, high
bandwidth consumption, and degraded user experiences.

Valkey TimeSeries provides a set of discovery commands—`TS.METRICNAMES`, `TS.LABELNAMES`, and `TS.LABELVALUES`—designed
specifically to solve these problems. Inspired by the need for better high-cardinality exploration in modern
observability platforms, these commands push filtering, fuzzy matching, and sorting down to the server.

This can drastically improve DX by allowing users and UI components to find exactly what they need instantly, without
transferring unnecessary data over the network.

## The Discovery Commands

Valkey TimeSeries offers three primary commands for metadata discovery:

1. **`TS.METRICNAMES`**: Searches for metric names (i.e., the `__name__` label).
2. **`TS.LABELNAMES`**: Searches for all available label keys across your series.
3. **`TS.LABELVALUES`**: Searches for all available values for a specific label key.

All three commands share a common set of search, filtering, and sorting capabilities.

## Key Features

### 1. Server-Side Filtering and Limits

Instead of returning every label in the database, you can restrict your search space using series selectors (the
`FILTER` argument) and time ranges (`FILTER_BY_RANGE`). You can also cap the number of results returned using `LIMIT`.
This eliminates the need to download unnecessary label data to the client just to populate a dropdown menu.

### 2. Fuzzy Matching and Typo Tolerance

The `SEARCH` argument allows you to provide one or more search terms. You can enable fuzzy matching using algorithms
like `jarowinkler` or `subsequence`, and tune how strict the match should be with `FUZZY_THRESHOLD`.
Users frequently mistype metric names or only remember parts of a label (e.g., typing "mem" when searching for "
memory_usage"). Fuzzy matching provides highly relevant autocomplete suggestions instantly.

To combine terms with boolean logic instead, pass one `SEARCH_EXPR` in place of `SEARCH`, for example
`TS.METRICNAMES SEARCH_EXPR "cpu AND NOT (idle OR steal)"`. Terms combine with `AND`, `OR`, `NOT` and parentheses;
positive terms match with the same fuzzy settings as `SEARCH` terms, negated ones as plain substrings, and every
accepting path must require a positive term.

#### Supported Algorithms

* **Jaro-Winkler (`jarowinkler`)**: Specifically designed for short strings like names and labels. It measures the edit
  distance between strings, giving more weight to strings that match from the beginning. This is ideal for handling
  typos or character transpositions.
* **Subsequence (`subsequence`)**: Matches results where the search term exists as a subsequence (the characters appear
  in the target string in the same relative order, but not necessarily contiguously). For example, searching `nde` would
  match `node`. This is useful for "fuzzy" shorthand typing.

The `FUZZY_THRESHOLD` (ranging from `0.0` to `1.0`) controls the sensitivity. A threshold of `1.0` requires an exact
match (or perfect similarity), while lower values allow for increasing degrees of fuzziness.

### 3. Relevance Sorting

Results can be sorted alphabetically (`value`), by occurrence (`cardinality`), or by how well they matched the search
terms (`score`).
When building drill-down menus or autocomplete fields, the most relevant or most frequently used labels should appear at
the top.

### 4. Rich Metadata

By including the `INCLUDE_METADATA` flag, the commands return not just the string names, but also the relevance
`score` and the `cardinality` (the number of times that label/value appears).
UI clients and AI/ML query assistants can use this metadata to make informed decisions about which queries to run next
or how to render exploration paths.

### 5. Cluster-Aware Fanout

In a Valkey cluster, these commands automatically fan out to all nodes. The coordinator node merges the results,
recalculates global cardinalities, and applies the final sorts and limits before returning a unified response to the
client.

## Command Syntax Summary

All discovery commands share a similar syntax profile:

```text
TS.<COMMAND> [label]
  [SEARCH term [term ...] | SEARCH_EXPR expression]
  [FUZZY_THRESHOLD threshold]
  [FUZZY_ALGORITHM jarowinkler|subsequence]
  [IGNORE_CASE]
  [INCLUDE_METADATA]
  [SORTBY <value|score|cardinality> [ASC|DESC]]
  [FILTER_BY_RANGE [NOT] fromTimestamp toTimestamp]
  [LIMIT limit]
  [FILTER selector ...]
```

*(Note: `TS.LABELVALUES` requires a `label` name as its first argument).*

## Practical Examples

### Autocomplete for a Metric Name

A user is typing "cpu" into an explorer UI. You want to provide the top 5 closest matching metric names,
case-insensitively, sorting the best matches first:

```text
TS.METRICNAMES SEARCH cpu IGNORE_CASE FUZZY_ALGORITHM jarowinkler SORTBY score DESC LIMIT 5
```

### Drill-down by Environment

You want to see all the different regions where your `api_server` is running, but only for the `production` environment:

```text
TS.LABELVALUES region FILTER service=api_server env=production
```

### Finding High-Cardinality Labels

You want to find the most frequently used label names in your dataset to understand your cardinality distribution,
including the exact counts (`INCLUDE_METADATA`):

```text
TS.LABELNAMES INCLUDE_METADATA SORTBY cardinality DESC LIMIT 10
```