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

The `SEARCH` argument takes one or more search terms, and a value matches if it matches any of them. Matching is
fuzzy by default: without `FUZZY_ALGORITHM`, terms match by `subsequence`, as in the Prometheus search API. Choose
`jarowinkler` instead with `FUZZY_ALGORITHM`, and tune how strict the match should be with `FUZZY_THRESHOLD`. Users frequently mistype metric names or only remember parts of a label (e.g., typing "mem" when
searching for "memory_usage"). Fuzzy matching provides highly relevant autocomplete suggestions instantly.

A request may carry at most 32 `SEARCH` terms, counting every `SEARCH` keyword in it. To require or exclude terms
rather than accept any of them, use a [boolean search expression](#3-boolean-search-expressions) instead.

#### Supported Algorithms

* **Subsequence (`subsequence`, the default)**: Matches results where the search term exists as a subsequence (the
  characters appear in the target string in the same relative order, but not necessarily contiguously). For example,
  searching `nde` would match `node`. This is useful for "fuzzy" shorthand typing. At the default threshold of `0` it
  is permissive: `cpu` also matches `c_p_u_total`. Raise `FUZZY_THRESHOLD` to prefer contiguous matches.
* **Jaro-Winkler (`jarowinkler`)**: Specifically designed for short strings like names and labels. It measures the edit
  distance between strings, giving more weight to strings that match from the beginning. This is ideal for handling
  typos or character transpositions. At the default threshold of `0` it matches only values that contain the term.

How a term matches depends on the algorithm and on `FUZZY_THRESHOLD`, a whole percentage from `0` to `100` (default
`0`), as in the Prometheus search API. Scores are always between `0.0` and `1.0`, and a threshold of `N` requires a
fuzzy score of at least `N / 100`: `80` requires `0.8`.

* `jarowinkler`: a value that contains the term always matches, scoring `1.0` when it starts with the term and less the
  later the term occurs (down to `0.1`). With a threshold above `0`, a value that does not contain the term also
  matches when its Jaro-Winkler similarity to the term is at least the threshold, and scores that similarity. At `0`,
  only values that contain the term match.
* `subsequence`: a value that starts with the term always matches, scoring `1.0`. Any other value must contain the
  term's characters in order and have a subsequence score of at least the threshold. At `0`, any such value matches.

The threshold only narrows the fuzzy matches. `100` is not an exact match: a value that contains the term
(`jarowinkler`) or starts with it (`subsequence`) matches at any threshold. A threshold that is not a whole number in
that range, such as `0.8` or `101`, fails with `TSDB: FUZZY_THRESHOLD must be a whole number in [0..100]`.

### 3. Boolean Search Expressions

`SEARCH_EXPR` takes one expression in place of `SEARCH` terms. It combines terms with `AND`, `OR`, `NOT` and
parentheses, so a single request can require several terms and exclude others:

```text
TS.METRICNAMES SEARCH_EXPR "cpu AND NOT (idle OR steal)"
```

This returns metric names that contain `cpu` but neither `idle` nor `steal`, such as `node_cpu_seconds_total`, while
leaving out `node_cpu_idle_seconds`.

#### Syntax

* `NOT` binds more tightly than `AND`, which binds more tightly than `OR`: `a OR b AND NOT c` means `a OR (b AND (NOT
  c))`. Use parentheses to group terms differently.
* Operators are case-insensitive: `and`, `And` and `AND` are the same operator.
* Every pair of terms needs an operator between them. There is no implied `AND`, so `cpu idle` is an error; write
  `cpu AND idle`.
* Quote a term to search for text that would otherwise be read as an operator or split at a space, such as `"and"` or
  `"node cpu"`. Inside quotes, `\"` and `\\` are the only escapes.
* The whole expression is a single command argument, so quote it as your client requires, as in the example above.

#### Matching

* Positive terms match the way `SEARCH` terms do, honouring `FUZZY_ALGORITHM`, `FUZZY_THRESHOLD` and `IGNORE_CASE`.
* Negated terms match as plain substrings, honouring `IGNORE_CASE` only. A fuzzy `NOT` would also exclude values that
  merely resemble the term.
* Every way an expression can accept a value must require at least one positive term. An expression that could accept
  a value on negations alone is refused, for example `NOT cpu` or `cpu OR NOT idle`.

#### Scoring

A match scores the mean of the scores of the positive terms it matched. When an `OR` leaves more than one way to
match, the value gets the best of those means. Negated terms do not contribute to the score.

#### Limits and errors

* An expression is limited to 4096 bytes, 32 terms and 16 levels of nesting, where each `NOT` and each parenthesised
  group adds one level.
* `SEARCH` and `SEARCH_EXPR` are mutually exclusive, and `SEARCH_EXPR` may be given only once. An empty expression is
  refused.
* An invalid expression fails with an error that starts with `TSDB: search expression:` and gives the byte position of
  the problem, for example `TSDB: search expression: unterminated quoted term at position 4`.
* In a cluster, the coordinator checks the expression before fanning out, so an invalid expression fails once instead
  of on every shard.

### 4. Relevance Sorting

Results can be sorted alphabetically (`value`), by occurrence (`cardinality`), or by how well they matched the search
terms (`score`). Without `SORTBY`, a request with `SEARCH` or `SEARCH_EXPR` sorts by `score`, best first, and any
other request sorts by `value`.
When building drill-down menus or autocomplete fields, the most relevant or most frequently used labels should appear at
the top.

### 5. Rich Metadata

By including the `INCLUDE_METADATA` flag, the commands return not just the string names, but also the relevance
`score` and the `cardinality` (the number of times that label/value appears).
UI clients and AI/ML query assistants can use this metadata to make informed decisions about which queries to run next
or how to render exploration paths.

### 6. Cluster-Aware Fanout

In a Valkey cluster, these commands automatically fan out to all nodes. The coordinator node merges the results,
recalculates global cardinalities, and applies the final sorts and limits before returning a unified response to the
client.

## Command Syntax Summary

All discovery commands share a similar syntax profile:

```text
TS.<COMMAND> [label]
  [SEARCH term [term ...] | SEARCH_EXPR expression]
  [FUZZY_THRESHOLD threshold]
  [FUZZY_ALGORITHM subsequence|jarowinkler]
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

### Excluding Noise with a Boolean Expression

A user wants CPU metrics but not the idle or steal counters, which would otherwise crowd the top of the list. The
expression requires `cpu` and excludes both, case-insensitively:

```text
TS.METRICNAMES SEARCH_EXPR "cpu AND NOT (idle OR steal)" IGNORE_CASE LIMIT 20
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