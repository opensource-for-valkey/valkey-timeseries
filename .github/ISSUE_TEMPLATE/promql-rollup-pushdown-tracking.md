---
name: PromQL Rollup Pushdown Implementation Tracking
about: Track implementation milestones, semantic gates, and rollout readiness for PromQL rollup pushdown
title: "PromQL rollup pushdown: implementation tracking"
labels: ["promql", "enhancement", "tracking"]
assignees: []
---

## Objective

Track end-to-end delivery of PromQL rollup pushdown as defined in docs/promql-rollup-pushdown-plan.md.

## Scope

- Implement rollup pushdown safely with semantic parity.
- Support mixed-version fallback behavior.
- Add conformance and cluster coverage before default-on.

## Non-Goals

- No pushdown for absent_over_time.
- No default-on until semantic and operational gates are complete.

---

## Milestone Status

- [ ] PR 0 complete: semantic lock, no protocol changes
- [ ] PR 1 complete: instant-only protocol skeleton behind config flag
- [ ] PR 2 complete: full range-grid support in one fanout
- [ ] PR 3 complete: broaden function support set
- [ ] PR 4 complete: outer aggregation fusion

---

## PR 0: Semantic Lock (Blocker)

### Gate G1: Range-query rollup step semantics

- [ ] Confirm expected multi-step behavior for rollups in range queries
- [ ] Add golden parity tests against Prometheus for sparse and dense inputs
- [ ] Verify no accidental last-step-only behavior

Evidence:
- Tests/PR links:
- Notes:

### Gate G2: Label and metric-name parity

- [ ] Document label retention/drop-name behavior for rollups
- [ ] Add explicit tests for sum_over_time, rate, changes, quantile_over_time
- [ ] Ensure local and pushed-down paths share one rule

Evidence:
- Tests/PR links:
- Notes:

### Gate G3: Missing-window vs NaN semantics

- [ ] Represent empty-window output as missing series-step samples
- [ ] Distinguish genuine NaN from no-output windows in tests
- [ ] Validate exact output shape parity

Evidence:
- Tests/PR links:
- Notes:

---

## PR 1: Protocol Skeleton (Instant Queries Only)

### API and Protocol

- [ ] Add RollupQuery and RollupQueryResponse wire types
- [ ] Add RollupKind enum and optional scalar parameter support
- [ ] Add applied handshake semantics
- [ ] Add coordinator fallback path when applied=false

### Engine Integration

- [ ] Add RollupFanoutCommand
- [ ] Add query_rollup API in QueryReader (default Unsupported)
- [ ] Add selector executor task path for rollup fanout
- [ ] Add evaluator pushed-down call hook for eligible call shapes

### Feature Flag

- [ ] Add ts-fanout-rollup-pushdown config toggle
- [ ] Confirm disabled mode forces coordinator-local rollup path

Evidence:
- PR links:
- Notes:

---

## PR 2: Full Range Grid Support

- [ ] One fanout request evaluates full step grid
- [ ] Shard uses coordinator-resolved grid and window parameters
- [ ] Response transports sparse series-step values
- [ ] Coordinator merge path preserves timestamps and shape

### Mixed-Version / Compatibility

- [ ] Unknown rollup kind returns applied=false with raw data
- [ ] Unsupported envelope errors trigger compatibility fallback
- [ ] Partial-apply cluster behavior is correct and deterministic

Evidence:
- PR links:
- Notes:

---

## PR 3: Broaden Eligible Function Set

- [ ] Enable all approved window-local rollup functions
- [ ] Keep absent_over_time coordinator-only
- [ ] Respect experimental function gating from coordinator

Evidence:
- PR links:
- Notes:

---

## PR 4: Outer Aggregation Fusion

- [ ] Support shard-side rollup then partial group accumulation
- [ ] Reuse existing aggregation partial merge/finalize strategy
- [ ] Preserve correctness for group-by and step dimensions

Evidence:
- PR links:
- Notes:

---

## Limits and Resource Guardrails

- [ ] Enforce raw input max_points_per_series shard-side
- [ ] Enforce rolled-up output max_points_per_series shard-side and coordinator-side
- [ ] Define payload cap behavior for large series x steps responses
- [ ] Add chunked or streamed decode handling where required
- [ ] Define timeout behavior and failure policy

Evidence:
- PR links:
- Notes:

---

## Required Test Matrix

### Parity

- [ ] Local single-node vs pushed-down cluster equivalence
- [ ] Exact value equality where deterministic
- [ ] Exact timestamp equality
- [ ] Exact output shape equality (series-step presence)

### Edge Cases

- [ ] Sparse windows and staleness boundaries
- [ ] Counter reset edge cases for rate/increase/irate
- [ ] NaN propagation cases
- [ ] @ and offset modifier correctness
- [ ] Experimental function toggle enforcement

### Compatibility

- [ ] Mixed-version cluster with applied and non-applied peers
- [ ] Coordinator fallback path exercised in CI

Evidence:
- Test files and commands:
- CI jobs:

---

## Documentation and Rollout

- [ ] Update docs with protocol behavior and fallback semantics
- [ ] Document ts-fanout-rollup-pushdown operational guidance
- [ ] Document interaction with existing aggregation pushdown toggle
- [ ] Add rolling-upgrade behavior notes

Evidence:
- PR links:
- Notes:

---

## Definition of Done

- [ ] All milestone checkboxes complete
- [ ] Conformance matrix passes in CI
- [ ] Mixed-version fallback verified
- [ ] Feature enabled by default only after approval

## Decision Log

- Date:
- Decision:
- Rationale:
- Owners:
