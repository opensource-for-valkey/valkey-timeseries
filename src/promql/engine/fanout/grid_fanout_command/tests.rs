use super::columns::*;
use super::transport::*;
use super::*;
use crate::common::Timestamp;
use crate::labels::{Label, Labels};
use crate::promql::engine::query_reader::AggregationParam;
use crate::promql::exec::aggregations::AggregationKind;
use crate::promql::exec::pipeline::for_each_step_sample;
use crate::series::chunks::WIRE_COMPRESSION_MIN_SAMPLES;

const RANGE_MS: i64 = 60_000;
const LOOKBACK_MS: i64 = 300_000;
const EVAL_TS: Timestamp = 300_000;

fn labels(name: &str, instance: &str) -> Labels {
    Labels::new(vec![
        Label::new("__name__", name),
        Label::new("instance", instance),
    ])
}

fn series(instance: &str, points: &[(i64, f64)]) -> RangeSample<EvalLabels> {
    RangeSample {
        labels: labels("m", instance).into(),
        samples: points
            .iter()
            .map(|&(timestamp, value)| Sample { timestamp, value })
            .collect(),
    }
}

/// A single-evaluation stepped request at `EVAL_TS`, asking for the
/// picks' own timestamps so the fan-out can be compared point for point
/// with the single-node stage.
fn stepped_request() -> GridRequest {
    GridRequest {
        step_ms: 0,
        query_start: EVAL_TS,
        query_end: EVAL_TS,
        range_end_ms: EVAL_TS,
        lookback_delta_ms: LOOKBACK_MS,
        rollup: None,
        aggregation: None,
        sample_timestamps: true,
    }
}

/// The same request over a grid: eleven steps at 30s.
fn stepped_grid_request() -> GridRequest {
    GridRequest {
        step_ms: 30_000,
        query_start: 0,
        query_end: EVAL_TS,
        ..stepped_request()
    }
}

fn rollup_request(kind: RollupKind) -> GridRequest {
    GridRequest {
        rollup: Some(GridRollup {
            kind,
            range_ms: RANGE_MS,
            param: param_for(kind),
        }),
        ..stepped_request()
    }
}

/// A whole-grid rollup: eleven windows at a 30s step, each 60s wide, so
/// consecutive windows overlap — the shape the push-down exists for.
fn rollup_grid_request(kind: RollupKind) -> GridRequest {
    GridRequest {
        step_ms: 30_000,
        query_start: 0,
        query_end: EVAL_TS,
        ..rollup_request(kind)
    }
}

fn fused(base: GridRequest, agg: AggregationKind, by: &[&str]) -> GridRequest {
    fused_with(base, agg, by, None)
}

fn fused_with(
    base: GridRequest,
    agg: AggregationKind,
    by: &[&str],
    param: Option<AggregationParam>,
) -> GridRequest {
    GridRequest {
        aggregation: Some(GridAggregation {
            kind: agg,
            modifier: (!by.is_empty())
                .then(|| LabelModifier::Include(promql_parser::label::Labels::new(by.to_vec()))),
            param,
        }),
        ..base
    }
}

/// The rollup's parameter, where it takes one.
fn param_for(kind: RollupKind) -> Option<f64> {
    (kind == RollupKind::QuantileOverTime).then_some(0.9)
}

fn command(request: GridRequest) -> GridFanoutCommand {
    GridFanoutCommand::new(Matchers::empty(), request, 0, 0, Duration::from_secs(1))
}

fn node(port: u16) -> NodeInfo {
    NodeInfo::for_test(port)
}

/// A partial that covers no samples cannot come from a shard; merged, it
/// conjured a group of its own for the step.
#[test]
fn test_partial_without_samples_is_rejected() {
    use crate::promql::generated::{AggregationPartialState, Label as ProtoLabel};
    let request = fused(stepped_grid_request(), AggregationKind::Sum, &["job"]);
    let step_ts = request.window_ends()[0];
    let response = |state: Option<AggregationPartialState>| GridQueryResponse {
        series: Vec::new(),
        partials: vec![GridGroupPartial {
            labels: vec![ProtoLabel {
                name: "job".to_string(),
                value: "z".to_string(),
            }],
            step_ts,
            state,
            ..Default::default()
        }],
        ..Default::default()
    };
    for (what, state) in [
        ("no state", None),
        ("zero count", Some(AggregationPartialState::default())),
    ] {
        let mut cmd = command(request.clone());
        let err = cmd
            .on_response(response(state), &node(7000))
            .expect_err(what);
        assert!(err.to_string().contains("partial"), "{what}: {err}");
    }

    let mut cmd = command(request);
    let real = AggregationPartialState {
        count: 1,
        acc1: 5.0,
        ..Default::default()
    };
    assert!(cmd.on_response(response(Some(real)), &node(7000)).is_ok());
}

/// A peer that rejects the operation as unknown (an older build) latches the
/// fallback; an ordinary failure does not.
#[test]
fn test_unsupported_peer_latches_fallback() {
    let mut cmd = command(stepped_grid_request());
    assert!(!cmd.peer_unsupported());
    cmd.on_error(FanoutError::invalid_message(), &node(7000));
    assert!(cmd.peer_unsupported());

    let mut cmd = command(stepped_grid_request());
    cmd.on_error(FanoutError::timeout(), &node(7000));
    assert!(!cmd.peer_unsupported());
}

/// One shard's response, produced the way `get_local_response` produces it
/// once the windows have been read.
fn response(request: &GridRequest, windows: Vec<RangeSample<EvalLabels>>) -> GridQueryResponse {
    shard_response(request, &request.window_ends(), windows).unwrap()
}

/// A response carrying every series raw, whatever its size.
fn raw_response(windows: Vec<RangeSample<EvalLabels>>) -> GridQueryResponse {
    let mut symbols = SymbolTableBuilder::default();
    let raw = windows
        .into_iter()
        .map(|s| range_sample_to_proto(s, &mut symbols).unwrap())
        .collect();
    GridQueryResponse {
        raw,
        labels: Some(symbols.finish()),
        ..Default::default()
    }
}

/// One rendered point: `(step, sample timestamp, value)`.
type Point = (i64, i64, String);

/// An outcome as sorted `(labels, points)`, so results compare irrespective
/// of the order shards answered in. Stepped points carry the sample's own
/// timestamp beside the step; the others carry zero there.
fn rendered(outcome: GridOutcome) -> Vec<(String, Vec<Point>)> {
    let mut out: Vec<_> = match outcome {
        GridOutcome::Stepped(series) => series
            .into_iter()
            .map(|s| {
                let points = s
                    .points
                    .iter()
                    .map(|p| {
                        (
                            p.step_ts,
                            p.sample.timestamp,
                            format!("{:?}", p.sample.value),
                        )
                    })
                    .collect();
                (s.labels.to_string(), points)
            })
            .collect(),
        GridOutcome::Rolled(series) | GridOutcome::Reduced(series) | GridOutcome::Raw(series) => {
            series
                .into_iter()
                .map(|s| {
                    let points = s
                        .samples
                        .iter()
                        .map(|p| (p.timestamp, 0, format!("{:?}", p.value)))
                        .collect();
                    (s.labels.to_string(), points)
                })
                .collect()
        }
        GridOutcome::Unsupported => panic!("a test shard never refuses the operation"),
    };
    out.sort();
    out
}

fn test_shards() -> Vec<Vec<RangeSample<EvalLabels>>> {
    vec![
        vec![
            series("0", &[(250_000, 1.0), (260_000, 2.0), (300_000, 3.0)]),
            series("1", &[(280_000, 7.0)]),
        ],
        vec![series("2", &[(245_000, 5.0), (299_000, 9.0)])],
        // Every sample is outside the window (240s, 300]: this series must
        // not appear in a rollup result at all.
        vec![series("3", &[(100_000, 4.0)])],
    ]
}

/// Dense data, so every series has more samples than the grid has steps
/// and the shards answer in the staged form rather than raw.
fn dense_shards() -> Vec<Vec<RangeSample<EvalLabels>>> {
    let dense = |instance: &str, base: f64| {
        series(
            instance,
            &(0..=30)
                .map(|i| (i * 10_000, base + i as f64))
                .collect::<Vec<_>>(),
        )
    };
    vec![
        vec![dense("0", 0.0), dense("1", 100.0)],
        vec![dense("2", 1000.0)],
        // Ends early: absent from the later steps.
        vec![series(
            "3",
            &(0..=5).map(|i| (i * 10_000, i as f64)).collect::<Vec<_>>(),
        )],
    ]
}

/// Every request shape a range query issues, so each contract below is
/// checked for all of them.
fn request_shapes() -> Vec<(String, GridRequest)> {
    let mut shapes = vec![
        ("stepped/instant".to_string(), stepped_request()),
        ("stepped/grid".to_string(), stepped_grid_request()),
        (
            "stepped/fused sum by".to_string(),
            fused(stepped_grid_request(), AggregationKind::Sum, &["__name__"]),
        ),
        (
            "stepped/fused avg".to_string(),
            fused(stepped_grid_request(), AggregationKind::Avg, &[]),
        ),
        // The selecting and counting operators: k is larger than any one
        // shard's share, so the coordinator's re-selection has to choose
        // across shards.
        (
            "stepped/fused topk 1".to_string(),
            fused_with(
                stepped_grid_request(),
                AggregationKind::Topk,
                &[],
                Some(AggregationParam::Scalar(1.0)),
            ),
        ),
        (
            "stepped/fused topk 3".to_string(),
            fused_with(
                stepped_grid_request(),
                AggregationKind::Topk,
                &[],
                Some(AggregationParam::Scalar(3.0)),
            ),
        ),
        (
            "stepped/fused bottomk by".to_string(),
            fused_with(
                stepped_grid_request(),
                AggregationKind::Bottomk,
                &["__name__"],
                Some(AggregationParam::Scalar(2.0)),
            ),
        ),
        (
            "stepped/fused limitk".to_string(),
            fused_with(
                stepped_grid_request(),
                AggregationKind::Limitk,
                &[],
                Some(AggregationParam::Scalar(2.0)),
            ),
        ),
        (
            "stepped/fused limit_ratio".to_string(),
            fused_with(
                stepped_grid_request(),
                AggregationKind::LimitRatio,
                &[],
                Some(AggregationParam::Scalar(0.5)),
            ),
        ),
        (
            "stepped/fused count_values".to_string(),
            fused_with(
                stepped_grid_request(),
                AggregationKind::CountValues,
                &[],
                Some(AggregationParam::Label("v".into())),
            ),
        ),
        (
            "rate/fused topk".to_string(),
            fused_with(
                rollup_grid_request(RollupKind::Rate),
                AggregationKind::Topk,
                &[],
                Some(AggregationParam::Scalar(2.0)),
            ),
        ),
        (
            "sum_over_time/fused count_values by".to_string(),
            fused_with(
                rollup_grid_request(RollupKind::SumOverTime),
                AggregationKind::CountValues,
                &["__name__"],
                Some(AggregationParam::Label("v".into())),
            ),
        ),
    ];
    for kind in RollupKind::all() {
        shapes.push((format!("{kind:?}/instant"), rollup_request(kind)));
        shapes.push((format!("{kind:?}/grid"), rollup_grid_request(kind)));
        shapes.push((
            format!("{kind:?}/fused"),
            fused(
                rollup_grid_request(kind),
                AggregationKind::Sum,
                &["__name__"],
            ),
        ));
    }
    shapes
}

/// Feed every shard's response to a command and take the result.
fn fan_out(request: &GridRequest, shards: &[Vec<RangeSample<EvalLabels>>]) -> GridOutcome {
    let mut cmd = command(request.clone());
    for (index, shard) in shards.iter().enumerate() {
        cmd.on_response(response(request, shard.clone()), &node(7000 + index as u16))
            .expect("shard response accepted");
    }
    cmd.into_result().unwrap()
}

/// The push-down contract: evaluating at the shards and combining equals
/// evaluating the concatenated input on one node — for every shape, and
/// whether the shards answered staged or raw.
#[test]
fn test_fanout_matches_single_node() {
    for shards in [test_shards(), dense_shards()] {
        let all: Vec<RangeSample<EvalLabels>> = shards.iter().flatten().cloned().collect();
        for (shape, request) in request_shapes() {
            let want = rendered(request.evaluate(all.clone()).unwrap());

            assert_eq!(want, rendered(fan_out(&request, &shards)), "{shape}");

            // Shards that shipped everything raw are compensated here.
            let mut cmd = command(request.clone());
            for (index, shard) in shards.iter().enumerate() {
                cmd.on_response(raw_response(shard.clone()), &node(7000 + index as u16))
                    .unwrap();
            }
            assert_eq!(
                want,
                rendered(cmd.into_result().unwrap()),
                "{shape} (all raw)"
            );

            // And a mix: one staged shard, the rest raw.
            let mut cmd = command(request.clone());
            cmd.on_response(response(&request, shards[0].clone()), &node(7000))
                .unwrap();
            for (index, shard) in shards.iter().enumerate().skip(1) {
                cmd.on_response(raw_response(shard.clone()), &node(7000 + index as u16))
                    .unwrap();
            }
            assert_eq!(
                want,
                rendered(cmd.into_result().unwrap()),
                "{shape} (mixed)"
            );
        }
    }
}

/// The stepped stage is `for_each_step_sample`'s rule, run on the shard:
/// the last sample at or before each step and inside the lookback, with
/// the sample's own timestamp carried through.
#[test]
fn test_stepped_selection_matches_the_step_loop_rule() {
    let request = GridRequest {
        lookback_delta_ms: 25_000,
        ..stepped_grid_request()
    };
    let input = series(
        "0",
        &[(5_000, 1.0), (35_000, 2.0), (36_000, 3.0), (200_000, 4.0)],
    );
    let ends = request.window_ends();

    let mut want = Vec::new();
    for_each_step_sample(
        &input.samples,
        ends.iter().copied(),
        25_000,
        |step, latest| {
            if let Some(s) = latest {
                want.push((step, s.timestamp, format!("{:?}", s.value)));
            }
        },
    );
    // Steps 0s (no sample yet), 30s (the 5s sample is exactly one lookback
    // back, and the lower bound is exclusive), 60s (3.0 at 36s), 90s …
    // 180s (36s is beyond the lookback), 210s (4.0 at 200s), then nothing
    // again.
    assert_eq!(
        want,
        vec![
            (60_000, 36_000, "3.0".to_string()),
            (210_000, 200_000, "4.0".to_string()),
        ]
    );

    let got = rendered(request.evaluate(vec![input]).unwrap());
    assert_eq!(got, vec![("m{instance=\"0\"}".to_string(), want)]);
}

/// `@` collapses the grid onto one window end; `offset` shifts it. The
/// coordinator resolves both into the geometry, and the shard sees only
/// window ends — so an `@` request is a single-evaluation request at the
/// pinned end, and an offset one is the grid shifted uniformly.
#[test]
fn test_modifiers_are_geometry() {
    // `m @ 120` over any grid: one window end.
    let pinned = GridRequest {
        step_ms: 30_000,
        query_start: 120_000,
        query_end: 120_000,
        range_end_ms: 120_000,
        ..stepped_request()
    };
    assert_eq!(pinned.window_ends(), vec![120_000]);
    let out = request_points(&pinned, series("0", &[(100_000, 1.0), (130_000, 2.0)]));
    assert_eq!(out, vec![(120_000, 100_000, "1.0".to_string())]);

    // `m offset 1m` over 120s..240s at 60s: ends at 60s, 120s, 180s.
    let shifted = GridRequest {
        step_ms: 60_000,
        query_start: 60_000,
        query_end: 180_000,
        range_end_ms: 180_000,
        ..stepped_request()
    };
    assert_eq!(shifted.window_ends(), vec![60_000, 120_000, 180_000]);
    let out = request_points(&shifted, series("0", &[(100_000, 1.0), (130_000, 2.0)]));
    assert_eq!(
        out,
        vec![
            (120_000, 100_000, "1.0".to_string()),
            (180_000, 130_000, "2.0".to_string()),
        ]
    );
}

fn request_points(request: &GridRequest, input: RangeSample<EvalLabels>) -> Vec<Point> {
    rendered(request.evaluate(vec![input]).unwrap())
        .pop()
        .map(|(_, points)| points)
        .unwrap_or_default()
}

/// The size rule: a series with fewer samples in the span than the grid
/// has steps is shipped raw, one with more is staged — per series, in one
/// response — and the coordinator lands on the single-node answer.
#[test]
fn test_size_rule_ships_the_smaller_form() {
    let request = stepped_grid_request(); // 11 window ends
    let sparse = series("sparse", &[(0, 1.0), (150_000, 2.0)]);
    let dense = series(
        "dense",
        &(0..=30).map(|i| (i * 10_000, i as f64)).collect::<Vec<_>>(),
    );
    assert!(ships_raw(&request.window_ends(), &sparse));
    assert!(!ships_raw(&request.window_ends(), &dense));

    let resp = response(&request, vec![sparse.clone(), dense.clone()]);
    assert_eq!(resp.raw.len(), 1, "the sparse series travels raw");
    assert_eq!(resp.series.len(), 1, "the dense one travels stepped");
    assert!(resp.partials.is_empty());

    let mut cmd = command(request.clone());
    cmd.on_response(resp, &node(7000)).unwrap();
    assert_eq!(
        rendered(cmd.into_result().unwrap()),
        rendered(
            request
                .evaluate(vec![sparse.clone(), dense.clone()])
                .unwrap()
        ),
    );

    // Fused with a reduction, the two share a group, and the dense series
    // stages: the group's partials are shipped regardless, so the sparse
    // series is folded into them on the shard rather than sent raw for
    // the coordinator to fold.
    let fused = fused(request, AggregationKind::Sum, &["__name__"]);
    let resp = response(&fused, vec![sparse.clone(), dense.clone()]);
    assert!(
        resp.raw.is_empty(),
        "the sparse series joins its group's partials"
    );
    assert!(resp.series.is_empty());
    assert!(!resp.partials.is_empty());
    let mut cmd = command(fused.clone());
    cmd.on_response(resp, &node(7000)).unwrap();
    assert_eq!(
        rendered(cmd.into_result().unwrap()),
        rendered(fused.evaluate(vec![sparse, dense]).unwrap()),
    );
}

fn series_with(pairs: &[(&str, &str)], points: &[(i64, f64)]) -> RangeSample<EvalLabels> {
    RangeSample {
        labels: EvalLabels::from_pairs(pairs),
        samples: points
            .iter()
            .map(|&(timestamp, value)| Sample { timestamp, value })
            .collect(),
    }
}

fn by(labels: &[&str]) -> LabelModifier {
    LabelModifier::Include(promql_parser::label::Labels::new(labels.to_vec()))
}

fn without(labels: &[&str]) -> LabelModifier {
    LabelModifier::Exclude(promql_parser::label::Labels::new(labels.to_vec()))
}

fn fused_modifier(
    base: GridRequest,
    agg: AggregationKind,
    modifier: Option<LabelModifier>,
) -> GridRequest {
    GridRequest {
        aggregation: Some(GridAggregation {
            kind: agg,
            modifier,
            param: None,
        }),
        ..base
    }
}

/// `count` sparse series of one sample each in `job`, spread over the
/// 300s span so their samples land on different steps.
fn sparse_job(job: &str, count: usize) -> Vec<RangeSample<EvalLabels>> {
    (0..count)
        .map(|i| {
            let instance = format!("{job}-{i}");
            series_with(
                &[("__name__", "m"), ("job", job), ("instance", &instance)],
                &[((i as i64 * 7_919) % 300_001, (i % 13) as f64 + 1.0)],
            )
        })
        .collect()
}

/// Rendered results compared with the fused-aggregation tolerance: labels
/// and steps exactly, values to a relative 1e-9 — partials merged across
/// shards sum in a different order than a single-node fold.
fn assert_rendered_close(want: &[(String, Vec<Point>)], got: &[(String, Vec<Point>)], what: &str) {
    assert_eq!(want.len(), got.len(), "{what}: series count");
    for ((w_labels, w_points), (g_labels, g_points)) in want.iter().zip(got) {
        assert_eq!(w_labels, g_labels, "{what}: labels");
        assert_eq!(
            w_points.len(),
            g_points.len(),
            "{what}: {w_labels} step count"
        );
        for (w, g) in w_points.iter().zip(g_points) {
            assert_eq!((w.0, w.1), (g.0, g.1), "{what}: {w_labels} step");
            let (w, g): (f64, f64) = (w.2.parse().unwrap(), g.2.parse().unwrap());
            if w.is_nan() || g.is_nan() {
                assert_eq!(w.is_nan(), g.is_nan(), "{what}: {w_labels} NaN-ness");
                continue;
            }
            let tolerance = 1e-9 * w.abs().max(1.0);
            assert!((w - g).abs() <= tolerance, "{what}: {w_labels} {w} != {g}");
        }
    }
}

/// The reduction-aware rule: a group of series that would each travel raw
/// under the per-series rule is answered in partials instead when those
/// are the smaller form. `sum by (job) (rate(m[1m]))` over two hundred
/// one-sample series is eleven partials per group, not two hundred spans
/// for the coordinator to stage and fold one at a time.
#[test]
fn test_reduction_folds_a_sparse_group() {
    let base = rollup_grid_request(RollupKind::Rate); // 11 window ends
    let windows: Vec<_> = sparse_job("api", 200)
        .into_iter()
        .chain(sparse_job("web", 200))
        .collect();
    let ends = base.window_ends();
    assert!(
        windows.iter().all(|s| ships_raw(&ends, s)),
        "every series is raw under the per-series rule"
    );

    for (what, request) in [
        (
            "sum by (job)",
            fused_modifier(base.clone(), AggregationKind::Sum, Some(by(&["job"]))),
        ),
        (
            "sum",
            fused_modifier(base.clone(), AggregationKind::Sum, None),
        ),
        (
            "sum without (instance)",
            fused_modifier(
                base.clone(),
                AggregationKind::Sum,
                Some(without(&["instance"])),
            ),
        ),
    ] {
        let plan = transport_plan(&request, &ends, &windows);
        assert!(
            plan.iter().all(|&raw| !raw),
            "{what}: every series is promoted"
        );

        let resp = response(&request, windows.clone());
        assert!(resp.raw.is_empty(), "{what}: nothing travels raw");
        assert!(resp.series.is_empty(), "{what}");
        assert!(
            resp.partials.len() <= 2 * ends.len(),
            "{what}: at most one partial per (group, step), got {}",
            resp.partials.len()
        );

        let mut cmd = command(request.clone());
        cmd.on_response(resp, &node(7000)).unwrap();
        assert_eq!(
            rendered(cmd.into_result().unwrap()),
            rendered(request.evaluate(windows.clone()).unwrap()),
            "{what}"
        );
    }
}

/// A group whose raw spans are smaller than its partials keeps shipping
/// raw: one series with one sample is a span of one sample, against a
/// partial per window that sample reaches.
#[test]
fn test_reduction_keeps_a_lone_sparse_series_raw() {
    let lone = series("only", &[(150_000, 1.0)]);
    for (what, base) in [
        ("stepped", stepped_grid_request()),
        ("rate", rollup_grid_request(RollupKind::Rate)),
    ] {
        let request = fused(base, AggregationKind::Sum, &["__name__"]);
        let ends = request.window_ends();
        assert_eq!(
            transport_plan(&request, &ends, std::slice::from_ref(&lone)),
            vec![true],
            "{what}"
        );

        let resp = response(&request, vec![lone.clone()]);
        assert_eq!(resp.raw.len(), 1, "{what}: the lone series travels raw");
        assert!(resp.partials.is_empty(), "{what}");

        let mut cmd = command(request.clone());
        cmd.on_response(resp, &node(7000)).unwrap();
        assert_eq!(
            rendered(cmd.into_result().unwrap()),
            rendered(request.evaluate(vec![lone.clone()]).unwrap()),
            "{what}"
        );
    }
}

/// Groups decide independently, in one response: a group with a staged
/// member folds its sparse mates in, a crowded sparse group is promoted
/// on size, and a lone sparse series in a group of its own stays raw.
/// The coordinator lands on the single-node answer over the mixture.
#[test]
fn test_groups_decide_independently() {
    let request = fused_modifier(
        stepped_grid_request(),
        AggregationKind::Sum,
        Some(by(&["job"])),
    );
    let ends = request.window_ends();

    let dense = series_with(
        &[("__name__", "m"), ("job", "staged"), ("instance", "0")],
        &(0..=30).map(|i| (i * 10_000, i as f64)).collect::<Vec<_>>(),
    );
    let mate = series_with(
        &[("__name__", "m"), ("job", "staged"), ("instance", "1")],
        &[(20_000, 5.0)],
    );
    let lone = series_with(
        &[("__name__", "m"), ("job", "lone"), ("instance", "2")],
        &[(150_000, 1.0)],
    );
    let crowd = sparse_job("crowd", 100);

    let mut windows = vec![dense.clone(), lone.clone(), mate.clone()];
    windows.extend(crowd.iter().cloned());

    let plan = transport_plan(&request, &ends, &windows);
    assert_eq!(&plan[..3], &[false, true, false], "dense, lone, mate");
    assert!(plan[3..].iter().all(|&raw| !raw), "the crowd is promoted");

    let resp = response(&request, windows.clone());
    assert_eq!(resp.raw.len(), 1, "only the lone series travels raw");
    assert!(resp.series.is_empty());
    let table = resp.labels.clone().unwrap_or_default();
    let mut groups: Vec<String> = resp
        .partials
        .iter()
        .map(|p| {
            p.label_name_refs
                .iter()
                .zip(&p.label_value_refs)
                .map(|(&n, &v)| format!("{}={}", table.names[n as usize], table.values[v as usize]))
                .collect::<Vec<_>>()
                .join(",")
        })
        .collect();
    groups.sort();
    groups.dedup();
    assert_eq!(groups, vec!["job=crowd", "job=staged"]);

    let mut cmd = command(request.clone());
    cmd.on_response(resp, &node(7000)).unwrap();
    assert_eq!(
        rendered(cmd.into_result().unwrap()),
        rendered(request.evaluate(windows).unwrap()),
    );
}

/// Only a reduction answers in partials. Unfused, or fused with a
/// selecting or counting operator, the response is per series and the
/// per-series rule stands, however many sparse series share a group.
#[test]
fn test_only_reductions_decide_per_group() {
    let base = stepped_grid_request();
    let windows = sparse_job("api", 50);
    let ends = base.window_ends();
    let per_series: Vec<bool> = windows.iter().map(|s| ships_raw(&ends, s)).collect();
    assert!(per_series.iter().all(|&raw| raw));

    for (what, request) in [
        ("unfused", base.clone()),
        (
            "topk",
            fused_with(
                base.clone(),
                AggregationKind::Topk,
                &["job"],
                Some(AggregationParam::Scalar(2.0)),
            ),
        ),
        (
            "count_values",
            fused_with(
                base.clone(),
                AggregationKind::CountValues,
                &["job"],
                Some(AggregationParam::Label("v".into())),
            ),
        ),
    ] {
        assert_eq!(
            transport_plan(&request, &ends, &windows),
            per_series,
            "{what}"
        );
    }

    // The same series under a reduction: promoted.
    let reduced = fused(base, AggregationKind::Sum, &["job"]);
    assert!(
        transport_plan(&reduced, &ends, &windows)
            .iter()
            .all(|&raw| !raw)
    );
}

/// Every reduction, every modifier shape, both stages: sparse groups
/// split across shards fold on the shards and the coordinator's merge
/// lands on the single-node answer.
#[test]
fn test_every_reduction_folds_sparse_groups_like_single_node() {
    const REDUCTIONS: [AggregationKind; 8] = [
        AggregationKind::Sum,
        AggregationKind::Avg,
        AggregationKind::Min,
        AggregationKind::Max,
        AggregationKind::Count,
        AggregationKind::Group,
        AggregationKind::Stddev,
        AggregationKind::Stdvar,
    ];
    // Three shards; both jobs straddle all of them.
    let shards: Vec<Vec<RangeSample<EvalLabels>>> = (0..3)
        .map(|shard| {
            sparse_job("api", 60)
                .into_iter()
                .chain(sparse_job("web", 60))
                .enumerate()
                .filter(|(i, _)| i % 3 == shard)
                .map(|(_, s)| s)
                .collect()
        })
        .collect();
    let all: Vec<RangeSample<EvalLabels>> = shards.iter().flatten().cloned().collect();

    for base in [
        stepped_grid_request(),
        rollup_grid_request(RollupKind::SumOverTime),
    ] {
        for kind in REDUCTIONS {
            for modifier in [
                None,
                Some(by(&["job"])),
                Some(by(&["__name__", "job"])),
                Some(by(&["missing"])),
                Some(without(&["instance"])),
            ] {
                let what = format!(
                    "{kind:?} modifier={modifier:?} rollup={:?}",
                    base.rollup.as_ref().map(|r| r.kind)
                );
                let request = fused_modifier(base.clone(), kind, modifier);
                let want = rendered(request.evaluate(all.clone()).unwrap());

                let mut cmd = command(request.clone());
                for (index, shard) in shards.iter().enumerate() {
                    let resp = response(&request, shard.clone());
                    assert!(
                        resp.raw.is_empty(),
                        "{what}: shard {index} folded its groups"
                    );
                    cmd.on_response(resp, &node(7000 + index as u16)).unwrap();
                }
                assert_rendered_close(&want, &rendered(cmd.into_result().unwrap()), &what);
            }
        }
    }
}

/// The promotion rule at its boundaries: a tie goes to the partials, a
/// staged member promotes unconditionally, the partials are bounded by
/// the grid, and synthetic counts do not overflow.
#[test]
fn test_group_estimate_boundaries() {
    let group = |raw_bytes, raw_samples, staged| GroupEstimate {
        partial_bytes: 50,
        raw_bytes,
        raw_samples,
        staged,
    };

    // Ten samples reaching two windows each: 20 partials at 50 bytes.
    assert!(group(1000, 10, false).promotes(100, 2), "a tie folds");
    assert!(group(1001, 10, false).promotes(100, 2));
    assert!(!group(999, 10, false).promotes(100, 2));

    // The grid bounds the partials: 10 000 samples reaching 20 windows
    // each is still at most 100 partials.
    assert!(group(5_001, 10_000, false).promotes(100, 20));
    assert!(!group(4_999, 10_000, false).promotes(100, 20));

    // A staged member: promoted whatever the sizes.
    assert!(group(1, 1, true).promotes(100, 20));

    // Saturating, not wrapping.
    assert!(!group(u64::MAX - 1, u64::MAX, false).promotes(u64::MAX, u64::MAX));
    assert!(group(u64::MAX, u64::MAX, false).promotes(u64::MAX, u64::MAX));
    assert!(
        group(0, 0, false).promotes(0, 1),
        "an empty grid has nothing to ship"
    );
}

/// A sample reaches every window ending within `backward` of it, which a
/// grid at `step` has `ceil(backward / step)` of.
#[test]
fn test_windows_per_sample() {
    assert_eq!(
        windows_per_sample(&stepped_request()),
        1,
        "single evaluation"
    );
    // Lookback 300s at a 30s step.
    assert_eq!(windows_per_sample(&stepped_grid_request()), 10);
    // Range 60s at a 30s step.
    assert_eq!(
        windows_per_sample(&rollup_grid_request(RollupKind::Rate)),
        2
    );
    let coarse = GridRequest {
        step_ms: 3_600_000,
        ..rollup_grid_request(RollupKind::Rate)
    };
    assert_eq!(
        windows_per_sample(&coarse),
        1,
        "a step wider than the range"
    );
    let odd = GridRequest {
        rollup: Some(GridRollup {
            kind: RollupKind::Rate,
            range_ms: 45_000,
            param: None,
        }),
        ..rollup_grid_request(RollupKind::Rate)
    };
    assert_eq!(windows_per_sample(&odd), 2, "rounds up");
    let none = GridRequest {
        lookback_delta_ms: 0,
        ..stepped_grid_request()
    };
    assert_eq!(windows_per_sample(&none), 1, "never zero");
}

/// The wire estimates track the proto layout: a raw span is its labels
/// once plus its samples, a partial is the group's labels every time
/// plus the accumulators its operator sets.
#[test]
fn test_wire_estimates() {
    let one = series("0", &[(0, 1.0)]);
    let labels = labels_wire_bytes(one.labels.iter());
    // `__name__="m"` and `instance="0"`.
    assert_eq!(labels, 2 * wire::LABEL_OVERHEAD + 8 + 1 + 8 + 1);
    assert_eq!(
        raw_wire_bytes(&one),
        wire::RAW_SERIES_OVERHEAD + labels + wire::RAW_SAMPLE_UNCOMPRESSED
    );

    let many = series(
        "0",
        &(0..WIRE_COMPRESSION_MIN_SAMPLES as i64)
            .map(|i| (i, 1.0))
            .collect::<Vec<_>>(),
    );
    assert_eq!(
        raw_wire_bytes(&many),
        wire::RAW_SERIES_OVERHEAD
            + labels
            + WIRE_COMPRESSION_MIN_SAMPLES as u64 * wire::RAW_SAMPLE_COMPRESSED,
        "a span at the codec threshold is estimated compressed"
    );

    // Grouping labels: what the modifier keeps, and nothing for `sum(...)`.
    assert_eq!(labels_wire_bytes(one.labels.grouping_labels(None)), 0);
    let by_name = by(&["__name__"]);
    assert_eq!(
        labels_wire_bytes(one.labels.grouping_labels(Some(&by_name))),
        wire::LABEL_OVERHEAD + 8 + 1
    );
    let without_name = without(&["__name__"]);
    assert_eq!(
        labels_wire_bytes(one.labels.grouping_labels(Some(&without_name))),
        wire::LABEL_OVERHEAD + 8 + 1
    );

    assert_eq!(partial_accumulators(AggregationKind::Count), 0);
    assert_eq!(partial_accumulators(AggregationKind::Max), 1);
    assert_eq!(partial_accumulators(AggregationKind::Sum), 2);
    assert_eq!(partial_accumulators(AggregationKind::Stddev), 3);
}

/// Over a grid, a series contributes only the steps whose window held
/// samples — the sparse shape the transport has to carry. A shard that
/// staged and one that shipped raw must produce the same set of steps.
#[test]
fn test_grid_transport_is_sparse() {
    let request = rollup_grid_request(RollupKind::CountOverTime);
    // A gap between t=30s and t=270s: the windows in between are empty.
    // Twelve samples so the series stages rather than ships raw.
    let mut points: Vec<(i64, f64)> = (0..=30_000).step_by(5_000).map(|t| (t, 1.0)).collect();
    points.extend((270_000..=300_000).step_by(6_000).map(|t| (t, 2.0)));
    let gappy = vec![series("0", &points)];

    let staged = response(&request, gappy.clone());
    assert_eq!(staged.series.len(), 1);
    let steps: Vec<i64> = decode_columns(&request.window_ends(), &staged.series[0])
        .unwrap()
        .map(|(step_ts, _, _)| step_ts)
        .collect();
    // Windows are `(end - 60s, end]`, so the early samples are reported at
    // steps 0s..=60s (the 90s window starts just past t=30s) and the late
    // ones at 270s and 300s. Nothing between.
    assert_eq!(steps, vec![0, 30_000, 60_000, 270_000, 300_000]);

    let mut cmd = command(request.clone());
    cmd.on_response(raw_response(gappy), &node(7000)).unwrap();
    let GridOutcome::Rolled(fallback) = cmd.into_result().unwrap() else {
        panic!("a rollup request answers Rolled");
    };
    assert_eq!(fallback.len(), 1);
    assert_eq!(
        fallback[0]
            .samples
            .iter()
            .map(|p| p.timestamp)
            .collect::<Vec<_>>(),
        steps,
    );
}

/// A series whose window is empty produces no output at all, and a NaN
/// rolled-up value is a result and must be reported. Confusing the two is
/// the failure this protocol has to avoid.
#[test]
fn test_absence_is_not_nan() {
    let request = rollup_request(RollupKind::CountOverTime);
    let outside = vec![series("3", &[(100_000, 4.0)])];
    let mut cmd = command(request.clone());
    cmd.on_response(raw_response(outside), &node(7000)).unwrap();
    assert!(
        rendered(cmd.into_result().unwrap()).is_empty(),
        "a series with no samples in the window must not be reported"
    );

    let request = rollup_request(RollupKind::SumOverTime);
    let nan_series = vec![series("0", &[(250_000, f64::NAN)])];
    let mut cmd = command(request.clone());
    cmd.on_response(raw_response(nan_series), &node(7000))
        .unwrap();
    let GridOutcome::Rolled(rolled) = cmd.into_result().unwrap() else {
        panic!("a rollup request answers Rolled");
    };
    assert_eq!(rolled.len(), 1, "the NaN result is a result");
    assert!(rolled[0].samples[0].value.is_nan());
}

/// The request carries the resolved grid, rollup and aggregation, and
/// survives a proto round trip into exactly what the coordinator asked.
/// The columnar form addresses a point by its window index: a bitmap of
/// the windows that produced a value, and the values in that order. What
/// comes out of the decoder is what went into the encoder, over gaps and
/// across byte boundaries of the bitmap.
#[test]
fn test_columns_round_trip() {
    let window_ends: Vec<i64> = (0..=20).map(|i| i * 30_000).collect();
    // Present at windows 0, 7, 8 (a byte boundary), 15 and 20 only.
    let points: Vec<(i64, i64, f64)> = [0usize, 7, 8, 15, 20]
        .iter()
        .map(|&i| {
            (
                window_ends[i],
                window_ends[i] - 1_234 * i as i64,
                i as f64 * 0.5,
            )
        })
        .collect();

    let (presence, values, sample_lag) = encode_columns(&window_ends, points.iter().copied(), true);
    assert_eq!(presence, vec![0b1000_0001, 0b1000_0001, 0b0001_0000]);
    assert_eq!(values.len(), 5);
    assert_eq!(
        sample_lag,
        vec![0, 1_234 * 7, 1_234 * 8, 1_234 * 15, 1_234 * 20]
    );
    let series = ProtoGridSeries {
        presence,
        values,
        sample_lag,
        ..Default::default()
    };
    let decoded: Vec<(i64, i64, f64)> = decode_columns(&window_ends, &series).unwrap().collect();
    assert_eq!(decoded, points);

    // Without the lag column a pick is stamped with its window end.
    let (presence, values, sample_lag) =
        encode_columns(&window_ends, points.iter().copied(), false);
    assert!(sample_lag.is_empty());
    let series = ProtoGridSeries {
        presence,
        values,
        sample_lag,
        ..Default::default()
    };
    let decoded: Vec<(i64, i64, f64)> = decode_columns(&window_ends, &series).unwrap().collect();
    let stamped: Vec<(i64, i64, f64)> = points.iter().map(|&(s, _, v)| (s, s, v)).collect();
    assert_eq!(decoded, stamped);

    // Nothing present: an empty bitmap, no values.
    let (presence, values, _) = encode_columns(&window_ends, std::iter::empty(), true);
    assert_eq!(presence, vec![0, 0, 0]);
    assert!(values.is_empty());
}

/// A stepped request that does not ask for sample timestamps ships no
/// lag column and stamps each pick with its step, which is what every
/// consumer but `timestamp()` sees anyway.
#[test]
fn test_sample_timestamps_travel_only_on_request() {
    let request = GridRequest {
        sample_timestamps: false,
        ..stepped_grid_request()
    };
    let dense = || {
        series(
            "0",
            &[
                (5_000, 1.0),
                (65_000, 2.0),
                (125_000, 3.0),
                (185_000, 4.0),
                (245_000, 5.0),
                (305_000, 6.0),
                (365_000, 7.0),
                (425_000, 8.0),
                (485_000, 9.0),
                (545_000, 10.0),
                (595_000, 11.0),
                (599_000, 12.0),
            ],
        )
    };
    let resp = response(&request, vec![dense()]);
    assert_eq!(resp.series.len(), 1);
    assert!(resp.series[0].sample_lag.is_empty());

    let mut cmd = command(request.clone());
    cmd.on_response(resp, &node(7000)).unwrap();
    let GridOutcome::Stepped(stepped) = cmd.into_result().unwrap() else {
        panic!("a stepped request answers Stepped");
    };
    assert!(
        stepped[0]
            .points
            .iter()
            .all(|p| p.sample.timestamp == p.step_ts)
    );

    // The values are the ones the single-node stage picks; only the
    // timestamps differ.
    let GridSeries::Stepped(local) = request.per_series(&request.window_ends(), vec![dense()])
    else {
        panic!()
    };
    let values = |s: &[SteppedSeries]| -> Vec<(i64, f64)> {
        s[0].points
            .iter()
            .map(|p| (p.step_ts, p.sample.value))
            .collect()
    };
    assert_eq!(values(&stepped), values(&local));

    // Asked for, the lags travel and the picks' own timestamps come back.
    let asking = GridRequest {
        sample_timestamps: true,
        ..request
    };
    let mut cmd = command(asking.clone());
    cmd.on_response(response(&asking, vec![dense()]), &node(7000))
        .unwrap();
    let GridOutcome::Stepped(stepped) = cmd.into_result().unwrap() else {
        panic!()
    };
    assert!(
        stepped[0]
            .points
            .iter()
            .any(|p| p.sample.timestamp != p.step_ts)
    );
}

/// Columns that disagree with each other or with the grid are a corrupt
/// response, not a short one.
#[test]
fn test_malformed_columns_are_rejected() {
    let request = stepped_grid_request();
    let windows = request.window_ends().len();
    let malformed = [
        // A bit past the last window.
        ProtoGridSeries {
            presence: {
                let mut p = vec![0u8; windows.div_ceil(8)];
                p[windows / 8] |= 1 << (windows % 8);
                p
            },
            values: vec![1.0],
            ..Default::default()
        },
        // Two windows present, one value.
        ProtoGridSeries {
            presence: vec![0b11],
            values: vec![1.0],
            ..Default::default()
        },
        // A lag column shorter than the values.
        ProtoGridSeries {
            presence: vec![0b11],
            values: vec![1.0, 2.0],
            sample_lag: vec![0],
            ..Default::default()
        },
    ];
    for series in malformed {
        let mut cmd = command(request.clone());
        let err = cmd
            .on_response(
                GridQueryResponse {
                    series: vec![series],
                    ..Default::default()
                },
                &node(7000),
            )
            .unwrap_err();
        assert!(err.to_string().contains("malformed grid series"), "{err}");
    }
}

#[test]
fn test_request_round_trip() {
    use crate::fanout::serialization::{Deserialized, Serialized};

    let request = GridRequest {
        // `@`/`offset` already resolved: the shard is told the window end.
        range_end_ms: EVAL_TS - 3_600_000,
        query_start: EVAL_TS - 3_600_000,
        query_end: EVAL_TS - 3_600_000,
        ..fused(
            rollup_request(RollupKind::QuantileOverTime),
            AggregationKind::Sum,
            &["job", "env"],
        )
    };
    let cmd = GridFanoutCommand::new(
        Matchers::empty(),
        request,
        1_000,
        11_000,
        Duration::from_secs(1),
    );

    let wire = cmd.generate_request();
    let mut buf = Vec::new();
    wire.serialize(&mut buf);
    let decoded = GridQuery::deserialize(&buf).unwrap();
    assert_eq!(decoded, wire);
    assert_eq!(decoded.max_series, 1_000);
    assert_eq!(decoded.max_points_per_series, 11_000);
    assert!(decoded.selector.is_some());

    let round_tripped = decode_request(&decoded).expect("well-formed request");
    assert_eq!(round_tripped.window_ends(), vec![EVAL_TS - 3_600_000]);
    assert_eq!(round_tripped.lookback_delta_ms, LOOKBACK_MS);
    let rollup = round_tripped.rollup.expect("a rollup");
    assert_eq!(rollup.kind, RollupKind::QuantileOverTime);
    assert_eq!(rollup.range_ms, RANGE_MS);
    assert_eq!(rollup.param, Some(0.9));
    let aggregation = round_tripped.aggregation.expect("fused");
    assert_eq!(aggregation.kind, AggregationKind::Sum);
    assert_eq!(
        aggregation.modifier,
        Some(LabelModifier::Include(promql_parser::label::Labels::new(
            vec!["job", "env"]
        )))
    );

    // A stepped request carries neither part.
    let wire = command(stepped_grid_request()).generate_request();
    assert!(wire.rollup.is_none() && wire.aggregation.is_none());
    let stepped = decode_request(&wire).unwrap();
    assert!(stepped.rollup.is_none() && stepped.aggregation.is_none());
    assert_eq!(stepped.window_ends().len(), 11);
}

/// A rollup this node does not know, or an aggregation it cannot fuse, is a
/// corrupt request — every node runs the same build — and is refused, never
/// mistaken for the zero variant or quietly degraded.
#[test]
fn test_decode_rejects_malformed_requests() {
    let mut req = command(rollup_request(RollupKind::SumOverTime)).generate_request();
    assert!(decode_request(&req).is_ok());

    for kind in [0, 99, -1] {
        req.rollup.as_mut().unwrap().kind = kind;
        assert!(decode_request(&req).is_err(), "rollup kind {kind}");
    }

    let mut req = command(fused(
        stepped_grid_request(),
        AggregationKind::Sum,
        &["job"],
    ))
    .generate_request();
    assert!(decode_request(&req).is_ok());
    req.aggregation.as_mut().unwrap().kind = ProtoAggregationKind::Unspecified as i32;
    assert!(
        decode_request(&req).is_err(),
        "an unspecified operator cannot be fused"
    );
    req.aggregation.as_mut().unwrap().kind = 99;
    assert!(decode_request(&req).is_err());

    // A grid wider than the ceiling: the shard would walk and collect every
    // window end before any series or point limit applies.
    let mut req = command(stepped_grid_request()).generate_request();
    req.step_ms = 1;
    req.query_start = 0;
    req.query_end = i64::MAX;
    let err = decode_request(&req).expect_err("unbounded grid");
    assert!(err.to_string().contains("steps"), "{err}");
    req.query_end = crate::promql::MAX_GRID_STEPS as i64 - 1;
    assert!(
        decode_request(&req).is_ok(),
        "a grid at the ceiling decodes"
    );

    // A selecting operator decodes with its parameter.
    let req = command(fused_with(
        stepped_grid_request(),
        AggregationKind::Topk,
        &["job"],
        Some(AggregationParam::Scalar(3.0)),
    ))
    .generate_request();
    let decoded = decode_request(&req).unwrap();
    assert!(matches!(
        decoded.aggregation.as_ref().and_then(|a| a.param.as_ref()),
        Some(AggregationParam::Scalar(k)) if *k == 3.0
    ));
    let req = command(fused_with(
        stepped_grid_request(),
        AggregationKind::CountValues,
        &[],
        Some(AggregationParam::Label("v".into())),
    ))
    .generate_request();
    let decoded = decode_request(&req).unwrap();
    assert!(matches!(
        decoded.aggregation.as_ref().and_then(|a| a.param.as_ref()),
        Some(AggregationParam::Label(l)) if l == "v"
    ));
}

/// A response whose lists contradict the request is rejected instead of
/// being counted twice: partials for an unfused request, or per-series
/// values for a fused one.
#[test]
fn test_mismatched_payload_is_rejected() {
    let mut cmd = command(stepped_grid_request());
    let stray = GridQueryResponse {
        series: Vec::new(),
        partials: vec![GridGroupPartial::default()],
        ..Default::default()
    };
    assert!(cmd.on_response(stray, &node(7000)).is_err());

    let mut cmd = command(fused(
        rollup_grid_request(RollupKind::SumOverTime),
        AggregationKind::Sum,
        &["__name__"],
    ));
    let stray = GridQueryResponse {
        series: vec![ProtoGridSeries::default()],
        partials: vec![GridGroupPartial::default()],
        ..Default::default()
    };
    assert!(cmd.on_response(stray, &node(7000)).is_err());

    // Raw alongside either list is legitimate: that is the size rule.
    let mut cmd = command(stepped_grid_request());
    let mut symbols = SymbolTableBuilder::default();
    let raw = range_sample_to_proto(series("0", &[(EVAL_TS, 1.0)]), &mut symbols).unwrap();
    let mixed = GridQueryResponse {
        series: vec![ProtoGridSeries::default()],
        raw: vec![raw],
        labels: Some(symbols.finish()),
        ..Default::default()
    };
    assert!(cmd.on_response(mixed, &node(7000)).is_ok());

    // A raw series whose chunk does not decode is a corrupt response.
    let mut cmd = command(stepped_grid_request());
    let garbage = GridQueryResponse {
        raw: vec![crate::promql::generated::RangeSample {
            data: Some(crate::promql::generated::SampleData {
                version: 1,
                compression: 2,
                data: vec![0xFF, 0x00, 0x13],
            }),
            ..Default::default()
        }],
        ..Default::default()
    };
    let err = cmd.on_response(garbage, &node(7000)).unwrap_err();
    assert!(err.to_string().contains("undecodable raw series"), "{err}");
}

/// The geometry a request describes, which both sides derive through the
/// one [`grid_window_ends`] — so what is pinned here is the shape itself
/// rather than the two sides agreeing about it.
#[test]
fn test_window_ends_and_fetch_bounds() {
    // Instant: one window, at the resolved end; reaching back one full
    // range for a rollup, one lookback for a stepped selection, exclusive
    // of the lower bound.
    let instant = rollup_request(RollupKind::SumOverTime);
    assert_eq!(instant.window_ends(), vec![EVAL_TS]);
    assert_eq!(
        instant.fetch_bounds(),
        Some((EVAL_TS - RANGE_MS + 1, EVAL_TS))
    );
    let stepped = stepped_request();
    assert_eq!(
        stepped.fetch_bounds(),
        Some((EVAL_TS - LOOKBACK_MS + 1, EVAL_TS))
    );

    // Grid: every step, and one span covering every window rather than one
    // fetch per step.
    let grid = GridRequest {
        step_ms: 30_000,
        query_start: 60_000,
        query_end: 180_000,
        ..rollup_request(RollupKind::SumOverTime)
    };
    assert_eq!(
        grid.window_ends(),
        vec![60_000, 90_000, 120_000, 150_000, 180_000]
    );
    assert_eq!(grid.fetch_bounds(), Some((60_000 - RANGE_MS + 1, 180_000)));

    // A grid with no steps in it describes nothing to read.
    let empty = GridRequest {
        step_ms: 30_000,
        query_start: 180_000,
        query_end: 60_000,
        ..stepped_request()
    };
    assert!(empty.window_ends().is_empty());
    assert_eq!(empty.fetch_bounds(), None);
}

/// One shard's read as the shard sees it, over every transport case at
/// once: a group with dense members (staged) and sparse ones (promoted
/// because the group ships partials anyway), a sparse-only group (promoted
/// on size), and a lone sparse series (kept raw).
fn mixed_transport_windows() -> Vec<RangeSample<EvalLabels>> {
    let dense = |job: &str, i: usize| {
        let instance = format!("{job}-dense-{i}");
        series_with(
            &[("__name__", "m"), ("job", job), ("instance", &instance)],
            &(0..=30)
                .map(|t| (t * 10_000, (i * 100 + t as usize) as f64))
                .collect::<Vec<_>>(),
        )
    };
    let mut windows = Vec::new();
    for i in 0..5 {
        windows.push(dense("api", i));
        windows.extend(sparse_job("api", 40).into_iter().skip(i * 8).take(8));
    }
    windows.extend(sparse_job("web", 200));
    windows.push(series_with(
        &[("__name__", "m"), ("job", "solo"), ("instance", "solo-0")],
        &[(150_000, 7.0)],
    ));
    windows
}

/// A shard builds its response a batch at a time ([`ShardGrid`]); the
/// answer must be the one the whole read gives in one batch, however the
/// batches cut across groups — the transport decision is deferred until
/// every batch is in, so a group split over batches is sized as a whole.
#[test]
fn test_shard_grid_in_batches_answers_as_one_batch() {
    let windows = mixed_transport_windows();
    let requests = [
        ("stepped", stepped_grid_request(), false),
        ("rate", rollup_grid_request(RollupKind::Rate), false),
        (
            "sum by (job) stepped",
            fused(stepped_grid_request(), AggregationKind::Sum, &["job"]),
            true,
        ),
        (
            "sum by (job) rate",
            fused(
                rollup_grid_request(RollupKind::Rate),
                AggregationKind::Sum,
                &["job"],
            ),
            true,
        ),
        (
            "max by (job) rate",
            fused(
                rollup_grid_request(RollupKind::Rate),
                AggregationKind::Max,
                &["job"],
            ),
            true,
        ),
    ];
    for (what, request, is_fused) in requests {
        let ends = request.window_ends();
        let whole = shard_response(&request, &ends, windows.clone()).unwrap();

        let mut grid = ShardGrid::new(&request, &ends);
        for batch in windows.chunks(7) {
            grid.add(batch.to_vec());
        }
        let split = grid.finish().unwrap();

        assert_eq!(split.raw.len(), whole.raw.len(), "{what}: raw spans");
        assert_eq!(split.series.len(), whole.series.len(), "{what}: series");
        assert_eq!(
            split.partials.len(),
            whole.partials.len(),
            "{what}: partials"
        );
        if is_fused {
            // The sparse groups are folded; the lone series may go either way
            // (one accumulator for `max` makes even it cheaper as partials).
            assert!(split.raw.len() <= 1, "{what}: the sparse groups are folded");
        }

        let want = rendered(request.evaluate(windows.clone()).unwrap());
        let mut cmd = command(request.clone());
        cmd.on_response(split, &node(7000)).unwrap();
        let got = rendered(cmd.into_result().unwrap());
        if is_fused {
            assert_rendered_close(&want, &got, what);
        } else {
            assert_eq!(got, want, "{what}");
        }
    }
}
