//! `exec`: bounded executors, the lanes that run blocking, GIL-taking work off the main thread.
//! Everything here is read from the executors' own counts.

use crate::commands::analysis_lane_stats;
use crate::fanout::request_lane_stats;
use metered::Registry;
use metered::entry::{counter_value, gauge_value};

pub(super) fn register(registry: &mut Registry<'static>) {
    registry
        .register(
            gauge_value("fanout_queued")
                .read(|_: &()| request_lane_stats().queued)
                .help("Jobs waiting for a worker on the ts-fanout-request lane (peer requests and local fanout shares)"),
        )
        .register(
            gauge_value("fanout_running")
                .read(|_: &()| request_lane_stats().running)
                .help("Jobs a worker is running on the ts-fanout-request lane"),
        )
        .register(
            counter_value("fanout_rejected")
                .read(|_: &()| request_lane_stats().rejected)
                .help("Jobs refused because the ts-fanout-request queue was full (answered as busy)"),
        )
        .register(
            gauge_value("analysis_queued")
                .read(|_: &()| analysis_lane_stats().queued)
                .help("Jobs waiting for a worker on the ts-analysis lane"),
        )
        .register(
            gauge_value("analysis_running")
                .read(|_: &()| analysis_lane_stats().running)
                .help("Jobs a worker is running on the ts-analysis lane"),
        )
        .register(
            counter_value("analysis_rejected")
                .read(|_: &()| analysis_lane_stats().rejected)
                .help("Jobs refused because the ts-analysis queue was full"),
        );
}
