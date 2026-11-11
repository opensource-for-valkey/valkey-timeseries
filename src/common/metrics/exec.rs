//! `exec`: bounded executors, the lanes that run blocking, GIL-taking work off the main thread.
//! Everything here is read from the executors' own counts.

use crate::commands::{analysis_lane_stats, mrange_lane_stats};
use crate::fanout::{reply_lane_stats, request_lane_stats};
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
            gauge_value("fanout_reply_queued")
                .read(|_: &()| reply_lane_stats().queued)
                .help("Fanout replies waiting for a worker on the ts-fanout-reply lane"),
        )
        .register(
            gauge_value("fanout_reply_running")
                .read(|_: &()| reply_lane_stats().running)
                .help("Fanout replies a worker is building on the ts-fanout-reply lane"),
        )
        .register(
            counter_value("fanout_reply_rejected")
                .read(|_: &()| reply_lane_stats().rejected)
                .help("Fanout replies the full ts-fanout-reply queue refused, built on the main thread instead"),
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
        )
        .register(
            gauge_value("mrange_queued")
                .read(|_: &()| mrange_lane_stats().queued)
                .help("Standalone TS.MRANGE reads waiting for a worker on the ts-mrange lane"),
        )
        .register(
            gauge_value("mrange_running")
                .read(|_: &()| mrange_lane_stats().running)
                .help("Standalone TS.MRANGE reads a worker is decoding and answering on the ts-mrange lane"),
        )
        .register(
            counter_value("mrange_rejected")
                .read(|_: &()| mrange_lane_stats().rejected)
                .help("Standalone TS.MRANGE reads the full ts-mrange queue refused, answered on the main thread instead"),
        );
}
