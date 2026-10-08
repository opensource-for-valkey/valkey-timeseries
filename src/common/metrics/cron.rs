//! `cron`: the module's cron handler, which schedules the periodic background tasks.

use super::duration_histogram;
use crate::series::background_tasks::{CRON_TICKS, cron_interval_ms};
use metered::entry::{counter, gauge_value, metric};
use metered::{BucketHistogram, Registry};
use std::sync::LazyLock;
use std::sync::atomic::AtomicU64;

/// Cron ticks skipped because the server was loading or shutting down.
pub static TICKS_SKIPPED: AtomicU64 = AtomicU64::new(0);

/// Main-thread time per cron tick spent dispatching background tasks.
pub static TICK_DURATION: LazyLock<BucketHistogram> = duration_histogram();

pub(super) fn register(registry: &mut Registry<'static>) {
    registry
        .register(
            counter("ticks")
                .source(&CRON_TICKS)
                .help("Cron ticks that ran the background-task scheduler"),
        )
        .register(
            counter("ticks_skipped")
                .source(&TICKS_SKIPPED)
                .help("Cron ticks skipped because the server was loading or shutting down"),
        )
        .register(
            gauge_value("interval_seconds")
                .read(|_: &()| cron_interval_ms() as f64 / 1000.0)
                .help("Time between cron ticks, derived from the server's hz")
                .unit("seconds"),
        )
        .register(
            metric("tick_duration_seconds")
                .source(LazyLock::force(&TICK_DURATION))
                .help("Main-thread time per cron tick spent dispatching background tasks")
                .unit("seconds"),
        );
}
