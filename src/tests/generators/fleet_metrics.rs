//! Sample values for the Kubernetes fleet built by [`super::labels`].
//!
//! [`FleetTopology`] decides *which* series a fleet has; [`FleetMetrics`] decides what they read
//! scrape by scrape, so a fixture behaves under PromQL the way a real fleet does rather than as
//! unrelated noise:
//!
//! * **Every target is scraped on its own slot** -- an offset into the interval derived from
//!   `(cluster, job, instance)`, plus a few ms of jitter -- so the series of one target share
//!   timestamps and different targets don't.
//! * **Counters behave like counters.** They open at what the process accumulated before the
//!   window (uptime × rate), only go up, and reset when the container restarts. A restart shows
//!   everywhere it would in a real cluster: `kube_pod_container_status_restarts_total` steps,
//!   cAdvisor and process counters reset, the app's `up` reads 0 and its other series miss that
//!   scrape. A few pods start the window Pending and only get container / app series once
//!   Running.
//! * **Families that must agree, do.** The modes of one CPU add up to the elapsed time;
//!   histogram buckets are cumulative and `le="+Inf"` equals `_count` equals
//!   `http_requests_total`; `MemAvailable` stays under `MemTotal`; `/` and `/var/lib/containerd`
//!   share a device and so a size; `go_gc_duration_seconds` quantiles are ordered; JVM GC counts
//!   advance when the simulated eden / old gen are collected; `lo` receives what it transmits;
//!   `process_cpu_seconds_total` tracks the container's cAdvisor CPU.
//! * **Load follows the sun.** Activity is a diurnal curve in the `region`'s local time times a
//!   slow random walk, and it drives CPU, traffic, latency, allocation rate and load averages.
//!   Now and then one service in one cluster has a bad few minutes: 5xx rates jump and every
//!   route slows down.
//!
//! Values are rounded the way the exporters report them (node_exporter CPU at 1/100 s, bytes as
//! integers, memory at kB or page granularity), because value entropy is what the chunk encoders
//! see.
//!
//! Nothing is cached. [`FleetMetrics::samples`] rebuilds the stream of whatever entity a series
//! belongs to (a host, a container, a histogram group) from that entity's seed, so series can be
//! generated in any order, in parallel or one at a time, and still agree. The price is that a
//! shared stream is rebuilt once per member series.

use crate::common::rounding::round_to_decimal_digits;
use crate::common::{Sample, Timestamp};
use crate::labels::Label;
use crate::tests::generators::create_rng;
use crate::tests::generators::labels::{
    FleetPreset, FleetTopology, LE_BUCKETS, SeriesSpec, TopologyError, fnv1a,
};
use rand::RngExt;
use rand::prelude::{IndexedRandom, StdRng};
use rand_distr::{Binomial, Distribution, LogNormal, Normal, Poisson};
use statrs::function::erf::erfc;
use std::collections::HashMap;
use std::f64::consts::{PI, SQRT_2};
use std::ops::Range;

const KIB: f64 = 1024.0;
const MIB: f64 = 1024.0 * KIB;
const GIB: f64 = 1024.0 * MIB;
const PAGE: f64 = 4096.0;
const DAY: f64 = 86_400.0;

/// Mixed into the topology seed so value streams never share a seed with the label generator.
const VALUE_SALT: u64 = 0x5CA9_E0F1_EE75;

const N_LE: usize = LE_BUCKETS.len();

/// How busy time splits over the non-idle modes. Sums to 1, so the modes of one CPU always add
/// up to the elapsed time.
const BUSY_MODES: &[(&str, f64)] = &[
    ("user", 0.62),
    ("system", 0.22),
    ("iowait", 0.06),
    ("softirq", 0.04),
    ("steal", 0.025),
    ("nice", 0.02),
    ("irq", 0.015),
];

/// JDK defaults for a 240 MiB reserved code cache, as Micrometer reports them.
const CODE_HEAP_MAX: [f64; 3] = [5_836_800.0, 122_908_672.0, 122_912_768.0];
const COMPRESSED_CLASS_SPACE_MAX: f64 = GIB;

/// When and how often a fleet is scraped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScrapeConfig {
    /// Start of the first scrape cycle, ms since the epoch. Wall-clock time matters: load follows
    /// the time of day in each series' `region`.
    pub start: Timestamp,
    /// Scrape interval in ms. Every target is scraped once per interval.
    pub interval_ms: i64,
    /// Scrapes per target; a series has at most this many samples.
    pub samples: usize,
    /// Each scrape lands up to this many ms either side of its target's slot. Clamped below half
    /// the interval so a target's timestamps stay strictly increasing.
    pub jitter_ms: i64,
}

impl Default for ScrapeConfig {
    /// An hour of 15 s scrapes from 2026-01-01T00:00:00Z.
    fn default() -> Self {
        Self {
            start: 1_767_225_600_000,
            interval_ms: 15_000,
            samples: 240,
            jitter_ms: 10,
        }
    }
}

/// A [`FleetTopology`]'s series together with realistic values for each of them.
///
/// ```ignore
/// let fleet = FleetMetrics::preset(FleetPreset::Small, ScrapeConfig::default());
/// for (series, samples) in fleet.iter() {
///     // TS.CREATE series.key LABELS ... / TS.MADD ...
/// }
/// ```
pub struct FleetMetrics {
    series: Vec<SeriesSpec>,
    scrape: ScrapeConfig,
    /// CPU count per host, keyed by the host's seed. The label generator decides it and only
    /// `node_cpu_seconds_total` reveals it.
    cores: HashMap<u64, usize>,
    seed: u64,
}

impl FleetMetrics {
    pub fn new(topology: &FleetTopology, scrape: ScrapeConfig) -> Result<Self, TopologyError> {
        let series = topology.try_generate()?;
        let mut this = Self {
            series: Vec::new(),
            scrape,
            cores: HashMap::new(),
            seed: topology.seed ^ VALUE_SALT,
        };
        for spec in &series {
            let s = View(&spec.labels);
            if s.name() == "node_cpu_seconds_total" {
                let cpu: usize = s.get("cpu").parse().expect("numeric cpu label");
                let host = this.seed_of(&[s.get("cluster"), s.get("node")]);
                let cores = this.cores.entry(host).or_default();
                *cores = (*cores).max(cpu + 1);
            }
        }
        this.series = series;
        Ok(this)
    }

    pub fn preset(preset: FleetPreset, scrape: ScrapeConfig) -> Self {
        Self::new(&FleetTopology::preset(preset), scrape).expect("presets validate")
    }

    pub fn series(&self) -> &[SeriesSpec] {
        &self.series
    }

    pub fn scrape(&self) -> &ScrapeConfig {
        &self.scrape
    }

    /// Every series with its samples, generated lazily in series order.
    pub fn iter(&self) -> impl Iterator<Item = (&SeriesSpec, Vec<Sample>)> + '_ {
        self.series.iter().map(move |s| (s, self.samples(s)))
    }

    /// The samples of one series of this fleet: at most [`ScrapeConfig::samples`], fewer where
    /// the target was down or the pod not yet running.
    ///
    /// Panics on a metric name this module has no model for, i.e. one the label generator never
    /// emits -- so a new family there fails the tests here instead of getting made-up values.
    pub fn samples(&self, series: &SeriesSpec) -> Vec<Sample> {
        let s = View(&series.labels);
        let ts = self.scrape_times(s);
        let values = self.values(s, &ts);
        debug_assert_eq!(values.len(), ts.len());
        ts.into_iter()
            .zip(values)
            .filter_map(|(t, v)| v.map(|v| Sample::new(t, v)))
            .collect()
    }

    fn values(&self, s: View, ts: &[Timestamp]) -> Vec<Option<f64>> {
        if !s.get("app_kubernetes_io_name").is_empty() {
            return self.application(s);
        }
        let always =
            |values: Vec<f64>| -> Vec<Option<f64>> { values.into_iter().map(Some).collect() };
        match s.get("job") {
            "node-exporter" => always(self.node_exporter(s, ts)),
            "kube-state-metrics" => always(self.kube_state_metrics(s)),
            "kubelet" => self.cadvisor(s),
            job => panic!("no value model for job {job:?} ({})", s.name()),
        }
    }

    // ---- seeds, clock, activity -------------------------------------------------------------

    fn seed_of(&self, parts: &[&str]) -> u64 {
        let mut bytes = Vec::with_capacity(96);
        for part in parts {
            bytes.extend_from_slice(part.as_bytes());
            bytes.push(0x1f);
        }
        self.seed ^ fnv1a(&bytes)
    }

    fn rng(&self, parts: &[&str]) -> StdRng {
        create_rng(Some(self.seed_of(parts)))
    }

    fn pod_rng(&self, s: View, stream: &str) -> StdRng {
        self.rng(&[s.get("cluster"), s.get("namespace"), s.get("pod"), stream])
    }

    fn container_rng(&self, s: View, stream: &str) -> StdRng {
        self.rng(&[
            s.get("cluster"),
            s.get("namespace"),
            s.get("pod"),
            s.get("container"),
            stream,
        ])
    }

    fn n(&self) -> usize {
        self.scrape.samples
    }

    fn dt(&self) -> f64 {
        self.scrape.interval_ms.max(1) as f64 / 1000.0
    }

    /// Nominal wall-clock second of tick `i`. Shared streams are indexed by tick rather than by
    /// any one target's timestamps, so the targets that read them agree.
    fn tick_secs(&self, i: usize) -> f64 {
        self.scrape.start as f64 / 1000.0 + i as f64 * self.dt()
    }

    fn scrape_times(&self, s: View) -> Vec<Timestamp> {
        let interval = self.scrape.interval_ms.max(1);
        let jitter = self.scrape.jitter_ms.clamp(0, (interval - 1) / 2);
        let mut rng = self.rng(&[s.get("cluster"), s.get("job"), s.get("instance"), "scrape"]);
        let slot = self.scrape.start + rng.random_range(0..interval);
        (0..self.n())
            .map(|i| {
                let jitter = if jitter > 0 {
                    rng.random_range(-jitter..=jitter)
                } else {
                    0
                };
                slot + i as i64 * interval + jitter
            })
            .collect()
    }

    /// A multiplicative random walk around 1: `exp` of an AR(1) process with correlation time
    /// `tau_s` and stationary standard deviation `sigma`.
    fn walk(&self, mut rng: StdRng, tau_s: f64, sigma: f64) -> Vec<f64> {
        let phi = (-self.dt() / tau_s).exp();
        let start = Normal::new(0.0, sigma).expect("valid normal");
        let step = Normal::new(0.0, sigma * (1.0 - phi * phi).sqrt()).expect("valid normal");
        let mut x = start.sample(&mut rng);
        (0..self.n())
            .map(|_| {
                let v = x.exp();
                x = phi * x + step.sample(&mut rng);
                v
            })
            .collect()
    }

    /// Load relative to the daily mean: the region's diurnal curve times a random walk.
    fn activity(&self, rng: StdRng, region: &str, tau_s: f64, sigma: f64) -> Vec<f64> {
        let offset = utc_offset_hours(region);
        self.walk(rng, tau_s, sigma)
            .into_iter()
            .enumerate()
            .map(|(i, w)| diurnal(self.tick_secs(i), offset) * w)
            .collect()
    }

    // ---- node_exporter ----------------------------------------------------------------------

    fn host<'a>(&self, s: View<'a>) -> Host<'a> {
        let (cluster, node) = (s.get("cluster"), s.get("node"));
        let mut rng = self.rng(&[cluster, node, "host"]);
        let cores = self
            .cores
            .get(&self.seed_of(&[cluster, node]))
            .copied()
            .unwrap_or(1);
        let per_core = [4.0, 8.0].choose(&mut rng).expect("non-empty") * GIB;
        // MemTotal is physical memory less what the kernel reserves at boot, in kB.
        let mem_total = round_to(cores as f64 * per_core * rng.random_range(0.97..0.985), KIB);
        Host {
            cluster,
            node,
            region: s.get("region"),
            cores,
            mem_total,
            uptime_s: log_uniform(&mut rng, DAY, 120.0 * DAY).round(),
            base_util: rng.random_range(0.15..0.6),
        }
    }

    fn host_activity(&self, h: &Host) -> Vec<f64> {
        self.activity(
            self.rng(&[h.cluster, h.node, "activity"]),
            h.region,
            900.0,
            0.25,
        )
    }

    fn host_util(&self, h: &Host) -> Vec<f64> {
        self.host_activity(h)
            .into_iter()
            .map(|a| (h.base_util * a).clamp(0.01, 0.97))
            .collect()
    }

    fn node_exporter(&self, s: View, ts: &[Timestamp]) -> Vec<f64> {
        let h = self.host(s);
        let n = self.n();
        match s.name() {
            "up" => vec![1.0; n],
            "node_cpu_seconds_total" => self.node_cpu(&h, s.get("cpu"), s.get("mode")),
            "node_load1" => self.node_load(&h, 60.0),
            "node_load5" => self.node_load(&h, 300.0),
            "node_load15" => self.node_load(&h, 900.0),
            "node_memory_MemTotal_bytes" => vec![h.mem_total; n],
            "node_memory_MemAvailable_bytes" => self.node_memory(&h).0,
            "node_memory_Cached_bytes" => self.node_memory(&h).1,
            "node_filesystem_size_bytes" => vec![self.node_filesystem(&h, s).0; n],
            "node_filesystem_avail_bytes" => self.node_filesystem(&h, s).1,
            "node_network_receive_bytes_total" | "node_network_transmit_bytes_total" => {
                self.node_network(&h, s)
            }
            "node_boot_time_seconds" => vec![(self.scrape.start / 1000) as f64 - h.uptime_s; n],
            "node_time_seconds" => {
                // The exporter reads the clock a millisecond or two after the scrape starts.
                let mut rng = self.rng(&[h.cluster, h.node, "time"]);
                ts.iter()
                    .map(|t| (*t as f64 + rng.random_range(0.5..3.0)) / 1000.0)
                    .collect()
            }
            name => panic!("no value model for {name}"),
        }
    }

    fn node_cpu(&self, h: &Host, cpu: &str, mode: &str) -> Vec<f64> {
        let share = |busy: f64| {
            if mode == "idle" {
                return 1.0 - busy;
            }
            let (_, share) = BUSY_MODES
                .iter()
                .find(|(m, _)| *m == mode)
                .unwrap_or_else(|| panic!("no value model for cpu mode {mode:?}"));
            busy * share
        };
        let own = self.walk(self.rng(&[h.cluster, h.node, cpu, "cpu"]), 60.0, 0.2);
        let dt = self.dt();
        let mut v = h.uptime_s * share(h.base_util);
        self.host_util(h)
            .into_iter()
            .zip(own)
            .map(|(u, w)| {
                v += dt * share((u * w).clamp(0.002, 0.998));
                round2(v)
            })
            .collect()
    }

    /// The kernel's load average: an exponentially damped run-queue length with time constant
    /// `tau_s`.
    fn node_load(&self, h: &Host, tau_s: f64) -> Vec<f64> {
        let util = self.host_util(h);
        let mut rng = self.rng(&[h.cluster, h.node, "loadavg"]);
        let noise = Normal::new(1.0_f64, 0.15).expect("valid normal");
        let decay = (-self.dt() / tau_s).exp();
        let cores = h.cores as f64;
        let mut load = cores * util.first().copied().unwrap_or(0.0);
        util.into_iter()
            .map(|u| {
                let runnable = cores * u * noise.sample(&mut rng).max(0.0);
                load = load * decay + runnable * (1.0 - decay);
                round2(load)
            })
            .collect()
    }

    /// `(MemAvailable, Cached)`, in kB like `/proc/meminfo`.
    fn node_memory(&self, h: &Host) -> (Vec<f64>, Vec<f64>) {
        let mut rng = self.rng(&[h.cluster, h.node, "memory"]);
        let (used0, cache0) = (rng.random_range(0.3..0.75), rng.random_range(0.1..0.3));
        let walk = self.walk(self.rng(&[h.cluster, h.node, "memory-walk"]), 1800.0, 0.04);
        self.host_activity(h)
            .into_iter()
            .zip(walk)
            .map(|(a, w)| {
                let used = (used0 * (0.9 + 0.1 * a) * w).clamp(0.05, 0.97);
                let avail = h.mem_total * (1.0 - used);
                let cached = (h.mem_total * cache0 / w).min(0.9 * avail);
                (round_to(avail, KIB), round_to(cached, KIB))
            })
            .unzip()
    }

    /// `(size, avail)` of the filesystem behind a mount. A block device mounted twice (`/` and
    /// `/var/lib/containerd`) is one filesystem, so both mounts read the same.
    fn node_filesystem(&self, h: &Host, s: View) -> (f64, Vec<f64>) {
        let (device, fstype, mountpoint) = (s.get("device"), s.get("fstype"), s.get("mountpoint"));
        let fs = if fstype == "tmpfs" {
            mountpoint
        } else {
            device
        };
        let mut rng = self.rng(&[h.cluster, h.node, fs, "fs"]);
        let gib = |choices: &[f64], rng: &mut StdRng| choices.choose(rng).expect("non-empty") * GIB;
        // (size, used at the window start, steady growth in bytes/s, image pulls, root-reserved)
        let (size, mut used, growth, pulls, reserved) = match fstype {
            "vfat" => (106_858_496.0, 6_402_048.0, 0.0, false, 0.0),
            "tmpfs" => {
                // systemd sizes /run at 20% of RAM; /dev/shm defaults to half.
                let frac = if mountpoint == "/run" { 0.2 } else { 0.5 };
                let size = round_to(h.mem_total * frac, PAGE);
                let used = round_to(size * rng.random_range(0.0..0.002), PAGE);
                (size, used, rng.random_range(0.0..64.0), false, 0.0)
            }
            "xfs" => {
                let size = gib(&[100.0, 200.0, 500.0], &mut rng);
                let used = size * rng.random_range(0.1..0.5);
                let growth = log_uniform(&mut rng, 20.0 * KIB, 300.0 * KIB);
                (size, used, growth, false, 0.0)
            }
            _ => {
                let size = gib(&[80.0, 100.0, 200.0], &mut rng);
                let used = size * rng.random_range(0.3..0.7);
                let growth = log_uniform(&mut rng, 50.0 * KIB, 500.0 * KIB);
                (size, used, growth, true, 0.05)
            }
        };
        let dt = self.dt();
        let avail = (0..self.n())
            .map(|_| {
                used += growth * dt * rng.random_range(0.0..2.0);
                if pulls && rng.random_bool(0.002) {
                    used += log_uniform(&mut rng, 50.0 * MIB, 800.0 * MIB);
                }
                // kubelet image GC: past the 85% high mark it frees down to the 80% low mark.
                if used > 0.85 * size {
                    used = 0.8 * size;
                }
                round_to((size * (1.0 - reserved) - used).max(0.0), PAGE)
            })
            .collect();
        (size, avail)
    }

    fn node_network(&self, h: &Host, s: View) -> Vec<f64> {
        let device = s.get("device");
        let base = log_uniform(&mut self.rng(&[h.cluster, h.node, "net"]), MIB, 30.0 * MIB);
        let share = match device {
            "eth0" => 1.0,
            "cni0" => 0.3,
            "flannel.1" => 0.15,
            "lo" => 0.05,
            "eth1" => 0.02,
            _ => 0.0,
        };
        // Loopback receives exactly what it transmits, so both directions share one stream.
        let stream = if device == "lo" { "lo" } else { s.name() };
        let mut rng = self.rng(&[h.cluster, h.node, device, stream]);
        let direction = if device == "lo" {
            1.0
        } else {
            rng.random_range(0.5..1.3)
        };
        let rate = base * share * direction;
        let noise = LogNormal::new(0.0, 0.15).expect("valid lognormal");
        let dt = self.dt();
        // `docker0` sits idle on a containerd node: a few kB from boot, then nothing.
        let mut v = (rate * h.uptime_s + rng.random_range(0.0..20_000.0)).round();
        self.host_activity(h)
            .into_iter()
            .map(|a| {
                v += (rate * a * dt * noise.sample(&mut rng)).round();
                v
            })
            .collect()
    }

    // ---- kube-state-metrics -----------------------------------------------------------------

    fn kube_state_metrics(&self, s: View) -> Vec<f64> {
        let n = self.n();
        match s.name() {
            "kube_pod_info" => vec![1.0; n],
            "kube_pod_status_phase" => {
                let (running_from, _) = self.pod_life(s);
                let phase = s.get("phase");
                (0..n)
                    .map(|i| {
                        let current = if i < running_from {
                            "Pending"
                        } else {
                            "Running"
                        };
                        if phase == current { 1.0 } else { 0.0 }
                    })
                    .collect()
            }
            "kube_pod_container_status_restarts_total" => {
                let life = self.container_life(s);
                (0..n).map(|i| life.restarts_by(i) as f64).collect()
            }
            name => panic!("no value model for {name}"),
        }
    }

    // ---- pods and containers ----------------------------------------------------------------

    /// `(running_from, pod age at running_from in seconds)`. A few pods start the window Pending
    /// (scheduling, image pull) and go Running partway through.
    fn pod_life(&self, s: View) -> (usize, f64) {
        let mut rng = self.pod_rng(s, "life");
        let age = log_uniform(&mut rng, 600.0, 30.0 * DAY);
        let n = self.n();
        if n >= 2 && rng.random_bool(0.015) {
            (rng.random_range(1..=n / 2), 0.0)
        } else {
            (0, age)
        }
    }

    fn container_life(&self, s: View) -> Life {
        let (running_from, pod_age_s) = self.pod_life(s);
        let mut rng = self.container_rng(s, "life");
        let n = self.n();
        let roll: f64 = rng.random();
        // (restarts before the window, restarts inside it): a few crash-looping containers, the
        // odd OOM kill, and a tail that restarted once or twice some time ago.
        let (prior_restarts, in_window) = if running_from > 0 {
            (0, 0)
        } else if roll < 0.03 {
            (rng.random_range(3..=40), rng.random_range(1..=4))
        } else if roll < 0.04 {
            (rng.random_range(0..=2), 1)
        } else if roll < 0.12 {
            (rng.random_range(1..=3), 0)
        } else {
            (0, 0)
        };
        let mut restarts: Vec<usize> = if running_from + 1 < n {
            (0..in_window)
                .map(|_| rng.random_range(running_from + 1..n))
                .collect()
        } else {
            Vec::new()
        };
        restarts.sort_unstable();
        restarts.dedup();
        let uptime_s = if prior_restarts > 0 {
            log_uniform(&mut rng, 60.0, pod_age_s)
        } else {
            pod_age_s
        };
        Life {
            running_from,
            restarts,
            prior_restarts,
            uptime_s,
            pod_age_s,
        }
    }

    /// A container's resource shape. Keyed by container name, so every replica of a service in
    /// every cluster is sized alike (and every `istio-proxy` like every other), then scaled by a
    /// per-pod factor for the uneven balancing real replicas see.
    fn profile(&self, s: View) -> Profile {
        let container = s.get("container");
        let mut rng = self.rng(&[container, "profile"]);
        let (cpu, mem, rx) = match container {
            "istio-proxy" => (
                (0.01, 0.08),
                (40.0 * MIB, 120.0 * MIB),
                (5.0 * KIB, 200.0 * KIB),
            ),
            "log-shipper" => ((0.003, 0.02), (15.0 * MIB, 40.0 * MIB), (100.0, KIB)),
            _ => ((0.005, 1.5), (32.0 * MIB, 2.0 * GIB), (KIB, 5.0 * MIB)),
        };
        let replica = self.pod_rng(s, "replica").random_range(0.8..1.2);
        Profile {
            cpu_cores: log_uniform(&mut rng, cpu.0, cpu.1) * replica,
            mem_bytes: log_uniform(&mut rng, mem.0, mem.1),
            rx_bytes_per_s: log_uniform(&mut rng, rx.0, rx.1) * replica,
        }
    }

    /// Load on one pod: its service's, shared by the replicas behind one load balancer, times a
    /// small per-pod wobble.
    fn pod_activity(&self, s: View) -> Vec<f64> {
        let (cluster, pod) = (s.get("cluster"), s.get("pod"));
        let service = self.rng(&[cluster, service_of(pod), "activity"]);
        let shared = self.activity(service, s.get("region"), 600.0, 0.2);
        let own = self.walk(self.pod_rng(s, "activity"), 300.0, 0.05);
        shared.into_iter().zip(own).map(|(a, b)| a * b).collect()
    }

    // ---- cAdvisor ---------------------------------------------------------------------------

    fn cadvisor(&self, s: View) -> Vec<Option<f64>> {
        let life = self.container_life(s);
        let values = match s.name() {
            "container_cpu_usage_seconds_total" => self.container_cpu(s, &life),
            "container_memory_working_set_bytes" => self.working_set(s, &life),
            "container_network_receive_bytes_total" => {
                let p = self.profile(s);
                let mut rng = self.container_rng(s, "rx");
                let noise = LogNormal::new(0.0, 0.2).expect("valid lognormal");
                let dt = self.dt();
                let incs: Vec<f64> = self
                    .pod_activity(s)
                    .into_iter()
                    .map(|a| (p.rx_bytes_per_s * a * dt * noise.sample(&mut rng)).round())
                    .collect();
                // The pod sandbox owns the network namespace, so this outlives restarts.
                life.counter((p.rx_bytes_per_s * life.pod_age_s).round(), &incs, false)
            }
            name => panic!("no value model for {name}"),
        };
        mask(values, |i| life.running(i))
    }

    /// CPU seconds a container has used since it (re)started. A JIT-ing process burns extra CPU
    /// for its first minute or two.
    fn container_cpu(&self, s: View, life: &Life) -> Vec<f64> {
        let p = self.profile(s);
        let mut rng = self.container_rng(s, "cpu");
        let noise = LogNormal::new(0.0, 0.1).expect("valid lognormal");
        let dt = self.dt();
        let incs: Vec<f64> = self
            .pod_activity(s)
            .into_iter()
            .enumerate()
            .map(|(i, a)| {
                let warmup = 1.0 + 2.0 * (-life.uptime_at(i, dt) / 60.0).exp();
                p.cpu_cores * a * warmup * dt * noise.sample(&mut rng)
            })
            .collect();
        life.counter(p.cpu_cores * life.uptime_s, &incs, true)
    }

    fn working_set(&self, s: View, life: &Life) -> Vec<f64> {
        let p = self.profile(s);
        let walk = self.walk(self.container_rng(s, "working-set"), 1800.0, 0.03);
        let dt = self.dt();
        self.pod_activity(s)
            .into_iter()
            .zip(walk)
            .enumerate()
            .map(|(i, (a, w))| {
                // Heaps and caches fill over the first few minutes after a (re)start.
                let warm = 0.45 + 0.55 * (1.0 - (-life.uptime_at(i, dt) / 300.0).exp());
                round_to(p.mem_bytes * warm * w * (0.95 + 0.05 * a), PAGE)
            })
            .collect()
    }

    // ---- the application's own /metrics -----------------------------------------------------

    fn application(&self, s: View) -> Vec<Option<f64>> {
        let life = self.container_life(s);
        let name = s.name();
        let n = self.n();
        if name == "up" {
            return (0..n)
                .map(|i| {
                    life.running(i)
                        .then_some(if life.restarted_at(i) { 0.0 } else { 1.0 })
                })
                .collect();
        }
        let values: Vec<f64> = match name {
            "jvm_memory_used_bytes" => {
                let pool = s.get("id");
                self.jvm(s, &life).iter().map(|t| t.used(pool)).collect()
            }
            "jvm_memory_max_bytes" => vec![jvm_max_bytes(s.get("id"), self.jvm_heap_max(s)); n],
            "jvm_threads_states_threads" => self.jvm_threads(s),
            "jvm_gc_pause_seconds_count" | "jvm_gc_pause_seconds_sum" => {
                let major = s.get("action") == "end of major GC";
                let count = name.ends_with("_count");
                self.jvm(s, &life)
                    .iter()
                    .map(|t| {
                        let (c, sum) = if major { t.major } else { t.minor };
                        if count { c } else { sum }
                    })
                    .collect()
            }
            "go_goroutines" => {
                let mut rng = self.container_rng(s, "goroutines");
                let base = log_uniform(&mut rng, 15.0, 400.0);
                let noise = Normal::new(0.0, 0.5 * base.sqrt()).expect("valid normal");
                self.pod_activity(s)
                    .into_iter()
                    .map(|a| {
                        (base * (0.7 + 0.3 * a) + noise.sample(&mut rng))
                            .round()
                            .max(1.0)
                    })
                    .collect()
            }
            "go_threads" => {
                // The runtime adds OS threads when goroutines block in syscalls and never
                // gives them back.
                let mut rng = self.container_rng(s, "threads");
                let base = rng.random_range(6..=20) as f64;
                let mut threads = base;
                (0..n)
                    .map(|i| {
                        if life.restarted_at(i) {
                            threads = base;
                        } else if rng.random_bool(0.003) {
                            threads += 1.0;
                        }
                        threads
                    })
                    .collect()
            }
            "go_memstats_alloc_bytes" => self.go_heap(s, &life).iter().map(|h| h.0).collect(),
            "go_memstats_heap_inuse_bytes" => self.go_heap(s, &life).iter().map(|h| h.1).collect(),
            "go_gc_duration_seconds" => self.go_gc_quantile(s),
            "process_cpu_seconds_total" => self
                .container_cpu(s, &life)
                .into_iter()
                .map(round2)
                .collect(),
            "process_resident_memory_bytes" => {
                let share = self.container_rng(s, "rss").random_range(0.85..0.98);
                self.working_set(s, &life)
                    .into_iter()
                    .map(|w| round_to(w * share, PAGE))
                    .collect()
            }
            "process_open_fds" => {
                let mut rng = self.container_rng(s, "fds");
                let (base, per_load) =
                    (rng.random_range(8..60) as f64, rng.random_range(0.0..50.0));
                self.pod_activity(s)
                    .into_iter()
                    .map(|a| base + poisson(&mut rng, per_load * a))
                    .collect()
            }
            "http_requests_total" | "http_request_duration_seconds_count" => {
                self.http(s, &life).iter().map(|t| t.count).collect()
            }
            "http_request_duration_seconds_sum" => {
                self.http(s, &life).iter().map(|t| t.sum).collect()
            }
            "http_request_duration_seconds_bucket" => {
                let le = s.get("le");
                let k = LE_BUCKETS
                    .iter()
                    .position(|b| *b == le)
                    .unwrap_or_else(|| panic!("no value model for le={le:?}"));
                self.http(s, &life).iter().map(|t| t.buckets[k]).collect()
            }
            name => panic!("no value model for {name}"),
        };
        // A restarting process misses that scrape: Prometheus records `up 0` and nothing else.
        mask(values, |i| life.running(i) && !life.restarted_at(i))
    }

    fn jvm_heap_max(&self, s: View) -> f64 {
        round_to((self.profile(s).mem_bytes * 0.7).max(256.0 * MIB), MIB)
    }

    /// A G1 heap, tick by tick: allocation fills eden, each eden collection promotes a slice to
    /// the old gen, and the old gen is collected back to the live set when it reaches 85%.
    fn jvm(&self, s: View, life: &Life) -> Vec<JvmTick> {
        let heap = self.jvm_heap_max(s);
        let mut rng = self.container_rng(s, "jvm");
        let region = if heap >= 4.0 * GIB { 2.0 * MIB } else { MIB };
        let eden_cap = heap * rng.random_range(0.15..0.35);
        let live = heap * rng.random_range(0.15..0.35);
        let promote = rng.random_range(0.01..0.05);
        let alloc_rate = log_uniform(&mut rng, 5.0 * MIB, 200.0 * MIB);
        let metaspace_cap = log_uniform(&mut rng, 60.0 * MIB, 180.0 * MIB);
        let code_caps = [
            rng.random_range(1.2..2.5) * MIB,
            log_uniform(&mut rng, 10.0 * MIB, 60.0 * MIB),
            log_uniform(&mut rng, 5.0 * MIB, 40.0 * MIB),
        ];
        // (median, shape) of a G1 evacuation pause and of a full collection.
        let (minor_pause, major_pause): ((f64, f64), (f64, f64)) = ((0.008, 0.5), (0.15, 0.4));
        let old_limit = 0.85 * heap;

        // What the counters read after `uptime_s` at the average allocation rate.
        let allocated = life.uptime_s * alloc_rate;
        let minor_n = (allocated / eden_cap).floor();
        let major_n = (allocated * promote / (old_limit - live)).floor();
        let mut minor = (
            minor_n,
            minor_n * lognormal_mean(minor_pause.0.ln(), minor_pause.1),
        );
        let mut major = (
            major_n,
            major_n * lognormal_mean(major_pause.0.ln(), major_pause.1),
        );
        let mut eden = eden_cap * rng.random::<f64>();
        let mut old = live + (old_limit - live) * rng.random::<f64>();
        let mut survivor = region * rng.random_range(1..=8) as f64;

        let dt = self.dt();
        self.pod_activity(s)
            .into_iter()
            .enumerate()
            .map(|(i, a)| {
                if life.restarted_at(i) {
                    (eden, old, minor, major) = (0.0, 0.3 * live, (0.0, 0.0), (0.0, 0.0));
                }
                eden += alloc_rate * a * dt;
                let collections = (eden / eden_cap).floor();
                if collections > 0.0 {
                    eden -= collections * eden_cap;
                    minor.0 += collections;
                    minor.1 += pause_sum(&mut rng, minor_pause, collections);
                    survivor = region * rng.random_range(1..=8) as f64;
                    old += collections * eden_cap * promote;
                    while old >= old_limit {
                        major.0 += 1.0;
                        major.1 += pause_sum(&mut rng, major_pause, 1.0);
                        old = live * rng.random_range(0.9..1.1) + (old - old_limit);
                    }
                }
                let up = life.uptime_at(i, dt);
                let fill = |cap: f64, tau_s: f64| round_to(cap * (1.0 - (-up / tau_s).exp()), KIB);
                let metaspace = fill(metaspace_cap, 300.0);
                JvmTick {
                    eden: eden.round(),
                    survivor,
                    old: old.round(),
                    metaspace,
                    class_space: round_to(0.12 * metaspace, KIB),
                    code: [
                        fill(code_caps[0], 30.0),
                        fill(code_caps[1], 600.0),
                        fill(code_caps[2], 1800.0),
                    ],
                    minor,
                    major,
                }
            })
            .collect()
    }

    fn jvm_threads(&self, s: View) -> Vec<f64> {
        let state = s.get("state");
        let mut rng = self.container_rng(s, state);
        let base = match state {
            "runnable" => rng.random_range(4.0..12.0),
            "waiting" => rng.random_range(20.0..80.0),
            "timed-waiting" => rng.random_range(10.0..40.0),
            _ => 0.0,
        };
        self.pod_activity(s)
            .into_iter()
            .map(|a| match state {
                "runnable" => poisson(&mut rng, base * a).max(1.0),
                "blocked" => poisson(&mut rng, 0.15 * a * a),
                "waiting" | "timed-waiting" => base.round() + poisson(&mut rng, a),
                // `terminated` / `new`: a pooled server never shows any.
                _ => 0.0,
            })
            .collect()
    }

    /// `(go_memstats_alloc_bytes, go_memstats_heap_inuse_bytes)`: the GOGC=100 sawtooth. The heap
    /// grows at the allocation rate until it doubles the live set, then a GC drops it back. The
    /// live set is sized so the heap peak fits in the container's working set.
    fn go_heap(&self, s: View, life: &Life) -> Vec<(f64, f64)> {
        let mut rng = self.container_rng(s, "go-heap");
        let live = self.profile(s).mem_bytes * rng.random_range(0.15..0.35);
        let rate = log_uniform(&mut rng, 64.0 * KIB, 32.0 * MIB);
        let fragmentation = rng.random_range(1.05..1.25);
        let mut alloc = live * rng.random_range(1.0..2.0);
        let dt = self.dt();
        self.pod_activity(s)
            .into_iter()
            .enumerate()
            .map(|(i, a)| {
                if life.restarted_at(i) {
                    alloc = 0.3 * live;
                }
                alloc += rate * a * dt;
                if alloc >= 2.0 * live {
                    alloc = live * rng.random_range(0.95..1.05) + (alloc - 2.0 * live) % live;
                }
                (alloc.round(), round_to(alloc * fragmentation, 8192.0))
            })
            .collect()
    }

    /// `go_gc_duration_seconds{quantile}`. Every quantile series of one process draws the same
    /// pauses, so the quantiles stay ordered.
    fn go_gc_quantile(&self, s: View) -> Vec<f64> {
        let mut rng = self.container_rng(s, "gc-pauses");
        let median = log_uniform(&mut rng, 20e-6, 300e-6);
        let spread = LogNormal::new(0.0, 0.1).expect("valid lognormal");
        let quantile = s.get("quantile");
        (0..self.n())
            .map(|_| {
                let m = median * spread.sample(&mut rng);
                let max = m * rng.random_range(3.0..20.0);
                match quantile {
                    "0" => 0.35 * m,
                    "0.25" => 0.7 * m,
                    "0.5" => m,
                    "0.75" => 1.45 * m,
                    "1" => max,
                    q => panic!("no value model for quantile={q:?}"),
                }
            })
            .collect()
    }

    /// One `(pod, method, route, status_code)` request stream, from which `http_requests_total`
    /// and the duration histogram are all read.
    fn http(&self, s: View, life: &Life) -> Vec<HttpTick> {
        let (cluster, service) = (s.get("cluster"), s.get("job"));
        let (method, route, code) = (s.get("method"), s.get("route"), s.get("status_code"));
        let probe = !route.starts_with("/api");

        // A route's traffic and latency belong to the service, so replicas everywhere agree.
        let mut route_rng = self.rng(&[service, method, route, "route"]);
        let (rps, median, sigma) = match route {
            // Liveness and readiness, every 10 s each.
            "/healthz" => (0.2, route_rng.random_range(0.0005..0.0015), 0.3),
            "/metrics" => (1.0 / self.dt(), route_rng.random_range(0.002..0.02), 0.3),
            _ => (
                log_uniform(&mut route_rng, 0.5, 80.0),
                log_uniform(&mut route_rng, 0.003, 0.25),
                route_rng.random_range(0.5..1.1),
            ),
        };
        let mut code_rng = self.rng(&[service, method, route, code, "status"]);
        let (share, latency) = match code.as_bytes().first() {
            _ if probe => (1.0, 1.0),
            Some(b'2') => (code_rng.random_range(0.97..0.999), 1.0),
            Some(b'4') => (log_uniform(&mut code_rng, 5e-4, 3e-2), 0.3),
            // A 500 fails after doing the work; a 502 / 503 is an upstream refusing fast.
            _ => (
                log_uniform(&mut code_rng, 1e-4, 5e-3),
                if code == "500" { 1.5 } else { 0.1 },
            ),
        };
        let server_error = code.starts_with('5');
        let incident = self.incident(cluster, service);
        let mu0 = (median * latency).ln();
        let edges: [f64; N_LE] = std::array::from_fn(|k| match LE_BUCKETS[k] {
            "+Inf" => f64::INFINITY,
            le => le.parse().expect("numeric le"),
        });

        // What the process served before the window, at the route's average shape.
        let served = (rps * share * life.uptime_s).round();
        let mut tick = HttpTick {
            count: served,
            sum: served * lognormal_mean(mu0, sigma),
            buckets: edges.map(|le| (served * lognormal_cdf(le, mu0, sigma)).round()),
        };
        let act = self.pod_activity(s);
        let mut rng = self.rng(&[
            cluster,
            s.get("namespace"),
            s.get("pod"),
            method,
            route,
            code,
            "http",
        ]);
        let dt = self.dt();
        (0..self.n())
            .map(|i| {
                if !life.running(i) {
                    return tick;
                }
                if life.restarted_at(i) {
                    tick = HttpTick::default();
                }
                let incident_now = incident.as_ref().is_some_and(|r| r.contains(&i));
                let load = if probe { 1.0 } else { act[i] };
                // Latency climbs with load, and triples during an incident.
                let mu = mu0
                    + (1.0 + 0.3 * (load - 1.0).max(0.0)).ln()
                    + if incident_now { 3f64.ln() } else { 0.0 };
                let burst = if incident_now && server_error {
                    30.0
                } else {
                    1.0
                };
                let requests = poisson(&mut rng, rps * share * load * burst * dt) as u64;
                tick.observe(requests, &edges, mu, sigma, &mut rng);
                tick
            })
            .collect()
    }

    /// Now and then one service in one cluster has a bad few minutes.
    fn incident(&self, cluster: &str, service: &str) -> Option<Range<usize>> {
        let mut rng = self.rng(&[cluster, service, "incident"]);
        let n = self.n();
        (n > 0 && rng.random_bool(0.1)).then(|| {
            let start = rng.random_range(0..n);
            start..start + rng.random_range(4..=40)
        })
    }
}

#[derive(Clone, Copy)]
struct View<'a>(&'a [Label]);

impl<'a> View<'a> {
    fn get(self, name: &str) -> &'a str {
        self.0
            .iter()
            .find(|l| l.name == name)
            .map_or("", |l| l.value.as_str())
    }

    fn name(self) -> &'a str {
        self.get("__name__")
    }
}

struct Host<'a> {
    cluster: &'a str,
    node: &'a str,
    region: &'a str,
    cores: usize,
    mem_total: f64,
    uptime_s: f64,
    /// Mean CPU utilisation, 0..1.
    base_util: f64,
}

struct Profile {
    cpu_cores: f64,
    mem_bytes: f64,
    rx_bytes_per_s: f64,
}

/// When a container runs, and when it restarts.
struct Life {
    /// First tick the pod is Running; non-zero only for pods that open the window Pending.
    running_from: usize,
    /// Ticks at which the container restarts inside the window, ascending.
    restarts: Vec<usize>,
    prior_restarts: u32,
    /// Process uptime at `running_from`, seconds.
    uptime_s: f64,
    /// Pod age at `running_from`, seconds.
    pod_age_s: f64,
}

impl Life {
    fn running(&self, i: usize) -> bool {
        i >= self.running_from
    }

    fn restarted_at(&self, i: usize) -> bool {
        self.restarts.binary_search(&i).is_ok()
    }

    fn restarts_by(&self, i: usize) -> u32 {
        self.prior_restarts + self.restarts.partition_point(|&r| r <= i) as u32
    }

    fn uptime_at(&self, i: usize, dt: f64) -> f64 {
        match self.restarts.iter().rev().find(|&&r| r <= i) {
            Some(&r) => (i - r) as f64 * dt + dt / 2.0,
            None => self.uptime_s + i.saturating_sub(self.running_from) as f64 * dt,
        }
    }

    /// Accumulate per-tick increments into a counter that reads `initial` when the pod starts
    /// running and, if `resets`, starts over with each container restart.
    fn counter(&self, initial: f64, incs: &[f64], resets: bool) -> Vec<f64> {
        let mut v = 0.0;
        incs.iter()
            .enumerate()
            .map(|(i, inc)| {
                if i < self.running_from {
                    return 0.0;
                }
                if i == self.running_from {
                    v = initial;
                }
                if resets && self.restarted_at(i) {
                    // Restarted somewhere inside the interval.
                    v = 0.5 * inc;
                } else {
                    v += inc;
                }
                v
            })
            .collect()
    }
}

struct JvmTick {
    eden: f64,
    survivor: f64,
    old: f64,
    metaspace: f64,
    class_space: f64,
    /// `non-nmethods`, `profiled nmethods`, `non-profiled nmethods`.
    code: [f64; 3],
    /// `(count, seconds)` of minor and major collections.
    minor: (f64, f64),
    major: (f64, f64),
}

impl JvmTick {
    fn used(&self, pool: &str) -> f64 {
        match pool {
            "G1 Eden Space" => self.eden,
            "G1 Old Gen" => self.old,
            "G1 Survivor Space" => self.survivor,
            "Metaspace" => self.metaspace,
            "Compressed Class Space" => self.class_space,
            "CodeHeap 'non-nmethods'" => self.code[0],
            "CodeHeap 'profiled nmethods'" => self.code[1],
            "CodeHeap 'non-profiled nmethods'" => self.code[2],
            pool => panic!("no value model for JVM pool {pool:?}"),
        }
    }
}

/// Micrometer reports -1 for the pools without a fixed ceiling (eden, survivor, metaspace).
fn jvm_max_bytes(pool: &str, heap_max: f64) -> f64 {
    match pool {
        "G1 Old Gen" => heap_max,
        "Compressed Class Space" => COMPRESSED_CLASS_SPACE_MAX,
        "CodeHeap 'non-nmethods'" => CODE_HEAP_MAX[0],
        "CodeHeap 'profiled nmethods'" => CODE_HEAP_MAX[1],
        "CodeHeap 'non-profiled nmethods'" => CODE_HEAP_MAX[2],
        _ => -1.0,
    }
}

/// Cumulative state of one request stream, as its counters read after a scrape.
#[derive(Clone, Copy, Default)]
struct HttpTick {
    count: f64,
    sum: f64,
    /// Cumulative, one per [`LE_BUCKETS`] entry.
    buckets: [f64; N_LE],
}

impl HttpTick {
    /// Add `n` requests with log-normal latencies: spread over the buckets by a multinomial draw
    /// (a chain of binomials) and summed at each bucket's conditional mean.
    fn observe(&mut self, n: u64, edges: &[f64; N_LE], mu: f64, sigma: f64, rng: &mut StdRng) {
        let (mut remaining, mut mass, mut lower, mut below) = (n, 1.0, 0.0, 0u64);
        for (k, &upper) in edges.iter().enumerate() {
            let p = (lognormal_cdf(upper, mu, sigma) - lognormal_cdf(lower, mu, sigma)).max(0.0);
            let c = if k + 1 == N_LE || p >= mass {
                remaining
            } else if remaining == 0 || p <= 0.0 {
                0
            } else {
                Binomial::new(remaining, p / mass)
                    .expect("probability in [0, 1]")
                    .sample(rng)
            };
            remaining -= c;
            mass -= p;
            if c > 0 {
                let mean = if p > 1e-12 {
                    lognormal_partial_mean(lower, upper, mu, sigma) / p
                } else {
                    lower
                };
                self.sum += c as f64 * mean;
            }
            below += c;
            self.buckets[k] += below as f64;
            lower = upper;
        }
        self.count += n as f64;
    }
}

/// Load relative to the daily mean: 1.45 at 14:00 local, 0.55 at 02:00.
fn diurnal(epoch_secs: f64, utc_offset_hours: f64) -> f64 {
    let hour = (epoch_secs / 3600.0 + utc_offset_hours).rem_euclid(24.0);
    1.0 + 0.45 * (2.0 * PI * (hour - 14.0) / 24.0).cos()
}

/// Standard-time offsets for the regions the label generator uses.
fn utc_offset_hours(region: &str) -> f64 {
    match region {
        "us-west-2" => -8.0,
        "us-east-1" | "ca-central-1" => -5.0,
        "sa-east-1" => -3.0,
        "eu-central-1" => 1.0,
        "ap-southeast-1" => 8.0,
        "ap-northeast-1" => 9.0,
        _ => 0.0,
    }
}

/// The service behind a ReplicaSet pod name, `<service>-<rs hash>-<pod hash>`.
fn service_of(pod: &str) -> &str {
    pod.rsplitn(3, '-').nth(2).unwrap_or(pod)
}

fn mask(values: Vec<f64>, keep: impl Fn(usize) -> bool) -> Vec<Option<f64>> {
    values
        .into_iter()
        .enumerate()
        .map(|(i, v)| keep(i).then_some(v))
        .collect()
}

fn round_to(x: f64, step: f64) -> f64 {
    (x / step).round() * step
}

fn round2(x: f64) -> f64 {
    round_to_decimal_digits(x, 2)
}

fn log_uniform(rng: &mut StdRng, lo: f64, hi: f64) -> f64 {
    if hi <= lo {
        return lo;
    }
    rng.random_range(lo.ln()..hi.ln()).exp()
}

fn poisson(rng: &mut StdRng, lambda: f64) -> f64 {
    if lambda > 0.0 {
        Poisson::new(lambda).expect("valid poisson").sample(rng)
    } else {
        0.0
    }
}

/// Total of `k` log-normal pauses with median `median` and shape `sigma`: drawn one by one for a
/// handful, by the normal approximation beyond that.
fn pause_sum(rng: &mut StdRng, (median, sigma): (f64, f64), k: f64) -> f64 {
    let dist = LogNormal::new(median.ln(), sigma).expect("valid lognormal");
    if k <= 16.0 {
        return (0..k as usize).map(|_| dist.sample(rng)).sum();
    }
    let mean = lognormal_mean(median.ln(), sigma);
    let sd = mean * (sigma * sigma).exp_m1().sqrt();
    let z: f64 = Normal::new(0.0, 1.0).expect("valid normal").sample(rng);
    (k * mean + k.sqrt() * sd * z).max(0.0)
}

fn phi(z: f64) -> f64 {
    if z == f64::INFINITY {
        1.0
    } else if z == f64::NEG_INFINITY {
        0.0
    } else {
        0.5 * erfc(-z / SQRT_2)
    }
}

fn lognormal_mean(mu: f64, sigma: f64) -> f64 {
    (mu + sigma * sigma / 2.0).exp()
}

fn lognormal_cdf(x: f64, mu: f64, sigma: f64) -> f64 {
    if x <= 0.0 {
        0.0
    } else {
        phi((x.ln() - mu) / sigma)
    }
}

/// `∫ x f(x) dx` over `[a, b]` for the log-normal density `f`.
fn lognormal_partial_mean(a: f64, b: f64, mu: f64, sigma: f64) -> f64 {
    let shifted = |x: f64| {
        if x <= 0.0 {
            0.0
        } else {
            phi((x.ln() - mu - sigma * sigma) / sigma)
        }
    };
    lognormal_mean(mu, sigma) * (shifted(b) - shifted(a))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::OnceLock;

    const TICKS: usize = 40;

    /// The small preset with every series' samples, generated once for all the tests.
    fn fleet() -> &'static (FleetMetrics, Vec<Vec<Sample>>) {
        static FLEET: OnceLock<(FleetMetrics, Vec<Vec<Sample>>)> = OnceLock::new();
        FLEET.get_or_init(|| {
            let scrape = ScrapeConfig {
                samples: TICKS,
                ..ScrapeConfig::default()
            };
            let metrics = FleetMetrics::preset(FleetPreset::Small, scrape);
            let samples = metrics
                .series()
                .iter()
                .map(|s| metrics.samples(s))
                .collect();
            (metrics, samples)
        })
    }

    fn label<'a>(series: &'a SeriesSpec, name: &str) -> &'a str {
        View(&series.labels).get(name)
    }

    /// The series' labels minus `without` (and `__name__`), as a grouping key.
    fn key(series: &SeriesSpec, without: &[&str]) -> String {
        let mut labels: Vec<String> = series
            .labels
            .iter()
            .filter(|l| l.name != "__name__" && !without.contains(&l.name.as_str()))
            .map(|l| format!("{}={}", l.name, l.value))
            .collect();
        labels.sort();
        labels.join(",")
    }

    fn named<'a>(
        name: &'a str,
    ) -> impl Iterator<Item = (&'static SeriesSpec, &'static [Sample])> + 'a {
        let (metrics, samples) = fleet();
        metrics
            .series()
            .iter()
            .zip(samples)
            .filter(move |(s, _)| label(s, "__name__") == name)
            .map(|(s, v)| (s, v.as_slice()))
    }

    fn values(samples: &[Sample]) -> Vec<f64> {
        samples.iter().map(|s| s.value).collect()
    }

    #[test]
    fn every_series_has_a_model() {
        let (metrics, samples) = fleet();
        let mut full = 0;
        for (series, samples) in metrics.series().iter().zip(samples) {
            assert!(samples.len() <= TICKS, "{series:?}");
            full += usize::from(samples.len() == TICKS);
            for s in samples {
                assert!(s.value.is_finite(), "{series:?}: {s:?}");
            }
            for w in samples.windows(2) {
                assert!(w[0].timestamp < w[1].timestamp, "{series:?}");
            }
        }
        // Only pending pods and restarting containers lose scrapes.
        assert!(full * 100 > metrics.series().len() * 95, "{full}");
    }

    #[test]
    fn same_seed_same_values() {
        let (metrics, samples) = fleet();
        let again = FleetMetrics::preset(FleetPreset::Small, *metrics.scrape());
        let bits = |v: &[Sample]| -> Vec<(i64, u64)> {
            v.iter().map(|s| (s.timestamp, s.value.to_bits())).collect()
        };
        for (series, expected) in metrics.series().iter().zip(samples).step_by(37) {
            assert_eq!(bits(&again.samples(series)), bits(expected), "{series:?}");
        }
    }

    #[test]
    fn a_target_shares_one_scrape_slot() {
        let (metrics, samples) = fleet();
        let mut slots: HashMap<String, Vec<i64>> = HashMap::new();
        for (series, samples) in metrics.series().iter().zip(samples) {
            let target = ["cluster", "job", "instance"]
                .map(|l| label(series, l))
                .join("/");
            let slot = slots
                .entry(target)
                .or_insert_with(|| metrics.scrape_times(View(&series.labels)));
            for s in samples {
                assert!(slot.contains(&s.timestamp), "{series:?}");
            }
        }
        let firsts: std::collections::HashSet<i64> = slots.values().map(|ts| ts[0]).collect();
        assert!(
            firsts.len() > slots.len() / 2,
            "targets should not share slots"
        );
    }

    #[test]
    fn counters_only_reset_with_their_container() {
        let (metrics, samples) = fleet();
        let mut resets = 0;
        for (series, samples) in metrics.series().iter().zip(samples) {
            let name = label(series, "__name__");
            let counter = ["_total", "_count", "_sum", "_bucket"]
                .iter()
                .any(|suffix| name.ends_with(suffix));
            if !counter {
                continue;
            }
            for w in samples.windows(2) {
                if w[1].value < w[0].value {
                    resets += 1;
                    let life = metrics.container_life(View(&series.labels));
                    assert!(
                        !label(series, "container").is_empty() && !life.restarts.is_empty(),
                        "{series:?} went backwards without a restart: {w:?}"
                    );
                }
            }
        }
        assert!(resets > 0, "no container restarted inside the window");
    }

    #[test]
    fn cpu_modes_add_up_to_elapsed_time() {
        let dt = fleet().0.dt();
        let mut busy: HashMap<String, f64> = HashMap::new();
        for (series, samples) in named("node_cpu_seconds_total") {
            *busy.entry(key(series, &["mode"])).or_default() +=
                samples[TICKS - 1].value - samples[0].value;
        }
        assert!(!busy.is_empty());
        for (cpu, total) in busy {
            let elapsed = (TICKS - 1) as f64 * dt;
            assert!((total - elapsed).abs() < 0.1, "{cpu}: {total} vs {elapsed}");
        }
    }

    #[test]
    fn histograms_are_cumulative_and_agree_with_their_counters() {
        let mut groups: HashMap<String, HashMap<String, Vec<f64>>> = HashMap::new();
        let (metrics, samples) = fleet();
        for (series, samples) in metrics.series().iter().zip(samples) {
            let name = label(series, "__name__");
            if !name.starts_with("http_request") {
                continue;
            }
            let member = match name {
                "http_request_duration_seconds_bucket" => label(series, "le").to_string(),
                name => name.to_string(),
            };
            groups
                .entry(key(series, &["le"]))
                .or_default()
                .insert(member, values(samples));
        }
        assert!(!groups.is_empty());
        for (group, members) in &groups {
            let count = &members["http_request_duration_seconds_count"];
            assert_eq!(count, &members["http_requests_total"], "{group}");
            assert_eq!(count, &members["+Inf"], "{group}");
            for w in LE_BUCKETS.windows(2) {
                let (lo, hi) = (&members[w[0]], &members[w[1]]);
                assert!(
                    lo.iter().zip(hi).all(|(a, b)| a <= b),
                    "{group}: le {} > le {}",
                    w[0],
                    w[1]
                );
            }
        }
    }

    #[test]
    fn memory_and_disk_stay_within_capacity() {
        let by_host = |name| -> HashMap<String, Vec<f64>> {
            named(name).map(|(s, v)| (key(s, &[]), values(v))).collect()
        };
        let total = by_host("node_memory_MemTotal_bytes");
        let avail = by_host("node_memory_MemAvailable_bytes");
        let cached = by_host("node_memory_Cached_bytes");
        for (host, total) in &total {
            let (avail, cached) = (&avail[host], &cached[host]);
            for i in 0..TICKS {
                assert!(cached[i] <= avail[i] && avail[i] <= total[i], "{host} @{i}");
            }
        }

        let size = by_host("node_filesystem_size_bytes");
        let mut root: HashMap<String, Vec<f64>> = HashMap::new();
        for (series, samples) in named("node_filesystem_avail_bytes") {
            let avail = values(samples);
            assert!(
                avail
                    .iter()
                    .zip(&size[&key(series, &[])])
                    .all(|(a, s)| a <= s),
                "{series:?}"
            );
            if label(series, "device") == "/dev/nvme0n1p1" {
                let host = key(series, &["mountpoint"]);
                if let Some(other) = root.insert(host, avail.clone()) {
                    assert_eq!(other, avail, "`/` and containerd share a filesystem");
                }
            }
        }
        assert!(!root.is_empty());
    }

    #[test]
    fn loopback_receives_what_it_transmits() {
        let rx: HashMap<String, Vec<f64>> = named("node_network_receive_bytes_total")
            .filter(|(s, _)| label(s, "device") == "lo")
            .map(|(s, v)| (key(s, &[]), values(v)))
            .collect();
        assert!(!rx.is_empty());
        for (series, samples) in named("node_network_transmit_bytes_total") {
            if label(series, "device") == "lo" {
                assert_eq!(rx[&key(series, &[])], values(samples), "{series:?}");
            }
        }
    }

    #[test]
    fn gc_quantiles_are_ordered() {
        let mut by_process: HashMap<String, Vec<(String, Vec<f64>)>> = HashMap::new();
        for (series, samples) in named("go_gc_duration_seconds") {
            by_process
                .entry(key(series, &["quantile"]))
                .or_default()
                .push((label(series, "quantile").to_string(), values(samples)));
        }
        assert!(!by_process.is_empty());
        for (process, mut quantiles) in by_process {
            quantiles.sort_by(|a, b| a.0.parse::<f64>().unwrap().total_cmp(&b.0.parse().unwrap()));
            for w in quantiles.windows(2) {
                assert!(w[0].1.iter().zip(&w[1].1).all(|(a, b)| a <= b), "{process}");
            }
        }
    }

    #[test]
    fn a_restart_shows_up_everywhere() {
        let (metrics, _) = fleet();
        let (series, samples) = named("up")
            .find(|(s, _)| {
                !label(s, "app_kubernetes_io_name").is_empty()
                    && !metrics.container_life(View(&s.labels)).restarts.is_empty()
            })
            .expect("some app container restarts inside the window");
        let life = metrics.container_life(View(&series.labels));
        let r = life.restarts[0];
        let ts = metrics.scrape_times(View(&series.labels));
        let up = samples
            .iter()
            .find(|s| s.timestamp == ts[r])
            .expect("up is always scraped");
        assert_eq!(up.value, 0.0);

        let (_, restarts) = named("kube_pod_container_status_restarts_total")
            .find(|(s, _)| {
                ["cluster", "namespace", "pod", "container"]
                    .iter()
                    .all(|l| label(s, l) == label(series, l))
            })
            .expect("kube-state-metrics sees the container");
        assert_eq!(restarts[r].value, restarts[r - 1].value + 1.0);

        // The app's other series miss the scrape the process was down for.
        let cpu = named("process_cpu_seconds_total")
            .chain(named("jvm_threads_states_threads"))
            .find(|(s, _)| label(s, "pod") == label(series, "pod"))
            .expect("every app exposes runtime metrics");
        assert!(cpu.1.iter().all(|s| s.timestamp != ts[r]));
    }

    #[test]
    fn some_pods_start_pending() {
        let pending = named("kube_pod_status_phase")
            .filter(|(s, v)| label(s, "phase") == "Pending" && v[0].value == 1.0)
            .count();
        assert!(pending > 0);
    }
}
