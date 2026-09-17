//! Realistic label topologies for measuring what label interning saves.
//!
//! [`MetricName`](crate::labels::MetricName) interns each `key=value` pair once, so what it
//! saves is entirely a function of how label values repeat across series. A "ten labels with
//! random values" fixture gets that wrong in both directions: it shares nothing, so interning
//! looks useless, and it has no long identifiers, so the pool looks cheap. Real scrape targets
//! are layered instead, and this generator reproduces that layering for a Kubernetes fleet as a
//! Prometheus-style scrape would see it:
//!
//! * **fleet-wide** (`cluster`, `region`, `env`, `job`): a handful of values shared by every
//!   series in the keyspace;
//! * **per-target** (`instance`, `node`, `pod`, `image`, `uid`): shared by the tens to hundreds
//!   of series a single host or pod exposes;
//! * **per-series** (`id`, histogram `le` × `route` × `status_code`): the identifiers that drive
//!   real cardinality. The cgroup `id` is shared by nothing and ~150 bytes long, so it dominates
//!   whatever the pool retains; the histogram dimensions are shared by every replica of a service,
//!   so they are where interning pays most.
//!
//! The metric families are the ones a fleet actually runs -- node_exporter, cAdvisor (via the
//! kubelet), kube-state-metrics, an HTTP-instrumented application layer with the default
//! Prometheus histogram buckets, and JVM / Go runtime metrics -- with label names spelled the
//! way those exporters spell them. Everything is derived from a seed, so a preset reproduces the
//! same series set byte for byte on any toolchain.

use crate::labels::Label;
use crate::tests::generators::create_rng;
use rand::RngExt;
use rand::prelude::{IndexedRandom, StdRng};
use rand_distr::{Distribution, Zipf};
use std::fmt;

/// A series as `TS.CREATE` would receive it: a key and its labels, `__name__` included.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeriesSpec {
    pub key: String,
    pub labels: Vec<Label>,
}

/// Fleet sizes that the report tool and the unit tests agree on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FleetPreset {
    /// One cluster, ~17k series. Fast enough for a unit test.
    Small,
    /// Three clusters, ~120k series.
    Medium,
    /// An eight-region fleet, ~1M series; a few seconds to generate.
    Large,
}

impl FleetPreset {
    pub const fn all() -> &'static [FleetPreset] {
        &[Self::Small, Self::Medium, Self::Large]
    }

    pub const fn id(self) -> &'static str {
        match self {
            Self::Small => "small",
            Self::Medium => "medium",
            Self::Large => "large",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Self::all().iter().copied().find(|p| p.id() == s)
    }
}

/// Base seed for the fleet presets. Distinct from [`super::DEFAULT_SEED`] so a preset never
/// shares a stream with the sample-value datasets.
pub const DEFAULT_FLEET_SEED: u64 = 0x1ABE_15ED_F1EE;

/// The most distinct `/api/v1/...` routes an HTTP service can expose: every [`RESOURCES`] entry
/// in its collection form and its `{id}` form. `http_surface` draws routes until it has
/// `routes_per_service` distinct ones, so asking for more than this would never finish.
pub const MAX_ROUTES_PER_SERVICE: usize = RESOURCES.len() * 2;

/// Why a [`FleetTopology`] cannot be generated. Checked by [`FleetTopology::validate`] before
/// any series is built, so a bad knob is a usage error rather than a hang or a panic deep in
/// the builder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopologyError {
    /// `clusters > 0` with `hosts_per_cluster == 0`: every pod is scheduled onto one of the
    /// cluster's hosts, so a hostless cluster has nowhere to run anything.
    HostlessClusters { clusters: usize },
    /// `routes_per_service` exceeds [`MAX_ROUTES_PER_SERVICE`], the route vocabulary.
    TooManyRoutes { requested: usize, max: usize },
}

impl fmt::Display for TopologyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HostlessClusters { clusters } => write!(
                f,
                "{clusters} cluster(s) with hosts_per_cluster = 0: every cluster needs at least one host"
            ),
            Self::TooManyRoutes { requested, max } => write!(
                f,
                "routes_per_service = {requested} exceeds the {max} distinct routes a service can expose"
            ),
        }
    }
}

impl std::error::Error for TopologyError {}

/// The knobs that shape a fleet. Every count is a target the generator hits exactly except
/// `pods_per_cluster`, which it fills replica-set by replica-set and may overshoot by one
/// deployment's replica count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FleetTopology {
    pub clusters: usize,
    pub hosts_per_cluster: usize,
    /// How many product namespaces (the [`NAMESPACES`] catalogue after the platform ones) each
    /// cluster runs. Pods are spread over them by a Zipf law, so a few namespaces are large and
    /// most are small, as in any real cluster.
    pub namespaces_per_cluster: usize,
    /// Product pods per cluster, on top of the platform DaemonSets and controllers.
    pub pods_per_cluster: usize,
    /// Distinct `(route)` values an HTTP service exposes. Each route then fans out over methods,
    /// status codes and histogram buckets.
    pub routes_per_service: usize,
    pub seed: u64,
}

impl FleetTopology {
    pub fn preset(preset: FleetPreset) -> Self {
        match preset {
            FleetPreset::Small => Self {
                clusters: 1,
                hosts_per_cluster: 8,
                namespaces_per_cluster: 8,
                pods_per_cluster: 60,
                routes_per_service: 4,
                seed: DEFAULT_FLEET_SEED,
            },
            FleetPreset::Medium => Self {
                clusters: 3,
                hosts_per_cluster: 40,
                namespaces_per_cluster: 20,
                pods_per_cluster: 100,
                routes_per_service: 6,
                seed: DEFAULT_FLEET_SEED,
            },
            FleetPreset::Large => Self {
                clusters: 8,
                hosts_per_cluster: 120,
                namespaces_per_cluster: 35,
                pods_per_cluster: 280,
                routes_per_service: 8,
                seed: DEFAULT_FLEET_SEED,
            },
        }
    }

    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Check the knobs against what the builder can actually produce. `clusters == 0` is a
    /// valid, empty fleet; the other counts are clamped or filled by the builder as documented
    /// on the fields, so only the two combinations that would hang or panic are rejected.
    pub fn validate(&self) -> Result<(), TopologyError> {
        if self.clusters > 0 && self.hosts_per_cluster == 0 {
            return Err(TopologyError::HostlessClusters {
                clusters: self.clusters,
            });
        }
        if self.routes_per_service > MAX_ROUTES_PER_SERVICE {
            return Err(TopologyError::TooManyRoutes {
                requested: self.routes_per_service,
                max: MAX_ROUTES_PER_SERVICE,
            });
        }
        Ok(())
    }

    /// Generate every series in the fleet. Keys are `ts:<n>` in generation order.
    ///
    /// Panics if the topology fails [`validate`](Self::validate); callers that take knobs from
    /// outside (the report tool's overrides) should use [`try_generate`](Self::try_generate)
    /// or validate first and report the error themselves.
    pub fn generate(&self) -> Vec<SeriesSpec> {
        self.try_generate()
            .unwrap_or_else(|e| panic!("invalid fleet topology: {e}"))
    }

    /// [`generate`](Self::generate), returning the validation error instead of panicking.
    pub fn try_generate(&self) -> Result<Vec<SeriesSpec>, TopologyError> {
        self.validate()?;
        let mut rng = create_rng(Some(self.seed));
        let mut fleet = FleetBuilder::new(self, &mut rng);
        for cluster_idx in 0..self.clusters {
            fleet.cluster(cluster_idx);
        }
        Ok(fleet.series)
    }
}

// ---------------------------------------------------------------------------------------------
// Vocabularies. Real names, because value *length* is half of what the pool costs.
// ---------------------------------------------------------------------------------------------

const REGIONS: &[&str] = &[
    "us-east-1",
    "us-west-2",
    "eu-west-1",
    "eu-central-1",
    "ap-southeast-1",
    "ap-northeast-1",
    "sa-east-1",
    "ca-central-1",
];

/// Weighted so most clusters are production, as most fleets are.
const ENVS: &[(&str, u32)] = &[("prod", 6), ("staging", 2), ("dev", 1)];

/// Namespaces in the order a cluster adopts them: the [`PLATFORM_WORKLOADS`] ones first, which
/// every cluster runs, then the product namespaces a cluster picks from.
pub const NAMESPACES: &[&str] = &[
    "kube-system",
    "monitoring",
    "ingress-nginx",
    "logging",
    "cert-manager",
    "checkout",
    "payments",
    "orders",
    "cart",
    "catalog",
    "search",
    "identity",
    "notifications",
    "billing",
    "fraud",
    "recommendations",
    "ads",
    "analytics",
    "data-platform",
    "ml-serving",
    "feature-store",
    "streaming",
    "storage",
    "gateway",
    "web",
    "mobile-bff",
    "support",
    "crm",
    "finance",
    "growth",
    "experimentation",
    "inventory",
    "shipping",
    "pricing",
    "promotions",
    "reviews",
    "media",
    "auth",
    "sessions",
    "audit",
];

const TEAMS: &[&str] = &[
    "platform",
    "commerce",
    "payments",
    "data",
    "growth",
    "security",
    "infra",
    "core-services",
];

/// Workload suffixes for a product namespace. `-api` / `-gateway` / `-web` / `-bff` serve HTTP.
const SERVICE_KINDS: &[&str] = &[
    "api",
    "worker",
    "consumer",
    "scheduler",
    "gateway",
    "web",
    "cron",
    "indexer",
    "bff",
];

/// The platform namespaces run well-known workloads rather than `<ns>-<kind>` ones. A
/// DaemonSet runs one pod per host; anything else is a controller with a replica or two.
const PLATFORM_WORKLOADS: &[(&str, &[(&str, Workload)])] = &[
    (
        "kube-system",
        &[
            ("coredns", Workload::Controller),
            ("kube-proxy", Workload::DaemonSet),
            ("metrics-server", Workload::Controller),
            ("aws-node", Workload::DaemonSet),
            ("ebs-csi-node", Workload::DaemonSet),
            ("cluster-autoscaler", Workload::Controller),
        ],
    ),
    (
        "monitoring",
        &[
            ("prometheus", Workload::Controller),
            ("alertmanager", Workload::Controller),
            ("grafana", Workload::Controller),
            ("node-exporter", Workload::DaemonSet),
            ("kube-state-metrics", Workload::Controller),
        ],
    ),
    (
        "ingress-nginx",
        &[("ingress-nginx-controller", Workload::Controller)],
    ),
    (
        "logging",
        &[
            ("fluent-bit", Workload::DaemonSet),
            ("loki", Workload::Controller),
            ("promtail", Workload::DaemonSet),
        ],
    ),
    (
        "cert-manager",
        &[
            ("cert-manager", Workload::Controller),
            ("cert-manager-webhook", Workload::Controller),
        ],
    ),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Workload {
    DaemonSet,
    Controller,
}

fn is_platform_service(service: &str) -> bool {
    PLATFORM_WORKLOADS
        .iter()
        .any(|(_, list)| list.iter().any(|(s, _)| *s == service))
}

/// Replica counts, weighted towards small deployments with a long tail.
const REPLICA_CHOICES: &[usize] = &[1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 5, 6, 8, 10, 12, 16, 20, 32];

/// Kubernetes QoS classes, weighted the way `requests`/`limits` usually get written. Guaranteed
/// pods sit directly under `kubepods.slice`; the other two get a class sub-slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QosClass {
    Guaranteed,
    Burstable,
    BestEffort,
}

const QOS_CLASSES: &[QosClass] = &[
    QosClass::Burstable,
    QosClass::Burstable,
    QosClass::Burstable,
    QosClass::Guaranteed,
    QosClass::BestEffort,
];

const CPU_MODES: &[&str] = &[
    "idle", "user", "system", "iowait", "irq", "softirq", "steal", "nice",
];

const CORE_CHOICES: &[usize] = &[4, 8, 16, 16, 32, 48, 64];

const MOUNTPOINTS: &[(&str, &str, &str)] = &[
    ("/dev/nvme0n1p1", "ext4", "/"),
    ("/dev/nvme0n1p1", "ext4", "/var/lib/containerd"),
    ("/dev/nvme1n1", "xfs", "/var/lib/kubelet"),
    ("tmpfs", "tmpfs", "/run"),
    ("tmpfs", "tmpfs", "/dev/shm"),
    ("/dev/nvme0n1p15", "vfat", "/boot/efi"),
];

const NET_DEVICES: &[&str] = &["eth0", "eth1", "lo", "docker0", "cni0", "flannel.1"];

const NODE_GAUGES: &[&str] = &[
    "node_memory_MemTotal_bytes",
    "node_memory_MemAvailable_bytes",
    "node_memory_Cached_bytes",
    "node_load1",
    "node_load5",
    "node_load15",
    "node_boot_time_seconds",
    "node_time_seconds",
];

/// The default `prometheus.DefBuckets`, as a client library renders them.
const LE_BUCKETS: &[&str] = &[
    "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10", "+Inf",
];

const RESOURCES: &[&str] = &[
    "users",
    "orders",
    "carts",
    "products",
    "payments",
    "sessions",
    "tokens",
    "events",
    "search",
    "recommendations",
    "invoices",
    "shipments",
    "notifications",
    "webhooks",
    "accounts",
    "addresses",
];

const POD_PHASES: &[&str] = &["Pending", "Running", "Succeeded", "Failed", "Unknown"];

const JVM_POOLS: &[(&str, &str)] = &[
    ("heap", "G1 Eden Space"),
    ("heap", "G1 Old Gen"),
    ("heap", "G1 Survivor Space"),
    ("nonheap", "Metaspace"),
    ("nonheap", "Compressed Class Space"),
    ("nonheap", "CodeHeap 'non-nmethods'"),
    ("nonheap", "CodeHeap 'profiled nmethods'"),
    ("nonheap", "CodeHeap 'non-profiled nmethods'"),
];

const JVM_THREAD_STATES: &[&str] = &[
    "runnable",
    "blocked",
    "waiting",
    "timed-waiting",
    "terminated",
    "new",
];

const GO_GAUGES: &[&str] = &[
    "go_goroutines",
    "go_threads",
    "go_memstats_alloc_bytes",
    "go_memstats_heap_inuse_bytes",
    "process_cpu_seconds_total",
    "process_resident_memory_bytes",
    "process_open_fds",
];

const GO_GC_QUANTILES: &[&str] = &["0", "0.25", "0.5", "0.75", "1"];

/// Kubernetes' own alphabet for generated pod / replica-set suffixes.
const K8S_SUFFIX_ALPHABET: &[u8] = b"bcdfghjklmnpqrstvwxz2456789";

// ---------------------------------------------------------------------------------------------
// Generator
// ---------------------------------------------------------------------------------------------

struct Host {
    /// `ip-10-42-7-113.us-east-1.compute.internal`
    name: String,
    /// `10.42.7.113`
    ip: String,
    cores: usize,
}

struct Pod {
    namespace: &'static str,
    team: &'static str,
    service: String,
    replica_set: String,
    name: String,
    uid: String,
    ip: String,
    version: String,
    qos: QosClass,
    /// `(container, image)`; the first is the workload, the rest are sidecars.
    containers: Vec<(String, String)>,
    host: usize,
}

/// The HTTP surface of one service, shared by every replica -- which is exactly the sharing
/// that makes `route=` / `status_code=` / `le=` pairs cheap once interned.
struct HttpSurface {
    /// `(method, route, status_code)`
    combos: Vec<(&'static str, String, &'static str)>,
}

enum Runtime {
    Jvm,
    Go,
}

struct FleetBuilder<'a> {
    topology: &'a FleetTopology,
    rng: &'a mut StdRng,
    series: Vec<SeriesSpec>,
    next_key: usize,
}

/// One series' labels, with the fleet-wide ones already in place. Built from scratch per series
/// rather than cloning a prefix so the generator's own allocation pattern is dull and predictable.
struct LabelSet {
    labels: Vec<Label>,
}

impl LabelSet {
    fn new(name: &str, fleet: &[(&str, &str)], capacity: usize) -> Self {
        let mut labels = Vec::with_capacity(capacity + fleet.len() + 1);
        labels.push(Label::new("__name__", name));
        for (k, v) in fleet {
            labels.push(Label::new(*k, *v));
        }
        Self { labels }
    }

    fn with(mut self, key: &str, value: &str) -> Self {
        self.labels.push(Label::new(key, value));
        self
    }
}

impl<'a> FleetBuilder<'a> {
    fn new(topology: &'a FleetTopology, rng: &'a mut StdRng) -> Self {
        Self {
            topology,
            rng,
            series: Vec::new(),
            next_key: 0,
        }
    }

    fn push(&mut self, set: LabelSet) {
        let key = format!("ts:{}", self.next_key);
        self.next_key += 1;
        self.series.push(SeriesSpec {
            key,
            labels: set.labels,
        });
    }

    fn cluster(&mut self, cluster_idx: usize) {
        let region = REGIONS[cluster_idx % REGIONS.len()];
        let env = weighted(ENVS, self.rng);
        let cluster = format!("{env}-{region}-{:02}", cluster_idx / REGIONS.len() + 1);
        let fleet: [(&str, &str); 3] = [("cluster", &cluster), ("region", region), ("env", env)];

        let hosts = self.hosts(region);
        let pods = self.pods(&hosts);

        for host in &hosts {
            self.node_exporter(&fleet, host);
        }
        // kube-state-metrics is one target per cluster; its `instance` is a pod IP.
        let ksm_instance = format!("{}:8080", self.pod_ip());
        for pod in &pods {
            self.kube_state_metrics(&fleet, &ksm_instance, pod, &hosts);
            self.cadvisor(&fleet, pod, &hosts);
            self.application(&fleet, pod);
        }
    }

    fn hosts(&mut self, region: &str) -> Vec<Host> {
        (0..self.topology.hosts_per_cluster)
            .map(|_| {
                let (b, c, d) = (
                    self.rng.random_range(0..4u8) + 40,
                    self.rng.random_range(0..=255u8),
                    self.rng.random_range(1..=254u8),
                );
                Host {
                    name: format!("ip-10-{b}-{c}-{d}.{region}.compute.internal"),
                    ip: format!("10.{b}.{c}.{d}"),
                    cores: *CORE_CHOICES.choose(self.rng).expect("non-empty"),
                }
            })
            .collect()
    }

    fn pod_ip(&mut self) -> String {
        format!(
            "10.{}.{}.{}",
            self.rng.random_range(100..=131u8),
            self.rng.random_range(0..=255u8),
            self.rng.random_range(1..=254u8)
        )
    }

    fn k8s_suffix(&mut self, len: usize) -> String {
        (0..len)
            .map(|_| *K8S_SUFFIX_ALPHABET.choose(self.rng).expect("non-empty") as char)
            .collect()
    }

    fn uuid(&mut self) -> String {
        let (a, b) = (self.rng.random::<u64>(), self.rng.random::<u64>());
        format!(
            "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
            a >> 32,
            (a >> 16) & 0xffff,
            a & 0xfff,
            0x8000 | ((b >> 48) & 0x3fff),
            b & 0xffff_ffff_ffff
        )
    }

    fn hex64(&mut self) -> String {
        let (a, b, c, d) = (
            self.rng.random::<u64>(),
            self.rng.random::<u64>(),
            self.rng.random::<u64>(),
            self.rng.random::<u64>(),
        );
        format!("{a:016x}{b:016x}{c:016x}{d:016x}")
    }

    /// The platform tier every cluster runs, then the product pod budget filled deployment by
    /// deployment. Product namespaces are drawn by a Zipf law over the cluster's catalogue so
    /// the head namespaces carry most of the pods.
    fn pods(&mut self, hosts: &[Host]) -> Vec<Pod> {
        let mut pods = Vec::with_capacity(self.topology.pods_per_cluster + 4 * hosts.len());
        // A service's image version rolls out fleet-wide, so a handful of versions per service
        // is realistic; more than that and `image=` would be a per-pod label, which it isn't.
        let mut versions: Vec<(String, Vec<String>)> = Vec::new();

        for (ns_idx, (namespace, services)) in PLATFORM_WORKLOADS.iter().enumerate() {
            let team = TEAMS[ns_idx % TEAMS.len()];
            for (service, workload) in services.iter() {
                let (replicas, pinned) = match workload {
                    Workload::DaemonSet => (hosts.len(), true),
                    Workload::Controller => (self.rng.random_range(1..=2usize), false),
                };
                self.replica_set(
                    namespace,
                    team,
                    service,
                    replicas,
                    pinned,
                    hosts,
                    &mut versions,
                    &mut pods,
                );
            }
        }

        let product = &NAMESPACES[PLATFORM_WORKLOADS.len()..];
        let ns_count = self.topology.namespaces_per_cluster.clamp(1, product.len());
        let zipf = Zipf::new(ns_count as f64, 1.05).expect("valid zipf");
        let platform_pods = pods.len();
        while pods.len() - platform_pods < self.topology.pods_per_cluster {
            let ns_idx = (zipf.sample(self.rng) as usize).clamp(1, ns_count) - 1;
            let namespace = product[ns_idx];
            let team = TEAMS[(ns_idx + PLATFORM_WORKLOADS.len()) % TEAMS.len()];
            let service = format!(
                "{namespace}-{}",
                SERVICE_KINDS.choose(self.rng).expect("non-empty")
            );
            let replicas = *REPLICA_CHOICES.choose(self.rng).expect("non-empty");
            self.replica_set(
                namespace,
                team,
                &service,
                replicas,
                false,
                hosts,
                &mut versions,
                &mut pods,
            );
        }
        pods
    }

    /// One ReplicaSet (or DaemonSet, when `pinned`) worth of pods.
    #[allow(clippy::too_many_arguments)]
    fn replica_set(
        &mut self,
        namespace: &'static str,
        team: &'static str,
        service: &str,
        replicas: usize,
        pinned: bool,
        hosts: &[Host],
        versions: &mut Vec<(String, Vec<String>)>,
        pods: &mut Vec<Pod>,
    ) {
        let version = {
            let entry = match versions.iter().position(|(s, _)| s == service) {
                Some(i) => &versions[i],
                None => {
                    let major = self.rng.random_range(0..4u8);
                    let minor = self.rng.random_range(0..30u8);
                    let list = (0..3u8)
                        .map(|i| {
                            let patch = self.rng.random_range(0..9u8) + i * 3;
                            format!("v{major}.{minor}.{patch}")
                        })
                        .collect();
                    versions.push((service.to_string(), list));
                    versions.last().expect("just pushed")
                }
            };
            entry.1.choose(self.rng).expect("non-empty").clone()
        };

        let registry = match team {
            "platform" | "infra" => "123456789012.dkr.ecr.us-east-1.amazonaws.com",
            "data" => "ghcr.io/example",
            _ => "registry.example.com",
        };
        let image = format!("{registry}/{namespace}/{service}:{version}");
        let replica_set = format!("{service}-{}", self.k8s_suffix(10));

        for replica in 0..replicas {
            let mut containers = vec![(service.to_string(), image.clone())];
            if !pinned && self.rng.random_bool(0.35) {
                containers.push((
                    "istio-proxy".into(),
                    "docker.io/istio/proxyv2:1.22.3".into(),
                ));
            }
            if !pinned && self.rng.random_bool(0.15) {
                containers.push((
                    "log-shipper".into(),
                    "cr.fluentbit.io/fluent/fluent-bit:3.1.4".into(),
                ));
            }
            let host = if pinned {
                replica
            } else {
                self.rng.random_range(0..hosts.len())
            };
            let name = format!("{replica_set}-{}", self.k8s_suffix(5));
            pods.push(Pod {
                namespace,
                team,
                service: service.to_string(),
                replica_set: replica_set.clone(),
                name,
                uid: self.uuid(),
                ip: self.pod_ip(),
                version: version.clone(),
                qos: *QOS_CLASSES.choose(self.rng).expect("non-empty"),
                containers,
                host,
            });
        }
    }

    // ---- exporters --------------------------------------------------------------------------

    fn node_exporter(&mut self, fleet: &[(&str, &str)], host: &Host) {
        let instance = format!("{}:9100", host.ip);
        let base = |name: &str| {
            LabelSet::new(name, fleet, 5)
                .with("job", "node-exporter")
                .with("instance", &instance)
                .with("node", &host.name)
        };

        self.push(base("up"));
        for cpu in 0..host.cores {
            let cpu = cpu.to_string();
            for mode in CPU_MODES {
                self.push(
                    base("node_cpu_seconds_total")
                        .with("cpu", &cpu)
                        .with("mode", mode),
                );
            }
        }
        for (device, fstype, mountpoint) in MOUNTPOINTS {
            for name in ["node_filesystem_size_bytes", "node_filesystem_avail_bytes"] {
                self.push(
                    base(name)
                        .with("device", device)
                        .with("fstype", fstype)
                        .with("mountpoint", mountpoint),
                );
            }
        }
        for device in NET_DEVICES {
            for name in [
                "node_network_receive_bytes_total",
                "node_network_transmit_bytes_total",
            ] {
                self.push(base(name).with("device", device));
            }
        }
        for name in NODE_GAUGES {
            self.push(base(name));
        }
    }

    fn kube_state_metrics(
        &mut self,
        fleet: &[(&str, &str)],
        instance: &str,
        pod: &Pod,
        hosts: &[Host],
    ) {
        let host = &hosts[pod.host];
        let base = |name: &str| {
            LabelSet::new(name, fleet, 8)
                .with("job", "kube-state-metrics")
                .with("instance", instance)
                .with("namespace", pod.namespace)
                .with("pod", &pod.name)
                .with("uid", &pod.uid)
        };

        self.push(
            base("kube_pod_info")
                .with("node", &host.name)
                .with("host_ip", &host.ip)
                .with("pod_ip", &pod.ip)
                .with("created_by_kind", "ReplicaSet")
                .with("created_by_name", &pod.replica_set)
                .with("host_network", "false"),
        );
        for phase in POD_PHASES {
            self.push(base("kube_pod_status_phase").with("phase", phase));
        }
        for (container, _) in &pod.containers {
            self.push(
                base("kube_pod_container_status_restarts_total").with("container", container),
            );
        }
    }

    fn cadvisor(&mut self, fleet: &[(&str, &str)], pod: &Pod, hosts: &[Host]) {
        let host = &hosts[pod.host];
        let instance = format!("{}:10250", host.ip);
        let pod_slice = pod.uid.replace('-', "_");
        let pod_scope = match pod.qos {
            QosClass::Guaranteed => format!("/kubepods.slice/kubepods-pod{pod_slice}.slice"),
            QosClass::Burstable => format!(
                "/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod{pod_slice}.slice"
            ),
            QosClass::BestEffort => format!(
                "/kubepods.slice/kubepods-besteffort.slice/kubepods-besteffort-pod{pod_slice}.slice"
            ),
        };
        for (container, image) in &pod.containers {
            // A systemd cgroup path under containerd: the longest label a fleet carries, and
            // unique per container, so nothing about it can be shared.
            let id = format!("{pod_scope}/cri-containerd-{}.scope", self.hex64());
            for name in [
                "container_cpu_usage_seconds_total",
                "container_memory_working_set_bytes",
                "container_network_receive_bytes_total",
            ] {
                self.push(
                    LabelSet::new(name, fleet, 9)
                        .with("job", "kubelet")
                        .with("metrics_path", "/metrics/cadvisor")
                        .with("instance", &instance)
                        .with("node", &host.name)
                        .with("namespace", pod.namespace)
                        .with("pod", &pod.name)
                        .with("container", container)
                        .with("image", image)
                        .with("id", &id),
                );
            }
        }
    }

    /// The pod's own `/metrics`: runtime gauges for every workload, plus the HTTP histogram
    /// family for the ones that serve requests.
    fn application(&mut self, fleet: &[(&str, &str)], pod: &Pod) {
        let port = if is_http_service(&pod.service) {
            8080
        } else {
            9090
        };
        let instance = format!("{}:{port}", pod.ip);
        let base = |name: &str| {
            LabelSet::new(name, fleet, 6)
                .with("job", &pod.service)
                .with("instance", &instance)
                .with("namespace", pod.namespace)
                .with("pod", &pod.name)
                .with("container", &pod.service)
                .with("app_kubernetes_io_name", &pod.service)
                .with("app_kubernetes_io_version", &pod.version)
                .with("team", pod.team)
        };

        self.push(base("up"));
        match runtime_of(&pod.service) {
            Runtime::Jvm => {
                for (area, id) in JVM_POOLS {
                    self.push(
                        base("jvm_memory_used_bytes")
                            .with("area", area)
                            .with("id", id),
                    );
                    self.push(
                        base("jvm_memory_max_bytes")
                            .with("area", area)
                            .with("id", id),
                    );
                }
                for state in JVM_THREAD_STATES {
                    self.push(base("jvm_threads_states_threads").with("state", state));
                }
                for (action, cause) in [
                    ("end of minor GC", "G1 Evacuation Pause"),
                    ("end of major GC", "G1 Compaction Pause"),
                ] {
                    for name in ["jvm_gc_pause_seconds_count", "jvm_gc_pause_seconds_sum"] {
                        self.push(base(name).with("action", action).with("cause", cause));
                    }
                }
            }
            Runtime::Go => {
                for name in GO_GAUGES {
                    self.push(base(name));
                }
                for quantile in GO_GC_QUANTILES {
                    self.push(base("go_gc_duration_seconds").with("quantile", quantile));
                }
            }
        }

        if is_http_service(&pod.service) {
            let surface = self.http_surface(&pod.service);
            for (method, route, code) in &surface.combos {
                let http = |name: &str| {
                    base(name)
                        .with("method", method)
                        .with("route", route)
                        .with("status_code", code)
                };
                self.push(http("http_requests_total"));
                self.push(http("http_request_duration_seconds_count"));
                self.push(http("http_request_duration_seconds_sum"));
                for le in LE_BUCKETS {
                    self.push(http("http_request_duration_seconds_bucket").with("le", le));
                }
            }
        }
    }

    /// Which `(method, route, status)` combinations a service has actually served. Derived from
    /// the service name alone so every replica -- in every cluster -- exposes the same set.
    fn http_surface(&mut self, service: &str) -> HttpSurface {
        let mut rng = create_rng(Some(self.topology.seed ^ fnv1a(service.as_bytes())));
        let mut combos = Vec::new();
        let mut routes: Vec<String> = vec!["/healthz".into(), "/metrics".into()];
        while routes.len() < self.topology.routes_per_service + 2 {
            let resource = RESOURCES.choose(&mut rng).expect("non-empty");
            let route = if rng.random_bool(0.5) {
                format!("/api/v1/{resource}")
            } else {
                format!("/api/v1/{resource}/{{id}}")
            };
            if !routes.contains(&route) {
                routes.push(route);
            }
        }
        for route in routes {
            let is_probe = !route.starts_with("/api");
            let is_item = route.ends_with("{id}");
            let mut methods = vec!["GET"];
            if !is_probe {
                if !is_item && rng.random_bool(0.6) {
                    methods.push("POST");
                }
                if is_item {
                    if rng.random_bool(0.4) {
                        methods.push("PUT");
                    }
                    if rng.random_bool(0.3) {
                        methods.push("DELETE");
                    }
                }
            }
            for method in methods {
                let success = match method {
                    "POST" => "201",
                    "DELETE" => "204",
                    _ => "200",
                };
                combos.push((method, route.clone(), success));
                if is_probe {
                    continue;
                }
                let errors: &[&str] = &["400", "401", "403", "404", "429", "500", "502", "503"];
                let n = rng.random_range(1..=3usize);
                for code in errors.sample(&mut rng, n) {
                    combos.push((method, route.clone(), *code));
                }
            }
        }
        // `sample` order is not stable across the reference and the report, so pin it.
        combos.sort();
        HttpSurface { combos }
    }
}

fn is_http_service(service: &str) -> bool {
    service.ends_with("-api")
        || service.ends_with("-gateway")
        || service.ends_with("-web")
        || service.ends_with("-bff")
        || matches!(
            service,
            "grafana" | "prometheus" | "alertmanager" | "ingress-nginx-controller"
        )
}

/// Which runtime a service is written in. Product APIs and workers are the JVM shops; the
/// platform and everything else is Go.
fn runtime_of(service: &str) -> Runtime {
    if is_platform_service(service) {
        return Runtime::Go;
    }
    if service.ends_with("-api") || service.ends_with("-worker") || service.ends_with("-consumer") {
        Runtime::Jvm
    } else {
        Runtime::Go
    }
}

fn weighted<'a>(choices: &[(&'a str, u32)], rng: &mut StdRng) -> &'a str {
    choices
        .choose_weighted(rng, |(_, w)| *w)
        .expect("non-empty, positive weights")
        .0
}

/// Stable across toolchains, unlike `DefaultHasher`; see [`super::dataset_seed`].
fn fnv1a(bytes: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    bytes.iter().fold(FNV_OFFSET, |hash, b| {
        (hash ^ u64::from(*b)).wrapping_mul(FNV_PRIME)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};

    fn small() -> Vec<SeriesSpec> {
        FleetTopology::preset(FleetPreset::Small).generate()
    }

    #[test]
    fn same_seed_same_fleet() {
        assert_eq!(small(), small());
    }

    #[test]
    fn different_seed_different_fleet() {
        let other = FleetTopology::preset(FleetPreset::Small)
            .with_seed(7)
            .generate();
        assert_ne!(small(), other);
    }

    #[test]
    fn label_sets_are_unique_and_well_formed() {
        let fleet = small();
        let mut seen = HashSet::with_capacity(fleet.len());
        for series in &fleet {
            assert!(
                series.labels.iter().any(|l| l.name == "__name__"),
                "{series:?}"
            );
            assert!(series.labels.len() <= crate::labels::MAX_LABELS_PER_SERIES);
            let mut sorted = series.labels.clone();
            sorted.sort();
            let names: HashSet<&str> = sorted.iter().map(|l| l.name.as_str()).collect();
            assert_eq!(
                names.len(),
                sorted.len(),
                "duplicate label name in {series:?}"
            );
            for l in &sorted {
                assert!(!l.value.is_empty(), "{l:?}");
                assert!(!l.value.contains(['\n', '"']), "{l:?}");
            }
            assert!(seen.insert(sorted), "duplicate label set: {series:?}");
        }
    }

    /// The property the whole module exists for: the fleet has to *look* like a real one, with
    /// a few fleet-wide values, many per-target ones and a class of per-series identifiers.
    #[test]
    fn cardinality_is_layered() {
        let fleet = small();
        let mut values: HashMap<&str, HashSet<&str>> = HashMap::new();
        for series in &fleet {
            for l in &series.labels {
                values.entry(&l.name).or_default().insert(&l.value);
            }
        }
        let card = |k: &str| values.get(k).map_or(0, HashSet::len);

        assert_eq!(card("cluster"), 1);
        assert!(card("__name__") > 30, "{}", card("__name__"));
        // 60 product pods plus the DaemonSets (one per host) and controllers.
        assert!(card("pod") > 60 + 5 * 8, "{}", card("pod"));
        assert_eq!(card("node"), 8);
        assert!(
            values["id"].iter().any(|v| v.starts_with("G1 ")),
            "no JVM series"
        );
        assert_eq!(card("le"), LE_BUCKETS.len());
        // `id` carries both the JVM pool names and the cgroup paths; every cgroup is unique.
        let cgroups = values["id"]
            .iter()
            .filter(|v| v.starts_with("/kubepods"))
            .count();
        let containers: usize = fleet
            .iter()
            .filter(|s| s.labels.iter().any(|l| l.name == "metrics_path"))
            .count()
            / 3;
        assert_eq!(cgroups, containers);
    }

    #[test]
    fn presets_scale_as_documented() {
        let small = small().len();
        assert!((5_000..20_000).contains(&small), "small preset: {small}");
    }

    #[test]
    fn presets_validate() {
        for preset in FleetPreset::all() {
            assert_eq!(FleetTopology::preset(*preset).validate(), Ok(()));
        }
    }

    #[test]
    fn hostless_clusters_are_rejected() {
        let mut topology = FleetTopology::preset(FleetPreset::Small);
        topology.hosts_per_cluster = 0;
        assert_eq!(
            topology.validate(),
            Err(TopologyError::HostlessClusters { clusters: 1 })
        );
        assert!(topology.try_generate().is_err());

        // No clusters means no pods to schedule, so no hosts is fine: an empty fleet.
        topology.clusters = 0;
        assert_eq!(topology.try_generate(), Ok(Vec::new()));
    }

    #[test]
    fn routes_are_capped_by_the_vocabulary() {
        let mut topology = FleetTopology::preset(FleetPreset::Small);
        topology.routes_per_service = MAX_ROUTES_PER_SERVICE + 1;
        assert_eq!(
            topology.validate(),
            Err(TopologyError::TooManyRoutes {
                requested: MAX_ROUTES_PER_SERVICE + 1,
                max: MAX_ROUTES_PER_SERVICE,
            })
        );

        // The cap itself is reachable: every route in the vocabulary gets drawn.
        topology.routes_per_service = MAX_ROUTES_PER_SERVICE;
        assert_eq!(topology.validate(), Ok(()));
        let fleet = topology.try_generate().expect("cap is generatable");
        let routes: HashSet<&str> = fleet
            .iter()
            .flat_map(|s| s.labels.iter())
            .filter(|l| l.name == "route")
            .map(|l| l.value.as_str())
            .collect();
        // The two probe routes on top of the API vocabulary.
        assert_eq!(routes.len(), MAX_ROUTES_PER_SERVICE + 2, "{routes:?}");
    }

    #[test]
    #[should_panic(expected = "invalid fleet topology")]
    fn generate_panics_on_invalid_topology() {
        let mut topology = FleetTopology::preset(FleetPreset::Small);
        topology.hosts_per_cluster = 0;
        topology.generate();
    }
}
