#!/usr/bin/env bash
#
# server_bench.sh — comparative server benchmarks: Valkey TimeSeries versus the
# pinned RedisTimeSeries reference. See docs/plans/rts-comparative-benchmarks-plan.md
# and tools/server_bench/README.md.
#
#   tools/server_bench.sh --profile smoke --dry-run      # counts, fixture size, budget; no servers
#   tools/server_bench.sh --profile smoke                 # subject (local process) vs reference (Docker)
#   tools/server_bench.sh --profile smoke --self-check    # two subject processes; harness check only
#   tools/server_bench.sh --profile core --trials 5 --read-duration 30
#   tools/server_bench.sh --scenario path/to/custom.json
#
# Options:
#   --profile NAME          scenario tools/server_bench/scenarios/NAME.json (default smoke)
#   --scenario FILE         any scenario file
#   --dry-run               print the plan and exit without starting anything
#   --preflight-only        start servers, validate everything, write the manifest, stop
#   --self-check            the "reference" is a second subject build (never a product comparison)
#   --subject-url URL       use an externally managed subject; never reconfigured or flushed
#   --reference-url URL     use an externally managed reference (validated against the pin)
#   --subject-docker        run the subject as the docker-compose.bench.yml service (Linux module)
#   --trials N              override the scenario's trial count
#   --read-duration SECS    override the scenario's timed read duration
#   --out DIR               run directory root (default target/bench-reports/server)
#   --keep                  leave owned servers running on exit
#   --skip-build            do not (re)build the module, exporter or driver
#   -h, --help
#
# Environment (all optional): VALKEY_SERVER_PATH, MODULE_PATH, PYTHON_BIN,
# COMPAT_REFERENCE_MODE (auto|docker|binary), BENCH_CPUS / BENCH_CPUSET /
# BENCH_MEM_LIMIT / BENCH_IO_THREADS / BENCH_VALKEY_IMAGE (docker-compose.bench.yml).
#
# Ownership: everything this script starts, it stops — and only that. An external
# URL is used as-is; a reference container someone else started stays up. The
# reference lifecycle and version validation are the shared tests/reference_server.sh,
# pointed at the benchmark compose overlay and its own compose project.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TESTS_DIR="$ROOT_DIR/tests"
BENCH_DIR="$ROOT_DIR/tools/server_bench"
COMPOSE_FILE="$ROOT_DIR/docker-compose.bench.yml"
COMPOSE_PROJECT="valkey-ts-bench"
SERVER_VERSION="${SERVER_VERSION:-9.0.0}"

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*" >&2; }
warn() { printf '\033[1;33mwarn:\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31merror:\033[0m %s\n' "$*" >&2; exit 1; }

usage() { sed -n '3,36p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; }

# ---------------------------------------------------------------------------
# options
# ---------------------------------------------------------------------------
PROFILE="smoke"
SCENARIO=""
DRY_RUN=false
PREFLIGHT_ONLY=false
SELF_CHECK=false
SUBJECT_URL=""
REFERENCE_URL=""
SUBJECT_DOCKER=false
TRIALS=""
READ_DURATION=""
OUT_DIR="$ROOT_DIR/target/bench-reports/server"
KEEP=false
SKIP_BUILD=false

need_value() { [ $# -ge 2 ] || die "$1 requires a value"; }
while [ $# -gt 0 ]; do
    case "$1" in
        --profile)        need_value "$@"; PROFILE="$2"; shift ;;
        --scenario)       need_value "$@"; SCENARIO="$2"; shift ;;
        --dry-run)        DRY_RUN=true ;;
        --preflight-only) PREFLIGHT_ONLY=true ;;
        --self-check)     SELF_CHECK=true ;;
        --subject-url)    need_value "$@"; SUBJECT_URL="$2"; shift ;;
        --reference-url)  need_value "$@"; REFERENCE_URL="$2"; shift ;;
        --subject-docker) SUBJECT_DOCKER=true ;;
        --trials)         need_value "$@"; TRIALS="$2"; shift ;;
        --read-duration)  need_value "$@"; READ_DURATION="${2%s}"; shift ;;
        --out)            need_value "$@"; OUT_DIR="$2"; shift ;;
        --keep)           KEEP=true ;;
        --skip-build)     SKIP_BUILD=true ;;
        -h|--help)        usage; exit 0 ;;
        *)                echo "error: unknown option '$1'" >&2; usage >&2; exit 1 ;;
    esac
    shift
done

[ -n "$SCENARIO" ] || SCENARIO="$BENCH_DIR/scenarios/$PROFILE.json"
[ -f "$SCENARIO" ] || die "scenario not found: $SCENARIO"
if [ "$SELF_CHECK" = true ] && [ -n "$REFERENCE_URL" ]; then
    die "--self-check and --reference-url are mutually exclusive"
fi
if [ "$SUBJECT_DOCKER" = true ] && [ -n "$SUBJECT_URL" ]; then
    die "--subject-docker and --subject-url are mutually exclusive"
fi

# ---------------------------------------------------------------------------
# python (only for the shared reference helper's probes)
# ---------------------------------------------------------------------------
PY=()
if [ -n "${PYTHON_BIN:-}" ]; then
    PY=("$PYTHON_BIN")
elif [ -n "${VIRTUAL_ENV:-}" ] && [ -x "$VIRTUAL_ENV/bin/python" ]; then
    PY=("$VIRTUAL_ENV/bin/python")
elif [ -x "$ROOT_DIR/.venv/bin/python" ]; then
    PY=("$ROOT_DIR/.venv/bin/python")
elif command -v uv >/dev/null 2>&1; then
    PY=(uv run --project "$ROOT_DIR" python3)
else
    PY=(python3)
fi

# ---------------------------------------------------------------------------
# reference lifecycle (shared helper, pointed at the benchmark overlay)
# ---------------------------------------------------------------------------
COMPAT_REF_PY=("${PY[@]}")
COMPAT_REFERENCE_COMPOSE_FILE="$COMPOSE_FILE"
COMPAT_REFERENCE_COMPOSE_PROJECT="$COMPOSE_PROJECT"
COMPAT_REFERENCE_PORT="${COMPAT_REFERENCE_PORT:-16479}"
COMPAT_REFERENCE_URL="$REFERENCE_URL"
[ "$KEEP" = true ] && COMPAT_KEEP_REFERENCE=1
# shellcheck source=tests/reference_server.sh
. "$TESTS_DIR/reference_server.sh"

# ---------------------------------------------------------------------------
# teardown
# ---------------------------------------------------------------------------
OWNED_PIDS=()
OWNED_WORKDIRS=()
SUBJECT_DOCKER_OWNED=false

cleanup() {
    local status=$?
    set +e
    if [ "$KEEP" = true ] && [ ${#OWNED_PIDS[@]} -gt 0 ]; then
        log "--keep: leaving owned subject server(s) running (pids ${OWNED_PIDS[*]})"
    else
        local pid
        for pid in "${OWNED_PIDS[@]+"${OWNED_PIDS[@]}"}"; do
            if kill -0 "$pid" 2>/dev/null; then
                log "stopping owned server (pid $pid)"
                kill "$pid" 2>/dev/null
                wait "$pid" 2>/dev/null
            fi
        done
        local dir
        for dir in "${OWNED_WORKDIRS[@]+"${OWNED_WORKDIRS[@]}"}"; do
            if [ "$status" -eq 0 ]; then
                rm -rf "$dir"
            else
                warn "server log kept at $dir"
            fi
        done
        if [ "$SUBJECT_DOCKER_OWNED" = true ]; then
            log "stopping subject container"
            docker compose -f "$COMPOSE_FILE" -p "$COMPOSE_PROJECT" stop subject >/dev/null 2>&1
        fi
    fi
    compat_reference_stop
    exit "$status"
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# build artifacts
# ---------------------------------------------------------------------------
module_ext() {
    case "$(uname)" in
        Darwin) echo ".dylib" ;;
        Linux)  echo ".so" ;;
        *)      die "unsupported OS: $(uname)" ;;
    esac
}

MODULE_PATH_RESOLVED=""
ensure_module() {
    if [ -n "${MODULE_PATH:-}" ] && [ -f "$MODULE_PATH" ]; then
        MODULE_PATH_RESOLVED="$MODULE_PATH"
        log "module: $MODULE_PATH_RESOLVED (from MODULE_PATH)"
        return
    fi
    local path="$ROOT_DIR/target/release/libvalkey_timeseries$(module_ext)"
    if [ "$SKIP_BUILD" = false ]; then
        # The live module keeps its normal allocator: never build it with the
        # tool features (enable-system-alloc would change what is measured).
        log "building the module (cargo build --release)"
        (cd "$ROOT_DIR" && cargo build --release)
    fi
    [ -f "$path" ] || die "module not found at $path"
    MODULE_PATH_RESOLVED="$path"
    log "module: $MODULE_PATH_RESOLVED"
}

EXPORTER="$ROOT_DIR/target/release/benchmark_dataset"
ensure_exporter() {
    if [ "$SKIP_BUILD" = false ]; then
        log "building the fixture exporter (benchmark_dataset)"
        (cd "$ROOT_DIR" && cargo build --release --features enable-system-alloc,test-utils --bin benchmark_dataset)
    fi
    [ -x "$EXPORTER" ] || die "exporter not found at $EXPORTER"
}

DRIVER="$BENCH_DIR/target/release/server_bench"
ensure_driver() {
    if [ "$SKIP_BUILD" = false ]; then
        log "building the driver (tools/server_bench, separate workspace)"
        (cd "$BENCH_DIR" && cargo build --release --locked 2>/dev/null || cargo build --release)
    fi
    [ -x "$DRIVER" ] || die "driver not found at $DRIVER"
}

SERVER_PATH_RESOLVED=""
ensure_server() {
    local candidates=()
    [ -n "${VALKEY_SERVER_PATH:-}" ] && candidates+=("$VALKEY_SERVER_PATH")
    candidates+=("$TESTS_DIR/build/binaries/$SERVER_VERSION/valkey-server")
    local path
    for path in "${candidates[@]}"; do
        if [ -x "$path" ]; then
            SERVER_PATH_RESOLVED="$path"
            log "valkey-server: $SERVER_PATH_RESOLVED"
            return
        fi
    done
    if command -v valkey-server >/dev/null 2>&1; then
        SERVER_PATH_RESOLVED="$(command -v valkey-server)"
        log "valkey-server: $SERVER_PATH_RESOLVED (from PATH)"
        return
    fi
    die "no valkey-server found; set VALKEY_SERVER_PATH or run build.sh once to build tests/build/binaries/$SERVER_VERSION"
}

# ---------------------------------------------------------------------------
# servers we own
# ---------------------------------------------------------------------------
free_port() {
    "${PY[@]}" - <<'PY'
import socket
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind(("127.0.0.1", 0))
    print(s.getsockname()[1])
PY
}

# Starts a subject process with the baseline flags (same as docker-compose.bench.yml).
# Sets STARTED_URL; not a command substitution, so the ownership arrays it
# appends to stay visible to cleanup().
STARTED_URL=""
start_subject_process() {  # $1=label
    local port workdir pid
    port="$(free_port)"
    workdir="$(mktemp -d "${TMPDIR:-/tmp}/server-bench-$1.XXXXXX")"
    OWNED_WORKDIRS+=("$workdir")
    # TZ=UTC0: with TZ unset (or naming a zoneinfo *file*, such as UTC or
    # GMT0), macOS libc re-reads the file in every localtime_r, and the server
    # calls that once per event-loop iteration (updateCachedTime); it was half
    # of all main-thread samples in local profiles. A POSIX TZ string with no
    # file behind it is parsed once and cached. glibc caches either way, so the
    # containers are unaffected.
    TZ=UTC0 "$SERVER_PATH_RESOLVED" \
        --port "$port" \
        --dir "$workdir" \
        --logfile "$workdir/server.log" \
        --loadmodule "$MODULE_PATH_RESOLVED" \
        --enable-debug-command yes \
        --notify-keyspace-events "" \
        --maxmemory-policy noeviction \
        --save '' \
        --appendonly no \
        --io-threads "${BENCH_IO_THREADS:-1}" \
        >/dev/null 2>&1 &
    pid=$!
    OWNED_PIDS+=("$pid")
    STARTED_URL="redis://127.0.0.1:$port"
    if ! _compat_ref_wait_ping "$STARTED_URL" 30; then
        tail -n 25 "$workdir/server.log" >&2 2>/dev/null || true
        die "$1 valkey-server failed to start on port $port"
    fi
    log "$1: $STARTED_URL (pid $pid)"
}

start_subject_docker() {
    command -v docker >/dev/null 2>&1 || die "--subject-docker needs docker"
    local port="${BENCH_SUBJECT_PORT:-16480}"
    case "$MODULE_PATH_RESOLVED" in
        *.so) ;;
        *) die "--subject-docker needs a Linux build of the module (.so); set MODULE_PATH" ;;
    esac
    local already
    already="$(docker compose -f "$COMPOSE_FILE" -p "$COMPOSE_PROJECT" ps -q subject 2>/dev/null || true)"
    [ -z "$already" ] || die "a $COMPOSE_PROJECT subject container is already running; stop it first"
    SUBJECT_DOCKER_OWNED=true
    log "starting the subject container on port $port"
    BENCH_MODULE_PATH="$MODULE_PATH_RESOLVED" BENCH_SUBJECT_PORT="$port" \
        docker compose -f "$COMPOSE_FILE" -p "$COMPOSE_PROJECT" up -d --wait subject \
        || die "could not start the subject container"
    STARTED_URL="redis://127.0.0.1:$port"
    _compat_ref_wait_ping "$STARTED_URL" 30 || die "subject container never answered on $STARTED_URL"
    log "subject: $STARTED_URL (docker)"
}

image_digest() {  # $1=service -> RepoDigest of the running container, or empty
    local id
    id="$(docker compose -f "$COMPOSE_FILE" -p "$COMPOSE_PROJECT" ps -q "$1" 2>/dev/null || true)"
    [ -n "$id" ] || return 0
    docker inspect --format '{{index .RepoDigests 0}}' "$(docker inspect --format '{{.Image}}' "$id")" 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
ensure_driver
ensure_exporter

# The fixture spec is owned by the scenario; ask the driver for the export
# arguments so the shape is never restated here.
EXPORT_ARGS=()
while IFS= read -r line; do
    EXPORT_ARGS+=("$line")
done < <("$DRIVER" fixture-args --scenario "$SCENARIO")
[ ${#EXPORT_ARGS[@]} -gt 0 ] || die "could not derive fixture arguments from $SCENARIO"

if [ "$DRY_RUN" = true ]; then
    "$DRIVER" dry-run --scenario "$SCENARIO"
    echo
    "$EXPORTER" --dry-run "${EXPORT_ARGS[@]}"
    exit 0
fi

# Fixtures are cached by their export arguments (values only, in the fixed
# order the driver prints them: series, samples, workload, model, interval,
# prefix, labels, value length); the driver re-verifies the digests on every
# run, so a stale cache entry cannot go unnoticed.
FIXTURE_KEY="${EXPORT_ARGS[1]}x${EXPORT_ARGS[3]}-${EXPORT_ARGS[5]}-${EXPORT_ARGS[7]}-${EXPORT_ARGS[9]}ms-${EXPORT_ARGS[11]}-l${EXPORT_ARGS[13]}-v${EXPORT_ARGS[15]}"
FIXTURE_KEY="$(printf '%s' "$FIXTURE_KEY" | tr -c 'A-Za-z0-9_,.x-' '_')"
FIXTURE_DIR="$OUT_DIR/fixtures/$FIXTURE_KEY"
if [ -f "$FIXTURE_DIR/fixture.json" ]; then
    log "fixture cached at $FIXTURE_DIR"
else
    mkdir -p "$OUT_DIR/fixtures"
    log "exporting fixture to $FIXTURE_DIR"
    "$EXPORTER" --out "$FIXTURE_DIR" "${EXPORT_ARGS[@]}" >/dev/null
fi

ensure_module

NOTES=(--note "deployment.host_os=$(uname -s)" --note "deployment.host_arch=$(uname -m)")
DEPLOYMENT="mixed"

# --- subject -----------------------------------------------------------------
SUBJECT_ARGS=()
if [ -n "$SUBJECT_URL" ]; then
    _compat_ref_wait_ping "$SUBJECT_URL" 5 || die "--subject-url $SUBJECT_URL is not reachable"
    log "subject: $SUBJECT_URL (external)"
    NOTES+=(--note "deployment.subject=external")
elif [ "$SUBJECT_DOCKER" = true ]; then
    start_subject_docker
    SUBJECT_URL="$STARTED_URL"
    SUBJECT_ARGS+=(--subject-owned)
    NOTES+=(--note "deployment.subject=docker" --note "deployment.subject_image=$(image_digest subject)")
else
    ensure_server
    start_subject_process subject
    SUBJECT_URL="$STARTED_URL"
    SUBJECT_ARGS+=(--subject-owned)
    NOTES+=(--note "deployment.subject=process" --note "deployment.subject_binary=$SERVER_PATH_RESOLVED")
fi

# --- reference ---------------------------------------------------------------
REFERENCE_ARGS=()
if [ "$SELF_CHECK" = true ]; then
    ensure_server
    start_subject_process reference
    REFERENCE_URL="$STARTED_URL"
    REFERENCE_ARGS+=(--self-check --reference-owned)
    NOTES+=(--note "deployment.reference=process-self-check")
    DEPLOYMENT="self-check"
else
    compat_reference_start || die "could not provide a reference server"
    REFERENCE_URL="$COMPAT_REFERENCE_URL"
    REFERENCE_ARGS+=(--reference-pin "$COMPAT_REFERENCE_VERSION:$COMPAT_REFERENCE_MODULE_VERSION")
    if [ "$COMPAT_REFERENCE_OWNED" = 1 ]; then
        REFERENCE_ARGS+=(--reference-owned)
    fi
    NOTES+=(--note "deployment.reference=$COMPAT_REFERENCE_KIND")
    if [ "$COMPAT_REFERENCE_KIND" = docker ]; then
        NOTES+=(--note "deployment.reference_image=$(image_digest reference)")
        NOTES+=(--note "deployment.compose_limits=cpus=${BENCH_CPUS:-1.0},cpuset=${BENCH_CPUSET:-},mem=${BENCH_MEM_LIMIT:-4g},io_threads=${BENCH_IO_THREADS:-1}")
        if [ "$SUBJECT_DOCKER" = true ]; then
            DEPLOYMENT="containers-equal-limits"
        fi
    fi
fi
NOTES+=(--note "deployment.kind=$DEPLOYMENT")
if [ "$DEPLOYMENT" != "containers-equal-limits" ]; then
    warn "deployment '$DEPLOYMENT' is exploratory: results are not publishable (plan, 'Comparable environments')"
fi

# --- drive -------------------------------------------------------------------
OVERRIDES=()
[ -n "$TRIALS" ] && OVERRIDES+=(--trials "$TRIALS")
[ -n "$READ_DURATION" ] && OVERRIDES+=(--read-duration-seconds "$READ_DURATION")

VERB=run
[ "$PREFLIGHT_ONLY" = true ] && VERB=preflight

mkdir -p "$OUT_DIR"
export SERVER_BENCH_RUSTC="$(rustc -V 2>/dev/null || true)"
set +e
"$DRIVER" "$VERB" \
    --scenario "$SCENARIO" \
    --fixture "$FIXTURE_DIR" \
    --out "$OUT_DIR" \
    --subject "$SUBJECT_URL" \
    --reference "$REFERENCE_URL" \
    --module-path "$MODULE_PATH_RESOLVED" \
    --repo-root "$ROOT_DIR" \
    "${SUBJECT_ARGS[@]+"${SUBJECT_ARGS[@]}"}" \
    "${REFERENCE_ARGS[@]+"${REFERENCE_ARGS[@]}"}" \
    "${OVERRIDES[@]+"${OVERRIDES[@]}"}" \
    "${NOTES[@]}"
status=$?
set -e
if [ "$status" -ne 0 ]; then
    die "driver failed (exit $status); artifacts, if any, are under $OUT_DIR"
fi
log "done; runs are under $OUT_DIR"
