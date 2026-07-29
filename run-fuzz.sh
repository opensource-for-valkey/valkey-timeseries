#!/usr/bin/env bash
#
# run-fuzz.sh — drive the RedisTimeSeries differential compatibility fuzzer
# (test plan §4.3, tests/compat/README.md).
#
# What it does, in order:
#
#   1. resolves a Python interpreter and installs the test dependencies if they
#      are missing (uv / pip / pip3, from pyproject.toml or requirements.txt),
#   2. clones valkey-test-framework and builds valkey-server + the module if the
#      artifacts the harness needs are not on disk,
#   3. starts the pinned RedisTimeSeries reference container (docker compose),
#   4. starts a subject valkey-server with the module loaded and puts it in
#      `ts-compatibility-mode strict`,
#   5. runs the Hypothesis fuzzer against both, in rounds, until the example cap
#      or the wall-clock budget is spent,
#   6. tears down everything it started.
#
# Step 4 is the point of the script. The fuzzer diffs every reply against
# RedisTimeSeries, so a subject left in the default `extended` mode fails on the
# first *intentional* gated divergence (e.g. DIV-0023) and Hypothesis stops the
# test there — a "soak" that dies in seconds having explored a few hundred
# examples. `strict` selects the RedisTimeSeries side of those gated cases, so a
# failure means an *unregistered* divergence: a real finding.
#
# Clean-room rule: the reference image is a black-box target only. Never consult
# RedisTimeSeries source or test code when investigating a finding — see
# tests/compat/README.md.
#
# ---------------------------------------------------------------------------
# Usage: ./run-fuzz.sh [options] [-- extra pytest args]
#
# Workload
#   -n, --examples N        Hypothesis examples per protocol per round
#                           (COMPAT_FUZZ_MAX_EXAMPLES, default 150).
#   -d, --duration DUR      Keep starting fresh rounds until DUR of wall clock
#                           has elapsed; accepts 90s / 20m / 2h / bare seconds.
#                           Throughput is ~55-60 examples/sec/protocol, so pair
#                           a large --examples with the budget you actually want
#                           (plan §4.3 suggests 20m nightly). Default: one round.
#   -r, --rounds N          Run exactly N rounds (mutually exclusive with
#                           --duration). Default 1.
#   -p, --protocol P        resp2 | resp3 | both (default both). Selects the
#                           RESP parametrization via pytest -k.
#   -s, --suite S           fuzz (default) | corpus | all. `corpus` replays the
#                           checked-in regression corpus; `all` runs the whole
#                           tests/compat suite with the fuzzer enabled.
#   -k, --filter EXPR       Extra pytest -k expression, ANDed with --protocol.
#
# Reproducibility
#       --seed N            Fixed Hypothesis seed (--hypothesis-seed).
#       --derandomize       Fixed internal seed (COMPAT_FUZZ_DERANDOMIZE=1).
#                           With --duration/--rounds every round then explores
#                           the same examples, so use it for debugging, not soaks.
#       --stats             Print Hypothesis statistics per round.
#
# Servers
#       --compat-mode M     strict (default) | extended | keep. What to set
#                           ts.ts-compatibility-mode to on the subject. `keep`
#                           leaves the server's current value alone.
#       --subject-url URL   Use an already-running subject (COMPAT_SUBJECT_URL);
#                           skips the local build+launch. --compat-mode is still
#                           applied, and the previous value restored on exit.
#       --reference-url URL Use an already-running reference (COMPAT_REFERENCE_URL);
#                           skips Docker entirely.
#       --reference-port P  Host port for the compose reference (default 16379).
#       --keep-reference    Leave the reference container running afterwards.
#       --server-version V  valkey-server version to build/use (default unstable).
#
# Build / environment
#       --skip-build        Never build; fail if the module or server is missing.
#       --rebuild           Force `cargo build --release` even if the module exists.
#       --skip-install      Do not install Python dependencies.
#       --python PATH       Interpreter to use (default: $VIRTUAL_ENV, then
#                           ./.venv, then `uv run python3`, then python3).
#
# Output
#       --report PATH       Conformance report path (COMPAT_REPORT_PATH,
#                           default test-data/compat-report.json).
#   -v, --verbose           Verbose pytest output (-v instead of -q).
#       --dry-run           Print what would run, then exit.
#   -h, --help              This message.
#
# Everything after `--` is passed to pytest verbatim.
#
# Examples
#   ./run-fuzz.sh                        # quick 150-example check
#   ./run-fuzz.sh -n 20000 -d 20m --stats  # nightly soak
#   ./run-fuzz.sh -p resp3 --derandomize --seed 4 -v   # debug a find
#   ./run-fuzz.sh -s corpus              # replay the corpus only
#   ./run-fuzz.sh --reference-url redis://127.0.0.1:16379
#
# Exit status: 0 all rounds clean; otherwise the failing pytest status (1 =
# divergence or error, 5 = nothing collected).
# ---------------------------------------------------------------------------

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTS_DIR="$ROOT_DIR/tests"
COMPAT_DIR="$TESTS_DIR/compat"

COMPOSE_FILE="$ROOT_DIR/docker-compose.compat.yml"
TEST_FRAMEWORK_DIR="$TESTS_DIR/valkeytestframework"
TEST_FRAMEWORK_REPO="https://github.com/valkey-io/valkey-test-framework"
VALKEY_REPO_URL="https://github.com/valkey-io/valkey.git"

# ---------------------------------------------------------------------------
# defaults (every one overridable by flag; env wins over the built-in default)
# ---------------------------------------------------------------------------
EXAMPLES="${COMPAT_FUZZ_MAX_EXAMPLES:-150}"
DURATION=""
ROUNDS=1
PROTOCOL="both"
SUITE="fuzz"
FILTER=""
SEED=""
DERANDOMIZE="${COMPAT_FUZZ_DERANDOMIZE:-}"
STATS=false
COMPAT_MODE="strict"
SUBJECT_URL="${COMPAT_SUBJECT_URL:-}"
REFERENCE_URL="${COMPAT_REFERENCE_URL:-}"
REFERENCE_PORT="${COMPAT_REFERENCE_PORT:-16379}"
KEEP_REFERENCE="${COMPAT_KEEP_REFERENCE:-}"
SERVER_VERSION="${SERVER_VERSION:-unstable}"
SKIP_BUILD=false
REBUILD=false
SKIP_INSTALL=false
PYTHON_BIN=""
REPORT_PATH="${COMPAT_REPORT_PATH:-$ROOT_DIR/test-data/compat-report.json}"
VERBOSE=false
DRY_RUN=false
PYTEST_EXTRA=()

usage() { sed -n '/^# Usage:/,/^# ----/p' "${BASH_SOURCE[0]}" | sed -e 's/^# \{0,1\}//' -e '$d'; }

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*" >&2; }
warn() { printf '\033[1;33mwarn:\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31merror:\033[0m %s\n' "$*" >&2; exit 1; }

# 90s / 20m / 2h / bare seconds -> seconds
parse_duration() {
    local raw="$1" num unit
    num="${raw%[smhSMH]}"
    unit="${raw#"$num"}"
    [[ "$num" =~ ^[0-9]+$ ]] || die "invalid duration: $raw (use 90s, 20m, 2h)"
    unit="$(printf '%s' "$unit" | tr '[:upper:]' '[:lower:]')"
    case "$unit" in
        ""|s) echo "$num" ;;
        m)    echo $((num * 60)) ;;
        h)    echo $((num * 3600)) ;;
        *)    die "invalid duration unit in: $raw" ;;
    esac
}

# ---------------------------------------------------------------------------
# argument parsing
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        -n|--examples)       EXAMPLES="$2"; shift 2 ;;
        -d|--duration)       DURATION="$2"; shift 2 ;;
        -r|--rounds)         ROUNDS="$2"; shift 2 ;;
        -p|--protocol)       PROTOCOL="$2"; shift 2 ;;
        -s|--suite)          SUITE="$2"; shift 2 ;;
        -k|--filter)         FILTER="$2"; shift 2 ;;
        --seed)              SEED="$2"; shift 2 ;;
        --derandomize)       DERANDOMIZE=1; shift ;;
        --stats)             STATS=true; shift ;;
        --compat-mode)       COMPAT_MODE="$2"; shift 2 ;;
        --subject-url)       SUBJECT_URL="$2"; shift 2 ;;
        --reference-url)     REFERENCE_URL="$2"; shift 2 ;;
        --reference-port)    REFERENCE_PORT="$2"; shift 2 ;;
        --keep-reference)    KEEP_REFERENCE=1; shift ;;
        --server-version)    SERVER_VERSION="$2"; shift 2 ;;
        --skip-build)        SKIP_BUILD=true; shift ;;
        --rebuild)           REBUILD=true; shift ;;
        --skip-install)      SKIP_INSTALL=true; shift ;;
        --python)            PYTHON_BIN="$2"; shift 2 ;;
        --report)            REPORT_PATH="$2"; shift 2 ;;
        -v|--verbose)        VERBOSE=true; shift ;;
        --dry-run)           DRY_RUN=true; shift ;;
        -h|--help)           usage; exit 0 ;;
        --)                  shift; PYTEST_EXTRA=("$@"); break ;;
        *)                   die "unknown option: $1 (try --help)" ;;
    esac
done

[[ "$EXAMPLES" =~ ^[0-9]+$ ]] || die "--examples takes an integer, got: $EXAMPLES"
[[ "$ROUNDS" =~ ^[0-9]+$ && "$ROUNDS" -gt 0 ]] || die "--rounds takes a positive integer"
case "$PROTOCOL" in resp2|resp3|both) ;; *) die "--protocol must be resp2, resp3 or both" ;; esac
case "$SUITE" in fuzz|corpus|all) ;; *) die "--suite must be fuzz, corpus or all" ;; esac
case "$COMPAT_MODE" in strict|extended|keep) ;; *) die "--compat-mode must be strict, extended or keep" ;; esac

DURATION_SECS=""
if [[ -n "$DURATION" ]]; then
    [[ "$ROUNDS" == 1 ]] || die "--duration and --rounds are mutually exclusive"
    DURATION_SECS="$(parse_duration "$DURATION")"
fi
if [[ -n "$DERANDOMIZE" && ( -n "$DURATION_SECS" || "$ROUNDS" -gt 1 ) ]]; then
    warn "--derandomize with multiple rounds: every round explores the same examples"
fi

# ---------------------------------------------------------------------------
# reference server lifecycle (shared with build.sh)
#
# The helper exposes provision/start/stop and reports whether we own the reference;
# it installs no traps of its own, so the teardown below stays in charge.
# ---------------------------------------------------------------------------
# shellcheck source=tests/reference_server.sh
. "$TESTS_DIR/reference_server.sh"

# ---------------------------------------------------------------------------
# teardown (registered as soon as we own a resource)
# ---------------------------------------------------------------------------
SUBJECT_PID=""
SUBJECT_WORKDIR=""
REFERENCE_STARTED=false
PREVIOUS_COMPAT_MODE=""
EXTERNAL_SUBJECT=false

cleanup() {
    local status=$?
    set +e
    if [[ -n "$PREVIOUS_COMPAT_MODE" && "$EXTERNAL_SUBJECT" == true ]]; then
        log "restoring ts-compatibility-mode=$PREVIOUS_COMPAT_MODE on the external subject"
        set_compat_mode "$SUBJECT_URL" "$PREVIOUS_COMPAT_MODE" >/dev/null
    fi
    if [[ -n "$SUBJECT_PID" ]] && kill -0 "$SUBJECT_PID" 2>/dev/null; then
        log "stopping subject server (pid $SUBJECT_PID)"
        kill "$SUBJECT_PID" 2>/dev/null
        wait "$SUBJECT_PID" 2>/dev/null
    fi
    if [[ -n "$SUBJECT_WORKDIR" && "$status" -eq 0 ]]; then
        rm -rf "$SUBJECT_WORKDIR"
    elif [[ -n "$SUBJECT_WORKDIR" ]]; then
        warn "subject log kept at $SUBJECT_WORKDIR/subject.log"
    fi
    compat_reference_stop
    exit "$status"
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# python interpreter + dependencies
# ---------------------------------------------------------------------------
PY=()
resolve_python() {
    if [[ -n "$PYTHON_BIN" ]]; then
        PY=("$PYTHON_BIN")
    elif [[ -n "${VIRTUAL_ENV:-}" && -x "$VIRTUAL_ENV/bin/python" ]]; then
        PY=("$VIRTUAL_ENV/bin/python")
    elif [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
        PY=("$ROOT_DIR/.venv/bin/python")
    elif command -v uv >/dev/null 2>&1; then
        PY=(uv run --project "$ROOT_DIR" python3)
    elif command -v python3 >/dev/null 2>&1; then
        PY=(python3)
    else
        die "no python3 found; install Python 3.11+ or pass --python"
    fi
    log "python: ${PY[*]} ($("${PY[@]}" --version 2>&1))"
}

have_deps() { "${PY[@]}" -c 'import valkey, pytest, yaml, hypothesis' >/dev/null 2>&1; }

ensure_python_deps() {
    if have_deps; then
        return
    fi
    if [[ "$SKIP_INSTALL" == true ]]; then
        die "missing Python dependencies (valkey, pytest, PyYAML, hypothesis) and --skip-install was given"
    fi
    log "installing Python dependencies"
    if command -v uv >/dev/null 2>&1; then
        (cd "$ROOT_DIR" && uv sync)
        # uv sync populates .venv; prefer it from here on unless --python was given.
        if [[ -z "$PYTHON_BIN" && -x "$ROOT_DIR/.venv/bin/python" ]]; then
            PY=("$ROOT_DIR/.venv/bin/python")
        fi
    elif command -v pip3 >/dev/null 2>&1; then
        pip3 install -r "$ROOT_DIR/requirements.txt"
    elif command -v pip >/dev/null 2>&1; then
        pip install -r "$ROOT_DIR/requirements.txt"
    else
        die "neither uv, pip nor pip3 is available; install one, or install requirements.txt manually"
    fi
    have_deps || die "dependencies still missing after install (need valkey, pytest, PyYAML, hypothesis)"
}

ensure_test_framework() {
    [[ -d "$TEST_FRAMEWORK_DIR" ]] && return
    if [[ "$SKIP_BUILD" == true ]]; then
        die "valkey-test-framework missing at $TEST_FRAMEWORK_DIR and --skip-build was given"
    fi
    log "cloning valkey-test-framework"
    local tmp
    tmp="$(mktemp -d)"
    git clone --depth 1 "$TEST_FRAMEWORK_REPO" "$tmp/valkey-test-framework"
    mkdir -p "$TEST_FRAMEWORK_DIR"
    mv "$tmp/valkey-test-framework/src"/* "$TEST_FRAMEWORK_DIR/"
    rm -rf "$tmp"
}

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
    if [[ -n "${MODULE_PATH:-}" && -f "${MODULE_PATH}" && "$REBUILD" == false ]]; then
        MODULE_PATH_RESOLVED="$MODULE_PATH"
        log "module: $MODULE_PATH_RESOLVED (from MODULE_PATH)"
        return
    fi
    local path="$ROOT_DIR/target/release/libvalkey_timeseries$(module_ext)"
    if [[ ! -f "$path" || "$REBUILD" == true ]]; then
        [[ "$SKIP_BUILD" == true ]] && die "module not built at $path and --skip-build was given"
        log "building the module (cargo build --release)"
        (cd "$ROOT_DIR" && cargo build --release)
    fi
    [[ -f "$path" ]] || die "module still not found at $path after building"
    MODULE_PATH_RESOLVED="$path"
    log "module: $MODULE_PATH_RESOLVED"
}

SERVER_PATH_RESOLVED=""
ensure_server() {
    local path="${VALKEY_SERVER_PATH:-$TESTS_DIR/build/binaries/$SERVER_VERSION/valkey-server}"
    if [[ ! -x "$path" ]]; then
        [[ "$SKIP_BUILD" == true ]] && die "valkey-server not found at $path and --skip-build was given"
        log "building valkey-server ($SERVER_VERSION) — this takes a few minutes"
        mkdir -p "$TESTS_DIR/build/binaries/$SERVER_VERSION"
        (
            cd "$TESTS_DIR/build"
            rm -rf valkey
            git clone "$VALKEY_REPO_URL"
            cd valkey
            git checkout "$SERVER_VERSION"
            make -j"$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)"
            cp src/valkey-server "$TESTS_DIR/build/binaries/$SERVER_VERSION/"
        )
    fi
    [[ -x "$path" ]] || die "valkey-server still not found at $path after building"
    SERVER_PATH_RESOLVED="$path"
    log "valkey-server: $SERVER_PATH_RESOLVED"
}

# ---------------------------------------------------------------------------
# server helpers (python, so we depend only on the valkey client we already need)
# ---------------------------------------------------------------------------
wait_for_ping() {
    local url="$1" timeout="${2:-30}"
    "${PY[@]}" - "$url" "$timeout" <<'PY'
import sys, time
import valkey

url, timeout = sys.argv[1], float(sys.argv[2])
deadline = time.monotonic() + timeout
while time.monotonic() < deadline:
    try:
        client = valkey.Valkey.from_url(url, socket_connect_timeout=1)
        client.ping()
        client.close()
        sys.exit(0)
    except Exception:
        time.sleep(0.2)
sys.exit(1)
PY
}

# Prints the value the parameter had *before* the call. `keep` only reads it.
# Module configs are namespaced by module name (DIV-0008), hence the `ts.` prefix.
set_compat_mode() {
    local url="$1" mode="$2"
    "${PY[@]}" - "$url" "$mode" <<'PY'
import sys
import valkey

url, mode = sys.argv[1], sys.argv[2]
param = "ts.ts-compatibility-mode"
client = valkey.Valkey.from_url(url, decode_responses=True)
current = client.execute_command("CONFIG", "GET", param)
if isinstance(current, dict):
    previous = next(iter(current.values()))
elif len(current) >= 2:
    previous = current[1]
else:
    print("", end="")
    sys.exit(3)
if mode != "keep":
    client.execute_command("CONFIG", "SET", param, mode)
print(previous)
PY
}

free_port() {
    "${PY[@]}" - <<'PY'
import socket

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind(("127.0.0.1", 0))
    print(s.getsockname()[1])
PY
}

start_reference() {
    # Hand our resolved interpreter and knobs to the shared helper, then adopt what it
    # reports back. Ownership stays visible here: REFERENCE_STARTED drives cleanup().
    COMPAT_REF_PY=("${PY[@]}")
    COMPAT_REFERENCE_URL="$REFERENCE_URL"
    COMPAT_REFERENCE_PORT="$REFERENCE_PORT"
    COMPAT_KEEP_REFERENCE="$KEEP_REFERENCE"

    compat_reference_start || die "could not provide a reference server"

    REFERENCE_URL="$COMPAT_REFERENCE_URL"
    if [[ "$COMPAT_REFERENCE_OWNED" == 1 ]]; then
        REFERENCE_STARTED=true
    fi
    log "reference: $REFERENCE_URL ($COMPAT_REFERENCE_KIND)"
}

start_subject() {
    if [[ -n "$SUBJECT_URL" ]]; then
        EXTERNAL_SUBJECT=true
        log "subject: $SUBJECT_URL (external)"
        wait_for_ping "$SUBJECT_URL" 5 || die "COMPAT_SUBJECT_URL=$SUBJECT_URL is not reachable"
        return
    fi
    ensure_module
    ensure_server

    local port workdir
    port="$(free_port)"
    workdir="$(mktemp -d "${TMPDIR:-/tmp}/compat-subject.XXXXXX")"
    SUBJECT_WORKDIR="$workdir"
    "$SERVER_PATH_RESOLVED" \
        --port "$port" \
        --dir "$workdir" \
        --logfile "$workdir/subject.log" \
        --loadmodule "$MODULE_PATH_RESOLVED" \
        --enable-debug-command yes \
        --notify-keyspace-events KEA \
        --maxmemory-policy noeviction \
        --save '' \
        --appendonly no \
        >/dev/null 2>&1 &
    SUBJECT_PID=$!
    SUBJECT_URL="redis://127.0.0.1:$port"
    if ! wait_for_ping "$SUBJECT_URL" 30; then
        tail -n 25 "$workdir/subject.log" >&2 2>/dev/null || true
        die "subject valkey-server failed to start on port $port"
    fi
    log "subject: $SUBJECT_URL (pid $SUBJECT_PID)"
}

apply_compat_mode() {
    if [[ "$COMPAT_MODE" == keep ]]; then
        local current
        current="$(set_compat_mode "$SUBJECT_URL" keep)" || die "could not read ts-compatibility-mode from the subject"
        log "subject ts-compatibility-mode left at $current"
        return
    fi
    local previous
    previous="$(set_compat_mode "$SUBJECT_URL" "$COMPAT_MODE")" \
        || die "could not set ts-compatibility-mode=$COMPAT_MODE (is the module loaded?)"
    PREVIOUS_COMPAT_MODE="$previous"
    log "subject ts-compatibility-mode: $previous -> $COMPAT_MODE"
    if [[ "$COMPAT_MODE" != strict ]]; then
        warn "the fuzzer will fail on gated intentional divergences outside strict mode"
    fi
}

# ---------------------------------------------------------------------------
# the run itself
# ---------------------------------------------------------------------------
build_pytest_argv() {
    local target
    case "$SUITE" in
        fuzz)   target="$COMPAT_DIR/test_compat_fuzz.py" ;;
        corpus) target="$COMPAT_DIR/test_compat_corpus.py" ;;
        all)    target="$COMPAT_DIR" ;;
    esac

    PYTEST_ARGV=(-m pytest "$target" --cache-clear)
    if [[ "$VERBOSE" == true ]]; then PYTEST_ARGV+=(-v); else PYTEST_ARGV+=(-q); fi

    local k=""
    [[ "$PROTOCOL" != both ]] && k="$PROTOCOL"
    if [[ -n "$FILTER" ]]; then
        k="${k:+$k and }($FILTER)"
    fi
    [[ -n "$k" ]] && PYTEST_ARGV+=(-k "$k")

    [[ -n "$SEED" ]] && PYTEST_ARGV+=("--hypothesis-seed=$SEED")
    [[ "$STATS" == true ]] && PYTEST_ARGV+=(--hypothesis-show-statistics)
    PYTEST_ARGV+=("${PYTEST_EXTRA[@]+"${PYTEST_EXTRA[@]}"}")
}

run_round() {
    local round="$1"
    log "round $round: $EXAMPLES examples/protocol, suite=$SUITE, protocol=$PROTOCOL"
    (cd "$ROOT_DIR" && "${PY[@]}" "${PYTEST_ARGV[@]}")
}

report_failure() {
    local status="$1"
    if [[ "$status" == 5 ]]; then
        warn "pytest collected nothing — check --protocol/--filter, and that hypothesis is installed"
        return
    fi
    cat >&2 <<EOF

A round failed. Before filing it as a bug:
  1. Hypothesis has shrunk the example — the minimal command sequence is in the
     failure output above.
  2. Check whether it is already registered: grep the command name in
     tests/compat/divergences.yml.
  3. If it is genuinely new, promote the shrunk sequence into
     tests/compat/corpus/ as a golden regression (see corpus/README.md).
  4. Investigate black-box only — never read RedisTimeSeries source or tests.
Conformance report: $REPORT_PATH
EOF
}

main() {
    resolve_python
    ensure_python_deps
    ensure_test_framework

    export COMPAT_FUZZ=1
    export COMPAT_FUZZ_MAX_EXAMPLES="$EXAMPLES"
    export COMPAT_REPORT_PATH="$REPORT_PATH"
    export SERVER_VERSION
    [[ -n "$DERANDOMIZE" ]] && export COMPAT_FUZZ_DERANDOMIZE=1

    build_pytest_argv

    if [[ "$DRY_RUN" == true ]]; then
        echo "python:  ${PY[*]}"
        echo "pytest:  ${PYTEST_ARGV[*]}"
        echo "env:     COMPAT_FUZZ=1 COMPAT_FUZZ_MAX_EXAMPLES=$EXAMPLES${DERANDOMIZE:+ COMPAT_FUZZ_DERANDOMIZE=1}"
        echo "subject: ${SUBJECT_URL:-<launched locally>} (ts-compatibility-mode $COMPAT_MODE)"
        echo "refer.:  ${REFERENCE_URL:-<compose reference on port $REFERENCE_PORT>}"
        if [[ -n "$DURATION_SECS" ]]; then
            echo "budget:  rounds until ${DURATION_SECS}s of wall clock elapse"
        else
            echo "budget:  $ROUNDS round(s)"
        fi
        exit 0
    fi

    start_reference
    start_subject
    apply_compat_mode

    export COMPAT_SUBJECT_URL="$SUBJECT_URL"
    export COMPAT_REFERENCE_URL="$REFERENCE_URL"
    mkdir -p "$(dirname "$REPORT_PATH")"

    local started round=0 status=0 elapsed=0
    started="$(date +%s)"
    while :; do
        round=$((round + 1))
        status=0
        run_round "$round" || status=$?
        if [[ "$status" -ne 0 ]]; then
            report_failure "$status"
            return "$status"
        fi
        elapsed=$(( $(date +%s) - started ))
        if [[ -n "$DURATION_SECS" ]]; then
            if [[ "$elapsed" -ge "$DURATION_SECS" ]]; then
                log "budget spent: $round round(s) clean in ${elapsed}s"
                break
            fi
            log "clean after ${elapsed}s of ${DURATION_SECS}s — starting another round"
        else
            [[ "$round" -ge "$ROUNDS" ]] && { log "$round round(s) clean in ${elapsed}s"; break; }
        fi
    done

    log "no unregistered divergences; report at $REPORT_PATH"
}

main "$@"
