#!/usr/bin/env bash

# Script to run format checks valkey-timeseries module, build it and generate .so files, run unit and integration tests.
#
# Usage: ./build.sh [clean|compat]
#
#   (no argument)  format checks, build, unit tests, integration tests
#   clean          remove build artifacts and exit
#   compat         also run the RedisTimeSeries compatibility suite (tests/compat),
#                  provisioning and starting a reference server first.
#                  Equivalent to RTS_COMPAT=1 ./build.sh
#
# See docs/rts-compat-build-integration-plan.md for the compatibility integration.

# Exit the script if any command fails. Unset variables are errors too, so every
# optional knob below is read with an explicit default.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
echo "Script Directory: $SCRIPT_DIR"

usage() {
    sed -n '3,14p' "${BASH_SOURCE[0]}" | sed -e 's/^# \{0,1\}//'
}

# Optional environment knobs, defaulted so `set -u` does not trip over them.
TEST_PATTERN="${TEST_PATTERN:-}"
ASAN_BUILD="${ASAN_BUILD:-}"
RTS_COMPAT="${RTS_COMPAT:-}"
COMPAT_REFERENCE_URL="${COMPAT_REFERENCE_URL:-}"

RUN_COMPAT=false

if [ "$#" -gt 1 ]; then
    echo "ERROR: too many arguments: $*" >&2
    usage >&2
    exit 2
fi

case "${1:-}" in
    "")
        ;;
    clean)
        echo "Cleaning build artifacts"
        rm -rf target/
        rm -rf tests/build/
        rm -rf test-data/
        echo "Clean completed."
        exit 0
        ;;
    compat)
        RUN_COMPAT=true
        ;;
    -h|--help)
        usage
        exit 0
        ;;
    *)
        echo "ERROR: unknown argument: $1" >&2
        usage >&2
        exit 2
        ;;
esac

# The compatibility suite needs a live reference server. It runs when asked for
# explicitly (`./build.sh compat`, RTS_COMPAT=1) or when an external reference is
# already pointed at (COMPAT_REFERENCE_URL).
if [ "$RTS_COMPAT" = "1" ] || [ -n "$COMPAT_REFERENCE_URL" ]; then
    RUN_COMPAT=true
fi

# ASAN + compat is rejected rather than silently degraded: the leak check below greps a
# single combined pytest output, and an ASAN-instrumented subject skews the
# timing-sensitive differential comparisons. No CI job combines them either.
if [ "$RUN_COMPAT" = true ] && [ -n "$ASAN_BUILD" ]; then
    echo "ERROR: ASAN_BUILD and the compatibility suite cannot be combined." >&2
    echo "Run './build.sh' under ASAN, and './build.sh compat' separately." >&2
    exit 2
fi

echo "Running cargo and clippy format checks..."
cargo fmt --check
cargo clippy --profile release --all-targets -- -D clippy::all

echo "Running cargo build release..."
RUSTFLAGS="-D warnings" cargo build --all --all-targets  --release

# Only run unit tests if no specific integration test is specified
if [[ -z "$TEST_PATTERN" ]]; then
  echo "Running unit tests..."
  cargo test --lib --tests --features enable-system-alloc
  echo "Running doc tests..."
  cargo test --doc --features enable-system-alloc
fi

# Ensure SERVER_VERSION environment variable is set
if [ -z "${SERVER_VERSION:-}" ]; then
    echo "ERROR: SERVER_VERSION environment variable is not set. Defaulting to unstable."
    export SERVER_VERSION="unstable"
fi

if [ "$SERVER_VERSION" != "unstable" ] && [ "$SERVER_VERSION" != "9.1" ]; then
  echo "ERROR: Unsupported version - $SERVER_VERSION"
  exit 1
fi

REPO_URL="https://github.com/valkey-io/valkey.git"
BINARY_PATH="tests/build/binaries/$SERVER_VERSION/valkey-server"

# Rebuild the "unstable" binary when it is older than this many days; release
# versions are immutable and are only built when missing.
UNSTABLE_MAX_AGE_DAYS=${UNSTABLE_MAX_AGE_DAYS:-7}

NEEDS_BUILD=false
if [ -f "$BINARY_PATH" ] && [ -x "$BINARY_PATH" ]; then
    echo "valkey-server binary '$BINARY_PATH' found."
    if [ "$SERVER_VERSION" = "unstable" ]; then
        if [ "$(uname)" = "Darwin" ]; then
            BINARY_MTIME=$(stat -f %m "$BINARY_PATH")
        else
            BINARY_MTIME=$(stat -c %Y "$BINARY_PATH")
        fi
        BINARY_AGE_DAYS=$(( ($(date +%s) - BINARY_MTIME) / 86400 ))
        if [ "$BINARY_AGE_DAYS" -ge "$UNSTABLE_MAX_AGE_DAYS" ]; then
            echo "Binary is $BINARY_AGE_DAYS days old (max $UNSTABLE_MAX_AGE_DAYS for \"unstable\"); rebuilding."
            NEEDS_BUILD=true
        fi
    fi
else
    echo "valkey-server binary '$BINARY_PATH' not found."
    NEEDS_BUILD=true
fi

if [ "$NEEDS_BUILD" = true ]; then
    mkdir -p "tests/build/binaries/$SERVER_VERSION"
    cd tests/build
    rm -rf valkey
    git clone "$REPO_URL"
    cd valkey
    git checkout "$SERVER_VERSION"
    make -j
    cp src/valkey-server ../binaries/$SERVER_VERSION/
    cd "$SCRIPT_DIR"
fi

# ASAN_BUILD only changes how pytest is invoked and what its output is scanned for — it
# instruments nothing itself. The "LeakSanitizer: detected memory leaks" line it greps for can
# only come from a valkey-server built with SANITIZER=address (see the asan-build job in
# .github/workflows/ci.yml, which instruments the server and leaves the Rust module alone: the
# module allocates through the server's allocator, so instrumenting the server covers it).
# Against an ordinary binary the grep can never match and the phase reports success having
# checked nothing, so refuse the run instead of passing vacuously.
if [ -n "$ASAN_BUILD" ]; then
    if ! grep -aq "AddressSanitizer" "$BINARY_PATH"; then
        echo "ERROR: ASAN_BUILD is set, but '$BINARY_PATH' is not ASAN-instrumented," >&2
        echo "so the leak check could not detect anything. Build an instrumented server:" >&2
        echo "  (cd tests/build/valkey && make distclean && make -j SANITIZER=address valkey-server)" >&2
        echo "  cp tests/build/valkey/src/valkey-server '$BINARY_PATH'" >&2
        exit 2
    fi
    if [ "$(uname)" = "Darwin" ]; then
        echo "ERROR: LeakSanitizer is not available on macOS, so ASAN_BUILD can not detect" >&2
        echo "leaks here (and an instrumented server wedges at startup on darwin/arm64)." >&2
        echo "Run the ASAN gate on Linux — CI's asan-build job, or a Linux container." >&2
        exit 2
    fi
fi

TEST_FRAMEWORK_REPO="https://github.com/valkey-io/valkey-test-framework"
TEST_FRAMEWORK_DIR="tests/valkeytestframework"

if [ -d "$TEST_FRAMEWORK_DIR" ]; then
    echo "valkeytestframework found."
else
    echo "Cloning valkey-test-framework..."
    git clone "$TEST_FRAMEWORK_REPO"
    mkdir -p "$TEST_FRAMEWORK_DIR"
    mv "valkey-test-framework/src"/* "$TEST_FRAMEWORK_DIR/"
    rm -rf valkey-test-framework
fi

REQUIREMENTS_FILE="requirements.txt"
USE_UV=false

# Check if uv is available
if command -v uv > /dev/null 2>&1; then
    echo "Using uv to install packages..."
    uv sync
    USE_UV=true
    # Check if pip is available
elif command -v pip > /dev/null 2>&1; then
    echo "Using pip to install packages..."
    pip install -r "$SCRIPT_DIR/$REQUIREMENTS_FILE"
# Check if pip3 is available
elif command -v pip3 > /dev/null 2>&1; then
    echo "Using pip3 to install packages..."
    pip3 install -r "$SCRIPT_DIR/$REQUIREMENTS_FILE"
else
    echo "Error: Neither uv, pip nor pip3 is available. Please install Python package installer."
    exit 1
fi

run_pytest() {
    if [ "$USE_UV" = "true" ]; then
        uv run python3 -m pytest "$@"
    else
        python3 -m pytest "$@"
    fi
}

os_type=$(uname)
MODULE_EXT=".so"
if [[ "$os_type" == "Darwin" ]]; then
  MODULE_EXT=".dylib"
elif [[ "$os_type" == "Linux" ]]; then
  MODULE_EXT=".so"
elif [[ "$os_type" == "Windows" ]]; then
  MODULE_EXT=".dll"
else
  echo "Unsupported OS type: $os_type"
  exit 1
fi

export MODULE_PATH="$SCRIPT_DIR/target/release/libvalkey_timeseries$MODULE_EXT"

# Run one pytest phase. Returns the pytest exit status, except that exit 5 ("no tests
# collected") is tolerated when TEST_PATTERN is set: a -k expression that matches the
# integration suite will usually match nothing in tests/compat, and vice versa.
run_phase() {
    local label="$1"
    shift
    local status=0

    echo "Running $label..."
    if [ -n "$ASAN_BUILD" ]; then
        set +e
        run_pytest --capture=sys --cache-clear -v "$@" 2>&1 | tee test_output.tmp
        status=${PIPESTATUS[0]}
        set -e

        # Check for memory leaks in the output
        if grep -q "LeakSanitizer: detected memory leaks" test_output.tmp; then
            RED='\033[0;31m'
            echo -e "${RED}Memory leaks detected in the following tests:"
            LEAKING_TESTS=$(grep -B 2 "LeakSanitizer: detected memory leaks" test_output.tmp | \
                            grep -v "LeakSanitizer" | \
                            grep ".*\.py::")

            LEAK_COUNT=$(echo "$LEAKING_TESTS" | wc -l)

            # Output each leaking test
            echo "$LEAKING_TESTS" | while read -r line; do
                echo "::error::Test with leak: $line"
            done

            echo -e "\n$LEAK_COUNT python integration tests have leaks detected in them"
            rm -f test_output.tmp
            return 1
        fi
        rm -f test_output.tmp
    else
        set +e
        run_pytest --cache-clear -v "$@"
        status=$?
        set -e
    fi

    if [ "$status" -eq 5 ] && [ -n "$TEST_PATTERN" ]; then
        echo "No tests matched TEST_PATTERN in $label."
        PHASES_EMPTY=$((PHASES_EMPTY + 1))
        return 0
    fi
    PHASES_RAN=$((PHASES_RAN + 1))
    return "$status"
}

# Tolerating "nothing collected" per phase must not add up to a green build that ran
# nothing at all: a typo'd TEST_PATTERN would otherwise look like success.
PHASES_RAN=0
PHASES_EMPTY=0
assert_something_ran() {
    if [ "$PHASES_RAN" -eq 0 ]; then
        echo "ERROR: TEST_PATTERN='$TEST_PATTERN' matched no tests in any phase" \
             "($PHASES_EMPTY phase(s) collected nothing)." >&2
        exit 1
    fi
}

# --- phase 1: everything except the compatibility suite ---------------------
# tests/compat is excluded here and run on its own below, so that a compat failure is
# attributable and the reference server is only up while it is actually needed.
PHASE1_ARGS=("$SCRIPT_DIR/tests/" "--ignore=$SCRIPT_DIR/tests/compat")
if [ -n "$TEST_PATTERN" ]; then
    PHASE1_ARGS+=(-k "$TEST_PATTERN")
else
    echo "TEST_PATTERN is not set. Running all integration tests."
fi
run_phase "the integration tests" "${PHASE1_ARGS[@]}"

# --- phase 2: the RedisTimeSeries compatibility suite -----------------------
if [ "$RUN_COMPAT" = true ]; then
    # The reference is provisioned, started and validated *here*, before pytest, so a
    # missing tool, a bad checksum or a pin mismatch fails the build. Delegating this to
    # conftest.py would hit its pytest.skip path and produce a green build that tested
    # nothing.
    if [ "$USE_UV" = "true" ]; then
        COMPAT_REF_PY=(uv run python3)
    else
        COMPAT_REF_PY=(python3)
    fi
    # shellcheck source=tests/reference_server.sh
    . "$SCRIPT_DIR/tests/reference_server.sh"

    cleanup() {
        local status=$?
        set +e
        compat_reference_stop
        exit "$status"
    }
    trap cleanup EXIT INT TERM

    if ! compat_reference_start; then
        echo "ERROR: could not provide a compatibility reference server." >&2
        exit 1
    fi
    export COMPAT_REFERENCE_URL

    # conftest.py only reaches its skip-on-Docker-failure branch when
    # COMPAT_REFERENCE_URL is unset; we always set it, so make the intent explicit.
    unset RTS_COMPAT

    # tests/compat/test_compat_replication.py needs Docker for the reference replica
    # even when the primary is external. Without this, an unavailable replica would be
    # a silent skip rather than a failure.
    export COMPAT_STRICT_SKIPS=1

    PHASE2_ARGS=("$SCRIPT_DIR/tests/compat" -rs)
    if [ -n "$TEST_PATTERN" ]; then
        PHASE2_ARGS+=(-k "$TEST_PATTERN")
    fi
    run_phase "the compatibility tests" "${PHASE2_ARGS[@]}"

    compat_reference_stop
fi

assert_something_ran

if [ "$RUN_COMPAT" = true ]; then
    echo "Build, Format Checks, Unit tests, Integration Tests and Compatibility Tests succeeded"
else
    echo "Build, Format Checks, Unit tests, and Integration Tests succeeded"
fi
