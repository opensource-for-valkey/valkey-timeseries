#!/usr/bin/env bash

# A cancelled run can leave servers behind, so clean up before starting -- but only
# our own. The previous version killed every valkey-server and every pytest on the
# host, which is antisocial on a shared machine and fatal when two runs overlap.
#
# The match is on the module every server in this suite loads. Matching the
# test-data path instead would miss the standalone tests, whose servers are started
# with a relative logfile and a cwd, so their command line never mentions it.
kill_our_servers() {
  pkill -9 -f "valkey-server.*libvalkey_timeseries" 2>/dev/null || true
}

echo "Killing servers left over from a previous run of this suite"
kill_our_servers

# A cancelled run must not leave servers holding their ports.
trap kill_our_servers EXIT INT TERM

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

# Serial unless asked otherwise; see docs/plans/parallel-integration-tests-plan.md.
PARALLEL_WORKERS="${PARALLEL_WORKERS:-0}"

is_valid_parallel_workers() {
  [ "$1" = "auto" ] || [ "$1" = "logical" ] || [[ "$1" =~ ^[0-9]+$ ]]
}

PASSTHRU=()
while [ "$#" -gt 0 ]; do
  case "$1" in
    --parallel|-j)
      shift
      if [ "$#" -gt 0 ] && [[ "$1" != -* ]] && is_valid_parallel_workers "$1"; then
        PARALLEL_WORKERS="$1"
        shift
      else
        PARALLEL_WORKERS="auto"
      fi
      ;;
    --parallel=*|-j=*)
      PARALLEL_WORKERS="${1#*=}"
      shift
      if ! is_valid_parallel_workers "$PARALLEL_WORKERS"; then
        echo "ERROR: invalid worker count '$PARALLEL_WORKERS'; expected auto, logical or an integer." >&2
        exit 2
      fi
      ;;
    *)
      PASSTHRU+=("$1")
      shift
      ;;
  esac
done
set -- "${PASSTHRU[@]+"${PASSTHRU[@]}"}"

if ! is_valid_parallel_workers "$PARALLEL_WORKERS"; then
  echo "ERROR: invalid PARALLEL_WORKERS='$PARALLEL_WORKERS'; expected auto, logical or an integer." >&2
  exit 2
fi

resolve_workers() {
  if [ "$1" = "auto" ] || [ "$1" = "logical" ]; then
    local cores
    if [ "$(uname)" = "Darwin" ]; then
      cores=$(sysctl -n hw.ncpu)
    else
      cores=$(nproc)
    fi
    [ "$cores" -gt 32 ] && cores=32
    [ "$cores" -lt 1 ] && cores=1
    echo "$cores"
  else
    echo "$1"
  fi
}

XDIST_ARGS=()
RESOLVED_WORKERS=$(resolve_workers "$PARALLEL_WORKERS")
if [ "$RESOLVED_WORKERS" -gt 1 ]; then
  # loadscope: several classes keep per-class state under test-data/, so a class
  # must not be split across workers.
  XDIST_ARGS=(-n "$RESOLVED_WORKERS" --dist=loadscope)
  echo "Running integration tests across $RESOLVED_WORKERS workers"
fi

BUILD=${BUILD:-debug}
# If environment variable SERVER_VERSION is not set, default to "unstable"
if [ -z "$SERVER_VERSION" ]; then
    echo "SERVER_VERSION environment variable is not set. Defaulting to \"unstable\"."
    export SERVER_VERSION="unstable"
fi
PROGNAME="${BASH_SOURCE[0]}"
CWD="$(cd "$(dirname "$PROGNAME")" &>/dev/null && pwd)"
BINARY_PATH="$CWD/build/binaries/$SERVER_VERSION/valkey-server"
PORT=${PORT:-6379}
ROOT=$(cd $CWD/.. && pwd)

export MODULE_PATH="$ROOT/target/$BUILD/libvalkey_timeseries${MODULE_EXT}"


REPO_URL="https://github.com/valkey-io/valkey.git"

# Rebuild the "unstable" binary when it is older than this many days; release
# versions (e.g. 9.0.4) are immutable and are only built when missing.
UNSTABLE_MAX_AGE_DAYS=${UNSTABLE_MAX_AGE_DAYS:-7}

NEEDS_BUILD=false
if [ -f "$BINARY_PATH" ] && [ -x "$BINARY_PATH" ]; then
    echo "valkey-server binary '$BINARY_PATH' found."
    if [ "$SERVER_VERSION" = "unstable" ]; then
        if [[ "$os_type" == "Darwin" ]]; then
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
    mkdir -p "$CWD/build/binaries/$SERVER_VERSION"
    mkdir -p "$CWD/.build"
    cd "$CWD/.build"
    rm -rf valkey
    git clone "$REPO_URL"
    cd valkey
    git checkout "$SERVER_VERSION"
    make -j
    cp src/valkey-server "$CWD/build/binaries/$SERVER_VERSION/"
fi

# cd to the current directory of the script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "${DIR}"

export SOURCE_DIR=$2
echo "Running integration tests against Valkey version $SERVER_VERSION"

if [[ ! -f "${BINARY_PATH}" ]] ; then
    echo "${BINARY_PATH} missing"
    exit 1
fi

if [[ ! -z "${TEST_PATTERN}" ]] ; then
    export TEST_PATTERN="-k ${TEST_PATTERN}"
fi

PYTEST_ARGS=(--cache-clear -vvv ${TEST_FLAG:-} "${XDIST_ARGS[@]+"${XDIST_ARGS[@]}"}" ./)
if [[ ! -z "${TEST_PATTERN}" ]] ; then
    python -m pytest "${PYTEST_ARGS[@]}" ${TEST_PATTERN}
else
    python -m pytest "${PYTEST_ARGS[@]}"
fi
