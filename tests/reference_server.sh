#!/usr/bin/env bash
#
# reference_server.sh — provision, start and stop the RedisTimeSeries compatibility
# reference server. Sourceable; used by build.sh and run-fuzz.sh.
#
# See docs/rts-compat-build-integration-plan.md. The digest-pinned redis:8.8 image in
# docker-compose.compat.yml is the canonical reference artifact (§3); the native binary
# is a secondary artifact that must track it and is only chosen automatically once an
# equivalence run has been recorded for the pin (§3.1).
#
# Clean-room rule: the reference is a black-box target. Only prebuilt Redis Ltd.
# binaries are fetched here — never RedisTimeSeries source. See tests/compat/README.md.
#
# ---------------------------------------------------------------------------
# Contract
#
#   compat_reference_provision   Ensure the native binary cache is populated. No
#                                process is started. Only meaningful in binary mode.
#   compat_reference_start       Start the reference if we need to. On success sets:
#                                  COMPAT_REFERENCE_URL     connection URL
#                                  COMPAT_REFERENCE_KIND    external|binary|docker
#                                  COMPAT_REFERENCE_OWNED   1 if we started it, else 0
#                                  COMPAT_REFERENCE_PID     binary mode only
#   compat_reference_stop        No-op unless COMPAT_REFERENCE_OWNED=1.
#
# Every function prints its own diagnostics and returns non-zero on failure; none of
# them call `exit` and none of them install a trap. Teardown policy belongs to the
# caller, which owns its own trap and decides what to do with COMPAT_REFERENCE_OWNED.
#
# Optional caller-supplied input:
#   COMPAT_REF_PY   bash array holding the python interpreter to use
#                   (e.g. COMPAT_REF_PY=(uv run python3)). Defaults to (python3).
# ---------------------------------------------------------------------------

# Resolved once, from this file's location, so the caller's cwd is irrelevant.
_COMPAT_REF_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
_COMPAT_REF_COMPOSE_FILE="$_COMPAT_REF_ROOT/docker-compose.compat.yml"

# --- pin -------------------------------------------------------------------
# Must track the image pinned in docker-compose.compat.yml. Bump both together, in
# one reviewed change recorded in docs/rts-reference-bumps.md.
COMPAT_REFERENCE_VERSION="${COMPAT_REFERENCE_VERSION:-8.8.0}"
COMPAT_REFERENCE_MODULE_VERSION="${COMPAT_REFERENCE_MODULE_VERSION:-80800}"

# The pin for which a §3.1 equivalence run (native binary vs. the pinned image) has
# been recorded in docs/rts-reference-bumps.md. While this does not match
# COMPAT_REFERENCE_VERSION, `auto` never selects binary mode — it is not yet known
# that the two artifacts behave identically.
_COMPAT_REF_EQUIVALENCE_VERIFIED_PIN=""

# --- knobs -----------------------------------------------------------------
COMPAT_REFERENCE_MODE="${COMPAT_REFERENCE_MODE:-auto}"     # auto|binary|docker
COMPAT_REFERENCE_URL="${COMPAT_REFERENCE_URL:-}"
COMPAT_REFERENCE_PORT="${COMPAT_REFERENCE_PORT:-}"         # empty: ephemeral (binary), 16379 (docker)
COMPAT_KEEP_REFERENCE="${COMPAT_KEEP_REFERENCE:-}"
COMPAT_REFERENCE_DEB_URL="${COMPAT_REFERENCE_DEB_URL:-}"
COMPAT_REFERENCE_DEB_SHA256="${COMPAT_REFERENCE_DEB_SHA256:-}"

# --- outputs ---------------------------------------------------------------
COMPAT_REFERENCE_KIND=""
COMPAT_REFERENCE_OWNED=0
COMPAT_REFERENCE_PID=""
COMPAT_REFERENCE_WORKDIR=""
_COMPAT_REF_CACHE_DIR=""

if ! declare -p COMPAT_REF_PY >/dev/null 2>&1; then
    COMPAT_REF_PY=(python3)
fi

_compat_ref_log()  { printf '\033[1;34m==>\033[0m reference: %s\n' "$*" >&2; }
_compat_ref_warn() { printf '\033[1;33mwarn:\033[0m reference: %s\n' "$*" >&2; }
_compat_ref_err()  { printf '\033[1;31merror:\033[0m reference: %s\n' "$*" >&2; }

# ---------------------------------------------------------------------------
# supported distro/arch matrix
#
# SHA256s read from the packages.redis.io `Packages` index on 2026-07-28. Note that
# the index also carries a `redis-server-dbgsym` stanza per dist/arch: these come from
# the exact `Filename:` entries, not a `Package: redis-server` prefix match.
#
# Status is either `verified` (a §3.1 equivalence run against the pinned image has been
# recorded for this tuple) or `candidate` (downloadable and believed correct, but not
# yet proven equivalent). `auto` only ever selects a `verified` tuple; an explicit
# COMPAT_REFERENCE_MODE=binary also accepts a `candidate` with a warning — that is how
# the first equivalence run for a tuple gets performed.
# ---------------------------------------------------------------------------
_compat_ref_matrix_entry() {  # $1=distro $2=arch -> "<sha256> <status>"
    case "$COMPAT_REFERENCE_VERSION/$1/$2" in
        8.8.0/jammy/amd64)    echo "af2888d300b44e5e803ba6ba5e42cd23e82f79c2888ead037b69be5ee9137deb candidate" ;;
        8.8.0/jammy/arm64)    echo "75d85bb7d44caf6216bfe96491757e4e3f0f63a5869810d92a75ab2d6aee450e candidate" ;;
        8.8.0/noble/amd64)    echo "8c373ccf6b981919260ffb4d8a10df591a5d413c2dfe421ba725a9e690fd66ad candidate" ;;
        8.8.0/noble/arm64)    echo "4b7d14d12f1d8b99acb019a326e9d43dcdbb2aa3d18dcde7243db3391c3d3da0 candidate" ;;
        8.8.0/bookworm/amd64) echo "3c9d7f38aacd8c26bce0f5f56553cf77136f70e92c3c65b17f2bdcd8200f9085 candidate" ;;
        8.8.0/bookworm/arm64) echo "bd016ee0546f5e96031c238b48cc71346de31db3fb53653c8ac97084c4a35cbd candidate" ;;
        *) return 1 ;;
    esac
}

# Never guess a distribution: an unrecognized one resolves to nothing, and the caller
# falls back to Docker or fails. A jammy .deb carries ABI assumptions that need not
# hold elsewhere.
_compat_ref_detect_distro() {
    local id="" codename=""
    [ -r /etc/os-release ] || return 1
    # shellcheck disable=SC1091
    id="$(. /etc/os-release && printf '%s' "${ID:-}")"
    codename="$(. /etc/os-release && printf '%s' "${VERSION_CODENAME:-}")"
    case "$id/$codename" in
        ubuntu/jammy|ubuntu/noble|debian/bookworm) printf '%s' "$codename" ;;
        *) return 1 ;;
    esac
}

_compat_ref_detect_arch() {
    case "$(uname -m)" in
        x86_64|amd64)  printf 'amd64' ;;
        aarch64|arm64) printf 'arm64' ;;
        *) return 1 ;;
    esac
}

_compat_ref_deb_url() {  # $1=distro $2=arch
    printf 'https://packages.redis.io/deb/pool/%s/r/re/redis-server_%s-1rl1~%s1_%s.deb' \
        "$1" "$COMPAT_REFERENCE_VERSION" "$1" "$2"
}

_compat_ref_sha256() {  # $1=file
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$1" | awk '{print $1}'
    else
        _compat_ref_err "no sha256sum or shasum available to verify the download"
        return 1
    fi
}

# ---------------------------------------------------------------------------
# provisioning
# ---------------------------------------------------------------------------

# Cache reuse requires the server binary (executable), the module (a *readable regular
# file* — it is dlopen'd, not exec'd, so it need not carry the execute bit) and a
# manifest describing exactly the artifact we are asking for. A manifest left by a
# previous pin invalidates the cache.
_compat_ref_cache_valid() {  # $1=cache_dir $2=url $3=sha256 $4=distro $5=arch
    local dir="$1"
    [ -f "$dir/redis-server" ] && [ -x "$dir/redis-server" ] || return 1
    [ -f "$dir/redistimeseries.so" ] && [ -r "$dir/redistimeseries.so" ] || return 1
    [ -f "$dir/manifest.json" ] || return 1
    "${COMPAT_REF_PY[@]}" - "$dir/manifest.json" "$2" "$3" "$COMPAT_REFERENCE_VERSION" \
        "$4" "$5" "$COMPAT_REFERENCE_MODULE_VERSION" <<'PY'
import json, sys

path, url, sha, version, distro, arch, module_version = sys.argv[1:8]
try:
    with open(path) as f:
        m = json.load(f)
except Exception:
    sys.exit(1)

ok = (
    m.get("url") == url
    and m.get("sha256") == sha
    and m.get("version") == version
    and m.get("distro") == distro
    and m.get("arch") == arch
    and str(m.get("module", {}).get("version")) == module_version
)
sys.exit(0 if ok else 1)
PY
}

_compat_ref_write_manifest() {  # $1=cache_dir $2=url $3=sha256 $4=distro $5=arch $6=server_version
    "${COMPAT_REF_PY[@]}" - "$1/manifest.json" "$2" "$3" "$COMPAT_REFERENCE_VERSION" \
        "$4" "$5" "$6" "$COMPAT_REFERENCE_MODULE_VERSION" <<'PY'
import datetime, json, sys

path, url, sha, version, distro, arch, server_version, module_version = sys.argv[1:9]
manifest = {
    "url": url,
    "sha256": sha,
    "version": version,
    "distro": distro,
    "arch": arch,
    "server_version": server_version,
    "module": {"name": "timeseries", "version": int(module_version)},
    "provisioned_at": datetime.datetime.now(datetime.timezone.utc)
    .replace(microsecond=0)
    .isoformat()
    .replace("+00:00", "Z"),
}
with open(path, "w") as f:
    json.dump(manifest, f, indent=2)
    f.write("\n")
PY
}

_compat_ref_unpack_deb() {  # $1=deb $2=dest_dir
    local deb="$1" dest="$2" tmp member
    tmp="$(mktemp -d "${TMPDIR:-/tmp}/compat-ref-unpack.XXXXXX")" || return 1

    if ! (cd "$tmp" && ar x "$deb" 2>/dev/null); then
        _compat_ref_err "could not unpack $deb with ar"
        rm -rf "$tmp"
        return 1
    fi

    member="$(ls "$tmp" | grep '^data\.tar\.' | head -1)"
    if [ -z "$member" ]; then
        _compat_ref_err "no data.tar.* member in $deb"
        rm -rf "$tmp"
        return 1
    fi

    local ok=1
    case "$member" in
        *.zst)
            if tar --zstd -xf "$tmp/$member" -C "$tmp" \
                ./usr/bin/redis-server ./usr/lib/redis/modules/redistimeseries.so 2>/dev/null; then
                ok=0
            elif command -v zstd >/dev/null 2>&1 &&
                zstd -dc "$tmp/$member" | tar -xf - -C "$tmp" \
                    ./usr/bin/redis-server ./usr/lib/redis/modules/redistimeseries.so 2>/dev/null; then
                ok=0
            fi
            ;;
        *.xz|*.gz)
            tar -xf "$tmp/$member" -C "$tmp" \
                ./usr/bin/redis-server ./usr/lib/redis/modules/redistimeseries.so 2>/dev/null && ok=0
            ;;
    esac

    if [ "$ok" -ne 0 ]; then
        _compat_ref_err "could not extract redis-server and redistimeseries.so from $member"
        _compat_ref_err "(needs GNU tar with --zstd, or the zstd binary)"
        rm -rf "$tmp"
        return 1
    fi

    mkdir -p "$dest"
    cp "$tmp/usr/bin/redis-server" "$dest/redis-server"
    cp "$tmp/usr/lib/redis/modules/redistimeseries.so" "$dest/redistimeseries.so"
    chmod +x "$dest/redis-server"
    chmod u+r "$dest/redistimeseries.so"
    rm -rf "$tmp"
}

# Populate the binary cache. Sets _COMPAT_REF_CACHE_DIR on success.
compat_reference_provision() {
    local distro arch entry sha status url

    if [ "$(uname)" != "Linux" ]; then
        _compat_ref_err "the native reference binary is Linux-only (this is $(uname));"
        _compat_ref_err "no prebuilt RedisTimeSeries $COMPAT_REFERENCE_VERSION exists for this platform"
        return 1
    fi

    if ! arch="$(_compat_ref_detect_arch)"; then
        _compat_ref_err "unsupported architecture: $(uname -m)"
        return 1
    fi

    if [ -n "$COMPAT_REFERENCE_DEB_URL" ] || [ -n "$COMPAT_REFERENCE_DEB_SHA256" ]; then
        if [ -z "$COMPAT_REFERENCE_DEB_URL" ] || [ -z "$COMPAT_REFERENCE_DEB_SHA256" ]; then
            _compat_ref_err "COMPAT_REFERENCE_DEB_URL and COMPAT_REFERENCE_DEB_SHA256 must be set together"
            return 1
        fi
        url="$COMPAT_REFERENCE_DEB_URL"
        sha="$COMPAT_REFERENCE_DEB_SHA256"
        distro="override"
        _compat_ref_warn "using the explicit deb override; this artifact is unverified against the pinned image"
    else
        if ! distro="$(_compat_ref_detect_distro)"; then
            _compat_ref_err "unrecognized or untested Linux distribution (see /etc/os-release)."
            _compat_ref_err "Supported: ubuntu jammy, ubuntu noble, debian bookworm."
            _compat_ref_err "Set COMPAT_REFERENCE_DEB_URL + COMPAT_REFERENCE_DEB_SHA256 to override"
            _compat_ref_err "deliberately, or use COMPAT_REFERENCE_MODE=docker / COMPAT_REFERENCE_URL."
            return 1
        fi
        if ! entry="$(_compat_ref_matrix_entry "$distro" "$arch")"; then
            _compat_ref_err "no pinned deb for $distro/$arch at version $COMPAT_REFERENCE_VERSION"
            return 1
        fi
        sha="${entry%% *}"
        status="${entry##* }"
        url="$(_compat_ref_deb_url "$distro" "$arch")"
        if [ "$status" != "verified" ]; then
            _compat_ref_warn "$distro/$arch is a *candidate* tuple: no equivalence run against the"
            _compat_ref_warn "pinned image is recorded yet (plan §3.1). Results are indicative only."
        fi
    fi

    _COMPAT_REF_CACHE_DIR="$_COMPAT_REF_ROOT/tests/build/binaries/reference/$COMPAT_REFERENCE_VERSION"

    if _compat_ref_cache_valid "$_COMPAT_REF_CACHE_DIR" "$url" "$sha" "$distro" "$arch"; then
        _compat_ref_log "cached binary at $_COMPAT_REF_CACHE_DIR"
        return 0
    fi

    local tool
    for tool in curl ar tar; do
        command -v "$tool" >/dev/null 2>&1 || {
            _compat_ref_err "$tool not found; it is needed to fetch and unpack the reference binary"
            return 1
        }
    done

    local tmpdeb actual
    tmpdeb="$(mktemp "${TMPDIR:-/tmp}/compat-ref.XXXXXX.deb")" || return 1
    _compat_ref_log "downloading $url"
    if ! curl -fsSL --retry 3 --retry-delay 2 -o "$tmpdeb" "$url"; then
        _compat_ref_err "download failed: $url"
        rm -f "$tmpdeb"
        return 1
    fi

    actual="$(_compat_ref_sha256 "$tmpdeb")" || { rm -f "$tmpdeb"; return 1; }
    if [ "$actual" != "$sha" ]; then
        _compat_ref_err "checksum mismatch for $url"
        _compat_ref_err "  expected $sha"
        _compat_ref_err "  actual   $actual"
        rm -f "$tmpdeb"
        return 1
    fi

    # The manifest is written only after the server has started and validated
    # (compat_reference_start), so a half-provisioned cache is never reused.
    rm -f "$_COMPAT_REF_CACHE_DIR/manifest.json"
    if ! _compat_ref_unpack_deb "$tmpdeb" "$_COMPAT_REF_CACHE_DIR"; then
        rm -f "$tmpdeb"
        return 1
    fi
    rm -f "$tmpdeb"

    _COMPAT_REF_PENDING_URL="$url"
    _COMPAT_REF_PENDING_SHA="$sha"
    _COMPAT_REF_PENDING_DISTRO="$distro"
    _COMPAT_REF_PENDING_ARCH="$arch"
    _compat_ref_log "provisioned $COMPAT_REFERENCE_VERSION ($distro/$arch) into $_COMPAT_REF_CACHE_DIR"
}

# ---------------------------------------------------------------------------
# health check / pin-drift guard
# ---------------------------------------------------------------------------

_compat_ref_wait_ping() {  # $1=url $2=timeout_s
    "${COMPAT_REF_PY[@]}" - "$1" "$2" <<'PY'
import sys, time
import valkey

url, timeout = sys.argv[1], float(sys.argv[2])
deadline = time.monotonic() + timeout
while time.monotonic() < deadline:
    try:
        c = valkey.Valkey.from_url(url, socket_connect_timeout=1)
        c.ping()
        c.close()
        sys.exit(0)
    except Exception:
        time.sleep(0.2)
sys.exit(1)
PY
}

# A silent upstream change would quietly reinterpret every version-named claim in
# divergences.yml and info-fields-8.8.yml, so a mismatch is fatal, not a warning.
_compat_ref_validate_pin() {  # $1=url
    "${COMPAT_REF_PY[@]}" - "$1" "$COMPAT_REFERENCE_VERSION" "$COMPAT_REFERENCE_MODULE_VERSION" <<'PY'
import sys
import valkey

url, want_server, want_module = sys.argv[1], sys.argv[2], int(sys.argv[3])
c = valkey.Valkey.from_url(url)


def _text(v):
    return v.decode() if isinstance(v, bytes) else v


def _modules(reply):
    """MODULE LIST comes back as a list of dicts or, on RESP2, a list of flat
    [name, <name>, ver, <ver>, ...] sequences depending on the client."""
    out = {}
    for entry in reply:
        if isinstance(entry, dict):
            fields = {_text(k): v for k, v in entry.items()}
        else:
            seq = list(entry)
            fields = {_text(k): v for k, v in zip(seq[::2], seq[1::2])}
        name = _text(fields.get("name"))
        if name is not None:
            out[name] = fields.get("ver")
    return out


server = _text(c.info("server").get("redis_version"))
modules = _modules(c.execute_command("MODULE", "LIST"))

problems = []
if server != want_server:
    problems.append(f"server version {server!r}, expected {want_server!r}")
got_module = modules.get("timeseries")
if got_module != want_module:
    problems.append(f"timeseries module {got_module!r}, expected {want_module!r}")

if problems:
    print("reference pin mismatch: " + "; ".join(problems), file=sys.stderr)
    print(
        "The registry claims in tests/compat/divergences.yml and info-fields-8.8.yml\n"
        "were probed against the pinned reference; re-probe them before moving the pin\n"
        "(docs/rts-reference-bumps.md).",
        file=sys.stderr,
    )
    sys.exit(1)
print(f"reference validated: redis {server}, timeseries {got_module}")
PY
}

_compat_ref_free_port() {
    "${COMPAT_REF_PY[@]}" - <<'PY'
import socket

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind(("127.0.0.1", 0))
    print(s.getsockname()[1])
PY
}

_compat_ref_port_free() {  # $1=port
    "${COMPAT_REF_PY[@]}" - "$1" <<'PY'
import socket, sys

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    try:
        s.bind(("127.0.0.1", int(sys.argv[1])))
    except OSError:
        sys.exit(1)
sys.exit(0)
PY
}

# ---------------------------------------------------------------------------
# start / stop
# ---------------------------------------------------------------------------

# Which mode to use when nothing was requested explicitly. Binary is only chosen when
# an equivalence run has been recorded for this exact pin (plan §3.1); until then the
# canonical Docker artifact is the only default.
_compat_ref_resolve_mode() {
    case "$COMPAT_REFERENCE_MODE" in
        docker|binary) printf '%s' "$COMPAT_REFERENCE_MODE" ;;
        auto)
            if [ "$(uname)" = "Linux" ] &&
               [ -n "$_COMPAT_REF_EQUIVALENCE_VERIFIED_PIN" ] &&
               [ "$_COMPAT_REF_EQUIVALENCE_VERIFIED_PIN" = "$COMPAT_REFERENCE_VERSION" ]; then
                printf 'binary'
            else
                printf 'docker'
            fi
            ;;
        *)
            _compat_ref_err "COMPAT_REFERENCE_MODE must be auto, binary or docker (got: $COMPAT_REFERENCE_MODE)"
            return 1
            ;;
    esac
}

_compat_ref_start_binary() {
    local port

    compat_reference_provision || return 1

    if [ -n "$COMPAT_REFERENCE_PORT" ]; then
        port="$COMPAT_REFERENCE_PORT"
        if ! _compat_ref_port_free "$port"; then
            _compat_ref_err "COMPAT_REFERENCE_PORT=$port is already in use"
            return 1
        fi
    else
        port="$(_compat_ref_free_port)" || return 1
    fi

    COMPAT_REFERENCE_WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/compat-reference.XXXXXX")" || return 1
    _compat_ref_log "starting $_COMPAT_REF_CACHE_DIR/redis-server on port $port"

    # Same flags as the compose reference service, so the two modes are configured
    # identically (docker-compose.compat.yml).
    "$_COMPAT_REF_CACHE_DIR/redis-server" \
        --port "$port" \
        --dir "$COMPAT_REFERENCE_WORKDIR" \
        --logfile "$COMPAT_REFERENCE_WORKDIR/reference.log" \
        --loadmodule "$_COMPAT_REF_CACHE_DIR/redistimeseries.so" \
        --enable-debug-command yes \
        --notify-keyspace-events KEA \
        --maxmemory-policy noeviction \
        --save "" \
        --appendonly no &
    COMPAT_REFERENCE_PID=$!
    COMPAT_REFERENCE_OWNED=1
    COMPAT_REFERENCE_KIND="binary"
    COMPAT_REFERENCE_URL="redis://127.0.0.1:$port"

    if ! _compat_ref_wait_ping "$COMPAT_REFERENCE_URL" 30; then
        _compat_ref_err "reference server never answered on $COMPAT_REFERENCE_URL"
        if [ -f "$COMPAT_REFERENCE_WORKDIR/reference.log" ]; then
            tail -25 "$COMPAT_REFERENCE_WORKDIR/reference.log" >&2
        fi
        return 1
    fi

    _compat_ref_validate_pin "$COMPAT_REFERENCE_URL" || return 1

    if [ -n "${_COMPAT_REF_PENDING_URL:-}" ]; then
        _compat_ref_write_manifest "$_COMPAT_REF_CACHE_DIR" "$_COMPAT_REF_PENDING_URL" \
            "$_COMPAT_REF_PENDING_SHA" "$_COMPAT_REF_PENDING_DISTRO" \
            "$_COMPAT_REF_PENDING_ARCH" "$COMPAT_REFERENCE_VERSION"
        unset _COMPAT_REF_PENDING_URL
    fi
}

_compat_ref_start_docker() {
    local port already=""

    command -v docker >/dev/null 2>&1 || {
        _compat_ref_err "docker not found. Start a reference yourself and set COMPAT_REFERENCE_URL,"
        _compat_ref_err "or use COMPAT_REFERENCE_MODE=binary on a supported Linux host."
        return 1
    }

    port="${COMPAT_REFERENCE_PORT:-16379}"

    # A container someone else started stays up afterwards; only stop what we start.
    already="$(docker compose -f "$_COMPAT_REF_COMPOSE_FILE" ps -q reference 2>/dev/null || true)"

    _compat_ref_log "starting the pinned reference container on port $port"
    if ! COMPAT_REFERENCE_PORT="$port" \
        docker compose -f "$_COMPAT_REF_COMPOSE_FILE" up -d --wait reference; then
        _compat_ref_err "could not start the reference container (is Docker running?)"
        return 1
    fi

    COMPAT_REFERENCE_KIND="docker"
    if [ -n "$already" ]; then
        _compat_ref_log "container was already running — leaving it up on exit"
        COMPAT_REFERENCE_OWNED=0
    else
        COMPAT_REFERENCE_OWNED=1
    fi
    COMPAT_REFERENCE_URL="redis://127.0.0.1:$port"

    if ! _compat_ref_wait_ping "$COMPAT_REFERENCE_URL" 30; then
        _compat_ref_err "container started but $COMPAT_REFERENCE_URL never answered"
        return 1
    fi

    _compat_ref_validate_pin "$COMPAT_REFERENCE_URL" || return 1
}

compat_reference_start() {
    local mode

    # Defaulted rather than assumed: `VAR=x . reference_server.sh` puts the assignment
    # in a temporary scope that bash discards when the builtin returns, leaving the
    # variable unset again despite the default applied at source time.
    COMPAT_REFERENCE_URL="${COMPAT_REFERENCE_URL:-}"

    if [ -n "$COMPAT_REFERENCE_URL" ]; then
        COMPAT_REFERENCE_KIND="external"
        COMPAT_REFERENCE_OWNED=0
        _compat_ref_log "using external reference $COMPAT_REFERENCE_URL"
        if ! _compat_ref_wait_ping "$COMPAT_REFERENCE_URL" 5; then
            _compat_ref_err "COMPAT_REFERENCE_URL=$COMPAT_REFERENCE_URL is not reachable"
            return 1
        fi
        _compat_ref_validate_pin "$COMPAT_REFERENCE_URL" || return 1
        return 0
    fi

    mode="$(_compat_ref_resolve_mode)" || return 1
    case "$mode" in
        binary) _compat_ref_start_binary ;;
        docker) _compat_ref_start_docker ;;
    esac
}

compat_reference_stop() {
    [ "$COMPAT_REFERENCE_OWNED" = "1" ] || return 0

    if [ -n "$COMPAT_KEEP_REFERENCE" ]; then
        _compat_ref_log "COMPAT_KEEP_REFERENCE set — leaving the reference running at $COMPAT_REFERENCE_URL"
        return 0
    fi

    case "$COMPAT_REFERENCE_KIND" in
        binary)
            if [ -n "$COMPAT_REFERENCE_PID" ] && kill -0 "$COMPAT_REFERENCE_PID" 2>/dev/null; then
                _compat_ref_log "stopping reference server (pid $COMPAT_REFERENCE_PID)"
                kill "$COMPAT_REFERENCE_PID" 2>/dev/null
                wait "$COMPAT_REFERENCE_PID" 2>/dev/null
            fi
            if [ -n "$COMPAT_REFERENCE_WORKDIR" ]; then
                rm -rf "$COMPAT_REFERENCE_WORKDIR"
            fi
            ;;
        docker)
            _compat_ref_log "stopping reference container"
            docker compose -f "$_COMPAT_REF_COMPOSE_FILE" stop reference >/dev/null 2>&1
            ;;
    esac

    COMPAT_REFERENCE_OWNED=0
    return 0
}
