#!/usr/bin/env bash
# docker-entrypoint.sh
# Runtime entrypoint for the valkey-timeseries Docker image.
#
# Configures Valkey at startup via environment variables, then execs
# valkey-server. Supports both standalone and cluster mode.
#
# Environment variables:
#   VALKEY_LOADMODULE       Path to the timeseries module .so
#                           (default: /usr/local/lib/libvalkey_timeseries.so)
#   VALKEY_PORT             Override the listen port (default: 6379)
#   VALKEY_BIND             Override the bind address (default: 0.0.0.0)
#   VALKEY_CLUSTER_ENABLED  Set to "yes" to enable cluster mode
#   VALKEY_CLUSTER_CONFIG   Cluster config file path
#                           (default: /data/nodes.conf)
#   VALKEY_CLUSTER_TIMEOUT  Cluster node timeout in ms (default: 5000)
#   VALKEY_LOGLEVEL         Set log level (default: notice)
#   VALKEY_EXTRA_ARGS       Additional arguments passed directly to
#                           valkey-server
#   VALKEY_APPENDONLY       Set to "yes" to enable AOF persistence
#   VALKEY_SAVE             Set to a save directive like "900 1" for
#                           RDB snapshots; empty disables persistence
#   VALKEY_REQUIREPASS      Set a requirepass password

set -e

# ── Defaults ────────────────────────────────────────────────────────
LOADMODULE="${VALKEY_LOADMODULE:-/usr/local/lib/libvalkey_timeseries.so}"
PORT="${VALKEY_PORT:-6379}"
BIND="${VALKEY_BIND:-0.0.0.0}"
LOGLEVEL="${VALKEY_LOGLEVEL:-notice}"
CLUSTER_CONFIG="${VALKEY_CLUSTER_CONFIG:-/data/nodes.conf}"
CLUSTER_TIMEOUT="${VALKEY_CLUSTER_TIMEOUT:-5000}"

# ── Build runtime config ────────────────────────────────────────────
CONFIG_DIR="${VALKEY_CONFIG_DIR:-/usr/local/etc/valkey}"
CONFIG_FILE="${CONFIG_DIR}/valkey.conf"

# Start with the packaged config, then append runtime overrides
cat "$CONFIG_FILE" > /tmp/valkey_runtime.conf

# Module path — ensure the module is referenced
if ! grep -q "^loadmodule" /tmp/valkey_runtime.conf; then
    echo "loadmodule ${LOADMODULE}" >> /tmp/valkey_runtime.conf
fi

# Override key settings via env vars
{
    echo ""
    echo "# ── Runtime overrides (docker-entrypoint.sh) ──"
    echo "port ${PORT}"
    echo "bind ${BIND}"
    echo "loglevel ${LOGLEVEL}"

    # Persistence
    if [ "${VALKEY_APPENDONLY}" = "yes" ]; then
        echo "appendonly yes"
    fi
    if [ -n "${VALKEY_SAVE}" ]; then
        echo "save ${VALKEY_SAVE}"
    else
        echo 'save ""'
    fi
    if [ -n "${VALKEY_REQUIREPASS}" ]; then
        echo "requirepass ${VALKEY_REQUIREPASS}"
    fi

    # Cluster mode
    if [ "${VALKEY_CLUSTER_ENABLED}" = "yes" ]; then
        echo "cluster-enabled yes"
        echo "cluster-config-file ${CLUSTER_CONFIG}"
        echo "cluster-node-timeout ${CLUSTER_TIMEOUT}"
    fi
} >> /tmp/valkey_runtime.conf

# ── Handle first argument ───────────────────────────────────────────
# If the first argument starts with '-', prepend valkey-server
# If the first argument is 'valkey-server', use it with our config
# Otherwise, exec whatever command was passed
if [ "${1#-}" != "$1" ]; then
    set -- valkey-server /tmp/valkey_runtime.conf "$@"
elif [ "$1" = "valkey-server" ]; then
    shift
    set -- valkey-server /tmp/valkey_runtime.conf "$@"
fi

# ── Allow the container to be started with `--user` ─────────────────
if [ "$1" = "valkey-server" ] && [ "$(id -u)" = "0" ]; then
    # Running as root; fix permissions on data dir if it exists
    mkdir -p /data
    chown -R valkey:valkey /data /tmp/valkey_runtime.conf 2>/dev/null || true
    if command -v gosu >/dev/null 2>&1; then
        exec gosu valkey "$@" ${VALKEY_EXTRA_ARGS}
    else
        echo "WARNING: gosu not found, running as root" >&2
        exec "$@" ${VALKEY_EXTRA_ARGS}
    fi
fi

exec "$@" ${VALKEY_EXTRA_ARGS}
