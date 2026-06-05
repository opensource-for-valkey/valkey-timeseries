#!/usr/bin/env bash
# build-docker.sh
# Helper script to build Docker images for valkey-timeseries with
# different configurations.
#
# Usage:
#   ./scripts/build-docker.sh [variant] [version] [features]
#
#   variant:  prod (default)  - multi-stage build on official valkey/valkey
#             source          - full source build (Valkey + module)
#   version:  8.1 (default)   - Valkey 8.1
#             8.0             - Valkey 8.0
#             unstable        - Valkey main branch (source build only)
#   features: (optional)      - Cargo features, e.g. "valkey_8_0"
#
# Examples:
#   ./scripts/build-docker.sh                        # prod, 8.1
#   ./scripts/build-docker.sh prod 8.0 valkey_8_0    # prod, 8.0, with valkey_8_0 feature
#   ./scripts/build-docker.sh source unstable        # full source build from main

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# ── Parse arguments ─────────────────────────────────────────────────
VARIANT="${1:-prod}"
VERSION="${2:-8.1}"
FEATURES="${3:-}"

# ── Validate ────────────────────────────────────────────────────────
case "$VARIANT" in
    prod|source) ;;
    *)
        echo "ERROR: Unknown variant '$VARIANT'. Use 'prod' or 'source'."
        exit 1
        ;;
esac

case "$VERSION" in
    8.0|8.1|unstable) ;;
    *)
        echo "ERROR: Unsupported version '$VERSION'. Use '8.0', '8.1', or 'unstable'."
        exit 1
        ;;
esac

# ── Build ───────────────────────────────────────────────────────────
echo "========================================"
echo "Building valkey-timeseries Docker image"
echo "  Variant : $VARIANT"
echo "  Version : $VERSION"
echo "  Features: ${FEATURES:-<none>}"
echo "========================================"

cd "$PROJECT_DIR"

BUILD_ARGS=()
TAG_BASE="valkey-timeseries"

if [ "$VARIANT" = "prod" ]; then
    DOCKERFILE="Dockerfile"
    TAG="${TAG_BASE}:${VERSION}"
    BUILD_ARGS+=(--build-arg "VALKEY_VERSION=${VERSION}")

    # Also tag as latest if building default version
    if [ "$VERSION" = "8.1" ]; then
        BUILD_ARGS+=(-t "${TAG_BASE}:latest")
    fi
else
    DOCKERFILE="Dockerfile.source"
    TAG="${TAG_BASE}:source-${VERSION}"
    BUILD_ARGS+=(--build-arg "SERVER_VERSION=${VERSION}")

    if [ "$VERSION" = "8.1" ]; then
        BUILD_ARGS+=(-t "${TAG_BASE}:source")
    fi
fi

if [ -n "$FEATURES" ]; then
    BUILD_ARGS+=(--build-arg "FEATURES=${FEATURES}")
    TAG="${TAG}-${FEATURES//,/-}"
fi

BUILD_ARGS+=(-t "$TAG")
BUILD_ARGS+=(-f "$DOCKERFILE")

echo ""
echo "Running: docker build ${BUILD_ARGS[*]} ."
echo ""

docker build "${BUILD_ARGS[@]}" .

echo ""
echo "Build complete. Image(s) ready:"
echo "  $TAG"
if [ "$VARIANT" = "prod" ] && [ "$VERSION" = "8.1" ]; then
    echo "  ${TAG_BASE}:latest"
elif [ "$VARIANT" = "source" ] && [ "$VERSION" = "8.1" ]; then
    echo "  ${TAG_BASE}:source"
fi
