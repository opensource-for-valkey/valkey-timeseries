# Multi-stage Dockerfile for valkey-timeseries module
#
# Build the Rust module in a builder stage, then copy into the official
# Valkey image for a minimal production image.
#
# Build arguments:
#   VALKEY_VERSION         - Valkey base image version (default: 9.1)
#   FEATURES               - Cargo features to pass (e.g., "valkey_8_0")
#   INCLUDE_SAMPLE_DATA    - Set to "true" to include sample datasets in the image
#
# Examples:
#   docker build -t valkey-timeseries:latest .
#   docker build --build-arg VALKEY_VERSION=8.0 --build-arg FEATURES=valkey_8_0 -t valkey-timeseries:8.0 .
#   docker build --build-arg INCLUDE_SAMPLE_DATA=true -t valkey-timeseries:latest .

# ── Stage 1: Build the Rust module ──────────────────────────────────
FROM rust:1.92-slim-bookworm AS builder

ARG FEATURES=""

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        protobuf-compiler \
        pkg-config \
        libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy manifests first for dependency caching
COPY Cargo.toml Cargo.lock* build.rs ./

# Copy source code
COPY src/ ./src/

# Build the module (release, cdylib)
RUN if [ -z "$FEATURES" ]; then \
        cargo build --release; \
    else \
        cargo build --release --features "$FEATURES"; \
    fi

# Strip the binary to reduce size
RUN strip target/release/libvalkey_timeseries.so 2>/dev/null || true

# ── Stage 2: Production image ───────────────────────────────────────
ARG VALKEY_VERSION=9.1
FROM valkey/valkey:${VALKEY_VERSION}

ARG INCLUDE_SAMPLE_DATA=""

LABEL org.opencontainers.image.title="valkey-timeseries"
LABEL org.opencontainers.image.description="Valkey module for timeseries data with PromQL support"
LABEL org.opencontainers.image.url="https://github.com/ccollie/valkey-timeseries"

# Copy the compiled module
COPY --from=builder /build/target/release/libvalkey_timeseries.so /usr/local/lib/

# Copy default configuration
COPY valkey.conf /usr/local/etc/valkey/valkey.conf

# Copy entrypoint script
COPY scripts/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Optionally include sample datasets
RUN mkdir -p /sample_data
COPY tests/data/ /sample_data/
# Note: sample data is always copied to /sample_data/; use LOAD_SAMPLE_DATA
# env var at runtime with docker-compose.data.yml or the load-sample-data.py
# script to ingest data into Valkey.

EXPOSE 6379

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["valkey-server", "/usr/local/etc/valkey/valkey.conf"]
