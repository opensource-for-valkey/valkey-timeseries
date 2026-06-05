# valkey-timeseries Makefile
# Wraps common Docker and host-native development commands.
#
# Quick reference:
#   make docker-build        Build production Docker image
#   make docker-build-source Build source Docker image (Valkey from source)
#   make docker-up           Start standalone container
#   make docker-down         Stop and remove standalone container
#   make docker-up-cluster   Start 3-node cluster
#   make docker-down-cluster Stop and remove cluster
#   make docker-shell        Open a shell in the running container
#   make docker-logs         Follow container logs
#   make docker-test         Run integration tests against running container
#
#   make build               Build module on host (cargo build --release)
#   make test                Run full test suite via build.sh
#   make lint                Format check + clippy
#   make clean               Remove build artifacts + Docker volumes

.PHONY: help build test lint clean
.PHONY: docker-build docker-build-source docker-up docker-down
.PHONY: docker-up-cluster docker-down-cluster docker-shell docker-logs docker-test

# ── Default target ──────────────────────────────────────────────────
help:
	@echo "valkey-timeseries — available targets:"
	@echo ""
	@echo "  Docker (containerized):"
	@echo "    make docker-build              Build production image (official Valkey base)"
	@echo "    make docker-build-source       Build source image (Valkey from source)"
	@echo "    make docker-up                 Start standalone container"
	@echo "    make docker-down               Stop and remove standalone container"
	@echo "    make docker-up-cluster         Start 3-node cluster for fanout testing"
	@echo "    make docker-down-cluster       Stop and remove cluster"
	@echo "    make docker-shell              Open a shell in the running container"
	@echo "    make docker-logs               Follow container logs"
	@echo "    make docker-test               Run integration tests against running container"
	@echo ""
	@echo "  Host (native):"
	@echo "    make build                     Build module on host (cargo build --release)"
	@echo "    make test                      Run full test suite (format, clippy, build, tests)"
	@echo "    make lint                      Format check + clippy only"
	@echo "    make clean                     Remove build artifacts + Docker data"

# ════════════════════════════════════════════════════════════════════
# Docker targets
# ════════════════════════════════════════════════════════════════════

docker-build:
	@./scripts/build-docker.sh prod 8.1

docker-build-source:
	@./scripts/build-docker.sh source 8.1

docker-up:
	docker compose up -d
	@echo ""
	@echo "Valkey with timeseries module is running."
	@echo "  Connect:  valkey-cli -h localhost -p 6379"
	@echo "  Test:     valkey-cli -h localhost TS.ADD test 1000 42"

docker-down:
	docker compose down -v

docker-up-cluster:
	docker compose -f docker-compose.yml -f docker-compose.cluster.yml up -d
	@echo ""
	@echo "Waiting for cluster to form..."
	@sleep 5
	@echo "Cluster nodes:"
	@docker compose -f docker-compose.yml -f docker-compose.cluster.yml exec -T valkey-timeseries valkey-cli cluster nodes 2>/dev/null || echo "  (try again in a moment — cluster may still be initializing)"

docker-down-cluster:
	docker compose -f docker-compose.yml -f docker-compose.cluster.yml down -v

docker-shell:
	docker compose exec valkey-timeseries bash

docker-logs:
	docker compose logs -f

docker-test:
	@echo "Running integration tests against docker-compose service..."
	@cd tests && \
		VALKEY_HOST=localhost \
		VALKEY_PORT=6379 \
		python3 -m pytest --cache-clear -v . -k "$(TEST_PATTERN)"

# ════════════════════════════════════════════════════════════════════
# Host-native targets
# ════════════════════════════════════════════════════════════════════

build:
	cargo build --release
	@echo "Module built: target/release/libvalkey_timeseries.$$(uname -s | tr '[:upper:]' '[:lower:]' | sed 's/darwin/.dylib/;s/linux/.so/;s/windows/.dll/')"

test:
	SERVER_VERSION=$${SERVER_VERSION:-unstable} ./build.sh

lint:
	cargo fmt --check
	cargo clippy --profile release --all-targets -- -D clippy::all

# ════════════════════════════════════════════════════════════════════
# Cleanup
# ════════════════════════════════════════════════════════════════════

clean:
	@echo "Cleaning Rust build artifacts..."
	cargo clean
	@echo "Removing test build artifacts..."
	rm -rf tests/build/ test-data/
	@if docker compose ps --status running 2>/dev/null | grep -q valkey-timeseries; then \
		echo "Stopping running containers..."; \
		docker compose down -v 2>/dev/null || true; \
	fi
	@if docker compose -f docker-compose.yml -f docker-compose.cluster.yml ps --status running 2>/dev/null | grep -q valkey-timeseries; then \
		docker compose -f docker-compose.yml -f docker-compose.cluster.yml down -v 2>/dev/null || true; \
	fi
	@echo "Clean complete."
