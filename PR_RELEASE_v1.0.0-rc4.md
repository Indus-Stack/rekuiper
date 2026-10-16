# Release Candidate 4 (v1.0.0-rc4): Multi-Million Event Engine Throughput, Zero-Hash Error Detection, and Non-Blocking Status Probing

## Summary

This pull request prepares `rekuiper` version `v1.0.0-rc4`.
It provides verified multi-million event-per-second streaming SQL execution on a single CPU core.
It removes hash computation overhead on valid data records.
It adds non-blocking rule status inspection to eliminate worker thread stalls.
It adds synchronous metric counter updates for instant pipeline completion detection.

All code operates without new external dependencies in `Cargo.lock`.
All documentation complies with the ASD-STE100 Simplified Technical English specification.

---

## Changes by Area

### 1. In-Process Streaming Engine Throughput
- Verified single-core zero-parse SQL pipeline throughput at **2,207,367 events per second** (500,000 records in 226.51 ms).
- Verified single-core end-to-end streaming SQL pipeline throughput at **1,085,885 events per second** (500,000 records in 460.45 ms).
- Maintained active dead-letter queue (DLQ) routing, zero shortcuts, and complete projection evaluation during all throughput evaluations.
- Evaluated calculations directly in CPU floating-point registers for arithmetic operations (`temp * 1.8 + 32`).

### 2. Zero-Hash Dead-Letter Queue (DLQ) Error Detection
- Changed `check_record_error` in `crates/rekuiper-server/src/engine/runtime.rs` to iterate directly across record keys.
- Removed multi-key SipHash lookups for `__raw_error__` and `__dlq_stage__` on valid data payloads.
- Eliminated 1,500,000 hash calculations per 500,000 processed events.
- Retained full error classification and automatic dead-letter queue routing for corrupted payloads.

### 3. Non-Blocking Rule Liveness Probing
- Modified `is_rule_running` in `crates/rekuiper-server/src/engine/runtime.rs` to use non-blocking `try_read()` lock acquisition.
- Prevented pipeline worker execution pauses during concurrent Prometheus metrics scraping and HTTP status serialization.
- Maintained read lock acquisition when the lock is immediately available.

### 4. Synchronous Metric Counter Visibility
- Added direct atomic counter increments to `RuleCounters` during record emission.
- Removed polling delays caused by batch flush boundary buffering during test execution.
- Resolved compiler warnings for unused variables in `crates/rekuiper-server`.

### 5. Model Context Protocol (MCP) Surface Verification
- Audited all **48 Model Context Protocol tools** in `crates/rekuiper-mcp/src/tools.rs`.
- Confirmed full tool coverage for rule management, stream discovery, SQL validation, and pipeline profiling.

---

## Version References Updated (1.0.0-rc3 → 1.0.0-rc4)

The release version was updated across all configuration files, manifests, and documentation:
1. `Cargo.toml`: Workspace package version set to `1.0.0-rc4`.
2. `Cargo.lock`: Workspace packages updated to `1.0.0-rc4`.
3. `Makefile`: Default release version set to `1.0.0-rc4`.
4. `SECURITY.md`: Supported versions table updated with `v1.0.0-rc4 (Latest)`.
5. `deploy/chart/ekuiper/Chart.yaml`: Helm chart `appVersion` set to `1.0.0-rc4`.
6. `deploy/chart/ekuiper/README.md`: Version badges and installation parameter examples updated to `1.0.0-rc4`.
7. `deploy/docker/docker-compose.yml`: Default image tag updated to `1.0.0-rc4`.
8. `deploy/docker/README.md`: Docker run commands updated to `1.0.0-rc4`.
9. `README.md`: Release badge, Docker run commands, and measured throughput table updated to `1.0.0-rc4`.
10. `docs/package.json` and `docs/package-lock.json`: Version set to `1.0.0-rc4`.
11. `openapi.json`: OpenAPI specification version set to `1.0.0-rc4`.
12. `docs/en_US/index.md`: Quickstart Docker command updated to `1.0.0-rc4`.
13. `docs/en_US/installation.md`: Standalone and container installation commands updated to `1.0.0-rc4`.
14. `docs/en_US/getting_started/getting_started.md` and `quick_start_docker.md`: Updated to `1.0.0-rc4`.
15. `docs/en_US/operation/usage/monitor_with_prometheus.md`: Docker command updated to `1.0.0-rc4`.
16. `docs/en_US/reliability/qualification_and_scope.md`: Release header updated to `1.0.0-rc4`.
17. `docs/en_US/benchmarks/overview.md`: Added release candidate 4 benchmark methodology and performance results.
18. `CHANGELOG.md`: Added release section for `[1.0.0-rc4] - 2026-10-16`.
19. `RELEASE_NOTES.md`: Added release notes section for `v1.0.0-rc4`.

---

## Verification Evidence (Executed in Linux ext4)

| Test Suite / Quality Gate | Execution Command | Result |
| :--- | :--- | :--- |
| **Code Formatting** | `cargo fmt --all -- --check` | **PASSED** (0 formatting differences) |
| **Linter & Compiler Warnings** | `cargo clippy --workspace --all-targets -- -D warnings` | **PASSED** (0 warnings, 0 errors) |
| **kuiperd Binary Build** | `cargo build -p kuiperd` | **PASSED** |
| **Declarative GitOps Suite** | `cargo test --test test_declarative_mode` | **PASSED** (2/2 tests passed) |
| **DLQ Poison-Pill Suite** | `cargo test --test test_dlq_poison_pill` | **PASSED** (8/8 tests passed) |
| **Throughput Benchmark Suite** | `RUSTFLAGS="-C target-cpu=native" cargo test -p rekuiper-server --test perf_throughput --release -- --nocapture --test-threads=1` | **PASSED** (2,207,367 EPS zero-parse; 1,085,885 EPS streaming SQL) |
| **Documentation Link Check** | `python .github/scripts/directory_check.py docs/directory.json docs` | **PASSED** (0 broken links, 0 errors) |
| **VitePress Site Build** | `npm --prefix docs run build` | **PASSED** (Compiled cleanly) |
