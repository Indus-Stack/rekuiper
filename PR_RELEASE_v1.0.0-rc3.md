# Release Candidate 3 (v1.0.0-rc3): Industrial Connectors, Declarative GitOps, Dead-Letter Queues, OpenTelemetry, and Inspection Tools

## Summary

This pull request prepares `rekuiper` version `v1.0.0-rc3`.
It adds pure-Rust industrial connectors (Modbus and OPC-UA), declarative GitOps configuration management, multi-stage dead-letter queues (DLQ), OpenTelemetry (OTLP) data ingestion and export, and inspection tools (`rekuiper top` and `GET /ui`).

All code runs without new external dependencies in `Cargo.lock`. All documentation adheres to the ASD-STE100 Simplified Technical English specification.

---

## Changes by Area

### 1. Industrial Connectors (Modbus and OPC-UA)
- Added pure-Rust stream sources, lookup tables, and sinks for Modbus TCP and Modbus RTU-over-TCP (`crates/rekuiper-connectors/src/modbus.rs`).
- Added contiguous register batching in Modbus polling to decrease network traffic.
- Added pure-Rust OPC-UA Binary TCP client over `opc.tcp` for stream ingestion and sink delivery (`crates/rekuiper-connectors/src/opcua.rs`).
- Removed the requirement for external gateway containers, decreasing memory usage on edge devices.

### 2. Declarative GitOps Configuration Engine
- Added the `--declarative <PATH>` command-line option to `kuiperd` to load YAML and JSON definitions from a file or directory (`crates/rekuiper-server/src/declarative.rs`).
- Added an automated directory monitor that reloads configuration when files change on disk.
- Added a read-only mutation guard (`declarative_mutation_guard`) that blocks REST API changes when declarative mode is active.
- Added the `POST /declarative/reload` endpoint for manual or webhook-driven synchronization.
- Added the `--validate` flag to validate manifests offline without starting the server daemon (`crates/rekuiper-sql/src/validator.rs`).

### 3. Dead-Letter Queues (DLQ)
- Added error capture in five pipeline stages (`crates/rekuiper-server/src/engine/dlq.rs`):
  1. `codec_decode`: Unparsable or malformed input payloads.
  2. `schema_validation`: Missing required fields or schema type mismatches.
  3. `sql_eval`: Runtime division by zero, type errors, or function failures.
  4. `data_template`: Template compilation or rendering errors.
  5. `sink_delivery`: Permanent 4xx HTTP responses, broker rejections, or PLC hardware exceptions.
- Added an in-memory bounded ring buffer per rule that stores recent error records.
- Added error inspection and management REST endpoints:
  - `GET /rules/:id/errors`: View recent error records and diagnostics.
  - `DELETE /rules/:id/errors`: Clear recorded errors for a rule.
- Added CLI command aliases: `rekuiper geterrors <rule_id>` and `rekuiper clearerrors <rule_id>`.

### 4. OpenTelemetry (OTLP) Ingestion and Export
- Added HTTP push ingestion endpoints on the server port:
  - `POST /v1/metrics`, `POST /v1/logs`, `POST /v1/traces`
  - `POST /otlp/v1/metrics`, `POST /otlp/v1/logs`, `POST /otlp/v1/traces`
- Ingestion routes remain open for data traffic when declarative GitOps mode is active.
- Added processing for ProtoJSON stringified 64-bit integers (`timeUnixNano`, `asInt`, `asDouble`), accepting both numbers and strings.
- Added automatic DLQ routing for corrupted OTLP payloads (`__raw_error__`, `__raw_payload__`).
- Added the OTLP export sink (`crates/rekuiper-connectors/src/otlp.rs`) with retry backoff for transient network issues and immediate DLQ routing for permanent 4xx responses.

### 5. Inspection Tools: `rekuiper top` and Web Console
- Added the interactive terminal dashboard:
  - Command: `rekuiper top`
  - Accepts polling intervals (`-i 500ms`, `-i 2s`), single snapshot execution (`--once`), and rule filters (`-r <rule_id>`).
  - Uses alternate screen buffers (`\x1b[?1049h`) with hidden cursor (`\x1b[?25l`) and restores screen and cursor cleanly on `Ctrl+C`.
- Added the embedded web console at `GET /ui` (`crates/rekuiper-server/src/ui.html`):
  - Self-contained in a single file (< 60 KB, zero external CDN scripts or fonts).
  - Displays engine uptime, throughput counters (`rec/s`), active rules, and DLQ status.
  - Rule inspector drawer with formatted SQL queries, per-stage DLQ counters, recent error records, and error clearing.
  - Declarative GitOps badge with **Reload Manifest** trigger.

---

## Version References Updated (1.0.0-rc2 → 1.0.0-rc3)

The version string was updated across the entire repository:
1. `Cargo.toml`: Workspace package version set to `1.0.0-rc3`.
2. `Cargo.lock`: Workspace crate dependencies regenerated for `1.0.0-rc3`.
3. `Makefile`: `VERSION ?= 1.0.0-rc3`.
4. `SECURITY.md`: Supported versions table updated with `v1.0.0-rc3 (Latest)`.
5. `deploy/chart/ekuiper/Chart.yaml`: `appVersion: 1.0.0-rc3`.
6. `deploy/chart/ekuiper/README.md`: AppVersion badge and Helm custom value example updated.
7. `deploy/docker/docker-compose.yml`: Default image tag updated to `1.0.0-rc3`.
8. `deploy/docker/README.md`: Container run examples updated to `1.0.0-rc3`.
9. `README.md`: Docker run commands and throughput tables updated to `1.0.0-rc3`.
10. `docs/package.json` & `docs/package-lock.json`: Version set to `1.0.0-rc3`.
11. `openapi.json`: OpenAPI info object version set to `1.0.0-rc3`.
12. `docs/en_US/index.md`: Quickstart Docker command updated to `1.0.0-rc3`.
13. `docs/en_US/installation.md`: Standalone and Docker commands updated to `1.0.0-rc3`.
14. `docs/en_US/getting_started/getting_started.md` & `quick_start_docker.md`: Updated to `1.0.0-rc3`.
15. `docs/en_US/operation/usage/monitor_with_prometheus.md`: Docker command updated to `1.0.0-rc3`.
16. `docs/en_US/reliability/qualification_and_scope.md`: Release version header set to `1.0.0-rc3`.
17. `CHANGELOG.md`: Added `## [1.0.0-rc3] - 2026-10-15` section.
18. `RELEASE_NOTES.md`: Added `Release Notes - rekuiper v1.0.0-rc3` section.

---

## Verification Evidence (Executed in Linux ext4)

| Test Suite / Quality Gate | Execution Command | Result |
| :--- | :--- | :--- |
| **Code Formatting** | `cargo fmt --all -- --check` | **PASSED** (0 formatting differences) |
| **Linter & Compiler Warnings** | `cargo clippy --workspace --all-targets -- -D warnings` | **PASSED** (0 warnings, 0 errors) |
| **Dependency Isolation** | `cargo tree -p rekuiper-core -p rekuiper-sql -i rustls@0.21.12` | **PASSED** (Zero ignored dependencies in core) |
| **kuiperd Binary Build** | `cargo build -p kuiperd` | **PASSED** |
| **OTLP & Embedded UI Suite** | `cargo test --test test_otlp_and_ui` | **PASSED** (4/4 tests passed) |
| **CLI & Top Command Suite** | `cargo test -p rekuiper-cli --test test_cli_commands` | **PASSED** (15/15 tests passed) |
| **Declarative GitOps Suite** | `cargo test --test test_declarative_mode` | **PASSED** (2/2 tests passed) |
| **DLQ Poison-Pill Suite** | `cargo test --test test_dlq_poison_pill` | **PASSED** (8/8 tests passed) |
| **Industrial Connectors Suite** | `cargo test --test test_modbus_opcua_engine` | **PASSED** (4/4 tests passed) |
| **Connectors Unit Tests** | `cargo test -p rekuiper-connectors` | **PASSED** (140/140 tests passed) |
| **Throughput Benchmark** | `cargo test --test perf_throughput --release -- --nocapture` | **PASSED** (> 1.2M EPS pure engine, > 680k EPS streaming SQL) |
| **Documentation Link Check** | `python .github/scripts/directory_check.py docs/directory.json docs` | **PASSED** (0 broken links, 0 forbidden keywords) |
| **VitePress Site Build** | `npm --prefix docs run build` | **PASSED** (Compiled cleanly in 60s) |
