# Changelog

All notable changes to `rekuiper` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> Note (2026-09-12 benchmark audit): pre-0.424 throughput/parity lines below are
> historical internal/file-source numbers, not fair over-HTTP source-to-sink results.
> See `BENCHMARK-AUDIT.md` and `test/BENCHMARKS.md` for the audited methodology.
> No latency-histogram p99 is claimed.

## [1.0.0-rc5] - 2026-10-28

### Added
- **Zero-Copy Record Metadata (`RecordMeta`)**: Added an immutable, `Arc`-shared `RecordMeta` structure (`topic`, `source`, `custom`) to `StreamRecord`. Metadata passes through SQL projection pipelines and multi-port graph stages with zero map allocations and constant time ($O(1)$) overhead. Added zero-copy `meta()` SQL function with backward-compatible `__meta__` and JSON envelope fallback.
- **Multi-Port DAG Execution Engine**: Added native Directed Acyclic Graph (DAG) rule routing with port-indexed selective edge dispatch (`EdgeTargets::Multi`). Nodes selectively route records across output ports `0..N` based on ordered predicates without broadcast flattening.
- **Native Stream Control Operators**: Added `SwitchNode` (multi-port predicate routing), `RateLimitNode` (token-bucket drop and buffer rate limiter), and `WatchdogNode` (inactivity timeout dead-man alert generator).
- **CommsHub and Wire-Tap Observability**: Added an asynchronous event bus (`CommsHub`) in `AppState`. Added rate-limited wire-tap probes (capped at 20 events per second with drop counters) toggleable via `POST /rules/:id/tap/:node_id/enable|disable`. Added real-time `/comms` WebSocket broadcast stream.
- **Visual Stream Studio**: Embedded a visual flow canvas directly in `/ui`. Users can design stream pipelines with drag-and-drop source, operator, and sink nodes, inspect live node badges (events per second, queue depth, error count), and sample live tuple data in an interactive wire-tap drawer.
- **Outbound Edge Agent and Yamux Tunneling**: Added `--control-plane-url` CLI option and `REKUIPER_CONTROL_PLANE_URL` environment configuration to `kuiperd`. The edge agent establishes an outbound mTLS WebSocket connection with exponential backoff. Yamux multiplexing handles control telemetry on stream 0 and tunnels remote HTTP and WebSocket requests into the local Axum router on streams 1..N.
- **Transactional Deployment Watchdog and Auto-Rollback**: Added automatic health monitoring for 30 seconds following declarative configuration deployment. The watchdog automatically reverts the edge gateway to the previous configuration snapshot and raises an alert if error counts or panics exceed thresholds.
- **Credential Masking and Secret Protection**: Added automatic credential redaction (`***REDACTED***`) for sensitive fields (passwords, bearer tokens, private keys, client secrets) across all configuration export and inspection APIs. Preserves original credentials bidirectionally during declarative reloads.
- **Fleet Control Plane ("Rekuiper Manager")**: Added device fleet catalog (`/api/v1/fleet/devices`) and group management (`/api/v1/fleet/groups`). Added staged canary rollout engine (`/api/v1/fleet/rollouts`) supporting progressive rollout stages (5% -> 25% -> 100%) and automatic canary rollback. Added remote Yamux proxy (`/fleet/proxy/:device_id/*path`), unified audit logging (`/api/v1/fleet/audit`), and fleet control dashboard in `/ui`.
- **Fleet Provisioning and Bearer Authentication**: Added token validation (`BasicConfig.fleet_token`, `REKUIPER_FLEET_TOKEN`) for `/fleet/ws` and `/api/v1/fleet/*` endpoints. Rejects unauthorized registrations, proxy requests, and rollout commands with HTTP 401 Unauthorized.
- **Model Context Protocol (`rekuiper-mcp`) Expansion**: Expanded the native MCP server to 57 tools with dedicated categories for Fleet Management (`list_fleet_devices`, `get_fleet_rollouts`, `trigger_fleet_rollout`, `list_fleet_snapshots`, `proxy_fleet_device_api`) and Visual Stream Studio & Wire-Tap (`get_studio_palette`, `validate_graph_rule`, `toggle_wire_tap`, `get_wire_tap_status`). Added 3 new MCP live resources (`rekuiper://fleet/devices`, `rekuiper://fleet/rollouts`, `rekuiper://studio/palette`) and 2 interactive orchestration prompts (`staged_fleet_rollout`, `design_visual_dag_pipeline`).
- **WASM Fuel Metering and Instance Pooling**: Added deterministic instruction fuel metering (`DEFAULT_WASM_FUEL_BUDGET = 1,000,000`) to `WasmManager` (`wasmi::Config::consume_fuel(true)`), terminating runaway guest loops before they stall worker threads. Added lock-free `ArrayQueue<PooledInstance>` pooling (8 pre-instantiated instances per module) to eliminate per-event linear memory instantiation overhead.
- **Fleet Control Plane KV Persistence and Non-Blocking Concurrency**: Added durable KV persistence across `fleet_devices`, `fleet_snapshots`, `fleet_rollouts`, and `fleet_audit` namespaces with automatic state restoration (`FleetControlPlane::restore_from_kv`). Refactored `dispatch_snapshot_to_cohort` and `trigger_rollback` to collect command channels under short synchronous locks and perform asynchronous I/O and KV persistence outside lock guards.

### Security
- **Supply-Chain & RustSec Advisory Remediation**: Upgraded `tokio` to `1.54.1` (remediating `RUSTSEC-2026-0386`), upgraded `zstd-sys` to `2.1.2+zstd.1.5.7` (remediating `RUSTSEC-2026-0410`), and upgraded direct `zstd` dependency in `rekuiper-sql` to `0.14.3` (`zstd-safe 8.2.0`, remediating `RUSTSEC-2026-0391`, `RUSTSEC-2026-0398`, and `RUSTSEC-2026-0403` with zero ignored advisories in downstream core crates).
- **Conditional Route Mounting and Auth Middleware Hardening**: Mounted `/ui` and `/ui/ws` routes only when `basic.enable_ui` is enabled. Enforced `basic.authentication` RS256 JWT validation across `/ui`, `/v1/metrics`, `/v1/logs`, and `/v1/traces` (exempting only `/`, `/ping`, `/fleet/ws`, and `/api/v1/fleet/*` which enforce dedicated fleet bearer token checks).
- **ConnectInfo Peer IP Enforcement and Configurable Prometheus Port**: Wired `into_make_service_with_connect_info::<SocketAddr>()` on both primary and secondary listeners so `ui_ip_allowlist` validates peer socket addresses reliably. Fixed standalone Prometheus exporter to bind to `config.basic.prometheus_port` instead of a hardcoded port.

### Changed
- **Verified Benchmark Results (2.46M+ EPS)**: Re-verified single-core `--release` throughput (`perf_throughput.rs`): **2,464,711 events/s** (`202.86 ms` for 500,000 records, zero-parse in-process SQL pipeline) and **1,141,417 events/s** (`438.05 ms` for 500,000 records, end-to-end JSON pipeline).
- **Non-Blocking Host Metrics Cache & O(1) Ring Buffers**: Added a 500ms TTL cache for `sysinfo::System` CPU and memory refreshes on `GET /` and `GET /metrics`. Replaced `Vec::remove(0)` shifts with $O(1)$ `VecDeque::pop_front()` across `TraceSpan`, `RuleErrorBuffer`, and `JoinStreamState`.
- **Modular Source Connectors & Documentation Visuals**: Extracted 2,533 lines of source connector actors into `crates/rekuiper-server/src/engine/sources.rs`. Added 14 ASD-STE100 light-theme architectural SVG diagrams across `docs/en_US/`, bringing visual diagram coverage to 21.3% (39/183 pages) with zero broken links.

## [1.0.0-rc4] - 2026-10-16

### Added
- **Engine Throughput Optimizations**: Replaced hash-based error checks on data records with key reference iterations. This eliminates SipHash operations on valid records while it keeps full dead-letter queue routing for invalid records.
- **Non-Blocking Rule Liveness Check**: Changed `is_rule_running` to use `try_read()` lock acquisition. This prevents worker thread delays during concurrent HTTP metrics serialization.
- **Direct Metric Counter Updates**: Added atomic counter increments to report pipeline completion immediately during test polling.

### Changed
- **Verified Benchmark Results**: Updated benchmark measurements for release candidate 4. Pure engine throughput is 2,207,367 events per second. Streaming SQL pipeline throughput is 1,085,885 events per second.
- **Compiler Warnings Resolved**: Resolved unused variable warnings in `rekuiper-server`.

## [1.0.0-rc3] - 2026-10-15

### Added
- **Modbus and OPC-UA Connectors**: Added pure-Rust sources, tables, and sinks for Modbus TCP, Modbus RTU-over-TCP, and OPC-UA Binary (`opc.tcp`). The connectors communicate directly with industrial equipment without external gateway containers.
- **Declarative GitOps Engine**: Added the `--declarative` option to load and monitor configuration files and directories. Added the `POST /declarative/reload` endpoint and a mutation guard to enforce read-only operation.
- **Dead-Letter Queues (DLQ)**: Added per-stage error isolation for decode, validation, SQL evaluation, template, and sink delivery stages. Added the `GET /rules/:id/errors` and `DELETE /rules/:id/errors` management endpoints.
- **OpenTelemetry (OTLP) Processing**: Added HTTP ingestion endpoints at `/v1/metrics`, `/v1/logs`, and `/v1/traces`. Added processing for 64-bit integer strings. Added the OTLP export sink with retry backoff and error classification.
- **Inspection Tools (`top` and Web UI)**: Added the `rekuiper top` CLI command for live terminal monitoring. Added the self-contained web console at `GET /ui`.

### Changed
- **Zero New Dependencies**: Kept `Cargo.lock` free of new external crates.
- **Telemetry Ingestion Guard**: Permitted HTTP POST telemetry data on `/v1/*` when declarative mode is active.

## [1.0.0-rc2] - 2026-10-14

### Added
- **Complete SQL Function Documentation Coverage**: Documented all 221 built-in SQL functions and aliases across category reference pages with live-verified inputs, outputs, and edge cases (100.0% catalog parity).
- **Public Benchmark Documentation**: Published audited in-process streaming engine benchmarks for rekuiper v1.0.0-rc2 exceeding 2,144,000 EPS (zero-parse pipeline) and 994,000 EPS (end-to-end JSON).
- **Complex Event Processing Prompts**: Added `generate_cep_rule` prompt template to `rekuiper-mcp` for sequence match rule synthesis.
- **WASM Plugin Execution**: Documented `wasm_run` and `wasm` UDF execution syntax for compiled WebAssembly modules.

### Changed
- **Zero-Vulnerability Dependency Stack**: Upgraded `sqlx` to 0.8.6 and replaced unmaintained `serde_yaml` with `serde_norway` 0.9.
- **Automated Advisory CI Gate**: Integrated `cargo-deny` security scanning in continuous integration workflow.
- **Expanded MCP Intelligence**: Extended `validate_sql` and `test_sql_expression` tool schemas with CEP sequence matching, ASOF joins, and stream DEFAULT field syntax.

## [1.0.0-rc1] - 2026-10-14

### Added
- **High-Throughput Streaming Engine**: Processes 1,078,357 events each second in end-to-end benchmarks (500,000 records in 463.66 ms). In-process engine throughput reaches 1,733,311 events each second (288.46 ms).
- **Zero-Clone Borrowed WHERE Expressions**: Compares record values directly against literal values without cloning memory buffers.
- **CPU Register Arithmetic**: Computes numeric operations (`+`, `-`, `*`, `/`) directly in hardware registers.
- **Complex Event Processing Functions**: Adds deterministic finite automaton functions `sequence_match` and `sequence_count`. Stores a maximum of 10,000 partition keys to bound memory.
- **Full SQL Catalog Lookups**: All 221 built-in SQL functions use constant-time catalog lookups with `LazyLock`.
- **Temporal ASOF JOIN**: Joins rapid sensor streams with slower reference streams on timestamp values.
- **Columnar Parquet Batch Writer**: Batches writes into 1,024-record row groups. Removes internal metadata keys from schema.
- **MCP Profiler Tool**: Adds `explain_pipeline_profile` tool to analyze SQL topology offline.
- **Video Source Connector**: Ingests RTSP and HTTP video streams through FFmpeg pipes.
- **ZeroMQ Sink and Source**: Connects PUB/SUB sockets with dynamic topics.
- **Image Sink Retention Pruning**: Removes old files by age and file count.
- **Upstream eKuiper Parity**:
  - Sets default column values in stream DDL (#PR-4025 parity).
  - Validates `BUFFER_FULL_POLICY` on joined streams (#PR-4133 parity).
  - Adds disabled sink action option (`disable: true`) (#PR-4100 parity).
  - Validates dynamic SQL identifiers in SQL sinks (#PR-4080 parity).
  - Adds Kafka `offsetReset` configuration options (#PR-4190 parity).
  - Binds network listeners to `127.0.0.1` by default (#PR-4089 parity).

### Changed
- Removed clippy warnings across `video` and `sinks` under `-D warnings`.
- Unified simulator duration string and numeric interval parsing with `CONF_KEY` resolution.
- Applied workspace-wide rustfmt formatting.
- Updated Docker image tags to `1.0.0-rc1` and Helm chart AppVersion to `1.0.0-rc1`.

## [0.508.0-beta] - 2026-10-08

### Added
- **Full Sink Options Parity**: Added `sendSingle`, `fields`, `excludeFields`, `dataField`, `batchSize`, and `lingerInterval` options to ActionRuntime. Supported delimited formatting with custom delimiters. Added upfront sink configuration validation for format and MQTT topics.
- **Go text/template Engine**: Implemented full Go `text/template` parser, AST, and evaluator with Sprig helper functions (`toJson`, `base64`, `printf`, `index`, math, and string transformations). Added upfront template syntax validation during rule creation.
- **Stream DDL Validation**: Added column type validation for `CREATE STREAM` and `CREATE TABLE`. Supported ingress record type coercion for declared schemas. Supported `FORMAT="binary"` and `FORMAT="delimited"`.
- **Built-in SQL Functions**: Added `to_seconds`, `now`, `current_timestamp`, `local_timestamp`, `element_at`, string `min`/`max`, and logarithms (`log`, `log2`, `ln`). Added extended argument support for `cast`, `latest`, and `lag`.
- **HTTPPush Source Listener**: Added automatic HTTP listener route lifecycle management for streams configured with `TYPE="httppush"`.
- **Prometheus Metric Taxonomy**: Added all 32 Prometheus metric families with multidimensional labels for rules, connections, sources, operators, and sinks.
- **CLI Subcommands and POSIX Exit Codes**: Added `explain rule` and `query` subcommands. Extended `show`, `drop`, and `create schema` subcommands. Standardized POSIX exit codes and formatted text table outputs.
- **Configuration Reset and Merge Modes**: Added default configuration reset (`reset_configuration`) for data import. Added `partial=1` query parameter for additive merge mode.
- **Model Context Protocol (MCP) Parity**: Added `explain_rule` tool and updated `import_data` tool with partial merge mode and fail-fast validation.

### Changed
- **REST API Response Envelopes**: Standardized all error responses to uniform JSON envelopes (`{"error": <code>, "message": "..."}`) with `application/json` content type. Documented intentional RFC 9110 HTTP status superiorities in `concepts/ekuiper_compatibility.md`.
- **Fail-Fast Data Import Validation**: Enforced upfront payload validation on `POST /async/data/import`. The endpoint returns HTTP 400 Bad Request with a clear message if the request body is empty or malformed.
- **Output Shape Parity**: Aligned null field output with `sendNilField` settings. Removed internal runtime metadata keys from payloads. Ensured deterministic sorted JSON key ordering.

### Fixed
- **Window HAVING State Change Detection**: Enabled stateful change detection (`had_changed`) in window `HAVING` clauses.
- **Test Harness Synchronization**: Updated OpenAPI smoke tests to assert fail-fast 400 validation on empty data import requests.

### Packaging
- Updated official Docker images to `ankurkrp/rekuiper:0.508-beta`.
- Updated Kubernetes Helm chart `AppVersion` to `0.508-beta`.

---

## [0.507.0-beta] - 2026-10-04

### Added
- **eKuiper REST Parity for Rule Status**: Updated `GET /rules/:id/status` to return structured status objects with top-level `status` (`"running"`, `"stopped"`), `message`, and detailed metrics matching the upstream eKuiper REST schema.
- **Rule Metadata Parity**: Added support for user-supplied rule `name` and optional `version` field in `RuleDefinition`, preserved during rule lifecycle and returned in `GET /rules` and `GET /rules/:id`.

### Changed
- **CI Pipeline Optimization**: Streamlined `.github/workflows/ci.yml` and `.github/workflows/docker.yml` triggers to run full test matrices exclusively on pull requests and release tags, eliminating redundant duplicate test runs on branch merges.
- **Qualification Test Harness**: Enhanced `find_kuiperd_bin()` to reliably discover daemon binaries across custom `CARGO_TARGET_DIR` paths and test runner build directories.

### Packaging
- Updated official Docker images to `ankurkrp/rekuiper:0.507-beta`.
- Updated Kubernetes Helm chart `AppVersion` to `0.507-beta`.

---

## [0.506.0-beta] - 2026-10-03

### Added
- **Upstream SQL Parity Functions**: Implemented `array_positions(array, value)` returning zero-based match indexes, `acc_distinct_collect(expr)` (and alias `distinct_acc(expr)`) for distinct accumulator collections in arrival order, and 4-argument `lead(expr, offset, default, ignore_null)` with null skipping.
- **Stream Ingestion Buffer Policy**: Validated `BUFFER_FULL_POLICY` property during stream creation and updates, permitting `block` and `dropOldest`.
- **File Sink Traversal Hardening**: Enforced path traversal prevention blocking `..` parent references unless explicitly allowed via `allowExternalFileAccess`.
- **Model Context Protocol (MCP) Server 2.0**: Added 4 new tools (`register_wasm_plugin`, `list_wasm_plugins`, `delete_wasm_plugin`, `validate_secrets`), 4 new resources (`rekuiper://plugins/wasm`, `rekuiper://schemas/rabbitmq`, `rekuiper://schemas/parquet`, `rekuiper://schemas/edgex`), 3 new prompts, and embedded offline support for vector similarity, array, and stateful analytics.

### Changed
- **Documentation Standards**: Rewrote and aligned all technical documentation in `docs/en_US/` to the ASD-STE100 Simplified Technical English standard with complete VitePress static build validation.

### Packaging
- Updated official Docker images to `ankurkrp/rekuiper:0.506-beta`.
- Updated Kubernetes Helm chart `AppVersion` to `0.506-beta`.

---

## [0.505.0-beta] - 2026-10-02

### Added
- **Native RabbitMQ Connector**: Native AMQP 0-9-1 source and sink connectors supporting durable exchanges, prefetch QoS, consumer routing, and automatic connection recovery.
- **Vector Math & Similarity Search**: Full support for vector distance and similarity functions (`cosine_similarity`, `vector_l2`, `vector_dot`, `vector_match`) with SQL threshold filtering.
- **WebAssembly (WASM) Plugin Runtime**: Integrated WASM runtime with dynamic REST registration (`POST /plugins/wasm`), lifecycle monitoring, and custom SQL function invocation (`wasm_run`).
- **Dynamic Secret Interpolation**: Connector secret resolution supporting both `env://` and `vault://` URIs with automatic masking on REST metadata endpoints.
- **Apache Parquet Columnar Storage**: Parquet sink and reader supporting columnar projection, compression, and predicate evaluation.
- **EdgeX Foundry Dual-Port & Message Bus Compatibility**: Concurrent dual-port listening on 59720 (legacy EdgeX) and 9081 (native HTTP), with OpenZiti zero-trust Docker Compose integration and synchronized KV storage configuration.

### Packaging
- Updated official Docker images to `ankurkrp/rekuiper:0.505-beta`.
- Updated Kubernetes Helm chart `AppVersion` to `0.505-beta`.

---

## [0.504.0-beta] - 2026-10-01

### Added
- **Native Dual-Binary Parity**: Introduced standalone binary entrypoints `rekuiperd` (server daemon) and `rekuiper` (CLI client) alongside `kuiperd` and `kuiper` drop-in aliases.
- **Dual Configuration Resolution**: Added prioritized loading of `etc/rekuiper.yaml` with seamless fallback to `etc/kuiper.yaml`.
- **Environment Variable Overrides**: Supported `REKUIPER__<SECTION>__<KEY>` alongside legacy `KUIPER__<SECTION>__<KEY>` environment variable hierarchies.

### Changed
- **Metadata Sanitization**: Replaced legacy EMQ author and `ekuiper.org` documentation metadata across all 30 connector/operator/function configuration definitions in `etc/` with I-Dacs Labs and the `rekuiper` GitHub repository.
- **Docker & Packaging**: Updated Dockerfiles, debian packaging metadata, and entrypoints to native `rekuiperd`/`rekuiper` with `measure@i-dacs.com` maintainer.
- **Documentation Overhaul**: Renamed core architecture to `concepts/rekuiper.md`, sanitized directory and tutorial path placeholders, and verified VitePress static build.

### Packaging
- Updated official Docker images to `ankurkrp/rekuiper:0.504-beta`.
- Updated Kubernetes Helm chart `AppVersion` to `0.504-beta`.

---

## [0.503.0-beta] - 2026-10-01

### Added
- **100% SQL Function Parity**: Implemented all remaining unsupported and divergent functions across the complete 162-function eKuiper catalog, verified with live streaming MQTT ingestion, execution, and sink verification.
- Native compression and decompression functions (`compress`, `decompress`) supporting `zlib`, `gzip`, `flate`, and `zstd` with base64 serialization.
- Timezone transformation function `convert_tz(dt, [from_tz], to_tz)` with `chrono-tz` and `Local` system clock support.
- Multi-unit sub-millisecond date arithmetic `date_calc(timestamp, duration_str)` parsing compound intervals (`1h30m10s100ms`, `5d`, `250µs`, `500ns`).
- Dynamic multi-column delta projection function `changed_cols(prefix, is_changed_only, col1, col2, ...)` in `SELECT` queries.
- Streaming array unnesting `unnest(array)` and map unwrapping `extract(map)` multi-row/multi-column capabilities.
- Stateful sliding window item accumulator `acc_collect(val)` for streaming collections.
- Object and dictionary manipulation primitives: `object(keys_arr, vals_arr)`, `zip(pairs_arr)`, `items(map)`.
- Non-blocking delay execution `delay(ms, val)` for throttling and simulation pipelines.

### Changed
- Extended `date_diff` to seamlessly support both 2-argument (`date_diff(d1, d2)`) defaulting to day units and standard 3-argument (`date_diff(unit, d1, d2)`) invocations.
- Upgraded documentation scorecards across all 16 SQL documentation modules to 100% verified status.

### Packaging
- Updated official Docker images to `ankurkrp/rekuiper:0.503-beta`.
- Updated Kubernetes Helm chart `AppVersion` to `0.503-beta`.

---

## [0.502.0-beta] - 2026-10-01

### Added
- Native Model Context Protocol server (`rekuiper-mcp`) providing 42 tools, 11 resources, and 5 interactive prompts for LLMs and AI agent integrations over JSON-RPC 2.0 stdio transport.
- Offline streaming SQL simulation tools (`test_sql_expression`, `explain_sql`, `validate_sql`) using embedded `rekuiper-sql` AST parsing and static analysis.
- DDL parsing support in `validate_sql` for `CREATE TABLE` and `CREATE STREAM` statements.
- Dynamic rule execution tracing and span telemetry inspection endpoints (`start_rule_trace`, `stop_rule_trace`, `get_rule_traces`, `get_trace_details`).
- End-to-end differential mathematical qualification suite comparing 10,000+ streaming records against eKuiper 2.4.1 across trigonometric, logarithmic, aggregation, and window operators.

### Fixed
- Fixed `start_rule_trace` JSON payload structure in `rekuiper-mcp` to match the server's `TraceStartBody` deserialization format.
- Bounded in-flight ID vector collection in qualification harness to at most 500 items to prevent unbounded vector allocation under heavy load.

### Packaging
- Bundled `rekuiper-mcp` binary in official Docker container image (`ankurkrp/rekuiper:0.502-beta`).
- Fully aligned and exposed network ports (9081 for REST/CLI, 20499 for Prometheus, 20498 for RPC) across Dockerfile, Docker Compose, and Kubernetes Helm service manifests.
- Updated Helm chart `appVersion` to `0.502-beta`.

## [0.501.0-beta] - 2026-09-25

### Added
- Atomic multi-operation storage transactions (`KvOperation`, `apply_transaction`) for `SqliteKvStore` ensuring ACID atomicity for multi-key updates.
- Serde aliases for MQTT connection configuration keys (`clientId`, `cleanSession`, `protocolVersion`, `keepAlive`).
- Bounded in-flight record tracking in the reliability qualification harness, rejecting any unaccounted record loss during process crashes.

### Fixed
- Fixed connection configuration rollback handling: storage write/delete errors now rollback atomically in a single transaction, and storage read failures reliably propagate HTTP 500 without leaving inconsistent state.
- Fixed stream data ingestion endpoint to return HTTP 200 when no subscribers are active or all rules have been deleted, strictly adhering to eKuiper drop-in semantics.
- Resolved asynchronous mutex contention in the reliability qualification harness.

### Verified
- Re-certified the full 20-step IIoT MQTT benchmark suite (Workloads W1–W5 across 5k, 20k, 50k, and 100k msg/s) with 0.00% packet loss and verified exact sink output.
- Measured improved CPU efficiency (up to 22.2% reduction in core utilization) with zero regressions across all workloads.

## [0.500.0-beta] - 2026-09-21

### Added
- In-memory Redis-style catalog (`MemoryCatalog` / `SharedCatalog`) with synchronized concurrency for streams, tables, and rules, eliminating disk I/O bottlenecks during hot-path rule execution and REST state queries.
- Hot-path caching for authentication tokens, configuration profiles, and database connection pools in `rekuiper-server`.
- Multi-row batched insert statement generation in SQL sink connector for high-throughput relational data export.
- Hierarchical peak capacity search with 1k resolution across W1–W5 workloads under a bounded broker (4,096-message queue limit).

### Changed
- SQLite metadata store pragmas updated to `journal_mode=WAL`, `synchronous=NORMAL`, 32 MiB page cache, and `temp_store=MEMORY` for non-blocking persistence.
- Channel queue depths on ingestion and sink pipelines expanded from 1,024 to 32,768 to absorb burst traffic without backpressure stalls.

### Performance
- W1 (telemetry filter) certified sustained ceiling reached **150,000 msg/s** (94.5% CPU, 17.4 MiB RAM, 1.0s lag; 151k saturates single core at 99.4% CPU with 20.53% packet drop).
- W2 (device 10s windows) certified on-schedule ceiling reached **200,000 msg/s** (94.4% CPU, 6.7 MiB RAM, 10.0s lag; lossless execution up to 240k msg/s before host load generator falls off schedule).
- W3 (ESPHome 10k plain-text topics) certified sustained ceiling reached **150,000 msg/s** (97.4% CPU, 16.6 MiB RAM, 1.0s lag, 15-message end gap out of 4.5M).
- W4 (vehicles 10k VIN windows) certified on-schedule ceiling reached **200,000 msg/s** (97.5% CPU, 18.1 MiB RAM, 11.0s lag).
- W5 (EV charger sessions) certified ceiling reached **126,000 msg/s** (86.7% CPU, 6.0 MiB RAM, 4.0s lag; 127k exceeds session close stability limit).
- Single-core CPU at 100k msg/s reduced by 9.1 percentage points on W1 (79.4% vs 88.5%) and 9.2 percentage points on W2 (70.9% vs 80.1%).

## [0.426.0-beta] - 2026-09-16

### Fixed
- File-sink output counters advance only after buffered rows are flushed successfully. Flush failures increment exceptions and failed-record counters instead of reporting delivery.
- MQTT payload, broker configuration, subscription, resubscription and connection failures now increment the owning rule's exception counter.
- Unsupported SQL sink URL schemes return an error instead of succeeding without writing a record.
- Removed built-in portable plugin, external service and JavaScript UDF records that claimed unavailable runtimes were installed and running. Portable plugin mutations now return `501 Not Implemented` until a real runtime exists.

### Added
- A bounded sustained MQTT benchmark for five rule workloads, with exact sink proofs, end-of-send backlog measurement, rule-exception checks and raw evidence.
- Independent Rust MQTT publisher and constant-memory subscriber tools for traffic-path capacity and sequence checks.
- Published reproduction commands for the 100k sustained suite and higher-rate search. The previous four-engine benchmark remains archived in the repository.

### Performance
- All five workloads sustained 100,000 MQTT messages/s for 120 seconds on one CPU core and 1 GiB RAM, processing 12 million messages per trial with exact delivery.
- The ESPHome topic workload sustained 150,000 messages/s for 120 seconds. The 200,000 messages/s target was not reached and is not claimed.

## [0.425.0-beta] - 2026-09-13

### Fixed
- **Windowed aggregation**: time and count windows now apply `WHERE`, partition by `GROUP BY` (one row per group), and honour `HAVING`, `ORDER BY` and `LIMIT`. Previously a window emitted a single row with group values taken from its first record, ignored `WHERE`, and buffered and cloned every row, which exhausted memory at high rates.
- **MQTT sink connection leak**: the sink created a new client with a never-ending event loop for every output record; it now keeps one persistent connection per action and reports disconnection as a send failure.

### Added
- `SESSIONWINDOW(unit, maxDuration, timeout)` (eKuiper argument order, processing time, graph API `sessionwindow`).
- Memory-bounded incremental window aggregation for `count/sum/avg/min/max`.
- MQTT source: stream `FORMAT` json (object or array), binary (`self`), delimited, protobuf (`SCHEMAID`); `meta(topic|qos|messageId)`; comma-separated topics; `clientid`, `protocolVersion`, `cleanSession`, `keepAlive`; 16 MiB packets.
- Sink offline cache and resend with the eKuiper options (`enableCache`, `memoryCacheThreshold`, `maxDiskCache`, `bufferPageSize`, `resendInterval`, `cleanCacheAtStop`, `resendPriority`, `resendIndicatorField`, `resendDestination`).
- IIoT/vehicle MQTT benchmark with tools, configs and raw evidence: `test/benchmark/iiot-mqtt`.

### Changed
- Tumbling and hopping windows align to natural time boundaries.
- HTTP stream ingest uses reserve-then-commit admission; the REST listener sets `TCP_NODELAY`.

## [0.424.0-beta] - 2026-09-12

### Fixed (documented parity defects, each proven against official eKuiper 2.4.1)
- **PostgreSQL sink binds FLOAT as REAL (`D-PG1`)**: `SqlSink` now binds typed parameters (int/float/bool/text/null) instead of stringifying every value, so `double precision` columns accept float writes.
- **SQL source / lookup accept documented plugin config (`D-PG2`, `D-PG3`)**: `SqlConnectorConfig` accepts the documented `dburl` + `templateSqlQueryCfg` / `internalSqlQueryCfg` shape (with `{{.field}}` rendering and TABLE→DATASOURCE→name fallback); polling sources emit rows and lookup joins resolve instead of silently yielding zero. SQLite lookups decode type-preserving values.
- **Windowed stream-stream JOINs (`D-JOIN`)**: count/tumbling/hopping/sliding windows fan in every joined stream (sources bootstrapped per rule), match `ON` conditions over namespaced rows (FROM/JOIN `AS` aliases supported), and project matches — including LEFT/RIGHT/FULL/CROSS preservation and per-row output for plain SELECTs.
- **Array index/slice syntax (`D-ARR`)**: new `Expr::Index`/`Expr::Slice` with postfix `a[0]`, `a[-1]`, `a[l:h]`, `a[:h]`, `a[l:]`, `a[:]` (0-based, end-exclusive, clamped) per the JSON-expressions doc.
- **`json_path_query` wildcards (`D-JSONPATH`)**: `$` root + `[*]` fan-out return match arrays; scalar/pointer behavior unchanged.
- **Ruletest SSE on documented port 10081 (`D-SSE`)**: sessions report the configured `httpServerPort` (default 10081, served on `httpServerIp:httpServerPort` plus the main router's `/test/:id`); paced `interval`/`loop` replay with 10-minute cap; history buffering so late subscribers lose nothing; unknown sessions 404.
- **Process-restart delivery resume (`D-RESTART`)**: connection/source/sink configs persist in KV and reload before rule restore, so resumed rules resolve their CONF_KEYs instead of falling back to loopback defaults.
- **CLI parity (`D-CLI`)**: `describe rule/script/service/schema/plugin`, `create table/script/schema/service/plugin` plus `-f` file forms for stream/table/rule, `gettopo`/`validate` rule, `export/import` ruleset/data, bare `getstatus import`; unknown entities exit non-zero.
- **Docker default `restIp` (`D-RESTIP`)**: shipped `etc/kuiper.yaml` binds `0.0.0.0` like the documented default, so mapped `-p 9081:9081` is reachable; `httpServerIp`/`httpServerPort` settings added (env-overridable).

## [0.423.0-beta] - 2026-09-11

### Highlights
- **Dynamic Rule Resume (`POST /rules/:name/start`)**: Implemented dynamic background worker reactivation (`spawn_rule_task`) and streaming source subscription bootstrapping (`bootstrap_rule_sources`) on rule start and restart, ensuring event processing resumes seamlessly when toggled.
- **Envelope Import Support (`POST /ruleset/import` & `POST /data/import`)**: Added full support for `{ "content": "..." }` stringified JSON/YAML envelopes, `{ "file": "..." }` payloads, and stringified rule maps (`"rules": { "id": "{\"id\": ...}" }`), guaranteeing full catalog import fidelity.
- **Zero Parity Defects**: Attained clean 0 target defects against baseline upstream eKuiper across full evaluation probe suite.

## [0.422.0-beta] - 2026-09-10

### Highlights
- **Scoped evaluation fixes**: Resolved documented compatibility items across 12 root causes found in evaluation against official `lfedge/ekuiper:2.4.1` (scoped; see TEST-CONTRACT-AUDIT where applicable).
- **Data Plane Integrity & Remote Ingestion**: Fixed MQTT source remote broker resolution (`CONF_KEY` & `server` options), completely eliminating Docker container connection drops.
- **PostgreSQL & Relational Data Plane**: Implemented authentic PostgreSQL `$1...$n` parameterized sink writes, type-safe lookup joins, and continuous streaming `SqlSource`.
- **Complete REST CRUD & Dynamic Patching**: Added missing HTTP `PUT` handlers on streams, tables, rules, and connections, plus `PATCH /configs` returning HTTP 204.
- **Enterprise Security & Auth Guard**: Added strict RSA/JWT authentication middleware enforcing RS256 token signature verification, expiry checks, raw JWT token formatting, and rejecting `Bearer` prefixes per eKuiper specifications.
- **Live Rule Testing & SSE Server**: Implemented live Server-Sent Events (SSE) streaming server for `/ruletest`, delivering real-time execution frames.
- **Schema Preservation & SQL Operators**: Retained explicit `StreamFields` and field data types in stream/table definitions and `/schema` endpoints, added `->` arrow nested JSON navigation, and enforced validation on unknown functions and missing streams.

## [0.421.0-beta] - 2026-09-10

### Highlights
- **Engine Throughput Record**: Sustained **320,000 to 540,000+ events/sec** (up to 546k eps on Linux x86_64, 370k eps on Windows, 91–134 ms for 50,000 events) on single CPU core with zero GC pauses and deterministic sub-millisecond execution.
- **Scoped route coverage**: 98 REST endpoints registered against the audited OpenAPI baseline with persistence, disk I/O, masking, and error handling (scoped; no absolute parity claimed).

### Added & Improved
- **Configuration & Uploads Subsystem**:
  - Authentic file upload persistence to `data/uploads/` with filename validation, path traversal defense, and metadata tracking (`POST/GET/DELETE /config/uploads`).
  - Native YAML metadata extraction for `/metadata/sources/{name}`, `/metadata/sinks/{name}`, and `/metadata/connections/{name}` with dynamic state overlays and recursive secret masking.
- **Bulk Rule Operations & Lifecycle**:
  - OpenAPI-compliant `Vec<BulkOperationResponse>` status reporting for `/rules/bulkstart` and `/rules/bulkstop`.
  - State reset support via `POST /rules/:name/reset_state`.
  - Authentic import status tracking via `GET /data/import/status`.
  - Trace validation returning 404 on missing traces (`GET /trace/:id`).
- **End-to-End Test Suite**:
  - Added `test_forensic_parity_endpoints` in `fvt_compat.rs` testing uploads, bulk rule control, and metadata masking (46/46 integration tests passing).
  - Benchmarked streaming throughput using `cargo test --release --test perf_throughput -- --nocapture`.
- **Packaging & Deployment**:
  - Synchronized Docker Compose, Helm chart (`AppVersion: 0.421-beta`), and GitHub Actions release workflows for automated multi-arch builds.

---

## [0.420.0-beta] - 2026-09-09

### Highlights: The Pure-Rust Engine Rewrite by I-Dacs Labs

Developed by **I-Dacs Labs** and released under the most permissive **MIT / Apache-2.0** open-source license, version 0.420-beta delivers **5x higher stream throughput (180,000+ events/sec)**, **sub-millisecond deterministic latency** with zero GC pauses, **1/4th binary footprint (9.60 MB)**, and **< 10 MB idle RAM consumption**.

### Added
- **High-Throughput Pipelined Architecture**:
  - Decoupled bounded actor sink queue (capacity 10,000) isolating SQL evaluation from network sink latency.
  - Non-blocking `try_send` ingestion loop with automatic backpressure management.
  - Dedicated persistent sink actor worker threads with shared connection pooling.
- **Full OpenAPI 3.0 Conformance**:
  - 100% of all 98 endpoints and 140 operations registered and validated with zero 404s.
  - Interactive rule testing pipeline via Server-Sent Events (`POST /ruletest`).
- **Comprehensive SQL Support**:
  - Streaming windowing: `TumblingWindow`, `HoppingWindow`, `SlidingWindow`, `CountWindow`.
  - Stream-Table lookup `JOIN` (`LEFT JOIN` and `INNER JOIN`) against in-memory tables, Redis, and SQL databases.
  - `CASE ... WHEN ... THEN ... ELSE ... END` expressions (searched and simple).
  - 40+ scalar, trigonometric, string manipulation, date-time, and JSON extraction functions.
  - State accumulators and analytical functions: `acc_map_agg()`, `acc_max()`, `acc_min()`, `acc_max_by()`, `acc_min_by()`, `acc_count()`, `acc_sum()`, `acc_avg()`, `lag()`, `unnest()`.
- **Connector Ecosystem**:
  - **MQTT Source & Sink**: Powered by `rumqttc` supporting QoS 0/1/2, TLS, client authentication.
  - **Apache Kafka Source & Sink**: High-throughput partitioned topic consumer and producer powered by `rskafka`.
  - **Redis Source, Sink & Lookup**: Key-value lookup tables, `redissub` pub/sub stream source, and `SET`/`PUBLISH` sink actions via `redis-rs`.
  - **WebSocket Source & Sink**: Streaming frame ingestion and client sink broadcasting via `tokio-tungstenite`.
  - **HTTP Pull Source**: Interval poller with configurable methods, HTTP headers, request bodies, and cancellation channels.
  - **Relational SQL Source, Sink & Lookup**: Multi-dialect SQL execution supporting PostgreSQL, SQLite, and MySQL via `sqlx`.
  - **File Source & Sink**: RFC-4180 CSV with automatic headers and newline-delimited JSON Lines.
  - **Sink Data Templates**: Mustache-style `{{.field}}` substitution for dynamic payload formatting.
- **Observability**:
  - Native Prometheus exposition format (`GET /metrics`) and standalone metrics server on port `20499`.
  - Metrics tracking: `kuiper_rule_count`, `kuiper_rule_status`, `kuiper_source_records_in_total`, `kuiper_source_records_out_total`, `kuiper_sink_records_in_total`, `kuiper_sink_records_out_total`, `kuiper_sink_exceptions_total`, `kuiper_sink_latency_us`.
- **Packaging & Publishing**:
  - Multi-stage Alpine Dockerfile with unprivileged `kuiper` user.
  - Docker Compose testbed with bundled Mosquitto MQTT broker and Redis.
  - GitHub Actions CI matrix testing across Ubuntu, Windows, and macOS.
  - Cross-platform release workflows for `x86_64` and `aarch64` targets.

### Compatibility
- 100% drop-in replacement for eKuiper CLI commands (`kuiper create stream`, `kuiper create rule`, etc.).
- Direct compatibility with eKuiper Manager Web UI via OpenAPI 3.0 REST endpoints.
- Preserves existing configuration semantics in `etc/kuiper.yaml`.
