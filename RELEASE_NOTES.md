# Release Notes - rekuiper v0.508-beta

`rekuiper` v0.508-beta achieves upstream LF Edge eKuiper parity across sink options, dynamic templates, DDL validation, built-in SQL functions, HTTPPush listeners, REST API envelopes, CLI tools, and Prometheus metrics.

Key highlights:
- **Sink Options Parity**: Added `sendSingle`, `fields`, `excludeFields`, `dataField`, `batchSize`, and `lingerInterval` options to ActionRuntime. Supported delimited formatting with custom delimiters. Added upfront sink configuration validation for format and MQTT topics.
- **Go text/template Engine**: Implemented full Go `text/template` parser, AST, and evaluator with Sprig helper functions (`toJson`, `base64`, `printf`, `index`, math, and string transformations). Added upfront template syntax validation during rule creation.
- **Stream DDL Validation**: Added column type validation for `CREATE STREAM` and `CREATE TABLE`. Supported ingress record type coercion for declared schemas. Supported `FORMAT="binary"` and `FORMAT="delimited"`.
- **Built-in SQL Functions**: Added `to_seconds`, `now`, `current_timestamp`, `local_timestamp`, `element_at`, string `min`/`max`, and logarithms (`log`, `log2`, `ln`). Added extended argument support for `cast`, `latest`, and `lag`.
- **HTTPPush Source Listener**: Added automatic HTTP listener route lifecycle management for streams configured with `TYPE="httppush"`.
- **Prometheus Metric Taxonomy**: Added all 32 Prometheus metric families with multidimensional labels for rules, connections, sources, operators, and sinks.
- **Fail-Fast Data Import Validation**: Enforced upfront payload validation on `POST /async/data/import`. The endpoint returns HTTP 400 Bad Request with a clear message if the request body is empty or malformed.
- **REST API Response Envelopes**: Standardized all error responses to uniform JSON envelopes (`{"error": <code>, "message": "..."}`) with `application/json` content type. Documented intentional RFC 9110 HTTP status superiorities in `concepts/ekuiper_compatibility.md`.
- **CLI Subcommands and POSIX Exit Codes**: Added `explain rule` and `query` subcommands. Extended `show`, `drop`, and `create schema` subcommands. Standardized POSIX exit codes and formatted text table outputs.
- **Native InfluxDB v1 & v2 Sinks**: Implemented built-in `influx` and `influx2` sinks compiling directly into the engine binary with zero C dynamic dependencies. Supports InfluxDB Line Protocol serialization, dynamic measurements, tag templates, token authentication, and MCP schema inspection.
- **Native TDengine 3.x REST Sink**: Implemented pure Rust built-in `tdengine` / `tdengine3` sink connecting to TDengine 3.x REST service (`/rest/sql`). Supports regular table inserts, dynamic table templates, super tables (`USING <sTable> TAGS (...)`), and MCP resource schemas without requiring `libtaos.so`.
- **Native Apache IoTDB Sink**: Implemented pure Rust built-in `iotdb` sink connecting directly to Apache IoTDB REST service (`/rest/v1/insertRecord`). Supports dynamic tree model device path templates, explicit or projected measurement arrays, batch tablet aggregation, and MCP resource inspection.
- **Native Image Sink**: Implemented pure Rust built-in `image` sink writing binary camera frames (JPEG/PNG) to disk with automated age (`maxAge`) and count (`maxCount`) retention pruning.
- **Random Source Compatibility Alias**: Added native backward-compatibility alias mapping `TYPE="random"` stream DDL directly to the high-performance Simulator source engine with `random/` and `simulator/` configuration resolution.
- **Model Context Protocol (MCP) Parity**: Added `explain_rule` tool and updated `import_data` tool with partial merge mode and fail-fast validation in ASD-STE100. Added `audit_security_posture` prompt, `rekuiper://schemas/influx`, `rekuiper://schemas/tdengine`, `rekuiper://schemas/iotdb`, `rekuiper://schemas/image`, and `rekuiper://schemas/random` resources.

- Docker image: `ankurkrp/rekuiper:0.508-beta` (plus `latest`)
- Helm chart: `deploy/chart/ekuiper` (`AppVersion: 0.508-beta`)

---

# Release Notes - rekuiper v0.507-beta

`rekuiper` v0.507-beta delivers upstream LF Edge eKuiper REST API and rule metadata parity, enhanced daemon qualification discovery, and GitHub Actions CI runner workflow optimizations.

Key highlights:
- **eKuiper REST Parity for Rule Status**: `GET /rules/:id/status` returns structured status objects containing top-level `status` (`"running"`, `"stopped"`), `message`, `source_statuses`, and execution metrics matching the eKuiper REST contract.
- **Rule Metadata Parity**: Preserves user-supplied rule `name` and optional `version` tag in `RuleDefinition`, returned in `GET /rules` and `GET /rules/:id`.
- **CI Pipeline Optimization**: Streamlined GitHub Actions workflow triggers across `.github/workflows/ci.yml` and `.github/workflows/docker.yml` to eliminate redundant duplicate matrix executions on branch merges.
- **Qualification Test Harness Hardening**: Enhanced binary discovery in `find_kuiperd_bin()` to reliably resolve `kuiperd` across custom `CARGO_TARGET_DIR` paths and test runner environments.

- Docker image: `ankurkrp/rekuiper:0.507-beta` (plus `latest`)
- Helm chart: `deploy/chart/ekuiper` (`AppVersion: 0.507-beta`)

---

# Release Notes - rekuiper v0.506-beta

`rekuiper` v0.506-beta adds upstream SQL parity functions, stream ingestion buffer policy enforcement, file sink traversal hardening, and Model Context Protocol (MCP) server 2.0. This release also aligns all project documentation with the ASD-STE100 technical English standard.

Key highlights:
- **Upstream SQL Parity Functions**: Adds `array_positions(array, value)`, `acc_distinct_collect(expr)` (and alias `distinct_acc(expr)`), and four-argument `lead(expr, offset, default, ignore_null)`.
- **Stream Ingestion Buffer Policy**: Validates `BUFFER_FULL_POLICY` configuration during stream creation, permitting `block` and `dropOldest`.
- **File Sink Traversal Hardening**: Enforces path traversal prevention blocking `..` parent references unless `allowExternalFileAccess` is true.
- **Model Context Protocol (MCP) Server 2.0**: Adds four tools, four resources, three prompts, and offline simulation for vector similarity and array analytics.
- **ASD-STE100 Technical Documentation**: Standardizes all English documentation pages to the ASD-STE100 specification with verified static VitePress compilation.

- MCP Documentation: [`crates/rekuiper-mcp/README.md`](crates/rekuiper-mcp/README.md)
- Docker image: `ankurkrp/rekuiper:0.506-beta` (plus `latest`)
- Helm chart: `deploy/chart/ekuiper` (`AppVersion: 0.506-beta`)

---

# Release Notes - rekuiper v0.505-beta

`rekuiper` v0.505-beta introduces six enterprise features: native RabbitMQ AMQP 0-9-1 source and sink connectors, vector similarity search and distance functions, WebAssembly (WASM) plugin runtime, dynamic secret interpolation with Vault and environment variables, Apache Parquet columnar storage, and EdgeX Foundry concurrent dual-port listening (59720 and 9081) with OpenZiti zero-trust architecture.

Key highlights:
- **Native RabbitMQ AMQP 0-9-1 Connector**: High-throughput message ingestion and publishing with support for durable exchanges, consumer routing keys, prefetch count QoS, and auto-acknowledgment.
- **Vector Math & Similarity Functions**: Native SQL functions `cosine_similarity(v1, v2)`, `vector_l2(v1, v2)`, `vector_dot(v1, v2)`, and `vector_match(v1, v2)` enabling real-time edge vector search and anomaly detection.
- **WebAssembly (WASM) Plugin Runtime**: Dynamic module upload and registration via REST (`/plugins/wasm`) with SQL execution via `wasm_run(...)`.
- **Dynamic Secret Interpolation**: Automatic resolution of `vault://` and `env://` URIs across all connector configurations, with automatic redaction in REST management endpoints.
- **Apache Parquet Columnar Sink & Reader**: Compact columnar file sink and streaming reader with projection pushdown and predicate filtering.
- **EdgeX Foundry Dual-Port & Message Bus Compatibility**: Concurrent dual-port listening on 59720 (legacy EdgeX) and 9081 (native HTTP) with zero-trust OpenZiti stack support.

- Docker image: `ankurkrp/rekuiper:0.505-beta` (plus `latest`)
- Helm chart: `deploy/chart/ekuiper` (`AppVersion: 0.505-beta`)

---

# Release Notes - rekuiper v0.504-beta

`rekuiper` v0.504-beta delivers native dual-binary target parity (`rekuiperd` engine daemon and `rekuiper` CLI tool), prioritized `etc/rekuiper.yaml` configuration and `REKUIPER__` environment variable loading, complete metadata and vendor sanitization, and full documentation site rebuild.

Key highlights:
- **Native Dual-Binary Target Parity**: Dedicated `rekuiperd` and `rekuiper` binary targets built alongside `kuiperd` and `kuiper` drop-in aliases with unified CLI parser metadata.
- **Prioritized Configuration & Env Overrides**: Supports native `etc/rekuiper.yaml` with automatic fallback to `etc/kuiper.yaml`, plus full support for `REKUIPER__<SECTION>__<KEY>` alongside `KUIPER__<SECTION>__<KEY>`.
- **Connector & Metadata Sanitization**: Cleaned all 30 connector, sink, function, and operator schemas in `etc/` to reference I-Dacs Labs author metadata and the active GitHub repository.
- **Documentation Overhaul & VitePress Validation**: Restored and validated the complete VitePress documentation site with all pages and navigation links rendering with zero build errors.
- **Docker & Packaging Parity**: Updated multi-stage Dockerfiles and Debian package controls to default to `rekuiperd` and `rekuiper`.

- Docker image: `ankurkrp/rekuiper:0.504-beta` (plus `latest`)
- Helm chart: `deploy/chart/ekuiper` (`AppVersion: 0.504-beta`)

---

# Release Notes - rekuiper v0.503-beta

`rekuiper` v0.503-beta achieves **100% SQL function parity (162 / 162 functions verified)** across the entire eKuiper SQL catalog with exhaustive live MQTT stream assertions, complete data transformation capabilities, sub-millisecond date arithmetic, timezone conversions, accumulator streaming aggregates, and multi-row/multi-column projections.

Key highlights:
- **100% SQL Function Parity (162 / 162 Verified)**: All 16 previously unsupported functions and 1 divergent function across Math, String, Array, Aggregate, JSON, Hashing, Window, Other, DateTime, Analytic, Transform, Object, Multi-Row, and Multi-Column categories are fully implemented and validated against live streaming MQTT payloads.
- **High-Performance Compression & Decompression**: Native zero-copy implementations of `compress(data, method)` and `decompress(data, method)` supporting `zlib`, `gzip`, `flate`/`deflate`, and `zstd` with base64 serialization and deserialization.
- **Timezone Conversion (`convert_tz`)**: Zero-heap timezone conversions powered by `chrono-tz` and `chrono::Local` supporting arbitrary IANA timezone identifiers (e.g. `'Asia/Shanghai'`, `'America/New_York'`, `'UTC'`, `'Local'`).
- **Sub-Millisecond Date Arithmetic (`date_calc`) & Dual-Arity `date_diff`**: Complete duration parsing supporting combined expressions (`1h30m10s100ms`, `5d`, `250µs`, `500ns`) with nanosecond arithmetic, alongside both 2-argument (`date_diff(d1, d2)`) and 3-argument (`date_diff(unit, d1, d2)`) compatibility.
- **Dynamic Multi-Column Projections (`changed_cols`)**: Stateful column delta generator in `SELECT` queries that tracks predecessor events and projects dynamically prefixed difference columns (`diff_<col>`).
- **Multi-Row & Object Operators**: Added `unnest(array)` for flat record expansion, `extract(map)` for map-to-column unwrapping, and object manipulation primitives (`object(keys, values)`, `zip(pairs)`, `items(map)`).
- **Streaming Accumulators (`acc_collect`)**: Stateful sliding window item collector and accumulator aggregation for real-time edge micro-batching.

- Qualification Report: [`test/e2e_diff/`](test/e2e_diff/)
- Function Documentation: [`docs/en_US/sqls/functions/overview.md`](docs/en_US/sqls/functions/overview.md)
- Docker image: `ankurkrp/rekuiper:0.503-beta` (plus `latest`)
- Helm chart: `deploy/chart/ekuiper` (`AppVersion: 0.503-beta`)

---

# Release Notes - rekuiper v0.502-beta

`rekuiper` v0.502-beta introduces the native Model Context Protocol (MCP) server for LLM assistants, complete offline streaming SQL intelligence, dynamic execution tracing, and hardened reliability qualification.

Key highlights:
- **Native Model Context Protocol Server (`rekuiper-mcp`)**: An enterprise-grade, native Rust MCP server implementing standard JSON-RPC 2.0 stdio transport with 42 tools, 11 queryable resources (`rekuiper://`), and 5 AI assistant prompts.
- **Embedded Offline SQL Intelligence**: Static AST analysis, query deconstruction (`explain_sql`), in-memory transformation simulation (`test_sql_expression`), and schema validation (`validate_sql`) for both `CREATE STREAM` and `CREATE TABLE` DDL without network overhead.
- **Dynamic Rule Tracing & Observability**: Real-time message execution tracing controls (`start_rule_trace`, `stop_rule_trace`, `get_rule_traces`, `get_trace_details`) with support for sampling strategies and step-by-step latency profiling.
- **99.98% Differential Mathematical Qualification**: Verified mathematical and streaming SQL operator equivalence across trigonometric, logarithmic, aggregation, and window functions against Go-based eKuiper 2.4.1.
- **Hardened Process-Level Qualification**: Strictly bounded in-flight transaction tracking during abrupt process termination and automated recovery validation.
- **Unified Multi-Platform Docker Packaging**: Container image includes `rekuiper-mcp` alongside `kuiperd` and `kuiper` CLI with multi-arch support.
- **Complete Port & Service Alignment**: Fully declares operational ports `9081` (REST API & CLI), `20499` (Prometheus metrics), and `20498` (RPC parity) across Dockerfile `EXPOSE`, Docker Compose, and Kubernetes Helm service manifests (`svc.yaml`, `statefulset.yaml`).

- MCP Documentation: [`crates/rekuiper-mcp/README.md`](crates/rekuiper-mcp/README.md)
- Qualification Report: [`test/e2e_diff/QUALIFICATION_REPORT.md`](test/e2e_diff/QUALIFICATION_REPORT.md)
- Docker image: `ankurkrp/rekuiper:0.502-beta` (plus `latest`)
- Helm chart: `deploy/chart/ekuiper` (`AppVersion: 0.502-beta`)

---

# Release Notes - rekuiper v0.501-beta

`rekuiper` v0.501-beta delivers transactional storage guarantees, strict configuration consistency, and process-level crash qualification.

Key highlights:
- **Atomic Multi-Operation Transactions**: `SqliteKvStore` implements atomic multi-operation batch mutations (`KvOperation`, `apply_transaction`) under transactional rollback to ensure catalog and configuration integrity even under abrupt failure.
- **Strict Configuration Consistency**: Configuration key mutations operate under atomic transaction guarantees, and configuration read errors reliably propagate HTTP 500 without leaving inconsistent memory state.
- **Reliability Qualification**: Includes the verified process-level qualification test harness with bounded in-flight message accounting, engine crash simulation, and immediate recovery verification.
- **eKuiper Parity**: Ingestion endpoints seamlessly return HTTP 200 when no active subscribers are attached to a stream, preserving exact drop-in compatibility.
- **Re-Certified Performance**: Re-verified the full 20-step IIoT MQTT ladder (W1–W5 across 5k–100k msg/s) with 0.00% packet loss and up to 22.2% reduction in CPU utilization.

- Reliability documentation: [`docs/en_US/reliability/qualification_and_scope.md`](docs/en_US/reliability/qualification_and_scope.md)
- Docker image: `ankurkrp/rekuiper:0.501-beta` (plus `latest`)
- Helm chart: `deploy/chart/ekuiper` (`AppVersion: 0.501-beta`)

---

# Release Notes - rekuiper v0.500-beta

`rekuiper` v0.500-beta introduces an in-memory Redis-style catalog and zero-disk
hot path architecture. Stream definitions, table schemas, and active rules are
held in memory with synchronized concurrency (`MemoryCatalog`), completely
removing SQLite disk contention from stream ingestion and query dispatch.
SQLite backing files operate under tuned WAL pragmas for durable background
synchronization.

Authentication keys, configuration files, and database connection pools are now
cached on the hot path, and channel buffer depths have been scaled to 32,768
messages to eliminate thread backpressure under heavy burst workloads.

This release includes an exact 1k-resolution peak capacity benchmark under a
bounded Mosquitto broker (4,096-message queue limit). Certified sustained ceilings
reach 150,000 msg/s for telemetry filters and ESPHome topics, 200,000 msg/s for
per-device and vehicle windows, and 126,000 msg/s for EV charger sessions on a
single CPU core and 1 GiB of RAM.

- Benchmark and exact peak results: [`test/benchmark/iiot-mqtt/BENCHMARK-0.500.md`](test/benchmark/iiot-mqtt/BENCHMARK-0.500.md)
- Benchmark suite and harness: [`test/benchmark/iiot-mqtt`](test/benchmark/iiot-mqtt/README.md)
- Archived 0.426 benchmark: [`BENCHMARK-0.426.md`](test/benchmark/iiot-mqtt/BENCHMARK-0.426.md)
- Docker image: `ankurkrp/rekuiper:0.500-beta` (plus `latest`)
- Helm chart: `deploy/chart/ekuiper` (`AppVersion: 0.500-beta`)

---

# Release Notes - rekuiper v0.426-beta

`rekuiper` v0.426-beta makes delivery metrics reflect real writes. File-sink
records count only after a successful flush, MQTT source errors reach the rule
exception counter, and unsupported SQL sink URLs fail instead of reporting a
write. Default portable plugins, external services and JavaScript UDFs no
longer advertise runtimes that are not installed.

The release includes a bounded MQTT benchmark with an external Rust publisher,
a constant-memory Rust subscriber probe, exact sink proofs and raw evidence.
All five workloads sustained 100,000 messages/s for 120 seconds on one CPU core
and 1 GiB RAM; the ESPHome topic workload sustained 150,000 messages/s. The
200,000 messages/s target was tested but is not claimed.

- Benchmark and method: [`test/benchmark/iiot-mqtt`](test/benchmark/iiot-mqtt/README.md)
- Archived 0.425 comparison: [`ARCHIVE-0.425-COMPARISON.md`](test/benchmark/iiot-mqtt/ARCHIVE-0.425-COMPARISON.md)
- Docker image: `ankurkrp/rekuiper:0.426-beta` (plus `latest`)
- Helm chart: `deploy/chart/ekuiper` (`AppVersion: 0.426-beta`)

---

# Release Notes - rekuiper v0.425-beta

`rekuiper` v0.425-beta makes windowed analytics correct and memory-bounded, adds
`SESSIONWINDOW`, extends the MQTT source (binary, delimited and protobuf payloads,
`meta(topic)`, multiple topics, session options), replaces the per-record MQTT sink
client with a persistent connection, and adds an offline sink cache with resend.
See CHANGELOG for details.

Benchmark: [test/benchmark/iiot-mqtt](test/benchmark/iiot-mqtt/README.md) compares
rekuiper with eKuiper 2.4.1, Telegraf 1.40.0 and Redpanda Connect 4.109.0 over MQTT
under identical limits, with configs, raw evidence and reproduction steps.

- Docker image: `ankurkrp/rekuiper:0.425-beta` (plus `latest`)
- Helm chart: `deploy/chart/ekuiper` (`AppVersion: 0.425-beta`)

---

# Release Notes - rekuiper v0.424-beta

`rekuiper` v0.424-beta fixes every documented compatibility defect found in the
0.423 evaluation against official LF Edge eKuiper 2.4.1: PostgreSQL float
sinks, SQL polling sources and lookup joins, windowed stream-stream joins,
array index/slice syntax, `json_path_query` wildcards, ruletest SSE on the
documented port 10081 with lossless replay, process-restart delivery resume,
CLI parity (`describe`/`-f`/`export`/`import`/`gettopo`/`validate`), and the
Docker-default `restIp`. See CHANGELOG for the per-defect list.

Built by **[I-Dacs Labs](https://i-dacs.com)**. Released under the permissive
**MIT License** (and dual Apache-2.0).

- Docker image: `ankurkrp/rekuiper:0.424-beta` (plus `latest`)
- Helm chart: `deploy/chart/ekuiper` (`AppVersion: 0.424-beta`)
- Precompiled binary targets: Linux x86_64, Windows x86_64, macOS x86_64 & Apple Silicon aarch64.

---

# Release Notes - rekuiper v0.423-beta (history)

`rekuiper` v0.423-beta resolves dynamic rule activation on start/restart, supports nested `{ "content": "..." }` and stringified rule map imports for both ruleset and configuration endpoints, and passes the evaluation probe suite used at that time (scoped; see TEST-CONTRACT-AUDIT where applicable).

Released under the permissive **MIT License** (and dual Apache-2.0). No throughput or parity multipliers are claimed here; see BENCHMARK-AUDIT.md for the fair over-HTTP audit.

---

## Scoped evaluation fixes across documented root causes

### 1. Data Plane & Remote Ingestion Integrity (D1)
- Fixed MQTT source broker resolution: `MqttConfig` now treats `topic` as optional with `#[serde(default)]`, allowing topic-less source configuration keys (`CONF_KEY`) to properly decode remote broker addresses (e.g. `{"server": "tcp://broker:1883"}`).
- Added case-insensitive lookup for stream options (`SERVER`, `server`, `CONF_KEY`, `conf_key`, `DATASOURCE`, `topic`).
- Completely resolved Docker container connection drops (`Connection refused`), restoring 100% remote broker delivery.

### 2. Complete REST Resource Updates & Dynamic Patching (D2)
- Added missing HTTP `PUT` handlers on `/streams/:name`, `/tables/:name`, `/rules/:name`, and `/connections/:id`, eliminating all 405 Method Not Allowed errors.
- Added `PATCH /configs` supporting dynamic configuration merging returning `HTTP 204 No Content` matching upstream eKuiper.

### 3. Comprehensive Import/Export Lifecycle (D3)
- Added `POST /data/export` and `POST /v2/data/export` with selective rule filtering (`{"rules": ["r1", "r2"]}`) and automatic dependency extraction (dependent streams, tables, and connections).
- Expanded `/data/import` to accept rules in both array and map forms (`{"id": def}`) with automated ID injection.
- Formatted import responses to match upstream envelopes (`ConfigResponse` and count-based ruleset strings).

### 4. Authentic Plugin & Service Validation (D4)
- Replaced mock/dummy 200/201 responses with genuine filesystem and URL validation.
- Missing files (`file:///nonexistent/...`) now reject with `HTTP 400` and detailed stat errors.
- Unregistered plugins, missing portable status, and unregistered function symbols strictly return `HTTP 404 Not Found`.

### 5. Input & Syntax Validation Parity (D5 & D7)
- Rule creation and update now validate stream existence, returning `HTTP 400` when referencing missing streams.
- Unknown SQL functions in rules are rejected (`HTTP 400` on create, `HTTP 422` on validate).
- `validate_rule` returns eKuiper's JSON envelope: `{"sources": [...], "valid": true}`.
- Ruletest creation rejects empty/invalid SQL with `HTTP 400`.
- Stream management queries (`SHOW STREAMS`, `DESCRIBE STREAM`) are executed via `POST /streams` with `HTTP 201`.
- Added support for the `->` arrow operator syntax (`a->b`, `a->'b'`, `a->b->c`) alongside standard dot notation (`a.b`).

### 6. Schema & Field Definition Preservation (D6)
- Retained parsed `StreamFields` (`[{"Name": "...", "FieldType": "..."}]`) in stream and table definitions.
- `GET /streams/:name` and `GET /tables/:name` return full `StreamFields` metadata and options.
- `GET /streams/:name/schema` and `GET /tables/:name/schema` return typed field maps (`{"col": {"type": "...", "index": 0}}`).
- Missing stream/table describe returns eKuiper's structured JSON 400 error envelope.

### 7. PostgreSQL & Relational Data Plane (D8)
- Real PostgreSQL `$1...$n` parameterized sink writes via `sqlx`.
- Type-safe lookup joins supporting both SQLite and PostgreSQL with cast conversions.
- Implemented `SqlSource` background polling actor for streams with `TYPE="sql"`.
- Added 3-second connection timeout guard to prevent pipeline deadlocks on unreachable databases.

### 8. Enterprise RSA/JWT Authentication Guard (D9)
- Implemented strict auth middleware active when `basic.authentication: true`.
- Enforces RS256 token verification against `etc/mgmt/public.pem` or `etc/public.pem`.
- Enforces token expiration (`exp`) and raw JWT token formatting (strictly rejecting `Bearer ` prefixes per eKuiper specification).
- Exempts public discovery routes (`GET /` and `GET /ping`).

### 9. Graceful Process Termination (D10)
- `POST /stop` and `GET /stop` now trigger a clean delayed shutdown (`std::process::exit(0)`), verifying exit code 0.

### 10. Live Ruletest Server-Sent Events (SSE) Server (D11)
- `POST /ruletest` binds an HTTP server on the returned port, streaming live rule test records as `text/event-stream` (`data: [...]\n\n`).
- Clean shutdown on `DELETE /ruletest/:name`.

### 11. Packaging & Environment Overrides (D12)
- Added multipart form extraction for `PUT /schemas/:type/:name/upload`.
- Added full support for `KUIPER__<SECTION>__<KEY>` environment variable overrides.

---

## Performance notes (scoped)

- Fair over-HTTP source-to-sink behavior is reported in BENCHMARK-AUDIT.md (no multipliers claimed).
- No latency-histogram p99 is claimed.
- Rust has no GC pauses by construction; no comparative GC-latency numbers are claimed.

---

## 📦 Artifacts & Distribution

- Multi-stage Docker image: `ankurkrp/rekuiper:0.423-beta`
- Helm chart: `deploy/chart/ekuiper` (`AppVersion: 0.423-beta`)
- Precompiled binary targets: Linux x86_64, Windows x86_64, macOS x86_64 & Apple Silicon aarch64.
