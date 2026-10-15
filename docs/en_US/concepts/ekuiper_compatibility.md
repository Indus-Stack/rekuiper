# eKuiper Compatibility & Architectural Enhancements

`rekuiper` is designed as a high-performance, drop-in compatible stream processing engine for eKuiper workloads. While `rekuiper` strives for 100% wire and SQL syntax parity with eKuiper, certain intentional design choices and behavioral enhancements deviate from legacy eKuiper implementations.

Where legacy eKuiper behavior violates standards, compromises system stability, or masks operational failures, `rekuiper` adopts modern, robust engineering practices. This document details each intentional difference, the architectural rationale behind it, and links to the relevant subsystem documentation.

---

## Superiority & Compatibility Matrix

| Subsystem | Legacy eKuiper Behavior | rekuiper Enhanced Behavior | Architectural Rationale | Documentation Page |
| :--- | :--- | :--- | :--- | :--- |
| **CLI Exit Codes** | Returns exit code `0` even on operational or syntax failure | Returns non-zero (`1` or `2`) on failure; `0` on success | Adheres to POSIX/UNIX standards; prevents silent failures in CI/CD pipelines, Docker health checks, and `set -e` scripts | [CLI Overview](../api/cli/overview.md#process-exit-codes--scripting-automation) |
| **REST Status Codes** | Returns `400 Bad Request` for missing resources (e.g. connections, schemas) | Returns `404 Not Found` for missing resources; `400` only for malformed payloads | Adheres to RFC 9110 REST semantics; allows standard HTTP clients and SDKs to differentiate missing resources from syntax errors | [REST API Overview](../api/restapi/overview.md#http-standards-compliance--status-codes) |
| **Execution Explain** | Returns unstructured ASCII text dump (`ProjectPlan_0 -> ...`) | Returns typed, structured JSON execution plan | Machine-readable; enables automated inspection by web visualizers, API gateways, and AI/MCP agents | [Rules REST API](../api/restapi/rules.md#explain-rule-execution-plan) |
| **String & Math Safety** | Panics on slice out-of-bounds in `substring`; crashes on negative codepoints in `chr` | Safe boundary clamping for `substring`; returns `null` for invalid `chr` inputs | Memory safety; eliminates runtime crashes in unattended edge environments | [String Functions](../sqls/functions/string_functions.md#substring) |
| **Datetime Functions** | Suffers timezone drift in `date_diff`; conflicting 0-based vs 1-based `day_of_week` indexing | Strict UTC normalization in `date_diff`; strictly conforms to SQL standard 1-based `day_of_week` | Deterministic timestamp calculation across timezones and daylight saving transitions | [Datetime Functions](../sqls/functions/datetime_functions.md#date_diff) |
| **Array & Decode Safety** | Reflection panic on heterogeneous arrays in `array_contains`; panics on unhandled decode | Non-panicking JSON comparison; null-safe branch evaluation in `decode` | Guarantees continuous 24/7 stream execution despite malformed or polymorphic telemetry data | [Array Functions](../sqls/functions/array_functions.md#array_contains) |
| **Sink Data Templates** | Silently passes malformed templates at creation, failing at runtime with `<no value>` | Upfront syntax and brace validation during `POST /rules` creation | Immediate developer feedback; prevents corrupted templates from deploying to production pipelines | [Data Templates](../guide/sinks/data_template.md#golang-template-overview) |
| **Prometheus Scraping** | Exposes `/metrics` solely on dedicated port 20499; returns 404 on REST port 9081 | Serves `/metrics` on both primary REST port 9081 and dedicated port | Cloud-native convention; simplifies Kubernetes pod scraping without multi-port ingress complexity | [Prometheus Monitoring](../operation/usage/monitor_with_prometheus.md#prometheus-metrics) |
| **Standard Error Envelopes** | Inconsistent error schemas (`plain text`, varying string keys, missing `Content-Type: application/json`) | Structured, uniform JSON error envelopes (`{"error": <code>, "message": "..."}` or `{"message": "..."}`) | Guarantees machine-parsability for API gateways, SDKs, and observability pipelines | [REST API Overview](../api/restapi/overview.md#error-handling) |
| **Idempotent Resource Lifecycle** | Inconsistent status codes on resource updates; raw plain text errors on mutation failures | RFC 9110 compliant idempotent updates (`PUT` returning `200 OK`) and explicit `404 Not Found` on missing resources | Simplifies GitOps, automated state synchronization, and declarative deployments | [REST API Overview](../api/restapi/overview.md#idempotent-operations) |
| **Import & Merge Semantics** | Inconsistent wipe vs merge behavior across versions | Defaults to full state reset (`reset_configuration`), wiping existing unreferenced streams/rules; supports `partial=1` merge mode | Parity with eKuiper declarative configuration management; guarantees clean restoration while supporting additive sync | [REST API Overview](../api/restapi/overview.md#configuration-import-and-export) |
| **Prometheus Metrics Taxonomy** | Legacy 8-metric export lacking detailed operator/source/sink breakdown and latency histograms | Full 32 metric families with multidimensional labels (`op`, `op_instance`, `type`, `rule`, `name`) and histograms, alongside backward-compatible series | Native compatibility with eKuiper Grafana dashboards and Prometheus Operator alert definitions | [Prometheus Monitoring](../operation/usage/monitor_with_prometheus.md#prometheus-metrics) |
| **Import Payload Validation** | Accepts empty payloads; starts background tasks that do nothing | Validates payloads before processing; returns `400 Bad Request` for empty payloads | Prevents task table pollution; gives immediate error feedback to the client | [Data REST API](../api/restapi/data.md#asynchronous-data-import) |

---

## Detailed Rationale by Subsystem

### 1. POSIX Process Exit Codes (CLI)
- **Difference**: When an operation fails in legacy eKuiper's CLI (e.g. `kuiper create rule bad_rule '{invalid json}'`), the process exits with status code `0`. In `rekuiper`, the CLI exits with code `1` on operational failures or `2` on argument/usage errors.
- **Why we chose this**: In UNIX environments, shell scripts running under `set -e` or CI/CD pipelines check `$?` to determine step success. Returning `0` silently masks critical deployment errors, allowing failing deployments to proceed undetected. Standard exit codes guarantee reliable automation.

### 2. HTTP RFC 9110 Status Codes (REST API)
- **Difference**: Legacy eKuiper returns `400 Bad Request` when querying or deleting a non-existent connection (`GET /connections/unknown_id`) or schema. `rekuiper` returns `404 Not Found`.
- **Why we chose this**: RFC 9110 establishes that `400 Bad Request` denotes client syntax or validation errors in the request message, while `404 Not Found` indicates the target resource does not exist. Caching proxies, reverse proxies, and client SDKs depend on `404` to trigger idempotent recreation logic.

### 3. Structured Execution Plans (`explain`)
- **Difference**: Legacy eKuiper emits raw text formatted for terminal human viewing (`ProjectPlan_0 -> FilterPlan_1`). `rekuiper` emits a structured JSON document representing nodes, operators, and parameters.
- **Why we chose this**: Modern architectures interact with stream processing engines programmatically. Structured JSON execution plans enable web consoles, topology visualizers, and LLM/MCP assistants to parse and optimize query plans without fragile text scraping.

### 4. Memory Safety & Panic Elimination in SQL Functions
- **Difference**: 
  - `substring(str, start, len)`: Safely clamps slice indices within string boundaries instead of triggering slice index panics in the Go runtime.
  - `chr(code)`: Validates codepoint ranges (0 to 127 for ASCII, valid Unicode scalar values) and returns `null` for invalid or negative integers rather than crashing the process.
  - `date_diff(d1, d2)`: Normalizes all operands to UTC via `chrono`, avoiding timezone offset drift.
  - `day_of_week(d)`: Conforms strictly to standard SQL where Sunday = 1 through Saturday = 7.
  - `array_contains(arr, val)`: Safely compares JSON values without Go reflection panics on mixed-type arrays.
- **Why we chose this**: Edge streaming gateways operate 24/7, often in remote or inaccessible physical installations. Engine crashes or unhandled panics halted by unexpected sensor payloads cause telemetry outages. Safe fallbacks (`null`) preserve continuous pipeline operation.

### 5. Upfront Sink Template Validation
- **Difference**: In legacy eKuiper, unclosed braces or invalid actions in a sink `dataTemplate` pass rule creation without warning, later emitting `<no value>` or dropping messages during execution. `rekuiper` parses and validates template syntax upon rule creation (`POST /rules`).
- **Why we chose this**: Catching configuration errors at rule registration time gives instant feedback to operators, preventing faulty rules from ever reaching production execution.

### 6. Cloud-Native Prometheus Metric Scraping
- **Difference**: Legacy eKuiper requires opening a secondary port (`20499`) for Prometheus and returns `404 Not Found` on `GET http://localhost:9081/metrics`. `rekuiper` serves Prometheus metrics on the primary HTTP port (`9081`) in addition to any configured standalone port.
- **Why we chose this**: In Kubernetes clusters, single-port pod scraping minimizes container port specifications, avoids network policy clutter, and aligns with standard Prometheus Operator service monitors.

### 7. Uniform JSON Error Envelopes
- **Difference**: Legacy eKuiper in several endpoints returns raw string bodies without standard JSON wrappers, or varies between unstructured plain text error strings and JSON without proper `Content-Type: application/json` headers. `rekuiper` guarantees that all error responses conform to standardized JSON envelopes with either `{"error": <code>, "message": "..."}` (for subsystem errors with numerical codes, e.g. rule error 1000, stream error 3000) or `{"message": "..."}` (for general HTTP errors).
- **Why we chose this**: Production edge clusters integrate with API gateways, microservice orchestrators, and automated SDKs. Unstructured plaintext responses break automated error decoding and log parsing. Uniform JSON payloads ensure seamless client-side serialization and machine-parseable diagnostics.

### 8. RFC 9110 Idempotent Updates and Predictable Lifecycle
- **Difference**: In legacy eKuiper, certain resource mutations or uploads use ad-hoc status codes. `rekuiper` strictly adheres to RFC 9110 §9.3.4 for `PUT` operations (such as `/schemas/.../upload`), returning `200 OK` on successful updates, and adheres to predictable lifecycle semantics (returning `404 Not Found` when acting on non-existent rules or schemas, and idempotent `200 OK` when deleting uploaded files).
- **Why we chose this**: Declarative configuration management and GitOps workflows (e.g. Terraform, Kubernetes Operators, edge fleet controllers) rely on standard HTTP idempotent semantics to converge system state without throwing spurious client errors on convergent reconcile loops.

### 9. Declarative State Management (Import & Merge Mode)
- **Difference**: By default, `POST /data/import` and `kuiper import` perform a complete state reset (`reset_configuration`), terminating running background pipelines and purging unreferenced streams, tables, and rules before applying the imported configuration. When `partial=1` or `partial=true` is specified, `rekuiper` switches to additive merge mode, updating only the entities declared in the payload without dropping unreferenced resources.
- **Why we chose this**: In edge fleet orchestration, operators often push canonical configuration packages that must completely replace current device state to eliminate drifted rules and memory leaks. At the same time, supporting `partial=1` enables granular incremental provisioning for dynamic microservices.

### 10. Comprehensive 32-Family Prometheus Metrics Taxonomy
- **Difference**: Legacy eKuiper exports a subset of basic counters without granular operator-level breakdown or histogram distributions in earlier revisions. `rekuiper` exports all 32 official Prometheus metric families—including source, operator, and sink records in/out, exceptions, message processed totals, buffer lengths, connection statuses, process latencies, and Prometheus histogram representations (`_bucket`, `_count`, `_sum`) with multidimensional labels (`op`, `op_instance`, `type`, `rule`, `name`, `status`, `le`). Furthermore, legacy rule-only series are simultaneously retained.
- **Why we chose this**: Industrial edge deployments rely heavily on Grafana dashboards designed for eKuiper and Prometheus alerting rules that measure operator latency percentiles (P95, P99) and queue buffer pressures. Providing complete metric families and histograms enables turnkey observability without altering dashboard queries.

### 11. Upfront Payload Validation for Data Import
- **Difference**: Legacy systems accept empty request bodies on `/async/data/import`. The server creates a task identifier and starts a background task. The background task does not do any work. `rekuiper` validates the request body immediately at the HTTP boundary. If the payload is empty or invalid JSON, `rekuiper` returns status code `400 Bad Request` with an error message (`{"message": "configuration unmarshal with error: empty payload"}`).
- **Why we chose this**:
  - Do not create background tasks for empty or invalid requests.
  - Return errors to the client immediately.
  - Prevent clients from waiting and polling for empty tasks.
  - Save memory and processor resources.



