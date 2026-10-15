# OpenTelemetry (OTLP) Source Connector

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>
<span style="background:green;color:white;padding:1px;margin:2px">otlp/http json</span>
<span style="background:green;color:white;padding:1px;margin:2px">zero collector</span>

The OpenTelemetry (OTLP) source connector receives telemetry data from OpenTelemetry SDKs, collectors, and agents. The connector runs in pure Rust without external dependencies.

rekuiper processes telemetry directly. It filters, aggregates, and detects anomalies in telemetry streams without separate collector containers.

## Ingestion Modes

rekuiper supports two ingestion modes:

1. **Server Endpoints (Default)**:
   External agents push OTLP JSON data to the rekuiper HTTP service:
   - `POST /v1/metrics`
   - `POST /v1/logs`
   - `POST /v1/traces`
   - `POST /otlp/v1/metrics`, `POST /otlp/v1/logs`, `POST /otlp/v1/traces`

   The engine routes incoming records to any active stream that uses `TYPE="otlp"`. In declarative GitOps mode, these ingestion routes stay open for data.

2. **Dedicated Network Listener**:
   A stream can bind a dedicated TCP/HTTP port (for example, `0.0.0.0:4318`).

## Stream DDL Syntax

Create an OTLP telemetry stream in SQL:

```sql
CREATE STREAM otlp_telemetry () WITH (
    TYPE="otlp",
    BIND="0.0.0.0:4318"
)
```

If omitting `BIND`, the stream consumes records pushed directly to the central HTTP server (`/v1/*`).

## Normalized Schemas

All incoming OTLP batches (`resourceMetrics`, `resourceLogs`, `resourceSpans`) are unpacked and flattened into individual SQL records:

### 1. Metrics (`signal = "metric"`)

| Field | Type | Description |
|---|---|---|
| `signal` | string | `"metric"` |
| `name` | string | Metric name (e.g. `"system.cpu.utilization"`, `"http_requests_total"`). |
| `type` | string | Metric kind: `"gauge"`, `"sum"`, `"histogram"`, `"summary"`. |
| `unit` | string | Metric unit (e.g. `"By"`, `"ms"`, `"1"`). |
| `value` | float/int | Data point value (`asDouble` or `asInt`). |
| `timestamp_unix_nano` | int | Nanosecond timestamp of the data point. |
| `resource` | object | Extracted resource attributes (e.g. `{"service.name": "edge-chiller", "host.id": "node-1"}`). |
| `attributes` | object | Data point attributes (e.g. `{"device": "eth0", "state": "idle"}`). |

### 2. Logs (`signal = "log"`)

| Field | Type | Description |
|---|---|---|
| `signal` | string | `"log"` |
| `body` | any | Log message body or structured object. |
| `severity_number` | int | OTel severity number (`1..=24`). |
| `severity_text` | string | OTel severity label (e.g. `"INFO"`, `"ERROR"`, `"WARN"`). |
| `timestamp_unix_nano` | int | Log emission timestamp in nanoseconds. |
| `trace_id` | string | Hexadecimal trace identifier (if present). |
| `span_id` | string | Hexadecimal span identifier (if present). |
| `resource` | object | Resource attributes map. |
| `attributes` | object | Log record attributes map. |

### 3. Traces (`signal = "span"`)

| Field | Type | Description |
|---|---|---|
| `signal` | string | `"span"` |
| `name` | string | Span operation name (e.g. `"/api/v1/checkout"`). |
| `kind` | string | Span kind (`"internal"`, `"server"`, `"client"`, `"producer"`, `"consumer"`). |
| `trace_id` | string | Hexadecimal trace ID. |
| `span_id` | string | Hexadecimal span ID. |
| `parent_span_id` | string | Hexadecimal parent span ID (if child span). |
| `duration_nano` | int | Span duration calculated from `endTimeUnixNano - startTimeUnixNano`. |
| `status_code` | string | Status: `"ok"`, `"error"`, or `"unset"`. |
| `status_message` | string | Error or status description. |
| `resource` | object | Resource attributes map. |
| `attributes` | object | Span attributes map. |

## ProtoJSON Specification Compatibility

According to the official OpenTelemetry ProtoJSON specification, 64-bit integer fields (`timeUnixNano`, `startTimeUnixNano`, `endTimeUnixNano`, and `asInt`) are encoded as JSON string literals (e.g. `"asInt": "100"`) by official OpenTelemetry Collectors and SDKs.

rekuiper automatically accepts **both** stringified numbers (`"42"`) and native numeric JSON literals (`42`) seamlessly without requiring pre-transformation.

## Anomaly Detection & Stream Processing Examples

### High CPU Metric Anomaly Detection

```sql
SELECT
    resource->service.name AS service,
    name,
    AVG(value) AS avg_cpu
FROM otlp_telemetry
WHERE signal = 'metric' AND name = 'system.cpu.utilization'
GROUP BY resource->service.name, name, TumblingWindow(ss, 10)
HAVING avg_cpu > 0.85
```

### Error Log Aggregation & Alerting

```sql
SELECT
    resource->service.name AS service,
    severity_text,
    body,
    COUNT(*) AS error_count
FROM otlp_telemetry
WHERE signal = 'log' AND severity_text = 'ERROR'
GROUP BY resource->service.name, severity_text, body, SlidingWindow(ss, 30)
HAVING error_count >= 5
```

## Dead-Letter Queue (DLQ) & Poison-Pill Routing

When an invalid or malformed payload is submitted to `POST /v1/{metrics,logs,traces}` or the standalone OTLP listener:
1. An HTTP `400 Bad Request` is returned to the client.
2. If any running rule uses `onError: "dlq"`, a synthetic error envelope (`{"__raw_error__": "...", "__raw_payload__": "..."}`) is emitted to the target stream.
3. The rule engine intercepts this record and stores it in the Dead-Letter Queue under `DlqStage::CodecDecode`.
