# Dead-Letter Queues (DLQ) & Poison-Pill Routing

In industrial stream processing deployments, sensors and external publishing systems frequently emit corrupted binary streams, truncated JSON payloads, out-of-spec types, or unexpected mathematical domain inputs (`NaN` or division by zero). Furthermore, downstream enterprise databases and REST APIs can reject malformed rows with HTTP `400 Bad Request` client errors.

Without poison-pill isolation, a single malformed event causes either:
1. **Head-of-Line Blocking**: Endless sink retry loops stall the entire stream pipeline.
2. **Silent Data Loss**: Unaccounted drops violate regulatory audit standards.

rekuiper introduces **Dead-Letter Queues (DLQ) and Multi-Stage Poison-Pill Routing**, providing bounded in-memory error retention, non-recursive external sink dispatch, zero-lock Prometheus metrics, and automated pipeline halt policies.

---

## Architecture Overview

rekuiper isolates failures across **5 discrete pipeline execution stages**:

| Stage | Name | Description |
| :--- | :--- | :--- |
| `0` | `CodecDecode` | Low-level payload decompression, UTF-8 validation, or JSON/Protobuf/Delimited decoding failures. |
| `1` | `SchemaValidation` | Ingress type-checking failures against declared `CREATE STREAM (col type, ...)` definitions. |
| `2` | `SqlEval` | Mathematical exceptions, casting mismatches, or runtime evaluation panics in SQL expressions. |
| `3` | `DataTemplate` | Template syntax errors, missing context variables, or serialization issues during sink formatting. |
| `4` | `SinkDelivery` | Permanent client failures (e.g. HTTP `4xx` client rejections, excluding retryable `408`/`429`). |

Each error captures a structured `DlqEnvelope`:
* `rule_id`: Identifier of the rule where the failure originated.
* `stage`: The `DlqStage` variant where execution failed.
* `error`: Descriptive error diagnostic.
* `timestamp`: Unix millisecond timestamp when the exception was recorded.
* `raw_payload`: Original source bytes or string (capped to 4 KB).
* `tuple`: Intermediary row state before the failing expression or action.
* `sink_type`: Name of the failing sink plugin (when stage is `SinkDelivery` or `DataTemplate`).

---

## Configuration

DLQ behavior is configured per rule under the `options` block:

```json
{
  "id": "rule_industrial_telemetry",
  "sql": "SELECT id, temp, abs(dev) AS dev_norm FROM sensor_stream",
  "actions": [
    {
      "rest": {
        "url": "http://api.internal.local/v1/telemetry",
        "method": "POST"
      }
    }
  ],
  "options": {
    "onError": "dlq",
    "dlq": {
      "bufferCapacity": 100,
      "includeRawPayload": true,
      "action": {
        "rest": {
          "url": "http://audit.internal.local/v1/dlq_inbox",
          "method": "POST"
        }
      }
    }
  }
}
```

### Options Specification

* `onError`: Determines rule behavior upon encountering a poison pill:
  * `"drop"` *(default)*: Drops the invalid event, records exceptions in rule metrics, and nullifies incompatible schema fields (baseline behavior).
  * `"dlq"`: Isolates the malformed record, routes the `DlqEnvelope` to the in-memory ring buffer, and dispatches to any configured external DLQ action.
  * `"halt"`: Immediately records the error to DLQ and stops the rule actor cleanly. The rule status transitions to `"stopped"` and preserves the causal diagnostic message.
* `dlq.bufferCapacity`: Bounded size of the in-memory circular ring buffer (default: `100` entries).
* `dlq.includeRawPayload`: When `true` (default), preserves the incoming message in `raw_payload`.
* `dlq.action`: Optional external sink configuration (e.g. `rest`, `mqtt`, `file`, or `log`) for offloading poison pills to enterprise audit stores.

---

## Edge Gateway Memory & Concurrency Protections

rekuiper is engineered for constrained edge gateways (256 MB RAM) and high-throughput streaming workloads:

1. **Lock-Free Hot-Path Metrics**: Stage counters are backed by `AtomicU64` primitives (`rekuiper_rule_dlq_records_total`). Scraping `/metrics` or reading status requires zero state locking.
2. **Strict 4 KB Payload Capping**: Incoming corrupt payloads (even multi-megabyte payloads) are truncated to at most **4,096 bytes** (UTF-8 character-boundary safe, suffixed with `"…[truncated]"`). A full 100-entry ring buffer consumes $\le 400\text{ KB}$ RAM.
3. **Non-Recursive, Non-Blocking External Dispatch**: External DLQ dispatch runs asynchronously with recursion disabled (`dlq_router: None`). If an external DLQ sink target fails, rekuiper increments `dlq_sink_failures` without recursing into the DLQ router.

---

## HTTP REST & CLI Diagnostic Inspection

### Inspecting Errors

Retrieve recent poison-pill records and error stage distribution:

```bash
# REST API
curl -s http://127.0.0.1:9081/rules/rule_industrial_telemetry/errors | jq .

# CLI Equivalent
rekuiper errors rule_industrial_telemetry
# or alias
rekuiper dlq rule_industrial_telemetry
```

**Example Response**:
```json
{
  "rule_id": "rule_industrial_telemetry",
  "total_errors": 1,
  "codec_decode": 0,
  "schema_validation": 1,
  "sql_eval": 0,
  "data_template": 0,
  "sink_delivery": 0,
  "sink_failures": 0,
  "recent_errors": [
    {
      "rule_id": "rule_industrial_telemetry",
      "stage": "SchemaValidation",
      "error": "Field 'temp' expected type 'float', got: \"invalid_float\"",
      "timestamp": 1728950400000,
      "tuple": {
        "id": 101,
        "temp": "invalid_float",
        "dev": "sensor_A"
      }
    }
  ]
}
```

### Clearing the Error Ring Buffer

Operational clearing of the in-memory DLQ buffer is permitted even when the engine runs in read-only Declarative GitOps Mode:

```bash
# REST API
curl -s -X DELETE http://127.0.0.1:9081/rules/rule_industrial_telemetry/errors

# CLI Equivalent
rekuiper errors rule_industrial_telemetry --clear
# or alias
rekuiper dlq rule_industrial_telemetry --clear
```

---

## Prometheus Monitoring

DLQ counters are exposed on the standard Prometheus scraping endpoint (`GET /metrics`):

```prometheus
# HELP rekuiper_rule_dlq_records_total total records routed to dead-letter queue by stage
# TYPE rekuiper_rule_dlq_records_total counter
rekuiper_rule_dlq_records_total{rule="rule_industrial_telemetry",stage="codec_decode"} 0
rekuiper_rule_dlq_records_total{rule="rule_industrial_telemetry",stage="schema_validation"} 1
rekuiper_rule_dlq_records_total{rule="rule_industrial_telemetry",stage="sql_eval"} 0
rekuiper_rule_dlq_records_total{rule="rule_industrial_telemetry",stage="data_template"} 0
rekuiper_rule_dlq_records_total{rule="rule_industrial_telemetry",stage="sink_delivery"} 0
```
