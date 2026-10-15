# Dead-Letter Queues (DLQ) and Error Routing

Invalid records occur during stream processing. Examples include malformed JSON, schema mismatches, division by zero, and sink delivery rejections.

rekuiper includes dead-letter queues (DLQ) to isolate and store invalid records without stopping the pipeline.

---

## Architecture Overview

The engine captures errors in five processing stages:

| Stage | Name | Description |
| :--- | :--- | :--- |
| `0` | `CodecDecode` | Payload decompression, UTF-8 validation, or JSON decoding failures. |
| `1` | `SchemaValidation` | Data type mismatches against declared stream schemas. |
| `2` | `SqlEval` | Math exceptions, invalid casts, or function evaluation failures. |
| `3` | `DataTemplate` | Template syntax errors or serialization failures during sink formatting. |
| `4` | `SinkDelivery` | Permanent client failures (such as HTTP `4xx` responses). |

Each error records this information:
* `rule_id`: Identifier of the rule.
* `stage`: The processing stage where execution failed.
* `error`: Error message text.
* `timestamp`: Unix millisecond timestamp.
* `raw_payload`: Source data bytes (limited to 4 KB).
* `tuple`: Intermediate record values before the failure.
* `sink_type`: Name of the failing sink plugin.

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

## Memory and Concurrency Controls

1. **Atomic Metrics**: Stage counters use atomic integers (`rekuiper_rule_dlq_records_total`). Reading metrics does not lock rule state.
2. **Payload Size Limit**: Corrupted input payloads are truncated to a maximum of 4,096 bytes.
3. **Non-Recursive Dispatch**: If an external DLQ sink fails, the engine increments `dlq_sink_failures` and stops. It does not recurse into the DLQ handler.

---

## Error Inspection

### Inspect Recent Errors

Retrieve recorded errors for a rule:

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

```text
# HELP rekuiper_rule_dlq_records_total total records routed to dead-letter queue by stage
# TYPE rekuiper_rule_dlq_records_total counter
rekuiper_rule_dlq_records_total{rule="rule_industrial_telemetry",stage="codec_decode"} 0
rekuiper_rule_dlq_records_total{rule="rule_industrial_telemetry",stage="schema_validation"} 1
rekuiper_rule_dlq_records_total{rule="rule_industrial_telemetry",stage="sql_eval"} 0
rekuiper_rule_dlq_records_total{rule="rule_industrial_telemetry",stage="data_template"} 0
rekuiper_rule_dlq_records_total{rule="rule_industrial_telemetry",stage="sink_delivery"} 0
```
