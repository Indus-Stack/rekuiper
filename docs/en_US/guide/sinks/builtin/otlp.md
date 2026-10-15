# OpenTelemetry (OTLP) Action

The OpenTelemetry (OTLP) action sends SQL query results to OpenTelemetry collectors and OTLP/HTTP endpoints. It does not require separate proxy containers or message queues.

## Properties

| Property Name | Optional | Description |
|---|---|---|
| `endpoint` | false | Complete target URL (e.g. `http://collector:4318/v1/metrics` or `https://otlp-gateway.internal/v1/logs`). |
| `signal` | true | Telemetry signal type: `"metrics"`, `"logs"`, or `"traces"`. If omitted, auto-detects from record `signal` or defaults to `"metrics"`. |
| `headers` | true | Key-value map of custom HTTP headers (e.g. API tokens, tenant headers, authorization). |
| `timeout` | true | HTTP request timeout in milliseconds. Default: `3000`. |

Other common sink properties are supported. Refer to [sink common properties](../overview.md#common-properties) for more information.

## Export Envelopes

The OTLP sink automatically wraps stream records into standard OpenTelemetry ProtoJSON resource envelopes:

- **Metrics**: Formats as `resourceMetrics` with `scopeMetrics`, mapping SQL record `name`, `value`, `unit`, `timestamp_unix_nano`, `resource`, and `attributes` into standard `NumberDataPoint` structures.
- **Logs**: Formats as `resourceLogs` with `scopeLogs`, wrapping `body`, `severity_number`, `severity_text`, `trace_id`, and `span_id`.
- **Traces**: Formats as `resourceSpans` with `scopeSpans`, populating span `trace_id`, `span_id`, `name`, `kind`, and `status`.

## Configuration Example

Export high-frequency aggregated machine metrics to an enterprise OpenTelemetry Collector:

```json
{
  "id": "rule_export_chiller_telemetry",
  "sql": "SELECT 'metric' AS signal, 'edge.chiller.temp_mean' AS name, AVG(temperature) AS value, 'Cel' AS unit, COUNT(*) AS sample_count FROM plc_stream GROUP BY TumblingWindow(ss, 5)",
  "actions": [
    {
      "otlp": {
        "endpoint": "http://otel-collector.prod:4318/v1/metrics",
        "signal": "metrics",
        "headers": {
          "Authorization": "Bearer edge-prod-token-xyz",
          "X-Scope-OrgID": "tenant-chiller-eu"
        },
        "timeout": 5000
      }
    }
  ],
  "options": {
    "onError": "dlq",
    "dlq": {
      "type": "memory",
      "topic": "dlq/otlp_export_errors"
    }
  }
}
```

## Error Handling & Dead-Letter Queue (DLQ)

The OTLP sink distinguishes between transient transport glitches and permanent protocol rejections:

- **Permanent Failures**: Responses with HTTP `400 Bad Request`, `401 Unauthorized`, `403 Forbidden`, or `422 Unprocessable Entity` indicate invalid credentials or schema incompatibility. rekuiper halts retrying immediately and routes the rejected record to the Dead-Letter Queue under `DlqStage::SinkDelivery`.
- **Transient Failures**: Responses with HTTP `429 Too Many Requests`, `502 Bad Gateway`, `503 Service Unavailable`, `504 Gateway Timeout`, or network connection drops trigger retry backoff according to the rule's sink buffer policy.
