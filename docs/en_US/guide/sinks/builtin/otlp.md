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

## Dead-Letter Queue (DLQ) Integration

The OTLP sink identifies permanent errors and transient errors:

- **Permanent Errors**: Responses with HTTP `400`, `401`, `403`, or `422`. The sink stops retries and sends the rejected record to the dead-letter queue under `DlqStage::SinkDelivery`.
- **Transient Errors**: Responses with HTTP `429`, `502`, `503`, `504`, or dropped network connections. The sink retries delivery according to the sink buffer policy.
