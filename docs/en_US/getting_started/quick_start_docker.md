# Quick Start with Docker

This document describes how to run **rekuiper** in Docker. It also describes how to deploy your first real-time streaming rule.

---

## 1. Start rekuiper

Start rekuiper with all standard network ports exposed:

```shell
docker run -d \
  --name rekuiper \
  -p 9081:9081 \
  -p 20498:20498 \
  -p 20499:20499 \
  ankurkrp/rekuiper:1.0.0-rc3
```

Verify that the engine operates and responds:

```shell
curl http://localhost:9081/ping
```

**Expected response:**
```text
pong
```

---

## 2. Create a Data Stream

A **Stream** defines the incoming data schema and the protocol source. Create an in-memory stream named `demo`:

```shell
curl -X POST http://localhost:9081/streams \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "CREATE STREAM demo (temperature float, humidity bigint) WITH (DATASOURCE=\"demo\", FORMAT=\"JSON\")"
  }'
```

**Expected response:**
```text
Stream demo is created.
```

---

## 3. Deploy a Streaming Rule

A **Rule** defines continuous SQL logic. The rule processes incoming events and sends results to target actions (sinks).

Create a rule named `rule_high_temp`. This rule triggers when `temperature` is greater than `30.0`°C and writes results to the engine log:

```shell
curl -X POST http://localhost:9081/rules \
  -H "Content-Type: application/json" \
  -d '{
    "id": "rule_high_temp",
    "sql": "SELECT temperature, humidity FROM demo WHERE temperature > 30.0",
    "actions": [
      {
        "log": {}
      }
    ]
  }'
```

**Expected response:**
```text
Rule rule_high_temp was created
```

---

## 4. Send Test Telemetry

Send test sensor readings through the rekuiper HTTP push endpoint:

```shell
# 1. Send an event below the threshold (temperature = 22.0) - The engine filters this out
curl -X POST http://localhost:9081/streams/demo/data \
  -H "Content-Type: application/json" \
  -d '{"temperature": 22.0, "humidity": 45}'

# 2. Send an event above the threshold (temperature = 34.8) - The engine triggers the rule
curl -X POST http://localhost:9081/streams/demo/data \
  -H "Content-Type: application/json" \
  -d '{"temperature": 34.8, "humidity": 55}'
```

---

## 5. Verify the Execution

Check the real-time metrics of the rule:

```shell
curl http://localhost:9081/rules/rule_high_temp/status
```

**Output:**
```json
{
  "source_demo_0_records_in_total": 2,
  "source_demo_0_records_out_total": 2,
  "op_filter_0_records_in_total": 2,
  "op_filter_0_records_out_total": 1,
  "op_project_0_records_in_total": 1,
  "op_project_0_records_out_total": 1,
  "sink_log_0_records_in_total": 1,
  "sink_log_0_records_out_total": 1
}
```

Interpretation of metrics:
- `source_demo_0_records_in_total`: `2` (the stream received two events).
- `op_filter_0_records_out_total`: `1` (the filter rejected the lower temperature and accepted the higher temperature).
- `sink_log_0_records_out_total`: `1` (the sink recorded the alert).

Read the container logs to view the output:

```shell
docker logs rekuiper | grep "LOG SINK"
```

---

## 6. Interactive OpenAPI / Swagger UI

rekuiper embeds an interactive OpenAPI Swagger UI directly inside the engine daemon.

Open your browser to:
```text
http://localhost:9081/
```

From this interface, you can test REST endpoints, inspect stream schemas, create rules, and query runtime status interactively.

---

## Next Steps

- Read [Core Architecture and Design](../concepts/rekuiper.md).
- Read about [Windowing Functions](../sqls/windows.md) (Tumbling, Hopping, Sliding, Session).
- Configure [Production Deployments](../installation.md).
- Connect external [MQTT Brokers](../guide/sources/builtin/mqtt.md).

