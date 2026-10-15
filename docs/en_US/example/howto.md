# Step-by-Step Guide: Running Cookbook Examples via CLI and REST API

This document describes how to execute cookbook examples with rekuiper using the `kuiper` CLI or the REST API. Before you begin, [install and start rekuiper](../installation.md).

---

## Data Preparation

Each cookbook example includes sample input data. You can supply custom data, or record live streaming telemetry to a local file through a rekuiper rule and replay that file during development and testing.

### Record Live Data to a File

rekuiper supports diverse source protocols and provides a high-throughput File sink. You can save live MQTT stream data to a local file using the `kuiper` CLI:

#### 1. Create the MQTT Stream

```bash
kuiper create stream mqttDemoStream '() WITH (TYPE="mqtt", DATASOURCE="demo/#")'
```

Or via the REST API:

```http
POST /streams
Content-Type: application/json

{
  "sql": "CREATE STREAM mqttDemoStream () WITH (TYPE=\"mqtt\", DATASOURCE=\"demo/#\")"
}
```

#### 2. Create the Recording Rule

Create a rule that writes incoming events to `data/mock.lines`:

```bash
kuiper create rule ruleRecordToFile '{
  "sql": "SELECT * FROM mqttDemoStream",
  "actions": [
    {
      "file": {
        "path": "data/mock.lines",
        "fileType": "lines",
        "hasHeader": false
      }
    }
  ]
}'
```

Or via the REST API:

```http
POST /rules
Content-Type: application/json

{
  "id": "ruleRecordToFile",
  "sql": "SELECT * FROM mqttDemoStream",
  "actions": [
    {
      "file": {
        "path": "data/mock.lines",
        "fileType": "lines"
      }
    }
  ]
}
```

#### 3. Verify Output File

The rule writes output lines to `data/mock.lines`. The file stores one JSON payload per line. Inspect the generated lines:

```bash
head -n 5 data/mock.lines
```

---

## Run Cookbook Examples

For testing and debugging, use the recorded File source as input. When the pipeline behaves as expected, replace the File source with your production MQTT or Kafka source.

### 1. Create the File Replay Stream

Create a stream pointing to `mock.lines`:

```bash
kuiper create stream demoStream '() WITH (TYPE="file", DATASOURCE="data/mock.lines", FORMAT="json")'
```

### 2. Subscribe to Output Topic

Open an MQTT client (such as Mosquitto CLI or MQTTX) and subscribe to the output topic:

```bash
mosquitto_sub -h 127.0.0.1 -t "result/ruleX"
```

### 3. Deploy the Rule

Submit the analytical rule with an MQTT sink:

```bash
kuiper create rule ruleX '{
  "sql": "SELECT * FROM demoStream WHERE temperature > 30",
  "actions": [
    {
      "mqtt": {
        "server": "tcp://127.0.0.1:1883",
        "topic": "result/ruleX"
      }
    },
    {
      "log": {}
    }
  ]
}'
```

### 4. Check Rule Metrics and Execution Status

Verify throughput and execution health:

```bash
kuiper getstatus rule ruleX
```

Or via REST:

```http
GET /rules/ruleX/status
```

---

## Summary

All cookbook scenarios can be executed reproducibly through the `kuiper` CLI or REST API. Adjust the SQL filter predicates and sink destinations to test custom edge analytics pipelines.
