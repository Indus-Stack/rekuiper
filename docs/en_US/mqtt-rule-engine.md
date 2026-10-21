---
title: MQTT Rule Engine in Rust — Architecture, SQL Syntax & Benchmarks
description: Technical architecture, streaming SQL rule syntax, and verified single-core benchmarks of rekuiper, a single-binary MQTT rule engine written in Rust.
keywords: MQTT Rust rule engine, MQTT rule engine, streaming SQL processor, IoT edge rule engine, MQTT stream processing
---

# MQTT Rule Engine in Rust — Architecture, SQL Syntax & Benchmarks

An **MQTT rule engine** processes, transforms, and routes MQTT message streams in real time. Rather than sending all raw sensor messages to a central cloud, the rule engine runs directly on the edge gateway. It evaluates SQL rules as each message arrives, sends alerts in sub-milliseconds, and summarizes telemetry before transmitting it upstream.

`rekuiper` is a single-binary MQTT rule engine and streaming SQL processor written in **Rust**. It provides deterministic tail latency, a flat 4.4 to 10.2 MiB heap memory profile under load, and sustained throughput of 150,000 to 200,000 messages per second on a single physical CPU core.

---

## 1. Architecture Overview

The following diagram illustrates how messages flow from MQTT publishers through the rekuiper engine to target destinations:

![rekuiper MQTT Rule Engine Architecture](/diagrams/mqtt-rule-engine-architecture.svg)

### Pipeline Stages

1. **Network Ingestion Actor**: Built on `rumqttc`, the ingestion layer manages persistent TCP/TLS connections to one or more MQTT brokers (MQTT v3.1.1 and v5.0). It subscribes to topic hierarchies with single-level (`+`) and multi-level (`#`) wildcards.
2. **Bounded Channel Ring Buffers**: Ingested messages enter fixed-capacity ring buffers. When downstream processing slows down, bounded channels apply backpressure upstream, preventing unbounded heap growth and out-of-memory crashes.
3. **Zero-Copy Stream Evaluator**: The SQL engine evaluates `WHERE` predicates directly on borrowed payload bytes. Numeric operations evaluate through CPU register fast-paths (`try_eval_numeric_fast`) without intermediate string allocations.
4. **Stateful Window Aggregator**: Messages flow into time-based or count-based windows (tumbling, sliding, hopping, or session windows). Window state stays in compact native memory structures.
5. **Store-and-Forward Sink Cache**: Output actions write results to MQTT topics, local databases, or HTTP endpoints. If the network link disconnects, an integrated disk-backed cache stores results and replays them in order upon reconnection.

---

## 2. Architectural Comparison Matrix

Edge engineers often evaluate several categories of tools for MQTT telemetry pipelines. The table below compares their architectural characteristics:

| Dimension | `rekuiper` (Rust) | Akasa / rumqttd (Rust) | Arroyo / RisingWave (Rust) | eKuiper (Go) | Node-RED (Node.js) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Primary Category** | **Edge MQTT SQL Rule Engine** | MQTT Broker | Distributed Streaming DB | Edge SQL Rule Engine | Visual Flow Wiring |
| **Deployment Model** | **Single static binary (~30 MB)** | Standalone Broker (~15 MB) | Multi-node Cloud Cluster | Binary (~65 MB) + Go Runtime | Node.js runtime + npm modules |
| **Rule Language** | **Standard ANSI SQL (221 functions)** | ❌ None (Topic match only) | Standard SQL | SQL (~170 functions) | JavaScript function nodes |
| **Stateful Time Windows** | **Yes (Tumbling, Sliding, Session)**| ❌ None | Yes | Yes | Custom manual state |
| **Single-Core MQTT Throughput** | **150,000–200,000 msg/s** | High (Broker only, no SQL) | *Cloud cluster only* | ~20,000 msg/s (loss > 50k) | 2,000–5,000 msg/s |
| **Heap Memory Profile** | **4.4–10.2 MiB (Flat plateau)** | ~15–30 MiB (Broker only) | Varies (S3 / State store) | 535–961.7 MiB (Spikes) | 120–350 MiB |
| **Garbage Collection (GC)** | **Zero GC (Deterministic Rust)** | Zero GC | Zero GC | Go GC pauses & drain lag | V8 GC sweeps |
| **Embedded Zero-Knowledge UI**| **Built-in Web Console (`/ui`)** | ❌ None | Separate web console | External manager | Built-in visual editor |
| **AI Tooling Integration** | **Native MCP Server (57 tools)** | ❌ None | ❌ None | ❌ None | Community nodes |

---

## 3. Writing an MQTT SQL Rule

Deploying an MQTT rule requires two steps: defining an input stream and creating a rule.

### Step 1: Create an MQTT Stream

Register a stream that subscribes to device telemetry topics:

```bash
curl -X POST http://localhost:9081/streams \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "CREATE STREAM devices_stream () WITH (DATASOURCE=\"devices/+/telemetry\", FORMAT=\"json\", TYPE=\"mqtt\", CONF_KEY=\"default\")"
  }'
```

### Step 2: Define the SQL Rule with Windowing & Sink Routing

This rule performs three operations:
1. Filters for temperature readings greater than 80.0 degrees Celsius.
2. Aggregates readings into 10-second tumbling windows grouped by device ID.
3. Routes summarized alerts to an MQTT alert topic and logs them locally.

```bash
curl -X POST http://localhost:9081/rules \
  -H "Content-Type: application/json" \
  -d '{
    "id": "high_temperature_window_alert",
    "sql": "SELECT device_id, avg(temperature) AS avg_temp, max(humidity) AS max_humidity, count(*) AS sample_count FROM devices_stream WHERE temperature > 80.0 GROUP BY device_id, TUMBLINGWINDOW(ss, 10)",
    "actions": [
      {
        "mqtt": {
          "server": "tcp://127.0.0.1:1883",
          "topic": "alerts/high_temperature",
          "sendSingle": true
        }
      },
      {
        "log": {}
      }
    ]
  }'
```

---

## 4. Single-Core Benchmark Measurements

All benchmarks run inside Docker containers constrained to **one physical CPU core** and **1 GiB of RAM** on Linux kernel 6.x. Sinks write all output to disk to prove zero message loss and exact calculations.

| Workload | Ingestion Rate | `rekuiper` Loss Rate | Memory (Heap) | Go eKuiper Loss Rate | eKuiper Memory |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Telemetry Filter** (W1) | 50,000 msg/s | **0.00%** | **4.4 MiB** | 40.8% loss | 15.4 MiB |
| **Telemetry Filter** (W1) | 100,000 msg/s | **0.00%** | **4.6 MiB** | 94.9% loss | 15.8 MiB |
| **Device Windows** (W2) | 20,000 msg/s | **0.00%** | **6.4 MiB** | 0.00% loss | 535.9 MiB |
| **Device Windows** (W2) | 50,000 msg/s | **0.00%** | **5.1 MiB** | 55.2% loss | 961.7 MiB |
| **Device Windows** (W2) | 100,000 msg/s | **0.00%** | **5.1 MiB** | 76.8% loss | 911.9 MiB |
| **10,000 Topics** (W3) | 100,000 msg/s | **0.00%** | **57.6 MiB** | 81.1% loss | 43.2 MiB |

For full methodology, test scripts, and four-engine comparison ladders (including Telegraf and Redpanda Connect), see the [Benchmark Overview](benchmarks/overview.md) and [Throughput Benchmarks](benchmarks/throughput.md).

---

## 5. Summary & Next Steps

`rekuiper` delivers memory safety, zero garbage-collection jitter, and enterprise streaming SQL in a compact, standalone Rust binary.

- [Getting Started Guide](getting_started/getting_started.md)
- [SQL Reference & Built-in Functions](sqls/overview.md)
- [MQTT Source Configuration](guide/sources/builtin/mqtt.md)
- [MQTT Sink Configuration](guide/sinks/builtin/mqtt.md)
- [Web Console & DAG Studio Guide](guide/ui/web_console_user_guide.md)
- [Inspect GitHub Repository](https://github.com/Indus-Stack/rekuiper)
