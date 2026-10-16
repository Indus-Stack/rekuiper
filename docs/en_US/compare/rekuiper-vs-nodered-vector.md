---
title: rekuiper vs. Node-RED vs. Vector
description: Architectural comparison between rekuiper, Node-RED, and Vector for IoT edge gateways, OT telemetry, and stream processing.
keywords: IoT edge rule engine comparison, Node-RED vs SQL stream processing, Vector vs eKuiper, lightweight edge telemetry engine
---

# rekuiper vs. Node-RED vs. Vector: Choosing the Right Edge Pipeline

Edge computing teams frequently compare `rekuiper`, **Node-RED**, and **Vector** when designing telemetry pipelines for industrial gateways. While all three tools run on edge hardware, they address fundamentally different engineering problems.

---

## Architectural Comparison Matrix

| Feature | `rekuiper` | Node-RED | Vector (Datadog) |
| :--- | :--- | :--- | :--- |
| **Primary Purpose** | Stateful edge SQL stream engine | Visual low-code flow wiring | Stateless log & metric forwarder |
| **Implementation Language** | Pure Rust | JavaScript (Node.js / V8) | Pure Rust |
| **Peak Event Throughput** | **1,000,000+ events/s** (150k–200k msg/s network) | 2,000–5,000 msg/s | 100,000–180,000 msg/s |
| **Typical Memory Footprint** | **< 35 MB RAM** | 120–350 MB RAM | 40–90 MB RAM |
| **Query & Logic Model** | Standard SQL (Windows, Joins) | Drag-and-drop visual nodes | Vector Remap Language (VRL) |
| **Stateful Time Windows** | **Yes** (Tumbling, Hopping, Sliding) | No (Requires custom code) | No (Stateless or batch only) |
| **Multi-Stream Joins** | **Yes** (Stream-to-Stream, Lookup) | No (Complex manual buffers) | No |
| **Industrial Protocols** | **Native** Modbus, OPC-UA, MQTT | Community nodes | MQTT only (via plugins) |
| **Fleet GitOps & Provisioning** | **Declarative YAML with SHA-256** | Exported JSON flow files | Declarative TOML / YAML |
| **Closed-Loop Actuation** | **Yes** (Modbus write, OPC-UA commands) | Yes (Low-frequency) | No (Forwarding only) |

---

## 1. `rekuiper` vs. Node-RED

### When to Use Node-RED
* **Low-Frequency Prototyping:** Connecting a home-automation device or testing an initial proof-of-concept.
* **Non-Developer Operators:** Users who cannot write SQL and prefer dragging visual wires between nodes.
* **Low-Volume Pipelines:** Message rates below 1,000 messages per second where JavaScript engine overhead is acceptable.

### Where Node-RED Breaks Down in Industrial Production
1. **Throughput Bottlenecks:** Node-RED runs on single-threaded Node.js V8. JSON deserialization of high-frequency sensor streams (10,000+ Hz) consumes 100% CPU and introduces high latency jitter.
2. **Configuration Drift Across Fleets:** Visual flow JSON files are difficult to version-control and diff. Maintaining consistency across 500 edge gateways requires custom tooling.
3. **No Native Windowed Calculations:** Implementing a 60-second tumbling average with change-detection thresholds requires custom JavaScript function nodes that maintain unsafe global state.

### How `rekuiper` Solves This
* **Declarative GitOps:** Define streams and rules in version-controlled YAML files verified by SHA-256 checksums ([`declarative_gitops.md`](../operation/declarative_gitops.md)).
* **Rust Performance:** Processes 1,000,000 to 2,200,000+ events per second on a single core with sub-millisecond execution latency.
* **Stateful SQL Windows:** Native ANSI SQL windowing calculates tumbling, sliding, and session summaries without custom code.

---

## 2. `rekuiper` vs. Vector (and Telegraf)

### When to Use Vector
* **Stateless Log Collection:** Reading system syslog files, container stdout, and routing them to Elasticsearch, Datadog, or S3.
* **Point-to-Point Metric Shipping:** Collecting host CPU, disk, and network counters and forwarding them directly to Prometheus or InfluxDB.
* **Pure Transformation without Time Memory:** Renaming JSON keys, hashing user IDs, or parsing grok patterns line by line.

### Where Stateless Forwarders Fall Short
Vector and Telegraf process records individually or in simple buffer flushes. They cannot perform:
1. **Stateful Window Aggregations:** Computing `avg()`, `stddev()`, or `min()` across sliding 5-minute time windows.
2. **Cross-Stream Joins:** Correlating live telemetry with lookup tables in Redis, PostgreSQL, or secondary sensor streams ([`joins.md`](../concepts/streaming/join.md)).
3. **Analytic Trend Detection:** Detecting rate-of-change with `LAG()`, `LEAD()`, or `CHANGED_COL()` functions.
4. **Closed-Loop Feedback:** Writing control registers back to Modbus PLCs or OPC-UA servers when an edge rule triggers.

### Why Teams Combine Vector and `rekuiper`
Many enterprise architectures deploy both tools side-by-side:
* **Vector** handles host operating system logs and container metrics.
* **`rekuiper`** sits on the OT device path, ingesting Modbus, OPC-UA, and MQTT sensor streams, evaluating SQL alert rules locally, and archiving compressed Parquet summaries.

---

## Summary: Which Tool Should You Pick?

```
┌─────────────────────────────────────────────────────────────────┐
│                       WHAT DO YOU NEED?                         │
└─────────────────────────────────────────────────────────────────┘
                                │
        ┌───────────────────────┼────────────────────────┐
        ▼                       ▼                        ▼
Drag-and-drop UI        Stateless log/metric       Stateful OT SQL
for home hobbyists      forwarding to cloud        stream processing
        │                       │                        │
        ▼                       ▼                        ▼
   [ Node-RED ]             [ Vector ]              [ rekuiper ]
   • Visual flows           • Log forwarding        • 1,000,000+ events/s
   • Low-volume (<2k/s)     • Line-by-line transform• Tumbling/sliding windows
   • High memory (V8)       • Rust performance      • Modbus, OPC-UA, Parquet
```

---

## Next Steps

* [Read the Getting Started Guide](../getting_started/getting_started.md)
* [Review Single-Core Throughput Benchmarks](../benchmarks/throughput.md)
* [Explore Native OT Connectors](../guide/connector.md)
