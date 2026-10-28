---
layout: home

hero:
  name: rekuiper
  text: Process Industrial & IoT Device Data Where It Is Produced.
  tagline: rekuiper is a single-binary MQTT rule engine and streaming SQL processor written in Rust. It runs on edge gateways between devices and cloud networks—filtering, aggregating, and routing live MQTT, Kafka, Modbus, and OPC-UA telemetry at 1,000,000–2,200,000+ events per second per core.
  actions:
    - theme: brand
      text: Run the 60-Second Demo
      link: /#try-it-in-60-seconds
    - theme: alt
      text: Star on GitHub
      link: https://github.com/Indus-Stack/rekuiper
    - theme: alt
      text: Migrate from eKuiper
      link: /operation/migration
    - theme: alt
      text: Explore MCP for AI Agents
      link: /mcp/overview

features:
  - title: Native OT + IT Protocol Ingestion
    details: Ingest Modbus TCP/RTU, OPC-UA, MQTT v3.1.1/v5, Kafka, Redis, and OTLP without separate protocol-translation containers.
    link: /guide/connector
    linkText: Explore connectors
  - title: Columnar Parquet & Time-Series Sinks
    details: Write windowed summaries directly to Snappy-compressed Apache Parquet with Arrow schema inference, InfluxDB, and TDengine.
    link: /guide/sinks/overview
    linkText: View sink formats
  - title: Declarative GitOps Provisioning
    details: Deploy gateways in immutable declarative mode with SHA-256 verification. Eliminate configuration drift across field fleets.
    link: /operation/declarative_gitops
    linkText: Read GitOps guide
  - title: Poison-Pill Isolation & Offline Cache
    details: Route malformed payloads to a Dead-Letter Queue without crashing rules. Disk-backed sink caching buffers outputs during network disconnects.
    link: /guide/rules/error_handling_dlq
    linkText: Inspect resilience
  - title: Built-in Web Observability UI
    details: Inspect live rule topologies, rule throughput, Prometheus metrics, and OpenTelemetry traces without auxiliary containers.
    link: /operation/observability_ui_top
    linkText: Open UI guide
  - title: First Edge SQL Engine with Native MCP
    details: Connect Claude, Cursor, or autonomous AI agents through Model Context Protocol (rekuiper-mcp) to inspect schemas and dry-run SQL queries.
    link: /mcp/overview
    linkText: Discover MCP
---

<div class="metric-grid">
  <div class="metric-card">
    <div class="metric-value">1,000,000+</div>
    <div class="metric-label">events/s per core</div>
    <div class="metric-desc">Continuous SQL stream throughput in 1.0.0-rc4 (50x–100x faster than Go eKuiper).</div>
  </div>
  <div class="metric-card">
    <div class="metric-value">&lt; 35 MB</div>
    <div class="metric-label">RAM Footprint</div>
    <div class="metric-desc">Predictable memory under 1,000 parallel rules with zero GC pauses.</div>
  </div>
  <div class="metric-card">
    <div class="metric-value">100%</div>
    <div class="metric-label">Wire Parity</div>
    <div class="metric-desc">Drop-in REST API, CLI, and SQL compatibility with upstream eKuiper.</div>
  </div>
  <div class="metric-card">
    <div class="metric-value">20+</div>
    <div class="metric-label">Native Connectors</div>
    <div class="metric-desc">Built-in MQTT, Modbus, OPC-UA, Kafka, Parquet, InfluxDB, and OTLP.</div>
  </div>
</div>

## Where it fits

![Where rekuiper fits: devices and feeds on the left, rekuiper on the gateway in the middle, destinations on the right](/diagrams/where-it-fits.svg)

You write a rule in SQL. rekuiper executes the rule on every message when the message arrives. You add, change, and remove rules through a REST API while the engine runs.

What changes when you deploy rekuiper on the edge:

- **Transmit less data:** One summary row per minute replaces sixty raw readings.
- **Decide locally:** An alert fires on the gateway in sub-milliseconds without cloud latency.
- **Survive network disconnects:** Results buffer in memory and on disk, then transmit in order when the uplink reconnects.

## Try it in 60 seconds

This demo requires Docker. A built-in simulator generates sensor readings.

**1. Start the engine.**

```shell
docker run -d --name rekuiper -p 9081:9081 indusstack/rekuiper:1.0.0-rc4
```

::: tip Built-in Web Observability UI
Open [http://localhost:9081/ui](http://localhost:9081/ui) in your browser to inspect live streams, rules, and rule metrics without installing companion containers.
:::

**2. Register simulated sensor readings (ten events per second).**

```shell
curl -X PUT http://localhost:9081/metadata/sources/simulator/confKeys/demo \
  -H "Content-Type: application/json" \
  -d '{
    "data": [
      {"deviceId": "pump-1", "temperature": 21.5},
      {"deviceId": "pump-1", "temperature": 23.0},
      {"deviceId": "pump-1", "temperature": 24.5}
    ],
    "interval": 100,
    "loop": true
  }'
```

**3. Create a stream to ingest the readings.**

```shell
curl -X POST http://localhost:9081/streams \
  -H "Content-Type: application/json" \
  -d '{"sql": "CREATE STREAM demo () WITH (TYPE=\"simulator\", CONF_KEY=\"demo\")"}'
```

**4. Add a rule to aggregate five seconds of readings into one row.**

```shell
curl -X POST http://localhost:9081/rules \
  -H "Content-Type: application/json" \
  -d '{
    "id": "five_second_summary",
    "sql": "SELECT count(*) AS readings, avg(temperature) AS avg_temp FROM demo GROUP BY TUMBLINGWINDOW(ss, 5)",
    "actions": [{ "log": {} }]
  }'
```

**5. Verify the output.**

```shell
docker logs -f rekuiper
```

```text
[RULE five_second_summary] Matched record: {"readings": Number(49), "avg_temp": Number(23.03)}
```

Fifty readings enter the pipeline. One row exits. To route this row to MQTT, Kafka, or a database, replace `log` with another [sink](./guide/sinks/overview.md).

Continue with the [Getting Started Guide](./getting_started/getting_started.md).

## Common patterns

Each pattern requires one or two SQL rules.

### Cut the cloud bill

Devices publish every second. Cloud analytics require one row per minute. Aggregate on the gateway and send the summary.

```sql
SELECT deviceId, avg(temperature) AS avg_temp, max(temperature) AS max_temp
FROM sensors
GROUP BY deviceId, TUMBLINGWINDOW(ss, 60)
```

Read more: [Windows](./concepts/streaming/windowing.md)

### Alert locally

A sensor value crosses a safety threshold. Filter on the gateway and publish an MQTT command or call a webhook immediately. The alert does not wait for cloud round trips.

```sql
SELECT deviceId, temperature, meta(topic) AS source_topic
FROM sensors
WHERE temperature > 80
```

Read more: [Rules](./guide/rules/overview.md) · [REST sink](./guide/sinks/builtin/rest.md) · [MQTT sink](./guide/sinks/builtin/mqtt.md)

### Survive a bad link

The cellular or WAN uplink drops for minutes or hours. Enable the sink cache. Results buffer on disk, and rekuiper resends them in order when the link reconnects.

```json
{
  "mqtt": {
    "server": "tcp://cloud.example.com:1883",
    "topic": "site1/summary",
    "enableCache": true
  }
}
```

Read more: [Sink cache](./guide/sinks/overview.md)

### One feed, many destinations

One data feed routes to storage, to a dashboard, and to an alert topic. Multiple rules subscribe to one stream. Each rule defines its own SQL and destinations.

| Rule | SQL | Destination |
| :--- | :--- | :--- |
| Archive | `SELECT * FROM sensors` | Parquet files on local disk |
| Dashboard | One-minute averages per device | InfluxDB |
| Alert | `WHERE temperature > 80` | MQTT command topic |

Read more: [Sinks](./guide/sinks/overview.md)

### Enrich before sending

Messages carry a device ID. Upstream applications require site and asset names. Join the stream with a lookup table in Redis or a SQL database.

```sql
SELECT s.deviceId, s.temperature, a.site, a.asset_name
FROM sensors AS s
INNER JOIN assets AS a ON s.deviceId = a.id
```

Read more: [Lookup tables](./guide/tables/lookup.md)

## Is it a fit?

| Use `rekuiper` when... | Look elsewhere when... |
| :--- | :--- |
| **Your data arrives from OT or IT protocols:** Native MQTT, Modbus TCP/RTU, OPC-UA, HTTP, WebSocket, Kafka, AMQP/RabbitMQ, Redis, OTLP, or ZeroMQ. | **You need legacy building-automation protocols not yet built in** (e.g., native BACnet or PROFINET without a protocol converter). |
| **You want streaming logic in SQL** (tumbling/hopping/sliding/session windows, joins, analytic functions) managed via **REST API, CLI, or Declarative GitOps YAML**. | **You want a drag-and-drop visual wiring tool** for non-SQL users (use Node-RED for low-volume visual scripting). |
| **Your edge hardware is constrained** (1–2 CPU cores, 64 MB–512 MB RAM) and cannot tolerate Go/JVM garbage-collection pauses or 500 MB+ container images. | **You need a multi-node distributed cloud shuffle cluster** (use Apache Flink or Spark Streaming for datacenter-scale petabyte joins). |
| **You need offline resilience & local sinks:** Disk-backed sink cache (`sink_cache.rs`), Dead-Letter Queues (`dlq.rs`), and direct writes to Parquet, InfluxDB v1/v2, TDengine, IoTDB, or SQL databases. | **You only need stateless log forwarding** without windowed aggregations, stateful joins, or local rule evaluation (use Vector or Telegraf). |
| **You are building AI-assisted IoT operations:** Native Model Context Protocol (`rekuiper-mcp`) lets AI agents inspect schemas, dry-run SQL rules, and diagnose live pipelines. | **You only need basic protocol passthrough** without filtering, enrichment, or stateful edge computations. |

## Measured on one CPU core

All tests run on dedicated Linux hardware constrained to **one physical CPU core** and **1 GiB of RAM**.

| Measure | Result | Tested on |
| :--- | :--- | :---: |
| **In-process stream throughput (Zero-parse)** | **2,464,711 events/s** (500k in 203 ms, 0 ms drain) | **1.0.0-rc4** |
| **End-to-end JSON stream throughput** | **1,141,417 events/s** (500k in 438 ms, 0 ms drain) | **1.0.0-rc4** |
| MQTT network throughput (Single rule) | 150,000 to 200,000 messages/s (0.00% loss) | 0.500 |
| Concurrent rules on one stream | 1,000 rules (500,000 evaluations/s) | 0.505 |
| Heap memory under load | 4.4 to 10.2 MiB (Flat plateau, zero GC pauses) | 1.0.0-rc4 |
| Static binary footprint | ~30 MB (Self-contained, zero external deps) | 1.0.0-rc4 |

::: details Comparison with eKuiper 2.4.1 under the same limits
| Capability | rekuiper (1.0.0-rc4) | eKuiper 2.4.1 | Performance Delta |
| :--- | :--- | :--- | :--- |
| **In-process stream throughput** | **1,000,000+ events/s** | N/A (Embedded runtime only) | Native Rust speed |
| **Network MQTT throughput** | **150,000 to 200,000 msg/s** | ~20,000 msg/s | **8x to 10x throughput** |
| **Parallel rule scaling** | **1,000 concurrent rules** | ~100 rules | **10x rule capacity** |
| **Memory stability** | **4.4 to 10.2 MiB (Flat)** | 15 to 886 MiB (GC spikes) | **Zero GC jitter, no OOM** |
| **Garbage collection** | **None** (Deterministic ownership) | Present (Stop-the-world sweeps) | Zero tail-latency spikes |
:::

Method and full results: [Benchmark Overview](./benchmarks/overview.md) · [Throughput](./benchmarks/throughput.md) · [Parallel rules](./benchmarks/parallel_rules.md)

## Connectors

| | Sources (data in) | Sinks (data out) |
| :--- | :--- | :--- |
| **Messaging** | MQTT, Kafka, RabbitMQ, Redis, ZeroMQ, WebSocket | MQTT, Kafka, RabbitMQ, Redis, ZeroMQ, WebSocket |
| **HTTP** | HTTP pull, HTTP push | REST |
| **Databases** | PostgreSQL, SQLite | PostgreSQL, SQLite, InfluxDB v1 and v2, TDengine, IoTDB |
| **Files and media** | CSV, JSON Lines, Parquet, video (FFmpeg) | CSV, JSON Lines, Parquet, image |
| **Industrial** | Modbus TCP/RTU, OPC-UA, EdgeX Foundry, Neuron | Modbus TCP/RTU, OPC-UA, EdgeX Foundry, Neuron |
| **Telemetry** | OpenTelemetry (OTLP/HTTP metrics, logs, traces) | OpenTelemetry (OTLP/HTTP) |
| **Test** | Simulator, Memory | Log, Memory, Nop |

Details: [Sources](./guide/sources/overview.md) · [Sinks](./guide/sinks/overview.md)

## Where to go next

| I want to | Go to |
| :--- | :--- |
| Build my first real pipeline with MQTT | [Getting Started Guide](./getting_started/getting_started.md) |
| Install on a gateway, in Docker, or in Kubernetes | [Installation and Deployment](./installation.md) |
| Learn the SQL | [SQL Reference](./sqls/overview.md) |
| Understand how the engine works | [Architecture](./concepts/rekuiper.md) |
| Move from eKuiper | [Migration guide](./operation/migration.md) |
| Automate with the REST API or CLI | [REST API](./api/restapi/overview.md) · [CLI](./api/cli/overview.md) |
| Inspect pipelines with zero companion containers | [Embedded UI & rekuiper top](./operation/observability_ui_top.md) |
| Manage distributed gateways with staged canary rollouts | [Fleet Management & Control Plane](./operation/fleet_management.md) |
| Design multi-port streaming DAGs with live wire-tap | [Visual Stream DAG Studio](./guide/rules/visual_studio.md) |
| Use an AI coding assistant with rekuiper | [Model Context Protocol](./mcp/overview.md) |
