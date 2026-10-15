---
layout: home

hero:
  name: rekuiper
  text: Process device data where it is produced.
  tagline: A small SQL engine that sits between your devices and your cloud. It filters, aggregates, and routes live data on the gateway, so you send upstream only what matters.
  actions:
    - theme: brand
      text: Run the 60-second demo
      link: /#try-it-in-60-seconds
    - theme: alt
      text: See where it fits
      link: /#where-it-fits
    - theme: alt
      text: Coming from eKuiper?
      link: /concepts/ekuiper_compatibility

features:
  - title: New to stream processing?
    details: You have device data and a cloud bill. See the five common patterns and what each one changes.
    link: /#common-patterns
    linkText: See the patterns
  - title: Already run eKuiper?
    details: rekuiper uses the same REST API, SQL, rules, and CLI. Read what stays the same and what you gain.
    link: /operation/migration
    linkText: Read the migration guide
  - title: Comparing tools?
    details: An honest list of where rekuiper fits and where another tool is the better choice.
    link: /#is-it-a-fit
    linkText: Check the fit
---

## Where it fits

![Where rekuiper fits: devices and feeds on the left, rekuiper on the gateway in the middle, destinations on the right](/diagrams/where-it-fits.svg)

You write a rule in SQL. rekuiper runs it on every message as the message arrives. You add, change, and remove rules through a REST API while the engine runs.

What changes when you put it in the path:

- **You send less data.** One summary row per minute replaces sixty raw readings.
- **You decide locally.** An alert fires on the gateway in milliseconds, without a cloud round trip.
- **You lose nothing when the link drops.** Results wait in memory and on disk, then go out in order when the link returns.

## Try it in 60 seconds

This demo needs only Docker. A built-in simulator makes the data.

**1. Start the engine.**

```shell
docker run -d --name rekuiper -p 9081:9081 ankurkrp/rekuiper:1.0.0-rc3
```

**2. Register simulated sensor readings, ten per second.**

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

**3. Create a stream that reads them.**

```shell
curl -X POST http://localhost:9081/streams \
  -H "Content-Type: application/json" \
  -d '{"sql": "CREATE STREAM demo () WITH (TYPE=\"simulator\", CONF_KEY=\"demo\")"}'
```

**4. Add a rule that turns every five seconds of readings into one row.**

```shell
curl -X POST http://localhost:9081/rules \
  -H "Content-Type: application/json" \
  -d '{
    "id": "five_second_summary",
    "sql": "SELECT count(*) AS readings, avg(temperature) AS avg_temp FROM demo GROUP BY TUMBLINGWINDOW(ss, 5)",
    "actions": [{ "log": {} }]
  }'
```

**5. Watch the result.**

```shell
docker logs -f rekuiper
```

```text
[RULE five_second_summary] Matched record: {"readings": Number(49), "avg_temp": Number(23.03)}
```

About fifty readings go in. One row comes out. To send that row to MQTT, Kafka, or a database, replace `log` with another [sink](./guide/sinks/overview.md).

Continue with the [Getting Started Guide](./getting_started/getting_started.md).

## Common patterns

Each pattern is one or two SQL rules.

### Cut the cloud bill

Devices publish every second. The cloud needs one row per minute. Aggregate on the gateway and send the summary.

```sql
SELECT deviceId, avg(temperature) AS avg_temp, max(temperature) AS max_temp
FROM sensors
GROUP BY deviceId, TUMBLINGWINDOW(ss, 60)
```

Read more: [Windows](./concepts/streaming/windowing.md)

### Alert locally

A reading crosses a limit and someone must know now. Filter on the gateway and call a webhook or publish an MQTT command. The alert does not wait for the cloud.

```sql
SELECT deviceId, temperature, meta(topic) AS source_topic
FROM sensors
WHERE temperature > 80
```

Read more: [Rules](./guide/rules/overview.md) · [REST sink](./guide/sinks/builtin/rest.md) · [MQTT sink](./guide/sinks/builtin/mqtt.md)

### Survive a bad link

The uplink drops for minutes or hours. Turn on the sink cache. Results wait in memory and on disk, and rekuiper resends them in order when the link returns.

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

The same data must go to storage, to a dashboard, and to an alert topic. Several rules read one stream. Each rule has its own SQL and its own destinations.

| Rule | SQL | Destination |
| :--- | :--- | :--- |
| Archive | `SELECT * FROM sensors` | Parquet files on local disk |
| Dashboard | One-minute averages per device | InfluxDB |
| Alert | `WHERE temperature > 80` | MQTT command topic |

Read more: [Sinks](./guide/sinks/overview.md)

### Enrich before sending

Messages carry a device ID. The cloud needs the site and the asset name. Join the stream with a lookup table in Redis or a SQL database.

```sql
SELECT s.deviceId, s.temperature, a.site, a.asset_name
FROM sensors AS s
INNER JOIN assets AS a ON s.deviceId = a.id
```

Read more: [Lookup tables](./guide/tables/lookup.md)

## Is it a fit?

| Use rekuiper when | Look elsewhere when |
| :--- | :--- |
| Your data arrives via industrial protocols (Modbus TCP/RTU, OPC-UA) or messages (MQTT, HTTP, WebSocket, Kafka, AMQP). | You need proprietary closed-source fieldbus protocols (e.g., Profinet IRT or EtherCAT master stacks) without an Ethernet gateway. |
| You want the logic in SQL, and you want to change it at runtime. | You need a visual flow builder for people who do not write SQL. |
| The machine has one or two CPU cores and limited memory. | You need a distributed cluster that scales across many nodes. rekuiper runs on one node. |
| You need time windows, joins, and rules that keep state. | You only need to collect metrics and forward them without processing. |

## Measured on one CPU core

All tests run in a container limited to one CPU core and 1 GiB of RAM.

| Measure | Result | Tested on |
| :--- | :--- | :--- |
| MQTT throughput, single rule | 150,000 to 200,000 messages per second | 0.500 |
| Parallel rules on one stream | 1,000 rules, 500,000 evaluations per second | 0.505 |
| Heap memory under load | 4.4 to 10.2 MiB | 0.500 |
| Footprint | One static binary, about 30 MB | 0.509 |

::: details Comparison with eKuiper 2.4.1 under the same limits
| Capability | rekuiper | eKuiper 2.4.1 |
| :--- | :--- | :--- |
| Single-core throughput | 150,000 to 200,000 msg/s | 20,000 msg/s |
| Parallel rules on one stream | 1,000 | 100 |
| Heap memory at load | 4.4 to 10.2 MiB | 15 to 886 MiB |
| Garbage collection | None | Present |
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
| Use an AI coding assistant with rekuiper | [Model Context Protocol](./mcp/overview.md) |
