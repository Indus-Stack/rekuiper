# Guided Rule Designer and Visual Schema Builder

This document explains the Guided 3-Step Rule Designer and Visual Schema Builder embedded in the rekuiper web console.

These tools give operators a simple, visual workflow to build SQL streaming pipelines, configure multi-sink delivery targets, test engine validation, and declare strongly-typed stream schemas without writing raw JSON by hand.

## Overview

```
 +-------------------------------------------------------------------------------+
 |                       GUIDED RULE PIPELINE DESIGNER                           |
 +-----------------------------------------------+-------------------------------+
 |               STEP-BY-STEP FORM               |      LIVE PREVIEW & ENGINE    |
 |                                               |                               |
 | [1. Source & SQL Query]                       | [Pipeline JSON Preview]       |
 |   - Rule ID: rule_motor_overheat_alert        |  {                            |
 |   - Visual Builder vs Raw SQL                 |    "id": "...",               |
 |   - Stream/Table Picker                       |    "sql": "SELECT ...",       |
 |   - Projections, WHERE, Windowing             |    "actions": [...],          |
 |   - GROUP BY, HAVING, ORDER BY, LIMIT         |    "options": {...}           |
 |                                               |  }                            |
 | [2. Multi-Sink Outputs]                       |                               |
 |   - Multiple Targets (MQTT, REST, Kafka, ...) | [Mask Secrets Toggle]         |
 |   - DataTemplate & SendSingle                 | [Copy JSON]                   |
 |                                               |                               |
 | [3. Runtime & Resilience]                     | [✔ Validate with Engine]     |
 |   - QoS 0 / 1 / 2, Checkpoints, Concurrency   |  Status: Engine Validated     |
 |   - Event-Time Semantics & Late Tolerance     |                               |
 +-----------------------------------------------+-------------------------------+
```

### Visual Workflow Diagram

<svg viewBox="0 0 800 240" xmlns="http://www.w3.org/2000/svg" style="max-width:100%; height:auto; background:#ffffff; border:1px solid #e2e8f0; border-radius:8px; margin: 16px 0;">
  <defs>
    <marker id="arrowhead" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
      <path d="M 0 0 L 10 5 L 0 10 z" fill="#2563eb"/>
    </marker>
  </defs>

  <!-- Step 1 Card -->
  <rect x="30" y="40" width="210" height="150" rx="8" fill="#f8fafc" stroke="#cbd5e1" stroke-width="1.5"/>
  <rect x="30" y="40" width="210" height="32" rx="8" fill="#eff6ff" stroke="#bfdbfe" stroke-width="1"/>
  <text x="42" y="61" fill="#1e40af" font-family="sans-serif" font-size="12" font-weight="700">Step 1: SQL &amp; Source</text>
  <text x="42" y="94" fill="#334155" font-family="sans-serif" font-size="11">Visual SQL Form</text>
  <text x="42" y="114" fill="#64748b" font-family="sans-serif" font-size="11">SELECT, WHERE, Window</text>
  <text x="42" y="134" fill="#64748b" font-family="sans-serif" font-size="11">GROUP BY &amp; HAVING</text>
  <text x="42" y="160" fill="#2563eb" font-family="monospace" font-size="10">SELECT * FROM demo</text>

  <!-- Arrow 1 to 2 -->
  <path d="M 240 115 L 280 115" stroke="#2563eb" stroke-width="2" marker-end="url(#arrowhead)"/>

  <!-- Step 2 Card -->
  <rect x="290" y="40" width="210" height="150" rx="8" fill="#f8fafc" stroke="#cbd5e1" stroke-width="1.5"/>
  <rect x="290" y="40" width="210" height="32" rx="8" fill="#eff6ff" stroke="#bfdbfe" stroke-width="1"/>
  <text x="302" y="61" fill="#1e40af" font-family="sans-serif" font-size="12" font-weight="700">Step 2: Multi-Sink Outputs</text>
  <text x="302" y="94" fill="#334155" font-family="sans-serif" font-size="11">Egress Routing</text>
  <text x="302" y="114" fill="#64748b" font-family="sans-serif" font-size="11">MQTT, REST, Kafka, File</text>
  <text x="302" y="134" fill="#64748b" font-family="sans-serif" font-size="11">DataTemplate &amp; Formatting</text>
  <text x="302" y="160" fill="#059669" font-family="monospace" font-size="10">sendSingle: true</text>

  <!-- Arrow 2 to 3 -->
  <path d="M 500 115 L 540 115" stroke="#2563eb" stroke-width="2" marker-end="url(#arrowhead)"/>

  <!-- Step 3 Card -->
  <rect x="550" y="40" width="220" height="150" rx="8" fill="#f8fafc" stroke="#cbd5e1" stroke-width="1.5"/>
  <rect x="550" y="40" width="220" height="32" rx="8" fill="#eff6ff" stroke="#bfdbfe" stroke-width="1"/>
  <text x="562" y="61" fill="#1e40af" font-family="sans-serif" font-size="12" font-weight="700">Step 3: Runtime &amp; Preflight</text>
  <text x="562" y="94" fill="#334155" font-family="sans-serif" font-size="11">QoS 0 / 1 / 2 Checkpoints</text>
  <text x="562" y="114" fill="#64748b" font-family="sans-serif" font-size="11">Event Time &amp; Late Tolerance</text>
  <text x="562" y="134" fill="#64748b" font-family="sans-serif" font-size="11">Live Engine Preflight Test</text>
  <text x="562" y="160" fill="#16a34a" font-family="monospace" font-size="10">POST /rules/validate ✔</text>
</svg>

## Guided 3-Step Rule Designer

The designer modal opens when you click **+ New Rule Pipeline** on the Pipelines and Rules screen or **Designer 🪄** on any existing rule row.

### Step 1: Source & SQL Query

1. **Pipeline ID**: Unique identifier for the pipeline.
2. **Construction Mode**:
   - **Visual Builder**: Generates standard ANSI SQL from field controls.
   - **Raw SQL**: Allows direct editing of complex queries with snippet helpers.
3. **Source Selection**: Dropdown populated from all registered streams and lookup tables.
4. **Projections**: Comma-separated list of columns or `*` for all attributes.
5. **WHERE Filter**: Boolean predicate to filter unneeded records early.
6. **Streaming Windowing**:
   - `None`: Record-by-record continuous stream evaluation.
   - `Tumbling Window (Time)`: Non-overlapping fixed time slice (e.g. 10 seconds).
   - `Tumbling Window (Count)`: Fixed batch of records (e.g. 100 rows).
   - `Hopping Window`: Overlapping sliding window with periodic step.
   - `Session Window`: Dynamic temporal aggregation bounded by inactivity gaps.
7. **GROUP BY, HAVING, ORDER BY, LIMIT**: Standard streaming SQL aggregations.

### Step 2: Multi-Sink Outputs

Pipelines can forward filtered or transformed data to one or more egress targets:
- **MQTT**: Broker URL, publish topic, QoS level, client ID.
- **REST / HTTP**: Target URL, HTTP method (POST/PUT), headers.
- **Kafka**: Broker seed list, topic.
- **File / Parquet**: Storage path, file rotation policy.
- **Memory**: In-memory bus for chaining into other local rules.
- **Log / Nop**: Debugging and high-speed benchmarking sinks.

Each sink supports formatting controls:
- **`sendSingle`**: Emits individual JSON objects instead of array envelopes.
- **`dataTemplate`**: Golang text/template format for customized payloads.
- **`omitIfEmpty`**: Drops empty transformation payloads.
- **`runAsync`**: Uses non-blocking background workers.

### Step 3: Runtime & Resilience Options

- **QoS Level**:
  - `0`: At Most Once (lowest latency).
  - `1`: At Least Once (guaranteed delivery).
  - `2`: Exactly Once (state snapshot recovery).
- **Checkpoint Interval**: Frequency of distributed state snapshots in milliseconds.
- **Operator Concurrency**: Worker thread allocation per streaming operator.
- **Buffer Length**: Ring buffer capacity for inter-operator queues.
- **Event-Time Processing**: Extracts timestamps from payloads (`ts` field) and applies `lateTolerance` windows.
- **Resilience Toggles**: Attach `__meta__` tags or route error envelopes directly down the pipeline.

### Live Engine Preflight Validation

Before saving, operators can click **Validate with Engine Preflight**. This calls `POST /rules/validate` against the live rekuiper engine to check:
1. SQL syntax and projection semantics.
2. Source stream availability in catalog.
3. Sink configuration validity.
4. Runtime options schema compliance.

---

## Visual Stream and Table Schema Builder

The schema builder opens when you click **+ New Stream** or **+ New Table** in the catalog view.

### Key Capabilities

1. **Topology Kind**: Switch between append-only event streams (`STREAM`) and static or dimension scan tables (`TABLE`).
2. **Schemaless Ingestion**: Toggle schemaless mode for dynamic, open JSON payloads without pre-declaring columns.
3. **Nested Fields Table**:
   - Field name.
   - Strong data types: `bigint`, `float`, `string`, `boolean`, `datetime`, `bytea`, `array`, `struct`.
   - Nullable flag.
4. **Configuration Keys (`CONF_KEY`)**: Select predefined connector credentials and endpoints or click **+ New ConfKey** to create them inline.
5. **Live DDL Preview**: Generates the exact SQL statement in real-time:

```sql
CREATE STREAM plant_telemetry (
    device_id string,
    temperature float,
    ts bigint
) WITH (
    DATASOURCE = "factory/+/sensors",
    FORMAT = "json",
    TYPE = "mqtt",
    CONF_KEY = "plant_cluster"
);
```

---

## API Reference

| Endpoint | Method | Description |
|:---|:---|:---|
| `/rules` | `POST` | Create a new rule pipeline |
| `/rules/:id` | `PUT` | Update an existing rule pipeline |
| `/rules/validate` | `POST` | Preflight validation of SQL and sinks |
| `/streams` | `POST` | Create a stream using DDL SQL |
| `/streams/:name` | `DELETE` | Delete a declared stream |
| `/tables` | `POST` | Create a lookup table using DDL SQL |
| `/tables/:name` | `DELETE` | Delete a declared table |
| `/metadata/sources/:type/confKeys/:name` | `PUT` | Save connector configuration key |
