# Schema Registry and Multi-Protocol Serializer Hub

This document describes the Schema Registry and Multi-Protocol Serializer Hub in rekuiper.

## Overview

The Schema Registry manages message schemas and data contracts for edge streaming. It provides contract management and parsing with zero external dependencies.

The registry supports four serialization formats:
1. **Google Protocol Buffers v3 (`protobuf`)**: Strongly-typed binary contracts with message type extraction.
2. **Custom Binary Codecs (`custom`)**: User-defined binary layouts and register specifications (e.g. Modbus RTU).
3. **JSON Schema (`json`)**: JSON Schema Draft-07 structured object validation contracts.
4. **Delimited / CSV (`delimited`)**: Field delimiter specifications for tabular and time-series streams.

---

## Architecture Flow

The diagram below shows how payloads move from edge sensors to the Schema Registry and through the stream processing engine:

<svg viewBox="0 0 880 260" xmlns="http://www.w3.org/2000/svg" style="width: 100%; max-width: 880px; height: auto; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;">
  <defs>
    <linearGradient id="grad-sensor" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#f8fafc" />
      <stop offset="100%" stop-color="#f1f5f9" />
    </linearGradient>
    <linearGradient id="grad-registry" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#ede9fe" />
      <stop offset="100%" stop-color="#ddd6fe" />
    </linearGradient>
    <linearGradient id="grad-engine" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#e0f2fe" />
      <stop offset="100%" stop-color="#bae6fd" />
    </linearGradient>
    <linearGradient id="grad-sink" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#f0fdf4" />
      <stop offset="100%" stop-color="#dcfce7" />
    </linearGradient>
    <marker id="arrow" viewBox="0 0 10 10" refX="6" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
      <path d="M 0 1 L 10 5 L 0 9 z" fill="#64748b" />
    </marker>
  </defs>

  <!-- Step 1: Ingestion Sources -->
  <rect x="20" y="30" width="170" height="200" rx="8" fill="url(#grad-sensor)" stroke="#cbd5e1" stroke-width="1.5" />
  <text x="105" y="60" text-anchor="middle" font-size="13" font-weight="700" fill="#0f172a">Ingress Sources</text>
  <rect x="35" y="80" width="140" height="32" rx="4" fill="#ffffff" stroke="#e2e8f0" />
  <text x="105" y="101" text-anchor="middle" font-size="11" fill="#334155">MQTT Broker (Proto3)</text>
  <rect x="35" y="122" width="140" height="32" rx="4" fill="#ffffff" stroke="#e2e8f0" />
  <text x="105" y="143" text-anchor="middle" font-size="11" fill="#334155">Kafka (Custom Codec)</text>
  <rect x="35" y="164" width="140" height="32" rx="4" fill="#ffffff" stroke="#e2e8f0" />
  <text x="105" y="185" text-anchor="middle" font-size="11" fill="#334155">File / CSV (Delimited)</text>

  <!-- Flow 1 to 2 -->
  <line x1="190" y1="130" x2="228" y2="130" stroke="#64748b" stroke-width="1.5" marker-end="url(#arrow)" />

  <!-- Step 2: Schema Registry Hub -->
  <rect x="230" y="20" width="210" height="220" rx="8" fill="url(#grad-registry)" stroke="#c4b5fd" stroke-width="1.5" />
  <text x="335" y="50" text-anchor="middle" font-size="13" font-weight="700" fill="#4c1d95">Schema Registry Hub</text>
  <rect x="245" y="70" width="180" height="42" rx="5" fill="#ffffff" stroke="#ddd6fe" />
  <text x="255" y="88" font-size="11" font-weight="600" fill="#5b21b6">Proto3 AST Parser</text>
  <text x="255" y="102" font-size="10" fill="#6d28d9">Extracts messages and tags</text>
  <rect x="245" y="120" width="180" height="42" rx="5" fill="#ffffff" stroke="#ddd6fe" />
  <text x="255" y="138" font-size="11" font-weight="600" fill="#5b21b6">Syntax Preflight</text>
  <text x="255" y="152" font-size="10" fill="#6d28d9">Validation and error detection</text>
  <rect x="245" y="170" width="180" height="52" rx="5" fill="#ffffff" stroke="#ddd6fe" />
  <text x="255" y="188" font-size="11" font-weight="600" fill="#5b21b6">Reference Tracking</text>
  <text x="255" y="202" font-size="10" fill="#6d28d9">Stream and sink dependencies</text>
  <text x="255" y="214" font-size="9" fill="#7c3aed">Safe drop guard verification</text>

  <!-- Flow 2 to 3 -->
  <line x1="440" y1="130" x2="478" y2="130" stroke="#64748b" stroke-width="1.5" marker-end="url(#arrow)" />

  <!-- Step 3: Stream Engine -->
  <rect x="480" y="30" width="180" height="200" rx="8" fill="url(#grad-engine)" stroke="#7dd3fc" stroke-width="1.5" />
  <text x="570" y="60" text-anchor="middle" font-size="13" font-weight="700" fill="#0369a1">Engine Deserializer</text>
  <rect x="495" y="80" width="150" height="32" rx="4" fill="#ffffff" stroke="#bae6fd" />
  <text x="570" y="101" text-anchor="middle" font-size="11" fill="#0284c7">Fast Byte Decode</text>
  <rect x="495" y="122" width="150" height="32" rx="4" fill="#ffffff" stroke="#bae6fd" />
  <text x="570" y="143" text-anchor="middle" font-size="11" fill="#0284c7">SQL Window &amp; Query</text>
  <rect x="495" y="164" width="150" height="32" rx="4" fill="#ffffff" stroke="#bae6fd" />
  <text x="570" y="185" text-anchor="middle" font-size="11" fill="#0284c7">Schema Type Coercion</text>

  <!-- Flow 3 to 4 -->
  <line x1="660" y1="130" x2="698" y2="130" stroke="#64748b" stroke-width="1.5" marker-end="url(#arrow)" />

  <!-- Step 4: Serialized Egress Sinks -->
  <rect x="700" y="30" width="160" height="200" rx="8" fill="url(#grad-sink)" stroke="#86efac" stroke-width="1.5" />
  <text x="780" y="60" text-anchor="middle" font-size="13" font-weight="700" fill="#15803d">Sink Serializer</text>
  <rect x="715" y="80" width="130" height="32" rx="4" fill="#ffffff" stroke="#bbf7d0" />
  <text x="780" y="101" text-anchor="middle" font-size="11" fill="#16a34a">Proto3 Serialization</text>
  <rect x="715" y="122" width="130" height="32" rx="4" fill="#ffffff" stroke="#bbf7d0" />
  <text x="780" y="143" text-anchor="middle" font-size="11" fill="#16a34a">Delimited / CSV Sink</text>
  <rect x="715" y="164" width="130" height="32" rx="4" fill="#ffffff" stroke="#bbf7d0" />
  <text x="780" y="185" text-anchor="middle" font-size="11" fill="#16a34a">Cloud / Broker Out</text>
</svg>

---

## Key Capabilities

### 1. Proto3 AST Parser and Symbol Extractor
The Schema Registry contains an in-memory client-side AST parser for Protocol Buffers version 3:
- Detects package declarations (e.g. `package rekuiper.industrial;`).
- Extracts message identifiers and their typed field definitions with tag numbers.
- Identifies enumeration types and numeric values.
- Validates syntax and braces without compiling external binaries.

### 2. Live Stream Designer Integration
When configuring a Stream or Table with format `protobuf` or `custom`:
- The Stream Designer displays the **Registered Schema ID** selector populated from the registry.
- For Protobuf schemas, the **Proto3 Message Type** dropdown populates with extracted messages.
- The **Autofill Fields 🪄** button maps Protobuf field types to rekuiper stream types automatically:
  - `string` -> `string (text)`
  - `double`, `float` -> `float (64-bit double)`
  - `int32`, `int64`, `uint32`, `uint64` -> `bigint (64-bit integer)`
  - `bool` -> `boolean`
  - `bytes` -> `bytea (raw byte buffer)`
  - `repeated <type>` -> `array`
- The Generated DDL SQL automatically updates with `SCHEMAID = "<id>"` and `MESSAGETYPE = "<type>"`.

### 3. Rule Designer Multi-Protocol Egress
Rule Designer Sinks support four serialization modes:
- `JSON`: Default serialized object or array.
- `Protobuf 3`: Serializes outgoing alert or telemetry events into binary protobuf payloads matching the schema contract.
- `Custom Codec`: Applies custom binary encoders.
- `Delimited`: Encodes egress rows using specified delimiter characters (e.g. comma or tab).

### 4. Reference Tracking and Safe Deletion Guard
The registry tracks all streams, scan tables, and rule egress actions that reference each schema.
- If a schema has active references, the registry displays an active reference badge.
- If a user attempts to drop an active schema, the registry displays a warning dialog listing all dependent pipelines before deletion.

---

## REST API Reference

The Schema Registry adheres to standard eKuiper REST endpoints:

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/schemas/protobuf` | List all registered Protobuf schema names |
| `GET` | `/schemas/protobuf/:name` | Get Protobuf schema definition and text content |
| `POST` | `/schemas/protobuf` | Register new Protobuf schema (`{ name, content }`) |
| `PUT` | `/schemas/protobuf/:name` | Update existing Protobuf schema text content |
| `DELETE` | `/schemas/protobuf/:name` | Drop Protobuf schema from registry |
| `GET` | `/schemas/custom` | List all registered custom codec schemas |
| `GET` | `/schemas/custom/:name` | Get custom codec schema definition |
| `POST` | `/schemas/custom` | Register custom codec schema |
| `PUT` | `/schemas/custom/:name` | Update custom codec schema |
| `DELETE` | `/schemas/custom/:name` | Drop custom codec schema |

---

## Example Usage

### 1. Register a Protobuf Schema via REST

```bash
curl -X POST http://localhost:9081/schemas/protobuf \
  -H "Content-Type: application/json" \
  -d '{
    "name": "factory_telemetry",
    "content": "syntax = \"proto3\";\npackage factory;\n\nmessage SensorReading {\n  string device_id = 1;\n  double temperature = 2;\n  double pressure = 3;\n  int64 timestamp = 4;\n}"
  }'
```

### 2. Create a Stream Using the Schema in SQL DDL

```sql
CREATE STREAM factory_stream (
    device_id string,
    temperature float,
    pressure float,
    timestamp bigint
) WITH (
    DATASOURCE = "factory/sensors",
    FORMAT = "protobuf",
    SCHEMAID = "factory_telemetry",
    MESSAGETYPE = "SensorReading",
    TYPE = "mqtt"
);
```

### 3. Configure a Rule with Protobuf Serialization Sink

```json
{
  "id": "rule_proto_alert",
  "sql": "SELECT device_id, temperature FROM factory_stream WHERE temperature > 85.0",
  "actions": [
    {
      "mqtt": {
        "server": "tcp://127.0.0.1:1883",
        "topic": "factory/alerts/proto",
        "format": "protobuf",
        "schemaId": "factory_telemetry",
        "messageType": "SensorReading",
        "sendSingle": true
      }
    }
  ]
}
```
