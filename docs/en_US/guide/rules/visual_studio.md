# Native Visual Stream Studio

This document explains the Native Visual Stream Studio embedded in rekuiper.

The studio gives users an interactive graphical interface to design, wire, test, and deploy streaming Directed Acyclic Graph (DAG) rules directly from the `kuiperd` single binary.

## Overview

```
 +-------------------------------------------------------------------------------+
 |                              STREAM DAG STUDIO                                |
 +-------------------+---------------------------------------+-------------------+
 | COMPONENT PALETTE |         INTERACTIVE DAG CANVAS        | INSPECTION DRAWER |
 |                   |                                       |                   |
 | [ Sources ]       |   [MQTT Source]                       | Node Properties:  |
 |  - MQTT Source    |         | (Port 0)                    |  ID: switch_1     |
 |  - Modbus PLC     |         v                             |                   |
 |  - OPC-UA         |   [Switch Router]                     | Wire-Tap Probe:   |
 |                   |    /           \                      |  Status: ACTIVE   |
 | [ Operators ]     |  (P0)          (P1)                   |  Limit: 20 msg/s  |
 |  - Switch Router  |   v             v                     |  Dropped: 0       |
 |  - Rate Limiter   | [Alert MQTT]   [Log Sink]             |                   |
 |  - Watchdog       |                                       | Live Tuple Feed:  |
 |  - Filter / SQL   |                                       | {"temp": 82.4}    |
 |                   |                                       |                   |
 | [ Sinks ]         |                                       | GraphRule JSON:   |
 |  - MQTT Sink      |                                       | Two-way live sync |
 |  - REST / Log     |                                       |                   |
 +-------------------+---------------------------------------+-------------------+
```

### Visual Architecture Diagram

<svg viewBox="0 0 800 240" xmlns="http://www.w3.org/2000/svg" style="max-width:100%; height:auto; background:#090d16; border:1px solid #1e293b; border-radius:8px; margin: 16px 0;">
  <defs>
    <marker id="arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
      <path d="M 0 0 L 10 5 L 0 10 z" fill="#ea580c"/>
    </marker>
  </defs>

  <!-- Canvas Background Grid Dots -->
  <pattern id="grid" width="20" height="20" patternUnits="userSpaceOnUse">
    <circle cx="2" cy="2" r="1" fill="#1e293b"/>
  </pattern>
  <rect width="800" height="240" fill="url(#grid)" />

  <!-- Node 1: MQTT Source -->
  <rect x="40" y="70" width="160" height="80" rx="8" fill="#141e2e" stroke="#0284c7" stroke-width="2"/>
  <rect x="40" y="70" width="160" height="24" rx="8" fill="#0284c7" fill-opacity="0.2"/>
  <text x="50" y="86" fill="#38bdf8" font-family="sans-serif" font-size="12" font-weight="700">📻 MQTT Source</text>
  <text x="50" y="112" fill="#94a3b8" font-family="monospace" font-size="10">topic: factory/plc</text>
  <circle cx="200" cy="110" r="5" fill="#fb923c" stroke="#090d16" stroke-width="2"/>

  <!-- Node 2: Switch Router -->
  <rect x="290" y="55" width="180" height="110" rx="8" fill="#141e2e" stroke="#ea580c" stroke-width="2"/>
  <rect x="290" y="55" width="180" height="24" rx="8" fill="#ea580c" fill-opacity="0.2"/>
  <text x="300" y="71" fill="#fb923c" font-family="sans-serif" font-size="12" font-weight="700">🔀 Switch Router</text>
  <text x="300" y="96" fill="#cbd5e1" font-family="monospace" font-size="10">P0: temp &gt; 75</text>
  <text x="300" y="118" fill="#94a3b8" font-family="monospace" font-size="10">P1: temp &lt;= 75</text>
  <text x="300" y="140" fill="#64748b" font-family="monospace" font-size="10">Default Fallback</text>
  <circle cx="290" cy="110" r="5" fill="#38bdf8" stroke="#090d16" stroke-width="2"/>
  <circle cx="470" cy="94" r="5" fill="#fb923c" stroke="#090d16" stroke-width="2"/>
  <circle cx="470" cy="116" r="5" fill="#fb923c" stroke="#090d16" stroke-width="2"/>
  <circle cx="470" cy="138" r="5" fill="#fb923c" stroke="#090d16" stroke-width="2"/>

  <!-- Wire 1: Source to Switch -->
  <path d="M 200 110 C 245 110, 245 110, 290 110" fill="none" stroke="#ea580c" stroke-width="2.5" marker-end="url(#arrow)"/>

  <!-- Node 3: Critical Alert Sink -->
  <rect x="560" y="30" width="160" height="70" rx="8" fill="#141e2e" stroke="#059669" stroke-width="2"/>
  <rect x="560" y="30" width="160" height="24" rx="8" fill="#059669" fill-opacity="0.2"/>
  <text x="570" y="46" fill="#34d399" font-family="sans-serif" font-size="12" font-weight="700">📤 Critical Alerts</text>
  <text x="570" y="72" fill="#94a3b8" font-family="monospace" font-size="10">topic: alarms/high</text>
  <circle cx="560" cy="65" r="5" fill="#38bdf8" stroke="#090d16" stroke-width="2"/>

  <!-- Wire 2: Switch P0 to Alert Sink -->
  <path d="M 470 94 C 515 94, 515 65, 560 65" fill="none" stroke="#ea580c" stroke-width="2.5" marker-end="url(#arrow)"/>

  <!-- Node 4: Normal Log Sink -->
  <rect x="560" y="130" width="160" height="70" rx="8" fill="#141e2e" stroke="#059669" stroke-width="2"/>
  <rect x="560" y="130" width="160" height="24" rx="8" fill="#059669" fill-opacity="0.2"/>
  <text x="570" y="146" fill="#34d399" font-family="sans-serif" font-size="12" font-weight="700">📜 Audit Log</text>
  <text x="570" y="172" fill="#94a3b8" font-family="monospace" font-size="10">daemon console</text>
  <circle cx="560" cy="165" r="5" fill="#38bdf8" stroke="#090d16" stroke-width="2"/>

  <!-- Wire 3: Switch P1 to Log Sink -->
  <path d="M 470 116 C 515 116, 515 165, 560 165" fill="none" stroke="#ea580c" stroke-width="2.5" marker-end="url(#arrow)"/>
</svg>

## Core Features

1. **Interactive DAG Canvas**
   - Connect sources, operators, and sinks with mouse interactions.
   - Support for multi-port routing nodes (`switch` operator with `port 0`, `port 1`, and `default` ports).
   - Real-time cubic Bezier curve rendering with auto-layout alignment.
   - Animated stream pulses across active connecting wires when telemetry tuples flow.

2. **Two-Way GraphRule JSON Synchronization**
   - Canvas modifications immediately update the serialized `GraphRule` JSON definition.
   - Text edits in the JSON editor update the visual DAG canvas upon clicking **Apply to Canvas**.
   - Direct one-click deployment to the running Kuiper engine via the **Deploy Pipeline** button.

3. **Live Node Metric Badges and /comms Wire-Tap**
   - Real-time WebSocket connection to `/comms`.
   - Node badges display total processed records.
   - Click-to-tap wire-tap inspection with a token-bucket rate limiter (20 messages/second).
   - Real-time `⚡ TAP` status badge displays when a probe is active.
   - Monospace telemetry inspection terminal for live record inspection.

4. **Natural-Language-to-Graph AI Assistant**
   - Describe stream flows in plain language (for example: *"Read factory MQTT -> rate limit 10/s -> route temp > 75 to alerts_mqtt, else log"*).
   - Click **Synthesize Pipeline** to compile the prompt into nodes, properties, and edges.

## Real-Time Wire-Tap Telemetry Format

When you enable a wire tap on a node, the daemon streams JSON-RPC events over `/comms` with the following schema:

```json
{
  "type": "tuple_sample",
  "rule_id": "rule_turbine_monitor",
  "node_id": "switch_1",
  "data": {
    "device_id": "turbine-alpha-01",
    "temperature": 78.4,
    "vibration": 0.042,
    "ts": 1740001250000
  },
  "dropped_since_last": 0,
  "timestamp": 1740001250100
}
```

- `rule_id`: Identifier of the running pipeline.
- `node_id`: Target node identifier with the active probe.
- `data`: Exact tuple map emitted by the node execution stage.
- `dropped_since_last`: Number of sample tuples discarded by the token-bucket rate limiter to preserve network bandwidth.

## Endpoints

- `GET /ui`: Serves the embedded single-binary console.
- `GET /api/v1/palette`: Returns dynamic node definitions and property schemas.
- `GET /comms`: WebSocket bus for live tuple samples and connector status events.
- `POST /rules/:id/tap/:node_id/enable`: Activates a token-bucket wire-tap probe on the specified node.
- `POST /rules/:id/tap/:node_id/disable`: Deactivates the wire-tap probe on the specified node.

## Model Context Protocol (MCP) Integration

AI agents integrate with Visual Stream Studio through the following `rekuiper-mcp` tools:

| MCP Tool | Function |
| :--- | :--- |
| `get_studio_palette` | Retrieves the catalog of visual components, ports, and configuration schemas. |
| `validate_graph_rule` | Validates a Visual DAG rule JSON definition offline before deployment. |
| `toggle_wire_tap` | Enables or disables a wire-tap probe on any DAG node with configurable rate limiting. |
| `get_wire_tap_status` | Queries active wire-tap probe status, message counters, and drop counts. |

You can also read the live palette resource via `rekuiper://studio/palette` or invoke the prompt `design_visual_dag_pipeline`.
