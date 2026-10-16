---
title: Industrial AI Agents with Model Context Protocol
description: Connect Claude, Cursor, and autonomous AI agents to physical edge IoT gateways using rekuiper-mcp and the Model Context Protocol.
keywords: MCP server for IoT, Model Context Protocol industrial IoT, AI agent MQTT SQL, physical AI edge gateway
---

# Industrial AI Agents with Model Context Protocol (rekuiper-mcp)

Physical AI and industrial edge operations require AI agents to inspect telemetry streams, validate SQL transformations, and manage gateway pipelines. **`rekuiper-mcp`** is a native Rust server implementing the [Model Context Protocol (MCP)](https://modelcontextprotocol.io/). It connects generative AI models (Claude, Cursor, custom agents) directly to live edge gateways over standard JSON-RPC 2.0.

---

## Architecture: Bridging AI Models and OT Telemetry

Large Language Models (LLMs) cannot safely interact directly with raw Modbus PLC registers or MQTT wildcards. `rekuiper-mcp` provides a structured, sandboxed boundary:

![rekuiper-mcp Architecture](/diagrams/mcp_architecture.svg)

### How It Works
1. **Model Context Protocol (JSON-RPC):** The AI assistant communicates with `rekuiper-mcp` via `stdio` transport.
2. **Schema & Stream Inspection:** The assistant queries available edge streams, schema types, and active rules.
3. **Offline SQL Validation (`ruletest`):** The assistant tests generated SQL against real or simulated message payloads before deploying.
4. **Controlled Gateway Deployment:** Once verified, the assistant deploys the rule through `rekuiper`'s REST API.

---

## Client Setup and Configuration

Add `rekuiper-mcp` to your MCP client configuration file:

### 1. Antigravity IDE & Cursor (`.cursor/mcp.json` or `mcp_config.json`)

```json
{
  "mcpServers": {
    "rekuiper": {
      "command": "rekuiper-mcp",
      "args": [
        "--server-url",
        "http://127.0.0.1:9081"
      ],
      "env": {
        "RUST_LOG": "info"
      }
    }
  }
}
```

### 2. Claude Desktop (`claude_desktop_config.json`)

* **Path on macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`
* **Path on Windows:** `%APPDATA%\Claude\claude_desktop_config.json`

```json
{
  "mcpServers": {
    "rekuiper": {
      "command": "rekuiper-mcp",
      "args": [
        "--server-url",
        "http://127.0.0.1:9081"
      ]
    }
  }
}
```

---

## Built-in MCP Tools for Industrial Agents

`rekuiper-mcp` exposes a complete toolset for autonomous agents:

| Tool Name | Parameters | Purpose |
| :--- | :--- | :--- |
| `list_streams` | None | Discovers all active input telemetry streams on the gateway. |
| `describe_stream` | `name` | Returns schema fields, data types, and transport configuration (MQTT, Modbus, OPC-UA). |
| `validate_sql` | `sql` | Checks SQL syntax and type safety offline using the `rekuiper` query parser. |
| `dry_run_rule` | `sql`, `mock_data` | Simulates rule execution against synthetic or captured payloads using `ruletest`. |
| `list_rules` | None | Lists running, paused, and stopped stream processing rules. |
| `create_rule` | `id`, `sql`, `actions` | Deploys a verified streaming pipeline to the gateway. |
| `get_rule_metrics` | `id` | Inspects messages in, messages out, processing latency, and dead-letter queue counters. |

---

## Real-World Conversational Example

Here is a real conversation between an industrial automation engineer and an AI assistant connected via `rekuiper-mcp`:

> **Engineer:**  
> *"Inspect our live boiler telemetry stream. If temperature exceeds 120°C for three consecutive readings, write an alert rule that publishes to `alerts/boiler_overheat` and logs an emergency record."*

> **AI Assistant (via `rekuiper-mcp`):**  
> 1. *Invokes `describe_stream("boiler_telemetry")` to check sensor property names.*  
> 2. *Writes SQL query:*  
>    ```sql
>    SELECT deviceId, temperature, meta(topic) AS source_topic
>    FROM boiler_telemetry
>    WHERE temperature > 120.0
>    GROUP BY deviceId, SLIDINGWINDOW(ss, 10)
>    HAVING count(*) >= 3
>    ```  
> 3. *Invokes `dry_run_rule` with 5 mock events to verify that normal readings are ignored and 3 spikes trigger exactly one alert.*  
> 4. *Invokes `create_rule("boiler_overheat_monitor", sql, actions)`.*  
> 5. *Reports: "Rule `boiler_overheat_monitor` is now active on the gateway. Current status: Running, 0 errors."*

---

## Safety and Guardrails for Industrial Operations

* **Zero Direct Process Control:** AI models never write directly to raw hardware registers. All actuation passes through strictly validated `rekuiper` sink definitions.
* **Deterministic Verification:** Every generated rule must pass `validate_sql` and `dry_run_rule` before registration.
* **Poison-Pill Protection:** Malformed payloads generated during testing route to Dead-Letter Queues without halting existing rules.

---

## Next Steps

* [Read the Full MCP Specification](../mcp/overview.md)
* [Explore Rule Testing and Debugging](../api/restapi/ruletest.md)
* [Review Dead-Letter Queues and Poison-Pill Isolation](../guide/rules/error_handling_dlq.md)
