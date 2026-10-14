# rekuiper-mcp: Model Context Protocol Server for rekuiper

An enterprise-grade, native Rust **Model Context Protocol (MCP)** server providing complete AI agent integration with the **rekuiper** lightweight edge streaming SQL engine.

`rekuiper-mcp` exposes the entire surface area of `rekuiper` to LLM assistants (Antigravity IDE, Claude Desktop, Cursor, Continue.dev, and custom agentic workflows) over standard JSON-RPC 2.0 stdio transport.

---

## Key Capabilities

- **Zero-Network Offline SQL Intelligence & Simulation**:
  - `validate_sql`: Offline AST parsing, static analysis, column resolution, and DDL validation directly through the embedded `rekuiper-sql` parser without network overhead.
  - `test_sql_expression`: Instant in-memory execution of streaming SQL transformations against mock event payloads using `Evaluator::eval_select`.
  - `explain_sql`: Deep AST query deconstruction into sources, projections, joins, where filters, group by keys, and time/count window clauses.

- **Full-Fidelity Stream & Table DDL & Event Ingestion**:
  - Complete lifecycle management for streaming datasources (`CREATE STREAM`, `DROP STREAM`, schema inspection, `httppush` source stream bindings).
  - Enrichment lookup tables (`CREATE TABLE`, backend drivers: File, SQLite, Redis, Memory).
  - Direct HTTP event push injection (`push_stream_data`, `push_table_data`) for real-time pipeline testing and data ingestion across standard (`/streams/:name/data`) or custom `httppush` endpoints (`DATASOURCE`).

- **End-to-End Rule & Topology Lifecycle**:
  - Full CRUD operations on streaming rules with advanced topological configurations (QoS, checkpointing, buffer length, restart strategies).
  - Dynamic lifecycle controls (`start`, `stop`, `restart`, `bulk_start_stop_rules`).
  - Runtime execution metrics, DAG topology inspection (`get_rule_topo`), and state reset (`reset_rule_state`).

- **Distributed Tracing & Real-Time Diagnostics**:
  - Dynamic trace session management (`start_rule_trace`, `stop_rule_trace`).
  - Trace event history retrieval and granular step latency profiling (`get_trace_details`).

- **Connection Pooling & Extensibility**:
  - Shared connection resource pool management for MQTT brokers, Kafka clusters, and relational databases.
  - JavaScript User Defined Functions (UDF) authoring and registration.
  - Native C/Rust and multi-language gRPC plugin discovery.

- **Configuration, Disaster Recovery & Telemetry**:
  - Hot runtime config inspection and dynamic updates.
  - Full or selective JSON catalog backup export and restore migrations.
  - Real-time engine health, memory/CPU consumption, and throughput metrics.

- **WebAssembly (WASM) & Dynamic Secrets**:
  - Full management of compiled `.wasm` modules (`register_wasm_plugin`, `list_wasm_plugins`, `delete_wasm_plugin`) with automatic UDF registration.
  - Static scanning and validation of dynamic secret syntax (`{{vault://...}}`, `{{env://...}}`) via `validate_secrets`.

- **Universal REST API Proxy (`execute_rekuiper_api`)**:
  - Allows executing arbitrary HTTP methods (`GET`, `POST`, `PUT`, `DELETE`, `PATCH`) against any present or future `rekuiper` REST endpoint, ensuring 100% API coverage.

- **15 First-Class Resources & 10 Interactive Prompts**:
  - Live queryable resources via `rekuiper://` URIs, including connector action schemas (`rabbitmq`, `parquet`, `edgex`) and Wasm plugins.
  - Expert troubleshooting, query optimization, vector similarity search, CEP sequence matching, security audit, and RabbitMQ pipeline generation prompts.

---

## Architecture & Stdio Transport

`rekuiper-mcp` communicates over standard input (`stdin`) and standard output (`stdout`) following the MCP specification:

```
[ LLM Agent / MCP Client ]
          |
     (JSON-RPC 2.0 via stdio)
          v
 [ rekuiper-mcp Server ] <---- Embedded rekuiper-sql (Offline AST & Simulation)
          |
     (REST API / HTTP)
          v
  [ rekuiper Engine Daemon ] (port 9081)
```

> **Strict Stdio Discipline**: Standard output (`stdout`) is strictly reserved for valid JSON-RPC 2.0 protocol messages. All diagnostics, initialization notices, and error logs are directed to standard error (`stderr`).

---

## Tool Catalog (47 Tools)

### 1. SQL Intelligence & Simulation
| Tool | Description |
| :--- | :--- |
| `validate_sql` | Offline AST syntax parser and static validator for SQL, window functions, CEP pattern matchers (`sequence_match`, `sequence_count`), `ASOF JOIN` syntax, and DDL statements (including `DEFAULT` column values and `BUFFER_FULL_POLICY`). |
| `test_sql_expression` | In-memory streaming query evaluator against mock JSON telemetry payloads, supporting vector search (`cosine_similarity`), CEP sequence patterns (`sequence_match`), array/object transforms (`json_map`, `object_erase`), and stateful analytics (`acc_distinct_collect`, `lead`). |
| `explain_sql` | Deconstructs streaming SQL into projections, sources, joins, and window clauses. |

### 2. Streams Management & Ingress
| Tool | Description |
| :--- | :--- |
| `list_streams` | Enumerate all registered streaming data sources. |
| `get_stream` | Inspect DDL schema, connector type, and serialization format. |
| `create_stream` | Register a new stream via streaming DDL statement. |
| `delete_stream` | Drop an existing stream from the catalog. |
| `push_stream_data` | Inject mock or real-time event payloads directly into a stream endpoint (supports `/streams/:name/data` or custom `TYPE="httppush"` endpoints, POST/PUT methods, and batch JSON arrays). |

### 3. Lookup Tables Management
| Tool | Description |
| :--- | :--- |
| `list_tables` | List all registered dimension and lookup tables. |
| `get_table` | Inspect table schema and backend storage configuration. |
| `create_table` | Create a dimension lookup table for stream enrichment joins. |
| `delete_table` | Drop a lookup table from the catalog. |
| `push_table_data` | Insert or update records in a lookup table. |

### 4. Rule Lifecycle & Orchestration
| Tool | Description |
| :--- | :--- |
| `list_rules` | List all deployed rules with execution statuses. |
| `get_rule` | Fetch complete JSON rule definition (SQL, action sinks, options). |
| `create_rule` | Deploy a new streaming rule pipeline with topology options. |
| `update_rule` | Update SQL logic, action sinks, or execution options for an existing rule. |
| `delete_rule` | Delete a rule pipeline from the engine. |
| `start_stop_rule` | Start, stop, or restart a rule execution pipeline. |
| `bulk_start_stop_rules` | Concurrently start, stop, or restart multiple rule pipelines. |
| `get_rule_status` | Retrieve real-time throughput metrics, latency, and error states. |
| `get_rule_topo` | Retrieve DAG topological execution graph of source, operator, and sink nodes. |
| `reset_rule_state` | Clear checkpointed state offsets and state store data for clean restart. |
| `explain_rule` | Retrieve structured JSON physical execution plan for a registered rule (`GET /rules/{name}/explain`). |

### 5. Distributed Tracing & Diagnostics
| Tool | Description |
| :--- | :--- |
| `start_rule_trace` | Activate real-time distributed tracing session for an active rule. |
| `stop_rule_trace` | Terminate distributed tracing session. |
| `get_rule_traces` | Retrieve list of recorded trace runs. |
| `get_trace_details` | Inspect per-operator step timings, inputs, and outputs of a trace session. |

### 6. Connection Pooling & Resource Management
| Tool | Description |
| :--- | :--- |
| `list_connections` | List all reusable connection resource definitions. |
| `create_connection` | Register a shared connection (MQTT, Kafka, Relational DB). |
| `delete_connection` | Remove a connection definition from the pool. |

### 7. Extensibility & Plugins
| Tool | Description |
| :--- | :--- |
| `list_plugins` | Enumerate installed native C/Rust or portable gRPC plugins. |
| `list_javascript_udfs` | List registered JavaScript User Defined Functions. |
| `create_javascript_udf` | Register a custom JavaScript scalar function for streaming SQL. |
| `delete_javascript_udf` | Remove a JavaScript UDF script. |
| `list_services` | Enumerate external microservices registered for RPC invocation. |

### 8. Global Configuration & Migrations
| Tool | Description |
| :--- | :--- |
| `get_configs` | Retrieve daemon runtime configuration and parameters. |
| `update_configs` | Dynamically update server configuration parameters. |
| `export_data` | Export JSON catalog backup of rules, streams, schemas, and configurations. |
| `import_data` | Import configurations from a JSON payload. Requires a non-empty payload. Supports state reset or `partial=true` merge mode. |

### 9. Health, Heartbeat & Telemetry
| Tool | Description |
| :--- | :--- |
| `get_engine_metrics` | Fetch engine uptime, memory/CPU consumption, and global throughput. |
| `ping_engine` | Test daemon connectivity and response latency. |

### 9. WebAssembly (WASM) & Dynamic Secrets Management
| Tool | Description |
| :--- | :--- |
| `register_wasm_plugin` | Register a compiled `.wasm` module into rekuiper's embedded runtime via `POST /plugins/wasm`. |
| `list_wasm_plugins` | Query all installed WebAssembly plugins and their exported UDF signatures. |
| `delete_wasm_plugin` | Unload and delete a WebAssembly plugin from the engine via `DELETE /plugins/wasm/{name}`. |
| `validate_secrets` | Offline syntax scanner and validator for dynamic secret templates (`{{vault://...}}`, `{{env://...}}`). |

### 10. Universal API Proxy
| Tool | Description |
| :--- | :--- |
| `execute_rekuiper_api` | Unconstrained REST proxy: execute any HTTP method (`GET`, `POST`, `PUT`, `DELETE`, `PATCH`) on any endpoint with arbitrary payloads. |

---

## Resources (URIs)

| URI | Description |
| :--- | :--- |
| `rekuiper://rules` | Active rules catalog with SQL queries and action sinks. |
| `rekuiper://streams` | Registered stream sources, schemas, and transport options. |
| `rekuiper://tables` | Dimension lookup tables for enrichment joins. |
| `rekuiper://connections` | Shared connection resource definitions. |
| `rekuiper://udfs` | Registered JavaScript UDFs. |
| `rekuiper://plugins` | Installed native and portable plugins. |
| `rekuiper://plugins/wasm` | Installed WebAssembly plugins and exported UDF signatures. |
| `rekuiper://configs` | Active global server configuration parameters. |
| `rekuiper://metrics` | Engine telemetry, memory usage, and throughput counters. |
| `rekuiper://metadata/sources` | Catalog of available source connectors. |
| `rekuiper://metadata/sinks` | Catalog of available sink connectors. |
| `rekuiper://metadata/functions` | Catalog of built-in SQL mathematical, string, and window functions. |
| `rekuiper://schemas/rabbitmq` | Configuration template for RabbitMQ AMQP 0-9-1 source and sink actions. |
| `rekuiper://schemas/parquet` | Configuration template for Apache Parquet columnar sink actions. |
| `rekuiper://schemas/edgex` | Configuration template for EdgeX Foundry dual-port listening (59880 / 59881). |

---

## Prompts

| Prompt | Purpose |
| :--- | :--- |
| `troubleshoot_rule` | Systematic diagnostic procedure for rules experiencing dropped messages or high latency. |
| `optimize_stream_sql` | SQL analysis for temporal window efficiency, memory pressure, and filter pushdown. |
| `generate_iot_alert_rule` | Generates end-to-end industrial IoT monitoring rules with deadbanding and alert sinks. |
| `create_end_to_end_pipeline` | Complete pipeline builder: DDL source stream, enrichment joins, windowing, and multiple sinks. |
| `diagnose_data_drop` | Root cause analysis for discrepancies between source message rate and sink throughput. |
| `generate_vector_search_rule` | Constructs an edge vector similarity search and anomaly detection rule using `cosine_similarity`. |
| `configure_rabbitmq_pipeline` | Constructs an enterprise pipeline routing stream records to RabbitMQ AMQP 0-9-1. |
| `create_wasm_plugin_rule` | Guides registration and stream query generation for compiled WebAssembly UDF modules. |
| `audit_security_posture` | Audits daemon runtime security configuration, network bindings, and authentication. |
| `generate_cep_rule` | Constructs a Complex Event Processing (CEP) sequence matching rule with `sequence_match` or `sequence_count`. |

---

## Client Configuration

### 1. Antigravity IDE / Claude Desktop (`mcp_config.json` or `claude_desktop_config.json`)

```json
{
  "mcpServers": {
    "rekuiper": {
      "command": "/usr/local/bin/rekuiper-mcp",
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

### 2. Windows Native (or via WSL)

```json
{
  "mcpServers": {
    "rekuiper": {
      "command": "wsl",
      "args": [
        "/usr/local/bin/rekuiper-mcp",
        "--server-url",
        "http://127.0.0.1:9081"
      ]
    }
  }
}
```

---

## Building from Source

```bash
# Debug build
cargo build -p rekuiper-mcp

# Optimized Release binary
cargo build --release -p rekuiper-mcp

# Run test suite
cargo test -p rekuiper-mcp
```
