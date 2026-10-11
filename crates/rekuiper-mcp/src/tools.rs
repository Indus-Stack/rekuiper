use crate::protocol::{CallToolResult, ToolDefinition};
use rekuiper_sql::eval::Evaluator;
use rekuiper_sql::parser::Parser;
use serde_json::{json, Value};
use std::collections::HashMap;

pub fn get_tool_definitions() -> Vec<ToolDefinition> {
    vec![
        // =========================================================================
        // 1. SQL Intelligence, AST Analysis & Offline Simulation
        // =========================================================================
        ToolDefinition {
            name: "validate_sql".to_string(),
            description: "Zero-network offline AST parser and static validator for rekuiper streaming SQL dialect and DDL syntax. Performs lexical tokenization, grammar parsing, column projection validation, WHERE expression type-checking, window clause verification (TumblingWindow, HoppingWindow, SlidingWindow, SessionWindow, CountWindow), and DDL schema validation (including BUFFER_FULL_POLICY='block|dropOldest', DATASOURCE, FORMAT) using native rekuiper-sql without connecting to or mutating a running daemon.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "The rekuiper SQL statement to parse and validate. Supports streaming SELECT queries (with window functions, joins, and aggregates) or DDL statements (CREATE STREAM, CREATE TABLE). Example: 'SELECT abs(vibe) AS v, avg(temp) AS avg_t FROM telemetry WHERE status = \"active\" GROUP BY TumblingWindow(ss, 10)'"
                    }
                },
                "required": ["sql"]
            }),
        },
        ToolDefinition {
            name: "test_sql_expression".to_string(),
            description: "In-memory streaming query simulator: parses and executes a rekuiper SQL SELECT query against a mock JSON event payload using the native rekuiper-sql evaluator. Tests vector similarity & distance functions (cosine_similarity, vector_l2, vector_dot, vector_match), array functions (array_positions, array_contains, deduplicate), streaming stateful analytics (lead, lag, acc_distinct_collect, distinct_acc, had_changed), mathematical transformations, and WHERE filter predicates in isolation, returning either the transformed JSON projection or a filtered notification.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "The rekuiper SQL SELECT query containing the projection expressions and optional WHERE filter clause. Example: 'SELECT cosine_similarity(features, [0.1, 0.4, 0.9]) AS sim FROM demo WHERE cosine_similarity(features, [0.1, 0.4, 0.9]) > 0.85'"
                    },
                    "data": {
                        "type": "object",
                        "description": "Mock input record represented as a JSON key-value object containing the simulated telemetry fields. Example: {\"features\": [0.12, 0.39, 0.88], \"status\": \"online\"}"
                    }
                },
                "required": ["sql", "data"]
            }),
        },
        ToolDefinition {
            name: "explain_sql".to_string(),
            description: "Deconstructs and analyzes the Abstract Syntax Tree (AST) of a rekuiper streaming SQL query. Extracts projected field expressions, source stream identifiers, JOIN conditions, WHERE filter predicates, GROUP BY partition keys, and time/count window specifications.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "The rekuiper streaming SQL query to deconstruct into structured AST components."
                    }
                },
                "required": ["sql"]
            }),
        },

        // =========================================================================
        // 2. Stream Management & Ingestion (DDL & Event Ingress)
        // =========================================================================
        ToolDefinition {
            name: "list_streams".to_string(),
            description: "Queries the rekuiper engine catalog to enumerate all registered streaming data sources, returning their stream names, underlying connectors, payload formats, and schemas.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {}
            }),
        },
        ToolDefinition {
            name: "get_stream".to_string(),
            description: "Retrieves the complete DDL schema definition and datasource configuration for a registered stream in the rekuiper engine catalog, including stream fields, payload serialization format (JSON, Protobuf, Binary), connector type (MQTT, EdgeX, Kafka, HTTP Pull/Push, Simulator), and connector options.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "The exact registered name of the stream to inspect (case-sensitive identifier matching the stream created via CREATE STREAM DDL)."
                    }
                },
                "required": ["name"]
            }),
        },
        ToolDefinition {
            name: "create_stream".to_string(),
            description: "Registers a new data stream in the rekuiper catalog by executing a streaming DDL statement. Configures data schemas and underlying connector bindings including protocol type, ingestion format, topic/datasource, and connection credentials.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "The complete rekuiper DDL statement specifying the stream name, optional typed field definitions, and datasource options in the WITH clause. Examples: 'CREATE STREAM telemetry (temp float, humidity float, status string) WITH (TYPE=\"mqtt\", FORMAT=\"json\", DATASOURCE=\"factory/line1/sensors\", CONF_KEY=\"broker_tls\")' or 'CREATE STREAM kafkaDemo () WITH (TYPE=\"kafka\", DATASOURCE=\"events\", OFFSET_RESET=\"earliest\")' or 'CREATE STREAM httpDemo () WITH (TYPE=\"httppush\", DATASOURCE=\"/api/data\", FORMAT=\"json\")'"
                    }
                },
                "required": ["sql"]
            }),
        },
        ToolDefinition {
            name: "delete_stream".to_string(),
            description: "Drops a registered data stream from the rekuiper catalog. Fails if active running rules currently depend on this stream as a source.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "The name of the stream to drop from the engine catalog."
                    }
                },
                "required": ["name"]
            }),
        },
        ToolDefinition {
            name: "push_stream_data".to_string(),
            description: "Directly ingests a mock or live JSON event payload into a stream via the rekuiper HTTP source endpoint (`/streams/:name/data` or custom `TYPE=\"httppush\"` endpoint). Allows injecting events into streaming pipelines for real-time testing, pipeline qualification, or edge REST-to-stream bridging.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Target stream name configured with HTTP datasource or standard stream accepting push data (used when endpoint is not specified)."
                    },
                    "endpoint": {
                        "type": "string",
                        "description": "Optional custom HTTP push endpoint path (e.g. '/api/data' or '/test_endpoint' as configured in `DATASOURCE` for `TYPE=\"httppush\"` streams). If omitted, defaults to '/streams/{name}/data'."
                    },
                    "method": {
                        "type": "string",
                        "enum": ["POST", "PUT"],
                        "description": "HTTP method to use when pushing data (defaults to 'POST')."
                    },
                    "data": {
                        "description": "The event payload to ingest: can be a single JSON object representing a discrete event (e.g. {\"temperature\": 24.5, \"vibration\": 0.12}) or an array of event objects for batch ingestion."
                    }
                },
                "required": ["data"]
            }),
        },

        // =========================================================================
        // 3. Table Management & Ingestion (Enrichment Dimensions)
        // =========================================================================
        ToolDefinition {
            name: "list_tables".to_string(),
            description: "Lists all registered dimension and lookup tables in the rekuiper catalog. Lookup tables provide static or slowly changing enrichment data for joining with real-time event streams.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {}
            }),
        },
        ToolDefinition {
            name: "get_table".to_string(),
            description: "Retrieves the schema and storage backend configuration for a specific lookup table (e.g., File, SQLite, Redis, or Memory).".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "The identifier of the lookup table to fetch from the engine catalog."
                    }
                },
                "required": ["name"]
            }),
        },
        ToolDefinition {
            name: "create_table".to_string(),
            description: "Registers a new lookup table in the rekuiper catalog using a Table DDL statement. Configures schema types and external storage providers (file, sqlite, redis, sql, or memory) for stream-to-table enrichment joins.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "The table DDL statement defining column types, primary key, and storage bindings in the WITH clause. Example: 'CREATE TABLE device_meta (device_id string, location string, model string) WITH (TYPE=\"file\", FORMAT=\"json\", DATASOURCE=\"etc/data/devices.json\", KEY=\"device_id\")'"
                    }
                },
                "required": ["sql"]
            }),
        },
        ToolDefinition {
            name: "delete_table".to_string(),
            description: "Drops a lookup table from the rekuiper catalog. Fails if active running rules currently join with this table.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "The identifier of the lookup table to drop."
                    }
                },
                "required": ["name"]
            }),
        },
        ToolDefinition {
            name: "push_table_data".to_string(),
            description: "Inserts or updates records in a rekuiper lookup table via HTTP ingestion (`/tables/:name/data`), enabling dynamic runtime updates of reference metadata used in stream joins.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Name of the target lookup table."
                    },
                    "data": {
                        "description": "JSON object or array of JSON objects representing table records with primary keys to insert or update."
                    }
                },
                "required": ["name", "data"]
            }),
        },

        // =========================================================================
        // 4. Rule Lifecycle & Real-Time Operations
        // =========================================================================
        ToolDefinition {
            name: "list_rules".to_string(),
            description: "Retrieves the catalog of all stream processing rules deployed in the rekuiper engine, including their unique rule identifiers, current execution state (running, stopped, crashed), and creation metadata.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {}
            }),
        },
        ToolDefinition {
            name: "get_rule".to_string(),
            description: "Fetches the full specification of a deployed rule, including its SQL query, target action sinks (MQTT, Kafka, Redis, SQL, Log, REST, File), transformation data templates, runtime execution options, and topological graph.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Unique identifier of the rule to inspect."
                    }
                },
                "required": ["name"]
            }),
        },
        ToolDefinition {
            name: "create_rule".to_string(),
            description: "Deploys a new continuous stream processing rule to the rekuiper engine. Compiles the SQL query into an execution DAG, binds action sinks, initializes state storage, and starts or queues the rule based on options.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Unique alphanumeric identifier for the rule (e.g. 'rule_temp_anomaly_detector')."
                    },
                    "sql": {
                        "type": "string",
                        "description": "The streaming SQL query executing continuously across incoming stream tuples, window buffers, and joins. Example: 'SELECT device_id, avg(temp) AS avg_temp FROM telemetry GROUP BY device_id, TumblingWindow(ss, 30) HAVING avg(temp) > 80.0'"
                    },
                    "actions": {
                        "type": "array",
                        "description": "Array of output sink configurations defining where processed results are dispatched. Common sink options include: 'sendSingle' (boolean, default false for REST/MQTT/WebSocket, true for File/Redis/Kafka), 'fields' (array of projected field names), 'excludeFields' (array of fields to omit), 'dataField' (string, extracts nested object), 'format' ('json', 'delimited', etc.), 'delimiter' (string), 'batchSize' (buffer record count), and 'lingerInterval' (buffer flush timeout ms). Example: [{\"mqtt\": {\"server\": \"tcp://127.0.0.1:1883\", \"topic\": \"factory/alerts\", \"sendSingle\": true, \"dataTemplate\": \"{\\\"alert\\\": \\\"overheat\\\", \\\"device\\\": \\\"{{.device_id}}\\\", \\\"avg_temp\\\": {{.avg_temp}}}\"}}]"
                    },
                    "options": {
                        "type": "object",
                        "description": "Optional runtime pipeline tuning parameters: `isEventTime` (boolean, enables watermark timestamp processing), `lateTolerance` (integer ms, maximum allowed out-of-order latency), `concurrency` (integer, parallel worker threads), `bufferLength` (integer, bounded queue buffer capacity), `checkpointInterval` (integer ms, state snapshot persistence interval), `sendError` (boolean, emits rule execution exceptions to error sink)."
                    }
                },
                "required": ["name", "sql", "actions"]
            }),
        },
        ToolDefinition {
            name: "update_rule".to_string(),
            description: "Updates the streaming query, action sinks, or runtime options of an existing rule. If the rule is actively running, it is hot-reloaded with the new topology and state.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Identifier of the rule to update."
                    },
                    "sql": {
                        "type": "string",
                        "description": "Updated streaming SQL query."
                    },
                    "actions": {
                        "type": "array",
                        "description": "Updated action sink definitions."
                    },
                    "options": {
                        "type": "object",
                        "description": "Updated runtime execution options."
                    }
                },
                "required": ["name"]
            }),
        },
        ToolDefinition {
            name: "delete_rule".to_string(),
            description: "Terminates execution (if running) and deletes a stream processing rule from the rekuiper engine, releasing all allocated topological resources, window buffers, and state stores.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Identifier of the rule to delete."
                    }
                },
                "required": ["name"]
            }),
        },
        ToolDefinition {
            name: "start_stop_rule".to_string(),
            description: "Controls the runtime lifecycle state of an individual stream processing rule on the rekuiper engine. Transitions the rule between 'running' and 'stopped', or restarts its execution pipeline.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Identifier of the target rule."
                    },
                    "action": {
                        "type": "string",
                        "enum": ["start", "stop", "restart"],
                        "description": "Lifecycle operation to perform: 'start' (spawns source readers and stream operators), 'stop' (gracefully shuts down sinks and pauses ingestion), or 'restart' (flushes pipeline and reinitializes)."
                    }
                },
                "required": ["name", "action"]
            }),
        },
        ToolDefinition {
            name: "bulk_start_stop_rules".to_string(),
            description: "Simultaneously starts or stops multiple stream processing rules in batch, or all rules deployed on the engine.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["start", "stop"],
                        "description": "Batch command: 'start' or 'stop'."
                    },
                    "rules": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Optional array of rule identifiers to target. If omitted or empty, applies to all deployed rules across the engine."
                    }
                },
                "required": ["action"]
            }),
        },
        ToolDefinition {
            name: "get_rule_status".to_string(),
            description: "Retrieves deep real-time operational telemetry for a rule: execution state, total messages read from sources, tuples emitted to sinks, dropped records (filtered vs error), exceptions count, and processing latency.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Identifier of the rule whose runtime metrics to query."
                    }
                },
                "required": ["name"]
            }),
        },
        ToolDefinition {
            name: "get_rule_topo".to_string(),
            description: "Retrieves the directed acyclic graph (DAG) topological schema of a deployed rule, detailing all source nodes, filter/projection operators, window accumulators, and sink nodes with internal channel connections.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Identifier of the rule whose execution DAG to inspect."
                    }
                },
                "required": ["name"]
            }),
        },
        ToolDefinition {
            name: "reset_rule_state".to_string(),
            description: "Flushes and resets the internal state stores of a running rule, clearing tumbling/sliding window buffers, join hash tables, and incremental aggregation accumulators without deleting the rule definition.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Identifier of the rule whose state store to reset."
                    }
                },
                "required": ["name"]
            }),
        },
        ToolDefinition {
            name: "explain_rule".to_string(),
            description: "Retrieves the structured JSON physical execution plan of a registered rule from the running engine via GET /rules/{name}/explain.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Identifier of the rule whose execution plan to inspect."
                    }
                },
                "required": ["name"]
            }),
        },

        // =========================================================================
        // 5. Tracing, Observability & Root-Cause Diagnostics
        // =========================================================================
        ToolDefinition {
            name: "start_rule_trace".to_string(),
            description: "Enables OpenTelemetry-compatible runtime message tracing on a rule to capture detailed end-to-end execution spans (source decode -> SQL transformation -> sink delivery) for individual tuples.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Identifier of the rule to trace."
                    },
                    "strategy": {
                        "type": "string",
                        "description": "Sampling strategy: 'always' (trace all records), 'sampling' (probabilistic sampling), or 'on-error' (trace failed/dropped records only)."
                    }
                },
                "required": ["name"]
            }),
        },
        ToolDefinition {
            name: "stop_rule_trace".to_string(),
            description: "Disables runtime message tracing on a rule to eliminate tracing overhead once diagnostics are complete.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Identifier of the rule on which to stop tracing."
                    }
                },
                "required": ["name"]
            }),
        },
        ToolDefinition {
            name: "get_rule_traces".to_string(),
            description: "Queries captured execution trace IDs for a rule, showing recent processed records and their corresponding span identifiers.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "rule_id": {
                        "type": "string",
                        "description": "Identifier of the rule."
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of recent trace IDs to retrieve (default: 20)."
                    }
                },
                "required": ["rule_id"]
            }),
        },
        ToolDefinition {
            name: "get_trace_details".to_string(),
            description: "Fetches full trace span tree for a specific trace ID, displaying start/end timestamps, step-by-step latency, input payload attributes, projection outputs, and sink delivery statuses.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "id": {
                        "type": "string",
                        "description": "The 32-character hexadecimal trace identifier to inspect."
                    }
                },
                "required": ["id"]
            }),
        },

        // =========================================================================
        // 6. Shared Connection Pooling & Enterprise Integration
        // =========================================================================
        ToolDefinition {
            name: "list_connections".to_string(),
            description: "Enumerates all reusable shared connection resources (e.g. centralized MQTT broker pools, shared relational database pools, Kafka cluster client configurations) configured on the rekuiper engine.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {}
            }),
        },
        ToolDefinition {
            name: "create_connection".to_string(),
            description: "Creates or updates a shared reusable connection resource, enabling multiple streams and sinks to share connection pooling, credentials, and TLS certificates without duplicating configs.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "id": {
                        "type": "string",
                        "description": "Unique identifier for the connection resource (e.g. 'factory_mqtt_broker')."
                    },
                    "typ": {
                        "type": "string",
                        "description": "Protocol connection driver: 'mqtt', 'sql', 'kafka', or 'edgex'."
                    },
                    "props": {
                        "type": "object",
                        "description": "Driver configuration object (e.g. server URL, client ID, username, password, TLS certificates, pool size)."
                    }
                },
                "required": ["id", "typ", "props"]
            }),
        },
        ToolDefinition {
            name: "delete_connection".to_string(),
            description: "Deletes a shared connection resource from the engine catalog. Fails if active streams or sinks currently reference this connection.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "id": {
                        "type": "string",
                        "description": "Identifier of the connection resource to remove."
                    }
                },
                "required": ["id"]
            }),
        },

        // =========================================================================
        // 7. Plugins, JavaScript UDFs & Microservices
        // =========================================================================
        ToolDefinition {
            name: "list_plugins".to_string(),
            description: "Lists installed native and portable plugins extending rekuiper with custom source connectors, sink targets, and SQL functions.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "plugin_type": {
                        "type": "string",
                        "enum": ["sources", "sinks", "functions", "portables"],
                        "description": "Plugin category to list: 'sources' (input connectors), 'sinks' (output connectors), 'functions' (scalar/aggregate functions), or 'portables' (multi-language gRPC plugins). Default: 'sources'."
                    }
                }
            }),
        },
        ToolDefinition {
            name: "list_javascript_udfs".to_string(),
            description: "Lists all registered JavaScript User Defined Functions (UDFs) available for inline execution within streaming SQL queries.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {}
            }),
        },
        ToolDefinition {
            name: "create_javascript_udf".to_string(),
            description: "Registers a custom JavaScript UDF script in the engine, enabling arbitrary algorithms, complex math, or custom parsers directly in SQL SELECT and WHERE clauses.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "id": {
                        "type": "string",
                        "description": "Function name as callable in SQL statements (e.g. 'fahrenheit_to_celsius')."
                    },
                    "script": {
                        "type": "string",
                        "description": "JavaScript function source code. Example: 'function fahrenheit_to_celsius(f) { return (f - 32.0) * 5.0 / 9.0; }'"
                    },
                    "description": {
                        "type": "string",
                        "description": "Documentation string explaining function inputs, outputs, and purpose."
                    }
                },
                "required": ["id", "script"]
            }),
        },
        ToolDefinition {
            name: "delete_javascript_udf".to_string(),
            description: "Removes a registered JavaScript UDF from the engine.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "id": {
                        "type": "string",
                        "description": "Identifier of the JavaScript UDF to delete."
                    }
                },
                "required": ["id"]
            }),
        },
        ToolDefinition {
            name: "list_services".to_string(),
            description: "Lists external microservices registered in rekuiper for remote RPC function invocation during stream processing.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {}
            }),
        },

        // =========================================================================
        // 8. Global Configuration & Data Migrations
        // =========================================================================
        ToolDefinition {
            name: "get_configs".to_string(),
            description: "Retrieves global runtime configuration parameters for the rekuiper daemon, including logging level, default source/sink timeouts, buffer capacities, and TLS settings.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {}
            }),
        },
        ToolDefinition {
            name: "update_configs".to_string(),
            description: "Dynamically patches global server configurations without restarting the daemon.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "configs": {
                        "type": "object",
                        "description": "Key-value configuration map of parameters to update."
                    }
                },
                "required": ["configs"]
            }),
        },
        ToolDefinition {
            name: "export_data".to_string(),
            description: "Exports a complete or selective JSON backup of all registered rules, streams, schemas, lookup tables, and configuration settings for disaster recovery or migration.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "rules": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Optional array of specific rule names to include. If omitted, exports all rules."
                    },
                    "streams": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Optional array of specific stream names to include. If omitted, exports all streams."
                    }
                }
            }),
        },
        ToolDefinition {
            name: "import_data".to_string(),
            description: "Imports rules, streams, tables, and configurations into the engine. The request payload must not be empty. By default, this tool clears old configurations before import. Set partial=true to merge configurations without clearing old resources.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "content": {
                        "description": "The JSON configuration payload to import. Must not be empty."
                    },
                    "partial": {
                        "type": "boolean",
                        "description": "Set to true to merge configurations without clearing old resources."
                    }
                },
                "required": ["content"]
            }),
        },

        // =========================================================================
        // 9. Engine Health, Telemetry & Heartbeat
        // =========================================================================
        ToolDefinition {
            name: "get_engine_metrics".to_string(),
            description: "Fetches comprehensive system-level telemetry: engine uptime, CPU consumption, memory allocation, active rule counts, and aggregate throughput across all streaming pipelines.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {}
            }),
        },
        ToolDefinition {
            name: "ping_engine".to_string(),
            description: "Sends a heartbeat ping to verify the health, responsiveness, and connectivity of the target rekuiper daemon.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {}
            }),
        },

        // =========================================================================
        // 10. Universal API Proxy ("Do Anything and Everything")
        // =========================================================================
        ToolDefinition {
            name: "execute_rekuiper_api".to_string(),
            description: "Universal REST proxy providing unconstrained access to EVERY present and future endpoint in the rekuiper REST API. Allows invoking arbitrary HTTP methods (GET, POST, PUT, DELETE, PATCH) against any path (e.g. '/metadata/sources', '/metadata/sinks', '/metadata/functions', '/rules/:name/cpu', '/rules/tags/match', '/async/task/:id'), with arbitrary query strings and JSON payloads. Ensures total feature completeness without API limitations.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "method": {
                        "type": "string",
                        "enum": ["GET", "POST", "PUT", "DELETE", "PATCH"],
                        "description": "HTTP method verb: 'GET', 'POST', 'PUT', 'DELETE', or 'PATCH'."
                    },
                    "endpoint": {
                        "type": "string",
                        "description": "API path relative to engine root (e.g. '/metadata/sources', '/rules/rule1/cpu', '/configs')."
                    },
                    "body": {
                        "description": "Optional JSON payload for request body on POST, PUT, or PATCH requests."
                    }
                },
                "required": ["method", "endpoint"]
            }),
        },

        // =========================================================================
        // 11. WebAssembly (Wasm) Plugin Management & Dynamic Secrets
        // =========================================================================
        ToolDefinition {
            name: "register_wasm_plugin".to_string(),
            description: "Registers a WebAssembly (.wasm) plugin module into rekuiper's embedded Wasm runtime via POST /plugins/wasm. The module's exported functions immediately become callable as native UDFs inside streaming SQL queries or via wasm_run(module, func, ...).".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Unique module identifier for the WASM plugin (e.g. 'math_wasm', 'anomaly_detector')."
                    },
                    "path": {
                        "type": "string",
                        "description": "Filesystem path or file URL to the compiled .wasm binary file on the server (e.g. '/plugins/wasm/math.wasm')."
                    },
                    "description": {
                        "type": "string",
                        "description": "Optional human-readable description of the plugin and its exported UDFs."
                    }
                },
                "required": ["name", "path"]
            }),
        },
        ToolDefinition {
            name: "list_wasm_plugins".to_string(),
            description: "Lists all installed WebAssembly (.wasm) plugins and their exported function signatures registered in the rekuiper daemon via GET /plugins/wasm.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {}
            }),
        },
        ToolDefinition {
            name: "delete_wasm_plugin".to_string(),
            description: "Unregisters and unloads a WebAssembly (.wasm) plugin module from the rekuiper daemon via DELETE /plugins/wasm/{name}.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "The unique name of the WASM plugin module to remove."
                    }
                },
                "required": ["name"]
            }),
        },
        ToolDefinition {
            name: "validate_secrets".to_string(),
            description: "Statically scans and validates dynamic secret template expressions (such as {{vault://path/to/key}} or {{env://VAR_NAME}}) in pipeline configs, connector properties, or sink options without exposing sensitive credentials.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "text": {
                        "type": "string",
                        "description": "Configuration string, JSON snippet, or template to scan for dynamic secret references."
                    }
                },
                "required": ["text"]
            }),
        },
    ]
}

async fn forward_request(
    client: &reqwest::Client,
    base_url: &str,
    method: reqwest::Method,
    path: &str,
    body: Option<Value>,
) -> CallToolResult {
    let clean_path = if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{}", path)
    };
    let url = format!("{}{}", base_url.trim_end_matches('/'), clean_path);
    let mut req = client.request(method, &url);
    if let Some(b) = body {
        req = req.json(&b);
    }
    match req.send().await {
        Ok(resp) => {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            if status.is_success() {
                CallToolResult::ok(text)
            } else {
                CallToolResult::err(format!("rekuiper error (HTTP {}): {}", status, text))
            }
        }
        Err(e) => CallToolResult::err(format!("Connection failed to rekuiper at {}: {}", url, e)),
    }
}

pub async fn execute_tool(
    client: &reqwest::Client,
    base_url: &str,
    name: &str,
    args: Value,
) -> CallToolResult {
    match name {
        // --- 1. SQL Analysis & Simulator ---
        "validate_sql" => {
            let Some(sql) = args.get("sql").and_then(|v| v.as_str()) else {
                return CallToolResult::err("Missing required string parameter: 'sql'");
            };
            let mut parser = Parser::new(sql);
            match parser.parse_select() {
                Ok(stmt) => {
                    let info = json!({
                        "valid": true,
                        "statement_type": "SELECT",
                        "projected_fields_count": stmt.fields.len(),
                        "has_where_clause": stmt.where_clause.is_some(),
                        "has_group_by": !stmt.group_by.is_empty(),
                        "has_window": stmt.window.is_some(),
                        "has_having": stmt.having.is_some(),
                        "sources": stmt.from,
                    });
                    CallToolResult::ok(serde_json::to_string_pretty(&info).unwrap_or_default())
                }
                Err(err) => {
                    let mut p_stream = Parser::new(sql);
                    if let Ok(create_stmt) = p_stream.parse_create_stream() {
                        let info = json!({
                            "valid": true,
                            "statement_type": "CREATE STREAM",
                            "stream_name": create_stmt.name,
                            "options": create_stmt.options,
                        });
                        return CallToolResult::ok(
                            serde_json::to_string_pretty(&info).unwrap_or_default(),
                        );
                    }
                    let mut p_table = Parser::new(sql);
                    if let Ok(create_table_stmt) = p_table.parse_create_table() {
                        let info = json!({
                            "valid": true,
                            "statement_type": "CREATE TABLE",
                            "table_name": create_table_stmt.name,
                            "options": create_table_stmt.options,
                        });
                        return CallToolResult::ok(
                            serde_json::to_string_pretty(&info).unwrap_or_default(),
                        );
                    }
                    CallToolResult::err(format!("SQL Syntax / Grammar Error: {}", err))
                }
            }
        }

        "test_sql_expression" => {
            let Some(sql) = args.get("sql").and_then(|v| v.as_str()) else {
                return CallToolResult::err("Missing required string parameter: 'sql'");
            };
            let Some(data_obj) = args.get("data").and_then(|v| v.as_object()) else {
                return CallToolResult::err("Missing required object parameter: 'data'");
            };

            let mut parser = Parser::new(sql);
            let stmt = match parser.parse_select() {
                Ok(s) => s,
                Err(e) => return CallToolResult::err(format!("SQL parse failed: {}", e)),
            };

            let record: HashMap<String, Value> = data_obj
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();

            match Evaluator::eval_select(&stmt, &record) {
                Some(projected) => {
                    let out = json!({
                        "status": "matched",
                        "output": projected
                    });
                    CallToolResult::ok(serde_json::to_string_pretty(&out).unwrap_or_default())
                }
                None => {
                    let out = json!({
                        "status": "filtered",
                        "message": "Input record was filtered out by WHERE clause condition."
                    });
                    CallToolResult::ok(serde_json::to_string_pretty(&out).unwrap_or_default())
                }
            }
        }

        "explain_sql" => {
            let Some(sql) = args.get("sql").and_then(|v| v.as_str()) else {
                return CallToolResult::err("Missing required string parameter: 'sql'");
            };
            let mut parser = Parser::new(sql);
            match parser.parse_select() {
                Ok(stmt) => {
                    let fields: Vec<String> =
                        stmt.fields.iter().map(|f| format!("{:?}", f)).collect();
                    let info = json!({
                        "valid": true,
                        "projections": fields,
                        "sources": stmt.from,
                        "where_predicate": stmt.where_clause.map(|w| format!("{:?}", w)),
                        "dimensions": stmt.group_by.iter().map(|g| format!("{:?}", g)).collect::<Vec<_>>(),
                        "window": stmt.window.map(|w| format!("{:?}", w)),
                    });
                    CallToolResult::ok(serde_json::to_string_pretty(&info).unwrap_or_default())
                }
                Err(e) => CallToolResult::err(format!("Failed to parse query: {}", e)),
            }
        }

        // --- 2. Stream Management ---
        "list_streams" => {
            forward_request(client, base_url, reqwest::Method::GET, "/streams", None).await
        }

        "get_stream" => {
            let Some(name) = args.get("name").and_then(|v| v.as_str()) else {
                return CallToolResult::err("Missing required parameter: 'name'");
            };
            let path = format!("/streams/{}", name);
            forward_request(client, base_url, reqwest::Method::GET, &path, None).await
        }

        "create_stream" => {
            let Some(sql) = args.get("sql").and_then(|v| v.as_str()) else {
                return CallToolResult::err("Missing required parameter: 'sql'");
            };
            let body = json!({ "sql": sql });
            forward_request(
                client,
                base_url,
                reqwest::Method::POST,
                "/streams",
                Some(body),
            )
            .await
        }

        "delete_stream" => {
            let Some(name) = args.get("name").and_then(|v| v.as_str()) else {
                return CallToolResult::err("Missing required parameter: 'name'");
            };
            let path = format!("/streams/{}", name);
            forward_request(client, base_url, reqwest::Method::DELETE, &path, None).await
        }

        "push_stream_data" => {
            let endpoint = args.get("endpoint").and_then(|v| v.as_str());
            let name = args.get("name").and_then(|v| v.as_str());
            let path = match (endpoint, name) {
                (Some(ep), _) => ep.to_string(),
                (None, Some(n)) => format!("/streams/{}/data", n),
                (None, None) => {
                    return CallToolResult::err(
                        "Missing required parameter: provide either 'name' or 'endpoint'",
                    );
                }
            };
            let method = match args
                .get("method")
                .and_then(|v| v.as_str())
                .map(|s| s.to_uppercase())
                .as_deref()
            {
                Some("PUT") => reqwest::Method::PUT,
                _ => reqwest::Method::POST,
            };
            let data = args.get("data").cloned().unwrap_or(json!({}));
            forward_request(client, base_url, method, &path, Some(data)).await
        }

        // --- 3. Table Management ---
        "list_tables" => {
            forward_request(client, base_url, reqwest::Method::GET, "/tables", None).await
        }

        "get_table" => {
            let Some(name) = args.get("name").and_then(|v| v.as_str()) else {
                return CallToolResult::err("Missing required parameter: 'name'");
            };
            let path = format!("/tables/{}", name);
            forward_request(client, base_url, reqwest::Method::GET, &path, None).await
        }

        "create_table" => {
            let Some(sql) = args.get("sql").and_then(|v| v.as_str()) else {
                return CallToolResult::err("Missing required parameter: 'sql'");
            };
            let body = json!({ "sql": sql });
            forward_request(
                client,
                base_url,
                reqwest::Method::POST,
                "/tables",
                Some(body),
            )
            .await
        }

        "delete_table" => {
            let Some(name) = args.get("name").and_then(|v| v.as_str()) else {
                return CallToolResult::err("Missing required parameter: 'name'");
            };
            let path = format!("/tables/{}", name);
            forward_request(client, base_url, reqwest::Method::DELETE, &path, None).await
        }

        "push_table_data" => {
            let Some(name) = args.get("name").and_then(|v| v.as_str()) else {
                return CallToolResult::err("Missing required parameter: 'name'");
            };
            let data = args.get("data").cloned().unwrap_or(json!({}));
            let path = format!("/tables/{}/data", name);
            forward_request(client, base_url, reqwest::Method::POST, &path, Some(data)).await
        }

        // --- 4. Rule Lifecycle & Operations ---
        "list_rules" => {
            forward_request(client, base_url, reqwest::Method::GET, "/rules", None).await
        }

        "get_rule" => {
            let Some(name) = args.get("name").and_then(|v| v.as_str()) else {
                return CallToolResult::err("Missing required parameter: 'name'");
            };
            let path = format!("/rules/{}", name);
            forward_request(client, base_url, reqwest::Method::GET, &path, None).await
        }

        "create_rule" => {
            let Some(name) = args.get("name").and_then(|v| v.as_str()) else {
                return CallToolResult::err("Missing required parameter: 'name'");
            };
            let Some(sql) = args.get("sql").and_then(|v| v.as_str()) else {
                return CallToolResult::err("Missing required parameter: 'sql'");
            };
            let Some(actions) = args.get("actions") else {
                return CallToolResult::err("Missing required parameter: 'actions'");
            };

            let mut body = json!({
                "id": name,
                "sql": sql,
                "actions": actions
            });
            if let Some(opts) = args.get("options") {
                body["options"] = opts.clone();
            }

            forward_request(
                client,
                base_url,
                reqwest::Method::POST,
                "/rules",
                Some(body),
            )
            .await
        }

        "update_rule" => {
            let Some(name) = args.get("name").and_then(|v| v.as_str()) else {
                return CallToolResult::err("Missing required parameter: 'name'");
            };
            let mut body = json!({ "id": name });
            if let Some(sql) = args.get("sql") {
                body["sql"] = sql.clone();
            }
            if let Some(actions) = args.get("actions") {
                body["actions"] = actions.clone();
            }
            if let Some(options) = args.get("options") {
                body["options"] = options.clone();
            }
            let path = format!("/rules/{}", name);
            forward_request(client, base_url, reqwest::Method::PUT, &path, Some(body)).await
        }

        "delete_rule" => {
            let Some(name) = args.get("name").and_then(|v| v.as_str()) else {
                return CallToolResult::err("Missing required parameter: 'name'");
            };
            let path = format!("/rules/{}", name);
            forward_request(client, base_url, reqwest::Method::DELETE, &path, None).await
        }

        "start_stop_rule" => {
            let Some(name) = args.get("name").and_then(|v| v.as_str()) else {
                return CallToolResult::err("Missing required parameter: 'name'");
            };
            let Some(action) = args.get("action").and_then(|v| v.as_str()) else {
                return CallToolResult::err("Missing required parameter: 'action'");
            };
            let path = format!("/rules/{}/{}", name, action);
            forward_request(client, base_url, reqwest::Method::POST, &path, None).await
        }

        "bulk_start_stop_rules" => {
            let Some(action) = args.get("action").and_then(|v| v.as_str()) else {
                return CallToolResult::err("Missing required parameter: 'action'");
            };
            let path = if action == "start" {
                "/rules/bulkstart"
            } else {
                "/rules/bulkstop"
            };
            let body = args.get("rules").cloned();
            forward_request(client, base_url, reqwest::Method::POST, path, body).await
        }

        "get_rule_status" => {
            let Some(name) = args.get("name").and_then(|v| v.as_str()) else {
                return CallToolResult::err("Missing required parameter: 'name'");
            };
            let path = format!("/rules/{}/status", name);
            forward_request(client, base_url, reqwest::Method::GET, &path, None).await
        }

        "get_rule_topo" => {
            let Some(name) = args.get("name").and_then(|v| v.as_str()) else {
                return CallToolResult::err("Missing required parameter: 'name'");
            };
            let path = format!("/rules/{}/topo", name);
            forward_request(client, base_url, reqwest::Method::GET, &path, None).await
        }

        "reset_rule_state" => {
            let Some(name) = args.get("name").and_then(|v| v.as_str()) else {
                return CallToolResult::err("Missing required parameter: 'name'");
            };
            let path = format!("/rules/{}/reset_state", name);
            forward_request(client, base_url, reqwest::Method::PUT, &path, None).await
        }
        "explain_rule" => {
            let Some(name) = args.get("name").and_then(|v| v.as_str()) else {
                return CallToolResult::err("Missing required parameter: 'name'");
            };
            let path = format!("/rules/{}/explain", name);
            forward_request(client, base_url, reqwest::Method::GET, &path, None).await
        }

        // --- 5. Tracing & Observability ---
        "start_rule_trace" => {
            let Some(name) = args.get("name").and_then(|v| v.as_str()) else {
                return CallToolResult::err("Missing required parameter: 'name'");
            };
            let path = format!("/rules/{}/trace/start", name);
            let strategy = args
                .get("strategy")
                .and_then(|v| v.as_str())
                .unwrap_or("always");
            forward_request(
                client,
                base_url,
                reqwest::Method::POST,
                &path,
                Some(json!({ "strategy": strategy })),
            )
            .await
        }

        "stop_rule_trace" => {
            let Some(name) = args.get("name").and_then(|v| v.as_str()) else {
                return CallToolResult::err("Missing required parameter: 'name'");
            };
            let path = format!("/rules/{}/trace/stop", name);
            forward_request(client, base_url, reqwest::Method::POST, &path, None).await
        }

        "get_rule_traces" => {
            let Some(rule_id) = args.get("rule_id").and_then(|v| v.as_str()) else {
                return CallToolResult::err("Missing required parameter: 'rule_id'");
            };
            let limit = args.get("limit").and_then(|v| v.as_i64()).unwrap_or(20);
            let path = format!("/trace/rule/{}?limit={}", rule_id, limit);
            forward_request(client, base_url, reqwest::Method::GET, &path, None).await
        }

        "get_trace_details" => {
            let Some(id) = args.get("id").and_then(|v| v.as_str()) else {
                return CallToolResult::err("Missing required parameter: 'id'");
            };
            let path = format!("/trace/{}", id);
            forward_request(client, base_url, reqwest::Method::GET, &path, None).await
        }

        // --- 6. Shared Connection Pooling ---
        "list_connections" => {
            forward_request(client, base_url, reqwest::Method::GET, "/connections", None).await
        }

        "create_connection" => {
            forward_request(
                client,
                base_url,
                reqwest::Method::POST,
                "/connections",
                Some(args),
            )
            .await
        }

        "delete_connection" => {
            let Some(id) = args.get("id").and_then(|v| v.as_str()) else {
                return CallToolResult::err("Missing required parameter: 'id'");
            };
            let path = format!("/connections/{}", id);
            forward_request(client, base_url, reqwest::Method::DELETE, &path, None).await
        }

        // --- 7. Plugins, UDFs & Services ---
        "list_plugins" => {
            let ptype = args
                .get("plugin_type")
                .and_then(|v| v.as_str())
                .unwrap_or("sources");
            let path = format!("/plugins/{}", ptype);
            forward_request(client, base_url, reqwest::Method::GET, &path, None).await
        }

        "list_javascript_udfs" => {
            forward_request(
                client,
                base_url,
                reqwest::Method::GET,
                "/udf/javascript",
                None,
            )
            .await
        }

        "create_javascript_udf" => {
            forward_request(
                client,
                base_url,
                reqwest::Method::POST,
                "/udf/javascript",
                Some(args),
            )
            .await
        }

        "delete_javascript_udf" => {
            let Some(id) = args.get("id").and_then(|v| v.as_str()) else {
                return CallToolResult::err("Missing required parameter: 'id'");
            };
            let path = format!("/udf/javascript/{}", id);
            forward_request(client, base_url, reqwest::Method::DELETE, &path, None).await
        }

        "list_services" => {
            forward_request(client, base_url, reqwest::Method::GET, "/services", None).await
        }

        // --- 8. Configuration & Migrations ---
        "get_configs" => {
            forward_request(client, base_url, reqwest::Method::GET, "/configs", None).await
        }

        "update_configs" => {
            let configs = args.get("configs").cloned().unwrap_or(json!({}));
            forward_request(
                client,
                base_url,
                reqwest::Method::PATCH,
                "/configs",
                Some(configs),
            )
            .await
        }

        "export_data" => {
            if args.get("rules").is_some() || args.get("streams").is_some() {
                forward_request(
                    client,
                    base_url,
                    reqwest::Method::POST,
                    "/data/export",
                    Some(args),
                )
                .await
            } else {
                forward_request(client, base_url, reqwest::Method::GET, "/data/export", None).await
            }
        }

        "import_data" => {
            let content = args.get("content").cloned().unwrap_or(json!({}));
            let is_partial = args
                .get("partial")
                .and_then(|v| {
                    if v.as_bool() == Some(true)
                        || v.as_str() == Some("1")
                        || v.as_str() == Some("true")
                    {
                        Some("?partial=1")
                    } else {
                        None
                    }
                })
                .unwrap_or("");
            let path = format!("/data/import{}", is_partial);
            forward_request(
                client,
                base_url,
                reqwest::Method::POST,
                &path,
                Some(content),
            )
            .await
        }

        // --- 9. Engine Health & Telemetry ---
        "get_engine_metrics" => {
            let status_url = format!("{}/rules/status/all", base_url.trim_end_matches('/'));
            let all_status = match client.get(&status_url).send().await {
                Ok(resp) => resp.text().await.ok(),
                Err(_) => None,
            };

            let rule_list_url = format!("{}/rules", base_url.trim_end_matches('/'));
            let rules_count = match client.get(&rule_list_url).send().await {
                Ok(resp) => resp
                    .json::<Vec<String>>()
                    .await
                    .ok()
                    .map(|v| v.len())
                    .unwrap_or(0),
                Err(_) => 0,
            };

            let res = json!({
                "engine": "rekuiper",
                "rules_active": rules_count,
                "protocol": "Model Context Protocol (MCP)",
                "rules_detail": all_status.unwrap_or_default(),
                "status": "online"
            });
            CallToolResult::ok(serde_json::to_string_pretty(&res).unwrap_or_default())
        }

        "ping_engine" => {
            forward_request(client, base_url, reqwest::Method::GET, "/ping", None).await
        }

        // --- 10. Universal Escape Hatch ---
        "execute_rekuiper_api" => {
            let Some(method_str) = args.get("method").and_then(|v| v.as_str()) else {
                return CallToolResult::err(
                    "Missing required parameter: 'method' (GET, POST, PUT, DELETE, PATCH)",
                );
            };
            let Some(endpoint) = args.get("endpoint").and_then(|v| v.as_str()) else {
                return CallToolResult::err("Missing required parameter: 'endpoint'");
            };

            let method = match method_str.to_uppercase().as_str() {
                "GET" => reqwest::Method::GET,
                "POST" => reqwest::Method::POST,
                "PUT" => reqwest::Method::PUT,
                "DELETE" => reqwest::Method::DELETE,
                "PATCH" => reqwest::Method::PATCH,
                other => {
                    return CallToolResult::err(format!("Unsupported HTTP method: '{}'", other))
                }
            };

            let body = args.get("body").cloned();
            forward_request(client, base_url, method, endpoint, body).await
        }

        // --- 11. WebAssembly & Dynamic Secrets Tools ---
        "register_wasm_plugin" => {
            forward_request(
                client,
                base_url,
                reqwest::Method::POST,
                "/plugins/wasm",
                Some(args),
            )
            .await
        }

        "list_wasm_plugins" => {
            forward_request(
                client,
                base_url,
                reqwest::Method::GET,
                "/plugins/wasm",
                None,
            )
            .await
        }

        "delete_wasm_plugin" => {
            let Some(name) = args.get("name").and_then(|v| v.as_str()) else {
                return CallToolResult::err("Missing required string parameter: 'name'");
            };
            let path = format!("/plugins/wasm/{}", name);
            forward_request(client, base_url, reqwest::Method::DELETE, &path, None).await
        }

        "validate_secrets" => {
            let Some(text) = args.get("text").and_then(|v| v.as_str()) else {
                return CallToolResult::err("Missing required string parameter: 'text'");
            };
            let mut secrets_found = Vec::new();
            let mut errors = Vec::new();
            let mut remaining = text;
            while let Some(start_idx) = remaining.find("{{") {
                let rest = &remaining[start_idx + 2..];
                if let Some(end_idx) = rest.find("}}") {
                    let content = rest[..end_idx].trim();
                    if content.starts_with("vault://") || content.starts_with("env://") {
                        secrets_found.push(content.to_string());
                    } else {
                        errors.push(format!(
                            "Unrecognized secret provider in template '{{{{{}}}}}': must start with 'vault://' or 'env://'",
                            content
                        ));
                    }
                    remaining = &rest[end_idx + 2..];
                } else {
                    errors.push("Unterminated secret template delimiter '{{'".to_string());
                    break;
                }
            }
            let valid = errors.is_empty();
            let res = json!({
                "valid": valid,
                "secret_references_count": secrets_found.len(),
                "secret_references": secrets_found,
                "errors": errors,
            });
            CallToolResult::ok(serde_json::to_string_pretty(&res).unwrap_or_default())
        }

        other => CallToolResult::err(format!("Tool '{}' not recognized", other)),
    }
}
