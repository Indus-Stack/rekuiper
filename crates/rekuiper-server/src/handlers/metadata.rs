use crate::state::{check_valid_name, named_entries, AppState};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use rekuiper_sql::builtin_function_metadata;
use serde_json::{json, Value};
use std::collections::HashSet;

pub fn find_etc_file(relative: &str) -> Option<std::path::PathBuf> {
    let mut candidates = vec![
        std::path::PathBuf::from("etc").join(relative),
        std::path::PathBuf::from("../etc").join(relative),
        std::path::PathBuf::from("../../etc").join(relative),
    ];
    if let Ok(manifest_dir) = std::env::var("CARGO_MANIFEST_DIR") {
        candidates.push(
            std::path::Path::new(&manifest_dir)
                .join("etc")
                .join(relative),
        );
        candidates.push(
            std::path::Path::new(&manifest_dir)
                .join("..")
                .join("etc")
                .join(relative),
        );
        candidates.push(
            std::path::Path::new(&manifest_dir)
                .join("..")
                .join("..")
                .join("etc")
                .join(relative),
        );
    }
    candidates.into_iter().find(|p| p.is_file())
}

static ETC_FILE_CACHE: parking_lot::RwLock<
    Option<std::collections::HashMap<String, Option<String>>>,
> = parking_lot::RwLock::new(None);

/// Read an etc file with in-memory caching to avoid filesystem access on hot paths.
pub fn read_etc_cached(relative: &str) -> Option<String> {
    if let Some(map) = ETC_FILE_CACHE.read().as_ref() {
        if let Some(cached) = map.get(relative) {
            return cached.clone();
        }
    }
    let content = find_etc_file(relative).and_then(|p| std::fs::read_to_string(p).ok());
    let mut guard = ETC_FILE_CACHE.write();
    let map = guard.get_or_insert_with(std::collections::HashMap::new);
    map.insert(relative.to_string(), content.clone());
    content
}

pub async fn list_source_metadata() -> impl IntoResponse {
    Json(named_entries(&[
        "edgex",
        "file",
        "http",
        "httppull",
        "httppush",
        "kafka",
        "memory",
        "modbus",
        "mqtt",
        "neuron",
        "opcua",
        "otlp",
        "rabbitmq",
        "random",
        "redis",
        "redisSub",
        "simulator",
        "sql",
        "video",
        "websocket",
        "zmq",
    ]))
}

pub async fn list_sink_metadata() -> impl IntoResponse {
    Json(named_entries(&[
        "edgex",
        "file",
        "http",
        "image",
        "influx",
        "influx2",
        "iotdb",
        "kafka",
        "log",
        "memory",
        "modbus",
        "mqtt",
        "neuron",
        "nop",
        "opcua",
        "otlp",
        "rabbitmq",
        "redis",
        "redisPub",
        "rest",
        "sql",
        "tdengine",
        "tdengine3",
        "websocket",
        "zmq",
    ]))
}

pub async fn list_function_metadata(State(state): State<AppState>) -> impl IntoResponse {
    let mut list: Vec<Value> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for meta in builtin_function_metadata() {
        seen.insert(meta.name.to_string());
        list.push(json!({
            "name": meta.name,
            "category": meta.category,
            "description": meta.description,
            "aggregate": meta.aggregate,
            "arity": meta.arity,
            "example": meta.example,
        }));
    }
    // Registered plugin/UDF functions extend the catalog; built-ins win on
    // name collisions. The registry stores no arity, so it is reported
    // honestly as unknown.
    let plugins = state
        .plugin_manager
        .list_plugins("function")
        .into_iter()
        .chain(state.plugin_manager.list_plugins("udf"));
    for plugin in plugins {
        let category = if plugin.plugin_type == "udf" {
            "udf"
        } else {
            "plugin"
        };
        let functions = if plugin.functions.is_empty() {
            vec![plugin.name.clone()]
        } else {
            plugin.functions.clone()
        };
        let description = plugin
            .description
            .clone()
            .unwrap_or_else(|| format!("Function provided by the {} plugin.", plugin.name));
        for func in functions {
            if seen.insert(func.clone()) {
                list.push(json!({
                    "name": func,
                    "category": category,
                    "description": description,
                    "aggregate": false,
                    "arity": "unknown",
                    "example": format!("{}()", func),
                }));
            }
        }
    }

    let js_udfs = state.js_udfs.read();
    for (id, udf) in js_udfs.iter() {
        if seen.insert(id.clone()) {
            list.push(json!({
                "name": id,
                "category": "udf",
                "description": if udf.description.is_empty() { format!("JavaScript UDF {}", id) } else { udf.description.clone() },
                "aggregate": udf.is_agg,
                "arity": "unknown",
                "example": format!("{}()", id),
            }));
        }
    }

    let services = state.services.read();
    for svc in services.values() {
        for f in &svc.functions {
            if seen.insert(f.func_name.clone()) {
                list.push(json!({
                    "name": f.func_name,
                    "category": "service",
                    "description": format!("External service function provided by {}", f.service_name),
                    "aggregate": false,
                    "arity": "unknown",
                    "example": format!("{}()", f.func_name),
                }));
            }
        }
    }

    Json(list)
}

pub async fn list_operator_metadata() -> impl IntoResponse {
    Json(named_entries(&[
        "+", "-", "*", "/", "=", "!=", "<", ">", "AND", "OR", "NOT", "BETWEEN", "IN",
    ]))
}

/// Dynamic Palette Schema for Visual Stream Studio
pub async fn get_palette_metadata() -> impl IntoResponse {
    let palette = json!({
        "categories": ["sources", "operators", "sinks"],
        "nodes": [
            // Sources
            {
                "category": "source",
                "type": "mqtt",
                "label": "MQTT Source",
                "icon": "radio",
                "color": "#0284c7",
                "description": "Subscribes to an MQTT broker topic and streams incoming payloads.",
                "inputs": 0,
                "outputs": 1,
                "multi_port": false,
                "properties": [
                    { "name": "datasource", "type": "string", "label": "Stream / Topic", "default": "demo", "required": true },
                    { "name": "server", "type": "string", "label": "Broker URL", "default": "tcp://127.0.0.1:1883" },
                    { "name": "qos", "type": "number", "label": "QoS Level (0-2)", "default": 1 }
                ]
            },
            {
                "category": "source",
                "type": "modbus",
                "label": "Modbus PLC",
                "icon": "cpu",
                "color": "#0284c7",
                "description": "Polls industrial Modbus TCP / RTU coils and holding registers.",
                "inputs": 0,
                "outputs": 1,
                "multi_port": false,
                "properties": [
                    { "name": "datasource", "type": "string", "label": "Stream Name", "default": "modbus_plc", "required": true },
                    { "name": "endpoint", "type": "string", "label": "TCP Endpoint", "default": "127.0.0.1:502" },
                    { "name": "intervalMs", "type": "number", "label": "Poll Interval (ms)", "default": 1000 },
                    { "name": "unitId", "type": "number", "label": "Unit / Slave ID", "default": 1 }
                ]
            },
            {
                "category": "source",
                "type": "opcua",
                "label": "OPC-UA Source",
                "icon": "server",
                "color": "#0284c7",
                "description": "Polls or subscribes to OPC-UA node IDs via opc.tcp.",
                "inputs": 0,
                "outputs": 1,
                "multi_port": false,
                "properties": [
                    { "name": "datasource", "type": "string", "label": "Stream Name", "default": "opcua_stream", "required": true },
                    { "name": "endpoint", "type": "string", "label": "Server URL", "default": "opc.tcp://127.0.0.1:4840" },
                    { "name": "intervalMs", "type": "number", "label": "Sampling Interval (ms)", "default": 1000 }
                ]
            },
            {
                "category": "source",
                "type": "httppull",
                "label": "HTTP Pull",
                "icon": "globe",
                "color": "#0284c7",
                "description": "Periodically polls a REST JSON endpoint.",
                "inputs": 0,
                "outputs": 1,
                "multi_port": false,
                "properties": [
                    { "name": "datasource", "type": "string", "label": "Stream Name", "default": "http_api", "required": true },
                    { "name": "url", "type": "string", "label": "Target URL", "default": "http://127.0.0.1:8080/data" },
                    { "name": "interval", "type": "number", "label": "Poll Interval (ms)", "default": 2000 }
                ]
            },
            {
                "category": "source",
                "type": "kafka",
                "label": "Kafka Source",
                "icon": "layers",
                "color": "#0284c7",
                "description": "Consumes messages from Apache Kafka topic partitions.",
                "inputs": 0,
                "outputs": 1,
                "multi_port": false,
                "properties": [
                    { "name": "datasource", "type": "string", "label": "Stream / Topic", "default": "kafka_stream", "required": true },
                    { "name": "brokers", "type": "string", "label": "Brokers", "default": "127.0.0.1:9092" },
                    { "name": "group_id", "type": "string", "label": "Consumer Group", "default": "rekuiper_group" }
                ]
            },
            {
                "category": "source",
                "type": "simulator",
                "label": "Simulator / Mock",
                "icon": "play-circle",
                "color": "#0284c7",
                "description": "Generates synthetic or replayed test telemetry events.",
                "inputs": 0,
                "outputs": 1,
                "multi_port": false,
                "properties": [
                    { "name": "datasource", "type": "string", "label": "Stream Name", "default": "sim_stream", "required": true },
                    { "name": "interval", "type": "number", "label": "Event Spacing (ms)", "default": 100 }
                ]
            },
            // Operators
            {
                "category": "operator",
                "type": "switch",
                "label": "Switch Router",
                "icon": "git-branch",
                "color": "#ea580c",
                "description": "Multi-port selective predicate routing without broadcast fan-out.",
                "inputs": 1,
                "outputs": 2,
                "multi_port": true,
                "properties": [
                    {
                        "name": "cases",
                        "type": "json",
                        "label": "Routing Cases [{\"case\": \"expr\", \"port\": 0}]",
                        "default": [
                            { "case": "temperature > 50", "port": 0 },
                            { "case": "temperature <= 50", "port": 1 }
                        ]
                    },
                    { "name": "default_port", "type": "number", "label": "Default Port", "default": 1 }
                ]
            },
            {
                "category": "operator",
                "type": "rate_limit",
                "label": "Rate Limiter",
                "icon": "shield",
                "color": "#6366f1",
                "description": "Token-bucket stream throttle: admits allowed messages and drops or sheds excess bursts.",
                "inputs": 1,
                "outputs": 1,
                "multi_port": false,
                "properties": [
                    { "name": "rate_limit", "type": "number", "label": "Limit (msgs/sec)", "default": 10.0 },
                    { "name": "burst", "type": "number", "label": "Max Burst Tokens", "default": 10.0 }
                ]
            },
            {
                "category": "operator",
                "type": "watchdog",
                "label": "Watchdog Timeout",
                "icon": "clock",
                "color": "#e11d48",
                "description": "Inactivity timer: raises synthetic alert tuples if input stream goes silent.",
                "inputs": 1,
                "outputs": 1,
                "multi_port": false,
                "properties": [
                    { "name": "timeout_ms", "type": "number", "label": "Silence Timeout (ms)", "default": 5000 },
                    {
                        "name": "alert_payload",
                        "type": "json",
                        "label": "Alert Payload",
                        "default": { "status": "INACTIVE_TIMEOUT", "severity": "HIGH" }
                    },
                    { "name": "repeat", "type": "boolean", "label": "Repeat Alert Ticks", "default": true }
                ]
            },
            {
                "category": "operator",
                "type": "filter",
                "label": "Filter",
                "icon": "filter",
                "color": "#7c3aed",
                "description": "Filters stream records based on a boolean SQL expression.",
                "inputs": 1,
                "outputs": 1,
                "multi_port": false,
                "properties": [
                    { "name": "expr", "type": "string", "label": "Filter Expression", "default": "temperature > 25" }
                ]
            },
            {
                "category": "operator",
                "type": "pick",
                "label": "Pick Projection",
                "icon": "columns",
                "color": "#7c3aed",
                "description": "Projects a subset of record fields forward.",
                "inputs": 1,
                "outputs": 1,
                "multi_port": false,
                "properties": [
                    { "name": "fields", "type": "json", "label": "Field Names", "default": ["temperature", "humidity"] }
                ]
            },
            {
                "category": "operator",
                "type": "window",
                "label": "Window Aggregator",
                "icon": "box",
                "color": "#7c3aed",
                "description": "Tumbling, hopping, sliding or session time/count windows.",
                "inputs": 1,
                "outputs": 1,
                "multi_port": false,
                "properties": [
                    { "name": "window_type", "type": "string", "label": "Window Type (tumbling/hopping/sliding)", "default": "tumbling" },
                    { "name": "length", "type": "number", "label": "Duration (ms) or Count", "default": 10000 }
                ]
            },
            {
                "category": "operator",
                "type": "sql",
                "label": "SQL Transform",
                "icon": "database",
                "color": "#7c3aed",
                "description": "Embedded SQL projection and aggregation statement.",
                "inputs": 1,
                "outputs": 1,
                "multi_port": false,
                "properties": [
                    { "name": "sql", "type": "string", "label": "SQL Statement", "default": "SELECT * FROM src WHERE temp > 20" }
                ]
            },
            // Sinks
            {
                "category": "sink",
                "type": "mqtt",
                "label": "MQTT Sink",
                "icon": "send",
                "color": "#059669",
                "description": "Publishes processed records to an MQTT broker topic.",
                "inputs": 1,
                "outputs": 0,
                "multi_port": false,
                "properties": [
                    { "name": "server", "type": "string", "label": "Broker URL", "default": "tcp://127.0.0.1:1883" },
                    { "name": "topic", "type": "string", "label": "Target Topic", "default": "rekuiper/alerts" },
                    { "name": "qos", "type": "number", "label": "QoS Level", "default": 1 }
                ]
            },
            {
                "category": "sink",
                "type": "rest",
                "label": "REST / HTTP Sink",
                "icon": "globe",
                "color": "#059669",
                "description": "Dispatches HTTP POST requests with output JSON payloads.",
                "inputs": 1,
                "outputs": 0,
                "multi_port": false,
                "properties": [
                    { "name": "url", "type": "string", "label": "Target URL", "default": "http://127.0.0.1:8080/ingest" },
                    { "name": "method", "type": "string", "label": "HTTP Method", "default": "POST" }
                ]
            },
            {
                "category": "sink",
                "type": "log",
                "label": "Log Sink",
                "icon": "terminal",
                "color": "#059669",
                "description": "Emits output tuples to the daemon tracing console.",
                "inputs": 1,
                "outputs": 0,
                "multi_port": false,
                "properties": []
            },
            {
                "category": "sink",
                "type": "file",
                "label": "File Sink",
                "icon": "file-text",
                "color": "#059669",
                "description": "Appends records to rolling JSON / Parquet files.",
                "inputs": 1,
                "outputs": 0,
                "multi_port": false,
                "properties": [
                    { "name": "path", "type": "string", "label": "File Path", "default": "output.json" }
                ]
            },
            {
                "category": "sink",
                "type": "kafka",
                "label": "Kafka Sink",
                "icon": "layers",
                "color": "#059669",
                "description": "Publishes output records to an Apache Kafka topic.",
                "inputs": 1,
                "outputs": 0,
                "multi_port": false,
                "properties": [
                    { "name": "brokers", "type": "string", "label": "Brokers", "default": "127.0.0.1:9092" },
                    { "name": "topic", "type": "string", "label": "Kafka Topic", "default": "rekuiper_out" }
                ]
            },
            {
                "category": "sink",
                "type": "influx",
                "label": "InfluxDB v1",
                "icon": "database",
                "color": "#059669",
                "description": "Writes stream records to InfluxDB 1.x using Line Protocol over HTTP.",
                "inputs": 1,
                "outputs": 0,
                "multi_port": false,
                "properties": [
                    { "name": "addr", "type": "string", "label": "Server URL", "default": "http://127.0.0.1:8086", "required": true },
                    { "name": "measurement", "type": "string", "label": "Measurement", "default": "rekuiper_metrics" },
                    { "name": "precision", "type": "string", "label": "Precision", "default": "ms" }
                ]
            },
            {
                "category": "sink",
                "type": "influx2",
                "label": "InfluxDB v2",
                "icon": "database",
                "color": "#059669",
                "description": "Writes stream records to InfluxDB 2.x/3.x with Org and Bucket authentication.",
                "inputs": 1,
                "outputs": 0,
                "multi_port": false,
                "properties": [
                    { "name": "addr", "type": "string", "label": "Server URL", "default": "http://127.0.0.1:8086", "required": true },
                    { "name": "org", "type": "string", "label": "Organization", "default": "my-org" },
                    { "name": "bucket", "type": "string", "label": "Bucket", "default": "iot_bucket" },
                    { "name": "token", "type": "string", "label": "API Token", "default": "******" },
                    { "name": "measurement", "type": "string", "label": "Measurement", "default": "rekuiper_metrics" }
                ]
            },
            {
                "category": "sink",
                "type": "tdengine",
                "label": "TDengine 3.x",
                "icon": "database",
                "color": "#059669",
                "description": "Writes high-throughput time-series records to TDengine database.",
                "inputs": 1,
                "outputs": 0,
                "multi_port": false,
                "properties": [
                    { "name": "host", "type": "string", "label": "Host", "default": "localhost" },
                    { "name": "port", "type": "number", "label": "Port", "default": 6041 },
                    { "name": "database", "type": "string", "label": "Database", "default": "power" },
                    { "name": "table", "type": "string", "label": "Table", "default": "meters" }
                ]
            },
            {
                "category": "sink",
                "type": "iotdb",
                "label": "Apache IoTDB",
                "icon": "database",
                "color": "#059669",
                "description": "Streams device telemetry records directly to Apache IoTDB REST service.",
                "inputs": 1,
                "outputs": 0,
                "multi_port": false,
                "properties": [
                    { "name": "addr", "type": "string", "label": "Server URL", "default": "http://127.0.0.1:18080" },
                    { "name": "device", "type": "string", "label": "Device Path", "default": "root.factory.d1" }
                ]
            },
            {
                "category": "sink",
                "type": "opcua",
                "label": "OPC-UA Sink",
                "icon": "server",
                "color": "#059669",
                "description": "Writes values to OPC-UA node attributes on industrial servers.",
                "inputs": 1,
                "outputs": 0,
                "multi_port": false,
                "properties": [
                    { "name": "endpoint", "type": "string", "label": "Server URL", "default": "opc.tcp://127.0.0.1:4840" },
                    { "name": "nodeId", "type": "string", "label": "Target Node ID", "default": "ns=2;s=Device1.SetPoint" }
                ]
            },
            {
                "category": "sink",
                "type": "modbus",
                "label": "Modbus PLC Sink",
                "icon": "cpu",
                "color": "#059669",
                "description": "Writes output values to Modbus TCP coils or holding registers.",
                "inputs": 1,
                "outputs": 0,
                "multi_port": false,
                "properties": [
                    { "name": "endpoint", "type": "string", "label": "TCP Endpoint", "default": "127.0.0.1:502" },
                    { "name": "slaveId", "type": "number", "label": "Slave ID", "default": 1 },
                    { "name": "address", "type": "number", "label": "Register Address", "default": 40001 }
                ]
            },
            {
                "category": "sink",
                "type": "redis",
                "label": "Redis Sink",
                "icon": "layers",
                "color": "#059669",
                "description": "Publishes to Redis pub/sub channel or appends to Redis stream/list.",
                "inputs": 1,
                "outputs": 0,
                "multi_port": false,
                "properties": [
                    { "name": "addr", "type": "string", "label": "Redis URL", "default": "redis://127.0.0.1:6379" },
                    { "name": "topic", "type": "string", "label": "Channel / Stream Key", "default": "rekuiper_alerts" },
                    { "name": "datatype", "type": "string", "label": "Data Type (channel/stream/list)", "default": "channel" }
                ]
            },
            {
                "category": "sink",
                "type": "parquet",
                "label": "Parquet Rolling Sink",
                "icon": "file-text",
                "color": "#059669",
                "description": "Rolls high-velocity records into compressed Apache Parquet columnar files.",
                "inputs": 1,
                "outputs": 0,
                "multi_port": false,
                "properties": [
                    { "name": "path", "type": "string", "label": "Directory Path", "default": "/data/parquet/telemetry" },
                    { "name": "rollingCount", "type": "number", "label": "Rolling Record Count", "default": 100000 },
                    { "name": "rollingIntervalMs", "type": "number", "label": "Rolling Interval (ms)", "default": 60000 }
                ]
            },
            {
                "category": "sink",
                "type": "otlp",
                "label": "OpenTelemetry OTLP",
                "icon": "activity",
                "color": "#059669",
                "description": "Exports edge stream metrics, logs, and spans to OpenTelemetry collectors.",
                "inputs": 1,
                "outputs": 0,
                "multi_port": false,
                "properties": [
                    { "name": "endpoint", "type": "string", "label": "Collector Endpoint", "default": "http://127.0.0.1:4318/v1/metrics" },
                    { "name": "protocol", "type": "string", "label": "Protocol", "default": "http/json" }
                ]
            },
            {
                "category": "sink",
                "type": "websocket",
                "label": "WebSocket Sink",
                "icon": "radio",
                "color": "#059669",
                "description": "Broadcasts stream records in real-time to connected WebSocket clients.",
                "inputs": 1,
                "outputs": 0,
                "multi_port": false,
                "properties": [
                    { "name": "addr", "type": "string", "label": "WebSocket URL / Path", "default": "ws://127.0.0.1:9090/ws/alerts" }
                ]
            },
            {
                "category": "sink",
                "type": "nop",
                "label": "NOP Drop",
                "icon": "slash",
                "color": "#64748b",
                "description": "Black-hole drop sink for benchmarking and dead-ends.",
                "inputs": 1,
                "outputs": 0,
                "multi_port": false,
                "properties": []
            }
        ]
    });
    Json(palette)
}

pub async fn list_metadata_connections(State(state): State<AppState>) -> impl IntoResponse {
    let conns: Vec<Value> = state.connections.read().values().cloned().collect();
    Json(conns)
}

pub async fn list_metadata_resources(State(state): State<AppState>) -> impl IntoResponse {
    let mut resources: Vec<Value> = Vec::new();
    for (id, conn) in state.connections.read().iter() {
        resources.push(json!({
            "id": id,
            "resource": id,
            "type": conn.get("type").and_then(|v| v.as_str()).unwrap_or("connection"),
            "status": "active",
        }));
    }
    Json(resources)
}

pub async fn get_connection_metadata(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if let Some(conn) = state.connections.read().get(&name).cloned() {
        return Json(conn).into_response();
    }
    if let Some(content) = read_etc_cached(&format!("connections/{}.json", name)) {
        if let Ok(val) = serde_json::from_str::<Value>(&content) {
            return Json(val).into_response();
        }
    }
    Json(json!({
        "id": name,
        "name": name,
        "about": {
            "description": format!("Connection configuration for {}", name)
        }
    }))
    .into_response()
}

pub fn mask_secrets(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (k, v) in map.iter_mut() {
                let lower = k.to_lowercase();
                if lower.contains("password") || lower.contains("token") {
                    *v = Value::String("******".to_string());
                } else {
                    mask_secrets(v);
                }
            }
        }
        Value::Array(arr) => {
            for v in arr.iter_mut() {
                mask_secrets(v);
            }
        }
        _ => {}
    }
}

pub async fn get_connection_yaml(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let found = read_etc_cached(&format!("connections/{}.yaml", name))
        .or_else(|| read_etc_cached("connections/connection.yaml"));

    let mut result_map: serde_json::Map<String, Value> = serde_json::Map::new();
    let mut raw_content = String::new();

    if let Some(content) = found {
        raw_content = content.clone();
        if let Ok(yaml_val) = serde_yaml::from_str::<Value>(&content) {
            if let Some(obj) = yaml_val.as_object() {
                for (k, v) in obj {
                    result_map.insert(k.clone(), v.clone());
                }
            }
        }
    }

    let prefix = format!("{}/", name);
    let dot_prefix = format!("{}.", name);
    let configs = state.connections.read();
    for (k, v) in configs.iter() {
        if let Some(conf_key) = k.strip_prefix(&prefix) {
            result_map.insert(conf_key.to_string(), v.clone());
        } else if let Some(conf_key) = k.strip_prefix(&dot_prefix) {
            result_map
                .entry(conf_key.to_string())
                .or_insert_with(|| v.clone());
        }
    }

    if result_map.is_empty() {
        if name == "mqtt" {
            result_map.insert(
                "default".to_string(),
                json!({
                    "server": "tcp://127.0.0.1:1883",
                    "protocolVersion": "3.1.1"
                }),
            );
            raw_content =
                "default:\n  server: \"tcp://127.0.0.1:1883\"\n  protocolVersion: \"3.1.1\"\n"
                    .to_string();
        } else {
            return (
                StatusCode::NOT_FOUND,
                format!("connection {} not found\n", name),
            )
                .into_response();
        }
    }

    if raw_content.is_empty() {
        raw_content = serde_yaml::to_string(&result_map).unwrap_or_default();
    }
    result_map.insert("yaml".to_string(), json!(raw_content));

    let mut final_val = Value::Object(result_map);
    mask_secrets(&mut final_val);
    Json(final_val).into_response()
}

pub async fn get_source_metadata(Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let found = if name == "mqtt" {
        read_etc_cached("mqtt_source.json").or_else(|| read_etc_cached("sources/mqtt.json"))
    } else if name == "http" {
        read_etc_cached("sources/http.json").or_else(|| read_etc_cached("sources/httppull.json"))
    } else {
        read_etc_cached(&format!("sources/{}.json", name))
    };

    if let Some(content) = found {
        if let Ok(json_val) = serde_json::from_str::<Value>(&content) {
            return Json(json_val).into_response();
        }
    }
    if let Some(fallback) = fallback_source_metadata(&name) {
        return Json(fallback).into_response();
    }
    (
        StatusCode::NOT_FOUND,
        format!("source {} not found\n", name),
    )
        .into_response()
}

pub async fn get_sink_metadata(Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let found = if name == "mqtt" {
        read_etc_cached("sinks/mqtt.json")
    } else if name == "http" {
        read_etc_cached("sinks/rest.json").or_else(|| read_etc_cached("sinks/http.json"))
    } else {
        read_etc_cached(&format!("sinks/{}.json", name))
    };

    if let Some(content) = found {
        if let Ok(json_val) = serde_json::from_str::<Value>(&content) {
            return Json(json_val).into_response();
        }
    }
    if let Some(fallback) = fallback_sink_metadata(&name) {
        return Json(fallback).into_response();
    }
    (StatusCode::NOT_FOUND, format!("sink {} not found\n", name)).into_response()
}

fn fallback_source_metadata(name: &str) -> Option<Value> {
    match name {
        "otlp" => Some(json!({
            "about": { "description": { "en_US": "OpenTelemetry (OTLP) push metrics, logs, and traces source." } },
            "properties": {
                "default": [
                    { "name": "bind", "type": "string", "default": "0.0.0.0:4318", "label": { "en_US": "Listener Bind Address" } }
                ]
            }
        })),
        "video" => Some(json!({
            "about": { "description": { "en_US": "Video streaming source capturing frames via FFmpeg pipeline." } },
            "properties": {
                "default": [
                    { "name": "url", "type": "string", "label": { "en_US": "Stream URL" } },
                    { "name": "interval", "type": "string", "default": "1s", "label": { "en_US": "Frame Capture Interval" } },
                    { "name": "codec", "type": "string", "default": "mjpeg", "label": { "en_US": "Video Frame Codec" } }
                ]
            }
        })),
        "zmq" | "zeromq" => Some(json!({
            "about": { "description": { "en_US": "ZeroMQ message subscriber source." } },
            "properties": {
                "default": [
                    { "name": "server", "type": "string", "default": "tcp://127.0.0.1:5563", "label": { "en_US": "Endpoint URL" } },
                    { "name": "topic", "type": "string", "default": "", "label": { "en_US": "Topic Subscription Prefix" } },
                    { "name": "mode", "type": "string", "default": "connect", "label": { "en_US": "Socket Mode" } }
                ]
            }
        })),
        "rabbitmq" => Some(json!({
            "about": { "description": { "en_US": "RabbitMQ AMQP 0-9-1 streaming consumer source." } },
            "properties": {
                "default": [
                    { "name": "server", "type": "string", "default": "amqp://guest:guest@127.0.0.1:5672/%2f", "label": { "en_US": "Broker Server URL" } },
                    { "name": "queue", "type": "string", "label": { "en_US": "Queue Name" } },
                    { "name": "exchange", "type": "string", "label": { "en_US": "Exchange Name" } },
                    { "name": "routingKey", "type": "string", "label": { "en_US": "Routing Key" } }
                ]
            }
        })),
        "sql" => Some(json!({
            "about": { "description": { "en_US": "SQL database polling source (SQLite and PostgreSQL)." } },
            "properties": {
                "default": [
                    { "name": "url", "type": "string", "label": { "en_US": "Database URL (dburl)" } },
                    { "name": "table", "type": "string", "label": { "en_US": "Table Name" } },
                    { "name": "interval", "type": "int", "default": 1000, "label": { "en_US": "Poll Interval (ms)" } }
                ]
            }
        })),
        _ => None,
    }
}

fn fallback_sink_metadata(name: &str) -> Option<Value> {
    match name {
        "otlp" => Some(json!({
            "about": { "description": { "en_US": "OpenTelemetry (OTLP) HTTP export sink." } },
            "properties": {
                "default": [
                    { "name": "endpoint", "type": "string", "label": { "en_US": "OTLP Endpoint URL" } },
                    { "name": "signal", "type": "string", "default": "metric", "label": { "en_US": "Telemetry Signal" } }
                ]
            }
        })),
        "zmq" | "zeromq" => Some(json!({
            "about": { "description": { "en_US": "ZeroMQ publisher sink." } },
            "properties": {
                "default": [
                    { "name": "server", "type": "string", "default": "tcp://127.0.0.1:5563", "label": { "en_US": "Endpoint URL" } },
                    { "name": "topic", "type": "string", "label": { "en_US": "Topic Prefix Frame" } },
                    { "name": "mode", "type": "string", "default": "connect", "label": { "en_US": "Socket Mode" } }
                ]
            }
        })),
        "rabbitmq" => Some(json!({
            "about": { "description": { "en_US": "RabbitMQ AMQP 0-9-1 publisher sink." } },
            "properties": {
                "default": [
                    { "name": "server", "type": "string", "default": "amqp://guest:guest@127.0.0.1:5672/%2f", "label": { "en_US": "Broker Server URL" } },
                    { "name": "exchange", "type": "string", "label": { "en_US": "Exchange Name" } },
                    { "name": "routingKey", "type": "string", "label": { "en_US": "Routing Key" } },
                    { "name": "queue", "type": "string", "label": { "en_US": "Target Queue" } }
                ]
            }
        })),
        "sql" => Some(json!({
            "about": { "description": { "en_US": "SQL database sink for SQLite and PostgreSQL." } },
            "properties": {
                "default": [
                    { "name": "url", "type": "string", "label": { "en_US": "Database URL" } },
                    { "name": "table", "type": "string", "label": { "en_US": "Target Table" } },
                    { "name": "fields", "type": "array", "label": { "en_US": "Columns to Write" } }
                ]
            }
        })),
        "image" => Some(json!({
            "about": { "description": { "en_US": "Image sink for persisting image frames with retention pruning." } },
            "properties": {
                "default": [
                    { "name": "path", "type": "string", "default": "/tmp/images", "label": { "en_US": "Directory Path" } },
                    { "name": "format", "type": "string", "default": "jpeg", "label": { "en_US": "Image Format" } },
                    { "name": "maxAge", "type": "string", "default": "72h", "label": { "en_US": "Retention Max Age" } },
                    { "name": "maxCount", "type": "int", "default": 1000, "label": { "en_US": "Retention Max Count" } }
                ]
            }
        })),
        "influx" | "influx2" => Some(json!({
            "about": { "description": { "en_US": "InfluxDB Line Protocol HTTP sink." } },
            "properties": {
                "default": [
                    { "name": "addr", "type": "string", "default": "http://127.0.0.1:8086", "label": { "en_US": "InfluxDB Server Address" } },
                    { "name": "measurement", "type": "string", "default": "kuiper", "label": { "en_US": "Measurement Name" } },
                    { "name": "precision", "type": "string", "default": "ms", "label": { "en_US": "Timestamp Precision" } }
                ]
            }
        })),
        "tdengine" | "tdengine3" => Some(json!({
            "about": { "description": { "en_US": "TDengine 3.x REST sink." } },
            "properties": {
                "default": [
                    { "name": "host", "type": "string", "default": "localhost", "label": { "en_US": "TDengine Host" } },
                    { "name": "port", "type": "int", "default": 6041, "label": { "en_US": "TDengine Port" } },
                    { "name": "user", "type": "string", "default": "root", "label": { "en_US": "Username" } },
                    { "name": "password", "type": "string", "default": "taosdata", "label": { "en_US": "Password" } },
                    { "name": "database", "type": "string", "label": { "en_US": "Database Name" } },
                    { "name": "table", "type": "string", "label": { "en_US": "Table / Subtable Name" } }
                ]
            }
        })),
        "iotdb" => Some(json!({
            "about": { "description": { "en_US": "Apache IoTDB REST v1 sink." } },
            "properties": {
                "default": [
                    { "name": "addr", "type": "string", "default": "http://127.0.0.1:18080", "label": { "en_US": "IoTDB Server Address" } },
                    { "name": "username", "type": "string", "default": "root", "label": { "en_US": "Username" } },
                    { "name": "password", "type": "string", "default": "root", "label": { "en_US": "Password" } },
                    { "name": "device", "type": "string", "label": { "en_US": "Device Path" } }
                ]
            }
        })),
        _ => None,
    }
}

pub async fn get_source_yaml(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let found = if name == "mqtt" {
        read_etc_cached("mqtt_source.yaml").or_else(|| read_etc_cached("sources/mqtt.yaml"))
    } else if name == "http" {
        read_etc_cached("sources/httppull.yaml").or_else(|| read_etc_cached("sources/http.yaml"))
    } else {
        read_etc_cached(&format!("sources/{}.yaml", name))
    };

    let mut result_map: serde_json::Map<String, Value> = serde_json::Map::new();
    let mut raw_content = String::new();

    if let Some(content) = found {
        raw_content = content.clone();
        if let Ok(yaml_val) = serde_yaml::from_str::<Value>(&content) {
            if let Some(obj) = yaml_val.as_object() {
                for (k, v) in obj {
                    result_map.insert(k.clone(), v.clone());
                }
            }
        }
    }

    let prefix = format!("{}/", name);
    let configs = state.source_configs.read();
    for (k, v) in configs.iter() {
        if let Some(conf_key) = k.strip_prefix(&prefix) {
            result_map.insert(conf_key.to_string(), v.clone());
        }
    }

    if result_map.is_empty() {
        if name == "mqtt" {
            result_map.insert(
                "default".to_string(),
                json!({
                    "server": "tcp://127.0.0.1:1883",
                    "protocolVersion": "3.1.1"
                }),
            );
            raw_content =
                "default:\n  server: \"tcp://127.0.0.1:1883\"\n  protocolVersion: \"3.1.1\"\n"
                    .to_string();
        } else {
            return (
                StatusCode::NOT_FOUND,
                format!("source {} not found\n", name),
            )
                .into_response();
        }
    }

    if raw_content.is_empty() {
        raw_content = serde_yaml::to_string(&result_map).unwrap_or_default();
    }
    result_map.insert("yaml".to_string(), json!(raw_content));

    let mut final_val = Value::Object(result_map);
    mask_secrets(&mut final_val);
    Json(final_val).into_response()
}

pub async fn get_sink_yaml(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let found = if name == "mqtt" {
        read_etc_cached("sinks/mqtt.yaml").or_else(|| read_etc_cached("mqtt_sink.yaml"))
    } else if name == "http" {
        read_etc_cached("sinks/rest.yaml").or_else(|| read_etc_cached("sinks/http.yaml"))
    } else {
        read_etc_cached(&format!("sinks/{}.yaml", name))
    };

    let mut result_map: serde_json::Map<String, Value> = serde_json::Map::new();
    let mut raw_content = String::new();

    if let Some(content) = found {
        raw_content = content.clone();
        if let Ok(yaml_val) = serde_yaml::from_str::<Value>(&content) {
            if let Some(obj) = yaml_val.as_object() {
                for (k, v) in obj {
                    result_map.insert(k.clone(), v.clone());
                }
            }
        }
    }

    let prefix = format!("{}/", name);
    let configs = state.sink_configs.read();
    for (k, v) in configs.iter() {
        if let Some(conf_key) = k.strip_prefix(&prefix) {
            result_map.insert(conf_key.to_string(), v.clone());
        }
    }

    if result_map.is_empty() {
        if name == "mqtt" {
            result_map.insert(
                "default".to_string(),
                json!({
                    "server": "tcp://127.0.0.1:1883",
                    "protocolVersion": "3.1.1"
                }),
            );
            raw_content =
                "default:\n  server: \"tcp://127.0.0.1:1883\"\n  protocolVersion: \"3.1.1\"\n"
                    .to_string();
        } else {
            return (StatusCode::NOT_FOUND, format!("sink {} not found\n", name)).into_response();
        }
    }

    if raw_content.is_empty() {
        raw_content = serde_yaml::to_string(&result_map).unwrap_or_default();
    }
    result_map.insert("yaml".to_string(), json!(raw_content));

    let mut final_val = Value::Object(result_map);
    mask_secrets(&mut final_val);
    Json(final_val).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fallback_source_metadata_complete() {
        for src in &["video", "zmq", "zeromq", "rabbitmq", "sql"] {
            let meta = fallback_source_metadata(src)
                .unwrap_or_else(|| panic!("missing fallback for source {}", src));
            assert!(meta.get("about").is_some());
            assert!(meta.get("properties").is_some());
        }
    }

    #[test]
    fn test_fallback_sink_metadata_complete() {
        for sink in &[
            "zmq",
            "zeromq",
            "rabbitmq",
            "sql",
            "image",
            "influx",
            "influx2",
            "tdengine",
            "tdengine3",
            "iotdb",
        ] {
            let meta = fallback_sink_metadata(sink)
                .unwrap_or_else(|| panic!("missing fallback for sink {}", sink));
            assert!(meta.get("about").is_some());
            assert!(meta.get("properties").is_some());
        }
    }
}
