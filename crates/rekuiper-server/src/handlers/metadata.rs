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
        "mqtt",
        "neuron",
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
        "mqtt",
        "neuron",
        "nop",
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
