use crate::engine::{
    bootstrap_rule_sources, cancel_rule_source, cancel_stream_sources, cancel_table_source,
    spawn_rule_task,
};
use crate::handlers::to_stream_fields;
use crate::routes::create_router;
use crate::state::{default_import_status, AppState, BatchRequestItem, BatchResponseItem};
use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use rekuiper_core::model::RuleDefinition;
use rekuiper_core::{StreamDefinition, TableDefinition};
use rekuiper_sql::Parser;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};

#[derive(serde::Deserialize, Default)]
pub struct ImportParams {
    pub partial: Option<String>,
    pub stop: Option<String>,
}

pub async fn reset_configuration(state: &AppState) {
    let rules = state.rule_manager.list_rules();
    for rule in rules {
        let _ = state.rule_manager.delete_rule(&rule.id).await;
        cancel_rule_source(state, &rule.id);
        state.trace_manager.stop_trace(&rule.id);
    }
    let streams = state.stream_manager.list_streams();
    for stream in streams {
        let _ = state.stream_manager.delete_stream(&stream).await;
        cancel_stream_sources(state, &stream);
    }
    let tables = state.table_manager.list_tables();
    for table in tables {
        let _ = state.table_manager.delete_table(&table).await;
        cancel_table_source(state, &table);
    }
}

#[allow(clippy::result_large_err)]
fn extract_payload_content(payload: &Value) -> Result<Value, Response> {
    if let Some(content) = payload.get("content") {
        if let Some(s) = content.as_str() {
            let res = if s.trim_start().starts_with('{') || s.trim_start().starts_with('[') {
                serde_json::from_str::<Value>(s).map_err(|e| e.to_string())
            } else {
                serde_yaml::from_str::<Value>(s).map_err(|e| e.to_string())
            };
            match res {
                Ok(v) if v.is_object() => Ok(v),
                Ok(_) => Err((
                    StatusCode::BAD_REQUEST,
                    Json(json!({
                        "message": "configuration unmarshal with error: expected object"
                    })),
                )
                    .into_response()),
                Err(e) => Err((
                    StatusCode::BAD_REQUEST,
                    Json(json!({
                        "message": format!("configuration unmarshal with error: {}", e)
                    })),
                )
                    .into_response()),
            }
        } else if content.is_object() {
            Ok(content.clone())
        } else {
            Err((
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "message": "configuration unmarshal with error: invalid content"
                })),
            )
                .into_response())
        }
    } else if let Some(file_val) = payload.get("file").and_then(|f| f.as_str()) {
        let file_path = file_val.strip_prefix("file://").unwrap_or(file_val);
        match std::fs::read_to_string(file_path) {
            Ok(s) => {
                let res = if s.trim_start().starts_with('{') || s.trim_start().starts_with('[') {
                    serde_json::from_str::<Value>(&s).map_err(|e| e.to_string())
                } else {
                    serde_yaml::from_str::<Value>(&s).map_err(|e| e.to_string())
                };
                match res {
                    Ok(v) if v.is_object() => Ok(v),
                    Ok(_) => Err((
                        StatusCode::BAD_REQUEST,
                        Json(json!({
                            "message": "configuration unmarshal with error: expected object in file"
                        })),
                    )
                        .into_response()),
                    Err(e) => Err((
                        StatusCode::BAD_REQUEST,
                        Json(json!({
                            "message": format!("configuration unmarshal with error: {}", e)
                        })),
                    )
                        .into_response()),
                }
            }
            Err(_) => Err((
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "message": "Fail to read file"
                })),
            )
                .into_response()),
        }
    } else if payload.get("streams").is_some()
        || payload.get("tables").is_some()
        || payload.get("rules").is_some()
    {
        Ok(payload.clone())
    } else {
        Err((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "message": "configuration unmarshal with error: missing content or file"
            })),
        )
            .into_response())
    }
}

pub async fn export_ruleset(State(state): State<AppState>) -> Response {
    let mut streams_map = serde_json::Map::new();
    for name in state.stream_manager.list_streams() {
        if let Some(def) = state.stream_manager.get_stream(&name) {
            streams_map.insert(name, Value::String(def.sql));
        }
    }
    let mut tables_map = serde_json::Map::new();
    for name in state.table_manager.list_tables() {
        if let Some(def) = state.table_manager.get_table(&name) {
            tables_map.insert(name, Value::String(def.sql));
        }
    }
    let mut rules_map = serde_json::Map::new();
    for rule in state.rule_manager.list_rules() {
        let mut r_val = serde_json::to_value(&rule).unwrap_or(Value::Null);
        crate::secrets::mask_secrets(&mut r_val);
        let rule_json = serde_json::to_string(&r_val).unwrap_or_default();
        rules_map.insert(rule.id, Value::String(rule_json));
    }
    (
        [
            (axum::http::header::CONTENT_TYPE, "application/json"),
            (
                axum::http::header::CONTENT_DISPOSITION,
                "attachment; filename=\"ekuiper_export.json\"",
            ),
        ],
        Json(json!({
            "streams": Value::Object(streams_map),
            "tables": Value::Object(tables_map),
            "rules": Value::Object(rules_map),
        })),
    )
        .into_response()
}

pub async fn export_data(State(state): State<AppState>) -> Response {
    let mut streams_map = serde_json::Map::new();
    for name in state.stream_manager.list_streams() {
        if let Some(def) = state.stream_manager.get_stream(&name) {
            streams_map.insert(name, Value::String(def.sql));
        }
    }
    let mut tables_map = serde_json::Map::new();
    for name in state.table_manager.list_tables() {
        if let Some(def) = state.table_manager.get_table(&name) {
            tables_map.insert(name, Value::String(def.sql));
        }
    }
    let mut rules_map = serde_json::Map::new();
    for rule in state.rule_manager.list_rules() {
        let mut r_val = serde_json::to_value(&rule).unwrap_or(Value::Null);
        crate::secrets::mask_secrets(&mut r_val);
        let rule_json = serde_json::to_string(&r_val).unwrap_or_default();
        rules_map.insert(rule.id, Value::String(rule_json));
    }
    (
        [
            (axum::http::header::CONTENT_TYPE, "application/json"),
            (
                axum::http::header::CONTENT_DISPOSITION,
                "attachment; filename=\"ekuiper_export.json\"",
            ),
        ],
        Json(json!({
            "streams": Value::Object(streams_map),
            "tables": Value::Object(tables_map),
            "rules": Value::Object(rules_map),
            "nativePlugins": {},
            "portablePlugins": {},
            "sourceConfig": {},
            "sinkConfig": {},
            "connectionConfig": {},
            "Service": {},
            "Schema": {},
            "uploads": {},
            "scripts": {}
        })),
    )
        .into_response()
}

pub async fn export_data_v2(State(state): State<AppState>) -> Response {
    let mut streams_map = serde_json::Map::new();
    for name in state.stream_manager.list_streams() {
        if let Some(def) = state.stream_manager.get_stream(&name) {
            streams_map.insert(name, json!({ "sql": def.sql }));
        }
    }
    let mut tables_map = serde_json::Map::new();
    for name in state.table_manager.list_tables() {
        if let Some(def) = state.table_manager.get_table(&name) {
            tables_map.insert(name, json!({ "sql": def.sql }));
        }
    }
    let mut rules_map = serde_json::Map::new();
    for rule in state.rule_manager.list_rules() {
        let mut rule_val = serde_json::to_value(&rule).unwrap_or(Value::Null);
        crate::secrets::mask_secrets(&mut rule_val);
        rules_map.insert(rule.id, rule_val);
    }
    let doc = json!({
        "streams": Value::Object(streams_map),
        "tables": Value::Object(tables_map),
        "rules": Value::Object(rules_map),
        "nativePlugins": {},
        "portablePlugins": {},
        "sourceConfig": {},
        "sinkConfig": {},
        "connectionConfig": {},
        "Service": {},
        "Schema": {},
        "uploads": {},
        "scripts": {},
    });
    let yaml_str = serde_yaml::to_string(&doc).unwrap_or_default();
    (
        [
            (axum::http::header::CONTENT_TYPE, "application/octet-stream"),
            (
                axum::http::header::CONTENT_DISPOSITION,
                "attachment; filename=\"ekuiper_export.yaml\"",
            ),
        ],
        yaml_str,
    )
        .into_response()
}

pub async fn export_data_selected(State(state): State<AppState>, body: Bytes) -> Response {
    let payload: Value = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body).unwrap_or(Value::Null)
    };

    let is_rule_id_array = payload.is_array();
    let wanted: Option<HashSet<String>> = if let Some(arr) = payload.as_array() {
        Some(
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect(),
        )
    } else {
        payload.get("rules").and_then(|v| v.as_array()).map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
    };

    let mut rules = state.rule_manager.list_rules();
    if let Some(ids) = &wanted {
        if !ids.is_empty() {
            rules.retain(|r| ids.contains(&r.id));
        }
    }
    let mut dep_names = HashSet::new();
    for rule in &rules {
        let mut parser = Parser::new(&rule.sql);
        if let Ok(stmt) = parser.parse_select() {
            dep_names.insert(stmt.from.clone());
            for join in &stmt.joins {
                dep_names.insert(join.target.clone());
            }
        }
    }
    let mut dep_names: Vec<String> = dep_names.into_iter().collect();
    dep_names.sort();

    // If caller passed {"rules": [...]} or {} (non-array object format used in fvt_compat),
    // return array structure to preserve compatibility.
    if !is_rule_id_array && payload.is_object() {
        let mut streams = Vec::new();
        let mut tables = Vec::new();
        for name in dep_names {
            if let Some(def) = state.stream_manager.get_stream(&name) {
                streams.push(def);
            } else if let Some(def) = state.table_manager.get_table(&name) {
                tables.push(def);
            }
        }
        return Json(json!({
            "streams": streams,
            "tables": tables,
            "rules": rules,
        }))
        .into_response();
    }

    let mut streams_map = serde_json::Map::new();
    let mut tables_map = serde_json::Map::new();
    for name in dep_names {
        if let Some(def) = state.stream_manager.get_stream(&name) {
            streams_map.insert(name, Value::String(def.sql));
        } else if let Some(def) = state.table_manager.get_table(&name) {
            tables_map.insert(name, Value::String(def.sql));
        }
    }
    let mut rules_map = serde_json::Map::new();
    for rule in rules {
        let rule_json = serde_json::to_string(&rule).unwrap_or_default();
        rules_map.insert(rule.id, Value::String(rule_json));
    }

    (
        [
            (axum::http::header::CONTENT_TYPE, "application/json"),
            (
                axum::http::header::CONTENT_DISPOSITION,
                "attachment; filename=\"ekuiper_export.json\"",
            ),
        ],
        Json(json!({
            "streams": Value::Object(streams_map),
            "tables": Value::Object(tables_map),
            "rules": Value::Object(rules_map),
            "nativePlugins": {},
            "portablePlugins": {},
            "sourceConfig": {},
            "sinkConfig": {},
            "connectionConfig": {},
            "Service": {},
            "Schema": {},
            "uploads": {},
            "scripts": {}
        })),
    )
        .into_response()
}

/// Counts of entities a data import actually created.
#[derive(Default)]
pub struct ImportCounts {
    pub streams: usize,
    pub tables: usize,
    pub rules: usize,
}

/// Core data import logic shared by synchronous and asynchronous endpoints.
/// Returns how many streams, tables and rules were actually created, plus the status error map.
pub async fn process_import_payload(state: &AppState, payload: &Value) -> (ImportCounts, Value) {
    let mut status = default_import_status();
    let mut counts = ImportCounts::default();

    if let Some(streams) = payload.get("streams") {
        if let Some(defs) = streams.as_array() {
            for item in defs {
                match serde_json::from_value::<StreamDefinition>(item.clone()) {
                    Ok(def) => {
                        let name = def.name.clone();
                        if let Err(e) = state.stream_manager.create_stream(def).await {
                            status["streams"][&name] = json!(e.to_string());
                        } else {
                            state.stream_bus.get_or_create(&name);
                            counts.streams += 1;
                        }
                    }
                    Err(e) => {
                        status["streams"]["unknown"] =
                            json!(format!("invalid stream definition: {}", e));
                    }
                }
            }
        } else if let Some(map) = streams.as_object() {
            for (name, val) in map {
                let sql_str = val
                    .as_str()
                    .or_else(|| val.get("sql").and_then(|v| v.as_str()))
                    .unwrap_or("");
                let mut parser = Parser::new(sql_str);
                if let Ok(stmt) = parser.parse_create_stream() {
                    let stream_name = stmt.name.clone();
                    if let Err(e) = state
                        .stream_manager
                        .create_stream(StreamDefinition {
                            name: stream_name.clone(),
                            sql: sql_str.to_string(),
                            stream_fields: to_stream_fields(stmt.fields),
                            options: stmt.options,
                        })
                        .await
                    {
                        status["streams"][&stream_name] = json!(e.to_string());
                    } else {
                        state.stream_bus.get_or_create(&stream_name);
                        counts.streams += 1;
                    }
                } else if !name.is_empty() {
                    if let Err(e) = state
                        .stream_manager
                        .create_stream(StreamDefinition {
                            name: name.clone(),
                            sql: sql_str.to_string(),
                            stream_fields: Vec::new(),
                            options: HashMap::new(),
                        })
                        .await
                    {
                        status["streams"][name] = json!(e.to_string());
                    } else {
                        state.stream_bus.get_or_create(name);
                        counts.streams += 1;
                    }
                }
            }
        }
    }

    if let Some(tables) = payload.get("tables") {
        if let Some(defs) = tables.as_array() {
            for item in defs {
                match serde_json::from_value::<TableDefinition>(item.clone()) {
                    Ok(def) => {
                        let name = def.name.clone();
                        if let Err(e) = state.table_manager.create_table(def).await {
                            status["tables"][&name] = json!(e.to_string());
                        } else {
                            counts.tables += 1;
                        }
                    }
                    Err(e) => {
                        status["tables"]["unknown"] =
                            json!(format!("invalid table definition: {}", e));
                    }
                }
            }
        } else if let Some(map) = tables.as_object() {
            for (name, val) in map {
                let sql_str = val
                    .as_str()
                    .or_else(|| val.get("sql").and_then(|v| v.as_str()))
                    .unwrap_or("");
                let mut parser = Parser::new(sql_str);
                if let Ok(stmt) = parser.parse_create_table() {
                    let table_name = stmt.name.clone();
                    if let Err(e) = state
                        .table_manager
                        .create_table(TableDefinition {
                            name: table_name.clone(),
                            sql: sql_str.to_string(),
                            stream_fields: to_stream_fields(stmt.fields),
                            options: stmt.options,
                        })
                        .await
                    {
                        status["tables"][&table_name] = json!(e.to_string());
                    } else {
                        counts.tables += 1;
                    }
                } else if !name.is_empty() {
                    if let Err(e) = state
                        .table_manager
                        .create_table(TableDefinition {
                            name: name.clone(),
                            sql: sql_str.to_string(),
                            stream_fields: Vec::new(),
                            options: HashMap::new(),
                        })
                        .await
                    {
                        status["tables"][name] = json!(e.to_string());
                    } else {
                        counts.tables += 1;
                    }
                }
            }
        }
    }

    if let Some(rules) = payload.get("rules") {
        let defs: Vec<Value> = if let Some(arr) = rules.as_array() {
            arr.clone()
        } else if let Some(map) = rules.as_object() {
            map.iter()
                .filter_map(|(k, v)| {
                    let mut obj = if let Some(s) = v.as_str() {
                        serde_json::from_str::<Value>(s).ok()?
                    } else {
                        v.clone()
                    };
                    if let Some(m) = obj.as_object_mut() {
                        if !m.contains_key("id") {
                            m.insert("id".to_string(), Value::String(k.clone()));
                        }
                    }
                    Some(obj)
                })
                .collect()
        } else {
            Vec::new()
        };
        for item in defs {
            let Ok(def) = serde_json::from_value::<RuleDefinition>(item.clone()) else {
                status["rules"]["unknown"] = json!("invalid rule definition");
                continue;
            };
            let mut parser = Parser::new(&def.sql);
            let Ok(select_stmt) = parser.parse_select() else {
                status["rules"][&def.id] = json!("failed to parse SQL");
                continue;
            };
            if let Err(e) = state.rule_manager.create_rule(def.clone()).await {
                status["rules"][&def.id] = json!(e.to_string());
                continue;
            }
            counts.rules += 1;
            spawn_rule_task(
                &state.rule_manager,
                &state.stream_bus,
                &state.stream_manager,
                &state.table_manager,
                &state.source_configs,
                &state.http_client,
                &state.trace_manager,
                &state.config,
                def.id.clone(),
                select_stmt.clone(),
                def.actions.clone(),
                def.options.clone(),
            );
            bootstrap_rule_sources(state, &def.id, &select_stmt);
        }
    }

    *state.latest_import_status.write() = status.clone();
    (counts, status)
}

/// Baseline `POST /data/import`: runs the import and answers the structured
/// configuration envelope. By default resets existing configuration unless `partial=1`.
pub async fn import_data(
    State(state): State<AppState>,
    Query(params): Query<ImportParams>,
    body: Bytes,
) -> Response {
    if body.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "message": "configuration unmarshal with error: empty payload" })),
        )
            .into_response();
    }
    let payload: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "message": format!("configuration unmarshal with error: {}", e) })),
            )
                .into_response();
        }
    };
    let actual_payload = match extract_payload_content(&payload) {
        Ok(p) => p,
        Err(resp) => return resp,
    };
    let is_partial = match params.partial.as_deref() {
        Some("1") | Some("true") => true,
        Some("0") | Some("false") => false,
        None => payload.get("content").is_none() && payload.get("file").is_none(),
        _ => false,
    };
    if !is_partial {
        reset_configuration(&state).await;
    }
    let (_counts, status) = process_import_payload(&state, &actual_payload).await;
    let mut errors = Vec::new();
    if let Some(obj) = status.as_object() {
        for (category, items) in obj {
            if let Some(items_map) = items.as_object() {
                for (name, err) in items_map {
                    errors.push(format!("{}.{}: {}", category, name, err));
                }
            }
        }
    }
    let error_msg = if errors.is_empty() {
        String::new()
    } else {
        errors.join("; ")
    };
    (
        StatusCode::OK,
        Json(json!({
            "ErrorMsg": error_msg,
            "ConfigResponse": status,
        })),
    )
        .into_response()
}

pub async fn import_v2_data(
    State(state): State<AppState>,
    Query(params): Query<ImportParams>,
    body: Bytes,
) -> Response {
    if body.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "message": "configuration unmarshal with error: empty payload" })),
        )
            .into_response();
    }
    let payload: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "message": format!("configuration unmarshal with error: {}", e) })),
            )
                .into_response();
        }
    };
    let actual_payload = match extract_payload_content(&payload) {
        Ok(p) => p,
        Err(resp) => return resp,
    };
    let is_partial = match params.partial.as_deref() {
        Some("1") | Some("true") => true,
        Some("0") | Some("false") => false,
        None => payload.get("content").is_none() && payload.get("file").is_none(),
        _ => false,
    };
    if !is_partial {
        reset_configuration(&state).await;
    }
    process_import_payload(&state, &actual_payload).await;
    (StatusCode::OK, "success\n").into_response()
}

/// Unified ruleset import: creates streams, tables and rules from an export
/// payload. Existing entities are left untouched.
pub async fn import_ruleset(State(state): State<AppState>, body: Bytes) -> Response {
    if body.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "message": "configuration unmarshal with error: empty payload" })),
        )
            .into_response();
    }
    let payload: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "message": format!("configuration unmarshal with error: {}", e) })),
            )
                .into_response();
        }
    };
    let actual_payload = match extract_payload_content(&payload) {
        Ok(p) => p,
        Err(resp) => return resp,
    };
    let (counts, _status) = process_import_payload(&state, &actual_payload).await;
    (
        StatusCode::OK,
        format!(
            "imported {} streams, {} tables and {} rules\n",
            counts.streams, counts.tables, counts.rules
        ),
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// eKuiper Manager OpenAPI: metadata discovery, connections, plugins,
// services, schemas and system utilities.
// ---------------------------------------------------------------------------

#[allow(dead_code)]
fn named_entries(names: &[&str]) -> Value {
    Value::Array(names.iter().map(|n| json!({ "name": n })).collect())
}

/// Rejects resource names carrying characters that break routing or the
/// manager UI (mirrors eKuiper's validation FVT expectations).
#[allow(clippy::result_large_err)]
fn check_valid_name(name: &str) -> Result<(), Response> {
    if name.contains(' ')
        || name.contains("%20")
        || name.contains(';')
        || name.contains('/')
        || name.contains('\\')
    {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("name '{}' contains invalid characters", name),
        )
            .into_response());
    }
    Ok(())
}

/// Executes a sequential batch of REST API requests within the engine.
pub async fn handle_batch_req(State(state): State<AppState>, body: Bytes) -> impl IntoResponse {
    let items: Vec<BatchRequestItem> = if body.is_empty() {
        Vec::new()
    } else {
        match serde_json::from_slice(&body) {
            Ok(v) => v,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({ "error": format!("invalid batch request JSON: {}", e) })),
                )
                    .into_response();
            }
        }
    };

    use tower::ServiceExt;

    let mut results = Vec::with_capacity(items.len());
    for item in items {
        let raw_path = if let Some(idx) = item.path.find("://") {
            if let Some(path_start) = item.path[idx + 3..].find('/') {
                &item.path[idx + 3 + path_start..]
            } else {
                "/"
            }
        } else {
            &item.path
        };
        let path = if raw_path.starts_with('/') {
            raw_path.to_string()
        } else {
            format!("/{}", raw_path)
        };

        if path == "/batch/req" {
            results.push(BatchResponseItem {
                code: 400,
                response: None,
                error: Some("nested batch requests are not supported".to_string()),
            });
            continue;
        }

        let method = match item.method.to_ascii_uppercase().as_str() {
            "GET" => axum::http::Method::GET,
            "POST" => axum::http::Method::POST,
            "PUT" => axum::http::Method::PUT,
            "DELETE" => axum::http::Method::DELETE,
            "PATCH" => axum::http::Method::PATCH,
            "HEAD" => axum::http::Method::HEAD,
            "OPTIONS" => axum::http::Method::OPTIONS,
            _ => {
                results.push(BatchResponseItem {
                    code: 400,
                    response: None,
                    error: Some(format!("unsupported HTTP method: {}", item.method)),
                });
                continue;
            }
        };

        let body_bytes = match item.body {
            None => Vec::new(),
            Some(Value::String(s)) => s.into_bytes(),
            Some(v) => serde_json::to_vec(&v).unwrap_or_default(),
        };

        let router = create_router(state.clone());
        let req_res = axum::http::Request::builder()
            .method(method)
            .uri(&path)
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(axum::body::Body::from(body_bytes));

        let req = match req_res {
            Ok(r) => r,
            Err(e) => {
                results.push(BatchResponseItem {
                    code: 400,
                    response: None,
                    error: Some(format!("invalid request: {}", e)),
                });
                continue;
            }
        };

        match router.oneshot(req).await {
            Ok(resp) => {
                let status = resp.status();
                let body = resp.into_body();
                let bytes_res = axum::body::to_bytes(body, 10 * 1024 * 1024).await;
                let body_str = match bytes_res {
                    Ok(b) => String::from_utf8_lossy(&b).to_string(),
                    Err(e) => format!("error reading response body: {}", e),
                };

                if status.is_success() {
                    results.push(BatchResponseItem {
                        code: status.as_u16(),
                        response: Some(body_str),
                        error: None,
                    });
                } else {
                    results.push(BatchResponseItem {
                        code: status.as_u16(),
                        response: None,
                        error: Some(body_str),
                    });
                }
            }
            Err(e) => {
                results.push(BatchResponseItem {
                    code: 500,
                    response: None,
                    error: Some(format!("internal server error: {}", e)),
                });
            }
        }
    }

    (StatusCode::OK, Json(results)).into_response()
}

// ---------------------------------------------------------------------------

pub async fn async_data_import(
    State(state): State<AppState>,
    Query(params): Query<ImportParams>,
    body: Bytes,
) -> Response {
    if body.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "message": "configuration unmarshal with error: empty payload" })),
        )
            .into_response();
    }
    let payload: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "message": format!("configuration unmarshal with error: {}", e) })),
            )
                .into_response();
        }
    };
    let actual_payload = match extract_payload_content(&payload) {
        Ok(p) => p,
        Err(resp) => return resp,
    };
    let is_partial = match params.partial.as_deref() {
        Some("1") | Some("true") => true,
        Some("0") | Some("false") => false,
        None => payload.get("content").is_none() && payload.get("file").is_none(),
        _ => false,
    };

    let task_id = format!("dataImport-{}", uuid::Uuid::new_v4().simple());
    let cancel_rx = state.task_manager.register_task(task_id.clone());

    let task_state = state.clone();
    let tid = task_id.clone();
    tokio::spawn(async move {
        // Yield briefly to simulate realistic background ingestion
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        if *cancel_rx.borrow() {
            task_state
                .task_manager
                .update_status(&tid, "cancelled", "task cancelled");
            return;
        }
        if !is_partial {
            reset_configuration(&task_state).await;
        }
        process_import_payload(&task_state, &actual_payload).await;
        if *cancel_rx.borrow() {
            task_state
                .task_manager
                .update_status(&tid, "cancelled", "task cancelled");
        } else {
            task_state
                .task_manager
                .update_status(&tid, "completed", "import completed");
        }
    });

    (
        StatusCode::OK,
        Json(json!({
            "id": task_id,
            "task_id": task_id,
            "status": "running"
        })),
    )
        .into_response()
}

pub async fn async_task_status(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&id) {
        return resp;
    }
    if let Some(task) = state.task_manager.get_task(&id) {
        (
            StatusCode::OK,
            Json(json!({
                "id": task.id,
                "task_id": task.id,
                "status": task.status,
                "message": task.message,
                "createdTimestamp": task.created_timestamp,
                "updatedTimestamp": task.updated_timestamp
            })),
        )
            .into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(json!({
                "message": format!("Task {} not found", id)
            })),
        )
            .into_response()
    }
}

pub async fn async_task_cancelled(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Response {
    if let Err(resp) = check_valid_name(&id) {
        return resp;
    }
    state.task_manager.cancel_task(&id);
    (
        StatusCode::OK,
        Json(json!({
            "id": id,
            "task_id": id,
            "status": "cancelled",
            "message": "task cancelled"
        })),
    )
        .into_response()
}

pub async fn import_status(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.latest_import_status.read().clone())
}
