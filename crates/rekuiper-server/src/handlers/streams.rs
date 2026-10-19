use crate::engine::{bootstrap_table_source, cancel_stream_sources, cancel_table_source};
use crate::state::{check_valid_name, AppState};
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use indexmap::IndexMap;
use rekuiper_core::model::{StreamField, StreamRecord};
use rekuiper_core::{StreamDefinition, TableDefinition, MAX_HTTP_BATCH_RECORDS};
use rekuiper_sql::{Parser, StreamColumn};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;

#[derive(Deserialize)]
pub struct CreateStreamPayload {
    #[serde(default)]
    sql: Option<String>,
    #[serde(default)]
    name: Option<String>,
}

/// Map parsed `CREATE` columns onto stored stream/table fields.
pub fn to_stream_fields(cols: Vec<StreamColumn>) -> Vec<StreamField> {
    cols.into_iter()
        .map(|c| StreamField {
            name: c.name,
            field_type: c.data_type,
            default_value: c.default_value,
        })
        .collect()
}

/// eKuiper-style describe envelope shared by `GET` and `DESCRIBE` paths.
pub fn describe_stream(def: &StreamDefinition) -> Value {
    json!({
        "Name": def.name,
        "StreamFields": def.stream_fields,
        "Options": def.options,
        "StreamType": 0,
        "Statement": serde_json::Value::Null,
    })
}

/// eKuiper-style describe envelope for lookup tables.
pub fn describe_table(def: &TableDefinition) -> Value {
    json!({
        "Name": def.name,
        "StreamFields": def.stream_fields,
        "Options": def.options,
        "StreamType": 1,
        "Statement": serde_json::Value::Null,
    })
}

fn validate_video_options(
    options: &HashMap<String, String>,
    source_configs: &std::sync::Arc<parking_lot::RwLock<HashMap<String, Value>>>,
) -> Result<(), String> {
    if !options
        .iter()
        .any(|(k, v)| k.eq_ignore_ascii_case("type") && v.eq_ignore_ascii_case("video"))
    {
        return Ok(());
    }
    let mut vid_cfg = rekuiper_connectors::VideoSourceConfig::default();
    if let Some(conf_key) = options
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("conf_key") || k.eq_ignore_ascii_case("confkey"))
        .map(|(_, v)| v.trim())
        .filter(|s| !s.is_empty())
    {
        let lookup = format!("video/{}", conf_key);
        let configs_guard = source_configs.read();
        let conf_val = configs_guard
            .get(&lookup)
            .or_else(|| configs_guard.get(conf_key))
            .cloned();
        drop(configs_guard);
        if let Some(val) = conf_val {
            if let Ok(c) = serde_json::from_value::<rekuiper_connectors::VideoSourceConfig>(val) {
                vid_cfg = c;
            }
        }
    }
    if let Some(url) = options
        .iter()
        .find(|(k, _)| {
            k.eq_ignore_ascii_case("url")
                || k.eq_ignore_ascii_case("server")
                || k.eq_ignore_ascii_case("datasource")
        })
        .map(|(_, v)| v.trim())
        .filter(|s| !s.is_empty())
    {
        vid_cfg.url = url.to_string();
    }
    if let Some(interval) = options
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("interval"))
        .map(|(_, v)| v.trim())
        .filter(|s| !s.is_empty())
    {
        vid_cfg.set_interval_str(interval);
    }
    if let Some(fp) = options
        .iter()
        .find(|(k, _)| {
            k.eq_ignore_ascii_case("ffmpeg_path") || k.eq_ignore_ascii_case("ffmpegpath")
        })
        .map(|(_, v)| v.trim())
        .filter(|s| !s.is_empty())
    {
        vid_cfg.ffmpeg_path = fp.to_string();
    }
    vid_cfg.validate()
}

pub async fn list_streams(State(state): State<AppState>) -> impl IntoResponse {
    let streams = state.stream_manager.list_streams();
    Json(streams)
}

pub async fn create_stream(
    State(state): State<AppState>,
    Json(payload): Json<CreateStreamPayload>,
) -> Response {
    if let Some(sql) = payload.sql {
        // Stream management statements run inline: SHOW STREAMS lists names,
        // DESCRIBE STREAM reports one definition (both answer 201).
        let mut words = sql.split_whitespace();
        let head = (
            words.next().map(|w| w.to_ascii_uppercase()),
            words.next().map(|w| w.to_ascii_uppercase()),
        );
        if head == (Some("SHOW".to_string()), Some("STREAMS".to_string())) {
            return (
                StatusCode::CREATED,
                Json(state.stream_manager.list_streams()),
            )
                .into_response();
        }
        if head == (Some("DESCRIBE".to_string()), Some("STREAM".to_string())) {
            let target = words
                .next()
                .unwrap_or("")
                .trim_matches(['"', '\'', '`', ';']);
            if target.is_empty() {
                return (
                    StatusCode::BAD_REQUEST,
                    "Missing stream name in DESCRIBE STREAM",
                )
                    .into_response();
            }
            if let Some(def) = state.stream_manager.get_stream(target) {
                return (StatusCode::CREATED, Json(describe_stream(&def))).into_response();
            }
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": 3000,
                    "message": format!(
                        "describe stream error: Describe stream fails, {} is not found.",
                        target
                    )
                })),
            )
                .into_response();
        }
        let mut parser = Parser::new(&sql);
        match parser.parse_create_stream() {
            Ok(stmt) => {
                for (k, v) in &stmt.options {
                    if k.eq_ignore_ascii_case("buffer_full_policy")
                        && !v.eq_ignore_ascii_case("block")
                        && !v.eq_ignore_ascii_case("dropOldest")
                    {
                        return (
                            StatusCode::BAD_REQUEST,
                            format!(
                                "Invalid buffer_full_policy: '{}', must be 'block' or 'dropOldest'",
                                v
                            ),
                        )
                            .into_response();
                    }
                    if (k.eq_ignore_ascii_case("offset_reset")
                        || k.eq_ignore_ascii_case("offsetreset"))
                        && !v.eq_ignore_ascii_case("earliest")
                        && !v.eq_ignore_ascii_case("latest")
                    {
                        return (
                            StatusCode::BAD_REQUEST,
                            format!(
                                "Invalid offset_reset: '{}', must be 'earliest' or 'latest'",
                                v
                            ),
                        )
                            .into_response();
                    }
                }
                if let Err(e) = validate_video_options(&stmt.options, &state.source_configs) {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(json!({
                            "error": 3000,
                            "message": format!("Stream command error: {}", e)
                        })),
                    )
                        .into_response();
                }
                let stream_def = StreamDefinition {
                    name: stmt.name.clone(),
                    sql: sql.clone(),
                    stream_fields: to_stream_fields(stmt.fields),
                    options: stmt.options,
                };
                if let Err(e) = state.stream_manager.create_stream(stream_def).await {
                    let msg = if e.to_string().contains("already exists") {
                        format!(
                            "Stream command error: Create stream fails: Item {} already exists.",
                            stmt.name
                        )
                    } else {
                        format!("Stream command error: {}", e)
                    };
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(json!({
                            "error": 3000,
                            "message": msg
                        })),
                    )
                        .into_response();
                }
                state.stream_bus.get_or_create(&stmt.name);
                (
                    StatusCode::CREATED,
                    format!("Stream {} is created.", stmt.name),
                )
                    .into_response()
            }
            Err(e) => (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": 3000,
                    "message": format!("Stream command error: {}", e)
                })),
            )
                .into_response(),
        }
    } else if let Some(name) = payload.name {
        let stream_def = StreamDefinition {
            name: name.clone(),
            sql: "".to_string(),
            stream_fields: Vec::new(),
            options: HashMap::new(),
        };
        if let Err(e) = state.stream_manager.create_stream(stream_def).await {
            let msg = if e.to_string().contains("already exists") {
                format!(
                    "Stream command error: Create stream fails: Item {} already exists.",
                    name
                )
            } else {
                format!("Stream command error: {}", e)
            };
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": 3000,
                    "message": msg
                })),
            )
                .into_response();
        }
        state.stream_bus.get_or_create(&name);
        (StatusCode::CREATED, format!("Stream {} is created.", name)).into_response()
    } else {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": 3000,
                "message": "Missing sql or name in request"
            })),
        )
            .into_response()
    }
}

pub async fn get_stream(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if let Some(def) = state.stream_manager.get_stream(&name) {
        Json(describe_stream(&def)).into_response()
    } else {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": 3000,
                "message": format!(
                    "describe stream error: Describe stream fails, {} is not found.",
                    name
                )
            })),
        )
            .into_response()
    }
}

pub async fn delete_stream(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    match state.stream_manager.delete_stream(&name).await {
        Ok(_) => {
            cancel_stream_sources(&state, &name);
            (StatusCode::OK, format!("Stream {} is dropped.", name)).into_response()
        }
        Err(e) => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}

/// Replace a stream definition (eKuiper `PUT /streams/:name`). Accepts raw
/// `CREATE STREAM ...` DDL or a JSON envelope carrying `sql`; the path name
/// is canonical. Missing streams 404 instead of being silently created.
pub async fn update_stream(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if state.stream_manager.get_stream(&name).is_none() {
        return (StatusCode::NOT_FOUND, format!("Stream {} not found", name)).into_response();
    }
    if body.is_empty() {
        return (StatusCode::BAD_REQUEST, "Missing stream definition").into_response();
    }
    let sql = match serde_json::from_slice::<Value>(&body) {
        Ok(Value::Object(map)) => match map.get("sql").and_then(|v| v.as_str()) {
            Some(s) => s.to_string(),
            None => return (StatusCode::BAD_REQUEST, "Missing sql in request").into_response(),
        },
        Ok(_) => return (StatusCode::BAD_REQUEST, "Missing sql in request").into_response(),
        Err(_) => match String::from_utf8(body.to_vec()) {
            Ok(s) => s,
            Err(_) => {
                return (StatusCode::BAD_REQUEST, "Invalid stream definition").into_response();
            }
        },
    };
    let mut parser = Parser::new(&sql);
    let stmt = match parser.parse_create_stream() {
        Ok(s) => s,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("Invalid SQL: {}", e)).into_response(),
    };
    for (k, v) in &stmt.options {
        if k.eq_ignore_ascii_case("buffer_full_policy")
            && !v.eq_ignore_ascii_case("block")
            && !v.eq_ignore_ascii_case("dropOldest")
        {
            return (
                StatusCode::BAD_REQUEST,
                format!(
                    "Invalid buffer_full_policy: '{}', must be 'block' or 'dropOldest'",
                    v
                ),
            )
                .into_response();
        }
        if (k.eq_ignore_ascii_case("offset_reset") || k.eq_ignore_ascii_case("offsetreset"))
            && !v.eq_ignore_ascii_case("earliest")
            && !v.eq_ignore_ascii_case("latest")
        {
            return (
                StatusCode::BAD_REQUEST,
                format!(
                    "Invalid offset_reset: '{}', must be 'earliest' or 'latest'",
                    v
                ),
            )
                .into_response();
        }
    }
    if let Err(e) = validate_video_options(&stmt.options, &state.source_configs) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": 3000,
                "message": format!("Stream command error: {}", e)
            })),
        )
            .into_response();
    }
    let stream_def = StreamDefinition {
        name: name.clone(),
        sql: sql.clone(),
        stream_fields: to_stream_fields(stmt.fields),
        options: stmt.options,
    };
    if let Err(e) = state.stream_manager.update_stream(stream_def).await {
        return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
    }
    state.stream_bus.get_or_create(&name);
    (StatusCode::OK, format!("Stream {} is replaced.", name)).into_response()
}

/// HTTP push source following the eKuiper REST API.
///
/// Bounded lossless admission: the whole request is decoded and validated
/// BEFORE any record is admitted; the stream handle is resolved once; then
/// the batch is admitted with reserve-then-commit, accept-once semantics
/// (`StreamBus::publish_batch`): records stay in order and contiguous on
/// every subscriber, and the handler awaits downstream capacity. A `2xx`
/// means accepted into the in-memory pipeline, not durable delivery.
/// Batches larger than `MAX_HTTP_BATCH_RECORDS` are rejected with `413` and
/// malformed batches with `400`, both before admission. A `503` is returned
/// only when every subscriber closed before any record was committed, so
/// every non-2xx response admitted zero records and is safe to retry. There
/// is no timeout-based partial failure: overload manifests as slower `2xx`
/// responses, never silent loss with success acknowledgements.
pub async fn push_stream_data(
    State(state): State<AppState>,
    Path(name): Path<String>,
    req: axum::extract::Request,
) -> Response {
    let stream_def = match state.stream_manager.get_stream(&name) {
        Some(s) => s,
        None => {
            return (StatusCode::NOT_FOUND, format!("Stream {} not found", name)).into_response()
        }
    };

    let body_bytes = match axum::body::to_bytes(req.into_body(), 10 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("Fail to read request body: {}\n", e),
            )
                .into_response();
        }
    };

    let format = stream_def
        .options
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("FORMAT"))
        .map(|(_, v)| v.trim())
        .unwrap_or("json")
        .to_ascii_lowercase();

    let mut records = Vec::new();

    if format == "binary" {
        let val = if let Ok(json_val) = serde_json::from_slice::<Value>(&body_bytes) {
            match json_val {
                Value::Object(map) => {
                    let mut data: HashMap<String, Value> = map.into_iter().collect();
                    if !data.contains_key("self") {
                        if let Some((_, v)) = data.iter().next() {
                            data.insert("self".to_string(), v.clone());
                        }
                    }
                    data
                }
                Value::String(s) => {
                    let mut data = HashMap::new();
                    data.insert("self".to_string(), Value::String(s));
                    data
                }
                other => {
                    let mut data = HashMap::new();
                    data.insert("self".to_string(), other);
                    data
                }
            }
        } else {
            let str_val = match std::str::from_utf8(&body_bytes) {
                Ok(s) => s.to_string(),
                Err(_) => {
                    use base64::Engine as _;
                    base64::engine::general_purpose::STANDARD.encode(&body_bytes)
                }
            };
            let mut data = HashMap::new();
            data.insert("self".to_string(), Value::String(str_val));
            data
        };
        let mut data = val;
        if stream_def.stream_fields.len() == 1 {
            let col = &stream_def.stream_fields[0].name;
            if let Some(v) = data.get("self").cloned() {
                data.entry(col.clone()).or_insert(v);
            }
        }
        records.push(StreamRecord::new(data));
    } else if format == "delimited" || format == "csv" {
        let delimiter = stream_def
            .options
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("DELIMITER"))
            .map(|(_, v)| rekuiper_connectors::DelimitedCodec::delimiter_from_name(v))
            .unwrap_or(',');
        let headers: Vec<String> = stream_def
            .stream_fields
            .iter()
            .map(|f| f.name.clone())
            .collect();
        let codec = rekuiper_connectors::DelimitedCodec::new(delimiter, headers);

        if let Ok(Value::Array(items)) = serde_json::from_slice::<Value>(&body_bytes) {
            for item in items {
                if let Value::Object(map) = item {
                    records.push(StreamRecord::new(
                        map.into_iter().collect::<IndexMap<_, _>>(),
                    ));
                }
            }
        } else if let Ok(Value::Object(map)) = serde_json::from_slice::<Value>(&body_bytes) {
            records.push(StreamRecord::new(
                map.into_iter().collect::<IndexMap<_, _>>(),
            ));
        } else if let Ok(text) = std::str::from_utf8(&body_bytes) {
            for line in text.lines() {
                let line = line.trim();
                if !line.is_empty() {
                    let row = codec.decode_row(line);
                    records.push(StreamRecord::new(row));
                }
            }
        } else {
            return (
                StatusCode::BAD_REQUEST,
                "Fail to decode delimited payload as UTF-8 string or JSON",
            )
                .into_response();
        }
    } else {
        // Standard JSON format
        let payload: Value = match serde_json::from_slice(&body_bytes) {
            Ok(v) => v,
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    "Expected a JSON object or an array of JSON objects",
                )
                    .into_response();
            }
        };

        let objects: Vec<Value> = match payload {
            Value::Array(items) => items,
            Value::Object(_) => vec![payload],
            _ => {
                return (
                    StatusCode::BAD_REQUEST,
                    "Expected a JSON object or an array of JSON objects",
                )
                    .into_response();
            }
        };

        if objects.len() > MAX_HTTP_BATCH_RECORDS {
            return (
                StatusCode::PAYLOAD_TOO_LARGE,
                format!(
                    "Batch of {} records exceeds limit of {}",
                    objects.len(),
                    MAX_HTTP_BATCH_RECORDS
                ),
            )
                .into_response();
        }

        for item in objects {
            let Value::Object(map) = item else {
                return (
                    StatusCode::BAD_REQUEST,
                    "Expected a JSON object or an array of JSON objects",
                )
                    .into_response();
            };
            let data: HashMap<String, Value> = map.into_iter().collect();
            records.push(StreamRecord::new(data));
        }
    }

    if records.len() > MAX_HTTP_BATCH_RECORDS {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            format!(
                "Batch of {} records exceeds limit of {}",
                records.len(),
                MAX_HTTP_BATCH_RECORDS
            ),
        )
            .into_response();
    }

    // Resolve the stream handle once per request
    let sender = state.stream_bus.get_or_create(&name);
    if records.is_empty() {
        return (StatusCode::OK, "Data ingested successfully.\n").into_response();
    }
    let _ = sender.send_batch(records).await;
    (StatusCode::OK, "Data ingested successfully.\n").into_response()
}

pub async fn list_tables(State(state): State<AppState>) -> impl IntoResponse {
    let tables = state.table_manager.list_tables();
    Json(tables)
}

/// Table lookup ingestion: accepts a single JSON object or an array of
/// objects and appends each as a lookup row for the table.
pub async fn push_table_data(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(payload): Json<Value>,
) -> Response {
    let objects: Vec<Value> = match payload {
        Value::Array(items) => items,
        Value::Object(_) => vec![payload],
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                "Expected a JSON object or an array of JSON objects",
            )
                .into_response();
        }
    };

    for item in objects {
        let Value::Object(map) = item else {
            return (
                StatusCode::BAD_REQUEST,
                "Expected a JSON object or an array of JSON objects",
            )
                .into_response();
        };
        let row: HashMap<String, Value> = map.into_iter().collect();
        state.table_manager.insert_table_row(&name, row);
    }

    (StatusCode::OK, "Table data ingested successfully.\n").into_response()
}

pub async fn create_table(
    State(state): State<AppState>,
    Json(payload): Json<CreateStreamPayload>,
) -> Response {
    let Some(sql) = payload.sql else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": 3000,
                "message": "Missing sql in request"
            })),
        )
            .into_response();
    };
    let mut parser = Parser::new(&sql);
    match parser.parse_create_table() {
        Ok(stmt) => {
            let table_def = TableDefinition {
                name: stmt.name.clone(),
                sql: sql.clone(),
                stream_fields: to_stream_fields(stmt.fields),
                options: stmt.options,
            };
            let table_name = stmt.name.clone();
            if let Err(e) = state.table_manager.create_table(table_def).await {
                let msg = if e.to_string().contains("already exists") {
                    format!(
                        "Table command error: Create table fails: Item {} already exists.",
                        table_name
                    )
                } else {
                    format!("Table command error: {}", e)
                };
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({
                        "error": 3000,
                        "message": msg
                    })),
                )
                    .into_response();
            }
            bootstrap_table_source(&state, &table_name);
            (
                StatusCode::CREATED,
                format!("Table {} is created.", table_name),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": 3000,
                "message": format!("Table command error: {}", e)
            })),
        )
            .into_response(),
    }
}

pub async fn get_table(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if let Some(def) = state.table_manager.get_table(&name) {
        Json(describe_table(&def)).into_response()
    } else {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": 3000,
                "message": format!(
                    "describe table error: Describe table fails, {} is not found.",
                    name
                )
            })),
        )
            .into_response()
    }
}

pub async fn delete_table(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    cancel_table_source(&state, &name);
    match state.table_manager.delete_table(&name).await {
        Ok(_) => (StatusCode::OK, format!("Table {} is dropped.", name)).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}

/// Replace a table definition (eKuiper `PUT /tables/:name`). Accepts raw
/// `CREATE TABLE ...` DDL or a JSON envelope carrying `sql`; the path name
/// is canonical. Missing tables 404 instead of being silently created.
pub async fn update_table(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if state.table_manager.get_table(&name).is_none() {
        return (StatusCode::NOT_FOUND, format!("Table {} not found", name)).into_response();
    }
    if body.is_empty() {
        return (StatusCode::BAD_REQUEST, "Missing table definition").into_response();
    }
    let sql = match serde_json::from_slice::<Value>(&body) {
        Ok(Value::Object(map)) => match map.get("sql").and_then(|v| v.as_str()) {
            Some(s) => s.to_string(),
            None => return (StatusCode::BAD_REQUEST, "Missing sql in request").into_response(),
        },
        Ok(_) => return (StatusCode::BAD_REQUEST, "Missing sql in request").into_response(),
        Err(_) => match String::from_utf8(body.to_vec()) {
            Ok(s) => s,
            Err(_) => {
                return (StatusCode::BAD_REQUEST, "Invalid table definition").into_response();
            }
        },
    };
    let mut parser = Parser::new(&sql);
    let stmt = match parser.parse_create_table() {
        Ok(s) => s,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("Invalid SQL: {}", e)).into_response(),
    };
    let table_def = TableDefinition {
        name: name.clone(),
        sql: sql.clone(),
        stream_fields: to_stream_fields(stmt.fields),
        options: stmt.options,
    };
    cancel_table_source(&state, &name);
    if let Err(e) = state.table_manager.update_table(table_def).await {
        return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
    }
    bootstrap_table_source(&state, &name);
    (StatusCode::OK, format!("Table {} is replaced.", name)).into_response()
}

pub async fn get_table_details(State(state): State<AppState>) -> impl IntoResponse {
    let defs = state.table_manager.list_table_definitions();
    let summaries: Vec<Value> = defs
        .into_iter()
        .map(|def| {
            let table_type = def
                .options
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("TYPE"))
                .map(|(_, v)| v.to_ascii_lowercase())
                .unwrap_or_else(|| "memory".to_string());
            let format = def
                .options
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("FORMAT"))
                .map(|(_, v)| v.to_ascii_lowercase())
                .unwrap_or_else(|| "json".to_string());
            json!({
                "name": def.name,
                "type": table_type,
                "format": format,
            })
        })
        .collect();
    Json(summaries)
}

pub async fn get_stream_details(State(state): State<AppState>) -> impl IntoResponse {
    let names = state.stream_manager.list_streams();
    let summaries: Vec<Value> = names
        .iter()
        .filter_map(|n| state.stream_manager.get_stream(n))
        .map(|def| {
            let stream_type = def
                .options
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("TYPE"))
                .map(|(_, v)| v.to_ascii_lowercase())
                .unwrap_or_else(|| "mqtt".to_string());
            let format = def
                .options
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("FORMAT"))
                .map(|(_, v)| v.to_ascii_lowercase())
                .unwrap_or_else(|| "json".to_string());
            json!({
                "name": def.name,
                "type": stream_type,
                "format": format,
            })
        })
        .collect();
    Json(summaries)
}

/// Field-type map for a describe subject: `{name: {type, index}}`.
pub fn field_schema_map(fields: &[StreamField]) -> Value {
    let mut schema_map = serde_json::Map::new();
    for (idx, field) in fields.iter().enumerate() {
        schema_map.insert(
            field.name.clone(),
            json!({
                "type": field.field_type.to_ascii_lowercase(),
                "index": idx
            }),
        );
    }
    Value::Object(schema_map)
}

pub async fn get_stream_schema(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Response {
    if let Some(def) = state.stream_manager.get_stream(&name) {
        Json(field_schema_map(&def.stream_fields)).into_response()
    } else {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": 3000,
                "message": format!(
                    "describe stream error: Describe stream fails, {} is not found.",
                    name
                )
            })),
        )
            .into_response()
    }
}

pub async fn get_table_schema(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Some(def) = state.table_manager.get_table(&name) {
        Json(field_schema_map(&def.stream_fields)).into_response()
    } else {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": 3000,
                "message": format!(
                    "describe table error: Describe table fails, {} is not found.",
                    name
                )
            })),
        )
            .into_response()
    }
}

pub async fn push_otlp_data(
    State(state): State<AppState>,
    req: axum::extract::Request,
) -> Response {
    let uri = req.uri();
    let query_stream = uri.query().and_then(|q| {
        q.split('&').find_map(|pair| {
            let (k, v) = pair.split_once('=')?;
            if k.eq_ignore_ascii_case("stream") {
                Some(v.to_string())
            } else {
                None
            }
        })
    });
    let header_stream = req
        .headers()
        .get("x-rekuiper-stream")
        .and_then(|v| v.to_str().ok().map(|s| s.to_string()));

    let target_stream = query_stream.or(header_stream).unwrap_or_else(|| {
        for name in state.stream_manager.list_streams() {
            if let Some(def) = state.stream_manager.get_stream(&name) {
                if def.options.iter().any(|(k, v)| {
                    k.eq_ignore_ascii_case("TYPE")
                        && (v.eq_ignore_ascii_case("otlp")
                            || v.eq_ignore_ascii_case("opentelemetry"))
                }) {
                    return name;
                }
            }
        }
        "otlp".to_string()
    });

    let body_bytes = match axum::body::to_bytes(req.into_body(), 10 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("Fail to read request body: {}\n", e),
            )
                .into_response();
        }
    };

    let stream_tx = state.stream_bus.get_or_create(&target_stream);

    match rekuiper_connectors::parse_otlp_payload(&body_bytes) {
        Ok(records) => {
            for rec in records {
                let _ = stream_tx.send(rec).await;
            }
            (
                StatusCode::OK,
                [("content-type", "application/json")],
                "{\"partialSuccess\":{}}\n",
            )
                .into_response()
        }
        Err(e) => {
            // Push synthetic error record so rules with onError: dlq capture it under DlqStage::CodecDecode
            let mut err_data = HashMap::new();
            err_data.insert("__raw_error__".to_string(), json!(e.to_string()));
            err_data.insert(
                "__raw_payload__".to_string(),
                json!(String::from_utf8_lossy(&body_bytes).to_string()),
            );
            let _ = stream_tx
                .send(rekuiper_core::model::StreamRecord::new(err_data))
                .await;

            (
                StatusCode::BAD_REQUEST,
                [("content-type", "application/json")],
                format!("{{\"error\":\"{}\"}}\n", e),
            )
                .into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AppState;
    use rekuiper_core::{RuleManager, StreamBus, StreamManager, TableManager};

    #[tokio::test]
    async fn test_create_stream_rejects_disallowed_video_url() {
        let bus = StreamBus::new();
        let state = AppState::new(
            "test".to_string(),
            rekuiper_conf::KuiperConfig::default(),
            StreamManager::new(),
            TableManager::new(),
            RuleManager::new(bus.clone()),
            bus,
        );
        let payload = CreateStreamPayload {
            sql: Some(
                "CREATE STREAM v_bad () WITH (TYPE=\"video\", URL=\"/etc/passwd\")".to_string(),
            ),
            name: None,
        };
        let resp = create_stream(State(state), Json(payload)).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_create_stream_accepts_valid_video_url() {
        let bus = StreamBus::new();
        let state = AppState::new(
            "test".to_string(),
            rekuiper_conf::KuiperConfig::default(),
            StreamManager::new(),
            TableManager::new(),
            RuleManager::new(bus.clone()),
            bus,
        );
        let payload = CreateStreamPayload {
            sql: Some(
                "CREATE STREAM v_good () WITH (TYPE=\"video\", URL=\"rtsp://127.0.0.1:8554/live\")"
                    .to_string(),
            ),
            name: None,
        };
        let resp = create_stream(State(state), Json(payload)).await;
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn test_update_stream_rejects_disallowed_video_url() {
        let bus = StreamBus::new();
        let state = AppState::new(
            "test".to_string(),
            rekuiper_conf::KuiperConfig::default(),
            StreamManager::new(),
            TableManager::new(),
            RuleManager::new(bus.clone()),
            bus,
        );
        let create_payload = CreateStreamPayload {
            sql: Some(
                "CREATE STREAM v_up () WITH (TYPE=\"video\", URL=\"rtsp://127.0.0.1:8554/live\")"
                    .to_string(),
            ),
            name: None,
        };
        let resp = create_stream(State(state.clone()), Json(create_payload)).await;
        assert_eq!(resp.status(), StatusCode::CREATED);

        let update_body = axum::body::Bytes::from(
            json!({
                "sql": "CREATE STREAM v_up () WITH (TYPE=\"video\", URL=\"/etc/shadow\")"
            })
            .to_string(),
        );
        let update_resp = update_stream(State(state), Path("v_up".to_string()), update_body).await;
        assert_eq!(update_resp.status(), StatusCode::BAD_REQUEST);
    }
}
