use crate::state::{
    check_valid_name, compile_and_register_js_udf, AppState, ExternalFunction, JavascriptUdf,
    ServiceDetail,
};
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use rekuiper_core::model::SchemaDefinition;
use rekuiper_core::PluginDefinition;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;

pub async fn list_services(State(state): State<AppState>) -> impl IntoResponse {
    let services = state.services.read();
    let mut names: Vec<String> = services.keys().cloned().collect();
    names.sort();
    Json(names)
}

pub async fn create_service(State(state): State<AppState>, Json(body): Json<Value>) -> Response {
    let name = body
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }

    if let Some(resp) = reject_missing_plugin_file(&body) {
        return resp;
    }

    if state.services.read().contains_key(&name) {
        return (
            StatusCode::BAD_REQUEST,
            format!("Service '{}' already exists", name),
        )
            .into_response();
    }

    let mut about = HashMap::new();
    if let Some(ab) = body.get("About").and_then(|v| v.as_object()) {
        for (k, v) in ab {
            about.insert(k.clone(), v.clone());
        }
    } else if let Some(file) = body.get("file").and_then(|v| v.as_str()) {
        about.insert("file".to_string(), json!(file));
    }

    let mut interfaces = HashMap::new();
    if let Some(ifaces) = body.get("Interfaces").and_then(|v| v.as_object()) {
        for (k, v) in ifaces {
            interfaces.insert(k.clone(), v.clone());
        }
    }

    let mut functions = Vec::new();
    if let Some(funcs) = body.get("functions").and_then(|v| v.as_array()) {
        for f in funcs {
            if let Ok(func) = serde_json::from_value::<ExternalFunction>(f.clone()) {
                functions.push(func);
            }
        }
    }

    if functions.is_empty() {
        for (iface_name, iface_val) in &interfaces {
            if let Some(methods) = iface_val.get("methods").and_then(|m| m.as_array()) {
                for m in methods {
                    if let Some(m_str) = m.as_str() {
                        functions.push(ExternalFunction {
                            service_name: name.clone(),
                            interface_name: iface_name.clone(),
                            addr: iface_val
                                .get("addr")
                                .and_then(|a| a.as_str())
                                .unwrap_or("")
                                .to_string(),
                            method_name: m_str.to_string(),
                            func_name: m_str.to_string(),
                        });
                    }
                }
            }
        }
    }

    let detail = ServiceDetail {
        about,
        interfaces,
        functions,
    };
    state.services.write().insert(name.clone(), detail);
    (
        StatusCode::CREATED,
        format!("Service '{}' registered", name),
    )
        .into_response()
}

pub async fn get_service(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let services = state.services.read();
    if let Some(service) = services.get(&name) {
        Json(service.clone()).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            format!("Service '{}' not found", name),
        )
            .into_response()
    }
}

pub async fn update_service(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<Value>,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let mut services = state.services.write();
    let entry = match services.get_mut(&name) {
        Some(s) => s,
        None => {
            return (
                StatusCode::NOT_FOUND,
                format!("Service '{}' not found", name),
            )
                .into_response();
        }
    };

    if let Some(ab) = body.get("About").and_then(|v| v.as_object()) {
        for (k, v) in ab {
            entry.about.insert(k.clone(), v.clone());
        }
    }
    if let Some(ifaces) = body.get("Interfaces").and_then(|v| v.as_object()) {
        for (k, v) in ifaces {
            entry.interfaces.insert(k.clone(), v.clone());
        }
    }
    if let Some(funcs) = body.get("functions").and_then(|v| v.as_array()) {
        let mut new_funcs = Vec::new();
        for f in funcs {
            if let Ok(func) = serde_json::from_value::<ExternalFunction>(f.clone()) {
                new_funcs.push(func);
            }
        }
        if !new_funcs.is_empty() {
            entry.functions = new_funcs;
        }
    }

    (StatusCode::OK, "Service updated").into_response()
}

pub async fn delete_service(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if state.services.write().remove(&name).is_some() {
        (StatusCode::OK, format!("Service '{}' deleted", name)).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            format!("Service '{}' not found", name),
        )
            .into_response()
    }
}

pub async fn list_service_functions(State(state): State<AppState>) -> impl IntoResponse {
    let services = state.services.read();
    let mut list = Vec::new();
    for svc in services.values() {
        for f in &svc.functions {
            list.push(f.clone());
        }
    }
    list.sort_by(|a, b| a.func_name.cmp(&b.func_name));
    Json(list)
}

pub async fn get_service_function(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let services = state.services.read();
    for svc in services.values() {
        for f in &svc.functions {
            if f.func_name.eq_ignore_ascii_case(&name) {
                return Json(f.clone()).into_response();
            }
        }
    }
    (
        StatusCode::NOT_FOUND,
        format!("External function '{}' not found", name),
    )
        .into_response()
}

pub async fn list_javascript_udfs(State(state): State<AppState>) -> impl IntoResponse {
    let udfs = state.js_udfs.read();
    let mut ids: Vec<String> = udfs.keys().cloned().collect();
    ids.sort();
    Json(ids)
}

pub async fn create_javascript_udf(
    State(state): State<AppState>,
    Json(udf): Json<JavascriptUdf>,
) -> Response {
    if let Err(resp) = check_valid_name(&udf.id) {
        return resp;
    }
    if state.js_udfs.read().contains_key(&udf.id) {
        return (
            StatusCode::BAD_REQUEST,
            format!("JavaScript UDF '{}' already exists", udf.id),
        )
            .into_response();
    }
    if let Err(e) = compile_and_register_js_udf(&udf) {
        return (StatusCode::BAD_REQUEST, e).into_response();
    }
    state.js_udfs.write().insert(udf.id.clone(), udf.clone());
    (
        StatusCode::CREATED,
        format!("JavaScript UDF '{}' created", udf.id),
    )
        .into_response()
}

pub async fn get_javascript_udf(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&id) {
        return resp;
    }
    let udfs = state.js_udfs.read();
    if let Some(udf) = udfs.get(&id) {
        Json(udf.clone()).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            format!("JavaScript UDF '{}' not found", id),
        )
            .into_response()
    }
}

pub async fn update_javascript_udf(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Response {
    if let Err(resp) = check_valid_name(&id) {
        return resp;
    }
    let mut udf = state
        .js_udfs
        .read()
        .get(&id)
        .cloned()
        .unwrap_or_else(|| JavascriptUdf {
            id: id.clone(),
            description: String::new(),
            script: format!("function {}(x) {{ return x; }}", id),
            is_agg: false,
        });

    if let Some(desc) = body.get("description").and_then(|v| v.as_str()) {
        udf.description = desc.to_string();
    }
    if let Some(sc) = body.get("script").and_then(|v| v.as_str()) {
        if !sc.trim().is_empty() {
            udf.script = sc.to_string();
        }
    }
    if let Some(agg) = body.get("isAgg").and_then(|v| v.as_bool()) {
        udf.is_agg = agg;
    }
    udf.id = id.clone();

    if let Err(e) = compile_and_register_js_udf(&udf) {
        return (StatusCode::BAD_REQUEST, e).into_response();
    }
    state.js_udfs.write().insert(id, udf);
    (StatusCode::OK, "JavaScript UDF updated").into_response()
}

pub async fn delete_javascript_udf(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Response {
    if let Err(resp) = check_valid_name(&id) {
        return resp;
    }
    if state.js_udfs.write().remove(&id).is_some() {
        rekuiper_core::plugin::get_global_udf_registry().unregister_udf(&id);
        (StatusCode::OK, "JavaScript UDF deleted").into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            format!("JavaScript UDF '{}' not found", id),
        )
            .into_response()
    }
}

// ---------------------------------------------------------------------------
// Schema registry (`/schemas/:kind[/:name]`).
// ---------------------------------------------------------------------------

/// Payload for registering or updating a schema; the kind always comes from
/// the URL path.
#[derive(Debug, serde::Deserialize)]
pub struct SchemaPayload {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    file: Option<String>,
}

pub async fn list_schemas(State(state): State<AppState>, Path(kind): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&kind) {
        return resp;
    }
    Json(state.schema_manager.list_schemas(&kind)).into_response()
}

pub async fn create_schema(
    State(state): State<AppState>,
    Path(kind): Path<String>,
    Json(payload): Json<SchemaPayload>,
) -> Response {
    if let Err(resp) = check_valid_name(&kind) {
        return resp;
    }
    if !kind.eq_ignore_ascii_case("protobuf") && !kind.eq_ignore_ascii_case("custom") {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "message": format!("unsupported schema type {}", kind)
            })),
        )
            .into_response();
    }
    let Some(name) = payload.name.filter(|n| !n.is_empty()) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"message": "Missing schema name"})),
        )
            .into_response();
    };
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if state.schema_manager.get_schema(&kind, &name).is_some() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "message": format!("Schema {} already registered", name)
            })),
        )
            .into_response();
    }
    let def = SchemaDefinition {
        name,
        kind,
        content: payload.content,
        file: payload.file,
    };
    let created = def.name.clone();
    let _ = state.schema_manager.register_schema(def).await;
    (
        StatusCode::CREATED,
        format!("Schema {} is created.\n", created),
    )
        .into_response()
}

pub async fn get_schema(
    State(state): State<AppState>,
    Path((kind, name)): Path<(String, String)>,
    headers: axum::http::HeaderMap,
) -> Response {
    if let Err(resp) = check_valid_name(&kind) {
        return resp;
    }
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let Some(def) = state.schema_manager.get_schema(&kind, &name) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({
                "message": format!("Schema {}/{} not found", kind, name)
            })),
        )
            .into_response();
    };
    let wants_text = headers
        .get(axum::http::header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.contains("text/plain"));
    if wants_text {
        (
            StatusCode::OK,
            [(
                axum::http::header::CONTENT_TYPE,
                "text/plain; charset=utf-8",
            )],
            def.content.unwrap_or_default(),
        )
            .into_response()
    } else {
        Json(def).into_response()
    }
}

pub async fn update_schema(
    State(state): State<AppState>,
    Path((kind, name)): Path<(String, String)>,
    Json(payload): Json<SchemaPayload>,
) -> Response {
    if let Err(resp) = check_valid_name(&kind) {
        return resp;
    }
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    // Upsert: merge over any existing definition so partial bodies work.
    let mut def = state
        .schema_manager
        .get_schema(&kind, &name)
        .unwrap_or(SchemaDefinition {
            name: name.clone(),
            kind: kind.clone(),
            content: None,
            file: None,
        });
    if payload.content.is_some() {
        def.content = payload.content;
    }
    if payload.file.is_some() {
        def.file = payload.file;
    }
    let _ = state.schema_manager.register_schema(def).await;
    (
        StatusCode::OK,
        format!("Schema {}/{} is updated.\n", kind, name),
    )
        .into_response()
}

/// Extract one file part from a `multipart/form-data` body without extra
/// dependencies: splits on the boundary and returns the bytes after the
/// first part's blank header line. Returns `None` when the shape is not a
/// recognizable single-file upload.
pub fn extract_multipart_file(body: &[u8], boundary: &str) -> Option<Vec<u8>> {
    if boundary.is_empty() {
        return None;
    }
    let sep = format!("--{}", boundary);
    let text = std::str::from_utf8(body).ok()?;
    for raw in text.split(&sep) {
        // Skip the preamble and the closing `--` epilogue, which carry no
        // headers and must not abort the scan.
        let mut splitter = raw.splitn(2, "\r\n\r\n");
        let headers = splitter.next().unwrap_or("");
        let Some(content) = splitter.next() else {
            continue;
        };
        if headers.to_ascii_lowercase().contains("filename=") {
            let content = content.strip_suffix("\r\n").unwrap_or(content);
            return Some(content.as_bytes().to_vec());
        }
    }
    None
}

/// Schema file upload (baseline `PUT /schemas/:type/:name/upload`): accepts
/// `multipart/form-data` file parts as well as raw body bytes, stores the
/// content, and answers `{"type","name"}`. An empty body registers an empty
/// shell so metadata-only flows keep working.
pub async fn upload_schema(
    State(state): State<AppState>,
    Path((kind, name)): Path<(String, String)>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Response {
    if let Err(resp) = check_valid_name(&kind) {
        return resp;
    }
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let content = if body.is_empty() {
        String::new()
    } else if let Some(content_type) = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
    {
        if content_type.starts_with("multipart/form-data") {
            let boundary = content_type
                .split("boundary=")
                .nth(1)
                .unwrap_or("")
                .trim()
                .trim_matches('"')
                .to_string();
            match extract_multipart_file(&body, &boundary) {
                Some(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
                None => {
                    return (StatusCode::BAD_REQUEST, "Invalid multipart upload").into_response();
                }
            }
        } else {
            String::from_utf8_lossy(&body).into_owned()
        }
    } else {
        String::from_utf8_lossy(&body).into_owned()
    };
    let _ = state
        .schema_manager
        .register_schema(SchemaDefinition {
            name: name.clone(),
            kind: kind.clone(),
            content: Some(content),
            file: None,
        })
        .await;
    (StatusCode::OK, Json(json!({ "type": kind, "name": name }))).into_response()
}

pub async fn delete_schema(
    State(state): State<AppState>,
    Path((kind, name)): Path<(String, String)>,
) -> Response {
    if let Err(resp) = check_valid_name(&kind) {
        return resp;
    }
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if state.schema_manager.get_schema(&kind, &name).is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({
                "message": format!("Schema {}/{} not found", kind, name)
            })),
        )
            .into_response();
    }
    let _ = state.schema_manager.delete_schema(&kind, &name).await;
    (
        StatusCode::OK,
        format!("Schema {}/{} is dropped.\n", kind, name),
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// Plugin registry (`/plugins/sources`, `/plugins/sinks`, `/plugins/functions`, `/plugins/udfs`).
// ---------------------------------------------------------------------------

const BUILTIN_SOURCES: &[&str] = &[
    "edgex",
    "file",
    "http",
    "httppull",
    "httppush",
    "kafka",
    "memory",
    "mqtt",
    "neuron",
    "redis",
    "redisSub",
    "simulator",
    "sql",
    "websocket",
];

const BUILTIN_SINKS: &[&str] = &[
    "edgex",
    "file",
    "http",
    "influx",
    "influx2",
    "iotdb",
    "kafka",
    "log",
    "memory",
    "mqtt",
    "neuron",
    "nop",
    "redis",
    "redisPub",
    "rest",
    "tdengine",
    "tdengine3",
    "websocket",
];

pub fn is_builtin_source(name: &str) -> bool {
    BUILTIN_SOURCES
        .iter()
        .any(|&s| s.eq_ignore_ascii_case(name))
}

pub fn is_builtin_sink(name: &str) -> bool {
    BUILTIN_SINKS.iter().any(|&s| s.eq_ignore_ascii_case(name))
}

pub async fn list_source_plugins(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.plugin_manager.list_plugins("source"))
}

pub async fn create_source_plugin(
    State(state): State<AppState>,
    Json(payload): Json<Value>,
) -> Response {
    create_plugin_of_type(&state, "source", payload).await
}

pub async fn get_source_plugin(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Response {
    get_typed_plugin(&state, "source", &name)
}

pub async fn update_source_plugin(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    update_typed_plugin(&state, "source", &name, body).await
}

pub async fn delete_source_plugin(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Response {
    delete_typed_plugin(&state, &name).await
}

pub async fn list_sink_plugins(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.plugin_manager.list_plugins("sink"))
}

pub async fn create_sink_plugin(
    State(state): State<AppState>,
    Json(payload): Json<Value>,
) -> Response {
    create_plugin_of_type(&state, "sink", payload).await
}

pub async fn get_sink_plugin(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    get_typed_plugin(&state, "sink", &name)
}

pub async fn update_sink_plugin(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    update_typed_plugin(&state, "sink", &name, body).await
}

pub async fn delete_sink_plugin(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Response {
    delete_typed_plugin(&state, &name).await
}

pub async fn list_function_plugins(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.plugin_manager.list_plugins("function"))
}

pub async fn create_function_plugin(
    State(state): State<AppState>,
    Json(payload): Json<Value>,
) -> Response {
    create_plugin_of_type(&state, "function", payload).await
}

pub async fn get_function_plugin(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Response {
    get_typed_plugin(&state, "function", &name)
}

pub async fn update_function_plugin(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    update_typed_plugin(&state, "function", &name, body).await
}

pub async fn delete_function_plugin(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Response {
    delete_typed_plugin(&state, &name).await
}

pub async fn register_function_plugin(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if state.plugin_manager.get_plugin(&name).is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": 1000,
                "message": format!("plugin {} is not found", name)
            })),
        )
            .into_response();
    }
    (StatusCode::OK, format!("Plugin {} is registered.\n", name)).into_response()
}

pub async fn list_prebuild_plugins() -> impl IntoResponse {
    Json(json!([]))
}

pub async fn list_wasm_plugins() -> impl IntoResponse {
    Json(rekuiper_core::wasm::get_global_wasm_registry().list_modules())
}

pub async fn create_wasm_plugin(Json(payload): Json<Value>) -> Response {
    let name = payload.get("name").and_then(|v| v.as_str()).unwrap_or("");
    if name.is_empty() {
        return (StatusCode::BAD_REQUEST, "Missing plugin 'name'").into_response();
    }
    let bytes = if let Some(b64) = payload.get("bytecode").and_then(|v| v.as_str()) {
        match base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b64) {
            Ok(b) => b,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    format!("Invalid base64 bytecode: {}", e),
                )
                    .into_response()
            }
        }
    } else if let Some(fpath) = payload.get("file").and_then(|v| v.as_str()) {
        let clean_path = fpath.trim_start_matches("file://");
        match std::fs::read(clean_path) {
            Ok(b) => b,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    format!("Failed to read file {}: {}", clean_path, e),
                )
                    .into_response()
            }
        }
    } else {
        return (
            StatusCode::BAD_REQUEST,
            "Missing 'file' or 'bytecode' parameter",
        )
            .into_response();
    };

    match rekuiper_core::wasm::get_global_wasm_registry().register_module_as_udfs(name, &bytes) {
        Ok(funcs) => (
            StatusCode::CREATED,
            Json(json!({"name": name, "functions": funcs})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to register WASM module: {}", e),
        )
            .into_response(),
    }
}

pub async fn delete_wasm_plugin(Path(name): Path<String>) -> Response {
    if rekuiper_core::wasm::get_global_wasm_registry().unregister_module(&name) {
        StatusCode::OK.into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            format!("WASM plugin {} not found", name),
        )
            .into_response()
    }
}

pub async fn list_udf_plugins(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.plugin_manager.list_plugins("udf"))
}

pub async fn create_udf_plugin(
    State(state): State<AppState>,
    Json(payload): Json<Value>,
) -> Response {
    create_plugin_of_type(&state, "udf", payload).await
}

pub async fn get_udf_plugin(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    get_typed_plugin(&state, "udf", &name)
}

pub async fn delete_udf_plugin(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Response {
    delete_typed_plugin(&state, &name).await
}

pub async fn list_portable_plugins(State(state): State<AppState>) -> impl IntoResponse {
    let mut names: Vec<String> = state.portable_plugins.read().keys().cloned().collect();
    names.sort();
    Json(names)
}

pub async fn create_portable_plugin(
    State(_state): State<AppState>,
    Json(payload): Json<Value>,
) -> Response {
    if payload.get("name").and_then(Value::as_str).is_none() {
        return (StatusCode::BAD_REQUEST, "missing name").into_response();
    }
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({
            "error": 1001,
            "message": "portable plugin execution is not implemented"
        })),
    )
        .into_response()
}

pub async fn get_portable_plugin(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if let Some((info, _)) = state.portable_plugins.read().get(&name) {
        Json(info.clone()).into_response()
    } else {
        (StatusCode::NOT_FOUND, format!("Plugin {} not found", name)).into_response()
    }
}

pub async fn update_portable_plugin(
    State(_state): State<AppState>,
    Path(name): Path<String>,
    _body: Bytes,
) -> Response {
    if let Err(resp) = check_valid_name(name.as_str()) {
        return resp;
    }
    (
        StatusCode::NOT_IMPLEMENTED,
        format!(
            "Portable plugin {} cannot be updated: runtime support is unavailable\n",
            name
        ),
    )
        .into_response()
}

pub async fn delete_portable_plugin(
    State(_state): State<AppState>,
    Path(name): Path<String>,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    (
        StatusCode::NOT_IMPLEMENTED,
        format!(
            "Portable plugin {} cannot be deleted: runtime support is unavailable\n",
            name
        ),
    )
        .into_response()
}

pub async fn get_portable_plugin_status(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if let Some((_, status)) = state.portable_plugins.read().get(&name) {
        Json(status.clone()).into_response()
    } else {
        (StatusCode::NOT_FOUND, format!("Plugin {} not found", name)).into_response()
    }
}

pub fn typed_plugin_payload(
    plugin_type: &str,
    mut payload: Value,
) -> Result<PluginDefinition, String> {
    if let Some(obj) = payload.as_object_mut() {
        obj.insert(
            "plugin_type".to_string(),
            Value::String(plugin_type.to_string()),
        );
    }
    serde_json::from_value::<PluginDefinition>(payload)
        .map_err(|e| format!("Invalid plugin definition: {}", e))
}

/// Reject plugin/service payloads pointing at unreadable local files
/// (baseline: `fail to download file ...: no such file or directory`).
/// Only `file://` URIs and plain local paths are verifiable here; remote
/// URLs pass through untouched.
pub fn reject_missing_plugin_file(payload: &Value) -> Option<Response> {
    let file_uri = payload.get("file").and_then(|v| v.as_str()).unwrap_or("");
    if file_uri.is_empty() {
        return None;
    }
    let is_local = file_uri.starts_with("file://") || !file_uri.contains("://");
    if !is_local {
        return None;
    }
    let raw = file_uri.strip_prefix("file://").unwrap_or(file_uri);
    // Windows drive URIs arrive as file:///C:/... — drop the leading slash
    // so the path resolves; POSIX absolutes (/tmp/...) pass through.
    let raw_bytes = raw.as_bytes();
    let path = if raw_bytes.len() >= 3
        && raw_bytes[0] == b'/'
        && raw_bytes[1].is_ascii_alphabetic()
        && raw_bytes[2] == b':'
    {
        &raw[1..]
    } else {
        raw
    };
    if !std::path::Path::new(path).exists() {
        return Some(
            (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": 1000,
                    "message": format!(
                        "fail to download file {}: stat {}: no such file or directory",
                        file_uri,
                        path
                    )
                })),
            )
                .into_response(),
        );
    }
    None
}

pub async fn create_plugin_of_type(
    state: &AppState,
    plugin_type: &str,
    payload: Value,
) -> Response {
    let name = payload
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if let Some(resp) = reject_missing_plugin_file(&payload) {
        return resp;
    }
    match typed_plugin_payload(plugin_type, payload) {
        Ok(def) => {
            let created = def.name.clone();
            if let Err(e) = state.plugin_manager.register_plugin(def).await {
                return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
            }
            (
                StatusCode::CREATED,
                format!("Plugin {} is created.\n", created),
            )
                .into_response()
        }
        Err(e) => (StatusCode::BAD_REQUEST, e).into_response(),
    }
}

pub async fn update_typed_plugin(
    state: &AppState,
    plugin_type: &str,
    name: &str,
    body: Bytes,
) -> Response {
    if let Err(resp) = check_valid_name(name) {
        return resp;
    }
    let Some(mut def) = state.plugin_manager.get_plugin(name) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": 1000,
                "message": format!("plugin {} is not found", name)
            })),
        )
            .into_response();
    };
    def.plugin_type = plugin_type.to_string();
    if !body.is_empty() {
        if let Ok(val) = serde_json::from_slice::<Value>(&body) {
            if let Some(file_str) = val.get("file").and_then(|v| v.as_str()) {
                def.file = Some(file_str.to_string());
            }
            if let Some(desc_str) = val.get("description").and_then(|v| v.as_str()) {
                def.description = Some(desc_str.to_string());
            }
            if let Some(funcs) = val.get("functions").and_then(|v| v.as_array()) {
                def.functions = funcs
                    .iter()
                    .filter_map(|f| f.as_str().map(|s| s.to_string()))
                    .collect();
            }
        }
    }
    if let Err(e) = state.plugin_manager.register_plugin(def).await {
        return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
    }
    (StatusCode::OK, format!("Plugin {} is updated.\n", name)).into_response()
}

pub fn get_typed_plugin(state: &AppState, plugin_type: &str, name: &str) -> Response {
    if let Err(resp) = check_valid_name(name) {
        return resp;
    }
    match state.plugin_manager.get_plugin(name) {
        Some(def) if def.plugin_type == plugin_type => Json(def).into_response(),
        Some(_) => (
            StatusCode::NOT_FOUND,
            format!("Plugin {} is not a {}", name, plugin_type),
        )
            .into_response(),
        None => {
            if (plugin_type == "source" && is_builtin_source(name))
                || (plugin_type == "sink" && is_builtin_sink(name))
            {
                Json(PluginDefinition {
                    name: name.to_string(),
                    plugin_type: plugin_type.to_string(),
                    file: None,
                    description: Some(format!("Built-in {} plugin", plugin_type)),
                    functions: Vec::new(),
                })
                .into_response()
            } else {
                (StatusCode::NOT_FOUND, format!("Plugin {} not found", name)).into_response()
            }
        }
    }
}

pub async fn delete_typed_plugin(state: &AppState, name: &str) -> Response {
    if let Err(resp) = check_valid_name(name) {
        return resp;
    }
    if state.plugin_manager.get_plugin(name).is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": 1000,
                "message": format!(
                    "fail to delete plugin {}: plugin {} is not found",
                    name, name
                )
            })),
        )
            .into_response();
    }
    let _ = state.plugin_manager.delete_plugin(name).await;
    (StatusCode::OK, format!("Plugin {} is dropped.\n", name)).into_response()
}

pub async fn get_config_uploads() -> impl IntoResponse {
    let upload_dir = std::path::PathBuf::from("data").join("uploads");
    let mut files = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&upload_dir) {
        for entry in entries.flatten() {
            if let Ok(file_type) = entry.file_type() {
                if file_type.is_file() {
                    let path = entry.path();
                    let abs_path = std::fs::canonicalize(&path)
                        .unwrap_or(path)
                        .to_string_lossy()
                        .to_string();
                    files.push(abs_path);
                }
            }
        }
    }
    files.sort();
    (StatusCode::OK, Json(files))
}

pub async fn upload_config_file(State(state): State<AppState>, body: Bytes) -> Response {
    let upload_dir = std::path::PathBuf::from("data").join("uploads");
    if let Err(e) = std::fs::create_dir_all(&upload_dir) {
        return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
    }

    #[derive(Deserialize)]
    struct UploadReq {
        name: Option<String>,
        content: Option<String>,
        file: Option<String>,
    }

    if let Ok(req) = serde_json::from_slice::<UploadReq>(&body) {
        let name = match req.name {
            Some(n) if !n.trim().is_empty() => n,
            _ => return (StatusCode::BAD_REQUEST, "missing file name").into_response(),
        };
        if let Err(resp) = check_valid_name(&name) {
            return resp;
        }

        let bytes_to_write = if let Some(content) = req.content {
            content.into_bytes()
        } else if let Some(file_url) = req.file {
            match state.http_client.get(&file_url).send().await {
                Ok(res) => match res.bytes().await {
                    Ok(b) => b.to_vec(),
                    Err(e) => {
                        return (
                            StatusCode::BAD_REQUEST,
                            format!("Failed to read file from URL: {}", e),
                        )
                            .into_response();
                    }
                },
                Err(e) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        format!("Failed to fetch file URL: {}", e),
                    )
                        .into_response();
                }
            }
        } else {
            return (StatusCode::BAD_REQUEST, "Missing content or file URL").into_response();
        };

        let file_path = upload_dir.join(&name);
        if let Err(e) = std::fs::write(&file_path, bytes_to_write) {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to write file: {}", e),
            )
                .into_response();
        }
        let abs_path = std::fs::canonicalize(&file_path)
            .unwrap_or(file_path)
            .to_string_lossy()
            .to_string();
        return (StatusCode::CREATED, abs_path).into_response();
    }

    (StatusCode::BAD_REQUEST, "invalid upload request body").into_response()
}

pub async fn delete_config_upload(Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let upload_dir = std::path::PathBuf::from("data").join("uploads");
    let file_path = upload_dir.join(&name);
    if file_path.exists() {
        let _ = std::fs::remove_file(&file_path);
    }
    (StatusCode::OK, "ok\n").into_response()
}
