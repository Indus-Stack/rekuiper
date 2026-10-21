use crate::state::{check_valid_name, persist_config_entry, unpersist_config_entry, AppState};
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use rekuiper_conf::KuiperConfig;
use rekuiper_core::KvOperation;
use serde_json::{json, Value};

pub async fn get_configs(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.config.read().clone())
}

/// Partial runtime configuration update (eKuiper `PATCH /configs`): deep
/// merges the JSON object into the live config and answers 204 No Content.
pub async fn patch_configs(State(state): State<AppState>, Json(patch): Json<Value>) -> Response {
    let Value::Object(overlay) = patch else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": 3000, "message": "Expected a JSON object"})),
        )
            .into_response();
    };
    let tz_val = overlay
        .get("timezone")
        .or_else(|| overlay.get("basic").and_then(|b| b.get("timezone")));
    if let Some(Value::String(tz_str)) = tz_val {
        if !rekuiper_sql::is_valid_timezone(tz_str) {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": 3000, "message": "Invalid TZ"})),
            )
                .into_response();
        }
    }
    let mut current = serde_json::to_value(state.config.read().clone()).unwrap_or(json!({}));
    merge_json_object(&mut current, &Value::Object(overlay));
    match serde_json::from_value::<KuiperConfig>(current) {
        Ok(updated) => {
            *state.config.write() = updated;
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": 3000,
                "message": format!("Invalid config patch: {}", e)
            })),
        )
            .into_response(),
    }
}

/// Recursively merges `overlay` objects into `base`; scalars and arrays are
/// replaced, nested objects merge key by key.
pub fn merge_json_object(base: &mut Value, overlay: &Value) {
    match (base, overlay) {
        (Value::Object(base_map), Value::Object(overlay_map)) => {
            for (k, v) in overlay_map {
                match base_map.get_mut(k) {
                    Some(existing) => merge_json_object(existing, v),
                    None => {
                        base_map.insert(k.clone(), v.clone());
                    }
                }
            }
        }
        (base_slot, v) => {
            *base_slot = v.clone();
        }
    }
}
pub async fn save_source_conf_key(
    State(state): State<AppState>,
    Path((name, conf_key)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if let Err(resp) = check_valid_name(&conf_key) {
        return resp;
    }
    let payload = if body.is_empty() {
        json!({})
    } else {
        serde_json::from_slice::<Value>(&body).unwrap_or_else(|_| json!({}))
    };
    if name.eq_ignore_ascii_case("video") {
        if let Ok(cfg) =
            serde_json::from_value::<rekuiper_connectors::VideoSourceConfig>(payload.clone())
        {
            if let Err(e) = cfg.validate_conf() {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({
                        "error": 3000,
                        "message": format!("Invalid video source configuration: {}", e)
                    })),
                )
                    .into_response();
            }
        }
    }
    let key = format!("{}/{}", name, conf_key);
    let _guard = state.config_op_lock.lock().await;
    if let Err(e) = persist_config_entry(&state, "source_configs", &key, &payload).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to persist configuration: {}", e),
        )
            .into_response();
    }
    state.source_configs.write().insert(key, payload);
    (StatusCode::OK, Json(json!({"message": "success"}))).into_response()
}

pub async fn get_source_conf_key(
    State(state): State<AppState>,
    Path((name, conf_key)): Path<(String, String)>,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if let Err(resp) = check_valid_name(&conf_key) {
        return resp;
    }
    let key = format!("{}/{}", name, conf_key);
    if let Some(val) = state.source_configs.read().get(&key).cloned() {
        Json(val).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(json!({
                "message": format!("Configuration key {} for {} not found", conf_key, name)
            })),
        )
            .into_response()
    }
}

pub async fn delete_source_conf_key(
    State(state): State<AppState>,
    Path((name, conf_key)): Path<(String, String)>,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if let Err(resp) = check_valid_name(&conf_key) {
        return resp;
    }
    let key = format!("{}/{}", name, conf_key);
    let _guard = state.config_op_lock.lock().await;
    if !state.source_configs.read().contains_key(&key) {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({
                "message": format!("Configuration key {} for {} not found", conf_key, name)
            })),
        )
            .into_response();
    }
    if let Err(e) = unpersist_config_entry(&state, "source_configs", &key).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to delete configuration: {}", e),
        )
            .into_response();
    }
    state.source_configs.write().remove(&key);
    (StatusCode::OK, Json(json!({"message": "success"}))).into_response()
}

pub async fn save_sink_conf_key(
    State(state): State<AppState>,
    Path((name, conf_key)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if let Err(resp) = check_valid_name(&conf_key) {
        return resp;
    }
    let payload = if body.is_empty() {
        json!({})
    } else {
        serde_json::from_slice::<Value>(&body).unwrap_or_else(|_| json!({}))
    };
    let key = format!("{}/{}", name, conf_key);
    let _guard = state.config_op_lock.lock().await;
    if let Err(e) = persist_config_entry(&state, "sink_configs", &key, &payload).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to persist configuration: {}", e),
        )
            .into_response();
    }
    state.sink_configs.write().insert(key, payload);
    (StatusCode::OK, Json(json!({"message": "success"}))).into_response()
}

pub async fn get_sink_conf_key(
    State(state): State<AppState>,
    Path((name, conf_key)): Path<(String, String)>,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if let Err(resp) = check_valid_name(&conf_key) {
        return resp;
    }
    let key = format!("{}/{}", name, conf_key);
    if let Some(val) = state.sink_configs.read().get(&key).cloned() {
        Json(val).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(json!({
                "message": format!("Configuration key {} for {} not found", conf_key, name)
            })),
        )
            .into_response()
    }
}

pub async fn delete_sink_conf_key(
    State(state): State<AppState>,
    Path((name, conf_key)): Path<(String, String)>,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if let Err(resp) = check_valid_name(&conf_key) {
        return resp;
    }
    let key = format!("{}/{}", name, conf_key);
    let _guard = state.config_op_lock.lock().await;
    if !state.sink_configs.read().contains_key(&key) {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({
                "message": format!("Configuration key {} for {} not found", conf_key, name)
            })),
        )
            .into_response();
    }
    if let Err(e) = unpersist_config_entry(&state, "sink_configs", &key).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to delete configuration: {}", e),
        )
            .into_response();
    }
    state.sink_configs.write().remove(&key);
    (StatusCode::OK, Json(json!({"message": "success"}))).into_response()
}

pub async fn save_connection_conf_key(
    State(state): State<AppState>,
    Path((name, conf_key)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if let Err(resp) = check_valid_name(&conf_key) {
        return resp;
    }
    let mut payload = if body.is_empty() {
        json!({})
    } else {
        serde_json::from_slice::<Value>(&body).unwrap_or_else(|_| json!({}))
    };
    let canonical_key = format!("{}/{}", name, conf_key);
    let dot_key = format!("{}.{}", name, conf_key);
    let _guard = state.config_op_lock.lock().await;

    // Preserve existing secrets when updated with "******" placeholder
    let existing_val = {
        let conns = state.connections.read();
        conns
            .get(&canonical_key)
            .or_else(|| conns.get(&dot_key))
            .cloned()
    };
    if let Some(existing) = existing_val {
        crate::secrets::preserve_masked_secrets(&mut payload, &existing);
    }

    if let Some(kv) = state.kv.as_ref() {
        let ops = [
            KvOperation::Set {
                namespace: "connections".to_string(),
                key: canonical_key.clone(),
                val: payload.to_string(),
            },
            KvOperation::Delete {
                namespace: "connections".to_string(),
                key: dot_key.clone(),
            },
        ];
        if let Err(e) = kv.apply_transaction(&ops).await {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to persist connection: {}", e),
            )
                .into_response();
        }
    }

    let mut conns = state.connections.write();
    conns.remove(&dot_key);
    conns.insert(canonical_key, payload);
    (StatusCode::OK, Json(json!({"message": "success"}))).into_response()
}

pub async fn get_connection_conf_key(
    State(state): State<AppState>,
    Path((name, conf_key)): Path<(String, String)>,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if let Err(resp) = check_valid_name(&conf_key) {
        return resp;
    }
    let canonical_key = format!("{}/{}", name, conf_key);
    let dot_key = format!("{}.{}", name, conf_key);

    let mem_val = {
        let conns = state.connections.read();
        conns
            .get(&canonical_key)
            .or_else(|| conns.get(&dot_key))
            .cloned()
    };
    if let Some(mut val) = mem_val {
        crate::secrets::mask_secrets(&mut val);
        return Json(val).into_response();
    }

    if let Some(kv) = state.kv.as_ref() {
        match kv.get("connections", &canonical_key).await {
            Ok(Some(raw)) => {
                if let Ok(mut val) = serde_json::from_str::<Value>(&raw) {
                    state.connections.write().insert(canonical_key, val.clone());
                    crate::secrets::mask_secrets(&mut val);
                    return Json(val).into_response();
                }
            }
            Ok(None) => {}
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Storage read error: {}", e),
                )
                    .into_response();
            }
        }
        match kv.get("connections", &dot_key).await {
            Ok(Some(raw)) => {
                if let Ok(mut val) = serde_json::from_str::<Value>(&raw) {
                    state.connections.write().insert(dot_key, val.clone());
                    crate::secrets::mask_secrets(&mut val);
                    return Json(val).into_response();
                }
            }
            Ok(None) => {}
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Storage read error: {}", e),
                )
                    .into_response();
            }
        }
    }

    (
        StatusCode::NOT_FOUND,
        Json(json!({
            "message": format!("Connection conf_key {} for {} not found", conf_key, name)
        })),
    )
        .into_response()
}

pub async fn delete_connection_conf_key(
    State(state): State<AppState>,
    Path((name, conf_key)): Path<(String, String)>,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if let Err(resp) = check_valid_name(&conf_key) {
        return resp;
    }
    let canonical_key = format!("{}/{}", name, conf_key);
    let dot_key = format!("{}.{}", name, conf_key);
    let _guard = state.config_op_lock.lock().await;

    let exists = {
        let conns = state.connections.read();
        conns.contains_key(&canonical_key) || conns.contains_key(&dot_key)
    };
    if !exists {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({
                "message": format!("Connection conf_key {} for {} not found", conf_key, name)
            })),
        )
            .into_response();
    }

    if let Some(kv) = state.kv.as_ref() {
        let ops = [
            KvOperation::Delete {
                namespace: "connections".to_string(),
                key: canonical_key.clone(),
            },
            KvOperation::Delete {
                namespace: "connections".to_string(),
                key: dot_key.clone(),
            },
        ];
        if let Err(e) = kv.apply_transaction(&ops).await {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to delete connection: {}", e),
            )
                .into_response();
        }
    }

    let mut conns = state.connections.write();
    conns.remove(&canonical_key);
    conns.remove(&dot_key);
    (StatusCode::OK, Json(json!({"message": "success"}))).into_response()
}

pub async fn register_source_connection(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let payload = if body.is_empty() {
        json!({ "id": name })
    } else {
        serde_json::from_slice::<Value>(&body).unwrap_or_else(|_| json!({ "id": name }))
    };
    let id = payload
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or(&name)
        .to_string();
    let _guard = state.config_op_lock.lock().await;
    if let Err(e) = persist_config_entry(&state, "connections", &id, &payload).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to persist connection: {}", e),
        )
            .into_response();
    }
    state.connections.write().insert(id, payload);
    (StatusCode::OK, Json(json!({"message": "success"}))).into_response()
}

pub async fn register_sink_connection(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let payload = if body.is_empty() {
        json!({ "id": name })
    } else {
        serde_json::from_slice::<Value>(&body).unwrap_or_else(|_| json!({ "id": name }))
    };
    let id = payload
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or(&name)
        .to_string();
    let _guard = state.config_op_lock.lock().await;
    if let Err(e) = persist_config_entry(&state, "connections", &id, &payload).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to persist connection: {}", e),
        )
            .into_response();
    }
    state.connections.write().insert(id, payload);
    (StatusCode::OK, Json(json!({"message": "success"}))).into_response()
}

pub async fn register_lookup_connection(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let payload = if body.is_empty() {
        json!({ "id": name })
    } else {
        serde_json::from_slice::<Value>(&body).unwrap_or_else(|_| json!({ "id": name }))
    };
    let id = payload
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or(&name)
        .to_string();
    let _guard = state.config_op_lock.lock().await;
    if let Err(e) = persist_config_entry(&state, "connections", &id, &payload).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to persist connection: {}", e),
        )
            .into_response();
    }
    state.connections.write().insert(id, payload);
    (StatusCode::OK, Json(json!({"message": "success"}))).into_response()
}

pub async fn list_connections(State(state): State<AppState>) -> impl IntoResponse {
    let mut conns: Vec<Value> = state.connections.read().values().cloned().collect();
    for c in &mut conns {
        crate::secrets::mask_secrets(c);
    }
    Json(conns)
}

pub async fn create_connection(
    State(state): State<AppState>,
    Json(mut payload): Json<Value>,
) -> Response {
    let id = payload
        .get("id")
        .and_then(|v| v.as_str())
        .or_else(|| payload.get("name").and_then(|v| v.as_str()))
        .unwrap_or("")
        .to_string();
    if id.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"message": "Missing connection id"})),
        )
            .into_response();
    }
    let canonical = id.replace('.', "/");
    let dot = id.replace('/', ".");
    {
        let conns = state.connections.read();
        if conns.contains_key(&id) || conns.contains_key(&canonical) || conns.contains_key(&dot) {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "message": format!("connection {} already been created", id)
                })),
            )
                .into_response();
        }
    }
    crate::secrets::interpolate_secrets(&mut payload, state.kv.as_deref());
    let _guard = state.config_op_lock.lock().await;
    if let Err(e) = persist_config_entry(&state, "connections", &id, &payload).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "message": format!("Failed to persist connection: {}", e)
            })),
        )
            .into_response();
    }
    state.connections.write().insert(id.clone(), payload);
    (StatusCode::CREATED, "success").into_response()
}

pub async fn get_connection(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    let canonical = id.replace('.', "/");
    let dot = id.replace('/', ".");
    let mem_val = {
        let conns = state.connections.read();
        conns
            .get(&id)
            .or_else(|| conns.get(&canonical))
            .or_else(|| conns.get(&dot))
            .cloned()
    };
    if let Some(mut conn) = mem_val {
        crate::secrets::mask_secrets(&mut conn);
        return Json(conn).into_response();
    }

    if let Some(kv) = state.kv.as_ref() {
        for candidate in [&id, &canonical, &dot] {
            match kv.get("connections", candidate).await {
                Ok(Some(raw)) => {
                    if let Ok(mut val) = serde_json::from_str::<Value>(&raw) {
                        state
                            .connections
                            .write()
                            .insert(candidate.to_string(), val.clone());
                        crate::secrets::mask_secrets(&mut val);
                        return Json(val).into_response();
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({
                            "message": format!("Storage read error: {}", e)
                        })),
                    )
                        .into_response();
                }
            }
        }
    }

    (
        StatusCode::NOT_FOUND,
        Json(json!({
            "message": format!("Connection {} not found", id)
        })),
    )
        .into_response()
}

pub async fn delete_connection(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    let _guard = state.config_op_lock.lock().await;
    let canonical = id.replace('.', "/");
    let dot = id.replace('/', ".");
    let exists = {
        let conns = state.connections.read();
        conns.contains_key(&id) || conns.contains_key(&canonical) || conns.contains_key(&dot)
    };
    if !exists {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({
                "message": format!("Connection {} not found", id)
            })),
        )
            .into_response();
    }

    if let Some(kv) = state.kv.as_ref() {
        let mut ops = vec![KvOperation::Delete {
            namespace: "connections".to_string(),
            key: id.clone(),
        }];
        if canonical != id {
            ops.push(KvOperation::Delete {
                namespace: "connections".to_string(),
                key: canonical.clone(),
            });
        }
        if dot != id && dot != canonical {
            ops.push(KvOperation::Delete {
                namespace: "connections".to_string(),
                key: dot.clone(),
            });
        }
        if let Err(e) = kv.apply_transaction(&ops).await {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to delete connection: {}", e),
            )
                .into_response();
        }
    }

    let mut conns = state.connections.write();
    conns.remove(&id);
    conns.remove(&canonical);
    conns.remove(&dot);
    (StatusCode::OK, "success").into_response()
}

/// Update-or-insert connection properties (eKuiper `PUT /connections/:id`):
/// merges the JSON body into the stored entry and returns it for readback.
pub async fn update_connection(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(mut payload): Json<Value>,
) -> Response {
    let _guard = state.config_op_lock.lock().await;
    let mut stored = state
        .connections
        .read()
        .get(&id)
        .cloned()
        .unwrap_or_else(|| json!({"id": id}));
    crate::secrets::preserve_masked_secrets(&mut payload, &stored);
    crate::secrets::interpolate_secrets(&mut payload, state.kv.as_deref());
    merge_json_object(&mut stored, &payload);
    if let Some(obj) = stored.as_object_mut() {
        obj.insert("id".to_string(), Value::String(id.clone()));
    }
    if let Err(e) = persist_config_entry(&state, "connections", &id, &stored).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to persist connection: {}", e),
        )
            .into_response();
    }
    state.connections.write().insert(id, stored.clone());
    let mut masked = stored;
    crate::secrets::mask_secrets(&mut masked);
    Json(masked).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rekuiper_core::{RuleManager, StreamBus, StreamManager, TableManager};

    #[tokio::test]
    async fn test_save_source_conf_key_rejects_disallowed_video_url() {
        let bus = StreamBus::new();
        let state = AppState::new(
            "test".to_string(),
            rekuiper_conf::KuiperConfig::default(),
            StreamManager::new(),
            TableManager::new(),
            RuleManager::new(bus.clone()),
            bus,
        );
        let payload = json!({
            "url": "file:///etc/shadow"
        });
        let body = Bytes::from(payload.to_string());
        let resp = save_source_conf_key(
            State(state),
            Path(("video".to_string(), "bad_conf".to_string())),
            body,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_save_source_conf_key_accepts_valid_video_conf() {
        let bus = StreamBus::new();
        let state = AppState::new(
            "test".to_string(),
            rekuiper_conf::KuiperConfig::default(),
            StreamManager::new(),
            TableManager::new(),
            RuleManager::new(bus.clone()),
            bus,
        );
        let payload = json!({
            "interval": 500,
            "url": "rtsp://camera.internal:554/live"
        });
        let body = Bytes::from(payload.to_string());
        let resp = save_source_conf_key(
            State(state),
            Path(("video".to_string(), "good_conf".to_string())),
            body,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
