use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use std::collections::HashSet;
use std::time::Duration;
use tokio::time::interval;

use crate::comms::CommsEvent;
use crate::state::AppState;

#[derive(Debug, Default, Deserialize)]
pub struct TapScopeQuery {
    pub session_id: Option<String>,
}

pub async fn enable_rule_tap(
    State(state): State<AppState>,
    Path((rule_id, node_id)): Path<(String, String)>,
    query: Option<Query<TapScopeQuery>>,
) -> Response {
    let session_id = query.and_then(|q| q.0.session_id);
    if let Some(ref sid) = session_id {
        state
            .comms_hub
            .enable_tap_for_session(sid, &rule_id, &node_id);
    } else {
        state.comms_hub.enable_tap(&rule_id, &node_id);
    }
    (
        StatusCode::OK,
        Json(json!({
            "status": "enabled",
            "rule": rule_id,
            "node": node_id,
            "session_id": session_id
        })),
    )
        .into_response()
}

pub async fn disable_rule_tap(
    State(state): State<AppState>,
    Path((rule_id, node_id)): Path<(String, String)>,
    query: Option<Query<TapScopeQuery>>,
) -> Response {
    let session_id = query.and_then(|q| q.0.session_id);
    if let Some(ref sid) = session_id {
        state
            .comms_hub
            .disable_tap_for_session(sid, &rule_id, &node_id);
    } else {
        state.comms_hub.disable_tap(&rule_id, &node_id);
    }
    (
        StatusCode::OK,
        Json(json!({
            "status": "disabled",
            "rule": rule_id,
            "node": node_id,
            "session_id": session_id
        })),
    )
        .into_response()
}

pub async fn get_rule_tap_status(
    State(state): State<AppState>,
    Path((rule_id, node_id)): Path<(String, String)>,
) -> Response {
    let active = state.comms_hub.is_tap_active(&rule_id, &node_id);
    (
        StatusCode::OK,
        Json(json!({
            "active": active,
            "rule": rule_id,
            "node": node_id
        })),
    )
        .into_response()
}

pub async fn comms_websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> Response {
    ws.on_upgrade(move |socket| handle_comms_socket(socket, state))
}

async fn handle_comms_socket(socket: WebSocket, state: AppState) {
    let session_id = uuid::Uuid::new_v4().to_string();
    let mut subscribed_rules: HashSet<String> = HashSet::new();
    let (mut sender, mut receiver) = socket.split();
    let mut comms_rx = state.comms_hub.subscribe();
    let mut ticker = interval(Duration::from_secs(15));
    // Consume immediate first tick
    ticker.tick().await;

    loop {
        tokio::select! {
            event = comms_rx.recv() => {
                match event {
                    Ok(event) => {
                        if !subscribed_rules.is_empty() {
                            if let CommsEvent::TupleSample { ref rule_id, .. } = event {
                                if !subscribed_rules.contains(rule_id) {
                                    continue;
                                }
                            }
                        }
                        if let Ok(msg_text) = serde_json::to_string(&event) {
                            if sender.send(Message::Text(msg_text)).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::debug!("CommsHub client lagged by {} messages", skipped);
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
            _ = ticker.tick() => {
                let hb = CommsEvent::Heartbeat {
                    timestamp: chrono::Utc::now().timestamp_millis(),
                    active_taps: state.comms_hub.active_tap_count(),
                };
                if let Ok(msg_text) = serde_json::to_string(&hb) {
                    if sender.send(Message::Text(msg_text)).await.is_err() {
                        break;
                    }
                }
            }
            msg = receiver.next() => {
                let Some(msg) = msg else {
                    break;
                };
                let Ok(msg) = msg else {
                    break;
                };
                match msg {
                    Message::Ping(payload) => {
                        if sender.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Message::Close(_) => {
                        break;
                    }
                    Message::Text(text) => {
                        if let Ok(cmd) = serde_json::from_str::<serde_json::Value>(&text) {
                            let action = cmd.get("action").and_then(|v| v.as_str());
                            if let Some("subscribe") = action {
                                if let Some(rules) = cmd.get("rules").and_then(|v| v.as_array()) {
                                    for r in rules.iter().filter_map(|v| v.as_str()) {
                                        subscribed_rules.insert(r.to_string());
                                    }
                                }
                            } else if let Some("unsubscribe") = action {
                                if let Some(rules) = cmd.get("rules").and_then(|v| v.as_array()) {
                                    for r in rules.iter().filter_map(|v| v.as_str()) {
                                        subscribed_rules.remove(r);
                                    }
                                }
                            } else if let (Some(action), Some(rule), Some(node)) = (
                                action,
                                cmd.get("rule").and_then(|v| v.as_str()),
                                cmd.get("node").and_then(|v| v.as_str()),
                            ) {
                                match action {
                                    "enable" | "tap_enable" => {
                                        state.comms_hub.enable_tap_for_session(
                                            &session_id,
                                            rule,
                                            node,
                                        );
                                    }
                                    "disable" | "tap_disable" => {
                                        state.comms_hub.disable_tap_for_session(
                                            &session_id,
                                            rule,
                                            node,
                                        );
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    state.comms_hub.release_session(&session_id);
}
