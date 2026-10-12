use crate::engine::clean_sink_value;
use crate::state::{AppState, RuletestReplay, RuletestSession, RULETEST_HISTORY_CAP};
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    routing::get,
    Json,
};
use parking_lot::RwLock;
use rekuiper_connectors::{parse_interval_ms, SimulatorConfig};
use rekuiper_core::model::StreamRecord;
use rekuiper_sql::{Evaluator, Parser, RuleState};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::broadcast;

// ---------------------------------------------------------------------------
// Interactive rule simulation (ruletest) with SSE streaming output.
// ---------------------------------------------------------------------------

/// Payload for `POST /ruletest`. All fields are optional so that probes
/// without a body still receive a usable session.
#[derive(Debug, Default, Deserialize)]
pub struct CreateRuletestPayload {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    sql: Option<String>,
    #[serde(default, rename = "mockSource")]
    mock_source: HashMap<String, SimulatorConfig>,
}

pub fn generate_ruletest_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("ruletest-{}-{}", std::process::id(), nanos)
}

pub async fn create_ruletest(State(state): State<AppState>, body: Bytes) -> Response {
    let payload: CreateRuletestPayload = if body.is_empty() {
        CreateRuletestPayload::default()
    } else {
        match serde_json::from_slice(&body) {
            Ok(p) => p,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    format!("Invalid ruletest payload: {}", e),
                )
                    .into_response();
            }
        }
    };
    // A simulation without a parseable SELECT statement is rejected, like
    // baseline eKuiper.
    let sql = payload.sql.clone().unwrap_or_default();
    if sql.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": 1000,
                "message": "fail to run rule: SQL is not a select statement."
            })),
        )
            .into_response();
    }
    if Parser::new(&sql).parse_select().is_err() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": 1000,
                "message": "fail to run rule: SQL is not a select statement."
            })),
        )
            .into_response();
    }
    // The SSE feed serves on the documented `httpServerPort` (default
    // 10081): a shared listener is bound at daemon startup (see
    // `test_sse_router`), so the reported port is stable and live from the
    // moment the session is created.
    let port = state.config.read().basic.http_server_port;
    let id = payload.id.unwrap_or_else(generate_ruletest_id);
    let (output_tx, _) = tokio::sync::broadcast::channel::<String>(256);
    let shutdown = Arc::new(tokio::sync::Notify::new());
    state.ruletests.write().insert(
        id.clone(),
        RuletestSession {
            id: id.clone(),
            sql,
            mock_source: payload.mock_source,
            output_tx,
            replay: Arc::new(RwLock::new(RuletestReplay::default())),
            port,
            shutdown,
        },
    );
    (StatusCode::OK, Json(json!({ "id": id, "port": port }))).into_response()
}

pub async fn start_ruletest(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    let Some(session) = state.ruletests.read().get(&name).cloned() else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": 1000, "message": format!("test rule {} not found", name)})),
        )
            .into_response();
    };
    // Paced replay of the mock source: rows stream at the configured
    // interval, looping until the session is deleted (or a 10-minute
    // session cap, mirroring baseline trial-run expiry). Every row is
    // buffered in session history so late SSE subscribers lose nothing.
    tokio::spawn(async move {
        let mut parser = Parser::new(&session.sql);
        let Ok(select_stmt) = parser.parse_select() else {
            return;
        };
        let conf = session.mock_source.get(&select_stmt.from).cloned();
        let data: Vec<HashMap<String, Value>> =
            conf.as_ref().map(|c| c.data.clone()).unwrap_or_default();
        if data.is_empty() {
            // No mock rows: fall back to a single empty trigger row so the
            // rule still evaluates once (baseline runs the real source).
            let rule_state = RuleState::default();
            for row in
                Evaluator::eval_select_stateful_multi(&select_stmt, &HashMap::new(), &rule_state)
            {
                emit_ruletest_line(&session, &row);
            }
            return;
        }
        let interval = conf
            .as_ref()
            .map(|c| parse_interval_ms(&c.interval))
            .unwrap_or_else(|| std::time::Duration::from_millis(10));
        let loop_data = conf.as_ref().is_some_and(|c| c.loop_data);
        let rule_state = RuleState::default();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10 * 60);
        loop {
            for record in &data {
                if tokio::time::Instant::now() >= deadline {
                    return;
                }
                for row in Evaluator::eval_select_stateful_multi(&select_stmt, record, &rule_state)
                {
                    emit_ruletest_line(&session, &row);
                }
                tokio::select! {
                    _ = session.shutdown.notified() => return,
                    _ = tokio::time::sleep(interval) => {}
                }
            }
            if !loop_data {
                return;
            }
        }
    });
    (StatusCode::OK, "started\n").into_response()
}

/// Buffer one replayed row into the bounded session ring (evicting the
/// oldest past the cap) and wake SSE subscribers via broadcast.
pub fn emit_ruletest_line(session: &RuletestSession, row: &HashMap<String, Value>) {
    let mut map = std::collections::BTreeMap::new();
    for (k, v) in row {
        if k == rekuiper_sql::eval::META_KEY || k.starts_with("__") {
            continue;
        }
        if v.is_null() {
            continue;
        }
        map.insert(k.clone(), clean_sink_value(v, false));
    }
    let line = serde_json::to_string(&map).unwrap_or_default();
    {
        let mut replay = session.replay.write();
        let seq = replay.next_seq;
        replay.next_seq = seq.saturating_add(1);
        replay.entries.push_back((seq, line.clone()));
        while replay.entries.len() > RULETEST_HISTORY_CAP {
            replay.entries.pop_front();
        }
    }
    let _ = session.output_tx.send(line);
}

pub async fn delete_ruletest(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    // Stopping the replay loop alongside the session.
    if let Some(session) = state.ruletests.write().remove(&name) {
        session.shutdown.notify_waiters();
    }
    (StatusCode::OK, "dropped\n").into_response()
}

/// Documented SSE feed: `GET /test/:id` streams the session replay as
/// `text/event-stream` — retained rows first (late subscribers backfill up
/// to the retention cap, then go live), then live rows indefinitely.
/// Served both on the main REST router and on the dedicated `httpServerPort`
/// listener (see [`test_sse_router`]).
pub async fn sse_ruletest(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    let Some(session) = state.ruletests.read().get(&name).cloned() else {
        return (
            StatusCode::NOT_FOUND,
            format!("Ruletest {} not found", name),
        )
            .into_response();
    };
    let rx = session.output_tx.subscribe();
    let replay = session.replay.clone();
    // Cursor resume over the sequence-numbered ring: the replay buffer is
    // the source of truth and broadcast messages are only wake-ups. A
    // subscriber sends every row newer than its cursor, in order; a cursor
    // older than the retained prefix skips the evicted gap (documented lag)
    // and resumes at the oldest retained row. Session deletion drops the
    // broadcast sender, which terminates the stream.
    struct SseCursor {
        rx: broadcast::Receiver<String>,
        replay: Arc<RwLock<RuletestReplay>>,
        cursor: u64,
        pending: VecDeque<String>,
    }
    let stream = futures::stream::unfold(
        SseCursor {
            rx,
            replay,
            cursor: 0,
            pending: VecDeque::new(),
        },
        |mut st| async move {
            loop {
                if let Some(line) = st.pending.pop_front() {
                    return Some((Ok::<_, axum::Error>(Event::default().data(line)), st));
                }
                // Single atomic snapshot: copy every row at/after the cursor
                // AND derive the next cursor from the last row actually
                // copied, under the same lock. Rows appended concurrently
                // after the snapshot stay above the cursor and are picked up
                // on the next pass — never skipped, never duplicated.
                // A cursor older than the retained prefix resumes at the
                // oldest retained row (documented lag gap).
                let (fresh, next): (Vec<String>, u64) = {
                    let guard = st.replay.read();
                    let mut fresh = Vec::new();
                    let mut next = st.cursor;
                    for (seq, line) in guard.entries.iter() {
                        if *seq >= st.cursor {
                            fresh.push(line.clone());
                            next = seq.saturating_add(1);
                        }
                    }
                    (fresh, next)
                };
                if !fresh.is_empty() {
                    st.cursor = next;
                    st.pending = fresh.into();
                    continue;
                }
                match st.rx.recv().await {
                    // A new row was buffered; refill from the ring.
                    Ok(_) => continue,
                    // Overflow drops broadcast copies only; the ring stays
                    // complete, so keep refilling from the cursor.
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        },
    );
    Sse::new(stream)
        .keep_alive(KeepAlive::new())
        .into_response()
}

/// Ingest handler for HTTP Push sources (`TYPE="httppush"`).
/// Directly accepts JSON object payloads or JSON arrays of objects pushed by HTTP clients
/// to configured endpoints (e.g. `POST :10081/xp/push`).
pub async fn http_data_push_handler(
    State(state): State<AppState>,
    req: axum::extract::Request,
) -> Response {
    let method = req.method().to_string();
    let raw_path = req.uri().path().to_string();
    let normalized_path = if raw_path.len() > 1 && raw_path.ends_with('/') {
        raw_path.trim_end_matches('/').to_string()
    } else {
        raw_path.clone()
    };

    let endpoint = {
        let guard = state.http_push_endpoints.read();
        guard
            .get(&normalized_path)
            .or_else(|| guard.get(&raw_path))
            .cloned()
    };

    let Some(endpoint) = endpoint else {
        return (StatusCode::NOT_FOUND, "Endpoint not found\n").into_response();
    };

    if !method.eq_ignore_ascii_case(&endpoint.method) {
        return (
            StatusCode::METHOD_NOT_ALLOWED,
            format!(
                "Method {} not allowed, expect {}\n",
                method, endpoint.method
            ),
        )
            .into_response();
    }

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

    let is_binary = endpoint.format.eq_ignore_ascii_case("binary");
    let mut records: Vec<StreamRecord> = if is_binary {
        use base64::Engine;
        let str_val = match std::str::from_utf8(&body_bytes) {
            Ok(s) => s.to_string(),
            Err(_) => base64::engine::general_purpose::STANDARD.encode(&body_bytes),
        };
        let mut map = HashMap::new();
        map.insert("self".to_string(), Value::String(str_val));
        vec![StreamRecord::new(map)]
    } else {
        match serde_json::from_slice::<Value>(&body_bytes) {
            Ok(Value::Object(map)) => {
                vec![StreamRecord::new(map.into_iter().collect())]
            }
            Ok(Value::Array(items)) => {
                let mut recs = Vec::with_capacity(items.len());
                for item in items {
                    if let Value::Object(map) = item {
                        recs.push(StreamRecord::new(map.into_iter().collect()));
                    }
                }
                recs
            }
            Ok(primitive) => {
                let mut map = HashMap::new();
                map.insert("self".to_string(), primitive);
                vec![StreamRecord::new(map)]
            }
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    format!("Fail to decode data: {}\n", e),
                )
                    .into_response();
            }
        }
    };

    if let Some(stream_def) = state.stream_manager.get_stream(&endpoint.stream_name) {
        if !stream_def.stream_fields.is_empty() {
            for record in &mut records {
                rekuiper_core::model::enforce_stream_schema(
                    &mut record.data,
                    &stream_def.stream_fields,
                );
            }
        }
    }

    if records.is_empty() {
        return (StatusCode::OK, "ok").into_response();
    }

    let sender = state.stream_bus.get_or_create(&endpoint.stream_name);
    let _ = sender.send_batch(records).await;

    (StatusCode::OK, "ok").into_response()
}

pub async fn create_router_fallback(
    State(state): State<AppState>,
    req: axum::extract::Request,
) -> Response {
    let raw_path = req.uri().path().to_string();
    let normalized_path = if raw_path.len() > 1 && raw_path.ends_with('/') {
        raw_path.trim_end_matches('/').to_string()
    } else {
        raw_path.clone()
    };
    let has_endpoint = {
        let guard = state.http_push_endpoints.read();
        guard.contains_key(&normalized_path) || guard.contains_key(&raw_path)
    };
    if has_endpoint {
        http_data_push_handler(State(state), req).await
    } else {
        (StatusCode::NOT_FOUND, ()).into_response()
    }
}

/// Dedicated HTTP data server listener serving the documented
/// `http://<httpServerIp>:<httpServerPort>/test/:id` endpoint as well as
/// registered HTTP push source endpoints (e.g. `DATASOURCE="/xp/push"`).
pub fn test_sse_router(state: AppState) -> axum::Router {
    axum::Router::new()
        .route("/test/:name", get(sse_ruletest))
        .fallback(http_data_push_handler)
        .with_state(state)
}
