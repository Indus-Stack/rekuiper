use axum::body::Body;
use axum::http::Request;
use futures::io::{AsyncReadExt, AsyncWriteExt};
use rekuiper_conf::KuiperConfig;
use rekuiper_core::kv::MemKvStore;
use rekuiper_core::model::DeclarativeManifest;
use rekuiper_core::{RuleManager, StreamBus, StreamManager, TableManager};
use rekuiper_server::agent::{
    read_frame, send_frame, AgentControlCommand, AgentHeartbeat, TokioIoCompat,
};
use rekuiper_server::comms::CommsEvent;
use rekuiper_server::declarative::{
    capture_current_manifest, reconcile_manifest, reconcile_manifest_with_watchdog,
    store_known_good_snapshot,
};
use rekuiper_server::routes::{create_router, AppState};
use rekuiper_server::secrets::{
    interpolate_secrets, interpolate_string, mask_secrets, preserve_masked_secrets,
};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tower::ServiceExt;

fn build_test_state() -> AppState {
    let config = KuiperConfig::default();
    let stream_bus = StreamBus::new();
    let stream_manager = StreamManager::new();
    let table_manager = TableManager::new();
    let rule_manager = RuleManager::new(stream_bus.clone());

    let mut state = AppState::new(
        "test_phase3".to_string(),
        config,
        stream_manager,
        table_manager,
        rule_manager,
        stream_bus,
    );
    let kv = Arc::new(MemKvStore::new());
    state.kv = Some(kv);
    state
}

// ---------------------------------------------------------------------------
// 3.1 & 3.2 Yamux Stream 0 (Heartbeat/Commands) + Streams 1..N (HTTP Proxy)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_yamux_agent_heartbeat_and_http_tunneling() {
    let state = build_test_state();
    let router = create_router(state.clone());

    // Create an in-memory duplex connection
    let (client_raw, server_raw) = tokio::io::duplex(65536);
    let client_io = TokioIoCompat::new(client_raw);
    let server_io = TokioIoCompat::new(server_raw);

    let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);

    // Spawn agent client on duplex end A
    let client_task = tokio::spawn(rekuiper_server::agent::run_yamux_agent(
        client_io,
        yamux::Mode::Client,
        state.clone(),
        router,
        "agent-edge-99".to_string(),
        Duration::from_millis(100),
        cancel_rx,
    ));

    // Server-side Yamux multiplexer on duplex end B
    let mut cfg = yamux::Config::default();
    cfg.set_read_after_close(true);
    let mut server_conn = yamux::Connection::new(server_io, cfg, yamux::Mode::Server);

    enum ServerCmd {
        OpenOutbound(tokio::sync::oneshot::Sender<Result<yamux::Stream, yamux::ConnectionError>>),
    }

    let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::channel::<ServerCmd>(16);
    let (inbound_tx, mut inbound_rx) = tokio::sync::mpsc::channel(16);

    // Server connection driver task: drives poll_next_inbound and services OpenOutbound requests
    let driver_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                inbound_res = futures::future::poll_fn(|cx| server_conn.poll_next_inbound(cx)) => {
                    match inbound_res {
                        Some(Ok(stream)) => {
                            let _ = inbound_tx.send(stream).await;
                        }
                        Some(Err(_)) | None => break,
                    }
                }
                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(ServerCmd::OpenOutbound(reply)) => {
                            let res = futures::future::poll_fn(|cx| server_conn.poll_new_outbound(cx)).await;
                            let _ = reply.send(res);
                        }
                        None => break,
                    }
                }
            }
        }
    });

    // Stream 0: Inbound stream accepted by server (initiated by client)
    let mut stream0 = inbound_rx
        .recv()
        .await
        .expect("Stream 0 must arrive from client");

    // Server reads Heartbeat from Stream 0
    let hb_bytes = read_frame(&mut stream0)
        .await
        .expect("Must receive heartbeat frame");
    let hb: AgentHeartbeat =
        serde_json::from_slice(&hb_bytes).expect("Heartbeat must parse as AgentHeartbeat");
    assert_eq!(hb.agent_id, "agent-edge-99");
    assert_eq!(hb.rules_count, 0);
    assert_eq!(hb.running_rules, 0);

    // Server sends Ping control command on Stream 0
    let ping_cmd = serde_json::to_vec(&AgentControlCommand::Ping).unwrap();
    send_frame(&mut stream0, &ping_cmd)
        .await
        .expect("Must send Ping command");

    // Server reads Pong response from Stream 0 (skipping any interleaved periodic heartbeat frames)
    let pong_val: Value = loop {
        let frame_bytes = read_frame(&mut stream0)
            .await
            .expect("Must receive Pong frame");
        let val: Value = serde_json::from_slice(&frame_bytes).expect("Must parse frame JSON");
        if val.get("agent_id").is_some() {
            continue;
        }
        break val;
    };
    assert_eq!(
        pong_val.get("status").and_then(|v| v.as_str()),
        Some("pong")
    );

    // Server opens Stream 1 to tunnel raw HTTP GET /rules into Agent router
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    cmd_tx
        .send(ServerCmd::OpenOutbound(reply_tx))
        .await
        .unwrap();
    let mut stream1 = reply_rx
        .await
        .unwrap()
        .expect("Open stream 1 should succeed");

    stream1
        .write_all(b"GET /rules HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    stream1.flush().await.unwrap();

    let mut resp_buf = vec![0u8; 4096];
    let n = stream1.read(&mut resp_buf).await.unwrap();
    let resp_str = String::from_utf8_lossy(&resp_buf[..n]);
    assert!(
        resp_str.starts_with("HTTP/1.1 200 OK"),
        "Unexpected response: {}",
        resp_str
    );
    assert!(resp_str.contains("[]"));

    // Server opens Stream 2 to tunnel raw HTTP GET /api/v1/palette into Agent router
    let (reply_tx2, reply_rx2) = tokio::sync::oneshot::channel();
    cmd_tx
        .send(ServerCmd::OpenOutbound(reply_tx2))
        .await
        .unwrap();
    let mut stream2 = reply_rx2
        .await
        .unwrap()
        .expect("Open stream 2 should succeed");

    stream2
        .write_all(b"GET /api/v1/palette HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    stream2.flush().await.unwrap();

    let mut resp_buf2 = vec![0u8; 16384];
    let n2 = stream2.read(&mut resp_buf2).await.unwrap();
    let resp_str2 = String::from_utf8_lossy(&resp_buf2[..n2]);
    assert!(
        resp_str2.starts_with("HTTP/1.1 200 OK"),
        "Unexpected response: {}",
        resp_str2
    );
    assert!(resp_str2.contains("influx"));
    assert!(resp_str2.contains("tdengine"));
    assert!(resp_str2.contains("modbus"));
    assert!(resp_str2.contains("opcua"));
    assert!(resp_str2.contains("kafka"));

    // Clean up
    let _ = cancel_tx.send(true);
    drop(cmd_tx);
    let _ = client_task.await;
    let _ = driver_handle.await;
}

// ---------------------------------------------------------------------------
// 3.3 Transactional Snapshot Watchdog & Rollback on DLQ Spike
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_snapshot_watchdog_auto_rollback_on_dlq_spike() {
    let state = build_test_state();
    let mut comms_rx = state.comms_hub.subscribe();

    // 1. Initial known-good manifest
    let yaml_init = r#"
version: v1
streams:
  stable_stream: "CREATE STREAM stable_stream () WITH (FORMAT=\"JSON\", TYPE=\"mqtt\", TOPIC=\"stable_topic\");"
rules:
  - id: rule_stable
    sql: "SELECT * FROM stable_stream"
    actions:
      - log: {}
"#;
    let manifest_init = DeclarativeManifest::from_str(yaml_init, "yaml", false)
        .expect("Initial manifest should parse");

    reconcile_manifest(&state, &manifest_init)
        .await
        .expect("Initial reconciliation should succeed");

    assert_eq!(state.rule_manager.list_rules().len(), 1);
    assert!(state.rule_manager.get_rule("rule_stable").is_some());

    // Snapshot is captured as known-good in KV
    let initial_snapshot = capture_current_manifest(&state);
    store_known_good_snapshot(&state, &initial_snapshot)
        .await
        .expect("Store known good should succeed");

    // 2. Reconcile a new manifest with poison_stream and rule_poison (configured with onError: dlq)
    let yaml_bad = r#"
version: v1
streams:
  stable_stream: "CREATE STREAM stable_stream () WITH (FORMAT=\"JSON\", TYPE=\"mqtt\", TOPIC=\"stable_topic\");"
  poison_stream: "CREATE STREAM poison_stream () WITH (FORMAT=\"JSON\", TYPE=\"mqtt\", TOPIC=\"poison_topic\");"
rules:
  - id: rule_stable
    sql: "SELECT * FROM stable_stream"
    actions:
      - log: {}
  - id: rule_poison
    sql: "SELECT * FROM poison_stream"
    actions:
      - log: {}
    options:
      onError: dlq
      dlq:
        topic: poison_dlq_out
"#;
    let manifest_bad =
        DeclarativeManifest::from_str(yaml_bad, "yaml", false).expect("Bad manifest should parse");

    // Reconcile with a watchdog window of 3s and low spike threshold (1)
    reconcile_manifest_with_watchdog(&state, &manifest_bad, Duration::from_secs(3), 1)
        .await
        .expect("Reconciling bad manifest should start rules and watchdog");

    assert_eq!(state.rule_manager.list_rules().len(), 2);
    assert!(state.rule_manager.get_rule("rule_poison").is_some());

    // 3. Trigger a DLQ error record on rule_poison via poison pill on poison_stream
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut err_data = std::collections::HashMap::new();
    err_data.insert(
        "__raw_error__".to_string(),
        serde_json::Value::String("Failed to decode JSON: invalid syntax".to_string()),
    );
    err_data.insert(
        "__raw_payload__".to_string(),
        serde_json::Value::String("{bad_json: missing_quotes}".to_string()),
    );
    let _ = state
        .stream_bus
        .get_or_create("poison_stream")
        .send(rekuiper_core::StreamRecord::new(err_data))
        .await;

    // 4. Verify watchdog catches DLQ spike, publishes CRITICAL alert, and rolls back
    let alert_event = tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            if let Ok(CommsEvent::Alert {
                source,
                level,
                message,
                ..
            }) = comms_rx.recv().await
            {
                if source == "watchdog" && level == "CRITICAL" {
                    return message;
                }
            }
        }
    })
    .await
    .expect("Watchdog alert should fire within timeout");

    assert!(
        alert_event.contains("DLQ error spike detected"),
        "Alert message was: {}",
        alert_event
    );

    // Give rollback brief moment to complete
    tokio::time::sleep(Duration::from_millis(300)).await;

    // 5. Verify rule_poison is eliminated and rule_stable is preserved
    let current_rules = state.rule_manager.list_rules();
    assert_eq!(current_rules.len(), 1);
    assert!(state.rule_manager.get_rule("rule_stable").is_some());
    assert!(state.rule_manager.get_rule("rule_poison").is_none());
}

// ---------------------------------------------------------------------------
// 3.4 Safe Secret Masking & Round-Trip Preservation
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_secret_masking_interpolation_and_roundtrip_preservation() {
    let state = build_test_state();
    let kv = state.kv.as_ref().unwrap();

    // 1. Setup environment variable and KV secrets
    std::env::set_var("TEST_BROKER_PWD", "vault_secret_9999");
    kv.set("secrets", "token_key", "sec_token_xyz_8888")
        .await
        .unwrap();

    // 2. Test String interpolation: ${ENV:VAR} and ${SECRET:key}
    let raw_tmpl = "user=admin;pass=${ENV:TEST_BROKER_PWD};tok=${SECRET:token_key};other=plain";
    let interpolated = interpolate_string(raw_tmpl, Some(kv.as_ref()));
    assert_eq!(
        interpolated,
        "user=admin;pass=vault_secret_9999;tok=sec_token_xyz_8888;other=plain"
    );

    // 3. Test Object interpolation & Masking
    let mut conn_payload = json!({
        "id": "mqtt_conn_edge",
        "username": "device_user",
        "password": "${ENV:TEST_BROKER_PWD}",
        "token": "${SECRET:token_key}",
        "secret": "plain_embedded_secret",
        "private_key": "rsa_private_key_content",
        "host": "192.168.1.10"
    });

    interpolate_secrets(&mut conn_payload, Some(kv.as_ref()));
    assert_eq!(conn_payload["password"].as_str(), Some("vault_secret_9999"));
    assert_eq!(conn_payload["token"].as_str(), Some("sec_token_xyz_8888"));

    // Save real secrets into state
    state
        .connections
        .write()
        .insert("mqtt_conn_edge".to_string(), conn_payload.clone());

    // Verify mask_secrets replaces all sensitive keys with "******"
    let mut masked_copy = conn_payload.clone();
    mask_secrets(&mut masked_copy);
    assert_eq!(masked_copy["password"].as_str(), Some("******"));
    assert_eq!(masked_copy["token"].as_str(), Some("******"));
    assert_eq!(masked_copy["secret"].as_str(), Some("******"));
    assert_eq!(masked_copy["private_key"].as_str(), Some("******"));
    assert_eq!(masked_copy["username"].as_str(), Some("device_user"));
    assert_eq!(masked_copy["host"].as_str(), Some("192.168.1.10"));

    // 4. Test Round-Trip PUT / update preservation:
    // Client sends back masked placeholders ("******") with updated username
    let mut incoming_update = json!({
        "id": "mqtt_conn_edge",
        "username": "updated_user",
        "password": "******",
        "token": "******",
        "secret": "******",
        "private_key": "******",
        "host": "192.168.1.20"
    });

    let existing = state
        .connections
        .read()
        .get("mqtt_conn_edge")
        .unwrap()
        .clone();
    preserve_masked_secrets(&mut incoming_update, &existing);

    // Verify existing real secrets were safely preserved
    assert_eq!(incoming_update["username"].as_str(), Some("updated_user"));
    assert_eq!(incoming_update["host"].as_str(), Some("192.168.1.20"));
    assert_eq!(
        incoming_update["password"].as_str(),
        Some("vault_secret_9999")
    );
    assert_eq!(
        incoming_update["token"].as_str(),
        Some("sec_token_xyz_8888")
    );
    assert_eq!(
        incoming_update["secret"].as_str(),
        Some("plain_embedded_secret")
    );
    assert_eq!(
        incoming_update["private_key"].as_str(),
        Some("rsa_private_key_content")
    );

    // 5. Test Router endpoint GET and PUT roundtrip via Axum
    let router = create_router(state.clone());

    // GET /connections/mqtt_conn_edge returns masked payload
    let get_req = Request::builder()
        .method("GET")
        .uri("/connections/mqtt_conn_edge")
        .body(Body::empty())
        .unwrap();

    let get_resp = router.clone().oneshot(get_req).await.unwrap();
    assert_eq!(get_resp.status(), axum::http::StatusCode::OK);
    let get_bytes = axum::body::to_bytes(get_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let get_val: Value = serde_json::from_slice(&get_bytes).unwrap();
    assert_eq!(get_val["password"].as_str(), Some("******"));
    assert_eq!(get_val["token"].as_str(), Some("******"));

    // PUT /connections/mqtt_conn_edge preserves stored secrets
    let put_req = Request::builder()
        .method("PUT")
        .uri("/connections/mqtt_conn_edge")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "username": "api_user_final",
                "password": "******",
                "token": "******",
                "host": "10.0.0.1"
            })
            .to_string(),
        ))
        .unwrap();

    let put_resp = router.clone().oneshot(put_req).await.unwrap();
    assert_eq!(put_resp.status(), axum::http::StatusCode::OK);

    let stored = state
        .connections
        .read()
        .get("mqtt_conn_edge")
        .unwrap()
        .clone();
    assert_eq!(stored["username"].as_str(), Some("api_user_final"));
    assert_eq!(stored["host"].as_str(), Some("10.0.0.1"));
    assert_eq!(stored["password"].as_str(), Some("vault_secret_9999"));
    assert_eq!(stored["token"].as_str(), Some("sec_token_xyz_8888"));
}
