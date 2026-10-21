use rekuiper_conf::KuiperConfig;
use rekuiper_core::kv::MemKvStore;
use rekuiper_core::model::DeclarativeManifest;
use rekuiper_core::{RuleManager, StreamBus, StreamManager, TableManager};
use rekuiper_server::agent::{connect_outbound_ws_agent, AgentMetadata};
use rekuiper_server::fleet::{
    FleetGroupSummary, FleetRollout, FleetSnapshot, RolloutStage, RolloutStatus,
};
use rekuiper_server::routes::{create_router, AppState};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;

fn build_test_state(name: &str) -> AppState {
    let config = KuiperConfig::default();
    let stream_bus = StreamBus::new();
    let stream_manager = StreamManager::new();
    let table_manager = TableManager::new();
    let rule_manager = RuleManager::new(stream_bus.clone());

    let mut state = AppState::new(
        name.to_string(),
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

#[tokio::test]
async fn test_phase4_fleet_control_plane_full_lifecycle() {
    // -----------------------------------------------------------------------
    // Step 1: Start Server-Side Control Plane on an ephemeral port
    // -----------------------------------------------------------------------
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Must bind ephemeral listener");
    let port = listener.local_addr().unwrap().port();
    let ws_url = format!("ws://127.0.0.1:{}/fleet/ws", port);
    let http_base = format!("http://127.0.0.1:{}", port);

    let cp_state = build_test_state("control_plane");
    let cp_router = create_router(cp_state.clone());

    let _server_handle = tokio::spawn(async move {
        let _ = axum::serve(listener, cp_router).await;
    });

    // Allow the server task to start listening
    tokio::time::sleep(Duration::from_millis(60)).await;

    // -----------------------------------------------------------------------
    // Step 2: Spawn Multiple Simulated Edge Agents connecting to /fleet/ws
    // -----------------------------------------------------------------------
    let (cancel_tx1, cancel_rx1) = tokio::sync::watch::channel(false);
    let (cancel_tx2, cancel_rx2) = tokio::sync::watch::channel(false);
    let (cancel_tx3, cancel_rx3) = tokio::sync::watch::channel(false);

    let edge1_state = build_test_state("edge_detroit_01");
    let edge1_router = create_router(edge1_state.clone());
    let edge1_meta = AgentMetadata {
        device_id: "edge-detroit-01".to_string(),
        name: "Detroit Stamping Press 01".to_string(),
        plant: "detroit-automotive".to_string(),
        region: "us-midwest".to_string(),
        device_type: "industrial-pc".to_string(),
        version: "1.0.0".to_string(),
    };

    let edge2_state = build_test_state("edge_detroit_02");
    let edge2_router = create_router(edge2_state.clone());
    let edge2_meta = AgentMetadata {
        device_id: "edge-detroit-02".to_string(),
        name: "Detroit Robotic Cell 02".to_string(),
        plant: "detroit-automotive".to_string(),
        region: "us-midwest".to_string(),
        device_type: "gateway".to_string(),
        version: "1.0.0".to_string(),
    };

    let edge3_state = build_test_state("edge_austin_01");
    let edge3_router = create_router(edge3_state.clone());
    let edge3_meta = AgentMetadata {
        device_id: "edge-austin-01".to_string(),
        name: "Austin Paint Shop 01".to_string(),
        plant: "austin-gigafactory".to_string(),
        region: "us-south".to_string(),
        device_type: "gateway".to_string(),
        version: "1.0.0".to_string(),
    };

    let ws_url1 = ws_url.clone();
    let ws_url2 = ws_url.clone();
    let ws_url3 = ws_url.clone();

    // Connect edge agents with rapid 100ms heartbeats for responsive testing
    tokio::spawn(async move {
        let _ = connect_outbound_ws_agent(
            &ws_url1,
            edge1_state.clone(),
            edge1_router,
            edge1_meta,
            Duration::from_millis(100),
            cancel_rx1,
        )
        .await;
    });

    tokio::spawn(async move {
        let _ = connect_outbound_ws_agent(
            &ws_url2,
            edge2_state.clone(),
            edge2_router,
            edge2_meta,
            Duration::from_millis(100),
            cancel_rx2,
        )
        .await;
    });

    tokio::spawn(async move {
        let _ = connect_outbound_ws_agent(
            &ws_url3,
            edge3_state.clone(),
            edge3_router,
            edge3_meta,
            Duration::from_millis(100),
            cancel_rx3,
        )
        .await;
    });

    // Wait for initial connection, Stream 0 establishment, and heartbeat delivery
    tokio::time::sleep(Duration::from_millis(500)).await;

    let http_client = reqwest::Client::new();

    // -----------------------------------------------------------------------
    // Step 3: Verify Device Fleet Catalog & Grouping APIs
    // -----------------------------------------------------------------------
    let devices_resp = http_client
        .get(format!("{}/api/v1/fleet/devices", http_base))
        .send()
        .await
        .expect("Query /api/v1/fleet/devices");
    assert_eq!(devices_resp.status(), 200);

    let devices: Vec<serde_json::Value> = devices_resp.json().await.expect("Parse devices JSON");
    assert_eq!(devices.len(), 3, "Expected 3 registered devices in catalog");

    let d1 = devices
        .iter()
        .find(|d| d["device_id"] == "edge-detroit-01")
        .expect("edge-detroit-01 must exist");
    assert_eq!(d1["status"], "online");
    assert_eq!(d1["plant"], "detroit-automotive");
    assert_eq!(d1["region"], "us-midwest");

    let groups_resp = http_client
        .get(format!("{}/api/v1/fleet/groups", http_base))
        .send()
        .await
        .expect("Query /api/v1/fleet/groups");
    assert_eq!(groups_resp.status(), 200);
    let groups: Vec<FleetGroupSummary> = groups_resp.json().await.expect("Parse groups JSON");
    let detroit_summary = groups
        .iter()
        .find(|g| g.name == "detroit-automotive")
        .expect("Detroit group");
    assert_eq!(detroit_summary.total_devices, 2);
    let austin_summary = groups
        .iter()
        .find(|g| g.name == "austin-gigafactory")
        .expect("Austin group");
    assert_eq!(austin_summary.total_devices, 1);

    // -----------------------------------------------------------------------
    // Step 4: Verify Remote HTTP Proxy Tunneling over Yamux Stream 1..N
    // -----------------------------------------------------------------------
    // Send a request to Control Plane: /api/v1/fleet/devices/edge-detroit-01/proxy/rules
    // Control plane opens an on-demand Yamux stream into edge1's local Axum router
    let proxy_rules_resp = http_client
        .get(format!(
            "{}/api/v1/fleet/devices/edge-detroit-01/proxy/rules",
            http_base
        ))
        .send()
        .await
        .expect("Tunneled GET /rules over Yamux");
    assert_eq!(proxy_rules_resp.status(), 200);
    let rules_body: serde_json::Value = proxy_rules_resp.json().await.expect("Parse rules JSON");
    assert!(rules_body.is_array());

    // -----------------------------------------------------------------------
    // Step 5: Test Staged Canary Rollout: (a) Healthy Rollout to Production
    // -----------------------------------------------------------------------
    let yaml_valid = r#"
version: v1
streams:
  stable_stream: "CREATE STREAM stable_stream () WITH (FORMAT=\"JSON\", TYPE=\"mqtt\", TOPIC=\"stable_topic\");"
rules:
  - id: rule_stable
    sql: "SELECT * FROM stable_stream"
    actions:
      - log: {}
"#;
    let manifest_valid = DeclarativeManifest::from_str(yaml_valid, "yaml", false)
        .expect("Valid manifest must parse");
    let valid_declarative = serde_json::to_value(&manifest_valid).unwrap();

    let snap1_req = json!({
        "name": "snap-healthy-v1",
        "description": "Healthy declarative snapshot",
        "declarative": valid_declarative
    });

    let create_snap1_resp = http_client
        .post(format!("{}/api/v1/fleet/snapshots", http_base))
        .json(&snap1_req)
        .send()
        .await
        .expect("Create snapshot snap-healthy-v1");
    assert_eq!(create_snap1_resp.status(), 201);
    let snap1_body: FleetSnapshot = create_snap1_resp.json().await.unwrap();
    assert!(
        !snap1_body.sha256.is_empty(),
        "Must have SHA-256 integrity hash"
    );

    // Launch rollout targeting Canary stage with auto_promote = false
    let rollout1_req = json!({
        "snapshot_id": snap1_body.snapshot_id,
        "target_group": "detroit-automotive",
        "auto_promote": false
    });

    let create_rollout1_resp = http_client
        .post(format!("{}/api/v1/fleet/rollouts", http_base))
        .json(&rollout1_req)
        .send()
        .await
        .expect("Launch rollout 1");
    assert_eq!(create_rollout1_resp.status(), 201);
    let rollout1: FleetRollout = create_rollout1_resp.json().await.unwrap();
    assert_eq!(rollout1.current_stage, RolloutStage::Canary);
    assert_eq!(rollout1.status, RolloutStatus::InProgress);
    assert_eq!(rollout1.canary_devices.len(), 1);

    // Wait for heartbeat evaluation cycle
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Promote rollout from Canary to 100% Production
    let promote_resp = http_client
        .post(format!(
            "{}/api/v1/fleet/rollouts/{}/promote",
            http_base, rollout1.rollout_id
        ))
        .send()
        .await
        .expect("Promote rollout to production");
    assert_eq!(promote_resp.status(), 200);
    let promoted: FleetRollout = promote_resp.json().await.unwrap();
    assert_eq!(promoted.current_stage, RolloutStage::Completed);
    assert_eq!(promoted.status, RolloutStatus::Completed);

    // -----------------------------------------------------------------------
    // Step 6: Test Staged Canary Rollout: (b) Faulty Snapshot Auto-Halt on Watchdog
    // -----------------------------------------------------------------------
    let yaml_faulty = r#"
version: v1
streams:
  faulty_stream: "CREATE STREAM faulty_stream () WITH (FORMAT=\"JSON\", TYPE=\"mqtt\", TOPIC=\"faulty_topic\");"
rules:
  - id: rule_faulty
    sql: "SELECT * FROM non_existent_stream_xyz"
    actions:
      - log: {}
"#;
    let manifest_faulty = DeclarativeManifest::from_str(yaml_faulty, "yaml", false)
        .expect("Faulty manifest must parse");
    let faulty_declarative = serde_json::to_value(&manifest_faulty).unwrap();

    let snap2_req = json!({
        "name": "snap-faulty-v2",
        "description": "Faulty declarative snapshot",
        "declarative": faulty_declarative
    });

    let create_snap2_resp = http_client
        .post(format!("{}/api/v1/fleet/snapshots", http_base))
        .json(&snap2_req)
        .send()
        .await
        .expect("Create snapshot snap-faulty-v2");
    assert_eq!(create_snap2_resp.status(), 201);
    let snap2_body: FleetSnapshot = create_snap2_resp.json().await.unwrap();

    let rollout2_req = json!({
        "snapshot_id": snap2_body.snapshot_id,
        "target_group": "detroit-automotive",
        "auto_promote": false
    });

    let create_rollout2_resp = http_client
        .post(format!("{}/api/v1/fleet/rollouts", http_base))
        .json(&rollout2_req)
        .send()
        .await
        .expect("Launch rollout 2");
    assert_eq!(create_rollout2_resp.status(), 201);
    let rollout2: FleetRollout = create_rollout2_resp.json().await.unwrap();

    // Check rollout status: when applied to Canary device, rule references non-existent stream,
    // causing reconciliation error, which halts the rollout immediately!
    assert_eq!(
        rollout2.status,
        RolloutStatus::RolledBack,
        "Faulty snapshot must auto-halt rollout into rolled_back state"
    );
    assert!(
        rollout2.message.contains("Canary failure") || rollout2.message.contains("halted"),
        "Must record failure reason in rollout message: {}",
        rollout2.message
    );

    // -----------------------------------------------------------------------
    // Step 7: Verify Audit Log & Observability
    // -----------------------------------------------------------------------
    let audit_resp = http_client
        .get(format!("{}/api/v1/fleet/audit", http_base))
        .send()
        .await
        .expect("Query /api/v1/fleet/audit");
    assert_eq!(audit_resp.status(), 200);
    let audit_entries: Vec<serde_json::Value> = audit_resp.json().await.unwrap();
    assert!(!audit_entries.is_empty(), "Must have audit log entries");

    let has_halt_entry = audit_entries
        .iter()
        .any(|e| e["action"] == "rollout_halted" && e["target_id"] == rollout2.rollout_id);
    assert!(
        has_halt_entry,
        "Audit trail must record halted rollout failure"
    );

    let metrics_text_resp = http_client
        .get(format!("{}/api/v1/fleet/metrics", http_base))
        .header("Accept", "text/plain")
        .send()
        .await
        .expect("Query /api/v1/fleet/metrics text/plain");
    assert_eq!(metrics_text_resp.status(), 200);
    let metrics_text = metrics_text_resp.text().await.unwrap();
    assert!(metrics_text.contains("rekuiper_fleet_connected_gateways 3"));

    let metrics_json_resp = http_client
        .get(format!("{}/api/v1/fleet/metrics", http_base))
        .send()
        .await
        .expect("Query /api/v1/fleet/metrics JSON");
    assert_eq!(metrics_json_resp.status(), 200);
    let metrics_json: serde_json::Value = metrics_json_resp.json().await.unwrap();
    assert_eq!(metrics_json["total_devices"], 3);

    // Cleanup edge agent tasks
    let _ = cancel_tx1.send(true);
    let _ = cancel_tx2.send(true);
    let _ = cancel_tx3.send(true);
}

#[tokio::test]
async fn test_fleet_auth_enforcement() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Must bind ephemeral listener");
    let port = listener.local_addr().unwrap().port();
    let http_base = format!("http://127.0.0.1:{}", port);
    let ws_url = format!("ws://127.0.0.1:{}/fleet/ws", port);

    let cp_state = build_test_state("control_plane_auth");
    // Configure fleet provisioning token
    cp_state.config.write().basic.fleet_token =
        Some("secure-provisioning-token-xyz-99".to_string());

    let cp_router = create_router(cp_state.clone());
    tokio::spawn(async move {
        let _ = axum::serve(listener, cp_router).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    let client = reqwest::Client::new();

    // 1. Unauthenticated request to /api/v1/fleet/devices must be blocked with 401 Unauthorized
    let unauth_resp = client
        .get(format!("{}/api/v1/fleet/devices", http_base))
        .send()
        .await
        .unwrap();
    assert_eq!(unauth_resp.status(), 401);

    // 2. Request with invalid bearer token must be blocked with 401 Unauthorized
    let bad_auth_resp = client
        .get(format!("{}/api/v1/fleet/devices", http_base))
        .header("Authorization", "Bearer invalid-token-123")
        .send()
        .await
        .unwrap();
    assert_eq!(bad_auth_resp.status(), 401);

    // 3. Request with correct bearer token succeeds with 200 OK
    let auth_resp = client
        .get(format!("{}/api/v1/fleet/devices", http_base))
        .header("Authorization", "Bearer secure-provisioning-token-xyz-99")
        .send()
        .await
        .unwrap();
    assert_eq!(auth_resp.status(), 200);

    // 4. WebSocket connection to /fleet/ws without token fails with 401
    let ws_res_unauth = tokio_tungstenite::connect_async(&ws_url).await;
    assert!(
        ws_res_unauth.is_err(),
        "Unauthenticated WebSocket must fail handshake"
    );

    // 5. WebSocket connection to /fleet/ws with ?token=<correct_token> succeeds
    let ws_authed_url = format!("{}?token=secure-provisioning-token-xyz-99", ws_url);
    let ws_res_auth = tokio_tungstenite::connect_async(&ws_authed_url).await;
    assert!(
        ws_res_auth.is_ok(),
        "Authenticated WebSocket handshake must succeed"
    );
}
