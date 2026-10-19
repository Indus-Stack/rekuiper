use rekuiper_conf::KuiperConfig;
use rekuiper_core::manager::{RuleManager, StreamManager, TableManager};
use rekuiper_core::StreamBus;
use rekuiper_server::routes::{create_router, AppState};
use serde_json::json;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

async fn spawn_test_server() -> (String, tokio::task::JoinHandle<()>, AppState) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind ephemeral port");
    let local_addr = listener.local_addr().unwrap();

    let stream_bus = StreamBus::new();
    let stream_manager = StreamManager::new();
    let rule_manager = RuleManager::new(stream_bus.clone());

    let mut config = KuiperConfig::default();
    config.basic.enable_private_net = true;

    let state = AppState::new(
        "test_issue22".to_string(),
        config,
        stream_manager,
        TableManager::new(),
        rule_manager,
        stream_bus,
    );

    let app = create_router(state.clone());
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (format!("http://{}", local_addr), handle, state)
}

#[tokio::test]
async fn test_send_nil_field_default_omits_nulls() {
    let (base_url, _handle, state) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // Create stream
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s1 () WITH (DATASOURCE=\"s1\");"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let mut sink_rx = state.stream_bus.subscribe("sink_nil_default");

    // Create rule: default sendNilField is false
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_nil_default",
            "sql": "SELECT id, null_val, temp FROM s1",
            "actions": [{"memory": {"topic": "sink_nil_default"}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // Send row with null_val = null
    let resp = client
        .post(format!("{}/streams/s1/data", base_url))
        .json(&json!({"id": 1, "null_val": null, "temp": 25}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let rec = tokio::time::timeout(std::time::Duration::from_secs(2), sink_rx.recv())
        .await
        .expect("timed out waiting for sink output")
        .expect("sink closed");
    assert_eq!(rec.data.get("id"), Some(&json!(1)));
    assert_eq!(rec.data.get("temp"), Some(&json!(25)));
    assert!(
        !rec.data.contains_key("null_val"),
        "default sendNilField (false) must omit null fields, but got: {:?}",
        rec.data
    );
}

#[tokio::test]
async fn test_send_nil_field_rule_option_inherited() {
    let (base_url, _handle, state) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // Create stream
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s2 () WITH (DATASOURCE=\"s2\");"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let mut sink_rx = state.stream_bus.subscribe("sink_nil_true");

    // Create rule: sendNilField: true in options
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_nil_true",
            "sql": "SELECT id, null_val, temp FROM s2",
            "actions": [{"memory": {"topic": "sink_nil_true"}}],
            "options": {"sendNilField": true}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // Send row with null_val = null
    let resp = client
        .post(format!("{}/streams/s2/data", base_url))
        .json(&json!({"id": 2, "null_val": null, "temp": 30}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let rec = tokio::time::timeout(std::time::Duration::from_secs(2), sink_rx.recv())
        .await
        .expect("timed out waiting for sink output")
        .expect("sink closed");
    assert_eq!(rec.data.get("id"), Some(&json!(2)));
    assert_eq!(rec.data.get("temp"), Some(&json!(30)));
    assert_eq!(
        rec.data.get("null_val"),
        Some(&serde_json::Value::Null),
        "rule option sendNilField: true must retain null fields"
    );
}

#[tokio::test]
async fn test_send_nil_field_sink_override() {
    let (base_url, _handle, state) = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s3 () WITH (DATASOURCE=\"s3\");"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let mut rx_inherit = state.stream_bus.subscribe("sink_inherit_true");
    let mut rx_override = state.stream_bus.subscribe("sink_override_false");

    // Rule options sendNilField: true, but second sink overrides sendNilField: false
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_nil_override",
            "sql": "SELECT id, null_val, temp FROM s3",
            "actions": [
                {"memory": {"topic": "sink_inherit_true"}},
                {"memory": {"topic": "sink_override_false", "sendNilField": false}}
            ],
            "options": {"sendNilField": true}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let resp = client
        .post(format!("{}/streams/s3/data", base_url))
        .json(&json!({"id": 3, "null_val": null, "temp": 35}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let rec_inherit = tokio::time::timeout(std::time::Duration::from_secs(2), rx_inherit.recv())
        .await
        .expect("timed out")
        .expect("closed");
    assert_eq!(
        rec_inherit.data.get("null_val"),
        Some(&serde_json::Value::Null)
    );

    let rec_override = tokio::time::timeout(std::time::Duration::from_secs(2), rx_override.recv())
        .await
        .expect("timed out")
        .expect("closed");
    assert!(
        !rec_override.data.contains_key("null_val"),
        "sink override sendNilField: false must omit null_val"
    );
}

#[tokio::test]
async fn test_select_star_does_not_emit_internal_rule_metadata() {
    let (base_url, _handle, state) = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s4 () WITH (DATASOURCE=\"s4\");"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let mut sink_rx = state.stream_bus.subscribe("sink_star");

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_star",
            "sql": "SELECT * FROM s4",
            "actions": [{"memory": {"topic": "sink_star"}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let resp = client
        .post(format!("{}/streams/s4/data", base_url))
        .json(&json!({"device": "dev1", "temp": 22}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let rec = tokio::time::timeout(std::time::Duration::from_secs(2), sink_rx.recv())
        .await
        .expect("timed out")
        .expect("closed");
    assert_eq!(rec.data.get("device"), Some(&json!("dev1")));
    assert_eq!(rec.data.get("temp"), Some(&json!(22)));
    assert!(
        !rec.data.contains_key("__rule_id__"),
        "SELECT * must not contain internal __rule_id__"
    );
    assert!(
        !rec.data.contains_key("__rule_start__"),
        "SELECT * must not contain internal __rule_start__"
    );
    assert!(
        !rec.data.contains_key("__meta__"),
        "SELECT * must not contain internal __meta__"
    );
}

#[tokio::test]
async fn test_stable_sorted_json_key_order_in_rest_sink() {
    let raw_payloads: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let raw_clone = raw_payloads.clone();

    // Mock HTTP server receiving REST sink payloads
    let mock_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        let app = axum::Router::new().route(
            "/sink",
            axum::routing::post(move |body: String| {
                let r = raw_clone.clone();
                async move {
                    r.lock().await.push(body);
                    axum::http::StatusCode::OK
                }
            }),
        );
        axum::serve(mock_listener, app).await.unwrap();
    });

    let (base_url, _handle, _state) = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s5 () WITH (DATASOURCE=\"s5\");"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_key_order",
            "sql": "SELECT z, a, m FROM s5",
            "actions": [{
                "rest": {
                    "url": format!("http://{}/sink", mock_addr),
                    "method": "POST",
                    "sendSingle": true
                }
            }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let resp = client
        .post(format!("{}/streams/s5/data", base_url))
        .json(&json!({"z": 100, "a": 200, "m": 300}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Wait for mock HTTP server to receive the payload
    let mut payload = None;
    for _ in 0..20 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let lock = raw_payloads.lock().await;
        if !lock.is_empty() {
            payload = Some(lock[0].clone());
            break;
        }
    }

    let raw = payload.expect("REST sink did not deliver payload");
    // Keys must preserve SELECT order: z, then a, then m (Issue #45)
    assert_eq!(
        raw, r#"{"z":100,"a":200,"m":300}"#,
        "JSON key order must preserve SELECT field order"
    );
}
