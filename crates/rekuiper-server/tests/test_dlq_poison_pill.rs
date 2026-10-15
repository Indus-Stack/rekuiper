use rekuiper_conf::KuiperConfig;
use rekuiper_core::manager::{RuleManager, StreamManager, TableManager};
use rekuiper_core::model::MAX_DLQ_RAW_PAYLOAD_BYTES;
use rekuiper_core::StreamBus;
use rekuiper_server::routes::{create_router, AppState};
use serde_json::json;
use tokio::net::TcpListener;

async fn spawn_test_server(declarative: bool) -> (String, tokio::task::JoinHandle<()>, AppState) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind ephemeral port");
    let local_addr = listener.local_addr().unwrap();

    let stream_bus = StreamBus::new();
    let stream_manager = StreamManager::new();
    let rule_manager = RuleManager::new(stream_bus.clone());

    let mut config = KuiperConfig::default();
    config.basic.enable_private_net = true;

    let mut state = AppState::new(
        "test_dlq".to_string(),
        config,
        stream_manager,
        TableManager::new(),
        rule_manager,
        stream_bus,
    );
    if declarative {
        state.declarative_mode = true;
        state.declarative_path = Some("dummy_manifest.yaml".into());
    }

    let app = create_router(state.clone());
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (format!("http://{}", local_addr), handle, state)
}

#[tokio::test]
async fn test_dlq_codec_decode_poison_pill() {
    let (base_url, _handle, state) = spawn_test_server(false).await;
    let client = reqwest::Client::new();

    // 1. Create stream
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s_codec () WITH (TYPE=\"memory\", FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 2. Create rule with onError: "dlq"
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "r_codec",
            "sql": "SELECT * FROM s_codec",
            "actions": [{"log": {}}],
            "options": {
                "onError": "dlq"
            }
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // 3. Inject a decode error via stream bus
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
        .get_or_create("s_codec")
        .send(rekuiper_core::StreamRecord::new(err_data))
        .await;

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // 4. Query DLQ errors API
    let resp = client
        .get(format!("{}/rules/r_codec/errors", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let stats: serde_json::Value = resp.json().await.unwrap();

    assert_eq!(stats.get("total_errors").and_then(|v| v.as_u64()), Some(1));
    assert_eq!(stats.get("codec_decode").and_then(|v| v.as_u64()), Some(1));

    let recent = stats.get("recent_errors").and_then(|v| v.as_array()).unwrap();
    assert_eq!(recent.len(), 1);
    assert_eq!(recent[0].get("stage").and_then(|v| v.as_str()), Some("CodecDecode"));
    assert_eq!(
        recent[0].get("raw_payload").and_then(|v| v.as_str()),
        Some("{bad_json: missing_quotes}")
    );

    // 5. Verify Prometheus metric
    let resp = client
        .get(format!("{}/metrics", base_url))
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("rekuiper_rule_dlq_records_total{rule=\"r_codec\",stage=\"codec_decode\"} 1"),
        "Missing prometheus dlq metric: {}",
        body
    );
}

#[tokio::test]
async fn test_dlq_schema_validation_poison_pill() {
    let (base_url, _handle, _state) = spawn_test_server(false).await;
    let client = reqwest::Client::new();

    // 1. Create stream with strict types
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s_schema (id bigint, temp float, dev string) WITH (TYPE=\"memory\", FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 2. Create rule with onError: "dlq"
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "r_schema",
            "sql": "SELECT id, temp, dev FROM s_schema",
            "actions": [{"log": {}}],
            "options": {
                "onError": "dlq"
            }
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // 3. Post invalid schema record: id is boolean (cannot coerce to bigint)
    let resp = client
        .post(format!("{}/streams/s_schema/data", base_url))
        .json(&json!({"id": [1, 2, 3], "temp": 24.5, "dev": "d1"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // 4. Query DLQ errors API
    let resp = client
        .get(format!("{}/rules/r_schema/errors", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let stats: serde_json::Value = resp.json().await.unwrap();

    assert_eq!(stats.get("total_errors").and_then(|v| v.as_u64()), Some(1));
    assert_eq!(stats.get("schema_validation").and_then(|v| v.as_u64()), Some(1));

    let recent = stats.get("recent_errors").and_then(|v| v.as_array()).unwrap();
    assert_eq!(recent.len(), 1);
    assert_eq!(recent[0].get("stage").and_then(|v| v.as_str()), Some("SchemaValidation"));
    assert!(recent[0].get("error").and_then(|v| v.as_str()).unwrap().contains("bigint"));
}

#[tokio::test]
async fn test_dlq_sql_eval_poison_pill() {
    let (base_url, _handle, _state) = spawn_test_server(false).await;
    let client = reqwest::Client::new();

    // 1. Create stream
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s_eval () WITH (TYPE=\"memory\", FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 2. Create rule with onError: "dlq"
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "r_eval",
            "sql": "SELECT id, abs(dev) as v FROM s_eval",
            "actions": [{"log": {}}],
            "options": {
                "onError": "dlq"
            }
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // 3. Post invalid row for abs(): string "not_a_num"
    let resp = client
        .post(format!("{}/streams/s_eval/data", base_url))
        .json(&json!({"id": 1, "dev": "not_a_num"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // 4. Query DLQ errors API
    let resp = client
        .get(format!("{}/rules/r_eval/errors", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let stats: serde_json::Value = resp.json().await.unwrap();

    assert_eq!(stats.get("total_errors").and_then(|v| v.as_u64()), Some(1));
    assert_eq!(stats.get("sql_eval").and_then(|v| v.as_u64()), Some(1));

    let recent = stats.get("recent_errors").and_then(|v| v.as_array()).unwrap();
    assert_eq!(recent.len(), 1);
    assert_eq!(recent[0].get("stage").and_then(|v| v.as_str()), Some("SqlEval"));
    assert!(recent[0].get("tuple").is_some());
}

#[tokio::test]
async fn test_dlq_raw_payload_capping() {
    let (base_url, _handle, state) = spawn_test_server(false).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s_cap () WITH (TYPE=\"memory\", FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "r_cap",
            "sql": "SELECT * FROM s_cap",
            "actions": [{"log": {}}],
            "options": {
                "onError": "dlq",
                "dlq": {
                    "includeRawPayload": true
                }
            }
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Send a massive payload: 10,000 bytes
    let huge_payload = "X".repeat(10000);
    let mut err_data = std::collections::HashMap::new();
    err_data.insert(
        "__raw_error__".to_string(),
        serde_json::Value::String("Corrupt frame".to_string()),
    );
    err_data.insert(
        "__raw_payload__".to_string(),
        serde_json::Value::String(huge_payload),
    );
    let _ = state
        .stream_bus
        .get_or_create("s_cap")
        .send(rekuiper_core::StreamRecord::new(err_data))
        .await;

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let resp = client
        .get(format!("{}/rules/r_cap/errors", base_url))
        .send()
        .await
        .unwrap();
    let stats: serde_json::Value = resp.json().await.unwrap();

    let recent = stats.get("recent_errors").and_then(|v| v.as_array()).unwrap();
    let raw = recent[0].get("raw_payload").and_then(|v| v.as_str()).unwrap();

    assert!(raw.len() <= MAX_DLQ_RAW_PAYLOAD_BYTES);
    assert!(raw.ends_with("…[truncated]"));
}

#[tokio::test]
async fn test_dlq_sink_delivery_http_4xx_permanent_error() {
    // Spawn mock HTTP server returning 400 Bad Request
    let mock_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            if let Ok((mut socket, _)) = mock_listener.accept().await {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = [0u8; 1024];
                let _ = socket.read(&mut buf).await;
                let response = "HTTP/1.1 400 Bad Request\r\nContent-Length: 26\r\nConnection: close\r\n\r\n{\"error\": \"Malformed body\"}";
                let _ = socket.write_all(response.as_bytes()).await;
            }
        }
    });

    let (base_url, _handle, _state) = spawn_test_server(false).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s_rest () WITH (TYPE=\"memory\", FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Create rule sending to our 400-returning mock server
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "r_rest_poison",
            "sql": "SELECT * FROM s_rest",
            "actions": [{
                "rest": {
                    "url": format!("http://{}/sink", mock_addr),
                    "method": "POST"
                }
            }],
            "options": {
                "onError": "dlq"
            }
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Post a record that gets delivered to the 400 sink
    let resp = client
        .post(format!("{}/streams/s_rest/data", base_url))
        .json(&json!({"foo": "bar"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Wait for sink delivery
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    // Check DLQ API
    let resp = client
        .get(format!("{}/rules/r_rest_poison/errors", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let stats: serde_json::Value = resp.json().await.unwrap();

    assert_eq!(stats.get("sink_delivery").and_then(|v| v.as_u64()), Some(1));
    let recent = stats.get("recent_errors").and_then(|v| v.as_array()).unwrap();
    assert_eq!(recent[0].get("stage").and_then(|v| v.as_str()), Some("SinkDelivery"));
    assert_eq!(recent[0].get("sink_type").and_then(|v| v.as_str()), Some("rest"));
}

#[tokio::test]
async fn test_dlq_on_error_halt() {
    let (base_url, _handle, _state) = spawn_test_server(false).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s_halt () WITH (TYPE=\"memory\", FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Create rule with onError: "halt"
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "r_halt",
            "sql": "SELECT id, abs(dev) as v FROM s_halt",
            "actions": [{"log": {}}],
            "options": {
                "onError": "halt"
            }
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Send poison pill
    let resp = client
        .post(format!("{}/streams/s_halt/data", base_url))
        .json(&json!({"id": 1, "dev": "poison"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Wait for halt to take effect
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    // Verify status is "stopped" and error is preserved
    let resp = client
        .get(format!("{}/rules/r_halt/status", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let status: serde_json::Value = resp.json().await.unwrap();

    assert_eq!(status.get("status").and_then(|v| v.as_str()), Some("stopped"));
    let last_exc = status.get("last_exception").and_then(|v| v.as_str()).unwrap_or("");
    assert!(
        last_exc.contains("call func abs error") || last_exc.contains("alias: v"),
        "Unexpected status message: {}",
        last_exc
    );
}

#[tokio::test]
async fn test_dlq_clear_api_and_declarative_allowlist() {
    // 1. Test standard server clear
    let (base_url, _handle, _state) = spawn_test_server(false).await;
    let client = reqwest::Client::new();

    let _ = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s_clr () WITH (TYPE=\"memory\", FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();

    let _ = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "r_clr",
            "sql": "SELECT id, abs(dev) as v FROM s_clr",
            "actions": [{"log": {}}],
            "options": { "onError": "dlq" }
        }))
        .send()
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let _ = client
        .post(format!("{}/streams/s_clr/data", base_url))
        .json(&json!({"id": 1, "dev": "invalid"}))
        .send()
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Query errors
    let resp = client
        .get(format!("{}/rules/r_clr/errors", base_url))
        .send()
        .await
        .unwrap();
    let stats: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(stats.get("recent_errors").and_then(|v| v.as_array()).unwrap().len(), 1);

    // Clear errors
    let resp = client
        .delete(format!("{}/rules/r_clr/errors", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Verify recent_errors is now empty, but total_errors remains 1
    let resp = client
        .get(format!("{}/rules/r_clr/errors", base_url))
        .send()
        .await
        .unwrap();
    let stats: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(stats.get("total_errors").and_then(|v| v.as_u64()), Some(1));
    assert_eq!(stats.get("recent_errors").and_then(|v| v.as_array()).unwrap().len(), 0);

    // 2. Test declarative mode allows DELETE /rules/:id/errors
    let (decl_url, _handle, state_decl) = spawn_test_server(true).await;
    let _ = state_decl.rule_manager.create_rule(
        rekuiper_core::RuleDefinition {
            id: "r_decl".to_string(),
            sql: "SELECT 1".to_string(),
            ..Default::default()
        },
    ).await;

    let resp = client
        .delete(format!("{}/rules/r_decl/errors", decl_url))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "DELETE /rules/:id/errors must not be blocked by declarative read-only guard"
    );
}

#[tokio::test]
async fn test_dlq_external_sink_dispatch() {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(10);

    let mock_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            if let Ok((mut socket, _)) = mock_listener.accept().await {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = vec![0u8; 4096];
                if let Ok(n) = socket.read(&mut buf).await {
                    let s = String::from_utf8_lossy(&buf[..n]).to_string();
                    let _ = tx.send(s).await;
                }
                let response = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                let _ = socket.write_all(response.as_bytes()).await;
            }
        }
    });

    let (base_url, _handle, _state) = spawn_test_server(false).await;
    let client = reqwest::Client::new();

    let _ = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s_ext () WITH (TYPE=\"memory\", FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();

    let _ = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "r_ext",
            "sql": "SELECT id, abs(dev) as v FROM s_ext",
            "actions": [{"log": {}}],
            "options": {
                "onError": "dlq",
                "dlq": {
                    "action": {
                        "rest": {
                            "url": format!("http://{}/dlq_inbox", mock_addr),
                            "method": "POST"
                        }
                    }
                }
            }
        }))
        .send()
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Send poison pill
    let _ = client
        .post(format!("{}/streams/s_ext/data", base_url))
        .json(&json!({"id": 1, "dev": "fail_eval"}))
        .send()
        .await
        .unwrap();

    // Check external sink received envelope
    let received = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
        .await
        .expect("External DLQ sink did not receive envelope")
        .expect("Channel closed");

    assert!(received.contains("rule_id") || received.contains("r_ext"));
    assert!(received.contains("SqlEval"));
}
