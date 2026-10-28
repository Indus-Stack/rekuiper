use rekuiper_conf::KuiperConfig;
use rekuiper_core::manager::{RuleManager, StreamManager, TableManager};
use rekuiper_core::StreamBus;
use rekuiper_server::routes::{create_router, AppState};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

// ---------------------------------------------------------------------------
// Mock Downstream OTLP Receiver Server
// ---------------------------------------------------------------------------
struct MockOtlpReceiver {
    port: u16,
    received_count: Arc<AtomicUsize>,
}

impl MockOtlpReceiver {
    async fn start(return_error: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let received_count = Arc::new(AtomicUsize::new(0));
        let count_clone = received_count.clone();

        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let count_c = count_clone.clone();

                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut temp = [0u8; 2048];
                    let mut content_length: Option<usize> = None;
                    let mut body_start: Option<usize> = None;

                    loop {
                        let n = match socket.read(&mut temp).await {
                            Ok(0) => break,
                            Ok(n) => n,
                            Err(_) => break,
                        };
                        buf.extend_from_slice(&temp[..n]);

                        if body_start.is_none() {
                            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                                let headers_str = String::from_utf8_lossy(&buf[..pos]);
                                body_start = Some(pos + 4);
                                for line in headers_str.lines() {
                                    if let Some((k, v)) = line.split_once(':') {
                                        if k.trim().eq_ignore_ascii_case("content-length") {
                                            content_length = v.trim().parse::<usize>().ok();
                                        }
                                    }
                                }
                            }
                        }

                        if let (Some(start), Some(len)) = (body_start, content_length) {
                            if buf.len() >= start + len {
                                count_c.fetch_add(1, Ordering::SeqCst);
                                if return_error {
                                    let resp = b"HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: 31\r\n\r\n{\"error\":\"invalid metric data\"}";
                                    let _ = socket.write_all(resp).await;
                                } else {
                                    let resp = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 20\r\n\r\n{\"partialSuccess\":{}}";
                                    let _ = socket.write_all(resp).await;
                                }
                                break;
                            }
                        }
                    }
                });
            }
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        Self {
            port,
            received_count,
        }
    }
}

async fn spawn_test_server() -> (String, tokio::task::JoinHandle<()>, AppState) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let stream_bus = StreamBus::new();
    let stream_manager = StreamManager::new();
    let rule_manager = RuleManager::new(stream_bus.clone());

    let mut config = KuiperConfig::default();
    config.basic.enable_private_net = true;
    config.basic.enable_ui = true;

    let state = AppState::new(
        "test_otlp_and_ui".to_string(),
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
async fn test_embedded_ui_status_endpoint() {
    let (base_url, _handle, _state) = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client.get(format!("{}/ui", base_url)).send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "text/html; charset=utf-8"
    );
    let html = resp.text().await.unwrap();
    assert!(html.contains("rekuiper"));
    assert!(html.contains("Pipelines &amp; Rules") || html.contains("Pipelines & Rules"));
    assert!(html.contains("Reload Manifest"));
}

#[tokio::test]
async fn test_ui_disabled_by_default() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let stream_bus = StreamBus::new();
    let stream_manager = StreamManager::new();
    let rule_manager = RuleManager::new(stream_bus.clone());

    // By default, enable_ui is false
    let config = KuiperConfig::default();
    assert!(!config.basic.enable_ui);

    let state = AppState::new(
        "test_ui_disabled".to_string(),
        config,
        stream_manager,
        TableManager::new(),
        rule_manager,
        stream_bus,
    );

    let app = create_router(state);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://{}/ui", local_addr))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);
    let body = resp.text().await.unwrap();
    assert!(body.contains("Web UI is disabled by default for security"));
}

#[tokio::test]
async fn test_ui_ip_allowlist_enforcement() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let stream_bus = StreamBus::new();
    let stream_manager = StreamManager::new();
    let rule_manager = RuleManager::new(stream_bus.clone());

    let mut config = KuiperConfig::default();
    config.basic.enable_ui = true;
    // Allowlist only internal 10.0.0.0/8 and 192.168.1.100
    config.basic.ui_ip_allowlist = vec!["10.0.0.0/8".to_string(), "192.168.1.100".to_string()];

    let state = AppState::new(
        "test_ui_ip_allowlist".to_string(),
        config,
        stream_manager,
        TableManager::new(),
        rule_manager,
        stream_bus,
    );

    let app = create_router(state);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = reqwest::Client::new();

    // 1. Request with disallowed IP in X-Forwarded-For -> 403 Forbidden
    let blocked_resp = client
        .get(format!("http://{}/ui", local_addr))
        .header("X-Forwarded-For", "203.0.113.50")
        .send()
        .await
        .unwrap();
    assert_eq!(blocked_resp.status(), reqwest::StatusCode::FORBIDDEN);
    let body = blocked_resp.text().await.unwrap();
    assert!(body.contains("Access denied: client IP"));

    // 2. Request with allowed IP (10.5.20.1) in X-Forwarded-For -> 200 OK
    let allowed_resp = client
        .get(format!("http://{}/ui", local_addr))
        .header("X-Forwarded-For", "10.5.20.1")
        .send()
        .await
        .unwrap();
    assert_eq!(allowed_resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_ui_password_authentication() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let stream_bus = StreamBus::new();
    let stream_manager = StreamManager::new();
    let rule_manager = RuleManager::new(stream_bus.clone());

    let mut config = KuiperConfig::default();
    config.basic.enable_ui = true;
    config.basic.ui_password = Some("SuperSecretEdgePassword123".to_string());

    let state = AppState::new(
        "test_ui_auth".to_string(),
        config,
        stream_manager,
        TableManager::new(),
        rule_manager,
        stream_bus,
    );

    let app = create_router(state);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = reqwest::Client::new();

    // 1. Check auth-status endpoint
    let status_resp = client
        .get(format!("http://{}/api/ui/auth-status", local_addr))
        .send()
        .await
        .unwrap();
    assert_eq!(status_resp.status(), reqwest::StatusCode::OK);
    let status_json: serde_json::Value = status_resp.json().await.unwrap();
    assert_eq!(status_json["auth_required"], true);
    assert_eq!(status_json["has_ui_password"], true);

    // 2. Attempt login with wrong password -> 401 Unauthorized
    let bad_login = client
        .post(format!("http://{}/api/ui/login", local_addr))
        .json(&serde_json::json!({ "token": "wrongpassword" }))
        .send()
        .await
        .unwrap();
    assert_eq!(bad_login.status(), reqwest::StatusCode::UNAUTHORIZED);

    // 3. Attempt login with correct password -> 200 OK
    let good_login = client
        .post(format!("http://{}/api/ui/login", local_addr))
        .json(&serde_json::json!({ "token": "SuperSecretEdgePassword123" }))
        .send()
        .await
        .unwrap();
    assert_eq!(good_login.status(), reqwest::StatusCode::OK);

    // 4. Test accessing protected REST endpoint with and without token
    let unauth_streams = client
        .get(format!("http://{}/streams", local_addr))
        .send()
        .await
        .unwrap();
    assert_eq!(unauth_streams.status(), reqwest::StatusCode::UNAUTHORIZED);

    let auth_streams = client
        .get(format!("http://{}/streams", local_addr))
        .header("Authorization", "Bearer SuperSecretEdgePassword123")
        .send()
        .await
        .unwrap();
    assert_eq!(auth_streams.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_otlp_metrics_and_logs_stream_pipeline() {
    let mock_receiver = MockOtlpReceiver::start(false).await;
    let (base_url, _handle, _state) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 1. Create OTLP stream
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM otlp () WITH (TYPE=\"otlp\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 2. Create rule routing high CPU metric to mock downstream OTLP sink
    let rule_json = json!({
        "id": "rule_otlp_cpu_alerts",
        "sql": "SELECT name, value, signal, attributes FROM otlp WHERE name = 'system.cpu.utilization' AND value > 0.5",
        "actions": [
            {
                "otlp": {
                    "endpoint": format!("http://127.0.0.1:{}/v1/metrics", mock_receiver.port),
                    "signal": "metric"
                }
            }
        ]
    });

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&rule_json)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // 3. Post standard OTLP/HTTP JSON metric batch with protojson stringified timestamp and value
    let otlp_payload = json!({
        "resourceMetrics": [
            {
                "resource": {
                    "attributes": [
                        { "key": "service.name", "value": { "stringValue": "edge-gw-01" } }
                    ]
                },
                "scopeMetrics": [
                    {
                        "metrics": [
                            {
                                "name": "system.cpu.utilization",
                                "unit": "1",
                                "gauge": {
                                    "dataPoints": [
                                        {
                                            "asDouble": "0.85",
                                            "timeUnixNano": "1760478000000000000",
                                            "attributes": [
                                                { "key": "cpu", "value": { "stringValue": "0" } }
                                            ]
                                        }
                                    ]
                                }
                            }
                        ]
                    }
                ]
            }
        ]
    });

    let push_resp = client
        .post(format!("{}/v1/metrics", base_url))
        .json(&otlp_payload)
        .send()
        .await
        .unwrap();
    assert_eq!(push_resp.status(), reqwest::StatusCode::OK);

    // 4. Verify downstream OTLP receiver received exported alert
    let mut received = false;
    for _ in 0..20 {
        if mock_receiver.received_count.load(Ordering::SeqCst) > 0 {
            received = true;
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
    assert!(
        received,
        "Downstream mock OTLP receiver did not receive exported metric"
    );
}

#[tokio::test]
async fn test_otlp_malformed_payload_routes_to_dlq() {
    let (base_url, _handle, state) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 1. Create stream
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM otlp () WITH (TYPE=\"otlp\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 2. Create rule with DLQ configured
    let rule_json = json!({
        "id": "rule_otlp_dlq_test",
        "sql": "SELECT * FROM otlp",
        "actions": [{ "log": {} }],
        "options": {
            "onError": "dlq",
            "dlq": {
                "action": {
                    "memory": {
                        "topic": "dlq_otlp_sink"
                    }
                }
            }
        }
    });

    let mut dlq_rx = state.stream_bus.subscribe("dlq_otlp_sink");

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&rule_json)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // 3. Post corrupted JSON payload to /v1/metrics
    let push_resp = client
        .post(format!("{}/v1/metrics", base_url))
        .header("content-type", "application/json")
        .body("NOT_VALID_JSON{foo:bar")
        .send()
        .await
        .unwrap();
    // HTTP response is 400 Bad Request
    assert_eq!(push_resp.status(), reqwest::StatusCode::BAD_REQUEST);

    // 4. Verify DLQ received the poison pill under DlqStage::CodecDecode
    let dlq_rec = tokio::time::timeout(tokio::time::Duration::from_secs(3), dlq_rx.recv())
        .await
        .expect("timed out waiting for DLQ OTLP record")
        .expect("channel closed");

    let stage = dlq_rec.data.get("stage").and_then(|v| v.as_str()).unwrap();
    assert_eq!(stage, "CodecDecode");

    let error_msg = dlq_rec.data.get("error").and_then(|v| v.as_str()).unwrap();
    assert!(
        error_msg.contains("Invalid OTLP JSON payload format")
            || error_msg.contains("expected value")
    );
}

#[tokio::test]
async fn test_otlp_sink_400_error_routes_to_dlq() {
    // Receiver configured to return HTTP 400 Bad Request
    let mock_receiver = MockOtlpReceiver::start(true).await;
    let (base_url, _handle, state) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 1. Create memory source stream
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM in_stream () WITH (TYPE=\"memory\", FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 2. Create rule exporting to mock receiver with DLQ
    let rule_json = json!({
        "id": "rule_otlp_sink_dlq",
        "sql": "SELECT name, value FROM in_stream",
        "actions": [
            {
                "otlp": {
                    "endpoint": format!("http://127.0.0.1:{}/v1/metrics", mock_receiver.port),
                    "signal": "metric"
                }
            }
        ],
        "options": {
            "onError": "dlq",
            "dlq": {
                "action": {
                    "memory": {
                        "topic": "dlq_otlp_sink_out"
                    }
                }
            }
        }
    });

    let mut dlq_rx = state.stream_bus.subscribe("dlq_otlp_sink_out");

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&rule_json)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // 3. Publish record to input stream
    let in_tx = state.stream_bus.get_or_create("in_stream");
    let mut data = std::collections::HashMap::new();
    data.insert("name".to_string(), json!("temp.celsius"));
    data.insert("value".to_string(), json!(42.0));
    let _ = in_tx
        .send(rekuiper_core::model::StreamRecord::new(data))
        .await;

    // 4. Verify permanent HTTP 400 maps to DlqStage::SinkDelivery
    let dlq_rec = tokio::time::timeout(tokio::time::Duration::from_secs(3), dlq_rx.recv())
        .await
        .expect("timed out waiting for DLQ OTLP sink record")
        .expect("channel closed");

    let stage = dlq_rec.data.get("stage").and_then(|v| v.as_str()).unwrap();
    assert_eq!(stage, "SinkDelivery");

    let sink_type = dlq_rec
        .data
        .get("sink_type")
        .and_then(|v| v.as_str())
        .unwrap();
    assert_eq!(sink_type, "otlp");

    let error_msg = dlq_rec.data.get("error").and_then(|v| v.as_str()).unwrap();
    assert!(error_msg.contains("HTTP 400"));
}

#[tokio::test]
async fn test_ui_ip_allowlist_fails_closed_and_jwt_coexistence() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let stream_bus = StreamBus::new();
    let stream_manager = StreamManager::new();
    let rule_manager = RuleManager::new(stream_bus.clone());

    let mut config = KuiperConfig::default();
    config.basic.enable_ui = true;
    config.basic.ui_ip_allowlist = vec!["127.0.0.1/32".to_string()];
    // Enable both basic.authentication (JWT) and ui_password simultaneously
    config.basic.authentication = true;
    config.basic.ui_password = Some("CoexistingAdminToken123".to_string());

    let state = AppState::new(
        "test_ui_coexist".to_string(),
        config,
        stream_manager,
        TableManager::new(),
        rule_manager,
        stream_bus,
    );

    let app = create_router(state);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = reqwest::Client::new();

    // 1. Without ConnectInfo or X-Forwarded-For, allowlist must fail closed (403 Forbidden), NOT default to 127.0.0.1
    let no_ip_resp = client
        .get(format!("http://{}/ui", local_addr))
        .send()
        .await
        .unwrap();
    assert_eq!(no_ip_resp.status(), reqwest::StatusCode::FORBIDDEN);

    // 2. With basic.authentication = true AND ui_password set, Bearer <ui_password> must succeed
    let auth_resp = client
        .get(format!("http://{}/streams", local_addr))
        .header("Authorization", "Bearer CoexistingAdminToken123")
        .send()
        .await
        .unwrap();
    assert_eq!(auth_resp.status(), reqwest::StatusCode::OK);
}
