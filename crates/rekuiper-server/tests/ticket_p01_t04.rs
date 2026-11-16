use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use rekuiper_conf::KuiperConfig;
use rekuiper_core::{RuleManager, StreamBus, StreamManager, TableManager};
use rekuiper_server::routes::create_router;
use rekuiper_server::state::AppState;
use serde_json::{json, Value};
use tower::ServiceExt;

fn build_test_state() -> AppState {
    let bus = StreamBus::new();
    let stream_mgr = StreamManager::new();
    let table_mgr = TableManager::new();
    let rule_mgr = RuleManager::new(bus.clone());
    AppState::new(
        "test-version".to_string(),
        KuiperConfig::default(),
        stream_mgr,
        table_mgr,
        rule_mgr,
        bus,
    )
}

async fn json_body(resp: axum::response::Response) -> Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn validate_explain_and_create_share_typed_planner_and_redact_secrets() {
    let state = build_test_state();
    let app = create_router(state);

    // 1. Create a non-replayable memory stream
    let create_stream = Request::builder()
        .method("POST")
        .uri("/streams")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "sql": "CREATE STREAM mem_in (temp float) WITH (TYPE=\"memory\")"
            })
            .to_string(),
        ))
        .unwrap();
    let resp = app.clone().oneshot(create_stream).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // 2. Durable rule over non-replayable stream must fail in BOTH /rules/validate and /rules (before tasks start)
    let invalid_rule = json!({
        "id": "durable_mem_rule",
        "sql": "SELECT temp FROM mem_in",
        "options": {
            "deliveryGuarantee": "at_least_once"
        },
        "actions": [{
            "mqtt": {
                "server": "tcp://user:supersecret@broker:1883",
                "topic": "sensors/temp",
                "password": "my-mqtt-password"
            }
        }]
    });

    let validate_req = Request::builder()
        .method("POST")
        .uri("/rules/validate")
        .header("content-type", "application/json")
        .body(Body::from(invalid_rule.to_string()))
        .unwrap();
    let validate_resp = app.clone().oneshot(validate_req).await.unwrap();
    assert_eq!(validate_resp.status(), StatusCode::BAD_REQUEST);
    let validate_json = json_body(validate_resp).await;
    assert_eq!(validate_json["error"], 1000);
    assert!(validate_json["diagnostics"]
        .as_array()
        .is_some_and(|d| !d.is_empty() && d[0]["code"] == "PLAN_MISSING_REPLAY"));
    assert_eq!(validate_json["limiting_component"], "source:mem_in");

    let create_req = Request::builder()
        .method("POST")
        .uri("/rules")
        .header("content-type", "application/json")
        .body(Body::from(invalid_rule.to_string()))
        .unwrap();
    let create_resp = app.clone().oneshot(create_req).await.unwrap();
    assert_eq!(create_resp.status(), StatusCode::BAD_REQUEST);

    // 3. Valid lightweight rule with secrets preserves existing validate/explain envelopes and redacts credentials
    let valid_rule = json!({
        "id": "lightweight_secret_rule",
        "sql": "SELECT temp FROM mem_in WHERE temp > 10",
        "options": {
            "api_key": "top-secret-key"
        },
        "actions": [{
            "mqtt": {
                "server": "tcp://alice:s3cr3t@broker:1883",
                "topic": "sensors/temp",
                "password": "raw-password-123"
            }
        }]
    });

    let validate_ok_req = Request::builder()
        .method("POST")
        .uri("/rules/validate")
        .header("content-type", "application/json")
        .body(Body::from(valid_rule.to_string()))
        .unwrap();
    let validate_ok_resp = app.clone().oneshot(validate_ok_req).await.unwrap();
    assert_eq!(validate_ok_resp.status(), StatusCode::OK);
    let validate_ok_json = json_body(validate_ok_resp).await;
    assert_eq!(validate_ok_json["valid"], true);
    assert_eq!(validate_ok_json["sources"], json!(["mem_in"]));
    assert_eq!(
        validate_ok_json["effective_guarantees"]["delivery"],
        "best_effort"
    );

    let create_ok_req = Request::builder()
        .method("POST")
        .uri("/rules")
        .header("content-type", "application/json")
        .body(Body::from(valid_rule.to_string()))
        .unwrap();
    let create_ok_resp = app.clone().oneshot(create_ok_req).await.unwrap();
    assert_eq!(create_ok_resp.status(), StatusCode::CREATED);

    let explain_req = Request::builder()
        .method("GET")
        .uri("/rules/lightweight_secret_rule/explain")
        .body(Body::empty())
        .unwrap();
    let explain_resp = app.clone().oneshot(explain_req).await.unwrap();
    assert_eq!(explain_resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(explain_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let explain_text = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(!explain_text.contains("raw-password-123"));
    assert!(!explain_text.contains("s3cr3t"));
    assert!(!explain_text.contains("top-secret-key"));
    let explain_json: Value = serde_json::from_str(&explain_text).unwrap();
    assert_eq!(explain_json["rule"], "lightweight_secret_rule");
    assert_eq!(explain_json["source"], "mem_in");
    assert_eq!(
        explain_json["plan"]["redacted_actions"][0]["mqtt"]["password"],
        "[REDACTED]"
    );
    assert_eq!(
        explain_json["plan"]["redacted_options"]["api_key"],
        "[REDACTED]"
    );
}
