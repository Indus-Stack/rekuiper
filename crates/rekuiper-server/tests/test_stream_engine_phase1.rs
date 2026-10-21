use axum::body::Body;
use axum::http::{Request, StatusCode};
use rekuiper_conf::KuiperConfig;
use rekuiper_core::model::{RecordMeta, StreamRecord};
use rekuiper_core::{RuleManager, StreamBus, StreamManager, TableManager};
use rekuiper_server::comms::CommsEvent;
use rekuiper_server::engine::operators::{
    RateLimitNode, RateLimitStrategy, SwitchNode, WatchdogNode,
};
use rekuiper_server::routes::create_router;
use rekuiper_server::state::AppState;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
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

#[tokio::test]
async fn test_phase1_record_meta_zero_copy_propagation() {
    // 1. Verify RecordMeta model and zero-copy clone across records
    let meta = Arc::new(
        RecordMeta::new()
            .with_topic("telemetry/sensors/factory1")
            .with_source("mqtt_gateway_01"),
    );

    let rec = StreamRecord::from_hashmap(
        [("voltage".to_string(), json!(220.5))]
            .into_iter()
            .collect(),
    )
    .with_meta(meta.clone());

    assert_eq!(
        rec.meta.as_ref().unwrap().topic.as_deref(),
        Some("telemetry/sensors/factory1")
    );
    assert_eq!(
        rec.meta.as_ref().unwrap().source.as_deref(),
        Some("mqtt_gateway_01")
    );

    // Zero-copy forwarded record shares same Arc
    let mut out_data = indexmap::IndexMap::new();
    out_data.insert("voltage_kv".to_string(), json!(0.2205));
    let out_rec = StreamRecord::with_timestamp_and_meta(out_data, rec.timestamp, rec.meta.clone());

    assert!(Arc::ptr_eq(
        rec.meta.as_ref().unwrap(),
        out_rec.meta.as_ref().unwrap()
    ));
}

#[tokio::test]
async fn test_phase1_switch_multi_port_selective_routing() {
    // 2. Verify Switch operator routes selectively per port (no broadcast fan-out)
    let mut props = HashMap::new();
    props.insert(
        "cases".to_string(),
        json!(["vibration > 80.0", "vibration > 40.0"]),
    );
    props.insert("default_port".to_string(), json!(2));

    let switch = SwitchNode::from_props(&props).expect("Valid switch definition");

    // Port 0: vibration = 95.0
    let r0 = StreamRecord::from_hashmap(
        [("vibration".to_string(), json!(95.0))]
            .into_iter()
            .collect(),
    );
    assert_eq!(switch.route(&r0), Some(0));

    // Port 1: vibration = 55.0
    let r1 = StreamRecord::from_hashmap(
        [("vibration".to_string(), json!(55.0))]
            .into_iter()
            .collect(),
    );
    assert_eq!(switch.route(&r1), Some(1));

    // Port 2: vibration = 15.0 (default port)
    let r2 = StreamRecord::from_hashmap(
        [("vibration".to_string(), json!(15.0))]
            .into_iter()
            .collect(),
    );
    assert_eq!(switch.route(&r2), Some(2));
}

#[tokio::test]
async fn test_phase1_rate_limit_and_watchdog_operators() {
    // 3. RateLimit: token bucket drop policy
    let rl = RateLimitNode::new(10.0, 3.0, RateLimitStrategy::Drop, 10);
    assert!(rl.acquire().await);
    assert!(rl.acquire().await);
    assert!(rl.acquire().await);
    // 4th immediate request drops
    assert!(!rl.acquire().await);
    assert_eq!(rl.dropped.load(std::sync::atomic::Ordering::Relaxed), 1);

    // 4. Watchdog: construct from props and verify timeout payload
    let mut wd_props = HashMap::new();
    wd_props.insert("timeout_ms".to_string(), json!(2500));
    wd_props.insert(
        "alert_payload".to_string(),
        json!({"status": "CRITICAL_HEARTBEAT_LOST"}),
    );
    let wd = WatchdogNode::from_props("wd_node_1", &wd_props);
    assert_eq!(wd.timeout, Duration::from_millis(2500));
    assert_eq!(wd.alert_payload["status"], "CRITICAL_HEARTBEAT_LOST");
}

#[tokio::test]
async fn test_phase1_wiretap_api_and_comms_bus() {
    let state = build_test_state();
    let app = create_router(state.clone());

    // 5. Test POST /rules/:id/tap/:node_id/enable
    let req = Request::builder()
        .method("POST")
        .uri("/rules/rule_press_4/tap/switch_vibration/enable")
        .body(Body::empty())
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(state
        .comms_hub
        .is_tap_active("rule_press_4", "switch_vibration"));

    // 6. Test GET /rules/:id/tap/:node_id/status
    let req = Request::builder()
        .method("GET")
        .uri("/rules/rule_press_4/tap/switch_vibration/status")
        .body(Body::empty())
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // 7. Test sample emission through CommsHub with active tap
    let mut comms_rx = state.comms_hub.subscribe();
    let test_map: indexmap::IndexMap<String, Value> = [
        ("temp".to_string(), json!(78.2)),
        ("vibration".to_string(), json!(92.4)),
    ]
    .into_iter()
    .collect();

    state
        .comms_hub
        .maybe_sample("rule_press_4", "switch_vibration", &test_map, 1700000000000);

    let sample_event = comms_rx.recv().await.expect("receive tuple sample");
    match sample_event {
        CommsEvent::TupleSample {
            rule_id,
            node_id,
            data,
            timestamp,
            dropped_since_last,
        } => {
            assert_eq!(rule_id, "rule_press_4");
            assert_eq!(node_id, "switch_vibration");
            assert_eq!(data["temp"], 78.2);
            assert_eq!(data["vibration"], 92.4);
            assert_eq!(timestamp, 1700000000000);
            assert_eq!(dropped_since_last, 0);
        }
        other => panic!("Unexpected event: {:?}", other),
    }

    // 8. Test POST /rules/:id/tap/:node_id/disable
    let req = Request::builder()
        .method("POST")
        .uri("/rules/rule_press_4/tap/switch_vibration/disable")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(!state
        .comms_hub
        .is_tap_active("rule_press_4", "switch_vibration"));
}
