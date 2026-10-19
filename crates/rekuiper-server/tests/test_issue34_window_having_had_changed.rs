use indexmap::IndexMap;
use rekuiper_conf::KuiperConfig;
use rekuiper_core::manager::{RuleManager, StreamManager, TableManager};
use rekuiper_core::{StreamBus, StreamRecord};
use rekuiper_server::routes::{create_router, AppState};
use serde_json::json;
use std::time::Duration;
use tokio::net::TcpListener;

async fn spawn_test_server() -> (String, tokio::task::JoinHandle<()>, AppState) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind ephemeral port");
    let local_addr = listener.local_addr().unwrap();

    let stream_bus = StreamBus::new();
    let stream_manager = StreamManager::new();
    let rule_manager = RuleManager::new(stream_bus.clone());

    let state = AppState::new(
        "test_issue34".to_string(),
        KuiperConfig::default(),
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
async fn test_count_window_having_had_changed() {
    let (base_url, _handle, state) = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s () WITH (TYPE=\"memory\", FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let mut sink_rx = state.stream_bus.subscribe("sink_cw_had_changed");

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_cw_had_changed",
            "sql": "SELECT latest(val) AS v FROM s GROUP BY COUNTWINDOW(2) HAVING had_changed(latest(val))",
            "actions": [{"memory": {"topic": "sink_cw_had_changed"}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Window 1: val = 10, 10 -> aggregate latest(val) is 10. First window evaluation: had_changed is true.
    let _ = state.stream_bus.publish(
        "s",
        StreamRecord::new(IndexMap::from([("val".to_string(), json!(10))])),
    );
    let _ = state.stream_bus.publish(
        "s",
        StreamRecord::new(IndexMap::from([("val".to_string(), json!(10))])),
    );

    let rec1 = tokio::time::timeout(Duration::from_secs(2), sink_rx.recv())
        .await
        .expect("timed out waiting for window 1")
        .expect("channel closed");
    assert_eq!(rec1.data.get("v"), Some(&json!(10)));

    // Window 2: val = 10, 10 -> aggregate latest(val) is 10 (unchanged from Window 1).
    // had_changed should return false, so HAVING filters this window output.
    let _ = state.stream_bus.publish(
        "s",
        StreamRecord::new(IndexMap::from([("val".to_string(), json!(10))])),
    );
    let _ = state.stream_bus.publish(
        "s",
        StreamRecord::new(IndexMap::from([("val".to_string(), json!(10))])),
    );

    let res_empty = tokio::time::timeout(Duration::from_millis(200), sink_rx.recv()).await;
    assert!(
        res_empty.is_err(),
        "repeated identical window result should be filtered by had_changed in HAVING"
    );

    // Window 3: val = 20, 20 -> aggregate latest(val) is 20 (changed from 10).
    // had_changed should return true, so Window 3 emits.
    let _ = state.stream_bus.publish(
        "s",
        StreamRecord::new(IndexMap::from([("val".to_string(), json!(20))])),
    );
    let _ = state.stream_bus.publish(
        "s",
        StreamRecord::new(IndexMap::from([("val".to_string(), json!(20))])),
    );

    let rec3 = tokio::time::timeout(Duration::from_secs(2), sink_rx.recv())
        .await
        .expect("timed out waiting for window 3")
        .expect("channel closed");
    assert_eq!(rec3.data.get("v"), Some(&json!(20)));
}

#[tokio::test]
async fn test_hopping_window_having_had_changed_issue34_repro() {
    let (base_url, _handle, state) = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM demo () WITH (TYPE=\"memory\", FORMAT=\"json\", TIMESTAMP=\"ts\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let mut sink_rx = state.stream_bus.subscribe("sink_hw_issue34");

    // Exact query from Issue #34:
    // SELECT latest(self) AS self, latest(quality) AS quality, latest(freshness) AS freshness
    // FROM demo
    // GROUP BY HoppingWindow(ss, 2, 1)
    // HAVING had_changed(concat(latest(self), ":", latest(quality), ":", latest(freshness)))
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_hw_issue34",
            "sql": "SELECT latest(self) AS self, latest(quality) AS quality, latest(freshness) AS freshness FROM demo GROUP BY HoppingWindow(ss, 2, 1) HAVING had_changed(concat(latest(self), \":\", latest(quality), \":\", latest(freshness)))",
            "options": {
                "isEventTime": true,
                "lateTolerance": 0
            },
            "actions": [{"memory": {"topic": "sink_hw_issue34"}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send events matching Issue #34 reproduction:
    let events = vec![
        (500, 1, 10, 100),
        (1000, 1, 10, 100),
        (1500, 1, 10, 100),
        (2000, 1, 10, 100),
        (2500, 2, 20, 200),
        (3000, 2, 20, 200),
        (3500, 2, 20, 200),
        (4000, 3, 30, 300),
        (4500, 3, 30, 300),
        (6500, 3, 30, 300), // Watermark advance past all windows
    ];

    for (ts, s, q, f) in events {
        let _ = state.stream_bus.publish(
            "demo",
            StreamRecord::new(IndexMap::from([
                ("ts".to_string(), json!(ts)),
                ("self".to_string(), json!(s)),
                ("quality".to_string(), json!(q)),
                ("freshness".to_string(), json!(f)),
            ])),
        );
    }

    let mut outputs = Vec::new();
    while let Ok(Some(rec)) = tokio::time::timeout(Duration::from_millis(500), sink_rx.recv()).await
    {
        outputs.push(rec);
    }

    // Verify change detection:
    // Every consecutive output must have a different concat result.
    assert!(!outputs.is_empty(), "expected outputs");
    for i in 1..outputs.len() {
        let prev = format!(
            "{}:{}:{}",
            outputs[i - 1].data["self"],
            outputs[i - 1].data["quality"],
            outputs[i - 1].data["freshness"]
        );
        let curr = format!(
            "{}:{}:{}",
            outputs[i].data["self"], outputs[i].data["quality"], outputs[i].data["freshness"]
        );
        assert_ne!(
            prev, curr,
            "Adjacent window outputs must have changed due to HAVING had_changed"
        );
    }
}
