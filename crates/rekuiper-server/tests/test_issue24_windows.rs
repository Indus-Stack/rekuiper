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
        "test_issue24".to_string(),
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
async fn test_validation_group_by_requires_window() {
    let (base_url, _handle, _state) = spawn_test_server().await;
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

    // 1. Creating rule with GROUP BY but no window must fail with 400
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_gb_no_window",
            "sql": "SELECT dev, count(*) AS c FROM s GROUP BY dev",
            "actions": [{"log": {}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("select stmt group by should be used with window"),
        "unexpected body: {}",
        body
    );

    // 2. Validate API must also reject
    let resp = client
        .post(format!("{}/rules/validate", base_url))
        .json(&json!({
            "id": "rule_gb_no_window",
            "sql": "SELECT dev, count(*) AS c FROM s GROUP BY dev",
            "actions": [{"log": {}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("select stmt group by should be used with window"),
        "unexpected body: {}",
        body
    );
}

#[tokio::test]
async fn test_validation_event_time_requires_timestamp_option() {
    let (base_url, _handle, _state) = spawn_test_server().await;
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

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_et_no_ts",
            "sql": "SELECT count(*) AS c FROM s GROUP BY TUMBLINGWINDOW(ss, 5)",
            "options": {
                "isEventTime": true
            },
            "actions": [{"log": {}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("preprocessor is set to be event time but stream option TIMESTAMP not found"),
        "unexpected body: {}",
        body
    );
}

#[tokio::test]
async fn test_stateless_single_row_aggregate() {
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

    let mut sink_rx = state.stream_bus.subscribe("sink_stateless_agg");

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_stateless_agg",
            "sql": "SELECT count(*) AS c FROM s",
            "actions": [{"memory": {"topic": "sink_stateless_agg"}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    tokio::time::sleep(Duration::from_millis(50)).await;

    let _ = state.stream_bus.publish(
        "s",
        StreamRecord::new(
            [("a".to_string(), json!(1))]
                .into_iter()
                .collect::<IndexMap<_, _>>(),
        ),
    );

    let rec = tokio::time::timeout(Duration::from_secs(2), sink_rx.recv())
        .await
        .expect("timed out waiting for output")
        .expect("channel closed");
    assert_eq!(rec.data.get("c"), Some(&json!(1)));
}

#[tokio::test]
async fn test_tumbling_window_zero_duration_emits_nothing() {
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

    let mut sink_rx = state.stream_bus.subscribe("sink_zero_window");

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_zero_win",
            "sql": "SELECT count(*) AS c FROM s GROUP BY TUMBLINGWINDOW(ss, 0)",
            "actions": [{"memory": {"topic": "sink_zero_window"}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    tokio::time::sleep(Duration::from_millis(50)).await;

    for i in 0..5 {
        let _ = state.stream_bus.publish(
            "s",
            StreamRecord::new(
                [("a".to_string(), json!(i))]
                    .into_iter()
                    .collect::<IndexMap<_, _>>(),
            ),
        );
    }

    // Must timeout without any output emitted
    let res = tokio::time::timeout(Duration::from_millis(300), sink_rx.recv()).await;
    assert!(
        res.is_err(),
        "TUMBLINGWINDOW(ss, 0) should not emit any output"
    );
}

#[tokio::test]
async fn test_window_start_and_end_event_time() {
    let (base_url, _handle, state) = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s () WITH (TYPE=\"memory\", FORMAT=\"json\", TIMESTAMP=\"ts\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let mut sink_rx = state.stream_bus.subscribe("sink_bounds");

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_bounds",
            "sql": "SELECT count(*) AS c, window_start() AS ws, window_end() AS we FROM s GROUP BY TUMBLINGWINDOW(ss, 2)",
            "options": {
                "isEventTime": true,
                "lateTolerance": 0
            },
            "actions": [{"memory": {"topic": "sink_bounds"}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send events in window [0, 2000): ts = 500, ts = 1500
    let _ = state.stream_bus.publish(
        "s",
        StreamRecord::new(
            [("ts".to_string(), json!(500)), ("v".to_string(), json!(1))]
                .into_iter()
                .collect::<IndexMap<_, _>>(),
        ),
    );
    let _ = state.stream_bus.publish(
        "s",
        StreamRecord::new(
            [("ts".to_string(), json!(1500)), ("v".to_string(), json!(2))]
                .into_iter()
                .collect::<IndexMap<_, _>>(),
        ),
    );

    // Advance watermark past 2000 by sending ts = 2500
    let _ = state.stream_bus.publish(
        "s",
        StreamRecord::new(
            [("ts".to_string(), json!(2500)), ("v".to_string(), json!(3))]
                .into_iter()
                .collect::<IndexMap<_, _>>(),
        ),
    );

    let rec = tokio::time::timeout(Duration::from_secs(2), sink_rx.recv())
        .await
        .expect("timed out waiting for window output")
        .expect("channel closed");

    assert_eq!(rec.data.get("c"), Some(&json!(2)));
    assert_eq!(rec.data.get("ws"), Some(&json!(0)));
    assert_eq!(rec.data.get("we"), Some(&json!(2000)));
}

#[tokio::test]
async fn test_window_filter_clause() {
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

    let mut sink_rx = state.stream_bus.subscribe("sink_filter");

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_filter",
            "sql": "SELECT count(*) AS c FROM s GROUP BY COUNTWINDOW(3) FILTER (WHERE a > 10)",
            "actions": [{"memory": {"topic": "sink_filter"}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send rows: 5 (rejected), 15 (kept), 2 (rejected), 25 (kept), 35 (kept -> triggers count 3)
    let vals = vec![5, 15, 2, 25, 35];
    for v in vals {
        let _ = state.stream_bus.publish(
            "s",
            StreamRecord::new(
                [("a".to_string(), json!(v))]
                    .into_iter()
                    .collect::<IndexMap<_, _>>(),
            ),
        );
    }

    let rec = tokio::time::timeout(Duration::from_secs(2), sink_rx.recv())
        .await
        .expect("timed out waiting for filter count window output")
        .expect("channel closed");

    assert_eq!(rec.data.get("c"), Some(&json!(3)));
}

#[tokio::test]
async fn test_hopping_window_event_time() {
    let (base_url, _handle, state) = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s () WITH (TYPE=\"memory\", FORMAT=\"json\", TIMESTAMP=\"ts\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let mut sink_rx = state.stream_bus.subscribe("sink_hop");

    // HOPPINGWINDOW(ss, 10, 5): length = 10s, hop = 5s
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_hop",
            "sql": "SELECT count(*) AS c, window_start() AS ws, window_end() AS we FROM s GROUP BY HOPPINGWINDOW(ss, 10, 5)",
            "options": {
                "isEventTime": true,
                "lateTolerance": 0
            },
            "actions": [{"memory": {"topic": "sink_hop"}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send 5 events in first 5s (0..5000): ts = 1000, 2000, 3000, 4000, 4500
    for ts in [1000, 2000, 3000, 4000, 4500] {
        let _ = state.stream_bus.publish(
            "s",
            StreamRecord::new(
                [("ts".to_string(), json!(ts))]
                    .into_iter()
                    .collect::<IndexMap<_, _>>(),
            ),
        );
    }

    // Advance watermark past 5000 by sending ts = 5500. This triggers 1st hop [0, 5000)
    let _ = state.stream_bus.publish(
        "s",
        StreamRecord::new(
            [("ts".to_string(), json!(5500))]
                .into_iter()
                .collect::<IndexMap<_, _>>(),
        ),
    );

    let rec1 = tokio::time::timeout(Duration::from_secs(2), sink_rx.recv())
        .await
        .expect("timed out waiting for 1st hop")
        .expect("channel closed");

    assert_eq!(rec1.data.get("c"), Some(&json!(5)));
    assert_eq!(rec1.data.get("we"), Some(&json!(5000)));

    // Send 4 more events in 5000..10000: ts = 6000, 7000, 8000, 9000
    for ts in [6000, 7000, 8000, 9000] {
        let _ = state.stream_bus.publish(
            "s",
            StreamRecord::new(
                [("ts".to_string(), json!(ts))]
                    .into_iter()
                    .collect::<IndexMap<_, _>>(),
            ),
        );
    }

    // Advance watermark past 10000 by sending ts = 10500. 2nd hop covers [0, 10000) = 5 + 5 = 10 events
    let _ = state.stream_bus.publish(
        "s",
        StreamRecord::new(
            [("ts".to_string(), json!(10500))]
                .into_iter()
                .collect::<IndexMap<_, _>>(),
        ),
    );

    let rec2 = tokio::time::timeout(Duration::from_secs(2), sink_rx.recv())
        .await
        .expect("timed out waiting for 2nd hop")
        .expect("channel closed");

    assert_eq!(rec2.data.get("c"), Some(&json!(10)));
    assert_eq!(rec2.data.get("we"), Some(&json!(10000)));
}

#[tokio::test]
async fn test_session_window_event_time_gap() {
    let (base_url, _handle, state) = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s () WITH (TYPE=\"memory\", FORMAT=\"json\", TIMESTAMP=\"ts\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let mut sink_rx = state.stream_bus.subscribe("sink_sess");

    // SESSIONWINDOW(ss, 60, 5): timeout = 5s, max_duration = 60s
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_sess",
            "sql": "SELECT count(*) AS c, window_start() AS ws, window_end() AS we FROM s GROUP BY SESSIONWINDOW(ss, 60, 5)",
            "options": {
                "isEventTime": true,
                "lateTolerance": 0
            },
            "actions": [{"memory": {"topic": "sink_sess"}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 1st session: 30 events with 1s gap: ts = 1000, 2000, ..., 30000
    for i in 1..=30 {
        let _ = state.stream_bus.publish(
            "s",
            StreamRecord::new(
                [("ts".to_string(), json!(i * 1000))]
                    .into_iter()
                    .collect::<IndexMap<_, _>>(),
            ),
        );
    }

    // Send 30s gap: next event at ts = 60000 (> 5000 timeout, triggers emission of 1st session)
    let _ = state.stream_bus.publish(
        "s",
        StreamRecord::new(
            [("ts".to_string(), json!(60000))]
                .into_iter()
                .collect::<IndexMap<_, _>>(),
        ),
    );

    let rec1 = tokio::time::timeout(Duration::from_secs(2), sink_rx.recv())
        .await
        .expect("timed out waiting for 1st session")
        .expect("channel closed");

    assert_eq!(rec1.data.get("c"), Some(&json!(30)));
    assert_eq!(rec1.data.get("ws"), Some(&json!(1000)));
    assert_eq!(rec1.data.get("we"), Some(&json!(35000)));

    // 2nd session: 4 more events (5 events total with ts=60000)
    for i in 1..=4 {
        let _ = state.stream_bus.publish(
            "s",
            StreamRecord::new(
                [("ts".to_string(), json!(60000 + i * 1000))]
                    .into_iter()
                    .collect::<IndexMap<_, _>>(),
            ),
        );
    }

    // Trigger second session via gap > 5s
    let _ = state.stream_bus.publish(
        "s",
        StreamRecord::new(
            [("ts".to_string(), json!(80000))]
                .into_iter()
                .collect::<IndexMap<_, _>>(),
        ),
    );

    let rec2 = tokio::time::timeout(Duration::from_secs(2), sink_rx.recv())
        .await
        .expect("timed out waiting for 2nd session")
        .expect("channel closed");

    assert_eq!(rec2.data.get("c"), Some(&json!(5)));
    assert_eq!(rec2.data.get("ws"), Some(&json!(60000)));
    assert_eq!(rec2.data.get("we"), Some(&json!(69000)));
}

#[tokio::test]
async fn test_sliding_window_over_when() {
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

    let mut sink_rx = state.stream_bus.subscribe("sink_slide_when");

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_slide_when",
            "sql": "SELECT temp, count(*) AS c FROM s GROUP BY SLIDINGWINDOW(ss, 5) OVER (WHEN temp > 30)",
            "actions": [{"memory": {"topic": "sink_slide_when"}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send temp = 20 (does NOT trigger trigger condition)
    let _ = state.stream_bus.publish(
        "s",
        StreamRecord::new(
            [("temp".to_string(), json!(20))]
                .into_iter()
                .collect::<IndexMap<_, _>>(),
        ),
    );

    let res = tokio::time::timeout(Duration::from_millis(200), sink_rx.recv()).await;
    assert!(res.is_err(), "temp = 20 should not trigger window emission");

    // Send temp = 35 (triggers condition! window contains 20 and 35)
    let _ = state.stream_bus.publish(
        "s",
        StreamRecord::new(
            [("temp".to_string(), json!(35))]
                .into_iter()
                .collect::<IndexMap<_, _>>(),
        ),
    );

    let rec = tokio::time::timeout(Duration::from_secs(2), sink_rx.recv())
        .await
        .expect("timed out waiting for sliding window trigger")
        .expect("channel closed");

    assert_eq!(rec.data.get("c"), Some(&json!(2)));
}

#[tokio::test]
async fn test_state_window_two_conditions() {
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

    let mut sink_rx = state.stream_bus.subscribe("sink_state2");

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_state2",
            "sql": "SELECT count(*) AS c FROM s GROUP BY STATEWINDOW(a = 1, a = 5)",
            "actions": [{"memory": {"topic": "sink_state2"}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // a = 0: not on begin
    let _ = state.stream_bus.publish(
        "s",
        StreamRecord::new(
            [("a".to_string(), json!(0))]
                .into_iter()
                .collect::<IndexMap<_, _>>(),
        ),
    );

    // a = 1: starts window (1st)
    let _ = state.stream_bus.publish(
        "s",
        StreamRecord::new(
            [("a".to_string(), json!(1))]
                .into_iter()
                .collect::<IndexMap<_, _>>(),
        ),
    );

    // a = 3: buffered (2nd)
    let _ = state.stream_bus.publish(
        "s",
        StreamRecord::new(
            [("a".to_string(), json!(3))]
                .into_iter()
                .collect::<IndexMap<_, _>>(),
        ),
    );

    // a = 5: emits window (3rd)
    let _ = state.stream_bus.publish(
        "s",
        StreamRecord::new(
            [("a".to_string(), json!(5))]
                .into_iter()
                .collect::<IndexMap<_, _>>(),
        ),
    );

    let rec = tokio::time::timeout(Duration::from_secs(2), sink_rx.recv())
        .await
        .expect("timed out waiting for state window emit")
        .expect("channel closed");

    assert_eq!(rec.data.get("c"), Some(&json!(3)));
}

#[tokio::test]
async fn test_state_window_single_condition() {
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

    let mut sink_rx = state.stream_bus.subscribe("sink_state1");

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_state1",
            "sql": "SELECT a, count(*) AS c FROM s GROUP BY STATEWINDOW(had_changed(a))",
            "actions": [{"memory": {"topic": "sink_state1"}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send a = 10 (starts 1st window)
    let _ = state.stream_bus.publish(
        "s",
        StreamRecord::new(
            [("a".to_string(), json!(10))]
                .into_iter()
                .collect::<IndexMap<_, _>>(),
        ),
    );

    // Send a = 10 (buffered in 1st window)
    let _ = state.stream_bus.publish(
        "s",
        StreamRecord::new(
            [("a".to_string(), json!(10))]
                .into_iter()
                .collect::<IndexMap<_, _>>(),
        ),
    );

    // Send a = 20 (had_changed(a) triggers emission of 1st window!)
    let _ = state.stream_bus.publish(
        "s",
        StreamRecord::new(
            [("a".to_string(), json!(20))]
                .into_iter()
                .collect::<IndexMap<_, _>>(),
        ),
    );

    let rec = tokio::time::timeout(Duration::from_secs(2), sink_rx.recv())
        .await
        .expect("timed out waiting for single condition state window emit")
        .expect("channel closed");

    assert_eq!(rec.data.get("a"), Some(&json!(10)));
    assert_eq!(rec.data.get("c"), Some(&json!(2)));
}

#[tokio::test]
async fn test_event_time_sliding_delay_includes_arrivals_during_delay() {
    // Exercise both an exact deadline and a watermark that jumps past it.
    for closing_ts in [1100, 1200] {
        let (base, handle, state) = spawn_test_server().await;
        let client = reqwest::Client::new();
        let response = client.post(format!("{base}/streams")).json(&json!({
        "sql": "CREATE STREAM delayed () WITH (TYPE=\"memory\", FORMAT=\"json\", TIMESTAMP=\"ts\")"
    })).send().await.unwrap();
        assert!(response.status().is_success());
        let mut sink = state.stream_bus.subscribe("delayed_out");
        let response = client.post(format!("{base}/rules")).json(&json!({
        "id":"delay", "sql":"SELECT count(*) AS c, window_start() AS ws, window_end() AS we FROM delayed GROUP BY SLIDINGWINDOW(ms, 500, 100) OVER (WHEN fire = true)",
        "options":{"isEventTime":true}, "actions":[{"memory":{"topic":"delayed_out"}}]
    })).send().await.unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::CREATED);
        for (ts, fire) in [(1000, true), (1050, false)] {
            assert!(client
                .post(format!("{base}/streams/delayed/data"))
                .json(&json!({"ts":ts,"fire":fire}))
                .send()
                .await
                .unwrap()
                .status()
                .is_success());
        }
        assert!(tokio::time::timeout(Duration::from_millis(50), sink.recv())
            .await
            .is_err());
        assert!(client
            .post(format!("{base}/streams/delayed/data"))
            .json(&json!({"ts":closing_ts,"fire":false}))
            .send()
            .await
            .unwrap()
            .status()
            .is_success());
        let row = tokio::time::timeout(Duration::from_secs(2), sink.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            row.data["c"],
            json!(3),
            "delay must include rows after the triggering event"
        );
        assert_eq!(row.data["ws"], json!(500));
        assert_eq!(row.data["we"], json!(closing_ts));
        handle.abort();
    }
}
