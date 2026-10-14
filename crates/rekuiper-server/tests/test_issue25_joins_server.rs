use rekuiper_conf::KuiperConfig;
use rekuiper_core::manager::{RuleManager, StreamManager, TableManager};
use rekuiper_core::StreamBus;
use rekuiper_server::routes::{create_router, AppState};
use serde_json::json;
use tokio::net::TcpListener;

async fn spawn_test_server_with_state() -> (String, tokio::task::JoinHandle<()>, AppState) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind ephemeral port");
    let local_addr = listener.local_addr().unwrap();

    let stream_bus = StreamBus::new();
    let stream_manager = StreamManager::new();
    let rule_manager = RuleManager::new(stream_bus.clone());

    let state = AppState::new(
        "test_issue25".to_string(),
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
async fn test_defect4_reject_join_without_window_for_multiple_streams() {
    let (base_url, _handle, _state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    // Create stream a and stream b
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM a () WITH (TYPE=\"memory\", FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM b () WITH (TYPE=\"memory\", FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 1. Attempt to create stream-stream join without window: must be rejected with 400 Bad Request
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_stream_join_no_window",
            "sql": "SELECT a.id, bv FROM a INNER JOIN b ON a.id = b.id",
            "actions": [{"log": {}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("a time window or count window is required to join multiple streams"),
        "unexpected error body: {}",
        body
    );

    // 2. Validate rule API must also reject stream-stream join without window
    let resp = client
        .post(format!("{}/rules/validate", base_url))
        .json(&json!({
            "id": "rule_stream_join_no_window",
            "sql": "SELECT a.id, bv FROM a INNER JOIN b ON a.id = b.id",
            "actions": [{"log": {}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("a time window or count window is required to join multiple streams"),
        "unexpected validate error body: {}",
        body
    );

    // 3. Create a table t; stream-table join WITHOUT window must be ACCEPTED
    let resp = client
        .post(format!("{}/tables", base_url))
        .json(&json!({
            "sql": "CREATE TABLE t (id BIGINT, label STRING) WITH (DATASOURCE=\"j/t\", FORMAT=\"json\", TYPE=\"memory\", KEY=\"id\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_stream_table_join_allowed",
            "sql": "SELECT a.id, label FROM a INNER JOIN t ON a.id = t.id",
            "actions": [{"log": {}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
}

#[tokio::test]
async fn test_defect1_stream_table_lookup_join() {
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    // HTTP injection publishes under the stream name, so no memory bus alias is needed.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s () WITH (TYPE=\"memory\", FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Create table t with key and retain_size
    let resp = client
        .post(format!("{}/tables", base_url))
        .json(&json!({
            "sql": "CREATE TABLE t (id BIGINT, label STRING) WITH (DATASOURCE=\"j/t\", FORMAT=\"json\", TYPE=\"memory\", KEY=\"id\", RETAIN_SIZE=\"10\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let mut sink_rx = state.stream_bus.subscribe("sink_stream_table");

    // Create rule: SELECT s.id AS id, v, label FROM s INNER JOIN t ON s.id = t.id
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_st_join",
            "sql": "SELECT s.id AS id, v, label FROM s INNER JOIN t ON s.id = t.id",
            "actions": [{"memory": {"topic": "sink_stream_table"}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // Send table rows via stream bus on table's datasource topic
    let _ = state.stream_bus.publish(
        "j/t",
        rekuiper_core::StreamRecord::new(
            [
                ("id".to_string(), json!(1)),
                ("label".to_string(), json!("one")),
            ]
            .into_iter()
            .collect(),
        ),
    );
    let _ = state.stream_bus.publish(
        "j/t",
        rekuiper_core::StreamRecord::new(
            [
                ("id".to_string(), json!(2)),
                ("label".to_string(), json!("two")),
            ]
            .into_iter()
            .collect(),
        ),
    );

    // Give the table background listener a moment to ingest
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Send stream row for id 1: {"id":1, "v":10}
    let resp = client
        .post(format!("{}/streams/s/data", base_url))
        .json(&json!({"id": 1, "v": 10}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let rec = tokio::time::timeout(std::time::Duration::from_secs(2), sink_rx.recv())
        .await
        .expect("timed out waiting for sink output")
        .expect("sink closed");
    assert_eq!(rec.data.get("id"), Some(&json!(1)));
    assert_eq!(rec.data.get("v"), Some(&json!(10)));
    assert_eq!(rec.data.get("label"), Some(&json!("one")));

    // Send stream row for id 2: {"id":2, "v":20}
    let resp = client
        .post(format!("{}/streams/s/data", base_url))
        .json(&json!({"id": 2, "v": 20}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let rec = tokio::time::timeout(std::time::Duration::from_secs(2), sink_rx.recv())
        .await
        .expect("timed out waiting for sink output")
        .expect("sink closed");
    assert_eq!(rec.data.get("id"), Some(&json!(2)));
    assert_eq!(rec.data.get("v"), Some(&json!(20)));
    assert_eq!(rec.data.get("label"), Some(&json!("two")));

    // Send stream row for id 3 (unmatched in table): should produce no output for INNER join
    let resp = client
        .post(format!("{}/streams/s/data", base_url))
        .json(&json!({"id": 3, "v": 30}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), sink_rx.recv())
            .await
            .is_err(),
        "unmatched inner join row must not emit"
    );

    // Now insert table row for id 3
    let _ = state.stream_bus.publish(
        "j/t",
        rekuiper_core::StreamRecord::new(
            [
                ("id".to_string(), json!(3)),
                ("label".to_string(), json!("three")),
            ]
            .into_iter()
            .collect(),
        ),
    );
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Send stream row for id 3 again: {"id":3, "v":31} -> now matches!
    let resp = client
        .post(format!("{}/streams/s/data", base_url))
        .json(&json!({"id": 3, "v": 31}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let rec = tokio::time::timeout(std::time::Duration::from_secs(2), sink_rx.recv())
        .await
        .expect("timed out waiting for sink output")
        .expect("sink closed");
    assert_eq!(rec.data.get("id"), Some(&json!(3)));
    assert_eq!(rec.data.get("v"), Some(&json!(31)));
    assert_eq!(rec.data.get("label"), Some(&json!("three")));
}

#[tokio::test]
async fn test_defect2_event_time_window_stream_join_pairing() {
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    // Create stream a with TIMESTAMP="ts"
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM a () WITH (TYPE=\"memory\", FORMAT=\"json\", TIMESTAMP=\"ts\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Create stream b with TIMESTAMP="ts"
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM b () WITH (TYPE=\"memory\", FORMAT=\"json\", TIMESTAMP=\"ts\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let mut sink_rx = state.stream_bus.subscribe("sink_et_join");

    // Create rule:
    // SELECT a.id AS id, av, bv FROM a INNER JOIN b ON a.id = b.id GROUP BY TUMBLINGWINDOW(ss, 10)
    // with isEventTime: true, lateTolerance: 0
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_et_join",
            "sql": "SELECT a.id AS id, av, bv FROM a INNER JOIN b ON a.id = b.id GROUP BY TUMBLINGWINDOW(ss, 10)",
            "actions": [{"memory": {"topic": "sink_et_join"}}],
            "options": {"isEventTime": true, "lateTolerance": 0}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // Stream a receives ids 0-5 with timestamps 1000..5000 (all within [0, 10000))
    for i in 0..=5 {
        let ts = 1000 + i * 500;
        let resp = client
            .post(format!("{}/streams/a/data", base_url))
            .json(&json!({"id": i, "av": format!("a{}", i), "ts": ts}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }

    // Stream b receives ids 1, 2, 2, 4, 7 with timestamps in [0, 10000)
    for (i, (id, ts)) in [(1, 1200), (2, 2100), (2, 2200), (4, 3500), (7, 7000)]
        .into_iter()
        .enumerate()
    {
        let resp = client
            .post(format!("{}/streams/b/data", base_url))
            .json(&json!({"id": id, "bv": format!("b{}_{}", id, i), "ts": ts}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }

    // Advance both streams past 10000 ms to trigger the tumbling window [0, 10000)
    let resp = client
        .post(format!("{}/streams/a/data", base_url))
        .json(&json!({"id": 99, "av": "closer_a", "ts": 10000}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .post(format!("{}/streams/b/data", base_url))
        .json(&json!({"id": 99, "bv": "closer_b", "ts": 10000}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // INNER JOIN in eKuiper 2.4.1 emits 4 rows: id 1, id 2, id 2, id 4!
    let mut matched_ids = Vec::new();
    for _ in 0..4 {
        let rec = tokio::time::timeout(std::time::Duration::from_secs(2), sink_rx.recv())
            .await
            .expect("timed out waiting for matched join output")
            .expect("sink closed");
        let id = rec.data.get("id").and_then(|v| v.as_i64()).unwrap();
        matched_ids.push(id);
    }
    matched_ids.sort();
    assert_eq!(matched_ids, vec![1, 2, 2, 4]);
}

#[tokio::test]
async fn test_stream_to_stream_asof_join_stateless() {
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    // Create fast stream s and slow stream r
    client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s () WITH (TYPE=\"memory\", FORMAT=\"json\", TIMESTAMP=\"ts\")"
        }))
        .send()
        .await
        .unwrap();

    client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM r () WITH (TYPE=\"memory\", FORMAT=\"json\", TIMESTAMP=\"ts\")"
        }))
        .send()
        .await
        .unwrap();

    let mut sink_rx = state.stream_bus.subscribe("sink_asof_out");

    // Stateless ASOF JOIN without window
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_asof_stateless",
            "sql": "SELECT s.id AS id, s.val AS s_val, r.price AS r_price FROM s ASOF JOIN r ON s.id = r.id",
            "actions": [{"memory": {"topic": "sink_asof_out"}}],
            "options": {"isEventTime": true}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // Ingest right stream events: id 1 at ts 100 with price 50, and at ts 200 with price 60
    client
        .post(format!("{}/streams/r/data", base_url))
        .json(&json!({"id": 1, "price": 50, "ts": 100}))
        .send()
        .await
        .unwrap();

    client
        .post(format!("{}/streams/r/data", base_url))
        .json(&json!({"id": 1, "price": 60, "ts": 200}))
        .send()
        .await
        .unwrap();

    // Small delay to ensure right stream forwarder processes the snapshot
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Ingest left stream event: id 1 at ts 250 with val 10
    // Should match latest right event <= 250 (which is ts 200, price 60)
    client
        .post(format!("{}/streams/s/data", base_url))
        .json(&json!({"id": 1, "val": 10, "ts": 250}))
        .send()
        .await
        .unwrap();

    let rec = tokio::time::timeout(std::time::Duration::from_secs(3), sink_rx.recv())
        .await
        .expect("timed out waiting for asof output")
        .expect("sink closed");

    assert_eq!(rec.data.get("id"), Some(&json!(1)));
    assert_eq!(rec.data.get("s_val"), Some(&json!(10)));
    assert_eq!(rec.data.get("r_price"), Some(&json!(60)));
}
