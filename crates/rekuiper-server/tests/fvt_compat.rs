use rekuiper_conf::KuiperConfig;
use rekuiper_core::{RuleManager, StreamBus, StreamManager, TableManager};
use rekuiper_server::routes::{create_router, AppState};
use serde_json::json;
use tokio::net::TcpListener;

async fn spawn_test_server() -> (String, tokio::task::JoinHandle<()>) {
    let (base_url, handle, _) = spawn_test_server_with_state().await;
    (base_url, handle)
}

async fn spawn_test_server_with_state() -> (String, tokio::task::JoinHandle<()>, AppState) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind ephemeral port");
    let local_addr = listener.local_addr().unwrap();

    let stream_bus = StreamBus::new();
    let stream_manager = StreamManager::new();
    let rule_manager = RuleManager::new(stream_bus.clone());

    let state = AppState::new(
        "fvt".to_string(),
        KuiperConfig::default(),
        stream_manager,
        TableManager::new(),
        rule_manager,
        stream_bus,
    );

    // Seed OpenAPI smoke-test fixtures in test harness state (not production).
    let now = chrono::Utc::now();
    state.trace_manager.record_trace(
        "rule_openapi",
        rekuiper_server::state::TraceSpan {
            name: "rule_openapi".to_string(),
            trace_id: "trace_1".to_string(),
            span_id: "0000000000000001".to_string(),
            parent_span_id: "0000000000000000".to_string(),
            attribute: None,
            links: None,
            start_time: now.to_rfc3339(),
            end_time: (now + chrono::Duration::microseconds(150)).to_rfc3339(),
            rule_id: Some("rule_openapi".to_string()),
            child_span: vec![],
        },
    );
    let _ = state.task_manager.register_task("task_1".to_string());
    state
        .task_manager
        .update_status("task_1", "completed", "seeded task");

    let app = create_router(state.clone());
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (format!("http://{}", local_addr), handle, state)
}

#[tokio::test]
async fn test_fvt_server_ping_and_root() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 1. Ping test (matches fvt/server_test.go)
    let resp = client
        .get(format!("{}/ping", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 2. Root metadata test (matches fvt/server_test.go)
    let resp = client.get(&base_url).send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["version"], "fvt");
    assert!(json["os"].is_string());
    assert!(json["arch"].is_string());
    assert!(json["upTimeSeconds"].is_number());
    assert!(json["cpuUsage"].is_string());
    assert!(json["memoryUsed"].is_string());
    assert!(json["memoryTotal"].is_string());
}

#[tokio::test]
async fn test_fvt_stream_and_rule_lifecycle() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 1. Create stream
    let create_stream_payload = json!({
        "sql": "CREATE STREAM demo () WITH (DATASOURCE=\"demo\", FORMAT=\"json\")"
    });
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&create_stream_payload)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 2. List streams
    let resp = client
        .get(format!("{}/streams", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let streams: Vec<String> = resp.json().await.unwrap();
    assert!(streams.contains(&"demo".to_string()));

    // 3. Create rule
    let create_rule_payload = json!({
        "id": "rule_fvt_1",
        "sql": "SELECT * FROM demo",
        "actions": [{ "log": {} }]
    });
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&create_rule_payload)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 4. Check rule status
    let resp = client
        .get(format!("{}/rules/rule_fvt_1/status", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let status: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(status["status"], "running");

    // 5. Stop rule
    let resp = client
        .post(format!("{}/rules/rule_fvt_1/stop", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(format!("{}/rules/rule_fvt_1/status", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let status: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(status["status"], "stopped");

    // 6. Delete rule
    let resp = client
        .delete(format!("{}/rules/rule_fvt_1", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 7. Delete stream
    let resp = client
        .delete(format!("{}/streams/demo", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_http_push_data_ingestion() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 1. Create stream `demo` via POST /streams.
    let create_stream_payload = json!({
        "sql": "CREATE STREAM demo () WITH (DATASOURCE=\"demo\", FORMAT=\"json\")"
    });
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&create_stream_payload)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 2. Create rule `rule_filter` via POST /rules.
    let create_rule_payload = json!({
        "id": "rule_filter",
        "sql": "SELECT * FROM demo WHERE temp > 25",
        "actions": [{ "log": {} }]
    });
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&create_rule_payload)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 3. Ingest an array of objects via POST /streams/demo/data.
    let resp = client
        .post(format!("{}/streams/demo/data", base_url))
        .json(&json!([{"temp": 20.0}, {"temp": 30.0}]))
        .send()
        .await
        .unwrap();
    // 4. Check status 200 OK.
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body = resp.text().await.unwrap();
    assert_eq!(body, "Data ingested successfully.\n");

    // Single-object ingestion is also accepted.
    let resp = client
        .post(format!("{}/streams/demo/data", base_url))
        .json(&json!({"temp": 25.0}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 5. Post data to a non-existent stream and verify 404 NOT_FOUND.
    let resp = client
        .post(format!("{}/streams/no_such_stream/data", base_url))
        .json(&json!([{"temp": 20.0}]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
}

async fn fetch_rule_status(
    client: &reqwest::Client,
    base_url: &str,
    rule: &str,
) -> serde_json::Value {
    client
        .get(format!("{}/rules/{}/status", base_url, rule))
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap()
}

async fn wait_for_rule_status(
    client: &reqwest::Client,
    base_url: &str,
    rule: &str,
    want_source: u64,
    want_sink: u64,
) -> serde_json::Value {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let status = fetch_rule_status(client, base_url, rule).await;
        let source = status["sourceRecordsInTotal"].as_u64().unwrap_or(u64::MAX);
        let sink = status["sinkRecordsOutTotal"].as_u64().unwrap_or(u64::MAX);
        if source == want_source && sink == want_sink {
            return status;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for rule {} status source={} sink={} (got source={} sink={})",
            rule,
            want_source,
            want_sink,
            source,
            sink
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn test_rule_count_window_execution() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 1. Create stream `sensor_stream` via POST /streams.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM sensor_stream () WITH (DATASOURCE=\"sensor_stream\", FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 2. Create rule `rule_count_win` via POST /rules.
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_count_win",
            "sql": "SELECT count(*), avg(temp) FROM sensor_stream GROUP BY COUNTWINDOW(3)",
            "actions": [{ "log": {} }]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 3. Post 3 records via POST /streams/sensor_stream/data.
    let resp = client
        .post(format!("{}/streams/sensor_stream/data", base_url))
        .json(&json!([{"temp": 10.0}, {"temp": 20.0}, {"temp": 30.0}]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 4. The window fires once: 3 records in, 1 aggregate out.
    let status = wait_for_rule_status(&client, &base_url, "rule_count_win", 3, 1).await;
    assert_eq!(status["sourceRecordsInTotal"], 3);
    assert_eq!(status["sinkRecordsOutTotal"], 1);
}

#[tokio::test]
async fn test_rule_metrics_increment() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM metric_stream () WITH (DATASOURCE=\"metric_stream\", FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_metrics",
            "sql": "SELECT * FROM metric_stream WHERE temp > 25",
            "actions": [{ "log": {} }]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Non-matching record: source increments, sink does not.
    let resp = client
        .post(format!("{}/streams/metric_stream/data", base_url))
        .json(&json!({"temp": 10.0}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let status = wait_for_rule_status(&client, &base_url, "rule_metrics", 1, 0).await;
    assert_eq!(status["sourceRecordsInTotal"], 1);
    assert_eq!(status["sinkRecordsOutTotal"], 0);

    // Matching record: both counters increment.
    let resp = client
        .post(format!("{}/streams/metric_stream/data", base_url))
        .json(&json!({"temp": 30.0}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let status = wait_for_rule_status(&client, &base_url, "rule_metrics", 2, 1).await;
    assert_eq!(status["sourceRecordsInTotal"], 2);
    assert_eq!(status["sinkRecordsOutTotal"], 1);
}

#[tokio::test]
async fn test_file_sink_counter_follows_written_rows() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();
    let mut path = std::env::temp_dir();
    path.push(format!(
        "rekuiper-file-counter-{}-{}.jsonl",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let path_text = path.to_string_lossy().to_string();

    let resp = client.post(format!("{}/streams", base_url))
        .json(&json!({"sql": "CREATE STREAM file_metric_stream () WITH (TYPE=\"memory\", FORMAT=\"json\")"}))
        .send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(
            &json!({"id": "file_metric_rule", "sql": "SELECT * FROM file_metric_stream",
            "actions": [{"file": {"path": path_text}}]}),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    let resp = client
        .post(format!("{}/streams/file_metric_stream/data", base_url))
        .json(&json!({"id": "written"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    wait_for_rule_status(&client, &base_url, "file_metric_rule", 1, 1).await;
    let text = tokio::fs::read_to_string(&path).await.unwrap();
    let lines: Vec<_> = text.lines().collect();
    assert_eq!(lines.len(), 1);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(lines[0]).unwrap()["id"],
        "written"
    );
    let _ = tokio::fs::remove_file(path).await;
}

#[tokio::test]
async fn test_file_sink_flush_failure_does_not_count_delivery() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();
    let mut path = std::env::temp_dir();
    path.push(format!(
        "rekuiper-file-failure-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    tokio::fs::create_dir(&path).await.unwrap();
    let resp = client.post(format!("{}/streams", base_url))
        .json(&json!({"sql": "CREATE STREAM failure_stream () WITH (TYPE=\"memory\", FORMAT=\"json\")"}))
        .send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(
            &json!({"id": "failure_rule", "sql": "SELECT * FROM failure_stream",
            "actions": [{"file": {"path": path.to_string_lossy()}}]}),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    client
        .post(format!("{}/streams/failure_stream/data", base_url))
        .json(&json!({"id": 1}))
        .send()
        .await
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let status = fetch_rule_status(&client, &base_url, "failure_rule").await;
        if status["exceptionsTotal"].as_u64().unwrap_or(0) > 0 {
            assert_eq!(status["sinkRecordsOutTotal"], 0);
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "file flush did not report failure"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    client
        .post(format!("{}/rules/failure_rule/stop", base_url))
        .send()
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let _ = tokio::fs::remove_dir(path).await;
}

#[tokio::test]
async fn test_tables_and_details_lifecycle() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // Seed a stream so /streamdetails has content.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM demo () WITH (DATASOURCE=\"demo\", FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 1. Create table `my_table` via POST /tables.
    let resp = client
        .post(format!("{}/tables", base_url))
        .json(&json!({
            "sql": "CREATE TABLE my_table () WITH (DATASOURCE=\"my_table\", FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    assert_eq!(resp.text().await.unwrap(), "Table my_table is created.");

    // 2. Verify GET /tables includes "my_table".
    let resp = client
        .get(format!("{}/tables", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let tables: Vec<String> = resp.json().await.unwrap();
    assert!(tables.contains(&"my_table".to_string()));

    // 3. Verify GET /tabledetails contains the definition of "my_table".
    let resp = client
        .get(format!("{}/tabledetails", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let details: Vec<serde_json::Value> = resp.json().await.unwrap();
    let entry = details.iter().find(|d| d["name"] == "my_table");
    assert!(
        entry.is_some(),
        "tabledetails should contain my_table: {:?}",
        details
    );

    // 4. Verify GET /streamdetails returns streams with details.
    let resp = client
        .get(format!("{}/streamdetails", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let stream_details: Vec<serde_json::Value> = resp.json().await.unwrap();
    let stream_entry = stream_details.iter().find(|d| d["name"] == "demo");
    assert!(
        stream_entry.is_some(),
        "streamdetails should contain demo: {:?}",
        stream_details
    );

    // 5. Verify GET /tables/my_table/schema returns the schema object.
    let resp = client
        .get(format!("{}/tables/my_table/schema", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let schema: serde_json::Value = resp.json().await.unwrap();
    // Fieldless definitions report an empty field map.
    assert_eq!(schema, json!({}));

    // Stream schema endpoint works too.
    let resp = client
        .get(format!("{}/streams/demo/schema", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 6. Drop the table and verify it is gone.
    let resp = client
        .delete(format!("{}/tables/my_table", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(resp.text().await.unwrap(), "Table my_table is dropped.");

    let resp = client
        .get(format!("{}/tables", base_url))
        .send()
        .await
        .unwrap();
    let tables: Vec<String> = resp.json().await.unwrap();
    assert!(!tables.contains(&"my_table".to_string()));

    let resp = client
        .get(format!("{}/tables/my_table", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let missing: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(missing["error"], json!(3000));
    assert!(
        missing["message"]
            .as_str()
            .unwrap_or("")
            .contains("my_table is not found"),
        "missing table envelope: {}",
        missing
    );
}

#[tokio::test]
async fn test_rule_validation_topo_and_migration() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    let response = client
        .post(format!("{}/rules/validate", base_url))
        .json(&json!({"id":"missing", "sql":"SELECT * FROM missing", "actions":[{"log":{}}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["error"], json!(1000));
    assert!(body["message"]
        .as_str()
        .unwrap()
        .contains("fail to get stream missing"));

    // Validation requires the sources to exist, just as rule creation does.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM test_stream () WITH (TYPE=\"memory\", FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // 1. Validate a valid rule via POST /rules/validate -> 200 OK.
    let resp = client
        .post(format!("{}/rules/validate", base_url))
        .json(&json!({
            "id": "test_rule",
            "sql": "SELECT * FROM test_stream WHERE temp > 25",
            "actions": [{ "log": {} }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let valid: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(valid["valid"], json!(true));
    assert_eq!(valid["sources"], json!(["test_stream"]));

    // 2. Validate an invalid rule (bad SQL) -> 400 Bad Request.
    let resp = client
        .post(format!("{}/rules/validate", base_url))
        .json(&json!({
            "id": "bad_rule",
            "sql": "SELECT FROM WHERE",
            "actions": [{ "log": {} }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);

    // 3. Deploy rule 	est_rule using the existing stream.
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "test_rule",
            "sql": "SELECT * FROM test_stream WHERE temp > 25",
            "actions": [{ "log": {} }]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 4. Query GET /rules/test_rule/topo -> sources contains "test_stream".
    let resp = client
        .get(format!("{}/rules/test_rule/topo", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let topo: serde_json::Value = resp.json().await.unwrap();
    let sources = topo["sources"]
        .as_array()
        .expect("topo.sources is an array");
    assert!(sources.iter().any(|s| s == "test_stream"));

    // Unknown rule topo -> 404.
    let resp = client
        .get(format!("{}/rules/no_such_rule/topo", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // 5. Query GET /rules/test_rule/explain -> 200 OK.
    let resp = client
        .get(format!("{}/rules/test_rule/explain", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let plan: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(plan["source"], "test_stream");

    // 6. Query GET /rules/status/all -> contains key "test_rule".
    let resp = client
        .get(format!("{}/rules/status/all", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let all: serde_json::Value = resp.json().await.unwrap();
    assert!(all.get("test_rule").is_some());

    // 7. Export via GET /data/export -> contains "test_stream" and "test_rule".
    let resp = client
        .get(format!("{}/data/export", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("test_stream"),
        "export should contain test_stream"
    );
    assert!(
        body.contains("test_rule"),
        "export should contain test_rule"
    );
}

#[tokio::test]
async fn test_openapi_metadata_and_connections() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // GET /metadata/sources contains mqtt, http, file.
    let resp = client
        .get(format!("{}/metadata/sources", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let sources: Vec<serde_json::Value> = resp.json().await.unwrap();
    let names: Vec<&str> = sources
        .iter()
        .filter_map(|s| s.get("name").and_then(|n| n.as_str()))
        .collect();
    assert!(names.contains(&"mqtt"));
    assert!(names.contains(&"http"));
    assert!(names.contains(&"file"));

    // GET /metadata/sinks contains mqtt, log, file.
    let resp = client
        .get(format!("{}/metadata/sinks", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let sinks: Vec<serde_json::Value> = resp.json().await.unwrap();
    let names: Vec<&str> = sinks
        .iter()
        .filter_map(|s| s.get("name").and_then(|n| n.as_str()))
        .collect();
    assert!(names.contains(&"mqtt"));
    assert!(names.contains(&"log"));
    assert!(names.contains(&"file"));

    // GET /metadata/functions returns 200 OK.
    let resp = client
        .get(format!("{}/metadata/functions", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let functions: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(!functions.is_empty());

    // GET /connections returns 200 OK array.
    let resp = client
        .get(format!("{}/connections", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let conns: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(conns.is_empty());

    // POST /connections creates a connection.
    let resp = client
        .post(format!("{}/connections", base_url))
        .json(&json!({
            "id": "test_conn",
            "type": "mqtt",
            "server": "tcp://127.0.0.1:1883"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // GET /connections/:id retrieves it.
    let resp = client
        .get(format!("{}/connections/test_conn", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let conn: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(conn["id"], "test_conn");

    // GET /connections now lists it.
    let resp = client
        .get(format!("{}/connections", base_url))
        .send()
        .await
        .unwrap();
    let conns: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(conns.iter().any(|c| c["id"] == "test_conn"));

    // DELETE /connections/:id drops it.
    let resp = client
        .delete(format!("{}/connections/test_conn", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(format!("{}/connections/test_conn", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // GET /plugins/sources returns 200 OK array.
    let resp = client
        .get(format!("{}/plugins/sources", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let plugins: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(plugins.is_empty());
}

#[tokio::test]
async fn test_all_openapi_paths_responding() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // Seed a stream + rule for the endpoints that require existing entities.
    client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM demo () WITH (DATASOURCE=\"demo\", FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_openapi",
            "sql": "SELECT * FROM demo WHERE temp > 25",
            "actions": [{ "log": {} }]
        }))
        .send()
        .await
        .unwrap();
    // Seed a ruletest session so its start endpoint is genuine (unknown
    // sessions 404 instead of fake-OK).
    client
        .post(format!("{}/ruletest", base_url))
        .json(&json!({
            "id": "rule_openapi",
            "sql": "SELECT * FROM demo",
            "mockSource": {"demo": {"data": [{"temp": 30}], "interval": 100, "loop": false}}
        }))
        .send()
        .await
        .unwrap();

    for path in [
        "/v2/rules/rule_openapi/status",
        "/rules/rule_openapi/schema",
        "/trace/rule/rule_openapi",
        "/trace/trace_1",
        "/async/task/task_1",
        "/metadata/connections/mqtt",
        "/metadata/sources/yaml/mqtt",
        "/metadata/sinks/yaml/mqtt",
        "/metadata/connections/yaml/mqtt",
        "/plugins/sources/mqtt",
        "/plugins/sinks/mqtt",
    ] {
        let resp = client
            .get(format!("{}{}", base_url, path))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }

    // Strict plugin endpoints need existing registrations; these are created here, ahead
    // of the POST loop that registers function symbols). Names are distinct
    // because the registry keys definitions by bare name.
    for (path, body) in [
        ("/plugins/sources", json!({"name": "mqtt_src_plug"})),
        ("/plugins/sinks", json!({"name": "mqtt_sink_plug"})),
        (
            "/plugins/functions",
            json!({"name": "echo_fn_plug", "functions": ["echo"]}),
        ),
    ] {
        let resp = client
            .post(format!("{}{}", base_url, path))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::CREATED, "POST {}", path);
    }

    // NOTE: POST /ruletest and POST /async/data/import are intentionally absent
    // here: without a request body they fail fast with 400 (covered by dedicated
    // validation tests below).
    for path in [
        "/ruletest/rule_openapi/start",
        "/rules/rule_openapi/trace/start",
        "/rules/rule_openapi/trace/stop",
        "/tracer",
        "/async/task/task_1/cancel",
        "/batch/req",
        "/plugins/functions/echo_fn_plug/register",
        "/metadata/sources/connection/mqtt",
        "/metadata/sinks/connection/mqtt",
        "/metadata/lookups/connection/mqtt",
    ] {
        let resp = client
            .post(format!("{}{}", base_url, path))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK, "POST {}", path);
    }

    // Verify /async/data/import responds properly: fails fast with 400 on empty body,
    // and returns 200 OK when given a valid payload.
    let resp = client
        .post(format!("{}/async/data/import", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "POST /async/data/import without body"
    );

    let resp = client
        .post(format!("{}/async/data/import", base_url))
        .json(&json!({"streams": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "POST /async/data/import with body"
    );

    for path in [
        "/rules/rule_openapi/tags",
        "/schemas/stream/myschema",
        "/schemas/stream/myschema/upload",
        "/plugins/sources/mqtt_src_plug",
        "/plugins/sinks/mqtt_sink_plug",
        "/plugins/functions/echo_fn_plug",
        "/metadata/sources/mqtt/confKeys/testConf",
        "/metadata/sinks/mqtt/confKeys/testConf",
        "/metadata/connections/mqtt/confKeys/testConf",
    ] {
        let resp = client
            .put(format!("{}{}", base_url, path))
            .json(&json!({}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK, "PUT {}", path);
    }

    let resp = client
        .patch(format!("{}/rules/rule_openapi/tags", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    for path in [
        "/rules/rule_openapi/tags",
        "/ruletest/rule_openapi",
        "/config/uploads/cfg.json",
        "/schemas/stream/myschema",
        "/plugins/sources/mqtt_src_plug",
        "/plugins/sinks/mqtt_sink_plug",
        "/plugins/functions/echo_fn_plug",
        "/metadata/sources/mqtt/confKeys/testConf",
        "/metadata/sinks/mqtt/confKeys/testConf",
        "/metadata/connections/mqtt/confKeys/testConf",
    ] {
        let resp = client
            .delete(format!("{}{}", base_url, path))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK, "DELETE {}", path);
    }

    // Rule schema for a missing rule is 404.
    let resp = client
        .get(format!("{}/rules/no_such_rule/schema", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_udf_plugin_endpoints() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // Empty registries list nothing.
    for path in ["/plugins/functions", "/plugins/udfs"] {
        let resp = client
            .get(format!("{}{}", base_url, path))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK, "GET {}", path);
        let items: Vec<serde_json::Value> = resp.json().await.unwrap();
        assert!(items.is_empty());
    }

    // Register a function plugin and a UDF plugin.
    let resp = client
        .post(format!("{}/plugins/functions", base_url))
        .json(&json!({"name": "my_math", "functions": ["cube", "clamp"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let resp = client
        .post(format!("{}/plugins/udfs", base_url))
        .json(&json!({"name": "my_udf", "functions": ["myfunc"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // Missing names are rejected.
    let resp = client
        .post(format!("{}/plugins/functions", base_url))
        .json(&json!({"functions": ["x"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);

    // Listings are segregated by plugin type.
    let resp = client
        .get(format!("{}/plugins/functions", base_url))
        .send()
        .await
        .unwrap();
    let items: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["name"], "my_math");
    assert_eq!(items[0]["plugin_type"], "function");

    let resp = client
        .get(format!("{}/plugins/udfs", base_url))
        .send()
        .await
        .unwrap();
    let items: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["name"], "my_udf");

    // Detail lookups, including cross-type misses.
    let resp = client
        .get(format!("{}/plugins/functions/my_math", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let def: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(def["functions"], json!(["cube", "clamp"]));

    let resp = client
        .get(format!("{}/plugins/udfs/my_math", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    let resp = client
        .get(format!("{}/plugins/functions/missing", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // Unregister; deletes stay idempotent.
    let resp = client
        .delete(format!("{}/plugins/functions/my_math", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let resp = client
        .get(format!("{}/plugins/functions", base_url))
        .send()
        .await
        .unwrap();
    let items: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(items.is_empty());

    let resp = client
        .delete(format!("{}/plugins/udfs/my_udf", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_schema_registry_crud() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();
    let book = "syntax = \"proto3\";\nmessage Book { string title = 1; int32 price = 2; }\n";

    // Missing schemas 404.
    let resp = client
        .get(format!("{}/schemas/protobuf/book", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // Register, then list.
    let resp = client
        .post(format!("{}/schemas/protobuf", base_url))
        .json(&json!({"name": "book", "content": book}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let resp = client
        .get(format!("{}/schemas/protobuf", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let names: Vec<String> = resp.json().await.unwrap();
    assert_eq!(names, vec!["book".to_string()]);
    // Other kinds are isolated.
    let resp = client
        .get(format!("{}/schemas/json", base_url))
        .send()
        .await
        .unwrap();
    let names: Vec<String> = resp.json().await.unwrap();
    assert!(names.is_empty());

    // Definition JSON.
    let resp = client
        .get(format!("{}/schemas/protobuf/book", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let def: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(def["name"], "book");
    assert_eq!(def["kind"], "protobuf");
    assert_eq!(def["content"], book);

    // Raw proto text via Accept: text/plain.
    let resp = client
        .get(format!("{}/schemas/protobuf/book", base_url))
        .header(reqwest::header::ACCEPT, "text/plain")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(resp.text().await.unwrap(), book);

    // Update content, then delete.
    let resp = client
        .put(format!("{}/schemas/protobuf/book", base_url))
        .json(&json!({"content": "syntax = \"proto3\";\nmessage Book { string title = 1; }"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let resp = client
        .get(format!("{}/schemas/protobuf/book", base_url))
        .send()
        .await
        .unwrap();
    let def: serde_json::Value = resp.json().await.unwrap();
    assert!(def["content"].as_str().unwrap().contains("string title"));

    // Upload endpoint upserts too.
    let resp = client
        .put(format!("{}/schemas/protobuf/mag/upload", base_url))
        .json(&json!({"content": book}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let resp = client
        .get(format!("{}/schemas/protobuf", base_url))
        .send()
        .await
        .unwrap();
    let names: Vec<String> = resp.json().await.unwrap();
    assert_eq!(names, vec!["book".to_string(), "mag".to_string()]);

    let resp = client
        .delete(format!("{}/schemas/protobuf/book", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let resp = client
        .get(format!("{}/schemas/protobuf/book", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_simulator_and_memory_pipeline() {
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    // 1. Configure the simulator source data.
    let resp = client
        .put(format!(
            "{}/metadata/sources/simulator/confKeys/sim1",
            base_url
        ))
        .json(&json!({
            "data": [{"a": 10}, {"a": 20}],
            "interval": "5ms",
            "loop": false,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 2. Create a simulator-backed stream.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM sim1 () WITH (TYPE=\"simulator\", CONF_KEY=\"sim1\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 3. Subscribe to the memory sink topic before the rule starts emitting.
    let mut rx = state.stream_bus.subscribe("test_mem_out");

    // 4. Create a rule projecting into the memory sink.
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_sim_mem",
            "sql": "SELECT a * 2 AS result FROM sim1",
            "actions": [{"memory": {"topic": "test_mem_out"}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 5. Await both records with a 2-second timeout each.
    let mut results = Vec::new();
    for _ in 0..2 {
        let record = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting for memory sink record")
            .expect("memory topic closed");
        results.push(
            record
                .data
                .get("result")
                .cloned()
                .unwrap_or(serde_json::Value::Null),
        );
    }
    assert_eq!(results, vec![json!(20), json!(40)]);

    // 6. Both outputs went through the rule's sink path.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let status: serde_json::Value = client
            .get(format!("{}/rules/rule_sim_mem/status", base_url))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let sink = status["sinkRecordsOutTotal"].as_u64().unwrap_or(0);
        if sink >= 2 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for sinkRecordsOutTotal >= 2 (got {})",
            sink
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn test_upstream_fvt_acc_max_by_vehicle_flow() {
    // Replicates ekuiper-upstream fvt/rule_test.go TestAccMaxByMapAggChargeCycle:
    // Rule 1 tracks per-SOC running max temp + its timestamp; Rule 2 reads
    // Rule 1's output through a memory stream and emits the full SOC map once
    // the upload flag is set.
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    for sql in [
        "CREATE STREAM vehicle_in (soc BIGINT, temp BIGINT, ts BIGINT, upload_flag BIGINT) WITH (FORMAT=\"json\")",
        "CREATE STREAM vehicle_stat_stream () WITH (TYPE=\"memory\", DATASOURCE=\"vehicle_stat\", FORMAT=\"json\")",
    ] {
        let resp = client
            .post(format!("{}/streams", base_url))
            .json(&json!({ "sql": sql }))
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success(), "create stream: {}", sql);
    }

    // Subscribe before creating rules so no memory message is lost.
    let mut rx = state.stream_bus.subscribe("vehicle_result");

    // Downstream rule first (mirrors the upstream test ordering).
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_vehicle_stat",
            "sql": "SELECT acc_map_agg(soc, object_construct('max_temp', max_temp, 'max_temp_ts', max_temp_ts)) AS data FROM vehicle_stat_stream WHERE upload_flag = 1",
            "actions": [{"memory": {"topic": "vehicle_result"}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_vehicle_in",
            "sql": "SELECT soc, acc_max(temp) OVER (PARTITION BY soc) AS max_temp, acc_max_by(ts, temp) OVER (PARTITION BY soc) AS max_temp_ts, upload_flag FROM vehicle_in",
            "actions": [{"memory": {"topic": "vehicle_stat"}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Push the four charge-cycle records.
    let resp = client
        .post(format!("{}/streams/vehicle_in/data", base_url))
        .json(&json!([
            {"soc": 18, "temp": 28, "ts": 1788000000000i64},
            {"soc": 18, "temp": 30, "ts": 1788000060000i64},
            {"soc": 19, "temp": 32, "ts": 1788000120000i64},
            {"soc": 20, "temp": 33, "ts": 1788000210000i64, "upload_flag": 1}
        ]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Await the single aggregated result.
    let record = tokio::time::timeout(std::time::Duration::from_secs(10), rx.recv())
        .await
        .expect("timed out waiting for vehicle_result")
        .expect("vehicle_result topic closed");
    let data = record
        .data
        .get("data")
        .and_then(|v| v.as_array())
        .expect("result should carry a data array");
    assert_eq!(data.len(), 3);
    let find = |key: &str| {
        data.iter()
            .find(|e| e.get("key").and_then(|k| k.as_str()) == Some(key))
            .unwrap_or_else(|| panic!("missing soc entry {}", key))
            .get("value")
            .cloned()
            .unwrap()
    };
    assert_eq!(find("18")["max_temp"], json!(30));
    assert_eq!(find("18")["max_temp_ts"], json!(1788000060000i64));
    assert_eq!(find("19")["max_temp"], json!(32));
    assert_eq!(find("19")["max_temp_ts"], json!(1788000120000i64));
    assert_eq!(find("20")["max_temp"], json!(33));
    assert_eq!(find("20")["max_temp_ts"], json!(1788000210000i64));
}

#[tokio::test]
async fn test_name_validation() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // Names with spaces (sent percent-encoded) are rejected with 400.
    for path in [
        "streams/invalid%20name",
        "tables/invalid%20name",
        "rules/invalid%20name",
    ] {
        let resp = client
            .get(format!("{}/{}", base_url, path))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::BAD_REQUEST,
            "GET {}",
            path
        );
        let body = resp.text().await.unwrap();
        assert!(
            body.contains("invalid characters"),
            "GET {} body should mention invalid characters: {}",
            path,
            body
        );
    }

    // Sane names still resolve (400 JSON envelope here, proving validation passed).
    let resp = client
        .get(format!("{}/streams/no_such_stream", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let missing: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(missing["error"], json!(3000));
}

#[tokio::test]
async fn test_csv_file_sink() {
    use rekuiper_connectors::{FileSink, Sink};
    use rekuiper_core::model::StreamRecord;
    use std::collections::HashMap;

    let mut path = std::env::temp_dir();
    path.push(format!(
        "rekuiper-csv-sink-{}-{}.csv",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    // Fresh file: no header exists yet.
    let _ = tokio::fs::remove_file(&path).await;

    let sink: FileSink = serde_json::from_value(json!({
        "path": path.to_string_lossy(),
        "format": "delimited",
        "hasHeader": true,
    }))
    .expect("FileSink should deserialize delimited options");

    for (a, b) in [(1, "x"), (2, "y")] {
        let mut data = HashMap::new();
        data.insert("a".to_string(), json!(a));
        data.insert("b".to_string(), json!(b));
        sink.send(&StreamRecord::new(data)).await.unwrap();
    }

    let content = tokio::fs::read_to_string(&path).await.unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 3, "header plus two rows: {:?}", lines);
    assert_eq!(lines[0], "a,b");
    assert_eq!(lines[1], "1,x");
    assert_eq!(lines[2], "2,y");

    let _ = tokio::fs::remove_file(&path).await;
}

#[tokio::test]
async fn test_ruletest_sse_and_unnest() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 1. Create the ruletest session with mock source data.
    let resp = client
        .post(format!("{}/ruletest", base_url))
        .json(&json!({
            "id": "rt_test_1",
            "sql": "SELECT id, time, unnest(data) FROM demo",
            "mockSource": {
                "demo": {
                    "loop": false,
                    "data": [{"id": "id1", "time": "2026-01-01", "data": [{"k": 1}, {"k": 2}]}]
                }
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 2. The response carries the session id and the REST port.
    let created: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(created["id"], "rt_test_1");
    assert!(created["port"].is_number());

    // 3. Start the simulation.
    let resp = client
        .post(format!("{}/ruletest/rt_test_1/start", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 4. Drop the session.
    let resp = client
        .delete(format!("{}/ruletest/rt_test_1", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_stream_table_lookup_join() {
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    // 1. Create table `alert_tbl`.
    let resp = client
        .post(format!("{}/tables", base_url))
        .json(&json!({
            "sql": "CREATE TABLE alert_tbl () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 2. Populate it with two lookup rows.
    let resp = client
        .post(format!("{}/tables/alert_tbl/data", base_url))
        .json(&json!([
            {"id": "d1", "status": "active"},
            {"id": "d2", "status": "warning"}
        ]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 3. Create stream `sensor_str`.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM sensor_str () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 4. LEFT JOIN rule fanning out to a memory topic.
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_join",
            "sql": "SELECT id, temp, status FROM sensor_str LEFT JOIN alert_tbl ON sensor_str.id = alert_tbl.id",
            "actions": [{"memory": {"topic": "join_res"}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 5. Subscribe before ingesting so nothing is lost.
    let mut rx = state.stream_bus.subscribe("join_res");

    // 6. Ingest one matching and one unmatched record.
    let resp = client
        .post(format!("{}/streams/sensor_str/data", base_url))
        .json(&json!([{"id": "d1", "temp": 25.0}, {"id": "d99", "temp": 30.0}]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 7. Matched row carries the table's status.
    let first = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for first join output")
        .expect("join_res topic closed");
    assert_eq!(first.data.get("id"), Some(&json!("d1")));
    assert_eq!(first.data.get("temp"), Some(&json!(25.0)));
    assert_eq!(first.data.get("status"), Some(&json!("active")));

    // Unmatched row still emits under LEFT JOIN with null status.
    let second = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for second join output")
        .expect("join_res topic closed");
    assert_eq!(second.data.get("id"), Some(&json!("d99")));
    assert_eq!(second.data.get("temp"), Some(&json!(30.0)));
    assert!(
        matches!(
            second.data.get("status"),
            None | Some(serde_json::Value::Null)
        ),
        "unmatched status should be null or missing, got {:?}",
        second.data.get("status")
    );
}

#[tokio::test]
async fn test_windowed_stream_stream_join() {
    // D-JOIN: doc-shape windowed stream-stream joins match on the ON key
    // instead of yielding all-null rows.
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    for stream in ["wsa", "wsb"] {
        let resp = client
            .post(format!("{}/streams", base_url))
            .json(&json!({
                "sql": format!("CREATE STREAM {} () WITH (FORMAT=\"json\")", stream)
            }))
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success());
    }
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_wjoin",
            "sql": "SELECT wsa.id AS id, wsb.val AS v FROM wsa INNER JOIN wsb ON wsa.id = wsb.id GROUP BY CountWindow(2)",
            "actions": [{"memory": {"topic": "wjoin_res"}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Subscribe before ingesting so nothing is lost.
    let mut rx = state.stream_bus.subscribe("wjoin_res");
    for (stream, body) in [
        ("wsa", json!({"id": 42})),
        ("wsb", json!({"id": 42, "val": 9})),
    ] {
        let resp = client
            .post(format!("{}/streams/{}/data", base_url, stream))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }
    let got = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for join output")
        .expect("wjoin_res topic closed");
    assert_eq!(got.data.get("id"), Some(&json!(42)));
    assert_eq!(got.data.get("v"), Some(&json!(9)));

    // LEFT JOIN with a left-only event emits the left row (null right side).
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_wleft",
            "sql": "SELECT wsa.id AS id, wsb.val AS v FROM wsa LEFT JOIN wsb ON wsa.id = wsb.id GROUP BY CountWindow(1)",
            "actions": [{"memory": {"topic": "wleft_res"}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let mut rx = state.stream_bus.subscribe("wleft_res");
    let resp = client
        .post(format!("{}/streams/wsa/data", base_url))
        .json(&json!({"id": 43}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let got = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for left-join output")
        .expect("wleft_res topic closed");
    assert_eq!(got.data.get("id"), Some(&json!(43)));
    assert!(
        matches!(got.data.get("v"), None | Some(serde_json::Value::Null)),
        "unmatched val should be null or missing, got {:?}",
        got.data.get("v")
    );
}

#[tokio::test]
async fn test_prometheus_metrics_scraping() {
    let (base_url, _handle, _state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    // Create a stream and a rule.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM prom_stream () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_prom",
            "sql": "SELECT * FROM prom_stream",
            "actions": [{"log": {}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Push a record through so counters advance.
    let resp = client
        .post(format!("{}/streams/prom_stream/data", base_url))
        .json(&json!({"temp": 25.0}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Wait until the rule has ingested the record.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let status: serde_json::Value = client
            .get(format!("{}/rules/rule_prom/status", base_url))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if status["sourceRecordsInTotal"].as_u64().unwrap_or(0) >= 1 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for rule_prom to ingest data"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // Scrape the Prometheus exposition.
    let resp = client
        .get(format!("{}/metrics", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    assert!(
        content_type.contains("text/plain"),
        "Content-Type should be Prometheus text exposition, got {}",
        content_type
    );
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("kuiper_rule_count{status=\"running\"}"),
        "missing running rule count: {}",
        body
    );
    assert!(
        body.contains("kuiper_rule_status{rule=\"rule_prom\"} 1"),
        "missing rule_prom status: {}",
        body
    );
    assert!(
        body.contains("kuiper_sink_records_in_total{rule=\"rule_prom\"}"),
        "missing rule_prom sink records: {}",
        body
    );
}

#[tokio::test]
async fn test_http_pull_source_pipeline() {
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    // Seed rule whose status endpoint doubles as the mock JSON source.
    // NOTE: /ping returns plain text, so a JSON object endpoint is used.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM seed () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_seed",
            "sql": "SELECT * FROM seed",
            "actions": [{"log": {}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 3. Point an httppull conf key at the seed rule's status endpoint.
    let mock_url = format!("{}/rules/rule_seed/status", base_url);
    let resp = client
        .put(format!(
            "{}/metadata/sources/httppull/confKeys/pull_cfg",
            base_url
        ))
        .json(&json!({"url": mock_url, "method": "get", "interval": 100}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 4. Create the pull-backed stream.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM ping_stream () WITH (TYPE=\"httppull\", CONF_KEY=\"pull_cfg\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 5. Rule the pulled records through a memory sink.
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_pull",
            "sql": "SELECT * FROM ping_stream",
            "actions": [{"memory": {"topic": "pull_results"}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 6. Subscribe to the results topic.
    let mut rx = state.stream_bus.subscribe("pull_results");

    // 7. Polled status objects arrive within 2 seconds.
    let record = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
        .await
        .expect("timed out waiting for http pull record")
        .expect("pull_results topic closed");
    assert_eq!(
        record.data.get("status"),
        Some(&json!("running")),
        "unexpected pull record: {:?}",
        record.data
    );
}

#[tokio::test]
async fn test_websocket_source_and_sink() {
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    // 1. Sink-side probe server: captures the first text frame it receives.
    let sink_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let sink_port = sink_listener.local_addr().unwrap().port();
    let (frame_tx, mut frame_rx) = tokio::sync::mpsc::channel::<String>(4);
    tokio::spawn(async move {
        let Ok((stream, _)) = sink_listener.accept().await else {
            return;
        };
        let Ok(mut ws) = accept_async(stream).await else {
            return;
        };
        while let Some(msg) = ws.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    let _ = frame_tx.send(text).await;
                    return;
                }
                Ok(_) => continue,
                Err(_) => return,
            }
        }
    });

    // 2. Source-side probe server: pushes one device reading per connection.
    let source_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let source_port = source_listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let Ok((stream, _)) = source_listener.accept().await else {
            return;
        };
        let Ok(mut ws) = accept_async(stream).await else {
            return;
        };
        let _ = ws
            .send(Message::Text(
                r#"{"id": "ws_device", "temp": 33.3}"#.to_string(),
            ))
            .await;
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let _ = ws.close(None).await;
    });

    // 3a. Sink test: rule fans stream records out over WebSocket.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM sink_stream () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_ws_sink",
            "sql": "SELECT * FROM sink_stream",
            "actions": [{"websocket": {
                "addr": format!("127.0.0.1:{}", sink_port),
                "path": "/ws_sink"
            }}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{}/streams/sink_stream/data", base_url))
        .json(&json!({"id": "ws_device", "temp": 33.3}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let frame = tokio::time::timeout(std::time::Duration::from_secs(2), frame_rx.recv())
        .await
        .expect("timed out waiting for websocket sink frame")
        .expect("sink probe channel closed");
    let frame_json: serde_json::Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(frame_json["id"], "ws_device");
    assert_eq!(frame_json["temp"], 33.3);

    // 3b. Source test: subscribe first so no broadcast is lost.
    let mut rx = state.stream_bus.subscribe("ws_out");

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": format!(
                "CREATE STREAM ws_source_stream () WITH (TYPE=\"websocket\", DATASOURCE=\"ws://127.0.0.1:{}/ws_source\")",
                source_port
            )
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 4. Rule the pulled readings into a memory topic.
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_ws_source",
            "sql": "SELECT * FROM ws_source_stream",
            "actions": [{"memory": {"topic": "ws_out"}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let record = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
        .await
        .expect("timed out waiting for websocket source record")
        .expect("ws_out topic closed");
    assert_eq!(record.data.get("id"), Some(&json!("ws_device")));
    assert_eq!(record.data.get("temp"), Some(&json!(33.3)));
}

// ---------------------------------------------------------------------------
// Hermetic in-process Redis replacement speaking just enough RESP for the
// redis connector paths (SET/GET/PUBLISH/SUBSCRIBE), so no external daemon
// is required on any platform.
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct MockRedisCmd {
    cmd: String,
    args: Vec<String>,
}

async fn read_resp_command<R>(reader: &mut R) -> Option<Vec<String>>
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    use tokio::io::{AsyncBufReadExt, AsyncReadExt};
    let trim = |s: &str| s.trim_end_matches(['\r', '\n']).to_string();
    let mut line = String::new();
    reader.read_line(&mut line).await.ok()?;
    let count: usize = trim(&line).strip_prefix('*')?.parse().ok()?;
    let mut args = Vec::with_capacity(count);
    for _ in 0..count {
        line.clear();
        reader.read_line(&mut line).await.ok()?;
        let len: usize = trim(&line).strip_prefix('$')?.parse().ok()?;
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf).await.ok()?;
        let mut crlf = [0u8; 2];
        reader.read_exact(&mut crlf).await.ok()?;
        args.push(String::from_utf8_lossy(&buf).into_owned());
    }
    Some(args)
}

async fn handle_mock_redis_conn(
    stream: tokio::net::TcpStream,
    cmd_tx: tokio::sync::mpsc::Sender<MockRedisCmd>,
) {
    use tokio::io::{AsyncWriteExt, BufReader};
    let (rh, mut wh) = stream.into_split();
    let mut rd = BufReader::new(rh);
    loop {
        let Some(argv) = read_resp_command(&mut rd).await else {
            break;
        };
        if argv.is_empty() {
            continue;
        }
        let cmd = argv[0].to_ascii_uppercase();
        let args = argv[1..].to_vec();
        let _ = cmd_tx
            .send(MockRedisCmd {
                cmd: cmd.clone(),
                args: args.clone(),
            })
            .await;
        let reply: Vec<u8> = match cmd.as_str() {
            "PING" => b"+PONG\r\n".to_vec(),
            "QUIT" => b"+OK\r\n".to_vec(),
            "CLIENT" => b"+OK\r\n".to_vec(),
            "SET" => b"+OK\r\n".to_vec(),
            "GET" => {
                let key = args.first().map(|s| s.as_str()).unwrap_or("");
                // Only d1 exists; anything else is a cache miss.
                if key == "d1" {
                    let body = r#"{"id":"d1","status":"active"}"#;
                    format!("${}\r\n{}\r\n", body.len(), body).into_bytes()
                } else {
                    b"$-1\r\n".to_vec()
                }
            }
            "PUBLISH" => b":1\r\n".to_vec(),
            "SUBSCRIBE" => {
                let channel = args.first().cloned().unwrap_or_default();
                let msg = r#"{"id":"r1","temp":21.5}"#;
                format!(
                    "*3\r\n$9\r\nsubscribe\r\n${}\r\n{}\r\n:1\r\n*3\r\n$7\r\nmessage\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    channel.len(),
                    channel,
                    channel.len(),
                    channel,
                    msg.len(),
                    msg
                )
                .into_bytes()
            }
            _ => format!("-ERR unknown command '{}'\r\n", argv[0]).into_bytes(),
        };
        if wh.write_all(&reply).await.is_err() {
            break;
        }
        if cmd == "QUIT" {
            break;
        }
    }
}

async fn spawn_mock_redis() -> (String, tokio::sync::mpsc::Receiver<MockRedisCmd>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("127.0.0.1:{}", listener.local_addr().unwrap().port());
    let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel(64);
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let cmd_tx = cmd_tx.clone();
            tokio::spawn(async move { handle_mock_redis_conn(stream, cmd_tx).await });
        }
    });
    (addr, cmd_rx)
}

async fn next_mock_cmd(
    rx: &mut tokio::sync::mpsc::Receiver<MockRedisCmd>,
    want: &str,
) -> MockRedisCmd {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for Redis command {}",
            want
        );
        match tokio::time::timeout(std::time::Duration::from_millis(200), rx.recv()).await {
            Ok(Some(cmd)) if cmd.cmd == want => return cmd,
            // Unrelated commands (e.g. lookups from other flows) are skipped.
            _ => continue,
        }
    }
}

#[tokio::test]
async fn test_redis_connectors_pipeline() {
    let (redis_addr, mut cmd_rx) = spawn_mock_redis().await;
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    // Lookup-table config pointing at the mock daemon.
    let resp = client
        .put(format!(
            "{}/metadata/sources/redis/confKeys/rt_redis",
            base_url
        ))
        .json(&json!({"addr": redis_addr}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // a/b) One stream fanning out to a Redis SET rule and a PUBLISH rule.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM redis_stream () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_redis_set",
            "sql": "SELECT * FROM redis_stream",
            "actions": [{"redis": {"addr": redis_addr, "field": "id"}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_redis_pub",
            "sql": "SELECT * FROM redis_stream",
            "actions": [{"redisPub": {"addr": redis_addr, "topic": "channel1"}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // c) Redis lookup table + LEFT JOIN rule.
    let resp = client
        .post(format!("{}/tables", base_url))
        .json(&json!({
            "sql": "CREATE TABLE redis_tbl () WITH (TYPE=\"redis\", CONF_KEY=\"rt_redis\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM sensor_redis () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let mut rx = state.stream_bus.subscribe("join_redis_res");
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_join_redis",
            "sql": "SELECT id, temp, status FROM sensor_redis LEFT JOIN redis_tbl ON sensor_redis.id = redis_tbl.id",
            "actions": [{"memory": {"topic": "join_redis_res"}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // a) Sink path: SET rk1 <json>.
    let resp = client
        .post(format!("{}/streams/redis_stream/data", base_url))
        .json(&json!({"id": "rk1", "temp": 22.5}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let set = next_mock_cmd(&mut cmd_rx, "SET").await;
    assert_eq!(set.args.first().map(|s| s.as_str()), Some("rk1"));
    let stored: serde_json::Value =
        serde_json::from_str(set.args.get(1).map(|s| s.as_str()).unwrap_or("")).unwrap();
    assert_eq!(stored["id"], "rk1");

    // b) Pub path: PUBLISH channel1 <json>.
    let published = next_mock_cmd(&mut cmd_rx, "PUBLISH").await;
    assert_eq!(published.args.first().map(|s| s.as_str()), Some("channel1"));
    let published_json: serde_json::Value =
        serde_json::from_str(published.args.get(1).map(|s| s.as_str()).unwrap_or("")).unwrap();
    assert_eq!(published_json["id"], "rk1");

    // c) Lookup path: matched row merges, unmatched row stays null.
    let resp = client
        .post(format!("{}/streams/sensor_redis/data", base_url))
        .json(&json!([{"id": "d1", "temp": 25.0}, {"id": "d99", "temp": 30.0}]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let first = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for first redis join output")
        .expect("join_redis_res topic closed");
    assert_eq!(first.data.get("id"), Some(&json!("d1")));
    assert_eq!(first.data.get("temp"), Some(&json!(25.0)));
    assert_eq!(first.data.get("status"), Some(&json!("active")));

    let second = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for second redis join output")
        .expect("join_redis_res topic closed");
    assert_eq!(second.data.get("id"), Some(&json!("d99")));
    assert_eq!(second.data.get("temp"), Some(&json!(30.0)));
    assert!(
        matches!(
            second.data.get("status"),
            None | Some(serde_json::Value::Null)
        ),
        "unmatched status should be null or missing, got {:?}",
        second.data.get("status")
    );
}

#[tokio::test]
async fn test_redis_sub_source() {
    let (redis_addr, _cmd_rx) = spawn_mock_redis().await;
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    // Point the redissub stream at the mock daemon.
    let resp = client
        .put(format!(
            "{}/metadata/sources/redis/confKeys/sub_conf",
            base_url
        ))
        .json(&json!({"addr": redis_addr}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM sub_stream () WITH (TYPE=\"redissub\", CONF_KEY=\"sub_conf\", DATASOURCE=\"testchan\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let mut rx = state.stream_bus.subscribe("sub_res");
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_redis_sub",
            "sql": "SELECT * FROM sub_stream",
            "actions": [{"memory": {"topic": "sub_res"}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let record = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for redis sub record")
        .expect("sub_res topic closed");
    assert_eq!(record.data.get("id"), Some(&json!("r1")));
    assert_eq!(record.data.get("temp"), Some(&json!(21.5)));
}

#[tokio::test]
async fn test_kafka_config_and_pipeline() {
    use rekuiper_connectors::KafkaConfig;

    // KafkaConfig parsing, defaults and topic handling.
    let cfg: KafkaConfig = serde_json::from_value(json!({})).unwrap();
    assert_eq!(cfg.brokers, "127.0.0.1:9092");
    assert_eq!(cfg.topic, None);
    assert_eq!(cfg.partition, 0);
    let cfg: KafkaConfig =
        serde_json::from_value(json!({"brokers": "a:9092,b:9092", "topic": "events"})).unwrap();
    assert_eq!(
        cfg.broker_list(),
        vec!["a:9092".to_string(), "b:9092".to_string()]
    );
    assert_eq!(cfg.topic.as_deref(), Some("events"));

    let (base_url, _handle, _state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    // Reserve a localhost port and leave it closed so broker dials fail fast.
    let closed_port = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let dead_brokers = format!("127.0.0.1:{}", closed_port);

    // Kafka stream + rule with a kafka action: full registration path.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM kafka_stream () WITH (TYPE=\"kafka\", DATASOURCE=\"events\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let rule_sql = "SELECT * FROM kafka_stream";
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_kafka",
            "sql": rule_sql,
            "actions": [{"kafka": {"brokers": dead_brokers, "topic": "output_events", "key": "id"}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // Topology exposes the kafka source stream.
    let resp = client
        .get(format!("{}/rules/rule_kafka/topo", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let topo: serde_json::Value = resp.json().await.unwrap();
    let sources = topo["sources"]
        .as_array()
        .expect("topo.sources is an array");
    assert!(sources.iter().any(|s| s == "kafka_stream"));

    // Ingest a record: the unreachable broker must surface as a counted
    // exception rather than crashing the server.
    let resp = client
        .post(format!("{}/streams/kafka_stream/data", base_url))
        .json(&json!({"id": "k1", "temp": 21.5}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        let status: serde_json::Value = client
            .get(format!("{}/rules/rule_kafka/status", base_url))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if status["exceptionsTotal"].as_u64().unwrap_or(0) >= 1 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for kafka exception counter"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    // Stopping the rule (and its kafka consumer) works cleanly.
    let resp = client
        .post(format!("{}/rules/rule_kafka/stop", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Server is still alive afterwards.
    let resp = client
        .get(format!("{}/ping", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_file_source_streaming_ingestion() {
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    // 1. Temporary JSON Lines file with three records.
    let mut db_path = std::env::temp_dir();
    db_path.push(format!(
        "rekuiper-file-source-{}-{}.json",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    // Forward slashes keep the SQL string literal simple on every platform.
    let data_path = db_path.to_string_lossy().replace('\\', "/");
    tokio::fs::write(&db_path, "{\"temp\": 20}\n{\"temp\": 25}\n{\"temp\": 30}\n")
        .await
        .unwrap();

    // 2. File-backed stream; the rule fans out to a memory topic.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": format!(
                "CREATE STREAM test_file_stream () WITH (TYPE=\"file\", DATASOURCE=\"{}\", FORMAT=\"json\")",
                data_path
            )
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let mut rx = state.stream_bus.subscribe("file_out");
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_file_src",
            "sql": "SELECT * FROM test_file_stream",
            "actions": [{"memory": {"topic": "file_out"}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 3. All three file records flow through the rule, in file order.
    let mut temps = Vec::new();
    for _ in 0..3 {
        let record = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting for file record")
            .expect("file_out topic closed");
        temps.push(
            record
                .data
                .get("temp")
                .cloned()
                .unwrap_or(serde_json::Value::Null),
        );
    }
    assert_eq!(temps, vec![json!(20), json!(25), json!(30)]);

    // Rule saw every record on its source side too.
    let status: serde_json::Value = client
        .get(format!("{}/rules/rule_file_src/status", base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(status["sourceRecordsInTotal"].as_u64().unwrap_or(0) >= 3);

    // 4. Stopping the rule cancels the file reader cleanly.
    let resp = client
        .post(format!("{}/rules/rule_file_src/stop", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert!(
        !state.source_cancels.read().contains_key("rule_file_src"),
        "stopped rule should release its source handle"
    );

    let _ = tokio::fs::remove_file(&db_path).await;
}

#[tokio::test]
async fn test_issue14_shared_stream_multi_rule_no_duplicate_delivery() {
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    // 1. Seed rule whose status endpoint is used as the mock HTTP pull source.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM seed_issue14 () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_seed_14",
            "sql": "SELECT * FROM seed_issue14",
            "actions": [{"log": {}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let mock_url = format!("{}/rules/rule_seed_14/status", base_url);
    let resp = client
        .put(format!(
            "{}/metadata/sources/httppull/confKeys/pull_cfg_14",
            base_url
        ))
        .json(&json!({"url": mock_url, "method": "get", "interval": 100}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 2. Create the pull-backed stream.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM shared_pull_s () WITH (TYPE=\"httppull\", CONF_KEY=\"pull_cfg_14\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 3. Create 2 rules consuming the same stream, outputting to distinct memory sinks.
    let mut rx0 = state.stream_bus.subscribe("pull_out_0");
    let mut rx1 = state.stream_bus.subscribe("pull_out_1");

    for (rule_id, topic) in [("pull_r0", "pull_out_0"), ("pull_r1", "pull_out_1")] {
        let resp = client
            .post(format!("{}/rules", base_url))
            .json(&json!({
                "id": rule_id,
                "sql": "SELECT * FROM shared_pull_s",
                "actions": [{"memory": {"topic": topic}}]
            }))
            .send()
            .await
            .unwrap();
        assert!(
            resp.status().is_success(),
            "failed to create rule {}",
            rule_id
        );
    }

    // 4. Verify stream source deduplication: only 1 underlying source task spawned!
    assert_eq!(
        state
            .stream_source_cancels
            .read()
            .get("shared_pull_s")
            .map(|v| v.len()),
        Some(1),
        "only 1 source task should be spawned for shared stream"
    );
    assert_eq!(
        state
            .stream_active_rules
            .read()
            .get("shared_pull_s")
            .map(|s| s.len()),
        Some(2),
        "both rules should be registered on shared stream"
    );

    // 5. Both rules receive polled records from the single producer.
    let r0 = tokio::time::timeout(std::time::Duration::from_secs(3), rx0.recv())
        .await
        .expect("timed out waiting for record on r0")
        .expect("channel closed");
    assert_eq!(r0.data.get("status"), Some(&json!("running")));

    let r1 = tokio::time::timeout(std::time::Duration::from_secs(3), rx1.recv())
        .await
        .expect("timed out waiting for record on r1")
        .expect("channel closed");
    assert_eq!(r1.data.get("status"), Some(&json!("running")));

    // 6. Stopping rule 0 keeps the stream source active for rule 1.
    let resp = client
        .post(format!("{}/rules/pull_r0/stop", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert!(
        state
            .stream_source_cancels
            .read()
            .contains_key("shared_pull_s"),
        "stream source must remain active while pull_r1 is running"
    );
    assert_eq!(
        state
            .stream_active_rules
            .read()
            .get("shared_pull_s")
            .map(|s| s.len()),
        Some(1)
    );

    // rule 1 continues receiving records seamlessly.
    let r1_next = tokio::time::timeout(std::time::Duration::from_secs(3), rx1.recv())
        .await
        .expect("timed out waiting for subsequent record on r1")
        .expect("channel closed");
    assert_eq!(r1_next.data.get("status"), Some(&json!("running")));

    // 7. Stopping rule 1 cancels the stream source cleanly.
    let resp = client
        .post(format!("{}/rules/pull_r1/stop", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert!(
        !state
            .stream_source_cancels
            .read()
            .contains_key("shared_pull_s"),
        "stream source should be cancelled once all rules stop"
    );
}

#[tokio::test]
async fn test_mqtt_source_lifecycle_and_defaults() {
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    async fn create_stream(client: &reqwest::Client, base_url: &str, sql: &str) {
        let resp = client
            .post(format!("{}/streams", base_url))
            .json(&json!({ "sql": sql }))
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success(), "create stream: {}", sql);
    }

    async fn create_rule(client: &reqwest::Client, base_url: &str, id: &str, sql: &str) {
        let resp = client
            .post(format!("{}/rules", base_url))
            .json(&json!({
                "id": id,
                "sql": sql,
                "actions": [{"log": {}}]
            }))
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success(), "create rule: {}", id);
    }

    fn has_source_handle(state: &AppState, rule_id: &str) -> bool {
        state.source_cancels.read().contains_key(rule_id)
    }

    // 1. Typeless stream defaults to MQTT: subscriber task is registered.
    create_stream(
        &client,
        &base_url,
        "CREATE STREAM demo () WITH (DATASOURCE=\"demo\")",
    )
    .await;
    create_rule(
        &client,
        &base_url,
        "rule_mqtt_default",
        "SELECT * FROM demo",
    )
    .await;
    assert!(
        has_source_handle(&state, "rule_mqtt_default"),
        "typeless stream should bootstrap an MQTT subscriber"
    );

    // 2. Explicit TYPE="mqtt" also bootstraps.
    create_stream(
        &client,
        &base_url,
        "CREATE STREAM m_mqtt () WITH (TYPE=\"mqtt\", DATASOURCE=\"telemetry\")",
    )
    .await;
    create_rule(
        &client,
        &base_url,
        "rule_mqtt_explicit",
        "SELECT * FROM m_mqtt",
    )
    .await;
    assert!(
        has_source_handle(&state, "rule_mqtt_explicit"),
        "TYPE=mqtt stream should bootstrap an MQTT subscriber"
    );

    // 2b. Explicit TYPE="edgex" also bootstraps an EdgeX source and handles edgex sink actions.
    create_stream(
        &client,
        &base_url,
        "CREATE STREAM m_edgex () WITH (TYPE=\"edgex\")",
    )
    .await;
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_edgex_explicit",
            "sql": "SELECT Temperature FROM m_edgex WHERE Temperature > 20.0",
            "actions": [{"edgex": {"topic": "edgex/alerts", "deviceName": "sensor"}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "create edgex rule");
    assert!(
        has_source_handle(&state, "rule_edgex_explicit"),
        "TYPE=edgex stream should bootstrap an EdgeX subscriber"
    );

    // 3. Other source types must not bootstrap MQTT.
    create_stream(
        &client,
        &base_url,
        "CREATE STREAM m_mem () WITH (TYPE=\"memory\")",
    )
    .await;
    create_rule(&client, &base_url, "rule_mem", "SELECT * FROM m_mem").await;
    assert!(
        !has_source_handle(&state, "rule_mem"),
        "TYPE=memory stream must not bootstrap an MQTT subscriber"
    );

    // 4. Stopping the rule cancels the source task and drops its handle.
    let resp = client
        .post(format!("{}/rules/rule_mqtt_default/stop", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert!(
        !has_source_handle(&state, "rule_mqtt_default"),
        "stopped rule should release its source handle"
    );
    let resp = client
        .post(format!("{}/rules/rule_edgex_explicit/stop", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert!(
        !has_source_handle(&state, "rule_edgex_explicit"),
        "stopped edgex rule should release its source handle"
    );
    let status: serde_json::Value = client
        .get(format!("{}/rules/rule_mqtt_default/status", base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["status"], "stopped");

    // 5. Multiple rules on the same MQTT stream share 1 source task without duplicates.
    create_rule(
        &client,
        &base_url,
        "rule_mqtt_explicit_2",
        "SELECT * FROM m_mqtt",
    )
    .await;
    assert_eq!(
        state
            .stream_source_cancels
            .read()
            .get("m_mqtt")
            .map(|v| v.len()),
        Some(1),
        "shared MQTT stream should maintain only 1 underlying source task"
    );
    assert_eq!(
        state
            .stream_active_rules
            .read()
            .get("m_mqtt")
            .map(|s| s.len()),
        Some(2),
        "both rules should be registered on m_mqtt"
    );
    // Stopping first rule does not cancel stream source.
    let resp = client
        .post(format!("{}/rules/rule_mqtt_explicit/stop", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert!(
        state.stream_source_cancels.read().contains_key("m_mqtt"),
        "stream source must remain active while rule_mqtt_explicit_2 is running"
    );
    // Stopping second rule cancels stream source cleanly.
    let resp = client
        .post(format!("{}/rules/rule_mqtt_explicit_2/stop", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert!(
        !state.stream_source_cancels.read().contains_key("m_mqtt"),
        "stream source should be cancelled when all rules stop"
    );
}

#[tokio::test]
async fn test_edgex_stream_e2e_data_pipeline() {
    use rekuiper_connectors::EdgeXCodec;
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    // 1. Create an EdgeX stream
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM edgex_demo () WITH (TYPE=\"edgex\", DATASOURCE=\"rules-events\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 2. Create rule that filters on flattened readings:
    // SELECT Temperature, Humidity FROM edgex_demo WHERE Temperature > 25.0
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_edgex_filter",
            "sql": "SELECT Temperature, Humidity FROM edgex_demo WHERE Temperature > 25.0",
            "actions": [
                {"memory": {"topic": "edgex_out"}},
                {"log": {}}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let mut out_rx = state.stream_bus.subscribe("edgex_out");

    // 3. Simulate EdgeX payload incoming on the stream bus
    let edgex_json = r#"{
        "apiVersion": "v3",
        "id": "evt-001",
        "deviceName": "HVAC_01",
        "profileName": "HVAC_Profile",
        "sourceName": "SensorReading",
        "origin": 1700000000,
        "readings": [
            {
                "id": "r-1",
                "deviceName": "HVAC_01",
                "resourceName": "Temperature",
                "valueType": "Float64",
                "value": "28.5"
            },
            {
                "id": "r-2",
                "deviceName": "HVAC_01",
                "resourceName": "Humidity",
                "valueType": "Float64",
                "value": "62.0"
            }
        ]
    }"#;

    let mut records = Vec::new();
    EdgeXCodec::decode(edgex_json.as_bytes(), None, &mut records).expect("decode edgex event");
    assert_eq!(records.len(), 1);

    let tx = state.stream_bus.get_or_create("edgex_demo");
    for r in records {
        tx.send(r).await.expect("send record");
    }

    // 4. Verify rule received, processed, and matched record
    let received = tokio::time::timeout(std::time::Duration::from_secs(3), out_rx.recv())
        .await
        .expect("rule should emit filtered output within 3s")
        .expect("channel not closed");

    assert_eq!(received.data.get("Temperature"), Some(&json!(28.5)));
    assert_eq!(received.data.get("Humidity"), Some(&json!(62.0)));

    // 5. Test EdgeX encoding on output record:
    let encoded = EdgeXCodec::encode(
        &received.data.to_hashmap(),
        "kuiper_alert",
        "alert_profile",
        "rule_edgex_filter",
    )
    .expect("encode edgex output");
    let out_event: serde_json::Value =
        serde_json::from_slice(&encoded).expect("parse encoded event");
    assert_eq!(out_event["deviceName"], "kuiper_alert");
    assert_eq!(out_event["sourceName"], "rule_edgex_filter");
    assert!(out_event["readings"].as_array().unwrap().len() >= 2);
}

#[tokio::test]
async fn test_sql_connector_and_data_template() {
    use rekuiper_connectors::apply_data_template;
    use std::collections::HashMap;

    // 1. Template rendering substitutes record fields.
    let mut data = HashMap::new();
    data.insert("id".to_string(), json!("d1"));
    data.insert("temp".to_string(), json!(25));
    let map: serde_json::Map<String, serde_json::Value> =
        data.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    assert_eq!(
        apply_data_template("device: {{.id}}, temp: {{.temp}}", &map),
        "device: d1, temp: 25"
    );

    // 2. Back the test with a temp-file SQLite database (file-backed so that
    // the per-operation pools used by sinks and lookups share one database).
    let mut db_path = std::env::temp_dir();
    db_path.push(format!(
        "rekuiper-sql-test-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    // `mode=rwc` or `create_if_missing`: ensure SQLite file is created cleanly on Windows.
    let db_url = format!("sqlite://{}", db_path.to_string_lossy().replace('\\', "/"));
    let pool = sqlx::sqlite::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new()
            .filename(&db_path)
            .create_if_missing(true),
    )
    .await
    .unwrap();
    sqlx::query("CREATE TABLE alerts (id TEXT, status TEXT)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO alerts (id, status) VALUES ('d1', 'active')")
        .execute(&pool)
        .await
        .unwrap();

    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    // 3. Rule writing rows through the sql sink action.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM sql_stream () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_sql_sink",
            "sql": "SELECT * FROM sql_stream",
            "actions": [{"sql": {"url": db_url, "table": "alerts", "fields": ["id", "status"]}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{}/streams/sql_stream/data", base_url))
        .json(&json!({"id": "d9", "temp": 30}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let row: Option<(String, String)> =
            sqlx::query_as("SELECT id, status FROM alerts WHERE id = 'd9'")
                .fetch_optional(&pool)
                .await
                .unwrap();
        if let Some((id, status)) = row {
            assert_eq!(id, "d9");
            assert_eq!(status, "");
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for sql sink insert"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // 4. LEFT JOIN enrichment against the TYPE=sql lookup table.
    let resp = client
        .put(format!(
            "{}/metadata/sources/sql/confKeys/sql_cfg",
            base_url
        ))
        .json(&json!({"url": db_url, "table": "alerts"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .post(format!("{}/tables", base_url))
        .json(&json!({
            "sql": "CREATE TABLE sql_tbl () WITH (TYPE=\"sql\", CONF_KEY=\"sql_cfg\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM sensor_sql () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let mut rx = state.stream_bus.subscribe("join_sql_res");
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_join_sql",
            "sql": "SELECT id, temp, status FROM sensor_sql LEFT JOIN sql_tbl ON sensor_sql.id = sql_tbl.id",
            "actions": [{"memory": {"topic": "join_sql_res"}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{}/streams/sensor_sql/data", base_url))
        .json(&json!({"id": "d1", "temp": 25.0}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let joined = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for sql join output")
        .expect("join_sql_res topic closed");
    assert_eq!(joined.data.get("id"), Some(&json!("d1")));
    assert_eq!(joined.data.get("temp"), Some(&json!(25.0)));
    assert_eq!(joined.data.get("status"), Some(&json!("active")));

    let _ = tokio::fs::remove_file(&db_path).await;
}

#[tokio::test]
async fn test_graph_rule_lifecycle_and_dag_execution() {
    let (base_url, _handle, _state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM graph_test_stream () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let graph = json!({
        "nodes": {
            "src": {"type": "source", "nodeType": "stream", "props": {"sourceName": "graph_test_stream"}},
            "f": {"type": "operator", "nodeType": "filter", "props": {"expr": "temperature > 25"}},
            "out": {"type": "sink", "nodeType": "log", "props": {}}
        },
        "topo": {
            "sources": ["src"],
            "edges": {"src": ["f"], "f": ["out"]}
        }
    });

    // Validate the graph rule without deploying it.
    let resp = client
        .post(format!("{}/rules/validate", base_url))
        .json(&json!({ "id": "rule_graph_val", "graph": graph }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Deploy the compiled DAG as a live rule.
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({ "id": "rule_graph_active", "graph": graph }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // The native DAG topology is reported back.
    let resp = client
        .get(format!("{}/rules/rule_graph_active/topo", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let topo: serde_json::Value = resp.json().await.unwrap();
    let sources = topo["sources"]
        .as_array()
        .expect("topo.sources is an array");
    assert!(
        sources
            .iter()
            .any(|s| s == "graph_test_stream" || s == "src"),
        "unexpected topo sources: {}",
        topo
    );
    assert_eq!(topo["edges"]["src"], json!(["f"]));

    // A matching record flows through the compiled filter to the sink.
    let resp = client
        .post(format!("{}/streams/graph_test_stream/data", base_url))
        .json(&json!({"temperature": 32.0}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let status: serde_json::Value = client
            .get(format!("{}/rules/rule_graph_active/status", base_url))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if status["sourceRecordsInTotal"].as_u64().unwrap_or(0) >= 1
            && status["sinkRecordsOutTotal"].as_u64().unwrap_or(0) >= 1
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for graph rule metrics: {}",
            status
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // Cleanup: drop the rule (the stream stays for other tests to ignore).
    let resp = client
        .delete(format!("{}/rules/rule_graph_active", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_hopping_window_overlapping_execution() {
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM hop_stream () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Subscribe before the rule exists so no window output is lost.
    let mut sink_rx = state.stream_bus.subscribe("hop_sink_topic");

    // Window length 300ms, hop 100ms: consecutive hops overlap heavily.
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_hop_test",
            "sql": "SELECT count(*) AS cnt, sum(val) AS total FROM hop_stream GROUP BY HOPPINGWINDOW(ms, 300, 100)",
            "actions": [{"memory": {"topic": "hop_sink_topic"}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    async fn post_data(client: &reqwest::Client, base_url: &str, payload: serde_json::Value) {
        let resp = client
            .post(format!("{}/streams/hop_stream/data", base_url))
            .json(&payload)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }

    async fn recv_output(rx: &mut rekuiper_core::StreamReceiver) -> serde_json::Value {
        tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting for window output")
            .expect("hop_sink_topic closed")
            .data
            .into_iter()
            .collect::<serde_json::Map<String, serde_json::Value>>()
            .into()
    }

    // Phase 1: two events in one POST land in the same hop-age bracket; the
    // first emission covering both proves the window fired over buffered rows.
    post_data(&client, &base_url, json!([{"val": 10}, {"val": 20}])).await;
    let hop1 = recv_output(&mut sink_rx).await;
    assert_eq!(hop1["cnt"], json!(2), "hop 1 output: {}", hop1);
    assert_eq!(hop1["total"], json!(30), "hop 1 output: {}", hop1);

    // Phase 2 (overlap): a third event arrives while the first two are still
    // inside the 300ms window — the next hop must retain all three. Stale
    // duplicate emissions are skipped by waiting for cnt == 3.
    post_data(&client, &base_url, json!([{"val": 30}])).await;
    let hop2 = loop {
        let out = recv_output(&mut sink_rx).await;
        if out["cnt"] == json!(3) {
            break out;
        }
    };
    assert_eq!(hop2["total"], json!(60), "hop 2 output: {}", hop2);

    // Phase 3 (expiration): wait until every buffered record is older than
    // the 300ms window, drain stale emissions, then send two fresh events.
    // The next hop must contain exactly the fresh pair.
    tokio::time::sleep(std::time::Duration::from_millis(450)).await;
    while tokio::time::timeout(std::time::Duration::from_millis(50), sink_rx.recv())
        .await
        .is_ok()
    {}
    post_data(&client, &base_url, json!([{"val": 40}, {"val": 30}])).await;
    let hop3 = recv_output(&mut sink_rx).await;
    assert_eq!(hop3["cnt"], json!(2), "hop 3 output: {}", hop3);
    assert_eq!(hop3["total"], json!(70), "hop 3 output: {}", hop3);

    // Metrics: all five source records seen; at least the three key hops out.
    let status: serde_json::Value = client
        .get(format!("{}/rules/rule_hop_test/status", base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["sourceRecordsInTotal"], json!(5));
    assert!(
        status["sinkRecordsOutTotal"].as_u64().unwrap_or(0) >= 3,
        "status: {}",
        status
    );

    // Delete the rule cleanly.
    let resp = client
        .delete(format!("{}/rules/rule_hop_test", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_sliding_window_event_triggered_execution() {
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM slide_stream () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Subscribe before the rule exists so no window output is lost.
    let mut sink_rx = state.stream_bus.subscribe("slide_sink_topic");

    // Trailing 300ms horizon, fired per event (no delay).
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_slide_test",
            "sql": "SELECT count(*) AS cnt, sum(val) AS total FROM slide_stream GROUP BY SLIDINGWINDOW(ms, 300)",
            "actions": [{"memory": {"topic": "slide_sink_topic"}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    async fn post_val(client: &reqwest::Client, base_url: &str, val: i64) {
        let resp = client
            .post(format!("{}/streams/slide_stream/data", base_url))
            .json(&json!({"val": val}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }

    async fn recv_output(rx: &mut rekuiper_core::StreamReceiver) -> serde_json::Value {
        tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting for window output")
            .expect("slide_sink_topic closed")
            .data
            .into_iter()
            .collect::<serde_json::Map<String, serde_json::Value>>()
            .into()
    }

    // Each arrival immediately emits over its trailing horizon.
    post_val(&client, &base_url, 10).await;
    let hop1 = recv_output(&mut sink_rx).await;
    assert_eq!(hop1["cnt"], json!(1), "event 1 output: {}", hop1);
    assert_eq!(hop1["total"], json!(10), "event 1 output: {}", hop1);

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    post_val(&client, &base_url, 20).await;
    let hop2 = recv_output(&mut sink_rx).await;
    assert_eq!(hop2["cnt"], json!(2), "event 2 output: {}", hop2);
    assert_eq!(hop2["total"], json!(30), "event 2 output: {}", hop2);

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    post_val(&client, &base_url, 30).await;
    let hop3 = recv_output(&mut sink_rx).await;
    assert_eq!(hop3["cnt"], json!(3), "event 3 output: {}", hop3);
    assert_eq!(hop3["total"], json!(60), "event 3 output: {}", hop3);

    // After 350ms all three are past the horizon: a fresh event sees only itself.
    tokio::time::sleep(std::time::Duration::from_millis(350)).await;
    post_val(&client, &base_url, 40).await;
    let hop4 = recv_output(&mut sink_rx).await;
    assert_eq!(hop4["cnt"], json!(1), "event 4 output: {}", hop4);
    assert_eq!(hop4["total"], json!(40), "event 4 output: {}", hop4);

    // Metrics: four source records, four window emissions.
    let status: serde_json::Value = client
        .get(format!("{}/rules/rule_slide_test/status", base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["sourceRecordsInTotal"], json!(4));
    assert_eq!(status["sinkRecordsOutTotal"], json!(4));

    // Delete the rule cleanly.
    let resp = client
        .delete(format!("{}/rules/rule_slide_test", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_count_window_hopping_overlap() {
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM count_hop_stream () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Subscribe before the rule exists so no window output is lost.
    let mut sink_rx = state.stream_bus.subscribe("count_hop_sink_topic");

    // Overlapping count window: 4-row windows advancing 2 rows at a time.
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_count_hop_test",
            "sql": "SELECT count(*) AS cnt, sum(val) AS total FROM count_hop_stream GROUP BY COUNTWINDOW(4, 2)",
            "actions": [{"memory": {"topic": "count_hop_sink_topic"}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    async fn post_vals(client: &reqwest::Client, base_url: &str, vals: &[i64]) {
        let payload: Vec<serde_json::Value> = vals.iter().map(|v| json!({"val": v})).collect();
        let resp = client
            .post(format!("{}/streams/count_hop_stream/data", base_url))
            .json(&payload)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }

    async fn recv_output(rx: &mut rekuiper_core::StreamReceiver) -> serde_json::Value {
        tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting for window output")
            .expect("count_hop_sink_topic closed")
            .data
            .into_iter()
            .collect::<serde_json::Map<String, serde_json::Value>>()
            .into()
    }

    async fn wait_source(client: &reqwest::Client, base_url: &str, want: u64) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let status: serde_json::Value = client
                .get(format!("{}/rules/rule_count_hop_test/status", base_url))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            if status["sourceRecordsInTotal"].as_u64().unwrap_or(0) >= want {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for {} source records",
                want
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    // Events 1..3 buffer without firing (3 < 4).
    post_vals(&client, &base_url, &[10, 20, 30]).await;
    wait_source(&client, &base_url, 3).await;
    assert!(
        sink_rx.try_recv().is_err(),
        "no window output expected before 4 records"
    );

    // Event 4 completes the first window: cnt = 4, total = 100.
    post_vals(&client, &base_url, &[40]).await;
    let hop1 = recv_output(&mut sink_rx).await;
    assert_eq!(hop1["cnt"], json!(4), "hop 1 output: {}", hop1);
    assert_eq!(hop1["total"], json!(100), "hop 1 output: {}", hop1);

    // Oldest two drain; 30 and 40 are retained across the hop.
    post_vals(&client, &base_url, &[50]).await;
    wait_source(&client, &base_url, 5).await;
    assert!(
        sink_rx.try_recv().is_err(),
        "no window output expected at 3 buffered records"
    );
    post_vals(&client, &base_url, &[60]).await;
    let hop2 = recv_output(&mut sink_rx).await;
    assert_eq!(hop2["cnt"], json!(4), "hop 2 output: {}", hop2);
    assert_eq!(hop2["total"], json!(180), "hop 2 output: {}", hop2);

    // Next hop slides to [50, 60, 70, 80].
    post_vals(&client, &base_url, &[70, 80]).await;
    let hop3 = recv_output(&mut sink_rx).await;
    assert_eq!(hop3["cnt"], json!(4), "hop 3 output: {}", hop3);
    assert_eq!(hop3["total"], json!(260), "hop 3 output: {}", hop3);

    // Metrics: 8 records in, 3 window emissions out.
    let status: serde_json::Value = client
        .get(format!("{}/rules/rule_count_hop_test/status", base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["sourceRecordsInTotal"], json!(8));
    assert_eq!(status["sinkRecordsOutTotal"], json!(3));

    // Delete the rule cleanly.
    let resp = client
        .delete(format!("{}/rules/rule_count_hop_test", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_event_time_watermark_and_late_tolerance() {
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM et_stream () WITH (FORMAT=\"json\", TIMESTAMP=\"ts\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Subscribe before the rule exists so no window output is lost.
    let mut sink_rx = state.stream_bus.subscribe("et_sink_topic");

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_et_test",
            "sql": "SELECT count(*) AS cnt, max(ts) AS max_ts, window_start() AS ws, window_end() AS we FROM et_stream GROUP BY SLIDINGWINDOW(ms, 500)",
            "actions": [{"memory": {"topic": "et_sink_topic"}}],
            "options": {"isEventTime": true, "lateTolerance": 100}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    async fn post_event(client: &reqwest::Client, base_url: &str, val: i64, ts: i64) {
        let resp = client
            .post(format!("{}/streams/et_stream/data", base_url))
            .json(&json!({"val": val, "ts": ts}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }

    async fn recv_output(rx: &mut rekuiper_core::StreamReceiver) -> serde_json::Value {
        tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting for window output")
            .expect("et_sink_topic closed")
            .data
            .into_iter()
            .collect::<serde_json::Map<String, serde_json::Value>>()
            .into()
    }

    // Event 1 (t=1000): watermark 900, output cnt=1/max_ts=1000.
    post_event(&client, &base_url, 1, 1000).await;
    let out1 = recv_output(&mut sink_rx).await;
    assert_eq!(out1["cnt"], json!(1), "output 1: {}", out1);
    assert_eq!(out1["max_ts"], json!(1000), "output 1: {}", out1);

    // Event 2 (t=950 >= W=900) is accepted; its window ends at 950, excluding 1000.
    post_event(&client, &base_url, 2, 950).await;
    let out2 = recv_output(&mut sink_rx).await;
    assert_eq!(out2["cnt"], json!(1), "output 2: {}", out2);
    assert_eq!(out2["max_ts"], json!(950), "output 2: {}", out2);
    assert_eq!(out2["ws"], json!(450));
    assert_eq!(out2["we"], json!(950));

    // Event 3 (t=1500): watermark 1400, horizon [1000, 1500] drops 950.
    post_event(&client, &base_url, 3, 1500).await;
    let out3 = recv_output(&mut sink_rx).await;
    assert_eq!(out3["cnt"], json!(2), "output 3: {}", out3);
    assert_eq!(out3["max_ts"], json!(1500), "output 3: {}", out3);

    // Event 4 (t=1100 < W=1400): late, dropped with no output.
    post_event(&client, &base_url, 4, 1100).await;
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), sink_rx.recv())
            .await
            .is_err(),
        "late event must not emit"
    );

    // Metrics prove the dropped event still counted as a source record.
    let status: serde_json::Value = client
        .get(format!("{}/rules/rule_et_test/status", base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["sourceRecordsInTotal"], json!(4));
    assert_eq!(status["sinkRecordsOutTotal"], json!(3));

    // Delete the rule cleanly.
    let resp = client
        .delete(format!("{}/rules/rule_et_test", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_buffer_length_and_send_error_options() {
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    // Shared source stream for both rules.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM opt_stream () WITH (TYPE=\"memory\", FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    async fn post_error(client: &reqwest::Client, base_url: &str) {
        let resp = client
            .post(format!("{}/streams/opt_stream/data", base_url))
            .json(&json!({"error": "sensor connection reset"}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }

    async fn rule_status(
        client: &reqwest::Client,
        base_url: &str,
        rule: &str,
    ) -> serde_json::Value {
        client
            .get(format!("{}/rules/{}/status", base_url, rule))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    // sendError: true — the error record is formatted and forwarded immediately.
    let mut sink_rx_true = state.stream_bus.subscribe("sink_err_true");
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_err_true",
            "sql": "SELECT * FROM opt_stream",
            "actions": [{"memory": {"topic": "sink_err_true"}}],
            "options": {"sendError": true, "bufferLength": 50}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    post_error(&client, &base_url).await;
    let out = tokio::time::timeout(std::time::Duration::from_secs(2), sink_rx_true.recv())
        .await
        .expect("sendError:true must forward the error record")
        .expect("sink_err_true closed");
    let out_json: serde_json::Value = out
        .data
        .into_iter()
        .collect::<serde_json::Map<String, serde_json::Value>>()
        .into();
    assert_eq!(
        out_json["error"],
        json!("sensor connection reset"),
        "forwarded: {}",
        out_json
    );
    assert_eq!(
        out_json["rule_id"],
        json!("rule_err_true"),
        "forwarded: {}",
        out_json
    );
    let status = rule_status(&client, &base_url, "rule_err_true").await;
    assert_eq!(status["sourceRecordsInTotal"], json!(1));
    assert_eq!(status["sinkRecordsOutTotal"], json!(1));
    assert_eq!(status["exceptionsTotal"], json!(1));

    // sendError: false (default) — counted as an exception, never forwarded.
    let mut sink_rx_false = state.stream_bus.subscribe("sink_err_false");
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_err_false",
            "sql": "SELECT * FROM opt_stream",
            "actions": [{"memory": {"topic": "sink_err_false"}}],
            "options": {"sendError": false, "bufferLength": 50}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    post_error(&client, &base_url).await;
    // Wait until the rule has demonstrably processed the error record (source
    // counter), so the subsequent empty assertion is meaningful and not a race.
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let s = rule_status(&client, &base_url, "rule_err_false").await;
            if s["sourceRecordsInTotal"] == json!(1) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("rule_err_false never processed the error record");
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), sink_rx_false.recv())
            .await
            .is_err(),
        "sendError:false must not forward error records"
    );
    let status = rule_status(&client, &base_url, "rule_err_false").await;
    assert_eq!(status["sinkRecordsOutTotal"], json!(0));
    assert_eq!(status["exceptionsTotal"], json!(1));

    // Clean delete of both rules.
    for rule in ["rule_err_true", "rule_err_false"] {
        let resp = client
            .delete(format!("{}/rules/{}", base_url, rule))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }
}

#[tokio::test]
async fn test_dynamic_metadata_functions() {
    let (base_url, _handle, _state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    async fn fetch_functions(client: &reqwest::Client, base_url: &str) -> Vec<serde_json::Value> {
        let resp = client
            .get(format!("{}/metadata/functions", base_url))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        resp.json().await.unwrap()
    }

    fn find<'a>(list: &'a [serde_json::Value], name: &str) -> &'a serde_json::Value {
        list.iter()
            .find(|entry| entry["name"] == json!(name))
            .unwrap_or_else(|| panic!("function {} missing from metadata", name))
    }

    // The catalog covers every built-in function.
    let functions = fetch_functions(&client, &base_url).await;
    assert!(
        functions.len() >= 185,
        "expected >= 185 functions, got {}",
        functions.len()
    );
    for name in [
        "crc32",
        "day_of_week",
        "object_pick",
        "percentile",
        "split_value",
        "row_number",
    ] {
        find(&functions, name);
    }

    // Spot-check categories and the aggregate flag.
    assert_eq!(find(&functions, "crc32")["category"], json!("crypto"));
    assert_eq!(find(&functions, "crc32")["aggregate"], json!(false));
    assert_eq!(
        find(&functions, "day_of_week")["category"],
        json!("datetime")
    );
    assert_eq!(find(&functions, "object_pick")["category"], json!("array"));
    assert_eq!(
        find(&functions, "percentile")["category"],
        json!("aggregate")
    );
    assert_eq!(find(&functions, "percentile")["aggregate"], json!(true));
    assert_eq!(
        find(&functions, "row_number")["category"],
        json!("aggregate")
    );
    assert_eq!(find(&functions, "row_number")["aggregate"], json!(true));

    // Registered plugin functions extend the catalog dynamically.
    let resp = client
        .post(format!("{}/plugins/functions", base_url))
        .json(&json!({
            "name": "meta_test_plugin",
            "functions": ["custom_fn1", "custom_fn2"]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let functions = fetch_functions(&client, &base_url).await;
    let custom = find(&functions, "custom_fn1");
    assert_eq!(custom["category"], json!("plugin"));
    find(&functions, "custom_fn2");

    // Deleting the plugin removes its functions again.
    let resp = client
        .delete(format!("{}/plugins/functions/meta_test_plugin", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let functions = fetch_functions(&client, &base_url).await;
    assert!(
        !functions
            .iter()
            .any(|entry| entry["name"] == json!("custom_fn1")),
        "custom_fn1 must disappear after plugin deletion"
    );
    assert!(
        !functions
            .iter()
            .any(|entry| entry["name"] == json!("custom_fn2")),
        "custom_fn2 must disappear after plugin deletion"
    );
}

#[tokio::test]
async fn test_rule_schema_introspection() {
    let (base_url, _handle, _state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM demo () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    async fn create_rule(client: &reqwest::Client, base_url: &str, id: &str, sql: &str) {
        let resp = client
            .post(format!("{}/rules", base_url))
            .json(&json!({
                "id": id,
                "sql": sql,
                "actions": [{"memory": {"topic": "schema_sink"}}]
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    }

    async fn fetch_schema(
        client: &reqwest::Client,
        base_url: &str,
        id: &str,
    ) -> (reqwest::StatusCode, serde_json::Value) {
        let resp = client
            .get(format!("{}/rules/{}/schema", base_url, id))
            .send()
            .await
            .unwrap();
        let status = resp.status();
        // Error responses are plain text; only success carries a JSON schema.
        let body = resp.json().await.unwrap_or(serde_json::Value::Null);
        (status, body)
    }

    // Dynamic identifier stays "any"; bigint literal widens `b + 1` to float.
    create_rule(
        &client,
        &base_url,
        "rule_schema_test",
        "SELECT a, b + 1 AS c FROM demo",
    )
    .await;
    let (status, schema) = fetch_schema(&client, &base_url, "rule_schema_test").await;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert_eq!(schema, json!({"a": "any", "c": "float"}));

    // Typed function calls resolve to their static return types.
    create_rule(
        &client,
        &base_url,
        "rule_typed_test",
        "SELECT concat(name, '!') AS greeting, count(*) AS cnt, isnull(val) AS is_missing FROM demo",
    )
    .await;
    let (status, schema) = fetch_schema(&client, &base_url, "rule_typed_test").await;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert_eq!(schema["greeting"], json!("string"), "schema: {}", schema);
    assert_eq!(schema["cnt"], json!("bigint"), "schema: {}", schema);
    assert_eq!(schema["is_missing"], json!("boolean"), "schema: {}", schema);

    // Unknown rule ids 404.
    let (status, _) = fetch_schema(&client, &base_url, "non_existent").await;
    assert_eq!(status, reqwest::StatusCode::NOT_FOUND);

    // Clean up rules and stream.
    for rule in ["rule_schema_test", "rule_typed_test"] {
        let resp = client
            .delete(format!("{}/rules/{}", base_url, rule))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }
    let resp = client
        .delete(format!("{}/streams/demo", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_process_cpu_and_memory_metrics() {
    let (base_url, _handle, _state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM demo () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "metric_test_rule",
            "sql": "SELECT * FROM demo",
            "actions": [{"memory": {"topic": "metric_sink"}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // Per-rule CPU/memory reflects the live process.
    let resp = client
        .get(format!("{}/rules/metric_test_rule/cpu", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let cpu: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(cpu["rule_id"], json!("metric_test_rule"));
    assert!(
        cpu["memory"].as_u64().unwrap_or(0) > 0,
        "memory must be positive: {}",
        cpu
    );
    assert!(
        cpu["cpu"].as_f64().unwrap_or(-1.0) >= 0.0,
        "cpu must be non-negative: {}",
        cpu
    );

    // Aggregate usage maps every known rule id.
    let resp = client
        .get(format!("{}/rules/usage/cpu", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let usage: serde_json::Value = resp.json().await.unwrap();
    assert!(usage.is_object(), "usage must be an object: {}", usage);
    assert!(
        usage.get("metric_test_rule").is_some(),
        "usage must contain the rule: {}",
        usage
    );

    // Metrics dump carries live process and system memory.
    let resp = client
        .get(format!("{}/metrics/dump", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let dump: serde_json::Value = resp.json().await.unwrap();
    assert!(
        dump["metrics"]["memory"].as_u64().unwrap_or(0) > 0,
        "dump memory must be positive: {}",
        dump
    );

    // Unknown rule ids 404.
    let resp = client
        .get(format!("{}/rules/non_existent/cpu", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // Clean up rule and stream.
    let resp = client
        .delete(format!("{}/rules/metric_test_rule", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let resp = client
        .delete(format!("{}/streams/demo", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_metadata_source_and_sink_documents() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 1. List source metadata: contains expanded set of connectors.
    let resp = client
        .get(format!("{}/metadata/sources", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let sources: Vec<serde_json::Value> = resp.json().await.unwrap();
    let source_names: Vec<&str> = sources
        .iter()
        .filter_map(|s| s.get("name").and_then(|v| v.as_str()))
        .collect();
    for expected in &[
        "mqtt",
        "file",
        "http",
        "httppull",
        "redis",
        "simulator",
        "memory",
    ] {
        assert!(
            source_names.contains(expected),
            "sources must contain '{}': {:?}",
            expected,
            source_names
        );
    }

    // 2. List sink metadata: contains expanded set of connectors.
    let resp = client
        .get(format!("{}/metadata/sinks", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let sinks: Vec<serde_json::Value> = resp.json().await.unwrap();
    let sink_names: Vec<&str> = sinks
        .iter()
        .filter_map(|s| s.get("name").and_then(|v| v.as_str()))
        .collect();
    for expected in &["mqtt", "file", "log", "memory", "redis", "rest"] {
        assert!(
            sink_names.contains(expected),
            "sinks must contain '{}': {:?}",
            expected,
            sink_names
        );
    }

    // 3. Disk-backed source metadata for mqtt: has about and properties.
    let resp = client
        .get(format!("{}/metadata/sources/mqtt", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let mqtt_src: serde_json::Value = resp.json().await.unwrap();
    assert!(
        mqtt_src.get("about").is_some(),
        "mqtt source metadata must have 'about': {}",
        mqtt_src
    );
    assert!(
        mqtt_src.get("properties").is_some() || mqtt_src.get("dataSource").is_some(),
        "mqtt source metadata must have properties or dataSource: {}",
        mqtt_src
    );

    // 4. Disk-backed source metadata for file.
    let resp = client
        .get(format!("{}/metadata/sources/file", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let file_src: serde_json::Value = resp.json().await.unwrap();
    assert!(
        file_src.get("about").is_some(),
        "file source metadata must have 'about': {}",
        file_src
    );

    // 5. Disk-backed sink metadata for mqtt.
    let resp = client
        .get(format!("{}/metadata/sinks/mqtt", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let mqtt_sink: serde_json::Value = resp.json().await.unwrap();
    assert!(
        mqtt_sink.get("about").is_some(),
        "mqtt sink metadata must have 'about': {}",
        mqtt_sink
    );

    // 6. Source YAML for mqtt returns real configuration from disk.
    let resp = client
        .get(format!("{}/metadata/sources/yaml/mqtt", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let mqtt_yaml: serde_json::Value = resp.json().await.unwrap();
    assert!(
        mqtt_yaml.get("default").is_some(),
        "mqtt source config keys must contain 'default': {}",
        mqtt_yaml
    );
    assert_eq!(
        mqtt_yaml["default"]["server"].as_str(),
        Some("tcp://127.0.0.1:1883")
    );

    // 7. Source YAML for file returns real configuration from disk.
    let resp = client
        .get(format!("{}/metadata/sources/yaml/file", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let file_yaml: serde_json::Value = resp.json().await.unwrap();
    assert!(
        file_yaml.get("default").is_some(),
        "file source config keys must contain 'default': {}",
        file_yaml
    );
    assert_eq!(file_yaml["default"]["fileType"].as_str(), Some("json"));

    // 8. Invalid resource names are rejected with 400 Bad Request.
    let resp = client
        .get(format!("{}/metadata/sources/invalid%20name", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_connection_metadata_and_resource_discovery() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 1. Initially connections are empty.
    let resp = client
        .get(format!("{}/metadata/connections", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let conns: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(conns.is_empty(), "initially empty: {:?}", conns);

    // 2. Create connection.
    let resp = client
        .post(format!("{}/connections", base_url))
        .json(&serde_json::json!({
            "id": "test_conn_1",
            "type": "mqtt",
            "server": "tcp://127.0.0.1:1883"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // 3. /metadata/connections reflects the new connection.
    let resp = client
        .get(format!("{}/metadata/connections", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let conns: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(conns.len(), 1);
    assert_eq!(conns[0]["id"], "test_conn_1");

    // 4. /metadata/connections/:name returns connection details.
    let resp = client
        .get(format!("{}/metadata/connections/test_conn_1", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let conn_meta: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(conn_meta["id"], "test_conn_1");
    assert_eq!(conn_meta["type"], "mqtt");

    // 5. /metadata/resources reflects connection as a live resource.
    let resp = client
        .get(format!("{}/metadata/resources", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let resources: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(resources.len(), 1);
    assert_eq!(resources[0]["id"], "test_conn_1");
    assert_eq!(resources[0]["type"], "mqtt");

    // 6. /metadata/connections/yaml/:name returns connection YAML from disk.
    let resp = client
        .get(format!("{}/metadata/connections/yaml/mqtt", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let yaml_resp: serde_json::Value = resp.json().await.unwrap();
    assert!(yaml_resp.get("yaml").is_some());

    // 7. Delete connection.
    let resp = client
        .delete(format!("{}/connections/test_conn_1", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 8. /metadata/connections is empty again.
    let resp = client
        .get(format!("{}/metadata/connections", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let conns: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(conns.is_empty());
}

#[tokio::test]
async fn test_confkeys_persistence_and_registration() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 1. Source confKeys CRUD
    let resp = client
        .put(format!(
            "{}/metadata/sources/mqtt/confKeys/custom_conf",
            base_url
        ))
        .json(&serde_json::json!({"server": "tcp://broker:1883"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(format!(
            "{}/metadata/sources/mqtt/confKeys/custom_conf",
            base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let cfg: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(cfg["server"], "tcp://broker:1883");

    let resp = client
        .delete(format!(
            "{}/metadata/sources/mqtt/confKeys/custom_conf",
            base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(format!(
            "{}/metadata/sources/mqtt/confKeys/custom_conf",
            base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // 2. Sink confKeys CRUD
    let resp = client
        .put(format!(
            "{}/metadata/sinks/mqtt/confKeys/sink_conf",
            base_url
        ))
        .json(&serde_json::json!({"topic": "out/events"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(format!(
            "{}/metadata/sinks/mqtt/confKeys/sink_conf",
            base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let cfg: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(cfg["topic"], "out/events");

    let resp = client
        .delete(format!(
            "{}/metadata/sinks/mqtt/confKeys/sink_conf",
            base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(format!(
            "{}/metadata/sinks/mqtt/confKeys/sink_conf",
            base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // 3. Connection confKeys CRUD
    let resp = client
        .put(format!(
            "{}/metadata/connections/mqtt/confKeys/conn_conf",
            base_url
        ))
        .json(&serde_json::json!({"server": "tcp://conn:1883"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(format!(
            "{}/metadata/connections/mqtt/confKeys/conn_conf",
            base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let cfg: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(cfg["server"], "tcp://conn:1883");

    let resp = client
        .delete(format!(
            "{}/metadata/connections/mqtt/confKeys/conn_conf",
            base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(format!(
            "{}/metadata/connections/mqtt/confKeys/conn_conf",
            base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // 4. Source/sink/lookup connection bindings
    let resp = client
        .post(format!("{}/metadata/sources/connection/mqtt", base_url))
        .json(&serde_json::json!({"id": "mqtt_reg_src", "server": "tcp://127.0.0.1:1883"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(format!("{}/metadata/connections/mqtt_reg_src", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_rule_tags_lifecycle_and_matching() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 1. Create a stream for rules to use
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&serde_json::json!({
            "sql": "create stream stream_tags () WITH (FORMAT=\"JSON\");"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // 2. Create rule 1 with initial tags ["edge", "production"]
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&serde_json::json!({
            "id": "rule_tag_1",
            "sql": "SELECT * FROM stream_tags",
            "actions": [{"log": {}}],
            "tags": ["edge", "production"]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // 3. Create rule 2 with no initial tags
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&serde_json::json!({
            "id": "rule_tag_2",
            "sql": "SELECT * FROM stream_tags",
            "actions": [{"log": {}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // 4. Verify rule 1 tags in GET /rules/:id
    let resp = client
        .get(format!("{}/rules/rule_tag_1", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let r1: serde_json::Value = resp.json().await.unwrap();
    let r1_tags: Vec<String> = serde_json::from_value(r1["tags"].clone()).unwrap();
    assert_eq!(r1_tags, vec!["edge", "production"]);

    // 5. Match tags via query parameters
    let resp = client
        .get(format!("{}/rules/tags/match?tags=edge", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let matched: Vec<String> = resp.json().await.unwrap();
    assert_eq!(matched, vec!["rule_tag_1"]);

    let resp = client
        .get(format!(
            "{}/rules/tags/match?tags=edge,production",
            base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let matched: Vec<String> = resp.json().await.unwrap();
    assert_eq!(matched, vec!["rule_tag_1"]);

    let resp = client
        .get(format!("{}/rules/tags/match?tags=staging", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let matched: Vec<String> = resp.json().await.unwrap();
    assert!(matched.is_empty());

    // 6. PATCH /rules/rule_tag_2/tags to add ["staging", "production"]
    let resp = client
        .patch(format!("{}/rules/rule_tag_2/tags", base_url))
        .json(&serde_json::json!({"tags": ["staging", "production"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 7. Query match for "production" should now match both rules
    let resp = client
        .get(format!("{}/rules/tags/match?tags=production", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let matched: Vec<String> = resp.json().await.unwrap();
    assert_eq!(matched, vec!["rule_tag_1", "rule_tag_2"]);

    // 8. Test POST /rules/tags/match with body {"keys": ["staging"]}
    let resp = client
        .post(format!("{}/rules/tags/match", base_url))
        .json(&serde_json::json!({"keys": ["staging"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let matched: Vec<String> = resp.json().await.unwrap();
    assert_eq!(matched, vec!["rule_tag_2"]);

    // 9. PUT /rules/rule_tag_1/tags to replace tags with ["canary"]
    let resp = client
        .put(format!("{}/rules/rule_tag_1/tags", base_url))
        .json(&serde_json::json!({"tags": ["canary"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(format!("{}/rules/tags/match?tags=canary", base_url))
        .send()
        .await
        .unwrap();
    let matched: Vec<String> = resp.json().await.unwrap();
    assert_eq!(matched, vec!["rule_tag_1"]);

    let resp = client
        .get(format!("{}/rules/tags/match?tags=edge", base_url))
        .send()
        .await
        .unwrap();
    let matched: Vec<String> = resp.json().await.unwrap();
    assert!(matched.is_empty());

    // 10. DELETE /rules/rule_tag_2/tags with {"keys": ["staging"]} removes staging only
    let resp = client
        .delete(format!("{}/rules/rule_tag_2/tags", base_url))
        .json(&serde_json::json!({"keys": ["staging"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(format!("{}/rules/tags/match?tags=production", base_url))
        .send()
        .await
        .unwrap();
    let matched: Vec<String> = resp.json().await.unwrap();
    assert_eq!(matched, vec!["rule_tag_2"]);

    // 11. DELETE /rules/rule_tag_2/tags with {} removes all tags
    let resp = client
        .delete(format!("{}/rules/rule_tag_2/tags", base_url))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(format!("{}/rules/tags/match?tags=production", base_url))
        .send()
        .await
        .unwrap();
    let matched: Vec<String> = resp.json().await.unwrap();
    assert!(matched.is_empty());

    // 12. Cleanup rules & stream
    let _ = client
        .delete(format!("{}/rules/rule_tag_1", base_url))
        .send()
        .await;
    let _ = client
        .delete(format!("{}/rules/rule_tag_2", base_url))
        .send()
        .await;
    let _ = client
        .delete(format!("{}/streams/stream_tags", base_url))
        .send()
        .await;
}

#[tokio::test]
async fn test_rule_execution_trace_buffer() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 1. Create stream
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&serde_json::json!({
            "sql": "create stream stream_trace () WITH (FORMAT=\"JSON\");"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // 2. Create rule
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&serde_json::json!({
            "id": "rule_trace_1",
            "sql": "SELECT * FROM stream_trace",
            "actions": [{"log": {}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // 3. Initial trace IDs list should be empty
    let resp = client
        .get(format!("{}/trace/rule/rule_trace_1", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let trace_ids: Vec<String> = resp.json().await.unwrap();
    assert!(trace_ids.is_empty());

    // 4. Non-existent rule trace start/stop should return 404
    let resp = client
        .post(format!("{}/rules/nonexistent/trace/start", base_url))
        .json(&serde_json::json!({"strategy": "always"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    let resp = client
        .post(format!("{}/rules/nonexistent/trace/stop", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // 5. Start trace on rule_trace_1
    let resp = client
        .post(format!("{}/rules/rule_trace_1/trace/start", base_url))
        .json(&serde_json::json!({"strategy": "always"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 6. Push 3 records to stream_trace
    for i in 1..=3 {
        let resp = client
            .post(format!("{}/streams/stream_trace/data", base_url))
            .json(&serde_json::json!({
                "temperature": 20.0 + (i as f64),
                "humidity": 50 + i
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // 7. Verify 3 trace IDs captured
    let resp = client
        .get(format!("{}/trace/rule/rule_trace_1", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let trace_ids: Vec<String> = resp.json().await.unwrap();
    assert_eq!(trace_ids.len(), 3);

    // 8. Test limit query param
    let resp = client
        .get(format!("{}/trace/rule/rule_trace_1?limit=2", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let limited_ids: Vec<String> = resp.json().await.unwrap();
    assert_eq!(limited_ids.len(), 2);

    // 9. Inspect a trace by ID
    let sample_id = &trace_ids[0];
    let resp = client
        .get(format!("{}/trace/{}", base_url, sample_id))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let span: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(span["Name"], "rule_trace_1");
    assert_eq!(span["TraceID"], sample_id.as_str());
    assert_eq!(span["ParentSpanID"], "0000000000000000");
    assert!(span["ChildSpan"].is_array());
    let child_spans = span["ChildSpan"].as_array().unwrap();
    assert!(!child_spans.is_empty());
    assert_eq!(child_spans[0]["Name"], "rule_trace_1_decoder");

    // 10. Stop tracing
    let resp = client
        .post(format!("{}/rules/rule_trace_1/trace/stop", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 11. Push a 4th record
    let resp = client
        .post(format!("{}/streams/stream_trace/data", base_url))
        .json(&serde_json::json!({
            "temperature": 35.0,
            "humidity": 75
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // 12. Verify count is still 3 (tracing was stopped)
    let resp = client
        .get(format!("{}/trace/rule/rule_trace_1", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let trace_ids_after: Vec<String> = resp.json().await.unwrap();
    assert_eq!(trace_ids_after.len(), 3);

    // 13. Test /tracer configuration endpoint
    let resp = client
        .post(format!("{}/tracer", base_url))
        .json(&serde_json::json!({
            "service_name": "rekuiper",
            "action": "start",
            "collector_url": "http://localhost:4318"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 14. Cleanup
    let _ = client
        .delete(format!("{}/rules/rule_trace_1", base_url))
        .send()
        .await;
    let _ = client
        .delete(format!("{}/streams/stream_trace", base_url))
        .send()
        .await;
}

#[tokio::test]
async fn test_async_task_lifecycle_and_cancellation() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 1. Unknown task returns 404
    let resp = client
        .get(format!("{}/async/task/nonexistent_task", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // 2. Pre-seeded task_1 returns 200
    let resp = client
        .get(format!("{}/async/task/task_1", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let t1: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(t1["id"], "task_1");
    assert_eq!(t1["status"], "completed");

    // 3. Spawn real background async data import
    let resp = client
        .post(format!("{}/async/data/import", base_url))
        .json(&serde_json::json!({
            "streams": [{
                "name": "async_stream_1",
                "sql": "create stream async_stream_1 () WITH (FORMAT=\"JSON\");",
                "options": {}
            }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let import_resp: serde_json::Value = resp.json().await.unwrap();
    let task_id = import_resp["id"].as_str().unwrap().to_string();
    assert!(task_id.starts_with("dataImport-"));
    assert_eq!(import_resp["status"], "running");

    // 4. Poll status until completed (max 2 seconds)
    let mut completed = false;
    for _ in 0..20 {
        let resp = client
            .get(format!("{}/async/task/{}", base_url, task_id))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        let status_val: serde_json::Value = resp.json().await.unwrap();
        if status_val["status"] == "completed" {
            completed = true;
            assert!(status_val["createdTimestamp"].as_i64().unwrap() > 0);
            assert!(status_val["updatedTimestamp"].as_i64().unwrap() > 0);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    }
    assert!(completed, "Async import task should have completed");

    // 5. Verify that the background import actually created the stream
    let resp = client
        .get(format!("{}/streams/async_stream_1", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 6. Test task cancellation on a new task
    let resp = client
        .post(format!("{}/async/data/import", base_url))
        .json(&serde_json::json!({
            "streams": []
        }))
        .send()
        .await
        .unwrap();
    let task_2_resp: serde_json::Value = resp.json().await.unwrap();
    let task_2_id = task_2_resp["id"].as_str().unwrap();

    let resp = client
        .post(format!("{}/async/task/{}/cancel", base_url, task_2_id))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let cancel_resp: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(cancel_resp["status"], "cancelled");

    let resp = client
        .get(format!("{}/async/task/{}", base_url, task_2_id))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let task_2_status: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(task_2_status["status"], "cancelled");

    // 7. Cleanup
    let _ = client
        .delete(format!("{}/streams/async_stream_1", base_url))
        .send()
        .await;
}

#[tokio::test]
async fn test_batch_request_pipeline() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 1. Empty batch request returns empty array
    let resp = client
        .post(format!("{}/batch/req", base_url))
        .json(&serde_json::json!([]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let items: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(items.is_empty());

    // 2. Batch request creating a stream, checking it, and pinging
    let batch_payload = serde_json::json!([
        {
            "method": "POST",
            "path": "/streams",
            "body": "{\"sql\":\"CREATE STREAM demobatch () WITH (DATASOURCE=\\\"/data1\\\", TYPE=\\\"websocket\\\")\"}"
        },
        {
            "method": "GET",
            "path": "/streams/demobatch"
        },
        {
            "method": "GET",
            "path": "/ping"
        }
    ]);

    let resp = client
        .post(format!("{}/batch/req", base_url))
        .json(&batch_payload)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let items: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(items.len(), 3);

    // Stream creation response
    assert_eq!(items[0]["code"], 201);
    assert!(items[0]["response"]
        .as_str()
        .unwrap()
        .contains("Stream demobatch is created"));

    // Stream get response
    assert_eq!(items[1]["code"], 200);
    assert!(items[1]["response"].as_str().unwrap().contains("demobatch"));

    // Ping response
    assert_eq!(items[2]["code"], 200);
    assert_eq!(items[2]["response"].as_str().unwrap(), "");

    // 3. Verify server state directly
    let resp = client
        .get(format!("{}/streams/demobatch", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 4. Batch request with object body (creating a rule) and error cases
    let batch_2 = serde_json::json!([
        {
            "action": "POST",
            "url": "/rules",
            "payload": {
                "id": "rule_batch_1",
                "sql": "SELECT * FROM demobatch",
                "actions": [{"log": {}}]
            }
        },
        {
            "method": "GET",
            "path": "/rules/rule_batch_1"
        },
        {
            "method": "GET",
            "path": "/streams/non_existent_stream"
        },
        {
            "method": "POST",
            "path": "/batch/req",
            "body": "[]"
        },
        {
            "method": "INVALID_METHOD",
            "path": "/ping"
        }
    ]);

    let resp = client
        .post(format!("{}/batch/req", base_url))
        .json(&batch_2)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let items: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(items.len(), 5);

    // Rule created
    assert_eq!(items[0]["code"], 201);
    assert!(items[0]["response"]
        .as_str()
        .unwrap()
        .contains("rule_batch_1 was created"));

    // Rule fetched
    assert_eq!(items[1]["code"], 200);
    assert!(items[1]["response"]
        .as_str()
        .unwrap()
        .contains("rule_batch_1"));

    // Not found stream (400 JSON describe envelope)
    assert_eq!(items[2]["code"], 400);
    assert!(items[2]["error"].as_str().unwrap().contains("not found"));

    // Nested batch request rejected
    assert_eq!(items[3]["code"], 400);
    assert!(items[3]["error"]
        .as_str()
        .unwrap()
        .contains("nested batch requests are not supported"));

    // Invalid method rejected
    assert_eq!(items[4]["code"], 400);
    assert!(items[4]["error"]
        .as_str()
        .unwrap()
        .contains("unsupported HTTP method"));

    // 5. Cleanup
    let _ = client
        .delete(format!("{}/rules/rule_batch_1", base_url))
        .send()
        .await;
    let _ = client
        .delete(format!("{}/streams/demobatch", base_url))
        .send()
        .await;
}

#[tokio::test]
async fn test_source_and_sink_plugin_registries() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 1. Initial state: no native plugins installed
    for path in ["/plugins/sources", "/plugins/sinks"] {
        let resp = client
            .get(format!("{}{}", base_url, path))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        let items: Vec<serde_json::Value> = resp.json().await.unwrap();
        assert!(items.is_empty());
    }

    // 2. Built-in introspection returns 200 OK
    let resp = client
        .get(format!("{}/plugins/sources/mqtt", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let mqtt_src: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(mqtt_src["name"], "mqtt");
    assert_eq!(mqtt_src["plugin_type"], "source");

    let resp = client
        .get(format!("{}/plugins/sinks/mqtt", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let mqtt_snk: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(mqtt_snk["name"], "mqtt");
    assert_eq!(mqtt_snk["plugin_type"], "sink");

    // Non-existent returns 404
    let resp = client
        .get(format!("{}/plugins/sources/non_existent_src", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // 3. Source plugin CRUD lifecycle (no file URI: local files must exist).
    let resp = client
        .post(format!("{}/plugins/sources", base_url))
        .json(&serde_json::json!({
            "name": "custom_src",
            "description": "Initial Custom Source"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let resp = client
        .get(format!("{}/plugins/sources", base_url))
        .send()
        .await
        .unwrap();
    let sources: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0]["name"], "custom_src");
    assert_eq!(sources[0]["plugin_type"], "source");

    let resp = client
        .get(format!("{}/plugins/sources/custom_src", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let src_info: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(src_info["description"], "Initial Custom Source");

    // Update source plugin
    let resp = client
        .put(format!("{}/plugins/sources/custom_src", base_url))
        .json(&serde_json::json!({
            "description": "Updated Custom Source"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(format!("{}/plugins/sources/custom_src", base_url))
        .send()
        .await
        .unwrap();
    let updated_src: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(updated_src["description"], "Updated Custom Source");

    // Delete source plugin
    let resp = client
        .delete(format!("{}/plugins/sources/custom_src", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(format!("{}/plugins/sources", base_url))
        .send()
        .await
        .unwrap();
    let sources_after: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(sources_after.is_empty());

    // 4. Sink plugin CRUD lifecycle (no file URI: local files must exist).
    let resp = client
        .post(format!("{}/plugins/sinks", base_url))
        .json(&serde_json::json!({
            "name": "custom_snk",
            "description": "Custom Sink"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let resp = client
        .get(format!("{}/plugins/sinks", base_url))
        .send()
        .await
        .unwrap();
    let sinks: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(sinks.len(), 1);
    assert_eq!(sinks[0]["name"], "custom_snk");
    assert_eq!(sinks[0]["plugin_type"], "sink");

    let resp = client
        .delete(format!("{}/plugins/sinks/custom_snk", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(format!("{}/plugins/sinks", base_url))
        .send()
        .await
        .unwrap();
    let sinks_after: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(sinks_after.is_empty());

    // 5. Prebuild endpoints return 200 OK
    for path in [
        "/plugins/sources/prebuild",
        "/plugins/sinks/prebuild",
        "/plugins/functions/prebuild",
    ] {
        let resp = client
            .get(format!("{}{}", base_url, path))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK, "GET {}", path);
    }
}

#[tokio::test]
async fn test_portable_plugins_report_unsupported_runtime() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{}/plugins/portables", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let names: Vec<String> = resp.json().await.unwrap();
    assert!(names.is_empty());

    let resp = client
        .post(format!("{}/plugins/portables", base_url))
        .json(&serde_json::json!({
            "name": "mirror",
            "file": "file:///var/plugins/portables/mirror.zip"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_IMPLEMENTED);

    let resp = client
        .get(format!("{}/plugins/portables/mirror", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    let resp = client
        .get(format!("{}/plugins/portables/mirror/status", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    let resp = client
        .put(format!("{}/plugins/portables/mirror", base_url))
        .json(&serde_json::json!({
            "version": "2.0.0",
            "executable": "custom_mirror.py"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_IMPLEMENTED);

    let resp = client
        .delete(format!("{}/plugins/portables/mirror", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_IMPLEMENTED);
}

#[tokio::test]
async fn test_external_services_and_javascript_udf_engine() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // Register an executable JS UDF in an initially empty registry.
    let resp = client
        .get(format!("{}/udf/javascript", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let udfs: Vec<String> = resp.json().await.unwrap();
    assert!(udfs.is_empty());

    let resp = client
        .post(format!("{}/udf/javascript", base_url))
        .json(&serde_json::json!({
            "id": "js_multiply",
            "description": "multiply two numbers in JS",
            "script": "function js_multiply(a, b) { return a * b; }",
            "isAgg": false
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let resp = client
        .get(format!("{}/udf/javascript", base_url))
        .send()
        .await
        .unwrap();
    let udfs: Vec<String> = resp.json().await.unwrap();
    assert!(udfs.contains(&"js_multiply".to_string()));

    let resp = client
        .get(format!("{}/udf/javascript/js_multiply", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let udf_val: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(udf_val["id"], "js_multiply");
    assert_eq!(udf_val["description"], "multiply two numbers in JS");

    // Direct invocation via global registry
    let res = rekuiper_core::plugin::get_global_udf_registry()
        .call_udf("js_multiply", &[serde_json::json!(6), serde_json::json!(7)]);
    assert_eq!(res, Some(serde_json::json!(42)));

    // Update JS UDF
    let resp = client
        .put(format!("{}/udf/javascript/js_multiply", base_url))
        .json(&serde_json::json!({
            "description": "updated multiply adding offset",
            "script": "function js_multiply(a, b) { return (a * b) + 10; }"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let res = rekuiper_core::plugin::get_global_udf_registry()
        .call_udf("js_multiply", &[serde_json::json!(6), serde_json::json!(7)]);
    assert_eq!(res, Some(serde_json::json!(52)));

    // Test SQL rule execution calling js_multiply
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&serde_json::json!({
            "sql": "CREATE STREAM js_stream () WITH (DATASOURCE=\"js_stream\", FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&serde_json::json!({
            "id": "rule_js_calc",
            "sql": "SELECT js_multiply(a, b) AS product FROM js_stream",
            "actions": [{
                "log": {}
            }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    let resp = client
        .post(format!("{}/streams/js_stream/data", base_url))
        .json(&serde_json::json!({
            "a": 5,
            "b": 8
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let status = wait_for_rule_status(&client, &base_url, "rule_js_calc", 1, 1).await;
    assert_eq!(status["status"], "running");

    let _ = client
        .delete(format!("{}/rules/rule_js_calc", base_url))
        .send()
        .await;
    let _ = client
        .delete(format!("{}/streams/js_stream", base_url))
        .send()
        .await;

    // Delete JS UDF
    let resp = client
        .delete(format!("{}/udf/javascript/js_multiply", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(format!("{}/udf/javascript/js_multiply", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    let res = rekuiper_core::plugin::get_global_udf_registry()
        .call_udf("js_multiply", &[serde_json::json!(6), serde_json::json!(7)]);
    assert_eq!(res, None);

    // 2. Services and external functions registry
    let resp = client
        .get(format!("{}/services", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let services: Vec<String> = resp.json().await.unwrap();
    assert!(services.is_empty());

    let resp = client
        .get(format!("{}/services/functions", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let funcs: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(funcs.is_empty());

    // Register a new service
    let resp = client
        .post(format!("{}/services", base_url))
        .json(&serde_json::json!({
            "name": "weather_service",
            "About": {
                "description": "Weather forecasting service",
                "version": "2.0"
            },
            "Interfaces": {
                "forecast": {
                    "addr": "http://weather.local:8080",
                    "methods": ["get_temp", "get_humidity"]
                }
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let resp = client
        .get(format!("{}/services", base_url))
        .send()
        .await
        .unwrap();
    let services: Vec<String> = resp.json().await.unwrap();
    assert!(services.contains(&"weather_service".to_string()));

    let resp = client
        .get(format!("{}/services/functions/get_temp", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let temp_fn: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(temp_fn["ServiceName"], "weather_service");
    assert_eq!(temp_fn["MethodName"], "get_temp");

    // Update service
    let resp = client
        .put(format!("{}/services/weather_service", base_url))
        .json(&serde_json::json!({
            "About": {
                "description": "Updated weather forecasting service",
                "version": "2.1"
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(format!("{}/services/weather_service", base_url))
        .send()
        .await
        .unwrap();
    let updated_svc: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(updated_svc["About"]["version"], "2.1");

    // Delete service
    let resp = client
        .delete(format!("{}/services/weather_service", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(format!("{}/services/weather_service", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    let resp = client
        .get(format!("{}/services/functions/get_temp", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_forensic_parity_endpoints() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 1. POST /config/uploads saves file to data/uploads and returns 201 Created with path.
    let upload_resp = client
        .post(format!("{}/config/uploads", base_url))
        .json(&serde_json::json!({
            "name": "test_upload.json",
            "content": "{\"key\": \"value\"}"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(upload_resp.status(), reqwest::StatusCode::CREATED);
    let saved_path = upload_resp.text().await.unwrap();
    assert!(
        saved_path.contains("test_upload.json"),
        "expected path with test_upload.json: {}",
        saved_path
    );

    // 2. GET /config/uploads lists uploaded file.
    let list_resp = client
        .get(format!("{}/config/uploads", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(list_resp.status(), reqwest::StatusCode::OK);
    let uploads: Vec<String> = list_resp.json().await.unwrap();
    assert!(
        uploads.iter().any(|u| u.contains("test_upload.json")),
        "uploads list must include test_upload.json: {:?}",
        uploads
    );

    // 3. DELETE /config/uploads/:name deletes uploaded file.
    let del_resp = client
        .delete(format!("{}/config/uploads/test_upload.json", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(del_resp.status(), reqwest::StatusCode::OK);

    // 4. GET /data/import/status returns Configuration error schema.
    let status_resp = client
        .get(format!("{}/data/import/status", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(status_resp.status(), reqwest::StatusCode::OK);
    let status_val: serde_json::Value = status_resp.json().await.unwrap();
    assert!(status_val.get("streams").is_some());
    assert!(status_val.get("tables").is_some());
    assert!(status_val.get("rules").is_some());

    // 5. Seed a rule and verify bulkstart and bulkstop return array of BulkOperationResponse.
    client
        .post(format!("{}/streams", base_url))
        .json(&serde_json::json!({
            "sql": "CREATE STREAM demo_bulk () WITH (DATASOURCE=\"demo\", FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    client
        .post(format!("{}/rules", base_url))
        .json(&serde_json::json!({
            "id": "rule_bulk_1",
            "sql": "SELECT * FROM demo_bulk",
            "actions": [{ "log": {} }],
            "tags": ["env:prod"]
        }))
        .send()
        .await
        .unwrap();

    let bulk_start_resp = client
        .post(format!("{}/rules/bulkstart", base_url))
        .json(&serde_json::json!({ "tags": ["env:prod"] }))
        .send()
        .await
        .unwrap();
    assert_eq!(bulk_start_resp.status(), reqwest::StatusCode::OK);
    let bulk_start_res: Vec<serde_json::Value> = bulk_start_resp.json().await.unwrap();
    assert!(
        bulk_start_res
            .iter()
            .any(|r| r["ruleId"] == "rule_bulk_1" && r["success"] == true),
        "bulkstart response: {:?}",
        bulk_start_res
    );

    let bulk_stop_resp = client
        .post(format!("{}/rules/bulkstop", base_url))
        .json(&serde_json::json!({ "tags": ["env:prod"] }))
        .send()
        .await
        .unwrap();
    assert_eq!(bulk_stop_resp.status(), reqwest::StatusCode::OK);
    let bulk_stop_res: Vec<serde_json::Value> = bulk_stop_resp.json().await.unwrap();
    assert!(
        bulk_stop_res
            .iter()
            .any(|r| r["ruleId"] == "rule_bulk_1" && r["success"] == true),
        "bulkstop response: {:?}",
        bulk_stop_res
    );

    // 6. PUT /rules/:name/reset_state returns 200 "success\n".
    let reset_resp = client
        .put(format!("{}/rules/rule_bulk_1/reset_state", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(reset_resp.status(), reqwest::StatusCode::OK);
    let reset_text = reset_resp.text().await.unwrap();
    assert!(reset_text.contains("success"));

    // 7. GET /trace/unknown_id returns 404 Not Found.
    let trace_resp = client
        .get(format!("{}/trace/non_existent_trace_999", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(trace_resp.status(), reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_put_update_endpoints_and_config_patch() {
    let (base_url, _handle, _state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    // PUT /streams/:name: create, update via JSON envelope, read back.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({"sql": "CREATE STREAM put_demo () WITH (FORMAT=\"json\")"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    let resp = client
        .put(format!("{}/streams/put_demo", base_url))
        .json(&json!({"sql": "CREATE STREAM put_demo (id BIGINT) WITH (FORMAT=\"json\")"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let updated: serde_json::Value = client
        .get(format!("{}/streams/put_demo", base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(updated["Name"], json!("put_demo"));
    assert_eq!(
        updated["StreamFields"],
        json!([{"Name": "id", "FieldType": "bigint"}]),
        "stream update readback: {}",
        updated
    );
    // Raw DDL body also updates.
    let resp = client
        .put(format!("{}/streams/put_demo", base_url))
        .body("CREATE STREAM put_demo (id BIGINT, ts BIGINT) WITH (FORMAT=\"json\")")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    // Missing streams 404.
    let resp = client
        .put(format!("{}/streams/no_such_stream", base_url))
        .json(&json!({"sql": "CREATE STREAM no_such_stream () WITH (FORMAT=\"json\")"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // PUT /tables/:name: create, update, read back.
    let resp = client
        .post(format!("{}/tables", base_url))
        .json(&json!({"sql": "CREATE TABLE put_tbl () WITH (FORMAT=\"json\")"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    let resp = client
        .put(format!("{}/tables/put_tbl", base_url))
        .json(&json!({"sql": "CREATE TABLE put_tbl (k STRING) WITH (FORMAT=\"json\")"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let updated: serde_json::Value = client
        .get(format!("{}/tables/put_tbl", base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(updated["Name"], json!("put_tbl"));
    assert_eq!(
        updated["StreamFields"],
        json!([{"Name": "k", "FieldType": "string"}]),
        "table update readback: {}",
        updated
    );

    // PUT /rules/:name: create, update SQL, verify the new projection.
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "put_rule",
            "sql": "SELECT a FROM put_demo",
            "actions": [{"memory": {"topic": "put_sink"}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    let resp = client
        .put(format!("{}/rules/put_rule", base_url))
        .json(&json!({
            "id": "put_rule",
            "sql": "SELECT a, a AS b FROM put_demo",
            "actions": [{"memory": {"topic": "put_sink"}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let schema: serde_json::Value = client
        .get(format!("{}/rules/put_rule/schema", base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        schema,
        json!({"a": "any", "b": "any"}),
        "rule update: {}",
        schema
    );
    // Missing rules 404.
    let resp = client
        .put(format!("{}/rules/no_such_rule", base_url))
        .json(&json!({
            "id": "no_such_rule",
            "sql": "SELECT a FROM put_demo",
            "actions": []
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // PUT /connections/:id: upsert then read back.
    let resp = client
        .put(format!("{}/connections/put_conn", base_url))
        .json(&json!({"server": "tcp://broker:1883"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let conn: serde_json::Value = client
        .get(format!("{}/connections/put_conn", base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(conn["server"], json!("tcp://broker:1883"), "conn: {}", conn);
    let resp = client
        .put(format!("{}/connections/put_conn", base_url))
        .json(&json!({"server": "tcp://broker2:1883", "username": "u"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let conn: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        conn["server"],
        json!("tcp://broker2:1883"),
        "conn: {}",
        conn
    );
    assert_eq!(conn["username"], json!("u"), "conn: {}", conn);

    // PATCH /configs returns 204 and merges into the live config.
    let before: serde_json::Value = client
        .get(format!("{}/configs", base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let resp = client
        .patch(format!("{}/configs", base_url))
        .json(&json!({"basic": {"timezone": "UTC"}}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);
    let after: serde_json::Value = client
        .get(format!("{}/configs", base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        after["basic"]["timezone"],
        json!("UTC"),
        "configs: {}",
        after
    );
    assert_eq!(
        after["basic"]["port"], before["basic"]["port"],
        "unrelated keys preserved: {}",
        after
    );

    // Cleanup.
    for rule in ["put_rule"] {
        let resp = client
            .delete(format!("{}/rules/{}", base_url, rule))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }
    for stream in ["put_demo"] {
        let resp = client
            .delete(format!("{}/streams/{}", base_url, stream))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }
    let resp = client
        .delete(format!("{}/tables/put_tbl", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let resp = client
        .delete(format!("{}/connections/put_conn", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_sql_source_polling_and_lookup_join() {
    // D8: file-backed SQLite database shared by per-operation pools.
    let mut db_path = std::env::temp_dir();
    db_path.push(format!(
        "rekuiper-sql-src-test-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    let db_url = format!("sqlite://{}", db_path.to_string_lossy().replace('\\', "/"));
    let pool = sqlx::sqlite::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new()
            .filename(&db_path)
            .create_if_missing(true),
    )
    .await
    .unwrap();
    sqlx::query("CREATE TABLE readings (id INTEGER, val INTEGER)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO readings (id, val) VALUES (1, 10), (2, 20)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("CREATE TABLE devinfo (id TEXT, loc TEXT)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO devinfo (id, loc) VALUES ('d1', 'room1')")
        .execute(&pool)
        .await
        .unwrap();
    drop(pool);

    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    // T10d-equivalent: a TYPE="sql" stream polls the table into the bus.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": format!(
                "CREATE STREAM sql_src_stream () WITH (TYPE=\"sql\", DATASOURCE=\"{}\", TABLE=\"readings\", INTERVAL=\"50\")",
                db_url
            )
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let mut src_rx = state.stream_bus.subscribe("sql_src_out");
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_sql_src",
            "sql": "SELECT * FROM sql_src_stream",
            "actions": [{"memory": {"topic": "sql_src_out"}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // The poller emits every table row with typed values.
    let first = tokio::time::timeout(std::time::Duration::from_secs(5), src_rx.recv())
        .await
        .expect("timed out waiting for polled SQL row")
        .expect("sql_src_out topic closed");
    assert_eq!(first.data.get("id"), Some(&json!(1)));
    assert_eq!(first.data.get("val"), Some(&json!(10)));

    // T10c-equivalent: a lookup join against a TYPE="sql" table enriches rows.
    let resp = client
        .post(format!("{}/tables", base_url))
        .json(&json!({
            "sql": format!(
                "CREATE TABLE devinfo () WITH (TYPE=\"sql\", URL=\"{}\")",
                db_url
            )
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM sensor_sql () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_sql_join",
            "sql": "SELECT id, loc FROM sensor_sql LEFT JOIN devinfo ON sensor_sql.id = devinfo.id",
            "actions": [{"memory": {"topic": "sql_join_out"}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let mut join_rx = state.stream_bus.subscribe("sql_join_out");
    let resp = client
        .post(format!("{}/streams/sensor_sql/data", base_url))
        .json(&json!({"id": "d1"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let joined = tokio::time::timeout(std::time::Duration::from_secs(5), join_rx.recv())
        .await
        .expect("timed out waiting for SQL lookup join output")
        .expect("sql_join_out topic closed");
    assert_eq!(joined.data.get("id"), Some(&json!("d1")));
    assert_eq!(joined.data.get("loc"), Some(&json!("room1")));

    // Cleanup.
    for rule in ["rule_sql_src", "rule_sql_join"] {
        let resp = client
            .delete(format!("{}/rules/{}", base_url, rule))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }
    for stream in ["sql_src_stream", "sensor_sql"] {
        let resp = client
            .delete(format!("{}/streams/{}", base_url, stream))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }
    let resp = client
        .delete(format!("{}/tables/devinfo", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let _ = std::fs::remove_file(&db_path);
}

#[tokio::test]
async fn test_stream_table_describe_and_schema_fields() {
    let (base_url, _handle, _state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    // Streams and tables retain declared columns.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM typed_stream (id BIGINT, temp FLOAT, name STRING) WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    let resp = client
        .post(format!("{}/tables", base_url))
        .json(&json!({
            "sql": "CREATE TABLE typed_tbl (id BIGINT, active BOOLEAN) WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // T02: describe carries explicit names and types.
    let resp = client
        .get(format!("{}/streams/typed_stream", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let described: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(described["Name"], json!("typed_stream"));
    assert_eq!(described["StreamType"], json!(0));
    assert_eq!(
        described["StreamFields"],
        json!([
            {"Name": "id", "FieldType": "bigint"},
            {"Name": "temp", "FieldType": "float"},
            {"Name": "name", "FieldType": "string"}
        ]),
        "describe: {}",
        described
    );

    // T02-schema: the schema endpoint maps names to type/index pairs.
    let resp = client
        .get(format!("{}/streams/typed_stream/schema", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let schema: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        schema,
        json!({
            "id": {"type": "bigint", "index": 0},
            "temp": {"type": "float", "index": 1},
            "name": {"type": "string", "index": 2}
        }),
        "schema: {}",
        schema
    );

    // T03: tables describe identically.
    let resp = client
        .get(format!("{}/tables/typed_tbl", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let described: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(described["StreamType"], json!(1));
    assert_eq!(
        described["StreamFields"],
        json!([
            {"Name": "id", "FieldType": "bigint"},
            {"Name": "active", "FieldType": "boolean"}
        ]),
        "table describe: {}",
        described
    );
    let resp = client
        .get(format!("{}/tables/typed_tbl/schema", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let schema: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(schema["active"], json!({"type": "boolean", "index": 1}));

    // Missing subjects answer the JSON error envelope.
    let resp = client
        .get(format!("{}/streams/nonexistent", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let missing: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(missing["error"], json!(3000));
    assert!(
        missing["message"]
            .as_str()
            .unwrap_or("")
            .contains("nonexistent is not found"),
        "envelope: {}",
        missing
    );

    // Cleanup.
    let resp = client
        .delete(format!("{}/streams/typed_stream", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let resp = client
        .delete(format!("{}/tables/typed_tbl", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_rule_input_validation() {
    let (base_url, _handle, _state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM vstream (a BIGINT) WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // T07: rules on missing streams are rejected.
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_ghost",
            "sql": "SELECT * FROM ghost",
            "actions": [{"log": {}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let rejected: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(rejected["error"], json!(1000));
    assert!(
        rejected["message"]
            .as_str()
            .unwrap_or("")
            .contains("fail to get stream ghost"),
        "rejection: {}",
        rejected
    );

    // Unknown functions are rejected at creation.
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_badfn",
            "sql": "SELECT nosuchfn(a) FROM vstream",
            "actions": [{"log": {}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let rejected: serde_json::Value = resp.json().await.unwrap();
    assert!(
        rejected["message"]
            .as_str()
            .unwrap_or("")
            .contains("function nosuchfn not found"),
        "rejection: {}",
        rejected
    );

    // ... and at update time.
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_upd",
            "sql": "SELECT a FROM vstream",
            "actions": [{"log": {}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    let resp = client
        .put(format!("{}/rules/rule_upd", base_url))
        .json(&json!({
            "id": "rule_upd",
            "sql": "SELECT nosuchfn(a) FROM vstream",
            "actions": [{"log": {}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);

    // Validation rejects unknown functions with 422, while creation/update use 400.
    let resp = client
        .post(format!("{}/rules/validate", base_url))
        .json(&json!({
            "id": "rule_val_bad",
            "sql": "SELECT nosuchfn(a) FROM vstream",
            "actions": [{"log": {}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    assert!(resp
        .text()
        .await
        .unwrap()
        .contains("function nosuchfn not found"));

    // Validation of a clean rule returns the sources envelope.
    let resp = client
        .post(format!("{}/rules/validate", base_url))
        .json(&json!({
            "id": "rule_val_ok",
            "sql": "SELECT count(*) AS cnt FROM vstream",
            "actions": [{"log": {}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let valid: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(valid, json!({"sources": ["vstream"], "valid": true}));

    // T20: simulations without (parseable) SELECT SQL are rejected.
    for body in [
        json!({"id": "rt_empty"}),
        json!({"id": "rt_garbage", "sql": "SELECT FROM WHERE"}),
    ] {
        let resp = client
            .post(format!("{}/ruletest", base_url))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
        let rejected: serde_json::Value = resp.json().await.unwrap();
        assert!(
            rejected["message"]
                .as_str()
                .unwrap_or("")
                .contains("SQL is not a select statement"),
            "rejection: {}",
            rejected
        );
    }

    // Stream management statements run through POST /streams.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({"sql": "SHOW STREAMS"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    let names: Vec<String> = resp.json().await.unwrap();
    assert!(names.contains(&"vstream".to_string()), "names: {:?}", names);

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({"sql": "DESCRIBE STREAM vstream"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    let described: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(described["Name"], json!("vstream"));
    assert_eq!(
        described["StreamFields"],
        json!([{"Name": "a", "FieldType": "bigint"}])
    );

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({"sql": "DESCRIBE STREAM ghost"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);

    // Cleanup.
    let resp = client
        .delete(format!("{}/rules/rule_upd", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let resp = client
        .delete(format!("{}/streams/vstream", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_data_import_export_parity() {
    let (base_url, _handle, _state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    // Seed two streams, a table and a rule per stream.
    for sql in [
        "CREATE STREAM ex_s1 (id BIGINT) WITH (FORMAT=\"json\")",
        "CREATE STREAM ex_s2 (id BIGINT) WITH (FORMAT=\"json\")",
    ] {
        let resp = client
            .post(format!("{}/streams", base_url))
            .json(&json!({"sql": sql}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    }
    let resp = client
        .post(format!("{}/tables", base_url))
        .json(&json!({"sql": "CREATE TABLE ex_t (id BIGINT) WITH (FORMAT=\"json\")"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    for (id, from) in [("rule_ex1", "ex_s1"), ("rule_ex2", "ex_s2")] {
        let resp = client
            .post(format!("{}/rules", base_url))
            .json(&json!({
                "id": id,
                "sql": format!("SELECT * FROM {}", from),
                "actions": [{"log": {}}]
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    }

    // T15: selective export returns only the requested rule plus its
    // dependencies (ex_s1, and no tables for this rule).
    let resp = client
        .post(format!("{}/data/export", base_url))
        .json(&json!({"rules": ["rule_ex1"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let exported: serde_json::Value = resp.json().await.unwrap();
    let rule_ids: Vec<&str> = exported["rules"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|r| r["id"].as_str())
        .collect();
    assert_eq!(rule_ids, vec!["rule_ex1"], "exported: {}", exported);
    let stream_names: Vec<&str> = exported["streams"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|s| s["name"].as_str())
        .collect();
    assert_eq!(stream_names, vec!["ex_s1"], "exported: {}", exported);
    assert!(exported["tables"].as_array().unwrap().is_empty());

    // Empty selection exports everything (also via the v2 route).
    let resp = client
        .post(format!("{}/v2/data/export", base_url))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let exported: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(exported["rules"].as_array().unwrap().len(), 2);

    // T15-import: the structured envelope, with map-form rules (ids filled
    // from the map keys) actually imported.
    let resp = client
        .post(format!("{}/data/import", base_url))
        .json(&json!({
            "streams": [
                {"name": "ex_im", "sql": "CREATE STREAM ex_im (id BIGINT) WITH (FORMAT=\"json\")"}
            ],
            "rules": {
                "rule_im1": {"sql": "SELECT * FROM ex_im", "actions": [{"log": {}}]}
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let envelope: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(envelope["ErrorMsg"], json!(""));
    for key in [
        "streams",
        "tables",
        "rules",
        "nativePlugins",
        "portablePlugins",
        "sourceConfig",
        "sinkConfig",
        "connectionConfig",
    ] {
        assert!(
            envelope["ConfigResponse"][key].is_object(),
            "envelope: {}",
            envelope
        );
    }
    let resp = client
        .get(format!("{}/rules/rule_im1", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // T16: ruleset import reports counts; re-importing reports zeros.
    let payload = json!({
        "streams": [
            {"name": "ex_rs", "sql": "CREATE STREAM ex_rs (id BIGINT) WITH (FORMAT=\"json\")"}
        ],
        "rules": [
            {"id": "rule_rs1", "sql": "SELECT * FROM ex_rs", "actions": [{"log": {}}]}
        ]
    });
    let resp = client
        .post(format!("{}/ruleset/import", base_url))
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.text().await.unwrap(),
        "imported 1 streams, 0 tables and 1 rules\n"
    );
    let resp = client
        .post(format!("{}/ruleset/import", base_url))
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.text().await.unwrap(),
        "imported 0 streams, 0 tables and 0 rules\n"
    );

    // Cleanup.
    for rule in ["rule_ex1", "rule_ex2", "rule_im1", "rule_rs1"] {
        let resp = client
            .delete(format!("{}/rules/{}", base_url, rule))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }
    for stream in ["ex_s1", "ex_s2", "ex_im", "ex_rs"] {
        let resp = client
            .delete(format!("{}/streams/{}", base_url, stream))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }
    let resp = client
        .delete(format!("{}/tables/ex_t", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_plugin_service_validation() {
    let (base_url, _handle, _state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    // T10: installing from a nonexistent file is a 400, not a 201.
    let resp = client
        .post(format!("{}/plugins/sources", base_url))
        .json(&json!({"name": "badplug", "file": "file:///nonexistent/rk_eval.zip"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let rejected: serde_json::Value = resp.json().await.unwrap();
    assert!(
        rejected["message"]
            .as_str()
            .unwrap_or("")
            .contains("no such file or directory"),
        "rejection: {}",
        rejected
    );

    // An existing local file installs fine (and cleans up afterwards).
    let mut plugin_file = std::env::temp_dir();
    plugin_file.push(format!("rekuiper-plug-{}.zip", std::process::id()));
    std::fs::write(&plugin_file, b"fake-plugin-bytes").unwrap();
    let file_uri = format!(
        "file:///{}",
        plugin_file.to_string_lossy().replace('\\', "/")
    );
    let resp = client
        .post(format!("{}/plugins/sources", base_url))
        .json(&json!({"name": "goodplug", "file": file_uri}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    let resp = client
        .delete(format!("{}/plugins/sources/goodplug", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let _ = std::fs::remove_file(&plugin_file);

    // PUT on unknown sink/source/function/portable plugins is a 404
    // (udfs expose no PUT route at all, so Axum answers 405 there).
    for kind in ["sources", "sinks", "functions"] {
        let resp = client
            .put(format!("{}/plugins/{}/noplug_xyz", base_url, kind))
            .json(&json!({}))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::NOT_FOUND,
            "PUT {}/noplug_xyz",
            kind
        );
    }

    // DELETE on unknown plugins of every typed kind is a 404.
    for kind in ["sources", "sinks", "functions", "udfs"] {
        let resp = client
            .delete(format!("{}/plugins/{}/noplug_xyz", base_url, kind))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::NOT_FOUND,
            "DELETE {}/noplug_xyz",
            kind
        );
        let missing: serde_json::Value = resp.json().await.unwrap();
        assert!(
            missing["message"]
                .as_str()
                .unwrap_or("")
                .contains("is not found"),
            "envelope: {}",
            missing
        );
    }

    // Symbol registration and portable status/PUT/DELETE miss the same way.
    let resp = client
        .post(format!(
            "{}/plugins/functions/noplug_xyz/register",
            base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    let resp = client
        .get(format!("{}/plugins/portables/noplug_xyz/status", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    let resp = client
        .put(format!("{}/plugins/portables/noplug_xyz", base_url))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_IMPLEMENTED);
    let resp = client
        .delete(format!("{}/plugins/portables/noplug_xyz", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_IMPLEMENTED);

    // T11: services with unreadable files are rejected; valid ones register.
    let resp = client
        .post(format!("{}/services", base_url))
        .json(&json!({"name": "badsvc", "file": "file:///nonexistent/svc.json"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let resp = client
        .post(format!("{}/services", base_url))
        .json(&json!({"name": "goodsvc"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    let resp = client
        .delete(format!("{}/services/goodsvc", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_schema_multipart_upload() {
    let (base_url, _handle, _state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();
    let proto = "syntax = \"proto3\";\nmessage Reading {\n  int64 id = 1;\n}\n";

    // Raw-bytes upload stores the content verbatim.
    let resp = client
        .put(format!("{}/schemas/protobuf/rawproto/upload", base_url))
        .header("Content-Type", "application/octet-stream")
        .body(proto.to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let uploaded: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(uploaded, json!({"type": "protobuf", "name": "rawproto"}));

    // Multipart upload extracts the file part.
    let boundary = "REKTESTBOUNDARY";
    let body = format!(
        "--{b}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"test.proto\"\r\nContent-Type: application/octet-stream\r\n\r\n{proto}\r\n--{b}--\r\n",
        b = boundary,
        proto = proto
    );
    let resp = client
        .put(format!("{}/schemas/protobuf/multiproto/upload", base_url))
        .header(
            "Content-Type",
            format!("multipart/form-data; boundary={}", boundary),
        )
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Both round-trip through the schema reader.
    for name in ["rawproto", "multiproto"] {
        let resp = client
            .get(format!("{}/schemas/protobuf/{}", base_url, name))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        let def: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(def["content"], json!(proto), "schema {}: {}", name, def);
    }
}

// D9: RS256 JWT vectors shared with the routes unit tests (key and tokens
// generated offline; the valid token expires in 2027, the expired one sat
// one hour in the past at generation).
const TEST_AUTH_PEM: &str = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAmdub6pqDN/MPofsOTQlf\npCF6O4vsisxaDPzKZM8pqiUIOGjDwfqRQkzikSPu/oK8jPouof9JkeesUjbKg+0w\nQ7aZXgRPr8PJkHeY27/4bFz1riFPDZ+rKAe8DvXIlcjb70H68AtGnRzUkVjVlzhn\n6qfJE4LMmLtdQodW4Hnd2Oo8qujRprtn8AMcX5H1phIVUHYdIZpt44SNetOgCPxZ\n/S/0VLi2qD7bh/bBj6VRvea/LyCKnC75r+wnJGIHYpeVXskMrDBH+lfV1GsoU9Ig\nm+5MYAkc0SrgYBVCUXXvQNrip3IQcaWlW4YhkJC2rVBCA2ibc1MMWOyA0xYfJfy1\nvQIDAQAB\n-----END PUBLIC KEY-----\n";
const TEST_AUTH_VALID: &str = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0ZXIiLCJleHAiOjE4MjA2MDE3MDB9.U3NGxZxNiHStyGFGZOKsz4PPM3aX2papMEOsKUqWnYC7NmgGxESTKWKtPM6M5McKUQwD3cfnnFH9p3XGdkTfPofxcnmJcrN8PMWdaVAcetYn_c5ScMWgQapjuHiO7jBQKljTU4AwuGrNQAMtxgBgSdEX-MG1AYZrNYcdgqXtfUPk8y_V2icNU8kVQRzRonHs4yaioIqy1IpBuxpb6A5AHRy07T_En_TlebfH7Tb-lf3tFDT8UUcjnfFJ-TPxFTGqLsPnLbdqXcrKi-PVqpbuFj2WSFlGinbZDGEsjhopdHPD1_PkC0Am3c4XmiCM3QswVqcUujwED71lcZXBdCFxPA";
const TEST_AUTH_EXPIRED: &str = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0ZXIiLCJleHAiOjE3ODkwNjIxMDB9.GNrAExENuYVgbt8bZebTYTmCf2o0DcAkWzhpfkBCtAvPflzzr9xSNaEOrtf2HJU5gqOm-ZDIztXsatd765CtZA-uLcqjbCa4TURbMv0OFHDhMVlVfn2xr17jr39IG1ck7Ymioz_ZSd3wXu0egABsCUrKigEvtRwcGJSWlAp1GdRiWLpT2-U6Xb15bgUdbCNgYxyXiWM4zBLHVILLLH-zPyPIbvHo82l3qOpu7c2SRw5aNPrY2KXPCxKm47NEOkf74LJk14MZODeRTOYczyi8Q4vtD6xEcp8-u_vVUv5Gv04mrO1gMCALOQAWfKq2PFA0QatGX_cw8f36m_EOLLfuWA";
const TEST_AUTH_WRONGKEY: &str = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0ZXIiLCJleHAiOjE4MjA2MDE3MDB9.Uu3vn8teCnUBsT-8updR4it_yyMoYdtqFGOvLgozxmsOmM8hb-FfqpeuqelBemgsbpwi2pbRVuOUxv0621ORempEpwfhCUnI80Rj9hPcOV0j8wYuX-xdsnvsNOPI1K6XlW9i1IOjToKxhptaPEPdYXf_jEq5v3P0zCITwevT2t6bpOdYzgSHjxq6CJiXIJHcP0idiQ1I3PqNhKmjJn3qi14GQ_xRk8Sf30VQDqsT76qXw0S92K-QKdcbc35oJW5x6oTdbgELv9YV-su1ooXTrLQ5zzY0g8qXhbkeLotXQEddaGMFeBOWchDax_km1g1gRbCS8f7TNEtO2xVkVHx-AQ";

#[tokio::test]
async fn test_jwt_authentication_vectors() {
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    // Deploy the test public key and switch this server to auth mode. Other
    // test servers keep authentication disabled, so the process-global env
    // var cannot disturb them (the guard short-circuits first).
    let mut pem_path = std::env::temp_dir();
    pem_path.push(format!("rekuiper-auth-test-{}.pem", std::process::id()));
    std::fs::write(&pem_path, TEST_AUTH_PEM).unwrap();
    std::env::set_var("KUIPER_AUTH_PUBLIC_KEY_FILE", &pem_path);
    state.config.write().basic.authentication = true;

    async fn get_guarded(
        client: &reqwest::Client,
        base_url: &str,
        auth: Option<&str>,
    ) -> (reqwest::StatusCode, String) {
        let mut req = client.get(format!("{}/streams", base_url));
        if let Some(token) = auth {
            req = req.header("Authorization", token);
        }
        let resp = req.send().await.unwrap();
        let status = resp.status();
        let body = resp.text().await.unwrap();
        (status, body)
    }

    // 1. Missing header.
    let (status, body) = get_guarded(&client, &base_url, None).await;
    assert_eq!(status, reqwest::StatusCode::UNAUTHORIZED);
    assert_eq!(body, "Missing authorization header\n");
    // 2. Bearer prefix is prohibited.
    let bearer = format!("Bearer {}", TEST_AUTH_VALID);
    let (status, body) = get_guarded(&client, &base_url, Some(&bearer)).await;
    assert_eq!(status, reqwest::StatusCode::UNAUTHORIZED);
    assert_eq!(
        body,
        "Bearer token is not supported, please use raw JWT token\n"
    );
    // 3. Malformed token.
    let (status, body) = get_guarded(&client, &base_url, Some("not-a-token")).await;
    assert_eq!(status, reqwest::StatusCode::UNAUTHORIZED);
    assert_eq!(body, "Invalid JWT format\n");
    // 4. Expired token.
    let (status, body) = get_guarded(&client, &base_url, Some(TEST_AUTH_EXPIRED)).await;
    assert_eq!(status, reqwest::StatusCode::UNAUTHORIZED);
    assert_eq!(body, "Token has expired\n");
    // 5. Wrong signing key.
    let (status, body) = get_guarded(&client, &base_url, Some(TEST_AUTH_WRONGKEY)).await;
    assert_eq!(status, reqwest::StatusCode::UNAUTHORIZED);
    assert_eq!(body, "Invalid token signature\n");
    // 6. Valid raw token passes.
    let (status, _) = get_guarded(&client, &base_url, Some(TEST_AUTH_VALID)).await;
    assert_eq!(status, reqwest::StatusCode::OK);

    // Public endpoints stay open without credentials.
    for path in ["/ping", "/"] {
        let resp = client
            .get(format!("{}{}", base_url, path))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK, "GET {}", path);
    }

    // Restore defaults so nothing leaks into other tests.
    state.config.write().basic.authentication = false;
    std::env::remove_var("KUIPER_AUTH_PUBLIC_KEY_FILE");
    let _ = std::fs::remove_file(&pem_path);
}

#[tokio::test]
async fn test_ruletest_live_sse_port() {
    let (base_url, _handle, _state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    // Create a session replaying two mock rows.
    let resp = client
        .post(format!("{}/ruletest", base_url))
        .json(&json!({
            "id": "rt_live",
            "sql": "SELECT b FROM ssesrc",
            "mockSource": {
                "ssesrc": {"data": [{"b": 4}, {"b": 6}]}
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let created: serde_json::Value = resp.json().await.unwrap();
    // Documented contract: the stable configured SSE port (10081 default).
    assert_eq!(created["port"], json!(10081));

    // Start the replay first: history buffering makes late subscribers
    // deterministic (no subscribe-before-start race).
    let started = client
        .post(format!("{}/ruletest/rt_live/start", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(started.status(), reqwest::StatusCode::OK);
    // Unknown sessions are a genuine 404, never fake-OK.
    let missing = client
        .post(format!("{}/ruletest/no_such_session/start", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), reqwest::StatusCode::NOT_FOUND);

    // The documented SSE endpoint streams both buffered frames.
    let sse_url = format!("{}/test/rt_live", base_url);
    let mut resp = client
        .get(&sse_url)
        .header("Accept", "text/event-stream")
        .send()
        .await
        .expect("SSE endpoint must accept connections");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok()),
        Some("text/event-stream")
    );
    let mut buf: Vec<u8> = Vec::new();
    let mut frames: Vec<String> = Vec::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while frames.len() < 2 && std::time::Instant::now() < deadline {
        match tokio::time::timeout(std::time::Duration::from_millis(500), resp.chunk()).await {
            Ok(Ok(Some(chunk))) => {
                buf.extend_from_slice(&chunk);
                while let Some(pos) = buf.windows(2).position(|w| w == b"\n\n") {
                    let frame: Vec<u8> = buf.drain(..pos + 2).collect();
                    for line in String::from_utf8_lossy(&frame).lines() {
                        if let Some(data) = line.strip_prefix("data:") {
                            frames.push(data.trim().to_string());
                        }
                    }
                }
            }
            _ => break,
        }
    }
    assert_eq!(
        frames,
        vec!["{\"b\":4}".to_string(), "{\"b\":6}".to_string()]
    );

    // Deleting the session drops the feed: the endpoint 404s afterwards.
    let resp = client
        .delete(format!("{}/ruletest/rt_live", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let gone = client.get(&sse_url).send().await.unwrap();
    assert_eq!(gone.status(), reqwest::StatusCode::NOT_FOUND);
}

/// Drain `want` SSE `data:` frames from an open response (deadline-bounded).
async fn drain_sse_frames(resp: &mut reqwest::Response, want: usize, secs: u64) -> Vec<String> {
    let mut buf: Vec<u8> = Vec::new();
    let mut frames: Vec<String> = Vec::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(secs);
    while frames.len() < want && std::time::Instant::now() < deadline {
        match tokio::time::timeout(std::time::Duration::from_millis(500), resp.chunk()).await {
            Ok(Ok(Some(chunk))) => {
                buf.extend_from_slice(&chunk);
                while let Some(pos) = buf.windows(2).position(|w| w == b"\n\n") {
                    let frame: Vec<u8> = buf.drain(..pos + 2).collect();
                    for line in String::from_utf8_lossy(&frame).lines() {
                        if let Some(data) = line.strip_prefix("data:") {
                            frames.push(data.trim().to_string());
                            if frames.len() >= want {
                                break;
                            }
                        }
                    }
                }
            }
            _ => break,
        }
    }
    frames
}

#[tokio::test]
async fn test_ruletest_large_replay_ordered_and_delete() {
    // Release-blocker regression: rows past the 10k retention cap must keep
    // streaming to live subscribers in order, without duplicates or gaps.
    let (base_url, _handle, _state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();
    const N: usize = 10_500;
    let data: Vec<serde_json::Value> = (0..N as u64).map(|i| json!({"n": i})).collect();
    let resp = client
        .post(format!("{}/ruletest", base_url))
        .json(&json!({
            "id": "rt_big",
            "sql": "SELECT n FROM big",
            "mockSource": {"big": {"data": data, "interval": 0, "loop": false}}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let started = client
        .post(format!("{}/ruletest/rt_big/start", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(started.status(), reqwest::StatusCode::OK);

    // A subscriber that stays connected receives the whole replay in exact
    // sequence order, including everything past the retention cap.
    let mut resp = client
        .get(format!("{}/test/rt_big", base_url))
        .header("Accept", "text/event-stream")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let frames = drain_sse_frames(&mut resp, N, 120).await;
    assert_eq!(frames.len(), N, "live subscriber must receive all rows");
    for (i, f) in frames.iter().enumerate() {
        assert_eq!(f, &format!("{{\"n\":{}}}", i), "row {} ordered", i);
    }
    drop(resp);

    // Deleting the session terminates open feeds and the endpoint 404s.
    let mut resp = client
        .get(format!("{}/test/rt_big", base_url))
        .header("Accept", "text/event-stream")
        .send()
        .await
        .unwrap();
    let probe = drain_sse_frames(&mut resp, 3, 30).await;
    assert!(!probe.is_empty());
    let del = client
        .delete(format!("{}/ruletest/rt_big", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(del.status(), reqwest::StatusCode::OK);
    // The open feed ends once its backlog drains: keep reading until the
    // server terminates the stream (deadline-bounded).
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    let mut terminated = false;
    while std::time::Instant::now() < deadline {
        match tokio::time::timeout(std::time::Duration::from_secs(2), resp.chunk()).await {
            Ok(Ok(None)) => {
                terminated = true;
                break;
            }
            Ok(Ok(Some(_))) => continue,
            _ => continue,
        }
    }
    assert!(
        terminated,
        "open SSE feed must terminate after session delete"
    );
    let gone = client
        .get(format!("{}/test/rt_big", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(gone.status(), reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_ruletest_loop_continues_past_cap() {
    // A looping session streams indefinitely: an active subscriber keeps
    // receiving well beyond the 10k-row retention cap.
    //
    // Timing portability: the replay interval is 0 (unthrottled) because a
    // nonzero millisecond interval cannot be paced portably — Windows timer
    // granularity (~15.6ms) stretches `sleep(1ms)` per row, so 10_300 rows
    // cannot finish in a bounded deadline there (CI: 9659/10300 in 120s on
    // windows-latest while macOS/Ubuntu passed, and the interval-0 gap test
    // passed on all three). Pacing semantics are covered separately by
    // `test_ruletest_paced_replay_spacing`.
    let (base_url, _handle, _state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/ruletest", base_url))
        .json(&json!({
            "id": "rt_loop",
            "sql": "SELECT n FROM loopsrc",
            "mockSource": {"loopsrc": {"data": [{"n": 0}, {"n": 1}, {"n": 2}], "interval": 0, "loop": true}}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let started = client
        .post(format!("{}/ruletest/rt_loop/start", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(started.status(), reqwest::StatusCode::OK);
    let mut resp = client
        .get(format!("{}/test/rt_loop", base_url))
        .header("Accept", "text/event-stream")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    const WANT: usize = 10_300;
    let t0 = std::time::Instant::now();
    let frames = drain_sse_frames(&mut resp, WANT, 120).await;
    let elapsed = t0.elapsed();
    assert_eq!(
        frames.len(),
        WANT,
        "looping feed must continue past the cap (got {}/{} in {:.1}s = {:.0} rows/s)",
        frames.len(),
        WANT,
        elapsed.as_secs_f64(),
        frames.len() as f64 / elapsed.as_secs_f64().max(0.001),
    );
    // Strict cyclic succession: no gaps, no reorders, no duplicates.
    let seq: Vec<u64> = frames
        .iter()
        .map(|f| {
            serde_json::from_str::<serde_json::Value>(f).unwrap()["n"]
                .as_u64()
                .unwrap()
        })
        .collect();
    for w in seq.windows(2) {
        assert_eq!(w[1], (w[0] + 1) % 3, "gap or reorder after n={}", w[0]);
    }
    drop(resp);
    let del = client
        .delete(format!("{}/ruletest/rt_loop", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(del.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_ruletest_paced_replay_spacing() {
    // Pacing semantics: a nonzero mock interval spaces replayed rows. Only
    // a lower bound is asserted (load can only add delay, and OS timers can
    // fire late but not early by any significant margin).
    let (base_url, _handle, _state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();
    let data: Vec<serde_json::Value> = (0..6u64).map(|i| json!({"n": i})).collect();
    let resp = client
        .post(format!("{}/ruletest", base_url))
        .json(&json!({
            "id": "rt_paced",
            "sql": "SELECT n FROM pacedsrc",
            "mockSource": {"pacedsrc": {"data": data, "interval": 100, "loop": false}}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let mut resp = client
        .get(format!("{}/test/rt_paced", base_url))
        .header("Accept", "text/event-stream")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let t0 = std::time::Instant::now();
    let started = client
        .post(format!("{}/ruletest/rt_paced/start", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(started.status(), reqwest::StatusCode::OK);
    let frames = drain_sse_frames(&mut resp, 6, 60).await;
    let elapsed = t0.elapsed();
    assert_eq!(frames.len(), 6, "paced replay must deliver all rows");
    for (i, f) in frames.iter().enumerate() {
        assert_eq!(f, &format!("{{\"n\":{}}}", i), "row {} ordered", i);
    }
    // 6 rows x 100ms pacing nominally take >= 600ms; allow wide margin.
    assert!(
        elapsed >= std::time::Duration::from_millis(400),
        "replay ignored pacing: 6 rows in {:.1}s",
        elapsed.as_secs_f64()
    );
    drop(resp);
    let del = client
        .delete(format!("{}/ruletest/rt_paced", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(del.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_ruletest_concurrent_stream_has_no_gaps() {
    // Cursor-race regression: with a fast looping producer and a live
    // subscriber, every frame must continue the exact cyclic sequence —
    // a skipped row (e.g. from a split snapshot/cursor update) breaks it.
    let (base_url, _handle, _state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();
    const CYCLE: usize = 500;
    const WANT: usize = 3000;
    let data: Vec<serde_json::Value> = (0..CYCLE as u64).map(|i| json!({"k": i})).collect();
    let resp = client
        .post(format!("{}/ruletest", base_url))
        .json(&json!({
            "id": "rt_gap",
            "sql": "SELECT k FROM gapsrc",
            "mockSource": {"gapsrc": {"data": data, "interval": 0, "loop": true}}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let started = client
        .post(format!("{}/ruletest/rt_gap/start", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(started.status(), reqwest::StatusCode::OK);
    let mut resp = client
        .get(format!("{}/test/rt_gap", base_url))
        .header("Accept", "text/event-stream")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let frames = drain_sse_frames(&mut resp, WANT, 120).await;
    assert_eq!(frames.len(), WANT, "concurrent feed must keep flowing");
    let seq: Vec<u64> = frames
        .iter()
        .map(|f| {
            serde_json::from_str::<serde_json::Value>(f).unwrap()["k"]
                .as_u64()
                .unwrap()
        })
        .collect();
    for w in seq.windows(2) {
        assert_eq!(
            w[1],
            (w[0] + 1) % CYCLE as u64,
            "gap or reorder after k={}",
            w[0]
        );
    }
    drop(resp);
    let del = client
        .delete(format!("{}/ruletest/rt_gap", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(del.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_data_and_ruleset_import_content_envelope() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // Test POST /data/import with stringified content envelope and stringified rule map
    let import_payload = serde_json::json!({
        "content": serde_json::json!({
            "streams": {
                "import_stream_1": "CREATE STREAM import_stream_1 () WITH (DATASOURCE=\"demo\", FORMAT=\"json\")"
            },
            "rules": {
                "import_rule_1": serde_json::json!({
                    "id": "import_rule_1",
                    "sql": "SELECT * FROM import_stream_1",
                    "actions": [{ "log": {} }]
                }).to_string()
            }
        }).to_string()
    });

    let resp = client
        .post(format!("{}/data/import", base_url))
        .json(&import_payload)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Verify stream exists
    let st_resp = client
        .get(format!("{}/streams/import_stream_1", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(st_resp.status(), reqwest::StatusCode::OK);

    // Verify rule exists
    let rule_resp = client
        .get(format!("{}/rules/import_rule_1", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(rule_resp.status(), reqwest::StatusCode::OK);

    // Test POST /ruleset/import
    let ruleset_payload = serde_json::json!({
        "content": serde_json::json!({
            "rules": {
                "import_rule_2": serde_json::json!({
                    "id": "import_rule_2",
                    "sql": "SELECT * FROM import_stream_1",
                    "actions": [{ "log": {} }]
                }).to_string()
            }
        }).to_string()
    });

    let r_resp = client
        .post(format!("{}/ruleset/import", base_url))
        .json(&ruleset_payload)
        .send()
        .await
        .unwrap();
    assert_eq!(r_resp.status(), reqwest::StatusCode::OK);

    let rule2_resp = client
        .get(format!("{}/rules/import_rule_2", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(rule2_resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_rule_stop_and_start_lifecycle() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    client
        .post(format!("{}/streams", base_url))
        .json(&serde_json::json!({
            "sql": "CREATE STREAM cycle_st () WITH (DATASOURCE=\"cycle_ds\", FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();

    client
        .post(format!("{}/rules", base_url))
        .json(&serde_json::json!({
            "id": "cycle_rule",
            "sql": "SELECT * FROM cycle_st",
            "actions": [{ "log": {} }]
        }))
        .send()
        .await
        .unwrap();

    // Stop rule
    let stop_resp = client
        .post(format!("{}/rules/cycle_rule/stop", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(stop_resp.status(), reqwest::StatusCode::OK);

    let status_resp = client
        .get(format!("{}/rules/cycle_rule/status", base_url))
        .send()
        .await
        .unwrap();
    let st: serde_json::Value = status_resp.json().await.unwrap();
    assert_eq!(st["status"], "stopped");

    // Start rule
    let start_resp = client
        .post(format!("{}/rules/cycle_rule/start", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(start_resp.status(), reqwest::StatusCode::OK);

    let status_resp2 = client
        .get(format!("{}/rules/cycle_rule/status", base_url))
        .send()
        .await
        .unwrap();
    let st2: serde_json::Value = status_resp2.json().await.unwrap();
    assert_eq!(st2["status"], "running");
}

#[tokio::test]
async fn test_rules_list_and_all_status_parity() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // Create stream
    let st_resp = client
        .post(format!("{}/streams", base_url))
        .json(&serde_json::json!({
            "sql": "CREATE STREAM parity_st () WITH (DATASOURCE=\"parity_ds\", TYPE=\"memory\", FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(st_resp.status(), reqwest::StatusCode::CREATED);

    // Verify streamdetails wire format
    let details_resp = client
        .get(format!("{}/streamdetails", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(details_resp.status(), reqwest::StatusCode::OK);
    let details: serde_json::Value = details_resp.json().await.unwrap();
    let stream_item = details
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["name"] == "parity_st")
        .unwrap();
    assert_eq!(stream_item["name"], "parity_st");
    assert_eq!(stream_item["type"], "memory");
    assert_eq!(stream_item["format"], "json");

    // Create rule
    let r_resp = client
        .post(format!("{}/rules", base_url))
        .json(&serde_json::json!({
            "id": "parity_rule",
            "name": "Parity Rule",
            "sql": "SELECT * FROM parity_st",
            "actions": [{ "log": {} }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r_resp.status(), reqwest::StatusCode::CREATED);

    // GET /rules format check
    let rules_resp = client
        .get(format!("{}/rules", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(rules_resp.status(), reqwest::StatusCode::OK);
    let rules: serde_json::Value = rules_resp.json().await.unwrap();
    let rule_item = rules
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["id"] == "parity_rule")
        .unwrap();
    assert_eq!(rule_item["id"], "parity_rule");
    assert_eq!(rule_item["name"], "Parity Rule");
    assert_eq!(rule_item["status"], "running");
    assert!(rule_item.get("tags").is_some());
    assert_eq!(rule_item["trace"], false);
    assert!(rule_item.get("version").is_some());

    // GET /rules/status/all format check
    let all_resp = client
        .get(format!("{}/rules/status/all", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(all_resp.status(), reqwest::StatusCode::OK);
    let all_status: serde_json::Value = all_resp.json().await.unwrap();
    assert!(all_status.get("parity_rule").is_some());
    let entry = &all_status["parity_rule"];
    assert_eq!(entry["status"], "running");
    assert_eq!(entry["exceptions_total"], 0);
    assert_eq!(entry["last_exception"], "");
}

#[tokio::test]
async fn test_issue16_repeated_start_delivers_each_input_once() {
    let (base, handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();
    assert!(client
        .post(format!("{base}/streams"))
        .json(&json!({
            "sql":"CREATE STREAM once () WITH (TYPE=\"memory\", FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap()
        .status()
        .is_success());
    let mut output = state.stream_bus.subscribe("once_out");
    assert!(client
        .post(format!("{base}/rules"))
        .json(&json!({
            "id":"once", "sql":"SELECT id FROM once", "actions":[{"memory":{"topic":"once_out"}}]
        }))
        .send()
        .await
        .unwrap()
        .status()
        .is_success());
    for action in ["stop", "stop", "start", "start", "start"] {
        assert_eq!(
            client
                .post(format!("{base}/rules/once/{action}"))
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::OK
        );
    }
    assert!(client
        .post(format!("{base}/streams/once/data"))
        .json(&json!([
            {"id":0},{"id":1},{"id":2},{"id":3},{"id":4},{"id":5}
        ]))
        .send()
        .await
        .unwrap()
        .status()
        .is_success());
    for id in 0..6 {
        let row = tokio::time::timeout(std::time::Duration::from_secs(2), output.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            row.data["id"],
            json!(id),
            "repeated starts must not duplicate outputs"
        );
    }
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), output.recv())
            .await
            .is_err()
    );
    let status: serde_json::Value = client
        .get(format!("{base}/rules/once/status"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["sourceRecordsInTotal"], json!(6));
    assert_eq!(status["sinkRecordsOutTotal"], json!(6));
    handle.abort();
}
