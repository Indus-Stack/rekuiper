//! Single-record HTTP ingestion regression tests.
//!
//! A request that carries exactly one record takes the lock-free publish path
//! instead of the batch admission gate. These tests prove that path keeps the
//! same delivery contract: every subscriber receives every record exactly once,
//! also when many single-record and batch requests run at the same time.

use rekuiper_conf::KuiperConfig;
use rekuiper_core::{RuleManager, StreamBus, StreamManager, TableManager};
use rekuiper_server::routes::{create_router, AppState};
use serde_json::json;
use tokio::net::TcpListener;

async fn spawn_test_server() -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind ephemeral port");
    let local_addr = listener.local_addr().unwrap();

    let stream_bus = StreamBus::new();
    let rule_manager = RuleManager::new(stream_bus.clone());
    let state = AppState::new(
        "single-record".to_string(),
        KuiperConfig::default(),
        StreamManager::new(),
        TableManager::new(),
        rule_manager,
        stream_bus,
    );

    let app = create_router(state);
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (format!("http://{}", local_addr), handle)
}

async fn create_stream_with_rules(client: &reqwest::Client, base_url: &str, rules: usize) {
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({"sql": "CREATE STREAM single_stream () WITH (FORMAT=\"json\")"}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    for i in 0..rules {
        let resp = client
            .post(format!("{}/rules", base_url))
            .json(&json!({
                "id": format!("single_rule_{}", i),
                "sql": "SELECT id, temp FROM single_stream WHERE temp > 20.0",
                "actions": [{"nop": {}}]
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    }
}

async fn rule_status(client: &reqwest::Client, base_url: &str, rule: &str) -> serde_json::Value {
    client
        .get(format!("{}/rules/{}/status", base_url, rule))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

/// Waits until the rule reports `want` ingested and delivered records, then
/// waits again to prove nothing arrives beyond `want`.
async fn assert_exactly(client: &reqwest::Client, base_url: &str, rule: &str, want: u64) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let status = rule_status(client, base_url, rule).await;
        if status["sourceRecordsInTotal"].as_u64().unwrap_or(0) >= want
            && status["sinkRecordsOutTotal"].as_u64().unwrap_or(0) >= want
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {} records on {} (status: {})",
            want,
            rule,
            status
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let status = rule_status(client, base_url, rule).await;
    assert_eq!(
        status["sourceRecordsInTotal"].as_u64(),
        Some(want),
        "duplicate or extra records on {}: {}",
        rule,
        status
    );
    assert_eq!(
        status["sinkRecordsOutTotal"].as_u64(),
        Some(want),
        "sink count differs on {}: {}",
        rule,
        status
    );
}

async fn post(client: &reqwest::Client, base_url: &str, body: serde_json::Value) -> u16 {
    client
        .post(format!("{}/streams/single_stream/data", base_url))
        .json(&body)
        .send()
        .await
        .unwrap()
        .status()
        .as_u16()
}

/// A single object and a one-element array both deliver exactly one record to
/// every subscriber of the stream.
#[tokio::test]
async fn test_single_object_and_one_element_array_reach_every_subscriber() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();
    create_stream_with_rules(&client, &base_url, 2).await;

    assert_eq!(
        post(&client, &base_url, json!({"id": 1, "temp": 25.0})).await,
        200
    );
    assert_eq!(
        post(&client, &base_url, json!([{"id": 2, "temp": 26.0}])).await,
        200
    );

    for rule in ["single_rule_0", "single_rule_1"] {
        assert_exactly(&client, &base_url, rule, 2).await;
    }
}

/// Posting to a stream without subscribers still succeeds.
#[tokio::test]
async fn test_single_record_without_subscribers_succeeds() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();
    create_stream_with_rules(&client, &base_url, 0).await;

    assert_eq!(
        post(&client, &base_url, json!({"id": 1, "temp": 25.0})).await,
        200
    );
}

/// Many concurrent single-record requests mixed with batch requests are all
/// delivered exactly once to every subscriber.
#[tokio::test]
async fn test_concurrent_single_and_batch_requests_deliver_exactly_once() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();
    create_stream_with_rules(&client, &base_url, 2).await;

    const SINGLE_TASKS: u64 = 32;
    const SINGLES_PER_TASK: u64 = 25;
    const BATCH_TASKS: u64 = 4;
    const BATCHES_PER_TASK: u64 = 5;
    const BATCH_SIZE: u64 = 20;

    let mut tasks = Vec::new();
    for t in 0..SINGLE_TASKS {
        let client = client.clone();
        let base_url = base_url.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..SINGLES_PER_TASK {
                let id = t * SINGLES_PER_TASK + i;
                assert_eq!(
                    post(&client, &base_url, json!({"id": id, "temp": 25.0})).await,
                    200
                );
            }
        }));
    }
    for t in 0..BATCH_TASKS {
        let client = client.clone();
        let base_url = base_url.clone();
        tasks.push(tokio::spawn(async move {
            for b in 0..BATCHES_PER_TASK {
                let batch: Vec<_> = (0..BATCH_SIZE)
                    .map(|i| json!({"id": 100_000 + t * 1000 + b * BATCH_SIZE + i, "temp": 25.0}))
                    .collect();
                assert_eq!(post(&client, &base_url, json!(batch)).await, 200);
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }

    let want = SINGLE_TASKS * SINGLES_PER_TASK + BATCH_TASKS * BATCHES_PER_TASK * BATCH_SIZE;
    for rule in ["single_rule_0", "single_rule_1"] {
        assert_exactly(&client, &base_url, rule, want).await;
    }
}
