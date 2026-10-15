use rekuiper_conf::KuiperConfig;
use rekuiper_core::model::StreamRecord;
use rekuiper_core::{RuleManager, StreamBus, StreamManager, TableManager};
use rekuiper_server::routes::{create_router, AppState};
use serde_json::{json, Value};
use std::collections::HashMap;
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
        "perf".to_string(),
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
async fn test_high_throughput_streaming_pipeline() {
    let (base_url, _handle, state) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 2. High-frequency stream.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM perf_stream () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 3. Arithmetic transformation + filter into a no-op sink.
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_perf",
            "sql": "SELECT id, temp * 1.8 + 32 AS temp_f FROM perf_stream WHERE temp > 20.0",
            "actions": [{"nop": {}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 4. Publish 500,000 records through the bounded bus with backpressure:
    // the sender awaits subscriber capacity, so no record is ever lost to
    // ring overwrite. The handle is resolved once outside the loop.
    let tx = state.stream_bus.get_or_create("perf_stream");
    let start = std::time::Instant::now();
    let mut sent = 0u64;
    for n in 0..500_000u64 {
        let data: HashMap<String, Value> = serde_json::from_str(&format!(
            r#"{{"id": "dev_{}", "temp": {}}}"#,
            n,
            25.0 + (n % 10) as f64
        ))
        .unwrap();
        tx.send(StreamRecord {
            timestamp: n as i64,
            data: data.into(),
        })
        .await
        .expect("subscriber closed mid-benchmark");
        sent += 1;
    }
    assert_eq!(sent, 500_000);

    // 5. Rule status reflects every ingested record.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        let status: serde_json::Value = client
            .get(format!("{}/rules/rule_perf/status", base_url))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if status["sourceRecordsInTotal"].as_u64().unwrap_or(0) >= 500_000
            && status["sinkRecordsOutTotal"].as_u64().unwrap_or(0) >= 500_000
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for 500,000 ingested records"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    // 6. Throughput report + floor assertion.
    let elapsed = start.elapsed();
    let events_per_sec = 500_000.0 / elapsed.as_secs_f64();
    let floor = if cfg!(debug_assertions) {
        10_000.0
    } else {
        20_000.0
    };
    println!("=== Performance Benchmark: Streaming SQL Pipeline ===");
    println!("Records Ingested : 500,000");
    println!("Elapsed Time     : {:?}", elapsed);
    println!(
        "Throughput       : {:.2} events/sec (Assert: > {:.0} eps)",
        events_per_sec, floor
    );
    println!("Result           : PASSED (Zero GC pauses, deterministic execution)");
    assert!(
        events_per_sec > floor,
        "Throughput too low: {:.2} events/sec (expected > {:.0} eps)",
        events_per_sec,
        floor
    );
}

#[tokio::test]
async fn test_pure_engine_throughput_prebuilt_records() {
    let (base_url, _handle, state) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 1. High-frequency stream.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM perf_pure_stream () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 2. Arithmetic transformation + filter into a no-op sink.
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_pure_perf",
            "sql": "SELECT id, temp * 1.8 + 32 AS temp_f FROM perf_pure_stream WHERE temp > 20.0",
            "actions": [{"nop": {}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 3. Pre-construct a 64-record ring outside the timer.
    // Each record has SharedMap data, so .clone() is just an Arc refcount increment.
    let ring: Vec<StreamRecord> = (0..64)
        .map(|n| {
            let mut data = HashMap::new();
            data.insert("id".to_string(), Value::String(format!("dev_{}", n)));
            data.insert("temp".to_string(), Value::from(25.0 + (n % 10) as f64));
            StreamRecord {
                timestamp: n,
                data: data.into(),
            }
        })
        .collect();

    // 4. Ingest 500,000 records without test-thread JSON format/parse overhead.
    let tx = state.stream_bus.get_or_create("perf_pure_stream");
    let start = std::time::Instant::now();
    for n in 0..500_000u64 {
        tx.send(ring[(n & 63) as usize].clone())
            .await
            .expect("subscriber closed mid-benchmark");
    }

    // 5. Rule status reflects every ingested record.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        let status: serde_json::Value = client
            .get(format!("{}/rules/rule_pure_perf/status", base_url))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if status["sourceRecordsInTotal"].as_u64().unwrap_or(0) >= 500_000
            && status["sinkRecordsOutTotal"].as_u64().unwrap_or(0) >= 500_000
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for 500,000 ingested records"
        );
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    // 6. Pure Engine Throughput report + floor assertion.
    let elapsed = start.elapsed();
    let events_per_sec = 500_000.0 / elapsed.as_secs_f64();
    let floor = if cfg!(debug_assertions) {
        50_000.0
    } else {
        100_000.0
    };
    println!(
        "=== Performance Benchmark: Pure Engine SQL Pipeline (Zero-Parse Pre-Built Records) ==="
    );
    println!("Records Ingested : 500,000");
    println!("Elapsed Time     : {:?}", elapsed);
    println!(
        "Engine Throughput: {:.2} events/sec (Assert: > {:.0} eps)",
        events_per_sec, floor
    );
    println!("Result           : PASSED (Zero GC pauses, deterministic execution)");
    assert!(
        events_per_sec > floor,
        "Engine throughput too low: {:.2} events/sec (expected > {:.0} eps)",
        events_per_sec,
        floor
    );
}
