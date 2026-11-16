use axum::body::Body;
use axum::http::{Request, StatusCode};
use rekuiper_conf::KuiperConfig;
use rekuiper_core::contracts::clock::{DeadlineCancellation, RealClock};
use rekuiper_core::manager::{RuleManager, StreamManager, TableManager};
use rekuiper_core::model::{RuleDefinition, StreamDefinition};
use rekuiper_core::runtime::StreamBus;
use rekuiper_server::engine::lookup_runtime::{
    lookup_runtime_for_backend, LookupError, LookupQueueConfig,
};
use rekuiper_server::routes::create_router;
use rekuiper_server::sink_cache::compute_destination_fingerprint;
use rekuiper_server::state::AppState;
use serde_json::json;
use std::collections::HashMap;
use std::time::Duration;
use tower::ServiceExt;

fn test_state() -> AppState {
    let bus = StreamBus::new();
    AppState::new(
        "test".to_owned(),
        KuiperConfig::default(),
        StreamManager::new(),
        TableManager::new(),
        RuleManager::new(bus.clone()),
        bus,
    )
}

#[test]
fn fingerprint_retains_redis_key_and_strips_url_credentials() {
    let mut redis_a = HashMap::new();
    redis_a.insert("addr".to_owned(), json!("127.0.0.1:6379"));
    redis_a.insert("key".to_owned(), json!("dest_a"));
    redis_a.insert("password".to_owned(), json!("pw1"));

    let mut redis_b = redis_a.clone();
    redis_b.insert("key".to_owned(), json!("dest_b"));
    redis_b.insert("password".to_owned(), json!("pw2"));

    assert_ne!(
        compute_destination_fingerprint("redis", &redis_a),
        compute_destination_fingerprint("redis", &redis_b),
        "Redis `key` must affect destination fingerprint"
    );

    let mut redis_url_key_a = HashMap::new();
    redis_url_key_a.insert("addr".to_owned(), json!("127.0.0.1:6379"));
    redis_url_key_a.insert(
        "key".to_owned(),
        json!("https://alice@cache.example/orders"),
    );
    let mut redis_url_key_b = HashMap::new();
    redis_url_key_b.insert("addr".to_owned(), json!("127.0.0.1:6379"));
    redis_url_key_b.insert("key".to_owned(), json!("https://bob@cache.example/orders"));
    assert_ne!(
        compute_destination_fingerprint("redis", &redis_url_key_a),
        compute_destination_fingerprint("redis", &redis_url_key_b),
        "Redis `key` containing `://` must not be URL-scrubbed"
    );

    let mut rest_a = HashMap::new();
    rest_a.insert(
        "url".to_owned(),
        json!("https://alice:secret1@api.example.com/ingest?token=tok1&tenant=t1"),
    );
    let mut rest_b = HashMap::new();
    rest_b.insert(
        "url".to_owned(),
        json!("https://bob:secret2@api.example.com/ingest?token=tok2&tenant=t1"),
    );
    assert_eq!(
        compute_destination_fingerprint("rest", &rest_a),
        compute_destination_fingerprint("rest", &rest_b),
        "URL userinfo and secret query params must be stripped from fingerprint"
    );
}

#[tokio::test]
async fn lookup_runtime_isolates_backends_and_separates_permit_wait_timeout() {
    let cfg = LookupQueueConfig {
        max_concurrent_lookups: 1,
        max_queued_requests: 8,
        max_queued_bytes: 1024 * 1024,
        permit_wait_timeout: Duration::from_millis(60),
        request_timeout: Duration::from_millis(500),
        ..LookupQueueConfig::default()
    };
    let redis_rt = lookup_runtime_for_backend("redis", cfg.clone());
    let sql_rt = lookup_runtime_for_backend("sql", cfg);

    // Hold the sole permit in the redis runtime.
    let redis_hold = redis_rt.clone();
    let holder = tokio::spawn(async move {
        let clock = RealClock::default();
        let cancel = DeadlineCancellation::default();
        let _ = redis_hold
            .execute_one(16, &clock, &cancel, || async {
                tokio::time::sleep(Duration::from_millis(220)).await;
                Ok::<_, String>(1)
            })
            .await;
    });
    tokio::time::sleep(Duration::from_millis(15)).await;

    // A second redis request times out waiting for a permit after 60ms.
    let clock = RealClock::default();
    let cancel = DeadlineCancellation::default();
    let err = redis_rt
        .execute_one(16, &clock, &cancel, || async { Ok::<_, String>(2) })
        .await
        .unwrap_err();
    assert!(matches!(err, LookupError::Timeout { .. }));

    // Meanwhile, the isolated sql runtime still succeeds immediately.
    let sql_res = sql_rt
        .execute_one(16, &clock, &cancel, || async { Ok::<_, String>(42) })
        .await
        .unwrap();
    assert_eq!(sql_res, 42);

    let _ = holder.await;
}

#[tokio::test]
async fn rabbitmq_stream_rejects_unsupported_ack_mode_at_create_and_update() {
    let state = test_state();
    let app = create_router(state.clone());

    let create_req = Request::builder()
        .method("POST")
        .uri("/streams")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "sql": "CREATE STREAM rmq_bad (v string) WITH (TYPE=\"rabbitmq\", DATASOURCE=\"q1\", ackMode=\"durable_outbox\")"
            })
            .to_string(),
        ))
        .unwrap();
    let resp = app.clone().oneshot(create_req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // Create a valid legacy RabbitMQ stream, then reject updating it to confirmed_delivery.
    state
        .stream_manager
        .create_stream(StreamDefinition {
            name: "rmq_ok".to_owned(),
            sql: "CREATE STREAM rmq_ok (v string) WITH (TYPE=\"rabbitmq\", DATASOURCE=\"q1\")"
                .to_owned(),
            stream_fields: Vec::new(),
            options: HashMap::new(),
        })
        .await
        .unwrap();

    let update_req = Request::builder()
        .method("PUT")
        .uri("/streams/rmq_ok")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "sql": "CREATE STREAM rmq_ok (v string) WITH (TYPE=\"rabbitmq\", DATASOURCE=\"q1\", ackMode=\"confirmed_delivery\")"
            })
            .to_string(),
        ))
        .unwrap();
    let resp2 = app.oneshot(update_req).await.unwrap();
    assert_eq!(resp2.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn begin_update_with_cleanup_serializes_concurrent_updates_and_supports_rollback() {
    let mgr = RuleManager::new(StreamBus::new());
    let mut log_action = HashMap::new();
    log_action.insert("log".to_owned(), json!({}));
    let actions = vec![log_action];

    let def_v1 = RuleDefinition {
        id: "r_atomic".to_owned(),
        name: Some("r_atomic".to_owned()),
        sql: "SELECT * FROM s1".to_owned(),
        actions,
        options: None,
        graph: None,
        tags: Vec::new(),
        version: None,
    };
    mgr.create_rule(def_v1.clone()).await.unwrap();
    mgr.start_rule("r_atomic").await.unwrap();

    let def_v2 = RuleDefinition {
        sql: "SELECT a FROM s1".to_owned(),
        ..def_v1.clone()
    };
    let guard = mgr.begin_update_with_cleanup(def_v2, || {}).await.unwrap();
    assert!(guard.was_running());

    // A concurrent second update must wait until `guard` is dropped, and after
    // rollback it must still observe `was_running == true` and `prev_def`.
    let mgr2 = mgr.clone();
    let def_v3 = RuleDefinition {
        sql: "SELECT b FROM s1".to_owned(),
        ..def_v1.clone()
    };
    let handle = tokio::spawn(async move { mgr2.update_rule(def_v3).await.unwrap() });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(
        !handle.is_finished(),
        "second update must block while first update holds RuleUpdateGuard"
    );

    mgr.rollback_update(&guard, true).await.unwrap();
    assert_eq!(mgr.get_rule("r_atomic").unwrap().sql, "SELECT * FROM s1");
    drop(guard);

    let second_saw_running = handle.await.unwrap();
    assert!(second_saw_running);
}

#[tokio::test]
async fn declarative_invalid_native_dag_preserves_existing_rule() {
    let state = test_state();
    state
        .stream_manager
        .create_stream(StreamDefinition {
            name: "s_in".to_owned(),
            sql: "CREATE STREAM s_in (a bigint) WITH (TYPE=\"memory\", DATASOURCE=\"in\")"
                .to_owned(),
            stream_fields: Vec::new(),
            options: HashMap::new(),
        })
        .await
        .unwrap();

    let mut action = HashMap::new();
    action.insert("log".to_owned(), json!({}));
    state
        .rule_manager
        .create_rule(RuleDefinition {
            id: "r_keep".to_owned(),
            name: Some("r_keep".to_owned()),
            sql: "SELECT * FROM s_in".to_owned(),
            actions: vec![action],
            options: None,
            graph: None,
            tags: Vec::new(),
            version: None,
        })
        .await
        .unwrap();
    state.rule_manager.start_rule("r_keep").await.unwrap();

    // Submit a declarative manifest replacing r_keep with an invalid native DAG (durable option).
    let manifest = json!({
        "streams": {},
        "tables": {},
        "rules": {
            "r_keep": {
                "id": "r_keep",
                "sql": "",
                "actions": [],
                "options": { "durable": true },
                "graph": {
                    "nodes": {
                        "src": { "type": "source", "nodeType": "source", "props": { "datasource": "s_in" } },
                        "sw": { "type": "switch", "nodeType": "switch", "props": {} }
                    },
                    "topo": {
                        "sources": ["src"],
                        "edges": { "src": ["sw"] }
                    }
                }
            }
        }
    });

    let manifest: rekuiper_core::model::DeclarativeManifest =
        serde_json::from_value(manifest).unwrap();
    let res = rekuiper_server::declarative::reconcile_manifest(&state, &manifest).await;
    assert!(
        res.is_err(),
        "invalid native DAG manifest must fail validation/reconciliation"
    );
    assert_eq!(
        state.rule_manager.get_rule_status("r_keep").unwrap().status,
        "running",
        "existing running rule must stay running when replacement native DAG is invalid"
    );
}

#[test]
fn lookup_config_rejects_excessive_concurrency_without_panicking() {
    let mut opts = HashMap::new();
    opts.insert("LOOKUP_MAX_CONCURRENT".to_owned(), usize::MAX.to_string());
    let err = LookupQueueConfig::parse_and_validate_table_options(&opts).unwrap_err();
    assert!(err.contains("LOOKUP_MAX_CONCURRENT"));

    // Infallible parser + runtime constructor must clamp instead of panicking.
    let clamped = LookupQueueConfig::from_table_options(&opts);
    assert!(clamped.max_concurrent_lookups <= tokio::sync::Semaphore::MAX_PERMITS);
    let _rt = rekuiper_server::engine::lookup_runtime::LookupRuntime::new(clamped);
}

#[tokio::test]
async fn declarative_sql_preflight_failure_preserves_existing_rule() {
    let state = test_state();
    state
        .stream_manager
        .create_stream(StreamDefinition {
            name: "s_decl".to_owned(),
            sql: "CREATE STREAM s_decl (a bigint) WITH (TYPE=\"memory\", DATASOURCE=\"in_decl\")"
                .to_owned(),
            stream_fields: Vec::new(),
            options: HashMap::new(),
        })
        .await
        .unwrap();

    let mut action = HashMap::new();
    action.insert("log".to_owned(), json!({}));
    state
        .rule_manager
        .create_rule(RuleDefinition {
            id: "r_sql_keep".to_owned(),
            name: Some("r_sql_keep".to_owned()),
            sql: "SELECT * FROM s_decl".to_owned(),
            actions: vec![action],
            options: None,
            graph: None,
            tags: Vec::new(),
            version: None,
        })
        .await
        .unwrap();
    state.rule_manager.start_rule("r_sql_keep").await.unwrap();

    // Duplicate action names fail action validation/preflight before touching the live rule.
    let manifest = json!({
        "streams": {
            "s_decl": "CREATE STREAM s_decl (a bigint) WITH (TYPE=\"memory\", DATASOURCE=\"in_decl\")"
        },
        "tables": {},
        "rules": {
            "r_sql_keep": {
                "id": "r_sql_keep",
                "sql": "SELECT a FROM s_decl",
                "actions": [
                    { "rest": { "name": "dup", "url": "http://127.0.0.1:9/a", "enableCache": true } },
                    { "rest": { "name": "dup", "url": "http://127.0.0.1:9/b", "enableCache": true } }
                ]
            }
        }
    });
    let manifest: rekuiper_core::model::DeclarativeManifest =
        serde_json::from_value(manifest).unwrap();
    let report = rekuiper_server::declarative::reconcile_manifest(&state, &manifest)
        .await
        .unwrap();
    assert_eq!(report.rules_failed, 1);
    assert_eq!(report.status, "partial_error");
    assert_eq!(
        state
            .rule_manager
            .get_rule_status("r_sql_keep")
            .unwrap()
            .status,
        "running"
    );
}

#[tokio::test]
async fn shutdown_batch_flush_timeout_persists_batch_to_cache() {
    use tokio::io::AsyncReadExt;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_task = tokio::spawn(async move {
        if let Ok((mut socket, _)) = listener.accept().await {
            let mut buf = [0u8; 1024];
            let _ = socket.read(&mut buf).await;
            // Hold the connection open past the 2-second shutdown batch-flush timeout.
            tokio::time::sleep(Duration::from_secs(4)).await;
        }
    });

    let mut cfg = KuiperConfig::default();
    cfg.basic.enable_private_net = true;
    let bus = StreamBus::new();
    let state = AppState::new(
        "test".to_owned(),
        cfg,
        StreamManager::new(),
        TableManager::new(),
        RuleManager::new(bus.clone()),
        bus,
    );

    let rule_id = format!(
        "r_batch_timeout_{}_{}",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
    );
    let cache_dir = std::path::Path::new("data/cache/sink").join(&rule_id);
    let _ = std::fs::remove_dir_all(&cache_dir);

    let app = create_router(state.clone());

    let stream_req = Request::builder()
        .method("POST")
        .uri("/streams")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "sql": "CREATE STREAM s_batch_timeout (a bigint) WITH (TYPE=\"memory\", DATASOURCE=\"in_batch_timeout\")"
            })
            .to_string(),
        ))
        .unwrap();
    let stream_resp = app.clone().oneshot(stream_req).await.unwrap();
    assert_eq!(stream_resp.status(), StatusCode::CREATED);

    let rule_req = Request::builder()
        .method("POST")
        .uri("/rules")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "id": rule_id,
                "sql": "SELECT a FROM s_batch_timeout",
                "actions": [{
                    "rest": {
                        "name": "slow_batch",
                        "url": format!("http://{addr}/ingest"),
                        "method": "POST",
                        "batchSize": 10,
                        "lingerInterval": 60000,
                        "enableCache": true,
                        "cleanCacheAtStop": false
                    }
                }]
            })
            .to_string(),
        ))
        .unwrap();
    let rule_resp = app.clone().oneshot(rule_req).await.unwrap();
    assert_eq!(rule_resp.status(), StatusCode::CREATED);

    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut row = HashMap::new();
    row.insert("a".to_owned(), json!(42));
    let _ = state.stream_bus.publish(
        "in_batch_timeout",
        rekuiper_core::model::StreamRecord::from_hashmap(row),
    );
    tokio::time::sleep(Duration::from_millis(50)).await;

    let stop_req = Request::builder()
        .method("POST")
        .uri(format!("/rules/{rule_id}/stop"))
        .body(Body::empty())
        .unwrap();
    let stop_resp = app.oneshot(stop_req).await.unwrap();
    assert_eq!(stop_resp.status(), StatusCode::OK);

    // Check that the timed-out batch was persisted to the rule's sink cache on disk.
    let mut has_cache_files = false;
    if let Ok(entries) = std::fs::read_dir(&cache_dir) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                if let Ok(sub) = std::fs::read_dir(&p) {
                    if sub
                        .flatten()
                        .any(|e| e.file_name().to_string_lossy().ends_with(".jsonl"))
                    {
                        has_cache_files = true;
                        break;
                    }
                }
            } else if entry.file_name().to_string_lossy().ends_with(".jsonl") {
                has_cache_files = true;
                break;
            }
        }
    }
    assert!(
        has_cache_files,
        "timed-out shutdown batch must be persisted to sink cache at {}",
        cache_dir.display()
    );

    server_task.abort();
    let _ = std::fs::remove_dir_all(&cache_dir);
}

#[tokio::test]
async fn inflight_batch_send_aborted_by_stop_persists_to_cache() {
    use tokio::io::AsyncReadExt;

    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel::<()>();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_task = tokio::spawn(async move {
        if let Ok((mut socket, _)) = listener.accept().await {
            let mut buf = [0u8; 1024];
            let _ = socket.read(&mut buf).await;
            let _ = accepted_tx.send(());
            // Hold the in-flight REST batch request open past the 5s sink drain abort timeout.
            tokio::time::sleep(Duration::from_secs(8)).await;
        }
    });

    let mut cfg = KuiperConfig::default();
    cfg.basic.enable_private_net = true;
    let bus = StreamBus::new();
    let state = AppState::new(
        "test".to_owned(),
        cfg,
        StreamManager::new(),
        TableManager::new(),
        RuleManager::new(bus.clone()),
        bus,
    );

    let rule_id = format!(
        "r_batch_abort_{}_{}",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
    );
    let cache_dir = std::path::Path::new("data/cache/sink").join(&rule_id);
    let _ = std::fs::remove_dir_all(&cache_dir);

    let app = create_router(state.clone());

    let stream_req = Request::builder()
        .method("POST")
        .uri("/streams")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "sql": "CREATE STREAM s_batch_abort (a bigint) WITH (TYPE=\"memory\", DATASOURCE=\"in_batch_abort\")"
            })
            .to_string(),
        ))
        .unwrap();
    let stream_resp = app.clone().oneshot(stream_req).await.unwrap();
    assert_eq!(stream_resp.status(), StatusCode::CREATED);

    let rule_req = Request::builder()
        .method("POST")
        .uri("/rules")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "id": rule_id,
                "sql": "SELECT a FROM s_batch_abort",
                "actions": [{
                    "rest": {
                        "name": "slow_inflight",
                        "url": format!("http://{addr}/ingest"),
                        "method": "POST",
                        "batchSize": 1,
                        "enableCache": true,
                        "cleanCacheAtStop": false
                    }
                }]
            })
            .to_string(),
        ))
        .unwrap();
    let rule_resp = app.clone().oneshot(rule_req).await.unwrap();
    assert_eq!(rule_resp.status(), StatusCode::CREATED);

    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut row = HashMap::new();
    row.insert("a".to_owned(), json!(99));
    let _ = state.stream_bus.publish(
        "in_batch_abort",
        rekuiper_core::model::StreamRecord::from_hashmap(row),
    );

    tokio::time::timeout(Duration::from_secs(2), accepted_rx)
        .await
        .expect("REST sink request should be in-flight")
        .unwrap();

    let stop_req = Request::builder()
        .method("POST")
        .uri(format!("/rules/{rule_id}/stop"))
        .body(Body::empty())
        .unwrap();
    let stop_resp = app.oneshot(stop_req).await.unwrap();
    assert_eq!(stop_resp.status(), StatusCode::OK);

    let mut has_cache_files = false;
    if let Ok(entries) = std::fs::read_dir(&cache_dir) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                if let Ok(sub) = std::fs::read_dir(&p) {
                    if sub
                        .flatten()
                        .any(|e| e.file_name().to_string_lossy().ends_with(".jsonl"))
                    {
                        has_cache_files = true;
                        break;
                    }
                }
            } else if entry.file_name().to_string_lossy().ends_with(".jsonl") {
                has_cache_files = true;
                break;
            }
        }
    }
    assert!(
        has_cache_files,
        "aborted in-flight batch must be persisted to sink cache at {}",
        cache_dir.display()
    );

    server_task.abort();
    let _ = std::fs::remove_dir_all(&cache_dir);
}

#[tokio::test]
async fn inflight_single_record_aborted_by_stop_persists_to_cache() {
    use tokio::io::AsyncReadExt;

    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel::<()>();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_task = tokio::spawn(async move {
        if let Ok((mut socket, _)) = listener.accept().await {
            let mut buf = [0u8; 1024];
            let _ = socket.read(&mut buf).await;
            let _ = accepted_tx.send(());
            // Hold the in-flight single-record REST request open past the 5s sink drain abort timeout.
            tokio::time::sleep(Duration::from_secs(8)).await;
        }
    });

    let mut cfg = KuiperConfig::default();
    cfg.basic.enable_private_net = true;
    let bus = StreamBus::new();
    let state = AppState::new(
        "test".to_owned(),
        cfg,
        StreamManager::new(),
        TableManager::new(),
        RuleManager::new(bus.clone()),
        bus,
    );

    let rule_id = format!(
        "r_single_abort_{}_{}",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
    );
    let cache_dir = std::path::Path::new("data/cache/sink").join(&rule_id);
    let _ = std::fs::remove_dir_all(&cache_dir);

    let app = create_router(state.clone());

    let stream_req = Request::builder()
        .method("POST")
        .uri("/streams")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "sql": "CREATE STREAM s_single_abort (a bigint) WITH (TYPE=\"memory\", DATASOURCE=\"in_single_abort\")"
            })
            .to_string(),
        ))
        .unwrap();
    let stream_resp = app.clone().oneshot(stream_req).await.unwrap();
    assert_eq!(stream_resp.status(), StatusCode::CREATED);

    let rule_req = Request::builder()
        .method("POST")
        .uri("/rules")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "id": rule_id,
                "sql": "SELECT a FROM s_single_abort",
                "actions": [{
                    "rest": {
                        "name": "slow_single",
                        "url": format!("http://{addr}/ingest"),
                        "method": "POST",
                        "enableCache": true,
                        "cleanCacheAtStop": false
                    }
                }]
            })
            .to_string(),
        ))
        .unwrap();
    let rule_resp = app.clone().oneshot(rule_req).await.unwrap();
    assert_eq!(rule_resp.status(), StatusCode::CREATED);

    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut row = HashMap::new();
    row.insert("a".to_owned(), json!(77));
    let _ = state.stream_bus.publish(
        "in_single_abort",
        rekuiper_core::model::StreamRecord::from_hashmap(row),
    );

    tokio::time::timeout(Duration::from_secs(2), accepted_rx)
        .await
        .expect("single-record REST sink request should be in-flight")
        .unwrap();

    let stop_req = Request::builder()
        .method("POST")
        .uri(format!("/rules/{rule_id}/stop"))
        .body(Body::empty())
        .unwrap();
    let stop_resp = app.oneshot(stop_req).await.unwrap();
    assert_eq!(stop_resp.status(), StatusCode::OK);

    let mut has_cache_files = false;
    if let Ok(entries) = std::fs::read_dir(&cache_dir) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                if let Ok(sub) = std::fs::read_dir(&p) {
                    if sub
                        .flatten()
                        .any(|e| e.file_name().to_string_lossy().ends_with(".jsonl"))
                    {
                        has_cache_files = true;
                        break;
                    }
                }
            } else if entry.file_name().to_string_lossy().ends_with(".jsonl") {
                has_cache_files = true;
                break;
            }
        }
    }
    assert!(
        has_cache_files,
        "aborted in-flight single record must be persisted to sink cache at {}",
        cache_dir.display()
    );

    server_task.abort();
    let _ = std::fs::remove_dir_all(&cache_dir);
}
