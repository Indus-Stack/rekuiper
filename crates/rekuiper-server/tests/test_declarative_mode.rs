use parking_lot::RwLock;
use rekuiper_conf::KuiperConfig;
use rekuiper_core::{RuleManager, StreamBus, StreamManager, TableManager};
use rekuiper_server::declarative::reconcile_declarative_path;
use rekuiper_server::routes::{create_router, load_config_maps, AppState};
use reqwest::StatusCode;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::net::TcpListener;

struct TempDir(PathBuf);
impl TempDir {
    fn new(name: &str) -> Self {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "rekuiper_decl_test_{}_{}",
            name,
            uuid::Uuid::new_v4()
        ));
        let _ = fs::create_dir_all(&p);
        Self(p)
    }

    fn file_path(&self, filename: &str) -> PathBuf {
        self.0.join(filename)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct DeclarativeServer {
    base_url: String,
    state: AppState,
    handle: tokio::task::JoinHandle<()>,
}

impl DeclarativeServer {
    async fn start(manifest_path: PathBuf) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let mut config = KuiperConfig::default();
        config.basic.http_server_ip = "127.0.0.1".to_string();
        config.basic.http_server_port = addr.port();

        let stream_bus = StreamBus::new();
        let stream_manager = StreamManager::new();
        let table_manager = TableManager::new();
        let rule_manager = RuleManager::new(stream_bus.clone());

        let mut state = AppState::new(
            "test_declarative".to_string(),
            config,
            stream_manager,
            table_manager,
            rule_manager,
            stream_bus,
        );

        state.declarative_mode = true;
        state.declarative_path = Some(manifest_path.clone());
        state.declarative_watch = false;
        state.active_rule_hashes = Arc::new(RwLock::new(HashMap::new()));
        state.declarative_reload_failures = Arc::new(AtomicU64::new(0));

        load_config_maps(&state).await.unwrap();

        // Initial manifest reconciliation
        reconcile_declarative_path(&state, &manifest_path)
            .await
            .expect("Initial declarative reconciliation should succeed");

        let app = create_router(state.clone());
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        Self {
            base_url: format!("http://{}", addr),
            state,
            handle,
        }
    }

    async fn shutdown(self) {
        self.handle.abort();
    }
}

#[tokio::test]
async fn test_declarative_mode_lifecycle_and_guard() {
    let temp = TempDir::new("lifecycle");
    let manifest_file = temp.file_path("pipelines.yaml");

    let initial_yaml = r#"
version: "1.0"
streams:
  demo_stream: "CREATE STREAM demo_stream (temperature float, humidity float) WITH (DATASOURCE=\"telemetry\", FORMAT=\"json\");"
rules:
  rule_alpha:
    sql: "SELECT temperature FROM demo_stream WHERE temperature > 30.0"
    actions:
      - log: {}
"#;
    fs::write(&manifest_file, initial_yaml).unwrap();

    let server = DeclarativeServer::start(manifest_file.clone()).await;
    let client = reqwest::Client::new();

    // 1. Verify initial reconciliation loaded stream and started rule
    let res = client
        .get(format!("{}/streams/demo_stream", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let res = client
        .get(format!("{}/rules/rule_alpha/status", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let status_body: Value = res.json().await.unwrap();
    assert_eq!(status_body["status"], "running");

    // 2. Verify Read-Only Mutation Guard (allowlist blocks mutating control plane)
    let res = client
        .post(format!("{}/streams", server.base_url))
        .json(&json!({"sql": "CREATE STREAM rogue () WITH (FORMAT=\"json\");"}))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);

    let res = client
        .post(format!("{}/tables", server.base_url))
        .json(&json!({"sql": "CREATE TABLE rogue () WITH (FORMAT=\"json\");"}))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);

    let res = client
        .post(format!("{}/rules", server.base_url))
        .json(&json!({"id": "rogue", "sql": "SELECT 1"}))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);

    let res = client
        .post(format!("{}/ruleset/import", server.base_url))
        .json(&json!({"streams": {}}))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);

    let res = client
        .delete(format!("{}/rules/rule_alpha", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);

    let res = client
        .delete(format!("{}/streams/demo_stream", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);

    let res = client
        .post(format!("{}/plugins/udfs", server.base_url))
        .json(&json!({"name": "rogue"}))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);

    // 3. Verify Allowlisted mutation paths: data ingestion plane & ruletest
    server.state.http_push_endpoints.write().insert(
        "/data/demo_stream".to_string(),
        rekuiper_server::state::HttpPushEndpoint {
            stream_name: "demo_stream".to_string(),
            path: "/data/demo_stream".to_string(),
            method: "POST".to_string(),
            format: "json".to_string(),
        },
    );
    let res = client
        .post(format!("{}/data/demo_stream", server.base_url))
        .json(&json!({"temperature": 42.0, "humidity": 65.0}))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // Ephemeral ruletest endpoint is allowlisted (must not return 403 Forbidden)
    let res = client
        .post(format!("{}/ruletest", server.base_url))
        .send()
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::FORBIDDEN);

    // 4. Verify Smart Diffing: update manifest with rule_alpha unchanged and rule_beta added
    let updated_yaml = r#"
version: "1.0"
streams:
  demo_stream: "CREATE STREAM demo_stream (temperature float, humidity float) WITH (DATASOURCE=\"telemetry\", FORMAT=\"json\");"
rules:
  rule_alpha:
    sql: "SELECT temperature FROM demo_stream WHERE temperature > 30.0"
    actions:
      - log: {}
  rule_beta:
    sql: "SELECT humidity FROM demo_stream WHERE humidity > 80.0"
    actions:
      - log: {}
"#;
    fs::write(&manifest_file, updated_yaml).unwrap();

    // Trigger reload via allowlisted POST /declarative/reload
    let res = client
        .post(format!("{}/declarative/reload", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let reload_report: Value = res.json().await.unwrap();
    assert_eq!(reload_report["status"], "reloaded");
    // rule_alpha was unchanged, rule_beta was started
    assert_eq!(reload_report["rules_unchanged"], 1);
    assert_eq!(reload_report["rules_started"], 1);

    // Verify both rules are running
    let res = client
        .get(format!("{}/rules/rule_beta/status", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let status_beta: Value = res.json().await.unwrap();
    assert_eq!(status_beta["status"], "running");

    // 5. Verify Transitive Invalidation: Change demo_stream DDL (add pressure float)
    let transitive_yaml = r#"
version: "1.0"
streams:
  demo_stream: "CREATE STREAM demo_stream (temperature float, humidity float, pressure float) WITH (DATASOURCE=\"telemetry\", FORMAT=\"json\");"
rules:
  rule_alpha:
    sql: "SELECT temperature FROM demo_stream WHERE temperature > 30.0"
    actions:
      - log: {}
  rule_beta:
    sql: "SELECT humidity FROM demo_stream WHERE humidity > 80.0"
    actions:
      - log: {}
"#;
    fs::write(&manifest_file, transitive_yaml).unwrap();

    let res = client
        .post(format!("{}/declarative/reload", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let reload_report: Value = res.json().await.unwrap();
    // Because upstream demo_stream changed, both rules that depend on it had their RuleEffectiveHash changed and restarted!
    assert_eq!(reload_report["rules_restarted"], 2);
    assert_eq!(reload_report["rules_unchanged"], 0);

    // 6. Verify Atomic Failure Handling: introduce invalid SQL syntax in manifest
    let invalid_yaml = r#"
version: "1.0"
streams:
  demo_stream: "CREATE STREAM demo_stream (temperature float) WITH (DATASOURCE=\"telemetry\", FORMAT=\"json\");"
rules:
  broken_rule:
    sql: "THIS IS INVALID SQL !!!"
    actions:
      - log: {}
"#;
    fs::write(&manifest_file, invalid_yaml).unwrap();

    let res = client
        .post(format!("{}/declarative/reload", server.base_url))
        .send()
        .await
        .unwrap();
    // Reload must fail atomically with 400 Bad Request
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);

    // Previously running rule_alpha must NOT be disturbed!
    let res = client
        .get(format!("{}/rules/rule_alpha/status", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let status_alpha: Value = res.json().await.unwrap();
    assert_eq!(status_alpha["status"], "running");

    // Verify Prometheus metric reflects failure
    let res = client
        .get(format!("{}/metrics", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let metrics_text = res.text().await.unwrap();
    assert!(metrics_text.contains("rekuiper_declarative_reload_failures_total 1"));

    server.shutdown().await;
}

#[tokio::test]
async fn test_declarative_directory_merge() {
    let temp = TempDir::new("dir_merge");
    let pipelines_dir = temp.file_path("pipelines.d");
    fs::create_dir_all(&pipelines_dir).unwrap();

    let file_01 = pipelines_dir.join("01-streams.yaml");
    let file_02 = pipelines_dir.join("02-rules.yaml");

    fs::write(
        &file_01,
        r#"
streams:
  shared_stream: "CREATE STREAM shared_stream () WITH (DATASOURCE=\"telemetry\", FORMAT=\"json\");"
"#,
    )
    .unwrap();

    fs::write(
        &file_02,
        r#"
rules:
  alert_rule:
    sql: "SELECT * FROM shared_stream"
    actions:
      - log: {}
"#,
    )
    .unwrap();

    let server = DeclarativeServer::start(pipelines_dir.clone()).await;
    let client = reqwest::Client::new();

    let res = client
        .get(format!("{}/streams/shared_stream", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let res = client
        .get(format!("{}/rules/alert_rule/status", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    server.shutdown().await;
}
