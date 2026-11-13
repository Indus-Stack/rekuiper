use anyhow::{bail, Result};
use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use parking_lot::Mutex;
use rekuiper_conf::KuiperConfig;
use rekuiper_core::kv::KvStore;
use rekuiper_core::manager::SchemaManager;
use rekuiper_core::{RuleManager, StreamBus, StreamManager, TableManager};
use rekuiper_server::routes::{create_router, AppState};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tower::ServiceExt;

#[derive(Default)]
struct FailingKv {
    data: Mutex<HashMap<(String, String), String>>,
    fail_writes: AtomicBool,
    fail_deletes: AtomicBool,
}

#[async_trait]
impl KvStore for FailingKv {
    async fn get(&self, namespace: &str, key: &str) -> Result<Option<String>> {
        Ok(self
            .data
            .lock()
            .get(&(namespace.to_owned(), key.to_owned()))
            .cloned())
    }

    async fn set(&self, namespace: &str, key: &str, val: &str) -> Result<()> {
        if self.fail_writes.load(Ordering::SeqCst) {
            bail!("injected kv write failure");
        }
        self.data
            .lock()
            .insert((namespace.to_owned(), key.to_owned()), val.to_owned());
        Ok(())
    }

    async fn delete(&self, namespace: &str, key: &str) -> Result<()> {
        if self.fail_deletes.load(Ordering::SeqCst) {
            bail!("injected kv delete failure");
        }
        self.data
            .lock()
            .remove(&(namespace.to_owned(), key.to_owned()));
        Ok(())
    }

    async fn list_all(&self, namespace: &str) -> Result<Vec<(String, String)>> {
        Ok(self
            .data
            .lock()
            .iter()
            .filter(|((ns, _), _)| ns == namespace)
            .map(|((_, k), v)| (k.clone(), v.clone()))
            .collect())
    }
}

fn build_state(kv: Arc<FailingKv>) -> AppState {
    let stream_bus = StreamBus::new();
    let mut state = AppState::new(
        "test".to_owned(),
        KuiperConfig::default(),
        StreamManager::new(),
        TableManager::new(),
        RuleManager::new(stream_bus.clone()),
        stream_bus,
    );
    state.schema_manager = SchemaManager::new_with_kv(kv);
    state
}

#[tokio::test]
async fn schema_handlers_propagate_persistence_failures_and_preserve_state() {
    let kv = Arc::new(FailingKv::default());
    let state = build_state(kv.clone());
    let app = create_router(state.clone());

    // 1. Injected write failure on create returns 500 and does not register schema.
    kv.fail_writes.store(true, Ordering::SeqCst);
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/schemas/custom")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name":"s1","content":"v1"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert!(state.schema_manager.get_schema("custom", "s1").is_none());

    // Simulate restart from KV: failed create is absent.
    let restarted = SchemaManager::new();
    let kv_trait: Arc<dyn KvStore> = kv.clone();
    restarted.load_from_kv(&kv_trait).await.unwrap();
    assert!(restarted.get_schema("custom", "s1").is_none());

    // 2. Successful create works when KV is healthy.
    kv.fail_writes.store(false, Ordering::SeqCst);
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/schemas/custom")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name":"s1","content":"v1"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // 3. Failed update returns 500 and keeps the old schema intact.
    kv.fail_writes.store(true, Ordering::SeqCst);
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/schemas/custom/s1")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"content":"v2"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        state
            .schema_manager
            .get_schema("custom", "s1")
            .unwrap()
            .content
            .as_deref(),
        Some("v1")
    );

    // 4. Failed delete returns 500 and keeps the schema intact.
    kv.fail_deletes.store(true, Ordering::SeqCst);
    let resp = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/schemas/custom/s1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert!(state.schema_manager.get_schema("custom", "s1").is_some());
}
