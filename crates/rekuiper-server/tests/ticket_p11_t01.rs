use rekuiper_core::model::StreamRecord;
use rekuiper_server::engine::sinks::resolve_rule_action_identities;
use rekuiper_server::sink_cache::{
    cache_dir_for_identity, CacheConfig, CacheOwnershipError, CacheOwnershipManifest, SinkCache,
};
use serde_json::json;
use std::collections::HashMap;
use std::path::PathBuf;

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "rekuiper_p11_t01_{name}_{}_{}",
        std::process::id(),
        i64::try_from(jiff::Timestamp::now().as_nanosecond()).unwrap_or(0)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn rest_action(
    name: Option<&str>,
    url: &str,
    token: &str,
) -> HashMap<String, HashMap<String, serde_json::Value>> {
    let mut props = HashMap::new();
    if let Some(n) = name {
        props.insert("name".to_owned(), json!(n));
    }
    props.insert("url".to_owned(), json!(url));
    props.insert("token".to_owned(), json!(token));
    let mut action = HashMap::new();
    action.insert("rest".to_owned(), props);
    action
}

#[test]
fn p11_t01_named_action_reorder_duplicate_rejection_and_cache_ownership() {
    let root = temp_dir("ownership");

    // 1. Two named REST actions keep their own backlog directories even when reordered.
    let a1 = rest_action(Some("primary_webhook"), "https://a.example/hook", "sec1");
    let a2 = rest_action(Some("backup_webhook"), "https://b.example/hook", "sec2");
    let ids_order1 = resolve_rule_action_identities(&[a1.clone(), a2.clone()]).unwrap();
    let ids_order2 = resolve_rule_action_identities(&[a2.clone(), a1.clone()]).unwrap();
    assert_eq!(ids_order1[0], ids_order2[1]);
    assert_eq!(ids_order1[1], ids_order2[0]);

    let dir_a1 = cache_dir_for_identity(&root, "rule_1", &ids_order1[0]);
    let manifest_a1 = CacheOwnershipManifest::new("rule_1", &ids_order1[0], "rest", &a1["rest"]);
    {
        let mut cache =
            SinkCache::open_with_ownership(CacheConfig::default(), dir_a1.clone(), &manifest_a1)
                .unwrap();
        let mut rec = StreamRecord::from_hashmap(HashMap::from([("v".to_owned(), json!(1))]));
        rec.timestamp = 1;
        cache.push(rec);
        cache.close();
    }

    // 2. Credential rotation (token changes, URL unchanged) retains destination ownership.
    let a1_rotated = rest_action(
        Some("primary_webhook"),
        "https://a.example/hook",
        "new_secret",
    );
    let manifest_rotated =
        CacheOwnershipManifest::new("rule_1", &ids_order2[1], "rest", &a1_rotated["rest"]);
    assert_eq!(
        manifest_a1.destination_fingerprint,
        manifest_rotated.destination_fingerprint
    );
    let cache_reopened =
        SinkCache::open_with_ownership(CacheConfig::default(), dir_a1.clone(), &manifest_rotated)
            .unwrap();
    assert_eq!(cache_reopened.len(), 1);
    cache_reopened.close();

    // 3. Changing destination URL fails ownership check instead of adopting wrong backlog.
    let a1_changed_dest = rest_action(Some("primary_webhook"), "https://evil.example/hook", "sec1");
    let manifest_changed =
        CacheOwnershipManifest::new("rule_1", &ids_order1[0], "rest", &a1_changed_dest["rest"]);
    let err = SinkCache::open_with_ownership(CacheConfig::default(), dir_a1, &manifest_changed)
        .err()
        .expect("changed destination must fail ownership check");
    assert!(matches!(err, CacheOwnershipError::OwnershipMismatch { .. }));

    // 4. Duplicate names fail.
    let dup = resolve_rule_action_identities(&[a1.clone(), a1]);
    assert!(dup.is_err());

    // 5. Unnamed actions preserve positional identifiers.
    let u0 = rest_action(None, "https://u0.example", "t");
    let u1 = rest_action(None, "https://u1.example", "t");
    let unnamed_ids = resolve_rule_action_identities(&[u0, u1]).unwrap();
    assert_eq!(unnamed_ids[0].as_str(), "index.0");
    assert_eq!(unnamed_ids[1].as_str(), "index.1");

    // 6. Unowned legacy cache directory is not silently adopted.
    let legacy_dir = root.join("legacy_unowned");
    std::fs::create_dir_all(&legacy_dir).unwrap();
    std::fs::write(legacy_dir.join("page_0.jsonl"), b"{}\n").unwrap();
    let legacy_err =
        SinkCache::open_with_ownership(CacheConfig::default(), legacy_dir, &manifest_a1)
            .err()
            .expect("unowned legacy cache must not be silently adopted");
    assert!(matches!(
        legacy_err,
        CacheOwnershipError::UnownedLegacyCache
    ));

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn p11_t01_routing_header_changes_ownership_while_credential_header_rotates() {
    let root = temp_dir("routing_headers");
    let mut base_props = HashMap::new();
    base_props.insert("name".to_owned(), json!("tenant_sink"));
    base_props.insert("url".to_owned(), json!("https://shared.example/ingest"));
    base_props.insert(
        "headers".to_owned(),
        json!({
            "X-Tenant-Id": "tenant-a",
            "Authorization": "Bearer secret-1"
        }),
    );
    let mut action_a = HashMap::new();
    action_a.insert("rest".to_owned(), base_props.clone());
    let ids = resolve_rule_action_identities(&[action_a]).unwrap();
    let dir = cache_dir_for_identity(&root, "rule_headers", &ids[0]);
    let manifest_a = CacheOwnershipManifest::new("rule_headers", &ids[0], "rest", &base_props);

    {
        let mut cache =
            SinkCache::open_with_ownership(CacheConfig::default(), dir.clone(), &manifest_a)
                .unwrap();
        let rec = StreamRecord::from_hashmap(HashMap::from([("tenant".to_owned(), json!("a"))]));
        cache.push(rec);
        cache.close();
    }

    // Rotating Authorization header preserves ownership for tenant-a.
    let mut rotated_props = base_props.clone();
    rotated_props.insert(
        "headers".to_owned(),
        json!({
            "X-Tenant-Id": "tenant-a",
            "Authorization": "Bearer secret-2"
        }),
    );
    let manifest_rotated =
        CacheOwnershipManifest::new("rule_headers", &ids[0], "rest", &rotated_props);
    let mut cache_rotated =
        SinkCache::open_with_ownership(CacheConfig::default(), dir.clone(), &manifest_rotated)
            .unwrap();
    assert!(cache_rotated.pop_front().is_some());
    cache_rotated.close();

    // Changing X-Tenant-Id routing header must be rejected as an ownership mismatch.
    let mut tenant_b_props = base_props;
    tenant_b_props.insert(
        "headers".to_owned(),
        json!({
            "X-Tenant-Id": "tenant-b",
            "Authorization": "Bearer secret-2"
        }),
    );
    let manifest_b = CacheOwnershipManifest::new("rule_headers", &ids[0], "rest", &tenant_b_props);
    let err = SinkCache::open_with_ownership(CacheConfig::default(), dir.clone(), &manifest_b)
        .err()
        .expect("changing routing header X-Tenant-Id must fail ownership check");
    assert!(matches!(err, CacheOwnershipError::OwnershipMismatch { .. }));

    // Adding a case-variant duplicate routing header alongside the original must also fail.
    let mut mixed_case_props = tenant_b_props;
    mixed_case_props.insert(
        "headers".to_owned(),
        json!({
            "X-Tenant-Id": "tenant-a",
            "x-tenant-id": "tenant-b",
            "Authorization": "Bearer secret-2"
        }),
    );
    let manifest_mixed =
        CacheOwnershipManifest::new("rule_headers", &ids[0], "rest", &mixed_case_props);
    let err_mixed =
        SinkCache::open_with_ownership(CacheConfig::default(), dir.clone(), &manifest_mixed)
            .err()
            .expect("adding case-variant routing header must fail ownership check");
    assert!(matches!(
        err_mixed,
        CacheOwnershipError::OwnershipMismatch { .. }
    ));

    // Changing top-level `headers` to `Headers` (which REST sink ignores) must also fail ownership check.
    let mut wrong_case_prop = HashMap::new();
    wrong_case_prop.insert("name".to_owned(), json!("tenant_sink"));
    wrong_case_prop.insert("url".to_owned(), json!("https://shared.example/ingest"));
    wrong_case_prop.insert(
        "Headers".to_owned(),
        json!({
            "X-Tenant-Id": "tenant-a",
            "Authorization": "Bearer secret-2"
        }),
    );
    let manifest_wrong_case =
        CacheOwnershipManifest::new("rule_headers", &ids[0], "rest", &wrong_case_prop);
    let err_wrong_case =
        SinkCache::open_with_ownership(CacheConfig::default(), dir, &manifest_wrong_case)
            .err()
            .expect("changing `headers` to `Headers` must fail ownership check");
    assert!(matches!(
        err_wrong_case,
        CacheOwnershipError::OwnershipMismatch { .. }
    ));

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn p11_t01_verify_ownership_is_read_only() {
    let root = temp_dir("verify_readonly");
    let mut props = HashMap::new();
    props.insert("name".to_owned(), json!("new_action"));
    props.insert("url".to_owned(), json!("https://example.invalid/a"));

    let mut action = HashMap::new();
    action.insert("rest".to_owned(), props.clone());
    let ids = resolve_rule_action_identities(&[action]).unwrap();
    let dir = cache_dir_for_identity(&root, "rule_ro", &ids[0]);
    let manifest = CacheOwnershipManifest::new("rule_ro", &ids[0], "rest", &props);

    let exists = SinkCache::verify_ownership(&dir, &manifest)
        .expect("fresh directory should pass read-only ownership check");
    assert!(!exists);
    assert!(
        !dir.exists(),
        "read-only ownership check must not create cache directory or ownership_manifest.json"
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn p11_t01_update_rule_awaits_sink_handle_before_returning() {
    use rekuiper_core::{RuleDefinition, RuleManager, StreamBus};
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    let mgr = RuleManager::new(StreamBus::new());
    let def: RuleDefinition = serde_json::from_value(json!({
        "id": "r_sync",
        "name": "r_sync",
        "sql": "SELECT * FROM demo",
        "actions": []
    }))
    .unwrap();
    mgr.create_rule(def.clone()).await.unwrap();
    mgr.start_rule("r_sync").await.unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::channel::<u8>(1);
    let cleaned = Arc::new(AtomicBool::new(false));
    let cleaned_bg = cleaned.clone();

    let compute = tokio::spawn(async move {
        let _keep_tx = tx;
        futures::future::pending::<()>().await;
    });
    let sink = tokio::spawn(async move {
        while rx.recv().await.is_some() {}
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        cleaned_bg.store(true, Ordering::SeqCst);
    });

    mgr.set_rule_handle("r_sync", compute);
    mgr.set_rule_sink_handle("r_sync", sink);

    let was_running = mgr.update_rule(def).await.unwrap();
    assert!(was_running);
    assert!(
        cleaned.load(Ordering::SeqCst),
        "update_rule must await old sink handle completion before returning"
    );
}

#[tokio::test]
async fn p11_t01_blocked_sink_does_not_stall_unrelated_rule_stop() {
    use rekuiper_core::{RuleDefinition, RuleManager, StreamBus};
    use std::sync::Arc;
    use tokio::sync::Notify;

    let mgr = Arc::new(RuleManager::new(StreamBus::new()));
    let def_a: RuleDefinition = serde_json::from_value(json!({
        "id": "rule_blocked",
        "name": "rule_blocked",
        "sql": "SELECT * FROM demo",
        "actions": []
    }))
    .unwrap();
    let def_b: RuleDefinition = serde_json::from_value(json!({
        "id": "rule_other",
        "name": "rule_other",
        "sql": "SELECT * FROM demo",
        "actions": []
    }))
    .unwrap();

    mgr.create_rule(def_a).await.unwrap();
    mgr.start_rule("rule_blocked").await.unwrap();
    mgr.create_rule(def_b).await.unwrap();
    mgr.start_rule("rule_other").await.unwrap();

    let release = Arc::new(Notify::new());
    let release_bg = release.clone();
    let compute_a = tokio::spawn(async move {
        futures::future::pending::<()>().await;
    });
    let sink_a = tokio::spawn(async move {
        release_bg.notified().await;
    });
    mgr.set_rule_handle("rule_blocked", compute_a);
    mgr.set_rule_sink_handle("rule_blocked", sink_a);

    let mgr_a = mgr.clone();
    let stop_a = tokio::spawn(async move { mgr_a.stop_rule("rule_blocked").await });
    tokio::task::yield_now().await;

    tokio::time::timeout(
        std::time::Duration::from_millis(250),
        mgr.stop_rule("rule_other"),
    )
    .await
    .expect("unrelated rule stop must not be stalled by blocked sink")
    .unwrap();

    release.notify_one();
    stop_a.await.unwrap().unwrap();
}

#[test]
fn p11_t01_verify_ownership_rejects_non_directory_cache_parent() {
    let root = temp_dir("non_dir_parent");
    std::fs::create_dir_all(&root).unwrap();
    let file_parent = root.join("rule_file");
    std::fs::write(&file_parent, b"not a directory").unwrap();

    let mut props = HashMap::new();
    props.insert("name".to_owned(), json!("a1"));
    props.insert("url".to_owned(), json!("https://example.invalid/a"));

    let mut action = HashMap::new();
    action.insert("rest".to_owned(), props.clone());
    let ids = resolve_rule_action_identities(&[action]).unwrap();
    let dir = cache_dir_for_identity(&root, "rule_file", &ids[0]);
    let manifest = CacheOwnershipManifest::new("rule_file", &ids[0], "rest", &props);

    let err = SinkCache::verify_ownership(&dir, &manifest)
        .expect_err("non-directory cache parent must fail preflight ownership check");
    assert!(matches!(err, CacheOwnershipError::Storage(_)));

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn p11_t01_canceled_update_leaves_recoverable_stopped_state_after_sink_close() {
    use rekuiper_core::{RuleDefinition, RuleManager, StreamBus};
    use std::sync::Arc;
    use tokio::sync::Notify;

    let mgr = Arc::new(RuleManager::new(StreamBus::new()));
    let def_v1: RuleDefinition = serde_json::from_value(json!({
        "id": "r_cancel",
        "name": "r_cancel",
        "sql": "SELECT 1 FROM demo",
        "actions": []
    }))
    .unwrap();
    let def_v2: RuleDefinition = serde_json::from_value(json!({
        "id": "r_cancel",
        "name": "r_cancel",
        "sql": "SELECT 2 FROM demo",
        "actions": []
    }))
    .unwrap();

    mgr.create_rule(def_v1).await.unwrap();
    mgr.start_rule("r_cancel").await.unwrap();

    let release = Arc::new(Notify::new());
    let release_bg = release.clone();
    let compute = tokio::spawn(async move {
        futures::future::pending::<()>().await;
    });
    let sink = tokio::spawn(async move {
        release_bg.notified().await;
    });
    mgr.set_rule_handle("r_cancel", compute);
    mgr.set_rule_sink_handle("r_cancel", sink);

    let timed_out = tokio::time::timeout(
        std::time::Duration::from_millis(30),
        mgr.update_rule(def_v2),
    )
    .await;
    assert!(
        timed_out.is_err(),
        "update_rule should wait on sink shutdown and time out"
    );

    release.notify_one();
    // Acquiring start_rule waits for the detached handoff to finish closing the sink.
    mgr.start_rule("r_cancel").await.unwrap();
    assert_eq!(
        mgr.get_rule_status("r_cancel").unwrap().status,
        "running",
        "start_rule must be able to resume after a canceled update"
    );
    assert_eq!(mgr.get_rule("r_cancel").unwrap().sql, "SELECT 2 FROM demo");
}

#[tokio::test]
async fn p11_t01_canceled_restart_leaves_recoverable_stopped_state_after_sink_close() {
    use rekuiper_core::{RuleDefinition, RuleManager, StreamBus};
    use std::sync::Arc;
    use tokio::sync::Notify;

    let mgr = Arc::new(RuleManager::new(StreamBus::new()));
    let def: RuleDefinition = serde_json::from_value(json!({
        "id": "r_restart_cancel",
        "name": "r_restart_cancel",
        "sql": "SELECT 1 FROM demo",
        "actions": []
    }))
    .unwrap();

    mgr.create_rule(def).await.unwrap();
    mgr.start_rule("r_restart_cancel").await.unwrap();

    let release = Arc::new(Notify::new());
    let release_bg = release.clone();
    let compute = tokio::spawn(async move {
        futures::future::pending::<()>().await;
    });
    let sink = tokio::spawn(async move {
        release_bg.notified().await;
    });
    mgr.set_rule_handle("r_restart_cancel", compute);
    mgr.set_rule_sink_handle("r_restart_cancel", sink);

    let timed_out = tokio::time::timeout(
        std::time::Duration::from_millis(30),
        mgr.restart_rule("r_restart_cancel"),
    )
    .await;
    assert!(
        timed_out.is_err(),
        "restart_rule should wait on sink shutdown and time out"
    );

    release.notify_one();
    mgr.start_rule("r_restart_cancel").await.unwrap();
    assert_eq!(
        mgr.get_rule_status("r_restart_cancel").unwrap().status,
        "running",
        "start_rule must be able to resume after a canceled restart"
    );
}

#[tokio::test]
async fn p11_t01_queued_delete_is_not_undone_by_restart() {
    use rekuiper_core::{KvStore, MemKvStore, RuleDefinition, RuleManager, StreamBus};
    use std::sync::Arc;
    use tokio::sync::Notify;

    let kv = Arc::new(MemKvStore::new());
    let mgr = Arc::new(RuleManager::new_with_kv(StreamBus::new(), kv.clone()));
    let def: RuleDefinition = serde_json::from_value(json!({
        "id": "r_del_restart",
        "name": "r_del_restart",
        "sql": "SELECT 1 FROM demo",
        "actions": []
    }))
    .unwrap();

    mgr.create_rule(def).await.unwrap();
    mgr.start_rule("r_del_restart").await.unwrap();

    let release = Arc::new(Notify::new());
    let release_bg = release.clone();
    let compute = tokio::spawn(async move {
        futures::future::pending::<()>().await;
    });
    let sink = tokio::spawn(async move {
        release_bg.notified().await;
    });
    mgr.set_rule_handle("r_del_restart", compute);
    mgr.set_rule_sink_handle("r_del_restart", sink);

    let mgr_r = mgr.clone();
    let restart_fut = tokio::spawn(async move { mgr_r.restart_rule("r_del_restart").await });
    tokio::task::yield_now().await;

    let mgr_d = mgr.clone();
    let delete_fut = tokio::spawn(async move { mgr_d.delete_rule("r_del_restart").await });
    tokio::task::yield_now().await;

    release.notify_one();
    restart_fut.await.unwrap().unwrap();
    delete_fut.await.unwrap().unwrap();

    assert!(mgr.get_rule("r_del_restart").is_none());
    assert!(
        kv.list_all("rules").await.unwrap().is_empty(),
        "deleted rule must not be resurrected in KV by restart"
    );
}

#[tokio::test]
async fn p11_t01_canceled_stop_holds_lock_until_old_sink_closes() {
    use rekuiper_core::{RuleDefinition, RuleManager, StreamBus};
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    use tokio::sync::Notify;

    let mgr = Arc::new(RuleManager::new(StreamBus::new()));
    let def: RuleDefinition = serde_json::from_value(json!({
        "id": "r_stop_cancel",
        "name": "r_stop_cancel",
        "sql": "SELECT 1 FROM demo",
        "actions": []
    }))
    .unwrap();

    mgr.create_rule(def).await.unwrap();
    mgr.start_rule("r_stop_cancel").await.unwrap();

    let release = Arc::new(Notify::new());
    let release_bg = release.clone();
    let cleaned = Arc::new(AtomicBool::new(false));
    let cleaned_bg = cleaned.clone();

    let compute = tokio::spawn(async move {
        futures::future::pending::<()>().await;
    });
    let sink = tokio::spawn(async move {
        release_bg.notified().await;
        cleaned_bg.store(true, Ordering::SeqCst);
    });
    mgr.set_rule_handle("r_stop_cancel", compute);
    mgr.set_rule_sink_handle("r_stop_cancel", sink);

    let timed_out = tokio::time::timeout(
        std::time::Duration::from_millis(30),
        mgr.stop_rule("r_stop_cancel"),
    )
    .await;
    assert!(
        timed_out.is_err(),
        "stop_rule should wait on sink shutdown and time out"
    );

    let mgr_s = mgr.clone();
    let mut start_fut = tokio::spawn(async move { mgr_s.start_rule("r_stop_cancel").await });
    let early = tokio::time::timeout(std::time::Duration::from_millis(50), &mut start_fut).await;
    assert!(
        early.is_err(),
        "start_rule must wait until the old sink finishes closing after a canceled stop"
    );
    assert!(!cleaned.load(Ordering::SeqCst));

    release.notify_one();
    start_fut.await.unwrap().unwrap();
    assert!(cleaned.load(Ordering::SeqCst));
}

#[tokio::test]
async fn p11_t01_persistence_failure_preserves_source_cleanup_across_lifecycle_ops() {
    use async_trait::async_trait;
    use rekuiper_core::{KvStore, RuleDefinition, RuleManager, StreamBus};
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    };

    struct FailingKv {
        fail: AtomicBool,
    }

    #[async_trait]
    impl KvStore for FailingKv {
        async fn get(&self, _ns: &str, _key: &str) -> anyhow::Result<Option<String>> {
            Ok(None)
        }
        async fn set(&self, _ns: &str, _key: &str, _value: &str) -> anyhow::Result<()> {
            if self.fail.load(Ordering::SeqCst) {
                anyhow::bail!("injected kv set failure");
            }
            Ok(())
        }
        async fn delete(&self, _ns: &str, _key: &str) -> anyhow::Result<()> {
            if self.fail.load(Ordering::SeqCst) {
                anyhow::bail!("injected kv delete failure");
            }
            Ok(())
        }
        async fn list_all(&self, _ns: &str) -> anyhow::Result<Vec<(String, String)>> {
            Ok(vec![])
        }
    }

    let kv = Arc::new(FailingKv {
        fail: AtomicBool::new(false),
    });
    let mgr = RuleManager::new_with_kv(StreamBus::new(), kv.clone());
    let def: RuleDefinition = serde_json::from_value(json!({
        "id": "r_kv_fail",
        "name": "r_kv_fail",
        "sql": "SELECT 1 FROM demo",
        "actions": []
    }))
    .unwrap();

    mgr.create_rule(def.clone()).await.unwrap();
    mgr.start_rule("r_kv_fail").await.unwrap();
    kv.fail.store(true, Ordering::SeqCst);

    let cleanup_calls = Arc::new(AtomicUsize::new(0));
    let bump = || {
        let c = cleanup_calls.clone();
        move || {
            c.fetch_add(1, Ordering::SeqCst);
        }
    };

    assert!(mgr
        .stop_rule_with_cleanup("r_kv_fail", bump())
        .await
        .is_err());
    assert!(mgr
        .restart_rule_with_cleanup("r_kv_fail", bump())
        .await
        .is_err());
    assert!(mgr.update_rule_with_cleanup(def, bump()).await.is_err());
    assert!(mgr
        .delete_rule_with_cleanup("r_kv_fail", bump())
        .await
        .is_err());
    assert_eq!(
        cleanup_calls.load(Ordering::SeqCst),
        0,
        "source cleanup must not run when persistence fails"
    );
    assert_eq!(mgr.get_rule_status("r_kv_fail").unwrap().status, "running");
}
