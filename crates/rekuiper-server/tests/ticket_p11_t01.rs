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
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
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
