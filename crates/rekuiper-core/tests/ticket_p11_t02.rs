use rekuiper_core::catalog::{
    CatalogEdge, CatalogEntry, CatalogError, CatalogGeneration, CatalogObjectId, CatalogObjectKind,
    CatalogObjectRef, CatalogStore,
};
use rekuiper_core::contracts::Generation;
use serde_json::json;
use std::collections::BTreeMap;

fn entry(
    id: &str,
    kind: CatalogObjectKind,
    name: &str,
    version: u64,
) -> (CatalogObjectId, CatalogEntry) {
    let oid = CatalogObjectId::new(id).unwrap();
    let e = CatalogEntry::new(oid.clone(), kind, name, version, json!({ "name": name })).unwrap();
    (oid, e)
}

#[tokio::test]
async fn p11_t02_atomic_generations_validation_rename_and_rollback() {
    let store = CatalogStore::open(":memory:").await.unwrap();
    assert!(store.current_generation().is_none());

    // 1. Commit generation 1: stream + rule depending on stream.
    let (s_id, s_entry) = entry("obj_stream_1", CatalogObjectKind::Stream, "mqtt_in", 1);
    let (r_id, r_entry) = entry("obj_rule_1", CatalogObjectKind::Rule, "high_temp", 1);
    let mut entries1 = BTreeMap::new();
    entries1.insert(s_id.clone(), s_entry.clone());
    entries1.insert(r_id.clone(), r_entry.clone());
    let gen1 = CatalogGeneration {
        generation: Generation(1),
        created_wall_millis: 1000,
        entries: entries1,
        edges: vec![CatalogEdge {
            from: r_entry.object_ref(),
            to: s_entry.object_ref(),
        }],
    };
    let snap1 = store.commit_candidate(None, gen1).await.unwrap();
    assert_eq!(snap1.generation, Generation(1));

    // Concurrent reader holds snap1 while we attempt invalid & valid commits.
    let reader_snap = store.current_generation().unwrap();

    // 2. Missing dependency version must fail and leave prior catalog unchanged.
    let mut bad_entries = snap1.entries.clone();
    let (_, r_v2) = entry("obj_rule_1", CatalogObjectKind::Rule, "high_temp", 2);
    bad_entries.insert(r_id.clone(), r_v2.clone());
    let bad_missing = CatalogGeneration {
        generation: Generation(2),
        created_wall_millis: 2000,
        entries: bad_entries.clone(),
        edges: vec![CatalogEdge {
            from: r_v2.object_ref(),
            to: CatalogObjectRef {
                kind: CatalogObjectKind::Stream,
                id: s_id.clone(),
                version: 99,
            },
        }],
    };
    let err = store
        .commit_candidate(Some(Generation(1)), bad_missing)
        .await
        .expect_err("missing dependency version must fail");
    assert!(matches!(err, CatalogError::MissingDependency { .. }));
    assert_eq!(
        store.current_generation().unwrap().generation,
        Generation(1)
    );

    // 3. Dependency cycle must fail and leave prior catalog unchanged.
    let cycle_gen = CatalogGeneration {
        generation: Generation(2),
        created_wall_millis: 2000,
        entries: bad_entries,
        edges: vec![
            CatalogEdge {
                from: r_v2.object_ref(),
                to: s_entry.object_ref(),
            },
            CatalogEdge {
                from: s_entry.object_ref(),
                to: r_v2.object_ref(),
            },
        ],
    };
    let err = store
        .commit_candidate(Some(Generation(1)), cycle_gen)
        .await
        .expect_err("dependency cycle must fail");
    assert!(matches!(err, CatalogError::DependencyCycle(_)));
    assert_eq!(
        store.current_generation().unwrap().generation,
        Generation(1)
    );

    // 4. Rename preserves CatalogObjectId in generation 2.
    let (_, r_renamed) = entry(
        "obj_rule_1",
        CatalogObjectKind::Rule,
        "high_temp_renamed",
        2,
    );
    let mut entries2 = snap1.entries.clone();
    entries2.insert(r_id.clone(), r_renamed.clone());
    let gen2 = CatalogGeneration {
        generation: Generation(2),
        created_wall_millis: 3000,
        entries: entries2,
        edges: vec![CatalogEdge {
            from: r_renamed.object_ref(),
            to: s_entry.object_ref(),
        }],
    };
    let snap2 = store
        .commit_candidate(Some(Generation(1)), gen2)
        .await
        .unwrap();
    assert_eq!(snap2.generation, Generation(2));
    assert_eq!(
        snap2
            .by_display_name(CatalogObjectKind::Rule, "high_temp_renamed")
            .unwrap()
            .id,
        r_id
    );
    // Reader holding snap1 still observes immutable generation 1.
    assert_eq!(
        reader_snap
            .by_display_name(CatalogObjectKind::Rule, "high_temp")
            .unwrap()
            .id,
        r_id
    );

    // 5. Old generation is readable and rollback creates generation 3 matching generation 1.
    let loaded1 = store.load_generation(Generation(1)).await.unwrap();
    assert_eq!(loaded1.generation, Generation(1));
    let snap3 = store
        .rollback_to(Generation(1), Generation(3), 4000)
        .await
        .unwrap();
    assert_eq!(snap3.generation, Generation(3));
    assert!(snap3
        .by_display_name(CatalogObjectKind::Rule, "high_temp")
        .is_some());
}

#[tokio::test]
async fn p11_t02_rejects_generations_above_sqlite_i64_max() {
    let dir = std::env::temp_dir().join(format!(
        "rekuiper_p11_t02_{}_{}",
        std::process::id(),
        i64::try_from(jiff::Timestamp::now().as_nanosecond()).unwrap_or(0)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let db_path = dir.join("catalog.db");
    let db_str = db_path.to_str().unwrap();
    let store = CatalogStore::open(db_str).await.unwrap();

    let gen1 = CatalogGeneration {
        generation: Generation(1),
        created_wall_millis: 1000,
        entries: BTreeMap::new(),
        edges: vec![],
    };
    store.commit_candidate(None, gen1).await.unwrap();

    let overflow_gen = CatalogGeneration {
        generation: Generation(u64::MAX),
        created_wall_millis: 2000,
        entries: BTreeMap::new(),
        edges: vec![],
    };
    let err = store
        .commit_candidate(Some(Generation(1)), overflow_gen)
        .await
        .expect_err("u64::MAX generation must be rejected before SQLite cast");
    assert!(matches!(err, CatalogError::InvalidObject(_)));

    let load_err = store
        .load_generation(Generation(u64::MAX))
        .await
        .expect_err("loading out-of-range generation must fail");
    assert!(matches!(load_err, CatalogError::InvalidObject(_)));

    drop(store);
    let reopened = CatalogStore::open(db_str).await.unwrap();
    assert_eq!(
        reopened.current_generation().map(|g| g.generation),
        Some(Generation(1))
    );
    drop(reopened);
    let _ = std::fs::remove_dir_all(&dir);
}
