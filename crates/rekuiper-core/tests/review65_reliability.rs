use rekuiper_core::catalog::CatalogStore;
use rekuiper_core::contracts::{
    ActionId, CheckpointEnvelope, CheckpointId, CheckpointMetadata, ContentChecksum,
    EffectDestination, EffectOutcome, EffectRecord, Generation, OperatorId, OutboxBoundary,
    PlanIdentity, RuleId, CHECKPOINT_FORMAT_VERSION,
};
use rekuiper_core::reliability::{
    deterministic_effect_id, OutboxEntry, OutboxError, ReliableSqliteStore, SegmentedOutbox,
    SegmentedOutboxConfig, SqliteDurabilityMode,
};
use std::collections::BTreeMap;
use std::path::PathBuf;

fn temp_dir(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "rekuiper_review65_{name}_{}_{}",
        std::process::id(),
        nanos
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn checksum(ch: char) -> ContentChecksum {
    let hex: String = std::iter::repeat_n(ch, 64).collect();
    ContentChecksum::try_from(format!("sha256:{hex}")).unwrap()
}

fn sample_cp(rule: &str, gen: u64, cp_id: u64) -> CheckpointEnvelope {
    CheckpointEnvelope {
        metadata: CheckpointMetadata {
            format_version: CHECKPOINT_FORMAT_VERSION,
            checkpoint_id: CheckpointId(cp_id),
            identity: PlanIdentity {
                rule_id: RuleId::try_from(rule).unwrap(),
                generation: Generation(gen),
                plan_fingerprint: checksum('a'),
                schema_fingerprints: BTreeMap::new(),
                extension_digests: BTreeMap::new(),
            },
            created_wall_millis: 1_700_000_000_000,
            source_positions: Vec::new(),
            participants: BTreeMap::new(),
            outbox: OutboxBoundary::default(),
        },
        payloads: BTreeMap::new(),
    }
}

fn sample_effect(rule: &str, gen: u64, seq: u64) -> EffectRecord {
    let rule_id = RuleId::try_from(rule).unwrap();
    let op_id = OperatorId::try_from("sink_0").unwrap();
    let action_id = ActionId::try_from("index.0").unwrap();
    let effect_id =
        deterministic_effect_id(&rule_id, Generation(gen), &op_id, &action_id, seq, 0).unwrap();
    EffectRecord {
        effect_id,
        rule_id,
        generation: Generation(gen),
        destination: EffectDestination {
            action_id,
            connection_id: None,
        },
        order_key: None,
        attempt: 0,
        terminal_outcome: None,
    }
}

#[test]
fn deterministic_effect_id_is_generation_independent() {
    let rule = RuleId::try_from("rule_a").unwrap();
    let op = OperatorId::try_from("sink_0").unwrap();
    let act = ActionId::try_from("named.primary").unwrap();
    let id_g1 = deterministic_effect_id(&rule, Generation(1), &op, &act, 42, 0).unwrap();
    let id_g2 = deterministic_effect_id(&rule, Generation(2), &op, &act, 42, 0).unwrap();
    assert_eq!(id_g1, id_g2);
}

#[test]
fn segmented_outbox_fences_stale_generation_and_retains_settled_dedupe_across_compaction() {
    let dir = temp_dir("seg_outbox");
    let mut cfg = SegmentedOutboxConfig::new(&dir);
    cfg.sync_on_write = false;
    let mut outbox = SegmentedOutbox::open(cfg).unwrap();

    let rule_id = RuleId::try_from("r1").unwrap();
    let action_id = ActionId::try_from("index.0").unwrap();
    let eff = sample_effect("r1", 2, 1);
    let eid = eff.effect_id.clone();

    let report = outbox
        .commit_state_and_enqueue(sample_cp("r1", 2, 1), vec![OutboxEntry::new(eff, vec![1])])
        .unwrap();
    assert_eq!(report.inserted, 1);

    // Stale generation (1 < 2) must be rejected.
    let stale_eff = sample_effect("r1", 1, 2);
    let err = outbox
        .commit_state_and_enqueue(
            sample_cp("r1", 1, 2),
            vec![OutboxEntry::new(stale_eff, vec![2])],
        )
        .unwrap_err();
    assert!(matches!(err, OutboxError::Checkpoint(_)));

    // Claim, ack, and compact the settled entry.
    let claimed = outbox
        .claim_pending(&rule_id, &action_id, "w1", 10)
        .unwrap();
    assert_eq!(claimed.len(), 1);
    let token = claimed[0].claim_token.clone();
    outbox.ack(&eid, &token, EffectOutcome::Delivered).unwrap();
    let live_after = outbox.compact_with_crash(None).unwrap();
    assert_eq!(live_after, 0);

    // Re-enqueuing the settled effect after compaction must still deduplicate.
    let eff_dup = sample_effect("r1", 2, 1);
    let dup_report = outbox
        .commit_state_and_enqueue(
            sample_cp("r1", 2, 3),
            vec![OutboxEntry::new(eff_dup, vec![1])],
        )
        .unwrap();
    assert_eq!(dup_report.inserted, 0);
    assert_eq!(dup_report.deduplicated, 1);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn sqlite_commit_with_outbox_rejects_mismatched_effect_identity() {
    let store = ReliableSqliteStore::open(":memory:", SqliteDurabilityMode::Full)
        .await
        .unwrap();
    let cp = sample_cp("rule_x", 1, 1);
    let wrong_rule_eff = sample_effect("other_rule", 1, 1);
    let err = store
        .commit_with_outbox(cp.clone(), vec![wrong_rule_eff])
        .await
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("does not match checkpoint envelope"));

    let wrong_gen_eff = sample_effect("rule_x", 2, 2);
    let err2 = store
        .commit_with_outbox(cp, vec![wrong_gen_eff])
        .await
        .unwrap_err();
    assert!(err2
        .to_string()
        .contains("does not match checkpoint envelope"));
}

#[tokio::test]
async fn catalog_store_open_normalizes_backslashes() {
    let dir = temp_dir("catalog_bs");
    let db_path = dir.join("catalog.db");
    let with_backslashes = db_path.to_string_lossy().replace('/', "\\");
    let _store = CatalogStore::open(with_backslashes).await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
