use rekuiper_core::contracts::{
    ActionId, CheckpointEnvelope, CheckpointId, CheckpointMetadata, CheckpointStore,
    CompletionStatus, ContentChecksum, EffectDestination, EffectOutcome, EffectRecord, Generation,
    OperatorId, OutboxBoundary, PlanIdentity, RuleId, CHECKPOINT_FORMAT_VERSION,
};
use rekuiper_core::reliability::{
    deterministic_effect_id, OutboxCapacity, OutboxEntry, OutboxError, ReliableSqliteStore,
    SqliteDurabilityMode, SqliteOutbox,
};
use std::collections::BTreeMap;

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

#[tokio::test]
async fn transactional_outbox_deduplicates_enforces_capacity_and_preserves_action_ownership() {
    let store = ReliableSqliteStore::open(":memory:", SqliteDurabilityMode::Full)
        .await
        .expect("open memory reliable sqlite");
    let outbox = SqliteOutbox::new(
        &store,
        OutboxCapacity {
            max_items: 2,
            max_bytes: 32,
        },
    );

    let rule = RuleId::try_from("rule_outbox").unwrap();
    let op = OperatorId::try_from("op_1").unwrap();
    let act_a = ActionId::try_from("mqtt_a").unwrap();
    let act_b = ActionId::try_from("rest_b").unwrap();

    let id_a = deterministic_effect_id(&rule, Generation(1), &op, &act_a, 10, 0).unwrap();
    let id_b = deterministic_effect_id(&rule, Generation(1), &op, &act_b, 10, 0).unwrap();
    assert_ne!(id_a, id_b);

    let entry_a = OutboxEntry::new(
        EffectRecord {
            effect_id: id_a.clone(),
            rule_id: rule.clone(),
            generation: Generation(1),
            destination: EffectDestination {
                action_id: act_a.clone(),
                connection_id: None,
            },
            order_key: None,
            attempt: 0,
            terminal_outcome: None,
        },
        b"hello".to_vec(),
    );
    let entry_b = OutboxEntry::new(
        EffectRecord {
            effect_id: id_b.clone(),
            rule_id: rule.clone(),
            generation: Generation(1),
            destination: EffectDestination {
                action_id: act_b.clone(),
                connection_id: None,
            },
            order_key: None,
            attempt: 0,
            terminal_outcome: None,
        },
        b"world".to_vec(),
    );

    // 1. Crash/rollback before commit retains neither checkpoint nor outbox effects.
    store.arm_commit_failure();
    assert!(outbox
        .commit_state_and_enqueue(sample_cp("rule_outbox", 1, 1), vec![entry_a.clone()])
        .await
        .is_err());
    assert!(store.load_latest(&rule).await.unwrap().is_none());
    assert!(store.outbox_record(&id_a).await.unwrap().is_none());

    // 2. Commit succeeds, retaining both state and effects.
    let rep = outbox
        .commit_state_and_enqueue(
            sample_cp("rule_outbox", 1, 1),
            vec![entry_a.clone(), entry_b.clone()],
        )
        .await
        .expect("commit succeeds");
    assert_eq!(rep.inserted, 2);
    assert_eq!(rep.deduplicated, 0);

    // 3. Full capacity refuses new items.
    let id_c = deterministic_effect_id(&rule, Generation(1), &op, &act_a, 11, 0).unwrap();
    let entry_c = OutboxEntry::new(
        EffectRecord {
            effect_id: id_c,
            rule_id: rule.clone(),
            generation: Generation(1),
            destination: EffectDestination {
                action_id: act_a.clone(),
                connection_id: None,
            },
            order_key: None,
            attempt: 0,
            terminal_outcome: None,
        },
        b"extra".to_vec(),
    );
    let cap_err = outbox
        .commit_state_and_enqueue(sample_cp("rule_outbox", 1, 2), vec![entry_c])
        .await
        .expect_err("capacity exceeded must refuse");
    assert!(matches!(cap_err, OutboxError::CapacityExceeded { .. }));

    // 4. Claim is scoped by action_id (action reorder cannot steal ownership) and ack frees capacity.
    let claimed_b = outbox.claim_pending(&rule, &act_b, "w1", 10).await.unwrap();
    assert_eq!(claimed_b.len(), 1);
    assert_eq!(claimed_b[0].effect.effect_id, id_b);
    assert!(!claimed_b[0].claim_token.is_empty());
    assert_eq!(
        outbox
            .ack(&id_b, &claimed_b[0].claim_token, EffectOutcome::Delivered)
            .await
            .unwrap(),
        Some(CompletionStatus::Applied)
    );

    // 5. Repeated insertion of id_a is idempotent (deduplicated=1).
    let rep2 = outbox
        .commit_state_and_enqueue(sample_cp("rule_outbox", 1, 3), vec![entry_a])
        .await
        .expect("deduplicate succeeds");
    assert_eq!(rep2.inserted, 0);
    assert_eq!(rep2.deduplicated, 1);
}
