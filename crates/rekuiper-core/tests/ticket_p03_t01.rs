use rekuiper_core::contracts::{
    ActionId, CheckpointEnvelope, CheckpointError, CheckpointId, CheckpointMetadata,
    CheckpointStore, CompletionStatus, ContentChecksum, EffectDestination, EffectId, EffectOutcome,
    EffectRecord, Generation, OperatorId, OutboxBoundary, ParticipantSnapshotRef, PartitionId,
    PlanIdentity, PositionCodecId, RuleId, SourceId, SourcePartition, SourcePosition,
    StateResourceUsage, StateSchemaVersion, StateSnapshot, CHECKPOINT_FORMAT_VERSION,
    STATE_ENVELOPE_VERSION,
};
use rekuiper_core::reliability::{ReliableSqliteStore, SqliteDurabilityMode};
use std::collections::{BTreeMap, BTreeSet};

fn checksum(ch: char) -> ContentChecksum {
    let hex: String = std::iter::repeat_n(ch, 64).collect();
    ContentChecksum::try_from(format!("sha256:{hex}")).unwrap()
}

fn sample_envelope(
    rule: &str,
    gen: u64,
    cp_id: u64,
    offset: i64,
) -> (CheckpointEnvelope, PlanIdentity, BTreeSet<OperatorId>) {
    let op_id = OperatorId::try_from("win_1").unwrap();
    let payload = format!("state-offset-{offset}").into_bytes();
    let snap_sum = checksum('c');
    let usage = StateResourceUsage {
        records: 2,
        bytes: payload.len() as u64,
    };

    let identity = PlanIdentity {
        rule_id: RuleId::try_from(rule).unwrap(),
        generation: Generation(gen),
        plan_fingerprint: checksum('a'),
        schema_fingerprints: BTreeMap::new(),
        extension_digests: BTreeMap::new(),
    };

    let pos = SourcePosition {
        partition: SourcePartition {
            source_id: SourceId::try_from("kafka_in").unwrap(),
            partition_id: PartitionId::try_from("p0").unwrap(),
        },
        generation: Generation(gen),
        codec: PositionCodecId::try_from("kafka_offset_v1").unwrap(),
        cursor: offset.to_be_bytes().to_vec(),
    };

    let envelope = CheckpointEnvelope {
        metadata: CheckpointMetadata {
            format_version: CHECKPOINT_FORMAT_VERSION,
            checkpoint_id: CheckpointId(cp_id),
            identity: identity.clone(),
            created_wall_millis: 1_700_000_000_000 + offset,
            source_positions: vec![pos],
            participants: BTreeMap::from([(
                op_id.clone(),
                ParticipantSnapshotRef {
                    operator_id: op_id.clone(),
                    state_version: StateSchemaVersion(1),
                    payload_checksum: snap_sum.clone(),
                    payload_bytes: payload.len() as u64,
                    usage,
                },
            )]),
            outbox: OutboxBoundary {
                last_effect_id: Some(EffectId::try_from("eff_1").unwrap()),
                pending_effect_count: 1,
            },
        },
        payloads: BTreeMap::from([(
            op_id.clone(),
            StateSnapshot {
                envelope_version: STATE_ENVELOPE_VERSION,
                operator_id: op_id.clone(),
                state_version: StateSchemaVersion(1),
                checksum: snap_sum,
                payload,
                usage,
            },
        )]),
    };

    (envelope, identity, BTreeSet::from([op_id]))
}

fn sample_effect(id: &str, rule: &str, gen: u64) -> EffectRecord {
    EffectRecord {
        effect_id: EffectId::try_from(id).unwrap(),
        rule_id: RuleId::try_from(rule).unwrap(),
        generation: Generation(gen),
        destination: EffectDestination {
            action_id: ActionId::try_from("mqtt_out").unwrap(),
            connection_id: None,
        },
        order_key: Some("k1".to_owned()),
        attempt: 0,
        terminal_outcome: None,
    }
}

#[tokio::test]
async fn file_sqlite_store_verifies_durability_rollback_fencing_and_reopen() {
    let dir = std::env::temp_dir().join(format!("rekuiper_p03_t01_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let db_path = dir.join("reliable.db");

    {
        let store = ReliableSqliteStore::open(&db_path, SqliteDurabilityMode::Full)
            .await
            .expect("open reliable sqlite");
        let dur = store.verified_durability();
        assert_eq!(dur.journal_mode.to_ascii_lowercase(), "wal");
        assert_eq!(dur.synchronous, 2);
        assert_eq!(dur.foreign_keys, 1);

        let (cp_gen2, identity, expected_ops) = sample_envelope("rule_a", 2, 10, 100);
        let eff = sample_effect("eff_1", "rule_a", 2);

        // Armed rollback leaves database completely unchanged.
        store.arm_commit_failure();
        let err = store
            .commit_with_outbox(cp_gen2.clone(), vec![eff.clone()])
            .await
            .expect_err("armed commit failure must roll back");
        assert!(matches!(err, CheckpointError::StorageFailure(_)));
        assert!(store
            .load_latest(&RuleId::try_from("rule_a").unwrap())
            .await
            .unwrap()
            .is_none());
        assert!(store
            .outbox_record(&EffectId::try_from("eff_1").unwrap())
            .await
            .unwrap()
            .is_none());

        // Real atomic commit succeeds.
        store
            .commit_with_outbox(cp_gen2.clone(), vec![eff])
            .await
            .expect("atomic commit succeeds");

        // Stale generation (1 < 2) fails.
        let (stale_cp, _, _) = sample_envelope("rule_a", 1, 11, 101);
        let err = store
            .commit_checkpoint(stale_cp)
            .await
            .expect_err("stale generation must fail");
        assert!(matches!(
            err,
            CheckpointError::GenerationMismatch {
                expected: Generation(2),
                actual: Generation(1)
            }
        ));

        // Idempotent effect completion.
        let eff_id = EffectId::try_from("eff_1").unwrap();
        assert_eq!(
            store
                .record_effect_completion(&eff_id, EffectOutcome::Delivered)
                .await
                .unwrap(),
            Some(CompletionStatus::Applied)
        );
        assert_eq!(
            store
                .record_effect_completion(&eff_id, EffectOutcome::Delivered)
                .await
                .unwrap(),
            Some(CompletionStatus::DuplicateIgnored)
        );

        let loaded = store
            .load_latest(&RuleId::try_from("rule_a").unwrap())
            .await
            .unwrap()
            .expect("checkpoint present");
        loaded
            .validate_restore(&identity, &expected_ops)
            .expect("valid restore");
        assert_eq!(loaded, cp_gen2);
    }

    // Reopen from disk and verify exact state and outbox effects survived.
    let reopened = ReliableSqliteStore::open(&db_path, SqliteDurabilityMode::Full)
        .await
        .expect("reopen reliable sqlite");
    let (expected_cp, identity, expected_ops) = sample_envelope("rule_a", 2, 10, 100);
    let loaded = reopened
        .load_latest(&RuleId::try_from("rule_a").unwrap())
        .await
        .unwrap()
        .expect("checkpoint persists across reopen");
    loaded
        .validate_restore(&identity, &expected_ops)
        .expect("reopened checkpoint validates");
    assert_eq!(loaded, expected_cp);

    let eff = reopened
        .outbox_record(&EffectId::try_from("eff_1").unwrap())
        .await
        .unwrap()
        .expect("outbox effect persists across reopen");
    assert_eq!(eff.terminal_outcome, Some(EffectOutcome::Delivered));
    assert_eq!(eff.attempt, 1);

    drop(reopened);
    let _ = std::fs::remove_dir_all(&dir);
}
