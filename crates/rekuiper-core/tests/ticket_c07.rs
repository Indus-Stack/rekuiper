use rekuiper_core::contracts::{
    ActionId, CheckpointEnvelope, CheckpointError, CheckpointId, CheckpointMetadata,
    CheckpointStore, CompletionStatus, ContentChecksum, EffectDestination, EffectDispatch,
    EffectId, EffectOutcome, EffectRecord, EffectSink, ErrorCode, ErrorObject, ErrorStage,
    Generation, OrderedEnvelope, OutboxBoundary, PartitionId, PlanIdentity, RuleId, SourceId,
    StructuredError, CHECKPOINT_FORMAT_VERSION,
};
use rekuiper_core::testing::{
    FakeCheckpointOutboxStore, FakeEffectSink, FakeReplaySource, FakeSourceError,
};
use std::collections::BTreeMap;

fn checksum(ch: char) -> ContentChecksum {
    let hex: String = std::iter::repeat_n(ch, 64).collect();
    ContentChecksum::try_from(format!("sha256:{hex}")).unwrap()
}

fn make_checkpoint(rule: &str, gen: u64, cp_id: u64) -> CheckpointEnvelope {
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

fn make_effect(id: &str, rule: &str, gen: u64) -> EffectRecord {
    EffectRecord {
        effect_id: EffectId::try_from(id).unwrap(),
        rule_id: RuleId::try_from(rule).unwrap(),
        generation: Generation(gen),
        destination: EffectDestination {
            action_id: ActionId::try_from("sink_1").unwrap(),
            connection_id: None,
        },
        order_key: None,
        attempt: 0,
        terminal_outcome: None,
    }
}

#[tokio::test]
async fn failed_commit_changes_nothing_and_stale_generation_fails() {
    let store = FakeCheckpointOutboxStore::new();
    let cp_gen2 = make_checkpoint("rule_1", 2, 10);
    let eff1 = make_effect("eff_1", "rule_1", 2);

    // Arm rollback failpoint: neither checkpoint nor outbox changes.
    store.arm_failpoint("commit_rollback");
    let err = store
        .commit_with_outbox(cp_gen2.clone(), vec![eff1.clone()])
        .expect_err("armed failpoint must fail");
    assert!(matches!(err, CheckpointError::StorageFailure(_)));
    assert!(store
        .load_latest(&RuleId::try_from("rule_1").unwrap())
        .await
        .unwrap()
        .is_none());
    assert!(store
        .outbox_record(&EffectId::try_from("eff_1").unwrap())
        .is_none());

    // Succeed on retry without failpoint.
    store
        .commit_with_outbox(cp_gen2, vec![eff1])
        .expect("commit succeeds");
    assert!(store
        .load_latest(&RuleId::try_from("rule_1").unwrap())
        .await
        .unwrap()
        .is_some());

    // Stale generation (1 < 2) must fail.
    let stale_cp = make_checkpoint("rule_1", 1, 11);
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
}

#[tokio::test]
async fn programmable_sink_models_ambiguous_and_idempotent_completion() {
    let store = FakeCheckpointOutboxStore::new();
    let eff_id = EffectId::try_from("eff_ambig").unwrap();
    store
        .commit_with_outbox(
            make_checkpoint("rule_1", 1, 1),
            vec![make_effect("eff_ambig", "rule_1", 1)],
        )
        .unwrap();

    let sink = FakeEffectSink::new();
    let err = StructuredError {
        code: ErrorCode::Timeout,
        stage: ErrorStage::Action,
        object: ErrorObject::Action(ActionId::try_from("sink_1").unwrap()),
        retriable: true,
        position: None,
    };
    sink.push_outcome(eff_id.clone(), EffectOutcome::Ambiguous { error: err });
    sink.push_outcome(eff_id.clone(), EffectOutcome::Delivered);

    let dispatch = EffectDispatch {
        effect_id: eff_id.clone(),
        rule_id: RuleId::try_from("rule_1").unwrap(),
        generation: Generation(1),
        destination: EffectDestination {
            action_id: ActionId::try_from("sink_1").unwrap(),
            connection_id: None,
        },
        order_key: None,
        payload: b"{}".to_vec(),
    };

    let first = sink.deliver(&dispatch).await;
    assert!(matches!(first, EffectOutcome::Ambiguous { .. }));
    assert_eq!(
        store.record_effect_completion(&eff_id, first),
        Some(CompletionStatus::Applied)
    );
    assert!(store
        .outbox_record(&eff_id)
        .unwrap()
        .terminal_outcome
        .is_none());

    let second = sink.deliver(&dispatch).await;
    assert_eq!(second, EffectOutcome::Delivered);
    assert_eq!(
        store.record_effect_completion(&eff_id, second.clone()),
        Some(CompletionStatus::Applied)
    );
    // Duplicate completion is idempotent.
    assert_eq!(
        store.record_effect_completion(&eff_id, second),
        Some(CompletionStatus::DuplicateIgnored)
    );
    assert_eq!(sink.dispatches().len(), 2);
}

#[test]
fn source_restart_resumes_exact_position_and_respects_failpoints() {
    let mut source = FakeReplaySource::new(
        SourceId::try_from("kafka_in").unwrap(),
        PartitionId::try_from("p0").unwrap(),
        Generation(1),
        "seq_v1",
    );
    let p0 = source.push(10, OrderedEnvelope::Data("a"));
    let p1 = source.push(11, OrderedEnvelope::Data("b"));
    let _p2 = source.push(12, OrderedEnvelope::Data("c"));

    source.arm_failpoint("read_error");
    assert!(matches!(
        source.next_envelope(),
        Err(FakeSourceError::Failpoint(_))
    ));

    let (pos, env) = source.next_envelope().unwrap().unwrap();
    assert_eq!(pos, p0);
    assert_eq!(env, OrderedEnvelope::Data("a"));

    // Simulate restart from committed p1: next read must be p2 ("c").
    source.resume_after(&p1).unwrap();
    let (pos, env) = source.next_envelope().unwrap().unwrap();
    assert_eq!(env, OrderedEnvelope::Data("c"));
    assert_eq!(pos.cursor, 12_i64.to_be_bytes());
    assert!(source.next_envelope().unwrap().is_none());
}
