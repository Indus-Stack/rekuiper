use rekuiper_core::contracts::{
    ActionId, CheckpointEnvelope, CheckpointError, CheckpointId, CheckpointMetadata,
    CheckpointStore, ContentChecksum, EffectDestination, EffectOutcome, EffectRecord, ErrorCode,
    ErrorObject, ErrorStage, Generation, OperatorId, OutboxBoundary, PlanIdentity, RuleId,
    StructuredError, CHECKPOINT_FORMAT_VERSION,
};
use rekuiper_core::reliability::{
    deterministic_effect_id, GenerationFence, OutboxCapacity, OutboxEntry, OutboxError,
    ReliableSqliteStore, SqliteDurabilityMode, SqliteOutbox,
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
async fn revoked_generation_blocks_late_writers_and_stale_claims_while_preserving_durable_effects()
{
    let store = ReliableSqliteStore::open(":memory:", SqliteDurabilityMode::Full)
        .await
        .expect("open memory reliable sqlite");
    let fence = GenerationFence::new(&store);
    let outbox = SqliteOutbox::new(&store, OutboxCapacity::default());

    let rule = RuleId::try_from("rule_fence").unwrap();
    let op = OperatorId::try_from("op_1").unwrap();
    let act = ActionId::try_from("rest_sink").unwrap();

    // Activate generation 1 and commit a checkpoint + durable outbox effect.
    let (gen1, cancel1) = fence.activate_next(&rule).await.unwrap();
    assert_eq!(gen1, Generation(1));

    let eff_id = deterministic_effect_id(&rule, gen1, &op, &act, 1, 0).unwrap();
    let ambig_err = StructuredError {
        code: ErrorCode::Timeout,
        stage: ErrorStage::Action,
        object: ErrorObject::Action(act.clone()),
        retriable: true,
        position: None,
    };
    let entry = OutboxEntry::new(
        EffectRecord {
            effect_id: eff_id.clone(),
            rule_id: rule.clone(),
            generation: gen1,
            destination: EffectDestination {
                action_id: act.clone(),
                connection_id: None,
            },
            order_key: None,
            attempt: 1,
            terminal_outcome: Some(EffectOutcome::Ambiguous { error: ambig_err }),
        },
        b"{\"temp\":99}".to_vec(),
    );
    outbox
        .commit_state_and_enqueue(sample_cp("rule_fence", 1, 10), vec![entry])
        .await
        .unwrap();

    // Stop/delete/update revokes generation 1 and cancels its runtime token.
    let revoked_gen = fence.fence_and_revoke(&rule, &cancel1).await.unwrap();
    assert_eq!(revoked_gen, Generation(1));

    // Late snapshot writer from generation 1 cannot recreate state.
    let late_write = store
        .commit_checkpoint(sample_cp("rule_fence", 1, 11))
        .await;
    assert!(matches!(
        late_write,
        Err(CheckpointError::GenerationMismatch { .. })
    ));

    // Stale dispatcher from generation 1 cannot claim effects.
    let stale_claim = fence
        .claim_pending_fenced(&outbox, &rule, &act, gen1, "stale_w", 10)
        .await;
    assert!(matches!(
        stale_claim,
        Err(OutboxError::Checkpoint(
            CheckpointError::GenerationMismatch { .. }
        ))
    ));

    // Cancellation/revocation did NOT discard the already-durable ambiguous effect.
    let unsettled = fence.list_unsettled_effects(&rule).await.unwrap();
    assert_eq!(unsettled.len(), 1);
    assert_eq!(unsettled[0].effect_id, eff_id);

    // Activating generation 2 succeeds and rejects generation 1.
    let (gen2, _cancel2) = fence.activate_next(&rule).await.unwrap();
    assert_eq!(gen2, Generation(2));
    assert!(fence.assert_active(&rule, gen2).await.is_ok());
    assert!(fence.assert_active(&rule, gen1).await.is_err());
}
