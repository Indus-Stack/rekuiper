use rekuiper_core::contracts::{
    ActionId, CheckpointEnvelope, CheckpointError, CheckpointId, CheckpointMetadata,
    CompletionStatus, ConnectionId, ContentChecksum, EffectDeliveryBoundary, EffectDestination,
    EffectId, EffectOutcome, EffectRecord, ErrorCode, ErrorObject, ErrorStage, Generation,
    OperatorId, OutboxBoundary, ParticipantSnapshotRef, PartitionId, PlanIdentity, PositionCodecId,
    RuleId, SourceId, SourcePartition, SourcePosition, StateError, StateResourceUsage,
    StateSchemaVersion, StateSnapshot, StructuredError, CHECKPOINT_FORMAT_VERSION,
    STATE_ENVELOPE_VERSION,
};
use std::collections::{BTreeMap, BTreeSet};

fn sample_checksum(ch: char) -> ContentChecksum {
    let hex: String = std::iter::repeat_n(ch, 64).collect();
    ContentChecksum::try_from(format!("sha256:{hex}")).expect("valid sha256 checksum")
}

fn sample_identity() -> PlanIdentity {
    PlanIdentity {
        rule_id: RuleId::try_from("rule_alpha").unwrap(),
        generation: Generation(7),
        plan_fingerprint: sample_checksum('a'),
        schema_fingerprints: BTreeMap::from([("orders".to_owned(), sample_checksum('b'))]),
        extension_digests: BTreeMap::new(),
    }
}

fn sample_checkpoint() -> (CheckpointEnvelope, BTreeSet<OperatorId>) {
    let op_id = OperatorId::try_from("win_tumbling_1").unwrap();
    let checksum = sample_checksum('c');
    let payload = b"state-bytes".to_vec();
    let usage = StateResourceUsage {
        records: 3,
        bytes: payload.len() as u64,
    };

    let snapshot = StateSnapshot {
        envelope_version: STATE_ENVELOPE_VERSION,
        operator_id: op_id.clone(),
        state_version: StateSchemaVersion(1),
        checksum: checksum.clone(),
        payload: payload.clone(),
        usage,
    };

    let part_ref = ParticipantSnapshotRef {
        operator_id: op_id.clone(),
        state_version: StateSchemaVersion(1),
        payload_checksum: checksum,
        payload_bytes: payload.len() as u64,
        usage,
    };

    let pos = SourcePosition {
        partition: SourcePartition {
            source_id: SourceId::try_from("kafka_orders").unwrap(),
            partition_id: PartitionId::try_from("p0").unwrap(),
        },
        generation: Generation(7),
        codec: PositionCodecId::try_from("kafka_offset_v1").unwrap(),
        cursor: 42_i64.to_be_bytes().to_vec(),
    };

    let envelope = CheckpointEnvelope {
        metadata: CheckpointMetadata {
            format_version: CHECKPOINT_FORMAT_VERSION,
            checkpoint_id: CheckpointId(12),
            identity: sample_identity(),
            created_wall_millis: 1_700_000_000_000,
            source_positions: vec![pos],
            participants: BTreeMap::from([(op_id.clone(), part_ref)]),
            outbox: OutboxBoundary {
                last_effect_id: Some(EffectId::try_from("eff:12:0").unwrap()),
                pending_effect_count: 1,
            },
        },
        payloads: BTreeMap::from([(op_id.clone(), snapshot)]),
    };

    (envelope, BTreeSet::from([op_id]))
}

#[test]
fn valid_checkpoint_round_trips_and_validates_restore() {
    let (envelope, expected_ops) = sample_checkpoint();
    let json = serde_json::to_string(&envelope).expect("serialize checkpoint");
    let decoded: CheckpointEnvelope = serde_json::from_str(&json).expect("deserialize checkpoint");
    assert_eq!(decoded, envelope);
    decoded
        .validate_restore(&sample_identity(), &expected_ops)
        .expect("valid restore");
}

#[test]
fn rejects_malformed_checksums_at_deserialization_and_construction() {
    assert!(ContentChecksum::try_from("md5:abcd").is_err());
    assert!(ContentChecksum::try_from("sha256:ABCD".to_owned() + &"0".repeat(60)).is_err());
    assert!(ContentChecksum::try_from("sha256:0000").is_err());

    let (envelope, _) = sample_checkpoint();
    let bad_json = serde_json::to_string(&envelope)
        .unwrap()
        .replace(&"a".repeat(64), "not_hex");
    assert!(serde_json::from_str::<CheckpointEnvelope>(&bad_json).is_err());
}

#[test]
fn rejects_unknown_versions_and_wrong_plan_or_generation() {
    let (mut envelope, expected_ops) = sample_checkpoint();
    let expected_identity = sample_identity();

    envelope.metadata.format_version = 99;
    assert_eq!(
        envelope.validate_restore(&expected_identity, &expected_ops),
        Err(CheckpointError::UnsupportedFormatVersion(99))
    );

    let (mut envelope, expected_ops) = sample_checkpoint();
    envelope.metadata.identity.generation = Generation(6);
    assert_eq!(
        envelope.validate_restore(&expected_identity, &expected_ops),
        Err(CheckpointError::GenerationMismatch {
            expected: Generation(7),
            actual: Generation(6),
        })
    );

    let (mut envelope, expected_ops) = sample_checkpoint();
    envelope.metadata.identity.plan_fingerprint = sample_checksum('f');
    assert_eq!(
        envelope.validate_restore(&expected_identity, &expected_ops),
        Err(CheckpointError::PlanFingerprintMismatch)
    );
}

#[test]
fn rejects_missing_participants_and_checksum_mismatches() {
    let (envelope, mut expected_ops) = sample_checkpoint();
    let expected_identity = sample_identity();

    let missing_op = OperatorId::try_from("agg_2").unwrap();
    expected_ops.insert(missing_op.clone());
    assert_eq!(
        envelope.validate_restore(&expected_identity, &expected_ops),
        Err(CheckpointError::MissingParticipant(missing_op))
    );

    let (mut envelope, expected_ops) = sample_checkpoint();
    let op_id = expected_ops.iter().next().unwrap().clone();
    envelope.payloads.get_mut(&op_id).unwrap().checksum = sample_checksum('d');
    assert_eq!(
        envelope.validate_restore(&expected_identity, &expected_ops),
        Err(CheckpointError::ChecksumMismatch(op_id))
    );
}

#[test]
fn state_snapshot_rejects_unknown_versions_and_wrong_operator() {
    let (envelope, expected_ops) = sample_checkpoint();
    let op_id = expected_ops.iter().next().unwrap();
    let mut snapshot = envelope.payloads.get(op_id).unwrap().clone();

    snapshot
        .validate(op_id, &[StateSchemaVersion(1)])
        .expect("valid snapshot");

    snapshot.envelope_version = 2;
    assert_eq!(
        snapshot.validate(op_id, &[StateSchemaVersion(1)]),
        Err(StateError::UnsupportedEnvelopeVersion(2))
    );

    snapshot.envelope_version = STATE_ENVELOPE_VERSION;
    assert_eq!(
        snapshot.validate(op_id, &[StateSchemaVersion(2)]),
        Err(StateError::UnsupportedStateVersion {
            operator_id: op_id.clone(),
            version: StateSchemaVersion(1),
        })
    );
}

#[test]
fn effect_completion_is_idempotent_and_distinguishes_boundaries() {
    assert!(EffectDeliveryBoundary::DurableAccepted < EffectDeliveryBoundary::Delivered);

    let mut record = EffectRecord {
        effect_id: EffectId::try_from("eff_1").unwrap(),
        rule_id: RuleId::try_from("rule_alpha").unwrap(),
        generation: Generation(7),
        destination: EffectDestination {
            action_id: ActionId::try_from("rest_sink_1").unwrap(),
            connection_id: Some(ConnectionId::try_from("http/prod").unwrap()),
        },
        order_key: Some("k1".to_owned()),
        attempt: 0,
        terminal_outcome: None,
    };

    // Retryable and Ambiguous outcomes do not finalize the record.
    assert_eq!(
        record.record_completion(EffectOutcome::Retryable {
            delay_hint_millis: Some(500),
        }),
        CompletionStatus::Applied
    );
    assert_eq!(record.attempt, 1);
    assert!(record.terminal_outcome.is_none());

    let err = StructuredError {
        code: ErrorCode::Timeout,
        stage: ErrorStage::Action,
        object: ErrorObject::Action(ActionId::try_from("rest_sink_1").unwrap()),
        retriable: true,
        position: None,
    };
    assert_eq!(
        record.record_completion(EffectOutcome::Ambiguous { error: err.clone() }),
        CompletionStatus::Applied
    );
    assert_eq!(record.attempt, 2);
    assert!(record.terminal_outcome.is_none());

    // First terminal outcome locks in; repeated completion cannot produce a second terminal outcome.
    assert_eq!(
        record.record_completion(EffectOutcome::Delivered),
        CompletionStatus::Applied
    );
    assert_eq!(record.attempt, 3);
    assert_eq!(record.terminal_outcome, Some(EffectOutcome::Delivered));

    assert_eq!(
        record.record_completion(EffectOutcome::PermanentFailure { error: err }),
        CompletionStatus::DuplicateIgnored
    );
    assert_eq!(record.attempt, 3);
    assert_eq!(record.terminal_outcome, Some(EffectOutcome::Delivered));
}
