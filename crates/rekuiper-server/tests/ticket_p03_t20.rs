//! Qualification harness for durable storage, single-source checkpoints,
//! outbox dispatch failpoints, and recovery failure injection (P03-T20).

use parking_lot::Mutex;
use rekuiper_connectors::kafka::{
    resolve_start_offset, KafkaRawRecord, KafkaReliableError, KafkaReliablePartitionReader,
    KafkaTombstonePolicy,
};
use rekuiper_core::contracts::{
    ActionId, CheckpointEnvelope, CheckpointId, CheckpointMetadata, ContentChecksum,
    DeadlineCancellation, EffectDestination, EffectOutcome, EffectRecord, ErrorCode, ErrorObject,
    ErrorStage, Generation, OperatorId, OutboxBoundary, PartitionId, PlanIdentity, RuleId,
    SourceId, SourcePartition, StateError, StateParticipant, StructuredError, TestClock,
    CHECKPOINT_FORMAT_VERSION,
};
use rekuiper_core::reliability::{
    deterministic_effect_id, GenerationFence, OutboxCapacity, OutboxEntry, ReliableSqliteStore,
    SqliteDurabilityMode, SqliteOutbox,
};
use rekuiper_server::engine::outbox_dispatch::{
    DispatchCoordinator, OutboxDispatcher, OutboxSinkAdapter, PermanentFailurePolicy,
};
use rekuiper_server::engine::retry::{AmbiguousPolicy, RetryPolicy};
use rekuiper_server::engine::runtime::reliable::{
    ReliableRuntimeError, ReliableSingleSourceRunner, ReliableSinkAdapter,
};
use rekuiper_server::engine::state::AnalyticStateParticipant;
use rekuiper_sql::parser::Parser;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QualificationClaimScope {
    ProcessCrashOnly,
    #[allow(dead_code)]
    HostPowerLossQualified,
}

/// Separates process-crash claims from host/power-loss claims and rejects
/// advertising unknown/unverified durability settings as qualified.
pub fn certify_durability_mode(
    mode: Option<SqliteDurabilityMode>,
    verified_journal_mode: &str,
    verified_synchronous: i64,
) -> Result<QualificationClaimScope, &'static str> {
    match mode {
        Some(SqliteDurabilityMode::Full) => {
            if verified_journal_mode.eq_ignore_ascii_case("wal") && verified_synchronous >= 2 {
                Ok(QualificationClaimScope::ProcessCrashOnly)
            } else {
                Err("SQLite PRAGMAs do not match durable WAL + FULL synchronous requirements")
            }
        }
        Some(SqliteDurabilityMode::Extra) => {
            if verified_journal_mode.eq_ignore_ascii_case("wal") && verified_synchronous >= 3 {
                Ok(QualificationClaimScope::ProcessCrashOnly)
            } else {
                Err("SQLite PRAGMAs do not match durable WAL + EXTRA synchronous requirements")
            }
        }
        None => Err("unknown durability mode cannot be advertised as crash-qualified"),
    }
}

struct FailpointSink {
    fail_before_deliver: AtomicBool,
    delivered: Mutex<Vec<Value>>,
}

impl FailpointSink {
    fn new(fail_before_deliver: bool) -> Self {
        Self {
            fail_before_deliver: AtomicBool::new(fail_before_deliver),
            delivered: Mutex::new(Vec::new()),
        }
    }
}

impl ReliableSinkAdapter for FailpointSink {
    async fn deliver_batch(&self, records: &[Value]) -> EffectOutcome {
        if self.fail_before_deliver.swap(false, Ordering::SeqCst) {
            return EffectOutcome::Retryable {
                delay_hint_millis: None,
            };
        }
        self.delivered.lock().extend(records.iter().cloned());
        EffectOutcome::Delivered
    }
}

struct OutboxFailpointSink {
    attempts: AtomicUsize,
    outcome_sequence: Vec<EffectOutcome>,
    delivered_keys: Mutex<Vec<String>>,
}

impl OutboxSinkAdapter for OutboxFailpointSink {
    async fn deliver(&self, _entry: &OutboxEntry, idempotency_key: &str) -> EffectOutcome {
        let idx = self.attempts.fetch_add(1, Ordering::SeqCst);
        let outcome = self
            .outcome_sequence
            .get(idx)
            .cloned()
            .unwrap_or(EffectOutcome::Delivered);
        if matches!(outcome, EffectOutcome::Delivered) {
            self.delivered_keys.lock().push(idempotency_key.to_string());
        }
        outcome
    }

    async fn send_to_dlq(&self, _entry: &OutboxEntry, _error: &StructuredError) {}
}

fn plan_identity(rule: &str, generation: u64) -> PlanIdentity {
    let hex: String = std::iter::repeat_n('c', 64).collect();
    PlanIdentity {
        rule_id: RuleId::try_from(rule).unwrap(),
        generation: Generation(generation),
        plan_fingerprint: ContentChecksum::try_from(format!("sha256:{hex}")).unwrap(),
        schema_fingerprints: BTreeMap::new(),
        extension_digests: BTreeMap::new(),
    }
}

fn kafka_partition() -> SourcePartition {
    SourcePartition {
        source_id: SourceId::try_from("kafka_qual").unwrap(),
        partition_id: PartitionId::try_from("0").unwrap(),
    }
}

#[tokio::test]
async fn failpoint_commit_disk_full_and_process_restart_preserve_input_and_duplicate_boundary() {
    let store = ReliableSqliteStore::open(":memory:", SqliteDurabilityMode::Full)
        .await
        .unwrap();
    let dur = store.verified_durability();
    let scope = certify_durability_mode(
        Some(SqliteDurabilityMode::Full),
        &dur.journal_mode,
        dur.synchronous,
    );
    // `:memory:` uses MEMORY journal mode, so it must NOT be advertised as WAL-qualified.
    assert!(scope.is_err());

    let stmt = Parser::new("SELECT v FROM kafka_qual")
        .parse_select()
        .unwrap();
    let op_id = OperatorId::try_from("analytic_qual").unwrap();
    let identity1 = plan_identity("rule_qual", 1);
    let partition = kafka_partition();
    let clock = TestClock::new(1_000);
    let cancel = DeadlineCancellation::default();
    let retry = RetryPolicy {
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(5),
        jitter_ratio: 0.0,
        max_attempts: Some(1),
        ..RetryPolicy::default()
    };

    // 1. Arm disk-full/I/O commit failure on first checkpoint
    store.arm_commit_failure();
    let sink1 = FailpointSink::new(false);
    let mut participant1 =
        AnalyticStateParticipant::new(op_id.clone(), identity1.plan_fingerprint.clone());
    participant1.record_value("last", json!(10));
    let mut runner1 = ReliableSingleSourceRunner::new(
        identity1.clone(),
        partition.clone(),
        stmt.clone(),
        retry.clone(),
    )
    .unwrap();
    let mut reader1 = KafkaReliablePartitionReader::new(
        partition.clone(),
        Generation(1),
        Some("qual_topic".to_string()),
        0,
        KafkaTombstonePolicy::Fail,
    );
    let m0 = reader1.prepare(KafkaRawRecord {
        offset: 0,
        payload: Some(br#"{"v": 10}"#.to_vec()),
    });
    let err = runner1
        .process_and_checkpoint(
            &mut reader1,
            &m0,
            &[&participant1],
            &sink1,
            &store,
            &clock,
            &cancel,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, ReliableRuntimeError::Coordinator(_)));
    // Commit failed -> committed offset remains 0 for deterministic replay
    assert_eq!(reader1.committed_next_offset(), 0);

    // 2. Restart with healthy commit and verify replay from offset 0 commits cleanly
    let identity2 = plan_identity("rule_qual", 2);
    let sink2 = FailpointSink::new(false);
    let mut participant2 = AnalyticStateParticipant::new(op_id, identity2.plan_fingerprint.clone());
    participant2.record_value("last", json!(10));
    let mut runner2 =
        ReliableSingleSourceRunner::new(identity2, partition.clone(), stmt, retry).unwrap();
    let mut reader2 = KafkaReliablePartitionReader::new(
        partition,
        Generation(2),
        Some("qual_topic".to_string()),
        0,
        KafkaTombstonePolicy::Fail,
    );
    let m0_replay = reader2.prepare(KafkaRawRecord {
        offset: 0,
        payload: Some(br#"{"v": 10}"#.to_vec()),
    });
    let cp1 = runner2
        .process_and_checkpoint(
            &mut reader2,
            &m0_replay,
            &[&participant2],
            &sink2,
            &store,
            &clock,
            &cancel,
        )
        .await
        .unwrap();
    assert_eq!(cp1.0, 1);
    assert_eq!(reader2.committed_next_offset(), 1);
    assert_eq!(sink2.delivered.lock().len(), 1);
}

#[tokio::test]
async fn failpoint_corrupt_snapshot_stale_generation_and_retention_loss_are_rejected() {
    let store = ReliableSqliteStore::open(":memory:", SqliteDurabilityMode::Full)
        .await
        .unwrap();
    let rule_id = RuleId::try_from("rule_fence").unwrap();

    // 1. Stale generation fencing
    let fence = GenerationFence::new(&store);
    let (g1, _) = fence.activate_next(&rule_id).await.unwrap();
    let (g2, _) = fence.activate_next(&rule_id).await.unwrap();
    assert!(fence.assert_active(&rule_id, g1).await.is_err());
    assert!(fence.assert_active(&rule_id, g2).await.is_ok());

    // 2. Corrupt snapshot rejected before mutating state
    let identity = plan_identity("rule_fence", 2);
    let op_id = OperatorId::try_from("op_corrupt").unwrap();
    let mut participant = AnalyticStateParticipant::new(op_id, identity.plan_fingerprint);
    let mut snap = participant.capture_snapshot().unwrap();
    snap.payload[0] ^= 0xFF;
    assert!(matches!(
        participant.restore_snapshot(&snap),
        Err(StateError::ChecksumMismatch(_) | StateError::CorruptSnapshot(_))
    ));

    // 3. Kafka retention loss rejected before intake
    assert_eq!(
        resolve_start_offset(50, 100, Some(10), "latest"),
        Err(KafkaReliableError::RetentionGap {
            requested_offset: 10,
            earliest_available: 50,
        })
    );

    // 4. File-backed WAL + FULL synchronous qualifies ProcessCrashOnly, whereas unknown is rejected
    let tmp_dir = std::env::temp_dir();
    let db_path = tmp_dir.join(format!("rekuiper_qual_{}.db", std::process::id()));
    let _ = std::fs::remove_file(&db_path);
    {
        let file_store = ReliableSqliteStore::open(&db_path, SqliteDurabilityMode::Full)
            .await
            .unwrap();
        let dur = file_store.verified_durability();
        let scope = certify_durability_mode(
            Some(SqliteDurabilityMode::Full),
            &dur.journal_mode,
            dur.synchronous,
        )
        .unwrap();
        assert_eq!(scope, QualificationClaimScope::ProcessCrashOnly);
        assert!(certify_durability_mode(None, &dur.journal_mode, dur.synchronous).is_err());
    }
    let _ = std::fs::remove_file(&db_path);
}

#[tokio::test]
async fn failpoint_outbox_ambiguous_and_retryable_effects_survive_restart() {
    let store = ReliableSqliteStore::open(":memory:", SqliteDurabilityMode::Full)
        .await
        .unwrap();
    let outbox = SqliteOutbox::new(&store, OutboxCapacity::default());
    let rule_id = RuleId::try_from("rule_outbox_qual").unwrap();
    let op_id = OperatorId::try_from("op_qual").unwrap();
    let action_id = ActionId::try_from("http_sink").unwrap();
    let identity = plan_identity("rule_outbox_qual", 1);

    let eff_id =
        deterministic_effect_id(&rule_id, Generation(1), &op_id, &action_id, 1, 0).unwrap();
    let entry = OutboxEntry::new(
        EffectRecord {
            effect_id: eff_id,
            rule_id: rule_id.clone(),
            generation: Generation(1),
            destination: EffectDestination {
                action_id: action_id.clone(),
                connection_id: None,
            },
            order_key: Some("k1".to_string()),
            attempt: 0,
            terminal_outcome: None,
        },
        br#"{"msg":"hello"}"#.to_vec(),
    );
    let envelope = CheckpointEnvelope {
        metadata: CheckpointMetadata {
            format_version: CHECKPOINT_FORMAT_VERSION,
            checkpoint_id: CheckpointId(1),
            identity,
            created_wall_millis: 1_700_000_000_000,
            source_positions: Vec::new(),
            participants: BTreeMap::new(),
            outbox: OutboxBoundary::default(),
        },
        payloads: BTreeMap::new(),
    };
    outbox
        .commit_state_and_enqueue(envelope, vec![entry])
        .await
        .unwrap();

    let coordinator = DispatchCoordinator::new();
    let policy = RetryPolicy {
        initial_backoff: Duration::from_millis(5),
        max_backoff: Duration::from_millis(5),
        jitter_ratio: 0.0,
        max_attempts: Some(1),
        ambiguous_policy: AmbiguousPolicy::SurfaceAmbiguous,
        ..RetryPolicy::default()
    };
    let clock = TestClock::new(1_700_000_000_000);
    let cancel = DeadlineCancellation::default();

    let dispatcher1 = OutboxDispatcher::new(
        "worker-qual-1",
        coordinator.clone(),
        policy.clone(),
        PermanentFailurePolicy::TerminalAndDlq,
    );
    let sink_ambig = OutboxFailpointSink {
        attempts: AtomicUsize::new(0),
        outcome_sequence: vec![EffectOutcome::Ambiguous {
            error: StructuredError {
                code: ErrorCode::InvalidData,
                stage: ErrorStage::Action,
                object: ErrorObject::Action(action_id.clone()),
                retriable: true,
                position: None,
            },
        }],
        delivered_keys: Mutex::new(Vec::new()),
    };
    let report1 = dispatcher1
        .dispatch_once(
            &outbox,
            &rule_id,
            &action_id,
            10,
            &sink_ambig,
            &clock,
            &cancel,
            None,
        )
        .await
        .unwrap();
    assert_eq!(report1.unsettled_ambiguous, 1);
    assert_eq!(report1.delivered, 0);

    // Restarted dispatcher resends the unsettled ambiguous effect and delivers it
    let dispatcher2 = OutboxDispatcher::new(
        "worker-qual-2",
        coordinator,
        policy,
        PermanentFailurePolicy::TerminalAndDlq,
    );
    let sink_ok = OutboxFailpointSink {
        attempts: AtomicUsize::new(0),
        outcome_sequence: vec![EffectOutcome::Delivered],
        delivered_keys: Mutex::new(Vec::new()),
    };
    let report2 = dispatcher2
        .dispatch_once(
            &outbox, &rule_id, &action_id, 10, &sink_ok, &clock, &cancel, None,
        )
        .await
        .unwrap();
    assert_eq!(report2.delivered, 1);
    assert_eq!(sink_ok.delivered_keys.lock().len(), 1);
}
