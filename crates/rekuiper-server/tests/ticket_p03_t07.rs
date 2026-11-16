use parking_lot::Mutex;
use rekuiper_connectors::kafka::{
    KafkaRawRecord, KafkaReliablePartitionReader, KafkaTombstonePolicy,
};
use rekuiper_core::contracts::{
    ContentChecksum, DeadlineCancellation, EffectOutcome, Generation, OperatorId, PartitionId,
    PlanIdentity, RuleId, SourceId, SourcePartition, TestClock,
};
use rekuiper_core::reliability::{ReliableSqliteStore, SqliteDurabilityMode};
use rekuiper_server::engine::retry::RetryPolicy;
use rekuiper_server::engine::runtime::reliable::{
    validate_reliable_single_source_plan, ReliableDeliveryGuarantee, ReliableRuntimeError,
    ReliableSingleSourceRunner, ReliableSinkAdapter,
};
use rekuiper_server::engine::state::AnalyticStateParticipant;
use rekuiper_sql::parser::Parser;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

struct RecordingSink {
    fail_next: AtomicBool,
    delivered: Mutex<Vec<Value>>,
}

impl ReliableSinkAdapter for RecordingSink {
    async fn deliver_batch(&self, records: &[Value]) -> EffectOutcome {
        if self.fail_next.swap(false, Ordering::SeqCst) {
            return EffectOutcome::Retryable {
                delay_hint_millis: None,
            };
        }
        self.delivered.lock().extend(records.iter().cloned());
        EffectOutcome::Delivered
    }
}

fn plan_identity(rule: &str, generation: u64) -> PlanIdentity {
    let hex: String = std::iter::repeat_n('b', 64).collect();
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
        source_id: SourceId::try_from("kafka_orders").unwrap(),
        partition_id: PartitionId::try_from("0").unwrap(),
    }
}

#[test]
fn unsupported_sql_shapes_are_rejected_visibly() {
    let join_stmt = Parser::new("SELECT a.id FROM s1 AS a INNER JOIN s2 AS b ON a.id = b.id")
        .parse_select()
        .unwrap();
    assert!(matches!(
        validate_reliable_single_source_plan(&join_stmt),
        Err(ReliableRuntimeError::UnsupportedShape(_))
    ));

    let union_stmt = Parser::new("SELECT id FROM s1 UNION SELECT id FROM s2")
        .parse_select()
        .unwrap();
    assert!(matches!(
        validate_reliable_single_source_plan(&union_stmt),
        Err(ReliableRuntimeError::UnsupportedShape(_))
    ));

    let sliding_stmt = Parser::new("SELECT id FROM s1 GROUP BY SLIDINGWINDOW(ss, 10)")
        .parse_select()
        .unwrap();
    assert!(matches!(
        validate_reliable_single_source_plan(&sliding_stmt),
        Err(ReliableRuntimeError::UnsupportedShape(_))
    ));

    let tumble_stmt = Parser::new("SELECT id FROM s1 GROUP BY TUMBLINGWINDOW(ss, 10)")
        .parse_select()
        .unwrap();
    assert_eq!(
        validate_reliable_single_source_plan(&tumble_stmt).unwrap(),
        ReliableDeliveryGuarantee::AtLeastOnce
    );
}

#[tokio::test]
async fn single_source_checkpoint_recovery_survives_failures_and_restores_before_intake() {
    let store = ReliableSqliteStore::open(":memory:", SqliteDurabilityMode::Full)
        .await
        .expect("sqlite");
    let identity = plan_identity("rule_single_kafka", 1);
    let partition = kafka_partition();
    let stmt = Parser::new("SELECT id, temp FROM kafka_orders WHERE temp > 10")
        .parse_select()
        .unwrap();
    let policy = RetryPolicy {
        initial_backoff: Duration::from_millis(5),
        max_backoff: Duration::from_millis(5),
        jitter_ratio: 0.0,
        max_attempts: Some(1),
        ..RetryPolicy::default()
    };
    let clock = TestClock::new(1_700_000_000_000);
    let cancel = DeadlineCancellation::default();

    let op_id = OperatorId::try_from("analytic_1").unwrap();
    let mut participant =
        AnalyticStateParticipant::new(op_id.clone(), identity.plan_fingerprint.clone());
    participant.record_value("last_seen", json!(0));

    let mut runner = ReliableSingleSourceRunner::new(
        identity.clone(),
        partition.clone(),
        stmt.clone(),
        policy.clone(),
    )
    .expect("valid plan");
    assert_eq!(
        runner.status().guarantee,
        ReliableDeliveryGuarantee::AtLeastOnce
    );

    let mut reader = KafkaReliablePartitionReader::new(
        partition.clone(),
        Generation(1),
        Some("orders".to_string()),
        0,
        KafkaTombstonePolicy::Fail,
    );
    let sink = RecordingSink {
        fail_next: AtomicBool::new(false),
        delivered: Mutex::new(Vec::new()),
    };

    // Offset 0 commits cleanly.
    participant.record_value("last_seen", json!(1));
    let m0 = reader.prepare(KafkaRawRecord {
        offset: 0,
        payload: Some(br#"{"id":1,"temp":25}"#.to_vec()),
    });
    let cp1 = runner
        .process_and_checkpoint(
            &mut reader,
            &m0,
            &[&participant],
            &sink,
            &store,
            &clock,
            &cancel,
        )
        .await
        .expect("cp1 commits");
    assert_eq!(cp1.0, 1);
    assert_eq!(reader.committed_next_offset(), 1);
    assert_eq!(runner.status().last_checkpoint_id, Some(cp1));
    assert_eq!(
        runner.status().checkpoint_age_ms(1_700_000_000_500),
        Some(500)
    );

    // Offset 1 fails during sink delivery -> checkpoint does NOT commit, offset stays 1.
    sink.fail_next.store(true, Ordering::SeqCst);
    let m1 = reader.prepare(KafkaRawRecord {
        offset: 1,
        payload: Some(br#"{"id":2,"temp":30}"#.to_vec()),
    });
    let err = runner
        .process_and_checkpoint(
            &mut reader,
            &m1,
            &[&participant],
            &sink,
            &store,
            &clock,
            &cancel,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, ReliableRuntimeError::Coordinator(_)));
    assert!(runner.status().last_failure.is_some());
    assert_eq!(reader.committed_next_offset(), 1);

    // Simulated restart: restore_before_intake recovers offset=1 and participant state.
    let mut restarted_participant =
        AnalyticStateParticipant::new(op_id, identity.plan_fingerprint.clone());
    let mut restarted_runner =
        ReliableSingleSourceRunner::new(identity, partition.clone(), stmt, policy)
            .expect("valid plan");
    let restored_offset = restarted_runner
        .restore_before_intake(&store, &mut [&mut restarted_participant])
        .await
        .expect("restore succeeds");
    assert_eq!(restored_offset, Some(1));
    assert_eq!(restarted_participant.get("last_seen"), Some(&json!(1)));

    // Replay from restored offset 1 succeeds and delivers id=2 without losing records.
    let mut restarted_reader = KafkaReliablePartitionReader::new(
        partition,
        Generation(1),
        Some("orders".to_string()),
        restored_offset.unwrap(),
        KafkaTombstonePolicy::Fail,
    );
    let replay_m1 = restarted_reader.prepare(KafkaRawRecord {
        offset: 1,
        payload: Some(br#"{"id":2,"temp":30}"#.to_vec()),
    });
    let cp2 = restarted_runner
        .process_and_checkpoint(
            &mut restarted_reader,
            &replay_m1,
            &[&restarted_participant],
            &sink,
            &store,
            &clock,
            &cancel,
        )
        .await
        .expect("cp2 commits");
    assert_eq!(cp2.0, 2);
    assert_eq!(restarted_reader.committed_next_offset(), 2);
    let delivered = sink.delivered.lock();
    assert_eq!(delivered.len(), 2);
    assert_eq!(delivered[0]["id"], json!(1));
    assert_eq!(delivered[1]["id"], json!(2));
}
