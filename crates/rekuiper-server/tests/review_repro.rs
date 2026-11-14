//! Reproductions and regression tests for PR 64 review findings.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use parking_lot::{Mutex, RwLock};
use rekuiper_conf::KuiperConfig;
use rekuiper_connectors::kafka::{
    resolve_start_offset, KafkaRawRecord, KafkaReliableError, KafkaReliablePartitionReader,
    KafkaTombstonePolicy,
};
use rekuiper_core::contracts::{
    ActionId, CheckpointEnvelope, CheckpointError, CheckpointId, CheckpointMetadata,
    CheckpointStore, ContentChecksum, DeadlineCancellation, EdgeId, EffectDestination, EffectId,
    EffectOutcome, EffectRecord, EventTimestamp, Generation, OperatorId, OrderedEnvelope,
    OutboxBoundary, PartitionId, PlanIdentity, RuleId, SourceId, SourcePartition, StateError,
    StateParticipant, StateResourceUsage, StateSchemaVersion, StateSnapshot, TestClock,
    CHECKPOINT_FORMAT_VERSION, STATE_ENVELOPE_VERSION,
};
use rekuiper_core::model::{
    EdgeTargets, GraphDefinition, GraphNode, GraphTopo, RuleStatus, StreamRecord,
};
use rekuiper_core::reliability::{
    GenerationFence, OutboxCapacity, OutboxEntry, OutboxError, ReliableSqliteStore,
    SqliteDurabilityMode, SqliteOutbox,
};
use rekuiper_core::resources::{BudgetLimits, ResourceBudget};
use rekuiper_core::{RuleCounters, RuleManager, StreamBus, StreamManager, TableManager};
use rekuiper_server::comms::CommsHub;
use rekuiper_server::engine::barrier_alignment::{
    AlignmentOutcome, BarrierAligner, BarrierAlignmentError,
};
use rekuiper_server::engine::operators::{
    RateLimitEnqueueOutcome, RateLimitNode, RateLimitOverflowPolicy,
};
use rekuiper_server::engine::outbox_dispatch::{
    DispatchCoordinator, OutboxDispatchError, OutboxDispatcher, OutboxSinkAdapter,
    PermanentFailurePolicy,
};
use rekuiper_server::engine::retry::RetryPolicy;
use rekuiper_server::engine::run_graph_rule;
use rekuiper_server::engine::runtime::reliable::{
    validate_participant_coverage, QualifiedPlanRunner, ReliableRuntimeError,
    ReliableSingleSourceRunner, ReliableSinkAdapter, RequiredParticipantKind,
};
use rekuiper_server::routes::create_router;
use rekuiper_server::state::AppState;
use rekuiper_sql::parser::Parser;
use rekuiper_sql::planning::{build_typed_plan_report, PlanCatalogInput};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tower::ServiceExt;

struct CountingSink {
    delivered: Mutex<Vec<Value>>,
}

impl ReliableSinkAdapter for CountingSink {
    async fn deliver_batch(&self, records: &[Value]) -> EffectOutcome {
        self.delivered.lock().extend(records.iter().cloned());
        EffectOutcome::Delivered
    }
}

struct NoopOutboxAdapter;

impl OutboxSinkAdapter for NoopOutboxAdapter {
    async fn deliver(&self, _entry: &OutboxEntry, _idempotency_key: &str) -> EffectOutcome {
        EffectOutcome::Delivered
    }
}

struct StubParticipant {
    id: OperatorId,
    tag: Option<&'static str>,
}

impl StateParticipant for StubParticipant {
    fn operator_id(&self) -> &OperatorId {
        &self.id
    }

    fn participant_kind_tag(&self) -> Option<&'static str> {
        self.tag
    }

    fn state_version(&self) -> StateSchemaVersion {
        StateSchemaVersion(1)
    }

    fn resource_usage(&self) -> StateResourceUsage {
        StateResourceUsage::default()
    }

    fn capture_snapshot(&self) -> Result<StateSnapshot, StateError> {
        let payload = b"{}".to_vec();
        Ok(StateSnapshot {
            envelope_version: STATE_ENVELOPE_VERSION,
            operator_id: self.id.clone(),
            state_version: StateSchemaVersion(1),
            checksum: ContentChecksum::sha256_bytes(&payload),
            payload,
            usage: StateResourceUsage::default(),
        })
    }

    fn restore_snapshot(&mut self, snapshot: &StateSnapshot) -> Result<(), StateError> {
        snapshot.validate(&self.id, &[StateSchemaVersion(1)])
    }
}

fn identity(rule: &str, gen: u64) -> PlanIdentity {
    let hex: String = std::iter::repeat_n('a', 64).collect();
    PlanIdentity {
        rule_id: RuleId::try_from(rule).unwrap(),
        generation: Generation(gen),
        plan_fingerprint: ContentChecksum::try_from(format!("sha256:{hex}")).unwrap(),
        schema_fingerprints: BTreeMap::new(),
        extension_digests: BTreeMap::new(),
    }
}

fn partition() -> SourcePartition {
    SourcePartition {
        source_id: SourceId::try_from("k").unwrap(),
        partition_id: PartitionId::try_from("0").unwrap(),
    }
}

fn envelope(rule: &str, cp: u64) -> CheckpointEnvelope {
    CheckpointEnvelope {
        metadata: CheckpointMetadata {
            format_version: CHECKPOINT_FORMAT_VERSION,
            checkpoint_id: CheckpointId(cp),
            identity: identity(rule, 1),
            created_wall_millis: 0,
            source_positions: Vec::new(),
            participants: BTreeMap::new(),
            outbox: OutboxBoundary::default(),
        },
        payloads: BTreeMap::new(),
    }
}

fn effect(rule: &str, id: &str) -> EffectRecord {
    EffectRecord {
        effect_id: EffectId::try_from(id).unwrap(),
        rule_id: RuleId::try_from(rule).unwrap(),
        generation: Generation(1),
        destination: EffectDestination {
            action_id: ActionId::try_from("mqtt_0").unwrap(),
            connection_id: None,
        },
        order_key: None,
        attempt: 0,
        terminal_outcome: None,
    }
}

fn build_test_state() -> AppState {
    let bus = StreamBus::new();
    let stream_mgr = StreamManager::new();
    let table_mgr = TableManager::new();
    let rule_mgr = RuleManager::new(bus.clone());
    AppState::new(
        "test-version".to_string(),
        KuiperConfig::default(),
        stream_mgr,
        table_mgr,
        rule_mgr,
        bus,
    )
}

/// records past 1024 in one multi-record Kafka message are dropped, yet the offset commits.
#[tokio::test]
async fn repro_runner_drops_records_beyond_1024_in_one_message() {
    let store = ReliableSqliteStore::open(":memory:", SqliteDurabilityMode::Full)
        .await
        .unwrap();
    let stmt = Parser::new("SELECT id FROM k").parse_select().unwrap();
    let policy = RetryPolicy {
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(1),
        jitter_ratio: 0.0,
        max_attempts: Some(1),
        ..RetryPolicy::default()
    };
    let mut runner =
        ReliableSingleSourceRunner::new(identity("r1", 1), partition(), stmt, policy).unwrap();
    let mut reader = KafkaReliablePartitionReader::new(
        partition(),
        Generation(1),
        Some("t".into()),
        0,
        KafkaTombstonePolicy::Fail,
    );
    let rows: Vec<Value> = (0..1500).map(|i| json!({ "id": i })).collect();
    let msg = reader.prepare(KafkaRawRecord {
        offset: 0,
        payload: Some(serde_json::to_vec(&rows).unwrap()),
    });
    let sink = CountingSink {
        delivered: Mutex::new(Vec::new()),
    };
    let clock = TestClock::new(0);
    let cancel = DeadlineCancellation::default();
    runner
        .process_and_checkpoint(&mut reader, &msg, &[], &sink, &store, &clock, &cancel)
        .await
        .unwrap();
    let delivered = sink.delivered.lock().len();
    println!(
        "REPRO delivered={delivered} of 1500, committed_next_offset={}",
        reader.committed_next_offset()
    );
    assert_eq!(delivered, 1500, "records lost while offset advanced");
}

/// commit_with_outbox uses INSERT OR REPLACE, so re-committing an effect wipes its Delivered outcome.
#[tokio::test]
async fn repro_recommit_resets_delivered_effect() {
    let store = ReliableSqliteStore::open(":memory:", SqliteDurabilityMode::Full)
        .await
        .unwrap();
    let e = effect("r2", "eff_1");
    store
        .commit_with_outbox(envelope("r2", 1), vec![e.clone()])
        .await
        .unwrap();
    store
        .record_effect_completion(&e.effect_id, EffectOutcome::Delivered)
        .await
        .unwrap();
    store
        .commit_with_outbox(envelope("r2", 2), vec![e.clone()])
        .await
        .unwrap();
    let rec = store.outbox_record(&e.effect_id).await.unwrap().unwrap();
    println!(
        "REPRO terminal_outcome after recommit = {:?}",
        rec.terminal_outcome
    );
    assert!(
        rec.terminal_outcome.is_some(),
        "Delivered outcome was erased"
    );
}

/// claim_pending ignores claimed_by, so two workers claim the same entry.
#[tokio::test]
async fn repro_two_workers_claim_same_entry() {
    let store = ReliableSqliteStore::open(":memory:", SqliteDurabilityMode::Full)
        .await
        .unwrap();
    let outbox = SqliteOutbox::new(&store, OutboxCapacity::default());
    outbox
        .commit_state_and_enqueue(
            envelope("r3", 1),
            vec![OutboxEntry {
                effect: effect("r3", "eff_x"),
                payload: b"p".to_vec(),
            }],
        )
        .await
        .unwrap();
    let rule = RuleId::try_from("r3").unwrap();
    let act = ActionId::try_from("mqtt_0").unwrap();
    let a = outbox
        .claim_pending(&rule, &act, "worker_a", 10)
        .await
        .unwrap();
    let b = outbox
        .claim_pending(&rule, &act, "worker_b", 10)
        .await
        .unwrap();
    println!("REPRO worker_a={} worker_b={}", a.len(), b.len());
    assert!(
        b.is_empty(),
        "entry already claimed by worker_a was claimed again"
    );
}

/// outbox capacity is global, so one rule's backlog blocks a different rule.
#[tokio::test]
async fn repro_capacity_is_global_across_rules() {
    let store = ReliableSqliteStore::open(":memory:", SqliteDurabilityMode::Full)
        .await
        .unwrap();
    let outbox = SqliteOutbox::new(
        &store,
        OutboxCapacity {
            max_items: 1,
            max_bytes: 1024,
        },
    );
    outbox
        .commit_state_and_enqueue(
            envelope("rule_a", 1),
            vec![OutboxEntry {
                effect: effect("rule_a", "eff_a"),
                payload: b"p".to_vec(),
            }],
        )
        .await
        .unwrap();
    let res = outbox
        .commit_state_and_enqueue(
            envelope("rule_b", 1),
            vec![OutboxEntry {
                effect: effect("rule_b", "eff_b"),
                payload: b"p".to_vec(),
            }],
        )
        .await;
    println!("REPRO rule_b enqueue = {res:?}");
    assert!(res.is_ok(), "rule_b blocked by rule_a backlog");
}

/// Kafka offsets are not contiguous (compaction, transaction markers); the reader rejects gaps.
#[test]
fn repro_kafka_offset_gap_blocks_progress() {
    let mut reader = KafkaReliablePartitionReader::new(
        partition(),
        Generation(1),
        None,
        0,
        KafkaTombstonePolicy::Fail,
    );
    let m0 = reader.prepare(KafkaRawRecord {
        offset: 0,
        payload: Some(br#"{"a":1}"#.to_vec()),
    });
    reader.commit_message(&m0).unwrap();
    let m2 = reader.prepare(KafkaRawRecord {
        offset: 2,
        payload: Some(br#"{"a":2}"#.to_vec()),
    });
    let res = reader.commit_message(&m2);
    println!("REPRO commit after gap = {res:?}");
    assert!(
        !matches!(res, Err(KafkaReliableError::OutOfOrderCommit { .. })),
        "offset gap blocks the partition"
    );
}

/// camelCase and header secrets are not redacted in validate/explain output.
#[test]
fn repro_secret_redaction_misses_common_keys() {
    let mut action = HashMap::new();
    action.insert(
        "rest".to_string(),
        json!({
            "url": "http://example.invalid",
            "headers": { "Authorization": "Bearer SECRET1" },
            "accessKey": "SECRET2",
            "privateKey": "SECRET3",
            "connectionString": "SECRET4"
        }),
    );
    let report = build_typed_plan_report(&PlanCatalogInput {
        rule_id: "r".into(),
        sql: "SELECT * FROM s".into(),
        options: None,
        actions: vec![action],
        streams: Vec::new(),
    });
    let out = serde_json::to_string(&report.redacted_actions).unwrap();
    let leaked: Vec<&str> = ["SECRET1", "SECRET2", "SECRET3", "SECRET4"]
        .into_iter()
        .filter(|s| out.contains(s))
        .collect();
    println!("REPRO leaked = {leaked:?}");
    assert!(leaked.is_empty(), "secrets leaked: {leaked:?}");
}

#[tokio::test]
async fn repro_same_worker_id_dispatch_coordinator_and_fenced_dispatch() {
    let store = ReliableSqliteStore::open(":memory:", SqliteDurabilityMode::Full)
        .await
        .unwrap();
    let fence = GenerationFence::new(&store);
    let rule = RuleId::try_from("r_coord").unwrap();
    let act = ActionId::try_from("mqtt_0").unwrap();
    let (gen1, _cancel) = fence.activate_next(&rule).await.unwrap();

    let coord = DispatchCoordinator::new();
    let lease1 = coord
        .try_acquire(&rule, &act, "same-worker")
        .expect("first lease must succeed");
    assert!(
        coord.try_acquire(&rule, &act, "same-worker").is_none(),
        "second caller with same worker_id must be rejected while first holds lease"
    );
    drop(lease1);

    let dispatcher = OutboxDispatcher::new(
        "same-worker",
        coord.clone(),
        RetryPolicy::default(),
        PermanentFailurePolicy::TerminalOnly,
    );
    let outbox = SqliteOutbox::new(&store, OutboxCapacity::default());
    outbox
        .commit_state_and_enqueue(
            envelope("r_coord", 1),
            vec![OutboxEntry {
                effect: effect("r_coord", "eff_coord_1"),
                payload: b"hello".to_vec(),
            }],
        )
        .await
        .unwrap();

    let clock = TestClock::new(0);
    let cancel = DeadlineCancellation::default();
    let rep = dispatcher
        .dispatch_once_fenced(
            &outbox,
            &rule,
            &act,
            gen1,
            10,
            &NoopOutboxAdapter,
            &clock,
            &cancel,
            None,
        )
        .await
        .unwrap();
    assert_eq!(rep.delivered, 1);

    fence.fence_and_revoke(&rule, &cancel).await.unwrap();
    let fenced_err = dispatcher
        .dispatch_once_fenced(
            &outbox,
            &rule,
            &act,
            gen1,
            10,
            &NoopOutboxAdapter,
            &clock,
            &cancel,
            None,
        )
        .await
        .expect_err("revoked generation must be fenced in dispatch transaction");
    assert!(matches!(
        fenced_err,
        OutboxDispatchError::Outbox(OutboxError::Checkpoint(
            CheckpointError::GenerationMismatch { .. }
        ))
    ));
}

#[tokio::test]
async fn repro_strict_generation_retention_and_corrupt_generation_errors() {
    let unique = format!(
        "rekuiper_review_repro_{}_{}.db",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
    );
    let db_path = std::env::temp_dir().join(unique);
    let _ = std::fs::remove_file(&db_path);

    {
        let store = ReliableSqliteStore::open(&db_path, SqliteDurabilityMode::Full)
            .await
            .unwrap()
            .with_retention(2, 1);
        let fence = GenerationFence::new(&store);
        let rule = RuleId::try_from("r_ret").unwrap();
        let (gen1, _) = fence.activate_next(&rule).await.unwrap();
        assert_eq!(gen1, Generation(1));

        // Unactivated higher generation (2) must not silently advance active fence.
        let mut future_env = envelope("r_ret", 1);
        future_env.metadata.identity.generation = Generation(2);
        let err = store
            .commit_with_outbox(future_env, Vec::new())
            .await
            .expect_err("unactivated future generation must be rejected");
        assert!(matches!(err, CheckpointError::GenerationMismatch { .. }));

        // Commit 4 checkpoints with 3 delivered effects + 1 unsettled effect.
        for cp in 1..=4u64 {
            let eff_id = format!("eff_ret_{cp}");
            let eff = effect("r_ret", &eff_id);
            store
                .commit_with_outbox(envelope("r_ret", cp), vec![eff.clone()])
                .await
                .unwrap();
            if cp <= 3 {
                store
                    .record_effect_completion(&eff.effect_id, EffectOutcome::Delivered)
                    .await
                    .unwrap();
            }
        }
        // Trigger one final commit so settled-outbox pruning runs after cp 3 was settled.
        let eff5 = effect("r_ret", "eff_ret_5");
        store
            .commit_with_outbox(envelope("r_ret", 5), vec![eff5.clone()])
            .await
            .unwrap();
        store
            .record_effect_completion(&eff5.effect_id, EffectOutcome::Delivered)
            .await
            .unwrap();
        store
            .commit_with_outbox(envelope("r_ret", 6), Vec::new())
            .await
            .unwrap();

        let db_url = format!("sqlite:{}", db_path.to_string_lossy().replace('\\', "/"));
        let raw_pool = sqlx::SqlitePool::connect(&db_url).await.unwrap();

        let (cp_count,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM reliable_checkpoints WHERE rule_id = ?")
                .bind("r_ret")
                .fetch_one(&raw_pool)
                .await
                .unwrap();
        assert_eq!(cp_count, 2, "checkpoint retention should keep latest 2");

        let (settled_count,): (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM reliable_outbox_effects WHERE rule_id = ? AND terminal_outcome_json IS NOT NULL",
        )
        .bind("r_ret")
        .fetch_one(&raw_pool)
        .await
        .unwrap();
        assert_eq!(settled_count, 1, "settled outbox retention should keep 1");

        let (unsettled_count,): (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM reliable_outbox_effects WHERE rule_id = ? AND effect_id = 'eff_ret_4' AND terminal_outcome_json IS NULL",
        )
        .bind("r_ret")
        .fetch_one(&raw_pool)
        .await
        .unwrap();
        assert_eq!(unsettled_count, 1, "unsettled effect must not be pruned");

        // Corrupt negative generation row must error instead of coercing to 0.
        sqlx::query("UPDATE reliable_generations SET generation = -5 WHERE rule_id = ?")
            .bind("r_ret")
            .execute(&raw_pool)
            .await
            .unwrap();

        let outbox = SqliteOutbox::new(&store, OutboxCapacity::default());
        let act = ActionId::try_from("mqtt_0").unwrap();
        let corrupt_claim = outbox
            .claim_pending_with_lease(&rule, &act, "worker", 10, 30_000, Some(gen1))
            .await;
        assert!(
            matches!(
                corrupt_claim,
                Err(OutboxError::Storage(_))
                    | Err(OutboxError::Checkpoint(CheckpointError::StorageFailure(_)))
            ),
            "corrupt generation row must fail closed: {corrupt_claim:?}"
        );
        raw_pool.close().await;
    }

    let _ = std::fs::remove_file(&db_path);
}

#[tokio::test]
async fn repro_runner_missing_participant_snapshot_errors() {
    let store = ReliableSqliteStore::open(":memory:", SqliteDurabilityMode::Full)
        .await
        .unwrap();
    store
        .commit_with_outbox(envelope("r_missing", 1), Vec::new())
        .await
        .unwrap();

    let stmt = Parser::new("SELECT id FROM k").parse_select().unwrap();
    let mut runner = ReliableSingleSourceRunner::new(
        identity("r_missing", 1),
        partition(),
        stmt.clone(),
        RetryPolicy::default(),
    )
    .unwrap();

    let op_id = OperatorId::try_from("analytic_op").unwrap();
    let mut stub = StubParticipant {
        id: op_id.clone(),
        tag: Some("analytic"),
    };

    let err = runner
        .restore_before_intake(&store, &mut [&mut stub])
        .await
        .expect_err("missing participant snapshot in envelope must error");
    assert!(matches!(
        err,
        ReliableRuntimeError::Checkpoint(CheckpointError::MissingParticipant(ref missing))
            if missing == &op_id
    ));

    let mut qrunner = QualifiedPlanRunner::new(
        identity("r_missing", 1),
        &stmt,
        false,
        1,
        BTreeSet::new(),
        &[&stub],
    )
    .unwrap();
    let qerr = qrunner
        .restore_before_intake(&store, &mut [&mut stub])
        .await
        .expect_err("QualifiedPlanRunner must also reject missing snapshot");
    assert!(matches!(
        qerr,
        ReliableRuntimeError::Checkpoint(CheckpointError::MissingParticipant(ref missing))
            if missing == &op_id
    ));
}

#[test]
fn repro_participant_kind_does_not_match_substrings() {
    let discount = StubParticipant {
        id: OperatorId::try_from("discount_filter").unwrap(),
        tag: None,
    };
    let estate = StubParticipant {
        id: OperatorId::try_from("real_estate").unwrap(),
        tag: None,
    };

    let err_count =
        validate_participant_coverage(&[RequiredParticipantKind::CountWindow], &[&discount])
            .expect_err("discount_filter must not satisfy CountWindow");
    assert!(matches!(
        err_count,
        ReliableRuntimeError::MissingParticipant("count")
    ));

    let err_state =
        validate_participant_coverage(&[RequiredParticipantKind::StateWindow], &[&estate])
            .expect_err("real_estate must not satisfy StateWindow");
    assert!(matches!(
        err_state,
        ReliableRuntimeError::MissingParticipant("state")
    ));
}

#[tokio::test]
async fn repro_runner_applies_count_window_semantics() {
    let store = ReliableSqliteStore::open(":memory:", SqliteDurabilityMode::Full)
        .await
        .unwrap();
    let stmt = Parser::new("SELECT count(*) AS cnt FROM k GROUP BY COUNTWINDOW(2)")
        .parse_select()
        .unwrap();
    let mut runner = ReliableSingleSourceRunner::new(
        identity("r_win", 1),
        partition(),
        stmt,
        RetryPolicy::default(),
    )
    .unwrap();
    let mut reader = KafkaReliablePartitionReader::new(
        partition(),
        Generation(1),
        None,
        0,
        KafkaTombstonePolicy::Fail,
    );
    let sink = CountingSink {
        delivered: Mutex::new(Vec::new()),
    };
    let clock = TestClock::new(0);
    let cancel = DeadlineCancellation::default();

    let m0 = reader.prepare(KafkaRawRecord {
        offset: 0,
        payload: Some(br#"{"v":10}"#.to_vec()),
    });
    runner
        .process_and_checkpoint(&mut reader, &m0, &[], &sink, &store, &clock, &cancel)
        .await
        .unwrap();
    assert_eq!(
        sink.delivered.lock().len(),
        0,
        "count window size 2 must buffer the first row instead of emitting per row"
    );

    let m1 = reader.prepare(KafkaRawRecord {
        offset: 1,
        payload: Some(br#"{"v":20}"#.to_vec()),
    });
    runner
        .process_and_checkpoint(&mut reader, &m1, &[], &sink, &store, &clock, &cancel)
        .await
        .unwrap();
    let delivered = sink.delivered.lock();
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0]["cnt"], json!(2));
}

#[tokio::test]
async fn repro_rate_limit_default_backpressure_and_graph_validation() {
    let mut props = HashMap::new();
    props.insert("rate".to_string(), json!("2"));
    props.insert("burst".to_string(), json!("1"));
    props.insert("strategy".to_string(), json!("buffer"));
    props.insert("max_buffer".to_string(), json!("1"));
    props.insert("max_buffer_bytes".to_string(), json!("4096"));

    let node = RateLimitNode::try_from_props(&props).expect("string-encoded numbers should parse");
    assert_eq!(
        node.config().overflow_policy,
        RateLimitOverflowPolicy::Backpressure,
        "buffer strategy without explicit overflow_policy must default to Backpressure"
    );
    let budget = node.default_budget();
    let mut buf = node.into_buffer(budget);
    assert!(matches!(
        buf.enqueue(1u64, 64),
        RateLimitEnqueueOutcome::AdmitNow(1)
    ));
    assert!(matches!(
        buf.enqueue(2u64, 64),
        RateLimitEnqueueOutcome::Buffered { evicted: false }
    ));
    assert!(matches!(
        buf.enqueue(3u64, 64),
        RateLimitEnqueueOutcome::BackpressureWait(3)
    ));

    // POST /rules must reject invalid rate_limit props with 400 Bad Request
    let app = create_router(build_test_state());
    let invalid_rule = json!({
        "id": "invalid_rl_rest",
        "sql": "",
        "actions": [{ "log": {} }],
        "graph": {
            "nodes": {
                "src": { "type": "source", "nodeType": "memory", "props": { "datasource": "s" } },
                "rl": { "type": "operator", "nodeType": "rate_limit", "props": { "rate": 0, "strategy": "buffer" } },
                "snk": { "type": "sink", "nodeType": "log", "props": {} }
            },
            "topo": {
                "sources": ["src"],
                "edges": { "src": ["rl"], "rl": ["snk"] }
            }
        }
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/rules")
                .header("content-type", "application/json")
                .body(Body::from(invalid_rule.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // Starting run_graph_rule with invalid rate_limit must halt visibly (stopped + last_exception)
    let mut nodes = HashMap::new();
    let mut rl_props = HashMap::new();
    rl_props.insert("rate".to_string(), json!(0));
    nodes.insert(
        "rl".to_string(),
        GraphNode {
            node_category: "operator".to_string(),
            node_type: "rate_limit".to_string(),
            props: rl_props,
        },
    );
    let graph = GraphDefinition {
        nodes,
        topo: GraphTopo::default(),
    };
    let bus = StreamBus::new();
    let rule_mgr = RuleManager::new(bus.clone());
    let counters = Arc::new(RuleCounters::default());
    let running = Arc::new(RwLock::new(RuleStatus::default()));
    run_graph_rule(
        "invalid_rl_run".to_string(),
        graph,
        bus,
        rule_mgr,
        counters,
        running.clone(),
        CommsHub::new(64),
        16,
    )
    .await;
    let st = running.read();
    assert_eq!(st.status, "stopped");
    assert!(!st.last_exception.is_empty());
}

#[test]
fn repro_barrier_aligner_buffers_watermarks_on_paused_edges() {
    const FAST: EdgeId = EdgeId(1);
    const SLOW: EdgeId = EdgeId(2);
    let budget = ResourceBudget::new(BudgetLimits {
        max_items: Some(8),
        max_logical_bytes: Some(4096),
        max_tasks: None,
    });
    let mut aligner =
        BarrierAligner::new(vec![FAST, SLOW], vec!["s1".to_string()], budget, 1_000).unwrap();

    assert!(matches!(
        aligner
            .push_envelope(
                FAST,
                OrderedEnvelope::Barrier {
                    checkpoint: CheckpointId(1),
                    generation: Generation(1),
                },
                0,
                100
            )
            .unwrap(),
        AlignmentOutcome::WaitingForBarriers { .. }
    ));

    aligner
        .push_envelope(FAST, OrderedEnvelope::Data("paused_d1"), 64, 110)
        .unwrap();
    let wm_outcome = aligner
        .push_envelope(
            FAST,
            OrderedEnvelope::Watermark {
                edge: FAST,
                timestamp: EventTimestamp::Millis(500),
            },
            0,
            120,
        )
        .unwrap();
    assert!(
        matches!(
            wm_outcome,
            AlignmentOutcome::BufferedPaused {
                edge: FAST,
                buffered_on_edge: 2
            }
        ),
        "watermark on paused edge must buffer after paused data, got {wm_outcome:?}"
    );
    assert_eq!(
        aligner.watermark(&FAST),
        None,
        "watermark must not advance ahead of buffered data"
    );

    let aligned = aligner
        .push_envelope(
            SLOW,
            OrderedEnvelope::Barrier {
                checkpoint: CheckpointId(1),
                generation: Generation(1),
            },
            0,
            130,
        )
        .unwrap();
    match aligned {
        AlignmentOutcome::Aligned(bar) => {
            assert_eq!(bar.drained_paused_data, vec![(FAST, "paused_d1")]);
            assert_eq!(
                bar.drained_paused_watermarks,
                vec![(FAST, EventTimestamp::Millis(500))]
            );
        }
        other => panic!("expected Aligned, got {other:?}"),
    }
    assert_eq!(aligner.watermark(&FAST), Some(EventTimestamp::Millis(500)));
}

#[tokio::test]
async fn repro_drop_oldest_bridge_exits_when_receiver_dropped() {
    let state = build_test_state();
    let bus = state.stream_bus.clone();
    let app = create_router(state);

    let create_stream = json!({
        "sql": "CREATE STREAM drop_idle () WITH (TYPE=\"memory\", BUFFER_FULL_POLICY=\"dropOldest\")"
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/streams")
                .header("content-type", "application/json")
                .body(Body::from(create_stream.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "stream create failed: {}",
        resp.status()
    );

    let create_rule = json!({
        "id": "r_drop_idle",
        "sql": "SELECT * FROM drop_idle",
        "actions": [{ "log": {} }]
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/rules")
                .header("content-type", "application/json")
                .body(Body::from(create_rule.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Let rule task subscribe, then delete the rule without ever publishing a record.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let del_resp = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/rules/r_drop_idle")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(del_resp.status().is_success());

    // Yield enough for the DropOldest bridge task's `tx.closed()` branch to run.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut data = indexmap::IndexMap::new();
    data.insert("x".to_string(), json!(1));
    let delivered = bus
        .publish_async("drop_idle", StreamRecord::new(data))
        .await
        .expect("publish_async should succeed with 0 delivered");
    assert_eq!(
        delivered, 0,
        "dropOldest bridge task must exit immediately on rule stop instead of waiting for a record"
    );
}

#[test]
fn repro_kafka_offset_beyond_high_watermark() {
    let err = resolve_start_offset(10, 50, Some(51), "earliest")
        .expect_err("restore offset past high watermark must error");
    assert_eq!(
        err,
        KafkaReliableError::OffsetBeyondHighWatermark {
            requested_next_offset: 51,
            high_watermark_offset: 50,
        }
    );
}

#[tokio::test]
async fn repro_dag_backpressure_does_not_drop_records() {
    let mut nodes = HashMap::new();
    nodes.insert(
        "src".to_string(),
        GraphNode {
            node_category: "source".to_string(),
            node_type: "memory".to_string(),
            props: HashMap::from([("datasource".to_string(), json!("bp_topic"))]),
        },
    );
    nodes.insert(
        "rl".to_string(),
        GraphNode {
            node_category: "operator".to_string(),
            node_type: "rate_limit".to_string(),
            props: HashMap::from([
                ("rate".to_string(), json!(40)),
                ("burst".to_string(), json!(2)),
                ("strategy".to_string(), json!("buffer")),
                ("max_buffer".to_string(), json!(2)),
                ("max_buffer_bytes".to_string(), json!(4096)),
                ("overflow_policy".to_string(), json!("backpressure")),
            ]),
        },
    );
    nodes.insert(
        "snk".to_string(),
        GraphNode {
            node_category: "sink".to_string(),
            node_type: "log".to_string(),
            props: HashMap::new(),
        },
    );

    let mut edges = HashMap::new();
    edges.insert(
        "src".to_string(),
        EdgeTargets::Single(vec!["rl".to_string()]),
    );
    edges.insert(
        "rl".to_string(),
        EdgeTargets::Single(vec!["snk".to_string()]),
    );

    let graph = GraphDefinition {
        nodes,
        topo: GraphTopo {
            sources: vec!["src".to_string()],
            edges,
        },
    };

    let bus = StreamBus::new();
    let rule_mgr = RuleManager::new(bus.clone());
    let counters = Arc::new(RuleCounters::default());
    let running = Arc::new(RwLock::new(RuleStatus {
        status: "running".to_string(),
        ..RuleStatus::default()
    }));
    let handle = tokio::spawn(run_graph_rule(
        "bp_rule".to_string(),
        graph,
        bus.clone(),
        rule_mgr,
        counters.clone(),
        running.clone(),
        CommsHub::new(64),
        1,
    ));

    tokio::time::sleep(Duration::from_millis(50)).await;

    for i in 0..20 {
        let mut data = indexmap::IndexMap::new();
        data.insert("seq".to_string(), json!(i));
        let delivered = bus
            .publish_async("bp_topic", StreamRecord::new(data))
            .await
            .expect("publish should succeed");
        assert_eq!(delivered, 1);
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while counters.sink_out.load(std::sync::atomic::Ordering::Relaxed) < 20
        && tokio::time::Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let reached = counters.sink_out.load(std::sync::atomic::Ordering::Relaxed);
    {
        let mut st = running.write();
        st.status = "stopped".to_string();
    }
    let _ = handle.await;
    assert_eq!(
        reached, 20,
        "all 20 published records must reach the sink under backpressure, got {reached}"
    );
}

#[tokio::test]
async fn repro_restart_preserves_open_window_rows() {
    let store = ReliableSqliteStore::open(":memory:", SqliteDurabilityMode::Full)
        .await
        .unwrap();
    let stmt = Parser::new("SELECT count(*) AS cnt FROM k GROUP BY COUNTWINDOW(2)")
        .parse_select()
        .unwrap();
    let id = identity("r_win_restart", 1);

    let mut runner1 = ReliableSingleSourceRunner::new(
        id.clone(),
        partition(),
        stmt.clone(),
        RetryPolicy::default(),
    )
    .unwrap();
    let mut reader1 = KafkaReliablePartitionReader::new(
        partition(),
        Generation(1),
        None,
        0,
        KafkaTombstonePolicy::Fail,
    );
    let sink1 = CountingSink {
        delivered: Mutex::new(Vec::new()),
    };
    let clock = TestClock::new(0);
    let cancel = DeadlineCancellation::default();

    let m0 = reader1.prepare(KafkaRawRecord {
        offset: 0,
        payload: Some(br#"{"v":10}"#.to_vec()),
    });
    runner1
        .process_and_checkpoint(&mut reader1, &m0, &[], &sink1, &store, &clock, &cancel)
        .await
        .unwrap();
    assert_eq!(sink1.delivered.lock().len(), 0);
    drop(runner1);

    // Restart runner and restore persisted open window state alongside offset 1.
    let mut runner2 =
        ReliableSingleSourceRunner::new(id, partition(), stmt, RetryPolicy::default()).unwrap();
    let restored = runner2
        .restore_before_intake(&store, &mut [])
        .await
        .unwrap();
    assert_eq!(restored, Some(1));

    let mut reader2 = KafkaReliablePartitionReader::new(
        partition(),
        Generation(1),
        None,
        restored.unwrap(),
        KafkaTombstonePolicy::Fail,
    );
    let sink2 = CountingSink {
        delivered: Mutex::new(Vec::new()),
    };
    let m1 = reader2.prepare(KafkaRawRecord {
        offset: 1,
        payload: Some(br#"{"v":20}"#.to_vec()),
    });
    runner2
        .process_and_checkpoint(&mut reader2, &m1, &[], &sink2, &store, &clock, &cancel)
        .await
        .unwrap();

    {
        let delivered = sink2.delivered.lock();
        assert_eq!(
            delivered.len(),
            1,
            "open count window must complete after restart"
        );
        assert_eq!(delivered[0]["cnt"], json!(2));
    }

    // Also verify TUMBLINGWINDOW preserves open rows and tumbling_start_ms across restart.
    let tumble_stmt = Parser::new("SELECT count(*) AS cnt FROM k GROUP BY TUMBLINGWINDOW(ss, 10)")
        .parse_select()
        .unwrap();
    let tumble_id = identity("r_tumble_restart", 1);
    let mut t_runner1 = ReliableSingleSourceRunner::new(
        tumble_id.clone(),
        partition(),
        tumble_stmt.clone(),
        RetryPolicy::default(),
    )
    .unwrap();
    let mut t_reader1 = KafkaReliablePartitionReader::new(
        partition(),
        Generation(1),
        None,
        0,
        KafkaTombstonePolicy::Fail,
    );
    let t_sink1 = CountingSink {
        delivered: Mutex::new(Vec::new()),
    };
    let t_clock1 = TestClock::new(1_000);
    let tm0 = t_reader1.prepare(KafkaRawRecord {
        offset: 0,
        payload: Some(br#"{"v":10}"#.to_vec()),
    });
    t_runner1
        .process_and_checkpoint(
            &mut t_reader1,
            &tm0,
            &[],
            &t_sink1,
            &store,
            &t_clock1,
            &cancel,
        )
        .await
        .unwrap();
    assert!(t_sink1.delivered.lock().is_empty());
    drop(t_runner1);

    let mut t_runner2 = ReliableSingleSourceRunner::new(
        tumble_id,
        partition(),
        tumble_stmt,
        RetryPolicy::default(),
    )
    .unwrap();
    let t_restored = t_runner2
        .restore_before_intake(&store, &mut [])
        .await
        .unwrap();
    assert_eq!(t_restored, Some(1));
    let mut t_reader2 = KafkaReliablePartitionReader::new(
        partition(),
        Generation(1),
        None,
        t_restored.unwrap(),
        KafkaTombstonePolicy::Fail,
    );
    let t_sink2 = CountingSink {
        delivered: Mutex::new(Vec::new()),
    };
    let t_clock2 = TestClock::new(12_000);
    let tm1 = t_reader2.prepare(KafkaRawRecord {
        offset: 1,
        payload: Some(br#"{"v":20}"#.to_vec()),
    });
    t_runner2
        .process_and_checkpoint(
            &mut t_reader2,
            &tm1,
            &[],
            &t_sink2,
            &store,
            &t_clock2,
            &cancel,
        )
        .await
        .unwrap();
    let t_delivered = t_sink2.delivered.lock();
    assert!(
        !t_delivered.is_empty(),
        "restored tumbling window row must flush once window closes"
    );
    assert_eq!(t_delivered[0]["cnt"], json!(1));
}

#[tokio::test]
async fn repro_out_of_order_kafka_message_rejected_before_checkpoint() {
    let store = ReliableSqliteStore::open(":memory:", SqliteDurabilityMode::Full)
        .await
        .unwrap();
    let stmt = Parser::new("SELECT id FROM k").parse_select().unwrap();
    let id = identity("r_ooo", 1);
    let mut runner =
        ReliableSingleSourceRunner::new(id.clone(), partition(), stmt, RetryPolicy::default())
            .unwrap();
    let mut reader = KafkaReliablePartitionReader::new(
        partition(),
        Generation(1),
        None,
        0,
        KafkaTombstonePolicy::Fail,
    );
    let _m0 = reader.prepare(KafkaRawRecord {
        offset: 0,
        payload: Some(br#"{"id":0}"#.to_vec()),
    });
    let m1 = reader.prepare(KafkaRawRecord {
        offset: 1,
        payload: Some(br#"{"id":1}"#.to_vec()),
    });

    let sink = CountingSink {
        delivered: Mutex::new(Vec::new()),
    };
    let clock = TestClock::new(0);
    let cancel = DeadlineCancellation::default();

    let err = runner
        .process_and_checkpoint(&mut reader, &m1, &[], &sink, &store, &clock, &cancel)
        .await
        .expect_err("processing offset 1 before offset 0 must fail");
    assert!(matches!(
        err,
        ReliableRuntimeError::Kafka(KafkaReliableError::OutOfOrderCommit {
            expected_offset: 0,
            actual_offset: 1,
        })
    ));
    assert!(
        sink.delivered.lock().is_empty(),
        "no effects may be delivered for rejected out-of-order message"
    );
    assert!(
        store.load_latest(&id.rule_id).await.unwrap().is_none(),
        "SQLite must not record a checkpoint for rejected out-of-order message"
    );
    assert_eq!(reader.committed_next_offset(), 0);
}

#[tokio::test]
async fn repro_rule_explain_redacts_stored_url_query_credentials() {
    let app = create_router(build_test_state());

    let create_stream = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/streams")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({ "sql": "CREATE STREAM s_explain () WITH (TYPE=\"memory\")" })
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(create_stream.status().is_success());

    let create_rule = json!({
        "id": "r_explain_url",
        "sql": "SELECT * FROM s_explain",
        "actions": [{
            "rest": {
                "url": "https://user:SECRET_PASS@example.invalid/hook?token=SECRET_Q&api_key=SECRET_K&keep=1"
            }
        }]
    });
    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/rules")
                .header("content-type", "application/json")
                .body(Body::from(create_rule.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);

    let explain_resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/rules/r_explain_url/explain")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(explain_resp.status(), StatusCode::OK);
    let body_bytes = axum::body::to_bytes(explain_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();
    for leaked in ["SECRET_PASS", "SECRET_Q", "SECRET_K"] {
        assert!(
            !body_str.contains(leaked),
            "explain response leaked secret {leaked}: {body_str}"
        );
    }
    let explain_json: Value = serde_json::from_str(&body_str).unwrap();
    let redacted_url = explain_json["plan"]["redacted_actions"][0]["rest"]["url"]
        .as_str()
        .expect("redacted rest url present");
    assert_eq!(
        redacted_url,
        "https://[REDACTED]@example.invalid/hook?token=[REDACTED]&api_key=[REDACTED]&keep=1"
    );
}

#[tokio::test]
async fn repro_native_dag_rejects_checkpoint_enabled() {
    let app = create_router(build_test_state());

    let create_stream = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/streams")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({ "sql": "CREATE STREAM s_dag_mem () WITH (TYPE=\"memory\")" })
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(create_stream.status().is_success());

    let native_dag_rule = json!({
        "id": "dag_native_cp",
        "sql": "",
        "actions": [{ "log": {} }],
        "options": { "checkpointEnabled": true },
        "graph": {
            "nodes": {
                "src": { "type": "source", "nodeType": "memory", "props": { "datasource": "s_dag_mem" } },
                "rl": { "type": "operator", "nodeType": "rate_limit", "props": { "rate": 10, "strategy": "buffer" } },
                "snk": { "type": "sink", "nodeType": "log", "props": {} }
            },
            "topo": {
                "sources": ["src"],
                "edges": { "src": ["rl"], "rl": ["snk"] }
            }
        }
    });

    for (method, uri) in [("POST", "/rules/validate"), ("POST", "/rules")] {
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(uri)
                    .header("content-type", "application/json")
                    .body(Body::from(native_dag_rule.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "native DAG with checkpointEnabled must be rejected at {uri}"
        );
    }

    let linear_mem_dag_rule = json!({
        "id": "dag_linear_cp",
        "sql": "",
        "actions": [{ "log": {} }],
        "options": { "checkpointEnabled": true },
        "graph": {
            "nodes": {
                "src": { "type": "source", "nodeType": "memory", "props": { "datasource": "s_dag_mem" } },
                "f": { "type": "operator", "nodeType": "filter", "props": { "expr": "v > 1" } },
                "snk": { "type": "sink", "nodeType": "log", "props": {} }
            },
            "topo": {
                "sources": ["src"],
                "edges": { "src": ["f"], "f": ["snk"] }
            }
        }
    });

    for (method, uri) in [("POST", "/rules/validate"), ("POST", "/rules")] {
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(uri)
                    .header("content-type", "application/json")
                    .body(Body::from(linear_mem_dag_rule.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "memory-source linear DAG with checkpointEnabled must be rejected at {uri}"
        );
    }

    // Creating the native DAG without checkpointEnabled succeeds, and updating it via PUT with
    // checkpointEnabled: true must be rejected.
    let valid_native = json!({
        "id": "dag_native_ok",
        "sql": "",
        "actions": [{ "log": {} }],
        "graph": {
            "nodes": {
                "src": { "type": "source", "nodeType": "memory", "props": { "datasource": "s_dag_mem" } },
                "rl": { "type": "operator", "nodeType": "rate_limit", "props": { "rate": 10, "strategy": "buffer" } },
                "snk": { "type": "sink", "nodeType": "log", "props": {} }
            },
            "topo": {
                "sources": ["src"],
                "edges": { "src": ["rl"], "rl": ["snk"] }
            }
        }
    });
    let ok_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/rules")
                .header("content-type", "application/json")
                .body(Body::from(valid_native.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ok_resp.status(), StatusCode::CREATED);

    let mut updated_native = valid_native;
    updated_native["options"] = json!({ "checkpointEnabled": true });
    let put_resp = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/rules/dag_native_ok")
                .header("content-type", "application/json")
                .body(Body::from(updated_native.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        put_resp.status(),
        StatusCode::BAD_REQUEST,
        "updating native DAG to checkpointEnabled must be rejected"
    );
}

#[tokio::test]
async fn repro_outbox_worker_cannot_overtake_leased_same_key_effect() {
    let store = ReliableSqliteStore::open(":memory:", SqliteDurabilityMode::Full)
        .await
        .unwrap();
    let outbox = SqliteOutbox::new(&store, OutboxCapacity::default());
    let mut e1 = effect("r_order", "eff_order_1");
    e1.order_key = Some("k1".to_string());
    let mut e2 = effect("r_order", "eff_order_2");
    e2.order_key = Some("k1".to_string());

    outbox
        .commit_state_and_enqueue(
            envelope("r_order", 1),
            vec![
                OutboxEntry {
                    effect: e1.clone(),
                    payload: b"first".to_vec(),
                },
                OutboxEntry {
                    effect: e2,
                    payload: b"second".to_vec(),
                },
            ],
        )
        .await
        .unwrap();

    let rule = RuleId::try_from("r_order").unwrap();
    let act = ActionId::try_from("mqtt_0").unwrap();
    let a = outbox
        .claim_pending(&rule, &act, "worker_a", 1)
        .await
        .unwrap();
    assert_eq!(a.len(), 1);
    assert_eq!(a[0].effect.effect_id, e1.effect_id);

    let b = outbox
        .claim_pending(&rule, &act, "worker_b", 10)
        .await
        .unwrap();
    assert!(
        b.is_empty(),
        "worker_b must not claim second effect while worker_a holds unsettled earlier effect on same order_key"
    );
}

#[test]
fn repro_paused_watermarks_charged_against_alignment_budget() {
    const FAST: EdgeId = EdgeId(1);
    const SLOW: EdgeId = EdgeId(2);
    let budget = ResourceBudget::new(BudgetLimits {
        max_items: Some(1),
        max_logical_bytes: Some(4096),
        max_tasks: None,
    });
    let mut aligner: BarrierAligner<&'static str> =
        BarrierAligner::new(vec![FAST, SLOW], vec!["s1".to_string()], budget, 1_000).unwrap();

    aligner
        .push_envelope(
            FAST,
            OrderedEnvelope::Barrier {
                checkpoint: CheckpointId(1),
                generation: Generation(1),
            },
            0,
            100,
        )
        .unwrap();

    let first = aligner
        .push_envelope(
            FAST,
            OrderedEnvelope::Watermark {
                edge: FAST,
                timestamp: EventTimestamp::Millis(100),
            },
            0,
            110,
        )
        .unwrap();
    assert!(matches!(
        first,
        AlignmentOutcome::BufferedPaused {
            edge: FAST,
            buffered_on_edge: 1
        }
    ));

    let second = aligner.push_envelope(
        FAST,
        OrderedEnvelope::Watermark {
            edge: FAST,
            timestamp: EventTimestamp::Millis(200),
        },
        0,
        120,
    );
    assert!(
        matches!(
            second,
            Err(BarrierAlignmentError::PausedBufferBudgetExceeded { edge: FAST, .. })
        ),
        "second watermark on paused edge must respect 1-item alignment buffer budget, got {second:?}"
    );
}
