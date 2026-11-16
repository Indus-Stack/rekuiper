use parking_lot::Mutex;
use rekuiper_core::contracts::{
    ActionId, CheckpointEnvelope, CheckpointId, CheckpointMetadata, ContentChecksum,
    DeadlineCancellation, EffectDestination, EffectId, EffectOutcome, EffectRecord, ErrorCode,
    ErrorObject, ErrorStage, Generation, OperatorId, OutboxBoundary, PlanIdentity, RuleId,
    StructuredError, TestClock, CHECKPOINT_FORMAT_VERSION,
};
use rekuiper_core::reliability::{
    deterministic_effect_id, OutboxCapacity, OutboxEntry, ReliableSqliteStore,
    SqliteDurabilityMode, SqliteOutbox,
};
use rekuiper_server::engine::outbox_dispatch::{
    DispatchCoordinator, OutboxDispatchError, OutboxDispatcher, OutboxSinkAdapter,
    PermanentFailurePolicy,
};
use rekuiper_server::engine::retry::{AmbiguousPolicy, RetryPolicy};
use std::collections::BTreeMap;
use std::time::Duration;

struct ScriptedSink {
    fail_key_a_until_call: usize,
    calls: Mutex<Vec<(String, String)>>,
    dlq: Mutex<Vec<EffectId>>,
}

impl OutboxSinkAdapter for ScriptedSink {
    async fn deliver(&self, entry: &OutboxEntry, idempotency_key: &str) -> EffectOutcome {
        let mut calls = self.calls.lock();
        calls.push((
            entry.effect.effect_id.as_str().to_string(),
            idempotency_key.to_string(),
        ));
        let count_for_id = calls
            .iter()
            .filter(|(id, _)| id == entry.effect.effect_id.as_str())
            .count();
        match entry.payload.as_slice() {
            b"perm-fail" => EffectOutcome::PermanentFailure {
                error: StructuredError {
                    code: ErrorCode::InvalidData,
                    stage: ErrorStage::Action,
                    object: ErrorObject::Action(entry.effect.destination.action_id.clone()),
                    retriable: false,
                    position: None,
                },
            },
            b"k1-first" if count_for_id <= self.fail_key_a_until_call => EffectOutcome::Retryable {
                delay_hint_millis: None,
            },
            _ => EffectOutcome::Delivered,
        }
    }

    async fn send_to_dlq(&self, entry: &OutboxEntry, _error: &StructuredError) {
        self.dlq.lock().push(entry.effect.effect_id.clone());
    }
}

fn make_entry(
    rule_id: &RuleId,
    op_id: &OperatorId,
    action_id: &ActionId,
    seq: u64,
    order_key: Option<&str>,
    payload: &[u8],
) -> OutboxEntry {
    let eff_id = deterministic_effect_id(rule_id, Generation(1), op_id, action_id, seq, 0)
        .expect("effect id");
    OutboxEntry::new(
        EffectRecord {
            effect_id: eff_id,
            rule_id: rule_id.clone(),
            generation: Generation(1),
            destination: EffectDestination {
                action_id: action_id.clone(),
                connection_id: None,
            },
            order_key: order_key.map(str::to_string),
            attempt: 0,
            terminal_outcome: None,
        },
        payload.to_vec(),
    )
}

#[tokio::test]
async fn outbox_dispatch_preserves_order_resends_on_restart_and_handles_dlq() {
    let store = ReliableSqliteStore::open(":memory:", SqliteDurabilityMode::Full)
        .await
        .expect("in-memory sqlite");
    let outbox = SqliteOutbox::new(&store, OutboxCapacity::default());
    let rule_id = RuleId::try_from("rule_dispatch").unwrap();
    let op_id = OperatorId::try_from("op_1").unwrap();
    let action_id = ActionId::try_from("http_action").unwrap();

    let e1 = make_entry(&rule_id, &op_id, &action_id, 1, Some("k1"), b"k1-first");
    let e2 = make_entry(&rule_id, &op_id, &action_id, 2, Some("k1"), b"k1-second");
    let e3 = make_entry(&rule_id, &op_id, &action_id, 3, Some("k2"), b"perm-fail");
    let e4 = make_entry(&rule_id, &op_id, &action_id, 4, Some("k3"), b"ok-k3");

    let hex: String = std::iter::repeat_n('a', 64).collect();
    let envelope = CheckpointEnvelope {
        metadata: CheckpointMetadata {
            format_version: CHECKPOINT_FORMAT_VERSION,
            checkpoint_id: CheckpointId(1),
            identity: PlanIdentity {
                rule_id: rule_id.clone(),
                generation: Generation(1),
                plan_fingerprint: ContentChecksum::try_from(format!("sha256:{hex}")).unwrap(),
                schema_fingerprints: BTreeMap::new(),
                extension_digests: BTreeMap::new(),
            },
            created_wall_millis: 1_700_000_000_000,
            source_positions: Vec::new(),
            participants: BTreeMap::new(),
            outbox: OutboxBoundary::default(),
        },
        payloads: BTreeMap::new(),
    };
    outbox
        .commit_state_and_enqueue(
            envelope,
            vec![e1.clone(), e2.clone(), e3.clone(), e4.clone()],
        )
        .await
        .expect("enqueue batch");

    let coordinator = DispatchCoordinator::new();
    let policy = RetryPolicy {
        initial_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_millis(10),
        jitter_ratio: 0.0,
        max_attempts: Some(1),
        ambiguous_policy: AmbiguousPolicy::SurfaceAmbiguous,
        ..RetryPolicy::default()
    };
    let clock = TestClock::new(1_700_000_000_000);
    let cancel = DeadlineCancellation::default();

    // Concurrent ownership conflict check: holding a lease blocks another worker.
    let held_lease = coordinator
        .try_acquire(&rule_id, &action_id, "worker-A")
        .expect("worker-A acquires lease");
    let dispatcher_b = OutboxDispatcher::new(
        "worker-B",
        coordinator.clone(),
        policy.clone(),
        PermanentFailurePolicy::TerminalAndDlq,
    );
    let sink = ScriptedSink {
        fail_key_a_until_call: 1,
        calls: Mutex::new(Vec::new()),
        dlq: Mutex::new(Vec::new()),
    };
    let conflict = dispatcher_b
        .dispatch_once(
            &outbox, &rule_id, &action_id, 10, &sink, &clock, &cancel, None,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        conflict,
        OutboxDispatchError::OwnershipConflict { .. }
    ));
    drop(held_lease);

    // Worker-B dispatches: e1 fails retryably (defers e2 on same key k1),
    // e3 hits permanent failure + DLQ, e4 delivers on independent key k3.
    let first = dispatcher_b
        .dispatch_once(
            &outbox, &rule_id, &action_id, 10, &sink, &clock, &cancel, None,
        )
        .await
        .expect("first dispatch");
    assert_eq!(first.unsettled_retryable, 1);
    assert_eq!(first.deferred_by_order_key, 1);
    assert_eq!(first.permanent_failures, 1);
    assert_eq!(first.delivered, 1);
    assert_eq!(
        sink.dlq.lock().as_slice(),
        std::slice::from_ref(&e3.effect.effect_id)
    );

    // Simulated restart: a new dispatcher claims remaining unsettled effects (e1, e2).
    let restarted = OutboxDispatcher::new(
        "worker-C",
        DispatchCoordinator::new(),
        policy,
        PermanentFailurePolicy::TerminalAndDlq,
    );
    let second = restarted
        .dispatch_once(
            &outbox, &rule_id, &action_id, 10, &sink, &clock, &cancel, None,
        )
        .await
        .expect("restarted dispatch");
    assert_eq!(second.claimed, 2);
    assert_eq!(second.delivered, 2);
    assert_eq!(
        second.delivered_ids,
        vec![e1.effect.effect_id, e2.effect.effect_id]
    );
}
