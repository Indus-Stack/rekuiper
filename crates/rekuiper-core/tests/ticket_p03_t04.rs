use rekuiper_core::contracts::{
    CheckpointId, CheckpointStore, ContentChecksum, DeadlineCancellation, Generation, OperatorId,
    OutboxBoundary, PlanIdentity, RuleId, StateError, StateParticipant, StateResourceUsage,
    StateSchemaVersion, StateSnapshot, TestClock, STATE_ENVELOPE_VERSION,
};
use rekuiper_core::reliability::{CheckpointCoordinator, CoordinatorError, CoordinatorState};
use rekuiper_core::testing::FakeCheckpointOutboxStore;
use std::collections::BTreeMap;
use std::time::Duration;

fn checksum(ch: char) -> ContentChecksum {
    let hex: String = std::iter::repeat_n(ch, 64).collect();
    ContentChecksum::try_from(format!("sha256:{hex}")).unwrap()
}

struct FakeParticipant {
    id: OperatorId,
    payload: Vec<u8>,
}

impl StateParticipant for FakeParticipant {
    fn operator_id(&self) -> &OperatorId {
        &self.id
    }

    fn state_version(&self) -> StateSchemaVersion {
        StateSchemaVersion(1)
    }

    fn resource_usage(&self) -> StateResourceUsage {
        StateResourceUsage {
            records: 1,
            bytes: self.payload.len() as u64,
        }
    }

    fn capture_snapshot(&self) -> Result<StateSnapshot, StateError> {
        Ok(StateSnapshot {
            envelope_version: STATE_ENVELOPE_VERSION,
            operator_id: self.id.clone(),
            state_version: StateSchemaVersion(1),
            checksum: checksum('b'),
            payload: self.payload.clone(),
            usage: self.resource_usage(),
        })
    }

    fn restore_snapshot(&mut self, snapshot: &StateSnapshot) -> Result<(), StateError> {
        self.payload = snapshot.payload.clone();
        Ok(())
    }
}

fn identity(gen: u64) -> PlanIdentity {
    PlanIdentity {
        rule_id: RuleId::try_from("rule_coord").unwrap(),
        generation: Generation(gen),
        plan_fingerprint: checksum('a'),
        schema_fingerprints: BTreeMap::new(),
        extension_digests: BTreeMap::new(),
    }
}

#[tokio::test]
async fn coordinator_commits_and_preserves_prior_commit_on_every_failpoint() {
    let store = FakeCheckpointOutboxStore::new();
    let clock = TestClock::new(1_700_000_000_000);
    let cancel = DeadlineCancellation::default();
    let p1 = FakeParticipant {
        id: OperatorId::try_from("op_1").unwrap(),
        payload: b"v1".to_vec(),
    };
    let mut coord = CheckpointCoordinator::new(identity(2));

    // Initial successful checkpoint (cp 1).
    let env1 = coord
        .run_checkpoint(
            CheckpointId(1),
            Generation(2),
            &[&p1],
            Vec::new(),
            OutboxBoundary::default(),
            &clock,
            Duration::from_secs(5),
            &cancel,
            || async { Ok(()) },
            &store,
        )
        .await
        .expect("cp 1 succeeds");
    assert_eq!(coord.state(), CoordinatorState::Idle);
    assert_eq!(env1.metadata.checkpoint_id, CheckpointId(1));

    // Failpoints at every transition leave cp 1 intact and return to Idle.
    for fp in ["capturing", "waiting_effects", "committing"] {
        coord.arm_failpoint(fp);
        let err = coord
            .run_checkpoint(
                CheckpointId(2),
                Generation(2),
                &[&p1],
                Vec::new(),
                OutboxBoundary::default(),
                &clock,
                Duration::from_secs(5),
                &cancel,
                || async { Ok(()) },
                &store,
            )
            .await
            .expect_err("failpoint must abort");
        assert_eq!(err, CoordinatorError::Failpoint(fp));
        assert_eq!(coord.state(), CoordinatorState::Idle);
        let latest = store
            .load_latest(&RuleId::try_from("rule_coord").unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(latest.metadata.checkpoint_id, CheckpointId(1));
    }

    // Stale generation is rejected without touching store.
    let stale_err = coord
        .run_checkpoint(
            CheckpointId(3),
            Generation(1),
            &[&p1],
            Vec::new(),
            OutboxBoundary::default(),
            &clock,
            Duration::from_secs(5),
            &cancel,
            || async { Ok(()) },
            &store,
        )
        .await
        .expect_err("stale generation rejected");
    assert!(matches!(
        stale_err,
        CoordinatorError::StaleGeneration { .. }
    ));
}

#[tokio::test]
async fn coordinator_aborts_cleanly_on_timeout_and_cancellation() {
    let store = FakeCheckpointOutboxStore::new();
    let clock = TestClock::new(1_700_000_000_000);
    let cancel = DeadlineCancellation::default();
    let p1 = FakeParticipant {
        id: OperatorId::try_from("op_1").unwrap(),
        payload: b"v1".to_vec(),
    };
    let mut coord = CheckpointCoordinator::new(identity(1));

    // Timeout while waiting for effects.
    let clock_clone = clock.clone();
    let err = coord
        .run_checkpoint(
            CheckpointId(1),
            Generation(1),
            &[&p1],
            Vec::new(),
            OutboxBoundary::default(),
            &clock,
            Duration::from_millis(100),
            &cancel,
            || async move {
                tokio::task::yield_now().await;
                clock_clone.advance(Duration::from_millis(100));
                std::future::pending::<Result<(), CoordinatorError>>().await
            },
            &store,
        )
        .await
        .expect_err("timeout must abort");
    assert_eq!(err, CoordinatorError::TimedOut);
    assert_eq!(coord.state(), CoordinatorState::Idle);

    // Pre-cancelled deadline aborts immediately.
    cancel.cancel();
    let err = coord
        .run_checkpoint(
            CheckpointId(2),
            Generation(1),
            &[&p1],
            Vec::new(),
            OutboxBoundary::default(),
            &clock,
            Duration::from_secs(10),
            &cancel,
            std::future::pending,
            &store,
        )
        .await
        .expect_err("cancel must abort");
    assert_eq!(err, CoordinatorError::Cancelled);
    assert_eq!(coord.state(), CoordinatorState::Idle);
}
