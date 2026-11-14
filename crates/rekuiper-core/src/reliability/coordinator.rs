//! Generation-fenced checkpoint coordinator state machine.

use crate::contracts::{
    CheckpointEnvelope, CheckpointError, CheckpointId, CheckpointMetadata, CheckpointStore, Clock,
    DeadlineCancellation, Generation, OperatorId, OutboxBoundary, ParticipantSnapshotRef,
    PlanIdentity, SourcePosition, StateError, StateParticipant, StateSnapshot,
    CHECKPOINT_FORMAT_VERSION,
};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoordinatorState {
    Idle,
    Capturing,
    WaitingEffects,
    Committing,
    Aborting,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CoordinatorError {
    #[error("checkpoint already in progress for generation {generation:?}: id={active_id:?}")]
    Busy {
        generation: Generation,
        active_id: CheckpointId,
    },
    #[error("stale generation: expected {expected:?}, got {actual:?}")]
    StaleGeneration {
        expected: Generation,
        actual: Generation,
    },
    #[error("participant capture failed: {0}")]
    CaptureFailed(#[from] StateError),
    #[error("checkpoint storage or validation failed: {0}")]
    CheckpointFailed(#[from] CheckpointError),
    #[error("checkpoint timed out waiting for effects")]
    TimedOut,
    #[error("checkpoint cancelled")]
    Cancelled,
    #[error("failpoint triggered at {0}")]
    Failpoint(&'static str),
}

/// Coordinates atomic, generation-fenced checkpoints across state participants
/// without holding live-state locks during storage I/O.
pub struct CheckpointCoordinator {
    identity: PlanIdentity,
    state: CoordinatorState,
    active_checkpoint: Option<CheckpointId>,
    failpoints: BTreeSet<&'static str>,
}

impl CheckpointCoordinator {
    pub fn new(identity: PlanIdentity) -> Self {
        Self {
            identity,
            state: CoordinatorState::Idle,
            active_checkpoint: None,
            failpoints: BTreeSet::new(),
        }
    }

    pub fn state(&self) -> CoordinatorState {
        self.state
    }

    pub fn active_checkpoint(&self) -> Option<CheckpointId> {
        self.active_checkpoint
    }

    pub fn arm_failpoint(&mut self, name: &'static str) {
        self.failpoints.insert(name);
    }

    /// Runs one complete checkpoint cycle:
    /// 1. `Idle -> Capturing`: capture immutable participant snapshots synchronously.
    /// 2. `Capturing -> WaitingEffects`: await `wait_effects` with `timeout` and `cancellation`.
    /// 3. `WaitingEffects -> Committing`: persist envelope via `store` (no participant locks held).
    /// 4. On any error/timeout/cancel: `Aborting -> Idle` leaving prior commits intact.
    #[allow(clippy::too_many_arguments)]
    pub async fn run_checkpoint<F, Fut>(
        &mut self,
        checkpoint_id: CheckpointId,
        generation: Generation,
        participants: &[&dyn StateParticipant],
        source_positions: Vec<SourcePosition>,
        outbox: OutboxBoundary,
        clock: &dyn Clock,
        timeout: Duration,
        cancellation: &DeadlineCancellation,
        wait_effects: F,
        store: &dyn CheckpointStore,
    ) -> Result<CheckpointEnvelope, CoordinatorError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<(), CoordinatorError>>,
    {
        if generation != self.identity.generation {
            return Err(CoordinatorError::StaleGeneration {
                expected: self.identity.generation,
                actual: generation,
            });
        }
        if self.state != CoordinatorState::Idle {
            return Err(CoordinatorError::Busy {
                generation: self.identity.generation,
                active_id: self.active_checkpoint.unwrap_or(checkpoint_id),
            });
        }

        self.state = CoordinatorState::Capturing;
        self.active_checkpoint = Some(checkpoint_id);

        if self.failpoints.remove("capturing") {
            return self.abort_with(CoordinatorError::Failpoint("capturing"));
        }

        // Capture immutable snapshots synchronously; no locks are held past this loop.
        let mut part_refs = BTreeMap::new();
        let mut payloads: BTreeMap<OperatorId, StateSnapshot> = BTreeMap::new();
        for participant in participants {
            let snapshot = match participant.capture_snapshot() {
                Ok(snap) => snap,
                Err(err) => return self.abort_with(CoordinatorError::CaptureFailed(err)),
            };
            let op_id = participant.operator_id().clone();
            part_refs.insert(
                op_id.clone(),
                ParticipantSnapshotRef {
                    operator_id: op_id.clone(),
                    state_version: snapshot.state_version,
                    payload_checksum: snapshot.checksum.clone(),
                    payload_bytes: snapshot.payload.len() as u64,
                    usage: snapshot.usage,
                },
            );
            payloads.insert(op_id, snapshot);
        }

        self.state = CoordinatorState::WaitingEffects;
        if self.failpoints.remove("waiting_effects") {
            return self.abort_with(CoordinatorError::Failpoint("waiting_effects"));
        }

        let target = clock.now().elapsed.saturating_add(timeout);
        let effects_result = tokio::select! {
            biased;
            wait_res = clock.wait_until(target, cancellation) => {
                match wait_res {
                    Ok(()) => Err(CoordinatorError::TimedOut),
                    Err(_) => Err(CoordinatorError::Cancelled),
                }
            }
            eff_res = wait_effects() => eff_res,
        };
        if let Err(err) = effects_result {
            return self.abort_with(err);
        }

        self.state = CoordinatorState::Committing;
        if self.failpoints.remove("committing") {
            return self.abort_with(CoordinatorError::Failpoint("committing"));
        }

        let envelope = CheckpointEnvelope {
            metadata: CheckpointMetadata {
                format_version: CHECKPOINT_FORMAT_VERSION,
                checkpoint_id,
                identity: self.identity.clone(),
                created_wall_millis: clock.now().wall_millis,
                source_positions,
                participants: part_refs,
                outbox,
            },
            payloads,
        };

        if let Err(err) = store.commit_checkpoint(envelope.clone()).await {
            return self.abort_with(CoordinatorError::CheckpointFailed(err));
        }

        self.state = CoordinatorState::Idle;
        self.active_checkpoint = None;
        Ok(envelope)
    }

    fn abort_with<T>(&mut self, err: CoordinatorError) -> Result<T, CoordinatorError> {
        self.state = CoordinatorState::Aborting;
        self.state = CoordinatorState::Idle;
        self.active_checkpoint = None;
        Err(err)
    }
}
