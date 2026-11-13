//! Transactional in-memory checkpoint and outbox store fake.

use crate::contracts::{
    CheckpointEnvelope, CheckpointError, CheckpointStore, CompletionStatus, EffectId,
    EffectOutcome, EffectRecord, RuleId,
};
use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Default)]
struct StoreInner {
    checkpoints: BTreeMap<RuleId, CheckpointEnvelope>,
    outbox: BTreeMap<EffectId, EffectRecord>,
    failpoints: BTreeSet<String>,
    op_log: Vec<String>,
}

/// Deterministic, generation-fenced in-memory checkpoint and outbox store.
#[derive(Default)]
pub struct FakeCheckpointOutboxStore {
    inner: Mutex<StoreInner>,
}

impl FakeCheckpointOutboxStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn arm_failpoint(&self, name: impl Into<String>) {
        self.inner.lock().failpoints.insert(name.into());
    }

    pub fn clear_failpoint(&self, name: &str) {
        self.inner.lock().failpoints.remove(name);
    }

    pub fn op_log(&self) -> Vec<String> {
        self.inner.lock().op_log.clone()
    }

    pub fn outbox_record(&self, effect_id: &EffectId) -> Option<EffectRecord> {
        self.inner.lock().outbox.get(effect_id).cloned()
    }

    /// Atomically commits a checkpoint and its associated outbox effect records.
    /// Rejects stale generations and leaves store state unchanged on rollback/failpoint.
    pub fn commit_with_outbox(
        &self,
        envelope: CheckpointEnvelope,
        effects: Vec<EffectRecord>,
    ) -> Result<(), CheckpointError> {
        let mut inner = self.inner.lock();
        let rule_id = envelope.metadata.identity.rule_id.clone();
        let incoming_gen = envelope.metadata.identity.generation;

        if let Some(existing) = inner.checkpoints.get(&rule_id) {
            let existing_gen = existing.metadata.identity.generation;
            if incoming_gen < existing_gen {
                inner.op_log.push(format!(
                    "reject_stale_generation:{rule_id}:{}",
                    incoming_gen.0
                ));
                return Err(CheckpointError::GenerationMismatch {
                    expected: existing_gen,
                    actual: incoming_gen,
                });
            }
        }

        if inner.failpoints.take("commit_rollback").is_some() {
            inner.op_log.push(format!("commit_rollback:{rule_id}"));
            return Err(CheckpointError::StorageFailure(
                "failpoint: commit_rollback".to_owned(),
            ));
        }

        for effect in effects {
            inner.outbox.insert(effect.effect_id.clone(), effect);
        }
        inner.checkpoints.insert(rule_id.clone(), envelope);
        inner
            .op_log
            .push(format!("commit_ok:{rule_id}:{}", incoming_gen.0));
        Ok(())
    }

    /// Records an effect completion idempotently in the outbox.
    pub fn record_effect_completion(
        &self,
        effect_id: &EffectId,
        outcome: EffectOutcome,
    ) -> Option<CompletionStatus> {
        let mut inner = self.inner.lock();
        let status = inner
            .outbox
            .get_mut(effect_id)
            .map(|rec| rec.record_completion(outcome));
        if let Some(st) = status {
            inner
                .op_log
                .push(format!("effect_completion:{}:{st:?}", effect_id.as_str()));
        }
        status
    }
}

#[async_trait]
impl CheckpointStore for FakeCheckpointOutboxStore {
    async fn commit_checkpoint(&self, envelope: CheckpointEnvelope) -> Result<(), CheckpointError> {
        self.commit_with_outbox(envelope, Vec::new())
    }

    async fn load_latest(
        &self,
        rule_id: &RuleId,
    ) -> Result<Option<CheckpointEnvelope>, CheckpointError> {
        let mut inner = self.inner.lock();
        if inner.failpoints.take("load_error").is_some() {
            inner.op_log.push(format!("load_error:{rule_id}"));
            return Err(CheckpointError::StorageFailure(
                "failpoint: load_error".to_owned(),
            ));
        }
        inner.op_log.push(format!("load_latest:{rule_id}"));
        Ok(inner.checkpoints.get(rule_id).cloned())
    }
}
