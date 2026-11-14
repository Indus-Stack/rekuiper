//! Durable outbox dispatcher with per-action claim ownership, key ordering,
//! idempotency tracking, bounded retries, and explicit permanent-failure/DLQ policy.

use crate::engine::retry::{execute_with_retry, RetryPolicy};
use parking_lot::Mutex;
use rekuiper_core::contracts::{
    ActionId, Clock, DeadlineCancellation, EffectId, EffectOutcome, Generation, RuleId,
    StructuredError,
};
use rekuiper_core::reliability::{OutboxEntry, OutboxError, SqliteOutbox, DEFAULT_CLAIM_LEASE_MS};
use rekuiper_core::resources::ResourceBudget;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Explicit policy for handling [`EffectOutcome::PermanentFailure`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PermanentFailurePolicy {
    /// Persist terminal outcome in outbox and forward to DLQ handler if present.
    #[default]
    TerminalAndDlq,
    /// Persist terminal outcome in outbox without DLQ emission.
    TerminalOnly,
}

/// Sink adapter interface used by [`OutboxDispatcher`].
#[allow(async_fn_in_trait)]
pub trait OutboxSinkAdapter: Send + Sync {
    async fn deliver(&self, entry: &OutboxEntry, idempotency_key: &str) -> EffectOutcome;

    async fn send_to_dlq(&self, _entry: &OutboxEntry, _error: &StructuredError) {}
}

/// Coordinates exclusive `(RuleId, ActionId)` ownership across concurrent
/// dispatcher workers so two dispatchers cannot claim conflicting ownership.
#[derive(Debug, Clone, Default)]
pub struct DispatchCoordinator {
    leases: Arc<Mutex<HashMap<(RuleId, ActionId), u64>>>,
    next_lease_id: Arc<AtomicU64>,
}

impl DispatchCoordinator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Attempts to acquire exclusive ownership of `(rule_id, action_id)`.
    pub fn try_acquire(
        &self,
        rule_id: &RuleId,
        action_id: &ActionId,
        worker_id: &str,
    ) -> Option<ActionDispatchLease> {
        let mut guard = self.leases.lock();
        let key = (rule_id.clone(), action_id.clone());
        if guard.contains_key(&key) {
            return None;
        }
        let lease_id = self.next_lease_id.fetch_add(1, Ordering::Relaxed) + 1;
        guard.insert(key.clone(), lease_id);
        Some(ActionDispatchLease {
            leases: Arc::clone(&self.leases),
            key,
            lease_id,
            worker_id: worker_id.to_string(),
        })
    }
}

/// RAII lease releasing `(RuleId, ActionId)` ownership on drop.
#[derive(Debug)]
pub struct ActionDispatchLease {
    leases: Arc<Mutex<HashMap<(RuleId, ActionId), u64>>>,
    key: (RuleId, ActionId),
    lease_id: u64,
    worker_id: String,
}

impl ActionDispatchLease {
    fn claim_owner(&self) -> String {
        format!("{}:{}", self.worker_id, self.lease_id)
    }
}

impl Drop for ActionDispatchLease {
    fn drop(&mut self) {
        let mut guard = self.leases.lock();
        if guard.get(&self.key) == Some(&self.lease_id) {
            guard.remove(&self.key);
        }
    }
}

/// Errors returned by [`OutboxDispatcher::dispatch_once`].
#[derive(Debug)]
pub enum OutboxDispatchError {
    OwnershipConflict {
        rule_id: RuleId,
        action_id: ActionId,
    },
    Outbox(OutboxError),
}

impl std::fmt::Display for OutboxDispatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OwnershipConflict { rule_id, action_id } => write!(
                f,
                "outbox dispatch ownership conflict for ({rule_id}, {action_id})"
            ),
            Self::Outbox(e) => write!(f, "outbox error: {e}"),
        }
    }
}

impl std::error::Error for OutboxDispatchError {}

impl From<OutboxError> for OutboxDispatchError {
    fn from(value: OutboxError) -> Self {
        Self::Outbox(value)
    }
}

/// Summary of one [`OutboxDispatcher::dispatch_once`] pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DispatchBatchReport {
    pub claimed: usize,
    pub delivered: usize,
    pub permanent_failures: usize,
    pub unsettled_retryable: usize,
    pub unsettled_ambiguous: usize,
    pub deferred_by_order_key: usize,
    pub delivered_ids: Vec<EffectId>,
}

/// Claims pending durable effects by `(rule_id, action_id)`, preserves ordering
/// per declared `order_key`, runs bounded retries, and persists terminal acks.
#[derive(Debug, Clone)]
pub struct OutboxDispatcher {
    worker_id: String,
    coordinator: DispatchCoordinator,
    retry_policy: RetryPolicy,
    permanent_policy: PermanentFailurePolicy,
}

impl OutboxDispatcher {
    pub fn new(
        worker_id: impl Into<String>,
        coordinator: DispatchCoordinator,
        retry_policy: RetryPolicy,
        permanent_policy: PermanentFailurePolicy,
    ) -> Self {
        Self {
            worker_id: worker_id.into(),
            coordinator,
            retry_policy,
            permanent_policy,
        }
    }

    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// Claims up to `limit` pending effects for `(rule_id, action_id)` under an
    /// exclusive coordinator lease and dispatches them.
    #[allow(clippy::too_many_arguments)]
    pub async fn dispatch_once<A: OutboxSinkAdapter>(
        &self,
        outbox: &SqliteOutbox<'_>,
        rule_id: &RuleId,
        action_id: &ActionId,
        limit: usize,
        adapter: &A,
        clock: &dyn Clock,
        cancellation: &DeadlineCancellation,
        task_budget: Option<&ResourceBudget>,
    ) -> Result<DispatchBatchReport, OutboxDispatchError> {
        self.dispatch_once_inner(
            outbox,
            rule_id,
            action_id,
            None,
            limit,
            adapter,
            clock,
            cancellation,
            task_budget,
        )
        .await
    }

    /// Atomically verifies `generation` is active and unrevoked inside the same
    /// SQLite claim transaction while acquiring the coordinator lease and dispatching.
    #[allow(clippy::too_many_arguments)]
    pub async fn dispatch_once_fenced<A: OutboxSinkAdapter>(
        &self,
        outbox: &SqliteOutbox<'_>,
        rule_id: &RuleId,
        action_id: &ActionId,
        generation: Generation,
        limit: usize,
        adapter: &A,
        clock: &dyn Clock,
        cancellation: &DeadlineCancellation,
        task_budget: Option<&ResourceBudget>,
    ) -> Result<DispatchBatchReport, OutboxDispatchError> {
        self.dispatch_once_inner(
            outbox,
            rule_id,
            action_id,
            Some(generation),
            limit,
            adapter,
            clock,
            cancellation,
            task_budget,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn dispatch_once_inner<A: OutboxSinkAdapter>(
        &self,
        outbox: &SqliteOutbox<'_>,
        rule_id: &RuleId,
        action_id: &ActionId,
        required_generation: Option<Generation>,
        limit: usize,
        adapter: &A,
        clock: &dyn Clock,
        cancellation: &DeadlineCancellation,
        task_budget: Option<&ResourceBudget>,
    ) -> Result<DispatchBatchReport, OutboxDispatchError> {
        let Some(lease) = self
            .coordinator
            .try_acquire(rule_id, action_id, &self.worker_id)
        else {
            return Err(OutboxDispatchError::OwnershipConflict {
                rule_id: rule_id.clone(),
                action_id: action_id.clone(),
            });
        };

        let claim_owner = lease.claim_owner();
        let entries = outbox
            .claim_pending_with_lease(
                rule_id,
                action_id,
                &claim_owner,
                limit,
                DEFAULT_CLAIM_LEASE_MS,
                required_generation,
            )
            .await?;

        let mut report = DispatchBatchReport {
            claimed: entries.len(),
            ..DispatchBatchReport::default()
        };
        let mut blocked_keys: HashSet<String> = HashSet::new();

        for entry in &entries {
            if let Some(order_key) = &entry.effect.order_key {
                if blocked_keys.contains(order_key) {
                    report.deferred_by_order_key += 1;
                    outbox
                        .release_claim(&entry.effect.effect_id, &claim_owner)
                        .await?;
                    continue;
                }
            }

            let idempotency_key = entry.effect.effect_id.as_str().to_string();
            let exec =
                execute_with_retry(&self.retry_policy, clock, cancellation, task_budget, |_| {
                    adapter.deliver(entry, &idempotency_key)
                })
                .await;

            match exec.outcome {
                EffectOutcome::Delivered => {
                    outbox
                        .ack(&entry.effect.effect_id, EffectOutcome::Delivered)
                        .await?;
                    report.delivered += 1;
                    report.delivered_ids.push(entry.effect.effect_id.clone());
                }
                EffectOutcome::PermanentFailure { error } => {
                    if self.permanent_policy == PermanentFailurePolicy::TerminalAndDlq {
                        adapter.send_to_dlq(entry, &error).await;
                    }
                    outbox
                        .ack(
                            &entry.effect.effect_id,
                            EffectOutcome::PermanentFailure { error },
                        )
                        .await?;
                    report.permanent_failures += 1;
                }
                EffectOutcome::Retryable { .. } => {
                    // Leave unsettled in SQLite outbox and release claim so retry/restart picks it up.
                    outbox
                        .release_claim(&entry.effect.effect_id, &claim_owner)
                        .await?;
                    report.unsettled_retryable += 1;
                    if let Some(order_key) = &entry.effect.order_key {
                        blocked_keys.insert(order_key.clone());
                    }
                }
                EffectOutcome::Ambiguous { .. } => {
                    // Leave unsettled for idempotent reconciliation and release claim for immediate retry.
                    outbox
                        .release_claim(&entry.effect.effect_id, &claim_owner)
                        .await?;
                    report.unsettled_ambiguous += 1;
                    if let Some(order_key) = &entry.effect.order_key {
                        blocked_keys.insert(order_key.clone());
                    }
                }
            }
        }

        Ok(report)
    }
}
