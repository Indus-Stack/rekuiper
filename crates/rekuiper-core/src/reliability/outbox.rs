//! Transactional SQLite outbox v1 queue with deterministic IDs, capacity
//! budgets, deduplication, and action-scoped claim/ack.

use super::sqlite::ReliableSqliteStore;
use crate::contracts::{
    ActionId, CheckpointEnvelope, CheckpointError, CompletionStatus, ConnectionId,
    EffectDestination, EffectId, EffectOutcome, EffectRecord, Generation, InvalidId, OperatorId,
    RuleId,
};
use sqlx::Row;
use std::time::{SystemTime, UNIX_EPOCH};

pub const DEFAULT_CLAIM_LEASE_MS: u64 = 30_000;

/// Capacity limits enforced before admitting new pending outbox entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutboxCapacity {
    pub max_items: usize,
    pub max_bytes: usize,
}

impl Default for OutboxCapacity {
    fn default() -> Self {
        Self {
            max_items: 10_000,
            max_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxEntry {
    pub effect: EffectRecord,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnqueueReport {
    pub inserted: usize,
    pub deduplicated: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OutboxError {
    #[error("outbox capacity exceeded ({resource}: current {current} + incoming {incoming} > limit {limit})")]
    CapacityExceeded {
        resource: &'static str,
        current: usize,
        incoming: usize,
        limit: usize,
    },
    #[error(transparent)]
    Checkpoint(#[from] CheckpointError),
    #[error("outbox storage error: {0}")]
    Storage(String),
}

/// Computes a deterministic, action-order-independent `EffectId` from stable
/// rule generation, operator identity, sequence number, and output ordinal.
pub fn deterministic_effect_id(
    rule_id: &RuleId,
    generation: Generation,
    operator_id: &OperatorId,
    action_id: &ActionId,
    sequence: u64,
    ordinal: u32,
) -> Result<EffectId, InvalidId> {
    // Use FNV-1a 64-bit over logical identities so any length fits <= 128 bytes
    // and action array reordering never changes an action's effect ID.
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for part in [
        rule_id.as_str().as_bytes(),
        b"\0",
        operator_id.as_str().as_bytes(),
        b"\0",
        action_id.as_str().as_bytes(),
    ] {
        for &b in part {
            hash ^= u64::from(b);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
    }
    let raw = format!(
        "eff_{}_{}_{}_{}_{:016x}",
        generation.0,
        sequence,
        ordinal,
        action_id.as_str(),
        hash
    );
    if raw.len() <= 128 {
        EffectId::try_from(raw)
    } else {
        EffectId::try_from(format!(
            "eff_{}_{}_{}_{:016x}",
            generation.0, sequence, ordinal, hash
        ))
    }
}

/// SQLite-backed transactional outbox v1 operating on `ReliableSqliteStore`.
pub struct SqliteOutbox<'a> {
    store: &'a ReliableSqliteStore,
    capacity: OutboxCapacity,
}

impl<'a> SqliteOutbox<'a> {
    pub fn new(store: &'a ReliableSqliteStore, capacity: OutboxCapacity) -> Self {
        Self { store, capacity }
    }

    /// Atomically persists a checkpoint (state + positions) and new outbox entries
    /// in a single SQLite transaction. Enforces item/byte limits and deduplicates
    /// repeated effect IDs idempotently.
    pub async fn commit_state_and_enqueue(
        &self,
        envelope: CheckpointEnvelope,
        entries: Vec<OutboxEntry>,
    ) -> Result<EnqueueReport, OutboxError> {
        self.store
            .commit_checkpoint_and_entries(envelope, entries, Some(self.capacity))
            .await
    }

    /// Claims up to `limit` pending (non-terminal) outbox entries owned by
    /// `(rule_id, action_id)` using [`DEFAULT_CLAIM_LEASE_MS`].
    pub async fn claim_pending(
        &self,
        rule_id: &RuleId,
        action_id: &ActionId,
        worker_id: &str,
        limit: usize,
    ) -> Result<Vec<OutboxEntry>, OutboxError> {
        self.claim_pending_with_lease(
            rule_id,
            action_id,
            worker_id,
            limit,
            DEFAULT_CLAIM_LEASE_MS,
            None,
        )
        .await
    }

    /// Atomically verifies optional active generation fencing, claims up to `limit`
    /// unclaimed or lease-expired pending entries owned by `(rule_id, action_id)`,
    /// and stamps a lease expiry timestamp.
    pub async fn claim_pending_with_lease(
        &self,
        rule_id: &RuleId,
        action_id: &ActionId,
        worker_id: &str,
        limit: usize,
        lease_duration_ms: u64,
        expected_generation: Option<Generation>,
    ) -> Result<Vec<OutboxEntry>, OutboxError> {
        let mut tx = self
            .store
            .pool()
            .begin()
            .await
            .map_err(|e| OutboxError::Storage(e.to_string()))?;

        if let Some(generation) = expected_generation {
            let row: Option<(i64, i64)> = sqlx::query_as(
                "SELECT generation, revoked FROM reliable_generations WHERE rule_id = ?",
            )
            .bind(rule_id.as_str())
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| OutboxError::Storage(e.to_string()))?;

            let Some((active_gen, revoked)) = row else {
                return Err(OutboxError::Checkpoint(
                    CheckpointError::GenerationMismatch {
                        expected: Generation(1),
                        actual: generation,
                    },
                ));
            };
            let active_u64 = u64::try_from(active_gen)
                .map_err(|_| OutboxError::Storage("corrupt negative generation".to_owned()))?;
            if revoked != 0 || generation.0 != active_u64 {
                return Err(OutboxError::Checkpoint(
                    CheckpointError::GenerationMismatch {
                        expected: Generation(active_u64.saturating_add(u64::from(revoked != 0))),
                        actual: generation,
                    },
                ));
            }
        }

        let now_ms = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(i64::MAX);
        let lease_ms = i64::try_from(lease_duration_ms).unwrap_or(i64::MAX);
        let lease_until_ms = now_ms.saturating_add(lease_ms);

        let rows = sqlx::query(
            "SELECT effect_id, rule_id, generation, action_id, connection_id, order_key, payload, attempt
             FROM reliable_outbox_effects
             WHERE rule_id = ?
               AND action_id = ?
               AND terminal_outcome_json IS NULL
               AND (claimed_by IS NULL OR claimed_until_ms IS NULL OR claimed_until_ms <= ?)
             ORDER BY rowid ASC
             LIMIT ?",
        )
        .bind(rule_id.as_str())
        .bind(action_id.as_str())
        .bind(now_ms)
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| OutboxError::Storage(e.to_string()))?;

        let mut claimed = Vec::with_capacity(rows.len());
        for row in rows {
            let eff_id_str: String = row.get("effect_id");
            let rule_str: String = row.get("rule_id");
            let gen_i64: i64 = row.get("generation");
            let act_str: String = row.get("action_id");
            let conn_str: Option<String> = row.get("connection_id");
            let order_key: Option<String> = row.get("order_key");
            let payload: Vec<u8> = row.get("payload");
            let attempt: i64 = row.get("attempt");

            let res = sqlx::query(
                "UPDATE reliable_outbox_effects
                 SET claimed_by = ?, claimed_until_ms = ?
                 WHERE effect_id = ?
                   AND terminal_outcome_json IS NULL
                   AND (claimed_by IS NULL OR claimed_until_ms IS NULL OR claimed_until_ms <= ?)",
            )
            .bind(worker_id)
            .bind(lease_until_ms)
            .bind(&eff_id_str)
            .bind(now_ms)
            .execute(&mut *tx)
            .await
            .map_err(|e| OutboxError::Storage(e.to_string()))?;

            if res.rows_affected() == 0 {
                continue;
            }

            claimed.push(OutboxEntry {
                effect: EffectRecord {
                    effect_id: EffectId::try_from(eff_id_str)
                        .map_err(|e| OutboxError::Storage(e.to_string()))?,
                    rule_id: RuleId::try_from(rule_str)
                        .map_err(|e| OutboxError::Storage(e.to_string()))?,
                    generation: Generation(u64::try_from(gen_i64).map_err(|_| {
                        OutboxError::Storage("corrupt negative generation".to_owned())
                    })?),
                    destination: EffectDestination {
                        action_id: ActionId::try_from(act_str)
                            .map_err(|e| OutboxError::Storage(e.to_string()))?,
                        connection_id: conn_str
                            .map(ConnectionId::try_from)
                            .transpose()
                            .map_err(|e| OutboxError::Storage(e.to_string()))?,
                    },
                    order_key,
                    attempt: u32::try_from(attempt)
                        .map_err(|_| OutboxError::Storage("corrupt negative attempt".to_owned()))?,
                    terminal_outcome: None,
                },
                payload,
            });
        }

        tx.commit()
            .await
            .map_err(|e| OutboxError::Storage(e.to_string()))?;
        Ok(claimed)
    }

    /// Releases an active claim held by `worker_id` so deferred or unsettled entries
    /// can be reclaimed immediately on the next dispatcher pass.
    pub async fn release_claim(
        &self,
        effect_id: &EffectId,
        worker_id: &str,
    ) -> Result<(), OutboxError> {
        sqlx::query(
            "UPDATE reliable_outbox_effects
             SET claimed_by = NULL, claimed_until_ms = NULL
             WHERE effect_id = ? AND claimed_by = ?",
        )
        .bind(effect_id.as_str())
        .bind(worker_id)
        .execute(self.store.pool())
        .await
        .map_err(|e| OutboxError::Storage(e.to_string()))?;
        Ok(())
    }

    /// Acknowledges an effect outcome idempotently.
    pub async fn ack(
        &self,
        effect_id: &EffectId,
        outcome: EffectOutcome,
    ) -> Result<Option<CompletionStatus>, OutboxError> {
        self.store
            .record_effect_completion(effect_id, outcome)
            .await
            .map_err(OutboxError::from)
    }
}
