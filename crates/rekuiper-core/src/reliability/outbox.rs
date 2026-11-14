//! Transactional SQLite outbox v1 queue with deterministic IDs, capacity
//! budgets, deduplication, and action-scoped claim/ack.

use super::sqlite::ReliableSqliteStore;
use crate::contracts::{
    ActionId, CheckpointEnvelope, CheckpointError, CompletionStatus, ConnectionId,
    EffectDestination, EffectId, EffectOutcome, EffectRecord, Generation, InvalidId, OperatorId,
    RuleId,
};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub const DEFAULT_CLAIM_LEASE_MS: u64 = 30_000;

static NEXT_CLAIM_NONCE: AtomicU64 = AtomicU64::new(1);

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboxEntry {
    pub effect: EffectRecord,
    pub payload: Vec<u8>,
    /// Unique lease token minted when this entry is claimed from the outbox.
    /// Empty before the entry has been claimed.
    #[serde(default)]
    pub claim_token: String,
}

impl OutboxEntry {
    /// Creates an unclaimed outbox entry.
    pub fn new(effect: EffectRecord, payload: Vec<u8>) -> Self {
        Self {
            effect,
            payload,
            claim_token: String::new(),
        }
    }

    /// Returns the unique lease token assigned to this claimed entry.
    pub fn claim_token(&self) -> &str {
        &self.claim_token
    }

    /// Alias for [`OutboxEntry::claim_token`].
    pub fn lease_token(&self) -> &str {
        &self.claim_token
    }
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
    #[error("outbox effect {effect_id:?} lease is not held by worker '{worker_id}'")]
    LeaseMismatch {
        effect_id: EffectId,
        worker_id: String,
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

        let batch_nonce = NEXT_CLAIM_NONCE.fetch_add(1, Ordering::Relaxed);
        let batch_prefix = format!("b{}-{}-{}", std::process::id(), now_ms, batch_nonce);
        let batch_like = format!("{batch_prefix}:%");

        let rows = sqlx::query(
            "SELECT effect_id, rule_id, generation, action_id, connection_id, order_key, payload, attempt, claim_seq
             FROM reliable_outbox_effects
             WHERE rule_id = ?
               AND action_id = ?
               AND terminal_outcome_json IS NULL
               AND (claimed_by IS NULL OR claimed_until_ms IS NULL OR claimed_until_ms <= ?)
               AND NOT EXISTS (
                   SELECT 1
                   FROM reliable_outbox_effects AS prior
                   WHERE prior.rule_id = reliable_outbox_effects.rule_id
                     AND prior.action_id = reliable_outbox_effects.action_id
                     AND reliable_outbox_effects.order_key IS NOT NULL
                     AND prior.order_key = reliable_outbox_effects.order_key
                     AND prior.terminal_outcome_json IS NULL
                     AND prior.rowid < reliable_outbox_effects.rowid
                     AND prior.claimed_by IS NOT NULL
                     AND prior.claimed_until_ms IS NOT NULL
                     AND prior.claimed_until_ms > ?
               )
             ORDER BY rowid ASC
             LIMIT ?",
        )
        .bind(rule_id.as_str())
        .bind(action_id.as_str())
        .bind(now_ms)
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
            let prev_seq: i64 = row.get("claim_seq");
            let next_seq = prev_seq.saturating_add(1);
            let row_nonce = NEXT_CLAIM_NONCE.fetch_add(1, Ordering::Relaxed);
            let claim_token = format!("{batch_prefix}:{next_seq}:{row_nonce}:{worker_id}");

            let res = sqlx::query(
                r#"UPDATE reliable_outbox_effects
                 SET claimed_by = ?, claim_token = ?, claimed_until_ms = ?, claim_seq = ?
                 WHERE effect_id = ?
                   AND claim_seq = ?
                   AND terminal_outcome_json IS NULL
                   AND (claimed_by IS NULL OR claimed_until_ms IS NULL OR claimed_until_ms <= ?)
                   AND NOT EXISTS (
                       SELECT 1
                       FROM reliable_outbox_effects AS prior
                       WHERE prior.rule_id = reliable_outbox_effects.rule_id
                         AND prior.action_id = reliable_outbox_effects.action_id
                         AND reliable_outbox_effects.order_key IS NOT NULL
                         AND prior.order_key = reliable_outbox_effects.order_key
                         AND prior.terminal_outcome_json IS NULL
                         AND prior.rowid < reliable_outbox_effects.rowid
                         AND prior.claimed_by IS NOT NULL
                         AND (prior.claim_token IS NULL OR prior.claim_token NOT LIKE ? ESCAPE '\')
                         AND prior.claimed_until_ms IS NOT NULL
                         AND prior.claimed_until_ms > ?
                   )"#,
            )
            .bind(worker_id)
            .bind(&claim_token)
            .bind(lease_until_ms)
            .bind(next_seq)
            .bind(&eff_id_str)
            .bind(prev_seq)
            .bind(now_ms)
            .bind(&batch_like)
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
                claim_token,
            });
        }

        tx.commit()
            .await
            .map_err(|e| OutboxError::Storage(e.to_string()))?;
        Ok(claimed)
    }

    /// Releases an active claim matching `claim_token` so deferred or unsettled
    /// entries can be reclaimed immediately on the next dispatcher pass.
    pub async fn release_claim(
        &self,
        effect_id: &EffectId,
        claim_token: &str,
    ) -> Result<(), OutboxError> {
        if claim_token.is_empty() {
            return Ok(());
        }
        sqlx::query(
            "UPDATE reliable_outbox_effects
             SET claimed_by = NULL, claim_token = NULL, claimed_until_ms = NULL
             WHERE effect_id = ? AND claim_token = ?",
        )
        .bind(effect_id.as_str())
        .bind(claim_token)
        .execute(self.store.pool())
        .await
        .map_err(|e| OutboxError::Storage(e.to_string()))?;
        Ok(())
    }

    /// Acknowledges an effect outcome idempotently, requiring the active `claim_token`
    /// when settling a non-terminal effect.
    pub async fn ack(
        &self,
        effect_id: &EffectId,
        claim_token: &str,
        outcome: EffectOutcome,
    ) -> Result<Option<CompletionStatus>, OutboxError> {
        self.store
            .record_claimed_effect_completion(effect_id, claim_token, outcome)
            .await
    }
}
