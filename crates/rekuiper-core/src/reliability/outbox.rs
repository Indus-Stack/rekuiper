//! Transactional SQLite outbox v1 queue with deterministic IDs, capacity
//! budgets, deduplication, and action-scoped claim/ack.

use super::sqlite::ReliableSqliteStore;
use crate::contracts::{
    ActionId, CheckpointEnvelope, CheckpointError, CompletionStatus, ConnectionId,
    EffectDestination, EffectId, EffectOutcome, EffectRecord, Generation, InvalidId, OperatorId,
    RuleId,
};
use sqlx::Row;

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
        let mut tx = self
            .store
            .pool()
            .begin()
            .await
            .map_err(|e| OutboxError::Storage(e.to_string()))?;

        let rule_id = envelope.metadata.identity.rule_id.as_str().to_owned();
        let incoming_gen = envelope.metadata.identity.generation;
        let cp_id = i64::try_from(envelope.metadata.checkpoint_id.0)
            .map_err(|_| OutboxError::Storage("checkpoint_id overflow".to_owned()))?;
        let gen_i64 = i64::try_from(incoming_gen.0)
            .map_err(|_| OutboxError::Storage("generation overflow".to_owned()))?;

        // 1. Generation fencing.
        let existing_gen: Option<(i64,)> =
            sqlx::query_as("SELECT generation FROM reliable_generations WHERE rule_id = ?")
                .bind(&rule_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| OutboxError::Storage(e.to_string()))?;
        if let Some((g,)) = existing_gen {
            let g_u64 = u64::try_from(g).unwrap_or(0);
            if incoming_gen.0 < g_u64 {
                return Err(OutboxError::Checkpoint(
                    CheckpointError::GenerationMismatch {
                        expected: Generation(g_u64),
                        actual: incoming_gen,
                    },
                ));
            }
        }

        // 2. Check pending outbox capacity before admitting new items.
        let (pending_items, pending_bytes): (i64, i64) = sqlx::query_as(
            "SELECT COUNT(*), COALESCE(SUM(LENGTH(payload)), 0)
             FROM reliable_outbox_effects
             WHERE terminal_outcome_json IS NULL",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| OutboxError::Storage(e.to_string()))?;

        let current_items = usize::try_from(pending_items).unwrap_or(usize::MAX);
        let current_bytes = usize::try_from(pending_bytes).unwrap_or(usize::MAX);
        let incoming_items = entries.len();
        let incoming_bytes: usize = entries.iter().map(|e| e.payload.len()).sum();

        if current_items.saturating_add(incoming_items) > self.capacity.max_items {
            return Err(OutboxError::CapacityExceeded {
                resource: "items",
                current: current_items,
                incoming: incoming_items,
                limit: self.capacity.max_items,
            });
        }
        if current_bytes.saturating_add(incoming_bytes) > self.capacity.max_bytes {
            return Err(OutboxError::CapacityExceeded {
                resource: "bytes",
                current: current_bytes,
                incoming: incoming_bytes,
                limit: self.capacity.max_bytes,
            });
        }

        // 3. Persist generation, checkpoint metadata, positions, and participant state.
        sqlx::query(
            "INSERT INTO reliable_generations (rule_id, generation) VALUES (?, ?)
             ON CONFLICT(rule_id) DO UPDATE SET generation = excluded.generation",
        )
        .bind(&rule_id)
        .bind(gen_i64)
        .execute(&mut *tx)
        .await
        .map_err(|e| OutboxError::Storage(e.to_string()))?;

        let metadata_json = serde_json::to_string(&envelope.metadata)
            .map_err(|e| OutboxError::Storage(e.to_string()))?;
        sqlx::query(
            "INSERT OR REPLACE INTO reliable_checkpoints
             (rule_id, checkpoint_id, generation, metadata_json, committed_at_ms)
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&rule_id)
        .bind(cp_id)
        .bind(gen_i64)
        .bind(metadata_json)
        .bind(envelope.metadata.created_wall_millis)
        .execute(&mut *tx)
        .await
        .map_err(|e| OutboxError::Storage(e.to_string()))?;

        for pos in &envelope.metadata.source_positions {
            sqlx::query(
                "INSERT OR REPLACE INTO reliable_positions
                 (rule_id, checkpoint_id, source_id, partition_id, generation, codec, cursor)
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&rule_id)
            .bind(cp_id)
            .bind(pos.partition.source_id.as_str())
            .bind(pos.partition.partition_id.as_str())
            .bind(i64::try_from(pos.generation.0).unwrap_or(i64::MAX))
            .bind(pos.codec.as_str())
            .bind(&pos.cursor)
            .execute(&mut *tx)
            .await
            .map_err(|e| OutboxError::Storage(e.to_string()))?;
        }

        for (op_id, snapshot) in &envelope.payloads {
            sqlx::query(
                "INSERT OR REPLACE INTO reliable_participants
                 (rule_id, checkpoint_id, operator_id, envelope_version, state_version, checksum, payload, records, bytes)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&rule_id)
            .bind(cp_id)
            .bind(op_id.as_str())
            .bind(i64::from(snapshot.envelope_version))
            .bind(i64::from(snapshot.state_version.0))
            .bind(snapshot.checksum.as_str())
            .bind(&snapshot.payload)
            .bind(i64::try_from(snapshot.usage.records).unwrap_or(i64::MAX))
            .bind(i64::try_from(snapshot.usage.bytes).unwrap_or(i64::MAX))
            .execute(&mut *tx)
            .await
            .map_err(|e| OutboxError::Storage(e.to_string()))?;
        }

        // 4. Insert outbox entries idempotently (INSERT OR IGNORE).
        let mut inserted = 0usize;
        let mut deduplicated = 0usize;
        for entry in entries {
            let outcome_json = entry
                .effect
                .terminal_outcome
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(|e| OutboxError::Storage(e.to_string()))?;
            let res = sqlx::query(
                "INSERT OR IGNORE INTO reliable_outbox_effects
                 (effect_id, rule_id, generation, action_id, connection_id, order_key, payload, claimed_by, attempt, terminal_outcome_json)
                 VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)",
            )
            .bind(entry.effect.effect_id.as_str())
            .bind(entry.effect.rule_id.as_str())
            .bind(i64::try_from(entry.effect.generation.0).unwrap_or(i64::MAX))
            .bind(entry.effect.destination.action_id.as_str())
            .bind(entry.effect.destination.connection_id.as_ref().map(|c| c.as_str()))
            .bind(entry.effect.order_key.as_deref())
            .bind(&entry.payload)
            .bind(i64::from(entry.effect.attempt))
            .bind(outcome_json)
            .execute(&mut *tx)
            .await
            .map_err(|e| OutboxError::Storage(e.to_string()))?;
            if res.rows_affected() > 0 {
                inserted += 1;
            } else {
                deduplicated += 1;
            }
        }

        if self.store.take_commit_failure() {
            tx.rollback()
                .await
                .map_err(|e| OutboxError::Storage(e.to_string()))?;
            return Err(OutboxError::Checkpoint(CheckpointError::StorageFailure(
                "injected commit rollback".to_owned(),
            )));
        }

        tx.commit()
            .await
            .map_err(|e| OutboxError::Storage(e.to_string()))?;
        Ok(EnqueueReport {
            inserted,
            deduplicated,
        })
    }

    /// Claims up to `limit` pending (non-terminal) outbox entries owned by
    /// `(rule_id, action_id)`. Action order never changes ownership.
    pub async fn claim_pending(
        &self,
        rule_id: &RuleId,
        action_id: &ActionId,
        worker_id: &str,
        limit: usize,
    ) -> Result<Vec<OutboxEntry>, OutboxError> {
        let mut tx = self
            .store
            .pool()
            .begin()
            .await
            .map_err(|e| OutboxError::Storage(e.to_string()))?;

        let rows = sqlx::query(
            "SELECT effect_id, rule_id, generation, action_id, connection_id, order_key, payload, attempt
             FROM reliable_outbox_effects
             WHERE rule_id = ? AND action_id = ? AND terminal_outcome_json IS NULL
             ORDER BY rowid ASC
             LIMIT ?",
        )
        .bind(rule_id.as_str())
        .bind(action_id.as_str())
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

            sqlx::query("UPDATE reliable_outbox_effects SET claimed_by = ? WHERE effect_id = ?")
                .bind(worker_id)
                .bind(&eff_id_str)
                .execute(&mut *tx)
                .await
                .map_err(|e| OutboxError::Storage(e.to_string()))?;

            claimed.push(OutboxEntry {
                effect: EffectRecord {
                    effect_id: EffectId::try_from(eff_id_str)
                        .map_err(|e| OutboxError::Storage(e.to_string()))?,
                    rule_id: RuleId::try_from(rule_str)
                        .map_err(|e| OutboxError::Storage(e.to_string()))?,
                    generation: Generation(u64::try_from(gen_i64).unwrap_or(0)),
                    destination: EffectDestination {
                        action_id: ActionId::try_from(act_str)
                            .map_err(|e| OutboxError::Storage(e.to_string()))?,
                        connection_id: conn_str
                            .map(ConnectionId::try_from)
                            .transpose()
                            .map_err(|e| OutboxError::Storage(e.to_string()))?,
                    },
                    order_key,
                    attempt: u32::try_from(attempt).unwrap_or(0),
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
