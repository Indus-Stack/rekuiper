//! Generation fencing and stop/update/delete lifecycle revocation.

use super::outbox::{OutboxEntry, OutboxError, SqliteOutbox, DEFAULT_CLAIM_LEASE_MS};
use super::sqlite::ReliableSqliteStore;
use crate::contracts::{
    ActionId, CheckpointError, DeadlineCancellation, EffectOutcome, EffectRecord, Generation,
    RuleId,
};
use sqlx::Row;

/// Serializes generation activation and revocation with reliable SQLite
/// checkpoints and outbox claims so stale writers/dispatchers cannot act.
pub struct GenerationFence<'a> {
    store: &'a ReliableSqliteStore,
}

impl<'a> GenerationFence<'a> {
    pub fn new(store: &'a ReliableSqliteStore) -> Self {
        Self { store }
    }

    /// Activates the next monotonically increasing generation for `rule_id` and
    /// returns a fresh `DeadlineCancellation` token.
    pub async fn activate_next(
        &self,
        rule_id: &RuleId,
    ) -> Result<(Generation, DeadlineCancellation), CheckpointError> {
        let mut tx = self
            .store
            .pool()
            .begin()
            .await
            .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?;

        let existing: Option<(i64,)> =
            sqlx::query_as("SELECT generation FROM reliable_generations WHERE rule_id = ?")
                .bind(rule_id.as_str())
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?;

        let next_gen = match existing {
            Some((g,)) => u64::try_from(g)
                .map_err(|_| {
                    CheckpointError::StorageFailure("corrupt negative generation".to_owned())
                })?
                .checked_add(1)
                .ok_or_else(|| CheckpointError::StorageFailure("generation overflow".to_owned()))?,
            None => 1,
        };
        let next_gen_i64 = i64::try_from(next_gen)
            .map_err(|_| CheckpointError::StorageFailure("generation overflow".to_owned()))?;

        sqlx::query(
            "INSERT INTO reliable_generations (rule_id, generation, revoked) VALUES (?, ?, 0)
             ON CONFLICT(rule_id) DO UPDATE SET generation = excluded.generation, revoked = 0",
        )
        .bind(rule_id.as_str())
        .bind(next_gen_i64)
        .execute(&mut *tx)
        .await
        .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?;

        tx.commit()
            .await
            .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?;

        Ok((Generation(next_gen), DeadlineCancellation::default()))
    }

    /// Cancels the active runtime token and persists revocation of the current
    /// generation before any replacement starts or after stop/delete.
    /// Does NOT discard already-durable pending or ambiguous outbox effects.
    pub async fn fence_and_revoke(
        &self,
        rule_id: &RuleId,
        cancel: &DeadlineCancellation,
    ) -> Result<Generation, CheckpointError> {
        cancel.cancel();

        let mut tx = self
            .store
            .pool()
            .begin()
            .await
            .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?;

        let existing: Option<(i64,)> =
            sqlx::query_as("SELECT generation FROM reliable_generations WHERE rule_id = ?")
                .bind(rule_id.as_str())
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?;

        let current_gen = match existing {
            Some((g,)) => u64::try_from(g).map_err(|_| {
                CheckpointError::StorageFailure("corrupt negative generation".to_owned())
            })?,
            None => 0,
        };
        let current_gen_i64 = i64::try_from(current_gen)
            .map_err(|_| CheckpointError::StorageFailure("generation overflow".to_owned()))?;
        sqlx::query(
            "INSERT INTO reliable_generations (rule_id, generation, revoked) VALUES (?, ?, 1)
             ON CONFLICT(rule_id) DO UPDATE SET revoked = 1",
        )
        .bind(rule_id.as_str())
        .bind(current_gen_i64)
        .execute(&mut *tx)
        .await
        .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?;

        tx.commit()
            .await
            .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?;

        Ok(Generation(current_gen))
    }

    /// Verifies that `generation` is currently active and unrevoked.
    pub async fn assert_active(
        &self,
        rule_id: &RuleId,
        generation: Generation,
    ) -> Result<(), CheckpointError> {
        let row: Option<(i64, i64)> = sqlx::query_as(
            "SELECT generation, revoked FROM reliable_generations WHERE rule_id = ?",
        )
        .bind(rule_id.as_str())
        .fetch_optional(self.store.pool())
        .await
        .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?;

        let Some((active_gen, revoked)) = row else {
            return Err(CheckpointError::GenerationMismatch {
                expected: Generation(1),
                actual: generation,
            });
        };
        let active_u64 = u64::try_from(active_gen).map_err(|_| {
            CheckpointError::StorageFailure("corrupt negative generation".to_owned())
        })?;
        if revoked != 0 || generation.0 != active_u64 {
            return Err(CheckpointError::GenerationMismatch {
                expected: Generation(active_u64.saturating_add(u64::from(revoked != 0))),
                actual: generation,
            });
        }
        Ok(())
    }

    /// Claims pending outbox effects only if `generation` is still active and
    /// unrevoked inside the same SQLite transaction.
    pub async fn claim_pending_fenced(
        &self,
        outbox: &SqliteOutbox<'_>,
        rule_id: &RuleId,
        action_id: &ActionId,
        generation: Generation,
        worker_id: &str,
        limit: usize,
    ) -> Result<Vec<OutboxEntry>, OutboxError> {
        outbox
            .claim_pending_with_lease(
                rule_id,
                action_id,
                worker_id,
                limit,
                DEFAULT_CLAIM_LEASE_MS,
                Some(generation),
            )
            .await
    }

    /// Lists durable outbox effects for `rule_id` that remain pending or
    /// `Ambiguous`, even after the rule generation has been fenced/revoked.
    pub async fn list_unsettled_effects(
        &self,
        rule_id: &RuleId,
    ) -> Result<Vec<EffectRecord>, CheckpointError> {
        let rows = sqlx::query(
            "SELECT effect_id, rule_id, generation, action_id, connection_id, order_key, attempt, terminal_outcome_json
             FROM reliable_outbox_effects
             WHERE rule_id = ?
             ORDER BY rowid ASC",
        )
        .bind(rule_id.as_str())
        .fetch_all(self.store.pool())
        .await
        .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?;

        let mut unsettled = Vec::new();
        for row in rows {
            let outcome_json: Option<String> = row.get("terminal_outcome_json");
            let outcome: Option<EffectOutcome> = outcome_json
                .map(|j| serde_json::from_str(&j))
                .transpose()
                .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?;
            if outcome.as_ref().is_some_and(EffectOutcome::is_terminal) {
                continue;
            }
            let eff_id_str: String = row.get("effect_id");
            let gen_i64: i64 = row.get("generation");
            let act_str: String = row.get("action_id");
            let conn_str: Option<String> = row.get("connection_id");
            let order_key: Option<String> = row.get("order_key");
            let attempt: i64 = row.get("attempt");

            unsettled.push(EffectRecord {
                effect_id: crate::contracts::EffectId::try_from(eff_id_str)
                    .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?,
                rule_id: rule_id.clone(),
                generation: Generation(u64::try_from(gen_i64).map_err(|_| {
                    CheckpointError::StorageFailure("corrupt negative generation".to_owned())
                })?),
                destination: crate::contracts::EffectDestination {
                    action_id: ActionId::try_from(act_str)
                        .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?,
                    connection_id: conn_str
                        .map(crate::contracts::ConnectionId::try_from)
                        .transpose()
                        .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?,
                },
                order_key,
                attempt: u32::try_from(attempt).map_err(|_| {
                    CheckpointError::StorageFailure("corrupt negative attempt".to_owned())
                })?,
                terminal_outcome: outcome,
            });
        }
        Ok(unsettled)
    }
}
