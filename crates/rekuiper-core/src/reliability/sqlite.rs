//! Atomic, generation-fenced SQLite checkpoint and outbox v1 store.

use super::outbox::{EnqueueReport, OutboxCapacity, OutboxEntry, OutboxError};
use crate::contracts::{
    ActionId, CheckpointEnvelope, CheckpointError, CheckpointId, CheckpointMetadata,
    CheckpointStore, CompletionStatus, ConnectionId, ContentChecksum, EffectDestination, EffectId,
    EffectOutcome, EffectRecord, Generation, OperatorId, PartitionId, PositionCodecId, RuleId,
    SourceId, SourcePartition, SourcePosition, StateResourceUsage, StateSchemaVersion,
    StateSnapshot,
};
use async_trait::async_trait;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};
use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};

pub const RELIABLE_SQLITE_SCHEMA_VERSION: u32 = 1;
pub const DEFAULT_CHECKPOINT_RETENTION: usize = 5;
pub const DEFAULT_OUTBOX_RETENTION: usize = 1000;

/// Synchronous durability level enforced on the reliable SQLite connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqliteDurabilityMode {
    Full,
    Extra,
}

impl SqliteDurabilityMode {
    fn pragma_name(self) -> &'static str {
        match self {
            Self::Full => "FULL",
            Self::Extra => "EXTRA",
        }
    }

    fn pragma_code(self) -> i64 {
        match self {
            Self::Full => 2,
            Self::Extra => 3,
        }
    }
}

/// Verified PRAGMA settings read back from SQLite after configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedSqliteDurability {
    pub journal_mode: String,
    pub synchronous: i64,
    pub foreign_keys: i64,
}

/// Versioned SQLite store keeping checkpoints, participant snapshots, source
/// positions, and outbox effects in one atomic SQLite transaction.
pub struct ReliableSqliteStore {
    pool: SqlitePool,
    verified_durability: VerifiedSqliteDurability,
    fail_next_commit: AtomicBool,
    checkpoint_retention: usize,
    outbox_retention: usize,
}

impl ReliableSqliteStore {
    /// Opens a file-backed or `:memory:` reliable SQLite store, enforces and
    /// verifies durability PRAGMAs, and migrates versioned tables.
    pub async fn open(
        path: impl AsRef<Path>,
        durability: SqliteDurabilityMode,
    ) -> Result<Self, CheckpointError> {
        let path = path.as_ref();
        let is_memory = path.as_os_str() == ":memory:";
        if !is_memory {
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent)
                        .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?;
                }
            }
        }

        let db_url = if is_memory {
            "sqlite::memory:".to_owned()
        } else {
            format!(
                "sqlite:{}?mode=rwc",
                path.to_string_lossy().replace('\\', "/")
            )
        };

        let options = SqliteConnectOptions::from_str(&db_url)
            .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?
            .create_if_missing(true);

        // Use max_connections(1) so per-connection PRAGMAs (synchronous, foreign_keys)
        // apply deterministically to the single reliable writer/reader connection.
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?;

        let pragmas = format!(
            "PRAGMA journal_mode = {}; PRAGMA synchronous = {}; PRAGMA foreign_keys = ON; PRAGMA busy_timeout = 5000;",
            if is_memory { "MEMORY" } else { "WAL" },
            durability.pragma_name()
        );
        sqlx::query(&pragmas)
            .execute(&pool)
            .await
            .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?;

        let (journal_mode,): (String,) = sqlx::query_as("PRAGMA journal_mode;")
            .fetch_one(&pool)
            .await
            .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?;
        let (synchronous,): (i64,) = sqlx::query_as("PRAGMA synchronous;")
            .fetch_one(&pool)
            .await
            .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?;
        let (foreign_keys,): (i64,) = sqlx::query_as("PRAGMA foreign_keys;")
            .fetch_one(&pool)
            .await
            .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?;

        let expected_journal = if is_memory { "memory" } else { "wal" };
        if journal_mode.to_ascii_lowercase() != expected_journal {
            return Err(CheckpointError::StorageFailure(format!(
                "expected journal_mode={expected_journal}, got {journal_mode}"
            )));
        }
        if synchronous != durability.pragma_code() {
            return Err(CheckpointError::StorageFailure(format!(
                "expected synchronous={}, got {synchronous}",
                durability.pragma_code()
            )));
        }
        if foreign_keys != 1 {
            return Err(CheckpointError::StorageFailure(format!(
                "expected foreign_keys=1, got {foreign_keys}"
            )));
        }

        let store = Self {
            pool,
            verified_durability: VerifiedSqliteDurability {
                journal_mode,
                synchronous,
                foreign_keys,
            },
            fail_next_commit: AtomicBool::new(false),
            checkpoint_retention: DEFAULT_CHECKPOINT_RETENTION,
            outbox_retention: DEFAULT_OUTBOX_RETENTION,
        };
        store.migrate().await?;
        Ok(store)
    }

    /// Configures per-rule retention limits for checkpoints and settled outbox rows.
    /// `max_checkpoints_per_rule` is clamped to at least 1 so the active checkpoint is preserved.
    pub fn with_retention(
        mut self,
        max_checkpoints_per_rule: usize,
        max_settled_outbox_per_rule: usize,
    ) -> Self {
        self.checkpoint_retention = max_checkpoints_per_rule.max(1);
        self.outbox_retention = max_settled_outbox_per_rule;
        self
    }

    pub fn verified_durability(&self) -> &VerifiedSqliteDurability {
        &self.verified_durability
    }

    pub(crate) fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Arms a one-shot failure right before transaction commit to test rollback.
    pub fn arm_commit_failure(&self) {
        self.fail_next_commit.store(true, Ordering::SeqCst);
    }

    pub(crate) fn take_commit_failure(&self) -> bool {
        self.fail_next_commit.swap(false, Ordering::SeqCst)
    }

    async fn migrate(&self) -> Result<(), CheckpointError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?;

        let ddl = [
            "CREATE TABLE IF NOT EXISTS reliable_schema_meta (
                version INTEGER PRIMARY KEY
            )",
            "INSERT OR IGNORE INTO reliable_schema_meta (version) VALUES (1)",
            "CREATE TABLE IF NOT EXISTS reliable_generations (
                rule_id TEXT PRIMARY KEY,
                generation INTEGER NOT NULL,
                revoked INTEGER NOT NULL DEFAULT 0
            )",
            "CREATE TABLE IF NOT EXISTS reliable_checkpoints (
                rule_id TEXT NOT NULL,
                checkpoint_id INTEGER NOT NULL,
                generation INTEGER NOT NULL,
                metadata_json TEXT NOT NULL,
                committed_at_ms INTEGER NOT NULL,
                PRIMARY KEY (rule_id, checkpoint_id)
            )",
            "CREATE TABLE IF NOT EXISTS reliable_positions (
                rule_id TEXT NOT NULL,
                checkpoint_id INTEGER NOT NULL,
                source_id TEXT NOT NULL,
                partition_id TEXT NOT NULL,
                generation INTEGER NOT NULL,
                codec TEXT NOT NULL,
                cursor BLOB NOT NULL,
                PRIMARY KEY (rule_id, checkpoint_id, source_id, partition_id),
                FOREIGN KEY (rule_id, checkpoint_id)
                    REFERENCES reliable_checkpoints (rule_id, checkpoint_id)
                    ON DELETE CASCADE
            )",
            "CREATE TABLE IF NOT EXISTS reliable_participants (
                rule_id TEXT NOT NULL,
                checkpoint_id INTEGER NOT NULL,
                operator_id TEXT NOT NULL,
                envelope_version INTEGER NOT NULL,
                state_version INTEGER NOT NULL,
                checksum TEXT NOT NULL,
                payload BLOB NOT NULL,
                records INTEGER NOT NULL,
                bytes INTEGER NOT NULL,
                PRIMARY KEY (rule_id, checkpoint_id, operator_id),
                FOREIGN KEY (rule_id, checkpoint_id)
                    REFERENCES reliable_checkpoints (rule_id, checkpoint_id)
                    ON DELETE CASCADE
            )",
            "CREATE TABLE IF NOT EXISTS reliable_outbox_effects (
                effect_id TEXT PRIMARY KEY,
                rule_id TEXT NOT NULL,
                generation INTEGER NOT NULL,
                action_id TEXT NOT NULL,
                connection_id TEXT,
                order_key TEXT,
                payload BLOB NOT NULL DEFAULT X'',
                claimed_by TEXT,
                claimed_until_ms INTEGER,
                attempt INTEGER NOT NULL,
                terminal_outcome_json TEXT
            )",
        ];

        for stmt in ddl {
            sqlx::query(stmt)
                .execute(&mut *tx)
                .await
                .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?;
        }

        let columns = sqlx::query("PRAGMA table_info(reliable_outbox_effects)")
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?;
        let has_claimed_until = columns
            .iter()
            .any(|row| row.get::<String, _>("name") == "claimed_until_ms");
        if !has_claimed_until {
            sqlx::query("ALTER TABLE reliable_outbox_effects ADD COLUMN claimed_until_ms INTEGER")
                .execute(&mut *tx)
                .await
                .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?;
        }

        tx.commit()
            .await
            .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?;
        Ok(())
    }

    /// Atomically commits a checkpoint, its participant snapshots, source positions,
    /// and outbox effects with generation fencing.
    pub async fn commit_with_outbox(
        &self,
        envelope: CheckpointEnvelope,
        effects: Vec<EffectRecord>,
    ) -> Result<(), CheckpointError> {
        let entries: Vec<OutboxEntry> = effects
            .into_iter()
            .map(|effect| OutboxEntry {
                effect,
                payload: Vec::new(),
            })
            .collect();
        self.commit_checkpoint_and_entries(envelope, entries, None)
            .await
            .map(|_| ())
            .map_err(|err| match err {
                OutboxError::Checkpoint(ce) => ce,
                OutboxError::Storage(msg) => CheckpointError::StorageFailure(msg),
                other @ OutboxError::CapacityExceeded { .. } => {
                    CheckpointError::StorageFailure(other.to_string())
                }
            })
    }

    pub(crate) async fn commit_checkpoint_and_entries(
        &self,
        envelope: CheckpointEnvelope,
        entries: Vec<OutboxEntry>,
        capacity: Option<OutboxCapacity>,
    ) -> Result<EnqueueReport, OutboxError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| OutboxError::Storage(e.to_string()))?;

        let rule_id = envelope.metadata.identity.rule_id.as_str().to_owned();
        let incoming_gen = envelope.metadata.identity.generation;
        let cp_id = i64::try_from(envelope.metadata.checkpoint_id.0).map_err(|_| {
            OutboxError::Checkpoint(CheckpointError::StorageFailure(
                "checkpoint_id overflow".to_owned(),
            ))
        })?;
        let gen_i64 = i64::try_from(incoming_gen.0).map_err(|_| {
            OutboxError::Checkpoint(CheckpointError::StorageFailure(
                "generation overflow".to_owned(),
            ))
        })?;

        // 1. Atomic generation fence check (strict equality once a generation exists).
        let existing: Option<(i64, i64)> = sqlx::query_as(
            "SELECT generation, revoked FROM reliable_generations WHERE rule_id = ?",
        )
        .bind(&rule_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| OutboxError::Storage(e.to_string()))?;

        match existing {
            Some((existing_gen, revoked)) => {
                let existing_gen_u64 = u64::try_from(existing_gen).map_err(|_| {
                    OutboxError::Checkpoint(CheckpointError::StorageFailure(
                        "corrupt generation".to_owned(),
                    ))
                })?;
                if revoked != 0 || incoming_gen.0 != existing_gen_u64 {
                    return Err(OutboxError::Checkpoint(
                        CheckpointError::GenerationMismatch {
                            expected: Generation(
                                existing_gen_u64.saturating_add(u64::from(revoked != 0)),
                            ),
                            actual: incoming_gen,
                        },
                    ));
                }
            }
            None => {
                sqlx::query(
                    "INSERT INTO reliable_generations (rule_id, generation, revoked) VALUES (?, ?, 0)",
                )
                .bind(&rule_id)
                .bind(gen_i64)
                .execute(&mut *tx)
                .await
                .map_err(|e| OutboxError::Storage(e.to_string()))?;
            }
        }

        // 2. Per-rule capacity check before admitting genuinely new non-terminal entries.
        if let Some(cap) = capacity {
            let mut seen_ids = HashSet::with_capacity(entries.len());
            let mut new_by_rule: BTreeMap<&str, (usize, usize)> = BTreeMap::new();
            for entry in &entries {
                if !seen_ids.insert(entry.effect.effect_id.as_str()) {
                    continue;
                }
                let exists: Option<(i64,)> =
                    sqlx::query_as("SELECT 1 FROM reliable_outbox_effects WHERE effect_id = ?")
                        .bind(entry.effect.effect_id.as_str())
                        .fetch_optional(&mut *tx)
                        .await
                        .map_err(|e| OutboxError::Storage(e.to_string()))?;
                if exists.is_some() || entry.effect.terminal_outcome.is_some() {
                    continue;
                }
                let slot = new_by_rule
                    .entry(entry.effect.rule_id.as_str())
                    .or_insert((0, 0));
                slot.0 = slot.0.saturating_add(1);
                slot.1 = slot.1.saturating_add(entry.payload.len());
            }

            for (target_rule, (incoming_items, incoming_bytes)) in new_by_rule {
                let (pending_items, pending_bytes): (i64, i64) = sqlx::query_as(
                    "SELECT COUNT(*), COALESCE(SUM(LENGTH(payload)), 0)
                     FROM reliable_outbox_effects
                     WHERE rule_id = ? AND terminal_outcome_json IS NULL",
                )
                .bind(target_rule)
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| OutboxError::Storage(e.to_string()))?;

                let current_items = usize::try_from(pending_items).unwrap_or(usize::MAX);
                let current_bytes = usize::try_from(pending_bytes).unwrap_or(usize::MAX);
                if current_items.saturating_add(incoming_items) > cap.max_items {
                    return Err(OutboxError::CapacityExceeded {
                        resource: "items",
                        current: current_items,
                        incoming: incoming_items,
                        limit: cap.max_items,
                    });
                }
                if current_bytes.saturating_add(incoming_bytes) > cap.max_bytes {
                    return Err(OutboxError::CapacityExceeded {
                        resource: "bytes",
                        current: current_bytes,
                        incoming: incoming_bytes,
                        limit: cap.max_bytes,
                    });
                }
            }
        }

        // 3. Persist checkpoint metadata, positions, and participant state.
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
            let pos_gen = i64::try_from(pos.generation.0).map_err(|_| {
                OutboxError::Checkpoint(CheckpointError::StorageFailure(
                    "generation overflow".to_owned(),
                ))
            })?;
            sqlx::query(
                "INSERT OR REPLACE INTO reliable_positions
                 (rule_id, checkpoint_id, source_id, partition_id, generation, codec, cursor)
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&rule_id)
            .bind(cp_id)
            .bind(pos.partition.source_id.as_str())
            .bind(pos.partition.partition_id.as_str())
            .bind(pos_gen)
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

        // 4. Insert outbox entries idempotently without overwriting existing/delivered outcomes.
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
            let eff_gen = i64::try_from(entry.effect.generation.0).map_err(|_| {
                OutboxError::Checkpoint(CheckpointError::StorageFailure(
                    "generation overflow".to_owned(),
                ))
            })?;

            let res = sqlx::query(
                "INSERT INTO reliable_outbox_effects
                 (effect_id, rule_id, generation, action_id, connection_id, order_key, payload, claimed_by, claimed_until_ms, attempt, terminal_outcome_json)
                 VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)
                 ON CONFLICT(effect_id) DO NOTHING",
            )
            .bind(entry.effect.effect_id.as_str())
            .bind(entry.effect.rule_id.as_str())
            .bind(eff_gen)
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

        // 5. Enforce bounded retention per rule inside the commit transaction.
        let cp_keep = i64::try_from(self.checkpoint_retention).unwrap_or(i64::MAX);
        sqlx::query(
            "DELETE FROM reliable_checkpoints
             WHERE rule_id = ?
               AND checkpoint_id NOT IN (
                   SELECT checkpoint_id FROM reliable_checkpoints
                   WHERE rule_id = ?
                   ORDER BY checkpoint_id DESC
                   LIMIT ?
               )",
        )
        .bind(&rule_id)
        .bind(&rule_id)
        .bind(cp_keep)
        .execute(&mut *tx)
        .await
        .map_err(|e| OutboxError::Storage(e.to_string()))?;

        let outbox_keep = i64::try_from(self.outbox_retention).unwrap_or(i64::MAX);
        sqlx::query(
            "DELETE FROM reliable_outbox_effects
             WHERE rule_id = ?
               AND terminal_outcome_json IS NOT NULL
               AND rowid NOT IN (
                   SELECT rowid FROM reliable_outbox_effects
                   WHERE rule_id = ? AND terminal_outcome_json IS NOT NULL
                   ORDER BY rowid DESC
                   LIMIT ?
               )",
        )
        .bind(&rule_id)
        .bind(&rule_id)
        .bind(outbox_keep)
        .execute(&mut *tx)
        .await
        .map_err(|e| OutboxError::Storage(e.to_string()))?;

        if self.take_commit_failure() {
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

    /// Loads an outbox effect record by ID.
    pub async fn outbox_record(
        &self,
        effect_id: &EffectId,
    ) -> Result<Option<EffectRecord>, CheckpointError> {
        let row = sqlx::query(
            "SELECT effect_id, rule_id, generation, action_id, connection_id, order_key, attempt, terminal_outcome_json
             FROM reliable_outbox_effects WHERE effect_id = ?",
        )
        .bind(effect_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?;

        let Some(row) = row else {
            return Ok(None);
        };
        Self::decode_effect_row(&row).map(Some)
    }

    /// Records an effect completion idempotently in SQLite.
    pub async fn record_effect_completion(
        &self,
        effect_id: &EffectId,
        outcome: EffectOutcome,
    ) -> Result<Option<CompletionStatus>, CheckpointError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?;

        let row = sqlx::query(
            "SELECT effect_id, rule_id, generation, action_id, connection_id, order_key, attempt, terminal_outcome_json
             FROM reliable_outbox_effects WHERE effect_id = ?",
        )
        .bind(effect_id.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?;

        let Some(row) = row else {
            return Ok(None);
        };
        let mut record = Self::decode_effect_row(&row)?;
        let status = record.record_completion(outcome);

        if status == CompletionStatus::Applied {
            let outcome_json = record
                .terminal_outcome
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?;
            sqlx::query(
                "UPDATE reliable_outbox_effects
                 SET attempt = ?, terminal_outcome_json = ?, claimed_by = NULL, claimed_until_ms = NULL
                 WHERE effect_id = ?",
            )
            .bind(i64::from(record.attempt))
            .bind(outcome_json)
            .bind(effect_id.as_str())
            .execute(&mut *tx)
            .await
            .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?;
        }

        tx.commit()
            .await
            .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?;
        Ok(Some(status))
    }

    fn decode_effect_row(row: &sqlx::sqlite::SqliteRow) -> Result<EffectRecord, CheckpointError> {
        let effect_id: String = row.get("effect_id");
        let rule_id: String = row.get("rule_id");
        let generation: i64 = row.get("generation");
        let action_id: String = row.get("action_id");
        let connection_id: Option<String> = row.get("connection_id");
        let order_key: Option<String> = row.get("order_key");
        let attempt: i64 = row.get("attempt");
        let terminal_outcome_json: Option<String> = row.get("terminal_outcome_json");

        let terminal_outcome = terminal_outcome_json
            .map(|json| serde_json::from_str(&json))
            .transpose()
            .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?;

        Ok(EffectRecord {
            effect_id: EffectId::try_from(effect_id)
                .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?,
            rule_id: RuleId::try_from(rule_id)
                .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?,
            generation: Generation(
                u64::try_from(generation).map_err(|_| {
                    CheckpointError::StorageFailure("invalid generation".to_owned())
                })?,
            ),
            destination: EffectDestination {
                action_id: ActionId::try_from(action_id)
                    .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?,
                connection_id: connection_id
                    .map(ConnectionId::try_from)
                    .transpose()
                    .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?,
            },
            order_key,
            attempt: u32::try_from(attempt)
                .map_err(|_| CheckpointError::StorageFailure("invalid attempt".to_owned()))?,
            terminal_outcome,
        })
    }
}

#[async_trait]
impl CheckpointStore for ReliableSqliteStore {
    async fn commit_checkpoint(&self, envelope: CheckpointEnvelope) -> Result<(), CheckpointError> {
        self.commit_with_outbox(envelope, Vec::new()).await
    }

    async fn load_latest(
        &self,
        rule_id: &RuleId,
    ) -> Result<Option<CheckpointEnvelope>, CheckpointError> {
        let row = sqlx::query(
            "SELECT checkpoint_id, metadata_json
             FROM reliable_checkpoints
             WHERE rule_id = ?
             ORDER BY checkpoint_id DESC
             LIMIT 1",
        )
        .bind(rule_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?;

        let Some(row) = row else {
            return Ok(None);
        };
        let cp_id: i64 = row.get("checkpoint_id");
        let metadata_json: String = row.get("metadata_json");
        let mut metadata: CheckpointMetadata = serde_json::from_str(&metadata_json)
            .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?;

        // Reconstruct source positions from normalized table to ensure relational integrity.
        let pos_rows = sqlx::query(
            "SELECT source_id, partition_id, generation, codec, cursor
             FROM reliable_positions
             WHERE rule_id = ? AND checkpoint_id = ?
             ORDER BY source_id, partition_id",
        )
        .bind(rule_id.as_str())
        .bind(cp_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?;

        let mut positions = Vec::with_capacity(pos_rows.len());
        for prow in pos_rows {
            let source_id: String = prow.get("source_id");
            let partition_id: String = prow.get("partition_id");
            let generation: i64 = prow.get("generation");
            let codec: String = prow.get("codec");
            let cursor: Vec<u8> = prow.get("cursor");
            positions.push(SourcePosition {
                partition: SourcePartition {
                    source_id: SourceId::try_from(source_id)
                        .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?,
                    partition_id: PartitionId::try_from(partition_id)
                        .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?,
                },
                generation: Generation(u64::try_from(generation).map_err(|_| {
                    CheckpointError::StorageFailure("invalid position generation".to_owned())
                })?),
                codec: PositionCodecId::try_from(codec)
                    .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?,
                cursor,
            });
        }
        if !positions.is_empty() {
            metadata.source_positions = positions;
        }
        metadata.checkpoint_id =
            CheckpointId(u64::try_from(cp_id).map_err(|_| {
                CheckpointError::StorageFailure("invalid checkpoint_id".to_owned())
            })?);

        let part_rows = sqlx::query(
            "SELECT operator_id, envelope_version, state_version, checksum, payload, records, bytes
             FROM reliable_participants
             WHERE rule_id = ? AND checkpoint_id = ?",
        )
        .bind(rule_id.as_str())
        .bind(cp_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?;

        let mut payloads = BTreeMap::new();
        for part in part_rows {
            let op_str: String = part.get("operator_id");
            let op_id = OperatorId::try_from(op_str)
                .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?;
            let env_ver: i64 = part.get("envelope_version");
            let state_ver: i64 = part.get("state_version");
            let checksum_str: String = part.get("checksum");
            let payload: Vec<u8> = part.get("payload");
            let records: i64 = part.get("records");
            let bytes: i64 = part.get("bytes");

            let snapshot = StateSnapshot {
                envelope_version: u32::try_from(env_ver).map_err(|_| {
                    CheckpointError::StorageFailure("invalid envelope_version".to_owned())
                })?,
                operator_id: op_id.clone(),
                state_version: StateSchemaVersion(u32::try_from(state_ver).map_err(|_| {
                    CheckpointError::StorageFailure("invalid state_version".to_owned())
                })?),
                checksum: ContentChecksum::try_from(checksum_str)
                    .map_err(|e| CheckpointError::StorageFailure(e.to_string()))?,
                payload,
                usage: StateResourceUsage {
                    records: u64::try_from(records).map_err(|_| {
                        CheckpointError::StorageFailure("invalid participant records".to_owned())
                    })?,
                    bytes: u64::try_from(bytes).map_err(|_| {
                        CheckpointError::StorageFailure("invalid participant bytes".to_owned())
                    })?,
                },
            };
            payloads.insert(op_id, snapshot);
        }

        Ok(Some(CheckpointEnvelope { metadata, payloads }))
    }
}
