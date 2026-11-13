//! Versioned capture/restore adapters for session and state windows.

use super::hop_slide::{FiredWindowBatch, MissedDeadlinePolicy, RetainedWindowRow};
use rekuiper_core::contracts::{
    ContentChecksum, LogicalDeadline, OperatorId, StateError, StateParticipant, StateResourceUsage,
    StateSchemaVersion, StateSnapshot, STATE_ENVELOPE_VERSION,
};
use ring::digest::{digest, SHA256};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub const SESSION_STATE_VERSION: StateSchemaVersion = StateSchemaVersion(1);

fn sha256_checksum(bytes: &[u8]) -> ContentChecksum {
    let d = digest(&SHA256, bytes);
    let mut hex = String::with_capacity(64);
    for b in d.as_ref() {
        use std::fmt::Write as _;
        let _ = write!(&mut hex, "{b:02x}");
    }
    ContentChecksum::try_from(format!("sha256:{hex}")).expect("valid sha256 digest")
}

fn encode_snapshot<T: Serialize>(
    operator_id: &OperatorId,
    records: u64,
    state: &T,
) -> Result<StateSnapshot, StateError> {
    let payload =
        serde_json::to_vec(state).map_err(|e| StateError::CorruptSnapshot(e.to_string()))?;
    let checksum = sha256_checksum(&payload);
    let bytes = payload.len() as u64;
    Ok(StateSnapshot {
        envelope_version: STATE_ENVELOPE_VERSION,
        operator_id: operator_id.clone(),
        state_version: SESSION_STATE_VERSION,
        checksum,
        payload,
        usage: StateResourceUsage { records, bytes },
    })
}

fn decode_snapshot<T: for<'de> Deserialize<'de>>(
    operator_id: &OperatorId,
    snapshot: &StateSnapshot,
) -> Result<T, StateError> {
    snapshot.validate(operator_id, &[SESSION_STATE_VERSION])?;
    let actual_checksum = sha256_checksum(&snapshot.payload);
    if actual_checksum != snapshot.checksum {
        return Err(StateError::CorruptSnapshot(format!(
            "checksum mismatch for operator `{}`",
            operator_id.as_str()
        )));
    }
    serde_json::from_slice(&snapshot.payload)
        .map_err(|e| StateError::CorruptSnapshot(e.to_string()))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyedFiredWindowBatch {
    pub partition_key: String,
    pub batch: FiredWindowBatch,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct SessionEntry {
    session_start_ms: i64,
    last_event_ms: i64,
    deadline: LogicalDeadline,
    rows: Vec<RetainedWindowRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SessionSnapshotPayload {
    plan_fingerprint: ContentChecksum,
    gap_ms: i64,
    max_duration_ms: Option<i64>,
    sessions: BTreeMap<String, SessionEntry>,
}

/// Versioned per-key session window state preserving gap/max-duration boundaries,
/// logical deadlines (never `Instant`), and retained rows.
#[derive(Debug, Clone)]
pub struct SessionWindowStateParticipant {
    operator_id: OperatorId,
    plan_fingerprint: ContentChecksum,
    gap_ms: i64,
    max_duration_ms: Option<i64>,
    missed_deadline_policy: MissedDeadlinePolicy,
    sessions: BTreeMap<String, SessionEntry>,
    next_seq: u64,
    last_restored_now_ms: Option<i64>,
}

impl SessionWindowStateParticipant {
    pub fn new(
        operator_id: OperatorId,
        plan_fingerprint: ContentChecksum,
        gap_ms: i64,
        max_duration_ms: Option<i64>,
        missed_deadline_policy: MissedDeadlinePolicy,
    ) -> Result<Self, StateError> {
        if gap_ms <= 0 {
            return Err(StateError::CorruptSnapshot(
                "session gap_ms must be positive".to_string(),
            ));
        }
        if let Some(max_ms) = max_duration_ms {
            if max_ms <= 0 {
                return Err(StateError::CorruptSnapshot(
                    "session max_duration_ms must be positive".to_string(),
                ));
            }
        }
        Ok(Self {
            operator_id,
            plan_fingerprint,
            gap_ms,
            max_duration_ms,
            missed_deadline_policy,
            sessions: BTreeMap::new(),
            next_seq: 1,
            last_restored_now_ms: None,
        })
    }

    fn compute_deadline(&self, session_start_ms: i64, last_event_ms: i64) -> LogicalDeadline {
        let gap_deadline = last_event_ms.saturating_add(self.gap_ms);
        let due_ms = match self.max_duration_ms {
            Some(max_ms) => gap_deadline.min(session_start_ms.saturating_add(max_ms)),
            None => gap_deadline,
        };
        LogicalDeadline(due_ms)
    }

    /// Baseline boundary rule (matches `engine/windows.rs` SESSIONWINDOW):
    /// - consecutive gap `<= gap_ms` stays in the open session (`>` closes it);
    /// - elapsed from `session_start_ms` `>= max_duration_ms` closes the open session.
    pub fn ingest(
        &mut self,
        partition_key: impl Into<String>,
        timestamp_ms: i64,
        payload: Value,
    ) -> Option<KeyedFiredWindowBatch> {
        let key = partition_key.into();
        let seq = self.next_seq;
        self.next_seq = self.next_seq.saturating_add(1);
        let row = RetainedWindowRow {
            seq,
            timestamp_ms,
            payload,
        };

        let should_close = self.sessions.get(&key).is_some_and(|entry| {
            let gap_exceeded = timestamp_ms.saturating_sub(entry.last_event_ms) > self.gap_ms;
            let max_reached = self.max_duration_ms.is_some_and(|max_ms| {
                timestamp_ms.saturating_sub(entry.session_start_ms) >= max_ms
            });
            gap_exceeded || max_reached
        });

        let emitted = if should_close {
            let closed = self.sessions.remove(&key).expect("checked is_some_and");
            let window_end_ms = closed
                .last_event_ms
                .saturating_add(self.gap_ms)
                .min(closed.deadline.0);
            Some(KeyedFiredWindowBatch {
                partition_key: key.clone(),
                batch: FiredWindowBatch {
                    window_start_ms: closed.session_start_ms,
                    window_end_ms,
                    rows: closed.rows.into_iter().map(|r| r.payload).collect(),
                },
            })
        } else {
            None
        };

        if let Some(entry) = self.sessions.get_mut(&key) {
            entry.last_event_ms = entry.last_event_ms.max(timestamp_ms);
            entry.rows.push(row);
            let gap_deadline = entry.last_event_ms.saturating_add(self.gap_ms);
            let due_ms = match self.max_duration_ms {
                Some(max_ms) => gap_deadline.min(entry.session_start_ms.saturating_add(max_ms)),
                None => gap_deadline,
            };
            entry.deadline = LogicalDeadline(due_ms);
        } else {
            let deadline = self.compute_deadline(timestamp_ms, timestamp_ms);
            self.sessions.insert(
                key,
                SessionEntry {
                    session_start_ms: timestamp_ms,
                    last_event_ms: timestamp_ms,
                    deadline,
                    rows: vec![row],
                },
            );
        }

        emitted
    }

    /// Fire or drop sessions whose deadline is `<= now_ms`.
    pub fn fire_due(&mut self, now_ms: i64) -> Vec<KeyedFiredWindowBatch> {
        self.resolve_due(now_ms, false)
    }

    /// Reconcile expired deadlines after recovery according to `missed_deadline_policy`.
    pub fn reconcile_after_restore(&mut self, now_ms: i64) -> Vec<KeyedFiredWindowBatch> {
        self.last_restored_now_ms = Some(now_ms);
        self.resolve_due(now_ms, true)
    }

    fn resolve_due(&mut self, now_ms: i64, is_recovery: bool) -> Vec<KeyedFiredWindowBatch> {
        let mut due_keys: Vec<(i64, String)> = self
            .sessions
            .iter()
            .filter(|(_, entry)| entry.deadline.0 <= now_ms)
            .map(|(k, entry)| (entry.deadline.0, k.clone()))
            .collect();
        due_keys.sort();

        let mut out = Vec::new();
        for (_, key) in due_keys {
            if let Some(entry) = self.sessions.remove(&key) {
                if is_recovery && self.missed_deadline_policy == MissedDeadlinePolicy::DropExpired {
                    continue;
                }
                out.push(KeyedFiredWindowBatch {
                    partition_key: key,
                    batch: FiredWindowBatch {
                        window_start_ms: entry.session_start_ms,
                        window_end_ms: entry.deadline.0,
                        rows: entry.rows.into_iter().map(|r| r.payload).collect(),
                    },
                });
            }
        }
        out
    }

    pub fn open_session_keys(&self) -> Vec<&str> {
        self.sessions.keys().map(String::as_str).collect()
    }

    pub fn open_deadline(&self, key: &str) -> Option<LogicalDeadline> {
        self.sessions.get(key).map(|s| s.deadline)
    }

    pub fn open_row_count(&self, key: &str) -> usize {
        self.sessions.get(key).map_or(0, |s| s.rows.len())
    }
}

impl StateParticipant for SessionWindowStateParticipant {
    fn operator_id(&self) -> &OperatorId {
        &self.operator_id
    }

    fn state_version(&self) -> StateSchemaVersion {
        SESSION_STATE_VERSION
    }

    fn resource_usage(&self) -> StateResourceUsage {
        let records = self.sessions.values().map(|s| s.rows.len() as u64).sum();
        let bytes = serde_json::to_vec(&self.sessions).map_or(0, |v| v.len() as u64);
        StateResourceUsage { records, bytes }
    }

    fn capture_snapshot(&self) -> Result<StateSnapshot, StateError> {
        let records = self.sessions.values().map(|s| s.rows.len() as u64).sum();
        let payload = SessionSnapshotPayload {
            plan_fingerprint: self.plan_fingerprint.clone(),
            gap_ms: self.gap_ms,
            max_duration_ms: self.max_duration_ms,
            sessions: self.sessions.clone(),
        };
        encode_snapshot(&self.operator_id, records, &payload)
    }

    fn restore_snapshot(&mut self, snapshot: &StateSnapshot) -> Result<(), StateError> {
        let payload: SessionSnapshotPayload = decode_snapshot(&self.operator_id, snapshot)?;
        if payload.plan_fingerprint != self.plan_fingerprint
            || payload.gap_ms != self.gap_ms
            || payload.max_duration_ms != self.max_duration_ms
        {
            return Err(StateError::CorruptSnapshot(
                "session window configuration or plan fingerprint mismatch".to_string(),
            ));
        }
        let max_seq = payload
            .sessions
            .values()
            .flat_map(|s| s.rows.iter().map(|r| r.seq))
            .max()
            .unwrap_or(0);
        self.sessions = payload.sessions;
        self.next_seq = max_seq.saturating_add(1);
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct StateWindowEntry {
    on_begin: bool,
    start_time_ms: i64,
    pending_deadline: Option<LogicalDeadline>,
    rows: Vec<RetainedWindowRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StateWindowSnapshotPayload {
    plan_fingerprint: ContentChecksum,
    has_end_condition: bool,
    partitions: BTreeMap<String, StateWindowEntry>,
}

/// Versioned state-window adapter preserving per-partition `on_begin` state,
/// pending deadlines, and retained rows without re-evaluating historical
/// nondeterministic predicates on restore.
#[derive(Debug, Clone)]
pub struct StateWindowStateParticipant {
    operator_id: OperatorId,
    plan_fingerprint: ContentChecksum,
    has_end_condition: bool,
    partitions: BTreeMap<String, StateWindowEntry>,
    next_seq: u64,
}

impl StateWindowStateParticipant {
    pub fn new(
        operator_id: OperatorId,
        plan_fingerprint: ContentChecksum,
        has_end_condition: bool,
    ) -> Self {
        Self {
            operator_id,
            plan_fingerprint,
            has_end_condition,
            partitions: BTreeMap::new(),
            next_seq: 1,
        }
    }

    /// Ingest a row with pre-evaluated condition booleans (`start_matched`, `end_matched`),
    /// matching `engine/windows.rs` STATEWINDOW transition semantics.
    pub fn ingest_evaluated(
        &mut self,
        partition_key: impl Into<String>,
        timestamp_ms: i64,
        payload: Value,
        start_matched: bool,
        end_matched: bool,
    ) -> Option<KeyedFiredWindowBatch> {
        let key = partition_key.into();
        let seq = self.next_seq;
        self.next_seq = self.next_seq.saturating_add(1);
        let row = RetainedWindowRow {
            seq,
            timestamp_ms,
            payload,
        };

        let entry = self
            .partitions
            .entry(key.clone())
            .or_insert_with(|| StateWindowEntry {
                on_begin: false,
                start_time_ms: 0,
                pending_deadline: None,
                rows: Vec::new(),
            });

        if self.has_end_condition {
            if !entry.on_begin {
                if start_matched {
                    entry.start_time_ms = timestamp_ms;
                    entry.on_begin = true;
                    entry.rows.push(row);
                }
                None
            } else {
                entry.rows.push(row);
                if end_matched {
                    entry.on_begin = false;
                    entry.pending_deadline = None;
                    let batch_rows = std::mem::take(&mut entry.rows);
                    Some(KeyedFiredWindowBatch {
                        partition_key: key,
                        batch: FiredWindowBatch {
                            window_start_ms: entry.start_time_ms,
                            window_end_ms: timestamp_ms,
                            rows: batch_rows.into_iter().map(|r| r.payload).collect(),
                        },
                    })
                } else {
                    None
                }
            }
        } else {
            // Single-condition mode: emit accumulated window when condition flips true,
            // starting a fresh window with the current row.
            if !entry.on_begin {
                if start_matched {
                    entry.on_begin = true;
                    entry.start_time_ms = timestamp_ms;
                    entry.rows.push(row);
                }
                None
            } else if start_matched {
                let prev_start = entry.start_time_ms;
                let batch_rows = std::mem::take(&mut entry.rows);
                entry.start_time_ms = timestamp_ms;
                entry.rows.push(row);
                Some(KeyedFiredWindowBatch {
                    partition_key: key,
                    batch: FiredWindowBatch {
                        window_start_ms: prev_start,
                        window_end_ms: timestamp_ms,
                        rows: batch_rows.into_iter().map(|r| r.payload).collect(),
                    },
                })
            } else {
                entry.rows.push(row);
                None
            }
        }
    }

    pub fn is_open(&self, key: &str) -> bool {
        self.partitions.get(key).is_some_and(|p| p.on_begin)
    }

    pub fn retained_row_count(&self, key: &str) -> usize {
        self.partitions.get(key).map_or(0, |p| p.rows.len())
    }
}

impl StateParticipant for StateWindowStateParticipant {
    fn operator_id(&self) -> &OperatorId {
        &self.operator_id
    }

    fn state_version(&self) -> StateSchemaVersion {
        SESSION_STATE_VERSION
    }

    fn resource_usage(&self) -> StateResourceUsage {
        let records = self.partitions.values().map(|p| p.rows.len() as u64).sum();
        let bytes = serde_json::to_vec(&self.partitions).map_or(0, |v| v.len() as u64);
        StateResourceUsage { records, bytes }
    }

    fn capture_snapshot(&self) -> Result<StateSnapshot, StateError> {
        let records = self.partitions.values().map(|p| p.rows.len() as u64).sum();
        let payload = StateWindowSnapshotPayload {
            plan_fingerprint: self.plan_fingerprint.clone(),
            has_end_condition: self.has_end_condition,
            partitions: self.partitions.clone(),
        };
        encode_snapshot(&self.operator_id, records, &payload)
    }

    fn restore_snapshot(&mut self, snapshot: &StateSnapshot) -> Result<(), StateError> {
        let payload: StateWindowSnapshotPayload = decode_snapshot(&self.operator_id, snapshot)?;
        if payload.plan_fingerprint != self.plan_fingerprint
            || payload.has_end_condition != self.has_end_condition
        {
            return Err(StateError::CorruptSnapshot(
                "state window configuration or plan fingerprint mismatch".to_string(),
            ));
        }
        let max_seq = payload
            .partitions
            .values()
            .flat_map(|p| p.rows.iter().map(|r| r.seq))
            .max()
            .unwrap_or(0);
        self.partitions = payload.partitions;
        self.next_seq = max_seq.saturating_add(1);
        Ok(())
    }
}
