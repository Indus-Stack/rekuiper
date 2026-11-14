//! Versioned capture/restore adapters for hopping and sliding window state.

use rekuiper_core::contracts::{
    ContentChecksum, LogicalDeadline, OperatorId, StateError, StateParticipant, StateResourceUsage,
    StateSchemaVersion, StateSnapshot, STATE_ENVELOPE_VERSION,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

pub const HOP_SLIDE_STATE_VERSION: StateSchemaVersion = StateSchemaVersion(1);

/// Policy for handling processing-time deadlines that were missed during downtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissedDeadlinePolicy {
    /// Fire all missed due windows in chronological order upon recovery.
    #[default]
    FireImmediatelyInOrder,
    /// Drop windows whose trigger deadline expired before recovery.
    DropExpired,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RetainedWindowRow {
    pub seq: u64,
    pub timestamp_ms: i64,
    pub payload: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FiredWindowBatch {
    pub window_start_ms: i64,
    pub window_end_ms: i64,
    pub rows: Vec<Value>,
}

fn encode_snapshot<T: Serialize>(
    operator_id: &OperatorId,
    records: u64,
    state: &T,
) -> Result<StateSnapshot, StateError> {
    let payload =
        serde_json::to_vec(state).map_err(|e| StateError::CorruptSnapshot(e.to_string()))?;
    let checksum = ContentChecksum::sha256_bytes(&payload);
    let bytes = payload.len() as u64;
    Ok(StateSnapshot {
        envelope_version: STATE_ENVELOPE_VERSION,
        operator_id: operator_id.clone(),
        state_version: HOP_SLIDE_STATE_VERSION,
        checksum,
        payload,
        usage: StateResourceUsage { records, bytes },
    })
}

fn decode_snapshot<T: for<'de> Deserialize<'de>>(
    operator_id: &OperatorId,
    snapshot: &StateSnapshot,
) -> Result<T, StateError> {
    snapshot.validate(operator_id, &[HOP_SLIDE_STATE_VERSION])?;
    let actual_checksum = ContentChecksum::sha256_bytes(&snapshot.payload);
    if actual_checksum != snapshot.checksum {
        return Err(StateError::CorruptSnapshot(format!(
            "checksum mismatch for operator `{}`",
            operator_id.as_str()
        )));
    }
    serde_json::from_slice(&snapshot.payload)
        .map_err(|e| StateError::CorruptSnapshot(e.to_string()))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HoppingSnapshotPayload {
    plan_fingerprint: ContentChecksum,
    window_length_ms: i64,
    hop_interval_ms: i64,
    watermark_ms: Option<i64>,
    next_deadline: Option<LogicalDeadline>,
    buckets: BTreeMap<i64, Vec<RetainedWindowRow>>,
    pending_triggers: BTreeSet<i64>,
}

/// Versioned hopping window state preserving overlapping buckets, triggers,
/// watermarks, and processing-time deadlines.
#[derive(Debug, Clone)]
pub struct HoppingWindowStateParticipant {
    operator_id: OperatorId,
    plan_fingerprint: ContentChecksum,
    window_length_ms: i64,
    hop_interval_ms: i64,
    watermark_ms: Option<i64>,
    next_deadline: Option<LogicalDeadline>,
    buckets: BTreeMap<i64, Vec<RetainedWindowRow>>,
    pending_triggers: BTreeSet<i64>,
}

impl HoppingWindowStateParticipant {
    pub fn new(
        operator_id: OperatorId,
        plan_fingerprint: ContentChecksum,
        window_length_ms: i64,
        hop_interval_ms: i64,
    ) -> Self {
        Self {
            operator_id,
            plan_fingerprint,
            window_length_ms: window_length_ms.max(1),
            hop_interval_ms: hop_interval_ms.max(1),
            watermark_ms: None,
            next_deadline: None,
            buckets: BTreeMap::new(),
            pending_triggers: BTreeSet::new(),
        }
    }

    /// Inserts a row into every overlapping hopping bucket `[start, start + length)`
    /// covering `timestamp_ms`.
    pub fn insert_row(&mut self, seq: u64, timestamp_ms: i64, payload: Value) {
        let earliest_start = (timestamp_ms - self.window_length_ms + 1)
            .div_euclid(self.hop_interval_ms)
            * self.hop_interval_ms;
        let latest_start = timestamp_ms.div_euclid(self.hop_interval_ms) * self.hop_interval_ms;

        let mut start = earliest_start;
        while start <= latest_start {
            let end = start + self.window_length_ms;
            if timestamp_ms >= start && timestamp_ms < end {
                self.buckets
                    .entry(start)
                    .or_default()
                    .push(RetainedWindowRow {
                        seq,
                        timestamp_ms,
                        payload: payload.clone(),
                    });
                self.pending_triggers.insert(end);
            }
            start += self.hop_interval_ms;
        }
        self.next_deadline = self.pending_triggers.first().copied().map(LogicalDeadline);
    }

    pub fn advance_watermark(&mut self, watermark_ms: i64) {
        self.watermark_ms = Some(
            self.watermark_ms
                .map_or(watermark_ms, |prev| prev.max(watermark_ms)),
        );
    }

    pub fn watermark_ms(&self) -> Option<i64> {
        self.watermark_ms
    }

    /// Fires all windows with `window_end_ms <= effective_ms` in chronological order,
    /// applying `policy` for missed deadlines.
    pub fn fire_due(&mut self, now_ms: i64, policy: MissedDeadlinePolicy) -> Vec<FiredWindowBatch> {
        let effective = self.watermark_ms.map_or(now_ms, |wm| wm.max(now_ms));
        let due_ends: Vec<i64> = self.pending_triggers.range(..=effective).copied().collect();

        let mut emitted = Vec::new();
        for end in due_ends {
            self.pending_triggers.remove(&end);
            let start = end - self.window_length_ms;
            if let Some(mut rows) = self.buckets.remove(&start) {
                if policy == MissedDeadlinePolicy::FireImmediatelyInOrder {
                    rows.sort_by_key(|r| r.seq);
                    emitted.push(FiredWindowBatch {
                        window_start_ms: start,
                        window_end_ms: end,
                        rows: rows.into_iter().map(|r| r.payload).collect(),
                    });
                }
            }
        }
        self.next_deadline = self.pending_triggers.first().copied().map(LogicalDeadline);
        emitted
    }
}

impl StateParticipant for HoppingWindowStateParticipant {
    fn operator_id(&self) -> &OperatorId {
        &self.operator_id
    }

    fn participant_kind_tag(&self) -> Option<&'static str> {
        Some("hopping")
    }

    fn state_version(&self) -> StateSchemaVersion {
        HOP_SLIDE_STATE_VERSION
    }

    fn resource_usage(&self) -> StateResourceUsage {
        let records: u64 = self.buckets.values().map(|v| v.len() as u64).sum();
        let bytes = records.saturating_mul(64);
        StateResourceUsage { records, bytes }
    }

    fn capture_snapshot(&self) -> Result<StateSnapshot, StateError> {
        let records: u64 = self.buckets.values().map(|v| v.len() as u64).sum();
        let payload = HoppingSnapshotPayload {
            plan_fingerprint: self.plan_fingerprint.clone(),
            window_length_ms: self.window_length_ms,
            hop_interval_ms: self.hop_interval_ms,
            watermark_ms: self.watermark_ms,
            next_deadline: self.next_deadline,
            buckets: self.buckets.clone(),
            pending_triggers: self.pending_triggers.clone(),
        };
        encode_snapshot(&self.operator_id, records, &payload)
    }

    fn restore_snapshot(&mut self, snapshot: &StateSnapshot) -> Result<(), StateError> {
        let decoded: HoppingSnapshotPayload = decode_snapshot(&self.operator_id, snapshot)?;
        if decoded.plan_fingerprint != self.plan_fingerprint {
            return Err(StateError::CorruptSnapshot(
                "hopping window plan fingerprint mismatch".to_string(),
            ));
        }
        self.window_length_ms = decoded.window_length_ms;
        self.hop_interval_ms = decoded.hop_interval_ms;
        self.watermark_ms = decoded.watermark_ms;
        self.next_deadline = decoded.next_deadline;
        self.buckets = decoded.buckets;
        self.pending_triggers = decoded.pending_triggers;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SlidingSnapshotPayload {
    plan_fingerprint: ContentChecksum,
    window_length_ms: i64,
    delay_ms: i64,
    watermark_ms: Option<i64>,
    next_deadline: Option<LogicalDeadline>,
    rows: Vec<RetainedWindowRow>,
    pending_triggers: BTreeSet<i64>,
}

/// Versioned sliding window state preserving retained rows, inclusive window
/// boundaries `[trigger - length, trigger]`, triggers, watermarks, and deadlines.
#[derive(Debug, Clone)]
pub struct SlidingWindowStateParticipant {
    operator_id: OperatorId,
    plan_fingerprint: ContentChecksum,
    window_length_ms: i64,
    delay_ms: i64,
    watermark_ms: Option<i64>,
    next_deadline: Option<LogicalDeadline>,
    rows: Vec<RetainedWindowRow>,
    pending_triggers: BTreeSet<i64>,
}

impl SlidingWindowStateParticipant {
    pub fn new(
        operator_id: OperatorId,
        plan_fingerprint: ContentChecksum,
        window_length_ms: i64,
        delay_ms: i64,
    ) -> Self {
        Self {
            operator_id,
            plan_fingerprint,
            window_length_ms: window_length_ms.max(1),
            delay_ms: delay_ms.max(0),
            watermark_ms: None,
            next_deadline: None,
            rows: Vec::new(),
            pending_triggers: BTreeSet::new(),
        }
    }

    pub fn insert_row(&mut self, seq: u64, timestamp_ms: i64, payload: Value) {
        self.rows.push(RetainedWindowRow {
            seq,
            timestamp_ms,
            payload,
        });
        self.pending_triggers.insert(timestamp_ms + self.delay_ms);
        self.next_deadline = self.pending_triggers.first().copied().map(LogicalDeadline);
    }

    pub fn advance_watermark(&mut self, watermark_ms: i64) {
        self.watermark_ms = Some(
            self.watermark_ms
                .map_or(watermark_ms, |prev| prev.max(watermark_ms)),
        );
    }

    /// Fires all due sliding triggers up to `now_ms`, preserving inclusive
    /// `[trigger_ts - window_length_ms, trigger_ts]` boundaries and input order.
    pub fn fire_due(&mut self, now_ms: i64, policy: MissedDeadlinePolicy) -> Vec<FiredWindowBatch> {
        let effective = self.watermark_ms.map_or(now_ms, |wm| wm.max(now_ms));
        let due: Vec<i64> = self.pending_triggers.range(..=effective).copied().collect();

        let mut emitted = Vec::new();
        for trigger_ts in due {
            self.pending_triggers.remove(&trigger_ts);
            if policy == MissedDeadlinePolicy::DropExpired {
                continue;
            }
            let start = trigger_ts - self.window_length_ms;
            let mut window_rows: Vec<&RetainedWindowRow> = self
                .rows
                .iter()
                .filter(|r| r.timestamp_ms >= start && r.timestamp_ms <= trigger_ts)
                .collect();
            window_rows.sort_by_key(|r| r.seq);
            emitted.push(FiredWindowBatch {
                window_start_ms: start,
                window_end_ms: trigger_ts,
                rows: window_rows.into_iter().map(|r| r.payload.clone()).collect(),
            });
        }

        // Evict rows older than the earliest remaining trigger's window start.
        if let Some(&next_trigger) = self.pending_triggers.first() {
            let min_keep = next_trigger - self.window_length_ms;
            self.rows.retain(|r| r.timestamp_ms >= min_keep);
            self.next_deadline = Some(LogicalDeadline(next_trigger));
        } else {
            let min_keep = effective - self.window_length_ms;
            self.rows.retain(|r| r.timestamp_ms >= min_keep);
            self.next_deadline = None;
        }
        emitted
    }
}

impl StateParticipant for SlidingWindowStateParticipant {
    fn operator_id(&self) -> &OperatorId {
        &self.operator_id
    }

    fn participant_kind_tag(&self) -> Option<&'static str> {
        Some("sliding")
    }

    fn state_version(&self) -> StateSchemaVersion {
        HOP_SLIDE_STATE_VERSION
    }

    fn resource_usage(&self) -> StateResourceUsage {
        let records = self.rows.len() as u64;
        let bytes = records.saturating_mul(64);
        StateResourceUsage { records, bytes }
    }

    fn capture_snapshot(&self) -> Result<StateSnapshot, StateError> {
        let payload = SlidingSnapshotPayload {
            plan_fingerprint: self.plan_fingerprint.clone(),
            window_length_ms: self.window_length_ms,
            delay_ms: self.delay_ms,
            watermark_ms: self.watermark_ms,
            next_deadline: self.next_deadline,
            rows: self.rows.clone(),
            pending_triggers: self.pending_triggers.clone(),
        };
        encode_snapshot(&self.operator_id, self.rows.len() as u64, &payload)
    }

    fn restore_snapshot(&mut self, snapshot: &StateSnapshot) -> Result<(), StateError> {
        let decoded: SlidingSnapshotPayload = decode_snapshot(&self.operator_id, snapshot)?;
        if decoded.plan_fingerprint != self.plan_fingerprint {
            return Err(StateError::CorruptSnapshot(
                "sliding window plan fingerprint mismatch".to_string(),
            ));
        }
        self.window_length_ms = decoded.window_length_ms;
        self.delay_ms = decoded.delay_ms;
        self.watermark_ms = decoded.watermark_ms;
        self.next_deadline = decoded.next_deadline;
        self.rows = decoded.rows;
        self.pending_triggers = decoded.pending_triggers;
        Ok(())
    }
}
