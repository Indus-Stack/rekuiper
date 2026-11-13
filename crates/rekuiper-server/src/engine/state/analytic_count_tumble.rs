//! Versioned capture/restore adapters for analytic, count-window, and
//! tumbling-window operator state.

use rekuiper_core::contracts::{
    ContentChecksum, LogicalDeadline, OperatorId, StateError, StateParticipant, StateResourceUsage,
    StateSchemaVersion, StateSnapshot, STATE_ENVELOPE_VERSION,
};
use ring::digest::{digest, SHA256};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub const ANALYTIC_COUNT_TUMBLE_STATE_VERSION: StateSchemaVersion = StateSchemaVersion(1);

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
        state_version: ANALYTIC_COUNT_TUMBLE_STATE_VERSION,
        checksum,
        payload,
        usage: StateResourceUsage { records, bytes },
    })
}

fn decode_snapshot<T: for<'de> Deserialize<'de>>(
    operator_id: &OperatorId,
    snapshot: &StateSnapshot,
) -> Result<T, StateError> {
    snapshot.validate(operator_id, &[ANALYTIC_COUNT_TUMBLE_STATE_VERSION])?;
    let actual_checksum = sha256_checksum(&snapshot.payload);
    if actual_checksum != snapshot.checksum {
        return Err(StateError::ChecksumMismatch(operator_id.clone()));
    }
    serde_json::from_slice(&snapshot.payload)
        .map_err(|e| StateError::CorruptSnapshot(e.to_string()))
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct AnalyticPayload {
    plan_fingerprint: ContentChecksum,
    accumulators: BTreeMap<String, Value>,
    row_count: u64,
}

/// Captures and restores owned analytic/accumulator state.
pub struct AnalyticStateParticipant {
    operator_id: OperatorId,
    plan_fingerprint: ContentChecksum,
    accumulators: BTreeMap<String, Value>,
    row_count: u64,
}

impl AnalyticStateParticipant {
    pub fn new(operator_id: OperatorId, plan_fingerprint: ContentChecksum) -> Self {
        Self {
            operator_id,
            plan_fingerprint,
            accumulators: BTreeMap::new(),
            row_count: 0,
        }
    }

    pub fn record_value(&mut self, key: impl Into<String>, value: Value) {
        self.accumulators.insert(key.into(), value);
        self.row_count += 1;
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.accumulators.get(key)
    }

    pub fn row_count(&self) -> u64 {
        self.row_count
    }
}

impl StateParticipant for AnalyticStateParticipant {
    fn operator_id(&self) -> &OperatorId {
        &self.operator_id
    }

    fn state_version(&self) -> StateSchemaVersion {
        ANALYTIC_COUNT_TUMBLE_STATE_VERSION
    }

    fn resource_usage(&self) -> StateResourceUsage {
        StateResourceUsage {
            records: self.accumulators.len() as u64,
            bytes: self
                .accumulators
                .values()
                .map(|v| v.to_string().len() as u64)
                .sum(),
        }
    }

    fn capture_snapshot(&self) -> Result<StateSnapshot, StateError> {
        let payload = AnalyticPayload {
            plan_fingerprint: self.plan_fingerprint.clone(),
            accumulators: self.accumulators.clone(),
            row_count: self.row_count,
        };
        encode_snapshot(&self.operator_id, self.accumulators.len() as u64, &payload)
    }

    fn restore_snapshot(&mut self, snapshot: &StateSnapshot) -> Result<(), StateError> {
        let decoded: AnalyticPayload = decode_snapshot(&self.operator_id, snapshot)?;
        if decoded.plan_fingerprint != self.plan_fingerprint {
            return Err(StateError::CorruptSnapshot(
                "analytic state plan_fingerprint mismatch".to_owned(),
            ));
        }
        self.accumulators = decoded.accumulators;
        self.row_count = decoded.row_count;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct CountWindowPayload {
    plan_fingerprint: ContentChecksum,
    size: usize,
    hop: usize,
    buffer: Vec<Value>,
}

/// Captures and restores owned `COUNTWINDOW(size, hop)` state.
pub struct CountWindowStateParticipant {
    operator_id: OperatorId,
    plan_fingerprint: ContentChecksum,
    size: usize,
    hop: usize,
    buffer: Vec<Value>,
}

impl CountWindowStateParticipant {
    pub fn new(
        operator_id: OperatorId,
        plan_fingerprint: ContentChecksum,
        size: usize,
        hop: usize,
    ) -> Self {
        Self {
            operator_id,
            plan_fingerprint,
            size: size.max(1),
            hop: hop.max(1),
            buffer: Vec::new(),
        }
    }

    /// Pushes one row and returns an emitted window batch when the window fires.
    pub fn push_row(&mut self, row: Value) -> Option<Vec<Value>> {
        self.buffer.push(row);
        if self.buffer.len() >= self.size {
            let emitted = self.buffer[..self.size].to_vec();
            let drain_len = self.hop.min(self.buffer.len());
            self.buffer.drain(..drain_len);
            Some(emitted)
        } else {
            None
        }
    }

    pub fn buffered_len(&self) -> usize {
        self.buffer.len()
    }
}

impl StateParticipant for CountWindowStateParticipant {
    fn operator_id(&self) -> &OperatorId {
        &self.operator_id
    }

    fn state_version(&self) -> StateSchemaVersion {
        ANALYTIC_COUNT_TUMBLE_STATE_VERSION
    }

    fn resource_usage(&self) -> StateResourceUsage {
        StateResourceUsage {
            records: self.buffer.len() as u64,
            bytes: self.buffer.iter().map(|v| v.to_string().len() as u64).sum(),
        }
    }

    fn capture_snapshot(&self) -> Result<StateSnapshot, StateError> {
        let payload = CountWindowPayload {
            plan_fingerprint: self.plan_fingerprint.clone(),
            size: self.size,
            hop: self.hop,
            buffer: self.buffer.clone(),
        };
        encode_snapshot(&self.operator_id, self.buffer.len() as u64, &payload)
    }

    fn restore_snapshot(&mut self, snapshot: &StateSnapshot) -> Result<(), StateError> {
        let decoded: CountWindowPayload = decode_snapshot(&self.operator_id, snapshot)?;
        if decoded.plan_fingerprint != self.plan_fingerprint
            || decoded.size != self.size
            || decoded.hop != self.hop
        {
            return Err(StateError::CorruptSnapshot(
                "count window plan or configuration mismatch".to_owned(),
            ));
        }
        self.buffer = decoded.buffer;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct TumbleWindowPayload {
    plan_fingerprint: ContentChecksum,
    window_ms: i64,
    is_event_time: bool,
    watermark_ms: Option<i64>,
    window_start_ms: Option<i64>,
    next_timer: Option<LogicalDeadline>,
    buffer: Vec<(i64, Value)>,
}

/// Captures and restores owned `TUMBLINGWINDOW` state, including watermark
/// progress and logical timer deadlines.
pub struct TumbleWindowStateParticipant {
    operator_id: OperatorId,
    plan_fingerprint: ContentChecksum,
    window_ms: i64,
    is_event_time: bool,
    watermark_ms: Option<i64>,
    window_start_ms: Option<i64>,
    next_timer: Option<LogicalDeadline>,
    buffer: Vec<(i64, Value)>,
}

impl TumbleWindowStateParticipant {
    pub fn new(
        operator_id: OperatorId,
        plan_fingerprint: ContentChecksum,
        window_ms: i64,
        is_event_time: bool,
    ) -> Self {
        Self {
            operator_id,
            plan_fingerprint,
            window_ms: window_ms.max(1),
            is_event_time,
            watermark_ms: None,
            window_start_ms: None,
            next_timer: None,
            buffer: Vec::new(),
        }
    }

    pub fn push_event(&mut self, event_ts_ms: i64, row: Value) {
        if self
            .watermark_ms
            .is_some_and(|w| self.is_event_time && event_ts_ms < w)
        {
            return;
        }
        if self.window_start_ms.is_none() {
            let aligned = event_ts_ms.div_euclid(self.window_ms) * self.window_ms;
            self.window_start_ms = Some(aligned);
            self.next_timer = Some(LogicalDeadline(aligned.saturating_add(self.window_ms)));
        }
        self.buffer.push((event_ts_ms, row));
    }

    pub fn advance_watermark(&mut self, watermark_ms: i64) -> Vec<Vec<Value>> {
        self.watermark_ms = Some(
            self.watermark_ms
                .map_or(watermark_ms, |w| w.max(watermark_ms)),
        );
        let mut emitted = Vec::new();
        while let Some(start) = self.window_start_ms {
            let end = start.saturating_add(self.window_ms);
            if watermark_ms < end {
                break;
            }
            let mut batch = Vec::new();
            let mut remaining = Vec::new();
            for (ts, val) in self.buffer.drain(..) {
                if ts < end {
                    batch.push(val);
                } else {
                    remaining.push((ts, val));
                }
            }
            self.buffer = remaining;
            if !batch.is_empty() {
                emitted.push(batch);
            }
            if self.buffer.is_empty() {
                self.window_start_ms = None;
                self.next_timer = None;
            } else {
                let next_ts = self.buffer.iter().map(|(ts, _)| *ts).min().unwrap_or(end);
                let next_start = next_ts.div_euclid(self.window_ms) * self.window_ms;
                self.window_start_ms = Some(next_start);
                self.next_timer = Some(LogicalDeadline(next_start.saturating_add(self.window_ms)));
            }
        }
        emitted
    }

    pub fn next_timer(&self) -> Option<LogicalDeadline> {
        self.next_timer
    }
}

impl StateParticipant for TumbleWindowStateParticipant {
    fn operator_id(&self) -> &OperatorId {
        &self.operator_id
    }

    fn state_version(&self) -> StateSchemaVersion {
        ANALYTIC_COUNT_TUMBLE_STATE_VERSION
    }

    fn resource_usage(&self) -> StateResourceUsage {
        StateResourceUsage {
            records: self.buffer.len() as u64,
            bytes: self
                .buffer
                .iter()
                .map(|(_, v)| v.to_string().len() as u64)
                .sum(),
        }
    }

    fn capture_snapshot(&self) -> Result<StateSnapshot, StateError> {
        let payload = TumbleWindowPayload {
            plan_fingerprint: self.plan_fingerprint.clone(),
            window_ms: self.window_ms,
            is_event_time: self.is_event_time,
            watermark_ms: self.watermark_ms,
            window_start_ms: self.window_start_ms,
            next_timer: self.next_timer,
            buffer: self.buffer.clone(),
        };
        encode_snapshot(&self.operator_id, self.buffer.len() as u64, &payload)
    }

    fn restore_snapshot(&mut self, snapshot: &StateSnapshot) -> Result<(), StateError> {
        let decoded: TumbleWindowPayload = decode_snapshot(&self.operator_id, snapshot)?;
        if decoded.plan_fingerprint != self.plan_fingerprint
            || decoded.window_ms != self.window_ms
            || decoded.is_event_time != self.is_event_time
        {
            return Err(StateError::CorruptSnapshot(
                "tumbling window plan or configuration mismatch".to_owned(),
            ));
        }
        self.watermark_ms = decoded.watermark_ms;
        self.window_start_ms = decoded.window_start_ms;
        self.next_timer = decoded.next_timer;
        self.buffer = decoded.buffer;
        Ok(())
    }
}
