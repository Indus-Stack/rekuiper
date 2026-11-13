//! Versioned capture/restore adapters for join buffers/indexes and CEP sequence operators.

use rekuiper_core::contracts::{
    ContentChecksum, OperatorId, StateError, StateParticipant, StateResourceUsage,
    StateSchemaVersion, StateSnapshot, STATE_ENVELOPE_VERSION,
};
use ring::digest::{digest, SHA256};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub const JOINS_CEP_STATE_VERSION: StateSchemaVersion = StateSchemaVersion(1);

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
        state_version: JOINS_CEP_STATE_VERSION,
        checksum,
        payload,
        usage: StateResourceUsage { records, bytes },
    })
}

fn decode_snapshot<T: for<'de> Deserialize<'de>>(
    operator_id: &OperatorId,
    snapshot: &StateSnapshot,
) -> Result<T, StateError> {
    snapshot.validate(operator_id, &[JOINS_CEP_STATE_VERSION])?;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinSnapshotKind {
    Inner,
    Left,
    Right,
    Full,
    Cross,
    Asof,
    LeftAsof,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JoinBufferedRow {
    pub seq: u64,
    pub input_id: String,
    pub key: String,
    pub event_ts_ms: i64,
    pub expires_at_ms: Option<i64>,
    pub matched: bool,
    pub payload: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JoinMatchedPair {
    pub key: String,
    pub left: Option<Value>,
    pub right: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JoinSnapshotPayload {
    plan_fingerprint: ContentChecksum,
    key_schema_fingerprint: ContentChecksum,
    join_kind: JoinSnapshotKind,
    ttl_ms: Option<i64>,
    left_rows: Vec<JoinBufferedRow>,
    right_rows: Vec<JoinBufferedRow>,
}

/// Versioned join-buffer adapter that persists retained rows, match flags, expiry,
/// and input identity, and deterministically rebuilds derived key indexes on restore.
#[derive(Debug, Clone)]
pub struct JoinBufferStateParticipant {
    operator_id: OperatorId,
    plan_fingerprint: ContentChecksum,
    key_schema_fingerprint: ContentChecksum,
    join_kind: JoinSnapshotKind,
    ttl_ms: Option<i64>,
    left_rows: Vec<JoinBufferedRow>,
    right_rows: Vec<JoinBufferedRow>,
    left_index: BTreeMap<String, Vec<usize>>,
    right_index: BTreeMap<String, Vec<usize>>,
    next_seq: u64,
}

impl JoinBufferStateParticipant {
    pub fn new(
        operator_id: OperatorId,
        plan_fingerprint: ContentChecksum,
        key_schema_fingerprint: ContentChecksum,
        join_kind: JoinSnapshotKind,
        ttl_ms: Option<i64>,
    ) -> Self {
        Self {
            operator_id,
            plan_fingerprint,
            key_schema_fingerprint,
            join_kind,
            ttl_ms,
            left_rows: Vec::new(),
            right_rows: Vec::new(),
            left_index: BTreeMap::new(),
            right_index: BTreeMap::new(),
            next_seq: 1,
        }
    }

    fn rebuild_indexes(&mut self) {
        self.left_index.clear();
        for (idx, row) in self.left_rows.iter().enumerate() {
            self.left_index
                .entry(row.key.clone())
                .or_default()
                .push(idx);
        }
        self.right_index.clear();
        for (idx, row) in self.right_rows.iter().enumerate() {
            self.right_index
                .entry(row.key.clone())
                .or_default()
                .push(idx);
        }
    }

    pub fn evict_expired(&mut self, now_ms: i64) -> usize {
        let before = self.left_rows.len() + self.right_rows.len();
        self.left_rows
            .retain(|r| r.expires_at_ms.is_none_or(|exp| exp > now_ms));
        self.right_rows
            .retain(|r| r.expires_at_ms.is_none_or(|exp| exp > now_ms));
        let after = self.left_rows.len() + self.right_rows.len();
        if after != before {
            self.rebuild_indexes();
        }
        before - after
    }

    pub fn ingest_left(
        &mut self,
        input_id: impl Into<String>,
        key: impl Into<String>,
        event_ts_ms: i64,
        payload: Value,
    ) -> Vec<JoinMatchedPair> {
        let key = key.into();
        let seq = self.next_seq;
        self.next_seq = self.next_seq.saturating_add(1);
        let expires_at_ms = self.ttl_ms.map(|ttl| event_ts_ms.saturating_add(ttl));

        let mut matches = Vec::new();
        let mut matched_any = false;

        match self.join_kind {
            JoinSnapshotKind::Cross => {
                for r in &mut self.right_rows {
                    r.matched = true;
                    matched_any = true;
                    matches.push(JoinMatchedPair {
                        key: key.clone(),
                        left: Some(payload.clone()),
                        right: Some(r.payload.clone()),
                    });
                }
            }
            JoinSnapshotKind::Asof | JoinSnapshotKind::LeftAsof => {
                if let Some(indices) = self.right_index.get(&key) {
                    let best = indices
                        .iter()
                        .copied()
                        .filter(|&i| self.right_rows[i].event_ts_ms <= event_ts_ms)
                        .max_by_key(|&i| (self.right_rows[i].event_ts_ms, self.right_rows[i].seq));
                    if let Some(idx) = best {
                        self.right_rows[idx].matched = true;
                        matched_any = true;
                        matches.push(JoinMatchedPair {
                            key: key.clone(),
                            left: Some(payload.clone()),
                            right: Some(self.right_rows[idx].payload.clone()),
                        });
                    }
                }
                if !matched_any && self.join_kind == JoinSnapshotKind::LeftAsof {
                    matches.push(JoinMatchedPair {
                        key: key.clone(),
                        left: Some(payload.clone()),
                        right: None,
                    });
                }
            }
            _ => {
                if let Some(indices) = self.right_index.get(&key) {
                    for &idx in indices {
                        self.right_rows[idx].matched = true;
                        matched_any = true;
                        matches.push(JoinMatchedPair {
                            key: key.clone(),
                            left: Some(payload.clone()),
                            right: Some(self.right_rows[idx].payload.clone()),
                        });
                    }
                }
            }
        }

        let idx = self.left_rows.len();
        self.left_rows.push(JoinBufferedRow {
            seq,
            input_id: input_id.into(),
            key: key.clone(),
            event_ts_ms,
            expires_at_ms,
            matched: matched_any,
            payload,
        });
        self.left_index.entry(key).or_default().push(idx);
        matches
    }

    pub fn ingest_right(
        &mut self,
        input_id: impl Into<String>,
        key: impl Into<String>,
        event_ts_ms: i64,
        payload: Value,
    ) -> Vec<JoinMatchedPair> {
        let key = key.into();
        let seq = self.next_seq;
        self.next_seq = self.next_seq.saturating_add(1);
        let expires_at_ms = self.ttl_ms.map(|ttl| event_ts_ms.saturating_add(ttl));

        let mut matches = Vec::new();
        let mut matched_any = false;

        match self.join_kind {
            JoinSnapshotKind::Cross => {
                for l in &mut self.left_rows {
                    l.matched = true;
                    matched_any = true;
                    matches.push(JoinMatchedPair {
                        key: key.clone(),
                        left: Some(l.payload.clone()),
                        right: Some(payload.clone()),
                    });
                }
            }
            JoinSnapshotKind::Asof | JoinSnapshotKind::LeftAsof => {
                // ASOF right arrival is buffered for subsequent left probes.
            }
            _ => {
                if let Some(indices) = self.left_index.get(&key) {
                    for &idx in indices {
                        self.left_rows[idx].matched = true;
                        matched_any = true;
                        matches.push(JoinMatchedPair {
                            key: key.clone(),
                            left: Some(self.left_rows[idx].payload.clone()),
                            right: Some(payload.clone()),
                        });
                    }
                }
            }
        }

        let idx = self.right_rows.len();
        self.right_rows.push(JoinBufferedRow {
            seq,
            input_id: input_id.into(),
            key: key.clone(),
            event_ts_ms,
            expires_at_ms,
            matched: matched_any,
            payload,
        });
        self.right_index.entry(key).or_default().push(idx);
        matches
    }

    pub fn unmatched_left_count(&self) -> usize {
        self.left_rows.iter().filter(|r| !r.matched).count()
    }

    pub fn unmatched_right_count(&self) -> usize {
        self.right_rows.iter().filter(|r| !r.matched).count()
    }

    pub fn index_len(&self) -> (usize, usize) {
        (self.left_index.len(), self.right_index.len())
    }
}

impl StateParticipant for JoinBufferStateParticipant {
    fn operator_id(&self) -> &OperatorId {
        &self.operator_id
    }

    fn state_version(&self) -> StateSchemaVersion {
        JOINS_CEP_STATE_VERSION
    }

    fn resource_usage(&self) -> StateResourceUsage {
        let records = (self.left_rows.len() + self.right_rows.len()) as u64;
        let bytes =
            serde_json::to_vec(&(&self.left_rows, &self.right_rows)).map_or(0, |v| v.len() as u64);
        StateResourceUsage { records, bytes }
    }

    fn capture_snapshot(&self) -> Result<StateSnapshot, StateError> {
        let records = (self.left_rows.len() + self.right_rows.len()) as u64;
        let payload = JoinSnapshotPayload {
            plan_fingerprint: self.plan_fingerprint.clone(),
            key_schema_fingerprint: self.key_schema_fingerprint.clone(),
            join_kind: self.join_kind,
            ttl_ms: self.ttl_ms,
            left_rows: self.left_rows.clone(),
            right_rows: self.right_rows.clone(),
        };
        encode_snapshot(&self.operator_id, records, &payload)
    }

    fn restore_snapshot(&mut self, snapshot: &StateSnapshot) -> Result<(), StateError> {
        let payload: JoinSnapshotPayload = decode_snapshot(&self.operator_id, snapshot)?;
        if payload.plan_fingerprint != self.plan_fingerprint
            || payload.key_schema_fingerprint != self.key_schema_fingerprint
            || payload.join_kind != self.join_kind
            || payload.ttl_ms != self.ttl_ms
        {
            return Err(StateError::CorruptSnapshot(
                "join plan, key schema, or configuration mismatch".to_string(),
            ));
        }
        let max_seq = payload
            .left_rows
            .iter()
            .chain(payload.right_rows.iter())
            .map(|r| r.seq)
            .max()
            .unwrap_or(0);
        self.left_rows = payload.left_rows;
        self.right_rows = payload.right_rows;
        self.next_seq = max_seq.saturating_add(1);
        self.rebuild_indexes();
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartialSequenceMatch {
    pub match_id: u64,
    pub key: String,
    pub step_index: usize,
    pub started_at_ms: i64,
    pub expires_at_ms: Option<i64>,
    pub ordering_key: u64,
    pub input_ids: Vec<String>,
    pub captured_events: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompletedSequenceMatch {
    pub key: String,
    pub ordering_keys: Vec<u64>,
    pub input_ids: Vec<String>,
    pub events: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CepSnapshotPayload {
    plan_fingerprint: ContentChecksum,
    key_schema_fingerprint: ContentChecksum,
    pattern_steps: Vec<String>,
    within_ms: Option<i64>,
    partial_matches: Vec<PartialSequenceMatch>,
}

/// Versioned CEP sequence operator adapter persisting match progress, ordering keys,
/// expiry, and input identity while deterministically rebuilding derived key indexes.
#[derive(Debug, Clone)]
pub struct CepSequenceStateParticipant {
    operator_id: OperatorId,
    plan_fingerprint: ContentChecksum,
    key_schema_fingerprint: ContentChecksum,
    pattern_steps: Vec<String>,
    within_ms: Option<i64>,
    partial_matches: Vec<PartialSequenceMatch>,
    active_by_key: BTreeMap<String, Vec<usize>>,
    next_match_id: u64,
}

impl CepSequenceStateParticipant {
    pub fn new(
        operator_id: OperatorId,
        plan_fingerprint: ContentChecksum,
        key_schema_fingerprint: ContentChecksum,
        pattern_steps: Vec<String>,
        within_ms: Option<i64>,
    ) -> Result<Self, StateError> {
        if pattern_steps.is_empty() {
            return Err(StateError::CorruptSnapshot(
                "CEP pattern_steps must not be empty".to_string(),
            ));
        }
        Ok(Self {
            operator_id,
            plan_fingerprint,
            key_schema_fingerprint,
            pattern_steps,
            within_ms,
            partial_matches: Vec::new(),
            active_by_key: BTreeMap::new(),
            next_match_id: 1,
        })
    }

    fn rebuild_index(&mut self) {
        self.active_by_key.clear();
        for (idx, m) in self.partial_matches.iter().enumerate() {
            self.active_by_key
                .entry(m.key.clone())
                .or_default()
                .push(idx);
        }
    }

    pub fn evict_expired(&mut self, now_ms: i64) -> usize {
        let before = self.partial_matches.len();
        self.partial_matches
            .retain(|m| m.expires_at_ms.is_none_or(|exp| exp > now_ms));
        let after = self.partial_matches.len();
        if after != before {
            self.rebuild_index();
        }
        before - after
    }

    pub fn advance(
        &mut self,
        key: impl Into<String>,
        ordering_key: u64,
        event_ts_ms: i64,
        input_id: impl Into<String>,
        step_name: &str,
        payload: Value,
    ) -> Vec<CompletedSequenceMatch> {
        let key = key.into();
        let input_id = input_id.into();
        self.evict_expired(event_ts_ms);

        let mut completed = Vec::new();
        let mut remaining = Vec::with_capacity(self.partial_matches.len() + 1);

        for mut m in std::mem::take(&mut self.partial_matches) {
            if m.key == key
                && self.pattern_steps.get(m.step_index).map(String::as_str) == Some(step_name)
            {
                m.step_index += 1;
                m.ordering_key = ordering_key;
                m.input_ids.push(input_id.clone());
                m.captured_events.push(payload.clone());
                if m.step_index == self.pattern_steps.len() {
                    completed.push(CompletedSequenceMatch {
                        key: m.key,
                        ordering_keys: vec![m.ordering_key],
                        input_ids: m.input_ids,
                        events: m.captured_events,
                    });
                    continue;
                }
            }
            remaining.push(m);
        }

        if self.pattern_steps[0] == step_name {
            if self.pattern_steps.len() == 1 {
                completed.push(CompletedSequenceMatch {
                    key: key.clone(),
                    ordering_keys: vec![ordering_key],
                    input_ids: vec![input_id],
                    events: vec![payload],
                });
            } else {
                let match_id = self.next_match_id;
                self.next_match_id = self.next_match_id.saturating_add(1);
                remaining.push(PartialSequenceMatch {
                    match_id,
                    key,
                    step_index: 1,
                    started_at_ms: event_ts_ms,
                    expires_at_ms: self.within_ms.map(|w| event_ts_ms.saturating_add(w)),
                    ordering_key,
                    input_ids: vec![input_id],
                    captured_events: vec![payload],
                });
            }
        }

        self.partial_matches = remaining;
        self.rebuild_index();
        completed
    }

    pub fn active_partial_count(&self) -> usize {
        self.partial_matches.len()
    }

    pub fn active_key_index_len(&self) -> usize {
        self.active_by_key.len()
    }
}

impl StateParticipant for CepSequenceStateParticipant {
    fn operator_id(&self) -> &OperatorId {
        &self.operator_id
    }

    fn state_version(&self) -> StateSchemaVersion {
        JOINS_CEP_STATE_VERSION
    }

    fn resource_usage(&self) -> StateResourceUsage {
        let records = self.partial_matches.len() as u64;
        let bytes = serde_json::to_vec(&self.partial_matches).map_or(0, |v| v.len() as u64);
        StateResourceUsage { records, bytes }
    }

    fn capture_snapshot(&self) -> Result<StateSnapshot, StateError> {
        let records = self.partial_matches.len() as u64;
        let payload = CepSnapshotPayload {
            plan_fingerprint: self.plan_fingerprint.clone(),
            key_schema_fingerprint: self.key_schema_fingerprint.clone(),
            pattern_steps: self.pattern_steps.clone(),
            within_ms: self.within_ms,
            partial_matches: self.partial_matches.clone(),
        };
        encode_snapshot(&self.operator_id, records, &payload)
    }

    fn restore_snapshot(&mut self, snapshot: &StateSnapshot) -> Result<(), StateError> {
        let payload: CepSnapshotPayload = decode_snapshot(&self.operator_id, snapshot)?;
        if payload.plan_fingerprint != self.plan_fingerprint
            || payload.key_schema_fingerprint != self.key_schema_fingerprint
            || payload.pattern_steps != self.pattern_steps
            || payload.within_ms != self.within_ms
        {
            return Err(StateError::CorruptSnapshot(
                "CEP plan, key schema, or pattern mismatch".to_string(),
            ));
        }
        let max_id = payload
            .partial_matches
            .iter()
            .map(|m| m.match_id)
            .max()
            .unwrap_or(0);
        self.partial_matches = payload.partial_matches;
        self.next_match_id = max_id.saturating_add(1);
        self.rebuild_index();
        Ok(())
    }
}
