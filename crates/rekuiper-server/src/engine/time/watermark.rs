//! Shared monotonic watermark state machine extracted from window operators.

use std::collections::{BTreeMap, BTreeSet};

/// Tracks per-input/partition maximum observed event time, unseen/closed status,
/// and monotonic minimum watermark progress across active inputs.
#[derive(Debug, Clone)]
pub struct WatermarkStateMachine {
    active_inputs: BTreeSet<String>,
    closed_inputs: BTreeSet<String>,
    stream_max_ts: BTreeMap<String, i64>,
    late_tolerance_ms: i64,
    watermark_ms: i64,
}

impl WatermarkStateMachine {
    pub fn new(inputs: impl IntoIterator<Item = String>, late_tolerance_ms: i64) -> Self {
        let active_inputs: BTreeSet<String> = inputs.into_iter().collect();
        Self {
            active_inputs,
            closed_inputs: BTreeSet::new(),
            stream_max_ts: BTreeMap::new(),
            late_tolerance_ms: late_tolerance_ms.max(0),
            watermark_ms: i64::MIN,
        }
    }

    /// Returns the current monotonic watermark (`i64::MIN` before all active inputs
    /// have been seen).
    pub fn watermark_ms(&self) -> i64 {
        self.watermark_ms
    }

    /// Returns true if an event with `event_ts` is late relative to the current watermark.
    pub fn is_late(&self, event_ts: i64) -> bool {
        event_ts < self.watermark_ms
    }

    /// Observes an event timestamp from `input_id` (including rows later rejected by
    /// `WHERE` filters, matching baseline `engine/windows.rs` semantics).
    /// Returns the updated monotonic watermark if it advanced.
    pub fn observe_event(&mut self, input_id: &str, event_ts: i64) -> Option<i64> {
        if event_ts < self.watermark_ms {
            return None;
        }
        let entry = self
            .stream_max_ts
            .entry(input_id.to_string())
            .or_insert(event_ts);
        *entry = (*entry).max(event_ts);
        self.recompute_watermark(event_ts)
    }

    /// Marks `input_id` as closed so it stops holding back watermark progress.
    /// Returns the updated monotonic watermark if closing unblocked advancement.
    pub fn close_input(&mut self, input_id: &str) -> Option<i64> {
        self.closed_inputs.insert(input_id.to_string());
        let fallback_ts = self
            .stream_max_ts
            .values()
            .copied()
            .max()
            .unwrap_or(self.watermark_ms);
        self.recompute_watermark(fallback_ts)
    }

    pub fn is_closed(&self, input_id: &str) -> bool {
        self.closed_inputs.contains(input_id)
    }

    pub fn max_observed_ts(&self, input_id: &str) -> Option<i64> {
        self.stream_max_ts.get(input_id).copied()
    }

    fn recompute_watermark(&mut self, fallback_ts: i64) -> Option<i64> {
        let all_active_seen = self
            .active_inputs
            .iter()
            .filter(|s| !self.closed_inputs.contains(*s))
            .all(|s| self.stream_max_ts.contains_key(*s));

        if !all_active_seen {
            return None;
        }

        let min_source_ts = self
            .active_inputs
            .iter()
            .filter(|s| !self.closed_inputs.contains(*s))
            .filter_map(|s| self.stream_max_ts.get(s))
            .min()
            .copied()
            .unwrap_or(fallback_ts);

        let candidate = min_source_ts.saturating_sub(self.late_tolerance_ms);
        if candidate > self.watermark_ms {
            self.watermark_ms = candidate;
            Some(candidate)
        } else {
            None
        }
    }
}
