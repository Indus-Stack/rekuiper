//! Multi-input checkpoint barrier alignment, paused-edge bounding, async lookup
//! draining/snapshotting, and v1 reliable feedback-cycle rejection.

use rekuiper_core::contracts::{CheckpointId, EdgeId, EventTimestamp, Generation, OrderedEnvelope};
use rekuiper_core::resources::budget::{
    BudgetCharge, BudgetError, BudgetReservation, ResourceBudget,
};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BarrierAlignmentError {
    NoParticipatingEdges,
    FeedbackCycleDetected {
        cycle_nodes: Vec<String>,
    },
    UnknownEdge(EdgeId),
    ConflictingCheckpoint {
        active: CheckpointId,
        incoming: CheckpointId,
    },
    PausedBufferBudgetExceeded {
        edge: EdgeId,
        reason: String,
    },
}

impl std::fmt::Display for BarrierAlignmentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoParticipatingEdges => write!(f, "no participating input edges configured"),
            Self::FeedbackCycleDetected { cycle_nodes } => write!(
                f,
                "reliable feedback cycles are rejected in v1: {:?}",
                cycle_nodes
            ),
            Self::UnknownEdge(edge) => write!(f, "unknown input edge `{edge:?}`"),
            Self::ConflictingCheckpoint { active, incoming } => write!(
                f,
                "conflicting checkpoint barrier {} while {} is aligning",
                incoming.0, active.0
            ),
            Self::PausedBufferBudgetExceeded { edge, reason } => write!(
                f,
                "paused-edge buffer budget exceeded on `{edge:?}`: {reason}"
            ),
        }
    }
}

impl std::error::Error for BarrierAlignmentError {}

/// Rejects any feedback cycle in a reliable operator graph (v1 policy).
pub fn validate_acyclic_reliable_graph(
    nodes: &[String],
    edges: &[(String, String)],
) -> Result<(), BarrierAlignmentError> {
    let mut in_degree: BTreeMap<String, usize> =
        nodes.iter().map(|n| (n.clone(), 0usize)).collect();
    let mut adj: BTreeMap<String, Vec<String>> = BTreeMap::new();

    for (from, to) in edges {
        in_degree.entry(from.clone()).or_insert(0);
        *in_degree.entry(to.clone()).or_insert(0) += 1;
        adj.entry(from.clone()).or_default().push(to.clone());
    }

    let mut queue: VecDeque<String> = in_degree
        .iter()
        .filter(|(_, deg)| **deg == 0)
        .map(|(n, _)| n.clone())
        .collect();

    let mut visited = 0usize;
    while let Some(node) = queue.pop_front() {
        visited += 1;
        if let Some(neighbors) = adj.get(&node) {
            for next in neighbors {
                if let Some(deg) = in_degree.get_mut(next) {
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push_back(next.clone());
                    }
                }
            }
        }
    }

    if visited != in_degree.len() {
        let cycle_nodes: Vec<String> = in_degree
            .into_iter()
            .filter(|(_, deg)| *deg > 0)
            .map(|(n, _)| n)
            .collect();
        return Err(BarrierAlignmentError::FeedbackCycleDetected { cycle_nodes });
    }

    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
pub enum LookupStatus {
    InFlight,
    Completed(Option<Value>),
    Snapshotted(Value),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlignmentAbortReason {
    Timeout,
    StalledLookupTimeout,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlignedBarrier<T> {
    pub checkpoint: CheckpointId,
    pub generation: Generation,
    pub drained_paused_data: Vec<(EdgeId, T)>,
    pub drained_paused_watermarks: Vec<(EdgeId, EventTimestamp)>,
    pub recorded_lookups: BTreeMap<u64, Value>,
    pub target_sinks: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AbortedAlignment<T> {
    pub checkpoint: CheckpointId,
    pub generation: Generation,
    pub reason: AlignmentAbortReason,
    pub released_paused_data: Vec<(EdgeId, T)>,
    pub released_paused_watermarks: Vec<(EdgeId, EventTimestamp)>,
    pub cancelled_lookups: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlignmentOutcome<T> {
    ForwardData {
        edge: EdgeId,
        item: T,
    },
    BufferedPaused {
        edge: EdgeId,
        buffered_on_edge: usize,
    },
    Watermark {
        edge: EdgeId,
        timestamp: EventTimestamp,
    },
    EndOfInput {
        edge: EdgeId,
    },
    WaitingForBarriers {
        checkpoint: CheckpointId,
        arrived: usize,
        expected: usize,
        pending_lookups: usize,
    },
    Aligned(AlignedBarrier<T>),
}

enum PausedBufferEntry<T> {
    Data(T, BudgetReservation),
    Watermark {
        edge: EdgeId,
        timestamp: EventTimestamp,
    },
}

struct DrainedPausedEntries<T> {
    data: Vec<(EdgeId, T)>,
    watermarks: Vec<(EdgeId, EventTimestamp)>,
}

struct ActiveAlignment<T> {
    checkpoint: CheckpointId,
    generation: Generation,
    deadline_ms: i64,
    arrived_barriers: BTreeSet<EdgeId>,
    paused_edges: BTreeSet<EdgeId>,
    paused_buffers: BTreeMap<EdgeId, Vec<PausedBufferEntry<T>>>,
}

/// Aligns checkpoint barriers across participating input edges while bounding
/// paused-edge buffers via `ResourceBudget`, tracking async lookups, and
/// propagating completion across every reachable sink.
pub struct BarrierAligner<T> {
    participating_edges: BTreeSet<EdgeId>,
    idle_edges: BTreeSet<EdgeId>,
    budget: ResourceBudget,
    alignment_timeout_ms: i64,
    reachable_sinks: BTreeSet<String>,
    active: Option<ActiveAlignment<T>>,
    outstanding_lookups: BTreeMap<u64, LookupStatus>,
    watermarks: BTreeMap<EdgeId, EventTimestamp>,
    pending_sink_acks: BTreeMap<CheckpointId, BTreeSet<String>>,
}

impl<T> BarrierAligner<T> {
    pub fn new(
        participating_edges: Vec<EdgeId>,
        reachable_sinks: Vec<String>,
        budget: ResourceBudget,
        alignment_timeout_ms: i64,
    ) -> Result<Self, BarrierAlignmentError> {
        if participating_edges.is_empty() {
            return Err(BarrierAlignmentError::NoParticipatingEdges);
        }
        Ok(Self {
            participating_edges: participating_edges.into_iter().collect(),
            idle_edges: BTreeSet::new(),
            budget,
            alignment_timeout_ms: alignment_timeout_ms.max(1),
            reachable_sinks: reachable_sinks.into_iter().collect(),
            active: None,
            outstanding_lookups: BTreeMap::new(),
            watermarks: BTreeMap::new(),
            pending_sink_acks: BTreeMap::new(),
        })
    }

    pub fn mark_edge_idle(
        &mut self,
        edge: &EdgeId,
        idle: bool,
    ) -> Result<(), BarrierAlignmentError> {
        if !self.participating_edges.contains(edge) {
            return Err(BarrierAlignmentError::UnknownEdge(*edge));
        }
        if idle {
            self.idle_edges.insert(*edge);
        } else {
            self.idle_edges.remove(edge);
        }
        Ok(())
    }

    pub fn register_lookup(&mut self, lookup_id: u64) {
        self.outstanding_lookups
            .insert(lookup_id, LookupStatus::InFlight);
    }

    pub fn complete_lookup(&mut self, lookup_id: u64, recorded_result: Option<Value>) {
        if let Some(status) = self.outstanding_lookups.get_mut(&lookup_id) {
            *status = LookupStatus::Completed(recorded_result);
        }
    }

    pub fn snapshot_lookup(&mut self, lookup_id: u64, recorded_result: Value) {
        if let Some(status) = self.outstanding_lookups.get_mut(&lookup_id) {
            *status = LookupStatus::Snapshotted(recorded_result);
        }
    }

    pub fn in_flight_lookup_count(&self) -> usize {
        self.outstanding_lookups
            .values()
            .filter(|s| matches!(s, LookupStatus::InFlight))
            .count()
    }

    pub fn push_envelope(
        &mut self,
        edge: EdgeId,
        envelope: OrderedEnvelope<T>,
        byte_size: u64,
        now_ms: i64,
    ) -> Result<AlignmentOutcome<T>, BarrierAlignmentError> {
        if !self.participating_edges.contains(&edge) {
            return Err(BarrierAlignmentError::UnknownEdge(edge));
        }

        // An edge producing an envelope is active (not idle).
        self.idle_edges.remove(&edge);

        match envelope {
            OrderedEnvelope::Data(item) => {
                if let Some(active) = self.active.as_mut() {
                    if active.paused_edges.contains(&edge) {
                        let reservation = self
                            .budget
                            .try_reserve(BudgetCharge::new(1, byte_size, 0))
                            .map_err(|e: BudgetError| {
                                BarrierAlignmentError::PausedBufferBudgetExceeded {
                                    edge,
                                    reason: e.to_string(),
                                }
                            })?;
                        let buf = active.paused_buffers.entry(edge).or_default();
                        buf.push(PausedBufferEntry::Data(item, reservation));
                        return Ok(AlignmentOutcome::BufferedPaused {
                            edge,
                            buffered_on_edge: buf.len(),
                        });
                    }
                }
                Ok(AlignmentOutcome::ForwardData { edge, item })
            }
            OrderedEnvelope::Watermark {
                edge: wm_edge,
                timestamp,
            } => {
                if let Some(active) = self.active.as_mut() {
                    if active.paused_edges.contains(&edge) {
                        let buf = active.paused_buffers.entry(edge).or_default();
                        buf.push(PausedBufferEntry::Watermark {
                            edge: wm_edge,
                            timestamp,
                        });
                        return Ok(AlignmentOutcome::BufferedPaused {
                            edge,
                            buffered_on_edge: buf.len(),
                        });
                    }
                }
                self.watermarks.insert(wm_edge, timestamp);
                Ok(AlignmentOutcome::Watermark {
                    edge: wm_edge,
                    timestamp,
                })
            }
            OrderedEnvelope::Barrier {
                checkpoint,
                generation,
            } => {
                if let Some(active) = self.active.as_ref() {
                    if active.checkpoint != checkpoint {
                        return Err(BarrierAlignmentError::ConflictingCheckpoint {
                            active: active.checkpoint,
                            incoming: checkpoint,
                        });
                    }
                } else {
                    self.active = Some(ActiveAlignment {
                        checkpoint,
                        generation,
                        deadline_ms: now_ms.saturating_add(self.alignment_timeout_ms),
                        arrived_barriers: BTreeSet::new(),
                        paused_edges: BTreeSet::new(),
                        paused_buffers: BTreeMap::new(),
                    });
                }

                if let Some(active) = self.active.as_mut() {
                    active.arrived_barriers.insert(edge);
                    active.paused_edges.insert(edge);
                }

                Ok(self.try_finish_alignment())
            }
            OrderedEnvelope::EndOfInput { .. } => {
                self.idle_edges.insert(edge);
                if self.active.is_some() {
                    Ok(self.try_finish_alignment())
                } else {
                    Ok(AlignmentOutcome::EndOfInput { edge })
                }
            }
        }
    }

    /// Re-evaluates whether an active alignment can now complete (e.g., after
    /// an edge becomes idle or the last in-flight lookup completes/snapshots).
    pub fn try_complete(&mut self) -> Option<AlignmentOutcome<T>> {
        self.active.as_ref()?;
        Some(self.try_finish_alignment())
    }

    fn try_finish_alignment(&mut self) -> AlignmentOutcome<T> {
        let expected_edges: Vec<EdgeId> = self
            .participating_edges
            .difference(&self.idle_edges)
            .cloned()
            .collect();
        let in_flight = self.in_flight_lookup_count();

        let ready = {
            let active = self.active.as_ref().expect("active alignment exists");
            let all_barriers_arrived = expected_edges
                .iter()
                .all(|e| active.arrived_barriers.contains(e));
            all_barriers_arrived && in_flight == 0
        };

        if !ready {
            let active = self.active.as_ref().expect("active alignment exists");
            return AlignmentOutcome::WaitingForBarriers {
                checkpoint: active.checkpoint,
                arrived: active.arrived_barriers.len(),
                expected: expected_edges.len(),
                pending_lookups: in_flight,
            };
        }

        let active = self.active.take().expect("active alignment exists");
        let drained = self.drain_paused_entries(active.paused_buffers);

        let mut recorded_lookups = BTreeMap::new();
        for (id, status) in std::mem::take(&mut self.outstanding_lookups) {
            match status {
                LookupStatus::Completed(Some(val)) | LookupStatus::Snapshotted(val) => {
                    recorded_lookups.insert(id, val);
                }
                LookupStatus::Completed(None) | LookupStatus::InFlight => {}
            }
        }

        let target_sinks: Vec<String> = self.reachable_sinks.iter().cloned().collect();
        if !self.reachable_sinks.is_empty() {
            self.pending_sink_acks
                .insert(active.checkpoint, self.reachable_sinks.clone());
        }

        AlignmentOutcome::Aligned(AlignedBarrier {
            checkpoint: active.checkpoint,
            generation: active.generation,
            drained_paused_data: drained.data,
            drained_paused_watermarks: drained.watermarks,
            recorded_lookups,
            target_sinks,
        })
    }

    fn drain_paused_entries(
        &mut self,
        paused_buffers: BTreeMap<EdgeId, Vec<PausedBufferEntry<T>>>,
    ) -> DrainedPausedEntries<T> {
        let mut data = Vec::new();
        let mut watermarks = Vec::new();
        for (edge, entries) in paused_buffers {
            for entry in entries {
                match entry {
                    PausedBufferEntry::Data(item, _reservation) => {
                        data.push((edge, item));
                    }
                    PausedBufferEntry::Watermark {
                        edge: wm_edge,
                        timestamp,
                    } => {
                        self.watermarks.insert(wm_edge, timestamp);
                        watermarks.push((wm_edge, timestamp));
                    }
                }
            }
        }
        DrainedPausedEntries { data, watermarks }
    }

    pub fn poll_timeout(&mut self, now_ms: i64) -> Option<AbortedAlignment<T>> {
        let expired = self
            .active
            .as_ref()
            .is_some_and(|a| now_ms >= a.deadline_ms);
        if !expired {
            return None;
        }
        let reason = if self.in_flight_lookup_count() > 0 {
            AlignmentAbortReason::StalledLookupTimeout
        } else {
            AlignmentAbortReason::Timeout
        };
        self.abort(reason)
    }

    pub fn abort(&mut self, reason: AlignmentAbortReason) -> Option<AbortedAlignment<T>> {
        let active = self.active.take()?;
        let released = self.drain_paused_entries(active.paused_buffers);

        let mut cancelled_lookups = Vec::new();
        self.outstanding_lookups.retain(|id, status| {
            if matches!(status, LookupStatus::InFlight) {
                cancelled_lookups.push(*id);
                false
            } else {
                true
            }
        });

        Some(AbortedAlignment {
            checkpoint: active.checkpoint,
            generation: active.generation,
            reason,
            released_paused_data: released.data,
            released_paused_watermarks: released.watermarks,
            cancelled_lookups,
        })
    }

    pub fn watermark(&self, edge: &EdgeId) -> Option<EventTimestamp> {
        self.watermarks.get(edge).copied()
    }

    /// Marks a reachable sink as having completed checkpoint `checkpoint`.
    /// Returns `true` when every reachable sink has acknowledged completion.
    pub fn ack_sink_completion(&mut self, checkpoint: CheckpointId, sink_id: &str) -> bool {
        let Some(remaining) = self.pending_sink_acks.get_mut(&checkpoint) else {
            return false;
        };
        remaining.remove(sink_id);
        if remaining.is_empty() {
            self.pending_sink_acks.remove(&checkpoint);
            true
        } else {
            false
        }
    }

    pub fn is_paused(&self, edge: &EdgeId) -> bool {
        self.active
            .as_ref()
            .is_some_and(|a| a.paused_edges.contains(edge))
    }

    pub fn budget_usage_items(&self) -> u64 {
        self.budget.usage().reserved_items
    }
}
