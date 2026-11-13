//! Data and controls that share the reliable execution queue.

use super::identity::{CheckpointId, Generation};
use serde::{Deserialize, Serialize};

/// Identity assigned to a directed edge within one plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EdgeId(pub u64);

/// Unix event time with its declared precision. OTLP's unsigned nanoseconds
/// remain distinct from milliseconds and retain their full range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "unit", content = "value", rename_all = "snake_case")]
pub enum EventTimestamp {
    Seconds(i64),
    Millis(i64),
    Micros(i64),
    Nanos(i64),
    OtlpNanos(u64),
}

/// Enqueue data and controls on the same FIFO path to preserve their order.
/// Reliable v1 plans require an acyclic topology.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderedEnvelope<T> {
    Data(T),
    Barrier {
        checkpoint: CheckpointId,
        generation: Generation,
    },
    Watermark {
        edge: EdgeId,
        timestamp: EventTimestamp,
    },
    EndOfInput {
        edge: EdgeId,
    },
}
