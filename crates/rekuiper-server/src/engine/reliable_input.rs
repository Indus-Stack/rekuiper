//! Ordered reliable source ingress queue with generation fencing and
//! per-partition multirow delivery accounting.

use parking_lot::Mutex;
use rekuiper_core::contracts::{
    CheckpointId, DeadlineCancellation, EdgeId, EventTimestamp, Generation, OrderedEnvelope,
    SourcePosition,
};
use rekuiper_core::model::StreamRecord;
use rekuiper_core::reliability::{BranchOutcome, PartitionDeliveryTracker};
use tokio::sync::mpsc;

/// One source message (possibly expanded into 0, 1, or N rows) sharing a single
/// source position.
#[derive(Debug, Clone)]
pub struct ReliableInputItem {
    pub position: SourcePosition,
    pub rows: Vec<StreamRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReliableIngressError {
    StaleGeneration {
        expected: Generation,
        actual: Generation,
    },
    Cancelled,
    Closed,
}

impl std::fmt::Display for ReliableIngressError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StaleGeneration { expected, actual } => {
                write!(
                    f,
                    "stale ownership generation: expected {expected:?}, got {actual:?}"
                )
            }
            Self::Cancelled => f.write_str("reliable ingress admission cancelled"),
            Self::Closed => f.write_str("reliable ingress channel closed"),
        }
    }
}

impl std::error::Error for ReliableIngressError {}

/// Bounded, ordered, generation-fenced reliable ingress queue.
/// Leaves the default `StreamReceiver` fast path untouched.
pub struct ReliableInputSender {
    generation: Generation,
    tx: mpsc::Sender<OrderedEnvelope<ReliableInputItem>>,
    tracker: Mutex<PartitionDeliveryTracker>,
}

pub struct ReliableInputReceiver {
    rx: mpsc::Receiver<OrderedEnvelope<ReliableInputItem>>,
}

pub fn reliable_input_channel(
    generation: Generation,
    admission_limit: usize,
) -> (ReliableInputSender, ReliableInputReceiver) {
    let cap = admission_limit.max(1);
    let (tx, rx) = mpsc::channel(cap);
    (
        ReliableInputSender {
            generation,
            tx,
            tracker: Mutex::new(PartitionDeliveryTracker::new()),
        },
        ReliableInputReceiver { rx },
    )
}

impl ReliableInputSender {
    pub fn generation(&self) -> Generation {
        self.generation
    }

    /// Admits a valid multirow source message in FIFO order before any subsequent
    /// barrier or EOF, enforcing ownership generation and cancellation.
    pub async fn admit_data(
        &self,
        item: ReliableInputItem,
        cancellation: &DeadlineCancellation,
    ) -> Result<(), ReliableIngressError> {
        if item.position.generation != self.generation {
            return Err(ReliableIngressError::StaleGeneration {
                expected: self.generation,
                actual: item.position.generation,
            });
        }
        let pos = item.position.clone();
        let branches = item.rows.len();

        self.send_envelope(OrderedEnvelope::Data(item), cancellation)
            .await?;
        self.tracker.lock().register_message(pos, branches);
        Ok(())
    }

    /// Records a malformed input message at `position` without admitting rows.
    /// Until `record_malformed_disposition` is called with `TerminalSuccess`
    /// (e.g. after durable DLQ disposition), this position blocks contiguous
    /// progress and cannot silently advance.
    pub fn register_malformed(&self, position: SourcePosition) -> Result<(), ReliableIngressError> {
        if position.generation != self.generation {
            return Err(ReliableIngressError::StaleGeneration {
                expected: self.generation,
                actual: position.generation,
            });
        }
        let mut tracker = self.tracker.lock();
        tracker.register_message(position.clone(), 1);
        tracker.complete_branch(&position, 0, BranchOutcome::Failed);
        Ok(())
    }

    /// Marks terminal disposition for a previously registered row branch or
    /// malformed message.
    pub fn complete_branch(
        &self,
        position: &SourcePosition,
        branch_index: usize,
        outcome: BranchOutcome,
    ) -> bool {
        self.tracker
            .lock()
            .complete_branch(position, branch_index, outcome)
    }

    /// Explicit terminal disposition for a malformed message (e.g. after DLQ/halt).
    pub fn complete_malformed_disposition(&self, position: &SourcePosition) -> bool {
        let mut tracker = self.tracker.lock();
        // Clear any prior Failed mark by registering terminal disposition only if
        // we allow explicit terminal override; in PartitionDeliveryTracker, Failed
        // intentionally blocks until resolved. Here we record TerminalSuccess on a
        // fresh single-branch slot or via a dedicated helper if needed.
        tracker.complete_branch(position, 0, BranchOutcome::TerminalSuccess)
    }

    pub fn contiguous_position(&self) -> Option<SourcePosition> {
        self.tracker.lock().contiguous_position().cloned()
    }

    pub async fn admit_barrier(
        &self,
        checkpoint: CheckpointId,
        generation: Generation,
        cancellation: &DeadlineCancellation,
    ) -> Result<(), ReliableIngressError> {
        if generation != self.generation {
            return Err(ReliableIngressError::StaleGeneration {
                expected: self.generation,
                actual: generation,
            });
        }
        self.send_envelope(
            OrderedEnvelope::Barrier {
                checkpoint,
                generation,
            },
            cancellation,
        )
        .await
    }

    pub async fn admit_watermark(
        &self,
        edge: EdgeId,
        timestamp: EventTimestamp,
        cancellation: &DeadlineCancellation,
    ) -> Result<(), ReliableIngressError> {
        self.send_envelope(OrderedEnvelope::Watermark { edge, timestamp }, cancellation)
            .await
    }

    pub async fn admit_eof(
        &self,
        edge: EdgeId,
        cancellation: &DeadlineCancellation,
    ) -> Result<(), ReliableIngressError> {
        self.send_envelope(OrderedEnvelope::EndOfInput { edge }, cancellation)
            .await
    }

    async fn send_envelope(
        &self,
        envelope: OrderedEnvelope<ReliableInputItem>,
        cancellation: &DeadlineCancellation,
    ) -> Result<(), ReliableIngressError> {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => Err(ReliableIngressError::Cancelled),
            res = self.tx.send(envelope) => res.map_err(|_| ReliableIngressError::Closed),
        }
    }
}

impl ReliableInputReceiver {
    pub async fn recv(&mut self) -> Option<OrderedEnvelope<ReliableInputItem>> {
        self.rx.recv().await
    }
}
