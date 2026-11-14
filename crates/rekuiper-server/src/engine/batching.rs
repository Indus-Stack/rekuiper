//! Bounded count, encoded-byte, and deadline action batch assembly.
//!
//! Preserves legacy `batchSize`/`linger` behavior when `max_bytes` is `None`.

use rekuiper_core::resources::{BudgetCharge, BudgetCommit, BudgetError, ResourceBudget};
use std::collections::VecDeque;
use std::time::Duration;

/// Batching thresholds for an action sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchConfig {
    /// Maximum item count per batch (`batchSize`).
    pub max_count: usize,
    /// Optional maximum encoded bytes per batch. `None` preserves legacy count/linger behavior.
    pub max_bytes: Option<usize>,
    /// Optional linger deadline from the first buffered item.
    pub linger: Option<Duration>,
}

impl BatchConfig {
    pub fn legacy(batch_size: usize, linger: Option<Duration>) -> Self {
        Self {
            max_count: batch_size.max(1),
            max_bytes: None,
            linger,
        }
    }
}

/// Reason a batch became ready to flush.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushReason {
    Count,
    Bytes,
    Deadline,
    Barrier,
    Shutdown,
}

/// Result of attempting to admit an item into [`ActionBatchAssembler`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushDecision {
    /// Item was buffered; no threshold reached yet.
    Buffered,
    /// Item was buffered and reached a flush threshold.
    FlushNow(FlushReason),
    /// Adding this item would exceed `max_count` or `max_bytes`; caller must flush
    /// the current batch first, then re-push this item.
    FlushCurrentFirst(FlushReason),
}

/// One item queued in a batch with its encoded byte size and optional budget commit.
#[derive(Debug)]
pub struct BatchItem<T> {
    pub seq: u64,
    pub encoded_bytes: usize,
    pub payload: T,
    budget_commit: Option<BudgetCommit>,
}

impl<T> BatchItem<T> {
    pub fn new(seq: u64, encoded_bytes: usize, payload: T) -> Self {
        Self {
            seq,
            encoded_bytes,
            payload,
            budget_commit: None,
        }
    }
}

/// Per-item delivery result returned by a batch sink adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ItemOutcome {
    Delivered,
    Retryable,
    PermanentFailure(String),
}

/// Summary after mapping per-item outcomes for a flushed batch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BatchOutcomeSummary {
    pub delivered: usize,
    pub permanent_failures: usize,
    pub retained_retryable: usize,
}

/// Errors produced by [`ActionBatchAssembler`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchError {
    OversizeRecord {
        encoded_bytes: usize,
        max_bytes: usize,
    },
    Budget(BudgetError),
}

impl std::fmt::Display for BatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OversizeRecord {
                encoded_bytes,
                max_bytes,
            } => write!(
                f,
                "record size {encoded_bytes} exceeds batch byte limit {max_bytes}"
            ),
            Self::Budget(err) => write!(f, "batch budget error: {err}"),
        }
    }
}

impl std::error::Error for BatchError {}

impl From<BudgetError> for BatchError {
    fn from(value: BudgetError) -> Self {
        Self::Budget(value)
    }
}

/// Ordered action batch assembler enforcing count, byte, deadline, barrier,
/// and budget invariants.
#[derive(Debug)]
pub struct ActionBatchAssembler<T> {
    config: BatchConfig,
    items: VecDeque<BatchItem<T>>,
    pending_bytes: usize,
    first_item_elapsed: Option<Duration>,
}

impl<T> ActionBatchAssembler<T> {
    pub fn new(config: BatchConfig) -> Self {
        Self {
            config: BatchConfig {
                max_count: config.max_count.max(1),
                ..config
            },
            items: VecDeque::new(),
            pending_bytes: 0,
            first_item_elapsed: None,
        }
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn pending_bytes(&self) -> usize {
        self.pending_bytes
    }

    /// Admits an item into the batch, rejecting oversize items and checking
    /// optional `ResourceBudget` capacity before buffering.
    pub fn try_push(
        &mut self,
        mut item: BatchItem<T>,
        now_elapsed: Duration,
        budget: Option<&ResourceBudget>,
    ) -> Result<PushDecision, BatchError> {
        if let Some(max_bytes) = self.config.max_bytes {
            if item.encoded_bytes > max_bytes {
                return Err(BatchError::OversizeRecord {
                    encoded_bytes: item.encoded_bytes,
                    max_bytes,
                });
            }
            if !self.items.is_empty()
                && self.pending_bytes.saturating_add(item.encoded_bytes) > max_bytes
            {
                return Ok(PushDecision::FlushCurrentFirst(FlushReason::Bytes));
            }
        }

        if self.items.len() >= self.config.max_count {
            return Ok(PushDecision::FlushCurrentFirst(FlushReason::Count));
        }

        if let Some(b) = budget {
            let res = b.try_reserve(BudgetCharge::new(1, item.encoded_bytes as u64, 0))?;
            item.budget_commit = Some(res.commit());
        }

        if self.items.is_empty() {
            self.first_item_elapsed = Some(now_elapsed);
        }
        self.pending_bytes = self.pending_bytes.saturating_add(item.encoded_bytes);
        self.items.push_back(item);

        if self.items.len() >= self.config.max_count {
            Ok(PushDecision::FlushNow(FlushReason::Count))
        } else if self
            .config
            .max_bytes
            .is_some_and(|max| self.pending_bytes >= max)
        {
            Ok(PushDecision::FlushNow(FlushReason::Bytes))
        } else {
            Ok(PushDecision::Buffered)
        }
    }

    /// Checks whether the linger deadline has elapsed for the current batch.
    pub fn check_deadline(&self, now_elapsed: Duration) -> Option<FlushReason> {
        let linger = self.config.linger?;
        let first = self.first_item_elapsed?;
        if now_elapsed.saturating_sub(first) >= linger && !self.items.is_empty() {
            Some(FlushReason::Deadline)
        } else {
            None
        }
    }

    /// A checkpoint barrier cannot overtake buffered items; returns `Some(Barrier)`
    /// when a flush is required before advancing past the barrier.
    pub fn on_barrier(&self) -> Option<FlushReason> {
        (!self.items.is_empty()).then_some(FlushReason::Barrier)
    }

    /// EOF/shutdown requires flushing any buffered items.
    pub fn on_shutdown(&self) -> Option<FlushReason> {
        (!self.items.is_empty()).then_some(FlushReason::Shutdown)
    }

    /// Drains all buffered items in exact FIFO order.
    pub fn take_batch(&mut self) -> Vec<BatchItem<T>> {
        self.pending_bytes = 0;
        self.first_item_elapsed = None;
        self.items.drain(..).collect()
    }

    /// Maps per-item outcomes: delivered and permanent failures are settled
    /// (releasing budget charges), while `Retryable` items are retained at the
    /// front of the buffer in original order.
    pub fn apply_outcomes(
        &mut self,
        batch: Vec<BatchItem<T>>,
        outcomes: &[ItemOutcome],
        now_elapsed: Duration,
    ) -> BatchOutcomeSummary {
        let mut summary = BatchOutcomeSummary::default();
        let mut retry_items = Vec::new();

        for (idx, item) in batch.into_iter().enumerate() {
            let outcome = outcomes.get(idx).unwrap_or(&ItemOutcome::Retryable);
            match outcome {
                ItemOutcome::Delivered => {
                    summary.delivered += 1;
                    drop(item);
                }
                ItemOutcome::PermanentFailure(_) => {
                    summary.permanent_failures += 1;
                    drop(item);
                }
                ItemOutcome::Retryable => {
                    summary.retained_retryable += 1;
                    retry_items.push(item);
                }
            }
        }

        if !retry_items.is_empty() {
            if self.first_item_elapsed.is_none() {
                self.first_item_elapsed = Some(now_elapsed);
            }
            for item in retry_items.into_iter().rev() {
                self.pending_bytes = self.pending_bytes.saturating_add(item.encoded_bytes);
                self.items.push_front(item);
            }
        }

        summary
    }
}
