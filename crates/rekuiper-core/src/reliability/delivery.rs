//! Idempotent delivery accounting and contiguous partition progress.

use crate::contracts::SourcePosition;
use std::collections::{BTreeSet, VecDeque};

/// Disposition of one fanout branch produced from a source message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BranchOutcome {
    /// Delivered to destination, intentionally filtered, or durably committed in state/outbox.
    TerminalSuccess,
    /// Failed permanently or awaiting retry; blocks source position completion.
    Failed,
}

#[derive(Debug)]
struct TrackedMessage {
    position: SourcePosition,
    expected_branches: usize,
    completed_branches: BTreeSet<usize>,
    failed: bool,
    terminal_complete: bool,
}

impl TrackedMessage {
    fn is_complete(&self) -> bool {
        self.terminal_complete
    }
}

/// Tracks in-flight source messages for one partition, deduplicating branch
/// callbacks and advancing contiguous progress only across completed prefixes.
#[derive(Debug, Default)]
pub struct PartitionDeliveryTracker {
    in_flight: VecDeque<TrackedMessage>,
    contiguous_committed: Option<SourcePosition>,
}

impl PartitionDeliveryTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers an ordered source message that expands into `expected_branches`
    /// downstream records/branches. When `expected_branches == 0` (e.g. an
    /// intentional filter), the message is immediately terminal-complete and
    /// advances the contiguous frontier if no earlier message is still pending.
    pub fn register_message(&mut self, position: SourcePosition, expected_branches: usize) -> u64 {
        let token = self.in_flight.len() as u64;
        let terminal_complete = expected_branches == 0;
        self.in_flight.push_back(TrackedMessage {
            position,
            expected_branches,
            completed_branches: BTreeSet::new(),
            failed: false,
            terminal_complete,
        });
        self.advance_frontier();
        token
    }

    /// Records a branch outcome idempotently. Returns `true` if this callback
    /// newly changed state, or `false` if it was a duplicate/unknown callback.
    pub fn complete_branch(
        &mut self,
        position: &SourcePosition,
        branch_index: usize,
        outcome: BranchOutcome,
    ) -> bool {
        let Some(entry) = self.in_flight.iter_mut().find(|m| &m.position == position) else {
            return false;
        };

        if branch_index >= entry.expected_branches {
            return false;
        }

        match outcome {
            BranchOutcome::Failed => {
                if entry.failed {
                    return false;
                }
                entry.failed = true;
                entry.terminal_complete = false;
                true
            }
            BranchOutcome::TerminalSuccess => {
                if !entry.completed_branches.insert(branch_index) {
                    return false;
                }
                if !entry.failed && entry.completed_branches.len() == entry.expected_branches {
                    entry.terminal_complete = true;
                    self.advance_frontier();
                }
                true
            }
        }
    }

    /// Returns the highest contiguous completed source position for this partition.
    pub fn contiguous_position(&self) -> Option<&SourcePosition> {
        self.contiguous_committed.as_ref()
    }

    /// Number of messages still ahead of or blocking the contiguous frontier.
    pub fn pending_count(&self) -> usize {
        self.in_flight.len()
    }

    fn advance_frontier(&mut self) {
        while self
            .in_flight
            .front()
            .is_some_and(TrackedMessage::is_complete)
        {
            if let Some(done) = self.in_flight.pop_front() {
                self.contiguous_committed = Some(done.position);
            }
        }
    }
}
