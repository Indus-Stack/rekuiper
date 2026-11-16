use rekuiper_core::contracts::{
    Generation, PartitionId, PositionCodecId, SourceId, SourcePartition, SourcePosition,
};
use rekuiper_core::reliability::{BranchOutcome, PartitionDeliveryTracker};

fn pos(offset: i64) -> SourcePosition {
    SourcePosition {
        partition: SourcePartition {
            source_id: SourceId::try_from("kafka_in").unwrap(),
            partition_id: PartitionId::try_from("p0").unwrap(),
        },
        generation: Generation(1),
        codec: PositionCodecId::try_from("kafka_offset_v1").unwrap(),
        cursor: offset.to_be_bytes().to_vec(),
    }
}

#[test]
fn out_of_order_completions_never_skip_missing_offsets() {
    let mut tracker = PartitionDeliveryTracker::new();
    let p1 = pos(1);
    let p2 = pos(2);
    let p3 = pos(3);

    tracker.register_message(p1.clone(), 1);
    tracker.register_message(p2.clone(), 1);
    tracker.register_message(p3.clone(), 1);

    // Complete offset 3 first: frontier must remain None (cannot skip 1 and 2).
    assert!(tracker.complete_branch(&p3, 0, BranchOutcome::TerminalSuccess));
    assert_eq!(tracker.contiguous_position(), None);

    // Complete offset 1: frontier advances to 1, still blocked on 2.
    assert!(tracker.complete_branch(&p1, 0, BranchOutcome::TerminalSuccess));
    assert_eq!(tracker.contiguous_position(), Some(&p1));

    // Complete offset 2: frontier jumps cleanly to 3.
    assert!(tracker.complete_branch(&p2, 0, BranchOutcome::TerminalSuccess));
    assert_eq!(tracker.contiguous_position(), Some(&p3));
    assert_eq!(tracker.pending_count(), 0);
}

#[test]
fn failed_fanout_branch_blocks_completion_and_duplicates_are_ignored() {
    let mut tracker = PartitionDeliveryTracker::new();
    let p1 = pos(1);
    tracker.register_message(p1.clone(), 2);

    // Duplicate callback for branch 0 does not count twice toward 2 branches.
    assert!(tracker.complete_branch(&p1, 0, BranchOutcome::TerminalSuccess));
    assert!(!tracker.complete_branch(&p1, 0, BranchOutcome::TerminalSuccess));
    assert_eq!(tracker.contiguous_position(), None);

    // Branch 1 fails: position cannot complete.
    assert!(tracker.complete_branch(&p1, 1, BranchOutcome::Failed));
    assert_eq!(tracker.contiguous_position(), None);
    assert_eq!(tracker.pending_count(), 1);
}

#[test]
fn zero_output_intentional_filter_terminates_immediately() {
    let mut tracker = PartitionDeliveryTracker::new();
    let p1 = pos(1);
    let p2 = pos(2);

    tracker.register_message(p1.clone(), 0);
    assert_eq!(tracker.contiguous_position(), Some(&p1));

    // Filtered message behind a pending message waits for the pending message first.
    let p3 = pos(3);
    tracker.register_message(p2.clone(), 1);
    tracker.register_message(p3.clone(), 0);
    assert_eq!(tracker.contiguous_position(), Some(&p1));

    assert!(tracker.complete_branch(&p2, 0, BranchOutcome::TerminalSuccess));
    assert_eq!(tracker.contiguous_position(), Some(&p3));
    assert_eq!(tracker.pending_count(), 0);
}
