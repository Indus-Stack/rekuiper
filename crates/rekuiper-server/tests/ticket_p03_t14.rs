use rekuiper_core::contracts::{CheckpointId, EdgeId, Generation, OrderedEnvelope};
use rekuiper_core::resources::budget::{BudgetLimits, ResourceBudget};
use rekuiper_server::engine::barrier_alignment::{
    validate_acyclic_reliable_graph, AlignmentAbortReason, AlignmentOutcome, BarrierAligner,
    BarrierAlignmentError,
};
use serde_json::json;

const FAST: EdgeId = EdgeId(1);
const SLOW: EdgeId = EdgeId(2);
const IDLE: EdgeId = EdgeId(3);
const IN1: EdgeId = EdgeId(10);

#[test]
fn fast_slow_idle_inputs_align_without_deadlock_and_propagate_to_all_sinks() {
    let budget = ResourceBudget::new(BudgetLimits {
        max_items: Some(2),
        max_logical_bytes: Some(1024),
        max_tasks: None,
    });
    let mut aligner = BarrierAligner::new(
        vec![FAST, SLOW, IDLE],
        vec!["sink_a".to_string(), "sink_b".to_string()],
        budget,
        1_000,
    )
    .unwrap();

    // Mark "idle" edge idle so fast+slow can align without waiting forever
    aligner.mark_edge_idle(&IDLE, true).unwrap();

    // Fast barrier arrives first
    let res = aligner
        .push_envelope(
            FAST,
            OrderedEnvelope::Barrier {
                checkpoint: CheckpointId(1),
                generation: Generation(1),
            },
            0,
            100,
        )
        .unwrap();
    assert!(matches!(
        res,
        AlignmentOutcome::WaitingForBarriers {
            arrived: 1,
            expected: 2,
            ..
        }
    ));

    // Post-barrier data on fast edge is buffered (bounded), not forwarded across barrier
    let buf1 = aligner
        .push_envelope(FAST, OrderedEnvelope::Data("fast_after_b1"), 64, 110)
        .unwrap();
    assert!(matches!(
        buf1,
        AlignmentOutcome::BufferedPaused {
            buffered_on_edge: 1,
            ..
        }
    ));
    let buf2 = aligner
        .push_envelope(FAST, OrderedEnvelope::Data("fast_after_b2"), 64, 120)
        .unwrap();
    assert!(matches!(
        buf2,
        AlignmentOutcome::BufferedPaused {
            buffered_on_edge: 2,
            ..
        }
    ));

    // Third paused item exceeds budget (max_items = 2)
    let over = aligner.push_envelope(FAST, OrderedEnvelope::Data("overflow"), 64, 130);
    assert!(matches!(
        over,
        Err(BarrierAlignmentError::PausedBufferBudgetExceeded { .. })
    ));

    // Slow pre-barrier data still forwards normally
    let slow_pre = aligner
        .push_envelope(SLOW, OrderedEnvelope::Data("slow_pre_b1"), 64, 140)
        .unwrap();
    assert_eq!(
        slow_pre,
        AlignmentOutcome::ForwardData {
            edge: SLOW,
            item: "slow_pre_b1"
        }
    );

    // Slow barrier completes alignment and drains buffered paused data in order; budget releases
    let aligned = aligner
        .push_envelope(
            SLOW,
            OrderedEnvelope::Barrier {
                checkpoint: CheckpointId(1),
                generation: Generation(1),
            },
            0,
            150,
        )
        .unwrap();
    match aligned {
        AlignmentOutcome::Aligned(bar) => {
            assert_eq!(bar.checkpoint, CheckpointId(1));
            assert_eq!(
                bar.drained_paused_data,
                vec![(FAST, "fast_after_b1"), (FAST, "fast_after_b2"),]
            );
            assert_eq!(bar.target_sinks, vec!["sink_a", "sink_b"]);
        }
        other => panic!("expected Aligned, got {other:?}"),
    }
    assert_eq!(aligner.budget_usage_items(), 0);

    // Completion must reach every reachable sink
    assert!(!aligner.ack_sink_completion(CheckpointId(1), "sink_a"));
    assert!(aligner.ack_sink_completion(CheckpointId(1), "sink_b"));
}

#[test]
fn stalled_lookup_blocks_commit_and_timeout_unblocks_and_cancels_cleanly() {
    let budget = ResourceBudget::new(BudgetLimits {
        max_items: Some(8),
        max_logical_bytes: Some(2048),
        max_tasks: None,
    });
    let mut aligner =
        BarrierAligner::new(vec![IN1], vec!["sink1".to_string()], budget, 500).unwrap();

    aligner.register_lookup(42);
    let wait = aligner
        .push_envelope(
            IN1,
            OrderedEnvelope::Barrier {
                checkpoint: CheckpointId(2),
                generation: Generation(1),
            },
            0,
            1_000,
        )
        .unwrap();
    assert!(matches!(
        wait,
        AlignmentOutcome::WaitingForBarriers {
            pending_lookups: 1,
            ..
        }
    ));

    // Buffer post-barrier item while paused waiting for lookup
    aligner
        .push_envelope(IN1, OrderedEnvelope::Data("paused_item"), 32, 1_100)
        .unwrap();
    assert_eq!(aligner.budget_usage_items(), 1);

    // Timeout at 1_500 aborts alignment, releases paused data, cancels stalled lookup, frees budget
    let aborted = aligner.poll_timeout(1_500).expect("should timeout");
    assert_eq!(aborted.reason, AlignmentAbortReason::StalledLookupTimeout);
    assert_eq!(aborted.released_paused_data, vec![(IN1, "paused_item")]);
    assert_eq!(aborted.cancelled_lookups, vec![42]);
    assert_eq!(aligner.budget_usage_items(), 0);
    assert!(!aligner.is_paused(&IN1));

    // Snapshot recorded lookup allows barrier crossing on next checkpoint
    aligner.register_lookup(99);
    aligner.snapshot_lookup(99, json!({"cached": true}));
    let aligned = aligner
        .push_envelope(
            IN1,
            OrderedEnvelope::Barrier {
                checkpoint: CheckpointId(3),
                generation: Generation(1),
            },
            0,
            2_000,
        )
        .unwrap();
    match aligned {
        AlignmentOutcome::Aligned(bar) => {
            assert_eq!(
                bar.recorded_lookups.get(&99),
                Some(&json!({"cached": true}))
            );
        }
        other => panic!("expected Aligned, got {other:?}"),
    }
}

#[test]
fn reliable_feedback_cycles_are_rejected_in_v1() {
    let nodes = vec!["src".to_string(), "op1".to_string(), "op2".to_string()];
    assert!(validate_acyclic_reliable_graph(
        &nodes,
        &[
            ("src".to_string(), "op1".to_string()),
            ("op1".to_string(), "op2".to_string()),
        ]
    )
    .is_ok());

    let cyclic = validate_acyclic_reliable_graph(
        &nodes,
        &[
            ("src".to_string(), "op1".to_string()),
            ("op1".to_string(), "op2".to_string()),
            ("op2".to_string(), "op1".to_string()),
        ],
    );
    assert!(matches!(
        cyclic,
        Err(BarrierAlignmentError::FeedbackCycleDetected { .. })
    ));
}
