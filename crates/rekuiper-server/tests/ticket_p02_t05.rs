use rekuiper_core::contracts::{Clock, TestClock};
use rekuiper_core::resources::{BudgetLimits, ResourceBudget};
use rekuiper_server::engine::batching::{
    ActionBatchAssembler, BatchConfig, BatchError, BatchItem, FlushReason, ItemOutcome,
    PushDecision,
};
use std::time::Duration;

#[test]
fn count_byte_deadline_and_barrier_thresholds_work_with_budget_and_partial_retry() {
    let clock = TestClock::new(1_700_000_000_000);
    let budget = ResourceBudget::new(BudgetLimits {
        max_items: Some(10),
        max_logical_bytes: Some(100),
        max_tasks: None,
    });

    let mut assembler = ActionBatchAssembler::new(BatchConfig {
        max_count: 3,
        max_bytes: Some(50),
        linger: Some(Duration::from_millis(200)),
    });

    // Oversize item fails immediately.
    assert_eq!(
        assembler
            .try_push(
                BatchItem::new(0, 60, "huge"),
                clock.now().elapsed,
                Some(&budget)
            )
            .unwrap_err(),
        BatchError::OversizeRecord {
            encoded_bytes: 60,
            max_bytes: 50
        }
    );

    // Push 2 items (30 bytes total) -> Buffered.
    assert_eq!(
        assembler
            .try_push(
                BatchItem::new(1, 15, "a"),
                clock.now().elapsed,
                Some(&budget)
            )
            .unwrap(),
        PushDecision::Buffered
    );
    assert_eq!(
        assembler
            .try_push(
                BatchItem::new(2, 15, "b"),
                clock.now().elapsed,
                Some(&budget)
            )
            .unwrap(),
        PushDecision::Buffered
    );
    assert_eq!(budget.usage().total_logical_bytes(), 30);

    // 3rd item of 25 bytes would exceed 50 -> FlushCurrentFirst(Bytes).
    assert_eq!(
        assembler
            .try_push(
                BatchItem::new(3, 25, "c"),
                clock.now().elapsed,
                Some(&budget)
            )
            .unwrap(),
        PushDecision::FlushCurrentFirst(FlushReason::Bytes)
    );

    // Flush batch and simulate partial failure: item 1 delivered, item 2 retryable.
    let batch = assembler.take_batch();
    assert_eq!(batch.len(), 2);
    let summary = assembler.apply_outcomes(
        batch,
        &[ItemOutcome::Delivered, ItemOutcome::Retryable],
        clock.now().elapsed,
    );
    assert_eq!(summary.delivered, 1);
    assert_eq!(summary.retained_retryable, 1);
    // Only item 2 (15 bytes) remains charged in the budget!
    assert_eq!(budget.usage().total_logical_bytes(), 15);
    assert_eq!(assembler.len(), 1);

    // Advance simulated clock past linger -> Deadline flush.
    assert_eq!(assembler.check_deadline(clock.now().elapsed), None);
    clock.advance(Duration::from_millis(200));
    assert_eq!(
        assembler.check_deadline(clock.now().elapsed),
        Some(FlushReason::Deadline)
    );

    // Barrier cannot overtake buffered item.
    assert_eq!(assembler.on_barrier(), Some(FlushReason::Barrier));
    let remaining = assembler.take_batch();
    assert_eq!(remaining[0].seq, 2);
    assembler.apply_outcomes(remaining, &[ItemOutcome::Delivered], clock.now().elapsed);
    assert_eq!(budget.usage().total_logical_bytes(), 0);
    assert_eq!(assembler.on_barrier(), None);
}

#[test]
fn legacy_batch_size_and_linger_preserved_when_byte_limit_unset() {
    let clock = TestClock::new(1_700_000_000_000);
    let mut legacy =
        ActionBatchAssembler::new(BatchConfig::legacy(2, Some(Duration::from_secs(1))));
    assert_eq!(
        legacy
            .try_push(BatchItem::new(1, 10_000, "x"), clock.now().elapsed, None)
            .unwrap(),
        PushDecision::Buffered
    );
    assert_eq!(
        legacy
            .try_push(BatchItem::new(2, 10_000, "y"), clock.now().elapsed, None)
            .unwrap(),
        PushDecision::FlushNow(FlushReason::Count)
    );
    let drained = legacy.take_batch();
    assert_eq!(drained.len(), 2);
    assert_eq!(drained[0].payload, "x");
    assert_eq!(drained[1].payload, "y");
}
