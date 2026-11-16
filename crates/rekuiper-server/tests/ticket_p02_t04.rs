use rekuiper_core::contracts::clock::{Clock, DeadlineCancellation, DeadlineCancelled, TestClock};
use rekuiper_core::resources::budget::{BudgetLimits, ResourceBudget};
use rekuiper_server::engine::operators::{
    RateLimitConfig, RateLimitEnqueueOutcome, RateLimitNode, RateLimitOverflowPolicy,
    RateLimitStrategy,
};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

#[test]
fn rejects_invalid_rate_burst_and_capacity_values() {
    let clock: Arc<dyn Clock> = Arc::new(TestClock::new(1_000));

    let invalid_configs = [
        RateLimitConfig {
            rate: 0.0,
            burst: 1.0,
            strategy: RateLimitStrategy::Buffer,
            max_buffer: 4,
            overflow_policy: RateLimitOverflowPolicy::DropNewest,
            max_buffer_bytes: 1024,
        },
        RateLimitConfig {
            rate: -5.0,
            burst: 1.0,
            strategy: RateLimitStrategy::Drop,
            max_buffer: 1,
            overflow_policy: RateLimitOverflowPolicy::DropNewest,
            max_buffer_bytes: 1024,
        },
        RateLimitConfig {
            rate: f64::NAN,
            burst: 1.0,
            strategy: RateLimitStrategy::Drop,
            max_buffer: 1,
            overflow_policy: RateLimitOverflowPolicy::DropNewest,
            max_buffer_bytes: 1024,
        },
        RateLimitConfig {
            rate: 10.0,
            burst: 0.5,
            strategy: RateLimitStrategy::Buffer,
            max_buffer: 4,
            overflow_policy: RateLimitOverflowPolicy::DropNewest,
            max_buffer_bytes: 1024,
        },
        RateLimitConfig {
            rate: 10.0,
            burst: 2.0,
            strategy: RateLimitStrategy::Buffer,
            max_buffer: 0,
            overflow_policy: RateLimitOverflowPolicy::DropNewest,
            max_buffer_bytes: 1024,
        },
    ];

    for cfg in invalid_configs {
        assert!(
            RateLimitNode::try_new(cfg, Arc::clone(&clock)).is_err(),
            "expected invalid rate limit config to fail validation"
        );
    }

    let mut bad_props = HashMap::new();
    bad_props.insert("rate".to_string(), json!(10.0));
    bad_props.insert("strategy".to_string(), json!("buffer"));
    bad_props.insert("max_buffer".to_string(), json!(0));
    assert!(RateLimitNode::try_from_props(&bad_props).is_err());

    let mut bad_policy = HashMap::new();
    bad_policy.insert("overflow_policy".to_string(), json!("unknown_policy"));
    assert!(RateLimitNode::try_from_props(&bad_policy).is_err());
}

#[test]
fn bounded_fifo_overflow_policies_and_budget_accounting() {
    let clock: Arc<dyn Clock> = Arc::new(TestClock::new(1_000));

    // 1. DropNewest
    let node_newest = RateLimitNode::try_new(
        RateLimitConfig {
            rate: 1.0,
            burst: 1.0,
            strategy: RateLimitStrategy::Buffer,
            max_buffer: 2,
            overflow_policy: RateLimitOverflowPolicy::DropNewest,
            max_buffer_bytes: 1024,
        },
        Arc::clone(&clock),
    )
    .expect("valid config");
    let budget = node_newest.default_budget();
    let mut buf = node_newest.into_buffer::<u64>(budget);

    assert!(matches!(
        buf.enqueue(1, 64),
        RateLimitEnqueueOutcome::AdmitNow(1)
    ));
    assert!(matches!(
        buf.enqueue(2, 64),
        RateLimitEnqueueOutcome::Buffered { evicted: false }
    ));
    assert!(matches!(
        buf.enqueue(3, 64),
        RateLimitEnqueueOutcome::Buffered { evicted: false }
    ));
    assert!(matches!(
        buf.enqueue(4, 64),
        RateLimitEnqueueOutcome::Dropped
    ));
    assert_eq!(buf.len(), 2);
    assert_eq!(buf.dropped_count(), 1);
    assert_eq!(buf.budget().usage().reserved_items, 2);

    buf.clear();
    assert_eq!(buf.budget().usage().reserved_items, 0);

    // 2. DropOldest
    let node_oldest = RateLimitNode::try_new(
        RateLimitConfig {
            rate: 1.0,
            burst: 1.0,
            strategy: RateLimitStrategy::Buffer,
            max_buffer: 2,
            overflow_policy: RateLimitOverflowPolicy::DropOldest,
            max_buffer_bytes: 1024,
        },
        Arc::clone(&clock),
    )
    .expect("valid config");
    let budget_oldest = node_oldest.default_budget();
    let mut buf_oldest = node_oldest.into_buffer::<u64>(budget_oldest);

    assert!(matches!(
        buf_oldest.enqueue(10, 64),
        RateLimitEnqueueOutcome::AdmitNow(10)
    ));
    assert!(matches!(
        buf_oldest.enqueue(20, 64),
        RateLimitEnqueueOutcome::Buffered { evicted: false }
    ));
    assert!(matches!(
        buf_oldest.enqueue(30, 64),
        RateLimitEnqueueOutcome::Buffered { evicted: false }
    ));
    assert!(matches!(
        buf_oldest.enqueue(40, 64),
        RateLimitEnqueueOutcome::Buffered { evicted: true }
    ));
    assert_eq!(buf_oldest.dropped_count(), 1);

    // 3. Fail
    let node_fail = RateLimitNode::try_new(
        RateLimitConfig {
            rate: 1.0,
            burst: 1.0,
            strategy: RateLimitStrategy::Buffer,
            max_buffer: 1,
            overflow_policy: RateLimitOverflowPolicy::Fail,
            max_buffer_bytes: 128,
        },
        Arc::clone(&clock),
    )
    .expect("valid config");
    let budget_fail = ResourceBudget::new(BudgetLimits {
        max_items: Some(1),
        max_logical_bytes: Some(128),
        max_tasks: None,
    });
    let mut buf_fail = node_fail.into_buffer::<u64>(budget_fail);
    assert!(matches!(
        buf_fail.enqueue(1, 64),
        RateLimitEnqueueOutcome::AdmitNow(1)
    ));
    assert!(matches!(
        buf_fail.enqueue(2, 64),
        RateLimitEnqueueOutcome::Buffered { evicted: false }
    ));
    assert!(matches!(
        buf_fail.enqueue(3, 64),
        RateLimitEnqueueOutcome::OverflowError(_)
    ));
    assert_eq!(buf_fail.overflow_error_count(), 1);
}

#[tokio::test]
async fn test_clock_burst_refill_cancellation_and_ten_thousand_queued_inputs() {
    let test_clock = Arc::new(TestClock::new(10_000));
    let clock: Arc<dyn Clock> = test_clock.clone();

    // 1. Burst = 2, rate = 2 tokens/sec, max_buffer = 64, DropNewest
    let node = RateLimitNode::try_new(
        RateLimitConfig {
            rate: 2.0,
            burst: 2.0,
            strategy: RateLimitStrategy::Buffer,
            max_buffer: 64,
            overflow_policy: RateLimitOverflowPolicy::DropNewest,
            max_buffer_bytes: 64 * 1024,
        },
        Arc::clone(&clock),
    )
    .expect("valid config");
    let budget = node.default_budget();
    let mut buf = node.into_buffer::<u64>(budget);

    // Feed 10,000 inputs without spawning 10,000 sleeping tasks.
    let mut admitted = Vec::new();
    for i in 0..10_000u64 {
        match buf.enqueue(i, 64) {
            RateLimitEnqueueOutcome::AdmitNow(v) => admitted.push(v),
            RateLimitEnqueueOutcome::Buffered { .. } | RateLimitEnqueueOutcome::Dropped => {}
            RateLimitEnqueueOutcome::OverflowError(err) => {
                panic!("unexpected overflow error: {err}")
            }
            RateLimitEnqueueOutcome::BackpressureWait(_) => {
                panic!("unexpected backpressure under DropNewest")
            }
        }
    }

    // Burst=2 admitted immediately; 64 buffered in FIFO; remaining 9,934 dropped.
    assert_eq!(admitted, vec![0, 1]);
    assert_eq!(buf.len(), 64);
    assert_eq!(buf.dropped_count(), 10_000 - 2 - 64);
    assert_eq!(buf.budget().usage().reserved_items, 64);

    // Advance TestClock by 1 second -> 2 tokens refill -> strictly FIFO [2, 3] drain.
    let cancellation = DeadlineCancellation::default();
    let clock_for_advance = test_clock.clone();
    tokio::spawn(async move {
        tokio::task::yield_now().await;
        clock_for_advance.advance(Duration::from_millis(500));
    });
    buf.wait_next_token(&cancellation)
        .await
        .expect("token wait should succeed after TestClock advance");
    assert_eq!(buf.drain_ready(), vec![2]);

    test_clock.advance(Duration::from_secs(1));
    assert_eq!(buf.drain_ready(), vec![3, 4]);

    // Cancellation ends waiting work cleanly and clears budget reservations.
    cancellation.cancel();
    let err = buf
        .wait_next_token(&cancellation)
        .await
        .expect_err("cancelled wait_next_token must fail promptly");
    assert_eq!(err, DeadlineCancelled);
    buf.clear();
    assert_eq!(buf.budget().usage().reserved_items, 0);
}
