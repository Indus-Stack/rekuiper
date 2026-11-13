use rekuiper_core::contracts::{
    ActionId, DeadlineCancellation, EffectOutcome, ErrorCode, ErrorObject, ErrorStage,
    StructuredError, TestClock,
};
use rekuiper_core::resources::{BudgetLimits, ResourceBudget};
use rekuiper_server::engine::retry::{
    classify_adapter_result, execute_with_retry, AdapterResult, AmbiguousPolicy, RetryPolicy,
    SeededJitter,
};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn seeded_jitter_and_adapter_classification_are_deterministic_and_accurate() {
    let policy = RetryPolicy {
        initial_backoff: Duration::from_millis(100),
        max_backoff: Duration::from_millis(1000),
        multiplier: 2.0,
        jitter_ratio: 0.2,
        max_retry_after: Duration::from_millis(500),
        seed: 42,
        ..RetryPolicy::default()
    };

    let mut j1 = SeededJitter::new(policy.seed);
    let mut j2 = SeededJitter::new(policy.seed);
    let seq1: Vec<Duration> = (0..4)
        .map(|i| policy.next_delay(i, None, &mut j1))
        .collect();
    let seq2: Vec<Duration> = (0..4)
        .map(|i| policy.next_delay(i, None, &mut j2))
        .collect();
    assert_eq!(seq1, seq2);

    // Retry-After is bounded by max_retry_after.
    let mut j3 = SeededJitter::new(policy.seed);
    let hinted = policy.next_delay(0, Some(9_999), &mut j3);
    assert_eq!(hinted, Duration::from_millis(500));

    let action_id = ActionId::try_from("http_sink").expect("valid action id");

    // HTTP 2xx with body drain error stays Delivered.
    assert_eq!(
        classify_adapter_result(
            &action_id,
            AdapterResult::HttpResponse {
                status: 200,
                body_drain_error: Some("unexpected EOF while draining body".to_string()),
                retry_after_millis: None,
            }
        ),
        EffectOutcome::Delivered
    );

    // 400 is PermanentFailure; 503 is Retryable; after-send transport failure is Ambiguous.
    assert!(matches!(
        classify_adapter_result(
            &action_id,
            AdapterResult::HttpResponse {
                status: 400,
                body_drain_error: None,
                retry_after_millis: None,
            }
        ),
        EffectOutcome::PermanentFailure { .. }
    ));
    assert!(matches!(
        classify_adapter_result(
            &action_id,
            AdapterResult::HttpResponse {
                status: 503,
                body_drain_error: None,
                retry_after_millis: Some(250),
            }
        ),
        EffectOutcome::Retryable {
            delay_hint_millis: Some(250)
        }
    ));
    assert!(matches!(
        classify_adapter_result(
            &action_id,
            AdapterResult::TransportError {
                after_send: true,
                message: "connection reset".to_string(),
            }
        ),
        EffectOutcome::Ambiguous { .. }
    ));
}

#[tokio::test]
async fn retry_executor_respects_permanent_cancellation_and_task_budget() {
    let clock = Arc::new(TestClock::new(1_700_000_000_000));
    let cancel = DeadlineCancellation::default();
    let policy = RetryPolicy {
        initial_backoff: Duration::from_millis(100),
        max_backoff: Duration::from_millis(400),
        multiplier: 2.0,
        jitter_ratio: 0.0,
        total_deadline: Some(Duration::from_secs(5)),
        max_attempts: Some(4),
        ambiguous_policy: AmbiguousPolicy::SurfaceAmbiguous,
        ..RetryPolicy::default()
    };

    // Permanent failure is never retried.
    let perm_calls = Arc::new(AtomicU32::new(0));
    let perm_calls_clone = Arc::clone(&perm_calls);
    let action_id = ActionId::try_from("http_sink").expect("valid action id");
    let perm_report = execute_with_retry(&policy, clock.as_ref(), &cancel, None, |_| {
        let c = Arc::clone(&perm_calls_clone);
        let aid = action_id.clone();
        async move {
            c.fetch_add(1, Ordering::SeqCst);
            EffectOutcome::PermanentFailure {
                error: StructuredError {
                    code: ErrorCode::InvalidData,
                    stage: ErrorStage::Action,
                    object: ErrorObject::Action(aid),
                    retriable: false,
                    position: None,
                },
            }
        }
    })
    .await;
    assert_eq!(perm_report.attempts, 1);
    assert!(matches!(
        perm_report.outcome,
        EffectOutcome::PermanentFailure { .. }
    ));

    // Cancellation stops retry wait immediately.
    let cancel_retry = DeadlineCancellation::default();
    let cancel_clone = cancel_retry.clone();
    let clock_clone = Arc::clone(&clock);
    let policy_clone = policy.clone();
    let handle = tokio::spawn(async move {
        execute_with_retry(
            &policy_clone,
            clock_clone.as_ref(),
            &cancel_clone,
            None,
            |_| async {
                EffectOutcome::Retryable {
                    delay_hint_millis: None,
                }
            },
        )
        .await
    });
    tokio::task::yield_now().await;
    cancel_retry.cancel();
    let cancel_report = handle.await.unwrap();
    assert!(cancel_report.cancelled);
    assert_eq!(cancel_report.attempts, 1);

    // Task budget bounds concurrent retries during an extended outage.
    let budget = ResourceBudget::new(BudgetLimits {
        max_items: None,
        max_logical_bytes: None,
        max_tasks: Some(1),
    });
    let held = budget
        .try_reserve(rekuiper_core::resources::BudgetCharge::new(0, 0, 1))
        .expect("reserve single task slot")
        .commit();
    let bounded_report =
        execute_with_retry(&policy, clock.as_ref(), &cancel, Some(&budget), |_| async {
            EffectOutcome::Delivered
        })
        .await;
    assert!(bounded_report.budget_exhausted);
    assert_eq!(bounded_report.attempts, 0);
    held.release();
}
