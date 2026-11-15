use indexmap::IndexMap;
use parking_lot::RwLock;
use rekuiper_core::contracts::clock::{DeadlineCancellation, TestClock};
use rekuiper_core::resources::BudgetUsage;
use rekuiper_core::TableManager;
use rekuiper_server::engine::lookup_runtime::{
    LookupError, LookupOrderMode, LookupQueueConfig, LookupRuntime,
};
use rekuiper_sql::{Parser, SelectStmt};
use serde_json::json;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

#[tokio::test]
async fn p02_t07_bounds_concurrency_and_queue_without_permit_leaks() {
    let runtime = LookupRuntime::new(LookupQueueConfig {
        max_queued_requests: 3,
        max_queued_bytes: 1024,
        max_concurrent_lookups: 2,
        permit_wait_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(5),
        order_mode: LookupOrderMode::Ordered,
        backpressure_on_full: false,
    });
    let clock = TestClock::new(0);
    let cancel = DeadlineCancellation::default();
    let gate = Arc::new(Notify::new());
    let active = Arc::new(AtomicUsize::new(0));
    let peak_active = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for i in 0..3 {
        let rt = runtime.clone();
        let gate = Arc::clone(&gate);
        let active = Arc::clone(&active);
        let peak_active = Arc::clone(&peak_active);
        let c = clock.clone();
        let cancel = cancel.clone();
        handles.push(tokio::spawn(async move {
            rt.execute_one(16, &c, &cancel, || async move {
                let now_active = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak_active.fetch_max(now_active, Ordering::SeqCst);
                gate.notified().await;
                active.fetch_sub(1, Ordering::SeqCst);
                if i == 1 {
                    Err("simulated backend failure".to_owned())
                } else {
                    Ok(i)
                }
            })
            .await
        }));
    }

    // Wait until all 3 requests have entered the queue (2 running, 1 waiting for permit).
    while runtime.budget_usage().total_items() < 3 {
        tokio::task::yield_now().await;
    }
    assert_eq!(runtime.in_flight_permits(), 2);
    assert!(peak_active.load(Ordering::SeqCst) <= 2);

    // 4th request must be rejected because max_queued_requests is 3.
    let overflow = runtime
        .execute_one(16, &clock, &cancel, || async { Ok::<_, String>(99) })
        .await
        .expect_err("queue full must reject 4th request");
    assert!(matches!(overflow, LookupError::QueueFull(_)));

    // Release queued/in-flight lookups; one returns Err to verify no permit leak on error.
    gate.notify_waiters();
    tokio::task::yield_now().await;
    gate.notify_waiters();

    let mut ok_count = 0;
    let mut err_count = 0;
    for h in handles {
        match h.await.unwrap() {
            Ok(_) => ok_count += 1,
            Err(LookupError::Backend(_)) => err_count += 1,
            Err(other) => panic!("unexpected error: {other:?}"),
        }
    }
    assert_eq!(ok_count, 2);
    assert_eq!(err_count, 1);

    // Zero permit or budget leaks after both Ok and Err outcomes.
    assert_eq!(runtime.available_permits(), 2);
    assert_eq!(runtime.in_flight_permits(), 0);
    assert_eq!(runtime.budget_usage(), BudgetUsage::default());
}

#[tokio::test]
async fn p02_t07_cancellation_and_timeout_release_permits_and_wake_immediately() {
    let runtime = LookupRuntime::new(LookupQueueConfig {
        max_queued_requests: 8,
        max_queued_bytes: 1024,
        max_concurrent_lookups: 1,
        permit_wait_timeout: Duration::from_millis(50),
        request_timeout: Duration::from_millis(50),
        order_mode: LookupOrderMode::Ordered,
        backpressure_on_full: true,
    });

    // 1. Timeout via TestClock
    let clock = TestClock::new(0);
    let cancel = DeadlineCancellation::default();
    let rt_timeout = runtime.clone();
    let clock_timeout = clock.clone();
    let cancel_timeout = cancel.clone();
    let h_timeout = tokio::spawn(async move {
        rt_timeout
            .execute_one(8, &clock_timeout, &cancel_timeout, || async {
                std::future::pending::<Result<(), String>>().await
            })
            .await
    });
    while runtime.in_flight_permits() < 1 {
        tokio::task::yield_now().await;
    }
    clock.advance(Duration::from_millis(50));
    let timeout_err = h_timeout.await.unwrap().expect_err("must time out");
    assert!(matches!(timeout_err, LookupError::Timeout { .. }));
    assert_eq!(runtime.available_permits(), 1);
    assert_eq!(runtime.budget_usage(), BudgetUsage::default());

    // 2. Cancellation wakes both in-flight and queued requests immediately
    let rt_cancel1 = runtime.clone();
    let clock_cancel1 = clock.clone();
    let cancel_cancel1 = cancel.clone();
    let h1 = tokio::spawn(async move {
        rt_cancel1
            .execute_one(8, &clock_cancel1, &cancel_cancel1, || async {
                std::future::pending::<Result<(), String>>().await
            })
            .await
    });
    while runtime.in_flight_permits() < 1 {
        tokio::task::yield_now().await;
    }

    let rt_cancel2 = runtime.clone();
    let clock_cancel2 = clock.clone();
    let cancel_cancel2 = cancel.clone();
    let h2 = tokio::spawn(async move {
        rt_cancel2
            .execute_one(8, &clock_cancel2, &cancel_cancel2, || async {
                Ok::<_, String>(())
            })
            .await
    });
    while runtime.budget_usage().total_items() < 2 {
        tokio::task::yield_now().await;
    }

    cancel.cancel();
    assert_eq!(h1.await.unwrap(), Err(LookupError::Cancelled));
    assert_eq!(h2.await.unwrap(), Err(LookupError::Cancelled));
    assert_eq!(runtime.available_permits(), 1);
    assert_eq!(runtime.budget_usage(), BudgetUsage::default());
}

#[tokio::test]
async fn p02_t07_ordered_by_default_and_explicit_unordered_mode() {
    let clock = TestClock::new(0);
    let cancel = DeadlineCancellation::default();
    let gate_first = Arc::new(Notify::new());
    let gate_second = Arc::new(Notify::new());

    // Ordered default preserves input sequence even when item 0 finishes after item 1.
    let ordered_rt = LookupRuntime::new(LookupQueueConfig {
        max_concurrent_lookups: 2,
        ..Default::default()
    });
    let g0 = Arc::clone(&gate_first);
    let g1 = Arc::clone(&gate_second);
    let batch_fut = ordered_rt.execute_batch(vec![(0usize, 8), (1usize, 8)], &clock, &cancel, {
        move |idx| {
            let g0 = Arc::clone(&g0);
            let g1 = Arc::clone(&g1);
            async move {
                if idx == 0 {
                    g0.notified().await;
                } else {
                    g1.notified().await;
                }
                Ok::<_, String>(idx * 10)
            }
        }
    });
    tokio::pin!(batch_fut);
    tokio::select! {
        _ = &mut batch_fut => panic!("batch should wait on gates"),
        _ = tokio::task::yield_now() => {}
    }
    // Complete item 1 first, then item 0.
    gate_second.notify_waiters();
    tokio::task::yield_now().await;
    gate_first.notify_waiters();
    let ordered_out = batch_fut.await;
    assert_eq!(ordered_out, vec![(0, Ok(0)), (1, Ok(10))]);

    // Unordered explicit mode emits item 1 as soon as it finishes.
    let unordered_rt = LookupRuntime::new(LookupQueueConfig {
        max_concurrent_lookups: 2,
        order_mode: LookupOrderMode::Unordered,
        ..Default::default()
    });
    let g0 = Arc::new(Notify::new());
    let g1 = Arc::new(Notify::new());
    let u_fut = unordered_rt.execute_batch(vec![(0usize, 8), (1usize, 8)], &clock, &cancel, {
        let g0 = Arc::clone(&g0);
        let g1 = Arc::clone(&g1);
        move |idx| {
            let g0 = Arc::clone(&g0);
            let g1 = Arc::clone(&g1);
            async move {
                if idx == 0 {
                    g0.notified().await;
                } else {
                    g1.notified().await;
                }
                Ok::<_, String>(idx * 10)
            }
        }
    });
    tokio::pin!(u_fut);
    tokio::select! {
        _ = &mut u_fut => panic!("unordered batch should wait on gates"),
        _ = tokio::task::yield_now() => {}
    }
    g1.notify_waiters();
    tokio::task::yield_now().await;
    g0.notify_waiters();
    let unordered_out = u_fut.await;
    assert_eq!(unordered_out, vec![(1, Ok(10)), (0, Ok(0))]);
}

#[tokio::test]
async fn p02_t07_preserves_first_match_lookup_join_semantics() {
    let tables = TableManager::new();
    tables.insert_table_row(
        "dim",
        HashMap::from([
            ("id".to_owned(), json!(1)),
            ("label".to_owned(), json!("first")),
        ]),
    );
    tables.insert_table_row(
        "dim",
        HashMap::from([
            ("id".to_owned(), json!(1)),
            ("label".to_owned(), json!("second")),
        ]),
    );

    let stmt: SelectStmt = Parser::new("SELECT * FROM s INNER JOIN dim ON s.id = dim.id")
        .parse_select()
        .unwrap();
    let record = IndexMap::from([("id".to_owned(), json!(1)), ("v".to_owned(), json!(42))]);
    let source_configs = Arc::new(RwLock::new(HashMap::new()));
    let rt = LookupRuntime::new(LookupQueueConfig::default());
    let clock = TestClock::new(0);
    let cancel = DeadlineCancellation::default();

    let joined = rekuiper_server::engine::joins::apply_lookup_joins_with_runtime(
        &tables,
        &source_configs,
        &stmt,
        &record,
        &rt,
        &clock,
        &cancel,
    )
    .await
    .expect("first matching row must join");
    assert_eq!(joined.get("label"), Some(&json!("first")));
}
