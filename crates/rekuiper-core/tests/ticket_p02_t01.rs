use rekuiper_core::resources::{
    BudgetCharge, BudgetDimension, BudgetError, BudgetLimits, ResourceBudget,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::watch;

#[tokio::test]
async fn concurrent_reservations_respect_limits_and_release_cleanly() {
    let budget = ResourceBudget::new(BudgetLimits {
        max_items: Some(3),
        max_logical_bytes: Some(300),
        max_tasks: Some(3),
    });

    let active_tasks = Arc::new(AtomicU64::new(0));
    let max_observed = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();

    for _ in 0..12 {
        let b = budget.clone();
        let active = Arc::clone(&active_tasks);
        let max_obs = Arc::clone(&max_observed);
        handles.push(tokio::spawn(async move {
            let res = b
                .reserve(BudgetCharge::new(1, 100, 1), None)
                .await
                .expect("reserve succeeds");
            let commit = res.commit();
            let cur = active.fetch_add(1, Ordering::SeqCst) + 1;
            max_obs.fetch_max(cur, Ordering::SeqCst);
            assert!(cur <= 3, "concurrent active count exceeded limit: {cur}");
            tokio::task::yield_now().await;
            active.fetch_sub(1, Ordering::SeqCst);
            commit.release();
        }));
    }

    for h in handles {
        h.await.expect("task joins");
    }

    assert!(max_observed.load(Ordering::SeqCst) <= 3);
    let usage = budget.usage();
    assert_eq!(usage.total_items(), 0);
    assert_eq!(usage.total_logical_bytes(), 0);
    assert_eq!(usage.total_tasks(), 0);
}

#[tokio::test]
async fn cancellation_oversize_shared_bytes_and_double_release_are_safe() {
    let budget = ResourceBudget::new(BudgetLimits {
        max_items: Some(2),
        max_logical_bytes: Some(100),
        max_tasks: Some(2),
    });

    // Oversize item fails immediately with explicit error.
    let err = budget
        .try_reserve(BudgetCharge::new(1, 150, 1))
        .unwrap_err();
    assert_eq!(
        err,
        BudgetError::OversizeItem {
            dimension: BudgetDimension::LogicalBytes,
            requested: 150,
            limit: 100,
        }
    );

    // Shared data: logical=80, physical=20. Charged 80, observed physical 20.
    let res1 = budget
        .try_reserve(BudgetCharge::for_shared_bytes(1, 80, 20))
        .expect("reserve shared");
    let usage = budget.usage();
    assert_eq!(usage.reserved_logical_bytes, 80);
    assert_eq!(usage.observed_physical_bytes, 20);

    // Second reservation requiring 50 bytes blocks, then cancels cleanly.
    let (cancel_tx, mut cancel_rx) = watch::channel(false);
    let b2 = budget.clone();
    let waiter = tokio::spawn(async move {
        b2.reserve(BudgetCharge::new(1, 50, 0), Some(&mut cancel_rx))
            .await
    });
    tokio::task::yield_now().await;
    cancel_tx.send(true).expect("send cancel");
    assert_eq!(waiter.await.unwrap().unwrap_err(), BudgetError::Cancelled);

    // Commit then double-release in place + Drop cannot underflow.
    let mut commit1 = res1.commit();
    commit1.release_in_place();
    commit1.release_in_place();
    drop(commit1);

    let final_usage = budget.usage();
    assert_eq!(final_usage.total_items(), 0);
    assert_eq!(final_usage.total_logical_bytes(), 0);
    assert_eq!(final_usage.observed_physical_bytes, 0);
}
