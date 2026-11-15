use rekuiper_connectors::scheduling::{
    run_nonoverlapping_poll_loop, MissedIntervalPolicy, PollIterationOutcome, PollSchedule,
    PollScheduleConfig,
};
use rekuiper_core::contracts::clock::{Clock, DeadlineCancellation, TestClock};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

#[test]
fn p02_t08_fake_clock_exact_instants_and_fleet_seed_distribution() {
    // Zero-jitter compatibility with offset.
    let mut sched = PollSchedule::new(
        PollScheduleConfig {
            interval: Duration::from_millis(100),
            offset: Duration::from_millis(25),
            jitter_max: Duration::ZERO,
            seed: 7,
            missed_policy: MissedIntervalPolicy::Skip,
        },
        Duration::ZERO,
    );
    assert_eq!(sched.next_due(), Duration::from_millis(25));
    let (next1, missed1) = sched.advance_after_poll(Duration::from_millis(30));
    assert_eq!(next1, Duration::from_millis(125));
    assert_eq!(missed1, 0);
    let (next2, missed2) = sched.advance_after_poll(Duration::from_millis(130));
    assert_eq!(next2, Duration::from_millis(225));
    assert_eq!(missed2, 0);

    // Different seeds distribute initial fleet starts within [offset, offset + jitter_max].
    let starts: Vec<Duration> = (1..=5)
        .map(|seed| {
            PollSchedule::new(
                PollScheduleConfig {
                    interval: Duration::from_secs(1),
                    offset: Duration::from_millis(10),
                    jitter_max: Duration::from_millis(200),
                    seed,
                    missed_policy: MissedIntervalPolicy::Skip,
                },
                Duration::ZERO,
            )
            .next_due()
        })
        .collect();
    for s in &starts {
        assert!(*s >= Duration::from_millis(10) && *s <= Duration::from_millis(210));
    }
    let unique_count = starts
        .iter()
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    assert!(unique_count > 1, "different seeds must distribute starts");
}

#[test]
fn p02_t08_missed_interval_policies_skip_delay_and_burst() {
    // Skip jumps to next grid instant past poll_finished.
    let mut skip = PollSchedule::new(
        PollScheduleConfig {
            interval: Duration::from_millis(100),
            missed_policy: MissedIntervalPolicy::Skip,
            ..Default::default()
        },
        Duration::ZERO,
    );
    let (due, missed) = skip.advance_after_poll(Duration::from_millis(250));
    assert_eq!(missed, 2);
    assert_eq!(due, Duration::from_millis(300));

    // Delay schedules `interval` after `poll_finished`.
    let mut delay = PollSchedule::new(
        PollScheduleConfig {
            interval: Duration::from_millis(100),
            missed_policy: MissedIntervalPolicy::Delay,
            ..Default::default()
        },
        Duration::ZERO,
    );
    let (due_d, missed_d) = delay.advance_after_poll(Duration::from_millis(250));
    assert_eq!(missed_d, 2);
    assert_eq!(due_d, Duration::from_millis(350));
}

#[tokio::test]
async fn p02_t08_slow_poll_never_overlaps_and_cancellation_wakes_immediately() {
    let clock = TestClock::new(0);
    let cancel = DeadlineCancellation::default();
    let in_flight = Arc::new(AtomicUsize::new(0));
    let max_in_flight = Arc::new(AtomicUsize::new(0));
    let poll_times = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let gate = Arc::new(Notify::new());

    let c = clock.clone();
    let cancel_task = cancel.clone();
    let in_flight_c = Arc::clone(&in_flight);
    let max_in_flight_c = Arc::clone(&max_in_flight);
    let poll_times_c = Arc::clone(&poll_times);
    let gate_c = Arc::clone(&gate);

    let handle = tokio::spawn(async move {
        let c_cb = c.clone();
        run_nonoverlapping_poll_loop(
            PollScheduleConfig {
                interval: Duration::from_millis(100),
                missed_policy: MissedIntervalPolicy::Skip,
                ..Default::default()
            },
            &c,
            &cancel_task,
            move |seq| {
                let in_flight = Arc::clone(&in_flight_c);
                let max_in_flight = Arc::clone(&max_in_flight_c);
                let poll_times = Arc::clone(&poll_times_c);
                let gate = Arc::clone(&gate_c);
                let c = c_cb.clone();
                async move {
                    let cur = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    max_in_flight.fetch_max(cur, Ordering::SeqCst);
                    poll_times.lock().push((seq, c.now().elapsed));
                    if seq == 0 {
                        gate.notified().await;
                    }
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                    if seq >= 1 {
                        PollIterationOutcome::Stop
                    } else {
                        PollIterationOutcome::Continue
                    }
                }
            },
        )
        .await
    });

    // Wait for poll 0 to start at t=0, advance clock past 250ms while poll 0 is still running.
    while in_flight.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    clock.advance(Duration::from_millis(250));
    tokio::task::yield_now().await;
    // Poll 1 must NOT have started yet because poll 0 is still in flight.
    assert_eq!(poll_times.lock().len(), 1);

    // Finish poll 0 at t=250ms; next due is t=300ms (2 missed intervals).
    gate.notify_waiters();
    tokio::task::yield_now().await;
    assert_eq!(poll_times.lock().len(), 1);
    clock.advance(Duration::from_millis(50));

    let report = handle.await.unwrap();
    assert_eq!(max_in_flight.load(Ordering::SeqCst), 1);
    assert_eq!(report.completed_polls, 2);
    assert_eq!(report.missed_intervals, 2);
    assert!(!report.cancelled);

    // Immediate cancellation while waiting for next tick.
    let cancel2 = DeadlineCancellation::default();
    let clock2 = TestClock::new(0);
    let cancel2_clone = cancel2.clone();
    let clock2_clone = clock2.clone();
    let h_cancel = tokio::spawn(async move {
        run_nonoverlapping_poll_loop(
            PollScheduleConfig {
                interval: Duration::from_secs(60),
                offset: Duration::from_secs(60),
                ..Default::default()
            },
            &clock2_clone,
            &cancel2_clone,
            |_| async { PollIterationOutcome::Continue },
        )
        .await
    });
    tokio::task::yield_now().await;
    cancel2.cancel();
    let report2 = h_cancel.await.unwrap();
    assert!(report2.cancelled);
    assert_eq!(report2.completed_polls, 0);
}
