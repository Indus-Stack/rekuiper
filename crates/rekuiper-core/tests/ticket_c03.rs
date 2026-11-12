use rekuiper_core::{
    CheckpointId, Clock, ClockReading, DeadlineCancellation, DeadlineCancelled, EdgeId,
    EventTimestamp, Generation, LogicalDeadline, OrderedEnvelope, RealClock, TestClock,
};
use std::{
    future::Future,
    pin::pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};
use tokio::sync::mpsc;

#[derive(Default)]
struct WakeFlag(AtomicBool);

impl Wake for WakeFlag {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[test]
fn advancing_test_clock_wakes_the_deadline_at_the_target() {
    let clock = TestClock::new(1_000);
    let cancellation = DeadlineCancellation::default();
    let mut deadline = pin!(clock.wait_until(Duration::from_millis(2), &cancellation));
    let flag = Arc::new(WakeFlag::default());
    let waker = Waker::from(flag.clone());
    let mut context = Context::from_waker(&waker);

    assert_eq!(deadline.as_mut().poll(&mut context), Poll::Pending);
    clock.clone().advance(Duration::from_micros(1_500));
    assert_eq!(
        clock.now(),
        ClockReading {
            wall_millis: 1_001,
            elapsed: Duration::from_micros(1_500),
        }
    );
    assert_eq!(deadline.as_mut().poll(&mut context), Poll::Pending);

    flag.0.store(false, Ordering::SeqCst);
    clock.advance(Duration::from_micros(500));
    assert!(flag.0.load(Ordering::SeqCst));
    assert_eq!(deadline.as_mut().poll(&mut context), Poll::Ready(Ok(())));
    assert_eq!(clock.now().wall_millis, 1_002);
}

#[test]
fn cancellation_wakes_waiters_and_wins_at_an_already_reached_deadline() {
    let clock = TestClock::new(0);
    let cancellation = DeadlineCancellation::default();
    let mut first = pin!(clock.wait_until(Duration::MAX, &cancellation));
    let mut second = pin!(clock.wait_until(Duration::MAX, &cancellation));
    let first_flag = Arc::new(WakeFlag::default());
    let second_flag = Arc::new(WakeFlag::default());
    let first_waker = Waker::from(first_flag.clone());
    let second_waker = Waker::from(second_flag.clone());
    let mut first_context = Context::from_waker(&first_waker);
    let mut second_context = Context::from_waker(&second_waker);

    assert_eq!(first.as_mut().poll(&mut first_context), Poll::Pending);
    assert_eq!(second.as_mut().poll(&mut second_context), Poll::Pending);
    cancellation.clone().cancel();
    assert!(first_flag.0.load(Ordering::SeqCst));
    assert!(second_flag.0.load(Ordering::SeqCst));
    assert_eq!(
        first.as_mut().poll(&mut first_context),
        Poll::Ready(Err(DeadlineCancelled))
    );
    assert_eq!(
        second.as_mut().poll(&mut second_context),
        Poll::Ready(Err(DeadlineCancelled))
    );
    assert_eq!(clock.now().elapsed, Duration::ZERO);

    let mut cancelled = pin!(clock.wait_until(Duration::ZERO, &cancellation));
    assert_eq!(
        cancelled.as_mut().poll(&mut first_context),
        Poll::Ready(Err(DeadlineCancelled))
    );
}

#[test]
fn downstream_clock_can_wait_for_shared_cancellation() {
    struct DownstreamClock(TestClock);

    #[async_trait::async_trait]
    impl Clock for DownstreamClock {
        fn now(&self) -> ClockReading {
            self.0.now()
        }

        async fn wait_until(
            &self,
            target: Duration,
            cancellation: &DeadlineCancellation,
        ) -> Result<(), DeadlineCancelled> {
            let completion = DeadlineCancellation::default();
            tokio::select! {
                biased;
                () = cancellation.cancelled() => Err(DeadlineCancelled),
                result = self.0.wait_until(target, &completion) => result,
            }
        }
    }

    let clock = DownstreamClock(TestClock::new(0));
    let cancellation = DeadlineCancellation::default();
    let target = clock.now().elapsed + Duration::from_millis(1);
    let mut wait = pin!(clock.wait_until(target, &cancellation));
    let flag = Arc::new(WakeFlag::default());
    let waker = Waker::from(flag.clone());
    let mut context = Context::from_waker(&waker);
    assert_eq!(wait.as_mut().poll(&mut context), Poll::Pending);

    cancellation.cancel();
    assert!(flag.0.load(Ordering::SeqCst));
    assert_eq!(
        wait.as_mut().poll(&mut context),
        Poll::Ready(Err(DeadlineCancelled))
    );

    let mut after_cancellation = pin!(clock.wait_until(target, &cancellation));
    assert_eq!(
        after_cancellation.as_mut().poll(&mut context),
        Poll::Ready(Err(DeadlineCancelled))
    );
}

#[tokio::test]
async fn real_clock_accepts_immediate_and_extreme_cancellable_targets() {
    let clock: &dyn Clock = &RealClock::default();
    let cancellation = DeadlineCancellation::default();
    assert_eq!(
        clock.wait_until(clock.now().elapsed, &cancellation).await,
        Ok(())
    );

    let mut far_future = pin!(clock.wait_until(Duration::MAX, &cancellation));
    let mut context = Context::from_waker(Waker::noop());
    assert_eq!(far_future.as_mut().poll(&mut context), Poll::Pending);
    cancellation.cancel();
    assert_eq!(
        far_future.as_mut().poll(&mut context),
        Poll::Ready(Err(DeadlineCancelled))
    );
}

#[test]
fn logical_deadline_recomputes_from_wall_time_after_restart() {
    let before_restart = TestClock::new(1_000);
    before_restart.advance(Duration::from_millis(20));
    let target = LogicalDeadline(1_100);
    assert_eq!(
        target.to_monotonic(before_restart.now()),
        Duration::from_millis(100)
    );

    let saved = serde_json::to_string(&target).unwrap();
    assert_eq!(saved, "1100");
    let restored: LogicalDeadline = serde_json::from_str(&saved).unwrap();
    let after_restart = TestClock::new(1_040);
    assert_eq!(
        restored.to_monotonic(after_restart.now()),
        Duration::from_millis(60)
    );
    let expired = TestClock::new(1_120);
    expired.advance(Duration::from_millis(5));
    assert_eq!(
        restored.to_monotonic(expired.now()),
        Duration::from_millis(5)
    );
}

#[test]
fn signed_timestamp_extremes_convert_without_overflow() {
    let oldest = TestClock::new(i64::MIN);
    assert_eq!(
        LogicalDeadline(i64::MAX).to_monotonic(oldest.now()),
        Duration::from_millis(u64::MAX)
    );
    let newest = TestClock::new(i64::MAX);
    assert_eq!(
        LogicalDeadline(i64::MIN).to_monotonic(newest.now()),
        Duration::ZERO
    );

    let reading = ClockReading {
        wall_millis: i64::MIN,
        elapsed: Duration::MAX - Duration::from_nanos(1),
    };
    assert_eq!(
        LogicalDeadline(i64::MAX).to_monotonic(reading),
        Duration::MAX
    );
    newest.advance(Duration::MAX);
    newest.advance(Duration::from_nanos(1));
    assert_eq!(newest.now().elapsed, Duration::MAX);
    assert_eq!(newest.now().wall_millis, i64::MAX);
    assert_eq!(
        serde_json::from_str::<LogicalDeadline>(&i64::MIN.to_string()).unwrap(),
        LogicalDeadline(i64::MIN)
    );
}

#[tokio::test]
async fn controls_follow_preceding_data_in_one_fifo() {
    let edge = EdgeId(7);
    let sequence = [
        OrderedEnvelope::Data("first"),
        OrderedEnvelope::Barrier {
            checkpoint: CheckpointId(1),
            generation: Generation(2),
        },
        OrderedEnvelope::Data("second"),
        OrderedEnvelope::Watermark {
            edge,
            timestamp: EventTimestamp::Millis(i64::MIN),
        },
        OrderedEnvelope::Watermark {
            edge,
            timestamp: EventTimestamp::OtlpNanos(u64::MAX),
        },
        OrderedEnvelope::EndOfInput { edge },
    ];
    let (sender, mut receiver) = mpsc::channel(sequence.len());
    for item in &sequence {
        sender.send(item.clone()).await.unwrap();
    }
    drop(sender);
    for expected in sequence {
        assert_eq!(receiver.recv().await, Some(expected));
    }
    assert_eq!(receiver.recv().await, None);
}
