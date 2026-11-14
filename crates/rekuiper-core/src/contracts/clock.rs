//! Wall time, monotonic time, and cancellable deadlines.

use super::Generation;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{
    future::Future,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::watch;

/// A wall/monotonic reading from one clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClockReading {
    pub wall_millis: i64,
    pub elapsed: Duration,
}

/// Persisted deadline expressed in signed Unix epoch milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LogicalDeadline(pub i64);

impl LogicalDeadline {
    /// Converts using a fresh clock reading, including after a process restart.
    /// Past deadlines are ready now and targets beyond the elapsed range saturate.
    pub fn to_monotonic(self, reading: ClockReading) -> Duration {
        let remaining_millis = if self.0 > reading.wall_millis {
            self.0.abs_diff(reading.wall_millis)
        } else {
            0
        };
        reading
            .elapsed
            .saturating_add(Duration::from_millis(remaining_millis))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("deadline wait cancelled")]
pub struct DeadlineCancelled;

/// Shared, one-shot cancellation for any number of deadline waiters.
#[derive(Debug, Clone)]
pub struct DeadlineCancellation {
    sender: watch::Sender<bool>,
    target_generation: Option<Generation>,
}

impl Default for DeadlineCancellation {
    fn default() -> Self {
        Self {
            sender: watch::channel(false).0,
            target_generation: None,
        }
    }
}

impl DeadlineCancellation {
    pub fn for_generation(generation: Generation) -> Self {
        Self {
            sender: watch::channel(false).0,
            target_generation: Some(generation),
        }
    }

    pub fn target_generation(&self) -> Option<Generation> {
        self.target_generation
    }

    pub fn cancel(&self) {
        self.sender.send_replace(true);
    }

    /// Waits until cancellation is requested. Once cancelled, this returns
    /// immediately for every caller, including callers that start waiting later.
    pub async fn cancelled(&self) {
        let mut receiver = self.sender.subscribe();
        let _ = receiver.wait_for(|cancelled| *cancelled).await;
    }
}

#[async_trait]
pub trait Clock: Send + Sync {
    fn now(&self) -> ClockReading;

    /// Waits for an elapsed-time target from this clock's monotonic origin.
    /// Cancellation wins if both cancellation and completion are observed ready.
    async fn wait_until(
        &self,
        target: Duration,
        cancellation: &DeadlineCancellation,
    ) -> Result<(), DeadlineCancelled>;
}

async fn cancellable(
    cancellation: &DeadlineCancellation,
    completion: impl Future<Output = ()>,
) -> Result<(), DeadlineCancelled> {
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(DeadlineCancelled),
        () = completion => Ok(()),
    }
}

fn wall_millis(value: i128) -> i64 {
    value.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

/// Production clock with a process-local monotonic origin.
#[derive(Debug, Clone)]
pub struct RealClock {
    origin: Instant,
}

impl Default for RealClock {
    fn default() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

#[async_trait]
impl Clock for RealClock {
    fn now(&self) -> ClockReading {
        let wall = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => duration.as_millis() as i128,
            Err(error) => -(error.duration().as_nanos().div_ceil(1_000_000) as i128),
        };
        ClockReading {
            wall_millis: wall_millis(wall),
            elapsed: self.origin.elapsed(),
        }
    }

    async fn wait_until(
        &self,
        target: Duration,
        cancellation: &DeadlineCancellation,
    ) -> Result<(), DeadlineCancelled> {
        // Bound each sleep so a distant target never overflows the timer's instant.
        const MAX_SLEEP: Duration = Duration::from_secs(24 * 60 * 60);
        cancellable(cancellation, async {
            loop {
                let remaining = target.saturating_sub(self.origin.elapsed());
                if remaining.is_zero() {
                    break;
                }
                tokio::time::sleep(remaining.min(MAX_SLEEP)).await;
            }
        })
        .await
    }
}

/// Deterministic clock that advances only when explicitly instructed.
#[derive(Debug, Clone)]
pub struct TestClock {
    start_wall_millis: i64,
    elapsed: watch::Sender<Duration>,
}

impl TestClock {
    pub fn new(start_wall_millis: i64) -> Self {
        Self {
            start_wall_millis,
            elapsed: watch::channel(Duration::ZERO).0,
        }
    }

    pub fn advance(&self, duration: Duration) {
        self.elapsed
            .send_modify(|elapsed| *elapsed = elapsed.saturating_add(duration));
    }
}

#[async_trait]
impl Clock for TestClock {
    fn now(&self) -> ClockReading {
        let elapsed = *self.elapsed.borrow();
        ClockReading {
            wall_millis: wall_millis(
                i128::from(self.start_wall_millis) + elapsed.as_millis() as i128,
            ),
            elapsed,
        }
    }

    async fn wait_until(
        &self,
        target: Duration,
        cancellation: &DeadlineCancellation,
    ) -> Result<(), DeadlineCancelled> {
        cancellable(cancellation, async {
            let mut receiver = self.elapsed.subscribe();
            let _ = receiver.wait_for(|elapsed| *elapsed >= target).await;
        })
        .await
    }
}
