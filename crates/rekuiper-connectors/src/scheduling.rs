//! Nonoverlapping poll/flush schedules with interval, offset, deterministic
//! seeded jitter, missed-interval accounting, and immediate cancellation.

use rekuiper_core::contracts::clock::{Clock, DeadlineCancellation, DeadlineCancelled};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// Policy when a slow poll finishes past one or more subsequent grid instants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MissedIntervalPolicy {
    /// Jump to the next grid instant strictly after `poll_finished` (default).
    #[default]
    Skip,
    /// Schedule `interval` after `poll_finished` (fixed-delay after slow poll).
    Delay,
    /// Catch up missed ticks sequentially without ever overlapping polls.
    Burst,
}

/// Configuration for [`PollSchedule`] and [`run_nonoverlapping_poll_loop`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PollScheduleConfig {
    pub interval: Duration,
    pub offset: Duration,
    pub jitter_max: Duration,
    pub seed: u64,
    pub missed_policy: MissedIntervalPolicy,
}

impl Default for PollScheduleConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(1),
            offset: Duration::ZERO,
            jitter_max: Duration::ZERO,
            seed: 0,
            missed_policy: MissedIntervalPolicy::Skip,
        }
    }
}

/// Control signal returned by each poll iteration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollIterationOutcome {
    Continue,
    Stop,
}

/// Summary returned when [`run_nonoverlapping_poll_loop`] exits.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PollScheduleReport {
    pub completed_polls: u64,
    pub missed_intervals: u64,
    pub cancelled: bool,
}

/// Deterministic splitmix64-derived jitter in `[0, jitter_max]`.
fn seeded_jitter(seed: u64, step: u64, jitter_max: Duration) -> Duration {
    if jitter_max.is_zero() {
        return Duration::ZERO;
    }
    let mut z = seed
        .wrapping_add(step.wrapping_mul(0x9E37_79B9_7F4A_7C15))
        .wrapping_add(0xD1B5_4A32_D192_ED03);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    let max_nanos = u64::try_from(jitter_max.as_nanos()).unwrap_or(u64::MAX);
    if max_nanos == 0 {
        return Duration::ZERO;
    }
    Duration::from_nanos(z % max_nanos.saturating_add(1))
}

/// Stateful nonoverlapping poll schedule driven by [`Clock`].
#[derive(Debug, Clone)]
pub struct PollSchedule {
    config: PollScheduleConfig,
    interval: Duration,
    step: u64,
    next_due: Duration,
    missed_intervals: u64,
    completed_polls: u64,
}

impl PollSchedule {
    /// Creates a new schedule anchored at `start_elapsed`.
    pub fn new(config: PollScheduleConfig, start_elapsed: Duration) -> Self {
        let interval = config.interval.max(Duration::from_millis(1));
        let jitter = seeded_jitter(config.seed, 0, config.jitter_max);
        let next_due = start_elapsed
            .saturating_add(config.offset)
            .saturating_add(jitter);
        Self {
            config,
            interval,
            step: 0,
            next_due,
            missed_intervals: 0,
            completed_polls: 0,
        }
    }

    pub fn next_due(&self) -> Duration {
        self.next_due
    }

    pub fn missed_intervals(&self) -> u64 {
        self.missed_intervals
    }

    pub fn completed_polls(&self) -> u64 {
        self.completed_polls
    }

    /// Advances the schedule after a poll that was due at `self.next_due` and
    /// finished at `poll_finished`. Returns `(next_due, newly_missed)`.
    pub fn advance_after_poll(&mut self, poll_finished: Duration) -> (Duration, u64) {
        self.completed_polls = self.completed_polls.saturating_add(1);
        self.step = self.step.saturating_add(1);
        let jitter = seeded_jitter(self.config.seed, self.step, self.config.jitter_max);
        let base_next = self.next_due.saturating_add(self.interval);

        let newly_missed = if poll_finished > base_next {
            let overrun_nanos = poll_finished.saturating_sub(base_next).as_nanos();
            let interval_nanos = self.interval.as_nanos().max(1);
            u64::try_from(overrun_nanos / interval_nanos + 1).unwrap_or(u64::MAX)
        } else {
            0
        };
        self.missed_intervals = self.missed_intervals.saturating_add(newly_missed);

        self.next_due = match self.config.missed_policy {
            MissedIntervalPolicy::Skip => {
                if newly_missed == 0 {
                    base_next.saturating_add(jitter)
                } else {
                    let skip_steps = u32::try_from(newly_missed).unwrap_or(u32::MAX);
                    base_next
                        .saturating_add(self.interval.saturating_mul(skip_steps))
                        .saturating_add(jitter)
                }
            }
            MissedIntervalPolicy::Delay => {
                if newly_missed == 0 {
                    base_next.saturating_add(jitter)
                } else {
                    poll_finished
                        .saturating_add(self.interval)
                        .saturating_add(jitter)
                }
            }
            MissedIntervalPolicy::Burst => base_next.saturating_add(jitter),
        };

        (self.next_due, newly_missed)
    }
}

/// Executes `poll_fn` sequentially on `schedule`, guaranteeing at most one
/// in-flight poll at any time and waking immediately on `cancellation`.
pub async fn run_nonoverlapping_poll_loop<F, Fut>(
    config: PollScheduleConfig,
    clock: &dyn Clock,
    cancellation: &DeadlineCancellation,
    mut poll_fn: F,
) -> PollScheduleReport
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = PollIterationOutcome>,
{
    let mut schedule = PollSchedule::new(config, clock.now().elapsed);
    let in_flight = AtomicBool::new(false);

    loop {
        if let Err(DeadlineCancelled) = clock.wait_until(schedule.next_due(), cancellation).await {
            return PollScheduleReport {
                completed_polls: schedule.completed_polls(),
                missed_intervals: schedule.missed_intervals(),
                cancelled: true,
            };
        }

        let was_busy = in_flight.swap(true, Ordering::SeqCst);
        debug_assert!(!was_busy, "poll loop must never overlap in-flight polls");
        let poll_seq = schedule.completed_polls();
        let outcome = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                in_flight.store(false, Ordering::SeqCst);
                return PollScheduleReport {
                    completed_polls: schedule.completed_polls(),
                    missed_intervals: schedule.missed_intervals(),
                    cancelled: true,
                };
            }
            res = poll_fn(poll_seq) => res,
        };
        in_flight.store(false, Ordering::SeqCst);

        let finished_at = clock.now().elapsed;
        schedule.advance_after_poll(finished_at);

        if outcome == PollIterationOutcome::Stop {
            return PollScheduleReport {
                completed_polls: schedule.completed_polls(),
                missed_intervals: schedule.missed_intervals(),
                cancelled: false,
            };
        }
    }
}
