//! Bounded async lookup runtime with request/byte queues, concurrency permits,
//! deadlines, cancellation, and ordered-by-default batch execution.

use futures::stream::{FuturesOrdered, FuturesUnordered, StreamExt};
use rekuiper_core::contracts::clock::{Clock, DeadlineCancellation, DeadlineCancelled};
use rekuiper_core::resources::{
    BudgetCharge, BudgetError, BudgetLimits, BudgetUsage, ResourceBudget,
};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

/// Output ordering contract for batched async lookups.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LookupOrderMode {
    /// Emit results in exact input sequence order (default).
    #[default]
    Ordered,
    /// Explicit opt-in for unordered completion; requires an explicit downstream contract.
    Unordered,
}

/// Configuration for [`LookupRuntime`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LookupQueueConfig {
    pub max_queued_requests: usize,
    pub max_queued_bytes: u64,
    pub max_concurrent_lookups: usize,
    pub request_timeout: Duration,
    pub order_mode: LookupOrderMode,
    /// When `true`, callers wait for queue capacity until `request_timeout` or
    /// cancellation; when `false`, full queues fail immediately with `QueueFull`.
    pub backpressure_on_full: bool,
}

impl Default for LookupQueueConfig {
    fn default() -> Self {
        Self {
            max_queued_requests: 256,
            max_queued_bytes: 4 * 1024 * 1024,
            max_concurrent_lookups: 16,
            request_timeout: Duration::from_secs(5),
            order_mode: LookupOrderMode::Ordered,
            backpressure_on_full: false,
        }
    }
}

/// Errors returned by [`LookupRuntime`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LookupError {
    QueueFull(BudgetError),
    Timeout { timeout: Duration },
    Cancelled,
    Backend(String),
}

impl std::fmt::Display for LookupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::QueueFull(err) => write!(f, "lookup queue capacity exceeded: {err}"),
            Self::Timeout { timeout } => write!(f, "lookup request timed out after {timeout:?}"),
            Self::Cancelled => f.write_str("lookup cancelled"),
            Self::Backend(msg) => write!(f, "lookup backend failed: {msg}"),
        }
    }
}

impl std::error::Error for LookupError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::QueueFull(err) => Some(err),
            _ => None,
        }
    }
}

impl From<BudgetError> for LookupError {
    fn from(err: BudgetError) -> Self {
        Self::QueueFull(err)
    }
}

/// Snapshot of runtime counters for observability and testing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LookupStatsSnapshot {
    pub completed: u64,
    pub timed_out: u64,
    pub cancelled: u64,
    pub rejected: u64,
    pub failed: u64,
}

#[derive(Debug, Default)]
struct LookupStats {
    completed: AtomicU64,
    timed_out: AtomicU64,
    cancelled: AtomicU64,
    rejected: AtomicU64,
    failed: AtomicU64,
}

impl LookupStats {
    fn snapshot(&self) -> LookupStatsSnapshot {
        LookupStatsSnapshot {
            completed: self.completed.load(Ordering::Relaxed),
            timed_out: self.timed_out.load(Ordering::Relaxed),
            cancelled: self.cancelled.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
        }
    }
}

/// Bounded, cancellable executor for asynchronous enrichment/table lookups.
#[derive(Debug, Clone)]
pub struct LookupRuntime {
    config: LookupQueueConfig,
    budget: ResourceBudget,
    permits: Arc<Semaphore>,
    stats: Arc<LookupStats>,
}

impl LookupRuntime {
    /// Creates a new [`LookupRuntime`] enforcing `config`.
    pub fn new(config: LookupQueueConfig) -> Self {
        let max_permits = config.max_concurrent_lookups.max(1);
        let budget = ResourceBudget::new(BudgetLimits {
            max_items: Some(u64::try_from(config.max_queued_requests.max(1)).unwrap_or(u64::MAX)),
            max_logical_bytes: Some(config.max_queued_bytes.max(1)),
            max_tasks: None,
        });
        Self {
            config,
            budget,
            permits: Arc::new(Semaphore::new(max_permits)),
            stats: Arc::new(LookupStats::default()),
        }
    }

    pub fn config(&self) -> &LookupQueueConfig {
        &self.config
    }

    pub fn budget_usage(&self) -> BudgetUsage {
        self.budget.usage()
    }

    pub fn available_permits(&self) -> usize {
        self.permits.available_permits()
    }

    pub fn in_flight_permits(&self) -> usize {
        self.config
            .max_concurrent_lookups
            .max(1)
            .saturating_sub(self.permits.available_permits())
    }

    pub fn stats(&self) -> LookupStatsSnapshot {
        self.stats.snapshot()
    }

    /// Executes one async lookup bounded by queue capacity, concurrency permits,
    /// `request_timeout` on `clock`, and `cancellation`.
    ///
    /// Both the queue reservation and concurrency permit are RAII-guarded, so
    /// errors, timeouts, and cancellations never leak capacity or permits.
    pub async fn execute_one<F, Fut, T>(
        &self,
        request_bytes: u64,
        clock: &dyn Clock,
        cancellation: &DeadlineCancellation,
        op: F,
    ) -> Result<T, LookupError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, String>>,
    {
        let deadline = clock
            .now()
            .elapsed
            .saturating_add(self.config.request_timeout);
        let charge = BudgetCharge::new(1, request_bytes.max(1), 0);

        let _reservation = if self.config.backpressure_on_full {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => {
                    self.stats.cancelled.fetch_add(1, Ordering::Relaxed);
                    return Err(LookupError::Cancelled);
                }
                sleep_res = clock.wait_until(deadline, cancellation) => {
                    return match sleep_res {
                        Err(DeadlineCancelled) => {
                            self.stats.cancelled.fetch_add(1, Ordering::Relaxed);
                            Err(LookupError::Cancelled)
                        }
                        Ok(()) => {
                            self.stats.timed_out.fetch_add(1, Ordering::Relaxed);
                            Err(LookupError::Timeout { timeout: self.config.request_timeout })
                        }
                    };
                }
                res = self.budget.reserve(charge, None) => match res {
                    Ok(guard) => guard,
                    Err(e) => {
                        self.stats.rejected.fetch_add(1, Ordering::Relaxed);
                        return Err(LookupError::QueueFull(e));
                    }
                }
            }
        } else {
            if self.is_cancelled(clock, cancellation).await {
                self.stats.cancelled.fetch_add(1, Ordering::Relaxed);
                return Err(LookupError::Cancelled);
            }
            match self.budget.try_reserve(charge) {
                Ok(guard) => guard,
                Err(e) => {
                    self.stats.rejected.fetch_add(1, Ordering::Relaxed);
                    return Err(LookupError::QueueFull(e));
                }
            }
        };

        let _permit = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                self.stats.cancelled.fetch_add(1, Ordering::Relaxed);
                return Err(LookupError::Cancelled);
            }
            sleep_res = clock.wait_until(deadline, cancellation) => {
                return match sleep_res {
                    Err(DeadlineCancelled) => {
                        self.stats.cancelled.fetch_add(1, Ordering::Relaxed);
                        Err(LookupError::Cancelled)
                    }
                    Ok(()) => {
                        self.stats.timed_out.fetch_add(1, Ordering::Relaxed);
                        Err(LookupError::Timeout { timeout: self.config.request_timeout })
                    }
                };
            }
            permit = Arc::clone(&self.permits).acquire_owned() => match permit {
                Ok(p) => p,
                Err(_) => {
                    self.stats.cancelled.fetch_add(1, Ordering::Relaxed);
                    return Err(LookupError::Cancelled);
                }
            }
        };

        let fut = op();
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                self.stats.cancelled.fetch_add(1, Ordering::Relaxed);
                Err(LookupError::Cancelled)
            }
            sleep_res = clock.wait_until(deadline, cancellation) => match sleep_res {
                Err(DeadlineCancelled) => {
                    self.stats.cancelled.fetch_add(1, Ordering::Relaxed);
                    Err(LookupError::Cancelled)
                }
                Ok(()) => {
                    self.stats.timed_out.fetch_add(1, Ordering::Relaxed);
                    Err(LookupError::Timeout { timeout: self.config.request_timeout })
                }
            },
            res = fut => match res {
                Ok(v) => {
                    self.stats.completed.fetch_add(1, Ordering::Relaxed);
                    Ok(v)
                }
                Err(e) => {
                    self.stats.failed.fetch_add(1, Ordering::Relaxed);
                    Err(LookupError::Backend(e))
                }
            }
        }
    }

    /// Executes a batch of lookup items concurrently up to `max_concurrent_lookups`,
    /// preserving input order by default (`LookupOrderMode::Ordered`) or emitting
    /// in completion order when `LookupOrderMode::Unordered` is explicitly configured.
    pub async fn execute_batch<I, F, Fut, T>(
        &self,
        items: Vec<(I, u64)>,
        clock: &dyn Clock,
        cancellation: &DeadlineCancellation,
        mut op: F,
    ) -> Vec<(usize, Result<T, LookupError>)>
    where
        F: FnMut(I) -> Fut,
        Fut: Future<Output = Result<T, String>>,
    {
        match self.config.order_mode {
            LookupOrderMode::Ordered => {
                let mut futs = FuturesOrdered::new();
                for (idx, (item, bytes)) in items.into_iter().enumerate() {
                    let fut = op(item);
                    futs.push_back(async move {
                        let res = self.execute_one(bytes, clock, cancellation, || fut).await;
                        (idx, res)
                    });
                }
                let mut out = Vec::with_capacity(futs.len());
                while let Some(pair) = futs.next().await {
                    out.push(pair);
                }
                out
            }
            LookupOrderMode::Unordered => {
                let mut futs = FuturesUnordered::new();
                for (idx, (item, bytes)) in items.into_iter().enumerate() {
                    let fut = op(item);
                    futs.push(async move {
                        let res = self.execute_one(bytes, clock, cancellation, || fut).await;
                        (idx, res)
                    });
                }
                let mut out = Vec::with_capacity(futs.len());
                while let Some(pair) = futs.next().await {
                    out.push(pair);
                }
                out
            }
        }
    }

    async fn is_cancelled(&self, clock: &dyn Clock, cancellation: &DeadlineCancellation) -> bool {
        let immediate = clock.now().elapsed;
        matches!(
            clock.wait_until(immediate, cancellation).await,
            Err(DeadlineCancelled)
        )
    }
}
