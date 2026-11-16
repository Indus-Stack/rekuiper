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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum LookupOrderMode {
    /// Emit results in exact input sequence order (default).
    #[default]
    Ordered,
    /// Explicit opt-in for unordered completion; requires an explicit downstream contract.
    Unordered,
}

/// Configuration for [`LookupRuntime`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LookupQueueConfig {
    pub max_queued_requests: usize,
    pub max_queued_bytes: u64,
    pub max_concurrent_lookups: usize,
    pub permit_wait_timeout: Duration,
    pub request_timeout: Duration,
    pub order_mode: LookupOrderMode,
    /// When `true`, callers wait for queue capacity until `permit_wait_timeout` or
    /// cancellation; when `false`, full queues fail immediately with `QueueFull`.
    pub backpressure_on_full: bool,
}

impl Default for LookupQueueConfig {
    fn default() -> Self {
        Self {
            max_queued_requests: 256,
            max_queued_bytes: 4 * 1024 * 1024,
            max_concurrent_lookups: 16,
            permit_wait_timeout: Duration::from_secs(2),
            request_timeout: Duration::from_secs(5),
            order_mode: LookupOrderMode::Ordered,
            backpressure_on_full: false,
        }
    }
}

impl LookupQueueConfig {
    /// Validates lookup runtime limits before constructing synchronization primitives.
    pub fn validate(&self) -> Result<(), String> {
        if self.max_concurrent_lookups == 0 || self.max_concurrent_lookups > Semaphore::MAX_PERMITS
        {
            return Err(format!(
                "LOOKUP_MAX_CONCURRENT ({}) must be between 1 and {}",
                self.max_concurrent_lookups,
                Semaphore::MAX_PERMITS
            ));
        }
        if self.max_queued_requests == 0 {
            return Err("LOOKUP_MAX_QUEUED_REQUESTS must be >= 1".to_owned());
        }
        if self.max_queued_bytes == 0 {
            return Err("LOOKUP_MAX_QUEUED_BYTES must be >= 1".to_owned());
        }
        Ok(())
    }

    /// Parses and validates per-table lookup tuning options, rejecting out-of-range
    /// values (including `LOOKUP_MAX_CONCURRENT > Semaphore::MAX_PERMITS`).
    pub fn parse_and_validate_table_options(
        opts: &std::collections::HashMap<String, String>,
    ) -> Result<Self, String> {
        let mut cfg = Self::default();
        if let Some(raw) = opts.get("LOOKUP_MAX_CONCURRENT") {
            let v = raw
                .parse::<usize>()
                .map_err(|e| format!("invalid LOOKUP_MAX_CONCURRENT '{raw}': {e}"))?;
            if v == 0 || v > Semaphore::MAX_PERMITS {
                return Err(format!(
                    "LOOKUP_MAX_CONCURRENT ({v}) must be between 1 and {}",
                    Semaphore::MAX_PERMITS
                ));
            }
            cfg.max_concurrent_lookups = v;
        }
        if let Some(raw) = opts.get("LOOKUP_MAX_QUEUED_REQUESTS") {
            let v = raw
                .parse::<usize>()
                .map_err(|e| format!("invalid LOOKUP_MAX_QUEUED_REQUESTS '{raw}': {e}"))?;
            if v == 0 {
                return Err("LOOKUP_MAX_QUEUED_REQUESTS must be >= 1".to_owned());
            }
            cfg.max_queued_requests = v;
        }
        if let Some(raw) = opts.get("LOOKUP_MAX_QUEUED_BYTES") {
            let v = raw
                .parse::<u64>()
                .map_err(|e| format!("invalid LOOKUP_MAX_QUEUED_BYTES '{raw}': {e}"))?;
            if v == 0 {
                return Err("LOOKUP_MAX_QUEUED_BYTES must be >= 1".to_owned());
            }
            cfg.max_queued_bytes = v;
        }
        if let Some(raw) = opts.get("LOOKUP_PERMIT_WAIT_MS") {
            let v = raw
                .parse::<u64>()
                .map_err(|e| format!("invalid LOOKUP_PERMIT_WAIT_MS '{raw}': {e}"))?;
            if v == 0 {
                return Err("LOOKUP_PERMIT_WAIT_MS must be >= 1".to_owned());
            }
            cfg.permit_wait_timeout = Duration::from_millis(v);
        }
        if let Some(raw) = opts.get("LOOKUP_TIMEOUT_MS") {
            let v = raw
                .parse::<u64>()
                .map_err(|e| format!("invalid LOOKUP_TIMEOUT_MS '{raw}': {e}"))?;
            if v == 0 {
                return Err("LOOKUP_TIMEOUT_MS must be >= 1".to_owned());
            }
            cfg.request_timeout = Duration::from_millis(v);
        }
        if let Some(raw) = opts.get("LOOKUP_BACKPRESSURE") {
            cfg.backpressure_on_full = parse_backpressure_value(raw)?;
        }
        cfg.validate()?;
        Ok(cfg)
    }

    /// Parses optional per-table lookup tuning keys (`LOOKUP_MAX_CONCURRENT`,
    /// `LOOKUP_MAX_QUEUED_REQUESTS`, `LOOKUP_MAX_QUEUED_BYTES`,
    /// `LOOKUP_PERMIT_WAIT_MS`, `LOOKUP_TIMEOUT_MS`, `LOOKUP_BACKPRESSURE`),
    /// clamping concurrency to `1..=Semaphore::MAX_PERMITS`.
    pub fn from_table_options(opts: &std::collections::HashMap<String, String>) -> Self {
        let mut cfg = Self::default();
        if let Some(v) = opts
            .get("LOOKUP_MAX_CONCURRENT")
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|&n| n > 0)
        {
            cfg.max_concurrent_lookups = v.min(Semaphore::MAX_PERMITS);
        }
        if let Some(v) = opts
            .get("LOOKUP_MAX_QUEUED_REQUESTS")
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|&n| n > 0)
        {
            cfg.max_queued_requests = v;
        }
        if let Some(v) = opts
            .get("LOOKUP_MAX_QUEUED_BYTES")
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|&n| n > 0)
        {
            cfg.max_queued_bytes = v;
        }
        if let Some(v) = opts
            .get("LOOKUP_PERMIT_WAIT_MS")
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|&n| n > 0)
        {
            cfg.permit_wait_timeout = Duration::from_millis(v);
        }
        if let Some(v) = opts
            .get("LOOKUP_TIMEOUT_MS")
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|&n| n > 0)
        {
            cfg.request_timeout = Duration::from_millis(v);
        }
        if let Some(raw) = opts.get("LOOKUP_BACKPRESSURE") {
            if let Ok(b) = parse_backpressure_value(raw) {
                cfg.backpressure_on_full = b;
            }
        }
        cfg
    }
}

fn parse_backpressure_value(raw: &str) -> Result<bool, String> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "block" | "wait" => Ok(true),
        "false" | "drop_new" | "fail_fast" => Ok(false),
        other => Err(format!(
            "invalid LOOKUP_BACKPRESSURE '{other}' (expected true/false, block/wait, or drop_new/fail_fast)"
        )),
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
        let max_permits = config
            .max_concurrent_lookups
            .clamp(1, Semaphore::MAX_PERMITS);
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
        let permit_deadline = clock
            .now()
            .elapsed
            .saturating_add(self.config.permit_wait_timeout);
        let charge = BudgetCharge::new(1, request_bytes.max(1), 0);

        let _reservation = if self.config.backpressure_on_full {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => {
                    self.stats.cancelled.fetch_add(1, Ordering::Relaxed);
                    return Err(LookupError::Cancelled);
                }
                sleep_res = clock.wait_until(permit_deadline, cancellation) => {
                    return match sleep_res {
                        Err(DeadlineCancelled) => {
                            self.stats.cancelled.fetch_add(1, Ordering::Relaxed);
                            Err(LookupError::Cancelled)
                        }
                        Ok(()) => {
                            self.stats.timed_out.fetch_add(1, Ordering::Relaxed);
                            Err(LookupError::Timeout { timeout: self.config.permit_wait_timeout })
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
            sleep_res = clock.wait_until(permit_deadline, cancellation) => {
                return match sleep_res {
                    Err(DeadlineCancelled) => {
                        self.stats.cancelled.fetch_add(1, Ordering::Relaxed);
                        Err(LookupError::Cancelled)
                    }
                    Ok(()) => {
                        self.stats.timed_out.fetch_add(1, Ordering::Relaxed);
                        Err(LookupError::Timeout { timeout: self.config.permit_wait_timeout })
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

        let exec_deadline = clock
            .now()
            .elapsed
            .saturating_add(self.config.request_timeout);
        let fut = op();
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                self.stats.cancelled.fetch_add(1, Ordering::Relaxed);
                Err(LookupError::Cancelled)
            }
            sleep_res = clock.wait_until(exec_deadline, cancellation) => match sleep_res {
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

type LookupRegistry =
    parking_lot::RwLock<std::collections::HashMap<(String, LookupQueueConfig), Arc<LookupRuntime>>>;

fn backend_registry() -> &'static LookupRegistry {
    static REGISTRY: std::sync::OnceLock<LookupRegistry> = std::sync::OnceLock::new();
    REGISTRY.get_or_init(|| parking_lot::RwLock::new(std::collections::HashMap::new()))
}

/// Returns an isolated [`LookupRuntime`] for `backend` and `config`.
pub fn lookup_runtime_for_backend(backend: &str, config: LookupQueueConfig) -> Arc<LookupRuntime> {
    let key = (backend.to_ascii_lowercase(), config.clone());
    if let Some(rt) = backend_registry().read().get(&key) {
        return Arc::clone(rt);
    }
    let mut map = backend_registry().write();
    Arc::clone(
        map.entry(key)
            .or_insert_with(|| Arc::new(LookupRuntime::new(config))),
    )
}

/// Aggregates lookup runtime counters per backend for observability/metrics export.
pub fn all_lookup_runtime_stats() -> Vec<(String, LookupStatsSnapshot)> {
    let mut by_backend: std::collections::BTreeMap<String, LookupStatsSnapshot> =
        std::collections::BTreeMap::new();
    for ((backend, _), rt) in backend_registry().read().iter() {
        let snap = rt.stats();
        let entry = by_backend.entry(backend.clone()).or_default();
        entry.completed += snap.completed;
        entry.rejected += snap.rejected;
        entry.timed_out += snap.timed_out;
        entry.cancelled += snap.cancelled;
        entry.failed += snap.failed;
    }
    by_backend.into_iter().collect()
}
