use anyhow::{bail, Result};
use rekuiper_core::contracts::clock::{Clock, DeadlineCancellation, DeadlineCancelled, RealClock};
use rekuiper_core::model::StreamRecord;
use rekuiper_core::resources::budget::{
    BudgetCharge, BudgetError, BudgetLimits, BudgetReservation, ResourceBudget,
};
use rekuiper_sql::{eval::Evaluator, parse_expression, Expr};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Native Stream Control: Switch (Multi-port ordered predicate routing)
#[derive(Debug, Clone)]
pub struct SwitchNode {
    pub cases: Vec<(Expr, usize)>,
    pub default_port: Option<usize>,
}

impl SwitchNode {
    pub fn from_props(props: &HashMap<String, Value>) -> Result<Self> {
        let mut cases = Vec::new();
        if let Some(cases_val) = props.get("cases").or_else(|| props.get("rules")) {
            if let Some(arr) = cases_val.as_array() {
                for (port_idx, item) in arr.iter().enumerate() {
                    let expr_str = if let Some(s) = item.as_str() {
                        s.trim()
                    } else if let Some(obj) = item.as_object() {
                        obj.get("expr")
                            .or_else(|| obj.get("condition"))
                            .and_then(|v| v.as_str())
                            .map(|s| s.trim())
                            .unwrap_or("")
                    } else {
                        ""
                    };
                    if !expr_str.is_empty() {
                        let parsed = parse_expression(expr_str).map_err(|e| {
                            anyhow::anyhow!("Invalid switch case expr '{}': {}", expr_str, e)
                        })?;
                        cases.push((parsed, port_idx));
                    }
                }
            }
        }
        let default_port = props
            .get("defaultPort")
            .or_else(|| props.get("default_port"))
            .and_then(|v| v.as_u64())
            .map(|n| n as usize);

        Ok(Self {
            cases,
            default_port,
        })
    }

    /// Evaluates input record against ordered predicates.
    /// Returns matching output port index (0..N).
    /// If no predicate matches and no default port is configured, returns None (dropped).
    pub fn route(&self, record: &StreamRecord) -> Option<usize> {
        for (expr, port) in &self.cases {
            if Evaluator::eval_bool(expr, record) {
                return Some(*port);
            }
        }
        self.default_port
    }
}

/// Strategy for RateLimitNode when limit is exceeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateLimitStrategy {
    Drop,
    Buffer,
}

/// Configured outcome when a bounded rate-limit buffer overflows or exhausts its budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateLimitOverflowPolicy {
    /// Drop the newly arriving record.
    DropNewest,
    /// Evict the oldest buffered record to make room for the newest record.
    DropOldest,
    /// Reject the newly arriving record with an explicit overflow error.
    Fail,
}

/// Validated configuration for [`RateLimitNode`].
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    pub rate: f64,
    pub burst: f64,
    pub strategy: RateLimitStrategy,
    pub max_buffer: usize,
    pub overflow_policy: RateLimitOverflowPolicy,
    pub max_buffer_bytes: u64,
}

impl RateLimitConfig {
    pub fn validate(&self) -> Result<()> {
        if !self.rate.is_finite() || self.rate <= 0.0 {
            bail!("rate_limit rate must be finite and > 0, got {}", self.rate);
        }
        if !self.burst.is_finite() || self.burst < 1.0 {
            bail!(
                "rate_limit burst must be finite and >= 1.0, got {}",
                self.burst
            );
        }
        if self.strategy == RateLimitStrategy::Buffer && self.max_buffer == 0 {
            bail!("rate_limit max_buffer must be >= 1 for buffer strategy");
        }
        if self.strategy == RateLimitStrategy::Buffer && self.max_buffer_bytes == 0 {
            bail!("rate_limit max_buffer_bytes must be >= 1 for buffer strategy");
        }
        Ok(())
    }
}

struct RateBucketState {
    tokens: f64,
    last_refill: Duration,
}

/// Native Stream Control: RateLimit (Token-bucket drop or bounded-FIFO buffer policy).
pub struct RateLimitNode {
    config: RateLimitConfig,
    clock: Arc<dyn Clock>,
    tokens: parking_lot::Mutex<RateBucketState>,
    pub dropped: Arc<AtomicU64>,
    pub overflow_errors: Arc<AtomicU64>,
}

/// Outcome of submitting an item to a [`RateLimitBuffer`].
#[derive(Debug)]
pub enum RateLimitEnqueueOutcome<T> {
    /// Token was available and FIFO was empty; emit immediately without buffering.
    AdmitNow(T),
    /// Buffered in the bounded FIFO (possibly after evicting the oldest item).
    Buffered { evicted: bool },
    /// Dropped under `Drop` strategy or `DropNewest` overflow policy.
    Dropped,
    /// Rejected under `Fail` overflow policy or budget exhaustion.
    OverflowError(String),
}

struct BufferedRateLimitEntry<T> {
    item: T,
    _reservation: BudgetReservation,
}

/// Bounded FIFO buffer + single cancellable token scheduler for [`RateLimitNode`].
pub struct RateLimitBuffer<T> {
    limiter: RateLimitNode,
    budget: ResourceBudget,
    queue: VecDeque<BufferedRateLimitEntry<T>>,
}

impl<T> RateLimitBuffer<T> {
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn budget(&self) -> &ResourceBudget {
        &self.budget
    }

    pub fn dropped_count(&self) -> u64 {
        self.limiter.dropped.load(Ordering::Relaxed)
    }

    pub fn overflow_error_count(&self) -> u64 {
        self.limiter.overflow_errors.load(Ordering::Relaxed)
    }

    /// Submit one item into the rate limiter, charging buffered items to `budget`.
    pub fn enqueue(&mut self, item: T, estimated_bytes: u64) -> RateLimitEnqueueOutcome<T> {
        if self.limiter.config.strategy == RateLimitStrategy::Drop {
            return if self.limiter.try_acquire() {
                RateLimitEnqueueOutcome::AdmitNow(item)
            } else {
                self.limiter.dropped.fetch_add(1, Ordering::Relaxed);
                RateLimitEnqueueOutcome::Dropped
            };
        }

        if self.queue.is_empty() && self.limiter.try_acquire() {
            return RateLimitEnqueueOutcome::AdmitNow(item);
        }

        let charge = BudgetCharge::new(1, estimated_bytes.max(1), 0);
        let mut evicted = false;

        if self.queue.len() >= self.limiter.config.max_buffer {
            match self.limiter.config.overflow_policy {
                RateLimitOverflowPolicy::DropNewest => {
                    self.limiter.dropped.fetch_add(1, Ordering::Relaxed);
                    return RateLimitEnqueueOutcome::Dropped;
                }
                RateLimitOverflowPolicy::Fail => {
                    self.limiter.overflow_errors.fetch_add(1, Ordering::Relaxed);
                    return RateLimitEnqueueOutcome::OverflowError(format!(
                        "rate_limit buffer capacity {} exceeded",
                        self.limiter.config.max_buffer
                    ));
                }
                RateLimitOverflowPolicy::DropOldest => {
                    if self.queue.pop_front().is_some() {
                        self.limiter.dropped.fetch_add(1, Ordering::Relaxed);
                        evicted = true;
                    }
                }
            }
        }

        let reservation = match self.budget.try_reserve(charge) {
            Ok(res) => res,
            Err(BudgetError::OversizeItem { .. }) => {
                self.limiter.overflow_errors.fetch_add(1, Ordering::Relaxed);
                return RateLimitEnqueueOutcome::OverflowError(
                    "rate_limit item exceeds rule memory budget".to_string(),
                );
            }
            Err(err) => {
                if self.limiter.config.overflow_policy == RateLimitOverflowPolicy::DropOldest {
                    while let Some(_evicted_entry) = self.queue.pop_front() {
                        self.limiter.dropped.fetch_add(1, Ordering::Relaxed);
                        evicted = true;
                        if let Ok(res) = self.budget.try_reserve(charge) {
                            self.queue.push_back(BufferedRateLimitEntry {
                                item,
                                _reservation: res,
                            });
                            return RateLimitEnqueueOutcome::Buffered { evicted };
                        }
                    }
                }
                return match self.limiter.config.overflow_policy {
                    RateLimitOverflowPolicy::DropNewest | RateLimitOverflowPolicy::DropOldest => {
                        self.limiter.dropped.fetch_add(1, Ordering::Relaxed);
                        RateLimitEnqueueOutcome::Dropped
                    }
                    RateLimitOverflowPolicy::Fail => {
                        self.limiter.overflow_errors.fetch_add(1, Ordering::Relaxed);
                        RateLimitEnqueueOutcome::OverflowError(format!(
                            "rate_limit budget exhausted: {err}"
                        ))
                    }
                };
            }
        };

        self.queue.push_back(BufferedRateLimitEntry {
            item,
            _reservation: reservation,
        });
        RateLimitEnqueueOutcome::Buffered { evicted }
    }

    /// Drain as many queued items as currently available tokens allow, in strict FIFO order.
    pub fn drain_ready(&mut self) -> Vec<T> {
        let mut ready = Vec::new();
        while !self.queue.is_empty() && self.limiter.try_acquire() {
            if let Some(entry) = self.queue.pop_front() {
                ready.push(entry.item);
            }
        }
        ready
    }

    /// Single cancellable wait until the next token becomes available.
    pub async fn wait_next_token(
        &self,
        cancellation: &DeadlineCancellation,
    ) -> std::result::Result<(), DeadlineCancelled> {
        self.limiter.wait_next_token(cancellation).await
    }

    /// Clear all buffered items, releasing all budget reservations immediately.
    pub fn clear(&mut self) {
        self.queue.clear();
    }
}

impl RateLimitNode {
    pub fn try_new(config: RateLimitConfig, clock: Arc<dyn Clock>) -> Result<Self> {
        config.validate()?;
        let initial_elapsed = clock.now().elapsed;
        let burst = config.burst;
        Ok(Self {
            config,
            clock,
            tokens: parking_lot::Mutex::new(RateBucketState {
                tokens: burst,
                last_refill: initial_elapsed,
            }),
            dropped: Arc::new(AtomicU64::new(0)),
            overflow_errors: Arc::new(AtomicU64::new(0)),
        })
    }

    pub fn new(rate: f64, burst: f64, strategy: RateLimitStrategy, max_buffer: usize) -> Self {
        let safe_max = max_buffer.max(1);
        let config = RateLimitConfig {
            rate,
            burst,
            strategy,
            max_buffer: safe_max,
            overflow_policy: RateLimitOverflowPolicy::DropNewest,
            max_buffer_bytes: (safe_max as u64).saturating_mul(64 * 1024),
        };
        Self::try_new(config, Arc::new(RealClock::default()))
            .expect("invalid RateLimitNode configuration")
    }

    pub fn try_from_props(props: &HashMap<String, Value>) -> Result<Self> {
        let rate = match props
            .get("rate")
            .or_else(|| props.get("limit"))
            .or_else(|| props.get("rate_limit"))
        {
            Some(v) => v
                .as_f64()
                .ok_or_else(|| anyhow::anyhow!("rate_limit rate must be a number"))?,
            None => 100.0,
        };

        let burst = match props.get("burst") {
            Some(v) => v
                .as_f64()
                .ok_or_else(|| anyhow::anyhow!("rate_limit burst must be a number"))?,
            None => rate.max(1.0),
        };

        let strategy = match props.get("strategy").and_then(|v| v.as_str()) {
            None => RateLimitStrategy::Drop,
            Some(s) if s.eq_ignore_ascii_case("drop") => RateLimitStrategy::Drop,
            Some(s) if s.eq_ignore_ascii_case("buffer") => RateLimitStrategy::Buffer,
            Some(other) => bail!("invalid rate_limit strategy: {other}"),
        };

        let max_buffer = match props.get("max_buffer").or_else(|| props.get("maxBuffer")) {
            None => 1024usize,
            Some(v) => {
                let raw = v.as_u64().ok_or_else(|| {
                    anyhow::anyhow!("rate_limit max_buffer must be a non-negative integer")
                })?;
                usize::try_from(raw)
                    .map_err(|_| anyhow::anyhow!("rate_limit max_buffer exceeds usize"))?
            }
        };

        let overflow_policy = match props
            .get("overflow_policy")
            .or_else(|| props.get("overflowPolicy"))
            .and_then(|v| v.as_str())
        {
            None => RateLimitOverflowPolicy::DropNewest,
            Some(s)
                if s.eq_ignore_ascii_case("drop_newest")
                    || s.eq_ignore_ascii_case("dropNewest") =>
            {
                RateLimitOverflowPolicy::DropNewest
            }
            Some(s)
                if s.eq_ignore_ascii_case("drop_oldest")
                    || s.eq_ignore_ascii_case("dropOldest") =>
            {
                RateLimitOverflowPolicy::DropOldest
            }
            Some(s)
                if s.eq_ignore_ascii_case("fail")
                    || s.eq_ignore_ascii_case("error")
                    || s.eq_ignore_ascii_case("reject") =>
            {
                RateLimitOverflowPolicy::Fail
            }
            Some(other) => bail!("invalid rate_limit overflow_policy: {other}"),
        };

        let max_buffer_bytes = match props
            .get("max_buffer_bytes")
            .or_else(|| props.get("maxBufferBytes"))
        {
            None => (max_buffer.max(1) as u64).saturating_mul(64 * 1024),
            Some(v) => v
                .as_u64()
                .ok_or_else(|| anyhow::anyhow!("rate_limit max_buffer_bytes must be an integer"))?,
        };

        Self::try_new(
            RateLimitConfig {
                rate,
                burst,
                strategy,
                max_buffer,
                overflow_policy,
                max_buffer_bytes,
            },
            Arc::new(RealClock::default()),
        )
    }

    pub fn from_props(props: &HashMap<String, Value>) -> Self {
        Self::try_from_props(props)
            .unwrap_or_else(|_| Self::new(100.0, 100.0, RateLimitStrategy::Drop, 1024))
    }

    pub fn config(&self) -> &RateLimitConfig {
        &self.config
    }

    pub fn into_buffer<T>(self, budget: ResourceBudget) -> RateLimitBuffer<T> {
        let cap = if self.config.strategy == RateLimitStrategy::Buffer {
            self.config.max_buffer
        } else {
            0
        };
        RateLimitBuffer {
            limiter: self,
            budget,
            queue: VecDeque::with_capacity(cap.min(1024)),
        }
    }

    pub fn default_budget(&self) -> ResourceBudget {
        ResourceBudget::new(BudgetLimits {
            max_items: Some(self.config.max_buffer.max(1) as u64),
            max_logical_bytes: Some(self.config.max_buffer_bytes.max(1)),
            max_tasks: None,
        })
    }

    fn refill_locked(&self, state: &mut RateBucketState) {
        let now_elapsed = self.clock.now().elapsed;
        let elapsed_secs = now_elapsed.saturating_sub(state.last_refill).as_secs_f64();
        state.last_refill = now_elapsed;
        state.tokens = (state.tokens + elapsed_secs * self.config.rate).min(self.config.burst);
    }

    pub fn try_acquire(&self) -> bool {
        let mut state = self.tokens.lock();
        self.refill_locked(&mut state);
        if state.tokens >= 1.0 {
            state.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    pub async fn wait_next_token(
        &self,
        cancellation: &DeadlineCancellation,
    ) -> std::result::Result<(), DeadlineCancelled> {
        let target = {
            let mut state = self.tokens.lock();
            self.refill_locked(&mut state);
            if state.tokens >= 1.0 {
                None
            } else {
                let needed = 1.0 - state.tokens;
                let wait_secs = (needed / self.config.rate).max(0.0);
                Some(
                    state
                        .last_refill
                        .saturating_add(Duration::from_secs_f64(wait_secs)),
                )
            }
        };
        match target {
            None => {
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => Err(DeadlineCancelled),
                    () = std::future::ready(()) => Ok(()),
                }
            }
            Some(deadline) => self.clock.wait_until(deadline, cancellation).await,
        }
    }

    /// Single-item acquisition helper for `Drop` strategy (zero sleeping tasks).
    pub async fn acquire(&self) -> bool {
        if self.try_acquire() {
            return true;
        }
        if self.config.strategy == RateLimitStrategy::Drop {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        let cancellation = DeadlineCancellation::default();
        loop {
            if self.wait_next_token(&cancellation).await.is_err() {
                return false;
            }
            if self.try_acquire() {
                return true;
            }
        }
    }
}

/// Native Stream Control: Watchdog (Inactivity timeout dead-man alert)
#[derive(Debug, Clone)]
pub struct WatchdogNode {
    pub timeout: Duration,
    pub alert_payload: Value,
    pub repeat: bool,
}

impl WatchdogNode {
    pub fn new(timeout: Duration, alert_payload: Value, repeat: bool) -> Self {
        Self {
            timeout,
            alert_payload,
            repeat,
        }
    }

    pub fn from_props(node_id: &str, props: &HashMap<String, Value>) -> Self {
        let timeout_ms = props
            .get("timeout")
            .or_else(|| props.get("timeout_ms"))
            .or_else(|| props.get("interval"))
            .and_then(|v| {
                v.as_u64()
                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            })
            .unwrap_or(5000);

        let alert_payload = props
            .get("alert_payload")
            .or_else(|| props.get("alertPayload"))
            .cloned()
            .unwrap_or_else(|| {
                serde_json::json!({
                    "event": "inactivity_timeout",
                    "node_id": node_id,
                    "timeout_ms": timeout_ms
                })
            });

        let repeat = props
            .get("repeat")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        Self::new(Duration::from_millis(timeout_ms), alert_payload, repeat)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_switch_routing_ordered_predicates() {
        let mut props = HashMap::new();
        props.insert(
            "cases".to_string(),
            serde_json::json!(["temp > 50", "temp > 20"]),
        );
        props.insert("default_port".to_string(), serde_json::json!(2));

        let switch = SwitchNode::from_props(&props).expect("parse switch");

        // Record with temp = 60 matches first case (port 0)
        let rec1 = StreamRecord::from_hashmap(
            [("temp".to_string(), serde_json::json!(60))]
                .into_iter()
                .collect(),
        );
        assert_eq!(switch.route(&rec1), Some(0));

        // Record with temp = 30 matches second case (port 1)
        let rec2 = StreamRecord::from_hashmap(
            [("temp".to_string(), serde_json::json!(30))]
                .into_iter()
                .collect(),
        );
        assert_eq!(switch.route(&rec2), Some(1));

        // Record with temp = 10 falls back to default port 2
        let rec3 = StreamRecord::from_hashmap(
            [("temp".to_string(), serde_json::json!(10))]
                .into_iter()
                .collect(),
        );
        assert_eq!(switch.route(&rec3), Some(2));
    }

    #[tokio::test]
    async fn test_rate_limit_token_bucket_drop() {
        // 2 tokens burst, 1 token/sec refill
        let rl = RateLimitNode::new(1.0, 2.0, RateLimitStrategy::Drop, 10);
        assert!(rl.acquire().await);
        assert!(rl.acquire().await);
        // Third token immediately depleted -> drops
        assert!(!rl.acquire().await);
        assert_eq!(rl.dropped.load(Ordering::Relaxed), 1);
    }
}
