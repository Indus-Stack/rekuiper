//! Centralized bounded retry, seeded jitter, and four-way effect outcome handling.

use rekuiper_core::contracts::{
    ActionId, Clock, DeadlineCancellation, EffectOutcome, ErrorCode, ErrorObject, ErrorStage,
    StructuredError,
};
use rekuiper_core::resources::{BudgetCharge, BudgetError, ResourceBudget};
use std::future::Future;
use std::time::Duration;

/// Policy for handling [`EffectOutcome::Ambiguous`] (for example, write-completed
/// but response lost or connection reset after request transmission).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AmbiguousPolicy {
    /// Reconcile or retry up to `max_attempts` when idempotency is supported.
    Reconcile { max_attempts: u32 },
    /// Surface ambiguous outcome immediately for operator / reconciliation handling
    /// without converting it into a permanent failure.
    SurfaceAmbiguous,
}

/// Bounded retry configuration with deterministic seeded jitter.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub multiplier: f64,
    pub jitter_ratio: f64,
    pub max_retry_after: Duration,
    pub total_deadline: Option<Duration>,
    pub max_attempts: Option<u32>,
    pub ambiguous_policy: AmbiguousPolicy,
    pub seed: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            multiplier: 2.0,
            jitter_ratio: 0.25,
            max_retry_after: Duration::from_secs(30),
            total_deadline: Some(Duration::from_secs(60)),
            max_attempts: Some(5),
            ambiguous_policy: AmbiguousPolicy::SurfaceAmbiguous,
            seed: 0x9e37_79b9_7f4a_7c15,
        }
    }
}

/// Deterministic seeded schedule helper (splitmix64).
#[derive(Debug, Clone)]
pub struct SeededJitter {
    state: u64,
}

impl SeededJitter {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Returns a deterministic float in `[0.0, 1.0)`.
    pub fn next_unit(&mut self) -> f64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^= z >> 31;
        ((z >> 11) as f64) / ((1u64 << 53) as f64)
    }
}

impl RetryPolicy {
    /// Computes the exact delay for `retry_index` (0-based) given an optional
    /// server `Retry-After` hint and a deterministic `SeededJitter`.
    pub fn next_delay(
        &self,
        retry_index: u32,
        retry_after_millis: Option<u64>,
        jitter: &mut SeededJitter,
    ) -> Duration {
        let base_secs =
            self.initial_backoff.as_secs_f64() * self.multiplier.powi(retry_index.min(30) as i32);
        let capped_secs = base_secs.min(self.max_backoff.as_secs_f64());
        let ratio = self.jitter_ratio.clamp(0.0, 1.0);
        let factor = if ratio == 0.0 {
            1.0
        } else {
            let unit = jitter.next_unit();
            // Spread in [1.0 - ratio, 1.0 + ratio]
            (1.0 - ratio) + (2.0 * ratio * unit)
        };
        let jittered = Duration::from_secs_f64((capped_secs * factor).max(0.0));
        let computed = jittered.min(self.max_backoff);

        if let Some(hint_ms) = retry_after_millis {
            let bounded_hint = Duration::from_millis(hint_ms).min(self.max_retry_after);
            computed.max(bounded_hint)
        } else {
            computed
        }
    }
}

/// Adapter-level delivery observation mapped to [`EffectOutcome`].
#[derive(Debug, Clone)]
pub enum AdapterResult {
    /// Successful delivery (including HTTP 2xx where draining the response body failed).
    HttpResponse {
        status: u16,
        body_drain_error: Option<String>,
        retry_after_millis: Option<u64>,
    },
    /// Transport error before or after request transmission.
    TransportError { after_send: bool, message: String },
    /// Explicit effect outcome from a contract-aware sink.
    Outcome(EffectOutcome),
}

/// Maps an adapter result to a four-way [`EffectOutcome`].
pub fn classify_adapter_result(action_id: &ActionId, result: AdapterResult) -> EffectOutcome {
    match result {
        AdapterResult::Outcome(outcome) => outcome,
        AdapterResult::HttpResponse {
            status,
            body_drain_error: _,
            retry_after_millis,
        } => match status {
            // 2xx stays Delivered even if draining the response body fails.
            200..=299 => EffectOutcome::Delivered,
            408 | 425 | 429 | 500..=599 => EffectOutcome::Retryable {
                delay_hint_millis: retry_after_millis,
            },
            _ => EffectOutcome::PermanentFailure {
                error: StructuredError {
                    code: ErrorCode::InvalidData,
                    stage: ErrorStage::Action,
                    object: ErrorObject::Action(action_id.clone()),
                    retriable: false,
                    position: None,
                },
            },
        },
        AdapterResult::TransportError {
            after_send,
            message: _,
        } => {
            if after_send {
                EffectOutcome::Ambiguous {
                    error: StructuredError {
                        code: ErrorCode::Unavailable,
                        stage: ErrorStage::Action,
                        object: ErrorObject::Action(action_id.clone()),
                        retriable: true,
                        position: None,
                    },
                }
            } else {
                EffectOutcome::Retryable {
                    delay_hint_millis: None,
                }
            }
        }
    }
}

/// Summary returned by [`execute_with_retry`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryExecutionReport {
    pub outcome: EffectOutcome,
    pub attempts: u32,
    pub delays_ms: Vec<u64>,
    pub cancelled: bool,
    pub budget_exhausted: bool,
}

/// Executes `attempt_fn` under bounded retries, seeded jitter, clock deadline,
/// cancellation, and optional task-budget concurrency bounds.
pub async fn execute_with_retry<F, Fut>(
    policy: &RetryPolicy,
    clock: &dyn Clock,
    cancellation: &DeadlineCancellation,
    task_budget: Option<&ResourceBudget>,
    mut attempt_fn: F,
) -> RetryExecutionReport
where
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = EffectOutcome>,
{
    let _permit = if let Some(budget) = task_budget {
        match budget.try_reserve(BudgetCharge::new(0, 0, 1)) {
            Ok(res) => Some(res.commit()),
            Err(BudgetError::Exhausted { .. } | BudgetError::OversizeItem { .. }) => {
                return RetryExecutionReport {
                    outcome: EffectOutcome::Retryable {
                        delay_hint_millis: Some(policy.initial_backoff.as_millis() as u64),
                    },
                    attempts: 0,
                    delays_ms: Vec::new(),
                    cancelled: false,
                    budget_exhausted: true,
                };
            }
            Err(BudgetError::Cancelled) => {
                return RetryExecutionReport {
                    outcome: EffectOutcome::Retryable {
                        delay_hint_millis: None,
                    },
                    attempts: 0,
                    delays_ms: Vec::new(),
                    cancelled: true,
                    budget_exhausted: false,
                };
            }
        }
    } else {
        None
    };

    let start = clock.now().elapsed;
    let deadline = policy.total_deadline.map(|d| start.saturating_add(d));
    let mut jitter = SeededJitter::new(policy.seed);
    let mut attempts: u32 = 0;
    let mut ambiguous_attempts: u32 = 0;
    let mut delays_ms = Vec::new();

    loop {
        attempts += 1;
        let outcome = attempt_fn(attempts).await;
        let hint_ms = match &outcome {
            EffectOutcome::Delivered | EffectOutcome::PermanentFailure { .. } => {
                return RetryExecutionReport {
                    outcome,
                    attempts,
                    delays_ms,
                    cancelled: false,
                    budget_exhausted: false,
                };
            }
            EffectOutcome::Ambiguous { .. } => {
                ambiguous_attempts += 1;
                match policy.ambiguous_policy {
                    AmbiguousPolicy::SurfaceAmbiguous => {
                        return RetryExecutionReport {
                            outcome,
                            attempts,
                            delays_ms,
                            cancelled: false,
                            budget_exhausted: false,
                        };
                    }
                    AmbiguousPolicy::Reconcile { max_attempts }
                        if ambiguous_attempts >= max_attempts =>
                    {
                        return RetryExecutionReport {
                            outcome,
                            attempts,
                            delays_ms,
                            cancelled: false,
                            budget_exhausted: false,
                        };
                    }
                    AmbiguousPolicy::Reconcile { .. } => None,
                }
            }
            EffectOutcome::Retryable { delay_hint_millis } => *delay_hint_millis,
        };

        if policy.max_attempts.is_some_and(|max| attempts >= max) {
            return RetryExecutionReport {
                outcome,
                attempts,
                delays_ms,
                cancelled: false,
                budget_exhausted: false,
            };
        }

        let delay = policy.next_delay(attempts - 1, hint_ms, &mut jitter);
        let now_elapsed = clock.now().elapsed;
        let target = now_elapsed.saturating_add(delay);
        if deadline.is_some_and(|dl| target > dl) {
            return RetryExecutionReport {
                outcome,
                attempts,
                delays_ms,
                cancelled: false,
                budget_exhausted: false,
            };
        }

        delays_ms.push(delay.as_millis() as u64);
        if clock.wait_until(target, cancellation).await.is_err() {
            return RetryExecutionReport {
                outcome,
                attempts,
                delays_ms,
                cancelled: true,
                budget_exhausted: false,
            };
        }
    }
}
