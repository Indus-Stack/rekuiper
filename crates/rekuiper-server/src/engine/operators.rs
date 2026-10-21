use anyhow::Result;
use rekuiper_core::model::StreamRecord;
use rekuiper_sql::{eval::Evaluator, parse_expression, Expr};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

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

/// Native Stream Control: RateLimit (Token-bucket drop or buffer policy)
pub struct RateLimitNode {
    rate: f64,
    burst: f64,
    strategy: RateLimitStrategy,
    _max_buffer: usize,
    tokens: parking_lot::Mutex<RateBucketState>,
    pub dropped: Arc<AtomicU64>,
}

struct RateBucketState {
    tokens: f64,
    last_check: Instant,
}

impl RateLimitNode {
    pub fn new(rate: f64, burst: f64, strategy: RateLimitStrategy, max_buffer: usize) -> Self {
        Self {
            rate: rate.max(0.001),
            burst: burst.max(1.0),
            strategy,
            _max_buffer: max_buffer.max(1),
            tokens: parking_lot::Mutex::new(RateBucketState {
                tokens: burst.max(1.0),
                last_check: Instant::now(),
            }),
            dropped: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn from_props(props: &HashMap<String, Value>) -> Self {
        let rate = props
            .get("rate")
            .or_else(|| props.get("limit"))
            .and_then(|v| v.as_f64().or_else(|| v.as_u64().map(|n| n as f64)))
            .unwrap_or(100.0);
        let burst = props
            .get("burst")
            .and_then(|v| v.as_f64().or_else(|| v.as_u64().map(|n| n as f64)))
            .unwrap_or(rate);
        let strat_str = props
            .get("strategy")
            .and_then(|v| v.as_str())
            .unwrap_or("drop");
        let strategy = if strat_str.eq_ignore_ascii_case("buffer") {
            RateLimitStrategy::Buffer
        } else {
            RateLimitStrategy::Drop
        };
        let max_buffer = props
            .get("max_buffer")
            .or_else(|| props.get("maxBuffer"))
            .and_then(|v| v.as_u64())
            .map(|n| n as usize)
            .unwrap_or(1024);

        Self::new(rate, burst, strategy, max_buffer)
    }

    /// Try to acquire token. If permitted, returns true.
    /// If not permitted:
    /// - If Drop: increments dropped counter and returns false.
    /// - If Buffer: sleeps until token is available.
    pub async fn acquire(&self) -> bool {
        loop {
            let wait_duration = {
                let mut state = self.tokens.lock();
                let now = Instant::now();
                let elapsed = now.duration_since(state.last_check).as_secs_f64();
                state.last_check = now;
                state.tokens = (state.tokens + elapsed * self.rate).min(self.burst);

                if state.tokens >= 1.0 {
                    state.tokens -= 1.0;
                    return true;
                }

                if self.strategy == RateLimitStrategy::Drop {
                    self.dropped.fetch_add(1, Ordering::Relaxed);
                    return false;
                }

                let needed = 1.0 - state.tokens;
                Duration::from_secs_f64(needed / self.rate)
            };

            tokio::time::sleep(wait_duration).await;
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
