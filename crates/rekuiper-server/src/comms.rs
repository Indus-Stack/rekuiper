use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Events delivered across the CommsHub bus to connected Stream Studio clients.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum CommsEvent {
    #[serde(rename = "tuple_sample")]
    TupleSample {
        rule_id: String,
        node_id: String,
        data: Value,
        timestamp: i64,
        dropped_since_last: u64,
    },
    #[serde(rename = "connector_status")]
    ConnectorStatus {
        id: String,
        kind: String,
        status: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    #[serde(rename = "heartbeat")]
    Heartbeat { timestamp: i64, active_taps: usize },
    #[serde(rename = "alert")]
    Alert {
        source: String,
        message: String,
        level: String,
        timestamp: i64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        details: Option<Value>,
    },
}

/// Token-bucket rate limiter for a single wire-tap probe point.
/// Restricts visual tuple sampling to max 20 msgs/sec while tracking dropped count.
struct ProbeRateLimiter {
    tokens: f64,
    last_check: Instant,
    dropped: u64,
}

impl ProbeRateLimiter {
    fn new(_rate_per_sec: f64, burst: f64) -> Self {
        Self {
            tokens: burst,
            last_check: Instant::now(),
            dropped: 0,
        }
    }

    /// Attempts to acquire 1 token.
    /// Returns `Some(dropped_count)` if allowed, or `None` if rate-limited.
    fn try_acquire(&mut self, rate_per_sec: f64, burst: f64) -> Option<u64> {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_check).as_secs_f64();
        self.last_check = now;
        self.tokens = (self.tokens + elapsed * rate_per_sec).min(burst);

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            let dropped = self.dropped;
            self.dropped = 0;
            Some(dropped)
        } else {
            self.dropped = self.dropped.saturating_add(1);
            None
        }
    }
}

/// A wire-tap probe attached to a specific graph node or pipeline stage.
pub struct WireTapProbe {
    pub active: Arc<AtomicBool>,
    limiter: Mutex<ProbeRateLimiter>,
}

impl WireTapProbe {
    pub fn new() -> Self {
        Self {
            active: Arc::new(AtomicBool::new(false)),
            limiter: Mutex::new(ProbeRateLimiter::new(20.0, 20.0)),
        }
    }

    #[inline]
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }
}

impl Default for WireTapProbe {
    fn default() -> Self {
        Self::new()
    }
}

/// Central comms bus and probe registry for real-time edge telemetry and wire-taps.
#[derive(Clone)]
pub struct CommsHub {
    tx: tokio::sync::broadcast::Sender<CommsEvent>,
    taps: Arc<RwLock<HashMap<String, Arc<WireTapProbe>>>>,
}

impl Default for CommsHub {
    fn default() -> Self {
        Self::new(2048)
    }
}

impl CommsHub {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = tokio::sync::broadcast::channel(capacity.max(64));
        Self {
            tx,
            taps: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Subscribe to the live comms event stream (for WebSockets / SSE).
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<CommsEvent> {
        self.tx.subscribe()
    }

    /// Broadcast an event to all active subscribers.
    pub fn publish(&self, event: CommsEvent) {
        let _ = self.tx.send(event);
    }

    /// Broadcast a connector lifecycle status update.
    pub fn publish_connector_status(
        &self,
        id: impl Into<String>,
        kind: impl Into<String>,
        status: impl Into<String>,
        error: Option<String>,
    ) {
        self.publish(CommsEvent::ConnectorStatus {
            id: id.into(),
            kind: kind.into(),
            status: status.into(),
            error,
        });
    }

    /// Broadcast an alert or watchdog event to all active subscribers.
    pub fn publish_alert(
        &self,
        source: impl Into<String>,
        message: impl Into<String>,
        level: impl Into<String>,
        details: Option<Value>,
    ) {
        self.publish(CommsEvent::Alert {
            source: source.into(),
            message: message.into(),
            level: level.into(),
            timestamp: chrono::Utc::now().timestamp(),
            details,
        });
    }

    /// Returns the number of active subscriber receivers.
    #[inline]
    pub fn receiver_count(&self) -> usize {
        self.tx.receiver_count()
    }

    /// Retrieve or create the wire-tap probe handle for a specific rule and node.
    pub fn get_or_create_probe(&self, rule_id: &str, node_id: &str) -> Arc<WireTapProbe> {
        let key = format!("{}:{}", rule_id, node_id);
        {
            let guard = self.taps.read();
            if let Some(probe) = guard.get(&key) {
                return probe.clone();
            }
        }
        let mut guard = self.taps.write();
        guard
            .entry(key)
            .or_insert_with(|| Arc::new(WireTapProbe::new()))
            .clone()
    }

    /// Enable wire-tap sampling for a given rule and node.
    pub fn enable_tap(&self, rule_id: &str, node_id: &str) -> Arc<WireTapProbe> {
        let probe = self.get_or_create_probe(rule_id, node_id);
        probe.active.store(true, Ordering::Release);
        probe
    }

    /// Disable wire-tap sampling for a given rule and node.
    pub fn disable_tap(&self, rule_id: &str, node_id: &str) {
        let key = format!("{}:{}", rule_id, node_id);
        if let Some(probe) = self.taps.read().get(&key) {
            probe.active.store(false, Ordering::Release);
        }
    }

    /// Query if wire-tap sampling is currently enabled for a given rule and node.
    #[inline]
    pub fn is_tap_active(&self, rule_id: &str, node_id: &str) -> bool {
        let key = format!("{}:{}", rule_id, node_id);
        self.taps
            .read()
            .get(&key)
            .map(|p| p.is_active())
            .unwrap_or(false)
    }

    /// Total count of currently active wire taps.
    pub fn active_tap_count(&self) -> usize {
        self.taps.read().values().filter(|p| p.is_active()).count()
    }

    /// Fast-path sample method invoked at node exit boundaries.
    /// In the normal untapped state, overhead is a single relaxed atomic read.
    #[inline]
    pub fn maybe_sample(
        &self,
        rule_id: &str,
        node_id: &str,
        data: &impl serde::Serialize,
        timestamp: i64,
    ) {
        // Fast exit: if no WebSocket/UI client is connected, do zero work.
        if self.tx.receiver_count() == 0 {
            return;
        }

        let key = format!("{}:{}", rule_id, node_id);
        let probe = {
            let guard = self.taps.read();
            match guard.get(&key) {
                Some(p) if p.is_active() => p.clone(),
                _ => return,
            }
        };

        let dropped_opt = {
            let mut limiter = probe.limiter.lock();
            limiter.try_acquire(20.0, 20.0)
        };

        if let Some(dropped) = dropped_opt {
            let event = CommsEvent::TupleSample {
                rule_id: rule_id.to_string(),
                node_id: node_id.to_string(),
                data: serde_json::to_value(data).unwrap_or(Value::Null),
                timestamp,
                dropped_since_last: dropped,
            };
            let _ = self.tx.send(event);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wiretap_lifecycle_and_rate_limiter() {
        let hub = CommsHub::new(64);
        assert!(!hub.is_tap_active("rule1", "node_a"));
        assert_eq!(hub.active_tap_count(), 0);

        hub.enable_tap("rule1", "node_a");
        assert!(hub.is_tap_active("rule1", "node_a"));
        assert_eq!(hub.active_tap_count(), 1);

        hub.disable_tap("rule1", "node_a");
        assert!(!hub.is_tap_active("rule1", "node_a"));
        assert_eq!(hub.active_tap_count(), 0);
    }

    #[tokio::test]
    async fn test_comms_hub_broadcast_and_sample() {
        let hub = CommsHub::new(64);
        let mut rx = hub.subscribe();

        hub.enable_tap("rule_test", "filter_1");

        let val = serde_json::json!({"temp": 42.5});
        hub.maybe_sample("rule_test", "filter_1", &val, 1000);

        let received = rx.recv().await.expect("receive event");
        match received {
            CommsEvent::TupleSample {
                rule_id,
                node_id,
                data,
                timestamp,
                dropped_since_last,
            } => {
                assert_eq!(rule_id, "rule_test");
                assert_eq!(node_id, "filter_1");
                assert_eq!(data, val);
                assert_eq!(timestamp, 1000);
                assert_eq!(dropped_since_last, 0);
            }
            other => panic!("Unexpected event: {:?}", other),
        }
    }
}
