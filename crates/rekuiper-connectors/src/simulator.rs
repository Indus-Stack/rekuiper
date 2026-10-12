use rekuiper_core::model::StreamRecord;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;

fn default_true() -> bool {
    true
}

fn default_interval_ms() -> Value {
    Value::from(10)
}

/// Interval for [`SimulatorSource`]: integer milliseconds (e.g. `10`) or a
/// duration string (e.g. `"10ms"`, `"1s"`).
pub fn parse_interval_ms(interval: &Value) -> std::time::Duration {
    const DEFAULT: std::time::Duration = std::time::Duration::from_millis(10);
    match interval {
        Value::Number(n) => {
            if let Some(ms) = n.as_u64() {
                std::time::Duration::from_millis(ms)
            } else if let Some(ms) = n.as_i64() {
                if ms >= 0 {
                    std::time::Duration::from_millis(ms as u64)
                } else {
                    DEFAULT
                }
            } else if let Some(f) = n.as_f64() {
                if f.is_finite() && f >= 0.0 {
                    std::time::Duration::from_millis(f as u64)
                } else {
                    DEFAULT
                }
            } else {
                DEFAULT
            }
        }
        Value::String(s) => parse_duration_str(s).unwrap_or(DEFAULT),
        _ => DEFAULT,
    }
}

fn parse_duration_str(s: &str) -> Option<std::time::Duration> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let idx = s.find(|c: char| c.is_alphabetic()).unwrap_or(s.len());
    let (num_part, unit_part) = s.split_at(idx);
    if num_part.trim().is_empty() {
        return None;
    }
    let n: f64 = num_part.trim().parse().ok()?;
    if !n.is_finite() || n < 0.0 {
        return None;
    }
    let mult_ms: f64 = match unit_part.trim().to_ascii_lowercase().as_str() {
        "" | "ms" => 1.0,
        "ns" => 1.0 / 1_000_000.0,
        "us" | "µs" => 1.0 / 1_000.0,
        "s" => 1_000.0,
        "m" => 60_000.0,
        "h" => 3_600_000.0,
        _ => return None,
    };
    Some(std::time::Duration::from_millis((n * mult_ms) as u64))
}

/// Simulator source configuration (eKuiper simulator source format).
#[derive(Debug, Clone, Deserialize)]
pub struct SimulatorConfig {
    /// Sequence of records to emit, in order.
    #[serde(default)]
    pub data: Vec<HashMap<String, Value>>,
    /// Emit interval: integer milliseconds or duration string. Defaults to 10ms.
    #[serde(default = "default_interval_ms")]
    pub interval: Value,
    /// Repeat the sequence indefinitely until the receiver is dropped.
    #[serde(rename = "loop", default = "default_true")]
    pub loop_data: bool,
}

/// Simulator source: replays a fixed sequence of records at a fixed interval.
///
/// With `loop_data: true` the sequence repeats until the receiver is closed;
/// with `false` a single pass is emitted and the total count is returned.
pub struct SimulatorSource {
    pub config: SimulatorConfig,
}

impl SimulatorSource {
    pub fn new(config: SimulatorConfig) -> Self {
        Self { config }
    }

    pub async fn run(self, sender: tokio::sync::mpsc::Sender<StreamRecord>) -> usize {
        if self.config.data.is_empty() {
            return 0;
        }
        let interval = parse_interval_ms(&self.config.interval);
        let mut sent = 0usize;
        loop {
            for item in &self.config.data {
                if sender.send(StreamRecord::new(item.clone())).await.is_err() {
                    return sent;
                }
                sent += 1;
                tokio::time::sleep(interval).await;
            }
            if !self.config.loop_data {
                break;
            }
        }
        sent
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_interval_ms() {
        // Integer numbers
        assert_eq!(
            parse_interval_ms(&json!(50)),
            std::time::Duration::from_millis(50)
        );
        assert_eq!(
            parse_interval_ms(&json!(-1)),
            std::time::Duration::from_millis(10)
        ); // Default on negative
        assert_eq!(
            parse_interval_ms(&json!(25.0)),
            std::time::Duration::from_millis(25)
        );

        // Duration strings
        assert_eq!(
            parse_interval_ms(&json!("100ms")),
            std::time::Duration::from_millis(100)
        );
        assert_eq!(
            parse_interval_ms(&json!("2s")),
            std::time::Duration::from_millis(2000)
        );
        assert_eq!(
            parse_interval_ms(&json!("1.5s")),
            std::time::Duration::from_millis(1500)
        );
        assert_eq!(
            parse_interval_ms(&json!("1m")),
            std::time::Duration::from_millis(60000)
        );
        assert_eq!(
            parse_interval_ms(&json!("invalid")),
            std::time::Duration::from_millis(10)
        );
        assert_eq!(
            parse_interval_ms(&Value::Null),
            std::time::Duration::from_millis(10)
        );
    }

    #[tokio::test]
    async fn test_simulator_source_run_no_loop() {
        let mut rec1 = HashMap::new();
        rec1.insert("temp".to_string(), json!(20.5));
        let mut rec2 = HashMap::new();
        rec2.insert("temp".to_string(), json!(21.0));

        let cfg = SimulatorConfig {
            data: vec![rec1, rec2],
            interval: json!("1ms"),
            loop_data: false,
        };

        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        let source = SimulatorSource::new(cfg);
        let handle = tokio::spawn(async move { source.run(tx).await });

        let r1 = rx.recv().await.expect("receive rec1");
        assert_eq!(r1.data.get("temp").unwrap(), &json!(20.5));
        let r2 = rx.recv().await.expect("receive rec2");
        assert_eq!(r2.data.get("temp").unwrap(), &json!(21.0));

        let total_sent = handle.await.unwrap();
        assert_eq!(total_sent, 2);
    }

    #[tokio::test]
    async fn test_simulator_source_run_empty_data() {
        let cfg = SimulatorConfig {
            data: Vec::new(),
            interval: json!(10),
            loop_data: false,
        };
        let (tx, _rx) = tokio::sync::mpsc::channel(10);
        let count = SimulatorSource::new(cfg).run(tx).await;
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_simulator_source_run_receiver_drop() {
        let mut rec = HashMap::new();
        rec.insert("val".to_string(), json!(1));

        let cfg = SimulatorConfig {
            data: vec![rec],
            interval: json!("2ms"),
            loop_data: true,
        };

        let (tx, mut rx) = tokio::sync::mpsc::channel(2);
        let source = SimulatorSource::new(cfg);
        let handle = tokio::spawn(async move { source.run(tx).await });

        // Read 1 item then drop receiver
        let _ = rx.recv().await;
        drop(rx);

        // Runner should detect channel closure and exit cleanly
        let res = tokio::time::timeout(std::time::Duration::from_millis(500), handle).await;
        assert!(
            res.is_ok(),
            "Simulator run should terminate promptly when receiver is dropped"
        );
    }
}
