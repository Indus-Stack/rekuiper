use anyhow::{Context, Result};
use bytes::Bytes;
use indexmap::IndexMap;
use rekuiper_core::model::StreamRecord;
use rekuiper_core::StreamSender;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::Mutex;
use zeromq::{PubSocket, Socket, SocketRecv, SocketSend, SubSocket, ZmqMessage};

pub const DEFAULT_ZMQ_SERVER: &str = "tcp://127.0.0.1:5563";

/// ZeroMQ Sink connector configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ZmqSinkConfig {
    /// ZeroMQ endpoint address, e.g. `"tcp://127.0.0.1:5563"`.
    #[serde(default = "default_zmq_endpoint")]
    pub server: String,
    /// Alias for server endpoint URL.
    #[serde(default)]
    pub url: Option<String>,
    /// Optional topic prefix to prepend as the first frame.
    #[serde(default)]
    pub topic: Option<String>,
    /// Socket mode: `"connect"` (default) or `"bind"`.
    #[serde(default = "default_zmq_mode")]
    pub mode: String,
}

impl Default for ZmqSinkConfig {
    fn default() -> Self {
        Self {
            server: default_zmq_endpoint(),
            url: None,
            topic: None,
            mode: default_zmq_mode(),
        }
    }
}

impl ZmqSinkConfig {
    pub fn endpoint(&self) -> &str {
        if let Some(ref u) = self.url {
            if !u.is_empty() {
                return u.as_str();
            }
        }
        if !self.server.is_empty() {
            &self.server
        } else {
            DEFAULT_ZMQ_SERVER
        }
    }

    pub fn is_bind(&self) -> bool {
        self.mode.eq_ignore_ascii_case("bind")
    }
}

fn default_zmq_endpoint() -> String {
    DEFAULT_ZMQ_SERVER.to_string()
}

fn default_zmq_mode() -> String {
    "connect".to_string()
}

/// ZeroMQ publisher sink.
pub struct ZmqSink {
    pub config: ZmqSinkConfig,
    socket: Arc<Mutex<Option<PubSocket>>>,
}

impl ZmqSink {
    pub fn new(config: ZmqSinkConfig) -> Self {
        Self {
            config,
            socket: Arc::new(Mutex::new(None)),
        }
    }

    /// Publishes a binary payload with an optional topic frame.
    pub async fn send_raw(&self, topic_override: Option<&str>, payload: &[u8]) -> Result<()> {
        let mut guard = self.socket.lock().await;
        if guard.is_none() {
            let mut sock = PubSocket::new();
            let endpoint = self.config.endpoint();
            let res = if self.config.is_bind() {
                sock.bind(endpoint).await.map(|_| ())
            } else {
                sock.connect(endpoint).await
            };
            res.with_context(|| format!("ZMQ sink failed to initialize on {}", endpoint))?;
            *guard = Some(sock);
        }

        let sock = guard.as_mut().unwrap();

        let topic = topic_override.or(self.config.topic.as_deref());
        let msg = match topic {
            Some(t) if !t.is_empty() => {
                let mut m = ZmqMessage::from(Bytes::copy_from_slice(t.as_bytes()));
                m.push_back(Bytes::copy_from_slice(payload));
                m
            }
            _ => ZmqMessage::from(Bytes::copy_from_slice(payload)),
        };

        if let Err(e) = sock.send(msg).await {
            // Drop socket on transmission failure so subsequent attempt reconnects
            *guard = None;
            return Err(anyhow::anyhow!("ZMQ sink transmission error: {}", e));
        }

        Ok(())
    }
}

/// ZeroMQ Source connector configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ZmqSourceConfig {
    /// ZeroMQ endpoint address, e.g. `"tcp://127.0.0.1:5563"`.
    #[serde(default = "default_zmq_endpoint")]
    pub server: String,
    /// Alias for server endpoint URL.
    #[serde(default)]
    pub url: Option<String>,
    /// ZeroMQ topic subscription prefix (default is empty string, subscribing to all).
    #[serde(default)]
    pub topic: Option<String>,
    /// Socket mode: `"connect"` (default) or `"bind"`.
    #[serde(default = "default_zmq_mode")]
    pub mode: String,
}

impl Default for ZmqSourceConfig {
    fn default() -> Self {
        Self {
            server: default_zmq_endpoint(),
            url: None,
            topic: None,
            mode: default_zmq_mode(),
        }
    }
}

impl ZmqSourceConfig {
    pub fn endpoint(&self) -> &str {
        if let Some(ref u) = self.url {
            if !u.is_empty() {
                return u.as_str();
            }
        }
        if !self.server.is_empty() {
            &self.server
        } else {
            DEFAULT_ZMQ_SERVER
        }
    }

    pub fn is_bind(&self) -> bool {
        self.mode.eq_ignore_ascii_case("bind")
    }

    pub fn topic_filter(&self) -> &str {
        self.topic.as_deref().unwrap_or("")
    }
}

/// Decodes an incoming ZeroMQ message payload into stream records.
pub fn decode_zmq_payload(payload: &[u8], topic: Option<&str>) -> Result<Vec<StreamRecord>> {
    let value: Value =
        serde_json::from_slice(payload).context("ZeroMQ message payload is not valid JSON")?;
    let mut records = Vec::new();
    match value {
        Value::Object(map) => {
            let mut rec = StreamRecord::new(map.into_iter().collect::<IndexMap<_, _>>());
            if let Some(t) = topic {
                let mut meta = std::collections::HashMap::new();
                meta.insert("topic".to_string(), Value::String(t.to_string()));
                rec.data.insert(
                    crate::META_KEY.to_string(),
                    Value::Object(meta.into_iter().collect()),
                );
            }
            records.push(rec);
        }
        Value::Array(items) => {
            for item in items {
                if let Value::Object(map) = item {
                    let mut rec = StreamRecord::new(map.into_iter().collect::<IndexMap<_, _>>());
                    if let Some(t) = topic {
                        let mut meta = std::collections::HashMap::new();
                        meta.insert("topic".to_string(), Value::String(t.to_string()));
                        rec.data.insert(
                            crate::META_KEY.to_string(),
                            Value::Object(meta.into_iter().collect()),
                        );
                    }
                    records.push(rec);
                } else {
                    tracing::warn!("Skipping non-object element in ZeroMQ batch array");
                }
            }
        }
        _ => anyhow::bail!("ZeroMQ JSON payload must be an object or array"),
    }
    Ok(records)
}

/// ZeroMQ subscriber source.
pub struct ZmqSource {
    pub config: ZmqSourceConfig,
    pub tx: StreamSender,
}

impl ZmqSource {
    pub fn new(config: ZmqSourceConfig, tx: StreamSender) -> Self {
        Self { config, tx }
    }

    /// Spawns the background subscriber task receiving ZeroMQ messages and
    /// forwarding decoded records to the stream processing pipeline.
    pub fn spawn(
        self,
        mut cancel_rx: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let endpoint = self.config.endpoint();
            let topic_sub = self.config.topic_filter();
            let mut backoff = tokio::time::Duration::from_millis(200);

            while !*cancel_rx.borrow() {
                let mut sub = SubSocket::new();
                let res = if self.config.is_bind() {
                    sub.bind(endpoint).await.map(|_| ())
                } else {
                    sub.connect(endpoint).await
                };

                match res {
                    Ok(_) => {
                        if let Err(e) = sub.subscribe(topic_sub).await {
                            tracing::warn!(
                                "ZMQ source subscription failed for topic '{}' on {}: {}; retrying...",
                                topic_sub,
                                endpoint,
                                e
                            );
                            tokio::select! {
                                _ = tokio::time::sleep(backoff) => {}
                                _ = cancel_rx.changed() => break,
                            }
                            backoff = (backoff * 2).min(tokio::time::Duration::from_secs(5));
                            continue;
                        }
                        backoff = tokio::time::Duration::from_millis(200);
                    }
                    Err(e) => {
                        tracing::warn!(
                            "ZMQ source connection failed on {}: {}; retrying...",
                            endpoint,
                            e
                        );
                        tokio::select! {
                            _ = tokio::time::sleep(backoff) => {}
                            _ = cancel_rx.changed() => break,
                        }
                        backoff = (backoff * 2).min(tokio::time::Duration::from_secs(5));
                        continue;
                    }
                }

                let mut should_reconnect = false;
                while !*cancel_rx.borrow() && !should_reconnect {
                    tokio::select! {
                        msg_res = sub.recv() => {
                            match msg_res {
                                Ok(msg) => {
                                    let (topic_extracted, payload_bytes) = if msg.len() > 1 {
                                        let top = msg.get(0).and_then(|b| std::str::from_utf8(b).ok());
                                        let pay = msg.get(1).map(|b| b.as_ref()).unwrap_or(&[]);
                                        (top, pay)
                                    } else if msg.len() == 1 {
                                        (None, msg.get(0).map(|b| b.as_ref()).unwrap_or(&[]))
                                    } else {
                                        continue;
                                    };

                                    match decode_zmq_payload(payload_bytes, topic_extracted) {
                                        Ok(records) => {
                                            for record in records {
                                                if matches!(self.tx.send(record).await, Err(rekuiper_core::PublishError::Closed)) {
                                                    return;
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            tracing::warn!("ZMQ source payload decoding error: {}", e);
                                        }
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!("ZMQ source receive error on {}: {}; attempting reconnect", endpoint, e);
                                    tokio::select! {
                                        _ = tokio::time::sleep(tokio::time::Duration::from_millis(500)) => {}
                                        _ = cancel_rx.changed() => return,
                                    }
                                    should_reconnect = true;
                                }
                            }
                        }
                        changed = cancel_rx.changed() => {
                            match changed {
                                Ok(_) => {
                                    if *cancel_rx.borrow() {
                                        return;
                                    }
                                }
                                Err(_) => return,
                            }
                        }
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zmq_sink_config_defaults() {
        let json = r#"{"server": "tcp://192.168.1.100:5563", "topic": "telemetry"}"#;
        let cfg: ZmqSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.endpoint(), "tcp://192.168.1.100:5563");
        assert_eq!(cfg.topic.as_deref(), Some("telemetry"));
        assert!(!cfg.is_bind());
    }

    #[test]
    fn test_zmq_sink_config_bind_mode() {
        let json = r#"{"server": "tcp://127.0.0.1:5564", "mode": "bind"}"#;
        let cfg: ZmqSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.endpoint(), "tcp://127.0.0.1:5564");
        assert!(cfg.is_bind());
    }

    #[test]
    fn test_zmq_source_config_defaults() {
        let json = r#"{"url": "tcp://10.0.0.1:5563", "topic": "events"}"#;
        let cfg: ZmqSourceConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.endpoint(), "tcp://10.0.0.1:5563");
        assert_eq!(cfg.topic_filter(), "events");
        assert!(!cfg.is_bind());
    }

    #[test]
    fn test_decode_zmq_payload_object() {
        let json_data = br#"{"temperature": 25.5, "humidity": 60}"#;
        let records = decode_zmq_payload(json_data, Some("sensors/d1")).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].data.get("temperature").unwrap(),
            &serde_json::json!(25.5)
        );
        let meta = records[0]
            .data
            .get(crate::META_KEY)
            .unwrap()
            .as_object()
            .unwrap();
        assert_eq!(meta.get("topic").unwrap().as_str().unwrap(), "sensors/d1");
    }

    #[test]
    fn test_decode_zmq_payload_array() {
        let json_data = br#"[{"id": 1}, {"id": 2}]"#;
        let records = decode_zmq_payload(json_data, None).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].data.get("id").unwrap(), &serde_json::json!(1));
        assert_eq!(records[1].data.get("id").unwrap(), &serde_json::json!(2));
    }

    #[tokio::test]
    async fn test_zmq_pub_sub_e2e_communication() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        let port = listener.local_addr().expect("local addr").port();
        drop(listener);
        let endpoint = format!("tcp://127.0.0.1:{}", port);

        // 1. Create Sink configured to bind
        let sink_cfg = ZmqSinkConfig {
            server: endpoint.clone(),
            url: None,
            topic: Some("devices/meter1".to_string()),
            mode: "bind".to_string(),
        };
        let sink = ZmqSink::new(sink_cfg);
        let payload = br#"{"voltage": 230.5, "current": 10.2}"#;
        // Bind the publisher
        let _ = sink.send_raw(None, payload).await;

        // 2. Create Source configured to connect and subscribe
        let source_cfg = ZmqSourceConfig {
            server: endpoint.clone(),
            url: None,
            topic: Some("devices/meter1".to_string()),
            mode: "connect".to_string(),
        };
        let bus = rekuiper_core::StreamBus::default();
        let tx = bus.get_or_create("demo");
        let mut rx = bus.subscribe("demo");
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        let source = ZmqSource::new(source_cfg, tx);
        let handle = source.spawn(cancel_rx);

        // Periodically emit until subscriber completes handshake and receives record
        let rec = tokio::time::timeout(tokio::time::Duration::from_secs(5), async {
            loop {
                let _ = sink.send_raw(None, payload).await;
                if let Ok(Some(r)) =
                    tokio::time::timeout(tokio::time::Duration::from_millis(50), rx.recv()).await
                {
                    return r;
                }
            }
        })
        .await
        .expect("timeout receiving record from ZMQ subscriber");

        assert_eq!(rec.data.get("voltage").unwrap(), &serde_json::json!(230.5));
        assert_eq!(rec.data.get("current").unwrap(), &serde_json::json!(10.2));

        let _ = cancel_tx.send(true);
        let _ = handle.await;
    }
}
