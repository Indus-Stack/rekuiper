use crate::META_KEY;
use anyhow::{Context, Result};
use indexmap::IndexMap;
use parking_lot::Mutex;
use rekuiper_core::model::StreamRecord;
use rekuiper_core::StreamSender;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

type WsClientStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

type WsSinkSlot = Arc<tokio::sync::Mutex<Option<WsClientStream>>>;
type WsSinkRegistry = Mutex<HashMap<String, WsSinkSlot>>;

static WS_SINKS: LazyLock<WsSinkRegistry> = LazyLock::new(|| Mutex::new(HashMap::new()));

fn ws_sink_slot(url: &str) -> WsSinkSlot {
    let mut guard = WS_SINKS.lock();
    guard
        .entry(url.to_string())
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(None)))
        .clone()
}

/// WebSocket endpoint configuration (eKuiper websocket source/sink format).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WebSocketConfig {
    /// `host:port`, e.g. `"127.0.0.1:8080"`.
    #[serde(default)]
    pub addr: Option<String>,
    /// Alias for [`WebSocketConfig::addr`].
    #[serde(default)]
    pub address: Option<String>,
    /// URL scheme, `"ws"` (default) or `"wss"`.
    #[serde(default = "default_ws_scheme")]
    pub scheme: String,
    /// URL path, e.g. `"/api/data"`.
    #[serde(default = "default_ws_path")]
    pub path: String,
}

fn default_ws_scheme() -> String {
    "ws".to_string()
}

fn default_ws_path() -> String {
    "/".to_string()
}

impl WebSocketConfig {
    pub fn target_url(&self) -> String {
        let host = self
            .addr
            .as_deref()
            .or(self.address.as_deref())
            .unwrap_or("127.0.0.1:8080");
        let mut p = self.path.clone();
        if !p.starts_with('/') {
            p = format!("/{}", p);
        }
        format!("{}://{}{}", self.scheme, host, p)
    }
}

/// Decode one incoming WebSocket text payload into records: a JSON object
/// yields a single [`StreamRecord`], a JSON array yields one record per
/// object element (non-objects are skipped).
pub fn decode_websocket_message(text: &str) -> Result<Vec<StreamRecord>> {
    let value: Value = serde_json::from_str(text).context("WebSocket payload is not valid JSON")?;
    match value {
        Value::Object(map) => Ok(vec![StreamRecord::new(
            map.into_iter().collect::<IndexMap<_, _>>(),
        )]),
        Value::Array(items) => {
            let mut records = Vec::new();
            for item in items {
                if let Value::Object(map) = item {
                    records.push(StreamRecord::new(
                        map.into_iter().collect::<IndexMap<_, _>>(),
                    ));
                } else {
                    tracing::warn!("Skipping non-object element in WebSocket message");
                }
            }
            Ok(records)
        }
        _ => anyhow::bail!("WebSocket message must be a JSON object or array"),
    }
}

/// WebSocket source: streams incoming text messages as [`StreamRecord`]s.
pub struct WebSocketSource {
    pub url: String,
    pub tx: StreamSender,
}

impl WebSocketSource {
    pub fn spawn(
        self,
        mut cancel_rx: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let (mut ws_stream, _) = match tokio_tungstenite::connect_async(&self.url).await {
                Ok(conn) => conn,
                Err(e) => {
                    tracing::warn!("WebSocket source connect to {} failed: {}", self.url, e);
                    return;
                }
            };
            use futures::StreamExt;
            loop {
                tokio::select! {
                    msg = ws_stream.next() => {
                        match msg {
                            Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) => {
                                match decode_websocket_message(&text) {
                                    Ok(records) => {
                                        for record in records {
                                            // All subscribers dropped: shut down cleanly.
                                            if self.tx.send(record).await.is_err() {
                                                return;
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        tracing::warn!("Skipping invalid WebSocket message: {}", e);
                                    }
                                }
                            }
                            Some(Ok(_)) => {}
                            Some(Err(e)) => {
                                tracing::warn!("WebSocket source error on {}: {}", self.url, e);
                                break;
                            }
                            None => break,
                        }
                    }
                    changed = cancel_rx.changed() => {
                        match changed {
                            Ok(_) => {
                                if *cancel_rx.borrow() {
                                    return;
                                }
                            }
                            // Cancellation sender dropped: shut down.
                            Err(_) => return,
                        }
                    }
                }
            }
        })
    }
}

/// WebSocket sink: transmits each record as a JSON text message over a
/// persistent WebSocket connection, reconnecting automatically if the peer
/// closes the stream.
pub struct WebSocketSink {
    pub url: String,
}

impl WebSocketSink {
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into() }
    }

    pub async fn send(&self, record: &StreamRecord) -> Result<()> {
        let mut map = std::collections::BTreeMap::new();
        for (k, v) in &record.data {
            if k == META_KEY || k.starts_with("__") {
                continue;
            }
            map.insert(k.clone(), v.clone());
        }
        let json_str = serde_json::to_string(&map)?;
        self.send_text(&json_str).await
    }

    /// Transmits a pre-rendered text payload over the persistent WebSocket stream.
    pub async fn send_text(&self, text: &str) -> Result<()> {
        use futures::SinkExt;
        use tokio_tungstenite::tungstenite::Message;

        let slot = ws_sink_slot(&self.url);
        let mut guard = slot.lock().await;
        if guard.is_none() {
            let (ws_stream, _) = tokio_tungstenite::connect_async(&self.url).await?;
            *guard = Some(ws_stream);
        }

        let ws_stream = guard.as_mut().expect("ws_stream initialized");
        if ws_stream
            .send(Message::Text(text.to_string()))
            .await
            .is_err()
        {
            // Peer closed or reset the connection: reconnect once and retry.
            *guard = None;
            let (mut fresh_stream, _) = tokio_tungstenite::connect_async(&self.url).await?;
            fresh_stream.send(Message::Text(text.to_string())).await?;
            *guard = Some(fresh_stream);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    #[tokio::test]
    async fn test_websocket_sink_reuses_persistent_connection() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let accept_count = Arc::new(AtomicUsize::new(0));
        let accept_count_clone = accept_count.clone();
        let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(8);

        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                accept_count_clone.fetch_add(1, Ordering::SeqCst);
                let tx = tx.clone();
                tokio::spawn(async move {
                    let Ok(mut ws) = accept_async(stream).await else {
                        return;
                    };
                    while let Some(Ok(msg)) = ws.next().await {
                        if let Message::Text(text) = msg {
                            let _ = tx.send(text).await;
                        }
                    }
                });
            }
        });

        let sink = WebSocketSink::new(format!("ws://127.0.0.1:{}/ws_persistent", port));
        sink.send_text(r#"{"seq":1}"#).await.unwrap();
        sink.send_text(r#"{"seq":2}"#).await.unwrap();
        sink.send_text(r#"{"seq":3}"#).await.unwrap();

        let mut received = Vec::new();
        for _ in 0..3 {
            let msg = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
                .await
                .expect("timed out waiting for ws frame")
                .expect("channel closed");
            received.push(msg);
        }
        assert_eq!(
            received,
            vec![r#"{"seq":1}"#, r#"{"seq":2}"#, r#"{"seq":3}"#]
        );
        assert_eq!(
            accept_count.load(Ordering::SeqCst),
            1,
            "All WebSocket frames must be sent over a single persistent connection"
        );
    }
}
