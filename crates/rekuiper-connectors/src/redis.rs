use crate::META_KEY;
use anyhow::{Context, Result};
use indexmap::IndexMap;
use parking_lot::RwLock;
use rekuiper_core::model::StreamRecord;
use rekuiper_core::StreamSender;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::LazyLock;

static REDIS_CONNS: LazyLock<RwLock<HashMap<String, redis::aio::MultiplexedConnection>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Resolve or establish a shared multiplexed async Redis connection for `url`.
pub async fn get_redis_connection(url: &str) -> Result<redis::aio::MultiplexedConnection> {
    if let Some(conn) = REDIS_CONNS.read().get(url).cloned() {
        return Ok(conn);
    }
    let client = redis::Client::open(url)?;
    let conn = client.get_multiplexed_async_connection().await?;
    let mut guard = REDIS_CONNS.write();
    let entry = guard.entry(url.to_string()).or_insert_with(|| conn.clone());
    Ok(entry.clone())
}

/// Evict a cached Redis connection on transport error so the next attempt reconnects.
pub fn evict_redis_connection(url: &str) {
    REDIS_CONNS.write().remove(url);
}

/// Redis sink configuration (eKuiper redis / redisPub sink format).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RedisSinkConfig {
    /// Server address, e.g. `"127.0.0.1:6379"` (or a full `redis://` URL).
    #[serde(default = "default_redis_addr")]
    pub addr: String,
    /// Optional logical database number.
    #[serde(default)]
    pub db: Option<u32>,
    /// Static key for `SET` (overridden by `field` when both resolve).
    #[serde(default)]
    pub key: Option<String>,
    /// Record field whose value supplies the `SET` key.
    #[serde(default)]
    pub field: Option<String>,
    /// Value encoding, `"string"` (default).
    #[serde(default = "default_redis_datatype")]
    pub data_type: String,
    /// When present the record is `PUBLISH`ed to this topic instead of `SET`.
    #[serde(default)]
    pub topic: Option<String>,
}

fn default_redis_addr() -> String {
    "127.0.0.1:6379".to_string()
}

fn default_redis_datatype() -> String {
    "string".to_string()
}

impl RedisSinkConfig {
    pub fn connection_url(&self) -> String {
        let addr = if self.addr.contains("://") {
            self.addr.clone()
        } else {
            format!("redis://{}", self.addr)
        };
        if let Some(db) = self.db {
            format!("{}/{}", addr.trim_end_matches('/'), db)
        } else {
            addr
        }
    }
}

/// Redis sink: `SET <key> <json>` per record, or `PUBLISH <topic> <json>`
/// when a topic is configured (redisPub action), multiplexed over a
/// persistent connection per Redis URL.
pub struct RedisSink {
    pub config: RedisSinkConfig,
}

impl RedisSink {
    pub fn new(config: RedisSinkConfig) -> Self {
        Self { config }
    }

    async fn connection(&self) -> Result<(String, redis::aio::MultiplexedConnection)> {
        let url = self.config.connection_url();
        let conn = get_redis_connection(&url).await?;
        Ok((url, conn))
    }

    pub async fn send_raw(&self, payload: &str, record: &StreamRecord) -> Result<()> {
        let (url, mut conn) = self.connection().await?;
        if let Some(topic) = self.config.topic.as_deref() {
            let res: redis::RedisResult<()> = redis::cmd("PUBLISH")
                .arg(topic)
                .arg(payload)
                .query_async(&mut conn)
                .await;
            if res.is_err() {
                evict_redis_connection(&url);
                let mut fresh = get_redis_connection(&url).await?;
                redis::cmd("PUBLISH")
                    .arg(topic)
                    .arg(payload)
                    .query_async::<()>(&mut fresh)
                    .await?;
            }
            return Ok(());
        }
        let key = self
            .config
            .field
            .as_deref()
            .and_then(|f| record.data.get(f))
            .map(redis_scalar_key)
            .filter(|k| !k.is_empty())
            .or_else(|| self.config.key.clone())
            .ok_or_else(|| anyhow::anyhow!("Redis sink needs a key: set `field` or `key`"))?;
        let res: redis::RedisResult<()> = redis::cmd("SET")
            .arg(&key)
            .arg(payload)
            .query_async(&mut conn)
            .await;
        if res.is_err() {
            evict_redis_connection(&url);
            let mut fresh = get_redis_connection(&url).await?;
            redis::cmd("SET")
                .arg(&key)
                .arg(payload)
                .query_async::<()>(&mut fresh)
                .await?;
        }
        Ok(())
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
        self.send_raw(&json_str, record).await
    }
}

/// Render a record value as a Redis key fragment.
fn redis_scalar_key(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

/// Redis pub/sub source: subscribes to a channel and forwards each JSON
/// payload as [`StreamRecord`]s.
pub struct RedisSubSource {
    pub url: String,
    pub channel: String,
    pub tx: StreamSender,
}

impl RedisSubSource {
    pub fn spawn(
        self,
        mut cancel_rx: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let client = match redis::Client::open(self.url.clone()) {
                Ok(client) => client,
                Err(e) => {
                    tracing::warn!("Redis sub source cannot parse url {}: {}", self.url, e);
                    return;
                }
            };
            let mut pubsub = match client.get_async_pubsub().await {
                Ok(pubsub) => pubsub,
                Err(e) => {
                    tracing::warn!("Redis sub source connect to {} failed: {}", self.url, e);
                    return;
                }
            };
            if let Err(e) = pubsub.subscribe(self.channel.clone()).await {
                tracing::warn!(
                    "Redis sub source subscribe to {} failed: {}",
                    self.channel,
                    e
                );
                return;
            }
            use futures::StreamExt;
            let mut messages = pubsub.on_message();
            loop {
                tokio::select! {
                    msg = messages.next() => {
                        let Some(msg) = msg else {
                            // Message stream ended.
                            break;
                        };
                        let payload: Vec<u8> = match msg.get_payload() {
                            Ok(payload) => payload,
                            Err(e) => {
                                tracing::warn!("Skipping invalid Redis message: {}", e);
                                continue;
                            }
                        };
                        match decode_redis_payload(&payload) {
                            Ok(records) => {
                                for record in records {
                                    // All subscribers dropped: shut down cleanly.
                                    if self.tx.send(record).await.is_err() {
                                        return;
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::warn!("Skipping invalid Redis payload: {}", e);
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
                            // Cancellation sender dropped: shut down.
                            Err(_) => return,
                        }
                    }
                }
            }
        })
    }
}

/// Decode one Redis message payload: a JSON object yields a single record,
/// a JSON array yields one record per object element.
fn decode_redis_payload(payload: &[u8]) -> Result<Vec<StreamRecord>> {
    let value: Value =
        serde_json::from_slice(payload).context("Redis payload is not valid JSON")?;
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
                    tracing::warn!("Skipping non-object element in Redis message");
                }
            }
            Ok(records)
        }
        _ => anyhow::bail!("Redis message must be a JSON object or array"),
    }
}

/// Point lookup against Redis using a shared multiplexed connection:
/// `GET <key>`, decoding the stored bytes as JSON when possible and
/// falling back to the raw scalar string. Returns `None` for missing keys.
pub async fn redis_lookup_key(addr: &str, key: &str) -> Result<Option<Value>> {
    let url = if addr.contains("://") {
        addr.to_string()
    } else {
        format!("redis://{}", addr)
    };
    let mut conn = get_redis_connection(&url).await?;
    let stored: Option<Vec<u8>> = match redis::cmd("GET").arg(key).query_async(&mut conn).await {
        Ok(val) => val,
        Err(_) => {
            evict_redis_connection(&url);
            let mut fresh = get_redis_connection(&url).await?;
            redis::cmd("GET").arg(key).query_async(&mut fresh).await?
        }
    };
    match stored {
        None => Ok(None),
        Some(bytes) => match serde_json::from_slice::<Value>(&bytes) {
            Ok(value) => Ok(Some(value)),
            Err(_) => Ok(Some(Value::String(
                String::from_utf8_lossy(&bytes).into_owned(),
            ))),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

    #[tokio::test]
    async fn test_redis_sink_and_lookup_reuse_single_tcp_connection() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("127.0.0.1:{}", listener.local_addr().unwrap().port());
        let accept_count = Arc::new(AtomicUsize::new(0));
        let accept_count_clone = accept_count.clone();

        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                accept_count_clone.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    let (rh, mut wh) = stream.into_split();
                    let mut rd = BufReader::new(rh);
                    let mut line = String::new();
                    loop {
                        line.clear();
                        if rd.read_line(&mut line).await.unwrap_or(0) == 0 {
                            break;
                        }
                        let Some(count) = line
                            .trim_end_matches(['\r', '\n'])
                            .strip_prefix('*')
                            .and_then(|s| s.parse::<usize>().ok())
                        else {
                            break;
                        };
                        let mut argv = Vec::with_capacity(count);
                        for _ in 0..count {
                            line.clear();
                            if rd.read_line(&mut line).await.unwrap_or(0) == 0 {
                                return;
                            }
                            let len: usize = line
                                .trim_end_matches(['\r', '\n'])
                                .strip_prefix('$')
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(0);
                            let mut buf = vec![0u8; len];
                            if rd.read_exact(&mut buf).await.is_err() {
                                return;
                            }
                            let mut crlf = [0u8; 2];
                            let _ = rd.read_exact(&mut crlf).await;
                            argv.push(String::from_utf8_lossy(&buf).into_owned());
                        }
                        let cmd = argv
                            .first()
                            .map(|s| s.to_ascii_uppercase())
                            .unwrap_or_default();
                        let reply: &[u8] = match cmd.as_str() {
                            "SET" | "CLIENT" | "PING" => b"+OK\r\n",
                            "PUBLISH" => b":1\r\n",
                            "GET" => b"$15\r\n{\"status\":\"ok\"}\r\n",
                            _ => b"+OK\r\n",
                        };
                        if wh.write_all(reply).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });

        let sink = RedisSink::new(RedisSinkConfig {
            addr: addr.clone(),
            db: None,
            key: Some("k1".to_string()),
            field: None,
            data_type: "string".to_string(),
            topic: None,
        });

        for i in 0..5 {
            let mut map = HashMap::new();
            map.insert("v".to_string(), serde_json::json!(i));
            sink.send(&StreamRecord::new(map)).await.unwrap();
        }
        for _ in 0..5 {
            let v = redis_lookup_key(&addr, "k1").await.unwrap();
            assert_eq!(v, Some(serde_json::json!({"status": "ok"})));
        }

        assert_eq!(
            accept_count.load(Ordering::SeqCst),
            1,
            "All SET and GET operations must multiplex over a single TCP connection"
        );
    }
}
