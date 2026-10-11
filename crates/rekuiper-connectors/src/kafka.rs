use crate::META_KEY;
use anyhow::{Context, Result};
use rekuiper_core::model::StreamRecord;
use rekuiper_core::StreamSender;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Kafka connection settings (eKuiper kafka source/sink format).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KafkaConfig {
    /// Comma-separated broker addresses, e.g. `"127.0.0.1:9092"`.
    #[serde(default = "default_kafka_brokers")]
    pub brokers: String,
    /// Topic to consume from / produce to (source streams take it from
    /// `DATASOURCE` when absent here).
    #[serde(default)]
    pub topic: Option<String>,
    /// Consumer group id (source side informational).
    #[serde(default)]
    pub group_id: Option<String>,
    /// Partition number (default 0).
    #[serde(default)]
    pub partition: i32,
    /// Record field supplying the produce message key (sink side).
    #[serde(default)]
    pub key: Option<String>,
    /// Offset reset policy: `"earliest"` or `"latest"` (default `"latest"`).
    #[serde(default = "default_offset_reset", alias = "offset_reset")]
    pub offset_reset: String,
}

fn default_kafka_brokers() -> String {
    "127.0.0.1:9092".to_string()
}

fn default_offset_reset() -> String {
    "latest".to_string()
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            brokers: default_kafka_brokers(),
            topic: None,
            group_id: None,
            partition: 0,
            key: None,
            offset_reset: default_offset_reset(),
        }
    }
}

impl KafkaConfig {
    pub fn broker_list(&self) -> Vec<String> {
        self.brokers
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    }
}

/// Bounded retry policy for Kafka control-plane calls: without a deadline
/// rskafka backs off indefinitely, which would wedge rule tasks and tests
/// when brokers are unreachable.
fn kafka_backoff_config() -> rskafka::BackoffConfig {
    rskafka::BackoffConfig {
        init_backoff: std::time::Duration::from_millis(100),
        max_backoff: std::time::Duration::from_secs(1),
        base: 2.0,
        deadline: Some(std::time::Duration::from_secs(5)),
    }
}

/// Kafka sink: serializes each record as JSON and produces it to the
/// configured topic/partition, keyed by the optional `key` field.
pub struct KafkaSink {
    pub config: KafkaConfig,
}

impl KafkaSink {
    pub async fn send_raw(&self, payload: Vec<u8>, record: &StreamRecord) -> Result<()> {
        match tokio::time::timeout(
            std::time::Duration::from_secs(10),
            self.send_raw_inner(payload, record),
        )
        .await
        {
            Ok(res) => res,
            Err(_) => anyhow::bail!(
                "Kafka produce to {} timed out",
                self.config.topic.as_deref().unwrap_or("default")
            ),
        }
    }

    async fn send_raw_inner(&self, payload: Vec<u8>, record: &StreamRecord) -> Result<()> {
        use rskafka::client::{
            partition::{Compression, UnknownTopicHandling},
            ClientBuilder,
        };
        use rskafka::record::Record;
        let brokers = self.config.broker_list();
        if brokers.is_empty() {
            anyhow::bail!("Kafka sink needs at least one broker");
        }
        let client = ClientBuilder::new(brokers)
            .backoff_config(kafka_backoff_config())
            .build()
            .await?;
        let topic = self.config.topic.as_deref().unwrap_or("default");
        let partition_client = client
            .partition_client(topic, self.config.partition, UnknownTopicHandling::Error)
            .await?;
        let key_bytes = self
            .config
            .key
            .as_ref()
            .and_then(|k| record.data.get(k))
            .map(value_to_key_bytes);
        let kafka_record = Record {
            key: key_bytes,
            value: Some(payload),
            headers: std::collections::BTreeMap::new(),
            timestamp: chrono::Utc::now(),
        };
        partition_client
            .produce(vec![kafka_record], Compression::NoCompression)
            .await?;
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
        let json_bytes = serde_json::to_vec(&map)?;
        self.send_raw(json_bytes, record).await
    }
}

/// Render a record value as Kafka message key bytes.
fn value_to_key_bytes(v: &Value) -> Vec<u8> {
    match v {
        Value::Null => Vec::new(),
        Value::String(s) => s.clone().into_bytes(),
        Value::Number(n) => n.to_string().into_bytes(),
        Value::Bool(b) => b.to_string().into_bytes(),
        other => serde_json::to_vec(other).unwrap_or_default(),
    }
}

/// Kafka source: consumes a topic partition from the latest offset and
/// forwards each JSON payload as [`StreamRecord`]s.
pub struct KafkaSource {
    pub config: KafkaConfig,
    pub tx: StreamSender,
}

impl KafkaSource {
    pub fn spawn(
        self,
        mut cancel_rx: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            use rskafka::client::{
                partition::{OffsetAt, UnknownTopicHandling},
                ClientBuilder,
            };
            let brokers = self.config.broker_list();
            if brokers.is_empty() {
                tracing::warn!("Kafka source has no brokers configured");
                return;
            }
            let client = match ClientBuilder::new(brokers)
                .backoff_config(kafka_backoff_config())
                .build()
                .await
            {
                Ok(client) => client,
                Err(e) => {
                    tracing::warn!("Kafka source connect failed: {}", e);
                    return;
                }
            };
            let topic = self.config.topic.as_deref().unwrap_or("default");
            let partition_client = match client
                .partition_client(topic, self.config.partition, UnknownTopicHandling::Error)
                .await
            {
                Ok(partition_client) => partition_client,
                Err(e) => {
                    tracing::warn!("Kafka source partition client failed: {}", e);
                    return;
                }
            };
            let offset_at = if self.config.offset_reset.eq_ignore_ascii_case("earliest") {
                OffsetAt::Earliest
            } else {
                OffsetAt::Latest
            };
            let mut offset = partition_client.get_offset(offset_at).await.unwrap_or(0);
            loop {
                tokio::select! {
                    fetched = partition_client.fetch_records(offset, 1..1_000_000, 1000) => {
                        match fetched {
                            Ok((records, _high_watermark)) => {
                                for rec in records {
                                    offset = rec.offset + 1;
                                    let payload = rec.record.value.as_deref().unwrap_or_default();
                                    match decode_kafka_payload(payload) {
                                        Ok(decoded) => {
                                            for record in decoded {
                                                // All subscribers dropped: shut down cleanly.
                                                if self.tx.send(record).await.is_err() {
                                                    return;
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            tracing::warn!("Skipping invalid Kafka payload: {}", e);
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::warn!("Kafka source fetch failed: {}", e);
                                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
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

/// Decode one Kafka record payload: a JSON object yields a single record, a
/// JSON array yields one record per object element.
fn decode_kafka_payload(payload: &[u8]) -> Result<Vec<StreamRecord>> {
    let value: Value =
        serde_json::from_slice(payload).context("Kafka payload is not valid JSON")?;
    match value {
        Value::Object(map) => Ok(vec![StreamRecord::new(map.into_iter().collect())]),
        Value::Array(items) => {
            let mut records = Vec::new();
            for item in items {
                if let Value::Object(map) = item {
                    records.push(StreamRecord::new(map.into_iter().collect()));
                } else {
                    tracing::warn!("Skipping non-object element in Kafka message");
                }
            }
            Ok(records)
        }
        _ => anyhow::bail!("Kafka message must be a JSON object or array"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kafka_config_offset_reset_defaults_and_aliases() {
        let default_cfg: KafkaConfig = serde_json::from_str("{}").expect("parse default config");
        assert_eq!(default_cfg.offset_reset, "latest");

        let camel_cfg: KafkaConfig = serde_json::from_str(r#"{"offsetReset": "earliest"}"#)
            .expect("parse camelCase offsetReset");
        assert_eq!(camel_cfg.offset_reset, "earliest");

        let snake_cfg: KafkaConfig = serde_json::from_str(r#"{"offset_reset": "earliest"}"#)
            .expect("parse snake_case offset_reset");
        assert_eq!(snake_cfg.offset_reset, "earliest");
    }
}
