use crate::META_KEY;
use anyhow::{Context, Result};
use indexmap::IndexMap;
use parking_lot::RwLock;
use rekuiper_core::model::{RecordMeta, StreamRecord};
use rekuiper_core::StreamSender;
use rskafka::client::partition::PartitionClient;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

type KafkaPartitionKey = (Vec<String>, String, i32);

static KAFKA_PARTITION_CLIENTS: LazyLock<RwLock<HashMap<KafkaPartitionKey, Arc<PartitionClient>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

async fn get_or_connect_partition_client(config: &KafkaConfig) -> Result<Arc<PartitionClient>> {
    use rskafka::client::{partition::UnknownTopicHandling, ClientBuilder};

    let brokers = config.broker_list();
    if brokers.is_empty() {
        anyhow::bail!("Kafka sink needs at least one broker");
    }
    let topic = config.topic.as_deref().unwrap_or("default").to_string();
    let key: KafkaPartitionKey = (brokers.clone(), topic.clone(), config.partition);

    if let Some(client) = KAFKA_PARTITION_CLIENTS.read().get(&key).cloned() {
        return Ok(client);
    }

    let client = ClientBuilder::new(brokers)
        .backoff_config(kafka_backoff_config())
        .build()
        .await?;
    let partition_client = Arc::new(
        client
            .partition_client(&topic, config.partition, UnknownTopicHandling::Error)
            .await?,
    );
    let mut guard = KAFKA_PARTITION_CLIENTS.write();
    let entry = guard.entry(key).or_insert_with(|| partition_client.clone());
    Ok(entry.clone())
}

fn evict_partition_client(config: &KafkaConfig) {
    let brokers = config.broker_list();
    let topic = config.topic.as_deref().unwrap_or("default").to_string();
    let key: KafkaPartitionKey = (brokers, topic, config.partition);
    KAFKA_PARTITION_CLIENTS.write().remove(&key);
}

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
/// configured topic/partition using a cached [`PartitionClient`], keyed by
/// the optional `key` field.
pub struct KafkaSink {
    pub config: KafkaConfig,
}

impl KafkaSink {
    pub fn new(config: KafkaConfig) -> Self {
        Self { config }
    }

    pub async fn send_raw(&self, payload: Vec<u8>, record: &StreamRecord) -> Result<()> {
        match tokio::time::timeout(
            std::time::Duration::from_secs(10),
            self.send_raw_inner(payload, record),
        )
        .await
        {
            Ok(res) => res,
            Err(_) => {
                evict_partition_client(&self.config);
                anyhow::bail!(
                    "Kafka produce to {} timed out",
                    self.config.topic.as_deref().unwrap_or("default")
                )
            }
        }
    }

    async fn send_raw_inner(&self, payload: Vec<u8>, record: &StreamRecord) -> Result<()> {
        use rskafka::client::partition::Compression;
        use rskafka::record::Record;

        let partition_client = get_or_connect_partition_client(&self.config).await?;
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
        if let Err(e) = partition_client
            .produce(vec![kafka_record], Compression::NoCompression)
            .await
        {
            evict_partition_client(&self.config);
            return Err(e.into());
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

/// Kafka source: consumes a topic partition from the configured offset and
/// forwards each JSON payload as [`StreamRecord`]s.
pub struct KafkaSource {
    pub config: KafkaConfig,
    pub tx: StreamSender,
    pub status_callback: Option<crate::ConnectorStatusCallback>,
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
                Ok(partition_client) => {
                    if let Some(ref cb) = self.status_callback {
                        cb(topic, "kafka", "connected", None);
                    }
                    partition_client
                }
                Err(e) => {
                    tracing::warn!("Kafka source partition client failed: {}", e);
                    if let Some(ref cb) = self.status_callback {
                        cb(topic, "kafka", "error", Some(&e.to_string()));
                    }
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
                                    match decode_kafka_payload(payload, Some(topic)) {
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
                                if let Some(ref cb) = self.status_callback {
                                    cb(topic, "kafka", "error", Some(&e.to_string()));
                                }
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
fn decode_kafka_payload(payload: &[u8], topic: Option<&str>) -> Result<Vec<StreamRecord>> {
    let value: Value =
        serde_json::from_slice(payload).context("Kafka payload is not valid JSON")?;
    let rec_meta = Arc::new(RecordMeta {
        topic: topic.map(Arc::from),
        source: Some(Arc::from("kafka")),
        custom: None,
    });
    match value {
        Value::Object(map) => {
            let mut rec = StreamRecord::new(map.into_iter().collect::<IndexMap<_, _>>());
            rec.meta = Some(rec_meta);
            Ok(vec![rec])
        }
        Value::Array(items) => {
            let mut records = Vec::new();
            for item in items {
                if let Value::Object(map) = item {
                    let mut rec = StreamRecord::new(map.into_iter().collect::<IndexMap<_, _>>());
                    rec.meta = Some(rec_meta.clone());
                    records.push(rec);
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

    #[test]
    fn test_kafka_broker_list_parsing() {
        let cfg = KafkaConfig {
            brokers: "  10.0.0.1:9092, 10.0.0.2:9092, , 10.0.0.3:9092  ".to_string(),
            ..Default::default()
        };
        assert_eq!(
            cfg.broker_list(),
            vec![
                "10.0.0.1:9092".to_string(),
                "10.0.0.2:9092".to_string(),
                "10.0.0.3:9092".to_string(),
            ]
        );
    }

    #[test]
    fn test_value_to_key_bytes() {
        assert_eq!(value_to_key_bytes(&Value::Null), Vec::<u8>::new());
        assert_eq!(
            value_to_key_bytes(&Value::String("dev1".to_string())),
            b"dev1".to_vec()
        );
        assert_eq!(value_to_key_bytes(&serde_json::json!(42)), b"42".to_vec());
        assert_eq!(
            value_to_key_bytes(&serde_json::json!(true)),
            b"true".to_vec()
        );
    }

    #[test]
    fn test_decode_kafka_payload() {
        // Single object
        let obj_payload = br#"{"sensor": "temp", "val": 25.5}"#;
        let records = decode_kafka_payload(obj_payload, Some("test_topic")).expect("decode object");
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].data.get("sensor"),
            Some(&serde_json::json!("temp"))
        );
        assert_eq!(
            records[0].meta.as_ref().and_then(|m| m.topic.as_deref()),
            Some("test_topic")
        );

        // Array of objects
        let arr_payload = br#"[{"id": 1}, {"id": 2}]"#;
        let records = decode_kafka_payload(arr_payload, Some("test_topic")).expect("decode array");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].data.get("id"), Some(&serde_json::json!(1)));
        assert_eq!(records[1].data.get("id"), Some(&serde_json::json!(2)));

        // Invalid JSON
        assert!(decode_kafka_payload(b"not json", None).is_err());

        // Invalid top-level scalar
        assert!(decode_kafka_payload(b"123", None).is_err());
    }
}
