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
    /// Optional explicit next offset to restore from in direct-partition mode.
    #[serde(default)]
    pub next_offset: Option<i64>,
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
            next_offset: None,
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
            let mut offset = if let Some(requested_next) = self.config.next_offset {
                let earliest = partition_client
                    .get_offset(OffsetAt::Earliest)
                    .await
                    .unwrap_or(0);
                match resolve_start_offset(
                    earliest,
                    earliest,
                    Some(requested_next),
                    &self.config.offset_reset,
                ) {
                    Ok(start) => start,
                    Err(e) => {
                        tracing::error!("Kafka source restore failed: {}", e);
                        if let Some(ref cb) = self.status_callback {
                            cb(topic, "kafka", "error", Some(&e.to_string()));
                        }
                        return;
                    }
                }
            } else {
                let offset_at = if self.config.offset_reset.eq_ignore_ascii_case("earliest") {
                    OffsetAt::Earliest
                } else {
                    OffsetAt::Latest
                };
                partition_client.get_offset(offset_at).await.unwrap_or(0)
            };
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

/// Terminal policy for Kafka tombstone (null/empty) records in reliable mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KafkaTombstonePolicy {
    /// Fail visibly and stop advancing when a tombstone is encountered.
    #[default]
    Fail,
    /// Skip the tombstone and allow committing its offset boundary.
    SkipAndAdvance,
}

/// Errors produced by reliable direct-partition Kafka replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KafkaReliableError {
    RetentionGap {
        requested_offset: i64,
        earliest_available: i64,
    },
    Tombstone {
        offset: i64,
    },
    DecodeFailed {
        offset: i64,
        message: String,
    },
    InvalidCursor,
    OutOfOrderCommit {
        expected_offset: i64,
        actual_offset: i64,
    },
}

impl std::fmt::Display for KafkaReliableError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RetentionGap {
                requested_offset,
                earliest_available,
            } => write!(
                f,
                "kafka retention gap: requested next offset {requested_offset} is older than earliest available {earliest_available}"
            ),
            Self::Tombstone { offset } => {
                write!(f, "kafka tombstone encountered at offset {offset}")
            }
            Self::DecodeFailed { offset, message } => {
                write!(f, "kafka decode failed at offset {offset}: {message}")
            }
            Self::InvalidCursor => write!(f, "invalid kafka offset cursor"),
            Self::OutOfOrderCommit {
                expected_offset,
                actual_offset,
            } => write!(
                f,
                "out-of-order kafka commit: expected offset {expected_offset}, got {actual_offset}"
            ),
        }
    }
}

impl std::error::Error for KafkaReliableError {}

/// Codec for big-endian `i64` next-offset Kafka cursors.
#[derive(Debug, Clone)]
pub struct KafkaPositionCodec {
    codec_id: rekuiper_core::contracts::PositionCodecId,
}

impl Default for KafkaPositionCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl KafkaPositionCodec {
    pub fn new() -> Self {
        Self {
            codec_id: rekuiper_core::contracts::PositionCodecId::try_from("kafka.offset.v1")
                .expect("valid static kafka codec id"),
        }
    }

    pub fn encode_next_offset(next_offset: i64) -> Vec<u8> {
        next_offset.to_be_bytes().to_vec()
    }

    pub fn decode_next_offset(cursor: &[u8]) -> std::result::Result<i64, KafkaReliableError> {
        let bytes: [u8; 8] = cursor
            .try_into()
            .map_err(|_| KafkaReliableError::InvalidCursor)?;
        Ok(i64::from_be_bytes(bytes))
    }

    pub fn make_position(
        &self,
        partition: rekuiper_core::contracts::SourcePartition,
        generation: rekuiper_core::contracts::Generation,
        next_offset: i64,
    ) -> rekuiper_core::contracts::SourcePosition {
        rekuiper_core::contracts::SourcePosition {
            partition,
            generation,
            codec: self.codec_id.clone(),
            cursor: Self::encode_next_offset(next_offset),
        }
    }
}

impl rekuiper_core::contracts::PositionCodec for KafkaPositionCodec {
    fn id(&self) -> &rekuiper_core::contracts::PositionCodecId {
        &self.codec_id
    }

    fn compare(&self, left: &[u8], right: &[u8]) -> Option<std::cmp::Ordering> {
        let l = Self::decode_next_offset(left).ok()?;
        let r = Self::decode_next_offset(right).ok()?;
        Some(l.cmp(&r))
    }
}

/// Resolves the initial Kafka offset for direct-partition consumption,
/// detecting retention gaps when an explicit `restore_next_offset` is supplied.
pub fn resolve_start_offset(
    earliest_available: i64,
    latest_available: i64,
    restore_next_offset: Option<i64>,
    offset_reset: &str,
) -> std::result::Result<i64, KafkaReliableError> {
    if let Some(requested) = restore_next_offset {
        if requested < earliest_available {
            return Err(KafkaReliableError::RetentionGap {
                requested_offset: requested,
                earliest_available,
            });
        }
        return Ok(requested);
    }
    if offset_reset.eq_ignore_ascii_case("earliest") {
        Ok(earliest_available)
    } else {
        Ok(latest_available)
    }
}

/// Raw Kafka message input before payload decoding.
#[derive(Debug, Clone)]
pub struct KafkaRawRecord {
    pub offset: i64,
    pub payload: Option<Vec<u8>>,
}

/// Kafka message envelope with metadata/position built before decode.
///
/// A multirecord JSON array payload expands into `records`, all sharing a
/// single commit boundary (`next_offset = offset + 1`) so partial commits are
/// impossible.
#[derive(Debug, Clone)]
pub struct KafkaPreparedMessage {
    pub offset: i64,
    pub next_offset: i64,
    pub position: rekuiper_core::contracts::SourcePosition,
    pub is_tombstone: bool,
    topic: Option<String>,
    payload: Option<Vec<u8>>,
}

impl KafkaPreparedMessage {
    /// Decodes the payload into `StreamRecord`s after metadata/position has been
    /// emitted, enforcing `tombstone_policy`.
    pub fn decode(
        &self,
        tombstone_policy: KafkaTombstonePolicy,
    ) -> std::result::Result<Vec<StreamRecord>, KafkaReliableError> {
        let Some(bytes) = self.payload.as_deref().filter(|b| !b.is_empty()) else {
            return match tombstone_policy {
                KafkaTombstonePolicy::Fail => Err(KafkaReliableError::Tombstone {
                    offset: self.offset,
                }),
                KafkaTombstonePolicy::SkipAndAdvance => Ok(Vec::new()),
            };
        };
        decode_kafka_payload(bytes, self.topic.as_deref()).map_err(|e| {
            KafkaReliableError::DecodeFailed {
                offset: self.offset,
                message: e.to_string(),
            }
        })
    }
}

/// Tracks direct-partition replay state and advances `committed_next_offset`
/// only when an entire message boundary is accepted/committed.
#[derive(Debug, Clone)]
pub struct KafkaReliablePartitionReader {
    partition: rekuiper_core::contracts::SourcePartition,
    generation: rekuiper_core::contracts::Generation,
    codec: KafkaPositionCodec,
    topic: Option<String>,
    tombstone_policy: KafkaTombstonePolicy,
    next_fetch_offset: i64,
    committed_next_offset: i64,
}

impl KafkaReliablePartitionReader {
    pub fn new(
        partition: rekuiper_core::contracts::SourcePartition,
        generation: rekuiper_core::contracts::Generation,
        topic: Option<String>,
        start_offset: i64,
        tombstone_policy: KafkaTombstonePolicy,
    ) -> Self {
        Self {
            partition,
            generation,
            codec: KafkaPositionCodec::new(),
            topic,
            tombstone_policy,
            next_fetch_offset: start_offset,
            committed_next_offset: start_offset,
        }
    }

    pub fn committed_next_offset(&self) -> i64 {
        self.committed_next_offset
    }

    pub fn committed_position(&self) -> rekuiper_core::contracts::SourcePosition {
        self.codec.make_position(
            self.partition.clone(),
            self.generation,
            self.committed_next_offset,
        )
    }

    /// Emits metadata/position BEFORE decode. Does not advance `committed_next_offset`.
    pub fn prepare(&mut self, raw: KafkaRawRecord) -> KafkaPreparedMessage {
        let next_offset = raw.offset + 1;
        if next_offset > self.next_fetch_offset {
            self.next_fetch_offset = next_offset;
        }
        let is_tombstone = raw.payload.as_ref().is_none_or(|b| b.is_empty());
        let position =
            self.codec
                .make_position(self.partition.clone(), self.generation, next_offset);
        KafkaPreparedMessage {
            offset: raw.offset,
            next_offset,
            position,
            is_tombstone,
            topic: self.topic.clone(),
            payload: raw.payload,
        }
    }

    pub fn tombstone_policy(&self) -> KafkaTombstonePolicy {
        self.tombstone_policy
    }

    /// Advances the durable boundary only after all records expanded from
    /// `msg` have been accepted/committed together.
    pub fn commit_message(
        &mut self,
        msg: &KafkaPreparedMessage,
    ) -> std::result::Result<rekuiper_core::contracts::SourcePosition, KafkaReliableError> {
        if msg.offset != self.committed_next_offset {
            return Err(KafkaReliableError::OutOfOrderCommit {
                expected_offset: self.committed_next_offset,
                actual_offset: msg.offset,
            });
        }
        self.committed_next_offset = msg.next_offset;
        Ok(msg.position.clone())
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
