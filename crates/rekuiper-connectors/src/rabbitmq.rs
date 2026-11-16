use anyhow::{Context, Result};
use async_trait::async_trait;
use futures::StreamExt;
use lapin::{options::*, types::FieldTable, BasicProperties, Connection, ConnectionProperties};
use rekuiper_core::model::StreamRecord;
use rekuiper_core::StreamSender;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::decode_payload_into;
use crate::secrets::SecretResolver;
use crate::{PayloadFormat, Sink};

/// Acknowledgement lifecycle mode for RabbitMQ sources.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RabbitMqAckMode {
    /// Acknowledge immediately after local stream bus admission (legacy default).
    #[default]
    LegacyAdmission,
    /// Acknowledge only after durable state/outbox acceptance is recorded.
    DurableOutbox,
    /// Acknowledge only after downstream final delivery is confirmed.
    ConfirmedDelivery,
}

/// Disposition policy for poison (undecodable) messages in reliable RabbitMQ mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RabbitMqPoisonPolicy {
    /// Reject without requeue (legacy default).
    #[default]
    RejectDrop,
    /// Persist/forward to DLQ first; only ack/reject after durable DLQ disposition,
    /// otherwise nack with `requeue: true`.
    DeadLetter,
    /// Nack with `requeue: true` so the broker retains the message.
    NackRequeue,
}

/// RabbitMQ AMQP 0-9-1 connection and routing configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RabbitMqConfig {
    /// AMQP broker URL, e.g. `amqp://guest:guest@127.0.0.1:5672/%2f` or `amqps://...`.
    #[serde(default = "default_rabbitmq_server", alias = "url")]
    pub server: String,
    /// Exchange name to bind to or publish to. Defaults to empty string (default direct exchange).
    #[serde(default)]
    pub exchange: String,
    /// Routing key for publishing or queue binding.
    #[serde(default, alias = "routingKey")]
    pub routing_key: String,
    /// Queue name for consuming or declaring.
    #[serde(default)]
    pub queue: String,
    /// Whether the queue/exchange is durable across broker restarts.
    #[serde(default = "default_true")]
    pub durable: bool,
    /// Whether the queue/exchange is auto-deleted when no longer in use.
    #[serde(default)]
    pub auto_delete: bool,
    /// Exclusive queue flag (used by only one connection).
    #[serde(default)]
    pub exclusive: bool,
    /// Consumer prefetch count (QoS).
    #[serde(default = "default_prefetch")]
    pub prefetch_count: u16,
    /// Whether messages are automatically acknowledged upon receipt by the broker (AMQP no_ack).
    /// Defaults to false (manual acknowledgments after pipeline admission for at-least-once reliability).
    #[serde(default, alias = "autoAck", alias = "no_ack")]
    pub auto_ack: bool,
    /// Optional username (supports dynamic secrets like `env://` or `vault://`).
    #[serde(default)]
    pub username: Option<String>,
    /// Optional password (supports dynamic secrets like `env://` or `vault://`).
    #[serde(default)]
    pub password: Option<String>,
    /// Payload decoding format (stream FORMAT).
    #[serde(skip)]
    pub format: PayloadFormat,
    /// Explicit acknowledgement lifecycle mode (defaults to legacy admission).
    #[serde(default, alias = "ackMode")]
    pub ack_mode: RabbitMqAckMode,
    /// Poison-message disposition policy (defaults to legacy reject-drop).
    #[serde(default, alias = "poisonPolicy")]
    pub poison_policy: RabbitMqPoisonPolicy,
}

fn default_rabbitmq_server() -> String {
    "amqp://guest:guest@127.0.0.1:5672/%2f".to_string()
}

fn default_true() -> bool {
    true
}

fn default_prefetch() -> u16 {
    100
}

impl Default for RabbitMqConfig {
    fn default() -> Self {
        Self {
            server: default_rabbitmq_server(),
            exchange: String::new(),
            routing_key: String::new(),
            queue: String::new(),
            durable: true,
            auto_delete: false,
            exclusive: false,
            prefetch_count: 100,
            auto_ack: false,
            username: None,
            password: None,
            format: PayloadFormat::Json,
            ack_mode: RabbitMqAckMode::LegacyAdmission,
            poison_policy: RabbitMqPoisonPolicy::RejectDrop,
        }
    }
}

impl RabbitMqConfig {
    /// Dynamically resolve any secrets (e.g. `vault://` or `env://`) in server URL, username, or password.
    pub async fn resolve_secrets(&mut self, resolver: &SecretResolver) -> Result<()> {
        self.server = resolver.resolve(&self.server).await?;
        if let Some(u) = &self.username {
            self.username = Some(resolver.resolve(u).await?);
        }
        if let Some(p) = &self.password {
            self.password = Some(resolver.resolve(p).await?);
        }
        if let (Some(u), Some(p)) = (&self.username, &self.password) {
            if !self.server.contains('@') {
                if let Some(rest) = self.server.strip_prefix("amqp://") {
                    self.server = format!("amqp://{}:{}@{}", u, p, rest);
                } else if let Some(rest) = self.server.strip_prefix("amqps://") {
                    self.server = format!("amqps://{}:{}@{}", u, p, rest);
                }
            }
        }
        Ok(())
    }

    /// Validates whether the live `RabbitMqSource` consumer supports `self.ack_mode`.
    ///
    /// Until the live AMQP consumer wires `RabbitMqReliableAckCoordinator` to
    /// durable outbox acceptance or downstream delivery confirmation, only
    /// `LegacyAdmission` is permitted.
    pub fn validate_source_ack_mode(&self) -> Result<()> {
        match self.ack_mode {
            RabbitMqAckMode::LegacyAdmission => Ok(()),
            RabbitMqAckMode::DurableOutbox | RabbitMqAckMode::ConfirmedDelivery => {
                anyhow::bail!(
                    "RabbitMQ source ackMode {:?} is not supported by the live consumer yet; \
                     refusing to acknowledge on local stream bus admission",
                    self.ack_mode
                )
            }
        }
    }
}

/// RabbitMQ AMQP 0-9-1 Streaming Source.
pub struct RabbitMqSource {
    pub config: RabbitMqConfig,
    pub tx: StreamSender,
}

impl RabbitMqSource {
    pub fn new(config: RabbitMqConfig, tx: StreamSender) -> Self {
        Self { config, tx }
    }

    /// Constructs a [`RabbitMqSource`] after validating that `config.ack_mode`
    /// is supported by the live consumer.
    pub fn try_new(config: RabbitMqConfig, tx: StreamSender) -> Result<Self> {
        config.validate_source_ack_mode()?;
        Ok(Self { config, tx })
    }

    /// Spawns background consumer task receiving AMQP deliveries and pushing to the stream pipeline.
    pub fn spawn(
        self,
        mut cancel_rx: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            if let Err(err) = self.config.validate_source_ack_mode() {
                tracing::error!("RabbitMQ source rejected: {err}");
                return;
            }
            let mut cfg = self.config.clone();
            let _ = cfg.resolve_secrets(&SecretResolver::from_env()).await;

            let conn = match Connection::connect(&cfg.server, ConnectionProperties::default()).await
            {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!("RabbitMQ source failed to connect to {}: {}", cfg.server, e);
                    return;
                }
            };

            let channel = match conn.create_channel().await {
                Ok(ch) => ch,
                Err(e) => {
                    tracing::warn!("RabbitMQ source failed to create channel: {}", e);
                    return;
                }
            };

            // Set QoS prefetch
            let _ = channel
                .basic_qos(self.config.prefetch_count, BasicQosOptions::default())
                .await;

            // Declare queue if configured
            let queue_name = if !self.config.queue.is_empty() {
                let opts = QueueDeclareOptions {
                    durable: self.config.durable,
                    auto_delete: self.config.auto_delete,
                    exclusive: self.config.exclusive,
                    nowait: false,
                    passive: false,
                };
                match channel
                    .queue_declare(&self.config.queue, opts, FieldTable::default())
                    .await
                {
                    Ok(q) => q.name().to_string(),
                    Err(e) => {
                        tracing::warn!(
                            "RabbitMQ failed to declare queue {}: {}",
                            self.config.queue,
                            e
                        );
                        return;
                    }
                }
            } else {
                String::new()
            };

            // Bind queue to exchange if exchange is specified
            if !self.config.exchange.is_empty() && !queue_name.is_empty() {
                let _ = channel
                    .queue_bind(
                        &queue_name,
                        &self.config.exchange,
                        &self.config.routing_key,
                        QueueBindOptions::default(),
                        FieldTable::default(),
                    )
                    .await;
            }

            let auto_ack = self.config.auto_ack;
            let mut consumer = match channel
                .basic_consume(
                    &queue_name,
                    "rekuiper_consumer",
                    BasicConsumeOptions {
                        no_ack: auto_ack,
                        ..Default::default()
                    },
                    FieldTable::default(),
                )
                .await
            {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!("RabbitMQ basic_consume failed: {}", e);
                    return;
                }
            };

            loop {
                tokio::select! {
                    delivery_opt = consumer.next() => {
                        match delivery_opt {
                            Some(Ok(delivery)) => {
                                let payload = &delivery.data;
                                let mut records = Vec::new();
                                match decode_payload_into(&self.config.format, payload, None, &mut records) {
                                    Ok(_) => {
                                        let mut send_ok = true;
                                        for record in records {
                                            if self.tx.send(record).await.is_err() {
                                                send_ok = false;
                                                break;
                                            }
                                        }
                                        if !send_ok {
                                            if !auto_ack {
                                                let _ = delivery
                                                    .nack(BasicNackOptions {
                                                        requeue: true,
                                                        ..Default::default()
                                                    })
                                                    .await;
                                            }
                                            return;
                                        }
                                        if !auto_ack {
                                            if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
                                                tracing::warn!("RabbitMQ ack failed: {}", e);
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        tracing::warn!("RabbitMQ payload decoding error: {}", e);
                                        if !auto_ack {
                                            let _ = delivery
                                                .reject(BasicRejectOptions { requeue: false })
                                                .await;
                                        }
                                    }
                                }
                            }
                            Some(Err(e)) => {
                                tracing::warn!("RabbitMQ delivery error: {}", e);
                                break;
                            }
                            None => break,
                        }
                    }
                    exit = cancel_rx.changed() => {
                        if exit.is_err() || *cancel_rx.borrow() {
                            return;
                        }
                    }
                }
            }
        })
    }
}

/// RabbitMQ AMQP 0-9-1 Streaming Sink.
pub struct RabbitMqSink {
    pub config: RabbitMqConfig,
    channel: Arc<Mutex<Option<lapin::Channel>>>,
}

impl RabbitMqSink {
    pub fn new(config: RabbitMqConfig) -> Self {
        Self {
            config,
            channel: Arc::new(Mutex::new(None)),
        }
    }

    async fn get_or_connect(&self) -> Result<lapin::Channel> {
        let mut lock = self.channel.lock().await;
        if let Some(ch) = &*lock {
            if ch.status().connected() {
                return Ok(ch.clone());
            }
        }

        let mut cfg = self.config.clone();
        let _ = cfg.resolve_secrets(&SecretResolver::from_env()).await;

        let conn = Connection::connect(&cfg.server, ConnectionProperties::default())
            .await
            .with_context(|| format!("Failed to connect to RabbitMQ server at {}", cfg.server))?;

        let channel = conn
            .create_channel()
            .await
            .context("Failed to create RabbitMQ channel")?;

        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .context("Failed to enable publisher confirms on RabbitMQ channel")?;

        if !cfg.queue.is_empty() {
            let opts = QueueDeclareOptions {
                durable: cfg.durable,
                auto_delete: cfg.auto_delete,
                exclusive: cfg.exclusive,
                nowait: false,
                passive: false,
            };
            let _ = channel
                .queue_declare(&cfg.queue, opts, FieldTable::default())
                .await;
        }

        *lock = Some(channel.clone());
        Ok(channel)
    }

    pub async fn send_raw(&self, payload: &[u8]) -> Result<()> {
        let channel = self.get_or_connect().await?;

        let routing_key = if self.config.routing_key.is_empty() && !self.config.queue.is_empty() {
            &self.config.queue
        } else {
            &self.config.routing_key
        };

        let confirmation = channel
            .basic_publish(
                &self.config.exchange,
                routing_key,
                BasicPublishOptions::default(),
                payload,
                BasicProperties::default(),
            )
            .await
            .context("Failed to publish message to RabbitMQ")?
            .await
            .context("Failed to confirm RabbitMQ publish")?;

        if matches!(
            confirmation,
            lapin::publisher_confirm::Confirmation::Nack(_)
        ) {
            anyhow::bail!("RabbitMQ broker negatively acknowledged (nacked) message publish");
        }

        Ok(())
    }
}

#[async_trait]
impl Sink for RabbitMqSink {
    async fn send(&self, record: &StreamRecord) -> Result<()> {
        let mut map = std::collections::BTreeMap::new();
        for (k, v) in &record.data {
            if k == crate::META_KEY || k.starts_with("__") {
                continue;
            }
            map.insert(k.clone(), v.clone());
        }
        let payload = serde_json::to_vec(&map)?;
        self.send_raw(&payload).await
    }
}

/// Epoch-fenced AMQP delivery token preventing stale delivery tags from being
/// acknowledged after channel loss or reconnect.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RabbitMqDeliveryToken {
    pub channel_epoch: u64,
    pub delivery_tag: u64,
}

/// Errors returned by [`RabbitMqReliableAckCoordinator`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RabbitMqAckError {
    PrefetchLimitExceeded {
        in_flight: usize,
        limit: usize,
    },
    StaleChannelEpoch {
        token: RabbitMqDeliveryToken,
        current_epoch: u64,
    },
    UnknownDelivery(RabbitMqDeliveryToken),
    DurableDispositionRequired(String),
    ChannelFailed(String),
}

impl std::fmt::Display for RabbitMqAckError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PrefetchLimitExceeded { in_flight, limit } => {
                write!(
                    f,
                    "RabbitMQ prefetch limit exceeded: {in_flight} >= {limit}"
                )
            }
            Self::StaleChannelEpoch {
                token,
                current_epoch,
            } => write!(
                f,
                "stale RabbitMQ delivery tag {} from epoch {} (current epoch {})",
                token.delivery_tag, token.channel_epoch, current_epoch
            ),
            Self::UnknownDelivery(token) => write!(
                f,
                "unknown RabbitMQ delivery tag {} (epoch {})",
                token.delivery_tag, token.channel_epoch
            ),
            Self::DurableDispositionRequired(msg) => {
                write!(f, "durable disposition failed before ack: {msg}")
            }
            Self::ChannelFailed(msg) => write!(f, "RabbitMQ channel operation failed: {msg}"),
        }
    }
}

impl std::error::Error for RabbitMqAckError {}

/// Broker channel boundary for acknowledging or nacking RabbitMQ deliveries.
#[async_trait]
pub trait RabbitMqBrokerChannel: Send + Sync {
    async fn ack(&self, delivery_tag: u64) -> Result<()>;
    async fn nack(&self, delivery_tag: u64, requeue: bool) -> Result<()>;
}

/// Coordinates reliable RabbitMQ acknowledgements against durable state/outbox
/// acceptance, confirmed delivery, bounded prefetch, poison-message DLQ
/// disposition, and channel-epoch fencing across reconnects.
#[derive(Debug)]
pub struct RabbitMqReliableAckCoordinator {
    ack_mode: RabbitMqAckMode,
    poison_policy: RabbitMqPoisonPolicy,
    prefetch_limit: usize,
    current_epoch: u64,
    pending: BTreeMap<RabbitMqDeliveryToken, bool>,
    order: VecDeque<RabbitMqDeliveryToken>,
}

impl RabbitMqReliableAckCoordinator {
    pub fn new(
        ack_mode: RabbitMqAckMode,
        poison_policy: RabbitMqPoisonPolicy,
        prefetch_count: u16,
    ) -> Self {
        Self {
            ack_mode,
            poison_policy,
            prefetch_limit: usize::from(prefetch_count.max(1)),
            current_epoch: 1,
            pending: BTreeMap::new(),
            order: VecDeque::new(),
        }
    }

    pub fn ack_mode(&self) -> RabbitMqAckMode {
        self.ack_mode
    }

    pub fn current_epoch(&self) -> u64 {
        self.current_epoch
    }

    pub fn in_flight_count(&self) -> usize {
        self.pending.len()
    }

    pub fn prefetch_limit(&self) -> usize {
        self.prefetch_limit
    }

    /// Registers a newly received delivery tag under the active channel epoch.
    pub fn register_delivery(
        &mut self,
        delivery_tag: u64,
    ) -> std::result::Result<RabbitMqDeliveryToken, RabbitMqAckError> {
        if self.pending.len() >= self.prefetch_limit {
            return Err(RabbitMqAckError::PrefetchLimitExceeded {
                in_flight: self.pending.len(),
                limit: self.prefetch_limit,
            });
        }
        let token = RabbitMqDeliveryToken {
            channel_epoch: self.current_epoch,
            delivery_tag,
        };
        self.pending.insert(token, false);
        self.order.push_back(token);
        Ok(token)
    }

    /// Advances the channel epoch on disconnect/reconnect and invalidates all
    /// pending delivery tags from previous epochs so they can never be acked on
    /// the new channel. Returns the number of invalidated deliveries.
    pub fn on_channel_lost(&mut self) -> usize {
        let dropped = self.pending.len();
        self.pending.clear();
        self.order.clear();
        self.current_epoch = self.current_epoch.saturating_add(1);
        dropped
    }

    /// Acknowledges a delivery only after verifying its channel epoch matches
    /// the active channel and (for `DurableOutbox` / `ConfirmedDelivery`) that
    /// durable acceptance or confirmed delivery was achieved.
    pub async fn ack_after_durable_acceptance<C: RabbitMqBrokerChannel + ?Sized>(
        &mut self,
        channel: &C,
        token: RabbitMqDeliveryToken,
        durable_or_confirmed: bool,
    ) -> std::result::Result<(), RabbitMqAckError> {
        self.validate_token(token)?;
        if self.ack_mode != RabbitMqAckMode::LegacyAdmission && !durable_or_confirmed {
            return Err(RabbitMqAckError::DurableDispositionRequired(
                "cannot ack before durable acceptance or confirmed delivery".to_owned(),
            ));
        }
        channel
            .ack(token.delivery_tag)
            .await
            .map_err(|e| RabbitMqAckError::ChannelFailed(e.to_string()))?;
        self.remove_token(token);
        Ok(())
    }

    /// Handles a poison (undecodable) message: in `DeadLetter` mode, never
    /// acknowledges or drops before durable DLQ persistence succeeds; if DLQ
    /// fails, nacks with `requeue: true` and returns an error so the caller
    /// does not report false completion.
    pub async fn handle_poison<C: RabbitMqBrokerChannel + ?Sized>(
        &mut self,
        channel: &C,
        token: RabbitMqDeliveryToken,
        dlq_durable_ok: bool,
    ) -> std::result::Result<(), RabbitMqAckError> {
        self.validate_token(token)?;
        match self.poison_policy {
            RabbitMqPoisonPolicy::RejectDrop => {
                channel
                    .nack(token.delivery_tag, false)
                    .await
                    .map_err(|e| RabbitMqAckError::ChannelFailed(e.to_string()))?;
                self.remove_token(token);
                Ok(())
            }
            RabbitMqPoisonPolicy::NackRequeue => {
                channel
                    .nack(token.delivery_tag, true)
                    .await
                    .map_err(|e| RabbitMqAckError::ChannelFailed(e.to_string()))?;
                self.remove_token(token);
                Ok(())
            }
            RabbitMqPoisonPolicy::DeadLetter => {
                if dlq_durable_ok {
                    channel
                        .ack(token.delivery_tag)
                        .await
                        .map_err(|e| RabbitMqAckError::ChannelFailed(e.to_string()))?;
                    self.remove_token(token);
                    Ok(())
                } else {
                    channel
                        .nack(token.delivery_tag, true)
                        .await
                        .map_err(|e| RabbitMqAckError::ChannelFailed(e.to_string()))?;
                    self.remove_token(token);
                    Err(RabbitMqAckError::DurableDispositionRequired(
                        "DLQ persistence failed; poison message requeued instead of acked"
                            .to_owned(),
                    ))
                }
            }
        }
    }

    /// Nacks a pending delivery (for example on downstream failure or shutdown).
    pub async fn nack_delivery<C: RabbitMqBrokerChannel + ?Sized>(
        &mut self,
        channel: &C,
        token: RabbitMqDeliveryToken,
        requeue: bool,
    ) -> std::result::Result<(), RabbitMqAckError> {
        self.validate_token(token)?;
        channel
            .nack(token.delivery_tag, requeue)
            .await
            .map_err(|e| RabbitMqAckError::ChannelFailed(e.to_string()))?;
        self.remove_token(token);
        Ok(())
    }

    fn validate_token(
        &self,
        token: RabbitMqDeliveryToken,
    ) -> std::result::Result<(), RabbitMqAckError> {
        if token.channel_epoch != self.current_epoch {
            return Err(RabbitMqAckError::StaleChannelEpoch {
                token,
                current_epoch: self.current_epoch,
            });
        }
        if !self.pending.contains_key(&token) {
            return Err(RabbitMqAckError::UnknownDelivery(token));
        }
        Ok(())
    }

    fn remove_token(&mut self, token: RabbitMqDeliveryToken) {
        self.pending.remove(&token);
        self.order.retain(|t| *t != token);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rabbitmq_config_defaults() {
        let json = r#"{"server": "amqp://user:pass@127.0.0.1:5672", "exchange": "events", "routingKey": "sensor.temp"}"#;
        let cfg: RabbitMqConfig = serde_json::from_str(json).expect("deserialize");
        assert_eq!(cfg.server, "amqp://user:pass@127.0.0.1:5672");
        assert_eq!(cfg.exchange, "events");
        assert_eq!(cfg.routing_key, "sensor.temp");
        assert!(cfg.durable);
        assert_eq!(cfg.prefetch_count, 100);
        assert!(!cfg.auto_ack);

        let json_auto_ack = r#"{"server": "amqp://127.0.0.1:5672", "autoAck": true}"#;
        let cfg_auto: RabbitMqConfig = serde_json::from_str(json_auto_ack).expect("deserialize");
        assert!(cfg_auto.auto_ack);
    }

    #[tokio::test]
    async fn test_rabbitmq_resolve_secrets_injects_credentials() {
        let mut cfg = RabbitMqConfig {
            server: "amqp://127.0.0.1:5672/vhost".to_string(),
            username: Some("myuser".to_string()),
            password: Some("mypass".to_string()),
            ..Default::default()
        };
        cfg.resolve_secrets(&SecretResolver::from_env())
            .await
            .unwrap();
        assert_eq!(cfg.server, "amqp://myuser:mypass@127.0.0.1:5672/vhost");

        let mut cfg_ssl = RabbitMqConfig {
            server: "amqps://rabbit.cloud.io:5671/vhost".to_string(),
            username: Some("u".to_string()),
            password: Some("p".to_string()),
            ..Default::default()
        };
        cfg_ssl
            .resolve_secrets(&SecretResolver::from_env())
            .await
            .unwrap();
        assert_eq!(cfg_ssl.server, "amqps://u:p@rabbit.cloud.io:5671/vhost");
    }

    #[tokio::test]
    async fn test_rabbitmq_live_cloudamqp_sink_and_source() {
        use rekuiper_core::StreamBus;
        use std::collections::HashMap;

        let Ok(server) = std::env::var("TEST_RABBITMQ_URL") else {
            // Skip live broker test if TEST_RABBITMQ_URL is not configured
            return;
        };

        let queue_id = format!(
            "kuiper_cloud_live_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );
        let cfg = RabbitMqConfig {
            server,
            queue: queue_id.clone(),
            routing_key: queue_id.clone(),
            durable: false,
            auto_delete: true,
            ..Default::default()
        };

        let bus = StreamBus::new();
        let mut rx = bus.subscribe("cloud_stream");
        let tx = bus.get_or_create("cloud_stream");
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        let source = RabbitMqSource::new(cfg.clone(), tx);
        let handle = source.spawn(cancel_rx);

        // Allow consumer to attach
        tokio::time::sleep(tokio::time::Duration::from_millis(600)).await;

        let sink = RabbitMqSink::new(cfg.clone());
        let mut data = HashMap::new();
        data.insert("sensor".to_string(), serde_json::json!("roundtrip_probe"));
        data.insert("temperature".to_string(), serde_json::json!(88.5));
        let record = StreamRecord::new(data);

        sink.send(&record).await.expect("publish");

        // Receive from source via StreamBus
        let received = tokio::time::timeout(tokio::time::Duration::from_secs(6), rx.recv())
            .await
            .expect("timeout waiting for RabbitMQ delivery")
            .expect("channel closed");

        assert_eq!(
            received.data.get("sensor").and_then(|v| v.as_str()),
            Some("roundtrip_probe")
        );
        assert_eq!(
            received.data.get("temperature").and_then(|v| v.as_f64()),
            Some(88.5)
        );
        let _ = cancel_tx.send(true);
        let _ = handle.await;
    }
}
