use anyhow::Result;
use async_trait::async_trait;
use parking_lot::Mutex;
use rekuiper_connectors::rabbitmq::{
    RabbitMqAckError, RabbitMqAckMode, RabbitMqBrokerChannel, RabbitMqConfig, RabbitMqPoisonPolicy,
    RabbitMqReliableAckCoordinator,
};
use std::collections::VecDeque;

#[derive(Default)]
struct FakeBroker {
    ready: Mutex<VecDeque<(u64, Vec<u8>)>>,
    acked: Mutex<Vec<u64>>,
    nacked: Mutex<Vec<(u64, bool)>>,
    next_tag: Mutex<u64>,
}

impl FakeBroker {
    fn publish(&self, payload: Vec<u8>) {
        self.ready.lock().push_back((0, payload));
    }

    fn deliver_next(&self) -> Option<(u64, Vec<u8>)> {
        let mut ready = self.ready.lock();
        let (_, payload) = ready.pop_front()?;
        let mut next = self.next_tag.lock();
        *next += 1;
        let tag = *next;
        Some((tag, payload))
    }
}

#[async_trait]
impl RabbitMqBrokerChannel for FakeBroker {
    async fn ack(&self, delivery_tag: u64) -> Result<()> {
        self.acked.lock().push(delivery_tag);
        Ok(())
    }

    async fn nack(&self, delivery_tag: u64, requeue: bool) -> Result<()> {
        self.nacked.lock().push((delivery_tag, requeue));
        Ok(())
    }
}

#[tokio::test]
async fn p03_t16_does_not_ack_before_durable_acceptance_and_fences_stale_epoch_on_reconnect() {
    let broker = FakeBroker::default();
    broker.publish(br#"{"v":1}"#.to_vec());
    broker.publish(br#"{"v":2}"#.to_vec());

    let mut coord = RabbitMqReliableAckCoordinator::new(
        RabbitMqAckMode::DurableOutbox,
        RabbitMqPoisonPolicy::DeadLetter,
        2,
    );

    let (tag1, _) = broker.deliver_next().unwrap();
    let tok1 = coord.register_delivery(tag1).unwrap();
    let (tag2, _) = broker.deliver_next().unwrap();
    let _tok2 = coord.register_delivery(tag2).unwrap();

    // Prefetch bound enforced.
    assert!(matches!(
        coord.register_delivery(99),
        Err(RabbitMqAckError::PrefetchLimitExceeded {
            in_flight: 2,
            limit: 2
        })
    ));

    // Must refuse ack before durable acceptance.
    let early_err = coord
        .ack_after_durable_acceptance(&broker, tok1, false)
        .await
        .expect_err("cannot ack before durable acceptance");
    assert!(matches!(
        early_err,
        RabbitMqAckError::DurableDispositionRequired(_)
    ));
    assert!(broker.acked.lock().is_empty());

    // Simulate channel loss / process restart before ack: epoch advances, stale tokens rejected.
    let invalidated = coord.on_channel_lost();
    assert_eq!(invalidated, 2);
    assert_eq!(coord.current_epoch(), 2);

    let stale_err = coord
        .ack_after_durable_acceptance(&broker, tok1, true)
        .await
        .expect_err("stale delivery tag from previous epoch must not ack on new channel");
    assert!(matches!(
        stale_err,
        RabbitMqAckError::StaleChannelEpoch { .. }
    ));
    assert!(broker.acked.lock().is_empty());

    // New delivery on epoch 2 succeeds once durable acceptance is confirmed.
    let tok2_re = coord.register_delivery(tag2).unwrap();
    coord
        .ack_after_durable_acceptance(&broker, tok2_re, true)
        .await
        .unwrap();
    assert_eq!(*broker.acked.lock(), vec![tag2]);
}

#[tokio::test]
async fn p03_t16_poison_message_never_acks_when_dlq_fails_and_preserves_legacy_defaults() {
    let broker = FakeBroker::default();
    let mut coord = RabbitMqReliableAckCoordinator::new(
        RabbitMqAckMode::ConfirmedDelivery,
        RabbitMqPoisonPolicy::DeadLetter,
        10,
    );

    let tok_fail = coord.register_delivery(1).unwrap();
    let err = coord
        .handle_poison(&broker, tok_fail, false)
        .await
        .expect_err("DLQ failure must prevent false completion");
    assert!(matches!(
        err,
        RabbitMqAckError::DurableDispositionRequired(_)
    ));
    assert!(broker.acked.lock().is_empty());
    assert_eq!(*broker.nacked.lock(), vec![(1, true)]);

    let tok_ok = coord.register_delivery(2).unwrap();
    coord.handle_poison(&broker, tok_ok, true).await.unwrap();
    assert_eq!(*broker.acked.lock(), vec![2]);

    // Legacy config deserialization preserves LegacyAdmission + RejectDrop defaults.
    let legacy_cfg: RabbitMqConfig = serde_json::from_str("{}").unwrap();
    assert_eq!(legacy_cfg.ack_mode, RabbitMqAckMode::LegacyAdmission);
    assert_eq!(legacy_cfg.poison_policy, RabbitMqPoisonPolicy::RejectDrop);
}

#[tokio::test]
async fn p03_t16_live_source_rejects_unwired_reliable_ack_modes() {
    use rekuiper_connectors::rabbitmq::RabbitMqSource;
    use rekuiper_core::StreamBus;

    let bus = StreamBus::new();
    let tx = bus.get_or_create("q1");

    for mode in [
        RabbitMqAckMode::DurableOutbox,
        RabbitMqAckMode::ConfirmedDelivery,
    ] {
        let cfg = RabbitMqConfig {
            ack_mode: mode,
            queue: "q1".to_owned(),
            ..Default::default()
        };
        assert!(
            cfg.validate_source_ack_mode().is_err(),
            "mode {mode:?} must be rejected until live consumer honors it"
        );
        assert!(RabbitMqSource::try_new(cfg.clone(), tx.clone()).is_err());

        // Even if constructed with `new` and spawned, it exits immediately without connecting.
        let (_cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        let handle = RabbitMqSource::new(cfg, tx.clone()).spawn(cancel_rx);
        handle.await.unwrap();
    }

    let legacy_cfg = RabbitMqConfig::default();
    assert!(RabbitMqSource::try_new(legacy_cfg, tx).is_ok());
}
