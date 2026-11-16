use indexmap::IndexMap;
use rekuiper_core::contracts::{CheckpointId, Generation, OrderedEnvelope};
use rekuiper_core::model::StreamRecord;
use rekuiper_core::resources::{lossy_channel, LossyPushError, LossyTryRecvError};
use rekuiper_core::runtime::{BusPredicate, StreamBus, SubscriberFilter};
use serde_json::json;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

fn rec(id: i64) -> StreamRecord {
    let mut data = IndexMap::new();
    data.insert("id".to_string(), json!(id));
    StreamRecord::new(data)
}

#[test]
fn lossy_queue_evicts_oldest_preserves_batch_suffix_and_rejects_control_envelopes() {
    let (tx, mut rx) = lossy_channel::<i32>(2);
    assert_eq!(tx.push(1), Ok(0));
    assert_eq!(tx.push(2), Ok(0));
    assert_eq!(tx.push(3), Ok(1));

    let metrics = rx.metrics();
    assert_eq!(metrics.capacity, 2);
    assert_eq!(metrics.len, 2);
    assert_eq!(metrics.evicted_total, 1);

    assert_eq!(rx.try_recv(), Ok(2));
    assert_eq!(rx.try_recv(), Ok(3));
    assert_eq!(rx.try_recv(), Err(LossyTryRecvError::Empty));

    // Oversized batch preserves newest suffix [30, 40]
    assert_eq!(tx.push(5), Ok(0));
    let evicted = tx.push_batch(vec![10, 20, 30, 40]).unwrap();
    assert_eq!(evicted, 3); // 5, 10, 20 evicted
    assert_eq!(rx.try_recv(), Ok(30));
    assert_eq!(rx.try_recv(), Ok(40));

    // Reliable control messages cannot enter an evictable queue.
    let (ctrl_tx, _ctrl_rx) = lossy_channel::<OrderedEnvelope<u64>>(4);
    let env = OrderedEnvelope::Barrier {
        checkpoint: CheckpointId(1),
        generation: Generation(1),
    };
    assert_eq!(
        ctrl_tx.push_reliable_control(env),
        Err(LossyPushError::ControlMessageRejected)
    );
}

#[tokio::test]
async fn stream_bus_lossy_and_blocking_subscribers_coexist_with_pre_filter_and_concurrency() {
    let bus = StreamBus::new();
    let mut blocking_rx = bus.subscribe_with_capacity("sensors", 8);
    let mut lossy_rx = bus.subscribe_lossy_filtered(
        "sensors",
        SubscriberFilter {
            predicate: BusPredicate::FieldGt {
                field: "id".to_string(),
                threshold: 0.0,
            },
            source_in_counter: Arc::new(AtomicU64::new(0)),
            filtered_counter: Arc::new(AtomicU64::new(0)),
        },
        2,
    );

    // Publish id=1, id=2, id=-99 (filtered before enqueue!), id=3
    bus.publish_async("sensors", rec(1)).await.unwrap();
    bus.publish_async("sensors", rec(2)).await.unwrap();
    bus.publish_async("sensors", rec(-99)).await.unwrap();
    bus.publish_async("sensors", rec(3)).await.unwrap();

    // Blocking subscriber receives all 4 in order.
    let mut blocking_ids = Vec::new();
    for _ in 0..4 {
        let r = blocking_rx.recv().await.unwrap();
        blocking_ids.push(r.data["id"].as_i64().unwrap());
    }
    assert_eq!(blocking_ids, vec![1, 2, -99, 3]);

    // Filtered lossy subscriber never saw -99 and evicted only 1, keeping [2, 3].
    assert_eq!(lossy_rx.metrics().evicted_total, 1);
    assert_eq!(lossy_rx.try_recv().unwrap().data["id"], json!(2));
    assert_eq!(lossy_rx.try_recv().unwrap().data["id"], json!(3));

    // Concurrent publishers + close/wakeup race safety.
    let (tx, mut rx) = lossy_channel::<u64>(4);
    let mut pubs = Vec::new();
    for p in 0..4u64 {
        let t = tx.clone();
        pubs.push(tokio::spawn(async move {
            for i in 0..25 {
                let _ = t.push(p * 100 + i);
            }
        }));
    }
    let consumer = tokio::spawn(async move {
        let mut count = 0usize;
        while rx.recv().await.is_some() {
            count += 1;
        }
        count
    });
    for h in pubs {
        h.await.unwrap();
    }
    tx.close();
    let _received = consumer.await.unwrap();
}
