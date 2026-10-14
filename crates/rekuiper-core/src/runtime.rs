use crate::model::StreamRecord;
use parking_lot::RwLock;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

/// Fast-path atomic predicate extracted from a rule's top-level WHERE clause.
/// Evaluated directly inside `StreamBus` before channel reservation and task wakeup.
#[derive(Debug, Clone)]
pub enum BusPredicate {
    /// `field = literal`
    FieldEq { field: String, value: Value },
    /// `field != literal`
    FieldNeq { field: String, value: Value },
    /// `field > f64`
    FieldGt { field: String, threshold: f64 },
    /// `field >= f64`
    FieldGte { field: String, threshold: f64 },
    /// `field < f64`
    FieldLt { field: String, threshold: f64 },
    /// `field <= f64`
    FieldLte { field: String, threshold: f64 },
    /// `meta(topic) = '...'` or `mqtt(topic) = '...'`
    TopicEq(String),
    /// Conjunction (`A AND B`) of fast predicates
    And(Vec<BusPredicate>),
}

impl BusPredicate {
    #[inline]
    pub fn matches(&self, record: &StreamRecord) -> bool {
        match self {
            BusPredicate::FieldEq { field, value } => {
                record.data.get(field).is_some_and(|v| v == value)
            }
            BusPredicate::FieldNeq { field, value } => {
                record.data.get(field).is_some_and(|v| v != value)
            }
            BusPredicate::FieldGt { field, threshold } => record
                .data
                .get(field)
                .and_then(|v| v.as_f64())
                .is_some_and(|n| n > *threshold),
            BusPredicate::FieldGte { field, threshold } => record
                .data
                .get(field)
                .and_then(|v| v.as_f64())
                .is_some_and(|n| n >= *threshold),
            BusPredicate::FieldLt { field, threshold } => record
                .data
                .get(field)
                .and_then(|v| v.as_f64())
                .is_some_and(|n| n < *threshold),
            BusPredicate::FieldLte { field, threshold } => record
                .data
                .get(field)
                .and_then(|v| v.as_f64())
                .is_some_and(|n| n <= *threshold),
            BusPredicate::TopicEq(expected) => record
                .data
                .get("__meta__")
                .or_else(|| record.data.get("__mqtt__"))
                .and_then(|m| m.get("topic"))
                .and_then(|t| t.as_str())
                .is_some_and(|t| t == expected),
            BusPredicate::And(preds) => preds.iter().all(|p| p.matches(record)),
        }
    }
}

/// Optional pre-filter and counter hooks for a single subscriber on `StreamBus`.
#[derive(Debug, Clone)]
pub struct SubscriberFilter {
    pub predicate: BusPredicate,
    pub source_in_counter: Arc<AtomicU64>,
    pub filtered_counter: Arc<AtomicU64>,
}

impl SubscriberFilter {
    #[inline]
    pub fn check_and_account(&self, record: &StreamRecord) -> bool {
        // Error records must always be delivered to the rule actor so `sendError`
        // and exception counters behave identically to unindexed execution.
        if record.data.contains_key("error") || record.data.contains_key("__error") {
            return true;
        }
        if self.predicate.matches(record) {
            true
        } else {
            self.source_in_counter.fetch_add(1, Ordering::Relaxed);
            self.filtered_counter.fetch_add(1, Ordering::Relaxed);
            false
        }
    }
}

/// Bounded per-subscriber queue depth. Backpressure (awaiting capacity)
/// applies beyond this; records are never silently overwritten.
pub const STREAM_QUEUE_CAPACITY: usize = 4096;

/// Upper bound on records accepted in one HTTP ingestion request.
pub const MAX_HTTP_BATCH_RECORDS: usize = 10_000;

/// Records reserved and committed per admission step of a batch publish.
/// Must not exceed [`STREAM_QUEUE_CAPACITY`]: tokio rejects reservations
/// larger than a channel's capacity.
pub const ADMISSION_CHUNK_RECORDS: usize = 512;

const _: () =
    assert!(ADMISSION_CHUNK_RECORDS > 0 && ADMISSION_CHUNK_RECORDS <= STREAM_QUEUE_CAPACITY);

type RecordTx = mpsc::Sender<StreamRecord>;

/// Send-side handle for one stream topic. Cloneable; never holds the global
/// topic lock across an await (senders are snapshot-cloned first).
#[derive(Clone, Debug)]
pub struct StreamSender {
    bus: StreamBus,
    topic: String,
    topic_ref: Arc<Topic>,
}

/// Receive-side handle: one bounded mpsc queue per subscriber.
pub type StreamReceiver = mpsc::Receiver<StreamRecord>;

/// Publish failure. Every error is returned only when zero records of the
/// request were admitted to any subscriber, so a caller may retry safely.
#[derive(Debug)]
pub enum PublishError {
    /// No subscribers registered on the topic; nothing was enqueued.
    NoSubscribers,
    /// Admission could not complete without waiting (a subscriber queue is
    /// full, or another publisher is mid-admission on the topic); nothing
    /// was enqueued.
    Full,
    /// Every selected subscriber queue closed before any record was
    /// committed; nothing was enqueued.
    Closed,
}

impl std::fmt::Display for PublishError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PublishError::NoSubscribers => write!(f, "no subscribers"),
            PublishError::Full => write!(f, "subscriber queue full"),
            PublishError::Closed => write!(f, "subscriber queue closed"),
        }
    }
}

impl std::error::Error for PublishError {}

#[derive(Debug, Default)]
struct Topic {
    senders: RwLock<Vec<(RecordTx, Option<SubscriberFilter>)>>,
    /// Serialises admission per topic: a request's records are reserved and
    /// committed without interleaving with other publishers of the topic.
    /// Separate from the global topic map lock, which is never held across
    /// an await.
    gate: tokio::sync::Mutex<()>,
}

impl Topic {
    fn snapshot(&self) -> (usize, Vec<(RecordTx, Option<SubscriberFilter>)>) {
        let senders = self.senders.read();
        (
            senders.len(),
            senders
                .iter()
                .filter(|(s, _)| !s.is_closed())
                .cloned()
                .collect(),
        )
    }

    fn purge_closed(&self) {
        let mut senders = self.senders.write();
        senders.retain(|(s, _)| !s.is_closed());
    }
}

#[derive(Clone, Default, Debug)]
pub struct StreamBus {
    topics: Arc<RwLock<HashMap<String, Arc<Topic>>>>,
    /// Records (per subscriber) that were not delivered because the
    /// subscriber closed after its request had started committing.
    undelivered_after_commit: Arc<AtomicU64>,
}

impl StreamBus {
    pub fn new() -> Self {
        Self::default()
    }

    /// Resolve (creating if absent) the send handle for a topic. Cheap:
    /// one short map read; no per-record global locking by callers that
    /// reuse the handle.
    pub fn get_or_create(&self, stream_name: &str) -> StreamSender {
        let topic_ref = self.get_or_create_topic(stream_name);
        StreamSender {
            bus: self.clone(),
            topic: stream_name.to_string(),
            topic_ref,
        }
    }

    fn get_topic(&self, stream_name: &str) -> Option<Arc<Topic>> {
        self.topics.read().get(stream_name).cloned()
    }

    fn get_or_create_topic(&self, stream_name: &str) -> Arc<Topic> {
        let existing = self.topics.read().get(stream_name).cloned();
        match existing {
            Some(t) => t,
            None => {
                let mut map = self.topics.write();
                map.entry(stream_name.to_string())
                    .or_insert_with(|| Arc::new(Topic::default()))
                    .clone()
            }
        }
    }

    /// Subscribe a new independent bounded queue on a topic. Every subscriber
    /// receives every record (fan-out); a slow subscriber applies
    /// backpressure to publishers, never silent loss of another subscriber's
    /// data.
    pub fn subscribe(&self, stream_name: &str) -> StreamReceiver {
        self.subscribe_with_capacity(stream_name, STREAM_QUEUE_CAPACITY)
    }

    /// Subscribe with a caller-specified bounded queue capacity.
    pub fn subscribe_with_capacity(&self, stream_name: &str, capacity: usize) -> StreamReceiver {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        let topic = self.get_or_create_topic(stream_name);
        topic.senders.write().push((tx, None));
        rx
    }

    /// Subscribe with an optional pre-filter evaluated inside `StreamBus`.
    pub fn subscribe_filtered(
        &self,
        stream_name: &str,
        filter: SubscriberFilter,
    ) -> StreamReceiver {
        self.subscribe_filtered_with_capacity(stream_name, filter, STREAM_QUEUE_CAPACITY)
    }

    /// Subscribe with an optional pre-filter and caller-specified bounded queue capacity.
    pub fn subscribe_filtered_with_capacity(
        &self,
        stream_name: &str,
        filter: SubscriberFilter,
        capacity: usize,
    ) -> StreamReceiver {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        let topic = self.get_or_create_topic(stream_name);
        topic.senders.write().push((tx, Some(filter)));
        rx
    }

    /// Records not delivered because their subscriber closed after the
    /// request had already started committing (accept-once lifecycle
    /// losses; see [`StreamBus::publish_batch`]).
    pub fn undelivered_after_commit(&self) -> u64 {
        self.undelivered_after_commit.load(Ordering::Relaxed)
    }

    /// Backpressured publish of one record to every subscriber.
    /// Zero-allocation fast path for single-record `StreamSender::send`.
    pub async fn publish_async(
        &self,
        stream_name: &str,
        record: StreamRecord,
    ) -> Result<usize, PublishError> {
        let Some(topic) = self.get_topic(stream_name) else {
            return Ok(0);
        };
        self.publish_async_topic(&topic, record).await
    }

    async fn publish_async_topic(
        &self,
        topic: &Topic,
        record: StreamRecord,
    ) -> Result<usize, PublishError> {
        let _admission = match topic.gate.try_lock() {
            Ok(guard) => guard,
            Err(_) => topic.gate.lock().await,
        };

        // Ultra-fast path: 1 live subscriber with available queue capacity.
        // Reserves and sends under a single read lock with ZERO heap allocations
        // and ZERO mpsc::Sender clones.
        let mut was_closed = false;
        let mut single_full_tx: Option<RecordTx> = None;
        {
            let senders = topic.senders.read();
            if senders.is_empty() {
                return Ok(0);
            }
            if senders.len() == 1 {
                let (sender, filter) = &senders[0];
                if let Some(f) = filter {
                    if !f.check_and_account(&record) {
                        return Ok(0);
                    }
                }
                match sender.try_reserve() {
                    Ok(permit) => {
                        permit.send(record);
                        return Ok(1);
                    }
                    Err(mpsc::error::TrySendError::Closed(())) => {
                        was_closed = true;
                    }
                    Err(mpsc::error::TrySendError::Full(())) => {
                        // Capture the single sender directly; skip snapshot() Vec allocation
                        // and do not re-evaluate `filter.check_and_account` after waking.
                        single_full_tx = Some(sender.clone());
                    }
                }
            }
        }
        if was_closed {
            topic.purge_closed();
            return Err(PublishError::Closed);
        }
        if let Some(sender) = single_full_tx {
            let permit = match sender.reserve().await {
                Ok(p) => p,
                Err(_) => {
                    topic.purge_closed();
                    return Err(PublishError::Closed);
                }
            };
            permit.send(record);
            return Ok(1);
        }

        // Multi-subscriber (>1) fallback path
        let (registered, selected) = topic.snapshot();
        if registered == 0 {
            return Ok(0);
        }
        if selected.len() < registered {
            topic.purge_closed();
        }
        if selected.is_empty() {
            return Err(PublishError::Closed);
        }

        // Multi-subscriber path: reserve 1 slot on each live subscriber
        let mut permits = Vec::with_capacity(selected.len());
        let mut any_closed = false;
        let mut any_passed_filter = false;
        for (sender, filter) in &selected {
            if let Some(f) = filter {
                if !f.check_and_account(&record) {
                    continue;
                }
            }
            any_passed_filter = true;
            let reserved = match sender.try_reserve() {
                Ok(p) => Some(p),
                Err(mpsc::error::TrySendError::Closed(())) => None,
                Err(mpsc::error::TrySendError::Full(())) => sender.reserve().await.ok(),
            };
            match reserved {
                Some(p) => permits.push(p),
                None => any_closed = true,
            }
        }
        if any_closed {
            topic.purge_closed();
        }
        if permits.is_empty() {
            if any_passed_filter {
                return Err(PublishError::Closed);
            } else {
                return Ok(0);
            }
        }
        let delivered = permits.len();
        let mut permits_iter = permits.into_iter();
        let last = permits_iter.next_back().expect("at least one permit");
        for p in permits_iter {
            p.send(record.clone());
        }
        last.send(record);
        Ok(delivered)
    }

    /// Backpressured publish of a whole validated batch, in order, to every
    /// subscriber selected when admission starts.
    ///
    /// Contract (reserve-then-commit, accept-once):
    /// - The per-topic admission gate is held for the whole request, so the
    ///   request's records stay contiguous and in order on every subscriber.
    /// - Records are admitted in chunks of [`ADMISSION_CHUNK_RECORDS`]:
    ///   capacity for a chunk is reserved on every selected subscriber
    ///   before any record of the chunk becomes visible.
    /// - Before the first chunk commits, a subscriber that closed is dropped
    ///   from the selection; if every selected subscriber closed, the
    ///   request fails with [`PublishError::Closed`] and zero records were
    ///   admitted anywhere (safe to retry).
    /// - Once the first chunk commits, the request is accepted: it returns
    ///   `Ok` even if subscribers close later, and records those subscribers
    ///   miss are counted in [`StreamBus::undelivered_after_commit`]. It
    ///   never turns a mid-request closure into a retryable error that would
    ///   duplicate the committed prefix.
    ///
    /// No registered subscribers is a success with `Ok(0)`. Returns the number
    /// of subscribers that received the complete request.
    pub async fn publish_batch(
        &self,
        stream_name: &str,
        records: Vec<StreamRecord>,
    ) -> Result<usize, PublishError> {
        let Some(topic) = self.get_topic(stream_name) else {
            return Ok(0);
        };
        self.publish_batch_topic(&topic, records).await
    }

    async fn publish_batch_topic(
        &self,
        topic: &Topic,
        records: Vec<StreamRecord>,
    ) -> Result<usize, PublishError> {
        if records.is_empty() {
            return Ok(0);
        }
        let _admission = match topic.gate.try_lock() {
            Ok(guard) => guard,
            Err(_) => topic.gate.lock().await,
        };
        let (registered, mut selected) = topic.snapshot();
        if registered == 0 {
            return Ok(0);
        }
        if selected.len() < registered {
            topic.purge_closed();
        }
        if selected.is_empty() {
            return Err(PublishError::Closed);
        }

        let mut remaining = records.len();
        let mut records = records.into_iter();
        let mut committed = false;
        while remaining > 0 {
            let chunk_size = remaining.min(ADMISSION_CHUNK_RECORDS);
            let chunk_records: Vec<StreamRecord> = records.by_ref().take(chunk_size).collect();

            // Determine matching record indices for each subscriber
            let mut sub_matches: Vec<(usize, Vec<usize>)> = Vec::with_capacity(selected.len());
            for (idx, (_, filter)) in selected.iter().enumerate() {
                let mut matched_indices = Vec::new();
                for (rec_idx, rec) in chunk_records.iter().enumerate() {
                    let keep = match filter {
                        Some(f) => f.check_and_account(rec),
                        None => true,
                    };
                    if keep {
                        matched_indices.push(rec_idx);
                    }
                }
                if !matched_indices.is_empty() {
                    sub_matches.push((idx, matched_indices));
                }
            }

            if sub_matches.is_empty() {
                // All live subscribers filtered out all records in this chunk.
                committed = true;
                remaining -= chunk_size;
                continue;
            }

            let mut subscriber_permits = Vec::with_capacity(sub_matches.len());
            let mut closed = Vec::new();
            for (idx, matched_indices) in sub_matches {
                let sender = &selected[idx].0;
                let needed = matched_indices.len();
                let reserved = match sender.try_reserve_many(needed) {
                    Ok(p) => Some(p),
                    Err(mpsc::error::TrySendError::Closed(())) => None,
                    Err(mpsc::error::TrySendError::Full(())) => {
                        sender.reserve_many(needed).await.ok()
                    }
                };
                match reserved {
                    Some(p) => subscriber_permits.push((idx, matched_indices, p)),
                    None => closed.push(idx),
                }
            }

            if committed && !closed.is_empty() {
                // Accepted request: those subscribers miss this chunk and
                // everything after it.
                self.undelivered_after_commit
                    .fetch_add((closed.len() * remaining) as u64, Ordering::Relaxed);
            }
            if subscriber_permits.is_empty() {
                topic.purge_closed();
                return if committed {
                    Ok(0)
                } else {
                    Err(PublishError::Closed)
                };
            }

            for (_sub_idx, matched_indices, mut permit) in subscriber_permits {
                for rec_idx in matched_indices {
                    permit
                        .next()
                        .expect("reserved slot")
                        .send(chunk_records[rec_idx].clone());
                }
            }

            committed = true;
            remaining -= chunk_size;
            if !closed.is_empty() {
                closed.sort_unstable();
                for idx in closed.into_iter().rev() {
                    selected.remove(idx);
                }
                topic.purge_closed();
            }
        }
        Ok(selected.len())
    }

    /// Non-blocking all-or-nothing publish for cyclic feedback (memory sink).
    /// Never awaits, so a rule feeding its own input stream cannot deadlock
    /// its sink worker against its input queue. Capacity is reserved on
    /// every live subscriber before the record becomes visible, so a
    /// concurrent publisher can never cause a partial fan-out. On `Full`
    /// (a queue is full, or another publisher is mid-admission on the topic)
    /// nothing was enqueued and the caller must account the drop explicitly.
    pub fn try_publish(
        &self,
        stream_name: &str,
        record: StreamRecord,
    ) -> Result<usize, PublishError> {
        let Some(topic) = self.get_topic(stream_name) else {
            return Err(PublishError::NoSubscribers);
        };
        self.try_publish_topic(&topic, record)
    }

    fn try_publish_topic(
        &self,
        topic: &Topic,
        record: StreamRecord,
    ) -> Result<usize, PublishError> {
        let Ok(_admission) = topic.gate.try_lock() else {
            if topic.snapshot().0 == 0 {
                return Err(PublishError::NoSubscribers);
            }
            return Err(PublishError::Full);
        };
        let (registered, selected) = topic.snapshot();
        if registered == 0 {
            return Err(PublishError::NoSubscribers);
        }
        if selected.len() < registered {
            topic.purge_closed();
        }
        if selected.is_empty() {
            return Err(PublishError::Closed);
        }

        let mut active_senders = Vec::new();
        for (sender, filter) in selected {
            if let Some(f) = filter {
                if !f.check_and_account(&record) {
                    continue;
                }
            }
            active_senders.push(sender);
        }
        if active_senders.is_empty() {
            // All live subscribers filtered out the record; counters accounted.
            return Ok(0);
        }

        let mut permits = Vec::with_capacity(active_senders.len());
        for sender in &active_senders {
            match sender.try_reserve() {
                Ok(p) => permits.push(p),
                // Dropping the acquired permits releases their capacity:
                // nothing becomes visible anywhere.
                Err(mpsc::error::TrySendError::Full(())) => return Err(PublishError::Full),
                Err(mpsc::error::TrySendError::Closed(())) => {}
            }
        }
        if permits.is_empty() {
            topic.purge_closed();
            return Err(PublishError::Closed);
        }
        let delivered = permits.len();
        let mut permits = permits.into_iter();
        let last = permits.next_back().expect("at least one reservation");
        for p in permits {
            p.send(record.clone());
        }
        last.send(record);
        Ok(delivered)
    }

    /// Synchronous legacy-compatible publish used only where no subscriber
    /// backpressure context exists (tests, best-effort sources migrating).
    /// Tries to enqueue without awaiting; returns receiver count.
    pub fn publish(&self, stream_name: &str, record: StreamRecord) -> Result<usize, PublishError> {
        self.try_publish(stream_name, record).or_else(|e| match e {
            PublishError::NoSubscribers => Ok(0),
            other => Err(other),
        })
    }
}

impl StreamSender {
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Backpressured send of one record to every subscriber of this topic.
    #[inline]
    pub async fn send(&self, record: StreamRecord) -> Result<usize, PublishError> {
        self.bus.publish_async_topic(&self.topic_ref, record).await
    }

    /// Backpressured send of a validated batch, in order, to every subscriber.
    /// See [`StreamBus::publish_batch`] for the admission contract.
    pub async fn send_batch(&self, records: Vec<StreamRecord>) -> Result<usize, PublishError> {
        self.bus.publish_batch_topic(&self.topic_ref, records).await
    }

    /// Non-blocking feedback publish (all-or-nothing).
    pub fn try_send(&self, record: StreamRecord) -> Result<usize, PublishError> {
        self.bus.try_publish_topic(&self.topic_ref, record)
    }

    /// Queue depth high-water hint: deepest current subscriber backlog.
    pub fn backlog_hint(&self) -> usize {
        let senders = self.topic_ref.senders.read();
        senders
            .iter()
            .map(|(s, _)| STREAM_QUEUE_CAPACITY.saturating_sub(s.capacity()))
            .max()
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_filtered_subscription_routing_and_counters() {
        let bus = StreamBus::new();
        let source_in = Arc::new(AtomicU64::new(0));
        let filtered = Arc::new(AtomicU64::new(0));

        let filter = SubscriberFilter {
            predicate: BusPredicate::FieldGt {
                field: "temp".to_string(),
                threshold: 50.0,
            },
            source_in_counter: source_in.clone(),
            filtered_counter: filtered.clone(),
        };

        let mut rx = bus.subscribe_filtered("sensor", filter);

        // Record with temp = 20 (rejected)
        let mut data1 = HashMap::new();
        data1.insert("temp".to_string(), serde_json::json!(20.0));
        let rec1 = StreamRecord::new(data1);

        bus.publish("sensor", rec1).unwrap();

        // Counter check: rejected event incremented source_in and filtered
        assert_eq!(source_in.load(Ordering::Relaxed), 1);
        assert_eq!(filtered.load(Ordering::Relaxed), 1);
        // Receiver should NOT have received this event
        assert!(rx.try_recv().is_err());

        // Record with temp = 75 (accepted)
        let mut data2 = HashMap::new();
        data2.insert("temp".to_string(), serde_json::json!(75.0));
        let rec2 = StreamRecord::new(data2);

        bus.publish("sensor", rec2).unwrap();

        // Counters should NOT be modified by the filter for matching records
        assert_eq!(source_in.load(Ordering::Relaxed), 1);
        assert_eq!(filtered.load(Ordering::Relaxed), 1);
        // Receiver receives matching record
        let received = rx.try_recv().unwrap();
        assert_eq!(received.data.get("temp"), Some(&serde_json::json!(75.0)));

        // Error records bypass filter
        let mut err_data = HashMap::new();
        err_data.insert("error".to_string(), serde_json::json!("sensor disconnect"));
        let err_rec = StreamRecord::new(err_data);

        bus.publish("sensor", err_rec).unwrap();
        let err_received = rx.try_recv().unwrap();
        assert!(err_received.data.contains_key("error"));
    }

    #[tokio::test]
    async fn test_publish_batch_filtered() {
        let bus = StreamBus::new();
        let source_in = Arc::new(AtomicU64::new(0));
        let filtered = Arc::new(AtomicU64::new(0));

        let filter = SubscriberFilter {
            predicate: BusPredicate::FieldEq {
                field: "city".to_string(),
                value: serde_json::json!("Paris"),
            },
            source_in_counter: source_in.clone(),
            filtered_counter: filtered.clone(),
        };

        let mut rx = bus.subscribe_filtered("weather", filter);

        let mut d1 = HashMap::new();
        d1.insert("city".to_string(), serde_json::json!("London"));
        let mut d2 = HashMap::new();
        d2.insert("city".to_string(), serde_json::json!("Paris"));
        let mut d3 = HashMap::new();
        d3.insert("city".to_string(), serde_json::json!("Berlin"));

        let records = vec![
            StreamRecord::new(d1),
            StreamRecord::new(d2),
            StreamRecord::new(d3),
        ];
        bus.publish_batch("weather", records).await.unwrap();

        assert_eq!(source_in.load(Ordering::Relaxed), 2);
        assert_eq!(filtered.load(Ordering::Relaxed), 2);

        let rec = rx.try_recv().unwrap();
        assert_eq!(rec.data.get("city"), Some(&serde_json::json!("Paris")));
        assert!(rx.try_recv().is_err());
    }
}
