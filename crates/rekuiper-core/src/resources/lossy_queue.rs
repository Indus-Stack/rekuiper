//! Explicit bounded `dropOldest` subscriber FIFO queue.
//!
//! Keeps evictions atomic, preserves the newest suffix for oversized batches,
//! provides race-safe close/wakeup, and rejects reliable control envelopes.

use crate::contracts::OrderedEnvelope;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Notify;

/// Errors returned when pushing to a lossy queue.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum LossyPushError {
    #[error("lossy subscriber queue closed")]
    Closed,
    #[error("reliable control messages cannot enter an evictable lossy queue")]
    ControlMessageRejected,
}

/// Errors returned by [`LossyQueueReceiver::try_recv`].
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum LossyTryRecvError {
    #[error("lossy subscriber queue is empty")]
    Empty,
    #[error("lossy subscriber queue closed")]
    Closed,
}

/// Observable snapshot of a [`LossyQueueSender`] / [`LossyQueueReceiver`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LossyQueueMetrics {
    pub capacity: usize,
    pub len: usize,
    pub evicted_total: u64,
}

#[derive(Debug)]
struct LossyInner<T> {
    capacity: usize,
    state: Mutex<LossyState<T>>,
    notify: Notify,
}

#[derive(Debug)]
struct LossyState<T> {
    queue: VecDeque<T>,
    closed: bool,
    evicted_total: u64,
}

/// Creates a bounded `dropOldest` FIFO channel with `capacity.max(1)` slots.
pub fn lossy_channel<T>(capacity: usize) -> (LossyQueueSender<T>, LossyQueueReceiver<T>) {
    let cap = capacity.max(1);
    let inner = Arc::new(LossyInner {
        capacity: cap,
        state: Mutex::new(LossyState {
            queue: VecDeque::with_capacity(cap),
            closed: false,
            evicted_total: 0,
        }),
        notify: Notify::new(),
    });
    (
        LossyQueueSender {
            inner: Arc::clone(&inner),
        },
        LossyQueueReceiver { inner },
    )
}

/// Producer handle for an explicit `dropOldest` subscriber queue.
#[derive(Debug)]
pub struct LossyQueueSender<T> {
    inner: Arc<LossyInner<T>>,
}

impl<T> Clone for LossyQueueSender<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> LossyQueueSender<T> {
    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

    pub fn is_closed(&self) -> bool {
        self.inner.state.lock().closed
    }

    pub fn metrics(&self) -> LossyQueueMetrics {
        let state = self.inner.state.lock();
        LossyQueueMetrics {
            capacity: self.inner.capacity,
            len: state.queue.len(),
            evicted_total: state.evicted_total,
        }
    }

    /// Atomically evicts the oldest item if full and appends `item`.
    /// Returns the number of evicted items (0 or 1).
    pub fn push(&self, item: T) -> Result<usize, LossyPushError> {
        let mut state = self.inner.state.lock();
        if state.closed {
            return Err(LossyPushError::Closed);
        }
        let mut evicted = 0usize;
        if state.queue.len() >= self.inner.capacity {
            state.queue.pop_front();
            state.evicted_total = state.evicted_total.saturating_add(1);
            evicted = 1;
        }
        state.queue.push_back(item);
        drop(state);
        self.inner.notify.notify_one();
        Ok(evicted)
    }

    /// Atomically appends a batch, evicting oldest entries and preserving the
    /// newest suffix of up to `capacity` items (even for oversized batches).
    pub fn push_batch(&self, items: Vec<T>) -> Result<usize, LossyPushError> {
        let mut state = self.inner.state.lock();
        if state.closed {
            return Err(LossyPushError::Closed);
        }
        if items.is_empty() {
            return Ok(0);
        }

        let cap = self.inner.capacity;
        let total = items.len();
        let mut evicted = 0usize;

        if total >= cap {
            // Entire existing queue + excess prefix of incoming batch are evicted;
            // only the newest `cap` items of `items` survive in FIFO order.
            let old_len = state.queue.len();
            state.queue.clear();
            let skip = total - cap;
            evicted = old_len + skip;
            state.queue.extend(items.into_iter().skip(skip));
        } else {
            let overflow = state.queue.len().saturating_add(total).saturating_sub(cap);
            for _ in 0..overflow {
                if state.queue.pop_front().is_some() {
                    evicted += 1;
                }
            }
            state.queue.extend(items);
        }

        state.evicted_total = state.evicted_total.saturating_add(evicted as u64);
        drop(state);
        self.inner.notify.notify_one();
        Ok(evicted)
    }

    /// Marks the queue closed and wakes any waiting receiver.
    pub fn close(&self) {
        let mut state = self.inner.state.lock();
        state.closed = true;
        drop(state);
        self.inner.notify.notify_waiters();
    }
}

impl<T> LossyQueueSender<OrderedEnvelope<T>> {
    /// Reliable control/barrier envelopes are forbidden on evictable queues.
    pub fn push_reliable_control(
        &self,
        _envelope: OrderedEnvelope<T>,
    ) -> Result<usize, LossyPushError> {
        Err(LossyPushError::ControlMessageRejected)
    }
}

/// Consumer handle for an explicit `dropOldest` subscriber queue.
#[derive(Debug)]
pub struct LossyQueueReceiver<T> {
    inner: Arc<LossyInner<T>>,
}

impl<T> LossyQueueReceiver<T> {
    pub fn metrics(&self) -> LossyQueueMetrics {
        let state = self.inner.state.lock();
        LossyQueueMetrics {
            capacity: self.inner.capacity,
            len: state.queue.len(),
            evicted_total: state.evicted_total,
        }
    }

    pub fn try_recv(&mut self) -> Result<T, LossyTryRecvError> {
        let mut state = self.inner.state.lock();
        if let Some(item) = state.queue.pop_front() {
            return Ok(item);
        }
        if state.closed {
            Err(LossyTryRecvError::Closed)
        } else {
            Err(LossyTryRecvError::Empty)
        }
    }

    /// Receives the next item, or returns `None` once closed and drained.
    /// Race-safe against concurrent `push` and `close`.
    pub async fn recv(&mut self) -> Option<T> {
        loop {
            let notified = self.inner.notify.notified();
            {
                let mut state = self.inner.state.lock();
                if let Some(item) = state.queue.pop_front() {
                    return Some(item);
                }
                if state.closed {
                    return None;
                }
            }
            notified.await;
        }
    }

    pub fn close(&mut self) {
        let mut state = self.inner.state.lock();
        state.closed = true;
        drop(state);
        self.inner.notify.notify_waiters();
    }
}

impl<T> Drop for LossyQueueReceiver<T> {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock();
        state.closed = true;
        drop(state);
        self.inner.notify.notify_waiters();
    }
}
