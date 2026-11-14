//! Cancellable reserve/commit/release accounting for item, logical-byte, and task budgets.

use parking_lot::Mutex;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{watch, Notify};

/// Dimension name for budget error diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetDimension {
    Items,
    LogicalBytes,
    Tasks,
}

impl std::fmt::Display for BudgetDimension {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Items => f.write_str("items"),
            Self::LogicalBytes => f.write_str("logical_bytes"),
            Self::Tasks => f.write_str("tasks"),
        }
    }
}

/// Configured upper bounds for a [`ResourceBudget`]. `None` means unbounded.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BudgetLimits {
    pub max_items: Option<u64>,
    pub max_logical_bytes: Option<u64>,
    pub max_tasks: Option<u64>,
}

/// Charge applied to a [`ResourceBudget`].
///
/// For shared/deduplicated data, `logical_bytes` carries the conservative
/// logical charge while `physical_bytes` records the observed physical size.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BudgetCharge {
    pub items: u64,
    pub logical_bytes: u64,
    pub physical_bytes: u64,
    pub tasks: u64,
}

impl BudgetCharge {
    pub fn new(items: u64, logical_bytes: u64, tasks: u64) -> Self {
        Self {
            items,
            logical_bytes,
            physical_bytes: logical_bytes,
            tasks,
        }
    }

    /// Conservative charge for shared data where observed physical bytes may
    /// be smaller than the logical byte footprint.
    pub fn for_shared_bytes(items: u64, logical_bytes: u64, observed_physical_bytes: u64) -> Self {
        Self {
            items,
            logical_bytes,
            physical_bytes: observed_physical_bytes,
            tasks: 0,
        }
    }
}

/// Snapshot of current budget usage, separating reserved vs. committed and
/// charged logical bytes vs. observed physical bytes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BudgetUsage {
    pub reserved_items: u64,
    pub reserved_logical_bytes: u64,
    pub reserved_tasks: u64,
    pub committed_items: u64,
    pub committed_logical_bytes: u64,
    pub committed_tasks: u64,
    pub observed_physical_bytes: u64,
}

impl BudgetUsage {
    pub fn total_items(&self) -> u64 {
        self.reserved_items.saturating_add(self.committed_items)
    }

    pub fn total_logical_bytes(&self) -> u64 {
        self.reserved_logical_bytes
            .saturating_add(self.committed_logical_bytes)
    }

    pub fn total_tasks(&self) -> u64 {
        self.reserved_tasks.saturating_add(self.committed_tasks)
    }
}

/// Errors returned by [`ResourceBudget`] operations.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum BudgetError {
    #[error("oversize request on {dimension}: requested {requested} exceeds limit {limit}")]
    OversizeItem {
        dimension: BudgetDimension,
        requested: u64,
        limit: u64,
    },
    #[error("budget exhausted on {dimension}: requested {requested}, available {available}")]
    Exhausted {
        dimension: BudgetDimension,
        requested: u64,
        available: u64,
    },
    #[error("budget reservation cancelled")]
    Cancelled,
}

#[derive(Debug)]
struct BudgetInner {
    limits: BudgetLimits,
    state: Mutex<BudgetUsage>,
    notify: Notify,
}

/// Thread-safe, cancellable resource budget for items, logical bytes, and tasks.
#[derive(Debug, Clone)]
pub struct ResourceBudget {
    inner: Arc<BudgetInner>,
}

impl ResourceBudget {
    pub fn new(limits: BudgetLimits) -> Self {
        Self {
            inner: Arc::new(BudgetInner {
                limits,
                state: Mutex::new(BudgetUsage::default()),
                notify: Notify::new(),
            }),
        }
    }

    pub fn limits(&self) -> BudgetLimits {
        self.inner.limits
    }

    pub fn usage(&self) -> BudgetUsage {
        *self.inner.state.lock()
    }

    /// Attempts to reserve `charge` without waiting.
    pub fn try_reserve(&self, charge: BudgetCharge) -> Result<BudgetReservation, BudgetError> {
        check_oversize(&self.inner.limits, &charge)?;
        let mut state = self.inner.state.lock();
        check_capacity(&self.inner.limits, &state, &charge)?;
        apply_reserve(&mut state, &charge);
        Ok(BudgetReservation {
            inner: Arc::clone(&self.inner),
            charge,
            active: true,
        })
    }

    /// Reserves `charge`, waiting until capacity is available or `cancel` fires.
    /// Dropping the returned future releases any unreturned reservation.
    pub async fn reserve(
        &self,
        charge: BudgetCharge,
        mut cancel: Option<&mut watch::Receiver<bool>>,
    ) -> Result<BudgetReservation, BudgetError> {
        check_oversize(&self.inner.limits, &charge)?;
        loop {
            if let Some(rx) = cancel.as_deref() {
                if *rx.borrow() {
                    return Err(BudgetError::Cancelled);
                }
            }
            let notified = self.inner.notify.notified();
            {
                let mut state = self.inner.state.lock();
                if check_capacity(&self.inner.limits, &state, &charge).is_ok() {
                    apply_reserve(&mut state, &charge);
                    return Ok(BudgetReservation {
                        inner: Arc::clone(&self.inner),
                        charge,
                        active: true,
                    });
                }
            }
            match cancel.as_deref_mut() {
                Some(rx) => {
                    tokio::select! {
                        biased;
                        changed = rx.changed() => {
                            if changed.is_err() || *rx.borrow() {
                                return Err(BudgetError::Cancelled);
                            }
                        }
                        _ = notified => {}
                    }
                }
                None => notified.await,
            }
        }
    }
}

/// Pending reservation guard. Automatically releases on drop unless committed.
#[derive(Debug)]
pub struct BudgetReservation {
    inner: Arc<BudgetInner>,
    charge: BudgetCharge,
    active: bool,
}

impl BudgetReservation {
    /// Transitions this reservation into a committed charge.
    pub fn commit(mut self) -> BudgetCommit {
        if self.active {
            self.active = false;
            let mut state = self.inner.state.lock();
            state.reserved_items = state.reserved_items.saturating_sub(self.charge.items);
            state.reserved_logical_bytes = state
                .reserved_logical_bytes
                .saturating_sub(self.charge.logical_bytes);
            state.reserved_tasks = state.reserved_tasks.saturating_sub(self.charge.tasks);
            state.committed_items = state.committed_items.saturating_add(self.charge.items);
            state.committed_logical_bytes = state
                .committed_logical_bytes
                .saturating_add(self.charge.logical_bytes);
            state.committed_tasks = state.committed_tasks.saturating_add(self.charge.tasks);
        }
        BudgetCommit {
            inner: Arc::clone(&self.inner),
            charge: self.charge,
            active: true,
        }
    }

    /// Explicitly cancels and releases the reservation.
    pub fn cancel(mut self) {
        self.release_reserved();
    }

    fn release_reserved(&mut self) {
        if !self.active {
            return;
        }
        self.active = false;
        let mut state = self.inner.state.lock();
        state.reserved_items = state.reserved_items.saturating_sub(self.charge.items);
        state.reserved_logical_bytes = state
            .reserved_logical_bytes
            .saturating_sub(self.charge.logical_bytes);
        state.reserved_tasks = state.reserved_tasks.saturating_sub(self.charge.tasks);
        state.observed_physical_bytes = state
            .observed_physical_bytes
            .saturating_sub(self.charge.physical_bytes);
        drop(state);
        self.inner.notify.notify_waiters();
    }
}

impl Drop for BudgetReservation {
    fn drop(&mut self) {
        self.release_reserved();
    }
}

/// Committed charge guard. Idempotent release prevents double-release underflow.
#[derive(Debug)]
pub struct BudgetCommit {
    inner: Arc<BudgetInner>,
    charge: BudgetCharge,
    active: bool,
}

impl BudgetCommit {
    /// Explicitly releases the committed charge.
    pub fn release(mut self) {
        self.release_committed();
    }

    /// Idempotent release safe to call even if `release` is triggered again on drop.
    pub fn release_in_place(&mut self) {
        self.release_committed();
    }

    fn release_committed(&mut self) {
        if !self.active {
            return;
        }
        self.active = false;
        let mut state = self.inner.state.lock();
        state.committed_items = state.committed_items.saturating_sub(self.charge.items);
        state.committed_logical_bytes = state
            .committed_logical_bytes
            .saturating_sub(self.charge.logical_bytes);
        state.committed_tasks = state.committed_tasks.saturating_sub(self.charge.tasks);
        state.observed_physical_bytes = state
            .observed_physical_bytes
            .saturating_sub(self.charge.physical_bytes);
        drop(state);
        self.inner.notify.notify_waiters();
    }
}

impl Drop for BudgetCommit {
    fn drop(&mut self) {
        self.release_committed();
    }
}

fn check_oversize(limits: &BudgetLimits, charge: &BudgetCharge) -> Result<(), BudgetError> {
    if let Some(limit) = limits.max_items {
        if charge.items > limit {
            return Err(BudgetError::OversizeItem {
                dimension: BudgetDimension::Items,
                requested: charge.items,
                limit,
            });
        }
    }
    if let Some(limit) = limits.max_logical_bytes {
        if charge.logical_bytes > limit {
            return Err(BudgetError::OversizeItem {
                dimension: BudgetDimension::LogicalBytes,
                requested: charge.logical_bytes,
                limit,
            });
        }
    }
    if let Some(limit) = limits.max_tasks {
        if charge.tasks > limit {
            return Err(BudgetError::OversizeItem {
                dimension: BudgetDimension::Tasks,
                requested: charge.tasks,
                limit,
            });
        }
    }
    Ok(())
}

fn check_capacity(
    limits: &BudgetLimits,
    usage: &BudgetUsage,
    charge: &BudgetCharge,
) -> Result<(), BudgetError> {
    if let Some(limit) = limits.max_items {
        let used = usage.total_items();
        let avail = limit.saturating_sub(used);
        if charge.items > avail {
            return Err(BudgetError::Exhausted {
                dimension: BudgetDimension::Items,
                requested: charge.items,
                available: avail,
            });
        }
    }
    if let Some(limit) = limits.max_logical_bytes {
        let used = usage.total_logical_bytes();
        let avail = limit.saturating_sub(used);
        if charge.logical_bytes > avail {
            return Err(BudgetError::Exhausted {
                dimension: BudgetDimension::LogicalBytes,
                requested: charge.logical_bytes,
                available: avail,
            });
        }
    }
    if let Some(limit) = limits.max_tasks {
        let used = usage.total_tasks();
        let avail = limit.saturating_sub(used);
        if charge.tasks > avail {
            return Err(BudgetError::Exhausted {
                dimension: BudgetDimension::Tasks,
                requested: charge.tasks,
                available: avail,
            });
        }
    }
    Ok(())
}

fn apply_reserve(state: &mut BudgetUsage, charge: &BudgetCharge) {
    state.reserved_items = state.reserved_items.saturating_add(charge.items);
    state.reserved_logical_bytes = state
        .reserved_logical_bytes
        .saturating_add(charge.logical_bytes);
    state.reserved_tasks = state.reserved_tasks.saturating_add(charge.tasks);
    state.observed_physical_bytes = state
        .observed_physical_bytes
        .saturating_add(charge.physical_bytes);
}
