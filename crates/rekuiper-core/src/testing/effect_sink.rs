//! Programmable external effect sink fake.

use crate::contracts::{EffectDispatch, EffectId, EffectOutcome, EffectSink};
use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::{BTreeMap, VecDeque};

#[derive(Default)]
struct SinkInner {
    scripted: BTreeMap<EffectId, VecDeque<EffectOutcome>>,
    default_outcome: Option<EffectOutcome>,
    dispatches: Vec<EffectDispatch>,
    op_log: Vec<String>,
}

/// Programmable `EffectSink` supporting scripted retryable, ambiguous,
/// permanent-failure, and delivered outcomes.
#[derive(Default)]
pub struct FakeEffectSink {
    inner: Mutex<SinkInner>,
}

impl FakeEffectSink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_outcome(&self, effect_id: EffectId, outcome: EffectOutcome) {
        self.inner
            .lock()
            .scripted
            .entry(effect_id)
            .or_default()
            .push_back(outcome);
    }

    pub fn set_default_outcome(&self, outcome: EffectOutcome) {
        self.inner.lock().default_outcome = Some(outcome);
    }

    pub fn dispatches(&self) -> Vec<EffectDispatch> {
        self.inner.lock().dispatches.clone()
    }

    pub fn op_log(&self) -> Vec<String> {
        self.inner.lock().op_log.clone()
    }
}

#[async_trait]
impl EffectSink for FakeEffectSink {
    async fn deliver(&self, dispatch: &EffectDispatch) -> EffectOutcome {
        let mut inner = self.inner.lock();
        inner.dispatches.push(dispatch.clone());
        let outcome = inner
            .scripted
            .get_mut(&dispatch.effect_id)
            .and_then(|queue| queue.pop_front())
            .or_else(|| inner.default_outcome.clone())
            .unwrap_or(EffectOutcome::Delivered);
        inner.op_log.push(format!(
            "deliver:{}:{outcome:?}",
            dispatch.effect_id.as_str()
        ));
        outcome
    }
}
