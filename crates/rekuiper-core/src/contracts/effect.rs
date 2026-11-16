//! Outbox identity, delivery boundaries, and idempotent effect outcomes.

use super::error::StructuredError;
use super::identity::{ActionId, ConnectionId, Generation, InvalidId, RuleId, MAX_ID_LEN};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Stable outbox effect identifier.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub struct EffectId(String);

impl EffectId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for EffectId {
    type Error = InvalidId;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_effect_id(&value)?;
        Ok(Self(value))
    }
}

impl TryFrom<&str> for EffectId {
    type Error = InvalidId;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        validate_effect_id(value)?;
        Ok(Self(value.to_owned()))
    }
}

fn validate_effect_id(value: &str) -> Result<(), InvalidId> {
    if value.is_empty() || value.len() > MAX_ID_LEN {
        return Err(InvalidId::Length);
    }
    let mut chars = value.chars();
    if !chars
        .next()
        .is_some_and(|ch| ch.is_alphanumeric() || ch == '_')
        || !chars.all(|ch| ch.is_alphanumeric() || matches!(ch, '_' | '-' | '.' | ':'))
    {
        return Err(InvalidId::Characters);
    }
    Ok(())
}

/// Target action and optional connection identity for an outbox effect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectDestination {
    pub action_id: ActionId,
    pub connection_id: Option<ConnectionId>,
}

/// Distinct progress boundaries in the reliable delivery lifecycle.
/// `DurableAccepted` is a source-acceptance boundary, never external delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectDeliveryBoundary {
    AdmittedInMemory,
    DurableAccepted,
    ProcessedIntoDurableState,
    Delivered,
    TransactionallyCommitted,
}

/// Four-way outcome returned by an external effect sink.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum EffectOutcome {
    Delivered,
    Retryable { delay_hint_millis: Option<u64> },
    PermanentFailure { error: StructuredError },
    Ambiguous { error: StructuredError },
}

impl EffectOutcome {
    /// Returns true when no further delivery retries should occur.
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Delivered | Self::PermanentFailure { .. })
    }
}

/// Result of applying a completion callback to an outbox record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionStatus {
    Applied,
    DuplicateIgnored,
}

/// Outbox record tracking effect identity, attempts, and terminal outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectRecord {
    pub effect_id: EffectId,
    pub rule_id: RuleId,
    pub generation: Generation,
    pub destination: EffectDestination,
    pub order_key: Option<String>,
    pub attempt: u32,
    pub terminal_outcome: Option<EffectOutcome>,
}

impl EffectRecord {
    /// Records a completion outcome idempotently. Once a terminal outcome is
    /// recorded, subsequent completions cannot produce a second terminal outcome.
    pub fn record_completion(&mut self, outcome: EffectOutcome) -> CompletionStatus {
        if self.terminal_outcome.is_some() {
            return CompletionStatus::DuplicateIgnored;
        }
        self.attempt = self.attempt.saturating_add(1);
        if outcome.is_terminal() {
            self.terminal_outcome = Some(outcome);
        }
        CompletionStatus::Applied
    }
}

/// Dispatch request handed to an external effect sink.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectDispatch {
    pub effect_id: EffectId,
    pub rule_id: RuleId,
    pub generation: Generation,
    pub destination: EffectDestination,
    pub order_key: Option<String>,
    pub payload: Vec<u8>,
}

#[async_trait]
pub trait EffectSink: Send + Sync {
    async fn deliver(&self, dispatch: &EffectDispatch) -> EffectOutcome;
}
