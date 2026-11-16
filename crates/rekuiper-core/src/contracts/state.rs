//! Versioned operator state participant contracts.

use super::checkpoint::ContentChecksum;
use super::identity::OperatorId;
use serde::{Deserialize, Serialize};

/// Current envelope version for serialized operator state snapshots.
pub const STATE_ENVELOPE_VERSION: u32 = 1;

/// Operator-owned schema version for its internal state payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StateSchemaVersion(pub u32);

/// Logical resource usage reported by a stateful operator.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateResourceUsage {
    pub records: u64,
    pub bytes: u64,
}

/// Immutable, versioned snapshot captured by an operator's owner task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateSnapshot {
    pub envelope_version: u32,
    pub operator_id: OperatorId,
    pub state_version: StateSchemaVersion,
    pub checksum: ContentChecksum,
    pub payload: Vec<u8>,
    pub usage: StateResourceUsage,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StateError {
    #[error("unsupported state envelope version: {0}")]
    UnsupportedEnvelopeVersion(u32),
    #[error("unsupported state schema version {version:?} for operator {operator_id}")]
    UnsupportedStateVersion {
        operator_id: OperatorId,
        version: StateSchemaVersion,
    },
    #[error("snapshot operator mismatch: expected {expected}, got {actual}")]
    OperatorMismatch {
        expected: OperatorId,
        actual: OperatorId,
    },
    #[error("snapshot payload checksum mismatch for operator {0}")]
    ChecksumMismatch(OperatorId),
    #[error("corrupt state snapshot: {0}")]
    CorruptSnapshot(String),
    #[error("state {resource} budget exceeded: limit {limit}, actual {actual}")]
    BudgetExceeded {
        resource: &'static str,
        limit: u64,
        actual: u64,
    },
}

impl StateSnapshot {
    /// Validates envelope version, operator ownership, and supported state schema versions.
    pub fn validate(
        &self,
        expected_operator: &OperatorId,
        supported_versions: &[StateSchemaVersion],
    ) -> Result<(), StateError> {
        if self.envelope_version != STATE_ENVELOPE_VERSION {
            return Err(StateError::UnsupportedEnvelopeVersion(
                self.envelope_version,
            ));
        }
        if &self.operator_id != expected_operator {
            return Err(StateError::OperatorMismatch {
                expected: expected_operator.clone(),
                actual: self.operator_id.clone(),
            });
        }
        if !supported_versions.contains(&self.state_version) {
            return Err(StateError::UnsupportedStateVersion {
                operator_id: self.operator_id.clone(),
                version: self.state_version,
            });
        }
        Ok(())
    }
}

/// Stateful operator participant that captures and restores owned snapshots
/// between records on its owner task.
pub trait StateParticipant: Send + Sync {
    fn operator_id(&self) -> &OperatorId;

    /// Optional explicit role tag (`"analytic"`, `"tumbling"`, `"count"`, `"hopping"`,
    /// `"sliding"`, `"session"`, `"state"`, `"join"`, `"cep"`, `"barrier"`) used to
    /// match required plan participant roles without ambiguous `operator_id` substrings.
    fn participant_kind_tag(&self) -> Option<&'static str> {
        None
    }

    fn state_version(&self) -> StateSchemaVersion;

    fn resource_usage(&self) -> StateResourceUsage;

    fn capture_snapshot(&self) -> Result<StateSnapshot, StateError>;

    fn restore_snapshot(&mut self, snapshot: &StateSnapshot) -> Result<(), StateError>;
}
