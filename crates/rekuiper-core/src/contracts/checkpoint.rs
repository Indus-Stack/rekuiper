//! Versioned, generation-fenced checkpoint metadata and commit interfaces.

use super::effect::EffectId;
use super::identity::{CheckpointId, Generation, OperatorId, RuleId};
use super::position::SourcePosition;
use super::state::{StateResourceUsage, StateSchemaVersion, StateSnapshot};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Current format version for checkpoint metadata envelopes.
pub const CHECKPOINT_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("content checksum must be formatted as sha256:<64 lowercase hex characters>")]
pub struct InvalidChecksum;

/// Validated content checksum formatted as `sha256:<64 lowercase hex chars>`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub struct ContentChecksum(String);

impl ContentChecksum {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ContentChecksum {
    type Error = InvalidChecksum;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_checksum(&value)?;
        Ok(Self(value))
    }
}

impl TryFrom<&str> for ContentChecksum {
    type Error = InvalidChecksum;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        validate_checksum(value)?;
        Ok(Self(value.to_owned()))
    }
}

fn validate_checksum(value: &str) -> Result<(), InvalidChecksum> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(InvalidChecksum);
    };
    if hex.len() != 64 || !hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        return Err(InvalidChecksum);
    }
    Ok(())
}

/// Plan, schema, and extension fingerprints bound to a checkpointed rule generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanIdentity {
    pub rule_id: RuleId,
    pub generation: Generation,
    pub plan_fingerprint: ContentChecksum,
    #[serde(default)]
    pub schema_fingerprints: BTreeMap<String, ContentChecksum>,
    #[serde(default)]
    pub extension_digests: BTreeMap<String, ContentChecksum>,
}

/// Metadata summary for one state participant stored inside a checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParticipantSnapshotRef {
    pub operator_id: OperatorId,
    pub state_version: StateSchemaVersion,
    pub payload_checksum: ContentChecksum,
    pub payload_bytes: u64,
    pub usage: StateResourceUsage,
}

/// Outbox watermark recorded alongside operator state and source positions.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboxBoundary {
    pub last_effect_id: Option<EffectId>,
    pub pending_effect_count: u64,
}

/// Atomic checkpoint metadata linking source progress, operator state, and outbox boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointMetadata {
    pub format_version: u32,
    pub checkpoint_id: CheckpointId,
    pub identity: PlanIdentity,
    pub created_wall_millis: i64,
    pub source_positions: Vec<SourcePosition>,
    pub participants: BTreeMap<OperatorId, ParticipantSnapshotRef>,
    pub outbox: OutboxBoundary,
}

/// Complete checkpoint envelope with metadata and participant payloads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointEnvelope {
    pub metadata: CheckpointMetadata,
    pub payloads: BTreeMap<OperatorId, StateSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CheckpointError {
    #[error("unsupported checkpoint format version: {0}")]
    UnsupportedFormatVersion(u32),
    #[error("checkpoint rule mismatch: expected {expected}, got {actual}")]
    RuleMismatch { expected: RuleId, actual: RuleId },
    #[error("checkpoint generation mismatch: expected {expected:?}, got {actual:?}")]
    GenerationMismatch {
        expected: Generation,
        actual: Generation,
    },
    #[error("checkpoint plan fingerprint mismatch")]
    PlanFingerprintMismatch,
    #[error("checkpoint schema or extension fingerprint mismatch for {0}")]
    DependencyFingerprintMismatch(String),
    #[error("missing state participant in checkpoint: {0}")]
    MissingParticipant(OperatorId),
    #[error("unexpected state participant in checkpoint: {0}")]
    UnexpectedParticipant(OperatorId),
    #[error("participant metadata mismatch for {0}")]
    ParticipantMetadataMismatch(OperatorId),
    #[error("participant checksum mismatch for {0}")]
    ChecksumMismatch(OperatorId),
    #[error("source position generation mismatch")]
    SourcePositionGenerationMismatch,
    #[error("checkpoint storage failure: {0}")]
    StorageFailure(String),
}

impl CheckpointEnvelope {
    /// Validates format version, plan/generation/dependency identity, participant
    /// completeness, checksum consistency, and source position ownership.
    pub fn validate_restore(
        &self,
        expected_identity: &PlanIdentity,
        expected_participants: &BTreeSet<OperatorId>,
    ) -> Result<(), CheckpointError> {
        let meta = &self.metadata;
        if meta.format_version != CHECKPOINT_FORMAT_VERSION {
            return Err(CheckpointError::UnsupportedFormatVersion(
                meta.format_version,
            ));
        }
        if meta.identity.rule_id != expected_identity.rule_id {
            return Err(CheckpointError::RuleMismatch {
                expected: expected_identity.rule_id.clone(),
                actual: meta.identity.rule_id.clone(),
            });
        }
        if meta.identity.generation != expected_identity.generation {
            return Err(CheckpointError::GenerationMismatch {
                expected: expected_identity.generation,
                actual: meta.identity.generation,
            });
        }
        if meta.identity.plan_fingerprint != expected_identity.plan_fingerprint {
            return Err(CheckpointError::PlanFingerprintMismatch);
        }
        if meta.identity.schema_fingerprints != expected_identity.schema_fingerprints {
            return Err(CheckpointError::DependencyFingerprintMismatch(
                "schemas".to_owned(),
            ));
        }
        if meta.identity.extension_digests != expected_identity.extension_digests {
            return Err(CheckpointError::DependencyFingerprintMismatch(
                "extensions".to_owned(),
            ));
        }

        for pos in &meta.source_positions {
            if pos.generation != expected_identity.generation {
                return Err(CheckpointError::SourcePositionGenerationMismatch);
            }
        }

        for expected in expected_participants {
            let Some(part_ref) = meta.participants.get(expected) else {
                return Err(CheckpointError::MissingParticipant(expected.clone()));
            };
            let Some(snapshot) = self.payloads.get(expected) else {
                return Err(CheckpointError::MissingParticipant(expected.clone()));
            };
            if &part_ref.operator_id != expected || &snapshot.operator_id != expected {
                return Err(CheckpointError::ParticipantMetadataMismatch(
                    expected.clone(),
                ));
            }
            if part_ref.state_version != snapshot.state_version
                || part_ref.payload_bytes != snapshot.payload.len() as u64
            {
                return Err(CheckpointError::ParticipantMetadataMismatch(
                    expected.clone(),
                ));
            }
            if part_ref.payload_checksum != snapshot.checksum {
                return Err(CheckpointError::ChecksumMismatch(expected.clone()));
            }
        }

        for actual in meta.participants.keys() {
            if !expected_participants.contains(actual) {
                return Err(CheckpointError::UnexpectedParticipant(actual.clone()));
            }
        }
        for actual in self.payloads.keys() {
            if !expected_participants.contains(actual) {
                return Err(CheckpointError::UnexpectedParticipant(actual.clone()));
            }
        }

        Ok(())
    }
}

/// Atomic, generation-fenced checkpoint persistence contract.
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    async fn commit_checkpoint(&self, envelope: CheckpointEnvelope) -> Result<(), CheckpointError>;

    async fn load_latest(
        &self,
        rule_id: &RuleId,
    ) -> Result<Option<CheckpointEnvelope>, CheckpointError>;
}
