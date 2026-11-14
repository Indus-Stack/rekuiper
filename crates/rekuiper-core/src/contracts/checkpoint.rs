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

    /// Computes a lowercase `sha256:<hex>` checksum using pure-Rust FIPS 180-4 SHA-256
    /// so `rekuiper-core` needs no extra cryptographic dependencies.
    pub fn sha256_bytes(bytes: &[u8]) -> Self {
        const K: [u32; 64] = [
            0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
            0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
            0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
            0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
            0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
            0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
            0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
            0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
            0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
            0xc67178f2,
        ];
        let mut h: [u32; 8] = [
            0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
            0x5be0cd19,
        ];
        let mut msg = bytes.to_vec();
        let bit_len = (bytes.len() as u64).wrapping_mul(8);
        msg.push(0x80);
        while (msg.len() % 64) != 56 {
            msg.push(0);
        }
        msg.extend_from_slice(&bit_len.to_be_bytes());
        for offset in (0..msg.len()).step_by(64) {
            let chunk = &msg[offset..offset + 64];
            let mut w = [0u32; 64];
            for i in 0..16 {
                w[i] = u32::from_be_bytes(chunk[4 * i..4 * i + 4].try_into().unwrap());
            }
            for i in 16..64 {
                let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
                let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
                w[i] = w[i - 16]
                    .wrapping_add(s0)
                    .wrapping_add(w[i - 7])
                    .wrapping_add(s1);
            }
            let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh] = h;
            for i in 0..64 {
                let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
                let ch = (e & f) ^ ((!e) & g);
                let t1 = hh
                    .wrapping_add(s1)
                    .wrapping_add(ch)
                    .wrapping_add(K[i])
                    .wrapping_add(w[i]);
                let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
                let maj = (a & b) ^ (a & c) ^ (b & c);
                let t2 = s0.wrapping_add(maj);
                hh = g;
                g = f;
                f = e;
                e = d.wrapping_add(t1);
                d = c;
                c = b;
                b = a;
                a = t1.wrapping_add(t2);
            }
            for (slot, delta) in h.iter_mut().zip([a, b, c, d, e, f, g, hh]) {
                *slot = slot.wrapping_add(delta);
            }
        }
        let mut hex = String::with_capacity(64);
        for word in h {
            use std::fmt::Write as _;
            let _ = write!(&mut hex, "{word:08x}");
        }
        Self::try_from(format!("sha256:{hex}")).expect("valid sha256 digest")
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
