//! Immutable savepoint export, checksum/dependency validation, operator migration,
//! and restore-to-new-rule lineage without stealing existing rule backlogs.

use crate::contracts::{
    CheckpointId, ContentChecksum, Generation, OperatorId, RuleId, StateError, StateSchemaVersion,
    StateSnapshot,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub const SAVEPOINT_FORMAT_VERSION: u32 = 1;

/// Pure-Rust FIPS 180-4 SHA-256 so `rekuiper-core` needs zero extra crypto dependencies.
fn sha256_checksum(bytes: &[u8]) -> ContentChecksum {
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
    for chunk in msg.chunks_exact(64) {
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
    ContentChecksum::try_from(format!("sha256:{hex}")).expect("valid sha256 digest")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SavepointError {
    UnsupportedFormatVersion(u32),
    CorruptManifest(String),
    CorruptParticipant {
        operator_id: OperatorId,
        reason: String,
    },
    MissingParticipant(OperatorId),
    MissingExtension(String),
    IncompatibleSchema {
        operator_id: OperatorId,
        found: StateSchemaVersion,
        supported: Vec<StateSchemaVersion>,
    },
    RuleIdentityCollision(RuleId),
    MigrationFailed {
        operator_id: OperatorId,
        reason: String,
    },
}

impl std::fmt::Display for SavepointError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedFormatVersion(v) => {
                write!(f, "unsupported savepoint format version {v}")
            }
            Self::CorruptManifest(msg) => write!(f, "corrupt savepoint manifest: {msg}"),
            Self::CorruptParticipant {
                operator_id,
                reason,
            } => write!(
                f,
                "corrupt participant `{}`: {reason}",
                operator_id.as_str()
            ),
            Self::MissingParticipant(op) => {
                write!(f, "missing required participant `{}`", op.as_str())
            }
            Self::MissingExtension(ext) => write!(f, "missing required extension `{ext}`"),
            Self::IncompatibleSchema {
                operator_id,
                found,
                supported,
            } => write!(
                f,
                "incompatible state schema for `{}`: found v{}, supported {:?}",
                operator_id.as_str(),
                found.0,
                supported.iter().map(|v| v.0).collect::<Vec<_>>()
            ),
            Self::RuleIdentityCollision(rule_id) => write!(
                f,
                "restore target rule `{}` collides with existing rule identity",
                rule_id.as_str()
            ),
            Self::MigrationFailed {
                operator_id,
                reason,
            } => write!(
                f,
                "migration failed for `{}`: {reason}",
                operator_id.as_str()
            ),
        }
    }
}

impl std::error::Error for SavepointError {}

/// Source-position lineage retained across savepoint export/restore so the new
/// rule knows where it resumed from without mutating the source rule's backlog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourcePositionLineage {
    pub source_id: String,
    pub cursor_json: serde_json::Value,
    pub epoch: u64,
}

/// Consistent version pinning inside an immutable savepoint manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SavepointVersions {
    pub catalog_version: String,
    pub state_format_version: u32,
    pub schema_version: String,
    pub effect_version: String,
    pub required_extensions: Vec<String>,
}

/// Immutable savepoint manifest referencing consistent catalog/state/schema/extension/effect
/// versions and participant snapshots with SHA-256 verification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SavepointManifest {
    pub format_version: u32,
    pub savepoint_id: String,
    pub origin_rule_id: RuleId,
    pub origin_generation: Generation,
    pub checkpoint_id: CheckpointId,
    pub plan_fingerprint: ContentChecksum,
    pub versions: SavepointVersions,
    pub source_lineage: Vec<SourcePositionLineage>,
    pub participant_checksums: BTreeMap<OperatorId, ContentChecksum>,
    pub manifest_checksum: ContentChecksum,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SavepointBundle {
    pub manifest: SavepointManifest,
    pub snapshots: BTreeMap<OperatorId, StateSnapshot>,
}

impl SavepointBundle {
    /// Exports an immutable savepoint bundle, computing participant and manifest checksums.
    #[allow(clippy::too_many_arguments)]
    pub fn export(
        savepoint_id: impl Into<String>,
        origin_rule_id: RuleId,
        origin_generation: Generation,
        checkpoint_id: CheckpointId,
        plan_fingerprint: ContentChecksum,
        versions: SavepointVersions,
        source_lineage: Vec<SourcePositionLineage>,
        snapshots: Vec<StateSnapshot>,
    ) -> Result<Self, SavepointError> {
        let mut snap_map = BTreeMap::new();
        let mut participant_checksums = BTreeMap::new();

        for snap in snapshots {
            let payload_digest = sha256_checksum(&snap.payload);
            if payload_digest != snap.checksum {
                return Err(SavepointError::CorruptParticipant {
                    operator_id: snap.operator_id,
                    reason: "snapshot payload does not match its checksum".to_string(),
                });
            }
            participant_checksums.insert(snap.operator_id.clone(), snap.checksum.clone());
            snap_map.insert(snap.operator_id.clone(), snap);
        }

        let mut manifest = SavepointManifest {
            format_version: SAVEPOINT_FORMAT_VERSION,
            savepoint_id: savepoint_id.into(),
            origin_rule_id,
            origin_generation,
            checkpoint_id,
            plan_fingerprint,
            versions,
            source_lineage,
            participant_checksums,
            manifest_checksum: sha256_checksum(b"pending"),
        };
        manifest.manifest_checksum = compute_manifest_checksum(&manifest)?;

        Ok(Self {
            manifest,
            snapshots: snap_map,
        })
    }

    pub fn validate_integrity(&self) -> Result<(), SavepointError> {
        if self.manifest.format_version != SAVEPOINT_FORMAT_VERSION {
            return Err(SavepointError::UnsupportedFormatVersion(
                self.manifest.format_version,
            ));
        }
        let expected_manifest_checksum = compute_manifest_checksum(&self.manifest)?;
        if expected_manifest_checksum != self.manifest.manifest_checksum {
            return Err(SavepointError::CorruptManifest(
                "manifest checksum mismatch".to_string(),
            ));
        }

        for (op_id, expected_checksum) in &self.manifest.participant_checksums {
            let Some(snap) = self.snapshots.get(op_id) else {
                return Err(SavepointError::MissingParticipant(op_id.clone()));
            };
            if &snap.operator_id != op_id {
                return Err(SavepointError::CorruptParticipant {
                    operator_id: op_id.clone(),
                    reason: format!("operator id mismatch `{}`", snap.operator_id.as_str()),
                });
            }
            if &snap.checksum != expected_checksum {
                return Err(SavepointError::CorruptParticipant {
                    operator_id: op_id.clone(),
                    reason: "manifest participant checksum mismatch".to_string(),
                });
            }
            let actual_payload_checksum = sha256_checksum(&snap.payload);
            if actual_payload_checksum != snap.checksum {
                return Err(SavepointError::CorruptParticipant {
                    operator_id: op_id.clone(),
                    reason: "snapshot payload checksum mismatch".to_string(),
                });
            }
        }
        Ok(())
    }
}

fn compute_manifest_checksum(
    manifest: &SavepointManifest,
) -> Result<ContentChecksum, SavepointError> {
    #[derive(Serialize)]
    struct ManifestDigestInput<'a> {
        format_version: u32,
        savepoint_id: &'a str,
        origin_rule_id: &'a RuleId,
        origin_generation: Generation,
        checkpoint_id: CheckpointId,
        plan_fingerprint: &'a ContentChecksum,
        versions: &'a SavepointVersions,
        source_lineage: &'a [SourcePositionLineage],
        participant_checksums: &'a BTreeMap<OperatorId, ContentChecksum>,
    }

    let bytes = serde_json::to_vec(&ManifestDigestInput {
        format_version: manifest.format_version,
        savepoint_id: &manifest.savepoint_id,
        origin_rule_id: &manifest.origin_rule_id,
        origin_generation: manifest.origin_generation,
        checkpoint_id: manifest.checkpoint_id,
        plan_fingerprint: &manifest.plan_fingerprint,
        versions: &manifest.versions,
        source_lineage: &manifest.source_lineage,
        participant_checksums: &manifest.participant_checksums,
    })
    .map_err(|e| SavepointError::CorruptManifest(e.to_string()))?;

    Ok(sha256_checksum(&bytes))
}

/// Explicit operator migration function from an older `StateSchemaVersion`
/// to a target `StateSchemaVersion`.
pub type StateMigrationFn = fn(&StateSnapshot) -> Result<StateSnapshot, StateError>;

#[derive(Default)]
pub struct MigrationRegistry {
    migrations: BTreeMap<(OperatorId, u32, u32), StateMigrationFn>,
}

impl MigrationRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(
        &mut self,
        operator_id: OperatorId,
        from_version: StateSchemaVersion,
        to_version: StateSchemaVersion,
        migrator: StateMigrationFn,
    ) {
        self.migrations
            .insert((operator_id, from_version.0, to_version.0), migrator);
    }

    pub fn migrate_if_needed(
        &self,
        snapshot: &StateSnapshot,
        supported_versions: &[StateSchemaVersion],
    ) -> Result<StateSnapshot, SavepointError> {
        if supported_versions.contains(&snapshot.state_version) {
            return Ok(snapshot.clone());
        }
        for target in supported_versions {
            if let Some(migrator) = self.migrations.get(&(
                snapshot.operator_id.clone(),
                snapshot.state_version.0,
                target.0,
            )) {
                let migrated = migrator(snapshot).map_err(|e| SavepointError::MigrationFailed {
                    operator_id: snapshot.operator_id.clone(),
                    reason: e.to_string(),
                })?;
                if sha256_checksum(&migrated.payload) != migrated.checksum {
                    return Err(SavepointError::MigrationFailed {
                        operator_id: snapshot.operator_id.clone(),
                        reason: "migrated snapshot checksum mismatch".to_string(),
                    });
                }
                return Ok(migrated);
            }
        }
        Err(SavepointError::IncompatibleSchema {
            operator_id: snapshot.operator_id.clone(),
            found: snapshot.state_version,
            supported: supported_versions.to_vec(),
        })
    }
}

/// Target environment descriptor used to validate before mutating any state.
#[derive(Debug, Clone)]
pub struct RestoreTargetSpec {
    pub new_rule_id: RuleId,
    pub existing_rule_ids: BTreeSet<RuleId>,
    pub available_extensions: BTreeSet<String>,
    pub required_participants: BTreeMap<OperatorId, Vec<StateSchemaVersion>>,
}

/// Restored rule plan produced only after all validation and migrations succeed;
/// original savepoint bundle remains untouched and immutable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoredRulePlan {
    pub new_rule_id: RuleId,
    pub new_generation: Generation,
    pub restored_from_savepoint: String,
    pub origin_rule_id: RuleId,
    pub origin_checkpoint_id: CheckpointId,
    pub source_lineage: Vec<SourcePositionLineage>,
    pub snapshots: BTreeMap<OperatorId, StateSnapshot>,
}

/// Validates integrity, extensions, rule identity non-collision, required participants,
/// and applies explicit operator migrations before producing a restored rule plan.
pub fn prepare_restore_to_new_rule(
    bundle: &SavepointBundle,
    target: &RestoreTargetSpec,
    migrations: &MigrationRegistry,
) -> Result<RestoredRulePlan, SavepointError> {
    // 1. Validate manifest and participant checksums before anything else.
    bundle.validate_integrity()?;

    // 2. Reject target rule ID collisions (must get a deliberate new identity).
    if target.new_rule_id == bundle.manifest.origin_rule_id
        || target.existing_rule_ids.contains(&target.new_rule_id)
    {
        return Err(SavepointError::RuleIdentityCollision(
            target.new_rule_id.clone(),
        ));
    }

    // 3. Validate all required extensions are available.
    for ext in &bundle.manifest.versions.required_extensions {
        if !target.available_extensions.contains(ext) {
            return Err(SavepointError::MissingExtension(ext.clone()));
        }
    }

    // 4. Validate required participants and run explicit migrations if needed.
    let mut restored_snapshots = BTreeMap::new();
    for (op_id, supported_versions) in &target.required_participants {
        let Some(snap) = bundle.snapshots.get(op_id) else {
            return Err(SavepointError::MissingParticipant(op_id.clone()));
        };
        let migrated = migrations.migrate_if_needed(snap, supported_versions)?;
        restored_snapshots.insert(op_id.clone(), migrated);
    }

    Ok(RestoredRulePlan {
        new_rule_id: target.new_rule_id.clone(),
        new_generation: Generation(1),
        restored_from_savepoint: bundle.manifest.savepoint_id.clone(),
        origin_rule_id: bundle.manifest.origin_rule_id.clone(),
        origin_checkpoint_id: bundle.manifest.checkpoint_id,
        source_lineage: bundle.manifest.source_lineage.clone(),
        snapshots: restored_snapshots,
    })
}
