use rekuiper_core::contracts::{
    CheckpointId, ContentChecksum, Generation, OperatorId, RuleId, StateError, StateResourceUsage,
    StateSchemaVersion, StateSnapshot, STATE_ENVELOPE_VERSION,
};
use rekuiper_core::reliability::{
    prepare_restore_to_new_rule, MigrationRegistry, RestoreTargetSpec, SavepointBundle,
    SavepointError, SavepointVersions, SourcePositionLineage,
};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};

fn sha256(bytes: &[u8]) -> ContentChecksum {
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
    ContentChecksum::try_from(format!("sha256:{hex}")).unwrap()
}

fn make_snapshot(op: &str, version: u32, payload: Vec<u8>) -> StateSnapshot {
    let checksum = sha256(&payload);
    let len = payload.len() as u64;
    StateSnapshot {
        envelope_version: STATE_ENVELOPE_VERSION,
        operator_id: OperatorId::try_from(op.to_string()).unwrap(),
        state_version: StateSchemaVersion(version),
        checksum,
        payload,
        usage: StateResourceUsage {
            records: 1,
            bytes: len,
        },
    }
}

fn base_versions() -> SavepointVersions {
    SavepointVersions {
        catalog_version: "1.0.0".to_string(),
        state_format_version: 1,
        schema_version: "2026-11".to_string(),
        effect_version: "1".to_string(),
        required_extensions: vec!["ext_geo".to_string()],
    }
}

fn migrate_v1_to_v2(snap: &StateSnapshot) -> Result<StateSnapshot, StateError> {
    let mut val: serde_json::Value = serde_json::from_slice(&snap.payload)
        .map_err(|e| StateError::CorruptSnapshot(e.to_string()))?;
    val["migrated"] = json!(true);
    let payload = serde_json::to_vec(&val).unwrap();
    Ok(make_snapshot(snap.operator_id.as_str(), 2, payload))
}

#[test]
fn savepoint_export_migration_and_restore_to_new_rule_preserve_immutability() {
    let origin_rule = RuleId::try_from("rule_orig".to_string()).unwrap();
    let new_rule = RuleId::try_from("rule_cloned".to_string()).unwrap();
    let op1 = OperatorId::try_from("win_1".to_string()).unwrap();

    let snap_v1 = make_snapshot(
        "win_1",
        1,
        serde_json::to_vec(&json!({"count": 7})).unwrap(),
    );
    let lineage = vec![SourcePositionLineage {
        source_id: "kafka_in".to_string(),
        cursor_json: json!({"partition": 0, "next_offset": 42}),
        epoch: 3,
    }];

    let bundle = SavepointBundle::export(
        "sp-001",
        origin_rule.clone(),
        Generation(4),
        CheckpointId(12),
        sha256(b"plan_v1"),
        base_versions(),
        lineage.clone(),
        vec![snap_v1],
    )
    .unwrap();

    // Missing extension rejects before any restore mutation
    let mut missing_ext_target = RestoreTargetSpec {
        new_rule_id: new_rule.clone(),
        existing_rule_ids: BTreeSet::new(),
        available_extensions: BTreeSet::new(),
        required_participants: BTreeMap::from([(op1.clone(), vec![StateSchemaVersion(2)])]),
    };
    let registry = MigrationRegistry::new();
    assert!(matches!(
        prepare_restore_to_new_rule(&bundle, &missing_ext_target, &registry),
        Err(SavepointError::MissingExtension(ref e)) if e == "ext_geo"
    ));

    // Rule identity collision rejects before restore
    missing_ext_target
        .available_extensions
        .insert("ext_geo".to_string());
    let mut collision_target = missing_ext_target.clone();
    collision_target.new_rule_id = origin_rule.clone();
    assert!(matches!(
        prepare_restore_to_new_rule(&bundle, &collision_target, &registry),
        Err(SavepointError::RuleIdentityCollision(_))
    ));

    // Register v1 -> v2 migration and restore to deliberate new rule identity
    let mut mig = MigrationRegistry::new();
    mig.register(
        op1.clone(),
        StateSchemaVersion(1),
        StateSchemaVersion(2),
        migrate_v1_to_v2,
    );
    let restored = prepare_restore_to_new_rule(&bundle, &missing_ext_target, &mig).unwrap();

    assert_eq!(restored.new_rule_id, new_rule);
    assert_eq!(restored.origin_rule_id, origin_rule);
    assert_eq!(restored.new_generation, Generation(1));
    assert_eq!(restored.source_lineage, lineage);
    let migrated_snap = restored.snapshots.get(&op1).unwrap();
    assert_eq!(migrated_snap.state_version, StateSchemaVersion(2));

    // Original savepoint remains untouched and readable at v1
    assert_eq!(
        bundle.snapshots.get(&op1).unwrap().state_version,
        StateSchemaVersion(1)
    );
    bundle.validate_integrity().unwrap();
}

#[test]
fn savepoint_rejects_corrupt_or_missing_participant() {
    let origin_rule = RuleId::try_from("rule_a".to_string()).unwrap();
    let new_rule = RuleId::try_from("rule_b".to_string()).unwrap();
    let op1 = OperatorId::try_from("op_1".to_string()).unwrap();
    let op_missing = OperatorId::try_from("op_2".to_string()).unwrap();

    let snap = make_snapshot("op_1", 1, b"{\"ok\":1}".to_vec());
    let mut bundle = SavepointBundle::export(
        "sp-002",
        origin_rule,
        Generation(1),
        CheckpointId(5),
        sha256(b"plan"),
        SavepointVersions {
            required_extensions: vec![],
            ..base_versions()
        },
        vec![],
        vec![snap],
    )
    .unwrap();

    // Missing required participant
    let target_missing = RestoreTargetSpec {
        new_rule_id: new_rule.clone(),
        existing_rule_ids: BTreeSet::new(),
        available_extensions: BTreeSet::new(),
        required_participants: BTreeMap::from([(op_missing, vec![StateSchemaVersion(1)])]),
    };
    assert!(matches!(
        prepare_restore_to_new_rule(&bundle, &target_missing, &MigrationRegistry::new()),
        Err(SavepointError::MissingParticipant(_))
    ));

    // Corrupt snapshot payload
    bundle.snapshots.get_mut(&op1).unwrap().payload[0] ^= 0xFF;
    let target_ok = RestoreTargetSpec {
        new_rule_id: new_rule,
        existing_rule_ids: BTreeSet::new(),
        available_extensions: BTreeSet::new(),
        required_participants: BTreeMap::from([(op1, vec![StateSchemaVersion(1)])]),
    };
    assert!(matches!(
        prepare_restore_to_new_rule(&bundle, &target_ok, &MigrationRegistry::new()),
        Err(SavepointError::CorruptParticipant { .. })
    ));
}
