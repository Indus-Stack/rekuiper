use rekuiper_core::contracts::{ContentChecksum, OperatorId, StateError, StateParticipant};
use rekuiper_server::engine::state::joins_cep::{
    CepSequenceStateParticipant, JoinBufferStateParticipant, JoinSnapshotKind,
};
use serde_json::json;

fn op_id(name: &str) -> OperatorId {
    OperatorId::try_from(name.to_string()).unwrap()
}

fn fp(hex_digit: char) -> ContentChecksum {
    let hex: String = std::iter::repeat_n(hex_digit, 64).collect();
    ContentChecksum::try_from(format!("sha256:{hex}")).unwrap()
}

#[test]
fn join_buffer_restart_preserves_multiplicity_rebuilds_indexes_and_keeps_expiry() {
    let mut uninterrupted = JoinBufferStateParticipant::new(
        op_id("join_op"),
        fp('1'),
        fp('a'),
        JoinSnapshotKind::Inner,
        Some(1_000),
    );
    let mut before = JoinBufferStateParticipant::new(
        op_id("join_op"),
        fp('1'),
        fp('a'),
        JoinSnapshotKind::Inner,
        Some(1_000),
    );

    let mut u_out = Vec::new();
    let mut s_out = Vec::new();

    // Two unmatched left rows with same key "k1" (multiplicity=2) and one expiring early
    for (id, k, ts, val) in [
        ("src_l", "k1", 100_i64, json!({"l": 1})),
        ("src_l", "k1", 500, json!({"l": 2})),
        ("src_l", "k_old", 10, json!({"l": 99})),
    ] {
        u_out.extend(uninterrupted.ingest_left(id, k, ts, val.clone()));
        s_out.extend(before.ingest_left(id, k, ts, val));
    }
    assert_eq!(before.unmatched_left_count(), 3);

    // Expire k_old before snapshot
    assert_eq!(uninterrupted.evict_expired(1_050), 1);
    assert_eq!(before.evict_expired(1_050), 1);

    let snap = before.capture_snapshot().unwrap();
    let mut after = JoinBufferStateParticipant::new(
        op_id("join_op"),
        fp('1'),
        fp('a'),
        JoinSnapshotKind::Inner,
        Some(1_000),
    );
    after.restore_snapshot(&snap).unwrap();
    assert_eq!(after.index_len(), (1, 0));
    assert_eq!(after.unmatched_left_count(), 2);

    // Expired row k_old must stay expired even if a late right row arrives for k_old
    u_out.extend(uninterrupted.ingest_right("src_r", "k_old", 1_060, json!({"r": 99})));
    s_out.extend(after.ingest_right("src_r", "k_old", 1_060, json!({"r": 99})));

    u_out.extend(uninterrupted.ingest_right("src_r", "k1", 1_070, json!({"r": 10})));
    s_out.extend(after.ingest_right("src_r", "k1", 1_070, json!({"r": 10})));

    assert_eq!(s_out, u_out);
    assert_eq!(s_out.len(), 2);

    // Key-schema change must be rejected without migration
    let mut wrong_key_schema = JoinBufferStateParticipant::new(
        op_id("join_op"),
        fp('1'),
        fp('b'),
        JoinSnapshotKind::Inner,
        Some(1_000),
    );
    assert!(matches!(
        wrong_key_schema.restore_snapshot(&snap),
        Err(StateError::CorruptSnapshot(_))
    ));
}

#[test]
fn cep_sequence_partial_match_survives_restart_and_rejects_plan_change() {
    let steps = vec!["A".to_string(), "B".to_string(), "C".to_string()];
    let mut uninterrupted = CepSequenceStateParticipant::new(
        op_id("cep_op"),
        fp('2'),
        fp('c'),
        steps.clone(),
        Some(500),
    )
    .unwrap();
    let mut before = CepSequenceStateParticipant::new(
        op_id("cep_op"),
        fp('2'),
        fp('c'),
        steps.clone(),
        Some(500),
    )
    .unwrap();

    let mut u_out = Vec::new();
    let mut s_out = Vec::new();

    for (key, ord, ts, src, step, val) in [
        ("dev1", 1_u64, 100_i64, "s1", "A", json!({"step": "A1"})),
        ("dev1", 2, 200, "s1", "B", json!({"step": "B1"})),
        ("dev2", 3, 50, "s2", "A", json!({"step": "A2"})), // expires at ts=600
    ] {
        u_out.extend(uninterrupted.advance(key, ord, ts, src, step, val.clone()));
        s_out.extend(before.advance(key, ord, ts, src, step, val));
    }
    assert_eq!(before.active_partial_count(), 2);

    let snap = before.capture_snapshot().unwrap();
    let mut after = CepSequenceStateParticipant::new(
        op_id("cep_op"),
        fp('2'),
        fp('c'),
        steps.clone(),
        Some(500),
    )
    .unwrap();
    after.restore_snapshot(&snap).unwrap();
    assert_eq!(after.active_key_index_len(), 2);

    // Complete dev1 after restart; dev2 expired at 550 so C at 600 must not match dev2
    u_out.extend(uninterrupted.advance("dev1", 4, 300, "s1", "C", json!({"step": "C1"})));
    s_out.extend(after.advance("dev1", 4, 300, "s1", "C", json!({"step": "C1"})));

    u_out.extend(uninterrupted.advance("dev2", 5, 600, "s2", "B", json!({"step": "B2"})));
    s_out.extend(after.advance("dev2", 5, 600, "s2", "B", json!({"step": "B2"})));

    assert_eq!(s_out, u_out);
    assert_eq!(s_out.len(), 1);
    assert_eq!(s_out[0].key, "dev1");
    assert_eq!(s_out[0].input_ids, vec!["s1", "s1", "s1"]);

    // Plan change rejected without migration
    let mut wrong_plan =
        CepSequenceStateParticipant::new(op_id("cep_op"), fp('9'), fp('c'), steps, Some(500))
            .unwrap();
    assert!(matches!(
        wrong_plan.restore_snapshot(&snap),
        Err(StateError::CorruptSnapshot(_))
    ));
}
