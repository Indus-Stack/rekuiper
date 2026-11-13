use rekuiper_core::contracts::{ContentChecksum, OperatorId, StateError, StateParticipant};
use rekuiper_server::engine::state::hop_slide::MissedDeadlinePolicy;
use rekuiper_server::engine::state::session_state::{
    SessionWindowStateParticipant, StateWindowStateParticipant,
};
use serde_json::json;

fn op_id(name: &str) -> OperatorId {
    OperatorId::try_from(name.to_string()).unwrap()
}

fn fp(tag: &str) -> ContentChecksum {
    ContentChecksum::try_from(format!("sha256:{tag}")).unwrap()
}

#[test]
fn session_window_exact_gap_boundary_and_restart_match_uninterrupted() {
    let mut uninterrupted = SessionWindowStateParticipant::new(
        op_id("session_op"),
        fp("plan_s1"),
        1_000,
        Some(2_500),
        MissedDeadlinePolicy::FireImmediatelyInOrder,
    )
    .unwrap();

    let mut before = SessionWindowStateParticipant::new(
        op_id("session_op"),
        fp("plan_s1"),
        1_000,
        Some(2_500),
        MissedDeadlinePolicy::FireImmediatelyInOrder,
    )
    .unwrap();

    let mut uninterrupted_out = Vec::new();
    let mut split_out = Vec::new();

    // Key A: 100, 1100 (exact gap == 1000 -> same session), 2101 (gap > 1000 -> closes first, opens second)
    // Key B: 200, 1100 (stays open across restart)
    for (key, ts, val) in [
        ("A", 100_i64, json!({"v": 1})),
        ("B", 200, json!({"v": 10})),
        ("A", 1_100, json!({"v": 2})),
    ] {
        if let Some(b) = uninterrupted.ingest(key, ts, val.clone()) {
            uninterrupted_out.push(b);
        }
        if let Some(b) = before.ingest(key, ts, val) {
            split_out.push(b);
        }
    }

    assert_eq!(before.open_session_keys(), vec!["A", "B"]);
    assert_eq!(before.open_row_count("A"), 2);
    assert_eq!(before.open_deadline("A").unwrap().at_ms, 2_100);

    let snap = before.capture().unwrap();
    let mut after = SessionWindowStateParticipant::new(
        op_id("session_op"),
        fp("plan_s1"),
        1_000,
        Some(2_500),
        MissedDeadlinePolicy::FireImmediatelyInOrder,
    )
    .unwrap();
    after.restore(&snap).unwrap();

    assert_eq!(after.open_session_keys(), vec!["A", "B"]);
    assert_eq!(after.open_deadline("A").unwrap().at_ms, 2_100);

    for (key, ts, val) in [
        ("A", 2_101_i64, json!({"v": 3})), // gap 1001 > 1000 -> closes A[100..2100]
        ("B", 2_700, json!({"v": 11})),    // 2700 - 200 == 2500 >= max_duration -> closes B
    ] {
        if let Some(b) = uninterrupted.ingest(key, ts, val.clone()) {
            uninterrupted_out.push(b);
        }
        if let Some(b) = after.ingest(key, ts, val) {
            split_out.push(b);
        }
    }

    uninterrupted_out.extend(uninterrupted.fire_due(5_000));
    split_out.extend(after.fire_due(5_000));

    assert_eq!(split_out, uninterrupted_out);
    assert_eq!(split_out.len(), 4);
    assert_eq!(
        split_out[0].batch.rows,
        vec![json!({"v": 1}), json!({"v": 2})]
    );
}

#[test]
fn session_window_expired_deadlines_obey_downtime_policy() {
    let mut src = SessionWindowStateParticipant::new(
        op_id("session_policy"),
        fp("plan_s2"),
        500,
        None,
        MissedDeadlinePolicy::FireImmediatelyInOrder,
    )
    .unwrap();
    src.ingest("k1", 1_000, json!({"x": 1}));
    src.ingest("k2", 2_000, json!({"x": 2}));
    let snap = src.capture().unwrap();

    let mut fire = SessionWindowStateParticipant::new(
        op_id("session_policy"),
        fp("plan_s2"),
        500,
        None,
        MissedDeadlinePolicy::FireImmediatelyInOrder,
    )
    .unwrap();
    fire.restore(&snap).unwrap();
    let fired = fire.reconcile_after_restore(2_000);
    assert_eq!(fired.len(), 1);
    assert_eq!(fired[0].partition_key, "k1");
    assert_eq!(fire.open_session_keys(), vec!["k2"]);

    let mut drop_expired = SessionWindowStateParticipant::new(
        op_id("session_policy"),
        fp("plan_s2"),
        500,
        None,
        MissedDeadlinePolicy::DropExpired,
    )
    .unwrap();
    drop_expired.restore(&snap).unwrap();
    let dropped = drop_expired.reconcile_after_restore(2_000);
    assert!(dropped.is_empty());
    assert_eq!(drop_expired.open_session_keys(), vec!["k2"]);
}

#[test]
fn state_window_transition_and_plan_mismatch_check() {
    let mut uninterrupted =
        StateWindowStateParticipant::new(op_id("state_op"), fp("plan_st1"), true);
    let mut before = StateWindowStateParticipant::new(op_id("state_op"), fp("plan_st1"), true);

    let mut u_out = Vec::new();
    let mut s_out = Vec::new();

    // First row ignored (start=false), second opens window (start=true), third stays in window
    for (ts, val, start_m, end_m) in [
        (10_i64, json!({"s": "idle"}), false, false),
        (20, json!({"s": "open"}), true, false),
        (30, json!({"s": "mid"}), false, false),
    ] {
        if let Some(b) = uninterrupted.ingest_evaluated("p1", ts, val.clone(), start_m, end_m) {
            u_out.push(b);
        }
        if let Some(b) = before.ingest_evaluated("p1", ts, val, start_m, end_m) {
            s_out.push(b);
        }
    }

    assert!(before.is_open("p1"));
    assert_eq!(before.retained_row_count("p1"), 2);

    let mut snap = before.capture().unwrap();
    let mut after = StateWindowStateParticipant::new(op_id("state_op"), fp("plan_st1"), true);
    after.restore(&snap).unwrap();
    assert!(after.is_open("p1"));
    assert_eq!(after.retained_row_count("p1"), 2);

    // Finish window after restart without re-evaluating historical start predicate
    if let Some(b) = uninterrupted.ingest_evaluated("p1", 40, json!({"s": "close"}), false, true) {
        u_out.push(b);
    }
    if let Some(b) = after.ingest_evaluated("p1", 40, json!({"s": "close"}), false, true) {
        s_out.push(b);
    }
    assert_eq!(s_out, u_out);
    assert_eq!(s_out.len(), 1);
    assert_eq!(
        s_out[0].batch.rows,
        vec![
            json!({"s": "open"}),
            json!({"s": "mid"}),
            json!({"s": "close"})
        ]
    );

    // Reject plan fingerprint mismatch and corrupt payload checksum
    let mut wrong_plan = StateWindowStateParticipant::new(op_id("state_op"), fp("other"), true);
    assert!(matches!(
        wrong_plan.restore(&snap),
        Err(StateError::CorruptSnapshot(_))
    ));
    snap.payload[0] ^= 0xFF;
    assert!(matches!(
        after.restore(&snap),
        Err(StateError::CorruptSnapshot(_))
    ));
}
