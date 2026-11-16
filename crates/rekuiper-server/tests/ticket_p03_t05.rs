use rekuiper_core::contracts::{
    ContentChecksum, OperatorId, StateError, StateParticipant, StateSchemaVersion,
};
use rekuiper_server::engine::state::{
    AnalyticStateParticipant, CountWindowStateParticipant, TumbleWindowStateParticipant,
};
use serde_json::json;

fn checksum(ch: char) -> ContentChecksum {
    let hex: String = std::iter::repeat_n(ch, 64).collect();
    ContentChecksum::try_from(format!("sha256:{hex}")).unwrap()
}

#[test]
fn count_and_tumble_windows_match_uninterrupted_execution_when_split_at_every_boundary() {
    let fp = checksum('a');
    let count_op = OperatorId::try_from("count_w").unwrap();
    let events = vec![json!(1), json!(2), json!(3), json!(4), json!(5), json!(6)];

    // Uninterrupted count window (size=3, hop=2).
    let mut baseline_count = CountWindowStateParticipant::new(count_op.clone(), fp.clone(), 3, 2);
    let mut expected_batches = Vec::new();
    for ev in &events {
        if let Some(batch) = baseline_count.push_row(ev.clone()) {
            expected_batches.push(batch);
        }
    }

    // Split at every single event boundary, snapshot -> recreate -> restore -> resume.
    for split_idx in 0..=events.len() {
        let mut first = CountWindowStateParticipant::new(count_op.clone(), fp.clone(), 3, 2);
        let mut actual_batches = Vec::new();
        for ev in &events[..split_idx] {
            if let Some(batch) = first.push_row(ev.clone()) {
                actual_batches.push(batch);
            }
        }
        let snap = first.capture_snapshot().unwrap();
        let mut restored = CountWindowStateParticipant::new(count_op.clone(), fp.clone(), 3, 2);
        restored.restore_snapshot(&snap).unwrap();
        for ev in &events[split_idx..] {
            if let Some(batch) = restored.push_row(ev.clone()) {
                actual_batches.push(batch);
            }
        }
        assert_eq!(actual_batches, expected_batches, "split at {split_idx}");
    }

    // Tumbling event-time window (1000ms) split across events and watermark advances.
    let tumble_op = OperatorId::try_from("tumble_w").unwrap();
    let timed_events = vec![(100, json!("a")), (500, json!("b")), (1200, json!("c"))];
    let mut baseline_tumble =
        TumbleWindowStateParticipant::new(tumble_op.clone(), fp.clone(), 1000, true);
    for (ts, val) in &timed_events {
        baseline_tumble.push_event(*ts, val.clone());
    }
    let expected_tumble = baseline_tumble.advance_watermark(2000);

    for split_idx in 0..=timed_events.len() {
        let mut first =
            TumbleWindowStateParticipant::new(tumble_op.clone(), fp.clone(), 1000, true);
        for (ts, val) in &timed_events[..split_idx] {
            first.push_event(*ts, val.clone());
        }
        let snap = first.capture_snapshot().unwrap();
        let mut restored =
            TumbleWindowStateParticipant::new(tumble_op.clone(), fp.clone(), 1000, true);
        restored.restore_snapshot(&snap).unwrap();
        for (ts, val) in &timed_events[split_idx..] {
            restored.push_event(*ts, val.clone());
        }
        let actual_tumble = restored.advance_watermark(2000);
        assert_eq!(
            actual_tumble, expected_tumble,
            "tumble split at {split_idx}"
        );
    }
}

#[test]
fn rejects_mismatched_plan_and_state_version_and_restores_analytic_state() {
    let op = OperatorId::try_from("analytic_1").unwrap();
    let mut part = AnalyticStateParticipant::new(op.clone(), checksum('a'));
    part.record_value("last_temp", json!(42.5));
    let mut snap = part.capture_snapshot().unwrap();

    // Wrong plan fingerprint fails restore.
    let mut wrong_plan = AnalyticStateParticipant::new(op.clone(), checksum('b'));
    assert!(matches!(
        wrong_plan.restore_snapshot(&snap),
        Err(StateError::CorruptSnapshot(_))
    ));

    // Wrong state version fails restore.
    snap.state_version = StateSchemaVersion(99);
    assert!(matches!(
        part.restore_snapshot(&snap),
        Err(StateError::UnsupportedStateVersion { .. })
    ));

    // Valid restore preserves accumulators and row_count.
    snap.state_version = StateSchemaVersion(1);
    let mut restored = AnalyticStateParticipant::new(op, checksum('a'));
    restored.restore_snapshot(&snap).unwrap();
    assert_eq!(restored.get("last_temp"), Some(&json!(42.5)));
    assert_eq!(restored.row_count(), 1);
}
