use rekuiper_core::contracts::{ContentChecksum, OperatorId, StateError, StateParticipant};
use rekuiper_server::engine::state::{
    HoppingWindowStateParticipant, MissedDeadlinePolicy, SlidingWindowStateParticipant,
};
use serde_json::json;

fn fp(ch: char) -> ContentChecksum {
    let hex: String = std::iter::repeat_n(ch, 64).collect();
    ContentChecksum::try_from(format!("sha256:{hex}")).unwrap()
}

#[test]
fn hopping_window_preserves_overlapping_membership_and_equals_uninterrupted_across_restart() {
    let op = OperatorId::try_from("hop_1").unwrap();
    let fp_a = fp('a');

    // Uninterrupted run: length=1000ms, hop=500ms
    let mut uninterrupted = HoppingWindowStateParticipant::new(op.clone(), fp_a.clone(), 1000, 500);
    uninterrupted.insert_row(1, 200, json!({"v": 1}));
    uninterrupted.insert_row(2, 600, json!({"v": 2}));
    uninterrupted.insert_row(3, 1100, json!({"v": 3}));
    let expected = uninterrupted.fire_due(2500, MissedDeadlinePolicy::FireImmediatelyInOrder);

    // Restarted run: snapshot after row 2, restore into fresh participant, insert row 3, fire
    let mut part1 = HoppingWindowStateParticipant::new(op.clone(), fp_a.clone(), 1000, 500);
    part1.insert_row(1, 200, json!({"v": 1}));
    part1.insert_row(2, 600, json!({"v": 2}));
    let snap = part1.capture_snapshot().expect("capture hop snapshot");

    let mut part2 = HoppingWindowStateParticipant::new(op, fp_a, 1000, 500);
    part2.restore_snapshot(&snap).expect("restore hop snapshot");
    part2.insert_row(3, 1100, json!({"v": 3}));
    let actual = part2.fire_due(2500, MissedDeadlinePolicy::FireImmediatelyInOrder);

    assert_eq!(actual, expected);
    // Row at 600ms belongs to both [0, 1000) and [500, 1500)
    assert!(actual
        .iter()
        .any(|b| b.window_start_ms == 0 && b.rows.len() == 2));
    assert!(actual
        .iter()
        .any(|b| b.window_start_ms == 500 && b.rows.len() == 2));
}

#[test]
fn sliding_window_obeys_missed_deadline_policy_and_rejects_corrupt_payload() {
    let op = OperatorId::try_from("slide_1").unwrap();
    let fp_b = fp('b');

    let mut slide = SlidingWindowStateParticipant::new(op.clone(), fp_b.clone(), 1000, 100);
    slide.insert_row(1, 1000, json!({"id": 1}));
    slide.insert_row(2, 1500, json!({"id": 2}));
    let mut snap = slide.capture_snapshot().expect("capture slide snapshot");

    // Corrupt payload fails before intake.
    snap.payload[0] ^= 0xff;
    let mut corrupt_target =
        SlidingWindowStateParticipant::new(op.clone(), fp_b.clone(), 1000, 100);
    assert!(matches!(
        corrupt_target.restore_snapshot(&snap),
        Err(StateError::CorruptSnapshot(_))
    ));

    // Valid restore fires due windows according to MissedDeadlinePolicy.
    let valid_snap = slide.capture_snapshot().unwrap();
    let mut restored_fire = SlidingWindowStateParticipant::new(op.clone(), fp_b.clone(), 1000, 100);
    restored_fire.restore_snapshot(&valid_snap).unwrap();
    let fired = restored_fire.fire_due(5000, MissedDeadlinePolicy::FireImmediatelyInOrder);
    assert_eq!(fired.len(), 2);
    assert_eq!(fired[0].rows, vec![json!({"id": 1})]);
    assert_eq!(fired[1].rows, vec![json!({"id": 1}), json!({"id": 2})]);

    let mut restored_drop = SlidingWindowStateParticipant::new(op, fp_b, 1000, 100);
    restored_drop.restore_snapshot(&valid_snap).unwrap();
    let dropped = restored_drop.fire_due(5000, MissedDeadlinePolicy::DropExpired);
    assert!(dropped.is_empty());
}
