use rekuiper_server::engine::time::watermark::WatermarkStateMachine;

#[test]
fn unseen_active_input_holds_progress_and_closed_input_releases_it() {
    let mut wm = WatermarkStateMachine::new(["s1".to_string(), "s2".to_string()], 100);
    assert_eq!(wm.watermark_ms(), i64::MIN);

    // s1 advances to 1_000, but s2 is unseen so watermark stays held at i64::MIN
    assert_eq!(wm.observe_event("s1", 1_000), None);
    assert_eq!(wm.watermark_ms(), i64::MIN);

    // s2 observes 500 -> watermark advances to min(1000, 500) - 100 = 400
    assert_eq!(wm.observe_event("s2", 500), Some(400));
    assert_eq!(wm.watermark_ms(), 400);

    // s1 advances to 2_000, s2 is still at 500 -> watermark stays 400
    assert_eq!(wm.observe_event("s1", 2_000), None);
    assert_eq!(wm.watermark_ms(), 400);

    // Closing s2 stops holding back s1 -> watermark advances to 2000 - 100 = 1900
    assert_eq!(wm.close_input("s2"), Some(1_900));
    assert_eq!(wm.watermark_ms(), 1_900);
    assert!(wm.is_closed("s2"));
}

#[test]
fn watermark_never_regresses_or_overflows_and_advances_on_where_rejected_rows() {
    let mut wm = WatermarkStateMachine::new(["s1".to_string()], 500);

    // Near i64::MIN: saturating_sub must not overflow/wrap
    assert_eq!(wm.observe_event("s1", i64::MIN + 10), Some(i64::MIN));
    assert_eq!(wm.watermark_ms(), i64::MIN);

    // Advance even if the caller later drops the row in WHERE
    let where_matched = false;
    let advanced = wm.observe_event("s1", 1_000);
    if !where_matched {
        // Row rejected after timestamp observation, watermark still advanced to 500
        assert_eq!(advanced, Some(500));
    }
    assert_eq!(wm.watermark_ms(), 500);

    // Out-of-order / older timestamp never regresses watermark
    assert_eq!(wm.observe_event("s1", 600), None);
    assert_eq!(wm.watermark_ms(), 500);

    // Late event (< watermark) is flagged late and does not regress
    assert!(wm.is_late(499));
    assert_eq!(wm.observe_event("s1", 499), None);
    assert_eq!(wm.watermark_ms(), 500);

    // Near i64::MAX: saturating arithmetic does not overflow
    assert_eq!(wm.observe_event("s1", i64::MAX), Some(i64::MAX - 500));
}
