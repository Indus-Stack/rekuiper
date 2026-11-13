use rekuiper_core::contracts::{
    EventTimestamp, FixtureDocument, FixtureError, FixtureStep, FIXTURE_SCHEMA_VERSION,
};

#[test]
fn parses_tiny_transform_and_tumbling_window_fixtures() {
    let tiny_raw = include_str!("../../../test/fixtures/contracts/tiny_transform.json");
    let tiny = FixtureDocument::from_json_str(tiny_raw).expect("valid tiny_transform fixture");
    assert_eq!(tiny.schema_version, FIXTURE_SCHEMA_VERSION);
    assert_eq!(tiny.steps.len(), 3);
    assert_eq!(tiny.expected.rows.len(), 1);
    assert_eq!(tiny.expected.effects.len(), 1);

    // Verify input ordering and exact OTLP nanosecond precision are preserved.
    match &tiny.steps[0] {
        FixtureStep::Input {
            event_time: Some(EventTimestamp::OtlpNanos(ns)),
            ..
        } => assert_eq!(*ns, 1_700_000_000_123_456_789),
        other => panic!("unexpected first step: {other:?}"),
    }
    match &tiny.steps[1] {
        FixtureStep::Input {
            event_time: Some(EventTimestamp::OtlpNanos(ns)),
            ..
        } => assert_eq!(*ns, 1_700_000_001_987_654_321),
        other => panic!("unexpected second step: {other:?}"),
    }

    let window_raw = include_str!("../../../test/fixtures/contracts/tumbling_window.json");
    let window = FixtureDocument::from_json_str(window_raw).expect("valid tumbling_window fixture");
    assert_eq!(window.steps.len(), 7);
    assert_eq!(window.expected.rows[0].payload["total"], 25);
}

#[test]
fn rejects_unknown_step_kinds_with_line_and_column() {
    let raw = include_str!("../../../test/fixtures/contracts/tiny_transform.json")
        .replace("\"kind\": \"end_of_input\"", "\"kind\": \"magic_step\"");
    let err = FixtureDocument::from_json_str(&raw).expect_err("unknown step kind must fail");
    match err {
        FixtureError::InvalidJson { line, column, .. } => {
            assert!(line > 1, "expected non-zero line, got {line}");
            assert!(column > 0, "expected non-zero column, got {column}");
        }
        other => panic!("expected InvalidJson with location, got {other:?}"),
    }
}

#[test]
fn rejects_contradictory_and_invalid_fixtures() {
    // allow_no_outputs: true while expected.rows is non-empty
    let contradictory = include_str!("../../../test/fixtures/contracts/tiny_transform.json")
        .replace(
            "\"rows\": [",
            "\"allow_no_outputs\": true,\n    \"rows\": [",
        );
    let err = FixtureDocument::from_json_str(&contradictory)
        .expect_err("contradictory expectations must fail");
    assert!(matches!(err, FixtureError::Validation(_)));

    // Restore referencing a checkpoint_id that was never created
    let bad_restore = include_str!("../../../test/fixtures/contracts/tumbling_window.json")
        .replace(
            "\"checkpoint_id\": 1\n    },\n    {\n      \"kind\": \"advance_clock\"",
            "\"checkpoint_id\": 99\n    },\n    {\n      \"kind\": \"advance_clock\"",
        );
    let err = FixtureDocument::from_json_str(&bad_restore)
        .expect_err("unknown restore checkpoint must fail");
    assert!(matches!(err, FixtureError::Validation(_)));
}
