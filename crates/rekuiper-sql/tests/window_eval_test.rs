//! Window trigger semantics (WHERE, GROUP BY, HAVING, ORDER BY, LIMIT),
//! the row-free incremental evaluator, SESSIONWINDOW parsing and source
//! metadata visibility.

use indexmap::IndexMap;
use rekuiper_sql::{
    Evaluator, IncrementalWindow, Parser, SlidingIncrementalWindow, TimeUnit, WindowDef,
};
use serde_json::{json, Value};

type Row = IndexMap<String, Value>;

fn row(v: Value) -> Row {
    v.as_object().unwrap().clone().into_iter().collect()
}

fn parse(sql: &str) -> rekuiper_sql::SelectStmt {
    Parser::new(sql).parse_select().expect("parse")
}

/// Deterministic telemetry: several devices, ints and floats, nulls and a
/// non-numeric value mixed in.
fn telemetry(n: usize) -> Vec<Row> {
    (0..n)
        .map(|i| {
            let temp = if i % 11 == 0 {
                Value::Null
            } else if i % 7 == 0 {
                json!("bad")
            } else if i % 2 == 0 {
                json!(i as i64 % 50)
            } else {
                json!((i % 90) as f64 / 3.0)
            };
            row(json!({
                "device": format!("dev_{}", i % 5),
                "site": if i % 3 == 0 { "north" } else { "south" },
                "temp": temp,
                "speed": (i % 130) as i64,
            }))
        })
        .collect()
}

fn sorted(mut rows: Vec<Row>) -> Vec<String> {
    let mut out: Vec<String> = rows
        .drain(..)
        .map(|r| {
            let mut keys: Vec<_> = r.into_iter().collect();
            keys.sort_by(|a, b| a.0.cmp(&b.0));
            serde_json::to_string(&keys).unwrap()
        })
        .collect();
    out.sort();
    out
}

#[test]
fn group_by_emits_one_row_per_group_in_first_seen_order() {
    let stmt = parse(
        "SELECT device, count(*) AS n, max(speed) AS top FROM telem GROUP BY device, TUMBLINGWINDOW(ss, 10)",
    );
    let rows = vec![
        row(json!({"device": "b", "speed": 5})),
        row(json!({"device": "a", "speed": 9})),
        row(json!({"device": "b", "speed": 7})),
    ];
    let out = Evaluator::eval_window(&stmt, rows);
    assert_eq!(out.len(), 2);
    assert_eq!(out[0]["device"], json!("b"));
    assert_eq!(out[0]["n"], json!(2));
    assert_eq!(out[0]["top"], json!(7));
    assert_eq!(out[1]["device"], json!("a"));
    assert_eq!(out[1]["n"], json!(1));
}

#[test]
fn where_filters_window_rows_before_aggregation() {
    let stmt = parse(
        "SELECT count(*) AS n, avg(speed) AS s FROM telem WHERE speed > 10 GROUP BY TUMBLINGWINDOW(ss, 10)",
    );
    let rows = vec![
        row(json!({"speed": 5})),
        row(json!({"speed": 20})),
        row(json!({"speed": 40})),
    ];
    let out = Evaluator::eval_window(&stmt, rows);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0]["n"], json!(2));
    assert_eq!(out[0]["s"], json!(30.0));

    // Every row filtered out: the window emits nothing.
    let none = Evaluator::eval_window(&stmt, vec![row(json!({"speed": 1}))]);
    assert!(none.is_empty());
}

#[test]
fn non_aggregate_window_projects_every_row() {
    let stmt =
        parse("SELECT device, speed FROM telem WHERE speed >= 7 GROUP BY TUMBLINGWINDOW(ss, 1)");
    let rows = vec![
        row(json!({"device": "a", "speed": 5, "x": 1})),
        row(json!({"device": "b", "speed": 7, "x": 1})),
        row(json!({"device": "c", "speed": 9, "x": 1})),
    ];
    let out = Evaluator::eval_window(&stmt, rows);
    assert_eq!(out.len(), 2);
    assert_eq!(out[0], row(json!({"device": "b", "speed": 7})));
    assert_eq!(out[1], row(json!({"device": "c", "speed": 9})));
}

#[test]
fn having_order_by_and_limit_shape_grouped_output() {
    let stmt = parse(
        "SELECT device, count(*) AS n FROM telem GROUP BY device, TUMBLINGWINDOW(ss, 10) HAVING count(*) > 1 ORDER BY n DESC LIMIT 2",
    );
    let mut rows = Vec::new();
    for (device, times) in [("a", 2), ("b", 5), ("c", 1), ("d", 3)] {
        for _ in 0..times {
            rows.push(row(json!({"device": device})));
        }
    }
    let out = Evaluator::eval_window(&stmt, rows);
    let got: Vec<_> = out
        .iter()
        .map(|r| (r["device"].clone(), r["n"].clone()))
        .collect();
    assert_eq!(got, vec![(json!("b"), json!(5)), (json!("d"), json!(3))]);
}

#[test]
fn incremental_window_matches_buffered_evaluation() {
    let cases = [
        "SELECT device, count(*) AS n, avg(temp) AS avg_temp, max(speed) AS max_speed FROM telem GROUP BY device, TUMBLINGWINDOW(ss, 10)",
        "SELECT device, site, count(temp), sum(temp), min(temp), max(temp), avg(speed) FROM telem WHERE speed > 20 GROUP BY device, site, TUMBLINGWINDOW(ss, 10)",
        "SELECT count(*) AS n, sum(speed) AS total FROM telem GROUP BY TUMBLINGWINDOW(ss, 10)",
        "SELECT site, speed, count(*) AS n FROM telem GROUP BY site, TUMBLINGWINDOW(ss, 10) ORDER BY n DESC LIMIT 1",
    ];
    let rows = telemetry(2_000);
    for sql in cases {
        let stmt = parse(sql);
        let mut inc = IncrementalWindow::try_new(&stmt)
            .unwrap_or_else(|| panic!("incremental plan for {sql}"));
        for r in &rows {
            inc.push(r);
        }
        let incremental = inc.take();
        let buffered = Evaluator::eval_window(&stmt, rows.clone());
        if stmt.order_by.is_empty() {
            assert_eq!(sorted(incremental), sorted(buffered), "{sql}");
        } else {
            assert_eq!(incremental, buffered, "{sql}");
        }
        assert!(inc.is_empty(), "take() resets the window");
    }
}

#[test]
fn incremental_sum_overflow_falls_back_to_float_like_buffered() {
    let stmt = parse("SELECT sum(v) AS s FROM telem GROUP BY TUMBLINGWINDOW(ss, 10)");
    let rows = vec![row(json!({"v": i64::MAX})), row(json!({"v": 10}))];
    let mut inc = IncrementalWindow::try_new(&stmt).unwrap();
    for r in &rows {
        inc.push(r);
    }
    assert_eq!(inc.take(), Evaluator::eval_window(&stmt, rows));
}

#[test]
fn incremental_plan_rejects_statements_that_need_rows() {
    for sql in [
        "SELECT device, collect(temp) FROM telem GROUP BY device, TUMBLINGWINDOW(ss, 10)",
        "SELECT device, count(*) FROM telem GROUP BY device, TUMBLINGWINDOW(ss, 10) HAVING count(*) > 2",
        "SELECT device, avg(temp) + 1 AS a FROM telem GROUP BY device, TUMBLINGWINDOW(ss, 10)",
        "SELECT * FROM telem GROUP BY TUMBLINGWINDOW(ss, 10)",
        "SELECT device, speed FROM telem GROUP BY TUMBLINGWINDOW(ss, 10)",
    ] {
        assert!(IncrementalWindow::try_new(&parse(sql)).is_none(), "{sql}");
    }
}

#[test]
fn session_window_parses_in_ekuiper_argument_order() {
    let stmt = parse("SELECT vin, count(*) FROM telem GROUP BY vin, SESSIONWINDOW(mi, 30, 2)");
    assert_eq!(
        stmt.window,
        Some(WindowDef::Session {
            unit: TimeUnit::Mi,
            max_duration: 30,
            timeout: 2,
        })
    );
    assert_eq!(stmt.group_by.len(), 1, "window is extracted from GROUP BY");
    assert!(
        Parser::new("SELECT count(*) FROM telem GROUP BY SESSIONWINDOW(ss, 10)")
            .parse_select()
            .is_err()
    );
}

#[test]
fn source_metadata_is_readable_but_never_projected() {
    let record = row(json!({
        "temp": 21.5,
        "__meta__": {"topic": "esphome/kitchen/sensor/temp/state", "qos": 1, "messageId": 42}
    }));
    let star = Evaluator::eval_select(&parse("SELECT * FROM telem"), &record).unwrap();
    assert_eq!(star, row(json!({"temp": 21.5})));

    let meta = Evaluator::eval_select(
        &parse("SELECT meta(topic) AS t, mqtt(topic) AS t2, meta(messageid) AS mid FROM telem"),
        &record,
    )
    .unwrap();
    assert_eq!(meta["t"], json!("esphome/kitchen/sensor/temp/state"));
    assert_eq!(meta["t2"], json!("esphome/kitchen/sensor/temp/state"));
    assert_eq!(meta["mid"], json!(42));
}

#[test]
fn window_having_had_changed_filters_identical_consecutive_results() {
    let stmt = parse(
        "SELECT latest(val) AS v FROM demo GROUP BY TUMBLINGWINDOW(ss, 1) HAVING had_changed(latest(val))",
    );
    let state = rekuiper_sql::RuleState::default();

    // First window: val = 10 -> had_changed evaluates to true, emits row
    let win1 = vec![row(json!({"val": 10}))];
    let out1 = Evaluator::eval_window_stateful(&stmt, win1, Some(&state));
    assert_eq!(out1.len(), 1);
    assert_eq!(out1[0]["v"], json!(10));

    // Second window: val = 10 -> had_changed evaluates to false because v is unchanged, filtered
    let win2 = vec![row(json!({"val": 10}))];
    let out2 = Evaluator::eval_window_stateful(&stmt, win2, Some(&state));
    assert_eq!(out2.len(), 0);

    // Third window: val = 20 -> had_changed evaluates to true because v changed to 20, emits row
    let win3 = vec![row(json!({"val": 20}))];
    let out3 = Evaluator::eval_window_stateful(&stmt, win3, Some(&state));
    assert_eq!(out3.len(), 1);
    assert_eq!(out3[0]["v"], json!(20));

    // Fourth window: val = 20 -> filtered
    let win4 = vec![row(json!({"val": 20}))];
    let out4 = Evaluator::eval_window_stateful(&stmt, win4, Some(&state));
    assert_eq!(out4.len(), 0);
}

#[test]
fn window_having_had_changed_tracks_state_per_group() {
    let stmt = parse(
        "SELECT device, latest(val) AS v FROM demo GROUP BY device, TUMBLINGWINDOW(ss, 1) HAVING had_changed(latest(val))",
    );
    let state = rekuiper_sql::RuleState::default();

    // Window 1: device "a" with 10, device "b" with 100 -> both emit
    let win1 = vec![
        row(json!({"device": "a", "val": 10})),
        row(json!({"device": "b", "val": 100})),
    ];
    let out1 = Evaluator::eval_window_stateful(&stmt, win1, Some(&state));
    assert_eq!(out1.len(), 2);

    // Window 2: device "a" with 10 (unchanged), device "b" with 200 (changed) -> only "b" emits
    let win2 = vec![
        row(json!({"device": "a", "val": 10})),
        row(json!({"device": "b", "val": 200})),
    ];
    let out2 = Evaluator::eval_window_stateful(&stmt, win2, Some(&state));
    assert_eq!(out2.len(), 1);
    assert_eq!(out2[0]["device"], json!("b"));
    assert_eq!(out2[0]["v"], json!(200));
}

#[test]
fn test_sliding_incremental_window_eviction_and_extrema() {
    let stmt = parse(
        "SELECT count(*) AS c, sum(v) AS s, min(v) AS mn, max(v) AS mx FROM telem GROUP BY SLIDINGWINDOW(ss, 10)",
    );
    let mut sliding = SlidingIncrementalWindow::try_new(&stmt).expect("sliding plan");

    // Push events at t=1000, 2000, 3000
    sliding.push(1000, &row(json!({"v": 10})));
    sliding.push(2000, &row(json!({"v": 5})));
    sliding.push(3000, &row(json!({"v": 20})));

    let snap = sliding.snapshot();
    assert_eq!(snap.len(), 1);
    assert_eq!(snap[0]["c"], json!(3));
    assert_eq!(snap[0]["s"], json!(35));
    assert_eq!(snap[0]["mn"], json!(5));
    assert_eq!(snap[0]["mx"], json!(20));

    // Evict before t=2500 -> removes t=1000 (v=10) and t=2000 (v=5)
    sliding.evict_before(2500);
    let snap2 = sliding.snapshot();
    assert_eq!(snap2.len(), 1);
    assert_eq!(snap2[0]["c"], json!(1));
    assert_eq!(snap2[0]["s"], json!(20));
    assert_eq!(snap2[0]["mn"], json!(20));
    assert_eq!(snap2[0]["mx"], json!(20));

    // Evict before t=4000 -> removes all
    sliding.evict_before(4000);
    assert!(sliding.is_empty());
    assert!(sliding.snapshot().is_empty());
}

#[test]
fn test_sliding_incremental_window_inactive_group_reclamation() {
    let stmt = parse(
        "SELECT dev, count(*) AS c, sum(v) AS s FROM telem GROUP BY dev, SLIDINGWINDOW(ss, 10)",
    );
    let mut sliding = SlidingIncrementalWindow::try_new(&stmt).expect("sliding plan");

    // Group dev-1 at t=1000, dev-2 at t=2000
    sliding.push(1000, &row(json!({"dev": "dev-1", "v": 10})));
    sliding.push(2000, &row(json!({"dev": "dev-2", "v": 20})));

    assert_eq!(sliding.snapshot().len(), 2);

    // Evict before 1500 -> dev-1 expires, dev-2 remains active
    sliding.evict_before(1500);
    assert_eq!(sliding.active_group_count(), 1);
    let snap = sliding.snapshot();
    assert_eq!(snap.len(), 1);
    assert_eq!(snap[0]["dev"], json!("dev-2"));

    // Evict before 3000 -> dev-2 expires, window must be empty and groups reclaimed
    sliding.evict_before(3000);
    assert!(sliding.is_empty());
    assert_eq!(sliding.active_group_count(), 0);
    assert_eq!(sliding.snapshot().len(), 0);

    // Re-push dev-1 at t=4000 -> starts clean
    sliding.push(4000, &row(json!({"dev": "dev-1", "v": 99})));
    let snap2 = sliding.snapshot();
    assert_eq!(snap2.len(), 1);
    assert_eq!(snap2[0]["dev"], json!("dev-1"));
    assert_eq!(snap2[0]["s"], json!(99));
}

#[test]
fn test_sliding_incremental_window_u64_extrema_and_sum_saturation() {
    let stmt = parse(
        "SELECT min(v) AS mn, max(v) AS mx, sum(v) AS s FROM telem GROUP BY SLIDINGWINDOW(ss, 10)",
    );
    let mut sliding = SlidingIncrementalWindow::try_new(&stmt).expect("sliding plan");

    // Push large u64 value: 18446744073709551615 (u64::MAX)
    let u64_max_json: Value = serde_json::from_str("18446744073709551615").unwrap();
    sliding.push(1000, &row(json!({"v": u64_max_json})));
    sliding.push(2000, &row(json!({"v": 100})));

    let snap = sliding.snapshot();
    assert_eq!(snap.len(), 1);
    assert_eq!(snap[0]["mn"], json!(100));
    // Verify exact u64 representation preserved without float truncation
    assert_eq!(snap[0]["mx"].to_string(), "18446744073709551615");

    // Test SUM saturating at i64::MAX
    let mut sliding_sum = SlidingIncrementalWindow::try_new(&stmt).expect("sliding plan");
    sliding_sum.push(1000, &row(json!({"v": i64::MAX - 10})));
    sliding_sum.push(2000, &row(json!({"v": 50})));
    let snap_sum = sliding_sum.snapshot();
    assert_eq!(snap_sum[0]["s"], json!(i64::MAX));

    // Expiration maintains non-crashing decrement: (i64::MAX - (i64::MAX - 10)) = 10
    sliding_sum.evict_before(1500);
    let snap_sum2 = sliding_sum.snapshot();
    assert_eq!(snap_sum2[0]["s"], json!(10));
}

#[test]
fn test_sliding_window_high_cardinality_acceptance() {
    let stmt = parse("SELECT id, count(*) as c FROM demo GROUP BY SlidingWindow(ss, 60), id");
    let mut sliding = SlidingIncrementalWindow::try_new(&stmt).expect("sliding plan");

    // Ingest 65,537 distinct valid groups: verify none are silently dropped!
    for i in 0..65_537 {
        let accepted = sliding.push(1000, &row(json!({"id": format!("dev_{}", i)})));
        assert!(accepted, "Valid distinct group {} must not be rejected", i);
    }

    let snap = sliding.snapshot();
    assert_eq!(
        snap.len(),
        65_537,
        "All 65,537 valid distinct groups must be output"
    );
}
