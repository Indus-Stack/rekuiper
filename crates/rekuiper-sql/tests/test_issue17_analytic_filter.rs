use rekuiper_sql::{
    eval::{Evaluator, RuleState},
    parser::Parser,
};
use serde_json::{json, Value};
use std::collections::HashMap;

fn run(sql: &str, row: Value, state: &RuleState) -> Result<Option<HashMap<String, Value>>, String> {
    let stmt = Parser::new(sql).parse_select().unwrap();
    let row = serde_json::from_value(row).unwrap();
    Evaluator::eval_select_filtered_stateful_fallible(&stmt, &row, state)
}

#[test]
fn filtered_scalar_errors_do_not_prevent_analytic_accumulation() {
    let state = RuleState::new();
    let sql = "SELECT abs(dev) AS v, acc_sum(temp) AS total FROM s WHERE upload = true";
    assert!(
        run(sql, json!({"dev":"bad", "temp":2, "upload":false}), &state)
            .unwrap()
            .is_none()
    );
    let output = run(sql, json!({"dev":-3, "temp":5, "upload":true}), &state)
        .unwrap()
        .unwrap();
    assert_eq!(output["v"], json!(3));
    assert_eq!(output["total"], json!(7));
    let err = run(sql, json!({"dev":"bad", "temp":4, "upload":true}), &state).unwrap_err();
    assert!(err.contains("run Select error: alias: v"), "{err}");
    let output = run(sql, json!({"dev":1, "temp":8, "upload":true}), &state)
        .unwrap()
        .unwrap();
    assert_eq!(output["total"], json!(19));
}

#[test]
fn shared_analytic_call_advances_once_per_row() {
    let state = RuleState::new();
    let sql = "SELECT acc_sum(v) AS total FROM s WHERE acc_sum(v) > 2";
    assert!(run(sql, json!({"v":1}), &state).unwrap().is_none());
    let output = run(sql, json!({"v":2}), &state).unwrap().unwrap();
    assert_eq!(output["total"], json!(3));
}

#[test]
fn analytic_errors_and_where_errors_are_not_silently_filtered() {
    let state = RuleState::new();
    let err = run(
        "SELECT acc_sum(abs(dev)) AS total FROM s WHERE upload = true",
        json!({"dev":"bad", "upload":false}),
        &state,
    )
    .unwrap_err();
    assert!(err.contains("run Select error: alias: total"), "{err}");
    let err = run(
        "SELECT acc_sum(v) AS total FROM s WHERE temp",
        json!({"v":1, "temp":25.5}),
        &state,
    )
    .unwrap_err();
    assert!(err.contains("run Where error: invalid condition"), "{err}");
}

#[test]
fn test_sequence_match_and_count_cep() {
    let state = RuleState::new();
    let sql = "SELECT sequence_match(30000, temp > 80.0, pressure > 120.0) AS matched, sequence_count(30000, temp > 80.0, pressure > 120.0) AS cnt FROM s";
    // Event 1: temp = 85 (step 1 matches, step 2 does not)
    let out1 = run(
        sql,
        json!({"temp": 85.0, "pressure": 50.0, "timestamp": 1000}),
        &state,
    )
    .unwrap()
    .unwrap();
    assert_eq!(out1["matched"], json!(false));
    assert_eq!(out1["cnt"], json!(0));

    // Event 2: pressure = 130 (step 2 matches -> complete sequence within 30s)
    let out2 = run(
        sql,
        json!({"temp": 70.0, "pressure": 130.0, "timestamp": 5000}),
        &state,
    )
    .unwrap()
    .unwrap();
    assert_eq!(out2["matched"], json!(true));
    assert_eq!(out2["cnt"], json!(1));

    // Event 3: regular event
    let out3 = run(
        sql,
        json!({"temp": 70.0, "pressure": 50.0, "timestamp": 6000}),
        &state,
    )
    .unwrap()
    .unwrap();
    assert_eq!(out3["matched"], json!(false));
    assert_eq!(out3["cnt"], json!(1));
}
