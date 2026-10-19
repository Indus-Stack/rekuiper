use rekuiper_sql::{Evaluator, Parser};
use serde_json::{json, Value};
use std::collections::HashMap;

fn json_to_record(v: &Value) -> HashMap<String, Value> {
    match v {
        Value::Object(m) => m.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        _ => HashMap::new(),
    }
}

fn run_sql(sql: &str, record: &serde_json::Value) -> indexmap::IndexMap<String, Value> {
    let mut parser = Parser::new(sql);
    let stmt = parser.parse_select().expect("failed to parse SQL");
    let rec = json_to_record(record);
    Evaluator::eval_select(&stmt, &rec).expect("failed to eval SQL")
}

#[test]
fn test_unaliased_column_names() {
    let rec = json!({
        "id": 1,
        "temp": -25,
        "name": "sensor",
        "humidity": 60
    });

    // Unaliased complex binary op yields kuiper_field_0
    let res = run_sql("SELECT id, temp + 1 FROM demo", &rec);
    assert_eq!(res.get("id"), Some(&json!(1)));
    assert_eq!(res.get("kuiper_field_0"), Some(&json!(-24)));
    assert_eq!(res.len(), 2);

    // Multiple unaliased complex expressions yield kuiper_field_0, kuiper_field_1
    let res = run_sql("SELECT id, temp + 1, humidity * 2 FROM demo", &rec);
    assert_eq!(res.get("id"), Some(&json!(1)));
    assert_eq!(res.get("kuiper_field_0"), Some(&json!(-24)));
    assert_eq!(res.get("kuiper_field_1"), Some(&json!(120)));
    assert_eq!(res.len(), 3);

    // Unaliased function calls yield function name (e.g. abs, upper)
    let res = run_sql("SELECT id, abs(temp), upper(name) FROM demo", &rec);
    assert_eq!(res.get("id"), Some(&json!(1)));
    assert_eq!(res.get("abs"), Some(&json!(25)));
    assert_eq!(res.get("upper"), Some(&json!("SENSOR")));
    assert_eq!(res.len(), 3);
}

#[test]
fn test_unnest_column_name() {
    let mut parser = Parser::new("SELECT unnest(arr) FROM demo");
    let stmt = parser.parse_select().expect("failed to parse SQL");
    let names = Evaluator::select_field_names(&stmt);
    assert_eq!(names, vec!["unnest"]);
}

#[test]
fn test_wildcard_strips_internal_metadata() {
    let rec = json!({
        "id": 42,
        "temp": 20,
        "__rule_id__": "rule_1",
        "__rule_start__": 123456789,
        "__meta__": {"topic": "t1"}
    });
    let res = run_sql("SELECT * FROM demo", &rec);
    assert_eq!(res.get("id"), Some(&json!(42)));
    assert_eq!(res.get("temp"), Some(&json!(20)));
    assert!(!res.contains_key("__rule_id__"));
    assert!(!res.contains_key("__rule_start__"));
    assert!(!res.contains_key("__meta__"));
    assert_eq!(res.len(), 2);
}

#[test]
fn test_groupby_expressions_do_not_leak_into_output() {
    let mut parser = Parser::new(
        "SELECT n % 2 AS parity, count(*) AS c FROM demo GROUP BY n % 2, TUMBLINGWINDOW(ss, 10)",
    );
    let stmt = parser.parse_select().expect("failed to parse SQL");
    let records = vec![
        json_to_record(&json!({"n": 1})),
        json_to_record(&json!({"n": 3})),
        json_to_record(&json!({"n": 5})),
    ];
    let res = Evaluator::eval_aggregate(&stmt, &records).expect("aggregate failed");
    assert_eq!(res.get("parity"), Some(&json!(1)));
    assert_eq!(res.get("c"), Some(&json!(3)));
    assert!(!res.contains_key("n % 2"));
    assert_eq!(res.len(), 2);
}
