use rekuiper_sql::{Evaluator, Parser, RuleState};
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

fn run_sql_records(
    sql: &str,
    records: &[serde_json::Value],
) -> Vec<indexmap::IndexMap<String, Value>> {
    let mut parser = Parser::new(sql);
    let stmt = parser.parse_select().expect("failed to parse SQL");
    let state = RuleState::default();
    let mut out = Vec::new();
    for r in records {
        let rec = json_to_record(r);
        let v = Evaluator::eval_select_stateful(&stmt, &rec, &state).expect("eval failed");
        out.push(v);
    }
    out
}

#[test]
fn test_bitwise_operators() {
    // 1. Bitwise AND, OR, XOR on integers
    let rec = json!({"i": 7, "j": 6, "k": 1});
    let res = run_sql("SELECT i & 3 AS a, k | 8 AS b, j ^ 5 AS c FROM demo", &rec);
    assert_eq!(res["a"], 3); // 7 & 3 = 3
    assert_eq!(res["b"], 9); // 1 | 8 = 9
    assert_eq!(res["c"], 3); // 6 ^ 5 = 3

    // Bitwise operators on booleans (logical AND/OR/XOR)
    let b_rec = json!({"t": true, "f": false});
    let b_res = run_sql(
        "SELECT t & f AS a, t | f AS b, t ^ f AS c, t ^ t AS d FROM demo",
        &b_rec,
    );
    assert_eq!(b_res["a"], false);
    assert_eq!(b_res["b"], true);
    assert_eq!(b_res["c"], true);
    assert_eq!(b_res["d"], false);
}

#[test]
fn test_wildcard_except_and_replace() {
    let rec = json!({
        "temp": 25,
        "arr": [1, 2, 3],
        "humidity": 60,
        "device": "sensor1"
    });

    // EXCEPT excludes the named columns
    let res1 = run_sql("SELECT * EXCEPT(temp, arr) FROM demo", &rec);
    assert!(!res1.contains_key("temp"));
    assert!(!res1.contains_key("arr"));
    assert_eq!(res1.get("humidity"), Some(&json!(60)));
    assert_eq!(res1.get("device"), Some(&json!("sensor1")));

    // REPLACE computes new values for the named columns while preserving others
    let res2 = run_sql("SELECT * REPLACE(temp * 2 AS temp) FROM demo", &rec);
    assert_eq!(res2.get("temp"), Some(&json!(50))); // integer arithmetic
    assert_eq!(res2.get("humidity"), Some(&json!(60)));
    assert_eq!(res2.get("device"), Some(&json!("sensor1")));
}

#[test]
fn test_backtick_identifiers() {
    // Backtick alias with spaces and symbols
    let rec1 = json!({"id": 1, "temp": 25.5});
    let res1 = run_sql("SELECT id AS i, temp AS `my temp` FROM demo", &rec1);
    assert_eq!(res1["i"], 1);
    assert_eq!(res1["my temp"], 25.5);

    // Bare backtick identifier with hyphen
    let rec2 = json!({"id": 2, "x-y": 42});
    let res2 = run_sql("SELECT id, `x-y` AS xy FROM demo", &rec2);
    assert_eq!(res2["id"], 2);
    assert_eq!(res2["xy"], 42);
}

#[test]
fn test_postfix_navigation() {
    // Chained index and object arrow access: obj["data"]->temp or items[1]->temp
    let rec = json!({
        "items": [
            {"temp": 20, "name": "first"},
            {"temp": 30, "name": "second"}
        ],
        "meta": {"info": {"ver": 2}}
    });

    let res = run_sql(
        "SELECT items[1]->temp AS v, meta->info->ver AS ver FROM demo",
        &rec,
    );
    assert_eq!(res["v"], 30);
    assert_eq!(res["ver"], 2);
}

#[test]
fn test_analytic_over_when() {
    // Analytic OVER (WHEN <cond>) ignores rows where condition is false, preserving state
    let stream = vec![
        json!({"id": 1, "temp": 15}), // WHEN false
        json!({"id": 2, "temp": 25}), // WHEN true (first qualifying, lag returns null)
        json!({"id": 3, "temp": 18}), // WHEN false (retains previous qualifying: 25)
        json!({"id": 4, "temp": 30}), // WHEN true (previous qualifying was 25)
    ];

    let results = run_sql_records(
        "SELECT id, lag(temp) OVER (WHEN temp > 20) AS v FROM demo",
        &stream,
    );
    assert_eq!(results[0]["v"], Value::Null); // temp=15 <= 20, no prior qualifying
    assert_eq!(results[1]["v"], Value::Null); // temp=25 > 20, first qualifying, no prior lag
    assert_eq!(results[2]["v"], 25); // temp=18 <= 20, returns last qualifying value (25)
    assert_eq!(results[3]["v"], 25); // temp=30 > 20, previous qualifying was 25

    // acc_count OVER (WHEN <cond>)
    let results2 = run_sql_records(
        "SELECT id, acc_count(temp) OVER (WHEN temp > 20) AS c FROM demo",
        &stream,
    );
    assert_eq!(results2[0]["c"], 0); // skipped, initial count is 0
    assert_eq!(results2[1]["c"], 1); // count = 1
    assert_eq!(results2[2]["c"], 1); // skipped, retains 1
    assert_eq!(results2[3]["c"], 2); // count = 2
}

#[test]
fn test_builtin_functions_parity() {
    // percentile_cont (aggregate function)
    let rows = [
        json!({"x": 10.0}),
        json!({"x": 20.0}),
        json!({"x": 30.0}),
        json!({"x": 40.0}),
    ];
    let mut parser = Parser::new("SELECT percentile_cont(x, 0.5) AS p50 FROM demo");
    let stmt = parser.parse_select().expect("failed to parse SQL");
    let recs: Vec<HashMap<String, Value>> = rows.iter().map(json_to_record).collect();
    let res1 = Evaluator::eval_aggregate(&stmt, &recs).expect("failed to eval SQL");
    assert_eq!(res1["p50"], 25.0);

    // regexp_substr
    let rec2 = json!({"s": "temperature: 36.5C"});
    let res2 = run_sql(
        "SELECT regexp_substr(s, '[0-9]+\\.[0-9]+') AS m FROM demo",
        &rec2,
    );
    assert_eq!(res2["m"], "36.5");
}
