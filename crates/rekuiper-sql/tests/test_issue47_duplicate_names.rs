use indexmap::IndexMap;
use rekuiper_sql::{Evaluator, Parser};
use serde_json::json;

/// Reproduction 1 from Issue #47:
/// Duplicate explicit aliases in a window query:
/// `SELECT sum(v) AS x, v AS x, count(*) AS n FROM "order/in" GROUP BY COUNTWINDOW(2)`
/// When two fields share the output name `x`, the column position of `x` must be retained
/// at index 0, and the value must reflect the last evaluated expression (first record's v = 1).
#[test]
fn test_duplicate_aliases_in_window_query() {
    let sql = r#"SELECT sum(v) AS x, v AS x, count(*) AS n FROM demo GROUP BY COUNTWINDOW(2)"#;
    let mut parser = Parser::new(sql);
    let stmt = parser.parse_select().expect("parse SQL");

    let mut rec1 = IndexMap::new();
    rec1.insert("v".to_string(), json!(1));
    let mut rec2 = IndexMap::new();
    rec2.insert("v".to_string(), json!(3));
    let records = vec![rec1, rec2];

    let out = Evaluator::eval_aggregate(&stmt, &records).expect("aggregate output");
    let keys: Vec<&String> = out.keys().collect();
    assert_eq!(
        keys,
        vec!["x", "n"],
        "Output keys must keep first position for duplicate names"
    );
    assert_eq!(
        out["x"],
        json!(1),
        "Duplicate name must keep last evaluated value"
    );
    assert_eq!(out["n"], json!(2));
}

/// Reproduction 2:
/// An aggregate alias followed by wildcard `*`:
/// `SELECT sum(v) AS v, * FROM demo GROUP BY COUNTWINDOW(2)`
/// Output column `v` remains at position 0, but its value is updated by the subsequent `*`
/// (which emits raw record fields, v = 1, a = "foo").
#[test]
fn test_aggregate_alias_followed_by_wildcard() {
    let sql = r#"SELECT sum(v) AS v, * FROM demo GROUP BY COUNTWINDOW(2)"#;
    let mut parser = Parser::new(sql);
    let stmt = parser.parse_select().expect("parse SQL");

    let mut rec1 = IndexMap::new();
    rec1.insert("v".to_string(), json!(1));
    rec1.insert("a".to_string(), json!("foo"));
    let mut rec2 = IndexMap::new();
    rec2.insert("v".to_string(), json!(3));
    rec2.insert("a".to_string(), json!("bar"));
    let records = vec![rec1, rec2];

    let out = Evaluator::eval_aggregate(&stmt, &records).expect("aggregate output");
    let keys: Vec<&String> = out.keys().collect();
    assert_eq!(
        keys,
        vec!["v", "a"],
        "Output keys must retain first-seen position"
    );
    assert_eq!(
        out["v"],
        json!(1),
        "Later wildcard overwrites earlier sum(v) AS v"
    );
    assert_eq!(out["a"], json!("foo"));
}

/// Reproduction 3:
/// Wildcard `*` followed by an aggregate alias of the same name:
/// `SELECT *, sum(v) AS v FROM demo GROUP BY COUNTWINDOW(2)`
/// Position 0 is `v` (from `*`), position 1 is `a` (from `*`).
/// The subsequent `sum(v) AS v` overwrites `v` at position 0 with 4.
#[test]
fn test_wildcard_followed_by_aggregate_alias() {
    let sql = r#"SELECT *, sum(v) AS v FROM demo GROUP BY COUNTWINDOW(2)"#;
    let mut parser = Parser::new(sql);
    let stmt = parser.parse_select().expect("parse SQL");

    let mut rec1 = IndexMap::new();
    rec1.insert("v".to_string(), json!(1));
    rec1.insert("a".to_string(), json!("foo"));
    let mut rec2 = IndexMap::new();
    rec2.insert("v".to_string(), json!(3));
    rec2.insert("a".to_string(), json!("bar"));
    let records = vec![rec1, rec2];

    let out = Evaluator::eval_aggregate(&stmt, &records).expect("aggregate output");
    let keys: Vec<&String> = out.keys().collect();
    assert_eq!(
        keys,
        vec!["v", "a"],
        "Output keys must retain first-seen position"
    );
    assert_eq!(
        out["v"],
        json!(4),
        "Later aggregate alias overwrites wildcard v"
    );
    assert_eq!(out["a"], json!("foo"));
}

/// Reproduction 4:
/// A GROUP BY key selected multiple times:
/// `SELECT id, id AS id, sum(v) AS s FROM demo GROUP BY id, COUNTWINDOW(2)`
#[test]
fn test_group_by_key_selected_multiple_times() {
    let sql = r#"SELECT id, id AS id, sum(v) AS s FROM demo GROUP BY id, COUNTWINDOW(2)"#;
    let mut parser = Parser::new(sql);
    let stmt = parser.parse_select().expect("parse SQL");

    let mut rec1 = IndexMap::new();
    rec1.insert("id".to_string(), json!("dev1"));
    rec1.insert("v".to_string(), json!(10));
    let mut rec2 = IndexMap::new();
    rec2.insert("id".to_string(), json!("dev1"));
    rec2.insert("v".to_string(), json!(20));
    let records = vec![rec1, rec2];

    let out = Evaluator::eval_aggregate(&stmt, &records).expect("aggregate output");
    let keys: Vec<&String> = out.keys().collect();
    assert_eq!(keys, vec!["id", "s"]);
    assert_eq!(out["id"], json!("dev1"));
    assert_eq!(out["s"], json!(30));
}

/// Test 5:
/// Single-record window parity between eval_select and eval_aggregate:
/// Both paths must produce identical output shapes and values when duplicate aliases exist.
#[test]
fn test_select_and_aggregate_duplicate_alias_parity() {
    let sql = "SELECT a AS x, 99 AS x, b FROM demo";
    let mut parser = Parser::new(sql);
    let stmt = parser.parse_select().expect("parse SQL");

    let mut record = IndexMap::new();
    record.insert("a".to_string(), json!(10));
    record.insert("b".to_string(), json!(20));

    let select_out = Evaluator::eval_select(&stmt, &record).expect("eval_select");
    let agg_out = Evaluator::eval_aggregate(&stmt, &[record]).expect("eval_aggregate");

    assert_eq!(
        select_out, agg_out,
        "eval_select and eval_aggregate must produce identical results for duplicate aliases"
    );
    assert_eq!(select_out.keys().collect::<Vec<_>>(), vec!["x", "b"]);
    assert_eq!(select_out["x"], json!(99));
    assert_eq!(select_out["b"], json!(20));
}
