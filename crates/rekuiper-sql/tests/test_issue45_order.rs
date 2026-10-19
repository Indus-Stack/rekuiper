use indexmap::IndexMap;
use rekuiper_sql::{Evaluator, Parser};
use serde_json::json;

#[test]
fn test_select_field_order_preserved() {
    let sql = "SELECT z, a, m FROM demo";
    let mut parser = Parser::new(sql);
    let stmt = parser.parse_select().expect("failed to parse SQL");

    let mut record = IndexMap::new();
    record.insert("a".to_string(), json!(1));
    record.insert("m".to_string(), json!(2));
    record.insert("z".to_string(), json!(3));

    let out = Evaluator::eval_select(&stmt, &record).expect("eval failed");
    let keys: Vec<&String> = out.keys().collect();
    assert_eq!(
        keys,
        vec!["z", "a", "m"],
        "Output keys must follow SELECT order"
    );

    let json_str = serde_json::to_string(&out).expect("serialize");
    assert_eq!(json_str, r#"{"z":3,"a":1,"m":2}"#);
}

#[test]
fn test_select_wildcard_preserves_input_order() {
    let sql = "SELECT * FROM demo";
    let mut parser = Parser::new(sql);
    let stmt = parser.parse_select().expect("failed to parse SQL");

    let mut record = IndexMap::new();
    record.insert("field_c".to_string(), json!(30));
    record.insert("field_a".to_string(), json!(10));
    record.insert("field_b".to_string(), json!(20));

    let out = Evaluator::eval_select(&stmt, &record).expect("eval failed");
    let keys: Vec<&String> = out.keys().collect();
    assert_eq!(
        keys,
        vec!["field_c", "field_a", "field_b"],
        "SELECT * must preserve input key order"
    );

    let json_str = serde_json::to_string(&out).expect("serialize");
    assert_eq!(json_str, r#"{"field_c":30,"field_a":10,"field_b":20}"#);
}

#[test]
fn test_select_computed_and_alias_order() {
    let sql = "SELECT x, a + 10 AS y, b FROM demo";
    let mut parser = Parser::new(sql);
    let stmt = parser.parse_select().expect("failed to parse SQL");

    let mut record = IndexMap::new();
    record.insert("a".to_string(), json!(5));
    record.insert("b".to_string(), json!("hello"));
    record.insert("x".to_string(), json!(true));

    let out = Evaluator::eval_select(&stmt, &record).expect("eval failed");
    let keys: Vec<&String> = out.keys().collect();
    assert_eq!(keys, vec!["x", "y", "b"]);

    let json_str = serde_json::to_string(&out).expect("serialize");
    assert_eq!(json_str, r#"{"x":true,"y":15,"b":"hello"}"#);
}
