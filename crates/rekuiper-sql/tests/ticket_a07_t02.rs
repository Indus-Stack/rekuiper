use rekuiper_sql::{Evaluator, Parser};
use serde_json::{json, Value};
use std::collections::HashMap;

fn eval_expr(sql: &str, rec: &HashMap<String, Value>) -> Value {
    let mut parser = Parser::new(sql);
    let stmt = parser.parse_select().unwrap();
    let row = Evaluator::eval_select(&stmt, rec).unwrap();
    row.get("res").cloned().unwrap()
}

#[test]
fn a07_t02_distinct_array_except_and_set_differential_cases() {
    let empty = HashMap::new();

    // 1. [1, 1, 2] except [2] -> [1] (distinct, first-seen order).
    assert_eq!(
        eval_expr("SELECT array_except([1, 1, 2], [2]) AS res FROM s", &empty),
        json!([1])
    );

    // 2. Preserves first-seen order across multiple distinct survivors.
    assert_eq!(
        eval_expr(
            "SELECT array_except([3, 1, 3, 2, 1], [2]) AS res FROM s",
            &empty
        ),
        json!([3, 1])
    );

    // 3. Nested arrays/objects do not panic and deduplicate via values_equal.
    let rec = HashMap::from([
        (
            "a".to_owned(),
            json!([{"k": 1}, {"k": 1}, [1, 2], [1, 2], {"k": 2}]),
        ),
        ("b".to_owned(), json!([{"k": 2}])),
    ]);
    assert_eq!(
        eval_expr("SELECT array_except(a, b) AS res FROM s", &rec),
        json!([{"k": 1}, [1, 2]])
    );

    // 4. Numeric & null behavior matches values_equal (1 == 1.0, null != null in SQL 3VL).
    let rec_num = HashMap::from([
        ("a".to_owned(), json!([1, 1.0, null, null, 2])),
        ("b".to_owned(), json!([1.0, null])),
    ]);
    assert_eq!(
        eval_expr("SELECT array_except(a, b) AS res FROM s", &rec_num),
        json!([null, null, 2])
    );

    // 5. Differential cases: array_union & array_intersect retain first-seen distinct order.
    assert_eq!(
        eval_expr(
            "SELECT array_union([1, 2, 1], [2, 3, 3]) AS res FROM s",
            &empty
        ),
        json!([1, 2, 3])
    );
    assert_eq!(
        eval_expr(
            "SELECT array_intersect([2, 1, 2, 3], [3, 2, 2]) AS res FROM s",
            &empty
        ),
        json!([2, 3])
    );
}
