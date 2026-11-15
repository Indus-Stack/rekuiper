use rekuiper_sql::{Evaluator, Parser};
use serde_json::json;
use std::collections::HashMap;

fn eval_single(sql: &str, record: HashMap<String, serde_json::Value>) -> serde_json::Value {
    let mut parser = Parser::new(sql);
    let stmt = parser.parse_select().unwrap();
    let row = Evaluator::eval_select(&stmt, &record).unwrap();
    row.get("picked").cloned().unwrap()
}

#[test]
fn a07_t01_immutable_nested_object_pick() {
    // 1. Exact dotted key wins over nested traversal.
    let rec_dotted = HashMap::from([(
        "obj".to_owned(),
        json!({ "a.b": "literal", "a": { "b": "nested" } }),
    )]);
    assert_eq!(
        eval_single(
            "SELECT object_pick(obj, 'a.b') AS picked FROM s",
            rec_dotted
        ),
        json!({ "a.b": "literal" })
    );

    // 2. Deep sibling selections retain hierarchy & string-list/array arguments work.
    let original = json!({ "a": { "b": 1, "c": 2, "d": 3 }, "x": 10 });
    let rec = HashMap::from([("obj".to_owned(), original.clone())]);
    assert_eq!(
        eval_single(
            "SELECT object_pick(obj, ['a.b', 'a.c', 'missing.path']) AS picked FROM s",
            rec.clone()
        ),
        json!({ "a": { "b": 1, "c": 2 } })
    );

    // 3. Parent-child overlap yields identical result regardless of selection order.
    let p_first = eval_single(
        "SELECT object_pick(obj, 'a', 'a.b') AS picked FROM s",
        rec.clone(),
    );
    let c_first = eval_single(
        "SELECT object_pick(obj, 'a.b', 'a') AS picked FROM s",
        rec.clone(),
    );
    assert_eq!(p_first, json!({ "a": { "b": 1, "c": 2, "d": 3 } }));
    assert_eq!(p_first, c_first);

    // 4. Duplicate selections & original object immutability.
    let dup = eval_single(
        "SELECT object_pick(obj, 'a.b', 'a.b') AS picked FROM s",
        rec.clone(),
    );
    assert_eq!(dup, json!({ "a": { "b": 1 } }));
    assert_eq!(rec["obj"], original);
}
