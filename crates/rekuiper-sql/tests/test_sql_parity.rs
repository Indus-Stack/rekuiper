use rekuiper_sql::{Evaluator, JoinType, Parser, RuleState, SetOp};
use serde_json::{json, Value};
use std::collections::HashMap;

type Record = HashMap<String, Value>;

fn rec(pairs: &[(&str, Value)]) -> Record {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

fn parse(sql: &str) -> rekuiper_sql::SelectStmt {
    let mut parser = Parser::new(sql);
    parser.parse_select().expect("Should parse")
}

// ---------------------------------------------------------------------------
// UNION / UNION ALL
// ---------------------------------------------------------------------------

#[test]
fn test_union_all_keeps_duplicates() {
    let stmt = parse("SELECT a FROM s1 UNION ALL SELECT a FROM s2");
    assert!(matches!(stmt.set_op, Some((SetOp::UnionAll, _))));

    let state = RuleState::default();
    // Duplicate rows survive UNION ALL.
    let rows = Evaluator::eval_select_stateful_multi(&stmt, &rec(&[("a", json!(1))]), &state);
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|r| r.get("a") == Some(&json!(1))));

    // NOTE: the stateful multi API projects without WHERE filtering (the
    // rule loop applies filters around evaluation), so both branches yield.
    let stmt = parse("SELECT a FROM s WHERE a > 5 UNION ALL SELECT a FROM s WHERE a < 3");
    let rows = Evaluator::eval_select_stateful_multi(&stmt, &rec(&[("a", json!(10))]), &state);
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_union_deduplicates() {
    let stmt = parse("SELECT a FROM s1 UNION SELECT a FROM s2");
    assert!(matches!(stmt.set_op, Some((SetOp::Union, _))));

    let state = RuleState::default();
    // Identical branch rows collapse to one.
    let rows = Evaluator::eval_select_stateful_multi(&stmt, &rec(&[("a", json!(1))]), &state);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("a"), Some(&json!(1)));

    // Distinct branch rows both survive.
    let stmt = parse("SELECT a FROM s1 UNION SELECT b FROM s2");
    let rows = Evaluator::eval_select_stateful_multi(
        &stmt,
        &rec(&[("a", json!(1)), ("b", json!(2))]),
        &state,
    );
    assert_eq!(rows.len(), 2);

    // Chained unions associate to the right and still dedupe globally.
    let stmt = parse("SELECT a FROM s1 UNION SELECT a FROM s2 UNION SELECT a FROM s3");
    let rows = Evaluator::eval_select_stateful_multi(&stmt, &rec(&[("a", json!(7))]), &state);
    assert_eq!(rows.len(), 1);
}

#[test]
fn test_union_single_row_projection() {
    // The single-row API merges both branch projections (right wins).
    let stmt = parse("SELECT a FROM s1 UNION ALL SELECT b FROM s2");
    let out = Evaluator::eval_select(&stmt, &rec(&[("a", json!(1)), ("b", json!(2))]))
        .expect("Should project");
    assert_eq!(out.get("a"), Some(&json!(1)));
    assert_eq!(out.get("b"), Some(&json!(2)));

    // A branch filtered out contributes nothing.
    let stmt = parse("SELECT a FROM s WHERE a > 100 UNION SELECT b FROM s");
    let out = Evaluator::eval_select(&stmt, &rec(&[("a", json!(1)), ("b", json!(2))]))
        .expect("Should project");
    assert_eq!(out.get("b"), Some(&json!(2)));
    assert!(!out.contains_key("a"));

    // Neither branch matches -> None (also for aggregates).
    let stmt = parse("SELECT a FROM s WHERE a > 100 UNION SELECT b FROM s WHERE b > 100");
    assert!(Evaluator::eval_select(&stmt, &rec(&[("a", json!(1))])).is_none());
}

// ---------------------------------------------------------------------------
// Nested JSON expressions
// ---------------------------------------------------------------------------

#[test]
fn test_nested_json_projection_and_filter() {
    let stmt =
        parse("SELECT doc.user.profile.age + 1 AS next_age FROM demo WHERE doc.user.active = true");
    let record = rec(&[(
        "doc",
        json!({"user": {"profile": {"age": 30}, "active": true}}),
    )]);
    let out = Evaluator::eval_select(&stmt, &record).expect("Should match");
    assert_eq!(out.get("next_age"), Some(&json!(31)));

    // Inactive user is filtered out.
    let record = rec(&[(
        "doc",
        json!({"user": {"profile": {"age": 30}, "active": false}}),
    )]);
    assert!(Evaluator::eval_select(&stmt, &record).is_none());

    // Missing intermediate objects project Null but keep the row when
    // unfiltered.
    let stmt = parse("SELECT doc.user.profile.age AS age FROM demo");
    let out = Evaluator::eval_select(&stmt, &rec(&[("doc", json!({}))])).expect("Row kept");
    assert_eq!(out.get("age"), Some(&Value::Null));
}

// ---------------------------------------------------------------------------
// Complex CASE
// ---------------------------------------------------------------------------

#[test]
fn test_complex_case_expression() {
    let stmt = parse(
        "SELECT CASE WHEN score >= 90 THEN upper('excellent') WHEN score >= 70 THEN upper('good') WHEN score >= 50 THEN 'avg' ELSE 'fail' END AS grade FROM demo",
    );
    let grade = |score: i64| {
        Evaluator::eval_select(&stmt, &rec(&[("score", json!(score))]))
            .expect("row projects")
            .shift_remove("grade")
            .unwrap()
    };
    assert_eq!(grade(95), json!("EXCELLENT"));
    assert_eq!(grade(90), json!("EXCELLENT"));
    assert_eq!(grade(75), json!("GOOD"));
    assert_eq!(grade(50), json!("avg"));
    assert_eq!(grade(10), json!("fail"));

    // Simple CASE over strings with an ELSE default.
    let stmt =
        parse("SELECT CASE level WHEN 'high' THEN 3 WHEN 'mid' THEN 2 ELSE 1 END AS n FROM demo");
    let level = |s: &str| {
        Evaluator::eval_select(&stmt, &rec(&[("level", json!(s))]))
            .expect("row projects")
            .shift_remove("n")
            .unwrap()
    };
    assert_eq!(level("high"), json!(3));
    assert_eq!(level("mid"), json!(2));
    assert_eq!(level("low"), json!(1));
}

// ---------------------------------------------------------------------------
// JOIN parsing (execution lives in the server rule loop; covered there)
// ---------------------------------------------------------------------------

#[test]
fn test_join_syntax_variants() {
    let stmt = parse("SELECT * FROM s LEFT JOIN t ON s.id = t.id");
    assert_eq!(stmt.joins.len(), 1);
    assert_eq!(stmt.joins[0].join_type, JoinType::Left);
    assert_eq!(stmt.joins[0].target, "t");
    assert!(stmt.joins[0].on.is_some());

    let stmt = parse("SELECT * FROM s INNER JOIN t ON s.id = t.id");
    assert_eq!(stmt.joins[0].join_type, JoinType::Inner);

    // Bare JOIN defaults to Inner; CROSS parses too.
    let stmt = parse("SELECT * FROM s JOIN t ON s.id = t.id");
    assert_eq!(stmt.joins[0].join_type, JoinType::Inner);
    let stmt = parse("SELECT * FROM s CROSS JOIN t");
    assert_eq!(stmt.joins[0].join_type, JoinType::Cross);
    assert!(stmt.joins[0].on.is_none());

    // Multiple joins chain in order.
    let stmt = parse("SELECT * FROM s LEFT JOIN t ON s.id = t.id INNER JOIN u ON s.id = u.id");
    assert_eq!(stmt.joins.len(), 2);
    assert_eq!(stmt.joins[0].target, "t");
    assert_eq!(stmt.joins[1].target, "u");

    // Documented alias shapes: FROM/JOIN targets accept AS and bare aliases.
    let stmt = parse(
        "SELECT a.id AS id FROM s AS a INNER JOIN t AS b ON a.id = b.id GROUP BY CountWindow(2)",
    );
    assert_eq!(stmt.from, "s");
    assert_eq!(stmt.from_alias.as_deref(), Some("a"));
    assert_eq!(stmt.joins[0].target, "t");
    assert_eq!(stmt.joins[0].alias.as_deref(), Some("b"));
    let stmt = parse("SELECT * FROM s a LEFT JOIN t b ON a.id = b.id");
    assert_eq!(stmt.from_alias.as_deref(), Some("a"));
    assert_eq!(stmt.joins[0].alias.as_deref(), Some("b"));
    // Clause keywords never become aliases.
    let stmt = parse("SELECT * FROM s WHERE s.id = 1");
    assert_eq!(stmt.from_alias, None);
}

// ---------------------------------------------------------------------------
// Aggregates with HAVING
// ---------------------------------------------------------------------------

fn id_batch() -> Vec<Record> {
    vec![
        rec(&[("id", json!("a")), ("v", json!(1))]),
        rec(&[("id", json!("a")), ("v", json!(2))]),
        rec(&[("id", json!("a")), ("v", json!(3))]),
        rec(&[("id", json!("b")), ("v", json!(1))]),
    ]
}

#[test]
fn test_aggregate_having_filter() {
    // count(*) over the batch is 4 > 2 -> row emitted.
    let stmt = parse("SELECT count(*) AS n FROM demo GROUP BY id HAVING count(*) > 2");
    let out = Evaluator::eval_aggregate(&stmt, &id_batch()).expect("HAVING passes");
    assert_eq!(out.get("n"), Some(&json!(4)));

    // Stricter threshold filters the row out.
    let stmt = parse("SELECT count(*) AS n FROM demo GROUP BY id HAVING count(*) > 10");
    assert!(Evaluator::eval_aggregate(&stmt, &id_batch()).is_none());

    // Aggregates compose with arithmetic in HAVING.
    let stmt = parse("SELECT sum(v) AS s FROM demo HAVING sum(v) >= 7");
    let out = Evaluator::eval_aggregate(&stmt, &id_batch()).expect("HAVING passes");
    assert_eq!(out.get("s"), Some(&json!(7)));
}
