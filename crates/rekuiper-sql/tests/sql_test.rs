use rekuiper_sql::{Evaluator, Parser, StreamColumn};
use serde_json::json;
use std::collections::HashMap;

#[test]
fn test_parse_create_stream() {
    let sql = r#"CREATE STREAM demo () WITH (DATASOURCE="demo", FORMAT="json")"#;
    let mut parser = Parser::new(sql);
    let stmt = parser
        .parse_create_stream()
        .expect("Should parse CREATE STREAM");
    assert_eq!(stmt.name, "demo");
    assert_eq!(stmt.options.get("DATASOURCE"), Some(&"demo".to_string()));
    assert_eq!(stmt.options.get("FORMAT"), Some(&"json".to_string()));
}

#[test]
fn test_parse_and_eval_select_all() {
    let sql = "SELECT * FROM demo";
    let mut parser = Parser::new(sql);
    let stmt = parser.parse_select().expect("Should parse SELECT *");
    assert_eq!(stmt.from, "demo");

    let mut record = HashMap::new();
    record.insert("temperature".to_string(), json!(25.5));
    record.insert("humidity".to_string(), json!(60));

    let result = Evaluator::eval_select(&stmt, &record).expect("Should match");
    assert_eq!(result.get("temperature"), Some(&json!(25.5)));
    assert_eq!(result.get("humidity"), Some(&json!(60)));
}

#[test]
fn test_parse_and_eval_select_filter() {
    let sql = "SELECT temperature FROM demo WHERE temperature > 30";
    let mut parser = Parser::new(sql);
    let stmt = parser
        .parse_select()
        .expect("Should parse SELECT with WHERE");

    let mut record_low = HashMap::new();
    record_low.insert("temperature".to_string(), json!(25.0));
    assert!(Evaluator::eval_select(&stmt, &record_low).is_none());

    let mut record_high = HashMap::new();
    record_high.insert("temperature".to_string(), json!(35.0));
    let matched = Evaluator::eval_select(&stmt, &record_high).expect("Should match");
    assert_eq!(matched.get("temperature"), Some(&json!(35.0)));
}

#[test]
fn test_arithmetic_precedence() {
    // * binds tighter than +: a + b * 2 == a + (b * 2)
    let sql = "SELECT a + b * 2 FROM demo WHERE a + 5 > 10";
    let mut parser = Parser::new(sql);
    let stmt = parser
        .parse_select()
        .expect("Should parse arithmetic SELECT");

    // Verify AST shape: SELECT field is Add(a, Mul(b, 2))
    assert_eq!(stmt.fields.len(), 1);
    let field = &stmt.fields[0];
    match field {
        rekuiper_sql::Expr::BinaryOp { left, op, right } => {
            assert_eq!(*op, rekuiper_sql::BinaryOperator::Add);
            assert_eq!(**left, rekuiper_sql::Expr::Identifier("a".to_string()));
            match &**right {
                rekuiper_sql::Expr::BinaryOp {
                    left: rl,
                    op: rop,
                    right: rr,
                } => {
                    assert_eq!(*rop, rekuiper_sql::BinaryOperator::Mul);
                    assert_eq!(**rl, rekuiper_sql::Expr::Identifier("b".to_string()));
                    assert_eq!(**rr, rekuiper_sql::Expr::Literal(json!(2)));
                }
                other => panic!("Expected b * 2 as right operand, got {:?}", other),
            }
        }
        other => panic!("Expected Add expression, got {:?}", other),
    }

    // a=6, b=3: SELECT => 6 + 3*2 = 12, WHERE 6+5=11 > 10 => match
    let mut rec_match = HashMap::new();
    rec_match.insert("a".to_string(), json!(6));
    rec_match.insert("b".to_string(), json!(3));
    let out = Evaluator::eval_select(&stmt, &rec_match).expect("Should match WHERE");
    // SELECT projection computes arithmetic; check via eval_val and via output values
    let computed = Evaluator::eval_val(field, &rec_match);
    assert_eq!(computed, json!(12));
    assert!(
        out.values().any(|v| *v == json!(12)),
        "output {:?} should contain 12",
        out
    );

    // a=4, b=10: SELECT => 4 + 10*2 = 24, WHERE 4+5=9 > 10 false => no match
    let mut rec_nomatch = HashMap::new();
    rec_nomatch.insert("a".to_string(), json!(4));
    rec_nomatch.insert("b".to_string(), json!(10));
    assert!(Evaluator::eval_select(&stmt, &rec_nomatch).is_none());
    // Direct eval still gives 24 (precedence check, not filtered)
    let computed2 = Evaluator::eval_val(field, &rec_nomatch);
    assert_eq!(computed2, json!(24));

    // Additional precedence checks: multiplicative / and % bind like *
    // 10 - 2 * 3 == 4, (10 - 2) * 3 == 24 would differ
    let sql2 = "SELECT * FROM demo WHERE a + b * 2 - c / 2 > 0";
    let mut p2 = Parser::new(sql2);
    let s2 = p2.parse_select().expect("Should parse mixed arithmetic");
    let mut r = HashMap::new();
    r.insert("a".to_string(), json!(1));
    r.insert("b".to_string(), json!(4));
    r.insert("c".to_string(), json!(6));
    // 1 + 8 - 3 = 6 > 0 true
    assert!(Evaluator::eval_select(&s2, &r).is_some());

    // Float support: 5.0 / 2 == 2.5
    let sql3 = "SELECT * FROM demo WHERE x / 2 > 2.0";
    let mut p3 = Parser::new(sql3);
    let s3 = p3.parse_select().expect("Should parse float division");
    let mut rf = HashMap::new();
    rf.insert("x".to_string(), json!(5.0));
    assert!(Evaluator::eval_select(&s3, &rf).is_some());
    rf.insert("x".to_string(), json!(4.0));
    assert!(Evaluator::eval_select(&s3, &rf).is_none());

    // Modulo: 10 % 3 == 1
    let sql4 = "SELECT * FROM demo WHERE a % 3 = 1";
    let mut p4 = Parser::new(sql4);
    let s4 = p4.parse_select().expect("Should parse modulo");
    let mut rm = HashMap::new();
    rm.insert("a".to_string(), json!(10));
    assert!(Evaluator::eval_select(&s4, &rm).is_some());
    rm.insert("a".to_string(), json!(11));
    assert!(Evaluator::eval_select(&s4, &rm).is_none());
}

#[test]
fn test_parentheses() {
    let sql = "SELECT (a + b) * 2 FROM demo WHERE (a > 5 OR b > 5) AND c = 1";
    let mut parser = Parser::new(sql);
    let stmt = parser
        .parse_select()
        .expect("Should parse parenthesized SELECT");

    // SELECT field should be Mul(Add(a,b), 2) — parentheses override precedence
    assert_eq!(stmt.fields.len(), 1);
    match &stmt.fields[0] {
        rekuiper_sql::Expr::BinaryOp { left, op, right } => {
            assert_eq!(*op, rekuiper_sql::BinaryOperator::Mul);
            match &**left {
                rekuiper_sql::Expr::BinaryOp { op: inner_op, .. } => {
                    assert_eq!(*inner_op, rekuiper_sql::BinaryOperator::Add);
                }
                other => panic!("Expected (a + b) as left, got {:?}", other),
            }
            assert_eq!(**right, rekuiper_sql::Expr::Literal(json!(2)));
        }
        other => panic!("Expected Mul expression, got {:?}", other),
    }

    // WHERE should be And(Or(a>5, b>5), c=1)
    match stmt.where_clause.as_ref().expect("WHERE present") {
        rekuiper_sql::Expr::BinaryOp { op, .. } => {
            assert_eq!(*op, rekuiper_sql::BinaryOperator::And);
        }
        other => panic!("Expected AND at top of WHERE, got {:?}", other),
    }

    // (3+4)*2=14, WHERE (F OR F)=F AND T => F => no match
    let mut r1 = HashMap::new();
    r1.insert("a".to_string(), json!(3));
    r1.insert("b".to_string(), json!(4));
    r1.insert("c".to_string(), json!(1));
    assert!(Evaluator::eval_select(&stmt, &r1).is_none());
    assert_eq!(Evaluator::eval_val(&stmt.fields[0], &r1), json!(14));

    // (6+1)*2=14, WHERE (T OR F)=T AND T => T => match
    let mut r2 = HashMap::new();
    r2.insert("a".to_string(), json!(6));
    r2.insert("b".to_string(), json!(1));
    r2.insert("c".to_string(), json!(1));
    let out2 = Evaluator::eval_select(&stmt, &r2).expect("Should match");
    assert!(out2.values().any(|v| *v == json!(14)));

    // (6+1)*2=14, WHERE (T OR F)=T AND F (c=0) => F => no match
    let mut r3 = HashMap::new();
    r3.insert("a".to_string(), json!(6));
    r3.insert("b".to_string(), json!(1));
    r3.insert("c".to_string(), json!(0));
    assert!(Evaluator::eval_select(&stmt, &r3).is_none());

    // Parentheses change OR/AND grouping: without parens, AND binds tighter.
    // (a>5 OR b>5) AND c=1 with a=1,b=10,c=0 => (F OR T)=T AND F => F
    let mut r4 = HashMap::new();
    r4.insert("a".to_string(), json!(1));
    r4.insert("b".to_string(), json!(10));
    r4.insert("c".to_string(), json!(0));
    assert!(Evaluator::eval_select(&stmt, &r4).is_none());
    // Same values with query lacking parens: a>5 OR b>5 AND c=1 == a>5 OR (b>5 AND c=1)
    // => F OR (T AND F) => F still false here; use c=1 to differentiate:
    // paren query: (F OR T) AND T => T; unparen: F OR (T AND T) => T (same). Use a=10,b=1,c=0:
    // paren: (T OR F) AND F => F; unparen: T OR (F AND F) => T (different).
    let sql_noparen = "SELECT * FROM demo WHERE a > 5 OR b > 5 AND c = 1";
    let mut pn = Parser::new(sql_noparen);
    let sn = pn.parse_select().expect("parse no-paren");
    let mut rd = HashMap::new();
    rd.insert("a".to_string(), json!(10));
    rd.insert("b".to_string(), json!(1));
    rd.insert("c".to_string(), json!(0));
    // AND tighter: 10>5=T OR (1>5=F AND 0=1=F) => T OR F => T => match
    assert!(Evaluator::eval_select(&sn, &rd).is_some());
    // With explicit parens forcing (OR first): (10>5 OR 1>5)=T AND 0=1=F => F => no match
    let sql_paren2 = "SELECT * FROM demo WHERE (a > 5 OR b > 5) AND c = 1";
    let mut pp = Parser::new(sql_paren2);
    let sp = pp.parse_select().expect("parse paren2");
    assert!(Evaluator::eval_select(&sp, &rd).is_none());
}

#[test]
fn test_between_and_in() {
    let sql = "SELECT * FROM demo WHERE temp BETWEEN 20 AND 30 AND status IN ('ok', 'warn')";
    let mut parser = Parser::new(sql);
    let stmt = parser.parse_select().expect("Should parse BETWEEN + IN");

    // Top-level WHERE should be AND(Between, InList)
    match stmt.where_clause.as_ref().expect("WHERE present") {
        rekuiper_sql::Expr::BinaryOp { left, op, right } => {
            assert_eq!(*op, rekuiper_sql::BinaryOperator::And);
            assert!(
                matches!(&**left, rekuiper_sql::Expr::Between { negated: false, .. }),
                "left should be Between, got {:?}",
                left
            );
            match &**right {
                rekuiper_sql::Expr::InList { negated, list, .. } => {
                    assert!(!negated);
                    assert_eq!(list.len(), 2);
                }
                other => panic!("right should be InList, got {:?}", other),
            }
        }
        other => panic!("Expected AND at top, got {:?}", other),
    }

    // temp=25, status=ok => match
    let mut r_ok = HashMap::new();
    r_ok.insert("temp".to_string(), json!(25));
    r_ok.insert("status".to_string(), json!("ok"));
    assert!(Evaluator::eval_select(&stmt, &r_ok).is_some());

    // temp=25, status=error => IN fails => no match
    let mut r_bad_status = HashMap::new();
    r_bad_status.insert("temp".to_string(), json!(25));
    r_bad_status.insert("status".to_string(), json!("error"));
    assert!(Evaluator::eval_select(&stmt, &r_bad_status).is_none());

    // temp=35, status=ok => BETWEEN fails => no match
    let mut r_hot = HashMap::new();
    r_hot.insert("temp".to_string(), json!(35));
    r_hot.insert("status".to_string(), json!("ok"));
    assert!(Evaluator::eval_select(&stmt, &r_hot).is_none());

    // Inclusive bounds: 20 and 30 match
    let mut r_low = HashMap::new();
    r_low.insert("temp".to_string(), json!(20));
    r_low.insert("status".to_string(), json!("warn"));
    assert!(Evaluator::eval_select(&stmt, &r_low).is_some());

    let mut r_high = HashMap::new();
    r_high.insert("temp".to_string(), json!(30));
    r_high.insert("status".to_string(), json!("ok"));
    assert!(Evaluator::eval_select(&stmt, &r_high).is_some());

    // Just outside: 19.9 no match
    let mut r_cold = HashMap::new();
    r_cold.insert("temp".to_string(), json!(19.9));
    r_cold.insert("status".to_string(), json!("ok"));
    assert!(Evaluator::eval_select(&stmt, &r_cold).is_none());

    // NOT variants
    let sql_not =
        "SELECT * FROM demo WHERE temp NOT BETWEEN 20 AND 30 AND status NOT IN ('ok', 'warn')";
    let mut pn = Parser::new(sql_not);
    let sn = pn
        .parse_select()
        .expect("Should parse NOT BETWEEN + NOT IN");
    match sn.where_clause.as_ref().unwrap() {
        rekuiper_sql::Expr::BinaryOp { left, right, .. } => {
            assert!(matches!(
                &**left,
                rekuiper_sql::Expr::Between { negated: true, .. }
            ));
            assert!(matches!(
                &**right,
                rekuiper_sql::Expr::InList { negated: true, .. }
            ));
        }
        other => panic!("Expected AND, got {:?}", other),
    }
    // temp=25,status=ok => both negated fail => no match
    assert!(Evaluator::eval_select(&sn, &r_ok).is_none());
    // temp=35,status=error => both negated true => match
    let mut r_both_out = HashMap::new();
    r_both_out.insert("temp".to_string(), json!(35));
    r_both_out.insert("status".to_string(), json!("error"));
    assert!(Evaluator::eval_select(&sn, &r_both_out).is_some());
}

#[test]
fn test_is_null() {
    let sql = "SELECT * FROM demo WHERE err IS NULL";
    let mut parser = Parser::new(sql);
    let stmt = parser.parse_select().expect("Should parse IS NULL");
    assert!(matches!(
        stmt.where_clause.as_ref().unwrap(),
        rekuiper_sql::Expr::IsNull { negated: false, .. }
    ));

    // Explicit null => match
    let mut r_null = HashMap::new();
    r_null.insert("err".to_string(), json!(null));
    r_null.insert("v".to_string(), json!(1));
    assert!(Evaluator::eval_select(&stmt, &r_null).is_some());

    // Missing key treated as NULL => match
    let mut r_missing = HashMap::new();
    r_missing.insert("v".to_string(), json!(1));
    assert!(Evaluator::eval_select(&stmt, &r_missing).is_some());

    // Non-null => no match
    let mut r_val = HashMap::new();
    r_val.insert("err".to_string(), json!("oops"));
    assert!(Evaluator::eval_select(&stmt, &r_val).is_none());

    // IS NOT NULL is the negation
    let sql_not = "SELECT * FROM demo WHERE err IS NOT NULL";
    let mut pn = Parser::new(sql_not);
    let sn = pn.parse_select().expect("Should parse IS NOT NULL");
    assert!(matches!(
        sn.where_clause.as_ref().unwrap(),
        rekuiper_sql::Expr::IsNull { negated: true, .. }
    ));
    assert!(Evaluator::eval_select(&sn, &r_null).is_none());
    assert!(Evaluator::eval_select(&sn, &r_missing).is_none());
    assert!(Evaluator::eval_select(&sn, &r_val).is_some());
}

#[test]
fn test_nested_field_access() {
    let sql = "SELECT dev.temp FROM demo WHERE dev.temp > 25.0";
    let mut parser = Parser::new(sql);
    let stmt = parser
        .parse_select()
        .expect("Should parse nested field access");

    // SELECT field should be FieldAccess(dev, temp)
    assert_eq!(stmt.fields.len(), 1);
    assert!(matches!(
        &stmt.fields[0],
        rekuiper_sql::Expr::FieldAccess { field, .. } if field == "temp"
    ));
    // WHERE should be Gt(FieldAccess, 25.0)
    match stmt.where_clause.as_ref().expect("WHERE present") {
        rekuiper_sql::Expr::BinaryOp { op, left, .. } => {
            assert_eq!(*op, rekuiper_sql::BinaryOperator::Gt);
            assert!(matches!(&**left, rekuiper_sql::Expr::FieldAccess { .. }));
        }
        other => panic!("Expected Gt, got {:?}", other),
    }

    // Nested match: dev.temp=26.5 > 25.0
    let mut r_hot = HashMap::new();
    r_hot.insert("dev".to_string(), json!({"temp": 26.5}));
    let out = Evaluator::eval_select(&stmt, &r_hot).expect("Should match nested");
    // Projection flattens to leaf name "temp"
    assert_eq!(out.get("temp"), Some(&json!(26.5)));

    // dev.temp=20.0 => no match
    let mut r_cold = HashMap::new();
    r_cold.insert("dev".to_string(), json!({"temp": 20.0}));
    assert!(Evaluator::eval_select(&stmt, &r_cold).is_none());

    // Missing nested field => NULL => no match
    let mut r_missing = HashMap::new();
    r_missing.insert("dev".to_string(), json!({"hum": 50}));
    assert!(Evaluator::eval_select(&stmt, &r_missing).is_none());

    // Deeper nesting: device.sensor.temp
    let sql_deep = "SELECT device.sensor.temp FROM demo WHERE device.sensor.temp > 25.0";
    let mut pd = Parser::new(sql_deep);
    let sd = pd.parse_select().expect("Should parse deep nesting");
    // Field should be nested FieldAccess(FieldAccess(device, sensor), temp)
    match &sd.fields[0] {
        rekuiper_sql::Expr::FieldAccess { parent, field } => {
            assert_eq!(field, "temp");
            assert!(matches!(&**parent, rekuiper_sql::Expr::FieldAccess { .. }));
        }
        other => panic!("Expected nested FieldAccess, got {:?}", other),
    }
    let mut r_deep = HashMap::new();
    r_deep.insert("device".to_string(), json!({"sensor": {"temp": 30.0}}));
    assert!(Evaluator::eval_select(&sd, &r_deep).is_some());
    let mut r_deep_cold = HashMap::new();
    r_deep_cold.insert("device".to_string(), json!({"sensor": {"temp": 10.0}}));
    assert!(Evaluator::eval_select(&sd, &r_deep_cold).is_none());
}

#[test]
fn test_math_functions() {
    let empty: HashMap<String, serde_json::Value> = HashMap::new();

    // Helper: parse SELECT <expr> FROM demo and eval the projection.
    let eval_expr = |sql: &str, record: &HashMap<String, serde_json::Value>| -> serde_json::Value {
        let mut p = Parser::new(sql);
        let stmt = p.parse_select().expect("parse math sql");
        assert_eq!(stmt.fields.len(), 1);
        Evaluator::eval_val(&stmt.fields[0], record)
    };

    // abs
    assert_eq!(eval_expr("SELECT abs(-5) FROM demo", &empty), json!(5));
    assert_eq!(eval_expr("SELECT abs(5) FROM demo", &empty), json!(5));
    assert_eq!(eval_expr("SELECT abs(-5.5) FROM demo", &empty), json!(5.5));
    let mut r = HashMap::new();
    r.insert("a".to_string(), json!(-7));
    assert_eq!(eval_expr("SELECT abs(a) FROM demo", &r), json!(7));

    // ceil: returns float (eKuiper math.Ceil)
    assert_eq!(eval_expr("SELECT ceil(2.3) FROM demo", &empty), json!(3.0));
    assert_eq!(
        eval_expr("SELECT ceil(-2.3) FROM demo", &empty),
        json!(-2.0)
    );
    assert_eq!(eval_expr("SELECT ceil(3) FROM demo", &empty), json!(3.0));

    // floor
    assert_eq!(eval_expr("SELECT floor(2.7) FROM demo", &empty), json!(2.0));
    assert_eq!(
        eval_expr("SELECT floor(-2.3) FROM demo", &empty),
        json!(-3.0)
    );

    // round to nearest integer (float result)
    assert_eq!(eval_expr("SELECT round(2.5) FROM demo", &empty), json!(3.0));
    assert_eq!(eval_expr("SELECT round(2.4) FROM demo", &empty), json!(2.0));
    assert_eq!(
        eval_expr("SELECT round(-2.5) FROM demo", &empty),
        json!(-3.0)
    );

    // sqrt
    assert_eq!(eval_expr("SELECT sqrt(9) FROM demo", &empty), json!(3.0));
    assert_eq!(eval_expr("SELECT sqrt(0) FROM demo", &empty), json!(0.0));
    assert_eq!(eval_expr("SELECT sqrt(-1) FROM demo", &empty), json!(null));

    // power (integer inputs with non-negative exponent stay integral)
    assert_eq!(eval_expr("SELECT power(2, 3) FROM demo", &empty), json!(8));
    assert_eq!(
        eval_expr("SELECT power(9, 0.5) FROM demo", &empty),
        json!(3.0)
    );

    // Null propagation
    let mut rn = HashMap::new();
    rn.insert("x".to_string(), json!(null));
    assert_eq!(eval_expr("SELECT abs(x) FROM demo", &rn), json!(null));
    assert_eq!(eval_expr("SELECT sqrt(x) FROM demo", &rn), json!(null));

    // AST shape: abs(-5) is Call
    let mut p = Parser::new("SELECT abs(-5) FROM demo");
    let stmt = p.parse_select().unwrap();
    assert!(matches!(
        &stmt.fields[0],
        rekuiper_sql::Expr::Call { name, args } if name.eq_ignore_ascii_case("abs") && args.len() == 1
    ));
}

#[test]
fn test_string_functions() {
    let empty: HashMap<String, serde_json::Value> = HashMap::new();
    let eval_expr = |sql: &str, record: &HashMap<String, serde_json::Value>| -> serde_json::Value {
        let mut p = Parser::new(sql);
        let stmt = p.parse_select().expect("parse string sql");
        assert_eq!(stmt.fields.len(), 1);
        Evaluator::eval_val(&stmt.fields[0], record)
    };

    // concat
    assert_eq!(
        eval_expr("SELECT concat('a', 'b', 'c') FROM demo", &empty),
        json!("abc")
    );
    assert_eq!(
        eval_expr("SELECT concat('temp:', '25') FROM demo", &empty),
        json!("temp:25")
    );

    // lower / upper
    assert_eq!(
        eval_expr("SELECT lower('ABC') FROM demo", &empty),
        json!("abc")
    );
    assert_eq!(
        eval_expr("SELECT upper('abc') FROM demo", &empty),
        json!("ABC")
    );
    let mut r = HashMap::new();
    r.insert("status".to_string(), json!("Ok"));
    assert_eq!(eval_expr("SELECT lower(status) FROM demo", &r), json!("ok"));
    assert_eq!(eval_expr("SELECT upper(status) FROM demo", &r), json!("OK"));

    // length (char count)
    assert_eq!(
        eval_expr("SELECT length('hello') FROM demo", &empty),
        json!(5)
    );
    assert_eq!(eval_expr("SELECT length('') FROM demo", &empty), json!(0));

    // trim
    assert_eq!(
        eval_expr("SELECT trim('  hi  ') FROM demo", &empty),
        json!("hi")
    );

    // substr 1-indexed: substr(s, start, [len])
    assert_eq!(
        eval_expr("SELECT substr('hello', 1, 2) FROM demo", &empty),
        json!("he")
    );
    assert_eq!(
        eval_expr("SELECT substr('hello', 2) FROM demo", &empty),
        json!("ello")
    );
    assert_eq!(
        eval_expr("SELECT substr('hello', 1) FROM demo", &empty),
        json!("hello")
    );

    // startswith / endswith
    assert_eq!(
        eval_expr("SELECT startswith('hello', 'he') FROM demo", &empty),
        json!(true)
    );
    assert_eq!(
        eval_expr("SELECT startswith('hello', 'lo') FROM demo", &empty),
        json!(false)
    );
    assert_eq!(
        eval_expr("SELECT endswith('hello', 'lo') FROM demo", &empty),
        json!(true)
    );
    assert_eq!(
        eval_expr("SELECT endswith('hello', 'he') FROM demo", &empty),
        json!(false)
    );

    // Nested: upper(concat(a, b))
    let mut rn = HashMap::new();
    rn.insert("a".to_string(), json!("he"));
    rn.insert("b".to_string(), json!("llo"));
    assert_eq!(
        eval_expr("SELECT upper(concat(a, b)) FROM demo", &rn),
        json!("HELLO")
    );
}

#[test]
fn test_cast_function() {
    let empty: HashMap<String, serde_json::Value> = HashMap::new();
    let eval_expr = |sql: &str, record: &HashMap<String, serde_json::Value>| -> serde_json::Value {
        let mut p = Parser::new(sql);
        let stmt = p.parse_select().expect("parse cast sql");
        assert_eq!(stmt.fields.len(), 1);
        Evaluator::eval_val(&stmt.fields[0], record)
    };

    // string -> number
    assert_eq!(
        eval_expr("SELECT cast('123', 'bigint') FROM demo", &empty),
        json!(123)
    );
    assert_eq!(
        eval_expr("SELECT cast('45.6', 'float') FROM demo", &empty),
        json!(45.6)
    );
    // int alias
    assert_eq!(
        eval_expr("SELECT cast('42', 'int') FROM demo", &empty),
        json!(42)
    );
    // number -> string
    assert_eq!(
        eval_expr("SELECT cast(123, 'string') FROM demo", &empty),
        json!("123")
    );
    assert_eq!(
        eval_expr("SELECT cast(45.6, 'string') FROM demo", &empty),
        json!("45.6")
    );
    // boolean casting
    assert_eq!(
        eval_expr("SELECT cast('true', 'boolean') FROM demo", &empty),
        json!(true)
    );
    assert_eq!(
        eval_expr("SELECT cast('false', 'bool') FROM demo", &empty),
        json!(false)
    );
    assert_eq!(
        eval_expr("SELECT cast(1, 'boolean') FROM demo", &empty),
        json!(true)
    );
    assert_eq!(
        eval_expr("SELECT cast(0, 'boolean') FROM demo", &empty),
        json!(false)
    );
    assert_eq!(
        eval_expr("SELECT cast(true, 'string') FROM demo", &empty),
        json!("true")
    );
    // float -> bigint truncates
    assert_eq!(
        eval_expr("SELECT cast(45.9, 'bigint') FROM demo", &empty),
        json!(45)
    );
    // invalid conversion -> Null
    assert_eq!(
        eval_expr("SELECT cast('abc', 'bigint') FROM demo", &empty),
        json!(null)
    );

    // cast over a column
    let mut r = HashMap::new();
    r.insert("v".to_string(), json!("77"));
    assert_eq!(
        eval_expr("SELECT cast(v, 'bigint') FROM demo", &r),
        json!(77)
    );
}

#[test]
fn test_coalesce_function() {
    let eval_expr = |sql: &str, record: &HashMap<String, serde_json::Value>| -> serde_json::Value {
        let mut p = Parser::new(sql);
        let stmt = p.parse_select().expect("parse coalesce sql");
        assert_eq!(stmt.fields.len(), 1);
        Evaluator::eval_val(&stmt.fields[0], record)
    };

    // coalesce(null, missing, 'default') -> 'default'
    let empty: HashMap<String, serde_json::Value> = HashMap::new();
    assert_eq!(
        eval_expr(
            "SELECT coalesce(null, missing, 'default') FROM demo",
            &empty
        ),
        json!("default")
    );
    // first non-null wins
    assert_eq!(
        eval_expr("SELECT coalesce('a', 'b') FROM demo", &empty),
        json!("a")
    );
    // all null -> Null
    assert_eq!(
        eval_expr("SELECT coalesce(null, null) FROM demo", &empty),
        json!(null)
    );
    // column present
    let mut r = HashMap::new();
    r.insert("a".to_string(), json!(null));
    r.insert("b".to_string(), json!(5));
    assert_eq!(
        eval_expr("SELECT coalesce(a, b, 10) FROM demo", &r),
        json!(5)
    );
    let mut r2 = HashMap::new();
    r2.insert("a".to_string(), json!(3));
    r2.insert("b".to_string(), json!(5));
    assert_eq!(eval_expr("SELECT coalesce(a, b) FROM demo", &r2), json!(3));
}

#[test]
fn test_functions_in_where() {
    // Functions in both SELECT projection and WHERE, per spec example.
    let sql = "SELECT abs(temp), upper(status) FROM demo WHERE length(status) > 2";
    let mut p = Parser::new(sql);
    let stmt = p
        .parse_select()
        .expect("Should parse functions in SELECT + WHERE");
    assert_eq!(stmt.fields.len(), 2);
    assert!(matches!(
        &stmt.fields[0],
        rekuiper_sql::Expr::Call { name, .. } if name.eq_ignore_ascii_case("abs")
    ));
    assert!(matches!(
        &stmt.fields[1],
        rekuiper_sql::Expr::Call { name, .. } if name.eq_ignore_ascii_case("upper")
    ));

    // length(status)=2 ('ok') -> 2 > 2 false => filtered
    let mut r_short = HashMap::new();
    r_short.insert("temp".to_string(), json!(-25));
    r_short.insert("status".to_string(), json!("ok"));
    assert!(Evaluator::eval_select(&stmt, &r_short).is_none());

    // length('warn')=4 > 2 true => match, projections computed
    let mut r_long = HashMap::new();
    r_long.insert("temp".to_string(), json!(-25));
    r_long.insert("status".to_string(), json!("warn"));
    let out = Evaluator::eval_select(&stmt, &r_long).expect("Should match");
    assert!(out.values().any(|v| *v == json!(25)));
    assert!(out.values().any(|v| *v == json!("WARN")));

    // Canonical spec example: abs + upper in WHERE
    let sql2 = "SELECT * FROM demo WHERE abs(temp) > 20 AND upper(status) = 'OK'";
    let mut p2 = Parser::new(sql2);
    let stmt2 = p2.parse_select().expect("parse functions_in_where");
    let mut m1 = HashMap::new();
    m1.insert("temp".to_string(), json!(-25));
    m1.insert("status".to_string(), json!("ok"));
    assert!(Evaluator::eval_select(&stmt2, &m1).is_some());

    let mut m2 = HashMap::new();
    m2.insert("temp".to_string(), json!(10));
    m2.insert("status".to_string(), json!("ok"));
    assert!(Evaluator::eval_select(&stmt2, &m2).is_none());

    let mut m3 = HashMap::new();
    m3.insert("temp".to_string(), json!(-30));
    m3.insert("status".to_string(), json!("warn"));
    assert!(Evaluator::eval_select(&stmt2, &m3).is_none());
}

#[test]
fn test_parse_window_syntax() {
    use rekuiper_sql::{TimeUnit, WindowDef};

    // GROUP BY TUMBLINGWINDOW(ss, 10)
    let mut p = Parser::new("SELECT count(*) FROM demo GROUP BY TUMBLINGWINDOW(ss, 10)");
    let stmt = p.parse_select().expect("parse tumbling window");
    assert_eq!(
        stmt.window,
        Some(WindowDef::TumblingTime {
            unit: TimeUnit::Ss,
            length: 10
        })
    );
    assert!(stmt.group_by.is_empty());
    assert!(stmt.having.is_none());

    // Window names are case-insensitive.
    let mut p = Parser::new("SELECT count(*) FROM demo GROUP BY tumblingwindow(SS, 10)");
    let stmt = p.parse_select().expect("parse lowercase tumbling window");
    assert_eq!(
        stmt.window,
        Some(WindowDef::TumblingTime {
            unit: TimeUnit::Ss,
            length: 10
        })
    );

    // GROUP BY COUNTWINDOW(5, 1)
    let mut p = Parser::new("SELECT * FROM demo GROUP BY COUNTWINDOW(5, 1)");
    let stmt = p.parse_select().expect("parse count window with interval");
    assert_eq!(
        stmt.window,
        Some(WindowDef::Count {
            size: 5,
            interval: Some(1)
        })
    );

    // COUNTWINDOW without interval.
    let mut p = Parser::new("SELECT * FROM demo GROUP BY COUNTWINDOW(5)");
    let stmt = p
        .parse_select()
        .expect("parse count window without interval");
    assert_eq!(
        stmt.window,
        Some(WindowDef::Count {
            size: 5,
            interval: None
        })
    );

    // GROUP BY id, SLIDINGWINDOW(mi, 1): id stays in group_by, window is extracted.
    let mut p = Parser::new("SELECT id, count(*) FROM demo GROUP BY id, SLIDINGWINDOW(mi, 1)");
    let stmt = p
        .parse_select()
        .expect("parse sliding window with group key");
    assert_eq!(
        stmt.window,
        Some(WindowDef::SlidingTime {
            unit: TimeUnit::Mi,
            length: 1,
            delay: None,
        })
    );
    assert_eq!(stmt.group_by.len(), 1);
    assert_eq!(
        stmt.group_by[0],
        rekuiper_sql::Expr::Identifier("id".to_string())
    );

    // HOPPINGWINDOW parsing.
    let mut p = Parser::new("SELECT count(*) FROM demo GROUP BY HOPPINGWINDOW(ss, 10, 5)");
    let stmt = p.parse_select().expect("parse hopping window");
    assert_eq!(
        stmt.window,
        Some(WindowDef::HoppingTime {
            unit: TimeUnit::Ss,
            length: 10,
            interval: 5
        })
    );

    // HAVING is captured.
    let mut p =
        Parser::new("SELECT avg(temp) FROM demo GROUP BY COUNTWINDOW(3) HAVING avg(temp) > 25");
    let stmt = p.parse_select().expect("parse having");
    assert!(stmt.having.is_some());
}

#[test]
fn test_eval_aggregate_functions() {
    let records: Vec<HashMap<String, serde_json::Value>> = [20.0, 30.0, 40.0]
        .iter()
        .map(|t| {
            let mut m = HashMap::new();
            m.insert("temp".to_string(), json!(*t));
            m
        })
        .collect();

    let eval_one = |sql: &str| -> serde_json::Value {
        let mut p = Parser::new(sql);
        let stmt = p.parse_select().expect("parse aggregate sql");
        let out = Evaluator::eval_aggregate(&stmt, &records).expect("aggregate output");
        assert_eq!(stmt.fields.len(), 1);
        let key = out
            .keys()
            .next()
            .unwrap_or_else(|| panic!("empty output for {}", sql))
            .clone();
        out[&key].clone()
    };

    assert_eq!(
        eval_one("SELECT count(*) FROM demo GROUP BY COUNTWINDOW(3)"),
        json!(3)
    );
    assert_eq!(
        eval_one("SELECT sum(temp) FROM demo GROUP BY COUNTWINDOW(3)"),
        json!(90.0)
    );
    assert_eq!(
        eval_one("SELECT avg(temp) FROM demo GROUP BY COUNTWINDOW(3)"),
        json!(30.0)
    );
    assert_eq!(
        eval_one("SELECT min(temp) FROM demo GROUP BY COUNTWINDOW(3)"),
        json!(20.0)
    );
    assert_eq!(
        eval_one("SELECT max(temp) FROM demo GROUP BY COUNTWINDOW(3)"),
        json!(40.0)
    );

    // count(col) skips nulls.
    let mut with_null = records.clone();
    let mut null_row = HashMap::new();
    null_row.insert("temp".to_string(), json!(null));
    with_null.push(null_row);
    let mut p = Parser::new("SELECT count(temp) FROM demo GROUP BY COUNTWINDOW(4)");
    let stmt = p.parse_select().unwrap();
    let out = Evaluator::eval_aggregate(&stmt, &with_null).expect("count output");
    assert!(out.values().any(|v| *v == json!(3)));

    // Grouped column values are retained.
    let mut grouped: Vec<HashMap<String, serde_json::Value>> = Vec::new();
    for t in [20.0, 30.0] {
        let mut m = HashMap::new();
        m.insert("id".to_string(), json!("s1"));
        m.insert("temp".to_string(), json!(t));
        grouped.push(m);
    }
    let mut p = Parser::new("SELECT id, avg(temp) FROM demo GROUP BY id, COUNTWINDOW(2)");
    let stmt = p.parse_select().unwrap();
    let out = Evaluator::eval_aggregate(&stmt, &grouped).expect("grouped output");
    assert_eq!(out.get("id"), Some(&json!("s1")));
    assert!(out.values().any(|v| *v == json!(25.0)));
}

#[test]
fn test_eval_having_filter() {
    let hot: Vec<HashMap<String, serde_json::Value>> = [20.0, 30.0, 40.0]
        .iter()
        .map(|t| {
            let mut m = HashMap::new();
            m.insert("temp".to_string(), json!(*t));
            m
        })
        .collect();
    // avg = 30.0 > 25 => match.
    let mut p =
        Parser::new("SELECT avg(temp) FROM demo GROUP BY COUNTWINDOW(3) HAVING avg(temp) > 25");
    let stmt = p.parse_select().expect("parse having match");
    let out = Evaluator::eval_aggregate(&stmt, &hot).expect("having should match");
    assert!(out.values().any(|v| *v == json!(30.0)));

    // avg = 20.0 > 25 is false => filtered out.
    let cold: Vec<HashMap<String, serde_json::Value>> = [18.0, 20.0, 22.0]
        .iter()
        .map(|t| {
            let mut m = HashMap::new();
            m.insert("temp".to_string(), json!(*t));
            m
        })
        .collect();
    assert!(Evaluator::eval_aggregate(&stmt, &cold).is_none());
}

#[test]
fn test_extended_math_functions() {
    let empty: HashMap<String, serde_json::Value> = HashMap::new();
    let eval_expr = |sql: &str, record: &HashMap<String, serde_json::Value>| -> serde_json::Value {
        let mut p = Parser::new(sql);
        let stmt = p.parse_select().expect("parse extended math sql");
        assert_eq!(stmt.fields.len(), 1);
        Evaluator::eval_val(&stmt.fields[0], record)
    };

    assert_eq!(eval_expr("SELECT sin(0) FROM demo", &empty), json!(0.0));
    assert_eq!(eval_expr("SELECT cos(0) FROM demo", &empty), json!(1.0));
    assert_eq!(eval_expr("SELECT exp(0) FROM demo", &empty), json!(1.0));
    assert_eq!(eval_expr("SELECT ln(1) FROM demo", &empty), json!(0.0));
    assert_eq!(eval_expr("SELECT sign(-42) FROM demo", &empty), json!(-1));
    assert_eq!(eval_expr("SELECT mod(10, 3) FROM demo", &empty), json!(1));

    // Spot-check the rest of the trig/log family.
    assert_eq!(eval_expr("SELECT tan(0) FROM demo", &empty), json!(0.0));
    assert_eq!(eval_expr("SELECT asin(0) FROM demo", &empty), json!(0.0));
    assert_eq!(eval_expr("SELECT acos(1) FROM demo", &empty), json!(0.0));
    assert_eq!(eval_expr("SELECT atan(0) FROM demo", &empty), json!(0.0));
    assert_eq!(
        eval_expr("SELECT atan2(0, 1) FROM demo", &empty),
        json!(0.0)
    );
    assert_eq!(eval_expr("SELECT log10(100) FROM demo", &empty), json!(2.0));
    // In eKuiper, log(x) is base-10; ln(x) is natural log.
    assert_eq!(eval_expr("SELECT log(100) FROM demo", &empty), json!(2.0));
    assert_eq!(
        eval_expr("SELECT ln(100) FROM demo", &empty),
        json!(4.605170185988092)
    );
    assert_eq!(eval_expr("SELECT sign(42) FROM demo", &empty), json!(1));
    assert_eq!(eval_expr("SELECT sign(0) FROM demo", &empty), json!(0));

    // Domain errors and nulls yield Null.
    assert_eq!(
        eval_expr("SELECT asin(2) FROM demo", &empty),
        serde_json::Value::Null
    );
    assert_eq!(
        eval_expr("SELECT ln(0) FROM demo", &empty),
        serde_json::Value::Null
    );
    assert_eq!(
        eval_expr("SELECT log10(-1) FROM demo", &empty),
        serde_json::Value::Null
    );
}

#[test]
fn test_extended_string_functions() {
    let empty: HashMap<String, serde_json::Value> = HashMap::new();
    let eval_expr = |sql: &str, record: &HashMap<String, serde_json::Value>| -> serde_json::Value {
        let mut p = Parser::new(sql);
        let stmt = p.parse_select().expect("parse extended string sql");
        assert_eq!(stmt.fields.len(), 1);
        Evaluator::eval_val(&stmt.fields[0], record)
    };

    assert_eq!(
        eval_expr("SELECT ltrim('  hi  ') FROM demo", &empty),
        json!("hi  ")
    );
    assert_eq!(
        eval_expr("SELECT rtrim('  hi  ') FROM demo", &empty),
        json!("  hi")
    );
    assert_eq!(
        eval_expr("SELECT lpad('hi', 4, '0') FROM demo", &empty),
        json!("00hi")
    );
    assert_eq!(
        eval_expr("SELECT rpad('hi', 4, 'x') FROM demo", &empty),
        json!("hixx")
    );
    assert_eq!(
        eval_expr(
            "SELECT replace('hello world', 'world', 'kuiper') FROM demo",
            &empty
        ),
        json!("hello kuiper")
    );
    assert_eq!(
        eval_expr("SELECT split('a,b,c', ',') FROM demo", &empty),
        json!(["a", "b", "c"])
    );
    assert_eq!(
        eval_expr("SELECT reverse('abc') FROM demo", &empty),
        json!("cba")
    );

    // Defaults and edge cases (eKuiper pads N spaces).
    assert_eq!(
        eval_expr("SELECT lpad('hi', 4) FROM demo", &empty),
        json!("    hi")
    );
    assert_eq!(
        eval_expr("SELECT rpad('hi', 2) FROM demo", &empty),
        json!("hi  ")
    );
}

#[test]
fn test_array_object_and_utility_functions() {
    let empty: HashMap<String, serde_json::Value> = HashMap::new();
    let eval_expr = |sql: &str, record: &HashMap<String, serde_json::Value>| -> serde_json::Value {
        let mut p = Parser::new(sql);
        let stmt = p.parse_select().expect("parse array/object sql");
        assert_eq!(stmt.fields.len(), 1);
        Evaluator::eval_val(&stmt.fields[0], record)
    };

    // Arrays/objects come from record columns (the SQL grammar has no
    // array/object literals); this exercises the same function semantics.
    let mut r = HashMap::new();
    r.insert("arr".to_string(), json!([1, 2, 3]));
    r.insert("words".to_string(), json!(["a", "b"]));
    r.insert("obj".to_string(), json!({"x": 1, "y": 2}));

    assert_eq!(
        eval_expr("SELECT array_contains(arr, 2) FROM demo", &r),
        json!(true)
    );
    assert_eq!(
        eval_expr("SELECT array_contains(arr, 9) FROM demo", &r),
        json!(false)
    );
    assert_eq!(
        eval_expr("SELECT array_join(words, '-') FROM demo", &r),
        json!("a-b")
    );
    assert_eq!(
        eval_expr("SELECT array_join(words) FROM demo", &r),
        json!("a,b")
    );

    // keys() order is unspecified; sort before asserting.
    let mut keys = eval_expr("SELECT keys(obj) FROM demo", &r);
    if let Some(arr) = keys.as_array_mut() {
        #[allow(clippy::unnecessary_sort_by)]
        arr.sort_by(|a, b| a.as_str().cmp(&b.as_str()));
    }
    assert_eq!(keys, json!(["x", "y"]));
    let mut values = eval_expr("SELECT values(obj) FROM demo", &r);
    if let Some(arr) = values.as_array_mut() {
        arr.sort_by_key(|a| a.to_string());
    }
    assert_eq!(values, json!([1, 2]));

    assert_eq!(
        eval_expr("SELECT isnan('not_num') FROM demo", &empty),
        json!(false)
    );
    assert_eq!(
        eval_expr("SELECT isnumeric('42.5') FROM demo", &empty),
        json!(true)
    );
    assert_eq!(
        eval_expr("SELECT isnumeric('abc') FROM demo", &empty),
        json!(false)
    );
    assert_eq!(
        eval_expr("SELECT nvl(null, 'default') FROM demo", &empty),
        json!("default")
    );
    assert_eq!(
        eval_expr("SELECT nvl('a', 'b') FROM demo", &empty),
        json!("a")
    );
}

#[test]
fn test_searched_case_expression() {
    let sql = "SELECT CASE WHEN size < 150 THEN 'S' WHEN size < 170 THEN 'M' ELSE 'L' END as sizeLabel FROM tbl";
    let mut parser = Parser::new(sql);
    let stmt = parser.parse_select().expect("Should parse searched CASE");

    let mut r_mid = HashMap::new();
    r_mid.insert("size".to_string(), json!(160));
    let out = Evaluator::eval_select(&stmt, &r_mid).expect("Should match");
    assert_eq!(out.get("sizeLabel"), Some(&json!("M")));
    assert_eq!(out.len(), 1);

    let mut r_big = HashMap::new();
    r_big.insert("size".to_string(), json!(180));
    let out = Evaluator::eval_select(&stmt, &r_big).expect("Should match");
    assert_eq!(out.get("sizeLabel"), Some(&json!("L")));

    let mut r_small = HashMap::new();
    r_small.insert("size".to_string(), json!(140));
    let out = Evaluator::eval_select(&stmt, &r_small).expect("Should match");
    assert_eq!(out.get("sizeLabel"), Some(&json!("S")));
}

#[test]
fn test_simple_case_expression() {
    let sql =
        "SELECT CASE color WHEN 'red' THEN 1 WHEN 'yellow' THEN 2 ELSE 3 END as code FROM tbl";
    let mut parser = Parser::new(sql);
    let stmt = parser.parse_select().expect("Should parse simple CASE");

    let mut r_red = HashMap::new();
    r_red.insert("color".to_string(), json!("red"));
    let out = Evaluator::eval_select(&stmt, &r_red).expect("Should match");
    assert_eq!(out.get("code"), Some(&json!(1)));

    let mut r_blue = HashMap::new();
    r_blue.insert("color".to_string(), json!("blue"));
    let out = Evaluator::eval_select(&stmt, &r_blue).expect("Should match");
    assert_eq!(out.get("code"), Some(&json!(3)));
}

#[test]
fn test_order_by_and_limit_syntax() {
    use rekuiper_sql::SortOrder;

    let sql =
        "SELECT a, b FROM demo WHERE a > 0 GROUP BY countwindow(5) ORDER BY a ASC, b DESC LIMIT 10";
    let mut parser = Parser::new(sql);
    let stmt = parser
        .parse_select()
        .expect("Should parse ORDER BY + LIMIT");
    assert_eq!(stmt.order_by.len(), 2);
    assert_eq!(stmt.order_by[0].order, SortOrder::Asc);
    assert_eq!(stmt.order_by[1].order, SortOrder::Desc);
    assert_eq!(stmt.limit, Some(10));
}

#[test]
fn test_lag_stateful() {
    use rekuiper_sql::RuleState;

    let mut parser = Parser::new("SELECT lag(temp) AS prev FROM demo");
    let stmt = parser.parse_select().expect("Should parse lag");
    let state = RuleState::default();

    let row = |t: serde_json::Value| {
        let mut record = HashMap::new();
        record.insert("temp".to_string(), t);
        Evaluator::eval_select_stateful(&stmt, &record, &state)
            .expect("row always projects")
            .remove("prev")
            .unwrap()
    };

    // No history yet -> Null; then each row sees the previous value.
    assert_eq!(row(json!(10)), serde_json::Value::Null);
    assert_eq!(row(json!(20)), json!(10));
    assert_eq!(row(json!(30)), json!(20));

    // Explicit offset and default.
    let mut parser = Parser::new("SELECT lag(temp, 2, -1) AS prev2 FROM demo");
    let stmt = parser.parse_select().expect("Should parse lag with args");
    let state = RuleState::default();
    let row = |t: serde_json::Value| {
        let mut record = HashMap::new();
        record.insert("temp".to_string(), t);
        Evaluator::eval_select_stateful(&stmt, &record, &state)
            .expect("row always projects")
            .remove("prev2")
            .unwrap()
    };
    assert_eq!(row(json!(10)), json!(-1));
    assert_eq!(row(json!(20)), json!(-1));
    assert_eq!(row(json!(30)), json!(10));
}

#[test]
fn test_unnest_stateful_multi() {
    use rekuiper_sql::RuleState;

    let mut parser = Parser::new("SELECT id, time, unnest(data) FROM demo");
    let stmt = parser.parse_select().expect("Should parse unnest");
    let state = RuleState::default();

    let mut record = HashMap::new();
    record.insert("id".to_string(), json!("id1"));
    record.insert("time".to_string(), json!("2026-01-01"));
    record.insert("data".to_string(), json!([{"k": 1}, {"k": 2}]));

    let rows = Evaluator::eval_select_stateful_multi(&stmt, &record, &state);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("id"), Some(&json!("id1")));
    assert_eq!(rows[0].get("time"), Some(&json!("2026-01-01")));
    assert_eq!(rows[0].get("k"), Some(&json!(1)));
    assert_eq!(rows[1].get("k"), Some(&json!(2)));
    assert_eq!(rows[1].get("id"), Some(&json!("id1")));

    // Without unnest the result is the single projected row.
    let mut parser = Parser::new("SELECT id FROM demo");
    let stmt = parser.parse_select().expect("Should parse plain select");
    let rows = Evaluator::eval_select_stateful_multi(&stmt, &record, &state);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("id"), Some(&json!("id1")));
}

#[test]
fn test_parse_arrow_field_access() {
    use rekuiper_sql::Expr;

    // D7: `a->b` is nested field access equivalent to `a.b`.
    let mut parser = Parser::new("SELECT payload->temperature FROM demo");
    let stmt = parser.parse_select().expect("arrow should parse");
    assert_eq!(stmt.fields.len(), 1);
    match &stmt.fields[0] {
        Expr::FieldAccess { parent, field } => {
            assert_eq!(field, "temperature");
            assert!(matches!(parent.as_ref(), Expr::Identifier(n) if n == "payload"));
        }
        other => panic!("expected FieldAccess, got {:?}", other),
    }

    // Quoted field forms.
    for sql in ["SELECT a->'b' FROM demo", "SELECT a->\"b\" FROM demo"] {
        let mut parser = Parser::new(sql);
        let stmt = parser.parse_select().expect("quoted arrow should parse");
        match &stmt.fields[0] {
            Expr::FieldAccess { field, .. } => assert_eq!(field, "b", "sql: {}", sql),
            other => panic!("expected FieldAccess, got {:?} ({})", other, sql),
        }
    }

    // Chained arrows nest left to right: (a->b)->c.
    let mut parser = Parser::new("SELECT a->b->c FROM demo");
    let stmt = parser.parse_select().expect("chained arrow should parse");
    match &stmt.fields[0] {
        Expr::FieldAccess { parent, field } => {
            assert_eq!(field, "c");
            match parent.as_ref() {
                Expr::FieldAccess { parent, field } => {
                    assert_eq!(field, "b");
                    assert!(matches!(parent.as_ref(), Expr::Identifier(n) if n == "a"));
                }
                other => panic!("expected inner FieldAccess, got {:?}", other),
            }
        }
        other => panic!("expected outer FieldAccess, got {:?}", other),
    }

    // Arrow and dot forms produce identical ASTs and identical evaluation.
    let mut dot = Parser::new("SELECT payload.temperature AS t FROM demo");
    let dot_stmt = dot.parse_select().unwrap();
    let mut arrow = Parser::new("SELECT payload->temperature AS t FROM demo");
    let arrow_stmt = arrow.parse_select().unwrap();
    assert_eq!(dot_stmt.fields, arrow_stmt.fields);
    let mut record = HashMap::new();
    record.insert("payload".to_string(), json!({"temperature": 22.5}));
    let dot_out = Evaluator::eval_select(&dot_stmt, &record).expect("dot eval");
    let arrow_out = Evaluator::eval_select(&arrow_stmt, &record).expect("arrow eval");
    assert_eq!(dot_out, arrow_out);
    assert_eq!(arrow_out.get("t"), Some(&json!(22.5)));

    // Plain subtraction still parses as arithmetic, not navigation.
    let mut parser = Parser::new("SELECT a - b FROM demo");
    let stmt = parser.parse_select().expect("subtraction should parse");
    assert!(
        matches!(
            stmt.fields[0],
            Expr::BinaryOp {
                op: rekuiper_sql::BinaryOperator::Sub,
                ..
            }
        ),
        "got {:?}",
        stmt.fields[0]
    );
}

#[test]
fn test_parse_create_stream_columns() {
    // D6: column names and types are retained from the definition.
    let mut parser =
        Parser::new("CREATE STREAM s (id BIGINT, temp FLOAT, name STRING) WITH (FORMAT=\"json\")");
    let stmt = parser.parse_create_stream().expect("columns should parse");
    assert_eq!(stmt.name, "s");
    assert_eq!(
        stmt.fields,
        vec![
            StreamColumn {
                name: "id".to_string(),
                data_type: "bigint".to_string(),
                default_value: None,
            },
            StreamColumn {
                name: "temp".to_string(),
                data_type: "float".to_string(),
                default_value: None,
            },
            StreamColumn {
                name: "name".to_string(),
                data_type: "string".to_string(),
                default_value: None,
            },
        ]
    );

    // Test with DEFAULT column values
    let mut parser_def = Parser::new(
        "CREATE STREAM s_def (id BIGINT DEFAULT 100, status STRING DEFAULT 'idle', active BOOLEAN DEFAULT true) WITH (FORMAT=\"json\")",
    );
    let stmt_def = parser_def.parse_create_stream().expect("columns with default should parse");
    assert_eq!(
        stmt_def.fields,
        vec![
            StreamColumn {
                name: "id".to_string(),
                data_type: "bigint".to_string(),
                default_value: Some("100".to_string()),
            },
            StreamColumn {
                name: "status".to_string(),
                data_type: "string".to_string(),
                default_value: Some("'idle'".to_string()),
            },
            StreamColumn {
                name: "active".to_string(),
                data_type: "boolean".to_string(),
                default_value: Some("true".to_string()),
            },
        ]
    );

    // Empty parens declare no columns; options still parse.
    let mut parser = Parser::new("CREATE STREAM e () WITH (FORMAT=\"json\")");
    let stmt = parser.parse_create_stream().expect("empty cols parse");
    assert!(stmt.fields.is_empty());
    assert_eq!(stmt.options.get("FORMAT"), Some(&"json".to_string()));

    // Parameterized types survive the comma scan; tables parse likewise.
    let mut parser =
        Parser::new("CREATE TABLE t (id BIGINT, price DECIMAL(10,2)) WITH (TYPE=\"sql\")");
    let stmt = parser.parse_create_table().expect("table cols parse");
    assert_eq!(stmt.fields.len(), 2);
    assert_eq!(stmt.fields[0].name, "id");
    assert_eq!(stmt.fields[1].data_type, "decimal(10,2)");
}

#[test]
fn test_issue23_stream_ddl_type_validation() {
    // 1. All valid catalog types parse successfully
    let mut parser = Parser::new(
        "CREATE STREAM valid_stream (
            id BIGINT,
            temp FLOAT,
            name STRING,
            active BOOLEAN,
            ts DATETIME,
            data BYTEA,
            arr ARRAY(STRING),
            st STRUCT(field1 STRING, field2 INT)
        ) WITH (FORMAT=\"json\")",
    );
    let stmt = parser
        .parse_create_stream()
        .expect("Valid types should parse");
    assert_eq!(stmt.fields.len(), 8);
    assert_eq!(stmt.fields[0].data_type, "bigint");
    assert_eq!(stmt.fields[1].data_type, "float");
    assert_eq!(stmt.fields[2].data_type, "string");
    assert_eq!(stmt.fields[3].data_type, "boolean");
    assert_eq!(stmt.fields[4].data_type, "datetime");
    assert_eq!(stmt.fields[5].data_type, "bytea");
    assert_eq!(stmt.fields[6].data_type, "array(string)");
    assert_eq!(
        stmt.fields[7].data_type,
        "struct(field1 string, field2 int)"
    );

    // 2. Default clause and constraints parse and clean the data type
    let mut parser = Parser::new(
        "CREATE STREAM with_defaults (
            temperature FLOAT DEFAULT 12.0,
            status STRING DEFAULT \"unknown\",
            active BOOLEAN DEFAULT false,
            user_id BIGINT DEFAULT 2,
            dev_id BIGINT NOT NULL
        ) WITH (FORMAT=\"json\")",
    );
    let stmt = parser.parse_create_stream().expect("Defaults should parse");
    assert_eq!(stmt.fields[0].data_type, "float");
    assert_eq!(stmt.fields[1].data_type, "string");
    assert_eq!(stmt.fields[2].data_type, "boolean");
    assert_eq!(stmt.fields[3].data_type, "bigint");
    assert_eq!(stmt.fields[4].data_type, "bigint");

    // 3. Unknown types are rejected with descriptive error
    let mut parser = Parser::new("CREATE STREAM bad (col foobar) WITH (FORMAT=\"json\")");
    let err = parser.parse_create_stream().unwrap_err();
    assert!(
        err.to_string()
            .contains("Unsupported or invalid data type 'foobar' for column 'col'"),
        "Unexpected error: {err}"
    );

    // 4. Invalid nested array type rejected
    let mut parser =
        Parser::new("CREATE STREAM bad_arr (arr ARRAY(invalid_t)) WITH (FORMAT=\"json\")");
    let err = parser.parse_create_stream().unwrap_err();
    assert!(
        err.to_string().contains("Unsupported or invalid data type"),
        "Unexpected error: {err}"
    );

    // 5. Invalid nested struct type rejected
    let mut parser =
        Parser::new("CREATE STREAM bad_st (st STRUCT(a INT, b bad_type)) WITH (FORMAT=\"json\")");
    let err = parser.parse_create_stream().unwrap_err();
    assert!(
        err.to_string().contains("Unsupported or invalid data type"),
        "Unexpected error: {err}"
    );
}

#[test]
fn test_issue24_sql_window_features() {
    use rekuiper_sql::{Evaluator, TimeUnit, WindowDef};
    use serde_json::json;
    use std::collections::HashMap;

    // 1. FILTER (WHERE ...) on window
    let mut p = Parser::new(
        "SELECT count(*) AS c FROM s GROUP BY TUMBLINGWINDOW(ss, 10) FILTER (WHERE temp > 20)",
    );
    let stmt = p.parse_select().expect("should parse window filter");
    assert_eq!(
        stmt.window,
        Some(WindowDef::TumblingTime {
            unit: TimeUnit::Ss,
            length: 10,
        })
    );
    assert!(stmt.window_filter.is_some());

    // 2. OVER (WHEN ...) on sliding window
    let mut p = Parser::new(
        "SELECT count(*) AS c FROM s GROUP BY SLIDINGWINDOW(ss, 5) OVER (WHEN temp > 30)",
    );
    let stmt = p
        .parse_select()
        .expect("should parse sliding window over when");
    assert_eq!(
        stmt.window,
        Some(WindowDef::SlidingTime {
            unit: TimeUnit::Ss,
            length: 5,
            delay: None,
        })
    );
    assert!(stmt.window_trigger_condition.is_some());

    // 3. FILTER + OVER
    let mut p = Parser::new("SELECT * FROM demo GROUP BY SlidingWindow(ss, 1) FILTER (WHERE revenue > 100) OVER (WHEN revenue > 200)");
    let stmt = p
        .parse_select()
        .expect("should parse window with filter and over");
    assert!(stmt.window_filter.is_some());
    assert!(stmt.window_trigger_condition.is_some());

    // 4. STATEWINDOW with 2 conditions and OVER (PARTITION BY ...)
    let mut p =
        Parser::new("SELECT * FROM demo GROUP BY STATEWINDOW(a = 1, a = 5) OVER (PARTITION BY b)");
    let stmt = p
        .parse_select()
        .expect("should parse statewindow with partition");
    assert!(matches!(stmt.window, Some(WindowDef::State { .. })));
    assert!(stmt.window_partition_by.is_some());

    // 5. STATEWINDOW single condition
    let mut p = Parser::new("SELECT * FROM demo GROUP BY STATEWINDOW(had_changed(a))");
    let stmt = p
        .parse_select()
        .expect("should parse statewindow single condition");
    assert!(matches!(
        stmt.window,
        Some(WindowDef::State {
            end_condition: None,
            ..
        })
    ));

    // 6. Stateless single-row aggregate: SELECT count(*) AS c FROM s
    let mut p =
        Parser::new("SELECT count(*) AS c, sum(val) AS s, min(val) AS lo, max(val) AS hi FROM s");
    let stmt = p.parse_select().expect("should parse select count");
    let mut row = HashMap::new();
    row.insert("val".to_string(), json!(42));
    let out = Evaluator::eval_select(&stmt, &row).expect("should project row");
    assert_eq!(out.get("c"), Some(&json!(1)));
    assert_eq!(out.get("s"), Some(&json!(42)));
    assert_eq!(out.get("lo"), Some(&json!(42)));
    assert_eq!(out.get("hi"), Some(&json!(42)));
}

#[test]
fn test_parse_create_stream_reject_garbage() {
    let mut p = Parser::new("CREATE STREAM cli_bad this is not a definition");
    let err = p.parse_create_stream().unwrap_err();
    assert_eq!(
        err.to_string(),
        "found \"this\", expected lparen after stream name."
    );

    let mut p = Parser::new("CREATE STREAM cli_bad \"this is not a definition\"");
    let err = p.parse_create_stream().unwrap_err();
    assert_eq!(
        err.to_string(),
        "found \"this\", expected lparen after stream name."
    );

    let mut p = Parser::new("CREATE TABLE cli_bad this is not a definition");
    let err = p.parse_create_table().unwrap_err();
    assert_eq!(
        err.to_string(),
        "found \"this\", expected lparen after table name."
    );
}
