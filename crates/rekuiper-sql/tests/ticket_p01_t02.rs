use rekuiper_sql::ast::{BinaryOperator, Expr};
use rekuiper_sql::eval::Evaluator;
use rekuiper_sql::functions::{builtin_function_descriptors, descriptor_for_function};
use rekuiper_sql::parser::Parser;
use rekuiper_sql::planning::{
    type_check_expr, Determinism, ExprType, SchemaEnv, SideEffect, Statefulness, TypeCheckError,
};
use serde_json::{json, Value};
use std::collections::HashMap;

#[test]
fn typed_planning_rejects_invalid_calls_and_strict_unknown_fields() {
    let reg = builtin_function_descriptors();
    let strict_schema = SchemaEnv::strict()
        .with_field("temp", ExprType::Float, false)
        .with_field("label", ExprType::String, true);

    // Invalid argument type: abs("not-a-number")
    let bad_type = Expr::Call {
        name: "abs".to_string(),
        args: vec![Expr::Identifier("label".to_string())],
    };
    assert!(matches!(
        type_check_expr(&bad_type, &strict_schema, reg),
        Err(TypeCheckError::TypeMismatch { .. })
    ));

    // Invalid arity: abs(1, 2)
    let bad_arity = Expr::Call {
        name: "abs".to_string(),
        args: vec![Expr::Literal(json!(1)), Expr::Literal(json!(2))],
    };
    assert!(matches!(
        type_check_expr(&bad_arity, &strict_schema, reg),
        Err(TypeCheckError::InvalidArity { .. })
    ));

    // Nullability violation: round(1.5, NULL)
    let bad_null = Expr::Call {
        name: "round".to_string(),
        args: vec![Expr::Literal(json!(1.5)), Expr::Literal(Value::Null)],
    };
    assert!(matches!(
        type_check_expr(&bad_null, &strict_schema, reg),
        Err(TypeCheckError::NullabilityViolation { .. })
    ));

    // Unknown field in strict schema
    let unknown_field = Expr::Identifier("missing".to_string());
    assert!(matches!(
        type_check_expr(&unknown_field, &strict_schema, reg),
        Err(TypeCheckError::UnknownField { .. })
    ));

    // Valid typed call succeeds
    let valid = Expr::Call {
        name: "abs".to_string(),
        args: vec![Expr::Identifier("temp".to_string())],
    };
    let typed = type_check_expr(&valid, &strict_schema, reg).expect("valid typed expr");
    assert_eq!(typed.ty, ExprType::Numeric);
    assert!(!typed.nullable);
}

#[test]
fn descriptors_distinguish_pure_nondeterministic_stateful_and_unknown_functions() {
    let abs_desc = descriptor_for_function("abs");
    assert_eq!(abs_desc.determinism, Determinism::Deterministic);
    assert_eq!(abs_desc.side_effect, SideEffect::Pure);
    assert_eq!(abs_desc.statefulness, Statefulness::Stateless);
    assert!(abs_desc.allows_durable_replay());
    assert!(abs_desc.allows_pure_rewrite());

    let uuid_desc = descriptor_for_function("newuuid");
    assert_eq!(uuid_desc.determinism, Determinism::NonDeterministic);
    assert!(!uuid_desc.allows_durable_replay());
    assert!(!uuid_desc.allows_pure_rewrite());

    let lag_desc = descriptor_for_function("lag");
    assert_eq!(
        lag_desc.statefulness,
        Statefulness::Stateful {
            supports_snapshot: true
        }
    );
    assert!(lag_desc.allows_durable_replay());
    assert!(!lag_desc.allows_pure_rewrite());

    // Missing metadata cannot silently enable durable replay or unsafe rewrites.
    let unknown = descriptor_for_function("unregistered_custom_fn");
    assert_eq!(unknown.determinism, Determinism::NonDeterministic);
    assert_eq!(unknown.side_effect, SideEffect::MutateExternal);
    assert!(!unknown.allows_durable_replay());
    assert!(!unknown.allows_pure_rewrite());
    assert!(unknown.capabilities.supported.is_empty());
}

#[test]
fn legacy_schemaless_expressions_remain_compatible_and_executable() {
    let reg = builtin_function_descriptors();
    let schemaless = SchemaEnv::schemaless();
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Call {
            name: "abs".to_string(),
            args: vec![Expr::Identifier("temp".to_string())],
        }),
        op: BinaryOperator::Add,
        right: Box::new(Expr::Literal(json!(2))),
    };

    let typed = type_check_expr(&expr, &schemaless, reg).expect("schemaless planning passes");
    assert_eq!(typed.ty, ExprType::Numeric);

    let mut row = HashMap::new();
    row.insert("temp".to_string(), json!(-5));
    let stmt = Parser::new("SELECT abs(temp) + 2 AS out FROM stream")
        .parse_select()
        .expect("parse sql");
    let evaluated = Evaluator::eval_select(&stmt, &row).expect("legacy eval executes");
    assert_eq!(evaluated.get("out"), Some(&json!(7)));
}
