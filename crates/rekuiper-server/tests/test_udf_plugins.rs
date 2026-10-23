use rekuiper_core::{get_global_udf_registry, MemKvStore, PluginDefinition, PluginManager, UdfFn};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

fn cube_handler(args: &[Value]) -> Value {
    let Some(x) = args.first() else {
        return Value::Null;
    };
    if let Some(i) = x.as_i64() {
        return match i.checked_pow(3) {
            Some(n) => Value::from(n),
            None => Value::Null,
        };
    }
    if let Some(f) = x.as_f64() {
        return json!(f.powi(3));
    }
    Value::Null
}

fn clamp_handler(args: &[Value]) -> Value {
    if args.len() != 3 {
        return Value::Null;
    }
    let (Some(lo), Some(hi)) = (args[1].as_f64(), args[2].as_f64()) else {
        return Value::Null;
    };
    let Some(v) = args[0].as_f64() else {
        return Value::Null;
    };
    // Preserve integers when every input is integral.
    if args.iter().all(|a| a.is_i64()) {
        let (v, lo, hi) = (v as i64, lo as i64, hi as i64);
        return Value::from(v.clamp(lo.min(hi), lo.max(hi)));
    }
    json!(v.clamp(lo.min(hi), lo.max(hi)))
}

fn math_plugin() -> PluginDefinition {
    PluginDefinition {
        name: "my_math".to_string(),
        plugin_type: "function".to_string(),
        file: None,
        description: Some("test math plugin".to_string()),
        functions: vec!["cube".to_string(), "clamp".to_string()],
    }
}

fn eval_project(sql: &str, record: &HashMap<String, Value>, alias: &str) -> Value {
    let mut parser = rekuiper_sql::Parser::new(sql);
    let stmt = parser.parse_select().expect("Should parse");
    rekuiper_sql::Evaluator::eval_select(&stmt, record)
        .expect("Should project")
        .shift_remove(alias)
        .unwrap()
}

#[test]
fn test_udf_scalar_cube_and_clamp() {
    let registry = get_global_udf_registry();
    registry.register_udf("cube", Arc::new(cube_handler) as UdfFn);
    registry.register_udf("clamp", Arc::new(clamp_handler) as UdfFn);

    // Unknown names still resolve to Null.
    assert_eq!(
        eval_project(
            "SELECT nosuchfn(val) AS v FROM demo",
            &HashMap::from([("val".to_string(), json!(3))]),
            "v"
        ),
        Value::Null
    );

    // SELECT cube(val) AS cubed FROM demo with {"val": 3} -> {"cubed": 27}.
    assert_eq!(
        eval_project(
            "SELECT cube(val) AS cubed FROM demo",
            &HashMap::from([("val".to_string(), json!(3))]),
            "cubed"
        ),
        json!(27)
    );

    // SELECT clamp(temp, 10, 30) AS clamped FROM demo: 45 clamps to 30.
    assert_eq!(
        eval_project(
            "SELECT clamp(temp, 10, 30) AS clamped FROM demo",
            &HashMap::from([("temp".to_string(), json!(45))]),
            "clamped"
        ),
        json!(30)
    );
    // In-range values pass through; UDFs compose with builtins.
    assert_eq!(
        eval_project(
            "SELECT clamp(cube(n), 0, 100) AS v FROM demo",
            &HashMap::from([("n".to_string(), json!(2))]),
            "v"
        ),
        json!(8)
    );
}

#[tokio::test]
async fn test_plugin_definition_crud_and_reload() {
    let kv: Arc<dyn rekuiper_core::KvStore> = Arc::new(MemKvStore::new());
    let manager = PluginManager::new_with_kv(kv.clone());

    // Create + read.
    manager.register_plugin(math_plugin()).await.unwrap();
    let def = manager.get_plugin("my_math").expect("stored");
    assert_eq!(def.plugin_type, "function");
    assert_eq!(def.functions, vec!["cube".to_string(), "clamp".to_string()]);

    // Empty names are rejected.
    let mut bad = math_plugin();
    bad.name = "  ".to_string();
    assert!(manager.register_plugin(bad).await.is_err());

    // List filters by type.
    assert_eq!(manager.list_plugins("function").len(), 1);
    assert!(manager.list_plugins("udf").is_empty());

    // Instance-level handler dispatch (independent of the global registry).
    manager.register_udf("cube", Arc::new(cube_handler) as UdfFn);
    assert_eq!(manager.call_udf("cube", &[json!(4)]), Some(json!(64)));
    assert_eq!(manager.call_udf("CUBE", &[json!(4)]), Some(json!(64)));
    assert_eq!(manager.call_udf("missing", &[json!(4)]), None);

    // Simulate restart: a fresh manager reloads definitions from KV...
    let restarted = PluginManager::new_with_kv(kv.clone());
    assert!(restarted.get_plugin("my_math").is_none());
    restarted.load_from_kv(&kv).await.unwrap();
    assert!(restarted.get_plugin("my_math").is_some());
    // ...but handlers are code and must be re-registered.
    assert_eq!(restarted.call_udf("cube", &[json!(4)]), None);
    restarted.register_udf("cube", Arc::new(cube_handler) as UdfFn);
    assert_eq!(restarted.call_udf("cube", &[json!(4)]), Some(json!(64)));

    // Delete removes the definition (and KV row).
    restarted.delete_plugin("my_math").await.unwrap();
    assert!(restarted.get_plugin("my_math").is_none());
    assert!(restarted.delete_plugin("my_math").await.is_err());
    let reloaded = PluginManager::new();
    reloaded.load_from_kv(&kv).await.unwrap();
    assert!(reloaded.get_plugin("my_math").is_none());
}
