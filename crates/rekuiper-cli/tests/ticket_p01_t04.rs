use rekuiper_cli::validate_manifest_with_plans;
use rekuiper_core::model::{DeclarativeManifest, DeclarativeRules, RuleDefinition};
use serde_json::json;
use std::collections::{BTreeMap, HashMap};

#[test]
fn cli_manifest_validation_uses_shared_typed_planner_and_redacts_secrets() {
    let mut streams = BTreeMap::new();
    streams.insert(
        "mem_in".to_string(),
        "CREATE STREAM mem_in (temp float) WITH (TYPE=\"memory\")".to_string(),
    );

    let invalid_manifest = DeclarativeManifest {
        version: "v1".to_string(),
        streams: streams.clone(),
        tables: BTreeMap::new(),
        rules: DeclarativeRules::List(vec![RuleDefinition {
            id: "durable_mem_rule".to_string(),
            name: None,
            sql: "SELECT temp FROM mem_in".to_string(),
            actions: vec![HashMap::from([(
                "log".to_string(),
                json!({ "password": "secret" }),
            )])],
            options: Some(HashMap::from([(
                "deliveryGuarantee".to_string(),
                json!("at_least_once"),
            )])),
            graph: None,
            tags: Vec::new(),
            version: None,
        }]),
        connections: BTreeMap::new(),
        schemas: BTreeMap::new(),
    };

    let errs = validate_manifest_with_plans(&invalid_manifest)
        .expect_err("durable rule over memory stream must fail CLI validation");
    assert_eq!(errs[0]["code"], "PLAN_MISSING_REPLAY");
    assert_eq!(errs[0]["component"], "source:mem_in");

    let valid_manifest = DeclarativeManifest {
        version: "v1".to_string(),
        streams,
        tables: BTreeMap::new(),
        rules: DeclarativeRules::List(vec![RuleDefinition {
            id: "ok_rule".to_string(),
            name: None,
            sql: "SELECT temp FROM mem_in".to_string(),
            actions: vec![HashMap::from([(
                "mqtt".to_string(),
                json!({ "server": "tcp://user:pass123@broker:1883", "token": "tok-xyz" }),
            )])],
            options: None,
            graph: None,
            tags: Vec::new(),
            version: None,
        }]),
        connections: BTreeMap::new(),
        schemas: BTreeMap::new(),
    };

    let (_summary, plans) =
        validate_manifest_with_plans(&valid_manifest).expect("lightweight rule should pass");
    assert_eq!(plans.len(), 1);
    let serialized = serde_json::to_string(&plans[0]).unwrap();
    assert!(!serialized.contains("pass123"));
    assert!(!serialized.contains("tok-xyz"));
}
