use rekuiper_core::manager::{RuleManager, StreamManager, TableManager};
use rekuiper_core::model::StreamDefinition;
use rekuiper_core::{resolve_buffer_full_policy, BufferFullPolicy, StreamBus, StreamRecord};
use rekuiper_server::handlers::rules::reject_invalid_rule;
use rekuiper_server::state::AppState;
use rekuiper_sql::parser::Parser;
use serde_json::json;
use std::collections::HashMap;

#[tokio::test]
async fn policy_resolution_mixed_policy_and_reliable_lossy_rejection() {
    let mut empty = HashMap::new();
    assert_eq!(
        resolve_buffer_full_policy(&empty).unwrap(),
        BufferFullPolicy::Block
    );
    empty.insert("BUFFER_FULL_POLICY".to_string(), "dropOldest".to_string());
    assert_eq!(
        resolve_buffer_full_policy(&empty).unwrap(),
        BufferFullPolicy::DropOldest
    );
    empty.insert("BUFFER_FULL_POLICY".to_string(), "invalid_val".to_string());
    assert!(resolve_buffer_full_policy(&empty).is_err());

    let bus = StreamBus::new();
    let sm = StreamManager::new();
    let mut drop_opts = HashMap::new();
    drop_opts.insert("BUFFER_FULL_POLICY".to_string(), "dropOldest".to_string());
    sm.create_stream(StreamDefinition {
        name: "lossy_in".to_string(),
        sql: "CREATE STREAM lossy_in () WITH (BUFFER_FULL_POLICY=\"dropOldest\")".to_string(),
        stream_fields: Vec::new(),
        options: drop_opts,
    })
    .await
    .unwrap();

    let state = AppState::new(
        "test".to_string(),
        rekuiper_conf::KuiperConfig::default(),
        sm,
        TableManager::new(),
        RuleManager::new(bus.clone()),
        bus,
    );

    let stmt = Parser::new("SELECT * FROM lossy_in")
        .parse_select()
        .unwrap();

    // Best-effort rule against lossy stream is allowed
    assert!(reject_invalid_rule(&state, &stmt, None, false).is_none());

    // Reliable plan against lossy stream is rejected
    let mut reliable_opts = HashMap::new();
    reliable_opts.insert("deliveryGuarantee".to_string(), json!("at_least_once"));
    assert!(reject_invalid_rule(&state, &stmt, Some(&reliable_opts), false).is_some());
}

#[tokio::test]
async fn drop_oldest_subscriber_evicts_and_exposes_per_stream_metrics() {
    let bus = StreamBus::new();
    let mut rx = bus.subscribe_lossy("s_lossy", 2);

    for i in 1..=5_i64 {
        let mut map = HashMap::new();
        map.insert("id".to_string(), json!(i));
        let rec = StreamRecord::new(map);
        bus.publish_async("s_lossy", rec).await.unwrap();
    }

    let metrics = bus.stream_buffer_metrics("s_lossy");
    assert_eq!(metrics.depth, 2);
    assert_eq!(metrics.dropped, 3);

    let first = rx.recv().await.unwrap();
    let second = rx.recv().await.unwrap();
    assert_eq!(first.data.get("id"), Some(&json!(4)));
    assert_eq!(second.data.get("id"), Some(&json!(5)));
}
