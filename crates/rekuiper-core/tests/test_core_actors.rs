use rekuiper_core::{
    compile_graph_to_sql_and_actions, GraphDefinition, GraphNode, GraphTopo, RuleDefinition,
    RuleManager, StreamBus,
};
use serde_json::json;
use std::collections::HashMap;

fn rule_def(id: &str) -> RuleDefinition {
    RuleDefinition {
        id: id.to_string(),
        name: None,
        version: None,
        sql: "SELECT * FROM demo".to_string(),
        actions: vec![],
        options: None,
        graph: None,
        tags: vec![],
    }
}

fn node(category: &str, node_type: &str, props: serde_json::Value) -> GraphNode {
    GraphNode {
        node_category: category.to_string(),
        node_type: node_type.to_string(),
        props: props
            .as_object()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect(),
    }
}

fn dag(
    nodes: Vec<(&str, GraphNode)>,
    sources: Vec<&str>,
    edges: Vec<(&str, Vec<&str>)>,
) -> GraphDefinition {
    GraphDefinition {
        nodes: nodes.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
        topo: GraphTopo {
            sources: sources.into_iter().map(|s| s.to_string()).collect(),
            edges: edges
                .into_iter()
                .map(|(k, v)| {
                    (
                        k.to_string(),
                        v.into_iter().map(|t| t.to_string()).collect(),
                    )
                })
                .collect::<HashMap<_, _>>(),
        },
    }
}

// ---------------------------------------------------------------------------
// Rule lifecycle
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_rule_lifecycle() {
    let mgr = RuleManager::new(StreamBus::new());

    // Create -> running.
    mgr.create_rule(rule_def("r1")).await.expect("create");
    assert_eq!(mgr.get_rule_status("r1").unwrap().status, "running");

    // Duplicate create fails; unknown ids fail everywhere.
    assert!(mgr.create_rule(rule_def("r1")).await.is_err());
    assert!(mgr.start_rule("missing").await.is_err());
    assert!(mgr.stop_rule("missing").await.is_err());
    assert!(mgr.delete_rule("missing").await.is_err());

    // Stop -> stopped; start -> running again.
    mgr.stop_rule("r1").await.expect("stop");
    assert_eq!(mgr.get_rule_status("r1").unwrap().status, "stopped");
    mgr.start_rule("r1").await.expect("start");
    assert_eq!(mgr.get_rule_status("r1").unwrap().status, "running");

    // Restart lands back on running.
    mgr.restart_rule("r1").await.expect("restart");
    assert_eq!(mgr.get_rule_status("r1").unwrap().status, "running");
    assert!(mgr.get_rule("r1").is_some());

    // Delete removes definition and status; restart then fails.
    mgr.delete_rule("r1").await.expect("delete");
    assert!(mgr.get_rule("r1").is_none());
    assert!(mgr.get_rule_status("r1").is_none());
    assert!(mgr.restart_rule("r1").await.is_err());
}

// ---------------------------------------------------------------------------
// Visual graph DAG compilation & validation
// ---------------------------------------------------------------------------

#[test]
fn test_dag_linear_compiles() {
    let graph = dag(
        vec![
            (
                "src",
                node("source", "stream", json!({"sourceName": "demo"})),
            ),
            (
                "f",
                node("operator", "filter", json!({"expr": "temp > 25"})),
            ),
            ("out", node("sink", "log", json!({}))),
        ],
        vec!["src"],
        vec![("src", vec!["f"]), ("f", vec!["out"])],
    );
    let (sql, actions) = compile_graph_to_sql_and_actions(&graph).expect("linear DAG compiles");
    assert_eq!(sql, "SELECT * FROM demo WHERE (temp > 25)");
    assert_eq!(actions.len(), 1);
    assert!(actions[0].contains_key("log"));
}

#[test]
fn test_dag_branching_compiles() {
    // Diamond: src fans out to two filters, both sink to out.
    let graph = dag(
        vec![
            (
                "src",
                node("source", "stream", json!({"sourceName": "demo"})),
            ),
            (
                "hot",
                node("operator", "filter", json!({"expr": "temp > 30"})),
            ),
            (
                "cold",
                node("operator", "filter", json!({"expr": "temp < 10"})),
            ),
            ("out", node("sink", "log", json!({}))),
        ],
        vec!["src"],
        vec![
            ("src", vec!["hot", "cold"]),
            ("hot", vec!["out"]),
            ("cold", vec!["out"]),
        ],
    );
    let (sql, actions) = compile_graph_to_sql_and_actions(&graph).expect("diamond compiles");
    assert!(sql.contains("FROM demo"), "unexpected sql: {}", sql);
    assert!(
        sql.contains("(temp > 30)") && sql.contains("(temp < 10)"),
        "unexpected sql: {}",
        sql
    );
    assert_eq!(actions.len(), 1);
}

#[test]
fn test_dag_cycle_rejected() {
    // A -> B -> A must not compile.
    let graph = dag(
        vec![
            ("a", node("source", "stream", json!({"sourceName": "demo"}))),
            ("b", node("operator", "filter", json!({"expr": "temp > 0"}))),
        ],
        vec!["a"],
        vec![("a", vec!["b"]), ("b", vec!["a"])],
    );
    let err = compile_graph_to_sql_and_actions(&graph).expect_err("cycle must fail");
    assert!(err.contains("cycle"), "unexpected error: {}", err);

    // Self-loops fail too.
    let graph = dag(
        vec![("a", node("source", "stream", json!({})))],
        vec!["a"],
        vec![("a", vec!["a"])],
    );
    assert!(compile_graph_to_sql_and_actions(&graph).is_err());

    // A graph with no source at all fails differently but still fails.
    let graph = dag(vec![], vec![], vec![]);
    assert!(compile_graph_to_sql_and_actions(&graph).is_err());
}

// ---------------------------------------------------------------------------
// Bounded stream bus broadcast
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_stream_bus_broadcast() {
    use rekuiper_core::StreamRecord;

    let bus = StreamBus::new();
    let mut rx1 = bus.subscribe("events");
    let mut rx2 = bus.subscribe("events");

    let n = 50;
    for i in 0..n {
        let mut data = HashMap::new();
        data.insert("seq".to_string(), json!(i));
        bus.publish(
            "events",
            StreamRecord {
                timestamp: i as i64,
                data: data.into(),
            },
        )
        .expect("publish");
    }

    // Both listeners observe every event in order.
    for rx in [&mut rx1, &mut rx2] {
        for i in 0..n {
            let record = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
                .await
                .expect("timed out")
                .expect("channel closed");
            assert_eq!(record.data.get("seq"), Some(&json!(i)));
        }
    }

    // Topics are isolated from each other.
    let mut other = bus.subscribe("other");
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), other.recv())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn test_stream_bus_bounded_no_overwrite() {
    use rekuiper_core::{StreamBus, StreamRecord, STREAM_QUEUE_CAPACITY};
    use std::collections::HashMap;

    // A stalled subscriber applies backpressure instead of losing records:
    // non-blocking feedback publish reports Full (nothing admitted), and
    // capacity returns after the subscriber drains.
    let bus = StreamBus::new();
    let mut rx = bus.subscribe("bounded");
    let record = || {
        let mut data = HashMap::new();
        data.insert("seq".to_string(), json!(1));
        StreamRecord::new(data)
    };
    for _ in 0..STREAM_QUEUE_CAPACITY {
        bus.try_publish("bounded", record())
            .expect("capacity must hold");
    }
    assert!(
        matches!(
            bus.try_publish("bounded", record()),
            Err(rekuiper_core::PublishError::Full)
        ),
        "full queue must reject explicitly, never overwrite"
    );
    // Drain one slot: admission succeeds again, order preserved.
    let first = rx.recv().await.expect("queued record must survive");
    assert_eq!(first.data.get("seq"), Some(&json!(1)));
    bus.try_publish("bounded", record())
        .expect("drained capacity must admit");
}
