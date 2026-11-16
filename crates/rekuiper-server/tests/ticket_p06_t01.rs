use rekuiper_core::model::{GraphDefinition, GraphNode, GraphTopo, StreamRecord};
use rekuiper_server::testing::{FixtureMode, ProductionFixtureRunner};
use serde_json::json;
use std::collections::HashMap;
use std::time::Duration;

fn row(pairs: &[(&str, serde_json::Value)]) -> HashMap<String, serde_json::Value> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect()
}

#[tokio::test]
async fn p06_t01_production_operators_timers_isolation_and_network_block() {
    // 1. Transform + stateful window driven by TestClock.
    let mut runner1 = ProductionFixtureRunner::default();
    let projected = runner1
        .run_sql_transform(
            "SELECT temp * 2 AS doubled FROM s WHERE temp > 10",
            &[row(&[("temp", json!(5))]), row(&[("temp", json!(15))])],
        )
        .unwrap();
    assert_eq!(projected.len(), 1);
    assert_eq!(projected[0].get("doubled"), Some(&json!(30)));

    runner1.schedule_timer("t1", Duration::from_secs(5), json!({"ping": 1}));
    assert!(runner1.poll_ready_timers().is_empty());
    runner1.advance_clock(Duration::from_secs(5));
    let fired = runner1.poll_ready_timers();
    assert_eq!(fired.len(), 1);
    assert_eq!(fired[0].0, "t1");

    runner1.push_window_event(Duration::from_secs(10), row(&[("v", json!(10))]));
    runner1.push_window_event(Duration::from_secs(10), row(&[("v", json!(20))]));
    assert!(runner1
        .flush_window_if_due("SELECT count(*) AS c FROM s")
        .unwrap()
        .is_empty());
    runner1.advance_clock(Duration::from_secs(10));
    let wins = runner1
        .flush_window_if_due("SELECT count(*) AS c FROM s")
        .unwrap();
    assert_eq!(wins.len(), 1);
    assert_eq!(wins[0].get("c"), Some(&json!(2)));

    // 2. Production DAG validation + SwitchNode operator.
    let mut nodes = HashMap::new();
    nodes.insert(
        "src".to_owned(),
        GraphNode {
            node_type: "mqtt".to_owned(),
            node_category: "source".to_owned(),
            props: HashMap::new(),
        },
    );
    let mut sw_props = HashMap::new();
    sw_props.insert("cases".to_owned(), json!(["temp > 20"]));
    nodes.insert(
        "sw".to_owned(),
        GraphNode {
            node_type: "switch".to_owned(),
            node_category: "operator".to_owned(),
            props: sw_props,
        },
    );
    nodes.insert(
        "sink".to_owned(),
        GraphNode {
            node_type: "log".to_owned(),
            node_category: "sink".to_owned(),
            props: HashMap::new(),
        },
    );
    let mut edges = HashMap::new();
    edges.insert(
        "src".to_owned(),
        rekuiper_core::model::EdgeTargets::Single(vec!["sw".to_owned()]),
    );
    edges.insert(
        "sw".to_owned(),
        rekuiper_core::model::EdgeTargets::Single(vec!["sink".to_owned()]),
    );
    let graph = GraphDefinition {
        nodes,
        topo: GraphTopo {
            sources: vec!["src".to_owned()],
            edges,
        },
    };
    let rec = StreamRecord {
        data: row(&[("temp", json!(25))]).into(),
        meta: None,
        timestamp: 1000,
    };
    let branch = runner1
        .validate_and_step_dag(&graph, Some("sw"), &rec)
        .unwrap();
    assert_eq!(branch, Some(0));

    // 3. Default mode blocks live network effects, while capture sink records effects.
    let net_err = runner1
        .attempt_live_network_effect("https://example.invalid/webhook")
        .expect_err("live network must fail in default mode");
    assert!(net_err.contains("prohibited"));

    runner1
        .capture_effect("eff_1", "act_1", b"ok".to_vec())
        .await
        .unwrap();
    assert_eq!(runner1.captured_dispatches().len(), 1);

    // 4. Case isolation: a fresh runner has empty timers, empty capture sink, and clock=0.
    let mut runner2 = ProductionFixtureRunner::new(FixtureMode::IsolatedDeterministic);
    assert!(runner2.captured_dispatches().is_empty());
    assert!(runner2.poll_ready_timers().is_empty());
}

#[test]
fn p06_t01_fixture_window_applies_production_where_filtering() {
    let mut runner = ProductionFixtureRunner::new(FixtureMode::IsolatedDeterministic);
    let win = Duration::from_secs(10);
    runner.push_window_event(win, HashMap::from([("v".to_owned(), serde_json::json!(1))]));
    runner.push_window_event(win, HashMap::from([("v".to_owned(), serde_json::json!(2))]));

    runner.advance_clock(win);
    let outs = runner
        .flush_window_if_due(
            "SELECT count(*) AS c FROM s WHERE v > 1 GROUP BY TumblingWindow(ss, 10)",
        )
        .unwrap();
    assert_eq!(outs.len(), 1);
    assert_eq!(outs[0].get("c").and_then(|v| v.as_i64()), Some(1));
}

#[test]
fn p06_t01_fixture_window_returns_all_grouped_and_projected_rows() {
    let mut runner = ProductionFixtureRunner::new(FixtureMode::IsolatedDeterministic);
    let win = Duration::from_secs(10);
    runner.push_window_event(
        win,
        HashMap::from([("k".to_owned(), json!("a")), ("v".to_owned(), json!(1))]),
    );
    runner.push_window_event(
        win,
        HashMap::from([("k".to_owned(), json!("a")), ("v".to_owned(), json!(2))]),
    );
    runner.push_window_event(
        win,
        HashMap::from([("k".to_owned(), json!("b")), ("v".to_owned(), json!(3))]),
    );

    runner.advance_clock(win);
    let grouped = runner
        .flush_window_if_due("SELECT k, count(*) AS c FROM s GROUP BY k, TumblingWindow(ss, 10)")
        .unwrap();
    assert_eq!(grouped.len(), 2, "grouped window must return all groups");
    assert_eq!(grouped[0].get("k"), Some(&json!("a")));
    assert_eq!(grouped[0].get("c").and_then(|v| v.as_i64()), Some(2));
    assert_eq!(grouped[1].get("k"), Some(&json!("b")));
    assert_eq!(grouped[1].get("c").and_then(|v| v.as_i64()), Some(1));
}
