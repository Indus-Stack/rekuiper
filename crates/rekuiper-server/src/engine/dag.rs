use parking_lot::RwLock;
use rekuiper_core::model::{EdgeTargets, GraphDefinition, StreamRecord};
use rekuiper_core::{RuleCounters, RuleManager, RuleStatus, StreamBus};
use rekuiper_sql::eval::Evaluator;
use rekuiper_sql::parse_expression;
use std::collections::HashMap;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc::{channel, Receiver, Sender};

use crate::comms::CommsHub;
use crate::engine::is_rule_running;
use crate::engine::operators::{RateLimitNode, SwitchNode, WatchdogNode};

/// Spawns a native DAG rule execution pipeline.
/// Each node in `graph.nodes` runs as a decoupled async actor connected by bounded MPSC channels.
///
/// Features:
/// 1. Multi-port selective routing for Switch nodes (no flattening into broadcast fan-out).
/// 2. Native Switch, RateLimit, and Watchdog operators.
/// 3. Instrumented Wire-Tap probes at every `:node_id` exit boundary.
#[allow(clippy::too_many_arguments)]
pub async fn run_graph_rule(
    rule_id: String,
    graph: GraphDefinition,
    stream_bus: StreamBus,
    _rule_manager: RuleManager,
    counters: Arc<RuleCounters>,
    running: Arc<RwLock<RuleStatus>>,
    comms_hub: CommsHub,
    buffer_len: usize,
) {
    let mut senders: HashMap<String, Sender<StreamRecord>> = HashMap::new();
    let mut receivers: HashMap<String, Receiver<StreamRecord>> = HashMap::new();

    // 1. Allocate bounded channel for every non-source node
    for (node_id, node) in &graph.nodes {
        if node.node_category != "source" {
            let (tx, rx) = channel::<StreamRecord>(buffer_len);
            senders.insert(node_id.clone(), tx);
            receivers.insert(node_id.clone(), rx);
        }
    }

    let senders = Arc::new(senders);
    let edges = Arc::new(graph.topo.edges.clone());

    // Helper to route records to targets
    fn send_to_port_targets(
        senders: &Arc<HashMap<String, Sender<StreamRecord>>>,
        edges: &Arc<HashMap<String, EdgeTargets>>,
        node_id: &str,
        port: usize,
        record: StreamRecord,
    ) {
        if let Some(target_edges) = edges.get(node_id) {
            let targets = target_edges.targets_for_port(port);
            for target in targets {
                if let Some(tx) = senders.get(target) {
                    let _ = tx.try_send(record.clone());
                }
            }
        }
    }

    // Helper to abort all child actor tasks when the parent task aborts or completes
    struct TaskAbortGuard(Vec<tokio::task::JoinHandle<()>>);
    impl Drop for TaskAbortGuard {
        fn drop(&mut self) {
            for h in &self.0 {
                h.abort();
            }
        }
    }

    let mut child_tasks = Vec::new();

    // 2. Spawn operator and sink node actors
    for (node_id, node) in graph.nodes.clone() {
        if node.node_category == "source" {
            continue;
        }

        let Some(mut rx) = receivers.remove(&node_id) else {
            continue;
        };

        let node_id_clone = node_id.clone();
        let rule_id_clone = rule_id.clone();
        let comms_hub = comms_hub.clone();
        let senders = senders.clone();
        let edges = edges.clone();
        let counters = counters.clone();
        let running = running.clone();

        child_tasks.push(tokio::spawn(async move {
            match node.node_category.as_str() {
                "operator" => {
                    match node.node_type.as_str() {
                        "switch" => {
                            let switch = match SwitchNode::from_props(&node.props) {
                                Ok(s) => s,
                                Err(e) => {
                                    tracing::error!(
                                        "[RULE {}][NODE {}] Invalid switch config: {}",
                                        rule_id_clone,
                                        node_id_clone,
                                        e
                                    );
                                    return;
                                }
                            };

                            while let Some(record) = rx.recv().await {
                                if !is_rule_running(&running) {
                                    break;
                                }
                                if let Some(port) = switch.route(&record) {
                                    // Wire-tap probe at switch node exit
                                    comms_hub.maybe_sample(
                                        &rule_id_clone,
                                        &node_id_clone,
                                        &record.data,
                                        record.timestamp,
                                    );
                                    send_to_port_targets(
                                        &senders,
                                        &edges,
                                        &node_id_clone,
                                        port,
                                        record,
                                    );
                                } else {
                                    counters.inc_filtered(1);
                                }
                            }
                        }
                        "rate_limit" => {
                            let limiter = RateLimitNode::from_props(&node.props);
                            while let Some(record) = rx.recv().await {
                                if !is_rule_running(&running) {
                                    break;
                                }
                                if limiter.acquire().await {
                                    comms_hub.maybe_sample(
                                        &rule_id_clone,
                                        &node_id_clone,
                                        &record.data,
                                        record.timestamp,
                                    );
                                    send_to_port_targets(
                                        &senders,
                                        &edges,
                                        &node_id_clone,
                                        0,
                                        record,
                                    );
                                } else {
                                    counters.inc_filtered(1);
                                }
                            }
                        }
                        "watchdog" => {
                            let watchdog = WatchdogNode::from_props(&node_id_clone, &node.props);
                            let mut last_activity = Instant::now();
                            let mut interval = tokio::time::interval(watchdog.timeout);
                            // Consume first tick
                            interval.tick().await;

                            loop {
                                tokio::select! {
                                    rec = rx.recv() => {
                                        let Some(record) = rec else { break; };
                                        if !is_rule_running(&running) { break; }
                                        last_activity = Instant::now();
                                        comms_hub.maybe_sample(&rule_id_clone, &node_id_clone, &record.data, record.timestamp);
                                        send_to_port_targets(&senders, &edges, &node_id_clone, 0, record);
                                    }
                                    _ = interval.tick() => {
                                        if !is_rule_running(&running) { break; }
                                        if last_activity.elapsed() >= watchdog.timeout {
                                            let alert_map: indexmap::IndexMap<String, serde_json::Value> = match watchdog.alert_payload.clone() {
                                                serde_json::Value::Object(m) => m.into_iter().collect(),
                                                other => {
                                                    let mut m = indexmap::IndexMap::new();
                                                    m.insert("alert".to_string(), other);
                                                    m
                                                }
                                            };
                                            let alert_rec = StreamRecord::new(alert_map);
                                            comms_hub.maybe_sample(&rule_id_clone, &node_id_clone, &alert_rec.data, alert_rec.timestamp);
                                            send_to_port_targets(&senders, &edges, &node_id_clone, 0, alert_rec);
                                            if !watchdog.repeat {
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        "filter" => {
                            let expr_str = node
                                .props
                                .get("expr")
                                .and_then(|v| v.as_str())
                                .unwrap_or("true");
                            let expr = match parse_expression(expr_str) {
                                Ok(e) => e,
                                Err(err) => {
                                    tracing::error!(
                                        "[RULE {}][NODE {}] Filter expr error: {}",
                                        rule_id_clone,
                                        node_id_clone,
                                        err
                                    );
                                    return;
                                }
                            };
                            while let Some(record) = rx.recv().await {
                                if !is_rule_running(&running) {
                                    break;
                                }
                                if Evaluator::eval_bool(&expr, &record) {
                                    comms_hub.maybe_sample(
                                        &rule_id_clone,
                                        &node_id_clone,
                                        &record.data,
                                        record.timestamp,
                                    );
                                    send_to_port_targets(
                                        &senders,
                                        &edges,
                                        &node_id_clone,
                                        0,
                                        record,
                                    );
                                } else {
                                    counters.inc_filtered(1);
                                }
                            }
                        }
                        "pick" => {
                            let fields: Vec<String> = node
                                .props
                                .get("fields")
                                .and_then(|v| v.as_array())
                                .map(|arr| {
                                    arr.iter()
                                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                        .collect()
                                })
                                .unwrap_or_default();

                            while let Some(record) = rx.recv().await {
                                if !is_rule_running(&running) {
                                    break;
                                }
                                let out_rec = if !fields.is_empty() {
                                    let mut new_map = indexmap::IndexMap::new();
                                    for f in &fields {
                                        if let Some(v) = record.data.get(f) {
                                            new_map.insert(f.clone(), v.clone());
                                        }
                                    }
                                    StreamRecord::with_timestamp_and_meta(
                                        new_map,
                                        record.timestamp,
                                        record.meta.clone(),
                                    )
                                } else {
                                    record
                                };
                                comms_hub.maybe_sample(
                                    &rule_id_clone,
                                    &node_id_clone,
                                    &out_rec.data,
                                    out_rec.timestamp,
                                );
                                send_to_port_targets(
                                    &senders,
                                    &edges,
                                    &node_id_clone,
                                    0,
                                    out_rec,
                                );
                            }
                        }
                        _ => {
                            // Default passthrough operator
                            while let Some(record) = rx.recv().await {
                                if !is_rule_running(&running) {
                                    break;
                                }
                                comms_hub.maybe_sample(
                                    &rule_id_clone,
                                    &node_id_clone,
                                    &record.data,
                                    record.timestamp,
                                );
                                send_to_port_targets(
                                    &senders,
                                    &edges,
                                    &node_id_clone,
                                    0,
                                    record,
                                );
                            }
                        }
                    }
                }
                "sink" => {
                    // Terminal sink node: capture output and update counters
                    while let Some(record) = rx.recv().await {
                        if !is_rule_running(&running) {
                            break;
                        }
                        comms_hub.maybe_sample(
                            &rule_id_clone,
                            &node_id_clone,
                            &record.data,
                            record.timestamp,
                        );
                        counters.sink_out.fetch_add(1, Relaxed);
                    }
                }
                _ => {}
            }
        }));
    }

    // 3. Spawn source node actors
    for (node_id, node) in graph.nodes.clone() {
        if node.node_category != "source" {
            continue;
        }

        let topic = node
            .props
            .get("datasource")
            .or_else(|| node.props.get("sourceName"))
            .or_else(|| node.props.get("topic"))
            .and_then(|v| v.as_str())
            .unwrap_or(&node_id)
            .to_string();

        let mut bus_rx = stream_bus.subscribe_with_capacity(&topic, buffer_len);
        let node_id_clone = node_id.clone();
        let rule_id_clone = rule_id.clone();
        let comms_hub = comms_hub.clone();
        let senders = senders.clone();
        let edges = edges.clone();
        let counters = counters.clone();
        let running = running.clone();

        child_tasks.push(tokio::spawn(async move {
            while let Some(record) = bus_rx.recv().await {
                if !is_rule_running(&running) {
                    break;
                }
                counters.inc_source(1);
                // Wire-tap probe at source output boundary
                comms_hub.maybe_sample(
                    &rule_id_clone,
                    &node_id_clone,
                    &record.data,
                    record.timestamp,
                );
                send_to_port_targets(&senders, &edges, &node_id_clone, 0, record);
            }
        }));
    }

    let _guard = TaskAbortGuard(child_tasks);
    while is_rule_running(&running) {
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
}

pub(crate) fn spawn_graph_rule_task(
    rule_manager: &RuleManager,
    stream_bus: &StreamBus,
    rule_id: String,
    graph: GraphDefinition,
    comms_hub: CommsHub,
    buffer_len: usize,
) {
    let loop_counters = rule_manager
        .rule_counters(&rule_id)
        .unwrap_or_else(|| Arc::new(RuleCounters::default()));
    let loop_running = rule_manager
        .rule_status_handle(&rule_id)
        .unwrap_or_else(|| Arc::new(RwLock::new(RuleStatus::default())));

    let handle = tokio::spawn(run_graph_rule(
        rule_id.clone(),
        graph,
        stream_bus.clone(),
        rule_manager.clone(),
        loop_counters,
        loop_running,
        comms_hub,
        buffer_len,
    ));

    rule_manager.set_rule_handle(&rule_id, handle);
}

#[cfg(test)]
mod tests {
    use super::*;
    use rekuiper_core::model::{EdgeTargets, GraphDefinition, GraphNode, GraphTopo};
    use serde_json::json;

    #[tokio::test]
    async fn test_dag_multi_port_switch_and_wiretap() {
        let stream_bus = StreamBus::new();
        let rule_mgr = RuleManager::new(stream_bus.clone());
        let comms_hub = CommsHub::new(64);

        // Build DAG:
        // src -> switch1:
        //   port 0 (temp > 50) -> sink_high
        //   port 1 (temp <= 50) -> sink_low
        let mut nodes = HashMap::new();
        nodes.insert(
            "src".to_string(),
            GraphNode {
                node_category: "source".to_string(),
                node_type: "mqtt".to_string(),
                props: [("datasource".to_string(), json!("demo_topic"))]
                    .into_iter()
                    .collect(),
            },
        );
        nodes.insert(
            "switch1".to_string(),
            GraphNode {
                node_category: "operator".to_string(),
                node_type: "switch".to_string(),
                props: [("cases".to_string(), json!(["temp > 50", "temp <= 50"]))]
                    .into_iter()
                    .collect(),
            },
        );
        nodes.insert(
            "sink_high".to_string(),
            GraphNode {
                node_category: "sink".to_string(),
                node_type: "log".to_string(),
                props: HashMap::new(),
            },
        );
        nodes.insert(
            "sink_low".to_string(),
            GraphNode {
                node_category: "sink".to_string(),
                node_type: "log".to_string(),
                props: HashMap::new(),
            },
        );

        let mut edges = HashMap::new();
        edges.insert(
            "src".to_string(),
            EdgeTargets::Single(vec!["switch1".to_string()]),
        );
        // Multi-port output: port 0 -> sink_high, port 1 -> sink_low
        edges.insert(
            "switch1".to_string(),
            EdgeTargets::Multi(vec![
                vec!["sink_high".to_string()],
                vec!["sink_low".to_string()],
            ]),
        );

        let graph = GraphDefinition {
            nodes,
            topo: GraphTopo {
                sources: vec!["src".to_string()],
                edges,
            },
        };

        let counters = Arc::new(RuleCounters::default());
        let running = Arc::new(RwLock::new(RuleStatus {
            status: "running".to_string(),
            ..RuleStatus::default()
        }));

        // Enable wire-tap on switch1 and sink_high
        comms_hub.enable_tap("rule_test", "switch1");
        comms_hub.enable_tap("rule_test", "sink_high");

        let mut comms_rx = comms_hub.subscribe();

        // Spawn DAG
        tokio::spawn(run_graph_rule(
            "rule_test".to_string(),
            graph,
            stream_bus.clone(),
            rule_mgr,
            counters.clone(),
            running.clone(),
            comms_hub.clone(),
            64,
        ));

        // Allow actors to spin up
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Publish high temp record (temp = 75)
        let rec_high =
            StreamRecord::from_hashmap([("temp".to_string(), json!(75))].into_iter().collect());
        let _ = stream_bus.publish("demo_topic", rec_high);

        // Verify wire-tap events
        let mut tap_events = Vec::new();
        for _ in 0..2 {
            if let Ok(Ok(ev)) =
                tokio::time::timeout(tokio::time::Duration::from_millis(500), comms_rx.recv()).await
            {
                tap_events.push(ev);
            }
        }

        assert_eq!(
            tap_events.len(),
            2,
            "Expected switch1 and sink_high wire-tap samples"
        );
        match &tap_events[0] {
            crate::comms::CommsEvent::TupleSample { node_id, data, .. } => {
                assert_eq!(node_id, "switch1");
                assert_eq!(data["temp"], 75);
            }
            _ => panic!("Expected TupleSample"),
        }
        match &tap_events[1] {
            crate::comms::CommsEvent::TupleSample { node_id, data, .. } => {
                assert_eq!(node_id, "sink_high");
                assert_eq!(data["temp"], 75);
            }
            _ => panic!("Expected TupleSample"),
        }
    }
}
