use parking_lot::RwLock;
use rekuiper_core::model::{EdgeTargets, GraphDefinition, StreamRecord};
use rekuiper_core::{RuleCounters, RuleManager, RuleStatus, StreamBus};
use rekuiper_sql::eval::Evaluator;
use rekuiper_sql::parse_expression;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{channel, Receiver, Sender};

use crate::comms::CommsHub;
use crate::engine::is_rule_running;
use crate::engine::operators::{
    RateLimitEnqueueOutcome, RateLimitNode, RateLimitOverflowPolicy, RateLimitStrategy, SwitchNode,
    WatchdogNode,
};
use rekuiper_core::contracts::clock::DeadlineCancellation;

fn compute_backpressure_targets(graph: &GraphDefinition) -> HashSet<String> {
    let mut backpressure_rl_nodes: Vec<String> = Vec::new();
    for (node_id, node) in &graph.nodes {
        if node.node_category == "operator" && node.node_type == "rate_limit" {
            if let Ok(rl) = RateLimitNode::try_from_props(&node.props) {
                let cfg = rl.config();
                if cfg.strategy == RateLimitStrategy::Buffer
                    && cfg.overflow_policy == RateLimitOverflowPolicy::Backpressure
                {
                    backpressure_rl_nodes.push(node_id.clone());
                }
            }
        }
    }
    if backpressure_rl_nodes.is_empty() {
        return HashSet::new();
    }

    let mut reverse_adj: HashMap<&str, Vec<&str>> = HashMap::new();
    let mut forward_adj: HashMap<&str, Vec<&str>> = HashMap::new();
    for (src, targets) in &graph.topo.edges {
        for target in targets.all_targets() {
            reverse_adj.entry(target).or_default().push(src.as_str());
            forward_adj.entry(src.as_str()).or_default().push(target);
        }
    }

    let mut covered: HashSet<String> = HashSet::new();
    let mut queue: VecDeque<&str> = VecDeque::new();
    for rl_id in &backpressure_rl_nodes {
        if covered.insert(rl_id.clone()) {
            queue.push_back(rl_id.as_str());
        }
    }
    while let Some(curr) = queue.pop_front() {
        if let Some(parents) = reverse_adj.get(curr) {
            for &parent in parents {
                if graph
                    .nodes
                    .get(parent)
                    .is_some_and(|n| n.node_category != "source")
                    && covered.insert(parent.to_string())
                {
                    queue.push_back(parent);
                }
            }
        }
    }

    let mut fwd_queue: VecDeque<&str> = backpressure_rl_nodes.iter().map(String::as_str).collect();
    let mut fwd_seen: HashSet<&str> = fwd_queue.iter().copied().collect();
    while let Some(curr) = fwd_queue.pop_front() {
        if let Some(children) = forward_adj.get(curr) {
            for &child in children {
                covered.insert(child.to_string());
                if fwd_seen.insert(child) {
                    fwd_queue.push_back(child);
                }
            }
        }
    }

    covered
}

async fn send_to_port_targets(
    senders: &HashMap<String, Sender<StreamRecord>>,
    edges: &HashMap<String, EdgeTargets>,
    backpressure_targets: &HashSet<String>,
    running: &Arc<RwLock<RuleStatus>>,
    node_id: &str,
    port: usize,
    record: StreamRecord,
) {
    let Some(target_edges) = edges.get(node_id) else {
        return;
    };
    let targets = target_edges.targets_for_port(port);
    for target in targets {
        let Some(tx) = senders.get(target) else {
            continue;
        };
        if backpressure_targets.contains(target) {
            let mut send_fut = std::pin::pin!(tx.send(record.clone()));
            loop {
                tokio::select! {
                    res = &mut send_fut => {
                        let _ = res;
                        break;
                    }
                    () = tokio::time::sleep(Duration::from_millis(20)) => {
                        if !is_rule_running(running) {
                            break;
                        }
                    }
                }
            }
        } else {
            let _ = tx.try_send(record.clone());
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn emit_dag_record(
    comms_hub: &CommsHub,
    rule_id: &str,
    node_id: &str,
    senders: &HashMap<String, Sender<StreamRecord>>,
    edges: &HashMap<String, EdgeTargets>,
    backpressure_targets: &HashSet<String>,
    running: &Arc<RwLock<RuleStatus>>,
    port: usize,
    record: StreamRecord,
) {
    comms_hub.maybe_sample(rule_id, node_id, &record.data, record.timestamp);
    send_to_port_targets(
        senders,
        edges,
        backpressure_targets,
        running,
        node_id,
        port,
        record,
    )
    .await;
}

fn halt_graph_rule(
    running: &Arc<RwLock<RuleStatus>>,
    counters: &Arc<RuleCounters>,
    err_msg: String,
) {
    counters.record_exception(&err_msg);
    let mut st = running.write();
    st.status = "stopped".to_string();
    st.last_stop_timestamp = chrono::Utc::now().timestamp_millis();
    st.message = format!("halted on error: {err_msg}");
    st.last_exception = err_msg;
}

/// Validates configuration properties for native DAG operator nodes.
pub fn validate_graph_nodes(graph: &GraphDefinition) -> Result<(), String> {
    for (node_id, node) in &graph.nodes {
        if node.node_category != "operator" {
            continue;
        }
        match node.node_type.as_str() {
            "rate_limit" => {
                RateLimitNode::try_from_props(&node.props)
                    .map_err(|e| format!("Invalid rate_limit config for node '{node_id}': {e}"))?;
            }
            "switch" => {
                SwitchNode::from_props(&node.props)
                    .map_err(|e| format!("Invalid switch config for node '{node_id}': {e}"))?;
            }
            "filter" => {
                let expr_str = node
                    .props
                    .get("expr")
                    .and_then(|v| v.as_str())
                    .unwrap_or("true");
                parse_expression(expr_str)
                    .map_err(|e| format!("Invalid filter expr for node '{node_id}': {e}"))?;
            }
            _ => {}
        }
    }
    validate_graph_acyclic(graph)?;
    Ok(())
}

/// Rejects feedback cycles and self-loops in a graph topology.
pub fn validate_graph_acyclic(graph: &GraphDefinition) -> Result<(), String> {
    let mut nodes: Vec<String> = graph.nodes.keys().cloned().collect();
    let mut edge_pairs: Vec<(String, String)> = Vec::new();
    for (src, targets) in &graph.topo.edges {
        if !nodes.contains(src) {
            nodes.push(src.clone());
        }
        for t in targets.all_targets() {
            if !nodes.iter().any(|n| n == t) {
                nodes.push(t.to_string());
            }
            let pair = (src.clone(), t.to_string());
            if !edge_pairs.contains(&pair) {
                edge_pairs.push(pair);
            }
        }
    }
    crate::engine::barrier_alignment::validate_acyclic_reliable_graph(&nodes, &edge_pairs)
        .map_err(|e| e.to_string())
}

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
    if let Err(err_msg) = validate_graph_nodes(&graph) {
        tracing::error!(
            rule_id = %rule_id,
            error = %err_msg,
            "Stopping graph rule due to invalid node configuration"
        );
        halt_graph_rule(&running, &counters, err_msg);
        return;
    }

    let mut senders: HashMap<String, Sender<StreamRecord>> = HashMap::new();
    let mut receivers: HashMap<String, Receiver<StreamRecord>> = HashMap::new();
    let channel_cap = buffer_len.max(1);

    // 1. Allocate bounded channel for every non-source node
    for (node_id, node) in &graph.nodes {
        if node.node_category != "source" {
            let (tx, rx) = channel::<StreamRecord>(channel_cap);
            senders.insert(node_id.clone(), tx);
            receivers.insert(node_id.clone(), rx);
        }
    }

    let senders = Arc::new(senders);
    let edges = Arc::new(graph.topo.edges.clone());
    let backpressure_targets = Arc::new(compute_backpressure_targets(&graph));

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
        let backpressure_targets = backpressure_targets.clone();
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
                                    let msg = format!(
                                        "[RULE {}][NODE {}] Invalid switch config: {}",
                                        rule_id_clone, node_id_clone, e
                                    );
                                    tracing::error!("{}", msg);
                                    halt_graph_rule(&running, &counters, msg);
                                    return;
                                }
                            };

                            while let Some(record) = rx.recv().await {
                                if !is_rule_running(&running) {
                                    break;
                                }
                                if let Some(port) = switch.route(&record) {
                                    emit_dag_record(
                                        &comms_hub,
                                        &rule_id_clone,
                                        &node_id_clone,
                                        &senders,
                                        &edges,
                                        &backpressure_targets,
                                        &running,
                                        port,
                                        record,
                                    )
                                    .await;
                                } else {
                                    counters.inc_filtered(1);
                                }
                            }
                        }
                        "rate_limit" => {
                            let limiter = match RateLimitNode::try_from_props(&node.props) {
                                Ok(l) => l,
                                Err(err) => {
                                    let msg = format!(
                                        "Invalid rate_limit configuration for node '{}': {}",
                                        node_id_clone, err
                                    );
                                    tracing::error!(
                                        rule_id = %rule_id_clone,
                                        node_id = %node_id_clone,
                                        error = %err,
                                        "Invalid rate_limit configuration"
                                    );
                                    halt_graph_rule(&running, &counters, msg);
                                    return;
                                }
                            };
                            let budget = limiter.default_budget();
                            let mut buffer = limiter.into_buffer::<StreamRecord>(budget);
                            let cancellation = DeadlineCancellation::default();

                            loop {
                                if !is_rule_running(&running) {
                                    cancellation.cancel();
                                    buffer.clear();
                                    break;
                                }
                                let has_pending = !buffer.is_empty();
                                tokio::select! {
                                    biased;
                                    () = cancellation.cancelled() => {
                                        buffer.clear();
                                        break;
                                    }
                                    res = buffer.wait_next_token(&cancellation), if has_pending => {
                                        if res.is_err() {
                                            buffer.clear();
                                            break;
                                        }
                                        for record in buffer.drain_ready() {
                                            emit_dag_record(
                                                &comms_hub,
                                                &rule_id_clone,
                                                &node_id_clone,
                                                &senders,
                                                &edges,
                                                &backpressure_targets,
                                                &running,
                                                0,
                                                record,
                                            )
                                            .await;
                                        }
                                    }
                                    rec = rx.recv() => {
                                        let Some(record) = rec else {
                                            while !buffer.is_empty() && is_rule_running(&running) {
                                                if buffer.wait_next_token(&cancellation).await.is_err() {
                                                    break;
                                                }
                                                for record in buffer.drain_ready() {
                                                    emit_dag_record(
                                                        &comms_hub,
                                                        &rule_id_clone,
                                                        &node_id_clone,
                                                        &senders,
                                                        &edges,
                                                        &backpressure_targets,
                                                        &running,
                                                        0,
                                                        record,
                                                    )
                                                    .await;
                                                }
                                            }
                                            buffer.clear();
                                            break;
                                        };
                                        if !is_rule_running(&running) {
                                            cancellation.cancel();
                                            buffer.clear();
                                            break;
                                        }
                                        let est_bytes = (record.data.len() as u64)
                                            .saturating_mul(64)
                                            .max(64);
                                        let mut outcome = buffer.enqueue(record, est_bytes);
                                        loop {
                                            match outcome {
                                                RateLimitEnqueueOutcome::AdmitNow(record) => {
                                                    emit_dag_record(
                                                        &comms_hub,
                                                        &rule_id_clone,
                                                        &node_id_clone,
                                                        &senders,
                                                        &edges,
                                                        &backpressure_targets,
                                                        &running,
                                                        0,
                                                        record,
                                                    )
                                                    .await;
                                                    break;
                                                }
                                                RateLimitEnqueueOutcome::Buffered { .. } => break,
                                                RateLimitEnqueueOutcome::BackpressureWait(pending) => {
                                                    if !is_rule_running(&running)
                                                        || buffer
                                                            .wait_next_token(&cancellation)
                                                            .await
                                                            .is_err()
                                                    {
                                                        cancellation.cancel();
                                                        buffer.clear();
                                                        return;
                                                    }
                                                    for ready_rec in buffer.drain_ready() {
                                                        emit_dag_record(
                                                            &comms_hub,
                                                            &rule_id_clone,
                                                            &node_id_clone,
                                                            &senders,
                                                            &edges,
                                                            &backpressure_targets,
                                                            &running,
                                                            0,
                                                            ready_rec,
                                                        )
                                                        .await;
                                                    }
                                                    outcome = buffer.enqueue(pending, est_bytes);
                                                }
                                                RateLimitEnqueueOutcome::Dropped => {
                                                    counters.inc_filtered(1);
                                                    break;
                                                }
                                                RateLimitEnqueueOutcome::OverflowError(msg) => {
                                                    tracing::warn!(
                                                        rule_id = %rule_id_clone,
                                                        node_id = %node_id_clone,
                                                        error = %msg,
                                                        "rate_limit buffer overflow"
                                                    );
                                                    counters.inc_exceptions(1);
                                                    counters.inc_filtered(1);
                                                    break;
                                                }
                                            }
                                        }
                                    }
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
                                        emit_dag_record(
                                            &comms_hub,
                                            &rule_id_clone,
                                            &node_id_clone,
                                            &senders,
                                            &edges,
                                            &backpressure_targets,
                                            &running,
                                            0,
                                            record,
                                        )
                                        .await;
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
                                            emit_dag_record(
                                                &comms_hub,
                                                &rule_id_clone,
                                                &node_id_clone,
                                                &senders,
                                                &edges,
                                                &backpressure_targets,
                                                &running,
                                                0,
                                                alert_rec,
                                            )
                                            .await;
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
                                    let msg = format!(
                                        "[RULE {}][NODE {}] Filter expr error: {}",
                                        rule_id_clone, node_id_clone, err
                                    );
                                    tracing::error!("{}", msg);
                                    halt_graph_rule(&running, &counters, msg);
                                    return;
                                }
                            };
                            while let Some(record) = rx.recv().await {
                                if !is_rule_running(&running) {
                                    break;
                                }
                                if Evaluator::eval_bool(&expr, &record) {
                                    emit_dag_record(
                                        &comms_hub,
                                        &rule_id_clone,
                                        &node_id_clone,
                                        &senders,
                                        &edges,
                                        &backpressure_targets,
                                        &running,
                                        0,
                                        record,
                                    )
                                    .await;
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
                                emit_dag_record(
                                    &comms_hub,
                                    &rule_id_clone,
                                    &node_id_clone,
                                    &senders,
                                    &edges,
                                    &backpressure_targets,
                                    &running,
                                    0,
                                    out_rec,
                                )
                                .await;
                            }
                        }
                        _ => {
                            // Default passthrough operator
                            while let Some(record) = rx.recv().await {
                                if !is_rule_running(&running) {
                                    break;
                                }
                                emit_dag_record(
                                    &comms_hub,
                                    &rule_id_clone,
                                    &node_id_clone,
                                    &senders,
                                    &edges,
                                    &backpressure_targets,
                                    &running,
                                    0,
                                    record,
                                )
                                .await;
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
        let backpressure_targets = backpressure_targets.clone();
        let counters = counters.clone();
        let running = running.clone();

        child_tasks.push(tokio::spawn(async move {
            while let Some(record) = bus_rx.recv().await {
                if !is_rule_running(&running) {
                    break;
                }
                counters.inc_source(1);
                emit_dag_record(
                    &comms_hub,
                    &rule_id_clone,
                    &node_id_clone,
                    &senders,
                    &edges,
                    &backpressure_targets,
                    &running,
                    0,
                    record,
                )
                .await;
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

/// Validates that a DAG graph is eligible for opt-in reliable execution:
/// - Rejects feedback cycles via [`crate::engine::barrier_alignment::validate_acyclic_reliable_graph`]
/// - Rejects unqualified stateful DAG operator node types (`watchdog`, `window`, `join`, `cep`)
///   unless a matching state participant is explicitly registered.
/// - Preserves the default lightweight `run_dag_rule` path untouched.
pub fn validate_reliable_dag_shape(
    graph: &GraphDefinition,
    registered_participants: &[&str],
) -> Result<(), String> {
    validate_graph_acyclic(graph)?;

    for (node_id, node) in &graph.nodes {
        let nt = node.node_type.to_ascii_lowercase();
        let requires_participant = matches!(nt.as_str(), "window" | "join" | "cep");
        if requires_participant
            && !registered_participants
                .iter()
                .any(|p| p.eq_ignore_ascii_case(node_id) || p.to_ascii_lowercase().contains(&nt))
        {
            return Err(format!(
                "unqualified or unregistered stateful DAG node '{}' (type '{}')",
                node_id, node.node_type
            ));
        }
        if nt == "watchdog" {
            return Err(format!(
                "unqualified DAG node '{}' of type 'watchdog' is not supported in reliable mode",
                node_id
            ));
        }
    }
    Ok(())
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
