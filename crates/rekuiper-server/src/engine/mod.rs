pub mod dag;
pub mod dlq;
pub mod joins;
pub mod operators;
pub mod runtime;
pub mod sinks;
pub mod sources;
pub mod windows;

pub use dag::*;
pub(crate) use dlq::*;
pub(crate) use joins::*;
pub use operators::*;
pub(crate) use runtime::*;
pub(crate) use sinks::*;
pub use sources::restore_running_rules;
pub(crate) use sources::*;
pub(crate) use windows::*;

use crate::sink_cache::{self, SinkCache};
use crate::state::{maybe_trace_record, TraceManager};
use parking_lot::RwLock;
use rekuiper_conf::KuiperConfig;
use rekuiper_core::model::{RuleStatus, StreamRecord};
use rekuiper_core::{
    RuleCounters, RuleManager, StreamBus, StreamManager, StreamReceiver, TableManager,
};
use rekuiper_sql::{SelectStmt, WindowDef};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// Extract an atomic StreamBus pre-filter from a SQL WHERE expression.
/// For `A AND B`, even if `B` is a complex function call, extracting `A`
/// still filters out non-matching events at the StreamBus before waking the rule!
pub(crate) fn extract_bus_predicate(
    expr: &rekuiper_sql::Expr,
) -> Option<rekuiper_core::BusPredicate> {
    use rekuiper_core::BusPredicate;
    use rekuiper_sql::{BinaryOperator, Expr};

    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => match (extract_bus_predicate(left), extract_bus_predicate(right)) {
            (Some(l), Some(r)) => Some(BusPredicate::And(vec![l, r])),
            (Some(single), None) | (None, Some(single)) => Some(single),
            (None, None) => None,
        },
        Expr::BinaryOp { left, op, right } => {
            // Check `meta(topic) = '...'` or `mqtt(topic) = '...'`
            if *op == BinaryOperator::Eq {
                if let (Expr::Call { name, args }, Expr::Literal(Value::String(expected))) =
                    (left.as_ref(), right.as_ref())
                {
                    if (name.eq_ignore_ascii_case("meta") || name.eq_ignore_ascii_case("mqtt"))
                        && args.len() == 1
                        && matches!(&args[0], Expr::Identifier(id) if id.eq_ignore_ascii_case("topic"))
                    {
                        return Some(BusPredicate::TopicEq(expected.clone()));
                    }
                }
            }
            let (Expr::Identifier(field), Expr::Literal(lit)) = (left.as_ref(), right.as_ref())
            else {
                return None;
            };
            match op {
                BinaryOperator::Eq => Some(BusPredicate::FieldEq {
                    field: field.clone(),
                    value: lit.clone(),
                }),
                BinaryOperator::Neq => Some(BusPredicate::FieldNeq {
                    field: field.clone(),
                    value: lit.clone(),
                }),
                BinaryOperator::Gt => lit.as_f64().map(|threshold| BusPredicate::FieldGt {
                    field: field.clone(),
                    threshold,
                }),
                BinaryOperator::Gte => lit.as_f64().map(|threshold| BusPredicate::FieldGte {
                    field: field.clone(),
                    threshold,
                }),
                BinaryOperator::Lt => lit.as_f64().map(|threshold| BusPredicate::FieldLt {
                    field: field.clone(),
                    threshold,
                }),
                BinaryOperator::Lte => lit.as_f64().map(|threshold| BusPredicate::FieldLte {
                    field: field.clone(),
                    threshold,
                }),
                _ => None,
            }
        }
        _ => None,
    }
}

pub(crate) fn is_bus_predicate_exhaustive(expr: &rekuiper_sql::Expr) -> bool {
    use rekuiper_sql::{BinaryOperator, Expr};
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => is_bus_predicate_exhaustive(left) && is_bus_predicate_exhaustive(right),
        Expr::BinaryOp { left, op, right } => {
            if *op == BinaryOperator::Eq {
                if let (Expr::Call { name, args }, Expr::Literal(Value::String(_))) =
                    (left.as_ref(), right.as_ref())
                {
                    if (name.eq_ignore_ascii_case("meta") || name.eq_ignore_ascii_case("mqtt"))
                        && args.len() == 1
                        && matches!(&args[0], Expr::Identifier(id) if id.eq_ignore_ascii_case("topic"))
                    {
                        return true;
                    }
                }
            }
            let (Expr::Identifier(_), Expr::Literal(lit)) = (left.as_ref(), right.as_ref()) else {
                return false;
            };
            match op {
                BinaryOperator::Eq | BinaryOperator::Neq => true,
                BinaryOperator::Gt
                | BinaryOperator::Gte
                | BinaryOperator::Lt
                | BinaryOperator::Lte => lit.as_f64().is_some(),
                _ => false,
            }
        }
        _ => false,
    }
}

/// Subscribe to the rule's source stream and spawn its window-aware
/// execution task, registering the join handle on the rule manager.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_rule_task(
    rule_manager: &RuleManager,
    stream_bus: &StreamBus,
    stream_manager: &StreamManager,
    table_manager: &TableManager,
    source_configs: &Arc<RwLock<HashMap<String, Value>>>,
    http_client: &reqwest::Client,
    trace_manager: &TraceManager,
    config: &Arc<RwLock<KuiperConfig>>,
    rule_id: String,
    select_stmt: SelectStmt,
    actions: Vec<HashMap<String, Value>>,
    rule_options: Option<HashMap<String, Value>>,
) {
    if rule_options
        .as_ref()
        .and_then(|o| o.get("enableRuleTracer"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        trace_manager.start_trace(&rule_id, "always".to_string());
    }
    let buffer_len = rule_options
        .as_ref()
        .and_then(|o| o.get("bufferLength"))
        .and_then(|v| v.as_u64())
        .and_then(|n| usize::try_from(n).ok())
        .map(|n| n.max(1))
        .unwrap_or(32_768);
    let from_topic = resolve_source_topic(stream_manager, &select_stmt.from);
    let from_stream_def = stream_manager.get_stream(&from_topic);
    let from_fields = from_stream_def.map(|s| s.stream_fields).unwrap_or_default();

    // Hot-path handles cloned once: rule loops bump lock-free atomics and
    // read the running flag without touching the global rules map per event.
    let loop_counters = rule_manager
        .rule_counters(&rule_id)
        .unwrap_or_else(|| Arc::new(RuleCounters::default()));
    let loop_running = rule_manager
        .rule_status_handle(&rule_id)
        .unwrap_or_else(|| Arc::new(RwLock::new(RuleStatus::default())));

    let is_event_time = rule_options
        .as_ref()
        .and_then(|o| o.get("isEventTime"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let send_error = rule_options
        .as_ref()
        .and_then(|o| o.get("sendError"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let enable_private_net = config.read().basic.enable_private_net;
    let dlq_router = DlqRouter::build(
        &rule_id,
        rule_options.as_ref(),
        loop_counters.clone(),
        rule_manager.clone(),
        stream_bus.clone(),
        http_client.clone(),
        enable_private_net,
        source_configs,
    );

    let (bus_pred, bus_pred_exhaustive) =
        if !rekuiper_sql::Evaluator::stmt_has_analytic(&select_stmt)
            && !is_event_time
            && !send_error
        {
            match select_stmt.where_clause.as_ref() {
                Some(w) => {
                    let pred = extract_bus_predicate(w);
                    let exhaustive = pred.is_some() && is_bus_predicate_exhaustive(w);
                    (pred, exhaustive)
                }
                None => (None, false),
            }
        } else {
            (None, false)
        };

    let rx = match bus_pred {
        Some(pred) => {
            let filter = rekuiper_core::SubscriberFilter {
                predicate: pred,
                source_in_counter: loop_counters.source_in.clone(),
                filtered_counter: loop_counters.filtered.clone(),
            };
            stream_bus.subscribe_filtered_with_capacity(&from_topic, filter, buffer_len)
        }
        None => stream_bus.subscribe_with_capacity(&from_topic, buffer_len),
    };
    let rx = if !from_fields.is_empty() {
        let (tx, coerced_rx) = tokio::sync::mpsc::channel(buffer_len);
        let fields = from_fields;
        let schema_dlq = dlq_router.clone();
        tokio::spawn(async move {
            let mut rx = rx;
            while let Some(mut record) = rx.recv().await {
                if schema_dlq.on_error == OnErrorPolicy::Dlq
                    || schema_dlq.on_error == OnErrorPolicy::Halt
                {
                    match rekuiper_core::model::enforce_stream_schema_checked(
                        &mut record.data,
                        &fields,
                    ) {
                        Ok(()) => {
                            if tx.send(record).await.is_err() {
                                break;
                            }
                        }
                        Err(err) => {
                            let payload_str = serde_json::to_string(&record.data).ok();
                            schema_dlq.route_error(
                                rekuiper_core::model::DlqStage::SchemaValidation,
                                err.to_string(),
                                payload_str.as_deref(),
                                Some(serde_json::to_value(&record.data).unwrap_or(Value::Null)),
                                None,
                            );
                            if schema_dlq.on_error == OnErrorPolicy::Halt {
                                break;
                            }
                        }
                    }
                } else {
                    rekuiper_core::model::enforce_stream_schema(&mut record.data, &fields);
                    if tx.send(record).await.is_err() {
                        break;
                    }
                }
            }
        });
        coerced_rx
    } else {
        rx
    };

    let window = select_stmt.window.clone();
    let tables = table_manager.clone();
    let confs = source_configs.clone();
    // Stream-stream joins fan in every joined stream: subscribe each join
    // target that is a stream (not a table) so windowed rules see both
    // sides. Table targets resolve per-row through lookups instead.
    let mut join_rxs: Vec<(String, StreamReceiver)> = Vec::new();
    for join in &select_stmt.joins {
        if table_manager.get_table(&join.target).is_none()
            && join.target != select_stmt.from
            && !join_rxs.iter().any(|(s, _)| s == &join.target)
        {
            let topic = resolve_source_topic(stream_manager, &join.target);
            let jrx = stream_bus.subscribe_with_capacity(&topic, buffer_len);
            let join_fields = stream_manager
                .get_stream(&topic)
                .map(|s| s.stream_fields)
                .unwrap_or_default();
            let jrx = if !join_fields.is_empty() {
                let (tx, coerced_jrx) = tokio::sync::mpsc::channel(buffer_len);
                let join_dlq = dlq_router.clone();
                tokio::spawn(async move {
                    let mut rx = jrx;
                    while let Some(mut record) = rx.recv().await {
                        if join_dlq.on_error == OnErrorPolicy::Dlq
                            || join_dlq.on_error == OnErrorPolicy::Halt
                        {
                            match rekuiper_core::model::enforce_stream_schema_checked(
                                &mut record.data,
                                &join_fields,
                            ) {
                                Ok(()) => {
                                    if tx.send(record).await.is_err() {
                                        break;
                                    }
                                }
                                Err(err) => {
                                    let payload_str = serde_json::to_string(&record.data).ok();
                                    join_dlq.route_error(
                                        rekuiper_core::model::DlqStage::SchemaValidation,
                                        err.to_string(),
                                        payload_str.as_deref(),
                                        Some(
                                            serde_json::to_value(&record.data)
                                                .unwrap_or(Value::Null),
                                        ),
                                        None,
                                    );
                                    if join_dlq.on_error == OnErrorPolicy::Halt {
                                        break;
                                    }
                                }
                            }
                        } else {
                            rekuiper_core::model::enforce_stream_schema(
                                &mut record.data,
                                &join_fields,
                            );
                            if tx.send(record).await.is_err() {
                                break;
                            }
                        }
                    }
                });
                coerced_jrx
            } else {
                jrx
            };
            join_rxs.push((join.target.clone(), jrx));
        }
    }

    // Bounded decoupled sink queue: the streaming evaluation loop never blocks
    // on sink network/disk I/O. Dropping `sink_tx` (rule end/cancel) lets the
    // worker flush remaining outputs and exit cleanly.
    let (sink_tx, mut sink_rx) = tokio::sync::mpsc::channel::<StreamRecord>(buffer_len);
    let prepared = prepare_actions(&actions, &rule_id, source_configs, rule_options.as_ref());
    let cache_configs = action_cache_configs(&actions);
    let sink_rule_id = rule_id.clone();
    let sink_rule_mgr = rule_manager.clone();
    let sink_stream_bus = stream_bus.clone();
    let sink_http_client = http_client.clone();
    let sink_trace_mgr = trace_manager.clone();
    let sink_config = config.clone();
    let sink_dlq_router = dlq_router.clone();
    let sink_counters = rule_manager
        .rule_counters(&rule_id)
        .unwrap_or_else(|| Arc::new(RuleCounters::default()));
    let all_nop = prepared
        .iter()
        .zip(&cache_configs)
        .all(|(action, cache_cfg)| matches!(action, PreparedAction::Nop) && cache_cfg.is_none());
    tokio::spawn(async move {
        use std::collections::HashMap as Map;
        use std::sync::atomic::Ordering::Relaxed;
        let mut files: Map<std::path::PathBuf, FileBatchWriter> = Map::new();
        // Per-action runtimes: persistent connections plus the offline cache
        // (pages left by a previous run are adopted and resent first).
        let cache_root = std::path::Path::new(SINK_CACHE_ROOT);
        let mut runtimes: Vec<ActionRuntime> = prepared
            .into_iter()
            .zip(cache_configs)
            .enumerate()
            .map(|(idx, (action, cache_cfg))| {
                let cache = cache_cfg.map(|cfg| {
                    SinkCache::open(cfg, sink_cache::cache_dir(cache_root, &sink_rule_id, idx))
                });
                ActionRuntime::new(action, cache)
            })
            .collect();
        let mut ticker = tokio::time::interval(SINK_TICK);
        // First tick fires immediately; consume it so time-flushes align.
        ticker.tick().await;
        let file_actions = runtimes
            .iter()
            .any(|rt| matches!(&rt.action, PreparedAction::File { .. }));
        let mut pending_file_records: u64 = 0;
        let mut last_live = tokio::time::Instant::now();
        loop {
            tokio::select! {
                rec = sink_rx.recv() => {
                    let Some(output_record) = rec else {
                        // Flush remaining batch_buffer for sinks with batching
                        for rt in runtimes.iter_mut() {
                            if !rt.batch_buffer.is_empty() {
                                let batch = std::mem::take(&mut rt.batch_buffer);
                                let mut batch_data = HashMap::new();
                                batch_data.insert(BATCH_ROWS_KEY.to_string(), Value::Array(batch));
                                let batch_record = StreamRecord::new(batch_data);
                                let enable_private_net = sink_config.read().basic.enable_private_net;
                                let ctx = SinkContext {
                                    rule_id: &sink_rule_id,
                                    rule_mgr: &sink_rule_mgr,
                                    stream_bus: &sink_stream_bus,
                                    http_client: &sink_http_client,
                                    enable_private_net,
                                    dlq_router: Some(&sink_dlq_router),
                                };
                                let _ = send_action(rt, &batch_record, None, &ctx, &mut files).await;
                            }
                        }
                        // Rule end/cancel/update/delete/shutdown: drain, flush
                        // every file destination, persist caches, then exit.
                        // Errors propagate to exceptions before exit.
                        let mut all_flushed = true;
                        for writer in files.values_mut() {
                            if let Err(e) = writer.flush().await {
                                tracing::warn!(
                                    "[RULE {}] file flush on shutdown failed: {}",
                                    sink_rule_id, e
                                );
                                sink_rule_mgr.inc_exceptions(&sink_rule_id, 1);
                                all_flushed = false;
                            }
                        }
                        if all_flushed {
                            sink_counters.sink_out.fetch_add(pending_file_records, Relaxed);
                        } else {
                            sink_rule_mgr.inc_sink_failed(&sink_rule_id, pending_file_records);
                        }
                        for rt in std::mem::take(&mut runtimes) {
                            if let Some(cache) = rt.cache {
                                if !cache.is_empty() && !cache.config().clean_at_stop {
                                    tracing::info!(
                                        "[RULE {}] persisting {} cached sink records for resend",
                                        sink_rule_id,
                                        cache.len()
                                    );
                                }
                                cache.close();
                            }
                        }
                        break;
                    };
                    last_live = tokio::time::Instant::now();
                    let enable_private_net = sink_config.read().basic.enable_private_net;
                    let tracing_active = sink_trace_mgr.is_tracing(&sink_rule_id);
                    let ctx = SinkContext {
                        rule_id: &sink_rule_id,
                        rule_mgr: &sink_rule_mgr,
                        stream_bus: &sink_stream_bus,
                        http_client: &sink_http_client,
                        enable_private_net,
                        dlq_router: Some(&sink_dlq_router),
                    };

                    let mut cur_output = output_record;
                    let mut drained = 0;
                    let mut batch_sink_out = 0u64;
                    loop {
                        if tracing_active {
                            maybe_trace_record(
                                &sink_trace_mgr,
                                &sink_rule_id,
                                &cur_output.data,
                                Some(&cur_output.data),
                            );
                        }
                        if all_nop {
                            batch_sink_out += 1;
                        } else {
                            let num_records = if let Some(Value::Array(a)) =
                                cur_output.data.get(BATCH_ROWS_KEY)
                            {
                                a.len() as u64
                            } else {
                                1
                            };
                            match deliver_record(&mut runtimes, &cur_output, &ctx, &mut files).await {
                                Delivery::Delivered => {
                                    if file_actions {
                                        pending_file_records += num_records;
                                    } else {
                                        batch_sink_out += num_records;
                                    }
                                }
                                Delivery::Cached => {}
                                Delivery::Failed => {
                                    sink_rule_mgr.inc_sink_failed(&sink_rule_id, num_records)
                                }
                            }
                        }
                        drained += 1;
                        if drained >= 64 {
                            break;
                        }
                        match sink_rx.try_recv() {
                            Ok(next) => cur_output = next,
                            Err(_) => break,
                        }
                    }
                    if batch_sink_out > 0 {
                        sink_counters.sink_out.fetch_add(batch_sink_out, Relaxed);
                    }
                    let need_flush = files.values().any(|w| w.should_flush());
                    if need_flush {
                        let mut all_flushed = true;
                        for writer in files.values_mut() {
                            if let Err(e) = writer.flush().await {
                                tracing::warn!(
                                    "[RULE {}] file batch flush failed: {}",
                                    sink_rule_id, e
                                );
                                sink_rule_mgr.inc_exceptions(&sink_rule_id, 1);
                                all_flushed = false;
                            }
                        }
                        if all_flushed {
                            sink_counters.sink_out.fetch_add(pending_file_records, Relaxed);
                            pending_file_records = 0;
                        }
                    }
                }
                _ = ticker.tick() => {
                    let mut any_error = false;
                    for writer in files.values_mut() {
                        if writer.pending > 0 {
                            if let Err(e) = writer.flush().await {
                                tracing::warn!(
                                    "[RULE {}] file timed flush failed: {}",
                                    sink_rule_id, e
                                );
                                any_error = true;
                            }
                        }
                    }
                    if any_error {
                        sink_rule_mgr.inc_exceptions(&sink_rule_id, 1);
                    } else {
                        sink_counters.sink_out.fetch_add(pending_file_records, Relaxed);
                        pending_file_records = 0;
                    }
                    let enable_private_net = sink_config.read().basic.enable_private_net;
                    let ctx = SinkContext {
                        rule_id: &sink_rule_id,
                        rule_mgr: &sink_rule_mgr,
                        stream_bus: &sink_stream_bus,
                        http_client: &sink_http_client,
                        enable_private_net,
                        dlq_router: Some(&sink_dlq_router),
                    };
                    let now = tokio::time::Instant::now();
                    for rt in runtimes.iter_mut() {
                        let linger_interval = rt.action.common_opts().map(|o| o.linger_interval).unwrap_or(0);
                        if linger_interval > 0 && !rt.batch_buffer.is_empty() {
                            let elapsed = now.duration_since(rt.last_batch_time).as_millis() as u64;
                            if elapsed >= linger_interval {
                                let batch = std::mem::take(&mut rt.batch_buffer);
                                rt.last_batch_time = now;
                                let mut batch_data = HashMap::new();
                                batch_data.insert(BATCH_ROWS_KEY.to_string(), Value::Array(batch));
                                let batch_record = StreamRecord::new(batch_data);
                                if let Err(err) = send_action(rt, &batch_record, None, &ctx, &mut files).await {
                                    report_send_error(rt, &ctx, &err, false);
                                }
                            }
                        }
                    }
                    let live_recent = last_live.elapsed() < SINK_TICK * 2;
                    for rt in runtimes.iter_mut() {
                        let resent = resend_cached(rt, live_recent, &ctx, &mut files).await;
                        if resent > 0 {
                            if file_actions {
                                pending_file_records += resent;
                            } else {
                                sink_counters.sink_out.fetch_add(resent, Relaxed);
                            }
                        }
                    }
                }
            }
        }
    });

    // Event-time mode for windowed rules: boundaries derive from payload
    // timestamps (stream TIMESTAMP field or well-known keys) instead of the
    // wall clock, with a late-tolerance grace window for out-of-order rows.
    let mut source_ts_fields = HashMap::new();
    let from_topic = resolve_source_topic(stream_manager, &select_stmt.from);
    if let Some(s) = stream_manager.get_stream(&from_topic) {
        if let Some((_, v)) = s
            .options
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("TIMESTAMP"))
        {
            source_ts_fields.insert(select_stmt.from.clone(), v.clone());
        }
    }
    for join in &select_stmt.joins {
        let topic = resolve_source_topic(stream_manager, &join.target);
        if let Some(s) = stream_manager.get_stream(&topic) {
            if let Some((_, v)) = s
                .options
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("TIMESTAMP"))
            {
                source_ts_fields.insert(join.target.clone(), v.clone());
            }
        }
    }
    let event_time = EventTimeConfig {
        enabled: rule_options
            .as_ref()
            .and_then(|o| o.get("isEventTime"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        late_tolerance_ms: rule_options
            .as_ref()
            .and_then(|o| o.get("lateTolerance"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0),
        timestamp_field: source_ts_fields.get(&select_stmt.from).cloned(),
        source_timestamp_fields: source_ts_fields,
    };
    let checkpoint_interval_ms = rule_options
        .as_ref()
        .and_then(|o| o.get("checkpointInterval"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    let handle = match window {
        None => tokio::spawn(run_stateless_rule(
            loop_counters,
            loop_running,
            rule_id.clone(),
            select_stmt,
            rx,
            tables,
            confs,
            sink_tx,
            send_error,
            join_rxs,
            event_time,
            rule_manager.clone(),
            checkpoint_interval_ms,
            dlq_router.clone(),
            bus_pred_exhaustive,
        )),
        Some(WindowDef::Count { size, interval }) => tokio::spawn(run_count_window_rule(
            loop_counters,
            loop_running,
            rule_id.clone(),
            select_stmt,
            rx,
            size,
            interval,
            sink_tx,
            send_error,
            join_rxs,
            tables.clone(),
            confs.clone(),
            dlq_router.clone(),
        )),
        Some(WindowDef::TumblingTime { unit, length }) => {
            if length == 0 {
                tokio::spawn(async move {
                    let mut rx = rx;
                    while rx.recv().await.is_some() {
                        if !is_rule_running(&loop_running) {
                            continue;
                        }
                        loop_counters.inc_source(1);
                    }
                })
            } else {
                let duration = tumbling_window_duration(&unit, length);
                tokio::spawn(run_tumbling_window_rule(
                    loop_counters,
                    loop_running,
                    rule_id.clone(),
                    select_stmt,
                    rx,
                    duration,
                    sink_tx,
                    event_time,
                    send_error,
                    join_rxs,
                    tables.clone(),
                    confs.clone(),
                    dlq_router.clone(),
                ))
            }
        }
        Some(WindowDef::HoppingTime {
            unit,
            length,
            interval,
        }) => {
            let window_length = tumbling_window_duration(&unit, length);
            let hop_interval = tumbling_window_duration(&unit, interval);
            tokio::spawn(run_hopping_window_rule(
                loop_counters,
                loop_running,
                rule_id.clone(),
                select_stmt,
                rx,
                window_length,
                hop_interval,
                sink_tx,
                event_time,
                send_error,
                join_rxs,
                tables.clone(),
                confs.clone(),
                dlq_router.clone(),
            ))
        }
        Some(WindowDef::SlidingTime {
            unit,
            length,
            delay,
        }) => {
            let window_length = tumbling_window_duration(&unit, length);
            let delay_dur = delay.map(|d| tumbling_window_duration(&unit, d));
            tokio::spawn(run_sliding_window_rule(
                loop_counters,
                loop_running,
                rule_id.clone(),
                select_stmt,
                rx,
                window_length,
                delay_dur,
                sink_tx,
                event_time,
                send_error,
                join_rxs,
                tables.clone(),
                confs.clone(),
                dlq_router.clone(),
            ))
        }
        Some(WindowDef::Session {
            unit,
            max_duration,
            timeout,
        }) => tokio::spawn(run_session_window_rule(
            loop_counters,
            loop_running,
            rule_id.clone(),
            select_stmt,
            rx,
            tumbling_window_duration(&unit, max_duration),
            tumbling_window_duration(&unit, timeout),
            sink_tx,
            event_time,
            send_error,
            join_rxs,
            tables.clone(),
            confs.clone(),
            dlq_router.clone(),
        )),
        Some(WindowDef::State {
            start_condition,
            end_condition,
        }) => tokio::spawn(run_state_window_rule(
            loop_counters,
            loop_running,
            rule_id.clone(),
            select_stmt,
            rx,
            start_condition,
            end_condition,
            sink_tx,
            event_time,
            send_error,
            join_rxs,
            tables.clone(),
            confs.clone(),
            dlq_router.clone(),
        )),
    };
    rule_manager.set_rule_handle(&rule_id, handle);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{
        der_read_tlv, load_config_maps, parse_pem_block, persist_config_entry, spki_to_pkcs1,
        unpersist_config_entry, verify_jwt_raw, AppState,
    };
    use rekuiper_connectors::PayloadFormat;
    use rekuiper_core::{KvStore, SchemaManager, StreamDefinition, TableDefinition};
    use rekuiper_sql::Parser;
    use serde_json::json;

    async fn test_stream_manager(options: &[(&str, &str)]) -> StreamManager {
        let manager = StreamManager::new();
        let opts = options
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<HashMap<_, _>>();
        manager
            .create_stream(StreamDefinition {
                name: "demo".to_string(),
                sql: String::new(),
                stream_fields: Vec::new(),
                options: opts,
            })
            .await
            .unwrap();
        manager
    }

    fn test_source_configs(pairs: &[(&str, Value)]) -> Arc<RwLock<HashMap<String, Value>>> {
        Arc::new(RwLock::new(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        ))
    }

    #[tokio::test]
    async fn resolve_mqtt_source_honors_confkey_without_topic() {
        // D1: a topic-less stored config must still contribute its broker URL.
        let manager = test_stream_manager(&[("CONF_KEY", "remotekey")]).await;
        let configs =
            test_source_configs(&[("mqtt/remotekey", json!({"server": "tcp://broker:1883"}))]);
        let cfg =
            resolve_mqtt_source(&manager, &configs, &SchemaManager::new(), "demo", "r1").unwrap();
        assert_eq!(cfg.server, "tcp://broker:1883");
        assert_eq!(cfg.topic, "demo");
    }

    #[tokio::test]
    async fn resolve_mqtt_source_is_case_insensitive() {
        // Lowercase SQL/JSON option spellings resolve like the uppercase ones.
        let manager =
            test_stream_manager(&[("conf_key", "remotekey"), ("datasource", "sensors/#")]).await;
        let configs =
            test_source_configs(&[("mqtt/remotekey", json!({"server": "tcp://broker:1883"}))]);
        let cfg =
            resolve_mqtt_source(&manager, &configs, &SchemaManager::new(), "demo", "r1").unwrap();
        assert_eq!(cfg.server, "tcp://broker:1883");
        assert_eq!(cfg.topic, "sensors/#");
    }

    #[tokio::test]
    async fn resolve_mqtt_source_server_option_overrides_confkey() {
        let manager =
            test_stream_manager(&[("CONF_KEY", "remotekey"), ("server", "tcp://override:1883")])
                .await;
        let configs =
            test_source_configs(&[("mqtt/remotekey", json!({"server": "tcp://broker:1883"}))]);
        let cfg =
            resolve_mqtt_source(&manager, &configs, &SchemaManager::new(), "demo", "r1").unwrap();
        assert_eq!(cfg.server, "tcp://override:1883");
    }

    #[tokio::test]
    async fn resolve_mqtt_source_falls_back_to_loopback() {
        let manager = test_stream_manager(&[]).await;
        let configs = test_source_configs(&[]);
        let cfg =
            resolve_mqtt_source(&manager, &configs, &SchemaManager::new(), "demo", "r1").unwrap();
        assert_eq!(cfg.server, "tcp://127.0.0.1:1883");
        assert_eq!(cfg.topic, "demo");
    }

    #[tokio::test]
    async fn resolve_sql_source_from_confkey() {
        // D8: a stored `sql/{key}` config provides url/table/interval.
        let manager = test_stream_manager(&[("TYPE", "sql"), ("CONF_KEY", "pg")]).await;
        let configs = test_source_configs(&[(
            "sql/pg",
            json!({"url": "postgres://db:5432/k", "table": "readings", "interval": 500}),
        )]);
        let cfg = resolve_sql_source(&manager, &configs, "demo", "r1").unwrap();
        assert_eq!(cfg.url, "postgres://db:5432/k");
        assert_eq!(cfg.table, "readings");
        assert_eq!(cfg.interval, 500);
    }

    #[tokio::test]
    async fn resolve_sql_source_from_stream_options() {
        // Lowercase option spellings resolve like the uppercase ones.
        // Documented plugin shape: dburl + templateSqlQueryCfg, table
        // anchored by the stream DATASOURCE.
        let manager = test_stream_manager(&[
            ("TYPE", "sql"),
            ("DATASOURCE", "rksrc"),
            ("CONF_KEY", "postgresql_config"),
        ])
        .await;
        let configs = test_source_configs(&[(
            "sql/postgresql_config",
            json!({
                "dburl": "postgres://u:p@h/db?sslmode=disable",
                "interval": 5000,
                "templateSqlQueryCfg": {"templateSql": "SELECT id, val FROM rksrc"},
            }),
        )]);
        let cfg = resolve_sql_source(&manager, &configs, "demo", "r1").unwrap();
        assert_eq!(cfg.url, "postgres://u:p@h/db?sslmode=disable");
        assert_eq!(cfg.table, "rksrc");
        assert_eq!(cfg.interval, 5000);
        assert_eq!(
            rekuiper_connectors::sql_source_query(&cfg),
            "SELECT id, val FROM rksrc"
        );
        let manager = test_stream_manager(&[
            ("type", "SQL"),
            ("datasource", "sqlite://x.db"),
            ("table", "sens"),
        ])
        .await;
        let cfg = resolve_sql_source(&manager, &test_source_configs(&[]), "demo", "r1").unwrap();
        assert_eq!(cfg.url, "sqlite://x.db");
        assert_eq!(cfg.table, "sens");
        assert_eq!(cfg.interval, 1000);
    }

    #[tokio::test]
    async fn resolve_sql_source_rejects_non_sql_types() {
        let manager = test_stream_manager(&[("TYPE", "mqtt")]).await;
        assert!(resolve_sql_source(&manager, &test_source_configs(&[]), "demo", "r1").is_none());
        let manager = test_stream_manager(&[]).await;
        assert!(resolve_sql_source(&manager, &test_source_configs(&[]), "demo", "r1").is_none());
    }

    #[tokio::test]
    async fn resolve_rabbitmq_source_honors_confkey_and_inline_overrides() {
        let manager = test_stream_manager(&[
            ("TYPE", "rabbitmq"),
            ("CONF_KEY", "rmq_cluster"),
            ("QUEUE", "override_queue"),
        ])
        .await;
        let configs = test_source_configs(&[(
            "rabbitmq/rmq_cluster",
            json!({
                "server": "amqp://user:pass@rmq:5672",
                "queue": "base_queue",
                "routingKey": "orders.#"
            }),
        )]);
        let cfg = resolve_rabbitmq_source(&manager, &configs, "demo").unwrap();
        assert_eq!(cfg.server, "amqp://user:pass@rmq:5672");
        assert_eq!(cfg.queue, "override_queue"); // Overridden inline
        assert_eq!(cfg.routing_key, "orders.#");
    }

    #[tokio::test]
    async fn resolve_zmq_source_honors_confkey_and_inline_overrides() {
        let manager = test_stream_manager(&[
            ("TYPE", "zmq"),
            ("CONF_KEY", "zmq_pubsub"),
            ("TOPIC", "override_topic"),
        ])
        .await;
        let configs = test_source_configs(&[(
            "zmq/zmq_pubsub",
            json!({
                "server": "tcp://zmq-broker:5555",
                "topic": "base_topic",
                "mode": "connect"
            }),
        )]);
        let cfg = resolve_zmq_source(&manager, &configs, "demo").unwrap();
        assert_eq!(cfg.server, "tcp://zmq-broker:5555");
        assert_eq!(cfg.topic.as_deref(), Some("override_topic")); // Overridden inline
        assert_eq!(cfg.mode, "connect");
    }

    #[tokio::test]
    async fn resolve_video_source_honors_confkey_and_inline_overrides() {
        let manager = test_stream_manager(&[
            ("TYPE", "video"),
            ("CONF_KEY", "camera_feed"),
            ("INTERVAL", "250ms"),
        ])
        .await;
        let configs = test_source_configs(&[(
            "video/camera_feed",
            json!({
                "url": "rtsp://camera:554/live",
                "interval": "1s",
                "codec": "mjpeg"
            }),
        )]);
        let cfg = resolve_video_source(&manager, &configs, "demo").unwrap();
        assert_eq!(cfg.url, "rtsp://camera:554/live");
        assert_eq!(cfg.interval, 250); // Overridden inline from 1000ms
        assert_eq!(cfg.codec, "mjpeg");
    }

    #[tokio::test]
    async fn resolve_simulator_source_honors_confkey_and_inline_overrides() {
        let manager = test_stream_manager(&[
            ("TYPE", "simulator"),
            ("CONF_KEY", "sim1"),
            ("INTERVAL", "50ms"),
            ("LOOP", "false"),
            ("DATA", r#"[{"sensor": "temp", "val": 99}]"#),
        ])
        .await;
        let configs = test_source_configs(&[(
            "simulator/sim1",
            json!({
                "data": [{"sensor": "temp", "val": 10}],
                "interval": "1s",
                "loop": true
            }),
        )]);
        let cfg = resolve_simulator_source(&manager, &configs, "demo", "r1").unwrap();
        assert_eq!(cfg.interval, json!("50ms")); // Overridden from 1s to 50ms
        assert_eq!(
            rekuiper_connectors::simulator::parse_interval_ms(&cfg.interval).as_millis(),
            50
        );
        assert!(!cfg.loop_data); // Overridden from true to false
        assert_eq!(cfg.data.len(), 1);
        assert_eq!(cfg.data[0].get("val"), Some(&json!(99))); // Overridden data

        // Check random alias works as well
        let manager_random = test_stream_manager(&[("TYPE", "random")]).await;
        let cfg_rnd =
            resolve_simulator_source(&manager_random, &test_source_configs(&[]), "demo", "r1")
                .unwrap();
        assert!(cfg_rnd.data.is_empty());
        assert!(cfg_rnd.loop_data);

        // Check rejection of non-simulator types
        let manager_mqtt = test_stream_manager(&[("TYPE", "mqtt")]).await;
        assert!(
            resolve_simulator_source(&manager_mqtt, &test_source_configs(&[]), "demo", "r1")
                .is_none()
        );
    }

    fn tagged(source: &str, pairs: &[(&str, Value)]) -> TaggedRow {
        TaggedRow {
            source: source.to_string(),
            data: pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        }
    }

    fn parse_stmt(sql: &str) -> SelectStmt {
        Parser::new(sql).parse_select().expect("join SQL parses")
    }

    #[tokio::test]
    async fn window_join_right_preserves_unmatched_without_left() {
        // RIGHT JOIN with an empty left side still emits right rows.
        let stmt = parse_stmt(
            "SELECT l.id AS id, r.val AS v FROM l RIGHT JOIN r ON l.id = r.id GROUP BY CountWindow(2)",
        );
        let tables = TableManager::new();
        let confs = test_source_configs(&[]);
        let batch = vec![tagged("r", &[("id", json!(1)), ("val", json!(9))])];
        let out = eval_window_join_batch(&tables, &confs, &stmt, &batch, None).await;
        assert_eq!(out.len(), 1);
        // Missing left side projects to Null; the preserved right side is intact.
        assert_eq!(out[0].get("id"), Some(&serde_json::Value::Null));
        assert_eq!(out[0].get("v"), Some(&json!(9)));
    }

    #[tokio::test]
    async fn window_join_inner_empty_left_yields_nothing() {
        let stmt = parse_stmt(
            "SELECT l.id AS id FROM l INNER JOIN r ON l.id = r.id GROUP BY CountWindow(2)",
        );
        let tables = TableManager::new();
        let confs = test_source_configs(&[]);
        let batch = vec![tagged("r", &[("id", json!(1))])];
        let out = eval_window_join_batch(&tables, &confs, &stmt, &batch, None).await;
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn window_join_cross_fanout_is_bounded() {
        // 201 x 201 pairs would fan out to 40401 rows; the cap holds.
        let stmt = parse_stmt("SELECT l.id AS id FROM l CROSS JOIN r GROUP BY CountWindow(50000)");
        let tables = TableManager::new();
        let confs = test_source_configs(&[]);
        let mut batch = Vec::new();
        for i in 0..201 {
            batch.push(tagged("l", &[("id", json!(i))]));
            batch.push(tagged("r", &[("id", json!(i))]));
        }
        let out = eval_window_join_batch(&tables, &confs, &stmt, &batch, None).await;
        assert_eq!(out.len(), MAX_JOIN_FANOUT);
    }

    #[tokio::test]
    async fn window_join_table_takes_first_on_match() {
        // Lookup tables resolve one row per key: the first ON-matching row
        // wins (point-lookup semantics, also used by the stateless path).
        let tables = TableManager::new();
        tables
            .create_table(TableDefinition {
                name: "t".to_string(),
                sql: String::new(),
                stream_fields: Vec::new(),
                options: HashMap::new(),
            })
            .await
            .unwrap();
        tables.insert_table_row(
            "t",
            [("id".to_string(), json!(1)), ("v".to_string(), json!("a"))]
                .into_iter()
                .collect(),
        );
        tables.insert_table_row(
            "t",
            [("id".to_string(), json!(1)), ("v".to_string(), json!("b"))]
                .into_iter()
                .collect(),
        );
        let stmt = parse_stmt(
            "SELECT s.id AS id, t.v AS v FROM s INNER JOIN t ON s.id = t.id GROUP BY CountWindow(2)",
        );
        let confs = test_source_configs(&[]);
        let batch = vec![tagged("s", &[("id", json!(1))])];
        let out = eval_window_join_batch(&tables, &confs, &stmt, &batch, None).await;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].get("v"), Some(&json!("a")));
    }

    #[tokio::test]
    async fn bootstrap_join_registers_all_source_cancels() {
        // Stream-stream joins bootstrap one producer per side. Every
        // producer's cancel handle must be retained: overwriting the entry
        // drops the first sender, and a dropped watch sender reads as
        // `Err` in the source task, which exits immediately (join side A
        // would silently stop delivering).
        let bus = StreamBus::new();
        let sm = StreamManager::new();
        for name in ["ja", "jb"] {
            sm.create_stream(StreamDefinition {
                name: name.to_string(),
                sql: String::new(),
                stream_fields: Vec::new(),
                options: HashMap::new(),
            })
            .await
            .unwrap();
        }
        let state = AppState::new(
            "test".to_string(),
            rekuiper_conf::KuiperConfig::default(),
            sm,
            TableManager::new(),
            RuleManager::new(bus.clone()),
            bus,
        );
        let mut parser = Parser::new(
            "SELECT ja.id AS id, jb.val AS v FROM ja INNER JOIN jb ON ja.id = jb.id GROUP BY CountWindow(2)",
        );
        let stmt = parser.parse_select().unwrap();
        bootstrap_rule_sources(&state, "rj", &stmt);
        let guards = state.source_cancels.read();
        let txs = guards.get("rj").expect("join rule has cancel handles");
        assert_eq!(txs.len(), 2, "one cancel sender per joined stream");
        drop(guards);
        cancel_rule_source(&state, "rj");
        assert!(!state.source_cancels.read().contains_key("rj"));
    }

    #[tokio::test]
    async fn config_maps_persist_and_reload_across_restart() {
        // D-RESTART: CONF_KEYs (MQTT brokers, SQL URLs) must survive a
        // daemon restart instead of falling back to loopback defaults.
        use rekuiper_core::MemKvStore;
        let kv: Arc<dyn KvStore> = Arc::new(MemKvStore::new());
        let state = AppState {
            kv: Some(kv.clone()),
            ..AppState::new(
                "test".to_string(),
                rekuiper_conf::KuiperConfig::default(),
                StreamManager::new(),
                TableManager::new(),
                RuleManager::new(StreamBus::new()),
                StreamBus::new(),
            )
        };
        persist_config_entry(
            &state,
            "source_configs",
            "mqtt/evalmqtt",
            &json!({"server": "tcp://broker:1883"}),
        )
        .await
        .unwrap();
        persist_config_entry(
            &state,
            "source_configs",
            "sql/postgresql_config",
            &json!({"dburl": "postgres://u:p@h/db", "interval": 5000}),
        )
        .await
        .unwrap();
        // A fresh daemon with empty maps reloads everything from KV.
        let fresh = AppState {
            kv: Some(kv),
            ..AppState::new(
                "test".to_string(),
                rekuiper_conf::KuiperConfig::default(),
                StreamManager::new(),
                TableManager::new(),
                RuleManager::new(StreamBus::new()),
                StreamBus::new(),
            )
        };
        load_config_maps(&fresh).await.unwrap();
        assert_eq!(
            fresh
                .source_configs
                .read()
                .get("mqtt/evalmqtt")
                .and_then(|v| v.get("server"))
                .and_then(|v| v.as_str()),
            Some("tcp://broker:1883")
        );
        assert!(fresh
            .source_configs
            .read()
            .contains_key("sql/postgresql_config"));
        // Deletes propagate too.
        unpersist_config_entry(&state, "source_configs", "mqtt/evalmqtt")
            .await
            .unwrap();
        let fresh2 = AppState {
            kv: Some(state.kv.clone().unwrap()),
            ..AppState::new(
                "test".to_string(),
                rekuiper_conf::KuiperConfig::default(),
                StreamManager::new(),
                TableManager::new(),
                RuleManager::new(StreamBus::new()),
                StreamBus::new(),
            )
        };
        load_config_maps(&fresh2).await.unwrap();
        assert!(!fresh2.source_configs.read().contains_key("mqtt/evalmqtt"));
        assert!(fresh2
            .source_configs
            .read()
            .contains_key("sql/postgresql_config"));
    }

    // D9: RS256 JWT vectors generated offline (2048-bit key, 1-year valid
    // token, 1-hour-expired token, keyless-claims token, wrong-key token).
    const TEST_RSA_PUBLIC_PEM: &str = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAmdub6pqDN/MPofsOTQlf\npCF6O4vsisxaDPzKZM8pqiUIOGjDwfqRQkzikSPu/oK8jPouof9JkeesUjbKg+0w\nQ7aZXgRPr8PJkHeY27/4bFz1riFPDZ+rKAe8DvXIlcjb70H68AtGnRzUkVjVlzhn\n6qfJE4LMmLtdQodW4Hnd2Oo8qujRprtn8AMcX5H1phIVUHYdIZpt44SNetOgCPxZ\n/S/0VLi2qD7bh/bBj6VRvea/LyCKnC75r+wnJGIHYpeVXskMrDBH+lfV1GsoU9Ig\nm+5MYAkc0SrgYBVCUXXvQNrip3IQcaWlW4YhkJC2rVBCA2ibc1MMWOyA0xYfJfy1\nvQIDAQAB\n-----END PUBLIC KEY-----\n";
    const TEST_JWT_VALID: &str = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0ZXIiLCJleHAiOjE4MjA2MDE3MDB9.U3NGxZxNiHStyGFGZOKsz4PPM3aX2papMEOsKUqWnYC7NmgGxESTKWKtPM6M5McKUQwD3cfnnFH9p3XGdkTfPofxcnmJcrN8PMWdaVAcetYn_c5ScMWgQapjuHiO7jBQKljTU4AwuGrNQAMtxgBgSdEX-MG1AYZrNYcdgqXtfUPk8y_V2icNU8kVQRzRonHs4yaioIqy1IpBuxpb6A5AHRy07T_En_TlebfH7Tb-lf3tFDT8UUcjnfFJ-TPxFTGqLsPnLbdqXcrKi-PVqpbuFj2WSFlGinbZDGEsjhopdHPD1_PkC0Am3c4XmiCM3QswVqcUujwED71lcZXBdCFxPA";
    const TEST_JWT_EXPIRED: &str = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0ZXIiLCJleHAiOjE3ODkwNjIxMDB9.GNrAExENuYVgbt8bZebTYTmCf2o0DcAkWzhpfkBCtAvPflzzr9xSNaEOrtf2HJU5gqOm-ZDIztXsatd765CtZA-uLcqjbCa4TURbMv0OFHDhMVlVfn2xr17jr39IG1ck7Ymioz_ZSd3wXu0egABsCUrKigEvtRwcGJSWlAp1GdRiWLpT2-U6Xb15bgUdbCNgYxyXiWM4zBLHVILLLH-zPyPIbvHo82l3qOpu7c2SRw5aNPrY2KXPCxKm47NEOkf74LJk14MZODeRTOYczyi8Q4vtD6xEcp8-u_vVUv5Gv04mrO1gMCALOQAWfKq2PFA0QatGX_cw8f36m_EOLLfuWA";
    const TEST_JWT_NOEXP: &str = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0ZXIifQ.dqsuFs9MyRMcgfPGvUAdCOJXKYFSMphpVASqmMH_n4aaAa3F7QeUz8ggX0931Z_TbgxvZ4Dp-Lf05eVSWOBWvpnjwSvn1JLw7axaEFl7BOEzsFzq_1gPMKMY1TXylXgUV2DHUhlayS53UcEvS5MP0vKQG07PsTOjFZACeuohilHX5vEmn9zwy67CbwL7Z3g32Msdb67pplMViAXGgau3UTYi5DOZFsSFlDrOj5w2Gg1EeZBpC1udtUvkIjHiW92LkoLI7HBzmRQwEuxGgOE134T2YGF5FuPjq-XpfiklV6SmrQpuHM_qGPtLHUm-6blwOgffAnr0uiCYVHZT5pe2hw";
    const TEST_JWT_WRONGKEY: &str = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0ZXIiLCJleHAiOjE4MjA2MDE3MDB9.Uu3vn8teCnUBsT-8updR4it_yyMoYdtqFGOvLgozxmsOmM8hb-FfqpeuqelBemgsbpwi2pbRVuOUxv0621ORempEpwfhCUnI80Rj9hPcOV0j8wYuX-xdsnvsNOPI1K6XlW9i1IOjToKxhptaPEPdYXf_jEq5v3P0zCITwevT2t6bpOdYzgSHjxq6CJiXIJHcP0idiQ1I3PqNhKmjJn3qi14GQ_xRk8Sf30VQDqsT76qXw0S92K-QKdcbc35oJW5x6oTdbgELv9YV-su1ooXTrLQ5zzY0g8qXhbkeLotXQEddaGMFeBOWchDax_km1g1gRbCS8f7TNEtO2xVkVHx-AQ";

    fn test_key_der() -> Vec<u8> {
        let (label, der) =
            parse_pem_block(TEST_RSA_PUBLIC_PEM.as_bytes()).expect("test PEM parses");
        assert_eq!(label, "PUBLIC KEY");
        spki_to_pkcs1(&der).expect("test SPKI unwraps to PKCS#1")
    }

    #[test]
    fn spki_unwrap_keeps_modulus_and_exponent() {
        // The unwrapped PKCS#1 body is a bare SEQUENCE of two INTEGERs.
        let pkcs1 = test_key_der();
        let (tag, content, rest) = der_read_tlv(&pkcs1).expect("PKCS#1 parses as DER");
        assert_eq!(tag, 0x30);
        assert!(rest.is_empty());
        let (n_tag, n_content, e_rest) = der_read_tlv(content).expect("modulus parses");
        assert_eq!(n_tag, 0x02);
        assert_eq!(n_content.len(), 257); // 2048-bit modulus + leading zero
        let (e_tag, e_content, e_end) = der_read_tlv(e_rest).expect("exponent parses");
        assert_eq!(e_tag, 0x02);
        assert_eq!(e_content, &[0x01, 0x00, 0x01]); // 65537
        assert!(e_end.is_empty());
    }

    #[test]
    fn jwt_verify_accepts_valid_token() {
        let key = test_key_der();
        assert_eq!(verify_jwt_raw(TEST_JWT_VALID, &key), Ok(()));
        // Tokens without `exp` carry no expiry to enforce.
        assert_eq!(verify_jwt_raw(TEST_JWT_NOEXP, &key), Ok(()));
    }

    #[test]
    fn jwt_verify_rejects_expired_token() {
        assert_eq!(
            verify_jwt_raw(TEST_JWT_EXPIRED, &test_key_der()),
            Err("Token has expired\n")
        );
    }

    #[test]
    fn jwt_verify_rejects_malformed_tokens() {
        let key = test_key_der();
        for bad in ["", "abc", "a.b", "a.b.c.d", "a..c", "!!!.@@@.###"] {
            assert_eq!(
                verify_jwt_raw(bad, &key),
                Err("Invalid JWT format\n"),
                "token: {}",
                bad
            );
        }
    }

    #[test]
    fn jwt_verify_rejects_non_rs256_alg() {
        use base64::Engine;
        let engine = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let none_hdr = engine.encode(r#"{"alg":"none","typ":"JWT"}"#);
        let hs256_hdr = engine.encode(r#"{"alg":"HS256","typ":"JWT"}"#);
        let payload = engine.encode(r#"{"sub":"tester"}"#);
        let sig = engine.encode(b"fakesig");
        let key = test_key_der();

        let tok_none = format!("{}.{}.{}", none_hdr, payload, sig);
        assert_eq!(verify_jwt_raw(&tok_none, &key), Err("Invalid JWT format\n"));

        let tok_hs256 = format!("{}.{}.{}", hs256_hdr, payload, sig);
        assert_eq!(
            verify_jwt_raw(&tok_hs256, &key),
            Err("Invalid JWT format\n")
        );
    }

    #[test]
    fn js_udf_bounds_infinite_loops_and_recursion() {
        use crate::state::{compile_and_register_js_udf, JavascriptUdf};

        // 1. Top-level infinite loop is rejected at registration time
        let bad_top_level = JavascriptUdf {
            id: "top_loop".to_string(),
            description: String::new(),
            script: "while (true) {} function top_loop(x) { return x; }".to_string(),
            is_agg: false,
        };
        assert!(compile_and_register_js_udf(&bad_top_level).is_err());

        // 2. Function-level infinite loop is aborted cleanly at invocation time and returns Null
        let fn_loop = JavascriptUdf {
            id: "infloop_udf".to_string(),
            description: String::new(),
            script: "function infloop_udf(x) { while (true) {} return x; }".to_string(),
            is_agg: false,
        };
        compile_and_register_js_udf(&fn_loop).expect("syntax is valid");
        let out =
            rekuiper_core::plugin::get_global_udf_registry().call_udf("infloop_udf", &[json!(1)]);
        assert_eq!(out, Some(Value::Null));

        // 3. Unbounded recursion is aborted cleanly at invocation time and returns Null
        let fn_rec = JavascriptUdf {
            id: "infrec_udf".to_string(),
            description: String::new(),
            script: "function infrec_udf(x) { return infrec_udf(x + 1); }".to_string(),
            is_agg: false,
        };
        compile_and_register_js_udf(&fn_rec).expect("syntax is valid");
        let out_rec =
            rekuiper_core::plugin::get_global_udf_registry().call_udf("infrec_udf", &[json!(1)]);
        assert_eq!(out_rec, Some(Value::Null));
    }

    #[test]
    fn test_load_auth_public_key_reloads_on_file_modification() {
        use crate::state::load_auth_public_key;
        let temp_dir = std::env::temp_dir().join(format!("test_auth_key_{}", uuid::Uuid::new_v4()));
        let _ = std::fs::create_dir_all(&temp_dir);
        let key_path = temp_dir.join("test_pub.pem");
        std::fs::write(&key_path, TEST_RSA_PUBLIC_PEM).expect("write key");

        std::env::set_var("REKUIPER_AUTH_PUBLIC_KEY_FILE", key_path.to_str().unwrap());

        let loaded1 = load_auth_public_key().expect("initial load");
        assert_eq!(loaded1, test_key_der());

        let loaded2 = load_auth_public_key().expect("cached load");
        assert_eq!(loaded2, test_key_der());

        std::env::remove_var("REKUIPER_AUTH_PUBLIC_KEY_FILE");
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn jwt_verify_rejects_wrong_key_and_tampering() {
        let key = test_key_der();
        assert_eq!(
            verify_jwt_raw(TEST_JWT_WRONGKEY, &key),
            Err("Invalid token signature\n")
        );
        // Swap two adjacent differing signature characters: the decoded
        // bytes must change, so verification has to fail.
        let sig_start = TEST_JWT_VALID.rfind('.').unwrap() + 1;
        let mut tampered = TEST_JWT_VALID.to_string();
        let bytes = tampered.as_bytes();
        let mut idx = None;
        for i in sig_start..bytes.len() - 1 {
            if bytes[i] != bytes[i + 1] {
                idx = Some(i);
                break;
            }
        }
        let i = idx.expect("signature has swappable characters");
        tampered.replace_range(
            i..i + 2,
            &format!("{}{}", bytes[i + 1] as char, bytes[i] as char),
        );
        assert_ne!(tampered, TEST_JWT_VALID);
        assert_eq!(
            verify_jwt_raw(&tampered, &key),
            Err("Invalid token signature\n")
        );
    }

    #[tokio::test]
    async fn resolve_edgex_source_defaults_and_options() {
        let manager = test_stream_manager(&[("TYPE", "edgex")]).await;
        let confs = test_source_configs(&[]);
        let schemas = SchemaManager::new();
        let cfg = resolve_edgex_source(&manager, &confs, &schemas, "demo", "rule1")
            .expect("resolves edgex stream");
        assert_eq!(cfg.server, "tcp://edgex-mqtt-broker:1883");
        assert_eq!(cfg.topic, "edgex/rules-events");
        assert_eq!(cfg.format, PayloadFormat::EdgeX);

        // Custom SERVER and DATASOURCE override defaults
        let manager_custom = test_stream_manager(&[
            ("TYPE", "edgex"),
            ("SERVER", "192.168.1.50:1883"),
            ("DATASOURCE", "custom/bus"),
        ])
        .await;
        let cfg_custom = resolve_edgex_source(&manager_custom, &confs, &schemas, "demo", "rule2")
            .expect("resolves custom edgex stream");
        assert_eq!(cfg_custom.server, "tcp://192.168.1.50:1883");
        assert_eq!(cfg_custom.topic, "custom/bus");
    }

    #[test]
    fn prepare_actions_parses_edgex_action() {
        let confs = test_source_configs(&[]);
        let mut action_map = HashMap::new();
        action_map.insert(
            "edgex".to_string(),
            serde_json::json!({
                "topic": "edgex/alerts",
                "deviceName": "testDevice",
                "profileName": "testProfile"
            }),
        );
        let actions = vec![action_map];
        let prepared = prepare_actions(&actions, "rule100", &confs, None);
        assert_eq!(prepared.len(), 1);
        match &prepared[0] {
            PreparedAction::EdgeX {
                device_name,
                profile_name,
                source_name,
                config,
                template,
            } => {
                assert_eq!(device_name, "testDevice");
                assert_eq!(profile_name, "testProfile");
                assert_eq!(source_name, "rule100");
                assert_eq!(config.server, "tcp://edgex-mqtt-broker:1883");
                assert_eq!(config.topic, "edgex/alerts");
                assert_eq!(config.format, PayloadFormat::EdgeX);
                assert!(template.is_none());
            }
            _ => panic!("expected PreparedAction::EdgeX"),
        }
    }

    #[test]
    fn test_prepare_actions_respects_disabled_flag() {
        let confs = test_source_configs(&[]);
        let mut action_enabled = HashMap::new();
        action_enabled.insert("log".to_string(), serde_json::json!({}));

        let mut action_disabled_inner = HashMap::new();
        action_disabled_inner.insert("log".to_string(), serde_json::json!({"disabled": true}));

        let mut action_disabled_outer = HashMap::new();
        action_disabled_outer.insert("log".to_string(), serde_json::json!({}));
        action_disabled_outer.insert("disabled".to_string(), serde_json::json!(true));

        let mut action_disable_inner = HashMap::new();
        action_disable_inner.insert("log".to_string(), serde_json::json!({"disable": true}));

        let mut action_disable_outer = HashMap::new();
        action_disable_outer.insert("log".to_string(), serde_json::json!({}));
        action_disable_outer.insert("disable".to_string(), serde_json::json!(true));

        let actions = vec![
            action_enabled,
            action_disabled_inner,
            action_disabled_outer,
            action_disable_inner,
            action_disable_outer,
        ];
        let prepared = prepare_actions(&actions, "rule_disabled_test", &confs, None);
        assert_eq!(prepared.len(), 1);
        let cache_confs = action_cache_configs(&actions);
        assert_eq!(cache_confs.len(), 1);
    }

    #[test]
    fn test_validate_sink_actions_skips_disabled() {
        let mut invalid_disabled = HashMap::new();
        invalid_disabled.insert("mqtt".to_string(), serde_json::json!({"disabled": true}));
        assert!(crate::handlers::rules::validate_sink_actions(&[invalid_disabled]).is_ok());

        let mut invalid_disable = HashMap::new();
        invalid_disable.insert("mqtt".to_string(), serde_json::json!({"disable": true}));
        assert!(crate::handlers::rules::validate_sink_actions(&[invalid_disable]).is_ok());

        let mut invalid_enabled = HashMap::new();
        invalid_enabled.insert(
            "mqtt".to_string(),
            serde_json::json!({"server": "tcp://127.0.0.1:1883"}),
        );
        assert!(crate::handlers::rules::validate_sink_actions(&[invalid_enabled]).is_err());
    }

    #[tokio::test]
    async fn test_reject_invalid_rule_conflicting_buffer_full_policy() {
        use crate::handlers::rules::reject_invalid_rule;
        use crate::state::AppState;
        use rekuiper_core::model::StreamDefinition;

        let sm = StreamManager::new();
        let bus = StreamBus::new();
        let mut opts1 = HashMap::new();
        opts1.insert("BUFFER_FULL_POLICY".to_string(), "block".to_string());
        sm.create_stream(StreamDefinition {
            name: "stream_block".to_string(),
            sql: "CREATE STREAM stream_block () WITH (BUFFER_FULL_POLICY=\"block\")".to_string(),
            stream_fields: Vec::new(),
            options: opts1.clone(),
        })
        .await
        .unwrap();

        let mut opts2 = HashMap::new();
        opts2.insert("BUFFER_FULL_POLICY".to_string(), "dropOldest".to_string());
        sm.create_stream(StreamDefinition {
            name: "stream_drop".to_string(),
            sql: "CREATE STREAM stream_drop () WITH (BUFFER_FULL_POLICY=\"dropOldest\")"
                .to_string(),
            stream_fields: Vec::new(),
            options: opts2,
        })
        .await
        .unwrap();

        sm.create_stream(StreamDefinition {
            name: "stream_block2".to_string(),
            sql: "CREATE STREAM stream_block2 () WITH (BUFFER_FULL_POLICY=\"block\")".to_string(),
            stream_fields: Vec::new(),
            options: opts1,
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

        // Conflicting: block + dropOldest
        let mut parser_conflict = Parser::new(
            "SELECT * FROM stream_block INNER JOIN stream_drop ON stream_block.id = stream_drop.id GROUP BY TumblingWindow(ss, 10)",
        );
        let stmt_conflict = parser_conflict.parse_select().unwrap();
        assert!(reject_invalid_rule(&state, &stmt_conflict, None, false).is_some());

        // Matching: block + block
        let mut parser_match = Parser::new(
            "SELECT * FROM stream_block INNER JOIN stream_block2 ON stream_block.id = stream_block2.id GROUP BY TumblingWindow(ss, 10)",
        );
        let stmt_match = parser_match.parse_select().unwrap();
        assert!(reject_invalid_rule(&state, &stmt_match, None, false).is_none());
    }
}
