use crate::engine::joins::apply_lookup_joins;
use crate::engine::sinks::enqueue_sink_record;
use indexmap::IndexMap;
use parking_lot::RwLock;
use rekuiper_core::model::{RuleStatus, StreamRecord};
use rekuiper_core::{RuleCounters, RuleManager, StreamReceiver, TableManager};
use rekuiper_sql::eval::RowLookup;
use rekuiper_sql::{Evaluator, RuleState, SelectStmt};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

#[inline]
pub(crate) fn is_rule_running(status: &Arc<RwLock<RuleStatus>>) -> bool {
    if let Some(guard) = status.try_read() {
        guard.status == "running"
    } else {
        true
    }
}

pub(crate) async fn restore_rule_checkpoint(
    rule_manager: &RuleManager,
    rule_id: &str,
    rule_state: &RuleState,
) {
    if let Some(saved_json) = rule_manager.load_rule_checkpoint(rule_id).await {
        if let Ok(map) = serde_json::from_str::<HashMap<String, Value>>(&saved_json) {
            let mut guard = rule_state.state.write();
            for (k, v) in map {
                guard.insert(k, v);
            }
        }
    }
}

pub(crate) fn spawn_checkpoint_ticker(
    rule_manager: RuleManager,
    rule_id: String,
    rule_state: RuleState,
    running: Arc<RwLock<RuleStatus>>,
    interval_ms: u64,
) {
    if interval_ms == 0 {
        return;
    }
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_millis(interval_ms));
        ticker.tick().await;
        loop {
            ticker.tick().await;
            if !is_rule_running(&running) {
                break;
            }
            let snapshot = {
                let guard = rule_state.state.read();
                guard.clone()
            };
            if let Ok(json) = serde_json::to_string(&snapshot) {
                let _ = rule_manager.save_rule_checkpoint(&rule_id, &json).await;
            }
        }
    });
}

/// Extracts an upstream error message when the record carries an `error` or
/// `__error` / `__raw_error__` field (emitted by source decoders on malformed payloads).
#[inline]
pub(crate) fn check_record_error<R: RowLookup + ?Sized>(data: &R) -> Option<String> {
    for (k, v) in data.iter_row() {
        if (k.starts_with('e') || k.starts_with('_'))
            && (k == "error" || k == "__error" || k == "__raw_error__")
        {
            return Some(
                v.as_str()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| v.to_string()),
            );
        }
    }
    None
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_runtime_error(
    counters: &RuleCounters,
    rule_id: &str,
    send_error: bool,
    sink: &tokio::sync::mpsc::Sender<StreamRecord>,
    err_msg: String,
    stage: Option<rekuiper_core::model::DlqStage>,
    record_data: Option<&IndexMap<String, Value>>,
    dlq_router: Option<&crate::engine::dlq::DlqRouter>,
) {
    if let Some(dlq) = dlq_router {
        let stg = stage.unwrap_or(rekuiper_core::model::DlqStage::SqlEval);
        let tuple_val = record_data.and_then(|d| serde_json::to_value(d).ok());
        let raw_str = record_data
            .and_then(|d| {
                d.get("__raw_payload__")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .or_else(|| record_data.and_then(|d| serde_json::to_string(d).ok()));
        dlq.route_error(stg, err_msg.clone(), raw_str.as_deref(), tuple_val, None);
    } else {
        counters.record_exception(&err_msg);
    }
    if send_error {
        let mut err_data = IndexMap::new();
        err_data.insert("error".to_string(), Value::String(err_msg.clone()));
        err_data.insert("rule_id".to_string(), Value::String(rule_id.to_string()));
        err_data.insert("__raw_error__".to_string(), Value::String(err_msg));
        enqueue_sink_record(counters, sink, StreamRecord::new(err_data)).await;
    }
}

/// Handles an upstream error record per the rule `sendError` option. Returns
/// `true` when the record was an error record (counted as an exception and,
/// when enabled, forwarded immediately to the sink); the caller must then
/// `continue` without normal projection or window-buffer insertion, mirroring
/// eKuiper semantics where the error event bypasses window aggregation.
pub(crate) async fn handle_error_record(
    counters: &RuleCounters,
    rule_id: &str,
    send_error: bool,
    sink: &tokio::sync::mpsc::Sender<StreamRecord>,
    record: &StreamRecord,
    dlq_router: Option<&crate::engine::dlq::DlqRouter>,
) -> bool {
    let Some(err_msg) = check_record_error(&record.data) else {
        return false;
    };
    handle_runtime_error(
        counters,
        rule_id,
        send_error,
        sink,
        err_msg,
        Some(rekuiper_core::model::DlqStage::CodecDecode),
        Some(&*record.data),
        dlq_router,
    )
    .await;
    true
}

/// Event-time configuration for windowed rules: when `enabled`, window
/// boundaries derive from payload event timestamps instead of arrival time,
/// with `late_tolerance_ms` grace for out-of-order events.
#[derive(Clone, Default)]
pub(crate) struct EventTimeConfig {
    pub(crate) enabled: bool,
    pub(crate) late_tolerance_ms: i64,
    pub(crate) timestamp_field: Option<String>,
    pub(crate) source_timestamp_fields: HashMap<String, String>,
}

pub(crate) fn parse_timestamp_val(v: &Value) -> Option<i64> {
    if let Some(n) = v.as_i64() {
        return Some(n);
    }
    if let Some(n) = v.as_u64() {
        return Some(n as i64);
    }
    if let Some(f) = v.as_f64() {
        return Some(f as i64);
    }
    if let Some(s) = v.as_str() {
        if let Ok(n) = s.parse::<i64>() {
            return Some(n);
        }
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
            return Some(dt.timestamp_millis());
        }
    }
    None
}

pub(crate) fn extract_event_timestamp<R: RowLookup + ?Sized>(
    data: &R,
    configured_field: Option<&str>,
) -> i64 {
    if let Some(field) = configured_field {
        if let Some(v) = data.get(field).and_then(parse_timestamp_val) {
            return v;
        }
    }
    for key in ["timestamp", "ts", "event_time", "time"] {
        if let Some(v) = data.get(key).and_then(parse_timestamp_val) {
            return v;
        }
    }
    chrono::Utc::now().timestamp_millis()
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_stateless_rule(
    counters: Arc<RuleCounters>,
    running: Arc<RwLock<RuleStatus>>,
    rule_id: String,
    select_stmt: SelectStmt,
    mut rx: StreamReceiver,
    table_manager: TableManager,
    source_configs: Arc<RwLock<HashMap<String, Value>>>,
    sink: tokio::sync::mpsc::Sender<StreamRecord>,
    send_error: bool,
    join_rxs: Vec<(String, StreamReceiver)>,
    event_time: EventTimeConfig,
    rule_manager: RuleManager,
    checkpoint_interval_ms: u64,
    dlq_router: crate::engine::dlq::DlqRouter,
    where_pre_filtered: bool,
) {
    // Running analytic state for acc_* cumulative functions. The stateful
    // projection below runs for every input row (advancing cumulative state
    // even for rows the WHERE filter later drops, mirroring eKuiper analytic
    // semantics); the input-row filter then decides emission.
    let rule_state = RuleState::default();
    let start_time_ms = chrono::Utc::now().timestamp_millis();
    rule_state
        .state
        .write()
        .insert("__rule_id__".to_string(), Value::String(rule_id.clone()));
    rule_state
        .state
        .write()
        .insert("__rule_start__".to_string(), Value::from(start_time_ms));

    // Restore saved state from checkpoint before event loop
    restore_rule_checkpoint(&rule_manager, &rule_id, &rule_state).await;

    // Periodic CoW snapshot ticker
    if checkpoint_interval_ms > 0 {
        spawn_checkpoint_ticker(
            rule_manager.clone(),
            rule_id.clone(),
            rule_state.clone(),
            running.clone(),
            checkpoint_interval_ms,
        );
    }

    let has_analytic = Evaluator::stmt_has_analytic(&select_stmt);
    let has_delay = Evaluator::stmt_has_delay(&select_stmt);
    let field_names = Evaluator::select_field_names(&select_stmt);

    let has_asof = select_stmt.joins.iter().any(|j| {
        matches!(
            j.join_type,
            rekuiper_sql::JoinType::Asof | rekuiper_sql::JoinType::LeftAsof
        )
    });
    let has_lookup_joins = select_stmt.joins.iter().any(|j| {
        !matches!(
            j.join_type,
            rekuiper_sql::JoinType::Asof | rekuiper_sql::JoinType::LeftAsof
        )
    });

    if has_asof {
        let mut join_rx = crate::engine::windows::spawn_join_forwarders(join_rxs);
        let mut asof_state = crate::engine::joins::AsofJoinState::new();
        let left_ts_field = event_time
            .source_timestamp_fields
            .get(&select_stmt.from)
            .cloned()
            .or_else(|| event_time.timestamp_field.clone());
        let mut sample_seq = 0u32;

        loop {
            tokio::select! {
                Some(tagged) = join_rx.recv() => {
                    let right_ts_field = event_time
                        .source_timestamp_fields
                        .get(&tagged.source)
                        .map(|s| s.as_str());
                    let right_ts = extract_event_timestamp(&tagged.data, right_ts_field);
                    asof_state.ingest_right_event(&select_stmt, &tagged.source, right_ts, &tagged.data);
                }
                Some(record) = rx.recv() => {
                    if !is_rule_running(&running) {
                        continue;
                    }
                    counters.inc_source(1);
                    if handle_error_record(&counters, &rule_id, send_error, &sink, &record, Some(&dlq_router)).await {
                        if dlq_router.on_error == crate::engine::dlq::OnErrorPolicy::Halt {
                            break;
                        }
                        continue;
                    }
                    sample_seq = sample_seq.wrapping_add(1);
                    let is_sampled = (sample_seq & 0x7F) == 0;
                    let eval_start = if is_sampled {
                        Some(std::time::Instant::now())
                    } else {
                        None
                    };
                    let left_ts = extract_event_timestamp(&record.data, left_ts_field.as_deref());
                    let Some(probed) = asof_state.probe_left_event(&select_stmt, left_ts, &record.data) else {
                        counters.inc_filtered(1);
                        if let Some(start) = eval_start {
                            counters.op_latency.observe(start.elapsed().as_micros() as u64);
                        }
                        continue;
                    };
                    if has_delay {
                        Evaluator::begin_async_eval();
                    }
                    let eval_res = if has_lookup_joins {
                        let Some(joined) = apply_lookup_joins(
                            &table_manager,
                            &source_configs,
                            &select_stmt,
                            &probed,
                        )
                        .await
                        else {
                            if has_delay {
                                let _ = Evaluator::finish_async_eval();
                            }
                            counters.inc_filtered(1);
                            if let Some(start) = eval_start {
                                counters.op_latency.observe(start.elapsed().as_micros() as u64);
                            }
                            continue;
                        };
                        Evaluator::eval_select_stateless_fast(
                            &select_stmt,
                            &field_names,
                            has_analytic,
                            &joined,
                            &rule_state,
                        )
                    } else {
                        Evaluator::eval_select_stateless_fast(
                            &select_stmt,
                            &field_names,
                            has_analytic,
                            &probed,
                            &rule_state,
                        )
                    };
                    if has_delay {
                        let delay_ms = Evaluator::finish_async_eval();
                        if delay_ms > 0 {
                            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                        }
                    }
                    if let Some(start) = eval_start {
                        counters.op_latency.observe(start.elapsed().as_micros() as u64);
                    }
                    match eval_res {
                        Ok(Some(output)) => {
                            let out_rec = StreamRecord::with_timestamp_and_meta(
                                output,
                                record.timestamp,
                                record.meta.clone(),
                            );
                            if !enqueue_sink_record(&counters, &sink, out_rec).await {
                                return;
                            }
                        }
                        Ok(None) => {
                            counters.inc_filtered(1);
                        }
                        Err(err) => {
                            handle_runtime_error(
                                &counters,
                                &rule_id,
                                send_error,
                                &sink,
                                err,
                                Some(rekuiper_core::model::DlqStage::SqlEval),
                                Some(&probed),
                                Some(&dlq_router),
                            ).await;
                            if dlq_router.on_error == crate::engine::dlq::OnErrorPolicy::Halt {
                                break;
                            }
                        }
                    }
                }
                else => break,
            }
        }
    } else {
        const DRAIN_BATCH_LIMIT: usize = 64;
        let mut sample_seq = 0u32;

        while let Some(mut record) = rx.recv().await {
            if !is_rule_running(&running) {
                break;
            }
            sample_seq = sample_seq.wrapping_add(1);
            let is_sampled = (sample_seq & 0x1FF) == 0;
            let batch_start = if is_sampled {
                Some(std::time::Instant::now())
            } else {
                None
            };
            let mut drained = 0usize;
            let mut batch_source_in = 0u64;
            let mut batch_filtered = 0u64;
            let mut batch_enqueued = 0u64;
            loop {
                'process: {
                    batch_source_in += 1;
                    // Synchronous check: only `.await` when an error field actually exists
                    if let Some(err_msg) = check_record_error(&record.data) {
                        counters.inc_source(batch_source_in);
                        batch_source_in = 0;
                        if batch_filtered > 0 {
                            counters
                                .filtered
                                .fetch_add(batch_filtered, std::sync::atomic::Ordering::Relaxed);
                            batch_filtered = 0;
                        }
                        if batch_enqueued > 0 {
                            counters
                                .enqueued
                                .fetch_add(batch_enqueued, std::sync::atomic::Ordering::Relaxed);
                            batch_enqueued = 0;
                        }
                        handle_runtime_error(
                            &counters,
                            &rule_id,
                            send_error,
                            &sink,
                            err_msg,
                            Some(rekuiper_core::model::DlqStage::CodecDecode),
                            Some(&*record.data),
                            Some(&dlq_router),
                        )
                        .await;
                        if dlq_router.on_error == crate::engine::dlq::OnErrorPolicy::Halt {
                            return;
                        }
                        break 'process;
                    }
                    if has_delay {
                        Evaluator::begin_async_eval();
                    }
                    let eval_res = if has_lookup_joins {
                        let Some(joined) = apply_lookup_joins(
                            &table_manager,
                            &source_configs,
                            &select_stmt,
                            &record.data,
                        )
                        .await
                        else {
                            if has_delay {
                                let _ = Evaluator::finish_async_eval();
                            }
                            batch_filtered += 1;
                            break 'process;
                        };
                        Evaluator::eval_select_stateless_fast(
                            &select_stmt,
                            &field_names,
                            has_analytic,
                            &joined,
                            &rule_state,
                        )
                    } else if where_pre_filtered && !has_analytic {
                        Evaluator::eval_select_stateful_with_names(
                            &select_stmt,
                            &field_names,
                            &record.data,
                            &rule_state,
                        )
                    } else {
                        Evaluator::eval_select_stateless_fast(
                            &select_stmt,
                            &field_names,
                            has_analytic,
                            &record.data,
                            &rule_state,
                        )
                    };
                    if has_delay {
                        let delay_ms = Evaluator::finish_async_eval();
                        if delay_ms > 0 {
                            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                        }
                    }
                    match eval_res {
                        Ok(Some(output)) => {
                            let out_rec = StreamRecord::with_timestamp_and_meta(
                                output,
                                record.timestamp,
                                record.meta.clone(),
                            );
                            match sink.try_send(out_rec) {
                                Ok(()) => {
                                    batch_enqueued += 1;
                                }
                                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                    counters.inc_source(batch_source_in);
                                    if batch_filtered > 0 {
                                        counters.filtered.fetch_add(
                                            batch_filtered,
                                            std::sync::atomic::Ordering::Relaxed,
                                        );
                                    }
                                    if batch_enqueued > 0 {
                                        counters.enqueued.fetch_add(
                                            batch_enqueued,
                                            std::sync::atomic::Ordering::Relaxed,
                                        );
                                    }
                                    return;
                                }
                                Err(tokio::sync::mpsc::error::TrySendError::Full(rec)) => {
                                    if batch_enqueued > 0 {
                                        counters.enqueued.fetch_add(
                                            batch_enqueued,
                                            std::sync::atomic::Ordering::Relaxed,
                                        );
                                        batch_enqueued = 0;
                                    }
                                    counters.observe_high_water(sink.max_capacity());
                                    let wait_start = std::time::Instant::now();
                                    match sink.send(rec).await {
                                        Ok(()) => {
                                            counters.add_blocked_micros(
                                                wait_start.elapsed().as_micros() as u64,
                                            );
                                            counters.inc_enqueued(1);
                                        }
                                        Err(_) => {
                                            counters.inc_source(batch_source_in);
                                            if batch_filtered > 0 {
                                                counters.filtered.fetch_add(
                                                    batch_filtered,
                                                    std::sync::atomic::Ordering::Relaxed,
                                                );
                                            }
                                            return;
                                        }
                                    }
                                }
                            }
                        }
                        Ok(None) => batch_filtered += 1,
                        Err(err) => {
                            counters.inc_source(batch_source_in);
                            batch_source_in = 0;
                            if batch_filtered > 0 {
                                counters.filtered.fetch_add(
                                    batch_filtered,
                                    std::sync::atomic::Ordering::Relaxed,
                                );
                                batch_filtered = 0;
                            }
                            if batch_enqueued > 0 {
                                counters.enqueued.fetch_add(
                                    batch_enqueued,
                                    std::sync::atomic::Ordering::Relaxed,
                                );
                                batch_enqueued = 0;
                            }
                            handle_runtime_error(
                                &counters,
                                &rule_id,
                                send_error,
                                &sink,
                                err,
                                Some(rekuiper_core::model::DlqStage::SqlEval),
                                Some(&*record.data),
                                Some(&dlq_router),
                            )
                            .await;
                            if dlq_router.on_error == crate::engine::dlq::OnErrorPolicy::Halt {
                                return;
                            }
                        }
                    }
                }

                drained += 1;
                if drained >= DRAIN_BATCH_LIMIT {
                    break;
                }
                match rx.try_recv() {
                    Ok(next) => record = next,
                    Err(_) => break,
                }
            }
            if batch_source_in > 0 {
                counters.inc_source(batch_source_in);
            }
            if batch_filtered > 0 {
                counters
                    .filtered
                    .fetch_add(batch_filtered, std::sync::atomic::Ordering::Relaxed);
            }
            if batch_enqueued > 0 {
                counters
                    .enqueued
                    .fetch_add(batch_enqueued, std::sync::atomic::Ordering::Relaxed);
            }
            if let Some(start) = batch_start {
                counters
                    .op_latency
                    .observe_batch(start.elapsed().as_micros() as u64, drained as u64);
            }
        }
    }
}
