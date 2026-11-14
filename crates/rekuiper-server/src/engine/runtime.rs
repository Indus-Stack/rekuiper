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
        const DRAIN_BATCH_LIMIT: usize = 1024;
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
                        Evaluator::eval_select_stateless_fast_shared(
                            &select_stmt,
                            &field_names,
                            has_analytic,
                            &joined,
                            &rule_state,
                        )
                    } else if where_pre_filtered && !has_analytic {
                        Evaluator::eval_select_stateful_with_names_shared(
                            &select_stmt,
                            &field_names,
                            &record.data,
                            &rule_state,
                        )
                    } else {
                        Evaluator::eval_select_stateless_fast_shared(
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

/// Opt-in reliable single-source SQL checkpoint recovery (P03-T07).
///
/// Kept completely separate from the default `run_rule_loop` hotpath so
/// non-reliable rules incur zero overhead.
pub mod reliable {
    use crate::engine::batching::{ActionBatchAssembler, BatchConfig, BatchItem, PushDecision};
    use crate::engine::retry::{execute_with_retry, RetryPolicy};
    use indexmap::IndexMap;
    use rekuiper_connectors::kafka::{
        KafkaPositionCodec, KafkaPreparedMessage, KafkaReliableError, KafkaReliablePartitionReader,
    };
    use rekuiper_core::contracts::{
        CheckpointError, CheckpointId, CheckpointStore, Clock, ContentChecksum,
        DeadlineCancellation, EffectOutcome, OperatorId, OutboxBoundary, PlanIdentity,
        SourcePartition, StateError, StateParticipant, StateResourceUsage, StateSchemaVersion,
        StateSnapshot, STATE_ENVELOPE_VERSION,
    };
    use rekuiper_core::model::StreamRecord;
    use rekuiper_core::reliability::{CheckpointCoordinator, CoordinatorError};
    use rekuiper_sql::ast::{TimeUnit, WindowDef};
    use rekuiper_sql::{Evaluator, SelectStmt};
    use serde::{Deserialize, Serialize};
    use serde_json::Value;
    use std::time::Duration;

    const RUNNER_WINDOW_OPERATOR_ID: &str = "__runner_window_state__";
    const RUNNER_WINDOW_STATE_VERSION: StateSchemaVersion = StateSchemaVersion(1);

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct PersistedRunnerWindowState {
        #[serde(default)]
        stmt_fingerprint: Option<String>,
        window_buffer: Vec<(i64, IndexMap<String, Value>)>,
        tumbling_start_ms: Option<i64>,
    }

    struct RunnerWindowStateParticipant {
        operator_id: OperatorId,
        state: PersistedRunnerWindowState,
    }

    impl RunnerWindowStateParticipant {
        fn new(
            stmt_fingerprint: Option<String>,
            window_buffer: Vec<(i64, IndexMap<String, Value>)>,
            tumbling_start_ms: Option<i64>,
        ) -> Self {
            let operator_id = OperatorId::try_from(RUNNER_WINDOW_OPERATOR_ID)
                .expect("valid static runner window operator id");
            Self {
                operator_id,
                state: PersistedRunnerWindowState {
                    stmt_fingerprint,
                    window_buffer,
                    tumbling_start_ms,
                },
            }
        }
    }

    impl StateParticipant for RunnerWindowStateParticipant {
        fn operator_id(&self) -> &OperatorId {
            &self.operator_id
        }

        fn state_version(&self) -> StateSchemaVersion {
            RUNNER_WINDOW_STATE_VERSION
        }

        fn resource_usage(&self) -> StateResourceUsage {
            let bytes = serde_json::to_vec(&self.state)
                .map(|b| u64::try_from(b.len()).unwrap_or(u64::MAX))
                .unwrap_or(0);
            StateResourceUsage {
                records: u64::try_from(self.state.window_buffer.len()).unwrap_or(u64::MAX),
                bytes,
            }
        }

        fn capture_snapshot(&self) -> Result<StateSnapshot, StateError> {
            let payload = serde_json::to_vec(&self.state)
                .map_err(|e| StateError::CorruptSnapshot(e.to_string()))?;
            let bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);
            Ok(StateSnapshot {
                envelope_version: STATE_ENVELOPE_VERSION,
                operator_id: self.operator_id.clone(),
                state_version: RUNNER_WINDOW_STATE_VERSION,
                checksum: ContentChecksum::sha256_bytes(&payload),
                payload,
                usage: StateResourceUsage {
                    records: u64::try_from(self.state.window_buffer.len()).unwrap_or(u64::MAX),
                    bytes,
                },
            })
        }

        fn restore_snapshot(&mut self, snapshot: &StateSnapshot) -> Result<(), StateError> {
            snapshot.validate(&self.operator_id, &[RUNNER_WINDOW_STATE_VERSION])?;
            let expected_checksum = ContentChecksum::sha256_bytes(&snapshot.payload);
            if expected_checksum != snapshot.checksum {
                return Err(StateError::ChecksumMismatch(self.operator_id.clone()));
            }
            self.state = serde_json::from_slice(&snapshot.payload)
                .map_err(|e| StateError::CorruptSnapshot(e.to_string()))?;
            Ok(())
        }
    }

    /// Exact delivery guarantee exposed by the single-source reliable engine.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ReliableDeliveryGuarantee {
        AtLeastOnce,
    }

    /// Errors returned during reliable plan validation, restore, or checkpointing.
    #[derive(Debug)]
    pub enum ReliableRuntimeError {
        UnsupportedShape(&'static str),
        MissingParticipant(&'static str),
        IncompleteBarrierActions(String),
        Kafka(KafkaReliableError),
        State(StateError),
        Checkpoint(CheckpointError),
        Coordinator(CoordinatorError),
        SinkRejected(String),
    }

    impl std::fmt::Display for ReliableRuntimeError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::UnsupportedShape(msg) => write!(f, "unsupported reliable plan shape: {msg}"),
                Self::MissingParticipant(kind) => {
                    write!(f, "missing required state participant: {kind}")
                }
                Self::IncompleteBarrierActions(missing) => {
                    write!(f, "barrier completion missing actions: {missing}")
                }
                Self::Kafka(e) => write!(f, "kafka reliable error: {e}"),
                Self::State(e) => write!(f, "state error: {e}"),
                Self::Checkpoint(e) => write!(f, "checkpoint error: {e}"),
                Self::Coordinator(e) => write!(f, "coordinator error: {e}"),
                Self::SinkRejected(msg) => write!(f, "sink rejected output: {msg}"),
            }
        }
    }

    impl std::error::Error for ReliableRuntimeError {}

    impl From<KafkaReliableError> for ReliableRuntimeError {
        fn from(v: KafkaReliableError) -> Self {
            Self::Kafka(v)
        }
    }

    impl From<StateError> for ReliableRuntimeError {
        fn from(v: StateError) -> Self {
            Self::State(v)
        }
    }

    impl From<CheckpointError> for ReliableRuntimeError {
        fn from(v: CheckpointError) -> Self {
            Self::Checkpoint(v)
        }
    }

    impl From<CoordinatorError> for ReliableRuntimeError {
        fn from(v: CoordinatorError) -> Self {
            Self::Coordinator(v)
        }
    }

    /// Validates that `stmt` is eligible for single-source reliable execution
    /// (single Kafka partition source; analytic, count, or tumbling window only;
    /// no joins or set operations).
    pub fn validate_reliable_single_source_plan(
        stmt: &SelectStmt,
    ) -> Result<ReliableDeliveryGuarantee, ReliableRuntimeError> {
        if !stmt.joins.is_empty() {
            return Err(ReliableRuntimeError::UnsupportedShape(
                "JOIN plans are not supported in single-source reliable mode",
            ));
        }
        if stmt.set_op.is_some() {
            return Err(ReliableRuntimeError::UnsupportedShape(
                "UNION/set-op plans are not supported in single-source reliable mode",
            ));
        }
        if let Some(ref win) = stmt.window {
            match win {
                WindowDef::TumblingTime { .. } | WindowDef::Count { .. } => {}
                _ => {
                    return Err(ReliableRuntimeError::UnsupportedShape(
                        "only tumbling, count, or analytic (windowless) plans are supported",
                    ));
                }
            }
        }
        Ok(ReliableDeliveryGuarantee::AtLeastOnce)
    }

    /// Observable checkpoint health and guarantee status for a reliable rule.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct ReliableCheckpointStatus {
        pub guarantee: ReliableDeliveryGuarantee,
        pub last_checkpoint_id: Option<CheckpointId>,
        pub last_checkpoint_wall_millis: Option<i64>,
        pub last_failure: Option<String>,
        pub restored_next_offset: Option<i64>,
    }

    impl ReliableCheckpointStatus {
        pub fn new(guarantee: ReliableDeliveryGuarantee) -> Self {
            Self {
                guarantee,
                last_checkpoint_id: None,
                last_checkpoint_wall_millis: None,
                last_failure: None,
                restored_next_offset: None,
            }
        }

        pub fn checkpoint_age_ms(&self, now_wall_millis: i64) -> Option<u64> {
            self.last_checkpoint_wall_millis
                .map(|ts| u64::try_from(now_wall_millis.saturating_sub(ts)).unwrap_or(0))
        }
    }

    /// Sink adapter used by [`ReliableSingleSourceRunner`] to deliver flushed
    /// output batches before checkpoint commit.
    #[allow(async_fn_in_trait)]
    pub trait ReliableSinkAdapter: Send + Sync {
        async fn deliver_batch(&self, records: &[Value]) -> EffectOutcome;
    }

    /// Coordinates restore-before-intake, evaluation, output flush, and
    /// checkpoint commit for a single Kafka partition + SQL plan.
    pub struct ReliableSingleSourceRunner {
        identity: PlanIdentity,
        partition: SourcePartition,
        stmt: SelectStmt,
        coordinator: CheckpointCoordinator,
        next_checkpoint_id: u64,
        status: ReliableCheckpointStatus,
        retry_policy: RetryPolicy,
        window_buffer: Vec<(i64, IndexMap<String, Value>)>,
        tumbling_start_ms: Option<i64>,
    }

    impl ReliableSingleSourceRunner {
        pub fn new(
            identity: PlanIdentity,
            partition: SourcePartition,
            stmt: SelectStmt,
            retry_policy: RetryPolicy,
        ) -> Result<Self, ReliableRuntimeError> {
            let guarantee = validate_reliable_single_source_plan(&stmt)?;
            let coordinator = CheckpointCoordinator::new(identity.clone());
            Ok(Self {
                identity,
                partition,
                stmt,
                coordinator,
                next_checkpoint_id: 1,
                status: ReliableCheckpointStatus::new(guarantee),
                retry_policy,
                window_buffer: Vec::new(),
                tumbling_start_ms: None,
            })
        }

        pub fn status(&self) -> &ReliableCheckpointStatus {
            &self.status
        }

        fn stmt_fingerprint(&self) -> String {
            ContentChecksum::sha256_bytes(format!("{:?}", self.stmt).as_bytes())
                .as_str()
                .to_owned()
        }

        /// Restores participant state and Kafka next-offset BEFORE intake begins.
        pub async fn restore_before_intake(
            &mut self,
            store: &dyn CheckpointStore,
            participants: &mut [&mut dyn StateParticipant],
        ) -> Result<Option<i64>, ReliableRuntimeError> {
            store
                .verify_generation(&self.identity.rule_id, self.identity.generation)
                .await?;
            let Some(envelope) = store.load_latest(&self.identity.rule_id).await? else {
                return Ok(None);
            };
            let mut expected_participants: std::collections::BTreeSet<OperatorId> = participants
                .iter()
                .map(|p| p.operator_id().clone())
                .collect();
            if self.stmt.window.is_some() {
                expected_participants.insert(
                    OperatorId::try_from(RUNNER_WINDOW_OPERATOR_ID)
                        .expect("valid static runner window operator id"),
                );
            }
            envelope.validate_restore(&self.identity, &expected_participants)?;

            let restored_window = if self.stmt.window.is_some() {
                let mut runner_window = RunnerWindowStateParticipant::new(None, Vec::new(), None);
                let op_id = runner_window.operator_id().clone();
                let Some(snapshot) = envelope.payloads.get(&op_id) else {
                    return Err(ReliableRuntimeError::Checkpoint(
                        CheckpointError::MissingParticipant(op_id),
                    ));
                };
                runner_window.restore_snapshot(snapshot)?;
                if let Some(ref saved_fp) = runner_window.state.stmt_fingerprint {
                    let expected_fp = self.stmt_fingerprint();
                    if saved_fp != &expected_fp {
                        return Err(ReliableRuntimeError::Checkpoint(
                            CheckpointError::PlanFingerprintMismatch,
                        ));
                    }
                }
                Some((
                    runner_window.state.window_buffer,
                    runner_window.state.tumbling_start_ms,
                ))
            } else {
                None
            };

            for participant in participants.iter_mut() {
                let op_id = participant.operator_id().clone();
                let Some(snapshot) = envelope.payloads.get(&op_id) else {
                    return Err(ReliableRuntimeError::Checkpoint(
                        CheckpointError::MissingParticipant(op_id),
                    ));
                };
                participant.restore_snapshot(snapshot)?;
            }
            if let Some((window_buffer, tumbling_start_ms)) = restored_window {
                self.window_buffer = window_buffer;
                self.tumbling_start_ms = tumbling_start_ms;
            }
            let mut restored_offset = None;
            for pos in &envelope.metadata.source_positions {
                if pos.partition == self.partition {
                    restored_offset = Some(KafkaPositionCodec::decode_next_offset(&pos.cursor)?);
                }
            }
            self.next_checkpoint_id = envelope.metadata.checkpoint_id.0.saturating_add(1);
            self.status.last_checkpoint_id = Some(envelope.metadata.checkpoint_id);
            self.status.last_checkpoint_wall_millis = Some(envelope.metadata.created_wall_millis);
            self.status.restored_next_offset = restored_offset;
            Ok(restored_offset)
        }

        fn stage_output_value(
            assembler: &mut ActionBatchAssembler<Value>,
            pending_batches: &mut Vec<Vec<BatchItem<Value>>>,
            seq: u64,
            val: Value,
            now_elapsed: Duration,
        ) -> Result<(), ReliableRuntimeError> {
            let bytes = serde_json::to_vec(&val).map(|b| b.len()).unwrap_or(0);
            let item = BatchItem::new(seq, bytes, val.clone());
            match assembler.try_push(item, now_elapsed, None) {
                Ok(PushDecision::Buffered) => Ok(()),
                Ok(PushDecision::FlushNow(_)) => {
                    pending_batches.push(assembler.take_batch());
                    Ok(())
                }
                Ok(PushDecision::FlushCurrentFirst(_)) => {
                    pending_batches.push(assembler.take_batch());
                    let retry_item = BatchItem::new(seq, bytes, val);
                    match assembler.try_push(retry_item, now_elapsed, None) {
                        Ok(PushDecision::Buffered) => Ok(()),
                        Ok(PushDecision::FlushNow(_)) => {
                            pending_batches.push(assembler.take_batch());
                            Ok(())
                        }
                        Ok(PushDecision::FlushCurrentFirst(_)) => {
                            Err(ReliableRuntimeError::SinkRejected(
                                "single output record exceeds batch capacity".to_owned(),
                            ))
                        }
                        Err(err) => Err(ReliableRuntimeError::SinkRejected(err.to_string())),
                    }
                }
                Err(err) => Err(ReliableRuntimeError::SinkRejected(err.to_string())),
            }
        }

        fn window_duration_ms(unit: &TimeUnit, length: u64) -> i64 {
            let unit_ms: i64 = match unit {
                TimeUnit::Ms => 1,
                TimeUnit::Ss => 1_000,
                TimeUnit::Mi => 60_000,
                TimeUnit::Hh => 3_600_000,
                TimeUnit::Dd => 86_400_000,
            };
            i64::try_from(length)
                .unwrap_or(i64::MAX)
                .saturating_mul(unit_ms)
                .max(1)
        }

        fn flush_due_tumbling_windows(
            &mut self,
            horizon_ms: i64,
            win_ms: i64,
            assembler: &mut ActionBatchAssembler<Value>,
            pending_batches: &mut Vec<Vec<BatchItem<Value>>>,
            out_seq: &mut u64,
            now_elapsed: Duration,
        ) -> Result<(), ReliableRuntimeError> {
            while let Some(start) = self.tumbling_start_ms {
                let end = start.saturating_add(win_ms);
                if horizon_ms < end {
                    break;
                }
                let mut due_rows = Vec::new();
                let mut remaining = Vec::new();
                for (ts, row) in self.window_buffer.drain(..) {
                    if ts < end {
                        due_rows.push(row);
                    } else {
                        remaining.push((ts, row));
                    }
                }
                self.window_buffer = remaining;
                if !due_rows.is_empty() {
                    for out in Evaluator::eval_window_filtered(&self.stmt, due_rows) {
                        let val = Value::Object(out.into_iter().collect());
                        Self::stage_output_value(
                            assembler,
                            pending_batches,
                            *out_seq,
                            val,
                            now_elapsed,
                        )?;
                        *out_seq = out_seq.saturating_add(1);
                    }
                }
                self.tumbling_start_ms = self
                    .window_buffer
                    .iter()
                    .map(|(ts, _)| *ts)
                    .min()
                    .map(|next_ts| next_ts.div_euclid(win_ms) * win_ms);
            }
            Ok(())
        }

        /// Processes one prepared Kafka message (single or multirecord JSON array),
        /// flushes output through `sink` BEFORE committing the checkpoint, and
        /// advances `reader` only after `store` commit succeeds.
        #[allow(clippy::too_many_arguments)]
        pub async fn process_and_checkpoint<S: ReliableSinkAdapter>(
            &mut self,
            reader: &mut KafkaReliablePartitionReader,
            prepared: &KafkaPreparedMessage,
            participants: &[&dyn StateParticipant],
            sink: &S,
            store: &dyn CheckpointStore,
            clock: &dyn Clock,
            cancellation: &DeadlineCancellation,
        ) -> Result<CheckpointId, ReliableRuntimeError> {
            reader.validate_commit(prepared)?;
            store
                .verify_generation(&self.identity.rule_id, self.identity.generation)
                .await?;
            let records: Vec<StreamRecord> = prepared.decode(reader.tombstone_policy())?;
            let prev_window_buffer = self.window_buffer.clone();
            let prev_tumbling_start_ms = self.tumbling_start_ms;
            let mut assembler = ActionBatchAssembler::new(BatchConfig::legacy(1024, None));
            let mut pending_batches: Vec<Vec<BatchItem<Value>>> = Vec::new();
            let now_snap = clock.now();
            let now_elapsed = now_snap.elapsed;
            let mut out_seq: u64 = 0;

            let eval_res: Result<(), ReliableRuntimeError> = match self.stmt.window.clone() {
                None => {
                    for rec in records {
                        if let Some(projected) = Evaluator::eval_select(&self.stmt, &rec.data) {
                            let val = Value::Object(projected.into_iter().collect());
                            Self::stage_output_value(
                                &mut assembler,
                                &mut pending_batches,
                                out_seq,
                                val,
                                now_elapsed,
                            )?;
                            out_seq = out_seq.saturating_add(1);
                        }
                    }
                    Ok(())
                }
                Some(WindowDef::Count { size, interval }) => {
                    let win_size = size.max(1);
                    let hop = interval.unwrap_or(win_size).max(1);
                    for rec in records {
                        if Evaluator::passes_where(&self.stmt, &rec.data) {
                            self.window_buffer
                                .push((now_snap.wall_millis, rec.data.into_inner()));
                            while self.window_buffer.len() >= win_size {
                                let window_rows: Vec<IndexMap<String, Value>> = self.window_buffer
                                    [..win_size]
                                    .iter()
                                    .map(|(_, r)| r.clone())
                                    .collect();
                                let drain_len = hop.min(self.window_buffer.len());
                                self.window_buffer.drain(..drain_len);
                                for out in Evaluator::eval_window_filtered(&self.stmt, window_rows)
                                {
                                    let val = Value::Object(out.into_iter().collect());
                                    if let Err(err) = Self::stage_output_value(
                                        &mut assembler,
                                        &mut pending_batches,
                                        out_seq,
                                        val,
                                        now_elapsed,
                                    ) {
                                        self.window_buffer = prev_window_buffer;
                                        self.tumbling_start_ms = prev_tumbling_start_ms;
                                        return Err(err);
                                    }
                                    out_seq = out_seq.saturating_add(1);
                                }
                            }
                        }
                    }
                    Ok(())
                }
                Some(WindowDef::TumblingTime { unit, length }) => {
                    let win_ms = Self::window_duration_ms(&unit, length);
                    for rec in records {
                        if Evaluator::passes_where(&self.stmt, &rec.data) {
                            let ev_ts = now_snap.wall_millis;
                            if let Err(err) = self.flush_due_tumbling_windows(
                                ev_ts,
                                win_ms,
                                &mut assembler,
                                &mut pending_batches,
                                &mut out_seq,
                                now_elapsed,
                            ) {
                                self.window_buffer = prev_window_buffer;
                                self.tumbling_start_ms = prev_tumbling_start_ms;
                                return Err(err);
                            }
                            if self.tumbling_start_ms.is_none() {
                                self.tumbling_start_ms = Some(ev_ts.div_euclid(win_ms) * win_ms);
                            }
                            self.window_buffer.push((ev_ts, rec.data.into_inner()));
                        }
                    }
                    self.flush_due_tumbling_windows(
                        now_snap.wall_millis,
                        win_ms,
                        &mut assembler,
                        &mut pending_batches,
                        &mut out_seq,
                        now_elapsed,
                    )
                }
                Some(_) => Err(ReliableRuntimeError::UnsupportedShape(
                    "only tumbling, count, or analytic (windowless) plans are supported",
                )),
            };
            if let Err(err) = eval_res {
                self.window_buffer = prev_window_buffer;
                self.tumbling_start_ms = prev_tumbling_start_ms;
                return Err(err);
            }

            if !assembler.is_empty() {
                pending_batches.push(assembler.take_batch());
            }

            let runner_window_participant = self.stmt.window.as_ref().map(|_| {
                RunnerWindowStateParticipant::new(
                    Some(self.stmt_fingerprint()),
                    self.window_buffer.clone(),
                    self.tumbling_start_ms,
                )
            });
            let mut all_participants: Vec<&dyn StateParticipant> = participants.to_vec();
            if let Some(ref rw) = runner_window_participant {
                if !all_participants
                    .iter()
                    .any(|p| p.operator_id() == rw.operator_id())
                {
                    all_participants.push(rw);
                }
            }

            let cp_id = CheckpointId(self.next_checkpoint_id);
            let source_positions = vec![prepared.position.clone()];
            let retry_policy = self.retry_policy.clone();

            let commit_res = self
                .coordinator
                .run_checkpoint(
                    cp_id,
                    self.identity.generation,
                    &all_participants,
                    source_positions,
                    OutboxBoundary::default(),
                    clock,
                    Duration::from_secs(30),
                    cancellation,
                    || async {
                        for batch in pending_batches {
                            let payloads: Vec<Value> =
                                batch.iter().map(|item| item.payload.clone()).collect();
                            let report = execute_with_retry(
                                &retry_policy,
                                clock,
                                cancellation,
                                None,
                                |_| sink.deliver_batch(&payloads),
                            )
                            .await;
                            match report.outcome {
                                EffectOutcome::Delivered => {}
                                EffectOutcome::PermanentFailure { error } => {
                                    return Err(CoordinatorError::CheckpointFailed(
                                        CheckpointError::StorageFailure(format!(
                                            "sink permanent failure (attempts={}): {error}",
                                            report.attempts
                                        )),
                                    ));
                                }
                                EffectOutcome::Retryable { delay_hint_millis } => {
                                    return Err(CoordinatorError::CheckpointFailed(
                                        CheckpointError::StorageFailure(format!(
                                            "sink retryable failure after {} attempt(s) (delay_hint_ms={delay_hint_millis:?})",
                                            report.attempts
                                        )),
                                    ));
                                }
                                EffectOutcome::Ambiguous { error } => {
                                    return Err(CoordinatorError::CheckpointFailed(
                                        CheckpointError::StorageFailure(format!(
                                            "sink ambiguous failure after {} attempt(s): {error}",
                                            report.attempts
                                        )),
                                    ));
                                }
                            }
                        }
                        Ok(())
                    },
                    store,
                )
                .await;

            match commit_res {
                Ok(envelope) => {
                    reader.commit_message(prepared)?;
                    self.next_checkpoint_id = self.next_checkpoint_id.saturating_add(1);
                    self.status.last_checkpoint_id = Some(envelope.metadata.checkpoint_id);
                    self.status.last_checkpoint_wall_millis =
                        Some(envelope.metadata.created_wall_millis);
                    self.status.last_failure = None;
                    Ok(envelope.metadata.checkpoint_id)
                }
                Err(err) => {
                    self.window_buffer = prev_window_buffer;
                    self.tumbling_start_ms = prev_tumbling_start_ms;
                    let msg = err.to_string();
                    self.status.last_failure = Some(msg);
                    Err(ReliableRuntimeError::Coordinator(err))
                }
            }
        }
    }

    /// Required state participant roles derived from a qualified SQL/DAG plan (P03-T15).
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    pub enum RequiredParticipantKind {
        Analytic,
        TumblingWindow,
        CountWindow,
        HoppingWindow,
        SlidingWindow,
        SessionWindow,
        StateWindow,
        JoinBuffer,
        CepSequence,
        BarrierAlignment,
    }

    impl RequiredParticipantKind {
        pub fn label(self) -> &'static str {
            match self {
                Self::Analytic => "analytic",
                Self::TumblingWindow => "tumbling",
                Self::CountWindow => "count",
                Self::HoppingWindow => "hopping",
                Self::SlidingWindow => "sliding",
                Self::SessionWindow => "session",
                Self::StateWindow => "state",
                Self::JoinBuffer => "join",
                Self::CepSequence => "cep",
                Self::BarrierAlignment => "barrier",
            }
        }

        fn matches_participant(self, participant: &dyn StateParticipant) -> bool {
            let label = self.label();
            if let Some(tag) = participant.participant_kind_tag() {
                return tag == label;
            }
            let id = participant.operator_id().as_str().to_ascii_lowercase();
            id == label
                || id.starts_with(&format!("{label}:"))
                || id.starts_with(&format!("{label}_"))
                || id.starts_with(&format!("{label}-"))
        }
    }

    fn expr_uses_lead_or_lag(expr: &rekuiper_sql::Expr) -> bool {
        use rekuiper_sql::Expr;
        match expr {
            Expr::Call { name, args } => {
                name.eq_ignore_ascii_case("lead")
                    || name.eq_ignore_ascii_case("lag")
                    || args.iter().any(expr_uses_lead_or_lag)
            }
            Expr::Over {
                call,
                partition_by,
                when,
            } => {
                expr_uses_lead_or_lag(call)
                    || partition_by.as_deref().is_some_and(expr_uses_lead_or_lag)
                    || when.as_deref().is_some_and(expr_uses_lead_or_lag)
            }
            Expr::BinaryOp { left, right, .. } => {
                expr_uses_lead_or_lag(left) || expr_uses_lead_or_lag(right)
            }
            Expr::UnaryOp { expr: inner, .. } | Expr::IsNull { expr: inner, .. } => {
                expr_uses_lead_or_lag(inner)
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                expr_uses_lead_or_lag(expr)
                    || expr_uses_lead_or_lag(low)
                    || expr_uses_lead_or_lag(high)
            }
            Expr::InList { expr, list, .. } => {
                expr_uses_lead_or_lag(expr) || list.iter().any(expr_uses_lead_or_lag)
            }
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                operand.as_deref().is_some_and(expr_uses_lead_or_lag)
                    || when_clauses
                        .iter()
                        .any(|(w, t)| expr_uses_lead_or_lag(w) || expr_uses_lead_or_lag(t))
                    || else_clause.as_deref().is_some_and(expr_uses_lead_or_lag)
            }
            Expr::FieldAccess { parent, .. } => expr_uses_lead_or_lag(parent),
            Expr::Index { base, index } => {
                expr_uses_lead_or_lag(base) || expr_uses_lead_or_lag(index)
            }
            Expr::Slice { base, lo, hi } => {
                expr_uses_lead_or_lag(base)
                    || lo.as_deref().is_some_and(expr_uses_lead_or_lag)
                    || hi.as_deref().is_some_and(expr_uses_lead_or_lag)
            }
            Expr::WildcardModified { replace, .. } => {
                replace.iter().any(|(e, _)| expr_uses_lead_or_lag(e))
            }
            Expr::Wildcard | Expr::Identifier(_) | Expr::Literal(_) => false,
        }
    }

    /// Returns true if any SQL clause invokes `LEAD` or `LAG` (deferred until P04-T06).
    pub fn stmt_uses_lead_or_lag(stmt: &SelectStmt) -> bool {
        stmt.fields.iter().any(expr_uses_lead_or_lag)
            || stmt
                .where_clause
                .as_ref()
                .is_some_and(expr_uses_lead_or_lag)
            || stmt.group_by.iter().any(expr_uses_lead_or_lag)
            || stmt.having.as_ref().is_some_and(expr_uses_lead_or_lag)
    }

    /// Computes required qualified state participants for a plan, rejecting `LEAD`/`LAG`
    /// and unqualified set-op shapes.
    pub fn required_participants_for_plan(
        stmt: &SelectStmt,
        has_cep: bool,
        input_edge_count: usize,
    ) -> Result<Vec<RequiredParticipantKind>, ReliableRuntimeError> {
        if stmt.set_op.is_some() {
            return Err(ReliableRuntimeError::UnsupportedShape(
                "UNION/set-op plans are not supported in qualified reliable mode",
            ));
        }
        if stmt_uses_lead_or_lag(stmt) {
            return Err(ReliableRuntimeError::UnsupportedShape(
                "LEAD/LAG state adapter is not yet integrated (pending P04-T06)",
            ));
        }

        let mut req = Vec::new();
        match crate::engine::windows::classify_window_participant(stmt.window.as_ref()) {
            crate::engine::windows::QualifiedWindowKind::None => {
                if !has_cep && stmt.joins.is_empty() {
                    req.push(RequiredParticipantKind::Analytic);
                }
            }
            crate::engine::windows::QualifiedWindowKind::Tumbling => {
                req.push(RequiredParticipantKind::TumblingWindow)
            }
            crate::engine::windows::QualifiedWindowKind::Count => {
                req.push(RequiredParticipantKind::CountWindow)
            }
            crate::engine::windows::QualifiedWindowKind::Hopping => {
                req.push(RequiredParticipantKind::HoppingWindow)
            }
            crate::engine::windows::QualifiedWindowKind::Sliding => {
                req.push(RequiredParticipantKind::SlidingWindow)
            }
            crate::engine::windows::QualifiedWindowKind::Session => {
                req.push(RequiredParticipantKind::SessionWindow)
            }
            crate::engine::windows::QualifiedWindowKind::State => {
                req.push(RequiredParticipantKind::StateWindow)
            }
        }
        if !stmt.joins.is_empty() {
            req.push(RequiredParticipantKind::JoinBuffer);
        }
        if has_cep {
            req.push(RequiredParticipantKind::CepSequence);
        }
        if input_edge_count > 1 || !stmt.joins.is_empty() {
            req.push(RequiredParticipantKind::BarrierAlignment);
        }
        Ok(req)
    }

    /// Ensures every required state participant role is covered by the registered participants.
    pub fn validate_participant_coverage(
        required: &[RequiredParticipantKind],
        participants: &[&dyn StateParticipant],
    ) -> Result<(), ReliableRuntimeError> {
        for kind in required {
            let covered = participants.iter().any(|p| kind.matches_participant(*p));
            if !covered {
                return Err(ReliableRuntimeError::MissingParticipant(kind.label()));
            }
        }
        Ok(())
    }

    /// Coordinates restore-before-intake and barrier-complete checkpoint commits across all
    /// qualified window, join, CEP, and multi-input alignment participants (P03-T15).
    pub struct QualifiedPlanRunner {
        identity: PlanIdentity,
        required_participants: Vec<RequiredParticipantKind>,
        expected_actions: std::collections::BTreeSet<String>,
        coordinator: CheckpointCoordinator,
        next_checkpoint_id: u64,
        status: ReliableCheckpointStatus,
    }

    impl QualifiedPlanRunner {
        pub fn new(
            identity: PlanIdentity,
            stmt: &SelectStmt,
            has_cep: bool,
            input_edge_count: usize,
            expected_actions: std::collections::BTreeSet<String>,
            participants: &[&dyn StateParticipant],
        ) -> Result<Self, ReliableRuntimeError> {
            let required_participants =
                required_participants_for_plan(stmt, has_cep, input_edge_count)?;
            validate_participant_coverage(&required_participants, participants)?;
            let coordinator = CheckpointCoordinator::new(identity.clone());
            Ok(Self {
                identity,
                required_participants,
                expected_actions,
                coordinator,
                next_checkpoint_id: 1,
                status: ReliableCheckpointStatus::new(ReliableDeliveryGuarantee::AtLeastOnce),
            })
        }

        pub fn required_participants(&self) -> &[RequiredParticipantKind] {
            &self.required_participants
        }

        pub fn status(&self) -> &ReliableCheckpointStatus {
            &self.status
        }

        pub async fn restore_before_intake(
            &mut self,
            store: &dyn CheckpointStore,
            participants: &mut [&mut dyn StateParticipant],
        ) -> Result<Vec<rekuiper_core::contracts::SourcePosition>, ReliableRuntimeError> {
            let refs: Vec<&dyn StateParticipant> = participants.iter().map(|p| &**p).collect();
            validate_participant_coverage(&self.required_participants, &refs)?;
            store
                .verify_generation(&self.identity.rule_id, self.identity.generation)
                .await?;
            let Some(envelope) = store.load_latest(&self.identity.rule_id).await? else {
                return Ok(Vec::new());
            };
            let expected_participants: std::collections::BTreeSet<OperatorId> = participants
                .iter()
                .map(|p| p.operator_id().clone())
                .collect();
            envelope.validate_restore(&self.identity, &expected_participants)?;
            for participant in participants.iter_mut() {
                let op_id = participant.operator_id().clone();
                let Some(snapshot) = envelope.payloads.get(&op_id) else {
                    return Err(ReliableRuntimeError::Checkpoint(
                        CheckpointError::MissingParticipant(op_id),
                    ));
                };
                participant.restore_snapshot(snapshot)?;
            }
            self.next_checkpoint_id = envelope.metadata.checkpoint_id.0.saturating_add(1);
            self.status.last_checkpoint_id = Some(envelope.metadata.checkpoint_id);
            self.status.last_checkpoint_wall_millis = Some(envelope.metadata.created_wall_millis);
            Ok(envelope.metadata.source_positions)
        }

        pub async fn commit_checkpoint(
            &mut self,
            store: &dyn CheckpointStore,
            participants: &[&dyn StateParticipant],
            source_positions: Vec<rekuiper_core::contracts::SourcePosition>,
            acked_actions: &std::collections::BTreeSet<String>,
            clock: &dyn Clock,
            cancellation: &DeadlineCancellation,
        ) -> Result<CheckpointId, ReliableRuntimeError> {
            validate_participant_coverage(&self.required_participants, participants)?;
            store
                .verify_generation(&self.identity.rule_id, self.identity.generation)
                .await?;
            let missing_actions: Vec<String> = self
                .expected_actions
                .difference(acked_actions)
                .cloned()
                .collect();
            if !missing_actions.is_empty() {
                return Err(ReliableRuntimeError::IncompleteBarrierActions(
                    missing_actions.join(", "),
                ));
            }
            let cp_id = CheckpointId(self.next_checkpoint_id);
            let envelope = self
                .coordinator
                .run_checkpoint(
                    cp_id,
                    self.identity.generation,
                    participants,
                    source_positions,
                    OutboxBoundary::default(),
                    clock,
                    Duration::from_secs(30),
                    cancellation,
                    || async { Ok(()) },
                    store,
                )
                .await?;
            self.next_checkpoint_id = self.next_checkpoint_id.saturating_add(1);
            self.status.last_checkpoint_id = Some(envelope.metadata.checkpoint_id);
            self.status.last_checkpoint_wall_millis = Some(envelope.metadata.created_wall_millis);
            self.status.last_failure = None;
            Ok(envelope.metadata.checkpoint_id)
        }
    }
}
