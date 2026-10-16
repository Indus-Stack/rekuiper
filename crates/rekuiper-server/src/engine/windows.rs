use crate::engine::joins::{eval_window_join_batch, TaggedRow};
use crate::engine::runtime::{
    extract_event_timestamp, handle_error_record, is_rule_running, EventTimeConfig,
};
use crate::engine::sinks::{enqueue_sink_record, BATCH_ROWS_KEY};
use parking_lot::RwLock;
use rekuiper_core::model::{RuleStatus, StreamRecord};
use rekuiper_core::{RuleCounters, StreamReceiver, TableManager};
use rekuiper_sql::{Evaluator, Expr, IncrementalWindow, RuleState, SelectStmt, TimeUnit};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[allow(clippy::too_many_arguments)]
pub(crate) async fn emit_window_batch(
    counters: &RuleCounters,
    table_manager: &TableManager,
    source_configs: &Arc<RwLock<HashMap<String, Value>>>,
    select_stmt: &SelectStmt,
    mut batch: Vec<TaggedRow>,
    prefiltered: bool,
    window_bounds: Option<(i64, i64)>,
    sink: &tokio::sync::mpsc::Sender<StreamRecord>,
    rule_state: Option<&RuleState>,
) {
    if batch.is_empty() {
        return;
    }
    if let Some((start_ms, end_ms)) = window_bounds {
        for row in &mut batch {
            row.data
                .insert("__window_start__".to_string(), Value::from(start_ms));
            row.data
                .insert("__window_end__".to_string(), Value::from(end_ms));
        }
    }
    let eval_start = std::time::Instant::now();
    let outputs = if select_stmt.joins.is_empty() {
        let rows: Vec<HashMap<String, Value>> =
            batch.into_iter().map(|r| r.data.into_inner()).collect();
        if prefiltered {
            Evaluator::eval_window_filtered_stateful(select_stmt, rows, rule_state)
        } else {
            Evaluator::eval_window_stateful(select_stmt, rows, rule_state)
        }
    } else {
        eval_window_join_batch(
            table_manager,
            source_configs,
            select_stmt,
            &batch,
            rule_state,
        )
        .await
    };
    counters
        .op_latency
        .observe(eval_start.elapsed().as_micros() as u64);
    emit_window_outputs(counters, sink, outputs).await;
}

/// Enqueue a window trigger's output rows in order (stops if the sink closed).
pub(crate) async fn emit_window_outputs(
    counters: &RuleCounters,
    sink: &tokio::sync::mpsc::Sender<StreamRecord>,
    outputs: Vec<HashMap<String, Value>>,
) {
    if outputs.is_empty() {
        return;
    }
    if outputs.len() == 1 {
        let _ = enqueue_sink_record(
            counters,
            sink,
            StreamRecord::new(outputs.into_iter().next().unwrap()),
        )
        .await;
    } else {
        let mut map = HashMap::new();
        let arr: Vec<Value> = outputs
            .into_iter()
            .map(|m| serde_json::to_value(m).unwrap_or(Value::Null))
            .collect();
        map.insert(BATCH_ROWS_KEY.to_string(), Value::Array(arr));
        let _ = enqueue_sink_record(counters, sink, StreamRecord::new(map)).await;
    }
}

/// Time-window ingest filter: without joins, `WHERE` is pushed below the
/// window (as eKuiper's predicate push-down does), so rejected rows are
/// never buffered. With joins it may reference joined columns and runs at
/// trigger time instead. FILTER (WHERE ...) is always evaluated before the window.
pub(crate) fn window_ingest_passes(
    select_stmt: &SelectStmt,
    data: &HashMap<String, Value>,
) -> bool {
    let where_pass = !select_stmt.joins.is_empty() || Evaluator::passes_where(select_stmt, data);
    if !where_pass {
        return false;
    }
    if let Some(filter) = &select_stmt.window_filter {
        return Evaluator::eval_bool(filter, data);
    }
    true
}

/// Contents of one open time window: row-free accumulators when the
/// projection allows it, otherwise the buffered rows.
pub(crate) enum WindowRows {
    Incremental(Box<IncrementalWindow>),
    Buffered(Vec<TaggedRow>),
}

impl WindowRows {
    fn new(select_stmt: &SelectStmt) -> Self {
        match IncrementalWindow::try_new(select_stmt) {
            Some(inc) => WindowRows::Incremental(Box::new(inc)),
            None => WindowRows::Buffered(Vec::new()),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            WindowRows::Incremental(inc) => inc.is_empty(),
            WindowRows::Buffered(rows) => rows.is_empty(),
        }
    }

    /// Adds one row; returns `false` when `WHERE` rejected it.
    fn push(&mut self, select_stmt: &SelectStmt, tagged: TaggedRow) -> bool {
        match self {
            WindowRows::Incremental(inc) => inc.push(&tagged.data),
            WindowRows::Buffered(rows) => {
                if !window_ingest_passes(select_stmt, &tagged.data) {
                    return false;
                }
                rows.push(tagged);
                true
            }
        }
    }

    /// Closes the window and emits its output rows.
    #[allow(clippy::too_many_arguments)]
    async fn emit(
        &mut self,
        counters: &RuleCounters,
        table_manager: &TableManager,
        source_configs: &Arc<RwLock<HashMap<String, Value>>>,
        select_stmt: &SelectStmt,
        window_bounds: Option<(i64, i64)>,
        sink: &tokio::sync::mpsc::Sender<StreamRecord>,
        rule_state: Option<&RuleState>,
    ) {
        match self {
            WindowRows::Incremental(inc) => {
                if !inc.is_empty() {
                    let eval_start = std::time::Instant::now();
                    let outputs = inc.take();
                    counters
                        .op_latency
                        .observe(eval_start.elapsed().as_micros() as u64);
                    emit_window_outputs(counters, sink, outputs).await;
                }
            }
            WindowRows::Buffered(rows) => {
                let batch = std::mem::take(rows);
                emit_window_batch(
                    counters,
                    table_manager,
                    source_configs,
                    select_stmt,
                    batch,
                    true,
                    window_bounds,
                    sink,
                    rule_state,
                )
                .await;
            }
        }
    }
}

/// Interval whose ticks fall on natural-time multiples of `period` (a 10 s
/// window ends at :00, :10, :20 regardless of rule start, as in eKuiper).
pub(crate) fn aligned_interval(period: std::time::Duration) -> tokio::time::Interval {
    let period_ms = (period.as_millis() as i64).max(1);
    let now_ms = chrono::Utc::now().timestamp_millis();
    let wait_ms = period_ms - now_ms.rem_euclid(period_ms);
    let start = tokio::time::Instant::now() + std::time::Duration::from_millis(wait_ms as u64);
    let mut interval = tokio::time::interval_at(start, period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval
}

/// Spawn forwarders that tag rows from joined streams and feed the runner's
/// local channel. Forwarders exit when the runner drops the channel or the
/// source bus closes.
pub(crate) fn spawn_join_forwarders(
    join_rxs: Vec<(String, StreamReceiver)>,
) -> tokio::sync::mpsc::Receiver<TaggedRow> {
    let (join_tx, join_rx) = tokio::sync::mpsc::channel::<TaggedRow>(1024);
    for (source, mut jrx) in join_rxs {
        let jtx = join_tx.clone();
        tokio::spawn(async move {
            while let Some(record) = jrx.recv().await {
                let tagged = TaggedRow {
                    source: source.clone(),
                    data: record.data,
                };
                if jtx.send(tagged).await.is_err() {
                    break;
                }
            }
        });
    }
    drop(join_tx);
    join_rx
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_count_window_rule(
    counters: Arc<RuleCounters>,
    running: Arc<RwLock<RuleStatus>>,
    rule_id: String,
    select_stmt: SelectStmt,
    mut rx: StreamReceiver,
    size: usize,
    interval: Option<usize>,
    sink: tokio::sync::mpsc::Sender<StreamRecord>,
    send_error: bool,
    join_rxs: Vec<(String, StreamReceiver)>,
    table_manager: TableManager,
    source_configs: Arc<RwLock<HashMap<String, Value>>>,
    dlq_router: crate::engine::dlq::DlqRouter,
) {
    let count = size.max(1);
    let hop = interval.unwrap_or(count).max(1);
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
    let mut buffer: Vec<TaggedRow> = Vec::new();
    let mut events_since_trigger: usize = 0;
    let from_source = select_stmt.from.clone();
    let mut join_rx = spawn_join_forwarders(join_rxs);
    // When every join forwarder has exited, the merge channel closes; the
    // rule keeps serving its FROM stream afterwards.
    let mut joins_open = true;
    loop {
        tokio::select! {
            res = rx.recv() => {
                match res {
            Some(record) => {
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
                if !window_ingest_passes(&select_stmt, &record.data) {
                    continue;
                }
                buffer.push(TaggedRow { source: from_source.clone(), data: record.data });
                events_since_trigger += 1;
                if hop <= count {
                    // Standard count window (tumbling when hop == count, overlapping when hop < count)
                    if buffer.len() >= count {
                        let batch: Vec<TaggedRow> = if hop == count {
                            buffer.drain(0..count).collect()
                        } else {
                            // Overlapping: discard only the oldest `hop` records.
                            let batch = buffer[0..count].to_vec();
                            buffer.drain(0..hop.min(buffer.len()));
                            batch
                        };
                        let now_ms = chrono::Utc::now().timestamp_millis();
                        emit_window_batch(&counters, &table_manager, &source_configs, &select_stmt, batch, false, Some((now_ms, now_ms)), &sink, Some(&rule_state)).await;
                    }
                } else {
                    // Sparsely sampled count window with gap (hop > count)
                    if buffer.len() > count {
                        buffer.remove(0);
                    }
                    if events_since_trigger >= hop {
                        if !buffer.is_empty() {
                            let now_ms = chrono::Utc::now().timestamp_millis();
                            emit_window_batch(&counters, &table_manager, &source_configs, &select_stmt, std::mem::take(&mut buffer), false, Some((now_ms, now_ms)), &sink, Some(&rule_state)).await;
                        }
                        events_since_trigger = 0;
                        buffer.clear();
                    }
                }
            }
            None => break,
                }
            }
            jrec = join_rx.recv(), if joins_open => {
                match jrec {
                    Some(tagged) => {
                        if !is_rule_running(&running) {
                            continue;
                        }
                        counters.inc_source(1);
                        let probe = StreamRecord::from_shared(tagged.data.clone());
                        if handle_error_record(&counters, &rule_id, send_error, &sink, &probe, Some(&dlq_router)).await {
                            if dlq_router.on_error == crate::engine::dlq::OnErrorPolicy::Halt {
                                break;
                            }
                            continue;
                        }
                        if !window_ingest_passes(&select_stmt, &tagged.data) {
                            continue;
                        }
                        buffer.push(tagged);
                        events_since_trigger += 1;
                        if hop <= count {
                            if buffer.len() >= count {
                                let batch: Vec<TaggedRow> = if hop == count {
                                    buffer.drain(0..count).collect()
                                } else {
                                    let batch = buffer[0..count].to_vec();
                                    buffer.drain(0..hop.min(buffer.len()));
                                    batch
                                };
                                let now_ms = chrono::Utc::now().timestamp_millis();
                                emit_window_batch(&counters, &table_manager, &source_configs, &select_stmt, batch, false, Some((now_ms, now_ms)), &sink, Some(&rule_state)).await;
                            }
                        } else if events_since_trigger >= hop {
                            if buffer.len() > count {
                                buffer.remove(0);
                            }
                            if !buffer.is_empty() {
                                let now_ms = chrono::Utc::now().timestamp_millis();
                                emit_window_batch(&counters, &table_manager, &source_configs, &select_stmt, std::mem::take(&mut buffer), false, Some((now_ms, now_ms)), &sink, Some(&rule_state)).await;
                            }
                            events_since_trigger = 0;
                            buffer.clear();
                        }
                    }
                    None => {
                        joins_open = false;
                    }
                }
            }
        }
    }
}

pub(crate) fn tumbling_window_duration(unit: &TimeUnit, length: u64) -> std::time::Duration {
    let millis: u128 = match unit {
        TimeUnit::Ms => length as u128,
        TimeUnit::Ss => length as u128 * 1_000,
        TimeUnit::Mi => length as u128 * 60_000,
        TimeUnit::Hh => length as u128 * 3_600_000,
        TimeUnit::Dd => length as u128 * 86_400_000,
    };
    let millis = millis.min(u64::MAX as u128) as u64;
    let duration = std::time::Duration::from_millis(millis);
    if duration.is_zero() {
        // tokio::time::interval panics on zero durations; clamp degenerate windows.
        std::time::Duration::from_millis(1)
    } else {
        duration
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_tumbling_window_rule(
    counters: Arc<RuleCounters>,
    running: Arc<RwLock<RuleStatus>>,
    rule_id: String,
    select_stmt: SelectStmt,
    mut rx: StreamReceiver,
    duration: std::time::Duration,
    sink: tokio::sync::mpsc::Sender<StreamRecord>,
    event_time: EventTimeConfig,
    send_error: bool,
    join_rxs: Vec<(String, StreamReceiver)>,
    table_manager: TableManager,
    source_configs: Arc<RwLock<HashMap<String, Value>>>,
    dlq_router: crate::engine::dlq::DlqRouter,
) {
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
    let mut ticker = aligned_interval(duration);
    // Processing-time window contents (row-free when the projection allows).
    let mut window = WindowRows::new(&select_stmt);
    // Event-time state: event-timestamped rows, the watermark, and the start
    // of the currently open event-time window (aligned to its length).
    let mut et_buffer: Vec<(i64, TaggedRow)> = Vec::new();
    let mut watermark: i64 = i64::MIN;
    let mut window_start: Option<i64> = None;
    let window_millis = duration.as_millis() as i64;
    let from_source = select_stmt.from.clone();
    let mut input_streams: Vec<String> = Vec::new();
    if table_manager.get_table(&from_source).is_none() {
        input_streams.push(from_source.clone());
    }
    let mut join_stream_names: Vec<String> = Vec::new();
    for (name, _) in &join_rxs {
        if !input_streams.contains(name) {
            input_streams.push(name.clone());
        }
        if !join_stream_names.contains(name) {
            join_stream_names.push(name.clone());
        }
    }
    let mut join_rx = spawn_join_forwarders(join_rxs);
    let mut joins_open = true;
    let mut stream_max_ts: HashMap<String, i64> = HashMap::new();
    let mut closed_streams: HashSet<String> = HashSet::new();
    // Ingest one row (FROM or joined stream) into the wall/event buffers.
    macro_rules! ingest {
        ($tagged:expr) => {{
            let tagged: TaggedRow = $tagged;
            if event_time.enabled {
                let ts_field = event_time
                    .source_timestamp_fields
                    .get(&tagged.source)
                    .map(|s| s.as_str())
                    .or(event_time.timestamp_field.as_deref());
                let event_ts = extract_event_timestamp(&tagged.data, ts_field);
                if event_ts < watermark {
                    // Late arrival beyond the tolerance horizon: drop.
                } else {
                    let cur_max = stream_max_ts
                        .entry(tagged.source.clone())
                        .or_insert(event_ts);
                    *cur_max = (*cur_max).max(event_ts);

                    let all_active_seen = input_streams
                        .iter()
                        .filter(|s| !closed_streams.contains(*s))
                        .all(|s| stream_max_ts.contains_key(s));

                    if all_active_seen {
                        let min_source_ts = input_streams
                            .iter()
                            .filter(|s| !closed_streams.contains(*s))
                            .filter_map(|s| stream_max_ts.get(s))
                            .min()
                            .copied()
                            .unwrap_or(event_ts);
                        let new_wm = min_source_ts.saturating_sub(event_time.late_tolerance_ms);
                        watermark = watermark.max(new_wm);
                    }

                    let aligned = event_ts - event_ts.rem_euclid(window_millis.max(1));
                    if window_start.is_none() {
                        window_start = Some(aligned);
                    } else if let Some(ws) = window_start {
                        if aligned < ws {
                            window_start = Some(aligned);
                        }
                    }
                    // Rows rejected by WHERE still advance the watermark.
                    if window_ingest_passes(&select_stmt, &tagged.data) {
                        et_buffer.push((event_ts, tagged));
                    }
                    // Close every window the watermark has passed.
                    while let Some(t0) = window_start {
                        let t_end = t0.saturating_add(window_millis);
                        if watermark < t_end {
                            break;
                        }
                        let (closed, open): (Vec<(i64, TaggedRow)>, Vec<(i64, TaggedRow)>) =
                            std::mem::take(&mut et_buffer)
                                .into_iter()
                                .partition(|(ts, _)| *ts < t_end);
                        et_buffer = open;
                        let batch: Vec<TaggedRow> = closed
                            .into_iter()
                            .filter(|(ts, _)| *ts >= t0)
                            .map(|(_, row)| row)
                            .collect();
                        window_start = Some(t_end);
                        emit_window_batch(
                            &counters,
                            &table_manager,
                            &source_configs,
                            &select_stmt,
                            batch,
                            true,
                            Some((t0, t_end)),
                            &sink,
                            Some(&rule_state),
                        )
                        .await;
                    }
                }
            } else {
                window.push(&select_stmt, tagged);
            }
        }};
    }
    loop {
        tokio::select! {
            res = rx.recv() => {
                match res {
                    Some(record) => {
                        if !is_rule_running(&running) {
                            continue;
                        }
                        counters.inc_source(1);
                        if handle_error_record(&counters, &rule_id, send_error, &sink, &record, Some(&dlq_router))
                            .await
                        {
                            if dlq_router.on_error == crate::engine::dlq::OnErrorPolicy::Halt {
                                break;
                            }
                            continue;
                        }
                        ingest!(TaggedRow { source: from_source.clone(), data: record.data });
                    }
                    None => break,
                }
            }
            jrec = join_rx.recv(), if joins_open => {
                match jrec {
                    Some(tagged) => {
                        if !is_rule_running(&running) {
                            continue;
                        }
                        counters.inc_source(1);
                        let probe = StreamRecord::from_shared(tagged.data.clone());
                        if handle_error_record(&counters, &rule_id, send_error, &sink, &probe, Some(&dlq_router)).await {
                            if dlq_router.on_error == crate::engine::dlq::OnErrorPolicy::Halt {
                                break;
                            }
                            continue;
                        }
                        ingest!(tagged);
                    }
                    None => {
                        joins_open = false;
                        for s in &join_stream_names {
                            closed_streams.insert(s.clone());
                        }
                    }
                }
            }
            _ = ticker.tick() => {
                if event_time.enabled {
                    // Windows close on watermark advance, never on the clock.
                    continue;
                }
                if window.is_empty() {
                    continue;
                }
                let now_ms = chrono::Utc::now().timestamp_millis();
                let end_ms = now_ms - now_ms.rem_euclid(window_millis.max(1));
                let start_ms = end_ms - window_millis;
                window.emit(&counters, &table_manager, &source_configs, &select_stmt, Some((start_ms, end_ms)), &sink, Some(&rule_state)).await;
            }
        }
    }
}

/// SESSIONWINDOW: in event time, groups events within consecutive gaps <= timeout,
/// cutting on max_duration or gap > timeout. In processing time, operates on idle timeout
/// and aligned max_duration intervals.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_session_window_rule(
    counters: Arc<RuleCounters>,
    running: Arc<RwLock<RuleStatus>>,
    rule_id: String,
    select_stmt: SelectStmt,
    mut rx: StreamReceiver,
    max_duration: std::time::Duration,
    timeout: std::time::Duration,
    sink: tokio::sync::mpsc::Sender<StreamRecord>,
    event_time: EventTimeConfig,
    send_error: bool,
    join_rxs: Vec<(String, StreamReceiver)>,
    table_manager: TableManager,
    source_configs: Arc<RwLock<HashMap<String, Value>>>,
    dlq_router: crate::engine::dlq::DlqRouter,
) {
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
    let mut max_check = aligned_interval(max_duration);
    let mut window = WindowRows::new(&select_stmt);
    let mut opened_at: Option<tokio::time::Instant> = None;
    let mut opened_at_ms: Option<i64> = None;
    let idle = tokio::time::sleep(timeout);
    tokio::pin!(idle);
    let from_source = select_stmt.from.clone();

    let mut input_streams: Vec<String> = Vec::new();
    if table_manager.get_table(&from_source).is_none() {
        input_streams.push(from_source.clone());
    }
    let mut join_stream_names: Vec<String> = Vec::new();
    for (name, _) in &join_rxs {
        if !input_streams.contains(name) {
            input_streams.push(name.clone());
        }
        if !join_stream_names.contains(name) {
            join_stream_names.push(name.clone());
        }
    }
    let mut join_rx = spawn_join_forwarders(join_rxs);
    let mut joins_open = true;
    let mut stream_max_ts: HashMap<String, i64> = HashMap::new();
    let mut closed_streams: HashSet<String> = HashSet::new();
    let mut watermark: i64 = i64::MIN;
    let mut session_buffer: Vec<(i64, TaggedRow)> = Vec::new();
    let mut session_start: Option<i64> = None;
    let mut last_event_ts: Option<i64> = None;
    let timeout_ms = (timeout.as_millis() as i64).max(1);
    let max_duration_ms = (max_duration.as_millis() as i64).max(1);

    macro_rules! ingest_session {
        ($tagged:expr) => {{
            let tagged: TaggedRow = $tagged;
            if event_time.enabled {
                let ts_field = event_time
                    .source_timestamp_fields
                    .get(&tagged.source)
                    .map(|s| s.as_str())
                    .or(event_time.timestamp_field.as_deref());
                let event_ts = extract_event_timestamp(&tagged.data, ts_field);
                if event_ts < watermark {
                    // Drop late arrival
                } else {
                    let cur_max = stream_max_ts
                        .entry(tagged.source.clone())
                        .or_insert(event_ts);
                    *cur_max = (*cur_max).max(event_ts);

                    let all_active_seen = input_streams
                        .iter()
                        .filter(|s| !closed_streams.contains(*s))
                        .all(|s| stream_max_ts.contains_key(s));

                    if all_active_seen {
                        let min_source_ts = input_streams
                            .iter()
                            .filter(|s| !closed_streams.contains(*s))
                            .filter_map(|s| stream_max_ts.get(s))
                            .min()
                            .copied()
                            .unwrap_or(event_ts);
                        let new_wm = min_source_ts.saturating_sub(event_time.late_tolerance_ms);
                        watermark = watermark.max(new_wm);
                    }

                    if window_ingest_passes(&select_stmt, &tagged.data) {
                        if let Some(last_ts) = last_event_ts {
                            if event_ts.saturating_sub(last_ts) > timeout_ms
                                || event_ts.saturating_sub(session_start.unwrap_or(event_ts))
                                    >= max_duration_ms
                            {
                                let s_start = session_start.unwrap_or(last_ts);
                                let s_end = last_ts.saturating_add(timeout_ms);
                                let batch: Vec<TaggedRow> = std::mem::take(&mut session_buffer)
                                    .into_iter()
                                    .map(|(_, r)| r)
                                    .collect();
                                session_start = Some(event_ts);
                                last_event_ts = Some(event_ts);
                                session_buffer.push((event_ts, tagged));
                                emit_window_batch(
                                    &counters,
                                    &table_manager,
                                    &source_configs,
                                    &select_stmt,
                                    batch,
                                    true,
                                    Some((s_start, s_end)),
                                    &sink,
                                    Some(&rule_state),
                                )
                                .await;
                            } else {
                                last_event_ts = Some(event_ts);
                                session_buffer.push((event_ts, tagged));
                            }
                        } else {
                            session_start = Some(event_ts);
                            last_event_ts = Some(event_ts);
                            session_buffer.push((event_ts, tagged));
                        }
                    }

                    if let Some(last_ts) = last_event_ts {
                        if watermark >= last_ts.saturating_add(timeout_ms) {
                            let s_start = session_start.take().unwrap_or(last_ts);
                            let s_end = last_ts.saturating_add(timeout_ms);
                            last_event_ts = None;
                            let batch: Vec<TaggedRow> = std::mem::take(&mut session_buffer)
                                .into_iter()
                                .map(|(_, r)| r)
                                .collect();
                            emit_window_batch(
                                &counters,
                                &table_manager,
                                &source_configs,
                                &select_stmt,
                                batch,
                                true,
                                Some((s_start, s_end)),
                                &sink,
                                Some(&rule_state),
                            )
                            .await;
                        }
                    }
                }
            } else if window.push(&select_stmt, tagged) {
                let now = tokio::time::Instant::now();
                if opened_at.is_none() {
                    opened_at = Some(now);
                    opened_at_ms = Some(chrono::Utc::now().timestamp_millis());
                }
                idle.as_mut().reset(now + timeout);
            }
        }};
    }

    loop {
        tokio::select! {
            res = rx.recv() => {
                match res {
                    Some(record) => {
                        if !is_rule_running(&running) {
                            continue;
                        }
                        counters.inc_source(1);
                        if handle_error_record(&counters, &rule_id, send_error, &sink, &record, Some(&dlq_router))
                            .await
                        {
                            if dlq_router.on_error == crate::engine::dlq::OnErrorPolicy::Halt {
                                break;
                            }
                            continue;
                        }
                        ingest_session!(TaggedRow { source: from_source.clone(), data: record.data });
                    }
                    None => break,
                }
            }
            jrec = join_rx.recv(), if joins_open => {
                match jrec {
                    Some(tagged) => {
                        if !is_rule_running(&running) {
                            continue;
                        }
                        counters.inc_source(1);
                        let probe = StreamRecord::from_shared(tagged.data.clone());
                        if handle_error_record(&counters, &rule_id, send_error, &sink, &probe, Some(&dlq_router)).await {
                            if dlq_router.on_error == crate::engine::dlq::OnErrorPolicy::Halt {
                                break;
                            }
                            continue;
                        }
                        ingest_session!(tagged);
                    }
                    None => {
                        joins_open = false;
                        for s in &join_stream_names {
                            closed_streams.insert(s.clone());
                        }
                    }
                }
            }
            _ = &mut idle, if opened_at.is_some() && !event_time.enabled => {
                opened_at = None;
                let now_ms = chrono::Utc::now().timestamp_millis();
                let start_ms = opened_at_ms.take().unwrap_or(now_ms);
                window.emit(&counters, &table_manager, &source_configs, &select_stmt, Some((start_ms, now_ms)), &sink, Some(&rule_state)).await;
            }
            _ = max_check.tick() => {
                if !event_time.enabled && opened_at.is_some_and(|start| start.elapsed() >= max_duration) {
                    opened_at = None;
                    let now_ms = chrono::Utc::now().timestamp_millis();
                    let start_ms = opened_at_ms.take().unwrap_or(now_ms);
                    window.emit(&counters, &table_manager, &source_configs, &select_stmt, Some((start_ms, now_ms)), &sink, Some(&rule_state)).await;
                }
            }
        }
    }

    if event_time.enabled && !session_buffer.is_empty() {
        let s_start = session_start.unwrap_or(0);
        let s_end = last_event_ts.unwrap_or(0).saturating_add(timeout_ms);
        let batch: Vec<TaggedRow> = session_buffer.into_iter().map(|(_, r)| r).collect();
        emit_window_batch(
            &counters,
            &table_manager,
            &source_configs,
            &select_stmt,
            batch,
            true,
            Some((s_start, s_end)),
            &sink,
            Some(&rule_state),
        )
        .await;
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_hopping_window_rule(
    counters: Arc<RuleCounters>,
    running: Arc<RwLock<RuleStatus>>,
    rule_id: String,
    select_stmt: SelectStmt,
    mut rx: StreamReceiver,
    length: std::time::Duration,
    hop: std::time::Duration,
    sink: tokio::sync::mpsc::Sender<StreamRecord>,
    event_time: EventTimeConfig,
    send_error: bool,
    join_rxs: Vec<(String, StreamReceiver)>,
    table_manager: TableManager,
    source_configs: Arc<RwLock<HashMap<String, Value>>>,
    dlq_router: crate::engine::dlq::DlqRouter,
) {
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
    let length_ms = length.as_millis() as i64;
    let hop_ms = (hop.as_millis() as i64).max(1);

    // Hops end on natural-time multiples of the hop (eKuiper alignment).
    let mut ticker = aligned_interval(hop);
    let mut buffer: Vec<(std::time::Instant, TaggedRow)> = Vec::new();

    let mut et_buffer: Vec<(i64, TaggedRow)> = Vec::new();
    let mut watermark: i64 = i64::MIN;
    let mut next_hop_end: Option<i64> = None;

    let from_source = select_stmt.from.clone();
    let mut input_streams: Vec<String> = Vec::new();
    if table_manager.get_table(&from_source).is_none() {
        input_streams.push(from_source.clone());
    }
    let mut join_stream_names: Vec<String> = Vec::new();
    for (name, _) in &join_rxs {
        if !input_streams.contains(name) {
            input_streams.push(name.clone());
        }
        if !join_stream_names.contains(name) {
            join_stream_names.push(name.clone());
        }
    }
    let mut join_rx = spawn_join_forwarders(join_rxs);
    let mut joins_open = true;
    let mut stream_max_ts: HashMap<String, i64> = HashMap::new();
    let mut closed_streams: HashSet<String> = HashSet::new();

    macro_rules! ingest_hop {
        ($tagged:expr) => {{
            let tagged: TaggedRow = $tagged;
            if event_time.enabled {
                let ts_field = event_time
                    .source_timestamp_fields
                    .get(&tagged.source)
                    .map(|s| s.as_str())
                    .or(event_time.timestamp_field.as_deref());
                let event_ts = extract_event_timestamp(&tagged.data, ts_field);
                if event_ts < watermark {
                    // Late arrival beyond tolerance: drop
                } else {
                    let cur_max = stream_max_ts
                        .entry(tagged.source.clone())
                        .or_insert(event_ts);
                    *cur_max = (*cur_max).max(event_ts);

                    let all_active_seen = input_streams
                        .iter()
                        .filter(|s| !closed_streams.contains(*s))
                        .all(|s| stream_max_ts.contains_key(s));

                    if all_active_seen {
                        let min_source_ts = input_streams
                            .iter()
                            .filter(|s| !closed_streams.contains(*s))
                            .filter_map(|s| stream_max_ts.get(s))
                            .min()
                            .copied()
                            .unwrap_or(event_ts);
                        let new_wm = min_source_ts.saturating_sub(event_time.late_tolerance_ms);
                        watermark = watermark.max(new_wm);
                    }

                    if next_hop_end.is_none() {
                        let aligned = event_ts - event_ts.rem_euclid(hop_ms) + hop_ms;
                        next_hop_end = Some(aligned);
                    }

                    if window_ingest_passes(&select_stmt, &tagged.data) {
                        et_buffer.push((event_ts, tagged));
                    }

                    while let Some(cur_end) = next_hop_end {
                        if watermark < cur_end {
                            break;
                        }
                        let cur_start = cur_end.saturating_sub(length_ms);
                        let batch: Vec<TaggedRow> = et_buffer
                            .iter()
                            .filter(|(ts, _)| *ts >= cur_start && *ts < cur_end)
                            .map(|(_, row)| row.clone())
                            .collect();
                        next_hop_end = Some(cur_end.saturating_add(hop_ms));
                        let retain_after = cur_end.saturating_add(hop_ms).saturating_sub(length_ms);
                        et_buffer.retain(|(ts, _)| *ts >= retain_after);
                        if !batch.is_empty() {
                            emit_window_batch(
                                &counters,
                                &table_manager,
                                &source_configs,
                                &select_stmt,
                                batch,
                                true,
                                Some((cur_start, cur_end)),
                                &sink,
                                Some(&rule_state),
                            )
                            .await;
                        }
                    }
                }
            } else if window_ingest_passes(&select_stmt, &tagged.data) {
                buffer.push((std::time::Instant::now(), tagged));
            }
        }};
    }

    loop {
        tokio::select! {
            res = rx.recv() => {
                match res {
                    Some(record) => {
                        if !is_rule_running(&running) {
                            continue;
                        }
                        counters.inc_source(1);
                        if handle_error_record(&counters, &rule_id, send_error, &sink, &record, Some(&dlq_router))
                            .await
                        {
                            if dlq_router.on_error == crate::engine::dlq::OnErrorPolicy::Halt {
                                break;
                            }
                            continue;
                        }
                        ingest_hop!(TaggedRow { source: from_source.clone(), data: record.data });
                    }
                    None => break,
                }
            }
            jrec = join_rx.recv(), if joins_open => {
                match jrec {
                    Some(tagged) => {
                        if !is_rule_running(&running) {
                            continue;
                        }
                        counters.inc_source(1);
                        let probe = StreamRecord::from_shared(tagged.data.clone());
                        if handle_error_record(&counters, &rule_id, send_error, &sink, &probe, Some(&dlq_router)).await {
                            if dlq_router.on_error == crate::engine::dlq::OnErrorPolicy::Halt {
                                break;
                            }
                            continue;
                        }
                        ingest_hop!(tagged);
                    }
                    None => {
                        joins_open = false;
                        for s in &join_stream_names {
                            closed_streams.insert(s.clone());
                        }
                    }
                }
            }
            _ = ticker.tick() => {
                if event_time.enabled {
                    continue;
                }
                let now = std::time::Instant::now();
                // Expire and discard records older than the full window length
                buffer.retain(|(ts, _)| now.duration_since(*ts) <= length);
                if buffer.is_empty() {
                    continue;
                }
                let now_ms = chrono::Utc::now().timestamp_millis();
                let end_ms = now_ms - now_ms.rem_euclid(hop_ms);
                let start_ms = end_ms.saturating_sub(length_ms);
                let batch: Vec<TaggedRow> =
                    buffer.iter().map(|(_, row)| row.clone()).collect();
                emit_window_batch(&counters, &table_manager, &source_configs, &select_stmt, batch, true, Some((start_ms, end_ms)), &sink, Some(&rule_state)).await;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_sliding_window_rule(
    counters: Arc<RuleCounters>,
    running: Arc<RwLock<RuleStatus>>,
    rule_id: String,
    select_stmt: SelectStmt,
    mut rx: StreamReceiver,
    length: std::time::Duration,
    delay: Option<std::time::Duration>,
    sink: tokio::sync::mpsc::Sender<StreamRecord>,
    event_time: EventTimeConfig,
    send_error: bool,
    join_rxs: Vec<(String, StreamReceiver)>,
    table_manager: TableManager,
    source_configs: Arc<RwLock<HashMap<String, Value>>>,
    dlq_router: crate::engine::dlq::DlqRouter,
) {
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
    let mut buffer: Vec<(std::time::Instant, TaggedRow)> = Vec::new();
    let mut pending_pt_triggers: std::collections::VecDeque<(
        std::time::Instant,
        std::time::Instant,
        i64,
    )> = std::collections::VecDeque::new();
    let mut sliding_inc =
        (!event_time.enabled && select_stmt.window_trigger_condition.is_none() && delay.is_none())
            .then(|| rekuiper_sql::SlidingIncrementalWindow::try_new(&select_stmt))
            .flatten();
    // Event-time state: event-timestamped rows plus the watermark.
    let mut et_buffer: Vec<(i64, TaggedRow)> = Vec::new();
    let mut pending_triggers: Vec<(i64, i64)> = Vec::new();
    let mut watermark: i64 = i64::MIN;
    let window_millis = length.as_millis() as i64;
    let from_source = select_stmt.from.clone();
    let mut input_streams: Vec<String> = Vec::new();
    if table_manager.get_table(&from_source).is_none() {
        input_streams.push(from_source.clone());
    }
    let mut join_stream_names: Vec<String> = Vec::new();
    for (name, _) in &join_rxs {
        if !input_streams.contains(name) {
            input_streams.push(name.clone());
        }
        if !join_stream_names.contains(name) {
            join_stream_names.push(name.clone());
        }
    }
    let mut join_rx = spawn_join_forwarders(join_rxs);
    let mut joins_open = true;
    let mut stream_max_ts: HashMap<String, i64> = HashMap::new();
    let mut closed_streams: HashSet<String> = HashSet::new();

    macro_rules! ingest_slide {
        ($tagged:expr) => {{
            let tagged: TaggedRow = $tagged;
            if !window_ingest_passes(&select_stmt, &tagged.data) {
                // Filtered below the window: neither buffered nor a trigger.
            } else if event_time.enabled {
                let ts_field = event_time
                    .source_timestamp_fields
                    .get(&tagged.source)
                    .map(|s| s.as_str())
                    .or(event_time.timestamp_field.as_deref());
                let event_ts = extract_event_timestamp(&tagged.data, ts_field);
                if event_ts < watermark {
                    // Late arrival beyond the tolerance horizon: drop.
                } else {
                    let cur_max = stream_max_ts
                        .entry(tagged.source.clone())
                        .or_insert(event_ts);
                    *cur_max = (*cur_max).max(event_ts);

                    let all_active_seen = input_streams
                        .iter()
                        .filter(|s| !closed_streams.contains(*s))
                        .all(|s| stream_max_ts.contains_key(s));

                    if all_active_seen {
                        let min_source_ts = input_streams
                            .iter()
                            .filter(|s| !closed_streams.contains(*s))
                            .filter_map(|s| stream_max_ts.get(s))
                            .min()
                            .copied()
                            .unwrap_or(event_ts);
                        let new_wm = min_source_ts.saturating_sub(event_time.late_tolerance_ms);
                        watermark = watermark.max(new_wm);
                    }

                    et_buffer.push((event_ts, tagged.clone()));
                    et_buffer.sort_by_key(|(ts, _)| *ts);

                    let should_trigger = match &select_stmt.window_trigger_condition {
                        Some(cond) => Evaluator::eval_bool(cond, &tagged.data),
                        None => true,
                    };

                    let delay_ms = delay.map(|d| d.as_millis() as i64).unwrap_or(0);
                    if should_trigger {
                        if delay_ms > 0 {
                            pending_triggers.push((event_ts, event_ts.saturating_add(delay_ms)));
                            pending_triggers.sort_by_key(|(_, end)| *end);
                        } else {
                            let start_ts = event_ts.saturating_sub(window_millis);
                            let batch: Vec<TaggedRow> = et_buffer
                                .iter()
                                .filter(|(ts, _)| *ts >= start_ts && *ts <= event_ts)
                                .map(|(_, row)| row.clone())
                                .collect();
                            emit_window_batch(
                                &counters,
                                &table_manager,
                                &source_configs,
                                &select_stmt,
                                batch,
                                true,
                                Some((start_ts, event_ts)),
                                &sink,
                                Some(&rule_state),
                            )
                            .await;
                        }
                    }

                    while !pending_triggers.is_empty() && watermark >= pending_triggers[0].1 {
                        // Close at the current watermark, which may jump past the delay deadline.
                        let (trigger_ts, _) = pending_triggers.remove(0);
                        let window_end_ts = watermark;
                        let start_ts = trigger_ts.saturating_sub(window_millis);
                        let batch: Vec<TaggedRow> = et_buffer
                            .iter()
                            .filter(|(ts, _)| *ts >= start_ts && *ts <= window_end_ts)
                            .map(|(_, row)| row.clone())
                            .collect();
                        emit_window_batch(
                            &counters,
                            &table_manager,
                            &source_configs,
                            &select_stmt,
                            batch,
                            true,
                            Some((start_ts, window_end_ts)),
                            &sink,
                            Some(&rule_state),
                        )
                        .await;
                    }

                    let min_pending = pending_triggers
                        .first()
                        .map(|(ts, _)| *ts)
                        .unwrap_or(event_ts);
                    let retain_ts = min_pending.min(event_ts).saturating_sub(window_millis);
                    et_buffer.retain(|(ts, _)| *ts >= retain_ts);
                }
            } else if let Some(ref mut inc) = sliding_inc {
                let now_ms = chrono::Utc::now().timestamp_millis();
                let cutoff_ms = now_ms.saturating_sub(window_millis);
                inc.evict_before(cutoff_ms);
                let _ = inc.push(now_ms, &tagged.data);
                let outputs = inc.snapshot();
                if !outputs.is_empty() {
                    emit_window_outputs(&counters, &sink, outputs).await;
                }
            } else {
                let now = std::time::Instant::now();
                buffer.push((now, tagged.clone()));
                let anchor = pending_pt_triggers
                    .front()
                    .map(|(t, _, _)| *t)
                    .unwrap_or(now);
                buffer.retain(|(ts, _)| anchor.saturating_duration_since(*ts) <= length);

                let should_trigger = match &select_stmt.window_trigger_condition {
                    Some(cond) => Evaluator::eval_bool(cond, &tagged.data),
                    None => true,
                };

                if should_trigger && !buffer.is_empty() {
                    let now_ms = chrono::Utc::now().timestamp_millis();
                    if let Some(delay_dur) = delay.filter(|d| !d.is_zero()) {
                        pending_pt_triggers.push_back((now, now + delay_dur, now_ms));
                    } else {
                        let start_ms = now_ms.saturating_sub(window_millis);
                        let batch: Vec<TaggedRow> =
                            buffer.iter().map(|(_, row)| row.clone()).collect();
                        emit_window_batch(
                            &counters,
                            &table_manager,
                            &source_configs,
                            &select_stmt,
                            batch,
                            true,
                            Some((start_ms, now_ms)),
                            &sink,
                            Some(&rule_state),
                        )
                        .await;
                    }
                }
            }
        }};
    }
    loop {
        tokio::select! {
            res = rx.recv() => {
            match res {
            Some(record) => {
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
                ingest_slide!(TaggedRow { source: from_source.clone(), data: record.data });
            }
            None => break,
            }
            }
            jrec = join_rx.recv(), if joins_open => {
                match jrec {
                    Some(tagged) => {
                        if !is_rule_running(&running) {
                            continue;
                        }
                        counters.inc_source(1);
                        let probe = StreamRecord::from_shared(tagged.data.clone());
                        if handle_error_record(&counters, &rule_id, send_error, &sink, &probe, Some(&dlq_router)).await {
                            if dlq_router.on_error == crate::engine::dlq::OnErrorPolicy::Halt {
                                break;
                            }
                            continue;
                        }
                        ingest_slide!(tagged);
                    }
                    None => {
                        joins_open = false;
                        for s in &join_stream_names {
                            closed_streams.insert(s.clone());
                        }
                    }
                }
            }
            _ = async {
                if let Some(&(_, deadline, _)) = pending_pt_triggers.front() {
                    tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
                } else {
                    std::future::pending::<()>().await;
                }
            }, if !pending_pt_triggers.is_empty() => {
                let now = std::time::Instant::now();
                while let Some(&(trigger_inst, deadline, trigger_ms)) = pending_pt_triggers.front() {
                    if now < deadline {
                        break;
                    }
                    pending_pt_triggers.pop_front();
                    if !is_rule_running(&running) {
                        continue;
                    }
                    let batch: Vec<TaggedRow> = buffer
                        .iter()
                        .filter(|(ts, _)| {
                            if *ts <= trigger_inst {
                                trigger_inst.saturating_duration_since(*ts) <= length
                            } else {
                                *ts <= deadline
                            }
                        })
                        .map(|(_, row)| row.clone())
                        .collect();
                    if !batch.is_empty() {
                        let end_ms = chrono::Utc::now().timestamp_millis();
                        let start_ms = trigger_ms.saturating_sub(window_millis);
                        emit_window_batch(
                            &counters,
                            &table_manager,
                            &source_configs,
                            &select_stmt,
                            batch,
                            true,
                            Some((start_ms, end_ms)),
                            &sink,
                            Some(&rule_state),
                        )
                        .await;
                    }
                }
                let anchor = pending_pt_triggers
                    .front()
                    .map(|(t, _, _)| *t)
                    .unwrap_or_else(std::time::Instant::now);
                buffer.retain(|(ts, _)| anchor.saturating_duration_since(*ts) <= length);
            }
        }
    }
}

/// STATEWINDOW: creates dynamic windows based on condition matches.
/// Two conditions: begins on start_condition, emits on end_condition.
/// Single condition: begins on start_condition, emits prior window when start_condition matches again.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_state_window_rule(
    counters: Arc<RuleCounters>,
    running: Arc<RwLock<RuleStatus>>,
    rule_id: String,
    select_stmt: SelectStmt,
    mut rx: StreamReceiver,
    start_condition: Expr,
    end_condition: Option<Expr>,
    sink: tokio::sync::mpsc::Sender<StreamRecord>,
    event_time: EventTimeConfig,
    send_error: bool,
    join_rxs: Vec<(String, StreamReceiver)>,
    table_manager: TableManager,
    source_configs: Arc<RwLock<HashMap<String, Value>>>,
    dlq_router: crate::engine::dlq::DlqRouter,
) {
    struct StateWindowState {
        on_begin: bool,
        start_time: i64,
        buffer: Vec<TaggedRow>,
    }

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
    let mut partitions: HashMap<String, StateWindowState> = HashMap::new();
    let from_source = select_stmt.from.clone();
    let mut join_rx = spawn_join_forwarders(join_rxs);
    let mut joins_open = true;

    macro_rules! ingest_state {
        ($tagged:expr) => {{
            let tagged: TaggedRow = $tagged;
            if !window_ingest_passes(&select_stmt, &tagged.data) {
                // Filtered below the window
            } else {
                let ts = if event_time.enabled {
                    let ts_field = event_time
                        .source_timestamp_fields
                        .get(&tagged.source)
                        .map(|s| s.as_str())
                        .or(event_time.timestamp_field.as_deref());
                    extract_event_timestamp(&tagged.data, ts_field)
                } else {
                    chrono::Utc::now().timestamp_millis()
                };

                let partition_key = match &select_stmt.window_partition_by {
                    Some(p_expr) => Evaluator::eval_val(p_expr, &tagged.data).to_string(),
                    None => String::new(),
                };

                let state = partitions
                    .entry(partition_key)
                    .or_insert_with(|| StateWindowState {
                        on_begin: false,
                        start_time: 0,
                        buffer: Vec::new(),
                    });

                match &end_condition {
                    Some(end_cond) => {
                        if !state.on_begin {
                            if Evaluator::eval_bool_stateful(
                                &start_condition,
                                &tagged.data,
                                &rule_state,
                            ) {
                                state.start_time = ts;
                                state.on_begin = true;
                                state.buffer.push(tagged);
                            }
                        } else {
                            state.buffer.push(tagged.clone());
                            if Evaluator::eval_bool_stateful(end_cond, &tagged.data, &rule_state) {
                                state.on_begin = false;
                                let batch = std::mem::take(&mut state.buffer);
                                emit_window_batch(
                                    &counters,
                                    &table_manager,
                                    &source_configs,
                                    &select_stmt,
                                    batch,
                                    true,
                                    Some((state.start_time, ts)),
                                    &sink,
                                    Some(&rule_state),
                                )
                                .await;
                            }
                        }
                    }
                    None => {
                        if !state.on_begin {
                            if Evaluator::eval_bool_stateful(
                                &start_condition,
                                &tagged.data,
                                &rule_state,
                            ) {
                                state.start_time = ts;
                                state.on_begin = true;
                                state.buffer.push(tagged);
                            }
                        } else if Evaluator::eval_bool_stateful(
                            &start_condition,
                            &tagged.data,
                            &rule_state,
                        ) {
                            let batch = std::mem::take(&mut state.buffer);
                            let prev_start = state.start_time;
                            state.start_time = ts;
                            state.on_begin = true;
                            state.buffer.push(tagged);
                            emit_window_batch(
                                &counters,
                                &table_manager,
                                &source_configs,
                                &select_stmt,
                                batch,
                                true,
                                Some((prev_start, ts)),
                                &sink,
                                Some(&rule_state),
                            )
                            .await;
                        } else {
                            state.buffer.push(tagged);
                        }
                    }
                }
            }
        }};
    }

    loop {
        tokio::select! {
            res = rx.recv() => {
                match res {
                    Some(record) => {
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
                        ingest_state!(TaggedRow { source: from_source.clone(), data: record.data });
                    }
                    None => break,
                }
            }
            jrec = join_rx.recv(), if joins_open => {
                match jrec {
                    Some(tagged) => {
                        if !is_rule_running(&running) {
                            continue;
                        }
                        counters.inc_source(1);
                        let probe = StreamRecord::from_shared(tagged.data.clone());
                        if handle_error_record(&counters, &rule_id, send_error, &sink, &probe, Some(&dlq_router)).await {
                            if dlq_router.on_error == crate::engine::dlq::OnErrorPolicy::Halt {
                                break;
                            }
                            continue;
                        }
                        ingest_state!(tagged);
                    }
                    None => {
                        joins_open = false;
                    }
                }
            }
        }
    }
}
