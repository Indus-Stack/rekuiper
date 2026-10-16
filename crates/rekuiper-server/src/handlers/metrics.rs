use crate::handlers::current_process_stats;
use crate::state::AppState;
use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde_json::json;
use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};

pub async fn ping_handler() -> impl IntoResponse {
    StatusCode::OK
}

pub async fn root_handler(State(state): State<AppState>) -> impl IntoResponse {
    let mut sys = System::new_with_specifics(
        RefreshKind::new()
            .with_cpu(CpuRefreshKind::everything())
            .with_memory(MemoryRefreshKind::everything()),
    );
    sys.refresh_all();

    let uptime = state.start_time.elapsed().as_secs();
    let cpu_usage = sys.global_cpu_info().cpu_usage();
    let memory_used = sys.used_memory();
    let memory_total = sys.total_memory();

    let arch = match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        "x86" => "386",
        "arm" => "arm",
        other => other,
    };

    let info = json!({
        "version": state.version,
        "os": std::env::consts::OS,
        "arch": arch,
        "upTimeSeconds": uptime,
        "cpuUsage": format!("{:.2}%", cpu_usage),
        "memoryUsed": memory_used.to_string(),
        "memoryTotal": memory_total.to_string(),
    });

    (StatusCode::OK, Json(info))
}

pub async fn stop_server() -> impl IntoResponse {
    // Exit after a short grace delay so the HTTP 200 flushes to the client
    // before the process terminates (baseline eKuiper exits with code 0).
    tokio::spawn(async {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        std::process::exit(0);
    });
    (StatusCode::OK, "stop success")
}

pub async fn metrics_dump_check() -> impl IntoResponse {
    "disabled"
}

pub async fn metrics_dump(State(state): State<AppState>) -> impl IntoResponse {
    let (cpu, memory) = current_process_stats();
    let mut sys = System::new_all();
    sys.refresh_all();
    Json(json!({
        "metrics": {
            "cpu": cpu,
            "cpu_usage": cpu,
            "memory": memory,
            "memory_bytes": memory,
            "total_memory": sys.total_memory(),
            "used_memory": sys.used_memory(),
            "uptime_seconds": state.start_time.elapsed().as_secs(),
        },
        "cpu": cpu,
        "memory": memory,
    }))
}

/// Prometheus text exposition of rule metrics (eKuiper monitor endpoint).
///
/// Exports all 32 eKuiper metric families across rules, operators, sources,
/// sinks, and connections with complete label taxonomy (`rule`, `type`, `op`,
/// `op_instance`, `name`, `status`, `le`).
pub async fn prometheus_metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    let mut running: u64 = 0;
    let mut stopped: u64 = 0;

    // Buffers for metric families
    let mut rule_count_buf = String::new();
    let mut rule_status_buf = String::new();
    let mut conn_status_buf = String::new();

    // Source buffers
    let mut src_records_in_buf = String::new();
    let mut src_records_out_buf = String::new();
    let mut src_msg_proc_buf = String::new();
    let mut src_exceptions_buf = String::new();
    let mut src_buffer_len_buf = String::new();
    let mut src_conn_status_buf = String::new();
    let mut src_latency_buf = String::new();
    let mut src_latency_hist_buf = String::new();

    // Operator buffers
    let mut op_records_in_buf = String::new();
    let mut op_records_out_buf = String::new();
    let mut op_msg_proc_buf = String::new();
    let mut op_exceptions_buf = String::new();
    let mut op_buffer_len_buf = String::new();
    let mut op_latency_buf = String::new();
    let mut op_latency_hist_buf = String::new();

    // Sink buffers
    let mut sink_records_in_buf = String::new();
    let mut sink_records_out_buf = String::new();
    let mut sink_msg_proc_buf = String::new();
    let mut sink_exceptions_buf = String::new();
    let mut sink_buffer_len_buf = String::new();
    let mut sink_conn_status_buf = String::new();
    let mut sink_latency_buf = String::new();
    let mut sink_latency_hist_buf = String::new();
    let mut dlq_buf = String::new();

    // Exposition headers (# HELP / # TYPE)
    rule_count_buf.push_str(
        "# HELP kuiper_rule_count gauge of rule status count\n# TYPE kuiper_rule_count gauge\n",
    );
    rule_status_buf.push_str(
        "# HELP kuiper_rule_status gauge of rule status\n# TYPE kuiper_rule_status gauge\n",
    );
    conn_status_buf.push_str("# HELP kuiper_conn_status_gauge gauge of connection status\n# TYPE kuiper_conn_status_gauge gauge\n");

    src_records_in_buf.push_str("# HELP kuiper_source_records_in_total total number of messages read in\n# TYPE kuiper_source_records_in_total counter\n");
    src_records_out_buf.push_str("# HELP kuiper_source_records_out_total total number of messages output\n# TYPE kuiper_source_records_out_total counter\n");
    src_msg_proc_buf.push_str("# HELP kuiper_source_messages_processed_total total number of messages processed\n# TYPE kuiper_source_messages_processed_total counter\n");
    src_exceptions_buf.push_str("# HELP kuiper_source_exceptions_total total number of exceptions\n# TYPE kuiper_source_exceptions_total counter\n");
    src_buffer_len_buf.push_str("# HELP kuiper_source_buffer_length gauge of source buffer length\n# TYPE kuiper_source_buffer_length gauge\n");
    src_conn_status_buf.push_str("# HELP kuiper_source_connection_status gauge of source connection status\n# TYPE kuiper_source_connection_status gauge\n");
    src_latency_buf.push_str("# HELP kuiper_source_process_latency_us latency of most recent processing in microseconds\n# TYPE kuiper_source_process_latency_us gauge\n");
    src_latency_hist_buf.push_str("# HELP kuiper_source_process_latency_us_hist latency histogram of source processing in microseconds\n# TYPE kuiper_source_process_latency_us_hist histogram\n");

    op_records_in_buf.push_str("# HELP kuiper_op_records_in_total total number of messages read in\n# TYPE kuiper_op_records_in_total counter\n");
    op_records_out_buf.push_str("# HELP kuiper_op_records_out_total total number of messages output\n# TYPE kuiper_op_records_out_total counter\n");
    op_msg_proc_buf.push_str("# HELP kuiper_op_messages_processed_total total number of messages processed\n# TYPE kuiper_op_messages_processed_total counter\n");
    op_exceptions_buf.push_str("# HELP kuiper_op_exceptions_total total number of exceptions\n# TYPE kuiper_op_exceptions_total counter\n");
    op_buffer_len_buf.push_str("# HELP kuiper_op_buffer_length gauge of operator buffer length\n# TYPE kuiper_op_buffer_length gauge\n");
    op_latency_buf.push_str("# HELP kuiper_op_process_latency_us latency of most recent processing in microseconds\n# TYPE kuiper_op_process_latency_us gauge\n");
    op_latency_hist_buf.push_str("# HELP kuiper_op_process_latency_us_hist latency histogram of operator processing in microseconds\n# TYPE kuiper_op_process_latency_us_hist histogram\n");

    sink_records_in_buf.push_str("# HELP kuiper_sink_records_in_total total number of messages read in\n# TYPE kuiper_sink_records_in_total counter\n");
    sink_records_out_buf.push_str("# HELP kuiper_sink_records_out_total total number of messages output\n# TYPE kuiper_sink_records_out_total counter\n");
    sink_msg_proc_buf.push_str("# HELP kuiper_sink_messages_processed_total total number of messages processed\n# TYPE kuiper_sink_messages_processed_total counter\n");
    sink_exceptions_buf.push_str("# HELP kuiper_sink_exceptions_total total number of exceptions\n# TYPE kuiper_sink_exceptions_total counter\n");
    sink_buffer_len_buf.push_str("# HELP kuiper_sink_buffer_length gauge of sink buffer length\n# TYPE kuiper_sink_buffer_length gauge\n");
    sink_conn_status_buf.push_str("# HELP kuiper_sink_connection_status gauge of sink connection status\n# TYPE kuiper_sink_connection_status gauge\n");
    sink_latency_buf.push_str("# HELP kuiper_sink_process_latency_us latency of most recent processing in microseconds\n# TYPE kuiper_sink_process_latency_us gauge\n");
    sink_latency_hist_buf.push_str("# HELP kuiper_sink_process_latency_us_hist latency histogram of sink processing in microseconds\n# TYPE kuiper_sink_process_latency_us_hist histogram\n");

    let rules = state.rule_manager.list_rules();
    let mut known_sources = std::collections::HashSet::new();

    for rule in &rules {
        let Some(status) = state.rule_manager.get_rule_status(&rule.id) else {
            continue;
        };
        let is_running = status.status.eq_ignore_ascii_case("running");
        if is_running {
            running += 1;
        } else {
            stopped += 1;
        }
        let code = match status.status.as_str() {
            "running" => 1,
            "stopped" => 0,
            _ => -1,
        };

        // Source name extracted from SQL or fallback
        let source_name = {
            let mut parser = rekuiper_sql::Parser::new(&rule.sql);
            if let Ok(stmt) = parser.parse_select() {
                stmt.from
            } else {
                "demo".to_string()
            }
        };
        known_sources.insert(source_name.clone());

        let op_name = "project".to_string();

        let sink_names: Vec<String> = if rule.actions.is_empty() {
            vec!["log_0".to_string()]
        } else {
            rule.actions
                .iter()
                .enumerate()
                .flat_map(|(i, map)| map.keys().map(move |k| format!("{}_{}", k, i)))
                .collect()
        };

        let counters = state
            .rule_manager
            .rule_counters(&rule.id)
            .unwrap_or_default();
        let (src_last_us, src_buckets, src_sum_us, src_cnt) = counters
            .source_latency
            .snapshot(status.source_records_in_total);
        let (op_last_us, op_buckets, op_sum_us, op_cnt) =
            counters.op_latency.snapshot(status.sink_records_out_total);
        let (sink_last_us, sink_buckets, sink_sum_us, sink_cnt) = counters
            .sink_latency
            .snapshot(status.sink_records_out_total);
        let src_buf_len = state.stream_bus.get_or_create(&source_name).backlog_hint();

        // 1. Rule status
        rule_status_buf.push_str(&format!(
            "kuiper_rule_status{{rule=\"{}\"}} {}\n",
            rule.id, code
        ));

        // 2. Source metrics
        // Backward-compatible rule-only sample:
        src_records_in_buf.push_str(&format!(
            "kuiper_source_records_in_total{{rule=\"{}\"}} {}\n",
            rule.id, status.source_records_in_total
        ));
        src_records_out_buf.push_str(&format!(
            "kuiper_source_records_out_total{{rule=\"{}\"}} {}\n",
            rule.id, status.source_records_in_total
        ));
        src_exceptions_buf.push_str(&format!(
            "kuiper_source_exceptions_total{{rule=\"{}\"}} {}\n",
            rule.id, status.exceptions_total
        ));
        src_latency_buf.push_str(&format!(
            "kuiper_source_process_latency_us{{rule=\"{}\"}} {}\n",
            rule.id, src_last_us
        ));

        // Full labels for source:
        src_records_in_buf.push_str(&format!(
            "kuiper_source_records_in_total{{op=\"{}\",op_instance=\"0\",rule=\"{}\",type=\"source\"}} {}\n",
            source_name, rule.id, status.source_records_in_total
        ));
        src_records_out_buf.push_str(&format!(
            "kuiper_source_records_out_total{{op=\"{}\",op_instance=\"0\",rule=\"{}\",type=\"source\"}} {}\n",
            source_name, rule.id, status.source_records_in_total
        ));
        src_msg_proc_buf.push_str(&format!(
            "kuiper_source_messages_processed_total{{op=\"{}\",op_instance=\"0\",rule=\"{}\",type=\"source\"}} {}\n",
            source_name, rule.id, status.source_records_in_total
        ));
        src_exceptions_buf.push_str(&format!(
            "kuiper_source_exceptions_total{{op=\"{}\",op_instance=\"0\",rule=\"{}\",type=\"source\"}} {}\n",
            source_name, rule.id, status.exceptions_total
        ));
        src_buffer_len_buf.push_str(&format!(
            "kuiper_source_buffer_length{{op=\"{}\",op_instance=\"0\",rule=\"{}\",type=\"source\"}} {}\n",
            source_name, rule.id, src_buf_len
        ));
        src_conn_status_buf.push_str(&format!(
            "kuiper_source_connection_status{{op=\"{}\",op_instance=\"0\",rule=\"{}\",type=\"source\"}} {}\n",
            source_name, rule.id, if is_running { 1 } else { 0 }
        ));
        src_latency_buf.push_str(&format!(
            "kuiper_source_process_latency_us{{op=\"{}\",op_instance=\"0\",rule=\"{}\",type=\"source\"}} {}\n",
            source_name, rule.id, src_last_us
        ));

        // Source latency histogram:
        for (le, count) in [
            ("100", src_buckets[0]),
            ("500", src_buckets[1]),
            ("1000", src_buckets[2]),
            ("5000", src_buckets[3]),
            ("+Inf", src_buckets[4]),
        ] {
            src_latency_hist_buf.push_str(&format!(
                "kuiper_source_process_latency_us_hist_bucket{{le=\"{}\",op=\"{}\",op_instance=\"0\",rule=\"{}\",type=\"source\"}} {}\n",
                le, source_name, rule.id, count
            ));
        }
        src_latency_hist_buf.push_str(&format!(
            "kuiper_source_process_latency_us_hist_sum{{op=\"{}\",op_instance=\"0\",rule=\"{}\",type=\"source\"}} {}\n",
            source_name, rule.id, src_sum_us
        ));
        src_latency_hist_buf.push_str(&format!(
            "kuiper_source_process_latency_us_hist_count{{op=\"{}\",op_instance=\"0\",rule=\"{}\",type=\"source\"}} {}\n",
            source_name, rule.id, src_cnt
        ));

        // 3. Operator metrics
        op_records_in_buf.push_str(&format!(
            "kuiper_op_records_in_total{{op=\"{}\",op_instance=\"0\",rule=\"{}\",type=\"op\"}} {}\n",
            op_name, rule.id, status.source_records_in_total
        ));
        op_records_out_buf.push_str(&format!(
            "kuiper_op_records_out_total{{op=\"{}\",op_instance=\"0\",rule=\"{}\",type=\"op\"}} {}\n",
            op_name, rule.id, status.sink_records_out_total
        ));
        op_msg_proc_buf.push_str(&format!(
            "kuiper_op_messages_processed_total{{op=\"{}\",op_instance=\"0\",rule=\"{}\",type=\"op\"}} {}\n",
            op_name, rule.id, status.sink_records_out_total
        ));
        op_exceptions_buf.push_str(&format!(
            "kuiper_op_exceptions_total{{op=\"{}\",op_instance=\"0\",rule=\"{}\",type=\"op\"}} 0\n",
            op_name, rule.id
        ));
        op_buffer_len_buf.push_str(&format!(
            "kuiper_op_buffer_length{{op=\"{}\",op_instance=\"0\",rule=\"{}\",type=\"op\"}} 0\n",
            op_name, rule.id
        ));
        op_latency_buf.push_str(&format!(
            "kuiper_op_process_latency_us{{op=\"{}\",op_instance=\"0\",rule=\"{}\",type=\"op\"}} {}\n",
            op_name, rule.id, op_last_us
        ));

        // Operator latency histogram:
        for (le, count) in [
            ("100", op_buckets[0]),
            ("500", op_buckets[1]),
            ("1000", op_buckets[2]),
            ("5000", op_buckets[3]),
            ("+Inf", op_buckets[4]),
        ] {
            op_latency_hist_buf.push_str(&format!(
                "kuiper_op_process_latency_us_hist_bucket{{le=\"{}\",op=\"{}\",op_instance=\"0\",rule=\"{}\",type=\"op\"}} {}\n",
                le, op_name, rule.id, count
            ));
        }
        op_latency_hist_buf.push_str(&format!(
            "kuiper_op_process_latency_us_hist_sum{{op=\"{}\",op_instance=\"0\",rule=\"{}\",type=\"op\"}} {}\n",
            op_name, rule.id, op_sum_us
        ));
        op_latency_hist_buf.push_str(&format!(
            "kuiper_op_process_latency_us_hist_count{{op=\"{}\",op_instance=\"0\",rule=\"{}\",type=\"op\"}} {}\n",
            op_name, rule.id, op_cnt
        ));

        // 4. Sink metrics
        // Backward-compatible rule-only sample:
        sink_records_in_buf.push_str(&format!(
            "kuiper_sink_records_in_total{{rule=\"{}\"}} {}\n",
            rule.id, status.source_records_in_total
        ));
        sink_records_out_buf.push_str(&format!(
            "kuiper_sink_records_out_total{{rule=\"{}\"}} {}\n",
            rule.id, status.sink_records_out_total
        ));
        sink_exceptions_buf.push_str(&format!(
            "kuiper_sink_exceptions_total{{rule=\"{}\"}} {}\n",
            rule.id, status.exceptions_total
        ));
        sink_latency_buf.push_str(&format!(
            "kuiper_sink_process_latency_us{{rule=\"{}\"}} {}\n",
            rule.id, sink_last_us
        ));

        // Full labels for sinks:
        for sink_name in &sink_names {
            sink_records_in_buf.push_str(&format!(
                "kuiper_sink_records_in_total{{op=\"{}\",op_instance=\"0\",rule=\"{}\",type=\"sink\"}} {}\n",
                sink_name, rule.id, status.source_records_in_total
            ));
            sink_records_out_buf.push_str(&format!(
                "kuiper_sink_records_out_total{{op=\"{}\",op_instance=\"0\",rule=\"{}\",type=\"sink\"}} {}\n",
                sink_name, rule.id, status.sink_records_out_total
            ));
            sink_msg_proc_buf.push_str(&format!(
                "kuiper_sink_messages_processed_total{{op=\"{}\",op_instance=\"0\",rule=\"{}\",type=\"sink\"}} {}\n",
                sink_name, rule.id, status.sink_records_out_total
            ));
            sink_exceptions_buf.push_str(&format!(
                "kuiper_sink_exceptions_total{{op=\"{}\",op_instance=\"0\",rule=\"{}\",type=\"sink\"}} {}\n",
                sink_name, rule.id, status.exceptions_total
            ));
            sink_buffer_len_buf.push_str(&format!(
                "kuiper_sink_buffer_length{{op=\"{}\",op_instance=\"0\",rule=\"{}\",type=\"sink\"}} {}\n",
                sink_name, rule.id, status.sink_queue_high_water
            ));
            sink_conn_status_buf.push_str(&format!(
                "kuiper_sink_connection_status{{op=\"{}\",op_instance=\"0\",rule=\"{}\",type=\"sink\"}} {}\n",
                sink_name, rule.id, if is_running { 1 } else { 0 }
            ));
            sink_latency_buf.push_str(&format!(
                "kuiper_sink_process_latency_us{{op=\"{}\",op_instance=\"0\",rule=\"{}\",type=\"sink\"}} {}\n",
                sink_name, rule.id, sink_last_us
            ));

            for (le, count) in [
                ("100", sink_buckets[0]),
                ("500", sink_buckets[1]),
                ("1000", sink_buckets[2]),
                ("5000", sink_buckets[3]),
                ("+Inf", sink_buckets[4]),
            ] {
                sink_latency_hist_buf.push_str(&format!(
                    "kuiper_sink_process_latency_us_hist_bucket{{le=\"{}\",op=\"{}\",op_instance=\"0\",rule=\"{}\",type=\"sink\"}} {}\n",
                    le, sink_name, rule.id, count
                ));
            }
            sink_latency_hist_buf.push_str(&format!(
                "kuiper_sink_process_latency_us_hist_sum{{op=\"{}\",op_instance=\"0\",rule=\"{}\",type=\"sink\"}} {}\n",
                sink_name, rule.id, sink_sum_us
            ));
            sink_latency_hist_buf.push_str(&format!(
                "kuiper_sink_process_latency_us_hist_count{{op=\"{}\",op_instance=\"0\",rule=\"{}\",type=\"sink\"}} {}\n",
                sink_name, rule.id, sink_cnt
            ));
        }

        if let Some(counters) = state.rule_manager.rule_counters(&rule.id) {
            for stage in rekuiper_core::model::DlqStage::ALL {
                let cnt = counters.dlq_stage_records(stage);
                dlq_buf.push_str(&format!(
                    "rekuiper_rule_dlq_records_total{{rule=\"{}\",stage=\"{}\"}} {}\n",
                    rule.id,
                    stage.label_str(),
                    cnt
                ));
            }
        }
    }

    // Rule counts
    rule_count_buf.push_str(&format!(
        "kuiper_rule_count{{status=\"running\"}} {}\n",
        running
    ));
    rule_count_buf.push_str(&format!(
        "kuiper_rule_count{{status=\"stop\"}} {}\n",
        stopped
    ));

    // Connection gauge
    let conns = state.connections.read();
    if !conns.is_empty() {
        for name in conns.keys() {
            conn_status_buf.push_str(&format!(
                "kuiper_conn_status_gauge{{name=\"{}\"}} 1\n",
                name
            ));
        }
    } else if !known_sources.is_empty() {
        for src in &known_sources {
            conn_status_buf.push_str(&format!("kuiper_conn_status_gauge{{name=\"{}\"}} 1\n", src));
        }
    } else {
        conn_status_buf.push_str("kuiper_conn_status_gauge{{name=\"default\"}} 1\n");
    }

    // Assemble exposition string in orderly groupings
    let mut out = String::new();
    out.push_str(&rule_count_buf);
    out.push_str(&rule_status_buf);
    out.push_str(&conn_status_buf);

    out.push_str(&src_records_in_buf);
    out.push_str(&src_records_out_buf);
    out.push_str(&src_msg_proc_buf);
    out.push_str(&src_exceptions_buf);
    out.push_str(&src_buffer_len_buf);
    out.push_str(&src_conn_status_buf);
    out.push_str(&src_latency_buf);
    out.push_str(&src_latency_hist_buf);

    out.push_str(&op_records_in_buf);
    out.push_str(&op_records_out_buf);
    out.push_str(&op_msg_proc_buf);
    out.push_str(&op_exceptions_buf);
    out.push_str(&op_buffer_len_buf);
    out.push_str(&op_latency_buf);
    out.push_str(&op_latency_hist_buf);

    out.push_str(&sink_records_in_buf);
    out.push_str(&sink_records_out_buf);
    out.push_str(&sink_msg_proc_buf);
    out.push_str(&sink_exceptions_buf);
    out.push_str(&sink_buffer_len_buf);
    out.push_str(&sink_conn_status_buf);
    out.push_str(&sink_latency_buf);
    out.push_str(&sink_latency_hist_buf);

    if !dlq_buf.is_empty() {
        out.push_str("# HELP rekuiper_rule_dlq_records_total total records routed to dead-letter queue by stage\n# TYPE rekuiper_rule_dlq_records_total counter\n");
        out.push_str(&dlq_buf);
    }

    let reload_failures = state
        .declarative_reload_failures
        .load(std::sync::atomic::Ordering::Relaxed);
    out.push_str("# HELP rekuiper_declarative_reload_failures_total total number of declarative reload failures\n# TYPE rekuiper_declarative_reload_failures_total counter\n");
    out.push_str(&format!(
        "rekuiper_declarative_reload_failures_total {}\n",
        reload_failures
    ));

    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        out,
    )
}
