use crate::engine::{bootstrap_rule_sources, cancel_rule_source, spawn_rule_task};
use crate::state::{check_valid_name, AppState, TracerConfig};
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use rekuiper_core::model::{compile_graph_to_sql_and_actions, RuleDefinition};
use rekuiper_core::{PluginDefinition, PluginManager};
use rekuiper_sql::{is_builtin_function, Evaluator, Expr, Parser, SelectStmt, TimeUnit, WindowDef};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use sysinfo::System;

pub async fn list_rules(State(state): State<AppState>) -> impl IntoResponse {
    let rules = state.rule_manager.list_rules();
    let summaries: Vec<Value> = rules
        .into_iter()
        .map(|r| {
            let status_str = match state.rule_manager.get_rule_status(&r.id) {
                Some(st) => st.status,
                None => "stopped".to_string(),
            };
            let name = r
                .name
                .filter(|n| !n.is_empty())
                .unwrap_or_else(|| r.id.clone());
            let tags = if r.tags.is_empty() {
                Value::Null
            } else {
                json!(r.tags)
            };
            let trace = state.trace_manager.is_tracing(&r.id);
            let version = r.version.unwrap_or_default();
            json!({
                "id": r.id,
                "name": name,
                "status": status_str,
                "tags": tags,
                "trace": trace,
                "version": version,
            })
        })
        .collect();
    Json(summaries)
}

pub async fn create_rule(
    State(state): State<AppState>,
    Json(mut rule): Json<RuleDefinition>,
) -> Response {
    if rule.id.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": 1000,
                "message": "Missing rule id."
            })),
        )
            .into_response();
    }
    if let Err(e) = validate_rule_options(&rule.options) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": 1000,
                "message": e
            })),
        )
            .into_response();
    }
    if let Err(e) = validate_sink_actions(&rule.actions) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": 1000,
                "message": e
            })),
        )
            .into_response();
    }
    // Graph rules carry no SQL: compile the DAG into SQL + actions first.
    if rule.sql.trim().is_empty() {
        if let Some(ref graph) = rule.graph {
            match compile_graph_to_sql_and_actions(graph) {
                Ok((sql, actions)) => {
                    rule.sql = sql;
                    if rule.actions.is_empty() {
                        rule.actions = actions;
                    }
                }
                Err(e) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(json!({
                            "error": 1000,
                            "message": format!("Invalid rule graph: {}", e)
                        })),
                    )
                        .into_response();
                }
            }
        }
    }
    if rule.actions.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": 1000,
                "message": "invalid rule json: Missing rule actions."
            })),
        )
            .into_response();
    }
    let mut parser = Parser::new(&rule.sql);
    let select_stmt = match parser.parse_select() {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": 1000,
                    "message": format!("Invalid rule SQL: {}", e)
                })),
            )
                .into_response();
        }
    };
    if let Some(resp) = reject_invalid_rule(&state, &select_stmt, rule.options.as_ref(), false) {
        return resp;
    }

    let rule_id = rule.id.clone();

    if let Err(e) = state.rule_manager.create_rule(rule.clone()).await {
        let msg = if e.to_string().contains("already exists") {
            format!("Rule {} already exists", rule_id)
        } else {
            e.to_string()
        };
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": 1000,
                "message": msg
            })),
        )
            .into_response();
    }

    // Spawn window-aware rule execution task and register its handle so
    // stop/delete can abort it cleanly.
    spawn_rule_task(
        &state.rule_manager,
        &state.stream_bus,
        &state.stream_manager,
        &state.table_manager,
        &state.source_configs,
        &state.http_client,
        &state.trace_manager,
        &state.config,
        rule_id.clone(),
        select_stmt.clone(),
        rule.actions.clone(),
        rule.options.clone(),
    );

    bootstrap_rule_sources(&state, &rule_id, &select_stmt);

    (
        StatusCode::CREATED,
        format!("Rule {} was created successfully.", rule_id),
    )
        .into_response()
}

pub async fn get_rule(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if let Some(rule) = state.rule_manager.get_rule(&name) {
        Json(rule).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": 1002,
                "message": format!("Rule {} not found", name)
            })),
        )
            .into_response()
    }
}

pub async fn get_rule_status(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if let Some(status) = state.rule_manager.get_rule_status(&name) {
        let mut val = serde_json::to_value(&status).unwrap_or(Value::Null);
        if let Value::Object(ref mut map) = val {
            map.insert(
                "last_exception".to_string(),
                Value::String(status.last_exception.clone()),
            );
            map.insert(
                "exceptions_total".to_string(),
                Value::from(status.exceptions_total),
            );
        }
        Json(val).into_response()
    } else {
        (StatusCode::NOT_FOUND, format!("Rule {} not found", name)).into_response()
    }
}

pub async fn get_all_rule_status(State(state): State<AppState>) -> impl IntoResponse {
    let mut all = HashMap::new();
    for rule in state.rule_manager.list_rules() {
        if let Some(status) = state.rule_manager.get_rule_status(&rule.id) {
            let last_exc = if !status.last_exception.is_empty() {
                status.last_exception
            } else {
                status.message
            };
            all.insert(
                rule.id,
                json!({
                    "status": status.status,
                    "last_exception": last_exc,
                    "exceptions_total": status.exceptions_total,
                }),
            );
        }
    }
    Json(all)
}

/// Collect every called function name in an expression (including CASE
/// branches and analytic OVER calls).
pub fn collect_called_functions(expr: &Expr, out: &mut Vec<String>) {
    match expr {
        Expr::Call { name, args } => {
            out.push(name.clone());
            for arg in args {
                collect_called_functions(arg, out);
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_called_functions(left, out);
            collect_called_functions(right, out);
        }
        Expr::UnaryOp { expr, .. } => collect_called_functions(expr, out),
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_called_functions(expr, out);
            collect_called_functions(low, out);
            collect_called_functions(high, out);
        }
        Expr::InList { expr, list, .. } => {
            collect_called_functions(expr, out);
            for item in list {
                collect_called_functions(item, out);
            }
        }
        Expr::IsNull { expr, .. } => collect_called_functions(expr, out),
        Expr::FieldAccess { parent, .. } => collect_called_functions(parent, out),
        Expr::Index { base, index } => {
            collect_called_functions(base, out);
            collect_called_functions(index, out);
        }
        Expr::Slice { base, lo, hi } => {
            collect_called_functions(base, out);
            if let Some(e) = lo {
                collect_called_functions(e, out);
            }
            if let Some(e) = hi {
                collect_called_functions(e, out);
            }
        }
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            if let Some(op) = operand {
                collect_called_functions(op, out);
            }
            for (w, t) in when_clauses {
                collect_called_functions(w, out);
                collect_called_functions(t, out);
            }
            if let Some(e) = else_clause {
                collect_called_functions(e, out);
            }
        }
        Expr::Over {
            call,
            partition_by,
            when,
        } => {
            collect_called_functions(call, out);
            if let Some(p) = partition_by {
                collect_called_functions(p, out);
            }
            if let Some(w) = when {
                collect_called_functions(w, out);
            }
        }
        Expr::Wildcard | Expr::WildcardModified { .. } | Expr::Identifier(_) | Expr::Literal(_) => {
        }
    }
}

/// Every function called anywhere in a SELECT statement (projections,
/// filters, grouping, joins, set-operation branches).
pub fn stmt_called_functions(stmt: &SelectStmt) -> Vec<String> {
    let mut out = Vec::new();
    for field in &stmt.fields {
        collect_called_functions(field, &mut out);
    }
    if let Some(w) = &stmt.where_clause {
        collect_called_functions(w, &mut out);
    }
    for g in &stmt.group_by {
        collect_called_functions(g, &mut out);
    }
    if let Some(h) = &stmt.having {
        collect_called_functions(h, &mut out);
    }
    for item in &stmt.order_by {
        collect_called_functions(&item.expr, &mut out);
    }
    for join in &stmt.joins {
        if let Some(on) = &join.on {
            collect_called_functions(on, &mut out);
        }
    }
    if let Some((_, rhs)) = &stmt.set_op {
        out.extend(stmt_called_functions(rhs));
    }
    out
}

/// First called function unknown to the built-in library, the global UDF
/// registry and registered function/UDF plugin definitions, if any.
pub fn find_unknown_function(stmt: &SelectStmt, plugins: &PluginManager) -> Option<String> {
    let global = rekuiper_core::plugin::get_global_udf_registry();
    let plugin_defs: Vec<PluginDefinition> = plugins
        .list_plugins("function")
        .into_iter()
        .chain(plugins.list_plugins("udf"))
        .collect();
    for name in stmt_called_functions(stmt) {
        if is_builtin_function(&name) || global.has_udf(&name) || plugins.has_udf(&name) {
            continue;
        }
        if plugin_defs
            .iter()
            .flat_map(|d| d.functions.iter())
            .any(|f| f.eq_ignore_ascii_case(&name))
        {
            continue;
        }
        return Some(name);
    }
    None
}

/// 422 rejection when a rule calls an unknown function; `None` when clean.
pub fn check_rule_functions(state: &AppState, stmt: &SelectStmt) -> Option<Response> {
    find_unknown_function(stmt, &state.plugin_manager).map(|bad_fn| {
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            format!(
                "invalid rule json: Parse SQL ... error: function {} not found.",
                bad_fn
            ),
        )
            .into_response()
    })
}

pub fn validate_rule_options(options: &Option<HashMap<String, Value>>) -> Result<(), String> {
    let Some(opts) = options else {
        return Ok(());
    };
    for (k, v) in opts {
        match k.as_str() {
            "qos" => {
                if !v.is_i64() && !v.is_u64() {
                    return Err(
                        "invalid rule json: qos must be an integer (0, 1, or 2)".to_string()
                    );
                }
                let q = v.as_i64().unwrap_or(-1);
                if !(0..=2).contains(&q) {
                    return Err("invalid rule json: qos must be 0, 1, or 2".to_string());
                }
            }
            "debug" | "isEventTime" | "sendMetaToSink" | "sendNilField" | "sendError"
                if !v.is_boolean() =>
            {
                return Err(format!("invalid rule json: {} must be a boolean", k));
            }
            "concurrency" | "bufferLength" if !v.is_i64() && !v.is_u64() => {
                return Err(format!("invalid rule json: {} must be an integer", k));
            }
            _ => {}
        }
    }
    Ok(())
}

pub fn validate_sink_actions(actions: &[HashMap<String, Value>]) -> Result<(), String> {
    const SUPPORTED_FORMATS: &[&str] = &[
        "json",
        "binary",
        "delimited",
        "protobuf",
        "custom",
        "urlencoded",
    ];
    for action in actions {
        if let Some(tpl) = action.get("dataTemplate").and_then(|v| v.as_str()) {
            rekuiper_connectors::validate_data_template(tpl)?;
        }
        for (kind, opts) in action {
            if kind == "disabled" || crate::engine::sinks::is_action_disabled(action, opts) {
                continue;
            }
            if let Some(tpl) = opts.get("dataTemplate").and_then(|v| v.as_str()) {
                rekuiper_connectors::validate_data_template(tpl)?;
            }
            if let Some(fmt) = opts.get("format").and_then(|v| v.as_str()) {
                if !SUPPORTED_FORMATS
                    .iter()
                    .any(|&f| f.eq_ignore_ascii_case(fmt))
                {
                    return Err(format!("format type {} not supported", fmt));
                }
            }
            if kind.eq_ignore_ascii_case("mqtt") {
                let topic = opts.get("topic").and_then(|v| v.as_str()).unwrap_or("");
                if topic.is_empty() {
                    return Err("mqtt sink is missing property topic".to_string());
                }
                let has_conn_selector = opts
                    .get("connectionSelector")
                    .or_else(|| opts.get("resourceId"))
                    .or_else(|| opts.get("confKey"))
                    .or_else(|| opts.get("CONF_KEY"))
                    .and_then(|v| v.as_str())
                    .is_some_and(|s| !s.is_empty());
                let server = opts.get("server").and_then(|v| v.as_str()).unwrap_or("");
                if server.is_empty() && !has_conn_selector {
                    return Err("missing server property".to_string());
                }
            }
            if kind.eq_ignore_ascii_case("rest") || kind.eq_ignore_ascii_case("http") {
                if let Some(m) = opts.get("method").and_then(|v| v.as_str()) {
                    let m_upper = m.to_uppercase();
                    if !matches!(
                        m_upper.as_str(),
                        "GET" | "POST" | "PUT" | "DELETE" | "HEAD" | "PATCH"
                    ) {
                        return Err(format!("Not supported HTTP method {}.", m));
                    }
                }
                if let Some(bt) = opts.get("bodyType").and_then(|v| v.as_str()) {
                    if bt.eq_ignore_ascii_case("form") {
                        let fmt = opts.get("format").and_then(|v| v.as_str()).unwrap_or("");
                        if !fmt.eq_ignore_ascii_case("urlencoded") {
                            return Err("format must be urlencoded if bodyType is form".to_string());
                        }
                    }
                }
            }
            if kind.eq_ignore_ascii_case("influx") {
                let addr = opts.get("addr").and_then(|v| v.as_str()).unwrap_or("");
                if addr.is_empty() {
                    return Err("influx sink is missing property addr".to_string());
                }
                let database = opts.get("database").and_then(|v| v.as_str()).unwrap_or("");
                if database.is_empty() {
                    return Err("influx sink is missing property database".to_string());
                }
                let measurement = opts.get("measurement").and_then(|v| v.as_str()).unwrap_or("");
                if measurement.is_empty() {
                    return Err("influx sink is missing property measurement".to_string());
                }
            }
            if kind.eq_ignore_ascii_case("influx2") {
                let addr = opts.get("addr").and_then(|v| v.as_str()).unwrap_or("");
                if addr.is_empty() {
                    return Err("influx2 sink is missing property addr".to_string());
                }
                let org = opts.get("org").and_then(|v| v.as_str()).unwrap_or("");
                if org.is_empty() {
                    return Err("influx2 sink is missing property org".to_string());
                }
                let bucket = opts.get("bucket").and_then(|v| v.as_str()).unwrap_or("");
                if bucket.is_empty() {
                    return Err("influx2 sink is missing property bucket".to_string());
                }
                let measurement = opts.get("measurement").and_then(|v| v.as_str()).unwrap_or("");
                if measurement.is_empty() {
                    return Err("influx2 sink is missing property measurement".to_string());
                }
            }
        }
    }
    Ok(())
}

pub fn check_duplicate_fields(stmt: &SelectStmt) -> Option<Response> {
    let mut seen_fields = HashSet::new();
    let names = Evaluator::select_field_names(stmt);
    for (idx, field) in stmt.fields.iter().enumerate() {
        if matches!(field, Expr::Wildcard | Expr::WildcardModified { .. }) {
            continue;
        }
        let name = &names[idx];
        if !seen_fields.insert(name.clone()) {
            return Some(
                (
                    StatusCode::BAD_REQUEST,
                    format!("duplicate field definition {}", name),
                )
                    .into_response(),
            );
        }
    }
    None
}

/// Shared create/update gate: the source stream or table must exist and
/// every called function must be known. Returns the rejection response
/// when the rule is invalid.
pub fn reject_invalid_rule(
    state: &AppState,
    stmt: &SelectStmt,
    options: Option<&HashMap<String, Value>>,
    validation: bool,
) -> Option<Response> {
    if let Some(resp) = check_duplicate_fields(stmt) {
        return Some(resp);
    }
    if !stmt.group_by.is_empty() && stmt.window.is_none() {
        return Some(
            (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": 1000,
                    "message": "select stmt group by should be used with window"
                })),
            )
                .into_response(),
        );
    }
    let is_event_time = options
        .and_then(|opts| opts.get("isEventTime"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if is_event_time {
        if let Some(stream) = state.stream_manager.get_stream(&stmt.from) {
            let has_timestamp = stream
                .options
                .keys()
                .any(|k| k.eq_ignore_ascii_case("TIMESTAMP"));
            if !has_timestamp {
                return Some(
                    (
                        StatusCode::BAD_REQUEST,
                        Json(json!({
                            "error": 1000,
                            "message": "preprocessor is set to be event time but stream option TIMESTAMP not found"
                        })),
                    )
                        .into_response(),
                );
            }
        }
    }
    if !stmt.joins.is_empty() && stmt.window.is_none() {
        let has_stream_target = stmt
            .joins
            .iter()
            .any(|j| state.table_manager.get_table(&j.target).is_none());
        if has_stream_target {
            return Some(
                (
                    StatusCode::BAD_REQUEST,
                    Json(json!({
                        "error": 1000,
                        "message": "a time window or count window is required to join multiple streams"
                    })),
                )
                    .into_response(),
            );
        }
    }
    // Check multi-stream BUFFER_FULL_POLICY consistency across joined streams
    let mut stream_policies: Vec<(&str, String)> = Vec::new();
    let get_policy = |stream: &rekuiper_core::model::StreamDefinition| -> String {
        stream
            .options
            .iter()
            .find_map(|(k, v)| {
                if k.eq_ignore_ascii_case("buffer_full_policy") {
                    Some(v.clone())
                } else {
                    None
                }
            })
            .unwrap_or_else(|| "dropOldest".to_string())
    };

    if let Some(s) = state.stream_manager.get_stream(&stmt.from) {
        stream_policies.push((&stmt.from, get_policy(&s)));
    }
    for j in &stmt.joins {
        if let Some(s) = state.stream_manager.get_stream(&j.target) {
            stream_policies.push((&j.target, get_policy(&s)));
        }
    }

    if stream_policies.len() > 1 {
        let (first_name, ref first_policy) = stream_policies[0];
        for (stream_name, policy) in &stream_policies[1..] {
            if !policy.eq_ignore_ascii_case(first_policy) {
                return Some(
                    (
                        StatusCode::BAD_REQUEST,
                        Json(json!({
                            "error": 1000,
                            "message": format!(
                                "conflicting BUFFER_FULL_POLICY in joined streams: stream '{}' uses '{}', but stream '{}' uses '{}'",
                                first_name, first_policy, stream_name, policy
                            )
                        })),
                    )
                        .into_response(),
                );
            }
        }
    }
    // Validation uses 422 for unresolved sources/functions; creation/update use 400.
    // Both paths reject the same invalid rule rather than skipping the checks.
    let resolution_status = if validation {
        StatusCode::UNPROCESSABLE_ENTITY
    } else {
        StatusCode::BAD_REQUEST
    };
    let stream_exists = state.stream_manager.get_stream(&stmt.from).is_some()
        || state.table_manager.get_table(&stmt.from).is_some();
    if !stream_exists {
        return Some(
            (
                resolution_status,
                Json(json!({
                    "error": 1000,
                    "message": format!(
                        "fail to get stream {}, please check if stream is created",
                        stmt.from
                    )
                })),
            )
                .into_response(),
        );
    }
    if let Some(bad_fn) = find_unknown_function(stmt, &state.plugin_manager) {
        return Some(
            (
                resolution_status,
                Json(json!({
                    "error": 1000,
                    "message": format!(
                        "invalid rule json: Parse SQL ... error: function {} not found.",
                        bad_fn
                    )
                })),
            )
                .into_response(),
        );
    }
    None
}

pub async fn validate_rule(
    State(state): State<AppState>,
    Json(rule): Json<RuleDefinition>,
) -> Response {
    if let Err(e) = validate_rule_options(&rule.options) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": 1000,
                "message": e
            })),
        )
            .into_response();
    }
    if let Err(e) = validate_sink_actions(&rule.actions) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": 1000,
                "message": e
            })),
        )
            .into_response();
    }
    if rule.actions.is_empty() && rule.graph.is_none() {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({
                "error": 1000,
                "message": "invalid rule json: Missing rule actions."
            })),
        )
            .into_response();
    }
    if rule.sql.trim().is_empty() {
        if let Some(ref graph) = rule.graph {
            return match compile_graph_to_sql_and_actions(graph) {
                Ok((sql, _)) => {
                    let mut parser = Parser::new(&sql);
                    match parser.parse_select() {
                        Ok(stmt) => {
                            if let Some(resp) =
                                reject_invalid_rule(&state, &stmt, rule.options.as_ref(), true)
                            {
                                return resp;
                            }
                            check_rule_functions(&state, &stmt).unwrap_or_else(|| {
                                Json(json!({
                                    "sources": graph.topo.sources,
                                    "valid": true
                                }))
                                .into_response()
                            })
                        }
                        Err(e) => (
                            StatusCode::BAD_REQUEST,
                            Json(json!({
                                "error": 1000,
                                "message": format!("Invalid rule SQL: {}", e)
                            })),
                        )
                            .into_response(),
                    }
                }
                Err(e) => (
                    StatusCode::BAD_REQUEST,
                    Json(json!({
                        "error": 1000,
                        "message": format!("Invalid rule graph: {}", e)
                    })),
                )
                    .into_response(),
            };
        }
    }
    let mut parser = Parser::new(&rule.sql);
    match parser.parse_select() {
        Ok(stmt) => {
            if let Some(resp) = reject_invalid_rule(&state, &stmt, rule.options.as_ref(), true) {
                return resp;
            }
            check_rule_functions(&state, &stmt).unwrap_or_else(|| {
                Json(json!({
                    "sources": [stmt.from],
                    "valid": true
                }))
                .into_response()
            })
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": 1000,
                "message": format!("Invalid rule SQL: {}", e)
            })),
        )
            .into_response(),
    }
}

pub async fn get_rule_topo(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let Some(rule) = state.rule_manager.get_rule(&name) else {
        return (StatusCode::NOT_FOUND, format!("Rule {} not found", name)).into_response();
    };
    // Graph rules report their native DAG topology.
    if let Some(ref graph) = rule.graph {
        if !graph.nodes.is_empty() {
            return Json(json!({
                "sources": graph.topo.sources,
                "nodes": graph.nodes.keys().cloned().collect::<Vec<_>>(),
                "edges": graph.topo.edges,
            }))
            .into_response();
        }
    }
    let mut parser = Parser::new(&rule.sql);
    let select_stmt = match parser.parse_select() {
        Ok(s) => s,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("Invalid rule SQL: {}", e)).into_response();
        }
    };
    let from = select_stmt.from.clone();
    let source_node = format!("source_{}", from);
    let mut edges = serde_json::Map::new();
    edges.insert(source_node.clone(), json!(["op_eval"]));
    edges.insert("op_eval".to_string(), json!(["sink_actions"]));
    Json(json!({
        "sources": [from],
        "nodes": [source_node, "op_eval", "sink_actions"],
        "edges": edges,
    }))
    .into_response()
}

pub async fn get_rule_explain(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let Some(rule) = state.rule_manager.get_rule(&name) else {
        return (StatusCode::NOT_FOUND, format!("Rule {} not found", name)).into_response();
    };
    let mut parser = Parser::new(&rule.sql);
    let select_stmt = match parser.parse_select() {
        Ok(s) => s,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("Invalid rule SQL: {}", e)).into_response();
        }
    };
    let action_kinds: Vec<String> = rule
        .actions
        .iter()
        .flat_map(|a| a.keys().cloned())
        .collect();
    Json(json!({
        "rule": name,
        "source": select_stmt.from,
        "projection": select_stmt.fields.iter().map(expr_to_string).collect::<Vec<_>>(),
        "filter": select_stmt.where_clause.as_ref().map(expr_to_string),
        "window": select_stmt.window.as_ref().map(window_to_string),
        "groupBy": select_stmt.group_by.iter().map(expr_to_string).collect::<Vec<_>>(),
        "having": select_stmt.having.as_ref().map(expr_to_string),
        "actions": action_kinds,
    }))
    .into_response()
}

pub async fn get_rule_scantables(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let Some(rule) = state.rule_manager.get_rule(&name) else {
        return (StatusCode::NOT_FOUND, format!("Rule {} not found", name)).into_response();
    };
    let mut parser = Parser::new(&rule.sql);
    let select_stmt = match parser.parse_select() {
        Ok(s) => s,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("Invalid rule SQL: {}", e)).into_response();
        }
    };
    let mut results = Vec::new();
    if state.table_manager.get_table(&select_stmt.from).is_some() {
        let rows = state.table_manager.get_table_rows(&select_stmt.from);
        for row in rows {
            results.push(json!({
                "emitter": select_stmt.from,
                "content": row
            }));
        }
    }
    for join in &select_stmt.joins {
        if state.table_manager.get_table(&join.target).is_some() {
            let rows = state.table_manager.get_table_rows(&join.target);
            for row in rows {
                results.push(json!({
                    "emitter": join.target,
                    "content": row
                }));
            }
        }
    }
    Json(results).into_response()
}

pub fn time_unit_to_string(unit: &TimeUnit) -> &'static str {
    match unit {
        TimeUnit::Dd => "dd",
        TimeUnit::Hh => "hh",
        TimeUnit::Mi => "mi",
        TimeUnit::Ss => "ss",
        TimeUnit::Ms => "ms",
    }
}

pub fn window_to_string(window: &WindowDef) -> String {
    match window {
        WindowDef::TumblingTime { unit, length } => {
            format!("TUMBLINGWINDOW({}, {})", time_unit_to_string(unit), length)
        }
        WindowDef::HoppingTime {
            unit,
            length,
            interval,
        } => format!(
            "HOPPINGWINDOW({}, {}, {})",
            time_unit_to_string(unit),
            length,
            interval
        ),
        WindowDef::SlidingTime {
            unit,
            length,
            delay,
        } => match delay {
            Some(d) => format!(
                "SLIDINGWINDOW({}, {}, {})",
                time_unit_to_string(unit),
                length,
                d
            ),
            None => format!("SLIDINGWINDOW({}, {})", time_unit_to_string(unit), length),
        },
        WindowDef::Count { size, interval } => match interval {
            Some(i) => format!("COUNTWINDOW({}, {})", size, i),
            None => format!("COUNTWINDOW({})", size),
        },
        WindowDef::Session {
            unit,
            max_duration,
            timeout,
        } => format!(
            "SESSIONWINDOW({}, {}, {})",
            time_unit_to_string(unit),
            max_duration,
            timeout
        ),
        WindowDef::State {
            start_condition,
            end_condition,
        } => match end_condition {
            Some(end) => format!(
                "STATEWINDOW({}, {})",
                expr_to_string(start_condition),
                expr_to_string(end)
            ),
            None => format!("STATEWINDOW({})", expr_to_string(start_condition)),
        },
    }
}

pub fn expr_to_string(expr: &Expr) -> String {
    match expr {
        Expr::Wildcard => "*".to_string(),
        Expr::WildcardModified { except, replace } => {
            let mut s = "*".to_string();
            if !except.is_empty() {
                s.push_str(&format!(" EXCEPT({})", except.join(", ")));
            }
            if !replace.is_empty() {
                let reps: Vec<String> = replace
                    .iter()
                    .map(|(e, c)| format!("{} AS {}", expr_to_string(e), c))
                    .collect();
                s.push_str(&format!(" REPLACE({})", reps.join(", ")));
            }
            s
        }
        Expr::Identifier(name) => name.clone(),
        Expr::Literal(v) => v.to_string(),
        Expr::BinaryOp { left, op, right } => {
            let op_str = match op {
                rekuiper_sql::BinaryOperator::Eq => "=",
                rekuiper_sql::BinaryOperator::Neq => "!=",
                rekuiper_sql::BinaryOperator::Lt => "<",
                rekuiper_sql::BinaryOperator::Lte => "<=",
                rekuiper_sql::BinaryOperator::Gt => ">",
                rekuiper_sql::BinaryOperator::Gte => ">=",
                rekuiper_sql::BinaryOperator::And => "AND",
                rekuiper_sql::BinaryOperator::Or => "OR",
                rekuiper_sql::BinaryOperator::Add => "+",
                rekuiper_sql::BinaryOperator::Sub => "-",
                rekuiper_sql::BinaryOperator::Mul => "*",
                rekuiper_sql::BinaryOperator::Div => "/",
                rekuiper_sql::BinaryOperator::Mod => "%",
                rekuiper_sql::BinaryOperator::Like => "LIKE",
                rekuiper_sql::BinaryOperator::BitAnd => "&",
                rekuiper_sql::BinaryOperator::BitOr => "|",
                rekuiper_sql::BinaryOperator::BitXor => "^",
            };
            format!(
                "{} {} {}",
                expr_to_string(left),
                op_str,
                expr_to_string(right)
            )
        }
        Expr::UnaryOp { op, expr } => match op {
            rekuiper_sql::UnaryOperator::Not => format!("NOT {}", expr_to_string(expr)),
            rekuiper_sql::UnaryOperator::Neg => format!("-{}", expr_to_string(expr)),
        },
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => format!(
            "{} {}BETWEEN {} AND {}",
            expr_to_string(expr),
            if *negated { "NOT " } else { "" },
            expr_to_string(low),
            expr_to_string(high)
        ),
        Expr::InList {
            expr,
            list,
            negated,
        } => format!(
            "{} {}IN ({})",
            expr_to_string(expr),
            if *negated { "NOT " } else { "" },
            list.iter()
                .map(expr_to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Expr::IsNull { expr, negated } => format!(
            "{} IS {}NULL",
            expr_to_string(expr),
            if *negated { "NOT " } else { "" }
        ),
        Expr::FieldAccess { parent, field } => {
            format!("{}.{}", expr_to_string(parent), field)
        }
        Expr::Index { base, index } => {
            format!("{}[{}]", expr_to_string(base), expr_to_string(index))
        }
        Expr::Slice { base, lo, hi } => format!(
            "{}[{}:{}]",
            expr_to_string(base),
            lo.as_ref().map(|e| expr_to_string(e)).unwrap_or_default(),
            hi.as_ref().map(|e| expr_to_string(e)).unwrap_or_default()
        ),
        Expr::Call { name, args } => format!(
            "{}({})",
            name,
            args.iter()
                .map(expr_to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Expr::Case { .. } => "CASE ... END".to_string(),
        Expr::Over { call, .. } => format!("{} OVER (...)", expr_to_string(call)),
    }
}

pub fn activate_rule(state: &AppState, rule_id: &str) {
    if let Some(rule) = state.rule_manager.get_rule(rule_id) {
        let mut parser = Parser::new(&rule.sql);
        if let Ok(select_stmt) = parser.parse_select() {
            spawn_rule_task(
                &state.rule_manager,
                &state.stream_bus,
                &state.stream_manager,
                &state.table_manager,
                &state.source_configs,
                &state.http_client,
                &state.trace_manager,
                &state.config,
                rule_id.to_string(),
                select_stmt.clone(),
                rule.actions.clone(),
                rule.options.clone(),
            );
            bootstrap_rule_sources(state, rule_id, &select_stmt);
        }
    }
}

pub async fn start_rule(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if state.rule_manager.get_rule(&name).is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": 1002,
                "message": format!("Rule {} not found", name)
            })),
        )
            .into_response();
    }
    if let Some(status) = state.rule_manager.get_rule_status(&name) {
        if status.status == "running" {
            return (StatusCode::OK, format!("Rule {} was started", name)).into_response();
        }
    }
    match state.rule_manager.start_rule(&name).await {
        Ok(_) => {
            activate_rule(&state, &name);
            (StatusCode::OK, format!("Rule {} was started", name)).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

pub async fn stop_rule(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if state.rule_manager.get_rule(&name).is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": 1002,
                "message": format!("Rule {} not found", name)
            })),
        )
            .into_response();
    }
    if let Some(status) = state.rule_manager.get_rule_status(&name) {
        if status.status == "stopped" {
            return (StatusCode::OK, format!("Rule {} was stopped.", name)).into_response();
        }
    }
    match state.rule_manager.stop_rule(&name).await {
        Ok(_) => {
            cancel_rule_source(&state, &name);
            state.trace_manager.stop_trace(&name);
            (StatusCode::OK, format!("Rule {} was stopped.", name)).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

pub async fn restart_rule(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if state.rule_manager.get_rule(&name).is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": 1002,
                "message": format!("Rule {} not found", name)
            })),
        )
            .into_response();
    }
    match state.rule_manager.restart_rule(&name).await {
        Ok(_) => {
            cancel_rule_source(&state, &name);
            state.trace_manager.stop_trace(&name);
            activate_rule(&state, &name);
            (StatusCode::OK, format!("Rule {} was restarted", name)).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

pub async fn delete_rule(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if state.rule_manager.get_rule(&name).is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": 1002,
                "message": format!("Rule {} not found", name)
            })),
        )
            .into_response();
    }
    match state.rule_manager.delete_rule(&name).await {
        Ok(_) => {
            cancel_rule_source(&state, &name);
            state.trace_manager.stop_trace(&name);
            (StatusCode::OK, format!("Rule {} is dropped.", name)).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Replace a rule definition (eKuiper `PUT /rules/:name`): if running, the
/// definition is swapped atomically and the worker resumes processing with the
/// new SQL and actions. If stopped, the definition is updated in place while
/// keeping the rule stopped. Missing rules 404.
pub async fn update_rule(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(mut rule): Json<RuleDefinition>,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if state.rule_manager.get_rule(&name).is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": 1002,
                "message": format!("Rule {} not found", name)
            })),
        )
            .into_response();
    }
    if let Err(e) = validate_rule_options(&rule.options) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": 1000,
                "message": e
            })),
        )
            .into_response();
    }
    rule.id = name.clone();
    // Graph rules carry no SQL: compile the DAG first (mirrors creation).
    if rule.sql.trim().is_empty() {
        if let Some(ref graph) = rule.graph {
            match compile_graph_to_sql_and_actions(graph) {
                Ok((sql, actions)) => {
                    rule.sql = sql;
                    if rule.actions.is_empty() {
                        rule.actions = actions;
                    }
                }
                Err(e) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(json!({
                            "error": 1000,
                            "message": format!("Invalid rule graph: {}", e)
                        })),
                    )
                        .into_response();
                }
            }
        }
    }
    if rule.actions.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": 1000,
                "message": "invalid rule json: Missing rule actions."
            })),
        )
            .into_response();
    }
    let mut parser = Parser::new(&rule.sql);
    let select_stmt = match parser.parse_select() {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": 1000,
                    "message": format!("Invalid rule SQL: {}", e)
                })),
            )
                .into_response();
        }
    };
    if let Some(resp) = reject_invalid_rule(&state, &select_stmt, rule.options.as_ref(), false) {
        return resp;
    }
    if let Err(e) = validate_sink_actions(&rule.actions) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": 1000,
                "message": e
            })),
        )
            .into_response();
    }
    let was_running = match state.rule_manager.update_rule(rule.clone()).await {
        Ok(running) => running,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": 1000,
                    "message": e.to_string()
                })),
            )
                .into_response();
        }
    };
    cancel_rule_source(&state, &name);
    state.trace_manager.stop_trace(&name);
    if was_running {
        spawn_rule_task(
            &state.rule_manager,
            &state.stream_bus,
            &state.stream_manager,
            &state.table_manager,
            &state.source_configs,
            &state.http_client,
            &state.trace_manager,
            &state.config,
            name.clone(),
            select_stmt.clone(),
            rule.actions.clone(),
            rule.options.clone(),
        );
        bootstrap_rule_sources(&state, &name, &select_stmt);
    }
    (
        StatusCode::OK,
        format!("Rule {} was updated successfully.", name),
    )
        .into_response()
}

pub async fn get_rule_schema(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&id) {
        return resp;
    }
    let Some(rule) = state.rule_manager.get_rule(&id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": 1002,
                "message": format!("Rule {} not found", id)
            })),
        )
            .into_response();
    };
    let mut parser = Parser::new(&rule.sql);
    match parser.parse_select() {
        // Graph rules carry no SELECT SQL: report an empty schema.
        Err(_) => (StatusCode::OK, Json(json!({}))).into_response(),
        Ok(stmt) => (
            StatusCode::OK,
            Json(Value::Object(Evaluator::infer_select_schema(&stmt))),
        )
            .into_response(),
    }
}

pub async fn bulk_start_rules(State(state): State<AppState>, body: Bytes) -> impl IntoResponse {
    let target_tags = if !body.is_empty() {
        if let Ok(val) = serde_json::from_slice::<Value>(&body) {
            extract_tags_from_value(&val)
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };
    let mut results = Vec::new();
    for rule in state.rule_manager.list_rules() {
        if !target_tags.is_empty() && !target_tags.iter().any(|t| rule.tags.contains(t)) {
            continue;
        }
        match state.rule_manager.start_rule(&rule.id).await {
            Ok(_) => results.push(json!({ "ruleId": rule.id, "success": true })),
            Err(e) => {
                results.push(json!({ "ruleId": rule.id, "success": false, "error": e.to_string() }))
            }
        }
    }
    (StatusCode::OK, Json(results))
}

pub async fn bulk_stop_rules(State(state): State<AppState>, body: Bytes) -> impl IntoResponse {
    let target_tags = if !body.is_empty() {
        if let Ok(val) = serde_json::from_slice::<Value>(&body) {
            extract_tags_from_value(&val)
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };
    let mut results = Vec::new();
    for rule in state.rule_manager.list_rules() {
        if !target_tags.is_empty() && !target_tags.iter().any(|t| rule.tags.contains(t)) {
            continue;
        }
        match state.rule_manager.stop_rule(&rule.id).await {
            Ok(_) => {
                cancel_rule_source(&state, &rule.id);
                results.push(json!({ "ruleId": rule.id, "success": true }));
            }
            Err(e) => {
                results.push(json!({ "ruleId": rule.id, "success": false, "error": e.to_string() }))
            }
        }
    }
    (StatusCode::OK, Json(results))
}

pub async fn reset_rule_state(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let Some(rule) = state.rule_manager.get_rule(&name) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": 1002,
                "message": format!("Rule {} not found", name)
            })),
        )
            .into_response();
    };

    if !body.is_empty() {
        if let Ok(Value::Object(map)) = serde_json::from_slice::<Value>(&body) {
            if let Some(params) = map.get("params").and_then(|p| p.as_object()) {
                if let Some(target_stream) = params.get("streamName").and_then(|v| v.as_str()) {
                    let mut sources = Vec::new();
                    if let Some(ref graph) = rule.graph {
                        sources.extend(graph.topo.sources.clone());
                    }
                    if !rule.sql.trim().is_empty() {
                        let mut parser = Parser::new(&rule.sql);
                        if let Ok(stmt) = parser.parse_select() {
                            sources.push(stmt.from);
                            for j in stmt.joins {
                                sources.push(j.target);
                            }
                        }
                    }
                    if !sources.is_empty() && !sources.iter().any(|s| s == target_stream) {
                        return (
                            StatusCode::BAD_REQUEST,
                            Json(json!({
                                "message": format!("stream {} not found in topo", target_stream)
                            })),
                        )
                            .into_response();
                    }
                }
            }
        }
    }

    match state.rule_manager.reset_rule_metrics(&name) {
        Ok(_) => (StatusCode::OK, "success").into_response(),
        Err(e) => (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": 1002,
                "message": e.to_string()
            })),
        )
            .into_response(),
    }
}

/// Current process CPU percent and RSS bytes via sysinfo, falling back to
/// global CPU and used memory when the process handle is unavailable.
pub fn current_process_stats() -> (f64, u64) {
    let mut sys = System::new_all();
    sys.refresh_all();
    if let Some(p) = sysinfo::get_current_pid()
        .ok()
        .and_then(|id| sys.process(id))
    {
        (p.cpu_usage() as f64, p.memory())
    } else {
        (sys.global_cpu_info().cpu_usage() as f64, sys.used_memory())
    }
}

pub async fn get_rule_cpu(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&id) {
        return resp;
    }
    if state.rule_manager.get_rule(&id).is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": 1002,
                "message": format!("Rule {} not found", id)
            })),
        )
            .into_response();
    }
    let (cpu, memory) = current_process_stats();
    Json(json!({
        "rule_id": id,
        "cpu": cpu,
        "cpu_percent": cpu,
        "memory": memory,
        "memory_bytes": memory,
    }))
    .into_response()
}

pub async fn rule_cpu_usage(State(state): State<AppState>) -> impl IntoResponse {
    let (cpu, _) = current_process_stats();
    let mut map = serde_json::Map::new();
    for rule in state.rule_manager.list_rules() {
        // Per-rule CPU accounting is unavailable: running rules share the
        // process measurement, stopped rules report zero.
        let usage = if state
            .rule_manager
            .get_rule_status(&rule.id)
            .is_some_and(|s| s.status == "running")
        {
            cpu
        } else {
            0.0
        };
        map.insert(rule.id, json!(usage));
    }
    Json(Value::Object(map))
}

#[derive(Deserialize, Default)]
pub struct TagMatchQuery {
    tags: Option<String>,
    keys: Option<String>,
}

pub fn extract_tags_from_value(val: &Value) -> Vec<String> {
    if let Some(arr) = val.get("tags").and_then(|t| t.as_array()) {
        return arr
            .iter()
            .filter_map(|s| s.as_str().map(|s| s.to_string()))
            .collect();
    }
    if let Some(arr) = val.get("keys").and_then(|t| t.as_array()) {
        return arr
            .iter()
            .filter_map(|s| s.as_str().map(|s| s.to_string()))
            .collect();
    }
    if let Some(arr) = val.as_array() {
        return arr
            .iter()
            .filter_map(|s| s.as_str().map(|s| s.to_string()))
            .collect();
    }
    Vec::new()
}

pub async fn put_rule_tags(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let val: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    let new_tags = extract_tags_from_value(&val);
    match state
        .rule_manager
        .update_rule_tags(&name, |tags| {
            *tags = new_tags;
        })
        .await
    {
        Ok(_) => (StatusCode::OK, Json(json!({"message": "success"}))).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}

pub async fn patch_rule_tags(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let val: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    let add_tags = extract_tags_from_value(&val);
    match state
        .rule_manager
        .update_rule_tags(&name, |tags| {
            for t in add_tags {
                if !tags.contains(&t) {
                    tags.push(t);
                }
            }
        })
        .await
    {
        Ok(_) => (StatusCode::OK, Json(json!({"message": "success"}))).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}

pub async fn delete_rule_tags(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let val: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    let remove_tags = extract_tags_from_value(&val);
    match state
        .rule_manager
        .update_rule_tags(&name, |tags| {
            if remove_tags.is_empty() {
                tags.clear();
            } else {
                tags.retain(|t| !remove_tags.contains(t));
            }
        })
        .await
    {
        Ok(_) => (StatusCode::OK, Json(json!({"message": "success"}))).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}

pub async fn rule_tags_match(
    State(state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<TagMatchQuery>,
    body: Bytes,
) -> Response {
    let mut search_tags: HashSet<String> = HashSet::new();
    if let Some(t_str) = query.tags.or(query.keys) {
        for t in t_str.split(',') {
            let trimmed = t.trim();
            if !trimmed.is_empty() {
                search_tags.insert(trimmed.to_string());
            }
        }
    }
    if search_tags.is_empty() && !body.is_empty() {
        if let Ok(val) = serde_json::from_slice::<Value>(&body) {
            for t in extract_tags_from_value(&val) {
                search_tags.insert(t);
            }
        }
    }

    let mut matched: Vec<String> = Vec::new();
    for rule in state.rule_manager.list_rules() {
        if search_tags.is_empty() {
            continue;
        }
        if search_tags.iter().all(|t| rule.tags.contains(t)) {
            matched.push(rule.id.clone());
        }
    }
    matched.sort();
    Json(matched).into_response()
}

#[derive(Deserialize, Default)]
pub struct TraceStartBody {
    strategy: Option<String>,
}

pub async fn start_rule_trace(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if state.rule_manager.get_rule(&name).is_none() {
        return (StatusCode::NOT_FOUND, format!("Rule {} not found", name)).into_response();
    }
    let strategy = if !body.is_empty() {
        serde_json::from_slice::<TraceStartBody>(&body)
            .ok()
            .and_then(|b| b.strategy)
            .unwrap_or_else(|| "always".to_string())
    } else {
        "always".to_string()
    };
    state.trace_manager.start_trace(&name, strategy);
    (StatusCode::OK, Json(json!({"message": "success"}))).into_response()
}

pub async fn stop_rule_trace(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if state.rule_manager.get_rule(&name).is_none() {
        return (StatusCode::NOT_FOUND, format!("Rule {} not found", name)).into_response();
    }
    state.trace_manager.stop_trace(&name);
    (StatusCode::OK, Json(json!({"message": "success"}))).into_response()
}

#[derive(Deserialize, Default)]
pub struct TraceQuery {
    limit: Option<usize>,
}

pub async fn get_rule_traces(
    State(state): State<AppState>,
    Path(rule_id): Path<String>,
    axum::extract::Query(query): axum::extract::Query<TraceQuery>,
) -> Response {
    if let Err(resp) = check_valid_name(&rule_id) {
        return resp;
    }
    let ids = state
        .trace_manager
        .list_rule_trace_ids(&rule_id, query.limit);
    (StatusCode::OK, Json(ids)).into_response()
}

pub async fn get_trace_by_id(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&id) {
        return resp;
    }
    if let Some(span) = state.trace_manager.get_trace(&id) {
        (StatusCode::OK, Json(span)).into_response()
    } else {
        (StatusCode::NOT_FOUND, format!("trace {} not found\n", id)).into_response()
    }
}

pub async fn set_tracer_config(State(state): State<AppState>, body: Bytes) -> Response {
    if !body.is_empty() {
        if let Ok(cfg) = serde_json::from_slice::<TracerConfig>(&body) {
            state.trace_manager.set_tracer_config(cfg);
        }
    }
    (StatusCode::OK, Json(json!({"message": "success"}))).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_sink_actions_influx() {
        let invalid_influx = vec![HashMap::from([
            ("influx".to_string(), json!({
                "database": "db1",
                "measurement": "m1"
            }))
        ])];
        assert!(validate_sink_actions(&invalid_influx).is_err());

        let valid_influx = vec![HashMap::from([
            ("influx".to_string(), json!({
                "addr": "http://127.0.0.1:8086",
                "database": "db1",
                "measurement": "m1"
            }))
        ])];
        assert!(validate_sink_actions(&valid_influx).is_ok());

        let invalid_influx2 = vec![HashMap::from([
            ("influx2".to_string(), json!({
                "addr": "http://127.0.0.1:8086",
                "bucket": "b1",
                "measurement": "m1"
            }))
        ])];
        assert!(validate_sink_actions(&invalid_influx2).is_err());

        let valid_influx2 = vec![HashMap::from([
            ("influx2".to_string(), json!({
                "addr": "http://127.0.0.1:8086",
                "org": "o1",
                "bucket": "b1",
                "measurement": "m1"
            }))
        ])];
        assert!(validate_sink_actions(&valid_influx2).is_ok());
    }
}
