pub use crate::engine::*;
pub use crate::handlers::*;
pub use crate::state::*;

use axum::{
    routing::{delete, get, post, put},
    Router,
};

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/ui", get(ui_handler))
        .route("/ui/", get(ui_handler))
        .route("/api/ui/auth-status", get(ui_auth_status))
        .route("/api/ui/login", post(ui_login))
        .route("/v1/metrics", post(push_otlp_data))
        .route("/v1/logs", post(push_otlp_data))
        .route("/v1/traces", post(push_otlp_data))
        .route("/otlp/v1/metrics", post(push_otlp_data))
        .route("/otlp/v1/logs", post(push_otlp_data))
        .route("/otlp/v1/traces", post(push_otlp_data))
        .route("/ping", get(ping_handler))
        .route("/version", get(root_handler))
        .route("/", get(root_handler).post(root_handler))
        .route("/streams", get(list_streams).post(create_stream))
        .route(
            "/streams/:name",
            get(get_stream).put(update_stream).delete(delete_stream),
        )
        .route("/streams/:name/data", post(push_stream_data))
        .route("/streams/:name/schema", get(get_stream_schema))
        .route("/tables", get(list_tables).post(create_table))
        .route(
            "/tables/:name",
            get(get_table).put(update_table).delete(delete_table),
        )
        .route("/tables/:name/data", post(push_table_data))
        .route("/tables/:name/schema", get(get_table_schema))
        .route("/tabledetails", get(get_table_details))
        .route("/streamdetails", get(get_stream_details))
        .route("/rules", get(list_rules).post(create_rule))
        .route("/rules/validate", post(validate_rule))
        .route("/rules/status/all", get(get_all_rule_status))
        .route(
            "/rules/:name",
            get(get_rule).put(update_rule).delete(delete_rule),
        )
        .route("/rules/:name/status", get(get_rule_status))
        .route(
            "/rules/:name/errors",
            get(get_rule_errors).delete(clear_rule_errors),
        )
        .route(
            "/rules/:name/dlq",
            get(get_rule_errors).delete(clear_rule_errors),
        )
        .route("/rules/:name/topo", get(get_rule_topo))
        .route("/rules/:name/explain", get(get_rule_explain))
        .route("/rules/:name/start", post(start_rule))
        .route("/rules/:name/stop", post(stop_rule))
        .route("/rules/:name/restart", post(restart_rule))
        .route("/rules/:name/reset_state", put(reset_rule_state))
        .route("/rules/:id/schema", get(get_rule_schema))
        .route("/rules/:id/cpu", get(get_rule_cpu))
        .route("/rules/:name/scantables", get(get_rule_scantables))
        .route(
            "/rules/:name/tags",
            put(put_rule_tags)
                .patch(patch_rule_tags)
                .delete(delete_rule_tags),
        )
        .route("/rules/:id/tap/:node_id/enable", post(enable_rule_tap))
        .route("/rules/:id/tap/:node_id/disable", post(disable_rule_tap))
        .route("/rules/:id/tap/:node_id/status", get(get_rule_tap_status))
        .route("/comms", get(comms_websocket_handler))
        .route("/v2/rules/:name/status", get(get_rule_status))
        .route("/ruletest", post(create_ruletest))
        .route("/ruletest/:name/start", post(start_ruletest))
        .route("/ruletest/:name", delete(delete_ruletest))
        .route("/test/:name", get(sse_ruletest))
        .route("/rules/:name/trace/start", post(start_rule_trace))
        .route("/rules/:name/trace/stop", post(stop_rule_trace))
        .route("/trace/rule/:rule_id", get(get_rule_traces))
        .route("/trace/:id", get(get_trace_by_id))
        .route("/tracer", post(set_tracer_config))
        .route("/async/data/import", post(async_data_import))
        .route("/async/task/:id", get(async_task_status))
        .route("/async/task/:id/cancel", post(async_task_cancelled))
        .route("/batch/req", post(handle_batch_req))
        .route("/rules/bulkstart", post(bulk_start_rules))
        .route("/rules/bulkstop", post(bulk_stop_rules))
        .route("/rules/usage/cpu", get(rule_cpu_usage))
        .route(
            "/rules/tags/match",
            get(rule_tags_match).post(rule_tags_match),
        )
        .route("/configs", get(get_configs).patch(patch_configs))
        .route("/config", get(get_configs).patch(patch_configs))
        .route(
            "/config/uploads",
            get(get_config_uploads).post(upload_config_file),
        )
        .route("/config/uploads/:name", delete(delete_config_upload))
        .route("/stop", get(stop_server).post(stop_server))
        .route("/data/import", post(import_data))
        .route("/data/export", get(export_data).post(export_data_selected))
        .route("/v2/data/import", post(import_v2_data))
        .route(
            "/v2/data/export",
            get(export_data_v2).post(export_data_selected),
        )
        .route("/ruleset/import", post(import_ruleset))
        .route("/ruleset/export", get(export_ruleset).post(export_ruleset))
        .route("/metadata/sources", get(list_source_metadata))
        .route("/metadata/sources/:name", get(get_source_metadata))
        .route("/metadata/sinks", get(list_sink_metadata))
        .route("/metadata/sinks/:name", get(get_sink_metadata))
        .route("/metadata/functions", get(list_function_metadata))
        .route("/metadata/operators", get(list_operator_metadata))
        .route("/metadata/palette", get(get_palette_metadata))
        .route("/api/v1/palette", get(get_palette_metadata))
        .route("/metadata/connections", get(list_metadata_connections))
        .route("/metadata/resource", get(list_metadata_resources))
        .route("/metadata/resources", get(list_metadata_resources))
        .route(
            "/connections",
            get(list_connections).post(create_connection),
        )
        .route(
            "/connections/:id",
            get(get_connection)
                .put(update_connection)
                .delete(delete_connection),
        )
        .route(
            "/plugins/sources",
            get(list_source_plugins).post(create_source_plugin),
        )
        .route("/plugins/sources/prebuild", get(list_prebuild_plugins))
        .route(
            "/plugins/sources/:name",
            get(get_source_plugin)
                .put(update_source_plugin)
                .delete(delete_source_plugin),
        )
        .route(
            "/plugins/sinks",
            get(list_sink_plugins).post(create_sink_plugin),
        )
        .route("/plugins/sinks/prebuild", get(list_prebuild_plugins))
        .route(
            "/plugins/sinks/:name",
            get(get_sink_plugin)
                .put(update_sink_plugin)
                .delete(delete_sink_plugin),
        )
        .route(
            "/plugins/functions",
            get(list_function_plugins).post(create_function_plugin),
        )
        .route("/plugins/functions/prebuild", get(list_prebuild_plugins))
        .route(
            "/plugins/functions/:name",
            get(get_function_plugin)
                .put(update_function_plugin)
                .delete(delete_function_plugin),
        )
        .route(
            "/plugins/functions/:name/register",
            post(register_function_plugin),
        )
        .route(
            "/plugins/portables",
            get(list_portable_plugins).post(create_portable_plugin),
        )
        .route(
            "/plugins/portables/:name",
            get(get_portable_plugin)
                .put(update_portable_plugin)
                .delete(delete_portable_plugin),
        )
        .route(
            "/plugins/portables/:name/status",
            get(get_portable_plugin_status),
        )
        .route(
            "/plugins/udfs",
            get(list_udf_plugins).post(create_udf_plugin),
        )
        .route(
            "/plugins/udfs/:name",
            get(get_udf_plugin).delete(delete_udf_plugin),
        )
        .route(
            "/plugins/wasm",
            get(list_wasm_plugins).post(create_wasm_plugin),
        )
        .route("/plugins/wasm/:name", delete(delete_wasm_plugin))
        .route("/services", get(list_services).post(create_service))
        .route(
            "/services/:name",
            get(get_service).put(update_service).delete(delete_service),
        )
        .route("/services/functions", get(list_service_functions))
        .route("/services/functions/:name", get(get_service_function))
        .route(
            "/udf/javascript",
            get(list_javascript_udfs).post(create_javascript_udf),
        )
        .route(
            "/udf/javascript/:id",
            get(get_javascript_udf)
                .put(update_javascript_udf)
                .delete(delete_javascript_udf),
        )
        .route("/schemas/:kind", get(list_schemas).post(create_schema))
        .route(
            "/schemas/:kind/:name",
            get(get_schema).put(update_schema).delete(delete_schema),
        )
        .route("/schemas/:kind/:name/upload", put(upload_schema))
        .route("/metadata/connections/:name", get(get_connection_metadata))
        .route("/metadata/sources/yaml/:name", get(get_source_yaml))
        .route(
            "/metadata/sources/:name/confKeys/:conf_key",
            get(get_source_conf_key)
                .put(save_source_conf_key)
                .post(save_source_conf_key)
                .delete(delete_source_conf_key),
        )
        .route("/metadata/sinks/yaml/:name", get(get_sink_yaml))
        .route(
            "/metadata/sinks/:name/confKeys/:conf_key",
            get(get_sink_conf_key)
                .put(save_sink_conf_key)
                .post(save_sink_conf_key)
                .delete(delete_sink_conf_key),
        )
        .route("/metadata/connections/yaml/:name", get(get_connection_yaml))
        .route(
            "/metadata/connections/:name/confKeys/:conf_key",
            get(get_connection_conf_key)
                .put(save_connection_conf_key)
                .post(save_connection_conf_key)
                .delete(delete_connection_conf_key),
        )
        .route(
            "/metadata/sources/connection/:name",
            post(register_source_connection),
        )
        .route(
            "/metadata/sinks/connection/:name",
            post(register_sink_connection),
        )
        .route(
            "/metadata/lookups/connection/:name",
            post(register_lookup_connection),
        )
        .route("/data/import/status", get(import_status))
        .route("/metrics/dump", get(metrics_dump))
        .route("/metrics/dump/check", get(metrics_dump_check))
        .route("/metrics", get(prometheus_metrics_handler))
        .route("/declarative/reload", post(declarative_reload_handler))
        .merge(crate::fleet::create_fleet_router(state.clone()))
        .fallback(create_router_fallback)
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            declarative_mutation_guard,
        ))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth_guard,
        ))
        .with_state(state)
}

pub async fn declarative_mutation_guard(
    axum::extract::State(state): axum::extract::State<AppState>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    if state.declarative_mode {
        let method = req.method();
        let path = req.uri().path();

        // Allow: All GET, HEAD, OPTIONS
        if method == axum::http::Method::GET
            || method == axum::http::Method::HEAD
            || method == axum::http::Method::OPTIONS
        {
            return next.run(req).await;
        }

        // Allow: POST /declarative/reload
        if path == "/declarative/reload" || path == "/declarative/reload/" {
            return next.run(req).await;
        }

        // Allow: Fleet Management Control Plane APIs & Proxy
        if path.starts_with("/api/v1/fleet/") || path == "/fleet/ws" {
            return next.run(req).await;
        }

        // Allow: POST /streams/:name/data and POST /tables/:name/data (data plane ingestion)
        if method == axum::http::Method::POST
            && (path.starts_with("/streams/") || path.starts_with("/tables/"))
            && path.ends_with("/data")
        {
            return next.run(req).await;
        }

        // Allow: POST /data/* and PUT /data/* (httppush data plane and selective export),
        // but strictly block /data/import control-plane mutations.
        if (method == axum::http::Method::POST || method == axum::http::Method::PUT)
            && path.starts_with("/data/")
            && path != "/data/import"
            && !path.starts_with("/data/import/")
        {
            return next.run(req).await;
        }

        // Allow: registered custom HTTP push endpoints
        if method == axum::http::Method::POST || method == axum::http::Method::PUT {
            let normalized = if path.len() > 1 && path.ends_with('/') {
                path.trim_end_matches('/')
            } else {
                path
            };
            let is_push_ep = {
                let guard = state.http_push_endpoints.read();
                guard.contains_key(normalized) || guard.contains_key(path)
            };
            if is_push_ep {
                return next.run(req).await;
            }
        }

        // Allow: POST /v1/* and POST /otlp/v1/* (OTLP data ingestion)
        if method == axum::http::Method::POST
            && (path.starts_with("/v1/") || path.starts_with("/otlp/v1/"))
        {
            return next.run(req).await;
        }

        // Allow: POST /ruletest* (ephemeral rule testing)
        if method == axum::http::Method::POST && path.starts_with("/ruletest") {
            return next.run(req).await;
        }

        // Allow: DELETE /rules/*/errors and DELETE /rules/*/dlq (operational DLQ clearing)
        if method == axum::http::Method::DELETE
            && path.starts_with("/rules/")
            && (path.ends_with("/errors") || path.ends_with("/dlq"))
        {
            return next.run(req).await;
        }

        // Block: every other mutating request with 403 Forbidden
        let body = serde_json::json!({
            "error": "declarative_mode_active",
            "message": "Instance is running in read-only Declarative Mode. Modify the manifest file to update resources."
        });
        return (axum::http::StatusCode::FORBIDDEN, axum::Json(body)).into_response();
    }

    next.run(req).await
}

pub async fn declarative_reload_handler(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    if !state.declarative_mode {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            axum::Json(serde_json::json!({
                "error": "not_in_declarative_mode",
                "message": "Instance is not running in Declarative Mode"
            })),
        )
            .into_response();
    }

    if let Some(ref path) = state.declarative_path {
        match crate::declarative::reconcile_declarative_path(&state, path).await {
            Ok(report) => (
                axum::http::StatusCode::OK,
                axum::Json(serde_json::json!(report)),
            )
                .into_response(),
            Err(e) => {
                state
                    .declarative_reload_failures
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                (
                    axum::http::StatusCode::BAD_REQUEST,
                    axum::Json(serde_json::json!({
                        "error": "declarative_reload_failed",
                        "message": e
                    })),
                )
                    .into_response()
            }
        }
    } else {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(serde_json::json!({
                "error": "no_declarative_path",
                "message": "Declarative path not configured"
            })),
        )
            .into_response()
    }
}

pub fn ip_matches_entry(client_ip: &std::net::IpAddr, entry: &str) -> bool {
    let entry = entry.trim();
    if entry.is_empty() {
        return false;
    }
    // Check CIDR (e.g. 192.168.1.0/24 or 10.0.0.0/8)
    if let Some((subnet_str, mask_str)) = entry.split_once('/') {
        if let (Ok(subnet), Ok(prefix_len)) = (
            subnet_str.parse::<std::net::IpAddr>(),
            mask_str.parse::<u8>(),
        ) {
            match (client_ip, subnet) {
                (std::net::IpAddr::V4(c), std::net::IpAddr::V4(s)) if prefix_len <= 32 => {
                    let mask = if prefix_len == 0 {
                        0
                    } else {
                        (!0u32) << (32 - prefix_len)
                    };
                    return (u32::from(*c) & mask) == (u32::from(s) & mask);
                }
                (std::net::IpAddr::V6(c), std::net::IpAddr::V6(s)) if prefix_len <= 128 => {
                    let mask = if prefix_len == 0 {
                        0
                    } else {
                        (!0u128) << (128 - prefix_len)
                    };
                    return (u128::from(*c) & mask) == (u128::from(s) & mask);
                }
                _ => {}
            }
        }
    }
    // Exact IP match (e.g. "127.0.0.1", "::1", "192.168.1.100")
    if let Ok(allowed_ip) = entry.parse::<std::net::IpAddr>() {
        return client_ip == &allowed_ip;
    }
    false
}

pub fn extract_client_ip(
    headers: &axum::http::HeaderMap,
    peer_addr: Option<std::net::SocketAddr>,
) -> Option<std::net::IpAddr> {
    let trust_forwarded = peer_addr.map(|a| a.ip().is_loopback()).unwrap_or(true);
    if trust_forwarded {
        if let Some(x_forwarded) = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
            if let Some(first_ip) = x_forwarded.split(',').next() {
                if let Ok(ip) = first_ip.trim().parse::<std::net::IpAddr>() {
                    return Some(ip);
                }
            }
        }
        if let Some(x_real_ip) = headers.get("x-real-ip").and_then(|v| v.to_str().ok()) {
            if let Ok(ip) = x_real_ip.trim().parse::<std::net::IpAddr>() {
                return Some(ip);
            }
        }
    }
    peer_addr.map(|a| a.ip())
}

pub async fn ui_handler(
    axum::extract::State(state): axum::extract::State<AppState>,
    req: axum::extract::Request,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let cfg = state.config.read();

    // 1. Check if Web UI is enabled (default is false for edge security)
    if !cfg.basic.enable_ui {
        return (
            axum::http::StatusCode::FORBIDDEN,
            [(axum::http::header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            "Web UI is disabled by default for security. Start rekuiper with --enable-ui or set 'basic.enableUi: true' in etc/kuiper.yaml to enable the web console.\n",
        )
            .into_response();
    }

    // 2. Check IP allowlist (if configured)
    if !cfg.basic.ui_ip_allowlist.is_empty() {
        let peer_addr = req
            .extensions()
            .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
            .map(|ci| ci.0);
        let Some(client_ip) = extract_client_ip(req.headers(), peer_addr) else {
            return (
                axum::http::StatusCode::FORBIDDEN,
                [(
                    axum::http::header::CONTENT_TYPE,
                    "text/plain; charset=utf-8",
                )],
                "Access denied: client IP could not be verified against ui_ip_allowlist\n",
            )
                .into_response();
        };
        let mut allowed = false;
        for entry in &cfg.basic.ui_ip_allowlist {
            if ip_matches_entry(&client_ip, entry) {
                allowed = true;
                break;
            }
        }
        if !allowed {
            return (
                axum::http::StatusCode::FORBIDDEN,
                [(axum::http::header::CONTENT_TYPE, "text/plain; charset=utf-8")],
                format!(
                    "Access denied: client IP {} is not permitted to access the web UI by ui_ip_allowlist\n",
                    client_ip
                ),
            )
                .into_response();
        }
    }

    drop(cfg);

    if let Ok(content) = std::fs::read_to_string("etc/ui.html") {
        return (
            [(axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8")],
            content,
        )
            .into_response();
    }
    (
        [(axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8")],
        include_str!("ui.html"),
    )
        .into_response()
}

pub async fn ui_auth_status(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let cfg = state.config.read();
    let auth_required = cfg.basic.authentication
        || cfg.basic.fleet_token.is_some()
        || cfg.basic.ui_password.is_some();
    (
        axum::http::StatusCode::OK,
        axum::Json(serde_json::json!({
            "auth_required": auth_required,
            "has_jwt": cfg.basic.authentication,
            "has_fleet_token": cfg.basic.fleet_token.is_some(),
            "has_ui_password": cfg.basic.ui_password.is_some(),
            "enable_ui": cfg.basic.enable_ui,
            "version": state.version,
        })),
    )
        .into_response()
}

#[derive(serde::Deserialize)]
pub struct UiLoginPayload {
    pub token: String,
}

pub async fn ui_login(
    axum::extract::State(state): axum::extract::State<AppState>,
    axum::Json(payload): axum::Json<UiLoginPayload>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let cfg = state.config.read();
    let token = payload.token.trim();

    // 1. Check custom UI password (constant-time)
    if let Some(ref pw) = cfg.basic.ui_password {
        if crate::state::constant_time_eq(token, pw) {
            return (
                axum::http::StatusCode::OK,
                axum::Json(serde_json::json!({ "status": "ok", "token": token })),
            )
                .into_response();
        }
    }

    // 2. Check fleet token (constant-time)
    if let Some(ref ft) = cfg.basic.fleet_token {
        if crate::state::constant_time_eq(token, ft) {
            return (
                axum::http::StatusCode::OK,
                axum::Json(serde_json::json!({ "status": "ok", "token": token })),
            )
                .into_response();
        }
    }

    // 3. Check JWT token
    if cfg.basic.authentication {
        if let Some(key_der) = load_auth_public_key() {
            if verify_jwt_raw(token, &key_der).is_ok() {
                return (
                    axum::http::StatusCode::OK,
                    axum::Json(serde_json::json!({ "status": "ok", "token": token })),
                )
                    .into_response();
            }
        }
    }

    (
        axum::http::StatusCode::UNAUTHORIZED,
        axum::Json(serde_json::json!({
            "error": "invalid_credentials",
            "message": "Invalid console password or authentication token"
        })),
    )
        .into_response()
}
