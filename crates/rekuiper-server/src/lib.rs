pub mod declarative;
pub mod engine;
pub mod handlers;
pub mod routes;
pub mod sink_cache;
pub mod state;

use anyhow::Result;
use parking_lot::RwLock;
use rekuiper_conf::KuiperConfig;
use rekuiper_core::{
    PluginManager, RuleManager, SchemaManager, SqliteKvStore, StreamBus, StreamManager,
    TableManager,
};
use routes::{
    create_router, load_config_maps, prometheus_metrics_handler, restore_running_rules,
    test_sse_router, AppState,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

#[derive(Debug, Clone, Default)]
pub struct ServerOptions {
    pub declarative_path: Option<std::path::PathBuf>,
    pub declarative_watch: bool,
}

pub async fn start_server(config: KuiperConfig, version: String) -> Result<()> {
    start_server_with_options(config, version, ServerOptions::default()).await
}

pub async fn start_server_with_options(
    config: KuiperConfig,
    version: String,
    options: ServerOptions,
) -> Result<()> {
    let init_start = Instant::now();
    let is_declarative = options.declarative_path.is_some();

    // Embedded KV persistence: honors `store.type` ("memory" -> MemKvStore,
    // default "sqlite" -> `data/sqliteKV.db`).
    let kv: Arc<dyn rekuiper_core::KvStore> = if config.store.r#type.eq_ignore_ascii_case("memory")
    {
        Arc::new(rekuiper_core::MemKvStore::new())
    } else {
        Arc::new(SqliteKvStore::new("data/sqliteKV.db").await?)
    };
    let stream_bus = StreamBus::new();
    let stream_manager = StreamManager::new_with_kv(kv.clone());
    let rule_manager = RuleManager::new_with_kv(stream_bus.clone(), kv.clone());
    let table_manager = TableManager::new_with_kv(kv.clone());
    let schema_manager = SchemaManager::new_with_kv(kv.clone());
    let plugin_manager = PluginManager::new_with_kv(kv.clone());

    let state = AppState {
        start_time: Instant::now(),
        version,
        config: Arc::new(RwLock::new(config.clone())),
        stream_manager,
        table_manager,
        rule_manager,
        schema_manager,
        plugin_manager,
        stream_bus,
        connections: Arc::new(RwLock::new(HashMap::new())),
        source_configs: Arc::new(RwLock::new(HashMap::new())),
        sink_configs: Arc::new(RwLock::new(HashMap::new())),
        config_op_lock: Arc::new(tokio::sync::Mutex::new(())),
        ruletests: Arc::new(RwLock::new(HashMap::new())),
        source_cancels: Arc::new(RwLock::new(HashMap::new())),
        stream_active_rules: Arc::new(RwLock::new(HashMap::new())),
        stream_source_cancels: Arc::new(RwLock::new(HashMap::new())),
        rule_streams: Arc::new(RwLock::new(HashMap::new())),
        stream_attach_meta: Arc::new(RwLock::new(HashMap::new())),
        stream_field_masks: Arc::new(RwLock::new(HashMap::new())),
        stream_rule_field_masks: Arc::new(RwLock::new(HashMap::new())),
        http_push_endpoints: Arc::new(RwLock::new(HashMap::new())),
        http_client: reqwest::Client::builder()
            .tcp_nodelay(true)
            .build()
            .unwrap_or_default(),
        trace_manager: routes::TraceManager::new(),
        task_manager: routes::TaskManager::new(),
        portable_plugins: routes::create_default_portables(),
        services: routes::create_default_services(),
        js_udfs: routes::create_default_js_udfs(),
        latest_import_status: Arc::new(RwLock::new(routes::default_import_status())),
        kv: Some(kv.clone()),
        declarative_mode: is_declarative,
        declarative_path: options.declarative_path.clone(),
        declarative_watch: options.declarative_watch,
        active_rule_hashes: Arc::new(RwLock::new(HashMap::new())),
        declarative_reload_failures: Arc::new(std::sync::atomic::AtomicU64::new(0)),
    };

    if let Some(ref d_path) = options.declarative_path {
        tracing::info!("Initializing server in Declarative Mode from {:?}", d_path);
        declarative::reconcile_declarative_path(&state, d_path)
            .await
            .map_err(|e| anyhow::anyhow!("Declarative manifest initialization failed: {}", e))?;
        if options.declarative_watch {
            declarative::spawn_declarative_watcher(state.clone(), d_path.clone());
        }
    } else {
        state.stream_manager.load_from_kv(&kv).await?;
        state.table_manager.load_from_kv(&kv).await?;
        state.rule_manager.load_from_kv(&kv).await?;
        state.schema_manager.load_from_kv(&kv).await?;
        state.plugin_manager.load_from_kv(&kv).await?;
        let proto_schemas = state
            .schema_manager
            .load_proto_dir(std::path::Path::new("etc/schemas"));
        if proto_schemas > 0 {
            tracing::info!("Loaded {} schemas from etc/schemas", proto_schemas);
        }
        load_config_maps(&state).await?;
        restore_running_rules(&state).await;
    }

    // Dedicated ruletest SSE listener serving the documented
    // `http://<httpServerIp>:<httpServerPort>/test/:id` endpoint. A bind
    // failure only warns (the main router still serves `/test/:id`); the
    // reported ruletest port always names this configured endpoint.
    {
        let sse_state = state.clone();
        let sse_ip = config.basic.http_server_ip.clone();
        let sse_port = config.basic.http_server_port;
        tokio::spawn(async move {
            let addr: SocketAddr = format!("{}:{}", sse_ip, sse_port)
                .parse()
                .unwrap_or_else(|_| SocketAddr::from(([127, 0, 0, 1], sse_port)));
            match tokio::net::TcpListener::bind(addr).await {
                Ok(listener) => {
                    tracing::info!("Serving ruletest SSE on port http://{}/test", addr);
                    if let Err(e) = axum::serve(listener, test_sse_router(sse_state)).await {
                        tracing::warn!("Ruletest SSE server error: {}", e);
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to bind ruletest SSE port {}: {}", sse_port, e);
                }
            }
        });
    }

    let app = create_router(state.clone())
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http());

    // Dedicated Prometheus listener when enabled on its own port, mirroring
    // eKuiper's prome_init behaviour.
    if config.basic.prometheus && config.basic.prometheus_port != config.basic.rest_port {
        let prom_port = config.basic.prometheus_port;
        let prom_app =
            axum::Router::new().route("/metrics", axum::routing::get(prometheus_metrics_handler));
        let prom_app = prom_app.with_state(state.clone());
        tokio::spawn(async move {
            let addr = SocketAddr::from(([0, 0, 0, 0], prom_port));
            match tokio::net::TcpListener::bind(addr).await {
                Ok(listener) => {
                    tracing::info!(
                        "Serving prometheus metrics on port http://localhost:{}/metrics",
                        prom_port
                    );
                    if let Err(e) = axum::serve(listener, prom_app).await {
                        tracing::warn!("Prometheus metrics server error: {}", e);
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to bind prometheus metrics port {}: {}",
                        prom_port,
                        e
                    );
                }
            }
        });
    }

    let addr: SocketAddr = format!("{}:{}", config.basic.rest_ip, config.basic.rest_port)
        .parse()
        .unwrap_or_else(|_| SocketAddr::from(([127, 0, 0, 1], config.basic.rest_port)));

    let rest_http_type = "http";
    println!(
        "Serving kuiper (version - {}) on port {}, and restful api on {}://{}.",
        state.version, config.basic.port, rest_http_type, addr
    );

    // Support EdgeX dual port (59720) concurrently alongside default port (9081)
    let edgex_port = 59720;
    if config.basic.rest_port != edgex_port {
        let edgex_app = app.clone();
        tokio::spawn(async move {
            let edgex_addr = SocketAddr::from(([0, 0, 0, 0], edgex_port));
            if let Ok(listener) = tokio::net::TcpListener::bind(edgex_addr).await {
                tracing::info!(
                    "Serving EdgeX dual-port REST API on http://0.0.0.0:{}",
                    edgex_port
                );
                let _ = axum::serve(listener, edgex_app).tcp_nodelay(true).await;
            }
        });
    }

    let listener = tokio::net::TcpListener::bind(addr).await?;
    println!("Server initialized and ready in {:?}", init_start.elapsed());
    axum::serve(listener, app).tcp_nodelay(true).await?;

    Ok(())
}
