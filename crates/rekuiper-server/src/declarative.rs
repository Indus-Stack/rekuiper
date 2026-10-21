use crate::engine::{
    bootstrap_graph_sources, cancel_rule_source, spawn_graph_rule_task, spawn_rule_task,
};
use crate::handlers::streams::to_stream_fields;
use crate::state::AppState;
use rekuiper_core::model::{
    DeclarativeManifest, DeclarativeRules, EdgeTargets, RuleDefinition, StreamDefinition,
    TableDefinition,
};
use rekuiper_sql::parser::Parser;
use rekuiper_sql::validator::validate_manifest;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

fn update_ctx_str(ctx: &mut ring::digest::Context, s: &str) {
    ctx.update(&(s.len() as u64).to_le_bytes());
    ctx.update(s.as_bytes());
}

fn finish_ctx_u64(ctx: ring::digest::Context) -> u64 {
    let digest = ctx.finish();
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&digest.as_ref()[..8]);
    u64::from_le_bytes(bytes)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconcileReport {
    pub status: String,
    pub streams_created: usize,
    pub streams_updated: usize,
    pub streams_deleted: usize,
    pub tables_created: usize,
    pub tables_updated: usize,
    pub tables_deleted: usize,
    pub rules_started: usize,
    pub rules_restarted: usize,
    pub rules_unchanged: usize,
    pub rules_stopped: usize,
}

impl Default for ReconcileReport {
    fn default() -> Self {
        Self {
            status: "reloaded".to_string(),
            streams_created: 0,
            streams_updated: 0,
            streams_deleted: 0,
            tables_created: 0,
            tables_updated: 0,
            tables_deleted: 0,
            rules_started: 0,
            rules_restarted: 0,
            rules_unchanged: 0,
            rules_stopped: 0,
        }
    }
}

/// Compute a deterministic SHA-256 composite content hash for a rule taking into account:
/// 1. Rule definition (id, name, version, sql, canonicalized actions, canonicalized options, graph, tags)
/// 2. Referenced stream DDLs
/// 3. Referenced table DDLs
/// 4. Referenced connection configurations
/// 5. Referenced schemas
pub fn compute_rule_effective_hash(rule: &RuleDefinition, manifest: &DeclarativeManifest) -> u64 {
    let mut ctx = ring::digest::Context::new(&ring::digest::SHA256);

    // 1. Rule definition (convert HashMaps via serde_json::to_value first so keys are sorted in BTreeMap order)
    update_ctx_str(&mut ctx, &rule.id);
    if let Some(ref name) = rule.name {
        update_ctx_str(&mut ctx, name);
    }
    if let Some(ref version) = rule.version {
        update_ctx_str(&mut ctx, version);
    }
    update_ctx_str(&mut ctx, &rule.sql);
    if let Ok(actions_val) = serde_json::to_value(&rule.actions) {
        if let Ok(actions_json) = serde_json::to_string(&actions_val) {
            update_ctx_str(&mut ctx, &actions_json);
        }
    }
    if let Ok(options_val) = serde_json::to_value(&rule.options) {
        if let Ok(options_json) = serde_json::to_string(&options_val) {
            update_ctx_str(&mut ctx, &options_json);
        }
    }
    if let Ok(graph_val) = serde_json::to_value(&rule.graph) {
        if let Ok(graph_json) = serde_json::to_string(&graph_val) {
            update_ctx_str(&mut ctx, &graph_json);
        }
    }
    for tag in &rule.tags {
        update_ctx_str(&mut ctx, tag);
    }

    // 2. Parse rule SQL to find referenced streams and tables
    let mut parser = Parser::new(&rule.sql);
    if let Ok(stmt) = parser.parse_select() {
        let from_source = stmt.from.trim().trim_matches('`').trim_matches('"');
        let from_lower = from_source.to_ascii_lowercase();
        update_ctx_str(&mut ctx, &from_lower);

        // Check if stream DDL declared
        for (s_name, s_ddl) in &manifest.streams {
            if s_name.eq_ignore_ascii_case(&from_lower) {
                update_ctx_str(&mut ctx, s_ddl);
            }
        }
        for (t_name, t_ddl) in &manifest.tables {
            if t_name.eq_ignore_ascii_case(&from_lower) {
                update_ctx_str(&mut ctx, t_ddl);
            }
        }

        // Joins
        for join in &stmt.joins {
            let target = join.target.trim().trim_matches('`').trim_matches('"');
            let target_lower = target.to_ascii_lowercase();
            update_ctx_str(&mut ctx, &target_lower);
            for (s_name, s_ddl) in &manifest.streams {
                if s_name.eq_ignore_ascii_case(&target_lower) {
                    update_ctx_str(&mut ctx, s_ddl);
                }
            }
            for (t_name, t_ddl) in &manifest.tables {
                if t_name.eq_ignore_ascii_case(&target_lower) {
                    update_ctx_str(&mut ctx, t_ddl);
                }
            }
        }
    }

    // 3. Hash connections and schemas (BTreeMap in DeclarativeManifest iterates in deterministic sorted key order)
    for (conn_name, conn_val) in &manifest.connections {
        update_ctx_str(&mut ctx, &conn_name.to_ascii_lowercase());
        if let Ok(s) = serde_json::to_string(conn_val) {
            update_ctx_str(&mut ctx, &s);
        }
    }
    for (schema_name, schema_def) in &manifest.schemas {
        update_ctx_str(&mut ctx, &schema_name.to_ascii_lowercase());
        if let Some(ref content) = schema_def.content {
            update_ctx_str(&mut ctx, content);
        }
    }

    finish_ctx_u64(ctx)
}

/// Reconciles an incoming DeclarativeManifest against current AppState,
/// with the Transactional Snapshot Watchdog enabled (default 30s window).
pub async fn reconcile_manifest(
    state: &AppState,
    manifest: &DeclarativeManifest,
) -> Result<ReconcileReport, String> {
    reconcile_manifest_with_watchdog(state, manifest, std::time::Duration::from_secs(30), 1).await
}

pub async fn reconcile_manifest_with_watchdog(
    state: &AppState,
    manifest: &DeclarativeManifest,
    watchdog_window: std::time::Duration,
    spike_threshold: u64,
) -> Result<ReconcileReport, String> {
    let has_existing = !state.rule_manager.list_rules().is_empty()
        || !state.stream_manager.list_streams().is_empty();

    let prev_manifest = if has_existing {
        let current = capture_current_manifest(state);
        let _ = store_known_good_snapshot(state, &current).await;
        current
    } else if let Ok(Some(existing)) = load_known_good_snapshot(state).await {
        existing
    } else {
        manifest.clone()
    };

    // 2. Perform reconciliation
    let report = reconcile_manifest_internal(state, manifest).await?;

    // 3. Spawn Transactional Snapshot Watchdog only if there was prior topology to protect
    if has_existing {
        spawn_snapshot_watchdog(
            state.clone(),
            prev_manifest,
            watchdog_window,
            spike_threshold,
        );
    } else {
        let _ = store_known_good_snapshot(state, manifest).await;
    }

    Ok(report)
}

/// Internal manifest reconciliation (called directly during rollback to avoid recursive watchdogs).
pub async fn reconcile_manifest_internal(
    state: &AppState,
    manifest: &DeclarativeManifest,
) -> Result<ReconcileReport, String> {
    // Step A: Pure offline validation before touching any running state
    let _summary = validate_manifest(manifest).map_err(|diagnostics| {
        let msgs: Vec<String> = diagnostics.into_iter().map(|d| d.to_string()).collect();
        format!(
            "Manifest validation failed with {} error(s):\n{}",
            msgs.len(),
            msgs.join("\n")
        )
    })?;

    let mut report = ReconcileReport::default();

    // Step B: Reconcile Connections with secret preservation & interpolation
    {
        let mut conns_guard = state.connections.write();
        for (name, val) in &manifest.connections {
            let mut conn_val = val.clone();
            if let Some(existing) = conns_guard.get(name) {
                crate::secrets::preserve_masked_secrets(&mut conn_val, existing);
            }
            crate::secrets::interpolate_secrets(&mut conn_val, state.kv.as_deref());
            conns_guard.insert(name.clone(), conn_val);
        }
    }

    // Step C: Reconcile Schemas
    for schema_def in manifest.schemas.values() {
        let _ = state
            .schema_manager
            .register_schema(schema_def.clone())
            .await;
    }

    // Step D: Reconcile Streams
    let existing_streams: HashMap<String, StreamDefinition> = state
        .stream_manager
        .list_streams()
        .into_iter()
        .filter_map(|name| state.stream_manager.get_stream(&name).map(|s| (name, s)))
        .collect();

    for (stream_name, ddl) in &manifest.streams {
        let mut parser = Parser::new(ddl);
        if let Ok(stmt) = parser.parse_create_stream() {
            let def = StreamDefinition {
                name: stream_name.clone(),
                sql: ddl.clone(),
                stream_fields: to_stream_fields(stmt.fields),
                options: stmt.options,
            };

            if let Some(existing) = existing_streams.get(stream_name) {
                if existing.sql != *ddl {
                    let _ = state.stream_manager.delete_stream(stream_name).await;
                    let _ = state.stream_manager.create_stream(def).await;
                    state.stream_bus.get_or_create(stream_name);
                    report.streams_updated += 1;
                }
            } else {
                let _ = state.stream_manager.create_stream(def).await;
                state.stream_bus.get_or_create(stream_name);
                report.streams_created += 1;
            }
        }
    }

    // Delete streams no longer in manifest
    for old_stream in existing_streams.keys() {
        if !manifest.streams.contains_key(old_stream) {
            let _ = state.stream_manager.delete_stream(old_stream).await;
            report.streams_deleted += 1;
        }
    }

    // Step E: Reconcile Tables
    let existing_tables: HashMap<String, TableDefinition> = state
        .table_manager
        .list_tables()
        .into_iter()
        .filter_map(|name| state.table_manager.get_table(&name).map(|t| (name, t)))
        .collect();

    for (table_name, ddl) in &manifest.tables {
        let mut parser = Parser::new(ddl);
        if let Ok(stmt) = parser.parse_create_table() {
            let def = TableDefinition {
                name: table_name.clone(),
                sql: ddl.clone(),
                stream_fields: to_stream_fields(stmt.fields),
                options: stmt.options,
            };

            if let Some(existing) = existing_tables.get(table_name) {
                if existing.sql != *ddl {
                    let _ = state.table_manager.delete_table(table_name).await;
                    let _ = state.table_manager.create_table(def).await;
                    report.tables_updated += 1;
                }
            } else {
                let _ = state.table_manager.create_table(def).await;
                report.tables_created += 1;
            }
        }
    }

    for old_table in existing_tables.keys() {
        if !manifest.tables.contains_key(old_table) {
            let _ = state.table_manager.delete_table(old_table).await;
            report.tables_deleted += 1;
        }
    }

    // Step F: Reconcile Rules with Smart Diffing
    let incoming_rules = manifest
        .rules
        .clone()
        .into_rules_map()
        .map_err(|e| format!("Failed to parse rules from manifest: {}", e))?;

    let existing_rule_ids: Vec<String> = state
        .rule_manager
        .list_rules()
        .into_iter()
        .map(|r| r.id)
        .collect();

    for (rule_id, rule_def) in &incoming_rules {
        let mut resolved_rule_def = rule_def.clone();
        if let Some(existing_rule) = state.rule_manager.get_rule(rule_id) {
            let mut inc_val =
                serde_json::to_value(&resolved_rule_def).unwrap_or(serde_json::Value::Null);
            let ex_val = serde_json::to_value(&existing_rule).unwrap_or(serde_json::Value::Null);
            crate::secrets::preserve_masked_secrets(&mut inc_val, &ex_val);
            crate::secrets::interpolate_secrets(&mut inc_val, state.kv.as_deref());
            if let Ok(deser) = serde_json::from_value::<RuleDefinition>(inc_val) {
                resolved_rule_def = deser;
            }
        } else {
            let mut inc_val =
                serde_json::to_value(&resolved_rule_def).unwrap_or(serde_json::Value::Null);
            crate::secrets::interpolate_secrets(&mut inc_val, state.kv.as_deref());
            if let Ok(deser) = serde_json::from_value::<RuleDefinition>(inc_val) {
                resolved_rule_def = deser;
            }
        }

        let effective_hash = compute_rule_effective_hash(&resolved_rule_def, manifest);
        let stored_hash = state.active_rule_hashes.read().get(rule_id).copied();
        let is_running = state
            .rule_manager
            .get_rule_status(rule_id)
            .map(|s| s.status == "running")
            .unwrap_or(false);

        if stored_hash == Some(effective_hash) && is_running {
            // Unchanged: leave rule actor untouched!
            report.rules_unchanged += 1;
            continue;
        }

        // Rule is new or modified: stop old version if present
        if existing_rule_ids.contains(rule_id) {
            cancel_rule_source(state, rule_id);
            let _ = state.rule_manager.stop_rule(rule_id).await;
            let _ = state.rule_manager.delete_rule(rule_id).await;
            report.rules_restarted += 1;
        } else {
            report.rules_started += 1;
        }

        // Handle native visual DAG rules or parse SQL
        if resolved_rule_def.sql.trim().is_empty() {
            if let Some(ref graph) = resolved_rule_def.graph {
                let is_native_dag =
                    graph.nodes.values().any(|n| {
                        matches!(n.node_type.as_str(), "switch" | "rate_limit" | "watchdog")
                    }) || graph
                        .topo
                        .edges
                        .values()
                        .any(|e| matches!(e, EdgeTargets::Multi(_)));

                if is_native_dag {
                    let _ = state
                        .rule_manager
                        .create_rule(resolved_rule_def.clone())
                        .await;
                    let buffer_len = resolved_rule_def
                        .options
                        .as_ref()
                        .and_then(|o| o.get("bufferLength"))
                        .and_then(|v| v.as_u64())
                        .map(|n| n as usize)
                        .unwrap_or(1024);

                    spawn_graph_rule_task(
                        &state.rule_manager,
                        &state.stream_bus,
                        rule_id.clone(),
                        graph.clone(),
                        state.comms_hub.clone(),
                        buffer_len,
                    );
                    bootstrap_graph_sources(state, rule_id, graph);

                    state
                        .active_rule_hashes
                        .write()
                        .insert(rule_id.clone(), effective_hash);
                    continue;
                }
            }
        }

        // Parse and start new rule actor
        let mut parser = Parser::new(&resolved_rule_def.sql);
        let select_stmt = parser
            .parse_select()
            .map_err(|e| format!("Failed to parse SQL for rule '{}': {}", rule_id, e))?;

        let _ = state
            .rule_manager
            .create_rule(resolved_rule_def.clone())
            .await;

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
            select_stmt,
            resolved_rule_def.actions.clone(),
            resolved_rule_def.options.clone(),
        );

        state
            .active_rule_hashes
            .write()
            .insert(rule_id.clone(), effective_hash);
    }

    // Stop and delete rules no longer present in manifest
    for old_rule_id in existing_rule_ids {
        if !incoming_rules.contains_key(&old_rule_id) {
            cancel_rule_source(state, &old_rule_id);
            let _ = state.rule_manager.stop_rule(&old_rule_id).await;
            let _ = state.rule_manager.delete_rule(&old_rule_id).await;
            state.active_rule_hashes.write().remove(&old_rule_id);
            report.rules_stopped += 1;
        }
    }

    Ok(report)
}

/// Load manifest from path on a blocking thread and reconcile against state.
pub async fn reconcile_declarative_path(
    state: &AppState,
    path: &Path,
) -> Result<ReconcileReport, String> {
    let path_buf = path.to_path_buf();
    let load_res =
        tokio::task::spawn_blocking(move || DeclarativeManifest::load_from_path(&path_buf, false))
            .await
            .map_err(|e| format!("Manifest loader task panicked: {}", e))?;

    let manifest = load_res.map_err(|diagnostics| {
        let msgs: Vec<String> = diagnostics.into_iter().map(|d| d.to_string()).collect();
        format!(
            "Failed to load manifest at {:?}:\n{}",
            path,
            msgs.join("\n")
        )
    })?;

    reconcile_manifest(state, &manifest).await
}

/// Compute a SHA-256 fingerprint of a path (handles atomic rename / inode swaps and directory additions).
pub fn compute_path_fingerprint(path: &Path) -> u64 {
    let mut ctx = ring::digest::Context::new(&ring::digest::SHA256);
    let mut any_hashed = false;

    if path.is_file() {
        if let Ok(bytes) = std::fs::read(path) {
            ctx.update(&(bytes.len() as u64).to_le_bytes());
            ctx.update(&bytes);
            any_hashed = true;
        }
    } else if path.is_dir() {
        if let Ok(entries) = std::fs::read_dir(path) {
            let mut files: Vec<PathBuf> =
                entries.filter_map(|e| e.ok().map(|x| x.path())).collect();
            files.sort();
            for f in files {
                if let Some(ext) = f.extension().and_then(|s| s.to_str()) {
                    let ext_lower = ext.to_lowercase();
                    if ext_lower == "yaml"
                        || ext_lower == "yml"
                        || ext_lower == "json"
                        || ext_lower == "toml"
                    {
                        if let Some(fname) = f.file_name().and_then(|n| n.to_str()) {
                            update_ctx_str(&mut ctx, fname);
                        }
                        if let Ok(bytes) = std::fs::read(&f) {
                            ctx.update(&(bytes.len() as u64).to_le_bytes());
                            ctx.update(&bytes);
                            any_hashed = true;
                        }
                    }
                }
            }
        }
    }

    if !any_hashed {
        return 0;
    }
    finish_ctx_u64(ctx)
}

/// Spawn a background watcher task that polls the target path via `spawn_blocking`.
pub fn spawn_declarative_watcher(state: AppState, path: PathBuf) {
    tokio::spawn(async move {
        tracing::info!("Starting declarative manifest watcher on {:?}", path);
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(1000));
        let p_init = path.clone();
        let mut last_hash = tokio::task::spawn_blocking(move || compute_path_fingerprint(&p_init))
            .await
            .unwrap_or(0);

        loop {
            interval.tick().await;
            let p_check = path.clone();
            let current_hash =
                tokio::task::spawn_blocking(move || compute_path_fingerprint(&p_check))
                    .await
                    .unwrap_or(0);
            if current_hash != 0 && current_hash != last_hash {
                tracing::info!(
                    "Declarative manifest change detected at {:?}, triggering hot reload",
                    path
                );
                match reconcile_declarative_path(&state, &path).await {
                    Ok(report) => {
                        tracing::info!("Declarative hot reload succeeded: {:?}", report);
                        last_hash = current_hash;
                    }
                    Err(e) => {
                        state
                            .declarative_reload_failures
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        tracing::error!(
                            "Declarative hot reload failed (keeping current topology running): {}",
                            e
                        );
                        // Update hash to prevent spamming errors on every tick until the user touches the file again
                        last_hash = current_hash;
                    }
                }
            }
        }
    });
}

/// Captures a complete DeclarativeManifest from the running AppState.
pub fn capture_current_manifest(state: &AppState) -> DeclarativeManifest {
    let mut connections = std::collections::BTreeMap::new();
    for (k, v) in state.connections.read().iter() {
        connections.insert(k.clone(), v.clone());
    }

    let mut schemas = std::collections::BTreeMap::new();
    for kind in &["protobuf", "custom"] {
        for name in state.schema_manager.list_schemas(kind) {
            if let Some(s) = state.schema_manager.get_schema(kind, &name) {
                schemas.insert(format!("{}/{}", kind, name), s);
            }
        }
    }

    let mut streams = std::collections::BTreeMap::new();
    for s_name in state.stream_manager.list_streams() {
        if let Some(def) = state.stream_manager.get_stream(&s_name) {
            streams.insert(s_name, def.sql);
        }
    }

    let mut tables = std::collections::BTreeMap::new();
    for t_name in state.table_manager.list_tables() {
        if let Some(def) = state.table_manager.get_table(&t_name) {
            tables.insert(t_name, def.sql);
        }
    }

    let rules = DeclarativeRules::List(state.rule_manager.list_rules());

    DeclarativeManifest {
        version: "v1".to_string(),
        connections,
        schemas,
        streams,
        tables,
        rules,
    }
}

/// Stores the known-good snapshot in the persistent KV store under `snapshots/known_good`.
pub async fn store_known_good_snapshot(
    state: &AppState,
    manifest: &DeclarativeManifest,
) -> Result<(), String> {
    let json_str = serde_json::to_string(manifest).map_err(|e| e.to_string())?;
    if let Some(kv) = &state.kv {
        kv.set("snapshots", "known_good", &json_str)
            .await
            .map_err(|e| format!("Failed to save known-good snapshot to KV: {}", e))?;
    }
    Ok(())
}

/// Loads the known-good snapshot from the persistent KV store under `snapshots/known_good`.
pub async fn load_known_good_snapshot(
    state: &AppState,
) -> Result<Option<DeclarativeManifest>, String> {
    if let Some(kv) = &state.kv {
        if let Ok(Some(json_str)) = kv.get("snapshots", "known_good").await {
            let manifest = serde_json::from_str::<DeclarativeManifest>(&json_str)
                .map_err(|e| format!("Failed to parse known-good snapshot from KV: {}", e))?;
            return Ok(Some(manifest));
        }
    }
    Ok(None)
}

/// Automatically rolls back to the previous known-good snapshot stored in KV.
pub async fn rollback_to_known_good(state: &AppState) -> Result<ReconcileReport, String> {
    let snapshot = load_known_good_snapshot(state)
        .await?
        .ok_or_else(|| "No known-good snapshot found in KV store".to_string())?;

    tracing::warn!("Rolling back to known-good snapshot!");
    reconcile_manifest_internal(state, &snapshot).await
}

/// Calculates the total count of DLQ error records across all currently configured rules.
pub fn get_total_dlq_errors(state: &AppState) -> u64 {
    state
        .rule_manager
        .list_rules()
        .iter()
        .filter_map(|r| state.rule_manager.get_rule_dlq_stats(&r.id))
        .map(|s| s.total_errors)
        .sum()
}

/// Inspects all rules for unexpected panics or crash exceptions.
pub fn check_rule_panics(state: &AppState) -> Option<String> {
    for r in state.rule_manager.list_rules() {
        if let Some(status) = state.rule_manager.get_rule_status(&r.id) {
            if status.status == "stopped" {
                if !status.last_exception.is_empty() {
                    return Some(format!(
                        "Rule '{}' crashed with exception: {}",
                        r.id, status.last_exception
                    ));
                }
                if !status.message.is_empty() {
                    let msg_low = status.message.to_lowercase();
                    if msg_low.contains("panic")
                        || msg_low.contains("error")
                        || msg_low.contains("crash")
                        || msg_low.contains("halt")
                    {
                        return Some(format!("Rule '{}' halted: {}", r.id, status.message));
                    }
                }
            }
        }
    }
    None
}

/// Spawns the Transactional Snapshot Watchdog background task.
/// Monitors rule panics and DLQ error spikes for `window` duration. If triggered,
/// automatically reverts to `prev_manifest` and publishes a CRITICAL CommsHub alert.
pub fn spawn_snapshot_watchdog(
    state: AppState,
    prev_manifest: DeclarativeManifest,
    window: std::time::Duration,
    spike_threshold: u64,
) {
    tokio::spawn(async move {
        let start = std::time::Instant::now();
        let baseline_dlq = get_total_dlq_errors(&state);
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(200));

        while start.elapsed() < window {
            interval.tick().await;

            // 1. Check for rule panics or fatal exceptions
            if let Some(panic_reason) = check_rule_panics(&state) {
                *state.last_watchdog_rollback.write() = Some(format!(
                    "Snapshot watchdog triggered rollback: {}",
                    panic_reason
                ));
                tracing::error!(
                    "Transactional Snapshot Watchdog detected rule failure: {}. Rolling back to previous snapshot!",
                    panic_reason
                );
                state.comms_hub.publish_alert(
                    "watchdog",
                    format!("Snapshot watchdog triggered rollback: {}", panic_reason),
                    "CRITICAL",
                    None,
                );
                if let Err(e) = reconcile_manifest_internal(&state, &prev_manifest).await {
                    tracing::error!("Watchdog rollback failed: {}", e);
                }
                return;
            }

            // 2. Check for DLQ error spikes
            let current_dlq = get_total_dlq_errors(&state);
            let spike = current_dlq.saturating_sub(baseline_dlq);
            if spike >= spike_threshold {
                *state.last_watchdog_rollback.write() = Some(format!(
                    "Snapshot watchdog triggered rollback: DLQ error spike detected ({} errors)",
                    spike
                ));
                tracing::error!(
                    "Transactional Snapshot Watchdog detected DLQ spike ({} new errors >= threshold {}). Rolling back to previous snapshot!",
                    spike,
                    spike_threshold
                );
                state.comms_hub.publish_alert(
                    "watchdog",
                    format!(
                        "Snapshot watchdog triggered rollback: DLQ error spike detected ({} errors)",
                        spike
                    ),
                    "CRITICAL",
                    None,
                );
                if let Err(e) = reconcile_manifest_internal(&state, &prev_manifest).await {
                    tracing::error!("Watchdog rollback failed: {}", e);
                }
                return;
            }
        }

        // Window passed cleanly: confirm snapshot as healthy
        tracing::info!(
            "Transactional Snapshot Watchdog window elapsed successfully; snapshot confirmed healthy."
        );
        let healthy_manifest = capture_current_manifest(&state);
        let _ = store_known_good_snapshot(&state, &healthy_manifest).await;
    });
}
