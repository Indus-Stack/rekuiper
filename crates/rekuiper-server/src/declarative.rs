use crate::engine::spawn_rule_task;
use crate::handlers::streams::to_stream_fields;
use crate::state::AppState;
use rekuiper_core::model::{
    DeclarativeManifest, RuleDefinition, StreamDefinition, TableDefinition,
};
use rekuiper_sql::parser::Parser;
use rekuiper_sql::validator::validate_manifest;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

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

/// Compute a composite content hash for a rule taking into account:
/// 1. Rule definition (id, sql, actions, options)
/// 2. Referenced stream DDLs
/// 3. Referenced table DDLs
/// 4. Referenced connection configurations
/// 5. Referenced schemas
pub fn compute_rule_effective_hash(rule: &RuleDefinition, manifest: &DeclarativeManifest) -> u64 {
    let mut hasher = DefaultHasher::new();

    // 1. Rule definition
    rule.id.hash(&mut hasher);
    rule.sql.hash(&mut hasher);
    if let Ok(actions_json) = serde_json::to_string(&rule.actions) {
        actions_json.hash(&mut hasher);
    }
    if let Ok(options_json) = serde_json::to_string(&rule.options) {
        options_json.hash(&mut hasher);
    }

    // 2. Parse rule SQL to find referenced streams and tables
    let mut parser = Parser::new(&rule.sql);
    if let Ok(stmt) = parser.parse_select() {
        let from_source = stmt.from.trim().trim_matches('`').trim_matches('"');
        let from_lower = from_source.to_ascii_lowercase();
        from_lower.hash(&mut hasher);

        // Check if stream DDL declared
        for (s_name, s_ddl) in &manifest.streams {
            if s_name.eq_ignore_ascii_case(&from_lower) {
                s_ddl.hash(&mut hasher);
            }
        }
        for (t_name, t_ddl) in &manifest.tables {
            if t_name.eq_ignore_ascii_case(&from_lower) {
                t_ddl.hash(&mut hasher);
            }
        }

        // Joins
        for join in &stmt.joins {
            let target = join.target.trim().trim_matches('`').trim_matches('"');
            let target_lower = target.to_ascii_lowercase();
            target_lower.hash(&mut hasher);
            for (s_name, s_ddl) in &manifest.streams {
                if s_name.eq_ignore_ascii_case(&target_lower) {
                    s_ddl.hash(&mut hasher);
                }
            }
            for (t_name, t_ddl) in &manifest.tables {
                if t_name.eq_ignore_ascii_case(&target_lower) {
                    t_ddl.hash(&mut hasher);
                }
            }
        }
    }

    // 3. Hash connections and schemas
    for (conn_name, conn_val) in &manifest.connections {
        conn_name.to_ascii_lowercase().hash(&mut hasher);
        if let Ok(s) = serde_json::to_string(conn_val) {
            s.hash(&mut hasher);
        }
    }
    for (schema_name, schema_def) in &manifest.schemas {
        schema_name.to_ascii_lowercase().hash(&mut hasher);
        schema_def.content.hash(&mut hasher);
    }

    hasher.finish()
}

/// Atomically validates and reconciles an incoming DeclarativeManifest against current AppState.
/// If validation fails, NO running rules or streams are modified.
pub async fn reconcile_manifest(
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

    // Step B: Reconcile Connections
    {
        let mut conns_guard = state.connections.write();
        for (name, val) in &manifest.connections {
            conns_guard.insert(name.clone(), val.clone());
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
        let effective_hash = compute_rule_effective_hash(rule_def, manifest);
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
            let _ = state.rule_manager.stop_rule(rule_id).await;
            let _ = state.rule_manager.delete_rule(rule_id).await;
            report.rules_restarted += 1;
        } else {
            report.rules_started += 1;
        }

        // Parse and start new rule actor
        let mut parser = Parser::new(&rule_def.sql);
        let select_stmt = parser
            .parse_select()
            .map_err(|e| format!("Failed to parse SQL for rule '{}': {}", rule_id, e))?;

        let _ = state.rule_manager.create_rule(rule_def.clone()).await;

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
            rule_def.actions.clone(),
            rule_def.options.clone(),
        );

        state
            .active_rule_hashes
            .write()
            .insert(rule_id.clone(), effective_hash);
    }

    // Stop and delete rules no longer present in manifest
    for old_rule_id in existing_rule_ids {
        if !incoming_rules.contains_key(&old_rule_id) {
            let _ = state.rule_manager.stop_rule(&old_rule_id).await;
            let _ = state.rule_manager.delete_rule(&old_rule_id).await;
            state.active_rule_hashes.write().remove(&old_rule_id);
            report.rules_stopped += 1;
        }
    }

    Ok(report)
}

/// Load manifest from path and reconcile against state.
pub async fn reconcile_declarative_path(
    state: &AppState,
    path: &Path,
) -> Result<ReconcileReport, String> {
    let manifest = DeclarativeManifest::load_from_path(path, false).map_err(|diagnostics| {
        let msgs: Vec<String> = diagnostics.into_iter().map(|d| d.to_string()).collect();
        format!(
            "Failed to load manifest at {:?}:\n{}",
            path,
            msgs.join("\n")
        )
    })?;

    reconcile_manifest(state, &manifest).await
}

/// Compute a fingerprint of a path (handles atomic rename / inode swaps and directory additions)
pub fn compute_path_fingerprint(path: &Path) -> u64 {
    let mut hasher = DefaultHasher::new();
    if path.is_file() {
        if let Ok(meta) = path.metadata() {
            meta.len().hash(&mut hasher);
            if let Ok(mtime) = meta.modified() {
                mtime.hash(&mut hasher);
            }
        }
        if let Ok(bytes) = std::fs::read(path) {
            bytes.hash(&mut hasher);
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
                        f.file_name().hash(&mut hasher);
                        if let Ok(meta) = f.metadata() {
                            meta.len().hash(&mut hasher);
                            if let Ok(mtime) = meta.modified() {
                                mtime.hash(&mut hasher);
                            }
                        }
                        if let Ok(bytes) = std::fs::read(&f) {
                            bytes.hash(&mut hasher);
                        }
                    }
                }
            }
        }
    }
    hasher.finish()
}

/// Spawn a background watcher task that polls the target path for modifications / inode swaps.
pub fn spawn_declarative_watcher(state: AppState, path: PathBuf) {
    tokio::spawn(async move {
        tracing::info!("Starting declarative manifest watcher on {:?}", path);
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(1000));
        let mut last_hash = compute_path_fingerprint(&path);

        loop {
            interval.tick().await;
            let current_hash = compute_path_fingerprint(&path);
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
