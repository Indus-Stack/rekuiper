use crate::kv::KvStore;
use crate::model::{
    RuleDefinition, RuleStatus, SchemaDefinition, StreamDefinition, TableDefinition,
};
use crate::runtime::StreamBus;
use anyhow::{bail, Result};
use parking_lot::RwLock;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;

#[derive(Clone, Default)]
pub struct StreamManager {
    streams: Arc<RwLock<HashMap<String, StreamDefinition>>>,
    kv: Option<Arc<dyn KvStore>>,
    op_lock: Arc<AsyncMutex<()>>,
}

impl StreamManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_with_kv(kv: Arc<dyn KvStore>) -> Self {
        Self {
            streams: Arc::new(RwLock::new(HashMap::new())),
            kv: Some(kv),
            op_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    async fn persist(&self, namespace: &str, key: &str, val: &str) -> Result<()> {
        if let Some(kv) = &self.kv {
            kv.set(namespace, key, val).await?;
        }
        Ok(())
    }

    async fn unpersist(&self, namespace: &str, key: &str) -> Result<()> {
        if let Some(kv) = &self.kv {
            kv.delete(namespace, key).await?;
        }
        Ok(())
    }

    pub async fn create_stream(&self, def: StreamDefinition) -> Result<()> {
        let _guard = self.op_lock.lock().await;
        {
            let map = self.streams.read();
            if map.contains_key(&def.name) {
                bail!("Stream {} already exists", def.name);
            }
        }
        let snapshot = serde_json::to_string(&def)?;
        self.persist("streams", &def.name, &snapshot).await?;
        self.streams.write().insert(def.name.clone(), def);
        Ok(())
    }

    pub async fn update_stream(&self, def: StreamDefinition) -> Result<()> {
        let _guard = self.op_lock.lock().await;
        {
            let map = self.streams.read();
            if !map.contains_key(&def.name) {
                bail!("Stream {} not found", def.name);
            }
        }
        let snapshot = serde_json::to_string(&def)?;
        self.persist("streams", &def.name, &snapshot).await?;
        self.streams.write().insert(def.name.clone(), def);
        Ok(())
    }

    pub fn get_stream(&self, name: &str) -> Option<StreamDefinition> {
        self.streams.read().get(name).cloned()
    }

    pub fn list_streams(&self) -> Vec<String> {
        self.streams.read().keys().cloned().collect()
    }

    pub async fn delete_stream(&self, name: &str) -> Result<()> {
        let _guard = self.op_lock.lock().await;
        {
            let map = self.streams.read();
            if !map.contains_key(name) {
                bail!("Stream {} not found", name);
            }
        }
        self.unpersist("streams", name).await?;
        self.streams.write().remove(name);
        Ok(())
    }

    /// Repopulates definitions previously stored under the `streams`
    /// namespace, skipping corrupt entries.
    pub async fn load_from_kv(&self, kv: &Arc<dyn KvStore>) -> Result<()> {
        for (key, val) in kv.list_all("streams").await? {
            match serde_json::from_str::<StreamDefinition>(&val) {
                Ok(def) => {
                    self.streams.write().insert(def.name.clone(), def);
                }
                Err(e) => {
                    tracing::warn!("Skipping corrupt stream entry {}: {}", key, e);
                }
            }
        }
        Ok(())
    }
}

pub type TableRow = HashMap<String, Value>;

#[derive(Clone, Default)]
pub struct TableManager {
    tables: Arc<RwLock<HashMap<String, TableDefinition>>>,
    rows: Arc<RwLock<HashMap<String, Vec<TableRow>>>>,
    kv: Option<Arc<dyn KvStore>>,
    op_lock: Arc<AsyncMutex<()>>,
}

impl TableManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_with_kv(kv: Arc<dyn KvStore>) -> Self {
        Self {
            tables: Arc::new(RwLock::new(HashMap::new())),
            rows: Arc::new(RwLock::new(HashMap::new())),
            kv: Some(kv),
            op_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    async fn persist(&self, namespace: &str, key: &str, val: &str) -> Result<()> {
        if let Some(kv) = &self.kv {
            kv.set(namespace, key, val).await?;
        }
        Ok(())
    }

    async fn unpersist(&self, namespace: &str, key: &str) -> Result<()> {
        if let Some(kv) = &self.kv {
            kv.delete(namespace, key).await?;
        }
        Ok(())
    }

    /// Appends or updates a lookup row in a table.
    /// When KEY / PRIMARY_KEY option is configured on the table, matches are replaced in-place.
    /// When RETAIN_SIZE is configured, oldest rows beyond RETAIN_SIZE are pruned.
    pub fn insert_table_row(&self, table: &str, row: HashMap<String, Value>) {
        let def = self.get_table(table);
        let key_col = def.as_ref().and_then(|d| {
            d.options
                .iter()
                .find(|(k, _)| {
                    k.eq_ignore_ascii_case("KEY") || k.eq_ignore_ascii_case("PRIMARY_KEY")
                })
                .map(|(_, v)| v.clone())
        });
        let retain_size: Option<usize> = def.as_ref().and_then(|d| {
            d.options
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("RETAIN_SIZE"))
                .and_then(|(_, v)| v.parse().ok())
        });
        let mut rows_map = self.rows.write();
        let rows = rows_map.entry(table.to_string()).or_default();
        if let Some(ref k) = key_col {
            if let Some(target_val) = row.get(k) {
                if let Some(pos) = rows.iter().position(|r| r.get(k) == Some(target_val)) {
                    rows[pos] = row;
                    return;
                }
            }
        }
        rows.push(row);
        let max_size = retain_size.unwrap_or(if key_col.is_some() { usize::MAX } else { 100 });
        while rows.len() > max_size {
            rows.remove(0);
        }
    }

    /// Returns all lookup rows stored for a table (empty when none).
    pub fn get_table_rows(&self, table: &str) -> Vec<HashMap<String, Value>> {
        self.rows.read().get(table).cloned().unwrap_or_default()
    }

    pub async fn create_table(&self, def: TableDefinition) -> Result<()> {
        let _guard = self.op_lock.lock().await;
        {
            let map = self.tables.read();
            if map.contains_key(&def.name) {
                bail!("Table {} already exists", def.name);
            }
        }
        let snapshot = serde_json::to_string(&def)?;
        self.persist("tables", &def.name, &snapshot).await?;
        self.tables.write().insert(def.name.clone(), def);
        Ok(())
    }

    pub async fn update_table(&self, def: TableDefinition) -> Result<()> {
        let _guard = self.op_lock.lock().await;
        {
            let map = self.tables.read();
            if !map.contains_key(&def.name) {
                bail!("Table {} not found", def.name);
            }
        }
        let snapshot = serde_json::to_string(&def)?;
        self.persist("tables", &def.name, &snapshot).await?;
        self.tables.write().insert(def.name.clone(), def);
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Option<TableDefinition> {
        self.tables.read().get(name).cloned()
    }

    pub fn list_tables(&self) -> Vec<String> {
        self.tables.read().keys().cloned().collect()
    }

    pub fn list_table_definitions(&self) -> Vec<TableDefinition> {
        self.tables.read().values().cloned().collect()
    }

    pub async fn delete_table(&self, name: &str) -> Result<()> {
        let _guard = self.op_lock.lock().await;
        {
            let map = self.tables.read();
            if !map.contains_key(name) {
                bail!("Table {} not found", name);
            }
        }
        self.unpersist("tables", name).await?;
        self.tables.write().remove(name);
        self.rows.write().remove(name);
        Ok(())
    }

    /// Repopulates table definitions previously stored under the `tables`
    /// namespace, skipping corrupt entries. Lookup rows are runtime data and
    /// are intentionally not restored.
    pub async fn load_from_kv(&self, kv: &Arc<dyn KvStore>) -> Result<()> {
        for (key, val) in kv.list_all("tables").await? {
            match serde_json::from_str::<TableDefinition>(&val) {
                Ok(def) => {
                    self.tables.write().insert(def.name.clone(), def);
                }
                Err(e) => {
                    tracing::warn!("Skipping corrupt table entry {}: {}", key, e);
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct SchemaManager {
    schemas: Arc<RwLock<HashMap<String, SchemaDefinition>>>,
    kv: Option<Arc<dyn KvStore>>,
    op_lock: Arc<AsyncMutex<()>>,
}

impl SchemaManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_with_kv(kv: Arc<dyn KvStore>) -> Self {
        Self {
            schemas: Arc::new(RwLock::new(HashMap::new())),
            kv: Some(kv),
            op_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    fn storage_key(kind: &str, name: &str) -> String {
        format!("{}/{}", kind, name)
    }

    pub async fn register_schema(&self, def: SchemaDefinition) -> Result<()> {
        let _guard = self.op_lock.lock().await;
        let key = Self::storage_key(&def.kind, &def.name);
        let snapshot = serde_json::to_string(&def)?;
        if let Some(kv) = &self.kv {
            kv.set("schemas", &key, &snapshot).await?;
        }
        self.schemas.write().insert(key, def);
        Ok(())
    }

    pub fn get_schema(&self, kind: &str, name: &str) -> Option<SchemaDefinition> {
        self.schemas
            .read()
            .get(&Self::storage_key(kind, name))
            .cloned()
    }

    /// Sorted schema names registered under one kind.
    pub fn list_schemas(&self, kind: &str) -> Vec<String> {
        let mut names: Vec<String> = self
            .schemas
            .read()
            .values()
            .filter(|def| def.kind == kind)
            .map(|def| def.name.clone())
            .collect();
        names.sort();
        names
    }

    pub async fn delete_schema(&self, kind: &str, name: &str) -> Result<()> {
        let _guard = self.op_lock.lock().await;
        let key = Self::storage_key(kind, name);
        {
            let map = self.schemas.read();
            if !map.contains_key(&key) {
                bail!("Schema {}/{} not found", kind, name);
            }
        }
        if let Some(kv) = &self.kv {
            kv.delete("schemas", &key).await?;
        }
        self.schemas.write().remove(&key);
        Ok(())
    }

    /// Repopulates schemas previously stored under the `schemas` namespace,
    /// skipping corrupt entries.
    pub async fn load_from_kv(&self, kv: &Arc<dyn KvStore>) -> Result<()> {
        for (key, val) in kv.list_all("schemas").await? {
            match serde_json::from_str::<SchemaDefinition>(&val) {
                Ok(def) => {
                    self.schemas.write().insert(key, def);
                }
                Err(e) => {
                    tracing::warn!("Skipping corrupt schema entry {}: {}", key, e);
                }
            }
        }
        Ok(())
    }

    /// Seeds the registry from `etc/schemas/{kind}/*.proto`-style trees:
    /// each immediate subdirectory is a kind, each `*.proto` file a schema
    /// named by its file stem. Missing trees load nothing.
    pub fn load_proto_dir(&self, base: &std::path::Path) -> usize {
        let kinds = match std::fs::read_dir(base) {
            Ok(entries) => entries,
            Err(_) => return 0,
        };
        let mut loaded = 0;
        for kind_entry in kinds.flatten() {
            let kind_path = kind_entry.path();
            if !kind_path.is_dir() {
                continue;
            }
            let kind = kind_entry.file_name().to_string_lossy().into_owned();
            let files = match std::fs::read_dir(&kind_path) {
                Ok(entries) => entries,
                Err(_) => continue,
            };
            for file_entry in files.flatten() {
                let path = file_entry.path();
                if !path.is_file() || path.extension().and_then(|e| e.to_str()) != Some("proto") {
                    continue;
                }
                let name = path
                    .file_stem()
                    .map(|s| s.to_string_lossy().into_owned())
                    .unwrap_or_default();
                if name.is_empty() {
                    continue;
                }
                match std::fs::read_to_string(&path) {
                    Ok(content) => {
                        let key = Self::storage_key(&kind, &name);
                        self.schemas.write().insert(
                            key,
                            SchemaDefinition {
                                name,
                                kind: kind.clone(),
                                content: Some(content),
                                file: Some(path.to_string_lossy().into_owned()),
                            },
                        );
                        loaded += 1;
                    }
                    Err(e) => {
                        tracing::warn!("Skipping unreadable schema file {:?}: {}", path, e);
                    }
                }
            }
        }
        loaded
    }
}

pub struct ActiveRule {
    pub def: RuleDefinition,
    pub status: Arc<RwLock<RuleStatus>>,
    pub handle: Option<JoinHandle<()>>,
    counters: Arc<RuleCounters>,
}

/// Per-rule atomic counters. Rule tasks hold an `Arc<RuleCounters>` cloned at
/// spawn time, so the hot path never takes the global rules-map lock: counts
/// are bumped lock-free and mirrored into the status snapshot.
#[derive(Debug)]
pub struct RuleCounters {
    pub source_in: Arc<std::sync::atomic::AtomicU64>,
    pub sink_out: std::sync::atomic::AtomicU64,
    pub exceptions: std::sync::atomic::AtomicU64,
    pub filtered: Arc<std::sync::atomic::AtomicU64>,
    pub enqueued: std::sync::atomic::AtomicU64,
    pub sink_failed: std::sync::atomic::AtomicU64,
    pub dropped: std::sync::atomic::AtomicU64,
    pub high_water: std::sync::atomic::AtomicU64,
    pub blocked_micros: std::sync::atomic::AtomicU64,
    pub last_exception: parking_lot::RwLock<String>,
}

impl Default for RuleCounters {
    fn default() -> Self {
        Self {
            source_in: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            sink_out: std::sync::atomic::AtomicU64::new(0),
            exceptions: std::sync::atomic::AtomicU64::new(0),
            filtered: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            enqueued: std::sync::atomic::AtomicU64::new(0),
            sink_failed: std::sync::atomic::AtomicU64::new(0),
            dropped: std::sync::atomic::AtomicU64::new(0),
            high_water: std::sync::atomic::AtomicU64::new(0),
            blocked_micros: std::sync::atomic::AtomicU64::new(0),
            last_exception: parking_lot::RwLock::new(String::new()),
        }
    }
}

impl RuleCounters {
    fn sync_to(&self, status: &Arc<RwLock<RuleStatus>>) {
        use std::sync::atomic::Ordering::Relaxed;
        let mut guard = status.write();
        guard.source_records_in_total = self.source_in.load(Relaxed);
        guard.sink_records_out_total = self.sink_out.load(Relaxed);
        guard.exceptions_total = self.exceptions.load(Relaxed);
        let last_exc = self.last_exception.read().clone();
        if !last_exc.is_empty() {
            guard.last_exception = last_exc.clone();
            guard.message = last_exc;
        }
        guard.source_records_filtered_total = self.filtered.load(Relaxed);
        guard.sink_records_enqueued_total = self.enqueued.load(Relaxed);
        guard.sink_records_failed_total = self.sink_failed.load(Relaxed);
        guard.dropped_by_policy_total = self.dropped.load(Relaxed);
        guard.sink_queue_high_water = self.high_water.load(Relaxed) as usize;
        guard.sink_blocked_micros_total = self.blocked_micros.load(Relaxed);
    }

    /// Lock-free hot-path bumps (rule loops hold the Arc; no map locks).
    pub fn inc_source(&self, n: u64) {
        use std::sync::atomic::Ordering::Relaxed;
        self.source_in.fetch_add(n, Relaxed);
    }
    pub fn inc_sink_out(&self, n: u64) {
        use std::sync::atomic::Ordering::Relaxed;
        self.sink_out.fetch_add(n, Relaxed);
    }
    pub fn inc_exceptions(&self, n: u64) {
        use std::sync::atomic::Ordering::Relaxed;
        self.exceptions.fetch_add(n, Relaxed);
    }
    pub fn record_exception(&self, err: &str) {
        use std::sync::atomic::Ordering::Relaxed;
        self.exceptions.fetch_add(1, Relaxed);
        *self.last_exception.write() = err.to_string();
    }
    pub fn inc_filtered(&self, n: u64) {
        use std::sync::atomic::Ordering::Relaxed;
        self.filtered.fetch_add(n, Relaxed);
    }
    pub fn inc_enqueued(&self, n: u64) {
        use std::sync::atomic::Ordering::Relaxed;
        self.enqueued.fetch_add(n, Relaxed);
    }
    pub fn inc_sink_failed(&self, n: u64) {
        use std::sync::atomic::Ordering::Relaxed;
        self.sink_failed.fetch_add(n, Relaxed);
    }
    pub fn inc_dropped(&self, n: u64) {
        use std::sync::atomic::Ordering::Relaxed;
        self.dropped.fetch_add(n, Relaxed);
    }
    pub fn observe_high_water(&self, depth: usize) {
        use std::sync::atomic::Ordering::Relaxed;
        let prev = self.high_water.load(Relaxed);
        if (depth as u64) > prev {
            self.high_water.store(depth as u64, Relaxed);
        }
    }
    pub fn add_blocked_micros(&self, micros: u64) {
        use std::sync::atomic::Ordering::Relaxed;
        self.blocked_micros.fetch_add(micros, Relaxed);
    }
}

#[derive(Clone)]
pub struct RuleManager {
    rules: Arc<RwLock<HashMap<String, Arc<RwLock<ActiveRule>>>>>,
    pub stream_bus: StreamBus,
    kv: Option<Arc<dyn KvStore>>,
    op_lock: Arc<AsyncMutex<()>>,
}

/// Persisted rule envelope: the definition plus the last known lifecycle
/// status, so a restarted daemon knows which rules to resume.
#[derive(serde::Serialize, serde::Deserialize)]
struct PersistedRule {
    def: RuleDefinition,
    status: String,
}

impl RuleManager {
    pub fn new(stream_bus: StreamBus) -> Self {
        Self {
            rules: Arc::new(RwLock::new(HashMap::new())),
            stream_bus,
            kv: None,
            op_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn new_with_kv(stream_bus: StreamBus, kv: Arc<dyn KvStore>) -> Self {
        Self {
            rules: Arc::new(RwLock::new(HashMap::new())),
            stream_bus,
            kv: Some(kv),
            op_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    async fn persist_rule(&self, id: &str, def: &RuleDefinition, status: &str) -> Result<()> {
        if let Some(kv) = &self.kv {
            let envelope = serde_json::json!({ "def": def, "status": status }).to_string();
            kv.set("rules", id, &envelope).await?;
        }
        Ok(())
    }

    pub async fn save_rule_checkpoint(&self, rule_id: &str, state_json: &str) -> Result<()> {
        if let Some(kv) = &self.kv {
            kv.set("rule_checkpoints", rule_id, state_json).await?;
        }
        Ok(())
    }

    pub async fn load_rule_checkpoint(&self, rule_id: &str) -> Option<String> {
        if let Some(kv) = &self.kv {
            kv.get("rule_checkpoints", rule_id).await.ok().flatten()
        } else {
            None
        }
    }

    pub async fn delete_rule_checkpoint(&self, rule_id: &str) -> Result<()> {
        if let Some(kv) = &self.kv {
            kv.delete("rule_checkpoints", rule_id).await?;
        }
        Ok(())
    }

    async fn unpersist_rule(&self, id: &str) -> Result<()> {
        if let Some(kv) = &self.kv {
            kv.delete("rules", id).await?;
        }
        Ok(())
    }

    pub async fn create_rule(&self, def: RuleDefinition) -> Result<()> {
        let _guard = self.op_lock.lock().await;
        {
            let map = self.rules.read();
            if map.contains_key(&def.id) {
                bail!("Rule {} already exists", def.id);
            }
        }
        self.persist_rule(&def.id, &def, "running").await?;
        let rule_id = def.id.clone();
        let active = ActiveRule {
            def,
            status: Arc::new(RwLock::new(RuleStatus {
                status: "running".to_string(),
                message: "".to_string(),
                source_records_in_total: 0,
                sink_records_out_total: 0,
                exceptions_total: 0,
                ..RuleStatus::default()
            })),
            handle: None,
            counters: Arc::new(RuleCounters::default()),
        };
        self.rules
            .write()
            .insert(rule_id, Arc::new(RwLock::new(active)));
        Ok(())
    }

    pub fn get_rule(&self, id: &str) -> Option<RuleDefinition> {
        self.rules.read().get(id).map(|r| r.read().def.clone())
    }

    pub async fn update_rule(&self, def: RuleDefinition) -> Result<bool> {
        let _guard = self.op_lock.lock().await;
        let rule_arc = {
            let map = self.rules.read();
            map.get(&def.id)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("Rule {} not found", def.id))?
        };
        let was_running = {
            let active = rule_arc.read();
            let is_running = active.status.read().status == "running";
            is_running
        };
        let status_str = if was_running { "running" } else { "stopped" };
        self.persist_rule(&def.id, &def, status_str).await?;
        {
            let mut active = rule_arc.write();
            active.def = def;
            if was_running {
                if let Some(old) = active.handle.take() {
                    old.abort();
                }
            }
        }
        Ok(was_running)
    }

    pub async fn update_rule_tags<F>(&self, id: &str, update_fn: F) -> Result<Vec<String>>
    where
        F: FnOnce(&mut Vec<String>),
    {
        let _guard = self.op_lock.lock().await;
        let rule_arc = {
            let map = self.rules.read();
            map.get(id)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("Rule {} not found", id))?
        };
        let (mut def, status_str) = {
            let active = rule_arc.read();
            let status = active.status.read().status.clone();
            (active.def.clone(), status)
        };
        update_fn(&mut def.tags);
        def.tags.sort();
        def.tags.dedup();
        self.persist_rule(id, &def, &status_str).await?;
        {
            let map = self.rules.read();
            if let Some(rule) = map.get(id) {
                rule.write().def.tags = def.tags.clone();
            }
        }
        Ok(def.tags)
    }

    pub fn list_rules(&self) -> Vec<RuleDefinition> {
        self.rules
            .read()
            .values()
            .map(|r| r.read().def.clone())
            .collect()
    }

    pub fn rule_counters(&self, id: &str) -> Option<Arc<RuleCounters>> {
        self.rules.read().get(id).map(|r| r.read().counters.clone())
    }

    pub fn rule_status_handle(&self, id: &str) -> Option<Arc<RwLock<RuleStatus>>> {
        self.rules.read().get(id).map(|r| r.read().status.clone())
    }

    pub fn get_rule_status(&self, id: &str) -> Option<RuleStatus> {
        self.rules.read().get(id).map(|r| {
            let active = r.read();
            active.counters.sync_to(&active.status);
            let snapshot = active.status.read().clone();
            snapshot
        })
    }

    fn bump(
        &self,
        id: &str,
        field: fn(&RuleCounters) -> &std::sync::atomic::AtomicU64,
        count: u64,
    ) {
        use std::sync::atomic::Ordering::Relaxed;
        let map = self.rules.read();
        if let Some(rule) = map.get(id) {
            let active = rule.read();
            field(&active.counters).fetch_add(count, Relaxed);
        }
    }

    pub fn inc_exceptions(&self, id: &str, count: u64) {
        self.bump(id, |c| &c.exceptions, count);
    }

    pub fn record_exception(&self, id: &str, err: &str) {
        let map = self.rules.read();
        if let Some(rule) = map.get(id) {
            let active = rule.read();
            active.counters.record_exception(err);
        }
    }

    pub fn inc_sink_failed(&self, id: &str, count: u64) {
        self.bump(id, |c| &c.sink_failed, count);
    }

    pub fn inc_dropped(&self, id: &str, count: u64) {
        self.bump(id, |c| &c.dropped, count);
    }

    pub fn reset_rule_metrics(&self, id: &str) -> Result<()> {
        use std::sync::atomic::Ordering::Relaxed;
        let map = self.rules.read();
        let rule_arc = map
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("Rule {} not found", id))?;
        let active = rule_arc.read();
        active.counters.source_in.store(0, Relaxed);
        active.counters.sink_out.store(0, Relaxed);
        active.counters.exceptions.store(0, Relaxed);
        active.counters.filtered.store(0, Relaxed);
        active.counters.enqueued.store(0, Relaxed);
        active.counters.sink_failed.store(0, Relaxed);
        active.counters.dropped.store(0, Relaxed);
        active.counters.high_water.store(0, Relaxed);
        active.counters.blocked_micros.store(0, Relaxed);
        active.counters.sync_to(&active.status);
        Ok(())
    }

    pub fn set_rule_handle(&self, id: &str, handle: JoinHandle<()>) {
        let map = self.rules.read();
        if let Some(rule) = map.get(id) {
            let mut active = rule.write();
            if active.status.read().status != "running" {
                handle.abort();
                active.handle = None;
                return;
            }
            if let Some(old) = active.handle.replace(handle) {
                old.abort();
            }
        } else {
            handle.abort();
        }
    }

    pub async fn start_rule(&self, id: &str) -> Result<()> {
        let _guard = self.op_lock.lock().await;
        let rule_arc = {
            let map = self.rules.read();
            map.get(id)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("Rule {} not found", id))?
        };
        let snapshot = rule_arc.read().def.clone();
        self.persist_rule(id, &snapshot, "running").await?;
        {
            if let Some(rule_arc) = self.rules.read().get(id) {
                let active = rule_arc.read();
                let mut st = active.status.write();
                st.status = "running".to_string();
                st.last_start_timestamp = chrono::Utc::now().timestamp_millis();
                st.message.clear();
            }
        }
        Ok(())
    }

    pub async fn stop_rule(&self, id: &str) -> Result<()> {
        let _guard = self.op_lock.lock().await;
        let rule_arc = {
            let map = self.rules.read();
            map.get(id)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("Rule {} not found", id))?
        };
        let snapshot = rule_arc.read().def.clone();
        self.persist_rule(id, &snapshot, "stopped").await?;
        {
            if let Some(rule_arc) = self.rules.read().get(id) {
                let mut rule = rule_arc.write();
                if let Some(handle) = rule.handle.take() {
                    handle.abort();
                }
                let mut st = rule.status.write();
                st.status = "stopped".to_string();
                st.last_stop_timestamp = chrono::Utc::now().timestamp_millis();
                st.message = "canceled manually".to_string();
            }
        }
        Ok(())
    }

    /// Atomic restart lifecycle transition: sets and persists the rule as `running`,
    /// aborting any previous task under the mutation lock.
    pub async fn restart_rule(&self, id: &str) -> Result<()> {
        let _guard = self.op_lock.lock().await;
        let rule_arc = {
            let map = self.rules.read();
            map.get(id)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("Rule {} not found", id))?
        };
        let snapshot = rule_arc.read().def.clone();
        self.persist_rule(id, &snapshot, "running").await?;
        {
            let mut rule = rule_arc.write();
            if let Some(handle) = rule.handle.take() {
                handle.abort();
            }
            let mut st = rule.status.write();
            st.status = "running".to_string();
            st.last_start_timestamp = chrono::Utc::now().timestamp_millis();
            st.message.clear();
        }
        Ok(())
    }

    pub async fn delete_rule(&self, id: &str) -> Result<()> {
        let _guard = self.op_lock.lock().await;
        {
            let map = self.rules.read();
            if !map.contains_key(id) {
                bail!("Rule {} not found", id);
            }
        }
        self.unpersist_rule(id).await?;
        let _ = self.delete_rule_checkpoint(id).await;
        {
            let mut map = self.rules.write();
            if let Some(rule_arc) = map.remove(id) {
                let mut rule = rule_arc.write();
                if let Some(handle) = rule.handle.take() {
                    handle.abort();
                }
            }
        }
        Ok(())
    }

    /// Repopulates rules previously stored under the `rules` namespace,
    /// restoring each persisted lifecycle status with fresh zeroed metrics.
    /// Unknown statuses are treated as stopped so nothing auto-starts that
    /// shouldn't.
    pub async fn load_from_kv(&self, kv: &Arc<dyn KvStore>) -> Result<()> {
        for (key, val) in kv.list_all("rules").await? {
            let stored: PersistedRule = match serde_json::from_str(&val) {
                Ok(stored) => stored,
                Err(e) => {
                    tracing::warn!("Skipping corrupt rule entry {}: {}", key, e);
                    continue;
                }
            };
            let status = if stored.status == "running" {
                "running"
            } else {
                "stopped"
            };
            let active = ActiveRule {
                def: stored.def,
                status: Arc::new(RwLock::new(RuleStatus {
                    status: status.to_string(),
                    message: String::new(),
                    source_records_in_total: 0,
                    sink_records_out_total: 0,
                    exceptions_total: 0,
                    ..RuleStatus::default()
                })),
                handle: None,
                counters: Arc::new(RuleCounters::default()),
            };
            self.rules
                .write()
                .insert(active.def.id.clone(), Arc::new(RwLock::new(active)));
        }
        Ok(())
    }
}
