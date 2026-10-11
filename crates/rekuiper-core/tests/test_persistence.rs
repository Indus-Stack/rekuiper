use async_trait::async_trait;
use rekuiper_core::{
    KvStore, MemKvStore, PluginDefinition, PluginManager, RuleDefinition, RuleManager,
    SchemaDefinition, SchemaManager, SqliteKvStore, StreamBus, StreamDefinition, StreamField,
    StreamManager, TableDefinition, TableManager,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

fn temp_db_path(tag: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let mut path = std::env::temp_dir();
    path.push(format!(
        "rekuiper-kv-test-{}-{}-{}.db",
        tag,
        std::process::id(),
        nanos
    ));
    path
}

fn stream_def(name: &str) -> StreamDefinition {
    StreamDefinition {
        name: name.to_string(),
        sql: format!("CREATE STREAM {} () WITH (FORMAT=\"json\")", name),
        stream_fields: vec![StreamField::new("id", "bigint")],
        options: HashMap::new(),
    }
}

fn table_def(name: &str) -> TableDefinition {
    TableDefinition {
        name: name.to_string(),
        sql: format!("CREATE TABLE {} () WITH (FORMAT=\"json\")", name),
        stream_fields: Vec::new(),
        options: HashMap::new(),
    }
}

fn rule_def(id: &str) -> RuleDefinition {
    RuleDefinition {
        id: id.to_string(),
        name: None,
        version: None,
        sql: "SELECT * FROM demo".to_string(),
        actions: vec![],
        options: None,
        graph: None,
        tags: vec![],
    }
}

#[tokio::test]
async fn test_kv_basic_operations() {
    let path = temp_db_path("basic");
    let _ = tokio::fs::remove_file(&path).await;
    let kv = SqliteKvStore::new(&path).await.unwrap();

    assert_eq!(kv.get("ns", "k").await.unwrap(), None);

    kv.set("ns", "k", "v1").await.unwrap();
    assert_eq!(kv.get("ns", "k").await.unwrap(), Some("v1".to_string()));

    // Upsert overwrites.
    kv.set("ns", "k", "v2").await.unwrap();
    assert_eq!(kv.get("ns", "k").await.unwrap(), Some("v2".to_string()));

    kv.set("ns", "other", "x").await.unwrap();
    kv.set("elsewhere", "k", "y").await.unwrap();
    let mut listed = kv.list_all("ns").await.unwrap();
    listed.sort();
    assert_eq!(
        listed,
        vec![
            ("k".to_string(), "v2".to_string()),
            ("other".to_string(), "x".to_string())
        ]
    );

    kv.delete("ns", "k").await.unwrap();
    assert_eq!(kv.get("ns", "k").await.unwrap(), None);
    assert_eq!(kv.list_all("elsewhere").await.unwrap().len(), 1);

    // MemKvStore obeys the same contract.
    let mem = MemKvStore::new();
    mem.set("ns", "k", "v").await.unwrap();
    assert_eq!(mem.get("ns", "k").await.unwrap(), Some("v".to_string()));
    mem.delete("ns", "k").await.unwrap();
    assert_eq!(mem.get("ns", "k").await.unwrap(), None);

    let _ = tokio::fs::remove_file(&path).await;
}

#[tokio::test]
async fn test_manager_persistence_and_restart() {
    let path = temp_db_path("restart");
    let _ = tokio::fs::remove_file(&path).await;

    // First "boot": managers backed by the SQLite file.
    let kv: Arc<dyn KvStore> = Arc::new(SqliteKvStore::new(&path).await.unwrap());
    let streams = StreamManager::new_with_kv(kv.clone());
    let tables = TableManager::new_with_kv(kv.clone());
    let bus = StreamBus::new();
    let rules = RuleManager::new_with_kv(bus, kv.clone());

    streams.create_stream(stream_def("demo")).await.unwrap();
    tables.create_table(table_def("alerts")).await.unwrap();
    rules.create_rule(rule_def("rule_running")).await.unwrap();
    rules.create_rule(rule_def("rule_paused")).await.unwrap();
    rules.stop_rule("rule_paused").await.unwrap();

    // Simulate restart: brand-new managers over the same file.
    drop((streams, tables, rules));
    let kv2: Arc<dyn KvStore> = Arc::new(SqliteKvStore::new(&path).await.unwrap());
    let streams2 = StreamManager::new_with_kv(kv2.clone());
    let tables2 = TableManager::new_with_kv(kv2.clone());
    let bus2 = StreamBus::new();
    let rules2 = RuleManager::new_with_kv(bus2, kv2.clone());

    streams2.load_from_kv(&kv2).await.unwrap();
    tables2.load_from_kv(&kv2).await.unwrap();
    rules2.load_from_kv(&kv2).await.unwrap();

    // Definitions survived, including declared stream fields.
    let restored = streams2.get_stream("demo").expect("demo survives");
    assert_eq!(restored.stream_fields.len(), 1);
    assert_eq!(restored.stream_fields[0].name, "id");
    assert_eq!(restored.stream_fields[0].field_type, "bigint");
    assert!(tables2.get_table("alerts").is_some());
    assert!(rules2.get_rule("rule_running").is_some());
    assert!(rules2.get_rule("rule_paused").is_some());

    // Lifecycle statuses survived: running rules resume, stopped ones stay.
    let running = rules2.get_rule_status("rule_running").unwrap();
    assert_eq!(running.status, "running");
    let paused = rules2.get_rule_status("rule_paused").unwrap();
    assert_eq!(paused.status, "stopped");

    // Deletions propagate too: delete, "restart" again, confirm absence.
    rules2.delete_rule("rule_paused").await.unwrap();
    streams2.delete_stream("demo").await.unwrap();
    drop((streams2, tables2, rules2));
    let kv3: Arc<dyn KvStore> = Arc::new(SqliteKvStore::new(&path).await.unwrap());
    let streams3 = StreamManager::new_with_kv(kv3.clone());
    let rules3 = RuleManager::new_with_kv(StreamBus::new(), kv3.clone());
    streams3.load_from_kv(&kv3).await.unwrap();
    rules3.load_from_kv(&kv3).await.unwrap();
    assert!(streams3.get_stream("demo").is_none());
    assert!(rules3.get_rule("rule_paused").is_none());
    assert!(rules3.get_rule("rule_running").is_some());

    let _ = tokio::fs::remove_file(&path).await;
}

struct FailingKvStore {
    inner: MemKvStore,
    fail_set: AtomicBool,
    fail_delete: AtomicBool,
}

impl FailingKvStore {
    fn new() -> Self {
        Self {
            inner: MemKvStore::new(),
            fail_set: AtomicBool::new(false),
            fail_delete: AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl KvStore for FailingKvStore {
    async fn get(&self, namespace: &str, key: &str) -> anyhow::Result<Option<String>> {
        self.inner.get(namespace, key).await
    }

    async fn set(&self, namespace: &str, key: &str, val: &str) -> anyhow::Result<()> {
        if self.fail_set.load(Ordering::SeqCst) {
            anyhow::bail!("Injected I/O failure on KV set: {}/{}", namespace, key);
        }
        self.inner.set(namespace, key, val).await
    }

    async fn delete(&self, namespace: &str, key: &str) -> anyhow::Result<()> {
        if self.fail_delete.load(Ordering::SeqCst) {
            anyhow::bail!("Injected I/O failure on KV delete: {}/{}", namespace, key);
        }
        self.inner.delete(namespace, key).await
    }

    async fn list_all(&self, namespace: &str) -> anyhow::Result<Vec<(String, String)>> {
        self.inner.list_all(namespace).await
    }
}

#[tokio::test]
async fn test_stream_persistence_failure_injection() {
    let failing_kv = Arc::new(FailingKvStore::new());
    let kv_trait: Arc<dyn KvStore> = failing_kv.clone();
    let streams = StreamManager::new_with_kv(kv_trait.clone());

    // 1. Injected write failure during create
    failing_kv.fail_set.store(true, Ordering::SeqCst);
    let create_err = streams.create_stream(stream_def("s_fail")).await;
    assert!(
        create_err.is_err(),
        "create_stream must fail on KV write error"
    );
    assert!(
        streams.get_stream("s_fail").is_none(),
        "stream must not exist in memory after failed create"
    );
    assert!(!streams.list_streams().contains(&"s_fail".to_string()));

    // Verify KV is also empty
    failing_kv.fail_set.store(false, Ordering::SeqCst);
    let streams_reload = StreamManager::new_with_kv(kv_trait.clone());
    streams_reload.load_from_kv(&kv_trait).await.unwrap();
    assert!(streams_reload.get_stream("s_fail").is_none());

    // 2. Successful create
    streams.create_stream(stream_def("s_fail")).await.unwrap();
    assert!(streams.get_stream("s_fail").is_some());

    // 3. Injected write failure during update
    failing_kv.fail_set.store(true, Ordering::SeqCst);
    let mut updated_def = stream_def("s_fail");
    updated_def.stream_fields.push(StreamField::new("temp", "float"));
    let update_err = streams.update_stream(updated_def).await;
    assert!(
        update_err.is_err(),
        "update_stream must fail on KV write error"
    );
    // Verify memory still holds original definition
    let current = streams.get_stream("s_fail").unwrap();
    assert_eq!(
        current.stream_fields.len(),
        1,
        "in-memory definition must not be corrupted by failed update"
    );

    // 4. Injected failure during delete
    failing_kv.fail_delete.store(true, Ordering::SeqCst);
    let delete_err = streams.delete_stream("s_fail").await;
    assert!(
        delete_err.is_err(),
        "delete_stream must fail on KV delete error"
    );
    assert!(
        streams.get_stream("s_fail").is_some(),
        "stream must remain in memory when delete fails"
    );

    // 5. Successful delete
    failing_kv.fail_delete.store(false, Ordering::SeqCst);
    streams.delete_stream("s_fail").await.unwrap();
    assert!(streams.get_stream("s_fail").is_none());
}

#[tokio::test]
async fn test_table_persistence_failure_injection() {
    let failing_kv = Arc::new(FailingKvStore::new());
    let kv_trait: Arc<dyn KvStore> = failing_kv.clone();
    let tables = TableManager::new_with_kv(kv_trait.clone());

    // Injected create failure
    failing_kv.fail_set.store(true, Ordering::SeqCst);
    assert!(tables.create_table(table_def("t_fail")).await.is_err());
    assert!(tables.get_table("t_fail").is_none());

    // Successful create
    failing_kv.fail_set.store(false, Ordering::SeqCst);
    tables.create_table(table_def("t_fail")).await.unwrap();
    assert!(tables.get_table("t_fail").is_some());

    // Injected update failure
    failing_kv.fail_set.store(true, Ordering::SeqCst);
    let mut updated = table_def("t_fail");
    updated.sql = "CREATE TABLE t_fail () WITH (FORMAT=\"binary\")".to_string();
    assert!(tables.update_table(updated).await.is_err());
    assert_eq!(
        tables.get_table("t_fail").unwrap().sql,
        "CREATE TABLE t_fail () WITH (FORMAT=\"json\")"
    );

    // Injected delete failure
    failing_kv.fail_delete.store(true, Ordering::SeqCst);
    assert!(tables.delete_table("t_fail").await.is_err());
    assert!(tables.get_table("t_fail").is_some());

    // Successful delete
    failing_kv.fail_delete.store(false, Ordering::SeqCst);
    tables.delete_table("t_fail").await.unwrap();
    assert!(tables.get_table("t_fail").is_none());
}

#[tokio::test]
async fn test_schema_persistence_failure_injection() {
    let failing_kv = Arc::new(FailingKvStore::new());
    let kv_trait: Arc<dyn KvStore> = failing_kv.clone();
    let schemas = SchemaManager::new_with_kv(kv_trait.clone());

    let def = SchemaDefinition {
        name: "device".to_string(),
        kind: "protobuf".to_string(),
        content: Some("syntax = \"proto3\";".to_string()),
        file: None,
    };

    // Injected register failure
    failing_kv.fail_set.store(true, Ordering::SeqCst);
    assert!(schemas.register_schema(def.clone()).await.is_err());
    assert!(schemas.get_schema("protobuf", "device").is_none());

    // Successful register
    failing_kv.fail_set.store(false, Ordering::SeqCst);
    schemas.register_schema(def).await.unwrap();
    assert!(schemas.get_schema("protobuf", "device").is_some());

    // Injected delete failure
    failing_kv.fail_delete.store(true, Ordering::SeqCst);
    assert!(schemas.delete_schema("protobuf", "device").await.is_err());
    assert!(schemas.get_schema("protobuf", "device").is_some());

    // Successful delete
    failing_kv.fail_delete.store(false, Ordering::SeqCst);
    schemas.delete_schema("protobuf", "device").await.unwrap();
    assert!(schemas.get_schema("protobuf", "device").is_none());
}

#[tokio::test]
async fn test_rule_persistence_failure_injection() {
    let failing_kv = Arc::new(FailingKvStore::new());
    let kv_trait: Arc<dyn KvStore> = failing_kv.clone();
    let bus = StreamBus::new();
    let rules = RuleManager::new_with_kv(bus, kv_trait.clone());

    let def = rule_def("r_fail");

    // Injected create failure
    failing_kv.fail_set.store(true, Ordering::SeqCst);
    assert!(rules.create_rule(def.clone()).await.is_err());
    assert!(rules.get_rule("r_fail").is_none());

    // Successful create
    failing_kv.fail_set.store(false, Ordering::SeqCst);
    rules.create_rule(def.clone()).await.unwrap();
    assert!(rules.get_rule("r_fail").is_some());
    assert_eq!(rules.get_rule_status("r_fail").unwrap().status, "running");

    // Injected stop failure
    failing_kv.fail_set.store(true, Ordering::SeqCst);
    assert!(rules.stop_rule("r_fail").await.is_err());
    assert_eq!(
        rules.get_rule_status("r_fail").unwrap().status,
        "running",
        "status must stay running on failed stop"
    );

    // Successful stop
    failing_kv.fail_set.store(false, Ordering::SeqCst);
    rules.stop_rule("r_fail").await.unwrap();
    assert_eq!(rules.get_rule_status("r_fail").unwrap().status, "stopped");

    // Injected start failure
    failing_kv.fail_set.store(true, Ordering::SeqCst);
    assert!(rules.start_rule("r_fail").await.is_err());
    assert_eq!(
        rules.get_rule_status("r_fail").unwrap().status,
        "stopped",
        "status must stay stopped on failed start"
    );

    // Injected update_rule_tags failure
    assert!(rules
        .update_rule_tags("r_fail", |tags| tags.push("sensor".to_string()))
        .await
        .is_err());
    assert!(
        rules.get_rule("r_fail").unwrap().tags.is_empty(),
        "tags must not be updated if persist fails"
    );

    // Successful tag update
    failing_kv.fail_set.store(false, Ordering::SeqCst);
    rules
        .update_rule_tags("r_fail", |tags| tags.push("sensor".to_string()))
        .await
        .unwrap();
    assert_eq!(
        rules.get_rule("r_fail").unwrap().tags,
        vec!["sensor".to_string()]
    );

    // Injected delete failure
    failing_kv.fail_delete.store(true, Ordering::SeqCst);
    assert!(rules.delete_rule("r_fail").await.is_err());
    assert!(
        rules.get_rule("r_fail").is_some(),
        "rule must remain in memory when delete fails"
    );

    // Successful delete
    failing_kv.fail_delete.store(false, Ordering::SeqCst);
    rules.delete_rule("r_fail").await.unwrap();
    assert!(rules.get_rule("r_fail").is_none());
}

#[tokio::test]
async fn test_plugin_persistence_failure_injection() {
    let failing_kv = Arc::new(FailingKvStore::new());
    let kv_trait: Arc<dyn KvStore> = failing_kv.clone();
    let plugins = PluginManager::new_with_kv(kv_trait.clone());

    let def = PluginDefinition {
        name: "math_udf".to_string(),
        plugin_type: "function".to_string(),
        file: None,
        description: Some("math".to_string()),
        functions: vec!["sqrt".to_string()],
    };

    // Injected register failure
    failing_kv.fail_set.store(true, Ordering::SeqCst);
    assert!(plugins.register_plugin(def.clone()).await.is_err());
    assert!(plugins.get_plugin("math_udf").is_none());

    // Successful register
    failing_kv.fail_set.store(false, Ordering::SeqCst);
    plugins.register_plugin(def).await.unwrap();
    assert!(plugins.get_plugin("math_udf").is_some());

    // Injected delete failure
    failing_kv.fail_delete.store(true, Ordering::SeqCst);
    assert!(plugins.delete_plugin("math_udf").await.is_err());
    assert!(plugins.get_plugin("math_udf").is_some());

    // Successful delete
    failing_kv.fail_delete.store(false, Ordering::SeqCst);
    plugins.delete_plugin("math_udf").await.unwrap();
    assert!(plugins.get_plugin("math_udf").is_none());
}

#[tokio::test]
async fn test_concurrent_mutations_consistency() {
    let path = temp_db_path("concurrent");
    let _ = tokio::fs::remove_file(&path).await;
    let kv: Arc<dyn KvStore> = Arc::new(SqliteKvStore::new(&path).await.unwrap());
    let streams = StreamManager::new_with_kv(kv.clone());

    // Spawn 20 tasks attempting to create streams with overlapping names
    let mut handles = Vec::new();
    for i in 0..20 {
        let sm = streams.clone();
        let name = format!("stream_{}", i % 5);
        handles.push(tokio::spawn(async move {
            let _ = sm.create_stream(stream_def(&name)).await;
        }));
    }
    for h in handles {
        let _ = h.await;
    }

    // Verify exactly 5 streams exist in memory
    let in_mem = streams.list_streams();
    assert_eq!(in_mem.len(), 5);

    // Verify reload from KV store yields exactly the same 5 streams
    let streams_reload = StreamManager::new_with_kv(kv.clone());
    streams_reload.load_from_kv(&kv).await.unwrap();
    let mut reloaded = streams_reload.list_streams();
    let mut expected = in_mem.clone();
    reloaded.sort();
    expected.sort();
    assert_eq!(reloaded, expected);

    let _ = tokio::fs::remove_file(&path).await;
}
