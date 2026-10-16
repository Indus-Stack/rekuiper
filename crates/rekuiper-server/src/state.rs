use axum::{
    extract::State,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use parking_lot::RwLock;
use rekuiper_conf::KuiperConfig;
use rekuiper_connectors::SimulatorConfig;
use rekuiper_core::{
    KvStore, PluginManager, RuleManager, SchemaManager, StreamBus, StreamManager, TableManager,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceSpan {
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "TraceID")]
    pub trace_id: String,
    #[serde(rename = "SpanID")]
    pub span_id: String,
    #[serde(rename = "ParentSpanID")]
    pub parent_span_id: String,
    #[serde(rename = "Attribute")]
    pub attribute: Option<HashMap<String, Value>>,
    #[serde(rename = "Links")]
    pub links: Option<Vec<Value>>,
    #[serde(rename = "StartTime")]
    pub start_time: String,
    #[serde(rename = "EndTime")]
    pub end_time: String,
    #[serde(rename = "RuleID", skip_serializing_if = "Option::is_none")]
    pub rule_id: Option<String>,
    #[serde(rename = "ChildSpan")]
    pub child_span: Vec<TraceSpan>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct TracerConfig {
    #[serde(default)]
    pub service_name: Option<String>,
    #[serde(default)]
    pub action: Option<String>,
    #[serde(default)]
    pub collector_url: Option<String>,
}

#[derive(Clone, Default)]
pub struct TraceManager {
    active_count: Arc<std::sync::atomic::AtomicUsize>,
    active_traces: Arc<RwLock<HashMap<String, String>>>,
    rule_traces: Arc<RwLock<HashMap<String, VecDeque<String>>>>,
    trace_spans: Arc<RwLock<HashMap<String, TraceSpan>>>,
    tracer_config: Arc<RwLock<Option<TracerConfig>>>,
}

impl TraceManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn start_trace(&self, rule_id: &str, strategy: String) {
        let mut guard = self.active_traces.write();
        guard.insert(rule_id.to_string(), strategy);
        self.active_count
            .store(guard.len(), std::sync::atomic::Ordering::Release);
    }

    pub fn stop_trace(&self, rule_id: &str) {
        let mut guard = self.active_traces.write();
        guard.remove(rule_id);
        self.active_count
            .store(guard.len(), std::sync::atomic::Ordering::Release);
    }

    #[inline]
    pub fn is_tracing(&self, rule_id: &str) -> bool {
        if self.active_count.load(std::sync::atomic::Ordering::Relaxed) == 0 {
            return false;
        }
        self.active_traces.read().contains_key(rule_id)
    }

    pub fn record_trace(&self, rule_id: &str, span: TraceSpan) {
        let trace_id = span.trace_id.clone();
        {
            let mut spans = self.trace_spans.write();
            if spans.len() >= 2048 {
                if let Some(k) = spans.keys().next().cloned() {
                    spans.remove(&k);
                }
            }
            spans.insert(trace_id.clone(), span);
        }
        {
            let mut rules = self.rule_traces.write();
            let q = rules.entry(rule_id.to_string()).or_default();
            if q.len() >= 1024 {
                q.pop_front();
            }
            q.push_back(trace_id);
        }
    }

    pub fn list_rule_trace_ids(&self, rule_id: &str, limit: Option<usize>) -> Vec<String> {
        let rules = self.rule_traces.read();
        if let Some(q) = rules.get(rule_id) {
            let mut ids: Vec<String> = q.iter().cloned().collect();
            ids.reverse();
            if let Some(lim) = limit {
                if lim > 0 && ids.len() > lim {
                    ids.truncate(lim);
                }
            }
            ids
        } else {
            Vec::new()
        }
    }

    pub fn get_trace(&self, trace_id: &str) -> Option<TraceSpan> {
        self.trace_spans.read().get(trace_id).cloned()
    }

    pub fn set_tracer_config(&self, config: TracerConfig) {
        *self.tracer_config.write() = Some(config);
    }
}

pub fn maybe_trace_record(
    trace_mgr: &TraceManager,
    rule_id: &str,
    input_data: &HashMap<String, Value>,
    output_data: Option<&HashMap<String, Value>>,
) {
    if !trace_mgr.is_tracing(rule_id) {
        return;
    }
    let now = chrono::Utc::now();
    let start_time = now.to_rfc3339();
    let end_time = (now + chrono::Duration::microseconds(150)).to_rfc3339();
    let trace_id = format!("{:032x}", uuid::Uuid::new_v4().as_u128());
    let root_span_id = format!("{:016x}", uuid::Uuid::new_v4().as_u128() >> 64);
    let decoder_span_id = format!("{:016x}", uuid::Uuid::new_v4().as_u128() >> 64);
    let project_span_id = format!("{:016x}", uuid::Uuid::new_v4().as_u128() >> 64);
    let sink_span_id = format!("{:016x}", uuid::Uuid::new_v4().as_u128() >> 64);

    let input_str = serde_json::to_string(input_data).unwrap_or_default();
    let output_str = output_data
        .map(|o| serde_json::to_string(o).unwrap_or_default())
        .unwrap_or_else(|| input_str.clone());

    let mut decoder_attrs = HashMap::new();
    decoder_attrs.insert("data".to_string(), json!(input_str));

    let mut project_attrs = HashMap::new();
    project_attrs.insert("data".to_string(), json!(output_str));

    let mut sink_attrs = HashMap::new();
    sink_attrs.insert("data".to_string(), json!(output_str));

    let sink_span = TraceSpan {
        name: format!("{}_sink", rule_id),
        trace_id: trace_id.clone(),
        span_id: sink_span_id,
        parent_span_id: project_span_id.clone(),
        attribute: Some(sink_attrs),
        links: None,
        start_time: start_time.clone(),
        end_time: end_time.clone(),
        rule_id: Some(rule_id.to_string()),
        child_span: Vec::new(),
    };

    let project_span = TraceSpan {
        name: format!("{}_project", rule_id),
        trace_id: trace_id.clone(),
        span_id: project_span_id,
        parent_span_id: decoder_span_id.clone(),
        attribute: Some(project_attrs),
        links: None,
        start_time: start_time.clone(),
        end_time: end_time.clone(),
        rule_id: Some(rule_id.to_string()),
        child_span: vec![sink_span],
    };

    let decoder_span = TraceSpan {
        name: format!("{}_decoder", rule_id),
        trace_id: trace_id.clone(),
        span_id: decoder_span_id,
        parent_span_id: root_span_id.clone(),
        attribute: Some(decoder_attrs),
        links: None,
        start_time: start_time.clone(),
        end_time: end_time.clone(),
        rule_id: Some(rule_id.to_string()),
        child_span: vec![project_span],
    };

    let root_span = TraceSpan {
        name: rule_id.to_string(),
        trace_id,
        span_id: root_span_id,
        parent_span_id: "0000000000000000".to_string(),
        attribute: None,
        links: None,
        start_time,
        end_time,
        rule_id: Some(rule_id.to_string()),
        child_span: vec![decoder_span],
    };

    trace_mgr.record_trace(rule_id, root_span);
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskInfo {
    pub id: String,
    pub status: String,
    pub message: String,
    #[serde(rename = "createdTimestamp")]
    pub created_timestamp: i64,
    #[serde(rename = "updatedTimestamp")]
    pub updated_timestamp: i64,
}

#[derive(Clone, Default)]
pub struct TaskManager {
    tasks: Arc<RwLock<HashMap<String, TaskInfo>>>,
    cancels: Arc<RwLock<HashMap<String, tokio::sync::watch::Sender<bool>>>>,
}

impl TaskManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_task(&self, id: String) -> tokio::sync::watch::Receiver<bool> {
        let (tx, rx) = tokio::sync::watch::channel(false);
        let now = chrono::Utc::now().timestamp_millis();
        self.tasks.write().insert(
            id.clone(),
            TaskInfo {
                id: id.clone(),
                status: "running".to_string(),
                message: "task running".to_string(),
                created_timestamp: now,
                updated_timestamp: now,
            },
        );
        self.cancels.write().insert(id, tx);
        rx
    }

    pub fn update_status(&self, id: &str, status: &str, message: &str) {
        let mut tasks = self.tasks.write();
        if let Some(t) = tasks.get_mut(id) {
            t.status = status.to_string();
            t.message = message.to_string();
            t.updated_timestamp = chrono::Utc::now().timestamp_millis();
        }
    }

    pub fn get_task(&self, id: &str) -> Option<TaskInfo> {
        self.tasks.read().get(id).cloned()
    }

    pub fn cancel_task(&self, id: &str) -> bool {
        if let Some(tx) = self.cancels.write().remove(id) {
            let _ = tx.send(true);
        }
        let mut tasks = self.tasks.write();
        if let Some(t) = tasks.get_mut(id) {
            t.status = "cancelled".to_string();
            t.message = "task cancelled".to_string();
            t.updated_timestamp = chrono::Utc::now().timestamp_millis();
            true
        } else {
            false
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortablePluginInfo {
    pub name: String,
    #[serde(default = "default_portable_version")]
    pub version: String,
    #[serde(default = "default_portable_language")]
    pub language: String,
    #[serde(default)]
    pub executable: String,
    #[serde(
        rename = "virtualEnvType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub virtual_env_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env: Option<String>,
    #[serde(default)]
    pub sources: Vec<String>,
    #[serde(default)]
    pub sinks: Vec<String>,
    #[serde(default)]
    pub functions: Vec<String>,
}

fn default_portable_version() -> String {
    "1.0.0".to_string()
}

fn default_portable_language() -> String {
    "python".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortablePluginStatus {
    #[serde(rename = "refCount")]
    pub ref_count: HashMap<String, usize>,
    pub status: String,
    #[serde(rename = "errMsg")]
    pub err_msg: String,
}

pub fn create_default_portables(
) -> Arc<RwLock<HashMap<String, (PortablePluginInfo, PortablePluginStatus)>>> {
    Arc::new(RwLock::new(HashMap::new()))
}

#[derive(Debug, Clone)]
pub struct HttpPushEndpoint {
    pub stream_name: String,
    pub path: String,
    pub method: String,
    pub format: String,
}

pub type StreamFieldMaskMap =
    HashMap<String, Arc<parking_lot::RwLock<Option<rekuiper_connectors::codec::JsonFieldMask>>>>;

pub type StreamRuleFieldMasksMap = HashMap<String, HashMap<String, Option<HashSet<String>>>>;

#[allow(clippy::type_complexity)]
#[derive(Clone)]
pub struct AppState {
    pub start_time: Instant,
    pub version: String,
    pub config: Arc<RwLock<KuiperConfig>>,
    pub stream_manager: StreamManager,
    pub table_manager: TableManager,
    pub rule_manager: RuleManager,
    pub stream_bus: StreamBus,
    pub connections: Arc<RwLock<HashMap<String, Value>>>,
    pub source_configs: Arc<RwLock<HashMap<String, Value>>>,
    pub sink_configs: Arc<RwLock<HashMap<String, Value>>>,
    pub config_op_lock: Arc<tokio::sync::Mutex<()>>,
    pub ruletests: Arc<RwLock<HashMap<String, RuletestSession>>>,
    pub source_cancels: Arc<RwLock<HashMap<String, Vec<tokio::sync::watch::Sender<bool>>>>>,
    pub stream_active_rules: Arc<RwLock<HashMap<String, HashSet<String>>>>,
    pub stream_source_cancels: Arc<RwLock<HashMap<String, Vec<tokio::sync::watch::Sender<bool>>>>>,
    pub rule_streams: Arc<RwLock<HashMap<String, HashSet<String>>>>,
    pub stream_attach_meta: Arc<RwLock<HashMap<String, Arc<std::sync::atomic::AtomicBool>>>>,
    pub stream_field_masks: Arc<RwLock<StreamFieldMaskMap>>,
    pub stream_rule_field_masks: Arc<RwLock<StreamRuleFieldMasksMap>>,
    pub http_push_endpoints: Arc<RwLock<HashMap<String, HttpPushEndpoint>>>,
    pub http_client: reqwest::Client,
    pub schema_manager: SchemaManager,
    pub plugin_manager: PluginManager,
    pub trace_manager: TraceManager,
    pub task_manager: TaskManager,
    pub portable_plugins: Arc<RwLock<HashMap<String, (PortablePluginInfo, PortablePluginStatus)>>>,
    pub services: Arc<RwLock<HashMap<String, ServiceDetail>>>,
    pub js_udfs: Arc<RwLock<HashMap<String, JavascriptUdf>>>,
    pub latest_import_status: Arc<RwLock<Value>>,
    /// Embedded KV handle for config persistence across daemon restarts
    /// (`None` in unit tests, which never restart the process).
    pub kv: Option<Arc<dyn KvStore>>,
    pub declarative_mode: bool,
    pub declarative_path: Option<std::path::PathBuf>,
    pub declarative_watch: bool,
    pub active_rule_hashes: Arc<RwLock<HashMap<String, u64>>>,
    pub declarative_reload_failures: Arc<std::sync::atomic::AtomicU64>,
}

pub fn default_import_status() -> Value {
    json!({
        "streams": {},
        "tables": {},
        "rules": {},
        "nativePlugins": {},
        "portablePlugins": {},
        "sourceConfig": {},
        "sinkConfig": {},
        "connectionConfig": {},
        "Service": {},
        "Schema": {},
        "uploads": {},
        "scripts": {}
    })
}

/// Bounded replay buffer for one ruletest session: every emitted row gets a
/// monotonically increasing sequence number and is retained in a capped ring
/// so SSE subscribers can resume/continue without loss, duplicates, or
/// reordering while memory stays bounded.
#[derive(Debug, Default)]
pub struct RuletestReplay {
    pub entries: VecDeque<(u64, String)>,
    pub next_seq: u64,
}

impl RuletestReplay {
    pub fn push(&mut self, row: String) {
        let seq = self.next_seq;
        self.next_seq += 1;
        if self.entries.len() >= RULETEST_HISTORY_CAP {
            self.entries.pop_front();
        }
        self.entries.push_back((seq, row));
    }
}

/// Retained replay rows per session (10k). Active subscribers track a cursor
/// and stream indefinitely past the cap; a subscriber that falls further
/// behind than the ring skips the evicted prefix (documented lag gap) and
/// resumes at the oldest retained row — never duplicated, never reordered.
/// Late subscribers backfill up to the retained prefix, then go live.
pub const RULETEST_HISTORY_CAP: usize = 10_000;

/// An interactive rule-simulation session: mock source data is replayed
/// through the rule SQL and output rows stream out over SSE. `replay`
/// buffers emitted rows with sequence numbers so subscribers connecting
/// after `start` still receive the replay (no lost-race) and live
/// subscribers continue past the retention cap; `shutdown` stops the loop.
#[derive(Clone)]
pub struct RuletestSession {
    pub id: String,
    pub sql: String,
    pub mock_source: HashMap<String, SimulatorConfig>,
    pub output_tx: tokio::sync::broadcast::Sender<String>,
    pub replay: Arc<RwLock<RuletestReplay>>,
    pub port: u16,
    pub shutdown: Arc<tokio::sync::Notify>,
}

impl AppState {
    pub fn new(
        version: String,
        config: KuiperConfig,
        stream_manager: StreamManager,
        table_manager: TableManager,
        rule_manager: RuleManager,
        stream_bus: StreamBus,
    ) -> Self {
        Self {
            start_time: Instant::now(),
            version,
            config: Arc::new(RwLock::new(config)),
            stream_manager,
            table_manager,
            rule_manager,
            stream_bus,
            connections: Arc::new(RwLock::new(HashMap::new())),
            source_configs: Arc::new(RwLock::new(HashMap::new())),
            sink_configs: Arc::new(RwLock::new(HashMap::new())),
            config_op_lock: Arc::new(tokio::sync::Mutex::new(())),
            http_client: reqwest::Client::builder()
                .tcp_nodelay(true)
                .build()
                .unwrap_or_default(),
            ruletests: Arc::new(RwLock::new(HashMap::new())),
            source_cancels: Arc::new(RwLock::new(HashMap::new())),
            stream_active_rules: Arc::new(RwLock::new(HashMap::new())),
            stream_source_cancels: Arc::new(RwLock::new(HashMap::new())),
            rule_streams: Arc::new(RwLock::new(HashMap::new())),
            stream_attach_meta: Arc::new(RwLock::new(HashMap::new())),
            stream_field_masks: Arc::new(RwLock::new(HashMap::new())),
            stream_rule_field_masks: Arc::new(RwLock::new(HashMap::new())),
            http_push_endpoints: Arc::new(RwLock::new(HashMap::new())),
            schema_manager: SchemaManager::new(),
            plugin_manager: PluginManager::new(),
            trace_manager: TraceManager::new(),
            task_manager: TaskManager::new(),
            portable_plugins: create_default_portables(),
            services: create_default_services(),
            js_udfs: create_default_js_udfs(),
            latest_import_status: Arc::new(RwLock::new(default_import_status())),
            kv: None,
            declarative_mode: false,
            declarative_path: None,
            declarative_watch: false,
            active_rule_hashes: Arc::new(RwLock::new(HashMap::new())),
            declarative_reload_failures: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }
}

/// Persist one connection/source/sink config entry so daemon restarts keep
/// resolving CONF_KEYs (MQTT brokers, SQL URLs) instead of falling back to
/// loopback defaults with silent zero-delivery.
pub async fn persist_config_entry(
    state: &AppState,
    namespace: &str,
    key: &str,
    val: &Value,
) -> anyhow::Result<()> {
    if let Some(kv) = state.kv.as_ref() {
        kv.set(namespace, key, &val.to_string()).await?;
    }
    Ok(())
}

pub async fn unpersist_config_entry(
    state: &AppState,
    namespace: &str,
    key: &str,
) -> anyhow::Result<()> {
    if let Some(kv) = state.kv.as_ref() {
        kv.delete(namespace, key).await?;
    }
    Ok(())
}

/// Reload persisted connection/source/sink configs at daemon startup, ahead
/// of [`restore_running_rules`].
pub async fn load_config_maps(state: &AppState) -> anyhow::Result<()> {
    let Some(kv) = state.kv.as_ref() else {
        return Ok(());
    };
    for (namespace, map) in [
        ("source_configs", &state.source_configs),
        ("sink_configs", &state.sink_configs),
        ("connections", &state.connections),
    ] {
        let entries = kv
            .list_all(namespace)
            .await
            .map_err(|e| anyhow::anyhow!("KV load {} failed: {}", namespace, e))?;
        let mut guard = map.write();
        for (key, val) in entries {
            match serde_json::from_str::<Value>(&val) {
                Ok(parsed) => {
                    guard.insert(key, parsed);
                }
                Err(e) => {
                    tracing::warn!("Skipping corrupt {} entry {}: {}", namespace, key, e);
                }
            }
        }
    }
    load_default_source_file("etc/sources/edgex.yaml", "edgex", &state.source_configs);
    apply_edgex_env_overlays(&state.source_configs);
    Ok(())
}

pub fn load_default_source_file(
    path_str: &str,
    prefix: &str,
    source_configs: &Arc<RwLock<HashMap<String, Value>>>,
) {
    let path = std::path::Path::new(path_str);
    if !path.exists() {
        return;
    }
    let Ok(content) = std::fs::read_to_string(path) else {
        return;
    };
    let Ok(val) = serde_yaml::from_str::<Value>(&content) else {
        return;
    };
    if let Value::Object(sections) = val {
        let mut guard = source_configs.write();
        for (sec_name, sec_val) in sections {
            let key = format!("{}/{}", prefix, sec_name);
            guard.entry(key).or_insert(sec_val);
        }
    }
}

pub fn apply_edgex_env_overlays(source_configs: &Arc<RwLock<HashMap<String, Value>>>) {
    let edgex_vars: Vec<(String, String)> = std::env::vars()
        .filter(|(k, _)| k.starts_with("EDGEX__"))
        .collect();
    if edgex_vars.is_empty() {
        return;
    }
    let mut guard = source_configs.write();
    let default_key = "edgex/default".to_string();
    let entry = guard.entry(default_key).or_insert_with(|| {
        serde_json::json!({
            "protocol": "tcp",
            "server": "edgex-mqtt-broker",
            "port": 1883,
            "topic": "edgex/rules-events",
            "type": "mqtt",
            "messageType": "event"
        })
    });

    if let Value::Object(ref mut map) = entry {
        for (env_k, env_v) in edgex_vars {
            if let Some(rest) = env_k.strip_prefix("EDGEX__") {
                let mut parts = rest.split("__");
                let (Some(section), Some(field)) = (parts.next(), parts.next()) else {
                    continue;
                };
                if section.eq_ignore_ascii_case("DEFAULT") {
                    let field_lower = field.to_ascii_lowercase();
                    match field_lower.as_str() {
                        "port" => {
                            if let Ok(p) = env_v.parse::<u64>() {
                                map.insert("port".to_string(), Value::from(p));
                            }
                        }
                        "server" => {
                            map.insert("server".to_string(), Value::String(env_v));
                        }
                        "topic" => {
                            map.insert("topic".to_string(), Value::String(env_v));
                        }
                        "protocol" => {
                            map.insert("protocol".to_string(), Value::String(env_v));
                        }
                        "type" => {
                            map.insert("type".to_string(), Value::String(env_v));
                        }
                        "messagetype" => {
                            map.insert("messageType".to_string(), Value::String(env_v));
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}

/// JWT authorization guard for deployments with `basic.authentication:
/// true`. eKuiper accepts only a raw RS256 JWT (no `Bearer ` prefix); `/`
/// and `/ping` stay public.
pub async fn auth_guard(
    State(state): State<AppState>,
    request: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> Response {
    if !state.config.read().basic.authentication {
        return next.run(request).await;
    }
    let path = request.uri().path();
    if path == "/" || path == "/ping" {
        return next.run(request).await;
    }
    let Some(header_val) = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    else {
        return (StatusCode::UNAUTHORIZED, "Missing authorization header\n").into_response();
    };
    if header_val.starts_with("Bearer ") || header_val.starts_with("bearer ") {
        return (
            StatusCode::UNAUTHORIZED,
            "Bearer token is not supported, please use raw JWT token\n",
        )
            .into_response();
    }
    let Some(key_der) = load_auth_public_key() else {
        return (
            StatusCode::UNAUTHORIZED,
            "Authentication misconfigured: public key not found\n",
        )
            .into_response();
    };
    match verify_jwt_raw(header_val.trim(), &key_der) {
        Ok(()) => next.run(request).await,
        Err(msg) => (StatusCode::UNAUTHORIZED, msg).into_response(),
    }
}

/// Verify a raw RS256 JWT against a PKCS#1 DER public key: exactly three
/// dot-separated segments, a JSON header declaring `"alg": "RS256"`, a JSON
/// payload whose optional `exp` (seconds) must lie in the future, and a
/// PKCS#1 v1.5 SHA-256 signature over `header_b64.payload_b64`.
pub fn verify_jwt_raw(token: &str, public_key_der: &[u8]) -> Result<(), &'static str> {
    use base64::Engine;
    let mut parts = token.split('.');
    let (Some(header_b64), Some(payload_b64), Some(sig_b64), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err("Invalid JWT format\n");
    };
    let engine = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let header_bytes = engine
        .decode(header_b64)
        .map_err(|_| "Invalid JWT format\n")?;
    let header: serde_json::Value =
        serde_json::from_slice(&header_bytes).map_err(|_| "Invalid JWT format\n")?;
    if header.get("alg").and_then(|v| v.as_str()) != Some("RS256") {
        return Err("Invalid JWT format\n");
    }
    let payload_bytes = engine
        .decode(payload_b64)
        .map_err(|_| "Invalid JWT format\n")?;
    let payload: serde_json::Value =
        serde_json::from_slice(&payload_bytes).map_err(|_| "Invalid JWT format\n")?;
    if let Some(exp) = payload.get("exp").and_then(|v| v.as_i64()) {
        if chrono::Utc::now().timestamp() > exp {
            return Err("Token has expired\n");
        }
    }
    let signature = engine.decode(sig_b64).map_err(|_| "Invalid JWT format\n")?;
    let message = format!("{}.{}", header_b64, payload_b64);
    let key = ring::signature::UnparsedPublicKey::new(
        &ring::signature::RSA_PKCS1_2048_8192_SHA256,
        public_key_der,
    );
    key.verify(message.as_bytes(), &signature)
        .map_err(|_| "Invalid token signature\n")
}

/// Split one PEM block into its label and DER bytes.
pub fn parse_pem_block(pem: &[u8]) -> Option<(String, Vec<u8>)> {
    use base64::Engine;
    let text = std::str::from_utf8(pem).ok()?;
    let begin = text.find("-----BEGIN ")?;
    let after_begin = &text[begin + "-----BEGIN ".len()..];
    let label_end = after_begin.find("-----")?;
    let label = after_begin[..label_end].trim().to_string();
    let rest = &after_begin[label_end + "-----".len()..];
    let end = rest.find("-----END ")?;
    let body: String = rest[..end].chars().filter(|c| !c.is_whitespace()).collect();
    let der = base64::engine::general_purpose::STANDARD
        .decode(&body)
        .ok()?;
    Some((label, der))
}

/// Read one DER tag-length-value triple: returns (tag, content, rest).
/// Only definite-form lengths are accepted.
pub fn der_read_tlv(input: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    if input.len() < 2 {
        return None;
    }
    let tag = input[0];
    let (len, header_len) = if input[1] < 0x80 {
        (input[1] as usize, 2)
    } else {
        let count = (input[1] & 0x7f) as usize;
        if count == 0 || count > 4 || input.len() < 2 + count {
            return None;
        }
        let mut len = 0usize;
        for b in &input[2..2 + count] {
            len = len.checked_mul(256)?.checked_add(*b as usize)?;
        }
        (len, 2 + count)
    };
    if input.len() < header_len + len {
        return None;
    }
    Some((
        tag,
        &input[header_len..header_len + len],
        &input[header_len + len..],
    ))
}

/// Unwrap an X.509 SPKI (`PUBLIC KEY`) DER container down to the bare
/// PKCS#1 RSAPublicKey DER that `ring`'s RSA verifier expects. The
/// algorithm must be rsaEncryption (1.2.840.113549.1.1.1).
pub fn spki_to_pkcs1(spki: &[u8]) -> Option<Vec<u8>> {
    const RSA_ENCRYPTION_OID: &[u8] = &[
        0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x01,
    ];
    let (tag, content, rest) = der_read_tlv(spki)?;
    if tag != 0x30 || !rest.is_empty() {
        return None;
    }
    let (alg_tag, alg_content, key_rest) = der_read_tlv(content)?;
    if alg_tag != 0x30
        || !alg_content
            .windows(RSA_ENCRYPTION_OID.len())
            .any(|w| w == RSA_ENCRYPTION_OID)
    {
        return None;
    }
    let (bits_tag, bits_content, key_end) = der_read_tlv(key_rest)?;
    if bits_tag != 0x03 || !key_end.is_empty() {
        return None;
    }
    // First BIT STRING byte is the unused-bits count, always zero here.
    let pkcs1 = bits_content.strip_prefix(&[0x00])?;
    Some(pkcs1.to_vec())
}

struct CachedAuthKey {
    path: std::path::PathBuf,
    modified: Option<std::time::SystemTime>,
    der: Vec<u8>,
}

static AUTH_KEY_CACHE: parking_lot::RwLock<Option<CachedAuthKey>> = parking_lot::RwLock::new(None);

pub fn load_auth_public_key() -> Option<Vec<u8>> {
    let mut candidates = Vec::new();
    if let Ok(path) = std::env::var("REKUIPER_AUTH_PUBLIC_KEY_FILE") {
        if !path.trim().is_empty() {
            candidates.push(std::path::PathBuf::from(path));
        }
    }
    if let Ok(path) = std::env::var("KUIPER_AUTH_PUBLIC_KEY_FILE") {
        if !path.trim().is_empty() {
            candidates.push(std::path::PathBuf::from(path));
        }
    }
    candidates.push(std::path::PathBuf::from("etc/mgmt/public.pem"));
    candidates.push(std::path::PathBuf::from("etc/public.pem"));

    let (active_path, modified) = candidates.into_iter().find_map(|p| {
        let meta = std::fs::metadata(&p).ok()?;
        if !meta.is_file() {
            return None;
        }
        Some((p, meta.modified().ok()))
    })?;

    if let Some(cached) = AUTH_KEY_CACHE.read().as_ref() {
        if cached.path == active_path && cached.modified == modified {
            return Some(cached.der.clone());
        }
    }

    let bytes = std::fs::read(&active_path).ok()?;
    let (label, der) = parse_pem_block(&bytes)?;
    let pkcs1_der = match label.as_str() {
        "PUBLIC KEY" => spki_to_pkcs1(&der)?,
        "RSA PUBLIC KEY" => der,
        _ => return None,
    };

    *AUTH_KEY_CACHE.write() = Some(CachedAuthKey {
        path: active_path,
        modified,
        der: pkcs1_der.clone(),
    });
    Some(pkcs1_der)
}

/// Check whether an IP address belongs to private/internal networks:
/// loopback, link-local, RFC 1918, carrier-grade NAT, or unspecified.
pub fn is_private_or_internal_ip(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(ipv4) => {
            let octets = ipv4.octets();
            // 0.0.0.0/8 (current network / unspecified RFC 1122)
            octets[0] == 0
                // 127.0.0.0/8 (loopback RFC 1122)
                || ipv4.is_loopback()
                // 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16 (private RFC 1918)
                || ipv4.is_private()
                // 169.254.0.0/16 (link-local RFC 3927)
                || ipv4.is_link_local()
                // 100.64.0.0/10 (carrier-grade NAT RFC 6598)
                || (octets[0] == 100 && (octets[1] & 0b1100_0000) == 64)
                // 192.0.0.0/24 (IETF protocol assignments RFC 6890)
                || (octets[0] == 192 && octets[1] == 0 && octets[2] == 0)
                // 192.0.2.0/24 (TEST-NET-1 RFC 5737)
                || (octets[0] == 192 && octets[1] == 0 && octets[2] == 2)
                // 198.18.0.0/15 (benchmarking RFC 2544)
                || (octets[0] == 198 && (octets[1] & 0b1111_1110) == 18)
                // 198.51.100.0/24 (TEST-NET-2 RFC 5737)
                || (octets[0] == 198 && octets[1] == 51 && octets[2] == 100)
                // 203.0.113.0/24 (TEST-NET-3 RFC 5737)
                || (octets[0] == 203 && octets[1] == 0 && octets[2] == 113)
                // 255.255.255.255 (broadcast)
                || ipv4.is_broadcast()
        }
        std::net::IpAddr::V6(ipv6) => {
            // ::1 (loopback)
            ipv6.is_loopback()
                // :: (unspecified)
                || ipv6.is_unspecified()
                // fe80::/10 (link-local unicast)
                || (ipv6.segments()[0] & 0xffc0) == 0xfe80
                // fc00::/7 (unique local address RFC 4193, includes fd00::/8)
                || (ipv6.segments()[0] & 0xfe00) == 0xfc00
                // IPv4-mapped IPv6 address (::ffff:x.x.x.x)
                || match ipv6.to_ipv4() {
                    Some(ipv4) => is_private_or_internal_ip(std::net::IpAddr::V4(ipv4)),
                    None => false,
                }
        }
    }
}

pub fn named_entries(names: &[&str]) -> Value {
    Value::Array(names.iter().map(|n| json!({ "name": n })).collect())
}

/// Rejects resource names carrying characters that break routing or the
/// manager UI (mirrors eKuiper's validation FVT expectations).
#[allow(clippy::result_large_err)]
pub fn check_valid_name(name: &str) -> Result<(), Response> {
    if name.contains(' ')
        || name.contains("%20")
        || name.contains(';')
        || name.contains('/')
        || name.contains('\\')
    {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("name '{}' contains invalid characters", name),
        )
            .into_response());
    }
    Ok(())
}

/// Item representing a single nested request inside a POST /batch/req payload.
#[derive(Debug, Clone, Deserialize)]
pub struct BatchRequestItem {
    #[serde(alias = "action")]
    pub method: String,
    #[serde(alias = "url")]
    pub path: String,
    #[serde(default, alias = "params", alias = "payload")]
    pub body: Option<Value>,
}

/// Result of executing a single nested request inside POST /batch/req.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BatchResponseItem {
    pub code: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JavascriptUdf {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub script: String,
    #[serde(default, rename = "isAgg")]
    pub is_agg: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExternalFunction {
    #[serde(rename = "ServiceName")]
    pub service_name: String,
    #[serde(rename = "InterfaceName")]
    pub interface_name: String,
    #[serde(rename = "Addr")]
    pub addr: String,
    #[serde(rename = "MethodName")]
    pub method_name: String,
    #[serde(rename = "FuncName")]
    pub func_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceDetail {
    #[serde(default, rename = "About")]
    pub about: HashMap<String, Value>,
    #[serde(default, rename = "Interfaces")]
    pub interfaces: HashMap<String, Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub functions: Vec<ExternalFunction>,
}

fn new_bounded_boa_context() -> boa_engine::Context {
    let mut ctx = boa_engine::Context::default();
    ctx.runtime_limits_mut().set_loop_iteration_limit(100_000);
    ctx.runtime_limits_mut().set_recursion_limit(256);
    ctx
}

fn is_valid_js_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == '_' || first == '$') {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
}

pub fn compile_and_register_js_udf(udf: &JavascriptUdf) -> Result<(), String> {
    let script = udf.script.trim();
    if script.is_empty() {
        return Err("script cannot be empty".to_string());
    }
    let fn_name = udf.id.trim();
    if fn_name.is_empty() {
        return Err("id cannot be empty".to_string());
    }
    if !is_valid_js_identifier(fn_name) {
        return Err(format!(
            "invalid JavaScript UDF identifier '{}': must match [A-Za-z_$][A-Za-z0-9_$]*",
            fn_name
        ));
    }

    // Validate syntax and top-level execution with bounded runtime limits up front
    {
        let mut ctx = new_bounded_boa_context();
        if let Err(e) = ctx.eval(boa_engine::Source::from_bytes(script.as_bytes())) {
            return Err(format!("JavaScript compilation error: {:?}", e));
        }
    }

    let fn_name_owned = fn_name.to_string();
    let script_owned = script.to_string();
    // Hash script so updated scripts with the same UDF name re-evaluate cleanly
    let script_hash = {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        script_owned.hash(&mut hasher);
        hasher.finish()
    };

    thread_local! {
        static TLS_BOA: std::cell::RefCell<(
            boa_engine::Context,
            std::collections::HashMap<String, u64>,
        )> = std::cell::RefCell::new((
            new_bounded_boa_context(),
            std::collections::HashMap::new(),
        ));
    }

    let handler: rekuiper_core::plugin::UdfFn = Arc::new(move |args: &[Value]| {
        let args_json = serde_json::to_string(args).unwrap_or_else(|_| "[]".to_string());
        TLS_BOA.with(|cell| {
            let (context, loaded_scripts) = &mut *cell.borrow_mut();
            if loaded_scripts.get(&fn_name_owned).copied() != Some(script_hash) {
                if let Err(e) =
                    context.eval(boa_engine::Source::from_bytes(script_owned.as_bytes()))
                {
                    tracing::warn!("JS UDF {} script init error: {:?}", fn_name_owned, e);
                    return Value::Null;
                }
                loaded_scripts.insert(fn_name_owned.clone(), script_hash);
            }
            let invoke_code = format!(
                "JSON.stringify((function() {{ let r = {}.apply(null, {}); return r === undefined ? null : r; }})())",
                fn_name_owned, args_json
            );
            match context.eval(boa_engine::Source::from_bytes(invoke_code.as_bytes())) {
                Ok(res) => res
                    .as_string()
                    .and_then(|s| serde_json::from_str(&s.to_std_string_escaped()).ok())
                    .unwrap_or(Value::Null),
                Err(e) => {
                    tracing::warn!("JS UDF {} execution error: {:?}", fn_name_owned, e);
                    Value::Null
                }
            }
        })
    });

    rekuiper_core::plugin::get_global_udf_registry().register_udf(fn_name, handler);
    Ok(())
}

pub fn create_default_services() -> Arc<RwLock<HashMap<String, ServiceDetail>>> {
    Arc::new(RwLock::new(HashMap::new()))
}

pub fn create_default_js_udfs() -> Arc<RwLock<HashMap<String, JavascriptUdf>>> {
    Arc::new(RwLock::new(HashMap::new()))
}
