pub mod joins;
pub mod runtime;
pub mod sinks;
pub mod windows;

pub(crate) use joins::*;
pub(crate) use runtime::*;
pub(crate) use sinks::*;
pub(crate) use windows::*;

use crate::handlers::stmt_called_functions;
use crate::sink_cache::{self, SinkCache};
use crate::state::{maybe_trace_record, AppState, HttpPushEndpoint, TraceManager};
use parking_lot::RwLock;
use rekuiper_conf::KuiperConfig;
use rekuiper_connectors::{
    FileSource, FileSourceConfig, HttpPullConfig, HttpPullSource, KafkaConfig, KafkaSource,
    MqttConfig, MqttSource, PayloadFormat, RabbitMqConfig, RabbitMqSource, RedisSubSource,
    SimulatorConfig, SimulatorSource, SqlConnectorConfig, SqlSource, VideoSource,
    VideoSourceConfig, WebSocketConfig, WebSocketSource, ZmqSource, ZmqSourceConfig,
};
use rekuiper_core::model::{RuleStatus, StreamField, StreamRecord};
use rekuiper_core::{
    RuleCounters, RuleManager, SchemaManager, StreamBus, StreamManager, StreamReceiver,
    TableDefinition, TableManager,
};
use rekuiper_sql::{Parser, SelectStmt, WindowDef};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// Starts background source producers (HTTP pull, WebSocket, RedisSub, Kafka,
/// simulator) for a rule based on its source stream type. Shared by rule
/// creation and daemon-bootstrap restore.
/// Resolve the MQTT source configuration for a rule's stream.
///
/// MQTT is the default streaming source: streams with no `TYPE`, an empty
/// `TYPE`, or `TYPE="mqtt"` ingest from the broker. Any other non-empty
/// `TYPE` (recognized sources like `kafka`, or anything else) resolves to
/// `None` here so exactly one source bootstrap owns the stream.
pub(crate) fn resolve_mqtt_source(
    stream_manager: &StreamManager,
    source_configs: &Arc<RwLock<HashMap<String, Value>>>,
    schemas: &SchemaManager,
    stream_name: &str,
    rule_id: &str,
) -> Option<MqttConfig> {
    let def = stream_manager.get_stream(stream_name)?;
    if let Some(kind) = def.options.get("TYPE") {
        if !kind.trim().is_empty() && !kind.eq_ignore_ascii_case("mqtt") {
            return None;
        }
    }
    let mut config = MqttConfig::default();
    // CONF_KEY lookup is case-insensitive (`CONF_KEY`, `conf_key`,
    // `confKey`): SQL definitions and imported/JSON definitions disagree on
    // case. Stored configs typically carry only connection parameters, so a
    // failed full decode still salvages server/credentials field by field.
    let conf_key = def
        .options
        .iter()
        .find(|(k, _)| {
            k.eq_ignore_ascii_case("CONF_KEY")
                || k.eq_ignore_ascii_case("confKey")
                || k.eq_ignore_ascii_case("connectionSelector")
        })
        .map(|(_, v)| v.trim());
    if let Some(key) = conf_key {
        if !key.is_empty() {
            let lookup1 = format!("mqtt/{}", key);
            let configs_guard = source_configs.read();
            let conf_val = configs_guard
                .get(&lookup1)
                .or_else(|| configs_guard.get(key))
                .or_else(|| configs_guard.get(&format!("connections/{}", key)))
                .cloned();
            drop(configs_guard);
            if let Some(val) = conf_val {
                if let Ok(stored) = serde_json::from_value::<MqttConfig>(val.clone()) {
                    config = stored;
                } else if let Some(srv) = val.get("server").and_then(|v| v.as_str()) {
                    if !srv.trim().is_empty() {
                        config.server = srv.to_string();
                    }
                    if let Some(u) = val.get("username").and_then(|v| v.as_str()) {
                        config.username = Some(u.to_string());
                    }
                    if let Some(p) = val.get("password").and_then(|v| v.as_str()) {
                        config.password = Some(p.to_string());
                    }
                } else {
                    tracing::warn!(
                        "[RULE {}] invalid mqtt config '{}': no server field",
                        rule_id,
                        lookup1,
                    );
                }
            }
        }
    }
    // Direct SERVER option overrides the configuration key (case-insensitive).
    let server_opt = def
        .options
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("SERVER"))
        .map(|(_, v)| v.trim());
    if let Some(srv) = server_opt {
        if !srv.is_empty() {
            config.server = srv.to_string();
        }
    }
    // DATASOURCE / topic (case-insensitive).
    let topic_opt = def
        .options
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("DATASOURCE") || k.eq_ignore_ascii_case("topic"))
        .map(|(_, v)| v.trim());
    if let Some(top) = topic_opt {
        if !top.is_empty() {
            config.topic = top.to_string();
        }
    }
    if config.topic.trim().is_empty() {
        config.topic = stream_name.to_string();
    }
    let client_id = def
        .options
        .get("CLIENTID")
        .or_else(|| def.options.get("CLIENT_ID"));
    if let Some(id) = client_id {
        if !id.trim().is_empty() {
            config.client_id = Some(id.clone());
        }
    }
    if let Some(user) = def.options.get("USERNAME") {
        if !user.is_empty() {
            config.username = Some(user.clone());
        }
    }
    if let Some(pass) = def.options.get("PASSWORD") {
        if !pass.is_empty() {
            config.password = Some(pass.clone());
        }
    }
    if let Some(qos) = def.options.get("QOS") {
        match qos.trim().parse::<u8>() {
            Ok(q) => config.qos = q,
            Err(_) => {
                tracing::warn!(
                    "[RULE {}] invalid mqtt QOS '{}', keeping {}",
                    rule_id,
                    qos,
                    config.qos
                );
            }
        }
    }
    config.format = resolve_payload_format(schemas, &def, rule_id)?;
    Some(config)
}

/// Resolve the MQTT configuration and EdgeX payload codec for a stream declaring
/// `TYPE="edgex"`. Default server is `tcp://edgex-mqtt-broker:1883` and default topic
/// is `edgex/rules-events`, configured via `etc/sources/edgex.yaml` or `EDGEX__*` overlays.
pub(crate) fn resolve_edgex_source(
    stream_manager: &StreamManager,
    source_configs: &Arc<RwLock<HashMap<String, Value>>>,
    _schemas: &SchemaManager,
    stream_name: &str,
    rule_id: &str,
) -> Option<MqttConfig> {
    let def = stream_manager.get_stream(stream_name)?;
    let kind = def.options.get("TYPE")?;
    if !kind.eq_ignore_ascii_case("edgex") {
        return None;
    }

    let mut config = MqttConfig {
        server: "tcp://edgex-mqtt-broker:1883".to_string(),
        topic: "edgex/rules-events".to_string(),
        format: PayloadFormat::EdgeX,
        ..Default::default()
    };

    let conf_key = def
        .options
        .iter()
        .find(|(k, _)| {
            k.eq_ignore_ascii_case("CONF_KEY")
                || k.eq_ignore_ascii_case("confKey")
                || k.eq_ignore_ascii_case("connectionSelector")
        })
        .map(|(_, v)| v.trim())
        .filter(|s| !s.is_empty())
        .unwrap_or("default");

    let lookup = format!("edgex/{}", conf_key);
    let configs_guard = source_configs.read();
    let conf_val = configs_guard
        .get(&lookup)
        .or_else(|| configs_guard.get(&lookup.to_ascii_lowercase()))
        .or_else(|| configs_guard.get("edgex/default"))
        .or_else(|| configs_guard.get(conf_key))
        .cloned();
    drop(configs_guard);

    if let Some(val) = conf_val {
        let srv = val
            .get("server")
            .and_then(|v| v.as_str())
            .unwrap_or("edgex-mqtt-broker");
        let port = val
            .get("port")
            .and_then(|v| {
                if let Some(n) = v.as_u64() {
                    Some(n)
                } else if let Some(s) = v.as_str() {
                    s.parse::<u64>().ok()
                } else {
                    None
                }
            })
            .unwrap_or(1883);
        let proto = val
            .get("protocol")
            .and_then(|v| v.as_str())
            .unwrap_or("tcp");

        if srv.contains("://") {
            config.server = srv.to_string();
        } else {
            config.server = format!("{}://{}:{}", proto, srv, port);
        }

        if let Some(t) = val.get("topic").and_then(|v| v.as_str()) {
            if !t.is_empty() {
                config.topic = t.to_string();
            }
        }
        if let Some(opt) = val.get("optional") {
            if let Some(u) = opt
                .get("Username")
                .or_else(|| opt.get("username"))
                .and_then(|v| v.as_str())
            {
                config.username = Some(u.to_string());
            }
            if let Some(p) = opt
                .get("Password")
                .or_else(|| opt.get("password"))
                .and_then(|v| v.as_str())
            {
                config.password = Some(p.to_string());
            }
            if let Some(cid) = opt
                .get("ClientId")
                .or_else(|| opt.get("clientId"))
                .and_then(|v| v.as_str())
            {
                config.client_id = Some(cid.to_string());
            }
        }
    }

    // Direct stream options take precedence
    if let Some(srv) = def.options.get("SERVER").filter(|s| !s.trim().is_empty()) {
        if srv.contains("://") {
            config.server = srv.to_string();
        } else {
            config.server = format!("tcp://{}", srv);
        }
    }
    if let Some(ds) = def
        .options
        .get("DATASOURCE")
        .filter(|s| !s.trim().is_empty())
    {
        config.topic = ds.trim().to_string();
    }

    tracing::info!(
        "[RULE {}] resolved edgex source for '{}': broker '{}', topic '{}'",
        rule_id,
        stream_name,
        config.server,
        config.topic
    );
    Some(config)
}

/// Payload decoding for a message source from the stream `FORMAT` (eKuiper
/// names: json, binary, delimited, protobuf with `SCHEMAID`). A protobuf
/// stream whose schema cannot be resolved yields `None` so the stream fails
/// loudly instead of decoding garbage; unknown formats fall back to JSON.
pub(crate) fn resolve_payload_format(
    schemas: &SchemaManager,
    def: &rekuiper_core::model::StreamDefinition,
    rule_id: &str,
) -> Option<rekuiper_connectors::PayloadFormat> {
    resolve_payload_format_options(
        schemas,
        &def.options,
        &def.stream_fields,
        &def.name,
        rule_id,
    )
}

pub(crate) fn resolve_payload_format_options(
    schemas: &SchemaManager,
    options: &HashMap<String, String>,
    stream_fields: &[StreamField],
    source_name: &str,
    rule_id: &str,
) -> Option<rekuiper_connectors::PayloadFormat> {
    use rekuiper_connectors::{DelimitedCodec, PayloadFormat};
    let opt = |name: &str| {
        options
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.trim())
    };
    let format = opt("FORMAT").unwrap_or("").to_ascii_lowercase();
    match format.as_str() {
        "" | "json" => Some(PayloadFormat::Json),
        "binary" => Some(PayloadFormat::Binary),
        "delimited" => {
            let delimiter = opt("DELIMITER")
                .filter(|d| !d.is_empty())
                .map(DelimitedCodec::delimiter_from_name)
                .unwrap_or(',');
            let headers = stream_fields.iter().map(|f| f.name.clone()).collect();
            Some(PayloadFormat::Delimited(DelimitedCodec::new(
                delimiter, headers,
            )))
        }
        "protobuf" => match resolve_proto_message(schemas, opt("SCHEMAID").unwrap_or("")) {
            Ok(message) => Some(PayloadFormat::Protobuf(Arc::new(message))),
            Err(e) => {
                tracing::warn!("[RULE {}] source '{}': {}", rule_id, source_name, e);
                None
            }
        },
        other => {
            tracing::warn!(
                "[RULE {}] source '{}': FORMAT '{}' is not supported for this source; decoding as JSON",
                rule_id,
                source_name,
                other
            );
            Some(PayloadFormat::Json)
        }
    }
}

/// Resolve `SCHEMAID` (`<schema>.<Message>`) against registered protobuf
/// schemas (inline content, else the schema file).
pub(crate) fn resolve_proto_message(
    schemas: &SchemaManager,
    schema_id: &str,
) -> Result<rekuiper_connectors::ProtoMessage, String> {
    let (schema, message) = schema_id.split_once('.').ok_or_else(|| {
        format!(
            "protobuf SCHEMAID '{}' must be <schema>.<message>",
            schema_id
        )
    })?;
    let def = schemas
        .get_schema("protobuf", schema)
        .ok_or_else(|| format!("protobuf schema '{}' is not registered", schema))?;
    let text = match (&def.content, &def.file) {
        (Some(content), _) if !content.trim().is_empty() => content.clone(),
        (_, Some(path)) => std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read protobuf schema file '{}': {}", path, e))?,
        _ => return Err(format!("protobuf schema '{}' has no content", schema)),
    };
    rekuiper_connectors::parse_proto(&text)?
        .remove(message)
        .ok_or_else(|| {
            format!(
                "message '{}' not found in protobuf schema '{}'",
                message, schema
            )
        })
}

pub(crate) fn cancel_stream_sources(state: &AppState, stream_name: &str) {
    if let Some(cancels) = state.stream_source_cancels.write().remove(stream_name) {
        for tx in cancels {
            let _ = tx.send(true);
        }
    }
    state.stream_active_rules.write().remove(stream_name);
    state.stream_attach_meta.write().remove(stream_name);
    state
        .http_push_endpoints
        .write()
        .retain(|_, ep| ep.stream_name != stream_name);
}

pub(crate) fn cancel_table_source(state: &AppState, table_name: &str) {
    let table_key = format!("$table/{}", table_name);
    if let Some(cancels) = state.stream_source_cancels.write().remove(&table_key) {
        for tx in cancels {
            let _ = tx.send(true);
        }
    }
}

pub(crate) fn resolve_mqtt_table_source(
    table_def: &TableDefinition,
    source_configs: &Arc<RwLock<HashMap<String, Value>>>,
    schemas: &SchemaManager,
) -> Option<MqttConfig> {
    if let Some(kind) = table_def.options.get("TYPE") {
        if !kind.trim().is_empty() && !kind.eq_ignore_ascii_case("mqtt") {
            return None;
        }
    }
    let mut config = MqttConfig::default();
    let conf_key = table_def
        .options
        .iter()
        .find(|(k, _)| {
            k.eq_ignore_ascii_case("CONF_KEY")
                || k.eq_ignore_ascii_case("confKey")
                || k.eq_ignore_ascii_case("connectionSelector")
        })
        .map(|(_, v)| v.trim());
    if let Some(key) = conf_key {
        if !key.is_empty() {
            let lookup1 = format!("mqtt/{}", key);
            let configs_guard = source_configs.read();
            let conf_val = configs_guard
                .get(&lookup1)
                .or_else(|| configs_guard.get(key))
                .or_else(|| configs_guard.get(&format!("connections/{}", key)))
                .cloned();
            drop(configs_guard);
            if let Some(val) = conf_val {
                if let Ok(stored) = serde_json::from_value::<MqttConfig>(val.clone()) {
                    config = stored;
                } else if let Some(srv) = val.get("server").and_then(|v| v.as_str()) {
                    if !srv.trim().is_empty() {
                        config.server = srv.to_string();
                    }
                    if let Some(u) = val.get("username").and_then(|v| v.as_str()) {
                        config.username = Some(u.to_string());
                    }
                    if let Some(p) = val.get("password").and_then(|v| v.as_str()) {
                        config.password = Some(p.to_string());
                    }
                }
            }
        }
    }
    if let Some((_, srv)) = table_def
        .options
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("SERVER"))
    {
        let srv = srv.trim();
        if !srv.is_empty() {
            config.server = srv.to_string();
        }
    }
    if let Some((_, top)) = table_def
        .options
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("DATASOURCE") || k.eq_ignore_ascii_case("topic"))
    {
        let top = top.trim();
        if !top.is_empty() {
            config.topic = top.to_string();
        }
    }
    if config.topic.trim().is_empty() {
        config.topic = table_def.name.clone();
    }
    if let Some(id) = table_def
        .options
        .get("CLIENTID")
        .or_else(|| table_def.options.get("CLIENT_ID"))
    {
        if !id.trim().is_empty() {
            config.client_id = Some(id.clone());
        }
    }
    if let Some(user) = table_def.options.get("USERNAME") {
        if !user.is_empty() {
            config.username = Some(user.clone());
        }
    }
    if let Some(pass) = table_def.options.get("PASSWORD") {
        if !pass.is_empty() {
            config.password = Some(pass.clone());
        }
    }
    if let Some(qos) = table_def.options.get("QOS") {
        if let Ok(q) = qos.trim().parse::<u8>() {
            config.qos = q;
        }
    }
    config.format = resolve_payload_format_options(
        schemas,
        &table_def.options,
        &table_def.stream_fields,
        &table_def.name,
        &table_def.name,
    )?;
    Some(config)
}

pub(crate) fn bootstrap_table_source(state: &AppState, table_name: &str) {
    let table_key = format!("$table/{}", table_name);
    if state.stream_source_cancels.read().contains_key(&table_key) {
        return;
    }
    let Some(def) = state.table_manager.get_table(table_name) else {
        return;
    };
    let datasource = def
        .options
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("DATASOURCE") || k.eq_ignore_ascii_case("TOPIC"))
        .map(|(_, v)| v.trim().to_string())
        .unwrap_or_else(|| table_name.to_string());
    let table_type = def
        .options
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("TYPE"))
        .map(|(_, v)| v.trim().to_string())
        .unwrap_or_default();

    let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
    state
        .stream_source_cancels
        .write()
        .insert(table_key.clone(), vec![cancel_tx]);

    let t_name = table_name.to_string();
    let table_mgr = state.table_manager.clone();

    // 1. Listen on the stream bus for this table's datasource topic (handles memory sinks and internal publication)
    let mut bus_rx = state.stream_bus.subscribe(&datasource);
    let mut table_bus_rx = if datasource != table_name {
        Some(state.stream_bus.subscribe(table_name))
    } else {
        None
    };
    let mut bus_cancel_rx = cancel_rx.clone();
    let t_name_bus = t_name.clone();
    let table_mgr_bus = table_mgr.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                record = bus_rx.recv() => {
                    match record {
                        Some(rec) => table_mgr_bus.insert_table_row(&t_name_bus, rec.data),
                        None => break,
                    }
                }
                record = async {
                    if let Some(ref mut rx) = table_bus_rx {
                        rx.recv().await
                    } else {
                        std::future::pending().await
                    }
                } => {
                    match record {
                        Some(rec) => table_mgr_bus.insert_table_row(&t_name_bus, rec.data),
                        None => break,
                    }
                }
                changed = bus_cancel_rx.changed() => {
                    if changed.is_err() || *bus_cancel_rx.borrow() {
                        break;
                    }
                }
            }
        }
    });

    // 2. If table is MQTT source (TYPE="mqtt" or default when not memory/file/redis/sql):
    let is_memory = table_type.eq_ignore_ascii_case("memory");
    let is_redis = table_type.eq_ignore_ascii_case("redis");
    let is_sql = table_type.eq_ignore_ascii_case("sql");
    let is_file = table_type.eq_ignore_ascii_case("file");
    if !is_memory && !is_redis && !is_sql && !is_file {
        if let Some(config) =
            resolve_mqtt_table_source(&def, &state.source_configs, &state.schema_manager)
        {
            let bus_topic = format!("$table_mqtt/{}", table_name);
            let stream_tx = state.stream_bus.get_or_create(&bus_topic);
            let mut mqtt_rx = state.stream_bus.subscribe(&bus_topic);
            let mut mqtt_cancel = cancel_rx.clone();
            let t_name_mqtt = t_name.clone();
            let table_mgr_mqtt = table_mgr.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        record = mqtt_rx.recv() => {
                            match record {
                                Some(rec) => table_mgr_mqtt.insert_table_row(&t_name_mqtt, rec.data),
                                None => break,
                            }
                        }
                        changed = mqtt_cancel.changed() => {
                            if changed.is_err() || *mqtt_cancel.borrow() {
                                break;
                            }
                        }
                    }
                }
            });
            MqttSource::new(config, stream_tx).spawn(cancel_rx.clone());
        }
    }
}

pub(crate) fn resolve_httppush_source(
    stream_manager: &StreamManager,
    source_configs: &Arc<RwLock<HashMap<String, Value>>>,
    stream_name: &str,
) -> Option<HttpPushEndpoint> {
    let def = stream_manager.get_stream(stream_name)?;
    let is_push = def
        .options
        .get("TYPE")
        .is_some_and(|t| t.eq_ignore_ascii_case("httppush") || t.eq_ignore_ascii_case("http_push"));
    if !is_push {
        return None;
    }

    let raw_path = def
        .options
        .get("DATASOURCE")
        .or_else(|| def.options.get("ENDPOINT"))
        .or_else(|| def.options.get("PATH"))
        .map(|s| s.trim())
        .unwrap_or("");
    let path = if raw_path.is_empty() {
        format!("/{}", stream_name)
    } else {
        format!("/{}", raw_path.trim_start_matches('/'))
    };

    let mut method = "POST".to_string();
    if let Some(m) = def.options.get("METHOD") {
        if !m.trim().is_empty() {
            method = m.trim().to_ascii_uppercase();
        }
    } else if let Some(key) = def.options.get("CONF_KEY").map(|k| k.trim()) {
        if !key.is_empty() {
            let lookup = format!("httppush/{}", key);
            let guard = source_configs.read();
            if let Some(val) = guard.get(&lookup).or_else(|| guard.get(key)) {
                if let Some(m) = val.get("method").and_then(|v| v.as_str()) {
                    method = m.trim().to_ascii_uppercase();
                }
            }
        }
    }

    let format = def
        .options
        .get("FORMAT")
        .map(|f| f.trim().to_ascii_lowercase())
        .unwrap_or_else(|| "json".to_string());

    Some(HttpPushEndpoint {
        stream_name: stream_name.to_string(),
        path,
        method,
        format,
    })
}

pub(crate) fn register_rule_stream_source(
    state: &AppState,
    stream_name: &str,
    rule_id: &str,
    cancel_tx: tokio::sync::watch::Sender<bool>,
) {
    state
        .stream_source_cancels
        .write()
        .entry(stream_name.to_string())
        .or_default()
        .push(cancel_tx);
    state
        .stream_active_rules
        .write()
        .entry(stream_name.to_string())
        .or_default()
        .insert(rule_id.to_string());
    state
        .rule_streams
        .write()
        .entry(rule_id.to_string())
        .or_default()
        .insert(stream_name.to_string());
    let (rule_cancel_tx, _rule_cancel_rx) = tokio::sync::watch::channel(false);
    state
        .source_cancels
        .write()
        .entry(rule_id.to_string())
        .or_default()
        .push(rule_cancel_tx);
}

/// Start source producers for one stream (MQTT/file/HTTP-pull/WebSocket/
/// RedisSub/Kafka/SQL/simulator, whichever its TYPE declares). `needs_meta`
/// asks message sources to attach per-message metadata for `meta()`.
/// Multiple rules reading from the same stream share a single underlying
/// source producer, preventing duplicate message ingestion.
pub(crate) fn bootstrap_stream_sources(
    state: &AppState,
    rule_id: &str,
    stream_name: &str,
    needs_meta: bool,
) {
    // If this rule is already registered for this stream, avoid duplicate registration.
    if let Some(rules) = state.stream_active_rules.read().get(stream_name) {
        if rules.contains(rule_id) {
            if needs_meta {
                if let Some(flag) = state.stream_attach_meta.read().get(stream_name) {
                    flag.store(true, std::sync::atomic::Ordering::Relaxed);
                }
            }
            return;
        }
    }

    // If a source task is already running for this stream, register this rule as
    // another consumer of the stream and do not spawn a duplicate source.
    if state.stream_source_cancels.read().contains_key(stream_name) {
        state
            .stream_active_rules
            .write()
            .entry(stream_name.to_string())
            .or_default()
            .insert(rule_id.to_string());
        state
            .rule_streams
            .write()
            .entry(rule_id.to_string())
            .or_default()
            .insert(stream_name.to_string());
        if needs_meta {
            if let Some(flag) = state.stream_attach_meta.read().get(stream_name) {
                flag.store(true, std::sync::atomic::Ordering::Relaxed);
            }
        }
        let (rule_cancel_tx, _rule_cancel_rx) = tokio::sync::watch::channel(false);
        state
            .source_cancels
            .write()
            .entry(rule_id.to_string())
            .or_default()
            .push(rule_cancel_tx);
        return;
    }

    // HTTP Push source streams listen on the configured HTTP data server endpoint.
    if let Some(endpoint) =
        resolve_httppush_source(&state.stream_manager, &state.source_configs, stream_name)
    {
        let (cancel_tx, mut cancel_rx) = tokio::sync::watch::channel(false);
        register_rule_stream_source(state, stream_name, rule_id, cancel_tx);
        let path = endpoint.path.clone();
        state
            .http_push_endpoints
            .write()
            .insert(path.clone(), endpoint);

        let state_clone = state.clone();
        let stream_name_clone = stream_name.to_string();
        tokio::spawn(async move {
            let _ = cancel_rx.changed().await;
            let mut guard = state_clone.http_push_endpoints.write();
            if let Some(ep) = guard.get(&path) {
                if ep.stream_name == stream_name_clone {
                    guard.remove(&path);
                }
            }
        });
        return;
    }

    // MQTT is the default streaming source: typeless streams and TYPE="mqtt"
    // subscribe to the broker topic and feed the rule pipeline.
    if let Some(mut config) = resolve_mqtt_source(
        &state.stream_manager,
        &state.source_configs,
        &state.schema_manager,
        stream_name,
        rule_id,
    ) {
        let meta_flag = Arc::new(std::sync::atomic::AtomicBool::new(needs_meta));
        config.attach_meta = needs_meta;
        state
            .stream_attach_meta
            .write()
            .insert(stream_name.to_string(), meta_flag.clone());

        let stream_tx = state.stream_bus.get_or_create(stream_name);
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        register_rule_stream_source(state, stream_name, rule_id, cancel_tx);
        MqttSource::new(config, stream_tx)
            .with_rule_counters(state.rule_manager.rule_counters(rule_id))
            .with_meta_flag(Some(meta_flag))
            .spawn(cancel_rx);
        return;
    }

    // EdgeX source streams connect to EdgeX MQTT message bus and decode Event / Reading DTOs.
    if let Some(mut config) = resolve_edgex_source(
        &state.stream_manager,
        &state.source_configs,
        &state.schema_manager,
        stream_name,
        rule_id,
    ) {
        let meta_flag = Arc::new(std::sync::atomic::AtomicBool::new(needs_meta));
        config.attach_meta = needs_meta;
        state
            .stream_attach_meta
            .write()
            .insert(stream_name.to_string(), meta_flag.clone());

        let stream_tx = state.stream_bus.get_or_create(stream_name);
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        register_rule_stream_source(state, stream_name, rule_id, cancel_tx);
        MqttSource::new(config, stream_tx)
            .with_rule_counters(state.rule_manager.rule_counters(rule_id))
            .with_meta_flag(Some(meta_flag))
            .spawn(cancel_rx);
        return;
    }

    // File source streams tail a line-delimited file into the stream bus.
    if let Some(config) = resolve_file_source(
        &state.stream_manager,
        &state.source_configs,
        stream_name,
        rule_id,
    ) {
        let stream_tx = state.stream_bus.get_or_create(stream_name);
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        register_rule_stream_source(state, stream_name, rule_id, cancel_tx);
        FileSource::new(config, stream_tx).spawn(cancel_rx);
        return;
    }

    // HTTP pull source streams poll a remote endpoint into the stream bus.
    if let Some(conf) = resolve_httppull_config(
        &state.stream_manager,
        &state.source_configs,
        stream_name,
        rule_id,
    ) {
        let stream_tx = state.stream_bus.get_or_create(stream_name);
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        register_rule_stream_source(state, stream_name, rule_id, cancel_tx);
        HttpPullSource {
            config: conf,
            tx: stream_tx,
        }
        .spawn(cancel_rx);
        return;
    }

    // WebSocket source streams forward incoming messages into the stream bus.
    if let Some(url) = resolve_websocket_url(
        &state.stream_manager,
        &state.source_configs,
        stream_name,
        rule_id,
    ) {
        let stream_tx = state.stream_bus.get_or_create(stream_name);
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        register_rule_stream_source(state, stream_name, rule_id, cancel_tx);
        WebSocketSource { url, tx: stream_tx }.spawn(cancel_rx);
        return;
    }

    // Redis subscription streams forward channel messages into the stream bus.
    if let Some((url, channel)) = resolve_redissub_source(
        &state.stream_manager,
        &state.source_configs,
        stream_name,
        rule_id,
    ) {
        let stream_tx = state.stream_bus.get_or_create(stream_name);
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        register_rule_stream_source(state, stream_name, rule_id, cancel_tx);
        RedisSubSource {
            url,
            channel,
            tx: stream_tx,
        }
        .spawn(cancel_rx);
        return;
    }

    // Kafka source streams consume a topic partition into the stream bus.
    if let Some(config) = resolve_kafka_source(
        &state.stream_manager,
        &state.source_configs,
        stream_name,
        rule_id,
    ) {
        let stream_tx = state.stream_bus.get_or_create(stream_name);
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        register_rule_stream_source(state, stream_name, rule_id, cancel_tx);
        KafkaSource {
            config,
            tx: stream_tx,
        }
        .spawn(cancel_rx);
        return;
    }

    // SQL source streams poll a database table into the stream bus.
    if let Some(config) = resolve_sql_source(
        &state.stream_manager,
        &state.source_configs,
        stream_name,
        rule_id,
    ) {
        let stream_tx = state.stream_bus.get_or_create(stream_name);
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        register_rule_stream_source(state, stream_name, rule_id, cancel_tx);
        SqlSource::new(config, stream_tx).spawn(cancel_rx);
        return;
    }

    // RabbitMQ source streams consume an AMQP queue into the stream bus.
    if let Some(def) = state.stream_manager.get_stream(stream_name) {
        let is_rmq = def
            .options
            .get("TYPE")
            .is_some_and(|t| t.eq_ignore_ascii_case("rabbitmq") || t.eq_ignore_ascii_case("amqp"));
        if is_rmq {
            let mut config = RabbitMqConfig::default();
            if let Some(srv) = def.options.get("SERVER").or_else(|| def.options.get("URL")) {
                config.server = srv.clone();
            }
            if let Some(q) = def
                .options
                .get("QUEUE")
                .or_else(|| def.options.get("DATASOURCE"))
            {
                config.queue = q.clone();
            }
            if let Some(ex) = def.options.get("EXCHANGE") {
                config.exchange = ex.clone();
            }
            if let Some(rk) = def
                .options
                .get("ROUTINGKEY")
                .or_else(|| def.options.get("ROUTING_KEY"))
            {
                config.routing_key = rk.clone();
            }
            if let Some(u) = def
                .options
                .get("USERNAME")
                .or_else(|| def.options.get("USER"))
            {
                config.username = Some(u.clone());
            }
            if let Some(p) = def.options.get("PASSWORD") {
                config.password = Some(p.clone());
            }
            if let Some(key) = def.options.get("CONF_KEY").cloned() {
                let lookup = format!("rabbitmq/{}", key);
                if let Some(conf_val) = state.source_configs.read().get(&lookup).cloned() {
                    if let Ok(c) = serde_json::from_value::<RabbitMqConfig>(conf_val) {
                        config = c;
                    }
                }
            }
            let stream_tx = state.stream_bus.get_or_create(stream_name);
            let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
            register_rule_stream_source(state, stream_name, rule_id, cancel_tx);
            RabbitMqSource::new(config, stream_tx).spawn(cancel_rx);
            return;
        }
    }

    // ZeroMQ source streams subscribe to a ZeroMQ endpoint into the stream bus.
    if let Some(def) = state.stream_manager.get_stream(stream_name) {
        let is_zmq = def
            .options
            .get("TYPE")
            .is_some_and(|t| t.eq_ignore_ascii_case("zmq") || t.eq_ignore_ascii_case("zeromq"));
        if is_zmq {
            let mut config = ZmqSourceConfig::default();
            if let Some(srv) = def.options.get("SERVER").or_else(|| def.options.get("URL")) {
                config.server = srv.clone();
            }
            if let Some(topic) = def
                .options
                .get("DATASOURCE")
                .or_else(|| def.options.get("TOPIC"))
            {
                config.topic = Some(topic.clone());
            }
            if let Some(mode) = def.options.get("MODE") {
                config.mode = mode.clone();
            }
            if let Some(key) = def.options.get("CONF_KEY").cloned() {
                let lookup = format!("zmq/{}", key);
                if let Some(conf_val) = state.source_configs.read().get(&lookup).cloned() {
                    if let Ok(c) = serde_json::from_value::<ZmqSourceConfig>(conf_val) {
                        config = c;
                    }
                }
            }
            let stream_tx = state.stream_bus.get_or_create(stream_name);
            let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
            register_rule_stream_source(state, stream_name, rule_id, cancel_tx);
            ZmqSource::new(config, stream_tx).spawn(cancel_rx);
            return;
        }
    }

    // Video source streams extract camera frames via FFmpeg into the stream bus.
    if let Some(def) = state.stream_manager.get_stream(stream_name) {
        let is_video = def
            .options
            .get("TYPE")
            .is_some_and(|t| t.eq_ignore_ascii_case("video"));
        if is_video {
            let mut config = VideoSourceConfig::default();
            if let Some(url) = def
                .options
                .get("URL")
                .or_else(|| def.options.get("SERVER"))
                .or_else(|| def.options.get("DATASOURCE"))
            {
                config.url = url.clone();
            }
            if let Some(interval) = def.options.get("INTERVAL") {
                config.set_interval_str(interval);
            }
            if let Some(codec) = def.options.get("CODEC") {
                config.codec = codec.clone();
            }
            if let Some(debug) = def
                .options
                .get("DEBUG_RESP")
                .or_else(|| def.options.get("DEBUGRESP"))
            {
                config.debug_resp = debug.eq_ignore_ascii_case("true");
            }
            if let Some(ffmpeg) = def
                .options
                .get("FFMPEG_PATH")
                .or_else(|| def.options.get("FFMPEGPATH"))
            {
                config.ffmpeg_path = ffmpeg.clone();
            }
            if let Some(key) = def.options.get("CONF_KEY").cloned() {
                let lookup = format!("video/{}", key);
                if let Some(conf_val) = state.source_configs.read().get(&lookup).cloned() {
                    if let Ok(c) = serde_json::from_value::<VideoSourceConfig>(conf_val) {
                        config = c;
                    }
                }
            }
            let stream_tx = state.stream_bus.get_or_create(stream_name);
            let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
            register_rule_stream_source(state, stream_name, rule_id, cancel_tx);
            VideoSource::new(config, stream_tx).spawn(cancel_rx);
            return;
        }
    }

    // Simulator source streams replay configured data into the stream bus.
    // Stream options are upper-cased by the SQL parser.
    if let Some(def) = state.stream_manager.get_stream(stream_name) {
        let is_simulator = def
            .options
            .get("TYPE")
            .is_some_and(|t| t.eq_ignore_ascii_case("simulator") || t.eq_ignore_ascii_case("random"));
        if is_simulator {
            let key = def.options.get("CONF_KEY").cloned().unwrap_or_else(|| "default".to_string());
            let lookup = format!("simulator/{}", key);
            let lookup_random = format!("random/{}", key);
            let conf_val = state.source_configs.read().get(&lookup).cloned()
                .or_else(|| state.source_configs.read().get(&lookup_random).cloned());
            if let Some(conf_val) = conf_val {
                    match serde_json::from_value::<SimulatorConfig>(conf_val) {
                        Ok(conf) => {
                            let stream_name_str = stream_name.to_string();
                            let bus = state.stream_bus.clone();
                            let (cancel_tx, mut cancel_rx) = tokio::sync::watch::channel(false);
                            register_rule_stream_source(state, stream_name, rule_id, cancel_tx);
                            tokio::spawn(async move {
                                let (tx, mut rx) = tokio::sync::mpsc::channel::<StreamRecord>(1024);
                                let sim_handle = tokio::spawn(async move {
                                    SimulatorSource::new(conf).run(tx).await
                                });
                                loop {
                                    tokio::select! {
                                        record = rx.recv() => {
                                            match record {
                                                Some(record) => {
                                                    let _ = bus.publish_async(&stream_name_str, record).await;
                                                }
                                                None => break,
                                            }
                                        }
                                        changed = cancel_rx.changed() => {
                                            if changed.is_err() || *cancel_rx.borrow() {
                                                sim_handle.abort();
                                                break;
                                            }
                                        }
                                    }
                                }
                                let _ = sim_handle.await;
                            });
                        }
                        Err(e) => {
                            tracing::warn!(
                                "[RULE {}] invalid simulator config '{}': {}",
                                rule_id,
                                lookup,
                                e
                            );
                        }
                    }
                }
        }
    }
}

/// Start source producers for a rule: its FROM source plus every joined
/// source (streams fan in messages; table targets bootstrap their source listeners).
pub(crate) fn bootstrap_rule_sources(state: &AppState, rule_id: &str, select_stmt: &SelectStmt) {
    // Metadata costs an allocation per message: attach it only for rules
    // that read it.
    let needs_meta = stmt_called_functions(select_stmt)
        .iter()
        .any(|f| f.eq_ignore_ascii_case("meta") || f.eq_ignore_ascii_case("mqtt"));
    if state.table_manager.get_table(&select_stmt.from).is_some() {
        bootstrap_table_source(state, &select_stmt.from);
    } else {
        bootstrap_stream_sources(state, rule_id, &select_stmt.from, needs_meta);
    }
    for join in &select_stmt.joins {
        if state.table_manager.get_table(&join.target).is_some() {
            bootstrap_table_source(state, &join.target);
        } else if join.target != select_stmt.from {
            bootstrap_stream_sources(state, rule_id, &join.target, needs_meta);
        }
    }
}

/// Respawns execution tasks (plus source producers) for every rule whose
/// persisted status is `running`, so a restarted daemon resumes processing
/// without manual intervention.
pub async fn restore_running_rules(state: &AppState) {
    for table_name in state.table_manager.list_tables() {
        bootstrap_table_source(state, &table_name);
    }
    for rule in state.rule_manager.list_rules() {
        let running = state
            .rule_manager
            .get_rule_status(&rule.id)
            .is_some_and(|s| s.status == "running");
        if !running {
            continue;
        }
        let mut parser = Parser::new(&rule.sql);
        let select_stmt = match parser.parse_select() {
            Ok(stmt) => stmt,
            Err(e) => {
                tracing::warn!("Skipping restore of rule {}: invalid SQL: {}", rule.id, e);
                continue;
            }
        };
        spawn_rule_task(
            &state.rule_manager,
            &state.stream_bus,
            &state.stream_manager,
            &state.table_manager,
            &state.source_configs,
            &state.http_client,
            &state.trace_manager,
            &state.config,
            rule.id.clone(),
            select_stmt.clone(),
            rule.actions.clone(),
            rule.options.clone(),
        );
        bootstrap_rule_sources(state, &rule.id, &select_stmt);
        tracing::info!("Restored running rule {}", rule.id);
    }
}

/// Resolve the HTTP pull configuration for a rule whose source stream
/// declares `TYPE="httppull"` (or `"http_pull"`).
///
/// Looks up `httppull/{conf_key}` then `http_pull/{conf_key}` in the stored
/// source configs; when no key matches, falls back to the stream's
/// `DATASOURCE` property as the poll URL with default method/interval.
pub(crate) fn resolve_httppull_config(
    stream_manager: &StreamManager,
    source_configs: &Arc<RwLock<HashMap<String, Value>>>,
    stream_name: &str,
    rule_id: &str,
) -> Option<HttpPullConfig> {
    let def = stream_manager.get_stream(stream_name)?;
    let kind = def.options.get("TYPE")?;
    if !(kind.eq_ignore_ascii_case("httppull") || kind.eq_ignore_ascii_case("http_pull")) {
        return None;
    }
    if let Some(key) = def.options.get("CONF_KEY") {
        if !key.is_empty() {
            for prefix in ["httppull", "http_pull"] {
                let lookup = format!("{}/{}", prefix, key);
                if let Some(conf_val) = source_configs.read().get(&lookup).cloned() {
                    match serde_json::from_value::<HttpPullConfig>(conf_val) {
                        Ok(conf) => return Some(conf),
                        Err(e) => {
                            tracing::warn!(
                                "[RULE {}] invalid http pull config '{}': {}",
                                rule_id,
                                lookup,
                                e
                            );
                            return None;
                        }
                    }
                }
            }
        }
    }
    let url = def.options.get("DATASOURCE").cloned().unwrap_or_default();
    if url.is_empty() {
        tracing::warn!(
            "[RULE {}] http pull stream '{}' has neither CONF_KEY config nor DATASOURCE url",
            rule_id,
            stream_name
        );
        return None;
    }
    Some(HttpPullConfig {
        url,
        method: "get".to_string(),
        interval: 1000,
        headers: HashMap::new(),
        body: None,
    })
}

/// Resolve the WebSocket URL for a rule whose source stream declares
/// `TYPE="websocket"`.
///
/// Looks up `websocket/{conf_key}` in the stored source configs and builds
/// the URL from it; otherwise parses `DATASOURCE` (used directly when it
/// starts with `ws://` or `wss://`, else treated as a path on the default
/// local endpoint).
pub(crate) fn resolve_websocket_url(
    stream_manager: &StreamManager,
    source_configs: &Arc<RwLock<HashMap<String, Value>>>,
    stream_name: &str,
    rule_id: &str,
) -> Option<String> {
    let def = stream_manager.get_stream(stream_name)?;
    let kind = def.options.get("TYPE")?;
    if !kind.eq_ignore_ascii_case("websocket") {
        return None;
    }
    if let Some(key) = def.options.get("CONF_KEY") {
        if !key.is_empty() {
            let lookup = format!("websocket/{}", key);
            if let Some(conf_val) = source_configs.read().get(&lookup).cloned() {
                match serde_json::from_value::<WebSocketConfig>(conf_val) {
                    Ok(conf) => return Some(conf.target_url()),
                    Err(e) => {
                        tracing::warn!(
                            "[RULE {}] invalid websocket config '{}': {}",
                            rule_id,
                            lookup,
                            e
                        );
                        return None;
                    }
                }
            }
        }
    }
    let datasource = def.options.get("DATASOURCE").cloned().unwrap_or_default();
    if datasource.is_empty() {
        tracing::warn!(
            "[RULE {}] websocket stream '{}' has neither CONF_KEY config nor DATASOURCE",
            rule_id,
            stream_name
        );
        return None;
    }
    let ds = datasource.trim();
    if ds.starts_with("ws://") || ds.starts_with("wss://") {
        Some(ds.to_string())
    } else {
        Some(format!(
            "ws://127.0.0.1:8080/{}",
            ds.trim_start_matches('/')
        ))
    }
}

/// Resolve a Redis server address from a stored source config value, which
/// may be a full object (`{"addr": ...}`) or a bare address string.
/// Falls back to the local default when absent or unparseable.
pub(crate) fn resolve_redis_addr(
    source_configs: &Arc<RwLock<HashMap<String, Value>>>,
    conf_key: &str,
) -> String {
    const DEFAULT: &str = "127.0.0.1:6379";
    if conf_key.is_empty() {
        return DEFAULT.to_string();
    }
    let stored = source_configs
        .read()
        .get(&format!("redis/{}", conf_key))
        .cloned();
    match stored {
        Some(Value::Object(map)) => map
            .get("addr")
            .or_else(|| map.get("address"))
            .or_else(|| map.get("url"))
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or(DEFAULT)
            .to_string(),
        Some(Value::String(s)) if !s.is_empty() => s,
        _ => DEFAULT.to_string(),
    }
}

pub(crate) fn resolve_redissub_source(
    stream_manager: &StreamManager,
    source_configs: &Arc<RwLock<HashMap<String, Value>>>,
    stream_name: &str,
    rule_id: &str,
) -> Option<(String, String)> {
    let def = stream_manager.get_stream(stream_name)?;
    let kind = def.options.get("TYPE")?;
    if !(kind.eq_ignore_ascii_case("redissub") || kind.eq_ignore_ascii_case("redis_sub")) {
        return None;
    }
    let channel = def.options.get("DATASOURCE").cloned().unwrap_or_default();
    if channel.is_empty() {
        tracing::warn!(
            "[RULE {}] redissub stream '{}' has no DATASOURCE channel",
            rule_id,
            stream_name
        );
        return None;
    }
    let conf_key = def.options.get("CONF_KEY").cloned().unwrap_or_default();
    let addr = resolve_redis_addr(source_configs, &conf_key);
    let url = if addr.contains("://") {
        addr
    } else {
        format!("redis://{}", addr)
    };
    Some((url, channel))
}

/// Resolve the [`KafkaConfig`] for a rule whose source stream declares
/// `TYPE="kafka"`: the topic comes from the stream `DATASOURCE` (falling back
/// to the config topic), brokers and friends from `kafka/{conf_key}` (or
/// defaults when no key matches).
pub(crate) fn resolve_kafka_source(
    stream_manager: &StreamManager,
    source_configs: &Arc<RwLock<HashMap<String, Value>>>,
    stream_name: &str,
    rule_id: &str,
) -> Option<KafkaConfig> {
    let def = stream_manager.get_stream(stream_name)?;
    let kind = def.options.get("TYPE")?;
    if !kind.eq_ignore_ascii_case("kafka") {
        return None;
    }
    let mut config = KafkaConfig {
        brokers: "127.0.0.1:9092".to_string(),
        topic: None,
        group_id: None,
        partition: 0,
        key: None,
        offset_reset: "latest".to_string(),
    };
    if let Some(key) = def.options.get("CONF_KEY") {
        if !key.is_empty() {
            let lookup = format!("kafka/{}", key);
            if let Some(conf_val) = source_configs.read().get(&lookup).cloned() {
                match serde_json::from_value::<KafkaConfig>(conf_val) {
                    Ok(parsed) => config = parsed,
                    Err(e) => {
                        tracing::warn!(
                            "[RULE {}] invalid kafka config '{}': {}",
                            rule_id,
                            lookup,
                            e
                        );
                        return None;
                    }
                }
            }
        }
    }
    if let Some(reset) = def
        .options
        .get("OFFSET_RESET")
        .or_else(|| def.options.get("OFFSETRESET"))
    {
        if !reset.trim().is_empty() {
            config.offset_reset = reset.trim().to_string();
        }
    }
    match def.options.get("DATASOURCE").cloned() {
        Some(topic) if !topic.is_empty() => config.topic = Some(topic),
        _ if config.topic.as_deref().is_some_and(|t| !t.is_empty()) => {}
        _ => {
            tracing::warn!(
                "[RULE {}] kafka stream '{}' has no DATASOURCE topic",
                rule_id,
                stream_name
            );
            return None;
        }
    }
    if config.broker_list().is_empty() {
        tracing::warn!(
            "[RULE {}] kafka stream '{}' has no brokers configured",
            rule_id,
            stream_name
        );
        return None;
    }
    Some(config)
}

/// Resolve the [`FileSourceConfig`] for a rule whose source stream declares
/// `TYPE="file"`: the path comes from `DATASOURCE` (falling back to a stored
/// `file/{conf_key}` config), with `FORMAT`/`fileType`, `hasHeader` and
/// `delimiter` layered from stream options over config defaults.
pub(crate) fn resolve_file_source(
    stream_manager: &StreamManager,
    source_configs: &Arc<RwLock<HashMap<String, Value>>>,
    stream_name: &str,
    rule_id: &str,
) -> Option<FileSourceConfig> {
    use rekuiper_connectors::DelimitedCodec;

    let def = stream_manager.get_stream(stream_name)?;
    let kind = def.options.get("TYPE")?;
    if !kind.eq_ignore_ascii_case("file") {
        return None;
    }
    let mut config = FileSourceConfig {
        path: String::new(),
        format: "json".to_string(),
        has_header: false,
        delimiter: None,
        interval: 0,
    };
    if let Some(key) = def.options.get("CONF_KEY") {
        if !key.is_empty() {
            let lookup = format!("file/{}", key);
            if let Some(conf_val) = source_configs.read().get(&lookup).cloned() {
                match serde_json::from_value::<FileSourceConfig>(conf_val) {
                    Ok(parsed) => config = parsed,
                    Err(e) => {
                        tracing::warn!(
                            "[RULE {}] invalid file config '{}': {}",
                            rule_id,
                            lookup,
                            e
                        );
                        return None;
                    }
                }
            }
        }
    }
    if let Some(path) = def.options.get("DATASOURCE") {
        if !path.trim().is_empty() {
            config.path = path.clone();
        }
    }
    if config.path.trim().is_empty() {
        tracing::warn!(
            "[RULE {}] file stream '{}' has neither DATASOURCE path nor file config",
            rule_id,
            stream_name
        );
        return None;
    }
    if let Some(format) = def
        .options
        .get("FORMAT")
        .filter(|s| !s.trim().is_empty())
        .or_else(|| def.options.get("FILETYPE").filter(|s| !s.trim().is_empty()))
    {
        config.format = format.clone();
    }
    if let Some(flag) = def
        .options
        .get("HASHEADER")
        .or_else(|| def.options.get("HAS_HEADER"))
    {
        match flag.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" => config.has_header = true,
            "false" | "0" | "no" | "" => config.has_header = false,
            _ => {}
        }
    }
    if let Some(delim) = def.options.get("DELIMITER").filter(|s| !s.is_empty()) {
        config.delimiter = Some(DelimitedCodec::delimiter_from_name(delim));
    }
    Some(config)
}

/// Signal cancellation to a rule's background streaming sources.
/// Streams shared with other running rules remain active; when the last
/// rule consuming a stream stops, the stream's background source task
/// is cancelled and cleanly disconnected.
pub(crate) fn cancel_rule_source(state: &AppState, rule_id: &str) {
    if let Some(txs) = state.source_cancels.write().remove(rule_id) {
        for tx in txs {
            let _ = tx.send(true);
        }
    }
    let streams = state.rule_streams.write().remove(rule_id);
    if let Some(streams) = streams {
        let mut active_rules = state.stream_active_rules.write();
        let mut source_cancels = state.stream_source_cancels.write();
        let mut attach_meta = state.stream_attach_meta.write();
        for stream in streams {
            if let Some(rules) = active_rules.get_mut(&stream) {
                rules.remove(rule_id);
                if rules.is_empty() {
                    active_rules.remove(&stream);
                    attach_meta.remove(&stream);
                    if let Some(cancels) = source_cancels.remove(&stream) {
                        for tx in cancels {
                            let _ = tx.send(true);
                        }
                    }
                    state
                        .http_push_endpoints
                        .write()
                        .retain(|_, ep| ep.stream_name != stream);
                }
            }
        }
    }
}

/// Resolve the bus topic a rule actually subscribes to: memory-type streams
/// (`TYPE="memory"`) re-export another topic via `DATASOURCE`, mirroring the
/// eKuiper memory source (used to chain rules through memory sinks).
pub(crate) fn resolve_source_topic(stream_manager: &StreamManager, stream_name: &str) -> String {
    if let Some(def) = stream_manager.get_stream(stream_name) {
        if def
            .options
            .get("TYPE")
            .is_some_and(|t| t.eq_ignore_ascii_case("memory"))
        {
            if let Some(ds) = def.options.get("DATASOURCE") {
                if !ds.is_empty() {
                    return ds.clone();
                }
            }
        }
    }
    stream_name.to_string()
}

/// Subscribe to the rule's source stream and spawn its window-aware
/// execution task, registering the join handle on the rule manager.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_rule_task(
    rule_manager: &RuleManager,
    stream_bus: &StreamBus,
    stream_manager: &StreamManager,
    table_manager: &TableManager,
    source_configs: &Arc<RwLock<HashMap<String, Value>>>,
    http_client: &reqwest::Client,
    trace_manager: &TraceManager,
    config: &Arc<RwLock<KuiperConfig>>,
    rule_id: String,
    select_stmt: SelectStmt,
    actions: Vec<HashMap<String, Value>>,
    rule_options: Option<HashMap<String, Value>>,
) {
    if rule_options
        .as_ref()
        .and_then(|o| o.get("enableRuleTracer"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        trace_manager.start_trace(&rule_id, "always".to_string());
    }
    let buffer_len = rule_options
        .as_ref()
        .and_then(|o| o.get("bufferLength"))
        .and_then(|v| v.as_u64())
        .and_then(|n| usize::try_from(n).ok())
        .map(|n| n.max(1))
        .unwrap_or(32_768);
    let from_topic = resolve_source_topic(stream_manager, &select_stmt.from);
    let from_stream_def = stream_manager.get_stream(&from_topic);
    let from_fields = from_stream_def.map(|s| s.stream_fields).unwrap_or_default();

    let rx = stream_bus.subscribe_with_capacity(&from_topic, buffer_len);
    let rx = if !from_fields.is_empty() {
        let (tx, coerced_rx) = tokio::sync::mpsc::channel(buffer_len);
        let fields = from_fields;
        tokio::spawn(async move {
            let mut rx = rx;
            while let Some(mut record) = rx.recv().await {
                rekuiper_core::model::enforce_stream_schema(&mut record.data, &fields);
                if tx.send(record).await.is_err() {
                    break;
                }
            }
        });
        coerced_rx
    } else {
        rx
    };

    let window = select_stmt.window.clone();
    let tables = table_manager.clone();
    let confs = source_configs.clone();
    // Stream-stream joins fan in every joined stream: subscribe each join
    // target that is a stream (not a table) so windowed rules see both
    // sides. Table targets resolve per-row through lookups instead.
    let mut join_rxs: Vec<(String, StreamReceiver)> = Vec::new();
    for join in &select_stmt.joins {
        if table_manager.get_table(&join.target).is_none()
            && join.target != select_stmt.from
            && !join_rxs.iter().any(|(s, _)| s == &join.target)
        {
            let topic = resolve_source_topic(stream_manager, &join.target);
            let jrx = stream_bus.subscribe_with_capacity(&topic, buffer_len);
            let join_fields = stream_manager
                .get_stream(&topic)
                .map(|s| s.stream_fields)
                .unwrap_or_default();
            let jrx = if !join_fields.is_empty() {
                let (tx, coerced_jrx) = tokio::sync::mpsc::channel(buffer_len);
                tokio::spawn(async move {
                    let mut rx = jrx;
                    while let Some(mut record) = rx.recv().await {
                        rekuiper_core::model::enforce_stream_schema(&mut record.data, &join_fields);
                        if tx.send(record).await.is_err() {
                            break;
                        }
                    }
                });
                coerced_jrx
            } else {
                jrx
            };
            join_rxs.push((join.target.clone(), jrx));
        }
    }

    // Bounded decoupled sink queue: the streaming evaluation loop never blocks
    // on sink network/disk I/O. Dropping `sink_tx` (rule end/cancel) lets the
    // worker flush remaining outputs and exit cleanly.
    let send_error = rule_options
        .as_ref()
        .and_then(|o| o.get("sendError"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let (sink_tx, mut sink_rx) = tokio::sync::mpsc::channel::<StreamRecord>(buffer_len);
    let prepared = prepare_actions(&actions, &rule_id, source_configs, rule_options.as_ref());
    let cache_configs = action_cache_configs(&actions);
    let sink_rule_id = rule_id.clone();
    let sink_rule_mgr = rule_manager.clone();
    let sink_stream_bus = stream_bus.clone();
    let sink_http_client = http_client.clone();
    let sink_trace_mgr = trace_manager.clone();
    let sink_config = config.clone();
    let sink_counters = rule_manager
        .rule_counters(&rule_id)
        .unwrap_or_else(|| Arc::new(RuleCounters::default()));
    tokio::spawn(async move {
        use std::collections::HashMap as Map;
        use std::sync::atomic::Ordering::Relaxed;
        let mut files: Map<std::path::PathBuf, FileBatchWriter> = Map::new();
        // Per-action runtimes: persistent connections plus the offline cache
        // (pages left by a previous run are adopted and resent first).
        let cache_root = std::path::Path::new(SINK_CACHE_ROOT);
        let mut runtimes: Vec<ActionRuntime> = prepared
            .into_iter()
            .zip(cache_configs)
            .enumerate()
            .map(|(idx, (action, cache_cfg))| {
                let cache = cache_cfg.map(|cfg| {
                    SinkCache::open(cfg, sink_cache::cache_dir(cache_root, &sink_rule_id, idx))
                });
                ActionRuntime::new(action, cache)
            })
            .collect();
        let mut ticker = tokio::time::interval(SINK_TICK);
        // First tick fires immediately; consume it so time-flushes align.
        ticker.tick().await;
        let file_actions = runtimes
            .iter()
            .any(|rt| matches!(&rt.action, PreparedAction::File { .. }));
        let mut pending_file_records: u64 = 0;
        let mut last_live = tokio::time::Instant::now();
        loop {
            tokio::select! {
                rec = sink_rx.recv() => {
                    let Some(output_record) = rec else {
                        // Flush remaining batch_buffer for sinks with batching
                        for rt in runtimes.iter_mut() {
                            if !rt.batch_buffer.is_empty() {
                                let batch = std::mem::take(&mut rt.batch_buffer);
                                let mut batch_data = HashMap::new();
                                batch_data.insert(BATCH_ROWS_KEY.to_string(), Value::Array(batch));
                                let batch_record = StreamRecord::new(batch_data);
                                let enable_private_net = sink_config.read().basic.enable_private_net;
                                let ctx = SinkContext {
                                    rule_id: &sink_rule_id,
                                    rule_mgr: &sink_rule_mgr,
                                    stream_bus: &sink_stream_bus,
                                    http_client: &sink_http_client,
                                    enable_private_net,
                                };
                                let _ = send_action(rt, &batch_record, None, &ctx, &mut files).await;
                            }
                        }
                        // Rule end/cancel/update/delete/shutdown: drain, flush
                        // every file destination, persist caches, then exit.
                        // Errors propagate to exceptions before exit.
                        let mut all_flushed = true;
                        for writer in files.values_mut() {
                            if let Err(e) = writer.flush().await {
                                tracing::warn!(
                                    "[RULE {}] file flush on shutdown failed: {}",
                                    sink_rule_id, e
                                );
                                sink_rule_mgr.inc_exceptions(&sink_rule_id, 1);
                                all_flushed = false;
                            }
                        }
                        if all_flushed {
                            sink_counters.sink_out.fetch_add(pending_file_records, Relaxed);
                        } else {
                            sink_rule_mgr.inc_sink_failed(&sink_rule_id, pending_file_records);
                        }
                        for rt in std::mem::take(&mut runtimes) {
                            if let Some(cache) = rt.cache {
                                if !cache.is_empty() && !cache.config().clean_at_stop {
                                    tracing::info!(
                                        "[RULE {}] persisting {} cached sink records for resend",
                                        sink_rule_id,
                                        cache.len()
                                    );
                                }
                                cache.close();
                            }
                        }
                        break;
                    };
                    last_live = tokio::time::Instant::now();
                    maybe_trace_record(
                        &sink_trace_mgr,
                        &sink_rule_id,
                        &output_record.data,
                        Some(&output_record.data),
                    );
                    let enable_private_net = sink_config.read().basic.enable_private_net;
                    let ctx = SinkContext {
                        rule_id: &sink_rule_id,
                        rule_mgr: &sink_rule_mgr,
                        stream_bus: &sink_stream_bus,
                        http_client: &sink_http_client,
                        enable_private_net,
                    };
                    let num_records = if let Some(Value::Array(a)) = output_record.data.get(BATCH_ROWS_KEY) {
                        a.len() as u64
                    } else {
                        1
                    };
                    match deliver_record(&mut runtimes, &output_record, &ctx, &mut files).await {
                        Delivery::Delivered => {
                            if file_actions {
                                pending_file_records += num_records;
                            } else {
                                sink_counters.sink_out.fetch_add(num_records, Relaxed);
                            }
                        }
                        // Counted as delivered when the resend succeeds.
                        Delivery::Cached => {}
                        Delivery::Failed => sink_rule_mgr.inc_sink_failed(&sink_rule_id, num_records),
                    }
                    let need_flush = files.values().any(|w| w.should_flush());
                    if need_flush {
                        let mut all_flushed = true;
                        for writer in files.values_mut() {
                            if let Err(e) = writer.flush().await {
                                tracing::warn!(
                                    "[RULE {}] file batch flush failed: {}",
                                    sink_rule_id, e
                                );
                                sink_rule_mgr.inc_exceptions(&sink_rule_id, 1);
                                all_flushed = false;
                            }
                        }
                        if all_flushed {
                            sink_counters.sink_out.fetch_add(pending_file_records, Relaxed);
                            pending_file_records = 0;
                        }
                    }
                }
                _ = ticker.tick() => {
                    let mut any_error = false;
                    for writer in files.values_mut() {
                        if writer.pending > 0 {
                            if let Err(e) = writer.flush().await {
                                tracing::warn!(
                                    "[RULE {}] file timed flush failed: {}",
                                    sink_rule_id, e
                                );
                                any_error = true;
                            }
                        }
                    }
                    if any_error {
                        sink_rule_mgr.inc_exceptions(&sink_rule_id, 1);
                    } else {
                        sink_counters.sink_out.fetch_add(pending_file_records, Relaxed);
                        pending_file_records = 0;
                    }
                    let enable_private_net = sink_config.read().basic.enable_private_net;
                    let ctx = SinkContext {
                        rule_id: &sink_rule_id,
                        rule_mgr: &sink_rule_mgr,
                        stream_bus: &sink_stream_bus,
                        http_client: &sink_http_client,
                        enable_private_net,
                    };
                    let now = tokio::time::Instant::now();
                    for rt in runtimes.iter_mut() {
                        let linger_interval = rt.action.common_opts().map(|o| o.linger_interval).unwrap_or(0);
                        if linger_interval > 0 && !rt.batch_buffer.is_empty() {
                            let elapsed = now.duration_since(rt.last_batch_time).as_millis() as u64;
                            if elapsed >= linger_interval {
                                let batch = std::mem::take(&mut rt.batch_buffer);
                                rt.last_batch_time = now;
                                let mut batch_data = HashMap::new();
                                batch_data.insert(BATCH_ROWS_KEY.to_string(), Value::Array(batch));
                                let batch_record = StreamRecord::new(batch_data);
                                if let Err(err) = send_action(rt, &batch_record, None, &ctx, &mut files).await {
                                    report_send_error(rt, &ctx, &err, false);
                                }
                            }
                        }
                    }
                    let live_recent = last_live.elapsed() < SINK_TICK * 2;
                    for rt in runtimes.iter_mut() {
                        let resent = resend_cached(rt, live_recent, &ctx, &mut files).await;
                        if resent > 0 {
                            if file_actions {
                                pending_file_records += resent;
                            } else {
                                sink_counters.sink_out.fetch_add(resent, Relaxed);
                            }
                        }
                    }
                }
            }
        }
    });

    // Event-time mode for windowed rules: boundaries derive from payload
    // timestamps (stream TIMESTAMP field or well-known keys) instead of the
    // wall clock, with a late-tolerance grace window for out-of-order rows.
    let mut source_ts_fields = HashMap::new();
    let from_topic = resolve_source_topic(stream_manager, &select_stmt.from);
    if let Some(s) = stream_manager.get_stream(&from_topic) {
        if let Some((_, v)) = s
            .options
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("TIMESTAMP"))
        {
            source_ts_fields.insert(select_stmt.from.clone(), v.clone());
        }
    }
    for join in &select_stmt.joins {
        let topic = resolve_source_topic(stream_manager, &join.target);
        if let Some(s) = stream_manager.get_stream(&topic) {
            if let Some((_, v)) = s
                .options
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("TIMESTAMP"))
            {
                source_ts_fields.insert(join.target.clone(), v.clone());
            }
        }
    }
    let event_time = EventTimeConfig {
        enabled: rule_options
            .as_ref()
            .and_then(|o| o.get("isEventTime"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        late_tolerance_ms: rule_options
            .as_ref()
            .and_then(|o| o.get("lateTolerance"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0),
        timestamp_field: source_ts_fields.get(&select_stmt.from).cloned(),
        source_timestamp_fields: source_ts_fields,
    };

    // Hot-path handles cloned once: rule loops bump lock-free atomics and
    // read the running flag without touching the global rules map per event.
    let loop_counters = rule_manager
        .rule_counters(&rule_id)
        .unwrap_or_else(|| Arc::new(RuleCounters::default()));
    let loop_running = rule_manager
        .rule_status_handle(&rule_id)
        .unwrap_or_else(|| Arc::new(RwLock::new(RuleStatus::default())));

    let handle = match window {
        None => tokio::spawn(run_stateless_rule(
            loop_counters,
            loop_running,
            rule_id.clone(),
            select_stmt,
            rx,
            tables,
            confs,
            sink_tx,
            send_error,
        )),
        Some(WindowDef::Count { size, interval }) => tokio::spawn(run_count_window_rule(
            loop_counters,
            loop_running,
            rule_id.clone(),
            select_stmt,
            rx,
            size,
            interval,
            sink_tx,
            send_error,
            join_rxs,
            tables.clone(),
            confs.clone(),
        )),
        Some(WindowDef::TumblingTime { unit, length }) => {
            if length == 0 {
                tokio::spawn(async move {
                    let mut rx = rx;
                    while rx.recv().await.is_some() {
                        if !is_rule_running(&loop_running) {
                            continue;
                        }
                        loop_counters.inc_source(1);
                    }
                })
            } else {
                let duration = tumbling_window_duration(&unit, length);
                tokio::spawn(run_tumbling_window_rule(
                    loop_counters,
                    loop_running,
                    rule_id.clone(),
                    select_stmt,
                    rx,
                    duration,
                    sink_tx,
                    event_time,
                    send_error,
                    join_rxs,
                    tables.clone(),
                    confs.clone(),
                ))
            }
        }
        Some(WindowDef::HoppingTime {
            unit,
            length,
            interval,
        }) => {
            let window_length = tumbling_window_duration(&unit, length);
            let hop_interval = tumbling_window_duration(&unit, interval);
            tokio::spawn(run_hopping_window_rule(
                loop_counters,
                loop_running,
                rule_id.clone(),
                select_stmt,
                rx,
                window_length,
                hop_interval,
                sink_tx,
                event_time,
                send_error,
                join_rxs,
                tables.clone(),
                confs.clone(),
            ))
        }
        Some(WindowDef::SlidingTime {
            unit,
            length,
            delay,
        }) => {
            let window_length = tumbling_window_duration(&unit, length);
            let delay_dur = delay.map(|d| tumbling_window_duration(&unit, d));
            tokio::spawn(run_sliding_window_rule(
                loop_counters,
                loop_running,
                rule_id.clone(),
                select_stmt,
                rx,
                window_length,
                delay_dur,
                sink_tx,
                event_time,
                send_error,
                join_rxs,
                tables.clone(),
                confs.clone(),
            ))
        }
        Some(WindowDef::Session {
            unit,
            max_duration,
            timeout,
        }) => tokio::spawn(run_session_window_rule(
            loop_counters,
            loop_running,
            rule_id.clone(),
            select_stmt,
            rx,
            tumbling_window_duration(&unit, max_duration),
            tumbling_window_duration(&unit, timeout),
            sink_tx,
            event_time,
            send_error,
            join_rxs,
            tables.clone(),
            confs.clone(),
        )),
        Some(WindowDef::State {
            start_condition,
            end_condition,
        }) => tokio::spawn(run_state_window_rule(
            loop_counters,
            loop_running,
            rule_id.clone(),
            select_stmt,
            rx,
            start_condition,
            end_condition,
            sink_tx,
            event_time,
            send_error,
            join_rxs,
            tables.clone(),
            confs.clone(),
        )),
    };
    rule_manager.set_rule_handle(&rule_id, handle);
}

/// Resolve the polling config for a `TYPE="sql"` source stream: a matching
/// `sql/{conf_key}` source config wins, otherwise the stream options (`URL`,
/// falling back to `DATASOURCE`, plus `TABLE` or the stream name and an
/// optional `INTERVAL` poll period). Option names match case-insensitively.
pub(crate) fn resolve_sql_source(
    stream_manager: &StreamManager,
    source_configs: &Arc<RwLock<HashMap<String, Value>>>,
    stream_name: &str,
    rule_id: &str,
) -> Option<SqlConnectorConfig> {
    let def = stream_manager.get_stream(stream_name)?;
    let kind = def
        .options
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("TYPE"))
        .map(|(_, v)| v.clone())
        .unwrap_or_default();
    if !kind.eq_ignore_ascii_case("sql") {
        return None;
    }
    let conf_key = def
        .options
        .iter()
        .find(|(k, _)| {
            k.eq_ignore_ascii_case("CONF_KEY")
                || k.eq_ignore_ascii_case("confKey")
                || k.eq_ignore_ascii_case("connectionSelector")
        })
        .map(|(_, v)| v.trim().to_string());
    if let Some(key) = conf_key {
        if !key.is_empty() {
            let lookup = format!("sql/{}", key);
            let configs_guard = source_configs.read();
            let conf_val = configs_guard
                .get(&lookup)
                .or_else(|| configs_guard.get(&key))
                .or_else(|| configs_guard.get(&format!("connections/{}", key)))
                .cloned();
            drop(configs_guard);
            if let Some(val) = conf_val {
                match serde_json::from_value::<SqlConnectorConfig>(val) {
                    Ok(mut conf) => {
                        // Documented plugin configs carry the URL as `dburl`
                        // and may omit the table: fall back to the stream
                        // options (DATASOURCE/TABLE) for whatever is missing.
                        if conf.url.trim().is_empty() {
                            conf.url = def
                                .options
                                .iter()
                                .find(|(k, _)| {
                                    k.eq_ignore_ascii_case("URL")
                                        || k.eq_ignore_ascii_case("DATASOURCE")
                                })
                                .map(|(_, v)| v.clone())
                                .unwrap_or_default();
                        }
                        if conf.table.trim().is_empty() {
                            conf.table = sql_table_fallback(&def.options, stream_name);
                        }
                        if conf.url.trim().is_empty() || conf.table.trim().is_empty() {
                            // A template-SQL config without any table anchor
                            // cannot poll; surface the misconfiguration.
                            tracing::warn!(
                                "[RULE {}] sql source config '{}' has no usable url/table",
                                rule_id,
                                lookup
                            );
                            return None;
                        }
                        return Some(conf);
                    }
                    Err(e) => {
                        tracing::warn!(
                            "[RULE {}] invalid sql source config '{}': {}",
                            rule_id,
                            lookup,
                            e
                        );
                        return None;
                    }
                }
            }
        }
    }
    let url = def
        .options
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("URL") || k.eq_ignore_ascii_case("DATASOURCE"))
        .map(|(_, v)| v.clone())
        .unwrap_or_default();
    if url.trim().is_empty() {
        return None;
    }
    let table = def
        .options
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("TABLE"))
        .map(|(_, v)| v.clone())
        .unwrap_or_else(|| stream_name.to_string());
    let interval = def
        .options
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("INTERVAL"))
        .and_then(|(_, v)| v.trim().parse::<u64>().ok())
        .unwrap_or(1000)
        .max(1);
    Some(SqlConnectorConfig {
        url,
        table,
        fields: Vec::new(),
        interval,
        template_sql_query_cfg: None,
        internal_sql_query_cfg: None,
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{
        der_read_tlv, load_config_maps, parse_pem_block, persist_config_entry, spki_to_pkcs1,
        unpersist_config_entry, verify_jwt_raw,
    };
    use rekuiper_core::{KvStore, StreamDefinition};
    use serde_json::json;

    async fn test_stream_manager(options: &[(&str, &str)]) -> StreamManager {
        let manager = StreamManager::new();
        let opts = options
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<HashMap<_, _>>();
        manager
            .create_stream(StreamDefinition {
                name: "demo".to_string(),
                sql: String::new(),
                stream_fields: Vec::new(),
                options: opts,
            })
            .await
            .unwrap();
        manager
    }

    fn test_source_configs(pairs: &[(&str, Value)]) -> Arc<RwLock<HashMap<String, Value>>> {
        Arc::new(RwLock::new(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        ))
    }

    #[tokio::test]
    async fn resolve_mqtt_source_honors_confkey_without_topic() {
        // D1: a topic-less stored config must still contribute its broker URL.
        let manager = test_stream_manager(&[("CONF_KEY", "remotekey")]).await;
        let configs =
            test_source_configs(&[("mqtt/remotekey", json!({"server": "tcp://broker:1883"}))]);
        let cfg =
            resolve_mqtt_source(&manager, &configs, &SchemaManager::new(), "demo", "r1").unwrap();
        assert_eq!(cfg.server, "tcp://broker:1883");
        assert_eq!(cfg.topic, "demo");
    }

    #[tokio::test]
    async fn resolve_mqtt_source_is_case_insensitive() {
        // Lowercase SQL/JSON option spellings resolve like the uppercase ones.
        let manager =
            test_stream_manager(&[("conf_key", "remotekey"), ("datasource", "sensors/#")]).await;
        let configs =
            test_source_configs(&[("mqtt/remotekey", json!({"server": "tcp://broker:1883"}))]);
        let cfg =
            resolve_mqtt_source(&manager, &configs, &SchemaManager::new(), "demo", "r1").unwrap();
        assert_eq!(cfg.server, "tcp://broker:1883");
        assert_eq!(cfg.topic, "sensors/#");
    }

    #[tokio::test]
    async fn resolve_mqtt_source_server_option_overrides_confkey() {
        let manager =
            test_stream_manager(&[("CONF_KEY", "remotekey"), ("server", "tcp://override:1883")])
                .await;
        let configs =
            test_source_configs(&[("mqtt/remotekey", json!({"server": "tcp://broker:1883"}))]);
        let cfg =
            resolve_mqtt_source(&manager, &configs, &SchemaManager::new(), "demo", "r1").unwrap();
        assert_eq!(cfg.server, "tcp://override:1883");
    }

    #[tokio::test]
    async fn resolve_mqtt_source_falls_back_to_loopback() {
        let manager = test_stream_manager(&[]).await;
        let configs = test_source_configs(&[]);
        let cfg =
            resolve_mqtt_source(&manager, &configs, &SchemaManager::new(), "demo", "r1").unwrap();
        assert_eq!(cfg.server, "tcp://127.0.0.1:1883");
        assert_eq!(cfg.topic, "demo");
    }

    #[tokio::test]
    async fn resolve_sql_source_from_confkey() {
        // D8: a stored `sql/{key}` config provides url/table/interval.
        let manager = test_stream_manager(&[("TYPE", "sql"), ("CONF_KEY", "pg")]).await;
        let configs = test_source_configs(&[(
            "sql/pg",
            json!({"url": "postgres://db:5432/k", "table": "readings", "interval": 500}),
        )]);
        let cfg = resolve_sql_source(&manager, &configs, "demo", "r1").unwrap();
        assert_eq!(cfg.url, "postgres://db:5432/k");
        assert_eq!(cfg.table, "readings");
        assert_eq!(cfg.interval, 500);
    }

    #[tokio::test]
    async fn resolve_sql_source_from_stream_options() {
        // Lowercase option spellings resolve like the uppercase ones.
        // Documented plugin shape: dburl + templateSqlQueryCfg, table
        // anchored by the stream DATASOURCE.
        let manager = test_stream_manager(&[
            ("TYPE", "sql"),
            ("DATASOURCE", "rksrc"),
            ("CONF_KEY", "postgresql_config"),
        ])
        .await;
        let configs = test_source_configs(&[(
            "sql/postgresql_config",
            json!({
                "dburl": "postgres://u:p@h/db?sslmode=disable",
                "interval": 5000,
                "templateSqlQueryCfg": {"templateSql": "SELECT id, val FROM rksrc"},
            }),
        )]);
        let cfg = resolve_sql_source(&manager, &configs, "demo", "r1").unwrap();
        assert_eq!(cfg.url, "postgres://u:p@h/db?sslmode=disable");
        assert_eq!(cfg.table, "rksrc");
        assert_eq!(cfg.interval, 5000);
        assert_eq!(
            rekuiper_connectors::sql_source_query(&cfg),
            "SELECT id, val FROM rksrc"
        );
        let manager = test_stream_manager(&[
            ("type", "SQL"),
            ("datasource", "sqlite://x.db"),
            ("table", "sens"),
        ])
        .await;
        let cfg = resolve_sql_source(&manager, &test_source_configs(&[]), "demo", "r1").unwrap();
        assert_eq!(cfg.url, "sqlite://x.db");
        assert_eq!(cfg.table, "sens");
        assert_eq!(cfg.interval, 1000);
    }

    #[tokio::test]
    async fn resolve_sql_source_rejects_non_sql_types() {
        let manager = test_stream_manager(&[("TYPE", "mqtt")]).await;
        assert!(resolve_sql_source(&manager, &test_source_configs(&[]), "demo", "r1").is_none());
        let manager = test_stream_manager(&[]).await;
        assert!(resolve_sql_source(&manager, &test_source_configs(&[]), "demo", "r1").is_none());
    }

    fn tagged(source: &str, pairs: &[(&str, Value)]) -> TaggedRow {
        TaggedRow {
            source: source.to_string(),
            data: pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        }
    }

    fn parse_stmt(sql: &str) -> SelectStmt {
        Parser::new(sql).parse_select().expect("join SQL parses")
    }

    #[tokio::test]
    async fn window_join_right_preserves_unmatched_without_left() {
        // RIGHT JOIN with an empty left side still emits right rows.
        let stmt = parse_stmt(
            "SELECT l.id AS id, r.val AS v FROM l RIGHT JOIN r ON l.id = r.id GROUP BY CountWindow(2)",
        );
        let tables = TableManager::new();
        let confs = test_source_configs(&[]);
        let batch = vec![tagged("r", &[("id", json!(1)), ("val", json!(9))])];
        let out = eval_window_join_batch(&tables, &confs, &stmt, &batch, None).await;
        assert_eq!(out.len(), 1);
        // Missing left side projects to Null; the preserved right side is intact.
        assert_eq!(out[0].get("id"), Some(&serde_json::Value::Null));
        assert_eq!(out[0].get("v"), Some(&json!(9)));
    }

    #[tokio::test]
    async fn window_join_inner_empty_left_yields_nothing() {
        let stmt = parse_stmt(
            "SELECT l.id AS id FROM l INNER JOIN r ON l.id = r.id GROUP BY CountWindow(2)",
        );
        let tables = TableManager::new();
        let confs = test_source_configs(&[]);
        let batch = vec![tagged("r", &[("id", json!(1))])];
        let out = eval_window_join_batch(&tables, &confs, &stmt, &batch, None).await;
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn window_join_cross_fanout_is_bounded() {
        // 201 x 201 pairs would fan out to 40401 rows; the cap holds.
        let stmt = parse_stmt("SELECT l.id AS id FROM l CROSS JOIN r GROUP BY CountWindow(50000)");
        let tables = TableManager::new();
        let confs = test_source_configs(&[]);
        let mut batch = Vec::new();
        for i in 0..201 {
            batch.push(tagged("l", &[("id", json!(i))]));
            batch.push(tagged("r", &[("id", json!(i))]));
        }
        let out = eval_window_join_batch(&tables, &confs, &stmt, &batch, None).await;
        assert_eq!(out.len(), MAX_JOIN_FANOUT);
    }

    #[tokio::test]
    async fn window_join_table_takes_first_on_match() {
        // Lookup tables resolve one row per key: the first ON-matching row
        // wins (point-lookup semantics, also used by the stateless path).
        let tables = TableManager::new();
        tables
            .create_table(TableDefinition {
                name: "t".to_string(),
                sql: String::new(),
                stream_fields: Vec::new(),
                options: HashMap::new(),
            })
            .await
            .unwrap();
        tables.insert_table_row(
            "t",
            [("id".to_string(), json!(1)), ("v".to_string(), json!("a"))]
                .into_iter()
                .collect(),
        );
        tables.insert_table_row(
            "t",
            [("id".to_string(), json!(1)), ("v".to_string(), json!("b"))]
                .into_iter()
                .collect(),
        );
        let stmt = parse_stmt(
            "SELECT s.id AS id, t.v AS v FROM s INNER JOIN t ON s.id = t.id GROUP BY CountWindow(2)",
        );
        let confs = test_source_configs(&[]);
        let batch = vec![tagged("s", &[("id", json!(1))])];
        let out = eval_window_join_batch(&tables, &confs, &stmt, &batch, None).await;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].get("v"), Some(&json!("a")));
    }

    #[tokio::test]
    async fn bootstrap_join_registers_all_source_cancels() {
        // Stream-stream joins bootstrap one producer per side. Every
        // producer's cancel handle must be retained: overwriting the entry
        // drops the first sender, and a dropped watch sender reads as
        // `Err` in the source task, which exits immediately (join side A
        // would silently stop delivering).
        let bus = StreamBus::new();
        let sm = StreamManager::new();
        for name in ["ja", "jb"] {
            sm.create_stream(StreamDefinition {
                name: name.to_string(),
                sql: String::new(),
                stream_fields: Vec::new(),
                options: HashMap::new(),
            })
            .await
            .unwrap();
        }
        let state = AppState::new(
            "test".to_string(),
            rekuiper_conf::KuiperConfig::default(),
            sm,
            TableManager::new(),
            RuleManager::new(bus.clone()),
            bus,
        );
        let mut parser = Parser::new(
            "SELECT ja.id AS id, jb.val AS v FROM ja INNER JOIN jb ON ja.id = jb.id GROUP BY CountWindow(2)",
        );
        let stmt = parser.parse_select().unwrap();
        bootstrap_rule_sources(&state, "rj", &stmt);
        let guards = state.source_cancels.read();
        let txs = guards.get("rj").expect("join rule has cancel handles");
        assert_eq!(txs.len(), 2, "one cancel sender per joined stream");
        drop(guards);
        cancel_rule_source(&state, "rj");
        assert!(!state.source_cancels.read().contains_key("rj"));
    }

    #[tokio::test]
    async fn config_maps_persist_and_reload_across_restart() {
        // D-RESTART: CONF_KEYs (MQTT brokers, SQL URLs) must survive a
        // daemon restart instead of falling back to loopback defaults.
        use rekuiper_core::MemKvStore;
        let kv: Arc<dyn KvStore> = Arc::new(MemKvStore::new());
        let state = AppState {
            kv: Some(kv.clone()),
            ..AppState::new(
                "test".to_string(),
                rekuiper_conf::KuiperConfig::default(),
                StreamManager::new(),
                TableManager::new(),
                RuleManager::new(StreamBus::new()),
                StreamBus::new(),
            )
        };
        persist_config_entry(
            &state,
            "source_configs",
            "mqtt/evalmqtt",
            &json!({"server": "tcp://broker:1883"}),
        )
        .await
        .unwrap();
        persist_config_entry(
            &state,
            "source_configs",
            "sql/postgresql_config",
            &json!({"dburl": "postgres://u:p@h/db", "interval": 5000}),
        )
        .await
        .unwrap();
        // A fresh daemon with empty maps reloads everything from KV.
        let fresh = AppState {
            kv: Some(kv),
            ..AppState::new(
                "test".to_string(),
                rekuiper_conf::KuiperConfig::default(),
                StreamManager::new(),
                TableManager::new(),
                RuleManager::new(StreamBus::new()),
                StreamBus::new(),
            )
        };
        load_config_maps(&fresh).await.unwrap();
        assert_eq!(
            fresh
                .source_configs
                .read()
                .get("mqtt/evalmqtt")
                .and_then(|v| v.get("server"))
                .and_then(|v| v.as_str()),
            Some("tcp://broker:1883")
        );
        assert!(fresh
            .source_configs
            .read()
            .contains_key("sql/postgresql_config"));
        // Deletes propagate too.
        unpersist_config_entry(&state, "source_configs", "mqtt/evalmqtt")
            .await
            .unwrap();
        let fresh2 = AppState {
            kv: Some(state.kv.clone().unwrap()),
            ..AppState::new(
                "test".to_string(),
                rekuiper_conf::KuiperConfig::default(),
                StreamManager::new(),
                TableManager::new(),
                RuleManager::new(StreamBus::new()),
                StreamBus::new(),
            )
        };
        load_config_maps(&fresh2).await.unwrap();
        assert!(!fresh2.source_configs.read().contains_key("mqtt/evalmqtt"));
        assert!(fresh2
            .source_configs
            .read()
            .contains_key("sql/postgresql_config"));
    }

    // D9: RS256 JWT vectors generated offline (2048-bit key, 1-year valid
    // token, 1-hour-expired token, keyless-claims token, wrong-key token).
    const TEST_RSA_PUBLIC_PEM: &str = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAmdub6pqDN/MPofsOTQlf\npCF6O4vsisxaDPzKZM8pqiUIOGjDwfqRQkzikSPu/oK8jPouof9JkeesUjbKg+0w\nQ7aZXgRPr8PJkHeY27/4bFz1riFPDZ+rKAe8DvXIlcjb70H68AtGnRzUkVjVlzhn\n6qfJE4LMmLtdQodW4Hnd2Oo8qujRprtn8AMcX5H1phIVUHYdIZpt44SNetOgCPxZ\n/S/0VLi2qD7bh/bBj6VRvea/LyCKnC75r+wnJGIHYpeVXskMrDBH+lfV1GsoU9Ig\nm+5MYAkc0SrgYBVCUXXvQNrip3IQcaWlW4YhkJC2rVBCA2ibc1MMWOyA0xYfJfy1\nvQIDAQAB\n-----END PUBLIC KEY-----\n";
    const TEST_JWT_VALID: &str = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0ZXIiLCJleHAiOjE4MjA2MDE3MDB9.U3NGxZxNiHStyGFGZOKsz4PPM3aX2papMEOsKUqWnYC7NmgGxESTKWKtPM6M5McKUQwD3cfnnFH9p3XGdkTfPofxcnmJcrN8PMWdaVAcetYn_c5ScMWgQapjuHiO7jBQKljTU4AwuGrNQAMtxgBgSdEX-MG1AYZrNYcdgqXtfUPk8y_V2icNU8kVQRzRonHs4yaioIqy1IpBuxpb6A5AHRy07T_En_TlebfH7Tb-lf3tFDT8UUcjnfFJ-TPxFTGqLsPnLbdqXcrKi-PVqpbuFj2WSFlGinbZDGEsjhopdHPD1_PkC0Am3c4XmiCM3QswVqcUujwED71lcZXBdCFxPA";
    const TEST_JWT_EXPIRED: &str = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0ZXIiLCJleHAiOjE3ODkwNjIxMDB9.GNrAExENuYVgbt8bZebTYTmCf2o0DcAkWzhpfkBCtAvPflzzr9xSNaEOrtf2HJU5gqOm-ZDIztXsatd765CtZA-uLcqjbCa4TURbMv0OFHDhMVlVfn2xr17jr39IG1ck7Ymioz_ZSd3wXu0egABsCUrKigEvtRwcGJSWlAp1GdRiWLpT2-U6Xb15bgUdbCNgYxyXiWM4zBLHVILLLH-zPyPIbvHo82l3qOpu7c2SRw5aNPrY2KXPCxKm47NEOkf74LJk14MZODeRTOYczyi8Q4vtD6xEcp8-u_vVUv5Gv04mrO1gMCALOQAWfKq2PFA0QatGX_cw8f36m_EOLLfuWA";
    const TEST_JWT_NOEXP: &str = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0ZXIifQ.dqsuFs9MyRMcgfPGvUAdCOJXKYFSMphpVASqmMH_n4aaAa3F7QeUz8ggX0931Z_TbgxvZ4Dp-Lf05eVSWOBWvpnjwSvn1JLw7axaEFl7BOEzsFzq_1gPMKMY1TXylXgUV2DHUhlayS53UcEvS5MP0vKQG07PsTOjFZACeuohilHX5vEmn9zwy67CbwL7Z3g32Msdb67pplMViAXGgau3UTYi5DOZFsSFlDrOj5w2Gg1EeZBpC1udtUvkIjHiW92LkoLI7HBzmRQwEuxGgOE134T2YGF5FuPjq-XpfiklV6SmrQpuHM_qGPtLHUm-6blwOgffAnr0uiCYVHZT5pe2hw";
    const TEST_JWT_WRONGKEY: &str = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0ZXIiLCJleHAiOjE4MjA2MDE3MDB9.Uu3vn8teCnUBsT-8updR4it_yyMoYdtqFGOvLgozxmsOmM8hb-FfqpeuqelBemgsbpwi2pbRVuOUxv0621ORempEpwfhCUnI80Rj9hPcOV0j8wYuX-xdsnvsNOPI1K6XlW9i1IOjToKxhptaPEPdYXf_jEq5v3P0zCITwevT2t6bpOdYzgSHjxq6CJiXIJHcP0idiQ1I3PqNhKmjJn3qi14GQ_xRk8Sf30VQDqsT76qXw0S92K-QKdcbc35oJW5x6oTdbgELv9YV-su1ooXTrLQ5zzY0g8qXhbkeLotXQEddaGMFeBOWchDax_km1g1gRbCS8f7TNEtO2xVkVHx-AQ";

    fn test_key_der() -> Vec<u8> {
        let (label, der) =
            parse_pem_block(TEST_RSA_PUBLIC_PEM.as_bytes()).expect("test PEM parses");
        assert_eq!(label, "PUBLIC KEY");
        spki_to_pkcs1(&der).expect("test SPKI unwraps to PKCS#1")
    }

    #[test]
    fn spki_unwrap_keeps_modulus_and_exponent() {
        // The unwrapped PKCS#1 body is a bare SEQUENCE of two INTEGERs.
        let pkcs1 = test_key_der();
        let (tag, content, rest) = der_read_tlv(&pkcs1).expect("PKCS#1 parses as DER");
        assert_eq!(tag, 0x30);
        assert!(rest.is_empty());
        let (n_tag, n_content, e_rest) = der_read_tlv(content).expect("modulus parses");
        assert_eq!(n_tag, 0x02);
        assert_eq!(n_content.len(), 257); // 2048-bit modulus + leading zero
        let (e_tag, e_content, e_end) = der_read_tlv(e_rest).expect("exponent parses");
        assert_eq!(e_tag, 0x02);
        assert_eq!(e_content, &[0x01, 0x00, 0x01]); // 65537
        assert!(e_end.is_empty());
    }

    #[test]
    fn jwt_verify_accepts_valid_token() {
        let key = test_key_der();
        assert_eq!(verify_jwt_raw(TEST_JWT_VALID, &key), Ok(()));
        // Tokens without `exp` carry no expiry to enforce.
        assert_eq!(verify_jwt_raw(TEST_JWT_NOEXP, &key), Ok(()));
    }

    #[test]
    fn jwt_verify_rejects_expired_token() {
        assert_eq!(
            verify_jwt_raw(TEST_JWT_EXPIRED, &test_key_der()),
            Err("Token has expired\n")
        );
    }

    #[test]
    fn jwt_verify_rejects_malformed_tokens() {
        let key = test_key_der();
        for bad in ["", "abc", "a.b", "a.b.c.d", "a..c", "!!!.@@@.###"] {
            assert_eq!(
                verify_jwt_raw(bad, &key),
                Err("Invalid JWT format\n"),
                "token: {}",
                bad
            );
        }
    }

    #[test]
    fn jwt_verify_rejects_wrong_key_and_tampering() {
        let key = test_key_der();
        assert_eq!(
            verify_jwt_raw(TEST_JWT_WRONGKEY, &key),
            Err("Invalid token signature\n")
        );
        // Swap two adjacent differing signature characters: the decoded
        // bytes must change, so verification has to fail.
        let sig_start = TEST_JWT_VALID.rfind('.').unwrap() + 1;
        let mut tampered = TEST_JWT_VALID.to_string();
        let bytes = tampered.as_bytes();
        let mut idx = None;
        for i in sig_start..bytes.len() - 1 {
            if bytes[i] != bytes[i + 1] {
                idx = Some(i);
                break;
            }
        }
        let i = idx.expect("signature has swappable characters");
        tampered.replace_range(
            i..i + 2,
            &format!("{}{}", bytes[i + 1] as char, bytes[i] as char),
        );
        assert_ne!(tampered, TEST_JWT_VALID);
        assert_eq!(
            verify_jwt_raw(&tampered, &key),
            Err("Invalid token signature\n")
        );
    }

    #[tokio::test]
    async fn resolve_edgex_source_defaults_and_options() {
        let manager = test_stream_manager(&[("TYPE", "edgex")]).await;
        let confs = test_source_configs(&[]);
        let schemas = SchemaManager::new();
        let cfg = resolve_edgex_source(&manager, &confs, &schemas, "demo", "rule1")
            .expect("resolves edgex stream");
        assert_eq!(cfg.server, "tcp://edgex-mqtt-broker:1883");
        assert_eq!(cfg.topic, "edgex/rules-events");
        assert_eq!(cfg.format, PayloadFormat::EdgeX);

        // Custom SERVER and DATASOURCE override defaults
        let manager_custom = test_stream_manager(&[
            ("TYPE", "edgex"),
            ("SERVER", "192.168.1.50:1883"),
            ("DATASOURCE", "custom/bus"),
        ])
        .await;
        let cfg_custom = resolve_edgex_source(&manager_custom, &confs, &schemas, "demo", "rule2")
            .expect("resolves custom edgex stream");
        assert_eq!(cfg_custom.server, "tcp://192.168.1.50:1883");
        assert_eq!(cfg_custom.topic, "custom/bus");
    }

    #[test]
    fn prepare_actions_parses_edgex_action() {
        let confs = test_source_configs(&[]);
        let mut action_map = HashMap::new();
        action_map.insert(
            "edgex".to_string(),
            serde_json::json!({
                "topic": "edgex/alerts",
                "deviceName": "testDevice",
                "profileName": "testProfile"
            }),
        );
        let actions = vec![action_map];
        let prepared = prepare_actions(&actions, "rule100", &confs, None);
        assert_eq!(prepared.len(), 1);
        match &prepared[0] {
            PreparedAction::EdgeX {
                device_name,
                profile_name,
                source_name,
                config,
                template,
            } => {
                assert_eq!(device_name, "testDevice");
                assert_eq!(profile_name, "testProfile");
                assert_eq!(source_name, "rule100");
                assert_eq!(config.server, "tcp://edgex-mqtt-broker:1883");
                assert_eq!(config.topic, "edgex/alerts");
                assert_eq!(config.format, PayloadFormat::EdgeX);
                assert!(template.is_none());
            }
            _ => panic!("expected PreparedAction::EdgeX"),
        }
    }

    #[test]
    fn test_prepare_actions_respects_disabled_flag() {
        let confs = test_source_configs(&[]);
        let mut action_enabled = HashMap::new();
        action_enabled.insert("log".to_string(), serde_json::json!({}));

        let mut action_disabled_inner = HashMap::new();
        action_disabled_inner.insert("log".to_string(), serde_json::json!({"disabled": true}));

        let mut action_disabled_outer = HashMap::new();
        action_disabled_outer.insert("log".to_string(), serde_json::json!({}));
        action_disabled_outer.insert("disabled".to_string(), serde_json::json!(true));

        let actions = vec![action_enabled, action_disabled_inner, action_disabled_outer];
        let prepared = prepare_actions(&actions, "rule_disabled_test", &confs, None);
        assert_eq!(prepared.len(), 1);
        let cache_confs = action_cache_configs(&actions);
        assert_eq!(cache_confs.len(), 1);
    }

    #[test]
    fn test_validate_sink_actions_skips_disabled() {
        let mut invalid_disabled = HashMap::new();
        invalid_disabled.insert("mqtt".to_string(), serde_json::json!({"disabled": true}));
        assert!(crate::handlers::rules::validate_sink_actions(&[invalid_disabled]).is_ok());

        let mut invalid_enabled = HashMap::new();
        invalid_enabled.insert("mqtt".to_string(), serde_json::json!({"server": "tcp://127.0.0.1:1883"}));
        assert!(crate::handlers::rules::validate_sink_actions(&[invalid_enabled]).is_err());
    }

    #[tokio::test]
    async fn test_reject_invalid_rule_conflicting_buffer_full_policy() {
        use crate::handlers::rules::reject_invalid_rule;
        use crate::state::AppState;
        use rekuiper_core::model::StreamDefinition;

        let sm = StreamManager::new();
        let bus = StreamBus::new();
        let mut opts1 = HashMap::new();
        opts1.insert("BUFFER_FULL_POLICY".to_string(), "block".to_string());
        sm.create_stream(StreamDefinition {
            name: "stream_block".to_string(),
            sql: "CREATE STREAM stream_block () WITH (BUFFER_FULL_POLICY=\"block\")".to_string(),
            stream_fields: Vec::new(),
            options: opts1.clone(),
        })
        .await
        .unwrap();

        let mut opts2 = HashMap::new();
        opts2.insert("BUFFER_FULL_POLICY".to_string(), "dropOldest".to_string());
        sm.create_stream(StreamDefinition {
            name: "stream_drop".to_string(),
            sql: "CREATE STREAM stream_drop () WITH (BUFFER_FULL_POLICY=\"dropOldest\")".to_string(),
            stream_fields: Vec::new(),
            options: opts2,
        })
        .await
        .unwrap();

        sm.create_stream(StreamDefinition {
            name: "stream_block2".to_string(),
            sql: "CREATE STREAM stream_block2 () WITH (BUFFER_FULL_POLICY=\"block\")".to_string(),
            stream_fields: Vec::new(),
            options: opts1,
        })
        .await
        .unwrap();

        let state = AppState::new(
            "test".to_string(),
            rekuiper_conf::KuiperConfig::default(),
            sm,
            TableManager::new(),
            RuleManager::new(bus.clone()),
            bus,
        );

        // Conflicting: block + dropOldest
        let mut parser_conflict = Parser::new(
            "SELECT * FROM stream_block INNER JOIN stream_drop ON stream_block.id = stream_drop.id GROUP BY TumblingWindow(ss, 10)",
        );
        let stmt_conflict = parser_conflict.parse_select().unwrap();
        assert!(reject_invalid_rule(&state, &stmt_conflict, None, false).is_some());

        // Matching: block + block
        let mut parser_match = Parser::new(
            "SELECT * FROM stream_block INNER JOIN stream_block2 ON stream_block.id = stream_block2.id GROUP BY TumblingWindow(ss, 10)",
        );
        let stmt_match = parser_match.parse_select().unwrap();
        assert!(reject_invalid_rule(&state, &stmt_match, None, false).is_none());
    }
}
