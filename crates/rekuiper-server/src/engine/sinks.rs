use crate::sink_cache::{self, CacheConfig, ResendPriority, SinkCache};
use crate::state::is_private_or_internal_ip;
use parking_lot::RwLock;
use rekuiper_connectors::{
    apply_data_template, apply_data_template_value, EdgeXCodec, FileSink, KafkaConfig, KafkaSink,
    MqttConfig, MqttSink, PayloadFormat, RabbitMqConfig, RabbitMqSink, RedisSink, RedisSinkConfig,
    Sink, SqlConnectorConfig, SqlSink, WebSocketConfig, WebSocketSink,
};
use rekuiper_core::model::StreamRecord;
use rekuiper_core::{RuleCounters, RuleManager, StreamBus};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
pub(crate) struct CommonSinkOpts {
    pub(crate) send_single: bool,
    pub(crate) send_nil_field: bool,
    pub(crate) fields: Option<Vec<String>>,
    pub(crate) exclude_fields: Option<Vec<String>>,
    pub(crate) data_field: Option<String>,
    pub(crate) format: Option<String>,
    pub(crate) batch_size: usize,
    pub(crate) linger_interval: u64,
}

pub fn is_action_disabled(action: &HashMap<String, Value>, opts: &Value) -> bool {
    let parse_bool = |v: &Value| {
        if let Some(b) = v.as_bool() {
            Some(b)
        } else if let Some(s) = v.as_str() {
            s.parse::<bool>().ok()
        } else {
            None
        }
    };
    action.get("disabled").and_then(parse_bool).unwrap_or(false)
        || opts.get("disabled").and_then(parse_bool).unwrap_or(false)
}

pub(crate) fn parse_common_opts(
    opts: &Value,
    rule_send_nil_field: bool,
    default_send_single: bool,
) -> CommonSinkOpts {
    let send_single = opts
        .get("sendSingle")
        .and_then(|v| {
            if let Some(b) = v.as_bool() {
                Some(b)
            } else if let Some(s) = v.as_str() {
                s.parse::<bool>().ok()
            } else {
                None
            }
        })
        .unwrap_or(default_send_single);
    let send_nil_field = opts
        .get("sendNilField")
        .and_then(|v| {
            if let Some(b) = v.as_bool() {
                Some(b)
            } else if let Some(s) = v.as_str() {
                s.parse::<bool>().ok()
            } else {
                None
            }
        })
        .unwrap_or(rule_send_nil_field);
    let fields = opts.get("fields").and_then(|v| v.as_array()).map(|arr| {
        arr.iter()
            .filter_map(|x| x.as_str().map(|s| s.to_string()))
            .collect()
    });
    let exclude_fields = opts
        .get("excludeFields")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect()
        });
    let data_field = opts
        .get("dataField")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());
    let format = opts
        .get("format")
        .and_then(|v| v.as_str())
        .map(|s| s.to_ascii_lowercase());
    let batch_size = opts
        .get("batchSize")
        .and_then(|v| {
            if let Some(n) = v.as_u64() {
                Some(n as usize)
            } else if let Some(s) = v.as_str() {
                s.parse::<usize>().ok()
            } else {
                None
            }
        })
        .unwrap_or(0);
    let linger_interval = opts
        .get("lingerInterval")
        .and_then(|v| {
            if let Some(n) = v.as_u64() {
                Some(n)
            } else if let Some(s) = v.as_str() {
                s.parse::<u64>().ok()
            } else {
                None
            }
        })
        .unwrap_or(0);
    CommonSinkOpts {
        send_single,
        send_nil_field,
        fields,
        exclude_fields,
        data_field,
        format,
        batch_size,
        linger_interval,
    }
}

pub(crate) fn clean_sink_value(v: &Value, send_nil_field: bool) -> Value {
    match v {
        Value::Array(arr) => Value::Array(
            arr.iter()
                .map(|item| clean_sink_value(item, send_nil_field))
                .collect(),
        ),
        Value::Object(obj) => {
            let mut map = std::collections::BTreeMap::new();
            for (k, val) in obj {
                if k == rekuiper_sql::eval::META_KEY || k.starts_with("__") {
                    continue;
                }
                if val.is_null() && !send_nil_field {
                    continue;
                }
                map.insert(k.clone(), clean_sink_value(val, send_nil_field));
            }
            serde_json::to_value(map).unwrap_or_else(|_| Value::Object(obj.clone()))
        }
        other => other.clone(),
    }
}

pub(crate) fn format_record_for_sink(
    data: &HashMap<String, Value>,
    opts: &CommonSinkOpts,
) -> Value {
    if data.contains_key("__raw_error__") {
        let mut map = std::collections::BTreeMap::new();
        if let Some(err) = data.get("error") {
            map.insert("error".to_string(), err.clone());
        }
        if let Some(rid) = data.get("rule_id") {
            map.insert("rule_id".to_string(), rid.clone());
        }
        return serde_json::to_value(map).unwrap_or(Value::Null);
    }
    if let Some(ref df) = opts.data_field {
        if let Some(val) = data.get(df) {
            if let Value::Object(obj) = val {
                let mut map = std::collections::BTreeMap::new();
                if let Some(ref fields) = opts.fields {
                    for f in fields {
                        let v = obj.get(f).unwrap_or(&Value::Null);
                        map.insert(f.clone(), clean_sink_value(v, opts.send_nil_field));
                    }
                } else {
                    for (k, v) in obj {
                        if let Some(ref ex) = opts.exclude_fields {
                            if ex.contains(k) {
                                continue;
                            }
                        }
                        if v.is_null() && !opts.send_nil_field {
                            continue;
                        }
                        map.insert(k.clone(), clean_sink_value(v, opts.send_nil_field));
                    }
                }
                return serde_json::to_value(map).unwrap_or_else(|_| val.clone());
            } else {
                return clean_sink_value(val, opts.send_nil_field);
            }
        }
        return Value::Null;
    }
    let mut map = std::collections::BTreeMap::new();
    if let Some(ref fields) = opts.fields {
        for f in fields {
            let val = data.get(f).unwrap_or(&Value::Null);
            if let Some(ref ex) = opts.exclude_fields {
                if ex.contains(f) {
                    continue;
                }
            }
            map.insert(f.clone(), clean_sink_value(val, opts.send_nil_field));
        }
    } else {
        for (k, v) in data {
            if k == rekuiper_sql::eval::META_KEY || k.starts_with("__") {
                continue;
            }
            if let Some(ref ex) = opts.exclude_fields {
                if ex.contains(k) {
                    continue;
                }
            }
            if v.is_null() && !opts.send_nil_field {
                continue;
            }
            map.insert(k.clone(), clean_sink_value(v, opts.send_nil_field));
        }
    }
    serde_json::to_value(map).unwrap_or(Value::Null)
}

pub(crate) fn format_record_for_sink_val(val: &Value, opts: &CommonSinkOpts) -> Value {
    match val {
        Value::Object(map) => {
            let hm: HashMap<String, Value> =
                map.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            format_record_for_sink(&hm, opts)
        }
        other => other.clone(),
    }
}

pub(crate) fn format_record_delimited(
    val: &Value,
    delimiter: &str,
    fields: Option<&[String]>,
) -> String {
    match val {
        Value::Object(map) => {
            let keys: Vec<String> = if let Some(f) = fields {
                f.to_vec()
            } else {
                let mut k: Vec<String> = map.keys().cloned().collect();
                k.sort();
                k
            };
            keys.iter()
                .map(|k| csv_cell(map.get(k), delimiter))
                .collect::<Vec<_>>()
                .join(delimiter)
        }
        Value::Array(arr) => arr
            .iter()
            .map(|item| format_record_delimited(item, delimiter, fields))
            .collect::<Vec<_>>()
            .join("\n"),
        other => csv_cell(Some(other), delimiter),
    }
}

pub(crate) fn to_sink_payload(val: Value, send_single: bool) -> Value {
    if send_single {
        val
    } else {
        Value::Array(vec![val])
    }
}

pub(crate) const BATCH_ROWS_KEY: &str = "__batch_rows__";

/// One sink action parsed once at rule start (never per record).
#[derive(Debug, Clone)]
pub(crate) enum PreparedAction {
    Log,
    Nop,
    File {
        path: std::path::PathBuf,
        template: Option<String>,
        delimited: bool,
        parquet: bool,
        has_header: bool,
        delimiter: String,
        opts: CommonSinkOpts,
    },
    Rest {
        url: String,
        method: String,
        headers: HashMap<String, String>,
        body_type: String,
        template: Option<String>,
        opts: CommonSinkOpts,
        format: Option<String>,
        delimiter: String,
    },
    Mqtt {
        config: Box<MqttConfig>,
        template: Option<String>,
        opts: CommonSinkOpts,
        format: Option<String>,
        delimiter: String,
    },
    WebSocket {
        url: String,
        template: Option<String>,
        opts: CommonSinkOpts,
    },
    Redis {
        config: Box<RedisSinkConfig>,
        opts: CommonSinkOpts,
    },
    Kafka {
        config: Box<KafkaConfig>,
        opts: CommonSinkOpts,
    },
    Sql {
        config: Box<SqlConnectorConfig>,
    },
    Memory {
        topic: String,
        send_nil_field: bool,
    },
    RabbitMq {
        config: Box<RabbitMqConfig>,
        template: Option<String>,
        opts: CommonSinkOpts,
    },
    EdgeX {
        device_name: String,
        profile_name: String,
        source_name: String,
        config: Box<MqttConfig>,
        template: Option<String>,
    },
    Unknown {
        kind: String,
    },
}

impl PreparedAction {
    pub(crate) fn common_opts(&self) -> Option<&CommonSinkOpts> {
        match self {
            PreparedAction::File { opts, .. } => Some(opts),
            PreparedAction::Rest { opts, .. } => Some(opts),
            PreparedAction::Mqtt { opts, .. } => Some(opts),
            PreparedAction::WebSocket { opts, .. } => Some(opts),
            PreparedAction::Redis { opts, .. } => Some(opts),
            PreparedAction::Kafka { opts, .. } => Some(opts),
            PreparedAction::RabbitMq { opts, .. } => Some(opts),
            _ => None,
        }
    }
}

pub(crate) fn prepare_actions(
    actions: &[HashMap<String, Value>],
    rule_id: &str,
    source_configs: &Arc<RwLock<HashMap<String, Value>>>,
    rule_options: Option<&HashMap<String, Value>>,
) -> Vec<PreparedAction> {
    let rule_send_nil_field = rule_options
        .and_then(|o| o.get("sendNilField"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let mut out = Vec::new();
    for action in actions {
        for (kind, opts) in action {
            if kind == "disabled" || is_action_disabled(action, opts) {
                continue;
            }
            match kind.as_str() {
                "log" => out.push(PreparedAction::Log),
                "nop" => out.push(PreparedAction::Nop),
                "file" => match serde_json::from_value::<FileSink>(opts.clone()) {
                    Ok(sink) => {
                        let delimited = sink
                            .format
                            .as_deref()
                            .is_some_and(|f| f.eq_ignore_ascii_case("delimited"))
                            || sink
                                .file_type
                                .as_deref()
                                .is_some_and(|t| t.eq_ignore_ascii_case("csv"));
                        let parquet = sink
                            .format
                            .as_deref()
                            .is_some_and(|f| f.eq_ignore_ascii_case("parquet"))
                            || sink
                                .file_type
                                .as_deref()
                                .is_some_and(|t| t.eq_ignore_ascii_case("parquet"))
                            || sink
                                .path
                                .extension()
                                .and_then(|ext| ext.to_str())
                                .is_some_and(|ext| ext.eq_ignore_ascii_case("parquet"));
                        let common_opts = parse_common_opts(opts, rule_send_nil_field, true);
                        out.push(PreparedAction::File {
                            path: sink.path.clone(),
                            template: action_template(opts).map(|s| s.to_string()),
                            delimited,
                            parquet,
                            has_header: sink.has_header,
                            delimiter: sink
                                .delimiter
                                .clone()
                                .filter(|d| !d.is_empty())
                                .unwrap_or_else(|| ",".to_string()),
                            opts: common_opts,
                        });
                    }
                    Err(_) => out.push(PreparedAction::Unknown {
                        kind: "file".to_string(),
                    }),
                },
                "rest" | "http" => {
                    let url = opts
                        .get("url")
                        .and_then(|v| v.as_str())
                        .filter(|s| !s.is_empty())
                        .unwrap_or("http://localhost")
                        .to_string();
                    let method = opts
                        .get("method")
                        .and_then(|v| v.as_str())
                        .unwrap_or("post")
                        .to_uppercase();
                    let body_type = opts
                        .get("bodyType")
                        .and_then(|v| v.as_str())
                        .unwrap_or("json")
                        .to_lowercase();
                    let mut headers = HashMap::new();
                    if let Some(h) = opts.get("headers").and_then(|v| v.as_object()) {
                        for (k, v) in h {
                            if let Some(s) = v.as_str() {
                                headers.insert(k.clone(), s.to_string());
                            } else {
                                headers.insert(k.clone(), v.to_string());
                            }
                        }
                    }
                    let format = opts
                        .get("format")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    let delimiter = opts
                        .get("delimiter")
                        .and_then(|v| v.as_str())
                        .unwrap_or(",")
                        .to_string();
                    let common_opts = parse_common_opts(opts, rule_send_nil_field, false);
                    out.push(PreparedAction::Rest {
                        url,
                        method,
                        headers,
                        body_type,
                        template: action_template(opts).map(|s| s.to_string()),
                        opts: common_opts,
                        format,
                        delimiter,
                    });
                }
                "mqtt" => match serde_json::from_value::<MqttConfig>(opts.clone()) {
                    Ok(config) => {
                        let format = opts
                            .get("format")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());
                        let delimiter = opts
                            .get("delimiter")
                            .and_then(|v| v.as_str())
                            .unwrap_or(",")
                            .to_string();
                        let common_opts = parse_common_opts(opts, rule_send_nil_field, false);
                        out.push(PreparedAction::Mqtt {
                            config: Box::new(config),
                            template: action_template(opts).map(|s| s.to_string()),
                            opts: common_opts,
                            format,
                            delimiter,
                        });
                    }
                    Err(_) => out.push(PreparedAction::Unknown {
                        kind: "mqtt".to_string(),
                    }),
                },
                "websocket" => match serde_json::from_value::<WebSocketConfig>(opts.clone()) {
                    Ok(ws_cfg) => {
                        let common_opts = parse_common_opts(opts, rule_send_nil_field, true);
                        out.push(PreparedAction::WebSocket {
                            url: ws_cfg.target_url(),
                            template: action_template(opts).map(|s| s.to_string()),
                            opts: common_opts,
                        });
                    }
                    Err(_) => out.push(PreparedAction::Unknown {
                        kind: "websocket".to_string(),
                    }),
                },
                "redis" | "redispub" | "redisPub" => {
                    match serde_json::from_value::<RedisSinkConfig>(opts.clone()) {
                        Ok(config) => {
                            let common_opts = parse_common_opts(opts, rule_send_nil_field, true);
                            out.push(PreparedAction::Redis {
                                config: Box::new(config),
                                opts: common_opts,
                            });
                        }
                        Err(_) => out.push(PreparedAction::Unknown {
                            kind: "redis".to_string(),
                        }),
                    }
                }
                "kafka" => match serde_json::from_value::<KafkaConfig>(opts.clone()) {
                    Ok(config) => {
                        let common_opts = parse_common_opts(opts, rule_send_nil_field, true);
                        out.push(PreparedAction::Kafka {
                            config: Box::new(config),
                            opts: common_opts,
                        });
                    }
                    Err(_) => out.push(PreparedAction::Unknown {
                        kind: "kafka".to_string(),
                    }),
                },
                "sql" => match serde_json::from_value::<SqlConnectorConfig>(opts.clone()) {
                    Ok(config) => out.push(PreparedAction::Sql {
                        config: Box::new(config),
                    }),
                    Err(_) => out.push(PreparedAction::Unknown {
                        kind: "sql".to_string(),
                    }),
                },
                "memory" => {
                    let send_nil_field = opts
                        .get("sendNilField")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(rule_send_nil_field);
                    out.push(PreparedAction::Memory {
                        topic: opts
                            .get("topic")
                            .and_then(|v| v.as_str())
                            .unwrap_or("default")
                            .to_string(),
                        send_nil_field,
                    });
                }
                "rabbitmq" | "amqp" => {
                    match serde_json::from_value::<RabbitMqConfig>(opts.clone()) {
                        Ok(config) => {
                            let common_opts = parse_common_opts(opts, rule_send_nil_field, true);
                            out.push(PreparedAction::RabbitMq {
                                config: Box::new(config),
                                template: action_template(opts).map(|s| s.to_string()),
                                opts: common_opts,
                            });
                        }
                        Err(_) => out.push(PreparedAction::Unknown {
                            kind: "rabbitmq".to_string(),
                        }),
                    }
                }
                "edgex" => {
                    let mut config = MqttConfig {
                        format: PayloadFormat::EdgeX,
                        ..Default::default()
                    };

                    let conf_key = opts
                        .get("confKey")
                        .or_else(|| opts.get("CONF_KEY"))
                        .or_else(|| opts.get("connectionSelector"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("default");

                    let lookup = format!("edgex/{}", conf_key);
                    let configs_guard = source_configs.read();
                    let default_conf = configs_guard
                        .get(&lookup)
                        .or_else(|| configs_guard.get(&lookup.to_ascii_lowercase()))
                        .or_else(|| configs_guard.get("edgex/default"))
                        .or_else(|| configs_guard.get(conf_key))
                        .cloned();
                    drop(configs_guard);

                    let def_srv = default_conf
                        .as_ref()
                        .and_then(|c| c.get("server"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("edgex-mqtt-broker");
                    let def_port = default_conf
                        .as_ref()
                        .and_then(|c| c.get("port"))
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
                    let def_proto = default_conf
                        .as_ref()
                        .and_then(|c| c.get("protocol"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("tcp");

                    let srv = opts
                        .get("server")
                        .and_then(|v| v.as_str())
                        .unwrap_or(def_srv);
                    let port = opts
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
                        .unwrap_or(def_port);
                    let proto = opts
                        .get("protocol")
                        .and_then(|v| v.as_str())
                        .unwrap_or(def_proto);

                    if srv.contains("://") {
                        config.server = srv.to_string();
                    } else {
                        config.server = format!("{}://{}:{}", proto, srv, port);
                    }

                    config.topic = opts
                        .get("topic")
                        .and_then(|v| v.as_str())
                        .unwrap_or("edgex/alerts")
                        .to_string();

                    let opt_sec = opts
                        .get("optional")
                        .or_else(|| default_conf.as_ref().and_then(|c| c.get("optional")));
                    if let Some(opt) = opt_sec {
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

                    let device_name = opts
                        .get("deviceName")
                        .or_else(|| opts.get("device_name"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("kuiper")
                        .to_string();

                    let profile_name = opts
                        .get("profileName")
                        .or_else(|| opts.get("profile_name"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("kuiperProfile")
                        .to_string();

                    let source_name = opts
                        .get("sourceName")
                        .or_else(|| opts.get("source_name"))
                        .and_then(|v| v.as_str())
                        .unwrap_or(rule_id)
                        .to_string();

                    out.push(PreparedAction::EdgeX {
                        device_name,
                        profile_name,
                        source_name,
                        config: Box::new(config),
                        template: action_template(opts).map(|s| s.to_string()),
                    });
                }
                other => out.push(PreparedAction::Unknown {
                    kind: other.to_string(),
                }),
            }
        }
    }
    out
}

/// Bounded per-file batch writer owned by one rule's sink worker: directories
/// are created once, one append handle is kept open per destination path, and
/// serialized rows accumulate in a bounded buffer flushed by size, count, or
/// time. `flush` writes buffered bytes (one `write_all` per file, so lines
/// are never torn); it is NOT an fsync/durability commit. `shutdown` flushes
/// remaining bytes; callers propagate I/O errors to rule exceptions.
pub(crate) struct FileBatchWriter {
    path: std::path::PathBuf,
    file: Option<tokio::fs::File>,
    buf: Vec<u8>,
    pub(crate) pending: usize,
    has_header_written: bool,
}

impl FileBatchWriter {
    pub(crate) fn new(path: std::path::PathBuf) -> Self {
        Self {
            path,
            file: None,
            buf: Vec::new(),
            pending: 0,
            has_header_written: false,
        }
    }

    async fn ensure_open(&mut self) -> std::io::Result<()> {
        if self.file.is_none() {
            if let Some(parent) = self.path.parent() {
                if !parent.as_os_str().is_empty() {
                    tokio::fs::create_dir_all(parent).await?;
                }
            }
            let file = tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)
                .await?;
            self.file = Some(file);
        }
        Ok(())
    }

    fn push_text(&mut self, text: &str) {
        self.buf.extend_from_slice(text.as_bytes());
        if !text.ends_with('\n') {
            self.buf.push(b'\n');
        }
        self.pending += 1;
    }

    fn push_delimited(
        &mut self,
        data: &HashMap<String, Value>,
        delimiter: &str,
        header: bool,
        opts: &CommonSinkOpts,
    ) {
        let keys: Vec<String> = if let Some(ref f) = opts.fields {
            f.clone()
        } else {
            let mut k: Vec<String> = data
                .keys()
                .filter(|k| *k != rekuiper_sql::eval::META_KEY && !k.starts_with("__"))
                .cloned()
                .collect();
            k.sort();
            k
        };
        if header && !self.has_header_written {
            let h = keys.join(delimiter);
            self.buf.extend_from_slice(h.as_bytes());
            self.buf.push(b'\n');
            self.has_header_written = true;
        }
        let row: String = keys
            .iter()
            .map(|k| csv_cell(data.get(k), delimiter))
            .collect::<Vec<_>>()
            .join(delimiter);
        self.buf.extend_from_slice(row.as_bytes());
        self.buf.push(b'\n');
        self.pending += 1;
    }

    pub(crate) fn should_flush(&self) -> bool {
        self.pending >= 100 || self.buf.len() >= 64 * 1024
    }

    pub(crate) async fn flush(&mut self) -> std::io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        self.ensure_open().await?;
        use tokio::io::AsyncWriteExt;
        if let Some(file) = self.file.as_mut() {
            file.write_all(&self.buf).await?;
            file.flush().await?;
        }
        self.buf.clear();
        self.pending = 0;
        Ok(())
    }
}

/// Render one CSV cell (mirrors connector semantics): missing/null as empty,
/// strings CSV-escaped, numbers/booleans plain, containers as compact JSON.
pub(crate) fn csv_cell(value: Option<&Value>, delim: &str) -> String {
    match value {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(s)) => {
            if s.contains(delim) || s.contains(['"', '\n', '\r']) {
                format!("\"{}\"", s.replace('"', "\"\""))
            } else {
                s.clone()
            }
        }
        Some(Value::Number(n)) => n.to_string(),
        Some(Value::Bool(b)) => b.to_string(),
        Some(other) => serde_json::to_string(other).unwrap_or_default(),
    }
}

/// Optional sink `dataTemplate` from action options, rendered against the
/// output record before transmission.
pub(crate) fn action_template(opts: &Value) -> Option<&str> {
    opts.get("dataTemplate").and_then(|v| v.as_str())
}

pub(crate) fn record_template_map(data: &HashMap<String, Value>) -> serde_json::Map<String, Value> {
    data.iter()
        .filter(|(k, _)| *k != rekuiper_sql::eval::META_KEY && !k.starts_with("__"))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

pub(crate) fn render_sink_template_row(template: &str, row: &Value) -> String {
    match row {
        Value::Object(m) => {
            let filtered: serde_json::Map<String, Value> = m
                .iter()
                .filter(|(k, _)| *k != rekuiper_sql::eval::META_KEY && !k.starts_with("__"))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            apply_data_template_value(template, &Value::Object(filtered))
        }
        other => apply_data_template_value(template, other),
    }
}

pub(crate) fn render_sink_template(
    template: &str,
    record: &HashMap<String, Value>,
    send_single: bool,
) -> String {
    let map = record_template_map(record);
    if send_single {
        apply_data_template_value(template, &Value::Object(map))
    } else {
        apply_data_template_value(template, &Value::Array(vec![Value::Object(map)]))
    }
}

pub(crate) fn render_sink_template_batch(template: &str, rows: &[Value]) -> String {
    let cleaned_rows: Vec<Value> = rows
        .iter()
        .map(|row| match row {
            Value::Object(m) => {
                let filtered: serde_json::Map<String, Value> = m
                    .iter()
                    .filter(|(k, _)| *k != rekuiper_sql::eval::META_KEY && !k.starts_with("__"))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                Value::Object(filtered)
            }
            other => other.clone(),
        })
        .collect();
    apply_data_template_value(template, &Value::Array(cleaned_rows))
}

/// Where persisted sink caches live (relative to the working directory, like
/// `data/sqliteKV.db`): `data/cache/sink/<rule>/<action index>/`.
pub(crate) const SINK_CACHE_ROOT: &str = "data/cache/sink";
/// Most cached records resent per sink worker tick (keeps live data moving).
pub(crate) const RESEND_BATCH_MAX: u64 = 4096;
/// Pause before probing a destination again after a failed resend.
pub(crate) const RESEND_RETRY_BACKOFF: std::time::Duration = std::time::Duration::from_secs(1);
/// Sink worker tick (file time-flush and cache resend cadence).
pub(crate) const SINK_TICK: std::time::Duration = std::time::Duration::from_millis(100);

/// Cache options per prepared action, in `prepare_actions` order.
pub(crate) fn action_cache_configs(actions: &[HashMap<String, Value>]) -> Vec<Option<CacheConfig>> {
    let mut out = Vec::new();
    for action in actions {
        for (kind, opts) in action {
            if kind == "disabled" || is_action_disabled(action, opts) {
                continue;
            }
            out.push(
                opts.as_object()
                    .map(|o| o.clone().into_iter().collect::<HashMap<String, Value>>())
                    .and_then(|o| CacheConfig::from_action(&o)),
            );
        }
    }
    out
}

/// Per-action sink state that lives as long as the rule's sink worker:
/// persistent connections and the offline cache.
pub(crate) struct ActionRuntime {
    pub(crate) action: PreparedAction,
    pub(crate) mqtt: Option<MqttSink>,
    pub(crate) sql: Option<SqlSink>,
    pub(crate) rabbitmq: Option<RabbitMqSink>,
    pub(crate) cache: Option<SinkCache>,
    /// Earliest time the next resend may be attempted.
    pub(crate) retry_at: tokio::time::Instant,
    pub(crate) last_warn: Option<std::time::Instant>,
    pub(crate) reported_dropped: u64,
    pub(crate) batch_buffer: Vec<Value>,
    pub(crate) last_batch_time: tokio::time::Instant,
}

impl ActionRuntime {
    pub(crate) fn new(action: PreparedAction, cache: Option<SinkCache>) -> Self {
        Self {
            action,
            mqtt: None,
            sql: None,
            rabbitmq: None,
            cache,
            retry_at: tokio::time::Instant::now(),
            last_warn: None,
            reported_dropped: 0,
            batch_buffer: Vec::new(),
            last_batch_time: tokio::time::Instant::now(),
        }
    }
}

/// Shared handles for sending one record.
pub(crate) struct SinkContext<'a> {
    pub(crate) rule_id: &'a str,
    pub(crate) rule_mgr: &'a RuleManager,
    pub(crate) stream_bus: &'a StreamBus,
    pub(crate) http_client: &'a reqwest::Client,
    pub(crate) enable_private_net: bool,
}

/// Why an action did not deliver a record.
pub(crate) enum SendError {
    /// Destination unreachable (network, broker, database): cacheable.
    Retry(String),
    /// The record can never be delivered as configured.
    Permanent(String),
    /// Dropped by policy and already accounted by the action.
    Dropped,
}

/// Outcome of one record across all actions (ordered by severity).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Delivery {
    Delivered,
    Cached,
    Failed,
}

fn http_op_name(method: &str) -> String {
    let mut chars = method.chars();
    match chars.next() {
        None => String::new(),
        Some(f) => f.to_uppercase().collect::<String>() + &chars.as_str().to_lowercase(),
    }
}

fn make_internal_net_error(method: &str, url: &str, ip: std::net::IpAddr, port: u16) -> SendError {
    let op = http_op_name(method);
    let dial_target = match ip {
        std::net::IpAddr::V4(v4) => format!("{}:{}", v4, port),
        std::net::IpAddr::V6(v6) => format!("[{}]:{}", v6, port),
    };
    SendError::Permanent(format!(
        "rest sink fails to send out the data:err={} \"{}\": dial tcp {}: ip {} is in internal network recoverAble=false method={} path=\"{}\"",
        op, url, dial_target, ip, method, url
    ))
}

#[allow(clippy::too_many_arguments)]
async fn execute_rest_request(
    http_client: &reqwest::Client,
    method: &str,
    final_url: &str,
    headers: &HashMap<String, String>,
    content_type: Option<&str>,
    body_str: Option<&str>,
    map: &HashMap<String, Value>,
    enable_private_net: bool,
) -> Result<(), SendError> {
    if !enable_private_net {
        if let Ok(url_obj) = reqwest::Url::parse(final_url) {
            let port = url_obj.port_or_known_default().unwrap_or(80);
            if let Some(host_str) = url_obj.host_str() {
                let cleaned = host_str.trim_start_matches('[').trim_end_matches(']');
                if let Ok(ip) = cleaned.parse::<std::net::IpAddr>() {
                    if is_private_or_internal_ip(ip) {
                        return Err(make_internal_net_error(method, final_url, ip, port));
                    }
                } else if let Ok(addrs) = tokio::net::lookup_host((cleaned, port)).await {
                    for addr in addrs {
                        if is_private_or_internal_ip(addr.ip()) {
                            return Err(make_internal_net_error(
                                method,
                                final_url,
                                addr.ip(),
                                port,
                            ));
                        }
                    }
                }
            }
        }
    }
    let req_method = match method {
        "GET" => reqwest::Method::GET,
        "PUT" => reqwest::Method::PUT,
        "DELETE" => reqwest::Method::DELETE,
        "HEAD" => reqwest::Method::HEAD,
        "PATCH" => reqwest::Method::PATCH,
        _ => reqwest::Method::POST,
    };
    let mut req_builder = http_client.request(req_method, final_url);
    for (hk, hv) in headers {
        let final_hv = if hv.contains("{{") {
            apply_data_template(hv, &record_template_map(map))
        } else {
            hv.clone()
        };
        req_builder = req_builder.header(hk.as_str(), final_hv);
    }
    if let Some(ct) = content_type {
        req_builder = req_builder.header(reqwest::header::CONTENT_TYPE, ct);
    }
    if let Some(body) = body_str {
        req_builder = req_builder.body(body.to_string());
    }
    let res = req_builder.send().await;
    res.map(|_| ())
        .map_err(|e| SendError::Retry(format!("rest action failed: {}", e)))
}

/// Send one record through one action. File actions buffer into the rule's
/// `FileBatchWriter`s (bounded, flushed by size/time/shutdown); `memory`
/// feedback uses non-blocking all-or-nothing publish and counts drops
/// instead of deadlocking against the rule's own input queue. `destination`
/// overrides the MQTT topic / REST URL (cache `resendDestination`).
pub(crate) async fn send_action(
    rt: &mut ActionRuntime,
    output: &StreamRecord,
    destination: Option<&str>,
    ctx: &SinkContext<'_>,
    files: &mut std::collections::HashMap<std::path::PathBuf, FileBatchWriter>,
) -> Result<(), SendError> {
    match &rt.action {
        PreparedAction::Log => {
            tracing::info!("[RULE {}] Matched record: {:?}", ctx.rule_id, output.data);
            Ok(())
        }
        PreparedAction::Nop => Ok(()),
        PreparedAction::File {
            path,
            template,
            delimited,
            parquet,
            has_header,
            delimiter,
            opts,
        } => {
            if *parquet {
                rekuiper_connectors::parquet_io::append_record_to_parquet(output, path)
                    .map_err(|e| SendError::Permanent(format!("parquet append error: {}", e)))?;
            } else {
                let writer = files
                    .entry(path.clone())
                    .or_insert_with(|| FileBatchWriter::new(path.clone()));
                if let Some(Value::Array(arr)) = output.data.get(BATCH_ROWS_KEY) {
                    if *delimited {
                        let text = format_record_delimited(
                            &Value::Array(arr.clone()),
                            delimiter,
                            opts.fields.as_deref(),
                        );
                        writer.push_text(&text);
                    } else if opts.send_single {
                        for row in arr {
                            if let Some(tpl) = template {
                                writer.push_text(&render_sink_template_row(tpl, row));
                            } else {
                                let formatted = format_record_for_sink_val(row, opts);
                                if let Ok(mut line) = serde_json::to_string(&formatted) {
                                    line.push('\n');
                                    writer.buf.extend_from_slice(line.as_bytes());
                                    writer.pending += 1;
                                }
                            }
                        }
                    } else if let Some(tpl) = template {
                        writer.push_text(&render_sink_template_batch(tpl, arr));
                    } else {
                        let formatted: Vec<Value> = arr
                            .iter()
                            .map(|r| format_record_for_sink_val(r, opts))
                            .collect();
                        if let Ok(mut line) = serde_json::to_string(&Value::Array(formatted)) {
                            line.push('\n');
                            writer.buf.extend_from_slice(line.as_bytes());
                            writer.pending += 1;
                        }
                    }
                } else if *delimited {
                    writer.push_delimited(&output.data, delimiter, *has_header, opts);
                } else if let Some(tpl) = template {
                    writer.push_text(&render_sink_template(tpl, &output.data, opts.send_single));
                } else {
                    let formatted = format_record_for_sink(&output.data, opts);
                    let payload = to_sink_payload(formatted, opts.send_single);
                    if let Ok(mut line) = serde_json::to_string(&payload) {
                        line.push('\n');
                        writer.buf.extend_from_slice(line.as_bytes());
                        writer.pending += 1;
                    }
                }
            }
            Ok(())
        }
        PreparedAction::Rest {
            url,
            method,
            headers,
            body_type,
            template,
            opts,
            format,
            delimiter,
        } => {
            let base_url = destination.unwrap_or(url);
            if base_url.is_empty() {
                return Err(SendError::Permanent("rest action missing url".to_string()));
            }
            let is_delimited = format.as_deref() == Some("delimited")
                || opts.format.as_deref() == Some("delimited");

            let resolve_url = |data_map: &HashMap<String, Value>| {
                if base_url.contains("{{") {
                    apply_data_template(base_url, &record_template_map(data_map))
                } else {
                    base_url.to_string()
                }
            };

            if method.eq_ignore_ascii_case("GET") || body_type == "none" {
                let final_url = resolve_url(&output.data);
                return execute_rest_request(
                    ctx.http_client,
                    method,
                    &final_url,
                    headers,
                    None,
                    None,
                    &output.data,
                    ctx.enable_private_net,
                )
                .await;
            }

            if let Some(Value::Array(arr)) = output.data.get(BATCH_ROWS_KEY) {
                if is_delimited {
                    let text = format_record_delimited(
                        &Value::Array(arr.clone()),
                        delimiter,
                        opts.fields.as_deref(),
                    );
                    let final_url = resolve_url(&output.data);
                    return execute_rest_request(
                        ctx.http_client,
                        method,
                        &final_url,
                        headers,
                        Some("text/plain"),
                        Some(&text),
                        &output.data,
                        ctx.enable_private_net,
                    )
                    .await;
                }
                if opts.send_single {
                    for row in arr {
                        let row_map: HashMap<String, Value> = match row {
                            Value::Object(m) => m.clone().into_iter().collect(),
                            _ => HashMap::new(),
                        };
                        let final_url = resolve_url(&row_map);
                        let body_str = if let Some(tpl) = template {
                            render_sink_template_row(tpl, row)
                        } else {
                            let formatted = format_record_for_sink_val(row, opts);
                            serde_json::to_string(&formatted)
                                .map_err(|e| SendError::Permanent(format!("json encode: {}", e)))?
                        };
                        let ct = if body_type == "text" {
                            "text/plain"
                        } else if body_type == "html" {
                            "text/html"
                        } else {
                            "application/json"
                        };
                        execute_rest_request(
                            ctx.http_client,
                            method,
                            &final_url,
                            headers,
                            Some(ct),
                            Some(&body_str),
                            &row_map,
                            ctx.enable_private_net,
                        )
                        .await?;
                    }
                    Ok(())
                } else {
                    let final_url = resolve_url(&output.data);
                    let body_str = if let Some(tpl) = template {
                        render_sink_template_batch(tpl, arr)
                    } else {
                        let formatted_arr: Vec<Value> = arr
                            .iter()
                            .map(|r| format_record_for_sink_val(r, opts))
                            .collect();
                        serde_json::to_string(&Value::Array(formatted_arr))
                            .map_err(|e| SendError::Permanent(format!("json encode: {}", e)))?
                    };
                    let ct = if body_type == "text" {
                        "text/plain"
                    } else if body_type == "html" {
                        "text/html"
                    } else {
                        "application/json"
                    };
                    execute_rest_request(
                        ctx.http_client,
                        method,
                        &final_url,
                        headers,
                        Some(ct),
                        Some(&body_str),
                        &output.data,
                        ctx.enable_private_net,
                    )
                    .await
                }
            } else {
                let final_url = resolve_url(&output.data);
                let (ct, body_str) = if is_delimited {
                    let formatted = format_record_for_sink(&output.data, opts);
                    let row =
                        format_record_delimited(&formatted, delimiter, opts.fields.as_deref());
                    ("text/plain", row)
                } else if body_type == "text" {
                    let s = match template {
                        Some(tpl) => render_sink_template(tpl, &output.data, opts.send_single),
                        None => {
                            let formatted = format_record_for_sink(&output.data, opts);
                            to_sink_payload(formatted, opts.send_single).to_string()
                        }
                    };
                    ("text/plain", s)
                } else if body_type == "html" {
                    let s = match template {
                        Some(tpl) => render_sink_template(tpl, &output.data, opts.send_single),
                        None => {
                            let formatted = format_record_for_sink(&output.data, opts);
                            to_sink_payload(formatted, opts.send_single).to_string()
                        }
                    };
                    ("text/html", s)
                } else {
                    let s = match template {
                        Some(tpl) => render_sink_template(tpl, &output.data, opts.send_single),
                        None => {
                            let formatted = format_record_for_sink(&output.data, opts);
                            serde_json::to_string(&to_sink_payload(formatted, opts.send_single))
                                .map_err(|e| {
                                    SendError::Permanent(format!("json encode error: {}", e))
                                })?
                        }
                    };
                    ("application/json", s)
                };
                execute_rest_request(
                    ctx.http_client,
                    method,
                    &final_url,
                    headers,
                    Some(ct),
                    Some(&body_str),
                    &output.data,
                    ctx.enable_private_net,
                )
                .await
            }
        }
        PreparedAction::Mqtt {
            config,
            template,
            opts,
            format,
            delimiter,
        } => {
            if rt.mqtt.is_none() {
                let sink = MqttSink::new((**config).clone()).map_err(|e| {
                    SendError::Permanent(format!("mqtt action configuration invalid: {}", e))
                })?;
                rt.mqtt = Some(sink);
            }
            let Some(sink) = rt.mqtt.as_ref() else {
                return Err(SendError::Permanent("mqtt sink unavailable".to_string()));
            };
            let is_delimited = format.as_deref() == Some("delimited")
                || opts.format.as_deref() == Some("delimited");

            let resolve_topic = |data_map: &HashMap<String, Value>| -> String {
                match destination {
                    Some(d) => d.to_string(),
                    None => {
                        if config.topic.contains("{{") {
                            apply_data_template(&config.topic, &record_template_map(data_map))
                        } else {
                            config.topic.clone()
                        }
                    }
                }
            };

            if let Some(Value::Array(arr)) = output.data.get(BATCH_ROWS_KEY) {
                if is_delimited {
                    let text = format_record_delimited(
                        &Value::Array(arr.clone()),
                        delimiter,
                        opts.fields.as_deref(),
                    );
                    let topic = resolve_topic(&output.data);
                    sink.send_raw_to(&topic, text.into_bytes())
                        .await
                        .map_err(|e| SendError::Retry(format!("mqtt action failed: {}", e)))?;
                } else if opts.send_single {
                    for row in arr {
                        let row_map: HashMap<String, Value> = match row {
                            Value::Object(m) => m.clone().into_iter().collect(),
                            _ => HashMap::new(),
                        };
                        let topic = resolve_topic(&row_map);
                        let payload = if let Some(tpl) = template {
                            render_sink_template_row(tpl, row).into_bytes()
                        } else {
                            let formatted = format_record_for_sink_val(row, opts);
                            serde_json::to_vec(&formatted).map_err(|e| {
                                SendError::Permanent(format!("mqtt payload encode: {}", e))
                            })?
                        };
                        sink.send_raw_to(&topic, payload)
                            .await
                            .map_err(|e| SendError::Retry(format!("mqtt action failed: {}", e)))?;
                    }
                } else {
                    let topic = resolve_topic(&output.data);
                    let payload = if let Some(tpl) = template {
                        render_sink_template_batch(tpl, arr).into_bytes()
                    } else {
                        let formatted_arr: Vec<Value> = arr
                            .iter()
                            .map(|r| format_record_for_sink_val(r, opts))
                            .collect();
                        serde_json::to_vec(&Value::Array(formatted_arr)).map_err(|e| {
                            SendError::Permanent(format!("mqtt payload encode: {}", e))
                        })?
                    };
                    sink.send_raw_to(&topic, payload)
                        .await
                        .map_err(|e| SendError::Retry(format!("mqtt action failed: {}", e)))?;
                }
                Ok(())
            } else {
                let payload = match template {
                    Some(tpl) => {
                        render_sink_template(tpl, &output.data, opts.send_single).into_bytes()
                    }
                    None => {
                        if let Some(Value::String(raw_err)) = output.data.get("__raw_error__") {
                            raw_err.as_bytes().to_vec()
                        } else if is_delimited {
                            let formatted = format_record_for_sink(&output.data, opts);
                            let row = format_record_delimited(
                                &formatted,
                                delimiter,
                                opts.fields.as_deref(),
                            );
                            row.into_bytes()
                        } else {
                            let formatted = format_record_for_sink(&output.data, opts);
                            serde_json::to_vec(&to_sink_payload(formatted, opts.send_single))
                                .map_err(|e| {
                                    SendError::Permanent(format!("mqtt payload encode: {}", e))
                                })?
                        }
                    }
                };
                let topic = resolve_topic(&output.data);
                sink.send_raw_to(&topic, payload)
                    .await
                    .map_err(|e| SendError::Retry(format!("mqtt action failed: {}", e)))
            }
        }
        PreparedAction::WebSocket {
            url,
            template,
            opts,
        } => {
            let sink = WebSocketSink { url: url.clone() };
            if let Some(Value::Array(arr)) = output.data.get(BATCH_ROWS_KEY) {
                if opts.send_single {
                    for row in arr {
                        let text = if let Some(tpl) = template {
                            render_sink_template_row(tpl, row)
                        } else {
                            let formatted = format_record_for_sink_val(row, opts);
                            serde_json::to_string(&formatted).map_err(|e| {
                                SendError::Permanent(format!("websocket payload encode: {}", e))
                            })?
                        };
                        sink.send_text(&text).await.map_err(|e| {
                            SendError::Retry(format!("websocket action failed: {}", e))
                        })?;
                    }
                } else {
                    let text = if let Some(tpl) = template {
                        render_sink_template_batch(tpl, arr)
                    } else {
                        let formatted_arr: Vec<Value> = arr
                            .iter()
                            .map(|r| format_record_for_sink_val(r, opts))
                            .collect();
                        serde_json::to_string(&Value::Array(formatted_arr)).map_err(|e| {
                            SendError::Permanent(format!("websocket payload encode: {}", e))
                        })?
                    };
                    sink.send_text(&text)
                        .await
                        .map_err(|e| SendError::Retry(format!("websocket action failed: {}", e)))?;
                }
                Ok(())
            } else {
                let res = match template {
                    Some(tpl) => {
                        sink.send_text(&render_sink_template(tpl, &output.data, opts.send_single))
                            .await
                    }
                    None => {
                        let formatted = format_record_for_sink(&output.data, opts);
                        let payload = to_sink_payload(formatted, opts.send_single);
                        let text = serde_json::to_string(&payload).map_err(|e| {
                            SendError::Permanent(format!("websocket payload encode: {}", e))
                        })?;
                        sink.send_text(&text).await
                    }
                };
                res.map_err(|e| SendError::Retry(format!("websocket action failed: {}", e)))
            }
        }
        PreparedAction::Redis { config, opts } => {
            let sink = RedisSink {
                config: (**config).clone(),
            };
            if let Some(Value::Array(arr)) = output.data.get(BATCH_ROWS_KEY) {
                if opts.send_single {
                    for row in arr {
                        let formatted = format_record_for_sink_val(row, opts);
                        let payload = serde_json::to_string(&formatted).map_err(|e| {
                            SendError::Permanent(format!("redis payload encode: {}", e))
                        })?;
                        sink.send_raw(&payload, output)
                            .await
                            .map_err(|e| SendError::Retry(format!("redis action failed: {}", e)))?;
                    }
                    Ok(())
                } else {
                    let formatted_arr: Vec<Value> = arr
                        .iter()
                        .map(|r| format_record_for_sink_val(r, opts))
                        .collect();
                    let payload =
                        serde_json::to_string(&Value::Array(formatted_arr)).map_err(|e| {
                            SendError::Permanent(format!("redis payload encode: {}", e))
                        })?;
                    sink.send_raw(&payload, output)
                        .await
                        .map_err(|e| SendError::Retry(format!("redis action failed: {}", e)))
                }
            } else {
                let formatted = format_record_for_sink(&output.data, opts);
                let payload = serde_json::to_string(&to_sink_payload(formatted, opts.send_single))
                    .map_err(|e| SendError::Permanent(format!("redis payload encode: {}", e)))?;
                sink.send_raw(&payload, output)
                    .await
                    .map_err(|e| SendError::Retry(format!("redis action failed: {}", e)))
            }
        }
        PreparedAction::Kafka { config, opts } => {
            let sink = KafkaSink {
                config: (**config).clone(),
            };
            if let Some(Value::Array(arr)) = output.data.get(BATCH_ROWS_KEY) {
                if opts.send_single {
                    for row in arr {
                        let formatted = format_record_for_sink_val(row, opts);
                        let payload = serde_json::to_vec(&formatted).map_err(|e| {
                            SendError::Permanent(format!("kafka payload encode: {}", e))
                        })?;
                        sink.send_raw(payload, output)
                            .await
                            .map_err(|e| SendError::Retry(format!("kafka action failed: {}", e)))?;
                    }
                    Ok(())
                } else {
                    let formatted_arr: Vec<Value> = arr
                        .iter()
                        .map(|r| format_record_for_sink_val(r, opts))
                        .collect();
                    let payload =
                        serde_json::to_vec(&Value::Array(formatted_arr)).map_err(|e| {
                            SendError::Permanent(format!("kafka payload encode: {}", e))
                        })?;
                    sink.send_raw(payload, output)
                        .await
                        .map_err(|e| SendError::Retry(format!("kafka action failed: {}", e)))
                }
            } else {
                let formatted = format_record_for_sink(&output.data, opts);
                let payload = serde_json::to_vec(&to_sink_payload(formatted, opts.send_single))
                    .map_err(|e| SendError::Permanent(format!("kafka payload encode: {}", e)))?;
                sink.send_raw(payload, output)
                    .await
                    .map_err(|e| SendError::Retry(format!("kafka action failed: {}", e)))
            }
        }
        PreparedAction::Sql { config } => {
            if rt.sql.is_none() {
                rt.sql = Some(SqlSink {
                    config: (**config).clone(),
                });
            }
            let sink = rt.sql.as_ref().unwrap();
            sink.insert_record(output)
                .await
                .map_err(|e| SendError::Retry(format!("sql action failed: {}", e)))
        }
        PreparedAction::Memory {
            topic,
            send_nil_field,
        } => {
            let final_topic = if topic.contains("{{") {
                apply_data_template(topic, &record_template_map(&output.data))
            } else {
                topic.clone()
            };
            if let Some(Value::Array(arr)) = output.data.get(BATCH_ROWS_KEY) {
                for row in arr {
                    let row_obj = match row {
                        Value::Object(m) => {
                            m.clone().into_iter().collect::<HashMap<String, Value>>()
                        }
                        _ => continue,
                    };
                    let mut clean_data = HashMap::new();
                    for (k, v) in &row_obj {
                        if k == rekuiper_sql::eval::META_KEY || k.starts_with("__") {
                            continue;
                        }
                        if v.is_null() && !send_nil_field {
                            continue;
                        }
                        clean_data.insert(k.clone(), clean_sink_value(v, *send_nil_field));
                    }
                    let mut clean_rec = output.clone();
                    clean_rec.data = clean_data;
                    let _ = ctx.stream_bus.try_publish(&final_topic, clean_rec);
                }
                Ok(())
            } else {
                let mut clean_data = HashMap::new();
                for (k, v) in &output.data {
                    if k == rekuiper_sql::eval::META_KEY || k.starts_with("__") {
                        continue;
                    }
                    if v.is_null() && !send_nil_field {
                        continue;
                    }
                    clean_data.insert(k.clone(), clean_sink_value(v, *send_nil_field));
                }
                let mut clean_rec = output.clone();
                clean_rec.data = clean_data;
                match ctx.stream_bus.try_publish(&final_topic, clean_rec) {
                    Ok(_) => Ok(()),
                    // Produced data with nobody listening: not an error.
                    Err(rekuiper_core::PublishError::NoSubscribers) => Ok(()),
                    Err(rekuiper_core::PublishError::Full) => {
                        tracing::warn!(
                            "[RULE {}] memory feedback queue full, dropping record",
                            ctx.rule_id
                        );
                        ctx.rule_mgr.inc_dropped(ctx.rule_id, 1);
                        ctx.rule_mgr.inc_exceptions(ctx.rule_id, 1);
                        Err(SendError::Dropped)
                    }
                    Err(rekuiper_core::PublishError::Closed) => {
                        ctx.rule_mgr.inc_dropped(ctx.rule_id, 1);
                        Err(SendError::Dropped)
                    }
                }
            }
        }
        PreparedAction::RabbitMq {
            config,
            template,
            opts,
        } => {
            if rt.rabbitmq.is_none() {
                rt.rabbitmq = Some(RabbitMqSink::new((**config).clone()));
            }
            let sink = rt.rabbitmq.as_ref().unwrap();
            if let Some(Value::Array(arr)) = output.data.get(BATCH_ROWS_KEY) {
                if opts.send_single {
                    for row in arr {
                        let payload = match template {
                            Some(tpl) => render_sink_template_row(tpl, row).into_bytes(),
                            None => {
                                let formatted = format_record_for_sink_val(row, opts);
                                serde_json::to_vec(&formatted).map_err(|e| {
                                    SendError::Permanent(format!("rabbitmq payload encode: {}", e))
                                })?
                            }
                        };
                        sink.send_raw(&payload).await.map_err(|e| {
                            SendError::Retry(format!("rabbitmq action failed: {}", e))
                        })?;
                    }
                    Ok(())
                } else {
                    let payload = match template {
                        Some(tpl) => render_sink_template_batch(tpl, arr).into_bytes(),
                        None => {
                            let formatted_arr: Vec<Value> = arr
                                .iter()
                                .map(|r| format_record_for_sink_val(r, opts))
                                .collect();
                            serde_json::to_vec(&Value::Array(formatted_arr)).map_err(|e| {
                                SendError::Permanent(format!("rabbitmq payload encode: {}", e))
                            })?
                        }
                    };
                    sink.send_raw(&payload)
                        .await
                        .map_err(|e| SendError::Retry(format!("rabbitmq action failed: {}", e)))
                }
            } else {
                let payload = match template {
                    Some(tpl) => {
                        render_sink_template(tpl, &output.data, opts.send_single).into_bytes()
                    }
                    None => {
                        let formatted = format_record_for_sink(&output.data, opts);
                        serde_json::to_vec(&to_sink_payload(formatted, opts.send_single)).map_err(
                            |e| SendError::Permanent(format!("rabbitmq payload encode: {}", e)),
                        )?
                    }
                };
                sink.send_raw(&payload)
                    .await
                    .map_err(|e| SendError::Retry(format!("rabbitmq action failed: {}", e)))
            }
        }
        PreparedAction::EdgeX {
            device_name,
            profile_name,
            source_name,
            config,
            template,
        } => {
            if rt.mqtt.is_none() {
                let sink = MqttSink::new((**config).clone()).map_err(|e| {
                    SendError::Permanent(format!("edgex action configuration invalid: {}", e))
                })?;
                rt.mqtt = Some(sink);
            }
            let Some(sink) = rt.mqtt.as_ref() else {
                return Err(SendError::Permanent("edgex sink unavailable".to_string()));
            };
            let payload = match template {
                Some(tpl) => render_sink_template(tpl, &output.data, true).into_bytes(),
                None => EdgeXCodec::encode(&output.data, device_name, profile_name, source_name)
                    .map_err(|e| SendError::Permanent(format!("edgex payload encode: {}", e)))?,
            };
            sink.send_raw_to(destination.unwrap_or(&config.topic), payload)
                .await
                .map_err(|e| SendError::Retry(format!("edgex action failed: {}", e)))
        }
        PreparedAction::Unknown { kind } => {
            tracing::debug!("[RULE {}] unknown action '{}', ignoring", ctx.rule_id, kind);
            Ok(())
        }
    }
}

/// Count a failed send as an exception; log at most once per second per
/// action so an outage at high rates cannot flood the log.
pub(crate) fn report_send_error(
    rt: &mut ActionRuntime,
    ctx: &SinkContext<'_>,
    err: &SendError,
    cached: bool,
) {
    let msg = match err {
        SendError::Retry(m) | SendError::Permanent(m) => m,
        SendError::Dropped => return,
    };
    ctx.rule_mgr.record_exception(ctx.rule_id, msg);
    let now = std::time::Instant::now();
    if rt
        .last_warn
        .is_none_or(|t| now.duration_since(t) >= std::time::Duration::from_secs(1))
    {
        rt.last_warn = Some(now);
        match &rt.cache {
            Some(cache) if cached => tracing::warn!(
                "[RULE {}] {} (cached for resend, {} records pending)",
                ctx.rule_id,
                msg,
                cache.len()
            ),
            _ => tracing::warn!("[RULE {}] {}", ctx.rule_id, msg),
        }
    }
}

/// Publish records the cache had to drop into the rule's `dropped` counter.
fn sync_cache_drops(rt: &mut ActionRuntime, ctx: &SinkContext<'_>) {
    if let Some(cache) = &rt.cache {
        let new = cache.dropped.saturating_sub(rt.reported_dropped);
        if new > 0 {
            ctx.rule_mgr.inc_dropped(ctx.rule_id, new);
            rt.reported_dropped = cache.dropped;
        }
    }
}

/// Send one output record through every action, caching recoverable
/// failures for actions with `enableCache`.
pub(crate) async fn deliver_record(
    runtimes: &mut [ActionRuntime],
    output: &StreamRecord,
    ctx: &SinkContext<'_>,
    files: &mut std::collections::HashMap<std::path::PathBuf, FileBatchWriter>,
) -> Delivery {
    let mut delivery = Delivery::Delivered;
    for rt in runtimes.iter_mut() {
        // Cache-first priority: while anything is cached, live data queues
        // behind it so the destination sees strict arrival order.
        if let Some(cache) = rt.cache.as_mut() {
            if cache.config().priority == ResendPriority::CacheFirst && !cache.is_empty() {
                cache.push(output.clone());
                sync_cache_drops(rt, ctx);
                delivery = delivery.max(Delivery::Cached);
                continue;
            }
        }

        // Sink batching (batchSize / lingerInterval)
        let batch_opts = rt.action.common_opts().cloned();
        if let Some(opts) = batch_opts {
            if opts.batch_size > 0 || opts.linger_interval > 0 {
                let rows: Vec<Value> =
                    if let Some(Value::Array(arr)) = output.data.get(BATCH_ROWS_KEY) {
                        arr.iter()
                            .map(|item| format_record_for_sink_val(item, &opts))
                            .collect()
                    } else {
                        vec![format_record_for_sink(&output.data, &opts)]
                    };
                rt.batch_buffer.extend(rows);
                if opts.batch_size > 0 && rt.batch_buffer.len() >= opts.batch_size {
                    while rt.batch_buffer.len() >= opts.batch_size {
                        let batch: Vec<Value> = rt.batch_buffer.drain(..opts.batch_size).collect();
                        rt.last_batch_time = tokio::time::Instant::now();
                        let mut batch_data = HashMap::new();
                        batch_data.insert(BATCH_ROWS_KEY.to_string(), Value::Array(batch));
                        let batch_record = StreamRecord::new(batch_data);
                        if let Err(err) = send_action(rt, &batch_record, None, ctx, files).await {
                            let cacheable =
                                matches!(err, SendError::Retry(_)) && rt.cache.is_some();
                            if let (true, Some(cache)) = (cacheable, rt.cache.as_mut()) {
                                cache.push(batch_record);
                            }
                            report_send_error(rt, ctx, &err, cacheable);
                            if cacheable {
                                sync_cache_drops(rt, ctx);
                                delivery = delivery.max(Delivery::Cached);
                            } else {
                                delivery = Delivery::Failed;
                            }
                        }
                    }
                }
                continue;
            }
        }

        if let Err(err) = send_action(rt, output, None, ctx, files).await {
            let cacheable = matches!(err, SendError::Retry(_)) && rt.cache.is_some();
            if let (true, Some(cache)) = (cacheable, rt.cache.as_mut()) {
                cache.push(output.clone());
            }
            report_send_error(rt, ctx, &err, cacheable);
            if cacheable {
                sync_cache_drops(rt, ctx);
                delivery = delivery.max(Delivery::Cached);
            } else {
                delivery = Delivery::Failed;
            }
        }
    }
    delivery
}

/// Resend cached records for one action, oldest first, paced by
/// `resendInterval`. Stops at the first failure and backs off before
/// probing the destination again. Returns the number of records delivered.
pub(crate) async fn resend_cached(
    rt: &mut ActionRuntime,
    live_recent: bool,
    ctx: &SinkContext<'_>,
    files: &mut std::collections::HashMap<std::path::PathBuf, FileBatchWriter>,
) -> u64 {
    let Some(cfg) = rt
        .cache
        .as_ref()
        .filter(|c| !c.is_empty())
        .map(|c| c.config().clone())
    else {
        return 0;
    };
    if cfg.priority == ResendPriority::LiveFirst && live_recent {
        return 0;
    }
    let mut sent = 0u64;
    while sent < RESEND_BATCH_MAX {
        let now = tokio::time::Instant::now();
        if now < rt.retry_at {
            break;
        }
        let Some(copy) = rt
            .cache
            .as_mut()
            .and_then(|c| c.front())
            .map(|r| sink_cache::resend_copy(&cfg, r))
        else {
            break;
        };
        match send_action(rt, &copy, cfg.destination.as_deref(), ctx, files).await {
            Ok(()) => {
                if let Some(cache) = rt.cache.as_mut() {
                    cache.pop_front();
                }
                sent += 1;
                if !cfg.resend_interval.is_zero() {
                    // Pace from the previous slot so sub-tick intervals still
                    // resend several records per tick.
                    let floor = now.checked_sub(SINK_TICK).unwrap_or(now);
                    rt.retry_at = rt.retry_at.max(floor) + cfg.resend_interval;
                }
            }
            Err(err @ SendError::Retry(_)) => {
                report_send_error(rt, ctx, &err, true);
                rt.retry_at = now + cfg.resend_interval.max(RESEND_RETRY_BACKOFF);
                break;
            }
            Err(err) => {
                // Undeliverable as configured: drop it rather than block the queue.
                report_send_error(rt, ctx, &err, false);
                if let Some(cache) = rt.cache.as_mut() {
                    cache.pop_front();
                }
                ctx.rule_mgr.inc_sink_failed(ctx.rule_id, 1);
            }
        }
    }
    sent
}

#[allow(dead_code)]
async fn dispatch_rule_actions_legacy(
    actions: &[HashMap<String, Value>],
    output: &StreamRecord,
    rule_id: &str,
    rule_mgr: &RuleManager,
    stream_bus: &StreamBus,
    http_client: &reqwest::Client,
) {
    for action in actions {
        for (kind, opts) in action {
            if kind == "disabled" || is_action_disabled(action, opts) {
                continue;
            }
            match kind.as_str() {
                "log" => {
                    tracing::info!("[RULE {}] Matched record: {:?}", rule_id, output.data);
                }
                "nop" => {}
                "file" => match serde_json::from_value::<FileSink>(opts.clone()) {
                    Ok(sink) => {
                        let res = match action_template(opts) {
                            Some(tpl) => {
                                sink.send_text(&apply_data_template(
                                    tpl,
                                    &record_template_map(&output.data),
                                ))
                                .await
                            }
                            None => sink.send(output).await,
                        };
                        if let Err(e) = res {
                            tracing::warn!("[RULE {}] file action failed: {}", rule_id, e);
                            rule_mgr.inc_exceptions(rule_id, 1);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("[RULE {}] invalid file action options: {}", rule_id, e);
                        rule_mgr.inc_exceptions(rule_id, 1);
                    }
                },
                "rest" | "http" => {
                    let url = opts.get("url").and_then(|v| v.as_str()).unwrap_or("");
                    if url.is_empty() {
                        tracing::warn!("[RULE {}] rest action missing url", rule_id);
                        rule_mgr.inc_exceptions(rule_id, 1);
                    } else {
                        let res = match action_template(opts) {
                            Some(tpl) => {
                                http_client
                                    .post(url)
                                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                                    .body(apply_data_template(
                                        tpl,
                                        &record_template_map(&output.data),
                                    ))
                                    .send()
                                    .await
                            }
                            None => http_client.post(url).json(&output.data).send().await,
                        };
                        if let Err(e) = res {
                            tracing::warn!("[RULE {}] rest action failed: {}", rule_id, e);
                            rule_mgr.inc_exceptions(rule_id, 1);
                        }
                    }
                }
                "mqtt" => match serde_json::from_value::<MqttConfig>(opts.clone()) {
                    Ok(cfg) => match MqttSink::new(cfg) {
                        Ok(sink) => {
                            let res = match action_template(opts) {
                                Some(tpl) => {
                                    sink.send_raw(
                                        apply_data_template(
                                            tpl,
                                            &record_template_map(&output.data),
                                        )
                                        .into_bytes(),
                                    )
                                    .await
                                }
                                None => sink.send(output).await,
                            };
                            if let Err(e) = res {
                                tracing::warn!("[RULE {}] mqtt action failed: {}", rule_id, e);
                                rule_mgr.inc_exceptions(rule_id, 1);
                            }
                        }
                        Err(e) => {
                            tracing::warn!("[RULE {}] mqtt action connect failed: {}", rule_id, e);
                            rule_mgr.inc_exceptions(rule_id, 1);
                        }
                    },
                    Err(e) => {
                        tracing::warn!("[RULE {}] invalid mqtt action options: {}", rule_id, e);
                        rule_mgr.inc_exceptions(rule_id, 1);
                    }
                },
                "websocket" => match serde_json::from_value::<WebSocketConfig>(opts.clone()) {
                    Ok(ws_cfg) => {
                        let sink = WebSocketSink {
                            url: ws_cfg.target_url(),
                        };
                        let res = match action_template(opts) {
                            Some(tpl) => {
                                sink.send_text(&apply_data_template(
                                    tpl,
                                    &record_template_map(&output.data),
                                ))
                                .await
                            }
                            None => sink.send(output).await,
                        };
                        if let Err(e) = res {
                            tracing::warn!("[RULE {}] websocket action failed: {}", rule_id, e);
                            rule_mgr.inc_exceptions(rule_id, 1);
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            "[RULE {}] invalid websocket action options: {}",
                            rule_id,
                            e
                        );
                        rule_mgr.inc_exceptions(rule_id, 1);
                    }
                },
                "redis" | "redispub" | "redisPub" => {
                    match serde_json::from_value::<RedisSinkConfig>(opts.clone()) {
                        Ok(cfg) => {
                            let sink = RedisSink { config: cfg };
                            if let Err(e) = sink.send(output).await {
                                tracing::warn!("[RULE {}] redis action failed: {}", rule_id, e);
                                rule_mgr.inc_exceptions(rule_id, 1);
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                "[RULE {}] invalid redis action options: {}",
                                rule_id,
                                e
                            );
                            rule_mgr.inc_exceptions(rule_id, 1);
                        }
                    }
                }
                "kafka" => match serde_json::from_value::<KafkaConfig>(opts.clone()) {
                    Ok(cfg) => {
                        let sink = KafkaSink { config: cfg };
                        if let Err(e) = sink.send(output).await {
                            tracing::warn!("[RULE {}] kafka action failed: {}", rule_id, e);
                            rule_mgr.inc_exceptions(rule_id, 1);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("[RULE {}] invalid kafka action options: {}", rule_id, e);
                        rule_mgr.inc_exceptions(rule_id, 1);
                    }
                },
                "sql" => match serde_json::from_value::<SqlConnectorConfig>(opts.clone()) {
                    Ok(cfg) => {
                        let sink = SqlSink { config: cfg };
                        if let Err(e) = sink.insert_record(output).await {
                            tracing::warn!("[RULE {}] sql action failed: {}", rule_id, e);
                            rule_mgr.inc_exceptions(rule_id, 1);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("[RULE {}] invalid sql action options: {}", rule_id, e);
                        rule_mgr.inc_exceptions(rule_id, 1);
                    }
                },
                "memory" => {
                    let topic = opts
                        .get("topic")
                        .and_then(|v| v.as_str())
                        .unwrap_or("default");
                    // Broadcast send fails only when nobody listens; the data
                    // has still been produced, so never count it as an exception.
                    let _ = stream_bus.publish(topic, output.clone());
                }
                "edgex" => {
                    let mut config = MqttConfig {
                        format: PayloadFormat::EdgeX,
                        ..Default::default()
                    };
                    let srv = opts
                        .get("server")
                        .and_then(|v| v.as_str())
                        .unwrap_or("edgex-mqtt-broker");
                    let port = opts
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
                    let proto = opts
                        .get("protocol")
                        .and_then(|v| v.as_str())
                        .unwrap_or("tcp");

                    if srv.contains("://") {
                        config.server = srv.to_string();
                    } else {
                        config.server = format!("{}://{}:{}", proto, srv, port);
                    }
                    config.topic = opts
                        .get("topic")
                        .and_then(|v| v.as_str())
                        .unwrap_or("edgex/alerts")
                        .to_string();

                    let device_name = opts
                        .get("deviceName")
                        .or_else(|| opts.get("device_name"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("kuiper");
                    let profile_name = opts
                        .get("profileName")
                        .or_else(|| opts.get("profile_name"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("kuiperProfile");
                    let source_name = opts
                        .get("sourceName")
                        .or_else(|| opts.get("source_name"))
                        .and_then(|v| v.as_str())
                        .unwrap_or(rule_id);

                    match MqttSink::new(config) {
                        Ok(sink) => {
                            let payload = match action_template(opts) {
                                Some(tpl) => {
                                    apply_data_template(tpl, &record_template_map(&output.data))
                                        .into_bytes()
                                }
                                None => match EdgeXCodec::encode(
                                    &output.data,
                                    device_name,
                                    profile_name,
                                    source_name,
                                ) {
                                    Ok(b) => b,
                                    Err(e) => {
                                        tracing::warn!(
                                            "[RULE {}] edgex encode failed: {}",
                                            rule_id,
                                            e
                                        );
                                        rule_mgr.inc_exceptions(rule_id, 1);
                                        continue;
                                    }
                                },
                            };
                            if let Err(e) = sink.send_raw(payload).await {
                                tracing::warn!("[RULE {}] edgex action failed: {}", rule_id, e);
                                rule_mgr.inc_exceptions(rule_id, 1);
                            }
                        }
                        Err(e) => {
                            tracing::warn!("[RULE {}] edgex action connect failed: {}", rule_id, e);
                            rule_mgr.inc_exceptions(rule_id, 1);
                        }
                    }
                }
                other => {
                    tracing::debug!("[RULE {}] unknown action '{}', ignoring", rule_id, other);
                }
            }
        }
    }
}

/// Enqueue an output record for the background sink worker. Fast path is a
/// lock-free `try_send`; under backpressure the evaluation loop awaits
/// capacity (bounded memory, no loss). Records enqueue (not completion) here;
/// the sink worker increments `sink_out` only after the actual operation.
/// Closed queues propagate as `false` (rule shutting down). High-water depth
/// and blocked time are accounted on the rule counters.
pub(crate) async fn enqueue_sink_record(
    counters: &RuleCounters,
    sink: &tokio::sync::mpsc::Sender<StreamRecord>,
    output_record: StreamRecord,
) -> bool {
    let depth = sink.max_capacity().saturating_sub(sink.capacity());
    counters.observe_high_water(depth);
    match sink.try_send(output_record) {
        Ok(()) => {
            counters.inc_enqueued(1);
            true
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => false,
        Err(tokio::sync::mpsc::error::TrySendError::Full(rec)) => {
            let depth = sink.max_capacity().saturating_sub(sink.capacity());
            counters.observe_high_water(depth);
            let start = std::time::Instant::now();
            match sink.send(rec).await {
                Ok(()) => {
                    counters.add_blocked_micros(start.elapsed().as_micros() as u64);
                    counters.inc_enqueued(1);
                    true
                }
                Err(_) => false,
            }
        }
    }
}
