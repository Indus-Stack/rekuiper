use crate::engine::sinks::{send_action, ActionRuntime, SendError, SinkContext};
use parking_lot::RwLock;
use rekuiper_core::model::{DlqEnvelope, DlqStage};
use rekuiper_core::{RuleCounters, RuleManager, StreamBus, StreamRecord};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OnErrorPolicy {
    #[default]
    Drop,
    Dlq,
    Halt,
}

impl OnErrorPolicy {
    pub fn from_options(options: Option<&HashMap<String, Value>>) -> Self {
        let Some(opts) = options else {
            return OnErrorPolicy::Drop;
        };
        if let Some(val) = opts.get("onError").and_then(|v| v.as_str()) {
            match val.to_ascii_lowercase().as_str() {
                "dlq" => return OnErrorPolicy::Dlq,
                "halt" => return OnErrorPolicy::Halt,
                "drop" => return OnErrorPolicy::Drop,
                _ => {}
            }
        }
        if opts.contains_key("dlq") {
            OnErrorPolicy::Dlq
        } else {
            OnErrorPolicy::Drop
        }
    }
}

pub struct DlqExternalSink {
    runtime: tokio::sync::Mutex<ActionRuntime>,
    http_client: reqwest::Client,
    stream_bus: StreamBus,
    rule_mgr: RuleManager,
    rule_id: String,
    enable_private_net: bool,
}

impl DlqExternalSink {
    pub(crate) fn new(
        runtime: ActionRuntime,
        http_client: reqwest::Client,
        stream_bus: StreamBus,
        rule_mgr: RuleManager,
        rule_id: String,
        enable_private_net: bool,
    ) -> Self {
        Self {
            runtime: tokio::sync::Mutex::new(runtime),
            http_client,
            stream_bus,
            rule_mgr,
            rule_id,
            enable_private_net,
        }
    }

    pub async fn dispatch(&self, envelope: &DlqEnvelope) -> Result<(), String> {
        let val = serde_json::to_value(envelope).map_err(|e| e.to_string())?;
        let map = match val {
            Value::Object(m) => m.into_iter().collect(),
            _ => HashMap::new(),
        };
        let record = StreamRecord::new(map);
        let mut rt = self.runtime.lock().await;
        let mut files = HashMap::new();
        // IMPORTANT: dlq_router is None to prevent recursive DLQ triggers (Refinement 3)
        let ctx = SinkContext {
            rule_id: &self.rule_id,
            rule_mgr: &self.rule_mgr,
            stream_bus: &self.stream_bus,
            http_client: &self.http_client,
            enable_private_net: self.enable_private_net,
            dlq_router: None,
        };
        send_action(&mut rt, &record, None, &ctx, &mut files)
            .await
            .map_err(|e| match e {
                SendError::Retry(m) => m,
                SendError::Permanent(m) => m,
                SendError::Dropped => "dropped".to_string(),
            })
    }
}

#[derive(Clone)]
pub struct DlqRouter {
    pub rule_id: String,
    pub on_error: OnErrorPolicy,
    pub include_raw_payload: bool,
    pub capacity: usize,
    pub counters: Arc<RuleCounters>,
    pub buffer: Arc<RwLock<VecDeque<DlqEnvelope>>>,
    pub external_sink: Option<Arc<DlqExternalSink>>,
    pub rule_manager: Option<RuleManager>,
}

impl DlqRouter {
    pub fn route_error(
        &self,
        stage: DlqStage,
        error_msg: String,
        raw_payload: Option<&str>,
        tuple: Option<Value>,
        sink_type: Option<&str>,
    ) {
        // 1. Record exception on counters
        self.counters.record_exception(&error_msg);

        if self.on_error == OnErrorPolicy::Drop {
            return;
        }

        // 2. Lock-free AtomicU64 stage counter increment (Refinement 1)
        self.counters.inc_dlq(stage);

        // 3. Create envelope with capped raw_payload (Refinement 2)
        let mut envelope = DlqEnvelope::new(&self.rule_id, stage, &error_msg);
        if self.include_raw_payload {
            if let Some(raw) = raw_payload {
                envelope = envelope.with_raw_payload(raw);
            }
        }
        if let Some(t) = tuple {
            envelope = envelope.with_tuple(t);
        }
        if let Some(st) = sink_type {
            envelope = envelope.with_sink_type(st);
        }

        // 4. Synchronously push to bounded ring buffer (Refinement 3)
        {
            let mut buf = self.buffer.write();
            if buf.len() >= self.capacity {
                buf.pop_front();
            }
            buf.push_back(envelope.clone());
        }

        // 5. Non-blocking external DLQ sink dispatch (Refinement 3)
        if let Some(ref ext) = self.external_sink {
            let ext = ext.clone();
            let c = self.counters.clone();
            let rid = self.rule_id.clone();
            tokio::spawn(async move {
                if let Err(e) = ext.dispatch(&envelope).await {
                    tracing::warn!(
                        "[RULE {}] External DLQ dispatch failed: {}. Not re-entering DLQ.",
                        rid, e
                    );
                    c.inc_dlq_sink_failures();
                }
            });
        }

        // 6. If onError is "halt", cleanly stop the rule (Refinement 4)
        if self.on_error == OnErrorPolicy::Halt {
            if let Some(ref rm) = self.rule_manager {
                let rm = rm.clone();
                let rid = self.rule_id.clone();
                let msg = error_msg.clone();
                tokio::spawn(async move {
                    let _ = rm.halt_rule_with_error(&rid, &msg).await;
                });
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn build(
        rule_id: &str,
        rule_options: Option<&HashMap<String, Value>>,
        counters: Arc<RuleCounters>,
        rule_manager: RuleManager,
        stream_bus: StreamBus,
        http_client: reqwest::Client,
        enable_private_net: bool,
        source_configs: &Arc<RwLock<HashMap<String, Value>>>,
    ) -> Self {
        let on_error = OnErrorPolicy::from_options(rule_options);
        let dlq_opt = rule_options.and_then(|o| o.get("dlq"));
        let (capacity, include_raw_payload, ext_action) = if let Some(Value::Object(map)) = dlq_opt {
            let cap = map
                .get("bufferCapacity")
                .or_else(|| map.get("buffer_capacity"))
                .and_then(|v| v.as_u64())
                .unwrap_or(100) as usize;
            let raw = map
                .get("includeRawPayload")
                .or_else(|| map.get("include_raw_payload"))
                .and_then(|v| v.as_bool())
                .unwrap_or(true);
            let act = map.get("action").cloned();
            (cap, raw, act)
        } else {
            (100, true, None)
        };

        let buffer = rule_manager
            .dlq_buffer(rule_id)
            .unwrap_or_else(|| Arc::new(RwLock::new(VecDeque::with_capacity(capacity))));

        let external_sink = if let Some(act_val) = ext_action {
            let actions_vec: Vec<HashMap<String, Value>> = match act_val {
                Value::Object(obj) => {
                    let mut action_map = HashMap::new();
                    for (k, v) in obj {
                        action_map.insert(k, v);
                    }
                    vec![action_map]
                }
                Value::Array(arr) => arr
                    .into_iter()
                    .filter_map(|v| match v {
                        Value::Object(obj) => Some(obj.into_iter().collect()),
                        _ => None,
                    })
                    .collect(),
                _ => Vec::new(),
            };
            let prepared = crate::engine::sinks::prepare_actions(
                &actions_vec,
                rule_id,
                source_configs,
                rule_options,
            );
            if let Some(first_action) = prepared.into_iter().next() {
                let runtime = ActionRuntime::new(first_action, None);
                Some(Arc::new(DlqExternalSink::new(
                    runtime,
                    http_client,
                    stream_bus,
                    rule_manager.clone(),
                    rule_id.to_string(),
                    enable_private_net,
                )))
            } else {
                None
            }
        } else {
            None
        };

        Self {
            rule_id: rule_id.to_string(),
            on_error,
            include_raw_payload,
            capacity,
            counters,
            buffer,
            external_sink,
            rule_manager: Some(rule_manager),
        }
    }
}

