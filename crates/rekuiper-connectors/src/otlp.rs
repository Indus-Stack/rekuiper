use anyhow::{Context, Result};
use rekuiper_core::model::StreamRecord;
use rekuiper_core::StreamSender;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{error, info};

// ---------------------------------------------------------------------------
// OTLP Flattening & Protocol Payload Decoding
// ---------------------------------------------------------------------------

/// Converts OTLP KeyValue array: `[{"key": "service.name", "value": {"stringValue": "edge"}}]`
/// into a flat JSON map: `{"service.name": "edge"}`.
pub fn otlp_attributes_to_map(attrs_val: Option<&Value>) -> HashMap<String, Value> {
    let mut map = HashMap::new();
    let Some(val) = attrs_val else {
        return map;
    };

    if let Some(arr) = val.as_array() {
        for item in arr {
            if let Some(k) = item.get("key").and_then(|v| v.as_str()) {
                let v = if let Some(v_obj) = item.get("value") {
                    extract_otlp_any_value(v_obj)
                } else {
                    Value::Null
                };
                map.insert(k.to_string(), v);
            }
        }
    } else if let Some(obj) = val.as_object() {
        for (k, v) in obj {
            map.insert(k.clone(), v.clone());
        }
    }
    map
}

/// Extracts primitive value from OTLP `AnyValue` envelope:
/// `{"stringValue": "s"}` -> `"s"`, `{"intValue": 1}` -> `1`, etc.
pub fn extract_otlp_any_value(v: &Value) -> Value {
    if let Some(s) = v.get("stringValue").and_then(|x| x.as_str()) {
        return json!(s);
    }
    if let Some(i) = v.get("intValue").or_else(|| v.get("int_value")) {
        if let Some(n) = i.as_i64() {
            return json!(n);
        } else if let Some(s) = i.as_str() {
            if let Ok(n) = s.parse::<i64>() {
                return json!(n);
            }
        }
    }
    if let Some(d) = v.get("doubleValue").or_else(|| v.get("double_value")) {
        if let Some(n) = d.as_f64() {
            return json!(n);
        } else if let Some(s) = d.as_str() {
            if let Ok(n) = s.parse::<f64>() {
                return json!(n);
            }
        }
    }
    if let Some(b) = v.get("boolValue").or_else(|| v.get("bool_value")).and_then(|x| x.as_bool()) {
        return json!(b);
    }
    if let Some(arr) = v.get("arrayValue").or_else(|| v.get("array_value")).and_then(|x| x.get("values")).and_then(|x| x.as_array()) {
        let list: Vec<Value> = arr.iter().map(extract_otlp_any_value).collect();
        return json!(list);
    }
    if let Some(kv) = v.get("kvlistValue").or_else(|| v.get("kvlist_value")).and_then(|x| x.get("values")) {
        return json!(otlp_attributes_to_map(Some(kv)));
    }
    v.clone()
}

fn extract_numeric_value(dp: &Value) -> Value {
    if let Some(d) = dp.get("asDouble").or_else(|| dp.get("as_double")) {
        if let Some(n) = d.as_f64() {
            return json!(n);
        } else if let Some(s) = d.as_str() {
            if let Ok(n) = s.parse::<f64>() {
                return json!(n);
            }
        }
    }
    if let Some(i) = dp.get("asInt").or_else(|| dp.get("as_int")) {
        if let Some(n) = i.as_i64() {
            return json!(n);
        } else if let Some(s) = i.as_str() {
            if let Ok(n) = s.parse::<i64>() {
                return json!(n);
            }
        }
    }
    Value::Null
}

fn extract_unix_nano(dp: &Value) -> i64 {
    let t = dp.get("timeUnixNano")
        .or_else(|| dp.get("time_unix_nano"))
        .or_else(|| dp.get("startTimeUnixNano"))
        .or_else(|| dp.get("start_time_unix_nano"))
        .or_else(|| dp.get("observedTimeUnixNano"));

    if let Some(val) = t {
        if let Some(n) = val.as_i64() {
            return n;
        } else if let Some(s) = val.as_str() {
            if let Ok(n) = s.parse::<i64>() {
                return n;
            }
        }
    }
    chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
}

/// Parses an OTLP JSON payload containing `resourceMetrics`, `resourceLogs`, or `resourceSpans`,
/// flattening each into uniform, SQL-friendly records.
pub fn parse_otlp_payload(body: &[u8]) -> Result<Vec<StreamRecord>> {
    let root: Value = serde_json::from_slice(body)
        .context("Invalid OTLP JSON payload format")?;

    let mut records = Vec::new();

    // 1. Process Metrics (resourceMetrics)
    if let Some(resource_metrics) = root.get("resourceMetrics").or_else(|| root.get("resource_metrics")).and_then(|v| v.as_array()) {
        for rm in resource_metrics {
            let resource = otlp_attributes_to_map(rm.get("resource").and_then(|r| r.get("attributes")));
            let scope_metrics = rm.get("scopeMetrics").or_else(|| rm.get("scope_metrics")).and_then(|v| v.as_array());
            if let Some(scopes) = scope_metrics {
                for sm in scopes {
                    if let Some(metrics) = sm.get("metrics").and_then(|v| v.as_array()) {
                        for m in metrics {
                            let name = m.get("name").and_then(|v| v.as_str()).unwrap_or("unknown");
                            let unit = m.get("unit").and_then(|v| v.as_str()).unwrap_or("");
                            
                            // Check gauge
                            if let Some(g) = m.get("gauge").and_then(|v| v.get("dataPoints").or_else(|| v.get("data_points"))).and_then(|v| v.as_array()) {
                                for dp in g {
                                    let mut data = HashMap::new();
                                    data.insert("signal".to_string(), json!("metric"));
                                    data.insert("name".to_string(), json!(name));
                                    data.insert("type".to_string(), json!("gauge"));
                                    data.insert("unit".to_string(), json!(unit));
                                    data.insert("value".to_string(), extract_numeric_value(dp));
                                    data.insert("timestamp_unix_nano".to_string(), json!(extract_unix_nano(dp)));
                                    data.insert("resource".to_string(), json!(resource));
                                    data.insert("attributes".to_string(), json!(otlp_attributes_to_map(dp.get("attributes"))));
                                    records.push(StreamRecord::new(data));
                                }
                            }
                            // Check sum
                            if let Some(s) = m.get("sum").and_then(|v| v.get("dataPoints").or_else(|| v.get("data_points"))).and_then(|v| v.as_array()) {
                                for dp in s {
                                    let mut data = HashMap::new();
                                    data.insert("signal".to_string(), json!("metric"));
                                    data.insert("name".to_string(), json!(name));
                                    data.insert("type".to_string(), json!("sum"));
                                    data.insert("unit".to_string(), json!(unit));
                                    data.insert("value".to_string(), extract_numeric_value(dp));
                                    data.insert("timestamp_unix_nano".to_string(), json!(extract_unix_nano(dp)));
                                    data.insert("resource".to_string(), json!(resource));
                                    data.insert("attributes".to_string(), json!(otlp_attributes_to_map(dp.get("attributes"))));
                                    records.push(StreamRecord::new(data));
                                }
                            }
                            // Check histogram
                            if let Some(h) = m.get("histogram").and_then(|v| v.get("dataPoints").or_else(|| v.get("data_points"))).and_then(|v| v.as_array()) {
                                for dp in h {
                                    let mut data = HashMap::new();
                                    data.insert("signal".to_string(), json!("metric"));
                                    data.insert("name".to_string(), json!(name));
                                    data.insert("type".to_string(), json!("histogram"));
                                    data.insert("unit".to_string(), json!(unit));
                                    data.insert("count".to_string(), dp.get("count").cloned().unwrap_or(json!(0)));
                                    data.insert("value".to_string(), dp.get("sum").cloned().unwrap_or(json!(0.0)));
                                    if let Some(min_v) = dp.get("min") {
                                        data.insert("min".to_string(), min_v.clone());
                                    }
                                    if let Some(max_v) = dp.get("max") {
                                        data.insert("max".to_string(), max_v.clone());
                                    }
                                    data.insert("timestamp_unix_nano".to_string(), json!(extract_unix_nano(dp)));
                                    data.insert("resource".to_string(), json!(resource));
                                    data.insert("attributes".to_string(), json!(otlp_attributes_to_map(dp.get("attributes"))));
                                    records.push(StreamRecord::new(data));
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // 2. Process Logs (resourceLogs)
    if let Some(resource_logs) = root.get("resourceLogs").or_else(|| root.get("resource_logs")).and_then(|v| v.as_array()) {
        for rl in resource_logs {
            let resource = otlp_attributes_to_map(rl.get("resource").and_then(|r| r.get("attributes")));
            let scope_logs = rl.get("scopeLogs").or_else(|| rl.get("scope_logs")).and_then(|v| v.as_array());
            if let Some(scopes) = scope_logs {
                for sl in scopes {
                    if let Some(log_records) = sl.get("logRecords").or_else(|| sl.get("log_records")).and_then(|v| v.as_array()) {
                        for lr in log_records {
                            let mut data = HashMap::new();
                            data.insert("signal".to_string(), json!("log"));
                            data.insert("severity_text".to_string(), lr.get("severityText").or_else(|| lr.get("severity_text")).cloned().unwrap_or(json!("INFO")));
                            data.insert("severity_number".to_string(), lr.get("severityNumber").or_else(|| lr.get("severity_number")).cloned().unwrap_or(json!(9)));
                            
                            let body_val = if let Some(b) = lr.get("body") {
                                extract_otlp_any_value(b)
                            } else {
                                Value::Null
                            };
                            data.insert("body".to_string(), body_val);
                            data.insert("trace_id".to_string(), lr.get("traceId").or_else(|| lr.get("trace_id")).cloned().unwrap_or(json!("")));
                            data.insert("span_id".to_string(), lr.get("spanId").or_else(|| lr.get("span_id")).cloned().unwrap_or(json!("")));
                            data.insert("timestamp_unix_nano".to_string(), json!(extract_unix_nano(lr)));
                            data.insert("resource".to_string(), json!(resource));
                            data.insert("attributes".to_string(), json!(otlp_attributes_to_map(lr.get("attributes"))));
                            records.push(StreamRecord::new(data));
                        }
                    }
                }
            }
        }
    }

    // 3. Process Traces (resourceSpans)
    if let Some(resource_spans) = root.get("resourceSpans").or_else(|| root.get("resource_spans")).and_then(|v| v.as_array()) {
        for rs in resource_spans {
            let resource = otlp_attributes_to_map(rs.get("resource").and_then(|r| r.get("attributes")));
            let scope_spans = rs.get("scopeSpans").or_else(|| rs.get("scope_spans")).and_then(|v| v.as_array());
            if let Some(scopes) = scope_spans {
                for ss in scopes {
                    if let Some(spans) = ss.get("spans").and_then(|v| v.as_array()) {
                        for sp in spans {
                            let mut data = HashMap::new();
                            data.insert("signal".to_string(), json!("span"));
                            data.insert("name".to_string(), sp.get("name").cloned().unwrap_or(json!("unknown")));
                            data.insert("trace_id".to_string(), sp.get("traceId").or_else(|| sp.get("trace_id")).cloned().unwrap_or(json!("")));
                            data.insert("span_id".to_string(), sp.get("spanId").or_else(|| sp.get("span_id")).cloned().unwrap_or(json!("")));
                            data.insert("parent_span_id".to_string(), sp.get("parentSpanId").or_else(|| sp.get("parent_span_id")).cloned().unwrap_or(json!("")));
                            
                            let start_time = extract_unix_nano(sp);
                            let end_time = sp.get("endTimeUnixNano")
                                .or_else(|| sp.get("end_time_unix_nano"))
                                .and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok())))
                                .unwrap_or(start_time);
                            let duration_ns = if end_time >= start_time { end_time - start_time } else { 0 };

                            data.insert("duration_ns".to_string(), json!(duration_ns));
                            let status_code = sp.get("status")
                                .and_then(|s| s.get("code").or_else(|| s.get("message")))
                                .map(|c| c.to_string())
                                .unwrap_or_else(|| "OK".to_string());
                            data.insert("status_code".to_string(), json!(status_code));
                            data.insert("timestamp_unix_nano".to_string(), json!(start_time));
                            data.insert("resource".to_string(), json!(resource));
                            data.insert("attributes".to_string(), json!(otlp_attributes_to_map(sp.get("attributes"))));
                            records.push(StreamRecord::new(data));
                        }
                    }
                }
            }
        }
    }

    // Fallback: If payload is already a direct record or array of records
    if records.is_empty() {
        if let Some(arr) = root.as_array() {
            for item in arr {
                if let Some(obj) = item.as_object() {
                    let mut data = HashMap::new();
                    for (k, v) in obj {
                        data.insert(k.clone(), v.clone());
                    }
                    records.push(StreamRecord::new(data));
                }
            }
        } else if let Some(obj) = root.as_object() {
            let mut data = HashMap::new();
            for (k, v) in obj {
                data.insert(k.clone(), v.clone());
            }
            records.push(StreamRecord::new(data));
        }
    }

    Ok(records)
}

// ---------------------------------------------------------------------------
// OTLP Standalone Source Listener
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtlpSourceConfig {
    pub bind: Option<String>,
    pub endpoint: Option<String>,
    pub interval: Option<String>,
}

pub struct OtlpSource {
    pub config: OtlpSourceConfig,
    pub tx: StreamSender,
}

impl OtlpSource {
    pub fn new(config: OtlpSourceConfig, tx: StreamSender) -> Self {
        Self { config, tx }
    }

    pub fn spawn(self, mut cancel_rx: watch::Receiver<bool>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let bind_addr = self.config.bind
                .as_deref()
                .or(self.config.endpoint.as_deref())
                .unwrap_or("0.0.0.0:4318");

            let listener = match TcpListener::bind(bind_addr).await {
                Ok(l) => {
                    info!("OTLP source listening on {}", bind_addr);
                    l
                }
                Err(e) => {
                    error!("Failed to bind OTLP source listener on {}: {}", bind_addr, e);
                    return;
                }
            };

            loop {
                tokio::select! {
                    _ = cancel_rx.changed() => {
                        if *cancel_rx.borrow() {
                            break;
                        }
                    }
                    accept_res = listener.accept() => {
                        let Ok((mut socket, _)) = accept_res else {
                            continue;
                        };
                        let tx_clone = self.tx.clone();

                        tokio::spawn(async move {
                            let mut buf = Vec::with_capacity(4096);
                            let mut temp = [0u8; 2048];
                            let mut content_length: Option<usize> = None;
                            let mut body_start: Option<usize> = None;

                            loop {
                                let n = match socket.read(&mut temp).await {
                                    Ok(0) => break,
                                    Ok(n) => n,
                                    Err(_) => break,
                                };
                                buf.extend_from_slice(&temp[..n]);

                                if body_start.is_none() {
                                    if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                                        let headers_str = String::from_utf8_lossy(&buf[..pos]);
                                        body_start = Some(pos + 4);
                                        for line in headers_str.lines() {
                                            if let Some((k, v)) = line.split_once(':') {
                                                if k.trim().eq_ignore_ascii_case("content-length") {
                                                    content_length = v.trim().parse::<usize>().ok();
                                                }
                                            }
                                        }
                                    }
                                }

                                if let (Some(start), Some(len)) = (body_start, content_length) {
                                    if buf.len() >= start + len {
                                        let body_bytes = &buf[start..start + len];
                                        match parse_otlp_payload(body_bytes) {
                                            Ok(recs) => {
                                                for rec in recs {
                                                    let _ = tx_clone.send(rec).await;
                                                }
                                                let resp = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 20\r\n\r\n{\"partialSuccess\":{}}";
                                                let _ = socket.write_all(resp).await;
                                            }
                                            Err(e) => {
                                                let mut err_data = HashMap::new();
                                                err_data.insert("__raw_error__".to_string(), json!(e.to_string()));
                                                err_data.insert("__raw_payload__".to_string(), json!(String::from_utf8_lossy(body_bytes).to_string()));
                                                let _ = tx_clone.send(StreamRecord::new(err_data)).await;

                                                let err_body = format!("{{\"error\":\"{}\"}}", e);
                                                let resp = format!(
                                                    "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                                                    err_body.len(),
                                                    err_body
                                                );
                                                let _ = socket.write_all(resp.as_bytes()).await;
                                            }
                                        }
                                        break;
                                    }
                                }
                            }
                        });
                    }
                }
            }
        })
    }
}

// ---------------------------------------------------------------------------
// OTLP Sink Connector
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtlpSinkConfig {
    pub endpoint: String,
    pub signal: Option<String>,
    pub headers: Option<HashMap<String, String>>,
    pub timeout: Option<u64>,
}

#[derive(Clone)]
pub struct OtlpSink {
    config: OtlpSinkConfig,
    client: reqwest::Client,
}

impl OtlpSink {
    pub fn new(config: OtlpSinkConfig) -> Self {
        let timeout_ms = config.timeout.unwrap_or(3000);
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self { config, client }
    }

    /// Formats a single output record or batch into an OTLP export payload and POSTs to the endpoint.
    /// Returns:
    /// - `Ok(())` on successful delivery (HTTP 2xx).
    /// - `Err((true, err))` on permanent client error (400, 401, 403, 422) -> routes to DLQ.
    /// - `Err((false, err))` on transient network/server error (408, 429, 5xx) -> routes to SinkCache.
    pub async fn write_record(&self, record: &HashMap<String, Value>) -> Result<(), (bool, String)> {
        let payload = self.format_otlp_payload(record);
        self.dispatch_otlp_http(&payload).await
    }

    pub async fn write_batch(&self, records: &[HashMap<String, Value>]) -> Result<(), (bool, String)> {
        for record in records {
            self.write_record(record).await?;
        }
        Ok(())
    }

    fn format_otlp_payload(&self, record: &HashMap<String, Value>) -> Value {
        let signal = self.config.signal.as_deref().unwrap_or_else(|| {
            record.get("signal").and_then(|v| v.as_str()).unwrap_or_else(|| {
                if record.contains_key("body") || record.contains_key("severity_number") {
                    "log"
                } else if record.contains_key("duration_ns") {
                    "span"
                } else {
                    "metric"
                }
            })
        });

        let resource_attrs = record.get("resource").cloned().unwrap_or_else(|| json!({}));
        let mut resource_kv = Vec::new();
        if let Some(obj) = resource_attrs.as_object() {
            for (k, v) in obj {
                resource_kv.push(json!({ "key": k, "value": { "stringValue": v.to_string().trim_matches('"') } }));
            }
        }

        let custom_attrs = record.get("attributes").cloned().unwrap_or_else(|| json!({}));
        let mut attrs_kv = Vec::new();
        if let Some(obj) = custom_attrs.as_object() {
            for (k, v) in obj {
                attrs_kv.push(json!({ "key": k, "value": { "stringValue": v.to_string().trim_matches('"') } }));
            }
        }

        let now_nano = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let time_nano = record.get("timestamp_unix_nano")
            .and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok())))
            .unwrap_or(now_nano);

        match signal {
            "log" => {
                let body_val = record.get("body").cloned().unwrap_or_else(|| json!(""));
                let body_envelope = if body_val.is_string() {
                    json!({ "stringValue": body_val })
                } else {
                    json!({ "stringValue": body_val.to_string() })
                };
                let severity_text = record.get("severity_text").and_then(|v| v.as_str()).unwrap_or("INFO");
                let severity_num = record.get("severity_number").and_then(|v| v.as_i64()).unwrap_or(9);

                json!({
                    "resourceLogs": [
                        {
                            "resource": { "attributes": resource_kv },
                            "scopeLogs": [
                                {
                                    "logRecords": [
                                        {
                                            "timeUnixNano": time_nano.to_string(),
                                            "severityNumber": severity_num,
                                            "severityText": severity_text,
                                            "body": body_envelope,
                                            "attributes": attrs_kv
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                })
            }
            "span" => {
                let name = record.get("name").and_then(|v| v.as_str()).unwrap_or("operation");
                let trace_id = record.get("trace_id").and_then(|v| v.as_str()).unwrap_or("");
                let span_id = record.get("span_id").and_then(|v| v.as_str()).unwrap_or("");
                let parent_span_id = record.get("parent_span_id").and_then(|v| v.as_str()).unwrap_or("");
                let duration_ns = record.get("duration_ns").and_then(|v| v.as_i64()).unwrap_or(0);
                let end_nano = time_nano + duration_ns;

                json!({
                    "resourceSpans": [
                        {
                            "resource": { "attributes": resource_kv },
                            "scopeSpans": [
                                {
                                    "spans": [
                                        {
                                            "name": name,
                                            "traceId": trace_id,
                                            "spanId": span_id,
                                            "parentSpanId": parent_span_id,
                                            "startTimeUnixNano": time_nano.to_string(),
                                            "endTimeUnixNano": end_nano.to_string(),
                                            "attributes": attrs_kv,
                                            "status": { "code": 1 }
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                })
            }
            _ => {
                // Metric export
                let name = record.get("name").and_then(|v| v.as_str()).unwrap_or("custom.metric");
                let unit = record.get("unit").and_then(|v| v.as_str()).unwrap_or("1");
                let val_num = record.get("value").and_then(|v| v.as_f64()).unwrap_or(0.0);

                json!({
                    "resourceMetrics": [
                        {
                            "resource": { "attributes": resource_kv },
                            "scopeMetrics": [
                                {
                                    "metrics": [
                                        {
                                            "name": name,
                                            "unit": unit,
                                            "gauge": {
                                                "dataPoints": [
                                                    {
                                                        "timeUnixNano": time_nano.to_string(),
                                                        "asDouble": val_num,
                                                        "attributes": attrs_kv
                                                    }
                                                ]
                                            }
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                })
            }
        }
    }

    async fn dispatch_otlp_http(&self, payload: &Value) -> Result<(), (bool, String)> {
        let mut req = self.client.post(&self.config.endpoint)
            .header("Content-Type", "application/json");

        if let Some(headers) = &self.config.headers {
            for (k, v) in headers {
                req = req.header(k, v);
            }
        }

        let resp = match req.json(payload).send().await {
            Ok(r) => r,
            Err(e) => {
                return Err((false, format!("OTLP network transport error: {}", e)));
            }
        };

        let status = resp.status();
        if status.is_success() {
            Ok(())
        } else {
            let status_code = status.as_u16();
            let body_text = resp.text().await.unwrap_or_else(|_| "".to_string());
            let err_msg = format!("OTLP server HTTP {}: {}", status_code, body_text);

            // Permanent errors: 400 Bad Request, 401 Unauthorized, 403 Forbidden, 422 Unprocessable
            let is_permanent = status_code == 400
                || status_code == 401
                || status_code == 403
                || status_code == 422;

            Err((is_permanent, err_msg))
        }
    }
}
