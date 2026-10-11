use crate::data_template::apply_data_template_value;
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Apache IoTDB sink configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IotdbConfig {
    /// IoTDB REST server address (e.g. "http://127.0.0.1:18080" or "127.0.0.1:18080").
    #[serde(default = "default_iotdb_addr")]
    pub addr: String,
    /// Username for HTTP Basic authentication.
    #[serde(default = "default_iotdb_user")]
    pub username: String,
    /// Password for HTTP Basic authentication.
    #[serde(default = "default_iotdb_password")]
    pub password: String,
    /// Target device path template (e.g. "root.factory.{{.deviceId}}").
    pub device: String,
    /// Projected measurement column names. If omitted, all non-timestamp fields are written.
    #[serde(default)]
    pub measurements: Option<Vec<String>>,
    /// Timestamp property name in input record (default "ts").
    #[serde(default = "default_ts_field_name")]
    pub ts_field_name: String,
}

fn default_iotdb_addr() -> String {
    "http://127.0.0.1:18080".to_string()
}
fn default_iotdb_user() -> String {
    "root".to_string()
}
fn default_iotdb_password() -> String {
    "root".to_string()
}
fn default_ts_field_name() -> String {
    "ts".to_string()
}

impl IotdbConfig {
    /// Generates the Apache IoTDB REST API endpoint URL.
    pub fn endpoint_url(&self) -> String {
        let trimmed = self.addr.trim();
        let base = if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
            trimmed.to_string()
        } else {
            format!("http://{}", trimmed)
        };
        let base = base.trim_end_matches('/');
        if base.contains("/rest/") || base.contains("/api/") {
            base.to_string()
        } else {
            format!("{}/rest/v1/insertRecord", base)
        }
    }

    /// Computes HTTP Basic Authentication header value.
    pub fn basic_auth_header(&self) -> String {
        let auth_str = format!("{}:{}", self.username, self.password);
        let encoded = base64::engine::general_purpose::STANDARD.encode(auth_str);
        format!("Basic {}", encoded)
    }

    /// Formats an event record into an Apache IoTDB REST record insertion payload.
    pub fn build_insert_payload(&self, record: &HashMap<String, Value>) -> Result<Value, String> {
        if record.is_empty() {
            return Err("record data is empty".to_string());
        }

        let record_val = Value::Object(record.iter().map(|(k, v)| (k.clone(), v.clone())).collect());

        // 1. Resolve device name
        let device = if self.device.contains("{{") {
            apply_data_template_value(&self.device, &record_val)
        } else {
            self.device.clone()
        };
        if device.trim().is_empty() {
            return Err("device path resolved to empty string".to_string());
        }

        // 2. Resolve timestamp
        let ts = if let Some(v) = record.get(&self.ts_field_name) {
            match v {
                Value::Number(n) => n.as_i64().unwrap_or_else(|| n.as_f64().map(|f| f as i64).unwrap_or(0)),
                Value::String(s) => s.parse::<i64>().unwrap_or(0),
                _ => 0,
            }
        } else {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0)
        };

        // 3. Resolve measurements and values
        let mut measurements: Vec<String> = Vec::new();
        let mut values: Vec<Value> = Vec::new();

        if let Some(ref m_list) = self.measurements {
            for m in m_list {
                if m == &self.ts_field_name {
                    continue;
                }
                if let Some(val) = record.get(m) {
                    measurements.push(m.clone());
                    values.push(val.clone());
                } else {
                    return Err(format!("measurement '{}' not found in record", m));
                }
            }
        } else {
            let mut keys: Vec<&String> = record.keys().collect();
            keys.sort();
            for k in keys {
                if k == &self.ts_field_name || k.starts_with("__") {
                    continue;
                }
                if let Some(val) = record.get(k) {
                    measurements.push(k.clone());
                    values.push(val.clone());
                }
            }
        }

        if measurements.is_empty() {
            return Err("no measurement values found to insert".to_string());
        }

        Ok(serde_json::json!({
            "device": device,
            "timestamps": [ts],
            "measurements": measurements,
            "values": [values]
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_iotdb_build_insert_payload() {
        let mut record = HashMap::new();
        record.insert("deviceId".to_string(), json!("pump_01"));
        record.insert("pressure".to_string(), json!(101.3));
        record.insert("temp".to_string(), json!(42.5));
        record.insert("ts".to_string(), json!(1690000000000i64));

        let cfg = IotdbConfig {
            addr: "127.0.0.1:18080".to_string(),
            username: "root".to_string(),
            password: "root".to_string(),
            device: "root.station.{{.deviceId}}".to_string(),
            measurements: Some(vec!["pressure".to_string(), "temp".to_string()]),
            ts_field_name: "ts".to_string(),
        };

        let payload = cfg.build_insert_payload(&record).unwrap();
        assert_eq!(payload["device"], "root.station.pump_01");
        assert_eq!(payload["timestamps"], json!([1690000000000i64]));
        assert_eq!(payload["measurements"], json!(["pressure", "temp"]));
        assert_eq!(payload["values"], json!([[101.3, 42.5]]));
    }

    #[test]
    fn test_iotdb_endpoint_and_auth() {
        let cfg = IotdbConfig {
            addr: "http://iotdb-cluster:18080".to_string(),
            username: "admin".to_string(),
            password: "secret".to_string(),
            device: "root.default".to_string(),
            measurements: None,
            ts_field_name: "ts".to_string(),
        };

        assert_eq!(cfg.endpoint_url(), "http://iotdb-cluster:18080/rest/v1/insertRecord");
        assert_eq!(cfg.basic_auth_header(), "Basic YWRtaW46c2VjcmV0");
    }
}
