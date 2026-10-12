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
    /// Generates the Apache IoTDB REST API endpoint URL (`/rest/v1/insertTablet`).
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
            format!("{}/rest/v1/insertTablet", base)
        }
    }

    /// Alias for `endpoint_url` for tablet ingestion.
    pub fn tablet_endpoint_url(&self) -> String {
        self.endpoint_url()
    }

    /// Alias for `endpoint_url` for single record ingestion.
    pub fn record_endpoint_url(&self) -> String {
        self.endpoint_url()
    }

    /// Computes HTTP Basic Authentication header value.
    pub fn basic_auth_header(&self) -> String {
        let auth_str = format!("{}:{}", self.username, self.password);
        let encoded = base64::engine::general_purpose::STANDARD.encode(auth_str);
        format!("Basic {}", encoded)
    }

    fn resolve_timestamp(&self, record: &HashMap<String, Value>) -> Result<i64, String> {
        if let Some(v) = record.get(&self.ts_field_name) {
            match v {
                Value::Number(n) => n
                    .as_i64()
                    .or_else(|| n.as_f64().map(|f| f as i64))
                    .ok_or_else(|| {
                        format!("invalid numeric timestamp in '{}'", self.ts_field_name)
                    }),
                Value::String(s) => s.parse::<i64>().map_err(|_| {
                    format!(
                        "unparseable string timestamp '{}' in '{}'",
                        s, self.ts_field_name
                    )
                }),
                other => Err(format!(
                    "unsupported timestamp type {:?} in '{}'",
                    other, self.ts_field_name
                )),
            }
        } else {
            Ok(SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0))
        }
    }

    /// Formats an event record into an Apache IoTDB REST tablet payload (`/rest/v1/insertTablet`).
    pub fn build_insert_payload(&self, record: &HashMap<String, Value>) -> Result<Value, String> {
        if record.is_empty() {
            return Err("record data is empty".to_string());
        }

        let record_val =
            Value::Object(record.iter().map(|(k, v)| (k.clone(), v.clone())).collect());

        // 1. Resolve device name
        let device = if self.device.contains("{{") {
            apply_data_template_value(&self.device, &record_val)
        } else {
            self.device.clone()
        };
        if device.trim().is_empty() {
            return Err("device path resolved to empty string".to_string());
        }

        // 2. Resolve timestamp strictly
        let ts = self.resolve_timestamp(record)?;

        // 3. Resolve measurements, dataTypes, and column-major values
        let mut measurements: Vec<String> = Vec::new();
        let mut data_types: Vec<String> = Vec::new();
        let mut values: Vec<Vec<Value>> = Vec::new();

        if let Some(ref m_list) = self.measurements {
            for m in m_list {
                if m == &self.ts_field_name {
                    continue;
                }
                if let Some(val) = record.get(m) {
                    if val.is_null() {
                        continue;
                    }
                    measurements.push(m.clone());
                    data_types.push(infer_iotdb_data_type(val).to_string());
                    values.push(vec![val.clone()]);
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
                    if val.is_null() {
                        continue;
                    }
                    measurements.push(k.clone());
                    data_types.push(infer_iotdb_data_type(val).to_string());
                    values.push(vec![val.clone()]);
                }
            }
        }

        if measurements.is_empty() {
            return Err("no measurement values found to insert".to_string());
        }

        Ok(serde_json::json!({
            "deviceId": device,
            "isAligned": false,
            "timestamps": [ts],
            "measurements": measurements,
            "dataTypes": data_types,
            "values": values
        }))
    }

    /// Formats a batch of stream records into properly grouped IoTDB tablet payloads (`/rest/v1/insertTablet`).
    /// Records sharing the same device are batched together into column-major tablet structures.
    pub fn build_batch_insert_payloads(
        &self,
        records: &[HashMap<String, Value>],
    ) -> Result<Vec<Value>, String> {
        if records.is_empty() {
            return Ok(Vec::new());
        }

        struct DeviceBatch {
            device: String,
            timestamps: Vec<i64>,
            rows: Vec<HashMap<String, Value>>,
        }

        let mut device_batches: Vec<DeviceBatch> = Vec::new();

        for rec in records {
            let record_val =
                Value::Object(rec.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
            let dev = if self.device.contains("{{") {
                apply_data_template_value(&self.device, &record_val)
            } else {
                self.device.clone()
            };
            if dev.trim().is_empty() {
                continue;
            }

            let ts = self.resolve_timestamp(rec)?;

            let mut found = false;
            for b in &mut device_batches {
                if b.device == dev {
                    b.timestamps.push(ts);
                    b.rows.push(rec.clone());
                    found = true;
                    break;
                }
            }
            if !found {
                device_batches.push(DeviceBatch {
                    device: dev,
                    timestamps: vec![ts],
                    rows: vec![rec.clone()],
                });
            }
        }

        let mut payloads = Vec::new();

        for batch in device_batches {
            let row_count = batch.rows.len();
            if row_count == 0 {
                continue;
            }

            // 1. Determine candidate measurements
            let candidate_measurements: Vec<String> = if let Some(ref m_list) = self.measurements {
                m_list
                    .iter()
                    .filter(|m| *m != &self.ts_field_name)
                    .cloned()
                    .collect()
            } else {
                let mut set = std::collections::BTreeSet::new();
                for r in &batch.rows {
                    for k in r.keys() {
                        if k != &self.ts_field_name && !k.starts_with("__") {
                            set.insert(k.clone());
                        }
                    }
                }
                set.into_iter().collect()
            };

            // 2. For each measurement, find data type from first non-null value across rows
            let mut measurements = Vec::new();
            let mut data_types = Vec::new();
            let mut values = Vec::new();

            for m in candidate_measurements {
                let first_non_null = batch
                    .rows
                    .iter()
                    .find_map(|r| r.get(&m).filter(|v| !v.is_null()));

                let Some(val) = first_non_null else {
                    // All rows are null or missing for this measurement; skip column
                    continue;
                };

                let dt = infer_iotdb_data_type(val);
                let col: Vec<Value> = batch
                    .rows
                    .iter()
                    .map(|r| match r.get(&m) {
                        Some(v) if !v.is_null() => {
                            if infer_iotdb_data_type(v) == dt {
                                v.clone()
                            } else {
                                Value::Null
                            }
                        }
                        _ => Value::Null,
                    })
                    .collect();

                measurements.push(m);
                data_types.push(dt.to_string());
                values.push(col);
            }

            if measurements.is_empty() {
                continue;
            }

            payloads.push(serde_json::json!({
                "deviceId": batch.device,
                "isAligned": false,
                "timestamps": batch.timestamps,
                "measurements": measurements,
                "dataTypes": data_types,
                "values": values
            }));
        }

        Ok(payloads)
    }
}

/// Maps a JSON value to an Apache IoTDB REST v1 data type name.
/// Numbers are mapped to DOUBLE to avoid series type conflicts.
pub fn infer_iotdb_data_type(val: &Value) -> &'static str {
    match val {
        Value::Bool(_) => "BOOLEAN",
        Value::Number(_) => "DOUBLE",
        Value::String(_) => "TEXT",
        _ => "TEXT",
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
        assert_eq!(payload["deviceId"], "root.station.pump_01");
        assert_eq!(payload["isAligned"], false);
        assert_eq!(payload["timestamps"], json!([1690000000000i64]));
        assert_eq!(payload["measurements"], json!(["pressure", "temp"]));
        assert_eq!(payload["dataTypes"], json!(["DOUBLE", "DOUBLE"]));
        assert_eq!(payload["values"], json!([[101.3], [42.5]]));
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

        assert_eq!(
            cfg.endpoint_url(),
            "http://iotdb-cluster:18080/rest/v1/insertTablet"
        );
        assert_eq!(
            cfg.tablet_endpoint_url(),
            "http://iotdb-cluster:18080/rest/v1/insertTablet"
        );
        assert_eq!(
            cfg.record_endpoint_url(),
            "http://iotdb-cluster:18080/rest/v1/insertTablet"
        );
        assert_eq!(cfg.basic_auth_header(), "Basic YWRtaW46c2VjcmV0");
    }

    #[test]
    fn test_iotdb_build_insert_payload_auto_measurements() {
        let mut record = HashMap::new();
        record.insert("temp".to_string(), json!(28.4));
        record.insert("active".to_string(), json!(true));
        record.insert("tag_str".to_string(), json!("ok"));
        record.insert("ts".to_string(), json!(1710000000000i64));

        let cfg = IotdbConfig {
            addr: "127.0.0.1:18080".to_string(),
            username: "root".to_string(),
            password: "root".to_string(),
            device: "root.ln.wf01.wt01".to_string(),
            measurements: None,
            ts_field_name: "ts".to_string(),
        };

        let payload = cfg.build_insert_payload(&record).unwrap();
        assert_eq!(payload["deviceId"], "root.ln.wf01.wt01");
        assert_eq!(payload["isAligned"], false);
        assert_eq!(payload["timestamps"], json!([1710000000000i64]));
        let m_arr = payload["measurements"].as_array().unwrap();
        assert_eq!(m_arr.len(), 3);
        assert!(m_arr.contains(&json!("active")));
        assert!(m_arr.contains(&json!("tag_str")));
        assert!(m_arr.contains(&json!("temp")));
        let dt_arr = payload["dataTypes"].as_array().unwrap();
        assert_eq!(dt_arr.len(), 3);
        assert!(dt_arr.contains(&json!("BOOLEAN")));
        assert!(dt_arr.contains(&json!("TEXT")));
        assert!(dt_arr.contains(&json!("DOUBLE")));
        let vals = payload["values"].as_array().unwrap();
        assert_eq!(vals.len(), 3);
        for col in vals {
            assert_eq!(col.as_array().unwrap().len(), 1);
        }
    }

    #[test]
    fn test_iotdb_build_batch_tablet_payload() {
        let cfg = IotdbConfig {
            addr: "127.0.0.1:18080".to_string(),
            username: "root".to_string(),
            password: "root".to_string(),
            device: "root.sg.dev".to_string(),
            measurements: Some(vec!["speed".to_string(), "status".to_string()]),
            ts_field_name: "ts".to_string(),
        };

        let mut r1 = HashMap::new();
        r1.insert("speed".to_string(), json!(120));
        r1.insert("status".to_string(), json!("running"));
        r1.insert("ts".to_string(), json!(1000i64));

        let mut r2 = HashMap::new();
        r2.insert("speed".to_string(), json!(135));
        r2.insert("status".to_string(), json!("idle"));
        r2.insert("ts".to_string(), json!(2000i64));

        let batches = cfg.build_batch_insert_payloads(&[r1, r2]).unwrap();
        assert_eq!(batches.len(), 1);
        let b = &batches[0];
        assert_eq!(b["deviceId"], "root.sg.dev");
        assert_eq!(b["isAligned"], false);
        assert_eq!(b["measurements"], json!(["speed", "status"]));
        assert_eq!(b["dataTypes"], json!(["DOUBLE", "TEXT"]));
        assert_eq!(b["timestamps"], json!([1000, 2000]));
        assert_eq!(b["values"], json!([[120, 135], ["running", "idle"]]));
    }

    #[test]
    fn test_iotdb_build_batch_tablet_payload_rejects_invalid_timestamp() {
        let cfg = IotdbConfig {
            addr: "127.0.0.1:18080".to_string(),
            username: "root".to_string(),
            password: "root".to_string(),
            device: "root.sg.dev".to_string(),
            measurements: Some(vec!["speed".to_string()]),
            ts_field_name: "ts".to_string(),
        };

        let mut r1 = HashMap::new();
        r1.insert("speed".to_string(), json!(120));
        r1.insert("ts".to_string(), json!("not_a_valid_number"));

        let res = cfg.build_batch_insert_payloads(&[r1]);
        assert!(res.is_err());
        assert!(res.unwrap_err().contains("unparseable string timestamp"));
    }

    #[test]
    fn test_iotdb_build_batch_tablet_payload_handles_mixed_types() {
        let cfg = IotdbConfig {
            addr: "127.0.0.1:18080".to_string(),
            username: "root".to_string(),
            password: "root".to_string(),
            device: "root.sg.dev".to_string(),
            measurements: Some(vec!["speed".to_string()]),
            ts_field_name: "ts".to_string(),
        };

        let mut r1 = HashMap::new();
        r1.insert("speed".to_string(), json!(120));
        r1.insert("ts".to_string(), json!(1000i64));

        let mut r2 = HashMap::new();
        // r2 has a string in a DOUBLE measurement column
        r2.insert("speed".to_string(), json!("offline"));
        r2.insert("ts".to_string(), json!(2000i64));

        let batches = cfg.build_batch_insert_payloads(&[r1, r2]).unwrap();
        assert_eq!(batches.len(), 1);
        let b = &batches[0];
        assert_eq!(b["dataTypes"], json!(["DOUBLE"]));
        // Mismatched type "offline" is nulled out so tablet is not rejected
        assert_eq!(b["values"], json!([[120, null]]));
    }
}
