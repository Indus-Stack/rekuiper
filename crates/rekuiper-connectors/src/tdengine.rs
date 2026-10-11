use crate::data_template::apply_data_template_value;
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// TDengine 3.x sink configuration (eKuiper `tdengine3` action format).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TdengineConfig {
    /// TDengine REST server hostname or IP.
    #[serde(default = "default_tdengine_host")]
    pub host: String,
    /// TDengine REST service port (default 6041).
    #[serde(default = "default_tdengine_port")]
    pub port: u16,
    /// Authentication username (default "root").
    #[serde(default = "default_tdengine_user")]
    pub user: String,
    /// Authentication password (default "taosdata").
    #[serde(default = "default_tdengine_password")]
    pub password: String,
    /// Target database name.
    pub database: String,
    /// Target table name (supports data template e.g. `"d_{{.device}}"`).
    pub table: String,
    /// Optional super table name (for automatic sub-table creation).
    #[serde(default)]
    pub s_table: Option<String>,
    /// Tag field property names (required when `s_table` is specified).
    #[serde(default)]
    pub tag_fields: Option<Vec<String>>,
    /// Projected data field property names to write. If omitted, all non-tag fields are written.
    #[serde(default)]
    pub fields: Option<Vec<String>>,
    /// Timestamp property name (default "ts").
    #[serde(default = "default_ts_field_name")]
    pub ts_field_name: String,
    /// Whether to supply the record's timestamp (`true`) or use database server clock `now` (`false`).
    #[serde(default = "default_true")]
    pub provide_ts: bool,
}

fn default_tdengine_host() -> String {
    "localhost".to_string()
}
fn default_tdengine_port() -> u16 {
    6041
}
fn default_tdengine_user() -> String {
    "root".to_string()
}
fn default_tdengine_password() -> String {
    "taosdata".to_string()
}
fn default_ts_field_name() -> String {
    "ts".to_string()
}
fn default_true() -> bool {
    true
}

impl TdengineConfig {
    /// Generates the TDengine RESTful connector endpoint URL.
    pub fn endpoint_url(&self) -> String {
        let host = self.host.trim_start_matches("http://").trim_start_matches("https://");
        format!("http://{}:{}/rest/sql/{}", host, self.port, self.database)
    }

    /// Computes HTTP Basic Authentication header value.
    pub fn basic_auth_header(&self) -> String {
        let auth_str = format!("{}:{}", self.user, self.password);
        let encoded = base64::engine::general_purpose::STANDARD.encode(auth_str);
        format!("Basic {}", encoded)
    }

    /// Formats a stream record into a TDengine INSERT SQL statement.
    pub fn build_insert_sql(&self, record: &HashMap<String, Value>) -> Result<String, String> {
        if record.is_empty() {
            return Err("record data is empty".to_string());
        }

        let record_val = Value::Object(record.iter().map(|(k, v)| (k.clone(), v.clone())).collect());

        // 1. Resolve table name
        let table = if self.table.contains("{{") {
            apply_data_template_value(&self.table, &record_val)
        } else {
            self.table.clone()
        };
        if table.trim().is_empty() {
            return Err("table name resolved to empty string".to_string());
        }

        // 2. Resolve optional super table
        let s_table = self.s_table.as_ref().map(|st| {
            if st.contains("{{") {
                apply_data_template_value(st, &record_val)
            } else {
                st.clone()
            }
        });

        // 3. Resolve tag values
        let mut tag_vals: Vec<String> = Vec::new();
        if let Some(ref tags) = self.tag_fields {
            for tag_name in tags {
                if let Some(val) = record.get(tag_name) {
                    tag_vals.push(format_sql_value(val));
                } else {
                    return Err(format!("tag field '{}' not found in record", tag_name));
                }
            }
        }

        // 4. Resolve columns (keys and values)
        let mut keys: Vec<String> = Vec::new();
        let mut vals: Vec<String> = Vec::new();

        // Timestamp column
        keys.push(self.ts_field_name.clone());
        if self.provide_ts {
            if let Some(ts_val) = record.get(&self.ts_field_name) {
                vals.push(format_sql_value(ts_val));
            } else {
                return Err(format!("timestamp field '{}' not found", self.ts_field_name));
            }
        } else {
            vals.push("now".to_string());
        }

        // Other columns
        let is_tag = |key: &str| -> bool {
            self.tag_fields.as_ref().is_some_and(|tf| tf.iter().any(|t| t == key))
        };

        if let Some(ref field_list) = self.fields {
            for f in field_list {
                if f == &self.ts_field_name || is_tag(f) {
                    continue;
                }
                if let Some(val) = record.get(f) {
                    keys.push(f.clone());
                    vals.push(format_sql_value(val));
                } else {
                    return Err(format!("field '{}' not found in record", f));
                }
            }
        } else {
            let mut sorted_keys: Vec<&String> = record.keys().collect();
            sorted_keys.sort();
            for k in sorted_keys {
                if k == &self.ts_field_name || is_tag(k) || k.starts_with("__") {
                    continue;
                }
                if let Some(val) = record.get(k) {
                    keys.push(k.clone());
                    vals.push(format_sql_value(val));
                }
            }
        }

        // 5. Construct statement
        let mut sql = format!("INSERT INTO {} ", table);
        if let Some(st) = s_table {
            sql.push_str(&format!("USING {} ", st));
            if !tag_vals.is_empty() {
                sql.push_str(&format!("TAGS ({}) ", tag_vals.join(", ")));
            }
        }
        sql.push_str(&format!("({}) VALUES ({})", keys.join(", "), vals.join(", ")));

        Ok(sql)
    }
}

fn format_sql_value(val: &Value) -> String {
    match val {
        Value::String(s) => format!("'{}'", s.replace('\'', "\\'")),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => if *b { "true".to_string() } else { "false".to_string() },
        Value::Null => "NULL".to_string(),
        other => format!("'{}'", other.to_string().replace('\'', "\\'")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_tdengine_build_insert_sql() {
        let mut record = HashMap::new();
        record.insert("deviceId".to_string(), json!("meter_101"));
        record.insert("temp".to_string(), json!(25.4));
        record.insert("humidity".to_string(), json!(60));
        record.insert("ts".to_string(), json!(1700000000000i64));

        let cfg = TdengineConfig {
            host: "localhost".to_string(),
            port: 6041,
            user: "root".to_string(),
            password: "taosdata".to_string(),
            database: "power".to_string(),
            table: "m_{{.deviceId}}".to_string(),
            s_table: Some("meters".to_string()),
            tag_fields: Some(vec!["deviceId".to_string()]),
            fields: Some(vec!["temp".to_string(), "humidity".to_string()]),
            ts_field_name: "ts".to_string(),
            provide_ts: true,
        };

        let sql = cfg.build_insert_sql(&record).unwrap();
        assert_eq!(
            sql,
            "INSERT INTO m_meter_101 USING meters TAGS ('meter_101') (ts, temp, humidity) VALUES (1700000000000, 25.4, 60)"
        );
    }

    #[test]
    fn test_tdengine_endpoint_and_auth() {
        let cfg = TdengineConfig {
            host: "192.168.1.50".to_string(),
            port: 6041,
            user: "root".to_string(),
            password: "taosdata".to_string(),
            database: "factory".to_string(),
            table: "sensor".to_string(),
            s_table: None,
            tag_fields: None,
            fields: None,
            ts_field_name: "ts".to_string(),
            provide_ts: false,
        };

        assert_eq!(cfg.endpoint_url(), "http://192.168.1.50:6041/rest/sql/factory");
        assert_eq!(cfg.basic_auth_header(), "Basic cm9vdDp0YW9zZGF0YQ==");
    }
}
