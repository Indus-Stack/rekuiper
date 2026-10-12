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
        let trimmed = self.host.trim();
        let (scheme, raw_host) = if trimmed.starts_with("https://") {
            ("https", trimmed.trim_start_matches("https://"))
        } else if trimmed.starts_with("http://") {
            ("http", trimmed.trim_start_matches("http://"))
        } else {
            ("http", trimmed)
        };
        let host = raw_host.split(':').next().unwrap_or(raw_host);
        let encoded_db = crate::influx::url_encode(&self.database);
        format!(
            "{}://{}:{}/rest/sql/{}",
            scheme, host, self.port, encoded_db
        )
    }

    /// Computes HTTP Basic Authentication header value.
    pub fn basic_auth_header(&self) -> String {
        let auth_str = format!("{}:{}", self.user, self.password);
        let encoded = base64::engine::general_purpose::STANDARD.encode(auth_str);
        format!("Basic {}", encoded)
    }

    /// Formats a single stream record into a TDengine table prefix and value tuple (`(prefix, "(v1, v2)")`).
    pub fn build_insert_parts(
        &self,
        record: &HashMap<String, Value>,
    ) -> Result<(String, String), String> {
        if record.is_empty() {
            return Err("record data is empty".to_string());
        }

        let record_val =
            Value::Object(record.iter().map(|(k, v)| (k.clone(), v.clone())).collect());

        // 1. Resolve table name and validate identifier
        let table = if self.table.contains("{{") {
            apply_data_template_value(&self.table, &record_val)
        } else {
            self.table.clone()
        };
        let table_trimmed = table.trim();
        if table_trimmed.is_empty() {
            return Err("table name resolved to empty string".to_string());
        }
        if !crate::sql::is_valid_sql_identifier(table_trimmed) {
            return Err(format!("invalid table identifier '{}'", table_trimmed));
        }

        // 2. Resolve optional super table and validate identifier
        let s_table = if let Some(ref st) = self.s_table {
            let rendered = if st.contains("{{") {
                apply_data_template_value(st, &record_val)
            } else {
                st.clone()
            };
            let st_trimmed = rendered.trim().to_string();
            if !crate::sql::is_valid_sql_identifier(&st_trimmed) {
                return Err(format!("invalid super table identifier '{}'", st_trimmed));
            }
            Some(st_trimmed)
        } else {
            None
        };

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
        if !crate::sql::is_valid_sql_identifier(&self.ts_field_name) {
            return Err(format!(
                "invalid timestamp field identifier '{}'",
                self.ts_field_name
            ));
        }
        keys.push(self.ts_field_name.clone());
        if self.provide_ts {
            if let Some(ts_val) = record.get(&self.ts_field_name) {
                vals.push(format_sql_value(ts_val));
            } else {
                return Err(format!(
                    "timestamp field '{}' not found",
                    self.ts_field_name
                ));
            }
        } else {
            vals.push("now".to_string());
        }

        // Other columns
        let is_tag = |key: &str| -> bool {
            self.tag_fields
                .as_ref()
                .is_some_and(|tf| tf.iter().any(|t| t == key))
        };

        if let Some(ref field_list) = self.fields {
            for f in field_list {
                if f == &self.ts_field_name || is_tag(f) {
                    continue;
                }
                if !crate::sql::is_valid_sql_identifier(f) {
                    return Err(format!("invalid column identifier '{}'", f));
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
                if !crate::sql::is_valid_sql_identifier(k) {
                    return Err(format!("invalid column identifier '{}'", k));
                }
                if let Some(val) = record.get(k) {
                    keys.push(k.clone());
                    vals.push(format_sql_value(val));
                }
            }
        }

        // 5. Construct prefix and value tuple
        let mut prefix = format!("{} ", table_trimmed);
        if let Some(st) = s_table {
            prefix.push_str(&format!("USING {} ", st));
            if !tag_vals.is_empty() {
                prefix.push_str(&format!("TAGS ({}) ", tag_vals.join(", ")));
            }
        }
        prefix.push_str(&format!("({})", keys.join(", ")));

        let values_tuple = format!("({})", vals.join(", "));
        Ok((prefix, values_tuple))
    }

    /// Formats a single stream record into a TDengine INSERT clause (`table [USING ...] (cols) VALUES (vals)`).
    pub fn build_insert_clause(&self, record: &HashMap<String, Value>) -> Result<String, String> {
        let (prefix, values_tuple) = self.build_insert_parts(record)?;
        Ok(format!("{} VALUES {}", prefix, values_tuple))
    }

    /// Formats a single stream record into a full TDengine INSERT SQL statement.
    pub fn build_insert_sql(&self, record: &HashMap<String, Value>) -> Result<String, String> {
        let clause = self.build_insert_clause(record)?;
        Ok(format!("INSERT INTO {}", clause))
    }

    /// Formats a batch of stream records into a single multi-row/multi-table TDengine INSERT statement.
    /// Consecutive rows targeting the same table schema are grouped into multi-row `VALUES (...) (...)` clauses.
    pub fn build_batch_insert_sql(
        &self,
        records: &[HashMap<String, Value>],
    ) -> Result<String, String> {
        if records.is_empty() {
            return Ok(String::new());
        }

        let mut groups: Vec<(String, Vec<String>)> = Vec::new();
        for rec in records {
            let (prefix, values_tuple) = match self.build_insert_parts(rec) {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!("TDengine batch record invalid: {}; skipping row", e);
                    continue;
                }
            };
            if let Some(last) = groups.last_mut() {
                if last.0 == prefix {
                    last.1.push(values_tuple);
                    continue;
                }
            }
            groups.push((prefix, vec![values_tuple]));
        }

        if groups.is_empty() {
            return Ok(String::new());
        }

        let clauses: Vec<String> = groups
            .into_iter()
            .map(|(prefix, tuples)| format!("{} VALUES {}", prefix, tuples.join(" ")))
            .collect();

        Ok(format!("INSERT INTO {}", clauses.join(" ")))
    }
}

fn format_sql_value(val: &Value) -> String {
    match val {
        Value::String(s) => format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'")),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => {
            if *b {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        Value::Null => "NULL".to_string(),
        other => format!(
            "'{}'",
            other.to_string().replace('\\', "\\\\").replace('\'', "\\'")
        ),
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

        assert_eq!(
            cfg.endpoint_url(),
            "http://192.168.1.50:6041/rest/sql/factory"
        );
        assert_eq!(cfg.basic_auth_header(), "Basic cm9vdDp0YW9zZGF0YQ==");
    }

    #[test]
    fn test_tdengine_build_insert_sql_plain_table() {
        let mut record = HashMap::new();
        record.insert("name".to_string(), json!("sensor's \"quote\""));
        record.insert("val".to_string(), json!(42));
        record.insert("nullable".to_string(), Value::Null);

        let cfg = TdengineConfig {
            host: "localhost".to_string(),
            port: 6041,
            user: "root".to_string(),
            password: "taosdata".to_string(),
            database: "test_db".to_string(),
            table: "sensors".to_string(),
            s_table: None,
            tag_fields: None,
            fields: None,
            ts_field_name: "ts".to_string(),
            provide_ts: false,
        };

        let sql = cfg.build_insert_sql(&record).unwrap();
        assert!(sql.starts_with("INSERT INTO sensors "));
        assert!(sql.contains("name"));
        assert!(sql.contains("nullable"));
        assert!(sql.contains("val"));
        assert!(sql.contains("'sensor\\'s \"quote\"'"));
        assert!(sql.contains("NULL"));
        assert!(sql.contains("42"));
    }

    #[test]
    fn test_tdengine_sql_injection_rejected() {
        let mut record = HashMap::new();
        record.insert("deviceId".to_string(), json!("meter; DROP TABLE meters;--"));
        record.insert("val".to_string(), json!(1));

        let cfg = TdengineConfig {
            host: "localhost".to_string(),
            port: 6041,
            user: "root".to_string(),
            password: "taosdata".to_string(),
            database: "test_db".to_string(),
            table: "m_{{.deviceId}}".to_string(),
            s_table: None,
            tag_fields: None,
            fields: None,
            ts_field_name: "ts".to_string(),
            provide_ts: false,
        };

        let err = cfg.build_insert_sql(&record).unwrap_err();
        assert!(err.contains("invalid table identifier"));
    }

    #[test]
    fn test_tdengine_trailing_backslash_escaped() {
        let mut record = HashMap::new();
        record.insert("info".to_string(), json!("folder\\"));

        let cfg = TdengineConfig {
            host: "localhost".to_string(),
            port: 6041,
            user: "root".to_string(),
            password: "taosdata".to_string(),
            database: "test_db".to_string(),
            table: "logs".to_string(),
            s_table: None,
            tag_fields: None,
            fields: None,
            ts_field_name: "ts".to_string(),
            provide_ts: false,
        };

        let sql = cfg.build_insert_sql(&record).unwrap();
        assert!(sql.contains("'folder\\\\'"));
    }

    #[test]
    fn test_tdengine_https_endpoint_and_batch() {
        let cfg = TdengineConfig {
            host: "https://taos.cloud.net:6041".to_string(),
            port: 6041,
            user: "root".to_string(),
            password: "taosdata".to_string(),
            database: "factory db".to_string(),
            table: "t1".to_string(),
            s_table: None,
            tag_fields: None,
            fields: None,
            ts_field_name: "ts".to_string(),
            provide_ts: false,
        };

        assert_eq!(
            cfg.endpoint_url(),
            "https://taos.cloud.net:6041/rest/sql/factory%20db"
        );

        let mut r1 = HashMap::new();
        r1.insert("v".to_string(), json!(10));
        let mut r2 = HashMap::new();
        r2.insert("v".to_string(), json!(20));

        let batch_sql = cfg.build_batch_insert_sql(&[r1, r2]).unwrap();
        assert_eq!(
            batch_sql,
            "INSERT INTO t1 (ts, v) VALUES (now, 10) (now, 20)"
        );

        // Multi-table batch
        let mut cfg_multi = cfg.clone();
        cfg_multi.table = "m_{{.dev}}".to_string();
        cfg_multi.fields = Some(vec!["v".to_string()]);
        let mut r_dev1 = HashMap::new();
        r_dev1.insert("dev".to_string(), json!("1"));
        r_dev1.insert("v".to_string(), json!(100));
        let mut r_dev2 = HashMap::new();
        r_dev2.insert("dev".to_string(), json!("2"));
        r_dev2.insert("v".to_string(), json!(200));

        let multi_sql = cfg_multi.build_batch_insert_sql(&[r_dev1, r_dev2]).unwrap();
        assert_eq!(
            multi_sql,
            "INSERT INTO m_1 (ts, v) VALUES (now, 100) m_2 (ts, v) VALUES (now, 200)"
        );
    }
}
