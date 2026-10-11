use crate::data_template::apply_data_template_value;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// InfluxDB v1 sink configuration (eKuiper `influx` action format).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InfluxConfig {
    /// Server address, e.g. `"http://127.0.0.1:8086"`.
    #[serde(default = "default_influx_addr")]
    pub addr: String,
    /// InfluxDB v1 database name.
    #[serde(default = "default_influx_database")]
    pub database: String,
    /// Measurement table name (supports dynamic templates e.g. `"{{.device}}"`).
    #[serde(default = "default_influx_measurement")]
    pub measurement: String,
    /// Optional HTTP Basic Auth username.
    #[serde(default)]
    pub username: Option<String>,
    /// Optional HTTP Basic Auth password.
    #[serde(default)]
    pub password: Option<String>,
    /// Timestamp precision: `"ms"` (default), `"s"`, `"us"`, or `"ns"`.
    #[serde(default = "default_precision")]
    pub precision: String,
    /// Optional tags map (values can be templates, e.g. `{"host": "{{.host}}", "loc": "edge"}`).
    #[serde(default)]
    pub tags: Option<HashMap<String, String>>,
    /// Optional list of field names to include. If omitted or empty, all non-tag fields are included.
    #[serde(default)]
    pub fields: Option<Vec<String>>,
    /// Optional payload property supplying the record timestamp.
    #[serde(default)]
    pub ts_field_name: Option<String>,
}

/// InfluxDB v2 sink configuration (eKuiper `influx2` action format).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Influx2Config {
    /// Server address, e.g. `"http://127.0.0.1:8086"`.
    #[serde(default = "default_influx_addr")]
    pub addr: String,
    /// InfluxDB v2 organization name.
    #[serde(default = "default_influx_org")]
    pub org: String,
    /// InfluxDB v2 bucket name.
    #[serde(default = "default_influx_bucket")]
    pub bucket: String,
    /// InfluxDB v2 authentication token.
    #[serde(default)]
    pub token: Option<String>,
    /// Measurement table name (supports dynamic templates e.g. `"{{.device}}"`).
    #[serde(default = "default_influx_measurement")]
    pub measurement: String,
    /// Timestamp precision: `"ms"` (default), `"s"`, `"us"`, or `"ns"`.
    #[serde(default = "default_precision")]
    pub precision: String,
    /// Optional tags map.
    #[serde(default)]
    pub tags: Option<HashMap<String, String>>,
    /// Optional list of field names to include.
    #[serde(default)]
    pub fields: Option<Vec<String>>,
    /// Optional payload property supplying the record timestamp.
    #[serde(default)]
    pub ts_field_name: Option<String>,
}

fn default_influx_addr() -> String {
    "http://127.0.0.1:8086".to_string()
}

fn default_influx_database() -> String {
    "kuiper".to_string()
}

fn default_influx_org() -> String {
    "kuiper".to_string()
}

fn default_influx_bucket() -> String {
    "kuiper".to_string()
}

fn default_influx_measurement() -> String {
    "kuiper".to_string()
}

fn default_precision() -> String {
    "ms".to_string()
}

pub fn url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => {
                out.push_str(&format!("%{:02X}", b));
            }
        }
    }
    out
}

impl InfluxConfig {
    /// Generates the HTTP endpoint URL for writing data to InfluxDB v1.
    pub fn write_url(&self) -> String {
        let base = self.addr.trim_end_matches('/');
        let mut url = format!("{}/write?db={}", base, url_encode(&self.database));
        if !self.precision.is_empty() {
            url.push_str(&format!("&precision={}", self.precision));
        }
        if let Some(ref u) = self.username {
            url.push_str(&format!("&u={}", url_encode(u)));
        }
        if let Some(ref p) = self.password {
            url.push_str(&format!("&p={}", url_encode(p)));
        }
        url
    }
}

impl Influx2Config {
    /// Generates the HTTP endpoint URL for writing data to InfluxDB v2.
    pub fn write_url(&self) -> String {
        let base = self.addr.trim_end_matches('/');
        let mut url = format!(
            "{}/api/v2/write?org={}&bucket={}",
            base,
            url_encode(&self.org),
            url_encode(&self.bucket)
        );
        if !self.precision.is_empty() {
            url.push_str(&format!("&precision={}", self.precision));
        }
        url
    }
}

/// Escapes measurement and tag keys/values according to InfluxDB Line Protocol specification.
/// Escapes commas, spaces, and equals signs.
pub fn escape_key(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if c == ' ' || c == ',' || c == '=' {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

/// Escapes string field values surrounded by double quotes.
/// Internal quotes and backslashes are escaped.
pub fn escape_string_field(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        if c == '"' || c == '\\' {
            out.push('\\');
        }
        out.push(c);
    }
    out.push('"');
    out
}

/// Serializes a single record map into an InfluxDB Line Protocol line.
///
/// Format:
/// `<measurement>[,<tag_key>=<tag_value>...] <field_key>=<field_value>... [<timestamp>]`
pub fn record_to_line_protocol(
    record: &HashMap<String, Value>,
    measurement_template: &str,
    tags_spec: Option<&HashMap<String, String>>,
    fields_filter: Option<&[String]>,
    ts_field_name: Option<&str>,
    precision: &str,
) -> Option<String> {
    // 1. Resolve measurement name (evaluate template if dynamic)
    let measurement = if measurement_template.contains("{{") {
        let record_val = Value::Object(record.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
        apply_data_template_value(measurement_template, &record_val)
    } else {
        measurement_template.to_string()
    };
    if measurement.trim().is_empty() {
        return None;
    }

    // 2. Evaluate and sort tags
    let mut evaluated_tags: Vec<(String, String)> = Vec::new();
    if let Some(tags_map) = tags_spec {
        let record_val = Value::Object(record.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
        for (k, v_tpl) in tags_map {
            let val = if v_tpl.contains("{{") {
                apply_data_template_value(v_tpl, &record_val)
            } else if let Some(v_direct) = record.get(v_tpl).and_then(|v| v.as_str()) {
                v_direct.to_string()
            } else {
                v_tpl.clone()
            };
            if !val.trim().is_empty() {
                evaluated_tags.push((k.clone(), val));
            }
        }
    }
    evaluated_tags.sort_by(|a, b| a.0.cmp(&b.0));

    // 3. Extract and filter fields
    let mut line_fields: Vec<(String, String)> = Vec::new();
    let is_tag_key = |key: &str| -> bool {
        tags_spec.is_some_and(|ts| ts.contains_key(key))
    };

    let field_keys: Vec<String> = if let Some(filter) = fields_filter {
        filter.to_vec()
    } else {
        record
            .keys()
            .filter(|k| !k.starts_with("__") && !is_tag_key(k) && ts_field_name != Some(k.as_str()))
            .cloned()
            .collect()
    };

    for k in field_keys {
        if let Some(val) = record.get(&k) {
            let formatted_val = match val {
                Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        format!("{}i", i)
                    } else if let Some(u) = n.as_u64() {
                        format!("{}i", u)
                    } else if let Some(f) = n.as_f64() {
                        if f.is_nan() || f.is_infinite() {
                            continue;
                        }
                        format!("{}", f)
                    } else {
                        continue;
                    }
                }
                Value::String(s) => escape_string_field(s),
                Value::Bool(b) => if *b { "true".to_string() } else { "false".to_string() },
                _ => continue,
            };
            line_fields.push((k, formatted_val));
        }
    }

    if line_fields.is_empty() {
        return None;
    }
    line_fields.sort_by(|a, b| a.0.cmp(&b.0));

    // 4. Resolve timestamp
    let now_millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);

    let raw_ts = if let Some(field) = ts_field_name {
        record.get(field).and_then(|v| {
            if let Some(i) = v.as_i64() {
                Some(i)
            } else if let Some(f) = v.as_f64() {
                Some(f as i64)
            } else if let Some(s) = v.as_str() {
                s.parse::<i64>().ok()
            } else {
                None
            }
        }).unwrap_or(now_millis)
    } else {
        now_millis
    };

    let final_ts = match precision {
        "s" => raw_ts / 1000,
        "us" => raw_ts * 1000,
        "ns" => raw_ts * 1_000_000,
        _ => raw_ts, // "ms"
    };

    // 5. Construct line
    let mut line = escape_key(&measurement);
    for (tk, tv) in evaluated_tags {
        line.push(',');
        line.push_str(&escape_key(&tk));
        line.push('=');
        line.push_str(&escape_key(&tv));
    }

    line.push(' ');

    let mut first_field = true;
    for (fk, fv) in line_fields {
        if !first_field {
            line.push(',');
        } else {
            first_field = false;
        }
        line.push_str(&escape_key(&fk));
        line.push('=');
        line.push_str(&fv);
    }

    line.push(' ');
    line.push_str(&final_ts.to_string());

    Some(line)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_escape_key() {
        assert_eq!(escape_key("temperature"), "temperature");
        assert_eq!(escape_key("room temp"), "room\\ temp");
        assert_eq!(escape_key("a=b,c d"), "a\\=b\\,c\\ d");
    }

    #[test]
    fn test_escape_string_field() {
        assert_eq!(escape_string_field("normal"), "\"normal\"");
        assert_eq!(escape_string_field("a \"quoted\" word"), "\"a \\\"quoted\\\" word\"");
        assert_eq!(escape_string_field("path\\to"), "\"path\\\\to\"");
    }

    #[test]
    fn test_record_to_line_protocol() {
        let mut record = HashMap::new();
        record.insert("device".to_string(), json!("sensor_01"));
        record.insert("temp".to_string(), json!(23.5));
        record.insert("humidity".to_string(), json!(65));
        record.insert("active".to_string(), json!(true));
        record.insert("status".to_string(), json!("ok"));
        record.insert("ts".to_string(), json!(1700000000000i64));

        let mut tags = HashMap::new();
        tags.insert("device_id".to_string(), "{{.device}}".to_string());
        tags.insert("site".to_string(), "factory_1".to_string());

        let line = record_to_line_protocol(
            &record,
            "telemetry",
            Some(&tags),
            None,
            Some("ts"),
            "ms",
        )
        .unwrap();

        assert!(line.starts_with("telemetry,device_id=sensor_01,site=factory_1 "));
        assert!(line.contains("active=true"));
        assert!(line.contains("humidity=65i"));
        assert!(line.contains("status=\"ok\""));
        assert!(line.contains("temp=23.5"));
        assert!(line.ends_with(" 1700000000000"));
    }

    #[test]
    fn test_write_urls() {
        let v1 = InfluxConfig {
            addr: "http://localhost:8086".to_string(),
            database: "metrics".to_string(),
            measurement: "cpu".to_string(),
            username: Some("admin".to_string()),
            password: Some("secret".to_string()),
            precision: "s".to_string(),
            tags: None,
            fields: None,
            ts_field_name: None,
        };
        assert_eq!(
            v1.write_url(),
            "http://localhost:8086/write?db=metrics&precision=s&u=admin&p=secret"
        );

        let v2 = Influx2Config {
            addr: "http://localhost:8086".to_string(),
            org: "my-org".to_string(),
            bucket: "my-bucket".to_string(),
            token: Some("tok123".to_string()),
            measurement: "cpu".to_string(),
            precision: "ms".to_string(),
            tags: None,
            fields: None,
            ts_field_name: None,
        };
        assert_eq!(
            v2.write_url(),
            "http://localhost:8086/api/v2/write?org=my-org&bucket=my-bucket&precision=ms"
        );
    }
}
