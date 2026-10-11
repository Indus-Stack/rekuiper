use anyhow::Result;
use async_trait::async_trait;
use rekuiper_core::model::StreamRecord;
pub use rekuiper_core::RuleCounters;

pub const META_KEY: &str = "__meta__";

pub mod codec;
pub use codec::*;

pub mod parquet_io;
pub use parquet_io::*;

pub mod secrets;
pub use secrets::*;

pub mod rabbitmq;
pub use rabbitmq::*;

pub mod data_template;
pub use data_template::*;

pub mod memory;
pub use memory::*;

pub mod http;
pub use http::*;

pub mod file;
pub use file::*;

pub mod mqtt;
pub use mqtt::*;
#[cfg(test)]
pub(crate) use mqtt::{drain_buffered_publishes, mqtt_options};

pub mod websocket;
pub use websocket::*;

pub mod redis;
pub use redis::*;

pub mod kafka;
pub use kafka::*;

pub mod simulator;
pub use simulator::*;

pub mod sql;
pub use sql::*;

pub mod influx;
pub use influx::*;

pub mod tdengine;
pub use tdengine::*;

pub mod iotdb;
pub use iotdb::*;

pub mod image_sink;
pub use image_sink::*;

pub mod zmq;
pub use zmq::*;

#[async_trait]
pub trait Sink: Send + Sync {
    async fn send(&self, record: &StreamRecord) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    use std::collections::HashMap;
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_kafka_sink_unreachable_errors() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let sink = KafkaSink {
            config: KafkaConfig {
                brokers: format!("127.0.0.1:{}", port),
                topic: Some("t".to_string()),
                group_id: None,
                partition: 0,
                key: None,
                offset_reset: "latest".to_string(),
            },
        };
        let mut data = HashMap::new();
        data.insert("id".to_string(), json!("k1"));
        // Must fail (not hang): the sink bounds the produce path.
        let res = tokio::time::timeout(
            std::time::Duration::from_secs(25),
            sink.send(&StreamRecord::new(data)),
        )
        .await
        .expect("kafka send hung past its internal timeout");
        assert!(res.is_err());
    }

    #[test]
    fn test_apply_data_template() {
        let mut data = serde_json::Map::new();
        data.insert("id".to_string(), json!("d1"));
        data.insert("temp".to_string(), json!(25.5));
        assert_eq!(
            apply_data_template("device: {{.id}}, temp: {{.temp}}", &data),
            "device: d1, temp: 25.5"
        );
        // Missing fields render as <no value>.
        assert_eq!(apply_data_template("x={{.missing}}", &data), "x=<no value>");
    }

    #[test]
    fn test_sql_connector_config_defaults() {
        let cfg: SqlConnectorConfig =
            serde_json::from_value(json!({"url": "sqlite::memory:", "table": "alerts"})).unwrap();
        assert_eq!(cfg.url, "sqlite::memory:");
        assert_eq!(cfg.table, "alerts");
        assert!(cfg.fields.is_empty());
        assert_eq!(cfg.interval, 1000);
    }

    #[test]
    fn test_kafka_config_defaults() {
        let cfg: KafkaConfig = serde_json::from_value(json!({})).unwrap();
        assert_eq!(cfg.brokers, "127.0.0.1:9092");
        assert_eq!(cfg.broker_list(), vec!["127.0.0.1:9092".to_string()]);
        assert_eq!(cfg.topic, None);
        assert_eq!(cfg.partition, 0);

        let cfg: KafkaConfig =
            serde_json::from_value(json!({"brokers": "a:9092, b:9092 ", "topic": "events"}))
                .unwrap();
        assert_eq!(
            cfg.broker_list(),
            vec!["a:9092".to_string(), "b:9092".to_string()]
        );
        assert_eq!(cfg.topic.as_deref(), Some("events"));
    }

    #[test]
    fn test_redis_sink_config_defaults() {
        let cfg: RedisSinkConfig = serde_json::from_value(json!({"field": "id"})).unwrap();
        assert_eq!(cfg.addr, "127.0.0.1:6379");
        assert_eq!(cfg.connection_url(), "redis://127.0.0.1:6379");
        assert_eq!(cfg.data_type, "string");
        assert_eq!(cfg.topic, None);
        let with_db: RedisSinkConfig =
            serde_json::from_value(json!({"addr": "127.0.0.1:6379", "db": 2})).unwrap();
        assert_eq!(with_db.connection_url(), "redis://127.0.0.1:6379/2");
    }

    #[test]
    fn test_http_pull_config_defaults() {
        let cfg: HttpPullConfig =
            serde_json::from_value(json!({"url": "http://127.0.0.1:8080/data"})).unwrap();
        assert_eq!(cfg.url, "http://127.0.0.1:8080/data");
        assert_eq!(cfg.method, "get");
        assert_eq!(cfg.interval, 1000);
        assert!(cfg.headers.is_empty());
        assert_eq!(cfg.body, None);
    }

    #[test]
    fn test_websocket_target_url() {
        let cfg: WebSocketConfig = serde_json::from_value(json!({
            "addr": "127.0.0.1:9001",
            "path": "/ws_sink",
        }))
        .unwrap();
        assert_eq!(cfg.scheme, "ws");
        assert_eq!(cfg.target_url(), "ws://127.0.0.1:9001/ws_sink");

        // `address` is accepted as an alias and missing slashes are fixed.
        let cfg: WebSocketConfig = serde_json::from_value(json!({
            "address": "example.com:443",
            "scheme": "wss",
            "path": "api/data",
        }))
        .unwrap();
        assert_eq!(cfg.target_url(), "wss://example.com:443/api/data");

        // Bare defaults point at the local endpoint root.
        let cfg: WebSocketConfig = serde_json::from_value(json!({})).unwrap();
        assert_eq!(cfg.target_url(), "ws://127.0.0.1:8080/");
    }

    fn unique_temp_path(tag: &str) -> PathBuf {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "rekuiper-{}-{}-{}.ldjson",
            tag,
            std::process::id(),
            nanos
        ));
        p
    }

    #[tokio::test]
    async fn test_file_sink_roundtrip() {
        let path = unique_temp_path("filesink");
        let sink = FileSink::new(&path);

        let mut d1 = HashMap::new();
        d1.insert("temp".to_string(), json!(25.0));
        let mut d2 = HashMap::new();
        d2.insert("temp".to_string(), json!(30.0));
        d2.insert("status".to_string(), json!("ok"));

        sink.send(&StreamRecord::new(d1.clone())).await.unwrap();
        sink.send(&StreamRecord::new(d2.clone())).await.unwrap();

        // Read raw file back: two newline-terminated JSON lines.
        let content = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(content.ends_with('\n'));
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2);
        let v0: Value = serde_json::from_str(lines[0]).unwrap();
        let v1: Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(v0["temp"], json!(25.0));
        assert_eq!(v1["temp"], json!(30.0));
        assert_eq!(v1["status"], json!("ok"));

        // FileSource reads the same file back into StreamRecords.
        let source = FileSource::new(
            FileSourceConfig {
                path: path.to_string_lossy().into_owned(),
                format: "json".to_string(),
                has_header: false,
                delimiter: None,
                interval: 0,
            },
            rekuiper_core::StreamBus::new().get_or_create("test"),
        );
        let records = source.read_records().await.unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].data.get("temp"), Some(&json!(25.0)));
        assert_eq!(records[1].data.get("status"), Some(&json!("ok")));

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn test_file_source_streaming() {
        let path = unique_temp_path("filesource");
        tokio::fs::write(&path, "{\"temp\": 20}\n{\"temp\": 25}\n{\"temp\": 30}\n")
            .await
            .unwrap();
        let bus = rekuiper_core::StreamBus::new();
        let tx = bus.get_or_create("test-filesource");
        let mut rx = bus.subscribe("test-filesource");
        let source = FileSource::new(
            FileSourceConfig {
                path: path.to_string_lossy().into_owned(),
                format: "json".to_string(),
                has_header: false,
                delimiter: None,
                interval: 0,
            },
            tx,
        );
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        let handle = source.spawn(cancel_rx);

        let mut temps = Vec::new();
        for _ in 0..3 {
            let record = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
                .await
                .expect("timed out waiting for file record")
                .expect("file channel closed");
            temps.push(record.data.get("temp").cloned().unwrap());
        }
        assert_eq!(temps, vec![json!(20), json!(25), json!(30)]);

        // Cancellation stops the tail loop cleanly.
        cancel_tx.send(true).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), handle)
            .await
            .expect("file source did not exit after cancel")
            .unwrap();

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[test]
    fn test_file_source_config_defaults() {
        let cfg: FileSourceConfig = serde_json::from_value(json!({"path": "data/a.json"})).unwrap();
        assert_eq!(cfg.format, "json");
        assert!(!cfg.has_header);
        assert_eq!(cfg.delimiter, None);
        assert_eq!(cfg.interval, 0);

        // Delimiter accepts single chars and friendly names.
        let cfg: FileSourceConfig =
            serde_json::from_value(json!({"path": "a.csv", "delimiter": "tab"})).unwrap();
        assert_eq!(cfg.delimiter, Some('\t'));
        let cfg: FileSourceConfig =
            serde_json::from_value(json!({"path": "a.csv", "delimiter": "|"})).unwrap();
        assert_eq!(cfg.delimiter, Some('|'));
    }

    #[test]
    fn test_parse_mqtt_server_url() {
        assert_eq!(
            super::parse_mqtt_server_url("tcp://127.0.0.1:1883").unwrap(),
            ("127.0.0.1".to_string(), 1883)
        );
        assert_eq!(
            super::parse_mqtt_server_url("127.0.0.1:1883").unwrap(),
            ("127.0.0.1".to_string(), 1883)
        );
        assert_eq!(
            super::parse_mqtt_server_url("ssl://broker.emqx.io:8883").unwrap(),
            ("broker.emqx.io".to_string(), 8883)
        );
        // Scheme defaults.
        assert_eq!(
            super::parse_mqtt_server_url("tcp://127.0.0.1").unwrap(),
            ("127.0.0.1".to_string(), 1883)
        );
        assert_eq!(
            super::parse_mqtt_server_url("ssl://broker.emqx.io").unwrap(),
            ("broker.emqx.io".to_string(), 8883)
        );
    }

    #[test]
    fn test_mqtt_config_serde() {
        let json = serde_json::json!({
            "server": "tcp://127.0.0.1:1883",
            "topic": "devices/result",
            "qos": 1,
            "clientId": "demo_001"
        });
        let cfg: super::MqttConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.server, "tcp://127.0.0.1:1883");
        assert_eq!(cfg.topic, "devices/result");
        assert_eq!(cfg.qos, 1);
        assert_eq!(cfg.client_id.as_deref(), Some("demo_001"));
        assert_eq!(cfg.username, None);
        assert_eq!(cfg.password, None);

        // Serializing preserves the eKuiper `clientId` field name.
        let ser = serde_json::to_value(&cfg).unwrap();
        assert_eq!(ser["server"], json!("tcp://127.0.0.1:1883"));
        assert_eq!(ser["topic"], json!("devices/result"));
        assert_eq!(ser["qos"], json!(1));
        assert_eq!(ser["clientId"], json!("demo_001"));

        // Round-trip back into the same config.
        let back: super::MqttConfig = serde_json::from_value(ser).unwrap();
        assert_eq!(back, cfg);
    }

    #[test]
    fn test_mqtt_config_confkey_without_topic() {
        // D1: source CONF_KEY entries carry only connection parameters (no
        // topic). They must decode with the stored broker URL instead of
        // failing and silently falling back to the loopback broker.
        let conf = serde_json::json!({"server": "tcp://broker:1883"});
        let cfg: super::MqttConfig = serde_json::from_value(conf).unwrap();
        assert_eq!(cfg.server, "tcp://broker:1883");
        assert_eq!(cfg.topic, "");
        // An empty object still yields the loopback default server.
        let cfg: super::MqttConfig = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(cfg.server, "tcp://127.0.0.1:1883");
        assert_eq!(cfg.topic, "");
    }

    #[test]
    fn test_sql_statement_builders() {
        // D8: SQLite keeps `?` placeholders; PostgreSQL numbers them, since
        // its wire protocol rejects `?`.
        let fields = vec!["id".to_string(), "val".to_string()];
        assert_eq!(
            super::sqlite_insert_sql("readings", &fields),
            "INSERT INTO readings (id, val) VALUES (?, ?)"
        );
        assert_eq!(
            super::pg_insert_sql("readings", &fields),
            "INSERT INTO readings (id, val) VALUES ($1, $2)"
        );
        assert!(
            !super::pg_insert_sql("readings", &fields).contains('?'),
            "postgres statements must not contain `?`"
        );
        assert_eq!(
            super::pg_lookup_sql("readings", "id"),
            "SELECT * FROM readings WHERE CAST(id AS TEXT) = $1 LIMIT 1"
        );
    }

    #[tokio::test]
    async fn test_sql_sink_rejects_unsupported_url() {
        let sink = super::SqlSink {
            config: super::SqlConnectorConfig {
                url: "mysql://localhost/readings".to_string(),
                table: "readings".to_string(),
                ..Default::default()
            },
        };
        let err = sink
            .insert_record(&StreamRecord::new(HashMap::new()))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Unsupported SQL sink URL scheme"));
    }

    #[test]
    fn test_is_valid_sql_identifier() {
        assert!(super::is_valid_sql_identifier("temperature"));
        assert!(super::is_valid_sql_identifier("_private_id"));
        assert!(super::is_valid_sql_identifier("device123"));
        assert!(super::is_valid_sql_identifier("A_b_1"));

        assert!(!super::is_valid_sql_identifier(""));
        assert!(!super::is_valid_sql_identifier("123abc"));
        assert!(!super::is_valid_sql_identifier("temp-celsius"));
        assert!(!super::is_valid_sql_identifier("temp celsius"));
        assert!(!super::is_valid_sql_identifier("col;DROP TABLE"));
        assert!(!super::is_valid_sql_identifier("field.subfield"));
    }

    #[tokio::test]
    async fn test_sql_sink_rejects_invalid_dynamic_identifiers() {
        let sink = super::SqlSink {
            config: super::SqlConnectorConfig {
                url: "sqlite::memory:".to_string(),
                table: "readings".to_string(),
                fields: Vec::new(), // dynamic fields mode
                ..Default::default()
            },
        };
        let mut data = HashMap::new();
        data.insert("valid_col".to_string(), json!(100));
        data.insert("invalid-col".to_string(), json!(200));

        let err = sink
            .insert_record(&StreamRecord::new(data))
            .await
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("Invalid dynamic field name 'invalid-col'"));
    }

    #[tokio::test]
    async fn test_pg_paths_fail_loudly_without_broker() {
        // D8: postgres URLs must attempt a real connection and surface the
        // error, never silently report Ok with zero rows written or read.
        // Port 1 on loopback refuses immediately, so this cannot hang.
        let url = "postgres://127.0.0.1:1/nonexistent";
        let cfg = super::SqlConnectorConfig {
            url: url.to_string(),
            table: "t".to_string(),
            fields: vec!["id".to_string()],
            interval: 1000,
            template_sql_query_cfg: None,
            internal_sql_query_cfg: None,
        };
        let sink = super::SqlSink {
            config: cfg.clone(),
        };
        let mut data = HashMap::new();
        data.insert("id".to_string(), json!(1));
        assert!(sink.insert_record(&StreamRecord::new(data)).await.is_err());
        assert!(super::sql_lookup_key(url, "t", "id", "1").await.is_err());
        let mut src =
            super::SqlSource::new(cfg, rekuiper_core::StreamBus::new().get_or_create("test"));
        assert!(src.poll_once().await.is_err());
    }

    #[tokio::test]
    async fn test_nop_sink() {
        let sink = super::NopSink;
        let mut data = HashMap::new();
        data.insert("temp".to_string(), json!(25.0));
        sink.send(&StreamRecord::new(data)).await.unwrap();
    }

    #[tokio::test]
    async fn test_simulator_source() {
        let config: super::SimulatorConfig = serde_json::from_value(json!({
            "data": [{"temp": 10.0}, {"temp": 20.0}, {"temp": 30.0}],
            "interval": 1,
            "loop": false,
        }))
        .unwrap();
        assert!(!config.loop_data);

        // `loop` defaults to true when missing.
        let defaulted: super::SimulatorConfig =
            serde_json::from_value(json!({"data": []})).unwrap();
        assert!(defaulted.loop_data);

        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        let count = super::SimulatorSource::new(config).run(tx).await;
        assert_eq!(count, 3);

        let mut temps = Vec::new();
        while let Some(record) = rx.recv().await {
            temps.push(record.data.get("temp").cloned().unwrap());
        }
        assert_eq!(temps, vec![json!(10.0), json!(20.0), json!(30.0)]);
    }

    #[test]
    fn test_sql_connector_config_documented_shape() {
        // Documented eKuiper plugin shape: dburl + templateSqlQueryCfg.
        let cfg: super::SqlConnectorConfig = serde_json::from_value(json!({
            "dburl": "postgres://u:p@h/db?sslmode=disable",
            "interval": 5000,
            "templateSqlQueryCfg": {"templateSql": "SELECT id, val FROM rksrc"}
        }))
        .unwrap();
        assert_eq!(cfg.url, "postgres://u:p@h/db?sslmode=disable");
        assert_eq!(cfg.interval, 5000);
        let t = cfg.template_sql_query_cfg.expect("template cfg parsed");
        assert_eq!(t.template_sql, "SELECT id, val FROM rksrc");
        // Native shape keeps working.
        let native: super::SqlConnectorConfig = serde_json::from_value(json!({
            "url": "sqlite::memory:", "table": "alerts"
        }))
        .unwrap();
        assert_eq!(native.table, "alerts");
        assert_eq!(native.interval, 1000);
        assert!(native.template_sql_query_cfg.is_none());
        // Internal incremental shape parses exactly as PUT by probes.
        let inc: super::SqlConnectorConfig = serde_json::from_value(json!({
            "dburl": "postgres://u:p@h/db?sslmode=disable",
            "interval": 3000,
            "internalSqlQueryCfg": {
                "table": "rksrc",
                "limit": 10,
                "indexFields": [{"indexField": "id", "indexValue": 0, "indexFieldType": "bigint"}],
            },
        }))
        .unwrap();
        assert_eq!(inc.url, "postgres://u:p@h/db?sslmode=disable");
        let inner = inc
            .internal_sql_query_cfg
            .clone()
            .expect("internal cfg parsed");
        assert_eq!(inner.table, "rksrc");
        assert_eq!(inner.limit, 10);
        assert_eq!(inner.index_fields.len(), 1);
        assert_eq!(inner.index_fields[0].index_field, "id");
        assert_eq!(
            super::sql_source_query(&inc),
            "SELECT * FROM rksrc WHERE id > 0 ORDER BY id ASC LIMIT 10"
        );
    }

    #[test]
    fn test_sql_source_query_builders() {
        let base = super::SqlConnectorConfig {
            url: "x".to_string(),
            table: "rksrc".to_string(),
            ..Default::default()
        };
        assert_eq!(super::sql_source_query(&base), "SELECT * FROM rksrc");
        let tpl = super::SqlConnectorConfig {
            template_sql_query_cfg: Some(super::TemplateSqlQueryCfg {
                template_sql: "SELECT id, val FROM rksrc".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(super::sql_source_query(&tpl), "SELECT id, val FROM rksrc");
        let tpl_vars = super::SqlConnectorConfig {
            template_sql_query_cfg: Some(super::TemplateSqlQueryCfg {
                template_sql: "select * from t where a > {{.a}} and b > {{ .b }}".to_string(),
                index_fields: vec![
                    super::IndexFieldCfg {
                        index_field: "a".to_string(),
                        index_value: json!(3),
                        ..Default::default()
                    },
                    super::IndexFieldCfg {
                        index_field: "b".to_string(),
                        index_value: json!("x'y"),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            super::sql_source_query(&tpl_vars),
            "select * from t where a > 3 and b > 'x''y'"
        );
        let internal = super::SqlConnectorConfig {
            internal_sql_query_cfg: Some(super::InternalSqlQueryCfg {
                table: "Student".to_string(),
                limit: 10,
                index_field: "stun".to_string(),
                index_value: json!(100),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            super::sql_source_query(&internal),
            "SELECT * FROM Student WHERE stun > 100 ORDER BY stun ASC LIMIT 10"
        );
    }

    fn sqlite_file_url(name: &str) -> String {
        let mut p = std::env::temp_dir();
        p.push(format!("rekuiper-{}-{}.db", name, std::process::id()));
        let _ = std::fs::remove_file(&p);
        format!("sqlite://{}?mode=rwc", p.display())
    }

    #[tokio::test]
    async fn test_sqlite_sink_sink_preserves_float_and_poll_typed() {
        let url = sqlite_file_url("sinkfloat");
        let pool = sqlx::sqlite::SqlitePool::connect(&url).await.unwrap();
        sqlx::query("CREATE TABLE rksink(eid TEXT PRIMARY KEY, temp REAL)")
            .execute(&pool)
            .await
            .unwrap();
        drop(pool);
        let sink = super::SqlSink {
            config: super::SqlConnectorConfig {
                url: url.clone(),
                table: "rksink".to_string(),
                fields: vec!["eid".to_string(), "temp".to_string()],
                ..Default::default()
            },
        };
        let mut data = HashMap::new();
        data.insert("eid".to_string(), json!("row-pg1"));
        data.insert("temp".to_string(), json!(33.5));
        sink.insert_record(&StreamRecord::new(data)).await.unwrap();
        // REAL column holds a real number, not text.
        let pool = sqlx::sqlite::SqlitePool::connect(&url).await.unwrap();
        let row: (String, f64) = sqlx::query_as("SELECT eid, temp FROM rksink WHERE eid='row-pg1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(row, ("row-pg1".to_string(), 33.5));
        let typeof_temp: String = sqlx::query_scalar("SELECT typeof(temp) FROM rksink")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(typeof_temp, "real");
        drop(pool);
        // Polling source decodes typed rows.
        let mut src = super::SqlSource::new(
            super::SqlConnectorConfig {
                url,
                table: "rksink".to_string(),
                ..Default::default()
            },
            rekuiper_core::StreamBus::new().get_or_create("test"),
        );
        let rows = src.poll_once().await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].data.get("temp"), Some(&json!(33.5)));
        assert!(rows[0].data.get("temp").unwrap().is_number());
        // Point lookup decodes typed values too.
        let hit = super::sql_lookup_key(&src.config.url, "rksink", "eid", "row-pg1")
            .await
            .unwrap()
            .expect("lookup hits");
        assert_eq!(hit.get("temp"), Some(&json!(33.5)));
        assert!(hit.get("temp").unwrap().is_number());
    }

    #[tokio::test]
    async fn test_sqlite_sink_null_is_null_not_default() {
        // Explicit JSON nulls and missing fields must land as SQL NULL —
        // never column DEFAULTs and never a mistyped NULL parameter.
        let url = sqlite_file_url("sinknulls");
        let pool = sqlx::sqlite::SqlitePool::connect(&url).await.unwrap();
        sqlx::query("CREATE TABLE rknull(eid TEXT PRIMARY KEY, temp REAL NULL DEFAULT 99.9, flag INTEGER NULL DEFAULT 1)")
            .execute(&pool)
            .await
            .unwrap();
        drop(pool);
        // Row-SQL builders: NULL literal for nulls, ordered placeholders else.
        assert_eq!(
            super::pg_insert_row_sql(
                "t",
                &["a".to_string(), "b".to_string(), "c".to_string()],
                &[false, true, false]
            ),
            "INSERT INTO t (a, b, c) VALUES ($1, NULL, $2)"
        );
        assert_eq!(
            super::sqlite_insert_row_sql("t", &["a".to_string(), "b".to_string()], &[true, false]),
            "INSERT INTO t (a, b) VALUES (NULL, ?)"
        );
        let sink = super::SqlSink {
            config: super::SqlConnectorConfig {
                url: url.clone(),
                table: "rknull".to_string(),
                fields: vec!["eid".to_string(), "temp".to_string(), "flag".to_string()],
                ..Default::default()
            },
        };
        let mut all_null = HashMap::new();
        all_null.insert("eid".to_string(), json!("n1"));
        all_null.insert("temp".to_string(), serde_json::Value::Null);
        all_null.insert("flag".to_string(), serde_json::Value::Null);
        sink.insert_record(&StreamRecord::new(all_null))
            .await
            .unwrap();
        let mut missing = HashMap::new();
        missing.insert("eid".to_string(), json!("n2"));
        sink.insert_record(&StreamRecord::new(missing))
            .await
            .unwrap();
        let mut mixed = HashMap::new();
        mixed.insert("eid".to_string(), json!("n3"));
        mixed.insert("temp".to_string(), json!(21));
        mixed.insert("flag".to_string(), json!(true));
        sink.insert_record(&StreamRecord::new(mixed)).await.unwrap();
        let pool = sqlx::sqlite::SqlitePool::connect(&url).await.unwrap();
        let rows: Vec<(String, Option<f64>, Option<i64>)> =
            sqlx::query_as("SELECT eid, temp, flag FROM rknull ORDER BY eid")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(
            rows,
            vec![
                ("n1".to_string(), None, None),
                ("n2".to_string(), None, None),
                ("n3".to_string(), Some(21.0), Some(1)),
            ]
        );
    }

    #[tokio::test]
    async fn test_sqlite_sink_batch_insert() {
        let url = sqlite_file_url("sinkbatch");
        let pool = sqlx::sqlite::SqlitePool::connect(&url).await.unwrap();
        sqlx::query("CREATE TABLE rkbatch(id INTEGER PRIMARY KEY, val REAL, label TEXT)")
            .execute(&pool)
            .await
            .unwrap();
        drop(pool);

        let sink = super::SqlSink {
            config: super::SqlConnectorConfig {
                url: url.clone(),
                table: "rkbatch".to_string(),
                fields: vec!["id".to_string(), "val".to_string(), "label".to_string()],
                ..Default::default()
            },
        };

        let mut records = Vec::new();
        for i in 1..=50 {
            let mut data = HashMap::new();
            data.insert("id".to_string(), json!(i));
            data.insert("val".to_string(), json!(i as f64 * 1.5));
            data.insert("label".to_string(), json!(format!("label-{}", i)));
            records.push(StreamRecord::new(data));
        }

        sink.insert_batch(&records).await.unwrap();

        let pool = sqlx::sqlite::SqlitePool::connect(&url).await.unwrap();
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM rkbatch")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count.0, 50);

        let first: (i64, f64, String) =
            sqlx::query_as("SELECT id, val, label FROM rkbatch WHERE id = 1")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(first, (1, 1.5, "label-1".to_string()));
    }

    #[tokio::test]
    async fn test_sqlite_lookup_typed_values() {
        let url = sqlite_file_url("lookuptyped");
        let pool = sqlx::sqlite::SqlitePool::connect(&url).await.unwrap();
        sqlx::query("CREATE TABLE rklook(id INTEGER PRIMARY KEY, name TEXT)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO rklook VALUES (1, 'one'), (2, 'two')")
            .execute(&pool)
            .await
            .unwrap();
        drop(pool);
        let hit = super::sql_lookup_key(&url, "rklook", "id", "1")
            .await
            .unwrap()
            .expect("lookup hits");
        assert_eq!(hit.get("id"), Some(&json!(1)));
        assert!(hit.get("id").unwrap().is_number());
        assert_eq!(hit.get("name"), Some(&json!("one")));
    }

    fn src_ids(rows: &[super::StreamRecord]) -> Vec<i64> {
        let mut ids: Vec<i64> = rows
            .iter()
            .filter_map(|r| r.data.get("id").and_then(|v| v.as_i64()))
            .collect();
        ids.sort_unstable();
        ids
    }

    #[tokio::test]
    async fn test_sqlite_source_advances_index_across_polls() {
        // Incremental polling: each poll emits only rows newer than the
        // tracked index (last row wins, ORDER BY .. ASC pagination).
        let url = sqlite_file_url("srcindex");
        let pool = sqlx::sqlite::SqlitePool::connect(&url).await.unwrap();
        sqlx::query("CREATE TABLE rksrc(id INTEGER PRIMARY KEY, val INTEGER)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO rksrc VALUES (1,10),(2,20),(3,30)")
            .execute(&pool)
            .await
            .unwrap();
        drop(pool);
        let mut src = super::SqlSource::new(
            super::SqlConnectorConfig {
                url: url.clone(),
                interval: 100,
                internal_sql_query_cfg: Some(super::InternalSqlQueryCfg {
                    table: "rksrc".to_string(),
                    limit: 10,
                    index_fields: vec![super::IndexFieldCfg {
                        index_field: "id".to_string(),
                        index_value: json!(0),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            },
            rekuiper_core::StreamBus::new().get_or_create("test"),
        );
        assert_eq!(src_ids(&src.poll_once().await.unwrap()), vec![1, 2, 3]);
        let pool = sqlx::sqlite::SqlitePool::connect(&url).await.unwrap();
        sqlx::query("INSERT INTO rksrc VALUES (4,40),(5,50)")
            .execute(&pool)
            .await
            .unwrap();
        drop(pool);
        assert_eq!(src_ids(&src.poll_once().await.unwrap()), vec![4, 5]);
        assert!(src.poll_once().await.unwrap().is_empty());
        // Template variant with the same index contract.
        let mut tsrc = super::SqlSource::new(
            super::SqlConnectorConfig {
                url,
                interval: 100,
                template_sql_query_cfg: Some(super::TemplateSqlQueryCfg {
                    template_sql: "select * from rksrc where id > {{.id}}".to_string(),
                    index_fields: vec![super::IndexFieldCfg {
                        index_field: "id".to_string(),
                        index_value: json!(3),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            },
            rekuiper_core::StreamBus::new().get_or_create("test"),
        );
        assert_eq!(src_ids(&tsrc.poll_once().await.unwrap()), vec![4, 5]);
    }

    #[test]
    fn test_delimited_parsing() {
        let headers = ["temp", "humidity", "status"]
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        let map = super::parse_delimited_line("22.5,50,ok", ',', &headers);
        assert_eq!(map.get("temp"), Some(&json!(22.5)));
        assert!(map.get("temp").unwrap().is_number());
        assert_eq!(map.get("humidity"), Some(&json!(50)));
        assert!(map.get("humidity").unwrap().is_number());
        assert_eq!(map.get("status"), Some(&json!("ok")));
        assert!(map.get("status").unwrap().is_string());
    }

    #[test]
    fn test_mqtt_payload_decode() {
        let payload = br#"{"temp": 25.0, "status": "ok"}"#;
        let record = super::decode_mqtt_payload(payload).unwrap();
        assert_eq!(record.data.get("temp"), Some(&json!(25.0)));
        assert_eq!(record.data.get("status"), Some(&json!("ok")));

        // Non-object payloads are rejected.
        assert!(super::decode_mqtt_payload(br#"[1, 2]"#).is_err());
        assert!(super::decode_mqtt_payload(br#"not json"#).is_err());
    }

    #[test]
    fn test_drain_buffered_publishes_batches_in_order() {
        use std::collections::VecDeque;
        let publish = |payload: Vec<u8>| {
            rumqttc::Event::Incoming(rumqttc::Packet::Publish(rumqttc::Publish::new(
                "bench/telemetry",
                rumqttc::QoS::AtMostOnce,
                payload,
            )))
        };
        let obj = |id: u32| format!("{{\"id\":{id}}}").into_bytes();

        // Order preserved, invalid payload skipped, stops at the first
        // non-publish event so poll() still handles it (and what follows).
        let mut events: VecDeque<rumqttc::Event> = VecDeque::new();
        events.push_back(publish(obj(2)));
        events.push_back(publish(obj(3)));
        events.push_back(publish(b"not json".to_vec()));
        events.push_back(publish(obj(4)));
        events.push_back(rumqttc::Event::Incoming(rumqttc::Packet::PingResp));
        events.push_back(publish(obj(5)));
        let mut config: super::MqttConfig =
            serde_json::from_value(json!({"topic": "bench/telemetry"})).unwrap();
        let mut batch = vec![super::decode_mqtt_payload(br#"{"id":1}"#).unwrap()];
        let counters = super::RuleCounters::default();
        super::drain_buffered_publishes(
            &config,
            &mut events,
            &mut batch,
            1024,
            Some(&counters),
            None,
        );
        assert_eq!(
            counters
                .exceptions
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        let ids: Vec<_> = batch.iter().map(|r| r.data["id"].clone()).collect();
        assert_eq!(ids, vec![json!(1), json!(2), json!(3), json!(4)]);
        assert_eq!(
            events.len(),
            2,
            "PingResp and the publish after it stay queued"
        );
        assert!(
            batch.iter().all(|r| !r.data.contains_key("__meta__")),
            "metadata is only attached on request"
        );

        // The batch size bound is respected; the rest stays queued in order.
        let mut events: VecDeque<rumqttc::Event> = (0..10).map(|i| publish(obj(i))).collect();
        let mut batch = Vec::new();
        super::drain_buffered_publishes(&config, &mut events, &mut batch, 4, None, None);
        let ids: Vec<_> = batch.iter().map(|r| r.data["id"].clone()).collect();
        assert_eq!(ids, vec![json!(0), json!(1), json!(2), json!(3)]);
        assert_eq!(events.len(), 6);

        // Requested metadata carries the publish topic and qos.
        config.attach_meta = true;
        let mut batch = Vec::new();
        super::drain_buffered_publishes(&config, &mut events, &mut batch, 1, None, None);
        assert_eq!(batch[0].data["__meta__"]["topic"], json!("bench/telemetry"));
        assert_eq!(batch[0].data["__meta__"]["qos"], json!(0));
        assert!(batch[0].data["__meta__"].get("messageId").is_some());
    }

    #[test]
    fn test_decode_payload_formats() {
        use super::{decode_payload_into, DelimitedCodec, PayloadFormat};
        let meta = || Some(json!({"topic": "t/1", "qos": 1, "messageId": 7}));

        // JSON array: one row per object, metadata on every row.
        let mut rows = Vec::new();
        decode_payload_into(
            &PayloadFormat::Json,
            br#"[{"a":1},{"a":2}]"#,
            meta(),
            &mut rows,
        )
        .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1].data["a"], json!(2));
        assert!(rows
            .iter()
            .all(|r| r.data["__meta__"]["topic"] == json!("t/1")));

        // Scalars and mixed arrays are rejected without leaving partial rows.
        let mut rows = Vec::new();
        assert!(decode_payload_into(&PayloadFormat::Json, b"23.5", None, &mut rows).is_err());
        assert!(
            decode_payload_into(&PayloadFormat::Json, br#"[{"a":1},2]"#, None, &mut rows).is_err()
        );
        assert!(rows.is_empty());

        // BINARY: ESPHome text state as text, other bytes as base64, in `self`.
        decode_payload_into(&PayloadFormat::Binary, b"23.5", None, &mut rows).unwrap();
        decode_payload_into(&PayloadFormat::Binary, &[0xff, 0x00], None, &mut rows).unwrap();
        assert_eq!(rows[0].data["self"], json!("23.5"));
        assert_eq!(rows[1].data["self"], json!("/wA="));

        // DELIMITED: positional colN names without a schema, stream columns with one.
        let mut rows = Vec::new();
        let plain = PayloadFormat::Delimited(DelimitedCodec::new(',', Vec::new()));
        decode_payload_into(&plain, b"V1,88.5,on\n", None, &mut rows).unwrap();
        assert_eq!(rows[0].data["col0"], json!("V1"));
        assert_eq!(rows[0].data["col1"], json!(88.5));
        assert_eq!(rows[0].data["col2"], json!("on"));
        let typed = PayloadFormat::Delimited(DelimitedCodec::new(
            ';',
            vec!["vin".to_string(), "speed".to_string()],
        ));
        decode_payload_into(&typed, b"V2;42", None, &mut rows).unwrap();
        assert_eq!(rows[1].data["vin"], json!("V2"));
        assert_eq!(rows[1].data["speed"], json!(42));

        // PROTOBUF: compact vehicle frame; garbage is an error.
        let messages = super::parse_proto(
            "syntax = \"proto3\";\nmessage Frame { string vin = 1; double speed = 2; }",
        )
        .unwrap();
        let frame = std::sync::Arc::new(messages["Frame"].clone());
        let bytes = super::ProtobufCodec::encode(&json!({"vin": "V3", "speed": 61.25}), &frame);
        let format = PayloadFormat::Protobuf(frame);
        let mut rows = Vec::new();
        decode_payload_into(&format, &bytes, meta(), &mut rows).unwrap();
        assert_eq!(rows[0].data["vin"], json!("V3"));
        assert_eq!(rows[0].data["speed"], json!(61.25));
        assert_eq!(rows[0].data["__meta__"]["messageId"], json!(7));
        assert!(decode_payload_into(&format, &[0x0a, 0x08], None, &mut rows).is_err());
    }

    #[test]
    fn test_mqtt_config_ekuiper_options() {
        let config: super::MqttConfig = serde_json::from_value(json!({
            "server": "tcp://broker:1883",
            "clientid": "vehicle-gw-1",
            "protocolVersion": "3.1.1",
            "cleanSession": false,
            "keepAlive": 60,
            "topic": "vehicles/+/telemetry, chargers/#"
        }))
        .unwrap();
        assert_eq!(config.client_id.as_deref(), Some("vehicle-gw-1"));
        assert_eq!(config.clean_session, Some(false));
        assert_eq!(config.keep_alive, Some(60));
        assert_eq!(
            config.topics(),
            vec!["vehicles/+/telemetry".to_string(), "chargers/#".to_string()]
        );
        assert!(super::mqtt_options(&config).is_ok());
        // Stored action JSON keeps the camelCase spelling.
        let action: super::MqttConfig =
            serde_json::from_value(json!({"clientId": "c2", "topic": "x"})).unwrap();
        assert_eq!(action.client_id.as_deref(), Some("c2"));
    }
}
