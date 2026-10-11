use crate::protocol::{ResourceContent, ResourceDefinition};
use serde_json::json;

pub fn get_resource_definitions() -> Vec<ResourceDefinition> {
    vec![
        ResourceDefinition {
            uri: "rekuiper://rules".to_string(),
            name: "Active Stream Rules".to_string(),
            description: Some("Catalog of all stream processing rules deployed in the rekuiper engine, including topology configurations, SQL queries, action sinks, and execution lifecycle states.".to_string()),
            mime_type: Some("application/json".to_string()),
        },
        ResourceDefinition {
            uri: "rekuiper://streams".to_string(),
            name: "Data Streams".to_string(),
            description: Some("Registry of active streaming data sources, schemas, and transport connectors (MQTT, EdgeX, Kafka, HTTP, Simulator).".to_string()),
            mime_type: Some("application/json".to_string()),
        },
        ResourceDefinition {
            uri: "rekuiper://tables".to_string(),
            name: "Lookup Tables".to_string(),
            description: Some("Catalog of static reference and dimension lookup tables available for enrichment joins in stream processing.".to_string()),
            mime_type: Some("application/json".to_string()),
        },
        ResourceDefinition {
            uri: "rekuiper://connections".to_string(),
            name: "Shared Connections".to_string(),
            description: Some("Shared reusable connection resource pool definitions (brokers, relational databases, Kafka clusters).".to_string()),
            mime_type: Some("application/json".to_string()),
        },
        ResourceDefinition {
            uri: "rekuiper://udfs".to_string(),
            name: "JavaScript UDFs".to_string(),
            description: Some("Registered custom JavaScript User Defined Functions available for scalar transformation in streaming SQL.".to_string()),
            mime_type: Some("application/json".to_string()),
        },
        ResourceDefinition {
            uri: "rekuiper://plugins".to_string(),
            name: "Engine Plugins".to_string(),
            description: Some("Catalog of installed native C/Rust plugins and portable multi-language gRPC extensions.".to_string()),
            mime_type: Some("application/json".to_string()),
        },
        ResourceDefinition {
            uri: "rekuiper://configs".to_string(),
            name: "Engine Configuration".to_string(),
            description: Some("Active global runtime configuration parameters, buffer sizes, logging levels, and connector defaults.".to_string()),
            mime_type: Some("application/json".to_string()),
        },
        ResourceDefinition {
            uri: "rekuiper://metrics".to_string(),
            name: "Engine Metrics".to_string(),
            description: Some("Real-time engine telemetry, CPU and memory utilization, active pipeline count, and throughput counters.".to_string()),
            mime_type: Some("application/json".to_string()),
        },
        ResourceDefinition {
            uri: "rekuiper://metadata/sources".to_string(),
            name: "Source Connectors Catalog".to_string(),
            description: Some("Catalog of available input source connector drivers, configuration schemas, and transport options.".to_string()),
            mime_type: Some("application/json".to_string()),
        },
        ResourceDefinition {
            uri: "rekuiper://metadata/sinks".to_string(),
            name: "Sink Connectors Catalog".to_string(),
            description: Some("Catalog of available action sink drivers (MQTT, Kafka, Redis, SQL, Log, REST, File) and output formatting templates.".to_string()),
            mime_type: Some("application/json".to_string()),
        },
        ResourceDefinition {
            uri: "rekuiper://metadata/functions".to_string(),
            name: "Built-in SQL Functions".to_string(),
            description: Some("Catalog of built-in mathematical, string, aggregate, conversion, and temporal SQL functions available in rekuiper SQL.".to_string()),
            mime_type: Some("application/json".to_string()),
        },
        ResourceDefinition {
            uri: "rekuiper://plugins/wasm".to_string(),
            name: "WebAssembly Plugins".to_string(),
            description: Some("Installed WebAssembly (.wasm) plugins and their exported UDF signatures.".to_string()),
            mime_type: Some("application/json".to_string()),
        },
        ResourceDefinition {
            uri: "rekuiper://schemas/rabbitmq".to_string(),
            name: "RabbitMQ Action Schema".to_string(),
            description: Some("Configuration template for RabbitMQ AMQP 0-9-1 source and sink actions with exchange, queue, and TLS options.".to_string()),
            mime_type: Some("application/json".to_string()),
        },
        ResourceDefinition {
            uri: "rekuiper://schemas/parquet".to_string(),
            name: "Parquet Columnar Sink Schema".to_string(),
            description: Some("Configuration template for Apache Parquet columnar sink actions with compression and row group sizing.".to_string()),
            mime_type: Some("application/json".to_string()),
        },
        ResourceDefinition {
            uri: "rekuiper://schemas/edgex".to_string(),
            name: "EdgeX Dual-Port Config Schema".to_string(),
            description: Some("Configuration template for EdgeX Foundry dual-port listening (59880 / 59881) for legacy and modern deployments.".to_string()),
            mime_type: Some("application/json".to_string()),
        },
        ResourceDefinition {
            uri: "rekuiper://schemas/influx".to_string(),
            name: "InfluxDB v1 and v2 Action Schema".to_string(),
            description: Some("Configuration template for InfluxDB v1 and v2 timeseries sink actions with measurement, tags, and line protocol options.".to_string()),
            mime_type: Some("application/json".to_string()),
        },
        ResourceDefinition {
            uri: "rekuiper://schemas/tdengine".to_string(),
            name: "TDengine 3.x Action Schema".to_string(),
            description: Some("Configuration template for TDengine 3.x timeseries sink action supporting tables and super tables.".to_string()),
            mime_type: Some("application/json".to_string()),
        },
        ResourceDefinition {
            uri: "rekuiper://schemas/iotdb".to_string(),
            name: "Apache IoTDB Action Schema".to_string(),
            description: Some("Configuration template for Apache IoTDB REST timeseries sink action with device tree paths and measurements.".to_string()),
            mime_type: Some("application/json".to_string()),
        },
        ResourceDefinition {
            uri: "rekuiper://schemas/image".to_string(),
            name: "Image Action Schema".to_string(),
            description: Some("Configuration template for Image sink action writing binary camera frames with retention pruning.".to_string()),
            mime_type: Some("application/json".to_string()),
        },
        ResourceDefinition {
            uri: "rekuiper://schemas/random".to_string(),
            name: "Random / Simulator Source Schema".to_string(),
            description: Some("Configuration template for mock telemetry source streams with data replay and frequency intervals.".to_string()),
            mime_type: Some("application/json".to_string()),
        },
        ResourceDefinition {
            uri: "rekuiper://schemas/zmq".to_string(),
            name: "ZeroMQ Action Schema".to_string(),
            description: Some("Configuration template for ZeroMQ PUB/SUB socket sink and source actions.".to_string()),
            mime_type: Some("application/json".to_string()),
        },
    ]
}

async fn fetch_endpoint(
    client: &reqwest::Client,
    base_url: &str,
    path: &str,
) -> Result<String, String> {
    let url = format!("{}{}", base_url.trim_end_matches('/'), path);
    client
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("Request to {} failed: {}", url, e))?
        .text()
        .await
        .map_err(|e| format!("Failed to read response from {}: {}", url, e))
}

pub async fn read_resource(
    client: &reqwest::Client,
    base_url: &str,
    uri: &str,
) -> Result<ResourceContent, String> {
    let text = match uri {
        "rekuiper://rules" => fetch_endpoint(client, base_url, "/rules").await?,
        "rekuiper://streams" => fetch_endpoint(client, base_url, "/streams").await?,
        "rekuiper://tables" => fetch_endpoint(client, base_url, "/tables").await?,
        "rekuiper://connections" => fetch_endpoint(client, base_url, "/connections").await?,
        "rekuiper://udfs" => fetch_endpoint(client, base_url, "/udf/javascript").await?,
        "rekuiper://plugins" => fetch_endpoint(client, base_url, "/plugins/sources").await?,
        "rekuiper://plugins/wasm" => fetch_endpoint(client, base_url, "/plugins/wasm").await?,
        "rekuiper://configs" => fetch_endpoint(client, base_url, "/configs").await?,
        "rekuiper://metadata/sources" => {
            fetch_endpoint(client, base_url, "/metadata/sources").await?
        }
        "rekuiper://metadata/sinks" => fetch_endpoint(client, base_url, "/metadata/sinks").await?,
        "rekuiper://metadata/functions" => {
            fetch_endpoint(client, base_url, "/metadata/functions").await?
        }
        "rekuiper://schemas/rabbitmq" => serde_json::to_string_pretty(&json!({
            "connector": "rabbitmq",
            "type": "sink",
            "properties": {
                "server": "amqp://guest:guest@localhost:5672/%2f",
                "exchange": "events.topic",
                "routingKey": "telemetry.processed",
                "durable": true,
                "autoDelete": false,
                "deliveryMode": 2,
                "expiration": "60000"
            }
        }))
        .unwrap_or_default(),
        "rekuiper://schemas/parquet" => serde_json::to_string_pretty(&json!({
            "connector": "file",
            "format": "parquet",
            "fileType": "parquet",
            "path": "data/parquet/telemetry.parquet",
            "properties": {
                "compression": "snappy",
                "rowGroupSize": 10000,
                "hasHeader": true
            }
        }))
        .unwrap_or_default(),
        "rekuiper://schemas/edgex" => serde_json::to_string_pretty(&json!({
            "connector": "edgex",
            "properties": {
                "protocol": "tcp",
                "server": "localhost",
                "port": 59880,
                "port_alt": 59881,
                "topic": "edgex/events/#",
                "type": "redis"
            }
        }))
        .unwrap_or_default(),
        "rekuiper://schemas/influx" => serde_json::to_string_pretty(&json!({
            "influx_v1": {
                "addr": "http://127.0.0.1:8086",
                "database": "telemetry",
                "measurement": "sensor_readings",
                "username": "admin",
                "password": "password",
                "precision": "ms",
                "tags": {
                    "device": "{{.device_id}}",
                    "location": "factory_floor"
                },
                "fields": ["temperature", "humidity", "vibration"],
                "tsFieldName": "ts"
            },
            "influx_v2": {
                "addr": "http://127.0.0.1:8086",
                "org": "my_org",
                "bucket": "iot_bucket",
                "token": "secret_token",
                "measurement": "sensor_readings",
                "precision": "ms",
                "tags": {
                    "device": "{{.device_id}}"
                },
                "tsFieldName": "ts"
            }
        }))
        .unwrap_or_default(),
        "rekuiper://schemas/tdengine" => serde_json::to_string_pretty(&json!({
            "host": "127.0.0.1",
            "port": 6041,
            "user": "root",
            "password": "password",
            "database": "power",
            "table": "m_{{.deviceId}}",
            "sTable": "meters",
            "tagFields": ["deviceId"],
            "fields": ["temp", "voltage", "current"],
            "tsFieldName": "ts",
            "provideTs": true
        }))
        .unwrap_or_default(),
        "rekuiper://schemas/iotdb" => serde_json::to_string_pretty(&json!({
            "addr": "http://127.0.0.1:18080",
            "username": "root",
            "password": "root",
            "device": "root.factory.{{.deviceId}}",
            "measurements": ["temperature", "humidity", "pressure"],
            "tsFieldName": "ts"
        }))
        .unwrap_or_default(),
        "rekuiper://schemas/image" => serde_json::to_string_pretty(&json!({
            "path": "/tmp/images/{{.deviceId}}",
            "format": "png",
            "imageField": "image",
            "fileName": "frame_{{.deviceId}}_{{.ts}}.png",
            "maxAge": 72,
            "maxCount": 1000
        }))
        .unwrap_or_default(),
        "rekuiper://schemas/random" => serde_json::to_string_pretty(&json!({
            "data": [
                {"temperature": 22.5, "humidity": 50.2},
                {"temperature": 25.1, "humidity": 48.9}
            ],
            "interval": 1000,
            "loop": true
        }))
        .unwrap_or_default(),
        "rekuiper://schemas/zmq" => serde_json::to_string_pretty(&json!({
            "server": "tcp://127.0.0.1:5563",
            "topic": "events",
            "mode": "connect",
            "sendSingle": true
        }))
        .unwrap_or_default(),
        "rekuiper://metrics" => {
            let url = format!("{}/rules", base_url.trim_end_matches('/'));
            let resp = client.get(&url).send().await;
            let rule_count = match resp {
                Ok(r) => r.json::<Vec<String>>().await.map(|v| v.len()).unwrap_or(0),
                Err(_) => 0,
            };
            let metrics = json!({
                "engine": "rekuiper",
                "rules_active": rule_count,
                "protocol": "Model Context Protocol (MCP)",
                "status": "online"
            });
            serde_json::to_string_pretty(&metrics).unwrap_or_default()
        }
        other => return Err(format!("Resource not found: '{}'", other)),
    };

    Ok(ResourceContent {
        uri: uri.to_string(),
        mime_type: Some("application/json".to_string()),
        text,
    })
}
