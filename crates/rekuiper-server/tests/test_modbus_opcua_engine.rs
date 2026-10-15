use rekuiper_conf::KuiperConfig;
use rekuiper_core::manager::{RuleManager, StreamManager, TableManager};
use rekuiper_core::StreamBus;
use rekuiper_server::routes::{create_router, AppState};
use serde_json::json;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

// ---------------------------------------------------------------------------
// Mock Modbus TCP Server for Engine Tests
// ---------------------------------------------------------------------------
struct MockModbusServer {
    port: u16,
    running: Arc<AtomicBool>,
}

impl MockModbusServer {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();

        tokio::spawn(async move {
            while running_clone.load(Ordering::Relaxed) {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };

                tokio::spawn(async move {
                    let mut buf = [0u8; 1024];
                    loop {
                        let n = match socket.read(&mut buf).await {
                            Ok(0) => break,
                            Ok(n) => n,
                            Err(_) => break,
                        };

                        if n < 7 {
                            continue;
                        }

                        let tid = u16::from_be_bytes([buf[0], buf[1]]);
                        let proto = u16::from_be_bytes([buf[2], buf[3]]);
                        let unit_id = buf[6];
                        let fc = buf[7];

                        // Trigger exception 0x02 on register 999
                        if n >= 10 {
                            let start_addr = u16::from_be_bytes([buf[8], buf[9]]);
                            if start_addr == 999 {
                                let mut resp = Vec::new();
                                resp.extend_from_slice(&tid.to_be_bytes());
                                resp.extend_from_slice(&proto.to_be_bytes());
                                resp.extend_from_slice(&3u16.to_be_bytes());
                                resp.push(unit_id);
                                resp.push(fc | 0x80);
                                resp.push(0x02);
                                let _ = socket.write_all(&resp).await;
                                continue;
                            }
                        }

                        match fc {
                            0x03 | 0x04 => {
                                let count = if n >= 12 {
                                    u16::from_be_bytes([buf[10], buf[11]])
                                } else {
                                    1
                                };
                                let byte_count = (count * 2) as u8;
                                let mut resp = Vec::new();
                                resp.extend_from_slice(&tid.to_be_bytes());
                                resp.extend_from_slice(&proto.to_be_bytes());
                                resp.extend_from_slice(&((3 + byte_count as u16).to_be_bytes()));
                                resp.push(unit_id);
                                resp.push(fc);
                                resp.push(byte_count);

                                for i in 0..count {
                                    match i {
                                        0 => resp.extend_from_slice(&0x41CCu16.to_be_bytes()), // 25.5 float32
                                        1 => resp.extend_from_slice(&0x0000u16.to_be_bytes()),
                                        2 => resp.extend_from_slice(&100u16.to_be_bytes()),
                                        _ => resp.extend_from_slice(&0u16.to_be_bytes()),
                                    }
                                }
                                let _ = socket.write_all(&resp).await;
                            }
                            0x05 | 0x06 => {
                                let _ = socket.write_all(&buf[..n]).await;
                            }
                            0x10 => {
                                let mut resp = Vec::new();
                                resp.extend_from_slice(&tid.to_be_bytes());
                                resp.extend_from_slice(&proto.to_be_bytes());
                                resp.extend_from_slice(&6u16.to_be_bytes());
                                resp.push(unit_id);
                                resp.push(fc);
                                resp.extend_from_slice(&buf[8..12]);
                                let _ = socket.write_all(&resp).await;
                            }
                            _ => {
                                let mut resp = Vec::new();
                                resp.extend_from_slice(&tid.to_be_bytes());
                                resp.extend_from_slice(&proto.to_be_bytes());
                                resp.extend_from_slice(&3u16.to_be_bytes());
                                resp.push(unit_id);
                                resp.push(fc | 0x80);
                                resp.push(0x01);
                                let _ = socket.write_all(&resp).await;
                            }
                        }
                    }
                });
            }
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        Self { port, running }
    }
}

impl Drop for MockModbusServer {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
    }
}

async fn spawn_test_server() -> (String, tokio::task::JoinHandle<()>, AppState) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let stream_bus = StreamBus::new();
    let stream_manager = StreamManager::new();
    let rule_manager = RuleManager::new(stream_bus.clone());

    let mut config = KuiperConfig::default();
    config.basic.enable_private_net = true;

    let state = AppState::new(
        "test_modbus_opcua_engine".to_string(),
        config,
        stream_manager,
        TableManager::new(),
        rule_manager,
        stream_bus,
    );

    let app = create_router(state.clone());
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (format!("http://{}", local_addr), handle, state)
}

#[tokio::test]
async fn test_modbus_stream_sql_rule_execution() {
    let modbus_server = MockModbusServer::start().await;
    let (base_url, _handle, state) = spawn_test_server().await;
    let client = reqwest::Client::new();

    let registers_json = json!([
        {
            "name": "temperature",
            "address": 0,
            "function": "holding",
            "dataType": "float32",
            "byteOrder": "ABCD"
        },
        {
            "name": "speed",
            "address": 2,
            "function": "holding",
            "dataType": "uint16"
        }
    ]).to_string();

    let create_stream_sql = format!(
        "CREATE STREAM modbus_in () WITH (TYPE=\"modbus\", ENDPOINT=\"127.0.0.1:{}\", INTERVAL=\"50ms\", REGISTERS='{}')",
        modbus_server.port,
        registers_json
    );

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({ "sql": create_stream_sql }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Create memory sink stream to capture output
    let mut sink_rx = state.stream_bus.subscribe("modbus_out");

    let rule_json = json!({
        "id": "rule_modbus_calc",
        "sql": "SELECT temperature * 2 AS double_temp, speed FROM modbus_in",
        "actions": [
            {
                "memory": {
                    "topic": "modbus_out"
                }
            }
        ]
    });

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&rule_json)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Wait for output record from rule
    let rec = tokio::time::timeout(tokio::time::Duration::from_secs(3), sink_rx.recv())
        .await
        .expect("timed out waiting for modbus rule output")
        .expect("channel closed");

    let double_temp = rec.data.get("double_temp").and_then(|v| v.as_f64()).unwrap();
    // 25.5 * 2 = 51.0
    assert!((double_temp - 51.0).abs() < 0.01);

    let speed = rec.data.get("speed").and_then(|v| v.as_i64()).unwrap();
    assert_eq!(speed, 100);
}

#[tokio::test]
async fn test_modbus_sink_exception_routes_to_dlq() {
    let modbus_server = MockModbusServer::start().await;
    let (base_url, _handle, state) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 1. Create source memory stream
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM in_stream () WITH (TYPE=\"memory\", FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 2. Create rule writing to Modbus register 999 (which triggers exception 0x02)
    let rule_json = json!({
        "id": "rule_modbus_dlq",
        "sql": "SELECT val FROM in_stream",
        "actions": [
            {
                "modbus": {
                    "endpoint": format!("127.0.0.1:{}", modbus_server.port),
                    "function": "holding",
                    "registerAddress": 999,
                    "field": "val"
                }
            }
        ],
        "options": {
            "onError": "dlq",
            "dlq": {
                "action": {
                    "memory": {
                        "topic": "dlq_modbus"
                    }
                }
            }
        }
    });

    let mut dlq_rx = state.stream_bus.subscribe("dlq_modbus");

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&rule_json)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // Publish input message
    let in_tx = state.stream_bus.get_or_create("in_stream");
    let mut data = std::collections::HashMap::new();
    data.insert("val".to_string(), json!(123));
    let _ = in_tx.send(rekuiper_core::model::StreamRecord::new(data)).await;

    // Wait for DLQ record
    let dlq_rec = tokio::time::timeout(tokio::time::Duration::from_secs(3), dlq_rx.recv())
        .await
        .expect("timed out waiting for DLQ modbus record")
        .expect("channel closed");

    let stage = dlq_rec.data.get("stage").and_then(|v| v.as_str()).unwrap();
    assert_eq!(stage, "SinkDelivery");

    let sink_type = dlq_rec.data.get("sink_type").and_then(|v| v.as_str()).unwrap();
    assert_eq!(sink_type, "modbus");

    let error_msg = dlq_rec.data.get("error").and_then(|v| v.as_str()).unwrap();
    assert!(error_msg.contains("Illegal Data Address") || error_msg.contains("0x02"));
}

// ---------------------------------------------------------------------------
// Mock OPC-UA Server for Engine Tests
// ---------------------------------------------------------------------------
struct MockOpcUaServer {
    port: u16,
    running: Arc<AtomicBool>,
}

impl MockOpcUaServer {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();

        tokio::spawn(async move {
            while running_clone.load(Ordering::Relaxed) {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };

                tokio::spawn(async move {
                    let mut buf = [0u8; 2048];
                    loop {
                        let n = match socket.read(&mut buf).await {
                            Ok(0) => break,
                            Ok(n) => n,
                            Err(_) => break,
                        };
                        if n < 8 {
                            continue;
                        }

                        let msg_type = &buf[0..3];
                        match msg_type {
                            b"HEL" => {
                                let mut ack = Vec::new();
                                ack.extend_from_slice(b"ACKF");
                                ack.extend_from_slice(&28u32.to_le_bytes());
                                ack.extend_from_slice(&0u32.to_le_bytes());
                                ack.extend_from_slice(&65536u32.to_le_bytes());
                                ack.extend_from_slice(&65536u32.to_le_bytes());
                                ack.extend_from_slice(&16777216u32.to_le_bytes());
                                ack.extend_from_slice(&5000u32.to_le_bytes());
                                let _ = socket.write_all(&ack).await;
                            }
                            b"OPN" => {
                                let mut opn_payload = Vec::new();
                                opn_payload.extend_from_slice(&1u32.to_le_bytes());
                                opn_payload.extend_from_slice(&1u32.to_le_bytes());
                                opn_payload.extend_from_slice(&1u32.to_le_bytes());
                                opn_payload.extend_from_slice(&0u64.to_le_bytes());
                                opn_payload.extend_from_slice(&3600000u32.to_le_bytes());
                                opn_payload.extend_from_slice(&1u32.to_le_bytes());
                                opn_payload.extend_from_slice(&1u32.to_le_bytes());
                                opn_payload.push(0x01);
                                opn_payload.push(0x00);
                                opn_payload.extend_from_slice(&449u16.to_le_bytes());
                                opn_payload.extend_from_slice(&0u64.to_le_bytes());
                                opn_payload.extend_from_slice(&1u32.to_le_bytes());
                                opn_payload.extend_from_slice(&0u32.to_le_bytes());

                                let total_len = 8 + opn_payload.len() as u32;
                                let mut resp = Vec::new();
                                resp.extend_from_slice(b"OPNF");
                                resp.extend_from_slice(&total_len.to_le_bytes());
                                resp.extend_from_slice(&opn_payload);
                                let _ = socket.write_all(&resp).await;
                            }
                            b"MSG" => {
                                if n >= 24 {
                                    let type_id_encoding = buf[20];
                                    let is_read = if type_id_encoding == 0x01 {
                                        let type_id = u16::from_le_bytes([buf[22], buf[23]]);
                                        type_id == 631
                                    } else {
                                        false
                                    };

                                    if is_read {
                                        let mut msg_payload = Vec::new();
                                        msg_payload.extend_from_slice(&1u32.to_le_bytes());
                                        msg_payload.extend_from_slice(&1u32.to_le_bytes());
                                        msg_payload.extend_from_slice(&2u32.to_le_bytes());
                                        msg_payload.extend_from_slice(&2u32.to_le_bytes());
                                        msg_payload.push(0x01);
                                        msg_payload.push(0x00);
                                        msg_payload.extend_from_slice(&634u16.to_le_bytes());
                                        msg_payload.extend_from_slice(&0u64.to_le_bytes());
                                        msg_payload.extend_from_slice(&2u32.to_le_bytes());
                                        msg_payload.extend_from_slice(&0u32.to_le_bytes());

                                        msg_payload.extend_from_slice(&1i32.to_le_bytes());
                                        msg_payload.push(0x01);
                                        msg_payload.push(0x0A);
                                        msg_payload.extend_from_slice(&45.0f32.to_le_bytes());

                                        let total_len = 8 + msg_payload.len() as u32;
                                        let mut resp = Vec::new();
                                        resp.extend_from_slice(b"MSGF");
                                        resp.extend_from_slice(&total_len.to_le_bytes());
                                        resp.extend_from_slice(&msg_payload);
                                        let _ = socket.write_all(&resp).await;
                                    }
                                }
                            }
                            _ => break,
                        }
                    }
                });
            }
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        Self { port, running }
    }
}

impl Drop for MockOpcUaServer {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
    }
}

#[tokio::test]
async fn test_opcua_stream_and_sink_rule_execution() {
    let opcua_server = MockOpcUaServer::start().await;
    let (base_url, _handle, state) = spawn_test_server().await;
    let client = reqwest::Client::new();

    let nodes_json = json!([
        {
            "name": "temperature",
            "nodeId": "ns=2;s=Temperature"
        }
    ]).to_string();

    let create_stream_sql = format!(
        "CREATE STREAM opcua_in () WITH (TYPE=\"opcua\", ENDPOINT=\"opc.tcp://127.0.0.1:{}\", INTERVAL=\"50ms\", NODES='{}')",
        opcua_server.port,
        nodes_json
    );

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({ "sql": create_stream_sql }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let mut sink_rx = state.stream_bus.subscribe("opcua_out");

    let rule_json = json!({
        "id": "rule_opcua_calc",
        "sql": "SELECT temperature + 5.0 AS adjusted_temp FROM opcua_in",
        "actions": [
            {
                "memory": {
                    "topic": "opcua_out"
                }
            }
        ]
    });

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&rule_json)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let rec = tokio::time::timeout(tokio::time::Duration::from_secs(3), sink_rx.recv())
        .await
        .expect("timed out waiting for opcua rule output")
        .expect("channel closed");

    let adjusted = rec.data.get("adjusted_temp").and_then(|v| v.as_f64()).unwrap();
    // 45.0 + 5.0 = 50.0
    assert!((adjusted - 50.0).abs() < 0.01);
}

#[tokio::test]
async fn test_modbus_table_lookup_join() {
    let modbus_server = MockModbusServer::start().await;
    let (base_url, _handle, state) = spawn_test_server().await;
    let client = reqwest::Client::new();

    let registers_json = json!([
        {
            "name": "modbus_temp",
            "address": 0,
            "function": "holding",
            "dataType": "float32",
            "byteOrder": "ABCD"
        }
    ]).to_string();

    // 1. Create source stream
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({ "sql": "CREATE STREAM sensor_stream () WITH (TYPE=\"memory\", FORMAT=\"json\")" }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 2. Create Modbus lookup table
    let create_table_sql = format!(
        "CREATE TABLE plc_table () WITH (TYPE=\"modbus\", ENDPOINT=\"127.0.0.1:{}\", REGISTERS='{}')",
        modbus_server.port,
        registers_json
    );
    let resp = client
        .post(format!("{}/tables", base_url))
        .json(&json!({ "sql": create_table_sql }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let mut out_rx = state.stream_bus.subscribe("join_out");

    // 3. Create join rule
    let rule_json = json!({
        "id": "rule_modbus_join",
        "sql": "SELECT sensor_stream.device, plc_table.modbus_temp FROM sensor_stream JOIN plc_table ON sensor_stream.device = 'sensor1'",
        "actions": [
            {
                "memory": {
                    "topic": "join_out"
                }
            }
        ]
    });

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&rule_json)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // Send input stream record
    let in_tx = state.stream_bus.get_or_create("sensor_stream");
    let mut data = std::collections::HashMap::new();
    data.insert("device".to_string(), json!("sensor1"));
    let _ = in_tx.send(rekuiper_core::model::StreamRecord::new(data)).await;

    // Receive joined record
    let out_rec = tokio::time::timeout(tokio::time::Duration::from_secs(3), out_rx.recv())
        .await
        .expect("timed out waiting for join output")
        .expect("channel closed");

    let temp = out_rec.data.get("modbus_temp").and_then(|v| v.as_f64()).unwrap();
    assert!((temp - 25.5).abs() < 0.01);
}
