use rekuiper_connectors::{
    modbus_crc16, modbus_read_snapshot, opcua_read_snapshot, ModbusConfig, ModbusRegister,
    ModbusSink, ModbusSinkConfig, OpcUaConfig, OpcUaNodeMapping, OpcUaSink, OpcUaSinkConfig,
};
use serde_json::json;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

// ---------------------------------------------------------------------------
// Mock Modbus TCP Server
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

                        // Exception test trigger: address 999
                        if n >= 10 {
                            let start_addr = u16::from_be_bytes([buf[8], buf[9]]);
                            if start_addr == 999 {
                                // Modbus Exception 0x02: Illegal Data Address
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
                            0x01 | 0x02 => {
                                let count = if n >= 12 {
                                    u16::from_be_bytes([buf[10], buf[11]])
                                } else {
                                    1
                                };
                                let byte_count = count.div_ceil(8) as u8;
                                let mut resp = Vec::new();
                                resp.extend_from_slice(&tid.to_be_bytes());
                                resp.extend_from_slice(&proto.to_be_bytes());
                                resp.extend_from_slice(&((3 + byte_count as u16).to_be_bytes()));
                                resp.push(unit_id);
                                resp.push(fc);
                                resp.push(byte_count);
                                resp.extend(vec![0x01; byte_count as usize]);
                                let _ = socket.write_all(&resp).await;
                            }
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
                                        0 => resp.extend_from_slice(&0x41CCu16.to_be_bytes()),
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
                            0x0F | 0x10 => {
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

// ---------------------------------------------------------------------------
// Mock OPC-UA Server
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

                                    let is_write = if type_id_encoding == 0x01 {
                                        let type_id = u16::from_le_bytes([buf[22], buf[23]]);
                                        type_id == 673
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
                                        msg_payload.extend_from_slice(&36.6f32.to_le_bytes());

                                        let total_len = 8 + msg_payload.len() as u32;
                                        let mut resp = Vec::new();
                                        resp.extend_from_slice(b"MSGF");
                                        resp.extend_from_slice(&total_len.to_le_bytes());
                                        resp.extend_from_slice(&msg_payload);
                                        let _ = socket.write_all(&resp).await;
                                    } else if is_write {
                                        let mut msg_payload = Vec::new();
                                        msg_payload.extend_from_slice(&1u32.to_le_bytes());
                                        msg_payload.extend_from_slice(&1u32.to_le_bytes());
                                        msg_payload.extend_from_slice(&3u32.to_le_bytes());
                                        msg_payload.extend_from_slice(&3u32.to_le_bytes());
                                        msg_payload.push(0x01);
                                        msg_payload.push(0x00);
                                        msg_payload.extend_from_slice(&676u16.to_le_bytes());
                                        msg_payload.extend_from_slice(&0u64.to_le_bytes());
                                        msg_payload.extend_from_slice(&3u32.to_le_bytes());
                                        msg_payload.extend_from_slice(&0u32.to_le_bytes());

                                        msg_payload.extend_from_slice(&1i32.to_le_bytes());
                                        msg_payload.extend_from_slice(&0u32.to_le_bytes());

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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn test_modbus_crc16_standard_vector() {
    let data = [0x01, 0x03, 0x00, 0x00, 0x00, 0x0A];
    let crc = modbus_crc16(&data);
    assert_eq!(crc, 0xCDC5);
}

#[tokio::test]
async fn test_modbus_tcp_snapshot_and_decoding() {
    let server = MockModbusServer::start().await;
    let endpoint = format!("127.0.0.1:{}", server.port);

    let config = ModbusConfig {
        endpoint: endpoint.clone(),
        transport: "tcp".to_string(),
        unit_id: 1,
        interval_ms: 100,
        timeout_ms: 2000,
        registers: vec![
            ModbusRegister {
                name: "temp".to_string(),
                function: "holding".to_string(),
                address: 0,
                data_type: "float32".to_string(),
                byte_order: Some("ABCD".to_string()),
                scale: Some(1.0),
                offset: Some(0.0),
                ..Default::default()
            },
            ModbusRegister {
                name: "speed".to_string(),
                function: "holding".to_string(),
                address: 2,
                data_type: "uint16".to_string(),
                byte_order: Some("ABCD".to_string()),
                scale: Some(0.1),
                offset: Some(5.0),
                ..Default::default()
            },
        ],
        ..Default::default()
    };

    let snapshot = modbus_read_snapshot(&config)
        .await
        .expect("snapshot read failed");
    assert!(snapshot.contains_key("temp"));
    assert!(snapshot.contains_key("speed"));

    let temp = snapshot.get("temp").unwrap().as_f64().unwrap();
    assert!((temp - 25.5).abs() < 0.001);

    let speed = snapshot.get("speed").unwrap().as_f64().unwrap();
    assert!((speed - 15.0).abs() < 0.001);
}

#[tokio::test]
async fn test_modbus_sink_write_coil_and_register() {
    let server = MockModbusServer::start().await;
    let endpoint = format!("127.0.0.1:{}", server.port);

    let sink_cfg = ModbusSinkConfig {
        endpoint: endpoint.clone(),
        function: "coil".to_string(),
        register_address: 0,
        field: Some("status".to_string()),
        ..Default::default()
    };
    let sink = ModbusSink::new(sink_cfg);

    let mut record = HashMap::new();
    record.insert("status".to_string(), json!(true));
    let res = sink.write_record(&record).await;
    assert!(res.is_ok());

    let reg_cfg = ModbusSinkConfig {
        endpoint,
        function: "holding".to_string(),
        register_address: 10,
        field: Some("target_temp".to_string()),
        ..Default::default()
    };
    let reg_sink = ModbusSink::new(reg_cfg);

    let mut reg_rec = HashMap::new();
    reg_rec.insert("target_temp".to_string(), json!(42));
    let reg_res = reg_sink.write_record(&reg_rec).await;
    assert!(reg_res.is_ok());
}

#[tokio::test]
async fn test_modbus_exception_permanent_error() {
    let server = MockModbusServer::start().await;
    let endpoint = format!("127.0.0.1:{}", server.port);

    let config = ModbusConfig {
        endpoint,
        registers: vec![ModbusRegister {
            name: "illegal".to_string(),
            function: "holding".to_string(),
            address: 999,
            data_type: "uint16".to_string(),
            ..Default::default()
        }],
        ..Default::default()
    };

    let res = modbus_read_snapshot(&config).await;
    assert!(res.is_err());
    let err_msg = res.unwrap_err().to_string();
    assert!(
        err_msg.contains("Illegal Data Address"),
        "expected exception message, got: {}",
        err_msg
    );
}

#[tokio::test]
async fn test_opcua_snapshot_and_decoding() {
    let server = MockOpcUaServer::start().await;
    let endpoint = format!("opc.tcp://127.0.0.1:{}", server.port);

    let config = OpcUaConfig {
        endpoint,
        nodes: vec![OpcUaNodeMapping {
            name: "temp".to_string(),
            node_id: "ns=2;s=Temperature".to_string(),
        }],
        ..Default::default()
    };

    let snapshot = opcua_read_snapshot(&config)
        .await
        .expect("opcua snapshot failed");
    assert!(snapshot.contains_key("temp"));
    let temp = snapshot.get("temp").unwrap().as_f64().unwrap();
    assert!((temp - 36.6).abs() < 0.01);
}

#[tokio::test]
async fn test_opcua_sink_write() {
    let server = MockOpcUaServer::start().await;
    let endpoint = format!("opc.tcp://127.0.0.1:{}", server.port);

    let sink_cfg = OpcUaSinkConfig {
        endpoint,
        node_id: "ns=2;s=SetPoint".to_string(),
        field: Some("target".to_string()),
        ..Default::default()
    };
    let sink = OpcUaSink::new(sink_cfg);

    let mut record = HashMap::new();
    record.insert("target".to_string(), json!(50.0));
    let res = sink.write_record(&record).await;
    assert!(res.is_ok());
}
