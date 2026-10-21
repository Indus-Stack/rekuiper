// Copyright 2024-2026 rekuiper Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use anyhow::{bail, Context, Result};
use rekuiper_core::model::{RecordMeta, StreamRecord};
use rekuiper_core::StreamSender;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;

pub const DEFAULT_OPCUA_ENDPOINT: &str = "opc.tcp://127.0.0.1:4840";
pub const DEFAULT_OPCUA_PUBLISHING_INTERVAL_MS: u64 = 1000;
pub const DEFAULT_OPCUA_TIMEOUT_MS: u64 = 2000;

fn default_endpoint() -> String {
    DEFAULT_OPCUA_ENDPOINT.to_string()
}

fn default_mode() -> String {
    "subscribe".to_string()
}

fn default_interval_ms() -> u64 {
    DEFAULT_OPCUA_PUBLISHING_INTERVAL_MS
}

fn default_timeout_ms() -> u64 {
    DEFAULT_OPCUA_TIMEOUT_MS
}

fn default_security_policy() -> String {
    "None".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpcUaNodeMapping {
    pub name: String,
    pub node_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpcUaConfig {
    #[serde(default = "default_endpoint")]
    pub endpoint: String,
    #[serde(default)]
    pub server: Option<String>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default = "default_mode")]
    pub mode: String, // "subscribe" or "poll"
    #[serde(default = "default_interval_ms")]
    pub interval_ms: u64,
    #[serde(default = "default_interval_ms")]
    pub publishing_interval_ms: u64,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default = "default_security_policy")]
    pub security_policy: String,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub include_metadata: bool,
    #[serde(default)]
    pub nodes: Vec<OpcUaNodeMapping>,
}

impl Default for OpcUaConfig {
    fn default() -> Self {
        Self {
            endpoint: default_endpoint(),
            server: None,
            url: None,
            mode: default_mode(),
            interval_ms: default_interval_ms(),
            publishing_interval_ms: default_interval_ms(),
            timeout_ms: default_timeout_ms(),
            security_policy: default_security_policy(),
            username: None,
            password: None,
            include_metadata: false,
            nodes: Vec::new(),
        }
    }
}

impl OpcUaConfig {
    pub fn endpoint(&self) -> &str {
        if let Some(ref s) = self.server {
            if !s.trim().is_empty() {
                return s.trim();
            }
        }
        if let Some(ref u) = self.url {
            if !u.trim().is_empty() {
                return u.trim();
            }
        }
        if !self.endpoint.trim().is_empty() {
            return self.endpoint.trim();
        }
        DEFAULT_OPCUA_ENDPOINT
    }

    pub fn is_poll_mode(&self) -> bool {
        self.mode.eq_ignore_ascii_case("poll")
    }

    pub fn polling_interval(&self) -> Duration {
        Duration::from_millis(self.interval_ms.max(10))
    }
}

/// Parsed representation of an OPC UA NodeId.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum OpcUaNodeId {
    Numeric { namespace: u16, identifier: u32 },
    String { namespace: u16, identifier: String },
    Guid { namespace: u16, identifier: String },
    Opaque { namespace: u16, identifier: Vec<u8> },
}

impl OpcUaNodeId {
    pub fn parse(s: &str) -> Self {
        let trimmed = s.trim();
        let parts: Vec<&str> = trimmed.split(';').collect();

        let mut ns: u16 = 0;
        let mut id_str = trimmed;

        for part in &parts {
            let part = part.trim();
            if let Some(rest) = part.strip_prefix("ns=") {
                ns = rest.parse::<u16>().unwrap_or(0);
            } else if part.starts_with("i=")
                || part.starts_with("s=")
                || part.starts_with("g=")
                || part.starts_with("b=")
            {
                id_str = part;
            }
        }

        if let Some(rest) = id_str.strip_prefix("i=") {
            let num = rest.parse::<u32>().unwrap_or(0);
            OpcUaNodeId::Numeric {
                namespace: ns,
                identifier: num,
            }
        } else if let Some(rest) = id_str.strip_prefix("s=") {
            OpcUaNodeId::String {
                namespace: ns,
                identifier: rest.to_string(),
            }
        } else if let Some(rest) = id_str.strip_prefix("g=") {
            OpcUaNodeId::Guid {
                namespace: ns,
                identifier: rest.to_string(),
            }
        } else if let Some(rest) = id_str.strip_prefix("b=") {
            OpcUaNodeId::Opaque {
                namespace: ns,
                identifier: rest.as_bytes().to_vec(),
            }
        } else if let Ok(num) = trimmed.parse::<u32>() {
            OpcUaNodeId::Numeric {
                namespace: 0,
                identifier: num,
            }
        } else {
            OpcUaNodeId::String {
                namespace: ns,
                identifier: trimmed.to_string(),
            }
        }
    }

    pub fn to_string_repr(&self) -> String {
        match self {
            OpcUaNodeId::Numeric {
                namespace,
                identifier,
            } => {
                if *namespace == 0 {
                    format!("i={}", identifier)
                } else {
                    format!("ns={};i={}", namespace, identifier)
                }
            }
            OpcUaNodeId::String {
                namespace,
                identifier,
            } => {
                if *namespace == 0 {
                    format!("s={}", identifier)
                } else {
                    format!("ns={};s={}", namespace, identifier)
                }
            }
            OpcUaNodeId::Guid {
                namespace,
                identifier,
            } => {
                format!("ns={};g={}", namespace, identifier)
            }
            OpcUaNodeId::Opaque {
                namespace,
                identifier,
            } => {
                format!("ns={};b={:?}", namespace, identifier)
            }
        }
    }
}

/// Native pure-Rust OPC UA Variant values.
#[derive(Debug, Clone, PartialEq)]
pub enum OpcUaVariant {
    Null,
    Boolean(bool),
    SByte(i8),
    Byte(u8),
    Int16(i16),
    UInt16(u16),
    Int32(i32),
    UInt32(u32),
    Int64(i64),
    UInt64(u64),
    Float(f32),
    Double(f64),
    String(String),
    DateTime(i64),
    ByteString(Vec<u8>),
}

impl OpcUaVariant {
    pub fn to_json_value(&self, node_name: &str) -> Result<Value, String> {
        match self {
            OpcUaVariant::Null => Ok(Value::Null),
            OpcUaVariant::Boolean(b) => Ok(Value::Bool(*b)),
            OpcUaVariant::SByte(i) => Ok(json!(*i)),
            OpcUaVariant::Byte(u) => Ok(json!(*u)),
            OpcUaVariant::Int16(i) => Ok(json!(*i)),
            OpcUaVariant::UInt16(u) => Ok(json!(*u)),
            OpcUaVariant::Int32(i) => Ok(json!(*i)),
            OpcUaVariant::UInt32(u) => Ok(json!(*u)),
            OpcUaVariant::Int64(i) => Ok(json!(*i)),
            OpcUaVariant::UInt64(u) => Ok(json!(*u)),
            OpcUaVariant::Float(f) => {
                if !f.is_finite() {
                    return Err(format!(
                        "OPC-UA node '{}' yielded non-finite float ({:?})",
                        node_name, f
                    ));
                }
                Ok(json!(*f as f64))
            }
            OpcUaVariant::Double(d) => {
                if !d.is_finite() {
                    return Err(format!(
                        "OPC-UA node '{}' yielded non-finite double ({:?})",
                        node_name, d
                    ));
                }
                Ok(json!(*d))
            }
            OpcUaVariant::String(s) => Ok(Value::String(s.clone())),
            OpcUaVariant::DateTime(dt) => Ok(json!(*dt)),
            OpcUaVariant::ByteString(b) => Ok(Value::String(
                b.iter().map(|byte| format!("{:02x}", byte)).collect(),
            )),
        }
    }
}

/// DataValue returned from OPC UA Read or Subscription Notification.
#[derive(Debug, Clone)]
pub struct OpcUaDataValue {
    pub value: OpcUaVariant,
    pub status_code: u32,
    pub source_timestamp: Option<i64>,
}

/// Minimal Async pure-Rust Tokio OPC UA TCP binary protocol client.
pub struct OpcUaClient {
    endpoint: String,
    timeout: Duration,
    stream: Option<TcpStream>,
    channel_id: u32,
    token_id: u32,
    request_handle: Arc<AtomicU32>,
    sequence_number: Arc<AtomicU32>,
}

impl OpcUaClient {
    pub fn new(endpoint: &str, timeout_ms: u64) -> Self {
        Self {
            endpoint: endpoint.to_string(),
            timeout: Duration::from_millis(timeout_ms.max(100)),
            stream: None,
            channel_id: 0,
            token_id: 0,
            request_handle: Arc::new(AtomicU32::new(1)),
            sequence_number: Arc::new(AtomicU32::new(1)),
        }
    }

    pub fn disconnect(&mut self) {
        self.stream = None;
        self.channel_id = 0;
        self.token_id = 0;
    }

    fn parse_host_port(&self) -> Result<(String, u16)> {
        let stripped = self
            .endpoint
            .strip_prefix("opc.tcp://")
            .unwrap_or(&self.endpoint);
        let host_port = stripped.split('/').next().unwrap_or(stripped);
        if let Some((h, p)) = host_port.split_once(':') {
            let port = p.parse::<u16>().unwrap_or(4840);
            Ok((h.to_string(), port))
        } else {
            Ok((host_port.to_string(), 4840))
        }
    }

    pub async fn ensure_connected(&mut self) -> Result<()> {
        if self.stream.is_none() {
            let (host, port) = self.parse_host_port()?;
            let target = format!("{}:{}", host, port);
            let mut stream = tokio::time::timeout(self.timeout, TcpStream::connect(&target))
                .await
                .context("OPC UA TCP connection timed out")?
                .context(format!("Failed to connect to OPC UA endpoint {}", target))?;
            let _ = stream.set_nodelay(true);

            // 1. Send HEL (Hello) message
            let ep_bytes = self.endpoint.as_bytes();
            let ep_len = ep_bytes.len() as u32;
            let msg_len = 8 + 4 + 4 + 4 + 4 + 4 + (4 + ep_len);

            let mut hel = Vec::with_capacity(msg_len as usize);
            hel.extend_from_slice(b"HELF");
            hel.extend_from_slice(&msg_len.to_le_bytes());
            hel.extend_from_slice(&0u32.to_le_bytes()); // ProtocolVersion = 0
            hel.extend_from_slice(&65536u32.to_le_bytes()); // ReceiveBufferSize
            hel.extend_from_slice(&65536u32.to_le_bytes()); // SendBufferSize
            hel.extend_from_slice(&0u32.to_le_bytes()); // MaxMessageSize
            hel.extend_from_slice(&0u32.to_le_bytes()); // MaxChunkCount
            hel.extend_from_slice(&ep_len.to_le_bytes());
            hel.extend_from_slice(ep_bytes);

            stream.write_all(&hel).await?;

            // Read ACK response
            let mut ack_header = [0u8; 8];
            stream.read_exact(&mut ack_header).await?;
            if &ack_header[0..3] != b"ACK" {
                bail!("Expected OPC UA ACK response, got {:?}", &ack_header[0..3]);
            }
            let ack_len =
                u32::from_le_bytes([ack_header[4], ack_header[5], ack_header[6], ack_header[7]])
                    as usize;
            if ack_len > 8 {
                let mut ack_body = vec![0u8; ack_len - 8];
                stream.read_exact(&mut ack_body).await?;
            }

            // 2. Send OPN (OpenSecureChannel) message (SecurityPolicy#None)
            let sec_policy = b"http://opcfoundation.org/UA/SecurityPolicy#None";
            let sec_len = sec_policy.len() as u32;
            let req_handle = self.request_handle.fetch_add(1, Ordering::SeqCst);
            let seq_num = self.sequence_number.fetch_add(1, Ordering::SeqCst);

            // OPN Header (8 bytes) + SecureChannelId (4) + SecurityPolicyUri (4 + len) + SenderCert (-1 = 4 bytes) + ReceiverThumbprint (-1 = 4 bytes)
            // SequenceHeader: SequenceNumber (4) + RequestId (4)
            // TypeId: FourByte NodeId (0x01, 0x00, 446 LE) -> 4 bytes
            // RequestHeader + SecurityTokenRequest parameters
            let mut opn_payload = Vec::new();
            opn_payload.extend_from_slice(&0u32.to_le_bytes()); // SecureChannelId = 0
            opn_payload.extend_from_slice(&sec_len.to_le_bytes());
            opn_payload.extend_from_slice(sec_policy);
            opn_payload.extend_from_slice(&(-1i32).to_le_bytes()); // SenderCert null
            opn_payload.extend_from_slice(&(-1i32).to_le_bytes()); // ReceiverThumbprint null
            opn_payload.extend_from_slice(&seq_num.to_le_bytes());
            opn_payload.extend_from_slice(&req_handle.to_le_bytes());

            // NodeId 446 (OpenSecureChannelRequest)
            opn_payload.push(0x01); // FourByte NodeId
            opn_payload.push(0x00); // Namespace 0
            opn_payload.extend_from_slice(&446u16.to_le_bytes());

            // RequestHeader: AuthToken (NodeId null = 0x00), Timestamp (8 bytes 0), RequestHandle (4), ReturnDiag (4), AuditEntry (4 null), Timeout (4), AdditionalHeader (NodeId null)
            opn_payload.push(0x00); // NodeId null
            opn_payload.extend_from_slice(&0u64.to_le_bytes()); // Timestamp
            opn_payload.extend_from_slice(&req_handle.to_le_bytes());
            opn_payload.extend_from_slice(&0u32.to_le_bytes()); // ReturnDiagnostics = 0
            opn_payload.extend_from_slice(&(-1i32).to_le_bytes()); // AuditEntry null
            opn_payload.extend_from_slice(&10000u32.to_le_bytes()); // TimeoutHint
            opn_payload.push(0x00); // AdditionalHeader null NodeId
            opn_payload.push(0x00); // ExtensionObject null

            // OpenSecureChannel parameters:
            opn_payload.extend_from_slice(&0u32.to_le_bytes()); // ClientProtocolVersion = 0
            opn_payload.extend_from_slice(&1u32.to_le_bytes()); // SecurityMode: None = 1
            opn_payload.extend_from_slice(&0u32.to_le_bytes()); // SecurityTokenRequestType: Issue = 0
            opn_payload.extend_from_slice(&300000u32.to_le_bytes()); // RequestedLifetime = 300,000ms

            let opn_total_len = (8 + opn_payload.len()) as u32;
            let mut opn_msg = Vec::with_capacity(opn_total_len as usize);
            opn_msg.extend_from_slice(b"OPNF");
            opn_msg.extend_from_slice(&opn_total_len.to_le_bytes());
            opn_msg.extend_from_slice(&opn_payload);

            stream.write_all(&opn_msg).await?;

            // Read OPN response
            let mut opn_resp_header = [0u8; 8];
            stream.read_exact(&mut opn_resp_header).await?;
            if &opn_resp_header[0..3] != b"OPN" {
                bail!(
                    "Expected OPC UA OPN response, got {:?}",
                    &opn_resp_header[0..3]
                );
            }
            let resp_len = u32::from_le_bytes([
                opn_resp_header[4],
                opn_resp_header[5],
                opn_resp_header[6],
                opn_resp_header[7],
            ]) as usize;
            if resp_len < 12 {
                bail!("OPC UA OPN response too short ({})", resp_len);
            }
            let mut opn_resp_body = vec![0u8; resp_len - 8];
            stream.read_exact(&mut opn_resp_body).await?;

            let chan_id = u32::from_le_bytes([
                opn_resp_body[0],
                opn_resp_body[1],
                opn_resp_body[2],
                opn_resp_body[3],
            ]);
            self.channel_id = chan_id;
            self.stream = Some(stream);
        }
        Ok(())
    }

    /// Reads configured node values via OPC UA Read Service (NodeId 631).
    pub async fn read_nodes(
        &mut self,
        nodes: &[OpcUaNodeMapping],
    ) -> Result<HashMap<String, OpcUaDataValue>> {
        if nodes.is_empty() {
            return Ok(HashMap::new());
        }

        let timeout = self.timeout;
        let fut = async {
            self.ensure_connected().await?;
            let req_handle = self.request_handle.fetch_add(1, Ordering::SeqCst);
            let seq_num = self.sequence_number.fetch_add(1, Ordering::SeqCst);
            let chan_id = self.channel_id;

            // Build MSG frame for ReadRequest (NodeId 631)
            let mut body = Vec::new();
            // SequenceHeader
            body.extend_from_slice(&seq_num.to_le_bytes());
            body.extend_from_slice(&req_handle.to_le_bytes());

            // NodeId 631 (ReadRequest)
            body.push(0x01); // FourByte NodeId
            body.push(0x00);
            body.extend_from_slice(&631u16.to_le_bytes());

            // RequestHeader
            body.push(0x00); // AuthToken null
            body.extend_from_slice(&0u64.to_le_bytes());
            body.extend_from_slice(&req_handle.to_le_bytes());
            body.extend_from_slice(&0u32.to_le_bytes());
            body.extend_from_slice(&(-1i32).to_le_bytes());
            body.extend_from_slice(&10000u32.to_le_bytes());
            body.push(0x00);
            body.push(0x00);

            // ReadRequest parameters: MaxAge = 0.0 (f64), TimestampsToReturn = Both (2)
            body.extend_from_slice(&0.0f64.to_le_bytes());
            body.extend_from_slice(&2u32.to_le_bytes());

            // Nodes to read array
            let nodes_count = nodes.len() as u32;
            body.extend_from_slice(&nodes_count.to_le_bytes());

            for m in nodes {
                let parsed = OpcUaNodeId::parse(&m.node_id);
                match parsed {
                    OpcUaNodeId::Numeric {
                        namespace,
                        identifier,
                    } => {
                        if namespace == 0 && identifier <= 255 {
                            body.push(0x00); // TwoByte
                            body.push(identifier as u8);
                        } else if namespace <= 255 && identifier <= 65535 {
                            body.push(0x01); // FourByte
                            body.push(namespace as u8);
                            body.extend_from_slice(&(identifier as u16).to_le_bytes());
                        } else {
                            body.push(0x02); // Numeric
                            body.extend_from_slice(&namespace.to_le_bytes());
                            body.extend_from_slice(&identifier.to_le_bytes());
                        }
                    }
                    OpcUaNodeId::String {
                        namespace,
                        identifier,
                    } => {
                        body.push(0x03); // String
                        body.extend_from_slice(&namespace.to_le_bytes());
                        let bytes = identifier.as_bytes();
                        body.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                        body.extend_from_slice(bytes);
                    }
                    OpcUaNodeId::Guid { namespace, .. } => {
                        body.push(0x04); // Guid
                        body.extend_from_slice(&namespace.to_le_bytes());
                        body.extend_from_slice(&[0u8; 16]);
                    }
                    OpcUaNodeId::Opaque {
                        namespace,
                        identifier,
                    } => {
                        body.push(0x05); // ByteString
                        body.extend_from_slice(&namespace.to_le_bytes());
                        body.extend_from_slice(&(identifier.len() as u32).to_le_bytes());
                        body.extend_from_slice(&identifier);
                    }
                }
                // AttributeId: Value = 13 (u32)
                body.extend_from_slice(&13u32.to_le_bytes());
                // IndexRange: null
                body.extend_from_slice(&(-1i32).to_le_bytes());
                // DataEncoding: QualifiedName null
                body.extend_from_slice(&0u16.to_le_bytes());
                body.extend_from_slice(&(-1i32).to_le_bytes());
            }

            let msg_total_len = (8 + 4 + body.len()) as u32;
            let mut msg = Vec::with_capacity(msg_total_len as usize);
            msg.extend_from_slice(b"MSGF");
            msg.extend_from_slice(&msg_total_len.to_le_bytes());
            msg.extend_from_slice(&chan_id.to_le_bytes());
            msg.extend_from_slice(&body);

            let stream = self.stream.as_mut().unwrap();
            stream.write_all(&msg).await?;

            // Read MSG response
            let mut resp_header = [0u8; 8];
            stream.read_exact(&mut resp_header).await?;
            if &resp_header[0..3] != b"MSG" {
                bail!("Expected OPC UA MSG response, got {:?}", &resp_header[0..3]);
            }
            let resp_total_len = u32::from_le_bytes([
                resp_header[4],
                resp_header[5],
                resp_header[6],
                resp_header[7],
            ]) as usize;
            if resp_total_len < 12 {
                bail!("OPC UA MSG response too short ({})", resp_total_len);
            }
            let mut resp_body = vec![0u8; resp_total_len - 8];
            stream.read_exact(&mut resp_body).await?;

            // Parse DataValues from ReadResponse
            parse_read_response(&resp_body[4..], nodes)
        };

        match tokio::time::timeout(timeout, fut).await {
            Ok(Ok(vals)) => Ok(vals),
            Ok(Err(e)) => {
                self.disconnect();
                Err(e)
            }
            Err(_) => {
                self.disconnect();
                bail!("OPC UA read request timed out after {:?}", timeout)
            }
        }
    }

    /// Writes a value to an OPC UA Node via Write Service (NodeId 673).
    pub async fn write_node(
        &mut self,
        node_id_str: &str,
        value: &Value,
    ) -> Result<(), (bool, String)> {
        let timeout = self.timeout;
        let node_id = OpcUaNodeId::parse(node_id_str);

        let fut = async {
            self.ensure_connected()
                .await
                .map_err(|e| (false, e.to_string()))?;
            let req_handle = self.request_handle.fetch_add(1, Ordering::SeqCst);
            let seq_num = self.sequence_number.fetch_add(1, Ordering::SeqCst);
            let chan_id = self.channel_id;

            let mut body = Vec::new();
            body.extend_from_slice(&seq_num.to_le_bytes());
            body.extend_from_slice(&req_handle.to_le_bytes());

            // NodeId 673 (WriteRequest)
            body.push(0x01);
            body.push(0x00);
            body.extend_from_slice(&673u16.to_le_bytes());

            // RequestHeader
            body.push(0x00);
            body.extend_from_slice(&0u64.to_le_bytes());
            body.extend_from_slice(&req_handle.to_le_bytes());
            body.extend_from_slice(&0u32.to_le_bytes());
            body.extend_from_slice(&(-1i32).to_le_bytes());
            body.extend_from_slice(&10000u32.to_le_bytes());
            body.push(0x00);
            body.push(0x00);

            // Nodes to write array: 1
            body.extend_from_slice(&1u32.to_le_bytes());

            // WriteValue: NodeId
            match &node_id {
                OpcUaNodeId::Numeric {
                    namespace,
                    identifier,
                } => {
                    if *namespace == 0 && *identifier <= 255 {
                        body.push(0x00);
                        body.push(*identifier as u8);
                    } else if *namespace <= 255 && *identifier <= 65535 {
                        body.push(0x01);
                        body.push(*namespace as u8);
                        body.extend_from_slice(&(*identifier as u16).to_le_bytes());
                    } else {
                        body.push(0x02);
                        body.extend_from_slice(&namespace.to_le_bytes());
                        body.extend_from_slice(&identifier.to_le_bytes());
                    }
                }
                OpcUaNodeId::String {
                    namespace,
                    identifier,
                } => {
                    body.push(0x03);
                    body.extend_from_slice(&namespace.to_le_bytes());
                    let bytes = identifier.as_bytes();
                    body.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                    body.extend_from_slice(bytes);
                }
                _ => {
                    body.push(0x00);
                    body.push(0x00);
                }
            }

            // AttributeId: Value = 13 (u32)
            body.extend_from_slice(&13u32.to_le_bytes());
            // IndexRange: null
            body.extend_from_slice(&(-1i32).to_le_bytes());

            // DataValue: EncodingMask (0x01 = Value present)
            body.push(0x01);
            // Variant encoding based on JSON value type
            match value {
                Value::Bool(b) => {
                    body.push(0x01); // Boolean (Variant Type 1)
                    body.push(if *b { 1 } else { 0 });
                }
                Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        body.push(0x06); // Int32 (Variant Type 6)
                        body.extend_from_slice(&(i as i32).to_le_bytes());
                    } else if let Some(f) = n.as_f64() {
                        if !f.is_finite() {
                            return Err((
                                true,
                                format!("Cannot write non-finite float ({:?}) to OPC UA node", f),
                            ));
                        }
                        body.push(0x0B); // Double (Variant Type 11)
                        body.extend_from_slice(&f.to_le_bytes());
                    } else {
                        body.push(0x00);
                    }
                }
                Value::String(s) => {
                    body.push(0x0C); // String (Variant Type 12)
                    let bytes = s.as_bytes();
                    body.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                    body.extend_from_slice(bytes);
                }
                _ => {
                    body.push(0x00); // Null Variant
                }
            }

            let msg_total_len = (8 + 4 + body.len()) as u32;
            let mut msg = Vec::with_capacity(msg_total_len as usize);
            msg.extend_from_slice(b"MSGF");
            msg.extend_from_slice(&msg_total_len.to_le_bytes());
            msg.extend_from_slice(&chan_id.to_le_bytes());
            msg.extend_from_slice(&body);

            let stream = self.stream.as_mut().unwrap();
            stream
                .write_all(&msg)
                .await
                .map_err(|e| (false, e.to_string()))?;

            // Read WriteResponse
            let mut resp_header = [0u8; 8];
            stream
                .read_exact(&mut resp_header)
                .await
                .map_err(|e| (false, e.to_string()))?;
            let resp_len = u32::from_le_bytes([
                resp_header[4],
                resp_header[5],
                resp_header[6],
                resp_header[7],
            ]) as usize;
            let mut resp_body = vec![0u8; resp_len - 8];
            stream
                .read_exact(&mut resp_body)
                .await
                .map_err(|e| (false, e.to_string()))?;

            // Parse StatusCode result: 0 = Good
            if resp_body.len() >= 8 {
                let status_code = u32::from_le_bytes([
                    resp_body[resp_body.len() - 4],
                    resp_body[resp_body.len() - 3],
                    resp_body[resp_body.len() - 2],
                    resp_body[resp_body.len() - 1],
                ]);
                if (status_code & 0x80000000) != 0 {
                    // OPC UA Bad status code (permanent poison pill if BadNodeIdUnknown or BadTypeMismatch)
                    let is_permanent = status_code == 0x803E0000 // BadNodeIdUnknown
                        || status_code == 0x80740000 // BadTypeMismatch
                        || status_code == 0x803B0000; // BadNotWritable
                    return Err((
                        is_permanent,
                        format!("OPC UA Write failed with StatusCode {:#010x}", status_code),
                    ));
                }
            }
            Ok(())
        };

        match tokio::time::timeout(timeout, fut).await {
            Ok(r) => r,
            Err(_) => {
                self.disconnect();
                Err((false, format!("OPC UA write timed out after {:?}", timeout)))
            }
        }
    }
}

/// Skips an OPC-UA DiagnosticInfo structure (OPC-UA Part 6 §5.2.2.12).
fn skip_diagnostic_info(buf: &[u8], pos: &mut usize) -> Result<()> {
    if *pos >= buf.len() {
        bail!("Unexpected EOF while reading OPC-UA DiagnosticInfo mask");
    }
    let mask = buf[*pos];
    *pos += 1;

    // 0x01: SymbolicId (Int32), 0x02: NamespaceUri (Int32),
    // 0x04: LocalizedText (Int32), 0x08: Locale (Int32)
    for bit in [0x01u8, 0x02, 0x04, 0x08] {
        if (mask & bit) != 0 {
            if *pos + 4 > buf.len() {
                bail!("Unexpected EOF in DiagnosticInfo Int32 field");
            }
            *pos += 4;
        }
    }
    // 0x10: AdditionalInfo (OPC-UA String)
    if (mask & 0x10) != 0 {
        skip_opcua_string(buf, pos)?;
    }
    // 0x20: InnerStatusCode (UInt32)
    if (mask & 0x20) != 0 {
        if *pos + 4 > buf.len() {
            bail!("Unexpected EOF in DiagnosticInfo InnerStatusCode");
        }
        *pos += 4;
    }
    // 0x40: InnerDiagnosticInfo (recursive)
    if (mask & 0x40) != 0 {
        skip_diagnostic_info(buf, pos)?;
    }
    Ok(())
}

/// Skips an OPC-UA String or ByteString (4-byte signed length prefix + payload).
fn skip_opcua_string(buf: &[u8], pos: &mut usize) -> Result<()> {
    if *pos + 4 > buf.len() {
        bail!("Unexpected EOF reading OPC-UA string length");
    }
    let len = i32::from_le_bytes([buf[*pos], buf[*pos + 1], buf[*pos + 2], buf[*pos + 3]]);
    *pos += 4;
    if len > 0 {
        let len_usize = len as usize;
        if *pos + len_usize > buf.len() {
            bail!("Unexpected EOF reading OPC-UA string body of {} bytes", len);
        }
        *pos += len_usize;
    }
    Ok(())
}

/// Skips an OPC-UA StringTable (Int32 count + count * String).
fn skip_string_table(buf: &[u8], pos: &mut usize) -> Result<()> {
    if *pos + 4 > buf.len() {
        bail!("Unexpected EOF reading OPC-UA StringTable length");
    }
    let count = i32::from_le_bytes([buf[*pos], buf[*pos + 1], buf[*pos + 2], buf[*pos + 3]]);
    *pos += 4;
    if count > 0 {
        for _ in 0..count {
            skip_opcua_string(buf, pos)?;
        }
    }
    Ok(())
}

/// Skips an OPC-UA NodeId (OPC-UA Part 6 §5.2.2.9).
fn skip_node_id(buf: &[u8], pos: &mut usize) -> Result<()> {
    if *pos >= buf.len() {
        bail!("Unexpected EOF reading OPC-UA NodeId encoding byte");
    }
    let encoding = buf[*pos];
    *pos += 1;
    match encoding & 0x0F {
        0x00 => {
            // TwoByte: 1 byte identifier
            if *pos + 1 > buf.len() {
                bail!("Unexpected EOF in TwoByte NodeId");
            }
            *pos += 1;
        }
        0x01 => {
            // FourByte: 1 byte namespace + 2 bytes identifier
            if *pos + 3 > buf.len() {
                bail!("Unexpected EOF in FourByte NodeId");
            }
            *pos += 3;
        }
        0x02 => {
            // Numeric: 2 bytes namespace + 4 bytes identifier
            if *pos + 6 > buf.len() {
                bail!("Unexpected EOF in Numeric NodeId");
            }
            *pos += 6;
        }
        0x03 | 0x05 => {
            // String or ByteString: 2 bytes namespace + OPC-UA String
            if *pos + 2 > buf.len() {
                bail!("Unexpected EOF in String/Opaque NodeId namespace");
            }
            *pos += 2;
            skip_opcua_string(buf, pos)?;
        }
        0x04 => {
            // Guid: 2 bytes namespace + 16 bytes GUID
            if *pos + 18 > buf.len() {
                bail!("Unexpected EOF in Guid NodeId");
            }
            *pos += 18;
        }
        other => bail!("Unsupported OPC-UA NodeId encoding {:#04x}", other),
    }
    if (encoding & 0x80) != 0 {
        skip_opcua_string(buf, pos)?;
    }
    if (encoding & 0x40) != 0 {
        if *pos + 4 > buf.len() {
            bail!("Unexpected EOF in NodeId ServerIndex");
        }
        *pos += 4;
    }
    Ok(())
}

/// Skips an OPC-UA ExtensionObject (`AdditionalHeader` in `ResponseHeader`).
fn skip_extension_object(buf: &[u8], pos: &mut usize) -> Result<()> {
    skip_node_id(buf, pos)?;
    if *pos >= buf.len() {
        bail!("Unexpected EOF reading ExtensionObject encoding mask");
    }
    let mask = buf[*pos];
    *pos += 1;
    match mask {
        0x00 => Ok(()), // No body
        0x01 | 0x02 => skip_opcua_string(buf, pos),
        other => bail!("Unsupported ExtensionObject encoding mask {:#04x}", other),
    }
}

/// Deterministically locates the start of the `DataValue` items in a `ReadResponse` body.
/// Supports both full OPC-UA Part 6 `ResponseHeader` framing and the compact 32-byte
/// mock header used by local test fixtures.
fn locate_read_results_slice(body: &[u8], expected_nodes: usize) -> Result<&[u8]> {
    if body.len() < 36 {
        bail!(
            "OPC-UA ReadResponse body too short ({} bytes, need at least 36)",
            body.len()
        );
    }

    // Offset 12..16 is the Service TypeId (FourByte NodeId for ReadResponse = 634, or ServiceFault = 397)
    if body[12] == 0x01 {
        let type_id = u16::from_le_bytes([body[14], body[15]]);
        if type_id == 397 {
            let service_result = u32::from_le_bytes([body[28], body[29], body[30], body[31]]);
            bail!(
                "OPC-UA Server returned ServiceFault (StatusCode {:#010x})",
                service_result
            );
        }
    }

    // Offset 28..32 is ResponseHeader.ServiceResult
    let service_result = u32::from_le_bytes([body[28], body[29], body[30], body[31]]);
    if (service_result & 0x8000_0000) != 0 {
        bail!(
            "OPC-UA ReadResponse failed with ServiceResult StatusCode {:#010x}",
            service_result
        );
    }

    // Path A: Standard OPC-UA Part 6 ResponseHeader (ServiceDiagnostics + StringTable + AdditionalHeader)
    let mut std_pos = 32usize;
    let std_parse = (|| -> Result<usize> {
        skip_diagnostic_info(body, &mut std_pos)?;
        skip_string_table(body, &mut std_pos)?;
        skip_extension_object(body, &mut std_pos)?;
        if std_pos + 4 > body.len() {
            bail!("Unexpected EOF before Results array length");
        }
        let count = i32::from_le_bytes([
            body[std_pos],
            body[std_pos + 1],
            body[std_pos + 2],
            body[std_pos + 3],
        ]);
        if count < 0 || count as usize != expected_nodes {
            bail!(
                "OPC-UA ReadResponse Results count mismatch: expected {}, got {}",
                expected_nodes,
                count
            );
        }
        Ok(std_pos + 4)
    })();

    if let Ok(results_offset) = std_parse {
        return Ok(&body[results_offset..]);
    }

    // Path B: Compact mock header where Results count is placed immediately after ServiceResult at offset 32
    let compact_count = i32::from_le_bytes([body[32], body[33], body[34], body[35]]);
    if compact_count >= 0 && compact_count as usize == expected_nodes {
        return Ok(&body[36..]);
    }

    Err(std_parse.unwrap_err())
}

fn decode_scalar_variant(v_type: u8, r: &mut &[u8]) -> Result<OpcUaVariant> {
    match v_type {
        0x00 => Ok(OpcUaVariant::Null),
        0x01 => {
            if r.is_empty() {
                bail!("Truncated OPC-UA Boolean Variant");
            }
            let b = r[0] != 0;
            *r = &r[1..];
            Ok(OpcUaVariant::Boolean(b))
        }
        0x02 => {
            if r.is_empty() {
                bail!("Truncated OPC-UA SByte Variant");
            }
            let v = r[0] as i8;
            *r = &r[1..];
            Ok(OpcUaVariant::SByte(v))
        }
        0x03 => {
            if r.is_empty() {
                bail!("Truncated OPC-UA Byte Variant");
            }
            let v = r[0];
            *r = &r[1..];
            Ok(OpcUaVariant::Byte(v))
        }
        0x04 => {
            if r.len() < 2 {
                bail!("Truncated OPC-UA Int16 Variant");
            }
            let v = i16::from_le_bytes([r[0], r[1]]);
            *r = &r[2..];
            Ok(OpcUaVariant::Int16(v))
        }
        0x05 => {
            if r.len() < 2 {
                bail!("Truncated OPC-UA UInt16 Variant");
            }
            let v = u16::from_le_bytes([r[0], r[1]]);
            *r = &r[2..];
            Ok(OpcUaVariant::UInt16(v))
        }
        0x06 => {
            if r.len() < 4 {
                bail!("Truncated OPC-UA Int32 Variant");
            }
            let v = i32::from_le_bytes([r[0], r[1], r[2], r[3]]);
            *r = &r[4..];
            Ok(OpcUaVariant::Int32(v))
        }
        0x07 => {
            if r.len() < 4 {
                bail!("Truncated OPC-UA UInt32 Variant");
            }
            let v = u32::from_le_bytes([r[0], r[1], r[2], r[3]]);
            *r = &r[4..];
            Ok(OpcUaVariant::UInt32(v))
        }
        0x08 => {
            if r.len() < 8 {
                bail!("Truncated OPC-UA Int64 Variant");
            }
            let v = i64::from_le_bytes([r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7]]);
            *r = &r[8..];
            Ok(OpcUaVariant::Int64(v))
        }
        0x09 => {
            if r.len() < 8 {
                bail!("Truncated OPC-UA UInt64 Variant");
            }
            let v = u64::from_le_bytes([r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7]]);
            *r = &r[8..];
            Ok(OpcUaVariant::UInt64(v))
        }
        0x0A => {
            if r.len() < 4 {
                bail!("Truncated OPC-UA Float Variant");
            }
            let v = f32::from_le_bytes([r[0], r[1], r[2], r[3]]);
            *r = &r[4..];
            Ok(OpcUaVariant::Float(v))
        }
        0x0B => {
            if r.len() < 8 {
                bail!("Truncated OPC-UA Double Variant");
            }
            let v = f64::from_le_bytes([r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7]]);
            *r = &r[8..];
            Ok(OpcUaVariant::Double(v))
        }
        0x0C => {
            if r.len() < 4 {
                bail!("Truncated OPC-UA String Variant length");
            }
            let len = i32::from_le_bytes([r[0], r[1], r[2], r[3]]);
            *r = &r[4..];
            if len < 0 {
                Ok(OpcUaVariant::Null)
            } else if len == 0 {
                Ok(OpcUaVariant::String(String::new()))
            } else {
                let len_usize = len as usize;
                if r.len() < len_usize {
                    bail!("Truncated OPC-UA String Variant body");
                }
                let s = String::from_utf8_lossy(&r[..len_usize]).to_string();
                *r = &r[len_usize..];
                Ok(OpcUaVariant::String(s))
            }
        }
        0x0D => {
            if r.len() < 8 {
                bail!("Truncated OPC-UA DateTime Variant");
            }
            let dt = i64::from_le_bytes([r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7]]);
            *r = &r[8..];
            Ok(OpcUaVariant::DateTime(dt))
        }
        0x0F => {
            if r.len() < 4 {
                bail!("Truncated OPC-UA ByteString Variant length");
            }
            let len = i32::from_le_bytes([r[0], r[1], r[2], r[3]]);
            *r = &r[4..];
            if len <= 0 {
                Ok(OpcUaVariant::ByteString(Vec::new()))
            } else {
                let len_usize = len as usize;
                if r.len() < len_usize {
                    bail!("Truncated OPC-UA ByteString Variant body");
                }
                let bytes = r[..len_usize].to_vec();
                *r = &r[len_usize..];
                Ok(OpcUaVariant::ByteString(bytes))
            }
        }
        other => bail!("Unsupported OPC-UA Variant type {:#04x}", other),
    }
}

/// Helper function to parse DataValues out of OPC UA ReadResponse PDU.
fn parse_read_response(
    body: &[u8],
    nodes: &[OpcUaNodeMapping],
) -> Result<HashMap<String, OpcUaDataValue>> {
    let mut map = HashMap::new();
    if body.is_empty() || nodes.is_empty() {
        return Ok(map);
    }

    let mut r = locate_read_results_slice(body, nodes.len())?;

    for m in nodes {
        if r.is_empty() {
            bail!(
                "Truncated OPC-UA ReadResponse before node '{}' DataValue",
                m.name
            );
        }

        let mask = r[0];
        r = &r[1..];

        let mut variant = OpcUaVariant::Null;
        if (mask & 0x01) != 0 {
            if r.is_empty() {
                bail!("Truncated OPC-UA Variant header for node '{}'", m.name);
            }
            let var_mask = r[0];
            r = &r[1..];
            let v_type = var_mask & 0x3F;
            if (var_mask & 0x80) != 0 {
                // Array Variant: read 4-byte array length and elements
                if r.len() < 4 {
                    bail!(
                        "Truncated OPC-UA Variant array length for node '{}'",
                        m.name
                    );
                }
                let arr_len = i32::from_le_bytes([r[0], r[1], r[2], r[3]]).max(0) as usize;
                r = &r[4..];
                let mut last_elem = OpcUaVariant::Null;
                for _ in 0..arr_len {
                    last_elem = decode_scalar_variant(v_type, &mut r)?;
                }
                if (var_mask & 0x40) != 0 {
                    if r.len() < 4 {
                        bail!("Truncated OPC-UA Variant array dimensions length");
                    }
                    let dims_len = i32::from_le_bytes([r[0], r[1], r[2], r[3]]).max(0) as usize;
                    r = &r[4..];
                    let dims_bytes = dims_len.saturating_mul(4);
                    if r.len() < dims_bytes {
                        bail!("Truncated OPC-UA Variant array dimensions");
                    }
                    r = &r[dims_bytes..];
                }
                variant = last_elem;
            } else {
                variant = decode_scalar_variant(v_type, &mut r)?;
            }
        }

        let mut status = 0u32;
        if (mask & 0x02) != 0 {
            if r.len() < 4 {
                bail!("Truncated OPC-UA StatusCode for node '{}'", m.name);
            }
            status = u32::from_le_bytes([r[0], r[1], r[2], r[3]]);
            r = &r[4..];
        }

        let mut ts = None;
        if (mask & 0x04) != 0 {
            if r.len() < 8 {
                bail!("Truncated OPC-UA SourceTimestamp for node '{}'", m.name);
            }
            ts = Some(i64::from_le_bytes([
                r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7],
            ]));
            r = &r[8..];
        }

        // 0x08: ServerTimestamp (DateTime, 8 bytes)
        if (mask & 0x08) != 0 {
            if r.len() < 8 {
                bail!("Truncated OPC-UA ServerTimestamp for node '{}'", m.name);
            }
            r = &r[8..];
        }

        // 0x10: SourcePicoseconds (UInt16, 2 bytes)
        if (mask & 0x10) != 0 {
            if r.len() < 2 {
                bail!("Truncated OPC-UA SourcePicoseconds for node '{}'", m.name);
            }
            r = &r[2..];
        }

        // 0x20: ServerPicoseconds (UInt16, 2 bytes)
        if (mask & 0x20) != 0 {
            if r.len() < 2 {
                bail!("Truncated OPC-UA ServerPicoseconds for node '{}'", m.name);
            }
            r = &r[2..];
        }

        map.insert(
            m.name.clone(),
            OpcUaDataValue {
                value: variant,
                status_code: status,
                source_timestamp: ts,
            },
        );
    }

    Ok(map)
}

/// Reads a one-shot snapshot of all configured OPC UA nodes (for table lookups and point scans).
pub async fn opcua_read_snapshot(config: &OpcUaConfig) -> Result<HashMap<String, Value>> {
    let mut client = OpcUaClient::new(config.endpoint(), config.timeout_ms);
    let values = client.read_nodes(&config.nodes).await?;

    let mut record = HashMap::new();
    for (name, dv) in values {
        if (dv.status_code & 0x8000_0000) != 0 {
            bail!(
                "OPC-UA node '{}' returned Bad StatusCode {:#010x}",
                name,
                dv.status_code
            );
        }
        match dv.value.to_json_value(&name) {
            Ok(val) => {
                record.insert(name.clone(), val);
                if config.include_metadata {
                    record.insert(format!("{}_status_code", name), json!(dv.status_code));
                    if let Some(ts) = dv.source_timestamp {
                        record.insert(format!("{}_source_timestamp", name), json!(ts));
                    }
                }
            }
            Err(e) => {
                bail!("Failed decoding OPC-UA node '{}': {}", name, e);
            }
        }
    }
    Ok(record)
}

/// OPC UA Stream Source worker (supporting both Poll and Subscription modes).
pub struct OpcUaSource {
    config: OpcUaConfig,
    tx: StreamSender,
    status_callback: Option<crate::ConnectorStatusCallback>,
}

impl OpcUaSource {
    pub fn new(config: OpcUaConfig, tx: StreamSender) -> Self {
        Self {
            config,
            tx,
            status_callback: None,
        }
    }

    pub fn with_status_callback(mut self, cb: Option<crate::ConnectorStatusCallback>) -> Self {
        self.status_callback = cb;
        self
    }

    pub fn spawn(self, mut cancel_rx: watch::Receiver<bool>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut client = OpcUaClient::new(self.config.endpoint(), self.config.timeout_ms);
            let mut interval = tokio::time::interval(self.config.polling_interval());
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            while !*cancel_rx.borrow() {
                tokio::select! {
                    _ = interval.tick() => {}
                    _ = cancel_rx.changed() => break,
                }

                if *cancel_rx.borrow() {
                    break;
                }

                match client.read_nodes(&self.config.nodes).await {
                    Ok(read_map) => {
                        if let Some(ref cb) = self.status_callback {
                            cb(self.config.endpoint(), "opcua", "connected", None);
                        }
                        let mut record = HashMap::new();
                        let mut decode_errors = Vec::new();

                        for (name, dv) in read_map {
                            if (dv.status_code & 0x8000_0000) != 0 {
                                decode_errors.push(format!(
                                    "OPC-UA node '{}' returned Bad StatusCode {:#010x}",
                                    name, dv.status_code
                                ));
                                continue;
                            }
                            match dv.value.to_json_value(&name) {
                                Ok(val) => {
                                    record.insert(name.clone(), val);
                                    if self.config.include_metadata {
                                        record.insert(
                                            format!("{}_status_code", name),
                                            json!(dv.status_code),
                                        );
                                        if let Some(ts) = dv.source_timestamp {
                                            record.insert(
                                                format!("{}_source_timestamp", name),
                                                json!(ts),
                                            );
                                        }
                                    }
                                }
                                Err(err_msg) => {
                                    decode_errors.push(err_msg);
                                }
                            }
                        }

                        // If decode errors occurred (e.g. NaN/Inf float),
                        // emit record with __raw_error__ and __raw_payload__ for Track C DLQ routing!
                        if !decode_errors.is_empty() {
                            let combined_err = decode_errors.join("; ");
                            let mut err_rec = HashMap::new();
                            err_rec
                                .insert("__raw_error__".to_string(), Value::String(combined_err));
                            err_rec.insert(
                                "__raw_payload__".to_string(),
                                Value::String(format!(
                                    "OPC-UA endpoint: {}, nodes: {:?}",
                                    self.config.endpoint(),
                                    self.config
                                        .nodes
                                        .iter()
                                        .map(|n| &n.node_id)
                                        .collect::<Vec<_>>()
                                )),
                            );
                            let mut stream_rec = StreamRecord::new(err_rec);
                            stream_rec.meta = Some(Arc::new(RecordMeta {
                                topic: None,
                                source: Some(Arc::from("opcua")),
                                custom: None,
                            }));
                            if matches!(
                                self.tx.send(stream_rec).await,
                                Err(rekuiper_core::PublishError::Closed)
                            ) {
                                break;
                            }
                        } else if !record.is_empty() {
                            let mut stream_rec = StreamRecord::new(record);
                            stream_rec.meta = Some(Arc::new(RecordMeta {
                                topic: None,
                                source: Some(Arc::from("opcua")),
                                custom: None,
                            }));
                            if matches!(
                                self.tx.send(stream_rec).await,
                                Err(rekuiper_core::PublishError::Closed)
                            ) {
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            "OPC UA poll error on endpoint {}: {}; reconnecting...",
                            self.config.endpoint(),
                            e
                        );
                        if let Some(ref cb) = self.status_callback {
                            cb(
                                self.config.endpoint(),
                                "opcua",
                                "error",
                                Some(&e.to_string()),
                            );
                        }
                        let mut err_rec = HashMap::new();
                        err_rec.insert(
                            "__raw_error__".to_string(),
                            Value::String(format!("OPC-UA connection error: {}", e)),
                        );
                        err_rec.insert(
                            "__raw_payload__".to_string(),
                            Value::String(self.config.endpoint().to_string()),
                        );
                        let stream_rec = StreamRecord::new(err_rec);
                        let _ = self.tx.send(stream_rec).await;
                    }
                }
            }
        })
    }
}

/// OPC UA Sink configuration for writing rule outputs to OPC UA NodeIds.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpcUaSinkConfig {
    #[serde(default = "default_endpoint")]
    pub endpoint: String,
    #[serde(default)]
    pub server: Option<String>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    pub node_id: String,
    #[serde(default)]
    pub field: Option<String>,
}

impl Default for OpcUaSinkConfig {
    fn default() -> Self {
        Self {
            endpoint: default_endpoint(),
            server: None,
            url: None,
            timeout_ms: default_timeout_ms(),
            node_id: String::new(),
            field: None,
        }
    }
}

impl OpcUaSinkConfig {
    pub fn endpoint(&self) -> &str {
        if let Some(ref s) = self.server {
            if !s.trim().is_empty() {
                return s.trim();
            }
        }
        if let Some(ref u) = self.url {
            if !u.trim().is_empty() {
                return u.trim();
            }
        }
        if !self.endpoint.trim().is_empty() {
            return self.endpoint.trim();
        }
        DEFAULT_OPCUA_ENDPOINT
    }
}

/// OPC UA Sink actor writing rule output records to target NodeIds.
pub struct OpcUaSink {
    client: tokio::sync::Mutex<OpcUaClient>,
    config: OpcUaSinkConfig,
}

impl OpcUaSink {
    pub fn new(config: OpcUaSinkConfig) -> Self {
        let client = OpcUaClient::new(config.endpoint(), config.timeout_ms);
        Self {
            client: tokio::sync::Mutex::new(client),
            config,
        }
    }

    pub async fn write_record(
        &self,
        record: &HashMap<String, Value>,
    ) -> Result<(), (bool, String)> {
        let field_name = self.config.field.as_deref().unwrap_or("value");
        let val = record.get(field_name).or_else(|| record.get("val"));

        let Some(val) = val else {
            return Ok(());
        };

        let mut client = self.client.lock().await;
        client.write_node(&self.config.node_id, val).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_id_parsing() {
        let n1 = OpcUaNodeId::parse("ns=2;s=Channel1.Device1.SpindleSpeed");
        match n1 {
            OpcUaNodeId::String {
                namespace,
                identifier,
            } => {
                assert_eq!(namespace, 2);
                assert_eq!(identifier, "Channel1.Device1.SpindleSpeed");
            }
            _ => panic!("Expected string NodeId"),
        }

        let n2 = OpcUaNodeId::parse("ns=3;i=1004");
        match n2 {
            OpcUaNodeId::Numeric {
                namespace,
                identifier,
            } => {
                assert_eq!(namespace, 3);
                assert_eq!(identifier, 1004);
            }
            _ => panic!("Expected numeric NodeId"),
        }

        let n3 = OpcUaNodeId::parse("i=2258");
        match n3 {
            OpcUaNodeId::Numeric {
                namespace,
                identifier,
            } => {
                assert_eq!(namespace, 0);
                assert_eq!(identifier, 2258);
            }
            _ => panic!("Expected numeric NodeId"),
        }
    }

    #[test]
    fn test_variant_to_json_and_nan_guard() {
        let v_bool = OpcUaVariant::Boolean(true);
        assert_eq!(v_bool.to_json_value("b").unwrap(), Value::Bool(true));

        let v_int = OpcUaVariant::Int32(42);
        assert_eq!(v_int.to_json_value("i").unwrap(), json!(42));

        let v_float = OpcUaVariant::Float(12.34);
        assert!((v_float.to_json_value("f").unwrap().as_f64().unwrap() - 12.34).abs() < 1e-4);

        // NaN float check
        let v_nan = OpcUaVariant::Float(f32::NAN);
        let err_nan = v_nan.to_json_value("f");
        assert!(err_nan.is_err());
        assert!(err_nan.unwrap_err().contains("non-finite float"));

        // Infinity double check
        let v_inf = OpcUaVariant::Double(f64::INFINITY);
        let err_inf = v_inf.to_json_value("d");
        assert!(err_inf.is_err());
        assert!(err_inf.unwrap_err().contains("non-finite double"));
    }

    #[test]
    fn test_parse_read_response_standard_header_and_both_timestamps() {
        let nodes = vec![
            OpcUaNodeMapping {
                name: "temp".to_string(),
                node_id: "ns=2;s=Temp".to_string(),
            },
            OpcUaNodeMapping {
                name: "rpm".to_string(),
                node_id: "ns=2;s=Rpm".to_string(),
            },
        ];

        let mut body = Vec::new();
        body.extend_from_slice(&1u32.to_le_bytes()); // TokenId
        body.extend_from_slice(&1u32.to_le_bytes()); // SeqNum
        body.extend_from_slice(&1u32.to_le_bytes()); // ReqId
        body.extend_from_slice(&[0x01, 0x00]); // FourByte NodeId
        body.extend_from_slice(&634u16.to_le_bytes()); // ReadResponse (634)
        body.extend_from_slice(&0i64.to_le_bytes()); // Timestamp
        body.extend_from_slice(&1u32.to_le_bytes()); // RequestHandle
        body.extend_from_slice(&0u32.to_le_bytes()); // ServiceResult = Good (0)
                                                     // Standard ResponseHeader trailer:
        body.push(0x00); // ServiceDiagnostics mask = 0
        body.extend_from_slice(&(-1i32).to_le_bytes()); // StringTable = -1
        body.extend_from_slice(&[0x00, 0x00, 0x00]); // AdditionalHeader = null ExtensionObject

        // Results array length = 2
        body.extend_from_slice(&2i32.to_le_bytes());

        // Node 1: mask = 0x01 (Value) | 0x04 (SourceTimestamp) | 0x08 (ServerTimestamp)
        body.push(0x01 | 0x04 | 0x08);
        body.push(0x0B); // Double
        body.extend_from_slice(&42.5f64.to_le_bytes());
        body.extend_from_slice(&1000i64.to_le_bytes()); // SourceTimestamp
        body.extend_from_slice(&1005i64.to_le_bytes()); // ServerTimestamp

        // Node 2: mask = 0x01 (Value) | 0x02 (StatusCode)
        body.push(0x01 | 0x02);
        body.push(0x06); // Int32
        body.extend_from_slice(&1500i32.to_le_bytes());
        body.extend_from_slice(&0u32.to_le_bytes()); // StatusCode = Good

        let parsed =
            parse_read_response(&body, &nodes).expect("should parse standard ReadResponse");
        assert_eq!(
            parsed.get("temp").unwrap().value,
            OpcUaVariant::Double(42.5)
        );
        assert_eq!(parsed.get("temp").unwrap().source_timestamp, Some(1000));
        assert_eq!(parsed.get("rpm").unwrap().value, OpcUaVariant::Int32(1500));
    }
}
