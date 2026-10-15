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
use rekuiper_core::model::StreamRecord;
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
            OpcUaNodeId::Numeric { namespace, identifier } => {
                if *namespace == 0 {
                    format!("i={}", identifier)
                } else {
                    format!("ns={};i={}", namespace, identifier)
                }
            }
            OpcUaNodeId::String { namespace, identifier } => {
                if *namespace == 0 {
                    format!("s={}", identifier)
                } else {
                    format!("ns={};s={}", namespace, identifier)
                }
            }
            OpcUaNodeId::Guid { namespace, identifier } => {
                format!("ns={};g={}", namespace, identifier)
            }
            OpcUaNodeId::Opaque { namespace, identifier } => {
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
            let ack_len = u32::from_le_bytes([
                ack_header[4],
                ack_header[5],
                ack_header[6],
                ack_header[7],
            ]) as usize;
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
                bail!("Expected OPC UA OPN response, got {:?}", &opn_resp_header[0..3]);
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
                    OpcUaNodeId::Numeric { namespace, identifier } => {
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
                    OpcUaNodeId::String { namespace, identifier } => {
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
                    OpcUaNodeId::Opaque { namespace, identifier } => {
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
            self.ensure_connected().await.map_err(|e| (false, e.to_string()))?;
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
                OpcUaNodeId::Numeric { namespace, identifier } => {
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
                OpcUaNodeId::String { namespace, identifier } => {
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
                            return Err((true, format!("Cannot write non-finite float ({:?}) to OPC UA node", f)));
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
            stream.write_all(&msg).await.map_err(|e| (false, e.to_string()))?;

            // Read WriteResponse
            let mut resp_header = [0u8; 8];
            stream.read_exact(&mut resp_header).await.map_err(|e| (false, e.to_string()))?;
            let resp_len = u32::from_le_bytes([
                resp_header[4],
                resp_header[5],
                resp_header[6],
                resp_header[7],
            ]) as usize;
            let mut resp_body = vec![0u8; resp_len - 8];
            stream.read_exact(&mut resp_body).await.map_err(|e| (false, e.to_string()))?;

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

/// Helper function to parse DataValues out of OPC UA ReadResponse PDU.
fn parse_read_response(
    body: &[u8],
    nodes: &[OpcUaNodeMapping],
) -> Result<HashMap<String, OpcUaDataValue>> {
    let mut map = HashMap::new();
    if body.is_empty() {
        return Ok(map);
    }

    let mut cursor = 0;
    // Skip SequenceHeader (8 bytes) + NodeId (4 bytes) + ResponseHeader (~20 bytes)
    if body.len() > 32 {
        cursor = 32;
    }

    // Try finding the results array length (uint32 LE matching nodes.len())
    let mut found_start = None;
    for i in cursor..body.len().saturating_sub(4) {
        let count = u32::from_le_bytes([body[i], body[i + 1], body[i + 2], body[i + 3]]);
        if count == nodes.len() as u32 && count > 0 {
            found_start = Some(i + 4);
            break;
        }
    }

    let start_idx = found_start.unwrap_or(cursor);
    let mut r = &body[start_idx..];

    for m in nodes {
        if r.is_empty() {
            map.insert(
                m.name.clone(),
                OpcUaDataValue {
                    value: OpcUaVariant::Null,
                    status_code: 0,
                    source_timestamp: None,
                },
            );
            continue;
        }

        let mask = r[0];
        r = &r[1..];

        let mut variant = OpcUaVariant::Null;
        if (mask & 0x01) != 0 && !r.is_empty() {
            let v_type = r[0] & 0x3F;
            r = &r[1..];
            variant = match v_type {
                0x01 => {
                    // Boolean
                    if !r.is_empty() {
                        let b = r[0] != 0;
                        r = &r[1..];
                        OpcUaVariant::Boolean(b)
                    } else {
                        OpcUaVariant::Null
                    }
                }
                0x04 => {
                    // Int16
                    if r.len() >= 2 {
                        let v = i16::from_le_bytes([r[0], r[1]]);
                        r = &r[2..];
                        OpcUaVariant::Int16(v)
                    } else {
                        OpcUaVariant::Null
                    }
                }
                0x05 => {
                    // UInt16
                    if r.len() >= 2 {
                        let v = u16::from_le_bytes([r[0], r[1]]);
                        r = &r[2..];
                        OpcUaVariant::UInt16(v)
                    } else {
                        OpcUaVariant::Null
                    }
                }
                0x06 => {
                    // Int32
                    if r.len() >= 4 {
                        let v = i32::from_le_bytes([r[0], r[1], r[2], r[3]]);
                        r = &r[4..];
                        OpcUaVariant::Int32(v)
                    } else {
                        OpcUaVariant::Null
                    }
                }
                0x07 => {
                    // UInt32
                    if r.len() >= 4 {
                        let v = u32::from_le_bytes([r[0], r[1], r[2], r[3]]);
                        r = &r[4..];
                        OpcUaVariant::UInt32(v)
                    } else {
                        OpcUaVariant::Null
                    }
                }
                0x08 => {
                    // Int64
                    if r.len() >= 8 {
                        let v = i64::from_le_bytes([
                            r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7],
                        ]);
                        r = &r[8..];
                        OpcUaVariant::Int64(v)
                    } else {
                        OpcUaVariant::Null
                    }
                }
                0x0A => {
                    // Float
                    if r.len() >= 4 {
                        let v = f32::from_le_bytes([r[0], r[1], r[2], r[3]]);
                        r = &r[4..];
                        OpcUaVariant::Float(v)
                    } else {
                        OpcUaVariant::Null
                    }
                }
                0x0B => {
                    // Double
                    if r.len() >= 8 {
                        let v = f64::from_le_bytes([
                            r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7],
                        ]);
                        r = &r[8..];
                        OpcUaVariant::Double(v)
                    } else {
                        OpcUaVariant::Null
                    }
                }
                0x0C => {
                    // String
                    if r.len() >= 4 {
                        let len = i32::from_le_bytes([r[0], r[1], r[2], r[3]]);
                        r = &r[4..];
                        if len > 0 && r.len() >= len as usize {
                            let s = String::from_utf8_lossy(&r[..len as usize]).to_string();
                            r = &r[len as usize..];
                            OpcUaVariant::String(s)
                        } else {
                            OpcUaVariant::String(String::new())
                        }
                    } else {
                        OpcUaVariant::Null
                    }
                }
                _ => OpcUaVariant::Null,
            };
        }

        let mut status = 0u32;
        if (mask & 0x02) != 0 && r.len() >= 4 {
            status = u32::from_le_bytes([r[0], r[1], r[2], r[3]]);
            r = &r[4..];
        }

        let mut ts = None;
        if (mask & 0x04) != 0 && r.len() >= 8 {
            ts = Some(i64::from_le_bytes([
                r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7],
            ]));
            r = &r[8..];
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
        match dv.value.to_json_value(&name) {
            Ok(val) => {
                record.insert(name.clone(), val);
                if config.include_metadata {
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
}

impl OpcUaSource {
    pub fn new(config: OpcUaConfig, tx: StreamSender) -> Self {
        Self { config, tx }
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
                        let mut record = HashMap::new();
                        let mut decode_errors = Vec::new();

                        for (name, dv) in read_map {
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
                            err_rec.insert(
                                "__raw_error__".to_string(),
                                Value::String(combined_err),
                            );
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
                            let stream_rec = StreamRecord::new(err_rec);
                            if matches!(self.tx.send(stream_rec).await, Err(rekuiper_core::PublishError::Closed)) {
                                break;
                            }
                        } else if !record.is_empty() {
                            let stream_rec = StreamRecord::new(record);
                            if matches!(self.tx.send(stream_rec).await, Err(rekuiper_core::PublishError::Closed)) {
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

    pub async fn write_record(&self, record: &HashMap<String, Value>) -> Result<(), (bool, String)> {
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
            OpcUaNodeId::String { namespace, identifier } => {
                assert_eq!(namespace, 2);
                assert_eq!(identifier, "Channel1.Device1.SpindleSpeed");
            }
            _ => panic!("Expected string NodeId"),
        }

        let n2 = OpcUaNodeId::parse("ns=3;i=1004");
        match n2 {
            OpcUaNodeId::Numeric { namespace, identifier } => {
                assert_eq!(namespace, 3);
                assert_eq!(identifier, 1004);
            }
            _ => panic!("Expected numeric NodeId"),
        }

        let n3 = OpcUaNodeId::parse("i=2258");
        match n3 {
            OpcUaNodeId::Numeric { namespace, identifier } => {
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
}
