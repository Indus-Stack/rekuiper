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
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;

pub const DEFAULT_MODBUS_ENDPOINT: &str = "127.0.0.1:502";
pub const DEFAULT_MODBUS_INTERVAL_MS: u64 = 1000;
pub const DEFAULT_MODBUS_TIMEOUT_MS: u64 = 2000;
pub const DEFAULT_MODBUS_MAX_GAP: u16 = 10;
pub const MAX_MODBUS_REGISTER_SPAN: u16 = 125;
pub const MAX_MODBUS_COIL_SPAN: u16 = 2000;

fn default_transport() -> String {
    "tcp".to_string()
}

fn default_endpoint() -> String {
    DEFAULT_MODBUS_ENDPOINT.to_string()
}

fn default_unit_id() -> u8 {
    1
}

fn default_interval_ms() -> u64 {
    DEFAULT_MODBUS_INTERVAL_MS
}

fn default_timeout_ms() -> u64 {
    DEFAULT_MODBUS_TIMEOUT_MS
}

fn default_max_gap() -> u16 {
    DEFAULT_MODBUS_MAX_GAP
}

fn default_byte_order() -> String {
    "ABCD".to_string()
}

fn default_function() -> String {
    "holding".to_string()
}

fn default_data_type() -> String {
    "uint16".to_string()
}

/// Modbus standard CRC-16 calculation for RTU framing (polynomial 0xA001).
pub fn modbus_crc16(data: &[u8]) -> u16 {
    let mut crc = 0xFFFFu16;
    for &b in data {
        crc ^= b as u16;
        for _ in 0..8 {
            if (crc & 0x0001) != 0 {
                crc = (crc >> 1) ^ 0xA001;
            } else {
                crc >>= 1;
            }
        }
    }
    crc
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModbusByteOrder {
    Abcd, // Big-Endian
    Cdab, // Word-swapped Big-Endian (Modicon/Schneider standard)
    Badc, // Byte-swapped
    Dcba, // Little-Endian
}

impl ModbusByteOrder {
    pub fn parse(s: &str) -> Self {
        match s.to_ascii_uppercase().as_str() {
            "CDAB" => ModbusByteOrder::Cdab,
            "BADC" => ModbusByteOrder::Badc,
            "DCBA" => ModbusByteOrder::Dcba,
            _ => ModbusByteOrder::Abcd,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModbusFunction {
    Coil = 1,          // FC01 Read Coils
    DiscreteInput = 2, // FC02 Read Discrete Inputs
    Holding = 3,       // FC03 Read Holding Registers
    Input = 4,         // FC04 Read Input Registers
}

impl ModbusFunction {
    pub fn parse(s: &str) -> Result<Self> {
        let lower = s.to_ascii_lowercase();
        match lower.as_str() {
            "coil" | "coils" | "fc01" | "1" => Ok(ModbusFunction::Coil),
            "discrete" | "discrete_input" | "discreteinput" | "fc02" | "2" => {
                Ok(ModbusFunction::DiscreteInput)
            }
            "holding" | "holding_register" | "holdingregister" | "fc03" | "3" => {
                Ok(ModbusFunction::Holding)
            }
            "input" | "input_register" | "inputregister" | "fc04" | "4" => {
                Ok(ModbusFunction::Input)
            }
            _ => bail!(
                "Unknown Modbus function '{}'; must be coil, discrete, holding, or input",
                s
            ),
        }
    }

    pub fn code(&self) -> u8 {
        *self as u8
    }

    pub fn is_bit_access(&self) -> bool {
        matches!(self, ModbusFunction::Coil | ModbusFunction::DiscreteInput)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModbusDataType {
    Bool,
    Int16,
    Uint16,
    Int32,
    Uint32,
    Float32,
    Int64,
    Uint64,
    Float64,
    String,
}

impl ModbusDataType {
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "bool" | "boolean" => Ok(ModbusDataType::Bool),
            "int16" | "short" => Ok(ModbusDataType::Int16),
            "uint16" | "ushort" | "word" => Ok(ModbusDataType::Uint16),
            "int32" | "int" => Ok(ModbusDataType::Int32),
            "uint32" | "uint" | "dword" => Ok(ModbusDataType::Uint32),
            "float32" | "float" | "single" => Ok(ModbusDataType::Float32),
            "int64" | "bigint" | "long" => Ok(ModbusDataType::Int64),
            "uint64" | "ubigint" | "ulong" | "qword" => Ok(ModbusDataType::Uint64),
            "float64" | "double" => Ok(ModbusDataType::Float64),
            "string" | "str" => Ok(ModbusDataType::String),
            _ => bail!("Unknown Modbus data type '{}'", s),
        }
    }

    pub fn word_count(&self, length: Option<u16>) -> u16 {
        match self {
            ModbusDataType::Bool | ModbusDataType::Int16 | ModbusDataType::Uint16 => 1,
            ModbusDataType::Int32 | ModbusDataType::Uint32 | ModbusDataType::Float32 => 2,
            ModbusDataType::Int64 | ModbusDataType::Uint64 | ModbusDataType::Float64 => 4,
            ModbusDataType::String => length.unwrap_or(1).max(1),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModbusRegister {
    pub name: String,
    #[serde(default = "default_function")]
    pub function: String,
    pub address: u16,
    #[serde(default = "default_data_type")]
    pub data_type: String,
    #[serde(default)]
    pub byte_order: Option<String>,
    #[serde(default)]
    pub scale: Option<f64>,
    #[serde(default)]
    pub offset: Option<f64>,
    #[serde(default)]
    pub length: Option<u16>,
}

impl Default for ModbusRegister {
    fn default() -> Self {
        Self {
            name: String::new(),
            function: default_function(),
            address: 0,
            data_type: default_data_type(),
            byte_order: None,
            scale: None,
            offset: None,
            length: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModbusConfig {
    #[serde(default = "default_transport")]
    pub transport: String,
    #[serde(default = "default_endpoint")]
    pub endpoint: String,
    #[serde(default)]
    pub server: Option<String>,
    #[serde(default)]
    pub address: Option<String>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default = "default_unit_id")]
    pub unit_id: u8,
    #[serde(default)]
    pub slave_id: Option<u8>,
    #[serde(default = "default_interval_ms")]
    pub interval_ms: u64,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default = "default_max_gap")]
    pub max_gap: u16,
    #[serde(default = "default_byte_order")]
    pub byte_order: String,
    #[serde(default)]
    pub address_base: Option<u16>,
    #[serde(default)]
    pub plc_addressing: bool,
    #[serde(default)]
    pub registers: Vec<ModbusRegister>,
}

impl Default for ModbusConfig {
    fn default() -> Self {
        Self {
            transport: default_transport(),
            endpoint: default_endpoint(),
            server: None,
            address: None,
            url: None,
            unit_id: default_unit_id(),
            slave_id: None,
            interval_ms: default_interval_ms(),
            timeout_ms: default_timeout_ms(),
            max_gap: default_max_gap(),
            byte_order: default_byte_order(),
            address_base: None,
            plc_addressing: false,
            registers: Vec::new(),
        }
    }
}

impl ModbusConfig {
    pub fn endpoint(&self) -> &str {
        if let Some(ref s) = self.server {
            if !s.trim().is_empty() {
                return s.trim();
            }
        }
        if let Some(ref a) = self.address {
            if !a.trim().is_empty() {
                return a.trim();
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
        DEFAULT_MODBUS_ENDPOINT
    }

    pub fn effective_unit_id(&self) -> u8 {
        self.slave_id.unwrap_or(self.unit_id)
    }

    pub fn is_rtu(&self) -> bool {
        self.transport.eq_ignore_ascii_case("rtu")
            || self.transport.eq_ignore_ascii_case("rtu_over_tcp")
    }

    pub fn default_byte_order(&self) -> ModbusByteOrder {
        ModbusByteOrder::parse(&self.byte_order)
    }
}

/// A validated, normalized register ready for polling and decoding.
#[derive(Debug, Clone)]
pub struct PreparedRegister {
    pub name: String,
    pub function: ModbusFunction,
    pub wire_address: u16,
    pub data_type: ModbusDataType,
    pub word_count: u16,
    pub byte_order: ModbusByteOrder,
    pub scale: Option<f64>,
    pub offset: Option<f64>,
    pub length: Option<u16>,
}

impl PreparedRegister {
    pub fn from_config(
        reg: &ModbusRegister,
        default_order: ModbusByteOrder,
        address_base: Option<u16>,
        plc_addressing: bool,
    ) -> Result<Self> {
        let function = ModbusFunction::parse(&reg.function)?;
        let data_type = ModbusDataType::parse(&reg.data_type)?;
        let word_count = if function.is_bit_access() {
            1
        } else {
            data_type.word_count(reg.length)
        };

        let mut wire_addr = reg.address;
        if plc_addressing {
            // Auto-strip 40001 / 30001 / 10001 / 00001 standard PLC notation
            match function {
                ModbusFunction::Holding if wire_addr >= 40001 => wire_addr -= 40001,
                ModbusFunction::Holding if wire_addr >= 4001 => wire_addr -= 4001,
                ModbusFunction::Input if wire_addr >= 30001 => wire_addr -= 30001,
                ModbusFunction::Input if wire_addr >= 3001 => wire_addr -= 3001,
                ModbusFunction::DiscreteInput if wire_addr >= 10001 => wire_addr -= 10001,
                ModbusFunction::DiscreteInput if wire_addr >= 1001 => wire_addr -= 1001,
                ModbusFunction::Coil if wire_addr >= 1 => wire_addr -= 1,
                _ => {}
            }
        } else if let Some(base) = address_base {
            if base == 1 {
                wire_addr = wire_addr.saturating_sub(1);
            }
        }

        let byte_order = reg
            .byte_order
            .as_deref()
            .map(ModbusByteOrder::parse)
            .unwrap_or(default_order);

        Ok(Self {
            name: reg.name.clone(),
            function,
            wire_address: wire_addr,
            data_type,
            word_count,
            byte_order,
            scale: reg.scale,
            offset: reg.offset,
            length: reg.length,
        })
    }
}

/// Coalesced batch read range spanning contiguous or near-contiguous registers.
#[derive(Debug, Clone)]
pub struct CoalescedRange {
    pub function: ModbusFunction,
    pub unit_id: u8,
    pub start_address: u16,
    pub quantity: u16,
    pub registers: Vec<PreparedRegister>,
}

/// Smart Register Coalescer:
/// Groups registers by (function, unit_id), sorts by address, and merges ranges where:
/// `next.address <= current_end + max_gap` and `total_span <= max_limit` (125 registers or 2000 coils).
pub fn coalesce_registers(
    registers: &[PreparedRegister],
    unit_id: u8,
    max_gap: u16,
) -> Vec<CoalescedRange> {
    if registers.is_empty() {
        return Vec::new();
    }

    // Group by ModbusFunction
    let mut by_func: HashMap<u8, Vec<PreparedRegister>> = HashMap::new();
    for reg in registers {
        by_func
            .entry(reg.function.code())
            .or_default()
            .push(reg.clone());
    }

    let mut result = Vec::new();

    for (_func_code, mut list) in by_func {
        list.sort_by_key(|r| r.wire_address);

        let mut current_range: Option<CoalescedRange> = None;

        for reg in list {
            let max_span = if reg.function.is_bit_access() {
                MAX_MODBUS_COIL_SPAN
            } else {
                MAX_MODBUS_REGISTER_SPAN
            };

            let reg_end = reg.wire_address.saturating_add(reg.word_count);

            match current_range.take() {
                None => {
                    current_range = Some(CoalescedRange {
                        function: reg.function,
                        unit_id,
                        start_address: reg.wire_address,
                        quantity: reg.word_count,
                        registers: vec![reg],
                    });
                }
                Some(mut curr) => {
                    let curr_end = curr.start_address.saturating_add(curr.quantity);
                    let proposed_end = curr_end.max(reg_end);
                    let proposed_span = proposed_end.saturating_sub(curr.start_address);

                    if reg.wire_address <= curr_end.saturating_add(max_gap)
                        && proposed_span <= max_span
                    {
                        curr.quantity = proposed_span;
                        curr.registers.push(reg);
                        current_range = Some(curr);
                    } else {
                        result.push(curr);
                        current_range = Some(CoalescedRange {
                            function: reg.function,
                            unit_id,
                            start_address: reg.wire_address,
                            quantity: reg.word_count,
                            registers: vec![reg],
                        });
                    }
                }
            }
        }

        if let Some(curr) = current_range {
            result.push(curr);
        }
    }

    // Stable ordering for deterministic poll requests
    result.sort_by_key(|r| (r.function.code(), r.start_address));
    result
}

/// Applies endianness decoding to 16-bit, 32-bit, and 64-bit Modbus words.
pub fn decode_register_value(
    data_type: ModbusDataType,
    byte_order: ModbusByteOrder,
    raw_words: &[u16],
    scale: Option<f64>,
    offset: Option<f64>,
    reg_name: &str,
    wire_address: u16,
) -> Result<Value, String> {
    let apply_math = |val: f64| -> Result<f64, String> {
        if !val.is_finite() {
            return Err(format!(
                "register '{}' at address {} decoded to non-finite float ({:?}, raw words: {:?})",
                reg_name, wire_address, val, raw_words
            ));
        }
        let scaled = if let Some(s) = scale { val * s } else { val };
        let offsetted = if let Some(o) = offset {
            scaled + o
        } else {
            scaled
        };
        if !offsetted.is_finite() {
            return Err(format!(
                "register '{}' at address {} scaled to non-finite float ({:?})",
                reg_name, wire_address, offsetted
            ));
        }
        Ok(offsetted)
    };

    match data_type {
        ModbusDataType::Bool => {
            let w = raw_words.first().copied().unwrap_or(0);
            Ok(Value::Bool(w != 0))
        }
        ModbusDataType::Int16 => {
            let w = raw_words.first().copied().unwrap_or(0);
            let bytes = match byte_order {
                ModbusByteOrder::Badc | ModbusByteOrder::Dcba => [w as u8, (w >> 8) as u8],
                _ => [(w >> 8) as u8, w as u8],
            };
            let i = i16::from_be_bytes(bytes) as f64;
            let val = apply_math(i)?;
            if scale.is_none() && offset.is_none() {
                Ok(json!(i16::from_be_bytes(bytes)))
            } else {
                Ok(json!(val))
            }
        }
        ModbusDataType::Uint16 => {
            let w = raw_words.first().copied().unwrap_or(0);
            let bytes = match byte_order {
                ModbusByteOrder::Badc | ModbusByteOrder::Dcba => [w as u8, (w >> 8) as u8],
                _ => [(w >> 8) as u8, w as u8],
            };
            let u = u16::from_be_bytes(bytes) as f64;
            let val = apply_math(u)?;
            if scale.is_none() && offset.is_none() {
                Ok(json!(u16::from_be_bytes(bytes)))
            } else {
                Ok(json!(val))
            }
        }
        ModbusDataType::Int32 | ModbusDataType::Uint32 | ModbusDataType::Float32 => {
            if raw_words.len() < 2 {
                return Err(format!(
                    "register '{}' at address {} expects 2 words (4 bytes), got {}",
                    reg_name,
                    wire_address,
                    raw_words.len()
                ));
            }
            let w0 = raw_words[0];
            let w1 = raw_words[1];
            let b0 = (w0 >> 8) as u8;
            let b1 = w0 as u8;
            let b2 = (w1 >> 8) as u8;
            let b3 = w1 as u8;

            let bytes: [u8; 4] = match byte_order {
                ModbusByteOrder::Abcd => [b0, b1, b2, b3],
                ModbusByteOrder::Cdab => [b2, b3, b0, b1],
                ModbusByteOrder::Badc => [b1, b0, b3, b2],
                ModbusByteOrder::Dcba => [b3, b2, b1, b0],
            };

            match data_type {
                ModbusDataType::Int32 => {
                    let i = i32::from_be_bytes(bytes) as f64;
                    let val = apply_math(i)?;
                    if scale.is_none() && offset.is_none() {
                        Ok(json!(i32::from_be_bytes(bytes)))
                    } else {
                        Ok(json!(val))
                    }
                }
                ModbusDataType::Uint32 => {
                    let u = u32::from_be_bytes(bytes) as f64;
                    let val = apply_math(u)?;
                    if scale.is_none() && offset.is_none() {
                        Ok(json!(u32::from_be_bytes(bytes)))
                    } else {
                        Ok(json!(val))
                    }
                }
                ModbusDataType::Float32 => {
                    let f = f32::from_be_bytes(bytes) as f64;
                    let val = apply_math(f)?;
                    Ok(json!(val))
                }
                _ => unreachable!(),
            }
        }
        ModbusDataType::Int64 | ModbusDataType::Uint64 | ModbusDataType::Float64 => {
            if raw_words.len() < 4 {
                return Err(format!(
                    "register '{}' at address {} expects 4 words (8 bytes), got {}",
                    reg_name,
                    wire_address,
                    raw_words.len()
                ));
            }
            let mut raw_bytes = [0u8; 8];
            for i in 0..4 {
                raw_bytes[i * 2] = (raw_words[i] >> 8) as u8;
                raw_bytes[i * 2 + 1] = raw_words[i] as u8;
            }

            let bytes: [u8; 8] = match byte_order {
                ModbusByteOrder::Abcd => raw_bytes,
                ModbusByteOrder::Cdab => [
                    raw_bytes[2],
                    raw_bytes[3],
                    raw_bytes[0],
                    raw_bytes[1],
                    raw_bytes[6],
                    raw_bytes[7],
                    raw_bytes[4],
                    raw_bytes[5],
                ],
                ModbusByteOrder::Badc => [
                    raw_bytes[1],
                    raw_bytes[0],
                    raw_bytes[3],
                    raw_bytes[2],
                    raw_bytes[5],
                    raw_bytes[4],
                    raw_bytes[7],
                    raw_bytes[6],
                ],
                ModbusByteOrder::Dcba => [
                    raw_bytes[7],
                    raw_bytes[6],
                    raw_bytes[5],
                    raw_bytes[4],
                    raw_bytes[3],
                    raw_bytes[2],
                    raw_bytes[1],
                    raw_bytes[0],
                ],
            };

            match data_type {
                ModbusDataType::Int64 => {
                    let i = i64::from_be_bytes(bytes) as f64;
                    let val = apply_math(i)?;
                    if scale.is_none() && offset.is_none() {
                        Ok(json!(i64::from_be_bytes(bytes)))
                    } else {
                        Ok(json!(val))
                    }
                }
                ModbusDataType::Uint64 => {
                    let u = u64::from_be_bytes(bytes) as f64;
                    let val = apply_math(u)?;
                    if scale.is_none() && offset.is_none() {
                        Ok(json!(u64::from_be_bytes(bytes)))
                    } else {
                        Ok(json!(val))
                    }
                }
                ModbusDataType::Float64 => {
                    let f = f64::from_be_bytes(bytes);
                    let val = apply_math(f)?;
                    Ok(json!(val))
                }
                _ => unreachable!(),
            }
        }
        ModbusDataType::String => {
            let mut s = String::new();
            for &w in raw_words {
                let (c1, c2) = match byte_order {
                    ModbusByteOrder::Badc | ModbusByteOrder::Dcba => {
                        ((w & 0xFF) as u8, (w >> 8) as u8)
                    }
                    _ => ((w >> 8) as u8, (w & 0xFF) as u8),
                };
                if c1 != 0 {
                    s.push(c1 as char);
                }
                if c2 != 0 {
                    s.push(c2 as char);
                }
            }
            Ok(Value::String(s))
        }
    }
}

/// Native Async Modbus Client maintaining an incrementing Transaction ID (tid)
/// and socket resync on mismatch or timeout.
pub struct ModbusClient {
    stream: Option<TcpStream>,
    endpoint: String,
    is_rtu: bool,
    timeout: Duration,
    tid: Arc<AtomicU16>,
}

impl ModbusClient {
    pub fn new(endpoint: &str, is_rtu: bool, timeout_ms: u64) -> Self {
        Self {
            stream: None,
            endpoint: endpoint.to_string(),
            is_rtu,
            timeout: Duration::from_millis(timeout_ms.max(100)),
            tid: Arc::new(AtomicU16::new(1)),
        }
    }

    async fn ensure_connected(&mut self) -> Result<&mut TcpStream> {
        if self.stream.is_none() {
            let s = tokio::time::timeout(self.timeout, TcpStream::connect(&self.endpoint))
                .await
                .context("Modbus TCP connect timed out")?
                .context(format!(
                    "Failed to connect to Modbus endpoint {}",
                    self.endpoint
                ))?;
            let _ = s.set_nodelay(true);
            self.stream = Some(s);
        }
        Ok(self.stream.as_mut().unwrap())
    }

    pub fn disconnect(&mut self) {
        self.stream = None;
    }

    /// Dispatches a read request (FC01, FC02, FC03, FC04) and parses the response.
    pub async fn read_range(&mut self, range: &CoalescedRange) -> Result<Vec<u16>> {
        let req_tid = self.tid.fetch_add(1, Ordering::SeqCst);
        let fc = range.function.code();
        let unit_id = range.unit_id;
        let addr = range.start_address;
        let qty = range.quantity;

        let mut pdu = [0u8; 5];
        pdu[0] = fc;
        pdu[1] = (addr >> 8) as u8;
        pdu[2] = addr as u8;
        pdu[3] = (qty >> 8) as u8;
        pdu[4] = qty as u8;

        let response_pdu = if self.is_rtu {
            self.send_and_recv_rtu(unit_id, &pdu).await?
        } else {
            self.send_and_recv_tcp(req_tid, unit_id, &pdu).await?
        };

        if response_pdu.is_empty() {
            bail!("Empty Modbus response PDU");
        }

        let resp_fc = response_pdu[0];
        if (resp_fc & 0x80) != 0 {
            let exc = if response_pdu.len() > 1 {
                response_pdu[1]
            } else {
                0
            };
            let exc_name = match exc {
                0x01 => " (Illegal Function)",
                0x02 => " (Illegal Data Address)",
                0x03 => " (Illegal Data Value)",
                0x04 => " (Slave Device Failure)",
                0x05 => " (Acknowledge)",
                0x06 => " (Slave Device Busy)",
                _ => "",
            };
            bail!(
                "Modbus exception {:#04x}{} for function {} (unit {}, address {})",
                exc,
                exc_name,
                fc,
                unit_id,
                addr
            );
        }

        if resp_fc != fc {
            bail!(
                "Modbus function code mismatch: expected {}, got {}",
                fc,
                resp_fc
            );
        }

        if response_pdu.len() < 2 {
            bail!("Truncated Modbus response: missing byte count");
        }

        let byte_count = response_pdu[1] as usize;
        let data_bytes = &response_pdu[2..];
        if data_bytes.len() < byte_count {
            bail!(
                "Truncated Modbus data: expected {} bytes, got {}",
                byte_count,
                data_bytes.len()
            );
        }

        if range.function.is_bit_access() {
            // Unpack coils / discrete inputs into 0/1 words
            let mut words = Vec::with_capacity(qty as usize);
            for bit_idx in 0..qty as usize {
                let byte_idx = bit_idx / 8;
                let bit_pos = bit_idx % 8;
                let val = if byte_idx < data_bytes.len() {
                    (data_bytes[byte_idx] >> bit_pos) & 1
                } else {
                    0
                };
                words.push(val as u16);
            }
            Ok(words)
        } else {
            // Unpack 16-bit registers (Big-Endian per Modbus spec)
            let mut words = Vec::with_capacity(byte_count / 2);
            let mut i = 0;
            while i + 1 < byte_count {
                words.push(u16::from_be_bytes([data_bytes[i], data_bytes[i + 1]]));
                i += 2;
            }
            Ok(words)
        }
    }

    /// Dispatches a single register / coil write (FC05, FC06, FC15, FC16).
    pub async fn write_pdu(&mut self, unit_id: u8, pdu: &[u8]) -> Result<Vec<u8>> {
        let req_tid = self.tid.fetch_add(1, Ordering::SeqCst);
        if self.is_rtu {
            self.send_and_recv_rtu(unit_id, pdu).await
        } else {
            self.send_and_recv_tcp(req_tid, unit_id, pdu).await
        }
    }

    async fn send_and_recv_tcp(
        &mut self,
        req_tid: u16,
        unit_id: u8,
        pdu: &[u8],
    ) -> Result<Vec<u8>> {
        let timeout = self.timeout;
        let fut = async {
            let stream = self.ensure_connected().await?;
            // Build MBAP frame
            let length = (pdu.len() + 1) as u16;
            let mut frame = Vec::with_capacity(7 + pdu.len());
            frame.extend_from_slice(&req_tid.to_be_bytes());
            frame.extend_from_slice(&[0x00, 0x00]); // Modbus Protocol ID = 0
            frame.extend_from_slice(&length.to_be_bytes());
            frame.push(unit_id);
            frame.extend_from_slice(pdu);

            stream.write_all(&frame).await?;

            // Read 7-byte MBAP header
            let mut mbap_buf = [0u8; 7];
            stream.read_exact(&mut mbap_buf).await?;

            let resp_tid = u16::from_be_bytes([mbap_buf[0], mbap_buf[1]]);
            let resp_proto = u16::from_be_bytes([mbap_buf[2], mbap_buf[3]]);
            let resp_len = u16::from_be_bytes([mbap_buf[4], mbap_buf[5]]);
            let resp_unit = mbap_buf[6];

            if resp_proto != 0 {
                bail!("Invalid Modbus protocol ID {}", resp_proto);
            }
            if resp_tid != req_tid {
                bail!(
                    "Modbus TCP transaction ID mismatch: expected {}, got {}",
                    req_tid,
                    resp_tid
                );
            }
            if resp_unit != unit_id {
                bail!(
                    "Modbus unit ID mismatch: expected {}, got {}",
                    unit_id,
                    resp_unit
                );
            }
            if resp_len < 2 {
                bail!("Invalid Modbus response length {}", resp_len);
            }

            let pdu_len = (resp_len - 1) as usize;
            let mut pdu_buf = vec![0u8; pdu_len];
            stream.read_exact(&mut pdu_buf).await?;
            Ok(pdu_buf)
        };

        match tokio::time::timeout(timeout, fut).await {
            Ok(Ok(pdu)) => Ok(pdu),
            Ok(Err(e)) => {
                // Drop and disconnect socket so corrupted frames never poison next polls
                self.disconnect();
                Err(e)
            }
            Err(_) => {
                self.disconnect();
                bail!("Modbus TCP request timed out after {:?}", timeout)
            }
        }
    }

    async fn send_and_recv_rtu(&mut self, unit_id: u8, pdu: &[u8]) -> Result<Vec<u8>> {
        let timeout = self.timeout;
        let fut = async {
            let stream = self.ensure_connected().await?;
            let mut frame = Vec::with_capacity(pdu.len() + 3);
            frame.push(unit_id);
            frame.extend_from_slice(pdu);
            let crc = modbus_crc16(&frame);
            frame.extend_from_slice(&crc.to_le_bytes()); // RTU CRC is Little-Endian

            stream.write_all(&frame).await?;

            // Read RTU response
            let mut buf = [0u8; 256];
            let n = stream.read(&mut buf).await?;
            if n < 4 {
                bail!("Modbus RTU response too short ({} bytes)", n);
            }
            let resp_frame = &buf[..n];
            let payload = &resp_frame[..n - 2];
            let resp_crc = u16::from_le_bytes([resp_frame[n - 2], resp_frame[n - 1]]);
            let calc_crc = modbus_crc16(payload);
            if resp_crc != calc_crc {
                bail!(
                    "Modbus RTU CRC mismatch: received {:#06x}, calculated {:#06x}",
                    resp_crc,
                    calc_crc
                );
            }
            if payload[0] != unit_id {
                bail!(
                    "Modbus RTU unit ID mismatch: expected {}, got {}",
                    unit_id,
                    payload[0]
                );
            }
            Ok(payload[1..].to_vec())
        };

        match tokio::time::timeout(timeout, fut).await {
            Ok(Ok(pdu)) => Ok(pdu),
            Ok(Err(e)) => {
                self.disconnect();
                Err(e)
            }
            Err(_) => {
                self.disconnect();
                bail!("Modbus RTU request timed out after {:?}", timeout)
            }
        }
    }
}

/// Reads a one-shot snapshot of all configured Modbus registers (used for table scans and point lookups).
pub async fn modbus_read_snapshot(config: &ModbusConfig) -> Result<HashMap<String, Value>> {
    let prep_regs: Vec<PreparedRegister> = config
        .registers
        .iter()
        .map(|r| {
            PreparedRegister::from_config(
                r,
                config.default_byte_order(),
                config.address_base,
                config.plc_addressing,
            )
        })
        .collect::<Result<Vec<_>>>()?;

    let coalesced = coalesce_registers(&prep_regs, config.effective_unit_id(), config.max_gap);
    let mut client = ModbusClient::new(config.endpoint(), config.is_rtu(), config.timeout_ms);

    let mut record = HashMap::new();

    for range in &coalesced {
        let words = client.read_range(range).await?;
        for reg in &range.registers {
            let offset = (reg.wire_address.saturating_sub(range.start_address)) as usize;
            let end = offset + reg.word_count as usize;
            if end <= words.len() {
                let slice = &words[offset..end];
                match decode_register_value(
                    reg.data_type,
                    reg.byte_order,
                    slice,
                    reg.scale,
                    reg.offset,
                    &reg.name,
                    reg.wire_address,
                ) {
                    Ok(val) => {
                        record.insert(reg.name.clone(), val);
                    }
                    Err(e) => {
                        bail!("Failed decoding register '{}': {}", reg.name, e);
                    }
                }
            }
        }
    }

    Ok(record)
}

/// Modbus Stream Source worker.
pub struct ModbusSource {
    config: ModbusConfig,
    tx: StreamSender,
}

impl ModbusSource {
    pub fn new(config: ModbusConfig, tx: StreamSender) -> Self {
        Self { config, tx }
    }

    pub fn spawn(self, mut cancel_rx: watch::Receiver<bool>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let default_order = self.config.default_byte_order();
            let prep_regs = match self
                .config
                .registers
                .iter()
                .map(|r| {
                    PreparedRegister::from_config(
                        r,
                        default_order,
                        self.config.address_base,
                        self.config.plc_addressing,
                    )
                })
                .collect::<Result<Vec<_>>>()
            {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!("Modbus register configuration error: {}", e);
                    return;
                }
            };

            let coalesced = coalesce_registers(
                &prep_regs,
                self.config.effective_unit_id(),
                self.config.max_gap,
            );

            let mut client = ModbusClient::new(
                self.config.endpoint(),
                self.config.is_rtu(),
                self.config.timeout_ms,
            );

            let mut interval =
                tokio::time::interval(Duration::from_millis(self.config.interval_ms.max(10)));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            while !*cancel_rx.borrow() {
                tokio::select! {
                    _ = interval.tick() => {}
                    _ = cancel_rx.changed() => break,
                }

                if *cancel_rx.borrow() {
                    break;
                }

                let mut record_map: HashMap<String, Value> = HashMap::new();
                let mut decode_errors: Vec<String> = Vec::new();
                let mut raw_bytes_read: Vec<u8> = Vec::new();

                for range in &coalesced {
                    match client.read_range(range).await {
                        Ok(words) => {
                            for w in &words {
                                raw_bytes_read.extend_from_slice(&w.to_be_bytes());
                            }
                            for reg in &range.registers {
                                let offset =
                                    (reg.wire_address.saturating_sub(range.start_address)) as usize;
                                let end = offset + reg.word_count as usize;
                                if end <= words.len() {
                                    let slice = &words[offset..end];
                                    match decode_register_value(
                                        reg.data_type,
                                        reg.byte_order,
                                        slice,
                                        reg.scale,
                                        reg.offset,
                                        &reg.name,
                                        reg.wire_address,
                                    ) {
                                        Ok(val) => {
                                            record_map.insert(reg.name.clone(), val);
                                        }
                                        Err(err_msg) => {
                                            decode_errors.push(err_msg);
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Modbus poll error on range starting at {} (function {:?}): {}",
                                range.start_address,
                                range.function,
                                e
                            );
                            decode_errors.push(format!(
                                "Modbus poll failure at address {}: {}",
                                range.start_address, e
                            ));
                        }
                    }
                }

                // If decode errors occurred (e.g. NaN/Inf or network truncation),
                // emit record with __raw_error__ and __raw_payload__ for Track C DLQ routing!
                if !decode_errors.is_empty() {
                    let combined_err = decode_errors.join("; ");
                    let raw_hex = raw_bytes_read
                        .iter()
                        .map(|b| format!("{:02X}", b))
                        .collect::<Vec<_>>()
                        .join(" ");

                    let mut err_record = HashMap::new();
                    err_record.insert("__raw_error__".to_string(), Value::String(combined_err));
                    err_record.insert(
                        "__raw_payload__".to_string(),
                        Value::String(format!("raw Modbus bytes: [{}]", raw_hex)),
                    );

                    let stream_rec = StreamRecord::new(err_record);
                    if matches!(
                        self.tx.send(stream_rec).await,
                        Err(rekuiper_core::PublishError::Closed)
                    ) {
                        break;
                    }
                } else if !record_map.is_empty() {
                    let stream_rec = StreamRecord::new(record_map);
                    if matches!(
                        self.tx.send(stream_rec).await,
                        Err(rekuiper_core::PublishError::Closed)
                    ) {
                        break;
                    }
                }
            }
        })
    }
}

/// Modbus Sink configuration for writing rule outputs to PLC coils or holding registers.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModbusSinkConfig {
    #[serde(default = "default_transport")]
    pub transport: String,
    #[serde(default = "default_endpoint")]
    pub endpoint: String,
    #[serde(default)]
    pub server: Option<String>,
    #[serde(default)]
    pub address: Option<String>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default = "default_unit_id")]
    pub unit_id: u8,
    #[serde(default)]
    pub slave_id: Option<u8>,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default = "default_byte_order")]
    pub byte_order: String,
    #[serde(default)]
    pub address_base: Option<u16>,
    #[serde(default)]
    pub plc_addressing: bool,
    #[serde(default = "default_function")]
    pub function: String, // "holding" or "coil"
    pub register_address: u16,
    #[serde(default = "default_data_type")]
    pub data_type: String,
    #[serde(default)]
    pub field: Option<String>,
}

impl Default for ModbusSinkConfig {
    fn default() -> Self {
        Self {
            transport: default_transport(),
            endpoint: default_endpoint(),
            server: None,
            address: None,
            url: None,
            unit_id: default_unit_id(),
            slave_id: None,
            timeout_ms: default_timeout_ms(),
            byte_order: default_byte_order(),
            address_base: None,
            plc_addressing: false,
            function: default_function(),
            register_address: 0,
            data_type: default_data_type(),
            field: None,
        }
    }
}

impl ModbusSinkConfig {
    pub fn endpoint(&self) -> &str {
        if let Some(ref s) = self.server {
            if !s.trim().is_empty() {
                return s.trim();
            }
        }
        if let Some(ref a) = self.address {
            if !a.trim().is_empty() {
                return a.trim();
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
        DEFAULT_MODBUS_ENDPOINT
    }

    pub fn effective_unit_id(&self) -> u8 {
        self.slave_id.unwrap_or(self.unit_id)
    }

    pub fn is_rtu(&self) -> bool {
        self.transport.eq_ignore_ascii_case("rtu")
            || self.transport.eq_ignore_ascii_case("rtu_over_tcp")
    }

    pub fn wire_address(&self) -> u16 {
        let mut addr = self.register_address;
        if self.plc_addressing {
            if addr >= 40001 {
                addr -= 40001;
            } else if addr >= 1 {
                addr -= 1;
            }
        } else if let Some(base) = self.address_base {
            if base == 1 {
                addr = addr.saturating_sub(1);
            }
        }
        addr
    }
}

/// Modbus Sink actor writing rule output records to coils or registers.
pub struct ModbusSink {
    client: tokio::sync::Mutex<ModbusClient>,
    config: ModbusSinkConfig,
}

impl ModbusSink {
    pub fn new(config: ModbusSinkConfig) -> Self {
        let client = ModbusClient::new(config.endpoint(), config.is_rtu(), config.timeout_ms);
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
            // Nothing to write for this record
            return Ok(());
        };

        let is_coil = self.config.function.eq_ignore_ascii_case("coil");
        let wire_addr = self.config.wire_address();
        let unit_id = self.config.effective_unit_id();

        let pdu = if is_coil {
            let b = val
                .as_bool()
                .unwrap_or_else(|| val.as_i64().unwrap_or(0) != 0);
            let val_word: u16 = if b { 0xFF00 } else { 0x0000 };
            vec![
                0x05, // FC05 Write Single Coil
                (wire_addr >> 8) as u8,
                wire_addr as u8,
                (val_word >> 8) as u8,
                val_word as u8,
            ]
        } else {
            // Holding register write (FC06 or FC16)
            let dt =
                ModbusDataType::parse(&self.config.data_type).map_err(|e| (true, e.to_string()))?;
            let bo = ModbusByteOrder::parse(&self.config.byte_order);

            match dt {
                ModbusDataType::Int16 | ModbusDataType::Uint16 => {
                    let num = val
                        .as_i64()
                        .unwrap_or_else(|| val.as_f64().unwrap_or(0.0) as i64)
                        as u16;
                    let (b0, b1) = match bo {
                        ModbusByteOrder::Badc | ModbusByteOrder::Dcba => {
                            (num as u8, (num >> 8) as u8)
                        }
                        _ => ((num >> 8) as u8, num as u8),
                    };
                    vec![0x06, (wire_addr >> 8) as u8, wire_addr as u8, b0, b1]
                }
                ModbusDataType::Float32 => {
                    let f = val.as_f64().unwrap_or(0.0) as f32;
                    if !f.is_finite() {
                        return Err((
                            true,
                            format!(
                                "Cannot write non-finite float32 ({:?}) to Modbus register",
                                f
                            ),
                        ));
                    }
                    let be = f.to_be_bytes();
                    let (b0, b1, b2, b3) = match bo {
                        ModbusByteOrder::Abcd => (be[0], be[1], be[2], be[3]),
                        ModbusByteOrder::Cdab => (be[2], be[3], be[0], be[1]),
                        ModbusByteOrder::Badc => (be[1], be[0], be[3], be[2]),
                        ModbusByteOrder::Dcba => (be[3], be[2], be[1], be[0]),
                    };
                    // FC16 Write Multiple Registers
                    vec![
                        0x10,
                        (wire_addr >> 8) as u8,
                        wire_addr as u8,
                        0x00,
                        0x02, // 2 registers
                        0x04, // 4 bytes
                        b0,
                        b1,
                        b2,
                        b3,
                    ]
                }
                _ => {
                    let num = val
                        .as_i64()
                        .unwrap_or_else(|| val.as_f64().unwrap_or(0.0) as i64)
                        as u16;
                    vec![
                        0x06,
                        (wire_addr >> 8) as u8,
                        wire_addr as u8,
                        (num >> 8) as u8,
                        num as u8,
                    ]
                }
            }
        };

        // Execute write through client lock
        let mut client = self.client.lock().await;
        match client.write_pdu(unit_id, &pdu).await {
            Ok(resp_pdu) => {
                if resp_pdu.is_empty() {
                    return Err((false, "Empty Modbus write response".to_string()));
                }
                let resp_fc = resp_pdu[0];
                if (resp_fc & 0x80) != 0 {
                    let exc = if resp_pdu.len() > 1 { resp_pdu[1] } else { 0 };
                    // Modbus exception 0x01 (Illegal Function), 0x02 (Illegal Data Address),
                    // 0x03 (Illegal Data Value) are permanent errors; 0x06 is retryable
                    let is_permanent = matches!(exc, 0x01..=0x03);
                    return Err((
                        is_permanent,
                        format!(
                            "Modbus write exception {:#04x} at address {}",
                            exc, wire_addr
                        ),
                    ));
                }
                Ok(())
            }
            Err(e) => Err((false, format!("Modbus write failed: {}", e))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_modbus_crc16() {
        // Standard Modbus RTU request: [0x01, 0x03, 0x00, 0x00, 0x00, 0x0A]
        // Standard expected CRC is 0xCDC5 (in RTU frame sent Little-Endian as [0xC5, 0xCD])
        let req = [0x01, 0x03, 0x00, 0x00, 0x00, 0x0A];
        let crc = modbus_crc16(&req);
        assert_eq!(crc, 0xCDC5);
    }

    #[test]
    fn test_smart_coalescer() {
        let regs = vec![
            PreparedRegister {
                name: "temp".to_string(),
                function: ModbusFunction::Holding,
                wire_address: 0,
                data_type: ModbusDataType::Float32,
                word_count: 2,
                byte_order: ModbusByteOrder::Cdab,
                scale: None,
                offset: None,
                length: None,
            },
            PreparedRegister {
                name: "pressure".to_string(),
                function: ModbusFunction::Holding,
                wire_address: 5,
                data_type: ModbusDataType::Float32,
                word_count: 2,
                byte_order: ModbusByteOrder::Cdab,
                scale: None,
                offset: None,
                length: None,
            },
            PreparedRegister {
                name: "remote_reg".to_string(),
                function: ModbusFunction::Holding,
                wire_address: 50, // Gap of 43 > max_gap (10)
                data_type: ModbusDataType::Int16,
                word_count: 1,
                byte_order: ModbusByteOrder::Abcd,
                scale: None,
                offset: None,
                length: None,
            },
        ];

        let coalesced = coalesce_registers(&regs, 1, 10);
        // Expect 2 ranges: range 1 from address 0 to 7 (quantity 7), range 2 at address 50
        assert_eq!(coalesced.len(), 2);
        assert_eq!(coalesced[0].start_address, 0);
        assert_eq!(coalesced[0].quantity, 7); // Covers 0..2 and 5..7
        assert_eq!(coalesced[0].registers.len(), 2);

        assert_eq!(coalesced[1].start_address, 50);
        assert_eq!(coalesced[1].quantity, 1);
        assert_eq!(coalesced[1].registers.len(), 1);
    }

    #[test]
    fn test_endianness_decoding() {
        // Value 25.5 as float32 in IEEE-754: 0x41CC0000
        // ABCD: [0x41, 0xCC, 0x00, 0x00] -> words: [0x41CC, 0x0000]
        // CDAB: [0x00, 0x00, 0x41, 0xCC] -> words: [0x0000, 0x41CC]
        // BADC: [0xCC, 0x41, 0x00, 0x00] -> words: [0xCC41, 0x0000]
        // DCBA: [0x00, 0x00, 0xCC, 0x41] -> words: [0x0000, 0xCC41]

        let abcd_words = [0x41CC, 0x0000];
        let val_abcd = decode_register_value(
            ModbusDataType::Float32,
            ModbusByteOrder::Abcd,
            &abcd_words,
            None,
            None,
            "t",
            0,
        )
        .unwrap();
        assert_eq!(val_abcd.as_f64().unwrap(), 25.5);

        let cdab_words = [0x0000, 0x41CC];
        let val_cdab = decode_register_value(
            ModbusDataType::Float32,
            ModbusByteOrder::Cdab,
            &cdab_words,
            None,
            None,
            "t",
            0,
        )
        .unwrap();
        assert_eq!(val_cdab.as_f64().unwrap(), 25.5);

        let badc_words = [0xCC41, 0x0000];
        let val_badc = decode_register_value(
            ModbusDataType::Float32,
            ModbusByteOrder::Badc,
            &badc_words,
            None,
            None,
            "t",
            0,
        )
        .unwrap();
        assert_eq!(val_badc.as_f64().unwrap(), 25.5);

        let dcba_words = [0x0000, 0xCC41];
        let val_dcba = decode_register_value(
            ModbusDataType::Float32,
            ModbusByteOrder::Dcba,
            &dcba_words,
            None,
            None,
            "t",
            0,
        )
        .unwrap();
        assert_eq!(val_dcba.as_f64().unwrap(), 25.5);
    }

    #[test]
    fn test_nan_infinity_rejection() {
        // +Infinity: 0x7F800000
        let inf_words = [0x7F80, 0x0000];
        let err_inf = decode_register_value(
            ModbusDataType::Float32,
            ModbusByteOrder::Abcd,
            &inf_words,
            None,
            None,
            "temp",
            4,
        );
        assert!(err_inf.is_err());
        assert!(err_inf.unwrap_err().contains("non-finite float"));

        // NaN: 0x7FC00000
        let nan_words = [0x7FC0, 0x0000];
        let err_nan = decode_register_value(
            ModbusDataType::Float32,
            ModbusByteOrder::Abcd,
            &nan_words,
            None,
            None,
            "temp",
            4,
        );
        assert!(err_nan.is_err());
        assert!(err_nan.unwrap_err().contains("non-finite float"));
    }

    #[test]
    fn test_scaling_and_offset() {
        // Raw int16: 250 -> scale 0.1, offset -5.0 -> 250 * 0.1 - 5.0 = 20.0
        let words = [250u16];
        let val = decode_register_value(
            ModbusDataType::Int16,
            ModbusByteOrder::Abcd,
            &words,
            Some(0.1),
            Some(-5.0),
            "t",
            0,
        )
        .unwrap();
        assert_eq!(val.as_f64().unwrap(), 20.0);
    }

    #[test]
    fn test_plc_addressing() {
        let r1 = ModbusRegister {
            name: "speed".to_string(),
            function: "holding".to_string(),
            address: 40001,
            data_type: "uint16".to_string(),
            byte_order: None,
            scale: None,
            offset: None,
            length: None,
        };
        let prep = PreparedRegister::from_config(&r1, ModbusByteOrder::Abcd, None, true).unwrap();
        assert_eq!(prep.wire_address, 0);

        let r2 = ModbusRegister {
            name: "pressure".to_string(),
            function: "input".to_string(),
            address: 30005,
            data_type: "uint16".to_string(),
            byte_order: None,
            scale: None,
            offset: None,
            length: None,
        };
        let prep2 = PreparedRegister::from_config(&r2, ModbusByteOrder::Abcd, None, true).unwrap();
        assert_eq!(prep2.wire_address, 4);
    }
}
