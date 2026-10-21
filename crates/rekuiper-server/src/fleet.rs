use anyhow::{Context, Result};
use axum::{
    body::Body,
    extract::{Path, Query, State, WebSocketUpgrade},
    http::{Request, StatusCode},
    response::{IntoResponse, Response},
    routing::{any, get, post},
    Json, Router,
};
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use futures::{Sink, Stream};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::agent::{read_frame, send_frame, AgentControlCommand, AgentHeartbeat};
use crate::state::AppState;
use rekuiper_core::kv::KvStore;

// ============================================================================
// Data Models for Fleet Management
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FleetDevice {
    pub device_id: String,
    pub name: String,
    pub plant: String,
    pub region: String,
    pub device_type: String,
    pub status: String, // "online", "offline", "degraded"
    pub version: String,
    pub cpu_usage: f32,
    pub memory_bytes: u64,
    pub active_snapshot_id: Option<String>,
    pub active_snapshot_hash: u64,
    pub rules_count: usize,
    pub running_rules: usize,
    pub dlq_count: u64,
    pub last_heartbeat: i64,
    pub registered_at: i64,
    #[serde(default)]
    pub last_rollback_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FleetGroupSummary {
    pub name: String,
    pub category: String, // "plant", "region", "device_type"
    pub total_devices: usize,
    pub online_devices: usize,
    pub offline_devices: usize,
    pub degraded_devices: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FleetSnapshot {
    pub snapshot_id: String,
    pub name: String,
    pub description: String,
    pub declarative: Value,
    pub sha256: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RolloutStage {
    Dev,
    Staging,
    Canary,
    Production,
    Completed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RolloutStatus {
    Pending,
    InProgress,
    Promoted,
    RolledBack,
    Halted,
    Completed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FleetRollout {
    pub rollout_id: String,
    pub snapshot_id: String,
    pub target_group: Option<String>,
    pub current_stage: RolloutStage,
    pub status: RolloutStatus,
    pub canary_devices: Vec<String>,
    pub production_devices: Vec<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FleetAuditEntry {
    pub id: String,
    pub timestamp: i64,
    pub action: String,
    pub actor: String,
    pub target_id: String,
    pub details: Value,
}

pub enum FleetServerCmd {
    OpenOutbound(tokio::sync::oneshot::Sender<Result<yamux::Stream, yamux::ConnectionError>>),
    SendControl(
        AgentControlCommand,
        tokio::sync::oneshot::Sender<Result<Value, String>>,
    ),
}

#[derive(Clone)]
pub struct ConnectedDeviceHandle {
    pub cmd_tx: tokio::sync::mpsc::Sender<FleetServerCmd>,
    pub connected_at: i64,
}

// ============================================================================
// Fleet Manager Engine
// ============================================================================

#[derive(Clone)]
pub struct FleetManager {
    pub devices: Arc<RwLock<HashMap<String, FleetDevice>>>,
    pub snapshots: Arc<RwLock<HashMap<String, FleetSnapshot>>>,
    pub rollouts: Arc<RwLock<HashMap<String, FleetRollout>>>,
    pub audit_log: Arc<RwLock<Vec<FleetAuditEntry>>>,
    pub active_connections: Arc<RwLock<HashMap<String, ConnectedDeviceHandle>>>,
    pub kv: Option<Arc<dyn KvStore>>,
}

impl Default for FleetManager {
    fn default() -> Self {
        Self::new()
    }
}

impl FleetManager {
    pub fn new() -> Self {
        Self {
            devices: Arc::new(RwLock::new(HashMap::new())),
            snapshots: Arc::new(RwLock::new(HashMap::new())),
            rollouts: Arc::new(RwLock::new(HashMap::new())),
            audit_log: Arc::new(RwLock::new(Vec::new())),
            active_connections: Arc::new(RwLock::new(HashMap::new())),
            kv: None,
        }
    }

    pub fn new_with_kv(kv: Arc<dyn KvStore>) -> Self {
        let mut mgr = Self::new();
        mgr.kv = Some(kv);
        mgr
    }

    pub fn record_audit(&self, action: &str, actor: &str, target_id: &str, details: Value) {
        let entry = FleetAuditEntry {
            id: format!("aud_{}", uuid::Uuid::new_v4().simple()),
            timestamp: chrono::Utc::now().timestamp(),
            action: action.to_string(),
            actor: actor.to_string(),
            target_id: target_id.to_string(),
            details,
        };

        {
            let mut log = self.audit_log.write();
            log.push(entry.clone());
            if log.len() > 1000 {
                log.remove(0);
            }
        }

        if let Some(kv) = &self.kv {
            let kv_clone = kv.clone();
            let key = entry.id.clone();
            let json_str = serde_json::to_string(&entry).unwrap_or_default();
            tokio::spawn(async move {
                let _ = kv_clone.set("fleet_audit", &key, &json_str).await;
            });
        }
    }

    pub fn register_or_update_heartbeat(&self, hb: AgentHeartbeat) {
        let now = chrono::Utc::now().timestamp();
        let device_id = hb.agent_id.clone();

        let mut devices = self.devices.write();
        let status =
            if hb.status.as_deref() == Some("degraded") || hb.last_rollback_reason.is_some() {
                "degraded".to_string()
            } else {
                "online".to_string()
            };

        if let Some(existing) = devices.get_mut(&device_id) {
            existing.status = status;
            existing.cpu_usage = hb.cpu_usage;
            existing.memory_bytes = hb.memory_bytes;
            existing.rules_count = hb.rules_count;
            existing.running_rules = hb.running_rules;
            existing.dlq_count = hb.dlq_count;
            existing.active_snapshot_hash = hb.snapshot_hash;
            if hb.snapshot_id.is_some() {
                existing.active_snapshot_id = hb.snapshot_id;
            }
            if hb.last_rollback_reason.is_some() {
                existing.last_rollback_reason = hb.last_rollback_reason;
            }
            existing.last_heartbeat = now;
        } else {
            let dev = FleetDevice {
                device_id: device_id.clone(),
                name: hb.name.unwrap_or_else(|| device_id.clone()),
                plant: hb.plant.unwrap_or_else(|| "Plant-Austin".to_string()),
                region: hb.region.unwrap_or_else(|| "US-South".to_string()),
                device_type: hb.device_type.unwrap_or_else(|| "IPC-Edge".to_string()),
                status,
                version: hb.version.unwrap_or_else(|| "2.0.0".to_string()),
                cpu_usage: hb.cpu_usage,
                memory_bytes: hb.memory_bytes,
                active_snapshot_id: hb.snapshot_id,
                active_snapshot_hash: hb.snapshot_hash,
                rules_count: hb.rules_count,
                running_rules: hb.running_rules,
                dlq_count: hb.dlq_count,
                last_heartbeat: now,
                registered_at: now,
                last_rollback_reason: hb.last_rollback_reason,
            };
            devices.insert(device_id.clone(), dev.clone());
        }

        if let Some(kv) = &self.kv {
            if let Some(dev) = devices.get(&device_id) {
                let kv_clone = kv.clone();
                let key = device_id.clone();
                let json_str = serde_json::to_string(dev).unwrap_or_default();
                tokio::spawn(async move {
                    let _ = kv_clone.set("fleet_devices", &key, &json_str).await;
                });
            }
        }
    }

    pub fn set_device_status(&self, device_id: &str, status: &str) {
        let mut devices = self.devices.write();
        if let Some(dev) = devices.get_mut(device_id) {
            dev.status = status.to_string();
            if let Some(kv) = &self.kv {
                let kv_clone = kv.clone();
                let key = device_id.to_string();
                let json_str = serde_json::to_string(dev).unwrap_or_default();
                tokio::spawn(async move {
                    let _ = kv_clone.set("fleet_devices", &key, &json_str).await;
                });
            }
        }
    }

    pub fn create_snapshot(
        &self,
        name: String,
        description: String,
        declarative: Value,
    ) -> Result<FleetSnapshot> {
        let canon_bytes =
            serde_json::to_vec(&declarative).context("Failed to serialize declarative snapshot")?;
        let digest = ring::digest::digest(&ring::digest::SHA256, &canon_bytes);
        let sha256 = digest
            .as_ref()
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<String>();
        let snapshot_id = format!("snap_{}", uuid::Uuid::new_v4().simple());
        let now = chrono::Utc::now().timestamp();

        let snap = FleetSnapshot {
            snapshot_id: snapshot_id.clone(),
            name,
            description,
            declarative,
            sha256,
            created_at: now,
        };

        self.snapshots
            .write()
            .insert(snapshot_id.clone(), snap.clone());
        self.record_audit(
            "snapshot_created",
            "admin",
            &snapshot_id,
            json!({
                "name": snap.name,
                "sha256": snap.sha256,
            }),
        );

        if let Some(kv) = &self.kv {
            let kv_clone = kv.clone();
            let key = snapshot_id;
            let json_str = serde_json::to_string(&snap).unwrap_or_default();
            tokio::spawn(async move {
                let _ = kv_clone.set("fleet_snapshots", &key, &json_str).await;
            });
        }

        Ok(snap)
    }

    pub async fn start_staged_rollout(
        &self,
        snapshot_id: &str,
        target_group: Option<String>,
        auto_promote: bool,
    ) -> Result<FleetRollout> {
        let snap = {
            let guard = self.snapshots.read();
            guard
                .get(snapshot_id)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("Snapshot '{}' not found", snapshot_id))?
        };

        let all_devs: Vec<FleetDevice> = self.devices.read().values().cloned().collect();
        let target_devs: Vec<FleetDevice> = all_devs
            .into_iter()
            .filter(|d| {
                if let Some(ref grp) = target_group {
                    d.plant == *grp || d.region == *grp || d.device_type == *grp || d.name == *grp
                } else {
                    true
                }
            })
            .collect();

        if target_devs.is_empty() {
            return Err(anyhow::anyhow!("No target devices found matching criteria"));
        }

        // Staged cohort calculation: Canary (5% of target devices, at least 1) -> Production (remaining 95%)
        let canary_count = ((target_devs.len() as f32 * 0.05).ceil() as usize)
            .max(1)
            .min(target_devs.len());
        let canary_devs: Vec<String> = target_devs[..canary_count]
            .iter()
            .map(|d| d.device_id.clone())
            .collect();
        let prod_devs: Vec<String> = target_devs[canary_count..]
            .iter()
            .map(|d| d.device_id.clone())
            .collect();

        let rollout_id = format!("ro_{}", uuid::Uuid::new_v4().simple());
        let now = chrono::Utc::now().timestamp();
        let rollout = FleetRollout {
            rollout_id: rollout_id.clone(),
            snapshot_id: snapshot_id.to_string(),
            target_group: target_group.clone(),
            current_stage: RolloutStage::Canary,
            status: RolloutStatus::InProgress,
            canary_devices: canary_devs.clone(),
            production_devices: prod_devs.clone(),
            created_at: now,
            updated_at: now,
            message: format!(
                "Deploying to Canary cohort ({} device(s))",
                canary_devs.len()
            ),
        };

        self.rollouts
            .write()
            .insert(rollout_id.clone(), rollout.clone());
        self.record_audit(
            "rollout_started",
            "admin",
            &rollout_id,
            json!({
                "snapshot_id": snapshot_id,
                "target_group": target_group,
                "canary_devices": canary_devs,
                "production_devices": prod_devs,
            }),
        );

        // 1. Deploy to Canary cohort first
        let mut canary_failed = false;
        let mut failure_reason = String::new();

        for dev_id in &canary_devs {
            let cmd = AgentControlCommand::ApplySnapshot {
                manifest: snap.declarative.clone(),
            };
            let res = self.send_device_control(dev_id, cmd).await;
            match res {
                Ok(val) => {
                    if let Some(status_str) = val.get("status").and_then(|s| s.as_str()) {
                        if status_str == "error" {
                            canary_failed = true;
                            failure_reason = val
                                .get("message")
                                .and_then(|m| m.as_str())
                                .unwrap_or("ApplySnapshot error")
                                .to_string();
                            break;
                        }
                    }
                }
                Err(e) => {
                    canary_failed = true;
                    failure_reason = e.to_string();
                    break;
                }
            }
        }

        // 2. Health check Canary cohort (detect degraded status or watchdog auto-rollback)
        if !canary_failed {
            for dev_id in &canary_devs {
                if let Some(dev) = self.devices.read().get(dev_id) {
                    if dev.status == "degraded" || dev.last_rollback_reason.is_some() {
                        canary_failed = true;
                        failure_reason = dev
                            .last_rollback_reason
                            .clone()
                            .unwrap_or_else(|| "Device entered degraded status".to_string());
                        break;
                    }
                }
            }
        }

        // 3. Health-gated branch: auto-halt on failure, otherwise promote to production
        if canary_failed {
            let halt_msg = format!(
                "Canary failure: {}. Rollout automatically halted & marked rolled_back.",
                failure_reason
            );
            {
                let mut r_guard = self.rollouts.write();
                if let Some(r) = r_guard.get_mut(&rollout_id) {
                    r.status = RolloutStatus::RolledBack;
                    r.message = halt_msg.clone();
                    r.updated_at = chrono::Utc::now().timestamp();
                }
            }
            self.record_audit(
                "rollout_halted",
                "system",
                &rollout_id,
                json!({
                    "reason": failure_reason,
                    "canary_devices": canary_devs,
                }),
            );
            // Trigger rollback on canary devices
            for dev_id in &canary_devs {
                let _ = self
                    .send_device_control(dev_id, AgentControlCommand::Rollback)
                    .await;
            }
            return Ok(self.rollouts.read().get(&rollout_id).cloned().unwrap());
        }

        // Canary healthy! If auto_promote, proceed to Production cohort
        if auto_promote {
            {
                let mut r_guard = self.rollouts.write();
                if let Some(r) = r_guard.get_mut(&rollout_id) {
                    r.current_stage = RolloutStage::Production;
                    r.message = format!(
                        "Canary healthy. Promoting to 100% Production ({} devices)",
                        prod_devs.len()
                    );
                    r.updated_at = chrono::Utc::now().timestamp();
                }
            }
            self.record_audit(
                "rollout_promoted",
                "system",
                &rollout_id,
                json!({
                    "stage": "production",
                    "production_devices": prod_devs,
                }),
            );

            // Deploy to Production cohort
            for dev_id in &prod_devs {
                let cmd = AgentControlCommand::ApplySnapshot {
                    manifest: snap.declarative.clone(),
                };
                let _ = self.send_device_control(dev_id, cmd).await;
            }

            // Mark completed
            {
                let mut r_guard = self.rollouts.write();
                if let Some(r) = r_guard.get_mut(&rollout_id) {
                    r.current_stage = RolloutStage::Completed;
                    r.status = RolloutStatus::Completed;
                    r.message =
                        "Staged rollout promoted to 100% production successfully.".to_string();
                    r.updated_at = chrono::Utc::now().timestamp();
                }
            }
            self.record_audit(
                "rollout_completed",
                "system",
                &rollout_id,
                json!({
                    "canary_count": canary_devs.len(),
                    "production_count": prod_devs.len(),
                }),
            );
        }

        Ok(self.rollouts.read().get(&rollout_id).cloned().unwrap())
    }

    pub async fn promote_rollout(&self, rollout_id: &str) -> Result<FleetRollout> {
        let (snap_id, prod_devs) = {
            let mut r_guard = self.rollouts.write();
            let r = r_guard
                .get_mut(rollout_id)
                .ok_or_else(|| anyhow::anyhow!("Rollout '{}' not found", rollout_id))?;
            r.current_stage = RolloutStage::Production;
            r.message = "Promoted manually to 100% Production".to_string();
            r.updated_at = chrono::Utc::now().timestamp();
            (r.snapshot_id.clone(), r.production_devices.clone())
        };

        let snap = {
            let guard = self.snapshots.read();
            guard
                .get(&snap_id)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("Snapshot not found"))?
        };

        for dev_id in &prod_devs {
            let cmd = AgentControlCommand::ApplySnapshot {
                manifest: snap.declarative.clone(),
            };
            let _ = self.send_device_control(dev_id, cmd).await;
        }

        {
            let mut r_guard = self.rollouts.write();
            if let Some(r) = r_guard.get_mut(rollout_id) {
                r.current_stage = RolloutStage::Completed;
                r.status = RolloutStatus::Completed;
                r.message = "Rollout completed successfully across entire fleet.".to_string();
                r.updated_at = chrono::Utc::now().timestamp();
            }
        }

        self.record_audit(
            "rollout_promoted",
            "admin",
            rollout_id,
            json!({"stage": "production"}),
        );
        Ok(self.rollouts.read().get(rollout_id).cloned().unwrap())
    }

    pub async fn rollback_rollout(&self, rollout_id: &str) -> Result<FleetRollout> {
        let (canary_devs, prod_devs) = {
            let mut r_guard = self.rollouts.write();
            let r = r_guard
                .get_mut(rollout_id)
                .ok_or_else(|| anyhow::anyhow!("Rollout not found"))?;
            r.status = RolloutStatus::RolledBack;
            r.message = "Rollout halted & rolled back manually by administrator.".to_string();
            r.updated_at = chrono::Utc::now().timestamp();
            (r.canary_devices.clone(), r.production_devices.clone())
        };

        for dev_id in canary_devs.iter().chain(prod_devs.iter()) {
            let _ = self
                .send_device_control(dev_id, AgentControlCommand::Rollback)
                .await;
        }

        self.record_audit("rollout_rollback", "admin", rollout_id, json!({}));
        Ok(self.rollouts.read().get(rollout_id).cloned().unwrap())
    }

    pub fn get_groups(&self) -> Vec<FleetGroupSummary> {
        let devices = self.devices.read();
        let mut map: HashMap<(String, String), (usize, usize, usize, usize)> = HashMap::new();

        for dev in devices.values() {
            let is_online = dev.status == "online";
            let is_degraded = dev.status == "degraded";
            let is_offline = !is_online && !is_degraded;

            // Plant category
            let entry = map
                .entry(("plant".to_string(), dev.plant.clone()))
                .or_insert((0, 0, 0, 0));
            entry.0 += 1;
            if is_online {
                entry.1 += 1;
            }
            if is_offline {
                entry.2 += 1;
            }
            if is_degraded {
                entry.3 += 1;
            }

            // Region category
            let entry_reg = map
                .entry(("region".to_string(), dev.region.clone()))
                .or_insert((0, 0, 0, 0));
            entry_reg.0 += 1;
            if is_online {
                entry_reg.1 += 1;
            }
            if is_offline {
                entry_reg.2 += 1;
            }
            if is_degraded {
                entry_reg.3 += 1;
            }

            // Device Type category
            let entry_type = map
                .entry(("device_type".to_string(), dev.device_type.clone()))
                .or_insert((0, 0, 0, 0));
            entry_type.0 += 1;
            if is_online {
                entry_type.1 += 1;
            }
            if is_offline {
                entry_type.2 += 1;
            }
            if is_degraded {
                entry_type.3 += 1;
            }
        }

        map.into_iter()
            .map(
                |((category, name), (total, online, offline, degraded))| FleetGroupSummary {
                    name,
                    category,
                    total_devices: total,
                    online_devices: online,
                    offline_devices: offline,
                    degraded_devices: degraded,
                },
            )
            .collect()
    }

    pub fn get_fleet_metrics(&self) -> Value {
        let devices = self.devices.read();
        let total_devices = devices.len();
        let online_devices = devices.values().filter(|d| d.status == "online").count();
        let degraded_devices = devices.values().filter(|d| d.status == "degraded").count();
        let offline_devices = total_devices.saturating_sub(online_devices + degraded_devices);

        let total_rules: usize = devices.values().map(|d| d.rules_count).sum();
        let total_running_rules: usize = devices.values().map(|d| d.running_rules).sum();
        let total_memory: u64 = devices.values().map(|d| d.memory_bytes).sum();
        let total_dlq: u64 = devices.values().map(|d| d.dlq_count).sum();
        let avg_cpu: f32 = if total_devices > 0 {
            devices.values().map(|d| d.cpu_usage).sum::<f32>() / total_devices as f32
        } else {
            0.0
        };

        let rollouts = self.rollouts.read();
        let active_rollouts = rollouts
            .values()
            .filter(|r| r.status == RolloutStatus::InProgress)
            .count();

        json!({
            "total_devices": total_devices,
            "online_devices": online_devices,
            "offline_devices": offline_devices,
            "degraded_devices": degraded_devices,
            "total_rules": total_rules,
            "total_running_rules": total_running_rules,
            "total_memory_bytes": total_memory,
            "avg_cpu_percent": avg_cpu,
            "total_dlq_errors": total_dlq,
            "active_rollouts": active_rollouts,
            "snapshots_count": self.snapshots.read().len()
        })
    }

    pub fn get_fleet_dlq_alerts(&self) -> Vec<Value> {
        let devices = self.devices.read();
        devices
            .values()
            .filter(|d| {
                d.dlq_count > 0 || d.status == "degraded" || d.last_rollback_reason.is_some()
            })
            .map(|d| {
                json!({
                    "device_id": d.device_id,
                    "name": d.name,
                    "plant": d.plant,
                    "region": d.region,
                    "status": d.status,
                    "dlq_count": d.dlq_count,
                    "last_rollback_reason": d.last_rollback_reason,
                    "last_heartbeat": d.last_heartbeat
                })
            })
            .collect()
    }

    pub async fn open_proxy_stream(&self, device_id: &str) -> Result<yamux::Stream> {
        let handle = {
            let guard = self.active_connections.read();
            guard.get(device_id).cloned().ok_or_else(|| {
                anyhow::anyhow!(
                    "Device '{}' is not currently connected to Control Plane",
                    device_id
                )
            })?
        };

        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        handle
            .cmd_tx
            .send(FleetServerCmd::OpenOutbound(reply_tx))
            .await
            .map_err(|_| {
                anyhow::anyhow!("Connection actor to device '{}' has terminated", device_id)
            })?;

        let res = reply_rx
            .await
            .map_err(|_| anyhow::anyhow!("Proxy stream open request cancelled"))??;
        Ok(res)
    }

    pub async fn send_device_control(
        &self,
        device_id: &str,
        cmd: AgentControlCommand,
    ) -> Result<Value> {
        let handle = {
            let guard = self.active_connections.read();
            guard
                .get(device_id)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("Device '{}' is not connected", device_id))?
        };

        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        handle
            .cmd_tx
            .send(FleetServerCmd::SendControl(cmd, reply_tx))
            .await
            .map_err(|_| anyhow::anyhow!("Device command channel dropped"))?;

        let res = reply_rx
            .await
            .map_err(|_| anyhow::anyhow!("Device command timed out or cancelled"))?
            .map_err(|e| anyhow::anyhow!("{}", e))?;
        Ok(res)
    }
}

// ============================================================================
// Axum WebSocket Adapter for Yamux Server Mode
// ============================================================================

use axum::extract::ws::{Message as AxumWsMessage, WebSocket as AxumWebSocket};

pub struct AxumWsStreamAdapter {
    ws: AxumWebSocket,
    read_buf: Vec<u8>,
    read_pos: usize,
}

impl AxumWsStreamAdapter {
    pub fn new(ws: AxumWebSocket) -> Self {
        Self {
            ws,
            read_buf: Vec::new(),
            read_pos: 0,
        }
    }
}

impl AsyncRead for AxumWsStreamAdapter {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let this = &mut *self;
        if this.read_pos < this.read_buf.len() {
            let available = this.read_buf.len() - this.read_pos;
            let to_copy = available.min(buf.len());
            buf[..to_copy].copy_from_slice(&this.read_buf[this.read_pos..this.read_pos + to_copy]);
            this.read_pos += to_copy;
            if this.read_pos >= this.read_buf.len() {
                this.read_buf.clear();
                this.read_pos = 0;
            }
            return std::task::Poll::Ready(Ok(to_copy));
        }

        loop {
            match std::pin::Pin::new(&mut this.ws).poll_next(cx) {
                std::task::Poll::Ready(Some(Ok(AxumWsMessage::Binary(bin)))) => {
                    if bin.is_empty() {
                        continue;
                    }
                    let to_copy = bin.len().min(buf.len());
                    buf[..to_copy].copy_from_slice(&bin[..to_copy]);
                    if to_copy < bin.len() {
                        this.read_buf = bin[to_copy..].to_vec();
                        this.read_pos = 0;
                    }
                    return std::task::Poll::Ready(Ok(to_copy));
                }
                std::task::Poll::Ready(Some(Ok(AxumWsMessage::Close(_))))
                | std::task::Poll::Ready(None) => {
                    return std::task::Poll::Ready(Ok(0));
                }
                std::task::Poll::Ready(Some(Ok(_))) => {
                    continue;
                }
                std::task::Poll::Ready(Some(Err(e))) => {
                    return std::task::Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        e,
                    )));
                }
                std::task::Poll::Pending => return std::task::Poll::Pending,
            }
        }
    }
}

impl AsyncWrite for AxumWsStreamAdapter {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let this = &mut *self;
        match std::pin::Pin::new(&mut this.ws).poll_ready(cx) {
            std::task::Poll::Ready(Ok(())) => {
                let msg = AxumWsMessage::Binary(buf.to_vec());
                match std::pin::Pin::new(&mut this.ws).start_send(msg) {
                    Ok(()) => std::task::Poll::Ready(Ok(buf.len())),
                    Err(e) => std::task::Poll::Ready(Err(std::io::Error::other(e))),
                }
            }
            std::task::Poll::Ready(Err(e)) => std::task::Poll::Ready(Err(std::io::Error::other(e))),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.ws)
            .poll_flush(cx)
            .map_err(std::io::Error::other)
    }

    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.ws)
            .poll_close(cx)
            .map_err(std::io::Error::other)
    }
}

// ============================================================================
// Server-Side WebSocket Acceptor (/fleet/ws)
// ============================================================================

pub async fn fleet_ws_acceptor(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    ws.on_upgrade(move |socket| handle_fleet_ws_client(socket, state))
}

pub async fn handle_fleet_ws_client(socket: AxumWebSocket, state: AppState) {
    let adapter = AxumWsStreamAdapter::new(socket);
    let mut cfg = yamux::Config::default();
    cfg.set_read_after_close(true);
    let mut server_conn = yamux::Connection::new(adapter, cfg, yamux::Mode::Server);

    let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::channel::<FleetServerCmd>(32);
    let (inbound_tx, mut inbound_rx) = tokio::sync::mpsc::channel::<yamux::Stream>(32);
    let (ctrl_cmd_tx, mut ctrl_cmd_rx) = tokio::sync::mpsc::channel::<(
        AgentControlCommand,
        tokio::sync::oneshot::Sender<Result<Value, String>>,
    )>(16);

    // Driver task: continuously polls inbound streams & processes outbound requests
    let ctrl_cmd_tx_clone = ctrl_cmd_tx.clone();
    let driver_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                inbound_res = futures::future::poll_fn(|cx| server_conn.poll_next_inbound(cx)) => {
                    match inbound_res {
                        Some(Ok(stream)) => {
                            let _ = inbound_tx.send(stream).await;
                        }
                        Some(Err(_)) | None => break,
                    }
                }
                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(FleetServerCmd::OpenOutbound(reply)) => {
                            let res = futures::future::poll_fn(|cx| server_conn.poll_new_outbound(cx)).await;
                            let _ = reply.send(res);
                        }
                        Some(FleetServerCmd::SendControl(cmd, reply)) => {
                            let _ = ctrl_cmd_tx_clone.send((cmd, reply)).await;
                        }
                        None => break,
                    }
                }
            }
        }
    });

    // Await Stream 0 (Client initiates Stream 0 immediately upon connection)
    let ctrl_stream = match inbound_rx.recv().await {
        Some(s) => s,
        None => return,
    };

    let (mut read_half, mut write_half) = futures::AsyncReadExt::split(ctrl_stream);

    // Read initial registration frame
    let first_frame = match read_frame(&mut read_half).await {
        Ok(f) => f,
        Err(_) => return,
    };

    let initial_hb: AgentHeartbeat = match serde_json::from_slice(&first_frame) {
        Ok(hb) => hb,
        Err(e) => {
            tracing::warn!("Failed to parse initial fleet registration frame: {}", e);
            return;
        }
    };

    let device_id = initial_hb.agent_id.clone();
    state.fleet.register_or_update_heartbeat(initial_hb);

    // Register active connection
    state.fleet.active_connections.write().insert(
        device_id.clone(),
        ConnectedDeviceHandle {
            cmd_tx: cmd_tx.clone(),
            connected_at: chrono::Utc::now().timestamp(),
        },
    );
    state
        .fleet
        .record_audit("device_connected", "system", &device_id, json!({}));

    // Stream 0 session loop: manages incoming telemetry frames and pending command dispatches
    let fleet_clone_session = state.fleet.clone();
    let write_task = tokio::spawn(async move {
        let mut pending_reply: Option<tokio::sync::oneshot::Sender<Result<Value, String>>> = None;

        loop {
            tokio::select! {
                cmd_req = ctrl_cmd_rx.recv() => {
                    match cmd_req {
                        Some((cmd, reply)) => {
                            let payload = serde_json::to_vec(&cmd).unwrap_or_default();
                            if let Err(e) = send_frame(&mut write_half, &payload).await {
                                let _ = reply.send(Err(format!("Failed to write command to Stream 0: {}", e)));
                                break;
                            }
                            pending_reply = Some(reply);
                        }
                        None => break,
                    }
                }
                frame_res = read_frame(&mut read_half) => {
                    match frame_res {
                        Ok(bytes) => {
                            if let Ok(val) = serde_json::from_slice::<Value>(&bytes) {
                                if val.get("status").is_some() && pending_reply.is_some() {
                                    if let Some(reply) = pending_reply.take() {
                                        let _ = reply.send(Ok(val));
                                    }
                                    continue;
                                }
                            }
                            if let Ok(hb) = serde_json::from_slice::<AgentHeartbeat>(&bytes) {
                                fleet_clone_session.register_or_update_heartbeat(hb);
                            }
                        }
                        Err(_) => break,
                    }
                }
            }
        }
    });

    // Monitor heartbeat updates and connection closure
    let fleet_clone = state.fleet.clone();
    let dev_id_clone = device_id.clone();

    // Wait until driver or write task exits
    let _ = driver_handle.await;
    write_task.abort();

    // Cleanup on disconnect
    fleet_clone.active_connections.write().remove(&dev_id_clone);
    fleet_clone.set_device_status(&dev_id_clone, "offline");
    fleet_clone.record_audit("device_disconnected", "system", &dev_id_clone, json!({}));
}

// ============================================================================
// REST Handlers for Fleet Catalog & Observability
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct DeviceFilterParams {
    pub plant: Option<String>,
    pub region: Option<String>,
    pub device_type: Option<String>,
    pub status: Option<String>,
    pub q: Option<String>,
}

pub async fn list_devices(
    State(state): State<AppState>,
    Query(params): Query<DeviceFilterParams>,
) -> impl IntoResponse {
    let devices = state.fleet.devices.read();
    let mut list: Vec<FleetDevice> = devices.values().cloned().collect();

    if let Some(ref p) = params.plant {
        list.retain(|d| d.plant.eq_ignore_ascii_case(p));
    }
    if let Some(ref r) = params.region {
        list.retain(|d| d.region.eq_ignore_ascii_case(r));
    }
    if let Some(ref t) = params.device_type {
        list.retain(|d| d.device_type.eq_ignore_ascii_case(t));
    }
    if let Some(ref s) = params.status {
        list.retain(|d| d.status.eq_ignore_ascii_case(s));
    }
    if let Some(ref q) = params.q {
        let q_low = q.to_lowercase();
        list.retain(|d| {
            d.device_id.to_lowercase().contains(&q_low) || d.name.to_lowercase().contains(&q_low)
        });
    }

    Json(list)
}

pub async fn get_device(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    let devices = state.fleet.devices.read();
    if let Some(dev) = devices.get(&id) {
        Json(dev).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("Device '{}' not found", id)})),
        )
            .into_response()
    }
}

pub async fn register_device(
    State(state): State<AppState>,
    Json(payload): Json<FleetDevice>,
) -> impl IntoResponse {
    let dev_id = payload.device_id.clone();
    state
        .fleet
        .devices
        .write()
        .insert(dev_id.clone(), payload.clone());
    state
        .fleet
        .record_audit("device_registered", "admin", &dev_id, json!(payload));
    (StatusCode::CREATED, Json(payload))
}

pub async fn delete_device(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let removed = state.fleet.devices.write().remove(&id);
    state.fleet.active_connections.write().remove(&id);
    if removed.is_some() {
        state
            .fleet
            .record_audit("device_deleted", "admin", &id, json!({}));
        (
            StatusCode::OK,
            Json(json!({"status": "deleted", "device_id": id})),
        )
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("Device '{}' not found", id)})),
        )
    }
}

pub async fn list_groups(State(state): State<AppState>) -> impl IntoResponse {
    let groups = state.fleet.get_groups();
    Json(groups)
}

// ----------------------------------------------------------------------------
// Snapshot Endpoints
// ----------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct CreateSnapshotRequest {
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub declarative: Value,
}

pub async fn create_snapshot_handler(
    State(state): State<AppState>,
    Json(req): Json<CreateSnapshotRequest>,
) -> Response {
    match state
        .fleet
        .create_snapshot(req.name, req.description, req.declarative)
    {
        Ok(snap) => (StatusCode::CREATED, Json(snap)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn list_snapshots(State(state): State<AppState>) -> impl IntoResponse {
    let snaps = state.fleet.snapshots.read();
    let list: Vec<FleetSnapshot> = snaps.values().cloned().collect();
    Json(list)
}

pub async fn get_snapshot(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    let snaps = state.fleet.snapshots.read();
    if let Some(snap) = snaps.get(&id) {
        Json(snap).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("Snapshot '{}' not found", id)})),
        )
            .into_response()
    }
}

// ----------------------------------------------------------------------------
// Rollout Endpoints
// ----------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct CreateRolloutRequest {
    pub snapshot_id: String,
    pub target_group: Option<String>,
    #[serde(default = "default_true")]
    pub auto_promote: bool,
}

fn default_true() -> bool {
    true
}

pub async fn create_rollout_handler(
    State(state): State<AppState>,
    Json(req): Json<CreateRolloutRequest>,
) -> Response {
    match state
        .fleet
        .start_staged_rollout(&req.snapshot_id, req.target_group, req.auto_promote)
        .await
    {
        Ok(rollout) => (StatusCode::CREATED, Json(rollout)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn list_rollouts(State(state): State<AppState>) -> impl IntoResponse {
    let rollouts = state.fleet.rollouts.read();
    let list: Vec<FleetRollout> = rollouts.values().cloned().collect();
    Json(list)
}

pub async fn get_rollout(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    let rollouts = state.fleet.rollouts.read();
    if let Some(r) = rollouts.get(&id) {
        Json(r).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("Rollout '{}' not found", id)})),
        )
            .into_response()
    }
}

pub async fn promote_rollout_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Response {
    match state.fleet.promote_rollout(&id).await {
        Ok(r) => Json(r).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn rollback_rollout_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Response {
    match state.fleet.rollback_rollout(&id).await {
        Ok(r) => Json(r).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

// ----------------------------------------------------------------------------
// Remote HTTP Proxy Tunneling (/api/v1/fleet/devices/:id/proxy/*path)
// ----------------------------------------------------------------------------

pub async fn proxy_root_handler(
    State(state): State<AppState>,
    Path(device_id): Path<String>,
    req: Request<Body>,
) -> Response {
    proxy_internal(state, device_id, "".to_string(), req).await
}

pub async fn proxy_path_handler(
    State(state): State<AppState>,
    Path((device_id, path)): Path<(String, String)>,
    req: Request<Body>,
) -> Response {
    proxy_internal(state, device_id, path, req).await
}

async fn proxy_internal(
    state: AppState,
    device_id: String,
    path: String,
    req: Request<Body>,
) -> Response {
    let method = req.method().clone();
    let query = req
        .uri()
        .query()
        .map(|q| format!("?{}", q))
        .unwrap_or_default();
    let clean_path = path.trim_start_matches('/');
    let target_path = if clean_path == "version" || clean_path.is_empty() {
        format!("/{}", query)
    } else {
        format!("/{}{}", clean_path, query)
    };

    // Open outbound Yamux stream (1..N) to target connected edge device
    let mut stream = match state.fleet.open_proxy_stream(&device_id).await {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": format!("Cannot proxy to device '{}': {}", device_id, e)})),
            )
                .into_response();
        }
    };

    // Format HTTP/1.1 wire request
    let (parts, body) = req.into_parts();
    let body_bytes = match axum::body::to_bytes(body, 32 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };

    let mut wire = format!("{} {} HTTP/1.1\r\n", parts.method, target_path);
    for (k, v) in parts.headers.iter() {
        if k != "host" {
            if let Ok(v_str) = v.to_str() {
                wire.push_str(&format!("{}: {}\r\n", k.as_str(), v_str));
            }
        }
    }
    if !parts.headers.contains_key("content-length") {
        wire.push_str(&format!("content-length: {}\r\n", body_bytes.len()));
    }
    wire.push_str("\r\n");

    let mut out_bytes = wire.into_bytes();
    out_bytes.extend_from_slice(&body_bytes);

    if let Err(e) = stream.write_all(&out_bytes).await {
        return (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error": format!("Proxy write failed: {}", e)})),
        )
            .into_response();
    }
    let _ = stream.flush().await;

    // Read HTTP response from edge agent's local router
    let mut resp_bytes = Vec::new();
    let mut buf = [0u8; 8192];
    let mut expected_total: Option<usize> = None;

    let read_result = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let n = stream.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            resp_bytes.extend_from_slice(&buf[..n]);

            if expected_total.is_none() {
                if let Some(pos) = resp_bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    let header_part = std::str::from_utf8(&resp_bytes[..pos]).unwrap_or("");
                    let mut cl: Option<usize> = None;
                    for line in header_part.lines() {
                        if let Some((k, v)) = line.split_once(':') {
                            if k.trim().eq_ignore_ascii_case("content-length") {
                                cl = v.trim().parse().ok();
                                break;
                            }
                        }
                    }
                    if let Some(len) = cl {
                        expected_total = Some(pos + 4 + len);
                    }
                }
            }

            if let Some(tot) = expected_total {
                if resp_bytes.len() >= tot {
                    break;
                }
            }
        }
        Ok::<(), std::io::Error>(())
    })
    .await;

    if read_result.is_err() || resp_bytes.is_empty() {
        return (
            StatusCode::GATEWAY_TIMEOUT,
            Json(json!({"error": "Edge gateway proxy timed out"})),
        )
            .into_response();
    }

    state.fleet.record_audit(
        "proxy_request",
        "api",
        &device_id,
        json!({
            "method": method.to_string(),
            "path": target_path,
        }),
    );

    match parse_http_response_bytes(&resp_bytes) {
        Ok(r) => r,
        Err(e) => (StatusCode::BAD_GATEWAY, Json(json!({"error": e}))).into_response(),
    }
}

pub fn parse_http_response_bytes(bytes: &[u8]) -> Result<Response, String> {
    let header_end = bytes
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| "Missing header separator \r\n\r\n in response".to_string())?;

    let header_str = std::str::from_utf8(&bytes[..header_end])
        .map_err(|e| format!("Invalid UTF-8 in response headers: {}", e))?;
    let mut lines = header_str.lines();
    let status_line = lines.next().ok_or("Empty response status line")?;
    let mut parts = status_line.split_whitespace();
    let _proto = parts.next().unwrap_or("HTTP/1.1");
    let code_str = parts.next().unwrap_or("200");
    let status_code = code_str.parse::<u16>().unwrap_or(200);

    let status = StatusCode::from_u16(status_code).unwrap_or(StatusCode::OK);
    let mut builder = Response::builder().status(status);

    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            builder = builder.header(k.trim(), v.trim());
        }
    }

    let body_bytes = bytes[header_end + 4..].to_vec();
    builder
        .body(Body::from(body_bytes))
        .map_err(|e| e.to_string())
}

// ----------------------------------------------------------------------------
// Fleet Observability Endpoints (/metrics, /dlq, /audit)
// ----------------------------------------------------------------------------

pub async fn fleet_metrics_handler(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
) -> Response {
    let metrics = state.fleet.get_fleet_metrics();
    let accept = headers
        .get(axum::http::header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if accept.contains("text/plain") {
        let text = format!(
            "# HELP rekuiper_fleet_connected_gateways Total gateways connected to Control Plane\n\
             # TYPE rekuiper_fleet_connected_gateways gauge\n\
             rekuiper_fleet_connected_gateways {}\n\
             # HELP rekuiper_fleet_gateways_online Online fleet gateways\n\
             # TYPE rekuiper_fleet_gateways_online gauge\n\
             rekuiper_fleet_gateways_online {}\n\
             # HELP rekuiper_fleet_total_rules Total rules deployed across fleet\n\
             # TYPE rekuiper_fleet_total_rules gauge\n\
             rekuiper_fleet_total_rules {}\n\
             # HELP rekuiper_fleet_total_dlq_count Total DLQ dropped records across fleet\n\
             # TYPE rekuiper_fleet_total_dlq_count counter\n\
             rekuiper_fleet_total_dlq_count {}\n",
            metrics["total_devices"],
            metrics["online_devices"],
            metrics["total_rules"],
            metrics["total_dlq_errors"]
        );
        (
            StatusCode::OK,
            [(
                axum::http::header::CONTENT_TYPE,
                "text/plain; version=0.0.4",
            )],
            text,
        )
            .into_response()
    } else {
        Json(metrics).into_response()
    }
}

pub async fn fleet_dlq_handler(State(state): State<AppState>) -> impl IntoResponse {
    let dlq = state.fleet.get_fleet_dlq_alerts();
    Json(dlq)
}

pub async fn fleet_audit_handler(State(state): State<AppState>) -> impl IntoResponse {
    let log = state.fleet.audit_log.read().clone();
    Json(log)
}

// ----------------------------------------------------------------------------
// Fleet Router Mount
// ----------------------------------------------------------------------------

pub fn create_fleet_router(state: AppState) -> Router<AppState> {
    Router::new()
        .route("/fleet/ws", get(fleet_ws_acceptor))
        .route(
            "/api/v1/fleet/devices",
            get(list_devices).post(register_device),
        )
        .route(
            "/api/v1/fleet/devices/:id",
            get(get_device).delete(delete_device),
        )
        .route("/api/v1/fleet/groups", get(list_groups))
        .route(
            "/api/v1/fleet/snapshots",
            get(list_snapshots).post(create_snapshot_handler),
        )
        .route("/api/v1/fleet/snapshots/:id", get(get_snapshot))
        .route(
            "/api/v1/fleet/rollouts",
            get(list_rollouts).post(create_rollout_handler),
        )
        .route("/api/v1/fleet/rollouts/:id", get(get_rollout))
        .route(
            "/api/v1/fleet/rollouts/:id/promote",
            post(promote_rollout_handler),
        )
        .route(
            "/api/v1/fleet/rollouts/:id/rollback",
            post(rollback_rollout_handler),
        )
        .route("/api/v1/fleet/devices/:id/proxy", any(proxy_root_handler))
        .route("/api/v1/fleet/devices/:id/proxy/", any(proxy_root_handler))
        .route(
            "/api/v1/fleet/devices/:id/proxy/*path",
            any(proxy_path_handler),
        )
        .route("/api/v1/fleet/metrics", get(fleet_metrics_handler))
        .route("/api/v1/fleet/dlq", get(fleet_dlq_handler))
        .route("/api/v1/fleet/audit", get(fleet_audit_handler))
        .with_state(state)
}
