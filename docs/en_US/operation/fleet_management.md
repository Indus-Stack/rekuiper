# Fleet Management & Control Plane

The Rekuiper Control Plane provides centralized management, observability, and staged rollouts for distributed edge gateways.

Edge `kuiperd` gateways connect outbound to the Rekuiper Server Control Plane across WebSocket and Yamux multiplexing. The Control Plane manages device state, distributes declarative snapshots, executes staged Canary rollouts, and tunnels remote HTTP requests into edge routers.

---

## 1. Architecture Overview

Edge gateways initiate outbound WebSocket connections to `/fleet/ws` on the Control Plane. Once the connection establishes, Yamux multiplexes the single TCP/TLS channel into independent streams:

- **Stream 0 (Control & Telemetry):** Carries bidirectional JSON frames. The Control Plane dispatches commands (`ApplySnapshot`, `Rollback`, `GetStatus`) using a FIFO response queue so concurrent commands resolve deterministically. Each active session receives a monotonic connection identifier (`conn_id`) so delayed cleanup from a previous socket never disconnects a newer session. Edge agents send periodic heartbeats (CPU, memory, rule count, DLQ alerts, and active snapshot integrity hashes).
- **Streams 1..N (Remote Proxy Channels):** Tunnel HTTP requests on-demand from the Control Plane (`/api/v1/fleet/devices/:id/proxy/*` or `/fleet/devices/:id/proxy/*`) into the local Axum router of the target edge gateway.

![Rekuiper Fleet Control Plane Multiplexing Architecture](/diagrams/fleet_control_plane.svg)

---

## 2. Edge Gateway Configuration

Start the edge gateway daemon with the Control Plane URL and device identification.

### Command-Line Arguments

| Option | Environment Variable | Default | Description |
| :--- | :--- | :--- | :--- |
| `--control-plane-url <URL>` | `REKUIPER_CONTROL_PLANE_URL` | None | WebSocket address of the Control Plane (for example, `ws://10.0.0.1:9081/fleet/ws`). |
| `--device-id <ID>` | `REKUIPER_DEVICE_ID` | Hostname | Unique device identifier in the fleet catalog. |
| `--device-name <NAME>` | `REKUIPER_DEVICE_NAME` | Device ID | Human-readable name for the device. |
| `--plant <PLANT>` | `REKUIPER_DEVICE_PLANT` | `default-plant` | Plant group tag (for example, `detroit-automotive`). |
| `--region <REGION>` | `REKUIPER_DEVICE_REGION` | `default-region` | Regional group tag (for example, `us-midwest`). |
| `--device-type <TYPE>` | `REKUIPER_DEVICE_TYPE` | `edge-gateway` | Device category tag. |
| `--fleet-token <TOKEN>` | `REKUIPER_FLEET_TOKEN` | None | Shared authentication token verified by constant-time comparison (`Authorization: Bearer <token>` or `X-Fleet-Token`). |

### Startup Command

```bash
# Launch edge gateway with outbound Control Plane connection
kuiperd \
  --control-plane-url "ws://control-plane.internal:9081/fleet/ws" \
  --device-id "gw-detroit-press-01" \
  --device-name "Detroit Stamping Press 01" \
  --plant "detroit-automotive" \
  --region "us-midwest" \
  --device-type "industrial-pc"
```

### Automatic Reconnection

If the WebSocket connection disconnects:
1. The edge agent immediately logs the disconnect event.
2. The agent schedules reconnection with exponential backoff: 1 second, 2 seconds, 4 seconds, up to a maximum of 30 seconds.
3. Once reconnected, the agent registers its state and sends its telemetry heartbeat immediately.

---

## 3. Device Fleet Catalog

The Control Plane records all registered edge gateways in memory and persists all fleet state (`fleet_devices`, `fleet_snapshots`, `fleet_rollouts`, and `fleet_audit`) in the internal key-value store (`kv.rs`). All persisted fleet records are reloaded automatically on server startup.

### Endpoints

- `GET /api/v1/fleet/devices` &mdash; Returns the list of all registered gateways (also mounted at `/fleet/devices`).
- `GET /api/v1/fleet/devices/:id` &mdash; Returns detailed state and telemetry for one device.
- `DELETE /api/v1/fleet/devices/:id` &mdash; Deregisters an offline device.
- `GET /api/v1/fleet/groups` &mdash; Returns device counts grouped by plant, region, and device type.

### Example Device Record

```json
{
  "device_id": "gw-detroit-press-01",
  "name": "Detroit Stamping Press 01",
  "plant": "detroit-automotive",
  "region": "us-midwest",
  "device_type": "industrial-pc",
  "labels": {
    "line": "stamping-01"
  },
  "endpoint": "ws://connected",
  "status": "online",
  "version": "1.0.0-rc5",
  "last_heartbeat": 1740000000,
  "cpu_usage": 14.2,
  "memory_mb": 46.0,
  "rules_count": 8,
  "running_rules": 8,
  "dlq_errors": 0,
  "active_snapshot_id": "snap-v2-1",
  "active_snapshot_hash": 15892301948231,
  "previous_snapshot_id": "snap-v2-0",
  "ConnectedViaWs": true
}
```

---

## 4. Immutable Snapshots & Integrity Verification

Snapshots define declarative manifests containing streams, tables, connections, and rules. Each snapshot calculates a deterministic SHA-256 hash across its manifest bytes.

### Creating a Snapshot

Send a `POST` request to `/api/v1/fleet/snapshots`:

```bash
curl -X POST http://localhost:9081/api/v1/fleet/snapshots \
  -H "Content-Type: application/json" \
  -d '{
    "name": "snapshot-press-v2-4",
    "description": "Added motor vibration anomaly rule",
    "declarative": {
      "version": "1.0",
      "streams": {
        "motor_vibration": "CREATE STREAM motor_vibration () WITH (DATASOURCE=\"vibration/raw\", FORMAT=\"json\");"
      },
      "rules": {
        "rule_vib_high": {
          "sql": "SELECT * FROM motor_vibration WHERE val > 80",
          "actions": [
            {"mqtt": {"server": "tcp://broker:1883", "topic": "alerts/vibration"}}
          ]
        }
      }
    }
  }'
```

The response contains the computed `sha256` digest:

```json
{
  "id": "snap-1",
  "name": "snapshot-press-v2-4",
  "description": "Added motor vibration anomaly rule",
  "created_at": 1740001000,
  "sha256": "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
  "declarative": {
    "version": "1.0",
    "streams": {
      "motor_vibration": "CREATE STREAM motor_vibration () WITH (DATASOURCE=\"vibration/raw\", FORMAT=\"json\");"
    },
    "rules": {
      "rule_vib_high": {
        "sql": "SELECT * FROM motor_vibration WHERE val > 80",
        "actions": [
          {"mqtt": {"server": "tcp://broker:1883", "topic": "alerts/vibration"}}
        ]
      }
    }
  }
}
```

---

## 5. Staged Rollout Pipeline & Canary Gate

The Control Plane manages declarative rollout pipelines through four progressive stages:

```
[canary (10%)] ──> [batch_25 (25%)] ──> [batch_50 (50%)] ──> [full / completed (100%)]
```

### Health-Gated Canary Promotion

When you target a cohort (for example, `plant:detroit-automotive` or `all`):

1. **Deploy to Canary:** The rollout pushes the snapshot first to the Canary cohort over Yamux Stream 0.
2. **Watchdog Evaluation:** The Control Plane monitors telemetry heartbeats from canary devices.
3. **Automatic Halt on Anomaly:**
   - If an edge device triggers an automatic watchdog rollback (runtime exception, panic, or rule error),
   - Or if an edge device reports an active DLQ count spike,
   - The Control Plane automatically marks the rollout as `rolled_back`, halts further deployment, and writes a failure record to the audit trail.
4. **Progressive Promotion:** If Canary devices remain healthy without rollbacks or DLQ spikes, an administrator calls `/api/v1/fleet/rollouts/:id/promote` to advance the rollout through `batch_25`, `batch_50`, and `full` (`completed`).

### Launching a Rollout

```bash
curl -X POST http://localhost:9081/api/v1/fleet/rollouts \
  -H "Content-Type: application/json" \
  -d '{
    "snapshot_id": "snap-1",
    "target_group": "plant:detroit-automotive",
    "auto_promote": false
  }'
```

### Promoting a Healthy Rollout

```bash
curl -X POST http://localhost:9081/api/v1/fleet/rollouts/rollout-1/promote
```

### Halting a Faulty Rollout

```bash
curl -X POST http://localhost:9081/api/v1/fleet/rollouts/rollout-1/rollback
```

---

## 6. Remote HTTP Proxy Tunneling

The Control Plane exposes an HTTP proxy that forwards requests directly into the local Axum router of any connected edge gateway. The Control Plane opens a new multiplexed Yamux stream (1..N) to the target edge agent, serializes the incoming HTTP request, sends it over the stream, and returns the edge response:

```bash
# Query active rules from a remote edge gateway behind a firewall
curl -X GET http://localhost:9081/api/v1/fleet/devices/gw-detroit-press-01/proxy/rules

# Query Prometheus metrics directly from the edge process
curl -X GET http://localhost:9081/api/v1/fleet/devices/gw-detroit-press-01/proxy/metrics

# Query edge version
curl -X GET http://localhost:9081/api/v1/fleet/devices/gw-detroit-press-01/proxy/version
```

Because edge gateways initiate outbound connections to the Control Plane, remote inspection works without opening inbound firewall ports on the edge network.

---

## 7. Global Observability & Immutable Audit Trail

### Unified Fleet Metrics (`GET /api/v1/fleet/metrics`)

Aggregates Prometheus metrics across all connected gateways into a unified Prometheus text output:

- Total connected gateways (`rekuiper_fleet_connected_gateways`)
- Gateways by status (`rekuiper_fleet_gateways_by_status`)
- Total active fleet rules (`rekuiper_fleet_total_active_rules`)
- Total fleet DLQ dropped tuples (`rekuiper_fleet_total_dlq_count`)
- Active rollouts count (`rekuiper_fleet_active_rollouts`)

### Unified DLQ Alerts (`GET /api/v1/fleet/dlq`)

Lists all edge gateways with accumulated dead-letter queue records or recorded watchdog rollbacks.

### Immutable Audit Trail (`GET /api/v1/fleet/audit`)

All administrative actions (snapshot creation, rollout launch, Canary promotion, watchdog halt, and remote proxy tunneling) generate an immutable audit log stored in the key-value store (`kv.rs`).

```json
[
  {
    "timestamp": 1740001000,
    "action": "create_snapshot",
    "target": "snapshot-press-v2-4",
    "status": "success",
    "details": "Hash: 9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
  },
  {
    "timestamp": 1740001050,
    "action": "start_rollout",
    "target": "rollout-1",
    "status": "success",
    "details": "Stage: canary, Target: plant:detroit-automotive"
  }
]
```

---

## 8. Web Console Interface

The built-in web console (`/ui`) provides a graphical interface for Fleet Management:

- **Summary KPI Strip:** Displays total edge gateways, online count, active rollouts, and global DLQ status.
- **Staged Pipeline Progress Bar:** Visualizes stage movement (Canary 10% &rarr; Batch 25% &rarr; Batch 50% &rarr; Full 100%) with Canary evaluation status and action triggers.
- **Filterable Catalog:** Filter edge gateways by plant, region, device type, or health status.
- **Remote Inspect Viewer:** Opens a live modal to inspect rules, metrics, and logs through the Yamux proxy tunnel.
- **Audit & DLQ Tables:** Shows live alerts and security audit logs with timestamps and status indicators.

---

## 9. Model Context Protocol (MCP) Integration

AI agents and automated operations pipelines can manage the fleet through `rekuiper-mcp`:

### Fleet Control Tools

| Tool | Description |
| :--- | :--- |
| `list_fleet_devices` | Returns connected edge gateways, status flags, and health telemetry. |
| `get_fleet_rollouts` | Queries active and past canary rollouts. |
| `trigger_fleet_rollout` | Starts a progressive phased rollout across device groups. |
| `list_fleet_snapshots` | Queries backup configuration snapshots for rollbacks. |
| `proxy_fleet_device_api` | Sends REST queries to an edge gateway through the control plane tunnel. |

### Live Fleet Resources

- `rekuiper://fleet/devices` &mdash; Current device inventory and heartbeat telemetry.
- `rekuiper://fleet/rollouts` &mdash; Active staged rollout status and stage metrics.

### Orchestration Prompts

Use the `staged_fleet_rollout` prompt to guide multi-stage canary deployments with automated health checks and verification steps.
