# Fleet Management & Control Plane

The Rekuiper Control Plane provides centralized management, observability, and staged rollouts for distributed edge gateways.

Edge `kuiperd` gateways connect outbound to the Rekuiper Server Control Plane across WebSocket and Yamux multiplexing. The Control Plane manages device state, distributes declarative snapshots, executes staged Canary rollouts, and tunnels remote HTTP requests into edge routers.

---

## 1. Architecture Overview

Edge gateways initiate outbound WebSocket connections to `/fleet/ws` on the Control Plane. Once the connection establishes, Yamux multiplexes the single TCP/TLS channel into independent streams:

- **Stream 0 (Control & Telemetry):** Carries bidirectional JSON-RPC frames. The Control Plane dispatches commands (`ApplySnapshot`, `Rollback`, `GetStatus`). Edge agents send periodic heartbeats (CPU, memory, rule count, DLQ alerts, and active snapshot integrity hashes).
- **Streams 1..N (Remote Proxy Channels):** Tunnel HTTP requests on-demand from the Control Plane (`/api/v1/fleet/devices/:id/proxy/*`) into the local Axum router of the target edge gateway.

<div style="margin: 24px 0; text-align: center;">
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 880 440" width="100%" height="auto" style="max-width: 880px; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 12px; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
  <defs>
    <linearGradient id="cpGrad" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#2563eb" />
      <stop offset="100%" stop-color="#1d4ed8" />
    </linearGradient>
    <linearGradient id="edgeGrad" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#0f172a" />
      <stop offset="100%" stop-color="#1e293b" />
    </linearGradient>
    <filter id="shadow" x="-5%" y="-5%" width="110%" height="110%">
      <feDropShadow dx="0" dy="4" stdDeviation="4" flood-color="#0f172a" flood-opacity="0.08" />
    </filter>
  </defs>

  <!-- Title & Background -->
  <rect x="0" y="0" width="880" height="440" fill="#f8fafc" rx="12" />
  <text x="440" y="36" text-anchor="middle" font-size="16" font-weight="700" fill="#0f172a" letter-spacing="-0.01em">Rekuiper Fleet Control Plane Multiplexing Architecture</text>
  <text x="440" y="56" text-anchor="middle" font-size="12" fill="#64748b">Outbound Edge WebSocket &amp; Yamux Multiplexed Streams</text>

  <!-- Control Plane Box -->
  <g filter="url(#shadow)">
    <rect x="40" y="90" width="280" height="310" rx="10" fill="#ffffff" stroke="#2563eb" stroke-width="2" />
    <rect x="40" y="90" width="280" height="42" rx="10" fill="url(#cpGrad)" />
    <text x="180" y="117" text-anchor="middle" font-size="14" font-weight="700" fill="#ffffff">Rekuiper Server (Control Plane)</text>
    
    <!-- Internal components -->
    <rect x="60" y="148" width="240" height="44" rx="6" fill="#eff6ff" stroke="#bfdbfe" stroke-width="1" />
    <text x="180" y="174" text-anchor="middle" font-size="12" font-weight="600" fill="#1e40af">Fleet Manager (AppState / kv.rs)</text>
    
    <rect x="60" y="202" width="240" height="44" rx="6" fill="#eff6ff" stroke="#bfdbfe" stroke-width="1" />
    <text x="180" y="228" text-anchor="middle" font-size="12" font-weight="600" fill="#1e40af">Staged Rollout &amp; Canary Engine</text>

    <rect x="60" y="256" width="240" height="44" rx="6" fill="#eff6ff" stroke="#bfdbfe" stroke-width="1" />
    <text x="180" y="282" text-anchor="middle" font-size="12" font-weight="600" fill="#1e40af">Remote HTTP Proxy Router</text>

    <rect x="60" y="310" width="240" height="70" rx="6" fill="#f8fafc" stroke="#cbd5e1" stroke-width="1" />
    <text x="180" y="332" text-anchor="middle" font-size="11" font-weight="600" fill="#334155">Server Yamux Acceptor</text>
    <text x="180" y="352" text-anchor="middle" font-size="11" fill="#64748b">GET /fleet/ws (WebSocket)</text>
    <text x="180" y="368" text-anchor="middle" font-size="10" fill="#059669">Session Manager &amp; Driver</text>
  </g>

  <!-- Network Channel (Yamux Multiplexing) -->
  <g>
    <!-- Multiplexed pipe -->
    <rect x="340" y="195" width="200" height="100" rx="8" fill="#f1f5f9" stroke="#cbd5e1" stroke-dasharray="4,4" />
    <text x="440" y="218" text-anchor="middle" font-size="12" font-weight="700" fill="#0f172a">TLS WebSocket Channel</text>
    <text x="440" y="234" text-anchor="middle" font-size="10" fill="#64748b">Yamux Multiplexing</text>

    <!-- Stream 0 Line -->
    <path d="M 320 252 L 560 252" stroke="#2563eb" stroke-width="2.5" />
    <polygon points="560,252 552,248 552,256" fill="#2563eb" />
    <polygon points="320,252 328,248 328,256" fill="#2563eb" />
    <text x="440" y="248" text-anchor="middle" font-size="10" font-weight="600" fill="#2563eb">Stream 0: Heartbeats &amp; Snapshot Commands</text>

    <!-- Stream 1..N Line -->
    <path d="M 320 276 L 560 276" stroke="#059669" stroke-width="2" stroke-dasharray="3,3" />
    <polygon points="560,276 552,272 552,280" fill="#059669" />
    <polygon points="320,276 328,272 328,280" fill="#059669" />
    <text x="440" y="290" text-anchor="middle" font-size="10" font-weight="600" fill="#059669">Streams 1..N: On-demand HTTP Proxy Tunnel</text>
  </g>

  <!-- Edge Gateways Box -->
  <g filter="url(#shadow)">
    <rect x="560" y="90" width="280" height="310" rx="10" fill="#ffffff" stroke="#0f172a" stroke-width="2" />
    <rect x="560" y="90" width="280" height="42" rx="10" fill="url(#edgeGrad)" />
    <text x="700" y="117" text-anchor="middle" font-size="14" font-weight="700" fill="#ffffff">Edge Kuiperd Agent</text>

    <!-- Edge Internal Components -->
    <rect x="580" y="148" width="240" height="44" rx="6" fill="#f8fafc" stroke="#cbd5e1" stroke-width="1" />
    <text x="700" y="174" text-anchor="middle" font-size="12" font-weight="600" fill="#0f172a">Outbound Agent Reconnect Loop</text>

    <rect x="580" y="202" width="240" height="44" rx="6" fill="#ecfdf5" stroke="#a7f3d0" stroke-width="1" />
    <text x="700" y="222" text-anchor="middle" font-size="11" font-weight="600" fill="#065f46">Declarative Snapshot Engine</text>
    <text x="700" y="238" text-anchor="middle" font-size="10" fill="#047857">Watchdog &amp; Auto-Rollback</text>

    <rect x="580" y="256" width="240" height="44" rx="6" fill="#f8fafc" stroke="#cbd5e1" stroke-width="1" />
    <text x="700" y="282" text-anchor="middle" font-size="12" font-weight="600" fill="#0f172a">Local Axum HTTP Router</text>

    <rect x="580" y="310" width="240" height="70" rx="6" fill="#eff6ff" stroke="#bfdbfe" stroke-width="1" />
    <text x="700" y="332" text-anchor="middle" font-size="11" font-weight="600" fill="#1e40af">Telemetry Reporter</text>
    <text x="700" y="352" text-anchor="middle" font-size="10" fill="#64748b">10s Heartbeats: CPU / Mem / Rules / DLQ</text>
    <text x="700" y="368" text-anchor="middle" font-size="10" fill="#2563eb">Active Snapshot Hash Verification</text>
  </g>
</svg>
</div>

---

## 2. Edge Gateway Configuration

Start the edge gateway daemon with the Control Plane URL and device identification.

### Command-Line Arguments

| Option | Environment Variable | Default | Description |
| :--- | :--- | :--- | :--- |
| `--control-plane-url <URL>` | `REKUIPER_CONTROL_PLANE_URL` | None | WebSocket address of the Control Plane (for example, `ws://10.0.0.1:9081/fleet/ws`). |
| `--device-id <ID>` | `REKUIPER_DEVICE_ID` | Hostname | Unique device identifier in the fleet catalog. |
| `--device-name <NAME>` | `REKUIPER_DEVICE_NAME` | Device ID | Human-readable name for the device. |
| `--plant <PLANT>` | `REKUIPER_DEVICE_PLANT` | `default-plant` | Plant group tag (for example, `plant-detroit`). |
| `--region <REGION>` | `REKUIPER_DEVICE_REGION` | `default-region` | Regional group tag (for example, `us-east`). |
| `--device-type <TYPE>` | `REKUIPER_DEVICE_TYPE` | `edge-gateway` | Device category tag. |

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

The Control Plane records all registered edge gateways in memory and persists them in the internal key-value store (`kv.rs`).

### Endpoints

- `GET /api/v1/fleet/devices` &mdash; Returns the list of all registered gateways.
- `GET /api/v1/fleet/devices/:id` &mdash; Returns detailed state and telemetry for one device.
- `DELETE /api/v1/fleet/devices/:id` &mdash; Deregisters an offline device.
- `GET /api/v1/fleet/groups` &mdash; Returns device counts grouped by plant, region, and device type.

### Example Device Record

```json
{
  "device_id": "gw-detroit-press-01",
  "name": "Detroit Stamping Press 01",
  "group": {
    "plant": "detroit-automotive",
    "region": "us-midwest",
    "device_type": "industrial-pc"
  },
  "status": "online",
  "version": "1.0.0-rc4",
  "last_heartbeat": 1740000000,
  "cpu_percent": 14.2,
  "memory_bytes": 48234496,
  "active_snapshot_id": "snap-v2-1",
  "active_snapshot_hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  "rule_count": 8,
  "dlq_count": 0,
  "last_rollback_reason": null
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
    "snapshot_id": "snapshot-press-v2-4",
    "description": "Added motor vibration anomaly rule",
    "manifest_yaml": "streams:\n  - sql: CREATE STREAM motor_vibration () WITH (DATASOURCE=\"vibration/raw\", FORMAT=\"json\");\nrules:\n  - id: rule_vib_high\n    sql: SELECT * FROM motor_vibration WHERE val > 80\n    actions:\n      - mqtt: { server: \"tcp://broker:1883\", topic: \"alerts/vibration\" }"
  }'
```

The response contains the computed `sha256_hash`:

```json
{
  "snapshot_id": "snapshot-press-v2-4",
  "sha256_hash": "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
  "description": "Added motor vibration anomaly rule",
  "created_at": 1740001000,
  "stream_count": 1,
  "rule_count": 1
}
```

---

## 5. Staged Rollout Pipeline & Canary Gate

The Control Plane manages declarative rollout pipelines through four progressive stages:

```
[Development] ──> [Staging] ──> [Canary (5%)] ──> [Production (100%)]
```

### Health-Gated Canary Promotion

When you target a cohort (for example, `plant-detroit` or `all`):

1. **Deploy to Canary:** The rollout pushes the snapshot first to the Canary cohort over Yamux Stream 0.
2. **Watchdog Evaluation:** The Control Plane enters `canary_evaluating` state and monitors telemetry heartbeats.
3. **Automatic Halt on Anomaly:**
   - If an edge device triggers an automatic watchdog rollback (runtime exception, panic, or rule error),
   - Or if an edge device reports an active DLQ count spike,
   - The Control Plane automatically marks the rollout as `rolled_back`, halts further deployment, and writes a failure record to the audit trail.
4. **Production Promotion:** If Canary devices remain healthy without rollbacks or DLQ spikes, an administrator calls `/api/v1/fleet/rollouts/:id/promote` to promote the snapshot to 100% of the fleet.

### Launching a Rollout

```bash
curl -X POST http://localhost:9081/api/v1/fleet/rollouts \
  -H "Content-Type: application/json" \
  -d '{
    "rollout_id": "rollout-press-v2-4",
    "snapshot_id": "snapshot-press-v2-4",
    "target_group": "detroit-automotive",
    "current_stage": "canary",
    "canary_percentage": 5
  }'
```

### Promoting a Healthy Rollout

```bash
curl -X POST http://localhost:9081/api/v1/fleet/rollouts/rollout-press-v2-4/promote
```

### Halting a Faulty Rollout

```bash
curl -X POST http://localhost:9081/api/v1/fleet/rollouts/rollout-press-v2-4/rollback
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
    "target": "rollout-press-v2-4",
    "status": "success",
    "details": "Stage: canary, Target: detroit-automotive"
  }
]
```

---

## 8. Web Console Interface

The built-in web console (`/ui`) provides a graphical interface for Fleet Management:

- **Summary KPI Strip:** Displays total edge gateways, online count, active rollouts, and global DLQ status.
- **Staged Pipeline Progress Bar:** Visualizes stage movement (Dev &rarr; Staging &rarr; Canary 5% &rarr; Prod 100%) with Canary evaluation status and action triggers.
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
