# Web Console User Journey and Operations Guide

This document describes the web console interface, security architecture, and operational journeys in rekuiper.

---

## 1. Security & Access Control Architecture

### Is an Exposed Web Console Safe?

In industrial edge, IIoT, and manufacturing environments, an unprotected web console is a critical security vulnerability. If exposed without restriction on `0.0.0.0` or public networks, unauthorized actors could alter streaming pipelines, tamper with PLC actuators, install malicious binaries, or exfiltrate sensor telemetry.

![rekuiper Web Console Dashboard](/screenshots/console_overview.png)

**rekuiper secures the web console through a three-layer defense:**
1. **Disabled by Default**: The web console (`/ui`) is disabled out-of-the-box. Operators must explicitly enable it with a command-line flag, environment variable, or configuration key.
2. **IP Address & CIDR Allowlist Filtering**: When enabled, operators can restrict console access exclusively to trusted workstations, engineering subnets, or VPN ranges (e.g. `127.0.0.1, 10.0.0.0/8`).
3. **Password, JWT, & Fleet Authentication**: Operators can require a dedicated console password, RS256 JWT token, or shared fleet token to unlock administration functions.

---

### Access Control Architecture Flow

The diagram below shows how connection requests are filtered before reaching the web console:

<svg viewBox="0 0 920 280" xmlns="http://www.w3.org/2000/svg" style="width: 100%; max-width: 920px; height: auto; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;">
  <defs>
    <linearGradient id="grad-client" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#f8fafc" />
      <stop offset="100%" stop-color="#f1f5f9" />
    </linearGradient>
    <linearGradient id="grad-firewall" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#fef2f2" />
      <stop offset="100%" stop-color="#fee2e2" />
    </linearGradient>
    <linearGradient id="grad-auth" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#fffbeb" />
      <stop offset="100%" stop-color="#fef3c7" />
    </linearGradient>
    <linearGradient id="grad-console" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#f0fdf4" />
      <stop offset="100%" stop-color="#dcfce7" />
    </linearGradient>
    <marker id="arrow" viewBox="0 0 10 10" refX="6" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
      <path d="M 0 1 L 10 5 L 0 9 z" fill="#64748b" />
    </marker>
  </defs>

  <!-- Step 1: Client Request -->
  <rect x="20" y="30" width="180" height="210" rx="8" fill="url(#grad-client)" stroke="#cbd5e1" stroke-width="1.5" />
  <text x="110" y="60" text-anchor="middle" font-size="13" font-weight="700" fill="#0f172a">Client Browser</text>
  <rect x="35" y="80" width="150" height="32" rx="4" fill="#ffffff" stroke="#e2e8f0" />
  <text x="110" y="101" text-anchor="middle" font-size="11" fill="#334155">GET /ui</text>
  <rect x="35" y="122" width="150" height="42" rx="4" fill="#ffffff" stroke="#e2e8f0" />
  <text x="110" y="141" text-anchor="middle" font-size="11" fill="#334155">Client IP Header</text>
  <text x="110" y="155" text-anchor="middle" font-size="10" fill="#64748b">(X-Forwarded-For)</text>
  <rect x="35" y="174" width="150" height="42" rx="4" fill="#ffffff" stroke="#e2e8f0" />
  <text x="110" y="193" text-anchor="middle" font-size="11" fill="#334155">Auth Token</text>
  <text x="110" y="207" text-anchor="middle" font-size="10" fill="#64748b">(Bearer / Password)</text>

  <!-- Flow 1 to 2 -->
  <line x1="200" y1="135" x2="238" y2="135" stroke="#64748b" stroke-width="1.5" marker-end="url(#arrow)" />

  <!-- Step 2: Enable & IP Allowlist Guard -->
  <rect x="240" y="20" width="210" height="230" rx="8" fill="url(#grad-firewall)" stroke="#fca5a5" stroke-width="1.5" />
  <text x="345" y="50" text-anchor="middle" font-size="13" font-weight="700" fill="#991b1b">Layer 1: Network Guard</text>
  <rect x="255" y="70" width="180" height="44" rx="5" fill="#ffffff" stroke="#fecaca" />
  <text x="265" y="88" font-size="11" font-weight="600" fill="#b91c1c">Enable Flag Check</text>
  <text x="265" y="104" font-size="10" fill="#dc2626">--enable-ui / enableUi: true</text>
  <rect x="255" y="124" width="180" height="54" rx="5" fill="#ffffff" stroke="#fecaca" />
  <text x="265" y="142" font-size="11" font-weight="600" fill="#b91c1c">IP Allowlist &amp; CIDR</text>
  <text x="265" y="158" font-size="10" fill="#dc2626">127.0.0.1, 10.0.0.0/8</text>
  <text x="265" y="170" font-size="9" fill="#ef4444">Blocks unauthorized IPs (403)</text>
  <rect x="255" y="188" width="180" height="44" rx="5" fill="#ffffff" stroke="#fecaca" />
  <text x="265" y="206" font-size="11" font-weight="600" fill="#b91c1c">Fail-Closed Boundary</text>
  <text x="265" y="222" font-size="10" fill="#dc2626">Protected edge default</text>

  <!-- Flow 2 to 3 -->
  <line x1="450" y1="135" x2="488" y2="135" stroke="#64748b" stroke-width="1.5" marker-end="url(#arrow)" />

  <!-- Step 3: Auth Guard -->
  <rect x="490" y="20" width="200" height="230" rx="8" fill="url(#grad-auth)" stroke="#fde047" stroke-width="1.5" />
  <text x="590" y="50" text-anchor="middle" font-size="13" font-weight="700" fill="#854d0e">Layer 2: Console Auth</text>
  <rect x="505" y="70" width="170" height="44" rx="5" fill="#ffffff" stroke="#fef08a" />
  <text x="515" y="88" font-size="11" font-weight="600" fill="#a16207">Console Password</text>
  <text x="515" y="104" font-size="10" fill="#ca8a04">--ui-password / env var</text>
  <rect x="505" y="124" width="170" height="44" rx="5" fill="#ffffff" stroke="#fef08a" />
  <text x="515" y="142" font-size="11" font-weight="600" fill="#a16207">RS256 JWT Token</text>
  <text x="515" y="158" font-size="10" fill="#ca8a04">Cryptographic signature</text>
  <rect x="505" y="178" width="170" height="54" rx="5" fill="#ffffff" stroke="#fef08a" />
  <text x="515" y="196" font-size="11" font-weight="600" fill="#a16207">Fleet Token</text>
  <text x="515" y="212" font-size="10" fill="#ca8a04">Provisioning bearer secret</text>
  <text x="515" y="224" font-size="9" fill="#eab308">Blocks unauthenticated (401)</text>

  <!-- Flow 3 to 4 -->
  <line x1="690" y1="135" x2="728" y2="135" stroke="#64748b" stroke-width="1.5" marker-end="url(#arrow)" />

  <!-- Step 4: Web Console Dashboard -->
  <rect x="730" y="30" width="170" height="210" rx="8" fill="url(#grad-console)" stroke="#86efac" stroke-width="1.5" />
  <text x="815" y="60" text-anchor="middle" font-size="13" font-weight="700" fill="#15803d">Unlocked Console</text>
  <rect x="745" y="80" width="140" height="32" rx="4" fill="#ffffff" stroke="#bbf7d0" />
  <text x="815" y="101" text-anchor="middle" font-size="11" fill="#16a34a">Visual Studio DAG</text>
  <rect x="745" y="122" width="140" height="32" rx="4" fill="#ffffff" stroke="#bbf7d0" />
  <text x="815" y="143" text-anchor="middle" font-size="11" fill="#16a34a">Rule Pipelines</text>
  <rect x="745" y="164" width="140" height="32" rx="4" fill="#ffffff" stroke="#bbf7d0" />
  <text x="815" y="185" text-anchor="middle" font-size="11" fill="#16a34a">Fleet &amp; Topologies</text>
</svg>

---

### Security Configuration Procedures

#### 1. Turn Console On or Off

By default, the web console is **off**. When disabled, visiting `http://<host>:9081/ui` returns `HTTP 403 Forbidden` with a security instruction message.

To enable the console:
- **CLI Flag**:
  ```bash
  kuiperd --enable-ui
  # or short alias:
  kuiperd --ui
  ```
- **Environment Variable**:
  ```bash
  export REKUIPER_ENABLE_UI=true
  kuiperd
  ```
- **Configuration File (`etc/kuiper.yaml`)**:
  ```yaml
  basic:
    enableUi: true
  ```

#### 2. Restrict Access via IP Allowlist & CIDR

To ensure only designated engineering subnets can connect to the web console:
- **CLI Flag**:
  ```bash
  kuiperd --enable-ui --ui-allow-ip "127.0.0.1, 10.0.0.0/8, 192.168.1.100"
  ```
- **Environment Variable**:
  ```bash
  export REKUIPER_UI_ALLOW_IP="127.0.0.1, 192.168.1.0/24"
  ```
- **Configuration File (`etc/kuiper.yaml`)**:
  ```yaml
  basic:
    enableUi: true
    uiIpAllowlist:
      - "127.0.0.1"
      - "10.0.0.0/8"
      - "192.168.1.0/24"
  ```
Any connection originating from an unlisted IP address (or when the client IP cannot be verified) is immediately rejected with `HTTP 403 Forbidden` (fail-closed). Forwarded IP headers (`X-Forwarded-For` and `X-Real-IP`) are trusted only when the direct TCP connection originates from a local reverse proxy (`127.0.0.1` or `::1`).

#### 3. Set a Console Password or JWT Token

To prevent unauthorized access across open physical plant terminals:
- **CLI Flag**:
  ```bash
  kuiperd --enable-ui --ui-password "FactoryPlantAdmin2026!"
  ```
- **Environment Variable**:
  ```bash
  export REKUIPER_UI_PASSWORD="FactoryPlantAdmin2026!"
  ```
- **Configuration File (`etc/kuiper.yaml`)**:
  ```yaml
  basic:
    enableUi: true
    uiPassword: "FactoryPlantAdmin2026!"
  ```
When set, the browser presents the **Console Authentication Dialog**. The operator enters the password or token once; the console securely caches the credential in `sessionStorage` and automatically signs all REST calls with `Authorization: Bearer <token>`.

![Console Security Guard Modal](/screenshots/console_security_modal.png)

---

## 2. Zero-Knowledge Guided Journeys

rekuiper is designed so that operators with **zero prior knowledge** of SQL syntax or stream processing internals can create, inspect, and deploy end-to-end industrial data pipelines.

---

### Journey 1: 1-Click Guided Quick-Start Pipeline

The console features a top onboarding banner with three guided choices:

1. **Step 1: Choose Source**:
   - `[Synthetic Sensor 🪄]`: Generates realistic temperature, vibration, and pressure telemetry inside the engine.
   - `[MQTT Telemetry]`: Connects to standard MQTT broker topic `factory/+/telemetry`.
   - `[Modbus PLC]`: Connects to industrial Modbus TCP registers.
   - `[OPC-UA Server]`: Ingests OPC-UA sensor node variables.
2. **Step 2: Choose Analytics Goal**:
   - `[Threshold Overheat Alert]`: Dispatches an alert when temperature exceeds 80°C.
   - `[10-Second Moving Average]`: Calculates rolling window aggregations.
   - `[Vibration Anomaly Spike]`: Identifies equipment mechanical anomalies.
3. **Step 3: Route Output**:
   - `[Edge Log]`: Prints formatted JSON to local system logs.
   - `[Cloud MQTT]`: Publishes alerts to `alerts/factory1`.
   - `[REST Webhook]`: Sends HTTP POST notifications to incident webhooks.
4. Click **🚀 Launch Pipeline Now**:
   - The engine automatically provisions the stream schema, constructs the SQL query, registers the sinks, and starts the rule in one transaction.

---

### Journey 2: Pipelines & Rules Studio

The **Pipelines & Rules** view (`#tab-btn-rules`) provides total control over continuous SQL streaming rules:

| Element / Button | Purpose |
| :--- | :--- |
| **+ New Rule Pipeline** | Opens the Visual Rule Designer modal (`#rule-designer-modal`). |
| **Rule Search Bar** | Real-time filter across rule IDs and statuses. |
| **Start (▶)** | Activates an idle rule in the streaming engine. |
| **Stop (⏹)** | Pauses stream consumption without deleting checkpoints. |
| **Restart (↻)** | Recycles operator state and restarts processing. |
| **Topology (📊)** | Visualizes the operator DAG with live record rates. |
| **DLQ Errors (⚠)** | Inspects poison-pill messages and dead-letter queues. |
| **Delete (🗑)** | Safely removes the rule pipeline after confirmation. |

#### Visual Rule Designer Workflow

![Guided Rule Designer Recipes](/screenshots/rule_designer_recipes.png)

1. **Rule ID**: Unique identifier for the pipeline (e.g. `rule_motor_overheat_alert`).
2. **Zero-Knowledge Rule Recipes**:
   - Click **🌡 Overheat Alert**: Pre-loads temperature threshold alert queries.
   - Click **📊 10s Average**: Pre-loads rolling 10-second tumbling window aggregations.
   - Click **📈 Anomaly Spike**: Pre-loads lag-based vibration difference calculations.
   - Click **🔄 Deduplicate**: Pre-loads `had_changed()` state-change filters.
3. **Interactive Clause Palette & Stream Field Pills**:
   - Click any **Stream Field Pill** (`[temperature]`, `[humidity]`, `[device_id]`) to append projections without typing field names.
   - Click **Window Selection** to choose Tumbling, Hopping, or Session windowing strategies with plain-language explanations.

![Window Explanations and SQL Formulation](/screenshots/rule_designer_window.png)

4. **Multi-Sink Outputs**:
   - Add multiple egress targets (MQTT, Webhook, File, Kafka) with custom payload templates.
5. **Runtime & Resilience**:
   - Configure QoS (0: At Most Once, 1: At Least Once, 2: Exactly Once), ring buffer capacities, and checkpoint intervals.
6. Click **Deploy Rule Pipeline 🚀** to execute preflight verification and activate the rule.

---

### Journey 3: Streams, Schemas & Connections

The **Streams, Schemas & Connections** catalog (`#tab-btn-catalog`) manages ingress sources and data contracts:

![IoT Stream Templates](/screenshots/stream_designer_templates.png)

| Feature / Button | Zero-Knowledge Capability |
| :--- | :--- |
| **+ New Stream / Table** | Opens the Visual Schema Builder (`#stream-designer-modal`). |
| **⚡ IoT Stream Templates** | Instantly configures *Temperature Sensor*, *Modbus PLC*, *OPC-UA Tags*, or *CANbus Vehicle*. |
| **✨ Infer from Sample JSON** | Paste any sample IoT JSON payload; the builder automatically detects fields and data types (`float`, `bigint`, `string`, `boolean`, `struct`). |
| **+ Add Field Row** | Adds structured columns with type dropdowns and nullable toggles. |
| **Autofill from Protobuf 🪄** | Reads registered `.proto` contracts and extracts fields into SQL types. |
| **ConfKey Manager** | Manages shared reusable connection credentials (MQTT brokers, Kafka clusters, Modbus PLCs). |

The diagram above illustrates the real-time inference workflow: pasting arbitrary JSON immediately maps payload keys into strongly typed schemas without SQL formulation.

---

### Journey 4: Extensions, JavaScript UDF Studio & Services

The **Extensions & Services** hub (`#tab-btn-plugins`) unlocks unlimited edge customization:

1. **Native & Wasm Plugins (`#ext-panel-plugins`)**:
   - Install custom sources, sinks, and Wasm bytecode modules.
   - Inspect loaded symbols, target architecture, and binary memory layouts.
2. **JavaScript UDF Studio (`#ext-panel-udfs`)**:
   - Author custom transformation functions in QuickJS without installing a compiler.
   - Select template presets (*Celsius to Fahrenheit*, *JSON Field Filter*, *Running Mean*).
   - **Interactive Preflight Sandbox**: Enter test inputs (`[25.0]`), click **⚡ Run Test in Sandbox**, and observe outputs and microsecond execution latency metrics before deploying.
   - **221+ SQL Function Palette**: Search and copy standard SQL functions across 14 categories.
3. **External Services & RPC Hub (`#ext-panel-services`)**:
   - Connect external microservices via gRPC or REST.
   - Call remote methods directly inside SQL queries (`SELECT ai_service.detect_defect(image) FROM camera_stream`).
4. **File Storage & Artifacts (`#ext-panel-uploads`)**:
   - Upload TLS certificates, Protobuf definitions, and ONNX models.
   - 1-click clipboard button copies the absolute edge path (`data/uploads/...`) for instant referencing.
5. **Data Backup & Migration (`#ext-panel-backup`)**:
   - 1-click JSON snapshot download (`/data/export`).
   - Declarative GitOps YAML bundle download (`/v2/data/export`).
   - Drag-and-drop system restore engine.

---

### Journey 5: Fleet Manager & Observability

The **Fleet Manager** view (`#tab-btn-fleet`) provides central command over distributed edge gateways:

- **Fleet Top**: Displays CPU usage, memory footprint, active rules, DLQ health badges, and network status for all connected nodes.
- **Rollout Engine**: Dispatches declarative pipeline snapshots across plant fleets.
- **Immutable Audit Trail**: Chronological event log of administrative mutations persisted in SQLite KV store.

---

### Journey 6: Ingestion & Telemetry Test Console

The **OTLP & Data Test Console** (`#tab-btn-ingest`) allows operators to validate pipelines with live payloads:

- Select pre-configured endpoints: `POST /v1/metrics`, `POST /v1/logs`, `POST /v1/traces`, or `POST /data/{stream}`.
- Click **Send Payload** to inject test telemetry and verify continuous SQL transformations in real time.

---

## 3. Contextual In-UI Documentation Links

To ensure operators never need to search externally, every interactive component in the web console provides direct contextual deep links to the documentation:

| UI Location | In-Console Trigger | Documentation Target | Purpose |
| :--- | :--- | :--- | :--- |
| **Topbar Navigation** | `📖 User Guide & Docs` | [Web Console User Guide](./web_console_user_guide.md) | Full user manual, security architecture, and operational journeys |
| **Topbar Navigation** | `🔒 Console Security Status` | [Security & Access Control Architecture](./web_console_user_guide.md#1-security--access-control-architecture) | Port exposure protection, IP allowlists, and credentials |
| **Rule Designer** | `📖 Rule Docs` | [Rule Overview](../rules/overview.md) | SQL syntax, temporal windows, and multi-sink actions |
| **Rule Designer** | `📖 Window Docs` | [Window Functions](../../sqls/windows.md) | Tumbling, Hopping, Session, and Counting window semantics |
| **Stream Designer** | `📖 Stream Docs` | [Stream Overview](../streams/overview.md) | Data sources, connectors, payload formats, and DDL syntax |
| **ConfKey Modal** | `📖 ConfKey Docs` | [Data Sources Configuration](../sources/overview.md) | Reusable connector profiles, TLS options, and credentials |
| **Schema Registry** | `📖 Schema Docs` | [Schema Registry Guide](../schemas/schema_registry.md) | Protobuf contracts, Custom formats, and AST validation |
| **Plugins & UDF Hub** | `📖 Plugin Docs` | [Plugins & Services Hub](../plugins/plugins_and_services_hub.md) | Wasm modules, JavaScript UDFs, and external RPC services |
| **Fleet Top Bar** | `📖 Fleet Docs` | [Fleet Deployment Guide](../../operation/fleet_management.md) | Health-gated rollouts, Canary verification, and device monitoring |
