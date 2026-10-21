# Metadata Property Forms and Shared Connection Pool Hub

This document describes the Shared Connection Pool Hub, Configuration Keys (ConfKeys) Manager, and dynamic PropertyForm architecture in rekuiper.

---

## 1. Overview

The web console provides a centralized connection management architecture. This architecture lets operators manage connector credentials, test preflight connectivity, and inspect pipeline dependencies before making changes.

```
       +-------------------------------------------------------+
       |             Shared Connection Pool Hub                |
       |  - Pooled socket endpoints (MQTT, Kafka, HTTP, Modbus)|
       |  - Real-time reference count safety checks            |
       +-------------------------------------------------------+
                                   |
         +-------------------------+-------------------------+
         |                                                   |
         v                                                   v
+------------------+                               +------------------+
| Dynamic Property |                               | ConfKeys Manager |
| Form Component   |                               | Hub              |
| - Strongly typed |                               | - Reusable key   |
|   controls       |                               |   profiles       |
| - Eye password   |                               | - Master-detail  |
|   reveal         |                               |   browser        |
| - Dual mode JSON |                               | - Preflight test |
|   sync           |                               +------------------+
+------------------+                                         |
         |                                                   |
         +-------------------------+-------------------------+
                                   |
                                   v
             +-------------------------------------------+
             |           Egress Sinks & Streams          |
             |  - Ingestion Streams & Lookup Tables      |
             |  - Heterogeneous Rule Sinks (MQTT, REST)  |
             +-------------------------------------------+
```

---

## 2. Shared Connection Pool Architecture

The following vector graphic displays the relationship between connection pools, configuration keys, pipelines, and backend storage:

<svg viewBox="0 0 820 380" xmlns="http://www.w3.org/2000/svg" style="background:#090d16; border-radius:8px; border:1px solid #1e293b; width:100%; height:auto; margin: 16px 0;">
  <defs>
    <linearGradient id="gradHub" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#0284c7" />
      <stop offset="100%" stop-color="#0369a1" />
    </linearGradient>
    <linearGradient id="gradForm" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#10b981" />
      <stop offset="100%" stop-color="#047857" />
    </linearGradient>
    <linearGradient id="gradKeys" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#8b5cf6" />
      <stop offset="100%" stop-color="#6d28d9" />
    </linearGradient>
  </defs>

  <!-- Title -->
  <text x="30" y="38" fill="#f8fafc" font-size="16" font-weight="700" font-family="system-ui, sans-serif">Rekuiper Connection Pool &amp; Configuration Hub Topology</text>
  <text x="30" y="58" fill="#94a3b8" font-size="12" font-family="system-ui, sans-serif">Zero-copy socket multiplexing and configuration safety checks</text>

  <!-- Top Hub Card -->
  <rect x="30" y="80" width="760" height="85" rx="6" fill="url(#gradHub)" opacity="0.9" />
  <text x="50" y="112" fill="#ffffff" font-size="15" font-weight="700" font-family="system-ui, sans-serif">🔌 Shared Connection Pool Hub (/connections)</text>
  <text x="50" y="134" fill="#e0f2fe" font-size="12" font-family="system-ui, sans-serif">Multiplexes client sessions across MQTT, HTTP, Kafka, OPC-UA, and Modbus sockets.</text>
  <text x="50" y="152" fill="#bae6fd" font-size="11" font-family="monospace">Safety Guard: Computes Active Reference Count before deletion (Blocks accidental pipeline failure)</text>

  <!-- Flow Arrows -->
  <path d="M 230 165 L 230 205" stroke="#38bdf8" stroke-width="2" stroke-dasharray="4,4" />
  <path d="M 590 165 L 590 205" stroke="#a78bfa" stroke-width="2" stroke-dasharray="4,4" />

  <!-- Left Box: Dynamic Property Form -->
  <rect x="30" y="210" width="370" height="145" rx="6" fill="#1e293b" stroke="#334155" stroke-width="1.5" />
  <rect x="30" y="210" width="370" height="34" rx="6" fill="url(#gradForm)" opacity="0.8" />
  <text x="45" y="232" fill="#ffffff" font-size="13" font-weight="700" font-family="system-ui, sans-serif">📝 Dynamic PropertyForm Component</text>
  <text x="45" y="266" fill="#cbd5e1" font-size="12" font-family="system-ui, sans-serif">• Discovers schema from /metadata/{type}</text>
  <text x="45" y="288" fill="#cbd5e1" font-size="12" font-family="system-ui, sans-serif">• Strong input typing (number, select, switch, textarea)</text>
  <text x="45" y="310" fill="#cbd5e1" font-size="12" font-family="system-ui, sans-serif">• Secret toggle (👁 / 🙈) with automatic masking</text>
  <text x="45" y="332" fill="#cbd5e1" font-size="12" font-family="system-ui, sans-serif">• Bidirectional live sync with Raw JSON editor</text>

  <!-- Right Box: ConfKeys Manager -->
  <rect x="420" y="210" width="370" height="145" rx="6" fill="#1e293b" stroke="#334155" stroke-width="1.5" />
  <rect x="420" y="210" width="370" height="34" rx="6" fill="url(#gradKeys)" opacity="0.8" />
  <text x="435" y="232" fill="#ffffff" font-size="13" font-weight="700" font-family="system-ui, sans-serif">🔑 Connector ConfKeys Hub</text>
  <text x="435" y="266" fill="#cbd5e1" font-size="12" font-family="system-ui, sans-serif">• Stores reusable profiles in /configs/sources/{type}</text>
  <text x="435" y="288" fill="#cbd5e1" font-size="12" font-family="system-ui, sans-serif">• Master-detail search and key selector</text>
  <text x="435" y="310" fill="#cbd5e1" font-size="12" font-family="system-ui, sans-serif">• Inline preflight connection validator (⚡)</text>
  <text x="435" y="332" fill="#cbd5e1" font-size="12" font-family="system-ui, sans-serif">• Integrated into Stream Designer and Rule Sinks</text>
</svg>

---

## 3. Dynamic PropertyForm Component

The `renderPropertyForm` function creates user interface forms dynamically from connector metadata schemas.

### 3.1 Metadata Resolution
The component resolves properties through the following procedure:
1. Requests `/metadata/sources/{type}`, `/metadata/sinks/{type}`, or `/metadata/connections/{type}`.
2. If the API returns property schemas, the component uses those schemas.
3. If the API returns empty definitions or 404, the component uses the built-in fallback schema catalog.

### 3.2 Supported Form Controls

| Control Type | Trigger Condition | Rendered Element | Behavior |
| :--- | :--- | :--- | :--- |
| **Password** | `control === 'password'` or name matches `secret`, `password`, `token` | `<input type="password">` with eye button | Click `👁` to show plain text. Click `🙈` to hide plain text. |
| **Select** | `values` array exists or `control === 'select'` | `<select>` dropdown | Presents allowed values from schema. |
| **Switch** | `type === 'bool'` or `control === 'switch'` | Toggle slider | Toggles `true` or `false`. |
| **Number** | `type === 'int'` or `type === 'float'` | `<input type="number">` | Converts input string to numeric value. |
| **Textarea** | `control === 'textarea'` or `type === 'json'` | `<textarea>` | Supports multi-line input and formatted JSON strings. |
| **Text** | Default string scalar | `<input type="text">` | Captures standard string configuration. |

### 3.3 Dual-Mode Raw JSON Synchronization
Operators can switch between **Visual Form** and **Raw JSON** at any time:
- Edits in the visual form immediately update the raw JSON buffer.
- Edits in the raw JSON editor validate on input. Valid JSON objects immediately update the form fields.
- Syntax errors show an alert box and prevent invalid state submission.

---

## 4. Shared Connection Pool Hub

The Connection Pool Hub manages reusable network connections.

### 4.1 Connection Table Fields
- **Connection ID**: Unique identifier for the shared connection pool.
- **Type & Target**: Connector protocol (`MQTT`, `HTTP`, `Kafka`, etc.) and the destination host or URL.
- **Active Pipeline References**: Live count of streams, tables, and rules that use this connection ID.
- **Configuration Preview**: Clean summary string of non-secret configuration parameters.
- **Actions**:
  - `Test ⚡`: Executes a preflight network connectivity check.
  - `Edit ✏`: Opens the connection modal with current values.
  - `Delete 🗑`: Performs reference-count safety validation before deletion.

### 4.2 Safe Deletion Procedure
Before deleting any shared connection, the system verifies pipeline dependencies:
1. Scans all active streams for matching `CONF_KEY` or `connectionSelector`.
2. Scans all active tables for matching connection options.
3. Scans all rule sink definitions for matching connector targets.
4. If references exist, the console displays a warning dialog that lists every dependent rule and stream. The user must confirm before the `DELETE /connections/{id}` request is sent.

---

## 5. Connector Configuration Keys (ConfKeys) Manager

Configuration keys (`confKeys`) let operators define named profiles for connectors without hardcoding credentials in every pipeline.

### 5.1 Endpoints Used
- List keys: `GET /metadata/sources/{type}/confKeys`
- Read key configuration: `GET /metadata/sources/{type}/confKeys/{keyName}`
- Save key configuration: `PUT /metadata/sources/{type}/confKeys/{keyName}`
- Delete key: `DELETE /metadata/sources/{type}/confKeys/{keyName}`

### 5.2 Preflight Validation
Click `⚡ Test Preflight` to validate credentials and server reachability before saving. The preflight check confirms that:
- Required host, URL, and port parameters are present.
- URL formats match protocol specifications.
- Credentials and TLS certificates match file paths on disk.

---

## 6. Designer Integration

The PropertyForm component is integrated directly into the visual designers:
- **Rule Designer (Step 2 - Sinks)**: Click `⚙ Advanced` on any sink card to configure protocol parameters, TLS certificates, and MQTT QoS.
- **Stream & Table Designer**: Click `⚙ Advanced Properties` next to the `ConfKey` selector to configure connector-specific polling intervals, headers, and socket settings.
