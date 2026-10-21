# Plugins, Extensible Functions, and Services Center

This document describes the Extensions, Plugins, JavaScript UDFs, External Services, File Storage, and Backup Migration Center in rekuiper.

---

## Overview

The Extensions and Services Center provides a unified administration console for edge extensibility. It allows operators to install native and WebAssembly (Wasm) plugins, write and test JavaScript user-defined functions (UDFs/UDAFs), discover RPC methods on external microservices, upload configuration files, and export or import complete system backups.

The system requires no external dependencies. The entire management hub is built into the single binary edge distribution of rekuiper.

---

## Architecture Flow

The diagram below shows the interaction between the five extensibility sub-modules, the edge SQL engine, and external services:

<svg viewBox="0 0 920 300" xmlns="http://www.w3.org/2000/svg" style="width: 100%; max-width: 920px; height: auto; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;">
  <defs>
    <linearGradient id="grad-plugins" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#eff6ff" />
      <stop offset="100%" stop-color="#dbeafe" />
    </linearGradient>
    <linearGradient id="grad-udf" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#faf5ff" />
      <stop offset="100%" stop-color="#f3e8ff" />
    </linearGradient>
    <linearGradient id="grad-core" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#f0fdf4" />
      <stop offset="100%" stop-color="#dcfce7" />
    </linearGradient>
    <linearGradient id="grad-services" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#fff7ed" />
      <stop offset="100%" stop-color="#ffedd5" />
    </linearGradient>
    <linearGradient id="grad-storage" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#f8fafc" />
      <stop offset="100%" stop-color="#e2e8f0" />
    </linearGradient>
    <marker id="arrow" viewBox="0 0 10 10" refX="6" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
      <path d="M 0 1 L 10 5 L 0 9 z" fill="#64748b" />
    </marker>
  </defs>

  <!-- Box 1: Native & Wasm Plugins -->
  <rect x="20" y="25" width="220" height="115" rx="8" fill="url(#grad-plugins)" stroke="#bfdbfe" stroke-width="1.5" />
  <text x="130" y="50" text-anchor="middle" font-size="13" font-weight="700" fill="#1e40af">1. Native &amp; Wasm Plugins</text>
  <text x="35" y="74" font-size="11" fill="#1e3a8a">• Source, Sink &amp; Function plugins</text>
  <text x="35" y="94" font-size="11" fill="#1e3a8a">• Wasm sandboxed execution</text>
  <text x="35" y="114" font-size="11" fill="#1e3a8a">• Prebuilt &amp; dynamic so/dylib</text>

  <!-- Box 2: JavaScript UDF Studio -->
  <rect x="20" y="160" width="220" height="115" rx="8" fill="url(#grad-udf)" stroke="#e9d5ff" stroke-width="1.5" />
  <text x="130" y="185" text-anchor="middle" font-size="13" font-weight="700" fill="#6b21a8">2. JavaScript UDF Studio</text>
  <text x="35" y="209" font-size="11" fill="#581c87">• QuickJS edge script runtime</text>
  <text x="35" y="229" font-size="11" fill="#581c87">• In-browser test sandbox</text>
  <text x="35" y="249" font-size="11" fill="#581c87">• 221+ SQL function palette</text>

  <!-- Flow left to center -->
  <line x1="240" y1="82" x2="318" y2="120" stroke="#64748b" stroke-width="1.5" marker-end="url(#arrow)" />
  <line x1="240" y1="217" x2="318" y2="180" stroke="#64748b" stroke-width="1.5" marker-end="url(#arrow)" />

  <!-- Center Box: Stream Engine Core -->
  <rect x="320" y="45" width="280" height="210" rx="8" fill="url(#grad-core)" stroke="#bbf7d0" stroke-width="1.5" />
  <text x="460" y="75" text-anchor="middle" font-size="14" font-weight="700" fill="#166534">rekuiper Stream Engine</text>
  <rect x="335" y="95" width="250" height="40" rx="4" fill="#ffffff" stroke="#86efac" />
  <text x="460" y="120" text-anchor="middle" font-size="12" font-weight="600" fill="#15803d">Continuous SQL Processor</text>
  <rect x="335" y="145" width="250" height="40" rx="4" fill="#ffffff" stroke="#86efac" />
  <text x="460" y="170" text-anchor="middle" font-size="12" font-weight="600" fill="#15803d">Zero-Copy Memory Bus</text>
  <rect x="335" y="195" width="250" height="40" rx="4" fill="#ffffff" stroke="#86efac" />
  <text x="460" y="220" text-anchor="middle" font-size="12" font-weight="600" fill="#15803d">Security Sandbox Isolation</text>

  <!-- Flow center to right -->
  <line x1="600" y1="120" x2="678" y2="82" stroke="#64748b" stroke-width="1.5" marker-end="url(#arrow)" />
  <line x1="600" y1="180" x2="678" y2="217" stroke="#64748b" stroke-width="1.5" marker-end="url(#arrow)" />

  <!-- Box 3: External Services & RPC -->
  <rect x="680" y="25" width="220" height="115" rx="8" fill="url(#grad-services)" stroke="#fed7aa" stroke-width="1.5" />
  <text x="790" y="50" text-anchor="middle" font-size="13" font-weight="700" fill="#9a3412">3. External Services &amp; RPC</text>
  <text x="695" y="74" font-size="11" fill="#7c2d12">• gRPC / REST service proxy</text>
  <text x="695" y="94" font-size="11" fill="#7c2d12">• Automated function discovery</text>
  <text x="695" y="114" font-size="11" fill="#7c2d12">• Dynamic SQL calling</text>

  <!-- Box 4 & 5: Storage & Backup -->
  <rect x="680" y="160" width="220" height="115" rx="8" fill="url(#grad-storage)" stroke="#cbd5e1" stroke-width="1.5" />
  <text x="790" y="185" text-anchor="middle" font-size="13" font-weight="700" fill="#334155">4 &amp; 5. Storage &amp; Backup</text>
  <text x="695" y="209" font-size="11" fill="#1e293b">• File storage (/config/uploads)</text>
  <text x="695" y="229" font-size="11" fill="#1e293b">• 1-click JSON snapshot export</text>
  <text x="695" y="249" font-size="11" fill="#1e293b">• Drag-and-drop system restore</text>
</svg>

---

## 1. Native and WebAssembly (Wasm) Plugins

The Plugins panel (`#ext-panel-plugins`) manages executable binaries and modules loaded by the engine.

### Plugin Types

The system classifies plugins into five categories:
1. **Source Plugins (`source`)**: Custom protocol adapters that ingest streaming data into the engine.
2. **Sink Plugins (`sink`)**: Custom publishers that deliver processed messages to proprietary backends.
3. **Function Plugins (`function`)**: Native compiled algorithms callable directly inside SQL statements.
4. **Wasm Plugins (`wasm`)**: WebAssembly bytecode modules running in a secure, memory-isolated runtime.
5. **Portable Plugins (`portable`)**: Independent system processes communicating over local IPC channels.

### Management Procedures

1. **View Installed Plugins**:
   - Open the web console at `http://<host>:9081/ui`.
   - Select the **Extensions & Services** tab.
   - Choose the **🔌 Native & Wasm Plugins** sub-tab.
   - Filter plugins by category: **All Plugins**, **Sources**, **Sinks**, **Functions**, or **Wasm**.

2. **Register a Plugin**:
   - Click the **+ Install Plugin** button.
   - Select the plugin type.
   - Enter the unique plugin name.
   - Provide the file URI or artifact location.
   - Click **Install Plugin** to initiate verification and loading.

3. **Inspect Plugin Metadata**:
   - Click the **Inspect** button on any installed plugin card.
   - Review file size, target architecture, export symbols, and memory status.

---

## 2. JavaScript UDFs and Extensible Functions Studio

The JavaScript Studio (`#ext-panel-udfs`) enables inline script creation and immediate verification.

### Capabilities

- **Zero-Compile Authoring**: Write custom transformation and aggregation logic without setting up a Rust or C build chain.
- **Embedded Engine**: Scripts run inside the embedded QuickJS engine for sub-millisecond execution latency.
- **Client-Side Test Sandbox**: Evaluate scripts with sample JSON parameters in the browser prior to saving to the edge server.
- **221+ Built-in Function Palette**: Search and copy standard SQL functions across 14 categories (Math, String, Aggregate, Conversion, Time, JSON, Array, Bitwise, Hashing, System, IIoT, EdgeX, Dynamic, State).

### Authoring and Testing Workflow

1. Click **+ New JS Function**.
2. Select an authoring template:
   - **Celsius to Fahrenheit**: Linear scalar transformation.
   - **JSON Field Filter**: Dynamic object filtering.
   - **Running Mean**: Stateful rolling accumulator.
3. Specify the Function Name (e.g. `celsius_to_fahrenheit`).
4. Write or customize the JavaScript function in the code editor:
   ```javascript
   function celsius_to_fahrenheit(c) {
     if (c === null || c === undefined) return null;
     return (c * 9.0 / 5.0) + 32.0;
   }
   ```
5. Enter test inputs into the **Test Input Arguments** box:
   ```json
   [25.0]
   ```
6. Click **⚡ Run Test in Sandbox**.
7. Confirm that the status badge indicates `✓ Sandbox Passed` with output `77` and verified execution latency (e.g. `12 µs`).
8. Click **Save JS Function** to deploy to the edge engine.

### Using in SQL Rules

Call the function directly inside streaming SQL queries:

```sql
SELECT
  device_id,
  temperature AS celsius,
  celsius_to_fahrenheit(temperature) AS fahrenheit
FROM demo_stream
WHERE celsius_to_fahrenheit(temperature) > 100.0;
```

---

## 3. External Services and RPC Discovery Hub

The External Services panel (`#ext-panel-services`) connects rekuiper to external microservices over gRPC or REST.

### Key Features

- **Service Registration**: Connect external containers, cloud endpoints, or on-premise RPC servers.
- **Automated Method Discovery**: Query service reflection to enumerate callable RPC methods.
- **Dynamic SQL Integration**: Call remote methods directly within SQL projections using the `service_name.method_name(args...)` syntax.

### Registration Procedure

1. Click **+ Register Service**.
2. Fill in the connection properties:
   - **Service Name**: Unique identifier (e.g. `defect_detector_rpc`).
   - **Protocol**: `gRPC` or `REST`.
   - **Endpoint URL**: Network address (e.g. `http://127.0.0.1:50051`).
   - **Timeout (ms)**: RPC call deadline (default: `5000`).
3. Click **Register Service**.
4. The system validates connectivity and lists all exported functions in the **Service Functions** grid.

---

## 4. File Storage and Artifacts Hub

The File Storage panel (`#ext-panel-uploads`) manages auxiliary edge files without external shared storage.

### Supported File Types

- **TLS / SSL Certificates**: Client certificates (`.pem`, `.crt`, `.key`) for mutual TLS.
- **Protobuf Specifications**: `.proto` definitions for binary message decoding.
- **Machine Learning Models**: `.onnx` and `.tflite` model files for inference rules.
- **Binary Codecs**: Compiled dynamic libraries and custom register lookup tables.

### Upload and Usage

1. Click **Upload File**.
2. Select a local file or drop it into the designated drop zone.
3. Click **Upload File**.
4. Use the **Copy Path** button to copy the absolute edge filesystem path (e.g. `data/uploads/sensor_contract.proto`) for use in stream configurations and sink rules.

---

## 5. Data Backup, Snapshot Export, and Migration Hub

The Backup and Migration panel (`#ext-panel-backup`) protects edge configurations against hardware failure and simplifies fleet provisioning.

### Backup Options

- **JSON Full Snapshot (`/data/export`)**: Produces a single JSON document containing all streams, tables, rules, plugins, JavaScript UDFs, schemas, and configurations.
- **GitOps YAML Bundle (`/v2/data/export`)**: Produces human-readable declarative YAML manifests for version-controlled GitOps workflows.

### Restore and Migration Procedure

1. Navigate to the **💾 Data Backup & Migration** sub-tab.
2. Under **Restore & Migrate Configuration**, select a backup file (`.json` or `.yaml`).
3. Select the merge strategy:
   - **Safe Merge (Default)**: Adds new definitions and updates modified entities without deleting unreferenced streams.
   - **Full Overwrite**: Replaces existing configurations with the contents of the backup snapshot.
4. Click **Apply Restore Snapshot**.
5. The console shows a real-time summary of restored entities:
   - Streams restored: `N`
   - Rules restored: `N`
   - UDFs restored: `N`
   - Services registered: `N`

---

## REST API Reference

The Extensions and Services Center is fully accessible through standard REST endpoints:

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/plugins/sources` | List installed native source plugins |
| `GET` | `/plugins/sinks` | List installed native sink plugins |
| `GET` | `/plugins/functions` | List installed native function plugins |
| `GET` | `/plugins/wasm` | List registered WebAssembly modules |
| `POST` | `/plugins/<type>` | Register a new plugin of specified type |
| `DELETE` | `/plugins/<type>/<name>` | Remove an installed plugin |
| `GET` | `/udf/javascript` | List all saved JavaScript UDFs and UDAFs |
| `POST` | `/udf/javascript` | Create or update a JavaScript function |
| `DELETE` | `/udf/javascript/<id>` | Delete a JavaScript function |
| `GET` | `/metadata/functions` | Retrieve the complete palette of 221+ built-in functions |
| `GET` | `/services` | List registered external microservices |
| `POST` | `/services` | Register a new external microservice |
| `GET` | `/services/functions` | Discover exported RPC functions |
| `GET` | `/config/uploads` | List uploaded files and certificates |
| `POST` | `/config/uploads` | Upload a new file artifact |
| `GET` | `/data/export` | Download full system JSON snapshot |
| `POST` | `/data/import` | Upload and apply system configuration snapshot |
