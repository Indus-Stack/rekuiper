# Declarative GitOps Mode

Declarative GitOps mode lets you manage stream processing pipelines with configuration files.

<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 860 195" width="100%" height="auto" role="img" aria-label="Declarative GitOps Workflow and Mutation Guard" font-family="-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif">
  <rect x="0" y="0" width="860" height="195" rx="10" fill="#f8fafc" stroke="#e2e8f0" stroke-width="1" />
  <text x="430" y="26" text-anchor="middle" font-size="14" font-weight="700" fill="#0f172a">Declarative GitOps Lifecycle &amp; Read-Only Mutation Guard</text>
  <rect x="25" y="48" width="230" height="125" rx="8" fill="#ffffff" stroke="#cbd5e1" stroke-width="1.5" />
  <text x="140" y="72" text-anchor="middle" font-size="12" font-weight="700" fill="#1e40af">1. Offline CI Validation</text>
  <text x="140" y="94" text-anchor="middle" font-size="11" fill="#334155">rekuiper validate pipelines.d/</text>
  <text x="140" y="114" text-anchor="middle" font-size="10" fill="#64748b">Checks SQL AST &amp; 221 Functions</text>
  <text x="140" y="132" text-anchor="middle" font-size="10" fill="#64748b">Verifies Stream/Table References</text>
  <text x="140" y="150" text-anchor="middle" font-size="10" fill="#2563eb">YAML / TOML / JSON Manifests</text>
  <path d="M 255 110 L 305 110" stroke="#64748b" stroke-width="2" />
  <polygon points="305,110 297,106 297,114" fill="#64748b" />
  <rect x="305" y="48" width="250" height="125" rx="8" fill="#ffffff" stroke="#ea580c" stroke-width="1.5" />
  <text x="430" y="72" text-anchor="middle" font-size="12" font-weight="700" fill="#c2410c">2. Differential Reconciliation</text>
  <text x="430" y="94" text-anchor="middle" font-size="11" fill="#334155">--declarative &amp; --watch / SIGHUP</text>
  <text x="430" y="114" text-anchor="middle" font-size="10" fill="#059669">Unchanged Rules Keep Running</text>
  <text x="430" y="132" text-anchor="middle" font-size="10" fill="#64748b">Restarts Modified Rules Only</text>
  <text x="430" y="150" text-anchor="middle" font-size="10" fill="#64748b">Atomic Rollback on Parse Error</text>
  <path d="M 555 110 L 605 110" stroke="#64748b" stroke-width="2" />
  <polygon points="605,110 597,106 597,114" fill="#64748b" />
  <rect x="605" y="48" width="230" height="125" rx="8" fill="#ffffff" stroke="#10b981" stroke-width="1.5" />
  <text x="720" y="72" text-anchor="middle" font-size="12" font-weight="700" fill="#047857">3. Read-Only Mutation Guard</text>
  <text x="720" y="94" text-anchor="middle" font-size="11" fill="#334155">Blocks Ad-Hoc REST Drift (403)</text>
  <text x="720" y="114" text-anchor="middle" font-size="10" fill="#059669">Permits GET, /v1/*, /ruletest</text>
  <text x="720" y="132" text-anchor="middle" font-size="10" fill="#059669">Permits POST /declarative/reload</text>
  <text x="720" y="150" text-anchor="middle" font-size="10" fill="#64748b">Zero Configuration Drift</text>
</svg>

Key capabilities:
- Define streams, tables, rules, connections, and schemas in YAML, TOML, or JSON manifest files.
- Validate pipelines offline before deployment with `rekuiper validate`.
- Run the rekuiper daemon in read-only declarative mode with `--declarative <PATH>`.
- Reload pipelines automatically on file changes with `--watch`, `SIGHUP`, or `POST /declarative/reload`.
- Use environment variables (`${VAR}` and `${VAR:-default}`) for configuration values.

---

## 1. Offline Validation in CI (`rekuiper validate`)

Before deploying manifests to production, lint them in CI pipelines. The validator checks manifest structure, SQL syntax, referential integrity (e.g. rules referencing undeclared streams), functions against the catalog, and sink action schemas:

```bash
# Validate a single manifest file (exit code 0 on success, 1 on errors)
rekuiper validate /path/to/pipelines.yaml

# Validate a directory of manifests (merged in lexicographical order)
rekuiper validate /etc/rekuiper/pipelines.d/

# Emit machine-readable JSON diagnostics for CI integration
rekuiper validate -o json /path/to/pipelines.yaml

# Allow unresolved environment variables during offline linting (e.g. CI without production secrets)
rekuiper validate --allow-unresolved-env /path/to/pipelines.yaml
```

When validation fails, `rekuiper validate` prints diagnostics with the specific entity, line, and descriptive error message:

```text
Validation failed for 'pipelines.yaml' with 1 error(s):
[Rule: high_temp_alert] Stream 'sensor_data' referenced in SQL 'SELECT * FROM sensor_data' is not declared in manifest or catalog
```

---

## 2. Daemon Configuration

To start rekuiper in declarative mode, pass the `--declarative` option or set `REKUIPER_DECLARATIVE_PATH`.

### Command-Line Arguments

| Option | Environment Variable | Default | Description |
| :--- | :--- | :--- | :--- |
| `--declarative <PATH>` | `REKUIPER_DECLARATIVE_PATH` | None | Path to a manifest file or directory. Activates declarative mode. |
| `--watch` | `REKUIPER_DECLARATIVE_WATCH` | `false` | Monitor files and reload configuration when files change. |

### Example Startup

```bash
# Launch with a single manifest file and file watching enabled
kuiperd --declarative /etc/rekuiper/pipelines.yaml --watch

# Or using environment variables
export REKUIPER_DECLARATIVE_PATH=/etc/rekuiper/pipelines.yaml
export REKUIPER_DECLARATIVE_WATCH=true
kuiperd
```

---

## 3. Manifest Specification

A manifest can be authored in YAML, TOML, or JSON. It represents a superset of the rekuiper ruleset format.

```yaml
version: "1.0"

connections:
  local_mqtt:
    type: "mqtt"
    servers: ["tcp://${MQTT_BROKER_HOST:-127.0.0.1}:1883"]
    username: "${MQTT_USERNAME}"
    password: "${MQTT_PASSWORD}"

schemas:
  sensor_proto:
    type: "protobuf"
    name: "sensor_proto"
    content: |
      syntax = "proto3";
      message Sensor {
        string device_id = 1;
        float temperature = 2;
        float humidity = 3;
      }

streams:
  sensor_stream: >
    CREATE STREAM sensor_stream ()
    WITH (DATASOURCE="devices/telemetry", FORMAT="protobuf", SCHEMAID="sensor_proto", CONNECTION="local_mqtt");

tables:
  device_metadata: >
    CREATE TABLE device_metadata (device_id string, location string, critical bool)
    WITH (DATASOURCE="metadata/devices", FORMAT="json", KEY="device_id");

rules:
  critical_temp_alert:
    sql: >
      SELECT s.device_id, s.temperature, m.location
      FROM sensor_stream AS s
      INNER JOIN device_metadata AS m ON s.device_id = m.device_id
      WHERE s.temperature > 85.0 AND m.critical = true
    actions:
      - mqtt:
          server: "tcp://${MQTT_BROKER_HOST:-127.0.0.1}:1883"
          topic: "alerts/high_temp"
      - log: {}
    options:
      sendError: true
      qos: 1
```

### Directory Loading (`pipelines.d/`)

When pointing `--declarative` to a directory:
1. All files ending in `.yaml`, `.yml`, `.json`, or `.toml` are read in lexicographical sort order (e.g. `01-streams.yaml`, `02-rules.yaml`).
2. Entities (streams, tables, rules, connections, schemas) are merged into a single cohesive pipeline state.
3. Duplicate IDs across files are caught and rejected with explicit error diagnostics.

---

## 4. Read-Only Mode (Mutation Guard)

When running with `--declarative`, rekuiper activates a read-only mode to prevent configuration drift.

### Permitted Requests

| Request | Behavior in Declarative Mode |
| :--- | :--- |
| `GET`, `HEAD`, `OPTIONS` | Permitted (read-only inspection of status, metrics, streams, and rules). |
| `POST /declarative/reload` | Permitted (re-reads and reloads manifest files). |
| `POST /v1/*`, `POST /data/*` | Permitted (telemetry and data ingestion). |
| `POST /ruletest*` | Permitted (temporary interactive rule testing). |
| Other `POST`, `PUT`, `PATCH`, `DELETE` | Blocked with HTTP 403 Forbidden (`{"error": "read_only_declarative_mode"}`). |

---

## 5. Reloading Pipelines

Reload pipelines at runtime:
1. **File Monitor (`--watch`)**: Detects file changes automatically.
2. **SIGHUP Signal**: Send `kill -HUP <PID>` to trigger a reload.
3. **HTTP Webhook**: Send a `POST` request to `http://<host>:9081/declarative/reload`.

### Rule Reconciliation

During a reload:
- **Unchanged rules**: Rules without changes continue running without interruption.
- **Modified rules**: Only rules with changed definitions are restarted.
- **Removed rules**: Rules removed from the manifest are stopped.
- **New rules**: Newly added rules are started.

### Error Handling

If a manifest contains syntax errors or invalid SQL:
- The reload operation stops immediately.
- Currently running rules continue to run.
- The engine increments the Prometheus counter `rekuiper_declarative_reload_failures_total`.
- The reload endpoint returns HTTP 400 Bad Request with error diagnostics.
