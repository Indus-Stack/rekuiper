# Declarative GitOps Mode

Declarative GitOps mode lets you manage stream processing pipelines with configuration files.

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
