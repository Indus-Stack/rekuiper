# Declarative GitOps & Config-as-Code Mode

Platform engineers managing fleet deployments (Kubernetes, NixOS, Balena, Ansible) require infrastructure and stream processing pipelines to be version-controlled, reproducible, and verifiable in CI/CD before deployment.

rekuiper provides first-class support for **Declarative GitOps Mode**:
- Define all streams, tables, rules, connections, and schemas in version-controlled YAML, TOML, or JSON manifests.
- Validate pipelines 100% offline in CI using `rekuiper validate`.
- Run rekuiper daemon in locked-down, read-only declarative mode (`--declarative <PATH>`).
- Automatically hot-reload pipelines on file updates (`--watch`), SIGHUP, or `POST /declarative/reload` without disrupting healthy running rules.
- Maintain secrets security via environment variable expansion (`${VAR}` and `${VAR:-default}`).

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

To launch rekuiper in Declarative Mode, pass the `--declarative` flag or set the `REKUIPER_DECLARATIVE_PATH` environment variable.

### Command-Line Arguments

| Flag | Environment Variable | Default | Description |
| :--- | :--- | :--- | :--- |
| `--declarative <PATH>` | `REKUIPER_DECLARATIVE_PATH` | *None* | Path to a manifest file (`pipelines.yaml`) or directory (`pipelines.d/`). Enables declarative mode. |
| `--watch` | `REKUIPER_DECLARATIVE_WATCH` | `false` | Enable background filesystem polling (1s interval) for atomic hot-reload. |

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

## 4. Read-Only Mutation Guard (Control-Plane Immutability)

When running with `--declarative`, rekuiper enforces strict immutability across all control-plane APIs to prevent state drift between the Git repository and the running instance.

### Allowlist Enforcement

| Request | Behavior in Declarative Mode |
| :--- | :--- |
| `GET`, `HEAD`, `OPTIONS` | **Allowed** (read-only inspection of status, metrics, streams, rules). |
| `POST /declarative/reload` | **Allowed** (triggers explicit re-reading and reconciliation of manifests). |
| `POST /data/*`, `PUT /data/*` | **Allowed** (data ingestion plane for HTTP push sources). |
| `POST /ruletest*` | **Allowed** (ephemeral interactive rule testing). |
| All other `POST`, `PUT`, `PATCH`, `DELETE` | **Blocked with HTTP 403 Forbidden** (`{"error": "read_only_declarative_mode"}`). |

This guarantees that ad-hoc REST mutations cannot tamper with pipelines in production.

---

## 5. Non-Disruptive Hot Reload & Smart Diffing

Pipelines can be reloaded dynamically at runtime:
1. **Filesystem Watcher (`--watch`)**: Detects file updates, atomic editor saves (`rename` / inode swaps), and Kubernetes ConfigMap symlink updates (`..data`).
2. **SIGHUP Signal**: Send `kill -HUP <PID>` to trigger an immediate reload.
3. **HTTP Webhook**: Send `POST http://<host>:9081/declarative/reload`.

### Transitive Dependency Hashing

rekuiper calculates a composite content hash for each rule:

$$\text{RuleEffectiveHash} = \text{Hash}\big(\text{RuleDef} \parallel \text{ReferencedStreamDDLs} \parallel \text{ReferencedTableDDLs} \parallel \text{ReferencedConnections} \parallel \text{ReferencedSchemas}\big)$$

During reload:
- **Unchanged rules**: If a rule definition and all its upstream streams, tables, connections, and schemas are identical, **the running task actor is left untouched**. In-flight state and accumulators continue processing without disruption.
- **Modified rules**: Only rules whose effective hash changed are cleanly restarted.
- **Deleted rules**: Rules removed from the manifest are stopped and unpersisted.
- **New rules**: Added rules are compiled and started immediately.

### Atomic Failure Handling

If a modified manifest contains invalid SQL, broken YAML syntax, or referential integrity errors:
- The reload is **atomically aborted**.
- The existing running rules remain active and undisturbed.
- Prometheus counter `rekuiper_declarative_reload_failures_total` is incremented.
- `POST /declarative/reload` returns `HTTP 400 Bad Request` with detailed validation diagnostics.
