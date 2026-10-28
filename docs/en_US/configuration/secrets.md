# Dynamic Secrets Management

This document describes dynamic secret resolution in `rekuiper`.

Do not store passwords, keys, or tokens in plain text in configuration files or stream definitions. Use dynamic secrets instead.

<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 840 185" width="100%" height="auto" role="img" aria-label="Dynamic Secret Resolution and Redaction Flow" font-family="-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif">
  <rect x="0" y="0" width="840" height="185" rx="10" fill="#f8fafc" stroke="#e2e8f0" stroke-width="1" />
  <text x="420" y="26" text-anchor="middle" font-size="14" font-weight="700" fill="#0f172a">Dynamic Secret Resolution, 300s Caching &amp; API Redaction</text>
  <rect x="25" y="48" width="235" height="115" rx="8" fill="#ffffff" stroke="#cbd5e1" stroke-width="1.5" />
  <text x="142" y="72" text-anchor="middle" font-size="12" font-weight="700" fill="#1e40af">1. Secret URI Reference</text>
  <text x="142" y="94" text-anchor="middle" font-size="10" fill="#334155">vault://mount/path#field</text>
  <text x="142" y="112" text-anchor="middle" font-size="10" fill="#334155">env://VARIABLE_NAME</text>
  <text x="142" y="132" text-anchor="middle" font-size="10" fill="#64748b">Used in YAML Configs &amp; SQL DDL</text>
  <path d="M 260 105 L 305 105" stroke="#64748b" stroke-width="2" />
  <polygon points="305,105 297,101 297,109" fill="#64748b" />
  <rect x="305" y="48" width="235" height="115" rx="8" fill="#ffffff" stroke="#ea580c" stroke-width="1.5" />
  <text x="422" y="72" text-anchor="middle" font-size="12" font-weight="700" fill="#c2410c">2. Resolver &amp; TTL Cache</text>
  <text x="422" y="94" text-anchor="middle" font-size="10" fill="#334155">Vault KV v1/v2 &amp; Process Env</text>
  <text x="422" y="112" text-anchor="middle" font-size="10" fill="#059669">300s (5-Minute) In-Memory Cache</text>
  <text x="422" y="132" text-anchor="middle" font-size="10" fill="#64748b">Stale-While-Unreachable Failover</text>
  <path d="M 540 105 L 585 105" stroke="#64748b" stroke-width="2" />
  <polygon points="585,105 577,101 577,109" fill="#64748b" />
  <rect x="585" y="48" width="230" height="115" rx="8" fill="#ffffff" stroke="#10b981" stroke-width="1.5" />
  <text x="700" y="72" text-anchor="middle" font-size="12" font-weight="700" fill="#047857">3. Redaction Boundary</text>
  <text x="700" y="94" text-anchor="middle" font-size="10" fill="#334155">Connectors Receive Plaintext</text>
  <text x="700" y="112" text-anchor="middle" font-size="10" fill="#dc2626">Logs Replace Values with [REDACTED]</text>
  <text x="700" y="132" text-anchor="middle" font-size="10" fill="#059669">REST APIs Preserve URI Only</text>
</svg>

`rekuiper` supports two secret resolution backends:
1. **HashiCorp Vault** (`vault://`): Retrieves secrets from an external Vault service.
2. **Environment Variables** (`env://`): Retrieves secrets from system or container environment variables.

## Secret URI Syntax

You can use secret URIs in connection strings, passwords, and custom headers.

### HashiCorp Vault Format

```text
vault://<mount>/<path>#<field>
```

- `<mount>`: The secret engine mount path (for example, `secret` or `kv`).
- `<path>`: The secret path in Vault (for example, `mqtt/broker` or `devices/factory1`).
- `<field>`: The field name to read from the secret data (for example, `password` or `token`).

### Environment Variable Format

```text
env://<VARIABLE_NAME>
```

- `<VARIABLE_NAME>`: The name of the environment variable (for example, `MQTT_PASSWORD`).

---

## Vault Configuration

`rekuiper` connects to HashiCorp Vault using HTTP requests. Configure connection parameters using these environment variables:

| Variable | Default | Description |
| :--- | :--- | :--- |
| `VAULT_ADDR` | `"http://127.0.0.1:8200"` | The base URL of the Vault server. |
| `VAULT_TOKEN` | (None) | The authentication token sent in the `X-Vault-Token` header. |

`rekuiper` supports both Vault KV Version 1 and KV Version 2 backends automatically.

---

## Secret Caching

To reduce network traffic to the Vault server, `rekuiper` caches resolved secrets in memory.

- **Cache Duration**: Secrets remain in memory for **300 seconds (5 minutes)**.
- **Cache Refresh**: The engine refreshes secrets automatically after the cache expires.
- **Failover**: If the secret backend is unreachable during a refresh, `rekuiper` continues to use the previous cached value.

---

## Usage Examples

### MQTT Connector Example

You can set secrets in configuration files:

```yaml
# etc/mqtt_source.yaml
default:
  server: "tcp://broker.internal:1883"
  username: "edge_collector"
  password: "vault://secret/mqtt#password"
```

Or you can use template interpolation in SQL:

```sql
CREATE STREAM factory_mqtt () WITH (
    TYPE = "mqtt",
    SERVER = "tcp://broker.internal:1883",
    USERNAME = "edge_collector",
    PASSWORD = "{{vault://secret/production/mqtt#token}}",
    DATASOURCE = "telemetry/#",
    FORMAT = "JSON"
);
```

### RabbitMQ Connector Example

```sql
CREATE STREAM amqp_feed () WITH (
    TYPE = "rabbitmq",
    SERVER = "amqp://guest:{{env://RABBIT_PASSWORD}}@10.0.0.15:5672/%2f",
    QUEUE = "events",
    FORMAT = "JSON"
);
```

---

## Secret Redaction

`rekuiper` protects your sensitive information:
- Logs do not print raw secret values. The system replaces secrets with `[REDACTED]`.
- API endpoints (`GET /streams` and `GET /rules`) display the original `vault://` or `env://` URI and do not expose the resolved secret.
