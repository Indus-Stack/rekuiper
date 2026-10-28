# Global Configurations

The global configuration file for rekuiper is `$kuiper/etc/kuiper.yaml`. The file uses YAML syntax.

You can override YAML configurations using environment variables. Environment variables take precedence over file settings. Format variable names using the `KUIPER__` prefix followed by YAML path keys separated by double underscores (`__`):

```bash
KUIPER__BASIC__DEBUG=true
```

## Basic Configuration Options

The `basic` section configures runtime logging, network binding, and security policies:

```yaml
basic:
  # debug | info | warn | error | fatal | panic
  loglevel: info 
  # true | false: enables verbose debug logging
  debug: false
  # true | false: outputs logs to stdout console
  consoleLog: false
  # true | false: outputs logs to the log file
  fileLog: true
  # syslog settings
  syslog:
    enable: false
    network: udp
    address: localhost:514
    level: info
    tag: kuiper
  # Maximum file size in bytes before rotation
  rotateSize: 10485760 # 10 MB
  # Maximum number of rotated log files to retain
  rotateCount: 3
  # Rotation interval in hours
  rotateTime: 24
  # Maximum file retention duration in hours
  maxAge: 72
  # Case sensitivity for SQL processing
  ignoreCase: false
  sql:
    # Max connections per database instance group; 0 = unlimited
    maxConnections: 0
  # Interval for rule schedule reconciliation
  rulePatrolInterval: 10s
  # Storage backend for configurations: file or kv
  cfgStorageType: file
  # Log each REST API invocation
  enableRestAuditLog: false
  # Allow rules to connect to private network addresses
  enablePrivateNet: false
  # Allow APIs to access files outside data/uploads
  allowExternalFileAccess: false
```

### Security Options

#### enablePrivateNet

The `enablePrivateNet` option determines whether rules and sinks can connect to private networks (such as `localhost` or `127.0.0.1`). Default is `false` for security.

> [!WARNING]
> Since version 2.4.0, `enablePrivateNet` defaults to `false`. The engine blocks connections to private network addresses by default. If your rules require local network access (such as local REST services or edge brokers), you must set this value to `true`.

#### allowExternalFileAccess

The `allowExternalFileAccess` option specifies whether file access APIs (such as `file://` URIs) can read files outside the `data/uploads` directory. Default is `false` to prevent path traversal vulnerabilities.

> [!WARNING]
> When `allowExternalFileAccess` is `false`, file access is strictly restricted to `data/uploads`. Set to `true` only if you require access to other host filesystem paths.

#### ignoreCase

The `ignoreCase` option controls case sensitivity in SQL processing. When `false`, the engine enforces case matching for column names to optimize parsing performance. Default is `false`.

## Logging Configuration

```yaml
basic:
  loglevel: info 
  debug: false
  consoleLog: false
  fileLog: true
  logDisableTimestamp: false
  rotateTime: 24
  maxAge: 72
```

When `debug` is `true`, the engine forces log output to debug level regardless of `loglevel`. Set `logDisableTimestamp` to `true` when forwarding logs to external aggregators that inject timestamps.

### System Log (syslog)

Enable syslog forwarding by setting `basic.syslog.enable: true` or by setting the environment variable `KuiperSyslogKey=true`.

```yaml
syslog:
  enable: false
  network: udp
  address: localhost:514
  level: info
  tag: kuiper
```

If you leave `network` and `address` empty, rekuiper connects to the local system syslog daemon.

### Log File Rotation

When `fileLog` is enabled, the engine rotates logs by size or elapsed time.

#### Rotate by Size

```yaml
rotateSize: 10485760 # 10 MB
rotateCount: 3
```

When the file size exceeds `rotateSize`, the engine rotates the file. The engine retains up to `rotateCount` archive files.

#### Rotate by Time

```yaml
rotateTime: 24
maxAge: 72
```

The engine splits log files every `rotateTime` hours and deletes archives older than `maxAge` hours.

## Global Timezone

```yaml
timezone: UTC
```

Specify a timezone name from the [IANA Time Zone Database](https://www.iana.org/time-zones). If empty, the engine uses `UTC`. Set to `Local` to adopt the host system timezone.

> [!NOTE]
> In Alpine Linux containers, you must install `tzdata` (`apk add tzdata`) to provide timezone definitions.

## CLI Network Binding

```yaml
basic:
  ip: 127.0.0.1
  port: 20498
```

Configures the listening address and TCP port for the rekuiper CLI server daemon. Defaults to `127.0.0.1` for local-only execution security.

## REST Service Configuration

```yaml
basic:
  restIp: 127.0.0.1
  restPort: 9081
  httpServerIp: 127.0.0.1
  httpServerPort: 10081
  restTls:
    certfile: /var/https-server.crt
    keyfile: /var/https-server.key
```

- `restIp`: The network IP address bound by the REST API server (`9081`), the secondary EdgeX listener (`59720`), and the Prometheus metrics server (`20499`). Defaults to `127.0.0.1` (PR #4089 security hardening).
- `restPort`: The HTTP port for the REST API server (default `9081`).
- `httpServerIp`: The network IP address bound by the ruletest SSE streaming server. Defaults to `127.0.0.1`.
- `httpServerPort`: The port for the ruletest SSE streaming server (default `10081`).
- `restTls`: Paths to TLS certificates and private keys. When configured, the REST API listens on HTTPS.
- `enableUi`: Enables the embedded Web Console at `GET /ui` (default `false`).
- `uiPassword`: Optional password required to unlock the Web Console (`POST /ui/auth`) and authenticate console API calls (`Authorization: Bearer <password>`). Verified using constant-time comparison.
- `uiIpAllowlist`: List of IP addresses or CIDR ranges permitted to access `/ui` and `/ui/auth`. Fails closed (`HTTP 403 Forbidden`) when a client IP does not match or cannot be verified; `X-Forwarded-For` and `X-Real-IP` headers are trusted only when the direct TCP peer is loopback (`127.0.0.1` or `::1`).
- `fleetToken`: Shared secret token for Fleet Control Plane WebSocket agents (`/fleet/ws`) and Fleet REST API calls (`Authorization: Bearer <token>` or `X-Fleet-Token`), verified in constant time.

<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 860 210" width="100%" height="auto" role="img" aria-label="rekuiper Network Listeners and Security Binding Architecture" font-family="-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif">
  <rect x="0" y="0" width="860" height="210" rx="10" fill="#f8fafc" stroke="#e2e8f0" stroke-width="1" />
  <text x="430" y="28" text-anchor="middle" font-size="14" font-weight="700" fill="#0f172a">Default Loopback Binding &amp; Listener Architecture (basic.restIp = 127.0.0.1)</text>
  <rect x="30" y="50" width="240" height="135" rx="8" fill="#ffffff" stroke="#cbd5e1" stroke-width="1.5" />
  <text x="150" y="75" text-anchor="middle" font-size="12" font-weight="700" fill="#1e40af">Primary REST &amp; Web UI</text>
  <text x="150" y="96" text-anchor="middle" font-size="11" fill="#334155">Bind: restIp:restPort (127.0.0.1:9081)</text>
  <text x="150" y="116" text-anchor="middle" font-size="10" fill="#64748b">Guards: RS256 JWT / uiPassword / fleetToken</text>
  <text x="150" y="134" text-anchor="middle" font-size="10" fill="#059669">Fail-Closed CIDR uiIpAllowlist</text>
  <text x="150" y="152" text-anchor="middle" font-size="10" fill="#64748b">Loopback-Trusted X-Forwarded-For</text>
  <rect x="310" y="50" width="240" height="135" rx="8" fill="#ffffff" stroke="#cbd5e1" stroke-width="1.5" />
  <text x="430" y="75" text-anchor="middle" font-size="12" font-weight="700" fill="#c2410c">Prometheus Metrics Server</text>
  <text x="430" y="96" text-anchor="middle" font-size="11" fill="#334155">Bind: restIp:prometheusPort (127.0.0.1:20499)</text>
  <text x="430" y="116" text-anchor="middle" font-size="10" fill="#64748b">Endpoint: GET /metrics</text>
  <text x="430" y="134" text-anchor="middle" font-size="10" fill="#059669">500ms TTL Cached Host Telemetry</text>
  <text x="430" y="152" text-anchor="middle" font-size="10" fill="#64748b">32 Multidimensional Metric Families</text>
  <rect x="590" y="50" width="240" height="135" rx="8" fill="#ffffff" stroke="#cbd5e1" stroke-width="1.5" />
  <text x="710" y="75" text-anchor="middle" font-size="12" font-weight="700" fill="#047857">EdgeX &amp; Ruletest Listeners</text>
  <text x="710" y="96" text-anchor="middle" font-size="11" fill="#334155">EdgeX: restIp:59720 (127.0.0.1:59720)</text>
  <text x="710" y="116" text-anchor="middle" font-size="11" fill="#334155">Ruletest SSE: httpServerIp:10081</text>
  <text x="710" y="134" text-anchor="middle" font-size="10" fill="#059669">Zero Unintended 0.0.0.0 Exposure</text>
  <text x="710" y="152" text-anchor="middle" font-size="10" fill="#64748b">Explicit Override via kuiper.yaml / Env</text>
</svg>

> [!NOTE]
> For security hardening, rekuiper defaults all listeners (`ip`, `restIp`, `httpServerIp`) to loopback (`127.0.0.1`), preventing unintentional exposure over external networks when authentication is disabled. When deploying inside Docker containers or edge gateways requiring external access, explicitly configure `restIp: 0.0.0.0` and `httpServerIp: 0.0.0.0` in `kuiper.yaml`, or set environment variables `KUIPER__BASIC__RESTIP=0.0.0.0` and `KUIPER__BASIC__HTTPSERVERIP=0.0.0.0`.

### REST Authentication

```yaml
basic:
  authentication: false
```

When `true`, rekuiper requires JWT RSA256 tokens for REST API calls. Refer to [REST Authentication](../api/restapi/authentication.md). Production deployments exposed on `0.0.0.0` should always enable authentication.

## Rule Patrol Interval

```yaml
basic:
  rulePatrolInterval: "10s"
```

Specifies the reconciliation interval used by the internal scheduler to inspect and trigger periodic rules.

## Prometheus Metrics Export

```yaml
basic:
  prometheus: true
  prometheusPort: 20499
```

When enabled, rekuiper exports Prometheus metrics on `http://<restIp>:20499/metrics` (default `http://127.0.0.1:20499/metrics`). You can set `prometheusPort` to match `restPort` to serve metrics through the REST API port.

## Plugin Hosts (Legacy Compatibility)

> [!NOTE]
> Go C-shared native dynamic plugins (`.so`) are not supported in rekuiper. Built-in connectors (including Kafka, SQL, Redis, and WebSocket) are compiled into the core engine. Custom functions and extensions run via WebAssembly (Wasm) or external service microservices.

In legacy installations, `pluginHosts` specifies the repository URL hosting prebuilt native plugins:

| Plugin Type | Legacy Plugins |
| :--- | :--- |
| `source` | `random`, `zmq` |
| `sink` | `file`, `image`, `influx`, `redis`, `tdengine`, `zmq` |
| `function` | `accumulateWordCount`, `countPlusOne`, `echo`, `geohash`, `image`, `labelImage` |

## Sink Cache Configuration

Configure default caching behavior for output sinks. You can override these settings at the rule level. Refer to [Sink Caching](../guide/sinks/overview.md#caching).

```yaml
sink:
  enableCache: false
  memoryCacheThreshold: 1024
  maxDiskCache: 1024000
  bufferPageSize: 256
  resendInterval: 0
  cleanCacheAtStop: false
```

- `enableCache`: Enables or disables disk/memory sink caching.
- `memoryCacheThreshold`: Maximum message count retained in memory before writing to disk.
- `maxDiskCache`: Maximum message count retained in disk storage.
- `bufferPageSize`: Message batch size per disk read/write operation.
- `resendInterval`: Retransmission interval in milliseconds.
- `cleanCacheAtStop`: Clears cached data when the rule stops.

## State and Configuration Storage

```yaml
basic:
  cfgStorageType: file
```

Set `cfgStorageType` to `kv` to store configuration data in the database backend defined under `store`.

### Store Settings

```yaml
store:
  type: sqlite
  extStateType: redis
  redis:
    host: localhost
    port: 6379
    password: kuiper
    timeout: 1000
    connectionSelector: edgex.redismsgbus
  sqlite:
    name: sqliteKV.db
```

- `type`: Database backend for internal state (`sqlite`, `redis`, or `fdb`).
- `extStateType`: Storage backend queried by SQL [get_keyed_state](../sqls/functions/other_functions.md#get_keyed_state).
- `sqlite.name`: SQLite database filename (defaults to `sqliteKV.db`).
- `redis.connectionSelector`: Reuses connection credentials defined in `etc/connections/connection.yaml`.

## Portable Plugin Runtime

Configure execution parameters for Python portable plugins:

```yaml
portable:
  pythonBin: python
  initTimeout: 5000
  sendTimeout: 5000
  recvTimeout: 5000
```

- `pythonBin`: Path to the Python executable.
- `initTimeout`: Initialization timeout in milliseconds.
- `sendTimeout`: IPC message send timeout in milliseconds.
- `recvTimeout`: IPC message receive timeout in milliseconds.

## Ruleset Provisioning

rekuiper supports automatic provisioning on first startup. Place a [ruleset file](../api/restapi/ruleset.md#ruleset-format) named `init.json` into the `etc` directory. The engine loads this ruleset once during initial startup.

## FoundationDB Storage Backend

To use FoundationDB as the metadata storage engine:

1. Install FoundationDB client libraries on the host. Refer to the [FoundationDB Documentation](https://apple.github.io/foundationdb/administration.html#default-cluster-file).
2. Download matching Go bindings in the rekuiper directory:

```bash
go get github.com/apple/foundationdb/bindings/go@6.2.0
```

3. Compile the binary using `make build_with_fdb`.
4. Configure `store` settings in `etc/kuiper.yaml`:

```yaml
store:
  type: fdb
  extStateType: fdb
  fdb:
    path: /etc/foundationdb/fdb.cluster
```
