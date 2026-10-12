# ZeroMQ Source Connector

::: tip Status: Supported as Built-in Connector in rekuiper
rekuiper includes a built-in ZeroMQ source connector that implements the ZMTP protocol over TCP. It does not require an external dynamic library.
:::

The ZeroMQ source connector subscribes to ZeroMQ publishers over TCP using a SUB socket and ingests messages into the streaming processing engine.

## Configuration Parameters

Configuration can be specified in `etc/sources/zmq.yaml` or directly inside the `CREATE STREAM` DDL `WITH` options:

```yaml
default:
  server: tcp://127.0.0.1:5563
  topic: events
  mode: connect

test:
  server: tcp://192.168.1.50:5563
  topic: sensor/temperature
  mode: connect
```

### Parameters

- `server`: ZeroMQ publisher endpoint URL address (such as `tcp://127.0.0.1:5563`).
- `url`: Optional alias for `server`.
- `topic`: ZeroMQ topic subscription prefix (default is empty string, which subscribes to all messages).
- `mode`: `"connect"` (default, connects to an external ZeroMQ publisher) or `"bind"` (binds locally and waits for publishers to connect).

## Stream Definition

### Inline Stream Configuration

```sql
CREATE STREAM zmq_telemetry (
  temperature FLOAT,
  humidity FLOAT,
  deviceId STRING
) WITH (
  TYPE = "zmq",
  SERVER = "tcp://127.0.0.1:5563",
  DATASOURCE = "telemetry/devices",
  FORMAT = "JSON"
);
```

### Configuration via `CONF_KEY`

```sql
CREATE STREAM zmq_demo () WITH (
  TYPE = "zmq",
  CONF_KEY = "test",
  FORMAT = "JSON"
);
```

## Multi-frame and Topic Ingestion

The ZeroMQ source connector automatically handles:
- **Two-frame messages**: Frame 0 is interpreted as the topic identifier, and Frame 1 contains the JSON record payload. The topic string is injected into the record's metadata (`meta(topic)`).
- **Single-frame messages**: The frame directly contains the JSON payload.
