# Sink Connectors Overview

In rekuiper, core sink connectors are compiled directly into the binary to provide high throughput and zero external runtime dependencies.

## Built-in Sinks

rekuiper includes the following built-in sink connectors:

- [Modbus sink](./builtin/modbus.md): Writes coils and holding registers to Modbus TCP and RTU devices.
- [OPC-UA sink](./builtin/opcua.md): Writes variable values to OPC-UA binary servers.
- [OpenTelemetry (OTLP) sink](./builtin/otlp.md): Exports records to OpenTelemetry Collectors and OTLP/HTTP endpoints.
- [MQTT sink](./builtin/mqtt.md): Publishes messages to an external MQTT broker.
- [RabbitMQ sink](./builtin/rabbitmq.md): Publishes messages to RabbitMQ exchanges using AMQP 0-9-1.
- [EdgeX sink](./builtin/edgex.md): Publishes events to the EdgeX Foundry message bus.
- [REST sink](./builtin/rest.md): Sends HTTP requests (`POST`, `PUT`, `PATCH`) to webhooks and REST endpoints.
- [Redis sink](./builtin/redis.md): Writes keys and stream entries to Redis.
- [RedisSub sink](./builtin/redisPub.md): Publishes messages to Redis pub/sub channels.
- [File sink](./builtin/file.md): Writes event records to local files in JSON, CSV, or line-delimited formats.
- [Memory sink](./builtin/memory.md): Publishes records to in-memory topics to chain rules.
- [Log sink](./builtin/log.md): Writes output records to system logs for debugging.
- [Nop sink](./builtin/nop.md): Discards output records for performance benchmarking.
- [Kafka sink](./plugin/kafka.md): Publishes records directly to Apache Kafka topics.
- [SQL sink](./plugin/sql.md): Inserts or updates records in relational databases through SQL.
- [InfluxDB sink](./plugin/influx.md): Publishes timeseries points directly to InfluxDB v1.x via Line Protocol.
- [InfluxDB v2 sink](./plugin/influx2.md): Publishes timeseries points directly to InfluxDB v2.x via Line Protocol.
- [TDengine 3 sink](./plugin/tdengine3.md): Ingests timeseries records into TDengine 3.x tables and super tables.
- [Apache IoTDB sink](./plugin/iotdb.md): Writes time-series points to Apache IoTDB REST service.
- [Image sink](./plugin/image.md): Saves binary camera image frames with retention pruning policies.
- [WebSocket sink](./builtin/websocket.md): Streams events to connected WebSocket clients.

> [!NOTE]
> Legacy Go C-shared dynamic plugins (`.so`) are not supported in rekuiper. High-demand connectors like Kafka, SQL, InfluxDB, TDengine 3, IoTDB, Image, and WebSocket are built into the binary. For custom integrations, use WebAssembly (Wasm) or an external HTTP service.

## Updatable Sinks

By default, sinks append records to external systems. When external systems support record updates (such as SQL databases or key-value stores), updatable sinks execute modifications and deletions.

The following sinks support updatable operations:

- Memory sink
- Redis sink
- SQL sink

To activate update operations, configure `rowkindField` to identify the action field in the output record:

```json
{
  "redis": {
    "addr": "127.0.0.1:6379",
    "dataType": "string",
    "field": "id",
    "rowkindField": "action",
    "sendSingle": true
  }
}
```

The output event must contain an action command field. Valid action values include `insert`, `update`, `upsert`, and `delete`:

```json
{"action": "update", "id": 5, "name": "abc"}
```

This event updates the record with `id = 5` to the new name.

## Common Properties

Each sink action supports these common configuration properties:

| Property Name | Type and Default | Description |
|---|---|---|
| `bufferLength` | int: `1024` | Maximum number of messages buffered in memory. When the buffer fills, the sink blocks incoming records until queued messages depart. |
| `disable` | bool: `false` | When set to `true`, disables this sink action. At least one sink action in a rule must remain active. |
| `omitIfEmpty` | bool: `false` | When set to `true`, discards empty `SELECT` query results instead of sending them to the sink. |
| `sendSingle` | bool: `false` | When `false`, the sink sends results as a JSON array (`{"result":"[{\"count\":30},{\"count\":20}]"}`). When `true`, the sink sends records individually (`{"count":30}`, then `{"count":20}`). |
| `dataTemplate` | string: `""` | [Go template](https://golang.org/pkg/text/template) string that transforms output payloads. Refer to [Data Templates](./data_template.md). |
| `format` | string: `"json"` | Serialization format: `"json"`, `"delimited"`, `"binary"`, `"protobuf"`, `"custom"`, or `"urlencoded"`. |
| `schemaId` | string: `""` | Schema identifier for encoding results. |
| `delimiter` | string: `","` | Delimiter character when using delimited formats. Default is a comma. |
| `fields` | []string: `nil` | Array of field names selected for output. If set, only specified fields are sent. |
| `excludeFields` | []string: `nil` | Array of field names excluded from output. If set, specified fields are omitted from sent records. |
| `dataField` | string: `""` | Top-level property key extracted from template output before field filtering. |
| `enableCache` | bool: Global default | Enables disk and memory caching during network failures. |
| `memoryCacheThreshold` | int: Global default | Maximum number of messages cached in memory for immediate replay after failure recovery. |
| `maxDiskCache` | int: Global default | Maximum number of messages cached on disk (FIFO order). |
| `bufferPageSize` | int: Global default | Buffer page size for bulk disk I/O operations. |
| `resendInterval` | int: Global default | Interval in milliseconds between resent messages after network recovery. |
| `cleanCacheAtStop` | bool: Global default | When `true`, clears memory and disk caches when a rule stops. When `false`, saves memory cache to disk upon stop. |
| `resendAlterQueue` | bool: Global default | When `true`, routes retransmitted cache records to an alternate queue. |
| `resendPriority` | int: Global default | Retransmission priority: `-1` (live data first), `0` (equal priority), `1` (cached data first). |
| `resendIndicatorField` | string: Global default | Boolean field name added to retransmitted messages (set to `true` on replay). |
| `resendDestination` | string: `""` | Alternate topic or URL for retransmitted messages. Refer to [Sinks with Resend Destination Support](#sinks-with-resend-destination-support). |
| `batchSize` | int: `0` | Number of messages accumulated before emission. |
| `lingerInterval` | int: `0` | Maximum wait time in milliseconds before sending accumulated batch records. |
| `compression` | string: `""` | Payload compression algorithm: `"zlib"`, `"gzip"`, `"flate"`, or `"zstd"`. |
| `encryption` | string: `""` | Payload encryption algorithm: `"aes"`. |

### AES Encryption Key Configuration

When a sink specifies `"encryption": "aes"`, configure `basic.aesKey` in `etc/kuiper.yaml`. The key must be a base64-encoded string of 16, 24, or 32 bytes (AES-128, AES-192, or AES-256).

Generate a 32-byte key:

```bash
openssl rand -base64 32
```

Configure the key in `etc/kuiper.yaml`:

```yaml
basic:
  aesKey: <base64-encoded-key>
```

Alternatively, set the environment variable `KUIPER__BASIC__AESKEY`.

> [!IMPORTANT]
> The distribution does not include a default AES key. Configure an AES key before deploying rules that use AES encryption; otherwise, the engine reports `AES Key is not defined`.

### Dynamic Properties

Sink parameters support dynamic values resolved from record fields using [Data Template](./data_template.md) syntax.

The following example resolves the MQTT publish topic dynamically from the query result:

```json
{
  "id": "rule1",
  "sql": "SELECT topic FROM demo",
  "actions": [
    {
      "mqtt": {
        "sendSingle": true,
        "topic": "prefix/{{.topic}}"
      }
    }
  ]
}
```

::: v-pre
When `sendSingle` is `false`, access array elements by index: `{{index . 0 "topic"}}`.
:::

## Sink Caching

Sinks provide two-tier caching (memory and disk) to prevent data loss during network disconnections.

When a transient network error occurs, the sink buffers records in memory. If the memory cache reaches `memoryCacheThreshold`, records spill to SQLite disk storage (`data/cache.db`). When the connection restores, the sink replays cached records automatically without restarting the rule.

### Retransmission Flow

1. **Error Detection**: Sinks distinguish transient network errors from permanent payload validation errors. Transient errors return failed delivery acknowledgments, which retain records in cache.
2. **Cache Rotation**: If disk storage fills, the earliest memory records are replaced with disk pages in FIFO order.
3. **Resend Execution**: In synchronous mode, the engine tests connectivity by sending the first cached record. When successful, the engine replays queued records sequentially according to `resendInterval`.
4. **Traffic Separation**: Configure `resendAlterQueue: true` to route retransmitted records to separate topics or endpoints and maintain priority ordering.

### Cache Configuration Example

```json
{
  "id": "rule1",
  "sql": "SELECT * FROM demo",
  "actions": [
    {
      "log": {},
      "mqtt": {
        "server": "tcp://127.0.0.1:1883",
        "topic": "result/cache",
        "qos": 0,
        "enableCache": true,
        "memoryCacheThreshold": 2048,
        "maxDiskCache": 204800,
        "bufferPageSize": 512,
        "resendInterval": 10
      }
    }
  ]
}
```

### Sinks with Resend Destination Support

The following sinks support alternate retransmission destinations through `resendDestination`:

- **MQTT sink**: Specifies an alternate MQTT retransmission topic.
- **REST sink**: Specifies an alternate HTTP endpoint URL.
- **Memory sink**: Specifies an alternate in-memory retransmission topic.

## Resource Reuse

Reuse connection configurations across multiple rules by creating a YAML file named after the sink type in `etc/sinks/` (for example, `etc/sinks/mqtt.yaml`):

```yaml
test:
  qos: 1
  server: "tcp://127.0.0.1:1883"
```

Reference the configuration block in rule definitions using `resourceId`:

```json
{
  "mqtt": {
    "resourceId": "test",
    "topic": "devices/demo_001/events",
    "protocolVersion": "3.1.1",
    "clientId": "demo_001"
  }
}
```

## Runtime Execution Nodes

The planner decomposes a logical sink action into an execution pipeline:

### Standard Pipeline

```txt
Transform --> Encode --> Compress --> Encrypt --> Cache --> Connect
```

- **Transform**: Applies `dataTemplate`, `dataField`, and `fields` projections.
- **Encode**: Serializes records into binary payloads based on `format` and schema definitions.
- **Compress**: Compresses payloads using the configured algorithm (`gzip`, `zstd`).
- **Encrypt**: Encrypts payloads using AES encryption.
- **Cache**: Manages memory and disk caching during network outages.
- **Connect**: Connects to the external system and delivers payloads.

### Batch Pipeline

When you configure `batchSize` or `lingerInterval`, the planner instantiates a batch pipeline:

```txt
Batch --> Transform --> Writer --> Compress --> Encrypt --> Cache --> Connect
```

- **Batch**: Monitors batch trigger conditions based on message counts or timeouts.
- **Writer**: Streams and serializes batch records before compression.

When using batching with `dataTemplate`, the template must define a single batch element, not the complete array. Configure `sendSingle: true` for record-oriented templates.
