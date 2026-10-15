# Source Connectors Overview

Source connectors ingest data from external systems into the rekuiper stream processing engine.

## Ingestion Modes

Source connectors support two ingestion modes:

- **Scan Mode**: Ingests event data continuously as an unbounded stream. Use this mode for stream definitions and scan tables.
- **Lookup Mode**: Queries external records on demand when a rule executes a join. Use this mode for lookup tables.

Connectors support one or both ingestion modes. Documentation pages indicate supported modes with badges.

## Built-in Sources

rekuiper includes the following built-in source connectors:

- [Modbus source](./builtin/modbus.md): Ingests register and coil values from Modbus TCP and RTU devices.
- [OPC-UA source](./builtin/opcua.md): Ingests variable nodes from OPC-UA binary servers over TCP.
- [OpenTelemetry (OTLP) source](./builtin/otlp.md): Ingests OTLP/HTTP JSON metrics, logs, and trace spans from OpenTelemetry SDKs and agents.
- [MQTT source](./builtin/mqtt.md): Subscribes to MQTT topics.
- [RabbitMQ source](./builtin/rabbitmq.md): Consumes messages from RabbitMQ queues using AMQP 0-9-1.
- [EdgeX source](./builtin/edgex.md): Ingests sensor readings and events from the EdgeX message bus.
- [HTTP pull source](./builtin/http_pull.md): Periodically pulls data from HTTP endpoints.
- [HTTP push source](./builtin/http_push.md): Ingests data pushed to rekuiper through HTTP POST requests.
- [WebSocket source](./builtin/websocket.md): Ingests real-time events over WebSocket connections.
- [Redis source](./builtin/redis.md): Reads Redis keys or queries Redis as a lookup table.
- [RedisSub source](./builtin/redisSub.md): Subscribes to Redis pub/sub channels.
- [Kafka source](./plugin/kafka.md): Consumes stream records from Apache Kafka topics.
- [SQL source](./plugin/sql.md): Periodically queries relational databases through SQL.
- [File source](./builtin/file.md): Reads data from local files or directories.
- [Memory source](./builtin/memory.md): Reads from internal in-memory topics to create rule pipelines.
- [Simulator source](./builtin/simulator.md): Generates synthetic sensor telemetry for testing.

> [!NOTE]
> Legacy Go C-shared dynamic plugins (`.so`) are not supported in rekuiper. Connectors like Kafka, SQL, and WebSocket are compiled natively into the binary. For custom source extensions, use WebAssembly (Wasm) or an external HTTP service.

## Using Sources in Streams and Tables

To use a source connector, create a stream or table and set the `TYPE` property in the `WITH` clause.

You can configure source behavior during stream creation by setting properties such as serialization format and decompression. For property definitions and DDL syntax, refer to [Stream Management](../streams/overview.md).

## Runtime Execution Nodes

In a rule topology, a data source begins as a logical node. At runtime, the planner expands the logical source into an execution pipeline composed of multiple physical nodes.

Splitting source processing into distinct nodes provides three benefits:

1. **Component Reuse and Modularity**: Shared operations (such as decompression and payload decoding) execute in standard reusable nodes.
2. **Sub-Task Observability**: Processing stages expose fine-grained metrics for latency and throughput.
3. **Parallel Execution**: Independent stages can execute concurrently across pipeline threads.

### Source Execution Pipeline

The physical execution plan arranges source operations into sequential stages:

```txt
Connector --> RateLimit --> Decompress --> Decode --> Preprocess
```

The planner creates pipeline nodes based on stream configurations:

- **Connector**: Connects to the external system and receives raw data. Every source creates this node.
- **RateLimit**: Applies to push sources (such as MQTT) when the `interval` property is configured. This node limits the ingestion frequency. For details, refer to [Down Sampling](./down_sample.md).
- **Decompress**: Applies when the source ingests compressed binary payloads and the `decompress` property is configured.
- **Decode**: Deserializes raw bytes into records based on the configured `format` and schema definitions.
- **Preprocess**: Validates records and converts data types when a stream specifies a schema and enables `strictValidation`.
