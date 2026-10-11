# Connectors

Connectors connect the rekuiper stream processing engine to external systems, such as message brokers, databases, and file systems.

Connectors ingest data from external systems into rekuiper and dispatch processed results to target endpoints. Connectors support edge environments, local networks, and cloud infrastructures.

Connectors belong to two categories:

- **Source Connectors**: Ingest data from external systems into the rekuiper processing pipeline.
- **Sink Connectors**: Dispatch processed query results from rekuiper to external destinations.

rekuiper provides built-in connectors for common protocols and supports plugin connectors for custom protocols.

## Source Connectors

[Source connectors](./sources/overview.md) ingest external data into streams or tables. Sources operate in two modes:

1. **Streaming mode**: Ingests sequential, unbounded events in real time.
2. **Reference mode**: Fetches batch snapshots or performs key lookups (used with tables).

To configure a source, specify the source type in the `WITH` clause of the stream or table definition.

### Built-in Source Connectors

rekuiper includes the following built-in source connectors:

- [MQTT source](./sources/builtin/mqtt.md): Ingests messages from MQTT topics.
- [EdgeX source](./sources/builtin/edgex.md): Ingests event data from EdgeX Foundry message buses.
- [HTTP pull source](./sources/builtin/http_pull.md): Pulls data periodically from HTTP endpoints.
- [HTTP push source](./sources/builtin/http_push.md): Receives incoming HTTP requests sent to rekuiper endpoints.
- [File source](./sources/builtin/file.md): Reads data from local files. Commonly used as tables.
- [Memory source](./sources/builtin/memory.md): Consumes events from internal memory topics to create rule pipelines.
- [Redis source](./sources/builtin/redis.md): Queries key-value data in Redis as a lookup table.

### Plugin Source Connectors

Use plugin connectors for specialized data protocols or custom systems:

- [SQL source](./sources/plugin/sql.md): Queries relational databases on a schedule or as a lookup table.
- [Video source](./sources/plugin/video.md): Captures image frames from video streams.
- [Random source](./sources/plugin/random.md): Generates synthetic mock data for functional testing.
- [ZeroMQ source](./sources/plugin/zmq.md): Consumes messages from ZeroMQ publishers.
- [Kafka source](./sources/plugin/kafka.md): Consumes event streams from Apache Kafka topics.

## Sink Connectors

Sink connectors transfer processed output records to external endpoints. Sinks support disk caching to manage network interruptions and prevent data loss. Sinks also support dynamic properties and shared connection pools.

### Built-in Sink Connectors

rekuiper includes the following built-in sink connectors:

- [MQTT sink](./sinks/builtin/mqtt.md): Publishes messages to an external MQTT broker.
- [EdgeX sink](./sinks/builtin/edgex.md): Sends events to EdgeX Foundry. Available when compiled with the EdgeX build tag.
- [REST sink](./sinks/builtin/rest.md): Sends HTTP requests to external web servers.
- [Redis sink](./sinks/builtin/redis.md): Writes key-value records and data structures to Redis.
- [File sink](./sinks/builtin/file.md): Writes output records to local files.
- [Memory sink](./sinks/builtin/memory.md): Publishes records to internal memory topics to feed downstream rules.
- [Log sink](./sinks/builtin/log.md): Writes output records to system log files for diagnostic debugging.
- [Nop sink](./sinks/builtin/nop.md): Discards output records without I/O operations for performance benchmarking.

### Plugin Sink Connectors

Use plugin sink connectors for external platforms and custom targets:

- [InfluxDB sink](./sinks/plugin/influx.md): Writes time-series points to InfluxDB v1.x.
- [InfluxDB v2 sink](./sinks/plugin/influx2.md): Writes time-series points to InfluxDB v2.x.
- [TDengine 3 sink](./sinks/plugin/tdengine3.md): Writes time-series records to TDengine 3.x tables and super tables.
- [Image sink](./sinks/plugin/image.md): Writes binary image frames to local storage.
- [ZeroMQ sink](./sinks/plugin/zmq.md): Publishes messages to ZeroMQ subscribers.
- [Kafka sink](./sinks/plugin/kafka.md): Produces messages to Apache Kafka topics.

### Data Templates in Sinks

[Data templates](./sinks/data_template.md) transform output payloads to match target external formats. Data templates use the Golang text template syntax to support field mapping, conditional formatting, and iteration.

## Batch Configuration

rekuiper supports batch import and export of connector, stream, and rule configurations through the REST API:

```json
{
  "streams": {},
  "tables": {},
  "rules": {},
  "nativePlugins": {},
  "portablePlugins": {},
  "sourceConfig": {},
  "sinkConfig": {}
}
```

For configuration import and export procedures, refer to [Data Import and Export Management](../api/restapi/data.md).
