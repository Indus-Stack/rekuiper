# Kafka Source Connector

::: tip Status: Supported as Built-in Connector in rekuiper
rekuiper includes a built-in Kafka source connector implemented directly in Rust with `rskafka`. You do not need to compile or deploy Go dynamic plugins (`.so` files).
:::

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>

The Kafka source connector consumes streaming records from Apache Kafka topics into the rekuiper stream processing engine.

## Configuration Properties

Configure the Kafka source in `$rekuiper/etc/sources/kafka.yaml` or directly in the stream definition properties:

| Property Name | Optional | Default Value | Description |
|---|---|---|---|
| `brokers` | True | `"127.0.0.1:9092"` | Comma-separated list of Kafka broker addresses (`host:port`). |
| `topic` | True | Stream `DATASOURCE` | Target Kafka topic name. If omitted, rekuiper uses the stream `DATASOURCE`. |
| `partition` | True | `0` | Specific topic partition index consumed by the connector. |
| `groupId` | True | None | Kafka consumer group identifier. |
| `offsetReset` | True | `"latest"` | Initial offset position when partition begins consuming: `"earliest"` or `"latest"`. |

rekuiper ingests messages from the configured topic partition and decodes JSON objects or arrays into stream records automatically.

## Create a Stream Source

### Using In-line Stream DDL

Define a stream pointing to a Kafka topic directly in SQL:

```sql
CREATE STREAM kafka_telemetry () WITH (
  TYPE = "kafka",
  DATASOURCE = "sensor_events",
  FORMAT = "json"
);
```

### Using a Named Configuration Key

Define broker connection settings in `$rekuiper/etc/sources/kafka.yaml`:

```yaml
production_cluster:
  brokers: "10.0.1.10:9092,10.0.1.11:9092"
  partition: 0
  groupId: "rekuiper_analytics_consumer"
```

Reference the configuration key in your stream definition:

```sql
CREATE STREAM clusterStream () WITH (
  TYPE = "kafka",
  DATASOURCE = "production_telemetry",
  CONF_KEY = "production_cluster",
  FORMAT = "json"
);
```

Run streaming queries against the Kafka topic:

```sql
SELECT deviceId, AVG(temperature) AS avgTemp
FROM clusterStream
GROUP BY deviceId, TumblingWindow(ss, 10);
```
