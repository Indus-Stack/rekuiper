# InfluxDB Sink

::: tip Status: Supported as Built-in Connector in rekuiper
The InfluxDB v1.x sink is compiled natively into the rekuiper engine binary. It provides zero-copy serialization of stream records into InfluxDB Line Protocol without external plugins or dynamic C libraries.
:::

## Overview

The `influx` sink publishes processed stream records directly to InfluxDB v1.x databases using HTTP Line Protocol. It formats tags, fields, and timestamps automatically, and supports connection pooling and batch writing.

## Configuration Properties

| Property Name | Optional | Description |
| :--- | :--- | :--- |
| `addr` | False | Network address of the InfluxDB server (e.g., `http://127.0.0.1:8086`). |
| `database` | False | Target InfluxDB database name. |
| `measurement` | False | Target measurement table name. Supports dynamic data templates (e.g., `"{{.device}}"`). |
| `username` | True | HTTP Basic Authentication username. |
| `password` | True | HTTP Basic Authentication password. |
| `precision` | True | Timestamp precision (`ms`, `s`, `us`, `ns`). Default is `ms`. |
| `tags` | True | Key-value mapping of tag names to template strings (e.g., `{"host": "{{.host}}", "zone": "east"}`). |
| `fields` | True | Array of field property names to write. If omitted, all non-tag payload fields are written. |
| `tsFieldName` | True | Payload property name containing the record timestamp. If omitted, the engine uses current epoch time. |
| `sendSingle` | True | Whether to emit records individually (`true`) or in array batches (`false`). Default is `true`. |

## Example Rule Configuration

The following rule publishes high-temperature alerts to InfluxDB v1.x:

```json
{
  "id": "rule_influx_alert",
  "sql": "SELECT deviceId, temperature, humidity, ts FROM sensorStream WHERE temperature > 80.0",
  "actions": [
    {
      "influx": {
        "addr": "http://127.0.0.1:8086",
        "database": "telemetry",
        "measurement": "sensor_readings",
        "tags": {
          "device": "{{.deviceId}}",
          "site": "factory_floor"
        },
        "fields": ["temperature", "humidity"],
        "tsFieldName": "ts",
        "precision": "ms"
      }
    }
  ]
}
```
