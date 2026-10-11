# InfluxDB v2 Sink

::: tip Status: Supported as Built-in Connector in rekuiper
The InfluxDB v2.x sink is compiled natively into the rekuiper engine binary. It provides zero-copy serialization of stream records into InfluxDB Line Protocol and posts directly to `/api/v2/write` with token authentication.
:::

## Overview

The `influx2` sink publishes processed stream records directly to InfluxDB v2.x instances (including InfluxDB Cloud) using Line Protocol over HTTP. It formats measurement tags, fields, and timestamps automatically, with support for batching and connection pooling.

## Configuration Properties

| Property Name | Optional | Description |
| :--- | :--- | :--- |
| `addr` | False | Network address of the InfluxDB v2 server (e.g., `http://127.0.0.1:8086` or InfluxDB Cloud URL). |
| `org` | False | Target InfluxDB v2 organization name. |
| `bucket` | False | Target InfluxDB v2 bucket name. |
| `token` | True | InfluxDB API authentication token. |
| `measurement` | False | Target measurement table name. Supports dynamic data templates (e.g., <code v-pre>"{{.device}}"</code>). |
| `precision` | True | Timestamp precision (`ms`, `s`, `us`, `ns`). Default is `ms`. |
| `tags` | True | Key-value mapping of tag names to template strings (e.g., <code v-pre>{"host": "{{.host}}", "site": "plant_1"}</code>). |
| `fields` | True | Array of field property names to write. If omitted, all non-tag payload fields are written. |
| `tsFieldName` | True | Payload property name containing the record timestamp. If omitted, the engine uses current epoch time. |
| `sendSingle` | True | Whether to emit records individually (`true`) or in array batches (`false`). Default is `true`. |

## Example Rule Configuration

The following rule publishes air quality metrics to InfluxDB v2:

```json
{
  "id": "rule_influx2_air_quality",
  "sql": "SELECT location, co2, temp, ts FROM sensorStream",
  "actions": [
    {
      "influx2": {
        "addr": "http://127.0.0.1:8086",
        "org": "my_org",
        "bucket": "iot_metrics",
        "token": "my-secret-influx-token",
        "measurement": "air_quality",
        "tags": {
          "location": "{{.location}}"
        },
        "fields": ["co2", "temp"],
        "tsFieldName": "ts",
        "precision": "ms"
      }
    }
  ]
}
```
