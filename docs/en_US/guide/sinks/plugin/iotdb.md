# Apache IoTDB Sink

The `iotdb` sink enables writing stream processing outputs directly into [Apache IoTDB](https://iotdb.apache.org/) time-series databases using IoTDB's native RESTful service.

In `rekuiper`, the Apache IoTDB connector is implemented in pure Rust without external dynamic dependencies, supporting high performance, connection pooling, dynamic device templates, and automatic batch tablet aggregation.

## Prerequisites

Ensure the Apache IoTDB REST service is enabled in IoTDB configuration (`iotdb-datanode.properties`: `enable_rest_service=true`, default port `18080`).

## Configuration Properties

Configure the `iotdb` sink action using the following properties:

| Property Name | Type | Required | Default | Description |
|---|---|---|---|---|
| `device` | String | **Yes** | - | Target IoTDB device path (e.g. `root.factory.d1`). Supports dynamic data templates (e.g. `root.factory.{{.deviceId}}`). |
| `addr` | String | No | `http://127.0.0.1:18080` | IoTDB REST service address (`host:port` or URL). |
| `username` | String | No | `root` | Authentication username. |
| `password` | String | No | `root` | Authentication password. |
| `measurements` | Array | No | - | List of measurement column names to insert. If omitted, all non-timestamp fields are included. |
| `tsFieldName` | String | No | `ts` | Name of the timestamp column in milliseconds. If omitted or not present, server wall clock is used. |

## Examples

### Static Device Path Insert

Writes high-frequency telemetry into device `root.ln.wf01.wt01`:

```json
{
  "id": "rule_iotdb_static",
  "sql": "SELECT temperature, humidity, pressure, ts FROM sensorStream",
  "actions": [
    {
      "iotdb": {
        "addr": "http://127.0.0.1:18080",
        "username": "root",
        "password": "root",
        "device": "root.ln.wf01.wt01",
        "measurements": ["temperature", "humidity", "pressure"],
        "tsFieldName": "ts"
      }
    }
  ]
}
```

### Dynamic Device Template

Uses Golang-style data template syntax to route telemetry dynamically to devices based on incoming `deviceId`:

```json
{
  "id": "rule_iotdb_dynamic",
  "sql": "SELECT deviceId, voltage, current, ts FROM powerStream",
  "actions": [
    {
      "iotdb": {
        "addr": "http://127.0.0.1:18080",
        "username": "root",
        "password": "password",
        "device": "root.station.{{.deviceId}}",
        "measurements": ["voltage", "current"],
        "tsFieldName": "ts"
      }
    }
  ]
}
```
