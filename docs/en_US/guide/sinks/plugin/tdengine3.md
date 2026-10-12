# TDengine 3 Sink

The `tdengine` (or `tdengine3`) sink enables writing stream processing outputs directly into [TDengine](https://tdengine.com/) 3.x instances using TDengine's native RESTful HTTP connector.

In `rekuiper`, the TDengine connector is a built-in component that connects to the TDengine REST service. It does not require `libtaos.so`.

## Prerequisites

Ensure the TDengine REST service (taosAdapter or built-in REST service) is running and accessible (default port `6041`).

## Configuration Properties

Configure the `tdengine` or `tdengine3` sink action using the following properties:

| Property Name | Type | Required | Default | Description |
|---|---|---|---|---|
| `database` | String | **Yes** | - | Target TDengine database name. |
| `table` | String | **Yes** | - | Target table name. Supports dynamic data templates (e.g. <code v-pre>d_{{.deviceId}}</code>). |
| `host` | String | No | `localhost` | TDengine REST server hostname or IP address. |
| `port` | Integer | No | `6041` | TDengine REST service port. |
| `user` | String | No | `root` | Authentication username. |
| `password` | String | No | `taosdata` | Authentication password. |
| `sTable` | String | No | - | Optional super table name. When specified, sub-tables are automatically created if they do not exist. |
| `tagFields` | Array | No | - | Field names used as tag columns when writing to a super table. |
| `fields` | Array | No | - | List of columns to project and insert. If omitted, all non-tag record fields are written. |
| `provideTs` | Boolean | No | `true` | If `true`, the value from `tsFieldName` is inserted. If `false`, database server clock `now` is used. |
| `tsFieldName` | String | No | `ts` | Name of the timestamp column in the input event stream. |

## Examples

### Regular Table Insert

Writes sensor telemetry directly to table `readings` in database `factory`:

```json
{
  "id": "rule_tdengine_regular",
  "sql": "SELECT deviceId, temperature, humidity, ts FROM sensorStream",
  "actions": [
    {
      "tdengine3": {
        "host": "127.0.0.1",
        "port": 6041,
        "database": "factory",
        "table": "readings",
        "user": "root",
        "password": "password",
        "fields": ["deviceId", "temperature", "humidity"],
        "tsFieldName": "ts",
        "provideTs": true
      }
    }
  ]
}
```

### Super Table with Automatic Sub-Table Creation

Uses dynamic table naming (<code v-pre>m_{{.deviceId}}</code>) and automatically instantiates sub-tables from super table `meters` with tag `deviceId`:

```json
{
  "id": "rule_tdengine_super_table",
  "sql": "SELECT deviceId, voltage, current, ts FROM powerStream",
  "actions": [
    {
      "tdengine3": {
        "host": "127.0.0.1",
        "port": 6041,
        "database": "power",
        "table": "m_{{.deviceId}}",
        "sTable": "meters",
        "tagFields": ["deviceId"],
        "fields": ["voltage", "current"],
        "tsFieldName": "ts",
        "provideTs": true
      }
    }
  ]
}
```
