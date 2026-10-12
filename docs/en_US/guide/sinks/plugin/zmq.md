# ZeroMQ Sink

::: tip Status: Supported as Built-in Connector in rekuiper
rekuiper includes a built-in ZeroMQ sink connector that implements the ZMTP protocol over TCP. It does not require an external dynamic library.
:::

The ZeroMQ sink publishes rule execution results to a ZeroMQ PUB socket over TCP.

## Configuration Parameters

| Property Name | Type | Optional | Default Value | Description |
| :--- | :--- | :--- | :--- | :--- |
| `server` | string | False | `"tcp://127.0.0.1:5563"` | The ZeroMQ endpoint URL address (such as `tcp://127.0.0.1:5563`). |
| `url` | string | True | None | Alias for `server`. |
| `topic` | string | True | None | The ZeroMQ topic name to publish to. Supports Golang-style data template interpolation (e.g. <code v-pre>"devices/{{.deviceId}}"</code>). |
| `mode` | string | True | `"connect"` | Connection mode: `"connect"` (connects to an external ZeroMQ broker or subscriber) or `"bind"` (binds locally and listens for subscribers). |
| `sendSingle` | bool | True | `true` | If true, sends each record individually. If false, sends arrays in batch mode. |
| `dataTemplate` | string | True | None | Golang-style template string to format output payloads prior to publication. |

Common sink properties (such as `sendSingle`, `dataTemplate`, and `resendDestination`) are fully supported. Refer to [Common Sink Properties](../overview.md#common-properties).

## Wire Protocol Framing

ZeroMQ PUB/SUB follows standard multi-frame encapsulation:
1. When `topic` is configured: Frame 0 contains the UTF-8 topic prefix (enabling ZeroMQ subscriber prefix filtering), and Frame 1 contains the JSON message payload bytes.
2. When `topic` is omitted: A single frame containing the JSON payload bytes is emitted.

## Sample Usage

### Basic Publication

```json
{
  "id": "rule_zmq_alerts",
  "sql": "SELECT deviceId, temperature FROM demo WHERE temperature > 50",
  "actions": [
    {
      "zmq": {
        "server": "tcp://127.0.0.1:5563",
        "topic": "alerts/temp",
        "sendSingle": true
      }
    }
  ]
}
```

### Dynamic Topic Interpolation with Data Template

```json
{
  "id": "rule_zmq_templated",
  "sql": "SELECT deviceId, pressure, ts FROM demo",
  "actions": [
    {
      "zmq": {
        "server": "tcp://127.0.0.1:5563",
        "topic": "telemetry/{{.deviceId}}",
        "dataTemplate": "{\"dev\":\"{{.deviceId}}\",\"val\":{{.pressure}},\"time\":{{.ts}}}"
      }
    }
  ]
}
```
