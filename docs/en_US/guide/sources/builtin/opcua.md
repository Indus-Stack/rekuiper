# OPC-UA Source and Sink Connector

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>
<span style="background:green;color:white;padding:1px;margin:2px">lookup table source</span>
<span style="background:green;color:white;padding:1px;margin:2px">sink</span>

The OPC-UA connector provides native, 100% pure-Rust connectivity to OPC Unified Architecture (OPC-UA) servers using the standard binary TCP protocol (`opc.tcp://`).

Unlike traditional systems that require deploying heavy companion containers (such as Node-RED or commercial protocol bridges) requiring hundreds of megabytes of RAM, rekuiper communicates directly with OPC-UA servers without any external C or OpenSSL dependencies.

## Architecture and Security

- **Protocol**: OPC-UA Binary TCP (`opc.tcp://`).
- **Security Policy**: `SecurityPolicy#None` (`http://opcfoundation.org/UA/SecurityPolicy#None`).
- **Authentication**: Supports both Anonymous identity tokens and Username / Password authentication in `ActivateSession`.
- **Zero C Dependencies**: Eliminates OpenSSL and glibc/musl toolchain friction for frictionless deployment on Alpine Linux and minimal edge gateways.

## Stream and Table DDL Syntax

### Stream Source

```sql
CREATE STREAM opcua_sensors () WITH (
    TYPE="opcua",
    ENDPOINT="opc.tcp://192.168.1.100:4840",
    INTERVAL="100ms",
    TIMEOUT="2000ms",
    USERNAME="operator",
    PASSWORD="password123",
    NODES='[
        {"name": "temperature", "nodeId": "ns=2;s=Device1.Temperature"},
        {"name": "pressure", "nodeId": "ns=2;s=Device1.Pressure"},
        {"name": "machine_state", "nodeId": "ns=2;i=1004"}
    ]'
)
```

### Lookup Table

Query OPC-UA variables on-demand to enrich incoming event streams:

```sql
CREATE TABLE machine_metadata () WITH (
    TYPE="opcua",
    ENDPOINT="opc.tcp://192.168.1.100:4840",
    NODES='[
        {"name": "serial_number", "nodeId": "ns=1;s=Device.SerialNumber"},
        {"name": "firmware_version", "nodeId": "ns=1;s=Device.Firmware"}
    ]'
)
```

## Configuration Parameters

### Source and Table Parameters

| Property | Type | Default | Description |
|---|---|---|---|
| `endpoint` | string | required | OPC-UA server URL (e.g. `opc.tcp://192.168.1.100:4840`). |
| `interval` | string | `"100ms"` | Polling frequency for streaming sources (e.g. `"50ms"`, `"1s"`). |
| `timeout` | string | `"1000ms"` | Network and request timeout. |
| `username` | string | optional | Username for user token authentication. |
| `password` | string | optional | Password for user token authentication. |
| `nodes` | JSON array / string | required | List of node specifications to poll. |

### Node Specification Schema

Each element in `nodes` defines:

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Key name in the emitted JSON record. |
| `nodeId` | string | required | OPC-UA NodeId formatted string. |

### Supported NodeId Formats

The connector parses standard OPC-UA NodeId string representations:

| Identifier Type | Format | Example |
|---|---|---|
| Numeric | `ns=<namespace>;i=<integer>` or `i=<integer>` | `ns=2;i=1001` or `i=2258` |
| String | `ns=<namespace>;s=<string>` | `ns=2;s=Device1.Sensors.Temp` |
| GUID | `ns=<namespace>;g=<uuid>` | `ns=2;g=72962b91-fa75-4ae6-8d59-5f80f68d3744` |
| ByteString | `ns=<namespace>;b=<base64>` | `ns=2;b=YWJjZA==` |

## Variant Decoding & NaN Guards

The connector automatically decodes OPC-UA Variants into native JSON types:
- **Primitives**: Boolean, SByte, Byte, Int16, UInt16, Int32, UInt32, Int64, UInt64, Float, Double, String, DateTime, Guid, ByteString.
- **NaN Rejection**: Floating-point values (Float / Double) are checked for `NaN` and `Infinity`. Corrupted or uninitialized values from faulty sensors are dropped to prevent SQL engine math evaluation panics.

## OPC-UA Sink Action (Control Closed Loop)

Write control setpoints and state changes to OPC-UA servers:

```json
{
  "id": "set_valve_position",
  "sql": "SELECT target_valve_pos FROM pid_stream WHERE target_valve_pos IS NOT NULL",
  "actions": [
    {
      "opcua": {
        "endpoint": "opc.tcp://192.168.1.100:4840",
        "nodeId": "ns=2;s=Device1.Actuators.Valve1",
        "field": "target_valve_pos"
      }
    }
  ],
  "options": {
    "onError": "dlq",
    "dlq": {
      "action": {
        "mqtt": {
          "server": "tcp://127.0.0.1:1883",
          "topic": "rekuiper/dlq/opcua_errors"
        }
      }
    }
  }
}
```

### Error Handling & Dead-Letter Queue (DLQ)

When an OPC-UA write fails with permanent status codes:
- `BadNodeIdUnknown` (`0x80340000`)
- `BadTypeMismatch` (`0x803C0000`)
- `BadNodeNotInHierarchy` (`0x80350000`)
- `BadNotWritable` (`0x803B0000`)

The connector treats the failure as a permanent delivery error and routes the output event directly to the Track C Dead-Letter Queue (`DlqStage::SinkDelivery`), maintaining auditability without stalling stream execution.
