# OPC-UA Source and Sink Connector

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>
<span style="background:green;color:white;padding:1px;margin:2px">lookup table source</span>
<span style="background:green;color:white;padding:1px;margin:2px">sink</span>

The OPC-UA connector connects to OPC Unified Architecture (OPC-UA) servers using the binary TCP protocol (`opc.tcp://`).

The connector runs in pure Rust without external C libraries or companion containers.

## Architecture and Security

- **Protocol**: OPC-UA Binary TCP (`opc.tcp://`).
- **Security Policy**: `SecurityPolicy#None`.
- **Authentication**: Anonymous identity tokens and Username / Password authentication.
- **Dependencies**: Zero C library dependencies.

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

## Variant Decoding and Float Validation

The connector decodes OPC-UA Variants into JSON types:
- **Primitive Types**: Boolean, SByte, Byte, Int16, UInt16, Int32, UInt32, Int64, UInt64, Float, Double, String, DateTime, Guid, ByteString.
- **Float Validation**: The connector checks Float and Double values for `NaN` and `Infinity`. It rejects invalid values to prevent SQL evaluation errors.

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

### Dead-Letter Queue (DLQ) Integration

When an OPC-UA write fails with permanent status codes:
- `BadNodeIdUnknown` (`0x80340000`)
- `BadTypeMismatch` (`0x803C0000`)
- `BadNodeNotInHierarchy` (`0x80350000`)
- `BadNotWritable` (`0x803B0000`)

The connector classifies the failure as a permanent delivery error. It sends the output event to the dead-letter queue under `DlqStage::SinkDelivery`.
