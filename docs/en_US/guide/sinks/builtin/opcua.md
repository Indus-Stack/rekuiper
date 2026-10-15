# OPC-UA Action

The OPC-UA action writes values to variable nodes on OPC-UA servers through `opc.tcp://` connections.

## Properties

| Property Name | Optional | Description |
|---|---|---|
| `endpoint` | false | OPC-UA server URL (e.g. `opc.tcp://192.168.1.100:4840`). |
| `nodeId` | false | Target OPC-UA NodeId string (e.g. `ns=2;s=Device1.TargetSpeed` or `ns=1;i=1001`). |
| `field` | false | The field name in the output record whose value should be written. |
| `username` | true | Optional username credential for user token authentication. |
| `password` | true | Optional password credential for user token authentication. |
| `timeout` | true | Socket and write request timeout (e.g. `1000ms`). Default: `1000ms`. |

Other common sink properties are supported. Refer to [sink common properties](../overview.md#common-properties) for more information.

## Example

```json
{
  "id": "rule_opcua_control",
  "sql": "SELECT setpoint FROM temperature_alerts WHERE setpoint IS NOT NULL",
  "actions": [
    {
      "opcua": {
        "endpoint": "opc.tcp://192.168.1.100:4840",
        "nodeId": "ns=2;s=HeaterController.TargetTemp",
        "field": "setpoint",
        "timeout": "2000ms"
      }
    }
  ]
}
```

## Dead-Letter Queue (DLQ) Integration

When an OPC-UA write fails with permanent status codes:
- `BadNodeIdUnknown` (`0x80340000`)
- `BadTypeMismatch` (`0x803C0000`)
- `BadNotWritable` (`0x803B0000`)

The connector classifies the error as permanent. It sends the output record to the dead-letter queue under `DlqStage::SinkDelivery`. For configuration options, refer to [Dead-Letter Queues](../../rules/error_handling_dlq.md).
