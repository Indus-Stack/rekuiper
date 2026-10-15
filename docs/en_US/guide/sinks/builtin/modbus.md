# Modbus Action

The Modbus action writes register and coil values to industrial Modbus TCP or Modbus RTU-over-TCP devices, enabling closed-loop control from streaming SQL rules.

## Properties

| Property Name | Optional | Description |
|---|---|---|
| `endpoint` | false | Host and port of the Modbus server (e.g. `192.168.1.50:502`) or Unix device path. |
| `mode` | true | Protocol framing mode: `"tcp"` (default) or `"rtu_over_tcp"` / `"rtu"`. |
| `unitId` | true | Target slave unit ID (`0..=255`). Default: `1`. Alias: `slaveId`. |
| `function` | true | Target function code: `"holding"` (FC06/FC16, default) or `"coil"` (FC05/FC15). |
| `registerAddress` | false | 0-based wire address of the target register or coil. |
| `field` | false | The field name in the output record whose value should be written. |
| `dataType` | true | Value type: `bool`, `uint16`, `int16`, `uint32`, `int32`, `uint64`, `int64`, `float32`, `float64`. Default: `uint16`. |
| `byteOrder` | true | Word/byte layout for multi-register fields: `ABCD` (default), `CDAB`, `BADC`, or `DCBA`. |
| `timeout` | true | Socket write timeout (e.g. `1000ms`). Default: `1000ms`. |

Other common sink properties are supported. Refer to [sink common properties](../overview.md#common-properties) for more information.

## Closed-Loop Example

Write a calculated setpoint to holding register 100 on an industrial chiller:

```json
{
  "id": "rule_chiller_actuation",
  "sql": "SELECT target_setpoint FROM telemetry_stream WHERE target_setpoint IS NOT NULL",
  "actions": [
    {
      "modbus": {
        "endpoint": "192.168.1.50:502",
        "unitId": 1,
        "function": "holding",
        "registerAddress": 100,
        "field": "target_setpoint",
        "dataType": "float32",
        "byteOrder": "CDAB"
      }
    }
  ]
}
```

## Error Handling & Dead-Letter Queue (DLQ)

When a write command fails due to Modbus device exceptions:
- `0x01`: Illegal Function
- `0x02`: Illegal Data Address
- `0x03`: Illegal Data Value

rekuiper flags the failure as a permanent delivery error and routes the output record along with error diagnostics to the configured Dead-Letter Queue (`DlqStage::SinkDelivery`). For full configuration options, refer to [Dead-Letter Queues (DLQ) & Error Routing](../../rules/error_handling_dlq.md).
