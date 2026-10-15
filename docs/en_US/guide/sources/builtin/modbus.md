# Modbus Source and Sink Connector

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>
<span style="background:green;color:white;padding:1px;margin:2px">lookup table source</span>
<span style="background:green;color:white;padding:1px;margin:2px">sink</span>

The Modbus connector connects to industrial automation equipment, PLCs, remote terminal units (RTUs), and power meters.

The connector runs in pure Rust. It connects directly to Modbus devices without external C libraries or gateway containers.

## Supported Protocol Modes

- **Modbus TCP**: MBAP header framing through TCP sockets (`mode="tcp"`).
- **Modbus RTU-over-TCP**: Binary packets with CRC-16 checksum through TCP sockets (`mode="rtu_over_tcp"` or `"rtu"`).
- **Modbus RTU over Serial Device**: On Unix targets, a path starting with `/` (such as `/dev/ttyUSB0`) connects to serial devices.

## Stream and Table DDL Syntax

### Stream Source

```sql
CREATE STREAM plc_sensors () WITH (
    TYPE="modbus",
    ENDPOINT="192.168.1.50:502",
    INTERVAL="100ms",
    TIMEOUT="1000ms",
    UNIT_ID=1,
    AUTO_OFFSET=true,
    REGISTERS='[
        {"name": "temperature", "address": 40001, "function": "holding", "dataType": "float32", "byteOrder": "ABCD", "scale": 0.1},
        {"name": "rpm", "address": 40003, "function": "holding", "dataType": "uint16"},
        {"name": "pump_running", "address": 1, "function": "coil", "dataType": "bool"}
    ]'
)
```

### Lookup Table

Modbus can also be queried on-demand to enrich incoming event streams during SQL joins:

```sql
CREATE TABLE plc_config () WITH (
    TYPE="modbus",
    ENDPOINT="192.168.1.50:502",
    UNIT_ID=1,
    REGISTERS='[
        {"name": "setpoint_temp", "address": 100, "function": "holding", "dataType": "float32"},
        {"name": "max_pressure", "address": 102, "function": "holding", "dataType": "uint16"}
    ]'
)
```

## Configuration Parameters

### Source and Table Parameters

| Property | Type | Default | Description |
|---|---|---|---|
| `endpoint` | string | required | Host and port (e.g. `192.168.1.50:502`) or Unix device path. |
| `mode` | string | `"tcp"` | Protocol framing mode: `"tcp"`, `"rtu_over_tcp"`, or `"rtu"`. |
| `interval` | string | `"100ms"` | Polling frequency (e.g. `"50ms"`, `"1s"`). Applies to streams. |
| `timeout` | string | `"1000ms"` | Socket read and write timeout. |
| `unitId` | int | `1` | Modbus slave/unit ID (`0..=255`). Alias: `slaveId`. |
| `autoOffset` | bool | `false` | When `true`, automatically converts 1-based PLC addresses (e.g., `40001` -> offset `0`, `30001` -> offset `0`) to 0-based wire addresses. |
| `registers` | JSON array / string | required | List of register definitions to poll. |

### Register Definition Schema

Each register object in `registers` supports:

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Key name in the emitted JSON record. |
| `address` | int | required | Register address (0-based wire index, or 1-based if `autoOffset` is true). |
| `function` | string | `"holding"` | Function code: `"holding"` (FC03), `"input"` (FC04), `"coil"` (FC01), or `"discreteInput"` (FC02). |
| `dataType` | string | `"uint16"` | Target type: `bool`, `uint16`, `int16`, `uint32`, `int32`, `uint64`, `int64`, `float32`, `float64`. |
| `byteOrder` | string | `"ABCD"` | Word/byte endianness layout for multi-register fields. |
| `scale` | float | `1.0` | Multiplier for linear scaling (`scaled = scale * raw + offset`). |
| `offset` | float | `0.0` | Additive offset for linear scaling. |

### Endianness Matrix (`byteOrder`)

For 32-bit and 64-bit types spanning multiple 16-bit registers:

| Byte Order | Name | Byte Sequence | Typical PLCs |
|---|---|---|---|
| `ABCD` | Big-Endian (Motorola) | `[B0, B1, B2, B3]` | Siemens S7, Schneider Electric |
| `CDAB` | Word-Swapped (Mid-Little Endian) | `[B2, B3, B0, B1]` | Modicon, Advantech, Moxa |
| `BADC` | Byte-Swapped (Mid-Big Endian) | `[B1, B0, B3, B2]` | Legacy RTUs |
| `DCBA` | Little-Endian (Intel) | `[B3, B2, B1, B0]` | PC-based soft PLCs, Beckhoff |

## Batch Register Reading

The engine groups register read requests to decrease network traffic:

1. It identifies registers with the same function code and unit ID.
2. It sorts registers by address.
3. It combines adjacent registers into one read request up to protocol limits (125 registers for holding/input registers, 2000 coils).
4. It extracts individual values from the single response.

## Float Validation

The connector validates floating-point numbers during register unpacking. It rejects `NaN` and infinite values to prevent SQL evaluation errors.

## Modbus Sink Action

rekuiper rules can write setpoints and control commands to Modbus devices:

```json
{
  "id": "pump_speed_control",
  "sql": "SELECT target_speed, pump_enable FROM sensor_stream WHERE temperature > 85.0",
  "actions": [
    {
      "modbus": {
        "endpoint": "192.168.1.50:502",
        "unitId": 1,
        "function": "holding",
        "registerAddress": 100,
        "field": "target_speed",
        "dataType": "uint16"
      }
    }
  ],
  "options": {
    "onError": "dlq",
    "dlq": {
      "action": {
        "mqtt": {
          "server": "tcp://127.0.0.1:1883",
          "topic": "rekuiper/dlq/modbus_errors"
        }
      }
    }
  }
}
```

### Supported Function Codes for Sinks

- `coil`: FC05 (Write Single Coil) or FC15 (Write Multiple Coils).
- `holding`: FC06 (Write Single Register) or FC16 (Write Multiple Registers).

### Dead-Letter Queue (DLQ) Integration

When a Modbus target returns exception codes:
- `0x01`: Illegal Function
- `0x02`: Illegal Data Address
- `0x03`: Illegal Data Value

The connector classifies the failure as a permanent delivery error. It sends the event to the dead-letter queue under `DlqStage::SinkDelivery`.
