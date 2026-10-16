# Connected Vehicles and EV Charging Stream Processing

Connected vehicles and Electric Vehicle (EV) charging networks generate large streams of high-frequency telemetry. Modern vehicles emit thousands of data points per second from controller area network (CAN) buses, battery management systems (BMS), motor controllers, and GPS units. Commercial EV charging stations monitor voltage, current, temperature, and session state across multiple charging connectors.

rekuiper deploys directly onto automotive telematics control units (T-Box), in-vehicle processors, and EV charging station controllers to process telemetry locally with sub-millisecond latency.

## Edge Architecture for Connected Vehicles

In automotive and charging architectures, rekuiper operates as a local streaming coprocessor:

![Connected Vehicles and EV Charging Architecture](/diagrams/iov_architecture.svg)

## Technical Advantages on Vehicle Hardware

Automotive telematics control units and charging station controllers operate under strict resource limits:

- **Sub-5MB Memory Footprint**: rekuiper operates within 4.4 to 6.4 MiB of RAM, running alongside embedded operating system services without memory contention.
- **Deterministic Execution Without Garbage Collection**: Vehicle control systems cannot tolerate garbage collection pauses that drop CAN bus frames. rekuiper provides deterministic stream processing.
- **High Single-Core Throughput**: In audited benchmarks, rekuiper achieves 126,000 msg/s on EV charger session workloads and 150,000 msg/s on telematics filter queries on a single CPU core.

## Primary Use Cases and SQL Rules

### 1. EV Charger Session Tracking with SESSIONWINDOW

EV charging management systems must track charging sessions dynamically without polling database tables. Using SQL session windows, rekuiper groups charging telemetry into sessions separated by inactivity intervals:

```sql
SELECT
  chargerId,
  connectorId,
  COUNT(*) AS sampleCount,
  AVG(voltage) AS avgVoltage,
  MAX(current) AS maxCurrent,
  SUM(powerKw * 0.00277) AS totalEnergyKwh
FROM
  charger_stream
GROUP BY
  chargerId,
  connectorId,
  SESSIONWINDOW(ss, 30)
```

When a vehicle unplugs and no readings arrive for 30 seconds, rekuiper closes the session window, computes total delivered energy, and publishes the session receipt to the billing service.

### 2. Adaptive Downsampling Using CHANGED_COLS

Transmitting raw sensor telemetry over cellular networks incurs high carrier data costs. Many signals (such as battery voltage during highway cruise or ambient cabin temperature) remain stable over long intervals.

rekuiper filters out redundant readings, emitting updates only when values change:

```sql
SELECT
  deviceId,
  CHANGED_COLS("", true, batteryVoltage, cabinTemp, tirePressure)
FROM
  telematics_stream
```

This reduces cellular data transmission by 60% to 80% while preserving full fidelity on abnormal readings.

### 3. Immediate Battery Safety Alerts

Detect abnormal battery cell temperatures or over-voltage conditions locally on the vehicle, triggering cooling loops or alarms without waiting for cloud roundtrips:

```sql
SELECT
  vehicleId,
  maxCellTemp,
  minCellTemp,
  (maxCellTemp - minCellTemp) AS cellDelta
FROM
  bms_stream
WHERE
  maxCellTemp > 55.0 OR (maxCellTemp - minCellTemp) > 8.0
```

The rule action dispatches an alert directly to the in-vehicle IPC socket and sends an urgent notification over MQTT:

```json
{
  "id": "bms_thermal_alert",
  "sql": "SELECT vehicleId, maxCellTemp, (maxCellTemp - minCellTemp) AS cellDelta FROM bms_stream WHERE maxCellTemp > 55.0 OR (maxCellTemp - minCellTemp) > 8.0",
  "actions": [
    {
      "mqtt": {
        "server": "tcp://127.0.0.1:1883",
        "topic": "vehicle/alerts/critical"
      }
    },
    {
      "file": {
        "path": "/var/log/vehicle/safety_events.log"
      }
    }
  ]
}
```

### 4. Geo-Fencing and Speed Boundary Checks

Correlate GPS coordinates and vehicle velocity in real time:

```sql
SELECT
  vehicleId,
  speed,
  latitude,
  longitude
FROM
  gps_stream
WHERE
  speed > 110.0
```

## Summary

Combining a sub-5MB memory footprint, zero garbage collection pauses, and standard SQL windowing, rekuiper provides a stream runtime optimized for automotive telematics and EV charging infrastructure.
