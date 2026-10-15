# Extract EdgeX Message Bus Metadata Using the Meta Function

When EdgeX Foundry publishes device readings to the message bus, events include metadata such as creation timestamps, device identifiers, and correlation tags. This guide describes how to extract event and reading metadata using the rekuiper `meta()` function.

## EdgeX Message Bus Data Model

EdgeX Foundry packages device data into hierarchical `Event` structures containing metadata and an array of `Reading` objects:

- **Event Structure**:
  - `ID`: Event identifier.
  - `DeviceName`: Name of the originating device.
  - `ProfileName`: Associated device profile name.
  - `SourceName`: Source command or event name.
  - `Origin`: Nanosecond timestamp recorded at creation.
  - `Tags`: Key-value metadata tags.
  - `Readings`: Array of sensor values:
    - `Id`: Reading identifier.
    - `Origin`: Reading timestamp.
    - `DeviceName`: Sensor device name.
    - `ResourceName`: Sensor metric name (for example: `temperature`).
    - `ProfileName`: Metric profile name.
    - `ValueType`: Data type identifier.
    - `Value`: Sensor reading value.

### Schema Changes in EdgeX v2

When migrating from EdgeX v1:
1. EdgeX v2 removes the `Pushed`, `Created`, and `Modified` metadata fields.
2. The `Device` field is renamed to `DeviceName` in both events and readings.
3. The `Name` field of readings is renamed to `ResourceName`.

## EdgeX Metadata Resolution in rekuiper

rekuiper ingests EdgeX bus events and maps top-level sensor values directly to stream columns. The engine preserves event and reading metadata inside an internal tuple accessible through `meta()`.

1. Define an EdgeX stream named `events`:

```sql
CREATE STREAM events () WITH (TYPE="edgex", CONF_KEY="default")
```

2. When devices publish events to the message bus:
   - Metric values (`temperature`, `humidity`) become queryable columns.
   - Event and reading headers become metadata attributes.

```json
{
  "apiVersion": "v2",
  "id": "e4b6c310-9114-41d3-bb74-0f73b88b0a1d",
  "deviceName": "demo2",
  "profileName": "Random-Float-Device",
  "sourceName": "Float",
  "origin": 1622449767000000000,
  "readings": [
    {
      "id": "5f130b0b-2da7-4e78-9b88-5183db2a450a",
      "origin": 1622449767000000000,
      "deviceName": "demo2",
      "resourceName": "temperature",
      "profileName": "Random-Float-Device",
      "valueType": "Float64",
      "value": 24.5
    },
    {
      "id": "78c92a11-8be2-4751-bb33-728b9394fae1",
      "origin": 1622449767000000000,
      "deviceName": "demo2",
      "resourceName": "humidity",
      "profileName": "Random-Float-Device",
      "valueType": "Float64",
      "value": 65.2
    }
  ]
}
```

3. Query columns and extract metadata attributes in SQL:

```sql
SELECT
  temperature,
  humidity,
  meta(deviceName) AS device,
  meta(origin) AS event_origin,
  meta(temperature->id) AS temp_reading_id
FROM events
```

### Extracting Event-Level Metadata

Pass the field name directly into `meta()`:

- `meta(deviceName)`: Returns `DeviceName` from the `Event` header.
- `meta(origin)`: Returns `Origin` creation timestamp from the `Event` header.

### Extracting Reading-Level Metadata

Extract reading metadata using the arrow syntax: `reading_name -> property`:

- `meta(temperature -> origin)`: Returns `Origin` for the `temperature` reading.
- `meta(humidity -> id)`: Returns reading `Id` for the `humidity` reading.

### Assigning Column Aliases in SELECT Clauses

When extracting multiple metadata fields in the `SELECT` clause, assign explicit column aliases using `AS` to prevent field name collisions:

```sql
SELECT
  temperature,
  humidity,
  meta(id) AS eid,
  meta(origin) AS eo,
  meta(temperature->id) AS tid,
  meta(temperature->origin) AS torigin,
  meta(Humidity->deviceName) AS hdevice,
  meta(Humidity->profileName) AS hprofile
FROM demo
WHERE meta(deviceName) = "demo2"
```

## Available Metadata Keys

### Event Metadata Fields

`id`, `deviceName`, `profileName`, `sourceName`, `origin`, `tags`, `correlationid`.

### Reading Metadata Fields

`id`, `deviceName`, `profileName`, `origin`, `valueType`.
