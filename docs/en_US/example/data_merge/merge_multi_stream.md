# Merge Data in Multiple Streams

## Problem Description

Telemetry frequently originates from multiple communication protocols or network domains. For example, in industrial IoT (IIoT), environmental sensors publish data through MQTT, while enterprise systems deliver production state through HTTP. In connected vehicle (IoV) systems, telematics and roadside units use separate channels. Downstream analytics require combining data from these disparate streams.

::: tip
To execute this scenario manually, refer to the [step-by-step guide](../howto.md).
:::

## Sample Input

This scenario uses two streams: one for temperature and one for humidity. Through rekuiper source abstractions, streams can ingest data from MQTT, HTTP, files, or other protocols.

**Data from stream1 (Temperature)**:

```json
{"device_id":"A","temperature":27.23,"ts":1681786070368}
{"device_id":"A","temperature":27.68,"ts":1681786070479}
{"device_id":"A","temperature":27.28,"ts":1681786070588}
{"device_id":"A","temperature":27.06,"ts":1681786070700}
{"device_id":"A","temperature":26.48,"ts":1681786070810}
{"device_id":"A","temperature":28.51,"ts":1681786070921}
{"device_id":"A","temperature":31.57,"ts":1681786071031}
{"device_id":"A","temperature":31.87,"ts":1681786071140}
{"device_id":"A","temperature":34.31,"ts":1681786071252}
{"device_id":"A","temperature":30.34,"ts":1681786071362}
```

**Data from stream2 (Humidity)**:

```json
{"device_id":"B","humidity":79.66,"ts":1681786070367}
{"device_id":"B","humidity":83.86,"ts":1681786070477}
{"device_id":"B","humidity":75.79,"ts":1681786070590}
{"device_id":"B","humidity":78.21,"ts":1681786070698}
{"device_id":"B","humidity":75.4,"ts":1681786070808}
{"device_id":"B","humidity":80.85,"ts":1681786070919}
{"device_id":"B","humidity":72.68,"ts":1681786071029}
{"device_id":"B","humidity":73.86,"ts":1681786071142}
{"device_id":"B","humidity":76.34,"ts":1681786071250}
{"device_id":"B","humidity":80.5,"ts":1681786071361}
```

## Desired Output

Combine data from both streams into unified events:

```json
{
  "temperature": 27.23,
  "humidity": 79.66
}
```

## Solutions

### 1. Merge Multiple Streams into One Stream through Memory Pipelines

You can consolidate multiple input streams into a single intermediate memory topic. Then you apply the merging rules described in [Merge Multiple Devices' Data in a Single Stream](./merge_single_stream.md).

1. Route `stream1` to a memory topic named `merged`:

   ```json
   {
     "id": "ruleMerge1",
     "name": "Route stream1 to memory topic",
     "sql": "SELECT * FROM stream1",
     "actions": [
       {
         "memory": {
           "topic": "merged",
           "sendSingle": true
         }
       }
     ]
   }
   ```

2. Route `stream2` to the same memory topic `merged`:

   ```json
   {
     "id": "ruleMerge2",
     "name": "Route stream2 to memory topic",
     "sql": "SELECT * FROM stream2",
     "actions": [
       {
         "memory": {
           "topic": "merged",
           "sendSingle": true
         }
       }
     ]
   }
   ```

3. Define a new stream that reads from the memory topic:

   ```sql
   CREATE STREAM mergedStream() WITH (TYPE="memory", FORMAT="json", DATASOURCE="merged");
   ```

   The stream `mergedStream` receives interleaved events from both sources:

   ```text
   {"device_id":"B","humidity":79.66,"ts":1681786070367}
   {"device_id":"A","temperature":27.23,"ts":1681786070368}
   {"device_id":"B","humidity":83.86,"ts":1681786070477}
   {"device_id":"A","temperature":27.68,"ts":1681786070479}
   {"device_id":"A","temperature":27.28,"ts":1681786070588}
   {"device_id":"B","humidity":75.79,"ts":1681786070590}
   {"device_id":"B","humidity":78.21,"ts":1681786070698}
   {"device_id":"A","temperature":27.06,"ts":1681786070700}
   ```

4. Apply single-stream merge rules to `mergedStream` to produce the final output.

### 2. Join Streams Directly with Windows

When records across streams share temporal or relational keys, join them directly by using windowed joins:

```json
{
  "id": "ruleJoin",
  "name": "Join stream1 and stream2 with tumbling window",
  "sql": "SELECT temperature, humidity FROM stream1 INNER JOIN stream2 ON stream1.ts - stream2.ts BETWEEN 0 AND 10 GROUP BY TumblingWindow(ms, 500)",
  "actions": [
    {
      "log": {}
    }
  ]
}
```

This rule divides the streams into 500-millisecond tumbling windows. Within each window, it matches temperature and humidity events whose timestamps differ by 10 milliseconds or less.

Example output:

```json
[{"humidity":79.66,"temperature":27.23},{"humidity":83.86,"temperature":27.68},{"humidity":78.21,"temperature":27.06},{"humidity":75.4,"temperature":26.48}]
[{"humidity":80.85,"temperature":28.51},{"humidity":72.68,"temperature":31.57},{"humidity":76.34,"temperature":34.31},{"humidity":80.5,"temperature":30.34}]
```

When streams share a common device key, use an equi-join:

```sql
SELECT temperature, humidity FROM stream1 INNER JOIN stream2 ON stream1.device_id = stream2.device_id GROUP BY TumblingWindow(ms, 500);
```

### Additional Merge Scenarios

For further discussion of custom merging patterns, visit the [GitHub Discussions](https://github.com/Indus-Stack/rekuiper/discussions) forum.
