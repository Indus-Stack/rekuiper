# Merge Multiple Devices' Data in a Single Stream

## Problem Description

In IoT environments, data collection software often multiplexes telemetry from many sensors into a single stream. Because each sensor has different sampling rates and response latencies, the stream contains interleaved, fragmented events where each event contains data from only one sensor. For example, sensor A samples temperature every second, sensor B samples humidity every 5 seconds, and sensor C samples pressure every 10 seconds. Downstream applications require complete, unified records that contain correlated sensor measurements from the same equipment.

::: tip
To execute this scenario manually, refer to the [step-by-step guide](../howto.md).
:::

## Sample Input

The input stream contains interleaved temperature and humidity records:

```text
{"device_id":"B","humidity":79.66,"ts":1681786070367}
{"device_id":"A","temperature":27.23,"ts":1681786070368}
{"device_id":"B","humidity":83.86,"ts":1681786070477}
{"device_id":"A","temperature":27.68,"ts":1681786070479}
{"device_id":"A","temperature":27.28,"ts":1681786070588}
{"device_id":"B","humidity":75.79,"ts":1681786070590}
{"device_id":"B","humidity":78.21,"ts":1681786070698}
{"device_id":"A","temperature":27.06,"ts":1681786070700}
{"device_id":"B","humidity":75.4,"ts":1681786070808}
{"device_id":"A","temperature":26.48,"ts":1681786070810}
{"device_id":"B","humidity":80.85,"ts":1681786070919}
{"device_id":"A","temperature":28.51,"ts":1681786070921}
{"device_id":"B","humidity":72.68,"ts":1681786071029}
{"device_id":"A","temperature":31.57,"ts":1681786071031}
{"device_id":"A","temperature":31.87,"ts":1681786071140}
{"device_id":"B","humidity":73.86,"ts":1681786071142}
{"device_id":"B","humidity":76.34,"ts":1681786071250}
{"device_id":"A","temperature":34.31,"ts":1681786071252}
{"device_id":"B","humidity":80.5,"ts":1681786071361}
{"device_id":"A","temperature":30.34,"ts":1681786071362}
```

## Desired Output

Combine data from related sensors into a unified event:

```json
{
  "temperature": 27.23,
  "humidity": 79.66,
  "ts": 1681786070368
}
```

You can configure different merge behaviors, output frequencies, and formats with SQL rules.

## Solutions

### 1. Emit Output for Each Incoming Event

This algorithm combines the latest known values of temperature and humidity whenever any event arrives. The output frequency matches the input stream frequency:

```sql
SELECT latest(temperature, 0) AS temperature, latest(humidity, 0) AS humidity, ts FROM demoStream;
```

The function `latest(temperature, 0)` returns the temperature value from the current event. If the event does not contain temperature, the function returns the most recent temperature value. If no temperature was previously received, it returns 0.

Example output:

```text
{"humidity":79.66,"temperature":0,"ts":1681786070367}
{"humidity":79.66,"temperature":27.23,"ts":1681786070368}
{"humidity":83.86,"temperature":27.23,"ts":1681786070477}
{"humidity":83.86,"temperature":27.68,"ts":1681786070479}
{"humidity":83.86,"temperature":27.28,"ts":1681786070588}
{"humidity":75.79,"temperature":27.28,"ts":1681786070590}
{"humidity":78.21,"temperature":27.28,"ts":1681786070698}
{"humidity":78.21,"temperature":27.06,"ts":1681786070700}
{"humidity":75.4,"temperature":27.06,"ts":1681786070808}
{"humidity":75.4,"temperature":26.48,"ts":1681786070810}
{"humidity":80.85,"temperature":26.48,"ts":1681786070919}
{"humidity":80.85,"temperature":28.51,"ts":1681786070921}
{"humidity":72.68,"temperature":28.51,"ts":1681786071029}
{"humidity":72.68,"temperature":31.57,"ts":1681786071031}
{"humidity":72.68,"temperature":31.87,"ts":1681786071140}
{"humidity":73.86,"temperature":31.87,"ts":1681786071142}
{"humidity":76.34,"temperature":31.87,"ts":1681786071250}
{"humidity":76.34,"temperature":34.31,"ts":1681786071252}
{"humidity":80.5,"temperature":34.31,"ts":1681786071361}
{"humidity":80.5,"temperature":30.34,"ts":1681786071362}
```

### 2. Emit Output Driven by a Primary Metric

This algorithm uses temperature as the primary metric. Whenever a temperature event arrives, the rule combines that value with the latest humidity value and emits the record. The output frequency matches the sampling frequency of temperature:

```sql
SELECT temperature, latest(humidity, 0) AS humidity, ts FROM demoStream WHERE isNull(temperature) = false;
```

The clause `WHERE isNull(temperature) = false` ignores events that do not contain temperature values.

Example output:

```text
{"humidity":79.66,"temperature":27.23,"ts":1681786070368}
{"humidity":83.86,"temperature":27.68,"ts":1681786070479}
{"humidity":83.86,"temperature":27.28,"ts":1681786070588}
{"humidity":78.21,"temperature":27.06,"ts":1681786070700}
{"humidity":75.4,"temperature":26.48,"ts":1681786070810}
{"humidity":80.85,"temperature":28.51,"ts":1681786070921}
{"humidity":72.68,"temperature":31.57,"ts":1681786071031}
{"humidity":72.68,"temperature":31.87,"ts":1681786071140}
{"humidity":76.34,"temperature":34.31,"ts":1681786071252}
{"humidity":80.5,"temperature":30.34,"ts":1681786071362}
```

### 3. Merge Records with Close Timestamps

When sensors sample data at roughly the same time, the interval between correlated readings is short:

```sql
SELECT latest(temperature, 0) AS temperature, latest(humidity, 0) AS humidity, ts FROM demoStream WHERE ts - lag(ts) < 10;
```

The condition `WHERE ts - lag(ts) < 10` filters out isolated readings and emits an event only when two consecutive readings arrive within 10 milliseconds of each other.

Example output:

```text
{"humidity":79.66,"temperature":27.23,"ts":1681786070368}
{"humidity":83.86,"temperature":27.68,"ts":1681786070479}
{"humidity":75.79,"temperature":27.28,"ts":1681786070590}
{"humidity":78.21,"temperature":27.06,"ts":1681786070700}
{"humidity":75.4,"temperature":26.48,"ts":1681786070810}
{"humidity":80.85,"temperature":28.51,"ts":1681786070921}
{"humidity":72.68,"temperature":31.57,"ts":1681786071031}
{"humidity":73.86,"temperature":31.87,"ts":1681786071142}
{"humidity":76.34,"temperature":34.31,"ts":1681786071252}
{"humidity":80.5,"temperature":30.34,"ts":1681786071362}
```

### 4. Down-Sampling by Time Window

This algorithm uses a fixed tumbling window to control the output rate and aggregates all readings collected within that duration:

```sql
SELECT merge_agg(*) AS result FROM demoStream GROUP BY TUMBLINGWINDOW(ms, 500);
```

The function `merge_agg` merges all fields received during the 500-millisecond window into one object.

Example output:

```json
{
  "result": {
    "device_id": "A",
    "humidity": 83.86,
    "temperature": 27.68,
    "ts": 1681786070479
  }
}
{
  "result": {
    "device_id": "A",
    "humidity": 80.85,
    "temperature": 28.51,
    "ts": 1681786070921
  }
}
```

### 5. Fixed-Interval Average Aggregation

When downstream applications need metric trends rather than individual raw samples, use windowed aggregation functions:

```sql
SELECT avg(temperature) AS temperature, avg(humidity) AS humidity, window_end() AS ts FROM demoStream GROUP BY TUMBLINGWINDOW(ms, 500);
```

The tumbling window groups readings every 500 milliseconds and calculates the arithmetic mean for each metric.

Example output:

```text
{"humidity":81.75999999999999,"temperature":27.455,"ts":1681786070500}
{"humidity":77.5625,"temperature":27.332500000000003,"ts":1681786071000}
```

The time window aligns with natural wall-clock intervals (500 ms, 1000 ms, 1500 ms).

### Additional Merge Scenarios

For further discussion of custom merging patterns, visit the [GitHub Discussions](https://github.com/ankur-paan/rekuiper/discussions) forum.
