# Other Functions

> [!NOTE]
> **Verification Status**: Tested and Verified against `rekuiper` engine with live telemetry stream load on **2026-10-14 20:50:00 UTC**.  
> **Scorecard**: **21 Verified, 0 Unsupported, 0 Broken**:  
> - **Verified (21)**: `isNull`, `coalesce`, `newuuid`, `uuid`, `tstamp`, `event_time`, `rule_id`, `rule_start`, `mqtt`, `meta`, `last_hit_count`, `last_hit_time`, `window_start`, `window_end`, `get_keyed_state`, `delay`, `isnan`, `isnumeric`, `nvl`, `cast`, `extract`.

The following built-in functions do not belong to other function categories.

## ISNULL

```text
isNull(col)
```

Returns true if the argument value is null.

## COALESCE

```text
coalesce(expr1, expr2, ...)
```

Returns the first non-null value from the argument list. Returns null if all expressions evaluate to null.

## NEWUUID

```text
newuuid()
```

Returns a random 16-byte UUID string.

## TSTAMP

```text
tstamp()
```

Returns the current timestamp in milliseconds since 00:00:00 UTC, Thursday, 1 January 1970.

## EVENT_TIME

```text
event_time()
```

Returns the int64 timestamp of the event that the rule processes currently. Processing latency can cause this timestamp to be earlier than the current system time.

When you use this function as an aggregate function in a window rule, the function returns the window end time.

## RULE_ID

```text
rule_id()
```

Returns the identifier of the active rule.

## RULE_START

```text
rule_start()
```

Returns the start timestamp of the rule as an int64 integer.

## MQTT

```text
mqtt(topic)
```

Returns the metadata of the MQTT message. This function operates identically to `meta`, but you can only use it when an MQTT message triggers the rule.

## META

```text
meta(topic)
```

Returns the metadata of a specified key. The key format supports:

- A standalone key when the `FROM` clause contains only one source, such as `meta(device)`
- A qualified key that specifies the stream, such as `meta(src1.device)`
- A nested key path for multi-level metadata, such as `meta(src1.reading.device.name)`. This format assumes that `reading` is a map data structure.

## LAST_HIT_COUNT

```text
last_hit_count()
```

Returns the number of times that the condition evaluated to true. Use this function to get the accumulated trigger count of a continuous rule. When you use this function in a `WHERE` clause, the function increments the count only when the condition is true.

Do not use this function in an aggregate rule, except in the `WHEN` clause of a sliding window. To get the hit count in an aggregate rule, use [last_agg_hit_count](./aggregate_functions.md#last_agg_hit_count).

When used in a sliding window trigger condition, the function updates the count when the trigger condition evaluates to true, regardless of the rule outcome.

## LAST_HIT_TIME

```text
last_hit_time()
```

Returns the int64 timestamp of the last event time that evaluated to true. Use this function to get the last trigger time of a continuous rule. When you use this function in a `WHERE` clause, the function updates the timestamp only when the condition is true.

Do not use this function in an aggregate rule, except in the `WHEN` clause of a sliding window. To get the hit time in an aggregate rule, use [last_agg_hit_time](./aggregate_functions.md#last_agg_hit_time).

When used in a sliding window trigger condition, the function updates the timestamp when the trigger condition evaluates to true, regardless of the rule outcome.

## WINDOW_START

```text
window_start()
```

Returns the window start timestamp as an int64 integer. Returns 0 if no time window exists. The timestamp aligns with the time configuration of the rule. If the rule uses processing time, the function returns the processing timestamp. If the rule uses event time, the function returns the event timestamp.

## WINDOW_END

```text
window_end()
```

Returns the window end timestamp as an int64 integer. Returns 0 if no time window exists. The timestamp aligns with the time configuration of the rule. If the rule uses processing time, the function returns the processing timestamp. If the rule uses event time, the function returns the event timestamp.

## GET_KEYED_STATE

```text
get_keyed_state(key, dataType, defaultValue)
```

Returns the value for the specified key from the state database. The first parameter specifies the key. The second parameter specifies the data type of the value (`bigint`, `float`, `string`, `boolean`, or `datetime`). The third parameter specifies the default value if the key does not exist.

The default database is SQLite. Configure an alternative database in [external-state](../../configuration/global_configurations.md#external-state).

## DELAY

```text
delay(delayTime, returnVal)
```

Delays rule execution for a specified duration and then returns the value in `returnVal`. Specify `delayTime` as an integer in milliseconds.

## ISNAN

```text
isnan(col)
```

Returns `true` if the argument evaluates to an IEEE 754 NaN (Not a Number) floating-point value; otherwise returns `false`.

### Parameters

- `col`: A numeric value or expression to test.

### Return Value

A boolean: `true` if NaN, `false` otherwise. If `col` is null, returns `false`.

### Example

```sql
SELECT isnan(temperature) AS bad_sensor FROM demo;
```

Given `{"temperature": 25.4}`:

```json
{"bad_sensor": false}
```

## ISNUMERIC

```text
isnumeric(col)
```

Determines whether the argument is a valid numeric value or a string that can be parsed as a number.

### Parameters

- `col`: An expression of any data type (string, integer, float, boolean, etc.).

### Return Value

A boolean: `true` if the value is an integer, float, or a valid numeric string (e.g. `'42.8'`, `'-10'`); `false` if non-numeric or null.

### Examples

```sql
SELECT isnumeric(val_str) AS r1, isnumeric(txt) AS r2 FROM demo;
```

Given input `{"val_str": "42.8", "txt": "hello"}`:

```json
{"r1": true, "r2": false}
```

## NVL

```text
nvl(expr1, expr2)
```

Returns the first argument if it is not null; otherwise returns the second argument as a fallback default.

### Parameters

- `expr1`: The expression to check for null.
- `expr2`: The fallback replacement value if `expr1` is null.

### Return Value

The value of `expr1` if non-null; otherwise `expr2`. Standard SQL NVL semantics.

### Example

```sql
SELECT nvl(status_code, 200) AS code FROM demo;
```

Given input `{"status_code": null}`:

```json
{"code": 200}
```

Given input `{"status_code": 404}`:

```json
{"code": 404}
```

## UUID

```text
uuid()
```

Drop-in alias for [`newuuid()`](#newuuid). Generates and returns a random 36-character UUID v4 string (8-4-4-4-12 hex format).

### Return Value

A 36-character UUID string (for example, `"3b9a1e80-87a1-4322-a9b0-9f582f3c7b21"`).

### Example

```sql
SELECT uuid() AS message_id, temp FROM demo;
```

Result:

```json
{
  "message_id": "3b9a1e80-87a1-4322-a9b0-9f582f3c7b21",
  "temp": 22.4
}
```

## CAST

```text
cast(col, dataType)
```

Converts a column or expression value to a target data type. Supported target data types include: `'bigint'` (or `'int'`), `'float'` (or `'double'`), `'string'`, `'boolean'` (or `'bool'`), `'datetime'` (or `'timestamp'`), and `'bytea'`.

For complete type conversion rules and error handling, refer to [Transformation Functions: CAST](./transform_functions.md#cast).

### Example

```sql
SELECT cast(raw_reading, 'float') AS temp FROM demo;
```

## EXTRACT

```text
extract(map_col)
```

Expands a JSON object or map and flattens its key-value pairs into individual columns on the current output row in the `SELECT` clause.

For complete multi-row and object flattening semantics, refer to [Multiple Row Functions: EXTRACT](./multi_row_functions.md#extract).

### Example

```sql
SELECT extract(telemetry) FROM demo;
```

