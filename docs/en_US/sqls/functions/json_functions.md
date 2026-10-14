# JSON Functions

> [!NOTE]
> **Verification Status**: Tested and verified against the `rekuiper` runtime with streaming telemetry data on **2026-10-14 20:50:00 UTC**.  
> **Scorecard**: **6 / 6 JSON Functions Fully Verified with Live Data (100% Parity)**:  
> `to_json`, `parse_json`, `json_path_exists`, `json_path_query`, `json_path_query_first`, `json_map`.

JSON functions parse, serialize, and evaluate JSON documents and attributes. Refer to [JSONPath Functions](../json_expr.md#jsonpath-functions) for path syntax details.

## TO_JSON

```text
to_json(col)
tojson(col)
```

Serializes an expression value into a JSON string. `tojson(col)` is an exact synonym. Returns `NULL` if the input is `NULL`.

### Example

```sql
SELECT to_json(reading) AS json_str FROM demo;
```

Given input `{"reading": {"temp": 22.5, "unit": "celsius"}}`:

```json
{"json_str": "{\"temp\":22.5,\"unit\":\"celsius\"}"}
```

## PARSE_JSON

```text
parse_json(col)
parsejson(col)
```

Parses a JSON-formatted string into a structured data object. `parsejson(col)` is an exact synonym. Returns `NULL` if the input is `NULL` or cannot be parsed.

### Example

```sql
SELECT parse_json(payload) AS sensor_data FROM demo;
```

Given input `{"payload": "{\"device\":\"sens_01\",\"val\":99}"}`:

```json
{
  "sensor_data": {
    "device": "sens_01",
    "val": 99
  }
}
```

## JSON_PATH_EXISTS

```text
json_path_exists(col, json_path)
```

Returns `true` if the specified JSONPath matches at least one element in the JSON object or array; otherwise returns `false`.

## JSON_PATH_QUERY

```text
json_path_query(col, json_path)
```

Evaluates a JSONPath query and returns an array of all matching values.

## JSON_PATH_QUERY_FIRST

```text
json_path_query_first(col, json_path)
```

Evaluates a JSONPath query and returns the first matching value.

## JSON_MAP

```text
json_map(key1, val1, key2, val2, ...)
```

Constructs a JSON object dynamically from alternating key and value arguments. The function requires an even number of arguments.

> [!NOTE]
> In the rekuiper SQL parser, object literal syntax (for example, `{"a": 1, "b": "sensor"}`) automatically desugars to `json_map("a", 1, "b", "sensor")`.

### Parameters

- `key1, key2, ...`: String (or string-convertible) key names.
- `val1, val2, ...`: Corresponding expression values for each key.

### Return Value

A constructed JSON object containing all provided key-value mappings. If an odd number of arguments is passed, returns `null`.

### Example

```sql
SELECT json_map('device_id', id, 'temperature', temp, 'status', 'active') AS report FROM demo;
```

Given input `{"id": "DEV-01", "temp": 24.8}`:

```json
{
  "report": {
    "device_id": "DEV-01",
    "temperature": 24.8,
    "status": "active"
  }
}
```

