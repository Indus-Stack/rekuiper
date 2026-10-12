# Data Types

In rekuiper, every column and expression has an associated data type that restricts valid values and available operations.

## Supported Data Types

rekuiper supports the following data types:

| Number | Data Type | Description |
|---|---|---|
| 1 | `bigint` | 64-bit signed integer. |
| 2 | `float` | 64-bit floating-point number. |
| 3 | `string` | Unicode text characters. |
| 4 | `datetime` | Date and time timestamp value. |
| 5 | `boolean` | Boolean value (`true` or `false`). |
| 6 | `bytea` | Binary byte array. In JSON streams, represent `bytea` fields as Base64-encoded strings. |
| 7 | `array` | Ordered list of scalar elements or nested struct values. |
| 8 | `struct` | Complex object consisting of named key-value attributes. |

## Compatibility for Comparison and Calculation

Binary operations evaluate expressions in `SELECT` and `WHERE` clauses. If an expression combines incompatible operand types, rekuiper generates a runtime error and delivers the error message to configured sinks.

Arrays and structs cannot participate directly in arithmetic or comparison operations. The following compatibility matrix specifies permitted operations between scalar types:

| Left Operand | `bigint` | `float` | `string` | `datetime` | `boolean` |
|---|---|---|---|---|---|
| `bigint` | Yes | Yes | No | No | No |
| `float` | Yes | Yes | No | No | No |
| `string` | No | No | Yes | No | No |
| `datetime` | Yes | Yes | Yes (if formatted correctly) | Yes | No |
| `boolean` | No | No | No | No | Yes |

The default parsing format for datetime strings is `"2006-01-02T15:04:05.000Z07:00"`.

### Null Value Handling

When an expression encounters a `nil` (null) operand:

1. Any comparison against `nil` evaluates to `false`.
2. Any arithmetic calculation with `nil` returns `nil`.

## Type Conversions

Use the built-in `cast(col, targetType)` function to convert values between data types at runtime. Refer to [transformation functions](./functions/transform_functions.md) for usage details.

## Default Column Values

Stream columns in `CREATE STREAM` statements can specify default fallback values using the `DEFAULT` clause:

```sql
CREATE STREAM telemetry (
    device_id string,
    temperature float DEFAULT 20.0,
    status string DEFAULT 'idle',
    retry_count bigint DEFAULT 0,
    active boolean DEFAULT true
) WITH (FORMAT = "json", DATASOURCE = "devices/telemetry");
```

When an incoming tuple is missing a declared column or the column value is explicitly `null`, the stream schema preprocessor automatically populates the configured `DEFAULT` value and coerces it into the declared data type.

