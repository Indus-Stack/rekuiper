# Transform Functions

> [!NOTE]
> **Verification Status**: Tested and Verified against `rekuiper` engine with live telemetry stream load on **2026-10-14 20:50:00 UTC**.  
> **Scorecard**: **13 Verified, 0 Unsupported, 0 Broken**:  
> - **Verified (13)**: `cast`, `convert_tz`, `to_seconds`, `encode` (base64), `base64_encode`, `decode` (base64), `base64_decode`, `compress` (zlib/gzip/flate/zstd), `decompress` (zlib/gzip/flate/zstd), `trunc`, `chr`, `hex2dec`, `dec2hex`.

Transform functions convert data types, encode payloads, and apply compression algorithms.

## CAST

```text
cast(col, dataType)
```

Converts a value to the specified target data type. Supported target types include: `bigint`, `float`, `string`, `boolean`, `bytea`, and `datetime`.

### Cast to datetime

When casting a value to `datetime`, the engine applies the following conversion rules:

1. If the input is already a `datetime` value, the function returns the value directly.
2. If the input is `bigint` or `float`, the function interprets the number as milliseconds since January 1, 1970 00:00:00 UTC.
3. If the input is `string`, the function parses the format into a `datetime` value. For supported time formats, refer to [TimeFormats](https://github.com/jinzhu/now/blob/f067b166b35a996b9ff5a0f610225e1458f23adc/main.go#L17-L27).
4. Conversions from other types are not supported.

## CONVERT_TZ

```text
convert_tz(col, "Asia/Shanghai")
```

Converts a timestamp to the specified target time zone. Specify time zones using the [IANA Time Zone Database](https://www.iana.org/time-zones) format. The default time zone is `UTC`. Specify `Local` to apply the host system time zone.

> [!NOTE]
> In Alpine Linux containers, you must install the `tzdata` package (for example, `apk add tzdata`) to provide time zone database files.

## TO_SECONDS

```text
to_seconds(col)
```

Converts `col` to a datetime value and returns Unix epoch time as the number of elapsed seconds since January 1, 1970 00:00:00 UTC.

## ENCODE

```text
encode(col, encodeType)
base64_encode(col)
```

Encodes binary or string payload data into a string representation using the specified encoding scheme. Currently, the engine supports `"base64"`.

`base64_encode(col)` is a convenient single-argument function equivalent to `encode(col, "base64")`.

### Example

```sql
SELECT base64_encode(payload) AS b64, encode(payload, "base64") AS b64_alt FROM demo;
```

Given input `{"payload": "Hello, Rekuiper!"}`:

```json
{
  "b64": "SGVsbG8sIFJla3VpcGVyIQ==",
  "b64_alt": "SGVsbG8sIFJla3VpcGVyIQ=="
}
```

## DECODE

```text
decode(col, encodeType)
base64_decode(col)
```

Decodes an encoded string into its original representation using the specified decoding scheme. Currently, the engine supports `"base64"`.

`base64_decode(col)` is a convenient single-argument function equivalent to `decode(col, "base64")`.

### Example

```sql
SELECT base64_decode(b64) AS original_text FROM demo;
```

Given input `{"b64": "SGVsbG8sIFJla3VpcGVyIQ=="}`:

```json
{
  "original_text": "Hello, Rekuiper!"
}
```

> [!NOTE]
> **Compatibility & Safety Note: Non-Crashing Null Safety in DECODE**
> In legacy eKuiper, unhandled decoding errors or unmapped targets in value decoders could panic at runtime when no default fallback was provided. `rekuiper` safely returns `null` on decode failures without panicking.
> 
> **Why we chose this difference**: Returning `null` follows SQL standards for conversion errors and prevents corrupt or malformed incoming sensor records from aborting active stream processing.

## COMPRESS

```text
compress(input, method)
```

Compresses an input string or binary payload using the specified algorithm. Supported methods include `'zlib'`, `'gzip'`, `'flate'`, and `'zstd'`.

## DECOMPRESS

```text
decompress(input, method)
```

Decompresses compressed payload data using the specified algorithm. Supported methods include `'zlib'`, `'gzip'`, `'flate'`, and `'zstd'`.

## TRUNC

```text
trunc(dec, int)
```

Truncates the decimal number `dec` to the number of decimal places specified by `int`. If the second argument is less than 0, the function uses 0. If greater than 34, the function uses 34. The function removes trailing zeros from the result.

## CHR

```text
chr(col)
```

Returns the ASCII character corresponding to the specified integer code.

> [!NOTE]
> **Compatibility & Safety Note: Panic-Free Codepoint Validation in CHR**
> Legacy eKuiper crashes on negative or out-of-range integer values passed to `chr()`. `rekuiper` validates codepoint bounds (0 to 127 for ASCII, valid Unicode scalar values) and safely returns `null` for invalid or negative inputs without crashing.
> 
> **Why we chose this difference**: Preventing server process crashes on abnormal sensor numbers is essential for edge gateways operating autonomously in unattended environments.

## HEX2DEC

```text
hex2dec(col)
```

Converts a hexadecimal string into its decimal integer value. The argument must be a string. Values such as `"0x10"` or `"10"` convert to integer `16`.

## DEC2HEX

```text
dec2hex(col)
```

Converts a decimal integer into a hexadecimal string with a `"0x"` prefix. For example, integer `16` converts to `"0x10"`.
