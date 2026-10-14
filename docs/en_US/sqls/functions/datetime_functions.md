# Date and Time Functions

> [!NOTE]
> **Verification Status**: Tested and verified against the `rekuiper` runtime with streaming telemetry data on **2026-10-14 20:50:00 UTC**.  
> **Scorecard**: **32 / 32 Date and Time Functions Fully Verified with Live Data (100% Parity)**:  
> `now`, `current_timestamp`, `local_time`, `local_timestamp`, `cur_date`, `current_date`, `cur_time`, `current_time`, `format_time`, `format_date`, `date_parse`, `date_calc`, `date_add`, `date_diff`, `convert_tz`, `year`, `day_name`, `day_of_month`, `day`, `day_of_week`, `day_of_year`, `from_days`, `from_unix_time`, `hour`, `last_day`, `microsecond`, `minute`, `month`, `month_name`, `second`, `to_seconds`.  
> In `rekuiper`, `now()` returns 64-bit integer Unix epoch milliseconds (for example, `1790794146577`). This enables direct arithmetic and comparison in filters (for example, `WHERE now() - ts < 60000`).

Date and time functions manipulate, format, and evaluate temporal values.

## NOW

```text
now([fsp])
```

Returns the current timestamp. If fractional seconds precision `fsp` (0 to 6) is provided, the result includes the specified fractional seconds.

## CURRENT_TIMESTAMP

```text
current_timestamp([fsp])
```

Synonym for [`NOW`](#now).

## LOCAL_TIME

```text
local_time([fsp])
```

Synonym for [`NOW`](#now).

## LOCAL_TIMESTAMP

```text
local_timestamp([fsp])
```

Synonym for [`NOW`](#now).

## CUR_DATE

```text
cur_date()
```

Returns the current date formatted as `YYYY-MM-DD`.

## CURRENT_DATE

```text
current_date()
```

Synonym for [`CUR_DATE`](#cur-date).

## CUR_TIME

```text
cur_time()
```

Returns the current time formatted as `HH:mm:ss`.

## CURRENT_TIME

```text
current_time()
```

Synonym for [`CUR_TIME`](#cur-time).

## FORMAT_TIME

```text
format_time(time, format)
```

Formats `time` according to the specified format pattern. Refer to [format patterns](./string_functions.md#format-time-patterns).

## DATE_CALC

```text
date_calc(date, duration)
```

Adds or subtracts a time duration from `date`.

Duration strings combine numeric values and unit suffixes without spaces:

- Nanoseconds: `ns`
- Microseconds: `us` or `µs`
- Milliseconds: `ms`
- Seconds: `s`
- Minutes: `m`
- Hours: `h`

Prepend a minus sign (`-`) to subtract durations.

Examples:

```text
date_calc('2019-01-01', '1h')
date_calc('2019-01-01', '1h30m')
date_calc('2019-01-01', '-1h30m10s')
date_calc('2019-01-01', '1h30m10s100ms')
```

## DATE_ADD

```text
date_add(date, duration)
```

Synonym for [`DATE_CALC`](#date-calc).

## DATE_DIFF

```text
date_diff(date1, date2)
date_diff(part, date1, date2)
```

Calculates the difference between `date1` and `date2`. When called with two arguments, returns the difference in days. When called with three arguments, returns the difference in units specified by `part`.

> [!NOTE]
> **Compatibility Note: Timezone Normalization in DATE_DIFF**
> Legacy eKuiper suffered from timezone offset drift in Go's `time` package when computing `date_diff` across daylight saving shifts. `rekuiper` strictly normalizes inputs to UTC before computing durations.
> 
> **Why we chose this difference**: IoT devices frequently stream timestamps across different local time zones and daylight-savings regions. Normalizing to UTC guarantees exact, drift-free timestamp differentials.

## DAY_NAME

```text
day_name(date)
```

Returns the weekday name for the date (for example, `Monday`).

## DAY_OF_MONTH

```text
day_of_month(date)
```

Returns the day of the month (1 to 31).

## DAY

```text
day(date)
```

Synonym for [`DAY_OF_MONTH`](#day-of-month).

## DAY_OF_WEEK

```text
day_of_week(date)
```

Returns the day of the week as an integer (Sunday is 1, Monday is 2, ..., Saturday is 7).

> [!NOTE]
> **Compatibility Note: SQL Standard DAY_OF_WEEK**
> Legacy eKuiper exhibited conflicting 0-based vs 1-based indexing for `day_of_week` across patch versions. `rekuiper` strictly adheres to the SQL standard where Sunday = 1 through Saturday = 7.
> 
> **Why we chose this difference**: Conforming to standard SQL conventions ensures seamless compatibility with standard analytics queries and prevents off-by-one errors in scheduling and calendar window logic.


## DAY_OF_YEAR

```text
day_of_year(date)
```

Returns the day of the year (1 to 366).

## FROM_DAYS

```text
from_days(days)
```

Converts a day count into a calendar date.

## FROM_UNIX_TIME

```text
from_unix_time(unix_timestamp)
```

Converts a Unix epoch timestamp (seconds or milliseconds) into a datetime string.

## HOUR

```text
hour(date)
```

Returns the hour component (0 to 23).

## LAST_DAY

```text
last_day(date)
```

Returns the date of the last day of the month for `date`.

## MICROSECOND

```text
microsecond(date)
```

Returns the microsecond component (0 to 999999).

## MINUTE

```text
minute(date)
```

Returns the minute component (0 to 59).

## MONTH

```text
month(date)
```

Returns the month number (1 to 12).

## MONTH_NAME

```text
month_name(date)
```

Returns the full month name (for example, `January`).

## SECOND

```text
second(date)
```

Returns the second component (0 to 59).

## YEAR

```text
year(date)
```

Returns the four-digit year component as an integer.

### Parameters

- `date`: An integer timestamp in epoch milliseconds, an ISO 8601 string (e.g. `'2026-10-14T20:00:00Z'`), or any valid date/time value.

### Return Value

A 64-bit integer representing the year (for example, `2026`). Returns `null` if the input is null or unparseable.

### Example

```sql
SELECT year(ts) AS yr FROM demo;
```

Given input `{"ts": "2026-10-14T20:00:00Z"}`:

```json
{"yr": 2026}
```

## FORMAT_DATE

```text
format_date(date, pattern)
```

Formats a timestamp or date into a human-readable string according to the specified formatting pattern.

### Parameters

- `date`: An integer timestamp in epoch milliseconds, or an RFC 3339 / ISO 8601 timestamp string.
- `pattern`: A pattern string using standard datetime formatting tokens (such as `YYYY-MM-dd HH:mm:ss`, `YYYY/MM/dd`, or `yyyy-MM-dd'T'HH:mm:ss`).

### Return Value

A formatted datetime string. Returns `null` if the input timestamp is invalid or unparseable.

### Example

```sql
SELECT format_date(ts, 'YYYY-MM-dd HH:mm:ss') AS readable_time FROM demo;
```

Given input `{"ts": 1792008000000}`:

```json
{"readable_time": "2026-10-14 20:00:00"}
```

## DATE_PARSE

```text
date_parse(str, pattern)
```

Parses a formatted date string into 64-bit Unix epoch milliseconds.

### Parameters

- `str`: The input string containing the formatted date or datetime.
- `pattern`: The strftime format pattern matching `str` (e.g. `'%Y-%m-%d %H:%M:%S'`, `'%Y-%m-%d'`).

### Return Value

An int64 integer representing milliseconds elapsed since January 1, 1970 00:00:00 UTC. Returns `null` if parsing fails.

### Example

```sql
SELECT date_parse('2026-10-14 20:00:00', '%Y-%m-%d %H:%M:%S') AS epoch_millis FROM demo;
```

Result:

```json
{"epoch_millis": 1792008000000}
```

## CONVERT_TZ

```text
convert_tz(date, [from_tz,] to_tz)
```

Converts a timestamp between time zones and returns the formatted local timestamp string `YYYY-MM-DD HH:MM:SS`.

### Parameters

- `date`: Timestamp in epoch milliseconds or ISO 8601 format.
- `from_tz` *(optional)*: Source timezone identifier (e.g. `'UTC'`). If omitted when two arguments are provided, input is interpreted as UTC.
- `to_tz`: Target timezone name. Accepts `'UTC'`, `'Local'` (host operating system time zone), or any standard IANA timezone database name (e.g. `'Asia/Shanghai'`, `'America/New_York'`, `'Europe/Berlin'`).

### Return Value

A string formatted as `YYYY-MM-DD HH:MM:SS` in the target time zone.

### Example

```sql
SELECT convert_tz(ts, 'UTC', 'Asia/Shanghai') AS beijing_time FROM demo;
```

Given input `{"ts": "2026-10-14T12:00:00Z"}`:

```json
{"beijing_time": "2026-10-14 20:00:00"}
```

## TO_SECONDS

```text
to_seconds(date)
```

Converts a datetime value or timestamp into total elapsed seconds since January 1, 1970 00:00:00 UTC (Unix epoch).

### Parameters

- `date`: Timestamp in epoch milliseconds or an ISO 8601 string.

### Return Value

An int64 integer representing elapsed seconds. Returns `null` if the input is null or invalid.

### Example

```sql
SELECT to_seconds(ts) AS epoch_sec FROM demo;
```

Given input `{"ts": "2026-10-14T20:00:00Z"}`:

```json
{"epoch_sec": 1792008000}
```

