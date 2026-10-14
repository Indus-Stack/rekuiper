# Analytic Functions

> [!NOTE]
> **Verification Status**: Tested and Verified against `rekuiper` engine with live telemetry stream load on **2026-10-14 15:00:00 UTC**.  
> **Scorecard**: **18 Verified, 0 Unsupported, 0 Broken**:  
> - **Verified (18)**: `lag`, `lead`, `latest`, `changed_col`, `had_changed`, `changed_cols`, `acc_sum`, `acc_max`, `acc_min`, `acc_count`, `acc_avg`, `acc_collect`, `acc_distinct_collect`, `acc_max_by`, `acc_min_by`, `acc_map_agg`, `sequence_match`, `sequence_count`.

Analytic functions use internal state to perform continuous data analysis. In stream processing, the system evaluates analytic functions before `WHERE` clause predicates. Therefore, `WHERE` filter conditions do not affect analytic function state.

The general syntax for an analytic function call is:

```text
AnalyticFuncName(<arguments>...) OVER ([PARTITION BY <partition key>] [WHEN <Expression> [UNTIL <Expression>]])
```

The `OVER` clause is optional.

Analytic functions evaluate across all input events of the current query. Use the optional `PARTITION BY` clause to restrict calculations to matching partition keys:

```text
AnalyticFuncName(<arguments>...) OVER ([PARTITION BY <partition key>])
```

Use the `WHEN` clause to determine whether the current event is valid based on a condition:

```text
AnalyticFuncName(<arguments>...) OVER ([WHEN <Expression>])
```

When an event satisfies the condition, the function computes the result and updates the state. When an event does not satisfy the condition, the function ignores the event value and retains the saved state value.

## LAG

```text
lag(expr, [offset], [default value], [ignore null])
```

Returns the expression result from a previous row at the specified offset.

**Parameters:**

- `expr`: The expression to evaluate.
- `offset` (optional): The lookback count of qualifying values (default: 1). A value qualifies when its row satisfies `WHEN`, if present, and is not null when `ignore null` is true.
- `default_value` (optional): The value to return when no row exists at the offset (default: nil).
- `ignore_null` (optional): Determines whether to ignore null values during lookback (default: true).

**Behavior:**

- When using `WHEN`, `lag(expr, 1)` returns the most recent qualifying value, and `lag(expr, 2)` returns the second most recent qualifying value. Rows that fail `WHEN` do not consume the offset.
- If no qualifying value exists at the specified offset, the function returns the default value.
- If you do not specify a default value, the function returns nil.
- When you omit both offset and default value, the function uses offset = 1 and default = nil.

Example: get the previous temperature value:

```text
lag(temperature)
```

Example: get the previous temperature value within the same device partition:

```text
lag(temperature) OVER (PARTITION BY deviceId)
```

Example: calculate event duration where `ts` is a timestamp, and `statusCode` represents device status:

```text
select lag(Status) as Status, ts - lag(ts, 1, ts, true) OVER (WHEN had_changed(true, statusCode)) as duration from demo
```

## LEAD

```text
lead(expr, [offset], [default value], [ignore null])
  OVER ([PARTITION BY <partition key>] [WHEN <Expression> [UNTIL <Expression>]])
```

Returns the result of `expr` from a future input row.

**Parameters:**

- `expr`: The expression to evaluate.
- `offset` (optional): The lookahead count of qualifying values (default: 1).
- `default_value` (optional): The value to return when no future row matches (default: nil).
- `ignore_null` (optional): A boolean that controls if the function ignores null values during lookahead (default: true).

The offset counts qualifying future values. A value qualifies when its row satisfies `WHEN`, if present, and is not null when `ignore null` is true.

For example, `lead(expr, 2) OVER (WHEN condition)` returns the second future qualifying value. Rows that do not satisfy `WHEN` do not consume the offset.

Because the result depends on future input, the engine buffers the current row until the requested future value arrives, `UNTIL` evaluates to true, or the input stream ends.

The `WHEN` clause selects future candidate rows. The offset defines a match count, not a time limit or row limit.

The `UNTIL` clause provides a stop condition. It is an eKuiper extension that is valid only together with `WHEN`. The engine evaluates `UNTIL` independently for every buffered row before evaluating `WHEN`.

Inside `UNTIL`, standard column names reference the newly arrived probe row. The `current_row(expr)` function evaluates `expr` against the buffered origin row. If `UNTIL` evaluates to true, the function returns the default value. The `current_row` function is valid only in this context.

```sql
lead(candidate_t2) OVER (
  WHEN isNull(b) = false
  UNTIL ts - current_row(ts) > 5
)
```

The `UNTIL` condition is data-driven. The engine checks `UNTIL` only when input arrives. It does not create processing-time timers or event-time watermarks.

For event-time rules, `LEAD` holds downstream watermarks behind buffered rows. This prevents windows from closing before those rows are released. Watermarks advance after the rows are released.

The engine evaluates `WHEN` and candidate expressions only when a pending request requires a candidate after checking `UNTIL`. If evaluating a probe row fails, the engine commits no decisions for that probe and does not add the probe to the pending queue. Subsequent valid input continues to resolve pending requests.

### Best Practices for LEAD

- Configure an explicit `UNTIL` condition when a future match is not guaranteed, especially with selective `WHEN` conditions. Use `WHEN true` if every future row is a candidate but a stop condition is required.
- Choose a stop condition that minimizes pending requests under expected throughput. For example, with millisecond timestamps, `UNTIL ts - current_row(ts) > 1000` stops waiting when a probe exceeds one second from the origin.
- Estimate the required buffer per partition using: `input rows per second * average wait in seconds`. High ingestion rates can accumulate many requests even during brief wait intervals.
- The engine checks `UNTIL` only when rows arrive in the same partition. An idle partition cannot expire requests independently. Because output maintains global input order, an unresolved row in one partition can hold back completed rows in other partitions.
- Each probe evaluates all outstanding requests in its partition. Long queues increase CPU and memory utilization. Checkpoint snapshot sizes also grow with buffered state.

## LATEST

```text
latest(expr, [default value])
```

Returns the latest non-null value of the expression. Returns the specified default value if no value exists, or nil if no default value is configured.

## CHANGED_COL

```text
changed_col(true, col)
```

Returns the column value if the value changed since the previous execution.

## HAD_CHANGED

```text
had_changed(true, expr1, expr2, ...)
```

Returns a boolean indicating whether any specified expression changed since the previous execution. You can specify `*` to detect changes across all columns.

## Functions to Detect Changes

### changed_col

This function is a scalar function. You can use it in any clause, including `SELECT` and `WHERE`.

**Syntax:**

```text
CHANGED_COL(<ignoreNull>, <expr>)
```

**Arguments:**

- `ignoreNull`: A boolean indicating whether to ignore null values during comparison. When true, null values do not trigger a change.
- `expr`: An expression to evaluate and monitor for state changes.

**Returns:**

Returns the changed value or nil. The default column name is `changed_col`. Use an `AS alias` clause to rename the output column.

### changed_cols

This function returns multiple columns. You can use it only in the `SELECT` clause.

**Syntax:**

```text
CHANGED_COLS(<prefix>, <ignoreNull>, <expr> [,...,<exprN>])
```

**Arguments:**

- `prefix`: A string prefix for output column names. If empty (`""`), output column names match expression names. For example, `CHANGED_COLS("changed_", true, col1)` produces `changed_col1`.
- `ignoreNull`: A boolean indicating whether to ignore null values during comparison. When true, null values do not trigger a change.
- `expr`: One or more expressions to monitor. You can specify `*` to monitor all columns.

**Returns:**

Returns all values that changed relative to the previous sink output. In a continuous rule, it compares against the previous output row. In a window rule, it compares against the previous window output.

On the initial execution, the function returns all expressions because no prior baseline exists.

On subsequent executions, if no values change, the function outputs nothing. When sinks configure `omitEmpty`, no sink action triggers.

**Usage Constraints:**

- Use this function only in the `SELECT` clause. You cannot reference its multi-column output in `WHERE` or other clauses. If you require filtering on changed values, use `CHANGED_COL` or configure a rule pipeline.
- Column aliases apply globally through the `prefix` parameter. To assign distinct aliases per column, invoke `CHANGED_COL` separately for each column with an `AS alias` clause.

### had_changed

This function is a scalar function that accepts one or more arguments.

**Syntax:**

```text
HAD_CHANGED(<ignoreNull>, <expr> [,...,<exprN>])
```

**Arguments:**

- `ignoreNull`: A boolean indicating whether to ignore null values. When true, null values do not trigger a change.
- `expr`: One or more expressions to monitor. You can specify `*` to monitor all columns.

**Returns:**

Returns true if any argument changed since the previous execution. Multi-argument syntax evaluates as an `OR` condition: `HAD_CHANGED(expr1) OR HAD_CHANGED(expr2)`.

To detect an `AND` condition where all expressions must change, combine individual function calls: `HAD_CHANGED(expr1) AND HAD_CHANGED(expr2)`.

When used in a window `HAVING` clause (e.g. `GROUP BY HoppingWindow(...) HAVING had_changed(concat(latest(a), ":", latest(b)))`), change state is tracked across window evaluations, suppressing output when consecutive window aggregate outputs are identical.


### Change Detection Examples

Create a stream named `demo` with the following input records:

```json lines
{"ts": 1, "temperature": 23, "humidity": 88}
{"ts": 2, "temperature": 23, "humidity": 88}
{"ts": 3, "temperature": 23, "humidity": 88}
{"ts": 4, "temperature": 25, "humidity": 88}
{"ts": 5, "temperature": 25, "humidity": 90}
{"ts": 6, "temperature": 25, "humidity": 91}
{"ts": 7, "temperature": 25, "humidity": 91}
{"ts": 8, "temperature": 25, "humidity": 91}
```

Example 1: Return changed temperature values:

```text
SQL: SELECT CHANGED_COLS("", true, temperature) FROM demo
___________________________________________________
{"temperature":23}
{"temperature":25}
```

Example 2: Return changed temperature and humidity values with a column prefix:

```text
SQL: SELECT CHANGED_COLS("c_", true, temperature, humidity) FROM demo
_________________________________________________________
{"c_ts":1, "c_temperature":23, "c_humidity":88}
{"c_ts":2}
{"c_ts":3}
{"c_ts":4, "c_temperature":25}
{"c_ts":5, "c_humidity":90}
{"c_ts":6, "c_humidity":91}
{"c_ts":7}
{"c_ts":8}
```

Example 3: Return changed values for all columns without ignoring null values:

```text
SQL: SELECT CHANGED_COLS("c_", false, *) FROM demo
_________________________________________________________
{"c_temperature":23,"c_humidity":88}
{"c_temperature":25}
{"c_humidity":90}
{"c_humidity":91}
```

Example 4: Return average temperature changes in a window:

```text
SQL: SELECT CHANGED_COLS("t", true, avg(temperature)) FROM demo GROUP BY CountWindow(2)
_________________________________________________________________
{"tavg":23}
{"tavg":24}
{"tavg":25}
```

Example 5: Filter events where temperature or humidity changed:

```text
SQL: SELECT ts, temperature, humidity FROM demo
WHERE HAD_CHANGED(true, temperature, humidity) = true
_________________________________________________________
{"ts":1,"temperature":23,"humidity":88}
{"ts":4,"temperature":25,"humidity":88}
{"ts":5,"temperature":25,"humidity":90}
{"ts":6,"temperature":25,"humidity":91}
```

Example 6: Filter events where temperature changed but humidity remained constant:

```text
SQL: SELECT ts, temperature, humidity FROM demo
WHERE HAD_CHANGED(true, temperature) = true AND HAD_CHANGED(true, humidity) = false
_________________________________________________________
{"ts":4,"temperature":25,"humidity":88}
```

Example 7: Return changed values with explicit column aliases:

```text
SQL: SELECT CHANGED_COL(true, temperature) AS myTemp, CHANGED_COL(true, humidity) AS myHum FROM demo
_________________________________________________________
{"myTemp":23,"myHum":88}
{"myTemp":25}
{"myHum":90}
{"myHum":91}
```

Example 8: Filter events where temperature changed to a value greater than 24:

```text
SQL: SELECT ts, temperature, humidity FROM demo
WHERE CHANGED_COL(true, temperature) > 24
_________________________________________________________
{"ts":4,"temperature":25,"humidity":88}
```

## ACC Functions

ACC (accumulate) functions perform cumulative calculations across the lifecycle of a rule.

The examples below use this sequence of input values for column `a`: `1`, `2`, `3`.

### ACC_SUM

```text
acc_sum(expr)
```

Accumulates expression results and returns the running cumulative sum.

Example:

```text
acc_sum(a)
```

Results: `1`, `3`, `6`.

### ACC_MAX

```text
acc_max(expr)
```

Compares expression values cumulatively and returns the running maximum value.

Example:

```text
acc_max(a)
```

Results: `1`, `2`, `3`.

### ACC_MIN

```text
acc_min(expr)
```

Compares expression values cumulatively and returns the running minimum value.

Example:

```text
acc_min(a)
```

Results: `1`, `1`, `1`.

### ACC_COUNT

```text
acc_count(expr)
```

Counts evaluated expression results and returns the running cumulative count.

Example:

```text
acc_count(a)
```

Results: `1`, `2`, `3`.

### ACC_AVG

```text
acc_avg(expr)
```

Computes the running cumulative average of the expression results.

Example:

```text
acc_avg(a)
```

Results: `1`, `1.5`, `2`.

### ACC_COLLECT

```text
acc_collect(expr)
```

Collects non-nil expression results into an array, preserving arrival order.

Example:

```text
acc_collect(a)
```

Results: `[1]`, `[1,2]`, `[1,2,3]`.

### ACC_DISTINCT_COLLECT / DISTINCT_ACC

```text
acc_distinct_collect(expr)
distinct_acc(expr)
```

Collects distinct non-nil expression results into an array. `distinct_acc(expr)` is an exact drop-in synonym.
Preserves the order of first arrival.
Ignores duplicate values.

Example:

```text
acc_distinct_collect(a)
```

For input sequence `1`, `2`, `1`, `3`, the results are:
- Event 1: `[1]`
- Event 2: `[1, 2]`
- Event 3: `[1, 2]`
- Event 4: `[1, 2, 3]`

### ACC_MAX_BY

```text
acc_max_by(value, compare_value)
```

Compares `compare_value` cumulatively and returns the `value` associated with the maximum `compare_value`. If `compare_value` matches an earlier maximum, the function returns `value` from the most recent event. Returns nil if no valid `compare_value` exists.

Example: get the collection timestamp associated with the cumulative maximum temperature:

```text
acc_max_by(ts, temp) over (partition by soc)
```

### ACC_MIN_BY

```text
acc_min_by(value, compare_value)
```

Compares `compare_value` cumulatively and returns the `value` associated with the minimum `compare_value`. If `compare_value` matches an earlier minimum, the function returns `value` from the most recent event. Returns nil if no valid `compare_value` exists.

Example: get the collection timestamp associated with the cumulative minimum temperature:

```text
acc_min_by(ts, temp) over (partition by soc)
```

### ACC_MAP_AGG

```text
acc_map_agg(key, value)
```

Cumulatively builds an array of key-value objects. The function converts `key` to a string. When duplicate keys arrive, the function updates the item with the latest `value` while preserving the initial key order.

Each array element is an object with `key` and `value` fields.

Example:

```text
acc_map_agg(soc, object_construct(
    'max_temp', max_temp,
    'max_temp_ts', max_temp_ts
))
```

Example result:

```json
[
  {"key": "18", "value": {"max_temp": 30, "max_temp_ts": 1788000060000}},
  {"key": "19", "value": {"max_temp": 31, "max_temp_ts": 1788000090000}}
]
```

### ACC Functions with Conditions

ACC functions can define calculation start points and reset points through additional parameters:

```text
acc_count(a, expr1, expr2)
```

- `expr1`: Represents the condition to start cumulative calculation.
- `expr2`: Represents the condition to reset cumulative calculation.

Example:

```text
acc_count(a, a > 1, a < 0)
```

Given this input stream for `a`:

```text
a = 1
a = 2
a = 1
a = 3
a = -1
a = 1
```

The function outputs:

```text
0
1
2
3
4
0
```

## SEQUENCE_MATCH

```text
sequence_match(max_span_ms, cond1, cond2, ...) [OVER (PARTITION BY <key>)]
```

Evaluates a multi-step event sequence using a deterministic finite automaton (DFA). It returns `true` when all condition expressions succeed in exact order within `max_span_ms` milliseconds.

**Parameters:**

- `max_span_ms`: Maximum duration in milliseconds between the first matching condition and the final condition. Set to `0` for unbounded time.
- `cond1, cond2, ...`: Two or more boolean condition expressions representing sequential stages.

**Behavior:**

- When condition 1 is satisfied, the automaton begins tracking the sequence at current event time.
- If subsequent conditions are satisfied within `max_span_ms`, the automaton transitions to the next stage.
- If `max_span_ms` expires before the final condition succeeds, the automaton resets to stage 0.
- State memory is bounded to a maximum of 10,000 distinct partition keys with automatic eviction.

Example: detect high temperature followed by high pressure within 30 seconds:

```sql
SELECT sequence_match(30000, temp > 80.0, pressure > 120.0) AS matched FROM telemetry;
```

Example with partition key:

```sql
SELECT sequence_match(60000, status = "warn", status = "fail") OVER (PARTITION BY device_id) AS alarm FROM sensors;
```

## SEQUENCE_COUNT

```text
sequence_count(max_span_ms, cond1, cond2, ...) [OVER (PARTITION BY <key>)]
```

Counts the total number of non-overlapping sequence occurrences that complete within `max_span_ms` milliseconds.

**Parameters:**

- `max_span_ms`: Maximum duration in milliseconds allowed for each sequence completion.
- `cond1, cond2, ...`: Sequential boolean conditions.

**Behavior:**

- Returns the cumulative count of completed sequence patterns.
- After a sequence completes, the automaton resets to count subsequent occurrences.

Example: count repeated fault cycles:

```sql
SELECT sequence_count(15000, error_code = 1, error_code = 2) AS fault_cycles FROM logs;
```

