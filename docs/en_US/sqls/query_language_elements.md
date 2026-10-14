# Query Language Elements

rekuiper provides clauses and expressions for querying, transforming, and aggregating stream data.

## Summary of Query Elements

| Element | Summary |
|---|---|
| [SELECT](#select) | Selects columns and expressions from input streams. |
| [FROM](#from) | Identifies the source stream or table. The `FROM` clause is mandatory in every `SELECT` query. |
| [JOIN](#join) | Combines records across multiple streams or between streams and lookup tables. Joining multiple streams requires a [window](./windows.md). |
| [WHERE](#where) | Filters rows according to search criteria. |
| [GROUP BY](#group-by) | Groups rows by columns, expressions, or [windows](./windows.md) for aggregate calculations. |
| [ORDER BY](#order-by) | Sorts query outputs in ascending or descending order. |
| [HAVING](#having) | Filters grouped rows based on aggregate conditions. |
| [LIMIT](#limit) | Restricts the maximum number of returned records. |

## SELECT

The `SELECT` clause retrieves fields and computes expressions from incoming streams.

### Syntax

```sql
SELECT
    * [EXCEPT(column_name, ...)] [REPLACE(expression AS column_name, ...)]
    | [source_stream.]column_name [AS column_alias]
    | expression
```

### Arguments

- `*`: Selects all fields from the input source.

  ```sql
  SELECT * FROM demo;
  ```

- `* EXCEPT(...)`: Excludes specified fields while returning all other columns:

  ```sql
  SELECT * EXCEPT(a, b) FROM demo;
  ```

- `* REPLACE(...)`: Replaces specific columns with calculated expressions while retaining other fields:

  ```sql
  SELECT * REPLACE(a + b AS c) FROM demo;
  ```

  When combining `EXCEPT` and `REPLACE`, `REPLACE` takes precedence if a column appears in both clauses:

  ```sql
  SELECT * EXCEPT(c1, c2) REPLACE(expr1 AS c1, expr3 AS c3) FROM stream1;
  ```

- `column_name`: Names the column to return. For nested attributes, use [JSON expressions](json_expr.md).
- `column_alias`: Assigns a new name to a column or calculated expression. Aliases cannot be referenced inside `WHERE`, `GROUP BY`, or `HAVING` clauses.
  
  An alias can be referenced in subsequent expressions within the same `SELECT` clause:

  ```sql
  SELECT a + 1 AS sum1, sum1 + 1 AS sum2 FROM demo;
  -- If a = 1: {"sum1": 2, "sum2": 3}
  ```

  If an alias shares its name with an existing column, the expression defining the alias references the original column, while subsequent expressions reference the alias:

  ```sql
  SELECT a + 1 AS a, a + 2 AS sum2 FROM demo;
  -- If a = 1: {"a": 2, "sum2": 4}
  ```

- `expression`: Constants, function invocations, column references, or operators.

## FROM

Specifies the input source. Every `SELECT` statement requires a `FROM` clause.

### Syntax

```sql
FROM source_stream [AS source_stream_alias]
```

## JOIN

Combines records across multiple streams or between a stream and a table. Multi-stream joins must run inside a window.

### Syntax

```sql
[LEFT | RIGHT | FULL | CROSS | ASOF | LEFT ASOF] JOIN
source_stream [AS alias]
ON stream1.column_name = stream2.column_name
```

### Join Types

- **LEFT JOIN**: Returns all records from the left stream (`stream1`) and matching records from the right stream (`stream2`). If no match exists, right fields evaluate to `NULL`:

  ```sql
  SELECT * FROM stream1 LEFT JOIN stream2 ON stream1.id = stream2.id GROUP BY COUNTWINDOW(5);
  ```

- **RIGHT JOIN**: Returns all records from the right stream (`stream2`) and matching records from the left stream (`stream1`). If no match exists, left fields evaluate to `NULL`:

  ```sql
  SELECT * FROM stream1 RIGHT JOIN stream2 ON stream1.id = stream2.id GROUP BY COUNTWINDOW(5);
  ```

- **FULL JOIN**: Returns all records when a match exists in either stream. Missing values are filled with `NULL`:

  ```sql
  SELECT * FROM stream1 FULL JOIN stream2 ON stream1.id = stream2.id GROUP BY COUNTWINDOW(5);
  ```

- **CROSS JOIN**: Produces the Cartesian product of both input streams ($m \times n$ rows):

  ```sql
  SELECT * FROM stream1 CROSS JOIN stream2 GROUP BY COUNTWINDOW(5);
  ```

- **ASOF JOIN**: Pairs each record from the left stream with the latest matching record from the right stream. It does not generate a Cartesian product. It aligns high-frequency streams with low-frequency streams without window buffering:

  ```sql
  SELECT s.id, s.val, r.price FROM s ASOF JOIN r ON s.id = r.id;
  ```

- **LEFT ASOF JOIN**: Preserves the left stream record even when no prior right stream record exists. Missing right fields evaluate to `NULL`:

  ```sql
  SELECT s.id, s.val, r.price FROM s LEFT ASOF JOIN r ON s.id = r.id;
  ```

## WHERE

Filters input records before windowing and aggregation.

### Syntax

```sql
WHERE search_condition
```

### Supported Operators

- Comparison operators: `=`, `<>`, `!=`, `>`, `>=`, `<`, `<=`
- Range check: `[NOT] BETWEEN lower_expr AND upper_expr`
- Pattern match: `[NOT] LIKE pattern_expr` (`%` matches zero or more characters; `_` matches a single character)
- Membership check: `[NOT] IN (expr1, expr2, ...)` or `[NOT] IN array_expr`
- Logical operators: `AND`, `OR`, `NOT`

Examples:

```sql
SELECT * FROM demo WHERE a > 10 AND a < 15;
SELECT * FROM demo WHERE a BETWEEN 10 AND 15;
SELECT * FROM demo WHERE name LIKE "sensor_%";
SELECT * FROM demo WHERE status IN ("active", "standby");
```

## GROUP BY

Groups rows into summary records according to column values, expressions, or temporal windows.

### Syntax

```sql
GROUP BY column_expression [, ...] | window_specification
```

Example grouping by column and window:

```sql
SELECT deviceId, avg(temperature) AS avgTemp
FROM demo
GROUP BY deviceId, TUMBLINGWINDOW(ss, 10);
```

> [!NOTE]
> Column expressions in `GROUP BY` cannot reference aliases declared in the `SELECT` clause.

### HAVING

Filters grouped records after aggregation.

```sql
SELECT deviceId, count(*) AS msgCount
FROM demo
GROUP BY deviceId, TUMBLINGWINDOW(ss, 10)
HAVING count(*) > 5;
```

## ORDER BY

Sorts query output by one or more columns.

### Syntax

```sql
ORDER BY column_name [ASC | DESC] [, ...]
```

- `ASC`: Sorts values in ascending order (default).
- `DESC`: Sorts values in descending order.

```sql
SELECT * FROM demo
GROUP BY COUNTWINDOW(5)
ORDER BY temperature DESC;
```

## LIMIT

Restricts the maximum number of output records returned by a query:

```sql
SELECT * FROM demo
GROUP BY COUNTWINDOW(5)
ORDER BY temperature DESC
LIMIT 10;
```

## CASE Expression

Evaluates conditional expressions and returns a matching value.

### Simple CASE Expression

Compares an expression against a set of values:

```sql
SELECT
    CASE color
        WHEN "red" THEN 1
        WHEN "yellow" THEN 2
        ELSE 3
    END AS colorCode,
    humidity
FROM demo;
```

### Searched CASE Expression

Evaluates a sequence of boolean predicates:

```sql
SELECT
    CASE
        WHEN size < 150 THEN "S"
        WHEN size < 170 THEN "M"
        WHEN size < 175 THEN "L"
        ELSE "XL"
    END AS sizeLabel
FROM demo;
```

## Lexical Elements

To use reserved keywords or special characters in column names and identifiers, refer to [Lexical Elements](lexical_elements.md).
