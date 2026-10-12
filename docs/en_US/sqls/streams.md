# Stream Statements

SQL Data Definition Language (DDL) statements create and manage data streams.

## CREATE STREAM

The `CREATE STREAM` statement registers a new stream connected to an external source:

```sql
CREATE STREAM stream_name
    ( column_name <data_type> [ ,...n ] )
    WITH ( property_name = expression [, ...] );
```

For detailed property definitions and stream options, refer to the [Stream Overview Guide](../guide/streams/overview.md).

### Example

```sql
CREATE STREAM my_stream ()
WITH (
    DATASOURCE = "topic/temperature",
    FORMAT = "json",
    KEY = "id",
    BUFFER_FULL_POLICY = "block"
);
```

### Buffer Policy

Use the `BUFFER_FULL_POLICY` property to control ingestion when the input buffer is full.

- `block`: The source pauses reading until the buffer has available space.
- `dropOldest`: The source deletes the oldest record to accept the incoming record. Default if omitted.

The server rejects other values with an error during stream creation and update.

#### Multi-Stream Rules (Joins)

When a rule query joins multiple streams (e.g. `FROM stream1 JOIN stream2 ON ...`), all streams referenced in the query must configure an identical `BUFFER_FULL_POLICY`. If one stream uses `block` while another uses `dropOldest`, the server rejects the rule with a validation error to prevent asymmetric backpressure deadlocks and erratic tuple drops.

## DESCRIBE STREAM

The `DESCRIBE STREAM` statement returns the schema and configuration of a registered stream:

```sql
DESCRIBE STREAM stream_name;
```

## DROP STREAM

The `DROP STREAM` statement removes a registered stream definition:

```sql
DROP STREAM stream_name;
```

> [!CAUTION]
> Delete all rules that reference the stream before running `DROP STREAM`.

## SHOW STREAMS

The `SHOW STREAMS` statement lists all streams currently registered in the database:

```sql
SHOW STREAMS;
```
