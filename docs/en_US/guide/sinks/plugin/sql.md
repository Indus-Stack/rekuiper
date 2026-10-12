# SQL Sink

::: tip Status: Supported as Built-in Connector in rekuiper
rekuiper includes a built-in SQL sink connector implemented directly in Rust with `sqlx`. You do not need to compile or deploy Go dynamic plugins (`.so` files).
:::

The SQL sink writes stream processing results into relational database tables.

## Supported Database Engines

rekuiper supports the following relational database engines natively:

- **PostgreSQL**: `postgres://username:password@hostname:5432/database` or `postgresql://...`
- **SQLite**: `sqlite://path/to/database.db` or in-memory `sqlite::memory:`

::: note
Legacy eKuiper compiled driver-specific Go plugins for MySQL, Oracle, and Microsoft SQL Server. rekuiper uses asynchronous Rust database connections via `sqlx`. For other database engines, use the [REST Sink](../builtin/rest.md) or external bridge microservices.
:::

## Configuration Properties

| Property Name | Optional | Default Value | Description |
|---|---|---|---|
| `url` (or `dburl`) | False | None | Connection URL string for the target database. |
| `table` | False | None | Target database table name. |
| `fields` | True | Inferred | Array of column names to write. If omitted, rekuiper uses the keys of the output record. Dynamic keys must match `[A-Za-z_][A-Za-z0-9_]*` (keys containing spaces or punctuation are rejected to prevent SQL syntax errors). |
| `sendSingle` | True | `false` | When `true`, writes each event record individually. |

rekuiper constructs parameterized SQL insert statements dynamically:
- SQLite uses `?` parameter markers.
- PostgreSQL uses `$1`, `$2`, `$3` positional parameter markers.
- Data values map to typed SQL parameters (`integer`, `float`, `boolean`, `text`, or untyped `NULL`).

## Sample Usage

### Write Records to PostgreSQL

The following rule processes sensor data and inserts the records into a PostgreSQL table:

```json
{
  "id": "rule_pg_sink",
  "sql": "SELECT deviceId, temperature, humidity, ts FROM sensorStream WHERE temperature > 25.0",
  "actions": [
    {
      "log": {}
    },
    {
      "sql": {
        "url": "postgres://postgres:password@localhost:5432/telemetry",
        "table": "sensor_readings",
        "fields": ["deviceId", "temperature", "humidity", "ts"]
      }
    }
  ]
}
```

### Write Records to SQLite

The following rule writes output records to a local SQLite database file:

```json
{
  "id": "rule_sqlite_sink",
  "sql": "SELECT id, alertCode, message FROM alertStream",
  "actions": [
    {
      "sql": {
        "url": "sqlite:///var/data/alerts.db",
        "table": "alerts",
        "fields": ["id", "alertCode", "message"]
      }
    }
  ]
}
```

### In-Memory SQLite for Testing

You can use an in-memory SQLite database for ephemeral testing without disk storage:

```json
{
  "id": "rule_memory_sqlite",
  "sql": "SELECT * FROM demoStream",
  "actions": [
    {
      "sql": {
        "url": "sqlite::memory:",
        "table": "temp_results"
      }
    }
  ]
}
```
