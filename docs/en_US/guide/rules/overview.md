# Rules Overview

rekuiper uses rules to define stream processing workflows. Rules ingest data from sources, execute transformations, and transmit results to external sinks.

A rule is a JSON object that defines the stream processing pipeline.

```json
{
  "id": "rule1",
  "sql": "SELECT demo.temperature, demo1.temp FROM demo left join demo1 on demo.timestamp = demo1.timestamp where demo.temperature > demo1.temp GROUP BY demo.temperature, HOPPINGWINDOW(ss, 20, 10)",
  "actions": [
    {
      "log": {}
    },
    {
      "mqtt": {
        "server": "tcp://47.52.67.87:1883",
        "topic": "demoSink"
      }
    }
  ]
}
```

A rule contains these core components:

- **id**: Unique rule identifier.
- **sql**: Processing query, written in the rekuiper SQL dialect.
- **actions**: Array of sink actions that transmit output data.

The table below describes rule definition parameters:

| Parameter Name | Optional | Description |
| :--- | :--- | :--- |
| `id` | false | Unique identifier of the rule within the rekuiper instance. |
| `name` | true | Display name or description of the rule. |
| `version` | true | Semantic or revision version tag of the rule definition. |
| `sql` | Required if `graph` is omitted | SQL query that defines data processing logic. |
| `actions` | Required if `graph` is omitted | Array of target sink actions. |
| `graph` | Required if `sql` is omitted | JSON representation of the Directed Acyclic Graph (DAG). |
| `options` | true | Map of execution options. |
| `triggered` | true | Controls whether the rule starts immediately after creation. Default is `true`. |
| `tags` | true | Array of string tags for filtering rules. |
| `temp` | true | Controls whether the rule is temporary. Temporary rules run only in the current session and do not persist across restarts. |

## Rule Logic

A rule defines data flow from input sources through SQL processing operators to output sinks.

You can define rule logic using two methods:

1. **SQL Query**: Uses declarative SQL queries and action arrays.
2. **Graph Model**: Uses a Directed Acyclic Graph (DAG) in JSON format. This method is suitable for visual interfaces.

### SQL Query Approach

Define rule logic with the `sql` and `actions` properties. The `sql` property specifies transformations on predefined streams. The `actions` array routes output records to target destinations.

#### SQL

The minimal SQL rule query is `SELECT * FROM demo`. rekuiper provides ANSI SQL syntax with streaming extensions and functions. Refer to the [SQL Reference](../../sqls/overview.md) for query syntax.

SQL queries specify two elements:

- The stream or table sources in the `FROM` clause.
- The transformation and aggregation logic.

Define the required stream before you create the rule. Refer to [Streams](../streams/overview.md) for stream definitions.

#### Actions

The `actions` property defines output destinations. A rule can include multiple actions. Each action represents a sink connector instance.

In the `actions` array, the object key specifies the sink connector type, and the object value specifies connector properties.

rekuiper includes built-in sinks such as `mqtt`, `rest`, and `file`. You can also add custom sink plugins. Refer to [Sinks](../sinks/overview.md) for details.

##### Disabling Actions

You can temporarily mute or disable an action without deleting its configuration by specifying `"disabled": true` either inside the connector properties or as a top-level action attribute:

```json
{
  "actions": [
    {
      "log": {}
    },
    {
      "mqtt": {
        "server": "tcp://127.0.0.1:1883",
        "topic": "factory/alerts",
        "disabled": true
      }
    }
  ]
}
```

When an action is disabled, rekuiper ignores it during pipeline execution, skipping connector initialization and buffer allocations.

### Graph Model

The `graph` property provides an alternative method to define a rule. The property defines a DAG in JSON format. This model maps directly to graphical user interfaces.

Example graph rule definition:

```json
{
  "id": "rule1",
  "name": "Test Condition",
  "graph": {
    "nodes": {
      "demo": {
        "type": "source",
        "nodeType": "mqtt",
        "props": {
          "datasource": "devices/+/messages"
        }
      },
      "humidityFilter": {
        "type": "operator",
        "nodeType": "filter",
        "props": {
          "expr": "humidity > 30"
        }
      },
      "logfunc": {
        "type": "operator",
        "nodeType": "function",
        "props": {
          "expr": "log(temperature) as log_temperature"
        }
      },
      "tempFilter": {
        "type": "operator",
        "nodeType": "filter",
        "props": {
          "expr": "log_temperature < 1.6"
        }
      },
      "pick": {
        "type": "operator",
        "nodeType": "pick",
        "props": {
          "fields": ["log_temperature as temp", "humidity"]
        }
      },
      "mqttout": {
        "type": "sink",
        "nodeType": "mqtt",
        "props": {
          "server": "tcp://${mqtt_srv}:1883",
          "topic": "devices/result"
        }
      }
    },
    "topo": {
      "sources": ["demo"],
      "edges": {
        "demo": ["humidityFilter"],
        "humidityFilter": ["logfunc"],
        "logfunc": ["tempFilter"],
        "tempFilter": ["pick"],
        "pick": ["mqttout"]
      }
    }
  }
}
```

The `graph` object contains:
- `nodes`: Defines individual source, operator, and sink nodes.
- `topo`: Defines directed edges between nodes.

Node types include built-in nodes (such as filter, window, and function) and plugin nodes. Refer to [Graph Rules](./graph_rule.md) for details.

## Rule Configuration Options

Configure rule behavior using the `options` object:

- **Debugging and Logging**: Configure debug levels and dedicated log files.
- **Time Semantics**: Select event time or processing time.
- **Fault Tolerance**: Configure handling for late-arriving events.
- **Concurrency**: Set parallel instance counts for processing phases.
- **Buffering**: Set memory buffer capacity.
- **QoS and Checkpointing**: Configure state persistence intervals and delivery guarantees.
- **Restart Strategy**: Configure automatic recovery after errors.
- **Schedules**: Define periodic execution schedules using cron expressions.

The table below describes rule execution options:

| Option Name | Type & Default | Description |
| :--- | :--- | :--- |
| `debug` | bool: `false` | Enables debug logging for this rule. If omitted, the rule inherits global settings. |
| `logFilename` | string: `""` | Sets a dedicated log file name in the log directory. If omitted, the rule uses global log files. |
| `isEventTime` | bool: `false` | Selects event time or processing time. If `true`, the rule extracts timestamps from the stream payload. |
| `lateTolerance` | int64: `0` | Maximum allowed lateness in milliseconds for event-time windows. Records exceeding this limit are dropped. |
| `concurrency` | int: `1` | Number of parallel execution instances per execution plan. Values greater than 1 may not preserve message order. |
| `bufferLength` | int: `1024` | Maximum queued messages in memory per plan. When the queue fills, backpressure pauses upstream ingestion. |
| `sendMetaToSink` | bool: `false` | Transmits event metadata to the target sink. |
| `sendError` | bool: `false` | Sends runtime error messages downstream to sinks. When `false`, errors write to logs only. |
| `qos` | int: `0` | Quality of Service level (`0`: At most once, `1`: At least once, `2`: Exactly once). Values $>0$ activate checkpointing. |
| `checkpointInterval` | int: `300000` | State snapshot interval in milliseconds. Applies only when `qos` is greater than 0. |
| `restartStrategy` | struct | Strategy for automatic recovery after failures. Refer to [Rule Restart Strategy](#rule-restart-strategy). |
| `cron` | string: `""` | Cron expression that defines periodic execution schedules. |
| `duration` | string: `""` | Execution runtime duration per cron cycle. Duration must not exceed cycle frequency. |
| `cronDatetimeRange` | list of struct | Valid time windows for scheduled rules. Refer to [Scheduled Rules](#scheduled-rules). |
| `enableRuleTracer` | bool: `false` | Enables data tracing for the rule. |
| `sendNilField` | bool: `false` | Outputs fields containing `null` values. |
| `planOptimizeStrategy` | struct | Configures rule query plan optimizations. |
| `disableBufferFullDiscard` | bool: `false` | Controls whether buffer overflow drops data or exerts backpressure. |

Refer to [State and Fault Tolerance](./state_and_fault_tolerance.md) for details on `qos` and checkpoints.

Define global defaults under `rules` in `etc/kuiper.yaml`. Rule-level options override global defaults.

### Plan Optimization Options

Configure query plan optimization through `planOptimizeStrategy`:

| Option Name | Type & Default | Description |
| :--- | :--- | :--- |
| `enableIncrementalWindow` | bool: `false` | Enables incremental window aggregation when supported functions and time windows are present. |

## Monitor Rule Status

Rule startup is asynchronous. When a client submits a start command, rekuiper completes static checks and initiates startup asynchronously.

The command response indicates that rekuiper accepted the request. It sets the expected rule status to `started`. Check runtime status to confirm that the rule executes.

Retrieve status for all rules using the [Show Rules API](../../api/restapi/rules.md#show-rules). Retrieve status for a single rule using the [Get Rule Status API](../../api/restapi/rules.md#get-the-status-of-a-rule).

### Inspect Rule Metrics

Consider this example rule:

```json
{
  "id": "rule",
  "sql": "select * from demo",
  "actions": [
     {
      "mqtt": {
        "server": "tcp://127.0.0.1:1883",
        "topic": "devices/+/messages",
        "qos": 1,
        "clientId": "demo_001",
        "retained": false
      }
    }
  ]
}
```

Query the rule status through the REST API:

```json
{
  "status": "running",
  "source_demo_0_records_in_total": 0,
  "source_demo_0_records_out_total": 0,
  "op_2_project_0_records_in_total": 0,
  "op_2_project_0_records_out_total": 0,
  "sink_mqtt_0_0_records_in_total": 0,
  "sink_mqtt_0_0_records_out_total": 0
}
```

Field explanations:
- `status`: Indicates execution state. Value `running` indicates active execution.
- Metric names use the format: `operatorType_information_concurrencyIndex_metricName`.

For example, in `source_demo_0_records_in_total`:
- `source`: Operator type.
- `demo`: Associated stream name.
- `0`: Operator instance index.
- `records_in_total`: Total records received by this operator instance.

When a client sends a test record to the stream, operator metrics increment:

```json
{
  "status": "running",
  "source_demo_0_records_in_total": 1,
  "source_demo_0_records_out_total": 1,
  "op_2_project_0_records_in_total": 1,
  "op_2_project_0_records_out_total": 1,
  "sink_mqtt_0_0_records_in_total": 1,
  "sink_mqtt_0_0_records_out_total": 1
}
```

The metric values change from 0 to 1, showing that each operator processed the record and the sink transmitted output.

When Prometheus metrics are enabled, Prometheus collects these counters. Refer to the [Metrics List](../../operation/usage/monitor_with_prometheus.md#metric-types).

## Automatic Rule Operation Strategies

rekuiper supports automated operation strategies:

1. **Rule Restart Strategy**: Restarts rules automatically after runtime failures.
2. **Periodic Rules**: Executes rules on a recurring schedule.

A global patrol mechanism manages automatic execution. The engine evaluates rules at fixed intervals and applies configured strategies. Configure this interval using `rulePatrolInterval` under `basic` in `etc/kuiper.yaml` (for example, `rulePatrolInterval: 10s`).

### Rule Restart Strategy

Configure automatic restart behavior using the `restartStrategy` option.

Set `attempts` to an integer greater than 0 to enable automatic restart attempts.

### Scheduled Rules

Rules support scheduled execution cycles. Configure periodic behavior with `cron` and `duration`:
- `cron`: Specifies the trigger interval with a cron expression.
- `duration`: Specifies execution duration per cycle.

For example, if `cron` triggers every hour and `duration` is 30 minutes, the rule runs for 30 minutes and pauses until the next hour.

Stopping a scheduled rule via the [Stop Rule API](../../api/restapi/rules.md#stop-a-rule) removes the rule from the scheduler and stops execution.

`cronDatetimeRange` parameters:

| Option Name | Type | Description |
| :--- | :--- | :--- |
| `begin` | string | Start timestamp string formatted as `YYYY-MM-DD hh:mm:ss`. |
| `end` | string | End timestamp string formatted as `YYYY-MM-DD hh:mm:ss`. |
| `beginTimestamp` | int | Start Unix epoch timestamp in milliseconds. |
| `endTimestamp` | int | End Unix epoch timestamp in milliseconds. |

Example `cronDatetimeRange` configuration:

```json
{
  "cronDatetimeRange": [
    {
      "begin": "2023-06-26 10:00:00",
      "end": "2023-06-26 20:00:00"
    },
    {
      "beginTimestamp": 1701401478000,
      "endTimestamp": 1701401578000
    }
  ]
}
```

#### Phased Rule Execution

When you configure `cronDatetimeRange` without `cron` and `duration`, the rule executes continuously until the end timestamp.

## Rule Versioning

Rules support an optional `version` field. When you update a rule, rekuiper compares the new version string with the existing version string.

The engine accepts updates only when the new version string is lexicographically greater than the existing version string.

### Versioning Logic

- **No Version Specified**: If neither the existing nor the new definition includes a `version` field, the update succeeds.
- **Version Specified**: If either definition includes a `version` field, rekuiper performs a lexical comparison.
- **Lexical Comparison**: The update succeeds only when the new `version` string is lexicographically greater than the current version string.
- **Unversioned Baseline**: An unversioned rule has the lowest possible version rank. Adding a `version` field to an unversioned rule always succeeds.

Use timestamps, such as Unix epoch values, for version strings. Monotonically increasing timestamps satisfy lexical comparison rules reliably.

