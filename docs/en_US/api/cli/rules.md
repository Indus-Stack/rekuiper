# Rules Management

The rekuiper rule command-line interface manages rules. You can create, display, describe, start, stop, restart, validate, drop rules, and inspect rule status and topology.

## Create a Rule

Use this command to create a new rule. Rule definitions use JSON format. For syntax details, refer to [Rules Overview](../../guide/rules/overview.md).

```shell
create rule $rule_name '$rule_json' | create rule $rule_name -f $rule_def_file
```

You can supply rule definitions through two methods:

### Specify the Rule Definition on the Command Line

Enclose the JSON definition string in quotes:

```shell
# bin/kuiper create rule rule1 '{"sql": "SELECT * from demo","actions": [{"log":  {}},{"mqtt":  {"server":"tcp://127.0.0.1:1883", "topic":"demoSink"}}]}'
```

This command creates a rule named `rule1`.

> [!NOTE]
> When you use single quotes for string literals inside single-quoted shell commands, the shell interprets the literal as a variable name:
>
> ```text
> $ echo '{"sql": "SELECT lower('abc') FROM demo"}'
> {"sql": "SELECT lower(abc) FROM demo"}
> ```
>
> Use double quotes for SQL string literals inside single-quoted JSON strings to prevent unexpected variable substitution.

### Specify the Rule Definition in a File

Use the `-f` flag to load complex rule definitions from a file:

```shell
# bin/kuiper create rule rule1 -f /tmp/rule.txt
```

Example contents of `/tmp/rule.txt`:

```json
{
  "sql": "SELECT * from demo",
  "actions": [
    {
      "log": {}
    },
    {
      "mqtt": {
        "server": "tcp://127.0.0.1:1883",
        "topic": "demoSink"
      }
    }
  ]
}
```

## Show Rules

Use this command to list all rules defined on the server with their current execution status:

```shell
show rules
```

Example output:

```shell
# bin/kuiper show rules
[
  {
    "id": "rule1",
    "status": "Running"
  },
  {
     "id": "rule2",
     "status": "Stopped: canceled by error."
  }
]
```

## Describe a Rule

Use this command to display the JSON definition of a rule:

```shell
describe rule $rule_name
```

Example output:

```shell
# bin/kuiper describe rule rule1
{
  "sql": "SELECT * from demo",
  "actions": [
    {
      "log": {}
    },
    {
      "mqtt": {
        "server": "tcp://127.0.0.1:1883",
        "topic": "demoSink"
      }
    }
  ]
}
```

## Drop a Rule

Use this command to delete a rule:

```shell
drop rule $rule_name
```

Example command:

```shell
# bin/kuiper drop rule rule1
Rule rule1 is dropped.
```

## Start a Rule

Use this command to start running a rule:

```shell
start rule $rule_name
```

Example command:

```shell
# bin/kuiper start rule rule1
Rule rule1 was started.
```

## Stop a Rule

Use this command to stop a running rule:

```shell
stop rule $rule_name
```

Example command:

```shell
# bin/kuiper stop rule rule1
Rule rule1 was stopped.
```

## Restart a Rule

Use this command to restart a rule:

```shell
restart rule $rule_name
```

Example command:

```shell
# bin/kuiper restart rule rule1
Rule rule1 was restarted.
```

## Get the Status of a Rule

Use this command to get the runtime metrics or error status of a rule:

```shell
getstatus rule $rule_name
```

When the rule is running, the command returns real-time metrics. When the rule is stopped, it returns `stopped: $reason`.

Example output:

```shell
# bin/kuiper getstatus rule rule1
{
    "source_demo_0_records_in_total":5,
    "source_demo_0_records_out_total":5,
    "source_demo_0_exceptions_total":0,
    "source_demo_0_process_latency_ms":0,
    "source_demo_0_buffer_length":0,
    "source_demo_0_last_invocation":"2020-01-02T11:28:33.054821",
    "op_filter_0_records_in_total":5,
    "op_filter_0_records_out_total":2,
    "op_filter_0_exceptions_total":0,
    "op_filter_0_process_latency_ms":0,
    "op_filter_0_buffer_length":0,
    "op_filter_0_last_invocation":"2020-01-02T11:28:33.054821"
}
```

## Get the Topology Structure of a Rule

Use this command to display the directed graph topology of a rule:

```shell
gettopo rule $rule_name
```

The returned JSON object contains two fields:

- `sources`: A string array containing the names of all input source nodes.
- `edges`: A map of directed graph edges grouped by originating operator node.

Example output:

```json
{
  "sources": [
    "source_stream"
  ],
  "edges": {
    "op_project": [
      "sink_log"
    ],
    "source_stream": [
      "op_project"
    ]
  }
}
```

## Validate a Rule or Manifest

Use this command to validate an online rule definition against a running server or validate a declarative manifest offline without starting rules:

```shell
validate rule $rule_name '$rule_json' | validate rule $rule_name -f $rule_def_file
validate $manifest_path [--allow-unresolved-env] [--json]
```

### Validate via Command Line

```shell
# bin/kuiper validate rule rule1 '{"sql": "SELECT * from demo","actions": [{"log":  {}},{"mqtt":  {"server":"tcp://127.0.0.1:1883", "topic":"demoSink"}}]}'
```

### Validate via File

```shell
# bin/kuiper validate rule rule1 -f /tmp/rule.txt
```

Online rule validation returns the server JSON response with `valid`, `sources`, secret-redacted `plan`, `effective_guarantees`, `limiting_component`, and `diagnostics`.

### Validate a Declarative Manifest Offline

```shell
# rekuiper-cli validate ./manifests --json
```

Offline manifest validation parses each rule through the same typed capability planner used by the REST API. When `--json` is set, the command prints `summary` and secret-redacted `plans` entries (`[REDACTED]` for credentials and URL userinfo).

## Explain a Rule

Use this command to retrieve the parsed SQL plan, redacted options and actions, `effective_guarantees`, and `limiting_component` for an existing rule:

```shell
# rekuiper-cli explain rule rule1
```
