# How to Troubleshoot Rules

rekuiper is a lightweight, high-performance SQL engine for edge computing. The engine processes streaming data from multiple sources and sends results to target sinks.

This document describes how to troubleshoot rekuiper rules when they do not operate as expected.

## Create the Rule

You must create a rule before you can troubleshoot it. You can create rules with the REST API or the CLI. This document uses the REST API for all rule management actions.

The example below creates a rule with the REST API:

```http request
###
POST http://{{host}}/rules
Content-Type: application/json

{
  "id": "rule1",
  "sql": "SELECT values.tag1 AS temperature, values.tag2 AS humidity FROM sensorStream",
  "actions": [
    {
      "influx": {
        "addr": "http://10.11.71.70:8086",
        "username": "",
        "password": "",
        "measurement": "test",
        "databasename": "mydb",
        "tagkey": "tagkey",
        "tagvalue": "tagvalue",
        "fields": "humidity,temperature"
      }
    }
  ]
}
```

### Troubleshooting Guidelines

When rule creation fails, the server returns an error message. Check the error message to identify the cause.

#### Check the HTTP Response

Examine the HTTP response from the server. When rule creation succeeds, the server returns this response:

```json
{
  "code": 200,
  "message": "OK"
}
```

When an error occurs, the server displays details in the response body. For example, if the SQL statement is invalid, the server returns this error:

```json
{
  "code": 400,
  "message": "invalid sql: near \"SELEC\": syntax error"
}
```

#### Check the Logs

The response body usually provides sufficient information. For more detail, check the rekuiper server logs.

The logs are located in the `logs` directory under the rekuiper installation directory. Use the `tail` command to view logs in real time.

When running in Docker, enable console logging. Set the environment variable `KUIPER__BASIC__CONSOLELOG=true` or set `consoleLog` to `true` in `etc/kuiper.yaml`.

Then use the `docker logs` command to inspect container logs.

### Common Errors

When you submit a rule, rekuiper validates and starts the rule. Common submission errors include:

#### Syntax Errors

**1. SQL syntax error**

If you submit a rule with SQL `SELECT temperature humidity FROM sensor`, the server returns this error message:

```text
HTTP/1.1 400 Bad Request

invalid rule json: Parse SQL SELECT temperature humidity FROM sensorStream error: found "humidity", expected FROM..
```

A comma is missing between the two fields. The SQL parser interprets `humidity` as a table name and expects `FROM` before it.

To resolve SQL parse errors, examine and correct the SQL syntax.

**2. Stream not found**

You must create a stream before you use it in a rule. If the referenced stream does not exist, the server returns this error:

```text
HTTP/1.1 400 Bad Request

create rule topo error: fail to get stream myStream, please check if stream is created
```

To resolve this error, create the stream first. Use the REST API to view existing streams and create the required stream.

**3. Rule ID exists**

Rule IDs must be unique in rekuiper. If you submit a rule with an existing ID, the server returns this error:

```text
HTTP/1.1 400 Bad Request

store the rule error: Item rule1 already exists
```

To resolve this error, assign a different ID to the rule or delete the existing rule.

## Diagnose the Rule

By default, rekuiper starts a rule immediately after creation. If the rule targets an MQTT topic, subscribe to that topic to receive output. If the rule does not produce expected results, use the diagnostic steps below.

### Diagnostic Procedures

Follow these procedures to diagnose rule execution:

**1. Check rule status**

Rule creation only validates static syntax. At runtime, external dependencies such as data sources can fail. Check the rule status to verify whether the rule is running or stopped.

Send a GET request through the REST API to check the status of rule `rule1`:

```http request
###
GET http://{{host}}/rules/rule1/status
```

If the rule fails to run, the server returns a response such as:

```json
{
  "status": "stopped",
  "message": "Stopped: mqtt sink is missing property topic."
}
```

The `message` field states why the rule stopped.

**2. Check the metrics**

If the rule status is `running` but produces no output, inspect the runtime metrics.

The status API returns metrics for all pipeline nodes, including sources, operators, and sinks. Each node reports values for input records, output records, and processing latency.

First, inspect the source metrics:

```text
"source_demo_0_records_in_total": 0,
"source_demo_0_records_out_total": 0,
```

If `records_in_total` is 0, the source receives no data. Verify the data source:
- Verify that the upstream publisher transmits data.
- Verify that the source configuration is correct. For example, if the MQTT source configuration specifies `topic1` but data is published to `topic2`, the source receives no data.

If source metrics show incoming records, inspect the metrics for operators and sinks.

When a rule includes a `WHERE` clause, the pipeline contains a `filter` operator. The filter drops records that do not match the condition. Check `filter_xxx_records_in_total` and `filter_xxx_records_out_total`.

If `records_out_total` is less than `records_in_total`, the operator filtered some records. If `records_out_total` is 0, the operator filtered all records.

If this filtering is unexpected, verify the raw data by enabling debug logs or creating debug rules.

**3. Check the debug logs**

If a rule stops, examine the server logs for failure details. If a rule runs but metrics are unexpected, enable debug logging to track data flow.

Refer to [Check the Logs](#check-the-logs) for log locations. To enable debug logs, set the log level to `debug` in `etc/kuiper.yaml`, or set the environment variable `KUIPER__BASIC__DEBUG=true`.

Below is an example debug log entry for a filter operator:

```text
2026-05-31T14:58:43.123456Z DEBUG rekuiper_core::plan::filter: filter received record payload={"temperature":-11.77,"ts":1684738889251} rule=rule1
```

The field `rule=rule1` at the end of the log line indicates the rule origin. The log shows data received by the filter operator:
`{"temperature":-11.77, "ts":1684738889251}`

This entry indicates stream record payload attributes `temperature=-11.77` and `ts=1684738889251`. Compare your `WHERE` condition with this payload to evaluate why records do not pass.

**4. Create debug rules**

To avoid searching through extensive debug logs, create a debug rule. For example, add a `log` sink alongside an `mqtt` sink to write output records directly to the log:

```json
{
  "id": "rule1",
  "sql": "SELECT * FROM mockStream WHERE temperature > 30",
  "actions": [
    {
      "mqtt": {
        "server": "{{broker address}}",
        "topic": "topic1"
      },
      "log": {
      }
    }
  ]
}
```

To diagnose filter behavior, create a secondary rule without filtering to print all received records:

```json
{
  "id": "rule1_debug",
  "sql": "SELECT * FROM mockStream",
  "actions": [
    {
      "log": {
      }
    }
  ]
}
```

If a filter condition uses calculated values, create a rule to output the calculation. For example, if the query contains `WHERE temperature - lag(temperature) > 1`, output `lag(temperature)` in the `SELECT` list to inspect intermediate values.

## End-to-End Troubleshooting Walkthrough

This section demonstrates how to troubleshoot a complete rule scenario. The rule reads data from a stream and transmits an alert when temperature increases by more than 1 degree.

First, create the input stream:

```http request
###
POST http://{{host}}/streams
Content-Type: application/json

{"sql":"CREATE STREAM mockStream() WITH (DATASOURCE=\"data/mock\", FORMAT=\"json\", TYPE=\"mqtt\");"}
```

The server returns HTTP status 200 upon creation. The stream is schemaless and subscribes to the MQTT topic `data/mock`. Test payloads use this JSON structure: `{"temperature": 10, "humidity": 20}`.

### Scenario 1: Rule with Syntax Error

Submit the initial rule definition through the REST API:

```http request
###
POST http://{{host}}/rules
Content-Type: application/json

{
  "id": "rule1",
  "sql": "SELECT temperature, humidity FROM mockStream WHERE temprature - laig(temperature) > 1",
  "actions": [
    {
      "mqtt": {
        "server": "tcp://yourserver:1883",
        "topic": "result"
      }
    }
  ]
}
```

The server returns HTTP status 400 with this error message:

```text
HTTP/1.1 400 Bad Request

Create rule error: Invalid rule json: Parse SQL SELECT temperature, humidity FROM mockStream WHERE temprature - laig(temperature) > 1 error: function laig not found.
```

The error indicates that function `laig` does not exist. Correct the typo to resolve the error.

### Scenario 2: Rule Fails to Run

Submit the corrected rule definition:

```http request
###
POST http://{{host}}/rules
Content-Type: application/json

{
  "id": "rule1",
  "sql": "SELECT temperature, humidity FROM mockStream WHERE temprature - lag(temperature) > 1",
  "actions": [
    {
      "mqtt": {
        "server": "tcp://yourserver:1883",
        "topic": "result"
      }
    }
  ]
}
```

The server creates the rule successfully, but the result topic receives no messages.

Check the rule status:

```http request
###
GET http://{{host}}/rules/rule1/status
```

If the MQTT broker is not reachable, the server returns this response:

```json
{
  "status": "stopped",
  "message": "Stopped: found error when connecting for tcp://yourserver:1883: network Error : dial tcp: lookup syno1.home: no such host."
}
```

The message indicates that the broker address is unreachable. Correct the sink broker address, verify broker availability, and restart the rule:

```http request
###
POST http://{{host}}/rules/rule1/start
```

Check the rule status again. When the rule runs, the server returns metrics:

```json
{
  "status": "running",
  "source_mockStream_0_records_in_total": 0,
  "source_mockStream_0_records_out_total": 0,
  "source_mockStream_0_process_latency_us": 0,
  "source_mockStream_0_buffer_length": 0,
  "source_mockStream_0_last_invocation": 0,
  "source_mockStream_0_exceptions_total": 0,
  "source_mockStream_0_last_exception": "",
  "source_mockStream_0_last_exception_time": 0,
  "op_2_analytic_0_records_in_total": 0,
  "op_2_analytic_0_records_out_total": 0,
  "op_2_analytic_0_process_latency_us": 0,
  "op_2_analytic_0_buffer_length": 0,
  "op_2_analytic_0_last_invocation": 0,
  "op_2_analytic_0_exceptions_total": 0,
  "op_2_analytic_0_last_exception": "",
  "op_2_analytic_0_last_exception_time": 0,
  "op_3_filter_0_records_in_total": 0,
  "op_3_filter_0_records_out_total": 0,
  "op_3_filter_0_process_latency_us": 0,
  "op_3_filter_0_buffer_length": 0,
  "op_3_filter_0_last_invocation": 0,
  "op_3_filter_0_exceptions_total": 0,
  "op_3_filter_0_last_exception": "",
  "op_3_filter_0_last_exception_time": 0,
  "op_4_project_0_records_in_total": 0,
  "op_4_project_0_records_out_total": 0,
  "op_4_project_0_process_latency_us": 0,
  "op_4_project_0_buffer_length": 0,
  "op_4_project_0_last_invocation": 0,
  "op_4_project_0_exceptions_total": 0,
  "op_4_project_0_last_exception": "",
  "op_4_project_0_last_exception_time": 0,
  "sink_mqtt_0_0_records_in_total": 0,
  "sink_mqtt_0_0_records_out_total": 0,
  "sink_mqtt_0_0_process_latency_us": 0,
  "sink_mqtt_0_0_buffer_length": 0,
  "sink_mqtt_0_0_last_invocation": 0,
  "sink_mqtt_0_0_exceptions_total": 0,
  "sink_mqtt_0_0_last_exception": "",
  "sink_mqtt_0_0_last_exception_time": 0
}
```

The metrics indicate that the rule runs but has received no data. Publish test data to `mockStream`:

```json
{
  "temperature": 10,
  "humidity": 20
}
```

Check the rule status. Metric `source_mockStream_0_records_in_total` remains 0.

Examine the stream definition. The stream specifies topic `data/mock`, but the client published to `mockStream`.

Publish the test payload to `data/mock`. The source metric increments:

```json
{
  "status": "running",
  "source_mockStream_0_records_in_total": 1,
  "source_mockStream_0_records_out_total": 1,
  "source_mockStream_0_process_latency_us": 753,
  "source_mockStream_0_buffer_length": 0,
  "source_mockStream_0_last_invocation": "2023-05-31T15:49:32.997547",
  "source_mockStream_0_exceptions_total": 0,
  "source_mockStream_0_last_exception": "",
  "source_mockStream_0_last_exception_time": 0,
  "op_2_analytic_0_records_in_total": 1,
  "op_2_analytic_0_records_out_total": 1,
  "op_2_analytic_0_process_latency_us": 0,
  "op_2_analytic_0_buffer_length": 0,
  "op_2_analytic_0_last_invocation": "2023-05-31T15:50:10.9103",
  "op_2_analytic_0_exceptions_total": 0,
  "op_2_analytic_0_last_exception": "",
  "op_2_analytic_0_last_exception_time": 0,
  "op_3_filter_0_records_in_total": 1,
  "op_3_filter_0_records_out_total": 0,
  "op_3_filter_0_process_latency_us": 0,
  "op_3_filter_0_buffer_length": 0,
  "op_3_filter_0_last_invocation": "2023-05-31T15:50:10.9103",
  "op_3_filter_0_exceptions_total": 0,
  "op_3_filter_0_last_exception": "",
  "op_3_filter_0_last_exception_time": 0,
  "op_4_project_0_records_in_total": 0,
  "op_4_project_0_records_out_total": 0,
  "op_4_project_0_process_latency_us": 0,
  "op_4_project_0_buffer_length": 0,
  "op_4_project_0_last_invocation": 0,
  "op_4_project_0_exceptions_total": 0,
  "op_4_project_0_last_exception": "",
  "op_4_project_0_last_exception_time": 0,
  "sink_mqtt_0_0_records_in_total": 0,
  "sink_mqtt_0_0_records_out_total": 0,
  "sink_mqtt_0_0_process_latency_us": 0,
  "sink_mqtt_0_0_buffer_length": 0,
  "sink_mqtt_0_0_last_invocation": 0,
  "sink_mqtt_0_0_exceptions_total": 0,
  "sink_mqtt_0_0_last_exception": "",
  "sink_mqtt_0_0_last_exception_time": 0
}
```

### Scenario 3: Diagnose Filter Behavior

Publish a second payload to `data/mock`:

```json
{
  "temperature": 15,
  "humidity": 25
}
```

Temperature increased by 5, which satisfies the filter condition. However, the result topic receives no message.

Examine the rule metrics:

```json
{
  "status": "running",
  "source_mockStream_0_records_in_total": 2,
  "source_mockStream_0_records_out_total": 2,
  "source_mockStream_0_process_latency_us": 753,
  "source_mockStream_0_buffer_length": 0,
  "source_mockStream_0_last_invocation": "2023-05-31T15:49:32.997547",
  "source_mockStream_0_exceptions_total": 0,
  "source_mockStream_0_last_exception": "",
  "source_mockStream_0_last_exception_time": 0,
  "op_2_analytic_0_records_in_total": 2,
  "op_2_analytic_0_records_out_total": 2,
  "op_2_analytic_0_process_latency_us": 0,
  "op_2_analytic_0_buffer_length": 0,
  "op_2_analytic_0_last_invocation": "2023-05-31T15:50:10.9103",
  "op_2_analytic_0_exceptions_total": 0,
  "op_2_analytic_0_last_exception": "",
  "op_2_analytic_0_last_exception_time": 0,
  "op_3_filter_0_records_in_total": 2,
  "op_3_filter_0_records_out_total": 0,
  "op_3_filter_0_process_latency_us": 0,
  "op_3_filter_0_buffer_length": 0,
  "op_3_filter_0_last_invocation": "2023-05-31T15:50:10.9103",
  "op_3_filter_0_exceptions_total": 0,
  "op_3_filter_0_last_exception": "",
  "op_3_filter_0_last_exception_time": 0,
  "op_4_project_0_records_in_total": 0,
  "op_4_project_0_records_out_total": 0,
  "op_4_project_0_process_latency_us": 0,
  "op_4_project_0_buffer_length": 0,
  "op_4_project_0_last_invocation": 0,
  "op_4_project_0_exceptions_total": 0,
  "op_4_project_0_last_exception": "",
  "op_4_project_0_last_exception_time": 0,
  "sink_mqtt_0_0_records_in_total": 0,
  "sink_mqtt_0_0_records_out_total": 0,
  "sink_mqtt_0_0_process_latency_us": 0,
  "sink_mqtt_0_0_buffer_length": 0,
  "sink_mqtt_0_0_last_invocation": 0,
  "sink_mqtt_0_0_exceptions_total": 0,
  "sink_mqtt_0_0_last_exception": "",
  "sink_mqtt_0_0_last_exception_time": 0
}
```

The metrics show that the source received 2 records, but `op_3_filter_0_records_out_total` is 0. All records were filtered.

To investigate, create a debug rule that moves the filter calculation into the `SELECT` clause:

```http request
###
POST http://{{host}}/rules
Content-Type: application/json

{
  "id": "ruleDebug",
  "sql": "SELECT temperature, humidity, temprature - lag(temperature) as diff FROM mockStream",
  "actions": [
    {
      "mqtt": {
        "server": "{{yourhost}}",
        "topic": "debug"
      }
    }
  ]
}
```

In the debug rule, remove the `WHERE` clause and move `temprature - lag(temperature)` to the `SELECT` clause. The query outputs calculation results for every input.

Restart both rules and publish the test payloads to `data/mock` again.

Inspect the output of `ruleDebug`:

```json lines
{
  "temperature": 15,
  "humidity": 20
}
{
  "temperature": 20,
  "humidity": 25
}
```

The output omits the `diff` field, which indicates a `null` value.

Examine the expression `temprature - lag(temperature)`. Notice the spelling error `temprature` instead of `temperature`.

In schemaless mode, the SQL parser cannot validate field names. Verify field spelling when you use schemaless streams.

### Scenario 4: Corrected Rule Execution

Update the rule with the corrected field name:

```http request
###
PUT http://{{host}}/rules/rule1
Content-Type: application/json

{
  "id": "rule1",
  "sql": "SELECT temperature, humidity FROM mockStream WHERE temperature - lag(temperature) > 1",
  "actions": [
    {
      "mqtt": {
        "server": "{{yourhost}}",
        "topic": "result"
      }
    }
  ]
}
```

The server restarts the rule and resets metrics. Publish the test payloads to `data/mock` in sequence:

```json lines
{
  "temperature": 15,
  "humidity": 20
}
{
  "temperature": 20,
  "humidity": 25
}
```

The rule condition is satisfied, and the sink receives output on the `result` topic:

```json
{
  "temperature": 20,
  "humidity": 25
}
```

## Summary

This document described how to troubleshoot rules using metrics, server logs, and diagnostic rules. Use these verification steps to identify syntax errors, connection failures, routing mismatches, and query defects.

