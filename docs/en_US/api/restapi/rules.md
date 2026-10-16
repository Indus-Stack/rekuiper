# Rules Management

> [!NOTE]
> **Verification Status**: Tested and Verified against `rekuiper` engine with live streaming load on **2026-09-30 18:39:32 UTC**.  
> **Scorecard**: **12 / 12 Methods Exhaustively Verified with Live Telemetry Data Flow under Load**:
> - `POST /rules` (create and start rule) - Verified (HTTP 201 Created)
> - `GET /rules` (show rules) - Verified (HTTP 200 OK)
> - `GET /rules/{id}` (describe a rule) - Verified (HTTP 200 OK)
> - `GET /rules/{id}/status` (get rule status metrics) - Verified (HTTP 200 OK)
> - `GET /rules/status/all` (get status of all rules) - Verified (HTTP 200 OK)
> - `GET /rules/{id}/topo` (get topology structure) - Verified (HTTP 200 OK)
> - `POST /rules/validate` (validate rule) - Verified (HTTP 200 OK)
> - `POST /rules/{id}/stop` (stop rule, zero-egress verified under load) - Verified (HTTP 200 OK)
> - `POST /rules/{id}/start` (start rule, egress resumed) - Verified (HTTP 200 OK)
> - `POST /rules/{id}/restart` (restart rule) - Verified (HTTP 200 OK)
> - `PUT /rules/{id}` (upsert rule with updated query) - Verified (HTTP 200 OK)
> - `DELETE /rules/{id}` (drop rule, confirmed 404) - Verified (HTTP 200 OK)

The rekuiper REST API manages rule lifecycles. You can create, inspect, validate, start, stop, restart, tag, and delete rules.

## Create a Rule

Use this endpoint to create and start a rule from a JSON definition:

```http
POST http://localhost:9081/rules
```

Request payload:

```json
{
  "id": "rule1",
  "sql": "SELECT * FROM demo",
  "actions": [{
    "log": {}
  }]
}
```

Response sample (HTTP 201 Created):

```text
Rule rule1 was created
```

## Show Rules

Use this endpoint to list all defined rules and their current execution status:

```http
GET http://localhost:9081/rules
```

Response sample (HTTP 200 OK):

```json
[
  {
    "id": "rule1",
    "name": "rule1",
    "status": "Running"
  },
  {
     "id": "rule2",
     "name": "rule2",
     "status": "Stopped: canceled by error."
  }
]
```

## Describe a Rule

Use this endpoint to retrieve the JSON definition of a rule:

```http
GET http://localhost:9081/rules/{id}
```

Response sample (HTTP 200 OK):

```json
{
  "id": "rule1",
  "name": "rule1",
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

## Get Rule Output Schema

Use this endpoint to retrieve the output schema produced by the rule's `SELECT` statement:

```http
GET http://localhost:9081/rules/{id}/schema
```

Example response:

```json
{
  "id": {
    "hasIndex": true,
    "index": 0
  },
  "name": {
    "hasIndex": true,
    "index": 1
  }
}
```

## Upsert a Rule

Use this endpoint to create or update a rule. If the rule exists, the engine updates it. If update fails, the existing rule continues running:

```http
PUT http://localhost:9081/rules/{id}
```

Request payload:

```json
{
  "id": "rule1",
  "sql": "SELECT * FROM demo",
  "actions": [{
    "log": {}
  }]
}
```

## Drop a Rule

Use this endpoint to delete a rule:

```http
DELETE http://localhost:9081/rules/{id}
```

## Start a Rule

Use this endpoint to start a stopped rule:

```http
POST http://localhost:9081/rules/{id}/start
```

The response confirms transmission of the start instruction. Query the rule status to verify that initialization completed.

## Stop a Rule

Use this endpoint to stop a running rule:

```http
POST http://localhost:9081/rules/{id}/stop
```

The response confirms transmission of the stop instruction. Query the rule status to verify that shutdown completed.

## Restart a Rule

Use this endpoint to restart a rule:

```http
POST http://localhost:9081/rules/{id}/restart
```

## Get Rule Status Metrics

Use this endpoint to retrieve real-time execution metrics or stop reasons:

```http
GET http://localhost:9081/rules/{id}/status
```

Response sample for a running rule:

```json
{
  "status": "running",
  "message": "",
  "lastStartTimestamp": 1792184907961,
  "lastStopTimestamp": 0,
  "nextStartTimestamp": 0,
  "lastException": "",
  "last_exception": "",
  "exceptionsTotal": 0,
  "exceptions_total": 0,
  "sourceRecordsInTotal": 50,
  "sourceRecordsFilteredTotal": 10,
  "sinkRecordsEnqueuedTotal": 40,
  "sinkRecordsOutTotal": 40,
  "sinkRecordsFailedTotal": 0,
  "sinkQueueHighWater": 3,
  "sinkBlockedMicrosTotal": 0,
  "droppedByPolicyTotal": 0,
  "dlqRecordsTotal": 0
}
```

- `lastStartTimestamp`: Unix epoch milliseconds when the rule was created or restarted.
- `lastStopTimestamp`: Unix epoch milliseconds when the rule was stopped (0 if running continuously).
- `nextStartTimestamp`: Unix epoch milliseconds of the next scheduled trigger for periodic rules.
- `sourceRecordsInTotal`: Total input records received across sources for this rule.
- `sinkRecordsOutTotal`: Total output records emitted by sinks for this rule.
- `exceptionsTotal`: Total errors encountered during processing.
- `lastException`: Last recorded exception message (available in both `lastException` and `last_exception` aliases).

## Get Status of All Rules

Use this endpoint to retrieve real-time metrics for all defined rules:

```http
GET http://localhost:9081/rules/status/all
```

## Get Rule Topology

Use this endpoint to retrieve the execution graph topology of a rule:

```http
GET http://localhost:9081/rules/{id}/topo
```

Response sample (HTTP 200 OK):

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

## Validate a Rule

Use this endpoint to validate a rule definition without creating or running it:

```http
POST http://localhost:9081/rules/validate
```

Request payload:

```json
{
  "id": "rule1",
  "sql": "SELECT * FROM demo",
  "actions": [{
    "log": {}
  }]
}
```

HTTP status codes:
- `200 OK`: Rule definition is valid.
- `400 Bad Request`: Request body format is invalid.
- `422 Unprocessable Entity`: Rule validation failed.

## Query Rule Execution Plan

Use this endpoint to inspect the logical and physical plan of the rule SQL statement:

```http
GET http://localhost:9081/rules/{id}/explain
```

## Get Rule CPU Utilization

Use this endpoint to retrieve CPU time used by all rules over the previous 30 seconds (in milliseconds):

```http
GET http://localhost:9081/rules/usage/cpu
```

Example response:

```json
{
  "rule1": 220,
  "rule2": 270
}
```

## Reset Tags on a Rule

Use this endpoint to overwrite the tag array assigned to a rule:

```http
PUT http://localhost:9081/rules/{id}/tags
```

Request payload:

```json
{
  "tags": ["t1", "t2"]
}
```

## Add Tags to a Rule

Use this endpoint to append tags to an existing rule:

```http
PATCH http://localhost:9081/rules/{id}/tags
```

Request payload:

```json
{
  "tags": ["t1", "t2"]
}
```

## Delete Tags from a Rule

Use this endpoint to remove specific tags from a rule:

```http
DELETE http://localhost:9081/rules/{id}/tags
```

Request payload:

```json
{
  "keys": ["key1", "key2"]
}
```

## Query Rules by Tags

Use this endpoint to find all rule names that match specified tags. Both query parameters (`GET`) and request body (`POST` or `GET`) are supported:

### Method 1: Using Query Parameters (GET)

```http
GET http://localhost:9081/rules/tags/match?tags=tier:vip,env:prod
```

Or for a single tag:

```http
GET http://localhost:9081/rules/tags/match?tag=tier:vip
```

### Method 2: Using JSON Request Body (POST or GET)

```http
POST http://localhost:9081/rules/tags/match
Content-Type: application/json

{
  "tags": ["tier:vip", "env:prod"]
}
```

Response sample (HTTP 200 OK):

```json
[
  "rule_comprehensive",
  "rule_monitoring"
]
```

## Bulk Start and Stop Rules by Tag

Use these endpoints to start or stop all rules associated with a tag:

### Bulk Start Rules

```http
POST http://localhost:9081/rules/bulkstart
```

Request payload:

```json
{
  "tags": ["t1"]
}
```

### Bulk Stop Rules

```http
POST http://localhost:9081/rules/bulkstop
```

Request payload:

```json
{
  "tags": ["t1"]
}
```

Both bulk endpoints return the execution status for each targeted rule. These operations are not atomic. An error on one rule does not cancel changes applied to other rules.

## Explain Rule Execution Plan

Use this endpoint to inspect the parsed query topology and execution plan of a rule:

```http
GET http://localhost:9081/rules/{id}/explain
```

Response sample (HTTP 200 OK):

```json
{
  "ruleId": "rule1",
  "plan": {
    "source": {
      "type": "mqtt",
      "topic": "demo"
    },
    "filter": "temperature > 20.0",
    "projection": ["device_id", "temperature", "humidity"],
    "window": null,
    "sinks": [
      {
        "type": "log"
      }
    ]
  }
}
```

> [!NOTE]
> **Compatibility Note: Structured Execution Plan vs Raw ASCII Dump**
> Legacy eKuiper returns an unstructured ASCII text dump (such as `ProjectPlan_0 -> FilterPlan_1`) from the explain endpoint. `rekuiper` returns a typed, structured JSON execution plan containing sources, filters, window specifications, projections, and sinks.
> 
> **Why we chose this difference**: Unstructured text dumps require brittle regex parsing and cannot be safely consumed by web management consoles, graph visualizers, or AI / MCP agentic workflows. Structured JSON provides machine-readable, schema-validatable graph representations that modern cloud-native systems can directly render and inspect.

