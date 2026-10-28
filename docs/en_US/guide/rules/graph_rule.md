# Graph Rules

rekuiper supports SQL queries and graph models to define rule logic. The graph API represents stream processing pipelines as Directed Acyclic Graphs (DAGs) in JSON format. This model maps directly to visual drag-and-drop user interfaces.

The `graph` object contains `nodes` and `topo` properties. `nodes` defines pipeline elements. `topo` defines directed connections (edges) between nodes.

![Directed Acyclic Graph (DAG) Pipeline Topology](/diagrams/topology_dag.svg)

Below is an example of a linear graph rule: `demo` -> `humidityFilter` -> `mqttout`. The rule reads from an MQTT source, filters by humidity, and transmits output to an MQTT sink:

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
        "humidityFilter": ["mqttout"]
      }
    }
  }
}
```

## Node Specification

Each node in the graph JSON contains at least three fields:

- `type`: Node role. Allowed values: `source`, `operator`, and `sink`.
- `nodeType`: Specific functional type. Includes built-in types and plugin extensions.
- `props`: Map of configuration properties for the specified `nodeType`.

### Source Nodes

For source nodes, `nodeType` specifies the connector type, such as `mqtt` or `edgex`. Refer to [Sources](../sources/overview.md) for all supported types.

Properties match standard stream definition parameters. Use `CONF_KEY` to reference preconfigured settings in configuration files.

In the example below, the source node connects to an MQTT broker using JSON formatting:

```json
  {
    "type": "source",
    "nodeType": "mqtt",
    "props": {
      "datasource": "devices/+/messages",
      "format": "json"
    }
  }
```

You must create streams or tables before you reference them in a rule.
- Set `sourceType` to `stream` or `table`.
- Set `sourceName` to the existing stream or table name.
- Verify that `nodeType` matches the connector type of the stream or table.

Example stream source node:

```json
  {
      "type": "source",
      "nodeType": "mqtt",
      "props": {
        "sourceType": "stream",
        "sourceName": "demoStream"
      }
  }
```

You can also reference lookup tables in source nodes. Only lookup tables connect to Join nodes; scan tables are not supported in graph rules.

Example lookup table source node:

```json
  {
      "type": "source",
      "nodeType": "redis",
      "props": {
        "sourceType": "table",
        "sourceName": "demoTable"
      }
  }
```

### Sink Nodes

For sink nodes, `nodeType` specifies the target connector type, such as `mqtt` or `edgex`. Refer to [Sinks](../sinks/overview.md) for supported types and configuration properties.

### Built-in Operator Node Types

rekuiper includes these built-in operator node types:

#### function

Evaluates a scalar function expression. The node outputs a new field with the function name or defined alias.

Properties:
- `expr`: String function expression.

Example:

```json
  {
    "type": "operator",
    "nodeType": "function",
    "props": {
      "expr": "log(temperature) as log_temperature"
    }
  }
```

#### aggfunc

Evaluates an aggregate function over a windowed collection of rows. The node aggregates multiple rows into a single summary row, or one row per group.

Properties:
- `expr`: String aggregate function expression.

Example:

```json
  {
    "type": "operator",
    "nodeType": "aggfunc",
    "props": {
      "expr": "count(*)"
    }
  }
```

#### filter

Filters records based on a boolean condition.

Properties:
- `expr`: Boolean condition expression.

Example:

```json
  {
    "type": "operator",
    "nodeType": "filter",
    "props": {
      "expr": "temperature > 20"
    }
  }
```

#### pick

Selects and projects output fields. Place this node at the end of a workflow to format output records.

Properties:
- `fields`: Array of field selection strings.

Example:

```json
  {
    "type": "operator",
    "nodeType": "pick",
    "props": {
      "fields": ["log_temperature", "humidity", "window_end()"]
    }
  }
```

#### window

Defines a [Window](../../sqls/windows.md) in the workflow. The node accepts individual rows and outputs a collection of rows.

Properties:
- `type`: Window type (`tumblingwindow`, `hoppingwindow`, `slidingwindow`, `sessionwindow`, `countwindow`).
- `unit`: Time unit. Refer to [Time Units](../../sqls/windows.md#time-units).
- `size`: Window duration or record count.
- `interval`: Window trigger interval.

Example:

```json
  {
    "type": "operator",
    "nodeType": "window",
    "props": {
      "type": "hoppingwindow",
      "unit": "ss",
      "size": 10,
      "interval": 5
    }
  }
```

#### join

Joins records from multiple sources. Inputs must be record collections produced by window nodes. The output is a collection of joined tuples.

Properties:
- `from`: Left source node identifier.
- `joins`: Array of join condition objects:
  - `name`: Right source node identifier.
  - `type`: Join type (`inner`, `left`, `right`, `full`, `cross`).
  - `on`: Boolean expression that defines the join condition.

Example stream-to-stream join:

```json
   {
    "type": "operator",
    "nodeType": "join",
    "props": {
      "from": "device1",
      "joins": [
        {
          "name": "device2",
          "type": "inner",
          "on": "abs(device1.ts - device2.ts) < 200"
        }
      ]
    }
  }
```

Join operators support stream-to-stream joins and stream-to-lookup-table joins. Stream-to-scan-table joins are not supported.

Stream-to-stream joins require a preceding window node. Stream-to-lookup-table joins support a single join condition.

Example stream-to-lookup-table join:

```json
   {
    "type": "operator",
    "nodeType": "join",
    "props": {
      "from": "demoStream",
      "joins": [
        {
          "name": "demoTable",
          "type": "inner",
          "on": "deviceStream.id = demoTable.id"
        }
      ]
    }
  }
```

#### groupby

Groups record collections by specified dimension expressions. Inputs must be record collections.

Properties:
- `dimensions`: Array of dimension expressions.

Example:

```json
  {
    "type": "operator",
    "nodeType": "groupby",
    "props": {
      "dimensions": ["device1.humidity"]
    }
  }
```

#### orderby

Sorts records in a window collection. Inputs must be record collections.

Properties:
- `sorts`: Array of sort condition objects:
  - `field`: Field name to sort by.
  - `order`: Sort direction (`asc` or `desc`).

Example:

```json
  {
    "type": "operator",
    "nodeType": "orderby",
    "props": {
      "sorts": [{
        "field": "count",
        "order": "desc"
      }]
    }
  }
```

#### switch

Routes messages to multiple pipeline branches based on conditional expressions.

Properties:
- `cases`: Ordered array of conditional expressions.
- `stopAtFirstMatch`: Boolean flag. If `true`, stops evaluation after the first matching case.

The `edges` definition maps the switch output paths using a two-dimensional array. Each index corresponds to the matching condition in `cases`.

Example switch rule definition:

```json
{
  "id": "ruleSwitch",
  "name": "Demonstrate how to use switch node",
  "graph": {
    "nodes": {
      "abc": {
        "type": "source",
        "nodeType": "mqtt",
        "props": {
          "datasource": "demo",
          "confKey": "syno"
        }
      },
      "switch": {
        "type": "operator",
        "nodeType": "switch",
        "props": {
          "cases": [
            "temperature > 20",
            "temperature <= 20"
          ],
          "stopAtFirstMatch": true
        }
      },
      "mqttpv": {
        "type": "sink",
        "nodeType": "mqtt",
        "props": {
          "server": "tcp://syno.home:1883",
          "topic": "result/switch1",
          "sendSingle": true
        }
      },
      "mqttpv2": {
        "type": "sink",
        "nodeType": "mqtt",
        "props": {
          "server": "tcp://syno.home:1883",
          "topic": "result/switch2",
          "sendSingle": true
        }
      }
    },
    "topo": {
      "sources": [
        "abc"
      ],
      "edges": {
        "abc": [
          "switch"
        ],
        "switch": [
          [
            "mqttpv"
          ],
          [
            "mqttpv2"
          ]
        ]
      }
    }
  }
}
```

#### script

Executes JavaScript logic on messages passing through the node.

Properties:
- `script`: String containing JavaScript code with an `exec` function.
- `isAgg`: Boolean flag.

When `isAgg` is `false`, the node accepts a single record and returns a single record. When `isAgg` is `true`, the node accepts an array of records and returns a processed array.

1. Single record processing example:

   ```json
   {
     "type": "operator",
      "nodeType": "script",
      "props": {
        "script": "function exec(msg, meta) {msg.temperature = 1.8 * msg.temperature + 32; return msg;}"
      }
   }
   ```

2. Window aggregated records processing example:

   ```json
   {
      "type": "operator",
      "nodeType": "script",
      "props": {
        "script": "function exec(msgs) {agg = {value:0}\nfor (let i = 0; i < msgs.length; i++) {\nagg.value = agg.value + msgs[i].value;\n}\nreturn agg;\n}",
        "isAgg": true
      }
   }
   ```

