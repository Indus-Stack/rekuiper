# Data Import and Export Management

The rekuiper REST API manages system-wide configuration import and export operations.

## Data Format

System data files use JSON or YAML formatting. Documents can contain `streams`, `tables`, `rules`, `nativePlugins`, `portablePlugins`, `sourceConfig`, `sinkConfig`, `connectionConfig`, `Service`, `Schema`, `uploads`, and `scripts`.

Example JSON export:

```json
{
    "streams": {
        "demo": "CREATE STREAM demo () WITH (DATASOURCE=\"users\", FORMAT=\"JSON\")"
    },
    "tables": {
      "T110": "\n CREATE TABLE T110\n (\n S1 string\n )\n WITH (DATASOURCE=\"test.json\", FORMAT=\"json\", TYPE=\"file\", KIND=\"scan\", );\n "
    },
    "rules": {
        "rule1": "{\"id\": \"rule1\",\"sql\": \"SELECT * FROM demo\",\"actions\": [{\"log\": {}}]}",
        "rule2": "{\"id\": \"rule2\",\"sql\": \"SELECT * FROM demo\",\"actions\": [{  \"log\": {}}]}"
    },
    "nativePlugins": {
        "functions_image": "{\"name\":\"image\",\"file\":\"https://example.com/plugins/image_amd64.zip\",\"shellParas\":[]}",
        "sources_video": "{\"name\":\"video\",\"file\":\"https://example.com/plugins/video_amd64.zip\",\"shellParas\":[]}"
    },
    "portablePlugins": {},
    "sourceConfig": {
      "mqtt": "{\"td\":{\"insecureSkipVerify\":false,\"password\":\"public\",\"protocolVersion\":\"3.1.1\",\"qos\":1,\"server\":\"tcp://10.0.0.1:1883\",\"username\":\"admin\"},\"test\":{\"insecureSkipVerify\":false,\"password\":\"public\",\"protocolVersion\":\"3.1.1\",\"qos\":1,\"server\":\"tcp://127.0.0.1:1883\",\"username\":\"admin\"}}"
    },
    "sinkConfig": {
      "edgex": "{\"test\":{\"bufferLength\":1024,\"contentType\":\"application/json\",\"enableCache\":false,\"format\":\"json\",\"messageType\":\"event\",\"omitIfEmpty\":false,\"port\":6379,\"protocol\":\"redis\",\"sendSingle\":true,\"server\":\"localhost\",\"topic\":\"application\",\"type\":\"redis\"}}"
    },
    "connectionConfig": {},
    "Service": {},
    "Schema": {},
    "uploads": {},
    "scripts": {
      "area": "{\"id\":\"area\",\"description\":\"calculate area\",\"script\":\"function area(x, y) { return x * y; }\",\"isAgg\":false}"
    }
}
```

## Import Data

By default, the import endpoint resets all existing configuration before importing new definitions. Set `partial=1` as a query parameter to merge new definitions without clearing existing data.

### Example 1: Import by Inline Text

```http
POST http://localhost:9081/data/import
Content-Type: application/json

{
  "content": "{json of the ruleset}"
}
```

### Example 2: Import by File URI

```http
POST http://localhost:9081/data/import
Content-Type: application/json

{
  "file": "file:///tmp/a.json"
}
```

### Example 3: Import and Stop the Server

Use `stop=1` when installing native plugins or schema changes that require a process restart:

```http
POST http://localhost:9081/data/import?stop=1
Content-Type: application/json

{
  "file": "file:///tmp/a.json"
}
```

### Example 4: Partial Import (Merge)

Use `partial=1` to overwrite matching streams, tables, rules, and configurations while retaining unrelated resources:

```http
POST http://localhost:9081/data/import?partial=1
Content-Type: application/json

{
  "file": "file:///tmp/a.json"
}
```

### Example 5: Asynchronous Import

Use this endpoint to import configurations asynchronously. The server creates a task identifier and executes the import in the background:

```http
POST http://localhost:9081/async/data/import
Content-Type: application/json

{
  "content": "$data json content"
}
```

Response sample:

```json
{
  "id": "$taskID"
}
```

#### Validation Rules
- The request body must not be empty.
- If the request body is empty, the server returns status code `400 Bad Request`.
- If the request body is invalid JSON, the server returns status code `400 Bad Request`.
- When validation passes, the server returns status code `200 OK` with a task identifier.

Query the status of an asynchronous background import task:

```http
GET http://localhost:9081/async/task/{id}
```

## Import Data Status

Use this endpoint to inspect errors recorded during the most recent import operation. If all maps in the response are empty, the import completed without errors:

```http
GET http://localhost:9081/data/import/status
```

Response sample when import succeeds:

```json
{
  "streams": {},
  "tables": {},
  "rules": {},
  "nativePlugins": {},
  "portablePlugins": {},
  "sourceConfig": {},
  "sinkConfig": {},
  "connectionConfig": {},
  "Service": {},
  "Schema": {},
  "uploads": {},
  "scripts": {}
}
```

Response sample when plugin download fails:

```json
{
  "streams": {},
  "tables": {},
  "rules": {},
  "nativePlugins": {  
    "sinks_tdengine": "fail to download file file:///root/plugins/sinks/tdengine_amd64.zip: stat /root/plugins/sinks/tdengine_amd64.zip: no such file or directory",
    "sources_random": "fail to download file file:///root/plugins/sources/random_amd64.zip: stat /root/plugins/sources/random_amd64.zip: no such file or directory"
  },
  "portablePlugins": {},
  "sourceConfig": {},
  "sinkConfig": {},
  "connectionConfig": {},
  "Service": {},
  "Schema": {},
  "uploads": {},
  "scripts": {}
}
```

## Export Data

Use these endpoints to download exported system data:

### Export All System Data

```http
GET http://localhost:9081/data/export
```

### Export Data for Specific Rules

Use this endpoint to export only the selected rules along with their dependent streams and tables. Both array and object payload formats are supported:

**Format A: Array of Rule IDs**
```http
POST http://localhost:9081/data/export
Content-Type: application/json

["rule1", "rule2"]
```

**Format B: Object with Rules Key**
```http
POST http://localhost:9081/data/export
Content-Type: application/json

{
  "rules": ["rule1", "rule2"]
}
```

## Import and Export Data in YAML Format (v2)

eKuiper supports importing and exporting configuration definitions using YAML format for enhanced readability.

### Export Configuration in YAML

```http
GET http://localhost:9081/v2/data/export
```

Example response:

```yaml
sourceConfig:
    sources.mqtt.mqttconf1:
        connectionSelector: mqttcon
        qos: 1
        sourceType: stream
connectionConfig:
    connections.mqtt.mqttcon:
        insecureSkipVerify: false
        protocolVersion: 3.1.1
        server: tcp://127.0.0.1:1883
streams:
    mqttstream1:
        sql: ' CREATE STREAM mqttstream1 () WITH (DATASOURCE="topic1", FORMAT="json", CONF_KEY="mqttconf1", TYPE="mqtt", SHARED="false");'
rules:
    rule1:
        triggered: false
        id: rule1
        sql: select * from mqttstream1
        actions:
            - log: {}
```

### Import Configuration in YAML

```http
POST http://localhost:9081/v2/data/import
Content-Type: application/json

{
  "file": "file:///tmp/a.yaml"
}
```
