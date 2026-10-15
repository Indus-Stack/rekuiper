# Process Protocol Buffers Data

rekuiper connects external systems through source and sink connectors using protocols such as MQTT and HTTP. In addition to standard JSON and delimiter-separated text, rekuiper natively supports Protocol Buffers (Protobuf) binary serialization with compact payload size and fast processing speed.

This tutorial explains how to register Protobuf schemas, decode incoming binary messages in sources, and encode output messages in sinks using the `kuiper` CLI and REST API.

---

## Prerequisites

Prepare the following components before you begin:
- An MQTT broker reachable at `tcp://127.0.0.1:1883` (such as Eclipse Mosquitto). When running rekuiper in Docker, configure the broker address through the environment variable `MQTT_SOURCE__DEFAULT__SERVER="tcp://127.0.0.1:1883"`.
- An MQTT client tool such as Mosquitto CLI or [MQTTX](https://mqttx.app/).

---

## 1. Schema Registry

Protobuf serialization requires a `.proto` schema definition. The following schema defines a `Book` message containing a `title` string and an integer `price`:

```protobuf
syntax = "proto3";

message Book {
  string title = 1;
  int32 price = 2;
}
```

### Register the Schema via REST API

Register the schema named `schema1` using the REST schema management API:

```http
POST /schemas/protobuf
Content-Type: application/json

{
  "name": "schema1",
  "content": "syntax = \"proto3\";\n\nmessage Book {\n  string title = 1;\n  int32 price = 2;\n}"
}
```

Or via CLI:

```bash
kuiper create schema protobuf schema1 -f /path/to/book.proto
```

Verify that the schema is registered:

```http
GET /schemas/protobuf/schema1
```

---

## 2. Ingest and Decode Protobuf Data

This section demonstrates how to ingest and decode Protobuf binary messages from an MQTT topic into structured SQL stream columns.

### Step 1: Create the Stream with Protobuf Format

Create a stream pointing to `schema1` and message type `Book`:

```bash
kuiper create stream protoDemo '() WITH (TYPE="mqtt", DATASOURCE="protoDemo", FORMAT="protobuf", SCHEMAID="schema1.Book")'
```

Or via REST:

```http
POST /streams
Content-Type: application/json

{
  "sql": "CREATE STREAM protoDemo () WITH (TYPE=\"mqtt\", DATASOURCE=\"protoDemo\", FORMAT=\"protobuf\", SCHEMAID=\"schema1.Book\")"
}
```

### Step 2: Create a Rule to Process Decoded Data

Create a rule that transforms the decoded data and publishes results to topic `result/protobuf`:

```http
POST /rules
Content-Type: application/json

{
  "id": "ruleDecode",
  "sql": "SELECT title, price FROM protoDemo WHERE price > 50",
  "actions": [
    {
      "mqtt": {
        "server": "tcp://127.0.0.1:1883",
        "topic": "result/protobuf",
        "sendSingle": true
      }
    },
    {
      "log": {}
    }
  ]
}
```

### Step 3: Verify Decoding

1. Subscribe to `result/protobuf`:
   ```bash
   mosquitto_sub -h 127.0.0.1 -t "result/protobuf"
   ```

2. Publish binary hex data corresponding to a `Book` (e.g. `title: "Rust Systems", price: 65`):
   ```bash
   echo -en '\x0a\x0c\x52\x75\x73\x74\x20\x53\x79\x73\x74\x65\x6d\x73\x10\x41' | mosquitto_pub -h 127.0.0.1 -t "protoDemo" -s
   ```

3. The subscriber receives the decoded JSON output:
   ```json
   {"title": "Rust Systems", "price": 65}
   ```

---

## 3. Encode and Write Protobuf Data

You can also read incoming JSON records, process them in SQL, and serialize output payloads as binary Protobuf messages to minimize outbound network bandwidth:

### Step 1: Create an Input Stream

```bash
kuiper create stream jsonStream '() WITH (TYPE="mqtt", DATASOURCE="events/json", FORMAT="json")'
```

### Step 2: Create a Rule with Protobuf Serialization in Sink Action

```http
POST /rules
Content-Type: application/json

{
  "id": "ruleEncode",
  "sql": "SELECT title, price FROM jsonStream",
  "actions": [
    {
      "mqtt": {
        "server": "tcp://127.0.0.1:1883",
        "topic": "result/protobufOut",
        "format": "protobuf",
        "schemaId": "schema1.Book",
        "sendSingle": true
      }
    }
  ]
}
```

### Step 3: Verify Binary Output

1. Subscribe to `result/protobufOut` using an MQTT client.
2. Publish a JSON message to `events/json`:
   ```bash
   mosquitto_pub -h 127.0.0.1 -t "events/json" -m '{"title":"Edge Guide","price":80}'
   ```
3. The subscriber on `result/protobufOut` receives the binary Protobuf serialized payload.

---

## Summary

rekuiper supports Protobuf serialization across all source and sink connectors. Define your schema in the Schema Registry, and reference the schema in stream and action definitions to encode and decode binary payloads.

## Further Reading

- [Codecs and Serialization](./serialization.md)
- [Schema Management REST API](../../api/restapi/schemas.md)
- [Schema Management CLI](../../api/cli/schemas.md)
