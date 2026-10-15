# Serialization

rekuiper uses an internal structured representation during stream computation. Source and sink connectors communicating with external systems require codecs to convert data formats. Specify the encoding and decoding configuration by setting `format` and optional `schemaId` in source or sink parameters.

---

## Supported Formats

rekuiper includes native support for schema-based and schemaless serialization formats: `json`, `protobuf`, `binary`, `delimited`, and `parquet`.

| Format | Codec Engine | Schema Definition | Primary Use Case |
| :--- | :--- | :--- | :--- |
| `json` | Built-in (`serde_json`) | Schemaless | Web APIs, MQTT payloads, general telemetry |
| `protobuf` | Built-in (Protobuf codec) | Schema-based (`*.proto`) | High-efficiency compact binary streams |
| `binary` | Built-in | Raw bytes | Image payloads, unparsed sensor blobs |
| `delimited` | Built-in | Custom delimiter | CSV, TSV, log lines |
| `parquet` | Built-in (Apache Arrow) | Automatic schema inference | Edge columnar archiving, Snappy compressed storage |

---

## 1. JSON Format

JSON is the default serialization format for rekuiper streams. Ingested payloads are parsed into dynamic key-value maps:

```json
{
  "mqtt": {
    "server": "tcp://127.0.0.1:1883",
    "topic": "telemetry",
    "format": "json"
  }
}
```

---

## 2. Protocol Buffers (Protobuf) Format

Protobuf is a strongly typed, compact binary serialization format. Message definitions specify the fields used during encoding and decoding:

```protobuf
syntax = "proto3";

message Book {
  string title = 1;
  int32 price = 2;
}
```

### Configuration

Reference registered schemas using the format `schemaId="<schema_name>.<message_type>"`:

```json
{
  "mqtt": {
    "server": "tcp://127.0.0.1:1883",
    "topic": "sample",
    "format": "protobuf",
    "schemaId": "proto1.Book"
  }
}
```

For a step-by-step walkthrough, refer to the [Protobuf Tutorial](./protobuf_tutorial.md).

---

## 3. Delimited (CSV/TSV) Format

For comma-separated or tab-separated payloads, specify `format="delimited"` along with the delimiter character:

```sql
CREATE STREAM csvStream () WITH (
    TYPE = "file",
    PATH = "data/sensor.csv",
    FORMAT = "delimited",
    DELIMITER = ","
);
```

---

## 4. Apache Parquet Format

`parquet` is an open-source columnar storage format. `rekuiper` includes built-in support for Apache Parquet through Apache Arrow.

Parquet provides high compression ratios and fast analytical scan performance on edge devices.

### Capabilities

- **Columnar Storage**: Data is organized into row groups and columns.
- **Batched Row Groups**: The writer batches records before flushing row groups to disk for high write throughput.
- **Snappy Compression**: `rekuiper` compresses Parquet files with Snappy by default.
- **Automatic Schema Inference**: The engine creates the Arrow schema automatically from your stream data. You do not need to register a schema file.
- **Metadata Sanitization**: Removes internal runtime metadata keys automatically from the Parquet schema.

### File Sink Example

To write stream outputs to a Parquet file, set `format` to `"parquet"` in the file action:

```json
{
  "file": {
    "path": "/kuiper/data/telemetry.parquet",
    "format": "parquet",
    "rollingCount": 1000
  }
}
```

### File Source Example

To read records from a Parquet file, create a stream with `FORMAT = "PARQUET"`:

```sql
CREATE STREAM parquet_history () WITH (
    TYPE = "file",
    FILETYPE = "parquet",
    PATH = "/kuiper/data/telemetry.parquet",
    FORMAT = "PARQUET"
);
```

---

## Schema Registry

Schemas define structured record formats. rekuiper stores schema files in `data/schemas/${type}` (for example, `data/schemas/protobuf`).

During startup, rekuiper scans the schema directory and registers all definitions automatically. Manage schemas at runtime through the Schema Registry API:

- [Protobuf Serialization Tutorial](./protobuf_tutorial.md)
- [Schema Registry REST API](../../api/restapi/schemas.md)
- [Schema Registry CLI](../../api/cli/schemas.md)
