# Serialization

rekuiper uses an internal map-based data structure during stream computation. Source and sink connectors communicating with external systems require codecs to convert data formats. Specify the encoding and decoding configuration by setting `format` and `schemaId` in source or sink parameters.

## Formats

rekuiper supports schema-based and schema-less serialization formats: `json`, `binary`, `delimited`, `protobuf`, and `custom`.

`protobuf` is a schema-based format. You must register the schema before referencing it in a rule.

The following configuration specifies Protobuf serialization for an MQTT sink:

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

rekuiper supports three codec implementations:

1. **Built-in Codecs**: Executed internally without external dependencies (for example, JSON parsing).
2. **Dynamic Schema Codecs**: Parse schema files at runtime (for example, Protobuf reading `*.proto` files).
3. **Static Plugin Codecs**: Use compiled shared libraries (`*.so`) for maximum parsing performance.

The following table summarizes supported formats and their capabilities:

| Format | Codec | Custom Codec | Schema |
|---|---|---|---|
| `json` | Built-in | Unsupported | Unsupported |
| `binary` | Built-in | Unsupported | Unsupported |
| `delimited` | Built-in (specify delimiter) | Unsupported | Unsupported |
| `protobuf` | Built-in | Supported | Supported and required |
| `parquet` | Built-in (Arrow/Snappy) | Unsupported | Inferred automatically |
| `custom` | Not built-in | Supported and required | Supported and optional |

### Format Extensions

You can implement custom codecs and schemas for `custom` and `protobuf` formats by creating Go plugins:

1. Implement the `Converter` interface. The `Encode` method serializes data into a byte array for sinks. The `Decode` method deserializes bytes into map structures for sources:

   ```go
   // Converter converts bytes & map or []map according to the schema
   type Converter interface {
       Encode(d interface{}) ([]byte, error)
       Decode(b []byte) (interface{}, error)
   }
   ```

2. Implement the `SchemaProvider` interface if the format is strongly typed. The method returns a JSON-schema representation used for SQL validation and optimization:

   ```go
   type SchemaProvider interface {
     GetSchemaJson() string
   }
   ```

3. Compile the code into a shared object plugin:

   ```shell
   go build -trimpath --buildmode=plugin -o data/test/myFormat.so internal/converter/custom/test/*.go
   ```

4. Register the schema by using the REST API:

   ```http
   POST http://localhost:9081/schemas/custom
   Content-Type: application/json

   {
     "name": "custom1",
     "soFile": "file:///tmp/custom1.so"
   }
   ```

5. Reference the format in sources or sinks by setting `format="custom"` and `schemaId="custom1"`.

Refer to [myFormat.go](https://github.com/lf-edge/ekuiper/blob/master/internal/converter/custom/test/myformat.go) for a complete sample implementation.

#### Build Format Plugins with Docker

Compile format plugins in an environment matching the target rekuiper binary. Official release images use Debian or Alpine Linux.

- **Debian**: Use the corresponding developer image (for example, `1.8.0-dev`).
- **Alpine**: Use the official Go Alpine image matching the engine version:

1. Create a `Makefile` in your plugin repository. Refer to the [sample project](https://github.com/lf-edge/ekuiper/tree/master/internal/converter/custom/test).
2. Check the `GO_VERSION` argument in the [Docker build file](https://github.com/lf-edge/ekuiper/blob/master/deploy/docker/Dockerfile) (for example, `1.25.4`).
3. Compile the plugin inside the Alpine container:

   ```shell
   cd ${yourProjectLoc}
   docker run --rm -it -v "$PWD":/usr/src/myapp -w /usr/src/myapp golang:1.25.4-alpine sh
   # Inside the container:
   apk add gcc make libc-dev
   make
   ```

4. Locate the compiled `.so` file and register it through the schema registry API.

### Static Protobuf

For high-throughput requirements, compile static Protobuf plugins instead of using dynamic schema parsing:

1. Generate Go code from your `.proto` definition by using `protoc`:

   ```shell
   protoc --go_opt=Mhelloworld.proto=com.main --go_out=. helloworld.proto
   ```

2. Move the generated `helloworld.pb.go` file into your plugin project and set package name to `main`.
3. Create a wrapper struct for each message type. Implement `Encode`, `Decode`, and accessor methods without reflection.
4. Compile the plugin:

   ```shell
   go build -trimpath --buildmode=plugin -o data/test/helloworld.so internal/converter/protobuf/test/*.go
   ```

5. Register the schema by providing both the `.proto` definition and the `.so` binary:

   ```http
   POST http://localhost:9081/schemas/protobuf
   Content-Type: application/json

   {
     "name": "helloworld",
     "file": "file:///tmp/helloworld.proto",
     "soFile": "file:///tmp/helloworld.so"
   }
   ```

6. Reference the registered schema in stream and action definitions.

Refer to the [helloworld protobuf sample](https://github.com/lf-edge/ekuiper/tree/master/internal/converter/protobuf/test) for a full implementation.

### Parquet Format

`parquet` is an open-source columnar storage format. `rekuiper` includes built-in support for Apache Parquet through Apache Arrow.

Parquet provides high compression ratios and fast analytical scan performance on edge devices.

#### Capabilities

- **Columnar Storage**: Data is organized into row groups and columns.
- **Batched Row Groups**: The writer collects 1,024 records before writing row groups to disk for high write throughput.
- **Snappy Compression**: `rekuiper` compresses Parquet files with Snappy by default.
- **Automatic Schema Inference**: The engine creates the Arrow schema automatically from your stream data. You do not need to register a schema file.
- **Metadata Sanitization**: Removes internal runtime metadata keys automatically from the Parquet schema.

#### File Sink Example

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

#### File Source Example

To read records from a Parquet file, create a stream with `FORMAT = "PARQUET"`:

```sql
CREATE STREAM parquet_history () WITH (
    TYPE = "file",
    FILETYPE = "parquet",
    PATH = "/kuiper/data/telemetry.parquet",
    FORMAT = "PARQUET"
);
```

## Schema Registry

Schemas define structured record formats. rekuiper stores schema files in `data/schemas/${type}` (for example, `data/schemas/protobuf`).

During startup, rekuiper scans the schema directory and registers all definitions automatically. Manage schemas at runtime through the Schema Registry API:

- [Schema Registry REST API](../../api/restapi/schemas.md)
- [Schema Registry CLI](../../api/cli/schemas.md)
