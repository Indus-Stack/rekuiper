# Extension Architecture

rekuiper provides a modular extension framework designed for ultra-high throughput, predictable sub-millisecond execution, and safe sandboxing on resource-constrained edge hardware.

---

## Extension Models in rekuiper

| Mechanism | Purpose | Supported Technologies | Isolation Model | Documentation |
| :--- | :--- | :--- | :--- | :--- |
| **Built-in Connectors** | Core streaming sources and sinks (MQTT, Kafka, SQL, Redis, WebSocket, File, RabbitMQ) | Compiled into Rust binary | In-process native | [Connectors Guide](../guide/connector.md) |
| **WebAssembly (Wasm)** | High-performance, sandboxed analytical functions and scalar UDFs | Rust, C, C++, Zig compiled to `.wasm` | Isolated bytecode sandbox (`wasmi`) | [WebAssembly Guide](./wasm/overview.md) |
| **In-Process Rust UDFs** | Native scalar handlers registered into the engine runtime | Rust closures (`UdfFn`) | In-process native | [Native Guide](./native/overview.md) |
| **External Services** | Remote microservice or AI inference endpoints (REST / gRPC) | Any language (FastAPI, Triton, etc.) | Independent network process | [External Services Guide](./external/external_func.md) |
| **Model Context Protocol (MCP)** | AI assistant control, offline SQL AST validation, pipeline inspection | Rust JSON-RPC stdio daemon | Isolated tool process | [MCP Overview](../mcp/overview.md) |

::: info Architecture Note
Legacy Go C-shared dynamic plugins (`.so`) and out-of-process IPC portable plugins from upstream eKuiper are unsupported. All high-demand streaming connectors are compiled directly into the rekuiper binary, eliminating external toolchain dependencies and glibc/musl version drift.
:::

---

## 1. WebAssembly (Wasm) Functions

[WebAssembly (Wasm)](./wasm/overview.md) provides sandboxed, high-performance function extensions. You can write algorithms in Rust, C, C++, or Zig and compile them into portable `.wasm` binaries targeting `wasm32-unknown-unknown`:

- **Safe Execution**: Linear memory sandboxing protects the core streaming engine from memory corruption and segfaults.
- **Embedded Engine**: Built-in `wasmi` interpreter requires zero external runtimes or system libraries.
- **Dynamic Deployment**: Register modules at runtime via `POST /plugins/wasm` with base64 bytecode or local file paths.

```sql
SELECT
  deviceId,
  scale_temperature(temperature) AS temp_fahrenheit
FROM
  sensor_stream;
```

Refer to the [WebAssembly Extension Guide](./wasm/overview.md).

---

## 2. In-Process Rust UDFs

For users embedding rekuiper or compiling custom engine distributions, `rekuiper-core` provides the [`PluginManager`](./native/overview.md) and global UDF registry.

Custom scalar functions can be registered directly as Rust closures (`Arc<dyn Fn(&[Value]) -> Value + Send + Sync>`) offering maximum execution performance with zero serialization overhead:

```rust
use rekuiper_core::plugin::get_global_udf_registry;
use serde_json::json;
use std::sync::Arc;

let registry = get_global_udf_registry();
registry.register_udf("scale", Arc::new(|args| {
    let v = args.first().and_then(|x| x.as_f64()).unwrap_or(0.0);
    json!(v * 100.0)
}));
```

Refer to the [Native Extension Guide](./native/overview.md).

---

## 3. External Services (REST and gRPC)

To invoke machine learning models (such as ONNX Runtime, PyTorch, or TensorFlow Lite) or query remote databases, configure an external service interface. rekuiper handles serializing SQL inputs and mapping the response back to stream columns:

```sql
SELECT
  deviceId,
  predict_defect(vibration, temperature)->score AS defect_risk
FROM
  telemetry_stream;
```

Refer to the [External Services Guide](./external/external_func.md).

---

## 4. Model Context Protocol (MCP)

For developer productivity and AI-assisted workflow automation, rekuiper includes a native [Model Context Protocol (MCP)](../mcp/overview.md) server (`rekuiper-mcp`).

It allows AI assistants (Cursor, Claude Desktop, Antigravity) to:
- Parse and validate streaming SQL ASTs offline without network overhead.
- Test SQL transformations in memory against mock event payloads.
- List, inspect, and deploy stream definitions and rule topologies.
- Query engine metrics and trace event latency across operator nodes.
