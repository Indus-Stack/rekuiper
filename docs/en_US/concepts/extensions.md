# Extensions

rekuiper includes built-in sources, sinks, and SQL functions. For specialized integrations, domain-specific calculations, and machine learning inference, rekuiper provides a modular extension framework.

---

## Extension Mechanisms

rekuiper supports four modern extension mechanisms designed for high performance, portability, and memory safety:

| Mechanism | Description | Supported Technologies | Link |
| :--- | :--- | :--- | :--- |
| **WebAssembly (Wasm)** | High-performance in-process functions compiled to WebAssembly bytecode. | Rust, C, C++, Zig (`wasm32-unknown-unknown`) | [WebAssembly Guide](../extension/wasm/overview.md) |
| **In-Process Rust UDFs** | Native scalar closures registered into the global engine catalog. | Pure Rust (`UdfFn`) | [Native Extension Guide](../extension/native/overview.md) |
| **External Services** | Functions backed by external microservices or AI models over HTTP REST or gRPC. | Python, Go, Node.js, C++, Rust | [External Services Guide](../extension/external/external_func.md) |
| **Model Context Protocol (MCP)** | Embedded tool server for AI assistants, AST validation, and pipeline testing. | Native JSON-RPC stdio daemon | [MCP Overview](../mcp/overview.md) |

---

## Extension Points

1. **Source Connectors**: Ingest data from external protocols into streams or tables. Core connectors (Kafka, SQL, Redis, WebSocket, File, MQTT, HTTP, RabbitMQ) are compiled directly into the binary.
2. **Sink Connectors**: Deliver processed records to external destinations.
3. **Analytical Functions**: Transform and aggregate streaming records using SQL.

---

## Further Reading

- [Extension Architecture Overview](../extension/overview.md)
