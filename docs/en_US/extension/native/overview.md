# Native Extensions and In-Process Rust UDFs

rekuiper provides an extensible user-defined function (UDF) and plugin subsystem implemented in `rekuiper-core` (`crates/rekuiper-core/src/plugin.rs`).

::: tip Recommended Extension Mechanisms
For dynamic extensions without recompiling the engine binary, use **[WebAssembly (Wasm)](../wasm/overview.md)** or **[External Services](../external/external_func.md)**. These mechanisms replace fragile dynamic `.so` libraries with memory-isolated bytecode and standardized RPC interfaces.
:::

---

## Architectural Principles

Legacy eKuiper used Go C-shared dynamic plugins (`.so`). rekuiper is implemented in 100% Rust to achieve zero garbage-collection jitter and sub-millisecond tail latency. **rekuiper does not load legacy Go dynamic plugins.**

Instead, native extensions in rekuiper operate on two distinct levels:
1. **In-Process Rust Scalar Handlers (`UdfFn`)**: Pure Rust closures registered directly into the engine runtime.
2. **Plugin Catalog Metadata (`PluginDefinition`)**: Persistent metadata tracking installed plugins, schemas, and function lists across node restarts.

---

## In-Process Rust UDF Interface

The primary mechanism for executing custom native code is the `PluginManager` scalar UDF registry:

```rust
use serde_json::Value;
use std::sync::Arc;

/// A scalar UDF handler: pure function from evaluated arguments to a value.
pub type UdfFn = Arc<dyn Fn(&[Value]) -> Value + Send + Sync>;
```

### Registering Custom Functions in Rust

When embedding rekuiper or extending the engine binary, register custom functions with `get_global_udf_registry()`:

```rust
use rekuiper_core::plugin::get_global_udf_registry;
use serde_json::{json, Value};
use std::sync::Arc;

fn setup_custom_udfs() {
    let registry = get_global_udf_registry();

    // Register a custom scalar multiplier
    registry.register_udf("my_multiply", Arc::new(|args: &[Value]| {
        if args.len() < 2 {
            return Value::Null;
        }
        let a = args[0].as_f64().unwrap_or(0.0);
        let b = args[1].as_f64().unwrap_or(0.0);
        json!(a * b)
    }));

    // Register a string formatter
    registry.register_udf("device_tag", Arc::new(|args: &[Value]| {
        let id = args.get(0).and_then(|v| v.as_str()).unwrap_or("unknown");
        json!(format!("edge-device:{}", id))
    }));
}
```

### Case-Insensitive Function Resolution

The SQL evaluator resolves function names case-insensitively. A function registered as `my_multiply` can be invoked in SQL as `MY_MULTIPLY(...)` or `my_multiply(...)`.

---

## Plugin Definition and Metadata Persistence

The `PluginManager` persists plugin declarations across engine restarts using the pluggable KV storage layer (under the `"plugins"` namespace):

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginDefinition {
    pub name: String,
    pub plugin_type: String, // "function", "source", "sink", "udf"
    pub file: Option<String>,
    pub description: Option<String>,
    pub functions: Vec<String>,
}
```

### Persistence Guarantees

- **Definitions Persist**: Plugin definitions registered via REST or configuration are serialized to SQLite/KV storage and reloaded upon engine bootstrap via `load_from_kv`.
- **Code Handlers Must Be Bootstrapped**: Because closures (`UdfFn`) and native symbols represent compiled code, the host process re-registers the executable handlers during application startup.

---

## Comparison of Extensibility Models

| Feature | In-Process Rust UDFs | WebAssembly (Wasm) | External Services |
| :--- | :--- | :--- | :--- |
| **Implementation** | Pure Rust closure (`UdfFn`) | Bytecode module (`wasmi`) | Remote HTTP / gRPC process |
| **Execution Speed** | Native machine speed (zero IPC) | Near-native interpreted Wasm | Network / Unix domain socket RPC |
| **Isolation** | Runs inside host process space | Sandboxed linear memory | Separate OS process |
| **Hot Reloading** | Process restart / re-registration | REST API dynamic registration | Independent service deployment |
| **Language Support** | Rust | Rust, C, C++, Zig | Any (Python, Go, Node.js, etc.) |

For sandboxed dynamic deployment without rebuilding the binary, use **[WebAssembly (Wasm)](../wasm/overview.md)** or **[External Services](../external/external_func.md)**.
