# WebAssembly (Wasm) Functions

rekuiper embeds a lightweight WebAssembly execution runtime built on [`wasmi`](https://github.com/wasmi-labs/wasmi). WebAssembly plugins provide sandboxed, in-process user-defined functions (UDFs) that execute with memory isolation on resource-constrained edge hardware.

---

## Architecture and Execution Model

The WebAssembly engine in `rekuiper-core` (`crates/rekuiper-core/src/wasm.rs`) pre-validates and instantiates Wasm bytecode at registration time.

### Calling Conventions and Type Conversions

Wasm functions export standard C-ABI scalar signatures. The runtime handles conversion between SQL JSON values (`serde_json::Value`) and Wasm value types:

| SQL / JSON Type | Wasm ValType | Coercion Rules |
| :--- | :--- | :--- |
| `BIGINT`, `INTEGER` | `i64` / `i32` | Cast directly to integer width. |
| `FLOAT` | `f64` / `f32` | Cast directly to floating-point width. |
| Return Value | `i32`, `i64`, `f32`, `f64` | Converted back to standard JSON numeric scalar. |

Functions returning `void` (no return value) produce `Value::Null` in the SQL stream.

---

## Developing a Guest Wasm Plugin in Rust

Any programming language that compiles to `wasm32-unknown-unknown` without host dependencies can be used. Rust is recommended.

### 1. Create the Library Project

```bash
cargo new --lib math_udf
cd math_udf
```

### 2. Configure `Cargo.toml`

Configure the library crate type as a C-compatible dynamic library (`cdylib`):

```toml
[package]
name = "math_udf"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
```

### 3. Implement Exported Functions in `src/lib.rs`

Export functions using standard `extern "C"` linkage and disable symbol name mangling:

```rust
#[no_mangle]
pub extern "C" fn add_numbers(a: i64, b: i64) -> i64 {
    a + b
}

#[no_mangle]
pub extern "C" fn scale_temperature(celsius: f64) -> f64 {
    celsius * 1.8 + 32.0
}

#[no_mangle]
pub extern "C" fn fib(n: i32) -> i32 {
    if n <= 1 {
        return n;
    }
    let mut a = 0;
    let mut b = 1;
    for _ in 2..=n {
        let temp = a + b;
        a = b;
        b = temp;
    }
    b
}
```

### 4. Compile to `wasm32-unknown-unknown`

```bash
rustup target add wasm32-unknown-unknown
cargo build --target wasm32-unknown-unknown --release
```

The compiled bytecode is located at `target/wasm32-unknown-unknown/release/math_udf.wasm`.

---

## Registering and Managing Wasm Plugins

rekuiper provides both filesystem autoloading and REST API management.

### Filesystem Autoloading

Place `.wasm` files in one of the scanned plugin directories. When a SQL query references an unknown function, rekuiper automatically inspects and loads matching modules:
- `plugins/wasm/<module_name>.wasm`
- `etc/plugins/wasm/<module_name>.wasm`
- `data/<module_name>.wasm`

### REST API Management

#### 1. Register via Base64 Bytecode

```http
POST /plugins/wasm
Content-Type: application/json

{
  "name": "math_udf",
  "bytecode": "AGFzbQEAAAA..."
}
```

#### 2. Register via File Path

```http
POST /plugins/wasm
Content-Type: application/json

{
  "name": "math_udf",
  "file": "file:///kuiper/plugins/wasm/math_udf.wasm"
}
```

**Response (`201 Created`):**
```json
{
  "name": "math_udf",
  "functions": ["add_numbers", "fib", "scale_temperature"]
}
```

All exported functions are immediately registered in the global SQL UDF catalog.

#### 3. List Registered Wasm Modules

```http
GET /plugins/wasm
```

**Response (`200 OK`):**
```json
[
  "math_udf"
]
```

#### 4. Delete a Wasm Module

```http
DELETE /plugins/wasm/math_udf
```

**Response (`200 OK`)**

Unloads the module and removes all its exported functions from the active SQL catalog.

---

## Sandbox Fuel Metering and Instance Pooling

The `rekuiper` WebAssembly runtime enforces deterministic execution limits and lock-free concurrency across streaming rules:

1. **Deterministic Fuel Metering (`DEFAULT_WASM_FUEL_BUDGET = 1,000,000`)**: Every WASM UDF invocation runs with `wasmi` fuel consumption enabled and a strict per-call budget of `1,000,000` instructions. A buggy or malicious `.wasm` module containing an infinite `loop {}` traps deterministically once its fuel budget is exhausted instead of starving Tokio worker threads.
2. **Lock-Free Instance Pooling (`runtime_pool`)**: Compiled `wasmi::Module` definitions maintain a pool of pre-instantiated `WasmRuntimeInstance` states (`Store` + `Instance`). Concurrent SQL rules check out an instance, execute the UDF outside the registry lock, and return the healthy instance to the pool upon completion.

<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 860 200" width="100%" height="auto" role="img" aria-label="WebAssembly Sandbox Fuel Metering and Instance Pool Architecture" font-family="-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif">
  <rect x="0" y="0" width="860" height="200" rx="10" fill="#f8fafc" stroke="#e2e8f0" stroke-width="1" />
  <text x="430" y="28" text-anchor="middle" font-size="14" font-weight="700" fill="#0f172a">WebAssembly (wasmi) UDF Sandbox &amp; Instance Pool</text>
  <rect x="25" y="50" width="220" height="125" rx="8" fill="#ffffff" stroke="#cbd5e1" stroke-width="1.5" />
  <text x="135" y="76" text-anchor="middle" font-size="12" font-weight="700" fill="#1e40af">1. Module Registry</text>
  <text x="135" y="98" text-anchor="middle" font-size="11" fill="#334155">POST /plugins/wasm</text>
  <text x="135" y="118" text-anchor="middle" font-size="10" fill="#64748b">Validates .wasm Magic Bytes</text>
  <text x="135" y="136" text-anchor="middle" font-size="10" fill="#64748b">Extracts Exported UDF Symbols</text>
  <text x="135" y="154" text-anchor="middle" font-size="10" fill="#2563eb">Pre-compiles wasmi::Module</text>
  <path d="M 245 112 L 295 112" stroke="#64748b" stroke-width="2" />
  <polygon points="295,112 287,108 287,116" fill="#64748b" />
  <rect x="295" y="50" width="260" height="125" rx="8" fill="#ffffff" stroke="#ea580c" stroke-width="1.5" />
  <text x="425" y="76" text-anchor="middle" font-size="12" font-weight="700" fill="#c2410c">2. Lock-Free Runtime Pool</text>
  <text x="425" y="98" text-anchor="middle" font-size="11" fill="#334155">Pooled WasmRuntimeInstance (Store + Instance)</text>
  <text x="425" y="118" text-anchor="middle" font-size="10" fill="#64748b">Checked Out Outside Global Mutex</text>
  <text x="425" y="136" text-anchor="middle" font-size="10" fill="#059669">Concurrent Multi-Rule SQL Execution</text>
  <text x="425" y="154" text-anchor="middle" font-size="10" fill="#64748b">Recycled on Clean Completion</text>
  <path d="M 555 112 L 605 112" stroke="#64748b" stroke-width="2" />
  <polygon points="605,112 597,108 597,116" fill="#64748b" />
  <rect x="605" y="50" width="230" height="125" rx="8" fill="#ffffff" stroke="#10b981" stroke-width="1.5" />
  <text x="720" y="76" text-anchor="middle" font-size="12" font-weight="700" fill="#047857">3. Deterministic Sandbox</text>
  <text x="720" y="98" text-anchor="middle" font-size="11" fill="#334155">Fuel Budget: 1,000,000 / Call</text>
  <text x="720" y="118" text-anchor="middle" font-size="10" fill="#64748b">Isolated Linear Memory</text>
  <text x="720" y="136" text-anchor="middle" font-size="10" fill="#dc2626">Traps Infinite Loops Instantly</text>
  <text x="720" y="154" text-anchor="middle" font-size="10" fill="#059669">Zero Host Syscall Access</text>
</svg>

---

## Invoking Wasm Functions in SQL

Once registered, you can invoke exported functions directly in SQL rules or through the `wasm_run` helper function:

### Direct Invocation

```sql
SELECT
  deviceId,
  scale_temperature(temperature) AS temp_fahrenheit,
  add_numbers(counter, 10) AS incremented_counter
FROM
  sensor_stream;
```

### Generic `wasm_run` Invocation

```sql
SELECT
  deviceId,
  wasm_run("scale_temperature", temperature) AS temp_fahrenheit
FROM
  sensor_stream;
```
