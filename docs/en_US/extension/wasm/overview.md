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
