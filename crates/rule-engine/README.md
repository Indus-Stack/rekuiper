# rule-engine

[![Crates.io](https://img.shields.io/crates/v/rule-engine.svg)](https://crates.io/crates/rule-engine)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](https://github.com/Indus-Stack/rekuiper)
[![Documentation](https://img.shields.io/badge/docs-rekuiper.indramqtt.com-blue)](https://rekuiper.indramqtt.com)

**rule-engine** (`rekuiper`) is a high-performance, lightweight edge stream processing engine and MQTT rule engine written in Rust. It functions as a drop-in replacement for LF Edge eKuiper (`kuiperd`), delivering sub-millisecond rule evaluation with ultra-low memory overhead on Linux, macOS, Windows, and embedded IoT architectures.

---

## Quickstart

### 1. Install via Cargo

```bash
cargo install rule-engine
```

*Or install pre-compiled binaries instantly:*
```bash
cargo binstall rule-engine
```

### 2. Scaffold Default Configuration

Initialize the configuration files (`kuiper.yaml`, `mqtt_source.yaml`, `client.yaml`):

```bash
rule-engine init
```

### 3. Start the Daemon

```bash
rule-engine
```

The daemon starts:
- **REST API**: `http://127.0.0.1:9081`
- **Embedded Web Console**: `http://127.0.0.1:9081/ui` (when enabled with `--ui`)
- **CLI RPC Port**: `20498`

---

## Use as an Embedded Library in Rust

You can also embed the streaming SQL processor and rule engine directly inside your own Rust application:

```toml
[dependencies]
rule-engine = "1.0.0-rc4"
```

```rust
use rule_engine::sql::Parser;

fn main() -> anyhow::Result<()> {
    // Parse streaming SQL queries with windowing, filters, and aggregations
    let mut parser = Parser::new("SELECT temperature, humidity FROM sensor WHERE temperature > 50");
    let stmt = parser.parse_select()?;
    println!("Parsed query: {:?}", stmt);
    Ok(())
}
```

Or depend exclusively on the lightweight SQL parsing & execution engine:
```toml
[dependencies]
rekuiper-sql = "1.0.0-rc4"
```

---

## Binary Names

Installing `rule-engine` installs three executable aliases:
1. `rule-engine`
2. `kuiperd` (100% LF Edge eKuiper drop-in compatibility)
3. `rekuiperd`

---

## Documentation & Community

- **Official Documentation**: [https://rekuiper.indramqtt.com](https://rekuiper.indramqtt.com)
- **Source Repository**: [https://github.com/Indus-Stack/rekuiper](https://github.com/Indus-Stack/rekuiper)
- **Docker Images**: `indusstack/rekuiper:latest`

## License

Licensed under either of:
- Apache License, Version 2.0 ([LICENSE-APACHE](https://github.com/Indus-Stack/rekuiper/blob/main/LICENSE-APACHE))
- MIT license ([LICENSE-MIT](https://github.com/Indus-Stack/rekuiper/blob/main/LICENSE-MIT))
