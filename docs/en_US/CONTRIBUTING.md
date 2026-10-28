# How to Contribute to rekuiper

This document describes how to contribute code, documentation, and tests to the `rekuiper` project.

`rekuiper` is a lightweight stream processing engine for edge devices, written in Rust.

---

## Code of Conduct

All contributors to `rekuiper` must follow professional conduct. Treat all community members with respect and courtesy.

---

## Report Defects and Security Vulnerabilities

### Report a Defect
1. Search existing issues in the [GitHub Issues](https://github.com/Indus-Stack/rekuiper/issues) tracker before you create a new issue.
2. If an issue does not exist, open a [New Issue](https://github.com/Indus-Stack/rekuiper/issues/new).
3. Include these items in your report:
   - Clear title and summary of the defect.
   - Operating system, hardware architecture, and Rust compiler version.
   - Exact steps to reproduce the defect.
   - Stream definitions, SQL queries, and rule configurations.
   - Actual output versus expected output.

### Report a Security Vulnerability
Do **not** report security vulnerabilities in public GitHub issues.

Report security vulnerabilities privately to the **I-Dacs Labs Security Team**:
- Email: **[measure@i-dacs.com](mailto:measure@i-dacs.com)**
- Subject: `[SECURITY] rekuiper vulnerability report`
- Review [SECURITY.md](../../SECURITY.md) for full disclosure details.

---

## Development Setup

### Prerequisites
- **Rust Toolchain**: Rust 1.78 or newer (`rustup toolchain install stable`).
- **Cargo Components**: `clippy` and `rustfmt`:
  ```bash
  rustup component add clippy rustfmt
  ```
- **Optional Services**: Docker (useful to run local test brokers like Mosquitto, Redis, or Kafka).

### Fork and Clone
1. Fork the repository to your GitHub account.
2. Clone your fork locally:
   ```bash
   git clone https://github.com/<your-username>/rekuiper.git
   cd rekuiper
   ```
3. Add the upstream repository remote:
   ```bash
   git remote add upstream https://github.com/Indus-Stack/rekuiper.git
   ```

---

## Cargo Workspace Architecture

`rekuiper` is structured as a modular Cargo workspace containing 10 crates:

| Crate | Directory | Purpose |
| :--- | :--- | :--- |
| `rekuiper-core` | `crates/rekuiper-core` | Core streaming runtime primitives, rule data models, `StreamRecord`, and internal bus. |
| `rekuiper-sql` | `crates/rekuiper-sql` | SQL lexer, AST parser, tumbling/hopping/sliding/count window engine, and scalar functions. |
| `rekuiper-conf` | `crates/rekuiper-conf` | Configuration loader (`etc/kuiper.yaml`), environment variable overrides, and dynamic config. |
| `rekuiper-connectors` | `crates/rekuiper-connectors` | Built-in connectors: MQTT, Kafka, Redis, WebSocket, SQL, HTTP Pull/Push, File, Memory. |
| `rekuiper-server` | `crates/rekuiper-server` | REST API, 100% OpenAPI 3.0 route handlers, Prometheus metrics server, pipeline runner. |
| `rekuiper-cli` | `crates/rekuiper-cli` | Command-line interface (`kuiper`) drop-in client binary. |
| `rekuiper-mcp` | `crates/rekuiper-mcp` | Model Context Protocol server exposing AI assistant tools for rule validation and control. |
| `kuiperd` | `crates/kuiperd` | Server daemon executable entrypoint (`kuiperd` and `rekuiperd`). |
| `rule-engine` | `crates/rule-engine` | High-level embeddable library façade for Rust applications. |
| `ruleengine` | `crates/ruleengine` | Crate alias re-exporting `rule-engine`. |

---

## Building and Running

### Build from Source
Build in debug mode:
```bash
cargo build
```

Build optimized release binaries:
```bash
cargo build --release
```

The compiled binaries are placed in:
- `target/release/kuiperd` (server daemon)
- `target/release/kuiper` (command-line client)
- `target/release/rekuiper-mcp` (MCP server)

### Run the Server Daemon
Start the server with local configuration:
```bash
./target/release/kuiperd --etc etc
```

Or run directly through Cargo:
```bash
cargo run --bin kuiperd -- --etc etc
```

The server listens on `http://127.0.0.1:9081` by default (configurable via `basic.restIp` in `etc/kuiper.yaml`). Prometheus metrics are available at `http://127.0.0.1:20499/metrics`.

### Use the CLI Client
Run client commands against the local daemon:
```bash
# Check version
./target/release/kuiper --version

# Create a stream
./target/release/kuiper create stream demo '() WITH (FORMAT="json", TYPE="mqtt", DATASOURCE="demo/telemetry")'

# List streams
./target/release/kuiper get stream demo
```

---

## Quality and Testing Standards

All contributions must pass automated tests and satisfy zero-warning code quality rules.

### Run Workspace Tests
Run all unit and integration tests:
```bash
cargo test --workspace
```

Run tests for a single crate:
```bash
cargo test -p rekuiper-sql
cargo test -p rekuiper-server --test fvt_compat
```

### Run Performance Benchmarks
Verify that code changes do not degrade streaming throughput:
```bash
cargo test --test perf_throughput -- --nocapture
```

### Code Formatting and Linting
Format the codebase:
```bash
cargo fmt --all
```

Run the Clippy linter with warnings treated as errors:
```bash
cargo clippy --workspace --all-targets -- -D warnings
```

---

## Rust Coding Conventions

### Import Order
Group `use` declarations in this order, separated by a blank line:

1. Standard library (`std::`)
2. External third-party crates (`tokio`, `serde`, `tracing`)
3. Workspace crates (`rekuiper_core`, `rekuiper_sql`)
4. Current crate modules (`crate::`, `super::`)

Example:
```rust
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tracing::{debug, error, info};

use rekuiper_core::model::StreamRecord;

use crate::error::Result;
```

### Error Handling
- Use structured error types defined with `thiserror` for crate APIs.
- Do not call `.unwrap()` or `.expect()` in production code paths.
- Return explicit `Result<T, E>` types.

### Concurrency and Asynchronous Code
- Use `tokio` for asynchronous input/output tasks.
- Keep lock holding times short when using `tokio::sync::Mutex` or `std::sync::RwLock`.
- Prefer message passing with bounded channels (`tokio::sync::mpsc::channel`) over shared mutable state.

### Observability
- Use structured logging with the `tracing` crate (`tracing::info!`, `tracing::debug!`, `tracing::warn!`, `tracing::error!`).
- Do not use `println!` or `eprintln!` in library crates.

---

## Debugging

### Debug with VS Code
Install these extensions:
- `rust-lang.rust-analyzer`
- `vadimcn.vscode-lldb`

Set breakpoints in Rust source files, then launch tests or binaries with the `Debug` lens above test functions.

### Control Log Levels
Enable detailed logging with the `RUST_LOG` environment variable:
```bash
RUST_LOG=debug ./target/release/kuiperd --etc etc
```

---

## Submitting a Pull Request

1. Create a topic branch from the `main` branch:
   ```bash
   git checkout -b feat/my-new-feature upstream/main
   ```
2. Make small, focused changes.
3. Verify that formatting, linting, and tests pass:
   ```bash
   cargo fmt --all -- --check
   cargo clippy --workspace --all-targets -- -D warnings
   cargo test --workspace
   ```
4. Sign off every commit (DCO requirement):
   ```bash
   git commit -s -m "feat(connectors): add support for batch sink flushing"
   ```
5. Follow conventional commit message format:
   - `feat`: New feature
   - `fix`: Defect fix
   - `docs`: Documentation update
   - `perf`: Performance improvement
   - `test`: Test suite update
   - `refactor`: Code restructuring without functional changes
   - `chore`: Build or dependency maintenance
6. Rebase your branch on `upstream/main` before opening your pull request:
   ```bash
   git fetch upstream
   git rebase upstream/main
   git push origin feat/my-new-feature
   ```
7. Open a pull request against the `main` branch on GitHub.

---

## Licensing

`rekuiper` is dual-licensed under:
- **Apache License, Version 2.0** ([LICENSE-APACHE](../../LICENSE-APACHE))
- **MIT License** ([LICENSE-MIT](../../LICENSE-MIT))

By contributing to `rekuiper`, you agree that your contributions will be licensed under these terms.
