# Contributing to rekuiper

Thank you for considering contributing to `rekuiper`! Your involvement is essential in making this high-performance stream processing engine the fastest and most reliable edge analytics engine in the ecosystem.

---

## Table of Contents
- [Code of Conduct](#code-of-conduct)
- [How to Contribute](#how-to-contribute)
  - [Reporting Bugs](#reporting-bugs)
  - [Reporting Security Vulnerabilities](#reporting-security-vulnerabilities)
- [Development Setup](#development-setup)
  - [Prerequisites](#prerequisites)
  - [Fork and Clone](#fork-and-clone)
  - [Workspace Crates Architecture](#workspace-crates-architecture)
- [Building and Running](#building-and-running)
  - [Building from Source](#building-from-source)
  - [Running the Server Daemon](#running-the-server-daemon)
  - [Using the CLI Client](#using-the-cli-client)
- [Testing & Quality Standards](#testing--quality-standards)
  - [Running Workspace Tests](#running-workspace-tests)
  - [Running Performance Benchmarks](#running-performance-benchmarks)
  - [Code Formatting & Linting](#code-formatting--linting)
- [Submitting a Pull Request](#submitting-a-pull-request)
  - [Commit Guidelines & DCO Sign-off](#commit-guidelines--dco-sign-off)
- [Licensing](#licensing)

---

## Code of Conduct

All participants in the rekuiper project must follow professional conduct. Treat all contributors with respect.

---

## How to Contribute

### Reporting Bugs
- Search existing [GitHub Issues](https://github.com/Indus-Stack/rekuiper/issues) before you open a new issue.
- Provide a clear title, reproduction steps, system environment (OS, CPU, Rust version), and sample JSON rules or stream definitions.

### Reporting Security Vulnerabilities
Please do **not** file public issues for security vulnerabilities. Review [SECURITY.md](./SECURITY.md) for private reporting procedures.

---

## Development Setup

### Prerequisites
- **Rust Toolchain**: Rust 1.78 or newer (`rustup toolchain install stable`).
- **Cargo Components**: `clippy` and `rustfmt`:
  ```bash
  rustup component add clippy rustfmt
  ```

### Fork and Clone
```bash
git clone https://github.com/<your-username>/rekuiper.git
cd rekuiper
git remote add upstream https://github.com/Indus-Stack/rekuiper.git
```

### Workspace Crates Architecture

`rekuiper` is organized into a modular Cargo workspace containing 8 crates:

| Crate | Purpose |
| :--- | :--- |
| `crates/rekuiper-core` | Core streaming runtime primitives, rule data models, `StreamRecord`, and bus abstractions |
| `crates/rekuiper-sql` | SQL lexer, AST parser, tumbling/hopping/sliding/count window engine, scalar functions |
| `crates/rekuiper-conf` | Configuration loader (`etc/kuiper.yaml`), environment overrides, and schema definitions |
| `crates/rekuiper-connectors`| High-speed connectors: MQTT, Kafka, Redis, WebSocket, SQL, HTTP Pull/Push, File, Memory |
| `crates/rekuiper-server` | REST API, 100% OpenAPI 3.0 route handlers, Prometheus metrics server, pipeline runners |
| `crates/rekuiper-cli` | Command-line client (`kuiper`) drop-in replacement |
| `crates/rekuiper-mcp` | Model Context Protocol server exposing AI assistant tools for validation and control |
| `crates/kuiperd` | Server daemon executable entrypoint (`kuiperd`) |

---

## Building and Running

### Building from Source

```bash
# Build in debug mode
cargo build

# Build fully optimized release binaries
cargo build --release

# Binaries will be located at:
# target/release/kuiperd
# target/release/kuiper
```

Or use the provided `Makefile`:
```bash
make build
```

### Running the Server Daemon

```bash
# Run server with local configuration
./target/release/kuiperd --etc etc

# Or via cargo directly
cargo run --bin kuiperd -- --etc etc
```

The server starts listening on `http://0.0.0.0:9081`. Prometheus metrics are exposed at `http://0.0.0.0:20499/metrics` (or `http://0.0.0.0:9081/metrics`).

### Using the CLI Client

```bash
# View CLI help
./target/release/kuiper --help

# Create a stream
./target/release/kuiper create stream demo '() WITH (FORMAT="json")'

# Query stream status
./target/release/kuiper get stream demo
```

---

## Testing & Quality Standards

All pull requests must pass the complete test suite and adhere to strict zero-warning policies.

### Running Workspace Tests

```bash
# Run all unit and integration tests across all 8 workspace crates
cargo test --workspace
```

### Running Performance Benchmarks

Ensure your changes maintain the 180,000+ events/sec throughput baseline:

```bash
cargo test --test perf_throughput -- --nocapture
```

### Code Formatting & Linting

Before pushing code:

```bash
# Format code
cargo fmt --all

# Check clippy linter (must pass with zero warnings)
cargo clippy --workspace --all-targets -- -D warnings
```

---

## Submitting a Pull Request

1. Create a descriptive feature branch: `git checkout -b feat/my-new-feature`.
2. Ensure `cargo test --workspace` passes cleanly.
3. Ensure `cargo fmt --all -- --check` and `cargo clippy --workspace` pass.
4. Keep git history clean with atomic, meaningful commits.

### Commit Guidelines & DCO Sign-off

`rekuiper` enforces the Developer Certificate of Origin (DCO). All commits must be signed off:

```bash
git commit -s -m "feat(connectors): add support for custom header transformations"
```

---

## Licensing

`rekuiper` is open source software released under the [MIT License](LICENSE) (or at your option, the [Apache License, Version 2.0](LICENSE-APACHE)). Any code contributed must be compatible with this permissive licensing model.
