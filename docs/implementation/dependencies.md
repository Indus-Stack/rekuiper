# Dependency and Registration Discipline (C06)

## Ownership and Governance

- **Single manifest owner:** Only the active `C06` dependency registration step modifies `Cargo.toml`, crate `Cargo.toml` manifests, or `Cargo.lock`. Feature tickets must not rewrite `Cargo.lock` independently.
- **Default policy:** Reuse existing locked workspace dependencies first. Do not promote optional connector/adapter dependencies into `rekuiper-core` or `rekuiper-sql`.
- **Advisory policy:** All workspace dependencies must pass `cargo deny check advisories` against `deny.toml`.

## Crate Isolation Boundaries

1. **`rekuiper-core`** (`crates/rekuiper-core/Cargo.toml`):
   - Core runtime, contracts, SQLite/segmented reliability, and WASM (`wasmi = "0.36"`).
   - Must not depend on `rekuiper-connectors`, `rekuiper-server`, `axum`, `reqwest`, `rumqttc`, `rskafka`, `lapin`, `zeromq`, `parquet`, or `arrow`.
2. **`rekuiper-sql`** (`crates/rekuiper-sql/Cargo.toml`):
   - Parser, planner, evaluator, and expression functions.
   - Depends only on `rekuiper-core` plus pure-Rust codec/crypto/time crates (`md-5`, `sha2`, `base64`, `uuid`, `rand`, `crc32fast`, `sha1`, `flate2`, `zstd`, `chrono-tz`).
3. **`rekuiper-connectors`** (`crates/rekuiper-connectors/Cargo.toml`):
   - Protocol adapters (`rumqttc`, `rskafka`, `redis`, `sqlx`, `reqwest`, `tokio-tungstenite`, `lapin`, `zeromq`, `parquet`, `arrow`) and optional feature flags (`modbus`, `opcua`, `otlp`).
4. **`rekuiper-server`** (`crates/rekuiper-server/Cargo.toml`):
   - HTTP/WebSocket API (`axum`, `tower`, `tower-http`), JS runtime (`boa_engine`), framing (`yamux`), crypto (`ring`), and engine orchestration.

## Adapter Registration Template

When a future ticket registers a new adapter dependency, record an entry in the table below before editing any manifest:

| Ticket | Crate | Version & Features | Supported Platforms | License | Build & Check Evidence |
| --- | --- | --- | --- | --- | --- |
| *(baseline)* | Workspace lockfile | Existing `Cargo.lock` | Linux, macOS, Windows | MIT / Apache-2.0 compatible | `cargo deny check advisories` |

## Exact Verification Commands

Run all checks on WSL `ext4` before accepting any dependency change:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p rekuiper-core --lib
cargo test -p rekuiper-sql --lib
cargo deny check advisories
```
