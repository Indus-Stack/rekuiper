# ankurkrp/rekuiper

[![Docker Pulls](https://img.shields.io/docker/pulls/ankurkrp/rekuiper?color=blue&logo=docker)](https://hub.docker.com/r/ankurkrp/rekuiper)
[![Release](https://img.shields.io/badge/release-v1.0.0--rc3-blue.svg)](https://github.com/ankur-paan/rekuiper/releases)
[![License: MIT or Apache-2.0](https://img.shields.io/badge/license-MIT%20%2F%20Apache--2.0-yellow.svg)](https://github.com/ankur-paan/rekuiper/blob/main/LICENSE)

`rekuiper` is a lightweight streaming SQL engine written in Rust for edge computing, IIoT gateways, connected vehicles, and robotics. It is a drop-in replacement for eKuiper. It processes 150k–200k messages per second per CPU core with a low memory footprint.

`rekuiper` includes an embedded **Model Context Protocol (MCP)** server (`rekuiper-mcp`). AI assistants (Antigravity IDE, Claude Desktop, Cursor) use this server to inspect schemas, validate SQL offline, query topologies, and trace streaming rules.

---

## Quick Start

### 1. Run with Docker

Start `rekuiper` with the REST API and Prometheus metrics enabled:

```bash
docker run -d --name rekuiper \
  -p 9081:9081 -p 59720:59720 -p 20499:20499 \
  -e KUIPER__BASIC__CONSOLELOG=true \
  -e KUIPER__BASIC__PROMETHEUS=true \
  ankurkrp/rekuiper:1.0.0-rc4
```

Check engine status:

```bash
curl http://localhost:9081/ping
# Response: pong
```

Open the OpenAPI Swagger UI in your browser:
```text
http://localhost:9081/
```

---

### 2. Run with Docker Compose

Clone the repository and start the multi-service stack:

```bash
git clone https://github.com/ankur-paan/rekuiper.git
cd rekuiper
docker compose -f deploy/docker/docker-compose.yml up -d
```

This stack starts:
- **rekuiper** (ports `9081`, `59720`, `20499`)
- **Eclipse Mosquitto MQTT Broker** (port `1883`)
- **Redis Cache** (port `6379`)

---

## Enterprise Features

### 1. Vector Database & Similarity Search
Perform vector similarity calculations directly in SQL queries:
- `cosine_similarity(vec1, vec2)`: Calculates cosine similarity between numeric vectors (-1.0 to 1.0).
- `vector_l2(vec1, vec2)`: Calculates Euclidean distance between points.
- `vector_dot(vec1, vec2)`: Computes the inner dot product.
- `vector_match(query_vec, candidate_array, top_k)`: Returns the top $K$ nearest vectors ordered by similarity.
- **SQL Similarity Predicates**: Filter streams with threshold clauses like `WHERE cosine_similarity(v1, v2) > 0.85`.

### 2. Native RabbitMQ (AMQP 0-9-1) Connector
Connect to RabbitMQ brokers natively without CGO or external shared libraries:
- **RabbitMQ Source**: Consumes queues and binds to exchanges with QoS prefetch control.
- **RabbitMQ Sink**: Publishes stream records to exchanges or direct queues with confirmation.
- Supports secure TLS (`amqps://`) and dynamic credentials.

### 3. WebAssembly (WASM) Plugin Runtime
Run custom user-defined functions (UDFs) in a secure, sandboxed WebAssembly runtime:
- Compile modules in Rust, C, C++, or Go (TinyGo).
- Manage plugins via REST: `POST /plugins/wasm`, `GET /plugins/wasm`, and `DELETE /plugins/wasm/:name`.
- Execute functions in SQL using direct function calls or `wasm_run('module', 'function', arg1, arg2)`.

### 4. Dynamic Secrets Resolution & Redaction
Protect credentials across streams and sinks:
- **HashiCorp Vault**: Read secrets dynamically via `{{vault://mount/path#field}}`.
- **Environment Variables**: Resolve secrets using `{{env://VARIABLE_NAME}}`.
- **Catalog Redaction**: REST API responses mask passwords and show original URI templates.

### 5. Apache Parquet Columnar Serialization
Store and read streaming telemetry using columnar Apache Parquet:
- **Parquet Sink**: Writes records with binary `PAR1` headers and Snappy compression.
- **Parquet Source**: Reads Parquet files with automatic Arrow schema inference, column projection, and predicate pushdown filtering.

### 6. EdgeX Foundry Dual-Port & OpenZiti Zero-Trust
Integrate with EdgeX Foundry microservices:
- **Concurrent Dual Ports**: The engine listens on port `9081` (eKuiper default) and port `59720` (EdgeX default) with full bidirectional REST parity.
- **EdgeX Message Bus**: Native source and sink for EdgeX V2 and V3 DTO events over MQTT, Redis, or ZeroMQ.
- **OpenZiti Zero-Trust**: Deploy with `deploy/docker/docker-compose-edgex-openziti.yml` to remove all public host ports.

---

## Exposed Network Ports

| Port | Protocol | Purpose | Default Bind |
| :--- | :--- | :--- | :--- |
| **`9081`** | HTTP / TCP | **REST API, OpenAPI UI & Management** (Standard eKuiper port). | `0.0.0.0:9081` |
| **`59720`** | HTTP / TCP | **EdgeX Application Service REST Port** (Concurrent parity with 9081). | `0.0.0.0:59720` |
| **`20499`** | HTTP / TCP | **Prometheus Metrics** (`/metrics` when enabled). | `0.0.0.0:20499` |
| **`20498`** | TCP | **eKuiper RPC Protocol Parity** (Legacy Go RPC communication). | `127.0.0.1:20498` |

---

## Configuration & Environment Variables

Override configuration keys in `etc/kuiper.yaml` with double underscores: `KUIPER__<SECTION>__<KEY>`.

### Common Configuration Variables

| Variable | Default | Description |
| :--- | :--- | :--- |
| `KUIPER__BASIC__LOGLEVEL` | `info` | Log verbosity: `debug`, `info`, `warn`, `error`. |
| `KUIPER__BASIC__CONSOLELOG` | `true` | Streams logs directly to stdout and stderr. |
| `KUIPER__BASIC__PROMETHEUS` | `false` | Enables the Prometheus `/metrics` scraping server. |
| `KUIPER__BASIC__PROMETHEUSPORT` | `20499` | Port for the Prometheus metrics listener. |
| `KUIPER__BASIC__AUTHENTICATION` | `false` | Enforces RSA JWT token validation on REST routes. |
| `KUIPER__BASIC__TIMEZONE` | `Local` | IANA timezone string (for example, `UTC` or `Local`). |
| `MQTT_SOURCE__DEFAULT__SERVER` | `tcp://127.0.0.1:1883` | Default upstream MQTT broker URL. |
| `VAULT_ADDR` | `http://127.0.0.1:8200` | HashiCorp Vault server address. |
| `VAULT_TOKEN` | (None) | HashiCorp Vault access token. |
| `RUST_LOG` | `info` | Rust tracing filter (`info`, `debug`, `warn`). |

---

## Persistent Storage & Volumes

| Container Path | Purpose |
| :--- | :--- |
| `/kuiper/data` | Stores SQLite database (`sqliteKV.db`), rule checkpoints, Parquet files, and WASM modules. |
| `/kuiper/etc` | Contains configuration files (`kuiper.yaml`, `mqtt_source.yaml`). Mount to supply custom configurations. |
| `/kuiper/log` | File-based logging directory when file logging is active. |

### Run with Persistent Volumes

```bash
docker run -d --name rekuiper \
  -p 9081:9081 -p 59720:59720 -p 20499:20499 \
  -v rekuiper-data:/kuiper/data \
  -v rekuiper-etc:/kuiper/etc \
  ankurkrp/rekuiper:1.0.0-rc4
```

---

## Command Line Interface & Tools

The container includes the `kuiper` CLI and `rekuiper-mcp` binary in `/kuiper/bin`:

```bash
# Check engine status
docker exec -it rekuiper kuiper ping

# Create a stream
docker exec -it rekuiper kuiper create stream demo '(temp float, humidity bigint) WITH (FORMAT="JSON", DATASOURCE="devices/+/events")'

# List active streams
docker exec -it rekuiper kuiper show streams

# Inspect the Model Context Protocol server
docker exec -it rekuiper /kuiper/bin/rekuiper-mcp --help
```

---

## Supported Architectures

Official images support:
- **`linux/amd64`** (x86_64 servers and edge gateways)
- **`linux/arm64`** (Raspberry Pi 4/5, NVIDIA Jetson, Apple Silicon, AWS Graviton)

---

## Links

- **GitHub Repository**: [https://github.com/ankur-paan/rekuiper](https://github.com/ankur-paan/rekuiper)
- **Releases & Binary Downloads**: [https://github.com/ankur-paan/rekuiper/releases](https://github.com/ankur-paan/rekuiper/releases)
- **Model Context Protocol Guide**: [crates/rekuiper-mcp](https://github.com/ankur-paan/rekuiper/tree/main/crates/rekuiper-mcp)
- **Issue Tracker**: [https://github.com/ankur-paan/rekuiper/issues](https://github.com/ankur-paan/rekuiper/issues)
