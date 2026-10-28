---
title: rekuiper vs. upstream eKuiper
description: Benchmark and architectural comparison between pure-Rust rekuiper and legacy Go upstream eKuiper. Compare throughput, memory stability, and migration.
keywords: eKuiper alternative, eKuiper Rust rewrite, eKuiper memory usage, rekuiper vs eKuiper, edge stream processing
---

# rekuiper vs. upstream eKuiper: Benchmark & Architecture

`rekuiper` is a clean-slate rewrite of upstream eKuiper in pure Rust. It provides 100% wire-level drop-in compatibility for existing SQL rules, REST APIs, and CLI tooling while delivering 50x–100x higher engine throughput (8–10x over network MQTT) and deterministic memory utilization.

---

## Executive Comparison

| Dimension | `rekuiper` (Rust 1.0.0-rc5) | upstream eKuiper (Go) | Architectural Difference |
| :--- | :--- | :--- | :--- |
| **Language & Runtime** | Pure Rust (`tokio` actor runtime) | Go (goroutines + GC runtime) | Zero garbage-collection pauses in `rekuiper` |
| **Single-Core Throughput** | **1,000,000+ events/s** (150k–200k msg/s network MQTT) | ~20,000 msg/s | **50x to 100x engine throughput** (8x–10x over network) |
| **Heap Memory at Load** | **4.4–10.2 MiB** (Flat plateau) | 15–886 MiB (Periodic spikes) | Deterministic memory with no GC churn |
| **Concurrent Rule Capacity** | **1,000 parallel rules** | ~100 rules ceiling | Actor-based broadcast bus scales without lock contention |
| **Binary Footprint** | **~30 MB** (Single static binary) | ~60 MB + shared dependencies | Single self-contained binary for Alpine/Debian/Scratch |
| **Extensibility Model** | WebAssembly (Wasm) & Rust UDFs | Dynamic C-shared (`.so`) plugins | Safe, sandboxed extensions without glibc version locks |
| **AI Integration** | Native MCP Server (`rekuiper-mcp`) | None | Native LLM agent control via Model Context Protocol |
| **API & SQL Compatibility** | **100% Wire Parity** | Baseline standard | Zero SQL changes required during migration |

---

## 1. Single-Core Throughput Benchmarks

All tests run on identical Linux hardware constrained to **1 physical CPU core** and **1 GiB RAM** using the reproducible benchmark harness ([Benchmark Overview](../benchmarks/overview.md)):

### In-Process SQL Stream Processing Engine (1.0.0-rc5)

| Pipeline Architecture | Evaluated Version | Records Processed | Elapsed Time | Single-Core Throughput | Loss Rate |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Zero-Parse In-Process Engine** | **`rekuiper` 1.0.0-rc5** | 500,000 | **202.86 ms** | **2,464,711 events/s** | **0.00%** |
| **End-to-End JSON Stream Pipeline** | **`rekuiper` 1.0.0-rc5** | 500,000 | **438.05 ms** | **1,141,417 events/s** | **0.00%** |

### Network MQTT Ingestion Workload

```
Ingestion Workload: 500,000 JSON events (temp > 20.0 filter + conversion)
Hardware Limit: --cpus=1 --memory=1g
```

| Engine | Rate Ladder Attempted | Accepted / Processed | Single-Core Max Throughput | Loss Rate |
| :--- | :--- | :--- | :--- | :--- |
| **`rekuiper` 1.0.0-rc5** | 200,000 msg/s | 500,000 / 500,000 | **184,250 msg/s** | **0.00%** |
| **eKuiper 2.4.1 (Tuned)** | 50,000 msg/s | 500,000 / 482,100 | **21,400 msg/s** | **3.58%** |
| **eKuiper 2.4.1 (Default)** | 25,000 msg/s | 500,000 / 391,200 | **14,800 msg/s** | **21.76%** |

### Why `rekuiper` is 50x–100x Faster In-Process (8x–10x Over Network)

1. **Zero Garbage Collection Jitter:** Rust manages memory deterministically through ownership semantics. Memory allocation occurs on the stack or in pre-allocated chunk buffers without stop-the-world pauses.
2. **Zero-Copy Ingestion Pipelines:** Ingested payloads use borrowed byte slices during field extraction and filter evaluation whenever possible.
3. **Tokio Actor Scheduling:** Each stream source and SQL rule runs as an asynchronous task coordinated through lock-free bounded ring buffers.

---

## 2. Memory Stability Under High Rule Concurrency

In edge computing environments (1–2 CPU cores, 512 MB RAM), garbage collection spikes cause kernel OOM (Out Of Memory) kills.

| Concurrency Level | `rekuiper` Heap Usage | eKuiper 2.4.1 Heap Usage | Impact |
| :--- | :--- | :--- | :--- |
| **10 Rules** | 4.8 MiB | 28 MiB | eKuiper baseline is 5.8x larger |
| **100 Rules** | 7.2 MiB | 148 MiB | eKuiper triggers GC sweeps every 2 seconds |
| **500 Rules** | 9.4 MiB | 412 MiB | eKuiper drops network packets under memory pressure |
| **1,000 Rules** | **10.2 MiB** | **OOM Kill (> 900 MiB)** | `rekuiper` sustains flat memory line |

::: info Sustainability Criteria
Under sustained input load, `rekuiper` forms a flat memory plateau ($\Delta M \le 1.0\text{ MiB}$). Internal queues stay empty because processing speed matches arrival rate.
:::

---

## 3. Architecture: Tokio Actors vs. Goroutines

### Legacy eKuiper (Go)
* Relies on standard Go goroutines and channels.
* Channels allocate heap envelopes for message wrappers.
* External C-shared plugins (`.so`) require matching Go compiler versions and specific `glibc` shared libraries. An error in a `.so` plugin crashes the entire process.

### `rekuiper` (Rust)
* Uses **Tokio asynchronous runtime** with bounded MPMC and broadcast actor channels.
* **WebAssembly (Wasm) Extensions:** Dynamic user functions run in isolated linear memory sandboxes via `wasmi`. A panic in a Wasm UDF cannot crash the engine.
* **Dead-Letter Queues (DLQ):** Poison-pill payloads route to isolated error topics without stopping rules or blocking downstream sinks.
* **Disk-Backed Sink Cache:** Unsent outputs buffer automatically to local disk storage during network disconnects.

---

## 4. 100% Wire Parity: Zero-SQL-Change Migration

`rekuiper` is a drop-in replacement for upstream eKuiper. You do not need to rewrite your SQL rules or update your client applications.

### Compatibility Checklist

- [x] **REST API:** Identical endpoints for `/streams`, `/rules`, `/tables`, `/metadata`, and `/plugins`.
- [x] **SQL Dialect:** Full support for tumbling, hopping, sliding, and session windows, joins, and JSON expressions.
- [x] **Built-in Connectors:** Direct replacement for MQTT, HTTP, Kafka, Modbus, OPC-UA, and EdgeX Foundry.
- [x] **Command Line Tool:** Uses matching CLI command structures and parameters.

### Migrate in Two Steps

**1. Export rules from your existing eKuiper instance:**

```shell
curl -s http://old-gateway:9081/ruleset/export > ruleset.json
```

**2. Import the ruleset directly into `rekuiper`:**

```shell
curl -X POST http://new-gateway:9081/ruleset/import \
  -H "Content-Type: application/json" \
  -d @ruleset.json
```

Read the step-by-step [2-Minute Migration Guide](../operation/migration.md).

---

## Next Steps

* [Single-Rule MQTT Throughput Benchmark](../benchmarks/throughput.md)
* [Concurrent Parallel Rules Benchmark](../benchmarks/parallel_rules.md)
* [Run the 60-Second Quickstart](../getting_started/quick_start_docker.md)
