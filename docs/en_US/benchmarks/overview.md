# Benchmarks: Streaming SQL & MQTT Rule Engine Performance (Single Core)

rekuiper is a high-performance MQTT rule engine and streaming SQL processor written in Rust, evaluated under strict container resource limits of one physical CPU core and 1 GiB of RAM. This document describes the benchmark methodology, test principles, and release-specific benchmark history for rekuiper.

---

## 1. Test Principles

All benchmarks adhere to three strict principles:

1. **Equal Resource Limits**: Each engine runs inside a Docker container on Linux. The host limits each container to **one physical CPU core** and **1 GiB of RAM**. Swap memory is disabled.
2. **Exact Output Proof**: Sinks write all processed output to disk. Verification scripts parse the output to prove zero message loss, zero duplicate messages, and correct calculations.
3. **Reproducible Workloads**: Dedicated generators send synthetic events on strict schedules outside the engine container.

---

## 2. Test Environment

Both `rekuiper` and competitor engines run with identical hardware constraints:

| Parameter | Constraint |
| :--- | :--- |
| **Host System** | x86-64 multi-core processor, Linux kernel 6.x |
| **CPU Assignment** | Pinned dedicated physical core (`--cpuset-cpus=2 --cpus=1`) |
| **Memory Limit** | 1 GiB maximum (`--memory=1g --memory-swap=1g`) |
| **Network** | Dedicated Docker bridge network (`rk-bench-net`) |
| **Metrics Collection** | Linux cgroup v2 interfaces (`cpu.stat`, `memory.stat`, `memory.current`) sampled every 1.0 second |

---

## 3. Sustainability Criteria

A test run is **sustainable** when the engine processes events at arrival rate without internal backlog:

### Stable Memory Plateau
Under steady load, engine memory must form a flat level line.
- The sampler records anonymous heap memory (`anon` from `memory.stat`) each second.
- If memory variation in the second half of the test window is small ($\Delta M \le 2.0\text{ MiB}$), the queue is empty.
- If memory increases continuously, internal queues store undelivered messages. This condition is **not sustainable**.

### Zero Drain Latency
When the generator stops, the engine must finish processing immediately.
- If the output sink closes within 1.0 second of the send window, drain latency is zero.
- If the sink continues to write records after the generator stops, data accumulated in internal buffers during the test.

---

## 4. Benchmark Catalog by Release Version

Different benchmark suites evaluate specific capabilities across versions:

| Suite Name | Evaluated Version | Workload Type | Key Result | Link |
| :--- | :---: | :--- | :--- | :--- |
| **In-Process Engine Throughput** | **rekuiper 1.0.0-rc4** | Continuous stream execution of 500k events on 1 core | Measures **2,207,367 events/s** (zero-parse pipeline) and **1,085,885 events/s** (JSON pipeline) with active dead-letter queue support. | [View Benchmark](#version-1-0-0-rc4-in-process-engine-throughput-benchmark) |
| **Concurrent Parallel Rules** | **rekuiper 0.505-beta** | Stream fan-out to $N$ parallel SQL rules | Sustains **1,000 parallel rules** (500,000 evals/s) on 1 core; eKuiper ceiling is 100 rules. | [View Benchmark](./parallel_rules.md) |
| **Single-Rule MQTT Throughput** | **rekuiper 0.500-beta** | Ingestion of 5 industrial MQTT shapes | Sustains **150,000 to 200,000 msg/s** loss-free on 1 core; 6.3x to 10x higher than competitors. | [View Benchmark](./throughput.md) |
| **120-Second Bounded Sustained** | **rekuiper 0.426-beta** | Long-duration MQTT ingest (12M events) | Sustains **100,000 msg/s for 120s** across all five workloads with 0.00% loss. | [View Benchmark](./throughput.md#version-0-426-bounded-sustained-ladder) |
| **Multi-Engine Ingestion Ladder** | **rekuiper 0.425-beta** | 30s rate ladder (5k to 100k msg/s) | First published 4-engine comparison against eKuiper 2.4.1, Telegraf, and Redpanda Connect. | [View Benchmark](./throughput.md#version-0-425-initial-comparison-ladder) |

---

## 5. Release-Specific Benchmark History

### Version 1.0.0-rc4: In-Process Engine Throughput Benchmark

rekuiper version 1.0.0-rc4 evaluates in-process SQL streaming throughput on a single CPU core.
The test runs with active dead-letter queue routing and complete projection evaluation.

#### Test Configuration

| Parameter | Specification |
| :--- | :--- |
| **Software Version** | rekuiper v1.0.0-rc4 |
| **Test Suite** | `crates/rekuiper-server/tests/perf_throughput.rs` |
| **Compiler Profile** | `--release` with `-C target-cpu=native` |
| **Storage Subsystem** | Native Linux ext4 filesystem |
| **Execution Model** | Single thread (`--test-threads=1`, `TOKIO_WORKER_THREADS=1`) |
| **Dataset Size** | 500,000 synthetic records |
| **SQL Workload** | `SELECT id, temp * 1.8 + 32 AS temp_f FROM perf_stream WHERE temp > 20.0` |

#### Workload Description

The engine receives 500,000 records on the `perf_stream` stream.
The SQL rule filters records where `temp > 20.0`.
The SQL rule calculates the Fahrenheit temperature with the formula `temp * 1.8 + 32`.
The engine delivers projected records to a bounded in-memory subscriber channel.

#### Measured Performance Results

rekuiper version 1.0.0-rc4 recorded the following performance metrics:

| Pipeline Architecture | Evaluated Version | Records Processed | Elapsed Time | Throughput | Memory Model | Drain Delay |
| :--- | :---: | :---: | :---: | :---: | :--- | :---: |
| **Zero-Parse In-Process Engine Pipeline** | **v1.0.0-rc4** | 500,000 | **226.51 ms** | **2,207,367 events/s** | Zero heap allocation, zero GC pauses | **0.0 ms** |
| **End-to-End JSON Streaming Pipeline** | **v1.0.0-rc4** | 500,000 | **460.45 ms** | **1,085,885 events/s** | Dynamic JSON parse and serialize | **0.0 ms** |

#### Key Enabling Optimizations in Version 1.0.0-rc4

1. **Zero-Allocation Error Key Checks**:
   The engine iterates directly over key-value pairs to detect error keys. This eliminates hash calculations on valid records. Dead-letter queue routing for invalid records remains active.

2. **Non-Blocking Rule Status (`try_read`)**:
   The function `is_rule_running` uses non-blocking reader lock acquisition. This prevents worker delays during concurrent metrics serialization.

3. **Direct Metric Visibility**:
   The engine increments atomic counters synchronously. This provides immediate rule completion detection in polling loops.

4. **CPU Register Arithmetic**:
   The evaluator computes numeric expressions (`temp * 1.8 + 32`) directly in CPU floating-point registers.

#### How to Reproduce

Execute the throughput test in a native Linux ext4 environment:

```bash
RUSTFLAGS="-C target-cpu=native" cargo test --release -p rekuiper-server --test perf_throughput -- --nocapture --test-threads=1
```

---

### Version 1.0.0-rc2: In-Process Engine Throughput Benchmark

rekuiper version 1.0.0-rc2 evaluates in-process SQL streaming throughput on a single CPU core.

#### Test Configuration

| Parameter | Specification |
| :--- | :--- |
| **Software Version** | rekuiper v1.0.0-rc2 |
| **Test Suite** | `crates/rekuiper-server/tests/perf_throughput.rs` |
| **Compiler Profile** | `--release` with `-C target-cpu=native` |
| **Storage Subsystem** | Native Linux ext4 filesystem |
| **Execution Model** | Single thread (`--test-threads=1`, `TOKIO_WORKER_THREADS=1`) |
| **Dataset Size** | 500,000 synthetic records |
| **SQL Workload** | `SELECT id, temp * 1.8 + 32 AS temp_f FROM perf_stream WHERE temp > 20.0` |

#### Workload Description

The engine receives 500,000 records on the `perf_stream` stream.
The SQL rule filters records where `temp > 20.0`.
The SQL rule calculates the Fahrenheit temperature with the formula `temp * 1.8 + 32`.
The engine delivers projected records to a bounded in-memory subscriber channel.

#### Measured Performance Results

rekuiper version 1.0.0-rc2 recorded the following performance metrics:

| Pipeline Architecture | Evaluated Version | Records Processed | Elapsed Time | Throughput | Memory Model | Drain Delay |
| :--- | :---: | :---: | :---: | :---: | :--- | :---: |
| **Zero-Parse In-Process Engine Pipeline** | **v1.0.0-rc2** | 500,000 | **233.17 ms** | **2,144,375 events/s** | Zero heap allocation, zero GC pauses | **0.0 ms** |
| **End-to-End JSON Streaming Pipeline** | **v1.0.0-rc2** | 500,000 | **502.96 ms** | **994,118 events/s** | Dynamic JSON parse and serialize | **0.0 ms** |

#### Key Enabling Optimizations in Version 1.0.0-rc2

1. **Zero-Clone Borrowed WHERE Evaluation**:
   The engine compares identifiers to literals with borrowed memory references.
   This optimization removes 1,000,000 memory copy operations for each 500,000 records.

2. **CPU Register Arithmetic**:
   The evaluator computes numeric expressions (`temp * 1.8 + 32`) directly in CPU registers.
   This optimization avoids dynamic memory allocations during projection.

3. **Single-Subscriber `try_reserve()` Channel Dispatch**:
   The dispatcher delivers events directly into bounded channel slots without queue locks.

4. **Lock-Free Atomic Counters**:
   The rule monitor increments atomic integers without global mutex locks.

#### How to Reproduce

Execute the throughput test in a native Linux ext4 environment:

```bash
RUSTFLAGS="-C target-cpu=native" cargo test --release -p rekuiper-server --test perf_throughput -- --nocapture --test-threads=1
```

---

### Version 0.505-beta: Parallel Rule Concurrency

rekuiper version 0.505-beta evaluates rule fan-out capacity from a single input stream.

- The engine evaluates up to **1,000 parallel rules** (500,000 evaluations per second) on one CPU core.
- Memory usage remains stable at 251.4 MiB with zero drain latency.
- In comparison, eKuiper version 2.4.1 reaches its limit at 100 parallel rules.
- See the complete test report in the [Concurrent Parallel Rules Benchmark](./parallel_rules.md).

---

### Version 0.500-beta: Peak Ingestion Ceilings

rekuiper version 0.500-beta introduces an in-memory catalog and zero-disk hot-path processing.

- The engine sustains **150,000 to 200,000 messages per second** without data loss on one CPU core.
- Single-core CPU utilization decreases by 9.1 percentage points compared to version 0.426-beta.
- See the complete test report in the [Single-Rule MQTT Throughput Benchmark](./throughput.md).

---

### Version 0.426-beta: Sustained Duration Qualification

rekuiper version 0.426-beta evaluates long-duration stability under continuous load.

- The engine processes **100,000 messages per second for 120 seconds** (12,000,000 events) without memory growth.
- Output sinks verify zero message loss and zero duplicate records across all five test workloads.
- See the detailed results in [Version 0.426 Bounded Sustained Ladder](./throughput.md#version-0-426-bounded-sustained-ladder).

---

### Version 0.425-beta: Baseline Comparison Ladder

rekuiper version 0.425-beta provides the first multi-engine throughput comparison.

- The test compares rekuiper against eKuiper 2.4.1, Telegraf 1.40.0, and Redpanda Connect 4.109.0.
- rekuiper demonstrates bounded memory consumption during incremental window aggregation for IoT streaming workloads.
- See the initial rate ladder in [Version 0.425 Initial Comparison Ladder](./throughput.md#version-0-425-initial-comparison-ladder).
