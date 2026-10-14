# Benchmark Overview

This document describes the benchmark methodology, test principles, and release-specific benchmark history for rekuiper.

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
| **In-Process Engine Throughput** | **rekuiper 1.0.0-rc1** | Stream execution of 500k events on 1 core | Exceeds **1,248,000 events/s** (pure engine) and **741,000 events/s** (JSON E2E) with zero GC pauses. | [View Benchmark](#rekuiper-1-0-0-rc1-in-process-engine-throughput-milestone-1-000-000-eps) |
| **Concurrent Parallel Rules** | **rekuiper 0.505-beta** | Stream fan-out to $N$ parallel SQL rules | Sustains **1,000 parallel rules** (500,000 evals/s) on 1 core; eKuiper ceiling is 100 rules. | [View Benchmark](./parallel_rules.md) |
| **Single-Rule MQTT Throughput** | **rekuiper 0.500-beta** | Ingestion of 5 industrial MQTT shapes | Sustains **150,000 to 200,000 msg/s** loss-free on 1 core; 6.3x to 10x higher than competitors. | [View Benchmark](./throughput.md) |
| **120-Second Bounded Sustained** | **rekuiper 0.426-beta** | Long-duration MQTT ingest (12M events) | Sustains **100,000 msg/s for 120s** across all five workloads with 0.00% loss. | [View Benchmark](./throughput.md#version-0-426-bounded-sustained-ladder) |
| **Multi-Engine Ingestion Ladder** | **rekuiper 0.425-beta** | 30s rate ladder (5k to 100k msg/s) | First published 4-engine comparison against eKuiper 2.4.1, Telegraf, and Redpanda Connect. | [View Benchmark](./throughput.md#version-0-425-initial-comparison-ladder) |

---

## 5. Summary of Release Benchmarks

### rekuiper 1.0.0-rc1: In-Process Engine Throughput Milestone (1,000,000+ EPS)
- Evaluated single-core in-process SQL streaming engine with `cargo test --release --test perf_throughput`.
- Processed 500,000 records through a continuous filter and arithmetic projection rule (`SELECT id, temp * 1.8 + 32 AS temp_f FROM perf_stream WHERE temp > 20.0`).
- **Pure Engine Dispatch**: **1,248,977.92 events/sec** (400.33 ms total runtime, pre-constructed `SharedMap` ring buffer, zero GC pauses).
- **End-to-End Pipeline**: **741,294.10 events/sec** (674.50 ms total runtime, live JSON serialization and parsing).
- Key enabling optimizations:
  1. **Zero-Clone Borrowed WHERE Fast-Path**: Compares `Identifier <op> Literal` directly with borrowed references, eliminating 1,000,000 heap clones per 500k events.
  2. **Register-Level Numeric Arithmetic**: Computes expressions (`temp * 1.8 + 32`) directly in CPU registers without intermediate heap allocation.
  3. **Single-Subscriber `try_reserve()` Fast-Path**: Delivers events directly into bounded Tokio channel slots without per-event allocations.
  4. **Lock-Free Atomic Counters**: Rule status loops increment atomic integers without touching global synchronization locks.

### rekuiper 0.505-beta: Parallel Rule Concurrency
- Evaluated maximum rule fan-out from a single stream.
- Reached **1,000 concurrent rules** (500,000 evaluations/s) on 1 core with flat 251.4 MiB heap memory and 0.0s drain lag.
- Demonstrated **10.0x higher concurrency** than eKuiper 2.4.1 (ceiling: 100 rules).

### rekuiper 0.500-beta: Peak Ingestion Ceilings
- Introduced zero-disk in-memory catalog and hot-path connection pooling.
- Established exact 1k-resolution peak capacity ceilings: 150,000 msg/s on W1 and W3; 200,000 msg/s on W2 and W4; 126,000 msg/s on W5.
- Reduced single-core CPU utilization at 100k msg/s by over 9 percentage points compared to 0.426.

### rekuiper 0.426-beta: Sustained Duration Qualification
- Verified long-duration stability under 120 seconds of continuous traffic at 100,000 msg/s.
- Processed 12 million events per trial without memory leaks or queue backlog.

### rekuiper 0.425-beta: Baseline Comparison Ladder
- Initial differential testing across four streaming engines on identical hardware.
- Proved memory-bounded incremental window aggregation for IoT streaming data.
