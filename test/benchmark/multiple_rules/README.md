# Concurrent Parallel Rules Benchmark

This benchmark measures the maximum number of concurrent SQL rules that an engine can execute on a single stream in parallel.

The benchmark compares **rekuiper 0.505** and **eKuiper 2.4.1** under identical hardware and container limits.

All test configurations, orchestration scripts, and telemetry logs are in this directory.

---

## 1. Test Objective

Industrial IoT gateways receive high-frequency data on a small number of streams. Multiple rules process this data simultaneously:
- Safety rules monitor temperature thresholds.
- Maintenance rules calculate vibration averages.
- Telemetry rules forward data to cloud endpoints.

This architecture creates a fan-out pattern ($1 \text{ stream event} \to N \text{ parallel rules}$).

This benchmark measures:
1. Maximum number of parallel rules supported on one CPU core.
2. Rule evaluation rate at full load.
3. Memory growth during sustained execution.
4. Channel queue stability and drain latency.

---

## 2. Test Configuration

Both engines run in Docker containers on Linux with identical resource constraints:

| Parameter | Value | Description |
| :--- | :--- | :--- |
| **CPU Limit** | `1.0 core` | Container pinned to core 2 (`--cpuset-cpus=2 --cpus=1`) |
| **Memory Limit** | `1 GiB` | Bounded memory with swap disabled (`--memory=1g --memory-swap=1g`) |
| **Worker Threads** | `1 thread` | `TOKIO_WORKER_THREADS=1` (rekuiper), `GOMAXPROCS=1` (eKuiper) |
| **Memory Target** | `900 MiB` | `GOMEMLIMIT=900MiB` (eKuiper Go runtime limit) |
| **Stream Type** | `httppush` | In-process HTTP push broadcast to active rules |
| **Sampling Interval** | `1.0 second` | Linux cgroup v2 metrics (`cpu.stat`, `memory.stat`, `memory.current`) |

---

## 3. Workload and Rule Definition

### Input Stream

The test creates a single input stream named `rawdata`:

```sql
CREATE STREAM rawdata () WITH (TYPE="httppush");
```

### Parallel Rules

The test creates $N$ parallel rules (`rule_1` to `rule_N`). Each rule evaluates the following SQL statement:

```sql
SELECT id, device, temp FROM rawdata WHERE temp > 21.0
```

Each rule sends matching records to a `nop` sink. The `nop` sink discards records without I/O operations. This isolates engine execution from disk or network bottlenecks.

### Load Profile

- **Input Event Rate**: Constant 500 events per second.
- **Batch Size**: 25 events per HTTP POST request (20 requests per second).
- **Rule Evaluation Rate**: $500 \times N$ rule evaluations per second.
- **Test Duration**: 30.0 seconds per tier.
- **Payload Structure**:
  ```json
  [
    {
      "id": "evt_100_1",
      "device": "dev_12",
      "temp": 24.5,
      "speed": 65.2,
      "ts": 1727913600000
    }
  ]
  ```

---

## 4. Sustainability Criteria

A test tier is **sustainable** when the engine processes events at arrival rate without internal backlog:

1. **Stable Memory Plateau**:
   - The test monitors anonymous heap memory (`anon` in `memory.stat`).
   - The engine must establish a flat memory plateau during the second half of the run (seconds 15 to 30).
   - The memory difference must remain near zero ($\Delta M_{15-30\text{s}} \le 2.0\text{ MiB}$).
   - Memory must not climb continuously toward the container boundary.

2. **Zero Drain Latency**:
   - When the load generator stops at 30.0 seconds, processing must finish immediately ($\text{Drain Lag} \le 1.0\text{s}$).
   - If processing continues after the generator stops, events accumulated in internal channel queues.

A test tier is **unsustainable** when:
- The CPU reaches 100% saturation and falls behind real-time arrival.
- Channel queues store undelivered messages, causing continuous heap memory growth.
- Drain latency exceeds 1.0 second after the generator completes.

---

## 5. Benchmark Results

### Table 1: Head-to-Head Comparison (Equal Baseline)

This table compares both engines at 50, 100, and 200 parallel rules:

| Parallel Rules | Ingest Rate | Rule Evaluation Rate | Engine | CPU Load (Mean) | Peak Memory (Anon) | Memory per Rule | Drain Lag | Sustainability Status |
| :---: | :---: | :---: | :--- | :---: | :---: | :---: | :---: | :--- |
| **50** | 500 msg/s | 25,000 /s | **rekuiper 0.505** | **13.2%** | **7.6 MiB** | **38 KiB** | **0.0s** | Sustained (Level) |
| 50 | 500 msg/s | 25,000 /s | eKuiper 2.4.1 | 48.7% | 33.0 MiB | 277 KiB | 0.0s | Sustained (Level) |
| **100** | 500 msg/s | 50,000 /s | **rekuiper 0.505** | **22.8%** | **10.1 MiB** | **40 KiB** | **0.0s** | Sustained (Level) |
| 100 | 500 msg/s | 50,000 /s | eKuiper 2.4.1 | 80.3% | 66.4 MiB | 254 KiB | +0.2s | **Sustained Ceiling** |
| **200** | 500 msg/s | 100,000 /s | **rekuiper 0.505** | **43.3%** | **15.2 MiB** | **37 KiB** | **0.0s** | Sustained (Level) |
| 200 | 500 msg/s | 100,000 /s | eKuiper 2.4.1 | 95.3% | 613.6 MiB | 2,940 KiB | **+28.1s** | **Failed (Queue Backlog)** |

---

### Table 2: Concurrency Summary (Ceiling Comparison)

| Performance Metric | rekuiper 0.505 | eKuiper 2.4.1 | Ratio / Difference |
| :--- | :---: | :---: | :---: |
| **Maximum Sustainable Rules** | **1,000 rules** | 100 rules | **10.0x higher concurrency** |
| **Sustained Rule Evaluations** | **500,000 evals/s** | 50,000 evals/s | **10.0x higher throughput** |
| **Idle Memory per Active Rule** | **~40 KiB / rule** | ~254 KiB / rule | **6.3x lower memory footprint** |
| **CPU Utilization at 100 Rules** | **22.8% of 1 core** | 80.3% of 1 core | **3.5x lower CPU utilization** |
| **Processing at 200 Rules** | **Sustainable (0.0s lag)** | Failed (+28.1s lag) | eKuiper accumulates queue backlog |
| **Peak Memory Stability** | **Level ($\Delta M = -1.1\text{ MiB}$)** | Accumulated (+201.9 MiB) | rekuiper maintains flat plateau |

---

### Table 3: rekuiper Concurrency Scaling Ladder

This ladder records rekuiper telemetry as the number of parallel rules increases from 50 to 2,000:

| Active Rules | Input Rate | Rule Evals / s | CPU Load (Mean) | Peak Heap (Anon) | Trajectory $\Delta M_{15-30s}$ | Drain Lag | Test Status |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| **50** | 500 msg/s | 25,000 /s | 13.2% | 7.6 MiB | +0.4 MiB (+5.6%) | 0.0s | Level (Sustainable) |
| **100** | 500 msg/s | 50,000 /s | 22.8% | 10.1 MiB | -0.1 MiB (-1.0%) | 0.0s | Level (Sustainable) |
| **200** | 500 msg/s | 100,000 /s | 43.3% | 15.2 MiB | -0.2 MiB (-1.3%) | 0.0s | Level (Sustainable) |
| **300** | 500 msg/s | 150,000 /s | 58.8% | 20.5 MiB | +0.1 MiB (+0.5%) | 0.0s | Level (Sustainable) |
| **500** | 500 msg/s | 250,000 /s | 87.0% | 70.6 MiB | -0.5 MiB (-0.7%) | 0.0s | Level (Sustainable) |
| **750** | 500 msg/s | 375,000 /s | 88.3% | 184.5 MiB | +1.2 MiB (+0.7%) | 0.0s | Level (Sustainable) |
| **1,000** | 500 msg/s | 500,000 /s | 90.1% | 251.4 MiB | -1.1 MiB (-0.4%) | 0.0s | **Certified Peak Ceiling** |
| **1,500** | 500 msg/s | 750,000 /s | 90.2% | 379.4 MiB | +18.4 MiB (+5.1%) | +9.8s | Queue Lag (Unsustainable) |
| **2,000** | 500 msg/s | 1,000,000 /s | 95.4% | 203.9 MiB | +42.1 MiB (+26.0%) | +75.0s | Queue Lag (Unsustainable) |

---

## 6. Analysis and Observations

### 1. Concurrency Capacity
- `rekuiper` sustained 1,000 concurrent rules on one physical CPU core. The engine evaluated 500,000 rule queries per second without queue accumulation.
- `eKuiper 2.4.1` reached its limit at 100 concurrent rules (50,000 evaluations per second). At 200 rules, eKuiper CPU saturated at 95.3%, memory rose to 613.6 MiB, and drain lag reached +28.1 seconds.
- `rekuiper` delivered **10.0x higher concurrent rule capacity** on identical hardware.

### 2. Memory Footprint per Active Rule
- `rekuiper` allocated approximately **40 KiB of heap memory per active rule**.
- `eKuiper` allocated approximately **254 KiB of heap memory per active rule** (**6.3x larger**).
- When under backpressure at 200 rules, eKuiper internal queues expanded memory to 613.6 MiB. In contrast, rekuiper memory remained stable at 15.2 MiB.

### 3. Execution Efficiency
- At 100 rules, `rekuiper` required **22.8% of one CPU core**.
- `eKuiper` required **80.3% of one CPU core** for the same workload (**3.5x higher CPU load**).

---

## 7. How to Reproduce

### Prerequisites
- Linux host or WSL2 on Windows with Docker.
- Python 3.8 or newer.

### Automated Execution

1. Navigate to the benchmark directory:
   ```bash
   cd test/benchmark/multiple_rules
   ```

2. Run the automated benchmark script:
   ```bash
   python3 run_benchmark.py
   ```

The script executes the following procedure:
- Starts the engine container with CPU and memory limits.
- Creates the `rawdata` stream.
- Deploys $N$ parallel rules concurrently.
- Records pre-load baseline memory.
- Ingests 500 events per second for 30.0 seconds.
- Samples cgroup v2 metrics every 1.0 second.
- Analyzes memory slope and drain lag.
- Cleans up containers and outputs telemetry to `evidence_parallel_rules.json`.
