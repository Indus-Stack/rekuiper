# Concurrent Parallel Rules Benchmark

This benchmark measures the maximum number of concurrent SQL rules that an engine can execute on a single stream in parallel before memory accumulates or drain latency increases.

The benchmark compares **rekuiper 0.505** with **eKuiper 2.4.1** under identical container limits.

---

## 1. Test Configuration

Both engines run with identical container limits in Docker:

| Parameter | Value | Description |
| :--- | :--- | :--- |
| **CPU Limit** | `1.0 core` | One pinned physical CPU core (`--cpuset-cpus=2 --cpus=1`) |
| **Memory Limit** | `1 GiB` | Bounded memory with swap disabled (`--memory=1g --memory-swap=1g`) |
| **Worker Threads** | `1 thread` | `TOKIO_WORKER_THREADS=1` (rekuiper), `GOMAXPROCS=1` (eKuiper) |
| **Memory Target** | `900 MiB` | `GOMEMLIMIT=900MiB` (eKuiper Go runtime limit) |
| **Stream Ingestion** | `httppush` | In-process HTTP push broadcast to active rules |
| **Sampling Interval** | `1.0 second` | Linux cgroup v2 metrics (`cpu.stat`, `memory.stat`, `memory.current`) |

### Workload SQL Rule

Each rule evaluates the following SQL statement:

```sql
SELECT id, device, temp FROM rawdata WHERE temp > 21.0;
```

Each rule sends matched records to a `nop` sink. The `nop` sink discards records without I/O operations.

### Load Profile

- **Input Ingestion Rate**: Constant 500 events per second over a 30.0-second window.
- **Rule Evaluation Rate**: $500 \times N$ rule evaluations per second.

---

## 2. Sustainability Criteria

A test tier is **sustainable** when the engine processes events at arrival rate without buffer backlog:

1. **Stable Memory Plateau**:
   - The test records anonymous heap memory (`anon` in `memory.stat`).
   - Memory during the second half of the run (seconds 15 to 30) must establish a flat plateau ($\Delta M \le 2.0\text{ MiB}$).
   - Memory must not climb steadily toward the container ceiling.

2. **Zero Drain Latency**:
   - When the load generator stops at 30.0 seconds, processing must finish immediately ($\text{Drain Lag} \le 1.0\text{s}$).

A test tier is **unsustainable** when:
1. **Queue Buffer Accumulation**: The CPU saturates at 100% capacity and falls behind message arrival. Channel queues store unconsumed records, and heap memory increases continuously.
2. **Drain Lag**: Processing continues past the 30.0-second window because accumulated records drain slowly from internal queues.

---

## 3. Benchmark Results

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

This ladder records rekuiper performance as active rules increase from 50 to 2,000:

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

## 4. Key Findings

### 10x Higher Rule Concurrency

- **rekuiper** sustains **1,000 parallel rules** on one CPU core. It evaluates **500,000 rule queries per second** with zero drain delay and stable memory.
- **eKuiper 2.4.1** reaches its sustainability ceiling at **100 rules**. At 200 rules, eKuiper CPU saturates at 95.3%, memory increases from 250 MiB to 613 MiB, and drain latency increases by +28.1 seconds.

### Memory Footprint per Active Rule

- **rekuiper** allocates approximately **40 KiB of baseline memory per active rule**.
- **eKuiper** allocates approximately **254 KiB of baseline memory per active rule** (**6.3x larger**).

### Execution Efficiency

- At 100 rules (50,000 rule evaluations/second), **rekuiper** consumes only **22.8% of one core**.
- **eKuiper** consumes **80.3% of one core** for the same workload (**3.5x higher CPU load**).

---

## 5. How to Reproduce

Run the automated test script in the repository:

```bash
# Run the benchmark ladder
python3 test/benchmark/multiple_rules/run_benchmark.py
```

Raw telemetry logs are in [test/benchmark/multiple_rules/](https://github.com/Indus-Stack/rekuiper/tree/main/test/benchmark/multiple_rules).
