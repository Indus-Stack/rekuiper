#!/usr/bin/env python3
"""
Benchmark maximum sustainable parallel rules for rekuiper in WSL with Docker.
Constraints: 1 CPU core (--cpuset-cpus=2 --cpus=1), 1 GiB RAM (--memory=1g --memory-swap=1g), TOKIO_WORKER_THREADS=1.
Ingestion: Single-stream broadcast to N parallel rules.
Criterion: Memory must stay LEVEL at load (flat plateau). If memory climbs steadily, internal buffers are accumulating.
"""

import concurrent.futures
import http.client
import json
import os
import subprocess
import sys
import threading
import time
import urllib.request
import urllib.error

# Configuration
ENGINE_IMAGE = os.environ.get("REK_IMAGE", "indusstack/rekuiper:latest")
ENGINE_NAME = "rekuiper-bench"
MGMT_PORT = 9081
STREAM_NAME = "rawdata"
RATE = int(os.environ.get("BENCH_RATE", "500"))  # 500 events/sec baseline
SECS = int(os.environ.get("BENCH_SECS", "30"))
BATCH_SIZE = int(os.environ.get("BENCH_BATCH", "25")) # 20 batches of 25 events = 500 events/sec

def sh(cmd, check=True):
    r = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"Command failed ({r.returncode}): {cmd}\nStderr: {r.stderr.strip()}")
    return r

def run_out(cmd):
    return sh(cmd, check=False).stdout.strip()

def http_post(url, data_dict):
    data = json.dumps(data_dict).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=10) as resp:
        return resp.status, resp.read().decode("utf-8")

def http_get(url):
    req = urllib.request.Request(url, headers={"Accept": "application/json"}, method="GET")
    with urllib.request.urlopen(req, timeout=5) as resp:
        return resp.status, resp.read().decode("utf-8")

def find_cgroup_dir(cid):
    candidates = [
        f"/sys/fs/cgroup/system.slice/docker-{cid}.scope",
        f"/sys/fs/cgroup/docker/{cid}",
    ]
    for c in candidates:
        if os.path.isfile(os.path.join(c, "cpu.stat")):
            return c
    try:
        for root, dirs, files in os.walk("/sys/fs/cgroup"):
            if cid in root and "cpu.stat" in files:
                return root
    except Exception:
        pass
    return None

class CgroupSampler(threading.Thread):
    def __init__(self, cg_dir, cid):
        super().__init__()
        self.cg_dir = cg_dir
        self.cid = cid
        self.stop_event = threading.Event()
        self.anon_history = []
        self.rss_history = []
        self.cpu_history = []
        self.daemon = True

    def run(self):
        last_usage = self.read_cpu_usage()
        last_time = time.time()
        while not self.stop_event.is_set():
            time.sleep(1.0)
            now = time.time()
            usage = self.read_cpu_usage()
            elapsed = now - last_time
            last_time = now

            if usage is not None and last_usage is not None and elapsed > 0:
                cpu_pct = (usage - last_usage) / 10000.0 / elapsed
                self.cpu_history.append(round(cpu_pct, 1))
            last_usage = usage

            anon = self.read_anon_mb()
            rss = self.read_rss_mb()
            if anon is not None:
                self.anon_history.append(round(anon, 2))
            if rss is not None:
                self.rss_history.append(round(rss, 2))

    def stop(self):
        self.stop_event.set()
        self.join(timeout=3)

    def read_cpu_usage(self):
        if self.cg_dir:
            try:
                with open(os.path.join(self.cg_dir, "cpu.stat"), "r") as f:
                    for line in f:
                        if line.startswith("usage_usec "):
                            return int(line.split()[1])
            except Exception:
                pass
        return None

    def read_anon_mb(self):
        if self.cg_dir:
            try:
                with open(os.path.join(self.cg_dir, "memory.stat"), "r") as f:
                    for line in f:
                        if line.startswith("anon "):
                            return int(line.split()[1]) / 1048576.0
            except Exception:
                pass
        try:
            out = run_out(f"docker stats --no-stream --format '{{{{.MemUsage}}}}' {ENGINE_NAME}")
            part = out.split("/")[0].strip()
            if "GiB" in part:
                return float(part.replace("GiB", "")) * 1024.0
            elif "MiB" in part:
                return float(part.replace("MiB", ""))
            elif "KiB" in part:
                return float(part.replace("KiB", "")) / 1024.0
        except Exception:
            pass
        return None

    def read_rss_mb(self):
        if self.cg_dir:
            try:
                with open(os.path.join(self.cg_dir, "memory.current"), "r") as f:
                    return int(f.read().strip()) / 1048576.0
            except Exception:
                pass
        return None

def start_engine():
    sh(f"docker rm -f {ENGINE_NAME} 2>/dev/null || true")
    cmd = (
        f"docker run -d --name {ENGINE_NAME} "
        f"--cpuset-cpus=2 --cpus=1 --memory=1g --memory-swap=1g "
        f"-e TOKIO_WORKER_THREADS=1 -e RUST_LOG=warn "
        f"-p {MGMT_PORT}:9081 {ENGINE_IMAGE}"
    )
    sh(cmd)
    
    cid = run_out(f"docker inspect {ENGINE_NAME} --format '{{{{.Id}}}}'")
    for _ in range(50):
        try:
            status, body = http_get(f"http://127.0.0.1:{MGMT_PORT}/ping")
            if status == 200 and "pong" in body:
                break
        except Exception:
            pass
        time.sleep(0.1)
    else:
        raise RuntimeError("rekuiper engine failed to respond on port 9081")
    return cid

def create_stream():
    url = f"http://127.0.0.1:{MGMT_PORT}/streams"
    sql = f'CREATE STREAM {STREAM_NAME} () WITH (TYPE="httppush");'
    status, body = http_post(url, {"sql": sql})
    if status not in (200, 201):
        raise RuntimeError(f"Create stream failed: {status} {body}")

def create_rules(rule_count):
    url = f"http://127.0.0.1:{MGMT_PORT}/rules"
    def post_one(idx):
        payload = {
            "id": f"rule_{idx}",
            "sql": f"SELECT id, device, temp FROM {STREAM_NAME} WHERE temp > 21.0",
            "actions": [{"nop": {}}]
        }
        status, body = http_post(url, payload)
        if status not in (200, 201):
            raise RuntimeError(f"Create rule_{idx} failed: {status} {body}")

    t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as ex:
        futures = [ex.submit(post_one, i) for i in range(1, rule_count + 1)]
        for f in concurrent.futures.as_completed(futures):
            f.result()
    dur = time.time() - t0
    return dur

def http_load_generator(target_rate, duration_secs, batch_size):
    """
    Sends target_rate events/sec in batches of batch_size to /streams/rawdata/data
    using persistent HTTP/1.1 connections.
    """
    batches_per_sec = max(1, target_rate // batch_size)
    interval = 1.0 / batches_per_sec
    total_batches = int(batches_per_sec * duration_secs)
    
    conn = http.client.HTTPConnection("127.0.0.1", MGMT_PORT, timeout=5)
    headers = {"Content-Type": "application/json"}
    
    sent_events = 0
    t_start = time.time()
    
    for b in range(total_batches):
        target_time = t_start + b * interval
        now = time.time()
        if target_time > now:
            time.sleep(target_time - now)
            
        # Build batch
        batch = [
            {
                "id": f"evt_{b}_{k}",
                "device": f"dev_{k % 100}",
                "temp": 20.0 + (k % 150) / 10.0,
                "speed": (k % 1300) / 10.0,
                "ts": int(time.time() * 1000)
            }
            for k in range(batch_size)
        ]
        body = json.dumps(batch)
        try:
            conn.request("POST", f"/streams/{STREAM_NAME}/data", body=body, headers=headers)
            resp = conn.getresponse()
            resp.read()
            sent_events += batch_size
        except Exception:
            # Reconnect on drop
            try:
                conn.close()
                conn = http.client.HTTPConnection("127.0.0.1", MGMT_PORT, timeout=5)
            except Exception:
                pass
                
    conn.close()
    return sent_events

def run_tier(rule_count):
    print(f"\n{'='*60}")
    print(f"[*] Testing Tier: {rule_count} Concurrent Parallel Rules")
    print(f"{'='*60}", flush=True)
    
    cid = start_engine()
    cg_dir = find_cgroup_dir(cid)
    
    create_stream()
    dur = create_rules(rule_count)
    print(f"[+] Successfully deployed {rule_count} rules in {dur:.2f}s", flush=True)
    
    time.sleep(2.0)
    
    sampler = CgroupSampler(cg_dir, cid)
    baseline_anon = sampler.read_anon_mb() or 0.0
    baseline_rss = sampler.read_rss_mb() or 0.0
    print(f"[*] Baseline Memory (pre-load): Anon={baseline_anon:.2f} MiB, RSS={baseline_rss:.2f} MiB", flush=True)
    
    sampler.start()
    
    print(f"[*] Ingesting {RATE} events/s ({RATE * rule_count:,} rule-evals/s) for {SECS}s...", flush=True)
    sent = http_load_generator(RATE, SECS, BATCH_SIZE)
    print(f"[+] Sent {sent} total events across stream bus.", flush=True)
    
    time.sleep(2.0)
    sampler.stop()
    
    anon_seq = sampler.anon_history
    cpu_seq = sampler.cpu_history
    
    peak_anon = max(anon_seq) if anon_seq else baseline_anon
    peak_cpu = max(cpu_seq) if cpu_seq else 0.0
    mean_cpu = round(sum(cpu_seq) / len(cpu_seq), 1) if cpu_seq else 0.0
    
    # Trajectory analysis in second half of window (seconds 15-30)
    half = len(anon_seq) // 2
    if len(anon_seq) >= 6:
        m_mid = anon_seq[half]
        m_end = anon_seq[-1]
        delta_m = m_end - m_mid
        pct_rise = ((m_end - m_mid) / max(m_mid, 1.0)) * 100.0
    else:
        m_mid = baseline_anon
        m_end = peak_anon
        delta_m = peak_anon - baseline_anon
        pct_rise = 0.0

    # Level criterion: delta_m <= 2.0 MiB in 2nd half, flat plateau
    is_level = (delta_m <= 2.0 and pct_rise <= 10.0 and peak_anon < 900.0)
    status_str = "LEVEL (SUSTAINABLE)" if is_level else "ACCUMULATING (UNSUSTAINABLE)"
    
    print(f"[-] Metric Summary for {rule_count} rules:")
    print(f"    Baseline Anon    : {baseline_anon:.2f} MiB")
    print(f"    Peak Anon Memory : {peak_anon:.2f} MiB")
    print(f"    Trajectory (15-30s): {m_mid:.2f} MiB -> {m_end:.2f} MiB (delta={delta_m:+.2f} MiB, {pct_rise:+.1f}%)")
    print(f"    CPU Utilization  : Mean={mean_cpu:.1f}%, Peak={peak_cpu:.1f}%")
    print(f"    Verdict          : {status_str}", flush=True)
    
    sh(f"docker rm -f {ENGINE_NAME} 2>/dev/null || true")
    
    return {
        "rules": rule_count,
        "baseline_anon_mb": baseline_anon,
        "peak_anon_mb": peak_anon,
        "m_mid_mb": m_mid,
        "m_end_mb": m_end,
        "delta_m_mb": delta_m,
        "pct_rise": pct_rise,
        "mean_cpu_pct": mean_cpu,
        "peak_cpu_pct": peak_cpu,
        "is_level": is_level,
        "status": status_str,
        "anon_seq": anon_seq,
        "cpu_seq": cpu_seq,
    }

def main():
    print(f"=== Starting rekuiper Parallel Rule Concurrency & Sustainability Benchmark ===")
    print(f"Resource constraints: 1 CPU core (cpuset=2), 1 GiB RAM (swap=1g), TOKIO_WORKER_THREADS=1")
    print(f"Load: {RATE} events/s for {SECS}s window on shared stream")
    print(f"================================================================================", flush=True)
    
    try:
        tiers = [50, 100, 200, 300, 500, 750, 1000, 1500, 2000, 2500, 3000]
        results = []
        max_sustainable = 0
        
        for count in tiers:
            res = run_tier(count)
            results.append(res)
            if res["is_level"]:
                max_sustainable = count
            else:
                print(f"[!] Sustainability ceiling reached at {count} rules (buffer accumulation detected).", flush=True)
                break
                
        print("\n" + "="*88)
        print("FINAL BENCHMARK RESULTS TABLE")
        print("="*88)
        print(f"{'Rules':<8} | {'Base Anon':<10} | {'Peak Anon':<10} | {'Delta M (15-30s)':<18} | {'Mean CPU':<10} | {'Status':<25}")
        print("-" * 88)
        for r in results:
            delta_str = f"{r['delta_m_mb']:+.2f} MB ({r['pct_rise']:+.1f}%)"
            print(f"{r['rules']:<8} | {r['baseline_anon_mb']:<10.2f} | {r['peak_anon_mb']:<10.2f} | {delta_str:<18} | {r['mean_cpu_pct']:<10.1f} | {r['status']:<25}")
        print("-" * 88)
        print(f"\n[+] MAXIMUM SUSTAINABLE PARALLEL RULES: {max_sustainable} RULES", flush=True)
        
        with open("evidence_parallel_rules.json", "w") as f:
            json.dump(results, f, indent=2)
        print(f"[+] Telemetry saved to evidence_parallel_rules.json", flush=True)
        
    finally:
        sh(f"docker rm -f {ENGINE_NAME} 2>/dev/null || true")

if __name__ == "__main__":
    main()
