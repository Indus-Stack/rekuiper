#!/usr/bin/env python3
"""
Fleet Manager and Live Telemetry Generator for Rekuiper.

Manages 1 Control Plane (port 9081) and a fleet of 5 Edge Rekuiper gateways (ports 9082..9086),
registers distinct streams and stream-processing rules to each edge node, and continuously
injects realistic randomized industrial IoT sensor payloads.
"""

import sys
import os
import time
import json
import signal
import random
import urllib.request
import urllib.error
import subprocess
from pathlib import Path

BASE_DIR = Path("/home/paanday/fleet")
BIN_PATH = BASE_DIR / "bin" / "kuiperd"

NODES = [
    {
        "id": "edge-gateway-01",
        "name": "Austin Reactor 1",
        "plant": "Austin-Plant",
        "region": "us-south",
        "type": "IPC-Industrial",
        "port": 9082,
        "cli_port": 20501,
        "sse_port": 10082,
        "stream": "turbine_stream",
        "stream_sql": "CREATE STREAM turbine_stream () WITH (FORMAT='json', DATASOURCE='turbine_stream');",
        "rule_id": "rule_turbine_monitor",
        "rule_sql": "SELECT device, avg(rpm) as avg_rpm, max(temp) as peak_temp FROM turbine_stream GROUP BY TumblingWindow(ss, 2)",
        "gen_payload": lambda: {
            "device": "Austin-Reactor-1",
            "rpm": round(random.uniform(1400.0, 3600.0), 2),
            "temp": round(random.uniform(60.0, 98.5), 2),
            "vibration": round(random.uniform(0.12, 1.85), 3),
            "ts": int(time.time() * 1000)
        }
    },
    {
        "id": "edge-gateway-02",
        "name": "Berlin Mill 2",
        "plant": "Berlin-Plant",
        "region": "eu-central",
        "type": "IPC-Industrial",
        "port": 9083,
        "cli_port": 20502,
        "sse_port": 10083,
        "stream": "vibration_stream",
        "stream_sql": "CREATE STREAM vibration_stream () WITH (FORMAT='json', DATASOURCE='vibration_stream');",
        "rule_id": "rule_vibration_anomaly",
        "rule_sql": "SELECT device, accel_x, accel_y, accel_z FROM vibration_stream WHERE abs(accel_x) > 1.2 OR abs(accel_y) > 1.2",
        "gen_payload": lambda: {
            "device": "Berlin-Mill-2",
            "accel_x": round(random.uniform(-1.75, 1.75), 3),
            "accel_y": round(random.uniform(-1.75, 1.75), 3),
            "accel_z": round(random.uniform(9.2, 10.4), 3),
            "ts": int(time.time() * 1000)
        }
    },
    {
        "id": "edge-gateway-03",
        "name": "Tokyo Substation 3",
        "plant": "Tokyo-Plant",
        "region": "ap-east",
        "type": "Gateway-DIN",
        "port": 9084,
        "cli_port": 20503,
        "sse_port": 10084,
        "stream": "power_stream",
        "stream_sql": "CREATE STREAM power_stream () WITH (FORMAT='json', DATASOURCE='power_stream');",
        "rule_id": "rule_power_optimization",
        "rule_sql": "SELECT device, voltage, current, voltage * current as power_watts FROM power_stream WHERE voltage * current > 1000",
        "gen_payload": lambda: {
            "device": "Tokyo-Sub-3",
            "voltage": round(random.uniform(215.0, 245.0), 2),
            "current": round(random.uniform(3.0, 8.2), 2),
            "phase": "A",
            "ts": int(time.time() * 1000)
        }
    },
    {
        "id": "edge-gateway-04",
        "name": "Shanghai Line 4",
        "plant": "Shanghai-Plant",
        "region": "ap-east",
        "type": "Gateway-DIN",
        "port": 9085,
        "cli_port": 20504,
        "sse_port": 10085,
        "stream": "env_stream",
        "stream_sql": "CREATE STREAM env_stream () WITH (FORMAT='json', DATASOURCE='env_stream');",
        "rule_id": "rule_env_watchdog",
        "rule_sql": "SELECT device, temperature, humidity, air_quality FROM env_stream WHERE temperature > 75.0 OR air_quality > 150",
        "gen_payload": lambda: {
            "device": "Shanghai-L4",
            "temperature": round(random.uniform(22.0, 85.0), 2),
            "humidity": round(random.uniform(40.0, 92.0), 1),
            "air_quality": random.randint(45, 210),
            "ts": int(time.time() * 1000)
        }
    },
    {
        "id": "edge-gateway-05",
        "name": "Detroit Press 5",
        "plant": "Detroit-Plant",
        "region": "us-east",
        "type": "IPC-Industrial",
        "port": 9086,
        "cli_port": 20505,
        "sse_port": 10086,
        "stream": "hydraulic_stream",
        "stream_sql": "CREATE STREAM hydraulic_stream () WITH (FORMAT='json', DATASOURCE='hydraulic_stream');",
        "rule_id": "rule_pressure_protection",
        "rule_sql": "SELECT device, pressure_psi, oil_temp FROM hydraulic_stream WHERE pressure_psi > 110.0",
        "gen_payload": lambda: {
            "device": "Detroit-Press-5",
            "pressure_psi": round(random.uniform(85.0, 135.0), 2),
            "oil_temp": round(random.uniform(50.0, 88.0), 1),
            "valve_open": random.random() > 0.3,
            "ts": int(time.time() * 1000)
        }
    }
]

CONTROL_PLANE = {
    "port": 9081,
    "cli_port": 20498,
    "sse_port": 10081,
    "dir": BASE_DIR / "control"
}

def http_post(url, data_dict, timeout=2.0):
    body = json.dumps(data_dict).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.getcode(), resp.read().decode("utf-8")

def http_get(url, timeout=2.0):
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.getcode(), resp.read().decode("utf-8")

def check_alive(port, timeout=1.0):
    try:
        code, _ = http_get(f"http://127.0.0.1:{port}/ping", timeout=timeout)
        return code == 200
    except Exception:
        return False

def wait_for_service(port, name, max_retries=30):
    for i in range(max_retries):
        if check_alive(port):
            return True
        time.sleep(0.5)
    return False

def write_kuiper_yaml(etc_dir: Path, rest_port: int, cli_port: int, sse_port: int):
    etc_dir.mkdir(parents=True, exist_ok=True)
    yaml_content = f"""basic:
  restPort: {rest_port}
  restIp: 0.0.0.0
  port: {cli_port}
  ip: 127.0.0.1
  httpServerPort: {sse_port}
  httpServerIp: 127.0.0.1
  logLevel: info
  consoleLog: false
  fileLog: true
  prometheus: false
  pprof: false
  authentication: false
"""
    (etc_dir / "kuiper.yaml").write_text(yaml_content)

def setup_directories():
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    write_kuiper_yaml(
        CONTROL_PLANE["dir"] / "etc",
        CONTROL_PLANE["port"],
        CONTROL_PLANE["cli_port"],
        CONTROL_PLANE["sse_port"]
    )
    (CONTROL_PLANE["dir"] / "data").mkdir(parents=True, exist_ok=True)
    (CONTROL_PLANE["dir"] / "log").mkdir(parents=True, exist_ok=True)

    for node in NODES:
        node_dir = BASE_DIR / f"node_{node['port']}"
        node["dir"] = node_dir
        write_kuiper_yaml(
            node_dir / "etc",
            node["port"],
            node["cli_port"],
            node["sse_port"]
        )
        (node_dir / "data").mkdir(parents=True, exist_ok=True)
        (node_dir / "log").mkdir(parents=True, exist_ok=True)

def kill_existing():
    print("[*] Checking and terminating any existing rekuiper/kuiperd processes...")
    try:
        subprocess.run(["pkill", "-9", "-f", "kuiperd"], check=False)
    except Exception:
        pass
    time.sleep(1)

def launch_fleet():
    processes = []
    
    # 1. Start Control Plane
    cp_log = open(CONTROL_PLANE["dir"] / "log" / "kuiperd.out", "w")
    cp_cmd = [
        str(BIN_PATH),
        "--etc", str(CONTROL_PLANE["dir"] / "etc"),
        "--data", str(CONTROL_PLANE["dir"] / "data"),
        "--log", str(CONTROL_PLANE["dir"] / "log")
    ]
    print(f"[*] Starting Control Plane on port {CONTROL_PLANE['port']}...")
    cp_proc = subprocess.Popen(cp_cmd, stdout=cp_log, stderr=subprocess.STDOUT)
    processes.append(cp_proc)

    if not wait_for_service(CONTROL_PLANE["port"], "Control Plane"):
        print("[!] Control Plane failed to start! Check logs at:", CONTROL_PLANE["dir"] / "log" / "kuiperd.out")
        sys.exit(1)
    print(f"[+] Control Plane is ONLINE at http://localhost:{CONTROL_PLANE['port']}/ui")

    # 2. Start 5 Edge Gateways
    cp_ws = f"ws://127.0.0.1:{CONTROL_PLANE['port']}/fleet/ws"
    for node in NODES:
        n_log = open(node["dir"] / "log" / "kuiperd.out", "w")
        n_cmd = [
            str(BIN_PATH),
            "--etc", str(node["dir"] / "etc"),
            "--data", str(node["dir"] / "data"),
            "--log", str(node["dir"] / "log"),
            "--control-plane-url", cp_ws,
            "--device-id", node["id"],
            "--device-name", node["name"],
            "--plant", node["plant"],
            "--region", node["region"],
            "--device-type", node["type"]
        ]
        print(f"[*] Starting Edge Node [{node['id']}] ({node['name']}) on port {node['port']}...")
        n_proc = subprocess.Popen(n_cmd, stdout=n_log, stderr=subprocess.STDOUT)
        processes.append(n_proc)

    for node in NODES:
        if not wait_for_service(node["port"], node["name"]):
            print(f"[!] Node {node['id']} failed to start on port {node['port']}")
        else:
            print(f"[+] Edge Node [{node['id']}] is ONLINE on port {node['port']}")

    time.sleep(1.5)
    return processes

def provision_streams_and_rules():
    print("\n[*] Provisioning streams and rules on each edge gateway...")
    for node in NODES:
        base_url = f"http://127.0.0.1:{node['port']}"
        # 1. Create Stream
        try:
            code, resp = http_post(f"{base_url}/streams", {"sql": node["stream_sql"]})
            print(f"  [+] Node [{node['id']}] created stream '{node['stream']}': HTTP {code}")
        except Exception as e:
            print(f"  [!] Node [{node['id']}] stream creation: {e}")

        # 2. Create Rule
        rule_def = {
            "id": node["rule_id"],
            "sql": node["rule_sql"],
            "actions": [
                {
                    "log": {}
                }
            ]
        }
        try:
            code, resp = http_post(f"{base_url}/rules", rule_def)
            print(f"  [+] Node [{node['id']}] created & started rule '{node['rule_id']}': HTTP {code}")
        except Exception as e:
            print(f"  [!] Node [{node['id']}] rule creation: {e}")

def run_continuous_injection(batch_interval_s=0.5):
    print("\n" + "="*70)
    print("🚀 FLEET RUNNING WITH CONTINUOUS DATA INJECTION")
    print(f"   Control Plane Dashboard: http://localhost:{CONTROL_PLANE['port']}/ui")
    print("   Inspect the 'Fleet Manager' tab to see all 5 edge devices online,")
    print("   and use 'Inspect Proxy' on any device to view live rules and metrics.")
    print("   Press Ctrl+C to stop.")
    print("="*70 + "\n")

    injected_counts = {node["id"]: 0 for node in NODES}
    total_injected = 0
    start_time = time.time()
    last_stat_time = start_time

    while True:
        cycle_start = time.time()
        for node in NODES:
            payload = node["gen_payload"]()
            stream_url = f"http://127.0.0.1:{node['port']}/streams/{node['stream']}/data"
            try:
                code, _ = http_post(stream_url, payload, timeout=1.0)
                if code in (200, 201):
                    injected_counts[node["id"]] += 1
                    total_injected += 1
            except Exception:
                pass

            # Also feed to Control Plane so main dashboard pipelines display live throughput
            cp_url = f"http://127.0.0.1:{CONTROL_PLANE['port']}/streams/{node['stream']}/data"
            try:
                http_post(cp_url, payload, timeout=0.5)
            except Exception:
                pass

        now = time.time()
        if now - last_stat_time >= 3.0:
            elapsed = now - start_time
            rate = total_injected / elapsed if elapsed > 0 else 0
            stats_str = " | ".join(f"{n['id'][-2:]}: {injected_counts[n['id']]}" for n in NODES)
            print(f"⏱️ [{elapsed:5.1f}s] Injected Total: {total_injected:5d} ({rate:4.1f} msg/s) | Nodes: {stats_str}")
            last_stat_time = now

        sleep_dur = max(0.05, batch_interval_s - (time.time() - cycle_start))
        time.sleep(sleep_dur)

def main():
    action = sys.argv[1] if len(sys.argv) > 1 else "run"

    if action == "stop":
        kill_existing()
        print("[+] Fleet stopped.")
        return

    if action == "status":
        print(f"Control Plane (port {CONTROL_PLANE['port']}): {'ONLINE' if check_alive(CONTROL_PLANE['port']) else 'OFFLINE'}")
        for node in NODES:
            alive = check_alive(node['port'])
            print(f"Node [{node['id']}] (port {node['port']}): {'ONLINE' if alive else 'OFFLINE'}")
        return

    kill_existing()
    setup_directories()
    procs = launch_fleet()

    def signal_handler(sig, frame):
        print("\n[*] Stopping fleet processes...")
        for p in procs:
            try:
                p.terminate()
            except Exception:
                pass
        kill_existing()
        print("[+] Clean exit complete.")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    provision_streams_and_rules()
    run_continuous_injection(batch_interval_s=0.5)

if __name__ == "__main__":
    main()
