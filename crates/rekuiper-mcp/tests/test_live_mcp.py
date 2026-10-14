#!/usr/bin/env python3
"""
End-to-End Live MCP Integration Test Runner for rekuiper-mcp.
Acts as a full MCP client communicating over stdio JSON-RPC 2.0 with the containerized server.
"""

import sys
import json
import subprocess
import time

def send_rpc(proc, method, params=None, req_id=1):
    payload = {
        "jsonrpc": "2.0",
        "id": req_id,
        "method": method
    }
    if params is not None:
        payload["params"] = params
    
    line = json.dumps(payload) + "\n"
    proc.stdin.write(line.encode("utf-8"))
    proc.stdin.flush()

    # Read response
    resp_line = proc.stdout.readline().decode("utf-8").strip()
    while not resp_line:
        resp_line = proc.stdout.readline().decode("utf-8").strip()
    
    return json.loads(resp_line)

def send_notification(proc, method, params=None):
    payload = {
        "jsonrpc": "2.0",
        "method": method
    }
    if params is not None:
        payload["params"] = params
    line = json.dumps(payload) + "\n"
    proc.stdin.write(line.encode("utf-8"))
    proc.stdin.flush()

def run_tests():
    cmd = [
        "docker", "run", "-i", "--rm",
        "--network", "rekuiper-live-net",
        "rekuiper-mcp:latest",
        "--server-url", "http://rekuiper-live:9081"
    ]
    print(f"[TEST] Launching rekuiper-mcp container: {' '.join(cmd)}")
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    tests_run = 0
    tests_passed = 0

    def assert_test(name, condition, details=""):
        nonlocal tests_run, tests_passed
        tests_run += 1
        if condition:
            tests_passed += 1
            print(f"  [PASS] {name} {details}")
        else:
            print(f"  [FAIL] {name} {details}")
            sys.exit(1)

    try:
        # 1. Handshake
        print("\n--- 1. Protocol Handshake ---")
        init_resp = send_rpc(proc, "initialize", {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "AntigravityE2E", "version": "1.0.0"}
        }, req_id=1)
        assert_test("Handshake version", init_resp.get("result", {}).get("protocolVersion") == "2024-11-05")
        assert_test("Server identifier", init_resp.get("result", {}).get("serverInfo", {}).get("name") == "rekuiper-mcp")

        send_notification(proc, "notifications/initialized")
        send_rpc(proc, "ping", req_id=2)

        # 2. Tools Enumeration
        print("\n--- 2. Tools Discovery ---")
        tools_resp = send_rpc(proc, "tools/list", req_id=3)
        tools = tools_resp.get("result", {}).get("tools", [])
        tool_names = [t["name"] for t in tools]
        assert_test("Total 43 tools registered", len(tools) == 43, f"Found {len(tools)} tools")
        assert_test("Contains validate_sql", "validate_sql" in tool_names)
        assert_test("Contains test_sql_expression", "test_sql_expression" in tool_names)
        assert_test("Contains explain_sql", "explain_sql" in tool_names)
        assert_test("Contains execute_rekuiper_api", "execute_rekuiper_api" in tool_names)
        assert_test("Contains ping_engine", "ping_engine" in tool_names)
        assert_test("Contains get_engine_metrics", "get_engine_metrics" in tool_names)

        # 3. Resources Enumeration & Read
        print("\n--- 3. Resources Discovery & Reading ---")
        res_resp = send_rpc(proc, "resources/list", req_id=4)
        resources = res_resp.get("result", {}).get("resources", [])
        assert_test("Total 11 resources registered", len(resources) == 11, f"Found {len(resources)}")

        read_res = send_rpc(proc, "resources/read", {"uri": "rekuiper://metrics"}, req_id=5)
        contents = read_res.get("result", {}).get("contents", [])
        assert_test("Read rekuiper://metrics", len(contents) > 0 and contents[0]["uri"] == "rekuiper://metrics")

        # 4. Prompts Enumeration & Get
        print("\n--- 4. Prompts Discovery & Resolution ---")
        prompt_resp = send_rpc(proc, "prompts/list", req_id=6)
        prompts = prompt_resp.get("result", {}).get("prompts", [])
        assert_test("Total 5 prompts registered", len(prompts) == 5, f"Found {len(prompts)}")

        p_get = send_rpc(proc, "prompts/get", {
            "name": "troubleshoot_rule",
            "arguments": {"rule_name": "rule_heat_check"}
        }, req_id=7)
        msgs = p_get.get("result", {}).get("messages", [])
        assert_test("Get troubleshoot_rule prompt", len(msgs) > 0 and "rule_heat_check" in msgs[0]["content"]["text"])

        # 5. Offline SQL Intelligence Tools
        print("\n--- 5. SQL Intelligence & Simulator Tools ---")
        val_res = send_rpc(proc, "tools/call", {
            "name": "validate_sql",
            "arguments": {
                "sql": "SELECT abs(vibe) AS v, avg(temp) AS avg_t FROM telemetry WHERE temp > 25.0 GROUP BY TumblingWindow(ss, 10)"
            }
        }, req_id=8)
        assert_test("validate_sql valid query", not val_res.get("result", {}).get("isError", False))

        sim_res = send_rpc(proc, "tools/call", {
            "name": "test_sql_expression",
            "arguments": {
                "sql": "SELECT abs(vibe) AS v, upper(status) AS s, temp * 1.8 + 32.0 AS deg_f FROM demo WHERE temp > 20",
                "data": {"vibe": -4.5, "status": "online", "temp": 30.0}
            }
        }, req_id=9)
        assert_test("test_sql_expression execution", not sim_res.get("result", {}).get("isError", False))
        sim_data = json.loads(sim_res["result"]["content"][0]["text"])
        assert_test("Simulator output projection", sim_data.get("output", {}).get("deg_f") == 86.0 and sim_data.get("output", {}).get("s") == "ONLINE")

        expl_res = send_rpc(proc, "tools/call", {
            "name": "explain_sql",
            "arguments": {
                "sql": "SELECT temp FROM telemetry WHERE temp > 50 GROUP BY HoppingWindow(ss, 10, 5)"
            }
        }, req_id=10)
        assert_test("explain_sql query analysis", not expl_res.get("result", {}).get("isError", False))

        # 6. Live Engine Heartbeat & Telemetry
        print("\n--- 6. Live rekuiper Daemon Telemetry ---")
        ping_res = send_rpc(proc, "tools/call", {"name": "ping_engine", "arguments": {}}, req_id=11)
        assert_test("ping_engine alive", not ping_res.get("result", {}).get("isError", False))

        metrics_res = send_rpc(proc, "tools/call", {"name": "get_engine_metrics", "arguments": {}}, req_id=12)
        assert_test("get_engine_metrics live data", not metrics_res.get("result", {}).get("isError", False))

        # 7. Live Stream & Ingestion Lifecycle
        print("\n--- 7. Live Stream DDL & Ingestion ---")
        create_str = send_rpc(proc, "tools/call", {
            "name": "create_stream",
            "arguments": {
                "sql": "CREATE STREAM mcp_e2e_stream (temperature float, vibration float, device string) WITH (FORMAT=\"json\", TYPE=\"httppull\")"
            }
        }, req_id=13)
        assert_test("create_stream mcp_e2e_stream", not create_str.get("result", {}).get("isError", False))

        get_str = send_rpc(proc, "tools/call", {
            "name": "get_stream",
            "arguments": {"name": "mcp_e2e_stream"}
        }, req_id=14)
        assert_test("get_stream definition", not get_str.get("result", {}).get("isError", False))

        push_str = send_rpc(proc, "tools/call", {
            "name": "push_stream_data",
            "arguments": {
                "name": "mcp_e2e_stream",
                "data": {"temperature": 28.5, "vibration": 0.05, "device": "sensor_01"}
            }
        }, req_id=15)
        assert_test("push_stream_data ingestion", not push_str.get("result", {}).get("isError", False))

        # 8. Live Rule Lifecycle & Topology
        print("\n--- 8. Live Rule Pipeline Lifecycle ---")
        create_r = send_rpc(proc, "tools/call", {
            "name": "create_rule",
            "arguments": {
                "name": "mcp_e2e_rule",
                "sql": "SELECT temperature, vibration FROM mcp_e2e_stream WHERE temperature > 25.0",
                "actions": [{"log": {}}]
            }
        }, req_id=16)
        assert_test("create_rule mcp_e2e_rule", not create_r.get("result", {}).get("isError", False))

        rule_st = send_rpc(proc, "tools/call", {
            "name": "get_rule_status",
            "arguments": {"name": "mcp_e2e_rule"}
        }, req_id=17)
        assert_test("get_rule_status telemetry", not rule_st.get("result", {}).get("isError", False))

        rule_topo = send_rpc(proc, "tools/call", {
            "name": "get_rule_topo",
            "arguments": {"name": "mcp_e2e_rule"}
        }, req_id=18)
        assert_test("get_rule_topo DAG graph", not rule_topo.get("result", {}).get("isError", False))

        # 9. Universal API Proxy
        print("\n--- 9. Universal API Proxy (execute_rekuiper_api) ---")
        proxy_res = send_rpc(proc, "tools/call", {
            "name": "execute_rekuiper_api",
            "arguments": {
                "method": "GET",
                "endpoint": "/rules"
            }
        }, req_id=19)
        assert_test("execute_rekuiper_api GET /rules", not proxy_res.get("result", {}).get("isError", False))

        # 10. Teardown / Cleanup
        print("\n--- 10. Pipeline Teardown ---")
        del_r = send_rpc(proc, "tools/call", {
            "name": "delete_rule",
            "arguments": {"name": "mcp_e2e_rule"}
        }, req_id=20)
        assert_test("delete_rule cleanup", not del_r.get("result", {}).get("isError", False))

        del_s = send_rpc(proc, "tools/call", {
            "name": "delete_stream",
            "arguments": {"name": "mcp_e2e_stream"}
        }, req_id=21)
        assert_test("delete_stream cleanup", not del_s.get("result", {}).get("isError", False))

        print(f"\n==========================================")
        print(f"ALL TESTS PASSED: {tests_passed} / {tests_run} SUCCESSFUL")
        print(f"==========================================")

    finally:
        proc.stdin.close()
        proc.terminate()
        proc.wait(timeout=5)

if __name__ == "__main__":
    run_tests()
