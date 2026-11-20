#!/usr/bin/env python3
"""Exercise named-zone SQL through a running packaged daemon."""

import json
import sys
import time
from urllib.error import URLError
from urllib.request import Request, urlopen

base = sys.argv[1]
deadline = time.monotonic() + 30
while True:
    try:
        with urlopen(base + "/ping", timeout=2):
            break
    except URLError:
        if time.monotonic() >= deadline:
            raise
        time.sleep(1)

payload = {
    "id": "tzdb_smoke",
    "sql": (
        "SELECT convert_tz('2024-03-10T06:59:59Z', 'America/New_York') AS before_gap, "
        "convert_tz('2024-03-10T07:00:00Z', 'America/New_York') AS after_gap FROM sample"
    ),
    "mockSource": {"sample": {"data": [{"id": 1}], "loop": False}},
}
request = Request(
    base + "/ruletest",
    data=json.dumps(payload).encode(),
    headers={"Content-Type": "application/json"},
)
with urlopen(request, timeout=5):
    pass
with urlopen(Request(base + "/ruletest/tzdb_smoke/start", method="POST"), timeout=5):
    pass
request = Request(base + "/test/tzdb_smoke", headers={"Accept": "text/event-stream"})
with urlopen(request, timeout=10) as response:
    for line in response:
        if line.startswith(b"data:"):
            actual = json.loads(line.removeprefix(b"data:"))
            expected = {
                "before_gap": "2024-03-10 01:59:59",
                "after_gap": "2024-03-10 03:00:00",
            }
            if actual != expected:
                raise SystemExit(f"Unexpected timezone result: {actual}")
            print(actual)
            break
    else:
        raise SystemExit("Packaged daemon emitted no timezone test row")
