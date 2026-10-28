# Rule Pipelines

You can create rule pipelines by routing the output of an upstream rule into downstream rules. Pipelines can use external message brokers, such as MQTT, or internal connectors.

Using the [Memory Source](../sources/builtin/memory.md) and [Memory Sink](../sinks/builtin/memory.md) creates internal pipelines without external broker dependencies.

## Implementation Procedure

Rule pipelines connect through shared memory topics. You create each pipeline stage independently through the REST API or CLI:

```shell
# 1. Create the input source stream
{"sql": "CREATE STREAM demo () WITH (DATASOURCE=\"demo\", FORMAT=\"JSON\")"}

# 2. Create the first rule that routes output to an in-memory topic
{
  "id": "rule1",
  "sql": "SELECT * FROM demo WHERE isNull(temperature)=false",
  "actions": [{
    "log": {},
    "memory": {
      "topic": "home/ch1/sensor1"
    }
  }]
}

# 3. Create a downstream stream from the memory topic
{"sql": "CREATE STREAM sensor1 () WITH (DATASOURCE=\"home/+/sensor1\", FORMAT=\"JSON\", TYPE=\"memory\")"}

# 4. Create downstream rules that consume the memory topic
{
  "id": "rule2-1",
  "sql": "SELECT avg(temperature) FROM sensor1 GROUP BY CountWindow(10)",
  "actions": [{
    "log": {},
    "memory": {
      "topic": "analytic/sensors"
    }
  }]
}

{
  "id": "rule2-2",
  "sql": "SELECT temperature + 273.15 as k FROM sensor1",
  "actions": [{
    "log": {}
  }]
}
```

The memory topic connects `rule1` to downstream rules: `rule1 -> {rule2-1, rule2-2}`.

<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 840 190" width="100%" height="auto" role="img" aria-label="In-Memory Multi-Rule Pipeline Fanout" font-family="-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif">
  <rect x="0" y="0" width="840" height="190" rx="10" fill="#f8fafc" stroke="#e2e8f0" stroke-width="1" />
  <text x="420" y="26" text-anchor="middle" font-size="14" font-weight="700" fill="#0f172a">In-Memory Rule Pipeline Fan-Out (Zero External Broker Overhead)</text>
  <rect x="30" y="65" width="170" height="80" rx="8" fill="#ffffff" stroke="#2563eb" stroke-width="1.5" />
  <text x="115" y="92" text-anchor="middle" font-size="12" font-weight="700" fill="#1e40af">Upstream: rule1</text>
  <text x="115" y="112" text-anchor="middle" font-size="10" fill="#334155">Filters null temperature</text>
  <text x="115" y="128" text-anchor="middle" font-size="10" fill="#64748b">Sink: memory (home/ch1/sensor1)</text>
  <path d="M 200 105 L 285 105" stroke="#2563eb" stroke-width="2" />
  <polygon points="285,105 277,101 277,109" fill="#2563eb" />
  <rect x="285" y="65" width="220" height="80" rx="8" fill="#eff6ff" stroke="#93c5fd" stroke-width="1.5" />
  <text x="395" y="92" text-anchor="middle" font-size="12" font-weight="700" fill="#1d4ed8">Memory Topic Bus</text>
  <text x="395" y="112" text-anchor="middle" font-size="10" fill="#1e40af">Topic: home/+/sensor1</text>
  <text x="395" y="128" text-anchor="middle" font-size="10" fill="#3b82f6">Wildcard Pattern Matching</text>
  <path d="M 505 88 L 595 68" stroke="#059669" stroke-width="2" />
  <polygon points="595,68 586,66 588,74" fill="#059669" />
  <path d="M 505 122 L 595 142" stroke="#059669" stroke-width="2" />
  <polygon points="595,142 588,136 586,144" fill="#059669" />
  <rect x="595" y="38" width="215" height="60" rx="8" fill="#ffffff" stroke="#10b981" stroke-width="1.5" />
  <text x="702" y="62" text-anchor="middle" font-size="12" font-weight="700" fill="#047857">Downstream: rule2-1</text>
  <text x="702" y="82" text-anchor="middle" font-size="10" fill="#334155">SELECT avg(temperature) CountWindow(10)</text>
  <rect x="595" y="112" width="215" height="60" rx="8" fill="#ffffff" stroke="#10b981" stroke-width="1.5" />
  <text x="702" y="136" text-anchor="middle" font-size="12" font-weight="700" fill="#047857">Downstream: rule2-2</text>
  <text x="702" y="156" text-anchor="middle" font-size="10" fill="#334155">SELECT temperature + 273.15 AS k</text>
</svg>

Notes:
- You can combine the memory sink with other sinks in the `actions` array of a single rule.
- Memory sources support MQTT-style wildcards (such as `+`) to subscribe to multiple memory topics.

