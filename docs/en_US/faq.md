# Frequently Asked Questions (FAQ)

This document answers common questions about **rekuiper**, its Rust architecture, compatibility with LF Edge eKuiper, performance, and security.

---

## 1. Architecture Overview

rekuiper is a pure-Rust, single-binary edge stream processing engine and SQL rule engine designed for Industrial IoT (IIoT), smart manufacturing, connected vehicles, and high-throughput telemetry pipelines.

<div style="background: #ffffff; border: 1px solid #e2e8f0; border-radius: 12px; padding: 20px; margin: 24px 0; text-align: center; box-shadow: 0 1px 3px rgba(0,0,0,0.05);">
  <svg viewBox="0 0 860 260" width="100%" height="260" xmlns="http://www.w3.org/2000/svg">
    <!-- Background styling -->
    <defs>
      <linearGradient id="faqGradIn" x1="0%" y1="0%" x2="100%" y2="100%">
        <stop offset="0%" stop-color="#EFF6FF" />
        <stop offset="100%" stop-color="#DBEAFE" />
      </linearGradient>
      <linearGradient id="faqGradCore" x1="0%" y1="0%" x2="100%" y2="100%">
        <stop offset="0%" stop-color="#FEF3C7" />
        <stop offset="100%" stop-color="#FDE68A" />
      </linearGradient>
      <linearGradient id="faqGradOut" x1="0%" y1="0%" x2="100%" y2="100%">
        <stop offset="0%" stop-color="#ECFDF5" />
        <stop offset="100%" stop-color="#A7F3D0" />
      </linearGradient>
    </defs>

    <!-- Step 1: Ingestion Sources -->
    <rect x="20" y="30" width="230" height="200" rx="10" fill="url(#faqGradIn)" stroke="#93C5FD" stroke-width="2" />
    <text x="135" y="60" text-anchor="middle" font-family="-apple-system, BlinkMacSystemFont, Segoe UI, Roboto" font-size="15" font-weight="700" fill="#1E3A8A">Ingestion Sources</text>
    <rect x="35" y="80" width="200" height="30" rx="6" fill="#FFFFFF" stroke="#BFDBFE" stroke-width="1"/>
    <text x="135" y="100" text-anchor="middle" font-family="-apple-system, BlinkMacSystemFont, Segoe UI, Roboto" font-size="12" font-weight="600" fill="#1E40AF">MQTT v3.1.1 / v5 + mTLS</text>
    <rect x="35" y="118" width="200" height="30" rx="6" fill="#FFFFFF" stroke="#BFDBFE" stroke-width="1"/>
    <text x="135" y="138" text-anchor="middle" font-family="-apple-system, BlinkMacSystemFont, Segoe UI, Roboto" font-size="12" font-weight="600" fill="#1E40AF">Modbus TCP/RTU &amp; OPC-UA</text>
    <rect x="35" y="156" width="200" height="30" rx="6" fill="#FFFFFF" stroke="#BFDBFE" stroke-width="1"/>
    <text x="135" y="176" text-anchor="middle" font-family="-apple-system, BlinkMacSystemFont, Segoe UI, Roboto" font-size="12" font-weight="600" fill="#1E40AF">HTTP, Kafka, OTLP, ZeroMQ</text>
    <text x="135" y="212" text-anchor="middle" font-family="-apple-system, BlinkMacSystemFont, Segoe UI, Roboto" font-size="11" fill="#3B82F6">150k - 200k msgs/sec per core</text>

    <!-- Arrow 1 -->
    <path d="M 255 130 L 295 130" stroke="#64748B" stroke-width="3" fill="none" />
    <polygon points="295,125 305,130 295,135" fill="#64748B" />

    <!-- Step 2: rekuiper Engine -->
    <rect x="310" y="30" width="240" height="200" rx="10" fill="url(#faqGradCore)" stroke="#FCD34D" stroke-width="2" />
    <text x="430" y="60" text-anchor="middle" font-family="-apple-system, BlinkMacSystemFont, Segoe UI, Roboto" font-size="15" font-weight="700" fill="#92400E">rekuiper Pure Rust Core</text>
    <rect x="325" y="80" width="210" height="30" rx="6" fill="#FFFFFF" stroke="#FDE68A" stroke-width="1"/>
    <text x="430" y="100" text-anchor="middle" font-family="-apple-system, BlinkMacSystemFont, Segoe UI, Roboto" font-size="12" font-weight="600" fill="#78350F">Tokio Stream Actor Pipeline</text>
    <rect x="325" y="118" width="210" height="30" rx="6" fill="#FFFFFF" stroke="#FDE68A" stroke-width="1"/>
    <text x="430" y="138" text-anchor="middle" font-family="-apple-system, BlinkMacSystemFont, Segoe UI, Roboto" font-size="12" font-weight="600" fill="#78350F">ANSI SQL + 221 Functions</text>
    <rect x="325" y="156" width="210" height="30" rx="6" fill="#FFFFFF" stroke="#FDE68A" stroke-width="1"/>
    <text x="430" y="176" text-anchor="middle" font-family="-apple-system, BlinkMacSystemFont, Segoe UI, Roboto" font-size="12" font-weight="600" fill="#78350F">Zero GC Pauses · &lt;35 MB RAM</text>
    <text x="430" y="212" text-anchor="middle" font-family="-apple-system, BlinkMacSystemFont, Segoe UI, Roboto" font-size="11" fill="#B45309">Sub-millisecond tail latency</text>

    <!-- Arrow 2 -->
    <path d="M 555 130 L 595 130" stroke="#64748B" stroke-width="3" fill="none" />
    <polygon points="595,125 605,130 595,135" fill="#64748B" />

    <!-- Step 3: Sinks & Observability -->
    <rect x="610" y="30" width="230" height="200" rx="10" fill="url(#faqGradOut)" stroke="#6EE7B7" stroke-width="2" />
    <text x="725" y="60" text-anchor="middle" font-family="-apple-system, BlinkMacSystemFont, Segoe UI, Roboto" font-size="15" font-weight="700" fill="#065F46">Sinks &amp; Observability</text>
    <rect x="625" y="80" width="200" height="30" rx="6" fill="#FFFFFF" stroke="#A7F3D0" stroke-width="1"/>
    <text x="725" y="100" text-anchor="middle" font-family="-apple-system, BlinkMacSystemFont, Segoe UI, Roboto" font-size="12" font-weight="600" fill="#047857">Parquet, InfluxDB, TDengine</text>
    <rect x="625" y="118" width="200" height="30" rx="6" fill="#FFFFFF" stroke="#A7F3D0" stroke-width="1"/>
    <text x="725" y="138" text-anchor="middle" font-family="-apple-system, BlinkMacSystemFont, Segoe UI, Roboto" font-size="12" font-weight="600" fill="#047857">Web UI Console (Auth Guard)</text>
    <rect x="625" y="156" width="200" height="30" rx="6" fill="#FFFFFF" stroke="#A7F3D0" stroke-width="1"/>
    <text x="725" y="176" text-anchor="middle" font-family="-apple-system, BlinkMacSystemFont, Segoe UI, Roboto" font-size="12" font-weight="600" fill="#047857">Model Context Protocol (MCP)</text>
    <text x="725" y="212" text-anchor="middle" font-family="-apple-system, BlinkMacSystemFont, Segoe UI, Roboto" font-size="11" fill="#059669">DLQ &amp; Local Sink Buffering</text>
  </svg>
</div>

---

## 2. General Questions

### What is the difference between rekuiper and LF Edge eKuiper?
- **Language**: `rekuiper` is written completely in **Rust**. Upstream LF Edge `eKuiper` is written in **Go**.
- **Compatibility**: `rekuiper` preserves 100% drop-in API, CLI, and SQL syntax compatibility with LF Edge eKuiper rulesets.
- **Garbage Collection**: Rust provides manual, deterministic memory management with zero garbage collection (GC) pauses. Go introduces periodic runtime GC sweeps that increase tail latency.
- **Resource Footprint**: `rekuiper` runs stably under 35 MB RAM even with 1,000 active SQL rules.
- **Built-in Connectors**: Native Rust Modbus, OPC-UA, Parquet, and MCP AI tooling are included in the single binary without C-shared plugins.

### Is rekuiper open source?
Yes. `rekuiper` is open source software distributed under the **Apache-2.0 / MIT** licenses.

---

## 3. Web Console & Security FAQ

### Why is the Web Console showing "Disabled" by default?
By default, the Web Console (`/ui`) is **disabled** (`enableUi: false`) to comply with zero-trust edge industrial hardening standards. This prevents unauthorized web access on industrial gateways.

### How do I enable the Web Console?
You can enable the Web Console via CLI flag, environment variable, or configuration file:

```bash
# Via CLI flag:
kuiperd --enable-ui

# With password protection and IP restrictions:
kuiperd --enable-ui --ui-password "SecretGatewayPass123" --ui-allow-ip "10.0.0.0/8,192.168.1.0/24"
```

In `etc/kuiper.yaml`:
```yaml
basic:
  enableUi: true
  uiPassword: "SecretGatewayPass123"
  uiIpAllowlist: "10.0.0.0/8,192.168.1.0/24"
```

### How does the IP allowlist work?
The allowlist accepts a comma-separated list of individual IPv4/IPv6 addresses and CIDR subnets (for example, `127.0.0.1, 10.0.0.0/8, ::1`). Any request originating outside the configured subnets immediately receives an HTTP `403 Forbidden` response.

---

## 4. Performance & Execution FAQ

### How fast is rekuiper?
In single-core benchmark tests:
- **Filter and Projection**: 150,000 to 200,000+ messages per second per core.
- **Windowed Aggregations**: Over 100,000 messages per second per core.
- **End-to-End Latency**: Median sub-millisecond (< 850 microseconds) processing time.

### How does rekuiper isolate failing payloads?
rekuiper features an integrated **Dead-Letter Queue (DLQ)**. When a malformed JSON payload or type error occurs in a stream, the engine routes the bad record to a designated DLQ sink topic without stopping the rule or dropping subsequent valid records.

---

## 5. Protocol & Connector FAQ

### Which protocols are supported out of the box?
- **Ingestion (Sources)**: MQTT (v3.1.1, v5, TLS/mTLS), HTTP/REST, Modbus TCP/RTU, OPC-UA, Apache Kafka, Redis Streams, WebSocket, ZeroMQ.
- **Delivery (Sinks)**: MQTT, HTTP Webhooks, Apache Parquet (Snappy-compressed with schema inference), InfluxDB v1/v2, TDengine 3, IoTDB, Kafka, SQL/PostgreSQL, File, Log, Memory.

### Can AI agents interact with rekuiper?
Yes. `rekuiper` includes a native Model Context Protocol (MCP) server crate (`rekuiper-mcp`). Industrial AI agents (in Claude Desktop, Cursor, or custom frameworks) can inspect active streams, dry-run SQL rules, and monitor telemetry pipelines safely.

---

## 6. Where can I find more documentation?

- [Built-in Web Console User Guide](/guide/ui/web_console_user_guide.html)
- [Declarative GitOps Mode](/operation/declarative_gitops.html)
- [Rule Designer & Visual Studio](/guide/rules/visual_studio.html)
- [Migration Guide from Go eKuiper](/operation/migration.html)
- [REST API Reference](/api/restapi/overview.html)
