---
title: Interactive OpenAPI / Swagger UI
description: Interactive OpenAPI 3.0 explorer for rekuiper REST Management API. Browse endpoints, inspect schemas, and test queries.
---

# Interactive OpenAPI / Swagger UI

`rekuiper` implements a 100% drop-in compatible REST management API. The complete API contract is specified in standard OpenAPI 3.0 format and is embedded directly into the binary on HTTP port `9081` (`/api-docs`) as well as hosted publicly below.

<div style="display: flex; gap: 12px; margin: 20px 0 28px 0; flex-wrap: wrap;">
  <a href="/swagger.html" target="_blank" style="display: inline-flex; align-items: center; gap: 6px; padding: 8px 16px; border-radius: 6px; background: var(--vp-c-brand-1); color: #ffffff; font-weight: 600; text-decoration: none; font-size: 13.5px;">
    Open Fullscreen Explorer ↗
  </a>
  <a href="/openapi.json" download target="_blank" style="display: inline-flex; align-items: center; gap: 6px; padding: 8px 16px; border-radius: 6px; background: var(--vp-c-bg-alt); border: 1px solid var(--vp-c-border); color: var(--vp-c-text-1); font-weight: 600; text-decoration: none; font-size: 13.5px;">
    Download openapi.json (250 KB)
  </a>
  <a href="/api/restapi/overview" style="display: inline-flex; align-items: center; gap: 6px; padding: 8px 16px; border-radius: 6px; background: var(--vp-c-bg-alt); border: 1px solid var(--vp-c-border); color: var(--vp-c-text-1); font-weight: 600; text-decoration: none; font-size: 13.5px;">
    Browse REST API Chapter
  </a>
</div>

::: info Live Server Execution
The interactive explorer below connects to your local `rekuiper` runtime at `http://localhost:9081`. To execute live requests against a running node, ensure your local `rekuiper` daemon is running.
:::

<div style="margin-top: 24px; border: 1px solid var(--vp-c-border); border-radius: 10px; overflow: hidden; background: #ffffff; box-shadow: 0 4px 12px rgba(15, 23, 42, 0.05);">
  <div style="padding: 10px 16px; background: #f8fafc; border-bottom: 1px solid var(--vp-c-border); display: flex; justify-content: space-between; align-items: center;">
    <span style="font-family: ui-monospace, monospace; font-size: 12px; font-weight: 600; color: #475569;">
      SPEC: OpenAPI 3.0.3 · 50+ REST Management Endpoints
    </span>
    <a href="/swagger.html" target="_blank" style="font-size: 12px; font-weight: 600; color: var(--vp-c-brand-1); text-decoration: none;">
      Launch Standalone ↗
    </a>
  </div>
  <iframe
    src="/swagger.html"
    style="width: 100%; height: 900px; border: none; display: block;"
    title="rekuiper OpenAPI Swagger Explorer"
    loading="lazy"
  ></iframe>
</div>

---

## Core Endpoint Categories

| Tag | Route Prefix | Functional Purpose |
| :--- | :--- | :--- |
| **System** | `/`, `/ping`, `/config` | Health checks, engine uptime, and runtime configurations |
| **Streams** | `/streams`, `/streams/{name}` | Dynamic stream creation, schema inspection, and lifecycle control |
| **Tables** | `/tables`, `/tables/{name}` | Lookup table definitions backed by SQLite, EdgeX, or static files |
| **Rules** | `/rules`, `/rules/{name}` | Start, stop, restart, delete, and inspect stateful SQL rules |
| **Rule Test** | `/rules/test`, `/rules/test/stop` | Real-time dry-run simulation of rules without publishing to sinks |
| **Tracing** | `/trace/rule/{id}`, `/trace/{id}` | OpenTelemetry span hierarchies and payload execution timelines |
| **Data & GitOps**| `/data/export`, `/data/import` | Full declarative pipeline bundle import/export with SHA-256 validation |
| **Prometheus** | `/metrics` | 32 Prometheus metrics counters and operator latency histograms |
