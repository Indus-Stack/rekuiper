# Administrator Guide

This guide describes operational administration, monitoring, maintenance, and visual management for rekuiper.

## Operations Sections

- **[Configuration](../configuration/configuration.md):** Details global configuration parameters, environment variable syntax, and configuration file precedence.
- **[Version Migration](./migration.md):** Details database schema and configuration migration procedures across releases.
- **[Monitoring with Prometheus](./usage/monitor_with_prometheus.md):** Guides setup and visualization of runtime metrics using Prometheus and Grafana.
- **[Error Codes](./usage/error_code.md):** References system error codes and troubleshooting actions.
- **[Data Tracing](./usage/trace_data.md):** Guides OpenTelemetry tracing configuration and span inspection.
- **[Reliability & Stable Scope](../reliability/qualification_and_scope.md):** References MQTT delivery guarantees, broker outage caching, engine recovery, backup, and rollback procedures.
- **[Declarative GitOps Mode](./declarative_gitops.md):** Details manifest validation (`rekuiper validate`), read-only `--declarative` execution, and live file watching (`--watch`).
- **[Fleet Management & Control Plane](./fleet_management.md):** Details outbound WebSocket/Yamux gateway multiplexing, immutable configuration snapshots, and health-gated Canary rollouts.
- **[Inspection Tools (`GET /ui` & `rekuiper top`)](./observability_ui_top.md):** Details the embedded air-gapped Web Console and terminal live monitor.

<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 860 190" width="100%" height="auto" role="img" aria-label="rekuiper Operations and Administration Overview" font-family="-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif">
  <rect x="0" y="0" width="860" height="190" rx="10" fill="#f8fafc" stroke="#e2e8f0" stroke-width="1" />
  <text x="430" y="26" text-anchor="middle" font-size="14" font-weight="700" fill="#0f172a">rekuiper Operations, Observability &amp; Control Plane Pillars</text>
  <rect x="25" y="46" width="250" height="122" rx="8" fill="#ffffff" stroke="#cbd5e1" stroke-width="1.5" />
  <text x="150" y="70" text-anchor="middle" font-size="12" font-weight="700" fill="#1e40af">1. Configuration &amp; GitOps</text>
  <text x="150" y="92" text-anchor="middle" font-size="10" fill="#334155">etc/rekuiper.yaml &amp; Env Overrides</text>
  <text x="150" y="110" text-anchor="middle" font-size="10" fill="#64748b">--declarative Read-Only Guard</text>
  <text x="150" y="128" text-anchor="middle" font-size="10" fill="#64748b">Vault &amp; Env Dynamic Secrets</text>
  <text x="150" y="146" text-anchor="middle" font-size="10" fill="#2563eb">2-Minute eKuiper Cutover</text>
  <rect x="305" y="46" width="250" height="122" rx="8" fill="#ffffff" stroke="#ea580c" stroke-width="1.5" />
  <text x="430" y="70" text-anchor="middle" font-size="12" font-weight="700" fill="#c2410c">2. Live Observability</text>
  <text x="430" y="92" text-anchor="middle" font-size="10" fill="#334155">32 Prometheus Metric Families</text>
  <text x="430" y="110" text-anchor="middle" font-size="10" fill="#64748b">OpenTelemetry Span Tracing</text>
  <text x="430" y="128" text-anchor="middle" font-size="10" fill="#64748b">Embedded Web Console (GET /ui)</text>
  <text x="430" y="146" text-anchor="middle" font-size="10" fill="#ea580c">Interactive CLI (rekuiper top)</text>
  <rect x="585" y="46" width="250" height="122" rx="8" fill="#ffffff" stroke="#10b981" stroke-width="1.5" />
  <text x="710" y="70" text-anchor="middle" font-size="12" font-weight="700" fill="#047857">3. Fleet &amp; Reliability</text>
  <text x="710" y="92" text-anchor="middle" font-size="10" fill="#334155">Yamux WebSocket Control Plane</text>
  <text x="710" y="110" text-anchor="middle" font-size="10" fill="#64748b">Staged Canary Rollouts &amp; Rollback</text>
  <text x="710" y="128" text-anchor="middle" font-size="10" fill="#64748b">5-Stage Dead-Letter Queues (DLQ)</text>
  <text x="710" y="146" text-anchor="middle" font-size="10" fill="#059669">Persistent Sink Outage Cache</text>
</svg>
