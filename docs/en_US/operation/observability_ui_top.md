# Inspection Tools: Embedded Web Console and rekuiper top

rekuiper includes browser and terminal inspection tools for edge devices and isolated networks.

<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 840 185" width="100%" height="auto" role="img" aria-label="Embedded Web Console and rekuiper top Architecture" font-family="-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif">
  <rect x="0" y="0" width="840" height="185" rx="10" fill="#f8fafc" stroke="#e2e8f0" stroke-width="1" />
  <text x="420" y="26" text-anchor="middle" font-size="14" font-weight="700" fill="#0f172a">Air-Gapped Inspection Architecture: Embedded Web Console &amp; rekuiper top</text>
  <rect x="30" y="46" width="240" height="118" rx="8" fill="#ffffff" stroke="#2563eb" stroke-width="1.5" />
  <text x="150" y="70" text-anchor="middle" font-size="12" font-weight="700" fill="#1e40af">Browser: GET /ui</text>
  <text x="150" y="92" text-anchor="middle" font-size="10" fill="#334155">Zero External CDNs or Fonts</text>
  <text x="150" y="110" text-anchor="middle" font-size="10" fill="#64748b">Visual DAG Studio &amp; Rule Inspector</text>
  <text x="150" y="128" text-anchor="middle" font-size="10" fill="#64748b">DLQ Viewer &amp; Fleet Rollouts</text>
  <text x="150" y="146" text-anchor="middle" font-size="10" fill="#059669">CIDR Allowlist &amp; Password Guard</text>
  <rect x="300" y="46" width="240" height="118" rx="8" fill="#ffffff" stroke="#ea580c" stroke-width="1.5" />
  <text x="420" y="70" text-anchor="middle" font-size="12" font-weight="700" fill="#c2410c">rekuiper Server (Port 9081)</text>
  <text x="420" y="92" text-anchor="middle" font-size="10" fill="#334155">Atomic Rule &amp; Stage Counters</text>
  <text x="420" y="110" text-anchor="middle" font-size="10" fill="#64748b">500ms Cached Host CPU &amp; Memory</text>
  <text x="420" y="128" text-anchor="middle" font-size="10" fill="#64748b">Non-Blocking try_read() Status</text>
  <text x="420" y="146" text-anchor="middle" font-size="10" fill="#ea580c">Single Binary • Zero Sidecars</text>
  <rect x="570" y="46" width="240" height="118" rx="8" fill="#ffffff" stroke="#10b981" stroke-width="1.5" />
  <text x="690" y="70" text-anchor="middle" font-size="12" font-weight="700" fill="#047857">Terminal: rekuiper top</text>
  <text x="690" y="92" text-anchor="middle" font-size="10" fill="#334155">SSH-Friendly TUI Monitor</text>
  <text x="690" y="110" text-anchor="middle" font-size="10" fill="#64748b">Live In/Out Throughput &amp; Status</text>
  <text x="690" y="128" text-anchor="middle" font-size="10" fill="#64748b">5-Stage DLQ Breakdown &amp; Envelopes</text>
  <text x="690" y="146" text-anchor="middle" font-size="10" fill="#059669">--once Snapshot Mode for CI</text>
</svg>

Key properties:
- The tools run inside the rekuiper binary without external containers.
- The web console uses no external scripts or fonts.
- All inspection functions operate without internet access.

---

## 1. Embedded Web Console (`GET /ui`)

Open `http://<rekuiper-host>:9081/ui` in a web browser to view the console.

### Key Features
- **No External Dependencies**: The page contains all HTML, CSS, and JavaScript. It does not load external fonts or scripts. It operates on isolated networks.
- **Live Metrics**: The console shows engine uptime, active rules, active streams, message throughput, and error counts. Select refresh intervals of 1s, 2s, or 5s.
- **Declarative Mode Integration**: In declarative mode, the console shows a status badge and a **Reload Manifest** button (`POST /declarative/reload`).
- **Rule Inspector**: Select a rule to view details:
  - Formatted SQL query text.
  - Per-stage error counters (`codec_decode`, `schema_validation`, `sql_eval`, `data_template`, and `sink_delivery`).
  - Recent error records from `GET /rules/:id/errors`.
  - A **Clear Errors** button (`DELETE /rules/:id/errors`).

---

## 2. Terminal Live Monitor (`rekuiper top`)

The CLI includes an interactive real-time process and pipeline monitor.

### Usage and Syntax

```bash
# Launch interactive live monitor (default 2s interval)
rekuiper top

# Fast polling with 500ms interval
rekuiper top -i 500ms

# Focus on a specific rule
rekuiper top -r rule_chiller_monitoring

# Single snapshot output (for CI checks and log files)
rekuiper top --once
```

### CLI Flags

| Flag | Short | Default | Description |
|---|---|---|---|
| `--interval` | `-i` | `2s` | Polling refresh interval (e.g. `1s`, `500ms`, `5s`). |
| `--once` | `-1` | `false` | Print a single frame and exit immediately without clearing the screen. |
| `--rule` | `-r` | None | Filter telemetry output to a specific rule ID. |
| `--url` | `-u` | `http://127.0.0.1:9081` | Base URL of the rekuiper server. |

### Terminal Control

In continuous mode, `rekuiper top` uses the alternate screen buffer and hides the cursor. When stopped with `Ctrl+C` or `SIGINT`, the command restores the standard screen buffer and cursor visibility.

### Live Display Sections

```
================================================================================
 REKUIPER TOP - Edge Stream Engine Status                     2026-10-15 11:00:00
 Server: http://127.0.0.1:59999  |  Mode: Declarative (GitOps)  |  Rules: 3 (3 running)
 Streams: 2  |  Total In: 124,590  |  Total Out: 124,582  |  DLQ Drops: 8
================================================================================

RULES
RULE ID                  STATUS     IN_REC     OUT_REC    ERRORS     LAST_ACTIVE
rule_modbus_chiller      running    54,120     54,120     0          1s ago
rule_otlp_telemetry      running    70,470     70,462     8          0s ago
rule_alert_dispatch      running    0          0          0          -

DEAD-LETTER QUEUE (DLQ) BREAKDOWN
RULE ID                  CODEC_DEC  SCHEMA_VAL SQL_EVAL   DATA_TMPL  SINK_DELIV
rule_otlp_telemetry      8          0          0          0          0

RECENT DLQ POISON-PILL ENVELOPES
[2026-10-15T10:59:45Z] rule_otlp_telemetry @ codec_decode: Invalid JSON syntax in OTLP payload
================================================================================
Press Ctrl+C to exit. Refreshing every 2s...
```
