# Zero-Container Observability: Embedded UI & `rekuiper top`

rekuiper includes built-in browser and terminal inspection tools designed specifically for air-gapped edge appliances and resource-constrained environments.

Traditional observability stacks require deploying external visualization containers (e.g. Grafana, Prometheus, or web dashboards) that consume hundreds of megabytes of RAM. With rekuiper:
- No companion containers are required.
- No Node.js runtime or external asset CDNs are contacted.
- All visualization operates within the existing single-binary rekuiper process.

---

## 1. Embedded Web Dashboard (`GET /ui`)

Navigating to `http://<rekuiper-host>:59999/ui` opens the embedded status dashboard.

### Key Features
- **No External Dependencies**: The page contains all HTML, CSS, and JavaScript in one file. It does not load external fonts or scripts. It operates on isolated networks without an internet connection.
- **Clear Display**: Tabular numbers prevent text jitter during updates.
- **Live Metrics**: The console displays engine uptime, active rules, active streams, message throughput, and error counts. You can select 1s, 2s, or 5s refresh intervals.
- **Declarative Mode Integration**: When declarative GitOps mode is active, the console shows a badge and a **Reload Manifest** button (`POST /declarative/reload`).
- **Rule Inspector**: You can click a rule to view details:
  - Formatted SQL query text.
  - Per-stage error counters (`codec_decode`, `schema_validation`, `sql_eval`, `data_template`, and `sink_delivery`).
  - Recent error envelopes from `GET /rules/:id/errors`.
  - A **Clear Errors** button (`DELETE /rules/:id/errors`).

---

## 2. Terminal Live Monitor (`rekuiper top`)

The CLI includes an interactive real-time process and pipeline monitor modeled after Linux `top` / `htop`.

### Usage & Syntax

```bash
# Launch interactive live monitor (default 2s interval)
rekuiper top

# Fast polling with 500ms interval
rekuiper top -i 500ms

# Focus on a specific rule
rekuiper top -r rule_chiller_monitoring

# Single snapshot output (ideal for CI/CD checks and terminal logs)
rekuiper top --once
```

### CLI Flags

| Flag | Short | Default | Description |
|---|---|---|---|
| `--interval` | `-i` | `2s` | Polling refresh interval (e.g. `1s`, `500ms`, `5s`). |
| `--once` | `-1` | `false` | Print a single frame and exit immediately without clearing the screen. |
| `--rule` | `-r` | None | Filter telemetry output to a specific rule ID. |
| `--url` | `-u` | `http://127.0.0.1:59999` | Base URL of the rekuiper server. |

### Terminal Ergonomics & Clean Exit

In continuous mode, `rekuiper top` runs within the terminal's alternate screen buffer (`\x1b[?1049h`) and hides the cursor (`\x1b[?25l`). When interrupted via `Ctrl+C` or `SIGINT`, the monitor intercepts the signal and restores the primary buffer and cursor visibility (`\x1b[?25h\x1b[?1049l`), leaving the user's shell completely clean.

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
