# Inspection Tools: Embedded Web Console and rekuiper top

rekuiper includes browser and terminal inspection tools for edge devices and isolated networks.

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
