# Command Line Tool

The rekuiper Command Line Interface (CLI) tool manages streams, tables, rules, and plugins.

The rekuiper CLI acts as a client to the rekuiper server daemon. The server runs the core engine that processes stream and rule definitions, manages execution state, and performs I/O operations.

```mermaid
flowchart LR
    CLI[kuiper CLI client] -->|REST API :9081| Daemon[rekuiper daemon]
    Daemon --> Storage[(SQLite KV Store)]
    Daemon --> Engine[Stream Engine]
```

## Available CLI Management Guides

- [Streams](streams.md)
- [Tables](tables.md)
- [Rules](rules.md)
- [Rulesets](ruleset.md)
- [Data Import and Export](data.md)
- [Plugins](plugins.md)
- [Schemas](schemas.md)
- [Services](services.md)

## Process Exit Codes & Scripting Automation

> [!NOTE]
> **Compatibility Note: Exit Codes**
> In legacy eKuiper, the `kuiper` CLI returns exit code `0` even when a command fails (e.g. invalid syntax, rule creation failure). In `rekuiper`, the CLI strictly adheres to standard POSIX/UNIX conventions: it returns `0` on success, `1` on operational or runtime failure, and `2` on invalid CLI usage or missing arguments.
> 
> **Why we chose this difference**: Returning `0` on failure silently masks errors in shell scripts (`set -e`), Docker health checks, and CI/CD deployment pipelines. Adhering to POSIX exit code conventions ensures automated pipelines reliably detect failures and halt execution before bad definitions impact downstream systems.

