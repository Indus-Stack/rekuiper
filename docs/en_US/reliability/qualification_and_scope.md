# Reliability Qualification, Scope, and Verification Status

> **Release Version**: `1.0.0-rc5`  
> **Status**: **Release Candidate 5** (Pre-GA)

This document defines verified capabilities, failure recovery semantics, operational procedures, and the qualification harness for **rekuiper** (`1.0.0-rc5`).

## 1. Implemented Capabilities and Verification Status

This section contrasts verified capabilities against outstanding qualification gates for complete transparency.

### 1.1 Verified Capabilities and Passed Tests

The test suite verifies the following implemented capabilities:

| Category | Component / Capability | Implementation Details | Verification Evidence |
| :--- | :--- | :--- | :--- |
| **Catalog Persistence** | `SqliteKvStore` / Manager Mutations | Performs atomic writes to KV before committing to memory. Write locks serialize concurrent mutations. Restores catalog on startup. | `crates/rekuiper-core/tests/test_persistence.rs` (8/8 tests passed) |
| **Rule Lifecycle Consistency** | `RuleManager` and HTTP Routes | Updating running rules resumes stream processing with updated queries and persists `running` state. Mutations serialize safely. | `crates/rekuiper-server/tests/test_rule_lifecycle.rs` (5/5 tests passed) |
| **Connection Consistency** | Connection and Configuration Routes | Maintains canonical persisted representations (`name/key`) with alias resolution (`name.key`). Write failures return 500 without corruption. | `crates/rekuiper-server/tests/test_config_consistency.rs` (6/6 tests passed) |
| **Transport Security** | MQTT TLS (Rustls 0.23 / 0.24) | Supports WebPKI and custom Root CAs. Supports mTLS certificates (SEC1, PKCS#1, PKCS#8) and SNI verification. Masks sensitive tokens. | `crates/rekuiper-connectors/tests/test_mqtt_tls.rs` (9/9 tests passed) |
| **Real-Broker Integration** | Mosquitto MQTT Integration | Tests QoS 0, 1, 2, wildcards (`+`, `#`), TLS, mTLS, and offline sink caching with ordered replay (`resendPriority: CacheFirst`). | `crates/rekuiper-server/tests/iiot_mqtt.rs` (4/4 tests passed with Mosquitto) |
| **Batch Admission & Queue Capacity** | Dynamic Chunk Reservation | Scales reservation chunks to match subscriber queue capacities (`min(batch, cap/2, 512)`). Propagates 400/503 HTTP errors. | `crates/rekuiper-core/src/runtime.rs` (`test_batch_admission_with_small_queue`) |
| **Crash-Safe Cache Replay** | Durable File Retention | Retains disk replay pages until verified delivery. Enforces bounded RAM fallback when disk writes fail. | `crates/rekuiper-server/src/sink_cache.rs` (`replay_preserves_page_file_until_all_records_popped`) |
| **Unified Numeric Comparisons** | Cross-Component Ordering | Synchronizes WHERE clause float/int comparison with Bus predicate equality. Fast-path int comparison. | `crates/rekuiper-core/src/model.rs`, `crates/rekuiper-sql/src/eval.rs` |
| **Storage Overwrite Protection** | Parquet Integrity Guard | Aborts append operations on corrupt or unreadable files without deleting or overwriting historical data. | `crates/rekuiper-connectors/src/parquet_io.rs` (`test_parquet_append_error_on_corrupt_file`) |
| **Sliding Window Optimization** | Compact Storage & Pruning | Compacts accumulators (SUM 24B, MIN/MAX 32B via `serde_json::Number`). Evicts inactive groups; group cardinality is unbounded by default with an optional with_max_groups cap. | `crates/rekuiper-sql/src/eval/window.rs` (verified) |
| **Surface Security Parity** | Route Authentication & CLI/MCP | Enforces auth guard on SSE data routers. Supports `--token` / `-t` and `REKUIPER_TOKEN` across CLI and MCP. Rejects unvalidated OPC-UA encryption. | `crates/rekuiper-server/src/handlers/ruletest.rs`, `crates/rekuiper-cli`, `crates/rekuiper-mcp` |
| **RabbitMQ Delivery Contracts** | Manual Ack & Confirm Mode | Guarantees at-least-once ingestion via manual ack (`autoAck: false`) and broker publisher confirms (`confirm_select`). | `crates/rekuiper-connectors/src/rabbitmq.rs` (verified) |
| **WASM Runtime Bounding** | Concurrency Pool Limits | Limits concurrent active WASM runtime instances to 64 per module, protecting memory under heavy load. | `crates/rekuiper-core/src/wasm.rs` (`test_wasm_concurrency_bounding`) |
| **Process Qualification** | Process Harness (`kuiperd`) | Executes standalone child processes under paced traffic, measures crash losses, injects storage faults, and checks recovery criteria. | `crates/rekuiper-server/tests/qualification_harness.rs` (verified) |

### 1.2 Outstanding Qualification Blockers

The engineering team requires the following test completions before declaring GA:

1. **Continuous Endurance Soak**: The qualification harness has verified short runs (5s to 10s). A continuous 24h to 72h endurance soak run remains pending.
2. **Multi-Binary Upgrade and Rollback Qualification**: Verification across separate compilation binaries via `REKUIPER_OLD_BIN` and `REKUIPER_NEW_BIN` remains pending.
3. **Multi-Broker Interoperability**: Testing against cloud brokers (AWS IoT Core, HiveMQ, Azure IoT Hub) beyond local Mosquitto instances remains pending.

### 1.3 Experimental Features

The following connectors function in `0.501.0-beta` but remain experimental until endurance qualification finishes:

- **Kafka Source & Sink (`kafka`)**: Built on `rskafka`.
- **Redis Source & Sink (`redis`, `redisPub`)**: Built on `redis-rs`.
- **SQL Source & Sink (`sql`)**: Built on `sqlx` (PostgreSQL and SQLite).
- **WebSocket Sink (`websocket`)**: Built on `tokio-tungstenite`.

### 1.4 Unsupported Features

- **Shared Subscriptions**: The engine does not support `$share/group/topic` load balancing.
- **Clustered / Distributed Engine Mode**: Distributed Raft consensus is not supported.
- **Cross-Node State Migration**: In-memory window states do not automatically migrate across separate physical nodes on hardware failure.

## 2. Delivery Semantics and Failure Behavior

### 2.1 MQTT QoS vs. End-to-End Delivery

- **MQTT QoS 0 (At most once)**: Messages emit without broker acknowledgments. Packet loss during network disconnection is expected.
- **MQTT QoS 1 (At least once)**: Messages deliver to the broker and acknowledge via `PUBACK`. Disconnections prior to acknowledgment can produce duplicate deliveries.
- **MQTT QoS 2 (Exactly once at transport)**: A four-step handshake (`PUBLISH`, `PUBREC`, `PUBREL`, `PUBCOMP`) prevents duplicate delivery across the network hop.

> [!IMPORTANT]
> A QoS 1 or 2 MQTT sink guarantees delivery to the downstream broker once a message exits the pipeline. It does not protect in-flight RAM records from abrupt power loss without upstream persistent source replay.

### 2.2 Broker Outages and Sink Caching

When downstream MQTT brokers experience transient outages:

1. **Buffering**: When `enableCache: true` is configured, outgoing records enqueue into a FIFO cache (in memory up to `memoryCacheThreshold`, spilling to disk in `data/cache/`).
2. **Resend Priority**: `resendPriority: 1` (`CacheFirst`) replays cached outage records in strict sequence before emitting live streaming records.
3. **Resend Indicator**: Replayed records set `"resent": true` via `resendIndicatorField`.
4. **Drop Policy**: If `maxCacheSize` is exceeded, the engine drops records according to policy (`dropOldest` or `dropCurrent`) and increments Prometheus counters.

### 2.3 Abrupt Process Termination and Power Loss

| System Component | Storage Location | Behavior on Crash / SIGKILL / Power Loss |
| :--- | :--- | :--- |
| **Catalog Definitions** (Streams, Rules, Tables, Schemas, Configs) | SQLite KV (`data/sqliteKV.db` with WAL) | **Preserved**. SQLite WAL ensures committed catalog definitions survive process crashes and reload on startup. |
| **Rule Lifecycle State** | SQLite KV (`rules` namespace) | **Preserved**. Rules in `running` status before shutdown resume execution upon startup. Stopped rules remain stopped. |
| **Sink Cache on Disk** | Filesystem (`data/cache/`) | **Preserved**. Disk cache pages survive daemon restarts. Cached records replay upon broker reconnection. |
| **In-Flight RAM Records** | Tokio MPSC Channels / Memory | **Lost**. In-memory records that entered the pipeline but were not yet emitted to sinks prior to power loss are lost. |
| **Window State in RAM** | In-Memory Aggregators | **Reset**. Unmaterialized tumbling and hopping window buffers in RAM reset on restart. Previously emitted records are unaffected. |

## 3. Operational Procedures

### 3.1 Backup Procedures

Execute the following commands to back up catalog state and configuration assets:

```bash
# 1. Perform an online SQLite KV backup
sqlite3 /var/lib/rekuiper/data/sqliteKV.db ".backup /var/backups/rekuiper/sqliteKV_$(date +%Y%m%d_%H%M%S).db"

# 2. Archive configuration files
tar -czf /var/backups/rekuiper/etc_$(date +%Y%m%d).tar.gz -C /etc/rekuiper etc/
```

### 3.2 Upgrade and Rollback Procedures

To upgrade rekuiper:

1. Stop the running engine daemon: `systemctl stop rekuiper`.
2. Execute the backup procedure described above.
3. Copy the new `kuiperd` binary into the system binary path.
4. Start the engine service: `systemctl start rekuiper`.
5. Verify operational health:
   - Run `curl -s http://localhost:9081/ping` (returns 200 OK).
   - Run `curl -s http://localhost:9081/rules/status/all` to confirm that previously running rules resumed execution.
6. If regressions occur, stop the service, restore the previous binary and SQLite database snapshot, and restart the daemon.

## 4. Automated Reliability Qualification Harness

The test suite includes a process-based qualification harness (`crates/rekuiper-server/tests/qualification_harness.rs`):

- **Process Isolation**: Launches `kuiperd` child processes with isolated configurations, dedicated storage, and ephemeral ports.
- **Paced Ingestion**: Streams records at the requested rate (`msg/s`) for the configured duration (`Duration`).
- **Independent Validation**: Subscribes an independent client (`rumqttc::AsyncClient`) to the broker to verify sequence IDs and payload order.
- **Crash Loss Accounting**: Terminates the child process abruptly, measures in-flight records, and verifies restart recovery.
- **Broker Outage Simulation**: Simulates broker connection loss and verifies cache replay upon reconnection.
- **Storage Fault Injection**: Verifies that simulated `ENOSPC` disk errors return HTTP 500 without corrupting in-memory state.
- **Machine-Readable Reports**: Exports execution metrics, hardware specifications, and timings to `target/qualification_results.json`.

### 4.1 Execute Qualification Tests

Run qualification scripts from the terminal:

```bash
# Execute short qualification test on Linux or macOS
./scripts/run_qualification.sh short 10 100

# Execute short qualification test on Windows PowerShell
.\scripts\run_qualification.ps1 -Mode short -Duration 10 -Rate 100

# Execute a 24-hour soak test
./scripts/run_qualification.sh soak 86400 100 /var/log/rekuiper_soak_24h.json
```
