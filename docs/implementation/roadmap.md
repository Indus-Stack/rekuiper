# Rekuiper: the PostgreSQL of rule engines

**Status:** proposed implementation roadmap, not an implementation report. **Reviewed:** 12 November 2026. **Baseline:** remote `origin/main` at [24848c8f88363bfe0004cc88c7db73302657aa0c](https://github.com/Indus-Stack/rekuiper/commit/24848c8f88363bfe0004cc88c7db73302657aa0c), fetched for this review. This includes [merged PR #52](https://github.com/Indus-Stack/rekuiper/pull/52). The checked-out feature branch was not used as the implementation baseline.

The ambition is a dependable, extensible, general-purpose rule engine that people can build products on for years: embedded at the edge, operated as a server, and eventually distributed. A feature taking a year is a sequencing concern, not a reason to exclude it.

**Adoption rule:** preserve rekuiper when its implementation offers the better contract. Improve it when code inspection, conformance tests, or comparable workloads demonstrate a gap. Competitor features are design evidence, not instructions to copy their internals or defaults. Performance superiority remains workload-specific until measured.

This replaces the eKuiper-only roadmap. The detailed specifications in Appendix A remain part of the implementation plan; the platform tracks below extend them into a broader architecture.

## 1. What the PostgreSQL ambition means

1. **Trustworthy semantics:** published contracts for types, nulls, time, ordering, state, failures, delivery, and recovery. Unsupported combinations fail validation. No accepted configuration option silently promises behavior the runtime does not implement.
2. **Durable execution:** recover state, timers, input progress, and pending effects together. Backups, savepoints, upgrades, and failover become routine, tested operations.
3. **Extensibility:** stable interfaces for functions, aggregates, operators, connectors, codecs, state stores, and language frontends. Extensions carry compatibility metadata, permissions, tests, and upgrade paths. PostgreSQL's versioned extension packaging is a useful precedent. [PostgreSQL extension documentation](https://www.postgresql.org/docs/current/extend-extensions.html)
4. **A coherent platform:** SQL, visual graphs, decision tables, and embedded APIs share planning, execution, diagnostics, and governance. Different evaluation models stay explicit: a streaming window and a synchronous decision have different contracts.
5. **Operational clarity:** every rule explains its execution plan and effective delivery guarantee. Operators can locate a stalled partition, reproduce a decision, inspect an outstanding effect, and roll back safely.
6. **An ecosystem:** compatibility policy, reference connectors, migration tools, qualification suites, reproducible benchmarks, documentation, and a maintainable contribution process.

Target three deployment profiles: an embedded library, a standalone edge/server process, and a coordinated cluster. The first two must continue working without a broker or external control plane. Distributed execution is planned after local recovery and fencing are correct.

## 2. Comparative landscape and verified baseline

This combines source inspection of rekuiper with primary documentation from the systems below. It is not a fresh benchmark of every engine, certification of every connector, or exhaustive review of every upstream issue. Pin upstream versions for implementation fixtures. Review licensing separately before importing code.

Telegraf is primarily a telemetry agent. Redpanda **Connect** is the processing comparison; the Redpanda broker is transport/storage infrastructure. Decision engines, stream processors, and telemetry pipelines contribute different lessons.

| System | Capability worth studying | Decision and destination |
|---|---|---|
| eKuiper | Edge SQL compatibility, delayed analytics, recent correctness fixes. [2.4.2](https://github.com/lf-edge/ekuiper/releases/tag/v2.4.2), [2.5 alpha.5](https://github.com/lf-edge/ekuiper/releases/tag/v2.5.0-alpha.5) | Preserve stronger existing behavior; carry forward all earlier findings in Appendix A. |
| Telegraf | Per-plugin collection scheduling/jitter and output flush/buffer controls. [Plugin options](https://docs.influxdata.com/telegraf/v1/configuration/plugin-options/) | Extend scheduling, telemetry fidelity, and connector qualification. P02, P07, P15. |
| Redpanda Connect / Benthos | Acknowledgement-based delivery, batching, and configuration tests. [Delivery](https://docs.redpanda.com/redpanda-connect/guides/delivery_semantics/), [batching](https://docs.redpanda.com/connect/configuration/batching/), [tests](https://docs.redpanda.com/connect/configuration/unit_testing/) | Adopt explicit acknowledgement composition and fixture-driven testing. Keep SQL and native Rust execution. P02–P03, P06. |
| Vector | Component-specific guarantees and bounded memory/disk buffering. [Guarantees](https://vector.dev/docs/architecture/guarantees/), [buffers](https://vector.dev/docs/architecture/buffering-model/) | Adopt capability declarations and durable queue contracts. P01–P03. |
| Fluent Bit | Input backpressure and memory/filesystem limits; some full-queue policies discard oldest chunks. [Backpressure](https://docs.fluentbit.io/manual/administration/backpressure) | Adopt capacity accounting and observability. Preserve explicit loss choices. P02–P03. |
| OpenTelemetry Collector | Exporter retry queues and persistent storage, with documented loss boundaries. [Resiliency](https://opentelemetry.io/docs/collector/resiliency/) | Extend existing OTLP and qualify outages and telemetry preservation. P03, P07. |
| Apache Flink | Coordinated recovery, savepoints, watermark idleness/alignment. [State](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/stateful-stream-processing/), [watermarks](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/event-time/generating_watermarks/), [savepoints](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/checkpoints_vs_savepoints/) | Adopt state/time contracts; preserve lightweight standalone execution. P03–P05, P14. |
| Kafka Streams | Explicit processing guarantees, including Kafka-scoped transactional processing. [Configuration](https://kafka.apache.org/41/streams/developer-guide/config-streams/) | Add real consumer-group lifecycle and later transactional Kafka pipelines. Qualify external effects separately. P03, P14. |
| ksqlDB | Event-time windows, grace periods, final-result emission. [Time and windows](https://docs.confluent.io/platform/8.4/ksqldb/concepts/time-and-windows-in-ksqldb-queries.html) | Define final versus changing results and late-data contracts. P05, P09. |
| Materialize | Incrementally maintained views and reusable indexed arrangements. [Views](https://materialize.com/docs/fundamentals/concepts/views/), [arrangements](https://materialize.com/docs/fundamentals/concepts/arrangements/) | Build maintained relations, shared indexes, and subscriptions on a changelog model. P09–P10. |
| RisingWave | Streaming SQL and plan-specific emit-on-window-close support. [Plan-shape limitations](https://github.com/risingwavelabs/risingwave/issues/18445), [release history](https://github.com/risingwavelabs/risingwave-docs/blob/main/changelog/release-notes.mdx) | Validate exact window/join combinations; do not infer universal support from a feature name. P05, P09. |
| Spark Structured Streaming | Late data, output modes, and checkpoint compatibility constraints. [Streaming API](https://spark.apache.org/docs/latest/streaming/apis-on-dataframes-and-datasets.html), [compatibility notes](https://spark.apache.org/docs/latest/streaming/additional-information.html) | Adopt explicit state/schema compatibility and result modes. P03, P05, P09. |
| Apache NiFi | Per-connection backpressure, provenance, and replay. [User guide](https://nifi.apache.org/nifi-docs/user-guide.html) | Extend Studio, trace, and DLQ into bounded provenance and controlled replay. P02, P06, P11. |
| Node-RED | Error/status handling and configurable context persistence. [Errors](https://nodered.org/docs/user-guide/handling-errors), [context](https://nodered.org/docs/user-guide/context) | Keep native DAG operators; improve reusable components and state visibility. P06, P08, P12. |
| EMQX rule engine | Action-level integration controls, fallback actions, and whole-rule tests. [Actions](https://docs.emqx.com/en/emqx/latest/develop/data-integration/data-bridges.html), [testing](https://docs.emqx.com/en/emqx/latest/develop/data-integration/rule-get-started.html) | Extend action identity and testing; preserve existing nonrecursive DLQ behavior. P06, P11; A6. |
| Esper | Temporal patterns, repetition, timer guards. [Esper 9 reference](https://esper.espertech.com/release-9.0.0/reference-esper/pdf/esper_reference.pdf) | Expand existing CEP with explicit, bounded temporal semantics. P04, P08. |
| Siddhi | Patterns/sequences, absence detection, output-rate policies. [Query guide](https://siddhi.io/en/v5.1/docs/query-guide/) | Add keyed temporal and suppression semantics. P08. |
| Kapacitor | Alert lifecycle, recovery, inhibition, and state-change-only notification. [Alert node](https://docs.influxdata.com/kapacitor/v1/reference/nodes/alert_node/) | Build a coherent industrial alert model beyond individual predicates. P08. |
| Drools / DMN | Decision tables and structured decision models. [DMN](https://docs.drools.org/latest/drools-docs/drools/DMN/index.html) | Add a conformance-driven decision frontend; scope forward chaining separately. P16. |
| OPA / CEL | Tested policy bundles and embeddable expressions. [OPA bundles](https://www.openpolicyagent.org/docs/management-bundles), [tests](https://www.openpolicyagent.org/docs/cli), [CEL](https://cel.dev/) | Adopt versioned policy artifacts and bounded evaluation; preserve language semantics. P11–P13, P16. |

### What remote main already provides

“Present” means implemented code, not certification of every production guarantee. All source links below are pinned to the reviewed main commit.

| Area | Evidence and implication |
|---|---|
| Record/admission path | [Shared records][rk-model] and [bounded runtime][rk-bus] already provide a fast native path. Preserve shared ownership and all-or-none nonblocking admission. |
| Event time | [Windows][rk-windows] track per-input maxima and their minimum, including unseen/closed inputs. Extend idle-partition and late-result handling rather than rebuilding watermarks. |
| Kafka | [The partition reader][rk-kafka] treats group ID as informational, advances a local offset before decode/admission, and leaves custom metadata empty. Real groups, durable progress, and richer metadata remain work. |
| RabbitMQ | [The connector][rk-rabbit] acknowledges input after pipeline admission and uses publisher confirms for output. Preserve these features; admission acknowledgement is not end-to-end completion. |
| Recovery | [Runtime][rk-runtime] saves an analytic-state map independently of full source/window/output progress. [Sink cache][rk-cache] explicitly lacks fsync and may lose unwritten records on crash. |
| Storage | [KV storage][rk-kv] includes real SQLite/memory transactions. The trait's default transaction method is sequential operations. SQLite uses synchronous=NORMAL. Stronger recovery must require verified atomicity and an explicit durability level. |
| Buffering | [Sink batching][rk-sinks] supports count/linger. [RateLimitNode][rk-operators] stores max_buffer as _max_buffer; buffer mode waits for tokens rather than enforcing that distinct configured queue. |
| Testing | [Ruletest][rk-ruletest] provides simulation/SSE but directly invokes the evaluator. Validate/explain/schema/trace/CLI/Studio already exist. Production-executor parity is the next objective. |
| DLQ | [Failure handling][rk-dlq] supports drop/DLQ/halt, bounded history, and nonrecursive external dispatch. The external path spawns a task per event; bound outstanding dispatch under outages. |
| Schemas/OTLP | [Schema APIs][rk-plugins] accept protobuf/custom, while Avro creation is explicitly unsupported. [OTLP][rk-otlp] already handles JSON metrics/logs/traces and histograms. Extend fidelity and formats. |
| Management/extensions | [Declarative reconciliation][rk-declarative] has hashing and a rollback watchdog; fleet, plugins, and WASM exist. Extend them into transactional activation, a stable SDK, and coordinated recovery. Fleet management is not distributed execution. |

## 3. Architecture and implementation tracks

Keep native Rust execution, shared values, bounded queues, embedded/standalone operation, industrial connectors, SQL plus DAG authoring, WASM isolation, and management tooling. Existing Parquet, local vector math, ASOF indexing, accumulator functions, and watchdogs do not need duplicate implementations.

Converge incrementally on a typed plan with stable operator identities, state schemas, input/output modes, determinism, resource budgets, and delivery requirements. SQL, DAG, decision, and policy frontends target explicit execution modes in that plan. The runtime owns ordered data/control processing, state, timers, and effects. A versioned catalog binds schemas, plans, connections, and extension versions. Introduce these boundaries through adapters around current execution, after conformance fixtures exist.

The interfaces and commands proposed below are design targets, not currently supported API claims.

### P01 — Semantic contracts and capability-aware planning

**Build:** a compatibility specification and conformance corpus around existing sql/{ast,parser,validator,eval}, core/model, and engine planning.

1. Specify numeric comparison/coercion, overflow, null versus missing, object paths, ordering, timestamp units, time zones, aggregate state, errors, and append/update/retract results. Preserve current deliberate behavior and record corrections as versioned compatibility changes.
2. Add typed planning and function signatures without removing schemaless input support. Resolve fields/types where known; make remaining runtime checks explicit.
3. Describe connector/operator capabilities in one registry: replay, acknowledgement, transactions, snapshot, determinism, result modes, ordering domain, and resource limits. Drive validation, documentation, Studio, MCP, and explain from it.
4. Reject unsupported combinations instead of silent fallback. A durable rule cannot downgrade because one extension lacks snapshots or one sink cannot accept updates.
5. Add stable error codes and structured location/operator context. Audit accepted-but-unused options and success responses after failed persistence, starting with schema creation's ignored register_schema result.

**Gate:** the same plan and diagnostics through SQL/DAG/server/CLI entry points, a versioned semantic corpus, and failure-injection tests proving failed durable mutations do not return success. This is a foundation for every advanced track.

### P02 — Resource budgets, buffers, batching, and cancellation

**Build:** extend core/runtime, engine/{operators,sources,sinks}, connector options, and metrics. Implement A3's explicit buffer policy while retaining blocking defaults.

1. Inventory ingress, subscribers, DAG edges, rate limiters, joins/LEAD, sink batches, retry/cache, lookups, trace, and external DLQ queues. Define count/byte budgets, overflow, cancellation, and ownership. Account conservatively for shared payloads; distinguish logical charged bytes from physical allocations.
2. Resolve the rate-limit max_buffer mismatch: implement the declared bounded queue or expose current waiting behavior as backpressure and reject unsupported queue settings. Preserve fairness/order and responsive stop without a task per waiting record.
3. Add byte limits to existing count/time batching. Specify oversize-single-record behavior, shutdown/checkpoint flush, per-action concurrency, partial success, and ordered/unordered retries.
4. Return typed outcomes: delivered, retryable, permanent failure, and ambiguous delivery. Use bounded Retry-After handling, capped backoff/jitter, and a total deadline. Do not retry an already accepted effect because response-body draining failed.
5. Bound async enrichment/lookup calls and pending bytes. Preserve existing deadlines; add cancellation to every wait. Pool compatible clients and keep failures isolated to the relevant action/rule.
6. Add collection/flush jitter and explicit polling cadence to poll-based connectors. Preserve event timestamps, prevent overlapping polls by default, expose missed intervals, and spread fleet reconnect load.

**Gate:** stalled sinks and mixed-size bursts remain within measured memory/disk limits, stop promptly, and reconcile accepted/emitted/retained/rejected/dropped counts. Benchmark the unchanged default path. Reliable plans reject lossy queues; control messages cannot be evicted.

### P03 — Flagship A: durable execution and recoverable effects

**Build:** implement A9's checkpoint protocol, strengthened by the delivery/outbox requirements here. Primary files are core/{model,runtime,manager,kv}, engine/{runtime,windows,joins,dag,sinks,sources}, sink_cache, and broker connectors.

**Stage 1 — Guarantee model and acknowledgement lifecycle.** Distinguish admitted into memory, durably accepted, processed into durable state, delivered, and transactionally committed. Add an internal reliable-path envelope/token without bloating default records. Compose one-to-many decoding, fanout, filtering, branches, and multiple actions. Intentional filtering can be terminal success; processing errors and failed DLQ delivery are not.

Window inputs can become durably accepted when unclosed state is checkpointed; requiring every input to wait for a future window output would stall progress. Couple input progress to durable state and outstanding effects. Explain reports the effective whole-rule guarantee and limiting component.

**Stage 2 — Storage and coordinator.** Require actual transactional behavior from checkpoint stores; replace the misleading sequential trait default with an explicit weaker capability or mandatory implementation. Preserve existing SQLite transactions. Define and verify synchronization/write ordering for the claimed failure model: process crash and machine/power loss are different. Do not call current unsynced cache writes or synchronous=NORMAL power-loss-safe without evidence.

Use versioned envelopes, checksums, plan/schema/extension fingerprints, stable operator IDs, source positions, output boundaries, and an atomic committed pointer with a retained prior generation. Fence stale async saves on stop/update/delete. Surface corruption and persistence failure. Legacy analytic-map import requires an explicit starting position and is not coordinated replay.

**Stage 3 — Recoverable single-source pipeline.** Begin with one replayable Kafka partition, a qualified SQL operator chain, and acknowledged sinks. Order data/barriers/EOF, capture state in its owner task, then asynchronously persist immutable snapshots. Flush and acknowledge prior outputs before checkpoint commit. A crash before commit restores the prior checkpoint and can duplicate effects already delivered; certify this as at-least-once.

Move all window/CEP/join/LEAD state into versioned snapshots, including pending rows, accumulators, watermarks, timers, and unresolved delayed values. Rebuild monotonic timers from stable logical deadlines and a specified downtime policy.

**Stage 4 — Durable outbox.** Unify reliable pending output with named cache ownership from A6. Store effect ID, payload/schema, destination identity, order key, attempts, and ack state. Commit operator state, source progress, and outbox references in one recoverable protocol. A database pointer does not make an unsynced payload file durable.

Use checksummed bounded segments, safe torn-tail recovery, checked flush/fsync, ack journals, and compaction after durable completion. Keep the old cache as a labelled compatibility mode. Storage full/error must backpressure or stop reliable execution, never silently evict acknowledged work.

**Stage 5 — Broker lifecycle.** Implement Kafka consumer groups as a separate mode with a suitable client: partition ownership epochs, rebalances, pause/resume, contiguous committed positions, and fencing. Keep direct-partition mode and explicit offsetReset. Document precedence of checkpoint positions and broker commits. Never commit beyond recoverable state/effects.

For reliable RabbitMQ mode, move acknowledgements from memory admission to durable processing/outbox or confirmed delivery; bound prefetch and define requeue behavior. Audit MQTT and other protocols separately: transport QoS does not prove rule-to-action reliability. Shared-source rules need independent replay cursors or a durable subscription log.

**Stage 6 — Multi-input and savepoints.** Add bounded barrier alignment with timeout/abort/unblock. Drain or snapshot async lookups; record nondeterministic results when replay requires them. Cover every reachable sink. Reject unsupported graph cycles until their feedback/channel-state protocol is specified.

Add consistent backups, user savepoints, restore-to-new-rule, and explicit state/schema migration. Preserve catalog, schemas, extension identities, state, and pending effects together. Test restore on the next supported release and retain a documented rollback path.

**Stage 7 — Scoped transactions and actuation.** Add Kafka consume-transform-produce transactions and transactional database effects where boundaries permit. Use stable idempotency keys for cooperative services. Arbitrary webhooks and device commands can have ambiguous outcomes; add a command ledger and reconciliation state rather than promise universally exactly-once physical effects.

**Gate:** real-broker crash tests before/after barriers, snapshot writes, outbox append, sink ack, commit, and compaction; disk-full/corruption; rebalances; retention gaps; stale saves; old-version restore; pending windows/LEAD; and missing extensions. Prove no missing records within each stated contract, expected duplicate boundaries, state consistency, bounded resources, and measured recovery time. Qualify combinations individually.

### P04 — Flagship B: ordered delayed analytics and streaming LEAD

**Build:** implement the full A8 specification. Keep delayed state in the SQL rule task, with stable call IDs and versioned snapshots. Extend existing AST/OVER planning rather than pretending a scalar evaluator can wait for future input.

Origin and probe evaluation contexts, UNTIL-before-WHEN, null/offset defaults, alias dependencies, probe visibility through WHERE, per-partition matching, and global output order are part of the contract. Preserve existing LEAD forms or provide an explicit compatibility transition.

Bound all held rows/bytes, including resolved rows waiting behind unresolved ones. Blocking input at the cap can deadlock the future event needed to resolve the queue; fail or use a separately specified terminal policy. Hold watermarks behind delayed output. EOF defaults unresolved values; idleness and stop do not mean EOF.

**Gate:** A8's differential, ordering, cap, alias, EOF, and restore fixtures. Durable mode depends on P03. Generalizing delayed expressions to DAG/multi-input/window combinations requires explicit semantics and capability checks.

### P05 — Event-time progress and result modes

**Build:** extend the existing minimum-of-input-watermarks behavior in engine/windows.

1. Extract common time policy from repeated loops. Track source partitions/edges, unseen inputs, closed inputs, idle timeout, and reactivation. Preserve watermark advancement by rows rejected by WHERE and monotonic progress.
2. Specify out-of-order delay separately from grace/retention after first output. Preserve the existing lateTolerance meaning during migration.
3. Offer explicit late-data policies: counted drop, side output/DLQ, or correction for compatible plans. Define final/append, update/upsert, and retract/changelog results. Corrections require retained state, stable keys, and compatible sinks from P09.
4. Add watermark alignment to bound fast-input state growth without deadlocking idle inputs. Propagate progress through joins, async lookups, delayed expressions, and DAG branches.
5. Define event-time versus processing-time timers, downtime behavior, EOF, stop, and source outage separately. Persist policy and deadlines.

**Gate:** deterministic cases for skewed/unseen/idle/reactivated inputs, boundary timestamps, late corrections, EOF, and restart across every supported window shape. Default final-window behavior remains compatible.

### P06 — Production-equivalent tests, provenance, DLQ, and replay

**Build:** extend handlers/ruletest, CLI, engine/dlq, trace/status, and Studio. Reuse current tools.

1. Run fixtures through the production planner/operators with synthetic inputs, controllable time/watermarks/EOF, isolated state, mock lookups, and capture sinks. Current evaluator-only simulation must not imply coverage of a production window or DAG it does not execute.
2. Add versioned fixture files with catalog/rules, ordered inputs/metadata, clock advances, expected rows/effects/errors, and state assertions. A proposed CLI test command provides deterministic seeds, failure diffs, JUnit/JSON, and failing exit status. Real effects require explicit integration-test mode.
3. Replace external DLQ task-per-event dispatch with bounded workers and cancellation. Envelopes carry source position, rule/operator version, stage, error category, attempts, and redacted provenance. Durable poison-message disposition participates in P03. Preserve nonrecursion and report DLQ delivery failure.
4. Add opt-in sampled provenance with byte/age limits: event identity, branch decisions, transformations, errors, effect identity, and ack state. Avoid full payload retention by default; support redaction and permissions.
5. Make replay a new execution with original lineage, selected rule version, target, rate limit, deduplication policy, and audit record. Start with dry replay into capture sinks; live replay has explicit authorization. Effect retry and source replay remain different operations.
6. Extend existing metrics with queue bytes/age, watermark lag, checkpoint health, cursor lag, pending effects, retries, and per-operator cost. Avoid event/device IDs in unbounded labels. Readiness reflects selected required pipeline capabilities.

**Gate:** identical fixture results via CLI/server, timers and restart included; DLQ outages remain bounded; dry replay cannot emit live effects; diagnostics identify the deliberately stalled stage.

### P07 — A complete telemetry processing profile

**Build:** extend the existing JSON OTLP connector and HTTP routes, not a second telemetry engine. Use Telegraf's scheduling controls and the Collector's retry/queue separation as references.

1. Define lossless internal mappings for resource and instrumentation-scope attributes, metric kind/unit, temporality/monotonicity, histogram buckets/exemplars, log severity/body, trace/span linkage, status, and nanosecond timestamps. Audit current encode/decode paths field by field before declaring individual gaps.
2. Add OTLP protobuf/HTTP and gRPC where missing, compression, content-type validation, request limits, and correct partial-success/retry behavior. Keep JSON compatibility. Support exponential histograms and other standard shapes through explicit capability tests rather than flattening unknown types silently.
3. Add metric-native transforms: label filtering/redaction, cardinality budgets, delta/rate conversion with reset handling, downsampling, and histogram-safe aggregation. These need keyed state and checkpoint semantics; arbitrary summation of cumulative counters is not an acceptable shortcut.
4. Expose collection schedules, jitter, batch bytes/count/time, export queues, and resource limits through a coherent telemetry profile using P02–P03.
5. Qualify OTLP and Prometheus/Influx interoperability with reference senders/receivers. Integrate existing agents where that is better than duplicating every host-metric input. Remote write, line protocol, and scrape support enter the connector program in P15.

**Gate:** reference payload round trips preserve every declared field and timestamp precision; cumulative/reset and histogram fixtures are correct; backend outages recover within the stated durable profile; cardinality explosions are bounded. Publish supported signal/transport/version combinations.

### P08 — Industrial alerts and a general temporal rule model

**Build:** extend existing sequence_match/count, watchdog, rate_limit, SQL state, and native DAG operators. Esper/Siddhi pattern models and Kapacitor alert lifecycle provide useful semantics.

1. Specify keyed alert states such as normal, pending, firing, acknowledged, and recovered. Separate detection state from notification state so a sink outage does not change whether a condition is true.
2. Implement explicit deadband/hysteresis, debounce, sustained condition, cooldown, repeat reminders, suppression/inhibition, and recovery notifications. First establish which cases current SQL/functions already express correctly; package those as reusable rules and add operators only for demonstrated gaps.
3. Generalize absence detection from an unkeyed watchdog to per-entity expectations and bounded temporal patterns: sequence, repetition, negative conditions, and bounded-until. Define overlap, match consumption, late-event behavior, state expiry, and event-time versus processing-time semantics.
4. Add keyed timers, state cardinality/byte limits, TTL, and snapshot/restore through P03. Preserve stable alert/effect IDs through restart; deterministic time fixtures must cover missed deadlines and flapping.
5. Provide reusable industrial rule packages with typed inputs and validated defaults. SQL and Studio authoring use the same operators; explaining a decision includes the observations and state transition that caused it.

**Gate:** tests for threshold chatter, missing devices, clock jumps, late recovery, duplicate input, restarted timers, notification outage, and many keys. Recovery events occur once per defined transition within the chosen delivery contract. Do not advertise new absence semantics merely because a watchdog exists.

### P09 — Changelogs, CDC, maintained relations, and reusable state

**Build:** a relational state layer alongside append-only streams. Materialize, RisingWave, ksqlDB, and Spark inform the design; current local tables and ASOF paths remain useful.

1. Introduce an explicit change envelope with key, operation, old/new values where available, source transaction/position, and event time. Preserve a fast append-only specialization.
2. Define insert/update/delete, duplicate, null-key, tombstone, and snapshot-boundary semantics. Add database change capture via qualified adapters, initially consuming standard CDC envelopes before implementing a native database replication client.
3. Maintain keyed lookup tables from CDC with consistent snapshot-to-log handover. Add temporal/versioned lookup and interval joins; distinguish event-time historical lookup from lookup of the latest dimension value.
4. Build incrementally maintained filters/projections, grouped aggregates, joins, and reusable indexed relations. Track retractions and shared state ownership. Non-invertible aggregates may need retained multisets or recomputation; specify memory/storage costs.
5. Expose named views, consistent reads, and resumable subscriptions with a visible completeness frontier. Propagate changes only to compatible sinks. Reuse P03 snapshots and P11 catalog dependencies.
6. Add SQL sink batch transaction and upsert/delete semantics at the actual engine call site. A connector helper alone does not prove that a production rule executes an atomic batch. Retry ambiguous commits using stable keys/transaction identity where available.

**Gate:** full recomputation is the differential oracle for randomized insert/update/delete workloads. Test CDC snapshots plus concurrent updates, deletes, duplicate log records, schema evolution, restart, temporal joins, and sink changes. No missing or double-applied state transitions within the specified source contract.

### P10 — Optimizer and performance engineering

**Build:** extend planning/explain and existing benchmark harnesses. A11 details the immediate hash-join experiment.

1. Compile expressions and key extraction once; cache immutable plans by semantic fingerprint. Specialize hot paths for known types without changing mixed-type/null behavior.
2. Add hash/indexed joins, predicate/projection pushdown, common-expression reuse, incremental aggregates, shared scans/arrangements, and batch/vector execution where measurements justify them.
3. Preserve the existing indexed ASOF path and nested-loop fallback for small/non-equality/unhashable inputs. Preserve match multiplicity and observable output ordering. Pushdown must respect probe visibility, state updates, and event-time progress.
4. Add resource-aware strategy selection using row counts, key cardinality/skew, state bytes, and selectivity. Explain shows estimates, selected strategy, fallback reason, and actual sampled operator cost.
5. Benchmark edge and server profiles separately, including small rows, wide records, skew, multi-rule fanout, heavy expressions, joins, and durable state. Retain shared-value and single-record ingress advantages.
6. Consider adaptive reoptimization only at a safe deployment/checkpoint boundary with state migration. Do not change a stateful physical plan underneath live state without a protocol.

**Gate:** result equivalence before speed claims. Report sustainable source-to-observed-sink throughput, p50/p95/p99 latency, RSS, CPU, disk, loss/duplicates, and recovery cost with matching delivery guarantees. Use pinned builds/configs, independent event IDs, warmup, repeated runs, raw results, and equal resource limits. Extend existing cross-engine scripts; historical provisional benchmark documents are not evidence of universal superiority.

### P11 — Versioned catalog, atomic activation, and safe upgrades

**Build:** extend core managers/KV, declarative reconciliation, fleet, schema/plugin managers, and stable identities from A6. Existing hashing and rollback are foundations.

1. Make rules, streams, tables/views, schemas, connections, extensions, and named actions catalog objects with immutable versions, dependencies, and ownership. Use stable IDs independently of display names.
2. Compile a candidate generation against a consistent catalog snapshot. Validate capabilities, fixtures, secrets references, resource reservations, and migrations before activation. Readers must not observe a half-updated dependency graph.
3. Atomically commit the catalog generation; drain/checkpoint and fence old runtime owners before activating conflicting writers. Persist activation progress so a crash can resume or roll back safely. Database atomicity alone does not atomically swap external source consumers.
4. Add deployment diffs, dry runs, compatibility diagnostics, and explicit state/backlog migration plans. Preserve healthy unchanged rules. Existing rollback cannot undo an external webhook; account for effects through P03.
5. Package rules, schemas, test fixtures, extension digests, and metadata as versioned artifacts. Verify signatures when configured, pin versions, and separate secret references from artifacts. Avoid simultaneous GitOps/API ownership without conflict rules.
6. Extend fleet canaries and rollback with capability/version checks, state compatibility, health windows, and auditable generation IDs. Include catalog plus pending effects in backup/restore workflows.

**Gate:** interrupted updates never expose mixed generations; stale owners cannot commit state/effects; failed candidates leave the old generation usable; rename/reorder preserves backlog ownership; compatible upgrades restore state; incompatible changes require a visible migration. Test across supported release pairs.

### P12 — Extension SDK, reusable components, and embedding

**Build:** evolve core/plugin, core/wasm, connector abstractions, planner registries, and host APIs. Definitions persisting across restart do not by themselves constitute a stable executable extension lifecycle.

1. Publish versioned interfaces for scalar/aggregate/table functions, stateful operators, sources/sinks, codecs, and state backends. Include type signatures, determinism/side effects, cancellation, resource charging, snapshot versions, and capability declarations.
2. Package extensions with dependency constraints, target engine range, artifact digest, permissions, migrations, docs, and fixtures. Resolve dependencies and load verified artifacts at bootstrap. Track references so an in-use version cannot disappear during execution.
3. Keep WASM as a bounded extension path: instruction fuel, memory, host-call limits, deadlines, and declared I/O permissions. Add out-of-process adapters for broader language ecosystems. Native in-process extensions are an explicitly trusted tier with tighter ABI/version coupling.
4. Support reusable parameterized SQL/DAG components with typed ports, isolated state identities, compiled dependency graphs, and example tests. Prevent recursive expansion or dependency cycles unless deliberately supported.
5. Extract a stable Rust embedding facade without requiring the server. Add versioned C ABI and Python/JavaScript bindings after lifecycle, thread safety, cancellation, memory ownership, and error contracts are fixed.
6. Provide an extension template and conformance kit, including recovery, leak, malformed input, and compatibility tests. Catalog listings distinguish maintained/certified capabilities from experimental adapters.

**Gate:** a third party implements a connector and stateful operator without editing engine internals; they survive restart and a supported upgrade; revocation/uninstall cannot corrupt live plans; sandbox violations are contained. Measure extension overhead against native paths and publish the tradeoff.

### P13 — Governance, isolation, and trustworthy operations

**Build:** extend current authentication, secrets, management routes, tracing, and runtime budgets. Inventory current enforcement first; do not equate bearer authentication with complete multitenant authorization.

1. Introduce namespaces and scoped identities for catalog, data, state, checkpoints, effects, and extensions. Authorize read, edit, run, deploy, replay, secret-use, extension-install, and administration separately across REST/CLI/MCP/Studio/fleet.
2. Implement RBAC with explicit policy hooks where richer decisions are needed. Support external identity integration; retain a simple standalone mode.
3. Apply per-namespace CPU/task, memory, disk, input-rate, state, and outbound-concurrency quotas. Fair scheduling and backpressure must contain noisy neighbors; untrusted extensions may require process isolation.
4. Centralize egress, filesystem, secret-use, and extension permissions. Keep existing restrictions and A2's anchored file operations. Redact logs/provenance and scope secret rotation without changing destination identity.
5. Record administrative changes, artifact versions, replay, and effect reconciliation in a bounded/exportable audit trail. Define retention, backup encryption, and incident diagnostics.
6. Add reliable liveness/readiness, capability/version endpoints, and operational commands for checkpoint, drain, backup, inspect, and restore. Document privileges and failure modes for each.

**Gate:** cross-namespace access tests through every control surface; quota exhaustion cannot corrupt another rule; revoked credentials/extensions cannot continue unauthorized work; redaction survives errors and tracing; restore enforces ownership and permissions.

### P14 — High availability and distributed rule execution

**Build:** a planned long-horizon capability, not merely more fleet APIs. Depend on P03, P05, P11, and P13.

1. Start with active/passive recovery of a qualified single-node rule. Persist ownership epochs/leases and fence stale source readers, state writers, and effects. A lease timeout alone is insufficient if an old process can still write to a destination.
2. Choose a proven coordination/consensus implementation and state storage contract through an ADR. Define quorum, failure domains, retention, and disaster recovery. Keep standalone mode independent.
3. Add partitioned execution by explicit keys and ordering domains. Specify data exchange, partition ownership, per-partition watermarks, barrier alignment, backpressure, and reshardable operator state.
4. Move/rescale state through savepoints or a bounded online-transfer protocol. Fence old ownership, replay from the correct cursor, and reconcile outbox effects. Support skew handling and deliberate placement constraints.
5. Separate control-plane availability from data-plane progress. A control-plane outage should have a specified effect on existing rules, deployments, secrets refresh, and leases.
6. Add coordinated rolling upgrades, cluster backups, multi-zone recovery, and eventually cross-region failover with explicit consistency and RPO/RTO. Do not imply synchronous global consistency without its protocol and cost.

**Gate:** kill leaders/workers, partition the network, lose storage quorum, delay heartbeats, and rescale during load. Prove ownership fencing, state/result equivalence, duplicate/loss contract, bounded alignment, and measured failover. Publish which plan shapes can distribute; unsupported global operators remain explicit.

### P15 — Interoperability, schemas, and the connector ecosystem

**Build:** A4/A5 provide immediate Kafka metadata/JSONL work; extend the existing schema registry and connector framework.

1. Preserve binary keys/headers, offsets as integers, source time, ingestion time, schema identity, and trace context. Preserve duplicate headers where the protocol/client exposes them; full-fidelity mode must select a capable client. Share metadata across decoded rows. Metadata is not an acknowledgement token.
2. Version schemas by immutable fingerprint and dependencies; define compatibility/default/null rules and bind a running plan to a version. Extend protobuf/custom with JSON Schema, Avro and registry wire formats, then additional codecs through P12.
3. Add external schema-registry adapters with bounded caches, pinned IDs, authentication, invalidation, and outage behavior. Test evolving schemas during replay and rolling upgrades.
4. Maintain a connector capability matrix, not just a list of names: source/sink/lookup, auth/TLS, ordering, replay, ack boundary, batches/partial failure, schema modes, cancellation, and recovery certification.
5. Qualify and deepen existing industrial and messaging connectors first. Add adapters for NATS/JetStream, object storage, CDC, Prometheus remote write/scrape, and other demanded ecosystems through the SDK. Each needs an end-to-end reference workload and protocol tests.
6. Support embedding/REST clients and migration tools for eKuiper SQL, selected Node-RED flows, and telemetry/pipeline configurations. Translate only proven equivalent subsets and report unsupported semantics; do not promise arbitrary config conversion.
7. Treat PostgreSQL wire interoperability as a planned optional access layer for catalog/query/subscription workflows, with a precise supported SQL/protocol subset and driver tests. Wire compatibility does not imply full PostgreSQL SQL or transaction compatibility.
8. Add external vector retrieval using A10: typed bounded requests, cancellation, safe filters, dimension/metric validation, ranked multirow results, and reproducible quality/latency benchmarks. Local vector math remains the preferred path when it already meets the workload.

**Gate:** connector/codec conformance tests, schema evolution and malformed-input fixtures, real-service outage/restart tests, compatibility-labelled migrations, and published operational limits. Backend selection remains evidence-driven; duration alone does not exclude a connector.

### P16 — Decision tables, policy evaluation, and forward chaining

**Build:** make general-purpose decision workloads a first-class extension of the platform, with a separate synchronous evaluation contract. Drools/DMN and OPA/CEL provide the reference categories.

1. Add a typed decision-table frontend with explicit hit policies such as unique, first, priority, and collect; validate overlaps, unreachable rows, missing outcomes, and output types. Provide an explanation trace identifying matched rules and precedence.
2. Version decision models and input schemas in P11. Compile deterministic, bounded plans once; expose batch and request/response evaluation without a permanently running stream or mandatory durable queue.
3. Add DMN/FEEL compatibility in declared levels with official conformance fixtures. Reuse a maintained implementation through a suitable adapter where that preserves semantics better than a partial reimplementation. Unsupported constructs are validation errors.
4. Integrate CEL or policy engines as versioned frontend/extensions where justified, preserving their type/null/error rules. Avoid translating another language into approximately similar SQL. Policy bundles share artifact governance and test infrastructure.
5. Plan a separate forward-chaining execution mode: working-memory facts, assertions/retractions, agenda/conflict resolution, salience, truth maintenance, and explanation provenance. Prototype RETE-style versus alternative incremental matching on representative rules before choosing the algorithm.
6. Bound agenda cycles, recursion, fact growth, evaluation time, and side effects. Stage effects through the common ledger when durable behavior is requested; pure evaluation remains available and cheap.
7. Add temporal decisions that compose with P08 through explicit time/state interfaces. Keep a distinction between one-shot policy evaluation and an indefinitely running temporal rule.

**Gate:** decision-table/DMN conformance, deterministic explanations, fact-update/retraction correctness, cycle/resource-limit tests, and differential fixtures against the selected reference versions. Ship supported levels progressively; the full target stays on the roadmap even if it is a year-scale effort.

### P17 — Compatibility, qualification, and an open ecosystem

**Build:** an ongoing release discipline, not a final documentation sprint.

1. Publish a semantic/versioning policy for SQL, APIs, extension ABI, state formats, configuration, and catalog migrations. Label experimental, supported, and deprecated features consistently.
2. Maintain reference workloads for industrial control, telemetry export, streaming SQL, CDC enrichment, decision services, and disconnected-edge recovery. Include expected outcomes and failure scenarios.
3. Extend existing integration and qualification harnesses with sanitised real-service fixtures, fuzzing/property tests for parser/value/queue/state boundaries, fault injection, and architecture/platform coverage.
4. Publish reproducible benchmark configurations and raw evidence with guarantees matched. Keep microbenchmarks separate from end-to-end measurements; validate independent sink observations and drain time.
5. Provide migration guides, operator runbooks, examples, extension templates, and a public compatibility matrix. Generate capability documentation from P01/P12 so it cannot casually drift from runtime validation.
6. Establish maintainers and release ownership for core versus extensions, compatibility review, security reporting, dependency/license review, and reproducible packaging. Encourage outside extensions without forcing every integration into the core binary.

**Gate:** every supported feature has a contract, docs, qualification owner, upgrade story, and representative test. A clean fresh deployment and a restored older deployment must both be exercised before a supported release.

## 4. Delivery sequence: dependencies, not a short-term feature ceiling

These are capability milestones, not calendar promises. Staffing and prototype evidence determine dates. Multiple tracks can proceed independently, but a release cannot bypass its semantic/recovery prerequisites. Year-scale work remains explicitly in the destination.

| Milestone | Implementation scope | Exit condition |
|---|---|---|
| M0 — Truthful foundation | P01/P17; A1/A2/A7 correctness; rate-limit and DLQ resource fixes; production fixture runner groundwork | Accepted options are real or rejected; supported semantics and known gaps are testable; baseline performance/recovery evidence recorded. |
| M1 — Reliable standalone | P02; A4–A6; P03 stages 1–4; P06 fixtures/diagnostics; P11 stable catalog/action identity groundwork | Qualified single-source recovery and durable effects, bounded outages, correct ownership, and honest delivery status. |
| M2 — Temporal and telemetry depth | P04–P08; P03 multi-input/savepoints; richer schemas | Delayed/temporal rules recover correctly; late-data modes are explicit; qualified telemetry transport and schema evolution. LEAD can ship earlier in non-durable mode with that limitation stated. |
| M3 — Programmable platform | P09–P13, P15–P16 incrementally; P10 optimization throughout | Maintained relations, safe generation upgrades, usable embedding/extension interfaces, governed decision services, and published conformance levels. |
| M4 — Resilient scale | P14 after local durability, fencing, and catalog prerequisites | Demonstrated active/passive recovery, then partitioned execution and rescaling with tested ownership/state/effect correctness. |
| M5 — Ecosystem maturity | P17 continuously; broaden certified extensions/connectors, migration tools, language levels, operational automation | External contributors can build reliable extensions; upgrades and recovery remain routine as the capability set grows. |

Critical dependency chains:

- P01 contracts → P02 resource/control boundaries + A6 identity → P03 durable state/effects → P11 recoverable activation → P14 failover/distribution.
- P06 deterministic execution fixtures → P04 delayed analytics + P05 time semantics → P08 richer temporal rules.
- P03 state recovery + P05 result modes + P11 versioning → P09 CDC/maintained relations.
- P01 typed/capability contracts + P11 artifact identity → P12 stable SDK → broader P15 adapters and P16 language integrations.
- P13 isolation must be qualified before offering shared untrusted multitenant hosting.
- P10 optimizations are incremental and may ship independently when correctness/resource gates pass.

First implementation PRs should be bounded and reviewable: reproduce/fix REST reuse; anchored image writes; object/array corrections; ignored persistence-error handling; rate-limit option honesty; bounded external DLQ dispatch; shared metadata/codec contracts; named action identity; and a production-executor fixture harness. Then split durability into capability declarations, transactional store qualification, ordered controls, one-source checkpointing, individual operator state adapters, outbox, and broker lifecycle. Do not put all recovery work into one unreviewable PR.

## 5. Acceptance, measurements, and adoption decisions

Every implementation PR records the contract, current failing example or missing capability, relevant pinned reference, code boundaries, compatibility impact, and validation. For speculative performance work, record the hypothesis and keep the old path until measurements justify replacement.

Use the repository's current CI and formatting checks, targeted core/SQL/connector/server tests, existing sink/schema/ingress/lifecycle fixtures, and qualification harness. A documentation-only roadmap change does not require running the runtime suite.

Before implementation, record the chosen workload's event rates/sizes, key cardinality, memory/disk budget, latency target, recovery objectives, and supported failure model. Establish numerical regression budgets from the baseline rather than inventing universal throughput goals. Test steady state, overload, outage, replay, upgrade, and recovery under the same semantic contract.

| Dimension | Required evidence |
|---|---|
| Correctness | Deterministic expected outputs, property/differential fixtures where appropriate, no silent semantic downgrade. |
| Delivery | Independent input/output identities; accepted, terminal, pending, failed, duplicate, and deliberately dropped counts reconcile under failure. |
| Resources | Bounded memory, state, disk, pending tasks, retries, and metric cardinality under an extended outage. |
| Recovery | Qualified crash points, state equivalence, source-position correctness, effect ownership, and measured RPO/RTO for the chosen profile. |
| Performance | Comparable source-to-observed-sink work; sustainable rates and tail latency; raw reproducible evidence and confidence/variance. |
| Compatibility | Prior configs/APIs/state fixtures, declared migrations, upgrade/rollback tests, and explicit unsupported cases. |
| Operability | Explainable capability limits, useful error/status, bounded diagnostics, and a tested recovery runbook. |

“Keep” is a valid outcome: preserve Rust deep value comparison, immutable/shared record handling, bounded blocking defaults, existing Parquet, local vector functions, ASOF indexing, already implemented SQL accumulators, and the current native fast path unless a concrete test demonstrates an improvement. A stronger delivery guarantee may justify additional cost in its opt-in profile; compare equivalent contracts.

Reject a proposed implementation if it loses semantics, hides failure, breaks recovery ownership, or adds avoidable hot-path cost without workload benefit. Redesign it rather than remove the long-term capability simply because the first approach failed.

## 6. Traceability for the original findings

| Previous finding | Detailed specification | Broader destination |
|---|---|---|
| REST response consumption / connection reuse | A1 | P02 retry/effect semantics |
| Image path containment | A2 | P13 filesystem isolation |
| Advertised dropOldest / buffer policy | A3 | P02 resource budgets; P03 reliable-mode validation |
| Kafka broker metadata | A4 | P15 envelopes; P03 real groups and recovery |
| Shared JSON Lines format | A5 | P15 codecs and schema evolution |
| Named actions / cache ownership | A6 | P11 catalog identities; P03 outbox |
| Nested object_pick / array set semantics | A7 | P01 conformance |
| Already implemented release functions, IoTDB, Parquet | A7 | Keep; qualify compatibility rather than rebuild |
| Streaming LEAD / WHEN / UNTIL | A8 | P04 flagship delayed execution; P05 time |
| Coordinated checkpoint recovery | A9 | P03 flagship durable execution; P14 HA |
| External vector lookup / multirow results | A10 | P15 retrieval extensions; P02 bounded enrichment |
| Hash joins | A11 | P10 optimizer; P09 maintained relations |
| Lookup cancellation observation | P02 and A10 | Verify existing deadlines first; add only demonstrated gaps. Upstream [#4221](https://github.com/lf-edge/ekuiper/pull/4221) was a 2.4-branch change. |
| TDengine encoding observation | P15 qualification | Preserve existing escaping/identifier handling; reproduce any remaining encoding mismatch before changing it. [#4245](https://github.com/lf-edge/ekuiper/pull/4245) |
| Atomic SQL row-kind batches | P09 engine-level integration | Validate active sink call paths and transaction scope; a library method alone is insufficient. [#4188](https://github.com/lf-edge/ekuiper/pull/4188) |

## Appendix A — Detailed compatibility and implementation specifications

These specifications retain the concrete algorithms, migration safeguards, and acceptance cases from the earlier eKuiper review. They are implementation modules within the broader roadmap. P03 adds stronger delivery/storage contracts to A9; P15 adds richer protocol/schema fidelity to A4/A5. Conditional choices below are evidence gates, not exclusions based on implementation duration.

### A1. REST sink connection reuse

**Adopt if the connection test reproduces the gap.** `engine/sinks.rs::execute_rest_request` reads response status and returns. The separate HTTP connector in `rekuiper-connectors/src/http.rs` also has a status-only send path. Trace SQL, graph, batch and cache replay to identify the active callers. [Upstream fix #4212](https://github.com/lf-edge/ekuiper/pull/4212) · [Connection exhaustion report #4211](https://github.com/lf-edge/ekuiper/issues/4211)

1. Add an HTTP/1.1 integration fixture that counts accepted TCP connections across repeated requests through the production sink. Use a nonempty response body large enough that it is not already consumed with the headers. Exercise success, permanent `4xx`, and retryable `429`/`5xx` separately.
2. Drain ignored response bodies in chunks under a byte and time budget. Use the existing request deadline where available. Stop reading at the budget, allowing that connection to close. Share the behavior between active status-only send paths if they have the same contract.
3. Preserve the response status classification, private-network policy, redirects, headers, and retry/cache behavior. A successful delivery stays successful if its ignored body stalls or truncates after the status arrives. Surface drain failure as a diagnostic without retrying an already accepted write.

**Acceptance.** A series of ordinary nonempty responses reuses a connection. Slow, oversized and truncated bodies return within the configured bound. Sink results and retry counters match their existing status contract. Extend `test_issue18_sinks.rs` or the closest existing REST fixture and run private-network tests.

### A2. Image sink write containment

**Adopt directory-anchored I/O. Keep rekuiper's existing strict filename validation.** `rekuiper-connectors/src/image_sink.rs` rejects hostile filename components and then calls `create_dir_all` and `write` using resolved paths. Its `path` and `fileName` can also be rendered from row data. [Upstream fix #4246](https://github.com/lf-edge/ekuiper/pull/4246)

1. Separate the administrator-trusted directory root from row-rendered components. Preserve configured absolute directories and the existing static filename contract. For directory templates, use a fixed configured root or a safe literal prefix. Fail validation for an unanchored template. Document how existing templates can supply a trusted root.
2. Evaluate a safe Rust directory-relative API on supported platforms. Anchor subdirectory creation, file open and cleanup to the directory handle. Block intermediate and final symlink escapes, including Windows junctions and reparse points. A canonicalize-then-write check leaves a race and is insufficient. Fail closed if containment cannot be enforced on a supported code path.
3. Where replacing existing images is required, write a new file in the same directory and atomically replace the directory entry using anchored operations. Clean up the temporary file after write failure. Scope retention enumeration and deletes to the same directory and existing image naming rules.

**Acceptance.** Normal writes, configured replacement and retention still work. Test final and intermediate links, directory-template escapes, a link swapped concurrently with a write, a failed write, and Windows-specific paths. Assert that the outside target is untouched and unrelated files survive retention. Exercise Linux and Windows in CI.

### A3. Stream buffer policy

**Keep current backpressure for omitted policy and for explicit `block`.** eKuiper's legacy defaults can discard records. rekuiper already uses bounded channels and blocking publication for normal ingress. Current DDL and join validation accept explicit `dropOldest`, but the engine does not appear to apply it. Reproduce the difference under saturation. If the project chooses to withdraw `dropOldest`, reject it explicitly and update the docs so the accepted option cannot silently block. [Upstream stream policy #4133](https://github.com/lf-edge/ekuiper/pull/4133)

1. Resolve the policy from stream definitions once, using the same parser in stream mutation, rule validation, import and startup. Keep the existing mixed-policy check. Treat sink-cache overflow as its own policy. Audit existing memory feedback paths, which use nonblocking admission to avoid cycles.
2. Keep the Tokio `mpsc` reserve/send implementation and default public `StreamBus` methods for blocking callers. For explicitly lossy subscriptions, add an internal bounded per-subscriber FIFO that supports atomic eviction and append. Route only records selected by that subscriber's filter. Make the receiver wakeup and close protocol race-safe and avoid waiting with the topic registry lock held.
3. Wire all relevant publishing paths in `rekuiper-core/src/runtime.rs`: `publish_async`, `publish_batch`, and the nonblocking APIs used by feedback. Preserve existing admission semantics for blocking and feedback paths. In lossy mode, each full queue replaces its oldest pending data record and reports the eviction. Keep the newest capacity-sized suffix for an oversized batch. Return published, closed, and evicted outcomes distinctly where callers need them. Keep checkpoint barriers out of lossy queues.
4. Tie waits to subscriber lifetime and producer cancellation. Report per-stream drops and queue depth with bounded labels. Document when `dropOldest` is suitable, such as latest-value dashboards, and reject it for rule plans that request replay-based delivery guarantees.

**Acceptance.** A capacity-two queue fed `[1,2,3]` yields `[2,3]` and one eviction in lossy mode. The same blocking queue backpressures the producer and yields all three in order. Cover concurrent publishers, fast and slow subscribers, batch overflow, subscriber removal while blocked, filter routing, feedback and HTTP ingestion. Run `perf_lossless` and `perf_throughput` against the baseline on the same host. Preserve the single-subscriber fast path and default-mode allocation profile.

### A4. Kafka source metadata

**Adopt additional metadata using the existing `Arc<RecordMeta>`.** Keep the current payload shape, topic/source fields, and `offsetReset` behavior. The source actor in `rekuiper-connectors/src/kafka.rs` has broker offsets and record key/headers before `decode_kafka_payload` reduces the input to bytes. [Upstream Kafka metadata #4214](https://github.com/lf-edge/ekuiper/pull/4214)

1. Construct metadata at the fetched broker record boundary. Set the configured partition, full-width offset, broker timestamp, optional key, and headers. Preserve upstream-compatible text fields for valid UTF-8 and use a documented reversible representation for binary values. Keep the compatibility header object with its documented last-value behavior, and expose ordered raw headers when the source client preserves duplicates. Verify client limitations; never reconstruct duplicate information already discarded by a client. Full-fidelity mode requires a client that preserves the protocol data.
2. Pass one shared metadata reference to every row decoded from the broker record, including array payloads and future JSONL batches. Keep those fields out of payload JSON and sink serialization unless a rule explicitly projects them. Verify that source normalization, graph operators and the evaluator preserve metadata, including `meta(\`partition\`)` and `meta(headers)->traceId`.
3. Use offsets internally as integers. Keep broker tombstones and malformed-payload handling compatible with current behavior. Add metadata construction tests and a Kafka or Redpanda rule test with known broker offsets, missing headers and a multirecord payload. Compare throughput and allocations to topic-only metadata. Share immutable topic/source allocations where profiling shows they matter.

**Acceptance.** A rule can route on a header and emit the exact broker offset and partition. All rows from one broker record report the same metadata, and existing projections emit the same data as before.

### A5. JSON Lines across connectors

**Adopt the shared payload format. Keep current single-JSON and file semantics.** The existing file support does not give `FORMAT="jsonl"` consistent behavior across network payloads. [Upstream JSON Lines format #4218](https://github.com/lf-edge/ekuiper/pull/4218)

1. Add a streaming object decoder and framed writer in `rekuiper-connectors/src/codec.rs`. Use the existing JSON library's stream deserializer. Define strict JSON Lines framing for LF/CRLF-separated UTF-8 objects and a separately explicit concatenated-JSON compatibility mode for whitespace-separated or adjacent objects. Do not silently call both wire contracts strict JSON Lines. Empty payloads yield zero rows. Include the record index in errors for malformed input and non-objects. Apply existing byte/row limits before queuing records, and reject a malformed payload before publishing a valid prefix.
2. Register `jsonl` in stream and sink format validation and `PayloadFormat` where applicable. Start with HTTP push/ingest and REST output. Then integrate MQTT, Kafka, HTTP pull, and other connectors with configurable payload formats. Pass each decoded object through the same schema coercion, metadata, projection and DLQ path as ordinary JSON. Keep wire acknowledgement at the connector-message level. Do not repeat broker acknowledgement for every decoded row.
3. Use the shared writer for supported textual sinks and existing `batchSize`, `sendSingle` and `dataTemplate` paths. A network batch uses LF separators and no trailing separator. A file append adds a separator between successive batches. Render raw templates per row under the existing template contract and test that framing does not double-encode output. Specify the HTTP media type for `jsonl` and preserve explicit user headers.

**Acceptance.** An HTTP request containing two objects yields two typed rows, while a malformed second object triggers the defined error path with no earlier row published from that request. A batch round-trips through the new codec. Cover empty bodies, CRLF, explicit concatenated-object compatibility, strict-mode rejection of invalid framing, object versus array rejection, file append, templates, MQTT/Kafka metadata and unchanged `json` behavior. Extend schema, ingress and output shape integration tests.

### A6. Named sink actions

**Adopt the optional name and stable identity for explicitly named actions. Keep existing unnamed metrics, cache identities and graph node IDs.** The SQL runtime currently opens caches by prepared action index. [Upstream named actions #4234](https://github.com/lf-edge/ekuiper/pull/4234)

1. Parse optional `name` inside the connector object, as in `{"rest":{"name":"archive",...}}`. Resolve enabled action identity once during validation and reuse it in `prepare_actions`, status/explain and startup. Validate length and identifier grammar. Reject collisions with enabled generated names and relevant operator/resend IDs. Keep connector type separate from the name for dispatch and type-specific metrics.
2. Give named actions per-action counters and labels. Integrate with logs, Prometheus, explain output and Studio. Keep legacy aggregate series and unnamed identifiers. Confirm that disabled actions and graph rules retain their existing behavior.
3. Version the named cache key and store an ownership manifest tied to rule, action identity, connector type and destination configuration. Hash or redact secrets in that metadata. Preserve existing numeric paths for unnamed actions. Adopt a legacy cache only where ownership can be verified and the migration can be journaled or performed atomically while the rule is stopped. Existing caches without provenance need an explicit drain or operator-approved mapping. Keep them available for inspection. Reordering named actions should not change their ownership. Treat a rename or incompatible destination update as a new identity unless an explicit migration maps it.

**Acceptance.** Two REST sinks expose separate names and stats. Reordering them during an outage replays each cache only to its original destination. Cover duplicate and invalid names, rename with a nonempty cache, restart, disabled actions, default naming and existing unnamed dashboards.

### A7. SQL function parity and features already present

**Adopt nested `object_pick` paths. Keep Rust's safe array comparison and immutable evaluation.** `rekuiper-sql/src/eval/array.rs::func_object_pick` currently does a top-level `get`. [Nested object report #4237](https://github.com/lf-edge/ekuiper/issues/4237) · [Fix #4238](https://github.com/lf-edge/ekuiper/pull/4238)

1. Use a path walk and merge into a new result object. Preserve both string-list and array argument forms. Resolve an exact literal key first to preserve existing rules that use keys containing dots. Fall back to nested traversal when the exact key is absent. Keep sibling selections at the correct depth. Test selecting a parent and child in either order, duplicate and absent paths, deep siblings, mixed scalar/object branches and input immutability.
2. Add differential fixtures for `array_union`, `array_intersect` and `array_except` covering nested arrays, objects, numeric equality, duplicates and order. Keep the current deep comparisons. Fix `array_except([1,1,2],[2])` to return `[1]`. Its implementation preserves both `1` values even though rekuiper documents distinct results. Deduplicate survivors in first-seen order using the existing equality helper and note the correction in the changelog. Keep existing union/intersection ordering unless a compatibility fixture shows a reason to change it. Measure before adopting a scalar hashing optimization. [rekuiper array function contract](https://github.com/Indus-Stack/rekuiper/blob/24848c8f88363bfe0004cc88c7db73302657aa0c/docs/en_US/sqls/functions/array_functions.md#array_except) · [Upstream set fix #4240](https://github.com/lf-edge/ekuiper/pull/4240)

**Keep already implemented release features.** Use pinned fixtures to check `array_avg`, `acc_max_by`/`acc_min_by` tie behavior, `acc_map_agg` ordering and updates, `array_positions`, `acc_distinct_collect`, DDL defaults, disabled actions, `offsetReset`, IoTDB and Parquet. Keep rekuiper's bounded accumulator behavior and explicit Kafka offset selection. The current partition reader does not acquire consumer-group behavior from an upstream default change. Compare Parquet configuration/schema examples when upstream's open PR stabilizes, and add an import alias only if a migration needs one. [2.4.2 release](https://github.com/lf-edge/ekuiper/releases/tag/v2.4.2) · [Open Parquet PR #4243](https://github.com/lf-edge/ekuiper/pull/4243)

### A8. Streaming `LEAD` with `WHEN`, `UNTIL`, and `current_row()`

**Proposed architecture decision.** Use an ordered operator in the SQL streaming rule task. The current scalar fallback in `eval/analytic.rs` returns a default where it has no future row. A window or scalar evaluator alone cannot implement waiting across future input. A separate actor would also add queueing and a snapshot boundary. Keep existing evaluator paths for plans without delayed expressions. [Upstream LEAD #4122](https://github.com/lf-edge/ekuiper/pull/4122)

#### SQL contract and planning

Use a differential fixture based on this syntax. Partition and order refer to the stream presented to the analytic operator.

```sql
SELECT device_id, ts, reading,
       lead(reading, 1, -1) OVER (
           PARTITION BY device_id
           WHEN recovered = true
           UNTIL ts - current_row(ts) > 5000
       ) AS recovery_reading
FROM device_events;
```

1. Extend the existing `OVER` AST and parser in `rekuiper-sql/src/{ast,parser}.rs` with an optional termination predicate. Handle `UNTIL` in that clause without changing unrelated identifier parsing. In `validator.rs`, require `WHEN` with `UNTIL`, restrict `UNTIL` to `LEAD`, and allow one-argument `current_row` only inside that predicate. Validate one to four LEAD arguments, a positive integer literal offset, and a boolean `ignoreNull`. Defaults are offset `1`, result `NULL`, and `ignoreNull = true`.
2. Resolve ordinary references in `UNTIL` against the incoming probe row and `current_row(expr)` against the buffered origin. Give each planned call a stable ID and resolve alias dependencies before runtime. Support arguments that use eager analytic aliases and expressions that consume an already resolved LEAD alias. Reject cycles and nested delayed expressions with a clear diagnostic. Use fixtures to pin the order of `WHERE`, eager analytics, delayed analytics and final projection, including which rows can act as probes. Disable predicate pushdown for a delayed rule where it would remove eligible probes.
3. Start with single-stream SQL rules. Extend to other plan shapes as their semantics and control flow are verified. Validate window collections and graph rules explicitly rather than silently executing a scalar fallback for delayed syntax. Inventory existing bare `lead()` uses before changing their results. If required, gate the behavior change behind an explicit compatibility option during migration. Expose the planned operator and memory limits through explain.

#### Execution and limits

1. Implement the operator in the server SQL runtime with SQL expressions evaluated through existing evaluator helpers. Maintain one FIFO of origin records and per-call, per-partition indexes of unresolved requests. An incoming row probes earlier requests in its own partition before registering its own request. Evaluate `UNTIL` first. A true result completes that request with its origin's default. A qualifying `WHEN` and value decrement the remaining offset. Respect null skipping. Release a record only when all its calls resolve and it reaches the FIFO head. Retain origin fields, metadata and timestamps until projection, using existing shared record data where possible.
2. Evaluate shared probe values once per input and call where dependencies allow. Stage per-input updates until validation succeeds so a failed expression cannot partially change the pending queue. Use existing error reporting and DLQ policy. Bound pending rows and bytes across the whole rule, including completed rows held behind an unresolved origin. At the limit, fail the rule with an actionable error and a metric. Blocking intake at the limit could prevent the next qualifying row from ever arriving. Add a configured cap for offsets and use an indexed or direct queue fast path for unconditional lookahead only after correctness is established.
3. Add an explicit finite-source EOF control through the ordered rule input. Flush pending rows with their defaults before propagating EOF. A temporarily idle input or an administrative stop has different semantics. Idle partitions wait for later probes. A restart restores unresolved rows from a checkpoint where configured. Delay any downstream watermark that would overtake buffered output when support expands to plans with event-time operators.
4. Define a versioned state payload with the origin FIFO, sequence counter, call IDs, captured defaults, remaining offsets, resolved values and metadata needed for projection. Restore and validate it against the same plan before intake resumes. Wire it into the checkpoint coordinator below. Keep durable lookahead disabled for sources and sinks that cannot meet its replay contract.

**Acceptance.** Compare output to eKuiper for positive offsets, null modes, partition isolation, a probe satisfying both `UNTIL` and `WHEN`, multiple calls and alias dependencies, and EOF flush. Verify order across partitions, cap behavior with no matching future row, SQL diagnostics, unchanged behavior for rules without delayed expressions, and restart from a checkpoint with both pending and resolved-but-held rows. Benchmark conditional lookahead by pending rows per partition and memory retained per origin. The offset counts matches and does not bound how long a request waits.

### A9. Checkpoint recovery

**Proposed architecture decision.** Use coordinated barriers and immutable, versioned snapshots for replayable rules. Preserve the current lightweight analytic-state persistence for legacy rules while migrating. The detached ticker in `engine/runtime.rs` serializes the analytic state map independently of source progress, window state and sink output. A timer should request a checkpoint from the rule task, which owns the point at which it captures state. [Upstream checkpoint isolation #4195](https://github.com/lf-edge/ekuiper/pull/4195) · [Deferred snapshot work #4194](https://github.com/lf-edge/ekuiper/pull/4194)

#### Storage, lifecycle, and state contracts

1. Add a coordinated checkpoint store beside `RuleManager`'s legacy `rule_checkpoints` methods. Use a separate versioned namespace or table with an atomic committed pointer and at least one previous generation. The envelope contains format version, plan fingerprint, rule generation, checkpoint ID, source positions, versioned operator state, and output acknowledgement boundaries. Validate version, checksum, fingerprint and required participants on restore. An unsupported or corrupt snapshot surfaces in rule status. Only fall back to a verified previous generation under an explicit recovery policy.
2. Separate legacy analytic-map import from replayable recovery. A legacy checkpoint can seed analytic state in a documented migration mode. Since it has no source position, the operator must choose a starting offset before enabling replay guarantees. Older binaries can continue to read their legacy namespace. Document upgrade, rollback, and recovery from retained offsets. Validate migration against a stored fixture from the current format.
3. Have one coordinator per rule generation and one active checkpoint request. Serialize checkpoint commit and discard with stop/update/delete. A durable generation check prevents a stale writer from publishing after the rule changes, even if an asynchronous save is already in progress. Surface failure and timeout, retain the last valid checkpoint, and tie any force-save acknowledgement to its exact checkpoint ID. Restore all participating state before permitting new input.

#### Barrier and delivery protocol

1. Start with an opt-in single-source SQL path using a replayable source, such as one Kafka partition, and an acknowledged sink. Use an internal ordered control path for data, EOF and barrier markers. Preserve the existing record channel and public publish methods on the default path. The source emits its marker after prior data in the same ordering domain. Store the next offset only after all decoded rows from a broker message have been accepted. A malformed message must reach its documented terminal disposition before the cursor can advance. Resume from the saved offset and report retention gaps clearly.
2. The rule task processes all input preceding the marker, then captures owned operator state at that boundary. An asynchronous worker can encode and store an immutable snapshot. Keep queue control and capture deterministic. For fast paths use copy-on-write or immutable segments where benchmarks justify them, with no full-state clone per row. Do not advance checkpoint offsets for data discarded by lossy buffer policies.
3. Send a corresponding barrier through the sink queue. Flush prior batches and require durable acknowledgements from every participating action before committing the checkpoint. Define the acknowledgement per connector and initially enable the contract only for sinks that can fulfill it. A failed sink, timeout, or process crash before commit leaves the previous checkpoint in effect. Replay can deliver duplicate outputs when the sink acknowledged after the previous durable checkpoint. A coordinated durable outbox may later allow checkpoints to progress during outages. Integrate it with sink-cache ownership and ordering before promising this behavior with `enableCache`.
4. Move window state out of loop-local variables into explicit versioned operator payloads in `engine/windows.rs` and `eval/window.rs`. Add the implementations incrementally for count, tumbling, hopping, sliding, session and state windows, then join buffers, CEP state and delayed LEAD. Save watermarks, timers, pending triggers, rows and accumulator state as applicable. Encode logical window boundaries using stable timestamps. Rebuild monotonic timers on restore from a documented downtime policy, and test missed deadlines and late events. Implement dirty-state tracking only after measuring capture cost and verifying that restored state is complete.
5. Extend the barrier protocol to multiple input edges with alignment, bounded buffering and abort that unblocks all paused edges. A graph operator must forward barriers in order along each output, and sink completion must cover every reachable action. Shared sources need independently restorable cursors or a durable replay layer so one rule cannot rewind another rule's live reader. Gate unsupported graph shapes, shared-reader restart and non-replayable sources by capability. Keep guarantees explicit per source, operator and sink combination. External effects can be deduplicated by a sink-supported idempotency key or transactional protocol as a separate capability.

**Acceptance.** Extend `qualification_harness.rs` with a crash/restart test on a real replayable source and a sink that records acknowledgements. Inject failures before source marker, during snapshot encoding, before sink ack, after sink ack but before commit, during atomic commit and after commit. Verify no missing records within the supported at-least-once contract, expected duplicate bounds based on the last checkpoint, and consistent state after resume. Add save/stop/delete races, wrong-plan and corrupt snapshots, cross-version restore, in-flight window and LEAD recovery, and watermark/timer continuity. Measure checkpoint pause, write latency, restore time, memory, and ingest throughput against the same workload without checkpoints.

### A10. Milvus and pgvector lookup joins

**Conditional adoption.** Keep existing in-process vector math. External similarity search solves a different problem when the indexed history cannot live inside rekuiper. Require a workload with concrete data size, top-K, accuracy and latency targets. Validate pgvector first if the deployment already uses Postgres. Add Milvus when a user workload warrants that connector. [Feature issue #4106](https://github.com/lf-edge/ekuiper/issues/4106) · [Open PR #4242](https://github.com/lf-edge/ekuiper/pull/4242)

1. Prototype a typed lookup request with vector, dimension, metric, top-K, deadline and scalar filters. Reuse existing value validation, pools, secrets and request policy where applicable. Resolve the vector equality form only for an explicitly configured vector table and field. Keep ordinary equality lookups on their existing path.
2. Bind vector and filter values in pgvector queries and validate or safely quote identifiers. For Milvus, validate endpoints and use existing TLS/egress policy plus safe filter encoding. Reject invalid dimensions, nonfinite elements, unsupported metrics and excessive top-K. Bound concurrent remote calls, apply cancellation and define timeout/DLQ behavior. Preserve the backend distance and specify any normalized score by metric.
3. The current `engine/joins.rs` lookup paths select a first match. Add ranked multi-row expansion for vector lookups in the streaming and applicable window paths. Keep ordinary lookup cardinality. Test match order, `LEFT LOOKUP` no-match output, ties, mixed filters, authentication errors, stalled queries and cancellation. Compare a real end-to-end workload against local vector matching before deciding to ship either connector.

### A11. Hash joins

**Benchmark first. Keep existing lookup and indexed `ASOF` paths.** The ordinary window join has nested scans in `engine/joins.rs::eval_window_join_batch`. The upstream issue proposes a hash join for large windows. [Open proposal #4247](https://github.com/lf-edge/ekuiper/issues/4247)

1. Benchmark the current implementation on small and large windows with unique keys, duplicates, skew, mixed numeric keys, nulls, and non-equality predicates. Measure latency, throughput, peak memory and output cardinality. Use the current output as the differential reference for supported semantics.
2. If measurements show a win, compile simple equi-join keys and build a bounded per-window index from normalized keys to ordered row indices. Key equality must follow rekuiper's existing integer/float and null semantics. Keep residual predicate evaluation and exact equality checks. Preserve match multiplicity, left and right order where observable, and unmatched-row behavior for outer joins. Use the current join for unhashable keys, non-equijoins, small inputs or exhausted memory budgets.
3. Add result-equivalence tests against the nested loop and a benchmark that selects the crossover threshold. Expose the chosen strategy in explain. Keep the existing path whenever the hash index fails the correctness, memory or throughput gate. Only consider a user-facing hint if workload evidence shows that automatic selection needs an override.

[rk-model]: https://github.com/Indus-Stack/rekuiper/blob/24848c8f88363bfe0004cc88c7db73302657aa0c/crates/rekuiper-core/src/model.rs
[rk-bus]: https://github.com/Indus-Stack/rekuiper/blob/24848c8f88363bfe0004cc88c7db73302657aa0c/crates/rekuiper-core/src/runtime.rs
[rk-windows]: https://github.com/Indus-Stack/rekuiper/blob/24848c8f88363bfe0004cc88c7db73302657aa0c/crates/rekuiper-server/src/engine/windows.rs
[rk-kafka]: https://github.com/Indus-Stack/rekuiper/blob/24848c8f88363bfe0004cc88c7db73302657aa0c/crates/rekuiper-connectors/src/kafka.rs
[rk-rabbit]: https://github.com/Indus-Stack/rekuiper/blob/24848c8f88363bfe0004cc88c7db73302657aa0c/crates/rekuiper-connectors/src/rabbitmq.rs
[rk-runtime]: https://github.com/Indus-Stack/rekuiper/blob/24848c8f88363bfe0004cc88c7db73302657aa0c/crates/rekuiper-server/src/engine/runtime.rs
[rk-cache]: https://github.com/Indus-Stack/rekuiper/blob/24848c8f88363bfe0004cc88c7db73302657aa0c/crates/rekuiper-server/src/sink_cache.rs
[rk-kv]: https://github.com/Indus-Stack/rekuiper/blob/24848c8f88363bfe0004cc88c7db73302657aa0c/crates/rekuiper-core/src/kv.rs
[rk-sinks]: https://github.com/Indus-Stack/rekuiper/blob/24848c8f88363bfe0004cc88c7db73302657aa0c/crates/rekuiper-server/src/engine/sinks.rs
[rk-operators]: https://github.com/Indus-Stack/rekuiper/blob/24848c8f88363bfe0004cc88c7db73302657aa0c/crates/rekuiper-server/src/engine/operators.rs
[rk-ruletest]: https://github.com/Indus-Stack/rekuiper/blob/24848c8f88363bfe0004cc88c7db73302657aa0c/crates/rekuiper-server/src/handlers/ruletest.rs
[rk-dlq]: https://github.com/Indus-Stack/rekuiper/blob/24848c8f88363bfe0004cc88c7db73302657aa0c/crates/rekuiper-server/src/engine/dlq.rs
[rk-plugins]: https://github.com/Indus-Stack/rekuiper/blob/24848c8f88363bfe0004cc88c7db73302657aa0c/crates/rekuiper-server/src/handlers/plugins.rs
[rk-otlp]: https://github.com/Indus-Stack/rekuiper/blob/24848c8f88363bfe0004cc88c7db73302657aa0c/crates/rekuiper-connectors/src/otlp.rs
[rk-declarative]: https://github.com/Indus-Stack/rekuiper/blob/24848c8f88363bfe0004cc88c7db73302657aa0c/crates/rekuiper-server/src/declarative.rs


## Appendix B — Executable, dependency-aware ticket backlog

**Baseline checked again:** 12 November 2026, remote main at `24848c8f88363bfe0004cc88c7db73302657aa0c`. These are proposed tickets, not GitHub issues already created or completed implementation. Every P01–P17 track and A1–A11 specification has ticket coverage below.

### B1. How a smaller model executes one ticket

Give the worker **one ticket**, its referenced roadmap section, this execution protocol, and the contract definitions below. Do not give it an entire flagship feature as a coding assignment.

1. Read repository instructions, the named files on current main, and the ticket's dependencies. Record the actual starting SHA. If the behavior is already correct, prove it with the acceptance case and close as **KEEP** instead of rewriting it.
2. Implement only the named behavior. Preserve existing APIs/defaults unless the ticket explicitly changes them. New paths and interface names below are **proposed**, not assertions that those files exist.
3. Put new modules and fixtures in the ticket-owned paths. A component ticket may test its module through a test-local path import before production registration; use the real shared contract types. Do not create a second production contract or a runtime stub that returns success.
4. Use fakes only at the declared external boundary. The fake must implement the same contract and run the same conformance cases as the real adapter. Mock success never qualifies a connector, delivery guarantee, or recovery path for release.
5. Run the ticket's named test target and its acceptance cases. Confirm tests actually ran; zero selected tests is failure. Record exact commands and results. Do not broaden a PR to repair unrelated failures; report baseline versus introduced failures separately.
6. Leave an implementation handoff under `docs/implementation/tickets/<ID>.md`: baseline SHA, changed files, exported interfaces, fixture paths, acceptance results, compatibility/migration notes, limitations, and exact follow-up IDs. Keep credentials and real payloads out.
7. A ticket is **DONE** only when its deliverable and acceptance pass. **COMPONENT DONE** is allowed when its isolated implementation passes but production wiring belongs to another ticket. **INTEGRATED** requires that wiring ticket. **QUALIFIED** requires the real-service/failure/upgrade tests named by the release gate.
8. If an assumption fails, record the smallest contract question and mark only the affected ticket **BLOCKED**; other ready tickets continue. Do not invent semantics, silently skip a test, advertise an unimplemented option, or mark an epic complete because its mocks pass.

The smallest-model workflow is intentionally constrained: contracts and expected behavior are provided here, implementation is bounded, and integration/recovery changes still receive normal code review. No plan can guarantee that every model will implement every safety-critical protocol correctly without review and tests.

### B2. What “non-blocking” means here

- **Build after** lists actual prerequisites for coding/testing the component. **Integrate after** lists additional prerequisites for production wiring or qualification. These are different edges.
- A ticket with no build prerequisites is immediately available. Components use the frozen contracts and deterministic fakes; they do not wait for the full connector, control plane, UI, or cluster.
- Integration tickets replace fakes with real implementations, connect lifecycle/cancellation, and exercise the normal entry point. Unsupported paths remain rejected or unavailable until those tests pass.
- Do not remove a correctness dependency to create apparent parallelism. A checkpoint cannot be qualified before its source/state/effect boundary exists.
- Reserve files before starting. Tickets touching the same monolithic file can be prepared independently but must merge serially. Separate worktrees/checkouts prevent accidental edits to another ticket; rebase and rerun affected tests after the shared-file owner lands.
- Shared-file owners: contract bootstrap owns core exports; SQL integration owns AST/parser/validator registration; runtime integration owns engine/mod/runtime/windows; management integration owns routes/state/CLI; dependency registration owns workspace manifests/lockfile. Components own their new submodules and dedicated tests. Hand off small registration diffs to the relevant integrator.
- Do not add a global feature flag per ticket. Component code can remain unregistered. Introduce a named public capability/profile only in its integration ticket, disabled/rejected until its qualification gate passes.
- Every external adapter follows **contract → fake/conformance tests → implementation → production wiring → real-service qualification**. Backends can be added in parallel after the contract.
- Conditional capability dependencies named inside a ticket apply only when enabling that capability. Record them in the C08 ledger and P17-T04 qualification registry. A missing condition means that capability remains unavailable/unqualified; it never becomes a skipped passing test.

### B3. Fixed starting contracts

Freeze these version-1 contracts through C01–C05. Names can adapt to existing conventions during bootstrap, but semantics cannot change independently in a downstream ticket.

| Contract | Required shape and behavior |
|---|---|
| Identity | Stable logical rule/operator/action/connection IDs, separate display names, unsigned generation/checkpoint counters, and source plus partition identity. Renames do not silently change ownership. Validate lengths/characters at boundaries. |
| Source position | Opaque codec-tagged bytes plus source/partition identity and ownership generation. Kafka uses next offset as a signed integer inside its codec. Positions from different codecs/partitions cannot be compared. Progress is contiguous across all rows expanded from one input message. |
| Time | Inject a Clock with wall-time milliseconds, monotonic elapsed time, and cancellable deadlines. TestClock advances deterministically. Persist logical deadlines, not process-local Instant values. Event timestamps keep their declared precision; OTLP nanoseconds use lossless separate values. |
| Ordered control | A reliable internal envelope carries Data, Barrier(checkpoint, generation), Watermark(edge, timestamp), or EndOfInput(edge). Controls share ordering with preceding data. Default public record channels remain unchanged. Feedback cycles are unsupported in reliable v1 until qualified. |
| Effect outcome | Delivered, Retryable(delay hint), PermanentFailure, or Ambiguous. DurableAccepted is a separate source boundary, not a synonym for external delivery. Completion/fanout callbacks are idempotent; duplicate completion cannot advance progress twice. |
| State participant | Stable operator ID and state version; capture owned immutable snapshot, validate/restore before intake, report resource usage. The operator owner captures between records; background encoders cannot read mutable live state unsafely. |
| Checkpoint | Versioned JSON metadata plus checksummed payloads: rule/generation/plan/schema/extension identities, source positions, participant states, and effect boundary. Commit atomically with generation comparison; incompatible or corrupt restore fails explicitly. |
| Outbox v1 | Keep state, input positions, and pending effect rows in one reliable SQLite transaction, with payloads in that database for the first version. This avoids inventing cross-file atomicity. Stable effect IDs and ack state survive retry/restart. Segmented storage is a later implementation with its own crash protocol. |
| Capability | Each implementation declares input/result modes, ordering, replay/ack/durability/transaction/state/migration support, determinism, limits, and qualification level. Unspecified means unsupported. The planner computes the intersection needed by the entire plan. |
| Changelog | Stable key, Insert/Update/Delete operation, optional before/after values, source transaction/position, and event time. Append-only remains specialized. Duplicate source identity is distinct from equal payload. |
| Error | Stable code, stage, object/operator ID, retriable flag, redacted message, and optional source position. Preserve old API envelopes through adapters; do not expose secrets in raw errors. |
| Extension | Versioned manifest with engine/ABI compatibility, typed ports/functions, capabilities, permissions, artifact digest, state version, and migrations. Snapshot determinism and external side effects are declared explicitly. |

**Defaults for new reliable profiles:** fail closed on storage failure, no lossy queue, bounded counts/bytes/tasks, stable identity, at-least-once effects unless a specific transactional contract is qualified. Retain legacy lightweight defaults until the relevant integration ticket documents a migration.

**Dependency policy:** reuse current locked dependencies first. C06 registers required additions and platform/license checks; do not let independent workers each rewrite Cargo.lock. Foreign protocol/library versions are pinned in adapter fixtures when that adapter is implemented. Cluster v1 uses an external quorum coordination adapter, not a newly invented consensus algorithm.

### B4. Paths, tests, and completion evidence

All paths below are relative to the Rust repository `ekuiper/`, not this Markdown file's parent. Shorthand: `core/` = `crates/rekuiper-core/src/`; `sql/` = `crates/rekuiper-sql/src/`; `conn/` = `crates/rekuiper-connectors/src/`; `server/` = `crates/rekuiper-server/src/`; `cli/` = `crates/rekuiper-cli/src/`. A directory marked “new” is a proposed module boundary; reuse an equivalent existing module if main has evolved, and explain the mapping.

For each code ticket create or extend a meaningful target named `ticket_<lowercase_id_with_underscores>` in the owning crate's tests directory. If an existing fixture already covers the case, extend it and put its exact command in the handoff instead of duplicating it.

| Validation profile | Commands, from ekuiper/ |
|---|---|
| CORE | `cargo test -p rekuiper-core --test <target>` |
| SQL | `cargo test -p rekuiper-sql --test <target>` |
| CONN | `cargo test -p rekuiper-connectors --test <target>` |
| SERVER | `cargo test -p rekuiper-server --test <target>` |
| CLI | `cargo test -p rekuiper-cli --test <target>` |
| CROSS | Run the ticket targets in each changed crate, then the affected integration suites. |
| SERVICE | A real local/container service with pinned image/config and unique test namespace, plus the owning crate's target. Missing service means not qualified, never a silently passing skip. |
| DOC | Check referenced paths, examples/schema, links, and ID coverage; no runtime tests for prose-only changes. |

Every code PR also runs `cargo fmt --all -- --check` and affected compilation/lints. The integration/release owner runs the existing CI equivalents: `cargo clippy --workspace --all-targets -- -D warnings`, `cargo build -p kuiperd`, and `cargo test --workspace --verbose` on the supported platform matrix. External-service and failure-injection jobs are separate required qualification evidence for the corresponding capability.


**Backlog size:** 196 tickets. IDs are stable; gaps in numbering are intentional. All statuses start proposed. No ticket is marked built or qualified by this planning exercise.

| Lane | Tickets | Parallel-work boundary |
|---|---:|---|
| C: Shared contracts and scaffolding | 8 | Freeze shared types and deterministic fakes first |
| A: Independent correctness and KEEP tickets | 9 | Independent fixes; serialize edits to the same file |
| P01: Semantic contracts and capability-aware planning | 14 | New leaf modules can proceed against contracts; named integration tickets own wiring |
| P02: Resource budgets, buffers, batching, and cancellation | 8 | New leaf modules can proceed against contracts; named integration tickets own wiring |
| P03: Flagship A: durable execution and recoverable effects | 23 | New leaf modules can proceed against contracts; named integration tickets own wiring |
| P04: Flagship B: ordered delayed analytics and streaming LEAD | 7 | New leaf modules can proceed against contracts; named integration tickets own wiring |
| P05: Event-time progress and result modes | 6 | New leaf modules can proceed against contracts; named integration tickets own wiring |
| P06: Production-equivalent tests, provenance, DLQ, and replay | 7 | New leaf modules can proceed against contracts; named integration tickets own wiring |
| P07: A complete telemetry processing profile | 8 | New leaf modules can proceed against contracts; named integration tickets own wiring |
| P08: Industrial alerts and a general temporal rule model | 7 | New leaf modules can proceed against contracts; named integration tickets own wiring |
| P09: Changelogs, CDC, maintained relations, and reusable state | 15 | New leaf modules can proceed against contracts; named integration tickets own wiring |
| P10: Optimizer and performance engineering | 8 | New leaf modules can proceed against contracts; named integration tickets own wiring |
| P11: Versioned catalog, atomic activation, and safe upgrades | 7 | New leaf modules can proceed against contracts; named integration tickets own wiring |
| P12: Extension SDK, reusable components, and embedding | 10 | New leaf modules can proceed against contracts; named integration tickets own wiring |
| P13: Governance, isolation, and trustworthy operations | 7 | New leaf modules can proceed against contracts; named integration tickets own wiring |
| P14: High availability and distributed rule execution | 11 | New leaf modules can proceed against contracts; named integration tickets own wiring |
| P15: Interoperability, schemas, and the connector ecosystem | 24 | New leaf modules can proceed against contracts; named integration tickets own wiring |
| P16: Decision tables, policy evaluation, and forward chaining | 10 | New leaf modules can proceed against contracts; named integration tickets own wiring |
| P17: Compatibility, qualification, and an open ecosystem | 7 | New leaf modules can proceed against contracts; named integration tickets own wiring |

### B5. C — Shared contracts and scaffolding

<a id="ticket-c01"></a>

#### C01 — Freeze identities, positions, and errors

**Owns:** core/contracts/{identity,position,error}.rs (new); core/lib.rs. **Validation:** CORE (Core crate test). **Target:** `ticket_c01`.

**Build after:** None. **Integrate after:** None.

**Implementation steps:**

1. Implement validated ID newtypes, generation/checkpoint counters, codec-tagged opaque source positions, and structured redacted errors.
2. Keep display names separate and provide conversion adapters for existing identifiers; no runtime behavior change.

**Acceptance:** Golden JSON round trips; invalid IDs rejected; foreign position codecs/partitions cannot compare; errors redact secret fields; legacy IDs still convert.

<a id="ticket-c02"></a>

#### C02 — Freeze capabilities and plan requirements

**Owns:** core/contracts/capability.rs (new). **Validation:** CORE (Core crate test). **Target:** `ticket_c02`.

**Build after:** [C01](#ticket-c01). **Integrate after:** None.

**Implementation steps:**

1. Implement B3 capability descriptors and a pure requirement checker.
2. Unspecified fields mean unsupported.
3. Return every failing component and reason in stable order; keep certification level distinct from claimed implementation capability.

**Acceptance:** A replayable source plus nonsnapshotting operator fails durable validation; a retract plan plus append-only sink fails; a legacy lightweight plan retains its current acceptance.

<a id="ticket-c03"></a>

#### C03 — Freeze clocks and ordered controls

**Owns:** core/contracts/{clock,control}.rs (new). **Validation:** CORE (Core crate test). **Target:** `ticket_c03`.

**Build after:** [C01](#ticket-c01). **Integrate after:** None.

**Implementation steps:**

1. Define injected real/test clocks and internal Data/Barrier/Watermark/EndOfInput envelope types.
2. Use deterministic cancellable waits; preserve StreamRecord/public StreamReceiver.
3. Encode stable deadlines separately from monotonic runtime instants.

**Acceptance:** Advance test time without sleeps; cancellation wakes a waiter; controls preserve queue order; timestamp extremes and restart deadline conversion have fixed expected results.

<a id="ticket-c04"></a>

#### C04 — Freeze state, checkpoint, and effect interfaces

**Owns:** core/contracts/{state,checkpoint,effect}.rs (new). **Validation:** CORE (Core crate test). **Target:** `ticket_c04`.

**Build after:** [C01](#ticket-c01), [C02](#ticket-c02), [C03](#ticket-c03). **Integrate after:** None.

**Implementation steps:**

1. Define versioned participant snapshots, atomic generation-fenced checkpoint commit, outbox identity, and four external effect outcomes from B3.
2. Specify method inputs/results and ownership; add serialized example envelopes without storage or network implementations.

**Acceptance:** Golden fixtures reject unknown versions, missing participants, wrong plan/generation, and malformed hashes; repeated completion cannot produce a second terminal outcome.

<a id="ticket-c05"></a>

#### C05 — Freeze executable fixture schema

**Owns:** core/contracts/fixture.rs (new); test/fixtures/contracts/ (new). **Validation:** CORE (Core crate test). **Target:** `ticket_c05`.

**Build after:** [C01](#ticket-c01), [C03](#ticket-c03). **Integrate after:** None.

**Implementation steps:**

1. Define versioned fixture JSON for catalog, inputs, metadata, clock advances, watermarks, EOF, expected rows/effects/errors, and optional restore.
2. Include a tiny transform and a window example with manually calculated expectations.

**Acceptance:** Parse valid examples; reject unknown step kinds and contradictory expectations; preserve input ordering and precise timestamps; malformed fixtures identify their location.

<a id="ticket-c06"></a>

#### C06 — Provide dependency and registration discipline

**Owns:** docs/implementation/dependencies.md (new); workspace manifests/lockfile only when required. **Validation:** DOC (Document/schema/tool checks).

**Build after:** None. **Integrate after:** None.

**Implementation steps:**

1. Record existing dependencies and one owner for manifest/lockfile edits.
2. For an adapter addition, pin its resolved version, features, supported platforms, license, and build evidence; make only the requested dependency/feature registration patch.

**Acceptance:** Document exact build commands and dependency diff; existing default core/SQL dependency isolation stays intact; an optional adapter does not become an unconditional core dependency.

<a id="ticket-c07"></a>

#### C07 — Implement deterministic fake boundaries

**Owns:** core/testing/{source,state_store,effect_sink}.rs (new, test support). **Validation:** CORE (Core crate test). **Target:** `ticket_c07`.

**Build after:** [C04](#ticket-c04), [C05](#ticket-c05). **Integrate after:** None.

**Implementation steps:**

1. Implement ordered replay source, transactional memory checkpoint/outbox store, and programmable effect sink against real contracts.
2. Add named failpoints and operation logs; fakes must model ambiguous outcomes and atomic commit rollback, not only happy paths.

**Acceptance:** Use the same conformance cases for later adapters; failed commit changes nothing; stale generations fail; duplicated completion is idempotent; source restart resumes its exact position.

<a id="ticket-c08"></a>

#### C08 — Create ticket ledger and independent-work checker

**Owns:** docs/implementation/backlog.json; test/tools/check_backlog.py (new). **Validation:** DOC (Document/schema/tool checks).

**Build after:** None. **Integrate after:** None.

**Implementation steps:**

1. Encode ticket IDs, build/integration edges, owned files, status, and evidence links from this appendix.
2. Implement validation for unknown IDs, dependency cycles, duplicate ownership reservations, and ready-work selection.
3. Status starts proposed, not done.

**Acceptance:** A fixture with a cycle fails; missing dependency fails; an unrelated ready ticket remains selectable when another is blocked; component-done never implies integrated/qualified.


### B6. A — Independent correctness and KEEP tickets

<a id="ticket-a01-t01"></a>

#### A01-T01 — Reproduce REST connection consumption behavior

**Owns:** crates/rekuiper-server/tests support; existing test_issue18_sinks.rs. **Validation:** SERVER (Server crate test). **Target:** `ticket_a01_t01`.

**Build after:** None. **Integrate after:** None.

**Implementation steps:**

1. Add a local HTTP/1.1 server counting accepted sockets and returning controlled nonempty, slow, truncated, and large bodies.
2. Drive the real REST action, including batch/cache replay entry points.
3. Record current status/retry behavior before changing it.

**Acceptance:** Repeated requests expose reuse counts; fixtures distinguish 2xx, permanent 4xx, 429, and 5xx; no arbitrary sleep-based assertions; test identifies the active status-only code paths.

<a id="ticket-a01-t02"></a>

#### A01-T02 — Bound response draining without duplicate delivery

**Owns:** server/engine/sinks.rs; conn/http.rs. **Validation:** CROSS (Affected crates and integration tests). **Target:** `ticket_a01_t02`.

**Build after:** [A01-T01](#ticket-a01-t01). **Integrate after:** None.

**Implementation steps:**

1. Implement chunked ignored-body draining under byte/time budgets only on affected status-only paths.
2. Preserve response classification and egress/redirect policy.
3. A successful status remains delivered if later body drain fails; count the diagnostic separately.

**Acceptance:** A01-T01 reuse case improves or proves already correct; oversized/stalled bodies terminate within the configured bound; a 2xx drain failure never schedules a retry; error statuses retain existing retry decisions.

<a id="ticket-a02-t01"></a>

#### A02-T01 — Specify image root and relative-path validation

**Owns:** conn/image_sink.rs; conn/image_paths.rs (new). **Validation:** CONN (Connector crate test). **Target:** `ticket_a02_t01`.

**Build after:** None. **Integrate after:** None.

**Implementation steps:**

1. Separate trusted configured root from row-rendered components.
2. Preserve valid static absolute roots and strict basenames; reject unanchored templates, parent traversal, drive/UNC escapes, and empty invalid components.
3. Produce resolved relative components without opening files.

**Acceptance:** Table-driven Windows/POSIX path cases; valid legacy static paths remain valid; invalid cases fail before directory creation; input templates cannot select a different trusted root.

<a id="ticket-a02-t02"></a>

#### A02-T02 — Implement anchored POSIX image operations

**Owns:** conn/image_io/unix.rs (new). **Validation:** CONN (Connector crate test). **Target:** `ticket_a02_t02`.

**Build after:** [A02-T01](#ticket-a02-t01). **Integrate after:** None.

**Implementation steps:**

1. Implement directory-handle-relative creation/open/replace/cleanup with no-follow behavior for every path component.
2. Write a temporary file inside the anchored directory, then replace the entry atomically.
3. Use a vetted API registered through C06 if needed.

**Acceptance:** Linux/macOS tests reject intermediate/final symlinks and link-swap races; failed writes clean temporary files; replacement never modifies a file outside the root.

<a id="ticket-a02-t03"></a>

#### A02-T03 — Implement anchored Windows image operations

**Owns:** conn/image_io/windows.rs (new). **Validation:** CONN (Connector crate test). **Target:** `ticket_a02_t03`.

**Build after:** [A02-T01](#ticket-a02-t01). **Integrate after:** None.

**Implementation steps:**

1. Implement the same anchored I/O contract for Windows using handles and explicit reparse-point rejection/containment.
2. Do not replace the contract with canonicalize-then-write.
3. Preserve supported overwrite/retention behavior; return a structured unsupported error for unenforceable cases.

**Acceptance:** Windows junction, symlink, final reparse, rename-race, and overwrite fixtures remain within the root. Missing privilege produces an explicit unqualified test result rather than silently passing containment.

<a id="ticket-a02-t04"></a>

#### A02-T04 — Wire anchored image writing and retention

**Owns:** conn/image_sink.rs; conn/image_io/mod.rs (new). **Validation:** CONN (Connector crate test). **Target:** `ticket_a02_t04`.

**Build after:** [A02-T02](#ticket-a02-t02), [A02-T03](#ticket-a02-t03). **Integrate after:** None.

**Implementation steps:**

1. Use the platform implementation for normal writes, overwrite, temporary cleanup, and retention deletion.
2. Remove direct unsafe fallback writes.
3. Add root-template migration examples and validate configuration before starting the connector.

**Acceptance:** Production sink passes both platform suites; retention cannot traverse foreign links; write failure leaves no partial target; legacy supported static-root configurations still work.

<a id="ticket-a07-t01"></a>

#### A07-T01 — Implement immutable nested object_pick

**Owns:** sql/eval/array.rs. **Validation:** SQL (SQL crate test). **Target:** `ticket_a07_t01`.

**Build after:** None. **Integrate after:** None.

**Implementation steps:**

1. Resolve exact literal keys before dotted traversal; merge selected paths into a new object.
2. Preserve string-list/array arguments.
3. Define parent-child overlap by copying the selected parent value, independent of selection order; never mutate the input.

**Acceptance:** Exact dotted key wins; deep sibling selections retain hierarchy; parent/child order yields the same result; absent paths and duplicate selections behave consistently; original object remains unchanged.

<a id="ticket-a07-t02"></a>

#### A07-T02 — Correct distinct array_except results

**Owns:** sql/eval/array.rs; array function documentation. **Validation:** SQL (SQL crate test). **Target:** `ticket_a07_t02`.

**Build after:** None. **Integrate after:** None.

**Implementation steps:**

1. Keep existing deep comparison and first-seen order.
2. Deduplicate surviving elements in array_except using the current equality helper.
3. Add differential cases for union/intersect/except; avoid a new hash-based equality implementation.

**Acceptance:** [1,1,2] except [2] returns [1]; nested arrays/objects do not panic; numeric/null behavior matches current equality; existing union/intersect ordering remains unchanged.

<a id="ticket-a07-t03"></a>

#### A07-T03 — Qualify features already better or present

**Owns:** SQL/connector compatibility tests; docs/implementation/kept-features.md. **Validation:** CROSS (Affected crates and integration tests). **Target:** `ticket_a07_t03`.

**Build after:** None. **Integrate after:** None.

**Implementation steps:**

1. Add fixtures for array_avg, acc_max_by/min_by ties, acc_map_agg updates, array_positions, acc_distinct_collect, disabled actions, DDL defaults, Parquet, IoTDB, and explicit Kafka offsets.
2. Record KEEP where current behavior meets the contract; correct only reproduced mismatches.

**Acceptance:** Each named capability has a fixture or linked existing passing test; bounded accumulators remain bounded; no duplicate feature implementation; any upstream dialect differences are labelled with pinned references.


### B7. P01 — Semantic contracts and capability-aware planning

<a id="ticket-p01-t01"></a>

#### P01-T01 — Create value-semantics conformance corpus

**Owns:** crates/rekuiper-sql/tests; test/fixtures/semantics/ (new). **Validation:** SQL (SQL crate test). **Target:** `ticket_p01_t01`.

**Build after:** None. **Integrate after:** None.

**Implementation steps:**

1. Write manually checked fixtures for null/missing, numeric comparison/overflow, deep values, literal/nested keys, ordering, and timestamp conversions.
2. Execute through existing evaluator; separate intentional behavior from bugs requiring a follow-up.

**Acceptance:** Fixtures exercise boundary values and expected errors; every expectation cites current contract or approved correction; no expected output is generated by the implementation under test.

<a id="ticket-p01-t02"></a>

#### P01-T02 — Add typed expression and function-descriptor infrastructure

**Owns:** sql/functions.rs; sql/planning/types.rs (new). **Validation:** SQL (SQL crate test). **Target:** `ticket_p01_t02`.

**Build after:** [C02](#ticket-c02). **Integrate after:** None.

**Implementation steps:**

1. Define argument/result/nullability/determinism/side-effect/statefulness descriptors and type-check known schema fields.
2. Register a few representative pure/stateful functions through adapters; family tickets fill out the registry.
3. Preserve generic runtime evaluation for legacy schemaless plans and use conservative unsupported capabilities for missing descriptors.

**Acceptance:** Known invalid calls fail; valid legacy schemaless expressions still execute; representative descriptor tests pass; missing metadata cannot silently enable durable replay or unsafe optimizer rewrites.

<a id="ticket-p01-t03"></a>

#### P01-T03 — Build capability-aware plan validation

**Owns:** sql/planning/capabilities.rs (new). **Validation:** SQL (SQL crate test). **Target:** `ticket_p01_t03`.

**Build after:** [C02](#ticket-c02), [P01-T02](#ticket-p01-t02). **Integrate after:** None.

**Implementation steps:**

1. Translate logical plan/operator requirements into C02 checks.
2. Report all incompatible nodes with stable IDs, including delayed/stateful modes, result modes, and required replay guarantees.
3. Default lightweight validation stays compatible.

**Acceptance:** Durable plus lossy queue, retract plus append-only sink, and stateful extension without snapshot fail with distinct diagnostics; supported ordinary SELECT remains unchanged.

<a id="ticket-p01-t04"></a>

#### P01-T04 — Expose typed plans and diagnostics consistently

**Owns:** server/handlers/rules.rs; cli/lib.rs; explain/validate adapters. **Validation:** CROSS (Affected crates and integration tests). **Target:** `ticket_p01_t04`.

**Build after:** [P01-T03](#ticket-p01-t03). **Integrate after:** None.

**Implementation steps:**

1. Wire one planner into server validate/explain and CLI validation using adapters over the existing catalog. P11-T03 later reuses this planner for immutable candidate generations; it is not a prerequisite for current API integration.
2. Preserve existing response envelopes through adapters while adding stable codes, locations, effective guarantees, and limiting component.
3. Do not expose secrets in plan output.

**Acceptance:** Equivalent CLI/REST inputs return the same semantic result; invalid combinations fail before tasks start; existing envelope compatibility tests pass; plans redact connection credentials.

<a id="ticket-p01-t05"></a>

#### P01-T05 — Stop reporting success after schema persistence failure

**Owns:** server/handlers/plugins.rs; schema manager tests. **Validation:** SERVER (Server crate test). **Target:** `ticket_p01_t05`.

**Build after:** None. **Integrate after:** None.

**Implementation steps:**

1. Propagate register/update/delete persistence errors instead of discarding them.
2. Use a failing store fixture and preserve the previous active definition when persistence fails.
3. Apply the same fix to other directly reproduced handler cases, without a broad refactor.

**Acceptance:** Injected write failure returns the documented error envelope; failed create is absent after restart; failed update keeps the old schema; successful create/update still pass existing API tests.

<a id="ticket-p01-t06"></a>

#### P01-T06 — Audit accepted configuration against real behavior

**Owns:** docs/implementation/config-contracts.md; targeted config tests. **Validation:** CROSS (Affected crates and integration tests). **Target:** `ticket_p01_t06`.

**Build after:** None. **Integrate after:** None.

**Implementation steps:**

1. Enumerate public runtime-affecting settings from typed configs, validation, UI metadata, and docs.
2. For each, identify consuming code and one observable acceptance case.
3. Separate proven no-op settings from settings whose path needs investigation; fix only bounded cases with a dedicated follow-up.

**Acceptance:** Every audited option has default/type/consumer/test; accepted no-op settings are rejected/deprecated or covered by an implementation ticket; keyword presence alone is not used as proof of behavior.

<a id="ticket-p01-t10"></a>

#### P01-T10 — Describe numeric function family

**Owns:** sql/planning/functions/numeric.rs (new); sql/eval/math.rs. **Validation:** SQL (SQL crate test). **Target:** `ticket_p01_t10`.

**Build after:** [P01-T02](#ticket-p01-t02), [P01-T01](#ticket-p01-t01). **Integrate after:** None.

**Implementation steps:**

1. Inventory functions implemented in math.rs, add typed descriptors and aliases through the common registry, and preserve the evaluator contract.
2. Write family-specific fixtures for numeric boundaries, nonfinite values, coercion and overflow.
3. Mark genuinely unsupported capabilities explicitly; do not change evaluation to fit a descriptor.

**Acceptance:** Every function/alias in this family is covered or explicitly accounted for; descriptor-derived validation matches real evaluator outcomes; pure/stateful classification prevents invalid optimization/replay; no duplicate registry names.

<a id="ticket-p01-t11"></a>

#### P01-T11 — Describe string function family

**Owns:** sql/planning/functions/string.rs (new); sql/eval/string.rs. **Validation:** SQL (SQL crate test). **Target:** `ticket_p01_t11`.

**Build after:** [P01-T02](#ticket-p01-t02), [P01-T01](#ticket-p01-t01). **Integrate after:** None.

**Implementation steps:**

1. Inventory functions implemented in string.rs, add typed descriptors and aliases through the common registry, and preserve the evaluator contract.
2. Write family-specific fixtures for Unicode, byte-versus-character positions, nulls and invalid patterns.
3. Mark genuinely unsupported capabilities explicitly; do not change evaluation to fit a descriptor.

**Acceptance:** Every function/alias in this family is covered or explicitly accounted for; descriptor-derived validation matches real evaluator outcomes; pure/stateful classification prevents invalid optimization/replay; no duplicate registry names.

<a id="ticket-p01-t12"></a>

#### P01-T12 — Describe array/object function family

**Owns:** sql/planning/functions/array_object.rs (new); sql/eval/array.rs. **Validation:** SQL (SQL crate test). **Target:** `ticket_p01_t12`.

**Build after:** [P01-T02](#ticket-p01-t02), [P01-T01](#ticket-p01-t01). **Integrate after:** None.

**Implementation steps:**

1. Inventory functions implemented in array.rs, add typed descriptors and aliases through the common registry, and preserve the evaluator contract.
2. Write family-specific fixtures for deep equality, ordering, literal/nested paths, duplicate and null semantics.
3. Mark genuinely unsupported capabilities explicitly; do not change evaluation to fit a descriptor.

**Acceptance:** Every function/alias in this family is covered or explicitly accounted for; descriptor-derived validation matches real evaluator outcomes; pure/stateful classification prevents invalid optimization/replay; no duplicate registry names.

<a id="ticket-p01-t13"></a>

#### P01-T13 — Describe json function family

**Owns:** sql/planning/functions/json.rs (new); sql/eval/json.rs. **Validation:** SQL (SQL crate test). **Target:** `ticket_p01_t13`.

**Build after:** [P01-T02](#ticket-p01-t02), [P01-T01](#ticket-p01-t01). **Integrate after:** None.

**Implementation steps:**

1. Inventory functions implemented in json.rs, add typed descriptors and aliases through the common registry, and preserve the evaluator contract.
2. Write family-specific fixtures for missing versus null, path syntax, malformed input and output type.
3. Mark genuinely unsupported capabilities explicitly; do not change evaluation to fit a descriptor.

**Acceptance:** Every function/alias in this family is covered or explicitly accounted for; descriptor-derived validation matches real evaluator outcomes; pure/stateful classification prevents invalid optimization/replay; no duplicate registry names.

<a id="ticket-p01-t14"></a>

#### P01-T14 — Describe datetime function family

**Owns:** sql/planning/functions/datetime.rs (new); sql/eval/datetime.rs. **Validation:** SQL (SQL crate test). **Target:** `ticket_p01_t14`.

**Build after:** [P01-T02](#ticket-p01-t02), [P01-T01](#ticket-p01-t01). **Integrate after:** None.

**Implementation steps:**

1. Inventory functions implemented in datetime.rs, add typed descriptors and aliases through the common registry, and preserve the evaluator contract.
2. Write family-specific fixtures for timestamp units, time zones, invalid formats and deterministic versus clock-dependent calls.
3. Mark genuinely unsupported capabilities explicitly; do not change evaluation to fit a descriptor.

**Acceptance:** Every function/alias in this family is covered or explicitly accounted for; descriptor-derived validation matches real evaluator outcomes; pure/stateful classification prevents invalid optimization/replay; no duplicate registry names.

<a id="ticket-p01-t15"></a>

#### P01-T15 — Describe aggregate function family

**Owns:** sql/planning/functions/aggregate.rs (new); sql/eval/aggregate.rs. **Validation:** SQL (SQL crate test). **Target:** `ticket_p01_t15`.

**Build after:** [P01-T02](#ticket-p01-t02), [P01-T01](#ticket-p01-t01). **Integrate after:** None.

**Implementation steps:**

1. Inventory functions implemented in aggregate.rs, add typed descriptors and aliases through the common registry, and preserve the evaluator contract.
2. Write family-specific fixtures for group input/result types, empty groups, retention, merge and retract support.
3. Mark genuinely unsupported capabilities explicitly; do not change evaluation to fit a descriptor.

**Acceptance:** Every function/alias in this family is covered or explicitly accounted for; descriptor-derived validation matches real evaluator outcomes; pure/stateful classification prevents invalid optimization/replay; no duplicate registry names.

<a id="ticket-p01-t16"></a>

#### P01-T16 — Describe analytic function family

**Owns:** sql/planning/functions/analytic.rs (new); sql/eval/analytic.rs. **Validation:** SQL (SQL crate test). **Target:** `ticket_p01_t16`.

**Build after:** [P01-T02](#ticket-p01-t02), [P01-T01](#ticket-p01-t01). **Integrate after:** None.

**Implementation steps:**

1. Inventory functions implemented in analytic.rs, add typed descriptors and aliases through the common registry, and preserve the evaluator contract.
2. Write family-specific fixtures for statefulness, ordered evaluation, LEAD/alias dependencies and snapshot requirements.
3. Mark genuinely unsupported capabilities explicitly; do not change evaluation to fit a descriptor.

**Acceptance:** Every function/alias in this family is covered or explicitly accounted for; descriptor-derived validation matches real evaluator outcomes; pure/stateful classification prevents invalid optimization/replay; no duplicate registry names.

<a id="ticket-p01-t17"></a>

#### P01-T17 — Describe transform function family

**Owns:** sql/planning/functions/transform.rs (new); sql/eval/transform.rs. **Validation:** SQL (SQL crate test). **Target:** `ticket_p01_t17`.

**Build after:** [P01-T02](#ticket-p01-t02), [P01-T01](#ticket-p01-t01). **Integrate after:** None.

**Implementation steps:**

1. Inventory functions implemented in transform.rs, add typed descriptors and aliases through the common registry, and preserve the evaluator contract.
2. Write family-specific fixtures for row expansion/cardinality, field types, metadata propagation and deterministic behavior.
3. Mark genuinely unsupported capabilities explicitly; do not change evaluation to fit a descriptor.

**Acceptance:** Every function/alias in this family is covered or explicitly accounted for; descriptor-derived validation matches real evaluator outcomes; pure/stateful classification prevents invalid optimization/replay; no duplicate registry names.


### B8. P02 — Resource budgets, buffers, batching, and cancellation

<a id="ticket-p02-t01"></a>

#### P02-T01 — Implement count/byte budget primitives

**Owns:** core/resources/budget.rs (new). **Validation:** CORE (Core crate test). **Target:** `ticket_p02_t01`.

**Build after:** [C01](#ticket-c01). **Integrate after:** None.

**Implementation steps:**

1. Implement cancellable reserve/commit/release for item, logical-byte, and task budgets.
2. Make reservation release automatic on cancellation/error.
3. Define conservative charges for shared data and distinguish charged versus observed physical bytes.

**Acceptance:** Concurrent reservations never exceed configured limits; cancellation releases all charges; oversize items return explicit errors; double release cannot underflow counters.

<a id="ticket-p02-t02"></a>

#### P02-T02 — Implement explicit lossy subscriber FIFO

**Owns:** core/runtime.rs; core/resources/lossy_queue.rs (new). **Validation:** CORE (Core crate test). **Target:** `ticket_p02_t02`.

**Build after:** [P02-T01](#ticket-p02-t01). **Integrate after:** None.

**Implementation steps:**

1. Implement opt-in dropOldest subscription queues while keeping current mpsc block/default path.
2. Filter before enqueue, atomically evict/append, preserve batch newest suffix, and make close/wakeup race-safe.
3. Do not change nonblocking all-or-none feedback admission.

**Acceptance:** Capacity two with 1,2,3 yields 2,3 and one eviction; blocking yields all in order; concurrent publishers/removal/oversized batches pass; reliable control messages cannot enter an evictable queue.

<a id="ticket-p02-t03"></a>

#### P02-T03 — Wire and validate stream buffer policies

**Owns:** server/engine/sources.rs; stream/DDL validation; core runtime adapters. **Validation:** CROSS (Affected crates and integration tests). **Target:** `ticket_p02_t03`.

**Build after:** [P02-T02](#ticket-p02-t02), [P01-T03](#ticket-p01-t03). **Integrate after:** None.

**Implementation steps:**

1. Resolve policy once across create/import/start and wire every publication path.
2. Preserve existing mixed-policy validation and feedback behavior.
3. Reject lossy policies for reliable plans; expose per-stream depth/drop counts without high-cardinality labels.

**Acceptance:** HTTP, async, batch, and feedback fixtures agree on admission behavior; missing policy remains block; explicit dropOldest has visible effect; reliable plus lossy is rejected.

<a id="ticket-p02-t04"></a>

#### P02-T04 — Make rate-limit buffer capacity real

**Owns:** server/engine/operators.rs; DAG integration. **Validation:** SERVER (Server crate test). **Target:** `ticket_p02_t04`.

**Build after:** [P02-T01](#ticket-p02-t01), [C03](#ticket-c03). **Integrate after:** None.

**Implementation steps:**

1. Replace unused max_buffer behavior with a bounded FIFO for buffer strategy, charged to rule budgets.
2. Use a single cancellable scheduler for token availability; preserve drop strategy and per-edge FIFO.
3. Reject invalid rate/burst/capacity values.

**Acceptance:** Buffer overflow follows explicit configured outcome; cancellation ends waiting work; burst/refill tests use TestClock; 10000 queued inputs cannot create 10000 unbounded sleeping tasks.

<a id="ticket-p02-t05"></a>

#### P02-T05 — Add bounded count/byte/time action batches

**Owns:** server/engine/batching.rs (new); sinks adapter. **Validation:** SERVER (Server crate test). **Target:** `ticket_p02_t05`.

**Build after:** [P02-T01](#ticket-p02-t01), [C03](#ticket-c03), [C04](#ticket-c04). **Integrate after:** None.

**Implementation steps:**

1. Implement batch assembly with count, encoded-byte, and deadline thresholds.
2. Define oversize-record error, stable ordering, EOF/shutdown/barrier flush, and per-item result mapping.
3. Preserve existing batchSize/linger behavior when byte limit is unset.

**Acceptance:** Each threshold independently flushes; a barrier cannot overtake a batch; partial failures retain only unsettled items; clock-driven tests avoid wall sleeps; buffers stay within charge limits.

<a id="ticket-p02-t06"></a>

#### P02-T06 — Centralize bounded retry and effect outcomes

**Owns:** server/engine/retry.rs (new). **Validation:** SERVER (Server crate test). **Target:** `ticket_p02_t06`.

**Build after:** [C04](#ticket-c04), [C03](#ticket-c03), [P02-T01](#ticket-p02-t01). **Integrate after:** None.

**Implementation steps:**

1. Map adapter results to Delivered/Retryable/Permanent/Ambiguous.
2. Implement capped exponential backoff with injected seeded jitter, bounded Retry-After, total deadline, and cancellation.
3. Ambiguous delivery follows idempotency/reconciliation policy, not automatic permanent failure.

**Acceptance:** Exact seeded retry schedule; 2xx drain failure stays delivered; permanent failures are not retried; cancellation stops waits; retries and pending tasks remain bounded during an extended outage.

<a id="ticket-p02-t07"></a>

#### P02-T07 — Bound asynchronous lookup concurrency

**Owns:** server/engine/joins.rs; server/engine/lookup_runtime.rs (new). **Validation:** SERVER (Server crate test). **Target:** `ticket_p02_t07`.

**Build after:** [P02-T01](#ticket-p02-t01), [C03](#ticket-c03). **Integrate after:** None.

**Implementation steps:**

1. Add bounded request/byte queues, concurrency permits, deadline and cancellation around existing async lookup paths.
2. Preserve current timeouts and ordinary first-match semantics.
3. Keep ordered output by default; expose unordered mode only with an explicit future contract.

**Acceptance:** Stalled requests cannot consume unbounded tasks; rule stop cancels queued/inflight work; permit leaks are absent after errors; ordinary lookup outputs and ordering match baseline.

<a id="ticket-p02-t08"></a>

#### P02-T08 — Add nonoverlapping poll/flush schedules

**Owns:** conn/scheduling.rs (new); poll-source adapters. **Validation:** CONN (Connector crate test). **Target:** `ticket_p02_t08`.

**Build after:** [C03](#ticket-c03). **Integrate after:** None.

**Implementation steps:**

1. Implement interval, offset, seeded jitter, and missed-interval policy.
2. Default to one in-flight poll and preserve event timestamps.
3. Adapt one existing polling connector first, then reuse the helper for others; do not change service-input semantics.

**Acceptance:** Fake-clock schedule matches exact expected instants; a slow poll does not overlap; cancellation wakes immediately; different seeds distribute fleet starts; existing zero-jitter cadence stays compatible.


### B9. P03 — Flagship A: durable execution and recoverable effects

<a id="ticket-p03-t01"></a>

#### P03-T01 — Implement atomic reliable SQLite storage

**Owns:** core/reliability/sqlite.rs (new). **Validation:** CORE (Core crate test). **Target:** `ticket_p03_t01`.

**Build after:** [C04](#ticket-c04), [C07](#ticket-c07). **Integrate after:** None.

**Implementation steps:**

1. Create versioned tables for generations, checkpoints, positions, participant payloads, outbox effects, and acknowledgements.
2. Commit checkpoint/state/effects atomically with generation comparison.
3. Use checked durable connection settings for this separate profile; retain legacy KV semantics.

**Acceptance:** Run C07 transaction conformance against a file database; rollback changes nothing; stale generation fails; reopen returns exact state/effects; requested durability settings are read back and verified.

<a id="ticket-p03-t02"></a>

#### P03-T02 — Implement idempotent delivery accounting

**Owns:** core/reliability/delivery.rs (new). **Validation:** CORE (Core crate test). **Target:** `ticket_p03_t02`.

**Build after:** [C04](#ticket-c04). **Integrate after:** None.

**Implementation steps:**

1. Track one source message expanding to many records/branches.
2. Complete a position only after all required branches are terminal or covered by committed state/outbox.
3. Deduplicate callbacks and maintain contiguous partition progress across out-of-order completions.

**Acceptance:** Completing offsets 3 then 1 does not skip 2; one failed fanout branch prevents completion; zero-output intentional filter terminates correctly; duplicate callbacks do not decrement counters twice.

<a id="ticket-p03-t03"></a>

#### P03-T03 — Implement ordered reliable source ingress

**Owns:** server/engine/reliable_input.rs (new). **Validation:** SERVER (Server crate test). **Target:** `ticket_p03_t03`.

**Build after:** [C03](#ticket-c03), [P03-T02](#ticket-p03-t02). **Integrate after:** None.

**Implementation steps:**

1. Add reliable-path input queues with source position, ownership generation, and ordered controls.
2. Do not alter the default public StreamReceiver.
3. Apply cancellation and admission limits; define malformed-message terminal disposition before advancing progress.

**Acceptance:** Data precedes its barrier/EOF; a multirow message is one position; rejected/malformed input cannot silently advance; old ownership generations are refused; lightweight ingress benchmark stays unchanged.

<a id="ticket-p03-t04"></a>

#### P03-T04 — Implement a coordinator against fake participants

**Owns:** core/reliability/coordinator.rs (new). **Validation:** CORE (Core crate test). **Target:** `ticket_p03_t04`.

**Build after:** [C04](#ticket-c04), [C07](#ticket-c07), [P03-T02](#ticket-p03-t02). **Integrate after:** None.

**Implementation steps:**

1. Use explicit Idle/Capturing/WaitingEffects/Committing/Aborting states with one active checkpoint per generation.
2. Gather immutable snapshots, verify participants, await required effect boundaries, commit atomically, and abort on timeout/cancel.
3. Never hold live-state locks during storage I/O.

**Acceptance:** Named failpoints at every transition leave the previous commit valid; timeout unblocks participants; stale completions cannot commit; duplicate requests receive a stable busy/existing-ID response.

<a id="ticket-p03-t05"></a>

#### P03-T05 — Snapshot analytic, count, and tumbling state

**Owns:** server/engine/state/analytic_count_tumble.rs (new); runtime/windows adapters. **Validation:** SERVER (Server crate test). **Target:** `ticket_p03_t05`.

**Build after:** [C04](#ticket-c04). **Integrate after:** None.

**Implementation steps:**

1. Move owned loop state behind capture/restore adapters without changing normal outputs.
2. Include accumulators, retained rows, window boundaries, source progress policy, watermark, and timers.
3. Serialize versioned payloads; reject mismatched plan/state version.

**Acceptance:** Split each fixture at every event boundary, snapshot/recreate/resume, and compare with uninterrupted execution; empty/open windows restore; capture never shares mutable data with an encoder.

<a id="ticket-p03-t06"></a>

#### P03-T06 — Add replay-position support to direct Kafka source

**Owns:** conn/kafka.rs; Kafka position codec. **Validation:** CONN (Connector crate test). **Target:** `ticket_p03_t06`.

**Build after:** [C01](#ticket-c01), [C04](#ticket-c04). **Integrate after:** None.

**Implementation steps:**

1. Allow explicit next-offset restore in direct-partition mode.
2. Emit metadata/position before decode; advance only via accepted/committed reliable boundaries.
3. Detect retention gaps and tombstones using a specified terminal policy; preserve legacy earliest/latest mode.

**Acceptance:** Real broker restart resumes the supplied next offset; multirecord payload does not partially commit; missing retained offset fails visibly; ordinary direct reader configuration remains compatible.

<a id="ticket-p03-t07"></a>

#### P03-T07 — Integrate single-source SQL checkpoint recovery

**Owns:** server/engine/runtime.rs; reliable engine registration. **Validation:** SERVER (Server crate test). **Target:** `ticket_p03_t07`.

**Build after:** [P03-T01](#ticket-p03-t01), [P03-T03](#ticket-p03-t03), [P03-T04](#ticket-p03-t04), [P03-T05](#ticket-p03-t05), [P03-T06](#ticket-p03-t06), [P02-T05](#ticket-p02-t05), [P02-T06](#ticket-p02-t06). **Integrate after:** None.

**Implementation steps:**

1. Wire one Kafka partition, qualified analytic/count/tumble plans, and acknowledged actions through the real coordinator.
2. Restore before intake.
3. Flush output before checkpoint commit; keep unsupported shapes rejected.
4. Expose checkpoint ID/age/failure and exact guarantee.

**Acceptance:** Real broker plus recording sink survives crash before/after sink ack/commit with no missing records under at-least-once semantics; expected duplicates occur only within replay scope; no unsupported plan silently runs reliable.

<a id="ticket-p03-t08"></a>

#### P03-T08 — Persist state and pending effects as one transaction

**Owns:** core/reliability/outbox.rs (new). **Validation:** CORE (Core crate test). **Target:** `ticket_p03_t08`.

**Build after:** [P03-T01](#ticket-p03-t01), [P03-T02](#ticket-p03-t02). **Integrate after:** None.

**Implementation steps:**

1. Implement enqueue/deduplicate/claim/ack APIs over the reliable database.
2. Assign effect ID from stable rule generation/operator/input-or-checkpoint identity and deterministic output ordinal.
3. Keep payloads in SQLite v1; enforce item/byte capacity and destination ownership.

**Acceptance:** Crash before commit retains neither state nor effects; after commit retains both; repeated effect insertion is idempotent; full storage refuses admission; action reorder cannot change ownership.

<a id="ticket-p03-t09"></a>

#### P03-T09 — Dispatch durable effects with bounded retries

**Owns:** server/engine/outbox_dispatch.rs (new). **Validation:** SERVER (Server crate test). **Target:** `ticket_p03_t09`.

**Build after:** [P03-T08](#ticket-p03-t08), [P02-T06](#ticket-p02-t06), [P02-T01](#ticket-p02-t01). **Integrate after:** [P11-T01](#ticket-p11-t01).

**Implementation steps:**

1. Claim pending effects by stable action identity, send through existing action adapters, and persist terminal acknowledgements.
2. Preserve ordering per declared key/action.
3. Track ambiguous delivery and idempotency keys; restart resumes unsettled effects.

**Acceptance:** Sink outage remains bounded; restart resends unsettled effects without deleting them early; permanent failures follow explicit terminal/DLQ policy; two dispatchers cannot claim conflicting ownership concurrently.

<a id="ticket-p03-t10"></a>

#### P03-T10 — Fencing and stop/update/delete races

**Owns:** core/reliability/generation.rs (new); manager lifecycle adapters. **Validation:** CROSS (Affected crates and integration tests). **Target:** `ticket_p03_t10`.

**Build after:** [P03-T01](#ticket-p03-t01), [P03-T04](#ticket-p03-t04). **Integrate after:** None.

**Implementation steps:**

1. Serialize generation changes with checkpoint/outbox commits.
2. Cancel old runtime ownership and persist its revocation before activating a replacement.
3. Ignore delayed old callbacks and fence claim/commit operations; distinguish in-flight external ambiguity.

**Acceptance:** Force a late snapshot writer after delete/update and prove it cannot recreate state; stale dispatcher cannot claim new effects; cancellation does not discard already durable pending effects.

<a id="ticket-p03-t11"></a>

#### P03-T11 — Snapshot hopping and sliding window state

**Owns:** server/engine/state/hop_slide.rs (new). **Validation:** SERVER (Server crate test). **Target:** `ticket_p03_t11`.

**Build after:** [C04](#ticket-c04), [P03-T05](#ticket-p03-t05). **Integrate after:** None.

**Implementation steps:**

1. Extract versioned hopping/sliding state including overlapping buckets, pending triggers, retained rows/accumulators, watermarks, and deadlines.
2. Preserve output order and boundary inclusivity; use existing evaluator helpers.

**Acceptance:** Restart at every trigger/window boundary equals uninterrupted output; overlapping membership is preserved; missed processing-time deadlines obey documented policy; corrupt payload fails before intake.

<a id="ticket-p03-t12"></a>

#### P03-T12 — Snapshot session and state windows

**Owns:** server/engine/state/session_state.rs (new). **Validation:** SERVER (Server crate test). **Target:** `ticket_p03_t12`.

**Build after:** [C04](#ticket-c04), [P03-T05](#ticket-p03-t05). **Integrate after:** None.

**Implementation steps:**

1. Capture per-key sessions, gap/max-duration boundaries, state-window conditions, pending timers, and all retained data.
2. Restore keys and deadlines before processing input; do not serialize Instant or re-evaluate historical nondeterministic predicates.

**Acceptance:** Open sessions survive restart; event at exact gap follows baseline boundary rule; expired deadlines follow chosen downtime policy; state-window transition output matches uninterrupted run.

<a id="ticket-p03-t13"></a>

#### P03-T13 — Snapshot joins and current CEP operators

**Owns:** server/engine/state/joins_cep.rs (new). **Validation:** SERVER (Server crate test). **Target:** `ticket_p03_t13`.

**Build after:** [C04](#ticket-c04), [P03-T05](#ticket-p03-t05). **Integrate after:** None.

**Implementation steps:**

1. Add snapshot adapters for existing join buffers/indexes and sequence operators.
2. Persist match progress, ordering keys, expiry, and input identity.
3. Rebuild derived indexes from stored state deterministically; do not serialize pointers.

**Acceptance:** Restart with partial sequence and unmatched join rows gives identical matches/multiplicity; expired state stays expired; plan/key-schema change is rejected without migration.

<a id="ticket-p03-t14"></a>

#### P03-T14 — Align multi-input barriers and async work

**Owns:** server/engine/barrier_alignment.rs (new). **Validation:** SERVER (Server crate test). **Target:** `ticket_p03_t14`.

**Build after:** [C03](#ticket-c03), [P03-T04](#ticket-p03-t04), [P02-T01](#ticket-p02-t01). **Integrate after:** None.

**Implementation steps:**

1. Align each checkpoint across participating input edges; bound paused-edge data and unblock on abort.
2. Drain outstanding lookups or snapshot recorded results before crossing.
3. Reject reliable feedback cycles in v1; propagate completion to every reachable sink.

**Acceptance:** Fast/slow/idle inputs cannot deadlock; timeout releases paused edges; no output crosses a barrier incorrectly; a stalled lookup prevents commit and cancels cleanly; memory stays within limits.

<a id="ticket-p03-t15"></a>

#### P03-T15 — Integrate complete qualified state participants

**Owns:** server/engine/{runtime,windows,dag}.rs. **Validation:** SERVER (Server crate test). **Target:** `ticket_p03_t15`.

**Build after:** [P03-T07](#ticket-p03-t07), [P03-T10](#ticket-p03-t10), [P03-T11](#ticket-p03-t11), [P03-T12](#ticket-p03-t12), [P03-T13](#ticket-p03-t13), [P03-T14](#ticket-p03-t14). **Integrate after:** [P04-T06](#ticket-p04-t06).

**Implementation steps:**

1. Register supported window/join/CEP participants and multi-input alignment.
2. Compute participant coverage during validation.
3. Integrate LEAD only after its state adapter; keep unqualified graph shapes rejected.
4. Preserve default lightweight path.

**Acceptance:** Each enabled shape passes snapshot-split equivalence and crash recovery; omission of one participant fails validation; barrier completion covers all actions; legacy execution fixtures stay unchanged.

<a id="ticket-p03-t16"></a>

#### P03-T16 — Add reliable RabbitMQ acknowledgement mode

**Owns:** conn/rabbitmq.rs; source acknowledgement adapter. **Validation:** SERVICE (Real-service qualification plus owning-crate tests). **Target:** `ticket_p03_t16`.

**Build after:** [P03-T02](#ticket-p03-t02), [P03-T08](#ticket-p03-t08), [P03-T09](#ticket-p03-t09). **Integrate after:** None.

**Implementation steps:**

1. Add explicit acknowledgement mode tied to durable state/outbox or confirmed final delivery.
2. Bound prefetch, handle nack/requeue and channel loss, and preserve current legacy admission mode.
3. Never acknowledge a poison message before durable chosen disposition.

**Acceptance:** Kill process before/after durable acceptance and verify redelivery/no missing input; reconnect does not ack stale delivery tags; DLQ failure prevents false completion; publisher confirm behavior is preserved.

<a id="ticket-p03-t17"></a>

#### P03-T17 — Implement Kafka group lifecycle adapter

**Owns:** conn/kafka_group.rs (new); optional client registration. **Validation:** SERVICE (Real-service qualification plus owning-crate tests). **Target:** `ticket_p03_t17`.

**Build after:** [C06](#ticket-c06), [P03-T02](#ticket-p03-t02), [P03-T10](#ticket-p03-t10). **Integrate after:** None.

**Implementation steps:**

1. Implement subscribe/assignment, partition epochs, pause/resume, rebalance revoke/drain, and contiguous commits against the group contract.
2. Keep direct-partition mode separate.
3. Pin client/version and protocol fixtures; commit only positions covered by recoverable state/effects.

**Acceptance:** Two consumers rebalance without simultaneous unfenced ownership; crash/rejoin resumes correct offsets; revoke while checkpointing cannot overcommit; groupId is no longer informational in this explicit mode.

<a id="ticket-p03-t18"></a>

#### P03-T18 — Implement savepoint/export/restore core

**Owns:** core/reliability/savepoint.rs (new). **Validation:** CORE (Core crate test). **Target:** `ticket_p03_t18`.

**Build after:** [P03-T01](#ticket-p03-t01), [P03-T10](#ticket-p03-t10). **Integrate after:** None.

**Implementation steps:**

1. Create immutable manifests referencing consistent catalog/state/schema/extension/effect versions.
2. Validate checksums and dependency availability before restore-to-new-rule.
3. Support explicit operator migration functions and retain source-position lineage.

**Acceptance:** Missing/corrupt participant or extension rejects restore before mutation; supported migration round trips; old savepoint remains readable; restored rule gets deliberate new identity without stealing backlog.

<a id="ticket-p03-t19"></a>

#### P03-T19 — Expose checkpoint and savepoint operations

**Owns:** server/handlers/recovery.rs (new); routes/state; cli/lib.rs. **Validation:** CROSS (Affected crates and integration tests). **Target:** `ticket_p03_t19`.

**Build after:** [P03-T15](#ticket-p03-t15), [P03-T18](#ticket-p03-t18), [P11-T03](#ticket-p11-t03), [P13-T02](#ticket-p13-t02). **Integrate after:** None.

**Implementation steps:**

1. Add authenticated inspect/checkpoint/savepoint/restore/drain operations with exact IDs and asynchronous status.
2. Validate permissions and capability before work.
3. Do not return success when only a background task was launched.

**Acceptance:** CLI/REST report the same lifecycle; failed save has a failed status; force-save result references the actual committed ID; unauthorized restore/replay fails; cancellation is explicit.

<a id="ticket-p03-t20"></a>

#### P03-T20 — Qualify durable storage and recovery failures

**Owns:** crates/rekuiper-server/tests/qualification_harness.rs; reliability fixtures. **Validation:** SERVICE (Real-service qualification plus owning-crate tests). **Target:** `ticket_p03_t20`.

**Build after:** [P03-T07](#ticket-p03-t07), [P03-T10](#ticket-p03-t10). **Integrate after:** None.

**Implementation steps:**

1. Qualify the single-source checkpoint path first: inject process termination at named commit/ack points, disk-full/I/O errors, corrupt snapshots, stale generation, and retention loss.
2. Verify input/output identities and restored state independently.
3. Separate process-crash results from host/power-loss claims.
4. Add outbox/segment/compaction failpoints when P03-T09/P03-T21 are integrated. Their evidence is mandatory for those capabilities, but their absence does not block checkpoint-only qualification.

**Acceptance:** Every failpoint has expected retained checkpoint/effects and duplicate boundary; no accepted durable input disappears; unknown durability settings cannot be advertised as qualified.

<a id="ticket-p03-t21"></a>

#### P03-T21 — Add segmented outbox backend only behind conformance

**Owns:** core/reliability/segmented_outbox.rs (new). **Validation:** CORE (Core crate test). **Target:** `ticket_p03_t21`.

**Build after:** [P03-T08](#ticket-p03-t08), [P03-T20](#ticket-p03-t20). **Integrate after:** None.

**Implementation steps:**

1. Implement the same outbox interface with checksummed append segments and a write-ahead manifest protocol.
2. Persist segment data before committing references; journal ack/compaction, retain recoverable old segments, and define torn-tail handling.
3. Keep SQLite v1 as fallback.

**Acceptance:** Crash at each segment/manifest/compaction step passes the same transaction oracle; orphan segments are safely reclaimed; missing payload fails visibly; measured workload justifies backend selection.

<a id="ticket-p03-t22"></a>

#### P03-T22 — Implement stateless transactional Kafka pipelines

**Owns:** conn/kafka_transactions.rs (new); reliable action adapter. **Validation:** SERVICE (Real-service qualification plus owning-crate tests). **Target:** `ticket_p03_t22`.

**Build after:** [P03-T17](#ticket-p03-t17), [P03-T15](#ticket-p03-t15). **Integrate after:** None.

**Implementation steps:**

1. Implement a deliberately stateless Kafka-to-Kafka mode: output records and consumed input offsets commit in one broker transaction using a fenced producer epoch.
2. Local SQLite is not an independent commit authority in this mode.
3. Reject stateful operators and external actions until a compatible transactional state backend is qualified.

**Acceptance:** Abort/crash/rebalance/fenced-producer tests expose only committed outputs with matching offsets; crash after broker commit cannot replay visible output as a new committed effect; unsupported stateful/external actions fail validation.

<a id="ticket-p03-t23"></a>

#### P03-T23 — Add Kafka-authoritative transactional state changelog

**Owns:** core/reliability/kafka_state.rs; Kafka transaction adapter (new). **Validation:** SERVICE (Real-service qualification plus owning-crate tests). **Target:** `ticket_p03_t23`.

**Build after:** [P03-T22](#ticket-p03-t22), [P03-T15](#ticket-p03-t15). **Integrate after:** None.

**Implementation steps:**

1. Persist versioned operator state/changelog and checkpoint identity in the same Kafka transaction as output and input offsets.
2. Restore from committed changelog; local state database is a rebuildable cache.
3. Bound transaction duration/state size and initially support one qualified input partition/operator set.

**Acceptance:** Crash before broker commit exposes no partial state/output; crash after commit before local cache update restores correct state/offsets; aborted changelog is ignored; unsupported participant state or timeout aborts without advancing.


### B10. P04 — Flagship B: ordered delayed analytics and streaming LEAD

<a id="ticket-p04-t01"></a>

#### P04-T01 — Parse and validate delayed LEAD syntax

**Owns:** sql/{ast,parser,validator}.rs. **Validation:** SQL (SQL crate test). **Target:** `ticket_p04_t01`.

**Build after:** [P01-T02](#ticket-p01-t02). **Integrate after:** None.

**Implementation steps:**

1. Implement A8 OVER WHEN/UNTIL/current_row grammar and argument restrictions.
2. Preserve bare/window LEAD compatibility; create a delayed-call plan representation without runtime evaluation changes.
3. Use stable call IDs derived from normalized plan position.

**Acceptance:** Valid A8 example parses; missing WHEN, illegal current_row placement, zero/nonliteral offset, invalid null mode, and nested delayed cycles return located errors; old syntax corpus passes.

<a id="ticket-p04-t02"></a>

#### P04-T02 — Implement origin/probe expression binding

**Owns:** sql/planning/delayed.rs (new). **Validation:** SQL (SQL crate test). **Target:** `ticket_p04_t02`.

**Build after:** [P04-T01](#ticket-p04-t01). **Integrate after:** None.

**Implementation steps:**

1. Bind ordinary fields to probe and current_row expressions to captured origin.
2. Resolve eager alias dependencies and post-resolution expressions; reject cycles.
3. Mark probe-sensitive predicates so optimizer cannot discard required probe rows.

**Acceptance:** Origin/probe with identical field names yields distinct correct values; eager alias works; delayed recursion fails; WHERE probe-visibility fixtures produce specified matches.

<a id="ticket-p04-t03"></a>

#### P04-T03 — Implement bounded pending-origin state machine

**Owns:** server/engine/lead/state.rs (new). **Validation:** SERVER (Server crate test). **Target:** `ticket_p04_t03`.

**Build after:** [P04-T02](#ticket-p04-t02), [P02-T01](#ticket-p02-t01). **Integrate after:** None.

**Implementation steps:**

1. Implement global origin FIFO and per-call/per-partition pending indexes.
2. Probe old origins before registering new; evaluate UNTIL first, then qualifying WHEN/null/offset.
3. Stage updates atomically and charge resolved-but-held rows to the cap.

**Acceptance:** Two partitions preserve global output order; UNTIL and WHEN true returns default; multiple calls wait together; cap fails without blocking future input; expression error does not partially mutate state.

<a id="ticket-p04-t04"></a>

#### P04-T04 — Handle delayed EOF, idleness, and watermarks

**Owns:** server/engine/lead/control.rs (new). **Validation:** SERVER (Server crate test). **Target:** `ticket_p04_t04`.

**Build after:** [P04-T03](#ticket-p04-t03), [C03](#ticket-c03). **Integrate after:** None.

**Implementation steps:**

1. Default unresolved values only on finite EOF, then forward EOF.
2. Keep idle partitions pending and stop distinct.
3. Compute a downstream watermark bound from held origins so progress cannot overtake future emitted output.

**Acceptance:** EOF flushes defaults in original order; idle produces no synthetic EOF; watermark stays behind held output and advances after release; cancellation releases resources without inventing completion.

<a id="ticket-p04-t05"></a>

#### P04-T05 — Wire LEAD into the real SQL task

**Owns:** server/engine/runtime.rs; delayed plan registration. **Validation:** SERVER (Server crate test). **Target:** `ticket_p04_t05`.

**Build after:** [P04-T03](#ticket-p04-t03), [P04-T04](#ticket-p04-t04), [P01-T03](#ticket-p01-t03). **Integrate after:** None.

**Implementation steps:**

1. Add the operator only for supported single-stream plans.
2. Evaluate projection after delayed values resolve, preserve origin metadata/time, and route errors through existing policy.
3. Explain shows memory/order limits; unsupported shapes fail validation.

**Acceptance:** Run A8 fixtures through normal ingress and actions; non-delayed throughput/outputs stay compatible; original row metadata survives; window/DAG delayed forms are explicitly rejected until supported.

<a id="ticket-p04-t06"></a>

#### P04-T06 — Snapshot and restore pending LEAD

**Owns:** server/engine/lead/snapshot.rs (new). **Validation:** SERVER (Server crate test). **Target:** `ticket_p04_t06`.

**Build after:** [P04-T03](#ticket-p04-t03), [C04](#ticket-c04). **Integrate after:** None.

**Implementation steps:**

1. Serialize call IDs, origin sequence/FIFO, remaining offsets, defaults, resolved-but-held values, and projection metadata.
2. Validate plan/call schema during restore and rebuild derived partition indexes before intake.

**Acceptance:** Snapshot split at every input matches uninterrupted outputs; unresolved and resolved-held rows both survive; wrong call ID/version fails; restoring at cap charges the same bytes.

<a id="ticket-p04-t07"></a>

#### P04-T07 — Qualify delayed analytics and durable integration

**Owns:** server delayed integration tests; qualification_harness. **Validation:** SERVER (Server crate test). **Target:** `ticket_p04_t07`.

**Build after:** [P04-T05](#ticket-p04-t05), [P04-T06](#ticket-p04-t06), [P03-T15](#ticket-p03-t15), [P06-T02](#ticket-p06-t02). **Integrate after:** None.

**Implementation steps:**

1. Run pinned eKuiper compatibility fixtures, production ordering/cap tests, and checkpoint/restart with pending LEAD.
2. Record which legacy LEAD forms retain old behavior and document any explicit migration mode.

**Acceptance:** Offsets/null modes/UNTIL precedence/aliases/EOF pass; no missing origin after restart; no unsupported mode is advertised; retained-memory benchmark includes head-of-line blocking.


### B11. P05 — Event-time progress and result modes

<a id="ticket-p05-t01"></a>

#### P05-T01 — Extract common watermark state machine

**Owns:** server/engine/time/watermark.rs (new). **Validation:** SERVER (Server crate test). **Target:** `ticket_p05_t01`.

**Build after:** [C03](#ticket-c03). **Integrate after:** None.

**Implementation steps:**

1. Represent per-edge/partition maximum time, unseen/closed status, and monotonic minimum progress.
2. Extract shared logic from windows without adding idle behavior yet.
3. Preserve WHERE-rejected row advancement.

**Acceptance:** Golden current-window fixtures remain identical; unseen active input holds progress; closed inputs stop holding it; watermark never regresses or overflows at timestamp boundaries.

<a id="ticket-p05-t02"></a>

#### P05-T02 — Implement idle and reactivation policy

**Owns:** server/engine/time/idleness.rs (new). **Validation:** SERVER (Server crate test). **Target:** `ticket_p05_t02`.

**Build after:** [P05-T01](#ticket-p05-t01). **Integrate after:** None.

**Implementation steps:**

1. Use injected processing-time deadlines to mark idle edges, remove only idle edges from minimum progress, and reactivate on input.
2. Route old reactivation events through late-data policy; do not interpret idleness as EOF.

**Acceptance:** Idle fast/slow/no-first-event cases have fixed results; reactivation cannot rewind progress; repeated idleness transitions do not duplicate window closure; deadlines survive restore.

<a id="ticket-p05-t03"></a>

#### P05-T03 — Define late-data and result-mode validation

**Owns:** sql/planning/time_policy.rs (new); engine time contracts. **Validation:** SQL (SQL crate test). **Target:** `ticket_p05_t03`.

**Build after:** [P01-T03](#ticket-p01-t03), [C01](#ticket-c01). **Integrate after:** None.

**Implementation steps:**

1. Keep lateTolerance meaning; introduce separately named grace/retention and final/upsert/retract requirements.
2. Validate correction-producing plans against sink modes.
3. Define late counted-drop and side-output envelopes without silently enabling corrections.

**Acceptance:** Old configs compile unchanged; grace cannot be negative; retract-to-append sink fails; every late disposition has a distinct diagnostic/metric category.

<a id="ticket-p05-t04"></a>

#### P05-T04 — Bound watermark alignment

**Owns:** server/engine/time/alignment.rs (new). **Validation:** SERVER (Server crate test). **Target:** `ticket_p05_t04`.

**Build after:** [P05-T01](#ticket-p05-t01), [P05-T02](#ticket-p05-t02), [P02-T01](#ticket-p02-t01). **Integrate after:** None.

**Implementation steps:**

1. Pause fast inputs based on configured event-time lead/state budget; release on slow progress, idle, cancellation, or alignment abort.
2. Integrate ordered controls without a separate overtaking channel.

**Acceptance:** One silent partition cannot deadlock stop; bounded fast-input buffering; aligned outputs match unaligned reference; idle/reactivation and barrier arrival races pass.

<a id="ticket-p05-t05"></a>

#### P05-T05 — Integrate time policies across windows and joins

**Owns:** server/engine/{windows,joins,dag}.rs. **Validation:** SERVER (Server crate test). **Target:** `ticket_p05_t05`.

**Build after:** [P05-T02](#ticket-p05-t02), [P05-T03](#ticket-p05-t03), [P05-T04](#ticket-p05-t04), [P03-T11](#ticket-p03-t11), [P03-T12](#ticket-p03-t12). **Integrate after:** None.

**Implementation steps:**

1. Replace repeated watermark logic with common policy and wire explicit late side output/drop counts.
2. Propagate time through async joins and held LEAD.
3. Enable correction modes only when P09 supplies their state/output semantics.

**Acceptance:** All window boundary/EOF fixtures remain compatible; late inputs reach exactly one disposition; shared source partitions are tracked separately; restart preserves policy and progress.

<a id="ticket-p05-t06"></a>

#### P05-T06 — Qualify time semantics and corrections

**Owns:** crates/rekuiper-server/tests temporal conformance. **Validation:** SERVER (Server crate test). **Target:** `ticket_p05_t06`.

**Build after:** [P05-T05](#ticket-p05-t05), [P06-T02](#ticket-p06-t02). **Integrate after:** None.

**Implementation steps:**

1. Run deterministic skew, idle, late, grace, EOF, timestamp precision, and restart scenarios for each enabled shape.
2. Qualify existing final/append windows independently. When P09-T06 supplies correction-producing execution, compare its update/retract results against a reference table; correction capability stays unqualified until those cases pass.

**Acceptance:** No final result changes after its declared closure; permitted corrections converge to reference state; unsupported result modes fail; memory expiry matches retained grace.


### B12. P06 — Production-equivalent tests, provenance, DLQ, and replay

<a id="ticket-p06-t01"></a>

#### P06-T01 — Implement production executor fixture harness

**Owns:** server/testing/fixture_runner.rs (new). **Validation:** SERVER (Server crate test). **Target:** `ticket_p06_t01`.

**Build after:** [C05](#ticket-c05), [C07](#ticket-c07). **Integrate after:** None.

**Implementation steps:**

1. Instantiate real planner/operators with synthetic source, TestClock, isolated state, fake lookup, and capture sink.
2. Expose step/run/assert APIs; prohibit live outbound effects in default test mode.
3. Avoid the evaluator-only shortcut for stateful plans.

**Acceptance:** Transform/window/DAG fixtures exercise production operators; clock advance deterministically triggers timers; fixture state cannot leak across cases; network attempts fail in default mode.

<a id="ticket-p06-t02"></a>

#### P06-T02 — Add CLI fixture test command and reports

**Owns:** cli/lib.rs; CLI fixture/report modules (new). **Validation:** CLI (CLI crate test). **Target:** `ticket_p06_t02`.

**Build after:** [P06-T01](#ticket-p06-t01). **Integrate after:** None.

**Implementation steps:**

1. Add proposed test command loading versioned fixture files, deterministic seed, selection, JSON/JUnit reports, and useful diffs.
2. Report executed/passed/failed/skipped counts; failure or zero matching requested tests returns nonzero.

**Acceptance:** A known failing fixture fails the process; selected zero cases fails; CLI and harness outputs match; reports include rule/operator and expected-versus-actual values.

<a id="ticket-p06-t03"></a>

#### P06-T03 — Unify server ruletest with real execution

**Owns:** server/handlers/ruletest.rs; Studio test adapters. **Validation:** SERVER (Server crate test). **Target:** `ticket_p06_t03`.

**Build after:** [P06-T01](#ticket-p06-t01). **Integrate after:** None.

**Implementation steps:**

1. Route supported ruletest sessions through the production harness, retaining existing SSE envelope/history behavior.
2. Clearly reject unsupported fixture shapes.
3. Reuse cancellation and isolated state; no live sink by default.

**Acceptance:** Existing SSE compatibility passes; window/DAG outcomes match production fixture runner; cancelling/disconnecting cleans tasks; repeated sessions share no mutable state.

<a id="ticket-p06-t04"></a>

#### P06-T04 — Bound and persist DLQ delivery

**Owns:** server/engine/dlq.rs; bounded dispatch adapter. **Validation:** SERVER (Server crate test). **Target:** `ticket_p06_t04`.

**Build after:** [P02-T01](#ticket-p02-t01). **Integrate after:** None.

**Implementation steps:**

1. Replace task-per-event dispatch with bounded workers and cancellation.
2. Keep legacy bounded history, drop/DLQ/halt, and no recursion.
3. Add versioned source/operator/rule/error envelopes and land bounded legacy dispatch independently. Reuse current retry behavior until P02-T06 is integrated.
4. Enable reliable DLQ only after P03-T08/P03-T09 provide durable enqueue/delivery and its failure tests pass. Until then reject reliable-DLQ configuration; bounded legacy dispatch does not wait for the outbox.

**Acceptance:** External DLQ outage stays within queue/task budgets; failure increments separate counters; no recursive dispatch; reliable mode cannot acknowledge poison input when durable DLQ enqueue fails.

<a id="ticket-p06-t05"></a>

#### P06-T05 — Implement bounded redacted provenance

**Owns:** server/engine/provenance.rs (new). **Validation:** SERVER (Server crate test). **Target:** `ticket_p06_t05`.

**Build after:** [C01](#ticket-c01), [P02-T01](#ticket-p02-t01). **Integrate after:** None.

**Implementation steps:**

1. Record optional event/branch/operator/effect lineage with sampling, byte/age caps, redaction, and permission hooks.
2. Keep tracing off inexpensive.
3. Store IDs and selected fields rather than full payloads by default.

**Acceptance:** Cap eviction is deterministic and counted; secret fields never appear; disabled tracing adds no payload clones; provenance links failed effect to its originating rule generation.

<a id="ticket-p06-t06"></a>

#### P06-T06 — Implement dry and controlled live replay

**Owns:** server/handlers/replay.rs (new); replay executor. **Validation:** SERVER (Server crate test). **Target:** `ticket_p06_t06`.

**Build after:** [P06-T01](#ticket-p06-t01), [P06-T05](#ticket-p06-t05), [P11-T03](#ticket-p11-t03), [P13-T02](#ticket-p13-t02). **Integrate after:** None.

**Implementation steps:**

1. Create replay jobs with original lineage, selected artifact version, target, rate limit, deduplication policy, and audit entry.
2. Default capture-only; live effects require authorized explicit mode.
3. Separate source reprocessing from effect retry.

**Acceptance:** Dry replay cannot write live actions; source and effect retry have distinct IDs; wrong version/permission fails before work; cancellation leaves consistent job status and retained provenance.

<a id="ticket-p06-t07"></a>

#### P06-T07 — Expose actionable health and pipeline metrics

**Owns:** server/handlers/{metrics,rules}.rs; existing status/explain adapters. **Validation:** SERVER (Server crate test). **Target:** `ticket_p06_t07`.

**Build after:** [C02](#ticket-c02), [P02-T01](#ticket-p02-t01). **Integrate after:** None.

**Implementation steps:**

1. Add bounded-label metrics as each producer becomes available: queue age/bytes and per-stage cost first, checkpoint/cursor metrics after P03-T07, and DLQ metrics after P06-T04. Absent producers must not delay existing metric families or falsely report healthy durability.
2. Define readiness based on selected required capabilities; keep liveness separate. Register pending-effect/retry/watermark families only with their actual producers and independently tested meanings.

**Acceptance:** A stalled sink is visible at its stage; secrets/event IDs never become labels; readiness changes only for configured critical failures; legacy metric names remain available.


### B13. P07 — A complete telemetry processing profile

<a id="ticket-p07-t01"></a>

#### P07-T01 — Define lossless telemetry value mapping

**Owns:** conn/telemetry/model.rs (new); otlp codec fixtures. **Validation:** CONN (Connector crate test). **Target:** `ticket_p07_t01`.

**Build after:** [C01](#ticket-c01). **Integrate after:** None.

**Implementation steps:**

1. Create typed resource/scope/signal mapping preserving timestamps, metric kind/unit/temporality/monotonicity, log body/severity, span links/status, and unknown supported attributes.
2. Inventory existing JSON OTLP fields; preserve legacy projections through adapters.

**Acceptance:** Round-trip reference JSON including integer extremes and nanoseconds; metric versus log versus trace remains distinguishable; declared unknown fields are either retained or explicitly rejected, never silently flattened.

<a id="ticket-p07-t02"></a>

#### P07-T02 — Implement stateful cumulative/delta conversion

**Owns:** conn/telemetry/metrics.rs (new). **Validation:** CONN (Connector crate test). **Target:** `ticket_p07_t02`.

**Build after:** [P07-T01](#ticket-p07-t01), [C04](#ticket-c04). **Integrate after:** None.

**Implementation steps:**

1. Implement cumulative/delta/rate transforms with per-series reset/start-time state, declared time units, and bounded retention.
2. Keep non-counter and incompatible temporality transformations explicit errors.
3. Snapshot series state through C04; do not mix series with different resource/scope identities.

**Acceptance:** Counter reset and out-of-order cases yield expected deltas/errors; restart at every sample matches uninterrupted output; expired series release memory; no floating conversion loses integer precision silently.

<a id="ticket-p07-t03"></a>

#### P07-T03 — Implement OTLP protobuf over HTTP

**Owns:** conn/telemetry/http.rs (new); existing OTLP routes. **Validation:** CONN (Connector crate test). **Target:** `ticket_p07_t03`.

**Build after:** [P07-T01](#ticket-p07-t01), [C06](#ticket-c06). **Integrate after:** None.

**Implementation steps:**

1. Add protobuf decode/encode, content-type handling, compression, request-size limits, and protocol-correct success/partial-success outcomes.
2. Reuse common telemetry mapping and existing JSON paths; bound decompressed size independently.

**Acceptance:** Reference sender/receiver payloads round-trip; malformed protobuf and compression bombs fail within limits; partial-success counts are not treated as full delivery; JSON compatibility tests remain unchanged.

<a id="ticket-p07-t04"></a>

#### P07-T04 — Implement OTLP gRPC transport

**Owns:** conn/telemetry/grpc.rs (new). **Validation:** SERVICE (Real-service qualification plus owning-crate tests). **Target:** `ticket_p07_t04`.

**Build after:** [P07-T01](#ticket-p07-t01), [C06](#ticket-c06). **Integrate after:** None.

**Implementation steps:**

1. Implement generated protocol service/client adapters using pinned schema definitions, deadlines, compression/size limits, TLS, and cancellation.
2. Map all three signals into the shared model; keep transport-specific errors out of signal semantics.

**Acceptance:** Reference client/server interoperability for metrics/logs/traces; deadline and cancellation release work; oversized/malformed requests reject correctly; retryable transport failures map to typed outcomes.

<a id="ticket-p07-t05"></a>

#### P07-T05 — Add telemetry cardinality and redaction budgets

**Owns:** conn/telemetry/policy.rs (new). **Validation:** CONN (Connector crate test). **Target:** `ticket_p07_t05`.

**Build after:** [P07-T01](#ticket-p07-t01), [P02-T01](#ticket-p02-t01). **Integrate after:** None.

**Implementation steps:**

1. Implement label allow/deny/redaction rules and bounded per-series tracking with deterministic overflow policy.
2. Apply policy before allocating new long-lived metric state; preserve resource/scope separation and required identifying fields.

**Acceptance:** Unbounded unique labels cannot grow state past configured limits; redacted secrets disappear from outputs/errors; overflow is counted; existing series continue according to documented policy.

<a id="ticket-p07-t06"></a>

#### P07-T06 — Wire telemetry processing profile

**Owns:** server/engine/telemetry.rs (new); metadata/schema registration. **Validation:** SERVER (Server crate test). **Target:** `ticket_p07_t06`.

**Build after:** [C02](#ticket-c02), [P07-T01](#ticket-p07-t01), [P02-T05](#ticket-p02-t05). **Integrate after:** [P03-T09](#ticket-p03-t09).

**Implementation steps:**

1. Register a telemetry profile that consumes independently available signal/transport/transform adapters from the capability registry.
2. Retain current JSON support.
3. Wire batching/scheduling and optional reliable outbox per adapter; missing gRPC or histogram transforms must not block a qualified JSON profile.

**Acceptance:** Enabled adapters use real production mapping; absent adapters are rejected individually; existing JSON works before other transports land; each reliable transport requires its outbox qualification.

<a id="ticket-p07-t07"></a>

#### P07-T07 — Qualify telemetry round trips and outages

**Owns:** telemetry service fixtures; server qualification harness. **Validation:** SERVICE (Real-service qualification plus owning-crate tests). **Target:** `ticket_p07_t07`.

**Build after:** [P07-T06](#ticket-p07-t06). **Integrate after:** None.

**Implementation steps:**

1. Use reference senders/receivers for JSON/protobuf HTTP/gRPC and all declared signal shapes.
2. Compare independent decoded outputs, not only success counters.
3. Exercise cumulative resets, histograms, compression, retry/partial success, restart, and schema changes.
4. Run each available adapter independently; reliable-profile claims additionally require P03-T20 evidence, while lightweight JSON qualification can proceed earlier.

**Acceptance:** No declared field or timestamp precision is lost; counter/histogram oracle passes; outage recovery meets chosen contract; untested transport/signal combinations remain unqualified.

<a id="ticket-p07-t08"></a>

#### P07-T08 — Implement histogram and exemplar preservation

**Owns:** conn/telemetry/histogram.rs (new). **Validation:** CONN (Connector crate test). **Target:** `ticket_p07_t08`.

**Build after:** [P07-T01](#ticket-p07-t01). **Integrate after:** None.

**Implementation steps:**

1. Implement explicit and exponential histogram mapping, bucket/count/sum validation, exemplars, scale conversion rules, and supported aggregation compatibility.
2. Preserve unknown-but-supported attributes; reject incompatible bucket merges rather than coerce.

**Acceptance:** Pinned histogram/exemplar fixtures round-trip; bucket counts equal declared totals; incompatible scale/boundaries reject or follow an exact documented conversion; timestamp and trace linkage survive.


### B14. P08 — Industrial alerts and a general temporal rule model

<a id="ticket-p08-t01"></a>

#### P08-T01 — Implement keyed alert lifecycle

**Owns:** server/engine/alerts/lifecycle.rs (new). **Validation:** SERVER (Server crate test). **Target:** `ticket_p08_t01`.

**Build after:** [C03](#ticket-c03), [C04](#ticket-c04). **Integrate after:** None.

**Implementation steps:**

1. Define Normal/Pending/Firing/Acknowledged/Recovered state and explicit transition inputs.
2. Separate detection state from notification attempts; use stable alert occurrence IDs.
3. Support one keyed rule with no network effects.

**Acceptance:** Repeated true inputs do not invent new occurrences; acknowledgement does not clear the condition; recovery links to the firing occurrence; sink errors cannot change detection state.

<a id="ticket-p08-t02"></a>

#### P08-T02 — Implement deadband, sustain, debounce, and cooldown

**Owns:** server/engine/alerts/conditions.rs (new). **Validation:** SERVER (Server crate test). **Target:** `ticket_p08_t02`.

**Build after:** [P08-T01](#ticket-p08-t01). **Integrate after:** None.

**Implementation steps:**

1. Implement threshold enter/exit bands, sustained-duration entry, debounce, and cooldown using injected time.
2. First map equivalent existing SQL functions; reuse them where their behavior is exact.
3. Define inclusive/exclusive threshold boundaries.

**Acceptance:** Chattering input yields the specified transition count; exact threshold/time boundaries are deterministic; null/missing observations follow explicit error/ignore policy; independent keys cannot affect each other.

<a id="ticket-p08-t03"></a>

#### P08-T03 — Implement keyed absence and temporal timers

**Owns:** server/engine/alerts/absence.rs (new). **Validation:** SERVER (Server crate test). **Target:** `ticket_p08_t03`.

**Build after:** [P08-T01](#ticket-p08-t01), [P02-T01](#ticket-p02-t01). **Integrate after:** None.

**Implementation steps:**

1. Extend watchdog semantics to per-key expected observations with bounded key state and expiry.
2. Keep event-time and processing-time absence separate; define first-observation/startup behavior, repeat reminders, and late recovery.

**Acceptance:** A never-seen configured key and an inactive observed key follow distinct documented rules; expiry frees budget; late events cannot rewind time; restart fixtures preserve due timers.

<a id="ticket-p08-t04"></a>

#### P08-T04 — Implement bounded sequence and negative patterns

**Owns:** server/engine/cep/pattern.rs (new); planner pattern descriptors. **Validation:** SERVER (Server crate test). **Target:** `ticket_p08_t04`.

**Build after:** [C03](#ticket-c03), [C04](#ticket-c04), [P02-T01](#ticket-p02-t01). **Integrate after:** None.

**Implementation steps:**

1. Add explicit sequence/repetition/bounded-until/negative-condition state machines.
2. Freeze overlap and consumption modes in descriptors; reuse existing sequence helpers where equivalent.
3. Cap partial matches and explain rejection when the cap is exceeded.

**Acceptance:** A-B-without-C fixtures cover overlaps and time boundaries; no-match streams stay bounded; late events follow declared policy; duplicate observation does not create unintended duplicate match identity.

<a id="ticket-p08-t05"></a>

#### P08-T05 — Implement suppression, inhibition, and reminders

**Owns:** server/engine/alerts/notifications.rs (new). **Validation:** SERVER (Server crate test). **Target:** `ticket_p08_t05`.

**Build after:** [P08-T01](#ticket-p08-t01), [P08-T02](#ticket-p08-t02). **Integrate after:** None.

**Implementation steps:**

1. Derive notification intents from detection transitions, inhibition dependencies, state-change-only mode, and reminder deadlines.
2. Detect dependency cycles.
3. Assign deterministic effect IDs; notification history is separate from detection state.

**Acceptance:** Parent-inhibited child emits no unwanted notification but retains detection; recovery is delivered according to contract; reminders do not duplicate on repeated ticks; cyclic inhibition rejects at validation.

<a id="ticket-p08-t06"></a>

#### P08-T06 — Persist and integrate alert/CEP state

**Owns:** server/engine/alerts/snapshot.rs; runtime/DAG adapters. **Validation:** SERVER (Server crate test). **Target:** `ticket_p08_t06`.

**Build after:** [P08-T02](#ticket-p08-t02), [P08-T03](#ticket-p08-t03), [P08-T04](#ticket-p08-t04), [P08-T05](#ticket-p08-t05), [P03-T15](#ticket-p03-t15). **Integrate after:** None.

**Implementation steps:**

1. Implement versioned state/timer snapshots and wire SQL/DAG authoring to the same operators.
2. Send notification intents through effect/outbox paths; preserve current watchdog behavior for legacy configs.
3. Explain state transitions and memory limits.

**Acceptance:** Restart before/after firing/reminder/recovery matches uninterrupted results; outbox outage does not change detection; unsupported temporal modes fail; existing watchdog tests pass.

<a id="ticket-p08-t07"></a>

#### P08-T07 — Ship industrial packages and conformance fixtures

**Owns:** docs/examples/alerts/ (new); Studio metadata; alert tests. **Validation:** CROSS (Affected crates and integration tests). **Target:** `ticket_p08_t07`.

**Build after:** [P08-T06](#ticket-p08-t06), [P06-T02](#ticket-p06-t02). **Integrate after:** None.

**Implementation steps:**

1. Package typed parameterized examples for deadband, sustained alarm, missing device, correlated failure, inhibition, and recovery.
2. Include fixtures, resource defaults, and generated graph/SQL mappings.
3. Do not embed delivery credentials.

**Acceptance:** Each example passes CLI production fixtures; invalid parameters reject; SQL and graph variants produce equal transitions; fleet-scale key-cardinality and flapping tests remain bounded.


### B15. P09 — Changelogs, CDC, maintained relations, and reusable state

<a id="ticket-p09-t01"></a>

#### P09-T01 — Implement change-event model and validation

**Owns:** core/relations/change.rs (new). **Validation:** CORE (Core crate test). **Target:** `ticket_p09_t01`.

**Build after:** [C01](#ticket-c01). **Integrate after:** None.

**Implementation steps:**

1. Define versioned key/Insert/Update/Delete/before/after/source-transaction-position/time envelope.
2. Keep append-only specialization.
3. Reject contradictory operations and distinguish duplicate source position from equal payload.

**Acceptance:** Insert/update/delete golden fixtures round-trip; null-key policy is explicit; duplicate identity is recognized without discarding equal-payload distinct events; malformed before/after pairs reject.

<a id="ticket-p09-t02"></a>

#### P09-T02 — Decode standard CDC envelopes

**Owns:** conn/cdc/decode.rs (new). **Validation:** CONN (Connector crate test). **Target:** `ticket_p09_t02`.

**Build after:** [P09-T01](#ticket-p09-t01). **Integrate after:** None.

**Implementation steps:**

1. Implement a declared version/subset of CDC envelope formats using pinned fixtures, including snapshot markers, transaction metadata, deletes/tombstones, and schema identity.
2. Add a capability descriptor; unknown formats fail rather than guess.

**Acceptance:** Captured sanitized fixtures map to exact changes/positions; tombstones do not become empty inserts; transaction order preserved; unsupported envelope versions are explicit errors.

<a id="ticket-p09-t03"></a>

#### P09-T03 — Maintain snapshot-consistent lookup relations

**Owns:** core/relations/store.rs (new). **Validation:** CORE (Core crate test). **Target:** `ticket_p09_t03`.

**Build after:** [P09-T01](#ticket-p09-t01), [C04](#ticket-c04). **Integrate after:** None.

**Implementation steps:**

1. Apply snapshot rows then log changes under explicit snapshot boundary/position rules.
2. Deduplicate by source identity, maintain indexed current state, and expose snapshot-consistent reads.
3. Persist state and source boundary atomically in reliable mode.

**Acceptance:** Concurrent snapshot/log fixture equals reference table; delete/update/dedup semantics hold; reader never sees half-applied transaction; restore resumes without losing the handover boundary.

<a id="ticket-p09-t04"></a>

#### P09-T04 — Implement temporal lookup and interval joins

**Owns:** server/engine/relations/temporal_join.rs (new). **Validation:** SERVER (Server crate test). **Target:** `ticket_p09_t04`.

**Build after:** [P09-T03](#ticket-p09-t03), [P05-T01](#ticket-p05-t01). **Integrate after:** None.

**Implementation steps:**

1. Retain versioned dimension rows with valid-time ranges and bounded retention.
2. Distinguish latest-value lookup from event-time lookup; implement interval membership with documented endpoints and outer/no-match output.
3. Reuse existing ASOF path where equivalent.

**Acceptance:** Historical fact joins to historical dimension, not latest value; exact boundaries and overlapping intervals are deterministic; expiry honors watermark; unmatched/duplicate rows preserve expected cardinality.

<a id="ticket-p09-t05"></a>

#### P09-T05 — Implement incremental count and sum

**Owns:** core/relations/aggregate.rs (new). **Validation:** CORE (Core crate test). **Target:** `ticket_p09_t05`.

**Build after:** [P09-T01](#ticket-p09-t01), [P09-T03](#ticket-p09-t03). **Integrate after:** None.

**Implementation steps:**

1. Maintain grouped count/sum under insert/update/delete with exact existing null/numeric semantics, stable key identity, and state budgets.
2. Apply transaction batches atomically; expose a reusable accumulator interface for later aggregates.

**Acceptance:** Randomized changes equal full recomputation after every transaction; last-member removal follows contract; duplicate source identity does not double-apply; numeric boundaries and nulls match current evaluator.

<a id="ticket-p09-t06"></a>

#### P09-T06 — Implement incremental joins and result changes

**Owns:** server/engine/relations/incremental_join.rs (new). **Validation:** SERVER (Server crate test). **Target:** `ticket_p09_t06`.

**Build after:** [P09-T03](#ticket-p09-t03), [P09-T05](#ticket-p09-t05), [P10-T02](#ticket-p10-t02). **Integrate after:** None.

**Implementation steps:**

1. Maintain keyed join indexes and emit inserts/deletes/updates with stable result identity.
2. Preserve multiplicity and outer-join unmatched transitions.
3. Expose final/upsert/retract compatibility to P05; keep append-only path specialized.

**Acceptance:** Full recomputation oracle matches after each side update/delete; outer match creation retracts prior unmatched row; skew and duplicate keys stay bounded by explicit budgets; result sink capability is checked.

<a id="ticket-p09-t07"></a>

#### P09-T07 — Expose named maintained views and subscriptions

**Owns:** server/handlers/views.rs (new); relation runtime. **Validation:** SERVER (Server crate test). **Target:** `ticket_p09_t07`.

**Build after:** [P09-T06](#ticket-p09-t06), [P11-T03](#ticket-p11-t03). **Integrate after:** None.

**Implementation steps:**

1. Register versioned named views with dependency graph, consistent reads, and resume tokens carrying a completeness frontier.
2. Validate token/version compatibility and retain subscription history within explicit limits.

**Acceptance:** Reader sees a consistent committed frontier; reconnect resumes without silent gap within retention; expired token returns a clear error; view dependency cycle is rejected.

<a id="ticket-p09-t08"></a>

#### P09-T08 — Wire transactional SQL upsert/delete batches

**Owns:** conn/sql.rs; server/engine/sinks.rs SQL adapter. **Validation:** SERVICE (Real-service qualification plus owning-crate tests). **Target:** `ticket_p09_t08`.

**Build after:** [P09-T01](#ticket-p09-t01), [P02-T05](#ticket-p02-t05), [P03-T08](#ticket-p03-t08). **Integrate after:** None.

**Implementation steps:**

1. Implement real engine calls for atomic batches of insert/update/delete using bound values and validated identifiers.
2. Define stable key mapping and ambiguous commit recovery.
3. Preserve legacy insert-only mode; do not claim helper availability as integration.

**Acceptance:** Force failure on middle row and prove rollback; update/delete affect intended keys only; crash after commit follows dedup/ambiguity policy; production rule path invokes the transaction API.

<a id="ticket-p09-t09"></a>

#### P09-T09 — Qualify CDC and maintained relations

**Owns:** CDC service fixtures; relation qualification harness. **Validation:** SERVICE (Real-service qualification plus owning-crate tests). **Target:** `ticket_p09_t09`.

**Build after:** [P09-T02](#ticket-p09-t02), [P09-T04](#ticket-p09-t04), [P09-T07](#ticket-p09-t07), [P09-T08](#ticket-p09-t08), [P03-T20](#ticket-p03-t20). **Integrate after:** None.

**Implementation steps:**

1. Run snapshot-plus-concurrent-log, duplicate/reordered protocol-valid delivery, schema change, restart, and sink failure scenarios.
2. Use independent database snapshots/full recomputation as the oracle; record source adapter limits.

**Acceptance:** State and emitted changes converge to oracle with no missing transaction; incompatible schema stops visibly; restored subscriptions remain valid or fail explicitly; temporal history retention is measurable.

<a id="ticket-p09-t10"></a>

#### P09-T10 — Implement incremental avg/min/max state

**Owns:** core/relations/aggregate_ordered.rs (new). **Validation:** CORE (Core crate test). **Target:** `ticket_p09_t10`.

**Build after:** [P09-T05](#ticket-p09-t05). **Integrate after:** None.

**Implementation steps:**

1. Build average from count/sum and min/max from a bounded value multiset preserving current equality/order semantics.
2. Handle duplicate counts and key removal; persist versioned state and derived-index rebuild rules.

**Acceptance:** Insert/update/delete differential tests match evaluator; deleting one duplicate preserves remaining extremum; empty groups behave correctly; restart reconstructs multiset and average exactly.

<a id="ticket-p09-t11"></a>

#### P09-T11 — Implement statistical aggregate retractions

**Owns:** core/relations/aggregate_stats.rs (new). **Validation:** CORE (Core crate test). **Target:** `ticket_p09_t11`.

**Build after:** [P09-T05](#ticket-p09-t05). **Integrate after:** None.

**Implementation steps:**

1. Add declared variance/statistical functions using a numerically validated inverse or bounded retained-sample recomputation.
2. For sketches lacking exact deletion, declare approximation/error semantics or use recomputation; never pretend an insertion-only sketch supports exact retract.

**Acceptance:** Randomized updates/deletes match documented precision/error bounds against full recomputation; degenerate counts and repeated equal values remain stable; resource cap rejects unsupported retention.

<a id="ticket-p09-t12"></a>

#### P09-T12 — Add generic retained-state aggregate fallback

**Owns:** core/relations/aggregate_fallback.rs (new). **Validation:** CORE (Core crate test). **Target:** `ticket_p09_t12`.

**Build after:** [P09-T05](#ticket-p09-t05), [P01-T02](#ticket-p01-t02). **Integrate after:** None.

**Implementation steps:**

1. Support additional registered aggregates by bounded retained inputs and recomputation when no safe incremental inverse exists.
2. Declare per-function changelog capability and state cost; keep unsupported/unbounded cases rejected.

**Acceptance:** Supported function corpus matches full recomputation; deleting/updating rows cannot leave stale aggregate values; cap produces explicit error; an unqualified function cannot advertise retract support.

<a id="ticket-p09-t13"></a>

#### P09-T13 — Implement native PostgreSQL CDC snapshot boundary

**Owns:** conn/cdc/postgres_snapshot.rs (new). **Validation:** SERVICE (Real-service qualification plus owning-crate tests). **Target:** `ticket_p09_t13`.

**Build after:** [P09-T01](#ticket-p09-t01), [C06](#ticket-c06), [C04](#ticket-c04). **Integrate after:** None.

**Implementation steps:**

1. Create a consistent initial snapshot tied to a logical replication start position using the pinned supported server/client protocol.
2. Track tables/schema/version and snapshot progress; retain a resumable boundary and explicit replication-slot ownership/cleanup policy.

**Acceptance:** Concurrent updates during snapshot are neither lost nor double-applied after log handover; restart resumes or explicitly restarts snapshot; permissions/slot limits fail visibly; unused slot cleanup is ownership-safe.

<a id="ticket-p09-t14"></a>

#### P09-T14 — Implement native PostgreSQL logical change reader

**Owns:** conn/cdc/postgres_log.rs (new). **Validation:** SERVICE (Real-service qualification plus owning-crate tests). **Target:** `ticket_p09_t14`.

**Build after:** [P09-T01](#ticket-p09-t01), [C06](#ticket-c06), [P03-T02](#ticket-p03-t02). **Integrate after:** None.

**Implementation steps:**

1. Decode the selected logical replication protocol into change/transaction envelopes, preserving LSN, transaction boundaries, schema changes, tombstones and value types.
2. Advance acknowledged LSN only after durable rule acceptance; bound WAL backlog and expose retention pressure.

**Acceptance:** Insert/update/delete transaction fixtures retain exact order; reconnect resumes committed LSN; unsupported type/schema change stops visibly; no early acknowledgement discards needed WAL.

<a id="ticket-p09-t15"></a>

#### P09-T15 — Integrate native CDC snapshot-to-log handover

**Owns:** conn/cdc/postgres.rs (new); source capability registration. **Validation:** SERVICE (Real-service qualification plus owning-crate tests). **Target:** `ticket_p09_t15`.

**Build after:** [P09-T13](#ticket-p09-t13), [P09-T14](#ticket-p09-t14), [P09-T03](#ticket-p09-t03), [P03-T07](#ticket-p03-t07). **Integrate after:** None.

**Implementation steps:**

1. Wire snapshot and buffered log reader at the recorded consistent boundary, deduplicate overlapping positions, and expose qualified snapshot/streaming phases.
2. Use production reliable ingress and table state; preserve external-envelope CDC adapter as another source.

**Acceptance:** Real database concurrent-write oracle matches final maintained relation; crash at handover restores correctly; oversized lag follows explicit failure/backpressure policy; source cannot advertise reliability before all tests pass.


### B16. P10 — Optimizer and performance engineering

<a id="ticket-p10-t01"></a>

#### P10-T01 — Establish fair optimizer baseline workloads

**Owns:** test/benchmark/ existing harness; join fixtures. **Validation:** CROSS (Affected crates and integration tests). **Target:** `ticket_p10_t01`.

**Build after:** None. **Integrate after:** None.

**Implementation steps:**

1. Add deterministic small/large/skewed join, fanout, typed/wide records, and durable workloads.
2. Record observed output identities/cardinality, tail latency, CPU/RSS/state bytes, and drain.
3. Pin configs and guarantees, not only executable versions.

**Acceptance:** Baseline runs are reproducible; independent output counts reconcile; repeated runs include variance; microbenchmarks are labelled separately from network-to-sink measurements.

<a id="ticket-p10-t02"></a>

#### P10-T02 — Implement canonical equijoin key extraction

**Owns:** sql/planning/join_key.rs (new). **Validation:** SQL (SQL crate test). **Target:** `ticket_p10_t02`.

**Build after:** [P01-T01](#ticket-p01-t01). **Integrate after:** None.

**Implementation steps:**

1. Compile simple equijoin key extractors using existing numeric/null equality semantics.
2. Produce hashable normalized candidates plus exact residual/equality recheck.
3. Mark unsupported keys/predicates for fallback; never change SQL equality to suit hashing.

**Acceptance:** Mixed integer/float, null, nested/unhashable, duplicates, and composite key fixtures match existing evaluator equality; unequal values cannot become false-positive output after recheck.

<a id="ticket-p10-t03"></a>

#### P10-T03 — Implement bounded ordered hash join

**Owns:** server/engine/joins/hash.rs (new). **Validation:** SERVER (Server crate test). **Target:** `ticket_p10_t03`.

**Build after:** [P10-T02](#ticket-p10-t02), [P02-T01](#ticket-p02-t01). **Integrate after:** None.

**Implementation steps:**

1. Index candidate-side row positions in stable order; probe preserving multiplicity and required output ordering.
2. Support qualified outer behavior and residual predicates.
3. Fall back on tiny/non-equi/unhashable/over-budget cases.

**Acceptance:** Differential results equal current nested loop over randomized/skewed fixtures; outer unmatched rows remain correct; budget failure uses safe fallback; benchmark identifies crossover before enabling selection.

<a id="ticket-p10-t04"></a>

#### P10-T04 — Compile and reuse safe expression work

**Owns:** sql/planning/compiled_expr.rs (new). **Validation:** SQL (SQL crate test). **Target:** `ticket_p10_t04`.

**Build after:** [P01-T02](#ticket-p01-t02). **Integrate after:** None.

**Implementation steps:**

1. Compile repeated field/key access and immutable pure expressions once.
2. Cache by semantic plan/schema/extension identity.
3. Exclude nondeterministic/side-effect/stateful expressions from unsafe reuse; retain generic evaluator fallback.

**Acceptance:** Differential corpus matches generic evaluator; schema/extension change invalidates cache; repeated stateful function calls retain documented evaluation count; allocation benchmark demonstrates actual benefit.

<a id="ticket-p10-t05"></a>

#### P10-T05 — Implement semantics-aware pushdown

**Owns:** sql/planning/optimizer.rs (new). **Validation:** SQL (SQL crate test). **Target:** `ticket_p10_t05`.

**Build after:** [P01-T03](#ticket-p01-t03), [P04-T02](#ticket-p04-t02), [P05-T01](#ticket-p05-t01). **Integrate after:** None.

**Implementation steps:**

1. Push safe predicates/projections only when state/probe visibility, event-time advancement, outer joins, and error behavior are preserved.
2. Make each transformation independently explainable and disableable for diagnosis.

**Acceptance:** Optimized and unoptimized fixture results/errors match; LEAD probes and watermark advancement remain intact; no projection removes required metadata/state key; invalid rewrite fails a differential test.

<a id="ticket-p10-t06"></a>

#### P10-T06 — Select and explain physical strategies

**Owns:** sql/planning/cost.rs (new); explain adapter. **Validation:** SQL (SQL crate test). **Target:** `ticket_p10_t06`.

**Build after:** [P10-T01](#ticket-p10-t01), [P10-T03](#ticket-p10-t03), [P10-T04](#ticket-p10-t04), [P10-T05](#ticket-p10-t05). **Integrate after:** None.

**Implementation steps:**

1. Choose nested/hash/indexed strategies using measured thresholds, cardinality/skew hints, and memory budgets.
2. Explain estimates, selected path, and fallback reason.
3. Preserve existing ASOF indexing and allow safe explicit diagnostic override.

**Acceptance:** Small-input regression stays within recorded budget; skew/over-budget plans choose safe path; explain reflects actual executed strategy; no unsupported hint silently changes semantics.

<a id="ticket-p10-t07"></a>

#### P10-T07 — Share compatible scans and indexed arrangements

**Owns:** server/engine/plans/shared.rs (new). **Validation:** SERVER (Server crate test). **Target:** `ticket_p10_t07`.

**Build after:** [P09-T06](#ticket-p09-t06), [P10-T06](#ticket-p10-t06), [P11-T03](#ticket-p11-t03). **Integrate after:** None.

**Implementation steps:**

1. Share immutable scans/index arrangements only across equal plan/schema/security identities with reference-counted lifecycle and charged state ownership.
2. Keep mutable operator state separate unless its sharing contract is explicitly supported.

**Acceptance:** Removing one consumer preserves another's state; unauthorized namespaces never share data; output equality holds; measured reuse improves cost without hiding shared memory charges.

<a id="ticket-p10-t08"></a>

#### P10-T08 — Add measured typed batch execution

**Owns:** server/engine/plans/batch.rs (new). **Validation:** SERVER (Server crate test). **Target:** `ticket_p10_t08`.

**Build after:** [P10-T04](#ticket-p10-t04), [P10-T06](#ticket-p10-t06), [P02-T05](#ticket-p02-t05). **Integrate after:** None.

**Implementation steps:**

1. Implement one typed projection/filter batch path with scalar fallback, then expose it as a selectable physical strategy.
2. Preserve error/order/stateful-function semantics; enable only for qualified operator subsets and measured crossover.

**Acceptance:** Scalar/batch differential corpus matches rows and errors; small input can retain scalar path; batch size stays within byte/count budgets; benchmark documents throughput and tail-latency tradeoff.


### B17. P11 — Versioned catalog, atomic activation, and safe upgrades

<a id="ticket-p11-t01"></a>

#### P11-T01 — Implement named action identities and cache ownership

**Owns:** server/engine/sinks.rs; server/sink_cache.rs; core identity adapter. **Validation:** CROSS (Affected crates and integration tests). **Target:** `ticket_p11_t01`.

**Build after:** [C01](#ticket-c01). **Integrate after:** None.

**Implementation steps:**

1. Implement A6 optional inner name validation and stable action identity.
2. Preserve unnamed identifiers.
3. Write versioned backlog ownership manifests excluding credentials; destination changes/renames require explicit new ownership or stopped migration.

**Acceptance:** Two REST actions reorder without swapping backlogs; duplicate names fail; credential rotation retains destination ownership; unknown legacy cache is not silently adopted by a different target.

<a id="ticket-p11-t02"></a>

#### P11-T02 — Implement immutable catalog generations

**Owns:** core/catalog/{model,store}.rs (new). **Validation:** CORE (Core crate test). **Target:** `ticket_p11_t02`.

**Build after:** [C01](#ticket-c01), [P03-T01](#ticket-p03-t01). **Integrate after:** None.

**Implementation steps:**

1. Version rules/streams/tables/views/schemas/connections/extensions/actions and dependency edges.
2. Commit a candidate catalog generation atomically, retain previous versions, and distinguish object identity from display name.
3. Use reliable store transaction semantics.

**Acceptance:** Concurrent reader observes one generation; dependency cycle/missing version fails; rename preserves ID; failed transaction leaves prior catalog unchanged; old generation is readable for rollback.

<a id="ticket-p11-t03"></a>

#### P11-T03 — Compile complete candidate generations

**Owns:** server/deployment/candidate.rs (new). **Validation:** SERVER (Server crate test). **Target:** `ticket_p11_t03`.

**Build after:** [P11-T02](#ticket-p11-t02), [P01-T03](#ticket-p01-t03). **Integrate after:** None.

**Implementation steps:**

1. Resolve one immutable catalog snapshot, schemas/extensions, capabilities, secret references, fixtures, and resource reservations into a candidate plan.
2. Produce a deterministic digest and explicit migration/diff report; do not activate tasks here.

**Acceptance:** Same semantic inputs yield same digest; changed schema/extension changes digest; unsupported migration/resource overcommit fails before live mutation; secrets do not enter exported artifact.

<a id="ticket-p11-t04"></a>

#### P11-T04 — Implement recoverable runtime activation

**Owns:** server/deployment/activate.rs (new); declarative adapter. **Validation:** SERVER (Server crate test). **Target:** `ticket_p11_t04`.

**Build after:** [P11-T03](#ticket-p11-t03), [P03-T10](#ticket-p03-t10), [P03-T18](#ticket-p03-t18). **Integrate after:** None.

**Implementation steps:**

1. Persist activation stages; drain/checkpoint old generation, fence owners, atomically publish catalog, and activate replacement.
2. Resume or rollback after crash without mixing readers/writers.
3. Preserve unchanged healthy rules.

**Acceptance:** Crash at every activation stage leaves recoverable old/new ownership; stale generation cannot commit; failed replacement does not destroy pending effects; old external effects are not pretended undone.

<a id="ticket-p11-t05"></a>

#### P11-T05 — Implement artifact bundles and ownership rules

**Owns:** core/catalog/bundle.rs (new). **Validation:** CORE (Core crate test). **Target:** `ticket_p11_t05`.

**Build after:** [P11-T02](#ticket-p11-t02), [C05](#ticket-c05). **Integrate after:** None.

**Implementation steps:**

1. Package rules/schemas/fixtures/extension digests with manifest version and optional signature verification.
2. Keep secrets as references.
3. Define GitOps versus API ownership conflict detection; reject ambiguous concurrent control rather than overwrite silently.

**Acceptance:** Tampered artifact fails verification when required; missing dependency fails; deterministic bundle digest; API cannot mutate GitOps-owned object without explicit ownership transfer.

<a id="ticket-p11-t06"></a>

#### P11-T06 — Extend fleet rollout with generation compatibility

**Owns:** server/fleet.rs; deployment status adapter. **Validation:** SERVER (Server crate test). **Target:** `ticket_p11_t06`.

**Build after:** [P11-T04](#ticket-p11-t04), [P11-T05](#ticket-p11-t05), [P13-T02](#ticket-p13-t02). **Integrate after:** None.

**Implementation steps:**

1. Add capability/version preflight, selected canary group, state migration checks, health-window evaluation, and generation-aware rollback to existing fleet flow.
2. Keep per-device outcome and recovery instructions; respect disconnected nodes.

**Acceptance:** Unsupported device is excluded with reason; failed canary stops later rollout; reconnect reconciles intended generation without duplicate ownership; rollback checks state compatibility before activation.

<a id="ticket-p11-t07"></a>

#### P11-T07 — Qualify upgrades, rollback, and backup restore

**Owns:** server deployment/restore qualification fixtures. **Validation:** SERVICE (Real-service qualification plus owning-crate tests). **Target:** `ticket_p11_t07`.

**Build after:** [P11-T04](#ticket-p11-t04), [P11-T06](#ticket-p11-t06), [P03-T19](#ticket-p03-t19). **Integrate after:** None.

**Implementation steps:**

1. Run prior supported release/config/state fixtures through update, crash-resume, rollback, and restore.
2. Include nonempty outbox, action reorder/rename, schema/extension migration, and disconnected fleet members.

**Acceptance:** No mixed catalog generation or lost backlog; unsupported downgrade fails before mutation; consistent restored bundle includes effects and schema/extension identity; runbook steps reproduce results.


### B18. P12 — Extension SDK, reusable components, and embedding

<a id="ticket-p12-t01"></a>

#### P12-T01 — Define versioned extension manifests and ABI contracts

**Owns:** core/extensions/{manifest,abi}.rs (new). **Validation:** CORE (Core crate test). **Target:** `ticket_p12_t01`.

**Build after:** [C02](#ticket-c02), [C04](#ticket-c04). **Integrate after:** None.

**Implementation steps:**

1. Implement typed functions/ports, engine/ABI compatibility, artifact digest, permissions, capabilities, state versions, and migrations.
2. Map current PluginDefinition through an adapter.
3. Define trust tiers for WASM, out-of-process, and native modules.

**Acceptance:** Unknown ABI or missing capability rejects; old plugin definitions load through adapter; serialization golden fixtures remain stable; secret values are excluded from manifests.

<a id="ticket-p12-t02"></a>

#### P12-T02 — Implement extension dependency and lifecycle registry

**Owns:** core/extensions/registry.rs (new). **Validation:** CORE (Core crate test). **Target:** `ticket_p12_t02`.

**Build after:** [P12-T01](#ticket-p12-t01), [P11-T02](#ticket-p11-t02). **Integrate after:** None.

**Implementation steps:**

1. Resolve pinned versions/dependencies, detect cycles, retain versions referenced by running plans/savepoints, and load artifacts at bootstrap.
2. Distinguish installed definition from successfully loaded executable.
3. Fail loading without changing active version.

**Acceptance:** Restart re-registers actual handlers; missing/bad artifact shows failure; in-use version cannot be removed; upgrade keeps old runtime references until drain; dependency cycle fails deterministically.

<a id="ticket-p12-t03"></a>

#### P12-T03 — Extend WASM host resource and permission controls

**Owns:** core/wasm.rs; extensions/wasm_adapter.rs (new). **Validation:** CORE (Core crate test). **Target:** `ticket_p12_t03`.

**Build after:** [P12-T01](#ticket-p12-t01), [P02-T01](#ticket-p02-t01), [C03](#ticket-c03). **Integrate after:** None.

**Implementation steps:**

1. Bind instruction/memory/host-call budgets, cancellation/deadline, and declared I/O permissions to an extension invocation.
2. Preserve existing safe execution/pooling.
3. Charge retained state and define deterministic snapshot requirements.

**Acceptance:** Infinite loop, allocation storm, denied host call, and cancelled invocation terminate within policy; pool reuse cannot leak prior tenant data; normal UDF results remain compatible.

<a id="ticket-p12-t04"></a>

#### P12-T04 — Implement out-of-process extension protocol

**Owns:** core/extensions/process_protocol.rs; host adapter (new). **Validation:** CORE (Core crate test). **Target:** `ticket_p12_t04`.

**Build after:** [P12-T01](#ticket-p12-t01), [C03](#ticket-c03), [C04](#ticket-c04). **Integrate after:** None.

**Implementation steps:**

1. Define framed versioned request/result/cancel/snapshot messages, bounded frames/inflight calls, health handshake, and process lifetime.
2. Use explicit types/outcomes and no inherited secrets beyond declared inputs.
3. Native modules remain explicitly trusted.

**Acceptance:** Malformed/oversized frames fail safely; worker crash fails pending calls without hanging; cancellation releases permits; incompatible protocol cannot register; snapshot round-trip conformance passes.

<a id="ticket-p12-t05"></a>

#### P12-T05 — Implement reusable parameterized SQL/DAG components

**Owns:** sql/planning/components.rs (new). **Validation:** SQL (SQL crate test). **Target:** `ticket_p12_t05`.

**Build after:** [P01-T02](#ticket-p01-t02), [P11-T03](#ticket-p11-t03). **Integrate after:** None.

**Implementation steps:**

1. Expand typed input/output/parameter components with isolated stable instance IDs and dependency versions.
2. Validate ports and parameter types before execution.
3. Detect recursive expansion; share only declared immutable code, not mutable instance state.

**Acceptance:** Two instances keep separate state/timers; invalid port/parameter fails at location; cyclic dependency rejects; expanded plan matches an equivalent handwritten plan.

<a id="ticket-p12-t06"></a>

#### P12-T06 — Publish stable Rust embedding facade

**Owns:** crates/rekuiper-core embedding facade or dedicated crate registered by C06. **Validation:** CORE (Core crate test). **Target:** `ticket_p12_t06`.

**Build after:** [P12-T01](#ticket-p12-t01), [P06-T01](#ticket-p06-t01). **Integrate after:** None.

**Implementation steps:**

1. Expose build-plan/start/push/advance/read/stop APIs with explicit ownership, errors, cancellation, and optional persistence.
2. Avoid requiring HTTP server startup.
3. Separate one-shot decision evaluation from continuous streaming handles.

**Acceptance:** An external Rust example embeds transform/window execution without server ports; handle drop stops owned tasks; concurrent calls follow documented thread-safety contract; error values are stable.

<a id="ticket-p12-t07"></a>

#### P12-T07 — Implement versioned C embedding ABI

**Owns:** embedding FFI module; C header/examples (new). **Validation:** CROSS (Affected crates and integration tests). **Target:** `ticket_p12_t07`.

**Build after:** [P12-T06](#ticket-p12-t06). **Integrate after:** None.

**Implementation steps:**

1. Expose opaque versioned handles, explicit buffer allocation/free, structured errors, cancellation, and ABI feature negotiation.
2. Catch boundary panics where applicable; document string/byte ownership and thread safety.
3. Do not expose Rust layout types.

**Acceptance:** C smoke program runs a rule; invalid/null handle returns error; repeated free misuse is handled per documented contract; allocator ownership and cross-thread permitted operations pass sanitizer/platform tests.

<a id="ticket-p12-t08"></a>

#### P12-T08 — Implement Python embedding bindings

**Owns:** bindings/python/ (new). **Validation:** CROSS (Affected crates and integration tests). **Target:** `ticket_p12_t08`.

**Build after:** [P12-T07](#ticket-p12-t07). **Integrate after:** None.

**Implementation steps:**

1. Implement the Python binding over P12-T07 with explicit engine-handle lifetime, async cancellation, typed errors, byte ownership, and callback/backpressure behavior.
2. Keep evaluation in the engine and provide one-shot/streaming examples.
3. Use the actual compiled ABI in tests.

**Acceptance:** Python runs shared fixtures and binary payload round trips; context-manager/dispose releases handles/tasks; cancellation propagates; packaging builds against the supported ABI.

<a id="ticket-p12-t09"></a>

#### P12-T09 — Ship extension template and conformance kit

**Owns:** templates/extensions; extension qualification fixtures. **Validation:** CROSS (Affected crates and integration tests). **Target:** `ticket_p12_t09`.

**Build after:** [P12-T02](#ticket-p12-t02), [P12-T03](#ticket-p12-t03), [P12-T04](#ticket-p12-t04), [P12-T05](#ticket-p12-t05). **Integrate after:** None.

**Implementation steps:**

1. Provide scalar, stateful operator, codec, and connector starter templates with manifests, fixtures, permissions, migration example, and failure tests.
2. Verify installation, restart, upgrade, and revocation against the actual registry.

**Acceptance:** A sample third-party extension builds without core edits; stateful sample survives restore; wrong permission/ABI fails; capability documentation is generated from tested manifest.

<a id="ticket-p12-t10"></a>

#### P12-T10 — Implement JavaScript embedding bindings

**Owns:** bindings/javascript/ (new). **Validation:** CROSS (Affected crates and integration tests). **Target:** `ticket_p12_t10`.

**Build after:** [P12-T07](#ticket-p12-t07). **Integrate after:** None.

**Implementation steps:**

1. Implement the JavaScript/Node binding over the same C ABI, with explicit dispose, Promise cancellation, byte buffers, typed errors, and bounded callback delivery.
2. Keep JS runtime objects out of Rust worker threads; register packaging dependencies through C06.

**Acceptance:** Actual compiled-engine fixture passes; aborted Promise cancels work; garbage collection is not the only cleanup path; no handle leaks after repeated streaming sessions.


### B19. P13 — Governance, isolation, and trustworthy operations

<a id="ticket-p13-t01"></a>

#### P13-T01 — Implement namespace and principal ownership model

**Owns:** core/governance/identity.rs (new). **Validation:** CORE (Core crate test). **Target:** `ticket_p13_t01`.

**Build after:** [C01](#ticket-c01), [P11-T02](#ticket-p11-t02). **Integrate after:** None.

**Implementation steps:**

1. Add namespace/principal/action ownership to catalog/state/effects without changing simple standalone defaults.
2. Define resource addressing and migration for existing default namespace.
3. Keep display names separate from authorization identity.

**Acceptance:** Same display name in two namespaces remains isolated; old catalog migrates to default namespace; foreign namespace cannot resolve state/effect identifiers; stable IDs survive rename.

<a id="ticket-p13-t02"></a>

#### P13-T02 — Implement deny-by-default authorization decisions

**Owns:** core/governance/authorize.rs (new). **Validation:** CORE (Core crate test). **Target:** `ticket_p13_t02`.

**Build after:** [P13-T01](#ticket-p13-t01). **Integrate after:** None.

**Implementation steps:**

1. Define actions for read/edit/run/deploy/replay/secret-use/install/admin and role-to-scope grants.
2. Evaluate explicit principal/resource/action; missing grant denies.
3. Provide a hook for external policy adapters without coupling core to one language.

**Acceptance:** Table-driven allow/deny matrix covers inherited scopes, revoked grants, unknown resources, and secret use; read permission never implies deploy/replay/install.

<a id="ticket-p13-t03"></a>

#### P13-T03 — Enforce authorization on every control surface

**Owns:** server routes/handlers; CLI/MCP/fleet adapters. **Validation:** CROSS (Affected crates and integration tests). **Target:** `ticket_p13_t03`.

**Build after:** [P13-T02](#ticket-p13-t02). **Integrate after:** None.

**Implementation steps:**

1. Build an endpoint/tool inventory and route all sensitive operations through the same authorizer.
2. Propagate principal identity to background jobs.
3. Preserve authentication setup and standalone compatibility; UI visibility is not enforcement.

**Acceptance:** For each inventoried operation, unauthorized direct REST/tool request fails; background job cannot lose principal scope; CLI/MCP and REST make identical decisions; hidden UI controls are still enforced server-side.

<a id="ticket-p13-t04"></a>

#### P13-T04 — Implement namespace resource fairness

**Owns:** core/resources/fair_scheduler.rs (new). **Validation:** CORE (Core crate test). **Target:** `ticket_p13_t04`.

**Build after:** [P13-T01](#ticket-p13-t01), [P02-T01](#ticket-p02-t01). **Integrate after:** None.

**Implementation steps:**

1. Partition CPU/task/memory/disk/rate/outbound quotas by namespace and rule.
2. Schedule bounded work fairly; release charges on stop/error.
3. Enforce global plus namespace caps without relying on untrusted self-reporting.

**Acceptance:** Noisy namespace cannot consume another's reservation; cancellation returns capacity; global and local limits both hold; measured quiet-rule latency stays within the selected test budget.

<a id="ticket-p13-t05"></a>

#### P13-T05 — Centralize secret, egress, and filesystem permissions

**Owns:** core/governance/resources.rs (new); connector/extension adapters. **Validation:** CROSS (Affected crates and integration tests). **Target:** `ticket_p13_t05`.

**Build after:** [P13-T02](#ticket-p13-t02), [P12-T01](#ticket-p12-t01), [A02-T04](#ticket-a02-t04). **Integrate after:** None.

**Implementation steps:**

1. Apply declared secret references, destination/egress, filesystem-root, and extension permissions at resource creation/use.
2. Preserve current restrictions and anchored image I/O; rotate credentials without changing destination backlog identity.

**Acceptance:** Denied outbound/file/secret access fails before side effect; rotation does not reroute queued effects; error/provenance output redacts values; direct extension access cannot bypass the guard.

<a id="ticket-p13-t06"></a>

#### P13-T06 — Add audit trail and operational recovery commands

**Owns:** server audit module; CLI operations adapters. **Validation:** CROSS (Affected crates and integration tests). **Target:** `ticket_p13_t06`.

**Build after:** [P13-T03](#ticket-p13-t03), [P06-T05](#ticket-p06-t05), [P03-T19](#ticket-p03-t19). **Integrate after:** None.

**Implementation steps:**

1. Record authorized administrative mutations, artifact versions, replay, restore, and reconciliation with principal/generation/result.
2. Bound local retention and support export.
3. Add inspect/drain/backup status using existing operations rather than alternate execution paths.

**Acceptance:** Audit includes failures and async final outcomes without secrets; retention is bounded; unauthorized operations leave appropriate denial evidence; CLI runbook commands reproduce server results.

<a id="ticket-p13-t07"></a>

#### P13-T07 — Qualify identity integration and tenant isolation

**Owns:** auth/service fixtures; cross-namespace qualification. **Validation:** SERVICE (Real-service qualification plus owning-crate tests). **Target:** `ticket_p13_t07`.

**Build after:** [P13-T03](#ticket-p13-t03), [P13-T04](#ticket-p13-t04), [P13-T05](#ticket-p13-t05), [P13-T06](#ticket-p13-t06). **Integrate after:** None.

**Implementation steps:**

1. Add a scoped external identity adapter behind existing auth configuration and test expiry/revocation/rotation.
2. Exercise every surface with two namespaces, resource exhaustion, restore, and untrusted extension failure.

**Acceptance:** Expired/revoked identity cannot start new work; namespace A cannot inspect/restore/replay B data; failures stay contained; standalone mode remains operable without an identity service.


### B20. P14 — High availability and distributed rule execution

<a id="ticket-p14-t01"></a>

#### P14-T01 — Freeze distributed ownership and placement contracts

**Owns:** core/cluster/contracts.rs (new). **Validation:** CORE (Core crate test). **Target:** `ticket_p14_t01`.

**Build after:** [C04](#ticket-c04), [P11-T02](#ticket-p11-t02). **Integrate after:** None.

**Implementation steps:**

1. Define worker identity, lease/epoch, partition assignment, state location, capability/version constraints, and effect ownership.
2. Specify active/passive first, per-key partitioning later.
3. External effect ambiguity remains governed by P03, not hidden by leases.

**Acceptance:** Contract fixtures reject stale epoch, incompatible worker, conflicting assignment, and missing state version; partition/key ordering domains are explicit and serializable.

<a id="ticket-p14-t02"></a>

#### P14-T02 — Implement quorum coordination adapter

**Owns:** core/cluster/coordination.rs (new). **Validation:** SERVICE (Real-service qualification plus owning-crate tests). **Target:** `ticket_p14_t02`.

**Build after:** [P14-T01](#ticket-p14-t01), [C06](#ticket-c06). **Integrate after:** None.

**Implementation steps:**

1. Implement the external coordination interface against an etcd v3-compatible quorum service: compare-and-swap assignment, leased ownership, watches with revision resume, and explicit quorum-loss behavior.
2. Keep a fake adapter for deterministic tests; do not invent consensus.

**Acceptance:** Concurrent claim yields one owner; expired lease cannot renew old epoch; interrupted watch resumes or resyncs safely; loss of quorum refuses new ownership; pinned real-service tests pass.

<a id="ticket-p14-t03"></a>

#### P14-T03 — Implement active/passive rule recovery

**Owns:** server/cluster/failover.rs (new). **Validation:** SERVER (Server crate test). **Target:** `ticket_p14_t03`.

**Build after:** [P14-T02](#ticket-p14-t02), [P03-T19](#ticket-p03-t19), [P03-T10](#ticket-p03-t10), [P11-T04](#ticket-p11-t04). **Integrate after:** None.

**Implementation steps:**

1. Start a standby from committed state only after acquiring a new epoch.
2. Fence store/claim operations of the old owner and reconcile pending effects.
3. Separate already-inflight ambiguous external writes from fresh ownership; report the qualified delivery limit.

**Acceptance:** Kill active worker and verify state/source resume; stale active cannot commit/claim; partitioned old process cannot become a second valid owner; effect duplicates match stated at-least-once/transactional contract.

<a id="ticket-p14-t04"></a>

#### P14-T04 — Implement bounded partition data exchange

**Owns:** core/cluster/exchange.rs (new). **Validation:** CORE (Core crate test). **Target:** `ticket_p14_t04`.

**Build after:** [P14-T01](#ticket-p14-t01), [P02-T01](#ticket-p02-t01), [C03](#ticket-c03). **Integrate after:** None.

**Implementation steps:**

1. Route keyed records and ordered barriers/watermarks between workers with bounded frames/queues, authentication hooks, sequence numbers, retry identity, and cancellation.
2. Preserve per-partition order; reject unsupported global ordering claims.

**Acceptance:** Reordered/retried transport frames are handled deterministically; disconnect backpressures within budget; control cannot overtake data; wrong epoch/namespace frame is rejected.

<a id="ticket-p14-t05"></a>

#### P14-T05 — Partition physical plans and state ownership

**Owns:** server/cluster/planner.rs (new). **Validation:** SERVER (Server crate test). **Target:** `ticket_p14_t05`.

**Build after:** [P14-T04](#ticket-p14-t04), [P01-T03](#ticket-p01-t03), [P05-T04](#ticket-p05-t04). **Integrate after:** None.

**Implementation steps:**

1. Assign supported keyed operators and exchange edges by declared partition key.
2. Keep unsupported global/cyclic operators explicit.
3. Track per-partition watermarks/barriers and state budgets; preserve standalone plan path.

**Acceptance:** Partitioned transform/aggregate results equal single-worker oracle; same key has one active owner; unsupported shape fails before deployment; skewed partition cannot silently overrun budget.

<a id="ticket-p14-t06"></a>

#### P14-T06 — Implement savepoint-based rescaling

**Owns:** server/cluster/rescale.rs (new). **Validation:** SERVER (Server crate test). **Target:** `ticket_p14_t06`.

**Build after:** [P14-T05](#ticket-p14-t05), [P03-T18](#ticket-p03-t18), [P11-T04](#ticket-p11-t04). **Integrate after:** None.

**Implementation steps:**

1. Checkpoint/drain, write a partition transfer manifest, fence old epochs, redistribute versioned state, and resume from correct positions.
2. Start with stop-and-rescale; retain old checkpoint until successful activation.
3. Online transfer is a later separately qualified mode.

**Acceptance:** Rescale 1→2→1 under input replay matches oracle; crash during transfer recovers old/new generation deterministically; effects retain ownership; partial manifests never activate.

<a id="ticket-p14-t07"></a>

#### P14-T07 — Separate control-plane outages from execution

**Owns:** server/cluster/lifecycle.rs (new). **Validation:** SERVER (Server crate test). **Target:** `ticket_p14_t07`.

**Build after:** [P14-T03](#ticket-p14-t03), [P14-T05](#ticket-p14-t05), [P13-T05](#ticket-p13-t05). **Integrate after:** None.

**Implementation steps:**

1. Specify and implement existing-rule continuation until lease/credential deadlines, while blocking unsafe new assignments/deployments without quorum.
2. Expose degraded status and reconnect reconciliation; never extend leases locally without authority.

**Acceptance:** Control-plane loss has deterministic running/stopping behavior; expired permissions do not refresh themselves; reconnect does not duplicate ownership; operators can distinguish unavailable management from failed data execution.

<a id="ticket-p14-t08"></a>

#### P14-T08 — Implement coordinated rolling worker upgrades

**Owns:** server/cluster/upgrade.rs; recovery runbooks (new). **Validation:** SERVER (Server crate test). **Target:** `ticket_p14_t08`.

**Build after:** [P14-T06](#ticket-p14-t06), [P14-T07](#ticket-p14-t07), [P11-T07](#ticket-p11-t07). **Integrate after:** None.

**Implementation steps:**

1. Roll compatible workers through savepoints and ownership transfer, retaining old artifacts and rollback compatibility information.
2. Validate protocol/state/extension versions before drain and update placement constraints per generation.

**Acceptance:** Supported mixed versions interoperate; incompatible worker is rejected before draining healthy owner; crash during rollout resumes known generation; rollback checks state compatibility.

<a id="ticket-p14-t09"></a>

#### P14-T09 — Qualify cluster failure and rescale matrix

**Owns:** cluster chaos/qualification fixtures (new). **Validation:** SERVICE (Real-service qualification plus owning-crate tests). **Target:** `ticket_p14_t09`.

**Build after:** [P14-T03](#ticket-p14-t03). **Integrate after:** None.

**Implementation steps:**

1. Qualify active/passive ownership first with worker termination, network partitions, quorum/storage loss, delayed heartbeats, and clock skew.
2. Use independent state/output oracle and record exact failure schedule; certify only tested plan shapes.
3. Add partition/rescale/rolling-upgrade scenarios as P14-T05/P14-T06/P14-T08 land, and DR/online-migration scenarios after P14-T10/P14-T11. Each capability requires its own scenarios; missing later capabilities do not block active/passive certification.

**Acceptance:** No two valid owners or missing durable input within contract; stale writes fenced; alignment bounded; recovery and duplicate limits measured; standalone execution needs no cluster services.

<a id="ticket-p14-t10"></a>

#### P14-T10 — Implement multi-zone and cross-region recovery manifests

**Owns:** server/cluster/disaster_recovery.rs; runbooks (new). **Validation:** SERVER (Server crate test). **Target:** `ticket_p14_t10`.

**Build after:** [P14-T06](#ticket-p14-t06), [P03-T18](#ticket-p03-t18), [P11-T07](#ticket-p11-t07). **Integrate after:** None.

**Implementation steps:**

1. Create verified consistent recovery manifests with replicated catalog/state/effect artifacts and source retention prerequisites.
2. Define asynchronous cross-region restore to a known frontier with declared measured RPO; new region must acquire/fence ownership before intake.

**Acceptance:** Zone/region-loss rehearsal restores a verified frontier; unavailable newer artifacts fall back only under explicit policy; stale region cannot resume valid ownership; RPO/RTO measurements include pending effects.

<a id="ticket-p14-t11"></a>

#### P14-T11 — Implement bounded online state migration

**Owns:** server/cluster/online_transfer.rs (new). **Validation:** SERVER (Server crate test). **Target:** `ticket_p14_t11`.

**Build after:** [P14-T06](#ticket-p14-t06), [P14-T04](#ticket-p14-t04), [P03-T20](#ticket-p03-t20). **Integrate after:** None.

**Implementation steps:**

1. Build on stop-and-rescale: copy immutable snapshot, stream bounded ordered deltas to catch up, coordinate final barrier, fence old epoch, then activate destination.
2. Abort retains old authoritative owner; keep stop-and-rescale fallback.

**Acceptance:** Migration under load equals reference state; crash before/after final barrier selects exactly one authoritative owner; delta backlog is bounded; unsupported operator state uses stop-and-rescale.


### B21. P15 — Interoperability, schemas, and the connector ecosystem

<a id="ticket-p15-t01"></a>

#### P15-T01 — Expose full Kafka metadata without payload pollution

**Owns:** conn/kafka.rs; core/model.rs metadata adapter. **Validation:** CONN (Connector crate test). **Target:** `ticket_p15_t01`.

**Build after:** [C01](#ticket-c01). **Integrate after:** None.

**Implementation steps:**

1. Implement A4 partition/offset/timestamp/key/headers at broker-record boundary, sharing metadata across decoded rows.
2. Keep compatibility header object; add lossless binary/raw duplicate representation only when client preserves it.
3. Report client limitation rather than fabricate data.

**Acceptance:** Known broker fixture emits exact integer offset/partition; binary key round-trips; multirow records share metadata; existing payload projections unchanged; duplicate-header capability is accurately declared.

<a id="ticket-p15-t02"></a>

#### P15-T02 — Implement common strict JSONL and stream codecs

**Owns:** conn/codec.rs; codec framing modules. **Validation:** CONN (Connector crate test). **Target:** `ticket_p15_t02`.

**Build after:** None. **Integrate after:** None.

**Implementation steps:**

1. Implement A5 bounded streaming decode with strict newline mode and explicit concatenated-object compatibility mode.
2. Validate full bounded message before emitting a prefix; identify failing record/byte.
3. Add framed writer with clear array/nonobject handling.

**Acceptance:** Two valid lines decode in order; malformed second line emits no prefix; escaped newline/CRLF/empty/oversize/adjacent cases follow selected mode; encoder/decoder round trip.

<a id="ticket-p15-t03"></a>

#### P15-T03 — Wire JSONL across ingress and textual output

**Owns:** conn payload dispatch; server ingress/sinks adapters. **Validation:** CROSS (Affected crates and integration tests). **Target:** `ticket_p15_t03`.

**Build after:** [P15-T02](#ticket-p15-t02), [P15-T01](#ticket-p15-t01), [P02-T05](#ticket-p02-t05). **Integrate after:** None.

**Implementation steps:**

1. Register formats for HTTP push/pull, MQTT, Kafka, REST and file paths with common schema/metadata handling.
2. Preserve broker-message acknowledgement across expansion, templates/sendSingle/batching, HTTP headers, and file append separators.

**Acceptance:** Same payload yields same rows across adapters; Kafka/metadata preserved; no duplicate per-row broker ack; append batches remain valid framing; existing JSON output shape tests pass.

<a id="ticket-p15-t04"></a>

#### P15-T04 — Version schemas and compatibility checks

**Owns:** core/catalog/schema.rs (new); schema registry adapter. **Validation:** CORE (Core crate test). **Target:** `ticket_p15_t04`.

**Build after:** [P11-T02](#ticket-p11-t02). **Integrate after:** None.

**Implementation steps:**

1. Assign immutable schema fingerprints/versions and dependencies; bind plans to version.
2. Define field addition/removal/default/null/type-change compatibility for the supported schema kinds.
3. Never mutate a running plan's schema binding.

**Acceptance:** Compatible/ incompatible evolution matrix is deterministic; replay uses pinned version; concurrent registry update does not change live interpretation; failed persistence keeps prior schema.

<a id="ticket-p15-t05"></a>

#### P15-T05 — Implement declared JSON Schema validation

**Owns:** conn/schema/json_schema.rs (new). **Validation:** CONN (Connector crate test). **Target:** `ticket_p15_t05`.

**Build after:** [P15-T04](#ticket-p15-t04), [C06](#ticket-c06). **Integrate after:** None.

**Implementation steps:**

1. Implement a declared JSON Schema version/subset using pinned reference fixtures, explicit null/default handling, bounded recursion/input, and located errors.
2. Preserve existing JSON/protobuf/custom parsing; schema validation must not silently mutate input or claim unsupported keywords.

**Acceptance:** Supported reference cases pass; invalid data names field/schema location; unsupported constructs reject; defaults apply only under the documented explicit conversion mode; pathological nesting stays bounded.

<a id="ticket-p15-t06"></a>

#### P15-T06 — Implement external registry resolution

**Owns:** conn/schema/registry.rs (new). **Validation:** SERVICE (Real-service qualification plus owning-crate tests). **Target:** `ticket_p15_t06`.

**Build after:** [P15-T04](#ticket-p15-t04), [P02-T06](#ticket-p02-t06), [C06](#ticket-c06). **Integrate after:** None.

**Implementation steps:**

1. Resolve schema IDs/versions via a registry adapter with authentication, deadlines, bounded cache, pinned IDs, and explicit offline/miss policy.
2. Credential changes invalidate only relevant clients, not schema identity.

**Acceptance:** Cache eviction stays bounded; registry outage with pinned cached schema follows policy; unknown ID fails; auth values never leak; schema change cannot alter an already bound plan.

<a id="ticket-p15-t07"></a>

#### P15-T07 — Implement typed vector lookup and multirow expansion

**Owns:** server/engine/lookup/vector.rs (new). **Validation:** SERVER (Server crate test). **Target:** `ticket_p15_t07`.

**Build after:** [P02-T07](#ticket-p02-t07), [P01-T02](#ticket-p01-t02). **Integrate after:** None.

**Implementation steps:**

1. Implement A10 request contract: dimension, finite vector, metric, top-K, filters, deadline; preserve ranked multirow results and backend distance.
2. Add LEFT no-match behavior only for vector tables; ordinary first-match lookup remains unchanged.

**Acceptance:** Invalid dimension/nonfinite/excessive K rejects before I/O; ranked ties/order/cardinality fixtures pass; no-match left emits one null-extended row; cancellation releases permits.

<a id="ticket-p15-t08"></a>

#### P15-T08 — Implement pgvector lookup backend

**Owns:** conn/vector/pgvector.rs (new). **Validation:** SERVICE (Real-service qualification plus owning-crate tests). **Target:** `ticket_p15_t08`.

**Build after:** [P15-T07](#ticket-p15-t07), [C06](#ticket-c06). **Integrate after:** None.

**Implementation steps:**

1. Use existing Postgres pool/secrets where compatible; bind vectors/filter values, validate identifiers, and map declared distance metric/top-K.
2. Implement deadline/cancel and schema/dimension checks; never interpolate untrusted SQL fragments.

**Acceptance:** Real database nearest-neighbor oracle matches configured metric/order; adversarial filter values cannot alter SQL; stalled query cancels; auth/schema errors are structured.

<a id="ticket-p15-t09"></a>

#### P15-T09 — Implement Milvus lookup backend

**Owns:** conn/vector/milvus.rs (new). **Validation:** SERVICE (Real-service qualification plus owning-crate tests). **Target:** `ticket_p15_t09`.

**Build after:** [P15-T07](#ticket-p15-t07), [C06](#ticket-c06). **Integrate after:** None.

**Implementation steps:**

1. Implement declared collection/vector-field/metric/top-K mapping using pinned protocol/client, safe scalar filter builder, TLS/egress controls, deadline/cancel, and result score preservation.
2. Keep backend-specific validation outside generic SQL equality.

**Acceptance:** Real-service seeded vectors yield expected ranking; invalid dimensions/filters reject; cancellation and backend outage bounded; normalized score is only exposed with documented metric conversion.

<a id="ticket-p15-t10"></a>

#### P15-T10 — Implement NATS and JetStream source adapter

**Owns:** conn/nats/ (new). **Validation:** SERVICE (Real-service qualification plus owning-crate tests). **Target:** `ticket_p15_t10`.

**Build after:** [C02](#ticket-c02), [C04](#ticket-c04), [C06](#ticket-c06), [P03-T02](#ticket-p03-t02), [P02-T06](#ticket-p02-t06). **Integrate after:** None.

**Implementation steps:**

1. Implement explicit core-NATS versus durable JetStream source modes, subject mapping, message/delivery identity, bounded subscription/pull/prefetch, acknowledgement/requeue, and reconnect.
2. Durable acknowledgement uses the reliable acceptance callback; core NATS declares no replay.

**Acceptance:** Real service redelivery and reconnect cases pass; no early JetStream acknowledgement in reliable mode; core NATS cannot request replay guarantees; cancellation/prefetch bounds hold.

<a id="ticket-p15-t11"></a>

#### P15-T11 — Implement object-storage source cursor

**Owns:** conn/object_storage/ (new). **Validation:** SERVICE (Real-service qualification plus owning-crate tests). **Target:** `ticket_p15_t11`.

**Build after:** [C02](#ticket-c02), [C04](#ticket-c04), [C06](#ticket-c06), [P02-T06](#ticket-p02-t06). **Integrate after:** None.

**Implementation steps:**

1. Implement bounded listing/read with stable object key/version/etag identity, checkpointed cursor, and explicit concurrent mutation policy.
2. Preserve source ordering contract and object-level acknowledgement across decoded rows; do not use listing order as a durable unique identity.

**Acceptance:** Repeated/incomplete listings do not silently skip objects; changed version is detected; multirow object commits as one position; cancellation/large objects stay bounded; restart resumes correct object cursor.

<a id="ticket-p15-t12"></a>

#### P15-T12 — Implement Prometheus scrape ingestion

**Owns:** conn/prometheus/ (new). **Validation:** SERVICE (Real-service qualification plus owning-crate tests). **Target:** `ticket_p15_t12`.

**Build after:** [P07-T01](#ticket-p07-t01), [P07-T02](#ticket-p07-t02), [P07-T05](#ticket-p07-t05), [C06](#ticket-c06). **Integrate after:** None.

**Implementation steps:**

1. Implement bounded nonoverlapping scrape with protocol/type/label mapping into P07 model.
2. Preserve timestamps, stale/reset semantics, histogram shapes, resource identity, and cardinality policy.
3. Reject unsupported sample shapes explicitly.

**Acceptance:** Reference scrape fixtures preserve declared metrics; stalled target cancels; polling never overlaps by default; label flood remains bounded; stale/reset samples match oracle.

<a id="ticket-p15-t13"></a>

#### P15-T13 — Implement eKuiper artifact import subset

**Owns:** cli migration modules; import fixtures. **Validation:** CLI (CLI crate test). **Target:** `ticket_p15_t13`.

**Build after:** [P01-T03](#ticket-p01-t03), [P12-T05](#ticket-p12-t05), [P15-T04](#ticket-p15-t04). **Integrate after:** None.

**Implementation steps:**

1. Translate a declared pinned eKuiper config/SQL/action subset into versioned artifacts.
2. Preserve compatible syntax/settings, convert credentials to references, emit required fixture expectations and explicit unsupported diagnostics; do not activate on import.

**Acceptance:** Golden imports pass production fixtures; unsupported timing/delivery/plugin behavior is named; re-import is deterministic; ownership conflict cannot overwrite active rules.

<a id="ticket-p15-t14"></a>

#### P15-T14 — Add optional PostgreSQL wire access subset

**Owns:** server/protocol/pgwire.rs (new). **Validation:** SERVICE (Real-service qualification plus owning-crate tests). **Target:** `ticket_p15_t14`.

**Build after:** [P09-T07](#ticket-p09-t07), [P13-T03](#ticket-p13-t03), [C06](#ticket-c06). **Integrate after:** None.

**Implementation steps:**

1. Expose explicitly supported catalog/read/subscription operations with authentication, parameter binding, cancellation, and documented protocol/error/type mapping.
2. Reuse engine APIs; do not claim full PostgreSQL transactions or SQL.
3. Keep HTTP independent.

**Acceptance:** Declared driver smoke tests pass; unsupported SQL returns correct explicit error; query cancel releases subscription; namespace isolation holds; protocol cannot bypass authorization.

<a id="ticket-p15-t15"></a>

#### P15-T15 — Register connector-specific qualification and adoption gates

**Owns:** connector qualification registry; real-service fixtures. **Validation:** SERVICE (Real-service qualification plus owning-crate tests). **Target:** `ticket_p15_t15`.

**Build after:** [C02](#ticket-c02), [C07](#ticket-c07). **Integrate after:** None.

**Implementation steps:**

1. Define per-adapter conformance entries for outage/restart, schema, order, partial-batch, cancellation, and recovery.
2. Consume each adapter's real-service evidence independently; absent adapters remain unqualified.
3. Define separate local-versus-external vector benchmark inputs and acceptance thresholds, without waiting for all backends.

**Acceptance:** One passing connector can qualify while another fails or is absent; each matrix entry links service/version/evidence; no unsupported guarantees inferred; vector adoption requires its separate real-backend comparison.

<a id="ticket-p15-t16"></a>

#### P15-T16 — Implement Avro values and registry wire framing

**Owns:** conn/schema/avro.rs (new). **Validation:** CONN (Connector crate test). **Target:** `ticket_p15_t16`.

**Build after:** [P15-T04](#ticket-p15-t04), [C06](#ticket-c06). **Integrate after:** None.

**Implementation steps:**

1. Implement a pinned Avro schema/value subset, reader/writer evolution, null unions, defaults, and declared registry wire framing.
2. Resolve schema identity through an injected resolver so registry I/O is independently replaceable.

**Acceptance:** Reference binary fixtures round-trip; writer/reader compatible evolution succeeds; invalid union/index/schema ID fails; bounded decode rejects oversized recursive inputs without emitting a prefix.

<a id="ticket-p15-t17"></a>

#### P15-T17 — Implement NATS and JetStream sink adapter

**Owns:** conn/nats/sink.rs (new). **Validation:** SERVICE (Real-service qualification plus owning-crate tests). **Target:** `ticket_p15_t17`.

**Build after:** [C02](#ticket-c02), [C04](#ticket-c04), [C06](#ticket-c06), [P02-T06](#ticket-p02-t06). **Integrate after:** None.

**Implementation steps:**

1. Implement subject/key mapping, bounded pending publish, JetStream confirmations/dedup headers, and core-NATS's weaker result contract.
2. Map disconnect/timeout to accurate effect outcomes and preserve metadata encoding.

**Acceptance:** Reference subscription observes exact payload/metadata; ack-loss becomes retryable/ambiguous as defined; pending publish count bounded; no broker persistence guarantee is claimed for core NATS.

<a id="ticket-p15-t18"></a>

#### P15-T18 — Implement idempotent object-storage sink

**Owns:** conn/object_storage/sink.rs (new). **Validation:** SERVICE (Real-service qualification plus owning-crate tests). **Target:** `ticket_p15_t18`.

**Build after:** [C02](#ticket-c02), [C04](#ticket-c04), [C06](#ticket-c06), [P02-T05](#ticket-p02-t05), [P02-T06](#ticket-p02-t06). **Integrate after:** None.

**Implementation steps:**

1. Implement deterministic action/effect-owned object keys, conditional/idempotent writes where available, multipart upload lifecycle, and bounded retries.
2. Separate backup-store usage from normal payload output and declare backend consistency assumptions.

**Acceptance:** Crash/retry cannot overwrite another action's object; incomplete multipart upload is resumed or cleaned per policy; checksum/length verified; ambiguous response is not silently classified as delivered.

<a id="ticket-p15-t19"></a>

#### P15-T19 — Implement Prometheus remote-write exporter

**Owns:** conn/prometheus/remote_write.rs (new). **Validation:** SERVICE (Real-service qualification plus owning-crate tests). **Target:** `ticket_p15_t19`.

**Build after:** [P07-T01](#ticket-p07-t01), [P07-T02](#ticket-p07-t02), [P07-T05](#ticket-p07-t05), [C06](#ticket-c06), [P02-T05](#ticket-p02-t05), [P02-T06](#ticket-p02-t06). **Integrate after:** None.

**Implementation steps:**

1. Implement pinned remote-write protocol encoding, compression, series/histogram mapping, request limits, and retry classification.
2. Preserve per-series ordering and expose unsupported signal shapes; use the common batch/effect contracts.

**Acceptance:** Reference receiver accepts and decodes identical series; out-of-order/retry handling follows declared protocol; compressed/uncompressed bounds hold; stalled backend does not grow pending tasks.

<a id="ticket-p15-t20"></a>

#### P15-T20 — Implement Node-RED flow import subset

**Owns:** cli/migrate/node_red.rs (new). **Validation:** CLI (CLI crate test). **Target:** `ticket_p15_t20`.

**Build after:** [P01-T03](#ticket-p01-t03), [P12-T05](#ticket-p12-t05). **Integrate after:** None.

**Implementation steps:**

1. Translate an explicit set of stateless routing/transformation nodes and connections into DAG components.
2. Preserve ordering/branch semantics where representable; reject custom function/context/timing nodes unless a qualified mapping exists.

**Acceptance:** Golden flows equal their expected fixture outputs; unsupported nodes list exact IDs; duplicate names receive stable unique identities; no approximation is activated silently.

<a id="ticket-p15-t21"></a>

#### P15-T21 — Implement Telegraf configuration import subset

**Owns:** cli/migrate/telegraf.rs (new). **Validation:** CLI (CLI crate test). **Target:** `ticket_p15_t21`.

**Build after:** [P07-T06](#ticket-p07-t06), [P15-T04](#ticket-p15-t04). **Integrate after:** None.

**Implementation steps:**

1. Map selected input/processor/output configurations with collection/flush/batch/jitter and metric semantics.
2. Require compatible registered connectors; emit a migration report for unsupported plugins, timestamp/tag semantics, and delivery differences.

**Acceptance:** Selected configs validate and reproduce telemetry fixtures; unsupported plugin names remain visible; no lossy default is silently treated as reliable; secret references remain external.

<a id="ticket-p15-t22"></a>

#### P15-T22 — Implement Redpanda Connect configuration import subset

**Owns:** cli/migrate/connect.rs (new). **Validation:** CLI (CLI crate test). **Target:** `ticket_p15_t22`.

**Build after:** [P01-T03](#ticket-p01-t03), [P12-T05](#ticket-p12-t05), [P15-T04](#ticket-p15-t04). **Integrate after:** None.

**Implementation steps:**

1. Translate selected input/routing/mapping/output constructs only where equivalent semantics have fixtures.
2. Preserve metadata, message expansion, error/fallback, and acknowledgement requirements.
3. Reject unsupported Bloblang/processors with exact config paths.

**Acceptance:** Supported fixtures match reference outputs/errors; branch/fallback does not silently change ack behavior; unsupported mapping refuses activation; generated artifact includes a reproducible test.

<a id="ticket-p15-t23"></a>

#### P15-T23 — Measure pgvector against local matching

**Owns:** test/benchmark/vector_pgvector.py (new); dataset fixtures. **Validation:** SERVICE (Real-service qualification plus owning-crate tests). **Target:** `ticket_p15_t23`.

**Build after:** [P15-T08](#ticket-p15-t08), [P10-T01](#ticket-p10-t01). **Integrate after:** None.

**Implementation steps:**

1. Generate fixed vectors/queries with known exact neighbors, run local vector math and pgvector under declared data sizes/filter/top-K/concurrency, and measure result quality, latency, resource use and failure behavior.
2. Keep guarantees/precision comparable.

**Acceptance:** Raw seeded results and independent neighbor oracle retained; no adoption based only on throughput; select local matching when it meets the target better; backend configuration/version documented.

<a id="ticket-p15-t24"></a>

#### P15-T24 — Measure Milvus against local matching

**Owns:** test/benchmark/vector_milvus.py (new); shared vector dataset. **Validation:** SERVICE (Real-service qualification plus owning-crate tests). **Target:** `ticket_p15_t24`.

**Build after:** [P15-T09](#ticket-p15-t09), [P10-T01](#ticket-p10-t01). **Integrate after:** None.

**Implementation steps:**

1. Reuse the fixed vector dataset/oracle and benchmark Milvus against local matching with declared index/metric/filter/consistency settings.
2. Report recall/quality alongside latency/resources; test stalled backend cancellation through the real lookup operator.

**Acceptance:** Independent quality and end-to-end latency results retained; ties/filter/no-match correctness verified; approximate recall tradeoff explicit; this benchmark does not wait for pgvector qualification.


### B22. P16 — Decision tables, policy evaluation, and forward chaining

<a id="ticket-p16-t01"></a>

#### P16-T01 — Define decision-table IR and hit policies

**Owns:** sql/decision/table.rs (new). **Validation:** SQL (SQL crate test). **Target:** `ticket_p16_t01`.

**Build after:** [P01-T02](#ticket-p01-t02), [C01](#ticket-c01). **Integrate after:** None.

**Implementation steps:**

1. Represent typed inputs/outputs, ordered rules, unique/first/priority/collect hit policies, and rule identity.
2. Keep request/response decision mode separate from streams.
3. Preserve exact input/error semantics in a versioned serialized artifact.

**Acceptance:** Golden tables round-trip; missing output type/priority is rejected; equal display names retain distinct rule IDs; no streaming runtime is required to compile a table.

<a id="ticket-p16-t02"></a>

#### P16-T02 — Validate overlap, coverage, and unreachable rules

**Owns:** sql/decision/validate.rs (new). **Validation:** SQL (SQL crate test). **Target:** `ticket_p16_t02`.

**Build after:** [P16-T01](#ticket-p16-t01). **Integrate after:** None.

**Implementation steps:**

1. Analyze supported predicate intervals/enumerations for overlap, unreachable rows, and missing outcomes.
2. Mark unprovable coverage as unknown instead of pretending complete.
3. Produce actionable row/column diagnostics tied to hit policy.

**Acceptance:** Unique overlapping ranges fail; first-hit unreachable rule warns/fails by explicit policy; disjoint/exhaustive enum fixtures pass; complex unsupported proof remains explicitly unknown.

<a id="ticket-p16-t03"></a>

#### P16-T03 — Implement deterministic decision evaluation

**Owns:** sql/decision/evaluate.rs (new); embedding adapter. **Validation:** SQL (SQL crate test). **Target:** `ticket_p16_t03`.

**Build after:** [P16-T01](#ticket-p16-t01), [P16-T02](#ticket-p16-t02), [C03](#ticket-c03). **Integrate after:** None.

**Implementation steps:**

1. Compile typed predicates once, evaluate hit policy exactly, and return result plus bounded explanation trace of matched rules/precedence.
2. Add batch evaluation and deadline/cost limits; pure mode forbids effects and mutable global state.

**Acceptance:** All hit-policy fixtures have manually expected results; deterministic same-input output/trace; timeout/budget error is explicit; batch cases cannot leak state between requests.

<a id="ticket-p16-t04"></a>

#### P16-T04 — Implement declared DMN/FEEL subset adapter

**Owns:** sql/decision/dmn.rs (new); conformance fixtures. **Validation:** SQL (SQL crate test). **Target:** `ticket_p16_t04`.

**Build after:** [P16-T03](#ticket-p16-t03), [C06](#ticket-c06). **Integrate after:** None.

**Implementation steps:**

1. Start with literal expressions and typed decision tables using a pinned supported DMN/FEEL version and selected official fixtures.
2. Bind a maintained evaluator through the extension boundary where appropriate.
3. List every supported construct; reject the remainder.

**Acceptance:** Selected official cases pass without expectation rewriting; unsupported constructs reject before execution; type/null/error differences are preserved rather than approximated as SQL.

<a id="ticket-p16-t05"></a>

#### P16-T05 — Implement CEL expression extension

**Owns:** extensions/cel/ (new). **Validation:** CROSS (Affected crates and integration tests). **Target:** `ticket_p16_t05`.

**Build after:** [P12-T01](#ticket-p12-t01), [P12-T04](#ticket-p12-t04), [P16-T03](#ticket-p16-t03), [C06](#ticket-c06). **Integrate after:** None.

**Implementation steps:**

1. Expose a versioned typed CEL compiler/evaluator adapter with immutable compiled program, bounded host data/functions, deadline/cost reporting, and native CEL null/error semantics.
2. Use declared extension permissions; do not translate approximately into SQL.

**Acceptance:** Pinned language corpus and host-function denial tests pass; repeated compile/evaluate is deterministic; resource cap produces explicit error; extension ABI/version mismatch is rejected.

<a id="ticket-p16-t06"></a>

#### P16-T06 — Implement policy-bundle decision adapter

**Owns:** extensions/policy/ (new); decision service adapter. **Validation:** SERVICE (Real-service qualification plus owning-crate tests). **Target:** `ticket_p16_t06`.

**Build after:** [P12-T04](#ticket-p12-t04), [P11-T05](#ticket-p11-t05), [P16-T03](#ticket-p16-t03). **Integrate after:** None.

**Implementation steps:**

1. Integrate a versioned policy evaluator through the out-of-process contract with pinned bundle identity and input schema.
2. Validate/signature-check artifacts in the deployment path; separate evaluation from bundle update.
3. Return bounded decision/explanation metadata.

**Acceptance:** Bundle rotation does not mix versions within a request; invalid signature fails when required; evaluator crash times out/cancels cleanly; tests use pinned policy semantics and isolated data.

<a id="ticket-p16-t07"></a>

#### P16-T07 — Implement working-memory facts and truth dependencies

**Owns:** core/decision/facts.rs (new). **Validation:** CORE (Core crate test). **Target:** `ticket_p16_t07`.

**Build after:** [C01](#ticket-c01), [C04](#ticket-c04), [P09-T01](#ticket-p09-t01). **Integrate after:** None.

**Implementation steps:**

1. Add assert/update/retract operations, fact identity/version, derived-fact dependency records, and transactional evaluation batches.
2. Truth maintenance retracts facts whose supporting facts disappear.
3. Bound fact count/bytes and dependency graph growth.

**Acceptance:** Retracting a base fact removes unsupported derived facts; equal-payload distinct facts remain distinct; duplicate operation identity is idempotent; cyclic support cannot keep an otherwise unsupported fact alive.

<a id="ticket-p16-t08"></a>

#### P16-T08 — Implement bounded agenda and conflict resolution

**Owns:** core/decision/agenda.rs (new). **Validation:** CORE (Core crate test). **Target:** `ticket_p16_t08`.

**Build after:** [P16-T07](#ticket-p16-t07), [P16-T03](#ticket-p16-t03), [C03](#ticket-c03). **Integrate after:** None.

**Implementation steps:**

1. Schedule matches with explicit salience, deterministic tie-break, no-loop/refraction policy, and firing/cycle budgets.
2. Stage effects separately from fact updates; pure mode has no external effects.
3. Snapshot agenda/facts when durable mode requested.

**Acceptance:** Known conflict fixtures produce exact firing sequence; self-triggering loop hits a clear budget error; cancellation stops promptly; restart at firing boundary preserves declared semantics.

<a id="ticket-p16-t09"></a>

#### P16-T09 — Optimize incremental rule matching with oracle

**Owns:** core/decision/matching.rs (new). **Validation:** CORE (Core crate test). **Target:** `ticket_p16_t09`.

**Build after:** [P16-T07](#ticket-p16-t07), [P16-T08](#ticket-p16-t08), [P10-T01](#ticket-p10-t01). **Integrate after:** None.

**Implementation steps:**

1. Keep a simple full-match reference evaluator.
2. Implement indexed incremental matching for the supported predicate subset; prototype shared condition nodes only after correctness.
3. Fall back for unsupported predicates and bound retained partial matches.

**Acceptance:** Random fact changes produce same activations/retractions as reference; duplicate/skewed facts preserve multiplicity; measured workload determines whether shared matching is enabled; memory limits hold.

<a id="ticket-p16-t10"></a>

#### P16-T10 — Expose and qualify decision services

**Owns:** server/handlers/decisions.rs (new); CLI/embedding adapters. **Validation:** CROSS (Affected crates and integration tests). **Target:** `ticket_p16_t10`.

**Build after:** [P16-T03](#ticket-p16-t03), [P16-T04](#ticket-p16-t04), [P16-T08](#ticket-p16-t08), [P13-T03](#ticket-p13-t03), [P11-T03](#ticket-p11-t03). **Integrate after:** None.

**Implementation steps:**

1. Expose version-pinned single/batch decisions and optional stateful sessions with authorization, deadlines, traces, and explicit durable-effect mode.
2. Register CEL/policy adapters independently when qualified.
3. Do not require them for native tables.

**Acceptance:** REST/CLI/embedded results agree; unsupported language level fails; namespace/session isolation holds; official/table/fact conformance suites pass; streaming deployments remain unaffected.


### B23. P17 — Compatibility, qualification, and an open ecosystem

<a id="ticket-p17-t01"></a>

#### P17-T01 — Publish compatibility and deprecation policy

**Owns:** docs/compatibility/ (new); capability registry output. **Validation:** DOC (Document/schema/tool checks).

**Build after:** [C02](#ticket-c02). **Integrate after:** None.

**Implementation steps:**

1. Specify API/config/SQL/state/extension versioning, qualification levels, supported upgrade windows, and deprecation evidence.
2. Document KEEP decisions and intentional dialect differences.
3. Derive implemented capabilities from registry rather than handwritten universal claims.

**Acceptance:** Every public capability names status and migration policy; no mock-only component is labelled supported; legacy versus reliable guarantees are distinguishable.

<a id="ticket-p17-t02"></a>

#### P17-T02 — Create cross-engine reference workload packs

**Owns:** test/workloads/ (new); existing benchmark adapters. **Validation:** CROSS (Affected crates and integration tests). **Target:** `ticket_p17_t02`.

**Build after:** [C05](#ticket-c05), [P10-T01](#ticket-p10-t01). **Integrate after:** None.

**Implementation steps:**

1. Add industrial alert, telemetry, stream SQL, CDC enrichment, decision, and disconnected-edge fixtures with expected outputs/failures.
2. Pin each reference engine/config and compare only equivalent contracts; unsupported mappings are labelled.

**Acceptance:** Independent expected identities/results reconcile; configs are reproducible; network/sink/guarantee differences cannot be hidden in a single ranking; raw measurements retained.

<a id="ticket-p17-t03"></a>

#### P17-T03 — Add boundary fuzz and property suites

**Owns:** core/sql/connector tests; fuzz targets. **Validation:** CROSS (Affected crates and integration tests). **Target:** `ticket_p17_t03`.

**Build after:** [P01-T01](#ticket-p01-t01), [C04](#ticket-c04). **Integrate after:** None.

**Implementation steps:**

1. Fuzz parser/value/codec inputs and property-test queue reservations, source progress, checkpoint transitions, and state serialization.
2. Use deterministic seeds and shrinkable failure cases; persist regressions instead of generating broad implementation-mirroring tests.

**Acceptance:** Any discovered failure becomes a minimal fixture; cancellation never leaks budget; round-trip state invariants hold; malformed data cannot trigger panic or unbounded resource growth.

<a id="ticket-p17-t04"></a>

#### P17-T04 — Implement per-capability qualification registry

**Owns:** test/qualification/registry.json and runner (new). **Validation:** CROSS (Affected crates and integration tests). **Target:** `ticket_p17_t04`.

**Build after:** [C02](#ticket-c02), [C07](#ticket-c07), [C08](#ticket-c08). **Integrate after:** None.

**Implementation steps:**

1. Map capability/version/profile/platform to required test commands and evidence.
2. Run adapters independently; missing infrastructure marks that capability unqualified.
3. Keep component tests separate from real-service/crash/upgrade certification.

**Acceptance:** A failed optional connector does not block another's qualification; missing evidence cannot become pass; registry rejects stale binary/config digests; mandatory release profile gates remain enforceable.

<a id="ticket-p17-t05"></a>

#### P17-T05 — Automate supported release and platform matrix

**Owns:** .github/workflows/; packaging verification fixtures. **Validation:** CROSS (Affected crates and integration tests). **Target:** `ticket_p17_t05`.

**Build after:** [P17-T04](#ticket-p17-t04). **Integrate after:** None.

**Implementation steps:**

1. Extend current Windows/Linux/macOS CI with targeted service/failure jobs, default/optional dependency builds, package install/start/stop, and restored-state cases.
2. Keep heavy tests separate from fast PR feedback, with required gates per capability.

**Acceptance:** Clean install and prior-version restore both tested; optional backends do not break default builds; required capability failure prevents its supported release label; test count/skip status visible.

<a id="ticket-p17-t06"></a>

#### P17-T06 — Publish runnable runbooks and generated examples

**Owns:** docs/runbooks/; docs/examples/; documentation generation. **Validation:** DOC (Document/schema/tool checks).

**Build after:** [P17-T01](#ticket-p17-t01), [P06-T02](#ticket-p06-t02). **Integrate after:** None.

**Implementation steps:**

1. Write install, inspect, drain, backup, restore, rollback, DLQ replay, credential rotation, and extension upgrade procedures tied to commands/fixtures.
2. Generate capability tables and example configs; mark experimental paths explicitly.

**Acceptance:** Example configurations validate; safe runbook rehearsals reproduce expected status; links/commands exist; docs cannot advertise a capability absent from the registry.

<a id="ticket-p17-t07"></a>

#### P17-T07 — Establish extension and release maintenance metadata

**Owns:** MAINTAINERS/CODEOWNERS or existing equivalents; release templates. **Validation:** DOC (Document/schema/tool checks).

**Build after:** [P17-T01](#ticket-p17-t01), [P12-T09](#ticket-p12-t09). **Integrate after:** None.

**Implementation steps:**

1. Assign core/extension ownership, compatibility review, issue reproduction template, security reporting, dependency/license review, artifact provenance, and release checklist.
2. Use current project conventions rather than introduce competing trackers.

**Acceptance:** Every supported extension/capability has a maintainer and qualification owner; release template requires actual evidence and migration notes; documentation-only work does not claim runtime certification.


### B24. Coverage and integration rules

All 17 platform tracks have their own ticket lane above. The original implementation specifications map as follows; these are references to the same work, not duplicate tickets.

| Specification | Implementation tickets | Integration or broader capability |
|---|---|---|
| A1 | [A01-T01](#ticket-a01-t01), [A01-T02](#ticket-a01-t02) | [P02-T06](#ticket-p02-t06) |
| A2 | [A02-T01](#ticket-a02-t01), [A02-T02](#ticket-a02-t02), [A02-T03](#ticket-a02-t03), [A02-T04](#ticket-a02-t04) | [P13-T05](#ticket-p13-t05) |
| A3 | [P02-T01](#ticket-p02-t01), [P02-T02](#ticket-p02-t02), [P02-T03](#ticket-p02-t03) | [P03-T03](#ticket-p03-t03) |
| A4 | [P15-T01](#ticket-p15-t01) | [P03-T06](#ticket-p03-t06), [P03-T17](#ticket-p03-t17) |
| A5 | [P15-T02](#ticket-p15-t02), [P15-T03](#ticket-p15-t03) | [P15-T04](#ticket-p15-t04) |
| A6 | [P11-T01](#ticket-p11-t01) | [P03-T08](#ticket-p03-t08), [P03-T09](#ticket-p03-t09), [P11-T04](#ticket-p11-t04) |
| A7 | [A07-T01](#ticket-a07-t01), [A07-T02](#ticket-a07-t02), [A07-T03](#ticket-a07-t03) | [P01-T01](#ticket-p01-t01), [P01-T10](#ticket-p01-t10), [P01-T11](#ticket-p01-t11), [P01-T12](#ticket-p01-t12), [P01-T13](#ticket-p01-t13), [P01-T14](#ticket-p01-t14), [P01-T15](#ticket-p01-t15), [P01-T16](#ticket-p01-t16), [P01-T17](#ticket-p01-t17) |
| A8 | [P04-T01](#ticket-p04-t01), [P04-T02](#ticket-p04-t02), [P04-T03](#ticket-p04-t03), [P04-T04](#ticket-p04-t04), [P04-T05](#ticket-p04-t05), [P04-T06](#ticket-p04-t06), [P04-T07](#ticket-p04-t07) | [P03-T15](#ticket-p03-t15) |
| A9 | [P03-T01](#ticket-p03-t01), [P03-T02](#ticket-p03-t02), [P03-T03](#ticket-p03-t03), [P03-T04](#ticket-p03-t04), [P03-T05](#ticket-p03-t05), [P03-T06](#ticket-p03-t06), [P03-T07](#ticket-p03-t07), [P03-T10](#ticket-p03-t10), [P03-T11](#ticket-p03-t11), [P03-T12](#ticket-p03-t12), [P03-T13](#ticket-p03-t13), [P03-T14](#ticket-p03-t14), [P03-T15](#ticket-p03-t15), [P03-T18](#ticket-p03-t18), [P03-T19](#ticket-p03-t19), [P03-T20](#ticket-p03-t20) | [P03-T08](#ticket-p03-t08), [P03-T09](#ticket-p03-t09), [P03-T16](#ticket-p03-t16), [P03-T17](#ticket-p03-t17), [P03-T21](#ticket-p03-t21), [P03-T22](#ticket-p03-t22), [P03-T23](#ticket-p03-t23) |
| A10 | [P15-T07](#ticket-p15-t07), [P15-T08](#ticket-p15-t08), [P15-T09](#ticket-p15-t09), [P15-T23](#ticket-p15-t23), [P15-T24](#ticket-p15-t24) | [P02-T07](#ticket-p02-t07) |
| A11 | [P10-T01](#ticket-p10-t01), [P10-T02](#ticket-p10-t02), [P10-T03](#ticket-p10-t03), [P10-T06](#ticket-p10-t06) | [P09-T06](#ticket-p09-t06) |

For shared interfaces, a change that breaks a consumer requires a versioned contract update, its golden-fixture change, and listed dependent tickets. Updating only the producer is not sufficient. Keep compatibility adapters until the documented migration completes.

For family work, every enabled member needs its own conformance evidence. P01's descriptor tickets cover individual function families; codec/connector/telemetry adapters register independently. A framework ticket does not certify all current or future implementations.

For filesystem, broker transactions, state migrations, and cluster ownership, normal code review examines the protocol as well as passing tests. No model may weaken an acceptance case to close its own ticket. A limitation can be an explicit supported-subset boundary, but the capability registry must enforce it.


### B25. Ready queues and release gates

**Immediately buildable tickets:** [C01](#ticket-c01), [C06](#ticket-c06), [C08](#ticket-c08), [A01-T01](#ticket-a01-t01), [A02-T01](#ticket-a02-t01), [A07-T01](#ticket-a07-t01), [A07-T02](#ticket-a07-t02), [A07-T03](#ticket-a07-t03), [P01-T01](#ticket-p01-t01), [P01-T05](#ticket-p01-t05), [P10-T01](#ticket-p10-t01), [P15-T02](#ticket-p15-t02), [P01-T06](#ticket-p01-t06). Ready means prerequisites are absent, not that two workers may edit the same file simultaneously.

| First dispatch lane | Ticket order | File-conflict rule |
|---|---|---|
| Contracts | [C01](#ticket-c01), then [C02](#ticket-c02) and [C03](#ticket-c03) independently; [C04](#ticket-c04) after both; [C05](#ticket-c05) after C03; [C07](#ticket-c07) after C04/C05 | Follow the explicit dependency rows. Reserve core exports for bootstrap. |
| REST | [A01-T01](#ticket-a01-t01) → [A01-T02](#ticket-a01-t02) | Own HTTP sink/helper edits; coordinate later retry wiring. |
| Image I/O | [A02-T01](#ticket-a02-t01) → [A02-T02](#ticket-a02-t02) and [A02-T03](#ticket-a02-t03) → [A02-T04](#ticket-a02-t04) | Platform files are independent; final image_sink wiring is serialized. |
| SQL correctness | [A07-T01](#ticket-a07-t01), [A07-T02](#ticket-a07-t02), [P01-T01](#ticket-p01-t01) | object_pick and array_except share array.rs: merge serially or agree nonoverlapping patches; tests can be prepared separately. |
| Persistence errors | [P01-T05](#ticket-p01-t05) | Bounded existing-handler correction; does not wait for the future catalog. |
| Formats | [P15-T02](#ticket-p15-t02) before connector wiring | Own the codec implementation; network adapters depend only on its contract. |
| Evidence/tooling | [C08](#ticket-c08), [P10-T01](#ticket-p10-t01), [P01-T06](#ticket-p01-t06) | Read-only inspection and new test/docs files avoid runtime integration conflicts. |


After these, use the actual dependency graph rather than a fixed team-wide phase. For example, LEAD parser work, schema models, telemetry mappings, resource primitives, and decision tables can proceed concurrently when their small contract prerequisites are complete. A Windows image backend does not block JSONL, and Milvus does not block pgvector.

The following gates are capability-specific. Each gate applies only to the profile being enabled, so one unfinished adapter cannot hold the entire engine hostage.

| Capability being enabled | Mandatory closing evidence |
|---|---|
| Correctness fixes | Reproduction/KEEP evidence plus the corresponding production-entry regression. |
| Reliable single-source SQL | P03-T07 and P03-T20; source/action identity and limits wired; no unqualified participant or lossy path. |
| Durable outbox-backed source acknowledgement | P03-T08/T09/T10 plus source-specific redelivery and real-service crash tests. |
| LEAD without recovery | P04-T01–T05 and ordering/EOF/cap fixtures. No durable claim. |
| Checkpointed LEAD | P04-T06/T07 and its actual P03 participant integration. |
| Telemetry transport | Its own codec/transport tests and P07-T07 subset evidence. Reliable export additionally needs qualified P03 effects. |
| CDC/maintained relations | Required P09 operator/snapshot/handover tests and the selected source/sink service evidence. |
| Decision language | Native table/agenda tests or that language adapter's pinned conformance subset; no dependency on unrelated language adapters. |
| Extensions/embedding | Corresponding P12 ABI/lifecycle/permission tests, actual compiled sample, and supported-platform checks. |
| Multitenant hosting | P13-T03–T07, including every exposed REST/CLI/MCP/fleet path and resource isolation. |
| Active/passive HA | P14-T01–T03 plus their P14-T09 scenarios; distributed partitioning can remain disabled. |
| Partitioned/rescaled cluster | P14-T04–T07 plus rescale/failure scenarios; online migration/DR qualify independently. |
| Supported release | P17-T04/T05 registry and CI evidence for advertised capabilities; truthful compatibility/migration docs. |

A smaller model's completion reply should contain: **ticket ID; outcome (KEEP / COMPONENT DONE / INTEGRATED / QUALIFIED / BLOCKED); starting SHA; changed files; exact tests and counts; remaining limitations; handoff path; newly ready ticket IDs.** It must not claim an entire platform track complete after finishing one component.

