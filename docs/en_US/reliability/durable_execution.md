# Durable Execution and Recovery Contracts

This document describes the core reliable execution contracts, atomic checkpoint storage, and ordered ingress controls in `rekuiper`.

![Reliable Checkpoint, Outbox, and Delivery Flow](/diagrams/reliable_checkpoint_flow.svg)

## Core Contracts

`rekuiper-core` defines versioned contracts for durable rules:

- **Identities (`contracts/identity.rs`)**: Logical IDs (`RuleId`, `OperatorId`, `ActionId`, `ConnectionId`, `SourceId`, `PartitionId`, `PositionCodecId`) use 1 to 128 UTF-8 bytes. Display names stay separate from logical ownership. `Generation` and `CheckpointId` use unsigned 64-bit counters.
- **Source Positions (`contracts/position.rs`)**: `SourcePosition` stores opaque cursor bytes with source, partition, generation, and codec metadata. Comparisons fail if partitions, generations, or codecs do not match.
- **Structured Errors (`contracts/error.rs`)**: `StructuredError` maps each error code to a fixed public message. Deserialization discards incoming wire messages to prevent secret leaks.
- **Capabilities (`contracts/capability.rs`)**: Components declare supported input modes, result modes, ordering scope, and limits explicitly. Missing declarations mean unsupported.
- **Clocks and Ordered Controls (`contracts/clock.rs`, `contracts/control.rs`)**: `OrderedEnvelope` keeps data, checkpoint barriers, watermarks, and end-of-input markers in one FIFO queue.
- **State, Checkpoints, and Effects (`contracts/state.rs`, `contracts/checkpoint.rs`, `contracts/effect.rs`)**: `CheckpointEnvelope` binds plan, schema, and extension fingerprints to participant snapshots and outbox boundaries. Effect completion is idempotent: one terminal outcome (`Delivered` or `PermanentFailure`) locks the record.

## Atomic SQLite Checkpoint and Outbox Store

`ReliableSqliteStore` (`crates/rekuiper-core/src/reliability/sqlite.rs`) stores checkpoints, source positions, participant state, and outbox effects in one SQLite transaction:

1. The store enables `PRAGMA journal_mode = WAL`, `PRAGMA synchronous = FULL` (or `EXTRA`), and `PRAGMA foreign_keys = ON`.
2. The store reads back each PRAGMA setting and fails closed if SQLite does not confirm it.
3. Commits check `reliable_generations` and reject stale generations before writing.
4. `SqliteOutbox` (`crates/rekuiper-core/src/reliability/outbox.rs`) enqueues outbox entries in the same SQLite transaction, enforces `max_items` and `max_bytes` limits, deduplicates repeated effect IDs, and scopes `claim_pending` by `(rule_id, action_id)`.
5. Legacy `SqliteKvStore` behavior remains unchanged for lightweight rules.

## Operator State Snapshots

- `crates/rekuiper-server/src/engine/state/analytic_count_tumble.rs` provides `AnalyticStateParticipant`, `CountWindowStateParticipant`, and `TumbleWindowStateParticipant`.
- Each participant captures an owned, SHA-256 checksummed snapshot between records without touching the lightweight execution hotpath.

## Ordered Reliable Ingress and Delivery Accounting

- `ReliableInputSender` (`crates/rekuiper-server/src/engine/reliable_input.rs`) enforces ownership generation and cancellable bounded admission without changing the default `StreamReceiver` fast path.
- `PartitionDeliveryTracker` (`crates/rekuiper-core/src/reliability/delivery.rs`) tracks multi-row fanout per source position.
- Zero-branch filtered messages complete immediately when at the head of the partition queue.
- Out-of-order completions never skip an incomplete offset, and malformed inputs block progress until a terminal disposition is recorded.

## Schema Persistence Error Propagation

Schema endpoints (`POST /schemas/:kind`, `PUT /schemas/:kind/:name`, `PUT /schemas/:kind/:name/upload`, and `DELETE /schemas/:kind/:name`) now propagate storage errors and return HTTP `500` instead of returning success when KV persistence fails.

## Generation Fencing and Outbox Dispatch

![Single-Source Recovery, Fencing, and Outbox Dispatch](/diagrams/reliable_outbox_recovery.svg)

- **Generation Fencing (`core/reliability/generation.rs`)**: `GenerationFence` activates new rule generations and revokes older generations. Revoked writers cannot commit checkpoints or claim pending outbox entries, while durable unsettled effects remain intact.
- **Outbox Dispatch (`server/engine/outbox_dispatch.rs`)**: `OutboxDispatcher` and `DispatchCoordinator` enforce exclusive `(rule_id, action_id)` leases, preserve per-`order_key` ordering, pass deterministic idempotency keys, and route permanent failures to an explicit DLQ policy. Unsettled `Retryable` and `Ambiguous` effects stay in SQLite so restarts resend them.

## Resource Budgets, Batching, and Bounded Retries

- **Resource Budgets (`core/resources/budget.rs`)**: `ResourceBudget` tracks item, logical-byte, observed physical-byte, and task charges with cancellable reservations and idempotent commit release.
- **Action Batching (`server/engine/batching.rs`)**: `ActionBatchAssembler` flushes independently on count, encoded-byte, deadline, barrier, and shutdown boundaries. Partial failures retain only unsettled retryable items, and unset byte limits preserve legacy `batchSize` and `linger` behavior.
- **Bounded Retries (`server/engine/retry.rs`)**: `execute_with_retry` classifies adapter results into `Delivered`, `Retryable`, `PermanentFailure`, and `Ambiguous`, keeps HTTP `2xx` body-drain errors as `Delivered`, and applies deterministic seeded jitter with bounded `Retry-After` hints.

## Direct Kafka Replay and Single-Source SQL Recovery

- **Kafka Position Codec (`connectors/src/kafka.rs`)**: `KafkaPositionCodec` encodes `i64` next-offset cursors, detects retention gaps before intake, emits metadata and position before payload decoding, and advances only on full multirecord commit boundaries.
- **Single-Source Runner (`server/engine/runtime.rs`)**: `ReliableSingleSourceRunner` validates supported single-source plans (analytic, count, and tumbling windows), restores state and Kafka offset before intake, flushes sink output before checkpoint commit, and exposes `AtLeastOnce` guarantee and checkpoint age telemetry.

## Typed SQL Planning Descriptors

- **Typed Planning (`sql/planning/types.rs`, `sql/functions.rs`)**: `type_check_expr` and `FunctionDescriptorRegistry` validate known schema fields, arity, nullability, determinism, side effects, and statefulness. Unknown functions receive conservative descriptors that block unsafe rewrites and durable replay, while legacy schemaless runtime evaluation remains unchanged.
- **Capability-Aware Plan Validation (`sql/planning/capabilities.rs`)**: `validate_plan_capabilities` checks `PlanExecutionProfile` requirements against source replay/offset, sink idempotency, state-snapshot, and ordered-barrier capabilities. Strict reliable or durable profiles fail before execution with structured `PlanDiagnosticKind` diagnostics, while default best-effort plans continue unchanged.

## Opt-In Lossy Subscriber Queues and Window Snapshots

- **Opt-In Lossy Subscriber FIFO (`core/resources/lossy_queue.rs`, `core/runtime.rs`)**: `lossy_channel` provides a bounded `dropOldest` ring queue for non-reliable subscribers via `subscribe_lossy` and `subscribe_lossy_filtered`. The default blocking `mpsc` fast path has zero hotpath overhead (`has_lossy: AtomicBool` guard), and `push_reliable_control` rejects control barriers and watermarks on lossy queues.
- **Hopping and Sliding Window Snapshots (`server/engine/state/hop_slide.rs`)**: `HoppingWindowStateParticipant` and `SlidingWindowStateParticipant` capture overlapping buckets, pending triggers, watermarks, and logical deadlines with SHA-256 checksums and plan-fingerprint validation, applying `MissedDeadlinePolicy` (`FireImmediatelyInOrder` or `DropExpired`) on recovery.
- **Session and State Window Snapshots (`server/engine/state/session_state.rs`)**: `SessionWindowStateParticipant` and `StateWindowStateParticipant` persist per-key open sessions, gap and max-duration boundaries, `on_begin` state flags, and logical deadlines without serializing `Instant` or re-evaluating historical nondeterministic predicates.
- **Join and CEP Snapshots (`server/engine/state/joins_cep.rs`)**: `JoinBufferStateParticipant` and `CepSequenceStateParticipant` persist retained rows, match progress, expiry timestamps, and input identities, deterministically rebuild derived key indexes on restore, and reject plan or key-schema mismatches without migration.

