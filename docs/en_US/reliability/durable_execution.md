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
4. Legacy `SqliteKvStore` behavior remains unchanged for lightweight rules.

## Ordered Reliable Ingress and Delivery Accounting

- `ReliableInputSender` (`crates/rekuiper-server/src/engine/reliable_input.rs`) enforces ownership generation and cancellable bounded admission without changing the default `StreamReceiver` fast path.
- `PartitionDeliveryTracker` (`crates/rekuiper-core/src/reliability/delivery.rs`) tracks multi-row fanout per source position.
- Zero-branch filtered messages complete immediately when at the head of the partition queue.
- Out-of-order completions never skip an incomplete offset, and malformed inputs block progress until a terminal disposition is recorded.

## Schema Persistence Error Propagation

Schema endpoints (`POST /schemas/:kind`, `PUT /schemas/:kind/:name`, `PUT /schemas/:kind/:name/upload`, and `DELETE /schemas/:kind/:name`) now propagate storage errors and return HTTP `500` instead of returning success when KV persistence fails.

