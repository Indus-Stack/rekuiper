# Memory Allocator

This page describes the memory allocator that the `rekuiper` daemon uses and why.

## What changed

On Unix systems, the daemon uses the **jemalloc** allocator in place of the system allocator.
This applies to the `kuiperd`, `rekuiperd`, `rule-engine` and `ruleengine` binaries.
On Windows, the daemon continues to use the system allocator.

## Why

The official Docker image is built for the musl C library (Alpine Linux).
The musl allocator uses one coarse lock.
When many threads allocate memory at the same time, the threads wait for each other.
Adding CPU cores then gives little or no extra throughput.

jemalloc gives each thread its own allocation arenas.
Threads do not wait for each other during normal operation.

## Single-record requests

A request to `POST /streams/{name}/data` can contain one record or many records.

- A request with one record uses a lock-free publish path.
- A request with two or more records keeps the all-or-nothing batch admission gate.

Both paths deliver each record to each subscriber of the stream exactly once.
The lock-free path matters for workloads that send one event per request.
The batch admission gate lets only one batch request at a time enter a stream.

<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 860 200" width="100%" height="auto" role="img" aria-label="Single-record and batch ingestion paths" font-family="-apple-system, BlinkMacSystemFont, Segoe UI, sans-serif">
  <rect x="0" y="0" width="860" height="200" rx="10" fill="#f8fafc" stroke="#e2e8f0" stroke-width="1" />
  <text x="430" y="26" text-anchor="middle" font-size="14" font-weight="700" fill="#0f172a">HTTP ingestion: one record or many records</text>
  <rect x="25" y="70" width="170" height="60" rx="8" fill="#ffffff" stroke="#cbd5e1" stroke-width="1.5" />
  <text x="110" y="96" text-anchor="middle" font-size="12" font-weight="700" fill="#1e40af">POST /streams/{name}/data</text>
  <text x="110" y="114" text-anchor="middle" font-size="10" fill="#64748b">count the records</text>
  <path d="M195 90 L290 62" stroke="#94a3b8" stroke-width="1.5" fill="none" />
  <path d="M195 110 L290 138" stroke="#94a3b8" stroke-width="1.5" fill="none" />
  <rect x="290" y="40" width="250" height="44" rx="8" fill="#ecfdf5" stroke="#10b981" stroke-width="1.5" />
  <text x="415" y="60" text-anchor="middle" font-size="12" font-weight="700" fill="#065f46">One record</text>
  <text x="415" y="76" text-anchor="middle" font-size="10" fill="#065f46">lock-free publish to each subscriber</text>
  <rect x="290" y="116" width="250" height="44" rx="8" fill="#eff6ff" stroke="#3b82f6" stroke-width="1.5" />
  <text x="415" y="136" text-anchor="middle" font-size="12" font-weight="700" fill="#1e40af">Two or more records</text>
  <text x="415" y="152" text-anchor="middle" font-size="10" fill="#1e40af">all-or-nothing batch admission gate</text>
  <path d="M540 62 L640 90" stroke="#94a3b8" stroke-width="1.5" fill="none" />
  <path d="M540 138 L640 110" stroke="#94a3b8" stroke-width="1.5" fill="none" />
  <rect x="640" y="70" width="195" height="60" rx="8" fill="#ffffff" stroke="#cbd5e1" stroke-width="1.5" />
  <text x="737" y="96" text-anchor="middle" font-size="12" font-weight="700" fill="#0f172a">Rule queues</text>
  <text x="737" y="114" text-anchor="middle" font-size="10" fill="#64748b">each record delivered once</text>
</svg>

## Limits

- You cannot select another allocator at run time.
  To use a different allocator, build the daemon from source and change the `#[global_allocator]` in `crates/kuiperd/src/lib.rs`.
- Memory-profiling tools that assume the system allocator can report different numbers.
